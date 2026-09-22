"""Tests for complete configuration, overlays, and source provenance."""

from datetime import datetime, timedelta, timezone
import json
import threading

import pytest
from pydantic import ValidationError

from orchestrator.config import (
    ConfigCandidate,
    ConfigManager,
    ConfigOverlay,
    ConfigSource,
    ConfigurationLoadError,
    EffectiveConfig,
    ModelRegistryManifest,
    ModelSpec,
    PolicyEnvelope,
    PriceSpec,
    ProviderSpec,
    RunConfigSnapshot,
    RunConfigSnapshotError,
    config_overlay_schema,
    effective_config_schema,
    load_config_overlay_yaml,
    load_effective_config_yaml,
    resolve_effective_config,
)
from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.snapshots import SnapshotStore
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


NOW = datetime(2026, 9, 22, tzinfo=timezone.utc)
ROLES = (
    "planner",
    "coder",
    "document_analyst",
    "researcher",
    "tester",
    "reviewer",
    "director",
)


def registry() -> ModelRegistryManifest:
    provider = ProviderSpec(
        id="primary",
        adapter="openai_responses",
        secret_ref="env:MAESTRO_TEST_KEY",
        enabled=True,
    )
    price = PriceSpec(
        currency="USD",
        input_minor_per_million=100,
        output_minor_per_million=200,
        max_tool_cost_minor=10,
        estimator_id="tokenizer.v1",
        effective_from=NOW,
        expires_at=NOW + timedelta(days=30),
    )
    models = tuple(
        ModelSpec(
            id=model_id,
            provider="primary",
            remote_model=f"remote-{model_id}",
            tier=tier,
            capabilities={"text", "tools"},
            context_window=32_000,
            max_output_tokens=8_000,
            supported_reasoning_efforts={"low", "medium", "high"},
            price=price,
        )
        for model_id, tier in (
            ("model-economy", "economy"),
            ("model-standard", "standard"),
            ("model-high", "high"),
        )
    )
    denied_providers = tuple(
        ProviderSpec(
            id=provider_id,
            adapter="openai_responses",
            secret_ref=f"env:{provider_id.upper().replace('-', '_')}_KEY",
            enabled=False,
        )
        for provider_id in ("blocked-provider", "user-deny", "project-deny")
    )
    return ModelRegistryManifest(providers=(provider, *denied_providers), models=models)


def selector(rule_id: str, model_id: str = "model-standard", priority: int = 10) -> dict:
    return {
        "id": rule_id,
        "priority": priority,
        "when": {"risk": ["high"]},
        "select": {"model": model_id},
        "reasoning_effort": None,
        "fallback": [],
        "allow_degraded": False,
    }


def guard(rule_id: str, max_agents: int = 8) -> dict:
    return {
        "id": rule_id,
        "when": {"risk": ["high", "critical"]},
        "constraints": {"max_agents": max_agents},
    }


def config_data(reg: ModelRegistryManifest) -> dict:
    profile = {
        "candidates": ["tier:standard"],
        "reasoning_effort": "medium",
        "max_output_tokens": 1_000,
        "retries": 2,
        "escalate_to": "tier:high",
    }
    presets = {}
    for name in ("economic", "balanced", "quality", "custom"):
        presets[name] = {
            "requested_budget": {
                "max_cost_minor": 1_000,
                "max_total_tokens": 50_000,
                "max_agents": 10,
                "max_depth": 4,
                "max_concurrency": 4,
                "max_parallel_candidates": 3,
            },
            "roles": {role: dict(profile) for role in ROLES},
            "selector_rules": [selector(f"{name}-risk")],
            "guard_rules": [guard(f"{name}-guard")],
            "health_policy_ref": "default-health",
        }
    return {
        "schema_version": 1,
        "active_preset": "balanced",
        "registry_manifest_ref": reg.content_hash,
        "policy_envelope": {
            "currency": "USD",
            "max_cost_minor": 500,
            "max_total_tokens": 40_000,
            "max_agents": 8,
            "max_depth": 4,
            "max_concurrency": 4,
            "allowed_models": ["model-standard", "model-high"],
            "denied_providers": ["blocked-provider"],
            "min_tier": "standard",
            "require_independent_review": False,
            "require_approval": False,
            "max_parallel_candidates": 3,
        },
        "mandatory_guard_rules": [guard("system-risk-guard", 6)],
        "presets": presets,
        "classifier": {
            "id": "orchestrator.deterministic.v1",
            "version": "1.0.0",
            "normalization_version": "unicode-nfkc-v1",
            "taxonomy_version": "maestro-task-taxonomy-v1",
        },
        "health_policies": {
            "default-health": {
                "id": "default-health",
                "failure_window_ms": 60_000,
                "degrade_after": 2,
                "open_after": 4,
                "recovery_successes": 2,
                "cooldown_ms": 10_000,
                "max_probe_permits": 1,
            }
        },
    }


def base_config(reg: ModelRegistryManifest) -> EffectiveConfig:
    return EffectiveConfig.model_validate(config_data(reg))


def test_effective_config_is_complete_and_content_addressed():
    reg = registry()
    config = base_config(reg)

    assert config.registry_manifest_ref == reg.content_hash
    assert set(config.presets) == {"economic", "balanced", "quality", "custom"}
    assert set(config.presets["balanced"].roles) == set(ROLES)
    assert config.content_hash.startswith("sha256:")
    assert len(config.content_hash.removeprefix("sha256:")) == 64


def test_four_layers_resolve_with_scalar_precedence_and_null_fallback():
    reg = registry()
    system = base_config(reg)
    result = resolve_effective_config(
        system,
        registry=reg,
        user_global=ConfigOverlay.model_validate(
            {"active_preset": "economic", "presets": {"balanced": {"roles": {"coder": {
                "max_output_tokens": 2_000, "reasoning_effort": "high"
            }}}}}
        ),
        project=ConfigOverlay.model_validate(
            {"active_preset": "quality", "presets": {"balanced": {"roles": {"coder": {
                "max_output_tokens": 3_000
            }}}}}
        ),
        run=ConfigOverlay.model_validate(
            {"active_preset": "balanced", "presets": {"balanced": {"roles": {"coder": {
                "max_output_tokens": None, "reasoning_effort": "low"
            }}}}}
        ),
    )

    assert result.config.active_preset == "balanced"
    coder = result.config.presets["balanced"].roles["coder"]
    assert coder.max_output_tokens == 3_000
    assert coder.reasoning_effort == "low"
    assert result.field_sources["/active_preset"] == (ConfigSource.RUN,)
    assert result.field_sources["/presets/balanced/roles/coder/max_output_tokens"] == (ConfigSource.PROJECT,)
    assert result.field_sources["/presets/balanced/roles/coder/reasoning_effort"] == (ConfigSource.RUN,)
    assert result.field_sources[
        "/presets/balanced/selector_rules/balanced-risk/reasoning_effort"
    ] == (ConfigSource.SYSTEM_DEFAULT,)


def test_ordinary_lists_replace_and_selector_rules_merge_by_id():
    reg = registry()
    result = resolve_effective_config(
        base_config(reg),
        registry=reg,
        user_global=ConfigOverlay.model_validate(
            {"presets": {"balanced": {
                "roles": {"coder": {"candidates": ["model-economy"]}},
                "selector_rules": [selector("balanced-risk", "model-high", 20), selector("user-added")],
            }}}
        ),
        project=ConfigOverlay.model_validate(
            {"presets": {"balanced": {"selector_rules": [{"id": "user-added", "disabled": True}]}}}
        ),
    )

    preset = result.config.presets["balanced"]
    assert preset.roles["coder"].candidates == ("model-economy",)
    assert [(rule.id, rule.priority, rule.select.model) for rule in preset.selector_rules] == [
        ("balanced-risk", 20, "model-high")
    ]
    assert result.field_sources["/presets/balanced/selector_rules/balanced-risk/priority"] == (ConfigSource.USER_GLOBAL,)
    assert result.field_sources["/presets/balanced/selector_rules/user-added"] == (ConfigSource.PROJECT,)


def test_duplicate_selector_ids_in_one_layer_are_rejected():
    with pytest.raises(ValidationError, match="unique within one overlay layer"):
        ConfigOverlay.model_validate(
            {"presets": {"balanced": {"selector_rules": [selector("same"), selector("same")]}}}
        )


def test_same_priority_selectors_that_can_overlap_are_rejected():
    data = config_data(registry())
    data["presets"]["balanced"]["selector_rules"].append(selector("second-risk"))
    with pytest.raises(ValidationError, match="same-priority selector rules"):
        EffectiveConfig.model_validate(data)


def test_role_escalation_cycles_and_invalid_health_thresholds_are_rejected():
    reg = registry()
    data = config_data(reg)
    data["presets"]["balanced"]["roles"]["reviewer"]["escalate_to"] = "director"
    data["presets"]["balanced"]["roles"]["director"]["escalate_to"] = "reviewer"
    with pytest.raises(ValidationError, match="must not contain cycles"):
        EffectiveConfig.model_validate(data)

    data = config_data(reg)
    data["health_policies"]["default-health"]["open_after"] = 2
    with pytest.raises(ValidationError, match="open_after must be greater"):
        EffectiveConfig.model_validate(data)


def test_monetary_budget_requires_currency_and_escalation_must_increase_tier():
    reg = registry()
    data = config_data(reg)
    data["policy_envelope"]["currency"] = None
    with pytest.raises(ValidationError, match="explicit currency"):
        EffectiveConfig.model_validate(data)

    data = config_data(reg)
    data["policy_envelope"].pop("max_total_tokens")
    with pytest.raises(ValidationError, match="max_total_tokens"):
        EffectiveConfig.model_validate(data)

    data = config_data(reg)
    data["presets"]["balanced"]["roles"]["coder"]["escalate_to"] = "tier:standard"
    with pytest.raises(ValueError, match="strictly higher tier"):
        resolve_effective_config(EffectiveConfig.model_validate(data), registry=reg)


def test_invalid_selector_fallback_and_config_load_errors_fail_closed():
    import yaml

    reg = registry()
    data = config_data(reg)
    data["presets"]["balanced"]["selector_rules"][0]["fallback"] = [{"model": "model-high"}]
    with pytest.raises(ValueError, match="fallback must stay at the selected tier"):
        resolve_effective_config(EffectiveConfig.model_validate(data), registry=reg)

    secret = "sk-should-never-appear-in-config-errors"
    source = yaml.safe_dump(base_config(reg).model_dump(mode="json"), sort_keys=False)
    source = source.replace(reg.content_hash, secret)
    with pytest.raises(ConfigurationLoadError) as raised:
        load_effective_config_yaml(source)
    assert secret not in str(raised.value)


def test_role_and_selector_reasoning_effort_must_exist_on_registered_candidates():
    reg = registry()
    with pytest.raises(ValueError, match="role reasoning effort is unsupported"):
        resolve_effective_config(
            base_config(reg), registry=reg,
            project=ConfigOverlay.model_validate({"presets": {"balanced": {"roles": {
                "coder": {"reasoning_effort": "ultra"}
            }}}}),
        )

    selector_rule = selector("balanced-risk")
    selector_rule["reasoning_effort"] = "ultra"
    data = config_data(reg)
    data["presets"]["balanced"]["selector_rules"] = [selector_rule]
    with pytest.raises(ValueError, match="selector reasoning effort is unsupported"):
        resolve_effective_config(EffectiveConfig.model_validate(data), registry=reg)


def test_guard_rules_accumulate_and_cannot_be_replaced_or_disabled():
    reg = registry()
    result = resolve_effective_config(
        base_config(reg),
        registry=reg,
        user_global=ConfigOverlay.model_validate(
            {"presets": {"balanced": {"guard_rules": [guard("user-guard", 3)]}}}
        ),
    )
    ids = {rule.id for rule in result.config.presets["balanced"].guard_rules}
    assert {"balanced-guard", "user-guard"} <= ids
    assert result.config.mandatory_guard_rules[0].id == "system-risk-guard"

    with pytest.raises(ValueError, match="cannot be replaced or weakened"):
        resolve_effective_config(
            base_config(reg),
            registry=reg,
            user_global=ConfigOverlay.model_validate(
                {"presets": {"balanced": {"guard_rules": [guard("balanced-guard", 100)]}}}
            ),
        )
    with pytest.raises(ValidationError):
        ConfigOverlay.model_validate({"mandatory_guard_rules": []})


def test_policy_envelopes_tighten_monotonically_and_keep_all_sources():
    reg = registry()
    result = resolve_effective_config(
        base_config(reg),
        registry=reg,
        user_global=ConfigOverlay.model_validate({"policy_envelope": {
            "max_cost_minor": 900,
            "allowed_models": ["model-standard"],
            "denied_providers": ["user-deny"],
            "min_tier": "high",
            "require_approval": True,
            "max_parallel_candidates": 2,
        }}),
        project=ConfigOverlay.model_validate({"policy_envelope": {
            "max_cost_minor": 700,
            "allowed_models": ["model-standard", "model-high"],
            "denied_providers": ["project-deny"],
            "max_parallel_candidates": 1,
        }}),
        run=ConfigOverlay.model_validate({"policy_envelope": {"max_cost_minor": 600}}),
    )

    policy = result.config.policy_envelope
    assert policy.max_cost_minor == 500
    assert policy.allowed_models == frozenset({"model-standard"})
    assert policy.denied_providers == frozenset({"blocked-provider", "user-deny", "project-deny"})
    assert policy.min_tier == "high"
    assert policy.require_approval is True
    assert policy.max_parallel_candidates == 1
    assert result.policy_envelope_sources["max_cost_minor"] == (
        ConfigSource.SYSTEM_DEFAULT, ConfigSource.USER_GLOBAL, ConfigSource.PROJECT, ConfigSource.RUN
    )
    assert result.policy_envelope_sources["allowed_models"] == (
        ConfigSource.SYSTEM_DEFAULT, ConfigSource.USER_GLOBAL, ConfigSource.PROJECT
    )


def test_health_and_classifier_mappings_merge_recursively_with_null_fallback():
    reg = registry()
    result = resolve_effective_config(
        base_config(reg),
        registry=reg,
        user_global=ConfigOverlay.model_validate({
            "health_policies": {"default-health": {"degrade_after": 3}},
            "classifier": {"version": "1.1.0"},
        }),
        project=ConfigOverlay.model_validate({"health_policies": {"default-health": {
            "recovery_successes": 4
        }}}),
        run=ConfigOverlay.model_validate({
            "health_policies": {"default-health": {"degrade_after": None}},
            "classifier": {"version": None},
        }),
    )

    assert result.config.health_policies["default-health"].degrade_after == 3
    assert result.config.health_policies["default-health"].recovery_successes == 4
    assert result.config.classifier.version == "1.1.0"
    assert result.field_sources["/health_policies/default-health/degrade_after"] == (
        ConfigSource.USER_GLOBAL,
    )
    assert result.field_sources["/health_policies/default-health/recovery_successes"] == (
        ConfigSource.PROJECT,
    )


def test_currency_mismatch_and_registry_mutation_fail_closed():
    reg = registry()
    with pytest.raises(ValueError, match="exchange-rate snapshot"):
        resolve_effective_config(
            base_config(reg), registry=reg,
            user_global=ConfigOverlay.model_validate({"policy_envelope": {"currency": "EUR"}}),
        )
    with pytest.raises(ValidationError):
        ConfigOverlay.model_validate({"registry_manifest_ref": "sha256:" + "0" * 64})


def test_resolved_mappings_are_deeply_immutable_and_hash_excludes_provenance():
    reg = registry()
    system = base_config(reg)
    via_user = resolve_effective_config(
        system, registry=reg,
        user_global=ConfigOverlay.model_validate({"presets": {"balanced": {"roles": {"coder": {"retries": 5}}}}}),
    )
    via_project = resolve_effective_config(
        system, registry=reg,
        project=ConfigOverlay.model_validate({"presets": {"balanced": {"roles": {"coder": {"retries": 5}}}}}),
    )

    assert via_user.content_hash == via_project.content_hash
    assert via_user.field_sources["/presets/balanced/roles/coder/retries"] != via_project.field_sources[
        "/presets/balanced/roles/coder/retries"
    ]
    with pytest.raises(TypeError, match="immutable"):
        via_user.config.presets["balanced"] = via_user.config.presets["quality"]
    with pytest.raises(TypeError, match="immutable"):
        via_user.field_sources["/active_preset"] = (ConfigSource.RUN,)


def test_references_escalation_health_and_complete_preset_are_validated():
    reg = registry()
    data = config_data(reg)
    data["presets"].pop("quality")
    with pytest.raises(ValidationError, match="required preset definitions"):
        EffectiveConfig.model_validate(data)

    data = config_data(reg)
    data["presets"]["balanced"]["health_policy_ref"] = "missing"
    with pytest.raises(ValidationError, match="unknown health policy"):
        EffectiveConfig.model_validate(data)

    data = config_data(reg)
    data["presets"]["balanced"]["roles"]["coder"]["candidates"] = ["missing-model"]
    with pytest.raises(ValueError, match="candidate reference does not resolve"):
        resolve_effective_config(EffectiveConfig.model_validate(data), registry=reg)


def test_yaml_loaders_and_json_schemas_cover_full_config_and_overlay():
    import yaml

    reg = registry()
    full = load_effective_config_yaml(
        yaml.safe_dump(base_config(reg).model_dump(mode="json"), sort_keys=False)
    )
    overlay = load_config_overlay_yaml("active_preset: quality\npresets:\n  balanced:\n    roles:\n      coder:\n        retries: 3\n")

    assert full.content_hash == base_config(reg).content_hash
    assert overlay.presets["balanced"].roles["coder"].retries == 3
    full_schema = effective_config_schema()
    overlay_schema = config_overlay_schema()
    assert full_schema["properties"]["presets"]["additionalProperties"]["$ref"].endswith("Preset")
    assert full_schema["additionalProperties"] is False
    assert overlay_schema["additionalProperties"] is False
    assert "registry_manifest_ref" not in overlay_schema["properties"]


def runtime_candidate(reg: ModelRegistryManifest, requested_cost: int) -> ConfigCandidate:
    overlay = ConfigOverlay.model_validate(
        {
            "presets": {
                "balanced": {"requested_budget": {"max_cost_minor": requested_cost}}
            }
        }
    )
    resolved = resolve_effective_config(
        base_config(reg), registry=reg, project=overlay
    )
    return ConfigCandidate(
        resolved=resolved,
        registry=reg,
        source_details={"project": f"fixture:budget-{requested_cost}"},
    )


def test_failed_reload_keeps_active_config_and_generation_unchanged():
    reg = registry()
    active = runtime_candidate(reg, 100)
    manager = ConfigManager(active)

    rejected = manager.reload(lambda: ConfigCandidate.model_validate({"invalid": True}))

    assert not rejected.applied
    assert rejected.error_code == "candidate_rejected"
    assert rejected.generation == 1
    assert manager.generation == 1
    assert manager.active is active
    assert rejected.effective_config_hash == active.resolved.config.content_hash


def test_config_and_snapshot_reject_registry_or_hash_mismatch(tmp_path):
    reg = registry()
    candidate = runtime_candidate(reg, 100)
    changed_registry_data = reg.model_dump(mode="json")
    changed_registry_data["providers"][0]["secret_ref"] = "env:OTHER_TEST_KEY"
    changed_registry = ModelRegistryManifest.model_validate(changed_registry_data)
    with pytest.raises(ValidationError, match="candidate registry"):
        ConfigCandidate(resolved=candidate.resolved, registry=changed_registry)

    store = SQLiteEventStore(tmp_path / "snapshot-model-validation.db")
    snapshot = ConfigManager(candidate).start_run("snapshot-validation", store)
    snapshot_data = snapshot.model_dump(mode="json")

    bad_effective_hash = dict(snapshot_data, effective_config_hash="sha256:" + "0" * 64)
    with pytest.raises(ValidationError, match="effective_config_hash"):
        RunConfigSnapshot.model_validate_json(json.dumps(bad_effective_hash))

    bad_registry_hash = dict(snapshot_data, registry_manifest_hash="sha256:" + "0" * 64)
    with pytest.raises(ValidationError, match="registry_manifest_hash"):
        RunConfigSnapshot.model_validate_json(json.dumps(bad_registry_hash))

    mismatched_registry = dict(
        snapshot_data,
        registry_manifest=changed_registry.model_dump(mode="json"),
        registry_manifest_hash=changed_registry.content_hash,
    )
    with pytest.raises(ValidationError, match="references a different registry"):
        RunConfigSnapshot.model_validate_json(json.dumps(mismatched_registry))
    store.close()


def test_reload_rejects_non_candidate_results_without_mutating_state():
    candidate = runtime_candidate(registry(), 100)
    manager = ConfigManager(candidate)

    result = manager.reload(lambda: None)

    assert not result.applied
    assert manager.active is candidate
    assert manager.generation == 1
    with pytest.raises(TypeError, match="callable"):
        manager.reload(None)


def test_run_start_during_candidate_validation_uses_whole_old_config(tmp_path):
    reg = registry()
    old_candidate = runtime_candidate(reg, 100)
    new_candidate = runtime_candidate(reg, 200)
    manager = ConfigManager(old_candidate)
    candidate_loading = threading.Event()
    allow_candidate = threading.Event()
    reload_results = []

    def load_candidate():
        candidate_loading.set()
        assert allow_candidate.wait(timeout=5)
        return new_candidate

    reload_thread = threading.Thread(
        target=lambda: reload_results.append(manager.reload(load_candidate))
    )
    reload_thread.start()
    assert candidate_loading.wait(timeout=5)
    assert manager.active is old_candidate

    store = SQLiteEventStore(tmp_path / "config-race.db")
    run_snapshot = manager.start_run("race-run", store)
    assert run_snapshot.effective_config_hash == old_candidate.resolved.config.content_hash
    assert run_snapshot.registry_manifest_hash == old_candidate.registry.content_hash
    assert run_snapshot.resolved_config.field_sources["/presets/balanced/requested_budget/max_cost_minor"] == (
        ConfigSource.PROJECT,
    )

    allow_candidate.set()
    reload_thread.join(timeout=5)
    assert not reload_thread.is_alive()
    assert reload_results[0].applied
    assert manager.active is new_candidate
    next_snapshot = manager.start_run("after-reload", store)
    assert next_snapshot.effective_config_hash == new_candidate.resolved.config.content_hash
    store.close()


def test_concurrent_reloads_validate_one_candidate_at_a_time():
    reg = registry()
    candidates = [runtime_candidate(reg, cost) for cost in (100, 200, 300)]
    manager = ConfigManager(candidates[0])
    first_loading = threading.Event()
    release_first = threading.Event()
    second_called = threading.Event()
    second_attempting = threading.Event()
    lock = threading.Lock()
    active_loaders = 0
    max_loaders = 0
    completed = []

    def tracked_loader(candidate, *, wait=False):
        nonlocal active_loaders, max_loaders
        with lock:
            active_loaders += 1
            max_loaders = max(max_loaders, active_loaders)
        try:
            if wait:
                first_loading.set()
                assert release_first.wait(timeout=5)
            else:
                second_called.set()
            return candidate
        finally:
            with lock:
                active_loaders -= 1

    first = threading.Thread(
        target=lambda: completed.append(
            manager.reload(lambda: tracked_loader(candidates[1], wait=True))
        )
    )
    def run_second_reload():
        second_attempting.set()
        completed.append(manager.reload(lambda: tracked_loader(candidates[2])))

    second = threading.Thread(target=run_second_reload)
    first.start()
    assert first_loading.wait(timeout=5)
    second.start()
    # The second thread is known to be live before the first builder is released.
    assert second_attempting.wait(timeout=1)
    release_first.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive() and not second.is_alive()
    assert max_loaders == 1
    assert second_called.is_set()
    assert all(result.applied for result in completed)
    assert manager.generation == 3
    assert manager.active is candidates[2]


def test_restart_replays_original_config_registry_and_provenance(tmp_path):
    database = tmp_path / "run-snapshot.db"
    reg = registry()
    original = runtime_candidate(reg, 100)
    changed = runtime_candidate(reg, 300)
    store = SQLiteEventStore(database)
    manager = ConfigManager(original)
    expected = manager.start_run("persistent-run", store)
    store.close()

    restarted_store = SQLiteEventStore(database)
    restarted_manager = ConfigManager(changed)
    replayed = restarted_manager.restore_run("persistent-run", restarted_store)
    checkpoint = SnapshotStore(restarted_store).load_valid(
        "run_config",
        "persistent-run",
        expected_event_version=1,
        expected_source_event_id=restarted_store.read_stream("run", "persistent-run")[0].event_id,
    )

    assert replayed.effective_config_hash == expected.effective_config_hash
    assert replayed.registry_manifest_hash == expected.registry_manifest_hash
    assert replayed.resolved_config.field_sources == expected.resolved_config.field_sources
    assert replayed.source_details == expected.source_details
    assert replayed.effective_config_hash != changed.resolved.config.content_hash
    assert checkpoint is not None
    assert checkpoint.state["effective_config_hash"] == expected.effective_config_hash
    restarted_store.close()


def test_competing_run_starts_share_one_immutable_created_snapshot(tmp_path):
    database = tmp_path / "competing-run-starts.db"
    reg = registry()
    candidates = [runtime_candidate(reg, 100), runtime_candidate(reg, 300)]
    start_barrier = threading.Barrier(2)
    results = []
    errors = []

    def start(candidate):
        store = SQLiteEventStore(database)
        try:
            start_barrier.wait(timeout=5)
            results.append(ConfigManager(candidate).start_run("contended-run", store))
        except Exception as exc:  # pragma: no cover - asserted empty below
            errors.append(exc)
        finally:
            store.close()

    threads = [threading.Thread(target=start, args=(candidate,)) for candidate in candidates]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert len(results) == 2
    assert results[0].effective_config_hash == results[1].effective_config_hash
    assert results[0].effective_config_hash in {
        candidate.resolved.config.content_hash for candidate in candidates
    }
    verification_store = SQLiteEventStore(database)
    created_events = verification_store.read_stream("run", "contended-run")
    assert len(created_events) == 1
    assert created_events[0].event_type == "RunCreated"
    verification_store.close()


def test_failed_checkpoint_write_is_replayed_and_repaired_on_retry(tmp_path, monkeypatch):
    database = tmp_path / "run-checkpoint-retry.db"
    reg = registry()
    original = runtime_candidate(reg, 100)
    changed = runtime_candidate(reg, 300)
    manager = ConfigManager(original)
    store = SQLiteEventStore(database)

    def fail_checkpoint(*args, **kwargs):
        raise OSError("simulated checkpoint failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(SnapshotStore, "save_snapshot", fail_checkpoint)
        with pytest.raises(OSError, match="simulated checkpoint failure"):
            manager.start_run("retry-run", store)

    events = store.read_stream("run", "retry-run")
    assert len(events) == 1
    assert events[0].event_type == "RunCreated"
    manager.reload(lambda: changed)
    store.close()

    restarted_store = SQLiteEventStore(database)
    restarted_manager = ConfigManager(changed)
    retried = restarted_manager.start_run("retry-run", restarted_store)

    assert retried.effective_config_hash == original.resolved.config.content_hash
    assert retried.source_details == original.source_details
    checkpoint = SnapshotStore(restarted_store).load_valid(
        "run_config", "retry-run", expected_event_version=1
    )
    assert checkpoint is not None
    restarted_store.close()


def test_run_snapshot_recovery_rejects_missing_or_malformed_creation_event(tmp_path):
    store = SQLiteEventStore(tmp_path / "invalid-run-snapshot.db")
    manager = ConfigManager(runtime_candidate(registry(), 100))

    with pytest.raises(RunConfigSnapshotError, match="missing"):
        manager.restore_run("missing-run", store)

    store.append(
        "run",
        "malformed-run",
        0,
        [
            EventDraft(
                "RunCreated",
                {"run_id": "malformed-run", "config_snapshot": {}},
            )
        ],
        "create-malformed",
    )
    with pytest.raises(RunConfigSnapshotError, match="failed validation"):
        manager.restore_run("malformed-run", store)

    store.append("run", "wrong-first-event", 0, [EventDraft("RunChanged", {})], "wrong-first")
    with pytest.raises(RunConfigSnapshotError, match="does not begin with RunCreated"):
        manager.start_run("wrong-first-event", store)
    store.close()
