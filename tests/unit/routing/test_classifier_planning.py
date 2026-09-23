from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.config.effective import EffectiveConfig, GuardRule
from orchestrator.config.models import (
    ModelRegistryManifest,
    ModelSpec,
    PriceSpec,
    ProviderSpec,
)
from orchestrator.routing import PlanningError, TaskClassifier, compile_node_contract
from orchestrator.security import PolicyAuthority, PolicyManifest


NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
ROLES = (
    "planner",
    "coder",
    "document_analyst",
    "researcher",
    "tester",
    "reviewer",
    "director",
)


def registry():
    provider = ProviderSpec(
        id="primary",
        adapter="openai_responses",
        secret_ref="env:MODEL_KEY",
        enabled=True,
    )
    price = PriceSpec(
        currency="USD",
        input_minor_per_million=100,
        output_minor_per_million=100,
        max_tool_cost_minor=0,
        estimator_id="tokens.v1",
        effective_from=NOW - timedelta(days=1),
        expires_at=NOW + timedelta(days=5),
    )
    model = ModelSpec(
        id="model-economy",
        provider="primary",
        remote_model="remote-economy",
        tier="economy",
        capabilities={"text"},
        context_window=128_000,
        max_output_tokens=2_000,
        supported_reasoning_efforts={"none", "low"},
        price=price,
    )
    return ModelRegistryManifest(providers=(provider,), models=(model,))


def policy_manifest(model_ids=("model-economy",)):
    return PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions={"model_invoke"},
                allowed_tools={f"model:{model_id}" for model_id in model_ids},
            ),
        ),
    )


def config(reg):
    profile = {
        "candidates": ["model-economy"],
        "reasoning_effort": "none",
        "max_output_tokens": 1_000,
        "retries": 1,
        "escalate_to": None,
    }
    preset = {
        "requested_budget": {
            "max_cost_minor": 2_000,
            "max_total_tokens": 20_000,
            "max_agents": 20,
            "max_depth": 4,
            "max_concurrency": 4,
            "max_parallel_candidates": 2,
        },
        "roles": {role: dict(profile) for role in ROLES},
        "selector_rules": [],
        "guard_rules": [],
        "health_policy_ref": "health-default",
    }
    return EffectiveConfig.model_validate(
        {
            "schema_version": 1,
            "active_preset": "balanced",
            "registry_manifest_ref": reg.content_hash,
            "policy_envelope": {
                "currency": "USD",
                "max_cost_minor": 3_000,
                "max_total_tokens": 30_000,
                "max_agents": 30,
                "max_depth": 5,
                "max_concurrency": 6,
                "allowed_models": ["model-economy"],
                "allowed_providers": ["primary"],
                "allowed_capabilities": ["text"],
                "denied_models": [],
                "denied_providers": [],
                "min_tier": "economy",
                "require_independent_review": False,
                "require_approval": False,
                "max_parallel_candidates": 3,
            },
            "mandatory_guard_rules": [],
            "presets": {name: preset for name in ("economic", "balanced", "quality", "custom")},
            "classifier": {
                "id": "orchestrator.deterministic.v1",
                "version": "1.0.0",
                "normalization_version": "unicode-nfkc-v1",
                "taxonomy_version": "maestro-task-taxonomy-v1",
            },
            "health_policies": {
                "health-default": {
                    "id": "health-default",
                    "failure_window_ms": 60_000,
                    "degrade_after": 2,
                    "open_after": 4,
                    "recovery_successes": 2,
                    "cooldown_ms": 10_000,
                    "max_probe_permits": 1,
                }
            },
        }
    )


def test_classifier_is_local_deterministic_and_unknown_fails_closed():
    reg = registry()
    specification = config(reg).classifier
    classifier = TaskClassifier(specification)
    first = classifier.classify("Implement a small change")
    second = classifier.classify("Ｉｍｐｌｅｍｅｎｔ a small change")
    unknown = classifier.classify("transfigure the flarm")
    ambiguous = classifier.classify("implement the fix and research sources")

    assert first.input_hash == second.input_hash
    assert first.task_class == "code_change"
    assert first.complexity == "low"
    assert first.risk == "low"
    assert unknown.task_class == ambiguous.task_class == "unknown"
    assert unknown.risk == "high"
    assert unknown.complexity == "unknown"
    assert "transfigure" not in str(unknown.model_dump())


def test_planning_freezes_baseline_guard_and_requested_budget_caps():
    reg = registry()
    effective = config(reg)
    security = policy_manifest()
    contract = compile_node_contract(
        run_id="run-1",
        node_id="node-1",
        role="coder",
        task_text="implement the security fix",
        config=effective,
        registry=reg,
        policy_manifest=security,
        context_tokens=4_000,
        max_output_tokens=700,
        required_capabilities=("text",),
    )

    assert contract.classification.task_class == "code_change"
    assert contract.classification.risk == "high"
    assert contract.policy_envelope.min_tier == "standard"
    assert contract.require_independent_review is True
    assert contract.policy_envelope.max_cost_minor == 2_000
    assert "system:classification-risk-floor" in contract.matched_guard_rule_ids
    assert contract.contract_hash.startswith("sha256:")


def test_guard_rules_only_tighten_and_incompatible_planning_fails_closed():
    reg = registry()
    effective = config(reg)
    presets = dict(effective.presets)
    presets["balanced"] = effective.presets["balanced"].model_copy(
        update={
            "guard_rules": (
                GuardRule(
                    id="small-budget",
                    when={"role": ["coder"]},
                    constraints={"max_cost_minor": 10},
                ),
            )
        }
    )
    tighter = effective.model_copy(update={"presets": presets})
    security = policy_manifest()
    contract = compile_node_contract(
        run_id="run-1",
        node_id="node-1",
        role="coder",
        task_text="implement a small change",
        config=tighter,
        registry=reg,
        policy_manifest=security,
        context_tokens=10,
        max_output_tokens=100,
    )
    assert contract.policy_envelope.max_cost_minor == 10
    with pytest.raises(PlanningError, match="registry_snapshot_mismatch"):
        compile_node_contract(
            run_id="run-1",
            node_id="node-1",
            role="coder",
            task_text="implement a small change",
            config=effective,
            registry=reg.model_copy(update={"models": ()}),
            policy_manifest=security,
            context_tokens=10,
        )


def test_planning_rejects_role_output_overflow_and_disallowed_classifier_version():
    reg = registry()
    effective = config(reg)
    with pytest.raises(PlanningError, match="output_requirement_exceeds_role_limit"):
        compile_node_contract(
            run_id="run-1",
            node_id="node-1",
            role="coder",
            task_text="implement a small change",
            config=effective,
            registry=reg,
            policy_manifest=policy_manifest(),
            context_tokens=10,
            max_output_tokens=1_001,
        )
    classifier = effective.classifier.model_copy(update={"normalization_version": "unknown-v1"})
    with pytest.raises(ValueError, match="unsupported classifier normalization"):
        TaskClassifier(classifier)
