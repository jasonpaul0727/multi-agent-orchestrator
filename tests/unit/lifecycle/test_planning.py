from datetime import datetime, timedelta, timezone
import json

import pytest

from orchestrator.config import (
    ConfigCandidate,
    ConfigManager,
    EffectiveConfig,
    resolve_effective_config,
)
from orchestrator.config.models import ModelRegistryManifest, ModelSpec, PriceSpec, ProviderSpec
from orchestrator.lifecycle import (
    GraphPlanningService,
    LifecycleController,
    LifecycleError,
    NodeSpec,
    NodeProposal,
)
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.routing import PlanningError
from orchestrator.security import PolicyAuthority, PolicyManifest


NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
ROLES = (
    "planner", "coder", "document_analyst", "researcher", "tester", "reviewer", "director"
)


def _system(policy_tool: str = "model:model-economy"):
    provider = ProviderSpec(
        id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True
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
    registry = ModelRegistryManifest(providers=(provider,), models=(model,))
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
    config = EffectiveConfig.model_validate(
        {
            "schema_version": 1,
            "active_preset": "balanced",
            "registry_manifest_ref": registry.content_hash,
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
            "presets": {
                name: preset for name in ("economic", "balanced", "quality", "custom")
            },
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
    policy = PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions={"model_invoke"},
                allowed_tools={policy_tool},
            ),
        )
    )
    return ConfigCandidate(
        resolved=resolve_effective_config(config, registry=registry),
        registry=registry,
    ), policy


def _started_run(path, run_id="run-plan"):
    candidate, policy = _system()
    store = SQLiteEventStore(path)
    ConfigManager(candidate).start_run(run_id, store)
    lifecycle = LifecycleController(store)
    lifecycle.initialize_run(run_id)
    return store, lifecycle, policy


def test_planning_service_freezes_contracts_and_does_not_persist_raw_task_text(tmp_path):
    store, lifecycle, policy = _started_run(tmp_path / "plan.db")
    service = GraphPlanningService(lifecycle)
    raw_secret_marker = "task-text-must-not-be-persisted-6eead9"
    state = service.append_proposal(
        "run-plan",
        (
            NodeProposal(
                node_id="plan",
                role="planner",
                task_text=f"make a plan for {raw_secret_marker}",
                context_tokens=4_000,
                max_output_tokens=900,
            ),
            NodeProposal(
                node_id="implement",
                role="coder",
                task_text="implement a small change",
                depends_on=("plan",),
                tool_ids=("read_file",),
                required_capabilities=("text",),
                context_tokens=8_000,
            ),
        ),
        policy_manifest=policy,
        expected_graph_version=0,
        idempotency_key="initial-plan",
    )

    assert state.graph_version == 1
    assert state.node("plan").status == "ready"
    assert state.node("implement").status == "blocked"
    for node in state.nodes:
        contract = node.spec.planning_contract
        assert contract is not None
        assert contract.run_id == "run-plan"
        assert contract.config_hash == state.config_hash
        assert contract.registry_hash == state.registry_hash
        assert contract.contract_hash == node.spec.planning_contract_hash
    events = store.read_stream("run_lifecycle", "run-plan")
    graph_event = next(event for event in events if event.event_type == "GraphNodesAppended")
    serialized = json.dumps(graph_event.payload, sort_keys=True)
    assert raw_secret_marker not in serialized
    assert "input_hash" in serialized
    replayed = LifecycleController(store).replay("run-plan")
    assert replayed == state


def test_planning_contract_survives_store_reopen_and_graph_replay(tmp_path):
    path = tmp_path / "plan-reopen.db"
    store, lifecycle, policy = _started_run(path)
    frozen = GraphPlanningService(lifecycle).append_proposal(
        "run-plan",
        (NodeProposal(node_id="work", role="coder", task_text="implement the bug fix", context_tokens=1_000),),
        policy_manifest=policy,
        expected_graph_version=0,
        idempotency_key="initial-plan",
    )
    frozen_contract = frozen.node("work").spec.planning_contract
    store.close()

    reopened = SQLiteEventStore(path)
    restored = LifecycleController(reopened).replay("run-plan")
    assert restored.node("work").spec.planning_contract == frozen_contract
    assert restored.node("work").spec.planning_contract_hash == frozen_contract.contract_hash


def test_planning_append_is_idempotent_after_the_graph_version_advances(tmp_path):
    store, lifecycle, policy = _started_run(tmp_path / "planning-idempotency.db")
    service = GraphPlanningService(lifecycle)
    proposals = (
        NodeProposal(node_id="work", role="coder", task_text="implement a bug fix", context_tokens=10),
    )
    first = service.append_proposal(
        "run-plan", proposals, policy_manifest=policy,
        expected_graph_version=0, idempotency_key="initial-plan",
    )
    event_version = store.current_version("run_lifecycle", "run-plan")
    retried = service.append_proposal(
        "run-plan", proposals, policy_manifest=policy,
        expected_graph_version=0, idempotency_key="initial-plan",
    )

    assert retried == first
    assert store.current_version("run_lifecycle", "run-plan") == event_version


def test_planning_rejects_policy_drift_without_mutating_the_graph(tmp_path):
    store, lifecycle, policy = _started_run(tmp_path / "policy-drift.db")
    service = GraphPlanningService(lifecycle)
    service.append_proposal(
        "run-plan",
        (NodeProposal(node_id="one", role="coder", task_text="implement a fix", context_tokens=10),),
        policy_manifest=policy,
        expected_graph_version=0,
        idempotency_key="one",
    )
    changed_policy = _system("model:another-model")[1]
    before = lifecycle.replay("run-plan")
    with pytest.raises(PlanningError, match="run_policy_snapshot_mismatch"):
        service.append_proposal(
            "run-plan",
            (NodeProposal(node_id="two", role="coder", task_text="implement another fix", context_tokens=10),),
            policy_manifest=changed_policy,
            expected_graph_version=1,
            idempotency_key="two",
        )
    assert lifecycle.replay("run-plan") == before
    assert store.current_version("run_lifecycle", "run-plan") == before.event_version
    with pytest.raises(LifecycleError, match="PolicyManifest changed"):
        lifecycle.append_nodes(
            "run-plan",
            (before.node("one").spec,),
            expected_graph_version=1,
            idempotency_key="direct-policy-drift",
            policy_manifest=changed_policy,
        )


def test_planning_rejects_stale_version_empty_or_duplicate_proposals(tmp_path):
    _, lifecycle, policy = _started_run(tmp_path / "invalid-proposals.db")
    service = GraphPlanningService(lifecycle)
    with pytest.raises(PlanningError, match="empty_graph_proposal"):
        service.append_proposal(
            "run-plan", (), policy_manifest=policy, expected_graph_version=0, idempotency_key="empty"
        )
    duplicate = NodeProposal(node_id="same", role="coder", task_text="implement a fix", context_tokens=1)
    with pytest.raises(PlanningError, match="duplicate_proposed_node_id"):
        service.append_proposal(
            "run-plan",
            (duplicate, duplicate),
            policy_manifest=policy,
            expected_graph_version=0,
            idempotency_key="duplicate",
        )
    with pytest.raises(PlanningError, match="stale_graph_version"):
        service.append_proposal(
            "run-plan",
            (NodeProposal(node_id="one", role="coder", task_text="implement a fix", context_tokens=1),),
            policy_manifest=policy,
            expected_graph_version=1,
            idempotency_key="stale",
        )


def test_planning_rejects_invalid_node_contract_and_bad_dependencies(tmp_path):
    _, lifecycle, policy = _started_run(tmp_path / "bad-contract.db")
    service = GraphPlanningService(lifecycle)
    with pytest.raises(PlanningError, match="output_requirement_exceeds_role_limit"):
        service.append_proposal(
            "run-plan",
            (NodeProposal(
                node_id="too-large", role="coder", task_text="implement a fix",
                context_tokens=1, max_output_tokens=10_001,
            ),),
            policy_manifest=policy,
            expected_graph_version=0,
            idempotency_key="too-large",
        )
    with pytest.raises(LifecycleError, match="dependency references a node"):
        service.append_proposal(
            "run-plan",
            (NodeProposal(
                node_id="orphan", role="coder", task_text="implement a fix",
                context_tokens=1, depends_on=("missing",),
            ),),
            policy_manifest=policy,
            expected_graph_version=0,
            idempotency_key="orphan",
        )


def test_proposal_schema_rejects_duplicate_tools_and_blank_text():
    with pytest.raises(ValueError):
        NodeProposal(
            node_id="work", role="coder", task_text=" ", context_tokens=0, tool_ids=("read", "read")
        )
    with pytest.raises(ValueError):
        NodeProposal(
            node_id="work", role="coder", task_text="implement a fix", context_tokens=0,
            tool_ids=("read", "read"),
        )


def test_proposal_schema_rejects_malformed_sequences_and_parent_ids():
    with pytest.raises(ValueError, match="depends_on must be an array"):
        NodeProposal(
            node_id="work", role="coder", task_text="implement a fix", context_tokens=0,
            depends_on="not-an-array",
        )
    with pytest.raises(ValueError, match="stable identifier"):
        NodeProposal(
            node_id="work", role="coder", task_text="implement a fix", context_tokens=0,
            parent_agent_instance_id="invalid parent",
        )
    with pytest.raises(ValueError, match="unique identifiers"):
        NodeProposal(
            node_id="work", role="coder", task_text="implement a fix", context_tokens=0,
            required_capabilities=("text", "invalid value"),
        )


def test_service_and_lifecycle_reject_untrusted_runtime_values(tmp_path):
    _, lifecycle, policy = _started_run(tmp_path / "invalid-runtime-values.db")
    service = GraphPlanningService(lifecycle)
    proposal = NodeProposal(
        node_id="work", role="coder", task_text="implement a fix", context_tokens=10
    )
    with pytest.raises(TypeError, match="validated PolicyManifest"):
        service.append_proposal(
            "run-plan", (proposal,), policy_manifest=object(),
            expected_graph_version=0, idempotency_key="bad-policy",
        )
    with pytest.raises(TypeError, match="validated NodeProposal"):
        service.append_proposal(
            "run-plan", (object(),), policy_manifest=policy,
            expected_graph_version=0, idempotency_key="bad-proposal",
        )
    with pytest.raises(TypeError, match="validated PolicyManifest"):
        lifecycle.append_nodes(
            "run-plan",
            (NodeSpec(node_id="work", role="coder", planning_contract_hash="sha256:" + "e" * 64),),
            expected_graph_version=0,
            idempotency_key="bad-lifecycle-policy", policy_manifest=object(),
        )


def test_replay_rejects_a_persisted_invalid_policy_snapshot(tmp_path):
    store, lifecycle, _ = _started_run(tmp_path / "invalid-policy-replay.db")
    store.append(
        "run_lifecycle",
        "run-plan",
        1,
        (
            EventDraft(
                "GraphNodesAppended",
                {
                    "graph_version": 1,
                    "nodes": [],
                    "policy_manifest": {"unknown": "field"},
                    "policy_manifest_hash": "sha256:" + "0" * 64,
                },
            ),
        ),
        "corrupt-policy-snapshot",
    )
    with pytest.raises(LifecycleError, match="PolicyManifest failed validation"):
        lifecycle.replay("run-plan")


def test_graph_append_cannot_drop_frozen_policy_or_contract(tmp_path):
    _, lifecycle, policy = _started_run(tmp_path / "cannot-drop-policy.db")
    planned = GraphPlanningService(lifecycle).append_proposal(
        "run-plan",
        (NodeProposal(node_id="one", role="coder", task_text="implement a fix", context_tokens=10),),
        policy_manifest=policy,
        expected_graph_version=0,
        idempotency_key="initial-plan",
    )
    with pytest.raises(LifecycleError, match="requires its frozen PolicyManifest"):
        lifecycle.append_nodes(
            "run-plan",
            (planned.node("one").spec,),
            expected_graph_version=1,
            idempotency_key="drop-policy",
        )


def test_planning_refuses_legacy_nodes_without_frozen_contracts(tmp_path):
    _, lifecycle, policy = _started_run(tmp_path / "legacy-node.db")
    lifecycle.append_nodes(
        "run-plan",
        (NodeSpec(node_id="legacy", role="coder", planning_contract_hash="sha256:" + "e" * 64),),
        expected_graph_version=0,
        idempotency_key="legacy-node",
    )
    with pytest.raises(PlanningError, match="existing_graph_contract_unavailable"):
        GraphPlanningService(lifecycle).append_proposal(
            "run-plan",
            (NodeProposal(node_id="new", role="coder", task_text="implement a fix", context_tokens=10),),
            policy_manifest=policy,
            expected_graph_version=1,
            idempotency_key="new-node",
        )


def test_replay_rejects_policy_hash_tampering_and_policy_replacement(tmp_path):
    store, lifecycle, first_policy = _started_run(tmp_path / "policy-tamper.db")
    second_policy = _system("model:another-model")[1]
    first_node = NodeSpec(
        node_id="first", role="coder", planning_contract_hash="sha256:" + "a" * 64
    )
    second_node = NodeSpec(
        node_id="second", role="coder", planning_contract_hash="sha256:" + "b" * 64
    )
    events = (
        EventDraft(
            "GraphNodesAppended",
            {
                "graph_version": 1,
                "nodes": [first_node.model_dump(mode="json")],
                "policy_manifest": first_policy.model_dump(mode="json"),
                "policy_manifest_hash": first_policy.content_hash,
            },
        ),
        EventDraft(
            "GraphNodesAppended",
            {
                "graph_version": 2,
                "nodes": [second_node.model_dump(mode="json")],
                "policy_manifest": second_policy.model_dump(mode="json"),
                "policy_manifest_hash": second_policy.content_hash,
            },
        ),
    )
    store.append("run_lifecycle", "run-plan", 1, events, "forged-policy-replacement")
    with pytest.raises(LifecycleError, match="changed after Run planning"):
        lifecycle.replay("run-plan")
