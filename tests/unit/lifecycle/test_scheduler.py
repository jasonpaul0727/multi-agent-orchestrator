from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
import multiprocessing
import os
from threading import Barrier

import pytest

from orchestrator.artifacts import ArtifactRecord, ArtifactStore
from orchestrator.budget import BudgetExhausted, BudgetLedger, CostEstimate, UsageRecord
from orchestrator.agents import (
    AgentConcurrencyLimitExceeded,
    AgentDepthLimitExceeded,
    AgentInstance,
    AgentLimitExceeded,
    AgentRegistry,
    AgentRegistryError,
    AgentRegistryLimits,
    reduce_agent_registry,
)
from orchestrator.config import (
    ConfigCandidate,
    ConfigManager,
    EffectiveConfig,
    ModelRegistryManifest,
    ModelSpec,
    ProviderSpec,
    resolve_effective_config,
)
from orchestrator.lifecycle import (
    GraphError,
    LifecycleConflict,
    LifecycleController,
    LifecycleError,
    NodeSpec,
    validate_graph_append,
)
from orchestrator.lifecycle.models import AttemptState, NodeState, RunLifecycleState
from orchestrator.models import ModelGatewayFailure
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.routing import (
    CandidateAssessment,
    RecoveryAuthorization,
    RoutingDecision,
    RoutingRequest,
    compile_node_contract,
)
from orchestrator.recovery import RunRecoveryCoordinator, RunRecoveryError
from orchestrator.scheduler import (
    ConcurrencyLimitExceeded,
    ConcurrencyLimits,
    Scheduler,
    SchedulerError,
    StaleRoutingDecision,
)
from orchestrator.scheduler.core import _verify_persisted_recovery_authorization
from orchestrator.security import PolicyAuthority, PolicyEngine, PolicyManifest, PolicyRequest


NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
HASH = "sha256:" + "e" * 64
ROLES = ("planner", "coder", "document_analyst", "researcher", "tester", "reviewer", "director")


def registry():
    provider = ProviderSpec(
        id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True
    )
    model = ModelSpec(
        id="model-1",
        provider="primary",
        remote_model="remote-model-1",
        tier="economy",
        capabilities={"text", "tools"},
        context_window=32_000,
        max_output_tokens=2_000,
        supported_reasoning_efforts={"none", "low"},
        local_zero_cost=True,
    )
    return ModelRegistryManifest(providers=(provider,), models=(model,))


def effective_config(reg, *, max_agents=8, max_depth=4, max_concurrency=4):
    role = {
        "candidates": ["model-1"],
        "reasoning_effort": "none",
        "max_output_tokens": 1_000,
        "retries": 2,
        "escalate_to": None,
    }
    preset = {
        "requested_budget": {
            "max_cost_minor": 100,
            "max_total_tokens": 100,
            "max_agents": max_agents,
            "max_depth": max_depth,
            "max_concurrency": max_concurrency,
            "max_parallel_candidates": 2,
        },
        "roles": {item: dict(role) for item in ROLES},
        "selector_rules": [],
        "guard_rules": [],
        "health_policy_ref": "health-default",
    }
    config = EffectiveConfig.model_validate(
        {
            "schema_version": 1,
            "active_preset": "balanced",
            "registry_manifest_ref": reg.content_hash,
            "policy_envelope": {
                "currency": "USD",
                "max_cost_minor": 100,
                "max_total_tokens": 100,
                "max_agents": max_agents,
                "max_depth": max_depth,
                "max_concurrency": max_concurrency,
                "allowed_models": ["model-1"],
                "allowed_providers": ["primary"],
                "allowed_capabilities": ["text", "tools"],
                "denied_models": [],
                "denied_providers": [],
                "min_tier": "economy",
                "require_independent_review": False,
                "require_approval": False,
                "max_parallel_candidates": 2,
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
    return resolve_effective_config(config, registry=reg)


def run_setup(
    store, *, run_id="run-1", nodes=None, max_agents=8, max_depth=4, max_concurrency=4
):
    reg = registry()
    resolved = effective_config(
        reg, max_agents=max_agents, max_depth=max_depth, max_concurrency=max_concurrency
    )
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run(run_id, store)
    controller = LifecycleController(store)
    controller.initialize_run(run_id)
    if nodes is None:
        nodes = (NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH, max_attempts=2),)
    if nodes:
        controller.append_nodes(
            run_id,
            tuple(nodes),
            expected_graph_version=0,
            idempotency_key="initial-graph",
        )
        controller.start_run(run_id)
    manifest = PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions={"model_invoke"},
                allowed_tools={"model:model-1"},
            ),
        )
    )
    return reg, resolved.config, controller, manifest


def run_setup_with_frozen_contract(store, *, max_attempts=3):
    reg = registry()
    resolved = effective_config(reg)
    manifest = PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions={"model_invoke"},
                allowed_tools={"model:model-1"},
            ),
        )
    )
    contract = compile_node_contract(
        run_id="run-1",
        node_id="node-1",
        role="coder",
        task_text="implement a small change",
        config=resolved.config,
        registry=reg,
        policy_manifest=manifest,
        context_tokens=1_000,
        max_output_tokens=500,
        required_capabilities=("text",),
    )
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run("run-1", store)
    lifecycle = LifecycleController(store)
    lifecycle.initialize_run("run-1")
    lifecycle.append_nodes(
        "run-1",
        (NodeSpec(
            node_id="node-1",
            role="coder",
            planning_contract_hash=contract.contract_hash,
            planning_contract=contract,
            max_attempts=max_attempts,
        ),),
        expected_graph_version=0,
        idempotency_key="initial-graph",
        policy_manifest=manifest,
    )
    lifecycle.start_run("run-1")
    return reg, resolved.config, lifecycle, manifest, contract


def routed_pair(
    reg,
    config,
    manifest,
    *,
    run_id="run-1",
    node_id="node-1",
    role="coder",
    attempt=1,
    contract_hash=HASH,
    recovery_action="initial",
    recovery_authorization=None,
    prior_decision_hash=None,
    previous_model_id=None,
    failure_category=None,
    retry_level=0,
    exhausted_model_ids=(),
):
    request = RoutingRequest(
        request_id=f"request-{node_id}-{attempt}",
        run_id=run_id,
        node_id=node_id,
        attempt_id=f"attempt-{node_id}-{attempt}",
        fencing_generation=attempt,
        config_hash=config.content_hash,
        registry_hash=reg.content_hash,
        planning_contract_hash=contract_hash,
        policy_manifest_hash=manifest.content_hash,
        routing_as_of_event_time=NOW,
        prior_decision_hash=prior_decision_hash,
        previous_model_id=previous_model_id,
        recovery_action=recovery_action,
        recovery_authorization=recovery_authorization,
        failure_category=failure_category,
        retry_level=retry_level,
        exhausted_model_ids=exhausted_model_ids,
    )
    normalized_policy_hash = "sha256:" + hashlib.sha256(
        json.dumps(
            {
                "action": "model_invoke",
                "attempt_id": request.attempt_id,
                "fencing_generation": attempt,
                "model_id": "model-1",
                "provider_id": "primary",
                "request_hash": request.request_hash,
                "run_id": run_id,
                "node_id": node_id,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    policy_request = PolicyRequest(
        request_id=request.request_id,
        run_id=run_id,
        node_id=node_id,
        attempt_id=request.attempt_id,
        fencing_generation=attempt,
        role=role,
        action_category="model_invoke",
        tool_id="model:model-1",
        required_permission="read-only",
        normalized_request_hash=normalized_policy_hash,
        policy_manifest_hash=manifest.content_hash,
        revocation_version=0,
        emergency_deny_version=0,
    )
    policy_decision = PolicyEngine().evaluate(policy_request, manifest)
    cost = CostEstimate(
        amount_minor=20,
        currency="USD",
        token_limit=10,
        input_tokens=4,
        output_tokens=6,
        snapshot_id=HASH,
        fx_snapshot_id=HASH,
        tokenizer_snapshot_id=HASH,
        price_snapshot_id=HASH,
        estimator_snapshot_id=HASH,
    )
    decision = RoutingDecision(
        request_id=request.request_id,
        request_hash=request.request_hash,
        run_id=run_id,
        node_id=node_id,
        attempt_id=request.attempt_id,
        fencing_generation=attempt,
        config_hash=config.content_hash,
        registry_hash=reg.content_hash,
        planning_contract_hash=contract_hash,
        policy_manifest_hash=manifest.content_hash,
        eligibility_snapshot_id=HASH,
        matched_selector_rule_id=None,
        candidate_assessments=(
            CandidateAssessment(
                model_id="model-1",
                provider_id="primary",
                configured_rank=0,
                eligible=True,
                estimated_cost=cost,
                health_state="healthy",
                policy_decision=policy_decision,
                exclusion_reasons=(),
            ),
        ),
        eligible_order=("model-1",),
        reasoning_effort="low",
        outcome="selected",
        selected_model_id="model-1",
        selected_provider_id="primary",
        blocked_reason=None,
    )
    return request, decision


def scheduler(store, *, system=8, run=8, provider=8, tool=8, artifact_store=None):
    return Scheduler(
        store,
        limits=ConcurrencyLimits(
            system_active_attempts=system,
            run_active_attempts=run,
            provider_active_attempts=provider,
            tool_active_attempts=tool,
        ),
        artifact_store=artifact_store,
    )


def accept(scheduler_, request, decision):
    return scheduler_.accept_routing(
        request,
        decision,
        accepted_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(minutes=1),
    )


def test_graph_validation_enforces_append_only_dependencies_count_and_depth():
    root = NodeSpec(node_id="root", role="planner", planning_contract_hash=HASH)
    child = NodeSpec(
        node_id="child", role="coder", planning_contract_hash=HASH, depends_on=("root",)
    )
    validated = validate_graph_append((), (child, root), max_nodes=2, max_depth=1)
    assert tuple(item.node_id for item in validated) == ("child", "root")
    with pytest.raises(GraphError, match="does not exist"):
        validate_graph_append((), (child,), max_nodes=5, max_depth=3)
    with pytest.raises(GraphError, match="acyclic"):
        validate_graph_append(
            (),
            (
                NodeSpec(node_id="a", role="coder", planning_contract_hash=HASH, depends_on=("b",)),
                NodeSpec(node_id="b", role="coder", planning_contract_hash=HASH, depends_on=("a",)),
            ),
            max_nodes=5,
            max_depth=3,
        )


def test_run_lifecycle_rejects_invalid_transitions_and_freezes_graph_versions(tmp_path):
    store = SQLiteEventStore(tmp_path / "lifecycle.db")
    _, _, lifecycle, _ = run_setup(store, nodes=())

    # The helper intentionally has no graph only if we append one explicitly;
    # initialization alone cannot start an empty Run.
    controller = LifecycleController(store)
    with pytest.raises(LifecycleError, match="initial graph"):
        controller.start_run("run-1")
    root = NodeSpec(node_id="root", role="planner", planning_contract_hash=HASH)
    state = controller.append_nodes(
        "run-1", (root,), expected_graph_version=0, idempotency_key="graph-root"
    )
    assert state.graph_version == 1
    assert controller.append_nodes(
        "run-1", (root,), expected_graph_version=0, idempotency_key="graph-root"
    ).graph_version == 1
    with pytest.raises(LifecycleConflict, match="graph version"):
        controller.append_nodes(
            "run-1",
            (NodeSpec(node_id="other", role="coder", planning_contract_hash=HASH),),
            expected_graph_version=0,
            idempotency_key="graph-stale",
        )
    controller.start_run("run-1")
    assert controller.pause_run("run-1", reason_code="user_request").status == "paused"
    assert controller.resume_run("run-1").status == "running"


def test_lifecycle_replays_event_tail_from_checkpoint_after_checkpoint_write_failure(
    tmp_path, monkeypatch
):
    store = SQLiteEventStore(tmp_path / "lifecycle-tail-replay.db")
    _, _, lifecycle, _ = run_setup(store)
    checkpoint_before = lifecycle.snapshots.load_valid("run_lifecycle", "run-1")
    assert checkpoint_before is not None

    def fail_checkpoint(*args, **kwargs):
        raise OSError("simulated checkpoint interruption")

    monkeypatch.setattr(lifecycle.snapshots, "save_snapshot", fail_checkpoint)
    with pytest.raises(OSError, match="checkpoint interruption"):
        lifecycle.pause_run("run-1", reason_code="user_request")
    monkeypatch.undo()

    applied = []
    from orchestrator.lifecycle import controller as lifecycle_module

    original_apply = lifecycle_module.apply_lifecycle_event

    def count_tail_events(run_id, state, event):
        applied.append(event.event_type)
        return original_apply(run_id, state, event)

    monkeypatch.setattr(lifecycle_module, "apply_lifecycle_event", count_tail_events)
    state = lifecycle.replay("run-1")
    assert state.status == "paused"
    assert applied == ["RunPaused"]
    latest_checkpoint = lifecycle.snapshots.load_valid("run_lifecycle", "run-1")
    assert latest_checkpoint is not None
    assert latest_checkpoint.event_version == checkpoint_before.event_version


def test_lifecycle_replay_discards_invalid_checkpoint_and_rebuilds_full_stream(tmp_path):
    store = SQLiteEventStore(tmp_path / "lifecycle-invalid-snapshot.db")
    _, _, lifecycle, _ = run_setup(store)
    expected = lifecycle.replay("run-1")
    store._connection.execute(
        "UPDATE snapshots SET source_event_id = ? WHERE aggregate_type = ? AND aggregate_id = ?",
        ("not-the-anchored-event", "run_lifecycle", "run-1"),
    )

    recovered = lifecycle.replay("run-1")

    assert recovered == expected
    assert lifecycle.snapshots.load_valid("run_lifecycle", "run-1") is None
    assert lifecycle.checkpoint("run-1") == expected
    assert lifecycle.snapshots.load_valid("run_lifecycle", "run-1") is not None


def test_lifecycle_checkpoint_is_used_after_database_reopen(tmp_path, monkeypatch):
    path = tmp_path / "lifecycle-restart.db"
    store = SQLiteEventStore(path)
    _, _, lifecycle, _ = run_setup(store)
    expected = lifecycle.pause_run("run-1", reason_code="user_request")
    store.close()

    reopened = SQLiteEventStore(path)
    recovered = LifecycleController(reopened)
    applied = []
    from orchestrator.lifecycle import controller as lifecycle_module

    original_apply = lifecycle_module.apply_lifecycle_event

    def count_replayed_events(run_id, state, event):
        applied.append(event.event_type)
        return original_apply(run_id, state, event)

    monkeypatch.setattr(lifecycle_module, "apply_lifecycle_event", count_replayed_events)
    assert recovered.replay("run-1") == expected
    assert applied == []
    reopened.close()


def test_awaiting_user_freezes_scheduling_until_hashed_response(tmp_path):
    store = SQLiteEventStore(tmp_path / "awaiting-user.db")
    reg, config, lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)

    waiting = lifecycle.await_user(
        "run-1", request_id="input-1", reason_code="clarify_scope", context_hash=HASH
    )
    assert lifecycle.await_user(
        "run-1", request_id="input-1", reason_code="clarify_scope", context_hash=HASH
    ) == waiting
    assert waiting.status == "awaiting_user"
    assert waiting.awaiting_user_request_id == "input-1"
    with pytest.raises(LifecycleError, match="running Run"):
        accept(control, request, decision)
    with pytest.raises(LifecycleConflict, match="outstanding request"):
        lifecycle.record_user_response("run-1", request_id="other-input", response_hash=HASH)

    resumed = lifecycle.record_user_response(
        "run-1", request_id="input-1", response_hash=HASH
    )
    assert resumed.status == "running"
    assert resumed.awaiting_user_request_id is None
    assert lifecycle.record_user_response(
        "run-1", request_id="input-1", response_hash=HASH
    ) == resumed


def test_run_user_and_cancel_commands_validate_stable_identifiers_and_hashes(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-command-validation.db")
    _, _, lifecycle, _ = run_setup(store)

    with pytest.raises(ValueError, match="stable identifier"):
        lifecycle.await_user(
            "run-1", request_id="bad id", reason_code="clarify_scope", context_hash=HASH
        )
    with pytest.raises(ValueError, match="lowercase code"):
        lifecycle.await_user(
            "run-1", request_id="input-1", reason_code="Bad-Reason", context_hash=HASH
        )
    with pytest.raises(ValueError, match="context_hash"):
        lifecycle.await_user(
            "run-1", request_id="input-1", reason_code="clarify_scope", context_hash="not-hash"
        )
    with pytest.raises(ValueError, match="stable identifier"):
        lifecycle.record_user_response("run-1", request_id="bad id", response_hash=HASH)
    with pytest.raises(ValueError, match="response_hash"):
        lifecycle.record_user_response("run-1", request_id="input-1", response_hash="not-hash")
    with pytest.raises(ValueError, match="lowercase code"):
        lifecycle.request_cancel("run-1", reason_code="Bad-Reason")
    with pytest.raises(ValueError, match="stop_receipt_hash"):
        lifecycle.record_attempt_cancelled(
            "run-1", node_id="node-1", attempt_id="attempt-1",
            fencing_generation=1, stop_receipt_hash="not-hash", causation_id=HASH,
        )
    with pytest.raises(LifecycleError, match="cancelling Run"):
        lifecycle.record_attempt_cancelled(
            "run-1", node_id="node-1", attempt_id="attempt-1",
            fencing_generation=1, stop_receipt_hash=HASH, causation_id=HASH,
        )


def test_run_cancel_waits_for_stop_ack_and_cancels_ready_nodes(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-cancel.db")
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH),
        NodeSpec(node_id="node-2", role="tester", planning_contract_hash=HASH),
    )
    reg, config, lifecycle, manifest = run_setup(store, nodes=nodes)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, node_id="node-1")
    accepted = accept(control, request, decision)

    cancelling = lifecycle.request_cancel("run-1", reason_code="user_request")
    assert lifecycle.request_cancel("run-1", reason_code="user_request") == cancelling
    assert cancelling.status == "cancelling"
    assert cancelling.node("node-1").status == "running"
    assert cancelling.node("node-2").status == "ready"
    with pytest.raises(LifecycleError, match="running Run"):
        next_request, next_decision = routed_pair(
            reg, config, manifest, node_id="node-2", role="tester"
        )
        accept(control, next_request, next_decision)
    with pytest.raises(SchedulerError, match="requires usage or an explicit no-effect"):
        control.acknowledge_cancellation(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, stopped_at=NOW + timedelta(seconds=5),
            stop_receipt_hash=HASH,
        )
    with pytest.raises(SchedulerError, match="no-effect receipt hash"):
        control.acknowledge_cancellation(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, stopped_at=NOW + timedelta(seconds=5),
            stop_receipt_hash=HASH, no_effect_receipt_hash="invalid-proof",
        )

    control.acknowledge_cancellation(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, stopped_at=NOW + timedelta(seconds=5),
        stop_receipt_hash=HASH, no_effect_receipt_hash=HASH,
    )
    control.acknowledge_cancellation(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, stopped_at=NOW + timedelta(seconds=5),
        stop_receipt_hash=HASH, no_effect_receipt_hash=HASH,
    )
    cancelled = lifecycle.replay("run-1")
    assert cancelled.status == "cancelled"
    assert cancelled.node("node-1").status == "cancelled"
    assert cancelled.node("node-1").attempts[-1].status == "cancelled"
    assert cancelled.node("node-2").status == "cancelled"
    agent = control.agents.replay("run-1").agent(accepted.agent_instance_id)
    assert agent.status == "cancelled"
    assert control._ledger_for_run("run-1").get_reservation(
        accepted.reservation.reservation_id, run_id="run-1"
    ).status == "released"


def test_cancelled_attempt_with_provider_usage_still_settles_cost(tmp_path):
    store = SQLiteEventStore(tmp_path / "cancel-with-usage.db")
    reg, config, lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, request, decision)
    lifecycle.request_cancel("run-1", reason_code="user_request")
    usage = UsageRecord(
        reservation_id=accepted.reservation.reservation_id,
        run_id="run-1",
        settlement_key="cancelled-provider-usage",
        currency="USD",
        input_tokens=3,
        output_tokens=2,
        cost_minor=7,
    )

    control.acknowledge_cancellation(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, stopped_at=NOW + timedelta(seconds=5),
        stop_receipt_hash=HASH, usage=usage,
    )

    reservation = control._ledger_for_run("run-1").get_reservation(
        accepted.reservation.reservation_id, run_id="run-1"
    )
    assert reservation.status == "committed"
    assert control._ledger_for_run("run-1").available("run-1").used_minor == 7
    assert lifecycle.replay("run-1").status == "cancelled"


def test_cancellation_cannot_relabel_unknown_attempt_as_stopped(tmp_path):
    store = SQLiteEventStore(tmp_path / "cancel-unknown.db")
    _, _, lifecycle, _ = run_setup(store)
    control = scheduler(store)
    reg = registry()
    config = effective_config(reg)
    manifest = PolicyManifest(
        authorities=(PolicyAuthority(
            source="system", max_permission="read-only",
            allowed_actions={"model_invoke"}, allowed_tools={"model:model-1"},
        ),)
    )
    request, decision = routed_pair(reg, config, manifest)
    accept(control, request, decision)
    lifecycle.request_cancel("run-1", reason_code="user_request")
    assert control.mark_expired_attempts_unknown(as_of=NOW + timedelta(minutes=2)) == (
        request.attempt_id,
    )
    with pytest.raises(LifecycleConflict, match="reconciled, not cancelled"):
        control.acknowledge_cancellation(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, stopped_at=NOW + timedelta(minutes=3),
            stop_receipt_hash=HASH, no_effect_receipt_hash=HASH,
        )
    control.reconcile_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, reconciled_at=NOW + timedelta(minutes=4),
        outcome="failed", known_no_effect=True,
    )
    assert lifecycle.replay("run-1").status == "cancelled"


def test_cancellation_races_route_acceptance_without_post_cancel_admission(tmp_path):
    path = tmp_path / "cancel-admission-race.db"
    seed = SQLiteEventStore(path)
    reg, config, _, manifest = run_setup(seed)
    pair = routed_pair(reg, config, manifest)
    seed.close()
    barrier = Barrier(2)

    def accept_route():
        connection = SQLiteEventStore(path)
        try:
            barrier.wait()
            try:
                accept(scheduler(connection), *pair)
                return "accepted"
            except LifecycleError:
                return "rejected"
        finally:
            connection.close()

    def cancel_run():
        connection = SQLiteEventStore(path)
        try:
            barrier.wait()
            return LifecycleController(connection).request_cancel(
                "run-1", reason_code="race_cancel"
            ).status
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        route_result = pool.submit(accept_route)
        cancel_result = pool.submit(cancel_run)
        route_status = route_result.result()
        cancel_status = cancel_result.result()

    check = SQLiteEventStore(path)
    final_state = LifecycleController(check).replay("run-1")
    assert cancel_status in {"cancelling", "cancelled"}
    assert final_state.status in {"cancelling", "cancelled"}
    if final_state.status == "cancelling":
        assert route_status == "accepted"
        assert final_state.node("node-1").status == "running"
    else:
        assert route_status == "rejected"
        assert final_state.node("node-1").status == "cancelled"


def test_route_acceptance_atomically_reserves_budget_slots_and_fences_attempt(tmp_path):
    store = SQLiteEventStore(tmp_path / "scheduler.db")
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH, max_attempts=2),
        NodeSpec(
            node_id="node-2", role="tester", planning_contract_hash=HASH, depends_on=("node-1",)
        ),
    )
    reg, config, lifecycle, manifest = run_setup(store, nodes=nodes)
    request, decision = routed_pair(reg, config, manifest)
    control = scheduler(store, system=1, provider=1, tool=1)
    accepted = accept(control, request, decision)

    assert accepted.accepted_route.budget_reservation_id == accepted.reservation.reservation_id
    assert accepted.accepted_route.reasoning_effort == decision.reasoning_effort
    assert accepted.reservation.reserved_minor == 20
    assert lifecycle.replay("run-1").node("node-1").status == "running"
    registry_state = control.agents.replay("run-1")
    assert registry_state.total_created == registry_state.active_count == 1
    assert registry_state.for_attempt(request.attempt_id).status == "active"
    assert registry_state.for_attempt(request.attempt_id).reasoning_effort == decision.reasoning_effort
    assert store.read_stream("scheduler", "global")[0].event_type == "RoutingDecisionAccepted"
    repeated = accept(control, request, decision)
    assert repeated == accepted
    assert len(store.read_stream("budget", "run-1")) == 1

    usage = UsageRecord(
        reservation_id=accepted.reservation.reservation_id,
        run_id="run-1",
        settlement_key="settle-attempt-1",
        currency="USD",
        input_tokens=4,
        output_tokens=6,
        cost_minor=12,
    )
    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-node-1-1",
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=10),
        outcome="succeeded",
        usage=usage,
    )
    state = lifecycle.replay("run-1")
    assert state.status == "running"
    assert state.node("node-2").status == "ready"

    next_request, next_decision = routed_pair(
        reg, config, manifest, node_id="node-2", role="tester"
    )
    next_accepted = accept(control, next_request, next_decision)
    control.finish_attempt(
        run_id="run-1",
        node_id="node-2",
        attempt_id=next_request.attempt_id,
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=20),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=next_accepted.reservation.reservation_id,
            run_id="run-1",
            settlement_key="settle-node-2",
            currency="USD",
            input_tokens=4,
            output_tokens=6,
            cost_minor=12,
        ),
    )
    assert lifecycle.replay("run-1").status == "succeeded"
    registry_state = control.agents.replay("run-1")
    assert registry_state.total_created == 2
    assert registry_state.active_count == 0
    assert registry_state.for_attempt(request.attempt_id).status == "completed"
    balance = BudgetLedger(store).available("run-1")
    assert balance.used_minor == 24
    assert balance.reserved_minor == 0


def test_run_recovery_reconstructs_active_and_settled_control_plane_state(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery.db")
    reg, config, lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, request, decision)

    recovered = RunRecoveryCoordinator(store).recover("run-1")
    assert recovered.lifecycle.status == "running"
    assert recovered.agents.total_created == recovered.agents.active_count == 1
    assert recovered.budget.reserved_minor == accepted.reservation.reserved_minor
    assert len(recovered.active_attempts) == 1
    assert recovered.active_attempts[0].attempt_id == request.attempt_id
    assert recovered.active_attempts[0].status == "active"

    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=accepted.reservation.reservation_id,
            run_id="run-1", settlement_key="run-recovery-success", currency="USD",
            input_tokens=1, output_tokens=1, cost_minor=1,
        ),
    )
    recovered = RunRecoveryCoordinator(store).recover("run-1")
    assert recovered.lifecycle.status == "succeeded"
    assert recovered.agents.active_count == 0
    assert recovered.budget.reserved_minor == recovered.budget.unknown_minor == 0
    assert recovered.budget.used_minor == 1
    assert recovered.active_attempts == ()


def test_scheduler_persists_recovery_evidence_and_consumes_retry_authorization_once(tmp_path):
    store = SQLiteEventStore(tmp_path / "persisted-recovery.db")
    reg, config, _lifecycle, manifest, contract = run_setup_with_frozen_contract(store)
    control = scheduler(store)
    first_request, first_decision = routed_pair(
        reg, config, manifest, contract_hash=contract.contract_hash
    )
    accepted = accept(control, first_request, first_decision)
    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=first_request.attempt_id,
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=5),
        outcome="failed",
        known_no_effect=True,
    )
    failure = ModelGatewayFailure(
        code="rate_limited",
        phase="provider",
        outcome="known_failure",
        retryable=True,
        http_status=429,
        provider_request_id="must-not-be-persisted",
    )
    plan = control.plan_attempt_recovery(
        run_id="run-1",
        node_id="node-1",
        attempt_id=first_request.attempt_id,
        fencing_generation=1,
        source_decision=first_decision,
        contract=contract,
        failure=failure,
    )

    assert plan.outcome == "authorized"
    assert plan.action == "same_model_retry"
    assert plan.authorization is not None
    assert control.plan_attempt_recovery(
        run_id="run-1",
        node_id="node-1",
        attempt_id=first_request.attempt_id,
        fencing_generation=1,
        source_decision=first_decision,
        contract=contract,
        failure=failure.model_copy(update={"provider_request_id": "different-request-id"}),
    ) == plan
    persisted = store.read_stream("scheduler", "global")
    classified_event = next(item for item in persisted if item.event_type == "AttemptFailureClassified")
    assert classified_event.payload["classification"]["evidence"]["retry_safe"] is True
    assert "provider_request_id" not in json.dumps(classified_event.payload)
    assert sum(item.event_type == "RecoveryPlanCreated" for item in persisted) == 1

    forged = RecoveryAuthorization.create(
        action="same_model_retry",
        source_decision_hash=first_decision.decision_hash,
        previous_model_id="model-1",
        authorized_model_ids=("model-1",),
        policy_manifest_hash=manifest.content_hash,
        evidence_hash=HASH,
    )
    forged_request, forged_decision = routed_pair(
        reg,
        config,
        manifest,
        attempt=2,
        contract_hash=contract.contract_hash,
        recovery_action="same_model_retry",
        recovery_authorization=forged,
        prior_decision_hash=first_decision.decision_hash,
        previous_model_id="model-1",
        failure_category="transient",
        retry_level=1,
    )
    with pytest.raises(StaleRoutingDecision, match="not persisted"):
        accept(control, forged_request, forged_decision)

    lowered_counter_request, lowered_counter_decision = routed_pair(
        reg,
        config,
        manifest,
        attempt=2,
        contract_hash=contract.contract_hash,
        recovery_action="same_model_retry",
        recovery_authorization=plan.authorization,
        prior_decision_hash=first_decision.decision_hash,
        previous_model_id="model-1",
        failure_category="transient",
        retry_level=0,
    )
    with pytest.raises(StaleRoutingDecision, match="counters differ"):
        accept(control, lowered_counter_request, lowered_counter_decision)

    retry_request, retry_decision = routed_pair(
        reg,
        config,
        manifest,
        attempt=2,
        contract_hash=contract.contract_hash,
        recovery_action="same_model_retry",
        recovery_authorization=plan.authorization,
        prior_decision_hash=first_decision.decision_hash,
        previous_model_id="model-1",
        failure_category="transient",
        retry_level=1,
    )
    retried = accept(control, retry_request, retry_decision)
    assert retried.accepted_route.model_id == "model-1"
    persisted_recovery_events = store.read_stream("scheduler", "global")
    with pytest.raises(StaleRoutingDecision, match="not allowed by its persisted plan"):
        _verify_persisted_recovery_authorization(
            persisted_recovery_events,
            request=retry_request.model_copy(update={"recovery_action": "same_tier_fallback"}),
            decision=retry_decision,
        )
    with pytest.raises(StaleRoutingDecision, match="no failure evidence"):
        _verify_persisted_recovery_authorization(
            [
                event for event in persisted_recovery_events
                if event.event_type != "AttemptFailureClassified"
            ],
            request=retry_request,
            decision=retry_decision,
        )
    with pytest.raises(StaleRoutingDecision, match="source attempt is missing"):
        _verify_persisted_recovery_authorization(
            [
                event for event in persisted_recovery_events
                if not (
                    event.event_type == "RoutingDecisionAccepted"
                    and event.payload.get("attempt_id") == first_request.attempt_id
                )
            ],
            request=retry_request,
            decision=retry_decision,
        )
    with pytest.raises(StaleRoutingDecision, match="next fencing generation"):
        _verify_persisted_recovery_authorization(
            persisted_recovery_events,
            request=retry_request.model_copy(update={"node_id": "child-node"}),
            decision=retry_decision,
        )
    with pytest.raises(StaleRoutingDecision, match="already consumed"):
        _verify_persisted_recovery_authorization(
            persisted_recovery_events, request=retry_request, decision=retry_decision
        )
    recovered = RunRecoveryCoordinator(store).recover("run-1")
    assert len(recovered.active_attempts) == 1
    assert recovered.active_attempts[0].attempt_id == "attempt-node-1-2"
    assert recovered.active_attempts[0].fencing_generation == 2
    accepted_retry = next(
        item for item in store.read_stream("scheduler", "global")
        if item.event_type == "RoutingDecisionAccepted"
        and item.payload.get("attempt_id") == retry_request.attempt_id
    )
    assert accepted_retry.payload["recovery_authorization_hash"] == plan.authorization.authorization_hash

    retried_reservation = retried.reservation
    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=retry_request.attempt_id,
        fencing_generation=2,
        completed_at=NOW + timedelta(seconds=10),
        outcome="failed",
        known_no_effect=True,
    )
    fallback_plan = control.plan_attempt_recovery(
        run_id="run-1",
        node_id="node-1",
        attempt_id=retry_request.attempt_id,
        fencing_generation=2,
        source_decision=retry_decision,
        contract=contract,
        failure=ModelGatewayFailure(
            code="provider_unavailable",
            phase="provider",
            outcome="known_failure",
            retryable=False,
            http_status=503,
        ),
    )
    assert fallback_plan.outcome == "blocked"
    persisted_after_fallback = store.read_stream("scheduler", "global")
    second_classification = next(
        item for item in persisted_after_fallback
        if item.event_type == "AttemptFailureClassified"
        and item.payload.get("attempt_id") == retry_request.attempt_id
    )
    assert second_classification.payload["classification"]["evidence"]["exhausted_model_ids"] == ["model-1"]
    assert retried_reservation.reserved_minor == 20

    class ReadOnlySchedulerEvents:
        def __init__(self, events):
            self.events = events

        def read_stream(self, stream_type, stream_id):
            assert (stream_type, stream_id) == ("scheduler", "global")
            return self.events

    replay_events = store.read_stream("scheduler", "global")
    tampered_retry_payload = dict(accepted_retry.payload)
    tampered_retry_payload["retry_level"] = 0
    tampered_events = [
        event.model_copy(update={"payload": tampered_retry_payload})
        if event.event_id == accepted_retry.event_id else event
        for event in replay_events
    ]
    with pytest.raises(RunRecoveryError, match="counters disagree"):
        RunRecoveryCoordinator(ReadOnlySchedulerEvents(tampered_events))._scheduler_state("run-1")
    without_plans = [
        event for event in replay_events if event.event_type != "RecoveryPlanCreated"
    ]
    with pytest.raises(RunRecoveryError, match="no persisted plan"):
        RunRecoveryCoordinator(ReadOnlySchedulerEvents(without_plans))._scheduler_state("run-1")
    wrong_source_payload = dict(accepted_retry.payload)
    wrong_source_payload["prior_decision_hash"] = HASH
    wrong_source_events = [
        event.model_copy(update={"payload": wrong_source_payload})
        if event.event_id == accepted_retry.event_id else event
        for event in replay_events
    ]
    with pytest.raises(RunRecoveryError, match="disagrees with its persisted plan"):
        RunRecoveryCoordinator(ReadOnlySchedulerEvents(wrong_source_events))._scheduler_state("run-1")
    first_plan_event = next(
        event for event in replay_events
        if event.event_type == "RecoveryPlanCreated"
        and event.payload.get("attempt_id") == first_request.attempt_id
    )
    first_classification = next(
        event for event in replay_events
        if event.event_type == "AttemptFailureClassified"
        and event.payload.get("attempt_id") == first_request.attempt_id
    )
    source_acceptance = next(
        event for event in replay_events
        if event.event_type == "RoutingDecisionAccepted"
        and event.payload.get("attempt_id") == first_request.attempt_id
    )
    source_release = next(
        event for event in replay_events
        if event.event_type == "AttemptSlotReleased"
        and event.payload.get("attempt_id") == first_request.attempt_id
    )

    def parse_scheduler_events(events):
        return RunRecoveryCoordinator(ReadOnlySchedulerEvents(events))._scheduler_state("run-1")

    with pytest.raises(RunRecoveryError, match="precedes a durable Attempt outcome"):
        parse_scheduler_events([source_acceptance, first_classification])
    bad_classification_hash = first_classification.model_copy(update={
        "payload": {**first_classification.payload, "decision_hash": HASH},
    })
    with pytest.raises(RunRecoveryError, match="bound to a different route"):
        parse_scheduler_events([source_acceptance, source_release, bad_classification_hash])
    malformed_classification = first_classification.model_copy(update={
        "payload": {**first_classification.payload, "classification": {"invalid": True}},
    })
    with pytest.raises(RunRecoveryError, match="persisted failure classification is invalid"):
        parse_scheduler_events([source_acceptance, source_release, malformed_classification])
    success_release = source_release.model_copy(update={
        "payload": {**source_release.payload, "outcome": "succeeded"},
    })
    with pytest.raises(RunRecoveryError, match="no failed Attempt outcome"):
        parse_scheduler_events([source_acceptance, success_release, first_classification])
    duplicate_classification = first_classification.model_copy(update={
        "event_id": "duplicate-failure-classification",
        "stream_version": first_classification.stream_version + 1,
    })
    with pytest.raises(RunRecoveryError, match="duplicate failure classifications"):
        parse_scheduler_events([
            source_acceptance, source_release, first_classification, duplicate_classification
        ])
    bad_plan_evidence = first_plan_event.model_copy(update={
        "payload": {**first_plan_event.payload, "evidence_hash": HASH},
    })
    with pytest.raises(RunRecoveryError, match="not bound to its failure evidence"):
        parse_scheduler_events([
            source_acceptance, source_release, first_classification, bad_plan_evidence
        ])
    duplicate_plan = first_plan_event.model_copy(update={
        "event_id": "duplicate-recovery-plan",
        "stream_version": first_plan_event.stream_version + 1,
    })
    with pytest.raises(RunRecoveryError, match="duplicate RecoveryPlan events"):
        parse_scheduler_events([
            source_acceptance, source_release, first_classification,
            first_plan_event, duplicate_plan,
        ])

    evidence_hash = first_classification.payload["classification"]["evidence"]["evidence_hash"]
    reviewer_authorization = RecoveryAuthorization.create(
        action="reviewer_node",
        source_decision_hash=first_decision.decision_hash,
        previous_model_id="model-1",
        authorized_model_ids=("model-1",),
        policy_manifest_hash=manifest.content_hash,
        evidence_hash=evidence_hash,
    )
    reviewer_plan_payload = dict(first_plan_event.payload)
    reviewer_plan = dict(reviewer_plan_payload["plan"])
    reviewer_plan["action"] = "reviewer_node"
    reviewer_plan["authorization"] = reviewer_authorization.model_dump(mode="json")
    reviewer_plan_payload["plan"] = reviewer_plan
    reviewer_retry_payload = dict(accepted_retry.payload)
    reviewer_retry_payload["recovery_action"] = "reviewer_node"
    reviewer_retry_payload["recovery_authorization_hash"] = reviewer_authorization.authorization_hash
    reviewer_events = [
        event.model_copy(update={"payload": reviewer_plan_payload})
        if event.event_id == first_plan_event.event_id
        else event.model_copy(update={"payload": reviewer_retry_payload})
        if event.event_id == accepted_retry.event_id
        else event
        for event in replay_events
    ]
    with pytest.raises(RunRecoveryError, match="supported same-node boundary"):
        RunRecoveryCoordinator(ReadOnlySchedulerEvents(reviewer_events))._scheduler_state("run-1")

    mismatched_evidence_authorization = RecoveryAuthorization.create(
        action=plan.action,
        source_decision_hash=first_decision.decision_hash,
        previous_model_id="model-1",
        authorized_model_ids=plan.authorized_model_ids,
        policy_manifest_hash=manifest.content_hash,
        evidence_hash=HASH,
    )
    invalid_plan = dict(first_plan_event.payload["plan"])
    invalid_plan["authorization"] = mismatched_evidence_authorization.model_dump(mode="json")
    invalid_plan_payload = dict(first_plan_event.payload)
    invalid_plan_payload["plan"] = invalid_plan
    invalid_plan_events = [
        event.model_copy(update={"payload": invalid_plan_payload})
        if event.event_id == first_plan_event.event_id else event
        for event in replay_events
        if event.stream_version <= first_plan_event.stream_version
    ]
    with pytest.raises(RunRecoveryError, match="authorization is inconsistent"):
        RunRecoveryCoordinator(ReadOnlySchedulerEvents(invalid_plan_events))._scheduler_state("run-1")

def test_scheduler_rejects_route_using_policy_other_than_frozen_node_policy(tmp_path):
    store = SQLiteEventStore(tmp_path / "frozen-node-policy.db")
    reg, config, _lifecycle, _manifest, contract = run_setup_with_frozen_contract(store)
    changed_manifest = PolicyManifest(
        authorities=(
            PolicyAuthority(
                source="system",
                max_permission="read-only",
                allowed_actions={"model_invoke"},
                allowed_tools={"model:other-model"},
            ),
        )
    )
    request, decision = routed_pair(
        reg,
        config,
        changed_manifest,
        contract_hash=contract.contract_hash,
    )

    with pytest.raises(StaleRoutingDecision, match="route policy version differs"):
        accept(scheduler(store), request, decision)


def test_scheduler_persists_unknown_failure_as_reconciliation_only(tmp_path):
    store = SQLiteEventStore(tmp_path / "unknown-recovery-plan.db")
    reg, config, _lifecycle, manifest, contract = run_setup_with_frozen_contract(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, contract_hash=contract.contract_hash)
    accept(control, request, decision)
    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=5),
        outcome="outcome_unknown",
    )

    plan = control.plan_attempt_recovery(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        source_decision=decision,
        contract=contract,
        failure=ModelGatewayFailure(
            code="timeout",
            phase="transport",
            outcome="unknown",
            retryable=False,
        ),
    )

    assert plan.outcome == "blocked"
    assert plan.reason == "unknown_outcome_requires_reconciliation"
    events = store.read_stream("scheduler", "global")
    classified = next(item for item in events if item.event_type == "AttemptFailureClassified")
    assert classified.payload["classification"]["disposition"] == "reconciliation_required"
    assert sum(item.event_type == "RecoveryPlanCreated" for item in events) == 1
    recovered = RunRecoveryCoordinator(store).recover("run-1")
    assert recovered.active_attempts[0].status == "outcome_unknown"


def test_scheduler_recovery_requires_accepted_and_durably_ended_source(tmp_path):
    store = SQLiteEventStore(tmp_path / "recovery-source-boundary.db")
    reg, config, _lifecycle, manifest, contract = run_setup_with_frozen_contract(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, contract_hash=contract.contract_hash)
    failure = ModelGatewayFailure(
        code="rate_limited",
        phase="provider",
        outcome="known_failure",
        retryable=True,
    )

    with pytest.raises(StaleRoutingDecision, match="source attempt"):
        control.plan_attempt_recovery(
            run_id="run-1",
            node_id="node-1",
            attempt_id=request.attempt_id,
            fencing_generation=1,
            source_decision=decision.model_copy(update={"attempt_id": "other-attempt"}),
            contract=contract,
            failure=failure,
        )
    with pytest.raises(StaleRoutingDecision, match="persisted accepted route"):
        control.plan_attempt_recovery(
            run_id="run-1",
            node_id="node-1",
            attempt_id=request.attempt_id,
            fencing_generation=1,
            source_decision=decision,
            contract=contract,
            failure=failure,
        )

    accept(control, request, decision)
    with pytest.raises(SchedulerError, match="durably ended Attempt"):
        control.plan_attempt_recovery(
            run_id="run-1",
            node_id="node-1",
            attempt_id=request.attempt_id,
            fencing_generation=1,
            source_decision=decision,
            contract=contract,
            failure=failure,
        )

    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=5),
        outcome="failed",
        known_no_effect=True,
    )
    with pytest.raises(SchedulerError, match="OutcomeUnknown"):
        control.plan_attempt_recovery(
            run_id="run-1",
            node_id="node-1",
            attempt_id=request.attempt_id,
            fencing_generation=1,
            source_decision=decision,
            contract=contract,
            failure=ModelGatewayFailure(
                code="timeout", phase="transport", outcome="unknown", retryable=False
            ),
        )


def test_scheduler_recovery_rejects_known_failure_after_success(tmp_path):
    store = SQLiteEventStore(tmp_path / "recovery-success-mismatch.db")
    reg, config, _lifecycle, manifest, contract = run_setup_with_frozen_contract(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, contract_hash=contract.contract_hash)
    accepted = accept(control, request, decision)
    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=5),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=accepted.reservation.reservation_id,
            run_id="run-1",
            settlement_key="recovery-success-mismatch",
            currency="USD",
            input_tokens=1,
            output_tokens=1,
            cost_minor=1,
        ),
    )
    with pytest.raises(SchedulerError, match="durably failed Attempt"):
        control.plan_attempt_recovery(
            run_id="run-1",
            node_id="node-1",
            attempt_id=request.attempt_id,
            fencing_generation=1,
            source_decision=decision,
            contract=contract,
            failure=ModelGatewayFailure(
                code="rate_limited", phase="provider", outcome="known_failure", retryable=True
            ),
        )


def test_recovery_authorizer_rejects_missing_or_unintegrated_graph_authorizations():
    reg = registry()
    config = effective_config(reg).config
    manifest = PolicyManifest(
        authorities=(PolicyAuthority(
            source="system",
            max_permission="read-only",
            allowed_actions={"model_invoke"},
            allowed_tools={"model:model-1"},
        ),)
    )
    request, decision = routed_pair(reg, config, manifest)
    with pytest.raises(StaleRoutingDecision, match="authorization is missing"):
        _verify_persisted_recovery_authorization([], request=request, decision=decision)

    authorization = RecoveryAuthorization.create(
        action="reviewer_node",
        source_decision_hash=decision.decision_hash,
        previous_model_id="model-1",
        authorized_model_ids=("model-1",),
        policy_manifest_hash=manifest.content_hash,
        evidence_hash=HASH,
    )
    reviewer_request, reviewer_decision = routed_pair(
        reg,
        config,
        manifest,
        attempt=2,
        recovery_action="reviewer_node",
        recovery_authorization=authorization,
        prior_decision_hash=decision.decision_hash,
        previous_model_id="model-1",
    )
    with pytest.raises(SchedulerError, match="graph or health-control integration"):
        _verify_persisted_recovery_authorization(
            [], request=reviewer_request, decision=reviewer_decision
        )


def test_scheduler_recovery_keeps_success_settlement_failure_blocked(tmp_path):
    store = SQLiteEventStore(tmp_path / "recovery-settlement-blocked.db")
    reg, config, _lifecycle, manifest, contract = run_setup_with_frozen_contract(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, contract_hash=contract.contract_hash)
    accept(control, request, decision)
    control.finish_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        completed_at=NOW + timedelta(seconds=5),
        outcome="outcome_unknown",
    )

    plan = control.plan_attempt_recovery(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        source_decision=decision,
        contract=contract,
        failure=ModelGatewayFailure(
            code="usage_unavailable",
            phase="settlement",
            outcome="known_success",
            retryable=False,
        ),
    )

    assert plan.outcome == "blocked"
    assert plan.reason == "successful_call_requires_settlement"
    assert RunRecoveryCoordinator(store).recover("run-1").active_attempts[0].status == "outcome_unknown"


def test_run_recovery_classifies_effect_intent_as_unknown_until_receipt(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-effect.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, request, decision)
    intent = EventDraft(
        "EffectIntentRecorded",
        {
            "effect_id": "effect-1",
            "recovery_class": "queryable",
            "provider_idempotency_key": "provider-key-must-not-escape",
        },
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id=decision.decision_hash,
    )
    [stored_intent] = store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [intent],
        "effect-intent-1",
    )

    recovered = RunRecoveryCoordinator(store).recover(request.run_id)
    [effect] = recovered.effects
    assert effect.status == "outcome_unknown"
    assert effect.recovery_class == "queryable"
    assert effect.intent_event_id == stored_intent.event_id
    assert effect.receipt_event_id is None
    assert effect.provider_idempotency_key_hash == "sha256:" + hashlib.sha256(
        b"provider-key-must-not-escape"
    ).hexdigest()
    assert "provider-key-must-not-escape" not in repr(effect)
    assert recovered.active_attempts[0].attempt_id == accepted.accepted_route.attempt_id

    receipt = EventDraft(
        "EffectReceiptRecorded",
        {"effect_id": "effect-1", "outcome": "applied", "receipt_hash": HASH},
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id=stored_intent.event_id,
    )
    [stored_receipt] = store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [receipt],
        "effect-receipt-1",
    )
    recovered = RunRecoveryCoordinator(store).recover(request.run_id)
    assert recovered.effects[0].status == "applied"
    assert recovered.effects[0].receipt_event_id == stored_receipt.event_id


@pytest.mark.parametrize(
    ("effect_metadata", "error"),
    [
        ({"recovery_class": "automatic-anything"}, "unsupported recovery class"),
        ({"provider_idempotency_key_hash": "not-a-sha256"}, "idempotency-key hash is invalid"),
    ],
)
def test_run_recovery_rejects_invalid_effect_recovery_metadata(tmp_path, effect_metadata, error):
    store = SQLiteEventStore(tmp_path / "run-recovery-invalid-effect.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accept(control, request, decision)
    store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [
            EventDraft(
                "EffectIntentRecorded",
                {"effect_id": "effect-invalid", **effect_metadata},
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                causation_id=decision.decision_hash,
            )
        ],
        "effect-invalid-metadata",
    )

    with pytest.raises(RunRecoveryError, match=error):
        RunRecoveryCoordinator(store).recover(request.run_id)


@pytest.mark.parametrize(
    ("receipt_payload", "error"),
    [
        ({"effect_id": "effect-1", "outcome": None}, "no valid outcome"),
        ({"effect_id": "effect-1", "outcome": "applied"}, "no valid evidence reference"),
    ],
)
def test_run_recovery_rejects_receipt_without_outcome_or_evidence(tmp_path, receipt_payload, error):
    store = SQLiteEventStore(tmp_path / "run-recovery-invalid-receipt.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accept(control, request, decision)
    intent = EventDraft(
        "EffectIntentRecorded",
        {"effect_id": "effect-1"},
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id=decision.decision_hash,
    )
    store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [intent],
        "effect-intent-for-invalid-receipt",
    )
    receipt = EventDraft(
        "EffectReceiptRecorded",
        receipt_payload,
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id="effect-1",
    )
    store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [receipt],
        "effect-invalid-receipt",
    )

    with pytest.raises(RunRecoveryError, match=error):
        RunRecoveryCoordinator(store).recover(request.run_id)


def test_run_recovery_treats_legacy_receipt_id_as_applied(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-legacy-receipt.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accept(control, request, decision)
    intent = EventDraft(
        "EffectIntentRecorded",
        {"effect_id": "effect-legacy"},
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id=decision.decision_hash,
    )
    store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [intent],
        "effect-intent-legacy-receipt",
    )
    receipt = EventDraft(
        "EffectReceiptRecorded",
        {"effect_id": "effect-legacy", "receipt_id": "legacy-receipt"},
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id="effect-legacy",
    )
    store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [receipt],
        "effect-legacy-receipt",
    )

    assert RunRecoveryCoordinator(store).recover(request.run_id).effects[0].status == "applied"


def test_run_recovery_refuses_terminal_attempt_with_unresolved_effect(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-effect-terminal.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, request, decision)
    store.append(
        "budget",
        request.run_id,
        store.current_version("budget", request.run_id),
        [
            EventDraft(
                "EffectIntentRecorded",
                {"effect_id": "effect-pending", "recovery_class": "manual_only"},
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                causation_id=decision.decision_hash,
            )
        ],
        "effect-intent-pending",
    )
    control.finish_attempt(
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        completed_at=NOW + timedelta(seconds=5),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=accepted.reservation.reservation_id,
            run_id=request.run_id,
            settlement_key="effect-terminal-settlement",
            currency="USD",
            input_tokens=1,
            output_tokens=1,
            cost_minor=1,
        ),
    )
    with pytest.raises(RunRecoveryError, match="terminal Attempt has an unresolved external effect"):
        RunRecoveryCoordinator(store).recover(request.run_id)


def test_run_recovery_verifies_run_artifact_metadata_and_content(tmp_path):
    database = tmp_path / "run-recovery-artifact.db"
    store = SQLiteEventStore(database)
    run_setup(store, nodes=())
    artifact_root = tmp_path / "private-artifacts"
    artifacts = ArtifactStore(artifact_root, event_store=store)
    record = artifacts.publish_bytes(
        b"recovery evidence",
        source={"run_id": "run-1", "node_id": "node-1"},
        artifact_type="verification-report",
    )

    with pytest.raises(RunRecoveryError, match="no artifact verifier"):
        RunRecoveryCoordinator(store).recover("run-1")

    store.close()
    reopened = SQLiteEventStore(database)
    restarted_artifacts = ArtifactStore(artifact_root, event_store=reopened)
    recovered = RunRecoveryCoordinator(reopened, artifact_store=restarted_artifacts).recover("run-1")
    assert len(recovered.artifacts) == 1
    assert recovered.artifacts[0].digest == record.digest
    assert recovered.artifacts[0].publication_id == record.publication_id
    assert recovered.artifacts[0].size == len(b"recovery evidence")

    (artifact_root / record.digest.removeprefix("sha256:")).write_bytes(b"tampered")
    with pytest.raises(RunRecoveryError, match="durable-state invariant"):
        RunRecoveryCoordinator(reopened, artifact_store=restarted_artifacts).recover("run-1")


def test_scheduler_recovery_admission_accepts_artifacts_with_configured_verifier(tmp_path):
    store = SQLiteEventStore(tmp_path / "scheduler-artifact-admission.db")
    reg, config, _lifecycle, manifest = run_setup(store)
    artifacts = ArtifactStore(tmp_path / "scheduler-artifact-root", event_store=store)
    record = artifacts.publish_bytes(
        b"already published",
        source={"run_id": "run-1"},
        artifact_type="worker-output",
    )
    control = scheduler(store, artifact_store=artifacts)
    request, decision = routed_pair(reg, config, manifest)

    accepted = accept(control, request, decision)

    assert accepted.accepted_route.attempt_id == request.attempt_id
    assert control.recovery.recover(request.run_id).artifacts[0].digest == record.digest


def test_run_recovery_rejects_artifact_fencing_without_attempt_provenance(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-artifact-fencing.db")
    run_setup(store, nodes=())
    artifacts = ArtifactStore(tmp_path / "artifact-fencing", event_store=store)
    artifacts.publish_bytes(
        b"bad provenance",
        source={"run_id": "run-1", "node_id": "node-1", "fencing_generation": "1"},
        artifact_type="verification-report",
    )

    with pytest.raises(RunRecoveryError, match="fencing generation without an Attempt"):
        RunRecoveryCoordinator(store, artifact_store=artifacts).recover("run-1")


def test_run_recovery_restores_cancelled_attempt_with_terminal_evidence(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-cancelled.db")
    reg, config, lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, request, decision)
    lifecycle.request_cancel("run-1", reason_code="recover_cancel")
    control.acknowledge_cancellation(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, stopped_at=NOW + timedelta(seconds=5),
        stop_receipt_hash="sha256:" + "f" * 64,
        no_effect_receipt_hash="sha256:" + "a" * 64,
    )

    recovered = RunRecoveryCoordinator(store).recover("run-1")
    assert recovered.lifecycle.status == "cancelled"
    assert recovered.agents.for_attempt(request.attempt_id).status == "cancelled"
    assert recovered.budget.reserved_minor == recovered.budget.unknown_minor == 0
    assert recovered.active_attempts == ()


def test_run_recovery_scheduler_event_parser_fails_closed_on_corrupt_sequences(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-parser.db")
    reg, config, _, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accept(control, request, decision)
    accepted = store.read_stream("scheduler", "global")[0]
    payload = dict(accepted.payload)

    def event(event_type, *, event_id, stream_version, payload_override=None,
              node_id=None, attempt_id=None, run_id="run-1", fencing_generation=1):
        return accepted.model_copy(update={
            "event_id": event_id,
            "stream_version": stream_version,
            "event_type": event_type,
            "idempotency_key": f"test:{event_id}",
            "payload": payload if payload_override is None else payload_override,
            "node_id": accepted.node_id if node_id is None else node_id,
            "attempt_id": accepted.attempt_id if attempt_id is None else attempt_id,
            "run_id": run_id,
            "fencing_generation": fencing_generation,
        })

    unknown_payload = dict(payload)
    unknown_payload["outcome"] = "outcome_unknown"
    unknown = event(
        "AttemptOutcomeUnknown", event_id="test-unknown", stream_version=2,
        payload_override=unknown_payload,
    )
    released_payload = dict(payload)
    released_payload["outcome"] = "failed"
    released = event(
        "AttemptSlotReleased", event_id="test-released", stream_version=3,
        payload_override=released_payload,
    )
    unrelated_payload = dict(payload)
    unrelated_payload["run_id"] = "another-run"
    unrelated = event(
        "UnexpectedSchedulerEvent", event_id="unrelated-run", stream_version=4,
        payload_override=unrelated_payload, run_id="another-run",
    )

    class ReadOnlySchedulerEvents:
        def __init__(self, events):
            self.events = events

        def read_stream(self, stream_type, stream_id):
            assert (stream_type, stream_id) == ("scheduler", "global")
            return self.events

    valid = RunRecoveryCoordinator(
        ReadOnlySchedulerEvents([accepted, unknown, released, unrelated])
    )
    attempts, by_ref = valid._scheduler_state("run-1")
    assert attempts[(request.node_id, request.attempt_id)]["status"] == "released"
    assert by_ref[payload["attempt_ref"]]["terminal_outcome"] == "failed"

    def parser(events):
        return RunRecoveryCoordinator(ReadOnlySchedulerEvents(events))._scheduler_state("run-1")

    duplicate_acceptance = accepted.model_copy(update={
        "event_id": "duplicate-accepted", "stream_version": 2,
        "idempotency_key": "test:duplicate-accepted",
    })
    bad_route_payload = dict(payload)
    bad_route_payload["accepted_route"] = {"model_id": "invalid"}
    missing_attempt = "attempt-not-accepted"
    missing_node = "node-not-accepted"
    missing_attempt_ref = "sha256:" + hashlib.sha256(
        json.dumps(("run-1", missing_node, missing_attempt), separators=(",", ":"))
        .encode("utf-8")
    ).hexdigest()
    unknown_attempt_payload = dict(unknown_payload)
    unknown_attempt_payload.update({
        "attempt_ref": missing_attempt_ref,
        "node_id": missing_node,
        "attempt_id": missing_attempt,
    })
    bad_identity = event(
        "UnexpectedSchedulerEvent", event_id="bad-identity", stream_version=2,
        run_id=None,
    )
    stale_generation = event(
        "AttemptOutcomeUnknown", event_id="stale-generation", stream_version=2,
        fencing_generation=2,
    )
    inconsistent_recovery_payload = dict(payload)
    inconsistent_recovery_payload["recovery_action"] = "same_model_retry"

    cases = (
        ([accepted, duplicate_acceptance], "duplicates a route acceptance"),
        ([event("RoutingDecisionAccepted", event_id="bad-route", stream_version=1,
               payload_override=bad_route_payload)], "accepted route is invalid"),
        ([event("RoutingDecisionAccepted", event_id="bad-time", stream_version=1,
                payload_override={**payload, "accepted_at": "not-a-time"})],
         "timestamp is invalid"),
        ([event("RoutingDecisionAccepted", event_id="naive-time", stream_version=1,
                payload_override={**payload, "accepted_at": "2026-09-23T12:00:00"})],
         "lacks a timezone"),
        ([event("RoutingDecisionAccepted", event_id="expired-time", stream_version=1,
                payload_override={**payload, "lease_expires_at": payload["accepted_at"]})],
         "expires before route acceptance"),
        ([event("RoutingDecisionAccepted", event_id="inconsistent-recovery", stream_version=1,
                payload_override=inconsistent_recovery_payload)], "inconsistent recovery context"),
        ([bad_identity], "invalid Run execution identity"),
        ([event("AttemptOutcomeUnknown", event_id="unknown-ref", stream_version=1,
               payload_override=unknown_attempt_payload, node_id=missing_node,
               attempt_id=missing_attempt)], "unknown accepted Attempt"),
        ([accepted, event("AttemptOutcomeUnknown", event_id="stale-context", stream_version=2,
                          node_id="other-node")], "invalid Run execution identity"),
        ([accepted, unknown, unknown.model_copy(update={"event_id": "unknown-again",
                                                       "stream_version": 3})], "out of order"),
        ([accepted, released, released.model_copy(update={"event_id": "release-again",
                                                          "stream_version": 3})], "out of order"),
        ([accepted, event("AttemptSlotReleased", event_id="no-outcome", stream_version=2,
                          payload_override=payload)], "no valid terminal outcome"),
        ([accepted, event("UnexpectedSchedulerEvent", event_id="unsupported", stream_version=2)],
         "unsupported during recovery"),
        ([accepted, stale_generation], "stale execution context"),
    )
    for events, message in cases:
        with pytest.raises(RunRecoveryError, match=message):
            parser(events)


def test_run_recovery_sanitizes_unexpected_storage_failures(tmp_path, monkeypatch):
    store = SQLiteEventStore(tmp_path / "run-recovery-storage-failure.db")

    def fail_read(*_args, **_kwargs):
        raise RuntimeError("secret-token-and-provider-payload")

    monkeypatch.setattr(store, "read_stream_with_version", fail_read)
    with pytest.raises(RunRecoveryError, match="durable-state invariant") as error:
        RunRecoveryCoordinator(store).recover("run-1")
    assert "secret-token" not in str(error.value)


def test_run_recovery_survives_process_death_before_and_after_acceptance_commit(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("the crash-injection harness currently requires fork")

    def die_in_child(database, request, decision, crash_point):
        child_store = SQLiteEventStore(database)
        control = scheduler(child_store)
        if crash_point == "before_commit":
            register_attempt = control.agents.register_attempt

            def register_then_die(*args, **kwargs):
                register_attempt(*args, **kwargs)
                os._exit(73)

            control.agents.register_attempt = register_then_die
        accept(control, request, decision)
        # Models a lost IPC response after the SQLite transaction committed.
        os._exit(73)

    for crash_point in ("before_commit", "after_commit"):
        database = tmp_path / f"run-recovery-{crash_point}.db"
        store = SQLiteEventStore(database)
        reg, config, _, manifest = run_setup(store)
        request, decision = routed_pair(reg, config, manifest)
        store.close()

        process = multiprocessing.get_context("fork").Process(
            target=die_in_child,
            args=(str(database), request, decision, crash_point),
        )
        process.start()
        process.join(timeout=20)
        if process.is_alive():
            process.kill()
            process.join()
            pytest.fail(f"child process timed out at crash point {crash_point}")
        assert process.exitcode == 73

        reopened = SQLiteEventStore(database)
        recovered = RunRecoveryCoordinator(reopened).recover("run-1")
        control = scheduler(reopened)
        if crash_point == "before_commit":
            assert recovered.lifecycle.node("node-1").attempts == ()
            assert recovered.agents.total_created == 0
            assert recovered.budget.reserved_minor == 0
            assert recovered.active_attempts == ()
        else:
            assert len(recovered.lifecycle.node("node-1").attempts) == 1
            assert recovered.agents.total_created == 1
            assert recovered.budget.reserved_minor == 20
            assert len(recovered.active_attempts) == 1
            repeated = accept(control, request, decision)
            assert repeated.accepted_route.attempt_id == request.attempt_id
            assert len(reopened.read_stream("scheduler", "global")) == 1
            assert len(reopened.read_stream("budget", "run-1")) == 1


def test_run_recovery_survives_process_death_before_and_after_reconciliation_commit(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("the crash-injection harness currently requires fork")

    def reconcile_then_die(database, request, crash_point):
        child_store = SQLiteEventStore(database)
        control = scheduler(child_store)
        if crash_point == "before_commit":
            record = control.lifecycle.record_attempt_reconciled

            def record_then_die(*args, **kwargs):
                record(*args, **kwargs)
                os._exit(76)

            control.lifecycle.record_attempt_reconciled = record_then_die
        control.reconcile_attempt(
            run_id="run-1",
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=1,
            reconciled_at=NOW + timedelta(minutes=3),
            outcome="failed",
            known_no_effect=True,
        )
        # Models a lost IPC response after the multi-stream transaction commits.
        os._exit(77)

    for crash_point in ("before_commit", "after_commit"):
        database = tmp_path / f"reconciliation-recovery-{crash_point}.db"
        store = SQLiteEventStore(database)
        reg, config, _, manifest = run_setup(store)
        request, decision = routed_pair(reg, config, manifest)
        control = scheduler(store)
        accept(control, request, decision)
        control.finish_attempt(
            run_id="run-1",
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=1,
            completed_at=NOW + timedelta(minutes=2),
            outcome="outcome_unknown",
        )
        store.close()

        process = multiprocessing.get_context("fork").Process(
            target=reconcile_then_die,
            args=(str(database), request, crash_point),
        )
        process.start()
        process.join(timeout=20)
        if process.is_alive():
            process.kill()
            process.join()
            pytest.fail(f"child process timed out at reconciliation crash point {crash_point}")
        assert process.exitcode == (76 if crash_point == "before_commit" else 77)

        reopened = SQLiteEventStore(database)
        recovery = RunRecoveryCoordinator(reopened).recover("run-1")
        control = scheduler(reopened)
        releases = [
            event for event in reopened.read_stream("scheduler", "global")
            if event.event_type == "AttemptSlotReleased"
        ]
        if crash_point == "before_commit":
            assert recovery.lifecycle.node("node-1").status == "awaiting_reconciliation"
            assert recovery.agents.for_attempt(request.attempt_id).status == "outcome_unknown"
            assert recovery.budget.unknown_minor == 20
            assert recovery.active_attempts[0].status == "outcome_unknown"
            assert releases == []
            control.reconcile_attempt(
                run_id="run-1", node_id=request.node_id, attempt_id=request.attempt_id,
                fencing_generation=1, reconciled_at=NOW + timedelta(minutes=3),
                outcome="failed", known_no_effect=True,
            )
        else:
            assert recovery.lifecycle.node("node-1").status == "ready"
            assert recovery.agents.for_attempt(request.attempt_id).status == "failed"
            assert recovery.budget.unknown_minor == 0
            assert recovery.active_attempts == ()
            assert len(releases) == 1
            control.reconcile_attempt(
                run_id="run-1", node_id=request.node_id, attempt_id=request.attempt_id,
                fencing_generation=1, reconciled_at=NOW + timedelta(minutes=3),
                outcome="failed", known_no_effect=True,
            )
            assert len([
                event for event in reopened.read_stream("scheduler", "global")
                if event.event_type == "AttemptSlotReleased"
            ]) == 1


def test_run_recovery_replays_effect_state_after_process_death(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("the crash-injection harness currently requires fork")

    def record_effect_then_die(database, request, decision, include_receipt):
        child_store = SQLiteEventStore(database)
        effect_id = "effect-with-receipt" if include_receipt else "effect-without-receipt"
        events = [
            EventDraft(
                "EffectIntentRecorded",
                {"effect_id": effect_id, "recovery_class": "manual_only"},
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                causation_id=decision.decision_hash,
            )
        ]
        if include_receipt:
            events.append(
                EventDraft(
                    "EffectReceiptRecorded",
                    {"effect_id": effect_id, "outcome": "applied", "receipt_hash": HASH},
                    run_id=request.run_id,
                    node_id=request.node_id,
                    attempt_id=request.attempt_id,
                    fencing_generation=request.fencing_generation,
                    causation_id=decision.decision_hash,
                )
            )
        child_store.append(
            "budget",
            request.run_id,
            child_store.current_version("budget", request.run_id),
            events,
            f"effect-crash-{include_receipt}",
        )
        # Simulate abrupt process termination after the durable effect record.
        os._exit(74)

    for include_receipt, expected_status in (
        (False, "outcome_unknown"),
        (True, "applied"),
    ):
        database = tmp_path / f"run-recovery-effect-{include_receipt}.db"
        store = SQLiteEventStore(database)
        reg, config, _lifecycle, manifest = run_setup(store)
        control = scheduler(store)
        request, decision = routed_pair(reg, config, manifest)
        accept(control, request, decision)
        store.close()

        process = multiprocessing.get_context("fork").Process(
            target=record_effect_then_die,
            args=(str(database), request, decision, include_receipt),
        )
        process.start()
        process.join(timeout=20)
        if process.is_alive():
            process.kill()
            process.join()
            pytest.fail("effect crash-injection child process timed out")
        assert process.exitcode == 74

        reopened = SQLiteEventStore(database)
        recovered = RunRecoveryCoordinator(reopened).recover(request.run_id)
        assert len(recovered.effects) == 1
        assert recovered.effects[0].status == expected_status
        assert recovered.budget.reserved_minor == 20
        assert len(recovered.active_attempts) == 1
        reopened.close()


def test_run_recovery_verifies_artifact_after_publisher_process_death(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("the crash-injection harness currently requires fork")

    def publish_artifact_then_die(database, artifact_root):
        child_store = SQLiteEventStore(database)
        ArtifactStore(artifact_root, event_store=child_store).publish_bytes(
            b"durably published before worker process death",
            source={"run_id": "run-1", "node_id": "node-1"},
            artifact_type="worker-output",
        )
        os._exit(75)

    database = tmp_path / "run-recovery-artifact-process-death.db"
    artifact_root = tmp_path / "run-recovery-artifact-process-death"
    store = SQLiteEventStore(database)
    run_setup(store, nodes=())
    store.close()

    process = multiprocessing.get_context("fork").Process(
        target=publish_artifact_then_die,
        args=(str(database), str(artifact_root)),
    )
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("artifact crash-injection child process timed out")
    assert process.exitcode == 75

    reopened = SQLiteEventStore(database)
    artifacts = ArtifactStore(artifact_root, event_store=reopened)
    recovered = RunRecoveryCoordinator(reopened, artifact_store=artifacts).recover("run-1")
    assert len(recovered.artifacts) == 1
    assert recovered.artifacts[0].size == len(b"durably published before worker process death")
    reopened.close()


def test_run_recovery_attributes_interrupted_artifact_publications(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("the crash-injection harness currently requires fork")

    def interrupt_publication(database, artifact_root, crash_point):
        child_store = SQLiteEventStore(database)
        artifacts = ArtifactStore(artifact_root, event_store=child_store)
        if crash_point == "after_intent":
            record_intent = artifacts._record_intent

            def intent_then_die(record):
                record_intent(record)
                os._exit(79)

            artifacts._record_intent = intent_then_die
        else:
            def metadata_then_die(_record):
                os._exit(80)

            artifacts._record_metadata = metadata_then_die
        artifacts.publish_bytes(
            b"candidate interrupted during artifact publication",
            source={"run_id": "run-1", "node_id": "node-1"},
            artifact_type="worker-output",
        )
        os._exit(81)

    for crash_point, expected_state, expected_exit in (
        ("after_intent", "missing", 79),
        ("after_blob", "orphaned_blob", 80),
    ):
        database = tmp_path / f"run-artifact-intent-{crash_point}.db"
        artifact_root = tmp_path / f"run-artifact-intent-{crash_point}"
        store = SQLiteEventStore(database)
        run_setup(store, nodes=())
        store.close()

        process = multiprocessing.get_context("fork").Process(
            target=interrupt_publication,
            args=(str(database), str(artifact_root), crash_point),
        )
        process.start()
        process.join(timeout=20)
        if process.is_alive():
            process.kill()
            process.join()
            pytest.fail(f"artifact publication child timed out at {crash_point}")
        assert process.exitcode == expected_exit

        reopened = SQLiteEventStore(database)
        artifacts = ArtifactStore(artifact_root, event_store=reopened)
        recovered = RunRecoveryCoordinator(
            reopened, artifact_store=artifacts
        ).recover("run-1")
        assert recovered.artifacts == ()
        assert len(recovered.pending_artifacts) == 1
        assert recovered.pending_artifacts[0].content_state == expected_state
        assert recovered.pending_artifacts[0].node_id == "node-1"
        digest = recovered.pending_artifacts[0].digest
        assert artifacts.find_orphan_blobs() == (
            (digest,) if expected_state == "orphaned_blob" else ()
        )
        reopened.close()


@pytest.mark.parametrize(
    "source, expected_error",
    [
        (
            {"run_id": "run-1", "fencing_generation": "1"},
            "fencing generation without an Attempt",
        ),
        (
            {
                "run_id": "run-1", "node_id": "node-1",
                "attempt_id": "attempt-not-accepted", "fencing_generation": "1",
            },
            "unknown Attempt",
        ),
        (
            {
                "run_id": "run-1", "node_id": "node-1",
                "attempt_id": "attempt-node-1-1", "fencing_generation": "2",
            },
            "stale fencing generation",
        ),
    ],
)
def test_run_recovery_rejects_pending_artifact_with_invalid_attempt_provenance(
    tmp_path, source, expected_error
):
    store = SQLiteEventStore(tmp_path / "invalid-pending-artifact.db")
    _reg, _config, _lifecycle, manifest = run_setup(store)
    request, decision = routed_pair(_reg, _config, manifest)
    accept(scheduler(store), request, decision)
    artifacts = ArtifactStore(tmp_path / "invalid-pending-artifacts", event_store=store)
    content = b"unpublished candidate"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    intent = ArtifactRecord.from_metadata(
        digest,
        size=len(content),
        artifact_type="worker-output",
        media_type="application/octet-stream",
        source=source,
        schema_version=1,
        redaction_state="unknown",
        readable_scope=("run-1",),
        references=(),
        lifecycle_state="temporary",
    )
    artifacts._record_intent(intent)

    with pytest.raises(RunRecoveryError, match=expected_error):
        RunRecoveryCoordinator(store, artifact_store=artifacts).recover("run-1")


def test_route_acceptance_fails_closed_when_replayed_streams_disagree(tmp_path):
    store = SQLiteEventStore(tmp_path / "run-recovery-mismatch.db")
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH),
        NodeSpec(node_id="node-2", role="coder", planning_contract_hash=HASH),
    )
    reg, config, lifecycle, manifest = run_setup(store, nodes=nodes)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, node_id="node-1")
    accepted = accept(control, request, decision)

    # Simulate a partial cross-stream write: the Agent transition exists, but
    # no matching scheduler/lifecycle/budget completion was committed.
    control.agents.complete_attempt(
        "run-1", node_id="node-1", attempt_id=request.attempt_id,
        agent_instance_id=accepted.agent_instance_id, fencing_generation=1,
        causation_id=decision.decision_hash, outcome="failed",
    )
    second_request, second_decision = routed_pair(
        reg, config, manifest, node_id="node-2"
    )
    before_scheduler = len(store.read_stream("scheduler", "global"))
    before_budget = len(store.read_stream("budget", "run-1"))
    with pytest.raises(SchedulerError, match="recovery consistency check failed"):
        accept(control, second_request, second_decision)
    assert len(store.read_stream("scheduler", "global")) == before_scheduler
    assert len(store.read_stream("budget", "run-1")) == before_budget
    with pytest.raises(RunRecoveryError, match="states disagree"):
        RunRecoveryCoordinator(store).recover("run-1")


def test_unknown_result_keeps_resources_until_explicit_reconciliation(tmp_path):
    store = SQLiteEventStore(tmp_path / "unknown.db")
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH, max_attempts=2),
        NodeSpec(node_id="node-2", role="coder", planning_contract_hash=HASH, max_attempts=2),
    )
    reg, config, lifecycle, manifest = run_setup(store, nodes=nodes)
    control = scheduler(store, system=1, provider=1, tool=1)
    request, decision = routed_pair(reg, config, manifest, node_id="node-1")
    accepted = accept(control, request, decision)
    with pytest.raises(LifecycleConflict, match="late attempt result"):
        control.finish_attempt(
            run_id="run-1",
            node_id="node-1",
            attempt_id=request.attempt_id,
            fencing_generation=1,
            completed_at=NOW + timedelta(minutes=2),
            outcome="succeeded",
            usage=UsageRecord(
                reservation_id=accepted.reservation.reservation_id,
                run_id="run-1",
                settlement_key="late-result",
                currency="USD",
                cost_minor=10,
            ),
        )
    assert BudgetLedger(store).get_reservation(accepted.reservation.reservation_id, run_id="run-1").status == "reserved"
    assert control.mark_expired_attempts_unknown(as_of=NOW + timedelta(minutes=2)) == (request.attempt_id,)
    assert control.mark_expired_attempts_unknown(as_of=NOW + timedelta(minutes=2)) == ()
    assert lifecycle.replay("run-1").node("node-1").status == "awaiting_reconciliation"
    unknown_agents = control.agents.replay("run-1")
    assert unknown_agents.total_created == 1
    assert unknown_agents.active_count == unknown_agents.unknown_count == 1
    second_request, second_decision = routed_pair(reg, config, manifest, node_id="node-2")
    with pytest.raises(ConcurrencyLimitExceeded, match="system"):
        accept(control, second_request, second_decision)

    control.reconcile_attempt(
        run_id="run-1",
        node_id="node-1",
        attempt_id=request.attempt_id,
        fencing_generation=1,
        reconciled_at=NOW + timedelta(minutes=3),
        outcome="failed",
        known_no_effect=True,
    )
    state = lifecycle.replay("run-1")
    assert state.node("node-1").status == "ready"
    reconciled_agents = control.agents.replay("run-1")
    assert reconciled_agents.total_created == 1
    assert reconciled_agents.active_count == 0
    assert reconciled_agents.for_attempt(request.attempt_id).status == "failed"
    retry_request, retry_decision = routed_pair(reg, config, manifest, node_id="node-1", attempt=2)
    retry = accept(control, retry_request, retry_decision)
    assert retry.accepted_route.fencing_generation == 2
    assert retry.agent_instance_id != accepted.agent_instance_id
    assert control.agents.replay("run-1").total_created == 2
    assert accepted.reservation.reservation_id != accepted.accepted_route.decision_id


def test_stale_decisions_capacity_and_budget_fail_before_creating_partial_holds(tmp_path):
    store = SQLiteEventStore(tmp_path / "reject.db")
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH),
        NodeSpec(node_id="node-2", role="coder", planning_contract_hash=HASH),
    )
    reg, config, _, manifest = run_setup(store, nodes=nodes)
    control = scheduler(store, system=1, provider=1, tool=1)
    request, decision = routed_pair(reg, config, manifest, node_id="node-1")
    accept(control, request, decision)

    second_request, second_decision = routed_pair(reg, config, manifest, node_id="node-2")
    with pytest.raises(ConcurrencyLimitExceeded):
        accept(control, second_request, second_decision)
    assert len(store.read_stream("budget", "run-1")) == 1

    wrong_request, wrong_decision = routed_pair(reg, config, manifest, node_id="node-2", contract_hash=HASH)
    changed = wrong_decision.model_copy(update={"config_hash": HASH})
    with pytest.raises(StaleRoutingDecision):
        accept(control, wrong_request, changed)
    assert len(store.read_stream("budget", "run-1")) == 1


def test_route_budget_exhaustion_leaves_lifecycle_budget_and_slots_unchanged(tmp_path):
    store = SQLiteEventStore(tmp_path / "budget-reject.db")
    reg, config, lifecycle, manifest = run_setup(store)
    request, decision = routed_pair(reg, config, manifest)
    assessment = decision.candidate_assessments[0].model_copy(
        update={"estimated_cost": CostEstimate(amount_minor=101, currency="USD", token_limit=10)}
    )
    decision = decision.model_copy(update={"candidate_assessments": (assessment,)})
    with pytest.raises(BudgetExhausted):
        accept(scheduler(store), request, decision)
    assert store.read_stream("budget", "run-1") == []
    assert store.read_stream("scheduler", "global") == []
    assert lifecycle.replay("run-1").node("node-1").status == "ready"


@pytest.mark.parametrize(
    ("scope", "message"),
    (("provider", "provider"), ("run", "Run"), ("tool", "tool")),
)
def test_provider_run_and_tool_concurrency_slots_are_bounded(tmp_path, scope, message):
    store = SQLiteEventStore(tmp_path / f"{scope}.db")
    nodes = (
        NodeSpec(
            node_id="node-1", role="coder", planning_contract_hash=HASH, tool_ids=("read_file",)
        ),
        NodeSpec(
            node_id="node-2", role="coder", planning_contract_hash=HASH, tool_ids=("read_file",)
        ),
    )
    reg, config, _, manifest = run_setup(store, nodes=nodes)
    control = scheduler(
        store,
        system=8,
        run=1 if scope == "run" else 8,
        provider=1 if scope == "provider" else 8,
        tool=1 if scope == "tool" else 8,
    )
    first_request, first_decision = routed_pair(reg, config, manifest, node_id="node-1")
    accept(control, first_request, first_decision)
    second_request, second_decision = routed_pair(reg, config, manifest, node_id="node-2")
    with pytest.raises(ConcurrencyLimitExceeded, match=message):
        accept(control, second_request, second_decision)
    assert len(store.read_stream("budget", "run-1")) == 1


def test_failure_after_nested_budget_reservation_rolls_back_every_stream(tmp_path, monkeypatch):
    store = SQLiteEventStore(tmp_path / "rollback.db")
    reg, config, lifecycle, manifest = run_setup(store)
    request, decision = routed_pair(reg, config, manifest)
    control = scheduler(store)
    original_append = store.append
    baseline_lifecycle = store.read_stream("run_lifecycle", "run-1")
    baseline_checkpoint = lifecycle.snapshots.load_valid("run_lifecycle", "run-1")
    assert baseline_checkpoint is not None

    def fail_scheduler_append(stream_type, *args, **kwargs):
        if stream_type == "scheduler":
            raise RuntimeError("injected scheduler append failure")
        return original_append(stream_type, *args, **kwargs)

    monkeypatch.setattr(store, "append", fail_scheduler_append)
    with pytest.raises(RuntimeError, match="injected"):
        accept(control, request, decision)
    assert store.read_stream("budget", "run-1") == []
    assert store.read_stream("scheduler", "global") == []
    assert store.read_stream("agent_registry", "run-1") == []
    assert store.read_stream("run_lifecycle", "run-1") == baseline_lifecycle
    unchanged_checkpoint = lifecycle.snapshots.load_valid("run_lifecycle", "run-1")
    assert unchanged_checkpoint == baseline_checkpoint
    monkeypatch.undo()
    assert LifecycleController(store).replay("run-1").node("node-1").status == "ready"


def test_route_acceptance_uses_cross_connection_cas_for_global_capacity(tmp_path):
    path = tmp_path / "parallel.db"
    seed = SQLiteEventStore(path)
    nodes = (
        NodeSpec(node_id="node-a", role="coder", planning_contract_hash=HASH),
        NodeSpec(node_id="node-b", role="coder", planning_contract_hash=HASH),
    )
    reg, config, _, manifest = run_setup(seed, nodes=nodes)
    pairs = (
        routed_pair(reg, config, manifest, node_id="node-a"),
        routed_pair(reg, config, manifest, node_id="node-b"),
    )
    seed.close()

    def attempt(pair):
        connection = SQLiteEventStore(path)
        try:
            try:
                return accept(scheduler(connection, system=1), *pair)
            except ConcurrencyLimitExceeded as error:
                return error
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(attempt, pairs))
    assert sum(isinstance(item, ConcurrencyLimitExceeded) for item in results) == 1
    check = SQLiteEventStore(path)
    assert len(check.read_stream("scheduler", "global")) == 1
    assert len(check.read_stream("budget", "run-1")) == 1


def test_lifecycle_guards_missing_snapshot_invalid_graph_and_transition_replays(tmp_path):
    store = SQLiteEventStore(tmp_path / "lifecycle-guards.db")
    controller = LifecycleController(store)
    with pytest.raises(LifecycleError, match="RunConfigSnapshot is missing"):
        controller.config_snapshot("missing")
    with pytest.raises(LifecycleError, match="not been initialized"):
        controller.replay("missing")

    run_setup(store, nodes=())
    assert controller.initialize_run("run-1").status == "created"
    with pytest.raises(ValueError, match="must not be empty"):
        controller.append_nodes(
            "run-1", (), expected_graph_version=0, idempotency_key="empty"
        )
    with pytest.raises(LifecycleError, match="initial graph"):
        controller.start_run("run-1")

    root = NodeSpec(node_id="root", role="planner", planning_contract_hash=HASH)
    controller.append_nodes(
        "run-1", (root,), expected_graph_version=0, idempotency_key="root"
    )
    with pytest.raises(LifecycleError, match="does not exist"):
        controller.append_nodes(
            "run-1",
            (NodeSpec(
                node_id="orphan", role="coder", planning_contract_hash=HASH,
                depends_on=("missing-parent",),
            ),),
            expected_graph_version=1,
            idempotency_key="orphan",
        )
    controller.start_run("run-1")
    with pytest.raises(ValueError, match="lowercase code"):
        controller.pause_run("run-1", reason_code="Not-Stable")
    paused = controller.pause_run("run-1", reason_code="user_request")
    assert controller.pause_run("run-1", reason_code="user_request") == paused
    with pytest.raises(LifecycleError, match="only a running Run"):
        controller.pause_run("run-1", reason_code="different_reason")
    with pytest.raises(LifecycleError, match="non-terminal Run"):
        controller.append_nodes(
            "run-1",
            (NodeSpec(node_id="paused-add", role="coder", planning_contract_hash=HASH),),
            expected_graph_version=1,
            idempotency_key="paused-add",
        )
    resumed = controller.resume_run("run-1")
    assert controller.resume_run("run-1") == resumed


def test_lifecycle_acceptance_rejects_invalid_attempt_status_and_fencing(tmp_path):
    store = SQLiteEventStore(tmp_path / "attempt-guards.db")
    run_setup(store)
    controller = LifecycleController(store)
    invalid = AttemptState(
        attempt_id="attempt-invalid",
        agent_instance_id="agent-invalid",
        fencing_generation=1,
        decision_hash=HASH,
        policy_manifest_hash=HASH,
        reasoning_effort="low",
        model_id="model-1",
        provider_id="primary",
        reservation_id="reservation-invalid",
        lease_expires_at=(NOW + timedelta(minutes=1)).isoformat(),
        status="succeeded",
    )
    with pytest.raises(LifecycleError, match="must start in accepted"):
        controller.record_attempt_accepted(
            "run-1", node_id="node-1", attempt=invalid, decision_hash=HASH,
            causation_id=HASH,
        )

    invalid = invalid.model_copy(update={"status": "accepted", "fencing_generation": 2})
    with pytest.raises(LifecycleConflict, match="not the next generation"):
        controller.record_attempt_accepted(
            "run-1", node_id="node-1", attempt=invalid, decision_hash=HASH,
            causation_id=HASH,
        )


def test_scheduler_rejects_invalid_acceptance_clock_and_changed_idempotent_replay(tmp_path):
    store = SQLiteEventStore(tmp_path / "acceptance-guards.db")
    reg, config, _, manifest = run_setup(store)
    request, decision = routed_pair(reg, config, manifest)
    control = scheduler(store)
    with pytest.raises(ValueError, match="UTC offset"):
        control.accept_routing(
            request, decision, accepted_at=datetime(2026, 9, 23, 12),
            lease_expires_at=NOW + timedelta(minutes=1),
        )
    with pytest.raises(SchedulerError, match="later than acceptance"):
        control.accept_routing(
            request, decision, accepted_at=NOW - timedelta(seconds=1),
            lease_expires_at=NOW + timedelta(minutes=1),
        )
    with pytest.raises(SchedulerError, match="lease must extend"):
        control.accept_routing(
            request, decision, accepted_at=NOW + timedelta(seconds=1),
            lease_expires_at=NOW + timedelta(seconds=1),
        )
    accepted = accept(control, request, decision)
    with pytest.raises(SchedulerError, match="idempotency key was reused"):
        control.accept_routing(
            request, decision, accepted_at=NOW + timedelta(seconds=2),
            lease_expires_at=NOW + timedelta(minutes=1),
        )
    assert accepted.reservation.status == "reserved"
    assert len(store.read_stream("scheduler", "global")) == 1


def test_scheduler_completion_and_reconciliation_require_durable_evidence(tmp_path):
    store = SQLiteEventStore(tmp_path / "completion-guards.db")
    reg, config, lifecycle, manifest = run_setup(store)
    request, decision = routed_pair(reg, config, manifest)
    control = scheduler(store)
    accepted = accept(control, request, decision)

    with pytest.raises(SchedulerError, match="needs usage or an explicit no-effect"):
        control.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
            outcome="succeeded",
        )
    evidence = UsageRecord(
        reservation_id=accepted.reservation.reservation_id,
        run_id="run-1", settlement_key="completion-evidence", currency="USD",
        input_tokens=1, output_tokens=1, cost_minor=1,
    )
    with pytest.raises(SchedulerError, match="mutually exclusive"):
        control.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
            outcome="failed", usage=evidence, known_no_effect=True,
        )
    with pytest.raises(SchedulerError, match="only for a failed attempt"):
        control.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
            outcome="succeeded", known_no_effect=True,
        )
    with pytest.raises(SchedulerError, match="unknown outcome cannot include"):
        control.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
            outcome="outcome_unknown", usage=evidence,
        )

    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
        outcome="outcome_unknown",
    )
    with pytest.raises(LifecycleConflict, match="OutcomeUnknown"):
        control.reconcile_attempt(
            run_id="run-1", node_id="node-1", attempt_id="attempt-never-accepted",
            fencing_generation=1, reconciled_at=NOW + timedelta(minutes=3),
            outcome="failed", known_no_effect=True,
        )

    marked_at = NOW + timedelta(seconds=5)
    with pytest.raises(LifecycleConflict, match="predates"):
        control.reconcile_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, reconciled_at=marked_at - timedelta(seconds=1),
            outcome="failed", known_no_effect=True,
        )
    with pytest.raises(SchedulerError, match="needs reported usage or no-effect"):
        control.reconcile_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, reconciled_at=NOW + timedelta(minutes=3),
            outcome="failed",
        )
    with pytest.raises(SchedulerError, match="mutually exclusive"):
        control.reconcile_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, reconciled_at=NOW + timedelta(minutes=3),
            outcome="failed", usage=evidence, known_no_effect=True,
        )
    with pytest.raises(SchedulerError, match="only for a failed attempt"):
        control.reconcile_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, reconciled_at=NOW + timedelta(minutes=3),
            outcome="succeeded", known_no_effect=True,
        )

    reconciled_at = NOW + timedelta(minutes=3)
    control.reconcile_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, reconciled_at=reconciled_at,
        outcome="failed", known_no_effect=True,
    )
    control.reconcile_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, reconciled_at=reconciled_at,
        outcome="failed", known_no_effect=True,
    )
    with pytest.raises(SchedulerError, match="idempotency key was reused"):
        control.reconcile_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=1, reconciled_at=reconciled_at + timedelta(seconds=1),
            outcome="failed", known_no_effect=True,
        )
    assert lifecycle.replay("run-1").node("node-1").status == "ready"


def test_cumulative_agent_limit_is_not_refunded_after_a_retry(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-total-limit.db")
    reg, config, _, manifest = run_setup(
        store,
        nodes=(NodeSpec(
            node_id="node-1", role="coder", planning_contract_hash=HASH, max_attempts=2
        ),),
        max_agents=1,
    )
    control = scheduler(store)
    first_request, first_decision = routed_pair(reg, config, manifest)
    first = accept(control, first_request, first_decision)
    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=first_request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=10),
        outcome="failed", known_no_effect=True,
    )
    before = store.read_stream("budget", "run-1")
    second_request, second_decision = routed_pair(reg, config, manifest, attempt=2)
    with pytest.raises(AgentLimitExceeded, match="cumulative Agent limit"):
        accept(control, second_request, second_decision)
    assert store.read_stream("budget", "run-1") == before
    assert control.agents.replay("run-1").total_created == 1
    assert control.agents.replay("run-1").for_attempt(first_request.attempt_id).status == "failed"
    assert first.agent_instance_id


def test_agent_depth_is_derived_from_frozen_parent_instance(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-depth-limit.db")
    parent_attempt_id = "attempt-node-1-1"
    parent_agent_id = AgentRegistry.agent_id_for_attempt("run-1", parent_attempt_id)
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH),
        NodeSpec(
            node_id="node-2", role="tester", planning_contract_hash=HASH,
            parent_agent_instance_id=parent_agent_id,
        ),
    )
    reg, config, _, manifest = run_setup(store, nodes=nodes, max_depth=0)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest, node_id="node-1")
    accepted = accept(control, request, decision)
    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=10),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=accepted.reservation.reservation_id,
            run_id="run-1", settlement_key="parent-success", currency="USD",
            input_tokens=1, output_tokens=1, cost_minor=1,
        ),
    )
    child_request, child_decision = routed_pair(
        reg, config, manifest, node_id="node-2", role="tester"
    )
    with pytest.raises(AgentDepthLimitExceeded, match="depth exceeds"):
        accept(control, child_request, child_decision)
    assert control.agents.replay("run-1").total_created == 1
    assert control.agents.replay("run-1").agent(parent_agent_id).depth == 0


def test_child_agent_records_parent_attempt_as_creator(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-parent-creator.db")
    parent_attempt_id = "attempt-node-1-1"
    parent_agent_id = AgentRegistry.agent_id_for_attempt("run-1", parent_attempt_id)
    nodes = (
        NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH),
        NodeSpec(
            node_id="node-2", role="tester", planning_contract_hash=HASH,
            parent_agent_instance_id=parent_agent_id,
        ),
    )
    reg, config, _, manifest = run_setup(store, nodes=nodes, max_depth=1)
    control = scheduler(store)
    parent_request, parent_decision = routed_pair(reg, config, manifest, node_id="node-1")
    parent = accept(control, parent_request, parent_decision)
    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=parent_request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=10),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=parent.reservation.reservation_id,
            run_id="run-1", settlement_key="parent-creator-success", currency="USD",
            input_tokens=1, output_tokens=1, cost_minor=1,
        ),
    )
    child_request, child_decision = routed_pair(
        reg, config, manifest, node_id="node-2", role="tester"
    )

    child = accept(control, child_request, child_decision)
    instance = control.agents.replay("run-1").agent(child.agent_instance_id)
    assert instance.parent_agent_instance_id == parent.agent_instance_id
    assert instance.created_by_attempt_id == parent_request.attempt_id
    assert instance.depth == 1


def test_agent_registry_replay_and_transition_idempotency_are_fenced(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-replay.db")
    reg, config, lifecycle, manifest = run_setup(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, manifest)
    accepted = accept(control, request, decision)
    node_spec = lifecycle.replay("run-1").node("node-1").spec
    attempt = lifecycle.replay("run-1").node("node-1").attempts[0]
    with pytest.raises(AgentRegistryError, match="only an accepted model attempt"):
        control.agents.register_attempt(
            "run-1", node=node_spec, attempt=attempt.model_copy(update={"status": "succeeded"})
        )
    with pytest.raises(AgentRegistryError, match="deterministic attempt binding"):
        control.agents.register_attempt(
            "run-1", node=node_spec,
            attempt=attempt.model_copy(update={"agent_instance_id": "agent-forged"}),
        )
    assert control.agents.register_attempt("run-1", node=node_spec, attempt=attempt).agent_instance_id == accepted.agent_instance_id
    with pytest.raises(LifecycleConflict, match="stale or not in an allowed state"):
        control.agents.complete_attempt(
            "run-1", node_id="node-1", attempt_id=request.attempt_id,
            agent_instance_id=accepted.agent_instance_id, fencing_generation=2,
            causation_id=decision.decision_hash, outcome="succeeded",
        )

    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=10),
        outcome="succeeded",
        usage=UsageRecord(
            reservation_id=accepted.reservation.reservation_id,
            run_id="run-1", settlement_key="agent-replay-success", currency="USD",
            input_tokens=1, output_tokens=1, cost_minor=1,
        ),
    )
    control.agents.complete_attempt(
        "run-1", node_id="node-1", attempt_id=request.attempt_id,
        agent_instance_id=accepted.agent_instance_id, fencing_generation=1,
        causation_id=decision.decision_hash, outcome="succeeded",
    )
    with pytest.raises(LifecycleConflict, match="idempotency key was reused"):
        control.agents.complete_attempt(
            "run-1", node_id="node-1", attempt_id=request.attempt_id,
            agent_instance_id=accepted.agent_instance_id, fencing_generation=1,
            causation_id="sha256:" + "a" * 64, outcome="succeeded",
        )
    state = control.agents.replay("run-1")
    assert state.total_created == 1
    assert state.active_count == 0
    with pytest.raises(KeyError):
        state.agent("not-an-agent")
    with pytest.raises(KeyError):
        state.for_attempt("not-an-attempt")


def test_agent_concurrency_cap_counts_unknown_and_active_instances(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-active-limit.db")
    reg, config, lifecycle, manifest = run_setup(
        store,
        nodes=(
            NodeSpec(node_id="node-1", role="coder", planning_contract_hash=HASH),
            NodeSpec(node_id="node-2", role="coder", planning_contract_hash=HASH),
        ),
        max_concurrency=1,
    )
    control = scheduler(store, system=8, run=8, provider=8, tool=8)
    first_request, first_decision = routed_pair(reg, config, manifest, node_id="node-1")
    accepted = accept(control, first_request, first_decision)
    second_request, second_decision = routed_pair(reg, config, manifest, node_id="node-2")
    second_attempt = AttemptState(
        attempt_id=second_request.attempt_id,
        agent_instance_id=AgentRegistry.agent_id_for_attempt("run-1", second_request.attempt_id),
        fencing_generation=1,
        decision_hash=second_decision.decision_hash,
        policy_manifest_hash=second_request.policy_manifest_hash,
        reasoning_effort=second_decision.reasoning_effort,
        model_id="model-1",
        provider_id="primary",
        reservation_id="reservation-manual-agent-cap",
        lease_expires_at=(NOW + timedelta(minutes=1)).isoformat(),
        status="accepted",
    )
    lifecycle.record_attempt_accepted(
        "run-1", node_id="node-2", attempt=second_attempt,
        decision_hash=second_decision.decision_hash,
        causation_id=second_decision.decision_hash,
    )
    with pytest.raises(AgentConcurrencyLimitExceeded, match="active/unknown Agent limit"):
        control.agents.register_attempt(
            "run-1", node=lifecycle.replay("run-1").node("node-2").spec,
            attempt=second_attempt,
        )
    state = control.agents.replay("run-1")
    assert state.total_created == state.active_count == 1
    assert state.for_attempt(first_request.attempt_id).agent_instance_id == accepted.agent_instance_id


def test_agent_registry_rejects_missing_and_unfinished_parents_atomically(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-parent-checks.db")
    expected_parent_id = AgentRegistry.agent_id_for_attempt(
        "run-1", "attempt-parent-1"
    )
    reg, config, lifecycle, manifest = run_setup(
        store,
        nodes=(
            NodeSpec(node_id="parent", role="coder", planning_contract_hash=HASH),
            NodeSpec(
                node_id="missing-parent-child", role="tester", planning_contract_hash=HASH,
                parent_agent_instance_id="agent-does-not-exist",
            ),
            NodeSpec(
                node_id="active-parent-child", role="tester", planning_contract_hash=HASH,
                parent_agent_instance_id=expected_parent_id,
            ),
        ),
    )
    control = scheduler(store)
    parent_request, parent_decision = routed_pair(
        reg, config, manifest, node_id="parent"
    )
    accept(control, parent_request, parent_decision)
    child_request, child_decision = routed_pair(
        reg, config, manifest, node_id="missing-parent-child", role="tester"
    )
    budget_before = store.read_stream("budget", "run-1")
    with pytest.raises(AgentRegistryError, match="parent Agent instance does not exist"):
        accept(control, child_request, child_decision)
    assert store.read_stream("budget", "run-1") == budget_before
    assert lifecycle.replay("run-1").node("missing-parent-child").status == "ready"

    active_child_request, active_child_decision = routed_pair(
        reg, config, manifest, node_id="active-parent-child", role="tester"
    )
    with pytest.raises(AgentRegistryError, match="completed parent attempt"):
        accept(control, active_child_request, active_child_decision)
    assert lifecycle.replay("run-1").node("active-parent-child").status == "ready"
    assert control.agents.replay("run-1").total_created == 1


def test_lifecycle_boundary_models_reject_unstable_collections_and_ids():
    with pytest.raises(ValueError, match="depends_on must be an array"):
        NodeSpec.model_validate({
            "node_id": "node", "role": "coder", "planning_contract_hash": HASH,
            "depends_on": "not-an-array",
        })
    with pytest.raises(ValueError, match="unique stable node IDs"):
        NodeSpec(
            node_id="node", role="coder", planning_contract_hash=HASH,
            depends_on=("parent", "parent"),
        )
    with pytest.raises(ValueError, match="stable agent ID"):
        NodeSpec(
            node_id="node", role="coder", planning_contract_hash=HASH,
            parent_agent_instance_id="bad agent id",
        )
    with pytest.raises(ValueError, match="tool_ids must be an array"):
        NodeSpec.model_validate({
            "node_id": "node", "role": "coder", "planning_contract_hash": HASH,
            "tool_ids": "not-an-array",
        })
    with pytest.raises(ValueError, match="unique stable identifiers"):
        NodeSpec(
            node_id="node", role="coder", planning_contract_hash=HASH,
            tool_ids=("read_file", "read_file"),
        )
    with pytest.raises(ValueError, match="ISO-8601"):
        AttemptState(
            attempt_id="attempt", agent_instance_id="agent", fencing_generation=1,
            decision_hash=HASH, policy_manifest_hash=HASH, reasoning_effort="low", model_id="model",
            provider_id="provider", reservation_id="reservation", lease_expires_at="later",
            status="accepted",
        )
    with pytest.raises(ValueError, match="UTC offset"):
        AttemptState(
            attempt_id="attempt", agent_instance_id="agent", fencing_generation=1,
            decision_hash=HASH, policy_manifest_hash=HASH, reasoning_effort="low", model_id="model",
            provider_id="provider", reservation_id="reservation",
            lease_expires_at="2026-09-23T12:00:00", status="accepted",
        )
    with pytest.raises(ValueError, match="must be an array"):
        NodeState(
            spec=NodeSpec(node_id="node", role="coder", planning_contract_hash=HASH),
            status="ready", attempts="not-an-array",
        )
    with pytest.raises(ValueError, match="must be an array"):
        RunLifecycleState(
            run_id="run", status="created", config_hash=HASH, registry_hash=HASH,
            max_nodes=1, max_depth=0, graph_version=0, event_version=1,
            nodes="not-an-array",
        )
    with pytest.raises(ValueError):
        AgentRegistryLimits(max_total_agents=1, max_depth=-1, max_concurrent_agents=1)
    with pytest.raises(ValueError):
        AgentInstance(
            agent_instance_id="bad id", run_id="run", node_id="node",
            attempt_id="attempt", created_by_attempt_id="attempt", depth=0,
            role="coder", model_id="model", provider_id="provider",
            decision_hash=HASH, policy_manifest_hash=HASH, reasoning_effort="low", fencing_generation=1,
            status="created",
        )


def _append_agent_event(
    store, run_id, event_type, payload, *, key, event_run_id=None,
    node_id="node", attempt_id="attempt",
):
    current = store.read_stream("agent_registry", run_id)
    event = EventDraft(
        event_type,
        payload,
        run_id=run_id if event_run_id is None else event_run_id,
        node_id=node_id,
        attempt_id=attempt_id,
        fencing_generation=1,
        correlation_id=run_id,
        causation_id=HASH,
    )
    store.append("agent_registry", run_id, len(current), [event], key)


def _created_agent_payload():
    return {
        "instance": AgentInstance(
            agent_instance_id="agent-1",
            run_id="run-1",
            node_id="node",
            attempt_id="attempt",
            created_by_attempt_id="attempt",
            depth=0,
            role="coder",
            model_id="model-1",
            provider_id="primary",
            decision_hash=HASH,
            policy_manifest_hash=HASH,
            reasoning_effort="low",
            fencing_generation=1,
            status="created",
        ).model_dump(mode="json")
    }


def test_agent_registry_replay_rejects_corrupt_event_identity_and_duplicate_ids(tmp_path):
    store = SQLiteEventStore(tmp_path / "agent-corrupt-events.db")
    _append_agent_event(
        store, "run-1", "AgentInstanceCreated", _created_agent_payload(),
        key="create-1", event_run_id="run-other",
    )
    with pytest.raises(AgentRegistryError, match="identity does not match"):
        reduce_agent_registry("run-1", store.read_stream("agent_registry", "run-1"))

    duplicate_store = SQLiteEventStore(tmp_path / "agent-duplicate-events.db")
    payload = _created_agent_payload()
    _append_agent_event(duplicate_store, "run-1", "AgentInstanceCreated", payload, key="create-1")
    _append_agent_event(duplicate_store, "run-1", "AgentInstanceCreated", payload, key="create-2")
    with pytest.raises(AgentRegistryError, match="duplicated"):
        reduce_agent_registry("run-1", duplicate_store.read_stream("agent_registry", "run-1"))


@pytest.mark.parametrize(
    ("parent_status", "child_depth", "child_creator", "message"),
    (
        ("active", 1, "attempt", "completed parent instance"),
        ("completed", 2, "attempt", "depth or creator context"),
        ("completed", 1, "forged-creator", "depth or creator context"),
    ),
)
def test_agent_registry_replay_revalidates_parent_depth_and_creator(
    tmp_path, parent_status, child_depth, child_creator, message
):
    store = SQLiteEventStore(tmp_path / f"agent-parent-replay-{parent_status}-{child_depth}.db")
    _append_agent_event(store, "run-1", "AgentInstanceCreated", _created_agent_payload(), key="root")
    _append_agent_event(
        store, "run-1", "AgentStarted", {"agent_instance_id": "agent-1"}, key="root-start"
    )
    if parent_status == "completed":
        _append_agent_event(
            store, "run-1", "AgentCompleted",
            {"agent_instance_id": "agent-1", "outcome": "succeeded"}, key="root-finish",
        )
    child = AgentInstance(
        agent_instance_id="agent-2", run_id="run-1", node_id="child-node",
        attempt_id="child-attempt", created_by_attempt_id=child_creator,
        parent_agent_instance_id="agent-1", depth=child_depth, role="tester",
        model_id="model-2", provider_id="primary", decision_hash=HASH,
        policy_manifest_hash=HASH, reasoning_effort="low", fencing_generation=1, status="created",
    )
    _append_agent_event(
        store, "run-1", "AgentInstanceCreated", {"instance": child.model_dump(mode="json")},
        key="child", node_id="child-node", attempt_id="child-attempt",
    )
    with pytest.raises(AgentRegistryError, match=message):
        reduce_agent_registry("run-1", store.read_stream("agent_registry", "run-1"))


@pytest.mark.parametrize(
    ("events", "message"),
    (
        ((("AgentStarted", {"agent_instance_id": "agent-1"}),), "unknown instance"),
        (
            (
                ("AgentInstanceCreated", _created_agent_payload()),
                ("AgentCompleted", {"agent_instance_id": "agent-1", "outcome": "failed"}),
            ),
            "must record a succeeded",
        ),
        (
            (
                ("AgentInstanceCreated", _created_agent_payload()),
                ("AgentStarted", {"agent_instance_id": "agent-1"}),
                ("AgentFailed", {"agent_instance_id": "agent-1", "outcome": "succeeded"}),
            ),
            "must record a failed",
        ),
        (
            (
                ("AgentInstanceCreated", _created_agent_payload()),
                ("AgentStarted", {"agent_instance_id": "agent-1"}),
                ("AgentOutcomeUnknown", {"agent_instance_id": "agent-1"}),
                ("AgentReconciled", {"agent_instance_id": "agent-1", "outcome": "unknown"}),
            ),
            "must resolve an unknown",
        ),
        (
            (
                ("AgentInstanceCreated", _created_agent_payload()),
                ("AgentStarted", {"agent_instance_id": "agent-1"}),
                ("UnexpectedAgentEvent", {"agent_instance_id": "agent-1"}),
            ),
            "unsupported Agent registry event",
        ),
        (
            (
                ("AgentInstanceCreated", _created_agent_payload()),
                ("AgentCancelled", {"agent_instance_id": "agent-1", "outcome": "cancelled"}),
            ),
            None,
        ),
    ),
)
def test_agent_registry_replay_enforces_instance_state_machine(tmp_path, events, message):
    store = SQLiteEventStore(tmp_path / f"agent-state-machine-{len(events)}.db")
    for index, (event_type, payload) in enumerate(events):
        _append_agent_event(store, "run-1", event_type, payload, key=f"event-{index}")
    if message is None:
        assert reduce_agent_registry(
            "run-1", store.read_stream("agent_registry", "run-1")
        ).agent("agent-1").status == "cancelled"
    else:
        with pytest.raises(AgentRegistryError, match=message):
            reduce_agent_registry("run-1", store.read_stream("agent_registry", "run-1"))
