from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
from threading import Barrier

import pytest

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
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.routing import CandidateAssessment, RoutingDecision, RoutingRequest
from orchestrator.scheduler import (
    ConcurrencyLimitExceeded,
    ConcurrencyLimits,
    Scheduler,
    SchedulerError,
    StaleRoutingDecision,
)
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


def scheduler(store, *, system=8, run=8, provider=8, tool=8):
    return Scheduler(
        store,
        limits=ConcurrencyLimits(
            system_active_attempts=system,
            run_active_attempts=run,
            provider_active_attempts=provider,
            tool_active_attempts=tool,
        ),
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
