from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest

from orchestrator.budget import BudgetExhausted, BudgetLedger, CostEstimate, UsageRecord
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
from orchestrator.lifecycle.models import AttemptState
from orchestrator.persistence import SQLiteEventStore
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


def effective_config(reg):
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
            "max_agents": 8,
            "max_depth": 4,
            "max_concurrency": 4,
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
                "max_agents": 8,
                "max_depth": 4,
                "max_concurrency": 4,
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


def run_setup(store, *, run_id="run-1", nodes=None):
    reg = registry()
    resolved = effective_config(reg)
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
    assert accepted.reservation.reserved_minor == 20
    assert lifecycle.replay("run-1").node("node-1").status == "running"
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
    retry_request, retry_decision = routed_pair(reg, config, manifest, node_id="node-1", attempt=2)
    assert accept(control, retry_request, retry_decision).accepted_route.fencing_generation == 2
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
    reg, config, _, manifest = run_setup(store)
    request, decision = routed_pair(reg, config, manifest)
    control = scheduler(store)
    original_append = store.append

    def fail_lifecycle_append(stream_type, *args, **kwargs):
        if stream_type == "run_lifecycle":
            raise RuntimeError("injected lifecycle append failure")
        return original_append(stream_type, *args, **kwargs)

    monkeypatch.setattr(store, "append", fail_lifecycle_append)
    with pytest.raises(RuntimeError, match="injected"):
        accept(control, request, decision)
    assert store.read_stream("budget", "run-1") == []
    assert store.read_stream("scheduler", "global") == []
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
        fencing_generation=1,
        decision_hash=HASH,
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
