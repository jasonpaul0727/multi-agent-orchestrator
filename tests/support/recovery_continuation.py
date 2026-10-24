"""Real SQLite histories for explicit recovery continuation tests."""

from datetime import timedelta
import hashlib
import json
from types import SimpleNamespace

from orchestrator.budget import BudgetLedger, UsageRecord
from orchestrator.models import ModelGatewayFailure
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.unit.lifecycle.test_scheduler import (
    NOW, _fixture_contract, _resolver_durable_state, effective_config, registry,
    routed_pair, run_setup_with_frozen_contract, scheduler,
)


def recovery_case(store, *, failure_code="rate_limited", unknown=False,
                  legacy_input=False, escalation_role=None, accounted_usage=False):
    if escalation_role is None:
        reg, config, lifecycle, policy, contract = run_setup_with_frozen_contract(store)
    else:
        reg, config, lifecycle, policy, contract = _role_recovery_setup(store, escalation_role)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, policy, contract_hash=contract.contract_hash)
    times = dict(accepted_at=NOW + timedelta(seconds=1), lease_expires_at=NOW + timedelta(minutes=1))
    if legacy_input:
        accepted = control.accept_routing(request, decision, **times)
    else:
        accepted = WorkerInputService(control).accept_routing_with_input(
            request, decision, task_text="implement a small change", **times,
        )
    usage = UsageRecord(run_id="run-1", reservation_id=accepted.reservation.reservation_id,
                        settlement_key="accounted-provider-usage", currency="USD", cost_minor=1) if accounted_usage else None
    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
        outcome="outcome_unknown" if unknown else "failed",
        known_no_effect=not unknown and usage is None, usage=usage,
    )
    plan = control.plan_attempt_recovery(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, source_decision=decision, contract=contract,
        failure=ModelGatewayFailure(
            code=failure_code, phase="provider", outcome="unknown" if unknown else "known_failure",
            retryable=not unknown, http_status=None if unknown else 429,
        ),
    )
    return SimpleNamespace(
        store=store, scheduler=control, registry=reg, config=config, lifecycle=lifecycle,
        policy=policy, contract=contract, request=request, decision=decision,
        accepted=accepted, plan=plan,
    )


def durable_state(store):
    """Include all SQLite rows plus public resource projections."""
    control = scheduler(store)
    recovered = control.recovery.recover("run-1")
    return (
        tuple(store._connection.iterdump()), _resolver_durable_state(store),
        BudgetLedger(store).available("run-1"), control.agents.replay("run-1"),
        recovered.active_attempts,
    )


def consume_recovery(case, *, source_decision=None, plan=None, retry_level=1,
                     exhausted_model_ids=()):
    """Admit a real successor through the persisted Scheduler authorization."""
    source_decision = case.decision if source_decision is None else source_decision
    plan = case.plan if plan is None else plan
    request, decision = routed_pair(
        case.registry, case.config, case.policy,
        attempt=source_decision.fencing_generation + 1, contract_hash=case.contract.contract_hash,
        recovery_action=plan.action, recovery_authorization=plan.authorization,
        prior_decision_hash=source_decision.decision_hash, previous_model_id="model-1",
        failure_category="transient", retry_level=retry_level,
        exhausted_model_ids=exhausted_model_ids,
    )
    accepted = WorkerInputService(case.scheduler).accept_routing_with_input(
        request, decision, task_text="implement a small change",
        accepted_at=NOW + timedelta(seconds=6 + request.fencing_generation),
        lease_expires_at=NOW + timedelta(minutes=2),
    )
    return request, decision, accepted


def _role_recovery_setup(store, role):
    from orchestrator.config import ConfigCandidate, ConfigManager, EffectiveConfig, resolve_effective_config
    from orchestrator.lifecycle import LifecycleController, NodeSpec
    from orchestrator.security import PolicyAuthority, PolicyManifest

    reg = registry()
    values = effective_config(reg).config.model_dump(mode="json")
    for preset in values["presets"].values():
        preset["roles"]["coder"]["escalate_to"] = role
    resolved = resolve_effective_config(EffectiveConfig.model_validate(values), registry=reg)
    policy = PolicyManifest(authorities=(PolicyAuthority(
        source="system", max_permission="read-only", allowed_actions={"model_invoke"},
        allowed_tools={"model:model-1"},
    ),))
    contract = _fixture_contract(reg, resolved.config, policy)
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run("run-1", store)
    lifecycle = LifecycleController(store)
    lifecycle.initialize_run("run-1")
    lifecycle.append_nodes("run-1", (NodeSpec(
        node_id="node-1", role="coder", planning_contract_hash=contract.contract_hash,
        planning_contract=contract, max_attempts=3,
    ),), expected_graph_version=0, idempotency_key="initial-graph", policy_manifest=policy)
    lifecycle.start_run("run-1")
    return reg, resolved.config, lifecycle, policy, contract


def rewrite_event_payload(store, event_type, mutate, *, attempt_id=None, stream_type="scheduler",
                          stream_id="global"):
    """Inject hostile serialized history without weakening production write APIs."""
    event = next(event for event in store.read_stream(stream_type, stream_id)
                 if event.event_type == event_type
                 and (attempt_id is None or event.attempt_id == attempt_id))
    payload = json.loads(json.dumps(event.payload))
    mutate(payload)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    trigger = store._connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'"
    ).fetchone()[0]
    store._connection.execute("DROP TRIGGER events_immutable_update")
    try:
        store._connection.execute(
            "UPDATE events SET payload_json=?, payload_hash=? WHERE event_id=?",
            (encoded, hashlib.sha256(encoded.encode()).hexdigest(), event.event_id),
        )
    finally:
        store._connection.execute(trigger)


def duplicate_event(store, event_type):
    """Clone a corrupt row directly, preserving valid JSON hashes and stream numbering."""
    row = store._connection.execute(
        "SELECT * FROM events WHERE event_type=? ORDER BY stream_version LIMIT 1", (event_type,),
    ).fetchone()
    values = dict(row)
    values["event_id"] = "corrupt-duplicate-event"
    version = store._connection.execute(
        "SELECT current_version FROM stream_versions WHERE stream_type=? AND stream_id=?",
        (values["stream_type"], values["stream_id"]),
    ).fetchone()[0] + 1
    values["stream_version"] = version
    names = ",".join(values)
    parameters = ",".join("?" for _ in values)
    store._connection.execute(f"INSERT INTO events ({names}) VALUES ({parameters})", tuple(values.values()))
    store._connection.execute(
        "UPDATE stream_versions SET current_version=? WHERE stream_type=? AND stream_id=?",
        (version, values["stream_type"], values["stream_id"]),
    )
