"""Real SQLite histories for explicit recovery continuation tests."""

from datetime import timedelta
from types import SimpleNamespace

from orchestrator.budget import BudgetLedger
from orchestrator.models import ModelGatewayFailure
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.unit.lifecycle.test_scheduler import (
    NOW, _resolver_durable_state, routed_pair, run_setup_with_frozen_contract, scheduler,
)


def recovery_case(store, *, failure_code="rate_limited", unknown=False):
    reg, config, lifecycle, policy, contract = run_setup_with_frozen_contract(store)
    control = scheduler(store)
    request, decision = routed_pair(reg, config, policy, contract_hash=contract.contract_hash)
    accepted = WorkerInputService(control).accept_routing_with_input(
        request, decision, task_text="implement a small change",
        accepted_at=NOW + timedelta(seconds=1), lease_expires_at=NOW + timedelta(minutes=1),
    )
    control.finish_attempt(
        run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
        fencing_generation=1, completed_at=NOW + timedelta(seconds=5),
        outcome="outcome_unknown" if unknown else "failed", known_no_effect=not unknown,
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
