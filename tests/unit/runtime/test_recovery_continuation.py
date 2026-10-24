"""Atomic admission consumes durable recovery authority with explicit input."""

from datetime import timedelta
import hashlib
import json
import traceback

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.support.recovery_continuation import (
    consume_recovery, durable_state, recovery_case, rewrite_event_payload, routing_observations,
)
from tests.unit.lifecycle.test_scheduler import NOW, routed_pair, scheduler


TEXT = "implement a small change"


def continuation(case):
    # Import inside the executing test so absence is a behavioral RED.
    from orchestrator.runtime import recovery_continuation
    return recovery_continuation.RecoveryContinuationService(
        case.scheduler, WorkerInputService(case.scheduler),
    )


def admit(case, **changes):
    from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
    values = dict(
        run_id="run-1", node_id="node-1", source_attempt_id="attempt-node-1-1",
        source_fencing_generation=1, task_text=TEXT,
        routing_inputs=RecoveryRoutingInputs(**routing_observations(case)),
        accepted_at=NOW + timedelta(seconds=7), lease_expires_at=NOW + timedelta(minutes=2),
    )
    values.update(changes)
    return continuation(case).admit(**values)


def test_known_failure_admits_derived_successor_once(tmp_path):
    # Catches duplicate admission/holds, synthetic routes, wrong IDs or prompt persistence.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        original = tuple(store.read_stream("scheduler", "global"))
        result = admit(case)
        assert result.status == "admitted" and result.task is None
        route = result.accepted_route
        digest = hashlib.sha256(json.dumps({
            "schema": "maestro.recovery-successor.v1", "run_id": "run-1", "node_id": "node-1",
            "source_attempt_id": "attempt-node-1-1", "source_fencing_generation": 1,
            "authorization_hash": case.plan.authorization.authorization_hash,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        assert route.attempt_id == "recovery-attempt-" + digest
        assert (route.fencing_generation, route.model_id, route.provider_id) == (2, "model-1", "primary")
        events = store.read_stream("scheduler", "global")
        assert tuple(events[:len(original)]) == original
        assert len(events) == len(original) + 1
        successor = events[-1]
        assert successor.event_type == "RoutingDecisionAccepted"
        assert successor.idempotency_key == "accept:recovery-request-" + digest
        assert successor.payload["accepted_route"] == route.model_dump(mode="json")
        assert successor.payload["recovery_authorization_hash"] == case.plan.authorization.authorization_hash
        assert successor.payload["verification_admission"] == original[0].payload["verification_admission"]
        assert (successor.payload["retry_level"], successor.payload["exhausted_model_ids"],
                successor.payload["failure_category"]) == (1, [], "transient")
        assert len([e for e in store.read_stream("budget", "run-1") if e.event_type == "BudgetReserved"]) == 2
        agents = case.scheduler.agents.replay("run-1")
        assert (agents.total_created, agents.active_count) == (2, 1)
        active, = case.scheduler.recovery.recover("run-1").active_attempts
        assert active.attempt_id == route.attempt_id
        before = durable_state(store)
        repeated = admit(case)
        assert repeated.status == "already_admitted" and repeated.accepted_route == route
        assert durable_state(store) == before
        assert TEXT not in repr(result) and TEXT not in "\n".join(store._connection.iterdump())


@pytest.mark.parametrize("text", [TEXT + " ", "implement a different change", " ", "\ud800"],
                         ids=["trailing-space", "different", "blank", "surrogate"])
def test_changed_input_cannot_admit(tmp_path, text):
    # Catches normalization or classification standing in for exact source input equality.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, task_text=text)
        assert caught.value.code in {"recovery_input_invalid", "recovery_input_mismatch"}
        assert durable_state(store) == before
        assert "implement a different change" not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("missing,status", [("task_text", "waiting_for_input"), ("routing_inputs", "waiting_for_routing")])
def test_missing_input_or_routing_waits_without_effects(tmp_path, missing, status):
    # Catches acquiring a hold before all explicit admission inputs arrive.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        result = admit(case, **{missing: None})
        assert result.status == status and result.task is None and result.accepted_route is None
        assert durable_state(store) == before


@pytest.mark.parametrize("field,value", [
    ("retry_level", True), ("exhausted_model_ids", ["model-1", "model-1"]),
    ("verification_context", None), ("graph_version", 99),
])
def test_corrupt_source_counters_or_frozen_binding_cannot_admit(tmp_path, field, value):
    # Catches trusting a source without Task 1's exact historical validation.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        rewrite_event_payload(store, "RoutingDecisionAccepted", lambda p: p.update({field: value}))
        before = durable_state(store)
        with pytest.raises(RecoveryContinuationError):
            admit(case)
        assert durable_state(store) == before


def test_nested_acceptance_failure_rolls_back_all_resources(tmp_path, monkeypatch):
    # Catches the outer snapshot failing to roll back nested Scheduler writes.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True)
        before = durable_state(store)
        register = case.scheduler.agents.register_attempt

        def fail_after_register(*args, **kwargs):
            register(*args, **kwargs)
            raise RuntimeError("opaque failure")

        monkeypatch.setattr(case.scheduler.agents, "register_attempt", fail_after_register)
        with pytest.raises(RecoveryContinuationError):
            admit(case)
        assert durable_state(store) == before


@pytest.mark.parametrize("code,escalate,action,model", [
    ("provider_unavailable", None, "same_tier_fallback", "model-2"),
    ("context_length_exceeded", "tier:standard", "capability_escalation", "model-3"),
])
def test_real_router_admits_fallback_and_escalation(tmp_path, code, escalate, action, model):
    # Catches reusing the failed model or ignoring the persisted authorized candidates.
    with SQLiteEventStore(tmp_path / "recovery.db") as store:
        case = recovery_case(store, routing_budget=True, multi_model=True,
                             failure_code=code, escalation_role=escalate)
        assert case.decision.selected_model_id == "model-1"
        assert len(case.decision.candidate_assessments) == (3 if escalate is None else 2)
        assert case.plan.action == action
        source_events = tuple(store.read_stream("scheduler", "global"))
        result = admit(case)
        assert (result.status, result.accepted_route.model_id) == ("admitted", model)
        event = store.read_stream("scheduler", "global")[-1]
        assert event.payload["recovery_action"] == action
        assert event.payload["prior_decision_hash"] == case.decision.decision_hash
        assert event.payload["recovery_authorization_hash"] == case.plan.authorization.authorization_hash
        assert tuple(store.read_stream("scheduler", "global")[:-1]) == source_events
        assert case.scheduler.agents.replay("run-1").total_created == 2
        before = durable_state(store)
        assert admit(case, routing_inputs=None).accepted_route == result.accepted_route
        assert durable_state(store) == before


@pytest.mark.parametrize("legacy", [False, True], ids=["derived", "existing-scheduler-api"])
def test_committed_response_loss_replays_exact_identity_before_routing(tmp_path, monkeypatch, legacy):
    # Catches duplicate holds after lost response, new observations/time or legacy consumed IDs.
    database = tmp_path / "replay.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store, routing_budget=True)
        if legacy:
            _, _, accepted = consume_recovery(case)
            route = accepted.accepted_route
        else:
            append = store.append_checked

            def lose_outer_response(stream_type, stream_id, key, decide):
                result = append(stream_type, stream_id, key, decide)
                if key == "recovery-continuation-snapshot":
                    raise RuntimeError("response lost after commit")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(store, "append_checked", lose_outer_response)
                with pytest.raises(RecoveryContinuationError):
                    admit(case)
            from orchestrator.models import AcceptedModelRoute
            route = AcceptedModelRoute.model_validate(
                store.read_stream("scheduler", "global")[-1].payload["accepted_route"],
            )
        before = durable_state(store)
    with SQLiteEventStore(database) as reopened:
        case.store, case.scheduler = reopened, scheduler(reopened)
        from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
        observations = routing_observations(case)
        observations["eligibility"] = observations["eligibility"].model_copy(update={
            "available_tokens": 0, "authorization_revoked": True,
        })
        for routing in (None, RecoveryRoutingInputs(**observations)):
            result = admit(case, routing_inputs=routing, accepted_at=NOW + timedelta(seconds=20),
                           lease_expires_at=NOW + timedelta(minutes=4))
            assert result.status == "already_admitted" and result.accepted_route == route
            assert result.task is None and result.opportunity.successor == route
            assert durable_state(reopened) == before
        waiting = admit(case, task_text=None, routing_inputs=None)
        assert waiting.status == "waiting_for_input" and waiting.accepted_route == route
        with pytest.raises(RecoveryContinuationError) as caught:
            admit(case, task_text="different explicit input", routing_inputs=None)
        assert caught.value.code == "recovery_input_mismatch"
        assert durable_state(reopened) == before


@pytest.mark.parametrize("state", ["failed", "paused", "cancelling", "expired", "unknown", "newer"])
def test_consumed_inactive_successor_keeps_identity_without_stale_authority(tmp_path, state):
    # Catches historical consumption being returned as current active authority.
    with SQLiteEventStore(tmp_path / "inactive.db") as store:
        case = recovery_case(store, routing_budget=True)
        _, _, accepted = consume_recovery(case)
        as_of = NOW + timedelta(seconds=20)
        if state in {"failed", "unknown", "newer"}:
            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=accepted.accepted_route.attempt_id,
                fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
                outcome="outcome_unknown" if state == "unknown" else "failed",
                known_no_effect=state != "unknown",
            )
        if state == "paused":
            case.lifecycle.pause_run("run-1", reason_code="user_request")
        elif state == "cancelling":
            case.lifecycle.request_cancel("run-1", reason_code="user_request")
        elif state == "expired":
            as_of = NOW + timedelta(minutes=2)
        elif state == "newer":
            request, decision = routed_pair(case.registry, case.config, case.policy, attempt=3,
                                            contract_hash=case.contract.contract_hash)
            WorkerInputService(case.scheduler).accept_routing_with_input(
                request, decision, task_text=TEXT, accepted_at=NOW + timedelta(seconds=15),
                lease_expires_at=NOW + timedelta(minutes=3),
            )
        before = durable_state(store)
        result = admit(case, routing_inputs=None, accepted_at=as_of,
                       lease_expires_at=NOW + timedelta(minutes=4))
        assert result.status == "successor_inactive"
        assert result.accepted_route == accepted.accepted_route and result.task is None
        assert result.opportunity.successor == accepted.accepted_route
        assert durable_state(store) == before


def test_later_same_model_retry_preserves_exhausted_model_safety_boundary(tmp_path):
    # Catches bypassing Router's conservative exhausted-model rule to force a retry.
    from orchestrator.models import ModelGatewayFailure
    from orchestrator.runtime.recovery_opportunities import RecoveryOpportunityService

    with SQLiteEventStore(tmp_path / "retry.db") as store:
        case = recovery_case(store, routing_budget=True)
        request, decision, _ = consume_recovery(case)
        case.scheduler.finish_attempt(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
            outcome="failed", known_no_effect=True,
        )
        plan = case.scheduler.plan_attempt_recovery(
            run_id="run-1", node_id="node-1", attempt_id=request.attempt_id,
            fencing_generation=2, source_decision=decision, contract=case.contract,
            failure=ModelGatewayFailure(code="rate_limited", phase="provider",
                                       outcome="known_failure", retryable=True, http_status=429),
        )
        assert plan.action == "same_model_retry"
        latest = RecoveryOpportunityService(case.scheduler).list_opportunities(run_id="run-1")[-1]
        assert latest.status == "ready"
        before = durable_state(store)
        result = admit(case, source_attempt_id=request.attempt_id, source_fencing_generation=2,
                       accepted_at=NOW + timedelta(seconds=15))
        assert result.status == "blocked" and result.accepted_route is None
        assert durable_state(store) == before
        classified = store.read_stream("scheduler", "global")[-2]
        evidence = classified.payload["classification"]["evidence"]
        assert (evidence["retry_level"], evidence["exhausted_model_ids"]) == (1, ["model-1"])
