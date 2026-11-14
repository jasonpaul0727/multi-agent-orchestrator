"""Offline characterization of initial routing after a synthetic failed attempt.

These tests describe existing admission behavior, not a Provider send, a
production no-effect proof, or a decision about global v1 routing policy.
"""

from contextlib import contextmanager
from datetime import timedelta

import pytest

from orchestrator.models import ModelGatewayFailure
from orchestrator.persistence import SQLiteEventStore
from orchestrator.routing import ModelRouter, RoutingRequest
from orchestrator.runtime.recovery_opportunities import RecoveryOpportunityService
from orchestrator.runtime.worker_input import WorkerInputError
from orchestrator.runtime.worker_input_service import WorkerInputService
from orchestrator.scheduler.core import StaleRoutingDecision
from tests.support.recovery_continuation import (
    durable_state,
    forbid_recovery_execution,
    recovery_case,
    routing_observations,
)
from tests.unit.lifecycle.test_scheduler import NOW
from tests.unit.runtime.test_recovery_continuation import admit


@contextmanager
def _forbid_all_execution():
    # Add the general Worker and the provider-specific gateway to the shared guard.
    from orchestrator.models.transport import ProviderModelGateway, SystemdProviderHTTPSTransport
    from orchestrator.runtime.general_worker_process import GeneralWorkerProcess

    with forbid_recovery_execution() as attempted, pytest.MonkeyPatch.context() as patch:
        for owner, method in (
            (ProviderModelGateway, "invoke"),
            (SystemdProviderHTTPSTransport, "post_json"),
            (GeneralWorkerProcess, "start_turn"),
            (GeneralWorkerProcess, "run_turn"),
        ):
            def forbidden(*args, boundary=f"{owner.__name__}.{method}", **kwargs):
                attempted.append(boundary)
                raise AssertionError("offline characterization crossed an execution boundary")

            patch.setattr(owner, method, forbidden)
        yield attempted

def _initial_request(case, generation, seconds):
    values = case.request.model_dump(mode="python")
    values.update(
        request_id=f"offline-initial-{generation}",
        attempt_id=f"offline-attempt-{generation}",
        fencing_generation=generation,
        routing_as_of_event_time=NOW + timedelta(seconds=seconds),
    )
    request = RoutingRequest(**values)
    assert request.recovery_action == "initial"
    assert request.recovery_authorization is None
    assert request.prior_decision_hash is None
    assert request.retry_level == 0 and request.exhausted_model_ids == ()
    decision = ModelRouter().route(
        request, config=case.config, registry=case.registry, contract=case.contract,
        **routing_observations(case),
    )
    assert decision.request_hash == request.request_hash
    assert decision.outcome == "selected" and decision.selected_model_id == "model-1"
    return request, decision


def _admit_initial(case, request, decision, *, with_input, accepted_seconds):
    times = {
        "accepted_at": NOW + timedelta(seconds=accepted_seconds),
        "lease_expires_at": NOW + timedelta(minutes=2),
    }
    if with_input:
        return WorkerInputService(case.scheduler).accept_routing_with_input(
            request, decision, task_text="implement a small change", **times,
        )
    return case.scheduler.accept_routing(request, decision, **times)


@pytest.mark.parametrize("with_input", [False, True], ids=["direct-scheduler", "worker-input"])
@pytest.mark.parametrize("unknown", [False, True], ids=["synthetic-known-failure", "unknown"])
def test_initial_after_failed_attempt_or_unknown_is_characterized(tmp_path, with_input, unknown):
    with _forbid_all_execution() as attempted:
        with SQLiteEventStore(tmp_path / "initial-after-terminal.db") as store:
            # recovery_case uses a real Router for the first selected route when
            # multi_model=True; its terminal no-effect assertion is synthetic.
            case = recovery_case(store, unknown=unknown, routing_budget=True, multi_model=True)
            node = case.lifecycle.replay("run-1").node("node-1")
            assert node.spec.max_attempts == 3
            assert [attempt.status for attempt in node.attempts] == [
                "outcome_unknown" if unknown else "failed"
            ]
            assert node.status == ("awaiting_reconciliation" if unknown else "ready")
            request, decision = _initial_request(case, 2, 6)
            events_before = tuple(store.read_stream("scheduler", "global"))
            state_before = durable_state(store)
            budget_before = case.scheduler._ledger_for_run("run-1").available("run-1")
            if unknown:
                expected = WorkerInputError if with_input else StaleRoutingDecision
                with pytest.raises(expected):
                    _admit_initial(case, request, decision, with_input=with_input, accepted_seconds=7)
                assert durable_state(store) == state_before
                assert tuple(store.read_stream("scheduler", "global")) == events_before
                assert budget_before.unknown_tokens == 1500
            else:
                accepted = _admit_initial(case, request, decision, with_input=with_input, accepted_seconds=7)
                events_after = tuple(store.read_stream("scheduler", "global"))
                assert events_after[:-1] == events_before
                event = events_after[-1]
                assert event.event_type == "RoutingDecisionAccepted"
                assert event.payload["request_hash"] == request.request_hash
                assert event.payload["decision_hash"] == decision.decision_hash
                assert event.payload["recovery_action"] == "initial"
                assert event.payload["recovery_authorization_hash"] is None
                assert accepted.accepted_route.model_id == decision.selected_model_id
                assert accepted.accepted_route.fencing_generation == 2
                assert accepted.reservation.status == "reserved"
                node_after = case.lifecycle.replay("run-1").node("node-1")
                assert node_after.status == "running"
                assert [attempt.status for attempt in node_after.attempts] == ["failed", "accepted"]
                budget_after = case.scheduler._ledger_for_run("run-1").available("run-1")
                assert budget_after.reserved_tokens == 1500 and budget_before.reserved_tokens == 0
                assert budget_after.reservation_version == budget_before.reservation_version + 1
        assert attempted == []


def test_initial_can_enter_after_real_continuation_reports_second_retry_blocked(tmp_path, monkeypatch):
    with _forbid_all_execution() as attempted:
        with SQLiteEventStore(tmp_path / "two-synthetic-failures.db") as store:
            case = recovery_case(store, routing_budget=True, multi_model=True)
            real_route = ModelRouter.route
            routed = []

            def capture(self, request, **kwargs):
                decision = real_route(self, request, **kwargs)
                routed.append((request, decision))
                return decision

            with monkeypatch.context() as patch:
                patch.setattr(ModelRouter, "route", capture)
                first = admit(case)
            assert first.status == "admitted" and first.accepted_route.model_id == "model-1"
            assert len(routed) == 1 and routed[0][1].selected_model_id == "model-1"
            source_request, source_decision = routed[0]
            assert source_request.recovery_authorization.authorization_hash == case.plan.authorization.authorization_hash
            first_event = store.read_stream("scheduler", "global")[-1]
            assert first_event.payload["request_hash"] == source_request.request_hash
            assert first_event.payload["decision_hash"] == source_decision.decision_hash
            assert first_event.payload["recovery_authorization_hash"] == case.plan.authorization.authorization_hash
            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=first.accepted_route.attempt_id,
                fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
                outcome="failed", known_no_effect=True,
            )
            plan = case.scheduler.plan_attempt_recovery(
                run_id="run-1", node_id="node-1", attempt_id=first.accepted_route.attempt_id,
                fencing_generation=2, source_decision=source_decision, contract=case.contract,
                failure=ModelGatewayFailure(
                    code="rate_limited", phase="provider", outcome="known_failure",
                    retryable=True, http_status=429,
                ),
            )
            records = tuple(store.read_stream("scheduler", "global"))
            evidence = records[-2].payload["classification"]["evidence"]
            assert evidence["retry_level"] == 1 and evidence["exhausted_model_ids"] == ["model-1"]
            assert plan.outcome == "authorized" and plan.action == "same_model_retry"
            assert plan.authorization.evidence_hash == evidence["evidence_hash"]
            assert plan.authorization.source_decision_hash == source_decision.decision_hash
            assert records[-1].payload["plan"]["authorization"]["authorization_hash"] == plan.authorization.authorization_hash
            opportunity = RecoveryOpportunityService(case.scheduler).list_opportunities(run_id="run-1")[-1]
            assert (opportunity.status, opportunity.reason) == ("blocked", "routing_candidates_unavailable")
            blocked = admit(case, source_attempt_id=first.accepted_route.attempt_id,
                source_fencing_generation=2, accepted_at=NOW + timedelta(seconds=15))
            assert (blocked.status, blocked.reason) == ("blocked", "routing_candidates_unavailable")
            assert tuple(store.read_stream("scheduler", "global")) == records
            request, decision = _initial_request(case, 3, 15)
            before_budget = case.scheduler._ledger_for_run("run-1").available("run-1")
            accepted = _admit_initial(case, request, decision, with_input=True, accepted_seconds=16)
            events = tuple(store.read_stream("scheduler", "global"))
            assert events[:-1] == records
            event = events[-1]
            assert event.event_type == "RoutingDecisionAccepted"
            assert event.payload["request_hash"] == request.request_hash
            assert event.payload["decision_hash"] == decision.decision_hash
            assert event.payload["recovery_action"] == "initial"
            assert event.payload["recovery_authorization_hash"] is None
            assert accepted.accepted_route.model_id == "model-1"
            assert accepted.accepted_route.fencing_generation == 3
            node = case.lifecycle.replay("run-1").node("node-1")
            assert node.spec.max_attempts == 3
            assert [attempt.status for attempt in node.attempts] == ["failed", "failed", "accepted"]
            after_budget = case.scheduler._ledger_for_run("run-1").available("run-1")
            assert before_budget.reserved_tokens == 0 and after_budget.reserved_tokens == 1500
            assert after_budget.reservation_version == before_budget.reservation_version + 1

            # The normal max_attempts limit remains enforced even on this path.
            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=accepted.accepted_route.attempt_id,
                fencing_generation=3, completed_at=NOW + timedelta(seconds=18),
                outcome="failed", known_no_effect=True,
            )
            terminal_node = case.lifecycle.replay("run-1").node("node-1")
            assert terminal_node.status == "failed" and len(terminal_node.attempts) == 3
            fourth_request, fourth_decision = _initial_request(case, 4, 19)
            before_fourth = durable_state(store)
            before_fourth_events = tuple(store.read_stream("scheduler", "global"))
            with pytest.raises(WorkerInputError):
                _admit_initial(case, fourth_request, fourth_decision, with_input=True, accepted_seconds=20)
            assert durable_state(store) == before_fourth
            assert tuple(store.read_stream("scheduler", "global")) == before_fourth_events
        assert attempted == []
