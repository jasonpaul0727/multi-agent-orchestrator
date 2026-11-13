"""Offline characterization of a recovery chain using the real router."""

from datetime import timedelta

import pytest

from orchestrator.models import ModelGatewayFailure
from orchestrator.persistence import SQLiteEventStore
from orchestrator.routing import ModelRouter
from orchestrator.runtime.recovery_opportunities import RecoveryOpportunityService
from tests.support.recovery_continuation import (
    durable_state,
    forbid_recovery_execution,
    recovery_case,
)
from tests.unit.lifecycle.test_scheduler import NOW
from tests.unit.runtime.test_recovery_continuation import admit


@pytest.mark.parametrize("outcome", ["known_failure", "unknown"])
def test_real_router_chain_preserves_history_and_blocks_later_retry(tmp_path, monkeypatch, outcome):
    # A wrapper records decisions while executing the real router unchanged.
    routed = []
    actual_route = ModelRouter.route

    def capture_route(self, request, **kwargs):
        decision = actual_route(self, request, **kwargs)
        routed.append((request, decision))
        return decision

    with forbid_recovery_execution() as attempted:
        with SQLiteEventStore(tmp_path / "recovery-chain.db") as store:
            # The fixture also invokes the real router for the initial route.
            case = recovery_case(store, routing_budget=True, multi_model=True)
            assert case.plan.action == "same_model_retry"
            original = tuple(store.read_stream("scheduler", "global"))
            monkeypatch.setattr(ModelRouter, "route", capture_route)

            first = admit(case)
            assert first.status == "admitted" and first.task is None
            assert len(routed) == 1
            first_request, first_decision = routed[0]
            assert first_decision.outcome == "selected"
            assert first_decision.selected_model_id == first.accepted_route.model_id == "model-1"
            assert first_request.recovery_authorization.authorization_hash == case.plan.authorization.authorization_hash
            after_first = tuple(store.read_stream("scheduler", "global"))
            assert after_first[:-1] == original
            accepted = after_first[-1]
            assert accepted.event_type == "RoutingDecisionAccepted"
            assert accepted.payload["decision_hash"] == first_decision.decision_hash
            assert accepted.payload["request_hash"] == first_request.request_hash
            assert accepted.payload["recovery_authorization_hash"] == case.plan.authorization.authorization_hash
            assert accepted.payload["accepted_route"] == first.accepted_route.model_dump(mode="json")

            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=first.accepted_route.attempt_id,
                fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
                outcome="failed" if outcome == "known_failure" else "outcome_unknown",
                known_no_effect=outcome == "known_failure",
            )
            plan = case.scheduler.plan_attempt_recovery(
                run_id="run-1", node_id="node-1", attempt_id=first.accepted_route.attempt_id,
                fencing_generation=2, source_decision=first_decision, contract=case.contract,
                failure=ModelGatewayFailure(
                    code="rate_limited" if outcome == "known_failure" else "timeout",
                    phase="provider" if outcome == "known_failure" else "transport",
                    outcome=outcome, retryable=outcome == "known_failure",
                    http_status=429 if outcome == "known_failure" else None,
                ),
            )
            records = tuple(store.read_stream("scheduler", "global"))
            assert records[:len(after_first)] == after_first
            classification, plan_event = records[-2:]
            assert (classification.event_type, plan_event.event_type) == (
                "AttemptFailureClassified", "RecoveryPlanCreated",
            )
            assert classification.payload["decision_hash"] == first_decision.decision_hash
            assert plan_event.payload["decision_hash"] == first_decision.decision_hash
            evidence = classification.payload["classification"]["evidence"]
            assert evidence["retry_level"] == 1
            assert evidence["exhausted_model_ids"] == ["model-1"]
            opportunity = RecoveryOpportunityService(case.scheduler).list_opportunities(run_id="run-1")[-1]
            before = durable_state(store)
            if outcome == "known_failure":
                assert plan.action == "same_model_retry" and plan.outcome == "authorized"
                assert plan.authorization.evidence_hash == evidence["evidence_hash"]
                assert plan.authorization.source_decision_hash == first_decision.decision_hash
                assert plan_event.payload["plan"]["authorization"]["authorization_hash"] == plan.authorization.authorization_hash
                assert opportunity.status == "ready"
            else:
                assert plan.outcome == "blocked" and plan.authorization is None
                assert opportunity.status == "reconciliation_required"

            second = admit(
                case, source_attempt_id=first.accepted_route.attempt_id,
                source_fencing_generation=2, accepted_at=NOW + timedelta(seconds=15),
            )
            assert second.accepted_route is None and second.task is None
            if outcome == "known_failure":
                assert (second.status, second.reason) == ("blocked", "routing_candidates_unavailable")
                assert len(routed) == 2
                later_request, later_decision = routed[-1]
                assert later_request.recovery_authorization.authorization_hash == plan.authorization.authorization_hash
                assert later_request.exhausted_model_ids == ("model-1",)
                assert later_request.recovery_authorization.authorized_model_ids == ("model-1",)
                profile = case.config.presets[case.config.active_preset].roles["coder"]
                assert later_request.retry_level == 2 <= profile.retries
                assert later_decision.candidate_assessments == ()
                assert later_decision.outcome == "blocked" and later_decision.selected_model_id is None
            else:
                assert second.status == "reconciliation_required"
                assert len(routed) == 1  # unresolved outcome never reaches the router
            assert durable_state(store) == before
            assert tuple(store.read_stream("scheduler", "global")) == records
    assert attempted == []
