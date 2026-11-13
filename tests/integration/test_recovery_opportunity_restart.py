"""Offline recovery opportunities after all owning SQLite handles close."""

from datetime import timedelta
import hashlib
import json
import multiprocessing
import os

import pytest

from orchestrator.budget import BudgetLedger
from orchestrator.models import ModelGatewayFailure
from orchestrator.persistence import SQLiteEventStore
from orchestrator.routing import ModelRouter
from orchestrator.runtime.recovery_continuation import RecoveryContinuationService, RecoveryRoutingInputs
from orchestrator.runtime.recovery_opportunities import RecoveryOpportunityService
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.support.recovery_continuation import forbid_recovery_execution, recovery_case, routing_observations
from tests.unit.lifecycle.test_scheduler import NOW, scheduler
from tests.unit.runtime.test_recovery_continuation import admit


INPUT = "synthetic-private-restart-input-c2f9c1"


def _snapshot(store):
    # The digest covers every logical SQLite row without exposing its contents.
    dump = json.dumps(tuple(store._connection.iterdump()), ensure_ascii=False)
    assert INPUT not in dump
    scheduler_events = tuple((e.event_id, e.payload_hash) for e in store.read_stream("scheduler", "global"))
    budget_events = tuple((e.event_type, e.event_id, e.payload_hash) for e in store.read_stream("budget", "run-1"))
    return (hashlib.sha256(dump.encode()).hexdigest(), scheduler_events, budget_events,
            repr(BudgetLedger(store).available("run-1")),
            tuple(tuple(row) for row in store._connection.execute(
                "SELECT event_id, payload_hash FROM events ORDER BY rowid")))


def _reopen_and_inspect(database, pipe, observations, first_route, first_auth, first_evidence, second_auth,
                        second_evidence, expected, original_snapshot):
    # This process owns its connection. Execution guard records even swallowed attempts.
    with forbid_recovery_execution() as attempted:
        with SQLiteEventStore(database) as store:
            control = scheduler(store)
            before = _snapshot(store)
            balance_before = BudgetLedger(store).available("run-1")
            assert before == original_snapshot
            older, latest = RecoveryOpportunityService(control).list_opportunities(run_id="run-1")
            assert older.status == "already_admitted"
            assert older.authorization_hash == first_auth
            assert older.evidence_hash == first_evidence
            assert older.successor.model_dump(mode="json") == first_route
            assert latest.authorization_hash == second_auth
            assert latest.evidence_hash == second_evidence
            assert latest.source_attempt_id == first_route["attempt_id"]
            assert INPUT not in repr((older, latest))
            if expected == "other_model":
                assert (latest.status, latest.reason) == ("ready", "bounded_same_model_retry")
            elif expected == "unknown":
                assert (latest.status, latest.reason) == ("reconciliation_required", "provider_outcome_unknown")
            else:
                assert (latest.status, latest.reason) == ("blocked", "routing_candidates_unavailable")
            assert _snapshot(store) == before
            result = RecoveryContinuationService(control, WorkerInputService(control)).admit(
                run_id="run-1", node_id="node-1", source_attempt_id=first_route["attempt_id"],
                source_fencing_generation=2, task_text=INPUT, routing_inputs=observations,
                accepted_at=NOW + timedelta(seconds=15), lease_expires_at=NOW + timedelta(minutes=2),
            )
            after = _snapshot(store)
            assert result.task is None
            assert INPUT not in repr(result)
            if expected == "other_model":
                assert result.status == "admitted" and result.accepted_route.model_id == "model-2"
                assert after[4][:len(before[4])] == before[4]
                assert after[1][:-1] == before[1] and len(after[1]) == len(before[1]) + 1
                assert after[2][:-1] == before[2] and len(after[2]) == len(before[2]) + 1
                assert after[2][-1][0] == "BudgetReserved"
                latest_event = store.read_stream("scheduler", "global")[-1]
                assert latest_event.event_type == "RoutingDecisionAccepted"
                assert latest_event.payload["recovery_authorization_hash"] == second_auth
                reservation = store.read_stream("budget", "run-1")[-1]
                assert reservation.payload["reservation_id"] == latest_event.payload["reservation_id"]
                balance_after = BudgetLedger(store).available("run-1")
                assert balance_after.available_tokens < balance_before.available_tokens
                assert balance_after.reserved_tokens > balance_before.reserved_tokens
                assert latest_event.payload["accepted_route"] == result.accepted_route.model_dump(mode="json")
            else:
                assert result.accepted_route is None
                assert result.status == ("reconciliation_required" if expected == "unknown" else "blocked")
                assert after == before
            assert attempted == []
            pipe.send({"pid": os.getpid(), "status": result.status,
                       "first_auth": older.authorization_hash,
                       "second_auth": latest.authorization_hash,
                       "second_evidence": latest.evidence_hash})
    pipe.close()


@pytest.mark.parametrize("scenario", ["blocked", "unknown", "other_model"])
def test_real_recovery_chain_reopens_in_fresh_process(tmp_path, monkeypatch, scenario):
    database = tmp_path / "restart.db"
    routed = []
    original_route = ModelRouter.route

    def capture_route(self, request, **kwargs):
        decision = original_route(self, request, **kwargs)
        routed.append(decision)
        return decision

    with forbid_recovery_execution() as attempted:
        with SQLiteEventStore(database) as store:
            case = recovery_case(
                store, routing_budget=True, multi_model=True, task_text=INPUT,
                failure_code="provider_unavailable" if scenario == "other_model" else "rate_limited",
            )
            first_auth = case.plan.authorization.authorization_hash
            first_evidence = case.plan.authorization.evidence_hash
            with monkeypatch.context() as patch:
                patch.setattr(ModelRouter, "route", capture_route)
                first = admit(case, task_text=INPUT)
            assert first.status == "admitted" and len(routed) == 1
            assert first.accepted_route.model_id == ("model-2" if scenario == "other_model" else "model-1")
            first_decision = routed[0]
            assert first_decision.selected_model_id == first.accepted_route.model_id
            accepted_event = store.read_stream("scheduler", "global")[-1]
            assert accepted_event.payload["decision_hash"] == first_decision.decision_hash
            assert accepted_event.payload["recovery_authorization_hash"] == first_auth
            unknown = scenario == "unknown"
            case.scheduler.finish_attempt(
                run_id="run-1", node_id="node-1", attempt_id=first.accepted_route.attempt_id,
                fencing_generation=2, completed_at=NOW + timedelta(seconds=12),
                outcome="outcome_unknown" if unknown else "failed", known_no_effect=not unknown,
            )
            plan = case.scheduler.plan_attempt_recovery(
                run_id="run-1", node_id="node-1", attempt_id=first.accepted_route.attempt_id,
                fencing_generation=2, source_decision=first_decision, contract=case.contract,
                failure=ModelGatewayFailure(
                    code="timeout" if unknown else "rate_limited",
                    phase="transport" if unknown else "provider",
                    outcome="unknown" if unknown else "known_failure", retryable=not unknown,
                    http_status=None if unknown else 429,
                ),
            )
            events = store.read_stream("scheduler", "global")
            classification, plan_event = events[-2:]
            assert classification.event_type == "AttemptFailureClassified"
            assert plan_event.event_type == "RecoveryPlanCreated"
            evidence = classification.payload["classification"]["evidence"]
            assert evidence["exhausted_model_ids"] == ["model-1"]
            assert classification.payload["decision_hash"] == first_decision.decision_hash
            assert plan_event.payload["decision_hash"] == first_decision.decision_hash
            second_auth = None if plan.authorization is None else plan.authorization.authorization_hash
            if not unknown:
                assert plan.authorization.evidence_hash == evidence["evidence_hash"]
                assert plan_event.payload["plan"]["authorization"]["authorization_hash"] == second_auth
            observations = RecoveryRoutingInputs(**routing_observations(case))
            before = _snapshot(store)
            balance_before = BudgetLedger(store).available("run-1")
            route = first.accepted_route.model_dump(mode="json")
    assert attempted == []

    # Spawn only after the parent store is closed. The pipe and child are ours.
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_reopen_and_inspect,
        args=(database, sender, observations, route, first_auth, first_evidence, second_auth,
              evidence["evidence_hash"], scenario, before),
    )
    try:
        process.start()
        sender.close()
        assert receiver.poll(30), "recovery child did not finish within deadline"
        result = receiver.recv()
        process.join(timeout=5)
        assert process.exitcode == 0, "recovery child failed or did not exit"
        assert result["pid"] != os.getpid()
        assert result["status"] == ("admitted" if scenario == "other_model" else
                                    "reconciliation_required" if unknown else "blocked")
        assert (result["first_auth"], result["second_auth"], result["second_evidence"]) == (
            first_auth, second_auth, evidence["evidence_hash"],
        )
    finally:
        if process.pid is not None:
            if process.is_alive():
                process.kill()
            process.join(timeout=5)
            assert not process.is_alive(), "recovery child cleanup unconfirmed"
        receiver.close()
        sender.close()
