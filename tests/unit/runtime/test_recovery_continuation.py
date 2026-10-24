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
    durable_state, recovery_case, rewrite_event_payload, routing_observations,
)
from tests.unit.lifecycle.test_scheduler import NOW


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
