"""Current post-admission authority, real lock expiry and private input boundaries."""

from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
import threading
import time
import traceback

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.models.provider_calls import SQLiteProviderCallJournal
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.routing import ModelRouter
from orchestrator.runtime.recovery_continuation import RecoveryContinuationService
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from orchestrator.runtime.worker_input_service import WorkerInputService
from tests.support.recovery_continuation import durable_state, recovery_case, routing_observations
from tests.unit.lifecycle.test_scheduler import _application_limits, _provider_call_request_for_crash, routed_pair
from tests.unit.runtime.test_recovery_preparation import TEXT, live_arguments


@pytest.mark.parametrize("transition,status,reason", [
    ("pause", "blocked", "run_paused"),
    ("cancel", "blocked", "run_cancelling"),
    ("expiry", "successor_inactive", "recovery_successor_inactive"),
    ("newer", "successor_inactive", "recovery_successor_inactive"),
    ("provider-dispatching", "reconciliation_required", "provider_outcome_unknown"),
    ("provider-unknown", "reconciliation_required", "provider_outcome_unknown"),
    ("effect-unknown", "reconciliation_required", "provider_outcome_unknown"),
    ("attempt-unknown", "reconciliation_required", "provider_outcome_unknown"),
])
def test_preparation_rechecks_authority_changed_after_committed_admission(
    tmp_path, monkeypatch, transition, status, reason,
):
    # Catches treating a committed route or prior-ready snapshot as present task authority.
    with SQLiteEventStore(tmp_path / "authority.db") as store:
        case = recovery_case(store, routing_budget=True)
        service = RecoveryContinuationService(case.scheduler, WorkerInputService(case.scheduler))
        arguments = live_arguments(case)
        if transition == "expiry":
            arguments["lease_expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=1)
        append = store.append_checked
        boundary = []

        def change_before_preparation(stream_type, stream_id, key, decide):
            if key == "recovery-preparation-snapshot":
                assert not store._connection.in_transaction
                route = case.scheduler.recovery.recover("run-1").active_attempts[0]
                assert route.fencing_generation == 2
                identity = dict(run_id="run-1", node_id="node-1", attempt_id=route.attempt_id,
                                fencing_generation=2)
                if transition == "pause":
                    case.lifecycle.pause_run("run-1", reason_code="user_request")
                elif transition == "cancel":
                    case.lifecycle.request_cancel("run-1", reason_code="user_request")
                elif transition == "expiry":
                    expires = arguments["lease_expires_at"]
                    assert datetime.now(timezone.utc) < expires
                    threading.Event().wait((expires - datetime.now(timezone.utc)).total_seconds() + .02)
                    assert datetime.now(timezone.utc) >= expires
                elif transition in {"newer", "attempt-unknown"}:
                    case.scheduler.finish_attempt(
                        **identity, completed_at=datetime.now(timezone.utc),
                        outcome="outcome_unknown" if transition == "attempt-unknown" else "failed",
                        known_no_effect=transition != "attempt-unknown",
                    )
                    if transition == "newer":
                        now = datetime.now(timezone.utc)
                        request, _ = routed_pair(case.registry, case.config, case.policy, attempt=3,
                                                contract_hash=case.contract.contract_hash)
                        request = request.model_copy(update={"routing_as_of_event_time": now})
                        decision = ModelRouter().route(
                            request, config=case.config, registry=case.registry, contract=case.contract,
                            **routing_observations(case, as_of=now),
                        )
                        assert decision.outcome == "selected"
                        WorkerInputService(case.scheduler).accept_routing_with_input(
                            request, decision, task_text=TEXT, accepted_at=now,
                            lease_expires_at=now + timedelta(minutes=2),
                        )
                elif transition == "effect-unknown":
                    accepted_event = next(event for event in store.read_stream("scheduler", "global")
                                          if event.attempt_id == route.attempt_id
                                          and event.event_type == "RoutingDecisionAccepted")
                    store.append("budget", "run-1", store.current_version("budget", "run-1"), [EventDraft(
                        "EffectIntentRecorded", {"effect_id": "recovery-effect-1", "recovery_class": "queryable"},
                        **identity, causation_id=accepted_event.payload["decision_hash"],
                    )], "recovery-effect-intent")
                else:
                    active = case.scheduler.resolve_active_attempt(**identity, as_of=datetime.now(timezone.utc))
                    request = _provider_call_request_for_crash(active, case.registry)
                    journal = SQLiteProviderCallJournal(store)
                    journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                                          provider_correlation_id="bounded-recovery-call", request_body=b"{}")
                    if transition == "provider-unknown":
                        journal.record_outcome(request, outcome="unknown", failure_code="timeout")
                boundary.append((route.attempt_id, durable_state(store)))
            return append(stream_type, stream_id, key, decide)

        monkeypatch.setattr(store, "append_checked", change_before_preparation)
        result = service.continue_recovery(**arguments)
        assert len(boundary) == 1
        assert (result.status, result.reason, result.task) == (status, reason, None)
        assert result.accepted_route.attempt_id == boundary[0][0]
        assert durable_state(store) == boundary[0][1]
        assert len([event for event in store.read_stream("scheduler", "global")
                    if event.event_type == "RoutingDecisionAccepted"]) == (3 if transition == "newer" else 2)


def test_preparation_lock_wait_uses_actual_utc_after_committed_admission(tmp_path, monkeypatch):
    # Catches sampling current time before waiting for the SECOND transaction's real lock.
    database = tmp_path / "contention.db"
    ready, begin_hold, held = (threading.Event() for _ in range(3))
    entered, stop, finished = (threading.Event() for _ in range(3))
    errors, observations = [], {}

    def hold_second_connection():
        try:
            with SQLiteEventStore(database) as other:
                ready.set()
                assert begin_hold.wait(4), "preparation boundary never reached"
                other._connection.execute("BEGIN IMMEDIATE")
                try:
                    accepted = [event for event in other.read_stream("scheduler", "global")
                                if event.event_type == "RoutingDecisionAccepted"]
                    assert len(accepted) == 2, "admission must commit before contention begins"
                    expires = datetime.fromisoformat(accepted[-1].payload["lease_expires_at"])
                    observations["held_at"] = datetime.now(timezone.utc)
                    assert observations["held_at"] < expires
                    observations["expires"] = expires
                    held.set()
                    deadline = time.monotonic() + 4
                    while datetime.now(timezone.utc) < expires:
                        assert time.monotonic() < deadline
                        assert not entered.is_set() and not finished.is_set()
                        if stop.wait(.01):
                            raise AssertionError("contention interrupted before expiry")
                    assert not entered.is_set() and not finished.is_set()
                    observations["released_at"] = datetime.now(timezone.utc)
                finally:
                    other._connection.execute("ROLLBACK")
        except BaseException as exc:
            errors.append(exc)
            ready.set()
            held.set()

    with SQLiteEventStore(database) as store:
        case = recovery_case(store, routing_budget=True)
        clock_calls = []

        def forbidden_clock():
            clock_calls.append("called")
            raise RuntimeError("injected clock must remain unused")

        service = RecoveryContinuationService(case.scheduler, WorkerInputService(case.scheduler, clock=forbidden_clock))
        holder = threading.Thread(target=hold_second_connection, daemon=True)
        holder.start()
        try:
            assert ready.wait(4) and not errors
            arguments = live_arguments(case)
            arguments["lease_expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=1.5)
            append = store.append_checked

            def contend_only_preparation(stream_type, stream_id, key, decide):
                if key == "recovery-preparation-snapshot":
                    assert not store._connection.in_transaction
                    assert len([event for event in store.read_stream("scheduler", "global")
                                if event.event_type == "RoutingDecisionAccepted"]) == 2
                    begin_hold.set()
                    assert held.wait(4) and not errors

                    def observe_callback(events, version):
                        entered.set()
                        observations["entered_at"] = datetime.now(timezone.utc)
                        assert store._connection.in_transaction
                        value = decide(events, version)
                        assert value is None
                        return value

                    return append(stream_type, stream_id, key, observe_callback)
                return append(stream_type, stream_id, key, decide)

            with monkeypatch.context() as patch:
                patch.setattr(store, "append_checked", contend_only_preparation)
                result = service.continue_recovery(**arguments)
            finished.set()
            holder.join(4)
            assert not holder.is_alive() and not errors
            assert observations["held_at"] < observations["expires"] <= observations["released_at"] <= observations["entered_at"]
            assert result.status == "successor_inactive" and result.task is None
            assert result.accepted_route is not None and clock_calls == []
            assert case.scheduler.agents.replay("run-1").total_created == 2
            assert case.scheduler.agents.replay("run-1").active_count == 1
            assert len([event for event in store.read_stream("budget", "run-1")
                        if event.event_type == "BudgetReserved"]) == 2
            before = durable_state(store)
            arguments.update(routing_inputs=None, accepted_at=datetime.now(timezone.utc),
                             lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=2))
            repeated = service.continue_recovery(**arguments)
            assert repeated.status == "successor_inactive" and repeated.task is None
            assert repeated.accepted_route == result.accepted_route and durable_state(store) == before
        finally:
            stop.set()
            begin_hold.set()
            holder.join(4)
        assert not holder.is_alive()


def test_replay_input_and_preparation_failures_keep_private_text_out_of_surfaces(tmp_path, monkeypatch, caplog, capsys):
    # Catches prompt persistence, unsafe result repr, exception chains or host logging on failures.
    sentinel = "PRIVATE_RECOVERY_TEXT_v3_never_persist_or_log"
    database = tmp_path / "private.db"
    with SQLiteEventStore(database) as store:
        case = recovery_case(store, routing_budget=True, task_text=sentinel)
        service = RecoveryContinuationService(case.scheduler, WorkerInputService(case.scheduler))
        arguments = live_arguments(case)
        arguments["task_text"] = sentinel
        prepared = service.continue_recovery(**arguments)
        assert prepared.status == "prepared" and prepared.task.task_text == sentinel
        before = durable_state(store)
        arguments.update(task_text=sentinel + " changed", routing_inputs=None)
        with pytest.raises(RecoveryContinuationError) as caught:
            service.continue_recovery(**arguments)
        assert caught.value.code == "recovery_input_mismatch"
        assert caught.value.__cause__ is None and caught.value.__context__ is None
        assert durable_state(store) == before
        arguments["task_text"] = sentinel
        append = store.append_checked

        def fail_response(stream_type, stream_id, key, decide):
            value = append(stream_type, stream_id, key, decide)
            if key == "recovery-preparation-snapshot":
                raise RuntimeError(sentinel)
            return value

        monkeypatch.setattr(store, "append_checked", fail_response)
        failed = service.continue_recovery(**arguments)
        assert (failed.status, failed.reason, failed.task) == ("admitted", "task_preparation_failed", None)
        assert failed.accepted_route == prepared.accepted_route and durable_state(store) == before
        emitted = capsys.readouterr()
        surfaces = (repr(prepared), repr(prepared.task), repr(failed), str(caught.value), repr(caught.value),
                    repr(vars(caught.value)), "".join(traceback.format_exception(caught.value)),
                    caplog.text, emitted.out, emitted.err, "\n".join(store._connection.iterdump()))
        assert all(sentinel not in surface for surface in surfaces)
        assert Path(str(database) + "-wal").exists()
        for path in (database, Path(str(database) + "-wal"), Path(str(database) + "-shm")):
            if path.exists():
                assert sentinel.encode() not in path.read_bytes()


@pytest.mark.parametrize("field,value,code", [
    ("run_id", "", "recovery_continuation_invalid"),
    ("node_id", "invalid node", "recovery_continuation_invalid"),
    ("source_attempt_id", [], "recovery_continuation_invalid"),
    ("source_fencing_generation", True, "recovery_continuation_invalid"),
    ("source_fencing_generation", 0, "recovery_continuation_invalid"),
    ("accepted_at", None, "recovery_continuation_invalid"),
    ("lease_expires_at", datetime(2026, 1, 1), "recovery_continuation_invalid"),
    ("task_text", 123, "recovery_input_invalid"),
    ("task_text", "\ud800", "recovery_input_invalid"),
], ids=["empty-run", "invalid-node", "non-string-source", "boolean-fence", "zero-fence", "missing-time",
        "naive-time", "non-string-text", "surrogate-text"])
def test_application_continuation_rejects_invalid_arguments_without_writes(tmp_path, field, value, code):
    # Catches bypassing finalized Task 2 argument guards through the new host entry point.
    with ControlPlaneApplication(tmp_path / "invalid.db", limits=_application_limits()) as app:
        case = recovery_case(app._event_store, routing_budget=True)
        arguments = live_arguments(case)
        arguments[field] = value
        before = durable_state(app._event_store)
        with pytest.raises(RecoveryContinuationError) as caught:
            app.continue_recovery(**arguments)
        assert caught.value.code == code and durable_state(app._event_store) == before
        assert caught.value.__cause__ is None and caught.value.__context__ is None


def test_application_rejects_executable_datetime_without_callback(tmp_path):
    # Catches new entry points invoking custom tzinfo while checking supplied acceptance time.
    calls = []

    class HostTimezone(tzinfo):
        def utcoffset(self, value):
            calls.append("utcoffset")
            raise RuntimeError("foreign timezone")

    with ControlPlaneApplication(tmp_path / "timezone.db", limits=_application_limits()) as app:
        case = recovery_case(app._event_store, routing_budget=True)
        arguments = live_arguments(case)
        arguments["accepted_at"] = datetime(2026, 1, 1, tzinfo=HostTimezone())
        before = durable_state(app._event_store)
        with pytest.raises(RecoveryContinuationError) as caught:
            app.continue_recovery(**arguments)
        assert caught.value.code == "recovery_continuation_invalid" and calls == []
        assert durable_state(app._event_store) == before


@pytest.mark.parametrize("target", ["application", "service", "internal-input"])
def test_recovery_exposes_no_as_of_override(tmp_path, target):
    # Catches an override allowing the host to prepare using stale time instead of owned UTC.
    with ControlPlaneApplication(tmp_path / "override.db", limits=_application_limits()) as app:
        case = recovery_case(app._event_store, routing_budget=True)
        arguments = live_arguments(case)
        before = durable_state(app._event_store)
        if target == "internal-input":
            method = app._worker_inputs._prepare_recovery_worker_task
            arguments = dict(run_id="run-1", node_id="node-1", attempt_id="attempt-node-1-1",
                             fencing_generation=1, task_text=TEXT)
        else:
            method = app.continue_recovery if target == "application" else app._recovery_continuation.continue_recovery
        with pytest.raises(TypeError):
            method(**arguments, as_of=datetime(2000, 1, 1, tzinfo=timezone.utc))
        assert durable_state(app._event_store) == before
