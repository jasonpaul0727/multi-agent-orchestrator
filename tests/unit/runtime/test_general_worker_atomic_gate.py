"""Deterministic characterization of the v1 prepare/release boundary."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournalError
from orchestrator.scheduler import Scheduler
from orchestrator.scheduler.core import SchedulerError
from tests.unit.lifecycle.test_scheduler import _application_limits
from tests.unit.runtime.test_general_worker_authority import dispatch_case
from tests.unit.runtime.test_general_worker_execution import execution_case, vars_identity
from tests.unit.runtime.test_general_worker_journal import epoch


TIMEOUT = 10


def release_on_other_connection(db, identity, entered, resume):
    assert entered.wait(TIMEOUT), "prepare boundary was not reached"
    try:
        with SQLiteEventStore(db) as store:
            scheduler = Scheduler(store, limits=_application_limits())
            scheduler.finish_attempt(**identity, completed_at=datetime.now(timezone.utc),
                outcome="failed", known_no_effect=True)
    finally:
        resume.set()


def scheduler_events(store):
    return store.read_stream("scheduler", "global")


def test_service_release_wins_after_second_eligibility_before_prepare(tmp_path, monkeypatch):
    case = execution_case(tmp_path)
    entered, resume = Event(), Event()
    starts = []
    original = case.service._journal.prepare_turn

    def paused_prepare(*args, **kwargs):
        entered.set()
        assert resume.wait(TIMEOUT), "release did not finish"
        return original(*args, **kwargs)

    monkeypatch.setattr(case.service._journal, "prepare_turn", paused_prepare)
    monkeypatch.setattr(case.service._worker, "prepare_launch", lambda: epoch()[0])
    def record_start(*args, **kwargs):
        starts.append((args, kwargs))
        raise RuntimeError("call-recording sentinel; no systemd was launched")
    monkeypatch.setattr(case.service._worker, "start_turn", record_start)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(release_on_other_connection, tmp_path / "private/events.db",
                vars_identity(case.identity), entered, resume)
            result = asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
            future.result(timeout=TIMEOUT)
        assert any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.app._event_store))
        assert result.status == "blocked"
        assert starts == []
        assert not any(e.event_type == 'GeneralWorkerTurnPrepared' for e in
            case.app._event_store.read_stream('general_worker', case.identity.stream_id))
    finally:
        case.app.close()


def test_release_wins_after_eligibility_before_prepare(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    entered, resume = Event(), Event()
    try:
        operation = case.journal.request(**case.registration)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(release_on_other_connection, tmp_path / "events.db", case.identity, entered, resume)
            case.authority.resolve_eligibility(operation, purpose="request")
            entered.set()
            assert resume.wait(TIMEOUT), "release did not finish"
            future.result(timeout=TIMEOUT)
            assert any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
            with pytest.raises(GeneralWorkerJournalError):
                case.journal.prepare_turn(operation, phase="request", prepared=epoch()[0])
        assert case.journal.read(operation.identity).turns == ()
    finally:
        case.store.close()


def test_prepare_wins_before_release(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    entered, resume = Event(), Event()
    try:
        operation = case.journal.request(**case.registration)
        case.authority.resolve_eligibility(operation, purpose="request")
        operation = case.journal.prepare_turn(operation, phase="request", prepared=epoch()[0])
        assert len(case.journal.read(operation.identity).turns) == 1
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(release_on_other_connection, tmp_path / "events.db", case.identity, entered, resume)
            entered.set()
            with pytest.raises(SchedulerError):
                future.result(timeout=TIMEOUT)
        assert not any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
    finally:
        case.store.close()


def test_registration_after_release_is_rejected(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
            outcome="failed", known_no_effect=True)
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.request(**case.registration)
        assert case.journal.read(case.operation.identity) is None
    finally:
        case.store.close()


@pytest.mark.parametrize("statements", [False, True])
def test_replaced_ambient_transaction_cannot_inherit_owned_write(tmp_path, statements):
    case = dispatch_case(tmp_path, register=False)
    try:
        def replace_transaction(_events, _version):
            if statements:
                case.store._connection.execute("COMMIT;")
                case.store._connection.execute("BEGIN;")
            else:
                case.store._connection.commit()
                case.store._connection.execute("BEGIN IMMEDIATE")
            with pytest.raises(GeneralWorkerJournalError):
                case.journal.request(**case.registration)
        case.store.append_checked("scheduler", "global", "replace-transaction-test", replace_transaction)
        assert case.journal.read(case.operation.identity) is None
    finally:
        case.store.close()


@pytest.mark.parametrize("base_method", [False, pytest.param(True, marks=pytest.mark.xfail(
    reason="same-process code can bypass the private connection subclass", strict=True))])
def test_owned_transaction_monitor_cannot_be_disabled(tmp_path, base_method):
    case = dispatch_case(tmp_path, register=False)
    try:
        def replace_transaction(_events, _version):
            with pytest.raises((RuntimeError, GeneralWorkerJournalError)):
                if base_method:
                    import sqlite3
                    sqlite3.Connection.set_trace_callback(case.store._connection, None)
                else:
                    case.store._connection.set_trace_callback(None)
                case.store._connection.commit()
                case.store._connection.execute("BEGIN IMMEDIATE")
                case.journal.request(**case.registration)
        case.store.append_checked("scheduler", "global", "disable-monitor-test", replace_transaction)
        assert case.journal.read(case.operation.identity) is None
    finally:
        case.store.close()


def test_owned_nested_registration_and_prepare_share_outer_commit(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        def nested(_events, _version):
            operation = case.journal.request(**case.registration)
            case.journal.prepare_turn(operation, phase="request", prepared=epoch()[0])
        case.store.append_checked("scheduler", "global", "nested-gate-test", nested)
        assert len(case.journal.read(case.operation.identity).turns) == 1
        assert not case.store._has_owned_write()
    finally:
        case.store.close()
