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


def test_public_registration_and_prepare_replay_after_progress(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared, ack, stop = epoch()
        ready = case.journal.prepare_turn(operation, phase="request", prepared=prepared)
        assert case.journal.prepare_turn(operation, phase="request", prepared=prepared) == ready
        acknowledged = case.journal.ack_turn(ready, receipt=ack)
        stopped = case.journal.stop_turn(acknowledged, receipt=stop)
        assert case.journal.request(**case.registration) == stopped
        assert case.journal.prepare_turn(operation, phase="request", prepared=prepared) == stopped
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.prepare_turn(operation, phase="request",
                prepared=epoch("9c483536-3c61-41e7-977b-6559b2732937")[0])
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
            outcome="failed", known_no_effect=True)
        assert case.journal.request(**case.registration) == stopped
        assert case.journal.prepare_turn(operation, phase="request", prepared=prepared) == stopped
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.prepare_turn(stopped, phase="request",
                prepared=epoch("9c483536-3c61-41e7-977b-6559b2732937")[0])
    finally:
        case.store.close()


def test_release_ignores_corrupt_unrelated_worker_streams(tmp_path, monkeypatch):
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    unrelated = []
    try:
        for number in range(24):
            stream = f"foreign-{number}"
            unrelated.append(stream)
            case.store.append("general_worker", stream, 0, [EventDraft("GeneralWorkerRequested", {},
                run_id=f"other-{number}", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
                correlation_id=f"other-{number}", causation_id="decision-foreign")], f"foreign-{number}")
        touched = []
        original = case.store.read_stream
        def record(stream_type, stream_id, *args, **kwargs):
            if stream_type == "general_worker" and stream_id in unrelated:
                touched.append(stream_id)
            return original(stream_type, stream_id, *args, **kwargs)
        monkeypatch.setattr(case.store, "read_stream", record)
        case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
            outcome="failed", known_no_effect=True)
        assert touched == []
        assert any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
    finally:
        case.store.close()


def test_release_rejects_corrupt_target_worker_stream(tmp_path):
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    try:
        case.store.append("general_worker", "corrupt-target", 0, [EventDraft("GeneralWorkerRequested", {},
            run_id="run-1", node_id="node-1", attempt_id="another-attempt", fencing_generation=1,
            correlation_id="run-1", causation_id="decision-foreign")], "bad-target")
        with pytest.raises(ValueError):
            case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                outcome="failed", known_no_effect=True)
        assert not any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
    finally:
        case.store.close()


def test_release_rejects_target_stream_count_above_bound(tmp_path, monkeypatch):
    import orchestrator.runtime.general_worker_gate as gate_module
    from orchestrator.runtime.general_worker_journal import _request_operation, _safe
    case = dispatch_case(tmp_path, register=False)
    try:
        monkeypatch.setattr(gate_module, "MAX_RELEASE_STREAMS", 2, raising=False)
        for number in range(3):
            registration = dict(case.registration)
            registration["context"] = registration["context"].model_copy(
                update={"attempt_id": f"historical-{number}"})
            data = _safe(registration)
            case.journal._append(_request_operation(data), "Requested", data)
        with pytest.raises((GeneralWorkerJournalError, SchedulerError, ValueError)):
            case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                outcome="failed", known_no_effect=True)
        assert not any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
    finally:
        case.store.close()


def test_execute_exact_prepare_replay_does_not_launch_again(tmp_path, monkeypatch):
    case = execution_case(tmp_path)
    starts, returned = [], []
    try:
        task = case.app._worker_inputs._prepare_general_worker_task(**vars_identity(case.identity), task_text=case.text)
        operation = case.service._register(case.identity, task)
        prepared, ack, stop = epoch()
        operation = case.journal.prepare_turn(operation, phase="request", prepared=prepared)
        operation = case.journal.ack_turn(operation, receipt=ack)
        operation = case.journal.stop_turn(operation, receipt=stop)
        original = case.service._journal.prepare_turn
        def record_prepare(*args, **kwargs):
            result = original(*args, **kwargs)
            returned.append(result)
            return result
        monkeypatch.setattr(case.service._journal, "prepare_turn", record_prepare)
        monkeypatch.setattr(case.service._worker, "prepare_launch", lambda: prepared)
        def record_start(*args, **kwargs):
            starts.append(True)
            raise RuntimeError("record-only sentinel")
        monkeypatch.setattr(case.service._worker, "start_turn", record_start)
        result = asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
        assert returned == [operation]
        assert starts == []
        assert result.status == "blocked"
    finally:
        case.app.close()


def test_legacy_worker_scope_migrates_atomically(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    operation = case.journal.request(**case.registration)
    connection = case.store._connection
    for name in ("gw_scope_insert", "gw_scope_check", "gw_scope_immutable_update", "gw_scope_immutable_delete"):
        connection.execute(f"DROP TRIGGER IF EXISTS {name}")
    connection.execute("DROP TABLE IF EXISTS general_worker_scope")
    connection.execute("DELETE FROM schema_migrations WHERE version = 5")
    case.store.close()
    with SQLiteEventStore(tmp_path / "events.db") as reopened:
        assert reopened.stream_ids_for_worker_node("run-1", "node-1", limit=10) == [operation.identity.stream_id]


@pytest.mark.parametrize("bad_sql", [
    "run_id = NULL", "run_id = ''", "fencing_generation = 'bad'",
])
def test_legacy_worker_scope_rejects_unattributable_history(tmp_path, bad_sql):
    from orchestrator.persistence import EventDraft
    path = tmp_path / "legacy.db"
    with SQLiteEventStore(path) as store:
        for name in ("gw_scope_insert", "gw_scope_check", "gw_scope_immutable_update", "gw_scope_immutable_delete"):
            store._connection.execute(f"DROP TRIGGER IF EXISTS {name}")
        store._connection.execute("DROP TABLE IF EXISTS general_worker_scope")
        store._connection.execute("DELETE FROM schema_migrations WHERE version = 5")
        store.append("general_worker", "missing-context", 0, [EventDraft("GeneralWorkerRequested", {},
            run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
            correlation_id="run-1", causation_id="decision-foreign")], "bad-legacy")
        # Simulate an old database damaged outside the append-only API.
        store._connection.execute("DROP TRIGGER events_immutable_update")
        store._connection.execute(f"UPDATE events SET {bad_sql} WHERE stream_id = 'missing-context'")
    with pytest.raises(ValueError, match="general worker scope"):
        SQLiteEventStore(path)
    import sqlite3
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT 1 FROM sqlite_master WHERE name = ?", ("general_worker_scope",)).fetchone() is None
        assert connection.execute("SELECT 1 FROM schema_migrations WHERE version = 5").fetchone() is None


def test_release_rejects_scope_hiding_known_admitted_stream(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        # Model external damage that also bypassed the immutable-scope trigger.
        case.store._connection.execute("DROP TRIGGER gw_scope_immutable_update")
        case.store._connection.execute("UPDATE general_worker_scope SET run_id='other-run' WHERE stream_id=?",
            (operation.identity.stream_id,))
        with pytest.raises(ValueError, match="scope hides"):
            case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                outcome="failed", known_no_effect=True)
        assert not any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
    finally:
        case.store.close()


def test_release_uses_one_locked_utc_sample_for_all_checks(tmp_path, monkeypatch):
    import orchestrator.runtime.general_worker_gate as gate_module
    import orchestrator.scheduler.core as scheduler_module
    case = dispatch_case(tmp_path, register=False)
    try:
        case.journal.request(**case.registration)
        completed_at = datetime.now(timezone.utc)
        locked_samples = []
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                value = super().now(tz)
                if case.store._has_owned_write():
                    locked_samples.append(value)
                return value
        monkeypatch.setattr(gate_module, "datetime", Clock)
        monkeypatch.setattr(scheduler_module, "datetime", Clock)
        case.scheduler.finish_attempt(**case.identity, completed_at=completed_at,
            outcome="failed", known_no_effect=True)
        assert len(locked_samples) == 1
    finally:
        case.store.close()


def test_prepare_replay_rejects_forged_operation_commitments(tmp_path):
    from dataclasses import replace
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        case.journal.prepare_turn(operation, phase="request", prepared=prepared)
        forged = replace(operation, input_commitment="sha256:" + "b" * 64)
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.prepare_turn(forged, phase="request", prepared=prepared)
    finally:
        case.store.close()


def test_scope_index_rolls_back_with_worker_registration(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        def abort(_events, _version):
            case.journal.request(**case.registration)
            assert case.store.stream_ids_for_worker_node("run-1", "node-1", limit=10)
            raise RuntimeError("roll back outer transaction")
        with pytest.raises(RuntimeError, match="roll back outer transaction"):
            case.store.append_checked("scheduler", "global", "rollback-scope-test", abort)
        assert case.store.stream_ids_for_worker_node("run-1", "node-1", limit=10) == []
        assert case.journal.read(case.operation.identity) is None
    finally:
        case.store.close()


def test_corrupt_worker_history_is_bounded_before_decoding(tmp_path, monkeypatch):
    from orchestrator.persistence import EventDraft
    from orchestrator.runtime.general_worker_journal import MAX_EVENTS
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        event = EventDraft("GeneralWorkerTurnPrepared", {}, **case.identity,
            correlation_id="run-1", causation_id="decision-foreign")
        case.store.append("general_worker", operation.identity.stream_id, 1,
            [event] * MAX_EVENTS, "oversized-history")
        case.store._connection.execute("UPDATE stream_versions SET current_version=1 WHERE stream_type=? AND stream_id=?",
            ("general_worker", operation.identity.stream_id))
        decoded = []
        original = case.store._row_to_event
        def counted(row):
            decoded.append(1)
            return original(row)
        monkeypatch.setattr(case.store, "_row_to_event", counted)
        with pytest.raises(GeneralWorkerJournalError):
            case.journal.read(operation.identity)
        assert len(decoded) <= MAX_EVENTS
    finally:
        case.store.close()


def test_nested_prepare_rollback_never_grants_fresh_launch(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            case.journal.prepare_turn(operation, phase="request", prepared=prepared)
            raise RuntimeError("abort")
        with pytest.raises(RuntimeError, match="abort"):
            case.store.append_checked("scheduler", "global", "rollback-prepare", outer)
        assert case.journal.read(operation.identity) == operation
        assert case.journal._fresh_prepare is None
        assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()


def test_nested_prepare_savepoint_rollback_never_grants_fresh_launch(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            def nested(_nested_events, _nested_version):
                case.journal.prepare_turn(operation, phase="request", prepared=prepared)
                raise RuntimeError("abort savepoint")
            with pytest.raises(RuntimeError, match="abort savepoint"):
                case.store.append_checked("scheduler", "global", "nested-rollback", nested)
        case.store.append_checked("scheduler", "global", "commit-outer", outer)
        assert case.journal.read(operation.identity) == operation
        assert case.journal._fresh_prepare is None
        assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()


def test_nested_prepare_commit_grants_one_fresh_launch(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            case.journal.prepare_turn(operation, phase="request", prepared=prepared)
            assert case.journal._fresh_prepare is None
        case.store.append_checked("scheduler", "global", "commit-prepare", outer)
        assert case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
        assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()


def test_real_attempt_boundaries_and_live_worker_census(tmp_path):
    from datetime import timedelta
    from pydantic import ValidationError
    from orchestrator.lifecycle import NodeSpec
    from orchestrator.models import ModelGatewayFailure
    from orchestrator.runtime.general_worker_authority import general_worker_host_scope
    from orchestrator.runtime.general_worker_journal import metadata_hash
    from tests.support.attempt_acceptance import ADMISSION
    from tests.unit.lifecycle.test_scheduler import routed_pair

    case = dispatch_case(tmp_path, register=False, max_attempts=100, retries=100, max_agents=100)
    routing, decision = case.routing, case.decision
    failure = ModelGatewayFailure(code="rate_limited", phase="provider", outcome="known_failure",
        retryable=True, http_status=429)

    def verify_other_terminal_paths(identity, number):
        import sqlite3
        from orchestrator.lifecycle import LifecycleController
        for entry in ("cancel", "reconcile"):
            clone = tmp_path / f"boundary-{number}-{entry}.db"
            with sqlite3.connect(clone) as destination:
                case.store._connection.backup(destination)
            with SQLiteEventStore(clone) as store:
                scheduler = Scheduler(store, limits=_application_limits())
                now = datetime.now(timezone.utc)
                if entry == "cancel":
                    LifecycleController(store).request_cancel("run-1", reason_code="user_request")
                    scheduler.acknowledge_cancellation(**identity, stopped_at=now,
                        stop_receipt_hash="sha256:" + "a" * 64, no_effect_receipt_hash="sha256:" + "b" * 64)
                else:
                    scheduler.finish_attempt(**identity, completed_at=now, outcome="outcome_unknown")
                    scheduler.reconcile_attempt(**identity, reconciled_at=now, outcome="failed", known_no_effect=True)
                assert any(e.event_type == "AttemptSlotReleased" and e.attempt_id == identity["attempt_id"]
                    for e in scheduler_events(store))

    try:

        with pytest.raises(ValidationError):
            NodeSpec(node_id="node-1", role="coder", planning_contract=case.contract,
                planning_contract_hash=case.contract.contract_hash, max_attempts=101)
        for number in range(1, 101):
            identity = dict(run_id="run-1", node_id="node-1", attempt_id=routing.attempt_id,
                fencing_generation=number)
            if number in (64, 65, 100):
                verify_other_terminal_paths(identity, number)
                binding = case.scheduler._resolve_general_verification_binding(**identity, as_of=datetime.now(timezone.utc))
                operation = case.journal.request(context=binding.context, input_commitment="sha256:" + "a" * 64,
                    accepted_route_sha256=metadata_hash(binding.active_attempt.accepted_route.model_dump(mode="json")),
                    workspace_identity_hash=case.lifecycle.replay("run-1").workspace_identity_hash,
                    host_scope_hash=general_worker_host_scope(case.store), output_byte_limit=16384)
                prepared, ack, stop = epoch()
                operation = case.journal.prepare_turn(operation, phase="request", prepared=prepared)
                with pytest.raises(SchedulerError):
                    case.scheduler.finish_attempt(**identity, completed_at=datetime.now(timezone.utc),
                        outcome="failed", known_no_effect=True)
                operation = case.journal.ack_turn(operation, receipt=ack)
                case.journal.stop_turn(operation, receipt=stop)
            case.scheduler.finish_attempt(**identity, completed_at=datetime.now(timezone.utc),
                outcome="failed", known_no_effect=True)
            if number == 100:
                break
            plan = case.scheduler.plan_attempt_recovery(**identity, source_decision=decision,
                contract=case.contract, failure=failure)
            assert plan.outcome == "authorized", (number, plan)
            routing, next_decision = routed_pair(case.registry, case.config, case.policy,
                attempt=number + 1, contract_hash=case.contract.contract_hash,
                recovery_action=plan.action, recovery_authorization=plan.authorization,
                prior_decision_hash=decision.decision_hash, previous_model_id="model-1",
                failure_category="transient", retry_level=number,
                exhausted_model_ids=() if number == 1 else ("model-1",))
            now = datetime.now(timezone.utc)
            case.scheduler.accept_routing(routing, next_decision, accepted_at=now,
                lease_expires_at=now + timedelta(hours=6), **ADMISSION)
            decision = next_decision
        assert len(case.lifecycle.replay("run-1").node("node-1").attempts) == 100
        assert sum(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store)) == 100
    finally:
        case.store.close()


def test_rolled_back_prepare_cannot_claim_another_journals_reinsert(tmp_path):
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
    case = dispatch_case(tmp_path, register=False)
    other = GeneralWorkerJournal(case.store)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            def nested(_nested_events, _nested_version):
                case.journal.prepare_turn(operation, phase="request", prepared=prepared)
                raise RuntimeError("abort savepoint")
            with pytest.raises(RuntimeError, match="abort savepoint"):
                case.store.append_checked("scheduler", "global", "undo-first", nested)
            other.prepare_turn(operation, phase="request", prepared=prepared)
        case.store.append_checked("scheduler", "global", "commit-second", outer)
        assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
        assert other._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()


@pytest.mark.parametrize("count", [100, 101])
def test_worker_census_at_supported_limit_and_above(tmp_path, monkeypatch, count):
    from orchestrator.runtime.general_worker_journal import _request_operation, _safe
    from orchestrator.runtime import general_worker_journal as journal_module
    case = dispatch_case(tmp_path, register=False)
    try:
        operations = []
        for number in range(count):
            registration = dict(case.registration)
            registration["context"] = registration["context"].model_copy(
                update={"attempt_id": f"census-{number}"})
            data = _safe(registration)
            operation = case.journal._append(_request_operation(data), "Requested", data)
            operations.append(operation)
        # The last indexed identity is deliberately left unstopped.
        operation = max(operations, key=lambda item: item.identity.stream_id)
        prepared, ack, stop = epoch()
        operation = case.journal._append(operation, "TurnPrepared", _safe(dict(phase="request", prepared=prepared)))
        checked = []
        original = journal_module._require_worker_stopped
        def record(candidate):
            checked.append(candidate.identity.stream_id)
            original(candidate)
        monkeypatch.setattr(journal_module, "_require_worker_stopped", record)
        if count == 101:
            with pytest.raises(ValueError, match="scope exceeds release bound"):
                case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                    outcome="failed", known_no_effect=True)
        else:
            with pytest.raises(ValueError):
                case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                    outcome="failed", known_no_effect=True)
            assert set(checked) == {item.identity.stream_id for item in operations}
            operation = case.journal.ack_turn(operation, receipt=ack)
            case.journal.stop_turn(operation, receipt=stop)
            case.scheduler.finish_attempt(**case.identity, completed_at=datetime.now(timezone.utc),
                outcome="failed", known_no_effect=True)
        released = any(e.event_type == "AttemptSlotReleased" for e in scheduler_events(case.store))
        assert released == (count == 100)
    finally:
        case.store.close()


def test_fresh_prepare_cannot_be_consumed_before_outer_commit(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            case.journal.prepare_turn(operation, phase="request", prepared=prepared)
            assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
        case.store.append_checked("scheduler", "global", "early-consume", outer)
        assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()


def test_nested_exact_replay_does_not_publish_fresh_permission(tmp_path):
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            ready = case.journal.prepare_turn(operation, phase="request", prepared=prepared)
            assert case.journal.prepare_turn(operation, phase="request", prepared=prepared) == ready
        case.store.append_checked("scheduler", "global", "nested-replay", outer)
        assert case.journal._fresh_prepare is None
        assert not case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()


def test_nested_prepare_publishes_after_outer_append_commit(tmp_path):
    from orchestrator.persistence import EventDraft
    case = dispatch_case(tmp_path, register=False)
    try:
        operation = case.journal.request(**case.registration)
        prepared = epoch()[0]
        def outer(_events, _version):
            case.journal.prepare_turn(operation, phase="request", prepared=prepared)
            assert case.journal._fresh_prepare is None
            return [EventDraft("NestedCommitObserved", {}, correlation_id="run-1")]
        case.store.append_checked("scheduler", "global", "nested-with-draft", outer)
        assert case.journal._consume_fresh_prepare(operation.identity, "request", prepared)
    finally:
        case.store.close()
