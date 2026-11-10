"""Bounded scope-index foundation for the disconnected v2 registry."""
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from orchestrator.persistence.mediation_identity import mediation_stream_id
OP = mediation_stream_id("run-1", "node-1", "attempt-node-1-1", 1)

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.mediation_journal import MediationJournal
from tests.unit.runtime.test_mediation_review_fixes import operation


from contextlib import contextmanager


@contextmanager
def admitted_store(path):
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(path.parent, register=False)
    try:
        yield case.store
    finally:
        case.store.close()

def test_scope_is_visible_after_commit_on_an_independent_connection(tmp_path):
    path = tmp_path / "events.db"
    ready, committed = Event(), Event()
    with admitted_store(path) as store:
        def read():
            with SQLiteEventStore(path) as other:
                ready.set()
                assert committed.wait(10)
                return other.stream_ids_for_mediation_node("run-1", "node-1", limit=2)
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(read)
            assert ready.wait(10)
            MediationJournal(store).register(operation())
            committed.set()
            assert result.result(timeout=10) == [OP]


def test_scope_registration_rolls_back_with_outer_write(tmp_path):
    with admitted_store(tmp_path / "events.db") as store:
        def rollback(_events, _version):
            MediationJournal(store).register(operation())
            raise RuntimeError("abort")
        with pytest.raises(RuntimeError):
            store.append_checked("scheduler", "global", "scope-rollback", rollback)
        assert store.stream_ids_for_mediation_node("run-1", "node-1", limit=2) == []
        assert MediationJournal(store).load(OP) is None


def test_migration_indexes_existing_v2_history(tmp_path):
    path = tmp_path / "events.db"
    with admitted_store(path) as store:
        MediationJournal(store).register(operation())
        conn = store._connection
        conn.execute("DROP TRIGGER mediation_scope_check")
        conn.execute("DROP TRIGGER mediation_scope_insert")
        conn.execute("DROP TRIGGER mediation_scope_immutable_update")
        conn.execute("DROP TRIGGER mediation_scope_immutable_delete")
        conn.execute("DROP TABLE mediation_scope")
        conn.execute("DELETE FROM schema_migrations WHERE version=6")
    with SQLiteEventStore(path) as reopened:
        assert reopened.stream_ids_for_mediation_node("run-1", "node-1", limit=2) == [OP]


def test_legacy_migration_rejects_first_event_scope_disagreeing_with_payload(tmp_path):
    import sqlite3
    path = tmp_path / "events.db"
    with admitted_store(path) as store:
        MediationJournal(store).register(operation())
        conn = store._connection
        for trigger in ("mediation_scope_check", "mediation_scope_insert",
                        "mediation_scope_immutable_update", "mediation_scope_immutable_delete",
                        "events_immutable_update"):
            conn.execute("DROP TRIGGER " + trigger)
        conn.execute("DROP TABLE mediation_scope")
        conn.execute("DELETE FROM schema_migrations WHERE version=6")
        conn.execute("UPDATE events SET run_id='different-run' WHERE stream_type='mediation_v2'")
    with pytest.raises(ValueError):
        SQLiteEventStore(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT 1 FROM schema_migrations WHERE version=6").fetchone() is None
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mediation_scope'").fetchone() is None


@pytest.mark.parametrize("damage", ["missing", "moved"])
def test_scope_query_rejects_hidden_target(tmp_path, damage):
    with admitted_store(tmp_path / "events.db") as store:
        MediationJournal(store).register(operation())
        conn = store._connection
        if damage == "missing":
            conn.execute("DROP TRIGGER mediation_scope_immutable_delete")
            conn.execute("DELETE FROM mediation_scope")
        else:
            conn.execute("DROP TRIGGER mediation_scope_immutable_update")
            conn.execute("UPDATE mediation_scope SET run_id='other-run'")
        with pytest.raises(ValueError):
            store.stream_ids_for_mediation_node("run-1", "node-1", limit=2)


@pytest.mark.parametrize("damage", ["version_lag", "missing_version"])
def test_legacy_migration_rejects_physical_version_corruption(tmp_path, damage):
    import sqlite3
    import uuid
    path = tmp_path / "events.db"
    with admitted_store(path) as store:
        MediationJournal(store).register(operation())
        conn = store._connection
        for trigger in ("mediation_scope_check", "mediation_scope_insert",
                        "mediation_scope_immutable_update", "mediation_scope_immutable_delete"):
            conn.execute("DROP TRIGGER " + trigger)
        conn.execute("DROP TABLE mediation_scope")
        conn.execute("DELETE FROM schema_migrations WHERE version=6")
        if damage == "version_lag":
            conn.execute("""INSERT INTO events SELECT ?, stream_type, stream_id, 100, event_type,
                schema_version, occurred_at, payload_json, payload_hash, ?, run_id, node_id,
                attempt_id, fencing_generation, correlation_id, causation_id FROM events
                WHERE stream_type='mediation_v2' AND stream_version=1""",
                (str(uuid.uuid4()), str(uuid.uuid4())))
        else:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("DELETE FROM stream_versions WHERE stream_type='mediation_v2'")
    with pytest.raises(ValueError):
        SQLiteEventStore(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT 1 FROM schema_migrations WHERE version=6").fetchone() is None


def test_operation_id_cannot_be_chosen_by_caller_even_with_valid_commitment(tmp_path):
    from dataclasses import asdict
    from orchestrator.runtime.mediation_actors import MediationOperation
    from orchestrator.runtime.mediation_journal import MediationJournalError
    from tests.unit.runtime.test_mediation_review_fixes import digest
    data = asdict(operation())
    del data["commitment_sha256"]
    data["operation_id"] = "caller-chosen"
    with admitted_store(tmp_path / "events.db") as store:
        with pytest.raises(MediationJournalError):
            MediationJournal(store).register(MediationOperation(**data, commitment_sha256=digest(data)))


def test_same_identity_cannot_register_different_commitments(tmp_path):
    from dataclasses import asdict
    from orchestrator.runtime.mediation_actors import MediationOperation
    from orchestrator.runtime.mediation_journal import MediationJournalError
    from tests.unit.runtime.test_mediation_review_fixes import digest, J
    original = operation()
    data = asdict(original)
    del data["commitment_sha256"]
    data["workspace_sha256"] = J
    changed = MediationOperation(**data, commitment_sha256=digest(data))
    with admitted_store(tmp_path / "events.db") as store:
        journal = MediationJournal(store)
        first = journal.register(original)
        assert journal.register(original) == first
        with pytest.raises(MediationJournalError):
            journal.register(changed)


def test_non_admitted_operation_is_not_treated_as_authorized(tmp_path):
    from dataclasses import asdict
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    from orchestrator.runtime.mediation_actors import MediationOperation
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_mediation_review_fixes import digest
    case = dispatch_case(tmp_path, register=False)
    try:
        data = asdict(operation())
        del data["commitment_sha256"]
        data.update(run_id=case.identity["run_id"], node_id=case.identity["node_id"],
                    attempt_id="unadmitted", fencing_generation=7)
        data["operation_id"] = mediation_stream_id(data["run_id"], data["node_id"],
                                                    data["attempt_id"], data["fencing_generation"])
        from orchestrator.runtime.mediation_journal import MediationJournalError
        with pytest.raises(MediationJournalError):
            MediationJournal(case.store).register(MediationOperation(**data, commitment_sha256=digest(data)))
    finally:
        case.store.close()


@pytest.mark.parametrize("damage", ["moved", "deleted"])
def test_census_detects_v2_hidden_in_scope_and_event_headers(tmp_path, damage):
    from dataclasses import asdict
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    from orchestrator.runtime.mediation_actors import MediationOperation
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_mediation_review_fixes import digest
    case = dispatch_case(tmp_path, register=False)
    data = asdict(operation())
    del data["commitment_sha256"]
    data.update(case.identity)
    data["operation_id"] = mediation_stream_id(**case.identity)
    candidate = MediationOperation(**data, commitment_sha256=digest(data))
    try:
        MediationJournal(case.store).register(candidate)
        conn = case.store._connection
        if damage == "moved":
            conn.execute("DROP TRIGGER events_immutable_update")
            conn.execute("DROP TRIGGER mediation_scope_immutable_update")
            conn.execute("UPDATE events SET run_id='hidden-run' WHERE stream_type='mediation_v2'")
            conn.execute("UPDATE mediation_scope SET run_id='hidden-run'")
        else:
            conn.execute("DROP TRIGGER events_immutable_delete")
            conn.execute("DROP TRIGGER mediation_scope_immutable_delete")
            conn.execute("DELETE FROM events WHERE stream_type='mediation_v2'")
            conn.execute("DELETE FROM mediation_scope")
        gate = GeneralWorkerGate(case.store)
        def check(_events, _version):
            gate.check_release(case.identity["run_id"], case.identity["node_id"])
        with pytest.raises(ValueError):
            case.store.append_checked("scheduler", "global", "hidden-target-test", check)
    finally:
        case.store.close()


def test_legacy_caller_chosen_operation_is_rejected_without_reinterpretation(tmp_path):
    import hashlib
    import json
    import sqlite3
    from tests.unit.runtime.test_mediation_review_fixes import digest
    path = tmp_path / "events.db"
    with admitted_store(path) as store:
        MediationJournal(store).register(operation())
        conn = store._connection
        for trigger in ("mediation_scope_check", "mediation_scope_insert",
                        "mediation_scope_immutable_update", "mediation_scope_immutable_delete",
                        "events_immutable_update"):
            conn.execute("DROP TRIGGER " + trigger)
        conn.execute("DROP TABLE mediation_scope")
        conn.execute("DELETE FROM schema_migrations WHERE version=6")
        conn.execute("PRAGMA foreign_keys=OFF")
        payload = json.loads(conn.execute("SELECT payload_json FROM events WHERE stream_type='mediation_v2'").fetchone()[0])
        payload["operation_id"] = "legacy-chosen"
        payload["data"]["operation_id"] = "legacy-chosen"
        payload["data"]["commitment_sha256"] = digest(
            {k: v for k, v in payload["data"].items() if k != "commitment_sha256"})
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        hashed = hashlib.sha256(raw.encode()).hexdigest()
        conn.execute("""UPDATE events SET stream_id='legacy-chosen', payload_json=?, payload_hash=?,
            idempotency_key=?, causation_id=? WHERE stream_type='mediation_v2'""",
            (raw, hashed, "sha256:" + hashed, payload["data"]["commitment_sha256"]))
        conn.execute("UPDATE stream_versions SET stream_id='legacy-chosen' WHERE stream_type='mediation_v2'")
        conn.execute("UPDATE idempotency_records SET stream_id='legacy-chosen' WHERE stream_type='mediation_v2'")
    with pytest.raises(ValueError):
        SQLiteEventStore(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT stream_id FROM events WHERE stream_type='mediation_v2'").fetchone()[0] == "legacy-chosen"
        assert conn.execute("SELECT 1 FROM schema_migrations WHERE version=6").fetchone() is None


def test_new_v2_registration_without_real_admission_is_rejected(tmp_path):
    from orchestrator.runtime.mediation_journal import MediationJournalError
    with SQLiteEventStore(tmp_path / "empty.db") as store:
        with pytest.raises(MediationJournalError):
            MediationJournal(store).register(operation())
        assert MediationJournal(store).load(OP) is None


def test_v2_census_detects_hidden_v1_physical_rows_with_lagging_version(tmp_path):
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        operation_v1 = case.journal.request(**case.registration)
        conn = case.store._connection
        conn.execute("DROP TRIGGER gw_scope_immutable_delete")
        conn.execute("DELETE FROM general_worker_scope WHERE stream_id=?", (operation_v1.identity.stream_id,))
        conn.execute("UPDATE stream_versions SET current_version=0 WHERE stream_type='general_worker' AND stream_id=?",
                     (operation_v1.identity.stream_id,))
        gate = GeneralWorkerGate(case.store)
        def check(_events, _version):
            gate.check_v2_against_v1(case.identity["run_id"], case.identity["node_id"])
        with pytest.raises(ValueError):
            case.store.append_checked("scheduler", "global", "hidden-v1-test", check)
    finally:
        case.store.close()


def test_unadmitted_inert_v2_history_is_not_legitimate_census(tmp_path):
    from dataclasses import asdict
    from orchestrator.persistence import EventDraft
    from orchestrator.runtime.general_worker_gate import GeneralWorkerGate
    from orchestrator.runtime.mediation_actors import MediationOperation
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_mediation_review_fixes import digest
    case = dispatch_case(tmp_path, register=False)
    try:
        data = asdict(operation())
        del data["commitment_sha256"]
        data.update(run_id=case.identity["run_id"], node_id=case.identity["node_id"],
                    attempt_id="unadmitted", fencing_generation=7)
        data["operation_id"] = mediation_stream_id(data["run_id"], data["node_id"],
                                                    data["attempt_id"], data["fencing_generation"])
        candidate = MediationOperation(**data, commitment_sha256=digest(data))
        identity = dict(run_id=candidate.run_id, node_id=candidate.node_id,
                        attempt_id=candidate.attempt_id, fencing_generation=candidate.fencing_generation)
        payload = dict(schema_version=2, operation_id=candidate.operation_id,
                       action="Registered", data=asdict(candidate))
        draft = EventDraft("MediationRegistered", payload, **identity,
                           correlation_id=candidate.run_id, causation_id=candidate.commitment_sha256)
        # Inject inert metadata to exercise recovery, not a valid admission.
        case.store.append("mediation_v2", candidate.operation_id, 0, [draft], digest(payload))
        gate = GeneralWorkerGate(case.store)
        def check(_events, _version):
            gate.check_release(case.identity["run_id"], case.identity["node_id"])
        with pytest.raises(ValueError, match="no admitted attempt"):
            case.store.append_checked("scheduler", "global", "unadmitted-history-test", check)
    finally:
        case.store.close()


def test_inert_intent_outer_rollback_cannot_leak_launch_authority(tmp_path):
    from tests.unit.runtime.test_mediation_review_fixes import intent, record_inert
    from orchestrator.runtime.mediation_journal import MediationJournalError
    with admitted_store(tmp_path / "events.db") as store:
        journal = MediationJournal(store)
        journal.register(operation())
        actor = intent()
        def rollback(_events, _version):
            record_inert(journal, OP, actor)
            raise RuntimeError("abort")
        with pytest.raises(RuntimeError):
            store.append_checked("scheduler", "global", "intent-rollback", rollback)
        assert journal.load(OP).actors == ()
        with pytest.raises(MediationJournalError):
            journal.permit(OP, actor)


def test_terminal_census_uses_release_specific_context_index(tmp_path):
    with admitted_store(tmp_path / "events.db") as store:
        plan = store._connection.execute("""EXPLAIN QUERY PLAN SELECT event_id FROM events
            WHERE stream_type='scheduler' AND stream_id='global' AND event_type='AttemptSlotReleased'
              AND run_id=? AND node_id=? AND attempt_id=? AND fencing_generation=?
            ORDER BY stream_version LIMIT 2""",
            ("run-1", "node-1", "attempt-node-1-1", 1)).fetchall()
        assert any("ix_scheduler_release_context" in row["detail"] for row in plan)
