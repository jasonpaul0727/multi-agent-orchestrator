import sqlite3

import pytest

from orchestrator.persistence.sqlite_event_store import initialize_schema


def test_capacity_contract_migrates_and_reopens(tmp_path):
    path = tmp_path / "capacity.db"
    with sqlite3.connect(path) as db:
        initialize_schema(db)
        assert db.execute("SELECT max_actors, max_events, max_record_bytes, max_history_bytes FROM mediation_capacity_contract").fetchone() == (128, 387, 16384, 6340608)
        assert db.execute("SELECT 1 FROM schema_migrations WHERE version=7").fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE mediation_capacity_contract SET max_history_bytes=262144")
    with sqlite3.connect(path) as db:
        initialize_schema(db)
        assert db.execute("SELECT count(*) FROM mediation_capacity_contract").fetchone() == (1,)


def test_capacity_reserves_every_outstanding_evidence_and_seal():
    from orchestrator.persistence.mediation_capacity import fits_capacity
    assert fits_capacity(events=130, payload_bytes=130 * 16384, outstanding_acks=128, outstanding_stops=128, closed=True)
    assert fits_capacity(events=386, payload_bytes=386 * 16384, outstanding_acks=0, outstanding_stops=0, closed=True)
    assert not fits_capacity(events=131, payload_bytes=0, outstanding_acks=128, outstanding_stops=128, closed=True)
    assert not fits_capacity(events=130, payload_bytes=130 * 16384 + 1, outstanding_acks=128, outstanding_stops=128, closed=True)
    assert not fits_capacity(events=0, payload_bytes=0, outstanding_acks=129, outstanding_stops=0, closed=True)
    assert not fits_capacity(events=True, payload_bytes=0, outstanding_acks=0, outstanding_stops=0, closed=True)


def test_migration_rollback_and_two_connections_preserve_existing_data(tmp_path):
    path = tmp_path / "old.db"
    first = sqlite3.connect(path)
    second = sqlite3.connect(path)
    try:
        initialize_schema(first)
        first.execute("DROP TABLE mediation_capacity_contract")
        first.execute("DELETE FROM schema_migrations WHERE version=7")
        first.execute("INSERT INTO stream_versions VALUES (?, ?, ?)", ("legacy", "old", 0))
        first.commit()
        first.execute("BEGIN IMMEDIATE")
        initialize_schema(first)
        assert first.execute("SELECT version FROM schema_migrations WHERE version=7").fetchone()
        assert not second.execute("SELECT version FROM schema_migrations WHERE version=7").fetchone()
        first.rollback()
        assert not second.execute("SELECT version FROM schema_migrations WHERE version=7").fetchone()
        initialize_schema(second)
        initialize_schema(first)
        assert first.execute("SELECT current_version FROM stream_versions WHERE stream_type=? AND stream_id=?", ("legacy", "old")).fetchone() == (0,)
        assert first.execute("SELECT count(*) FROM mediation_capacity_contract").fetchone() == (1,)
    finally:
        first.close()
        second.close()


def test_missing_contract_fails_closed_on_reopen(tmp_path):
    path = tmp_path / "missing.db"
    with sqlite3.connect(path) as db:
        initialize_schema(db)
        db.execute("DELETE FROM mediation_capacity_contract")
    with sqlite3.connect(path) as db:
        with pytest.raises(ValueError, match="capacity contract"):
            initialize_schema(db)


@pytest.mark.parametrize("values", [(-1, 0, 0, 0), (0, -1, 0, 0),
    (0, 0, -1, 0), (0, 0, 0, -1), (2**63, 0, 0, 0),
    (0, 2**63, 0, 0), (0, 0, 2**63, 0)])
def test_capacity_rejects_invalid_or_overflow_values(values):
    from orchestrator.persistence.mediation_capacity import fits_capacity
    assert not fits_capacity(events=values[0], payload_bytes=values[1],
                             outstanding_acks=values[2], outstanding_stops=values[3], closed=True)


def test_open_stream_reserves_close_and_sqlite_row_factory(tmp_path):
    from orchestrator.persistence.mediation_capacity import fits_capacity
    from orchestrator.persistence import SQLiteEventStore
    assert fits_capacity(events=129, payload_bytes=129 * 16384, outstanding_acks=128, outstanding_stops=128, closed=False)
    assert not fits_capacity(events=130, payload_bytes=0, outstanding_acks=128, outstanding_stops=128, closed=False)
    store = SQLiteEventStore(tmp_path / "rows.db")
    store.close()
    store = SQLiteEventStore(tmp_path / "rows.db")
    store.close()


def test_v6_inert_history_survives_contract_migration(tmp_path):
    from orchestrator.runtime.mediation_journal import MediationJournal
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    from tests.unit.runtime.test_mediation_review_fixes import operation, intent, record_inert
    case = dispatch_case(tmp_path, register=False)
    try:
        journal = MediationJournal(case.store)
        op = operation()
        journal.register(op)
        before = record_inert(journal, op.operation_id, intent())
        db = case.store._connection
        db.execute("DROP TABLE mediation_capacity_contract")
        db.execute("DELETE FROM schema_migrations WHERE version=7")
        initialize_schema(db)
        assert journal.load(op.operation_id) == before
        assert before.actors[0].status == "launch_unknown"
    finally:
        case.store.close()
