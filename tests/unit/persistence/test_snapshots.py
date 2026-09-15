import json
import sqlite3

import pytest

from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.snapshots import SnapshotStore
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


def test_snapshot_round_trip_persists_source_and_canonical_state_hash(tmp_path):
    database = tmp_path / "snapshots.db"
    events = SQLiteEventStore(database)
    source = events.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {"run_id": "run-1"})], "create"
    )[0]
    snapshots = SnapshotStore(database)

    saved = snapshots.save_snapshot(
        "run",
        "run-1",
        event_version=source.stream_version,
        state={"status": "Created", "nested": {"b": 2, "a": 1}},
        schema_version=1,
        source_event_id=source.event_id,
    )

    loaded = snapshots.load_valid(
        "run",
        "run-1",
        expected_schema_version=1,
        expected_source_version=source.stream_version,
        expected_source_event_id=source.event_id,
    )

    assert loaded == saved
    assert loaded.state == {"status": "Created", "nested": {"b": 2, "a": 1}}
    assert len(loaded.state_hash) == 64
    assert loaded.event_version == source.stream_version
    assert loaded.source_event_id == source.event_id


def test_load_valid_returns_none_for_tampered_state_hash(tmp_path):
    database = tmp_path / "snapshots.db"
    events = SQLiteEventStore(database)
    source = events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")[0]
    snapshots = SnapshotStore(database)
    snapshots.save_snapshot("run", "run-1", 1, {"status": "Created"}, 1, source.event_id)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET state_hash = ? WHERE aggregate_id = ?",
            ("0" * 64, "run-1"),
        )

    snapshots = SnapshotStore(database)
    assert snapshots.load_valid("run", "run-1", expected_schema_version=1) is None


def test_load_valid_returns_none_for_malformed_schema_and_source_version(tmp_path):
    database = tmp_path / "snapshots.db"
    events = SQLiteEventStore(database)
    source = events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")[0]
    snapshots = SnapshotStore(database)
    snapshots.save_snapshot("run", "run-1", 1, {"status": "Created"}, 1, source.event_id)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET schema_version = ? WHERE aggregate_id = ?",
            (0, "run-1"),
        )

    snapshots = SnapshotStore(database)
    assert snapshots.load_valid("run", "run-1", expected_schema_version=1) is None

    snapshots.close()
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET event_version = ? WHERE aggregate_id = ?",
            (2, "run-1"),
        )

    snapshots = SnapshotStore(database)
    assert (
        snapshots.load_valid(
            "run",
            "run-1",
            expected_schema_version=1,
            expected_source_version=1,
        )
        is None
    )


def test_load_valid_returns_none_for_tampered_state_payload(tmp_path):
    database = tmp_path / "snapshots.db"
    events = SQLiteEventStore(database)
    source = events.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")[0]
    snapshots = SnapshotStore(database)
    snapshots.save_snapshot("run", "run-1", 1, {"status": "Created"}, 1, source.event_id)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET state_json = ? WHERE aggregate_id = ?",
            (json.dumps({"status": "Tampered"}), "run-1"),
        )

    snapshots = SnapshotStore(database)
    assert snapshots.load_valid("run", "run-1", expected_schema_version=1) is None


def test_event_payload_tampering_is_detected_before_recovery_can_use_it(tmp_path):
    database = tmp_path / "events.db"
    events = SQLiteEventStore(database)
    source = events.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {"status": "Created"})], "create"
    )[0]
    events.close()

    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER events_immutable_update")
        connection.execute(
            "UPDATE events SET payload_json = ? WHERE event_id = ?",
            (json.dumps({"status": "Tampered"}), source.event_id),
        )

    reopened = SQLiteEventStore(database)
    try:
        reopened.read_stream("run", "run-1")
    except Exception as exc:
        assert type(exc).__name__ == "EventIntegrityError"
    else:
        raise AssertionError("tampered event payload was accepted")


def test_save_supports_documented_version_keyword_without_source_event(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    saved = snapshots.save("run", "run-1", state={"status": "Created"}, version=1)

    assert saved.event_version == 1
    assert saved.source_event_id is None
    assert snapshots.load_valid("run", "run-1") == saved


def test_snapshot_state_is_detached_and_deeply_immutable(tmp_path):
    source = {"nested": {"items": ["original"]}}
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    saved = snapshots.save("run", "run-1", state=source, version=1)

    source["nested"]["items"].append("source-only")

    assert saved.state == {"nested": {"items": ["original"]}}
    with pytest.raises(TypeError):
        saved.state["new"] = "not allowed"
    with pytest.raises(TypeError):
        saved.state["nested"]["items"].append("not allowed")


def test_load_valid_without_expected_metadata_rejects_tampered_version(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state={"status": "Created"}, version=1)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET event_version = ? WHERE aggregate_id = ?",
            (2, "run-1"),
        )

    assert SnapshotStore(database).load_valid("run", "run-1") is None


def test_current_snapshot_with_missing_metadata_is_not_repaired_on_reopen(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state={"status": "Created"}, version=1)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET metadata_hash = NULL WHERE aggregate_id = ?",
            ("run-1",),
        )

    reopened = SnapshotStore(database)
    assert reopened.load_valid("run", "run-1") is None


def test_snapshot_timestamp_is_authenticated_metadata(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state={"status": "Created"}, version=1)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET created_at = ? WHERE aggregate_id = ?",
            ("2099-01-01T00:00:00+00:00", "run-1"),
        )

    assert SnapshotStore(database).load_valid("run", "run-1") is None


def test_snapshot_store_accepts_a_raw_connection_with_an_active_transaction(tmp_path):
    connection = sqlite3.connect(tmp_path / "snapshots.db")
    connection.execute("BEGIN")
    snapshots = SnapshotStore(connection)

    saved = snapshots.save("run", "run-1", state={"status": "Created"}, version=1)

    assert snapshots.load_valid("run", "run-1") == saved


def test_save_accepts_integer_state_when_version_is_explicit(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    saved = snapshots.save("counter", "counter-1", 7, version=1)

    assert saved.state == 7
    assert saved.event_version == 1


def test_save_rejects_ambiguous_legacy_positional_order(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    with pytest.raises(TypeError, match="state first"):
        snapshots.save("run", "run-1", 1, {"status": "Created"})
