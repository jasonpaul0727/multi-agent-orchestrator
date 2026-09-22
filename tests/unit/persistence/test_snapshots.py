import json
import sqlite3
import hashlib

import pytest

from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.snapshots import (
    SnapshotConflict,
    SnapshotIntegrityError,
    SnapshotStore,
    StaleSnapshot,
)
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore, canonical_json


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


def test_same_aggregate_replacement_updates_authenticated_metadata(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    snapshots.save("run", "run-1", state="Created", version=1, source_event_id="event-1")

    replacement = snapshots.save(
        "run", "run-1", state="Planning", version=2, source_event_id="event-2"
    )

    assert snapshots.load_valid("run", "run-1") == replacement


def test_stale_snapshot_version_cannot_overwrite_newer_checkpoint(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    latest = snapshots.save("run", "run-1", state="Planning", version=2)

    with pytest.raises(StaleSnapshot):
        snapshots.save("run", "run-1", state="Created", version=1)

    assert snapshots.load_valid("run", "run-1") == latest


def test_previous_metadata_hash_format_is_migrated_once_on_upgrade(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    saved = snapshots.save("run", "run-1", state="Created", version=1)
    snapshots.close()

    old_hash = hashlib.sha256(
        canonical_json(
            {
                "aggregate_id": saved.aggregate_id,
                "aggregate_type": saved.aggregate_type,
                "event_version": saved.event_version,
                "schema_version": saved.schema_version,
                "source_event_id": saved.source_event_id,
            }
        ).encode("utf-8")
    ).hexdigest()
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET metadata_hash = ? WHERE aggregate_id = ?",
            (old_hash, "run-1"),
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 3")

    upgraded = SnapshotStore(database)
    assert upgraded.load_valid("run", "run-1") is None
    with sqlite3.connect(database) as connection:
        migrated_hash = connection.execute(
            "SELECT metadata_hash FROM snapshots WHERE aggregate_id = ?",
            ("run-1",),
        ).fetchone()[0]
        assert migrated_hash is None
        assert connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = 3"
        ).fetchone() == (1,)


def test_upgrade_does_not_repair_arbitrary_current_metadata_tampering(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state="Created", version=1)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET metadata_hash = ? WHERE aggregate_id = ?",
            ("f" * 64, "run-1"),
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 3")

    upgraded = SnapshotStore(database)
    assert upgraded.load_valid("run", "run-1") is None


def test_upgrade_rejects_legacy_snapshot_with_tampered_created_at(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    saved = snapshots.save("run", "run-1", state="Created", version=1)
    snapshots.close()

    old_hash = hashlib.sha256(
        canonical_json(
            {
                "aggregate_id": saved.aggregate_id,
                "aggregate_type": saved.aggregate_type,
                "event_version": saved.event_version,
                "schema_version": saved.schema_version,
                "source_event_id": saved.source_event_id,
            }
        ).encode("utf-8")
    ).hexdigest()
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET created_at = ?, metadata_hash = ? WHERE aggregate_id = ?",
            ("2099-01-01T00:00:00+00:00", old_hash, "run-1"),
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 3")

    assert SnapshotStore(database).load_valid("run", "run-1") is None


def test_pre_metadata_hash_snapshot_is_unusable_after_migration(tmp_path):
    database = tmp_path / "snapshots.db"
    state_json = canonical_json("Created")
    state_hash = hashlib.sha256(state_json.encode("utf-8")).hexdigest()
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO schema_migrations (version) VALUES (1);
            CREATE TABLE snapshots (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_version INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                state_hash TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                source_event_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (aggregate_type, aggregate_id)
            );
            """
        )
        connection.execute(
            """
            INSERT INTO snapshots (
                aggregate_type, aggregate_id, event_version, state_json,
                state_hash, schema_version, source_event_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run",
                "run-1",
                99,
                state_json,
                state_hash,
                1,
                "tampered-source",
                "2099-01-01T00:00:00+00:00",
            ),
        )

    upgraded = SnapshotStore(database)
    assert upgraded.load_valid("run", "run-1") is None
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT metadata_hash FROM snapshots WHERE aggregate_id = ?",
            ("run-1",),
        ).fetchone()[0] is None


def test_same_version_conflicting_snapshot_is_rejected(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    original = snapshots.save("run", "run-1", state="Created", version=1)

    with pytest.raises(SnapshotConflict):
        snapshots.save("run", "run-1", state="Planning", version=1)

    assert snapshots.load_valid("run", "run-1") == original


def test_same_version_exact_snapshot_write_is_idempotent(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    original = snapshots.save(
        "run", "run-1", state="Created", version=1, source_event_id="event-1"
    )

    duplicate = snapshots.save(
        "run", "run-1", state="Created", version=1, source_event_id="event-1"
    )

    assert duplicate == original


def test_invalid_same_version_snapshot_can_be_replaced(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state="Created", version=1)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET metadata_hash = NULL WHERE aggregate_id = ?",
            ("run-1",),
        )

    replacement = SnapshotStore(database).save(
        "run", "run-1", state="Planning", version=1
    )

    assert SnapshotStore(database).load_valid("run", "run-1") == replacement


def test_non_numeric_event_version_snapshot_can_be_replaced(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots.save("run", "run-1", state="Created", version=1)
    snapshots.close()

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET event_version = ? WHERE aggregate_id = ?",
            ("not-a-version", "run-1"),
        )

    replacement = SnapshotStore(database).save(
        "run", "run-1", state="Planning", version=1
    )

    assert SnapshotStore(database).load_valid("run", "run-1") == replacement


def test_save_accepts_integer_state_when_version_is_explicit(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    with pytest.warns(DeprecationWarning, match="save_snapshot"):
        saved = snapshots.save("counter", "counter-1", 7, version=1)

    assert saved.state == 7
    assert saved.event_version == 1


def test_save_rejects_ambiguous_integer_positional_order(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    with pytest.raises(TypeError, match="integer"):
        snapshots.save("counter", "counter-1", 7, 1)


def test_legacy_positional_save_is_supported_with_deprecation_warning(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    with pytest.warns(DeprecationWarning, match="save_snapshot"):
        saved = snapshots.save(
            "run",
            "run-1",
            1,
            {"status": "Created"},
            source_event_id="event-1",
            schema_version=2,
        )

    assert saved.event_version == 1
    assert saved.state == {"status": "Created"}
    assert saved.schema_version == 2


def test_legacy_event_version_with_keyword_state_and_metadata_is_supported(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    with pytest.warns(DeprecationWarning, match="save_snapshot"):
        saved = snapshots.save(
            "run",
            "run-1",
            1,
            state={"status": "Created"},
            schema_version=2,
            source_event_id="event-1",
        )

    assert saved.event_version == 1
    assert saved.state == {"status": "Created"}
    assert saved.schema_version == 2
    assert saved.source_event_id == "event-1"


def test_state_first_positional_save_is_deprecated_but_deterministic(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")

    with pytest.warns(DeprecationWarning, match="save_snapshot"):
        saved = snapshots.save(
            "run", "run-1", {"status": "Created"}, 1, 2, "event-1"
        )

    assert saved.event_version == 1
    assert saved.schema_version == 2
    assert saved.source_event_id == "event-1"


def test_snapshot_save_legacy_and_integrity_arguments(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    with pytest.warns(DeprecationWarning, match="save_legacy"):
        saved = snapshots.save_legacy("run", "run-1", 1, {"status": "Ready"})
    assert saved.version == saved.source_version == 1
    assert saved.canonical_state_hash == saved.state_hash

    with pytest.raises(ValueError, match="state_hash does not match"):
        snapshots.save_snapshot(
            "run", "run-2", 1, {"status": "Ready"}, state_hash="0" * 64
        )
    with pytest.raises(ValueError, match="canonically JSON"):
        snapshots.save_snapshot("run", "run-3", 1, {"bad": float("nan")})


def test_snapshot_load_aliases_reject_conflicting_or_invalid_expectations(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    saved = snapshots.save(
        "run", "run-1", state={"status": "Ready"}, version=1
    )
    assert snapshots.load_valid("run", "run-1", version=1) == saved
    assert snapshots.load_valid("run", "run-1", expected_version=1) == saved
    assert snapshots.load_valid(
        "run", "run-1", schema_version=1, source_version=1
    ) == saved
    assert snapshots.load_valid("run", "run-1", version=2) is None
    assert snapshots.load_valid(
        "run", "run-1", expected_version=1, version=2
    ) is None
    with pytest.raises(ValueError, match="positive integer"):
        snapshots.load_valid("run", "run-1", expected_event_version=0)
    assert snapshots.load_valid("run", "missing") is None


def test_snapshot_save_rejects_ambiguous_or_duplicate_arguments(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    with pytest.raises(TypeError, match="at most five"):
        snapshots.save("run", "run-1", 1, {}, 1, "event", "hash", "extra")
    with pytest.raises(TypeError, match="state was provided twice"):
        snapshots.save("run", "run-1", {}, state={"duplicate": True})
    with pytest.raises(TypeError, match="version and event_version disagree"):
        snapshots.save(
            "run", "run-1", state={}, version=1, event_version=2
        )
    with pytest.raises(TypeError, match="requires version"):
        snapshots.save("run", "run-1", state={})
    with pytest.raises(TypeError, match="integer version"):
        snapshots.save("run", "run-1", state={}, version=True)


def test_load_valid_checks_source_event_and_schema_aliases(tmp_path):
    snapshots = SnapshotStore(tmp_path / "snapshots.db")
    saved = snapshots.save(
        "run", "run-1", state={}, version=1, schema_version=2, source_event_id="event-1"
    )
    assert snapshots.load_valid(
        "run",
        "run-1",
        expected_schema_version=2,
        expected_source_version=1,
        expected_source_event_id="event-1",
    ) == saved
    assert snapshots.load_valid(
        "run", "run-1", expected_source_event_id="different-event"
    ) is None
    assert snapshots.load_valid(
        "run", "run-1", expected_schema_version=1
    ) is None
    assert snapshots.load_valid(
        "run", "run-1", expected_source_version=2
    ) is None
    with pytest.raises(ValueError, match="expected_source_event_id"):
        snapshots.load_valid("run", "run-1", expected_source_event_id=" ")


def test_malformed_snapshot_json_is_unusable_and_owned_context_manager_closes(tmp_path):
    database = tmp_path / "snapshots.db"
    with SnapshotStore(database) as snapshots:
        snapshots.save("run", "run-1", state={}, version=1)
        connection = snapshots._connection
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE snapshots SET state_json = ? WHERE aggregate_id = ?",
            ("not-json", "run-1"),
        )
    assert SnapshotStore(database).load_valid("run", "run-1") is None


def test_snapshot_detects_malformed_current_version_after_write(tmp_path):
    database = tmp_path / "snapshots.db"
    snapshots = SnapshotStore(database)
    snapshots._connection.execute(
        """
        CREATE TRIGGER corrupt_snapshot_version AFTER INSERT ON snapshots
        BEGIN
            UPDATE snapshots SET event_version = 'not-an-integer'
            WHERE aggregate_type = NEW.aggregate_type AND aggregate_id = NEW.aggregate_id;
        END;
        """
    )
    with pytest.raises(SnapshotIntegrityError):
        snapshots.save("run", "run-1", state={}, version=1)
    assert snapshots.load_valid("run", "run-1") is None
