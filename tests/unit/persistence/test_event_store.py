from datetime import datetime
import hashlib
import sqlite3
import threading

import pytest
from pydantic import ValidationError

from orchestrator.persistence.events import EventContractError, EventDraft
from orchestrator.persistence import sqlite_event_store as sqlite_event_store_module
from orchestrator.persistence.sqlite_event_store import (
    EventIntegrityError,
    IdempotencyConflict,
    SQLiteEventStore,
    StaleStream,
)


def test_append_assigns_monotonic_stream_versions(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")

    first = store.append(
        "run",
        "run-1",
        expected_version=0,
        events=[
            EventDraft("RunCreated", {"run_id": "run-1"}),
            EventDraft("InputAccepted", {"run_id": "run-1"}),
        ],
        idempotency_key="create-run-1",
    )
    second = store.append(
        "run",
        "run-1",
        expected_version=2,
        events=[EventDraft("PlanningStarted", {"run_id": "run-1"})],
        idempotency_key="plan-run-1",
    )

    assert [event.stream_version for event in first + second] == [1, 2, 3]
    assert store.current_version("run", "run-1") == 3


def test_stale_expected_version_does_not_append(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create-run-1")

    with pytest.raises(StaleStream):
        store.append("run", "run-1", 0, [EventDraft("InputAccepted", {})], "stale")

    assert len(store.read_stream("run", "run-1")) == 1
    assert store.current_version("run", "run-1") == 1


def test_repeating_an_idempotency_key_returns_original_events(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    draft = [EventDraft("RunCreated", {"run_id": "run-1"})]

    first = store.append("run", "run-1", 0, draft, "same-key")
    store.append(
        "run",
        "run-1",
        1,
        [EventDraft("InputAccepted", {"run_id": "run-1"})],
        "next-key",
    )
    repeated = store.append("run", "run-1", 0, draft, "same-key")

    assert repeated == first
    assert len(store.read_stream("run", "run-1")) == 2


def test_same_idempotency_key_with_different_payload_is_rejected(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store.append("run", "run-1", 0, [EventDraft("RunCreated", {"n": 1})], "same-key")

    with pytest.raises(IdempotencyConflict):
        store.append("run", "run-1", 0, [EventDraft("RunCreated", {"n": 2})], "same-key")

    assert len(store.read_stream("run", "run-1")) == 1


def test_read_stream_after_version_returns_only_later_events(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store.append(
        "run",
        "run-1",
        0,
        [
            EventDraft("RunCreated", {"run_id": "run-1"}),
            EventDraft("InputAccepted", {"run_id": "run-1"}),
            EventDraft("PlanningStarted", {"run_id": "run-1"}),
        ],
        "initial-events",
    )

    later = store.read_stream("run", "run-1", after_version=1)

    assert [event.stream_version for event in later] == [2, 3]
    assert [event.event_type for event in later] == ["InputAccepted", "PlanningStarted"]


def test_events_persist_across_reopened_connection(tmp_path):
    database = tmp_path / "events.db"
    first_store = SQLiteEventStore(database)
    first = first_store.append(
        "attempt",
        "attempt-1",
        0,
        [EventDraft("AttemptStarted", {"attempt_id": "attempt-1"})],
        "start-attempt-1",
    )
    first_store.close()

    reopened = SQLiteEventStore(database)

    assert reopened.read_stream("attempt", "attempt-1") == first
    assert reopened.current_version("attempt", "attempt-1") == 1


def test_schema_migration_adds_execution_context_without_rewriting_legacy_events(tmp_path):
    database = tmp_path / "legacy.db"
    payload_hash = hashlib.sha256(b"{}").hexdigest()
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO schema_migrations (version) VALUES (1), (2), (3);
            CREATE TABLE stream_versions (
                stream_type TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                current_version INTEGER NOT NULL,
                PRIMARY KEY (stream_type, stream_id)
            );
            INSERT INTO stream_versions VALUES ('run', 'legacy-run', 1);
            CREATE TABLE events (
                event_id TEXT NOT NULL UNIQUE,
                stream_type TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                stream_version INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                correlation_id TEXT,
                causation_id TEXT,
                PRIMARY KEY (event_id)
            );
            """
        )
        connection.execute(
            """
            INSERT INTO events (
                event_id, stream_type, stream_id, stream_version, event_type,
                schema_version, occurred_at, payload_json, payload_hash,
                idempotency_key, correlation_id, causation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-event",
                "run",
                "legacy-run",
                1,
                "RunCreated",
                1,
                "2026-09-01T00:00:00+00:00",
                "{}",
                payload_hash,
                "create-run",
                None,
                None,
            ),
        )

    store = SQLiteEventStore(database)
    event = store.read_stream("run", "legacy-run")[0]

    assert event.event_id == "legacy-event"
    assert event.run_id is None
    assert event.attempt_id is None
    assert store._connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version = 4"
    ).fetchone() is not None


def test_event_draft_rejects_blank_types_and_non_json_payloads():
    with pytest.raises(ValidationError):
        EventDraft("   ", {})
    with pytest.raises(ValidationError):
        EventDraft("RunCreated", {"bad": object()})
    with pytest.raises(ValidationError):
        EventDraft("RunCreated", {1: "non-string-key"})


@pytest.mark.parametrize(
    "payload",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": "lone-\ud800-surrogate"},
        {"nested": [{"value": object()}]},
    ],
)
def test_event_draft_rejects_non_deterministic_json_values(payload):
    with pytest.raises(ValidationError):
        EventDraft("RunCreated", payload)


def test_event_draft_positional_forms_and_duplicate_arguments_are_validated():
    assert EventDraft("RunCreated", {}).payload == {}
    assert EventDraft(event_type="RunCreated", payload={}).event_type == "RunCreated"
    with pytest.raises(TypeError, match="at most"):
        EventDraft("RunCreated", {}, {})
    with pytest.raises(TypeError, match="both positionally"):
        EventDraft("RunCreated", event_type="InputAccepted")


@pytest.mark.parametrize("events", [None, [], [object()]])
def test_append_rejects_empty_noniterable_or_invalid_event_batches(tmp_path, events):
    store = SQLiteEventStore(tmp_path / "events.db")
    with pytest.raises(ValueError, match="events"):
        store.append("run", "run-1", 0, events, "invalid-events")


def test_append_checked_noop_and_failure_are_transactional(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    assert store.append_checked("run", "run-1", "no-op", lambda events, version: None) == []
    assert not store._connection.in_transaction

    with pytest.raises(RuntimeError, match="decision failed"):
        store.append_checked(
            "run", "run-1", "failure", lambda events, version: (_ for _ in ()).throw(RuntimeError("decision failed"))
        )
    assert not store._connection.in_transaction
    assert store.read_stream("run", "run-1") == []


def test_append_inside_outer_transaction_uses_savepoint_and_can_roll_back(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store._connection.execute("BEGIN")
    store.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {})], "create-in-outer"
    )
    assert store._connection.in_transaction
    assert store.current_version("run", "run-1") == 1
    store._connection.rollback()
    assert store.current_version("run", "run-1") == 0
    assert store.read_stream("run", "run-1") == []


def test_nested_append_checked_shares_the_outer_atomic_transaction(tmp_path):
    store = SQLiteEventStore(tmp_path / "nested-checked.db")

    def outer_decide(events, version):
        store.append_checked(
            "budget",
            "run-1",
            "nested-reserve",
            lambda nested_events, nested_version: [EventDraft("BudgetReserved", {"amount": 5})],
        )
        return [EventDraft("RoutingDecisionAccepted", {"accepted": True}, run_id="run-1",
                           node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
                           correlation_id="run-1", causation_id="decision-1")]

    store.append_checked("scheduler", "global", "outer-accept", outer_decide)
    assert store.read_stream("budget", "run-1")[0].payload["amount"] == 5
    assert store.read_stream("scheduler", "global")[0].payload["accepted"] is True

    def failing_outer(events, version):
        store.append_checked(
            "budget", "run-2", "nested-rollback",
            lambda nested_events, nested_version: [EventDraft("BudgetReserved", {"amount": 7})],
        )
        raise RuntimeError("abort outer transaction")

    with pytest.raises(RuntimeError, match="abort outer"):
        store.append_checked("scheduler", "global", "outer-rollback", failing_outer)
    assert store.read_stream("budget", "run-2") == []


def test_read_stream_with_version_respects_existing_transaction(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")
    store._connection.execute("BEGIN")
    events, version = store.read_stream_with_version("run", "run-1")
    assert [event.event_type for event in events] == ["RunCreated"]
    assert version == 1
    assert store._connection.in_transaction
    store._connection.rollback()


def test_stream_ids_are_sorted_and_context_manager_closes_connection(tmp_path):
    database = tmp_path / "events.db"
    with SQLiteEventStore(database) as store:
        store.append("run", "run-z", 0, [EventDraft("RunCreated", {})], "z")
        store.append("run", "run-a", 0, [EventDraft("RunCreated", {})], "a")
        assert store.stream_ids("run") == ["run-a", "run-z"]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        store.current_version("run", "run-a")


def test_idempotency_retry_inside_outer_transaction_releases_only_savepoint(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    draft = [EventDraft("RunCreated", {})]
    store._connection.execute("BEGIN")
    first = store.append("run", "run-1", 0, draft, "create")
    repeated = store.append("run", "run-1", 0, draft, "create")
    assert repeated == first
    assert store._connection.in_transaction
    store._connection.rollback()
    assert store.read_stream("run", "run-1") == []


def test_append_contract_failure_rolls_back_nested_savepoint(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    store._connection.execute("BEGIN")
    with pytest.raises(EventContractError, match="prior EffectIntentRecorded"):
        store.append(
            "run",
            "run-1",
            0,
            [
                EventDraft(
                    "EffectReceiptRecorded",
                    {"effect_id": "effect-1"},
                    run_id="run-1",
                    node_id="node-1",
                    attempt_id="attempt-1",
                    fencing_generation=1,
                    causation_id="intent-1",
                )
            ],
            "receipt-first",
        )
    assert store._connection.in_transaction
    assert store.read_stream("run", "run-1") == []
    store._connection.rollback()


def test_snapshot_reader_rolls_back_nested_savepoint_on_integrity_failure(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    event = store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "create")[0]
    store._connection.execute("DROP TRIGGER events_immutable_update")
    store._connection.execute(
        "UPDATE events SET payload_json = ? WHERE event_id = ?", ("{", event.event_id)
    )
    store._connection.commit()
    store._connection.execute("BEGIN")
    with pytest.raises(EventIntegrityError):
        store.read_stream_with_version("run", "run-1")
    assert store._connection.in_transaction
    store._connection.rollback()


def test_wal_initialization_retries_transient_locks(monkeypatch):
    class Cursor:
        def __init__(self, value=None):
            self.value = value

        def fetchone(self):
            return (self.value,)

    class Connection:
        row_factory = None

        def __init__(self):
            self.wal_reads = 0
            self.closed = False

        def execute(self, statement):
            if statement == "PRAGMA journal_mode":
                self.wal_reads += 1
                if self.wal_reads == 1:
                    raise sqlite3.OperationalError("database is locked")
                return Cursor("wal")
            return Cursor()

        def close(self):
            self.closed = True

    connection = Connection()
    sleeps = []
    monkeypatch.setattr(sqlite_event_store_module.sqlite3, "connect", lambda *args, **kwargs: connection)
    monkeypatch.setattr(sqlite_event_store_module.time, "sleep", sleeps.append)

    assert sqlite_event_store_module._open_connection("unused.db") is connection
    assert connection.wal_reads == 2
    assert sleeps


def test_wal_initialization_closes_connection_when_sqlite_refuses_wal(monkeypatch):
    class Cursor:
        def __init__(self, value):
            self.value = value

        def fetchone(self):
            return (self.value,)

    class Connection:
        row_factory = None

        def __init__(self):
            self.closed = False

        def execute(self, statement):
            if statement in {"PRAGMA journal_mode", "PRAGMA journal_mode = WAL"}:
                return Cursor("delete")
            return Cursor(None)

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(sqlite_event_store_module.sqlite3, "connect", lambda *args, **kwargs: connection)
    with pytest.raises(sqlite3.OperationalError, match="refused WAL"):
        sqlite_event_store_module._open_connection("unused.db")
    assert connection.closed


@pytest.mark.parametrize(
    ("stream_type", "stream_id", "expected_version", "idempotency_key"),
    [
        ("", "run-1", 0, "key"),
        ("run", "", 0, "key"),
        ("run", "run-1", -1, "key"),
        ("run", "run-1", True, "key"),
        ("run", "run-1", 0, ""),
    ],
)
def test_append_rejects_invalid_stream_arguments(
    tmp_path, stream_type, stream_id, expected_version, idempotency_key
):
    store = SQLiteEventStore(tmp_path / "events.db")

    with pytest.raises(ValueError):
        store.append(
            stream_type,
            stream_id,
            expected_version,
            [EventDraft("RunCreated", {})],
            idempotency_key,
        )


def test_stored_event_has_frozen_and_typed_boundary_fields(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    event = store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "key")[0]

    assert event.stream_version == 1
    assert event.schema_version == 1
    assert isinstance(event.occurred_at, datetime)
    with pytest.raises(ValidationError):
        event.stream_version = 2


def test_stored_event_payload_is_deeply_immutable_and_hash_consistent(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    source_payload = {"nested": {"value": 1}, "items": ["original"]}
    event = store.append(
        "run", "run-1", 0, [EventDraft("RunCreated", source_payload)], "key"
    )[0]

    source_payload["nested"]["value"] = 99
    source_payload["items"].append("source-only")

    with pytest.raises(TypeError):
        event.payload["new"] = "not allowed"
    with pytest.raises(TypeError):
        event.payload["nested"]["value"] = 2
    with pytest.raises(TypeError):
        event.payload["items"].append("not allowed")

    reread = store.read_stream("run", "run-1")[0]
    assert reread.payload == {"nested": {"value": 1}, "items": ["original"]}
    assert reread.payload_hash == event.payload_hash


def test_read_stream_rejects_a_tampered_payload_hash(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    event = store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "key")[0]

    store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER IF EXISTS events_immutable_update")
        connection.execute(
            "UPDATE events SET payload_hash = ? WHERE event_id = ?",
            ("0" * 64, event.event_id),
        )

    store = SQLiteEventStore(database)
    with pytest.raises(EventIntegrityError, match="payload_hash"):
        store.read_stream("run", "run-1")


def test_read_stream_rejects_a_tampered_payload(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    event = store.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {"value": "original"})], "key"
    )[0]

    store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER IF EXISTS events_immutable_update")
        connection.execute(
            "UPDATE events SET payload_json = ? WHERE event_id = ?",
            ('{"value":"tampered"}', event.event_id),
        )

    store = SQLiteEventStore(database)
    with pytest.raises(EventIntegrityError, match="payload_hash"):
        store.read_stream("run", "run-1")


def test_event_rows_reject_direct_sql_update_and_delete(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    event = store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "key")[0]

    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE events SET payload_hash = ? WHERE event_id = ?",
                ("0" * 64, event.event_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM events WHERE event_id = ?", (event.event_id,))

    assert store.read_stream("run", "run-1") == [event]


def test_store_connection_is_thread_affine(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    errors = []

    def read_from_another_thread():
        try:
            store.current_version("run", "run-1")
        except BaseException as exc:  # sqlite3 raises ProgrammingError here.
            errors.append(exc)

    thread = threading.Thread(target=read_from_another_thread)
    thread.start()
    thread.join()

    assert len(errors) == 1
    assert isinstance(errors[0], sqlite3.ProgrammingError)
    assert "thread" in (SQLiteEventStore.__doc__ or "").lower()


def test_copies_of_frozen_payloads_remain_immutable(tmp_path):
    import copy

    store = SQLiteEventStore(tmp_path / "events.db")
    event = store.append(
        "run",
        "run-1",
        0,
        [EventDraft("RunCreated", {"nested": {"items": [1]}})],
        "key",
    )[0]

    copied_payloads = [
        copy.copy(event.payload),
        copy.deepcopy(event.payload),
        event.model_copy(deep=True).payload,
    ]
    for payload in copied_payloads:
        with pytest.raises(TypeError):
            payload["new"] = "not allowed"
        with pytest.raises(TypeError):
            payload["nested"]["items"].append(2)
        assert payload == event.payload


def test_read_stream_rejects_a_malformed_non_ascii_payload_hash(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    event = store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "key")[0]

    store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER IF EXISTS events_immutable_update")
        connection.execute(
            "UPDATE events SET payload_hash = ? WHERE event_id = ?",
            ("é" * 64, event.event_id),
        )

    store = SQLiteEventStore(database)
    with pytest.raises(EventIntegrityError, match="payload_hash"):
        store.read_stream("run", "run-1")


def test_identifier_and_event_payload_surrogates_are_rejected_with_field_context(tmp_path):
    with pytest.raises(ValidationError, match="event_type"):
        EventDraft("bad\ud800", {})
    with pytest.raises(ValidationError, match="payload"):
        EventDraft("RunCreated", {"text": "bad\ud800"})

    store = SQLiteEventStore(tmp_path / "events.db")
    with pytest.raises(ValueError, match="stream_id"):
        store.append("run", "bad\ud800", 0, [EventDraft("RunCreated", {})], "key")
    with pytest.raises(ValueError, match="idempotency_key"):
        store.append("run", "run-1", 0, [EventDraft("RunCreated", {})], "bad\ud800")
