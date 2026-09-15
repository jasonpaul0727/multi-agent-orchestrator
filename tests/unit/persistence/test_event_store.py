from datetime import datetime
import sqlite3
import threading

import pytest
from pydantic import ValidationError

from orchestrator.persistence.events import EventDraft
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


def test_event_draft_rejects_blank_types_and_non_json_payloads():
    with pytest.raises(ValidationError):
        EventDraft("   ", {})
    with pytest.raises(ValidationError):
        EventDraft("RunCreated", {"bad": object()})
    with pytest.raises(ValidationError):
        EventDraft("RunCreated", {1: "non-string-key"})


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

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE events SET payload_hash = ? WHERE event_id = ?",
            ("0" * 64, event.event_id),
        )

    with pytest.raises(EventIntegrityError, match="payload_hash"):
        store.read_stream("run", "run-1")


def test_read_stream_rejects_a_tampered_payload(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    event = store.append(
        "run", "run-1", 0, [EventDraft("RunCreated", {"value": "original"})], "key"
    )[0]

    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE events SET payload_json = ? WHERE event_id = ?",
            ('{"value":"tampered"}', event.event_id),
        )

    with pytest.raises(EventIntegrityError, match="payload_hash"):
        store.read_stream("run", "run-1")


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
