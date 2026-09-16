from __future__ import annotations

import hashlib
import sqlite3

import pytest

from orchestrator.artifacts import (
    ArtifactAccessDenied,
    ArtifactIntegrityError,
    ArtifactNotFound,
    ArtifactStore,
)
from orchestrator.persistence import SQLiteEventStore


def test_publish_returns_content_hash_and_is_atomic(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")

    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})

    assert record.digest == "sha256:" + hashlib.sha256(b"result").hexdigest()
    assert record.size == len(b"result")
    assert artifacts.read_bytes(record.digest) == b"result"
    assert not list((tmp_path / "artifacts").glob(".tmp-*"))


def test_publish_rejects_modified_content_after_hashing(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})

    artifacts.corrupt_for_test(record.digest)

    with pytest.raises(ArtifactIntegrityError):
        artifacts.read_bytes(record.digest)


def test_read_enforces_scope(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")
    record = artifacts.publish_bytes(
        b"secret",
        source={"run_id": "run-1"},
        readable_scope=("run-1",),
    )

    assert artifacts.read_bytes(record.digest, caller_scope="run-1") == b"secret"
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(record.digest, caller_scope="run-2")


@pytest.mark.parametrize("digest", ["../secret", "sha256:../" + "0" * 58, "sha256:" + "A" * 64])
def test_read_rejects_malformed_digest_and_traversal(tmp_path, digest):
    artifacts = ArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError):
        artifacts.read_bytes(digest)


def test_missing_digest_is_typed(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ArtifactNotFound):
        artifacts.read_bytes("sha256:" + "0" * 64)


def test_duplicate_content_is_idempotent_and_keeps_first_metadata(tmp_path):
    artifacts = ArtifactStore(tmp_path / "artifacts")
    first = artifacts.publish_bytes(
        b"same",
        artifact_type="model_output",
        source={"run_id": "run-1"},
    )
    duplicate = artifacts.publish_bytes(
        b"same",
        artifact_type="tool_output",
        source={"run_id": "run-2"},
    )

    assert duplicate == first
    assert artifacts.read_bytes(first.digest) == b"same"


def test_metadata_is_recorded_after_durable_publication_and_survives_reopen(tmp_path):
    database = tmp_path / "control.db"
    root = tmp_path / "artifacts"
    events = SQLiteEventStore(database)
    artifacts = ArtifactStore(root, event_store=events)

    record = artifacts.publish_bytes(
        b"tool result",
        artifact_type="tool_output",
        media_type="text/plain",
        source={
            "run_id": "run-1",
            "node_id": "node-1",
            "attempt_id": "attempt-1",
            "tool": "shell",
            "model": "gpt-test",
        },
        schema_version=2,
        redaction_state="redacted",
        readable_scope=("run-1",),
        references=("attempt-1",),
        lifecycle_state="retained",
    )

    stored = events.read_stream("artifact", record.digest)
    assert len(stored) == 1
    assert stored[0].event_type == "ArtifactPublished"
    assert stored[0].payload["digest"] == record.digest
    assert (root / record.digest.removeprefix("sha256:")).is_file()

    events.close()
    reopened_events = SQLiteEventStore(database)
    reopened = ArtifactStore(root, event_store=reopened_events)
    assert reopened.read_bytes(record.digest, caller_scope="run-1") == b"tool result"
    assert reopened.get_record(record.digest) == record


def test_tampered_event_metadata_is_integrity_failure(tmp_path):
    database = tmp_path / "control.db"
    events = SQLiteEventStore(database)
    artifacts = ArtifactStore(tmp_path / "artifacts", event_store=events)
    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})

    events.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER IF EXISTS events_immutable_update")
        connection.execute(
            "UPDATE events SET payload_json = ? WHERE stream_id = ?",
            ('{"digest":"sha256:' + "0" * 64 + '"}', record.digest),
        )

    reopened = ArtifactStore(tmp_path / "artifacts", event_store=SQLiteEventStore(database))
    with pytest.raises(ArtifactIntegrityError):
        reopened.get_record(record.digest)
