from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import sqlite3
import threading
from pathlib import Path

import pytest
from pydantic import ValidationError

from orchestrator.artifacts import (
    ArtifactAccessDenied,
    ArtifactAccessGrant,
    ArtifactFilesystemError,
    ArtifactIntegrityError,
    ArtifactMetadataError,
    ArtifactNotFound,
    ArtifactRecord,
    ArtifactStore,
)
from orchestrator.artifacts import store as artifact_store_module
from orchestrator.persistence import SQLiteEventStore


def _grant(record, *, scope=("run-1",), expires_at=None, signature="valid"):
    return ArtifactAccessGrant(
        digest=record.digest,
        scope=scope,
        expires_at=expires_at or datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="control-plane",
        signature=signature,
    )


def _store(root, event_store=None):
    return ArtifactStore(
        root,
        event_store=event_store,
        grant_verifier=lambda grant: grant.signature == "valid",
    )


def test_publish_returns_content_hash_and_is_atomic(tmp_path):
    artifacts = _store(tmp_path / "artifacts")

    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})

    assert record.digest == "sha256:" + hashlib.sha256(b"result").hexdigest()
    assert record.size == len(b"result")
    assert artifacts.read_bytes(record.digest, grant=_grant(record)) == b"result"
    assert not list((tmp_path / "artifacts").glob(".tmp-*"))


def test_publish_rejects_modified_content_after_hashing(tmp_path):
    artifacts = _store(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", source={"run_id": "run-1"})

    with artifacts._path_for_digest(record.digest).open("ab") as artifact:
        artifact.write(b"corrupt")

    with pytest.raises(ArtifactIntegrityError):
        artifacts.read_bytes(record.digest, grant=_grant(record))
    with pytest.raises(ArtifactIntegrityError):
        artifacts.get_record(record.digest)


def test_read_enforces_scope(tmp_path):
    artifacts = _store(tmp_path / "artifacts")
    record = artifacts.publish_bytes(
        b"secret",
        source={"run_id": "run-1"},
        readable_scope=("run-1",),
    )

    assert artifacts.read_bytes(record.digest, grant=_grant(record)) == b"secret"
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(record.digest, grant=_grant(record, scope=("run-2",)))
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(record.digest)
    with pytest.raises(TypeError, match="authenticated grant"):
        artifacts.read_bytes(record.digest, caller_scope="run-1")


def test_read_requires_verifier_and_authenticated_grant(tmp_path):
    plain = ArtifactStore(tmp_path / "plain")
    record = plain.publish_bytes(b"result")
    with pytest.raises(ArtifactAccessDenied):
        plain.read_bytes(record.digest, grant=_grant(record))

    artifacts = _store(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(record.digest, grant=_grant(record, signature="forged"))
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(record.digest, grant=_grant(record, scope=("run-2",)))
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(
            record.digest,
            grant=ArtifactAccessGrant(
                digest="sha256:" + "0" * 64,
                scope=("run-1",),
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
                issuer="control-plane",
                signature="valid",
            ),
        )
    with pytest.raises(ArtifactAccessDenied):
        artifacts.read_bytes(
            record.digest,
            grant=_grant(record, expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)),
        )


@pytest.mark.parametrize("digest", ["../secret", "sha256:../" + "0" * 58, "sha256:" + "A" * 64])
def test_read_rejects_malformed_digest_and_traversal(tmp_path, digest):
    artifacts = _store(tmp_path / "artifacts")

    with pytest.raises(ValueError):
        artifacts.read_bytes(digest)


def test_missing_digest_is_typed(tmp_path):
    artifacts = _store(tmp_path / "artifacts")

    with pytest.raises(ArtifactNotFound):
        artifacts.read_bytes("sha256:" + "0" * 64)


def test_duplicate_content_deduplicates_bytes_but_preserves_provenance(tmp_path):
    events = SQLiteEventStore(tmp_path / "control.db")
    artifacts = _store(tmp_path / "artifacts", events)
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

    assert duplicate.digest == first.digest
    assert duplicate.publication_id != first.publication_id
    assert artifacts.list_publications(first.digest) == [first, duplicate]
    assert artifacts.read_bytes(first.digest, grant=_grant(first, scope=("run-1",))) == b"same"
    assert artifacts.read_bytes(first.digest, grant=_grant(duplicate, scope=("run-2",))) == b"same"


def test_concurrent_duplicate_publications_keep_both_provenances(tmp_path):
    database = tmp_path / "control.db"
    root = tmp_path / "artifacts"
    barrier = threading.Barrier(2)
    records = []
    errors = []

    def publish(index):
        events = SQLiteEventStore(database)
        artifacts = _store(root, events)
        try:
            barrier.wait()
            records.append(
                artifacts.publish_bytes(
                    b"same",
                    source={"run_id": f"run-{index}"},
                    readable_scope=(f"run-{index}",),
                )
            )
        except BaseException as exc:  # pragma: no cover - failure detail below
            errors.append(exc)
        finally:
            events.close()

    threads = [threading.Thread(target=publish, args=(index,)) for index in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(records) == 2
    assert records[0].digest == records[1].digest
    reopened = _store(root, SQLiteEventStore(database))
    publications = reopened.list_publications(records[0].digest)
    assert {publication.source_run_id for publication in publications} == {"run-1", "run-2"}


def test_metadata_is_recorded_after_durable_publication_and_survives_reopen(tmp_path):
    database = tmp_path / "control.db"
    root = tmp_path / "artifacts"
    events = SQLiteEventStore(database)
    artifacts = _store(root, events)

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
    reopened = _store(root, reopened_events)
    assert reopened.read_bytes(record.digest, grant=_grant(record)) == b"tool result"
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

    reopened = _store(tmp_path / "artifacts", SQLiteEventStore(database))
    with pytest.raises(ArtifactIntegrityError):
        reopened.get_record(record.digest)


@pytest.mark.parametrize("root", ["", ".", "..", "artifacts", "/", "/tmp", str(__file__)])
def test_root_must_be_dedicated_absolute_directory(tmp_path, root):
    with pytest.raises((ValueError, ArtifactFilesystemError)):
        ArtifactStore(root)

    with pytest.raises((ValueError, ArtifactFilesystemError)):
        ArtifactStore(str(tmp_path / "nested" / ".." / "artifacts"))


def test_root_rejects_cwd_workspace_and_symlink_targets(tmp_path):
    with pytest.raises(ValueError):
        ArtifactStore(str(Path.cwd()))

    symlink_target = tmp_path / "target"
    symlink_target.mkdir()
    symlink = tmp_path / "link"
    symlink.symlink_to(symlink_target, target_is_directory=True)
    with pytest.raises(ValueError):
        ArtifactStore(str(symlink / "artifacts"))

    valid = tmp_path / "dedicated" / "artifacts"
    store = ArtifactStore(str(valid))
    assert valid.is_dir()
    store.close()


def test_direct_models_validate_digest_metadata_and_whitespace():
    valid_digest = "sha256:" + "a" * 64
    valid_grant = ArtifactAccessGrant(
        digest=valid_digest,
        scope=("run-1",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        issuer="issuer",
        signature="signature",
    )
    assert valid_grant.digest == valid_digest
    assert ArtifactRecord(digest=valid_digest).digest == valid_digest
    assert ArtifactRecord(digest=valid_digest, type="tool_output").artifact_type == "tool_output"

    with pytest.raises(ValidationError):
        ArtifactRecord(digest="sha256:" + "A" * 64)
    with pytest.raises(ValidationError):
        ArtifactRecord(digest="not-a-digest")
    with pytest.raises(ValidationError):
        ArtifactRecord(digest=valid_digest, artifact_type=" ")
    with pytest.raises(ValidationError):
        ArtifactRecord(digest=valid_digest, artifact_type=" type")
    with pytest.raises(ValidationError):
        ArtifactRecord(digest=valid_digest, readable_scope=())
    with pytest.raises(ValidationError):
        ArtifactAccessGrant(
            digest=valid_digest,
            scope=("run-1",),
            expires_at=datetime.now() + timedelta(minutes=1),
            issuer="issuer",
            signature="signature",
        )
    assert not hasattr(ArtifactStore, "corrupt_for_test")


def test_append_failure_cleans_new_object_but_not_existing_dedup(tmp_path):
    events = SQLiteEventStore(tmp_path / "control.db")
    artifacts = _store(tmp_path / "artifacts", events)
    original = artifacts.publish_bytes(b"same", source={"run_id": "run-1"})
    path = artifacts._path_for_digest(original.digest)

    original_append = events.append

    def fail_append(*args, **kwargs):
        raise RuntimeError("append unavailable")

    events.append = fail_append
    with pytest.raises(ArtifactMetadataError):
        artifacts.publish_bytes(b"new", source={"run_id": "run-2"})
    assert not (tmp_path / "artifacts" / (hashlib.sha256(b"new").hexdigest())).exists()

    with pytest.raises(ArtifactMetadataError):
        artifacts.publish_bytes(b"same", source={"run_id": "run-2"})
    assert path.is_file()
    events.append = original_append


def test_filesystem_failures_are_typed_and_leave_no_staging(tmp_path, monkeypatch):
    artifacts = _store(tmp_path / "artifacts")

    def fail_replace(*args, **kwargs):
        raise OSError("replace unavailable")

    monkeypatch.setattr(artifact_store_module.os, "replace", fail_replace)
    with pytest.raises(ArtifactFilesystemError):
        artifacts.publish_bytes(b"result")
    assert not list((tmp_path / "artifacts").glob(".tmp-*"))

    monkeypatch.undo()
    def fail_fsync(*args, **kwargs):
        raise OSError("fsync unavailable")

    monkeypatch.setattr(artifact_store_module.os, "fsync", fail_fsync)
    with pytest.raises(ArtifactFilesystemError):
        artifacts.publish_bytes(b"fsync-failure")
    assert not list((tmp_path / "artifacts").glob(".tmp-*"))

    monkeypatch.undo()
    monkeypatch.setattr(artifact_store_module.os, "open", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("open unavailable")))
    with pytest.raises(ArtifactFilesystemError):
        artifacts.publish_bytes(b"other")
    assert not list((tmp_path / "artifacts").glob(".tmp-*"))
