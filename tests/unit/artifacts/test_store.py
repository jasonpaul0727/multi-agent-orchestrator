from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import multiprocessing
import os
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

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
from orchestrator.persistence import EventDraft


def _publish_from_process(root, database, index, queue):
    events = SQLiteEventStore(database)
    artifacts = _store(root, events)
    try:
        record = artifacts.publish_bytes(
            b"same-process-race",
            source={"run_id": f"run-{index}"},
            readable_scope=(f"run-{index}",),
        )
        queue.put(("ok", record.publication_id))
    except BaseException as exc:  # pragma: no cover - assertion reports detail
        queue.put(("error", type(exc).__name__, str(exc)))
    finally:
        events.close()


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
        artifacts.get_record(record.digest, _grant(record))


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


def test_metadata_reads_require_authenticated_grant_and_filter_provenance(tmp_path):
    artifacts = _store(tmp_path / "artifacts")
    first = artifacts.publish_bytes(
        b"metadata-secret",
        source={"run_id": "run-1"},
        readable_scope=("run-1",),
    )
    second = artifacts.publish_bytes(
        b"metadata-secret",
        source={"run_id": "run-2"},
        readable_scope=("run-2",),
    )

    for method in (
        lambda: artifacts.get_record(first.digest),
        lambda: artifacts.list_publications(first.digest),
        lambda: artifacts.get_records(first.digest),
    ):
        with pytest.raises(ArtifactAccessDenied):
            method()

    assert artifacts.get_record(first.digest, _grant(first, scope=("run-2",))) == second
    assert artifacts.list_publications(first.digest, _grant(first, scope=("run-2",))) == [second]

    grant = _grant(first, scope=("run-1",))
    assert artifacts.get_record(first.digest, grant) == first
    assert artifacts.get_records(first.digest, grant) == [first]
    assert artifacts.list_publications(first.digest, grant) == [first]
    assert second.source_run_id not in {
        publication.source_run_id for publication in artifacts.list_publications(first.digest, grant)
    }


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
    assert artifacts.list_publications(first.digest, _grant(first, scope=("run-1",))) == [first, duplicate]
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
    publications = reopened.list_publications(records[0].digest, _grant(records[0], scope=("run-1",)))
    assert {publication.source_run_id for publication in publications} == {"run-1"}
    all_publications = reopened.list_publications(records[0].digest, _grant(records[0], scope=("*",)))
    assert {publication.source_run_id for publication in all_publications} == {"run-1", "run-2"}


@pytest.mark.skipif(os.name == "nt", reason="cross-process file-lock test uses POSIX process semantics")
def test_cross_process_duplicate_publications_keep_committed_file(tmp_path):
    database = tmp_path / "control.db"
    root = tmp_path / "artifacts"
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    processes = [
        context.Process(target=_publish_from_process, args=(str(root), str(database), index, queue))
        for index in (1, 2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
    assert all(process.exitcode == 0 for process in processes)
    outcomes = [queue.get(timeout=5) for _ in processes]
    assert all(outcome[0] == "ok" for outcome in outcomes), outcomes

    reopened = _store(root, SQLiteEventStore(database))
    content_digest = "sha256:" + hashlib.sha256(b"same-process-race").hexdigest()
    grant = ArtifactAccessGrant(
        digest=content_digest,
        scope=("*",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="control-plane",
        signature="valid",
    )
    publications = reopened.list_publications(content_digest, grant)
    assert len(publications) == 2
    assert reopened.read_bytes(content_digest, grant=grant) == b"same-process-race"
    assert reopened._path_for_digest(content_digest).is_file()


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
    assert reopened.get_record(record.digest, _grant(record)) == record


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
        reopened.get_record(record.digest, _grant(record))


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


def test_artifact_metadata_normalizes_mapping_scopes_and_exposes_source_fields(tmp_path):
    artifacts = _store(tmp_path / "artifacts")
    record = artifacts.publish_bytes(
        b"normalized",
        source={
            "run_id": "run-1",
            "node_id": "node-1",
            "attempt_id": "attempt-1",
            "tool": "pytest",
            "model": "model-a",
            "optional": None,
        },
        readable_scope={"run": ["run-1", "run-2"]},
        references={"artifact": "sha256:" + "a" * 64},
    )
    assert record.source_run_id == "run-1"
    assert record.source_node_id == "node-1"
    assert record.source_attempt_id == "attempt-1"
    assert record.source_tool == "pytest"
    assert record.source_model == "model-a"
    assert record.type == record.artifact_type
    assert record.content_hash == record.digest
    assert record.source["optional"] is None
    assert "run:run-1" in record.readable_scope
    assert "artifact:sha256:" + "a" * 64 in record.references

    mapped_grant = _grant(record, scope={"run": ["run-1"]})
    assert "run:run-1" in mapped_grant.scope
    assert artifacts.read_bytes(record.digest, grant=mapped_grant) == b"normalized"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"source": ["not", "a", "mapping"]},
        {"source": {"run_id": "bad\nvalue"}},
        {"readable_scope": b"bytes-are-not-scope-labels"},
        {"readable_scope": {"run": 3}},
        {"references": object()},
    ],
)
def test_invalid_artifact_metadata_is_rejected_before_publication(tmp_path, kwargs):
    artifacts = _store(tmp_path / "artifacts")
    with pytest.raises((TypeError, ValueError)):
        artifacts.publish_bytes(b"invalid-metadata", **kwargs)
    assert not list((tmp_path / "artifacts").glob(".tmp-*"))


def test_artifact_rejects_surrogates_and_control_characters_in_text_fields():
    digest = "sha256:" + "a" * 64
    with pytest.raises(ValidationError, match="lone surrogate"):
        ArtifactRecord(digest=digest, artifact_type="bad\ud800type")
    with pytest.raises(ValidationError, match="control characters"):
        ArtifactAccessGrant(
            digest=digest,
            scope=("run-1",),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            issuer="bad\nissuer",
            signature="valid",
        )
    with pytest.raises(ValidationError):
        ArtifactRecord(digest=digest, created_at=datetime.now())


class _ObjectVerifier:
    def verify(self, grant):
        return True


class _RaisingVerifier:
    def verify(self, grant):
        raise RuntimeError("denied")


@pytest.mark.parametrize(
    ("verifier", "accepted"),
    [(_ObjectVerifier(), True), (_RaisingVerifier(), False)],
)
def test_verifier_objects_are_supported_and_fail_closed(tmp_path, verifier, accepted):
    artifacts = ArtifactStore(tmp_path / type(verifier).__name__, verifier=verifier)
    record = artifacts.publish_bytes(b"verified", readable_scope=("run-1",))
    grant = _grant(record)
    if accepted:
        assert artifacts.read_bytes(record.digest, access_grant=grant) == b"verified"
    else:
        with pytest.raises(ArtifactAccessDenied):
            artifacts.read_bytes(record.digest, access_grant=grant)


def test_artifact_grant_aliases_conflict_and_unbounded_metadata_query_is_denied(tmp_path):
    artifacts = _store(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    grant = _grant(record)
    with pytest.raises(TypeError, match="mutually exclusive"):
        artifacts.read_bytes(record.digest, grant, access_grant=grant)
    with pytest.raises(ArtifactAccessDenied):
        artifacts.get_records()


def test_owned_artifact_store_closes_its_metadata_database(tmp_path):
    with ArtifactStore(
        tmp_path / "owned-artifacts", grant_verifier=lambda grant: True
    ) as artifacts:
        artifacts.publish_bytes(b"owned", readable_scope=("run-1",))
        metadata_store = artifacts._event_store
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        metadata_store.current_version("artifact", "missing")


@pytest.mark.parametrize("bad_configuration", ["database-path", "duplicate-verifier"])
def test_artifact_store_rejects_unsafe_constructor_capabilities(tmp_path, bad_configuration):
    root = tmp_path / bad_configuration
    if bad_configuration == "database-path":
        with pytest.raises(TypeError, match="capability"):
            ArtifactStore(root, event_store=tmp_path / "events.db")
    else:
        with pytest.raises(TypeError, match="only one"):
            ArtifactStore(root, verifier=lambda grant: True, grant_verifier=lambda grant: True)


class _TamperingMetadataStore:
    def __init__(self, mutation):
        self.events = []
        self.mutation = mutation

    def current_version(self, stream_type, stream_id):
        return len(self.events)

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        draft = events[0]
        payload = dict(draft.payload)
        if self.mutation == "size":
            payload["size"] += 1
        elif self.mutation == "digest":
            payload["digest"] = "sha256:" + "0" * 64
        elif self.mutation == "not-object":
            payload = None
        event = SimpleNamespace(
            event_id="event-1",
            event_type="ArtifactPublished",
            payload=payload,
        )
        self.events.append(event)
        return [event]

    def read_stream(self, stream_type, stream_id):
        return list(self.events)


def test_artifact_store_rejects_mismatched_metadata_event_content(tmp_path):
    metadata = _TamperingMetadataStore("size")
    artifacts = _store(tmp_path / "size-mismatch", metadata)
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    with pytest.raises(ArtifactIntegrityError, match="size mismatch"):
        artifacts.read_bytes(record.digest, grant=_grant(record))


@pytest.mark.parametrize("mutation", ["digest", "not-object"])
def test_artifact_store_rejects_invalid_metadata_event_envelopes(tmp_path, mutation):
    metadata = _TamperingMetadataStore(mutation)
    artifacts = _store(tmp_path / mutation, metadata)
    with pytest.raises(ArtifactMetadataError) as failure:
        artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    assert isinstance(failure.value.__cause__, ArtifactIntegrityError)


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

    def fail_link(*args, **kwargs):
        raise OSError("link unavailable")

    monkeypatch.setattr(artifact_store_module.os, "link", fail_link)
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


@pytest.mark.parametrize("metadata_path_kind", ["symlink", "directory"])
def test_existing_metadata_database_must_be_regular_file(tmp_path, metadata_path_kind):
    root = tmp_path / "artifacts"
    root.mkdir()
    metadata_path = root / ".metadata.db"
    if metadata_path_kind == "symlink":
        target = tmp_path / "metadata-target"
        target.touch()
        metadata_path.symlink_to(target)
    else:
        metadata_path.mkdir()

    with pytest.raises(ArtifactMetadataError):
        ArtifactStore(root, grant_verifier=lambda grant: True)


def test_windows_directory_durability_path_is_supported(monkeypatch, tmp_path):
    artifacts = _store(tmp_path / "artifacts")
    monkeypatch.setattr(artifact_store_module.os, "name", "nt")
    monkeypatch.delattr(artifact_store_module.os, "O_DIRECTORY", raising=False)
    artifacts._fsync_directory()


@pytest.mark.parametrize("result", [1, "true", object(), None])
def test_verifier_must_return_bool(tmp_path, result):
    artifacts = ArtifactStore(tmp_path / "artifacts", grant_verifier=lambda grant: result)
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    with pytest.raises(ArtifactAccessDenied):
        artifacts.get_record(record.digest, _grant(record))


def test_async_verifier_is_rejected_without_unawaited_coroutine_warning(tmp_path, recwarn):
    async def verify(grant):
        return True

    artifacts = ArtifactStore(tmp_path / "artifacts", grant_verifier=verify)
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    with pytest.raises(ArtifactAccessDenied):
        artifacts.get_record(record.digest, _grant(record))
    assert not [warning for warning in recwarn if warning.category is RuntimeWarning]


def test_legacy_metadata_normalizes_only_when_replaying(tmp_path):
    database = tmp_path / "control.db"
    root = tmp_path / "artifacts"
    events = SQLiteEventStore(database)
    artifacts = ArtifactStore(root, event_store=events)
    content = b"legacy"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    path = artifacts._path_for_digest(digest)
    path.write_bytes(content)
    payload = {
        "digest": digest,
        "type": " legacy-output ",
        "size": len(content),
        "media_type": " text/plain ",
        "source": {" run_id ": " run-legacy "},
        "schema_version": 1,
        "redaction_state": " unknown ",
        "readable_scope": [" run-legacy "],
        "references": [],
        "lifecycle_state": " retained ",
        "created_at": datetime.now(timezone.utc).isoformat(),
        # Deliberately omit publication_id for the legacy event format.
    }
    events.append(
        "artifact",
        digest,
        0,
        [EventDraft("ArtifactPublished", payload)],
        "legacy-publication",
    )
    events.close()

    reopened_events = SQLiteEventStore(database)
    reopened = _store(root, reopened_events)
    record = reopened.get_record(
        digest,
        ArtifactAccessGrant(
            digest=digest,
            scope=("run-legacy",),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            issuer="control-plane",
            signature="valid",
        ),
    )
    assert record.artifact_type == "legacy-output"
    assert record.source_run_id == "run-legacy"
    assert record.readable_scope == ("run-legacy",)
    assert reopened.read_bytes(digest, grant=_grant(record, scope=("run-legacy",))) == content

    # A legacy empty scope remains readable as metadata by the parser but
    # authorizes no grant, preserving its prior deny-all meaning.
    empty_digest = "sha256:" + hashlib.sha256(b"legacy-empty").hexdigest()
    empty_path = reopened._path_for_digest(empty_digest)
    empty_path.write_bytes(b"legacy-empty")
    empty_payload = dict(payload)
    empty_payload.update(
        {
            "digest": empty_digest,
            "size": len(b"legacy-empty"),
            "readable_scope": [],
        }
    )
    reopened_events.append(
        "artifact",
        empty_digest,
        0,
        [EventDraft("ArtifactPublished", empty_payload)],
        "legacy-empty-publication",
    )
    assert reopened._load_records(empty_digest)[0].readable_scope == ()
    with pytest.raises(ArtifactAccessDenied):
        reopened.get_record(empty_digest, _grant(record, scope=("run-legacy",)))
    reopened_events.close()


class _BrokenReadStore:
    def current_version(self, stream_type, stream_id):
        return 0

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        return []

    def read_stream(self, stream_type, stream_id):
        raise RuntimeError("metadata read unavailable")


class _BrokenIterationStore(_BrokenReadStore):
    def read_stream(self, stream_type, stream_id):
        class ExplodingIterable:
            def __iter__(self):
                raise RuntimeError("metadata iteration unavailable")

        return ExplodingIterable()


class _CommitThenFailStore:
    def __init__(self, delegate):
        self.delegate = delegate

    def current_version(self, stream_type, stream_id):
        return self.delegate.current_version(stream_type, stream_id)

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        result = self.delegate.append(
            stream_type, stream_id, expected_version, events, idempotency_key
        )
        raise RuntimeError("append response lost after commit")

    def read_stream(self, stream_type, stream_id):
        return self.delegate.read_stream(stream_type, stream_id)


@pytest.mark.parametrize("event_store", [_BrokenReadStore(), _BrokenIterationStore()])
def test_event_store_read_and_iteration_failures_are_typed(tmp_path, event_store):
    artifacts = _store(tmp_path / (type(event_store).__name__), event_store)
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    with pytest.raises(ArtifactMetadataError):
        artifacts.get_record(record.digest, _grant(record))


def test_append_commit_then_error_never_deletes_referenced_object(tmp_path):
    database = tmp_path / "control.db"
    delegate = SQLiteEventStore(database)
    events = _CommitThenFailStore(delegate)
    artifacts = _store(tmp_path / "artifacts", events)
    content = b"committed-before-error"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()

    with pytest.raises(ArtifactMetadataError):
        artifacts.publish_bytes(content, readable_scope=("run-1",))

    assert artifacts._path_for_digest(digest).is_file()
    reopened = _store(tmp_path / "artifacts", delegate)
    grant = ArtifactAccessGrant(
        digest=digest,
        scope=("run-1",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="control-plane",
        signature="valid",
    )
    assert reopened.read_bytes(digest, grant=grant) == content
    delegate.close()


@pytest.mark.parametrize("path_kind", ["directory", "dangling_symlink"])
def test_digest_named_nonfile_is_integrity_failure(tmp_path, path_kind):
    artifacts = _store(tmp_path / "artifacts")
    record = artifacts.publish_bytes(b"result", readable_scope=("run-1",))
    path = artifacts._path_for_digest(record.digest)
    path.unlink()
    if path_kind == "directory":
        path.mkdir()
    else:
        path.symlink_to(tmp_path / "does-not-exist")

    with pytest.raises(ArtifactIntegrityError):
        artifacts.get_record(record.digest, _grant(record))


@pytest.mark.parametrize("publication_id", [None, "", 0, False, {"id": "bad"}])
def test_explicit_malformed_publication_id_is_integrity_failure(tmp_path, publication_id):
    database = tmp_path / "control.db"
    root = tmp_path / "artifacts"
    events = SQLiteEventStore(database)
    artifacts = ArtifactStore(root, event_store=events)
    content = b"bad-publication-id"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    artifacts._path_for_digest(digest).write_bytes(content)
    payload = {
        "publication_id": publication_id,
        "digest": digest,
        "type": "artifact",
        "size": len(content),
        "media_type": "application/octet-stream",
        "source": {},
        "schema_version": 1,
        "redaction_state": "unknown",
        "readable_scope": ["run-1"],
        "references": [],
        "lifecycle_state": "temporary",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    events.append(
        "artifact",
        digest,
        0,
        [EventDraft("ArtifactPublished", payload)],
        "bad-publication-id",
    )
    grant = ArtifactAccessGrant(
        digest=digest,
        scope=("run-1",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="control-plane",
        signature="valid",
    )
    with pytest.raises(ArtifactIntegrityError):
        artifacts.get_record(digest, grant)
    events.close()


class _NoReadAppendFailureStore:
    """An adapter whose append outcome is unknown and offers no stream reads."""

    def current_version(self, stream_type, stream_id):
        return 0

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        raise RuntimeError("append response lost")


class _ReadFailureAfterAppendStore(_NoReadAppendFailureStore):
    def __init__(self, error):
        self.error = error

    def read_stream(self, stream_type, stream_id):
        raise self.error


def test_append_failure_without_read_capability_retains_final_object(tmp_path):
    artifacts = _store(tmp_path / "artifacts", _NoReadAppendFailureStore())
    content = b"unknown-commit"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()

    with pytest.raises(ArtifactMetadataError):
        artifacts.publish_bytes(content, readable_scope=("run-1",))

    assert artifacts._path_for_digest(digest).is_file()


@pytest.mark.parametrize(
    "read_error",
    [
        OSError("metadata unavailable"),
        sqlite3.OperationalError("database unavailable"),
        RuntimeError("metadata unavailable"),
    ],
)
def test_append_failure_with_unreadable_stream_retains_final_object(
    tmp_path, read_error
):
    artifacts = _store(
        tmp_path / "artifacts",
        _ReadFailureAfterAppendStore(read_error),
    )
    content = b"unknown-commit"
    digest = "sha256:" + hashlib.sha256(content).hexdigest()

    with pytest.raises(ArtifactMetadataError):
        artifacts.publish_bytes(content, readable_scope=("run-1",))

    assert artifacts._path_for_digest(digest).is_file()
