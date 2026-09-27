from __future__ import annotations

import hashlib

import pytest

from orchestrator.artifacts import ArtifactStore
from orchestrator.runtime.artifact_admission import CandidateArtifactError, admit_candidate_artifacts
from orchestrator.runtime.contracts import ArtifactRef, AttemptContext, WorkerResult, WorkerTask


_HASH = "sha256:" + "a" * 64


def _context(**changes: object) -> AttemptContext:
    fields = {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "agent_instance_id": "agent-1",
        "fencing_generation": 3,
        "graph_version": 1,
        "input_manifest_hash": _HASH,
        "effective_config_hash": _HASH,
        "registry_hash": _HASH,
        "policy_manifest_hash": _HASH,
        "routing_decision_hash": _HASH,
        "planning_contract_hash": _HASH,
    }
    fields.update(changes)
    return AttemptContext.model_validate(fields)


def _task(**context_changes: object) -> WorkerTask:
    return WorkerTask(
        context=_context(**context_changes),
        role="coder",
        task_text="Prepare candidate output",
        input_artifacts=(),
        tool_capabilities=(),
        output_byte_limit=1024,
    )


def _publication(store: ArtifactStore, task: WorkerTask, *, source_changes: dict[str, str] | None = None):
    context = task.context
    source = {
        "run_id": context.run_id,
        "node_id": context.node_id,
        "attempt_id": context.attempt_id,
        "fencing_generation": str(context.fencing_generation),
        "agent_instance_id": context.agent_instance_id,
    }
    source.update(source_changes or {})
    return store.publish_bytes(
        b"verified candidate",
        source=source,
        artifact_type="code_patch",
        media_type="text/plain",
    )


def _candidate(task: WorkerTask, record, **reference_changes: object) -> WorkerResult:
    reference = {
        "digest": record.digest,
        "size_bytes": record.size,
        "artifact_type": record.artifact_type,
        "media_type": record.media_type,
    }
    reference.update(reference_changes)
    return WorkerResult(
        context=task.context,
        result_id="result-1",
        outcome="candidate",
        artifacts=(ArtifactRef.model_validate(reference),),
    )


def test_candidate_requires_verified_host_publication_for_exact_attempt(tmp_path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    task = _task()
    record = _publication(store, task)

    admitted = admit_candidate_artifacts(task, _candidate(task, record), store)

    assert len(admitted) == 1
    assert admitted[0].publication_id == record.publication_id
    assert admitted[0].digest == "sha256:" + hashlib.sha256(b"verified candidate").hexdigest()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "run-2"),
        ("node_id", "node-2"),
        ("attempt_id", "attempt-2"),
        ("fencing_generation", "2"),
        ("agent_instance_id", "agent-2"),
    ],
)
def test_candidate_rejects_cross_scope_publication(tmp_path, field: str, value: str) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    task = _task()
    record = _publication(store, task, source_changes={field: value})

    with pytest.raises(CandidateArtifactError, match="exact attempt publication"):
        admit_candidate_artifacts(task, _candidate(task, record), store)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("size_bytes", 1),
        ("artifact_type", "document"),
        ("media_type", "application/json"),
    ],
)
def test_candidate_rejects_metadata_mismatch(tmp_path, field: str, value: object) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    task = _task()
    record = _publication(store, task)

    with pytest.raises(CandidateArtifactError, match="metadata conflicts"):
        admit_candidate_artifacts(task, _candidate(task, record, **{field: value}), store)


def test_candidate_rejects_unpublished_and_tampered_bytes(tmp_path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    task = _task()
    record = _publication(store, task)
    valid_candidate = _candidate(task, record)
    unpublished = _candidate(task, record, digest="sha256:" + "b" * 64)

    with pytest.raises(CandidateArtifactError, match="exact attempt publication"):
        admit_candidate_artifacts(task, unpublished, store)

    (tmp_path / "artifacts" / record.digest.removeprefix("sha256:")).write_bytes(b"tampered")
    with pytest.raises(CandidateArtifactError, match="inventory could not be verified"):
        admit_candidate_artifacts(task, valid_candidate, store)


def test_candidate_rejects_stale_context_and_failed_result(tmp_path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    task = _task()
    record = _publication(store, task)
    stale = _candidate(_task(fencing_generation=4), record)

    with pytest.raises(ValueError, match="dispatched attempt"):
        admit_candidate_artifacts(task, stale, store)
    with pytest.raises(CandidateArtifactError, match="only a candidate"):
        admit_candidate_artifacts(
            task,
            WorkerResult(
                context=task.context,
                result_id="result-failed",
                outcome="failed",
                artifacts=(),
                failure_code="worker_error",
            ),
            store,
        )


def test_candidate_requires_every_artifact_to_be_published(tmp_path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    task = _task()
    record = _publication(store, task)
    result = WorkerResult(
        context=task.context,
        result_id="result-2",
        outcome="candidate",
        artifacts=(
            _candidate(task, record).artifacts[0],
            ArtifactRef(
                digest="sha256:" + "b" * 64,
                size_bytes=1,
                artifact_type="code_patch",
                media_type="text/plain",
            ),
        ),
    )

    with pytest.raises(CandidateArtifactError, match="exact attempt publication"):
        admit_candidate_artifacts(task, result, store)
