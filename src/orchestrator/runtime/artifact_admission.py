"""Trusted admission of untrusted Worker candidate artifact references.

This gate does not accept a node outcome. It only proves that every candidate
reference names bytes already published by the host ArtifactStore for the
exact dispatched attempt. An independent Verifier and the Scheduler must make
any subsequent success decision.
"""

from __future__ import annotations

from typing import Protocol

from orchestrator.artifacts.store import ArtifactRecord

from .contracts import RuntimeContractError, WorkerResult, WorkerTask, validate_worker_result


class _RunArtifactInventory(Protocol):
    def verify_run_artifacts(self, run_id: str) -> list[ArtifactRecord]: ...


class CandidateArtifactError(RuntimeContractError):
    """A Worker candidate cannot be bound to verified host-published bytes."""


def admit_candidate_artifacts(
    task: WorkerTask,
    result: WorkerResult,
    artifact_store: _RunArtifactInventory,
) -> tuple[ArtifactRecord, ...]:
    """Verify exact candidate bytes, metadata, and attempt provenance.

    The inventory operation is a trusted host-side operation: it re-hashes
    each Run artifact, checks its declared size, and does not expose bytes to
    the Worker. It must never be replaced by the Worker-supplied references.
    """

    validate_worker_result(task, result)
    if result.outcome != "candidate":
        raise CandidateArtifactError("only a candidate Worker result has artifacts to admit")

    context = task.context
    expected_source = {
        "run_id": context.run_id,
        "node_id": context.node_id,
        "attempt_id": context.attempt_id,
        "fencing_generation": str(context.fencing_generation),
        "agent_instance_id": context.agent_instance_id,
    }
    try:
        inventory = artifact_store.verify_run_artifacts(context.run_id)
    except Exception as exc:
        raise CandidateArtifactError("host artifact inventory could not be verified") from exc

    admitted: list[ArtifactRecord] = []
    for reference in result.artifacts:
        matches = [
            record
            for record in inventory
            if record.digest == reference.digest
            and all(record.source.get(key) == value for key, value in expected_source.items())
        ]
        if not matches:
            raise CandidateArtifactError("candidate artifact has no exact attempt publication")
        if any(
            record.size != reference.size_bytes
            or record.artifact_type != reference.artifact_type
            or record.media_type != reference.media_type
            for record in matches
        ):
            raise CandidateArtifactError("candidate artifact metadata conflicts with its publication")
        # Multiple identical publications of the same digest for this attempt
        # do not create multiple candidate artifacts. The latest publication
        # is deterministic after ArtifactStore's stable inventory ordering.
        admitted.append(matches[-1])
    return tuple(admitted)


__all__ = ["CandidateArtifactError", "admit_candidate_artifacts"]
