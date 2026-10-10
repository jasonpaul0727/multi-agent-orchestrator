"""Bounded, append-only persistence for non-authoritative Verifier proposals."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from orchestrator.persistence import EventDraft, SQLiteEventStore, StoredEvent
from orchestrator.runtime.contracts import (
    AttemptContext,
    BUILTIN_VERIFIER_ID,
    RuntimeContractError,
    VerificationEvidence,
    VerificationTask,
    validate_verification_evidence,
    validate_verification_task,
)


VERIFICATION_PROPOSAL_STREAM_TYPE = "verification_proposals"
VERIFICATION_PROPOSAL_EVENT_TYPE = "VerifierProposalRecorded"
VERIFICATION_PROPOSAL_SCHEMA_VERSION = 1
MAX_RECORD_BYTES = 1_048_576
MAX_PROPOSALS_PER_RUN = 1_024
MAX_RUN_PAYLOAD_BYTES = 16 * 1_048_576

_PAYLOAD_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "node_id",
        "attempt_id",
        "fencing_generation",
        "graph_version",
        "artifact_digests",
        "task",
        "task_sha256",
        "evidence",
        "evidence_sha256",
    }
)


class VerifierProposalJournalError(RuntimeError):
    """A proposal is invalid, conflicting, corrupt, over limit, or unavailable."""


class VerifierProposalRecord(BaseModel):
    """Immutable validated task/evidence pair recovered from the journal."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    task: VerificationTask
    evidence: VerificationEvidence
    task_sha256: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evidence_sha256: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    run_id: StrictStr
    node_id: StrictStr
    attempt_id: StrictStr
    fencing_generation: StrictInt
    graph_version: StrictInt
    artifact_digests: tuple[StrictStr, ...]


class VerifierProposalJournal:
    """Persist attempt-scoped verification proposals without accepting a Node.

    The journal trusts its host caller to have obtained evidence from the
    isolated verifier. It performs no lifecycle or Scheduler writes and does
    not read or modify ArtifactStore bytes.
    """

    def __init__(self, event_store: SQLiteEventStore) -> None:
        if not isinstance(event_store, SQLiteEventStore):
            raise TypeError("event_store must be a SQLiteEventStore")
        self._event_store = event_store

    def record(
        self,
        task: VerificationTask,
        evidence: VerificationEvidence,
    ) -> VerifierProposalRecord:
        """Validate and durably record one proposal, idempotently."""

        task_model, evidence_model = _revalidate_proposal(task, evidence)
        payload = _build_payload(task_model, evidence_model)
        encoded_payload = _canonical_bytes(payload)
        if len(encoded_payload) > MAX_RECORD_BYTES:
            raise VerifierProposalJournalError("proposal exceeds the per-record byte limit")

        context = task_model.context
        idempotency_key = _idempotency_key(context, payload["task_sha256"])
        draft = EventDraft(
            event_type=VERIFICATION_PROPOSAL_EVENT_TYPE,
            payload=payload,
            run_id=context.run_id,
            node_id=context.node_id,
            attempt_id=context.attempt_id,
            fencing_generation=context.fencing_generation,
            correlation_id=context.run_id,
            causation_id=payload["task_sha256"],
        )

        def decide(events: list[StoredEvent], version: int) -> list[EventDraft]:
            if version != len(events):
                raise VerifierProposalJournalError("proposal stream version is corrupt")
            existing_records, existing_sizes = _decode_stream(context.run_id, events)
            if len(existing_records) > MAX_PROPOSALS_PER_RUN:
                raise VerifierProposalJournalError("Run proposal count limit is exceeded")
            if sum(existing_sizes) > MAX_RUN_PAYLOAD_BYTES:
                raise VerifierProposalJournalError("Run proposal byte limit is exceeded")

            matching = [
                event for event in events if event.idempotency_key == idempotency_key
            ]
            if matching:
                if len(matching) != 1 or matching[0].payload != payload:
                    raise VerifierProposalJournalError("proposal identity conflicts with durable evidence")
                # Returning the exact draft lets EventStore verify that its
                # idempotency index still names the same append. It is a no-op
                # even when either configured Run cap has been reached.
                return [draft]

            if len(existing_records) >= MAX_PROPOSALS_PER_RUN:
                raise VerifierProposalJournalError("Run proposal count limit is reached")
            if sum(existing_sizes) + len(encoded_payload) > MAX_RUN_PAYLOAD_BYTES:
                raise VerifierProposalJournalError("Run proposal byte limit is reached")
            return [draft]

        try:
            appended = self._event_store.append_checked(
                VERIFICATION_PROPOSAL_STREAM_TYPE,
                context.run_id,
                idempotency_key,
                decide,
            )
            if len(appended) != 1:
                raise VerifierProposalJournalError("proposal append returned an invalid result")
            record, _encoded_size = _decode_event(context.run_id, appended[0])
            return record
        except VerifierProposalJournalError:
            raise
        except Exception as exc:
            raise VerifierProposalJournalError("proposal journal operation failed") from exc

    def read_run(self, run_id: str) -> tuple[VerifierProposalRecord, ...]:
        """Replay and validate every proposal in one Run's dedicated stream."""

        if not isinstance(run_id, str) or not run_id or run_id != run_id.strip():
            raise VerifierProposalJournalError("Run identity is invalid")
        try:
            events, version = self._event_store.read_stream_with_version(
                VERIFICATION_PROPOSAL_STREAM_TYPE,
                run_id,
            )
            if version != len(events) or any(
                event.stream_version != index
                for index, event in enumerate(events, start=1)
            ):
                raise VerifierProposalJournalError("proposal stream version is corrupt")
            records, sizes = _decode_stream(run_id, events)
            if len(records) > MAX_PROPOSALS_PER_RUN:
                raise VerifierProposalJournalError("Run proposal count limit is exceeded")
            if sum(sizes) > MAX_RUN_PAYLOAD_BYTES:
                raise VerifierProposalJournalError("Run proposal byte limit is exceeded")
            return tuple(records)
        except VerifierProposalJournalError:
            raise
        except Exception as exc:
            raise VerifierProposalJournalError("proposal journal replay failed") from exc

    def read_proposal(self, run_id: str, task_sha256: str) -> VerifierProposalRecord | None:
        """Resolve a proposal only after validating the complete Run stream."""
        records = self.read_run(run_id)
        matches = [record for record in records if record.task_sha256 == task_sha256]
        if len(matches) > 1:
            raise VerifierProposalJournalError("proposal task identity is duplicated")
        return matches[0] if matches else None


def _revalidate_proposal(
    task: VerificationTask,
    evidence: VerificationEvidence,
) -> tuple[VerificationTask, VerificationEvidence]:
    try:
        if not isinstance(task, VerificationTask) or not isinstance(evidence, VerificationEvidence):
            raise ValueError("wrong contract type")
        # Reparse even frozen instances: model_construct/model_copy can bypass
        # validators, so immutability alone is not a trust boundary.
        checked_task = VerificationTask.model_validate(task.model_dump(mode="json"))
        checked_evidence = VerificationEvidence.model_validate(evidence.model_dump(mode="json"))
        validate_verification_task(checked_task)
        if checked_evidence.verifier_id != BUILTIN_VERIFIER_ID:
            raise RuntimeContractError("unsupported built-in verifier")
        validate_verification_evidence(checked_task, checked_evidence)
        return checked_task, checked_evidence
    except Exception as exc:
        raise VerifierProposalJournalError("verification proposal failed validation") from exc


def _build_payload(task: VerificationTask, evidence: VerificationEvidence) -> dict[str, Any]:
    context = task.context
    task_json = task.model_dump(mode="json")
    evidence_json = evidence.model_dump(mode="json")
    task_hash = _content_hash(task_json)
    evidence_hash = _content_hash(evidence_json)
    return {
        "schema_version": VERIFICATION_PROPOSAL_SCHEMA_VERSION,
        "run_id": context.run_id,
        "node_id": context.node_id,
        "attempt_id": context.attempt_id,
        "fencing_generation": context.fencing_generation,
        "graph_version": context.graph_version,
        "artifact_digests": sorted(item.digest for item in task.candidate_artifacts),
        "task": task_json,
        "task_sha256": task_hash,
        "evidence": evidence_json,
        "evidence_sha256": evidence_hash,
    }


def _decode_stream(
    run_id: str,
    events: list[StoredEvent],
) -> tuple[list[VerifierProposalRecord], list[int]]:
    records: list[VerifierProposalRecord] = []
    sizes: list[int] = []
    for index, event in enumerate(events, start=1):
        if event.stream_version != index:
            raise VerifierProposalJournalError("proposal stream version is corrupt")
        record, size = _decode_event(run_id, event)
        records.append(record)
        sizes.append(size)
    return records, sizes


def _decode_event(
    run_id: str,
    event: StoredEvent,
) -> tuple[VerifierProposalRecord, int]:
    try:
        if (
            event.stream_type != VERIFICATION_PROPOSAL_STREAM_TYPE
            or event.stream_id != run_id
            or event.event_type != VERIFICATION_PROPOSAL_EVENT_TYPE
            or event.schema_version != VERIFICATION_PROPOSAL_SCHEMA_VERSION
            or not isinstance(event.payload, dict)
            or set(event.payload) != _PAYLOAD_FIELDS
        ):
            raise ValueError("event envelope mismatch")
        payload = event.payload
        if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
            raise ValueError("payload schema mismatch")
        task_raw = payload["task"]
        evidence_raw = payload["evidence"]
        if not isinstance(task_raw, dict) or not isinstance(evidence_raw, dict):
            raise ValueError("contract payload is malformed")
        task = VerificationTask.model_validate(task_raw)
        evidence = VerificationEvidence.model_validate(evidence_raw)
        # Durable contracts must already be in the normalized JSON form that
        # was hashed and written; do not silently normalize tampered data.
        if task_raw != task.model_dump(mode="json") or evidence_raw != evidence.model_dump(mode="json"):
            raise ValueError("contract payload is not canonical model JSON")
        validate_verification_task(task)
        if evidence.verifier_id != BUILTIN_VERIFIER_ID:
            raise ValueError("unsupported verifier")
        validate_verification_evidence(task, evidence)
        expected_payload = _build_payload(task, evidence)
        if _canonical_bytes(payload) != _canonical_bytes(expected_payload):
            raise ValueError("proposal bindings or content hashes do not match")

        context: AttemptContext = task.context
        expected_key = _idempotency_key(context, expected_payload["task_sha256"])
        if (
            context.run_id != run_id
            or payload["run_id"] != run_id
            or event.run_id != context.run_id
            or event.node_id != context.node_id
            or event.attempt_id != context.attempt_id
            or event.fencing_generation != context.fencing_generation
            or event.correlation_id != context.run_id
            or event.causation_id != expected_payload["task_sha256"]
            or event.idempotency_key != expected_key
        ):
            raise ValueError("event identity metadata does not match proposal")
        encoded = _canonical_bytes(payload)
        if len(encoded) > MAX_RECORD_BYTES:
            raise ValueError("proposal exceeds the per-record byte limit")
        record = VerifierProposalRecord(
            task=task,
            evidence=evidence,
            task_sha256=expected_payload["task_sha256"],
            evidence_sha256=expected_payload["evidence_sha256"],
            run_id=context.run_id,
            node_id=context.node_id,
            attempt_id=context.attempt_id,
            fencing_generation=context.fencing_generation,
            graph_version=context.graph_version,
            artifact_digests=tuple(expected_payload["artifact_digests"]),
        )
        return record, len(encoded)
    except VerifierProposalJournalError:
        raise
    except Exception as exc:
        # Do not put untrusted persisted content or task/evidence values in
        # the error text returned to a caller.
        raise VerifierProposalJournalError("durable proposal failed integrity validation") from exc


def _content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise VerifierProposalJournalError("proposal is not canonical JSON") from exc


def _idempotency_key(context: AttemptContext, task_sha256: str) -> str:
    identity = {
        "run_id": context.run_id,
        "node_id": context.node_id,
        "attempt_id": context.attempt_id,
        "fencing_generation": context.fencing_generation,
        "task_sha256": task_sha256,
    }
    identity_hash = hashlib.sha256(_canonical_bytes(identity)).hexdigest()
    return "verifier-proposal:" + identity_hash


__all__ = [
    "MAX_PROPOSALS_PER_RUN",
    "MAX_RECORD_BYTES",
    "MAX_RUN_PAYLOAD_BYTES",
    "VERIFICATION_PROPOSAL_EVENT_TYPE",
    "VERIFICATION_PROPOSAL_SCHEMA_VERSION",
    "VERIFICATION_PROPOSAL_STREAM_TYPE",
    "VerifierProposalJournal",
    "VerifierProposalJournalError",
    "VerifierProposalRecord",
]
