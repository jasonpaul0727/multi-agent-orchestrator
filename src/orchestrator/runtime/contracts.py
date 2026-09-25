"""Strict, bounded messages for an untrusted Worker/Verifier process boundary.

These are data contracts only. They deliberately contain no EventStore,
Scheduler, filesystem path, provider credential, or authority capability.
Control-plane services must validate every returned message before accepting
any state transition.
"""

from __future__ import annotations

import json
import re
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$", re.ASCII)
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)
_MAX_IPC_BYTES = 1_048_576
_MAX_ARTIFACTS = 128
_MAX_CHECKS = 256


class RuntimeContractError(ValueError):
    """An IPC message is malformed, oversized, or bound to another attempt."""


class _ContractModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class AttemptContext(_ContractModel):
    """Immutable identifiers/hashes a Worker result must echo exactly."""

    run_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    node_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    attempt_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    agent_instance_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    fencing_generation: StrictInt = Field(gt=0)
    graph_version: StrictInt = Field(ge=0)
    input_manifest_hash: StrictStr = Field(pattern=_DIGEST.pattern)
    effective_config_hash: StrictStr = Field(pattern=_DIGEST.pattern)
    registry_hash: StrictStr = Field(pattern=_DIGEST.pattern)
    policy_manifest_hash: StrictStr = Field(pattern=_DIGEST.pattern)
    routing_decision_hash: StrictStr = Field(pattern=_DIGEST.pattern)
    planning_contract_hash: StrictStr = Field(pattern=_DIGEST.pattern)


class ArtifactRef(_ContractModel):
    """Opaque content identity; never a worker-controlled filesystem path."""

    digest: StrictStr = Field(pattern=_DIGEST.pattern)
    size_bytes: StrictInt = Field(ge=0)
    artifact_type: StrictStr = Field(min_length=1, max_length=64, pattern=_IDENTIFIER.pattern)
    media_type: StrictStr = Field(min_length=1, max_length=128)

    @field_validator("media_type")
    @classmethod
    def safe_media_type(cls, value: str) -> str:
        if value != value.strip() or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
            raise ValueError("media_type must not contain surrounding whitespace or controls")
        return value


class ToolCapabilityRef(_ContractModel):
    """Opaque, one-use capability reference; never a credential or grant body."""

    grant_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    tool_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    scope_hash: StrictStr = Field(pattern=_DIGEST.pattern)


class WorkerTask(_ContractModel):
    """Minimal attempt-scoped input sent from host control plane to Worker."""

    context: AttemptContext
    role: StrictStr = Field(min_length=1, max_length=64, pattern=_IDENTIFIER.pattern)
    task_text: StrictStr = Field(min_length=1, max_length=100_000)
    input_artifacts: tuple[ArtifactRef, ...] = Field(max_length=_MAX_ARTIFACTS)
    tool_capabilities: tuple[ToolCapabilityRef, ...] = Field(max_length=64)
    output_byte_limit: StrictInt = Field(ge=1, le=1_073_741_824)

    @field_validator("task_text")
    @classmethod
    def non_blank_task(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("task_text must not be blank")
        return value

    @field_validator("input_artifacts", "tool_capabilities", mode="before")
    @classmethod
    def normalize_arrays(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("field must be an array")
        return tuple(value)

    @field_validator("tool_capabilities")
    @classmethod
    def unique_capabilities(
        cls, value: tuple[ToolCapabilityRef, ...]
    ) -> tuple[ToolCapabilityRef, ...]:
        grants = [item.grant_id for item in value]
        if len(grants) != len(set(grants)):
            raise ValueError("tool_capabilities must not contain duplicate grant IDs")
        return tuple(sorted(value, key=lambda item: item.grant_id))

    @model_validator(mode="after")
    def unique_input_digests(self) -> "WorkerTask":
        digests = [artifact.digest for artifact in self.input_artifacts]
        if len(digests) != len(set(digests)):
            raise ValueError("input_artifacts must not contain duplicate digests")
        return self


class WorkerResult(_ContractModel):
    """Untrusted candidate/failure proposal; never a durable success event."""

    context: AttemptContext
    result_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    outcome: Literal["candidate", "failed", "blocked"]
    artifacts: tuple[ArtifactRef, ...] = Field(max_length=_MAX_ARTIFACTS)
    failure_code: StrictStr | None = Field(default=None, max_length=64, pattern=_IDENTIFIER.pattern)

    @field_validator("artifacts", mode="before")
    @classmethod
    def normalize_artifacts(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("artifacts must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def outcome_shape(self) -> "WorkerResult":
        if self.outcome == "candidate" and not self.artifacts:
            raise ValueError("candidate result must reference at least one artifact")
        if self.outcome == "failed" and self.failure_code is None:
            raise ValueError("failed result requires a stable failure_code")
        if self.outcome != "failed" and self.failure_code is not None:
            raise ValueError("failure_code is only valid for failed results")
        digests = [artifact.digest for artifact in self.artifacts]
        if len(digests) != len(set(digests)):
            raise ValueError("result artifacts must not contain duplicate digests")
        return self


class VerificationTask(_ContractModel):
    """Read-only, artifact-scoped request to an independent Verifier."""

    context: AttemptContext
    candidate_artifacts: tuple[ArtifactRef, ...] = Field(min_length=1, max_length=_MAX_ARTIFACTS)
    required_check_ids: tuple[StrictStr, ...] = Field(min_length=1, max_length=_MAX_CHECKS)
    acceptance_contract: StrictStr = Field(min_length=1, max_length=32_000)

    @field_validator("candidate_artifacts", "required_check_ids", mode="before")
    @classmethod
    def normalize_arrays(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("field must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def unique_inputs(self) -> "VerificationTask":
        digests = [artifact.digest for artifact in self.candidate_artifacts]
        if len(digests) != len(set(digests)):
            raise ValueError("candidate_artifacts must not contain duplicate digests")
        if (
            len(self.required_check_ids) != len(set(self.required_check_ids))
            or any(not _IDENTIFIER.fullmatch(item) for item in self.required_check_ids)
        ):
            raise ValueError("required_check_ids must contain unique stable identifiers")
        if not self.acceptance_contract.strip():
            raise ValueError("acceptance_contract must not be blank")
        return self


class VerificationCheck(_ContractModel):
    check_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    passed: StrictBool
    evidence_digests: tuple[StrictStr, ...] = Field(min_length=1, max_length=_MAX_ARTIFACTS)

    @field_validator("evidence_digests", mode="before")
    @classmethod
    def normalize_digests(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("evidence_digests must be an array")
        return tuple(value)

    @field_validator("evidence_digests")
    @classmethod
    def valid_unique_digests(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not _DIGEST.fullmatch(item) for item in value) or len(set(value)) != len(value):
            raise ValueError("evidence_digests must contain unique SHA-256 digests")
        return tuple(sorted(value))


class VerificationEvidence(_ContractModel):
    """Verifier proposal; acceptance still belongs to the trusted control plane."""

    context: AttemptContext
    verifier_id: StrictStr = Field(min_length=1, max_length=128, pattern=_IDENTIFIER.pattern)
    outcome: Literal["accepted", "rejected", "inconclusive"]
    checks: tuple[VerificationCheck, ...] = Field(max_length=_MAX_CHECKS)
    inspected_digests: tuple[StrictStr, ...] = Field(max_length=_MAX_ARTIFACTS)

    @field_validator("checks", "inspected_digests", mode="before")
    @classmethod
    def normalize_arrays(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("field must be an array")
        return tuple(value)

    @field_validator("inspected_digests")
    @classmethod
    def valid_inspected_digests(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not _DIGEST.fullmatch(item) for item in value) or len(set(value)) != len(value):
            raise ValueError("inspected_digests must contain unique SHA-256 digests")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def unique_checks(self) -> "VerificationEvidence":
        ids = [check.check_id for check in self.checks]
        if len(ids) != len(set(ids)):
            raise ValueError("checks must not contain duplicate check IDs")
        return self


def validate_worker_result(task: WorkerTask, result: WorkerResult) -> None:
    """Reject stale/cross-attempt output before artifact verification or storage."""

    if result.context != task.context:
        raise RuntimeContractError("Worker result is not bound to the dispatched attempt")
    allowed_inputs = {item.digest for item in task.input_artifacts}
    if any(item.digest in allowed_inputs for item in result.artifacts):
        raise RuntimeContractError("Worker cannot relabel an input artifact as a new output")
    if sum(item.size_bytes for item in result.artifacts) > task.output_byte_limit:
        raise RuntimeContractError("Worker result exceeds the output byte limit")


def validate_verification_evidence(
    task: VerificationTask, evidence: VerificationEvidence
) -> None:
    """Bind evidence to the exact candidate set and complete required check set."""

    if evidence.context != task.context:
        raise RuntimeContractError("Verifier evidence is not bound to the dispatched attempt")
    candidates = {item.digest for item in task.candidate_artifacts}
    inspected = set(evidence.inspected_digests)
    if inspected - candidates:
        raise RuntimeContractError("Verifier references an artifact outside the candidate set")
    if evidence.outcome != "inconclusive" and inspected != candidates:
        raise RuntimeContractError("Verifier did not inspect the exact candidate artifact set")
    by_id = {check.check_id: check for check in evidence.checks}
    if set(by_id) != set(task.required_check_ids):
        raise RuntimeContractError("Verifier evidence is missing or adds acceptance checks")
    if any(set(check.evidence_digests) - candidates for check in evidence.checks):
        raise RuntimeContractError("Verifier check cites an artifact outside the candidate set")
    all_passed = all(check.passed for check in evidence.checks)
    if evidence.outcome == "accepted" and not all_passed:
        raise RuntimeContractError("Verifier cannot accept a candidate with a failed check")
    if evidence.outcome == "rejected" and all_passed:
        raise RuntimeContractError("Verifier cannot reject a candidate when all checks passed")


def decode_worker_result(payload: bytes) -> WorkerResult:
    return _decode_message(payload, WorkerResult)


def decode_verification_evidence(payload: bytes) -> VerificationEvidence:
    return _decode_message(payload, VerificationEvidence)


def _decode_message(payload: bytes, model: type[BaseModel]):
    if not isinstance(payload, bytes) or len(payload) > _MAX_IPC_BYTES:
        raise RuntimeContractError("IPC message exceeds its byte limit")
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object_pairs,
            parse_constant=_reject_json_constant,
        )
        return model.model_validate(value)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, RecursionError) as exc:
        raise RuntimeContractError("IPC message is malformed") from exc


def _unique_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("non-finite JSON values are forbidden")


__all__ = [
    "AttemptContext",
    "ArtifactRef",
    "RuntimeContractError",
    "ToolCapabilityRef",
    "VerificationCheck",
    "VerificationEvidence",
    "VerificationTask",
    "WorkerResult",
    "WorkerTask",
    "decode_verification_evidence",
    "decode_worker_result",
    "validate_verification_evidence",
    "validate_worker_result",
]
