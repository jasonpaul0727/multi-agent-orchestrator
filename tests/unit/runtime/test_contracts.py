from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from orchestrator.runtime import (
    AttemptContext,
    ArtifactRef,
    RuntimeContractError,
    ToolCapabilityRef,
    VerificationEvidence,
    VerificationTask,
    WorkerResult,
    WorkerTask,
    decode_verification_evidence,
    decode_worker_result,
    validate_verification_evidence,
    validate_worker_result,
)


_HASH = "sha256:" + "a" * 64
_OTHER_HASH = "sha256:" + "b" * 64


def _context(**overrides) -> AttemptContext:
    values = {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "agent_instance_id": "agent-1",
        "fencing_generation": 2,
        "graph_version": 3,
        "input_manifest_hash": _HASH,
        "effective_config_hash": _HASH,
        "registry_hash": _HASH,
        "policy_manifest_hash": _HASH,
        "routing_decision_hash": _HASH,
        "planning_contract_hash": _HASH,
    }
    values.update(overrides)
    return AttemptContext(**values)


def _artifact(digest: str = _OTHER_HASH, *, size: int = 3) -> ArtifactRef:
    return ArtifactRef(
        digest=digest,
        size_bytes=size,
        artifact_type="source",
        media_type="text/plain",
    )


def _task(**overrides) -> WorkerTask:
    values = {
        "context": _context(),
        "role": "coder",
        "task_text": "Implement the requested change.",
        "input_artifacts": (_artifact(),),
        "tool_capabilities": (
            ToolCapabilityRef(
                grant_id="grant-1",
                tool_id="system.readonly-command",
                scope_hash=_HASH,
            ),
        ),
        "output_byte_limit": 10,
    }
    values.update(overrides)
    return WorkerTask(**values)


def test_worker_task_contains_only_bounded_scoped_inputs() -> None:
    task = _task(tool_capabilities=(
        ToolCapabilityRef(grant_id="grant-2", tool_id="system.readonly-command", scope_hash=_HASH),
        ToolCapabilityRef(grant_id="grant-1", tool_id="system.readonly-command", scope_hash=_HASH),
    ))

    assert [item.grant_id for item in task.tool_capabilities] == ["grant-1", "grant-2"]
    assert "filesystem_path" not in task.model_dump()
    assert "event_store" not in task.model_dump()
    assert "credential" not in task.model_dump()


@pytest.mark.parametrize(
    "overrides",
    [
        {"fencing_generation": True},
        {"fencing_generation": 0},
        {"graph_version": -1},
        {"attempt_id": "bad id"},
        {"registry_hash": "not-a-hash"},
    ],
)
def test_attempt_context_rejects_invalid_or_unfenced_identity(overrides) -> None:
    with pytest.raises(ValidationError):
        _context(**overrides)


def test_worker_task_rejects_unbounded_or_ambiguous_scope() -> None:
    capability = ToolCapabilityRef(grant_id="grant-1", tool_id="tool-1", scope_hash=_HASH)
    with pytest.raises(ValidationError, match="duplicate grant"):
        _task(tool_capabilities=(capability, capability))
    with pytest.raises(ValidationError, match="duplicate digests"):
        _task(input_artifacts=(_artifact(), _artifact()))
    with pytest.raises(ValidationError, match="blank"):
        _task(task_text="  ")
    with pytest.raises(ValidationError):
        _task(output_byte_limit=1_073_741_825)
    with pytest.raises(ValidationError):
        _task(unexpected_control_handle="/private/events.db")
    with pytest.raises(ValidationError, match="array"):
        _task(tool_capabilities="grant-1")


def test_worker_result_is_only_a_candidate_not_a_success_transition() -> None:
    with pytest.raises(ValidationError, match="at least one artifact"):
        WorkerResult(context=_context(), result_id="result-1", outcome="candidate", artifacts=())
    with pytest.raises(ValidationError, match="failure_code"):
        WorkerResult(context=_context(), result_id="result-1", outcome="failed", artifacts=())
    with pytest.raises(ValidationError, match="only valid"):
        WorkerResult(
            context=_context(),
            result_id="result-1",
            outcome="blocked",
            artifacts=(),
            failure_code="denied",
        )
    with pytest.raises(ValidationError, match="duplicate digests"):
        WorkerResult(
            context=_context(),
            result_id="result-1",
            outcome="candidate",
            artifacts=(_artifact(), _artifact()),
        )
    with pytest.raises(ValidationError, match="array"):
        WorkerResult(
            context=_context(),
            result_id="result-1",
            outcome="blocked",
            artifacts="not-array",
        )


def test_worker_result_binding_rejects_stale_input_alias_and_size_overflow() -> None:
    task = _task(input_artifacts=(_artifact(_HASH),), output_byte_limit=5)
    stale = WorkerResult(
        context=_context(attempt_id="attempt-2"),
        result_id="result-1",
        outcome="candidate",
        artifacts=(_artifact(),),
    )
    with pytest.raises(RuntimeContractError, match="not bound"):
        validate_worker_result(task, stale)

    aliased_input = WorkerResult(
        context=task.context,
        result_id="result-2",
        outcome="candidate",
        artifacts=(_artifact(_HASH),),
    )
    with pytest.raises(RuntimeContractError, match="input artifact"):
        validate_worker_result(task, aliased_input)

    oversized = WorkerResult(
        context=task.context,
        result_id="result-3",
        outcome="candidate",
        artifacts=(_artifact(_OTHER_HASH, size=3), _artifact("sha256:" + "c" * 64, size=3)),
    )
    with pytest.raises(RuntimeContractError, match="byte limit"):
        validate_worker_result(task, oversized)


def _verification_task() -> VerificationTask:
    return VerificationTask(
        context=_context(),
        candidate_artifacts=(_artifact(),),
        required_check_ids=("tests-pass", "contract-pass"),
        acceptance_contract="Both required checks must pass against the candidate artifact.",
    )


def _evidence(**overrides) -> VerificationEvidence:
    values = {
        "context": _context(),
        "verifier_id": "verifier-1",
        "outcome": "accepted",
        "checks": (
            {"check_id": "tests-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
            {"check_id": "contract-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        ),
        "inspected_digests": (_OTHER_HASH,),
    }
    values.update(overrides)
    return VerificationEvidence(**values)


def test_verifier_must_cover_exact_attempt_artifact_and_check_set() -> None:
    task = _verification_task()
    validate_verification_evidence(task, _evidence())

    with pytest.raises(RuntimeContractError, match="not bound"):
        validate_verification_evidence(task, _evidence(context=_context(attempt_id="attempt-2")))
    with pytest.raises(RuntimeContractError, match="exact candidate"):
        validate_verification_evidence(task, _evidence(inspected_digests=()))
    with pytest.raises(RuntimeContractError, match="outside the candidate"):
        validate_verification_evidence(task, _evidence(inspected_digests=(_HASH,)))
    partial = _evidence(outcome="inconclusive", inspected_digests=())
    validate_verification_evidence(task, partial)
    with pytest.raises(RuntimeContractError, match="missing or adds"):
        validate_verification_evidence(task, _evidence(checks=()))
    with pytest.raises(RuntimeContractError, match="outside the candidate"):
        validate_verification_evidence(task, _evidence(checks=(
            {"check_id": "tests-pass", "passed": True, "evidence_digests": (_HASH,)},
            {"check_id": "contract-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        )))


@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"outcome": "accepted", "checks": (
            {"check_id": "tests-pass", "passed": False, "evidence_digests": (_OTHER_HASH,)},
            {"check_id": "contract-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        )}, "cannot accept"),
        ({"outcome": "rejected"}, "cannot reject"),
    ],
)
def test_verifier_outcome_cannot_contradict_check_results(overrides, match) -> None:
    with pytest.raises(RuntimeContractError, match=match):
        validate_verification_evidence(_verification_task(), _evidence(**overrides))


def test_contracts_reject_duplicate_or_unbounded_verifier_inputs() -> None:
    with pytest.raises(ValidationError, match="array"):
        VerificationTask(
            context=_context(), candidate_artifacts="not-array",
            required_check_ids=("checks-pass",), acceptance_contract="checks",
        )
    with pytest.raises(ValidationError, match="duplicate digests"):
        VerificationTask(
            context=_context(), candidate_artifacts=(_artifact(), _artifact()),
            required_check_ids=("checks-pass",), acceptance_contract="checks",
        )
    with pytest.raises(ValidationError, match="unique stable identifiers"):
        VerificationTask(
            context=_context(), candidate_artifacts=(_artifact(),),
            required_check_ids=("tests-pass", "tests-pass"), acceptance_contract="checks",
        )
    with pytest.raises(ValidationError, match="blank"):
        VerificationTask(
            context=_context(), candidate_artifacts=(_artifact(),),
            required_check_ids=("tests-pass",), acceptance_contract="  ",
        )
    with pytest.raises(ValidationError, match="duplicate check IDs"):
        _evidence(checks=(
            {"check_id": "tests-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
            {"check_id": "tests-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        ))
    with pytest.raises(ValidationError, match="SHA-256"):
        _evidence(inspected_digests=("../candidate",))
    with pytest.raises(ValidationError, match="SHA-256"):
        _evidence(checks=(
            {"check_id": "tests-pass", "passed": True, "evidence_digests": ("../candidate",)},
            {"check_id": "contract-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        ))
    with pytest.raises(ValidationError, match="at least 1 item"):
        _evidence(checks=(
            {"check_id": "tests-pass", "passed": True, "evidence_digests": ()},
            {"check_id": "contract-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        ))
    with pytest.raises(ValidationError, match="controls"):
        ArtifactRef(
            digest=_HASH,
            size_bytes=1,
            artifact_type="source",
            media_type="text/plain\nsecret",
        )
    with pytest.raises(ValidationError, match="array"):
        _evidence(inspected_digests="not-array")
    with pytest.raises(ValidationError, match="array"):
        _evidence(checks=(
            {"check_id": "tests-pass", "passed": True, "evidence_digests": "not-array"},
            {"check_id": "contract-pass", "passed": True, "evidence_digests": (_OTHER_HASH,)},
        ))


def test_worker_result_decoder_accepts_valid_json_and_rejects_ambiguous_or_unsafe_json() -> None:
    result = WorkerResult(
        context=_context(), result_id="result-1", outcome="candidate", artifacts=(_artifact(),)
    )
    payload = result.model_dump_json().encode("utf-8")
    assert decode_worker_result(payload) == result

    for invalid in (
        b"{\"a\":1,\"a\":2}",
        b"{\"value\":NaN}",
        b"\xff",
        b"not-json",
        json.dumps({"unexpected": True}).encode("utf-8"),
    ):
        with pytest.raises(RuntimeContractError, match="malformed"):
            decode_worker_result(invalid)
    with pytest.raises(RuntimeContractError, match="byte limit"):
        decode_worker_result(b" " * 1_048_577)
    with pytest.raises(RuntimeContractError, match="byte limit"):
        decode_worker_result(None)  # type: ignore[arg-type]


def test_verification_evidence_decoder_round_trips() -> None:
    evidence = _evidence(outcome="inconclusive")
    raw = evidence.model_dump_json().encode("utf-8")
    assert decode_verification_evidence(raw) == evidence
