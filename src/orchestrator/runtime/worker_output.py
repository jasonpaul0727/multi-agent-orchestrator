"""Pure Worker candidate bytes, without execution or publication authority."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import dataclass, field

from orchestrator.validation import revalidate_model

from .contracts import (
    ArtifactRef, AttemptContext, ToolCapabilityRef, WorkerResult, WorkerTask,
    validate_worker_result,
)

_FRAME_CAP = 1_048_576
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}", re.ASCII)


class _TooLarge(ValueError):
    """Private signal from host-owned cap checks."""


class WorkerOutputError(ValueError):
    """A stable failure containing only a trusted, non-sensitive literal."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or code not in ("worker_output_invalid", "worker_output_too_large"):
            code = "worker_output_invalid"
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class DecodedCandidateBlob:
    artifact: ArtifactRef
    content: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class DecodedWorkerOutput:
    result: WorkerResult
    blobs: tuple[DecodedCandidateBlob, ...]


def _revalidate_task(task: WorkerTask) -> WorkerTask:
    if not isinstance(task, WorkerTask):
        raise TypeError("expected WorkerTask")
    _preflight_model(task, WorkerTask)
    return revalidate_model(WorkerTask, task)


def _preflight_model(value: object, schema: type) -> None:
    # The allowlist always comes from the expected contract, never a subclass.
    if isinstance(value, schema):
        fields = value.__dict__
        extra = value.__pydantic_extra__
        if extra is not None and (type(extra) is not dict or extra):
            raise ValueError("unexpected model extras")
    elif type(value) is dict:
        fields = value
    else:
        raise TypeError("unexpected nested model")
    if not set(fields) <= set(schema.model_fields):
        raise ValueError("unexpected model fields")
    for name in schema.model_fields:
        child = fields[name]
        if schema is WorkerTask and name == "context":
            _preflight_model(child, AttemptContext)
        elif schema is WorkerTask and name in ("input_artifacts", "tool_capabilities"):
            if not isinstance(child, (list, tuple)):
                raise TypeError("expected array")
            item_schema = ArtifactRef if name == "input_artifacts" else ToolCapabilityRef
            for item in child:
                _preflight_model(item, item_schema)
        else:
            _check_tree(child)


def _decode_base64(encoded: str) -> bytes:
    content = base64.b64decode(encoded.encode("ascii", errors="strict"), validate=True)
    if base64.b64encode(content).decode("ascii") != encoded:
        raise ValueError("noncanonical encoding")
    return content


def _digest_bytes(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def decode_worker_output(payload: bytes, task: WorkerTask) -> DecodedWorkerOutput:
    """Validate a bounded proposal; acceptance grants no durable authority."""
    code = "worker_output_invalid"
    try:
        return _decode(payload, task)
    except _TooLarge:
        code = "worker_output_too_large"
    except Exception:
        pass
    # Leave owned handlers before raising: even `from None` retains context.
    raise WorkerOutputError(code)


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _reject_constant(_value: str) -> None:
    raise ValueError("nonfinite constant")


def _check_tree(value: object) -> None:
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("nonfinite number")
    elif isinstance(value, dict):
        for key, item in value.items():
            _check_tree(key)
            _check_tree(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_tree(item)


def _decode(payload: bytes, task: WorkerTask) -> DecodedWorkerOutput:
    if type(payload) is not bytes or not payload:
        raise ValueError("expected nonempty bytes")
    if len(payload) > _FRAME_CAP:
        raise _TooLarge()
    text = payload.decode("utf-8", errors="strict")
    if text.startswith("\ufeff"):
        raise ValueError("BOM forbidden")
    value = json.loads(text, object_pairs_hook=_unique_pairs, parse_constant=_reject_constant)
    _check_tree(value)
    if type(value) is not dict or set(value) != {"schema", "result", "blobs"}:
        raise ValueError("invalid envelope")
    if value["schema"] != "maestro.worker-output.v1" or type(value["result"]) is not dict:
        raise ValueError("invalid schema or result")
    if type(value["blobs"]) is not list:
        raise ValueError("expected blob array")
    for blob in value["blobs"]:
        if type(blob) is not dict or set(blob) != {"digest", "content_base64"}:
            raise ValueError("invalid blob fields")
        if (type(blob["digest"]) is not str or not _DIGEST.fullmatch(blob["digest"])
            or type(blob["content_base64"]) is not str):
            raise ValueError("invalid blob values")
    valid_task = _revalidate_task(task)
    result = WorkerResult.model_validate(value["result"])
    refs = {ref.digest: ref for ref in result.artifacts}
    proposed = value["blobs"]
    if result.outcome != "candidate":
        if refs or proposed:
            raise ValueError("noncandidate content")
        validate_worker_result(valid_task, result)
        return DecodedWorkerOutput(result, ())
    if not 1 <= len(proposed) <= 128:
        raise ValueError("blob count")
    by_digest = {blob["digest"]: blob["content_base64"] for blob in proposed}
    if len(by_digest) != len(proposed) or set(by_digest) != set(refs):
        raise ValueError("blob identity mismatch")
    cap = min(valid_task.output_byte_limit, 16_384)
    encoded_cap = 4 * ((cap + 2) // 3)
    if any(len(encoded) > encoded_cap for encoded in by_digest.values()):
        raise _TooLarge()
    # All encodings must be canonical before actual overflow wins over
    # integrity errors. Retain at most the cap while finishing that phase.
    contents = {}
    total = 0
    for digest, encoded in by_digest.items():
        content = _decode_base64(encoded)
        total += len(content)
        if total <= cap:
            contents[digest] = content
    if total > cap:
        raise _TooLarge()
    for digest, content in contents.items():
        if len(content) != refs[digest].size_bytes or _digest_bytes(content) != digest:
            raise ValueError("content integrity mismatch")
    validate_worker_result(valid_task, result)
    blobs = tuple(DecodedCandidateBlob(refs[digest], contents[digest])
                  for digest in sorted(contents))
    return DecodedWorkerOutput(result, blobs)


__all__ = [
    "DecodedCandidateBlob", "DecodedWorkerOutput", "WorkerOutputError", "decode_worker_output",
]
