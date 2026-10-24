"""Bounded, non-authoritative general Worker turns and proposals.

The wire validation and fixed proposal builder use only the standard library.
Host contract imports are lazy so the isolated child needs no site packages.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import json
import math
import re
from typing import TYPE_CHECKING, Literal
from uuid import UUID

if TYPE_CHECKING:
    from .contracts import AttemptContext, WorkerTask
    from .worker_output import DecodedWorkerOutput

MAX_FRAME_BYTES = 1_048_576
MAX_OUTPUT_BYTES = 16_384
_TURN_SCHEMA = "maestro.general-worker-turn.v1"
_REQUEST_SCHEMA = "maestro.general-worker-request.v1"
_COMPLETE_SCHEMA = "maestro.general-worker-complete.v1"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}", re.ASCII)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}", re.ASCII)
_TEXT_CONTEXT = {"run_id", "node_id", "attempt_id", "agent_instance_id"}
_HASH_CONTEXT = {"input_manifest_hash", "effective_config_hash", "registry_hash",
    "policy_manifest_hash", "routing_decision_hash", "planning_contract_hash"}
_TASK_FIELDS = {"context", "role", "task_text", "input_artifacts",
    "tool_capabilities", "output_byte_limit"}


class GeneralWorkerProtocolError(ValueError):
    """Detached, fixed-code failure containing no caller data."""

    def __init__(self) -> None:
        super().__init__("general_worker_protocol_invalid")


@dataclass(frozen=True, slots=True)
class GeneralWorkerTurn:
    phase: Literal["request", "complete"]
    launch_id: str
    task: WorkerTask = field(repr=False)
    response_text: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class GeneralWorkerRequest:
    context: AttemptContext
    launch_id: str
    input_commitment: str


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8", errors="strict")


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _constant(_value: str) -> None:
    raise ValueError("nonfinite value")


def _plain(value: object) -> object:
    """Copy only exact built-in values, without invoking subclass callbacks."""
    kind = type(value)
    if kind is str:
        value.encode("utf-8", errors="strict")
    elif kind is float:
        if not math.isfinite(value):
            raise ValueError("nonfinite value")
    elif kind is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("invalid field")
        return {_plain(key): _plain(item) for key, item in value.items()}
    elif kind in (list, tuple):
        return [_plain(item) for item in value]
    elif kind not in (int, bool, type(None)):
        raise ValueError("invalid value type")
    return value


def _load(payload: bytes) -> dict:
    if type(payload) is not bytes or not 0 < len(payload) <= MAX_FRAME_BYTES:
        raise ValueError("invalid frame")
    value = json.loads(payload.decode("utf-8", errors="strict"),
        object_pairs_hook=_pairs, parse_constant=_constant)
    _plain(value)
    if type(value) is not dict:
        raise ValueError("invalid envelope")
    return value


def _fields(value: object, names: set[str]) -> None:
    if type(value) is not dict or set(value) != names:
        raise ValueError("invalid fields")


def _text(value: object, *, maximum: int, pattern=None, blank=False) -> None:
    if type(value) is not str or not 1 <= len(value) <= maximum:
        raise ValueError("invalid text")
    value.encode("utf-8", errors="strict")
    if (blank and not value.strip()) or (pattern and not pattern.fullmatch(value)):
        raise ValueError("invalid text")


def _integer(value: object, minimum: int, maximum: int | None = None) -> None:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise ValueError("invalid integer")


def _context(value: dict) -> None:
    _fields(value, _TEXT_CONTEXT | _HASH_CONTEXT | {"fencing_generation", "graph_version"})
    for name in _TEXT_CONTEXT:
        _text(value[name], maximum=128, pattern=_IDENTIFIER)
    for name in _HASH_CONTEXT:
        _text(value[name], maximum=71, pattern=_DIGEST)
    _integer(value["fencing_generation"], 1)
    _integer(value["graph_version"], 0)


def _task(value: dict) -> None:
    _fields(value, _TASK_FIELDS)
    _context(value["context"])
    _text(value["role"], maximum=64, pattern=_IDENTIFIER)
    _text(value["task_text"], maximum=100_000, blank=True)
    _integer(value["output_byte_limit"], 1, 1_073_741_824)
    for name, cap in (("input_artifacts", 128), ("tool_capabilities", 64)):
        if type(value[name]) is not list or len(value[name]) > cap:
            raise ValueError("invalid array")
    digests = set()
    for artifact in value["input_artifacts"]:
        _fields(artifact, {"digest", "size_bytes", "artifact_type", "media_type"})
        _text(artifact["digest"], maximum=71, pattern=_DIGEST)
        _integer(artifact["size_bytes"], 0)
        _text(artifact["artifact_type"], maximum=64, pattern=_IDENTIFIER)
        media = artifact["media_type"]
        _text(media, maximum=128)
        if media != media.strip() or any(ord(c) < 32 or ord(c) == 127 for c in media):
            raise ValueError("invalid media type")
        if artifact["digest"] in digests:
            raise ValueError("duplicate artifact")
        digests.add(artifact["digest"])
    grants = set()
    for capability in value["tool_capabilities"]:
        _fields(capability, {"grant_id", "tool_id", "scope_hash"})
        _text(capability["grant_id"], maximum=128, pattern=_IDENTIFIER)
        _text(capability["tool_id"], maximum=128, pattern=_IDENTIFIER)
        _text(capability["scope_hash"], maximum=71, pattern=_DIGEST)
        if capability["grant_id"] in grants:
            raise ValueError("duplicate grant")
        grants.add(capability["grant_id"])
    if [item["grant_id"] for item in value["tool_capabilities"]] != sorted(grants):
        raise ValueError("noncanonical capability order")


def _validate_turn(value: dict) -> None:
    _fields(value, {"schema", "phase", "launch_id", "task", "response_text"})
    if value["schema"] != _TURN_SCHEMA or value["phase"] not in ("request", "complete"):
        raise ValueError("invalid turn kind")
    launch_id = value["launch_id"]
    if type(launch_id) is not str or str(UUID(launch_id)) != launch_id or UUID(launch_id).version != 4:
        raise ValueError("invalid launch identity")
    _task(value["task"])
    response = value["response_text"]
    if value["phase"] == "request":
        if response is not None:
            raise ValueError("unexpected response")
    elif type(response) is not str or len(response.encode("utf-8", errors="strict")) > min(
        MAX_OUTPUT_BYTES, value["task"]["output_byte_limit"]
    ):
        raise ValueError("invalid response")


def _model_fields(value: object, expected: type) -> dict:
    if type(value) is not expected:
        raise ValueError("unexpected contract type")
    fields = object.__getattribute__(value, "__dict__")
    extras = object.__getattribute__(value, "__pydantic_extra__")
    if (type(fields) is not dict or any(type(key) is not str for key in fields)
        or set(fields) != set(expected.model_fields) or extras is not None):
        raise ValueError("invalid constructed model")
    return fields


def _turn_value(turn: GeneralWorkerTurn) -> dict:
    from .contracts import ArtifactRef, AttemptContext, ToolCapabilityRef, WorkerTask

    if type(turn) is not GeneralWorkerTurn:
        raise ValueError("unexpected turn type")
    fields = _model_fields(turn.task, WorkerTask).copy()
    fields["context"] = _model_fields(fields["context"], AttemptContext)
    for name, expected in (("input_artifacts", ArtifactRef), ("tool_capabilities", ToolCapabilityRef)):
        if type(fields[name]) not in (list, tuple):
            raise ValueError("invalid array")
        fields[name] = [_model_fields(item, expected) for item in fields[name]]
    value = _plain({"schema": _TURN_SCHEMA, "phase": turn.phase,
        "launch_id": turn.launch_id, "task": fields, "response_text": turn.response_text})
    _validate_turn(value)
    return value


def encode_general_worker_turn(turn: GeneralWorkerTurn) -> bytes:
    try:
        raw = _canonical(_turn_value(turn))
        if len(raw) <= MAX_FRAME_BYTES:
            return raw
    except Exception:
        pass
    raise GeneralWorkerProtocolError()


def decode_general_worker_turn(payload: bytes) -> GeneralWorkerTurn:
    try:
        from .contracts import WorkerTask

        value = _load(payload)
        _validate_turn(value)
        return GeneralWorkerTurn(value["phase"], value["launch_id"],
            WorkerTask.model_validate(value["task"]), value["response_text"])
    except Exception:
        pass
    raise GeneralWorkerProtocolError()


def _input_commitment(task: dict) -> str:
    # Transport fingerprint; never replaces context.input_manifest_hash.
    return "sha256:" + hashlib.sha256(b"maestro.general-worker-input.v1\0" + _canonical(task)).hexdigest()


def _proposal_bytes(payload: bytes) -> bytes:
    """Fixed, stdlib-only child behavior; raw bytes stay on private pipes."""
    value = _load(payload)
    _validate_turn(value)
    task = value["task"]
    if value["phase"] == "request":
        proposal = {"schema": _REQUEST_SCHEMA, "launch_id": value["launch_id"],
            "context": task["context"], "input_commitment": _input_commitment(task)}
    else:
        content = value["response_text"].encode("utf-8", errors="strict")
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        output = {"schema": "maestro.worker-output.v1", "result": {
            "context": task["context"], "result_id": "general-" + value["launch_id"],
            "outcome": "candidate", "failure_code": None, "artifacts": [{
                "digest": digest, "size_bytes": len(content), "artifact_type": "text",
                "media_type": "text/plain; charset=utf-8"}]},
            "blobs": [{"digest": digest, "content_base64": base64.b64encode(content).decode("ascii")}]}
        proposal = {"schema": _COMPLETE_SCHEMA, "launch_id": value["launch_id"], "output": output}
    raw = _canonical(proposal)
    if len(raw) > MAX_FRAME_BYTES:
        raise ValueError("proposal exceeds frame limit")
    return raw


def decode_general_worker_proposal(
    payload: bytes, turn: GeneralWorkerTurn,
) -> GeneralWorkerRequest | DecodedWorkerOutput:
    try:
        value = _load(payload)
        expected = _turn_value(turn)
        if value.get("launch_id") != expected["launch_id"]:
            raise ValueError("launch mismatch")
        if expected["phase"] == "request":
            _fields(value, {"schema", "context", "launch_id", "input_commitment"})
            _context(value["context"])
            if (value["schema"] != _REQUEST_SCHEMA or value["context"] != expected["task"]["context"]
                or value["input_commitment"] != _input_commitment(expected["task"])):
                raise ValueError("request mismatch")
            from .contracts import AttemptContext

            return GeneralWorkerRequest(AttemptContext.model_validate(value["context"]),
                value["launch_id"], value["input_commitment"])
        _fields(value, {"schema", "launch_id", "output"})
        if value["schema"] != _COMPLETE_SCHEMA:
            raise ValueError("completion mismatch")
        from .worker_output import decode_worker_output

        decoded = decode_worker_output(_canonical(value["output"]), turn.task)
        if (decoded.result.outcome != "candidate" or len(decoded.blobs) != 1
            or decoded.blobs[0].content != turn.response_text.encode("utf-8")
            or decoded.blobs[0].artifact.artifact_type != "text"
            or decoded.blobs[0].artifact.media_type != "text/plain; charset=utf-8"):
            raise ValueError("terminal content mismatch")
        return decoded
    except Exception:
        pass
    raise GeneralWorkerProtocolError()


__all__ = ["GeneralWorkerTurn", "GeneralWorkerRequest", "GeneralWorkerProtocolError",
    "encode_general_worker_turn", "decode_general_worker_turn", "decode_general_worker_proposal"]
