"""One explicit tool-program proposal over bounded pipes; no execution authority.

The fixed child branch uses only the standard library. Host contracts below are
never imported into that branch or made available as control-plane handles.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

MAX_FRAME_BYTES = 1_048_576
MAX_PROPOSAL_BYTES = 16_384
TOOL_ID = "workspace.write-candidate"


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8", errors="strict")


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _constant(_value):
    raise ValueError("nonfinite number")


def _json(raw, limit):
    if type(raw) is not bytes or not raw or len(raw) > limit:
        raise ValueError("invalid frame")
    return json.loads(raw.decode("utf-8", errors="strict"),
                      object_pairs_hook=_pairs, parse_constant=_constant)


def operation_id(context):
    identity = {key: context[key] for key in (
        "run_id", "node_id", "attempt_id", "fencing_generation", "input_manifest_hash")}
    return "sha256:" + hashlib.sha256(b"maestro.workspace-worker.v1\0" + canonical(identity)).hexdigest()


def _scope(context, program):
    program_hash = "sha256:" + hashlib.sha256(canonical(program)).hexdigest()
    return "sha256:" + hashlib.sha256(canonical({"context": context, "program_hash": program_hash})).hexdigest()


def _child_main():
    try:
        envelope = _json(sys.stdin.buffer.read(MAX_FRAME_BYTES + 1), MAX_FRAME_BYTES)
        if set(envelope) != {"schema", "task"} or type(envelope["schema"]) is not int or envelope["schema"] != 1:
            return 64
        task = envelope["task"]
        program = _json(task["task_text"].encode(), MAX_FRAME_BYTES)
        if (set(program) != {"schema", "tool_id", "command"}
            or type(program["schema"]) is not int or program["schema"] != 1
            or program["tool_id"] != TOOL_ID or canonical(program).decode() != task["task_text"]
            or len(task["tool_capabilities"]) != 1):
            return 64
        proposal = {"schema": 1, "context": task["context"],
                    "request_id": operation_id(task["context"]),
                    "tool_reference": task["tool_capabilities"][0], "command": program["command"]}
        raw = canonical(proposal)
        if len(raw) > MAX_PROPOSAL_BYTES:
            return 64
        sys.stdout.buffer.write(raw)
        sys.stdout.buffer.flush()
        return 0
    except (ValueError, TypeError, KeyError, IndexError, OSError, RecursionError):
        return 64


if __name__ == "__main__":
    raise SystemExit(_child_main() if sys.argv[1:] == ["--child"] else 64)


from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator, model_validator
from orchestrator.runtime.contracts import AttemptContext, ToolCapabilityRef, WorkerTask
from orchestrator.tools.gateway import ToolRequest
from orchestrator.validation import revalidate_model


class WorkspaceWorkerError(ValueError):
    """Stable detached protocol/transport error, never containing input text."""


def checked(model, value):
    """Reject unsafe constructed extras recursively before rebuilding models."""
    def inspect(item):
        if isinstance(item, BaseModel):
            if (set(item.__dict__) - set(type(item).model_fields)) or item.model_extra:
                raise ValueError("unexpected fields")
            for field in type(item).model_fields:
                inspect(getattr(item, field))
        elif isinstance(item, (tuple, list)):
            for child in item:
                inspect(child)
    inspect(value)
    return revalidate_model(model, value)


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid",
                             hide_input_in_errors=True, populate_by_name=True)
    schema_id: Literal[1] = Field(default=1, alias="schema")

    @field_validator("schema_id", mode="before")
    @classmethod
    def strict_schema(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("unsupported schema")
        return value


class WorkspaceToolProgram(_Model):
    tool_id: Literal["workspace.write-candidate"] = TOOL_ID
    command: tuple[StrictStr, ...] = Field(min_length=1, max_length=128, repr=False)

    @field_validator("command", mode="before")
    @classmethod
    def command_array(cls, value):
        return ToolRequest.normalize_command(value)

    @field_validator("command")
    @classmethod
    def command_bound(cls, value):
        ToolRequest.validate_command(value)
        if len(canonical(value)) > 8_192:
            raise ValueError("command encoding exceeds bound")
        return value

    @property
    def canonical_text(self):
        return canonical(checked(WorkspaceToolProgram, self).model_dump(mode="json", by_alias=True)).decode()


def decode_workspace_program(raw: bytes) -> WorkspaceToolProgram:
    try:
        value = WorkspaceToolProgram.model_validate(_json(raw, MAX_FRAME_BYTES))
        if value.canonical_text.encode() != raw:
            raise ValueError("noncanonical program")
        return value
    except Exception:
        pass
    raise WorkspaceWorkerError("workspace_program_invalid") from None


class WorkspaceWorkerTask(_Model):
    task: WorkerTask = Field(repr=False)

    @model_validator(mode="after")
    def bound_task(self):
        task = checked(WorkerTask, self.task)
        program = decode_workspace_program(task.task_text.encode())
        scope = _scope(task.context.model_dump(mode="json"), program.model_dump(mode="json", by_alias=True))
        expected = ToolCapabilityRef(grant_id=operation_id(task.context.model_dump()),
                                     tool_id=TOOL_ID, scope_hash=scope)
        if task.tool_capabilities != (expected,) or task.input_artifacts or task.output_byte_limit != MAX_PROPOSAL_BYTES:
            raise ValueError("task capability is not bound")
        return self

    @classmethod
    def bind(cls, task: WorkerTask):
        try:
            task = checked(WorkerTask, task)
            if task.tool_capabilities or task.input_artifacts:
                raise ValueError("input already carries capabilities")
            program = decode_workspace_program(task.task_text.encode())
            context = task.context.model_dump(mode="json")
            ref = ToolCapabilityRef(grant_id=operation_id(context), tool_id=TOOL_ID,
                scope_hash=_scope(context, program.model_dump(mode="json", by_alias=True)))
            result = cls(task=WorkerTask.model_validate(task.model_dump() | {
                "tool_capabilities": (ref,), "output_byte_limit": MAX_PROPOSAL_BYTES}))
            if len(result.model_dump_json(by_alias=True).encode()) > MAX_FRAME_BYTES:
                raise ValueError("frame exceeds bound")
            return result
        except Exception:
            pass
        raise WorkspaceWorkerError("workspace_task_invalid") from None


class WorkspaceWorkerProposal(_Model):
    context: AttemptContext
    request_id: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    tool_reference: ToolCapabilityRef
    command: tuple[StrictStr, ...] = Field(min_length=1, max_length=128, repr=False)

    @field_validator("command", mode="before")
    @classmethod
    def command_array(cls, value):
        return ToolRequest.normalize_command(value)

    @field_validator("command")
    @classmethod
    def command_bound(cls, value):
        return WorkspaceToolProgram.command_bound(value)


def decode_workspace_proposal(raw: bytes) -> WorkspaceWorkerProposal:
    try:
        value = _json(raw, MAX_PROPOSAL_BYTES)
        if not isinstance(value, dict) or set(value) != {"schema", "context", "request_id", "tool_reference", "command"}:
            raise ValueError("invalid proposal fields")
        return WorkspaceWorkerProposal.model_validate(value)
    except Exception:
        pass
    raise WorkspaceWorkerError("workspace_proposal_invalid") from None


def validate_workspace_proposal(task, proposal):
    try:
        task = checked(WorkspaceWorkerTask, task)
        proposal = checked(WorkspaceWorkerProposal, proposal)
        program = decode_workspace_program(task.task.task_text.encode())
        if (proposal.context != task.task.context or proposal.command != program.command
            or proposal.tool_reference != task.task.tool_capabilities[0]
            or proposal.request_id != operation_id(task.task.context.model_dump())
            or len(proposal.model_dump_json(by_alias=True).encode()) > MAX_PROPOSAL_BYTES):
            raise ValueError("proposal binding mismatch")
        return
    except Exception:
        pass
    raise WorkspaceWorkerError("workspace_proposal_invalid") from None


def proposal_bytes(task):
    """Canonical proposal for the fixed interpreter's explicit program."""
    task = checked(WorkspaceWorkerTask, task)
    proposal = WorkspaceWorkerProposal(context=task.task.context,
        request_id=operation_id(task.task.context.model_dump()),
        tool_reference=task.task.tool_capabilities[0],
        command=decode_workspace_program(task.task.task_text.encode()).command)
    validate_workspace_proposal(task, proposal)
    return canonical(proposal.model_dump(mode="json", by_alias=True))


class IsolatedWorkspaceWorker:
    def __init__(self, *, launcher=None, limits=None):
        from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher
        self._launcher = launcher if launcher is not None else SystemdReadOnlyLauncher()
        self._limits = limits if limits is not None else SandboxLimits(timeout_seconds=10, output_bytes=16_384)
        self.termination_receipt = None

    def propose(self, workspace: str | Path, task: WorkspaceWorkerTask) -> WorkspaceWorkerProposal:
        try:
            task = checked(WorkspaceWorkerTask, task)
            raw = task.model_dump_json(by_alias=True).encode()
            if len(raw) > MAX_FRAME_BYTES:
                raise ValueError("frame exceeds bound")
        except Exception:
            raw = None
        if raw is None:
            raise WorkspaceWorkerError("workspace_task_invalid") from None
        self.termination_receipt = None
        session = self._launcher.launch(workspace,
            ["/usr/bin/python3", "@maestro-runtime@/orchestrator/runtime/workspace_worker.py", "--child"],
            limits=self._limits, input_bytes=raw)
        result = session.wait()
        if result.termination_confirmed:
            self.termination_receipt = result.termination_receipt
        if (not result.termination_confirmed or result.returncode != 0 or not result.input_written
            or result.cancelled or result.timed_out or result.output_limited or result.stderr):
            raise WorkspaceWorkerError("workspace_worker_transport_failed")
        proposal = decode_workspace_proposal(result.stdout)
        validate_workspace_proposal(task, proposal)
        return proposal
