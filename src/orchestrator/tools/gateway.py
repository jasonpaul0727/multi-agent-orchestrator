"""Audited, policy-bound read-only command gateway.

This is deliberately a single-tool vertical slice, not a general plugin host.
It delegates isolation to the measured Linux/systemd launcher and refuses to
execute when attempt authority, current policy state, durable audit, or the
read-only platform profile cannot be verified.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.isolation import SandboxLimits, SandboxResult, SandboxSession, SystemdReadOnlyLauncher
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.security.policy import PolicyDecision, PolicyEngine, PolicyManifest, PolicyRequest


READ_ONLY_COMMAND_TOOL_ID = "system.readonly-command"
_MAX_COMMAND_BYTES = 32 * 1024
_MAX_ARGUMENTS = 128
_SAFE_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$"


class _ToolModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class ToolRequest(_ToolModel):
    """One bounded read-only command request tied to an accepted attempt."""

    request_id: StrictStr = Field(min_length=1, pattern=_SAFE_IDENTIFIER)
    run_id: StrictStr = Field(min_length=1, pattern=_SAFE_IDENTIFIER)
    node_id: StrictStr = Field(min_length=1, pattern=_SAFE_IDENTIFIER)
    attempt_id: StrictStr = Field(min_length=1, pattern=_SAFE_IDENTIFIER)
    fencing_generation: StrictInt = Field(ge=0)
    role: StrictStr = Field(min_length=1, pattern=_SAFE_IDENTIFIER)
    causation_id: StrictStr = Field(min_length=1)
    workspace: StrictStr = Field(min_length=1)
    command: tuple[StrictStr, ...] = Field(min_length=1, max_length=_MAX_ARGUMENTS)

    @field_validator("command", mode="before")
    @classmethod
    def normalize_command(cls, value: object) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("command must be an argument array")
        return tuple(value)

    @field_validator("command")
    @classmethod
    def validate_command(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or not value[0].strip():
            raise ValueError("command executable must not be blank")
        if any(not item or "\x00" in item for item in value):
            raise ValueError("command arguments must be non-empty and must not contain NUL")
        if sum(len(item.encode("utf-8")) for item in value) > _MAX_COMMAND_BYTES:
            raise ValueError("command arguments exceed the request bound")
        return value

    @field_validator("workspace")
    @classmethod
    def validate_workspace(cls, value: str) -> str:
        if "\x00" in value or not Path(value).is_absolute():
            raise ValueError("workspace must be an absolute path")
        return value

    @model_validator(mode="after")
    def validate_causation(self) -> "ToolRequest":
        if self.causation_id != self.causation_id.strip():
            raise ValueError("causation_id must not have surrounding whitespace")
        return self


@dataclass(frozen=True)
class PolicyState:
    """Trusted current revocation state supplied by the control plane."""

    revocation_version: int
    emergency_deny_version: int
    revoked: bool = False
    emergency_denied: bool = False

    def __post_init__(self) -> None:
        for name in ("revocation_version", "emergency_deny_version"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if not isinstance(self.revoked, bool) or not isinstance(self.emergency_denied, bool):
            raise ValueError("policy-state flags must be booleans")


class AttemptAuthority(Protocol):
    """Control-plane check for accepted attempt, generation, and causation."""

    def is_current(self, request: ToolRequest) -> bool: ...


class _EventStore(Protocol):
    def append_checked(
        self,
        stream_type: str,
        stream_id: str,
        idempotency_key: str,
        decide: Callable[[list[StoredEvent], int], Sequence[EventDraft] | None],
    ) -> list[StoredEvent]: ...

    def read_stream(self, stream_type: str, stream_id: str, after_version: int = 0) -> list[StoredEvent]: ...


class _Session(Protocol):
    def wait(self) -> SandboxResult: ...

    def cancel(self) -> bool: ...


@dataclass(frozen=True)
class ToolExecutionResult:
    """Bounded process output; audit events retain hashes and lengths only."""

    request_id: str
    outcome: Literal["completed", "awaiting_approval", "denied", "authority_lost", "execution_unknown"]
    decision: PolicyDecision
    stdout: bytes = b""
    stderr: bytes = b""
    returncode: int | None = None
    stdout_sha256: str | None = None
    stderr_sha256: str | None = None
    termination_confirmed: bool = True
    cancelled: bool = False
    timed_out: bool = False
    output_limited: bool = False


class ToolAuditUnavailable(RuntimeError):
    """No execution is allowed unless its preceding audit append is durable."""


class ToolRequestAlreadyUsed(RuntimeError):
    """The request id already reserved or completed an execution attempt."""


class ToolGateway:
    """A single, policy-constrained command tool using read-only isolation.

    The supplied callbacks are trusted control-plane capabilities. In
    particular, ``attempt_authority.is_current`` must validate the accepted
    attempt and ``causation_id`` against durable scheduler state; returning
    false or raising always prevents execution.
    """

    def __init__(
        self,
        *,
        run_id: str,
        event_store: _EventStore,
        policy_manifest: PolicyManifest,
        attempt_authority: AttemptAuthority,
        policy_state: Callable[[ToolRequest], PolicyState],
        launcher: SystemdReadOnlyLauncher | Any | None = None,
        limits: SandboxLimits | None = None,
        monitor_interval_seconds: float = 0.05,
    ) -> None:
        if not isinstance(run_id, str) or re.fullmatch(_SAFE_IDENTIFIER, run_id) is None:
            raise ValueError("run_id must be a stable non-blank identifier")
        if monitor_interval_seconds < 0.01 or monitor_interval_seconds > 1:
            raise ValueError("monitor interval must be between 10 ms and 1 s")
        self._run_id = run_id
        self._events = event_store
        self._manifest = policy_manifest
        self._attempt_authority = attempt_authority
        self._policy_state = policy_state
        self._launcher = launcher or SystemdReadOnlyLauncher()
        self._limits = limits or SandboxLimits()
        self._monitor_interval = monitor_interval_seconds
        self._engine = PolicyEngine()

    def execute(self, request: ToolRequest) -> ToolExecutionResult:
        """Evaluate, durably reserve, and optionally execute one safe-read tool."""

        if not isinstance(request, ToolRequest):
            request = ToolRequest.model_validate(request)
        if request.run_id != self._run_id:
            raise PermissionError("tool request does not belong to this Run snapshot")
        if not self._attempt_is_current(request):
            self._record_block(request, "attempt_not_current")
            raise PermissionError("attempt is not current")

        try:
            state = self._read_policy_state(request)
        except Exception as exc:
            self._record_block(request, "policy_state_unavailable")
            raise PermissionError("current policy state is unavailable") from exc
        decision = self._decision(request, state)
        request_hash = self._request_hash(request)
        self._record_request(request, request_hash, state, decision)

        if decision.outcome == "deny":
            return ToolExecutionResult(request.request_id, "denied", decision)
        if decision.outcome == "needs_approval":
            return ToolExecutionResult(request.request_id, "awaiting_approval", decision)

        # Policy and attempt authority are rechecked immediately before the
        # one-use capability is consumed and the process is launched.
        try:
            current = self._read_policy_state(request)
        except Exception as exc:
            self._record_block(request, "policy_state_unavailable")
            raise PermissionError("current policy state is unavailable") from exc
        if current != state:
            self._record_block(request, "policy_state_changed")
            raise PermissionError("policy state changed after authorization")
        if not self._attempt_is_current(request):
            self._record_block(request, "attempt_not_current_before_launch")
            raise PermissionError("attempt is not current")
        self._consume_grant_and_start(request, request_hash, decision)

        try:
            session: _Session = self._launcher.launch(
                request.workspace,
                request.command,
                limits=self._limits,
            )
        except Exception as exc:
            self._record_terminal(request, "ToolExecutionFailed", {"reason": "launcher_unavailable"})
            raise

        try:
            result, authority_lost = self._wait_with_authority(session, request, state)
        except Exception:
            try:
                session.cancel()
            except Exception:
                pass
            self._record_terminal(
                request,
                "ToolExecutionOutcomeUnknown",
                {"outcome": "execution_unknown", "reason": "session_wait_failed", "termination_confirmed": False},
            )
            raise
        accepted = not authority_lost and self._authorized_now(request, state)
        if not result.termination_confirmed:
            outcome: Literal["completed", "authority_lost", "execution_unknown"] = "execution_unknown"
            terminal_type = "ToolExecutionOutcomeUnknown"
        elif not accepted:
            outcome = "authority_lost"
            terminal_type = "ToolExecutionCancelled"
        else:
            outcome = "completed"
            terminal_type = "ToolExecutionCompleted"
        self._record_terminal(request, terminal_type, _result_audit_payload(request, result, outcome))
        output_authorized = outcome == "completed"
        return ToolExecutionResult(
            request_id=request.request_id,
            outcome=outcome,
            decision=decision,
            stdout=result.stdout if output_authorized else b"",
            stderr=result.stderr if output_authorized else b"",
            returncode=result.returncode,
            stdout_sha256=_digest(result.stdout),
            stderr_sha256=_digest(result.stderr),
            termination_confirmed=result.termination_confirmed,
            cancelled=result.cancelled or authority_lost,
            timed_out=result.timed_out,
            output_limited=result.output_limited,
        )

    def _decision(self, request: ToolRequest, state: PolicyState) -> PolicyDecision:
        policy_request = PolicyRequest(
            request_id=request.request_id,
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=request.fencing_generation,
            role=request.role,
            action_category="safe_read",
            tool_id=READ_ONLY_COMMAND_TOOL_ID,
            required_permission="read-only",
            normalized_request_hash=self._request_hash(request),
            policy_manifest_hash=self._manifest.content_hash,
            revocation_version=state.revocation_version,
            emergency_deny_version=state.emergency_deny_version,
            revoked=state.revoked,
            emergency_denied=state.emergency_denied,
        )
        return self._engine.evaluate(policy_request, self._manifest)

    def _record_request(
        self,
        request: ToolRequest,
        request_hash: str,
        state: PolicyState,
        decision: PolicyDecision,
    ) -> None:
        seen = False
        stream_id = request.run_id

        def decide(events: list[StoredEvent], version: int) -> Sequence[EventDraft] | None:
            nonlocal seen
            existing = [
                event
                for event in events
                if event.event_type == "ToolRequestReceived"
                and event.payload.get("request_id") == request.request_id
            ]
            if existing:
                if any(event.payload.get("request_hash") != request_hash for event in existing):
                    raise ValueError("tool request id was reused with different content")
                seen = True
                return None
            context = _event_context(request, request.causation_id)
            drafts = [
                EventDraft(
                    "ToolRequestReceived",
                    {
                        "request_id": request.request_id,
                        "request_hash": request_hash,
                        "tool_id": READ_ONLY_COMMAND_TOOL_ID,
                        "policy_manifest_hash": self._manifest.content_hash,
                        "revocation_version": state.revocation_version,
                        "emergency_deny_version": state.emergency_deny_version,
                    },
                    **context,
                ),
                EventDraft("PolicyDecision", decision.model_dump(mode="json"), **context),
            ]
            if decision.outcome == "allow":
                drafts.append(
                    EventDraft(
                        "CapabilityGrant",
                        {
                            "grant_id": _grant_id(request.request_id),
                            "request_id": request.request_id,
                            "action_category": "safe_read",
                            "tool_id": READ_ONLY_COMMAND_TOOL_ID,
                            "scope_hash": request_hash,
                            "policy_manifest_hash": self._manifest.content_hash,
                            "one_use": True,
                        },
                        **context,
                    )
                )
            elif decision.outcome == "needs_approval":
                drafts.append(
                    EventDraft(
                        "ApprovalRequested",
                        {
                            "request_id": request.request_id,
                            "decision_hash": decision.decision_hash,
                            "scope_hash": request_hash,
                            "action_category": "safe_read",
                        },
                        **context,
                    )
                )
            return drafts

        try:
            self._events.append_checked("security", stream_id, f"tool-request:{request.request_id}", decide)
        except Exception as exc:
            raise ToolAuditUnavailable("tool request and policy decision were not durably audited") from exc
        if seen:
            raise ToolRequestAlreadyUsed("tool request id already exists; execution is never replayed implicitly")

    def _consume_grant_and_start(self, request: ToolRequest, request_hash: str, decision: PolicyDecision) -> None:
        seen = False

        def decide(events: list[StoredEvent], version: int) -> Sequence[EventDraft] | None:
            nonlocal seen
            scoped = [event for event in events if event.payload.get("request_id") == request.request_id]
            grants = [event for event in scoped if event.event_type == "CapabilityGrant"]
            starts = [event for event in scoped if event.event_type == "ToolExecutionStarted"]
            consumed = [event for event in scoped if event.event_type == "ToolCapabilityConsumed"]
            terminals = [
                event
                for event in scoped
                if event.event_type in {
                    "ToolExecutionCompleted",
                    "ToolExecutionCancelled",
                    "ToolExecutionFailed",
                    "ToolExecutionOutcomeUnknown",
                }
            ]
            if starts or terminals or consumed:
                seen = True
                return None
            if decision.outcome != "allow" or len(grants) != 1:
                raise ToolAuditUnavailable("no unique durable capability grant exists")
            grant = grants[0]
            if (
                grant.payload.get("scope_hash") != request_hash
                or grant.payload.get("grant_id") != _grant_id(request.request_id)
                or grant.payload.get("one_use") is not True
                or grant.payload.get("action_category") != "safe_read"
                or grant.payload.get("tool_id") != READ_ONLY_COMMAND_TOOL_ID
                or grant.payload.get("policy_manifest_hash") != self._manifest.content_hash
            ):
                raise ToolAuditUnavailable("capability grant scope does not match request")
            context = _event_context(request, grant.event_id)
            return [
                EventDraft(
                    "ToolCapabilityConsumed",
                    {
                        "grant_id": _grant_id(request.request_id),
                        "request_id": request.request_id,
                        "scope_hash": request_hash,
                    },
                    **context,
                ),
                EventDraft(
                    "ToolExecutionStarted",
                    {
                        "request_id": request.request_id,
                        "request_hash": request_hash,
                        "policy_decision_hash": decision.decision_hash,
                    },
                    **_event_context(request, grant.event_id),
                ),
            ]

        try:
            self._events.append_checked("security", request.run_id, f"tool-start:{request.request_id}", decide)
        except Exception as exc:
            raise ToolAuditUnavailable("one-use capability could not be durably consumed") from exc
        if seen:
            raise ToolRequestAlreadyUsed("tool request has already started")

    def _record_terminal(self, request: ToolRequest, event_type: str, payload: dict[str, Any]) -> None:
        key = f"tool-terminal:{request.request_id}"

        def decide(events: list[StoredEvent], version: int) -> Sequence[EventDraft] | None:
            if any(
                event.event_type
                in {
                    "ToolExecutionCompleted",
                    "ToolExecutionCancelled",
                    "ToolExecutionFailed",
                    "ToolExecutionOutcomeUnknown",
                }
                and event.payload.get("request_id") == request.request_id
                for event in events
            ):
                return None
            start = next(
                (
                    event
                    for event in reversed(events)
                    if event.event_type == "ToolExecutionStarted"
                    and event.payload.get("request_id") == request.request_id
                ),
                None,
            )
            if start is None:
                raise ToolAuditUnavailable("terminal tool event has no durable start")
            return [EventDraft(event_type, {"request_id": request.request_id, **payload}, **_event_context(request, start.event_id))]

        try:
            self._events.append_checked("security", request.run_id, key, decide)
        except Exception as exc:
            raise ToolAuditUnavailable("tool execution outcome could not be durably audited") from exc

    def _record_block(self, request: ToolRequest, reason: str) -> None:
        request_hash = self._request_hash(request)

        def decide(events: list[StoredEvent], version: int) -> Sequence[EventDraft] | None:
            if any(
                event.event_type == "ToolExecutionBlocked" and event.payload.get("request_id") == request.request_id
                for event in events
            ):
                return None
            return [
                EventDraft(
                    "ToolExecutionBlocked",
                    {"request_id": request.request_id, "request_hash": request_hash, "reason": reason},
                    **_event_context(request, request.causation_id),
                )
            ]

        try:
            self._events.append_checked("security", request.run_id, f"tool-block:{request.request_id}:{reason}", decide)
        except Exception as exc:
            raise ToolAuditUnavailable("blocked tool request could not be durably audited") from exc

    def _attempt_is_current(self, request: ToolRequest) -> bool:
        try:
            return self._attempt_authority.is_current(request) is True
        except Exception:
            return False

    def _read_policy_state(self, request: ToolRequest) -> PolicyState:
        state = self._policy_state(request)
        if not isinstance(state, PolicyState):
            raise TypeError("policy_state provider must return PolicyState")
        return state

    def _authorized_now(self, request: ToolRequest, initial_state: PolicyState) -> bool:
        try:
            return self._attempt_authority.is_current(request) is True and self._read_policy_state(request) == initial_state
        except Exception:
            return False

    def _wait_with_authority(self, session: _Session, request: ToolRequest, state: PolicyState) -> tuple[SandboxResult, bool]:
        stop = threading.Event()
        lost = threading.Event()

        def monitor() -> None:
            while not stop.is_set():
                if not self._authorized_now(request, state):
                    lost.set()
                    try:
                        session.cancel()
                    except Exception:
                        pass
                    return
                if stop.wait(self._monitor_interval):
                    return

        watcher = threading.Thread(target=monitor, name="maestro-tool-authority", daemon=True)
        watcher.start()
        try:
            result = session.wait()
        finally:
            stop.set()
            watcher.join(timeout=2)
        return result, lost.is_set()

    def _request_hash(self, request: ToolRequest) -> str:
        payload = {
            "request_id": request.request_id,
            "run_id": request.run_id,
            "node_id": request.node_id,
            "attempt_id": request.attempt_id,
            "fencing_generation": request.fencing_generation,
            "role": request.role,
            "causation_id": request.causation_id,
            "tool_id": READ_ONLY_COMMAND_TOOL_ID,
            "workspace": os.path.abspath(request.workspace),
            "command": request.command,
            "limits": {
                name: getattr(self._limits, name)
                for name in (
                    "memory_bytes",
                    "tasks",
                    "cpu_percent",
                    "timeout_seconds",
                    "output_bytes",
                    "nofile",
                    "file_bytes",
                )
            },
            "policy_manifest_hash": self._manifest.content_hash,
        }
        return _digest(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())


def _event_context(request: ToolRequest, causation_id: str) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "node_id": request.node_id,
        "attempt_id": request.attempt_id,
        "fencing_generation": request.fencing_generation,
        "causation_id": causation_id,
    }


def _grant_id(request_id: str) -> str:
    digest = hashlib.sha256(request_id.encode()).hexdigest()[:32]
    return f"tool-grant:{digest}"


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _result_audit_payload(
    request: ToolRequest,
    result: SandboxResult,
    outcome: Literal["completed", "authority_lost", "execution_unknown"],
) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "outcome": outcome,
        "returncode": result.returncode,
        "stdout_sha256": _digest(result.stdout),
        "stdout_bytes": len(result.stdout),
        "stderr_sha256": _digest(result.stderr),
        "stderr_bytes": len(result.stderr),
        "termination_confirmed": result.termination_confirmed,
        "cancelled": result.cancelled,
        "timed_out": result.timed_out,
        "output_limited": result.output_limited,
        "elapsed_milliseconds": max(0, round(result.elapsed_seconds * 1000)),
    }


__all__ = [
    "READ_ONLY_COMMAND_TOOL_ID",
    "AttemptAuthority",
    "PolicyState",
    "ToolAuditUnavailable",
    "ToolExecutionResult",
    "ToolGateway",
    "ToolRequest",
    "ToolRequestAlreadyUsed",
]
