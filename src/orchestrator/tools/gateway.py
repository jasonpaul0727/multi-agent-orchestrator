"""Audited, policy-bound read-only command gateway.

This is deliberately a single-tool vertical slice, not a general plugin host.
It delegates isolation to the measured Linux/systemd launcher and refuses to
execute when attempt authority, current policy state, durable audit, or the
read-only platform profile cannot be verified.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.isolation import SandboxLimits, SandboxResult, SandboxSession, SystemdReadOnlyLauncher
from orchestrator.approvals import (
    ApprovalError,
    ApprovalInvalid,
    ApprovalPolicyState,
    ApprovalRequest,
    ApprovalService,
    ConsumedApproval,
    EffectIntentSpec,
    ExecutionAttempt,
)
from orchestrator.budget import CostEstimate
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.security.policy import PolicyDecision, PolicyEngine, PolicyManifest, PolicyRequest
from orchestrator.workspace_identity import workspace_identity_hash


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
    approval_request_id: str | None = None


class ToolAuditUnavailable(RuntimeError):
    """No execution is allowed unless its preceding audit append is durable."""


class ToolRequestAlreadyUsed(RuntimeError):
    """The request id already reserved or completed an execution attempt."""


class _AuthorizationLost(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


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
        workspace: str | Path,
        event_store: _EventStore,
        policy_manifest: PolicyManifest,
        attempt_authority: AttemptAuthority,
        policy_state: Callable[[ToolRequest], PolicyState],
        launcher: SystemdReadOnlyLauncher | Any | None = None,
        limits: SandboxLimits | None = None,
        monitor_interval_seconds: float = 0.05,
        approval_service: ApprovalService | None = None,
        approval_requester: Callable[[ToolRequest], str] | None = None,
        approval_isolation_profile_hash: str | None = None,
        approval_now: Callable[[], datetime] | None = None,
        approval_ttl_seconds: int = 3_600,
    ) -> None:
        if not isinstance(run_id, str) or re.fullmatch(_SAFE_IDENTIFIER, run_id) is None:
            raise ValueError("run_id must be a stable non-blank identifier")
        if not isinstance(workspace, (str, Path)):
            raise TypeError("workspace must be a trusted absolute directory")
        workspace_path = Path(workspace)
        if not workspace_path.is_absolute() or not workspace_path.is_dir():
            raise ValueError("workspace must be a trusted existing absolute directory")
        try:
            canonical_workspace = workspace_path.resolve(strict=True)
        except OSError as exc:
            raise ValueError("workspace could not be resolved") from exc
        if canonical_workspace == Path(canonical_workspace.anchor):
            raise ValueError("workspace must not be a filesystem root")
        if monitor_interval_seconds < 0.01 or monitor_interval_seconds > 1:
            raise ValueError("monitor interval must be between 10 ms and 1 s")
        if isinstance(approval_ttl_seconds, bool) or not isinstance(approval_ttl_seconds, int):
            raise ValueError("approval_ttl_seconds must be an integer")
        if approval_ttl_seconds < 1 or approval_ttl_seconds > 86_400:
            raise ValueError("approval_ttl_seconds must be between 1 second and 24 hours")
        if approval_service is None:
            if approval_requester is not None or approval_isolation_profile_hash is not None:
                raise ValueError("approval requester/profile require an ApprovalService")
        else:
            if not approval_service.uses_event_store(event_store):
                raise ValueError("ToolGateway and ApprovalService must share one SQLite event store")
            if approval_requester is None:
                raise ValueError("approval integration requires a trusted authenticated requester")
            if (
                not isinstance(approval_isolation_profile_hash, str)
                or len(approval_isolation_profile_hash) != 71
                or not approval_isolation_profile_hash.startswith("sha256:")
                or any(char not in "0123456789abcdef" for char in approval_isolation_profile_hash[7:])
            ):
                raise ValueError("approval integration requires a measured isolation profile hash")
        self._run_id = run_id
        self._workspace = canonical_workspace
        self._workspace_identity_hash = workspace_identity_hash(canonical_workspace)
        self._events = event_store
        self._manifest = policy_manifest
        self._attempt_authority = attempt_authority
        self._policy_state = policy_state
        self._launcher = launcher or SystemdReadOnlyLauncher()
        self._limits = limits or SandboxLimits()
        self._monitor_interval = monitor_interval_seconds
        self._engine = PolicyEngine()
        self._approval_service = approval_service
        self._approval_requester = approval_requester
        self._approval_isolation_profile_hash = approval_isolation_profile_hash
        self._approval_now = approval_now or (lambda: datetime.now(timezone.utc))
        self._approval_ttl = timedelta(seconds=approval_ttl_seconds)

    def execute(
        self,
        request: ToolRequest,
        *,
        approval_grant_id: str | None = None,
    ) -> ToolExecutionResult:
        """Evaluate, durably reserve, and optionally execute one safe-read tool."""

        if not isinstance(request, ToolRequest):
            request = ToolRequest.model_validate(request)
        if request.run_id != self._run_id:
            raise PermissionError("tool request does not belong to this Run snapshot")
        if approval_grant_id is not None and (
            not isinstance(approval_grant_id, str)
            or re.fullmatch(_SAFE_IDENTIFIER, approval_grant_id) is None
        ):
            raise ValueError("approval_grant_id must be a stable identifier")
        if request.workspace != str(self._workspace):
            self._record_block(request, "workspace_not_bound")
            raise PermissionError("tool request workspace is not the trusted Run workspace")
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
            if self._approval_service is None:
                if approval_grant_id is not None:
                    self._record_block(request, "approval_service_unavailable")
                    return ToolExecutionResult(request.request_id, "denied", decision)
                return ToolExecutionResult(request.request_id, "awaiting_approval", decision)
            if approval_grant_id is None:
                try:
                    approval_request_id = self._create_approval_request(request, decision, state)
                except Exception as exc:
                    self._record_block(request, "approval_request_unavailable")
                    raise ToolAuditUnavailable("exact approval request could not be recorded") from exc
                return ToolExecutionResult(
                    request.request_id,
                    "awaiting_approval",
                    decision,
                    approval_request_id=approval_request_id,
                )
        elif approval_grant_id is not None:
            self._record_block(request, "unexpected_approval_grant")
            return ToolExecutionResult(request.request_id, "denied", decision)

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
        consumed_approval: ConsumedApproval | None = None
        approval_attempt: ExecutionAttempt | None = None
        if approval_grant_id is None:
            self._consume_grant_and_start(request, request_hash, decision, state)
        else:
            assert self._approval_service is not None
            assert self._approval_isolation_profile_hash is not None
            try:
                approval_request = self._approval_service.request_for_grant(approval_grant_id)
                intent = self._approval_effect_intent(request, approval_request.approval_request_id)
                if (
                    approval_request.run_id != request.run_id
                    or approval_request.node_id != request.node_id
                    or approval_request.tool_id != READ_ONLY_COMMAND_TOOL_ID
                    or approval_request.action_category != decision.request.action_category
                    or approval_request.target_hash != intent.target_hash
                    or approval_request.parameters_hash != intent.parameters_hash
                    or approval_request.effect_intent_hash != intent.content_hash
                    or approval_request.policy_manifest_hash != self._manifest.content_hash
                    or approval_request.revocation_version != state.revocation_version
                    or approval_request.emergency_deny_version != state.emergency_deny_version
                ):
                    raise ApprovalInvalid("approval scope does not match this exact tool request")
                approval_attempt = ExecutionAttempt(
                    run_id=request.run_id,
                    node_id=request.node_id,
                    attempt_id=request.attempt_id,
                    fencing_generation=request.fencing_generation,
                    isolation_profile_hash=self._approval_isolation_profile_hash,
                )
                self._approval_service.bind_to_attempt(approval_grant_id, approval_attempt)
                consumed_approval = self._approval_service.consume_and_intend(
                    approval_grant_id,
                    approval_attempt,
                    intent,
                    CostEstimate(
                        amount_minor=0,
                        currency=self._approval_service.budget_currency_for_run(request.run_id),
                        token_limit=None,
                        tool_fee_minor=0,
                        snapshot_id="tool-readonly-v1",
                    ),
                )
                self._consume_approved_and_start(
                    request,
                    request_hash,
                    decision,
                    state,
                    consumed_approval,
                )
            except ApprovalError:
                self._record_block(request, "approval_grant_invalid")
                return ToolExecutionResult(request.request_id, "denied", decision)

        if not self._authorized_now(request, state):
            if consumed_approval is not None and approval_attempt is not None:
                self._record_approval_receipt(
                    consumed_approval,
                    approval_attempt,
                    outcome="not_applied",
                    receipt_hash=_digest(b"authority-lost-before-launch"),
                )
            self._record_terminal(
                request,
                "ToolExecutionCancelled",
                {"outcome": "authority_lost", "reason": "authority_lost_before_launch"},
            )
            raise PermissionError("attempt authority changed before isolated launch")

        try:
            session: _Session = self._launcher.launch(
                str(self._workspace),
                request.command,
                limits=self._limits,
                expected_workspace_identity_hash=self._workspace_identity_hash,
            )
        except Exception as exc:
            terminal = (
                "ToolExecutionOutcomeUnknown"
                if consumed_approval is not None
                else "ToolExecutionFailed"
            )
            self._record_terminal(request, terminal, {"reason": "launcher_unavailable"})
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
        if consumed_approval is not None and approval_attempt is not None and result.termination_confirmed:
            try:
                receipt_hash = _digest(json.dumps(
                    _result_audit_payload(request, result, outcome),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"))
                self._record_approval_receipt(
                    consumed_approval,
                    approval_attempt,
                    outcome="applied",
                    receipt_hash=receipt_hash,
                )
            except Exception:
                outcome = "execution_unknown"
                terminal_type = "ToolExecutionOutcomeUnknown"
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
            elif decision.outcome == "needs_approval" and self._approval_service is None:
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

    def _create_approval_request(
        self,
        request: ToolRequest,
        decision: PolicyDecision,
        state: PolicyState,
    ) -> str:
        assert self._approval_service is not None
        assert self._approval_requester is not None
        if not self._attempt_is_current(request) or self._read_policy_state(request) != state:
            raise ApprovalInvalid("attempt authority or policy changed before approval request creation")
        approval_request_id = _approval_request_id(request)
        intent = self._approval_effect_intent(request, approval_request_id)
        now = self._approval_now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("approval clock must return an aware datetime")
        approved_request = ApprovalRequest(
            approval_request_id=approval_request_id,
            run_id=request.run_id,
            node_id=request.node_id,
            requester_id=self._approval_requester(request),
            origin_attempt_id=request.attempt_id,
            origin_fencing_generation=request.fencing_generation,
            causation_id=request.causation_id,
            action_category=decision.request.action_category,
            tool_id=READ_ONLY_COMMAND_TOOL_ID,
            target_hash=intent.target_hash,
            parameters_hash=intent.parameters_hash,
            effect_intent_hash=intent.content_hash,
            policy_manifest_hash=self._manifest.content_hash,
            revocation_version=state.revocation_version,
            emergency_deny_version=state.emergency_deny_version,
            expires_at=now.astimezone(timezone.utc) + self._approval_ttl,
        )
        self._approval_service.create_request(approved_request)
        return approval_request_id

    def _approval_effect_intent(
        self,
        request: ToolRequest,
        approval_request_id: str,
    ) -> EffectIntentSpec:
        assert self._approval_isolation_profile_hash is not None
        limits = {
            name: getattr(self._limits, name)
            for name in (
                "memory_bytes", "tasks", "cpu_percent", "timeout_seconds",
                "output_bytes", "nofile", "file_bytes",
            )
        }
        target_hash = _hash_json({
            "tool_id": READ_ONLY_COMMAND_TOOL_ID,
            "workspace_identity_hash": self._workspace_identity_hash,
        })
        parameters_hash = _hash_json({
            "command": request.command,
            "role": request.role,
            "limits": limits,
            "policy_manifest_hash": self._manifest.content_hash,
            "isolation_profile_hash": self._approval_isolation_profile_hash,
        })
        effect_id = "tool-effect:" + hashlib.sha256(approval_request_id.encode("utf-8")).hexdigest()[:32]
        return EffectIntentSpec(
            effect_id=effect_id,
            target_hash=target_hash,
            parameters_hash=parameters_hash,
            provider_idempotency_key=None,
            maximum_cost_minor=0,
            recovery_class="manual_only",
        )

    def _consume_approved_and_start(
        self,
        request: ToolRequest,
        request_hash: str,
        decision: PolicyDecision,
        state: PolicyState,
        consumed: ConsumedApproval,
    ) -> None:
        if decision.outcome != "needs_approval":
            raise ToolAuditUnavailable("approval grant is not attached to an approval-required action")
        budget_events = self._events.read_stream("budget", request.run_id)
        intent_event = next(
            (event for event in budget_events if event.event_id == consumed.intent_event_id),
            None,
        )
        consumed_event = next(
            (event for event in budget_events if event.event_id == consumed.consumed_event_id),
            None,
        )
        reservation_event = next(
            (
                event for event in budget_events
                if event.event_type == "BudgetReserved"
                and event.payload.get("reservation_id") == consumed.reservation.reservation_id
            ),
            None,
        )
        if (
            intent_event is None
            or consumed_event is None
            or reservation_event is None
            or intent_event.event_type != "EffectIntentRecorded"
            or consumed_event.event_type != "ApprovalGrantConsumed"
            or intent_event.payload.get("approval_grant_id") != consumed.approval_grant_id
            or consumed_event.payload.get("approval_grant_id") != consumed.approval_grant_id
            or intent_event.payload.get("effect_id") != consumed.effect_id
            or consumed_event.payload.get("effect_id") != consumed.effect_id
            or any(
                event.run_id != request.run_id
                or event.node_id != request.node_id
                or event.attempt_id != request.attempt_id
                or event.fencing_generation != request.fencing_generation
                for event in (intent_event, consumed_event, reservation_event)
            )
        ):
            raise ToolAuditUnavailable("ApprovalService did not persist a matching attempt-bound intent")

        seen = False

        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            nonlocal seen
            if not self._attempt_is_current(request):
                raise _AuthorizationLost("attempt_not_current_in_approval_transaction")
            try:
                current = self._read_policy_state(request)
            except Exception as exc:
                raise _AuthorizationLost("policy_state_unavailable_in_approval_transaction") from exc
            if current != state:
                raise _AuthorizationLost("policy_state_changed_in_approval_transaction")
            scoped = [event for event in events if event.payload.get("request_id") == request.request_id]
            if any(
                event.event_type in {
                    "ToolApprovalConsumed", "ToolExecutionStarted", "ToolExecutionCompleted",
                    "ToolExecutionCancelled", "ToolExecutionFailed", "ToolExecutionOutcomeUnknown",
                }
                for event in scoped
            ):
                seen = True
                return None
            context = _event_context(request, consumed.consumed_event_id)
            return [
                EventDraft(
                    "ToolApprovalConsumed",
                    {
                        "request_id": request.request_id,
                        "approval_request_id": consumed.approval_request_id,
                        "approval_grant_id": consumed.approval_grant_id,
                        "effect_id": consumed.effect_id,
                        "effect_intent_event_id": consumed.intent_event_id,
                        "approval_consumed_event_id": consumed.consumed_event_id,
                        "budget_reservation_id": consumed.reservation.reservation_id,
                        "request_hash": request_hash,
                    },
                    **context,
                ),
                EventDraft(
                    "ToolExecutionStarted",
                    {
                        "request_id": request.request_id,
                        "request_hash": request_hash,
                        "policy_decision_hash": decision.decision_hash,
                        "approval_grant_id": consumed.approval_grant_id,
                        "effect_intent_event_id": consumed.intent_event_id,
                    },
                    **_event_context(request, consumed.intent_event_id),
                ),
            ]

        try:
            self._events.append_checked(
                "security", request.run_id, f"tool-approval-start:{request.request_id}", decide
            )
        except _AuthorizationLost as exc:
            self._record_block(request, exc.reason)
            raise PermissionError("approval-bound tool authority changed before start") from exc
        except Exception as exc:
            raise ToolAuditUnavailable("approval-bound tool start could not be durably recorded") from exc
        if seen:
            raise ToolRequestAlreadyUsed("approval-bound tool request has already started")

    def _record_approval_receipt(
        self,
        consumed: ConsumedApproval,
        attempt: ExecutionAttempt,
        *,
        outcome: Literal["applied", "not_applied"],
        receipt_hash: str,
    ) -> None:
        if self._approval_service is None:
            raise ToolAuditUnavailable("ApprovalService is unavailable for effect receipt")
        self._approval_service.record_effect_receipt(
            consumed,
            attempt,
            outcome=outcome,
            receipt_hash=receipt_hash,
        )

    def _consume_grant_and_start(
        self,
        request: ToolRequest,
        request_hash: str,
        decision: PolicyDecision,
        state: PolicyState,
    ) -> None:
        seen = False

        def decide(events: list[StoredEvent], version: int) -> Sequence[EventDraft] | None:
            nonlocal seen
            # The security append holds SQLite's write transaction. A Run
            # cancellation/revocation cannot commit between this check and
            # the capability consumption that follows.
            if not self._attempt_is_current(request):
                raise _AuthorizationLost("attempt_not_current_in_transaction")
            try:
                current = self._read_policy_state(request)
            except Exception as exc:
                raise _AuthorizationLost("policy_state_unavailable_in_transaction") from exc
            if current != state:
                raise _AuthorizationLost("policy_state_changed_in_transaction")
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
        except _AuthorizationLost as exc:
            self._record_block(request, exc.reason)
            raise PermissionError("tool authority changed before capability consumption") from exc
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


def _approval_request_id(request: ToolRequest) -> str:
    encoded = f"{request.run_id}\0{request.request_id}".encode("utf-8")
    return "tool-approval:" + hashlib.sha256(encoded).hexdigest()[:32]


def _hash_json(value: object) -> str:
    return _digest(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))


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
