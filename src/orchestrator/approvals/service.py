"""Fail-closed approval request, grant, binding, and one-shot consumption.

Approvals in this module authorize one predeclared effect only. The approved
scope is immutable; grant binding requires a *new* accepted attempt, and
consumption atomically writes EffectIntent, ApprovalGrantConsumed, and the
budget reservation through one shared SQLite event store.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.budget import BudgetLedger, BudgetReservation, CostEstimate
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.security.policy import ActionCategory


_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$"
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_TTL = timedelta(hours=24)


class _ApprovalModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class ApprovalRequest(_ApprovalModel):
    """Hash-only, bounded description of one proposed sensitive effect."""

    approval_request_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    requester_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    origin_attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    origin_fencing_generation: StrictInt = Field(ge=0)
    causation_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    action_category: ActionCategory
    tool_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    target_hash: StrictStr
    parameters_hash: StrictStr
    effect_intent_hash: StrictStr
    policy_manifest_hash: StrictStr
    revocation_version: StrictInt = Field(ge=0)
    emergency_deny_version: StrictInt = Field(ge=0)
    expires_at: datetime

    @field_validator(
        "target_hash", "parameters_hash", "effect_intent_hash", "policy_manifest_hash"
    )
    @classmethod
    def validate_hashes(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("approval scope requires sha256 content hashes")
        return value

    @field_validator("expires_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("approval expiry must be timezone-aware")
        return value.astimezone(timezone.utc)

    @property
    def content_hash(self) -> str:
        return _hash(self.model_dump(mode="json"))


class ApprovalPrincipal(_ApprovalModel):
    """Identity returned by the configured authentication capability."""

    principal_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    principal_type: Literal["human", "service"]
    permissions: frozenset[StrictStr] = Field(min_length=1)

    @field_validator("permissions", mode="before")
    @classmethod
    def freeze_permissions(cls, value: object) -> frozenset[str]:
        if not isinstance(value, (set, frozenset, tuple, list)):
            raise ValueError("permissions must be an array")
        return frozenset(value)


@dataclass(frozen=True)
class ApprovalPolicyState:
    revocation_version: int
    emergency_deny_version: int

    def __post_init__(self) -> None:
        for value in (self.revocation_version, self.emergency_deny_version):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("approval policy versions must be non-negative integers")


class ExecutionAttempt(_ApprovalModel):
    """Fresh, accepted attempt plus its measured isolation profile hash."""

    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    fencing_generation: StrictInt = Field(ge=0)
    isolation_profile_hash: StrictStr

    @field_validator("isolation_profile_hash")
    @classmethod
    def validate_profile_hash(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("isolation profile must be a sha256 content hash")
        return value


class EffectIntentSpec(_ApprovalModel):
    """Finite effect identity whose complete hash is approved in advance."""

    effect_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    target_hash: StrictStr
    parameters_hash: StrictStr
    provider_idempotency_key: StrictStr | None = None
    maximum_cost_minor: StrictInt = Field(ge=0)
    recovery_class: Literal["idempotent", "queryable", "manual_only"]

    @field_validator("target_hash", "parameters_hash")
    @classmethod
    def validate_hashes(cls, value: str) -> str:
        if not _HASH.fullmatch(value):
            raise ValueError("effect scope requires sha256 content hashes")
        return value

    @property
    def content_hash(self) -> str:
        return _hash(self.model_dump(mode="json"))

    @property
    def provider_idempotency_key_hash(self) -> str | None:
        if self.provider_idempotency_key is None:
            return None
        return _digest(self.provider_idempotency_key.encode("utf-8"))


class _Authority(Protocol):
    def is_current(self, attempt: ExecutionAttempt) -> bool: ...


@dataclass(frozen=True)
class IssuedApproval:
    approval_request_id: str
    approval_grant_id: str
    request_hash: str
    expires_at: datetime


@dataclass(frozen=True)
class ConsumedApproval:
    approval_request_id: str
    approval_grant_id: str
    effect_id: str
    intent_event_id: str
    consumed_event_id: str
    reservation: BudgetReservation


class ApprovalError(RuntimeError):
    """Base error for an invalid, unauthorized, or unavailable approval."""


class ApprovalExpired(ApprovalError):
    pass


class ApprovalInvalid(ApprovalError):
    pass


class ApprovalAlreadyConsumed(ApprovalError):
    pass


class ApprovalService:
    """Persist and atomically consume exact one-shot approvals.

    The EventStore and BudgetLedger must share the same `SQLiteEventStore`
    object, not merely the same database path. This permits grant validation,
    effect intent, grant consumption, and budget reservation to commit or roll
    back in one SQLite transaction.
    """

    def __init__(
        self,
        *,
        event_store: SQLiteEventStore,
        budget_ledger: BudgetLedger,
        authenticator: Callable[[str], ApprovalPrincipal | dict[str, Any]],
        attempt_authority: _Authority,
        policy_state: Callable[[ApprovalRequest], ApprovalPolicyState],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if getattr(budget_ledger, "_event_store", None) is not event_store:
            raise ValueError("ApprovalService and BudgetLedger must share one SQLiteEventStore instance")
        self._events = event_store
        self._budget = budget_ledger
        self._authenticator = authenticator
        self._attempt_authority = attempt_authority
        self._policy_state = policy_state
        self._now = now or (lambda: datetime.now(timezone.utc))

    def create_request(self, request: ApprovalRequest) -> str:
        """Durably add an exact request; request IDs are immutable/idempotent."""

        if not isinstance(request, ApprovalRequest):
            request = ApprovalRequest.model_validate(request)
        now = self._current_time()
        if request.expires_at <= now or request.expires_at > now + _MAX_TTL:
            raise ApprovalExpired("approval request expiry must be in the next 24 hours")
        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            existing = self._find_request_event(events, request.approval_request_id)
            if existing is not None:
                if existing.payload.get("request_hash") != request.content_hash:
                    raise ApprovalInvalid("approval request id was reused with different scope")
                return None
            for other_run_id in self._events.stream_ids("security"):
                if other_run_id == request.run_id:
                    continue
                other_events = self._events.read_stream("security", other_run_id)
                if self._find_request_event(other_events, request.approval_request_id) is not None:
                    raise ApprovalInvalid("approval request id was already used by another Run")
            return [
                EventDraft(
                    "ApprovalRequested",
                    {
                        "approval_request_id": request.approval_request_id,
                        "request_hash": request.content_hash,
                        "approval_request": request.model_dump(mode="json"),
                    },
                    **_request_context(request, request.causation_id),
                )
            ]

        self._append_checked(
            "security",
            request.run_id,
            f"approval-request:{request.approval_request_id}",
            decide,
            "approval request could not be durably recorded",
        )
        return request.content_hash

    def approve(self, approval_request_id: str, credential: str, *, reason: str) -> IssuedApproval:
        principal = self._authenticate(credential, "approval:approve")
        reason_hash = _digest(_bounded_reason(reason).encode("utf-8"))
        run_id, events = self._load_request_stream(approval_request_id)
        request_event = self._find_request_event(events, approval_request_id)
        if request_event is None:
            raise ApprovalInvalid("approval request does not exist")
        request = self._request_from_event(request_event)
        grant_id = _grant_id(approval_request_id, request.run_id)
        flags = {"expired": False, "revoked": False}

        def decide(current: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            current_request = self._find_request_event(current, approval_request_id)
            if current_request is None or current_request.payload.get("request_hash") != request.content_hash:
                raise ApprovalInvalid("approval request changed while being approved")
            if self._is_terminal(current, approval_request_id):
                raise ApprovalInvalid("approval request is already denied, revoked, or expired")
            existing_grant = self._find_grant_event(current, grant_id)
            if existing_grant is not None:
                return None
            decision_time = self._current_time()
            if request.expires_at <= decision_time:
                flags["expired"] = True
                return [self._status_draft(request, "ApprovalExpired", "expired_before_approval")]
            current_policy = self._state_for(request)
            if (
                current_policy.revocation_version != request.revocation_version
                or current_policy.emergency_deny_version != request.emergency_deny_version
            ):
                flags["revoked"] = True
                return [self._status_draft(request, "ApprovalRevoked", "policy_version_changed")]
            ctx = _request_context(request, current_request.event_id)
            grant_payload = {
                "approval_grant_id": grant_id,
                "approval_request_id": approval_request_id,
                "request_hash": request.content_hash,
                "scope": _request_scope(request),
                "approved_by": principal.principal_id,
                "reason_hash": reason_hash,
                "issued_at": decision_time.isoformat(),
                "expires_at": request.expires_at.isoformat(),
                "one_use": True,
            }
            return [
                EventDraft(
                    "ApprovalGranted",
                    {
                        "approval_request_id": approval_request_id,
                        "approval_grant_id": grant_id,
                        "approved_by": principal.principal_id,
                        "reason_hash": reason_hash,
                    },
                    **ctx,
                ),
                EventDraft("ApprovalGrant", grant_payload, **ctx),
            ]

        appended = self._append_checked(
            "security",
            run_id,
            f"approval-approve:{approval_request_id}",
            decide,
            "approval grant could not be durably recorded",
        )
        if flags["expired"]:
            raise ApprovalExpired("approval request expired before approval")
        if flags["revoked"]:
            raise ApprovalInvalid("approval policy versions changed before approval")
        grant = next(
            (event for event in appended if event.event_type == "ApprovalGrant" and event.payload.get("approval_grant_id") == grant_id),
            None,
        )
        if grant is None:
            grant = self._find_grant_event(self._events.read_stream("security", run_id), grant_id)
        if grant is None:
            raise ApprovalInvalid("approval grant is unavailable")
        return IssuedApproval(
            approval_request_id=approval_request_id,
            approval_grant_id=grant_id,
            request_hash=request.content_hash,
            expires_at=request.expires_at,
        )

    def deny(self, approval_request_id: str, credential: str, *, reason: str) -> None:
        principal = self._authenticate(credential, "approval:deny")
        reason_hash = _digest(_bounded_reason(reason).encode("utf-8"))
        run_id, _events = self._load_request_stream(approval_request_id)
        request_event = self._find_request_event(self._events.read_stream("security", run_id), approval_request_id)
        if request_event is None:
            raise ApprovalInvalid("approval request does not exist")
        request = self._request_from_event(request_event)

        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            if self._is_terminal(events, approval_request_id) or self._find_grant_event(
                events, _grant_id(approval_request_id, request.run_id)
            ):
                raise ApprovalInvalid("approval request already has a terminal decision")
            return [
                EventDraft(
                    "ApprovalDenied",
                    {
                        "approval_request_id": approval_request_id,
                        "denied_by": principal.principal_id,
                        "reason_hash": reason_hash,
                    },
                    **_request_context(request, request_event.event_id),
                )
            ]

        self._append_checked(
            "security", run_id, f"approval-deny:{approval_request_id}", decide,
            "approval denial could not be durably recorded",
        )

    def revoke(self, approval_grant_id: str, credential: str, *, reason: str) -> None:
        principal = self._authenticate(credential, "approval:revoke")
        reason_hash = _digest(_bounded_reason(reason).encode("utf-8"))
        run_id, request, grant = self._load_grant(approval_grant_id)
        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            if self._find_consumed_event(run_id, approval_grant_id) is not None:
                raise ApprovalAlreadyConsumed("a consumed approval cannot be revoked")
            if any(
                event.event_type == "ApprovalRevoked"
                and event.payload.get("approval_grant_id") == approval_grant_id
                for event in events
            ):
                return None
            bound = self._find_bound_event(events, approval_grant_id)
            context = (
                _stored_attempt_context(bound, bound.event_id)
                if bound is not None
                else _request_context(request, grant.event_id)
            )
            return [
                EventDraft(
                    "ApprovalRevoked",
                    {
                        "approval_request_id": request.approval_request_id,
                        "approval_grant_id": approval_grant_id,
                        "revoked_by": principal.principal_id,
                        "reason_hash": reason_hash,
                    },
                    **context,
                )
            ]

        self._append_checked(
            "security", run_id, f"approval-revoke:{approval_grant_id}", decide,
            "approval revocation could not be durably recorded",
        )

    def bind_to_attempt(self, approval_grant_id: str, attempt: ExecutionAttempt) -> str:
        """Bind an approved scope to a fresh accepted attempt exactly once."""

        run_id, request, grant = self._load_grant(approval_grant_id)
        if attempt.run_id != request.run_id or attempt.node_id != request.node_id:
            raise ApprovalInvalid("attempt is outside the approved Run/Node scope")
        if attempt.attempt_id == request.origin_attempt_id:
            raise ApprovalInvalid("approval cannot resume the originating attempt")
        if attempt.fencing_generation <= request.origin_fencing_generation:
            raise ApprovalInvalid("approved attempt must have a newer fencing generation")
        if not self._attempt_is_current(attempt):
            raise ApprovalInvalid("attempt is not current")
        flags = {"expired": False}
        try:
            if self._current_time() >= request.expires_at:
                self._record_expired(request, approval_grant_id)
                raise ApprovalExpired("approval grant expired before attempt binding")
            self._require_current_policy(request)
        except ApprovalInvalid:
            if self._policy_mismatch(request):
                self._record_revoked(request, approval_grant_id, "policy_version_changed")
            raise
        binding_hash = _hash(
            {
                "grant_id": approval_grant_id,
                "attempt": attempt.model_dump(mode="json"),
                "policy_manifest_hash": request.policy_manifest_hash,
            }
        )
        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            if self._is_revoked_or_expired(events, request.approval_request_id, approval_grant_id):
                raise ApprovalInvalid("approval is revoked, denied, or expired")
            if self._current_time() >= request.expires_at:
                flags["expired"] = True
                return [self._status_draft(request, "ApprovalExpired", "expired_before_attempt_binding")]
            if any(
                event.event_type == "ApprovalGrantConsumed"
                and event.payload.get("approval_grant_id") == approval_grant_id
                for event in self._events.read_stream("budget", run_id)
            ):
                raise ApprovalAlreadyConsumed("approval grant was already consumed")
            if self._find_bound_event(events, approval_grant_id) is not None:
                raise ApprovalInvalid("approval grant is already bound to an attempt")
            self._require_current_policy(request)
            if not self._attempt_is_current(attempt):
                raise ApprovalInvalid("attempt authority changed before grant binding")
            return [
                EventDraft(
                    "ApprovalGrantBound",
                    {
                        "approval_request_id": request.approval_request_id,
                        "approval_grant_id": approval_grant_id,
                        "binding_hash": binding_hash,
                        "attempt_id": attempt.attempt_id,
                        "fencing_generation": attempt.fencing_generation,
                        "isolation_profile_hash": attempt.isolation_profile_hash,
                        "policy_manifest_hash": request.policy_manifest_hash,
                        "revocation_version": request.revocation_version,
                        "emergency_deny_version": request.emergency_deny_version,
                    },
                    **_attempt_context(attempt, grant.event_id),
                )
            ]

        try:
            stored = self._append_checked(
                "security", run_id, f"approval-bind:{approval_grant_id}", decide,
                "approval grant could not be bound to attempt",
            )
        except ApprovalInvalid:
            if self._policy_mismatch(request):
                self._record_revoked(request, approval_grant_id, "policy_version_changed")
            raise
        if flags["expired"]:
            raise ApprovalExpired("approval grant expired before attempt binding")
        bound = next((event for event in stored if event.event_type == "ApprovalGrantBound"), None)
        if bound is None:
            raise ApprovalInvalid("approval binding was not persisted")
        return bound.event_id

    def consume_and_intend(
        self,
        approval_grant_id: str,
        attempt: ExecutionAttempt,
        intent: EffectIntentSpec,
        estimate: CostEstimate,
        *,
        token_limit: int | None = None,
    ) -> ConsumedApproval:
        """Consume a grant with its effect intent and budget reserve atomically."""

        if not self._attempt_is_current(attempt):
            raise ApprovalInvalid("attempt is not current")
        run_id, request, _grant = self._load_grant(approval_grant_id)
        if estimate.amount_minor > intent.maximum_cost_minor:
            raise ApprovalInvalid("effect estimate exceeds the approved maximum cost")
        if intent.content_hash != request.effect_intent_hash:
            raise ApprovalInvalid("effect parameters differ from the approved intent")
        if intent.target_hash != request.target_hash or intent.parameters_hash != request.parameters_hash:
            raise ApprovalInvalid("effect target or parameters differ from the approved scope")
        try:
            policy = self._require_current_policy(request)
        except ApprovalInvalid:
            if self._policy_mismatch(request):
                self._record_revoked(request, approval_grant_id, "policy_version_changed")
            raise
        if self._current_time() >= request.expires_at:
            self._record_expired(request, approval_grant_id)
            raise ApprovalExpired("approval grant expired before consumption")
        bound_event = self._find_bound_event(self._events.read_stream("security", run_id), approval_grant_id)
        if bound_event is None:
            raise ApprovalInvalid("approval grant has not been bound to an accepted attempt")
        factory_called = False

        def approval_events() -> Iterable[EventDraft]:
            nonlocal factory_called
            factory_called = True
            events = self._events.read_stream("security", run_id)
            request_event = self._find_request_event(events, request.approval_request_id)
            grant = self._find_grant_event(events, approval_grant_id)
            bound = self._find_bound_event(events, approval_grant_id)
            if request_event is None or grant is None or bound is None:
                raise ApprovalInvalid("approval request, grant, or attempt binding is missing")
            if self._is_revoked_or_expired(events, request.approval_request_id, approval_grant_id):
                raise ApprovalInvalid("approval was denied, revoked, or expired")
            if self._find_consumed_event(run_id, approval_grant_id) is not None:
                raise ApprovalAlreadyConsumed("approval grant was already consumed")
            if not self._attempt_is_current(attempt):
                raise ApprovalInvalid("attempt authority changed before grant consumption")
            if self._current_time() >= request.expires_at:
                raise ApprovalExpired("approval request has expired")
            if self._state_for(request) != policy:
                raise ApprovalInvalid("approval policy changed before grant consumption")
            bound_scope = bound.payload
            if (
                bound.run_id != attempt.run_id
                or bound.node_id != attempt.node_id
                or bound.attempt_id != attempt.attempt_id
                or bound.fencing_generation != attempt.fencing_generation
                or bound_scope.get("isolation_profile_hash") != attempt.isolation_profile_hash
                or request.run_id != attempt.run_id
                or request.node_id != attempt.node_id
                or request.action_category != self._scope_from_grant(grant).get("action_category")
            ):
                raise ApprovalInvalid("execution attempt does not match the bound grant")
            if intent.effect_id in {
                event.payload.get("effect_id")
                for event in self._events.read_stream("budget", run_id)
                if event.event_type == "EffectIntentRecorded"
            }:
                raise ApprovalAlreadyConsumed("effect intent already exists")
            context = _attempt_context(attempt, bound.event_id)
            intent_payload = {
                "effect_id": intent.effect_id,
                "approval_grant_id": approval_grant_id,
                "effect_intent_hash": intent.content_hash,
                "request_hash": intent.parameters_hash,
                "target_hash": intent.target_hash,
                "provider_idempotency_key_hash": intent.provider_idempotency_key_hash,
                "maximum_cost_minor": intent.maximum_cost_minor,
                "recovery_class": intent.recovery_class,
            }
            return [
                EventDraft("EffectIntentRecorded", intent_payload, **context),
                EventDraft(
                    "ApprovalGrantConsumed",
                    {
                        "approval_grant_id": approval_grant_id,
                        "approval_request_id": request.approval_request_id,
                        "effect_id": intent.effect_id,
                        "effect_intent_hash": intent.content_hash,
                        "binding_hash": bound_scope["binding_hash"],
                    },
                    **context,
                ),
            ]

        try:
            reservation = self._budget.reserve(
                run_id,
                estimate,
                token_limit=token_limit,
                idempotency_key=f"approval:{approval_grant_id}",
                node_id=attempt.node_id,
                attempt_id=attempt.attempt_id,
                fencing_generation=attempt.fencing_generation,
                causation_id=bound_event.event_id,
                approval_grant_id=approval_grant_id,
                approval_event_factory=approval_events,
            )
        except ApprovalExpired:
            self._record_expired(request, approval_grant_id)
            raise
        except ApprovalInvalid:
            if self._policy_mismatch(request):
                self._record_revoked(request, approval_grant_id, "policy_version_changed")
            raise
        if not factory_called:
            raise ApprovalAlreadyConsumed("grant consumption request was replayed")
        committed = self._budget.read(run_id)
        consumed = next(
            event
            for event in committed
            if event.event_type == "ApprovalGrantConsumed"
            and event.payload.get("approval_grant_id") == approval_grant_id
        )
        intent_event = next(
            event
            for event in committed
            if event.event_type == "EffectIntentRecorded"
            and event.payload.get("approval_grant_id") == approval_grant_id
            and event.payload.get("effect_id") == intent.effect_id
        )
        return ConsumedApproval(
            approval_request_id=request.approval_request_id,
            approval_grant_id=approval_grant_id,
            effect_id=intent.effect_id,
            intent_event_id=intent_event.event_id,
            consumed_event_id=consumed.event_id,
            reservation=reservation,
        )

    def record_effect_receipt(
        self,
        consumed: ConsumedApproval,
        attempt: ExecutionAttempt,
        *,
        outcome: Literal["applied", "not_applied"],
        receipt_hash: str,
    ) -> str:
        if not _HASH.fullmatch(receipt_hash):
            raise ValueError("receipt_hash must be a sha256 content hash")
        if outcome not in {"applied", "not_applied"}:
            raise ValueError("effect receipt outcome must be applied or not_applied")
        run_id = attempt.run_id
        if (
            not isinstance(consumed, ConsumedApproval)
            or not consumed.approval_grant_id
            or not consumed.effect_id
        ):
            raise ApprovalInvalid("invalid consumed approval handle")

        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            intent = next(
                (
                    event for event in events
                    if event.event_type == "EffectIntentRecorded"
                    and event.payload.get("effect_id") == consumed.effect_id
                    and event.payload.get("approval_grant_id") == consumed.approval_grant_id
                ),
                None,
            )
            if intent is None or intent.attempt_id != attempt.attempt_id or intent.fencing_generation != attempt.fencing_generation:
                raise ApprovalInvalid("effect receipt does not match the consumed intent")
            receipts = [
                event for event in events
                if event.event_type == "EffectReceiptRecorded"
                and event.payload.get("effect_id") == consumed.effect_id
            ]
            if receipts:
                if receipts[-1].payload.get("receipt_hash") != receipt_hash or receipts[-1].payload.get("outcome") != outcome:
                    raise ApprovalInvalid("effect receipt was replayed with different result")
                return None
            return [
                EventDraft(
                    "EffectReceiptRecorded",
                    {
                        "effect_id": consumed.effect_id,
                        "approval_grant_id": consumed.approval_grant_id,
                        "outcome": outcome,
                        "receipt_hash": receipt_hash,
                    },
                    **_attempt_context(attempt, intent.event_id),
                )
            ]

        appended = self._append_checked(
            "budget", run_id, f"effect-receipt:{consumed.effect_id}", decide,
            "effect receipt could not be durably recorded",
        )
        event = next(
            (item for item in appended if item.event_type == "EffectReceiptRecorded"),
            None,
        )
        if event is None:
            event = next(
                item for item in self._events.read_stream("budget", run_id)
                if item.event_type == "EffectReceiptRecorded" and item.payload.get("effect_id") == consumed.effect_id
            )
        return event.event_id

    def _authenticate(self, credential: str, permission: str) -> ApprovalPrincipal:
        if not isinstance(credential, str) or not credential:
            raise ApprovalInvalid("authenticated approval authority is required")
        try:
            principal = self._authenticator(credential)
            if not isinstance(principal, ApprovalPrincipal):
                principal = ApprovalPrincipal.model_validate(principal)
        except Exception as exc:
            raise ApprovalInvalid("approver authentication failed") from exc
        if principal.principal_type not in {"human", "service"} or permission not in principal.permissions:
            raise ApprovalInvalid("principal lacks the required approval authority")
        return principal

    def _current_time(self) -> datetime:
        value = self._now()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ApprovalInvalid("approval clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc)

    def _state_for(self, request: ApprovalRequest) -> ApprovalPolicyState:
        state = self._policy_state(request)
        if not isinstance(state, ApprovalPolicyState):
            raise ApprovalInvalid("policy-state provider returned an invalid value")
        return state

    def _require_current_policy(self, request: ApprovalRequest) -> ApprovalPolicyState:
        state = self._state_for(request)
        if (
            state.revocation_version != request.revocation_version
            or state.emergency_deny_version != request.emergency_deny_version
        ):
            raise ApprovalInvalid("approval policy versions are stale")
        return state

    def _policy_mismatch(self, request: ApprovalRequest) -> bool:
        try:
            state = self._state_for(request)
        except ApprovalError:
            return True
        return (
            state.revocation_version != request.revocation_version
            or state.emergency_deny_version != request.emergency_deny_version
        )

    def _attempt_is_current(self, attempt: ExecutionAttempt) -> bool:
        try:
            return self._attempt_authority.is_current(attempt) is True
        except Exception:
            return False

    def _append_checked(
        self,
        stream_type: str,
        stream_id: str,
        key: str,
        decide: Callable[[list[StoredEvent], int], Iterable[EventDraft] | None],
        message: str,
    ) -> list[StoredEvent]:
        try:
            return self._events.append_checked(stream_type, stream_id, key, decide)
        except ApprovalError:
            raise
        except Exception as exc:
            raise ApprovalError(message) from exc

    def _load_request_stream(self, approval_request_id: str) -> tuple[str, list[StoredEvent]]:
        if not isinstance(approval_request_id, str) or re.fullmatch(_IDENTIFIER, approval_request_id) is None:
            raise ApprovalInvalid("approval request id is invalid")
        matches: list[tuple[str, list[StoredEvent]]] = []
        for run_id in self._events.stream_ids("security"):
            events = self._events.read_stream("security", run_id)
            if self._find_request_event(events, approval_request_id) is not None:
                matches.append((run_id, events))
        if not matches:
            raise ApprovalInvalid("approval request does not exist")
        if len(matches) != 1:
            raise ApprovalInvalid("approval request id is ambiguous across Runs")
        return matches[0]

    def _record_expired(self, request: ApprovalRequest, grant_id: str) -> None:
        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            if any(
                event.event_type == "ApprovalExpired"
                and event.payload.get("approval_request_id") == request.approval_request_id
                for event in events
            ):
                return None
            grant = self._find_grant_event(events, grant_id)
            return [
                EventDraft(
                    "ApprovalExpired",
                    {
                        "approval_request_id": request.approval_request_id,
                        "approval_grant_id": grant_id,
                        "reason_code": "expired_before_execution",
                    },
                    **_request_context(request, request.causation_id if grant is None else grant.event_id),
                )
            ]

        self._append_checked(
            "security", request.run_id, f"approval-expire:{grant_id}", decide,
            "approval expiration could not be durably recorded",
        )

    def _record_revoked(self, request: ApprovalRequest, grant_id: str, reason: str) -> None:
        def decide(events: list[StoredEvent], _version: int) -> Sequence[EventDraft] | None:
            if any(
                event.event_type == "ApprovalRevoked"
                and event.payload.get("approval_grant_id") == grant_id
                for event in events
            ):
                return None
            return [
                EventDraft(
                    "ApprovalRevoked",
                    {
                        "approval_request_id": request.approval_request_id,
                        "approval_grant_id": grant_id,
                        "reason_code": reason,
                    },
                    **_request_context(request, request.causation_id),
                )
            ]

        self._append_checked(
            "security", request.run_id, f"approval-revoke-stale:{grant_id}", decide,
            "stale approval could not be revoked",
        )

    def _load_grant(self, approval_grant_id: str) -> tuple[str, ApprovalRequest, StoredEvent]:
        if not isinstance(approval_grant_id, str) or re.fullmatch(_IDENTIFIER, approval_grant_id) is None:
            raise ApprovalInvalid("approval grant id is invalid")
        # Grant IDs are deterministic but deliberately opaque. A full scan is
        # bounded to per-Run security streams provided by the EventStore.
        for run_id in self._events.stream_ids("security"):
            events = self._events.read_stream("security", run_id)
            grant = self._find_grant_event(events, approval_grant_id)
            if grant is not None:
                request_event = self._find_request_event(events, grant.payload.get("approval_request_id"))
                if request_event is None:
                    break
                return run_id, self._request_from_event(request_event), grant
        raise ApprovalInvalid("approval grant does not exist")

    @staticmethod
    def _find_request_event(events: Sequence[StoredEvent], request_id: str) -> StoredEvent | None:
        return next(
            (
                event for event in events
                if event.event_type == "ApprovalRequested"
                and event.payload.get("approval_request_id") == request_id
                and isinstance(event.payload.get("approval_request"), dict)
            ),
            None,
        )

    @staticmethod
    def _request_from_event(event: StoredEvent) -> ApprovalRequest:
        return ApprovalRequest.model_validate_json(
            json.dumps(event.payload["approval_request"], ensure_ascii=False, sort_keys=True)
        )

    @staticmethod
    def _find_grant_event(events: Sequence[StoredEvent], grant_id: str) -> StoredEvent | None:
        return next(
            (event for event in events if event.event_type == "ApprovalGrant" and event.payload.get("approval_grant_id") == grant_id),
            None,
        )

    @staticmethod
    def _find_bound_event(events: Sequence[StoredEvent], grant_id: str) -> StoredEvent | None:
        return next(
            (event for event in events if event.event_type == "ApprovalGrantBound" and event.payload.get("approval_grant_id") == grant_id),
            None,
        )

    @staticmethod
    def _is_terminal(events: Sequence[StoredEvent], request_id: str) -> bool:
        return any(
            event.event_type in {"ApprovalDenied", "ApprovalRevoked", "ApprovalExpired"}
            and event.payload.get("approval_request_id") == request_id
            for event in events
        )

    @staticmethod
    def _is_revoked_or_expired(events: Sequence[StoredEvent], request_id: str, grant_id: str) -> bool:
        return any(
            event.event_type in {"ApprovalDenied", "ApprovalRevoked", "ApprovalExpired"}
            and (
                event.payload.get("approval_request_id") == request_id
                or event.payload.get("approval_grant_id") == grant_id
            )
            for event in events
        )

    def _find_consumed_event(self, run_id: str, grant_id: str) -> StoredEvent | None:
        return next(
            (
                event for event in self._events.read_stream("budget", run_id)
                if event.event_type == "ApprovalGrantConsumed"
                and event.payload.get("approval_grant_id") == grant_id
            ),
            None,
        )

    @staticmethod
    def _scope_from_grant(grant: StoredEvent) -> dict[str, Any]:
        value = grant.payload.get("scope")
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _status_draft(request: ApprovalRequest, event_type: str, reason: str) -> EventDraft:
        return EventDraft(
            event_type,
            {
                "approval_request_id": request.approval_request_id,
                "reason_code": reason,
            },
            **_request_context(request, request.causation_id),
        )


def _request_scope(request: ApprovalRequest) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "node_id": request.node_id,
        "origin_attempt_id": request.origin_attempt_id,
        "origin_fencing_generation": request.origin_fencing_generation,
        "action_category": request.action_category,
        "tool_id": request.tool_id,
        "target_hash": request.target_hash,
        "parameters_hash": request.parameters_hash,
        "effect_intent_hash": request.effect_intent_hash,
        "policy_manifest_hash": request.policy_manifest_hash,
        "revocation_version": request.revocation_version,
        "emergency_deny_version": request.emergency_deny_version,
    }


def _request_context(request: ApprovalRequest, causation_id: str) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "node_id": request.node_id,
        "attempt_id": request.origin_attempt_id,
        "fencing_generation": request.origin_fencing_generation,
        "causation_id": causation_id,
    }


def _attempt_context(attempt: ExecutionAttempt, causation_id: str) -> dict[str, Any]:
    return {
        "run_id": attempt.run_id,
        "node_id": attempt.node_id,
        "attempt_id": attempt.attempt_id,
        "fencing_generation": attempt.fencing_generation,
        "causation_id": causation_id,
    }


def _grant_id(request_id: str, run_id: str) -> str:
    return "approval-grant:" + hashlib.sha256(f"{run_id}\0{request_id}".encode()).hexdigest()[:32]


def _hash(value: object) -> str:
    return _digest(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _bounded_reason(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ApprovalInvalid("approval decision requires a bounded non-blank reason")
    if any(ord(character) < 0x20 and character not in "\t\n" for character in value):
        raise ApprovalInvalid("approval reason contains unsupported control characters")
    return value.strip()


def _stored_attempt_context(event: StoredEvent, causation_id: str) -> dict[str, Any]:
    return {
        "run_id": event.run_id,
        "node_id": event.node_id,
        "attempt_id": event.attempt_id,
        "fencing_generation": event.fencing_generation,
        "causation_id": causation_id,
    }


__all__ = [
    "ApprovalAlreadyConsumed",
    "ApprovalError",
    "ApprovalExpired",
    "ApprovalInvalid",
    "ApprovalPolicyState",
    "ApprovalPrincipal",
    "ApprovalRequest",
    "ApprovalService",
    "ConsumedApproval",
    "EffectIntentSpec",
    "ExecutionAttempt",
    "IssuedApproval",
]
