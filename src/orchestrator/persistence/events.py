"""Validated event boundary objects."""

from __future__ import annotations

from collections.abc import Mapping
import copy
from datetime import datetime
import math
import re
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationInfo,
    field_validator,
    model_validator,
)


def _validate_json_value(value: Any, *, path: str = "payload") -> None:
    """Reject values that cannot be represented deterministically as JSON."""

    if isinstance(value, str):
        if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
            raise ValueError(f"{path} contains a lone surrogate character")
        return
    if value is None or isinstance(value, (bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} has a non-string key")
            _validate_json_value(item, path=f"{path}.{key}")
        return
    raise ValueError(f"{path} contains a non-JSON value of type {type(value).__name__}")


def _validate_non_blank(value: Any, field_name: str) -> Any:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    return value


_SHA256_HEX = re.compile(r"[0-9a-f]{64}", re.ASCII)

_CAUSAL_EVENT_TYPES = frozenset(
    {
        "PolicyDecision",
        "CapabilityGrant",
        "ApprovalRequested",
        "ApprovalGrant",
        "ApprovalGranted",
        "ApprovalGrantConsumed",
        "ApprovalDenied",
        "ApprovalRevoked",
        "ApprovalExpired",
        "RoutingRequest",
        "RoutingDecision",
        "RoutingDecisionAccepted",
        "AttemptAccepted",
        "AttemptCompleted",
        "AttemptReconciled",
        "AttemptCancelled",
        "AttemptOutcomeUnknown",
        "AttemptSlotReleased",
        "RunAwaitingUser",
        "RunUserResponseReceived",
        "RunCancellationRequested",
        "RunCancelled",
        "AgentInstanceCreated",
        "AgentStarted",
        "AgentCompleted",
        "AgentFailed",
        "AgentOutcomeUnknown",
        "AgentReconciled",
        "AgentCancelled",
        "EffectIntentRecorded",
        "EffectReceiptRecorded",
    }
)
_RUN_LEVEL_CAUSAL_EVENT_TYPES = frozenset(
    {
        "RunAwaitingUser",
        "RunUserResponseReceived",
        "RunCancellationRequested",
        "RunCancelled",
    }
)
_BUDGET_EVENT_TYPES = frozenset(
    {
        "BudgetReserved",
        "UsageObserved",
        "CostCommitted",
        "BudgetReleased",
        "CostAdjusted",
        "OutcomeUnknown",
        "AwaitingReconciliation",
    }
)


class EventContractError(ValueError):
    """Raised when lifecycle, security, and budget event identities disagree."""


def validate_event_contract(events: list[Any]) -> None:
    """Validate ordering and identity rules for a single causal event stream.

    Budget ledgers can also be used as a stand-alone accounting primitive.
    Their legacy events remain valid without attempt metadata; once a budget
    event is attempt-scoped, all execution identities and causation are
    mandatory. Security, routing, approval, and external-effect events always
    require that full context.
    """

    intents: dict[str, Any] = {}
    receipts: set[str] = set()
    consumed_grants: dict[str, Any] = {}
    for event in events:
        event_type = event.event_type
        payload = event.payload or {}
        if event_type == "EffectIntentRecorded":
            effect_id = payload.get("effect_id")
            if not isinstance(effect_id, str) or not effect_id.strip():
                raise EventContractError("EffectIntentRecorded requires effect_id")
            if effect_id in intents:
                raise EventContractError("EffectIntentRecorded cannot duplicate effect_id")
            intents[effect_id] = event
        elif event_type == "EffectReceiptRecorded":
            effect_id = payload.get("effect_id")
            intent = intents.get(effect_id)
            if intent is None:
                raise EventContractError(
                    "EffectReceiptRecorded requires a prior EffectIntentRecorded in the same stream"
                )
            if (
                intent.attempt_id != event.attempt_id
                or intent.fencing_generation != event.fencing_generation
            ):
                raise EventContractError(
                    "effect receipt attempt and fencing generation must match its intent"
                )
            if effect_id in receipts:
                raise EventContractError("EffectReceiptRecorded cannot duplicate effect_id")
            receipts.add(effect_id)
        elif event_type == "ApprovalGrantConsumed":
            grant_id = payload.get("approval_grant_id") or payload.get("grant_id")
            if not isinstance(grant_id, str) or not grant_id.strip():
                raise EventContractError("ApprovalGrantConsumed requires approval_grant_id")
            if grant_id in consumed_grants:
                raise EventContractError("an ApprovalGrant can be consumed only once")
            consumed_grants[grant_id] = event
        elif event_type == "BudgetReserved":
            grant_id = payload.get("approval_grant_id")
            if grant_id is not None:
                consumed = consumed_grants.get(grant_id)
                if consumed is None:
                    raise EventContractError(
                        "approval-gated BudgetReserved requires prior ApprovalGrantConsumed"
                    )
                if consumed.attempt_id != event.attempt_id:
                    raise EventContractError(
                        "ApprovalGrantConsumed and BudgetReserved must share attempt_id"
                    )
                if consumed.fencing_generation != event.fencing_generation:
                    raise EventContractError(
                        "ApprovalGrantConsumed and BudgetReserved must share fencing_generation"
                    )
    # A consumed approval must be paired with the budget reservation it
    # authorizes, or with the external-effect intent it authorizes. The latter
    # is recorded before consumption as required by the effect protocol.
    for grant_id, consumed in consumed_grants.items():
        matching_reservations = [
            event
            for event in events
            if event.event_type == "BudgetReserved"
            and event.payload.get("approval_grant_id") == grant_id
        ]
        matching_intents = [
            event
            for event in events
            if event.event_type == "EffectIntentRecorded"
            and event.payload.get("approval_grant_id") == grant_id
        ]
        if not matching_reservations and not matching_intents:
            raise EventContractError(
                "ApprovalGrantConsumed must be paired with an approval-gated BudgetReserved or EffectIntentRecorded"
            )
        if matching_intents:
            intent = next(
                (event for event in matching_intents if event.stream_version < consumed.stream_version),
                None,
            )
            if intent is None:
                raise EventContractError("approval-gated EffectIntentRecorded must precede grant consumption")
            if (
                intent.attempt_id != consumed.attempt_id
                or intent.fencing_generation != consumed.fencing_generation
            ):
                raise EventContractError(
                    "ApprovalGrantConsumed and EffectIntentRecorded must share attempt_id and fencing_generation"
                )
    for effect_id, intent in intents.items():
        grant_id = intent.payload.get("approval_grant_id")
        if grant_id is None:
            continue
        consumed = consumed_grants.get(grant_id)
        if (
            consumed is None
            or consumed.stream_version <= intent.stream_version
            or consumed.payload.get("effect_id") != effect_id
        ):
            raise EventContractError(
                "approval-gated EffectIntentRecorded requires a later matching ApprovalGrantConsumed"
            )
        if (
            consumed.attempt_id != intent.attempt_id
            or consumed.fencing_generation != intent.fencing_generation
        ):
            raise EventContractError(
                "ApprovalGrantConsumed and EffectIntentRecorded must share attempt_id and fencing_generation"
            )


def _validate_execution_context(model: Any, *, event_type: str) -> None:
    if event_type in _RUN_LEVEL_CAUSAL_EVENT_TYPES:
        required = ("run_id", "correlation_id", "causation_id")
        missing = [name for name in required if getattr(model, name, None) is None]
        if missing:
            raise ValueError(
                f"{event_type} requires run-level causal context; missing {', '.join(missing)}"
            )
        return
    required = ("run_id", "node_id", "attempt_id", "fencing_generation", "causation_id")
    context_present = any(getattr(model, name, None) is not None for name in required)
    required_for_type = event_type in _CAUSAL_EVENT_TYPES
    if event_type in _BUDGET_EVENT_TYPES and not context_present:
        return
    if not required_for_type and not context_present:
        return
    missing = [name for name in required if getattr(model, name, None) is None]
    if missing:
        raise ValueError(
            f"{event_type} requires complete execution context; missing {', '.join(missing)}"
        )


def _validate_sha256_hex(value: Any, field_name: str = "payload_hash") -> str:
    if not isinstance(value, str) or _SHA256_HEX.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be 64 lowercase ASCII hex characters")
    return value


class _FrozenDict(dict[str, Any]):
    """A dict-compatible JSON object that rejects all mutation methods."""

    @staticmethod
    def _immutable(*args: Any, **kwargs: Any) -> None:
        raise TypeError("event payload is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable

    def __copy__(self) -> "_FrozenDict":
        return type(self)(self.items())

    def __deepcopy__(self, memo: dict[int, Any]) -> "_FrozenDict":
        existing = memo.get(id(self))
        if existing is not None:
            return existing
        copied = type(self)()
        memo[id(self)] = copied
        for key, value in self.items():
            dict.__setitem__(
                copied,
                copy.deepcopy(key, memo),
                copy.deepcopy(value, memo),
            )
        return copied


class _FrozenList(list[Any]):
    """A list-compatible JSON array that rejects all mutation methods."""

    @staticmethod
    def _immutable(*args: Any, **kwargs: Any) -> None:
        raise TypeError("event payload is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable

    def __copy__(self) -> "_FrozenList":
        return type(self)(self)

    def __deepcopy__(self, memo: dict[int, Any]) -> "_FrozenList":
        existing = memo.get(id(self))
        if existing is not None:
            return existing
        copied = type(self)()
        memo[id(self)] = copied
        for item in self:
            list.append(copied, copy.deepcopy(item, memo))
        return copied


def _freeze_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict((key, _freeze_json_value(item)) for key, item in value.items())
    if isinstance(value, list):
        return _FrozenList(_freeze_json_value(item) for item in value)
    return value


class EventDraft(BaseModel):
    """An event before its durable stream identity is assigned."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: StrictStr = Field(min_length=1)
    payload: dict[str, Any]
    run_id: StrictStr | None = None
    node_id: StrictStr | None = None
    attempt_id: StrictStr | None = None
    fencing_generation: StrictInt | None = Field(default=None, ge=0)
    correlation_id: StrictStr | None = None
    causation_id: StrictStr | None = None

    def __init__(self, *args: Any, **data: Any) -> None:
        # The small positional form keeps the append API pleasant while the
        # underlying Pydantic validation remains the single source of truth.
        if args:
            if len(args) > 2:
                raise TypeError("EventDraft accepts at most event_type and payload positionally")
            for field_name, value in zip(("event_type", "payload"), args):
                if field_name in data:
                    raise TypeError(f"{field_name} was provided both positionally and by keyword")
                data[field_name] = value
        super().__init__(**data)

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_event_type(cls, value: Any) -> Any:
        return _validate_non_blank(value, "event_type")

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            raise ValueError("payload must be a mapping")
        payload = dict(value)
        _validate_json_value(payload)
        return payload

    @field_validator(
        "run_id", "node_id", "attempt_id", "correlation_id", "causation_id", mode="before"
    )
    @classmethod
    def validate_optional_identifiers(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return _validate_non_blank(value, info.field_name)

    @model_validator(mode="after")
    def validate_execution_context(self) -> "EventDraft":
        _validate_execution_context(self, event_type=self.event_type)
        return self


class StoredEvent(BaseModel):
    """An immutable event read from the append-only event store."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: StrictStr = Field(min_length=1)
    stream_type: StrictStr = Field(min_length=1)
    stream_id: StrictStr = Field(min_length=1)
    stream_version: StrictInt = Field(gt=0)
    event_type: StrictStr = Field(min_length=1)
    schema_version: StrictInt = Field(gt=0)
    occurred_at: datetime
    payload: dict[str, Any]
    payload_hash: StrictStr = Field(min_length=64, max_length=64)
    idempotency_key: StrictStr = Field(min_length=1)
    run_id: StrictStr | None = None
    node_id: StrictStr | None = None
    attempt_id: StrictStr | None = None
    fencing_generation: StrictInt | None = Field(default=None, ge=0)
    correlation_id: StrictStr | None = None
    causation_id: StrictStr | None = None

    @field_validator("event_id", "stream_type", "stream_id", "idempotency_key", mode="before")
    @classmethod
    def validate_identifiers(cls, value: Any, info: ValidationInfo) -> Any:
        return _validate_non_blank(value, info.field_name)

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_event_type(cls, value: Any) -> Any:
        return _validate_non_blank(value, "event_type")

    @field_validator("payload_hash", mode="before")
    @classmethod
    def validate_payload_hash(cls, value: Any) -> Any:
        return _validate_sha256_hex(value)

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            raise ValueError("payload must be a mapping")
        payload = dict(value)
        _validate_json_value(payload)
        return payload

    @field_validator("correlation_id", "causation_id", mode="before")
    @classmethod
    def validate_optional_identifiers(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return value
        return _validate_non_blank(value, info.field_name)

    @model_validator(mode="after")
    def validate_execution_context(self) -> "StoredEvent":
        _validate_execution_context(self, event_type=self.event_type)
        return self

    @model_validator(mode="after")
    def freeze_payload(self) -> "StoredEvent":
        # This also detaches the stored event from any mutable EventDraft
        # payload object supplied to append().
        object.__setattr__(self, "payload", _freeze_json_value(self.payload))
        return self
