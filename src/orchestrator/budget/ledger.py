"""Event-sourced reservations and cost settlement.

The ledger intentionally has no process-global cache.  Every decision is
made from the budget stream and then committed with EventStore's stream CAS
and idempotency key.  A process restart therefore has exactly the same view
as a long-lived process.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
import hashlib
import json
from typing import Any, Protocol

from orchestrator.identifiers import new_id
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.persistence.sqlite_event_store import (
    IdempotencyConflict,
    SQLiteEventStore,
    StaleStream,
)

from .models import BudgetBalance, BudgetReservation, CostEstimate, RunLimit, UsageRecord


class BudgetError(RuntimeError):
    """Base class for deterministic budget failures."""


class BudgetExhausted(BudgetError):
    """A reservation would cross the Run's cost or token envelope."""


class BudgetLimitMismatch(BudgetError):
    """A reopened ledger was given an envelope different from persisted facts."""


class ReservationNotFound(BudgetError):
    """A reservation ID is not present in the durable budget stream."""


class ReservationStateError(BudgetError):
    """An operation is not valid for the reservation's current state."""


class BudgetReleasedError(ReservationStateError):
    """An already released or unresolved reservation cannot be released."""


class CurrencyMismatch(ValueError):
    """A money-bearing value uses a different currency from its Run."""

    def __init__(self, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(f"currency mismatch: expected {expected}, got {actual}")


class ReservedKeyError(ValueError):
    """A caller key entered a namespace reserved for ledger operations."""


class _EventStore(Protocol):
    def append(
        self,
        stream_type: str,
        stream_id: str,
        expected_version: int,
        events: Iterable[EventDraft],
        idempotency_key: str,
    ) -> list[StoredEvent]: ...

    def read_stream(
        self, stream_type: str, stream_id: str, after_version: int = 0
    ) -> list[StoredEvent]: ...

    def current_version(self, stream_type: str, stream_id: str) -> int: ...

    def append_checked(
        self,
        stream_type: str,
        stream_id: str,
        idempotency_key: str,
        decide: Callable[[list[StoredEvent], int], Iterable[EventDraft] | None],
    ) -> list[StoredEvent]: ...


_MILLION = 1_000_000
_BUDGET_STREAM = "budget"
_INTERNAL_KEY_PREFIXES = (
    "reserve:",
    "unknown:",
    "settle:",
    "release:",
    "reconcile:",
    # Kept reserved for databases written by an earlier hardening revision.
    "__budget_release__:",
    "__budget_reserve__:",
    "__budget_unknown__:",
    "__budget_settle__:",
    "__budget_reconcile__:",
)


@dataclass
class _ReservationState:
    reservation: BudgetReservation
    estimate: dict[str, Any]
    cost_minor: int = 0
    used_tokens: int = 0
    release_minor: int = 0
    release_tokens: int = 0
    usage: UsageRecord | None = None


def _identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    return value


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _ceil_million(numerator: int) -> int:
    return (numerator + _MILLION - 1) // _MILLION


def _caller_key(value: Any, field_name: str) -> str:
    value = _identifier(value, field_name)
    if value.startswith(_INTERNAL_KEY_PREFIXES):
        raise ReservedKeyError(
            f"{field_name} uses a reserved ledger idempotency namespace"
        )
    return value


def _digest_key(namespace: str, *parts: str) -> str:
    encoded = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return f"{namespace}caller:{digest}"


def _indexed_reservation_id(run_id: str, suffix: str) -> str:
    encoded = base64.urlsafe_b64encode(run_id.encode("utf-8")).decode("ascii").rstrip("=")
    return f"budget:{encoded}:{suffix}"


def _run_id_from_index(reservation_id: str) -> str | None:
    if not reservation_id.startswith("budget:"):
        return None
    parts = reservation_id.split(":", 2)
    if len(parts) != 3 or not parts[1]:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        run_id = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError, UnicodeEncodeError):
        return None
    return run_id or None


class BudgetLedger:
    """A durable per-Run budget ledger backed by an EventStore."""

    def __init__(
        self,
        event_store: _EventStore | SQLiteEventStore,
        run_limits: Mapping[str, RunLimit | Mapping[str, Any]] | None = None,
    ) -> None:
        self._event_store = event_store
        self._run_limits: dict[str, RunLimit] = {}
        if run_limits is not None:
            if not isinstance(run_limits, Mapping):
                raise TypeError("run_limits must be a mapping of run IDs to RunLimit")
            for run_id, limit in run_limits.items():
                run_id = _identifier(run_id, "run_id")
                self._run_limits[run_id] = (
                    limit if isinstance(limit, RunLimit) else RunLimit.model_validate(limit)
                )
        # If this is a reopened database, the first reservation event is the
        # persisted envelope.  Validate supplied limits immediately so a
        # caller cannot accidentally operate under a different budget.
        for run_id, configured in self._run_limits.items():
            persisted = self._persisted_limit(self.read(run_id))
            if persisted is not None:
                self._check_limit_compatibility(persisted, configured)

    @staticmethod
    def estimate(
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        reasoning_tokens: int = 0,
        cached_input_tokens: int = 0,
        cached_tokens: int | None = None,
        input_price_minor_per_million: int = 0,
        output_price_minor_per_million: int = 0,
        reasoning_price_minor_per_million: int = 0,
        cached_input_price_minor_per_million: int = 0,
        cached_price_minor_per_million: int | None = None,
        provider_fee_minor: int = 0,
        tool_fee_minor: int = 0,
        currency: str,
        snapshot_id: str = "unspecified",
        price_snapshot_id: str = "unspecified",
        tokenizer_snapshot_id: str = "unspecified",
        estimator_snapshot_id: str = "unspecified",
        token_limit: int | None = None,
    ) -> CostEstimate:
        """Calculate a cost using integer arithmetic and one final ceiling.

        Rates are minor currency units per million tokens.  All token classes
        are summed before the division, so several sub-minor components do
        not each round up and inflate the estimate.
        """

        if cached_tokens is not None:
            cached_input_tokens = cached_tokens
        if cached_price_minor_per_million is not None:
            cached_input_price_minor_per_million = cached_price_minor_per_million
        token_values = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "reasoning_tokens": reasoning_tokens,
            "cached_input_tokens": cached_input_tokens,
            "input_price_minor_per_million": input_price_minor_per_million,
            "output_price_minor_per_million": output_price_minor_per_million,
            "reasoning_price_minor_per_million": reasoning_price_minor_per_million,
            "cached_input_price_minor_per_million": cached_input_price_minor_per_million,
            "provider_fee_minor": provider_fee_minor,
            "tool_fee_minor": tool_fee_minor,
        }
        for name, value in token_values.items():
            token_values[name] = _nonnegative_int(value, name)
        if token_limit is not None:
            token_limit = _nonnegative_int(token_limit, "token_limit")
        numerator = (
            token_values["input_tokens"] * token_values["input_price_minor_per_million"]
            + token_values["output_tokens"] * token_values["output_price_minor_per_million"]
            + token_values["reasoning_tokens"]
            * token_values["reasoning_price_minor_per_million"]
            + token_values["cached_input_tokens"]
            * token_values["cached_input_price_minor_per_million"]
        )
        amount_minor = _ceil_million(numerator) + token_values["provider_fee_minor"] + token_values[
            "tool_fee_minor"
        ]
        return CostEstimate(
            amount_minor=amount_minor,
            currency=currency,
            token_limit=token_limit,
            input_tokens=token_values["input_tokens"],
            output_tokens=token_values["output_tokens"],
            reasoning_tokens=token_values["reasoning_tokens"],
            cached_input_tokens=token_values["cached_input_tokens"],
            provider_fee_minor=token_values["provider_fee_minor"],
            tool_fee_minor=token_values["tool_fee_minor"],
            input_price_minor_per_million=token_values["input_price_minor_per_million"],
            output_price_minor_per_million=token_values["output_price_minor_per_million"],
            reasoning_price_minor_per_million=token_values[
                "reasoning_price_minor_per_million"
            ],
            cached_input_price_minor_per_million=token_values[
                "cached_input_price_minor_per_million"
            ],
            snapshot_id=snapshot_id,
            price_snapshot_id=price_snapshot_id,
            tokenizer_snapshot_id=tokenizer_snapshot_id,
            estimator_snapshot_id=estimator_snapshot_id,
        )

    def reserve(
        self,
        run_id: str,
        estimate: CostEstimate,
        token_limit: int | None = None,
        *,
        reservation_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> BudgetReservation:
        run_id = _identifier(run_id, "run_id")
        if not isinstance(estimate, CostEstimate):
            raise TypeError("estimate must be a CostEstimate")
        reserved_tokens = estimate.token_limit if token_limit is None else token_limit
        if reserved_tokens is None:
            reserved_tokens = estimate.total_tokens
        reserved_tokens = _nonnegative_int(reserved_tokens, "token_limit")
        if reserved_tokens < estimate.total_tokens:
            raise ValueError("token_limit cannot be lower than the estimate token totals")
        reservation_id = (
            _identifier(reservation_id, "reservation_id")
            if reservation_id is not None
            else None
        )
        if idempotency_key is None:
            if reservation_id is None:
                reservation_id = _indexed_reservation_id(run_id, new_id())
            append_key = f"reserve:{reservation_id}"
        else:
            caller_key = _caller_key(idempotency_key, "idempotency_key")
            append_key = _digest_key("reserve:", run_id, caller_key)
            if reservation_id is None:
                # The identity is stable before the CAS callback executes,
                # so concurrent retries and legacy adapters converge on one
                # reservation even when the first append response is lost.
                identity = hashlib.sha256(
                    json.dumps(
                        (run_id, caller_key),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                reservation_id = _indexed_reservation_id(run_id, identity)

        def decide(events: list[StoredEvent], _version: int) -> Iterable[EventDraft] | None:
            limit = self._limit_for(run_id, events=events)
            self._check_currency(limit.currency, estimate.currency)
            payload = self._reservation_payload(
                run_id, reservation_id, estimate, reserved_tokens, limit
            )
            same_key = [event for event in events if event.idempotency_key == append_key]
            if same_key:
                original_event = next(
                    (
                        event
                        for event in same_key
                        if event.event_type == "BudgetReserved"
                    ),
                    None,
                )
                if original_event is None:
                    raise IdempotencyConflict(
                        f"idempotency key {append_key!r} was already used for a different operation"
                    )
                self._check_reservation_idempotency(same_key, payload)
                return None
            states = self._replay_states(run_id, events)
            existing_state = states.get(reservation_id)
            if existing_state is not None:
                raise IdempotencyConflict(
                    f"reservation ID {reservation_id!r} is already used in this Run"
                )
            balance = self._balance_from_events(run_id, events, limit, _version)
            self._ensure_fits(balance, estimate.amount_minor, reserved_tokens, estimate)
            return [EventDraft("BudgetReserved", payload)]

        events = self._transactional_append(run_id, append_key, decide)
        if not events:
            # The callback only returns no drafts for an existing key; a
            # conforming EventStore returns its original batch.  Retain a
            # defensive lookup for small third-party adapters.
            events = self._events_for_key(run_id, append_key)
        return self._reservation_from_event(
            next(event for event in events if event.event_type == "BudgetReserved")
        )

    def mark_unknown(
        self,
        reservation_id: str,
        *,
        run_id: str | None = None,
        reason: str = "result_unknown",
    ) -> BudgetReservation:
        reservation_id = _identifier(reservation_id, "reservation_id")
        run_id = self._resolve_run_id(reservation_id, run_id)
        reason = _identifier(reason, "reason")
        append_key = f"unknown:{reservation_id}"

        def decide(events: list[StoredEvent], _version: int) -> Iterable[EventDraft] | None:
            states = self._replay_states(run_id, events)
            try:
                state = states[reservation_id]
            except KeyError as exc:
                raise ReservationNotFound(f"unknown reservation {reservation_id!r}") from exc
            payload = {
                "adjustment_minor": 0,
                "adjustment_tokens": 0,
                "currency": state.reservation.currency,
                "reason": reason,
                "reservation_id": reservation_id,
                "run_id": run_id,
                "status": "unknown",
            }
            same_key = [event for event in events if event.idempotency_key == append_key]
            if same_key:
                marker = next(
                    (event for event in same_key if event.event_type == "CostAdjusted"),
                    None,
                )
                if marker is None or dict(marker.payload) != payload:
                    raise IdempotencyConflict(
                        f"unknown marker key {append_key!r} was already used differently"
                    )
                if state.reservation.status != "unknown":
                    raise ReservationStateError("unknown marker is not reflected in replay")
                return None
            if state.reservation.status == "unknown":
                return None
            if state.reservation.status != "reserved":
                raise ReservationStateError(
                    f"reservation {reservation_id!r} is already {state.reservation.status}"
                )
            return [EventDraft("CostAdjusted", payload)]

        self._transactional_append(run_id, append_key, decide)
        return self._state_for(reservation_id, run_id)[0].reservation

    def commit_usage(
        self,
        reservation_id: str,
        usage: UsageRecord | Mapping[str, Any] | None = None,
        *,
        settlement_key: str | None = None,
        run_id: str | None = None,
        **usage_fields: Any,
    ) -> UsageRecord:
        return self._settle_usage(
            reservation_id,
            usage,
            settlement_key=settlement_key,
            run_id=run_id,
            usage_fields=usage_fields,
            allow_unknown=False,
        )

    def reconcile_unknown(
        self,
        reservation_id: str,
        usage: UsageRecord | Mapping[str, Any] | None = None,
        settlement_key: str | None = None,
        *,
        run_id: str | None = None,
        **usage_fields: Any,
    ) -> UsageRecord:
        """Explicitly settle a previously unknown external effect.

        ``commit_usage`` intentionally rejects unknown reservations.  This
        method is the auditable reconciliation action and includes a
        ``CostAdjusted`` event in the same transaction as observation,
        commitment, and release of the unused worst-case hold.
        """

        if settlement_key is None:
            raise ValueError("settlement_key is required when reconciling unknown usage")
        return self._settle_usage(
            reservation_id,
            usage,
            settlement_key=settlement_key,
            run_id=run_id,
            usage_fields=usage_fields,
            allow_unknown=True,
        )

    def _settle_usage(
        self,
        reservation_id: str,
        usage: UsageRecord | Mapping[str, Any] | None,
        *,
        settlement_key: str | None,
        run_id: str | None,
        usage_fields: Mapping[str, Any],
        allow_unknown: bool,
    ) -> UsageRecord:
        reservation_id = _identifier(reservation_id, "reservation_id")
        if usage is None:
            usage = dict(usage_fields)
        elif usage_fields:
            raise TypeError("usage fields cannot be supplied with a usage mapping")
        run_id = self._resolve_run_id(reservation_id, run_id)
        if settlement_key is None:
            settlement_key = f"settle:{reservation_id}"
            append_key = settlement_key
        else:
            settlement_key = _caller_key(settlement_key, "settlement_key")
            append_key = _digest_key(
                "reconcile:" if allow_unknown else "settle:",
                run_id,
                reservation_id,
                settlement_key,
            )

        def decide(events: list[StoredEvent], version: int) -> Iterable[EventDraft] | None:
            # User settlement keys are persisted in payloads while the
            # operation-specific append key prevents collisions with reserve,
            # unknown, release, and reconciliation operations.
            same_key = [event for event in events if event.idempotency_key == append_key]
            settlement_events = self._settlement_events(
                events, reservation_id, settlement_key
            )
            states = self._replay_states(run_id, events)
            try:
                state = states[reservation_id]
            except KeyError as exc:
                raise ReservationNotFound(f"unknown reservation {reservation_id!r}") from exc
            if same_key or settlement_events:
                committed = next(
                    (
                        event
                        for event in (same_key or settlement_events)
                        if event.event_type == "CostCommitted"
                    ),
                    None,
                )
                if committed is None:
                    raise IdempotencyConflict(
                        f"settlement key {settlement_key!r} was already used for a different operation"
                    )
                prior_events = same_key or settlement_events
                record = self._build_usage(state, usage, settlement_key)
                self._check_settlement_fingerprint(prior_events, record)
                marker = self._reconciliation_marker(prior_events, reservation_id, settlement_key)
                if allow_unknown:
                    if marker is None:
                        raise ReservationStateError(
                            "reconcile_unknown cannot reuse an ordinary settlement"
                        )
                    self._check_reconciliation_fingerprint(marker, state, record)
                elif marker is not None:
                    raise ReservationStateError(
                        "ordinary commit cannot reuse a reconciliation settlement"
                    )
                return None
            if allow_unknown and state.reservation.status != "unknown":
                raise ReservationStateError(
                    "reconcile_unknown requires a reservation with status unknown"
                )
            if state.reservation.status == "unknown" and not allow_unknown:
                raise ReservationStateError(
                    "unknown reservations require explicit reconcile_unknown"
                )
            if state.reservation.status == "committed":
                if state.usage is not None and state.usage.settlement_key == settlement_key:
                    return None
                raise ReservationStateError("a committed reservation cannot be settled twice")
            if state.reservation.status == "released":
                raise ReservationStateError("released reservations cannot be committed")
            record = self._build_usage(state, usage, settlement_key)
            limit = self._limit_for(run_id, events=events)
            balance = self._balance_from_events(run_id, events, limit, version)
            self._ensure_actual_fits(balance, record, limit, state)
            release_minor = max(0, state.reservation.reserved_minor - record.cost_minor)
            release_tokens = max(0, state.reservation.reserved_tokens - record.total_tokens)
            committed_payload = self._committed_payload(record)
            released_payload = {
                "currency": record.currency,
                "reason": "settlement_remainder",
                "released_minor": release_minor,
                "released_tokens": release_tokens,
                "reservation_id": reservation_id,
                "run_id": run_id,
                "status": "committed",
            }
            drafts: list[EventDraft] = [
                EventDraft("UsageObserved", self._usage_payload(record)),
                EventDraft("CostCommitted", committed_payload),
                EventDraft("BudgetReleased", released_payload),
            ]
            if state.reservation.status == "unknown":
                drafts.append(
                    EventDraft(
                        "CostAdjusted",
                        {
                            "adjustment_minor": record.cost_minor
                            - state.reservation.reserved_minor,
                            "adjustment_tokens": record.total_tokens
                            - state.reservation.reserved_tokens,
                            "currency": record.currency,
                            "reason": "unknown_reconciled",
                            "reservation_id": reservation_id,
                            "run_id": run_id,
                            "settlement_key": settlement_key,
                            "status": "reconciled",
                        },
                    )
                )
            return drafts

        events = self._transactional_append(run_id, append_key, decide)
        committed = next(
            (event for event in events if event.event_type == "CostCommitted"), None
        )
        if committed is None:
            # ``append_checked`` returns original events for duplicate keys;
            # adapters that return an empty no-op batch can be replayed here.
            current = self.read(run_id)
            committed = next(
                (
                    event
                    for event in self._settlement_events(current, reservation_id, settlement_key)
                    if event.event_type == "CostCommitted"
                ),
                None,
            )
        if committed is None:
            raise ReservationStateError("settlement committed without a CostCommitted event")
        return self._usage_from_committed_event(committed)

    def release(
        self,
        reservation_id: str,
        *,
        run_id: str | None = None,
        reason: str = "released",
    ) -> BudgetReservation:
        reservation_id = _identifier(reservation_id, "reservation_id")
        run_id = self._resolve_run_id(reservation_id, run_id)
        reason = _identifier(reason, "reason")
        append_key = _digest_key("release:", run_id, reservation_id)

        def decide(events: list[StoredEvent], _version: int) -> Iterable[EventDraft] | None:
            states = self._replay_states(run_id, events)
            try:
                state = states[reservation_id]
            except KeyError as exc:
                raise ReservationNotFound(f"unknown reservation {reservation_id!r}") from exc
            payload = {
                "currency": state.reservation.currency,
                "reason": reason,
                "released_minor": state.reservation.reserved_minor,
                "released_tokens": state.reservation.reserved_tokens,
                "reservation_id": reservation_id,
                "run_id": run_id,
                "status": "released",
            }
            same_key = [event for event in events if event.idempotency_key == append_key]
            if same_key:
                marker = next(
                    (event for event in same_key if event.event_type == "BudgetReleased"),
                    None,
                )
                if marker is None or dict(marker.payload) != payload:
                    raise IdempotencyConflict(
                        f"release key {append_key!r} was already used differently"
                    )
                return None
            if state.reservation.status in {"released", "committed"}:
                # If settlement won a release race, returning the committed
                # state is deterministic and does not manufacture a release.
                if state.reservation.status == "released":
                    prior = next(
                        (
                            event
                            for event in events
                            if event.event_type == "BudgetReleased"
                            and event.payload.get("reservation_id") == reservation_id
                        ),
                        None,
                    )
                    if prior is not None and dict(prior.payload) != payload:
                        raise IdempotencyConflict(
                            "release request differs from the persisted release"
                        )
                return None
            if state.reservation.status == "unknown":
                raise BudgetReleasedError(
                    "unknown reservations remain held until explicit reconciliation"
                )
            return [EventDraft("BudgetReleased", payload)]

        self._transactional_append(run_id, append_key, decide)
        return self._state_for(reservation_id, run_id)[0].reservation

    def available(self, run_id: str) -> BudgetBalance:
        run_id = _identifier(run_id, "run_id")
        if hasattr(self._event_store, "read_stream_with_version"):
            events, version = self._event_store.read_stream_with_version(
                _BUDGET_STREAM, run_id
            )
        else:
            events = self.read(run_id)
            version = self._event_store.current_version(_BUDGET_STREAM, run_id)
        limit = self._limit_for(run_id, events=events, required=False)
        if limit is None:
            # An unconfigured Run can still be queried; no new hold can be
            # made until a finite envelope is supplied.
            limit = RunLimit(max_cost_minor=None, max_tokens=0, currency="USD")
        return self._balance_from_events(run_id, events, limit, version)

    balance = available

    @classmethod
    def reopen(
        cls,
        event_store: _EventStore | SQLiteEventStore,
        run_limits: Mapping[str, RunLimit | Mapping[str, Any]] | None = None,
    ) -> "BudgetLedger":
        """Load a ledger from durable events, validating any supplied limits."""

        return cls(event_store, run_limits=run_limits)

    load = reopen

    def read(self, run_id: str, after_version: int = 0) -> list[StoredEvent]:
        run_id = _identifier(run_id, "run_id")
        return self._event_store.read_stream(_BUDGET_STREAM, run_id, after_version)

    read_events = read

    def read_with_version(
        self, run_id: str, after_version: int = 0
    ) -> tuple[list[StoredEvent], int]:
        """Read an event tail and stream version from one snapshot."""

        run_id = _identifier(run_id, "run_id")
        reader = getattr(self._event_store, "read_stream_with_version", None)
        if callable(reader):
            return reader(_BUDGET_STREAM, run_id, after_version)
        events = self.read(run_id, after_version)
        return events, self._event_store.current_version(_BUDGET_STREAM, run_id)

    def replay(self, run_id: str) -> BudgetBalance:
        return self.available(run_id)

    read_replay = replay

    def get_reservation(
        self, reservation_id: str, *, run_id: str | None = None
    ) -> BudgetReservation:
        return self._state_for(reservation_id, run_id)[0].reservation

    def _limit_for(
        self,
        run_id: str,
        *,
        events: list[StoredEvent] | None = None,
        required: bool = True,
    ) -> RunLimit | None:
        if events is None:
            events = self.read(run_id)
        persisted = self._persisted_limit(events)
        configured = self._run_limits.get(run_id)
        if persisted is not None:
            if configured is not None:
                self._check_limit_compatibility(persisted, configured)
            # Events are authoritative after reopen.  A caller may repeat the
            # same envelope, but may not silently narrow or expand it.
            return persisted
        if configured is not None:
            return configured
        if required:
            raise BudgetError(f"no RunLimit configured for {run_id!r}")
        return None

    @staticmethod
    def _persisted_limit(events: list[StoredEvent]) -> RunLimit | None:
        first = next((event for event in events if event.event_type == "BudgetReserved"), None)
        if first is None:
            return None
        payload = first.payload
        return RunLimit(
            max_cost_minor=payload.get("max_cost_minor"),
            max_tokens=payload.get("max_tokens"),
            currency=payload.get("currency", "USD"),
            max_input_tokens=payload.get("max_input_tokens"),
            max_output_tokens=payload.get("max_output_tokens"),
            max_reasoning_tokens=payload.get("max_reasoning_tokens"),
            max_cached_input_tokens=payload.get("max_cached_input_tokens"),
        )

    @staticmethod
    def _check_limit_compatibility(persisted: RunLimit, configured: RunLimit) -> None:
        fields = (
            "max_cost_minor",
            "max_tokens",
            "currency",
            "max_input_tokens",
            "max_output_tokens",
            "max_reasoning_tokens",
            "max_cached_input_tokens",
        )
        for field_name in fields:
            if getattr(persisted, field_name) != getattr(configured, field_name):
                if field_name == "currency":
                    raise CurrencyMismatch(persisted.currency, configured.currency)
                raise BudgetLimitMismatch(
                    f"persisted RunLimit field {field_name!r} does not match supplied envelope"
                )

    @staticmethod
    def _check_currency(expected: str, actual: str) -> None:
        if expected.upper() != actual.upper():
            raise CurrencyMismatch(expected, actual)

    @staticmethod
    def _ensure_fits(
        balance: BudgetBalance,
        amount_minor: int,
        tokens: int,
        estimate: CostEstimate | None = None,
    ) -> None:
        if balance.max_cost_minor is not None and (balance.available_minor or 0) < amount_minor:
            raise BudgetExhausted("reservation exceeds the Run cost limit")
        if balance.max_tokens is not None and (balance.available_tokens or 0) < tokens:
            raise BudgetExhausted("reservation exceeds the Run token limit")
        if estimate is None:
            return
        held = {
            "input_tokens": balance.used_input_tokens
            + balance.reserved_input_tokens
            + balance.unknown_input_tokens,
            "output_tokens": balance.used_output_tokens
            + balance.reserved_output_tokens
            + balance.unknown_output_tokens,
            "reasoning_tokens": balance.reserved_reasoning_tokens
            + balance.used_reasoning_tokens
            + balance.unknown_reasoning_tokens,
            "cached_input_tokens": balance.used_cached_input_tokens
            + balance.reserved_cached_input_tokens
            + balance.unknown_cached_input_tokens,
        }
        caps = {
            "input_tokens": balance.max_input_tokens,
            "output_tokens": balance.max_output_tokens,
            "reasoning_tokens": balance.max_reasoning_tokens,
            "cached_input_tokens": balance.max_cached_input_tokens,
        }
        requested = {
            "input_tokens": estimate.input_tokens,
            "output_tokens": estimate.output_tokens,
            "reasoning_tokens": estimate.reasoning_tokens,
            "cached_input_tokens": estimate.cached_input_tokens,
        }
        for name, cap in caps.items():
            if cap is not None and held[name] + requested[name] > cap:
                raise BudgetExhausted(f"reservation exceeds the Run {name} limit")

    @staticmethod
    def _ensure_actual_fits(
        balance: BudgetBalance,
        record: UsageRecord,
        limit: RunLimit,
        state: _ReservationState,
    ) -> None:
        other_held_tokens = max(
            0,
            balance.held_tokens
            - state.reservation.reserved_tokens,
        )
        if limit.max_cost_minor is not None:
            other_held_minor = max(
                0,
                balance.held_minor - state.reservation.reserved_minor,
            )
            if balance.used_minor + other_held_minor + record.cost_minor > limit.max_cost_minor:
                raise BudgetExhausted("committed cost exceeds the Run cost limit")
        if limit.max_tokens is not None:
            if balance.used_tokens + other_held_tokens + record.total_tokens > limit.max_tokens:
                raise BudgetExhausted("committed tokens exceed the Run token limit")
        class_values = {
            "input_tokens": record.input_tokens,
            "output_tokens": record.output_tokens,
            "reasoning_tokens": record.reasoning_tokens,
            "cached_input_tokens": record.cached_input_tokens,
        }
        class_used = {
            "input_tokens": balance.used_input_tokens,
            "output_tokens": balance.used_output_tokens,
            "reasoning_tokens": balance.used_reasoning_tokens,
            "cached_input_tokens": balance.used_cached_input_tokens,
        }
        class_held = {
            "input_tokens": balance.reserved_input_tokens + balance.unknown_input_tokens,
            "output_tokens": balance.reserved_output_tokens + balance.unknown_output_tokens,
            "reasoning_tokens": balance.reserved_reasoning_tokens
            + balance.unknown_reasoning_tokens,
            "cached_input_tokens": balance.reserved_cached_input_tokens
            + balance.unknown_cached_input_tokens,
        }
        state_classes = {
            "input_tokens": state.estimate.get("input_tokens", 0),
            "output_tokens": state.estimate.get("output_tokens", 0),
            "reasoning_tokens": state.estimate.get("reasoning_tokens", 0),
            "cached_input_tokens": state.estimate.get("cached_input_tokens", 0),
        }
        for name, cap in (
            ("input_tokens", limit.max_input_tokens),
            ("output_tokens", limit.max_output_tokens),
            ("reasoning_tokens", limit.max_reasoning_tokens),
            ("cached_input_tokens", limit.max_cached_input_tokens),
        ):
            if cap is not None:
                other_class_held = max(0, class_held[name] - state_classes[name])
                if class_used[name] + other_class_held + class_values[name] > cap:
                    raise BudgetExhausted(f"committed {name} exceed the Run limit")

    def _transactional_append(
        self,
        run_id: str,
        idempotency_key: str,
        decide: Callable[[list[StoredEvent], int], Iterable[EventDraft] | None],
    ) -> list[StoredEvent]:
        append_checked = getattr(self._event_store, "append_checked", None)
        if callable(append_checked):
            return append_checked(_BUDGET_STREAM, run_id, idempotency_key, decide)
        # Compatibility for an older EventStore adapter.  SQLiteEventStore
        # always takes the locked path above; this bounded fallback retains
        # the original CAS behavior for custom adapters.
        for _attempt in range(3):
            events = self.read(run_id)
            version = self._event_store.current_version(_BUDGET_STREAM, run_id)
            drafts = decide(events, version)
            if drafts is None:
                return self._events_for_key(run_id, idempotency_key)
            try:
                appended = self._event_store.append(
                    _BUDGET_STREAM, run_id, version, drafts, idempotency_key
                )
                if appended:
                    return appended
                # Some legacy adapters commit successfully but lose the
                # response.  Recover the winner by the same durable key;
                # deterministic reservation IDs make this safe for retries.
                persisted = self._events_for_key(run_id, idempotency_key)
                if persisted:
                    return persisted
                return appended
            except StaleStream:
                continue
        raise ReservationStateError("budget stream changed during transactional append")

    def _events_for_key(self, run_id: str, key: str) -> list[StoredEvent]:
        return [event for event in self.read(run_id) if event.idempotency_key == key]

    @staticmethod
    def _settlement_events(
        events: list[StoredEvent], reservation_id: str, settlement_key: str
    ) -> list[StoredEvent]:
        matching = [
            event
            for event in events
            if event.event_type in {"UsageObserved", "CostCommitted", "CostAdjusted"}
            and event.payload.get("settlement_key") == settlement_key
        ]
        for event in matching:
            if event.payload.get("reservation_id") != reservation_id:
                raise IdempotencyConflict(
                    f"settlement key {settlement_key!r} was already used for another reservation"
                )
        return matching

    @staticmethod
    def _reconciliation_marker(
        events: list[StoredEvent], reservation_id: str, settlement_key: str
    ) -> StoredEvent | None:
        return next(
            (
                event
                for event in events
                if event.event_type == "CostAdjusted"
                and event.payload.get("status") == "reconciled"
                and event.payload.get("reservation_id") == reservation_id
                and event.payload.get("settlement_key") == settlement_key
            ),
            None,
        )

    @staticmethod
    def _check_reconciliation_fingerprint(
        marker: StoredEvent, state: _ReservationState, record: UsageRecord
    ) -> None:
        expected = {
            "adjustment_minor": record.cost_minor - state.reservation.reserved_minor,
            "adjustment_tokens": record.total_tokens - state.reservation.reserved_tokens,
            "currency": record.currency,
            "reason": "unknown_reconciled",
            "reservation_id": record.reservation_id,
            "run_id": record.run_id,
            "settlement_key": record.settlement_key,
            "status": "reconciled",
        }
        if dict(marker.payload) != expected:
            raise IdempotencyConflict(
                "reconciliation settlement key was already used for different usage"
            )

    def _state_for(
        self, reservation_id: str, run_id: str | None
    ) -> tuple[_ReservationState, str]:
        reservation_id = _identifier(reservation_id, "reservation_id")
        resolved_run_id = (
            self._run_id_from_reservation(reservation_id)
            if run_id is None
            else _identifier(run_id, "run_id")
        )
        states = self._replay_states(resolved_run_id, self.read(resolved_run_id))
        try:
            state = states[reservation_id]
        except KeyError as exc:
            raise ReservationNotFound(f"unknown reservation {reservation_id!r}") from exc
        if state.reservation.run_id != resolved_run_id:
            raise ReservationStateError("reservation belongs to a different Run")
        return state, resolved_run_id

    def _resolve_run_id(self, reservation_id: str, run_id: str | None) -> str:
        reservation_id = _identifier(reservation_id, "reservation_id")
        return (
            self._run_id_from_reservation(reservation_id)
            if run_id is None
            else _identifier(run_id, "run_id")
        )

    def _run_id_from_reservation(self, reservation_id: str) -> str:
        indexed_run_id = _run_id_from_index(reservation_id)
        if indexed_run_id is not None:
            indexed_states = self._replay_states(
                indexed_run_id, self.read(indexed_run_id)
            )
            indexed_state = indexed_states.get(reservation_id)
            if indexed_state is not None and indexed_state.reservation.run_id == indexed_run_id:
                return indexed_run_id
        stream_ids = getattr(self._event_store, "stream_ids", None)
        candidates = (
            stream_ids(_BUDGET_STREAM)
            if callable(stream_ids)
            else list(self._run_limits)
        )
        candidates = [
            run_id
            for run_id in candidates
            if reservation_id
            in {
                state.reservation.reservation_id
                for state in self._replay_states(run_id, self.read(run_id)).values()
            }
        ]
        if len(candidates) == 1:
            return candidates[0]
        raise ReservationNotFound(
            "run_id is required for custom reservation IDs that are not in the local limits"
        )

    @staticmethod
    def _reservation_payload(
        run_id: str,
        reservation_id: str,
        estimate: CostEstimate,
        reserved_tokens: int,
        limit: RunLimit,
    ) -> dict[str, Any]:
        payload = {
            "amount_minor": estimate.amount_minor,
            "cached_input_price_minor_per_million": estimate.cached_input_price_minor_per_million,
            "cached_input_tokens": estimate.cached_input_tokens,
            "currency": estimate.currency,
            "input_price_minor_per_million": estimate.input_price_minor_per_million,
            "input_tokens": estimate.input_tokens,
            "max_cost_minor": limit.max_cost_minor,
            "max_tokens": limit.max_tokens,
            "max_input_tokens": limit.max_input_tokens,
            "max_output_tokens": limit.max_output_tokens,
            "max_reasoning_tokens": limit.max_reasoning_tokens,
            "max_cached_input_tokens": limit.max_cached_input_tokens,
            "output_price_minor_per_million": estimate.output_price_minor_per_million,
            "output_tokens": estimate.output_tokens,
            "price_snapshot_id": estimate.price_snapshot_id,
            "estimator_snapshot_id": estimate.estimator_snapshot_id,
            "provider_fee_minor": estimate.provider_fee_minor,
            "reasoning_price_minor_per_million": estimate.reasoning_price_minor_per_million,
            "reasoning_tokens": estimate.reasoning_tokens,
            "reservation_id": reservation_id,
            "reserved_minor": estimate.amount_minor,
            "reserved_tokens": reserved_tokens,
            "reserved_input_tokens": estimate.input_tokens,
            "reserved_output_tokens": estimate.output_tokens,
            "reserved_reasoning_tokens": estimate.reasoning_tokens,
            "reserved_cached_input_tokens": estimate.cached_input_tokens,
            "run_id": run_id,
            "snapshot_id": estimate.snapshot_id,
            "status": "reserved",
            "tokenizer_snapshot_id": estimate.tokenizer_snapshot_id,
            "tool_fee_minor": estimate.tool_fee_minor,
        }
        return payload

    @staticmethod
    def _reservation_from_event(event: StoredEvent) -> BudgetReservation:
        payload = event.payload
        return BudgetReservation(
            reservation_id=payload["reservation_id"],
            run_id=payload["run_id"],
            reserved_minor=payload["reserved_minor"],
            reserved_tokens=payload["reserved_tokens"],
            reserved_input_tokens=payload.get("reserved_input_tokens", 0),
            reserved_output_tokens=payload.get("reserved_output_tokens", 0),
            reserved_reasoning_tokens=payload.get("reserved_reasoning_tokens", 0),
            reserved_cached_input_tokens=payload.get("reserved_cached_input_tokens", 0),
            currency=payload["currency"],
            reservation_version=event.stream_version,
            status=payload.get("status", "reserved"),
            snapshot_id=payload.get("snapshot_id", "unspecified"),
            tokenizer_snapshot_id=payload.get("tokenizer_snapshot_id", "unspecified"),
            price_snapshot_id=payload.get("price_snapshot_id", "unspecified"),
            estimator_snapshot_id=payload.get("estimator_snapshot_id", "unspecified"),
        )

    @staticmethod
    def _check_reservation_idempotency(
        events: list[StoredEvent], expected: Mapping[str, Any]
    ) -> None:
        original = next(
            (event for event in events if event.event_type == "BudgetReserved"), None
        )
        if original is None:
            raise IdempotencyConflict("idempotency key was used for a different operation")
        # Compare the complete request fingerprint, not just amount/currency.
        # Missing fields from a legacy event are intentionally a conflict: a
        # caller must not treat an incomplete historical reservation as the
        # same request.
        if dict(original.payload) != dict(expected):
            raise IdempotencyConflict(
                "idempotency key was already used for a different reservation request"
            )

    @staticmethod
    def _committed_payload(record: UsageRecord) -> dict[str, Any]:
        return {
            "cached_input_tokens": record.cached_input_tokens,
            "cost_minor": record.cost_minor,
            "currency": record.currency,
            "input_tokens": record.input_tokens,
            "output_tokens": record.output_tokens,
            "provider_fee_minor": record.provider_fee_minor,
            "reasoning_tokens": record.reasoning_tokens,
            "reservation_id": record.reservation_id,
            "run_id": record.run_id,
            "settlement_key": record.settlement_key,
            "status": "committed",
            "tool_fee_minor": record.tool_fee_minor,
            "tokens": record.total_tokens,
        }

    @staticmethod
    def _check_settlement_fingerprint(
        events: list[StoredEvent], record: UsageRecord
    ) -> None:
        observed = next(
            (event for event in events if event.event_type == "UsageObserved"), None
        )
        committed = next(
            (event for event in events if event.event_type == "CostCommitted"), None
        )
        if observed is None or committed is None:
            raise IdempotencyConflict("settlement key points to an incomplete settlement")
        expected_observed = BudgetLedger._usage_payload(record)
        expected_committed = BudgetLedger._committed_payload(record)
        if (
            dict(observed.payload) != expected_observed
            or dict(committed.payload) != expected_committed
        ):
            raise IdempotencyConflict(
                "settlement key was already used for different usage"
            )

    @staticmethod
    def _balance_from_events(
        run_id: str,
        events: list[StoredEvent],
        limit: RunLimit,
        version: int,
    ) -> BudgetBalance:
        states = BudgetLedger._replay_states(run_id, events)
        used_states = [
            state for state in states.values() if state.reservation.status == "committed"
        ]
        held_states = [
            state for state in states.values() if state.reservation.status == "reserved"
        ]
        unknown_states = [
            state for state in states.values() if state.reservation.status == "unknown"
        ]

        def estimate_count(name: str, selected: list[_ReservationState]) -> int:
            return sum(int(state.estimate.get(name, 0)) for state in selected)

        def usage_count(name: str) -> int:
            return sum(
                int(getattr(state.usage, name, 0))
                for state in used_states
                if state.usage is not None
            )

        used_minor = sum(state.cost_minor for state in used_states)
        used_tokens = sum(state.used_tokens for state in used_states)
        reserved_minor = sum(state.reservation.reserved_minor for state in held_states)
        reserved_tokens = sum(state.reservation.reserved_tokens for state in held_states)
        unknown_minor = sum(state.reservation.reserved_minor for state in unknown_states)
        unknown_tokens = sum(state.reservation.reserved_tokens for state in unknown_states)
        return BudgetBalance(
            run_id=run_id,
            currency=limit.currency,
            max_cost_minor=limit.max_cost_minor,
            max_tokens=limit.max_tokens,
            max_input_tokens=limit.max_input_tokens,
            max_output_tokens=limit.max_output_tokens,
            max_reasoning_tokens=limit.max_reasoning_tokens,
            max_cached_input_tokens=limit.max_cached_input_tokens,
            reserved_minor=reserved_minor,
            reserved_tokens=reserved_tokens,
            used_minor=used_minor,
            used_tokens=used_tokens,
            released_minor=sum(state.release_minor for state in states.values()),
            released_tokens=sum(state.release_tokens for state in states.values()),
            unknown_minor=unknown_minor,
            unknown_tokens=unknown_tokens,
            reserved_input_tokens=estimate_count("input_tokens", held_states),
            reserved_output_tokens=estimate_count("output_tokens", held_states),
            reserved_reasoning_tokens=estimate_count("reasoning_tokens", held_states),
            reserved_cached_input_tokens=estimate_count(
                "cached_input_tokens", held_states
            ),
            used_input_tokens=usage_count("input_tokens"),
            used_output_tokens=usage_count("output_tokens"),
            used_reasoning_tokens=usage_count("reasoning_tokens"),
            used_cached_input_tokens=usage_count("cached_input_tokens"),
            unknown_input_tokens=estimate_count("input_tokens", unknown_states),
            unknown_output_tokens=estimate_count("output_tokens", unknown_states),
            unknown_reasoning_tokens=estimate_count("reasoning_tokens", unknown_states),
            unknown_cached_input_tokens=estimate_count(
                "cached_input_tokens", unknown_states
            ),
            reservation_version=version,
            latest_event_id=events[-1].event_id if events else None,
        )

    @staticmethod
    def _replay_states(
        run_id: str, events: list[StoredEvent]
    ) -> dict[str, _ReservationState]:
        states: dict[str, _ReservationState] = {}
        for event in events:
            payload = event.payload
            reservation_id = payload.get("reservation_id")
            if not isinstance(reservation_id, str):
                continue
            if event.event_type == "BudgetReserved":
                states[reservation_id] = _ReservationState(
                    reservation=BudgetLedger._reservation_from_event(event),
                    estimate=dict(payload),
                )
            elif reservation_id not in states:
                continue
            state = states[reservation_id]
            if event.event_type == "UsageObserved":
                state.usage = BudgetLedger._usage_from_observed_event(event)
            elif event.event_type == "CostCommitted":
                state.cost_minor = _nonnegative_int(payload.get("cost_minor", 0), "cost_minor")
                state.used_tokens = _nonnegative_int(payload.get("tokens", 0), "tokens")
                if state.usage is None:
                    state.usage = BudgetLedger._usage_from_committed_event(event)
                else:
                    state.usage = state.usage.model_copy(update={"status": "committed"})
                state.reservation = state.reservation.model_copy(update={"status": "committed"})
            elif event.event_type == "BudgetReleased":
                state.release_minor += _nonnegative_int(
                    payload.get("released_minor", 0), "released_minor"
                )
                state.release_tokens += _nonnegative_int(
                    payload.get("released_tokens", 0), "released_tokens"
                )
                if state.reservation.status == "reserved":
                    status = payload.get("status", "released")
                    state.reservation = state.reservation.model_copy(update={"status": status})
            elif event.event_type == "CostAdjusted":
                if payload.get("status") == "unknown":
                    state.reservation = state.reservation.model_copy(update={"status": "unknown"})
        return states

    @staticmethod
    def _usage_payload(record: UsageRecord) -> dict[str, Any]:
        return {
            "cached_input_tokens": record.cached_input_tokens,
            "cost_minor": record.cost_minor,
            "currency": record.currency,
            "input_tokens": record.input_tokens,
            "output_tokens": record.output_tokens,
            "provider_fee_minor": record.provider_fee_minor,
            "reasoning_tokens": record.reasoning_tokens,
            "reservation_id": record.reservation_id,
            "run_id": record.run_id,
            "settlement_key": record.settlement_key,
            "tool_fee_minor": record.tool_fee_minor,
            "tokens": record.total_tokens,
        }

    @staticmethod
    def _usage_from_observed_event(event: StoredEvent) -> UsageRecord:
        payload = event.payload
        return UsageRecord(
            reservation_id=payload["reservation_id"],
            run_id=payload["run_id"],
            settlement_key=payload["settlement_key"],
            currency=payload["currency"],
            input_tokens=payload.get("input_tokens", 0),
            output_tokens=payload.get("output_tokens", 0),
            reasoning_tokens=payload.get("reasoning_tokens", 0),
            cached_input_tokens=payload.get("cached_input_tokens", 0),
            provider_fee_minor=payload.get("provider_fee_minor", 0),
            tool_fee_minor=payload.get("tool_fee_minor", 0),
            cost_minor=payload.get("cost_minor", 0),
            status="observed",
        )

    @staticmethod
    def _usage_from_committed_event(
        event: StoredEvent, fallback: UsageRecord | None = None
    ) -> UsageRecord:
        payload = event.payload
        if fallback is not None:
            return fallback
        return UsageRecord(
            reservation_id=payload["reservation_id"],
            run_id=payload["run_id"],
            settlement_key=payload["settlement_key"],
            currency=payload["currency"],
            cost_minor=payload.get("cost_minor", 0),
            input_tokens=payload.get("input_tokens", 0),
            output_tokens=payload.get("output_tokens", 0),
            reasoning_tokens=payload.get("reasoning_tokens", 0),
            cached_input_tokens=payload.get("cached_input_tokens", 0),
            provider_fee_minor=payload.get("provider_fee_minor", 0),
            tool_fee_minor=payload.get("tool_fee_minor", 0),
            status="committed",
        )

    @staticmethod
    def _build_usage(
        state: _ReservationState,
        usage: UsageRecord | Mapping[str, Any] | None,
        settlement_key: str,
    ) -> UsageRecord:
        if isinstance(usage, UsageRecord):
            values: dict[str, Any] = usage.model_dump()
        elif isinstance(usage, Mapping):
            values = dict(usage)
        elif usage is None:
            values = {}
        else:
            raise TypeError("usage must be a UsageRecord or mapping")
        aliases = {
            "input": "input_tokens",
            "output": "output_tokens",
            "reasoning": "reasoning_tokens",
            "cached": "cached_input_tokens",
            "cached_tokens": "cached_input_tokens",
            "amount_minor": "cost_minor",
            "actual_minor": "cost_minor",
        }
        for source, target in aliases.items():
            if source in values:
                if target in values:
                    raise ValueError(f"usage contains both {source} and {target}")
                values[target] = values.pop(source)
        supplied_reservation = values.get("reservation_id")
        if supplied_reservation is not None and supplied_reservation != state.reservation.reservation_id:
            raise ReservationStateError("usage reservation_id does not match the target reservation")
        supplied_run = values.get("run_id")
        if supplied_run is not None and supplied_run != state.reservation.run_id:
            raise ReservationStateError("usage run_id does not match the target reservation")
        supplied_settlement = values.get("settlement_key")
        if supplied_settlement is not None and supplied_settlement != settlement_key:
            raise ReservationStateError("usage settlement_key does not match the target settlement")
        allowed = {
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cached_input_tokens",
            "provider_fee_minor",
            "tool_fee_minor",
            "cost_minor",
            "currency",
        }
        unknown = set(values) - allowed - {"reservation_id", "run_id", "settlement_key", "status"}
        if unknown:
            raise ValueError(f"usage has unsupported fields: {sorted(unknown)!r}")
        currency = values.get("currency", state.reservation.currency)
        if not isinstance(currency, str):
            raise ValueError("usage currency must be a string")
        BudgetLedger._check_currency(state.reservation.currency, currency)
        counts = {
            name: _nonnegative_int(values.get(name, 0), name)
            for name in (
                "input_tokens",
                "output_tokens",
                "reasoning_tokens",
                "cached_input_tokens",
                "provider_fee_minor",
                "tool_fee_minor",
            )
        }
        explicit_cost = values.get("cost_minor")
        if explicit_cost is not None:
            explicit_cost = _nonnegative_int(explicit_cost, "cost_minor")
        estimate = state.estimate
        numerator = (
            counts["input_tokens"] * estimate.get("input_price_minor_per_million", 0)
            + counts["output_tokens"] * estimate.get("output_price_minor_per_million", 0)
            + counts["reasoning_tokens"]
            * estimate.get("reasoning_price_minor_per_million", 0)
            + counts["cached_input_tokens"]
            * estimate.get("cached_input_price_minor_per_million", 0)
        )
        calculated = (
            _ceil_million(numerator)
            + counts["provider_fee_minor"]
            + counts["tool_fee_minor"]
        )
        cost_minor = calculated if explicit_cost is None else explicit_cost
        return UsageRecord(
            reservation_id=state.reservation.reservation_id,
            run_id=state.reservation.run_id,
            settlement_key=settlement_key,
            currency=currency,
            input_tokens=counts["input_tokens"],
            output_tokens=counts["output_tokens"],
            reasoning_tokens=counts["reasoning_tokens"],
            cached_input_tokens=counts["cached_input_tokens"],
            provider_fee_minor=counts["provider_fee_minor"],
            tool_fee_minor=counts["tool_fee_minor"],
            cost_minor=cost_minor,
        )


__all__ = [
    "BudgetError",
    "BudgetExhausted",
    "BudgetLimitMismatch",
    "BudgetLedger",
    "BudgetReleasedError",
    "CurrencyMismatch",
    "IdempotencyConflict",
    "ReservedKeyError",
    "ReservationNotFound",
    "ReservationStateError",
]
