"""Event-sourced reservations and cost settlement.

The ledger intentionally has no process-global cache.  Every decision is
made from the budget stream and then committed with EventStore's stream CAS
and idempotency key.  A process restart therefore has exactly the same view
as a long-lived process.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import math
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


_MILLION = 1_000_000
_BUDGET_STREAM = "budget"


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
        limit = self._limit_for(run_id)
        self._check_currency(limit.currency, estimate.currency)
        reserved_tokens = estimate.token_limit if token_limit is None else token_limit
        if reserved_tokens is None:
            reserved_tokens = estimate.total_tokens
        reserved_tokens = _nonnegative_int(reserved_tokens, "token_limit")
        reservation_id = (
            _identifier(reservation_id, "reservation_id")
            if reservation_id is not None
            else f"{run_id}::{new_id()}"
        )
        append_key = idempotency_key or f"reserve:{reservation_id}"
        append_key = _identifier(append_key, "idempotency_key")

        # Idempotency is checked before exhaustion: a retried request must
        # return its original reservation even if later calls consumed the
        # remaining envelope.
        existing = self._events_for_key(run_id, append_key)
        if existing:
            if existing[0].event_type != "BudgetReserved":
                raise IdempotencyConflict(
                    f"idempotency key {append_key!r} was already used for a different operation"
                )
            original = self._reservation_from_event(existing[0])
            if (
                original.currency != estimate.currency
                or original.reserved_minor != estimate.amount_minor
                or original.reserved_tokens != reserved_tokens
            ):
                raise IdempotencyConflict(
                    f"idempotency key {append_key!r} was already used for a different reservation"
                )
            return original

        for _attempt in range(3):
            balance = self.available(run_id)
            self._ensure_fits(balance, estimate.amount_minor, reserved_tokens)
            payload = self._reservation_payload(
                run_id, reservation_id, estimate, reserved_tokens, limit
            )
            try:
                events = self._event_store.append(
                    _BUDGET_STREAM,
                    run_id,
                    self._event_store.current_version(_BUDGET_STREAM, run_id),
                    [EventDraft("BudgetReserved", payload)],
                    append_key,
                )
                return self._reservation_from_event(events[0])
            except StaleStream:
                continue
        # A final call gives the caller the EventStore's precise stale error.
        self._ensure_fits(self.available(run_id), estimate.amount_minor, reserved_tokens)
        expected = self._event_store.current_version(_BUDGET_STREAM, run_id)
        events = self._event_store.append(
            _BUDGET_STREAM,
            run_id,
            expected,
            [EventDraft("BudgetReserved", payload)],
            append_key,
        )
        return self._reservation_from_event(events[0])

    def mark_unknown(
        self,
        reservation_id: str,
        *,
        run_id: str | None = None,
        reason: str = "result_unknown",
    ) -> BudgetReservation:
        state, run_id = self._state_for(reservation_id, run_id)
        if state.reservation.status == "unknown":
            return state.reservation
        if state.reservation.status != "reserved":
            raise ReservationStateError(
                f"reservation {reservation_id!r} is already {state.reservation.status}"
            )
        reason = _identifier(reason, "reason")
        append_key = f"unknown:{reservation_id}"
        existing = self._events_for_key(run_id, append_key)
        if existing:
            return self._state_for(reservation_id, run_id)[0].reservation
        payload = {
            "adjustment_minor": 0,
            "adjustment_tokens": 0,
            "currency": state.reservation.currency,
            "reason": reason,
            "reservation_id": reservation_id,
            "run_id": run_id,
            "status": "unknown",
        }
        for _attempt in range(3):
            try:
                self._event_store.append(
                    _BUDGET_STREAM,
                    run_id,
                    self._event_store.current_version(_BUDGET_STREAM, run_id),
                    [EventDraft("CostAdjusted", payload)],
                    append_key,
                )
                return self._state_for(reservation_id, run_id)[0].reservation
            except StaleStream:
                state, _ = self._state_for(reservation_id, run_id)
                if state.reservation.status == "unknown":
                    return state.reservation
                if state.reservation.status != "reserved":
                    raise ReservationStateError(
                        f"reservation {reservation_id!r} is already {state.reservation.status}"
                    )
        raise ReservationStateError("budget stream changed while marking unknown")

    def commit_usage(
        self,
        reservation_id: str,
        usage: UsageRecord | Mapping[str, Any] | None = None,
        *,
        settlement_key: str | None = None,
        run_id: str | None = None,
        **usage_fields: Any,
    ) -> UsageRecord:
        if usage is None:
            usage = usage_fields
        elif usage_fields:
            raise TypeError("usage fields cannot be supplied with a usage mapping")
        state, run_id = self._state_for(reservation_id, run_id)
        if state.reservation.status == "committed":
            if state.usage is not None and (
                settlement_key is None or settlement_key == state.usage.settlement_key
            ):
                return state.usage.model_copy(update={"status": "committed"})
            raise ReservationStateError("a committed reservation cannot be settled twice")
        elif state.reservation.status == "released":
            raise ReservationStateError("released reservations cannot be committed")

        settlement_key = settlement_key or f"settle:{reservation_id}"
        settlement_key = _identifier(settlement_key, "settlement_key")
        existing = self._events_for_key(run_id, settlement_key)
        if existing:
            for event in existing:
                if event.event_type == "CostCommitted":
                    return self._usage_from_committed_event(event)
            raise IdempotencyConflict(
                f"settlement key {settlement_key!r} was already used for a different operation"
            )
        record = self._build_usage(state, usage, settlement_key)
        limit = self._limit_for(run_id)
        balance = self.available(run_id)
        # The current hold is included in available().  To test a settlement,
        # put the current reservation back into the available envelope and
        # then account for actual usage.
        available_minor = (
            None
            if limit.max_cost_minor is None
            else (balance.available_minor or 0) + state.reservation.reserved_minor
        )
        available_tokens = (
            None
            if limit.max_tokens is None
            else (balance.available_tokens or 0) + state.reservation.reserved_tokens
        )
        if available_minor is not None and balance.used_minor + record.cost_minor > limit.max_cost_minor:
            raise BudgetExhausted("committed cost exceeds the Run cost limit")
        if available_tokens is not None and balance.used_tokens + record.total_tokens > limit.max_tokens:
            raise BudgetExhausted("committed tokens exceed the Run token limit")
        # Avoid unused locals while retaining the explanatory calculations
        # above for reviewers and future per-reservation envelope rules.
        del available_minor, available_tokens
        release_minor = max(0, state.reservation.reserved_minor - record.cost_minor)
        release_tokens = max(0, state.reservation.reserved_tokens - record.total_tokens)
        observed_payload = self._usage_payload(record)
        committed_payload = {
            "cached_input_tokens": record.cached_input_tokens,
            "cost_minor": record.cost_minor,
            "currency": record.currency,
            "input_tokens": record.input_tokens,
            "output_tokens": record.output_tokens,
            "provider_fee_minor": record.provider_fee_minor,
            "reasoning_tokens": record.reasoning_tokens,
            "reservation_id": reservation_id,
            "run_id": run_id,
            "settlement_key": settlement_key,
            "status": "committed",
            "tool_fee_minor": record.tool_fee_minor,
            "tokens": record.total_tokens,
        }
        released_payload = {
            "currency": record.currency,
            "reason": "settlement_remainder",
            "released_minor": release_minor,
            "released_tokens": release_tokens,
            "reservation_id": reservation_id,
            "run_id": run_id,
            "status": "committed",
        }
        drafts = [
            EventDraft("UsageObserved", observed_payload),
            EventDraft("CostCommitted", committed_payload),
            EventDraft("BudgetReleased", released_payload),
        ]
        for _attempt in range(3):
            try:
                events = self._event_store.append(
                    _BUDGET_STREAM,
                    run_id,
                    self._event_store.current_version(_BUDGET_STREAM, run_id),
                    drafts,
                    settlement_key,
                )
                committed = next(
                    event for event in events if event.event_type == "CostCommitted"
                )
                return self._usage_from_committed_event(committed, fallback=record)
            except StaleStream:
                state, _ = self._state_for(reservation_id, run_id)
                existing = self._events_for_key(run_id, settlement_key)
                if existing:
                    return self._usage_from_committed_event(
                        next(event for event in existing if event.event_type == "CostCommitted")
                    )
                if state.reservation.status == "released":
                    raise ReservationStateError("released reservations cannot be committed")
        raise ReservationStateError("budget stream changed while settling usage")

    def release(
        self,
        reservation_id: str,
        *,
        run_id: str | None = None,
        reason: str = "released",
    ) -> BudgetReservation:
        state, run_id = self._state_for(reservation_id, run_id)
        if state.reservation.status == "released":
            return state.reservation
        if state.reservation.status == "unknown":
            raise BudgetReleasedError(
                "unknown reservations remain held until explicit reconciliation"
            )
        if state.reservation.status == "committed":
            return state.reservation
        reason = _identifier(reason, "reason")
        append_key = f"release:{reservation_id}"
        existing = self._events_for_key(run_id, append_key)
        if existing:
            return self._state_for(reservation_id, run_id)[0].reservation
        payload = {
            "currency": state.reservation.currency,
            "reason": reason,
            "released_minor": state.reservation.reserved_minor,
            "released_tokens": state.reservation.reserved_tokens,
            "reservation_id": reservation_id,
            "run_id": run_id,
            "status": "released",
        }
        for _attempt in range(3):
            try:
                self._event_store.append(
                    _BUDGET_STREAM,
                    run_id,
                    self._event_store.current_version(_BUDGET_STREAM, run_id),
                    [EventDraft("BudgetReleased", payload)],
                    append_key,
                )
                return self._state_for(reservation_id, run_id)[0].reservation
            except StaleStream:
                state, _ = self._state_for(reservation_id, run_id)
                if state.reservation.status == "released":
                    return state.reservation
        raise ReservationStateError("budget stream changed while releasing reservation")

    def available(self, run_id: str) -> BudgetBalance:
        run_id = _identifier(run_id, "run_id")
        limit = self._limit_for(run_id, required=False)
        events = self.read(run_id)
        states = self._replay_states(run_id, events)
        if limit is None:
            if events and events[0].event_type == "BudgetReserved":
                limit = RunLimit(
                    max_cost_minor=events[0].payload.get("max_cost_minor"),
                    max_tokens=events[0].payload.get("max_tokens"),
                    currency=events[0].payload.get("currency", "USD"),
                )
            else:
                # An unconfigured Run can still be queried; no new hold can
                # be made until a finite envelope is supplied.
                limit = RunLimit(max_cost_minor=None, max_tokens=0, currency="USD")
        used_minor = sum(state.cost_minor for state in states.values() if state.reservation.status == "committed")
        used_tokens = sum(
            state.used_tokens for state in states.values() if state.reservation.status == "committed"
        )
        reserved_minor = sum(
            state.reservation.reserved_minor
            for state in states.values()
            if state.reservation.status == "reserved"
        )
        reserved_tokens = sum(
            state.reservation.reserved_tokens
            for state in states.values()
            if state.reservation.status == "reserved"
        )
        unknown_minor = sum(
            state.reservation.reserved_minor
            for state in states.values()
            if state.reservation.status == "unknown"
        )
        unknown_tokens = sum(
            state.reservation.reserved_tokens
            for state in states.values()
            if state.reservation.status == "unknown"
        )
        released_minor = sum(state.release_minor for state in states.values())
        released_tokens = sum(state.release_tokens for state in states.values())
        return BudgetBalance(
            run_id=run_id,
            currency=limit.currency,
            max_cost_minor=limit.max_cost_minor,
            max_tokens=limit.max_tokens,
            reserved_minor=reserved_minor,
            reserved_tokens=reserved_tokens,
            used_minor=used_minor,
            used_tokens=used_tokens,
            released_minor=released_minor,
            released_tokens=released_tokens,
            unknown_minor=unknown_minor,
            unknown_tokens=unknown_tokens,
            reservation_version=self._event_store.current_version(_BUDGET_STREAM, run_id),
            latest_event_id=events[-1].event_id if events else None,
        )

    balance = available

    def read(self, run_id: str, after_version: int = 0) -> list[StoredEvent]:
        run_id = _identifier(run_id, "run_id")
        return self._event_store.read_stream(_BUDGET_STREAM, run_id, after_version)

    read_events = read

    def replay(self, run_id: str) -> BudgetBalance:
        return self.available(run_id)

    read_replay = replay

    def get_reservation(
        self, reservation_id: str, *, run_id: str | None = None
    ) -> BudgetReservation:
        return self._state_for(reservation_id, run_id)[0].reservation

    def _limit_for(self, run_id: str, *, required: bool = True) -> RunLimit | None:
        configured = self._run_limits.get(run_id)
        if configured is not None:
            return configured
        events = self.read(run_id)
        if events:
            first = next((event for event in events if event.event_type == "BudgetReserved"), None)
            if first is not None:
                payload = first.payload
                return RunLimit(
                    max_cost_minor=payload.get("max_cost_minor"),
                    max_tokens=payload.get("max_tokens"),
                    currency=payload.get("currency", "USD"),
                )
        if required:
            raise BudgetError(f"no RunLimit configured for {run_id!r}")
        return None

    @staticmethod
    def _check_currency(expected: str, actual: str) -> None:
        if expected.upper() != actual.upper():
            raise CurrencyMismatch(expected, actual)

    @staticmethod
    def _ensure_fits(balance: BudgetBalance, amount_minor: int, tokens: int) -> None:
        if balance.max_cost_minor is not None and (balance.available_minor or 0) < amount_minor:
            raise BudgetExhausted("reservation exceeds the Run cost limit")
        if balance.max_tokens is not None and (balance.available_tokens or 0) < tokens:
            raise BudgetExhausted("reservation exceeds the Run token limit")

    def _events_for_key(self, run_id: str, key: str) -> list[StoredEvent]:
        return [event for event in self.read(run_id) if event.idempotency_key == key]

    def _state_for(
        self, reservation_id: str, run_id: str | None
    ) -> tuple[_ReservationState, str]:
        reservation_id = _identifier(reservation_id, "reservation_id")
        resolved_run_id = run_id or self._run_id_from_reservation(reservation_id)
        resolved_run_id = _identifier(resolved_run_id, "run_id")
        states = self._replay_states(resolved_run_id, self.read(resolved_run_id))
        try:
            return states[reservation_id], resolved_run_id
        except KeyError as exc:
            raise ReservationNotFound(f"unknown reservation {reservation_id!r}") from exc

    def _run_id_from_reservation(self, reservation_id: str) -> str:
        marker = "::"
        if marker in reservation_id:
            return reservation_id.split(marker, 1)[0]
        candidates = [run_id for run_id in self._run_limits if reservation_id in {
            state.reservation.reservation_id
            for state in self._replay_states(run_id, self.read(run_id)).values()
        }]
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
            "output_price_minor_per_million": estimate.output_price_minor_per_million,
            "output_tokens": estimate.output_tokens,
            "price_snapshot_id": estimate.price_snapshot_id,
            "provider_fee_minor": estimate.provider_fee_minor,
            "reasoning_price_minor_per_million": estimate.reasoning_price_minor_per_million,
            "reasoning_tokens": estimate.reasoning_tokens,
            "reservation_id": reservation_id,
            "reserved_minor": estimate.amount_minor,
            "reserved_tokens": reserved_tokens,
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
            currency=payload["currency"],
            reservation_version=event.stream_version,
            status=payload.get("status", "reserved"),
            snapshot_id=payload.get("snapshot_id", "unspecified"),
            tokenizer_snapshot_id=payload.get("tokenizer_snapshot_id", "unspecified"),
            price_snapshot_id=payload.get("price_snapshot_id", "unspecified"),
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
    "BudgetLedger",
    "BudgetReleasedError",
    "CurrencyMismatch",
    "ReservationNotFound",
    "ReservationStateError",
]
