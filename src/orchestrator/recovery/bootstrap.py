"""Deterministic, generic recovery of an aggregate and its projections."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
import inspect
from typing import Any, TypeAlias

from orchestrator.persistence.events import StoredEvent
from orchestrator.persistence.snapshots import Snapshot, SnapshotStore
from orchestrator.persistence.sqlite_event_store import (
    EventIntegrityError,
    SQLiteEventStore,
)


Reducer: TypeAlias = Callable[[Any, StoredEvent], Any]
ProjectionReducer: TypeAlias = Callable[[Any, StoredEvent], Any]
Hook: TypeAlias = Callable[..., Any]

_LEASE_EVENT_TYPES = {"LeaseExpired", "LeaseExpiryDetected", "ExpiredLease"}
_UNKNOWN_EFFECT_EVENT_TYPES = {
    "UnknownEffect",
    "EffectUnknown",
    "UnknownEffectDetected",
    "EffectOutcomeUnknown",
    "OutcomeCannotBeDetermined",
    "OutcomeUnknown",
}


class RecoveryFailure(RuntimeError):
    """Base error for a recovery invariant that prevents safe bootstrap."""

    category = "recovery"

    def __init__(
        self,
        message: str = "recovery invariant failed",
        *,
        aggregate_type: str | None = None,
        aggregate_id: str | None = None,
    ) -> None:
        # Never include event payloads, reducer state, or hook exception text
        # in an invariant error.  Those values may contain secrets.
        self.aggregate_type = aggregate_type
        self.aggregate_id = aggregate_id
        super().__init__(message)


class EventChainFailure(RecoveryFailure):
    """Raised when stream versions, identity, or persisted event data disagree."""

    category = "event_chain"


class BudgetInvariantFailure(RecoveryFailure):
    """Raised when replay exceeds a caller-supplied recovery budget."""

    category = "budget"


class SecurityInvariantFailure(RecoveryFailure):
    """Raised when a caller-supplied security invariant rejects recovery."""

    category = "security"


# Descriptive aliases make the boundary easy to discover without requiring
# downstream modules to couple to one particular class spelling.
EventStreamIntegrityFailure = EventChainFailure
BudgetFailure = BudgetInvariantFailure
SecurityFailure = SecurityInvariantFailure


@dataclass(frozen=True)
class RecoveryResult:
    """Safe recovery output without an event payload dump."""

    aggregate_type: str
    aggregate_id: str
    state: Any
    event_version: int
    snapshot_used: bool
    replayed_from_version: int
    replayed_event_count: int
    projections: Mapping[str, Any] = field(default_factory=dict)
    expired_leases: tuple[Any, ...] = ()
    unknown_effects: tuple[Any, ...] = ()

    @property
    def version(self) -> int:
        return self.event_version

    @property
    def aggregate_state(self) -> Any:
        return self.state


class RecoveryBootstrap:
    """Recover one aggregate from validated events and an optional snapshot.

    The class deliberately knows nothing about a particular lifecycle.  A
    lifecycle supplies event reducers, projections, and optional invariant
    hooks; routing, leases, security, and effect modules can plug into those
    interfaces later without changing persistence.
    """

    def __init__(
        self,
        event_store: SQLiteEventStore,
        snapshot_store: SnapshotStore | None = None,
    ) -> None:
        self.event_store = event_store
        self.snapshot_store = snapshot_store

    def recover(
        self,
        aggregate_type: str,
        aggregate_id: str,
        reducers: Mapping[str, Reducer] | Reducer,
        *,
        initial_state: Any = None,
        projections: Mapping[str, ProjectionReducer | Mapping[str, ProjectionReducer]] | None = None,
        projection_states: Mapping[str, Any] | None = None,
        snapshot_schema_version: int | None = 1,
        max_replay_events: int | None = None,
        budget: int | Mapping[str, Any] | Hook | None = None,
        security_hook: Hook | None = None,
        lease_hook: Hook | None = None,
        lease_checker: Hook | None = None,
        effect_hook: Hook | None = None,
        effect_resolver: Hook | None = None,
        unknown_event_hook: Hook | None = None,
    ) -> RecoveryResult:
        """Validate, checkpoint, and replay an aggregate deterministically."""

        aggregate_type, aggregate_id = _validate_aggregate_identity(
            aggregate_type, aggregate_id
        )
        if not callable(reducers) and not isinstance(reducers, Mapping):
            raise TypeError("reducers must be a callable or event-type mapping")
        if projections is not None and not isinstance(projections, Mapping):
            raise TypeError("projections must be an event-type mapping")
        if projection_states is not None and not isinstance(projection_states, Mapping):
            raise TypeError("projection_states must be a mapping")
        if snapshot_schema_version is not None:
            _validate_positive_int(snapshot_schema_version, "snapshot_schema_version")
        replay_limit = _resolve_replay_limit(max_replay_events, budget)
        _ensure_distinct_hooks(
            lease_hook, lease_checker, "lease_hook", "lease_checker"
        )
        _ensure_distinct_hooks(
            effect_hook, effect_resolver, "effect_hook", "effect_resolver"
        )
        lease_hook = lease_hook or lease_checker
        effect_hook = effect_hook or effect_resolver

        all_events = self._read_and_validate_stream(aggregate_type, aggregate_id)
        self._validate_security(security_hook, all_events, aggregate_type, aggregate_id)

        snapshot = self._load_usable_snapshot(
            aggregate_type,
            aggregate_id,
            all_events,
            snapshot_schema_version,
        )
        if snapshot is None:
            state = initial_state
            replay_from = 0
        else:
            state = snapshot.state
            replay_from = snapshot.event_version
        tail = all_events[replay_from:]
        self._validate_budget(replay_limit, len(tail), aggregate_type, aggregate_id)
        self._validate_budget_hook(budget, tail, aggregate_type, aggregate_id)

        for event in tail:
            state = self._reduce_event(
                reducers,
                state,
                event,
                unknown_event_hook=unknown_event_hook,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )

        rebuilt_projections = self._rebuild_projections(
            all_events,
            projections,
            projection_states,
            aggregate_type,
            aggregate_id,
        )
        expired_leases = _event_findings(
            all_events,
            _LEASE_EVENT_TYPES,
            "lease_id",
        )
        unknown_effects = _event_findings(
            all_events,
            _UNKNOWN_EFFECT_EVENT_TYPES,
            "effect_id",
        )
        expired_leases = _merge_hook_findings(
            expired_leases,
            lease_hook,
            state,
            all_events,
            [event for event in all_events if event.event_type in _LEASE_EVENT_TYPES],
            aggregate_type,
            aggregate_id,
            invariant="event_chain",
        )
        unknown_effects = _merge_hook_findings(
            unknown_effects,
            effect_hook,
            state,
            all_events,
            [event for event in all_events if event.event_type in _UNKNOWN_EFFECT_EVENT_TYPES],
            aggregate_type,
            aggregate_id,
            invariant="event_chain",
        )

        return RecoveryResult(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            state=state,
            event_version=len(all_events),
            snapshot_used=snapshot is not None,
            replayed_from_version=replay_from,
            replayed_event_count=len(tail),
            projections=rebuilt_projections,
            expired_leases=expired_leases,
            unknown_effects=unknown_effects,
        )

    def _read_and_validate_stream(
        self, aggregate_type: str, aggregate_id: str
    ) -> list[StoredEvent]:
        try:
            atomic_reader = getattr(self.event_store, "read_stream_snapshot", None)
            if atomic_reader is None:
                atomic_reader = getattr(self.event_store, "read_stream_with_version", None)
            if atomic_reader is not None:
                events, current_version = atomic_reader(aggregate_type, aggregate_id)
            else:
                # Compatibility fallback for lightweight adapters.  The
                # SQLiteEventStore always supplies the atomic reader above.
                events = self.event_store.read_stream(aggregate_type, aggregate_id)
                current_version = self.event_store.current_version(aggregate_type, aggregate_id)
        except (EventIntegrityError, ValueError, TypeError) as exc:
            raise EventChainFailure(
                "event stream integrity validation failed",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            ) from exc
        if current_version != len(events):
            raise EventChainFailure(
                "event stream version does not match its event count",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )
        event_ids: set[str] = set()
        for expected_version, event in enumerate(events, start=1):
            if not isinstance(event, StoredEvent):
                raise EventChainFailure(
                    "event stream contains an invalid event object",
                    aggregate_type=aggregate_type,
                    aggregate_id=aggregate_id,
                )
            if (
                event.stream_type != aggregate_type
                or event.stream_id != aggregate_id
                or event.stream_version != expected_version
                or event.event_id in event_ids
            ):
                raise EventChainFailure(
                    "event stream has a broken identity or version chain",
                    aggregate_type=aggregate_type,
                    aggregate_id=aggregate_id,
                )
            event_ids.add(event.event_id)
        return events

    def _load_usable_snapshot(
        self,
        aggregate_type: str,
        aggregate_id: str,
        events: list[StoredEvent],
        expected_schema_version: int | None,
    ) -> Snapshot | None:
        if self.snapshot_store is None:
            return None
        try:
            snapshot = self.snapshot_store.load_valid(
                aggregate_type,
                aggregate_id,
                expected_schema_version=expected_schema_version,
            )
        except (ValueError, TypeError):
            return None
        if snapshot is None:
            return None
        if snapshot.event_version > len(events):
            return None
        source = events[snapshot.event_version - 1]
        if source.event_id != snapshot.source_event_id:
            return None
        return snapshot

    @staticmethod
    def _reduce_event(
        reducers: Mapping[str, Reducer] | Reducer,
        state: Any,
        event: StoredEvent,
        *,
        unknown_event_hook: Hook | None,
        aggregate_type: str,
        aggregate_id: str,
    ) -> Any:
        # An unresolved effect outcome is a terminal diagnostic, never a
        # lifecycle transition.  Do not pass it to wildcard/callable
        # reducers where it could accidentally schedule the effect again.
        if event.event_type in _UNKNOWN_EFFECT_EVENT_TYPES:
            return state
        if callable(reducers):
            reducer = reducers
        else:
            reducer = reducers.get(event.event_type) or reducers.get("*")
        if reducer is None:
            if event.event_type in _LEASE_EVENT_TYPES | _UNKNOWN_EFFECT_EVENT_TYPES:
                return state
            if unknown_event_hook is not None:
                try:
                    reducer_result = _invoke_hook(
                        unknown_event_hook,
                        (state, event),
                        (event,),
                    )
                except BaseException as exc:
                    raise EventChainFailure(
                        "unknown event hook failed during recovery",
                        aggregate_type=aggregate_type,
                        aggregate_id=aggregate_id,
                    ) from exc
                return state if reducer_result is None else reducer_result
            raise EventChainFailure(
                "event stream contains an event without a reducer",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )
        if not callable(reducer):
            raise EventChainFailure(
                "event reducer is not callable",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )
        try:
            return reducer(state, event)
        except RecoveryFailure:
            raise
        except BaseException as exc:
            raise EventChainFailure(
                "event reducer failed during recovery",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            ) from exc

    @staticmethod
    def _rebuild_projections(
        events: list[StoredEvent],
        projections: Mapping[str, ProjectionReducer | Mapping[str, ProjectionReducer]] | None,
        projection_states: Mapping[str, Any] | None,
        aggregate_type: str,
        aggregate_id: str,
    ) -> dict[str, Any]:
        if projections is None:
            return {}
        states = dict(projection_states or {})
        for name, projection in projections.items():
            if not callable(projection) and not isinstance(projection, Mapping):
                raise TypeError(f"projection {name!r} must be callable or a mapping")
            state = states.get(name)
            for event in events:
                handler = projection
                if isinstance(projection, Mapping):
                    handler = projection.get(event.event_type) or projection.get("*")
                    if handler is None:
                        continue
                if not callable(handler):
                    raise EventChainFailure(
                        "projection reducer is not callable",
                        aggregate_type=aggregate_type,
                        aggregate_id=aggregate_id,
                    )
                try:
                    state = handler(state, event)
                except RecoveryFailure:
                    raise
                except BaseException as exc:
                    raise EventChainFailure(
                        "projection rebuild failed during recovery",
                        aggregate_type=aggregate_type,
                        aggregate_id=aggregate_id,
                    ) from exc
            states[name] = state
        return states

    @staticmethod
    def _validate_budget(
        replay_limit: int | None,
        event_count: int,
        aggregate_type: str,
        aggregate_id: str,
    ) -> None:
        if replay_limit is not None and event_count > replay_limit:
            raise BudgetInvariantFailure(
                "recovery replay budget exceeded",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )

    @staticmethod
    def _validate_budget_hook(
        budget: int | Mapping[str, Any] | Hook | None,
        events: list[StoredEvent],
        aggregate_type: str,
        aggregate_id: str,
    ) -> None:
        if budget is None or isinstance(budget, (int, Mapping)):
            return
        try:
            accepted = _invoke_hook(budget, (events,), (len(events),))
        except BaseException as exc:
            raise BudgetInvariantFailure(
                "recovery budget hook failed",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            ) from exc
        if accepted is False:
            raise BudgetInvariantFailure(
                "recovery budget hook rejected replay",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )

    @staticmethod
    def _validate_security(
        security_hook: Hook | None,
        events: list[StoredEvent],
        aggregate_type: str,
        aggregate_id: str,
    ) -> None:
        if security_hook is None:
            return
        try:
            accepted = _invoke_hook(security_hook, (events,), (aggregate_type, aggregate_id, events))
        except BaseException as exc:
            raise SecurityInvariantFailure(
                "recovery security hook failed",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            ) from exc
        if accepted is False:
            raise SecurityInvariantFailure(
                "recovery security hook rejected event stream",
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )


def recover(
    event_store: SQLiteEventStore,
    aggregate_type: str,
    aggregate_id: str,
    reducers: Mapping[str, Reducer] | Reducer,
    *,
    snapshot_store: SnapshotStore | None = None,
    **kwargs: Any,
) -> RecoveryResult:
    """Convenience function for one-shot recovery."""

    return RecoveryBootstrap(event_store, snapshot_store).recover(
        aggregate_type, aggregate_id, reducers, **kwargs
    )


bootstrap_recovery = recover
recover_aggregate = recover


def _validate_aggregate_identity(aggregate_type: Any, aggregate_id: Any) -> tuple[str, str]:
    if not isinstance(aggregate_type, str) or not aggregate_type.strip():
        raise ValueError("aggregate_type must be a non-blank string")
    if not isinstance(aggregate_id, str) or not aggregate_id.strip():
        raise ValueError("aggregate_id must be a non-blank string")
    return aggregate_type, aggregate_id


def _validate_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _resolve_replay_limit(
    max_replay_events: int | None,
    budget: int | Mapping[str, Any] | Hook | None,
) -> int | None:
    if max_replay_events is not None:
        if isinstance(max_replay_events, bool) or not isinstance(max_replay_events, int):
            raise ValueError("max_replay_events must be a non-negative integer")
        if max_replay_events < 0:
            raise ValueError("max_replay_events must be a non-negative integer")
    limit = max_replay_events
    if isinstance(budget, int) and not isinstance(budget, bool):
        if budget < 0:
            raise ValueError("budget must be a non-negative integer")
        limit = budget if limit is None else min(limit, budget)
    elif isinstance(budget, Mapping):
        for key in ("max_replay_events", "max_events", "replay_limit"):
            if key in budget:
                candidate = budget[key]
                if isinstance(candidate, bool) or not isinstance(candidate, int) or candidate < 0:
                    raise ValueError("budget replay limit must be a non-negative integer")
                limit = candidate if limit is None else min(limit, candidate)
                break
    return limit


def _ensure_distinct_hooks(
    first: Hook | None,
    second: Hook | None,
    first_name: str,
    second_name: str,
) -> None:
    if first is not None and second is not None:
        raise ValueError(f"provide only one of {first_name} and {second_name}")


def _invoke_hook(hook: Hook, *candidates: tuple[Any, ...]) -> Any:
    """Call a hook using the first signature it explicitly supports."""

    try:
        signature = inspect.signature(hook)
    except (TypeError, ValueError):
        return hook(*candidates[0])
    for args in candidates:
        try:
            signature.bind(*args)
        except TypeError:
            continue
        return hook(*args)
    return hook(*candidates[0])


def _event_findings(
    events: Iterable[StoredEvent], event_types: set[str], payload_key: str
) -> tuple[str, ...]:
    findings: list[str] = []
    for event in events:
        if event.event_type not in event_types:
            continue
        candidate = event.payload.get(payload_key)
        if isinstance(candidate, str) and candidate.strip():
            findings.append(candidate)
        else:
            findings.append(event.event_id)
    return tuple(findings)


def _merge_hook_findings(
    existing: tuple[str, ...],
    hook: Hook | None,
    state: Any,
    events: list[StoredEvent],
    focused_events: list[StoredEvent],
    aggregate_type: str,
    aggregate_id: str,
    *,
    invariant: str,
) -> tuple[Any, ...]:
    if hook is None:
        return existing
    try:
        found = _invoke_hook(
            hook,
            *_finding_hook_candidates(
                hook,
                state,
                focused_events,
                events,
                aggregate_type,
                aggregate_id,
            ),
        )
    except BaseException as exc:
        failure_type = SecurityInvariantFailure if invariant == "security" else EventChainFailure
        raise failure_type(
            "recovery invariant hook failed",
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
        ) from exc
    if found is None or found is False:
        return existing
    if isinstance(found, str):
        additions = (found,)
    else:
        try:
            additions = tuple(found)
        except TypeError:
            additions = (found,)
    return existing + additions


def _finding_hook_candidates(
    hook: Hook,
    state: Any,
    focused_events: list[StoredEvent],
    events: list[StoredEvent],
    aggregate_type: str,
    aggregate_id: str,
) -> tuple[tuple[Any, ...], ...]:
    """Prefer findings for explicitly event-oriented one-argument hooks."""

    try:
        parameters = list(inspect.signature(hook).parameters.values())
    except (TypeError, ValueError):
        parameters = []
    first_name = parameters[0].name.lower() if parameters else ""
    wants_findings = any(
        marker in first_name
        for marker in ("event", "effect", "lease", "finding", "outcome")
    )
    if wants_findings:
        return (
            (focused_events,),
            (state, focused_events),
            (state, events),
            (aggregate_type, aggregate_id, state),
            (aggregate_type, aggregate_id, state, events),
            (state,),
        )
    return (
        (state,),
        (state, focused_events),
        (state, events),
        (aggregate_type, aggregate_id, state),
        (aggregate_type, aggregate_id, state, events),
        (focused_events,),
    )


__all__ = [
    "BudgetFailure",
    "BudgetInvariantFailure",
    "EventChainFailure",
    "EventStreamIntegrityFailure",
    "RecoveryBootstrap",
    "RecoveryFailure",
    "RecoveryResult",
    "SecurityFailure",
    "SecurityInvariantFailure",
    "bootstrap_recovery",
    "recover",
    "recover_aggregate",
]
