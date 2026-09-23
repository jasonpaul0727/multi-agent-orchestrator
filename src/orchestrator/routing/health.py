"""Event-sourced provider/model health circuits and atomic probe leases."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.config.effective import HealthPolicy, HealthState
from orchestrator.persistence import EventDraft, SQLiteEventStore


_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_HEALTH_STREAM = "provider_model_health"
_COORDINATOR_STREAM = "health_coordinator"
_COORDINATOR_ID = "global"


def _hash(value: object) -> str:
    data = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(data.encode("utf-8")).hexdigest()


def _require_aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a UTC offset")
    return value.astimezone(timezone.utc)


class _HealthModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class HealthAggregateKey(_HealthModel):
    """Stable health stream identity tied to one immutable registry."""

    registry_manifest_hash: StrictStr = Field(pattern=_HASH.pattern)
    scope: Literal["provider", "model"]
    provider_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    model_id: StrictStr | None = Field(default=None, pattern=_IDENTIFIER.pattern)

    @model_validator(mode="after")
    def validate_scope(self) -> "HealthAggregateKey":
        if (self.scope == "provider") != (self.model_id is None):
            raise ValueError("provider health keys omit model_id; model keys require it")
        return self

    @property
    def aggregate_id(self) -> str:
        return _hash(self.model_dump(mode="json"))


class HealthVersionRef(_HealthModel):
    aggregate_id: StrictStr = Field(pattern=_HASH.pattern)
    version: StrictInt = Field(gt=0)
    generation: StrictInt = Field(gt=0)


class HealthAggregateRef(_HealthModel):
    aggregate_id: StrictStr = Field(pattern=_HASH.pattern)
    version: StrictInt = Field(ge=0)
    generation: StrictInt = Field(ge=0)
    state: HealthState


class ProbeLease(_HealthModel):
    """One-use probe permit covering every open aggregate in a probe group."""

    lease_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    holder_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    registry_manifest_hash: StrictStr = Field(pattern=_HASH.pattern)
    provider_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    expires_at: datetime
    aggregates: tuple[HealthVersionRef, ...] = Field(min_length=1, max_length=2)
    lease_hash: StrictStr = Field(pattern=_HASH.pattern)

    @field_validator("expires_at")
    @classmethod
    def normalize_expiration(cls, value: datetime) -> datetime:
        return _require_aware(value, "expires_at")

    @field_validator("aggregates", mode="before")
    @classmethod
    def normalize_aggregates(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("aggregates must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def validate_lease(self) -> "ProbeLease":
        ids = [item.aggregate_id for item in self.aggregates]
        if len(ids) != len(set(ids)):
            raise ValueError("probe lease aggregate references must be unique")
        payload = self.model_dump(mode="json", exclude={"lease_hash"})
        if self.lease_hash != _hash(payload):
            raise ValueError("probe lease hash does not match its contents")
        return self

    @classmethod
    def create(
        cls,
        *,
        lease_id: str,
        holder_id: str,
        registry_manifest_hash: str,
        provider_id: str,
        model_id: str,
        expires_at: datetime,
        aggregates: tuple[HealthVersionRef, ...],
    ) -> "ProbeLease":
        payload = {
            "lease_id": lease_id,
            "holder_id": holder_id,
            "registry_manifest_hash": registry_manifest_hash,
            "provider_id": provider_id,
            "model_id": model_id,
            "expires_at": expires_at.astimezone(timezone.utc),
            "aggregates": aggregates,
        }
        draft = cls.model_construct(**payload, lease_hash="sha256:" + "0" * 64)
        serialized = draft.model_dump(mode="json", exclude={"lease_hash"})
        return cls(**payload, lease_hash=_hash(serialized))


class HealthAggregateState(_HealthModel):
    key: HealthAggregateKey
    version: StrictInt = Field(ge=0)
    generation: StrictInt = Field(ge=0)
    state: HealthState
    health_policy: HealthPolicy
    last_event_time: datetime | None = None
    transient_failure_times: tuple[datetime, ...] = ()
    consecutive_successes: StrictInt = Field(ge=0)
    cooldown_until: datetime | None = None
    active_probe_lease_id: StrictStr | None = None

    @field_validator("last_event_time", "cooldown_until")
    @classmethod
    def normalize_optional_times(cls, value: datetime | None, info: object) -> datetime | None:
        return None if value is None else _require_aware(value, getattr(info, "field_name", "timestamp"))

    @field_validator("transient_failure_times", mode="before")
    @classmethod
    def normalize_failure_times(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("transient_failure_times must be an array")
        return tuple(value)

    @property
    def snapshot_ref(self) -> dict[str, object]:
        return {
            "aggregate_id": self.key.aggregate_id,
            "version": self.version,
            "generation": self.generation,
            "state": self.state,
        }

    @property
    def routing_ref(self) -> HealthAggregateRef:
        return HealthAggregateRef(
            aggregate_id=self.key.aggregate_id,
            version=self.version,
            generation=self.generation,
            state=self.state,
        )


class HealthCircuitError(RuntimeError):
    """A stale, ineligible, or conflicting health-circuit operation."""


class ProbeLeaseConflict(HealthCircuitError):
    """Another active lease or a stale aggregate prevents a probe group CAS."""


def initial_health_state(key: HealthAggregateKey, policy: HealthPolicy) -> HealthAggregateState:
    return HealthAggregateState(
        key=key,
        version=0,
        generation=0,
        state="healthy",
        health_policy=policy,
        last_event_time=None,
        transient_failure_times=(),
        consecutive_successes=0,
        cooldown_until=None,
        active_probe_lease_id=None,
    )


def reduce_health_events(
    key: HealthAggregateKey,
    events: list[object],
    *,
    default_policy: HealthPolicy,
) -> HealthAggregateState:
    """Replay health events using only their recorded timestamps and policies."""

    state = initial_health_state(key, default_policy)
    for event in events:
        if getattr(event, "stream_type", None) != _HEALTH_STREAM or getattr(event, "stream_id", None) != key.aggregate_id:
            raise HealthCircuitError("health event belongs to a different aggregate")
        payload = event.payload
        try:
            event_time = _require_aware(datetime.fromisoformat(payload["event_time"]), "event_time")
            event_policy = HealthPolicy.model_validate(payload["health_policy"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HealthCircuitError("health event is missing replayable state inputs") from exc
        if state.last_event_time is not None and event_time < state.last_event_time:
            raise HealthCircuitError("health event time must be monotonic")
        if event_policy.id != state.health_policy.id and state.version > 0:
            # Policy changes are allowed only at event boundaries; the event
            # embeds the new immutable policy snapshot for replay.
            pass
        times = tuple(
            at for at in state.transient_failure_times
            if event_time - at <= timedelta(milliseconds=event_policy.failure_window_ms)
        )
        current = state.state
        generation = state.generation
        successes = state.consecutive_successes
        cooldown = state.cooldown_until
        active_lease = state.active_probe_lease_id

        if event.event_type == "HealthCallFailure":
            retry_after_ms = payload.get("retry_after_ms")
            if payload.get("failure_category") == "transient":
                times = (*times, event_time)
                successes = 0
                if current != "half_open" and (
                    retry_after_ms is not None or len(times) >= event_policy.open_after
                ):
                    if current != "open":
                        generation += 1
                    current = "open"
                    delay_ms = max(event_policy.cooldown_ms, retry_after_ms or 0)
                    cooldown = event_time + timedelta(milliseconds=delay_ms)
                elif current == "healthy" and len(times) >= event_policy.degrade_after:
                    current = "degraded"
                    generation += 1
            elif current == "degraded":
                successes = 0
        elif event.event_type == "HealthCallSuccess":
            if current == "degraded":
                successes += 1
                if successes >= event_policy.recovery_successes:
                    current = "healthy"
                    generation += 1
                    times = ()
                    cooldown = None
                    successes = 0
            elif current == "healthy":
                times = ()
                successes = 0
        elif event.event_type == "HealthProbeLeaseGranted":
            if current != "open" or cooldown is None or event_time < cooldown:
                raise HealthCircuitError("probe lease requires an open circuit past cooldown")
            lease_id = payload.get("lease_id")
            if not isinstance(lease_id, str) or not lease_id:
                raise HealthCircuitError("probe lease event requires a lease_id")
            if active_lease is not None:
                raise HealthCircuitError("health aggregate already has an active probe lease")
            current = "half_open"
            active_lease = lease_id
            generation += 1
        elif event.event_type in {"HealthProbeSucceeded", "HealthProbeFailed", "HealthProbeExpired"}:
            lease_id = payload.get("lease_id")
            if current != "half_open" or not active_lease or lease_id != active_lease:
                raise HealthCircuitError("probe result is not bound to the active lease")
            generation += 1
            active_lease = None
            successes = 0
            if event.event_type == "HealthProbeSucceeded":
                current = "healthy"
                times = ()
                cooldown = None
            else:
                current = "open"
                cooldown = event_time + timedelta(milliseconds=event_policy.cooldown_ms)
        else:
            raise HealthCircuitError("unsupported health event type")

        state = HealthAggregateState(
            key=key,
            version=event.stream_version,
            generation=generation,
            state=current,
            health_policy=event_policy,
            last_event_time=event_time,
            transient_failure_times=times,
            consecutive_successes=successes,
            cooldown_until=cooldown,
            active_probe_lease_id=active_lease,
        )
    return state


def effective_health_state(provider: HealthState, model: HealthState) -> HealthState:
    """Combine provider and model circuits in strict fail-closed precedence."""

    for state in ("open", "half_open", "degraded"):
        if provider == state or model == state:
            return state  # type: ignore[return-value]
    return "healthy"


class HealthController:
    """Durable health aggregation on the event store; no local counters/clocks."""

    def __init__(self, store: SQLiteEventStore) -> None:
        self.store = store

    def read(self, key: HealthAggregateKey, *, default_policy: HealthPolicy) -> HealthAggregateState:
        events, version = self.store.read_stream_with_version(_HEALTH_STREAM, key.aggregate_id)
        state = reduce_health_events(key, events, default_policy=default_policy)
        if version != state.version:
            raise HealthCircuitError("health stream version disagrees with replayed state")
        return state

    def record_outcome(
        self,
        *,
        provider_key: HealthAggregateKey,
        model_key: HealthAggregateKey,
        policy: HealthPolicy,
        event_time: datetime,
        idempotency_key: str,
        success: bool,
        failure_category: Literal["transient", "permanent"] = "transient",
        retry_after_ms: int | None = None,
    ) -> tuple[HealthAggregateState, HealthAggregateState]:
        event_time = _require_aware(event_time, "event_time")
        if provider_key.scope != "provider" or model_key.scope != "model":
            raise ValueError("call outcome requires provider and model health keys")
        if (
            provider_key.registry_manifest_hash != model_key.registry_manifest_hash
            or provider_key.provider_id != model_key.provider_id
        ):
            raise ValueError("provider and model health keys must share a registry and provider")
        if success and retry_after_ms is not None:
            raise ValueError("successful outcomes cannot carry retry_after_ms")
        if retry_after_ms is not None and retry_after_ms < 0:
            raise ValueError("retry_after_ms must be non-negative")
        keys = (provider_key, model_key)

        coordinator_key = f"health-outcome:{idempotency_key}"

        def append_group(events: list[object], version: int) -> list[EventDraft] | None:
            prior = next((event for event in events if event.idempotency_key == coordinator_key), None)
            if prior is not None:
                expected = {
                    "provider_aggregate_id": provider_key.aggregate_id,
                    "model_aggregate_id": model_key.aggregate_id,
                    "event_time": event_time.isoformat(),
                    "success": success,
                    "failure_category": failure_category,
                    "retry_after_ms": retry_after_ms,
                }
                if prior.event_type != "HealthOutcomeRecorded" or any(
                    prior.payload.get(name) != value for name, value in expected.items()
                ):
                    raise HealthCircuitError("health outcome idempotency key was reused for different input")
                return None
            for key in keys:
                prior = self.store.read_stream(_HEALTH_STREAM, key.aggregate_id)
                current = reduce_health_events(key, prior, default_policy=policy)
                event_type = "HealthCallSuccess" if success else "HealthCallFailure"
                payload: dict[str, object] = {
                    "event_time": event_time.isoformat(),
                    "health_policy": policy.model_dump(mode="json"),
                }
                if not success:
                    payload["failure_category"] = failure_category
                    payload["retry_after_ms"] = retry_after_ms
                draft = EventDraft(event_type, payload)
                self.store.append(
                    _HEALTH_STREAM,
                    key.aggregate_id,
                    current.version,
                    [draft],
                    f"{idempotency_key}:{key.scope}:{key.model_id or key.provider_id}",
                )
            return [
                EventDraft(
                    "HealthOutcomeRecorded",
                    {
                        "idempotency_key": idempotency_key,
                        "provider_aggregate_id": provider_key.aggregate_id,
                        "model_aggregate_id": model_key.aggregate_id,
                        "event_time": event_time.isoformat(),
                        "success": success,
                        "failure_category": failure_category,
                        "retry_after_ms": retry_after_ms,
                    },
                )
            ]

        self.store.append_checked(
            _COORDINATOR_STREAM,
            _COORDINATOR_ID,
            coordinator_key,
            append_group,
        )
        return (
            self.read(provider_key, default_policy=policy),
            self.read(model_key, default_policy=policy),
        )

    def acquire_probe_lease(
        self,
        *,
        provider_key: HealthAggregateKey,
        model_key: HealthAggregateKey,
        policy: HealthPolicy,
        event_time: datetime,
        lease_id: str,
        holder_id: str,
        expires_at: datetime,
        idempotency_key: str,
    ) -> ProbeLease:
        """Atomically transition all open target aggregates to half_open."""

        event_time = _require_aware(event_time, "event_time")
        expires_at = _require_aware(expires_at, "expires_at")
        if expires_at <= event_time:
            raise ValueError("probe lease must expire after acquisition event time")
        if provider_key.scope != "provider" or model_key.scope != "model":
            raise ValueError("probe group requires provider and model health keys")
        if (
            provider_key.registry_manifest_hash != model_key.registry_manifest_hash
            or provider_key.provider_id != model_key.provider_id
            or model_key.model_id is None
        ):
            raise ValueError("probe group keys must target the same model provider and registry")
        # Event-time expiry is recorded before any new CAS. No wall clock is
        # read, and expired groups cannot be silently replaced by another lease.
        self.expire_probe_leases(event_time=event_time)
        created: list[ProbeLease] = []

        def acquire(events: list[object], version: int) -> list[EventDraft] | None:
            prior = next((event for event in events if event.idempotency_key == idempotency_key), None)
            if prior is not None:
                stored = prior.payload.get("lease")
                if stored is not None:
                    prior_lease = ProbeLease.model_validate_json(json.dumps(stored))
                    if (
                        prior_lease.lease_id != lease_id
                        or prior_lease.holder_id != holder_id
                        or prior_lease.registry_manifest_hash != provider_key.registry_manifest_hash
                        or prior_lease.provider_id != provider_key.provider_id
                        or prior_lease.model_id != model_key.model_id
                        or prior_lease.expires_at != expires_at
                        or prior.payload.get("event_time") != event_time.isoformat()
                    ):
                        raise ProbeLeaseConflict("probe acquisition idempotency key was reused")
                    created.append(prior_lease)
                    return None
                raise ProbeLeaseConflict("idempotency key already names a different health action")

            active_ids: set[str] = set()
            for event in events:
                if event.event_type == "HealthProbeLeaseGranted":
                    lease = ProbeLease.model_validate_json(json.dumps(event.payload["lease"]))
                    if not any(
                        done.event_type in {"HealthProbeLeaseSucceeded", "HealthProbeLeaseFailed", "HealthProbeLeaseExpired"}
                        and done.payload.get("lease_id") == lease.lease_id
                        for done in events
                    ):
                        active_ids.update(ref.aggregate_id for ref in lease.aggregates)

            states = [
                self.read(provider_key, default_policy=policy),
                self.read(model_key, default_policy=policy),
            ]
            if any(state.key.aggregate_id in active_ids for state in states):
                raise ProbeLeaseConflict("a related aggregate already belongs to an active probe lease")
            if any(state.state == "half_open" for state in states):
                raise ProbeLeaseConflict("a related aggregate is already half-open")
            open_states = [state for state in states if state.state == "open"]
            if not open_states:
                raise ProbeLeaseConflict("probe lease requires at least one open aggregate")
            if any(state.cooldown_until is None or event_time < state.cooldown_until for state in open_states):
                raise ProbeLeaseConflict("one or more open aggregates remain in cooldown")
            refs = tuple(
                HealthVersionRef(
                    aggregate_id=state.key.aggregate_id,
                    version=state.version + 1,
                    generation=state.generation + 1,
                )
                for state in sorted(open_states, key=lambda item: item.key.aggregate_id)
            )
            lease = ProbeLease.create(
                lease_id=lease_id,
                holder_id=holder_id,
                registry_manifest_hash=provider_key.registry_manifest_hash,
                provider_id=provider_key.provider_id,
                model_id=model_key.model_id,
                expires_at=expires_at,
                aggregates=refs,
            )
            for state in open_states:
                self.store.append(
                    _HEALTH_STREAM,
                    state.key.aggregate_id,
                    state.version,
                    [
                        EventDraft(
                            "HealthProbeLeaseGranted",
                            {
                                "event_time": event_time.isoformat(),
                                "health_policy": policy.model_dump(mode="json"),
                                "lease_id": lease.lease_id,
                                "lease_hash": lease.lease_hash,
                            },
                        )
                    ],
                    f"{idempotency_key}:lease:{state.key.aggregate_id}",
                )
            created.append(lease)
            return [
                EventDraft(
                    "HealthProbeLeaseGranted",
                    {
                        "event_time": event_time.isoformat(),
                        "lease": lease.model_dump(mode="json"),
                        "lease_id": lease.lease_id,
                        "lease_hash": lease.lease_hash,
                    },
                )
            ]

        self.store.append_checked(
            _COORDINATOR_STREAM,
            _COORDINATOR_ID,
            idempotency_key,
            acquire,
        )
        if not created:
            events = self.store.read_stream(_COORDINATOR_STREAM, _COORDINATOR_ID)
            prior = next(
                (event for event in events if event.idempotency_key == idempotency_key), None
            )
            if prior is not None and prior.payload.get("lease"):
                created.append(ProbeLease.model_validate_json(json.dumps(prior.payload["lease"])))
        if not created:
            raise ProbeLeaseConflict("probe lease was not committed")
        return created[-1]

    def expire_probe_leases(self, *, event_time: datetime) -> tuple[str, ...]:
        """Append expiry outcomes for due leases using recorded event time."""

        event_time = _require_aware(event_time, "event_time")
        events = self.store.read_stream(_COORDINATOR_STREAM, _COORDINATOR_ID)
        completed = {
            event.payload.get("lease_id")
            for event in events
            if event.event_type in {
                "HealthProbeLeaseSucceeded",
                "HealthProbeLeaseFailed",
                "HealthProbeLeaseExpired",
            }
        }
        due: list[ProbeLease] = []
        for event in events:
            if event.event_type != "HealthProbeLeaseGranted":
                continue
            lease = ProbeLease.model_validate_json(json.dumps(event.payload["lease"]))
            if lease.lease_id not in completed and event_time >= lease.expires_at:
                due.append(lease)
        expired_ids: list[str] = []
        for lease in due:
            key = _find_health_key(
                self.store,
                aggregate_id=lease.aggregates[0].aggregate_id,
                registry_manifest_hash=lease.registry_manifest_hash,
                provider_id=lease.provider_id,
                model_id=lease.model_id,
            )
            aggregate_events = self.store.read_stream(_HEALTH_STREAM, key.aggregate_id)
            if not aggregate_events:
                raise HealthCircuitError("probe lease references a missing health aggregate")
            recorded_policy = HealthPolicy.model_validate(
                aggregate_events[-1].payload["health_policy"]
            )
            current = self.read(key, default_policy=recorded_policy)
            self.finish_probe_lease(
                lease,
                policy=current.health_policy,
                event_time=event_time,
                success=False,
                idempotency_key=f"expire-probe:{lease.lease_id}",
                expired=True,
            )
            expired_ids.append(lease.lease_id)
        return tuple(expired_ids)

    def finish_probe_lease(
        self,
        lease: ProbeLease,
        *,
        policy: HealthPolicy,
        event_time: datetime,
        success: bool,
        idempotency_key: str,
        expired: bool = False,
    ) -> tuple[HealthAggregateState, ...]:
        """Complete or expire a lease with a cross-aggregate compare-and-swap."""

        event_time = _require_aware(event_time, "event_time")
        if expired and event_time < lease.expires_at:
            raise ValueError("cannot expire a probe lease before its recorded expiration")
        if success and event_time >= lease.expires_at:
            raise ProbeLeaseConflict("expired probe leases cannot succeed")
        states: list[HealthAggregateState] = []

        def complete(events: list[object], version: int) -> list[EventDraft] | None:
            expected_type = (
                "HealthProbeLeaseExpired" if expired
                else "HealthProbeLeaseSucceeded" if success
                else "HealthProbeLeaseFailed"
            )
            prior = next((event for event in events if event.idempotency_key == idempotency_key), None)
            if prior is not None:
                if (
                    prior.event_type != expected_type
                    or prior.payload.get("lease_id") != lease.lease_id
                    or prior.payload.get("lease_hash") != lease.lease_hash
                    or prior.payload.get("event_time") != event_time.isoformat()
                ):
                    raise ProbeLeaseConflict("probe completion idempotency key was reused")
                return None
            grant = next(
                (
                    event for event in events
                    if event.event_type == "HealthProbeLeaseGranted"
                    and event.payload.get("lease_id") == lease.lease_id
                    and event.payload.get("lease_hash") == lease.lease_hash
                ),
                None,
            )
            if grant is None:
                raise ProbeLeaseConflict("probe lease is not present in the durable coordinator stream")
            if any(
                event.payload.get("lease_id") == lease.lease_id
                and event.event_type in {"HealthProbeLeaseSucceeded", "HealthProbeLeaseFailed", "HealthProbeLeaseExpired"}
                for event in events
            ):
                raise ProbeLeaseConflict("probe lease has already been completed")
            result_type = expected_type
            aggregate_result_type = (
                "HealthProbeExpired" if expired
                else "HealthProbeSucceeded" if success
                else "HealthProbeFailed"
            )
            for reference in lease.aggregates:
                key = _find_health_key(
                    self.store,
                    aggregate_id=reference.aggregate_id,
                    registry_manifest_hash=lease.registry_manifest_hash,
                    provider_id=lease.provider_id,
                    model_id=lease.model_id,
                )
                state = self.read(key, default_policy=policy)
                if (
                    state.version != reference.version
                    or state.generation != reference.generation
                    or state.state != "half_open"
                    or state.active_probe_lease_id != lease.lease_id
                ):
                    raise ProbeLeaseConflict("probe lease aggregate compare-and-swap failed")
                self.store.append(
                    _HEALTH_STREAM,
                    state.key.aggregate_id,
                    state.version,
                    [
                        EventDraft(
                            aggregate_result_type,
                            {
                                "event_time": event_time.isoformat(),
                                "health_policy": policy.model_dump(mode="json"),
                                "lease_id": lease.lease_id,
                            },
                        )
                    ],
                    f"{idempotency_key}:result:{state.key.aggregate_id}",
                )
            return [
                EventDraft(
                    result_type,
                    {
                        "event_time": event_time.isoformat(),
                        "lease_id": lease.lease_id,
                        "lease_hash": lease.lease_hash,
                    },
                )
            ]

        self.store.append_checked(
            _COORDINATOR_STREAM,
            _COORDINATOR_ID,
            idempotency_key,
            complete,
        )
        for reference in lease.aggregates:
            key = _find_health_key(
                self.store,
                aggregate_id=reference.aggregate_id,
                registry_manifest_hash=lease.registry_manifest_hash,
                provider_id=lease.provider_id,
                model_id=lease.model_id,
            )
            states.append(self.read(key, default_policy=policy))
        return tuple(states)


def _find_health_key(
    store: SQLiteEventStore,
    *,
    aggregate_id: str,
    registry_manifest_hash: str,
    provider_id: str,
    model_id: str,
) -> HealthAggregateKey:
    for scope, candidate_model in (("provider", None), ("model", model_id)):
        key = HealthAggregateKey(
            registry_manifest_hash=registry_manifest_hash,
            scope=scope,
            provider_id=provider_id,
            model_id=candidate_model,
        )
        if key.aggregate_id == aggregate_id:
            return key
    raise HealthCircuitError("probe lease references an aggregate outside its provider/model scope")


__all__ = [
    "HealthAggregateRef",
    "HealthAggregateKey",
    "HealthAggregateState",
    "HealthCircuitError",
    "HealthController",
    "HealthVersionRef",
    "ProbeLease",
    "ProbeLeaseConflict",
    "effective_health_state",
    "initial_health_state",
    "reduce_health_events",
]
