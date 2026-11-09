"""Durable v2 metadata registry. This module grants no launch authority.

Only registration, intent persistence and closure are implemented. ACK, stop
and seal remain unavailable until an independently owned verifier is wired.
"""

from dataclasses import asdict, replace
import hashlib
import json
import re

from orchestrator.persistence import EventDraft, SQLiteEventStore
from .mediation_actors import ActorIntent, MediationActor, MediationOperation, MediationState

STREAM = "mediation_v2"
SCHEMA_VERSION = 2
MAX_ACTORS = 128
MAX_EVENTS = MAX_ACTORS + 2
MAX_RECORD_BYTES = 16384
MAX_HISTORY_BYTES = 262144
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class MediationJournalError(RuntimeError):
    def __init__(self):
        super().__init__("mediation_journal_invalid")


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("ascii")


def _hash(value):
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _identifier(value):
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise ValueError()


def _digest(value):
    if type(value) is not str or not _DIGEST.fullmatch(value):
        raise ValueError()


def _positive(value):
    if type(value) is not int or value < 1 or value > 2**63 - 1:
        raise ValueError()


def _operation(value):
    if type(value) is not MediationOperation:
        raise ValueError()
    for item in (value.operation_id, value.run_id, value.node_id, value.attempt_id):
        _identifier(item)
    _positive(value.fencing_generation)
    _digest(value.commitment_sha256)
    return value


def _intent(value, operation):
    if type(value) is not ActorIntent:
        raise ValueError()
    for item in (value.operation_id, value.actor_id, value.kind, value.unit_name):
        _identifier(item)
    _positive(value.fencing_generation)
    _positive(value.sequence)
    _digest(value.intent_sha256)
    if value.operation_id != operation.operation_id or value.fencing_generation != operation.fencing_generation:
        raise ValueError()
    return value


def _decode(cls, data):
    if not isinstance(data, dict):
        raise ValueError()
    return cls(**data)


def _apply(state, action, data):
    if action == "Registered":
        if state is not None:
            raise ValueError()
        return MediationState(_operation(_decode(MediationOperation, data)))
    if state is None:
        raise ValueError()
    if action == "Permitted":
        actor = _intent(_decode(ActorIntent, data), state.operation)
        if state.closed or len(state.actors) >= MAX_ACTORS:
            raise ValueError()
        if any(a.intent.actor_id == actor.actor_id or a.intent.sequence == actor.sequence for a in state.actors):
            raise ValueError()
        return replace(state, actors=state.actors + (MediationActor(actor),), version=state.version + 1)
    if action == "Closed" and data == {} and not state.closed:
        return replace(state, closed=True, version=state.version + 1)
    raise ValueError()


def _replay(operation_id, events, version):
    if version != len(events) or len(events) > MAX_EVENTS:
        raise ValueError()
    state = None
    size = 0
    keys = set()
    for index, event in enumerate(events, 1):
        payload = event.payload
        if not isinstance(payload, dict) or set(payload) != {"schema_version", "operation_id", "action", "data"}:
            raise ValueError()
        if type(payload["schema_version"]) is not int or payload["schema_version"] != SCHEMA_VERSION:
            raise ValueError()
        if payload["operation_id"] != operation_id or event.stream_version != index:
            raise ValueError()
        action = payload["action"]
        if type(action) is not str or event.event_type != "Mediation" + action:
            raise ValueError()
        raw = _canonical(payload)
        size += len(raw)
        key = _hash(payload)
        if len(raw) > MAX_RECORD_BYTES or size > MAX_HISTORY_BYTES or event.idempotency_key != key or key in keys:
            raise ValueError()
        keys.add(key)
        state = _apply(state, action, payload["data"])
        op = state.operation
        if (op.operation_id != operation_id or event.run_id != op.run_id or event.node_id != op.node_id
                or event.attempt_id != op.attempt_id or event.fencing_generation != op.fencing_generation
                or event.correlation_id != op.run_id
                or event.causation_id != (_hash(events[index - 2].payload) if index > 1 else op.commitment_sha256)):
            raise ValueError()
    return state


class MediationJournal:
    def __init__(self, event_store):
        if type(event_store) is not SQLiteEventStore:
            raise MediationJournalError()
        self.event_store = event_store

    def load(self, operation_id):
        try:
            _identifier(operation_id)
            if self.event_store._connection.in_transaction and not self.event_store._has_owned_write():
                raise ValueError()
            events, version = self.event_store.read_stream_with_version(STREAM, operation_id, max_events=MAX_EVENTS)
            return _replay(operation_id, events, version)
        except Exception:
            raise MediationJournalError() from None

    def register(self, operation):
        try:
            _operation(operation)
            return self._append(operation.operation_id, "Registered", asdict(operation))
        except Exception:
            raise MediationJournalError() from None

    def permit(self, operation_id, actor_intent):
        try:
            if type(actor_intent) is not ActorIntent:
                raise ValueError()
            return self._append(operation_id, "Permitted", asdict(actor_intent))
        except Exception:
            raise MediationJournalError() from None

    def close(self, operation_id):
        try:
            return self._append(operation_id, "Closed", {})
        except Exception:
            raise MediationJournalError() from None

    def record_ack(self, operation_id, actor_id, ack):
        raise MediationJournalError()

    def record_stop(self, operation_id, actor_id, stop, transport_proof):
        raise MediationJournalError()

    def seal(self, operation_id):
        raise MediationJournalError()

    def _append(self, operation_id, action, data):
        _identifier(operation_id)
        store = self.event_store
        if store._connection.in_transaction and not store._has_owned_write():
            raise ValueError()
        payload = dict(schema_version=SCHEMA_VERSION, operation_id=operation_id, action=action, data=data)
        key = _hash(payload)
        result = None

        def decide(events, version):
            nonlocal result
            if not store._has_owned_write():
                raise ValueError()
            state = _replay(operation_id, events, version)
            existing = [event for event in events if event.idempotency_key == key]
            if existing:
                if len(existing) != 1 or _canonical(existing[0].payload) != _canonical(payload):
                    raise ValueError()
                result = state
                return None
            result = _apply(state, action, data)
            op = result.operation
            raw = _canonical(payload)
            if (op.operation_id != operation_id or len(events) >= MAX_EVENTS or len(raw) > MAX_RECORD_BYTES
                    or sum(len(_canonical(e.payload)) for e in events) + len(raw) > MAX_HISTORY_BYTES):
                raise ValueError()
            return [EventDraft("Mediation" + action, payload, run_id=op.run_id,
                node_id=op.node_id, attempt_id=op.attempt_id, fencing_generation=op.fencing_generation,
                correlation_id=op.run_id, causation_id=(_hash(events[-1].payload) if events else op.commitment_sha256))]

        store.append_checked(STREAM, operation_id, key, decide)
        return result
