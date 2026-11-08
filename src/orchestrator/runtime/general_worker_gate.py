"""SQLite-owned serialization for v1 Worker launch and release authority."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, TYPE_CHECKING

from orchestrator.persistence import SQLiteEventStore

if TYPE_CHECKING:
    from orchestrator.isolation import PreparedSandboxLaunch
    from .general_worker_journal import GeneralWorkerOperation


MAX_RELEASE_STREAMS = 64


class GeneralWorkerGate:
    def __init__(self, event_store: SQLiteEventStore):
        if type(event_store) is not SQLiteEventStore:
            raise ValueError()
        self._store = event_store
        self._scope = None
        self._now = None
        self._write_token = None
        if not event_store._connection.in_transaction:
            from .general_worker_authority import general_worker_host_scope
            self._scope = general_worker_host_scope(event_store)

    def _owned(self):
        if not self._store._has_owned_write():
            raise ValueError("owned write transaction required")

    def check_v1_prepare(self, operation: GeneralWorkerOperation,
        phase: Literal["registration", "request", "complete"],
        prepared: PreparedSandboxLaunch | None) -> None:
        self._owned()
        now = datetime.now(timezone.utc)
        from orchestrator.isolation import PreparedSandboxLaunch
        if (self._scope is None or type(phase) is not str
            or phase not in ("registration", "request", "complete")
            or (phase == "registration" and prepared is not None)
            or (phase != "registration" and type(prepared) is not PreparedSandboxLaunch)):
            raise ValueError()
        from orchestrator.scheduler.core import Scheduler, ConcurrencyLimits
        from .general_worker_authority import _validate_general_eligibility
        from .general_worker_journal import GeneralWorkerJournal
        limits = ConcurrencyLimits(system_active_attempts=1, run_active_attempts=1,
            provider_active_attempts=1, tool_active_attempts=1)
        scheduler = Scheduler(self._store, limits=limits)
        journal = GeneralWorkerJournal(self._store)
        events = self._store.read_stream("scheduler", "global")
        _validate_general_eligibility(scheduler, journal, operation,
            purpose="request" if phase in ("registration", "request") else "completion",
            host_scope_hash=self._scope, as_of=now, events=events)

    def check_release(self, run_id: str, node_id: str) -> None:
        self._owned()
        # Sample under ownership even though stop-proof classification itself
        # has no time-dependent branch.
        self._write_token = self._store._owned_write_token()
        self._now = datetime.now(timezone.utc)
        if self._now.utcoffset() != timezone.utc.utcoffset(self._now):
            raise ValueError()
        from orchestrator.lifecycle import LifecycleController
        from .general_worker_journal import (
            GeneralWorkerIdentity, GeneralWorkerJournal, MAX_EVENTS, _require_worker_stopped,
        )
        streams = self._store.stream_ids_for_worker_node(run_id, node_id, limit=MAX_RELEASE_STREAMS + 1)
        if len(streams) > MAX_RELEASE_STREAMS:
            raise ValueError("general worker scope exceeds release bound")
        # Known admitted identities also have deterministic stream IDs. Probe
        # them independently so corrupted scope metadata cannot hide them.
        attempts = LifecycleController(self._store).replay(run_id).node(node_id).attempts
        if len(attempts) > MAX_RELEASE_STREAMS:
            raise ValueError("attempt history exceeds release bound")
        indexed = set(streams)
        for attempt in attempts:
            identity = GeneralWorkerIdentity(run_id, node_id, attempt.attempt_id, attempt.fencing_generation)
            if self._store.current_version("general_worker", identity.stream_id) and identity.stream_id not in indexed:
                raise ValueError("general worker scope hides an admitted stream")
        journal = GeneralWorkerJournal(self._store)
        for stream_id in streams:
            version = self._store.current_version("general_worker", stream_id)
            if not 0 < version <= MAX_EVENTS:
                raise ValueError("invalid general worker stream length")
            events, _ = self._store.read_stream_with_version("general_worker", stream_id, max_events=MAX_EVENTS)
            if not events:
                raise ValueError()
            first = events[0]
            identity = GeneralWorkerIdentity(first.run_id, first.node_id, first.attempt_id, first.fencing_generation)
            if first.run_id != run_id or first.node_id != node_id or identity.stream_id != stream_id:
                raise ValueError("general worker scope disagrees with identity")
            operation = journal.read(identity)
            if operation is None:
                raise ValueError()
            _require_worker_stopped(operation)

    def _check_release_snapshot(self, scheduler, *, run_id, node_id, attempt_id, fencing_generation):
        from orchestrator.scheduler.core import Scheduler
        self._owned()
        if (type(scheduler) is not Scheduler or scheduler.event_store is not self._store
            or self._write_token is not self._store._owned_write_token() or self._now is None):
            raise ValueError("release gate belongs to another transaction")
        scheduler._resolve_general_release_snapshot(run_id=run_id, node_id=node_id,
            attempt_id=attempt_id, fencing_generation=fencing_generation, as_of=self._now)