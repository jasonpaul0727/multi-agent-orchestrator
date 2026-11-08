"""SQLite-owned serialization for v1 Worker launch and release authority."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, TYPE_CHECKING

from orchestrator.persistence import SQLiteEventStore

if TYPE_CHECKING:
    from orchestrator.isolation import PreparedSandboxLaunch
    from .general_worker_journal import GeneralWorkerOperation


class GeneralWorkerGate:
    def __init__(self, event_store: SQLiteEventStore):
        if type(event_store) is not SQLiteEventStore:
            raise ValueError()
        self._store = event_store
        self._scope = None
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
        now = datetime.now(timezone.utc)
        if now.utcoffset() != timezone.utc.utcoffset(now):
            raise ValueError()
        from .general_worker_journal import GeneralWorkerIdentity, GeneralWorkerJournal, _require_worker_stopped
        journal = GeneralWorkerJournal(self._store)
        for stream_id in self._store.stream_ids("general_worker"):
            events = self._store.read_stream("general_worker", stream_id)
            if not events:
                raise ValueError()
            first = events[0]
            if first.run_id == run_id and first.node_id == node_id:
                identity = GeneralWorkerIdentity(first.run_id, first.node_id, first.attempt_id, first.fencing_generation)
                operation = journal.read(identity)
                if operation is None:
                    raise ValueError()
                _require_worker_stopped(operation)
