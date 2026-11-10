"""SQLite-owned serialization for v1 Worker launch and release authority."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, TYPE_CHECKING

from orchestrator.persistence import SQLiteEventStore
from orchestrator.lifecycle.models import MAX_NODE_ATTEMPTS

if TYPE_CHECKING:
    from orchestrator.isolation import PreparedSandboxLaunch
    from .general_worker_journal import GeneralWorkerOperation


MAX_RELEASE_STREAMS = MAX_NODE_ATTEMPTS
MAX_RELEASE_ATTEMPTS = MAX_NODE_ATTEMPTS


def _check_scope_limit(scopes):
    if len(scopes) > MAX_RELEASE_STREAMS:
        raise ValueError("general worker scope exceeds release bound")


def _require_stopped_operations(operations):
    from .general_worker_journal import _require_worker_stopped
    for operation in operations:
        _require_worker_stopped(operation)


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

    def _v2_census(self, run_id: str, node_id: str, budget=None) -> bool:
        """Validate bounded v2 scope without authorizing or rejecting a safety fence."""
        self._owned()
        from orchestrator.lifecycle.cancel_completion import (CancellationBudget, CancellationHeld,
            read_event_window, read_run_catalog, read_version)
        from orchestrator.lifecycle.controller import LifecycleError, reduce_lifecycle
        from orchestrator.persistence.mediation_identity import mediation_stream_id
        if budget is None:
            budget = self._budget()
        events, version = read_event_window(self._store, "run_lifecycle", run_id, budget)
        try:
            state = reduce_lifecycle(run_id, events)
            attempts = state.node(node_id).attempts
        except (LifecycleError, ValueError, KeyError, TypeError):
            raise CancellationHeld("integrity") from None
        if state.event_version != version:
            raise CancellationHeld("integrity")
        if len(attempts) > min(MAX_RELEASE_ATTEMPTS, budget.max_nodes):
            raise CancellationHeld("budget")
        expected = {mediation_stream_id(run_id, node_id, a.attempt_id, a.fencing_generation):
                    (node_id, a.attempt_id, a.fencing_generation) for a in attempts}
        scopes, physical = read_run_catalog(self._store, run_id, budget, node_id=node_id,
                                            stream_types={"mediation_v2"})
        _check_scope_limit(scopes)
        scope_map = {}
        for kind, stream_id, node, attempt, generation in scopes:
            if expected.get(stream_id) != (node, attempt, generation):
                raise ValueError("mediation scope has no admitted attempt")
            if stream_id in scope_map:
                raise CancellationHeld("integrity")
            scope_map[stream_id] = (node, attempt, generation)
        physical_map = {}
        for kind, stream_id, node, attempt, generation, seq in physical:
            if scope_map.get(stream_id) != (node, attempt, generation):
                raise CancellationHeld("integrity")
            physical_map.setdefault(stream_id, []).append(seq)
        if set(scope_map) != set(physical_map):
            raise CancellationHeld("integrity")
        for stream_id in expected:
            value = read_version(self._store, "mediation_v2", stream_id, budget)
            events, event_version = read_event_window(self._store, "mediation_v2", stream_id, budget)
            present = value is not None or bool(events)
            if present != (stream_id in scope_map):
                raise CancellationHeld("integrity")
            if present and (value != event_version or sorted(physical_map[stream_id]) != list(range(1, value + 1))):
                raise CancellationHeld("integrity")
            if present:
                from .mediation_journal import _replay
                try:
                    if _replay(stream_id, events, event_version) is None:
                        raise ValueError()
                except (ValueError, TypeError, KeyError):
                    raise CancellationHeld("integrity") from None
        return bool(scope_map)

    @staticmethod
    def _budget():
        from orchestrator.lifecycle.cancel_completion import CancellationBudget
        return CancellationBudget(max_rows=4000, max_bytes=4 * 1024 * 1024,
            max_row_bytes=65536, max_streams=300, max_nodes=100)

    def _check_no_v2(self, run_id: str, node_id: str, budget=None) -> None:
        if self._v2_census(run_id, node_id, budget):
            raise ValueError("mediation operation is unresolved")

    def check_v2_admission(self, operation) -> None:
        """Recheck identity, lease and frozen binding; this grants no execution authority."""
        self._owned()
        from orchestrator.scheduler.core import Scheduler, ConcurrencyLimits
        scheduler = Scheduler(self._store, limits=ConcurrencyLimits(
            system_active_attempts=1, run_active_attempts=1,
            provider_active_attempts=1, tool_active_attempts=1))
        binding = scheduler._resolve_general_verification_binding(
            run_id=operation.run_id, node_id=operation.node_id,
            attempt_id=operation.attempt_id, fencing_generation=operation.fencing_generation,
            as_of=datetime.now(timezone.utc))
        context = binding.context
        if (context.run_id != operation.run_id or context.node_id != operation.node_id
                or context.attempt_id != operation.attempt_id
                or context.fencing_generation != operation.fencing_generation):
            raise ValueError("mediation admission identity mismatch")
    def _v1_operations(self, run_id, node_id, budget):
        from orchestrator.lifecycle.cancel_completion import (CancellationHeld, read_event_window,
            read_run_catalog, read_version, read_scheduler_release, validate_v1_links, validate_v1_terminal)
        from orchestrator.lifecycle.controller import LifecycleError, reduce_lifecycle
        from .general_worker_journal import GeneralWorkerIdentity, _replay, _require_worker_stopped
        events, version = read_event_window(self._store, "run_lifecycle", run_id, budget)
        try:
            state = reduce_lifecycle(run_id, events)
            attempts = state.node(node_id).attempts
        except (LifecycleError, ValueError, TypeError, KeyError):
            raise CancellationHeld("integrity") from None
        if state.event_version != version:
            raise CancellationHeld("integrity")
        if len(attempts) > min(MAX_RELEASE_ATTEMPTS, budget.max_nodes):
            raise CancellationHeld("budget")
        expected = {}
        for item in attempts:
            identity = GeneralWorkerIdentity(run_id, node_id, item.attempt_id, item.fencing_generation)
            expected[identity.stream_id] = identity
        scopes, physical = read_run_catalog(self._store, run_id, budget, node_id=node_id,
                                            stream_types={"general_worker"})
        _check_scope_limit(scopes)
        scope_map = {}
        for kind, stream_id, node, attempt, generation in scopes:
            identity = expected.get(stream_id)
            if (identity is None or stream_id in scope_map or
                (node, attempt, generation) != (identity.node_id, identity.attempt_id, identity.fencing_generation)):
                raise CancellationHeld("integrity")
            scope_map[stream_id] = (node, attempt, generation)
        physical_map = {}
        for kind, stream_id, node, attempt, generation, seq in physical:
            if scope_map.get(stream_id) != (node, attempt, generation):
                raise ValueError("general worker scope hides or disagrees with history")
            physical_map.setdefault(stream_id, []).append(seq)
        if set(scope_map) != set(physical_map):
            raise CancellationHeld("integrity")
        result = []
        for stream_id, identity in expected.items():
            value = read_version(self._store, "general_worker", stream_id, budget)
            events, event_version = read_event_window(self._store, "general_worker", stream_id, budget)
            present = value is not None or bool(events)
            if present != (stream_id in scope_map):
                raise CancellationHeld("integrity")
            if not present:
                continue
            if value != event_version or sorted(physical_map[stream_id]) != list(range(1, value + 1)):
                raise CancellationHeld("integrity")
            try:
                op = _replay(identity, events, event_version)
                if op is None:
                    raise ValueError()
                validate_v1_links(self._store, op, budget, allow_uncaptured_call=True)
                releases = read_scheduler_release(self._store, run_id, node_id, identity.attempt_id,
                                                  identity.fencing_generation, budget)
                if op.phase == "accepted":
                    validate_v1_terminal(self._store, op, budget)
                elif releases:
                    if any(x.payload.get("outcome") == "succeeded" for x in releases):
                        raise ValueError()
                    _require_worker_stopped(op)
            except CancellationHeld:
                raise
            except (ValueError, TypeError, KeyError):
                raise CancellationHeld("integrity") from None
            result.append((op, releases))
        return result

    def check_v2_against_v1(self, run_id: str, node_id: str) -> None:
        self._owned()
        from .general_worker_journal import _require_worker_stopped
        for operation, releases in self._v1_operations(run_id, node_id, self._budget()):
            _require_worker_stopped(operation)
            if not releases:
                raise ValueError("general worker operation is unresolved")

    def check_v1_prepare(self, operation: GeneralWorkerOperation,
        phase: Literal["registration", "request", "complete"],
        prepared: PreparedSandboxLaunch | None) -> None:
        self._owned()
        budget = self._budget()
        self._check_no_v2(operation.identity.run_id, operation.identity.node_id, budget)
        self._v1_operations(operation.identity.run_id, operation.identity.node_id, budget)
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
        budget = self._budget()
        self._check_no_v2(run_id, node_id, budget)
        self._write_token = self._store._owned_write_token()
        self._now = datetime.now(timezone.utc)
        operations = self._v1_operations(run_id, node_id, budget)
        _require_stopped_operations(operation for operation, _ in operations)

    def _check_release_snapshot(self, scheduler, *, run_id, node_id, attempt_id, fencing_generation):
        from orchestrator.scheduler.core import Scheduler
        self._owned()
        if (type(scheduler) is not Scheduler or scheduler.event_store is not self._store
            or self._write_token is not self._store._owned_write_token() or self._now is None):
            raise ValueError("release gate belongs to another transaction")
        scheduler._resolve_general_release_snapshot(run_id=run_id, node_id=node_id,
            attempt_id=attempt_id, fencing_generation=fencing_generation, as_of=self._now)
