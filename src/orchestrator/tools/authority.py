"""Durable, thread-safe authority check for isolated command gateways.

Each check opens its own SQLite connection and reads lifecycle, scheduler,
and Agent streams in one snapshot. This is intentionally an authority gate:
it does not make an ApprovalGrant or external effect safe to consume.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from orchestrator.agents.registry import AgentRegistry, reduce_agent_registry
from orchestrator.lifecycle.controller import reduce_lifecycle
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.workspace_identity import workspace_identity_hash

from .gateway import READ_ONLY_COMMAND_TOOL_ID, ToolRequest
from .workspace_write import WORKSPACE_WRITE_TOOL_ID


class DurableAttemptAuthority:
    """Check a tool request against one committed accepted-attempt snapshot.

    The database path, workspace, and frozen PolicyManifest hash must come
    from trusted host configuration, never from a Worker request. A fresh
    connection per call keeps the Gateway's monitor thread within SQLite's
    thread-affinity rules and prevents cached authorization after revocation.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        workspace: str | Path,
        policy_manifest_hash: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        database = Path(database_path)
        workspace_path = Path(workspace)
        if not database.is_absolute() or not database.is_file() or database.is_symlink():
            raise ValueError("authority database must be an existing absolute regular file")
        if not workspace_path.is_absolute() or not workspace_path.is_dir():
            raise ValueError("authority workspace must be an existing absolute directory")
        canonical_workspace = workspace_path.resolve(strict=True)
        if canonical_workspace == Path(canonical_workspace.anchor):
            raise ValueError("authority workspace must not be a filesystem root")
        if (
            not isinstance(policy_manifest_hash, str)
            or len(policy_manifest_hash) != 71
            or not policy_manifest_hash.startswith("sha256:")
            or any(char not in "0123456789abcdef" for char in policy_manifest_hash[7:])
        ):
            raise ValueError("policy_manifest_hash must be a SHA-256 digest")
        self._database = database.resolve(strict=True)
        self._workspace = str(canonical_workspace)
        self._workspace_identity_hash = workspace_identity_hash(canonical_workspace)
        self._policy_manifest_hash = policy_manifest_hash
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def is_current(self, request: ToolRequest) -> bool:
        """Fail closed on stale, inconsistent, expired, or unreadable state."""

        if not isinstance(request, ToolRequest) or request.workspace != self._workspace:
            return False
        try:
            if workspace_identity_hash(self._workspace) != self._workspace_identity_hash:
                return False
            now = self._clock()
            if now.tzinfo is None or now.utcoffset() is None:
                return False
            store = SQLiteEventStore.open_read_only(self._database)
            try:
                streams = store.read_streams_consistent(
                    (
                        ("run_lifecycle", request.run_id),
                        ("scheduler", "global"),
                        ("agent_registry", request.run_id),
                    )
                )
            finally:
                store.close()
            return self._matches(request, now, streams)
        except Exception:
            return False

    def _matches(self, request: ToolRequest, now: datetime, streams: dict) -> bool:
        tool_id = getattr(request, "tool_id", READ_ONLY_COMMAND_TOOL_ID)
        if tool_id not in {READ_ONLY_COMMAND_TOOL_ID, WORKSPACE_WRITE_TOOL_ID}:
            return False
        lifecycle_events = streams[("run_lifecycle", request.run_id)]
        scheduler_events = streams[("scheduler", "global")]
        agent_events = streams[("agent_registry", request.run_id)]
        lifecycle = reduce_lifecycle(request.run_id, lifecycle_events)
        agents = reduce_agent_registry(request.run_id, agent_events)
        if (
            lifecycle.status != "running"
            or lifecycle.policy_manifest_hash != self._policy_manifest_hash
            or lifecycle.workspace_identity_hash is None
            or lifecycle.workspace_identity_hash != self._workspace_identity_hash
        ):
            return False
        node = lifecycle.node(request.node_id)
        if (
            node.status != "running"
            or node.spec.role != request.role
            or tool_id not in node.spec.tool_ids
            or not node.attempts
        ):
            return False
        attempt = node.attempts[-1]
        expected_agent_id = AgentRegistry.agent_id_for_attempt(request.run_id, request.attempt_id)
        if (
            attempt.status != "accepted"
            or attempt.attempt_id != request.attempt_id
            or attempt.fencing_generation != request.fencing_generation
            or attempt.agent_instance_id != expected_agent_id
            or attempt.policy_manifest_hash != self._policy_manifest_hash
            or datetime.fromisoformat(attempt.lease_expires_at) <= now
        ):
            return False
        accepted_lifecycle = [
            event for event in lifecycle_events
            if event.event_type == "AttemptAccepted"
            and event.attempt_id == request.attempt_id
            and event.node_id == request.node_id
            and event.fencing_generation == request.fencing_generation
        ]
        if len(accepted_lifecycle) != 1 or request.causation_id != accepted_lifecycle[0].event_id:
            return False

        attempt_ref = _attempt_ref(request.run_id, request.node_id, request.attempt_id)
        accepted_routes = [
            event for event in scheduler_events
            if event.event_type == "RoutingDecisionAccepted"
            and event.payload.get("attempt_ref") == attempt_ref
        ]
        if len(accepted_routes) != 1:
            return False
        route = accepted_routes[0]
        if (
            route.run_id != request.run_id
            or route.node_id != request.node_id
            or route.attempt_id != request.attempt_id
            or route.fencing_generation != request.fencing_generation
            or route.payload.get("agent_instance_id") != expected_agent_id
            or route.payload.get("decision_hash") != attempt.decision_hash
            or route.payload.get("lease_expires_at") != attempt.lease_expires_at
            or tool_id not in route.payload.get("tool_ids", ())
            or datetime.fromisoformat(route.payload["lease_expires_at"]) <= now
        ):
            return False
        if any(
            event.event_type in {"AttemptSlotReleased", "AttemptOutcomeUnknown"}
            and event.payload.get("attempt_ref") == attempt_ref
            for event in scheduler_events
        ):
            return False
        try:
            agent = agents.agent(expected_agent_id)
        except KeyError:
            return False
        return (
            agent.status == "active"
            and agent.run_id == request.run_id
            and agent.node_id == request.node_id
            and agent.attempt_id == request.attempt_id
            and agent.fencing_generation == request.fencing_generation
            and agent.role == request.role
            and agent.decision_hash == attempt.decision_hash
            and agent.policy_manifest_hash == self._policy_manifest_hash
        )


def _attempt_ref(run_id: str, node_id: str, attempt_id: str) -> str:
    encoded = json.dumps((run_id, node_id, attempt_id), ensure_ascii=False, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = ["DurableAttemptAuthority"]
