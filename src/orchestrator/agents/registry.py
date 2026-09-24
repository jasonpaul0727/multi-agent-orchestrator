"""Event-sourced Agent instance accounting with cumulative and depth limits."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from orchestrator.config.effective import RoleName
from orchestrator.config.models import ReasoningEffort
from orchestrator.config.runtime import RunConfigSnapshot
from orchestrator.lifecycle.controller import LifecycleController, LifecycleConflict, LifecycleError
from orchestrator.lifecycle.models import AttemptState, NodeSpec
from orchestrator.persistence import EventDraft, SQLiteEventStore, StoredEvent


_AGENT_STREAM = "agent_registry"
_HASH = r"^sha256:[0-9a-f]{64}$"
_ID = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$"
AgentStatus = Literal["created", "active", "outcome_unknown", "completed", "failed", "cancelled"]


class AgentRegistryError(RuntimeError):
    """The requested Agent instance transition is invalid or stale."""


class AgentLimitExceeded(AgentRegistryError):
    """The Run has exhausted its cumulative Agent instance allowance."""


class AgentDepthLimitExceeded(AgentRegistryError):
    """A child Agent would exceed the frozen Run depth envelope."""


class AgentConcurrencyLimitExceeded(AgentRegistryError):
    """The Run has exhausted its active/unknown Agent allowance."""


class AgentRegistryLimits(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    max_total_agents: StrictInt = Field(gt=0)
    max_depth: StrictInt = Field(ge=0)
    max_concurrent_agents: StrictInt = Field(gt=0)


class AgentInstance(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    agent_instance_id: StrictStr = Field(min_length=1, pattern=_ID)
    run_id: StrictStr = Field(min_length=1, pattern=_ID)
    node_id: StrictStr = Field(min_length=1, pattern=_ID)
    attempt_id: StrictStr = Field(min_length=1, pattern=_ID)
    created_by_attempt_id: StrictStr = Field(min_length=1, pattern=_ID)
    parent_agent_instance_id: StrictStr | None = Field(default=None, pattern=_ID)
    depth: StrictInt = Field(ge=0)
    role: RoleName
    model_id: StrictStr = Field(min_length=1, pattern=_ID)
    provider_id: StrictStr = Field(min_length=1, pattern=_ID)
    decision_hash: StrictStr = Field(pattern=_HASH)
    policy_manifest_hash: StrictStr = Field(pattern=_HASH)
    reasoning_effort: ReasoningEffort
    fencing_generation: StrictInt = Field(gt=0)
    status: AgentStatus


class AgentRegistryState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    run_id: StrictStr = Field(min_length=1, pattern=_ID)
    event_version: StrictInt = Field(ge=0)
    total_created: StrictInt = Field(ge=0)
    active_count: StrictInt = Field(ge=0)
    instances: tuple[AgentInstance, ...]

    @property
    def unknown_count(self) -> int:
        return sum(item.status == "outcome_unknown" for item in self.instances)

    def agent(self, agent_instance_id: str) -> AgentInstance:
        for item in self.instances:
            if item.agent_instance_id == agent_instance_id:
                return item
        raise KeyError(agent_instance_id)

    def for_attempt(self, attempt_id: str) -> AgentInstance:
        for item in self.instances:
            if item.attempt_id == attempt_id:
                return item
        raise KeyError(attempt_id)


class AgentRegistry:
    """Maintain one durable Agent instance for each accepted model attempt.

    The Scheduler calls this inside the same SQLite write transaction as route,
    budget, lifecycle, and concurrency-slot acceptance. Cumulative counts are
    reconstructed from `AgentInstanceCreated`; terminal events never subtract
    from the count.
    """

    def __init__(self, event_store: SQLiteEventStore) -> None:
        self.event_store = event_store
        self.lifecycle = LifecycleController(event_store)

    @staticmethod
    def agent_id_for_attempt(run_id: str, attempt_id: str) -> str:
        encoded = json.dumps((run_id, attempt_id), ensure_ascii=False, separators=(",", ":"))
        return "agent-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]

    def replay(self, run_id: str) -> AgentRegistryState:
        return reduce_agent_registry(run_id, self.event_store.read_stream(_AGENT_STREAM, run_id))

    def register_attempt(self, run_id: str, *, node: NodeSpec, attempt: AttemptState) -> AgentInstance:
        if attempt.status != "accepted":
            raise AgentRegistryError("only an accepted model attempt creates an Agent instance")
        snapshot = self.lifecycle.config_snapshot(run_id)
        limits = _limits(snapshot)
        expected_agent_id = self.agent_id_for_attempt(run_id, attempt.attempt_id)
        if attempt.agent_instance_id != expected_agent_id:
            raise AgentRegistryError("attempt Agent ID is not the deterministic attempt binding")
        key = f"agent-create:{attempt.attempt_id}"

        def decide(events: list[StoredEvent], version: int):
            current = reduce_agent_registry(run_id, events)
            depth = 0
            parent = None
            if node.parent_agent_instance_id is not None:
                try:
                    parent = current.agent(node.parent_agent_instance_id)
                except KeyError as exc:
                    raise AgentRegistryError(
                        "parent Agent instance does not exist in this Run"
                    ) from exc
                if parent.status != "completed":
                    raise AgentRegistryError("child Agent requires a completed parent attempt")
                depth = parent.depth + 1
            if depth > limits.max_depth:
                raise AgentDepthLimitExceeded("Agent depth exceeds the frozen Run limit")

            payload = {
                "instance": AgentInstance(
                    agent_instance_id=attempt.agent_instance_id,
                    run_id=run_id,
                    node_id=node.node_id,
                    attempt_id=attempt.attempt_id,
                    created_by_attempt_id=(
                        parent.attempt_id if parent is not None else attempt.attempt_id
                    ),
                    parent_agent_instance_id=node.parent_agent_instance_id,
                    depth=depth,
                    role=node.role,
                    model_id=attempt.model_id,
                    provider_id=attempt.provider_id,
                    decision_hash=attempt.decision_hash,
                    policy_manifest_hash=attempt.policy_manifest_hash,
                    reasoning_effort=attempt.reasoning_effort,
                    fencing_generation=attempt.fencing_generation,
                    status="created",
                ).model_dump(mode="json")
            }
            drafts = (
                EventDraft(
                    "AgentInstanceCreated",
                    payload,
                    run_id=run_id,
                    node_id=node.node_id,
                    attempt_id=attempt.attempt_id,
                    fencing_generation=attempt.fencing_generation,
                    correlation_id=run_id,
                    causation_id=attempt.decision_hash,
                ),
                EventDraft(
                    "AgentStarted",
                    {"agent_instance_id": attempt.agent_instance_id},
                    run_id=run_id,
                    node_id=node.node_id,
                    attempt_id=attempt.attempt_id,
                    fencing_generation=attempt.fencing_generation,
                    correlation_id=run_id,
                    causation_id=attempt.decision_hash,
                ),
            )
            existing = [item for item in events if item.idempotency_key == key]
            if existing:
                if len(existing) != len(drafts) or any(
                    item.event_type != draft.event_type
                    or dict(item.payload) != draft.payload
                    or item.causation_id != attempt.decision_hash
                    or item.run_id != run_id
                    or item.node_id != node.node_id
                    or item.attempt_id != attempt.attempt_id
                    or item.fencing_generation != attempt.fencing_generation
                    for item, draft in zip(existing, drafts)
                ):
                    raise LifecycleConflict("Agent creation idempotency key was reused")
                return None
            lifecycle_state = self.lifecycle.replay(run_id)
            lifecycle_node = lifecycle_state.node(node.node_id)
            if lifecycle_node.spec != node or lifecycle_state.status != "running":
                raise LifecycleConflict("Agent creation does not match the running frozen node")
            if not any(
                item.attempt_id == attempt.attempt_id
                and item.agent_instance_id == attempt.agent_instance_id
                and item.fencing_generation == attempt.fencing_generation
                and item.status == "accepted"
                for item in lifecycle_node.attempts
            ):
                raise LifecycleConflict(
                    "Agent creation requires its matching accepted lifecycle attempt"
                )
            if current.total_created >= limits.max_total_agents:
                raise AgentLimitExceeded("Run has exhausted its cumulative Agent limit")
            if current.active_count >= limits.max_concurrent_agents:
                raise AgentConcurrencyLimitExceeded("Run active/unknown Agent limit is exhausted")
            if any(item.attempt_id == attempt.attempt_id for item in current.instances):
                raise AgentRegistryError("an attempt cannot create more than one Agent instance")
            if any(item.agent_instance_id == attempt.agent_instance_id for item in current.instances):
                raise AgentRegistryError("Agent instance ID is already present in this Run")
            return list(drafts)

        self.event_store.append_checked(_AGENT_STREAM, run_id, key, decide)
        return self.replay(run_id).agent(attempt.agent_instance_id)

    def complete_attempt(
        self,
        run_id: str,
        *,
        attempt_id: str,
        agent_instance_id: str,
        node_id: str,
        fencing_generation: int,
        causation_id: str,
        outcome: Literal["succeeded", "failed", "outcome_unknown"],
    ) -> None:
        event_type = {
            "succeeded": "AgentCompleted",
            "failed": "AgentFailed",
            "outcome_unknown": "AgentOutcomeUnknown",
        }[outcome]
        self._transition(
            run_id,
            event_type=event_type,
            attempt_id=attempt_id,
            agent_instance_id=agent_instance_id,
            node_id=node_id,
            fencing_generation=fencing_generation,
            causation_id=causation_id,
            outcome=outcome,
            allowed_statuses={"active"},
        )

    def reconcile_attempt(
        self,
        run_id: str,
        *,
        attempt_id: str,
        agent_instance_id: str,
        node_id: str,
        fencing_generation: int,
        causation_id: str,
        outcome: Literal["succeeded", "failed"],
    ) -> None:
        self._transition(
            run_id,
            event_type="AgentReconciled",
            attempt_id=attempt_id,
            agent_instance_id=agent_instance_id,
            node_id=node_id,
            fencing_generation=fencing_generation,
            causation_id=causation_id,
            outcome=outcome,
            allowed_statuses={"outcome_unknown"},
        )

    def cancel_attempt(
        self,
        run_id: str,
        *,
        attempt_id: str,
        agent_instance_id: str,
        node_id: str,
        fencing_generation: int,
        causation_id: str,
    ) -> None:
        self._transition(
            run_id,
            event_type="AgentCancelled",
            attempt_id=attempt_id,
            agent_instance_id=agent_instance_id,
            node_id=node_id,
            fencing_generation=fencing_generation,
            causation_id=causation_id,
            outcome="cancelled",
            allowed_statuses={"active"},
        )

    def _transition(
        self,
        run_id: str,
        *,
        event_type: str,
        attempt_id: str,
        agent_instance_id: str,
        node_id: str,
        fencing_generation: int,
        causation_id: str,
        outcome: str,
        allowed_statuses: set[str],
    ) -> None:
        payload = {"agent_instance_id": agent_instance_id, "outcome": outcome}
        key = f"agent-transition:{event_type}:{attempt_id}"

        def decide(events: list[StoredEvent], version: int):
            existing = [item for item in events if item.idempotency_key == key]
            if existing:
                event = existing[0] if len(existing) == 1 else None
                if (
                    event is None
                    or event.event_type != event_type
                    or dict(event.payload) != payload
                    or event.run_id != run_id
                    or event.node_id != node_id
                    or event.attempt_id != attempt_id
                    or event.fencing_generation != fencing_generation
                    or event.causation_id != causation_id
                ):
                    raise LifecycleConflict("Agent transition idempotency key was reused")
                return None
            state = reduce_agent_registry(run_id, events)
            try:
                instance = state.for_attempt(attempt_id)
            except KeyError as exc:
                raise LifecycleConflict("Agent result references an unknown attempt") from exc
            if (
                instance.agent_instance_id != agent_instance_id
                or instance.node_id != node_id
                or instance.fencing_generation != fencing_generation
                or instance.status not in allowed_statuses
            ):
                raise LifecycleConflict("Agent result is stale or not in an allowed state")
            return [
                EventDraft(
                    event_type,
                    payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    correlation_id=run_id,
                    causation_id=causation_id,
                )
            ]

        self.event_store.append_checked(_AGENT_STREAM, run_id, key, decide)


def reduce_agent_registry(run_id: str, events: list[StoredEvent]) -> AgentRegistryState:
    instances: dict[str, AgentInstance] = {}
    by_attempt: dict[str, str] = {}
    for event in events:
        payload = event.payload
        if event.run_id != run_id:
            raise AgentRegistryError("Agent event identity does not match its stream")
        if event.event_type == "AgentInstanceCreated":
            instance = AgentInstance.model_validate(payload.get("instance"))
            if (
                instance.run_id != run_id
                or instance.status != "created"
                or event.node_id != instance.node_id
                or event.attempt_id != instance.attempt_id
                or event.fencing_generation != instance.fencing_generation
                or event.causation_id != instance.decision_hash
            ):
                raise AgentRegistryError("AgentInstanceCreated context disagrees with its payload")
            if instance.agent_instance_id in instances or instance.attempt_id in by_attempt:
                raise AgentRegistryError("Agent instance or attempt ID is duplicated")
            if instance.parent_agent_instance_id is None:
                if instance.depth != 0 or instance.created_by_attempt_id != instance.attempt_id:
                    raise AgentRegistryError("root Agent depth or creator context is invalid")
            else:
                parent = instances.get(instance.parent_agent_instance_id)
                if parent is None or parent.status != "completed":
                    raise AgentRegistryError("child Agent requires a completed parent instance")
                if (
                    instance.depth != parent.depth + 1
                    or instance.created_by_attempt_id != parent.attempt_id
                ):
                    raise AgentRegistryError("child Agent depth or creator context is invalid")
            instances[instance.agent_instance_id] = instance
            by_attempt[instance.attempt_id] = instance.agent_instance_id
            continue

        agent_id = payload.get("agent_instance_id")
        instance = instances.get(agent_id)
        if instance is None:
            raise AgentRegistryError("Agent event references an unknown instance")
        if (
            event.node_id != instance.node_id
            or event.attempt_id != instance.attempt_id
            or event.fencing_generation != instance.fencing_generation
            or (event.event_type != "AgentCancelled" and event.causation_id != instance.decision_hash)
        ):
            raise AgentRegistryError("Agent event context disagrees with its instance")
        expected_from: set[str]
        next_status: str
        if event.event_type == "AgentStarted":
            expected_from, next_status = {"created"}, "active"
        elif event.event_type == "AgentOutcomeUnknown":
            expected_from, next_status = {"active"}, "outcome_unknown"
        elif event.event_type == "AgentCompleted":
            if payload.get("outcome") != "succeeded":
                raise AgentRegistryError("AgentCompleted must record a succeeded outcome")
            expected_from, next_status = {"active"}, "completed"
        elif event.event_type == "AgentFailed":
            if payload.get("outcome") != "failed":
                raise AgentRegistryError("AgentFailed must record a failed outcome")
            expected_from, next_status = {"active"}, "failed"
        elif event.event_type == "AgentReconciled":
            outcome = payload.get("outcome")
            if outcome not in {"succeeded", "failed"}:
                raise AgentRegistryError("AgentReconciled must resolve an unknown outcome")
            expected_from, next_status = {"outcome_unknown"}, "completed" if outcome == "succeeded" else "failed"
        elif event.event_type == "AgentCancelled":
            if payload.get("outcome") != "cancelled":
                raise AgentRegistryError("AgentCancelled must record a cancelled outcome")
            expected_from, next_status = {"created", "active"}, "cancelled"
        else:
            raise AgentRegistryError("unsupported Agent registry event")
        if instance.status not in expected_from:
            raise AgentRegistryError("Agent event violates the instance state machine")
        instances[agent_id] = instance.model_copy(update={"status": next_status})

    ordered = tuple(instances[key] for key in sorted(instances))
    active = sum(item.status in {"active", "outcome_unknown"} for item in ordered)
    return AgentRegistryState(
        run_id=run_id,
        event_version=events[-1].stream_version if events else 0,
        total_created=len(ordered),
        active_count=active,
        instances=ordered,
    )


def _limits(snapshot: RunConfigSnapshot) -> AgentRegistryLimits:
    config = snapshot.resolved_config.config
    requested = config.presets[config.active_preset].requested_budget
    envelope = config.policy_envelope
    return AgentRegistryLimits(
        max_total_agents=min(requested.max_agents, envelope.max_agents),
        max_depth=min(requested.max_depth, envelope.max_depth),
        max_concurrent_agents=min(requested.max_concurrency, envelope.max_concurrency),
    )


__all__ = [
    "AgentConcurrencyLimitExceeded",
    "AgentDepthLimitExceeded",
    "AgentInstance",
    "AgentLimitExceeded",
    "AgentRegistry",
    "AgentRegistryError",
    "AgentRegistryLimits",
    "AgentRegistryState",
    "reduce_agent_registry",
]
