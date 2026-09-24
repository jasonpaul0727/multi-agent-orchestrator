"""Immutable Run, DAG-node, and attempt state projected from lifecycle events."""

from __future__ import annotations

from datetime import datetime
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

from orchestrator.config.effective import RoleName
from orchestrator.config.models import ReasoningEffort


_HASH = r"^sha256:[0-9a-f]{64}$"
_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$"


class _LifecycleModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class NodeSpec(_LifecycleModel):
    """Append-only node contract reference and dependency declaration."""

    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    role: RoleName
    planning_contract_hash: StrictStr = Field(pattern=_HASH)
    depends_on: tuple[StrictStr, ...] = ()
    parent_agent_instance_id: StrictStr | None = None
    tool_ids: tuple[StrictStr, ...] = ()
    max_attempts: StrictInt = Field(default=1, ge=1, le=100)

    @field_validator("depends_on", mode="before")
    @classmethod
    def normalize_dependencies(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("depends_on must be an array")
        return tuple(value)

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(not _valid_identifier(item) for item in value):
            raise ValueError("dependencies must be unique stable node IDs")
        return tuple(sorted(value))

    @field_validator("parent_agent_instance_id")
    @classmethod
    def validate_parent_agent(cls, value: str | None) -> str | None:
        if value is not None and not _valid_identifier(value):
            raise ValueError("parent_agent_instance_id must be a stable agent ID")
        return value

    @field_validator("tool_ids", mode="before")
    @classmethod
    def normalize_tool_ids(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("tool_ids must be an array")
        return tuple(value)

    @field_validator("tool_ids")
    @classmethod
    def validate_tool_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(not _valid_identifier(item) for item in value):
            raise ValueError("tool IDs must be unique stable identifiers")
        return tuple(sorted(value))


class AttemptState(_LifecycleModel):
    attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    agent_instance_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    fencing_generation: StrictInt = Field(gt=0)
    decision_hash: StrictStr = Field(pattern=_HASH)
    policy_manifest_hash: StrictStr = Field(pattern=_HASH)
    reasoning_effort: ReasoningEffort
    model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    provider_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    reservation_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    lease_expires_at: StrictStr = Field(min_length=1)
    status: Literal["accepted", "succeeded", "failed", "outcome_unknown", "cancelled"]

    @field_validator("lease_expires_at")
    @classmethod
    def validate_expiry(cls, value: str) -> str:
        try:
            expiry = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("lease_expires_at must be an ISO-8601 timestamp") from exc
        if expiry.tzinfo is None or expiry.utcoffset() is None:
            raise ValueError("lease_expires_at must include a UTC offset")
        return value


class NodeState(_LifecycleModel):
    spec: NodeSpec
    status: Literal[
        "blocked", "ready", "running", "awaiting_reconciliation", "succeeded", "failed", "cancelled"
    ]
    attempts: tuple[AttemptState, ...] = ()

    @field_validator("attempts", mode="before")
    @classmethod
    def normalize_attempts(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("attempts must be an array")
        return tuple(value)


class RunLifecycleState(_LifecycleModel):
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER)
    status: Literal[
        "created", "running", "paused", "awaiting_user", "cancelling",
        "succeeded", "failed", "cancelled",
    ]
    awaiting_user_request_id: StrictStr | None = Field(default=None, pattern=_IDENTIFIER)
    config_hash: StrictStr = Field(pattern=_HASH)
    registry_hash: StrictStr = Field(pattern=_HASH)
    graph_version: StrictInt = Field(ge=0)
    event_version: StrictInt = Field(ge=1)
    nodes: tuple[NodeState, ...]

    @field_validator("nodes", mode="before")
    @classmethod
    def normalize_nodes(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("nodes must be an array")
        return tuple(value)

    def node(self, node_id: str) -> NodeState:
        for item in self.nodes:
            if item.spec.node_id == node_id:
                return item
        raise KeyError(node_id)


def _valid_identifier(value: str) -> bool:
    return bool(re.fullmatch(_IDENTIFIER, value))


__all__ = ["AttemptState", "NodeSpec", "NodeState", "RunLifecycleState"]
