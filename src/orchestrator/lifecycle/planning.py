"""Trusted host-side intake from task proposals to frozen append-only graph nodes."""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

from orchestrator.config.effective import RoleName
from orchestrator.routing.planning import PlanningError, compile_node_contract
from orchestrator.security import PolicyManifest

from .controller import LifecycleConflict, LifecycleController
from .models import NodeSpec, RunLifecycleState


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")


class NodeProposal(BaseModel):
    """Untrusted planner proposal; raw task text is consumed but never persisted."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)

    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    role: RoleName
    task_text: StrictStr = Field(min_length=1, max_length=100_000)
    depends_on: tuple[StrictStr, ...] = ()
    parent_agent_instance_id: StrictStr | None = None
    tool_ids: tuple[StrictStr, ...] = ()
    required_capabilities: tuple[StrictStr, ...] = ()
    context_tokens: StrictInt = Field(ge=0)
    max_output_tokens: StrictInt | None = Field(default=None, gt=0)
    max_attempts: StrictInt = Field(default=1, ge=1, le=100)

    @field_validator("depends_on", "tool_ids", "required_capabilities", mode="before")
    @classmethod
    def normalize_sequences(cls, value: object, info: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must be an array")
        return tuple(value)

    @field_validator("task_text")
    @classmethod
    def require_nonblank_task(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("task_text must not be blank")
        return value

    @field_validator("depends_on", "tool_ids", "required_capabilities")
    @classmethod
    def validate_unique_identifiers(
        cls, value: tuple[str, ...], info: object
    ) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(
            item != item.strip() or not _IDENTIFIER.fullmatch(item) for item in value
        ):
            raise ValueError(f"{getattr(info, 'field_name', 'field')} must contain unique identifiers")
        return tuple(sorted(value))

    @field_validator("parent_agent_instance_id")
    @classmethod
    def validate_parent_agent(cls, value: str | None) -> str | None:
        if value is not None and not _IDENTIFIER.fullmatch(value):
            raise ValueError("parent_agent_instance_id must be a stable identifier")
        return value


class GraphPlanningService:
    """Compile and durably append contracts against a Run's frozen config snapshot.

    This service is intended to be the only application-level graph proposal
    path. The lower-level lifecycle append primitive remains available to the
    trusted controller and tests, but it cannot bypass the contracts created
    here when used for a product plan.
    """

    def __init__(self, lifecycle: LifecycleController) -> None:
        self.lifecycle = lifecycle

    def append_proposal(
        self,
        run_id: str,
        proposals: tuple[NodeProposal, ...],
        *,
        policy_manifest: PolicyManifest,
        expected_graph_version: int,
        idempotency_key: str,
    ) -> RunLifecycleState:
        if not proposals:
            raise PlanningError("empty_graph_proposal")
        if not isinstance(policy_manifest, PolicyManifest):
            raise TypeError("policy_manifest must be a validated PolicyManifest")
        if any(not isinstance(item, NodeProposal) for item in proposals):
            raise TypeError("proposals must contain validated NodeProposal values")
        if len({item.node_id for item in proposals}) != len(proposals):
            raise PlanningError("duplicate_proposed_node_id")

        snapshot = self.lifecycle.config_snapshot(run_id)
        state = self.lifecycle.replay(run_id)
        if state.config_hash != snapshot.effective_config_hash:
            raise PlanningError("run_config_snapshot_mismatch")
        if state.registry_hash != snapshot.registry_manifest_hash:
            raise PlanningError("run_registry_snapshot_mismatch")
        if (
            state.policy_manifest_hash is not None
            and state.policy_manifest_hash != policy_manifest.content_hash
        ):
            raise PlanningError("run_policy_snapshot_mismatch")

        existing_contracts = [node.spec.planning_contract for node in state.nodes]
        if existing_contracts:
            if any(contract is None for contract in existing_contracts):
                raise PlanningError("existing_graph_contract_unavailable")
            policy_hashes = {contract.policy_manifest_hash for contract in existing_contracts if contract}
            if policy_hashes != {policy_manifest.content_hash}:
                raise PlanningError("run_policy_snapshot_mismatch")

        config = snapshot.resolved_config.config
        registry = snapshot.registry_manifest
        nodes: list[NodeSpec] = []
        for proposal in proposals:
            contract = compile_node_contract(
                run_id=run_id,
                node_id=proposal.node_id,
                role=proposal.role,
                task_text=proposal.task_text,
                config=config,
                registry=registry,
                policy_manifest=policy_manifest,
                context_tokens=proposal.context_tokens,
                max_output_tokens=proposal.max_output_tokens,
                required_capabilities=proposal.required_capabilities,
                tool_ids=proposal.tool_ids,
            )
            nodes.append(
                NodeSpec(
                    node_id=proposal.node_id,
                    role=proposal.role,
                    planning_contract_hash=contract.contract_hash,
                    planning_contract=contract,
                    depends_on=proposal.depends_on,
                    parent_agent_instance_id=proposal.parent_agent_instance_id,
                    tool_ids=proposal.tool_ids,
                    max_attempts=proposal.max_attempts,
                )
            )

        try:
            return self.lifecycle.append_nodes(
                run_id,
                tuple(nodes),
                expected_graph_version=expected_graph_version,
                idempotency_key=idempotency_key,
                policy_manifest=policy_manifest,
            )
        except LifecycleConflict as exc:
            raise PlanningError("stale_graph_version") from exc


__all__ = ["GraphPlanningService", "NodeProposal"]
