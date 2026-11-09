"""Immutable metadata for the disabled-by-default mediation registry."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MediationOperation:
    operation_id: str
    run_id: str
    node_id: str
    attempt_id: str
    fencing_generation: int
    task_input_sha256: str
    workspace_sha256: str
    capability_sha256: str
    manifest_sha256: str
    policy_authority_sha256: str
    commitment_sha256: str


@dataclass(frozen=True, slots=True)
class ActorIntent:
    operation_id: str
    fencing_generation: int
    actor_id: str
    sequence: int
    kind: str
    host_id: str
    boot_id: str
    unit_name: str
    launch_id: str
    parameters_sha256: str
    intent_sha256: str


@dataclass(frozen=True, slots=True)
class MediationActor:
    intent: ActorIntent
    status: str = "launch_unknown"


@dataclass(frozen=True, slots=True)
class MediationState:
    operation: MediationOperation
    actors: tuple[MediationActor, ...] = ()
    closed: bool = False
    version: int = 1
