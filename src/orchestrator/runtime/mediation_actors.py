"""Immutable metadata for the disabled-by-default mediation registry."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MediationOperation:
    operation_id: str
    run_id: str
    node_id: str
    attempt_id: str
    fencing_generation: int
    commitment_sha256: str


@dataclass(frozen=True, slots=True)
class ActorIntent:
    operation_id: str
    fencing_generation: int
    actor_id: str
    sequence: int
    kind: str
    unit_name: str
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
