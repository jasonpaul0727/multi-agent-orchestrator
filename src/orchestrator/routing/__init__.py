"""Task classification, planning contracts, and deterministic model routing."""

from .classifier import ClassificationResult, TaskClassifier
from .engine import (
    CandidateAssessment,
    EligibilityBlock,
    EligibilitySnapshot,
    HealthAggregateRef,
    ModelHealthSnapshot,
    ModelRouter,
    RecoveryAuthorization,
    RoutingDecision,
    RoutingRequest,
    SecretAvailability,
)
from .planning import PlanningError, PlanningNodeContract, compile_node_contract
from .health import (
    HealthAggregateKey,
    HealthAggregateState,
    HealthCircuitError,
    HealthController,
    HealthVersionRef,
    ProbeLease,
    ProbeLeaseConflict,
    effective_health_state,
    initial_health_state,
    reduce_health_events,
)
from .recovery import RecoveryController, RecoveryEvidence, RecoveryPlan

__all__ = [
    "CandidateAssessment",
    "ClassificationResult",
    "EligibilityBlock",
    "EligibilitySnapshot",
    "HealthAggregateRef",
    "HealthAggregateKey",
    "HealthAggregateState",
    "HealthCircuitError",
    "HealthController",
    "ModelHealthSnapshot",
    "ModelRouter",
    "PlanningError",
    "PlanningNodeContract",
    "RecoveryAuthorization",
    "RecoveryController",
    "RecoveryEvidence",
    "RecoveryPlan",
    "RoutingDecision",
    "RoutingRequest",
    "SecretAvailability",
    "HealthVersionRef",
    "ProbeLease",
    "ProbeLeaseConflict",
    "TaskClassifier",
    "compile_node_contract",
    "effective_health_state",
    "initial_health_state",
    "reduce_health_events",
]
