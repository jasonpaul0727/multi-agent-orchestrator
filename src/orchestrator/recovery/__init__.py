"""Generic deterministic recovery primitives."""

from .bootstrap import (
    BudgetFailure,
    BudgetInvariantFailure,
    EventChainFailure,
    EventStreamIntegrityFailure,
    RecoveryBootstrap,
    RecoveryFailure,
    RecoveryResult,
    SecurityFailure,
    SecurityInvariantFailure,
    bootstrap_recovery,
    recover,
    recover_aggregate,
)
from .run import RecoveredAttemptLease, RecoveredRun, RunRecoveryCoordinator, RunRecoveryError

__all__ = [
    "BudgetFailure",
    "BudgetInvariantFailure",
    "EventChainFailure",
    "EventStreamIntegrityFailure",
    "RecoveryBootstrap",
    "RecoveryFailure",
    "RecoveryResult",
    "RecoveredAttemptLease",
    "RecoveredRun",
    "RunRecoveryCoordinator",
    "RunRecoveryError",
    "SecurityFailure",
    "SecurityInvariantFailure",
    "bootstrap_recovery",
    "recover",
    "recover_aggregate",
]
