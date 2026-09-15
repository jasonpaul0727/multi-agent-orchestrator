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

__all__ = [
    "BudgetFailure",
    "BudgetInvariantFailure",
    "EventChainFailure",
    "EventStreamIntegrityFailure",
    "RecoveryBootstrap",
    "RecoveryFailure",
    "RecoveryResult",
    "SecurityFailure",
    "SecurityInvariantFailure",
    "bootstrap_recovery",
    "recover",
    "recover_aggregate",
]
