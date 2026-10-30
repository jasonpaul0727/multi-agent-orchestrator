"""Trusted host usage service shared by Scheduler and acceptance composition."""

from typing import Protocol, TYPE_CHECKING, runtime_checkable

from orchestrator.budget import UsageRecord

if TYPE_CHECKING:
    from orchestrator.scheduler.core import ActiveAttemptSnapshot


class ProposalAcceptanceError(RuntimeError):
    """Acceptance cannot prove every prerequisite; durable state stays held."""

    code = "proposal_acceptance_rejected"


@runtime_checkable
class AttemptUsageSource(Protocol):
    """Host-only settlement authority; no production backend exists yet."""

    def usage_for(self, snapshot: "ActiveAttemptSnapshot") -> UsageRecord: ...


class UnavailableAttemptUsageSource:
    def usage_for(self, snapshot: "ActiveAttemptSnapshot") -> UsageRecord:
        raise ProposalAcceptanceError("trusted Attempt usage is unavailable")
