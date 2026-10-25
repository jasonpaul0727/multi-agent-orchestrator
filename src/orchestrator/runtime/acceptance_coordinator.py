"""Private host orchestration of proof-bound Attempt acceptance.

Durable proposals remain untrusted input. Only the Scheduler terminal
transaction can consume the proof and release the Attempt's resource holds.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Callable

from orchestrator.artifacts import ArtifactStore, EphemeralArtifactGrantAuthority
from orchestrator.budget import UsageRecord
from orchestrator.scheduler import Scheduler
from orchestrator.validation import revalidate_model

from .proposal_verification import verify_proposal
from .verification_journal import VerifierProposalJournal, VerifierProposalRecord
from .verifier_process import IsolatedVerifierProcess
from .general_worker_journal import GeneralWorkerIdentity
from .attempt_usage import AttemptUsageSource, ProposalAcceptanceError, UnavailableAttemptUsageSource


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AttemptExecutionCoordinator:
    """Re-resolve, independently verify, and settle one exact durable proposal."""

    def __init__(
        self, *, scheduler: Scheduler, artifact_store: ArtifactStore,
        verifier: IsolatedVerifierProcess, journal: VerifierProposalJournal,
        grants: EphemeralArtifactGrantAuthority, usage_source: AttemptUsageSource,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(usage_source, AttemptUsageSource):
            raise TypeError("usage_source must implement the trusted AttemptUsageSource service")
        self._scheduler = scheduler
        self._artifact_store = artifact_store
        self._verifier = verifier
        self._journal = journal
        self._grants = grants
        self._usage_source = usage_source
        self._clock = clock

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.utcoffset() != timedelta(0):
            raise ProposalAcceptanceError("trusted clock must return an aware UTC time")
        return now

    def accept_proposal(
        self, *, run_id: str, node_id: str, attempt_id: str,
        fencing_generation: int, task_sha256: str,
    ) -> VerifierProposalRecord:
        """Caller supplies identity only; evidence, usage, and time are host-owned."""
        try:
            if type(task_sha256) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", task_sha256):
                raise ProposalAcceptanceError("proposal task hash is invalid")
            if any(dependency is None for dependency in (
                self._artifact_store, self._verifier, self._journal, self._grants,
                self._usage_source,
            )):
                raise ProposalAcceptanceError("proposal acceptance dependencies are unavailable")
            identity = dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                            fencing_generation=fencing_generation)
            if self._scheduler._general_operation(**identity) is not None:
                self._scheduler._finish_general_verified_attempt(
                    identity=GeneralWorkerIdentity(**identity), task_sha256=task_sha256)
                return self._journal.read_proposal(run_id, task_sha256)
            started_at = self._now()
            binding = self._scheduler.resolve_verification_binding(**identity, as_of=started_at)
            proposal = self._journal.read_proposal(run_id, task_sha256)
            if proposal is None or (
                proposal.task.context != binding.context
                or proposal.task.acceptance_contract != binding.verification_contract
                or tuple(sorted(proposal.task.required_check_ids)) != binding.required_check_ids
                or proposal.evidence.outcome != "accepted"
            ):
                raise ProposalAcceptanceError("proposal does not match frozen Attempt authority")

            verify_proposal(proposal, binding, verifier=self._verifier,
                artifact_store=self._artifact_store, grants=self._grants)
            usage = revalidate_model(UsageRecord, self._usage_source.usage_for(binding.active_attempt))
            completed_at = self._now()
            if completed_at < started_at:
                raise ProposalAcceptanceError("trusted completion clock moved backwards")
            # Scheduler rechecks cancellation, fencing, admission and lease in
            # the same append_checked transaction as all terminal/settlement writes.
            self._scheduler.finish_verified_attempt(
                **identity, task_sha256=proposal.task_sha256, usage=usage, completed_at=completed_at,
            )
            return proposal
        except ProposalAcceptanceError:
            raise
        except Exception:
            # Keep internal transport/state exceptions and untrusted data out
            # of the public fail-closed error surface.
            raise ProposalAcceptanceError("proposal acceptance prerequisites failed") from None
