"""Fresh independent proposal verification, always outside database authority."""

import hashlib
import json

from orchestrator.validation import revalidate_model
from .attempt_usage import ProposalAcceptanceError
from .contracts import BUILTIN_VERIFIER_ID, VerificationEvidence, validate_verification_evidence


def verify_proposal(proposal, binding, *, verifier, artifact_store, grants):
    evidence = verifier.verify(
        proposal.task, artifact_store,
        grant_for_digest=lambda digest: grants.issue(
            digest=digest, run_id=proposal.task.context.run_id,
            expires_at=binding.active_attempt.lease_expires_at,
        ),
    )
    _validate_proposal_evidence(proposal, evidence)


async def verify_proposal_async(proposal, binding, *, verifier, artifact_store, grants):
    evidence = await verifier.verify_async(
        proposal.task, artifact_store,
        grant_for_digest=lambda digest: grants.issue(digest=digest,
            run_id=proposal.task.context.run_id, expires_at=binding.active_attempt.lease_expires_at),
    )
    _validate_proposal_evidence(proposal, evidence)


def _validate_proposal_evidence(proposal, evidence):
    evidence = revalidate_model(VerificationEvidence, evidence)
    validate_verification_evidence(proposal.task, evidence)
    encoded = json.dumps(evidence.model_dump(mode="json"), ensure_ascii=False,
                         sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    evidence_hash = "sha256:" + hashlib.sha256(encoded).hexdigest()
    if (evidence.verifier_id != BUILTIN_VERIFIER_ID or evidence.outcome != "accepted"
        or evidence_hash != proposal.evidence_sha256):
        raise ProposalAcceptanceError("independent verification differs from durable proof")
