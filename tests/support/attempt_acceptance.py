"""Synthetic host commitments and proof-backed completion for tests only."""

import hashlib

from orchestrator.runtime.contracts import (
    ArtifactRef, AttemptContext, VerificationCheck, VerificationEvidence, VerificationTask,
    BUILTIN_VERIFICATION_CONTRACT_ID, BUILTIN_VERIFIER_ID,
)
from orchestrator.runtime.verification_journal import VerifierProposalJournal


# Commitment to the test's explicitly declared synthetic input manifest.
INPUT_MANIFEST = b'{"fixture":"synthetic-test-input","schema_version":1}'
INPUT_MANIFEST_HASH = "sha256:" + hashlib.sha256(INPUT_MANIFEST).hexdigest()
ADMISSION = dict(input_manifest_hash=INPUT_MANIFEST_HASH,
                 verification_contract=BUILTIN_VERIFICATION_CONTRACT_ID,
                 required_check_ids=("artifact-integrity",))


def proposal_for_attempt(control, *, run_id, node_id, attempt_id, context_changes=None,
                         outcome="accepted", required_check_ids=None):
    accepted = next(event for event in control.event_store.read_stream("scheduler", "global")
                    if event.event_type == "RoutingDecisionAccepted"
                    and event.run_id == run_id and event.node_id == node_id
                    and event.attempt_id == attempt_id)
    context = AttemptContext.model_validate(accepted.payload["verification_context"])
    if context_changes:
        context = context.model_copy(update=context_changes)
    artifact = ArtifactRef(digest="sha256:" + "c" * 64, size_bytes=5,
                           artifact_type="result", media_type="text/plain")
    task = VerificationTask(
        context=context, candidate_artifacts=(artifact,),
        required_check_ids=required_check_ids or tuple(accepted.payload["required_check_ids"]),
        acceptance_contract=accepted.payload["verification_contract"],
    )
    evidence = VerificationEvidence(
        context=context, verifier_id=BUILTIN_VERIFIER_ID, outcome=outcome,
        inspected_digests=(artifact.digest,),
        checks=tuple(VerificationCheck(check_id=check, passed=outcome == "accepted",
                                       evidence_digests=(artifact.digest,))
                     for check in task.required_check_ids),
    )
    return VerifierProposalJournal(control.event_store).record(task, evidence)


def finish_with_proof(control, *, run_id, node_id, attempt_id, fencing_generation,
                      completed_at, usage):
    proposal = proposal_for_attempt(control, run_id=run_id, node_id=node_id,
                                    attempt_id=attempt_id)
    control.finish_verified_attempt(
        run_id=run_id, node_id=node_id, attempt_id=attempt_id,
        fencing_generation=fencing_generation, completed_at=completed_at,
        usage=usage, task_sha256=proposal.task_sha256,
    )
