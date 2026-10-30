"""Untrusted Worker and Verifier boundary contracts."""

from .contracts import (
    AttemptContext,
    ArtifactRef,
    BUILTIN_VERIFICATION_CONTRACT_ID,
    BUILTIN_VERIFIER_ID,
    RuntimeContractError,
    SUPPORTED_VERIFICATION_CHECK_IDS,
    ToolCapabilityRef,
    VerificationCheck,
    VerificationEvidence,
    VerificationTask,
    WorkerResult,
    WorkerTask,
    decode_verification_evidence,
    decode_worker_result,
    validate_verification_evidence,
    validate_verification_task,
    validate_worker_result,
)
from .verifier_process import IsolatedVerifierProcess, VerifierProcessError
from .worker_input import (
    MAX_WORKER_INPUT_FRAME_BYTES,
    WORKER_INPUT_OUTPUT_BYTES,
    WorkerInputError,
    WorkerInputManifest,
    build_worker_input_manifest,
)
from .verification_journal import (
    VerifierProposalJournal,
    VerifierProposalJournalError,
    VerifierProposalRecord,
)

__all__ = [
    "AttemptContext",
    "ArtifactRef",
    "BUILTIN_VERIFICATION_CONTRACT_ID",
    "BUILTIN_VERIFIER_ID",
    "IsolatedVerifierProcess",
    "MAX_WORKER_INPUT_FRAME_BYTES",
    "RuntimeContractError",
    "SUPPORTED_VERIFICATION_CHECK_IDS",
    "ToolCapabilityRef",
    "VerificationCheck",
    "VerificationEvidence",
    "VerificationTask",
    "VerifierProcessError",
    "VerifierProposalJournal",
    "VerifierProposalJournalError",
    "VerifierProposalRecord",
    "WorkerResult",
    "WorkerTask",
    "WORKER_INPUT_OUTPUT_BYTES",
    "WorkerInputError",
    "WorkerInputManifest",
    "build_worker_input_manifest",
    "decode_verification_evidence",
    "decode_worker_result",
    "validate_verification_evidence",
    "validate_verification_task",
    "validate_worker_result",
    "WorkspaceToolProgram",
    "WorkspaceWorkerTask",
    "WorkspaceWorkerProposal",
    "IsolatedWorkspaceWorker",
    "WorkspaceExecutionHost",
    "WorkspaceExecutionResult",
    "WorkspaceExecutionService",
    "GeneralWorkerHostConfig",
    "GeneralWorkerExecutionService",
    "GeneralWorkerIdentity",
    "GeneralWorkerResult",
    "GeneralWorkerUnavailable",
]


def __getattr__(name):
    # These host services depend on tools/Scheduler, which themselves import
    # runtime contracts. Load them only after the contracts package is ready.
    if name in {"WorkspaceToolProgram", "WorkspaceWorkerTask", "WorkspaceWorkerProposal", "IsolatedWorkspaceWorker"}:
        from . import workspace_worker
        return getattr(workspace_worker, name)
    if name in {"WorkspaceExecutionHost", "WorkspaceExecutionResult", "WorkspaceExecutionService"}:
        from . import workspace_execution
        return getattr(workspace_execution, name)
    if name in {"GeneralWorkerHostConfig", "GeneralWorkerExecutionService", "GeneralWorkerUnavailable"}:
        from . import general_worker_execution
        return getattr(general_worker_execution, name)
    if name in {"GeneralWorkerIdentity", "GeneralWorkerResult"}:
        from . import general_worker_journal
        return getattr(general_worker_journal, name)
    raise AttributeError(name)
