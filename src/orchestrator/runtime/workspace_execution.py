"""Trusted, durable one-tool Worker composition; publication is not Node success."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import os
import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator

from orchestrator.approvals import ApprovalService, ConsumedApproval, ExecutionAttempt
from orchestrator.isolation import (
    IsolationNotStarted, SandboxLimits, SandboxTerminationReceipt,
    WorkspacePublishReceipt,
    acquire_workspace_write_lease, acknowledge_workspace_publication,
    acknowledge_workspace_recovery, inspect_workspace_publication_journal,
    recover_workspace_publications_with_outcomes, verify_workspace_isolation,
)
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.runtime.contracts import AttemptContext
from orchestrator.runtime.worker_input import build_worker_input_manifest
from orchestrator.runtime.worker_input_service import WorkerInputService
from orchestrator.runtime.workspace_worker import (
    IsolatedWorkspaceWorker, WorkspaceWorkerError, WorkspaceWorkerTask, canonical,
    decode_workspace_program, operation_id as _operation_id, proposal_bytes, validate_workspace_proposal,
)
from orchestrator.security import PolicyManifest
from orchestrator.tools.authority import DurableAttemptAuthority
from orchestrator.tools.gateway import PolicyState, ToolAuditUnavailable, ToolRequest
from orchestrator.tools.workspace_write import (
    WorkspaceWriteGateway, WorkspaceWriteRequest, _unresolved_executions,
    _unresolved_publications, _hash_json,
)
from orchestrator.workspace_identity import workspace_identity_hash

OPERATION_STREAM = "workspace_worker_operation"
_HASH = r"^sha256:[0-9a-f]{64}$"


def _utc_now():
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class WorkspaceExecutionHost:
    workspace: Path
    lease_root: Path
    journal_root: Path
    limits: SandboxLimits
    policy_state: Callable[[ToolRequest], PolicyState]
    clock: Callable[[], datetime] = _utc_now
    approval_factory: Callable[[SQLiteEventStore], ApprovalService] | None = None
    approval_requester: Callable[[ToolRequest], str] | None = None

    def __post_init__(self):
        if (any(not isinstance(value, Path) or not value.is_absolute() for value in
                (self.workspace, self.lease_root, self.journal_root))
            or not isinstance(self.limits, SandboxLimits)
            or not callable(self.policy_state) or not callable(self.clock)
            or (self.approval_factory is None) != (self.approval_requester is None)
            or self.approval_factory is not None and (
                not callable(self.approval_factory) or not callable(self.approval_requester))):
            raise ValueError("invalid workspace execution host")


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", hide_input_in_errors=True)


class PublicationReceipt(_Model):
    transaction_id: StrictStr = Field(pattern=r"^[0-9a-f]{32}$")
    manifest_hash: StrictStr = Field(pattern=_HASH)
    entries_published: StrictInt = Field(ge=0)
    lease_generation: StrictInt = Field(gt=0)


class WorkspaceExecutionResult(_Model):
    outcome: Literal["published", "blocked", "awaiting_approval", "failed", "outcome_unknown"]
    reason_code: Literal["workspace_published", "isolation_unavailable", "workspace_input_invalid",
        "authority_unavailable", "operation_in_progress", "worker_failed", "execution_failed",
        "execution_unknown", "approval_required", "policy_denied"]
    # No operation is reserved for a request rejected before durable admission.
    operation_id: StrictStr | None = Field(default=None, pattern=_HASH)
    approval_request_id: StrictStr | None = Field(default=None, max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
    publication_receipt: PublicationReceipt | None = None
    publication_atomicity: Literal["per_path_crash_recoverable"] = "per_path_crash_recoverable"

    @model_validator(mode="after")
    def coherent(self):
        reasons = {
            "published": {"workspace_published"},
            "blocked": {"isolation_unavailable", "workspace_input_invalid", "authority_unavailable", "policy_denied"},
            "awaiting_approval": {"approval_required"},
            "failed": {"worker_failed", "execution_failed"},
            "outcome_unknown": {"operation_in_progress", "execution_unknown"},
        }
        if self.reason_code not in reasons[self.outcome]:
            raise ValueError("reason does not match outcome")
        if self.operation_id is None and self.outcome != "blocked":
            raise ValueError("admitted outcomes require an operation identity")
        if (self.outcome == "published") != (self.publication_receipt is not None):
            raise ValueError("publication outcome requires its receipt")
        if self.outcome == "published" and (self.operation_id is None or self.reason_code != "workspace_published"):
            raise ValueError("publication identity is missing")
        if self.approval_request_id is not None and self.outcome != "awaiting_approval":
            raise ValueError("approval request on incompatible outcome")
        return self


class _Binding(_Model):
    context: AttemptContext
    role: StrictStr
    program_hash: StrictStr = Field(pattern=_HASH)
    proposal_hash: StrictStr = Field(pattern=_HASH)
    scope_hash: StrictStr = Field(pattern=_HASH)
    workspace_identity_hash: StrictStr = Field(pattern=_HASH)
    causation_id: StrictStr
    approval_grant_hash: StrictStr | None = Field(default=None, pattern=_HASH)


class _OperationCorrupt(ValueError):
    """Durable operation state is invalid; quarantine rather than execute."""


def _hash(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


class WorkspaceExecutionService:
    def __init__(self, *, scheduler, database: str | Path, host: WorkspaceExecutionHost | None):
        if host is not None and type(host) is not WorkspaceExecutionHost:
            raise TypeError("workspace host must be trusted configuration")
        self._scheduler = scheduler
        self._events = scheduler.event_store
        self._database = Path(database).absolute()
        self._host = host
        self._approvals = None
        if host is not None and host.approval_factory is not None:
            self._approvals = host.approval_factory(self._events)
            if not isinstance(self._approvals, ApprovalService) or not self._approvals.uses_event_store(self._events):
                raise ValueError("workspace approvals require this application's EventStore")

    def execute(self, *, run_id: str, node_id: str, attempt_id: str,
                fencing_generation: int, task_text: str,
                approval_grant_id: str | None = None) -> WorkspaceExecutionResult:
        host = self._host
        if host is None:
            return WorkspaceExecutionResult(outcome="blocked", reason_code="isolation_unavailable")
        # External callbacks are evaluated before entering the checked snapshot.
        try:
            now = host.clock()
            if not isinstance(now, datetime) or now.utcoffset() != timezone.utc.utcoffset(now):
                raise ValueError("invalid clock")
            if approval_grant_id is not None:
                import re
                if type(approval_grant_id) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}", approval_grant_id) is None:
                    raise ValueError("invalid approval reference")
            program = decode_workspace_program(task_text.encode("utf-8"))
            workspace_hash = workspace_identity_hash(host.workspace)
        except Exception:
            return WorkspaceExecutionResult(outcome="blocked", reason_code="workspace_input_invalid")
        prepared = {}
        identity = dict(run_id=run_id, node_id=node_id, attempt_id=attempt_id,
                        fencing_generation=fencing_generation)

        def prepare(_events, _version):
            historical = self._existing(identity, task_text, program, workspace_hash, approval_grant_id)
            if historical is not None:
                prepared["replay"] = historical
                return None
            # The clock here is a captured value, not a caller callback.
            task = WorkerInputService(self._scheduler, clock=lambda: now).prepare_worker_task(
                **identity, task_text=task_text)
            state = self._scheduler.lifecycle.replay(run_id)
            if (state.workspace_identity_hash != workspace_hash
                or "workspace.write-candidate" not in state.node(node_id).spec.tool_ids):
                raise ValueError("workspace authority mismatch")
            events = self._events.read_stream("run_lifecycle", run_id)
            policies = [PolicyManifest.model_validate(event.payload["policy_manifest"])
                        for event in events if "policy_manifest" in event.payload]
            if not policies or any(item.content_hash != task.context.policy_manifest_hash for item in policies):
                raise ValueError("policy binding mismatch")
            acceptances = [event for event in events if event.event_type == "AttemptAccepted"
                           and event.node_id == node_id and event.attempt_id == attempt_id
                           and event.fencing_generation == fencing_generation]
            if len(acceptances) != 1:
                raise ValueError("acceptance causation missing")
            envelope = WorkspaceWorkerTask.bind(task)
            binding = _Binding(context=task.context, role=task.role,
                program_hash=_hash(program.canonical_text.encode()),
                proposal_hash=_hash(proposal_bytes(envelope)),
                scope_hash=envelope.task.tool_capabilities[0].scope_hash,
                workspace_identity_hash=workspace_hash, causation_id=acceptances[0].event_id,
                approval_grant_hash=None if approval_grant_id is None else _hash(approval_grant_id.encode()))
            prepared.update(envelope=envelope, binding=binding, manifest=policies[0],
                            operation=_operation_id(task.context.model_dump()))
            return None

        corrupt = False
        try:
            self._events.append_checked("scheduler", "global", "workspace-worker-snapshot", prepare)
        except (_OperationCorrupt, sqlite3.Error, OSError, ToolAuditUnavailable):
            corrupt = True
        except Exception:
            return WorkspaceExecutionResult(outcome="blocked", reason_code="authority_unavailable")
        if corrupt:
            raise ToolAuditUnavailable("workspace operation replay failed") from None
        if "replay" in prepared:
            return prepared["replay"]
        envelope, binding, manifest, operation = (prepared[key] for key in
            ("envelope", "binding", "manifest", "operation"))
        request = WorkspaceWriteRequest(request_id=operation, **identity,
            role=binding.role, causation_id=binding.causation_id,
            workspace=str(host.workspace.resolve(strict=True)), command=program.command)
        authority = DurableAttemptAuthority(self._database, workspace=host.workspace,
            policy_manifest_hash=manifest.content_hash, clock=host.clock,
            expected_context=binding.context)
        # Ownership is committed in SQLite before any untrusted process launch.
        owned = False
        replay = None
        def reserve(events, _version):
            nonlocal owned, replay
            if events:
                replay = self._replay(events, binding, operation)
                return None
            owned = True
            return [self._draft("WorkspaceWorkerOperationRequested",
                               binding.model_dump(mode="json"), binding)]
        reservation_failed = False
        try:
            self._events.append_checked(OPERATION_STREAM, operation, "request", reserve)
        except Exception:
            reservation_failed = True
        if reservation_failed:
            raise ToolAuditUnavailable("workspace operation reservation failed") from None
        if not owned:
            return replay or WorkspaceExecutionResult(outcome="outcome_unknown",
                reason_code="operation_in_progress", operation_id=operation)

        result = None
        worker = None
        audit_failed = False
        try:
            if not authority.is_current(request):
                result = self._result(operation, "blocked", "authority_unavailable")
            else:
                admission = verify_workspace_isolation(host.workspace, limits=host.limits)
                if admission.outcome != "eligible" or admission.workspace_identity_hash != binding.workspace_identity_hash:
                    result = self._result(operation, "blocked", "isolation_unavailable")
            if result is None:
                try:
                    gateway = WorkspaceWriteGateway(run_id=run_id, workspace=host.workspace,
                        event_store=self._events, policy_manifest=manifest, attempt_authority=authority,
                        policy_state=host.policy_state, lease_root=host.lease_root,
                        journal_root=host.journal_root, limits=host.limits,
                        isolation_profile_hash=admission.profile_hash, approval_service=self._approvals,
                        approval_requester=host.approval_requester, approval_now=host.clock,
                        retain_publication=True)
                except ValueError:
                    raise IsolationNotStarted("workspace host isolation configuration is unavailable") from None
            if result is None:
                policy_state = host.policy_state(request)
                if not isinstance(policy_state, PolicyState) or policy_state.revoked or policy_state.emergency_denied:
                    result = self._result(operation, "blocked", "policy_denied")
                elif not authority.is_current(request):
                    result = self._result(operation, "blocked", "authority_unavailable")
            if result is None:
                worker = IsolatedWorkspaceWorker(limits=host.limits)
                proposal = worker.propose(host.workspace, envelope)
                validate_workspace_proposal(envelope, proposal)
                if worker.termination_receipt is None:
                    raise WorkspaceWorkerError("worker stop is unknown")
                self._record_worker_stop(binding, operation, worker.termination_receipt)
                if not authority.is_current(request):
                    result = self._result(operation, "blocked", "authority_unavailable")
                else:
                    output = gateway.execute(request, approval_grant_id=approval_grant_id)
                    if output.outcome == "completed" and output.publication_receipt is not None and output.termination_confirmed:
                        result = WorkspaceExecutionResult(outcome="published", reason_code="workspace_published",
                            operation_id=operation, publication_receipt=PublicationReceipt(**asdict(output.publication_receipt)))
                    elif output.outcome == "awaiting_approval":
                        result = WorkspaceExecutionResult(outcome="awaiting_approval", reason_code="approval_required",
                            operation_id=operation, approval_request_id=output.approval_request_id)
                    elif output.outcome == "blocked":
                        result = self._result(operation, "blocked", "isolation_unavailable")
                    elif output.outcome == "execution_unknown" or not output.termination_confirmed:
                        result = self._result(operation, "outcome_unknown", "execution_unknown")
                    elif output.outcome == "denied":
                        result = self._result(operation, "blocked", "policy_denied")
                    else:
                        result = self._result(operation, "failed", "execution_failed")
        except IsolationNotStarted:
            result = self._result(operation, "blocked", "isolation_unavailable")
        except WorkspaceWorkerError:
            result = self._result(operation, "failed", "worker_failed") if worker is not None and worker.termination_receipt is not None else self._result(operation, "outcome_unknown", "execution_unknown")
        except ToolAuditUnavailable:
            # An audit failure cannot be converted to an ordinary persisted block.
            audit_failed = True
        except Exception:
            result = self._result(operation, "outcome_unknown", "execution_unknown")
        if audit_failed:
            raise ToolAuditUnavailable("workspace execution audit failed; evidence retained") from None
        self._finish(binding, operation, result, worker)
        if result.publication_receipt is not None:
            # Service terminal receipt is durable before releasing publisher evidence.
            # Failure retains committed evidence for explicit recovery (Task 5).
            try:
                with acquire_workspace_write_lease(host.workspace, host.lease_root) as lease:
                    acknowledge_workspace_publication(host.workspace, lease, host.journal_root,
                        WorkspacePublishReceipt(**result.publication_receipt.model_dump()))
            except Exception:
                pass
        return result

    def reconcile_publication(self, *, run_id: str, operation_id: str) -> WorkspaceExecutionResult:
        """Explicitly reconcile one exact, durably admitted workspace operation.

        Startup and ordinary execution only validate/replay historical state. This
        method is the sole path that may recover a retained publication journal.
        It never launches a Worker or candidate process and fails closed unless
        the durable operation, Gateway receipts, process-stop proofs, workspace
        identity, and one exact journal transaction agree.
        """

        if (type(run_id) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]*", run_id) is None
                or type(operation_id) is not str or re.fullmatch(_HASH, operation_id) is None):
            return WorkspaceExecutionResult(outcome="blocked", reason_code="workspace_input_invalid")
        if self._host is None:
            return WorkspaceExecutionResult(outcome="blocked", reason_code="isolation_unavailable")

        unknown = self._result(operation_id, "outcome_unknown", "execution_unknown")
        host = self._host
        try:
            events = self._events.read_stream(OPERATION_STREAM, operation_id)
            if not events:
                return unknown
            binding = _Binding.model_validate(events[0].payload)
            if (binding.context.run_id != run_id
                    or operation_id != _operation_id(binding.context.model_dump())):
                return unknown
            replay = self._replay(events, binding, operation_id)
            if replay is not None:
                return self._reconcile_terminal_publication(binding, operation_id, replay)

            # Only a durable Worker stop plus all exact Gateway evidence can
            # authorize touching a publication journal. A Requested-only or
            # Worker-only stream remains unknown and retains scheduler occupancy.
            if len(events) != 2 or events[1].event_type != "WorkspaceWorkerStopped":
                return unknown
            worker_stop_payload = events[1].payload
            worker_stop = SandboxTerminationReceipt(**worker_stop_payload["termination_receipt"])
            if worker_stop_payload.get("proposal_hash") != binding.proposal_hash:
                return unknown

            current_identity = workspace_identity_hash(host.workspace)
            if current_identity != binding.workspace_identity_hash:
                return unknown
            admission = verify_workspace_isolation(host.workspace, limits=host.limits)
            if admission.outcome != "eligible" or admission.workspace_identity_hash != binding.workspace_identity_hash:
                return unknown

            evidence = self._publication_reconciliation_evidence(binding, operation_id, admission.profile_hash)
            if evidence is None:
                return unknown
            (intent, run_intent, workspace_intent, worker_stop, profile_hash,
             tool_started, candidate_result, approval_evidence) = evidence

            with acquire_workspace_write_lease(host.workspace, host.lease_root) as lease:
                lease.assert_current()
                admission = verify_workspace_isolation(host.workspace, limits=host.limits)
                if (admission.outcome != "eligible"
                        or admission.workspace_identity_hash != binding.workspace_identity_hash
                        or admission.profile_hash != profile_hash):
                    return unknown
                journal_names = sorted(os.listdir(host.journal_root))
                expected_names = {
                    "publish-" + intent["transaction_id"],
                    "staging-" + intent["transaction_id"],
                }
                if len(journal_names) != 1 or journal_names[0] not in expected_names:
                    return unknown
                snapshot = inspect_workspace_publication_journal(
                    host.workspace, lease, host.journal_root, intent["transaction_id"]
                )
                if (snapshot is None or snapshot.status == "unbound"
                        or snapshot.manifest_hash != intent["manifest_hash"]
                        or snapshot.lease_generation != intent["lease_generation"]):
                    return unknown
                if snapshot.status not in {"preparing", "prepared", "committed", "rolled_back", "not_started"}:
                    return unknown

                completed, aborted = self._publication_resolution_events(
                    binding, operation_id, intent, run_intent, workspace_intent
                )
                if snapshot.status in {"preparing", "prepared", "rolled_back", "not_started"} and completed:
                    return unknown
                if snapshot.status == "committed" and aborted:
                    return unknown
                # A schema-2 preparing record can prove no mutation started; a
                # prepared/rolled-back record must agree with the intended diff.
                if (snapshot.status in {"prepared", "rolled_back", "committed"}
                        and snapshot.entries_published != intent["entries"]):
                    return unknown

                recoveries = recover_workspace_publications_with_outcomes(
                    host.workspace, lease, host.journal_root
                )
                if (len(recoveries) != 1 or recoveries[0].transaction_id != intent["transaction_id"]):
                    return unknown
                recovery = recoveries[0]
                if (recovery.manifest_hash != intent["manifest_hash"]
                        or recovery.lease_generation != intent["lease_generation"]):
                    return unknown
                if recovery.outcome == "committed":
                    if recovery.entries_published != intent["entries"]:
                        return unknown
                    receipt = WorkspacePublishReceipt(
                        transaction_id=recovery.transaction_id,
                        manifest_hash=recovery.manifest_hash,
                        entries_published=recovery.entries_published,
                        lease_generation=recovery.lease_generation,
                    )
                    if approval_evidence is not None:
                        self._record_reconciled_approval_effect(
                            binding, approval_evidence, receipt
                        )
                    self._record_reconciled_publication(
                        binding, operation_id, intent, receipt, run_intent, workspace_intent,
                        tool_started, candidate_result.payload,
                    )
                    result = WorkspaceExecutionResult(
                        outcome="published", reason_code="workspace_published",
                        operation_id=operation_id, publication_receipt=PublicationReceipt(
                            **asdict(receipt)
                        ),
                    )
                    worker = SimpleNamespace(termination_receipt=worker_stop)
                    self._finish(binding, operation_id, result, worker)
                    acknowledge_workspace_publication(host.workspace, lease, host.journal_root, receipt)
                    return result
                if recovery.outcome in {"not_started", "rolled_back"}:
                    if recovery.entries_published not in {0, intent["entries"]}:
                        return unknown
                    self._record_reconciled_abort(
                        binding, operation_id, intent, recovery, run_intent, workspace_intent
                    )
                    result = self._result(operation_id, "failed", "execution_failed")
                    worker = SimpleNamespace(termination_receipt=worker_stop)
                    self._finish(binding, operation_id, result, worker)
                    acknowledge_workspace_recovery(host.workspace, lease, host.journal_root, recovery)
                    return result
                return unknown
        except Exception:
            # Validation, filesystem, and audit uncertainty all preserve the
            # durable journal and unknown-stop evidence for later investigation.
            return unknown

    def _publication_reconciliation_evidence(self, binding, operation, profile_hash):
        context = binding.context
        run_id = context.run_id
        workspace_stream = "workspace:" + binding.workspace_identity_hash[7:]
        run_events = self._events.read_stream("security", run_id)
        workspace_events = self._events.read_stream("workspace_publications", workspace_stream)
        execution_events = self._events.read_stream("workspace_executions", workspace_stream)
        try:
            unresolved_run = _unresolved_publications(run_events, run_id=run_id)
            unresolved_workspace = _unresolved_publications(workspace_events)
            unresolved_execution = _unresolved_executions(execution_events, binding.workspace_identity_hash)
        except Exception:
            return None
        identity = (run_id, operation)
        if (unresolved_run - {identity} or unresolved_workspace - {identity}
                or unresolved_execution):
            return None

        def exact_context(event, causation_id=None):
            return (
                (event.run_id, event.node_id, event.attempt_id, event.fencing_generation,
                 event.correlation_id)
                == (run_id, context.node_id, context.attempt_id,
                    context.fencing_generation, None)
                and (causation_id is None or event.causation_id == causation_id)
            )

        def matching(events, event_type):
            return [event for event in events if event.event_type == event_type
                    and event.run_id == run_id and event.payload.get("request_id") == operation]

        run_intents = matching(run_events, "WorkspacePublicationIntent")
        workspace_intents = matching(workspace_events, "WorkspacePublicationIntent")
        if len(run_intents) != 1 or len(workspace_intents) != 1:
            return None
        run_intent, workspace_intent = run_intents[0], workspace_intents[0]
        intent = run_intent.payload
        required = {"request_id", "transaction_id", "request_hash", "manifest_hash", "entries",
                    "bytes", "workspace_identity_hash", "policy_manifest_hash", "lease_generation"}
        if (set(intent) != required or workspace_intent.payload != intent
                or not exact_context(run_intent, binding.causation_id)
                or not exact_context(workspace_intent, binding.causation_id)
                or re.fullmatch(r"[0-9a-f]{32}", intent.get("transaction_id", "")) is None
                or re.fullmatch(_HASH, intent.get("request_hash", "")) is None
                or re.fullmatch(_HASH, intent.get("manifest_hash", "")) is None
                or re.fullmatch(_HASH, intent.get("workspace_identity_hash", "")) is None
                or re.fullmatch(_HASH, intent.get("policy_manifest_hash", "")) is None
                or intent["request_id"] != operation
                or intent["workspace_identity_hash"] != binding.workspace_identity_hash
                or intent["policy_manifest_hash"] != context.policy_manifest_hash
                or type(intent["entries"]) is not int or intent["entries"] < 0
                or type(intent["bytes"]) is not int or intent["bytes"] < 0
                or type(intent["lease_generation"]) is not int or intent["lease_generation"] < 1):
            return None

        starts = matching(run_events, "ToolExecutionStarted")
        if (len(starts) != 1 or starts[0].payload.get("request_hash") != intent["request_hash"]
                or not exact_context(starts[0])):
            return None
        candidate_results = matching(run_events, "ToolExecutionResultCaptured")
        if (len(candidate_results) != 1
                or not exact_context(candidate_results[0], starts[0].event_id)
                or run_events.index(candidate_results[0]) <= run_events.index(run_intent)):
            return None
        candidate_result = candidate_results[0]
        candidate_payload = candidate_result.payload
        candidate_fields = {
            "request_id", "outcome", "returncode", "stdout_sha256", "stdout_bytes",
            "stderr_sha256", "stderr_bytes", "termination_confirmed", "cancelled",
            "timed_out", "output_limited", "elapsed_milliseconds",
        }
        if (set(candidate_payload) != candidate_fields
                or candidate_payload.get("request_id") != operation
                or candidate_payload.get("outcome") != "completed"
                or type(candidate_payload.get("returncode")) is not int
                or candidate_payload.get("returncode") != 0
                or re.fullmatch(_HASH, candidate_payload.get("stdout_sha256", "")) is None
                or type(candidate_payload.get("stdout_bytes")) is not int
                or candidate_payload["stdout_bytes"] < 0
                or re.fullmatch(_HASH, candidate_payload.get("stderr_sha256", "")) is None
                or type(candidate_payload.get("stderr_bytes")) is not int
                or candidate_payload["stderr_bytes"] < 0
                or candidate_payload.get("termination_confirmed") is not True
                or candidate_payload.get("cancelled") is not False
                or candidate_payload.get("timed_out") is not False
                or candidate_payload.get("output_limited") is not False
                or type(candidate_payload.get("elapsed_milliseconds")) is not int
                or candidate_payload["elapsed_milliseconds"] < 0):
            return None
        terminal_types = ("ToolExecutionCompleted", "ToolExecutionCancelled", "ToolExecutionFailed",
                          "ToolExecutionOutcomeUnknown")
        terminals = [event for event in run_events if event.event_type in terminal_types
                     and event.run_id == run_id and event.payload.get("request_id") == operation]
        if len(terminals) > 1 or (terminals and not exact_context(terminals[0], starts[0].event_id)):
            return None
        if terminals:
            if terminals[0].event_type != "ToolExecutionCompleted":
                return None
            expected_terminal = {
                **candidate_payload,
                "publication_transaction_id": intent["transaction_id"],
                "publication_manifest_hash": intent["manifest_hash"],
                "entries_published": intent["entries"],
                "lease_generation": intent["lease_generation"],
            }
            if (terminals[0].payload != expected_terminal
                    or run_events.index(candidate_result) >= run_events.index(terminals[0])):
                return None

        execution_intents = [event for event in execution_events
                             if event.event_type == "WorkspaceExecutionIntended"
                             and event.run_id == run_id and event.payload.get("request_id") == operation]
        execution_stops = [event for event in execution_events
                           if event.event_type == "WorkspaceExecutionStopped"
                           and event.run_id == run_id and event.payload.get("request_id") == operation]
        if len(execution_intents) != 1 or len(execution_stops) != 1:
            return None
        execution_intent, execution_stop = execution_intents[0], execution_stops[0]
        common = {"request_id": operation, "workspace_identity_hash": binding.workspace_identity_hash,
                  "request_hash": intent["request_hash"], "profile_hash": profile_hash}
        if (execution_intent.payload != common or execution_stop.payload.get("proof") != "terminated"
                or execution_stop.payload.get("termination_receipt") is None
                or any(execution_stop.payload.get(key) != value for key, value in common.items())
                or execution_stop.payload.get("execution_intent_event_id") != execution_intent.event_id
                or not exact_context(execution_intent, binding.causation_id)
                or not exact_context(execution_stop, execution_intent.event_id)):
            return None
        tool_stop = SandboxTerminationReceipt(**execution_stop.payload["termination_receipt"])
        worker_events = self._events.read_stream(OPERATION_STREAM, operation)
        worker_stop = SandboxTerminationReceipt(**worker_events[1].payload["termination_receipt"])
        if tool_stop.unit_name == worker_stop.unit_name:
            return None

        # An operation carrying approval authority can be reconciled only when
        # the exact accepted attempt and its already-consumed effect receipt are
        # durable. Recovery never binds, consumes, or repeats an approval.
        approval_evidence = None
        if binding.approval_grant_hash is not None:
            approval_evidence = self._publication_approval_evidence(
                binding, operation, intent, profile_hash, run_events, starts[0]
            )
            if approval_evidence is None:
                return None
        elif matching(run_events, "ToolApprovalConsumed"):
            return None

        return (
            intent, run_intent, workspace_intent, worker_stop, profile_hash,
            starts[0], candidate_result, approval_evidence,
        )

    def _publication_approval_evidence(
        self, binding, operation, intent, profile_hash, security, tool_started
    ):
        context = binding.context

        def exact_context(event, causation_id=None):
            return (
                (event.run_id, event.node_id, event.attempt_id, event.fencing_generation,
                 event.correlation_id)
                == (context.run_id, context.node_id, context.attempt_id,
                    context.fencing_generation, None)
                and (causation_id is None or event.causation_id == causation_id)
            )

        bound = [event for event in security if event.event_type == "ApprovalGrantBound"
                 and event.run_id == context.run_id and event.attempt_id == context.attempt_id
                 and event.fencing_generation == context.fencing_generation]
        consumed = [event for event in security if event.event_type == "ToolApprovalConsumed"
                    and event.run_id == context.run_id
                    and event.payload.get("request_id") == operation]
        if len(bound) != 1 or len(consumed) != 1:
            return None

        bound_event = bound[0]
        bound_payload = bound_event.payload
        bound_fields = {
            "approval_request_id", "approval_grant_id", "binding_hash", "attempt_id",
            "fencing_generation", "isolation_profile_hash", "policy_manifest_hash",
            "revocation_version", "emergency_deny_version",
        }
        grant_id = bound_payload.get("approval_grant_id")
        approval_request_id = bound_payload.get("approval_request_id")
        if (set(bound_payload) != bound_fields
                or type(grant_id) is not str or _hash(grant_id.encode()) != binding.approval_grant_hash
                or type(approval_request_id) is not str
                or bound_payload.get("attempt_id") != context.attempt_id
                or bound_payload.get("fencing_generation") != context.fencing_generation
                or bound_payload.get("isolation_profile_hash") != profile_hash
                or bound_payload.get("policy_manifest_hash") != context.policy_manifest_hash
                or re.fullmatch(_HASH, bound_payload.get("binding_hash", "")) is None
                or type(bound_payload.get("revocation_version")) is not int
                or type(bound_payload.get("emergency_deny_version")) is not int
                or not exact_context(bound_event)):
            return None

        consumed_event_from_security = consumed[0]
        consumed_payload = consumed_event_from_security.payload
        consumed_fields = {
            "request_id", "approval_request_id", "approval_grant_id", "effect_id",
            "effect_intent_event_id", "approval_consumed_event_id", "budget_reservation_id",
            "request_hash",
        }
        if (set(consumed_payload) != consumed_fields
                or consumed_payload.get("request_id") != operation
                or consumed_payload.get("approval_request_id") != approval_request_id
                or consumed_payload.get("approval_grant_id") != grant_id
                or consumed_payload.get("request_hash") != intent["request_hash"]
                or type(consumed_payload.get("effect_id")) is not str
                or type(consumed_payload.get("budget_reservation_id")) is not str
                or not exact_context(consumed_event_from_security)):
            return None

        budget = self._events.read_stream("budget", context.run_id)
        effect_id = consumed_payload["effect_id"]
        intent_events = [event for event in budget if event.event_type == "EffectIntentRecorded"
                         and event.payload.get("approval_grant_id") == grant_id
                         and event.payload.get("effect_id") == effect_id]
        consumed_events = [event for event in budget if event.event_type == "ApprovalGrantConsumed"
                           and event.payload.get("approval_grant_id") == grant_id]
        if len(intent_events) != 1 or len(consumed_events) != 1:
            return None
        intent_event, consumed_event = intent_events[0], consumed_events[0]
        intent_payload = intent_event.payload
        intent_fields = {
            "effect_id", "approval_grant_id", "effect_intent_hash", "request_hash",
            "target_hash", "provider_idempotency_key_hash", "maximum_cost_minor",
            "recovery_class",
        }
        consumed_budget_payload = consumed_event.payload
        consumed_budget_fields = {
            "approval_grant_id", "approval_request_id", "effect_id", "effect_intent_hash",
            "binding_hash",
        }
        if (set(intent_payload) != intent_fields
                or intent_payload.get("effect_id") != effect_id
                or intent_payload.get("approval_grant_id") != grant_id
                or set(consumed_budget_payload) != consumed_budget_fields
                or consumed_budget_payload.get("approval_grant_id") != grant_id
                or consumed_budget_payload.get("approval_request_id") != approval_request_id
                or consumed_budget_payload.get("effect_id") != effect_id
                or consumed_budget_payload.get("effect_intent_hash") != intent_payload.get("effect_intent_hash")
                or consumed_budget_payload.get("binding_hash") != bound_payload.get("binding_hash")
                or consumed_payload.get("effect_intent_event_id") != intent_event.event_id
                or consumed_payload.get("approval_consumed_event_id") != consumed_event.event_id
                or not exact_context(intent_event, bound_event.event_id)
                or not exact_context(consumed_event, bound_event.event_id)
                or not exact_context(consumed_event_from_security, consumed_event.event_id)
                or not exact_context(tool_started, intent_event.event_id)
                or tool_started.payload.get("approval_grant_id") != grant_id
                or tool_started.payload.get("effect_intent_event_id") != intent_event.event_id):
            return None

        expected_receipt_hash = _hash_json({
            "transaction_id": intent["transaction_id"],
            "manifest_hash": intent["manifest_hash"],
            "entries_published": intent["entries"],
            "lease_generation": intent["lease_generation"],
        })
        effect_receipts = [
            event for event in budget if event.event_type == "EffectReceiptRecorded"
            and (event.payload.get("effect_id") == effect_id
                 or event.payload.get("approval_grant_id") == grant_id)
        ]
        if len(effect_receipts) > 1:
            return None
        receipt_present = bool(effect_receipts)
        if receipt_present:
            effect_receipt = effect_receipts[0]
            if (effect_receipt.payload != {
                    "effect_id": effect_id, "approval_grant_id": grant_id,
                    "outcome": "applied", "receipt_hash": expected_receipt_hash,
                }
                    or not exact_context(effect_receipt, intent_event.event_id)):
                return None

        if self._approvals is None:
            return None
        reservation_id = consumed_payload["budget_reservation_id"]
        reservation = self._approvals._budget.get_reservation(
            reservation_id, run_id=context.run_id
        )
        if (reservation.reservation_id != reservation_id
                or reservation.run_id != context.run_id
                or reservation.node_id != context.node_id
                or reservation.attempt_id != context.attempt_id
                or reservation.fencing_generation != context.fencing_generation
                or reservation.causation_id != bound_event.event_id):
            return None

        return {
            "consumed": ConsumedApproval(
                approval_request_id=approval_request_id,
                approval_grant_id=grant_id,
                effect_id=effect_id,
                intent_event_id=intent_event.event_id,
                consumed_event_id=consumed_event.event_id,
                reservation=reservation,
            ),
            "attempt": ExecutionAttempt(
                run_id=context.run_id,
                node_id=context.node_id,
                attempt_id=context.attempt_id,
                fencing_generation=context.fencing_generation,
                isolation_profile_hash=profile_hash,
            ),
            "receipt_hash": expected_receipt_hash,
            "receipt_present": receipt_present,
        }

    def _publication_resolution_events(self, binding, operation, intent, run_intent, workspace_intent):
        streams = (
            ("security", binding.context.run_id, run_intent),
            ("workspace_publications", "workspace:" + binding.workspace_identity_hash[7:], workspace_intent),
        )
        completion_payloads, abort_payloads = [], []
        for stream_type, stream_id, intent_event in streams:
            events = self._events.read_stream(stream_type, stream_id)
            matches = [event for event in events if event.run_id == binding.context.run_id
                       and event.payload.get("request_id") == operation
                       and event.event_type in {"WorkspacePublicationCompleted", "WorkspacePublicationAborted"}]
            if len(matches) > 1:
                raise ValueError("publication has conflicting resolution receipts")
            if not matches:
                continue
            event = matches[0]
            if (event.node_id, event.attempt_id, event.fencing_generation, event.correlation_id,
                event.causation_id) != (binding.context.node_id, binding.context.attempt_id,
                binding.context.fencing_generation, None, intent_event.event_id):
                raise ValueError("publication resolution context conflicts with intent")
            if (event.event_type == "WorkspacePublicationCompleted"
                    and event.payload != {**intent, "entries_published": intent["entries"]}):
                # Completion payloads include the original lease generation;
                # the exact journal lease is checked separately below.
                expected = {**intent, "entries_published": intent["entries"]}
                if event.payload != expected:
                    raise ValueError("publication completion receipt conflicts with intent")
            if event.event_type == "WorkspacePublicationCompleted":
                completion_payloads.append(event.payload)
            else:
                if (set(event.payload) != {"request_id", "transaction_id", "manifest_hash", "reason"}
                        or event.payload.get("transaction_id") != intent["transaction_id"]
                        or event.payload.get("manifest_hash") != intent["manifest_hash"]
                        or type(event.payload.get("reason")) is not str):
                    raise ValueError("publication abort receipt conflicts with intent")
                abort_payloads.append(event.payload)
        if completion_payloads and abort_payloads:
            raise ValueError("publication has both completed and aborted receipts")
        if abort_payloads and any(payload != abort_payloads[0] for payload in abort_payloads):
            raise ValueError("workspace and Run abort receipts conflict")
        return completion_payloads, abort_payloads

    def _record_reconciled_approval_effect(self, binding, evidence, receipt):
        expected_hash = _hash_json({
            "transaction_id": receipt.transaction_id,
            "manifest_hash": receipt.manifest_hash,
            "entries_published": receipt.entries_published,
            "lease_generation": receipt.lease_generation,
        })
        if expected_hash != evidence["receipt_hash"]:
            raise ValueError("approval receipt hash conflicts with recovered publication")
        if not evidence["receipt_present"]:
            if self._approvals is None:
                raise ValueError("approval service is unavailable for explicit reconciliation")
            self._approvals.record_effect_receipt(
                evidence["consumed"], evidence["attempt"],
                outcome="applied", receipt_hash=expected_hash,
            )

    def _record_reconciled_publication(
        self, binding, operation, intent, receipt, run_intent, workspace_intent,
        tool_started, candidate_payload,
    ):
        payload = {**intent, "entries_published": receipt.entries_published}
        self._append_reconciliation_receipt(
            "security", binding.context.run_id, f"workspace-publication-receipt:{operation}",
            "WorkspacePublicationCompleted", payload, binding, run_intent.event_id, operation,
        )
        tool_payload = {
            **candidate_payload,
            "publication_transaction_id": receipt.transaction_id,
            "publication_manifest_hash": receipt.manifest_hash,
            "entries_published": receipt.entries_published,
            "lease_generation": receipt.lease_generation,
        }
        self._append_reconciliation_receipt(
            "security", binding.context.run_id, f"tool-terminal:{operation}",
            "ToolExecutionCompleted", tool_payload, binding, tool_started.event_id, operation,
        )
        self._append_reconciliation_receipt(
            "workspace_publications", "workspace:" + binding.workspace_identity_hash[7:],
            f"workspace-publication-receipt:{binding.context.run_id}:{operation}",
            "WorkspacePublicationCompleted", payload, binding, workspace_intent.event_id, operation,
        )

    def _record_reconciled_abort(self, binding, operation, intent, recovery, run_intent, workspace_intent):
        existing_completed, existing_aborted = self._publication_resolution_events(
            binding, operation, intent, run_intent, workspace_intent
        )
        if existing_completed:
            raise ValueError("cannot abort a publication with a completion receipt")
        reason = (existing_aborted[0]["reason"] if existing_aborted
                  else "explicit_reconciliation_not_applied")
        payload = {"request_id": operation, "transaction_id": recovery.transaction_id,
                   "manifest_hash": recovery.manifest_hash, "reason": reason}
        self._append_reconciliation_receipt(
            "security", binding.context.run_id, f"workspace-publication-aborted:{operation}",
            "WorkspacePublicationAborted", payload, binding, run_intent.event_id, operation,
        )
        self._append_reconciliation_receipt(
            "workspace_publications", "workspace:" + binding.workspace_identity_hash[7:],
            f"workspace-publication-aborted:{binding.context.run_id}:{operation}",
            "WorkspacePublicationAborted", payload, binding, workspace_intent.event_id, operation,
        )

    def _append_reconciliation_receipt(self, stream_type, stream_id, idempotency_key,
                                       event_type, payload, binding, causation_id, operation):
        def append(events, _version):
            matching = [event for event in events if event.event_type == event_type
                        and event.run_id == binding.context.run_id
                        and event.payload.get("request_id") == operation]
            if matching:
                if len(matching) != 1 or matching[0].payload != payload:
                    raise ValueError("publication receipt replay conflicts with evidence")
                event = matching[0]
                if (event.node_id, event.attempt_id, event.fencing_generation, event.correlation_id,
                    event.causation_id) != (binding.context.node_id, binding.context.attempt_id,
                    binding.context.fencing_generation, None, causation_id):
                    raise ValueError("publication receipt context conflicts with evidence")
                return None
            return [EventDraft(event_type, payload, run_id=binding.context.run_id,
                node_id=binding.context.node_id, attempt_id=binding.context.attempt_id,
                fencing_generation=binding.context.fencing_generation,
                causation_id=causation_id)]
        self._events.append_checked(stream_type, stream_id, idempotency_key, append)

    def _reconcile_terminal_publication(self, binding, operation, result):
        if result.outcome != "published" or self._host is None:
            return result
        host = self._host
        try:
            with acquire_workspace_write_lease(host.workspace, host.lease_root) as lease:
                names = sorted(os.listdir(host.journal_root))
                if not names:
                    return result
                receipt = WorkspacePublishReceipt(**result.publication_receipt.model_dump())
                expected_names = {"publish-" + receipt.transaction_id,
                                  "acknowledged-" + receipt.transaction_id}
                if len(names) != 1 or names[0] not in expected_names:
                    return self._result(operation, "outcome_unknown", "execution_unknown")
                snapshot = inspect_workspace_publication_journal(
                    host.workspace, lease, host.journal_root, receipt.transaction_id
                )
                if snapshot is None:
                    return self._result(operation, "outcome_unknown", "execution_unknown")
                if snapshot.status != "committed" or snapshot.manifest_hash != receipt.manifest_hash:
                    return self._result(operation, "outcome_unknown", "execution_unknown")
                if (snapshot.entries_published != receipt.entries_published
                        or snapshot.lease_generation != receipt.lease_generation):
                    return self._result(operation, "outcome_unknown", "execution_unknown")
                acknowledge_workspace_publication(host.workspace, lease, host.journal_root, receipt)
            return result
        except Exception:
            return self._result(operation, "outcome_unknown", "execution_unknown")

    @staticmethod
    def _result(operation, outcome, reason):
        return WorkspaceExecutionResult(outcome=outcome, reason_code=reason, operation_id=operation)

    @staticmethod
    def _draft(kind, payload, binding):
        context = binding.context
        return EventDraft(kind, payload, run_id=context.run_id, node_id=context.node_id,
            attempt_id=context.attempt_id, fencing_generation=context.fencing_generation,
            correlation_id=context.run_id, causation_id=binding.causation_id)

    def _replay(self, events, binding, operation):
        self._validate_historical_binding(binding, operation)
        if (len(events) not in (1, 2, 3) or events[0].event_type != "WorkspaceWorkerOperationRequested"
            or _Binding.model_validate(events[0].payload) != binding):
            raise ValueError("conflicting workspace operation")
        for event in events:
            if (event.run_id, event.node_id, event.attempt_id, event.fencing_generation,
                event.causation_id, event.correlation_id) != (
                    binding.context.run_id, binding.context.node_id, binding.context.attempt_id,
                    binding.context.fencing_generation, binding.causation_id, binding.context.run_id):
                raise ValueError("operation event binding mismatch")
        if len(events) == 1:
            return None
        stop = None
        if events[1].event_type == "WorkspaceWorkerStopped":
            stop = events[1].payload
            if set(stop) != {"proposal_hash", "termination_receipt"} or stop["proposal_hash"] != binding.proposal_hash:
                raise ValueError("Worker stop proposal mismatch")
            from orchestrator.isolation import SandboxTerminationReceipt
            SandboxTerminationReceipt(**stop["termination_receipt"])
            if len(events) == 2:
                return None
        elif len(events) == 3:
            raise ValueError("unexpected operation sequence")
        last = events[-1]
        if last.event_type != "WorkspaceWorkerOperationFinished" or set(last.payload) != {"result", "worker_stop"}:
            raise ValueError("invalid operation terminal")
        result = WorkspaceExecutionResult.model_validate(last.payload["result"])
        if result.operation_id != operation:
            raise ValueError("terminal operation mismatch")
        if stop is not None and last.payload["worker_stop"] != stop["termination_receipt"]:
            raise ValueError("terminal Worker stop conflict")
        if last.payload["worker_stop"] is not None:
            from orchestrator.isolation import SandboxTerminationReceipt
            SandboxTerminationReceipt(**last.payload["worker_stop"])
        elif result.outcome == "published":
            raise ValueError("publication lacks Worker stop proof")
        if result.outcome == "published":
            if stop is None:
                raise ValueError("publication lacks preceding durable Worker stop")
            self._validate_publication(binding, operation, result.publication_receipt, last.payload["worker_stop"])
        return result

    def _record_worker_stop(self, binding, operation, receipt):
        payload = {"proposal_hash": binding.proposal_hash, "termination_receipt": asdict(receipt)}
        def record(events, _version):
            self._replay(events, binding, operation)
            if len(events) != 1:
                raise ValueError("operation no longer owns Worker stop")
            return [self._draft("WorkspaceWorkerStopped", payload, binding)]
        try:
            self._events.append_checked(OPERATION_STREAM, operation, "worker-stopped", record)
            return
        except Exception:
            pass
        raise ToolAuditUnavailable("Worker stop proof could not be recorded") from None

    def _validate_historical_binding(self, binding, operation):
        context = binding.context
        if operation != _operation_id(context.model_dump()):
            raise ValueError("operation identity mismatch")
        expected_scope = _hash(canonical({"context": context.model_dump(mode="json"),
                                          "program_hash": binding.program_hash}))
        if binding.scope_hash != expected_scope:
            raise ValueError("operation scope mismatch")
        state = self._scheduler.lifecycle.replay(context.run_id)
        node = state.node(context.node_id)
        if (state.workspace_identity_hash != binding.workspace_identity_hash
            or state.config_hash != context.effective_config_hash
            or state.registry_hash != context.registry_hash
            or state.policy_manifest_hash != context.policy_manifest_hash
            or state.graph_version < context.graph_version
            or node.spec.role != binding.role
            or node.spec.planning_contract_hash != context.planning_contract_hash
            or "workspace.write-candidate" not in node.spec.tool_ids):
            raise ValueError("operation frozen authority mismatch")
        routes = [event for event in self._events.read_stream("scheduler", "global")
                  if event.event_type == "RoutingDecisionAccepted"
                  and (event.run_id, event.node_id, event.attempt_id, event.fencing_generation) == (
                      context.run_id, context.node_id, context.attempt_id, context.fencing_generation)]
        if len(routes) != 1 or routes[0].payload.get("verification_context") != context.model_dump(mode="json"):
            raise ValueError("operation accepted context mismatch")
        accepted = [event for event in self._events.read_stream("run_lifecycle", context.run_id)
                    if event.event_type == "AttemptAccepted"
                    and (event.node_id, event.attempt_id, event.fencing_generation) == (
                        context.node_id, context.attempt_id, context.fencing_generation)]
        if len(accepted) != 1 or accepted[0].event_id != binding.causation_id:
            raise ValueError("operation acceptance mismatch")

    def _existing(self, identity, text, program, workspace_hash, grant):
        # Historical replay reports an existing outcome; it grants no new authority
        # and remains available after expiry/cancellation. Input must still match.
        state = self._scheduler.lifecycle.replay(identity["run_id"])
        role = state.node(identity["node_id"]).spec.role
        manifest = build_worker_input_manifest(run_id=identity["run_id"], node_id=identity["node_id"],
                                               role=role, task_text=text)
        operation = _operation_id(identity | {"input_manifest_hash": manifest.content_hash})
        events = self._events.read_stream(OPERATION_STREAM, operation)
        if not events:
            return None
        try:
            binding = _Binding.model_validate(events[0].payload)
            if (binding.program_hash != _hash(program.canonical_text.encode())
                or binding.workspace_identity_hash != workspace_hash
                or binding.approval_grant_hash != (None if grant is None else _hash(grant.encode()))):
                raise ValueError("operation request binding changed")
            return self._replay(events, binding, operation) or self._result(
                operation, "outcome_unknown", "operation_in_progress")
        except Exception:
            pass
        raise _OperationCorrupt("workspace operation replay failed") from None

    def validate_operations(self):
        """Read-only startup validation. Never retries or acknowledges old effects."""
        for operation in self._events.stream_ids(OPERATION_STREAM):
            events = self._events.read_stream(OPERATION_STREAM, operation)
            if not events:
                raise ValueError("empty operation stream")
            binding = _Binding.model_validate(events[0].payload)
            self._replay(events, binding, operation)

    def _validate_publication(self, binding, operation, receipt, worker_stop):
        # A service receipt must describe the exact independently audited Gateway
        # publication, with both supervised processes stopped in distinct units.
        expected = (binding.context.run_id, binding.context.node_id,
                    binding.context.attempt_id, binding.context.fencing_generation)
        def bound(event):
            return (event.run_id, event.node_id, event.attempt_id, event.fencing_generation) == expected
        workspace_stream = "workspace:" + binding.workspace_identity_hash[7:]
        run_events = self._events.read_stream("security", binding.context.run_id)
        workspace_events = self._events.read_stream("workspace_publications", workspace_stream)
        intents = []
        for events in (run_events, workspace_events):
            matches = [event for event in events if event.event_type == "WorkspacePublicationIntent"
                       and event.run_id == binding.context.run_id
                       and event.payload.get("request_id") == operation]
            if len(matches) != 1 or not bound(matches[0]) or matches[0].causation_id != binding.causation_id:
                raise ValueError("Gateway publication intent is missing or misbound")
            intents.append(matches[0])
        run_intent, workspace_intent = intents
        intent = run_intent.payload
        required = {"request_id", "transaction_id", "request_hash", "manifest_hash", "entries",
                    "bytes", "workspace_identity_hash", "policy_manifest_hash", "lease_generation"}
        if (set(intent) != required or workspace_intent.payload != intent
                or intent.get("request_id") != operation
                or intent.get("workspace_identity_hash") != binding.workspace_identity_hash
                or intent.get("policy_manifest_hash") != binding.context.policy_manifest_hash
                or intent.get("transaction_id") != receipt.transaction_id
                or intent.get("manifest_hash") != receipt.manifest_hash
                or intent.get("entries") != receipt.entries_published
                or intent.get("lease_generation") != receipt.lease_generation):
            raise ValueError("Gateway publication intent conflicts with service receipt")
        expected_receipt = {**intent, "entries_published": receipt.entries_published}
        for events, intent_event in ((run_events, run_intent), (workspace_events, workspace_intent)):
            completed = [event for event in events if event.event_type == "WorkspacePublicationCompleted"
                         and event.run_id == binding.context.run_id
                         and event.payload.get("request_id") == operation]
            if (len(completed) != 1 or not bound(completed[0])
                    or completed[0].payload != expected_receipt
                    or completed[0].causation_id != intent_event.event_id):
                raise ValueError("service receipt does not match Gateway publication")
        stopped = [event for event in self._events.read_stream("workspace_executions", workspace_stream)
                   if event.payload.get("request_id") == operation
                   and event.run_id == binding.context.run_id
                   and event.payload.get("termination_receipt") is not None]
        from orchestrator.isolation import SandboxTerminationReceipt
        if (len(stopped) != 1 or not bound(stopped[0])
            or stopped[0].event_type != "WorkspaceExecutionStopped"
            or stopped[0].payload.get("proof") != "terminated"
            or stopped[0].correlation_id is not None):
            raise ValueError("tool stop proof missing")
        execution_intents = [event for event in self._events.read_stream("workspace_executions", workspace_stream)
                             if event.event_type == "WorkspaceExecutionIntended"
                             and event.run_id == binding.context.run_id
                             and event.payload.get("request_id") == operation]
        if (len(execution_intents) != 1 or stopped[0].causation_id != execution_intents[0].event_id
                or execution_intents[0].causation_id != binding.causation_id
                or execution_intents[0].correlation_id is not None
                or execution_intents[0].payload.get("request_hash") != intent.get("request_hash")
                or execution_intents[0].payload.get("workspace_identity_hash") != binding.workspace_identity_hash):
            raise ValueError("tool stop proof is not bound to the exact publication request")
        tool_stop = SandboxTerminationReceipt(**stopped[0].payload["termination_receipt"])
        if tool_stop.unit_name == worker_stop["unit_name"]:
            raise ValueError("Worker and tool units are not distinct")

    def _finish(self, binding, operation, result, worker):
        payload = {"result": result.model_dump(mode="json"), "worker_stop":
            asdict(worker.termination_receipt) if worker is not None and worker.termination_receipt is not None else None}
        def finish(events, _version):
            previous = self._replay(events, binding, operation)
            if previous is not None:
                if events[-1].payload != payload:
                    raise ValueError("terminal operation conflict")
                return None
            if result.outcome == "published":
                self._validate_publication(binding, operation, result.publication_receipt, payload["worker_stop"])
            return [self._draft("WorkspaceWorkerOperationFinished", payload, binding)]
        try:
            self._events.append_checked(OPERATION_STREAM, operation, "terminal", finish)
            return
        except Exception:
            pass
        raise ToolAuditUnavailable("workspace terminal receipt failed; evidence retained") from None


__all__ = ["IsolatedWorkspaceWorker", "WorkspaceExecutionHost", "WorkspaceExecutionResult", "WorkspaceExecutionService"]
