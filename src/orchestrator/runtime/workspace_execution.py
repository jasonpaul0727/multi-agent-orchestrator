"""Trusted, durable one-tool Worker composition; publication is not Node success."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import sqlite3
from pathlib import Path
from typing import Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator

from orchestrator.approvals import ApprovalService
from orchestrator.isolation import (
    IsolationNotStarted, SandboxLimits, WorkspacePublishReceipt,
    acquire_workspace_write_lease, acknowledge_workspace_publication, verify_workspace_isolation,
)
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.runtime.contracts import AttemptContext
from orchestrator.runtime.worker_input import build_worker_input_manifest
from orchestrator.runtime.worker_input_service import WorkerInputService
from orchestrator.runtime.workspace_worker import (
    IsolatedWorkspaceWorker, WorkspaceWorkerError, WorkspaceWorkerTask, canonical,
    decode_workspace_program, operation_id, proposal_bytes, validate_workspace_proposal,
)
from orchestrator.security import PolicyManifest
from orchestrator.tools.authority import DurableAttemptAuthority
from orchestrator.tools.gateway import PolicyState, ToolAuditUnavailable, ToolRequest
from orchestrator.tools.workspace_write import WorkspaceWriteGateway, WorkspaceWriteRequest
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
                            operation=operation_id(task.context.model_dump()))
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
        if operation != operation_id(context.model_dump()):
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
        operation = operation_id(identity | {"input_manifest_hash": manifest.content_hash})
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
        for stream, stream_id in (("security", binding.context.run_id),
                ("workspace_publications", "workspace:" + binding.workspace_identity_hash[7:])):
            completed = [event for event in self._events.read_stream(stream, stream_id)
                         if event.event_type == "WorkspacePublicationCompleted"
                         and event.run_id == binding.context.run_id
                         and event.payload.get("request_id") == operation]
            if len(completed) != 1 or not bound(completed[0]) or any(completed[0].payload.get(key) != value
                                          for key, value in receipt.model_dump().items()):
                raise ValueError("service receipt does not match Gateway publication")
        stopped = [event for event in self._events.read_stream("workspace_executions",
                        "workspace:" + binding.workspace_identity_hash[7:])
                   if event.payload.get("request_id") == operation
                   and event.run_id == binding.context.run_id
                   and event.payload.get("termination_receipt") is not None]
        from orchestrator.isolation import SandboxTerminationReceipt
        if (len(stopped) != 1 or not bound(stopped[0])
            or stopped[0].event_type != "WorkspaceExecutionStopped"
            or stopped[0].payload.get("proof") != "terminated"):
            raise ValueError("tool stop proof missing")
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
