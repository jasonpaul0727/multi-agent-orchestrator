"""Attempt-fenced, approval-aware publication of isolated workspace candidates.

This is the only host-facing workspace-write gateway. Candidate code receives
no writable bind of the live workspace; publication remains a separate,
leased, journaled host operation after a durable intent event.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
from typing import Any, Protocol

from orchestrator.approvals import (
    ApprovalError,
    ApprovalInvalid,
    ApprovalPolicyState,
    ApprovalRequest,
    ApprovalService,
    ConsumedApproval,
    EffectIntentSpec,
    ExecutionAttempt,
)
from orchestrator.budget import CostEstimate
from orchestrator.isolation import (
    IsolationUnavailable,
    OverlayCandidateResult,
    SandboxLimits,
    SystemdOverlayCandidateLauncher,
    WorkspacePublishAuthorizationLost,
    WorkspacePublishConflict,
    WorkspacePublishError,
    WorkspacePublishReceipt,
    WorkspaceLeaseError,
    WorkspaceWriteLease,
    acquire_workspace_write_lease,
    publish_workspace_diff,
    recover_workspace_publications,
    recover_workspace_publications_with_outcomes,
)
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.security.policy import PolicyDecision, PolicyEngine, PolicyManifest, PolicyRequest
from orchestrator.workspace_identity import workspace_identity_hash

from .gateway import (
    AttemptAuthority,
    PolicyState,
    ToolAuditUnavailable,
    ToolExecutionResult,
    ToolRequest,
    ToolRequestAlreadyUsed,
    _approval_request_id,
    _digest,
    _event_context,
    _grant_id,
    _hash_json,
)


WORKSPACE_WRITE_TOOL_ID = "workspace.write-candidate"
_SAFE_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$"
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_TERMINALS = frozenset({
    "ToolExecutionCompleted", "ToolExecutionCancelled", "ToolExecutionFailed",
    "ToolExecutionOutcomeUnknown",
})


class WorkspaceWriteRequest(ToolRequest):
    """A distinct, statically routed capability to create reversible changes."""

    @property
    def tool_id(self) -> str:
        return WORKSPACE_WRITE_TOOL_ID


class _EventStore(Protocol):
    def append_checked(
        self,
        stream_type: str,
        stream_id: str,
        idempotency_key: str,
        decide: Callable[[list[StoredEvent], int], Sequence[EventDraft] | None],
    ) -> list[StoredEvent]: ...

    def read_stream(self, stream_type: str, stream_id: str, after_version: int = 0) -> list[StoredEvent]: ...


class _CandidateSession(Protocol):
    def wait(self) -> OverlayCandidateResult: ...

    def cancel(self) -> bool: ...

    def close(self) -> None: ...


class _CandidateLauncher(Protocol):
    def launch(
        self,
        workspace: str | Path,
        command: Sequence[str],
        *,
        limits: SandboxLimits | None = None,
        expected_workspace_identity_hash: str | None = None,
    ) -> _CandidateSession: ...


class WorkspaceWriteGateway:
    """Run code against a private OverlayFS candidate and publish under audit.

    Authority, policy, ApprovalService, and EventStore inputs are trusted host
    capabilities. The execution request itself is never a commit token.
    """

    def __init__(
        self,
        *,
        run_id: str,
        workspace: str | Path,
        event_store: _EventStore,
        policy_manifest: PolicyManifest,
        attempt_authority: AttemptAuthority,
        policy_state: Callable[[WorkspaceWriteRequest], PolicyState],
        lease_root: str | Path,
        journal_root: str | Path,
        launcher: _CandidateLauncher | Any | None = None,
        limits: SandboxLimits | None = None,
        monitor_interval_seconds: float = 0.05,
        isolation_profile_hash: str,
        approval_service: ApprovalService | None = None,
        approval_requester: Callable[[WorkspaceWriteRequest], str] | None = None,
        approval_now: Callable[[], datetime] | None = None,
        approval_ttl_seconds: int = 3_600,
    ) -> None:
        if not isinstance(run_id, str) or re.fullmatch(_SAFE_IDENTIFIER, run_id) is None:
            raise ValueError("run_id must be a stable identifier")
        workspace_path = Path(workspace)
        if not workspace_path.is_absolute() or not workspace_path.is_dir():
            raise ValueError("workspace must be an existing absolute directory")
        canonical_workspace = workspace_path.resolve(strict=True)
        if canonical_workspace == Path(canonical_workspace.anchor):
            raise ValueError("workspace must not be a filesystem root")
        if not isinstance(isolation_profile_hash, str) or not _HASH.fullmatch(isolation_profile_hash):
            raise ValueError("isolation_profile_hash must be a SHA-256 digest")
        if (
            isinstance(monitor_interval_seconds, bool)
            or not isinstance(monitor_interval_seconds, (int, float))
            or not 0.01 <= monitor_interval_seconds <= 1
        ):
            raise ValueError("monitor interval must be between 10 ms and 1 s")
        if isinstance(approval_ttl_seconds, bool) or not isinstance(approval_ttl_seconds, int):
            raise ValueError("approval_ttl_seconds must be an integer")
        if not 1 <= approval_ttl_seconds <= 86_400:
            raise ValueError("approval_ttl_seconds must be between 1 second and 24 hours")
        if approval_service is None:
            if approval_requester is not None:
                raise ValueError("approval requester requires an ApprovalService")
        elif not approval_service.uses_event_store(event_store) or approval_requester is None:
            raise ValueError("approval integration needs the same event store and an authenticated requester")

        lease_path = _private_external_directory(lease_root, canonical_workspace, "workspace lease root")
        journal_path = _private_external_directory(journal_root, canonical_workspace, "publication journal root")
        if _within(lease_path, journal_path) or _within(journal_path, lease_path):
            raise ValueError("lease and publication journal roots must be disjoint")

        self._run_id = run_id
        self._workspace = canonical_workspace
        self._workspace_identity_hash = workspace_identity_hash(canonical_workspace)
        self._publication_stream_id = "workspace:" + self._workspace_identity_hash.removeprefix("sha256:")
        self._events = event_store
        self._manifest = policy_manifest
        self._attempt_authority = attempt_authority
        self._policy_state = policy_state
        self._launcher = launcher or SystemdOverlayCandidateLauncher()
        self._limits = limits or SandboxLimits()
        self._monitor_interval = monitor_interval_seconds
        self._lease_root = lease_path
        self._journal_root = journal_path
        self._profile_hash = isolation_profile_hash
        self._approval_service = approval_service
        self._approval_requester = approval_requester
        self._approval_now = approval_now or (lambda: datetime.now(timezone.utc))
        self._approval_ttl = timedelta(seconds=approval_ttl_seconds)
        self._engine = PolicyEngine()

    def execute(
        self,
        request: WorkspaceWriteRequest,
        *,
        approval_grant_id: str | None = None,
    ) -> ToolExecutionResult:
        if not isinstance(request, WorkspaceWriteRequest):
            request = WorkspaceWriteRequest.model_validate(request)
        if request.run_id != self._run_id:
            raise PermissionError("write request does not belong to this Run")
        if approval_grant_id is not None and re.fullmatch(_SAFE_IDENTIFIER, approval_grant_id) is None:
            raise ValueError("approval_grant_id must be a stable identifier")
        if request.workspace != str(self._workspace):
            self._record_block(request, "workspace_not_bound")
            raise PermissionError("write request workspace is not bound to this Run")
        if not self._attempt_is_current(request):
            self._record_block(request, "attempt_not_current")
            raise PermissionError("attempt is not current")

        state = self._read_policy_state_or_block(request)
        decision = self._decision(request, state)
        request_hash = self._request_hash(request)
        self._record_request(request, request_hash, state, decision)
        if self._has_unresolved_publication(request.run_id):
            self._record_block(request, "prior_publication_outcome_unresolved")
            return ToolExecutionResult(request.request_id, "denied", decision)
        if decision.outcome == "deny":
            return ToolExecutionResult(request.request_id, "denied", decision)
        if decision.outcome == "needs_approval":
            if self._approval_service is None:
                if approval_grant_id is not None:
                    self._record_block(request, "approval_service_unavailable")
                    return ToolExecutionResult(request.request_id, "denied", decision)
                return ToolExecutionResult(request.request_id, "awaiting_approval", decision)
            if approval_grant_id is None:
                try:
                    approval_id = self._create_approval_request(request, decision, state)
                except Exception as exc:
                    self._record_block(request, "approval_request_unavailable")
                    raise ToolAuditUnavailable("exact write approval request could not be recorded") from exc
                return ToolExecutionResult(
                    request.request_id, "awaiting_approval", decision,
                    approval_request_id=approval_id,
                )
        elif approval_grant_id is not None:
            self._record_block(request, "unexpected_approval_grant")
            return ToolExecutionResult(request.request_id, "denied", decision)

        if not self._authorized_now(request, state):
            self._record_block(request, "authority_changed_before_start")
            raise PermissionError("attempt or policy authority changed before write start")

        try:
            lease = acquire_workspace_write_lease(self._workspace, self._lease_root)
        except WorkspaceLeaseError:
            self._record_block(request, "workspace_writer_unavailable")
            return ToolExecutionResult(request.request_id, "denied", decision)
        try:
            recovered = recover_workspace_publications(self._workspace, lease, self._journal_root)
            if recovered or self._has_unresolved_publication(request.run_id):
                self._record_block(request, "prior_publication_requires_reconciliation")
                return ToolExecutionResult(request.request_id, "denied", decision)
            if not self._authorized_now(request, state):
                self._record_block(request, "authority_changed_before_start")
                return ToolExecutionResult(request.request_id, "denied", decision)
            approval_attempt: ExecutionAttempt | None = None
            consumed: ConsumedApproval | None = None
            if approval_grant_id is None:
                self._consume_capability_and_start(request, request_hash, decision, state)
            else:
                try:
                    approval_attempt, consumed = self._consume_approval_and_start(
                        request, request_hash, decision, state, approval_grant_id
                    )
                except ApprovalError:
                    self._record_block(request, "approval_grant_invalid")
                    return ToolExecutionResult(request.request_id, "denied", decision)
            return self._execute_candidate_under_lease(
                request, decision, state, request_hash, lease, consumed, approval_attempt,
            )
        finally:
            lease.close()

    def _execute_candidate_under_lease(
        self,
        request: WorkspaceWriteRequest,
        decision: PolicyDecision,
        state: PolicyState,
        request_hash: str,
        lease: WorkspaceWriteLease,
        consumed: ConsumedApproval | None,
        approval_attempt: ExecutionAttempt | None,
    ) -> ToolExecutionResult:
        session: _CandidateSession | None = None
        try:
            if not self._authorized_now(request, state):
                raise _WriteAuthorityLost("authority_lost_before_launch")
            session = self._launcher.launch(
                self._workspace,
                request.command,
                limits=self._limits,
                expected_workspace_identity_hash=self._workspace_identity_hash,
            )
            result, lost = self._wait_with_authority(session, request, state)
            accepted = not lost and self._authorized_now(request, state)
            if not result.execution.termination_confirmed:
                return self._unknown(request, decision, "candidate_termination_unconfirmed", result)
            if not accepted:
                self._approval_receipt_if_consumed(
                    consumed, approval_attempt, "not_applied", _digest(b"authority-lost-before-publication")
                )
                self._terminal(request, "ToolExecutionCancelled", {"outcome": "authority_lost"})
                return self._result(request, decision, "authority_lost", result)
            if not _candidate_succeeded(result):
                receipt_hash = _digest(json.dumps(
                    _execution_payload(request, result), sort_keys=True
                ).encode())
                self._approval_receipt_if_consumed(consumed, approval_attempt, "not_applied", receipt_hash)
                self._terminal(request, "ToolExecutionFailed", _execution_payload(request, result))
                return self._result(request, decision, "failed", result)
            assert result.diff is not None
            return self._publish(
                request, decision, state, request_hash, result, lease, consumed, approval_attempt
            )
        except _WriteAuthorityLost as exc:
            self._approval_receipt_if_consumed(
                consumed, approval_attempt, "not_applied", _digest(str(exc).encode())
            )
            self._terminal(request, "ToolExecutionCancelled", {
                "outcome": "authority_lost", "reason": str(exc),
            })
            return ToolExecutionResult(request.request_id, "authority_lost", decision)
        except ToolRequestAlreadyUsed:
            raise
        except ToolAuditUnavailable:
            raise
        except Exception as exc:
            self._terminal_best_effort(request, "ToolExecutionOutcomeUnknown", {
                "outcome": "execution_unknown", "reason": _safe_error_code(exc),
            })
            return ToolExecutionResult(
                request.request_id, "execution_unknown", decision, termination_confirmed=False
            )
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

    def _publish(
        self,
        request: WorkspaceWriteRequest,
        decision: PolicyDecision,
        state: PolicyState,
        request_hash: str,
        candidate: OverlayCandidateResult,
        lease: WorkspaceWriteLease,
        consumed: ConsumedApproval | None,
        approval_attempt: ExecutionAttempt | None,
    ) -> ToolExecutionResult:
        assert candidate.diff is not None
        tx_id = os.urandom(16).hex()
        intent: dict[str, Any] | None = None
        try:
            lease.assert_current()
            if not self._authorized_now(request, state):
                raise _WriteAuthorityLost("authority_lost_before_publication")
            intent = {
                "request_id": request.request_id,
                "transaction_id": tx_id,
                "request_hash": request_hash,
                "manifest_hash": candidate.diff.manifest_hash,
                "entries": len(candidate.diff.entries),
                "bytes": candidate.diff.total_bytes,
                "workspace_identity_hash": self._workspace_identity_hash,
                "policy_manifest_hash": self._manifest.content_hash,
                "lease_generation": lease.generation,
            }
            global_intent_decider = lambda events, _version: self._new_event_once(
                events, request, "WorkspacePublicationIntent", intent, request.causation_id,
                require_start=False,
            )
            security_intent_decider = lambda events, _version: self._new_event_once(
                events, request, "WorkspacePublicationIntent", intent, request.causation_id,
            )
            # The workspace-scoped stream serializes unresolved intents across
            # Runs that bind to the same directory identity. The Run security
            # stream remains the human/audit-facing causal trace.
            self._append_required(
                "workspace_publications",
                self._publication_stream_id,
                f"workspace-publication-intent:{request.run_id}:{request.request_id}",
                global_intent_decider,
                "workspace publication intent was not durably audited",
            )
            self._append_required(
                "security",
                request.run_id,
                f"workspace-publication-intent:{request.request_id}",
                security_intent_decider,
                "workspace publication intent was not durably audited",
            )
            receipt = publish_workspace_diff(
                candidate.lower_root,
                self._workspace,
                candidate.diff,
                lease,
                self._journal_root,
                transaction_id=tx_id,
                authorize=lambda: self._authorized_now(request, state),
            )
            self._record_publication_completed(request, intent, receipt, consumed, approval_attempt)
        except _WriteAuthorityLost as exc:
            if intent is None:
                self._approval_receipt_if_consumed(
                    consumed, approval_attempt, "not_applied", _digest(str(exc).encode())
                )
                self._terminal(request, "ToolExecutionCancelled", {"outcome": "authority_lost"})
                return ToolExecutionResult(request.request_id, "authority_lost", decision)
            return self._reconcile_publication_failure(
                request, decision, candidate, lease, tx_id, intent, consumed, approval_attempt,
                reason=str(exc), not_applied_outcome="authority_lost",
            )
        except WorkspacePublishAuthorizationLost as exc:
            return self._reconcile_publication_failure(
                request, decision, candidate, lease, tx_id, intent, consumed, approval_attempt,
                reason=str(exc), not_applied_outcome="authority_lost",
            )
        except WorkspacePublishConflict as exc:
            return self._reconcile_publication_failure(
                request, decision, candidate, lease, tx_id, intent, consumed, approval_attempt,
                reason=str(exc), not_applied_outcome="failed",
            )
        except WorkspacePublishError as exc:
            return self._reconcile_publication_failure(
                request, decision, candidate, lease, tx_id, intent, consumed, approval_attempt,
                reason=str(exc), not_applied_outcome="failed",
            )

        self._terminal(
            request,
            "ToolExecutionCompleted",
            _execution_payload(request, candidate, outcome="completed", receipt=receipt),
        )
        return self._result(request, decision, "completed", candidate, receipt=receipt)

    def _reconcile_publication_failure(
        self,
        request: WorkspaceWriteRequest,
        decision: PolicyDecision,
        candidate: OverlayCandidateResult,
        lease: WorkspaceWriteLease,
        transaction_id: str,
        intent: dict[str, Any] | None,
        consumed: ConsumedApproval | None,
        approval_attempt: ExecutionAttempt | None,
        *,
        reason: str,
        not_applied_outcome: str,
    ) -> ToolExecutionResult:
        try:
            recovered = recover_workspace_publications_with_outcomes(
                self._workspace, lease, self._journal_root
            )
        except Exception:
            return self._publication_outcome_unknown(request, decision)
        recovery = next((item for item in recovered if item.transaction_id == transaction_id), None)

        # The publisher returns success after a durable committed marker even
        # if journal cleanup fails. A remaining prepared journal is resolved
        # only after recovery proves rollback; no journal proves no mutation.
        if recovery is None or recovery.outcome in {"not_started", "rolled_back"}:
            self._approval_receipt_if_consumed(
                consumed, approval_attempt, "not_applied", _digest(reason.encode())
            )
            if intent is not None:
                self._record_publication_aborted(request, intent, reason)
            if not_applied_outcome == "authority_lost":
                self._terminal(request, "ToolExecutionCancelled", {
                    "outcome": "authority_lost", "reason": _reason_code(reason),
                })
                return ToolExecutionResult(request.request_id, "authority_lost", decision)
            self._terminal(request, "ToolExecutionFailed", {
                "reason": _reason_code(reason), "publication_outcome": "not_applied",
            })
            return self._result(request, decision, "failed", candidate)

        if recovery.outcome == "committed" and intent is not None:
            receipt = WorkspacePublishReceipt(
                transaction_id=transaction_id,
                manifest_hash=recovery.manifest_hash or intent["manifest_hash"],
                entries_published=recovery.entries_published,
                lease_generation=recovery.lease_generation or lease.generation,
            )
            self._record_publication_completed(
                request, intent, receipt, consumed, approval_attempt
            )
            self._terminal(
                request,
                "ToolExecutionCompleted",
                _execution_payload(request, candidate, outcome="completed", receipt=receipt),
            )
            return self._result(request, decision, "completed", candidate, receipt=receipt)

        return self._publication_outcome_unknown(request, decision)

    def _publication_outcome_unknown(
        self, request: WorkspaceWriteRequest, decision: PolicyDecision
    ) -> ToolExecutionResult:
        self._terminal(request, "ToolExecutionOutcomeUnknown", {
            "outcome": "execution_unknown", "reason": "publication_recovery_uncertain",
        })
        return ToolExecutionResult(
            request.request_id, "execution_unknown", decision, termination_confirmed=False
        )

    def _record_publication_completed(
        self,
        request: WorkspaceWriteRequest,
        intent: dict[str, Any],
        receipt: WorkspacePublishReceipt,
        consumed: ConsumedApproval | None,
        approval_attempt: ExecutionAttempt | None,
    ) -> None:
        if consumed is not None and approval_attempt is not None:
            effect_hash = _hash_json({
                "transaction_id": receipt.transaction_id,
                "manifest_hash": receipt.manifest_hash,
                "entries_published": receipt.entries_published,
                "lease_generation": receipt.lease_generation,
            })
            self._approval_service.record_effect_receipt(
                consumed, approval_attempt, outcome="applied", receipt_hash=effect_hash
            )
        receipt_payload = {
            **intent,
            "lease_generation": receipt.lease_generation,
            "entries_published": receipt.entries_published,
        }
        security_decider = lambda events, _version: self._new_event_once(
            events, request, "WorkspacePublicationCompleted", receipt_payload,
            _publication_intent_event_id(events, request), require_start=False,
        )
        workspace_decider = lambda events, _version: self._new_event_once(
            events, request, "WorkspacePublicationCompleted", receipt_payload,
            _publication_intent_event_id(events, request), require_start=False,
        )
        # The workspace-wide intent remains unresolved until the Run audit
        # receipt is durable, so its failure keeps every later writer blocked.
        self._append_required(
            "security", request.run_id,
            f"workspace-publication-receipt:{request.request_id}", security_decider,
            "workspace publication receipt was not durably audited",
        )
        self._append_required(
            "workspace_publications", self._publication_stream_id,
            f"workspace-publication-receipt:{request.run_id}:{request.request_id}", workspace_decider,
            "workspace publication receipt was not durably audited",
        )

    def _decision(self, request: WorkspaceWriteRequest, state: PolicyState) -> PolicyDecision:
        policy_request = PolicyRequest(
            request_id=request.request_id,
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=request.fencing_generation,
            role=request.role,
            action_category="reversible_workspace_write",
            tool_id=WORKSPACE_WRITE_TOOL_ID,
            required_permission="workspace-write",
            normalized_request_hash=self._request_hash(request),
            policy_manifest_hash=self._manifest.content_hash,
            revocation_version=state.revocation_version,
            emergency_deny_version=state.emergency_deny_version,
            revoked=state.revoked,
            emergency_denied=state.emergency_denied,
        )
        return self._engine.evaluate(policy_request, self._manifest)

    def _request_hash(self, request: WorkspaceWriteRequest) -> str:
        return _hash_json({
            "request_id": request.request_id,
            "run_id": request.run_id,
            "node_id": request.node_id,
            "attempt_id": request.attempt_id,
            "fencing_generation": request.fencing_generation,
            "role": request.role,
            "causation_id": request.causation_id,
            "tool_id": WORKSPACE_WRITE_TOOL_ID,
            "workspace_identity_hash": self._workspace_identity_hash,
            "command": request.command,
            "limits": _limits_payload(self._limits),
            "isolation_profile_hash": self._profile_hash,
            "policy_manifest_hash": self._manifest.content_hash,
        })

    def _record_request(self, request, request_hash, state, decision) -> None:
        seen = False

        def decide(events, _version):
            nonlocal seen
            existing = [
                event for event in events
                if event.event_type == "ToolRequestReceived"
                and event.payload.get("request_id") == request.request_id
            ]
            if existing:
                if any(event.payload.get("request_hash") != request_hash for event in existing):
                    raise ValueError("request id was reused with different content")
                seen = True
                return None
            context = _event_context(request, request.causation_id)
            drafts = [
                EventDraft("ToolRequestReceived", {
                    "request_id": request.request_id,
                    "request_hash": request_hash,
                    "tool_id": WORKSPACE_WRITE_TOOL_ID,
                    "policy_manifest_hash": self._manifest.content_hash,
                    "revocation_version": state.revocation_version,
                    "emergency_deny_version": state.emergency_deny_version,
                }, **context),
                EventDraft("PolicyDecision", decision.model_dump(mode="json"), **context),
            ]
            if decision.outcome == "allow":
                drafts.append(EventDraft("CapabilityGrant", {
                    "grant_id": _grant_id(request.request_id),
                    "request_id": request.request_id,
                    "action_category": "reversible_workspace_write",
                    "tool_id": WORKSPACE_WRITE_TOOL_ID,
                    "scope_hash": request_hash,
                    "policy_manifest_hash": self._manifest.content_hash,
                    "one_use": True,
                }, **context))
            return drafts

        self._append_required(
            "security", request.run_id, f"tool-request:{request.request_id}", decide,
            "write request and decision were not durably audited",
        )
        if seen:
            raise ToolRequestAlreadyUsed("write request id already exists; execution is not replayed")

    def _consume_capability_and_start(self, request, request_hash, decision, state) -> None:
        seen = False

        def decide(events, _version):
            nonlocal seen
            if not self._authorized_now(request, state):
                raise _WriteAuthorityLost("authority_changed_in_start_transaction")
            scoped = [event for event in events if event.payload.get("request_id") == request.request_id]
            grants = [event for event in scoped if event.event_type == "CapabilityGrant"]
            if any(event.event_type in _TERMINALS | {"ToolExecutionStarted", "ToolCapabilityConsumed"} for event in scoped):
                seen = True
                return None
            if decision.outcome != "allow" or len(grants) != 1:
                raise ToolAuditUnavailable("no unique workspace-write grant exists")
            grant = grants[0]
            if (
                grant.payload.get("scope_hash") != request_hash
                or grant.payload.get("tool_id") != WORKSPACE_WRITE_TOOL_ID
                or grant.payload.get("action_category") != "reversible_workspace_write"
                or grant.payload.get("one_use") is not True
            ):
                raise ToolAuditUnavailable("workspace-write grant scope mismatch")
            return [
                EventDraft("ToolCapabilityConsumed", {
                    "grant_id": _grant_id(request.request_id),
                    "request_id": request.request_id,
                    "scope_hash": request_hash,
                }, **_event_context(request, grant.event_id)),
                EventDraft("ToolExecutionStarted", {
                    "request_id": request.request_id,
                    "request_hash": request_hash,
                    "policy_decision_hash": decision.decision_hash,
                }, **_event_context(request, grant.event_id)),
            ]

        try:
            self._events.append_checked("security", request.run_id, f"tool-start:{request.request_id}", decide)
        except _WriteAuthorityLost as exc:
            self._record_block(request, str(exc))
            raise PermissionError("attempt authority changed before write start") from exc
        except Exception as exc:
            if isinstance(exc, ToolAuditUnavailable):
                raise
            raise ToolAuditUnavailable("workspace-write capability was not durably consumed") from exc
        if seen:
            raise ToolRequestAlreadyUsed("workspace-write request has already started")

    def _create_approval_request(self, request, decision, state) -> str:
        assert self._approval_service is not None and self._approval_requester is not None
        if not self._authorized_now(request, state):
            raise ApprovalInvalid("attempt or policy changed before approval request")
        approval_id = _approval_request_id(request)
        intent = self._effect_intent(request, approval_id)
        now = self._approval_now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("approval clock must return an aware datetime")
        self._approval_service.create_request(ApprovalRequest(
            approval_request_id=approval_id,
            run_id=request.run_id,
            node_id=request.node_id,
            requester_id=self._approval_requester(request),
            origin_attempt_id=request.attempt_id,
            origin_fencing_generation=request.fencing_generation,
            causation_id=request.causation_id,
            action_category=decision.request.action_category,
            tool_id=WORKSPACE_WRITE_TOOL_ID,
            target_hash=intent.target_hash,
            parameters_hash=intent.parameters_hash,
            effect_intent_hash=intent.content_hash,
            policy_manifest_hash=self._manifest.content_hash,
            revocation_version=state.revocation_version,
            emergency_deny_version=state.emergency_deny_version,
            expires_at=now.astimezone(timezone.utc) + self._approval_ttl,
        ))
        return approval_id

    def _effect_intent(self, request: WorkspaceWriteRequest, approval_request_id: str) -> EffectIntentSpec:
        target_hash = _hash_json({
            "tool_id": WORKSPACE_WRITE_TOOL_ID,
            "workspace_identity_hash": self._workspace_identity_hash,
        })
        parameters_hash = _hash_json({
            "command": request.command,
            "limits": _limits_payload(self._limits),
            "isolation_profile_hash": self._profile_hash,
            "policy_manifest_hash": self._manifest.content_hash,
        })
        effect_id = "workspace-effect:" + hashlib.sha256(approval_request_id.encode()).hexdigest()[:32]
        return EffectIntentSpec(
            effect_id=effect_id,
            target_hash=target_hash,
            parameters_hash=parameters_hash,
            provider_idempotency_key=None,
            maximum_cost_minor=0,
            recovery_class="manual_only",
        )

    def _consume_approval_and_start(self, request, request_hash, decision, state, grant_id):
        assert self._approval_service is not None
        approved = self._approval_service.request_for_grant(grant_id)
        intent = self._effect_intent(request, approved.approval_request_id)
        if (
            decision.outcome != "needs_approval"
            or approved.run_id != request.run_id
            or approved.node_id != request.node_id
            or approved.tool_id != WORKSPACE_WRITE_TOOL_ID
            or approved.action_category != "reversible_workspace_write"
            or approved.target_hash != intent.target_hash
            or approved.parameters_hash != intent.parameters_hash
            or approved.effect_intent_hash != intent.content_hash
            or approved.policy_manifest_hash != self._manifest.content_hash
            or approved.revocation_version != state.revocation_version
            or approved.emergency_deny_version != state.emergency_deny_version
        ):
            raise ApprovalInvalid("approval scope does not match this write request")
        attempt = ExecutionAttempt(
            run_id=request.run_id,
            node_id=request.node_id,
            attempt_id=request.attempt_id,
            fencing_generation=request.fencing_generation,
            isolation_profile_hash=self._profile_hash,
        )
        self._approval_service.bind_to_attempt(grant_id, attempt)
        consumed = self._approval_service.consume_and_intend(
            grant_id,
            attempt,
            intent,
            CostEstimate(
                amount_minor=0,
                currency=self._approval_service.budget_currency_for_run(request.run_id),
                token_limit=None,
                tool_fee_minor=0,
                snapshot_id="workspace-candidate-v1",
            ),
        )
        self._consume_approved_and_start(request, request_hash, decision, state, consumed)
        return attempt, consumed

    def _consume_approved_and_start(self, request, request_hash, decision, state, consumed) -> None:
        budget_events = self._events.read_stream("budget", request.run_id)
        intent_event = next((event for event in budget_events if event.event_id == consumed.intent_event_id), None)
        consumed_event = next((event for event in budget_events if event.event_id == consumed.consumed_event_id), None)
        reservation_event = next((
            event for event in budget_events
            if event.event_type == "BudgetReserved"
            and event.payload.get("reservation_id") == consumed.reservation.reservation_id
        ), None)
        if (
            intent_event is None
            or consumed_event is None
            or reservation_event is None
            or intent_event.event_type != "EffectIntentRecorded"
            or consumed_event.event_type != "ApprovalGrantConsumed"
            or intent_event.payload.get("approval_grant_id") != consumed.approval_grant_id
            or consumed_event.payload.get("approval_grant_id") != consumed.approval_grant_id
            or intent_event.payload.get("effect_id") != consumed.effect_id
            or consumed_event.payload.get("effect_id") != consumed.effect_id
            or any(
                event.run_id != request.run_id
                or event.node_id != request.node_id
                or event.attempt_id != request.attempt_id
                or event.fencing_generation != request.fencing_generation
                for event in (intent_event, consumed_event, reservation_event)
            )
        ):
            raise ToolAuditUnavailable("ApprovalService did not persist a matching workspace Attempt intent")

        seen = False

        def decide(events, _version):
            nonlocal seen
            if not self._authorized_now(request, state):
                raise _WriteAuthorityLost("authority_changed_in_approval_transaction")
            if any(
                event.payload.get("request_id") == request.request_id
                and event.event_type in _TERMINALS | {"ToolExecutionStarted", "ToolApprovalConsumed"}
                for event in events
            ):
                seen = True
                return None
            context = _event_context(request, consumed.consumed_event_id)
            return [
                EventDraft("ToolApprovalConsumed", {
                    "request_id": request.request_id,
                    "approval_request_id": consumed.approval_request_id,
                    "approval_grant_id": consumed.approval_grant_id,
                    "effect_id": consumed.effect_id,
                    "effect_intent_event_id": consumed.intent_event_id,
                    "approval_consumed_event_id": consumed.consumed_event_id,
                    "budget_reservation_id": consumed.reservation.reservation_id,
                    "request_hash": request_hash,
                }, **context),
                EventDraft("ToolExecutionStarted", {
                    "request_id": request.request_id,
                    "request_hash": request_hash,
                    "policy_decision_hash": decision.decision_hash,
                    "approval_grant_id": consumed.approval_grant_id,
                    "effect_intent_event_id": consumed.intent_event_id,
                }, **_event_context(request, consumed.intent_event_id)),
            ]

        try:
            self._events.append_checked(
                "security", request.run_id, f"tool-approval-start:{request.request_id}", decide
            )
        except _WriteAuthorityLost as exc:
            attempt = ExecutionAttempt(
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                isolation_profile_hash=self._profile_hash,
            )
            self._approval_receipt_if_consumed(
                consumed, attempt, "not_applied", _digest(str(exc).encode())
            )
            self._record_block(request, str(exc))
            raise ApprovalInvalid("Attempt authority changed before approved write start") from exc
        except Exception as exc:
            raise ToolAuditUnavailable("approved workspace-write start was not durably recorded") from exc
        if seen:
            raise ToolRequestAlreadyUsed("approved request has already started")

    def _approval_receipt_if_consumed(self, consumed, attempt, outcome, receipt_hash) -> None:
        if consumed is not None and attempt is not None:
            assert self._approval_service is not None
            self._approval_service.record_effect_receipt(
                consumed, attempt, outcome=outcome, receipt_hash=receipt_hash
            )

    def _wait_with_authority(self, session, request, state):
        stop = threading.Event()
        lost = threading.Event()

        def monitor():
            while not stop.is_set():
                if not self._authorized_now(request, state):
                    lost.set()
                    try:
                        session.cancel()
                    except Exception:
                        pass
                    return
                stop.wait(self._monitor_interval)

        watcher = threading.Thread(target=monitor, name="maestro-workspace-authority", daemon=True)
        watcher.start()
        try:
            result = session.wait()
        finally:
            stop.set()
            watcher.join(timeout=2)
        if not isinstance(result, OverlayCandidateResult):
            raise IsolationUnavailable("candidate launcher returned an invalid result")
        return result, lost.is_set()

    def _terminal(self, request, event_type, payload) -> None:
        def decide(events, _version):
            if any(event.event_type in _TERMINALS and event.payload.get("request_id") == request.request_id for event in events):
                return None
            start = next((event for event in reversed(events)
                          if event.event_type == "ToolExecutionStarted"
                          and event.payload.get("request_id") == request.request_id), None)
            if start is None:
                raise ToolAuditUnavailable("write terminal has no durable start")
            return [EventDraft(
                event_type,
                {"request_id": request.request_id, **payload},
                **_event_context(request, start.event_id),
            )]

        self._append_required(
            "security", request.run_id, f"tool-terminal:{request.request_id}", decide,
            "write outcome could not be durably audited",
        )

    def _terminal_best_effort(self, request, event_type, payload) -> None:
        try:
            self._terminal(request, event_type, payload)
        except Exception:
            pass

    def _new_event_once(self, events, request, event_type, payload, causation_id, *, require_start=True):
        if any(
            event.event_type == event_type
            and event.run_id == request.run_id
            and event.payload.get("request_id") == request.request_id
            for event in events
        ):
            return None
        has_start = any(
            event.event_type == "ToolExecutionStarted"
            and event.run_id == request.run_id
            and event.payload.get("request_id") == request.request_id
            for event in events
        )
        has_intent = any(
            event.event_type == "WorkspacePublicationIntent"
            and event.run_id == request.run_id
            and event.payload.get("request_id") == request.request_id
            for event in events
        )
        if require_start and not has_start:
            raise ToolAuditUnavailable("workspace publication event has no durable execution start")
        if event_type != "WorkspacePublicationIntent" and not has_intent:
            raise ToolAuditUnavailable("workspace publication receipt has no durable intent")
        return [EventDraft(event_type, payload, **_event_context(request, causation_id))]

    def _record_publication_aborted(self, request, intent, reason) -> None:
        intent_event = next((
            event for event in self._events.read_stream("security", request.run_id)
            if event.event_type == "WorkspacePublicationIntent"
            and event.payload.get("request_id") == request.request_id
        ), None)
        if intent_event is None:
            raise ToolAuditUnavailable("cannot record abort without durable publication intent")
        payload = {
            "request_id": request.request_id,
            "transaction_id": intent["transaction_id"],
            "manifest_hash": intent["manifest_hash"],
            "reason": _reason_code(reason),
        }
        global_intent = next((
            event for event in self._events.read_stream("workspace_publications", self._publication_stream_id)
            if event.event_type == "WorkspacePublicationIntent"
            and event.run_id == request.run_id
            and event.payload.get("request_id") == request.request_id
        ), None)
        if global_intent is None:
            raise ToolAuditUnavailable("workspace-scoped intent is unavailable during rollback receipt")
        global_decider = lambda events, _version: self._new_event_once(
            events, request, "WorkspacePublicationAborted", payload, global_intent.event_id,
            require_start=False,
        )
        self._append_required(
            "security", request.run_id, f"workspace-publication-aborted:{request.request_id}",
            lambda events, _version: self._new_event_once(
                events, request, "WorkspacePublicationAborted", payload, intent_event.event_id,
            ),
            "workspace publication rollback receipt was not durably audited",
        )
        self._append_required(
            "workspace_publications", self._publication_stream_id,
            f"workspace-publication-aborted:{request.run_id}:{request.request_id}",
            global_decider,
            "workspace-scoped rollback receipt was not durably audited",
        )

    def _has_unresolved_publication(self, run_id: str) -> bool:
        try:
            workspace_events = self._events.read_stream("workspace_publications", self._publication_stream_id)
            run_events = self._events.read_stream("security", run_id)
        except Exception as exc:
            raise ToolAuditUnavailable("publication history is unavailable") from exc
        return bool(
            _unresolved_publications(workspace_events)
            or _unresolved_publications(run_events, run_id=run_id)
        )

    def _record_block(self, request, reason):
        request_hash = self._request_hash(request)
        self._append_required(
            "security", request.run_id, f"tool-block:{request.request_id}:{reason}",
            lambda events, _version: None if any(
                event.event_type == "ToolExecutionBlocked" and event.payload.get("request_id") == request.request_id
                for event in events
            ) else [EventDraft(
                "ToolExecutionBlocked",
                {"request_id": request.request_id, "request_hash": request_hash, "reason": reason},
                **_event_context(request, request.causation_id),
            )],
            "blocked write request was not durably audited",
        )

    def _read_policy_state_or_block(self, request):
        try:
            state = self._policy_state(request)
            if not isinstance(state, PolicyState):
                raise TypeError("policy state is invalid")
            return state
        except Exception as exc:
            self._record_block(request, "policy_state_unavailable")
            raise PermissionError("current policy state is unavailable") from exc

    def _attempt_is_current(self, request) -> bool:
        try:
            return self._attempt_authority.is_current(request) is True
        except Exception:
            return False

    def _authorized_now(self, request, state) -> bool:
        return self._attempt_is_current(request) and self._read_policy_state_safe(request) == state

    def _read_policy_state_safe(self, request):
        try:
            value = self._policy_state(request)
            return value if isinstance(value, PolicyState) else None
        except Exception:
            return None

    def _append_required(self, stream_type, stream_id, key, decide, message):
        try:
            self._events.append_checked(stream_type, stream_id, key, decide)
        except ToolAuditUnavailable:
            raise
        except Exception as exc:
            raise ToolAuditUnavailable(message) from exc

    def _result(self, request, decision, outcome, candidate, *, receipt: WorkspacePublishReceipt | None = None):
        execution = candidate.execution
        output = outcome == "completed"
        return ToolExecutionResult(
            request_id=request.request_id,
            outcome=outcome,
            decision=decision,
            stdout=execution.stdout if output else b"",
            stderr=execution.stderr if output else b"",
            returncode=execution.returncode,
            stdout_sha256=_digest(execution.stdout),
            stderr_sha256=_digest(execution.stderr),
            termination_confirmed=execution.termination_confirmed,
            cancelled=execution.cancelled,
            timed_out=execution.timed_out,
            output_limited=execution.output_limited,
        )

    def _unknown(self, request, decision, reason, candidate):
        self._terminal_best_effort(request, "ToolExecutionOutcomeUnknown", {
            **_execution_payload(request, candidate, outcome="execution_unknown"),
            "reason": reason,
        })
        return ToolExecutionResult(request.request_id, "execution_unknown", decision,
                                   termination_confirmed=False)


class _WriteAuthorityLost(WorkspacePublishError):
    pass


def _candidate_succeeded(result: OverlayCandidateResult) -> bool:
    execution = result.execution
    return (
        execution.termination_confirmed
        and execution.returncode == 0
        and not execution.cancelled
        and not execution.timed_out
        and not execution.output_limited
        and execution.input_written
        and result.diff is not None
        and result.candidate_error is None
    )


def _execution_payload(request, candidate, *, outcome="completed", receipt=None):
    execution = candidate.execution
    payload: dict[str, Any] = {
        "request_id": request.request_id,
        "outcome": outcome,
        "returncode": execution.returncode,
        "stdout_sha256": _digest(execution.stdout),
        "stdout_bytes": len(execution.stdout),
        "stderr_sha256": _digest(execution.stderr),
        "stderr_bytes": len(execution.stderr),
        "termination_confirmed": execution.termination_confirmed,
        "cancelled": execution.cancelled,
        "timed_out": execution.timed_out,
        "output_limited": execution.output_limited,
        "elapsed_milliseconds": max(0, round(execution.elapsed_seconds * 1000)),
    }
    if receipt is not None:
        payload.update({
            "publication_transaction_id": receipt.transaction_id,
            "publication_manifest_hash": receipt.manifest_hash,
            "entries_published": receipt.entries_published,
            "lease_generation": receipt.lease_generation,
        })
    return payload


def _limits_payload(limits: SandboxLimits) -> dict[str, Any]:
    return {name: getattr(limits, name) for name in (
        "memory_bytes", "tasks", "cpu_percent", "timeout_seconds",
        "output_bytes", "nofile", "file_bytes",
    )}


def _private_external_directory(root: str | Path, workspace: Path, label: str) -> Path:
    path = Path(root)
    if not path.is_absolute():
        raise ValueError(f"{label} must be an absolute path")
    try:
        info = path.lstat()
        canonical = path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(f"{label} must already exist") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
        or _within(canonical, workspace)
        or _within(workspace, canonical)
    ):
        raise ValueError(f"{label} must be an external current-user-owned mode-0700 directory")
    return canonical


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _safe_error_code(exc: Exception) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", type(exc).__name__)[:64] or "error"


def _reason_code(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value)[:64] or "unknown"


def _unresolved_publications(events, *, run_id: str | None = None) -> set[tuple[str | None, str]]:
    intents: set[tuple[str | None, str]] = set()
    resolved: set[tuple[str | None, str]] = set()
    for event in events:
        event_run_id = event.run_id
        if run_id is not None and event_run_id != run_id:
            continue
        request_id = event.payload.get("request_id")
        if not isinstance(request_id, str):
            continue
        identity = (event_run_id, request_id)
        if event.event_type == "WorkspacePublicationIntent":
            intents.add(identity)
        elif event.event_type in {"WorkspacePublicationCompleted", "WorkspacePublicationAborted"}:
            resolved.add(identity)
    return intents - resolved


def _publication_intent_event_id(events, request) -> str:
    intent = next((
        event for event in reversed(events)
        if event.event_type == "WorkspacePublicationIntent"
        and event.run_id == request.run_id
        and event.payload.get("request_id") == request.request_id
    ), None)
    if intent is None:
        raise ToolAuditUnavailable("workspace-scoped receipt has no intent event")
    return intent.event_id


__all__ = ["WORKSPACE_WRITE_TOOL_ID", "WorkspaceWriteGateway", "WorkspaceWriteRequest"]
