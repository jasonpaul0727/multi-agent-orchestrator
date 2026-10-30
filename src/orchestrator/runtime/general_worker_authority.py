"""Owned host scope and frozen control bindings for general Worker authority."""

from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import stat
from typing import Literal, TYPE_CHECKING
from uuid import UUID
from weakref import WeakKeyDictionary

from orchestrator.persistence import SQLiteEventStore
from orchestrator.artifacts import ArtifactStore
from orchestrator.models.gateway import ModelRequest, ModelResponse, TokenUsage, validate_gateway_request
from orchestrator.models.provider_calls import SQLiteProviderCallJournal
from orchestrator.runtime.general_worker_journal import (
    GeneralWorkerIdentity, GeneralWorkerJournal, GeneralWorkerOperation,
    GeneralWorkerJournalError, STREAM_TYPE, _guard, _identity_value, _safe, metadata_hash, _request_retry_allowed,
    _publication_for_operation,
)
from orchestrator.runtime.general_worker_protocol import _canonical, _model_fields, _text, _DIGEST, _pairs, _constant
from orchestrator.runtime.recovery_opportunities import _unsettled_calls
from orchestrator.tools.workspace_write import _unresolved_executions, _unresolved_publications

if TYPE_CHECKING:
    from orchestrator.config.models import ProviderAdapter
    from orchestrator.scheduler.core import Scheduler, VerificationBinding


@_guard
def general_worker_host_scope(event_store: SQLiteEventStore) -> str:
    """Read trusted Linux/file authority before locks; persist only its digest.

    This is deliberately re-sampled by the execution owner before external
    work, never during a checked callback. A copied DB is a different owner.
    """
    if type(event_store) is not SQLiteEventStore or event_store._connection.in_transaction:
        raise ValueError()
    databases = event_store._connection.execute("PRAGMA database_list").fetchall()
    main = [row for row in databases if row[1] == "main"]
    if len(main) != 1 or not main[0][2]:
        raise ValueError()
    path = Path(main[0][2]).resolve(strict=True)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError()
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    if str(UUID(boot_id)) != boot_id:
        raise ValueError()
    return metadata_hash({"domain": "maestro.general-worker-host-scope.v1", "boot_id": boot_id,
        "effective_uid": os.geteuid(), "database_path_sha256": metadata_hash(str(path)),
        "device": info.st_dev, "inode": info.st_ino})


@_guard
def validate_operation_binding(operation: GeneralWorkerOperation, binding, *,
    workspace_identity_hash: str, host_scope_hash: str) -> None:
    """Compare a persisted operation with the existing frozen VerificationBinding.

    No lease clock is sampled here: the owned Scheduler resolver must obtain
    this binding using trusted UTC after acquiring its authority transaction.
    """
    from orchestrator.scheduler.core import ActiveAttemptSnapshot, VerificationBinding

    if type(operation) is not GeneralWorkerOperation or type(binding) is not VerificationBinding:
        raise ValueError()
    _safe(operation)
    binding_fields = _model_fields(binding, VerificationBinding)
    active_fields = _model_fields(binding_fields["active_attempt"], ActiveAttemptSnapshot)
    context = _safe(binding_fields["context"])
    route = _safe(active_fields["accepted_route"])
    _text(workspace_identity_hash, maximum=71, pattern=_DIGEST)
    _text(host_scope_hash, maximum=71, pattern=_DIGEST)
    if (_safe(operation.context) != context
        or operation.accepted_route_sha256 != metadata_hash(route)
        or operation.workspace_identity_hash != workspace_identity_hash or operation.host_scope_hash != host_scope_hash):
        raise ValueError()


MAX_DISPATCH_PERMITS = 1024
GeneralWorkerPurpose = Literal["request", "dispatch", "completion", "acceptance"]


def _preflight_general_bytes(scheduler, journal, operation, proposal):
    """Read bounded private bytes once outside SQL; return publication IDs only."""
    artifacts = scheduler.recovery.artifact_store
    if (scheduler.event_store._connection.in_transaction or type(artifacts) is not ArtifactStore
        or artifacts._event_store is not scheduler.event_store or operation.capture is None):
        raise ValueError()
    response_ref = operation.capture.response_artifact
    output_ref, = proposal.task.candidate_artifacts
    if response_ref.artifact_type != "general-model-response" or response_ref.media_type != "application/json":
        raise ValueError()
    def read(ref, kind):
        record = _publication_for_operation(scheduler.event_store, operation, ref, kind)
        with artifacts._require_artifact_path(ref.digest).open("rb") as file:
            raw = file.read(ref.size_bytes + 1)
        if len(raw) != ref.size_bytes or "sha256:" + hashlib.sha256(raw).hexdigest() != ref.digest:
            raise ValueError()
        return raw, record.publication_id
    raw, response_id = read(response_ref, "response")
    value = json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_pairs, parse_constant=_constant)
    response = ModelResponse.model_validate(value)
    call = journal._provider_success(operation)
    if (raw != _canonical(response.model_dump(mode="json")) or response.request_id != operation.request_id
        or response.model_id != call.model_id or response.provider_request_id != call.provider_request_id
        or response.finish_reason != "stop" or response.tool_calls
        or response.usage != TokenUsage.model_validate(dict(call.usage))):
        raise ValueError()
    output, output_id = read(output_ref, "output")
    if not 0 < len(output) <= operation.output_byte_limit or response.output_text.encode("utf-8") != output:
        raise ValueError()
    return response_id, output_id


def _validate_general_usage(journal, operation, usage):
    call = journal._provider_success(operation)
    _validate_call_usage(call, usage)


def _validate_call_usage(call, usage):
    if usage.status != "committed":
        raise ValueError()
    for name in ("input_tokens", "output_tokens", "reasoning_tokens", "cached_input_tokens", "provider_fee_minor"):
        if call.usage.get(name) is not None and getattr(usage, name) != call.usage[name]:
            raise ValueError()


def _require_no_other_general_holds(scheduler, operation, recovered):
    if any(effect.node_id == operation.identity.node_id and effect.status == "outcome_unknown"
           for effect in recovered.effects):
        raise ValueError()
    workspace_stream = "workspace:" + operation.workspace_identity_hash[7:]
    if (_unresolved_publications(scheduler.event_store.read_stream("security", operation.identity.run_id),
            run_id=operation.identity.run_id)
        or _unresolved_publications(scheduler.event_store.read_stream("workspace_publications", workspace_stream))
        or _unresolved_executions(scheduler.event_store.read_stream("workspace_executions", workspace_stream),
            operation.workspace_identity_hash)):
        raise ValueError()


def _general_unsettled_calls(scheduler, *, run_id, sources):
    """Pure general policy: failure does not settle a Provider cost reservation.

    Reuse the legacy validator/hold definitions without changing legacy policy.
    Missing or unavailable failure usage is not evidence of zero cost.
    """
    result = list(_unsettled_calls(scheduler, run_id=run_id, sources=sources))
    journal = SQLiteProviderCallJournal(scheduler.event_store)
    for stream_id in scheduler.event_store.stream_ids("provider_call"):
        call = journal.read_call(stream_id)
        if call.run_id == run_id and call.status == "known_failure":
            reservation = scheduler._ledger_for_run(run_id).get_reservation(call.budget_reservation_id, run_id=run_id)
            if reservation.status not in {"committed", "released"}:
                result.append(call)
    return tuple(result)


@_guard
def _validate_general_release_authority(scheduler, journal, operation, *, events, entry, usage, known_no_effect, at):
    """Classify a known release from immutable Provider authority, never flags alone.

    Return True only when an external trusted usage sample is also required.
    Durable reconciliation is a separate metadata-only authority for startup.
    """
    identity = operation.identity
    recovered = scheduler.recovery.recover_control_state(identity.run_id)
    node = recovered.lifecycle.node(identity.node_id)
    sources = {event.payload["attempt_ref"]: event for event in events
        if event.event_type == "RoutingDecisionAccepted" and event.run_id == identity.run_id}
    source = next((event for event in sources.values() if event.node_id == identity.node_id
        and event.attempt_id == identity.attempt_id and event.fencing_generation == identity.fencing_generation), None)
    if (source is None or not node.attempts or node.attempts[-1].attempt_id != identity.attempt_id
        or node.attempts[-1].fencing_generation != identity.fencing_generation
        or _canonical(source.payload.get("verification_context")) != _canonical(_safe(operation.context))
        or recovered.lifecycle.workspace_identity_hash != operation.workspace_identity_hash):
        raise ValueError()
    own = None if operation.call is None else journal._provider_call(operation)
    for call in _general_unsettled_calls(scheduler, run_id=identity.run_id, sources=sources):
        if call.node_id == identity.node_id and (own is None or call.stream_id != own.stream_id):
            raise ValueError()
    provider = SQLiteProviderCallJournal(scheduler.event_store)
    for stream_id in scheduler.event_store.stream_ids("provider_call"):
        call = provider.read_call(stream_id)
        if call.run_id != identity.run_id or call.node_id != identity.node_id:
            continue
        if call.attempt_id == identity.attempt_id and (own is None or own.stream_id != stream_id):
            raise ValueError()
    _require_no_other_general_holds(scheduler, operation, recovered)
    if own is None:
        if operation.call is not None or not known_no_effect or usage is not None:
            raise ValueError()
        return False
    if entry == "reconcile":
        proof = own.reconciliation
        if (own.status not in {"settlement_pending", "reconciled"} or proof is None
            or own.reconciled_at != at or proof.usage != usage
            or known_no_effect != (proof.effect == "not_received")):
            raise ValueError()
        return False
    if (own.status not in {"known_success", "known_failure"} or own.termination_receipt is None
        or own.usage is None or own.usage.get("status") not in {"reported", "estimated"}
        or known_no_effect or usage is None):
        raise ValueError()
    _validate_call_usage(own, usage)
    return True


@dataclass(frozen=True, slots=True)
class _DispatchPermit:
    """Bounded process-local metadata; never request/response or credentials."""

    reservation_event_sha256: str
    model_request_sha256: str
    request_body_sha256: str
    host_scope_hash: str


def _validated_request(request):
    if type(request) is not ModelRequest:
        raise ValueError()
    value = _safe(request)
    if value["tools"]:
        raise ValueError()
    return ModelRequest.model_validate(value), metadata_hash(value)


def _request_identity(request):
    return GeneralWorkerIdentity(request.run_id, request.node_id, request.attempt_id, request.fencing_generation)


def _reservation_event_hash(journal, operation):
    events = journal.event_store.read_stream(STREAM_TYPE, operation.identity.stream_id)
    reserved = [event for event in events if event.event_type == "GeneralWorkerCallReserved"]
    if len(reserved) != 1:
        raise ValueError()
    event = reserved[0]
    return metadata_hash({"event_id": event.event_id, "payload_hash": event.payload_hash})


def _validate_general_eligibility(scheduler, journal, operation, *, purpose, host_scope_hash, as_of, events):
    """Owned metadata checks only; caller holds the same-store serialization lock."""
    if type(purpose) is not str or purpose not in {"request", "dispatch", "completion", "acceptance"}:
        raise ValueError()
    journal._validate_operation(operation)
    current = journal.read(operation.identity)
    if current is None or _canonical(_safe(current)) != _canonical(_safe(operation)):
        raise ValueError()
    identity = _identity_value(operation.identity)
    recovered = scheduler.recovery.recover_control_state(operation.identity.run_id)
    state = recovered.lifecycle
    node = state.node(operation.identity.node_id)
    if (state.status != "running" or not node.attempts
        or node.attempts[-1].attempt_id != operation.identity.attempt_id
        or node.attempts[-1].fencing_generation != operation.identity.fencing_generation
        or any(item.node_id == operation.identity.node_id and item.status == "outcome_unknown"
               for item in recovered.active_attempts)):
        raise ValueError()
    binding = scheduler._resolve_general_verification_binding(**identity, as_of=as_of)
    validate_operation_binding(operation, binding, workspace_identity_hash=state.workspace_identity_hash,
        host_scope_hash=host_scope_hash)
    phases = {"request": {"requested"}, "dispatch": {"worker_stopped", "call_reserved"},
        "completion": {"response_captured", "completion_stopped"}, "acceptance": {"proposed"}}
    if operation.phase not in phases[purpose] and not (purpose == "request" and _request_retry_allowed(operation)):
        raise ValueError()
    if purpose != "request" and (not operation.turns or any(
        turn.launch_receipt is None or turn.termination_receipt is None for turn in operation.turns)):
        raise ValueError()
    if purpose == "dispatch" and not operation.turns[-1].request_observed:
        raise ValueError()
    sources = {event.payload["attempt_ref"]: event for event in events
        if event.event_type == "RoutingDecisionAccepted" and event.run_id == operation.identity.run_id}
    for call in _general_unsettled_calls(scheduler, run_id=operation.identity.run_id, sources=sources):
        if call.node_id != operation.identity.node_id:
            continue
        # Only this captured success may be consumed; never an invoke exemption.
        if (purpose not in {"completion", "acceptance"} or operation.capture is None
            or operation.call is None or call.stream_id != operation.call.provider_call_stream_id
            or call.status != "known_success"):
            raise ValueError()
        journal._validate_links(operation)
    _require_no_other_general_holds(scheduler, operation, recovered)
    return binding


class SchedulerProviderDispatchAuthority:
    """Own fresh reservation and exactly one post-credential Provider intent.

    Restart/replay has no permit. Weak task ownership ties a claimed permit to
    one live Gateway invocation without retaining its request or coroutine.
    """

    def __init__(self, scheduler: Scheduler, journal: GeneralWorkerJournal):
        from orchestrator.scheduler.core import Scheduler

        if (type(scheduler) is not Scheduler or type(journal) is not GeneralWorkerJournal
            or type(scheduler.event_store) is not SQLiteEventStore
            or scheduler.event_store is not journal.event_store):
            raise GeneralWorkerJournalError()
        self.scheduler = scheduler
        self.journal = journal
        self.event_store = journal.event_store
        self.provider_journal = SQLiteProviderCallJournal(self.event_store)
        self.host_scope_hash = general_worker_host_scope(self.event_store)
        self._pending: dict[str, _DispatchPermit] = {}
        self._claims = WeakKeyDictionary()

    def _scope(self):
        from orchestrator.scheduler.core import Scheduler

        if (type(self.scheduler) is not Scheduler or type(self.journal) is not GeneralWorkerJournal
            or type(self.event_store) is not SQLiteEventStore
            or type(self.provider_journal) is not SQLiteProviderCallJournal
            or self.scheduler.event_store is not self.event_store
            or self.journal.event_store is not self.event_store
            or self.provider_journal.event_store is not self.event_store):
            raise ValueError()
        scope = general_worker_host_scope(self.event_store)
        if scope != self.host_scope_hash:
            raise ValueError()
        return scope

    @_guard
    def resolve_eligibility(self, operation: GeneralWorkerOperation, *, purpose: GeneralWorkerPurpose) -> VerificationBinding:
        """Check request/completion/acceptance metadata without dispatch permission."""
        scope = self._scope()
        self.journal._validate_operation(operation)
        result = None
        def check(events, _version):
            nonlocal result
            result = _validate_general_eligibility(self.scheduler, self.journal, operation,
                purpose=purpose, host_scope_hash=scope, as_of=datetime.now(timezone.utc), events=events)
        self.event_store.append_checked("scheduler", "global", "general-eligibility", check)
        return result

    @_guard
    def reserve_call(self, operation: GeneralWorkerOperation, *, request: ModelRequest,
        request_body: bytes) -> GeneralWorkerOperation:
        """Commit a NEW reservation, then mint the sole process-local permit."""
        scope = self._scope()  # also rejects ambient transaction before any file read
        self.journal._validate_operation(operation)
        request, request_hash = _validated_request(request)
        if type(request_body) is not bytes or not 0 < len(request_body) <= 1_048_576:
            raise ValueError()
        if len(self._pending) + sum(len(value) for value in self._claims.values()) >= MAX_DISPATCH_PERMITS:
            raise ValueError()
        result = permit = None
        def reserve(events, _version):
            nonlocal result, permit
            if operation.phase != "worker_stopped":
                raise ValueError()
            _validate_general_eligibility(self.scheduler, self.journal, operation,
                purpose="dispatch", host_scope_hash=scope, as_of=datetime.now(timezone.utc), events=events)
            snapshot = self.scheduler.lifecycle.config_snapshot(operation.identity.run_id)
            validate_gateway_request(request, snapshot.registry_manifest)
            result = self.journal.reserve_call(operation, request=request, request_body=request_body)
            permit = _DispatchPermit(_reservation_event_hash(self.journal, result),
                request_hash, result.call.request_body_sha256, scope)
        self.event_store.append_checked("scheduler", "global", "general-reserve-call", reserve)
        # This assignment occurs only after the outer commit returns successfully.
        self._pending[result.identity.stream_id] = permit
        return result

    def _match_permit(self, operation, request, request_hash, permit, scope):
        if (operation is None or operation.phase != "call_reserved" or operation.call is None
            or operation.request_id != request.request_id or operation.idempotency_key != request.idempotency_key
            or operation.call.model_request_sha256 != request_hash or permit.model_request_sha256 != request_hash
            or permit.request_body_sha256 != operation.call.request_body_sha256
            or permit.host_scope_hash != scope or operation.host_scope_hash != scope
            or permit.reservation_event_sha256 != _reservation_event_hash(self.journal, operation)):
            raise ValueError()

    async def is_accepted(self, request: ModelRequest) -> bool:
        """Claim one fresh permit before the broker; an idempotent read cannot mint it."""
        try:
            request, request_hash = _validated_request(request)
            identity = _request_identity(request)
            task = asyncio.current_task()
            if type(task) is not asyncio.Task:
                return False
            permit = self._pending.pop(identity.stream_id, None)
            if permit is None:
                return False
            scope = self._scope()
            def claim(events, _version):
                operation = self.journal.read(identity)
                self._match_permit(operation, request, request_hash, permit, scope)
                _validate_general_eligibility(self.scheduler, self.journal, operation,
                    purpose="dispatch", host_scope_hash=scope, as_of=datetime.now(timezone.utc), events=events)
            self.event_store.append_checked("scheduler", "global", "general-claim-dispatch", claim)
            self._claims.setdefault(task, {})[identity.stream_id] = permit
            return True
        except Exception:
            return False

    @_guard
    def record_intent(self, request: ModelRequest, *, provider_id: str, provider_adapter: ProviderAdapter,
        request_body: bytes, provider_correlation_id: str | None = None) -> str:
        """Consume this invocation's permit and atomically recheck before intent."""
        request, request_hash = _validated_request(request)
        identity = _request_identity(request)
        task = asyncio.current_task()
        if type(task) is not asyncio.Task:
            raise ValueError()
        claims = self._claims.get(task, {})
        permit = claims.pop(identity.stream_id, None)
        if not claims:
            self._claims.pop(task, None)
        if permit is None:
            raise ValueError()
        scope = self._scope()
        if (type(request_body) is not bytes or not 0 < len(request_body) <= 1_048_576
            or "sha256:" + hashlib.sha256(request_body).hexdigest() != permit.request_body_sha256
            or type(provider_id) is not str or type(provider_adapter) is not str
            or (provider_correlation_id is not None and type(provider_correlation_id) is not str)):
            raise ValueError()
        result = None
        def authorize(events, _version):
            nonlocal result
            operation = self.journal.read(identity)
            self._match_permit(operation, request, request_hash, permit, scope)
            _validate_general_eligibility(self.scheduler, self.journal, operation,
                purpose="dispatch", host_scope_hash=scope, as_of=datetime.now(timezone.utc), events=events)
            provider, _ = validate_gateway_request(request,
                self.scheduler.lifecycle.config_snapshot(identity.run_id).registry_manifest)
            if provider_id != provider.id or provider_adapter != provider.adapter:
                raise ValueError()
            result = self.provider_journal.record_intent(request, provider_id=provider_id,
                provider_adapter=provider_adapter, request_body=request_body,
                provider_correlation_id=provider_correlation_id)
        self.event_store.append_checked("scheduler", "global", "general-provider-intent", authorize)
        return result

    def _retire_invocation(self, request):
        """Gateway finally hook; never removes a different task's live claim."""
        try:
            request, _ = _validated_request(request)
            key = _request_identity(request).stream_id
            task = asyncio.current_task()
            claims = self._claims.get(task, {})
            claims.pop(key, None)
            if not claims:
                self._claims.pop(task, None)
        except Exception:
            pass
