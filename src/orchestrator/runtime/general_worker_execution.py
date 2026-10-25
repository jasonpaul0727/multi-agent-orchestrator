"""Explicit host mediation of bounded useful Worker turns.

Children propose data only. The owned journals, Gateway and Scheduler retain
dispatch, publication and terminal authority on the SQLite owner thread.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path

from orchestrator.artifacts import ArtifactStore, EphemeralArtifactGrantAuthority
from orchestrator.models import CostSnapshotRefs, ModelMessage, ModelRequest, ModelResponse, ProviderModelGateway, TokenUsage
from orchestrator.models.adapters import adapter_for
from orchestrator.models.gateway import validate_gateway_request
from orchestrator.persistence import SQLiteEventStore
from orchestrator.scheduler import Scheduler
from orchestrator.validation import revalidate_model
from .acceptance_coordinator import AttemptExecutionCoordinator
from .contracts import ArtifactRef, VerificationTask
from .general_worker_authority import (
    SchedulerProviderDispatchAuthority, _validate_general_eligibility, general_worker_host_scope,
)
from .general_worker_journal import (
    GeneralWorkerIdentity, GeneralWorkerJournal, GeneralWorkerResult, _identity_value,
    _publication_for_operation, metadata_hash,
)
from .general_worker_process import GeneralWorkerProcess
from .general_worker_protocol import GeneralWorkerRequest, GeneralWorkerTurn, _canonical, _input_commitment
from .verification_journal import VerifierProposalJournal
from .verifier_process import IsolatedVerifierProcess
from .worker_input_service import WorkerInputService
from .worker_output import DecodedWorkerOutput


class GeneralWorkerUnavailable(RuntimeError):
    def __init__(self):
        super().__init__("general_worker_unavailable")


class GeneralWorkerOwnership:
    """Private nonblocking advisory ownership; never evidence of process stop."""

    def __init__(self, root, operation_id):
        self._root, self._operation_id, self._descriptor = root, operation_id, None

    def acquire(self):
        import fcntl
        self._root.mkdir(mode=0o700, exist_ok=True)
        descriptor = os.open(self._root / (self._operation_id + ".lock"),
            os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return False
        self._descriptor = descriptor
        return True

    def close(self):
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None


class GeneralWorkerExecutionService:
    def __init__(self, scheduler, input_service, artifact_store, grant_authority,
        proposal_journal, acceptance_coordinator, provider_gateway, worker_process, workspace: Path):
        valid = False
        try:
            store = scheduler.event_store
            authority = provider_gateway.dispatch_authority
            if (type(scheduler) is not Scheduler or type(store) is not SQLiteEventStore
                or type(input_service) is not WorkerInputService or input_service._scheduler is not scheduler
                or type(artifact_store) is not ArtifactStore or artifact_store._event_store is not store
                or scheduler.recovery.artifact_store is not artifact_store
                or type(grant_authority) is not EphemeralArtifactGrantAuthority
                or scheduler._general_artifact_grants is not grant_authority
                or artifact_store._grant_verifier != grant_authority.verify
                or type(proposal_journal) is not VerifierProposalJournal or proposal_journal._event_store is not store
                or type(acceptance_coordinator) is not AttemptExecutionCoordinator
                or acceptance_coordinator._scheduler is not scheduler
                or acceptance_coordinator._artifact_store is not artifact_store
                or acceptance_coordinator._grants is not grant_authority
                or acceptance_coordinator._journal._event_store is not store
                or type(provider_gateway) is not ProviderModelGateway
                or type(authority) is not SchedulerProviderDispatchAuthority or authority.scheduler is not scheduler
                or authority.event_store is not store or provider_gateway.provider_call_journal is not authority.provider_journal
                or type(worker_process) is not GeneralWorkerProcess or store._connection.in_transaction):
                raise ValueError()
            root = Path(workspace).resolve(strict=True)
            database = Path(next(row[2] for row in store._connection.execute("PRAGMA database_list")
                if row[1] == "main")).resolve(strict=True)
            owner_root = database.parent / ".general-worker-owners"
            runtime = Path(__file__).resolve().parents[2]
            # /tmp is the launcher's private namespace; ambient executable and
            # config roots remain readable and cannot contain private authority.
            readable = (root, runtime, *(Path(path).resolve() for path in
                ("/usr", "/bin", "/lib", "/lib64", "/etc", "/dev")))
            for private in (database, artifact_store._root.resolve(), owner_root.resolve()):
                if any(private == public or private.is_relative_to(public) or public.is_relative_to(private)
                    for public in readable):
                    raise ValueError()
            general_worker_host_scope(store)
            valid = True
        except Exception:
            pass
        if not valid:
            raise GeneralWorkerUnavailable() from None
        self._scheduler, self._inputs, self._artifacts = scheduler, input_service, artifact_store
        self._grants, self._proposals, self._coordinator = grant_authority, proposal_journal, acceptance_coordinator
        self._gateway, self._worker, self._workspace = provider_gateway, worker_process, root
        self._authority, self._journal, self._owner_root = authority, authority.journal, owner_root

    async def execute(self, *, identity: GeneralWorkerIdentity, task_text: str) -> GeneralWorkerResult:
        operation = None
        ownership = None
        code = "general_worker_preparation_blocked"
        try:
            values = _identity_value(identity)
            task = self._inputs._prepare_general_worker_task(**values, task_text=task_text)
            operation = self._journal.read(identity)
            if operation is not None:
                # Explicit restart handling is added by the separately gated
                # recovery slice. No replay ever remints a dispatch permit.
                return GeneralWorkerResult(identity, operation.operation_id, "blocked", "general_worker_recovery_required")
            scope = self._authority._scope()
            result = None
            def request(events, _version):
                nonlocal result
                binding = self._scheduler._resolve_general_verification_binding(**values,
                    as_of=datetime.now(timezone.utc))
                state = self._scheduler.lifecycle.replay(identity.run_id)
                result = self._journal.request(context=task.context,
                    input_commitment=_input_commitment(task.model_dump(mode="json")),
                    accepted_route_sha256=metadata_hash(binding.active_attempt.accepted_route.model_dump(mode="json")),
                    workspace_identity_hash=state.workspace_identity_hash, host_scope_hash=scope,
                    output_byte_limit=min(task.output_byte_limit, 16_384))
                _validate_general_eligibility(self._scheduler, self._journal, result,
                    purpose="request", host_scope_hash=scope, as_of=datetime.now(timezone.utc), events=events)
            self._scheduler.event_store.append_checked("scheduler", "global", "general-request", request)
            operation = result
            ownership = GeneralWorkerOwnership(self._owner_root, operation.operation_id)
            if not await asyncio.to_thread(ownership.acquire):
                return GeneralWorkerResult(identity, operation.operation_id, "in_progress", "general_worker_in_progress")
            code = "general_worker_request_blocked"
            operation, proposal = await self._turn(operation, task, phase="request")
            if type(proposal) is not GeneralWorkerRequest:
                raise ValueError()
            binding = self._authority.resolve_eligibility(operation, purpose="dispatch")
            request = self._model_request(operation, task, binding)
            provider, model = validate_gateway_request(request, self._gateway.registry)
            body = adapter_for(provider.adapter).encode_request(request, model).body_json.encode("utf-8")
            code = "general_worker_provider_blocked"
            operation = self._authority.reserve_call(operation, request=request, request_body=body)
            response = await self._gateway.invoke(request)
            response = revalidate_model(ModelResponse, response)
            call = self._journal._provider_success(operation)
            content = response.output_text.encode("utf-8", errors="strict")
            raw = _canonical(response.model_dump(mode="json"))
            if (response.request_id != operation.request_id or response.model_id != call.model_id
                or response.provider_request_id != call.provider_request_id
                or response.usage != TokenUsage.model_validate(dict(call.usage))
                or response.finish_reason != "stop" or response.tool_calls
                or not 0 < len(content) <= operation.output_byte_limit or not 0 < len(raw) <= 1_048_576):
                raise ValueError()
            ref = self._publish(operation, raw, kind="response")
            operation = self._journal.capture_response(operation, response_artifact=ref,
                output_digest="sha256:" + hashlib.sha256(content).hexdigest(), output_byte_count=len(content))
            code = "general_worker_completion_blocked"
            operation, proposal = await self._turn(operation, task, phase="complete", response_text=response.output_text)
            if type(proposal) is not DecodedWorkerOutput or len(proposal.blobs) != 1:
                raise ValueError()
            blob, = proposal.blobs
            if blob.content != content:
                raise ValueError()
            output = self._publish(operation, blob.content, kind="output")
            if output != blob.artifact:
                raise ValueError()
            binding = self._authority.resolve_eligibility(operation, purpose="completion")
            verification_task = VerificationTask(context=task.context, candidate_artifacts=(output,),
                acceptance_contract=binding.verification_contract, required_check_ids=binding.required_check_ids)
            evidence = await IsolatedVerifierProcess(max_artifact_bytes=16_384).verify_async(
                verification_task, self._artifacts, grant_for_digest=lambda digest: self._grants.issue(
                    digest=digest, run_id=identity.run_id, expires_at=binding.active_attempt.lease_expires_at))
            proposal = self._proposals.record(verification_task, evidence)
            operation = self._journal.record_proposal(operation, task_sha256=proposal.task_sha256,
                evidence_sha256=proposal.evidence_sha256)
            code = "general_worker_acceptance_blocked"
            return await self._coordinator.accept_general_proposal_async(identity=identity,
                task_sha256=proposal.task_sha256)
        except Exception:
            pass
        finally:
            if ownership is not None:
                ownership.close()
        return GeneralWorkerResult(identity, operation.operation_id if operation else "", "blocked", code)

    async def _turn(self, operation, task, *, phase, response_text=None):
        self._authority.resolve_eligibility(operation, purpose="request" if phase == "request" else "completion")
        prepared = await asyncio.to_thread(self._worker.prepare_launch)
        # Recheck after the await, before recording any new launch intent.
        self._authority.resolve_eligibility(operation, purpose="request" if phase == "request" else "completion")
        operation = self._journal.prepare_turn(operation, phase=phase, prepared=prepared)
        turn = GeneralWorkerTurn(phase, prepared.launch_id, task, response_text)
        session = await asyncio.to_thread(self._worker.start_turn, self._workspace, turn,
            prepared_launch=prepared, expected_workspace_identity_hash=operation.workspace_identity_hash)
        operation = self._journal.ack_turn(operation, receipt=session.launch_receipt)
        result = await asyncio.to_thread(session.wait)
        operation = self._journal.stop_turn(operation, receipt=result.termination_receipt,
            request_proposal=result.proposal if type(result.proposal) is GeneralWorkerRequest else None)
        return operation, result.proposal

    def _model_request(self, operation, task, binding):
        snapshot = self._scheduler.lifecycle.config_snapshot(operation.identity.run_id)
        if snapshot.registry_manifest.content_hash != self._gateway.registry.content_hash:
            raise ValueError()
        state = self._scheduler.lifecycle.replay(operation.identity.run_id)
        contract = state.node(operation.identity.node_id).spec.planning_contract
        active = binding.active_attempt
        reservation = active.reservation
        return ModelRequest(request_id=operation.request_id, idempotency_key=operation.idempotency_key,
            **_identity_value(operation.identity), model_id=active.accepted_route.model_id,
            accepted_route=active.accepted_route, budget_reservation_id=reservation.reservation_id,
            messages=(ModelMessage(role="user", content=task.task_text),), tools=(),
            max_output_tokens=contract.max_output_tokens, reasoning_effort=active.accepted_route.reasoning_effort,
            timeout_ms=30_000, cost_snapshots=CostSnapshotRefs(registry_manifest_hash=operation.context.registry_hash,
                **{name: getattr(reservation, name) for name in ("tokenizer_snapshot_id", "fx_snapshot_id",
                    "price_snapshot_id", "estimator_snapshot_id")}))

    def _publish(self, operation, content, *, kind):
        source = {**{key: str(value) for key, value in _identity_value(operation.identity).items()},
            "agent_instance_id": operation.context.agent_instance_id,
            "general_worker_operation_id": operation.operation_id, "general_worker_kind": kind}
        response = kind == "response"
        record = self._artifacts.publish_bytes(content, source=source,
            artifact_type="general-model-response" if response else "text",
            media_type="application/json" if response else "text/plain; charset=utf-8",
            readable_scope=(operation.operation_id if response else operation.identity.run_id,))
        ref = ArtifactRef(digest=record.digest, size_bytes=record.size,
            artifact_type=record.artifact_type, media_type=record.media_type)
        _publication_for_operation(self._scheduler.event_store, operation, ref, kind, record.publication_id)
        return ref


__all__ = ["GeneralWorkerExecutionService", "GeneralWorkerUnavailable", "GeneralWorkerOwnership"]
