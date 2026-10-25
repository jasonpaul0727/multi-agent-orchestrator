"""Strict, bounded general Worker metadata; never task or response text.

These checkpoints describe host observations, not permission to execute. The
owned Scheduler authorities supply live eligibility at each execution boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta
from functools import wraps
import hashlib
import json
from typing import Literal

from orchestrator.artifacts import ArtifactStore
from orchestrator.budget import BudgetLedger
from orchestrator.isolation import PreparedSandboxLaunch, SandboxLaunchReceipt, SandboxTerminationReceipt
from orchestrator.isolation.launcher import _validate_prepared, _validate_launch_receipt
from orchestrator.models.gateway import AcceptedModelRoute, CostSnapshotRefs, ModelMessage, ModelRequest
from orchestrator.models.provider_calls import SQLiteProviderCallJournal, _stream_id
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.runtime.contracts import ArtifactRef, AttemptContext
from orchestrator.runtime.general_worker_protocol import (
    GeneralWorkerRequest, _canonical, _context, _DIGEST, _fields, _IDENTIFIER, _integer, _model_fields, _plain, _text,
)
from orchestrator.runtime.verification_journal import VerifierProposalJournal


STREAM_TYPE = "general_worker"
SCHEMA_VERSION = 1
MAX_EVENTS = 128
MAX_RECORD_BYTES = 16_384
MAX_HISTORY_BYTES = 262_144
MAX_TURNS = 32


class GeneralWorkerJournalError(RuntimeError):
    """Detached fixed-code failure: no original exception or payload retained."""

    def __init__(self):
        super().__init__("general_worker_journal_invalid")


def _guard(function):
    @wraps(function)
    def guarded(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception:
            pass
        raise GeneralWorkerJournalError()
    return guarded


def metadata_hash(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class GeneralWorkerIdentity:
    run_id: str
    node_id: str
    attempt_id: str
    fencing_generation: int

    @property
    def stream_id(self) -> str:
        return "general-identity-" + metadata_hash(_identity_value(self))[7:]


@dataclass(frozen=True, slots=True)
class GeneralWorkerResult:
    identity: GeneralWorkerIdentity
    operation_id: str
    status: Literal["accepted", "blocked", "in_progress"]
    code: str
    artifacts: tuple[ArtifactRef, ...] = ()
    task_sha256: str | None = None
    evidence_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class GeneralWorkerTurnCheckpoint:
    phase: Literal["request", "complete"]
    prepared: PreparedSandboxLaunch
    launch_receipt: SandboxLaunchReceipt | None = None
    termination_receipt: SandboxTerminationReceipt | None = None
    request_observed: bool = False


@dataclass(frozen=True, slots=True)
class GeneralWorkerCall:
    provider_call_stream_id: str
    model_request_sha256: str
    request_body_sha256: str
    accepted_route_id: str
    budget_reservation_id: str
    provider_id: str
    model_id: str
    idempotency_key_hash: str


@dataclass(frozen=True, slots=True)
class GeneralWorkerCapture:
    response_artifact: ArtifactRef
    output_digest: str
    output_byte_count: int
    request_body_sha256: str
    usage_sha256: str
    sender_receipt_sha256: str


@dataclass(frozen=True, slots=True)
class GeneralWorkerAcceptance:
    task_sha256: str
    evidence_sha256: str
    artifacts: tuple[ArtifactRef, ...]
    usage_sha256: str
    settlement_key: str
    completed_at: str
    capture_sha256: str
    turns_sha256: str
    response_publication_id: str
    output_publication_id: str


@dataclass(frozen=True, slots=True)
class GeneralWorkerOperation:
    identity: GeneralWorkerIdentity
    operation_id: str
    context: AttemptContext
    input_commitment: str
    accepted_route_sha256: str
    workspace_identity_hash: str
    host_scope_hash: str
    output_byte_limit: int
    request_id: str
    idempotency_key: str
    phase: str = "requested"
    turns: tuple[GeneralWorkerTurnCheckpoint, ...] = ()
    call: GeneralWorkerCall | None = None
    capture: GeneralWorkerCapture | None = None
    task_sha256: str | None = None
    evidence_sha256: str | None = None
    accepted_result: GeneralWorkerResult | None = None
    acceptance: GeneralWorkerAcceptance | None = None


_DATACLASSES = (GeneralWorkerIdentity, GeneralWorkerResult, GeneralWorkerTurnCheckpoint,
    GeneralWorkerCall, GeneralWorkerCapture, GeneralWorkerOperation, GeneralWorkerAcceptance,
    PreparedSandboxLaunch, SandboxLaunchReceipt, SandboxTerminationReceipt, GeneralWorkerRequest)
_MODELS = (AttemptContext, ArtifactRef, AcceptedModelRoute, ModelRequest, ModelMessage, CostSnapshotRefs)


def _safe(value):
    """Inspect exact contract types without invoking caller serialization hooks."""
    kind = type(value)
    if kind in _DATACLASSES:
        return {field.name: _safe(object.__getattribute__(value, field.name)) for field in fields(kind)}
    if kind in _MODELS:
        return {key: _safe(item) for key, item in _model_fields(value, kind).items()}
    if kind in (tuple, list):
        return [_safe(item) for item in value]
    if kind is dict:
        if any(type(key) is not str for key in value):
            raise ValueError()
        return {key: _safe(item) for key, item in value.items()}
    return _plain(value)


def _identity_value(identity):
    if type(identity) is not GeneralWorkerIdentity:
        raise ValueError()
    value = _safe(identity)
    for name in ("run_id", "node_id", "attempt_id"):
        _text(value[name], maximum=128, pattern=_IDENTIFIER)
    _integer(value["fencing_generation"], 1)
    return value


def _hashes(value, names):
    for name in names:
        _text(value[name], maximum=71, pattern=_DIGEST)


def _request_operation(data):
    _fields(data, {"context", "input_commitment", "accepted_route_sha256",
        "workspace_identity_hash", "host_scope_hash", "output_byte_limit"})
    _context(data["context"])
    _hashes(data, ("input_commitment", "accepted_route_sha256", "workspace_identity_hash", "host_scope_hash"))
    _integer(data["output_byte_limit"], 1, 16_384)
    context = AttemptContext.model_validate(data["context"])
    identity = GeneralWorkerIdentity(context.run_id, context.node_id, context.attempt_id, context.fencing_generation)
    digest = metadata_hash({"domain": "maestro.general-worker-operation.v1", **data})[7:]
    return GeneralWorkerOperation(identity=identity, operation_id="general-" + digest,
        context=context, input_commitment=data["input_commitment"],
        accepted_route_sha256=data["accepted_route_sha256"], workspace_identity_hash=data["workspace_identity_hash"],
        host_scope_hash=data["host_scope_hash"], output_byte_limit=data["output_byte_limit"],
        request_id="general-request-" + digest, idempotency_key="general-call-" + digest)


def _artifact(value):
    _fields(value, {"digest", "size_bytes", "artifact_type", "media_type"})
    _hashes(value, ("digest",))
    _integer(value["size_bytes"], 0, 1_048_576)
    _text(value["artifact_type"], maximum=64, pattern=_IDENTIFIER)
    _text(value["media_type"], maximum=128)
    return ArtifactRef.model_validate(value)


def _request_retry_allowed(operation):
    return (operation.phase == "worker_stopped" and operation.call is None and operation.capture is None
        and operation.turns and all(turn.phase == "request" and turn.launch_receipt is not None
            and turn.termination_receipt is not None and not turn.request_observed for turn in operation.turns))


def _require_worker_stopped(operation):
    if any(turn.launch_receipt is None or turn.termination_receipt is None for turn in operation.turns):
        raise ValueError()


def _publication_for_operation(store, operation, artifact, kind, publication_id=None):
    """Pure event metadata/provenance; never opens an artifact path."""
    source = {**{key: str(value) for key, value in _identity_value(operation.identity).items()},
        "agent_instance_id": operation.context.agent_instance_id,
        "general_worker_operation_id": operation.operation_id, "general_worker_kind": kind}
    scope = (operation.operation_id,) if kind == "response" else (operation.identity.run_id,)
    for event in store.read_stream("artifact", artifact.digest):
        if event.event_type != "ArtifactPublished":
            continue
        record = ArtifactStore._record_from_event(event, artifact.digest)
        if (record.source == source and record.readable_scope == scope and record.size == artifact.size_bytes
            and record.artifact_type == artifact.artifact_type and record.media_type == artifact.media_type
            and (publication_id is None or publication_id == record.publication_id)):
            return record
    raise ValueError()


def _apply(operation, action, data):
    """The sole state-transition algorithm, used for both writes and replay."""
    _plain(data)
    if action == "Requested":
        if operation is not None:
            raise ValueError()
        return _request_operation(data)
    if operation is None:
        raise ValueError()
    if action == "TurnPrepared":
        _fields(data, {"phase", "prepared"})
        _fields(data["prepared"], {"launch_id", "unit_name"})
        prepared = PreparedSandboxLaunch(**data["prepared"])
        _validate_prepared(prepared)
        phase = data["phase"]
        if (phase not in {"request", "complete"} or len(operation.turns) >= MAX_TURNS
            or any(turn.prepared.launch_id == prepared.launch_id for turn in operation.turns)
            or (phase == "request" and operation.phase != "requested" and not _request_retry_allowed(operation))
            or (phase == "complete" and operation.phase not in {"response_captured", "completion_stopped"})):
            raise ValueError()
        return replace(operation, phase="worker_requested" if phase == "request" else "completion_requested",
            turns=operation.turns + (GeneralWorkerTurnCheckpoint(phase, prepared),))
    if action in {"TurnAcknowledged", "TurnStopped"}:
        if operation.phase not in {"worker_requested", "completion_requested"} or not operation.turns:
            raise ValueError()
        turn = operation.turns[-1]
        if action == "TurnAcknowledged":
            _fields(data, {"receipt"})
            _fields(data["receipt"], {"launch_id", "unit_name", "control_group", "invocation_id", "profile_sha256"})
            receipt = SandboxLaunchReceipt(**data["receipt"])
            _validate_launch_receipt(turn.prepared, receipt)
            if turn.launch_receipt is not None or len(receipt.control_group) > 1024:
                raise ValueError()
            turn = replace(turn, launch_receipt=receipt)
        else:
            _fields(data, {"receipt", "request_observed", "launch_id"})
            _fields(data["receipt"], {"unit_name", "control_group", "active_state", "cgroup_empty"})
            receipt = SandboxTerminationReceipt(**data["receipt"])
            if (turn.launch_receipt is None or turn.termination_receipt is not None
                or type(data["request_observed"]) is not bool
                or data["launch_id"] != turn.prepared.launch_id
                or receipt.unit_name != turn.launch_receipt.unit_name
                or receipt.control_group != turn.launch_receipt.control_group
                or (turn.phase == "complete" and data["request_observed"])):
                raise ValueError()
            turn = replace(turn, termination_receipt=receipt, request_observed=data["request_observed"])
        return replace(operation, turns=operation.turns[:-1] + (turn,), phase=(
            "worker_stopped" if turn.phase == "request" else "completion_stopped"
        ) if action == "TurnStopped" else operation.phase)
    if action == "CallReserved":
        _fields(data, {field.name for field in fields(GeneralWorkerCall)})
        _hashes(data, ("model_request_sha256", "request_body_sha256", "idempotency_key_hash"))
        for name in {"provider_call_stream_id", "accepted_route_id", "budget_reservation_id", "provider_id", "model_id"}:
            _text(data[name], maximum=128, pattern=_IDENTIFIER)
        if (operation.phase != "worker_stopped" or not operation.turns[-1].request_observed
            or data["idempotency_key_hash"] != "sha256:" + hashlib.sha256(operation.idempotency_key.encode()).hexdigest()
            or data["accepted_route_id"] != "decision-" + operation.context.routing_decision_hash[7:]
            or data["provider_call_stream_id"] != "call-" + metadata_hash({
                **_identity_value(operation.identity), "request_id": operation.request_id})[7:]):
            raise ValueError()
        return replace(operation, phase="call_reserved", call=GeneralWorkerCall(**data))
    if action == "ResponseCaptured":
        _fields(data, {field.name for field in fields(GeneralWorkerCapture)})
        artifact = _artifact(data["response_artifact"])
        _hashes(data, ("output_digest", "request_body_sha256", "usage_sha256", "sender_receipt_sha256"))
        _integer(data["output_byte_count"], 0, operation.output_byte_limit)
        if operation.phase != "call_reserved" or operation.call is None:
            raise ValueError()
        return replace(operation, phase="response_captured", capture=GeneralWorkerCapture(
            **{**data, "response_artifact": artifact}))
    if action == "Proposed":
        _fields(data, {"task_sha256", "evidence_sha256"})
        _hashes(data, data)
        if operation.phase != "completion_stopped" or operation.capture is None:
            raise ValueError()
        return replace(operation, phase="proposed", **data)
    if action == "Accepted":
        _fields(data, {field.name for field in fields(GeneralWorkerAcceptance)})
        _hashes(data, ("task_sha256", "evidence_sha256", "usage_sha256", "capture_sha256", "turns_sha256"))
        for name in ("settlement_key", "response_publication_id", "output_publication_id"):
            _text(data[name], maximum=128, pattern=_IDENTIFIER)
        _text(data["completed_at"], maximum=64)
        completed = datetime.fromisoformat(data["completed_at"])
        if completed.utcoffset() != timedelta(0) or completed.isoformat() != data["completed_at"]:
            raise ValueError()
        if type(data["artifacts"]) is not list or len(data["artifacts"]) != 1:
            raise ValueError()
        artifacts = tuple(_artifact(value) for value in data["artifacts"])
        if (operation.phase != "proposed" or operation.capture is None
            or data["task_sha256"] != operation.task_sha256 or data["evidence_sha256"] != operation.evidence_sha256
            or data["capture_sha256"] != metadata_hash(_safe(operation.capture))
            or data["turns_sha256"] != metadata_hash(_safe(operation.turns))
            or artifacts != (ArtifactRef(digest=operation.capture.output_digest, size_bytes=operation.capture.output_byte_count,
                artifact_type="text", media_type="text/plain; charset=utf-8"),)):
            raise ValueError()
        _require_worker_stopped(operation)
        acceptance = GeneralWorkerAcceptance(**{**data, "artifacts": artifacts})
        return replace(operation, phase="accepted", acceptance=acceptance,
            accepted_result=GeneralWorkerResult(operation.identity, operation.operation_id, "accepted",
                "general_worker_accepted", artifacts, operation.task_sha256, operation.evidence_sha256))
    raise ValueError()


def _event_key(operation_id, action, data):
    return "general-event-" + metadata_hash({"operation_id": operation_id, "action": action, "data": data})[7:]


def _replay(identity, events, version):
    if version != len(events) or len(events) > MAX_EVENTS:
        raise ValueError()
    operation = None
    total = 0
    previous = None
    for index, event in enumerate(events, 1):
        # SQLite returns its own recursively frozen JSON containers. Decode the
        # already integrity-checked JSON into exact builtins for shape checks.
        payload = json.loads(_canonical(event.payload))
        _plain(payload)
        _fields(payload, {"schema_version", "operation_id", "action", "data"})
        size = len(_canonical(payload))
        total += size
        if (size > MAX_RECORD_BYTES or total > MAX_HISTORY_BYTES
            or type(payload["schema_version"]) is not int or payload["schema_version"] != SCHEMA_VERSION
            or event.schema_version != SCHEMA_VERSION or event.stream_type != STREAM_TYPE
            or event.stream_id != identity.stream_id or event.stream_version != index
            or event.event_type != "GeneralWorker" + payload["action"]
            or {key: getattr(event, key) for key in _identity_value(identity)} != _identity_value(identity)
            or event.correlation_id != identity.run_id):
            raise ValueError()
        operation = _apply(operation, payload["action"], payload["data"])
        if (operation.identity != identity or payload["operation_id"] != operation.operation_id
            or event.causation_id != (previous or operation.context.routing_decision_hash)
            or event.idempotency_key != _event_key(operation.operation_id, payload["action"], payload["data"])):
            raise ValueError()
        previous = metadata_hash(payload)
    return operation


class GeneralWorkerJournal:
    """Concrete, same-store checkpoints, with no generic append or success API."""

    def __init__(self, event_store: SQLiteEventStore):
        if type(event_store) is not SQLiteEventStore:
            raise GeneralWorkerJournalError()
        self.event_store = event_store

    @_guard
    def read(self, identity: GeneralWorkerIdentity) -> GeneralWorkerOperation | None:
        _identity_value(identity)
        events, version = self.event_store.read_stream_with_version(STREAM_TYPE, identity.stream_id)
        operation = _replay(identity, events, version)
        if operation is not None:
            self._validate_links(operation)
            self._validate_terminal_links(operation)
        return operation

    @_guard
    def validate_operations(self) -> None:
        for stream_id in self.event_store.stream_ids(STREAM_TYPE):
            events, version = self.event_store.read_stream_with_version(STREAM_TYPE, stream_id)
            if not events:
                raise ValueError()
            first = events[0]
            identity = GeneralWorkerIdentity(first.run_id, first.node_id, first.attempt_id, first.fencing_generation)
            if stream_id != identity.stream_id:
                raise ValueError()
            operation = _replay(identity, events, version)
            self._validate_links(operation)
            self._validate_terminal_links(operation)

    @_guard
    def request(self, *, context: AttemptContext, input_commitment: str,
        accepted_route_sha256: str, workspace_identity_hash: str, host_scope_hash: str,
        output_byte_limit: int) -> GeneralWorkerOperation:
        if type(context) is not AttemptContext:
            raise ValueError()
        data = _safe(dict(context=context, input_commitment=input_commitment,
            accepted_route_sha256=accepted_route_sha256, workspace_identity_hash=workspace_identity_hash,
            host_scope_hash=host_scope_hash, output_byte_limit=output_byte_limit))
        operation = _request_operation(data)
        return self._append(operation, "Requested", data)

    @_guard
    def prepare_turn(self, operation: GeneralWorkerOperation, *, phase: Literal["request", "complete"],
        prepared: PreparedSandboxLaunch) -> GeneralWorkerOperation:
        if type(prepared) is not PreparedSandboxLaunch:
            raise ValueError()
        return self._append(operation, "TurnPrepared", _safe(dict(phase=phase, prepared=prepared)))

    @_guard
    def ack_turn(self, operation: GeneralWorkerOperation, *, receipt: SandboxLaunchReceipt) -> GeneralWorkerOperation:
        if type(receipt) is not SandboxLaunchReceipt:
            raise ValueError()
        return self._append(operation, "TurnAcknowledged", _safe(dict(receipt=receipt)))

    @_guard
    def stop_turn(self, operation: GeneralWorkerOperation, *, receipt: SandboxTerminationReceipt,
        request_proposal: GeneralWorkerRequest | None = None) -> GeneralWorkerOperation:
        self._validate_operation(operation)
        if type(receipt) is not SandboxTerminationReceipt or not operation.turns:
            raise ValueError()
        if request_proposal is not None:
            if type(request_proposal) is not GeneralWorkerRequest:
                raise ValueError()
            value = _safe(request_proposal)
            _context(value["context"])
            if (value["context"] != _safe(operation.context)
                or value["input_commitment"] != operation.input_commitment
                or value["launch_id"] != operation.turns[-1].prepared.launch_id):
                raise ValueError()
        return self._append(operation, "TurnStopped", _safe(dict(receipt=receipt,
            request_observed=request_proposal is not None, launch_id=operation.turns[-1].prepared.launch_id)))

    @_guard
    def reserve_call(self, operation: GeneralWorkerOperation, *, request: ModelRequest,
        request_body: bytes) -> GeneralWorkerOperation:
        self._validate_operation(operation)
        if (type(request) is not ModelRequest or type(request_body) is not bytes
            or not 0 < len(request_body) <= 1_048_576):
            raise ValueError()
        value = _safe(request)
        if value["tools"]:
            raise ValueError()
        validated = ModelRequest.model_validate(value)
        if (any(value[name] != expected for name, expected in _identity_value(operation.identity).items())
            or validated.request_id != operation.request_id or validated.idempotency_key != operation.idempotency_key
            or metadata_hash(value["accepted_route"]) != operation.accepted_route_sha256
            or validated.accepted_route.registry_manifest_hash != operation.context.registry_hash):
            raise ValueError()
        return self._append(operation, "CallReserved", dict(
            provider_call_stream_id=_stream_id(validated), model_request_sha256=metadata_hash(value),
            request_body_sha256="sha256:" + hashlib.sha256(request_body).hexdigest(),
            accepted_route_id=validated.accepted_route.decision_id,
            budget_reservation_id=validated.budget_reservation_id, provider_id=validated.accepted_route.provider_id,
            model_id=validated.model_id,
            idempotency_key_hash="sha256:" + hashlib.sha256(validated.idempotency_key.encode()).hexdigest()))

    @_guard
    def capture_response(self, operation: GeneralWorkerOperation, *, response_artifact: ArtifactRef,
        output_digest: str, output_byte_count: int) -> GeneralWorkerOperation:
        self._validate_operation(operation)
        call = self._provider_success(operation)
        if type(response_artifact) is not ArtifactRef:
            raise ValueError()
        return self._append(operation, "ResponseCaptured", _safe(dict(response_artifact=response_artifact,
            output_digest=output_digest, output_byte_count=output_byte_count,
            request_body_sha256=call.request_hash, usage_sha256=metadata_hash(dict(call.usage)),
            sender_receipt_sha256=call.termination_receipt.receipt_hash)))

    @_guard
    def record_proposal(self, operation: GeneralWorkerOperation, *, task_sha256: str,
        evidence_sha256: str) -> GeneralWorkerOperation:
        return self._append(operation, "Proposed", _safe(dict(task_sha256=task_sha256, evidence_sha256=evidence_sha256)))

    @_guard
    def _record_accepted(self, operation: GeneralWorkerOperation, *, acceptance: GeneralWorkerAcceptance):
        """Scheduler-only nested checkpoint; public APIs cannot set success."""
        if not self.event_store._connection.in_transaction or type(acceptance) is not GeneralWorkerAcceptance:
            raise ValueError()
        return self._append(operation, "Accepted", _safe(acceptance))

    def _validate_operation(self, operation):
        if type(operation) is not GeneralWorkerOperation:
            raise ValueError()
        _safe(operation)
        _identity_value(operation.identity)
        _context(_safe(operation.context))
        _integer(operation.output_byte_limit, 1, 16_384)

    def _append(self, operation, action, data):
        self._validate_operation(operation)
        _plain(data)
        identity = operation.identity
        key = _event_key(operation.operation_id, action, data)
        payload = dict(schema_version=SCHEMA_VERSION, operation_id=operation.operation_id, action=action, data=data)
        result = None

        def decide(events, version):
            nonlocal result
            current = _replay(identity, events, version)
            existing = [event for event in events if event.idempotency_key == key]
            if existing:
                if len(existing) != 1 or existing[0].payload != payload:
                    raise ValueError()
                self._validate_links(current)
                result = current
                return [EventDraft("GeneralWorker" + action, payload, **_identity_value(identity),
                    correlation_id=identity.run_id, causation_id=existing[0].causation_id)]
            # Canonical comparison also rejects bool/int and int/float equality
            # tricks in constructed immutable projections.
            expected = None if action == "Requested" else operation
            if _canonical(_safe(current)) != _canonical(_safe(expected)):
                raise ValueError()
            result = _apply(current, action, data)
            self._validate_links(result)
            if (len(events) >= MAX_EVENTS or len(_canonical(payload)) > MAX_RECORD_BYTES
                or sum(len(_canonical(event.payload)) for event in events) + len(_canonical(payload)) > MAX_HISTORY_BYTES):
                raise ValueError()
            return [EventDraft("GeneralWorker" + action, payload, **_identity_value(identity),
                correlation_id=identity.run_id, causation_id=(metadata_hash(events[-1].payload)
                    if events else operation.context.routing_decision_hash))]

        self.event_store.append_checked(STREAM_TYPE, identity.stream_id, key, decide)
        return result

    def _provider_success(self, operation):
        call = self._provider_call(operation)
        if (call.status != "known_success" or call.usage is None
            or call.usage.get("status") not in {"reported", "estimated"} or call.termination_receipt is None):
            raise ValueError()
        return call

    def _provider_call(self, operation):
        if operation.call is None:
            raise ValueError()
        call = SQLiteProviderCallJournal(self.event_store).read_call(operation.call.provider_call_stream_id)
        if (call is None
            or any(getattr(call, name) != value for name, value in _identity_value(operation.identity).items())
            or call.request_id != operation.request_id or call.registry_manifest_hash != operation.context.registry_hash
            or call.request_hash != operation.call.request_body_sha256
            or any(getattr(call, name) != getattr(operation.call, name) for name in (
                "accepted_route_id", "budget_reservation_id", "provider_id", "model_id", "idempotency_key_hash"))):
            raise ValueError()
        return call

    def _validate_links(self, operation):
        if operation.capture is not None:
            call = self._provider_success(operation)
            capture = operation.capture
            if (capture.request_body_sha256 != call.request_hash
                or capture.usage_sha256 != metadata_hash(dict(call.usage))
                or capture.sender_receipt_sha256 != call.termination_receipt.receipt_hash):
                raise ValueError()
        if operation.task_sha256 is not None:
            proposal = VerifierProposalJournal(self.event_store).read_proposal(
                operation.identity.run_id, operation.task_sha256)
            expected = ArtifactRef(digest=operation.capture.output_digest,
                size_bytes=operation.capture.output_byte_count, artifact_type="text", media_type="text/plain; charset=utf-8")
            if (proposal is None or proposal.evidence_sha256 != operation.evidence_sha256
                or proposal.task.context != operation.context or proposal.task.candidate_artifacts != (expected,)):
                raise ValueError()

    def _validate_terminal_links(self, operation):
        """Reciprocal immutable success/usage links, without fresh time or files."""
        matches = [event for event in self.event_store.read_stream("scheduler", "global")
            if event.event_type == "AttemptSlotReleased" and
            all(getattr(event, key) == value for key, value in _identity_value(operation.identity).items())]
        successes = [event for event in matches if event.payload.get("outcome") == "succeeded"]
        if operation.phase != "accepted":
            if successes:
                raise ValueError()
            if matches:
                _require_worker_stopped(operation)
            return
        if len(matches) != 1 or len(successes) != 1 or operation.acceptance is None:
            raise ValueError()
        accepted, release = operation.acceptance, successes[0]
        if (release.payload.get("completed_at") != accepted.completed_at
            or release.payload.get("usage_hash") != accepted.usage_sha256
            or release.payload.get("task_sha256") != accepted.task_sha256
            or release.payload.get("evidence_sha256") != accepted.evidence_sha256):
            raise ValueError()
        states = BudgetLedger._replay_states(operation.identity.run_id,
            self.event_store.read_stream("budget", operation.identity.run_id))
        state = states[operation.call.budget_reservation_id]
        if (state.reservation.status != "committed" or state.usage is None
            or state.usage.settlement_key != accepted.settlement_key
            or metadata_hash(state.usage.model_dump(mode="json")) != accepted.usage_sha256):
            raise ValueError()
        _publication_for_operation(self.event_store, operation, operation.capture.response_artifact, "response",
            accepted.response_publication_id)
        _publication_for_operation(self.event_store, operation, accepted.artifacts[0], "output", accepted.output_publication_id)
