"""Metadata-only general Worker persistence and hostile replay boundaries."""

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from uuid import UUID

import pytest

from orchestrator.isolation import PreparedSandboxLaunch, SandboxLaunchReceipt, SandboxTerminationReceipt
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.runtime.general_worker_journal import (
    GeneralWorkerIdentity, GeneralWorkerJournal, GeneralWorkerJournalError,
    GeneralWorkerOperation,
)
from tests.unit.runtime.test_general_worker_protocol import LAUNCH, task
from orchestrator.runtime.general_worker_protocol import GeneralWorkerRequest


H = "sha256:" + "a" * 64


def requested(store):
    journal = GeneralWorkerJournal(store)
    operation = journal.request(context=task().context, input_commitment=H,
        accepted_route_sha256=H, workspace_identity_hash=H, host_scope_hash=H,
        output_byte_limit=16_384)
    return journal, operation


def epoch(launch_id=LAUNCH):
    unit = "maestro-attempt-" + UUID(launch_id).hex + ".service"
    prepared = PreparedSandboxLaunch(launch_id, unit)
    ack = SandboxLaunchReceipt(launch_id, unit,
        "/user.slice/user-1000.slice/user@1000.service/app.slice/" + unit,
        "1" * 32, H)
    stop = SandboxTerminationReceipt(unit, ack.control_group, "inactive", True)
    return prepared, ack, stop


def request_proposal(operation):
    return GeneralWorkerRequest(operation.context, operation.turns[-1].prepared.launch_id,
        operation.input_commitment)


@pytest.fixture
def store(tmp_path):
    with SQLiteEventStore(tmp_path / "events.db") as value:
        yield value


@pytest.mark.parametrize("field", ["input_commitment", "accepted_route_sha256",
    "workspace_identity_hash", "host_scope_hash", "output_byte_limit",
    "input_manifest_hash", "effective_config_hash", "registry_hash", "policy_manifest_hash",
    "routing_decision_hash", "planning_contract_hash", "graph_version", "agent_instance_id"])
def test_operation_binds_all_frozen_commitments(store, field):
    journal, operation = requested(store)
    assert journal.read(operation.identity) == operation
    kwargs = dict(context=task().context, input_commitment=H, accepted_route_sha256=H,
        workspace_identity_hash=H, host_scope_hash=H, output_byte_limit=16_384)
    changed = 8 if field in {"output_byte_limit", "graph_version"} else (
        "other-agent" if field == "agent_instance_id" else "sha256:" + "b" * 64)
    if field in kwargs:
        kwargs[field] = changed
    else:
        kwargs["context"] = kwargs["context"].model_copy(update={field: changed})
    with pytest.raises(GeneralWorkerJournalError):
        journal.request(**kwargs)
    assert journal.read(operation.identity) == operation


def test_metadata_replay_rejects_missing_cross_epoch_transition(store):
    journal, operation = requested(store)
    prepared, ack, stop = epoch()
    operation = journal.prepare_turn(operation, phase="request", prepared=prepared)
    operation = journal.ack_turn(operation, receipt=ack)
    operation = journal.stop_turn(operation, receipt=stop, request_proposal=request_proposal(operation))
    events = store.read_stream("general_worker", operation.identity.stream_id)
    # A syntactically valid, hash-checked checkpoint cannot replace a missing ack.
    with SQLiteEventStore(str(store._connection.execute("PRAGMA database_list").fetchone()[2]) + ".copy") as other:
        for event in (events[0], events[1], events[3]):
            other.append("general_worker", operation.identity.stream_id,
                other.current_version("general_worker", operation.identity.stream_id),
                [EventDraft(event.event_type, event.payload, run_id=event.run_id,
                    node_id=event.node_id, attempt_id=event.attempt_id,
                    fencing_generation=event.fencing_generation, correlation_id=event.correlation_id,
                    causation_id=event.causation_id)], event.idempotency_key)
        with pytest.raises(GeneralWorkerJournalError):
            GeneralWorkerJournal(other).validate_operations()


def test_db_wal_repr_and_errors_exclude_payload(store):
    journal, operation = requested(store)
    marker = "private-request-response-credential-sentinel"
    caught = None
    try:
        journal.request(context=task().context, input_commitment=marker,
            accepted_route_sha256=H, workspace_identity_hash=H, host_scope_hash=H,
            output_byte_limit=16_384)
    except GeneralWorkerJournalError as error:
        caught = error
    assert caught is not None and caught.__cause__ is None and caught.__context__ is None
    assert marker not in repr(caught) + repr(operation)
    from pathlib import Path
    path = Path(store._connection.execute("PRAGMA database_list").fetchone()[2])
    for candidate in (path, Path(str(path) + "-wal")):
        if candidate.exists():
            assert marker.encode() not in candidate.read_bytes()


def test_constructed_operation_rejected_before_callback(store):
    journal, operation = requested(store)
    called = []
    class Hostile(str):
        def __str__(self):
            called.append("str")
            return "private-payload"
        def encode(self, *args, **kwargs):
            called.append("encode")
            return b"private-payload"
    forged = replace(operation, operation_id=Hostile(operation.operation_id))
    with pytest.raises(GeneralWorkerJournalError):
        journal.prepare_turn(forged, phase="request", prepared=epoch()[0])
    assert called == []
    assert journal.read(operation.identity) == operation


def test_request_prepare_ack_stop_are_exactly_idempotent(store):
    journal, operation = requested(store)
    assert requested(store)[1] == operation
    prepared, ack, stop = epoch()
    ready = journal.prepare_turn(operation, phase="request", prepared=prepared)
    assert journal.prepare_turn(operation, phase="request", prepared=prepared) == ready
    observed = journal.ack_turn(ready, receipt=ack)
    assert journal.ack_turn(ready, receipt=ack) == observed
    stopped = journal.stop_turn(observed, receipt=stop, request_proposal=request_proposal(observed))
    assert journal.stop_turn(observed, receipt=stop, request_proposal=request_proposal(observed)) == stopped
    assert stopped.phase == "worker_stopped"
    assert stopped.turns[0].launch_receipt == ack
    assert stopped.turns[0].termination_receipt == stop
    journal.validate_operations()


def reserved(store):
    from orchestrator.runtime.general_worker_journal import metadata_hash
    from tests.unit.models.test_provider_call_journal import model_registry, model_request

    request = model_request(model_registry())
    context = task().context.model_copy(update={"registry_hash": request.accepted_route.registry_manifest_hash})
    route = request.accepted_route.model_copy(update={"fencing_generation": context.fencing_generation,
        "decision_id": "decision-" + context.routing_decision_hash[7:]})
    journal = GeneralWorkerJournal(store)
    operation = journal.request(context=context, input_commitment=H,
        accepted_route_sha256=metadata_hash(route.model_dump(mode="json")),
        workspace_identity_hash=H, host_scope_hash=H, output_byte_limit=16_384)
    request = request.model_copy(update={"request_id": operation.request_id,
        "idempotency_key": operation.idempotency_key, "fencing_generation": context.fencing_generation,
        "accepted_route": route, "tools": ()})
    prepared, ack, stop = epoch()
    operation = journal.prepare_turn(operation, phase="request", prepared=prepared)
    operation = journal.ack_turn(operation, receipt=ack)
    operation = journal.stop_turn(operation, receipt=stop, request_proposal=request_proposal(operation))
    stopped = operation
    operation = journal.reserve_call(operation, request=request, request_body=b"private-request-body-sentinel")
    assert journal.reserve_call(stopped, request=request, request_body=b"private-request-body-sentinel") == operation
    return journal, operation, request


def provider_success(store, request, *, usage=True, receipt=True, body=b"private-request-body-sentinel"):
    from orchestrator.models.provider_calls import SQLiteProviderCallJournal
    from orchestrator.models.provider_sender import ProviderSenderTerminationReceipt
    journal = SQLiteProviderCallJournal(store)
    journal.record_intent(request, provider_id=request.accepted_route.provider_id,
        provider_adapter="openai_responses", request_body=body,
        provider_correlation_id="offline-correlation")
    call = journal.read(request)
    stop = ProviderSenderTerminationReceipt(provider_call_stream_id=call.stream_id,
        **{name: getattr(call, name) for name in ("run_id", "node_id", "attempt_id", "fencing_generation",
            "accepted_route_id", "budget_reservation_id", "provider_id", "model_id", "registry_manifest_hash")},
        request_hash=call.request_hash, unit_name="maestro-provider-" + "b" * 32 + ".service",
        cgroup_path_hash=H, active_state="inactive", cgroup_empty=True, observed_at=datetime.now(timezone.utc))
    journal.record_outcome(request, outcome="known_success", http_status=200,
        usage={"status": "reported", "input_tokens": 1, "output_tokens": 2} if usage else None,
        termination_receipt=stop if receipt else None)
    return journal


def capture(journal, operation):
    from orchestrator.runtime.contracts import ArtifactRef
    return journal.capture_response(operation, response_artifact=ArtifactRef(digest=H,
        size_bytes=21, artifact_type="model-response", media_type="application/json"),
        output_digest="sha256:" + hashlib.sha256(b"private-response-sentinel").hexdigest(), output_byte_count=25)


def test_call_capture_and_proposal_replay_bind_both_digests(store):
    from orchestrator.runtime.contracts import ArtifactRef, VerificationTask
    from orchestrator.runtime.verification_journal import VerifierProposalJournal
    from tests.unit.runtime.test_verification_journal import _evidence
    journal, operation, request = reserved(store)
    provider_success(store, request)
    operation = capture(journal, operation)
    assert operation.capture.response_artifact.digest != operation.capture.output_digest
    prepared, ack, stop = epoch("9c483536-3c61-41e7-977b-6559b2732937")
    operation = journal.prepare_turn(operation, phase="complete", prepared=prepared)
    operation = journal.ack_turn(operation, receipt=ack)
    operation = journal.stop_turn(operation, receipt=stop)
    verification = VerificationTask(context=operation.context,
        candidate_artifacts=(ArtifactRef(digest=operation.capture.output_digest,
            size_bytes=operation.capture.output_byte_count, artifact_type="text", media_type="text/plain; charset=utf-8"),),
        required_check_ids=("artifact-integrity",), acceptance_contract="maestro.artifact-verification/v1")
    proposal = VerifierProposalJournal(store).record(verification, _evidence(verification))
    operation = journal.record_proposal(operation, task_sha256=proposal.task_sha256,
        evidence_sha256=proposal.evidence_sha256)
    assert operation.phase == "proposed" and operation.accepted_result is None
    assert GeneralWorkerJournal(store).read(operation.identity) == operation
    journal.validate_operations()
    assert journal.prepare_turn(operation, phase="complete", prepared=prepared) == operation
    assert len(operation.turns) == 2
    from pathlib import Path
    path = Path(store._connection.execute("PRAGMA database_list").fetchone()[2])
    for candidate in (path, Path(str(path) + "-wal")):
        if candidate.exists():
            content = candidate.read_bytes()
            assert b"private-request-body-sentinel" not in content
            assert b"private-response-sentinel" not in content
            assert b"private prompt" not in content


@pytest.mark.parametrize("missing", ["intent", "usage", "receipt"])
def test_capture_requires_exact_durable_provider_success(store, missing):
    journal, operation, request = reserved(store)
    if missing != "intent":
        provider_success(store, request, usage=missing != "usage", receipt=missing != "receipt")
    with pytest.raises(GeneralWorkerJournalError):
        capture(journal, operation)
    assert journal.read(operation.identity) == operation


@pytest.mark.parametrize("field", ["request_id", "idempotency_key", "messages", "timeout_ms", "tools"])
def test_reserved_call_rejects_rotated_or_changed_request(store, field):
    from orchestrator.models import ModelMessage
    journal, operation, request = reserved(store)
    from tests.unit.models.test_provider_call_journal import model_registry, model_request
    value = {"request_id": "rotated", "idempotency_key": "rotated",
        "messages": (ModelMessage(role="user", content="private-mutated"),),
        "timeout_ms": 1, "tools": model_request(model_registry()).tools}[field]
    with pytest.raises(GeneralWorkerJournalError):
        journal.reserve_call(operation, request=request.model_copy(update={field: value}),
            request_body=b"private-request-body-sentinel")
    assert journal.read(operation.identity) == operation


@pytest.mark.parametrize("fault", ["schema", "action", "extra", "context", "idempotency", "causation", "stream"])
def test_replay_rejects_strict_schema_and_event_bindings(tmp_path, fault):
    from orchestrator.runtime.general_worker_journal import _event_key
    with SQLiteEventStore(tmp_path / "source.db") as source:
        journal, operation = requested(source)
        event = source.read_stream("general_worker", operation.identity.stream_id)[0]
    payload = json.loads(json.dumps(event.payload))
    if fault == "schema": payload["schema_version"] = True
    if fault == "action": payload["action"] = "Accepted"
    if fault == "extra": payload["data"]["extra"] = "untrusted"
    if fault == "context": payload["data"]["context"]["graph_version"] += 1
    with SQLiteEventStore(tmp_path / "target.db") as target:
        target.append("general_worker", "different" if fault == "stream" else event.stream_id, 0,
            [EventDraft("GeneralWorker" + payload["action"], payload, run_id=event.run_id,
                node_id=event.node_id, attempt_id=event.attempt_id, fencing_generation=event.fencing_generation,
                correlation_id=event.correlation_id,
                causation_id=H if fault != "causation" else "sha256:" + "b" * 64)],
            "different" if fault == "idempotency" else _event_key(operation.operation_id, payload["action"], payload["data"]))
        with pytest.raises(GeneralWorkerJournalError):
            GeneralWorkerJournal(target).validate_operations()


@pytest.mark.parametrize("cap", ["MAX_EVENTS", "MAX_RECORD_BYTES", "MAX_HISTORY_BYTES", "MAX_TURNS"])
def test_caps_reject_new_write_without_losing_prior_history(store, monkeypatch, cap):
    import orchestrator.runtime.general_worker_journal as module
    journal, operation = requested(store)
    monkeypatch.setattr(module, cap, 1 if cap == "MAX_EVENTS" else 0)
    with pytest.raises(GeneralWorkerJournalError):
        journal.prepare_turn(operation, phase="request", prepared=epoch()[0])
    assert len(store.read_stream("general_worker", operation.identity.stream_id)) == 1


@pytest.mark.parametrize("field", ["context", "input_commitment", "launch_id"])
def test_request_checkpoint_rejects_foreign_worker_proposal(store, field):
    journal, operation = requested(store)
    prepared, ack, stop = epoch()
    operation = journal.prepare_turn(operation, phase="request", prepared=prepared)
    operation = journal.ack_turn(operation, receipt=ack)
    proposal = request_proposal(operation)
    value = {"context": proposal.context.model_copy(update={"input_manifest_hash": "sha256:" + "b" * 64}),
        "input_commitment": "sha256:" + "b" * 64,
        "launch_id": "9c483536-3c61-41e7-977b-6559b2732937"}[field]
    with pytest.raises(GeneralWorkerJournalError):
        journal.stop_turn(operation, receipt=stop, request_proposal=replace(proposal, **{field: value}))
    assert journal.read(operation.identity) == operation


def test_call_reservation_binds_encoded_body_independently(store):
    journal, operation, request = reserved(store)
    assert operation.call.request_body_sha256 == "sha256:" + hashlib.sha256(b"private-request-body-sentinel").hexdigest()
    with pytest.raises(GeneralWorkerJournalError):
        journal.reserve_call(operation, request=request, request_body=b"different-private-body")


def test_constructed_operation_numeric_coercion_is_rejected(store):
    journal, operation = requested(store)
    forged = replace(operation, output_byte_limit=float(operation.output_byte_limit))
    with pytest.raises(GeneralWorkerJournalError):
        journal.prepare_turn(forged, phase="request", prepared=epoch()[0])


@pytest.mark.parametrize("body", [b"", b"x" * 1_048_577, "not-bytes"])
def test_reserved_request_body_is_bounded_exact_bytes(store, body):
    journal, operation, request = reserved(store)
    with pytest.raises(GeneralWorkerJournalError):
        journal.reserve_call(operation, request=request, request_body=body)


def test_capture_rejects_provider_body_different_from_reservation(store):
    journal, operation, request = reserved(store)
    provider_success(store, request, body=b"changed-private-body")
    with pytest.raises(GeneralWorkerJournalError):
        capture(journal, operation)
