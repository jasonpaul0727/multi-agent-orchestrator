import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier

import pytest
from pydantic import ValidationError
import orchestrator.models as model_exports

from orchestrator.models import (
    AcceptedModelRoute,
    CostSnapshotRefs,
    ModelMessage,
    ModelGatewayError,
    ModelRequest,
    ModelToolDefinition,
    ProviderCallReplayBlocked,
    ProviderCredential,
    ProviderModelGateway,
    SQLiteProviderCallJournal,
)
from orchestrator.config.models import ModelRegistryManifest, ModelSpec, ProviderSpec
from orchestrator.budget.models import UsageRecord
from orchestrator.models.provider_calls import (
    ProviderCallJournalConflict,
    ProviderCallOutcomeConflict,
    ProviderCallReconciliation,
)
from orchestrator.models.transport import HTTPTransportResponse
from orchestrator.persistence import (
    EventContractError,
    EventDraft,
    IdempotencyConflict,
    SQLiteEventStore,
    StaleStream,
)


HASH = "sha256:" + "a" * 64


def model_registry():
    provider = ProviderSpec(
        id="primary",
        adapter="openai_responses",
        secret_ref="env:MODEL_KEY",
        enabled=True,
    )
    model = ModelSpec(
        id="model-1",
        provider="primary",
        remote_model="remote-model-1",
        tier="standard",
        capabilities={"text", "tools"},
        context_window=8_192,
        max_output_tokens=1_024,
        supported_reasoning_efforts={"none", "low", "medium", "high"},
        price=None,
        local_zero_cost=True,
    )
    return ModelRegistryManifest(providers=(provider,), models=(model,))


def model_request(registry):
    route = AcceptedModelRoute(
        decision_id="decision-1",
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        budget_reservation_id="reservation-1",
        model_id="model-1",
        provider_id="primary",
        reasoning_effort="low",
        registry_manifest_hash=registry.content_hash,
    )
    return ModelRequest(
        request_id="request-1",
        idempotency_key="idem-1",
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        budget_reservation_id="reservation-1",
        model_id="model-1",
        accepted_route=route,
        messages=(ModelMessage(role="user", content="private prompt"),),
        tools=(ModelToolDefinition(
            name="read_file",
            description="Read a file",
            input_schema_json='{"type":"object","properties":{"path":{"type":"string"}}}',
        ),),
        max_output_tokens=32,
        reasoning_effort="low",
        timeout_ms=5_000,
        cost_snapshots=CostSnapshotRefs(
            registry_manifest_hash=registry.content_hash,
            tokenizer_snapshot_id=HASH,
            fx_snapshot_id=HASH,
            price_snapshot_id=registry.content_hash,
            estimator_snapshot_id=HASH,
        ),
    )


class _Verifier:
    async def is_accepted(self, request):
        return True


class _Broker:
    def __init__(self):
        self.calls = 0

    async def acquire_provider_credential(self, **kwargs):
        self.calls += 1
        return ProviderCredential(
            "authorization", "secret", "primary", "https://api.openai.com/v1", "model_inference"
        )


class _FakeTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def post_json(self, **values):
        self.calls.append(values)
        return self.response


def _success_response():
    return HTTPTransportResponse(
        status=200,
        headers=(("x-request-id", "provider-req-1"),),
        body=json.dumps(
            {
                "id": "response-1",
                "status": "completed",
                "output": [{"type": "message", "content": [{"type": "output_text", "text": "private answer"}]}],
                "usage": {"input_tokens": 3, "output_tokens": 2},
            }
        ).encode(),
    )


def _provider_reconciliation_for(call, **overrides):
    values = {
        "provider_call_stream_id": call.stream_id,
        "provider_adapter": call.provider_adapter,
        "provider_correlation_id": call.provider_correlation_id,
        "run_id": call.run_id,
        "node_id": call.node_id,
        "attempt_id": call.attempt_id,
        "fencing_generation": call.fencing_generation,
        "provider_id": call.provider_id,
        "model_id": call.model_id,
        "accepted_route_id": call.accepted_route_id,
        "budget_reservation_id": call.budget_reservation_id,
        "registry_manifest_hash": call.registry_manifest_hash,
        "request_hash": call.request_hash,
        "effect": "not_received",
        "usage": None,
        "provider_request_id": None,
        "evidence_source": "provider_signed_receipt",
        "evidence_digest": "sha256:" + "b" * 64,
        "termination_receipt_hash": "sha256:" + "c" * 64,
        "observed_at": datetime(2026, 9, 29, 12, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return ProviderCallReconciliation(**values)


def test_provider_call_intent_and_receipt_survive_restart_without_prompt_or_output(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store_path = tmp_path / "provider-calls.db"
    first_store = SQLiteEventStore(store_path)
    journal = SQLiteProviderCallJournal(first_store)

    journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b'{"prompt":"private prompt"}',
        provider_correlation_id="maestro-restart-test",
    )
    journal.record_outcome(
        request,
        outcome="known_success",
        provider_request_id="provider-req-1",
        http_status=200,
        usage={"status": "reported", "input_tokens": 3, "output_tokens": 2},
    )
    first_store.close()

    reopened = SQLiteEventStore(store_path)
    recovered = SQLiteProviderCallJournal(reopened).read(request)

    assert recovered is not None
    assert recovered.status == "known_success"
    assert recovered.provider_request_id == "provider-req-1"
    assert recovered.usage == {"status": "reported", "input_tokens": 3, "output_tokens": 2}
    events = reopened.read_stream("provider_call", recovered.stream_id)
    serialized = repr([event.payload for event in events])
    assert "private prompt" not in serialized
    assert "private answer" not in serialized
    assert recovered.request_hash.startswith("sha256:")
    assert SQLiteProviderCallJournal(reopened).unresolved() == ()


def _sender_termination_receipt(call, **overrides):
    values = {
        "provider_call_stream_id": call.stream_id,
        "run_id": call.run_id,
        "node_id": call.node_id,
        "attempt_id": call.attempt_id,
        "fencing_generation": call.fencing_generation,
        "accepted_route_id": call.accepted_route_id,
        "budget_reservation_id": call.budget_reservation_id,
        "provider_id": call.provider_id,
        "model_id": call.model_id,
        "registry_manifest_hash": call.registry_manifest_hash,
        "request_hash": call.request_hash,
        "unit_name": "maestro-provider-" + "a" * 32 + ".service",
        "cgroup_path_hash": "sha256:" + "b" * 64,
        "active_state": "inactive",
        "cgroup_empty": True,
        "observed_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return model_exports.ProviderSenderTerminationReceipt(**values)


def test_provider_call_journal_replays_attempt_bound_sender_receipt(tmp_path):
    store_path = tmp_path / "sender-receipt.db"
    store = SQLiteEventStore(store_path)
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(store)
    journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-test",
    )
    call = journal.read(request)
    assert call is not None
    receipt = _sender_termination_receipt(call)
    assert receipt.receipt_hash.startswith("sha256:")
    assert len(receipt.receipt_hash) == 71

    journal.record_outcome(
        request, outcome="unknown", failure_code="timeout",
        termination_receipt=receipt,
    )
    store.close()
    with SQLiteEventStore(store_path) as reopened:
        recovered = SQLiteProviderCallJournal(reopened).read(request)
        assert recovered is not None and recovered.termination_receipt == receipt
        outcome = reopened.read_stream("provider_call", call.stream_id)[1]
        serialized = repr(outcome.payload)
        assert str(receipt.receipt_hash) not in serialized
        assert "/sys/fs/cgroup/" not in serialized


@pytest.mark.parametrize(
    "binding_override",
    [
        {"provider_call_stream_id": "other-call"},
        {"attempt_id": "other-attempt"},
        {"fencing_generation": 2},
        {"accepted_route_id": "other-route"},
    ],
)
def test_provider_call_journal_rejects_sender_receipt_from_another_call(
    tmp_path, binding_override
):
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "sender-binding.db"))
    journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-test",
    )
    call = journal.read(request)
    assert call is not None
    receipt = _sender_termination_receipt(call, **binding_override)

    with pytest.raises(ValueError, match="sender receipt.*intent|binding"):
        journal.record_outcome(
            request, outcome="unknown", failure_code="timeout",
            termination_receipt=receipt,
        )

    assert journal.read(request).status == "dispatching"


def test_provider_call_journal_rejects_malformed_sender_cgroup_digest(tmp_path):
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "sender-digest.db"))
    journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-test",
    )
    call = journal.read(request)
    assert call is not None

    with pytest.raises(ValidationError):
        _sender_termination_receipt(call, cgroup_path_hash="sha256:not-a-digest")


def test_provider_call_journal_revalidates_sender_receipt_model_copy(tmp_path):
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "sender-copy.db"))
    journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-test",
    )
    call = journal.read(request)
    assert call is not None
    forged = _sender_termination_receipt(call).model_copy(update={"cgroup_empty": False})

    with pytest.raises(ValidationError):
        journal.record_outcome(
            request, outcome="unknown", failure_code="timeout",
            termination_receipt=forged,
        )

    assert journal.read(request).status == "dispatching"


def test_legacy_provider_outcome_replays_without_sender_receipt(tmp_path):
    request = model_request(model_registry())
    store = SQLiteEventStore(tmp_path / "legacy-no-sender-receipt.db")
    journal = SQLiteProviderCallJournal(store)
    journal.record_intent(
        request, provider_id="primary", provider_adapter="openai_responses",
        request_body=b"{}", provider_correlation_id="maestro-test",
    )
    journal.record_outcome(request, outcome="not_sent", failure_code="cancelled")

    recovered = journal.read(request)

    assert recovered is not None
    assert recovered.termination_receipt is None


def test_provider_call_outcome_must_match_the_entire_frozen_call_scope(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "binding.db")
    journal = SQLiteProviderCallJournal(store)
    journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b'{"prompt":"secret"}', provider_correlation_id="maestro-test")
    mismatched_route = request.accepted_route.model_copy(update={"decision_id": "other-decision"})
    mismatched_request = request.model_copy(update={"accepted_route": mismatched_route})

    with pytest.raises(ValueError, match="binding"):
        journal.record_outcome(mismatched_request, outcome="known_success", http_status=200)

    assert journal.read(request).status == "dispatching"


def test_unresolved_provider_call_is_recoverable_but_not_replayed_after_restart(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    database = tmp_path / "unresolved.db"
    first = SQLiteEventStore(database)
    first_journal = SQLiteProviderCallJournal(first)
    first_journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b'{"prompt":"secret"}', provider_correlation_id="maestro-test")
    first.close()

    reopened = SQLiteEventStore(database)
    recovered = SQLiteProviderCallJournal(reopened)
    [pending] = recovered.unresolved()

    assert pending.status == "dispatching"
    with pytest.raises(ProviderCallReplayBlocked):
        recovered.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b'{"prompt":"secret"}', provider_correlation_id="maestro-test")


def test_event_store_rejects_provider_outcome_without_matching_intent(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "orphan-outcome.db")

    with pytest.raises(EventContractError, match="orphaned"):
        store.append(
            "provider_call",
            "orphan-call",
            expected_version=0,
            events=[EventDraft(
                "ProviderCallOutcomeRecorded",
                {
                    "outcome": "known_success",
                    "provider_request_id": None,
                    "http_status": 200,
                    "failure_code": None,
                    "usage": None,
                },
                run_id=request.run_id,
                node_id=request.node_id,
                attempt_id=request.attempt_id,
                fencing_generation=request.fencing_generation,
                causation_id=request.accepted_route.decision_id,
            )],
            idempotency_key="orphan-outcome",
        )


def test_provider_journal_rejects_invalid_store_provider_and_body(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    with pytest.raises(TypeError, match="durable SQLite"):
        SQLiteProviderCallJournal(object())
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "invalid-intent.db"))
    with pytest.raises(ValueError, match="accepted route"):
        journal.record_intent(request, provider_id="wrong-provider", provider_adapter="openai_responses", request_body=b"{}", provider_correlation_id="maestro-test")
    with pytest.raises(ValueError, match="bounded bytes"):
        journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body="not bytes", provider_correlation_id="maestro-test")
    with pytest.raises(ValueError, match="bounded bytes"):
        journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b"x" * 8_000_001, provider_correlation_id="maestro-test")


@pytest.mark.parametrize(
    "receipt",
    [
        {"outcome": "maybe"},
        {"outcome": "unknown", "provider_request_id": "bad\nheader"},
        {"outcome": "known_success", "http_status": True},
        {"outcome": "known_success"},
        {"outcome": "known_failure", "http_status": 200},
        {"outcome": "not_sent", "http_status": 503},
        {"outcome": "known_failure", "failure_code": "bad code"},
        {"outcome": "known_success", "usage": {"status": "unavailable", "input_tokens": 0}},
    ],
)
def test_provider_journal_rejects_malformed_terminal_receipts(tmp_path, receipt):
    registry = model_registry()
    request = model_request(registry)
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "invalid-receipt.db"))
    journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b"{}", provider_correlation_id="maestro-test")

    with pytest.raises(ValueError):
        journal.record_outcome(request, **receipt)

    assert journal.read(request).status == "dispatching"


def test_provider_journal_rejects_outcome_without_intent(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "missing-intent.db"))

    with pytest.raises(ValueError, match="prior durable intent"):
        journal.record_outcome(request, outcome="known_failure")


def test_provider_journal_idempotency_collision_cannot_claim_second_body(tmp_path, monkeypatch):
    registry = model_registry()
    request = model_request(registry)
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "claim-collision.db"))
    monkeypatch.setattr("orchestrator.models.provider_calls.new_id", lambda: "fixed-claim")
    journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b"first-body", provider_correlation_id="maestro-test")

    with pytest.raises(ProviderCallReplayBlocked):
        journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses", request_body=b"different-body", provider_correlation_id="maestro-test")


def test_known_outcome_between_reconciliation_snapshot_and_append_is_typed_conflict(tmp_path, monkeypatch):
    request = model_request(model_registry())
    path = tmp_path / "late-outcome.db"
    with SQLiteEventStore(path) as store:
        journal = SQLiteProviderCallJournal(store)
        stream_id = journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                                          request_body=b"{}", provider_correlation_id="maestro-race")
        proof = _provider_reconciliation_for(journal.read_call(stream_id))
        original_read = store.read_stream
        injected = []

        def read_then_commit_outcome(*args, **kwargs):
            events = original_read(*args, **kwargs)
            if args == ("provider_call", stream_id) and not injected:
                injected.append(True)
                with SQLiteEventStore(path) as competing:
                    SQLiteProviderCallJournal(competing).record_outcome(
                        request, outcome="known_success", http_status=200,
                    )
            return events

        monkeypatch.setattr(store, "read_stream", read_then_commit_outcome)
        with pytest.raises(ProviderCallJournalConflict):
            journal._append_reconciliation(stream_id, proof, datetime(2026, 9, 29, 15, tzinfo=timezone.utc))
        assert journal.read_call(stream_id).status == "known_success"
        assert len(original_read("provider_call", stream_id)) == 2


def test_provider_call_intent_is_single_winner_across_concurrent_store_connections(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    database = tmp_path / "racing-calls.db"
    SQLiteEventStore(database).close()

    def reserve():
        store = SQLiteEventStore(database)
        try:
            return SQLiteProviderCallJournal(store).record_intent(
                request,
                provider_id="primary",
                provider_adapter="openai_responses",
                request_body=b'{"model":"remote-model-1"}',
                provider_correlation_id="maestro-test",
            )
        except ProviderCallReplayBlocked:
            return "blocked"
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: reserve(), range(2)))

    assert results.count("blocked") == 1
    assert len({value for value in results if value != "blocked"}) == 1
    check = SQLiteEventStore(database)
    stream_ids = check.stream_ids("provider_call")
    assert len(stream_ids) == 1
    assert len(check.read_stream("provider_call", stream_ids[0])) == 1


def test_late_success_and_provider_reconciliation_have_one_durable_winner(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    database = tmp_path / "late-success-reconciliation-race.db"
    seed = SQLiteEventStore(database)
    journal = SQLiteProviderCallJournal(seed)
    journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b'{"prompt":"private prompt"}',
        provider_correlation_id="maestro-race-test",
    )
    call = journal.read(request)
    assert call is not None
    proof = _provider_reconciliation_for(call)
    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    stream_id = call.stream_id
    seed.close()

    barrier = Barrier(2)

    def try_write(operation):
        barrier.wait()
        store = SQLiteEventStore(database)
        writer = SQLiteProviderCallJournal(store)
        try:
            if operation == "outcome":
                writer.record_outcome(request, outcome="known_success", http_status=200)
            else:
                writer._append_reconciliation(stream_id, proof, reconciled_at)
            return "written"
        except (
            ProviderCallOutcomeConflict,
            ProviderCallJournalConflict,
            IdempotencyConflict,
            StaleStream,
        ):
            return "conflict"
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = (
            pool.submit(try_write, "outcome"),
            pool.submit(try_write, "reconciliation"),
        )
        results = [future.result() for future in futures]

    assert results.count("written") == 1
    assert results.count("conflict") == 1
    first_store = SQLiteEventStore(database)
    first_journal = SQLiteProviderCallJournal(first_store)
    snapshot = first_journal.read_call(stream_id)
    assert snapshot is not None
    assert snapshot.status in {"known_success", "settlement_pending"}
    events = first_store.read_stream("provider_call", stream_id)
    assert len(events) == 2
    assert sum(event.event_type == "ProviderCallOutcomeRecorded" for event in events) + sum(
        event.event_type == "ProviderCallReconciliationRecorded" for event in events
    ) == 1
    assert first_journal.unresolved() == ()
    if snapshot.status == "settlement_pending":
        assert first_journal.pending_settlements() == (snapshot,)
    else:
        assert first_journal.pending_settlements() == ()
    first_store.close()


def test_provider_conflict_exceptions_preserve_their_public_base_classes():
    assert issubclass(ProviderCallJournalConflict, RuntimeError)
    assert issubclass(ProviderCallOutcomeConflict, ValueError)


def test_gateway_persists_success_and_blocks_same_request_replay(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "gateway-calls.db")
    journal = SQLiteProviderCallJournal(store)
    transport = _FakeTransport(_success_response())
    broker = _Broker()
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=broker,
        transport=transport,
        provider_call_journal=journal,
    )

    response = asyncio.run(gateway.invoke(request))

    assert response.output_text == "private answer"
    assert journal.read(request).status == "known_success"
    assert len(transport.calls) == 1
    with pytest.raises(ModelGatewayError) as replay:
        asyncio.run(gateway.invoke(request))
    assert replay.value.failure.code == "idempotency_conflict"
    assert len(transport.calls) == 1
    assert broker.calls == 1


def test_gateway_persists_unknown_transport_outcome_and_never_replays(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "gateway-unknown.db")
    journal = SQLiteProviderCallJournal(store)

    class _LostResponse:
        calls = 0

        async def post_json(self, **kwargs):
            self.calls += 1
            raise TimeoutError("connection lost after dispatch")

    transport = _LostResponse()
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
        provider_call_journal=journal,
    )

    with pytest.raises(ModelGatewayError) as failure:
        asyncio.run(gateway.invoke(request))

    assert failure.value.failure.outcome == "unknown"
    assert journal.read(request).status == "unknown"
    with pytest.raises(ModelGatewayError) as replay:
        asyncio.run(gateway.invoke(request))
    assert replay.value.failure.code == "idempotency_conflict"
    assert transport.calls == 1


def test_gateway_fails_closed_without_durable_provider_call_journal():
    registry = model_registry()
    transport = _FakeTransport(_success_response())
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
    )

    with pytest.raises(ModelGatewayError) as failure:
        asyncio.run(gateway.invoke(model_request(registry)))

    assert failure.value.failure.code == "provider_journal_unavailable"
    assert failure.value.failure.outcome == "not_sent"
    assert transport.calls == []


def test_gateway_keeps_success_unresolved_if_receipt_persistence_fails(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "receipt-write-failure.db")
    journal = SQLiteProviderCallJournal(store)

    class _ReceiptWriteFailure:
        def read(self, *args, **kwargs):
            return journal.read(*args, **kwargs)

        def record_intent(self, *args, **kwargs):
            return journal.record_intent(*args, **kwargs)

        def record_outcome(self, *args, **kwargs):
            raise OSError("receipt store unavailable")

    transport = _FakeTransport(_success_response())
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
        provider_call_journal=_ReceiptWriteFailure(),
    )

    with pytest.raises(ModelGatewayError) as failure:
        asyncio.run(gateway.invoke(request))

    assert failure.value.failure.code == "outcome_unknown"
    assert failure.value.failure.outcome == "unknown"
    assert journal.read(request).status == "dispatching"
    assert len(transport.calls) == 1
    retry = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
        provider_call_journal=journal,
    )
    with pytest.raises(ModelGatewayError) as blocked:
        asyncio.run(retry.invoke(request))
    assert blocked.value.failure.code == "idempotency_conflict"
    assert len(transport.calls) == 1


def test_gateway_persists_and_sends_one_openai_correlation_id(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    store = SQLiteEventStore(tmp_path / "correlation.db")
    journal = SQLiteProviderCallJournal(store)
    transport = _FakeTransport(_success_response())
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
        provider_call_journal=journal,
    )

    asyncio.run(gateway.invoke(request))

    pending = journal.read(request)
    assert pending is not None
    assert pending.provider_correlation_id is not None
    headers = dict(transport.calls[0]["headers"])
    assert headers["X-Client-Request-Id"] == pending.provider_correlation_id
    assert pending.provider_correlation_id.isascii()
    assert 0 < len(pending.provider_correlation_id) <= 512


def test_openai_correlation_id_survives_provider_journal_restart(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    database = tmp_path / "correlation-restart.db"
    first_store = SQLiteEventStore(database)
    first_journal = SQLiteProviderCallJournal(first_store)
    first_journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-stable-correlation-id",
    )
    first_store.close()

    reopened = SQLiteProviderCallJournal(SQLiteEventStore(database))
    recovered = reopened.unresolved()[0]

    assert recovered.provider_correlation_id == "maestro-stable-correlation-id"
    assert recovered.provider_adapter == "openai_responses"


def test_provider_reconciliation_accepts_dispatching_or_unknown_only(tmp_path):
    registry = model_registry()
    request = model_request(registry)

    def proof_for(call):
        return ProviderCallReconciliation(
            provider_call_stream_id=call.stream_id,
            provider_adapter=call.provider_adapter,
            provider_correlation_id=call.provider_correlation_id,
            run_id=call.run_id,
            node_id=call.node_id,
            attempt_id=call.attempt_id,
            fencing_generation=call.fencing_generation,
            provider_id=call.provider_id,
            model_id=call.model_id,
            accepted_route_id=call.accepted_route_id,
            budget_reservation_id=call.budget_reservation_id,
            registry_manifest_hash=call.registry_manifest_hash,
            request_hash=call.request_hash,
            effect="not_received",
            usage=None,
            provider_request_id=None,
            evidence_source="provider_signed_receipt",
            evidence_digest="sha256:" + "b" * 64,
            termination_receipt_hash="sha256:" + "c" * 64,
            observed_at=datetime(2026, 9, 29, 12, tzinfo=timezone.utc),
        )

    reconciled_at = datetime(2026, 9, 29, 13, tzinfo=timezone.utc)
    dispatch_journal = SQLiteProviderCallJournal(
        SQLiteEventStore(tmp_path / "dispatching.db")
    )
    dispatch_journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-dispatching",
    )
    dispatch_call = dispatch_journal.read_call(dispatch_journal.unresolved()[0].stream_id)
    assert dispatch_call is not None
    dispatch_journal._append_reconciliation(
        dispatch_call.stream_id, proof_for(dispatch_call), reconciled_at
    )
    dispatch_result = dispatch_journal.read_call(dispatch_call.stream_id)
    assert dispatch_result is not None
    assert dispatch_result.status == "settlement_pending"
    assert dispatch_result.reconciliation == proof_for(dispatch_call)
    assert dispatch_result.reconciled_at == reconciled_at
    assert dispatch_result.settlement_applied is False
    assert len(dispatch_journal.pending_settlements()) == 1

    unknown_journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "unknown.db"))
    unknown_journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-unknown",
    )
    unknown_journal.record_outcome(request, outcome="unknown", failure_code="timeout")
    unknown_call = unknown_journal.read_call(unknown_journal.unresolved()[0].stream_id)
    assert unknown_call is not None
    unknown_journal._append_reconciliation(
        unknown_call.stream_id, proof_for(unknown_call), reconciled_at
    )
    unknown_result = unknown_journal.read_call(unknown_call.stream_id)
    assert unknown_result is not None
    assert unknown_result.status == "settlement_pending"

    terminal_journal = SQLiteProviderCallJournal(
        SQLiteEventStore(tmp_path / "known-success.db")
    )
    terminal_journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-terminal",
    )
    terminal_journal.record_outcome(request, outcome="known_success", http_status=200)
    # The known-success stream is terminal and is not included in unresolved calls.
    terminal_events = terminal_journal.event_store.stream_ids("provider_call")
    assert len(terminal_events) == 1
    terminal_call = terminal_journal.read_call(terminal_events[0])
    assert terminal_call is not None
    with pytest.raises(ProviderCallJournalConflict, match="terminal"):
        terminal_journal._append_reconciliation(
            terminal_call.stream_id, proof_for(terminal_call), reconciled_at
        )


def test_provider_reconciliation_requires_exact_usage_and_aware_timestamps(tmp_path):
    registry = model_registry()
    request = model_request(registry)
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "invalid-proof.db"))
    journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-proof",
    )
    call = journal.read_call(journal.unresolved()[0].stream_id)
    assert call is not None

    proof_fields = {
        "provider_call_stream_id": call.stream_id,
        "provider_adapter": call.provider_adapter,
        "provider_correlation_id": call.provider_correlation_id,
        "run_id": call.run_id,
        "node_id": call.node_id,
        "attempt_id": call.attempt_id,
        "fencing_generation": call.fencing_generation,
        "provider_id": call.provider_id,
        "model_id": call.model_id,
        "accepted_route_id": call.accepted_route_id,
        "budget_reservation_id": call.budget_reservation_id,
        "registry_manifest_hash": call.registry_manifest_hash,
        "request_hash": call.request_hash,
        "evidence_source": "provider_authoritative_api",
        "evidence_digest": "sha256:" + "d" * 64,
        "termination_receipt_hash": "sha256:" + "e" * 64,
        "observed_at": datetime(2026, 9, 29, tzinfo=timezone.utc),
    }
    with pytest.raises(ValueError, match="usage"):
        ProviderCallReconciliation(
            **proof_fields,
            effect="received_and_charged",
            usage=None,
            provider_request_id="provider-request-1",
        )
    with pytest.raises(ValueError, match="usage"):
        ProviderCallReconciliation(
            **proof_fields,
            effect="not_received",
            usage=UsageRecord(
                reservation_id=call.budget_reservation_id,
                run_id=call.run_id,
                settlement_key="settlement-1",
                currency="USD",
            ),
            provider_request_id=None,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        ProviderCallReconciliation(
            **{**proof_fields, "observed_at": datetime(2026, 9, 29)},
            effect="not_received",
            usage=None,
            provider_request_id=None,
        )


def test_received_and_charged_provider_reconciliation_round_trips_exact_usage(tmp_path):
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "charged-proof.db"))
    journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-charged",
    )
    call = journal.read_call(journal.unresolved()[0].stream_id)
    assert call is not None
    usage = UsageRecord(
        reservation_id=call.budget_reservation_id,
        run_id=call.run_id,
        settlement_key="provider-settlement-1",
        currency="USD",
        input_tokens=11,
        output_tokens=7,
        provider_fee_minor=12,
        cost_minor=12,
    )
    proof = _provider_reconciliation_for(
        call,
        effect="received_and_charged",
        usage=usage,
        provider_request_id="provider-request-1",
    )

    journal._append_reconciliation(
        call.stream_id, proof, datetime(2026, 9, 29, 16, tzinfo=timezone.utc)
    )
    replayed = journal.read_call(call.stream_id)

    assert replayed is not None
    assert replayed.reconciliation == proof
    assert replayed.reconciliation.usage == usage


def test_provider_settlement_marker_must_name_matching_reconciliation_event(tmp_path):
    store = SQLiteEventStore(tmp_path / "wrong-settlement-marker.db")
    journal = SQLiteProviderCallJournal(store)
    request = model_request(model_registry())
    journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-marker",
    )
    [intent] = store.read_stream("provider_call", store.stream_ids("provider_call")[0])
    call = journal.read_call(intent.stream_id)
    assert call is not None
    proof = _provider_reconciliation_for(call)
    reconciled_at = datetime(2026, 9, 29, 17, tzinfo=timezone.utc)
    reconciliation_event_id = journal._append_reconciliation(
        intent.stream_id, proof, reconciled_at
    )
    marker_payload = {
        key: value
        for key, value in proof.model_dump(mode="json").items()
        if key in {
            "provider_call_stream_id", "provider_adapter", "provider_correlation_id",
            "run_id", "node_id", "attempt_id", "fencing_generation", "provider_id",
            "model_id", "accepted_route_id", "budget_reservation_id",
            "registry_manifest_hash", "request_hash",
        }
    }
    marker_payload["reconciliation_event_id"] = "wrong-reconciliation-event"
    marker = EventDraft(
        "ProviderCallSchedulerSettlementApplied",
        marker_payload,
        run_id=request.run_id,
        node_id=request.node_id,
        attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        causation_id="wrong-reconciliation-event",
    )

    assert reconciliation_event_id != marker_payload["reconciliation_event_id"]
    with pytest.raises(EventContractError, match="identity binding"):
        store.append(
            "provider_call", intent.stream_id, 2, [marker], "wrong-marker"
        )


def test_provider_reconciliation_and_scheduler_marker_replays_are_exact(tmp_path):
    request = model_request(model_registry())
    journal = SQLiteProviderCallJournal(SQLiteEventStore(tmp_path / "exact-replay.db"))
    journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-replay",
    )
    call = journal.read_call(journal.unresolved()[0].stream_id)
    assert call is not None
    proof = _provider_reconciliation_for(call)
    reconciled_at = datetime(2026, 9, 29, 14, tzinfo=timezone.utc)

    reconciliation_event_id = journal._append_reconciliation(
        call.stream_id, proof, reconciled_at
    )
    assert journal._append_reconciliation(
        call.stream_id, proof, reconciled_at
    ) == reconciliation_event_id
    with pytest.raises(ProviderCallJournalConflict, match="conflict"):
        journal._append_reconciliation(
            call.stream_id,
            proof.model_copy(update={"evidence_digest": "sha256:" + "f" * 64}),
            reconciled_at,
        )
    with pytest.raises(ProviderCallJournalConflict, match="conflict"):
        journal._append_reconciliation(
            call.stream_id,
            proof,
            datetime(2026, 9, 29, 14, 1, tzinfo=timezone.utc),
        )

    journal._record_scheduler_settlement(call.stream_id, reconciliation_event_id)
    journal._record_scheduler_settlement(call.stream_id, reconciliation_event_id)
    settled = journal.read_call(call.stream_id)
    assert settled is not None
    assert settled.status == "reconciled"
    assert settled.settlement_applied is True
    assert settled.reconciliation_event_id == reconciliation_event_id
    assert journal.pending_settlements() == ()
    with pytest.raises(ProviderCallJournalConflict, match="conflict"):
        journal._record_scheduler_settlement(call.stream_id, "other-reconciliation")
    assert len(journal.event_store.read_stream("provider_call", call.stream_id)) == 3


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_provider_reconciliation_append_has_one_durable_winner(tmp_path, conflicting):
    request = model_request(model_registry())
    database = tmp_path / "concurrent-reconciliation.db"
    first_store = SQLiteEventStore(database)
    first_journal = SQLiteProviderCallJournal(first_store)
    first_journal.record_intent(
        request,
        provider_id="primary",
        provider_adapter="openai_responses",
        request_body=b"{}",
        provider_correlation_id="maestro-concurrent",
    )
    call = first_journal.read_call(first_journal.unresolved()[0].stream_id)
    assert call is not None
    proof = _provider_reconciliation_for(call)
    reconciled_at = datetime(2026, 9, 29, 15, tzinfo=timezone.utc)

    barrier = Barrier(2)

    other_proof = proof.model_copy(update={"evidence_digest": "sha256:" + "e" * 64}) if conflicting else proof

    def append_from_independent_connection(candidate):
        with SQLiteEventStore(database) as store:
            original_append = store.append

            def synchronized_append(stream_type, stream_id, **kwargs):
                # Both writers have finished all reads before either CAS commits.
                assert stream_type == "provider_call" and stream_id == call.stream_id
                assert kwargs["expected_version"] == 1
                barrier.wait(timeout=10)
                return original_append(stream_type, stream_id, **kwargs)

            store.append = synchronized_append
            try:
                return SQLiteProviderCallJournal(store)._append_reconciliation(
                    call.stream_id, candidate, reconciled_at
                )
            except ProviderCallJournalConflict:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(append_from_independent_connection, proof)
        second = pool.submit(append_from_independent_connection, other_proof)
        event_ids = {first.result(), second.result()}

    assert len(event_ids) == (2 if conflicting else 1)
    assert ("conflict" in event_ids) == conflicting
    events = first_store.read_stream("provider_call", call.stream_id)
    assert [event.event_type for event in events] == [
        "ProviderCallIntentRecorded",
        "ProviderCallReconciliationRecorded",
    ]
    assert first_journal.read_call(call.stream_id).reconciliation in (proof, other_proof)
    first_store.close()
