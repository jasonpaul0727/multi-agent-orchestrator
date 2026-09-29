import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

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
from orchestrator.models.transport import HTTPTransportResponse
from orchestrator.persistence import EventContractError, EventDraft, SQLiteEventStore


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
