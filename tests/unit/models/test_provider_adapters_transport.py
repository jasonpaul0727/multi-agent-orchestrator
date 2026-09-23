import asyncio
from email.message import Message
import io
import json
import threading
import time
from urllib.error import HTTPError, URLError

import pytest

from orchestrator.config.models import ModelRegistryManifest, ModelSpec, ProviderSpec
from orchestrator.models import (
    AcceptedModelRoute,
    AnthropicMessagesAdapter,
    CostSnapshotRefs,
    ModelGatewayError,
    ModelMessage,
    ModelRequest,
    ModelToolDefinition,
    OpenAICompatibleAdapter,
    OpenAIResponsesAdapter,
    ProviderCredential,
    ProviderModelGateway,
    UnavailableSecretBroker,
)
from orchestrator.models.transport import (
    HTTPTransportCancelled,
    HTTPTransportFailed,
    HTTPTransportResponse,
    HTTPTransportTimedOut,
    UrllibHTTPSTransport,
)


HASH = "sha256:" + "a" * 64


def model_registry(adapter="openai_responses"):
    provider = ProviderSpec(
        id="primary",
        adapter=adapter,
        endpoint="https://compat.example/v1" if adapter == "openai_compatible" else None,
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
        messages=(
            ModelMessage(role="system", content="Follow the tool policy."),
            ModelMessage(role="user", content="Read this file."),
        ),
        tools=(
            ModelToolDefinition(
                name="read_file",
                description="Read one file",
                input_schema_json='{"type":"object","properties":{"path":{"type":"string"}}}',
            ),
        ),
        max_output_tokens=512,
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


def test_openai_responses_codec_encodes_tools_and_decodes_usage_and_calls():
    registry = model_registry()
    call = model_request(registry)
    adapter = OpenAIResponsesAdapter()

    encoded = adapter.encode_request(call, registry.models[0])
    payload = json.loads(encoded.body_json)
    assert encoded.relative_path == "responses"
    assert payload["instructions"] == "Follow the tool policy."
    assert payload["max_output_tokens"] == 512
    assert payload["store"] is False
    assert payload["tools"][0]["name"] == "read_file"
    assert "authorization" not in {header.name for header in encoded.headers}

    decoded = adapter.decode_response(
        _adapter_response(
            {
                "id": "resp_1",
                "status": "completed",
                "output": [
                    {"type": "message", "content": [{"type": "output_text", "text": "done"}]},
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "read_file",
                        "arguments": '{"path":"a.py"}',
                    },
                ],
                "usage": {
                    "input_tokens": 14,
                    "output_tokens": 6,
                    "input_tokens_details": {"cached_tokens": 3},
                    "output_tokens_details": {"reasoning_tokens": 2},
                },
            }
        ),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert decoded.output_text == "done"
    assert decoded.finish_reason == "tool_calls"
    assert decoded.tool_calls[0].arguments_json == '{"path":"a.py"}'
    assert decoded.usage.input_tokens == 14
    assert decoded.usage.reasoning_tokens == 2
    assert decoded.usage.cached_input_tokens == 3


def test_anthropic_messages_codec_maps_tool_result_and_adaptive_effort():
    registry = model_registry("anthropic_messages")
    call = model_request(registry)
    adapter = AnthropicMessagesAdapter()
    encoded = adapter.encode_request(call, registry.models[0])
    body = json.loads(encoded.body_json)
    assert encoded.relative_path == "messages"
    assert body["system"] == "Follow the tool policy."
    assert body["thinking"] == {"type": "adaptive"}
    assert body["output_config"] == {"effort": "low"}
    assert encoded.headers[1].name == "anthropic-version"

    tool_history = call.model_copy(
        update={
            "messages": (
                *call.messages,
                ModelMessage(role="tool", name="read_file", tool_call_id="toolu_1", content="contents"),
            )
        }
    )
    tool_body = json.loads(adapter.encode_request(tool_history, registry.models[0]).body_json)
    assert tool_body["messages"][-1]["content"][0]["tool_use_id"] == "toolu_1"
    decoded = adapter.decode_response(
        _adapter_response(
            {
                "id": "msg_1",
                "content": [
                    {"type": "text", "text": "found"},
                    {"type": "tool_use", "id": "toolu_2", "name": "read_file", "input": {"path": "a.py"}},
                ],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 11, "output_tokens": 5, "cache_read_input_tokens": 2},
            }
        ),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert decoded.output_text == "found"
    assert decoded.finish_reason == "tool_calls"
    assert decoded.usage.cached_input_tokens == 2


def test_openai_compatible_codec_normalizes_chat_completions():
    registry = model_registry("openai_compatible")
    call = model_request(registry)
    adapter = OpenAICompatibleAdapter()
    encoded = adapter.encode_request(call, registry.models[0])
    body = json.loads(encoded.body_json)
    assert encoded.relative_path == "chat/completions"
    assert body["messages"][0]["role"] == "system"
    assert body["tools"][0]["function"]["name"] == "read_file"
    response = adapter.decode_response(
        _adapter_response(
            {
                "id": "chatcmpl_1",
                "choices": [
                    {
                        "finish_reason": "tool_calls",
                        "message": {
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "function": {"name": "read_file", "arguments": '{"path":"a.py"}'},
                                }
                            ],
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 18,
                    "completion_tokens": 7,
                    "prompt_tokens_details": {"cached_tokens": 2},
                    "completion_tokens_details": {"reasoning_tokens": 1},
                },
            }
        ),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert response.finish_reason == "tool_calls"
    assert response.usage.input_tokens == 18
    assert response.usage.cached_input_tokens == 2


def test_adapter_decoders_reject_malformed_payloads_and_report_unavailable_usage():
    registry = model_registry()
    call = model_request(registry)
    openai = OpenAIResponsesAdapter()
    anthropic = AnthropicMessagesAdapter()
    compatible = OpenAICompatibleAdapter()

    for adapter, response in (
        (openai, _adapter_response([])),
        (openai, _adapter_response({"status": "failed", "output": []})),
        (openai, _adapter_response({"status": "completed"})),
        (openai, _adapter_response({"output": [{"type": "function_call", "name": "missing"}]})),
        (anthropic, _adapter_response({"content": [{"type": "tool_use", "name": "tool", "input": []}]})),
        (compatible, _adapter_response({"choices": []})),
        (compatible, _adapter_response({"choices": [{"message": None}]})),
        (compatible, _adapter_response({"choices": [{"message": {"content": 5}}]})),
        (compatible, _adapter_response({"choices": [{"message": {"content": "", "tool_calls": {}}}]})),
    ):
        with pytest.raises(ValueError):
            adapter.decode_response(response, request_id=call.request_id, model_id=call.model_id)

    incomplete = openai.decode_response(
        _adapter_response(
            {
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [{"type": "message", "content": [{"type": "refusal"}]}],
                "usage": {"input_tokens": -1, "output_tokens": True},
            }
        ),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert incomplete.finish_reason == "content_filter"
    assert incomplete.usage.status == "unavailable"

    length_limited = openai.decode_response(
        _adapter_response({"status": "incomplete", "output": [], "usage": {"input_tokens": 1, "output_tokens": 2}}),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert length_limited.finish_reason == "length"
    assert length_limited.usage.status == "reported"

    anthropic_result = anthropic.decode_response(
        _adapter_response({"content": [], "stop_reason": "max_tokens", "usage": {}}),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert anthropic_result.finish_reason == "length"
    assert anthropic_result.usage.status == "unavailable"

    compatible_result = compatible.decode_response(
        _adapter_response({"id": "chat_1", "choices": [{"finish_reason": "length", "message": {}}]}),
        request_id=call.request_id,
        model_id=call.model_id,
    )
    assert compatible_result.finish_reason == "length"
    assert compatible_result.output_text == ""


def test_adapter_encoders_handle_empty_tools_optional_reasoning_and_tool_messages():
    registry = model_registry()
    call = model_request(registry).model_copy(
        update={
            "messages": (ModelMessage(role="user", content="hello"),),
            "tools": (),
            "reasoning_effort": "none",
        }
    )
    openai_body = json.loads(OpenAIResponsesAdapter().encode_request(call, registry.models[0]).body_json)
    assert "instructions" not in openai_body
    assert "tools" not in openai_body
    assert "reasoning" not in openai_body

    compatible_body = json.loads(OpenAICompatibleAdapter().encode_request(call, registry.models[0]).body_json)
    assert "tools" not in compatible_body

    anthropic = AnthropicMessagesAdapter()
    tool_call = call.model_copy(
        update={
            "messages": (
                ModelMessage(role="user", content="run tool"),
                ModelMessage(role="tool", name="read_file", tool_call_id="call-1", content="result"),
            )
        }
    )
    body = json.loads(anthropic.encode_request(tool_call, registry.models[0]).body_json)
    assert body["messages"][1]["content"][0]["type"] == "tool_result"

    unsupported_effort = call.model_copy(update={"reasoning_effort": "xhigh"})
    with pytest.raises(ValueError, match="not supported"):
        anthropic.encode_request(unsupported_effort, registry.models[0])


class _Verifier:
    async def is_accepted(self, request):
        return True


class _Broker:
    async def acquire_provider_credential(self, *, secret_ref, provider, endpoint, purpose):
        assert secret_ref == "env:MODEL_KEY"
        assert endpoint == "https://api.openai.com/v1"
        assert purpose == "model_inference"
        return ProviderCredential(header_name="Authorization", value="secret-value")


class _FakeTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def post_json(self, **values):
        self.calls.append(values)
        return self.response


def test_gateway_never_reads_secrets_and_uses_only_verified_route_and_broker():
    registry = model_registry()
    call = model_request(registry)
    success = HTTPTransportResponse(
        status=200,
        headers=(("x-request-id", "provider-request-1"),),
        body=json.dumps(
            {
                "id": "resp_1",
                "status": "completed",
                "output": [{"type": "message", "content": [{"type": "output_text", "text": "ok"}]}],
                "usage": {"input_tokens": 3, "output_tokens": 1},
            }
        ).encode(),
    )
    transport = _FakeTransport(success)
    unavailable = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        transport=transport,
    )
    with pytest.raises(ModelGatewayError) as failure:
        asyncio.run(unavailable.invoke(call))
    assert failure.value.failure.code == "credential_delivery_unsupported"
    assert transport.calls == []

    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
    )
    response = asyncio.run(gateway.invoke(call))
    assert response.output_text == "ok"
    sent = transport.calls[-1]
    assert sent["url"] == "https://api.openai.com/v1/responses"
    assert ("Authorization", "Bearer secret-value") in sent["headers"]
    assert ("Idempotency-Key", call.idempotency_key) in sent["headers"]
    assert "secret-value" not in repr(ProviderCredential("authorization", "secret-value"))


def test_gateway_normalizes_rate_limits_without_exposing_provider_body():
    registry = model_registry()
    call = model_request(registry)
    transport = _FakeTransport(
        HTTPTransportResponse(
            status=429,
            headers=(("retry-after", "2"), ("x-request-id", "rate-1")),
            body=b'{"error":{"code":"rate_limit_exceeded","message":"sensitive body"}}',
        )
    )
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=transport,
    )
    with pytest.raises(ModelGatewayError) as result:
        asyncio.run(gateway.invoke(call))
    assert result.value.failure.code == "rate_limited"
    assert result.value.failure.retry_after_ms == 2_000
    assert result.value.failure.provider_code == "rate_limit_exceeded"
    assert "sensitive body" not in str(result.value)


class _Cancellation:
    def __init__(self, *, cancelled=False):
        self.cancelled = cancelled


class _RejectingVerifier:
    async def is_accepted(self, request):
        return False


class _RaisingBroker:
    async def acquire_provider_credential(self, **kwargs):
        raise RuntimeError("never include broker details")


class _StaticBroker:
    def __init__(self, credential):
        self.credential = credential

    async def acquire_provider_credential(self, **kwargs):
        return self.credential


def _gateway_error(gateway, call, *, cancellation=None):
    with pytest.raises(ModelGatewayError) as result:
        asyncio.run(gateway.invoke(call, cancellation=cancellation))
    return result.value.failure


def test_gateway_fails_closed_for_route_broker_headers_and_cancellation():
    registry = model_registry()
    call = model_request(registry)
    no_verifier = ProviderModelGateway(registry=registry, secret_broker=_Broker())
    assert _gateway_error(no_verifier, call).code == "stale_decision"

    denied_route = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_RejectingVerifier(),
        secret_broker=_Broker(),
    )
    assert _gateway_error(denied_route, call).code == "stale_decision"

    broker_error = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_RaisingBroker(),
    )
    assert _gateway_error(broker_error, call).code == "credential_delivery_unsupported"

    bad_credential = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_StaticBroker(ProviderCredential("x-api-key", "bad-header")),
    )
    assert _gateway_error(bad_credential, call).code == "credential_unavailable"

    pre_cancelled = _Cancellation(cancelled=True)
    assert _gateway_error(denied_route, call, cancellation=pre_cancelled).code == "cancelled"


@pytest.mark.parametrize(
    ("status", "headers", "body", "expected", "outcome", "retryable"),
    [
        (301, (), b"", "invalid_response", "known_failure", False),
        (401, (), b'{"error":{"type":"bad_key"}}', "authentication_failed", "known_failure", False),
        (408, (), b"", "timeout", "unknown", False),
        (500, (), b'{"error":{"code":"server.down"}}', "provider_unavailable", "unknown", False),
        (400, (), b"not-json", "invalid_request", "known_failure", False),
    ],
)
def test_gateway_normalizes_provider_statuses_without_leaking_bodies(
    status, headers, body, expected, outcome, retryable
):
    registry = model_registry()
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=_FakeTransport(HTTPTransportResponse(status=status, headers=headers, body=body)),
    )
    failure = _gateway_error(gateway, model_request(registry))
    assert (failure.code, failure.outcome, failure.retryable) == (expected, outcome, retryable)
    assert (failure.provider_code is None) is (body in (b"", b"not-json"))


def test_gateway_maps_transport_timeout_cancel_and_malformed_response():
    registry = model_registry()
    call = model_request(registry)

    class _RaisingTransport:
        def __init__(self, error):
            self.error = error

        async def post_json(self, **kwargs):
            raise self.error

    for error, code in (
        (HTTPTransportTimedOut(), "timeout"),
        (HTTPTransportCancelled(may_have_been_sent=True), "cancelled"),
        (HTTPTransportFailed(may_have_been_sent=False), "transport_error"),
    ):
        gateway = ProviderModelGateway(
            registry=registry,
            accepted_route_verifier=_Verifier(),
            secret_broker=_Broker(),
            transport=_RaisingTransport(error),
        )
        failure = _gateway_error(gateway, call)
        assert failure.code == code
        assert failure.outcome == ("not_sent" if isinstance(error, HTTPTransportFailed) else "unknown")

    malformed = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=_FakeTransport(HTTPTransportResponse(status=200, headers=(), body=b"not-json")),
    )
    assert _gateway_error(malformed, call).code == "invalid_response"


def test_urllib_transport_uses_https_no_proxy_no_redirect_and_bounded_read(monkeypatch):
    observed = {}

    class _Response:
        status = 200
        headers = {"x-request-id": "req"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit):
            observed["read_limit"] = limit
            return b'{"ok":true}'

    class _Opener:
        def open(self, request, *, timeout):
            observed["method"] = request.method
            observed["url"] = request.full_url
            observed["headers"] = request.header_items()
            observed["timeout"] = timeout
            return _Response()

    def fake_build_opener(*handlers):
        observed["handlers"] = handlers
        return _Opener()

    monkeypatch.setattr("orchestrator.models.transport.build_opener", fake_build_opener)
    response = UrllibHTTPSTransport._send(
        "https://api.example/v1/responses",
        (("Content-Type", "application/json"),),
        b"{}",
        2.5,
        1024,
    )
    assert response.status == 200
    assert observed["method"] == "POST"
    assert observed["timeout"] == 2.5
    assert observed["read_limit"] == 1025
    assert any(handler.__class__.__name__ == "_NoRedirect" for handler in observed["handlers"])


def test_urllib_transport_rejects_unsafe_urls_and_pre_cancelled_calls():
    transport = UrllibHTTPSTransport()
    for url in ("http://api.example/v1", "https:///missing-host", "https://user:pass@api.example/v1"):
        with pytest.raises(ValueError, match="credential-free HTTPS"):
            asyncio.run(
                transport.post_json(
                    url=url,
                    headers=(),
                    body=b"{}",
                    timeout_ms=100,
                    cancellation=None,
                    max_response_bytes=1024,
                )
            )

    signal = _Cancellation(cancelled=True)
    with pytest.raises(HTTPTransportCancelled) as result:
        asyncio.run(
            transport.post_json(
                url="https://api.example/v1",
                headers=(),
                body=b"{}",
                timeout_ms=100,
                cancellation=signal,
                max_response_bytes=1024,
            )
        )
    assert result.value.may_have_been_sent is False


def test_urllib_transport_cancellation_and_timeout_report_unknown_send_state(monkeypatch):
    transport = UrllibHTTPSTransport()
    started = threading.Event()
    release = threading.Event()

    def blocking_send(*args):
        started.set()
        release.wait(1)
        return HTTPTransportResponse(status=200, headers=(), body=b"{}")

    monkeypatch.setattr(UrllibHTTPSTransport, "_send", staticmethod(blocking_send))

    async def cancel_after_send_started():
        signal = _Cancellation()
        task = asyncio.create_task(
            transport.post_json(
                url="https://api.example/v1",
                headers=(),
                body=b"{}",
                timeout_ms=1_000,
                cancellation=signal,
                max_response_bytes=1024,
            )
        )
        assert await asyncio.to_thread(started.wait, 1)
        signal.cancelled = True
        try:
            await task
        finally:
            release.set()

    with pytest.raises(HTTPTransportCancelled) as result:
        asyncio.run(cancel_after_send_started())
    assert result.value.may_have_been_sent is True

    started.clear()
    release.clear()

    async def timeout_after_send_started():
        task = asyncio.create_task(
            transport.post_json(
                url="https://api.example/v1",
                headers=(),
                body=b"{}",
                timeout_ms=20,
                cancellation=None,
                max_response_bytes=1024,
            )
        )
        assert await asyncio.to_thread(started.wait, 1)
        try:
            await task
        finally:
            release.set()

    with pytest.raises(HTTPTransportTimedOut):
        asyncio.run(timeout_after_send_started())
    time.sleep(0.02)


def test_urllib_transport_converts_socket_failures_and_reads_http_error_bodies(monkeypatch):
    async_transport = UrllibHTTPSTransport()
    with monkeypatch.context() as patch:
        patch.setattr(
            UrllibHTTPSTransport,
            "_send",
            staticmethod(lambda *args: (_ for _ in ()).throw(URLError("offline"))),
        )
        with pytest.raises(HTTPTransportFailed) as failed:
            asyncio.run(
                async_transport.post_json(
                    url="https://api.example/v1",
                    headers=(),
                    body=b"{}",
                    timeout_ms=100,
                    cancellation=None,
                    max_response_bytes=1024,
                )
            )
    assert failed.value.may_have_been_sent is True

    headers = Message()
    headers["x-request-id"] = "http-error-id"
    headers["set-cookie"] = "must-not-be-captured"
    http_error = HTTPError(
        "https://api.example/v1",
        503,
        "unavailable",
        headers,
        io.BytesIO(b'{"error":"down"}'),
    )

    class _Opener:
        def open(self, *args, **kwargs):
            raise http_error

    monkeypatch.setattr("orchestrator.models.transport.build_opener", lambda *args: _Opener())
    response = UrllibHTTPSTransport._send("https://api.example/v1", (), b"{}", 1, 1024)
    assert response.status == 503
    assert response.body == b'{"error":"down"}'
    assert response.headers == (("x-request-id", "http-error-id"),)

    class _TooLarge:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit):
            return b"x" * limit

    monkeypatch.setattr(
        "orchestrator.models.transport.build_opener",
        lambda *args: type("_Opener", (), {"open": lambda self, *a, **kw: _TooLarge()})(),
    )
    with pytest.raises(ValueError, match="byte limit"):
        UrllibHTTPSTransport._send("https://api.example/v1", (), b"{}", 1, 8)


def _adapter_response(value):
    from orchestrator.models import AdapterResponse

    return AdapterResponse(http_status=200, body_json=json.dumps(value))
