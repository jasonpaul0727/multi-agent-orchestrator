import pytest
from pydantic import ValidationError

from orchestrator.config.models import ModelRegistryManifest, ModelSpec, ProviderSpec
from orchestrator.models import (
    AcceptedModelRoute,
    AdapterRequest,
    AdapterRequestHeader,
    CostSnapshotRefs,
    ModelGatewayError,
    ModelGatewayFailure,
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ModelToolCall,
    TokenUsage,
    resolve_provider_url,
    validate_gateway_request,
    validate_gateway_response,
)


HASH = "sha256:" + "a" * 64


def request(**updates):
    values = {
        "request_id": "req-1",
        "idempotency_key": "idem-1",
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "fencing_generation": 2,
        "budget_reservation_id": "reservation-1",
        "model_id": "model-1",
        "accepted_route": AcceptedModelRoute(
            decision_id="decision-1",
            run_id="run-1",
            node_id="node-1",
            attempt_id="attempt-1",
            fencing_generation=2,
            budget_reservation_id="reservation-1",
            model_id="model-1",
            provider_id="primary",
            registry_manifest_hash=HASH,
        ),
        "messages": (ModelMessage(role="user", content="hello"),),
        "max_output_tokens": 512,
        "reasoning_effort": "medium",
        "timeout_ms": 30_000,
        "cost_snapshots": CostSnapshotRefs(
            registry_manifest_hash=HASH,
            tokenizer_snapshot_id=HASH,
            fx_snapshot_id=HASH,
            price_snapshot_id=HASH,
            estimator_snapshot_id=HASH,
        ),
    }
    values.update(updates)
    return ModelRequest(**values)


def test_model_request_binds_attempt_route_limits_and_cost_snapshots():
    call = request()

    assert call.accepted_routing_decision_id == "decision-1"
    assert call.fencing_generation == 2
    assert call.cost_snapshots.fx_snapshot_id == HASH
    with pytest.raises(ValidationError):
        request(api_key="must-never-enter-the-gateway-contract")
    with pytest.raises(ValidationError, match="tool messages require"):
        ModelMessage(role="tool", content="result")


def test_gateway_preflight_matches_registry_provider_model_and_generation():
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
        capabilities={"text"},
        context_window=4_096,
        max_output_tokens=1_024,
        supported_reasoning_efforts={"low", "medium"},
        price=None,
        local_zero_cost=True,
    )
    registry = ModelRegistryManifest(providers=(provider,), models=(model,))
    cost_refs = CostSnapshotRefs(
        registry_manifest_hash=registry.content_hash,
        tokenizer_snapshot_id=HASH,
        fx_snapshot_id=HASH,
        price_snapshot_id=registry.content_hash,
        estimator_snapshot_id=HASH,
    )
    route = AcceptedModelRoute(
        decision_id="decision-1",
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=2,
        budget_reservation_id="reservation-1",
        model_id="model-1",
        provider_id="primary",
        registry_manifest_hash=registry.content_hash,
    )
    call = request(model_id="model-1", accepted_route=route, cost_snapshots=cost_refs)

    selected_provider, selected_model = validate_gateway_request(call, registry)

    assert selected_provider.id == "primary"
    assert selected_model.id == "model-1"
    assert resolve_provider_url(
        selected_provider,
        AdapterRequest(relative_path="responses", body_json="{}"),
    ) == "https://api.openai.com/v1/responses"
    with pytest.raises(ValueError, match="output exceeds"):
        validate_gateway_request(call.model_copy(update={"max_output_tokens": 2_000}), registry)


def test_adapter_request_cannot_choose_host_or_authenticate():
    with pytest.raises(ValidationError, match="safe relative endpoint path"):
        AdapterRequest(relative_path="https://evil.example/v1", body_json="{}")
    with pytest.raises(ValidationError, match="safe relative endpoint path"):
        AdapterRequest(relative_path="v1/../admin", body_json="{}")
    with pytest.raises(ValidationError, match="safe relative endpoint path"):
        AdapterRequest(relative_path="responses%2fadmin", body_json="{}")
    with pytest.raises(ValidationError, match="transport-managed header"):
        AdapterRequestHeader(name="Authorization", value="secret")
    with pytest.raises(ValidationError, match="control characters"):
        AdapterRequestHeader(name="x-request-tag", value="ok\r\nAuthorization: secret")
    with pytest.raises(ValidationError, match="control characters"):
        AdapterRequestHeader(name="x-request-tag", value="bad\x00value")

    encoded = AdapterRequest(
        relative_path="responses",
        headers=(AdapterRequestHeader(name="content-type", value="application/json"),),
        body_json='{"model":"remote-model"}',
    )
    assert encoded.method == "POST"


def test_usage_contract_distinguishes_missing_from_zero_and_requires_base_counts():
    assert TokenUsage(status="reported", input_tokens=0, output_tokens=0).input_tokens == 0
    assert TokenUsage(status="unavailable").input_tokens is None
    with pytest.raises(ValidationError, match="requires input_tokens and output_tokens"):
        TokenUsage(status="reported", input_tokens=4)
    with pytest.raises(ValidationError, match="must not imply zero or partial"):
        TokenUsage(status="unavailable", input_tokens=0)


def test_gateway_failures_forbid_automatic_retry_after_unknown_outcome():
    with pytest.raises(ValidationError, match="successful or unknown outcome cannot be marked retryable"):
        ModelGatewayFailure(
            code="timeout",
            phase="transport",
            outcome="unknown",
            retryable=True,
        )
    with pytest.raises(ValidationError, match="only valid for rate_limited"):
        ModelGatewayFailure(
            code="provider_unavailable",
            phase="provider",
            outcome="known_failure",
            retryable=True,
            retry_after_ms=100,
        )

    failure = ModelGatewayFailure(
        code="rate_limited",
        phase="provider",
        outcome="known_failure",
        retryable=True,
        retry_after_ms=250,
        http_status=429,
    )
    error = ModelGatewayError(failure)
    assert error.failure is failure
    assert "429" not in str(error)


def test_tool_calls_are_untrusted_proposals_and_must_match_finish_reason():
    call = ModelToolCall(id="tool-call-1", name="read_file", arguments_json='{"path":"a.py"}')
    response = ModelResponse(
        request_id="req-1",
        model_id="model-1",
        output_text="",
        tool_calls=(call,),
        finish_reason="tool_calls",
        usage=TokenUsage(status="reported", input_tokens=10, output_tokens=4),
    )
    assert response.tool_calls[0].name == "read_file"
    request_with_tool = request(
        tools=(
            {
                "name": "read_file",
                "description": "Read a workspace file",
                "input_schema_json": '{"type":"object"}',
            },
        )
    )
    assert validate_gateway_response(request_with_tool, response) is response
    with pytest.raises(ValueError, match="not offered"):
        validate_gateway_response(request(), response)
    with pytest.raises(ValueError, match="request_id"):
        validate_gateway_response(request_with_tool, response.model_copy(update={"request_id": "other"}))
    with pytest.raises(ValueError, match="model_id"):
        validate_gateway_response(request_with_tool, response.model_copy(update={"model_id": "other"}))
    with pytest.raises(ValidationError, match="requires at least one tool call"):
        ModelResponse(
            request_id="req-1",
            model_id="model-1",
            finish_reason="tool_calls",
            usage=TokenUsage(status="unavailable"),
        )
