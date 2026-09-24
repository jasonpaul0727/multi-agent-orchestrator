"""Provider-neutral model Gateway and adapter boundary contracts.

This module intentionally defines contracts only. Provider codecs may translate
requests and responses, but transport, endpoint enforcement, credentials,
policy, budget reservation, retries, and event persistence remain Gateway duties.
"""

from __future__ import annotations

import json
import re
from typing import Literal, Protocol
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.config.models import (
    ModelRegistryManifest,
    ModelSpec,
    ProviderAdapter,
    ProviderSpec,
    ReasoningEffort,
)
from orchestrator.validation import revalidate_model


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_CONTENT_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_ADAPTER_PATH = re.compile(r"^[A-Za-z0-9._~/-]+$")
_FORBIDDEN_ADAPTER_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "host",
        "content-length",
        "x-api-key",
        "api-key",
    }
)
_AUTH_HEADER_MARKERS = ("auth", "api-key", "token", "cookie")


class _GatewayModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class ModelMessage(_GatewayModel):
    role: Literal["system", "developer", "user", "assistant", "tool"]
    content: StrictStr = Field(max_length=1_000_000)
    name: StrictStr | None = None
    tool_call_id: StrictStr | None = None

    @model_validator(mode="after")
    def validate_tool_message(self) -> "ModelMessage":
        if self.role == "tool":
            if not self.name or not self.tool_call_id:
                raise ValueError("tool messages require name and tool_call_id")
        elif self.tool_call_id is not None:
            raise ValueError("tool_call_id is only valid on tool messages")
        return self


class ModelToolDefinition(_GatewayModel):
    name: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    description: StrictStr = Field(min_length=1, max_length=2_000)
    input_schema_json: StrictStr = Field(min_length=2, max_length=128_000)

    @field_validator("input_schema_json")
    @classmethod
    def validate_schema_json(cls, value: str) -> str:
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("tool input schema must be JSON") from exc
        if not isinstance(parsed, dict) or parsed.get("type") != "object":
            raise ValueError("tool input schema must declare an object")
        return value


class CostSnapshotRefs(_GatewayModel):
    registry_manifest_hash: StrictStr
    tokenizer_snapshot_id: StrictStr
    fx_snapshot_id: StrictStr
    price_snapshot_id: StrictStr
    estimator_snapshot_id: StrictStr

    @field_validator(
        "registry_manifest_hash",
        "tokenizer_snapshot_id",
        "fx_snapshot_id",
        "price_snapshot_id",
        "estimator_snapshot_id",
    )
    @classmethod
    def validate_snapshot_hash(cls, value: str) -> str:
        if not _CONTENT_HASH.fullmatch(value):
            raise ValueError("snapshot reference must be a sha256 content hash")
        return value


class AcceptedModelRoute(_GatewayModel):
    """Durable accepted routing fact binding one model call to one attempt."""

    decision_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    fencing_generation: StrictInt = Field(ge=0)
    budget_reservation_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    provider_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    reasoning_effort: ReasoningEffort
    registry_manifest_hash: StrictStr

    @field_validator("registry_manifest_hash")
    @classmethod
    def validate_registry_hash(cls, value: str) -> str:
        if not _CONTENT_HASH.fullmatch(value):
            raise ValueError("registry_manifest_hash must be a sha256 content hash")
        return value


class ModelRequest(_GatewayModel):
    """Attempt-scoped call envelope; credentials and endpoints are not caller data."""

    request_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    idempotency_key: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    run_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    node_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    attempt_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    fencing_generation: StrictInt = Field(ge=0)
    budget_reservation_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    accepted_route: AcceptedModelRoute
    messages: tuple[ModelMessage, ...] = Field(min_length=1, max_length=10_000)
    tools: tuple[ModelToolDefinition, ...] = Field(default=(), max_length=256)
    max_output_tokens: StrictInt = Field(gt=0)
    reasoning_effort: ReasoningEffort | None = None
    timeout_ms: StrictInt = Field(gt=0, le=3_600_000)
    cost_snapshots: CostSnapshotRefs

    @field_validator("messages", "tools", mode="before")
    @classmethod
    def normalize_sequences(cls, value: object, info: object) -> tuple[object, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"{getattr(info, 'field_name', 'value')} must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def unique_tools(self) -> "ModelRequest":
        tool_names = [tool.name for tool in self.tools]
        if len(tool_names) != len(set(tool_names)):
            raise ValueError("tool names must be unique in one model request")
        route = self.accepted_route
        if (
            route.run_id != self.run_id
            or route.node_id != self.node_id
            or route.attempt_id != self.attempt_id
            or route.fencing_generation != self.fencing_generation
            or route.budget_reservation_id != self.budget_reservation_id
            or route.model_id != self.model_id
            or route.reasoning_effort != self.reasoning_effort
        ):
            raise ValueError("model request does not match its accepted route scope or effort")
        if route.registry_manifest_hash != self.cost_snapshots.registry_manifest_hash:
            raise ValueError("accepted route and request must use the same registry snapshot")
        if self.cost_snapshots.price_snapshot_id != route.registry_manifest_hash:
            raise ValueError("price snapshot must match the accepted registry snapshot")
        return self

    @property
    def accepted_routing_decision_id(self) -> str:
        return self.accepted_route.decision_id


class TokenUsage(_GatewayModel):
    """Provider-reported or estimator-derived usage; missing is never zero."""

    status: Literal["reported", "estimated", "unavailable"]
    input_tokens: StrictInt | None = Field(default=None, ge=0)
    output_tokens: StrictInt | None = Field(default=None, ge=0)
    reasoning_tokens: StrictInt | None = Field(default=None, ge=0)
    cached_input_tokens: StrictInt | None = Field(default=None, ge=0)
    provider_fee_minor: StrictInt | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_usage_status(self) -> "TokenUsage":
        counts = (
            self.input_tokens,
            self.output_tokens,
            self.reasoning_tokens,
            self.cached_input_tokens,
            self.provider_fee_minor,
        )
        if self.status in {"reported", "estimated"}:
            if self.input_tokens is None or self.output_tokens is None:
                raise ValueError("reported usage requires input_tokens and output_tokens")
        elif any(value is not None for value in counts):
            raise ValueError("unavailable usage must not imply zero or partial counts")
        return self


class ModelToolCall(_GatewayModel):
    id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    name: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    arguments_json: StrictStr = Field(min_length=2, max_length=1_000_000)

    @field_validator("arguments_json")
    @classmethod
    def validate_arguments(cls, value: str) -> str:
        try:
            arguments = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("tool arguments must be JSON") from exc
        if not isinstance(arguments, dict):
            raise ValueError("tool arguments must be a JSON object")
        return value


class ModelResponse(_GatewayModel):
    request_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    model_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    provider_request_id: StrictStr | None = Field(default=None, max_length=256)
    output_text: StrictStr = Field(default="", max_length=8_000_000)
    tool_calls: tuple[ModelToolCall, ...] = Field(default=(), max_length=256)
    finish_reason: Literal["stop", "tool_calls", "length", "content_filter"]
    usage: TokenUsage

    @field_validator("tool_calls", mode="before")
    @classmethod
    def normalize_tool_calls(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("tool_calls must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def validate_finish_reason(self) -> "ModelResponse":
        if self.finish_reason == "tool_calls" and not self.tool_calls:
            raise ValueError("tool_calls finish reason requires at least one tool call")
        if self.finish_reason != "tool_calls" and self.tool_calls:
            raise ValueError("tool calls require the tool_calls finish reason")
        return self


GatewayFailureCode = Literal[
    "invalid_request",
    "policy_denied",
    "budget_blocked",
    "stale_decision",
    "credential_unavailable",
    "credential_delivery_unsupported",
    "authentication_failed",
    "provider_unavailable",
    "rate_limited",
    "context_length_exceeded",
    "output_limit_exceeded",
    "invalid_response",
    "usage_unavailable",
    "timeout",
    "cancelled",
    "transport_error",
    "outcome_unknown",
    "idempotency_conflict",
]


class ModelGatewayFailure(_GatewayModel):
    """Sanitized, stable failure metadata; intentionally has no raw message/body."""

    code: GatewayFailureCode
    phase: Literal["preflight", "transport", "provider", "decode", "settlement"]
    outcome: Literal["not_sent", "known_failure", "known_success", "unknown"]
    retryable: StrictBool
    http_status: StrictInt | None = Field(default=None, ge=100, le=599)
    provider_code: StrictStr | None = Field(default=None, max_length=128, pattern=_IDENTIFIER.pattern)
    provider_request_id: StrictStr | None = Field(default=None, max_length=256)
    retry_after_ms: StrictInt | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def retry_safety(self) -> "ModelGatewayFailure":
        if self.outcome in {"known_success", "unknown"} and self.retryable:
            raise ValueError("a successful or unknown outcome cannot be marked retryable")
        if self.retry_after_ms is not None and self.code != "rate_limited":
            raise ValueError("retry_after_ms is only valid for rate_limited failures")
        return self


class ModelGatewayError(RuntimeError):
    """Exception wrapper for the sanitized provider-neutral failure object."""

    def __init__(self, failure: ModelGatewayFailure) -> None:
        if not isinstance(failure, ModelGatewayFailure):
            raise TypeError("failure must be a ModelGatewayFailure")
        self.failure = failure
        super().__init__(f"model gateway failed: {failure.code}")


class AdapterRequestHeader(_GatewayModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    value: StrictStr = Field(max_length=4_096)

    @field_validator("name")
    @classmethod
    def safe_header_name(cls, value: str) -> str:
        normalized = value.lower()
        if (
            not re.fullmatch(r"[a-z0-9-]+", normalized)
            or normalized in _FORBIDDEN_ADAPTER_HEADERS
            or any(marker in normalized for marker in _AUTH_HEADER_MARKERS)
        ):
            raise ValueError("adapter cannot set a transport-managed header")
        return normalized

    @field_validator("value")
    @classmethod
    def safe_header_value(cls, value: str) -> str:
        if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
            raise ValueError("header value cannot contain control characters")
        return value


class AdapterRequest(_GatewayModel):
    """Provider-specific request encoding relative to the registered endpoint base."""

    method: Literal["POST"] = "POST"
    relative_path: StrictStr = Field(min_length=2, max_length=2_048)
    headers: tuple[AdapterRequestHeader, ...] = ()
    body_json: StrictStr = Field(min_length=2, max_length=8_000_000)

    @field_validator("relative_path")
    @classmethod
    def local_path_only(cls, value: str) -> str:
        if (
            not _ADAPTER_PATH.fullmatch(value)
            or value.startswith("/")
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise ValueError("adapter path must be a safe relative endpoint path")
        return value

    @field_validator("headers", mode="before")
    @classmethod
    def normalize_headers(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("headers must be an array")
        return tuple(value)

    @field_validator("body_json")
    @classmethod
    def validate_body(cls, value: str) -> str:
        try:
            body = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("adapter body must be JSON") from exc
        if not isinstance(body, dict):
            raise ValueError("adapter body must be a JSON object")
        return value


class AdapterResponse(_GatewayModel):
    """Bounded provider HTTP response handed to an adapter decoder."""

    http_status: StrictInt = Field(ge=100, le=599)
    body_json: StrictStr = Field(min_length=2, max_length=8_000_000)
    provider_request_id: StrictStr | None = Field(default=None, max_length=256)

    @field_validator("body_json")
    @classmethod
    def validate_body(cls, value: str) -> str:
        try:
            json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("provider response body must be JSON") from exc
        return value


class ModelAdapter(Protocol):
    """Pure provider codec. It does not own network or secret access."""

    adapter: ProviderAdapter

    def encode_request(self, request: ModelRequest, model: ModelSpec) -> AdapterRequest: ...

    def decode_response(
        self,
        response: AdapterResponse,
        *,
        request_id: str,
        model_id: str,
    ) -> ModelResponse: ...


class CancellationSignal(Protocol):
    @property
    def cancelled(self) -> bool: ...


class ModelGateway(Protocol):
    """The single invocation boundary; implementations enforce control-plane checks."""

    async def invoke(
        self,
        request: ModelRequest,
        *,
        cancellation: CancellationSignal | None = None,
    ) -> ModelResponse: ...


def validate_gateway_request(
    request: ModelRequest,
    registry: ModelRegistryManifest,
) -> tuple[ProviderSpec, ModelSpec]:
    """Validate a selected model and its call limits against a frozen registry."""

    request = revalidate_model(ModelRequest, request)
    registry = revalidate_model(ModelRegistryManifest, registry)
    route = request.accepted_route
    if route.registry_manifest_hash != registry.content_hash:
        raise ValueError("accepted route does not reference the supplied registry snapshot")
    model = next((item for item in registry.models if item.id == route.model_id), None)
    if model is None:
        raise ValueError("accepted route references an unregistered model")
    provider = next((item for item in registry.providers if item.id == route.provider_id), None)
    if provider is None or model.provider != provider.id:
        raise ValueError("accepted route provider does not match the selected model")
    if not provider.enabled:
        raise ValueError("selected provider is disabled")
    if not model.local_zero_cost and model.price is None:
        raise ValueError("selected paid model has no frozen price")
    if request.max_output_tokens > min(model.max_output_tokens, model.context_window):
        raise ValueError("requested output exceeds the registered model limit")
    if (
        request.reasoning_effort is not None
        and request.reasoning_effort not in model.supported_reasoning_efforts
    ):
        raise ValueError("reasoning effort is not supported by the selected model")
    return provider, model


def validate_gateway_response(request: ModelRequest, response: ModelResponse) -> ModelResponse:
    """Bind a decoded provider response to the exact request and offered tools."""

    request = revalidate_model(ModelRequest, request)
    response = revalidate_model(ModelResponse, response)
    if response.request_id != request.request_id:
        raise ValueError("gateway response request_id does not match the request")
    if response.model_id != request.model_id:
        raise ValueError("gateway response model_id does not match the accepted route")
    offered_tools = {tool.name for tool in request.tools}
    if any(call.name not in offered_tools for call in response.tool_calls):
        raise ValueError("gateway response proposed a tool that was not offered")
    return response


def resolve_provider_url(provider: ProviderSpec, request: AdapterRequest) -> str:
    """Join an adapter-owned relative path to the endpoint fixed by the manifest."""

    provider = revalidate_model(ProviderSpec, provider)
    request = revalidate_model(AdapterRequest, request)
    endpoint = provider.effective_endpoint
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("provider endpoint is not a registered HTTPS origin")
    return f"{endpoint.rstrip('/')}/{request.relative_path}"


__all__ = [
    "AdapterRequest",
    "AdapterRequestHeader",
    "AdapterResponse",
    "AcceptedModelRoute",
    "CancellationSignal",
    "CostSnapshotRefs",
    "GatewayFailureCode",
    "ModelAdapter",
    "ModelGateway",
    "ModelGatewayError",
    "ModelGatewayFailure",
    "ModelMessage",
    "ModelRequest",
    "ModelResponse",
    "ModelToolCall",
    "ModelToolDefinition",
    "TokenUsage",
    "resolve_provider_url",
    "validate_gateway_request",
    "validate_gateway_response",
]
