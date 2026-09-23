"""Provider protocol codecs for the supported model APIs.

The codecs are pure: they never read credentials, choose endpoints, or perform
network I/O. All request/response payloads pass through Gateway contracts.
"""

from __future__ import annotations

import json
from typing import Any

from orchestrator.config.models import ModelSpec, ProviderAdapter
from orchestrator.models.gateway import (
    AdapterRequest,
    AdapterRequestHeader,
    AdapterResponse,
    ModelRequest,
    ModelResponse,
    ModelToolCall,
    TokenUsage,
)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _object(value: str) -> dict[str, Any]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("provider response must be a JSON object")
    return parsed


def _tool_definitions(request: ModelRequest) -> list[dict[str, object]]:
    return [
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": json.loads(tool.input_schema_json),
        }
        for tool in request.tools
    ]


def _usage(
    value: object,
    *,
    input_key: str,
    output_key: str,
    reasoning_path: tuple[str, ...] = (),
    cached_path: tuple[str, ...] = (),
) -> TokenUsage:
    if not isinstance(value, dict):
        return TokenUsage(status="unavailable")
    input_tokens = value.get(input_key)
    output_tokens = value.get(output_key)
    if (
        isinstance(input_tokens, bool)
        or not isinstance(input_tokens, int)
        or input_tokens < 0
        or isinstance(output_tokens, bool)
        or not isinstance(output_tokens, int)
        or output_tokens < 0
    ):
        return TokenUsage(status="unavailable")
    reasoning = _nested_int(value, reasoning_path)
    cached = _nested_int(value, cached_path)
    return TokenUsage(
        status="reported",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        reasoning_tokens=reasoning,
        cached_input_tokens=cached,
    )


def _nested_int(value: dict[str, Any], path: tuple[str, ...]) -> int | None:
    cursor: object = value
    for key in path:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor if isinstance(cursor, int) and not isinstance(cursor, bool) and cursor >= 0 else None


def _header(name: str, value: str) -> AdapterRequestHeader:
    return AdapterRequestHeader(name=name, value=value)


class OpenAIResponsesAdapter:
    adapter: ProviderAdapter = "openai_responses"

    def encode_request(self, request: ModelRequest, model: ModelSpec) -> AdapterRequest:
        instructions = [item.content for item in request.messages if item.role in {"system", "developer"}]
        input_items: list[dict[str, object]] = []
        for item in request.messages:
            if item.role in {"system", "developer"}:
                continue
            if item.role == "tool":
                input_items.append(
                    {
                        "type": "function_call_output",
                        "call_id": item.tool_call_id,
                        "output": item.content,
                    }
                )
            else:
                input_items.append({"role": item.role, "content": item.content})
        tools = [
            {
                "type": "function",
                "name": item.name,
                "description": item.description,
                "parameters": json.loads(item.input_schema_json),
            }
            for item in request.tools
        ]
        body: dict[str, object] = {
            "model": model.remote_model,
            "input": input_items,
            "max_output_tokens": request.max_output_tokens,
            "store": False,
        }
        if instructions:
            body["instructions"] = "\n\n".join(instructions)
        if tools:
            body["tools"] = tools
        if request.reasoning_effort not in (None, "none"):
            body["reasoning"] = {"effort": request.reasoning_effort}
        return AdapterRequest(
            relative_path="responses",
            headers=(_header("accept", "application/json"),),
            body_json=_json(body),
        )

    def decode_response(self, response: AdapterResponse, *, request_id: str, model_id: str) -> ModelResponse:
        body = _object(response.body_json)
        status = body.get("status")
        if status == "failed":
            raise ValueError("provider returned a failed response")
        output = body.get("output")
        if not isinstance(output, list):
            raise ValueError("response output items are missing")
        texts: list[str] = []
        calls: list[ModelToolCall] = []
        refusal = False
        for item in output:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "message" and isinstance(item.get("content"), list):
                for block in item["content"]:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "output_text" and isinstance(block.get("text"), str):
                        texts.append(block["text"])
                    elif block.get("type") == "refusal":
                        refusal = True
            elif item.get("type") == "function_call":
                call_id = item.get("call_id") or item.get("id")
                name = item.get("name")
                arguments = item.get("arguments")
                if not isinstance(call_id, str) or not isinstance(name, str) or not isinstance(arguments, str):
                    raise ValueError("function call item is malformed")
                calls.append(ModelToolCall(id=call_id, name=name, arguments_json=arguments))
        details = body.get("incomplete_details")
        reason = details.get("reason") if isinstance(details, dict) else None
        finish = (
            "tool_calls" if calls
            else "content_filter" if refusal
            else "length" if status == "incomplete" or reason == "max_output_tokens"
            else "stop"
        )
        return ModelResponse(
            request_id=request_id,
            model_id=model_id,
            provider_request_id=response.provider_request_id or _optional_string(body.get("id")),
            output_text="".join(texts),
            tool_calls=tuple(calls),
            finish_reason=finish,
            usage=_usage(
                body.get("usage"),
                input_key="input_tokens",
                output_key="output_tokens",
                reasoning_path=("output_tokens_details", "reasoning_tokens"),
                cached_path=("input_tokens_details", "cached_tokens"),
            ),
        )


class AnthropicMessagesAdapter:
    adapter: ProviderAdapter = "anthropic_messages"
    _EFFORTS = {"low", "medium", "high"}

    def encode_request(self, request: ModelRequest, model: ModelSpec) -> AdapterRequest:
        system = [item.content for item in request.messages if item.role in {"system", "developer"}]
        messages: list[dict[str, object]] = []
        for item in request.messages:
            if item.role in {"system", "developer"}:
                continue
            if item.role == "tool":
                block = {
                    "type": "tool_result",
                    "tool_use_id": item.tool_call_id,
                    "content": item.content,
                }
                if messages and messages[-1].get("role") == "user" and isinstance(messages[-1].get("content"), list):
                    messages[-1]["content"].append(block)  # type: ignore[union-attr]
                else:
                    messages.append({"role": "user", "content": [block]})
            elif item.role in {"user", "assistant"}:
                messages.append({"role": item.role, "content": item.content})
            else:
                raise ValueError("unsupported message role for Anthropic Messages")
        body: dict[str, object] = {
            "model": model.remote_model,
            "max_tokens": request.max_output_tokens,
            "messages": messages,
        }
        if system:
            body["system"] = "\n\n".join(system)
        if request.tools:
            body["tools"] = _tool_definitions(request)
        effort = request.reasoning_effort
        if effort not in (None, "none"):
            if effort not in self._EFFORTS:
                raise ValueError("requested reasoning effort is not supported by the Anthropic codec")
            body["thinking"] = {"type": "adaptive"}
            body["output_config"] = {"effort": effort}
        return AdapterRequest(
            relative_path="messages",
            headers=(
                _header("accept", "application/json"),
                _header("anthropic-version", "2023-06-01"),
            ),
            body_json=_json(body),
        )

    def decode_response(self, response: AdapterResponse, *, request_id: str, model_id: str) -> ModelResponse:
        body = _object(response.body_json)
        content = body.get("content")
        if not isinstance(content, list):
            raise ValueError("Anthropic content blocks are missing")
        texts: list[str] = []
        calls: list[ModelToolCall] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and isinstance(block.get("text"), str):
                texts.append(block["text"])
            elif block.get("type") == "tool_use":
                call_id = block.get("id")
                name = block.get("name")
                arguments = block.get("input")
                if not isinstance(call_id, str) or not isinstance(name, str) or not isinstance(arguments, dict):
                    raise ValueError("Anthropic tool-use block is malformed")
                calls.append(ModelToolCall(id=call_id, name=name, arguments_json=_json(arguments)))
        stop = body.get("stop_reason")
        finish = (
            "tool_calls" if calls or stop == "tool_use"
            else "length" if stop == "max_tokens"
            else "content_filter" if stop == "refusal"
            else "stop"
        )
        usage = body.get("usage")
        return ModelResponse(
            request_id=request_id,
            model_id=model_id,
            provider_request_id=response.provider_request_id or _optional_string(body.get("id")),
            output_text="".join(texts),
            tool_calls=tuple(calls),
            finish_reason=finish,
            usage=_usage(
                usage,
                input_key="input_tokens",
                output_key="output_tokens",
                cached_path=("cache_read_input_tokens",),
            ),
        )


class OpenAICompatibleAdapter(OpenAIResponsesAdapter):
    """OpenAI Chat Completions compatible codec (not the Responses API)."""

    adapter: ProviderAdapter = "openai_compatible"

    def encode_request(self, request: ModelRequest, model: ModelSpec) -> AdapterRequest:
        messages = [
            {
                "role": "tool" if item.role == "tool" else item.role,
                "content": item.content,
                **({"tool_call_id": item.tool_call_id, "name": item.name} if item.role == "tool" else {}),
            }
            for item in request.messages
        ]
        body: dict[str, object] = {
            "model": model.remote_model,
            "messages": messages,
            "max_tokens": request.max_output_tokens,
        }
        if request.tools:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": json.loads(tool.input_schema_json),
                    },
                }
                for tool in request.tools
            ]
        return AdapterRequest(
            relative_path="chat/completions",
            headers=(_header("accept", "application/json"),),
            body_json=_json(body),
        )

    def decode_response(self, response: AdapterResponse, *, request_id: str, model_id: str) -> ModelResponse:
        body = _object(response.body_json)
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise ValueError("compatible response choices are missing")
        choice = choices[0]
        message = choice.get("message")
        if not isinstance(message, dict):
            raise ValueError("compatible response message is missing")
        text = message.get("content")
        if text is None:
            text = ""
        if not isinstance(text, str):
            raise ValueError("compatible response content is not plain text")
        tool_calls: list[ModelToolCall] = []
        raw_calls = message.get("tool_calls", [])
        if not isinstance(raw_calls, list):
            raise ValueError("compatible response tool_calls is malformed")
        for raw in raw_calls:
            if not isinstance(raw, dict):
                raise ValueError("compatible tool call is malformed")
            function = raw.get("function")
            if not isinstance(function, dict):
                raise ValueError("compatible tool function is malformed")
            call_id, name, arguments = raw.get("id"), function.get("name"), function.get("arguments")
            if not isinstance(call_id, str) or not isinstance(name, str) or not isinstance(arguments, str):
                raise ValueError("compatible tool function is incomplete")
            tool_calls.append(ModelToolCall(id=call_id, name=name, arguments_json=arguments))
        finish_value = choice.get("finish_reason")
        finish = (
            "tool_calls" if tool_calls or finish_value == "tool_calls"
            else "length" if finish_value == "length"
            else "content_filter" if finish_value == "content_filter"
            else "stop"
        )
        return ModelResponse(
            request_id=request_id,
            model_id=model_id,
            provider_request_id=response.provider_request_id or _optional_string(body.get("id")),
            output_text=text,
            tool_calls=tuple(tool_calls),
            finish_reason=finish,
            usage=_usage(
                body.get("usage"),
                input_key="prompt_tokens",
                output_key="completion_tokens",
                reasoning_path=("completion_tokens_details", "reasoning_tokens"),
                cached_path=("prompt_tokens_details", "cached_tokens"),
            ),
        )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def adapter_for(name: ProviderAdapter) -> OpenAIResponsesAdapter | AnthropicMessagesAdapter | OpenAICompatibleAdapter:
    return {
        "openai_responses": OpenAIResponsesAdapter,
        "anthropic_messages": AnthropicMessagesAdapter,
        "openai_compatible": OpenAICompatibleAdapter,
    }[name]()


__all__ = [
    "AnthropicMessagesAdapter",
    "OpenAICompatibleAdapter",
    "OpenAIResponsesAdapter",
    "adapter_for",
]
