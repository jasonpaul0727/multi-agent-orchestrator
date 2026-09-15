"""Validated event boundary objects."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import math
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator


def _validate_json_value(value: Any, *, path: str = "payload") -> None:
    """Reject values that cannot be represented deterministically as JSON."""

    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} has a non-string key")
            _validate_json_value(item, path=f"{path}.{key}")
        return
    raise ValueError(f"{path} contains a non-JSON value of type {type(value).__name__}")


def _validate_non_blank(value: Any, field_name: str) -> Any:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    return value


class EventDraft(BaseModel):
    """An event before its durable stream identity is assigned."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: StrictStr = Field(min_length=1)
    payload: dict[str, Any]

    def __init__(self, *args: Any, **data: Any) -> None:
        # The small positional form keeps the append API pleasant while the
        # underlying Pydantic validation remains the single source of truth.
        if args:
            if len(args) > 2:
                raise TypeError("EventDraft accepts at most event_type and payload positionally")
            for field_name, value in zip(("event_type", "payload"), args):
                if field_name in data:
                    raise TypeError(f"{field_name} was provided both positionally and by keyword")
                data[field_name] = value
        super().__init__(**data)

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_event_type(cls, value: Any) -> Any:
        return _validate_non_blank(value, "event_type")

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            raise ValueError("payload must be a mapping")
        payload = dict(value)
        _validate_json_value(payload)
        return payload


class StoredEvent(BaseModel):
    """An immutable event read from the append-only event store."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: StrictStr = Field(min_length=1)
    stream_type: StrictStr = Field(min_length=1)
    stream_id: StrictStr = Field(min_length=1)
    stream_version: StrictInt = Field(gt=0)
    event_type: StrictStr = Field(min_length=1)
    schema_version: StrictInt = Field(gt=0)
    occurred_at: datetime
    payload: dict[str, Any]
    payload_hash: StrictStr = Field(min_length=1)
    idempotency_key: StrictStr = Field(min_length=1)
    correlation_id: StrictStr | None = None
    causation_id: StrictStr | None = None

    @field_validator("event_id", "stream_type", "stream_id", "idempotency_key", mode="before")
    @classmethod
    def validate_identifiers(cls, value: Any) -> Any:
        return _validate_non_blank(value, "identifier")

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_event_type(cls, value: Any) -> Any:
        return _validate_non_blank(value, "event_type")

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            raise ValueError("payload must be a mapping")
        payload = dict(value)
        _validate_json_value(payload)
        return payload
