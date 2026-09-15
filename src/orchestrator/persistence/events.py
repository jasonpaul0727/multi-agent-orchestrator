"""Validated event boundary objects."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import math
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationInfo,
    field_validator,
    model_validator,
)


def _validate_json_value(value: Any, *, path: str = "payload") -> None:
    """Reject values that cannot be represented deterministically as JSON."""

    if isinstance(value, str):
        if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
            raise ValueError(f"{path} contains a lone surrogate character")
        return
    if value is None or isinstance(value, (bool, int)):
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
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    return value


class _FrozenDict(dict[str, Any]):
    """A dict-compatible JSON object that rejects all mutation methods."""

    @staticmethod
    def _immutable(*args: Any, **kwargs: Any) -> None:
        raise TypeError("event payload is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _FrozenList(list[Any]):
    """A list-compatible JSON array that rejects all mutation methods."""

    @staticmethod
    def _immutable(*args: Any, **kwargs: Any) -> None:
        raise TypeError("event payload is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


def _freeze_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict((key, _freeze_json_value(item)) for key, item in value.items())
    if isinstance(value, list):
        return _FrozenList(_freeze_json_value(item) for item in value)
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
    def validate_identifiers(cls, value: Any, info: ValidationInfo) -> Any:
        return _validate_non_blank(value, info.field_name)

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

    @field_validator("correlation_id", "causation_id", mode="before")
    @classmethod
    def validate_optional_identifiers(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return value
        return _validate_non_blank(value, info.field_name)

    @model_validator(mode="after")
    def freeze_payload(self) -> "StoredEvent":
        # This also detaches the stored event from any mutable EventDraft
        # payload object supplied to append().
        object.__setattr__(self, "payload", _freeze_json_value(self.payload))
        return self
