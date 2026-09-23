"""Trusted-boundary revalidation helpers for immutable Pydantic contracts."""

from __future__ import annotations

from pydantic import BaseModel


def revalidate_model(model_type: type[BaseModel], value: BaseModel) -> BaseModel:
    """Recursively rebuild an immutable contract, catching unchecked model_copy updates."""

    if not isinstance(value, model_type):
        raise TypeError(f"expected {model_type.__name__}")
    fields = {
        name: _revalidate_value(getattr(value, name))
        for name in model_type.model_fields
    }
    return model_type.model_validate(fields)


def _revalidate_value(value: object) -> object:
    if isinstance(value, BaseModel):
        return revalidate_model(type(value), value)
    if isinstance(value, tuple):
        return tuple(_revalidate_value(item) for item in value)
    if isinstance(value, list):
        return [_revalidate_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _revalidate_value(item) for key, item in value.items()}
    if isinstance(value, frozenset):
        return frozenset(_revalidate_value(item) for item in value)
    if isinstance(value, set):
        return {_revalidate_value(item) for item in value}
    return value


__all__ = ["revalidate_model"]
