"""Strict, immutable configuration and model-registry boundary objects.

These objects intentionally contain secret references, never secret values.
The registry manifest is content-addressable so a Run can freeze exactly the
provider/model/price data used for routing and later replay.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_serializer,
    field_validator,
    model_validator,
)


Tier = Literal["economy", "standard", "high"]
ProviderAdapter = Literal[
    "openai_responses", "anthropic_messages", "openai_compatible"
]
ReasoningEffort = Literal[
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"
]

_CURRENCY = re.compile(r"^[A-Z]{3}$")
_ENV_SECRET = re.compile(r"^env:[A-Za-z_][A-Za-z0-9_]*$")
_SECRET_REF = re.compile(r"^(?:keyring|plugin):[^\s=]+$")
_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")


class _ConfigModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        hide_input_in_errors=True,
    )


def _currency(value: str) -> str:
    normalized = value.upper()
    if not _CURRENCY.fullmatch(normalized):
        raise ValueError("currency must be a three-letter ISO currency code")
    return normalized


def _aware_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("timestamp must be an ISO-8601 datetime") from error
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    if value.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return value


def _nonempty_identifiers(value: frozenset[str] | None, field_name: str) -> None:
    if value is not None and any(
        item != item.strip() or not _REFERENCE.fullmatch(item) for item in value
    ):
        raise ValueError(f"{field_name} must contain stable non-blank identifiers")


def _freeze_collection(value: object) -> frozenset[object]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        raise ValueError("value must be an array of identifiers")
    return frozenset(value)


class PolicyEnvelope(_ConfigModel):
    """Optional constraints contributed by one authority layer.

    None means this layer adds no constraint. Combining multiple envelopes is
    deliberately kept out of this model so callers must use the monotonic
    resolver rather than accidentally applying last-write-wins semantics.
    """

    currency: StrictStr | None = None
    max_cost_minor: StrictInt | None = Field(default=None, ge=0)
    max_total_tokens: StrictInt | None = Field(default=None, ge=0)
    max_agents: StrictInt | None = Field(default=None, ge=0)
    max_depth: StrictInt | None = Field(default=None, ge=0)
    max_concurrency: StrictInt | None = Field(default=None, ge=0)
    allowed_models: frozenset[StrictStr] | None = None
    allowed_providers: frozenset[StrictStr] | None = None
    allowed_capabilities: frozenset[StrictStr] | None = None
    denied_models: frozenset[StrictStr] | None = None
    denied_providers: frozenset[StrictStr] | None = None
    min_tier: Tier | None = None
    require_independent_review: StrictBool | None = None
    require_approval: StrictBool | None = None
    max_parallel_candidates: StrictInt | None = Field(default=None, ge=0)

    @field_validator(
        "allowed_models",
        "allowed_providers",
        "allowed_capabilities",
        "denied_models",
        "denied_providers",
        mode="before",
    )
    @classmethod
    def normalize_sets(cls, value: object) -> frozenset[object] | None:
        return None if value is None else _freeze_collection(value)

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str | None) -> str | None:
        return None if value is None else _currency(value)

    @model_validator(mode="after")
    def validate_sets(self) -> "PolicyEnvelope":
        for name in (
            "allowed_models",
            "allowed_providers",
            "allowed_capabilities",
            "denied_models",
            "denied_providers",
        ):
            _nonempty_identifiers(getattr(self, name), name)
        return self


class ProviderSpec(_ConfigModel):
    id: StrictStr = Field(min_length=1, pattern=_REFERENCE.pattern)
    adapter: ProviderAdapter
    secret_ref: StrictStr = Field(
        min_length=1,
        pattern=r"^(?:env:[A-Za-z_][A-Za-z0-9_]*|(?:keyring|plugin):[^\s=]+)$",
    )
    enabled: StrictBool

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        if value != value.strip() or not _REFERENCE.fullmatch(value):
            raise ValueError("provider id must be a stable non-blank reference")
        return value

    @field_validator("secret_ref")
    @classmethod
    def validate_secret_reference(cls, value: str) -> str:
        if not (_ENV_SECRET.fullmatch(value) or _SECRET_REF.fullmatch(value)):
            raise ValueError(
                "secret_ref must be an env:, keyring:, or plugin: reference; "
                "secret values are not accepted"
            )
        return value


class PriceSpec(_ConfigModel):
    """Frozen price inputs; all amounts are integer minor currency units."""

    currency: StrictStr = Field(pattern=r"^[A-Za-z]{3}$")
    input_minor_per_million: StrictInt = Field(ge=0)
    output_minor_per_million: StrictInt = Field(ge=0)
    max_tool_cost_minor: StrictInt = Field(ge=0)
    estimator_id: StrictStr = Field(min_length=1, pattern=_REFERENCE.pattern)
    effective_from: datetime
    expires_at: datetime

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        return _currency(value)

    @field_validator("effective_from", "expires_at", mode="before")
    @classmethod
    def validate_timestamps(cls, value: datetime | str) -> datetime:
        return _aware_datetime(value)

    @field_validator("estimator_id")
    @classmethod
    def validate_estimator_id(cls, value: str) -> str:
        if value != value.strip() or not _REFERENCE.fullmatch(value):
            raise ValueError("estimator_id must be a stable non-blank reference")
        return value

    @model_validator(mode="after")
    def validate_price_window(self) -> "PriceSpec":
        if self.expires_at <= self.effective_from:
            raise ValueError("expires_at must be later than effective_from")
        return self


class ModelSpec(_ConfigModel):
    id: StrictStr = Field(min_length=1, pattern=_REFERENCE.pattern)
    provider: StrictStr = Field(min_length=1, pattern=_REFERENCE.pattern)
    remote_model: StrictStr = Field(min_length=1, pattern=_REFERENCE.pattern)
    tier: Tier
    capabilities: frozenset[StrictStr] = Field(min_length=1)
    context_window: StrictInt = Field(gt=0)
    max_output_tokens: StrictInt = Field(gt=0)
    supported_reasoning_efforts: frozenset[ReasoningEffort] = Field(min_length=1)
    price: PriceSpec | None = None
    local_zero_cost: StrictBool = False

    @field_validator("capabilities", "supported_reasoning_efforts", mode="before")
    @classmethod
    def normalize_sets(cls, value: object) -> frozenset[object]:
        return _freeze_collection(value)

    @field_validator("id", "provider", "remote_model")
    @classmethod
    def validate_references(cls, value: str, info: object) -> str:
        if value != value.strip() or not _REFERENCE.fullmatch(value):
            raise ValueError(f"{getattr(info, 'field_name', 'reference')} must be stable")
        return value

    @field_validator("capabilities")
    @classmethod
    def validate_capabilities(cls, value: frozenset[str]) -> frozenset[str]:
        _nonempty_identifiers(value, "capabilities")
        return value

    @field_serializer("capabilities", "supported_reasoning_efforts")
    def serialize_sets(self, value: frozenset[str]) -> list[str]:
        return sorted(value)

    @model_validator(mode="after")
    def validate_limits_and_price(self) -> "ModelSpec":
        if self.max_output_tokens > self.context_window:
            raise ValueError("max_output_tokens cannot exceed context_window")
        if self.price is None and not self.local_zero_cost:
            raise ValueError("paid models require a frozen price specification")
        return self


class ModelRegistryManifest(_ConfigModel):
    """Immutable, de-keyed provider/model registry snapshot."""

    schema_version: Literal[1] = 1
    providers: tuple[ProviderSpec, ...] = Field(min_length=1)
    models: tuple[ModelSpec, ...] = Field(min_length=1)

    @field_validator("providers", "models", mode="before")
    @classmethod
    def normalize_sequences(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("value must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def validate_references_and_uniqueness(self) -> "ModelRegistryManifest":
        provider_ids = [provider.id for provider in self.providers]
        if len(provider_ids) != len(set(provider_ids)):
            raise ValueError("provider ids must be unique")
        model_ids = [model.id for model in self.models]
        if len(model_ids) != len(set(model_ids)):
            raise ValueError("model ids must be unique")
        known_providers = set(provider_ids)
        unknown = sorted({model.provider for model in self.models} - known_providers)
        if unknown:
            raise ValueError(f"models reference unknown providers: {unknown}")
        return self

    def canonical_json(self) -> str:
        """Return stable JSON independent of input order for content hashing."""
        payload = self.model_dump(mode="json")
        payload["providers"] = sorted(payload["providers"], key=lambda item: item["id"])
        payload["models"] = sorted(payload["models"], key=lambda item: item["id"])
        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @property
    def content_hash(self) -> str:
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"


def tighten_policy_envelopes(*layers: PolicyEnvelope) -> PolicyEnvelope:
    """Combine authority envelopes monotonically, failing closed on FX gaps."""
    currencies = {layer.currency for layer in layers if layer.currency is not None}
    if len(currencies) > 1:
        raise ValueError(
            "currency conversion requires an explicit frozen exchange-rate snapshot"
        )

    def minimum(field_name: str) -> int | None:
        values = [
            getattr(layer, field_name)
            for layer in layers
            if getattr(layer, field_name) is not None
        ]
        return min(values) if values else None

    def intersection(field_name: str) -> frozenset[str] | None:
        values = [
            getattr(layer, field_name)
            for layer in layers
            if getattr(layer, field_name) is not None
        ]
        if not values:
            return None
        return frozenset(values[0].intersection(*values[1:]))

    def union(field_name: str) -> frozenset[str] | None:
        values = [
            getattr(layer, field_name)
            for layer in layers
            if getattr(layer, field_name) is not None
        ]
        if not values:
            return None
        return frozenset().union(*values)

    tier_order = {"economy": 0, "standard": 1, "high": 2}
    tiers = [layer.min_tier for layer in layers if layer.min_tier is not None]
    return PolicyEnvelope(
        currency=next(iter(currencies), None),
        max_cost_minor=minimum("max_cost_minor"),
        max_total_tokens=minimum("max_total_tokens"),
        max_agents=minimum("max_agents"),
        max_depth=minimum("max_depth"),
        max_concurrency=minimum("max_concurrency"),
        allowed_models=intersection("allowed_models"),
        allowed_providers=intersection("allowed_providers"),
        allowed_capabilities=intersection("allowed_capabilities"),
        denied_models=union("denied_models"),
        denied_providers=union("denied_providers"),
        min_tier=max(tiers, key=tier_order.__getitem__) if tiers else None,
        require_independent_review=(
            any(layer.require_independent_review is True for layer in layers)
            if any(layer.require_independent_review is not None for layer in layers)
            else None
        ),
        require_approval=(
            any(layer.require_approval is True for layer in layers)
            if any(layer.require_approval is not None for layer in layers)
            else None
        ),
        max_parallel_candidates=minimum("max_parallel_candidates"),
    )


__all__ = [
    "ModelRegistryManifest",
    "ModelSpec",
    "PolicyEnvelope",
    "PriceSpec",
    "ProviderAdapter",
    "ProviderSpec",
    "ReasoningEffort",
    "Tier",
    "tighten_policy_envelopes",
]
