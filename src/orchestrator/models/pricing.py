"""Versioned tokenizer and FX snapshots used by deterministic cost estimates."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator, model_validator

from orchestrator.budget.models import CostEstimate
from orchestrator.config.models import ModelRegistryManifest


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_CURRENCY = re.compile(r"^[A-Z]{3}$")
_MILLION = 1_000_000


def _identifier(value: str, field_name: str) -> str:
    if value != value.strip() or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field_name} must be a stable non-blank identifier")
    return value


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return value


def _content_hash(value: BaseModel, *, sequence_field: str | None = None, key: str = "id") -> str:
    payload = value.model_dump(mode="json")
    if sequence_field is not None:
        payload[sequence_field] = sorted(payload[sequence_field], key=lambda item: item[key])
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


class _PricingModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)


class TokenizerBinding(_PricingModel):
    """Model-to-tokenizer/estimator binding frozen for cost reproduction."""

    model_id: StrictStr = Field(min_length=1)
    tokenizer_id: StrictStr = Field(min_length=1)
    tokenizer_version: StrictStr = Field(min_length=1)
    estimator_id: StrictStr = Field(min_length=1)
    estimator_version: StrictStr = Field(min_length=1)
    effective_from: datetime
    expires_at: datetime

    @field_validator("model_id", "tokenizer_id", "tokenizer_version", "estimator_id", "estimator_version")
    @classmethod
    def stable_identifiers(cls, value: str, info: object) -> str:
        return _identifier(value, getattr(info, "field_name", "identifier"))

    @field_validator("effective_from", "expires_at")
    @classmethod
    def require_aware_time(cls, value: datetime, info: object) -> datetime:
        return _aware(value, getattr(info, "field_name", "timestamp"))

    @model_validator(mode="after")
    def validate_window(self) -> "TokenizerBinding":
        if self.expires_at <= self.effective_from:
            raise ValueError("tokenizer binding expiry must follow its effective time")
        return self

    @property
    def content_hash(self) -> str:
        return _content_hash(self)


class TokenizerSnapshot(_PricingModel):
    """Immutable tokenizer/estimator bindings with a deterministic content ID."""

    schema_version: int = Field(default=1, ge=1, le=1)
    bindings: tuple[TokenizerBinding, ...] = Field(min_length=1)

    @field_validator("bindings", mode="before")
    @classmethod
    def normalize_bindings(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("bindings must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def unique_models(self) -> "TokenizerSnapshot":
        model_ids = [binding.model_id for binding in self.bindings]
        if len(model_ids) != len(set(model_ids)):
            raise ValueError("tokenizer snapshot must have one binding per model")
        return self

    @property
    def snapshot_id(self) -> str:
        return _content_hash(self, sequence_field="bindings", key="model_id")

    def binding_for(self, model_id: str, *, as_of: datetime) -> TokenizerBinding:
        as_of = _aware(as_of, "as_of")
        binding = next((item for item in self.bindings if item.model_id == model_id), None)
        if binding is None:
            raise CostingDataUnavailable("tokenizer_missing")
        if not binding.effective_from <= as_of < binding.expires_at:
            raise CostingDataUnavailable("tokenizer_expired")
        return binding


class ExchangeRate(_PricingModel):
    """Exact rational quote: target minor units per source minor unit."""

    source_currency: StrictStr = Field(pattern=r"^[A-Z]{3}$")
    target_currency: StrictStr = Field(pattern=r"^[A-Z]{3}$")
    numerator: StrictInt = Field(gt=0, le=10**12)
    denominator: StrictInt = Field(gt=0, le=10**12)

    @model_validator(mode="after")
    def distinct_currencies(self) -> "ExchangeRate":
        if self.source_currency == self.target_currency:
            raise ValueError("an FX rate must convert between distinct currencies")
        return self


class FXSnapshot(_PricingModel):
    """A frozen set of direct quotes into one budget currency."""

    schema_version: int = Field(default=1, ge=1, le=1)
    base_currency: StrictStr = Field(pattern=r"^[A-Z]{3}$")
    source_id: StrictStr = Field(min_length=1, pattern=_IDENTIFIER.pattern)
    effective_from: datetime
    expires_at: datetime
    rates: tuple[ExchangeRate, ...] = ()

    @field_validator("effective_from", "expires_at")
    @classmethod
    def require_aware_time(cls, value: datetime, info: object) -> datetime:
        return _aware(value, getattr(info, "field_name", "timestamp"))

    @field_validator("rates", mode="before")
    @classmethod
    def normalize_rates(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("rates must be an array")
        return tuple(value)

    @model_validator(mode="after")
    def validate_fx_table(self) -> "FXSnapshot":
        if self.expires_at <= self.effective_from:
            raise ValueError("FX snapshot expiry must follow its effective time")
        sources = [rate.source_currency for rate in self.rates]
        if len(sources) != len(set(sources)):
            raise ValueError("FX snapshot must have one direct quote per source currency")
        if any(rate.target_currency != self.base_currency for rate in self.rates):
            raise ValueError("all FX quotes must target the snapshot base currency")
        if self.base_currency in sources:
            raise ValueError("the base currency uses an implicit 1:1 rate")
        return self

    @property
    def snapshot_id(self) -> str:
        return _content_hash(self, sequence_field="rates", key="source_currency")

    def convert_minor(
        self,
        amount_minor: int,
        source_currency: str,
        target_currency: str,
        *,
        as_of: datetime,
    ) -> int:
        """Convert minor units using integer arithmetic and conservative ceiling."""

        if isinstance(amount_minor, bool) or not isinstance(amount_minor, int) or amount_minor < 0:
            raise ValueError("amount_minor must be a non-negative integer")
        if not _CURRENCY.fullmatch(source_currency) or not _CURRENCY.fullmatch(target_currency):
            raise ValueError("currency must be an uppercase three-letter code")
        as_of = _aware(as_of, "as_of")
        if not self.effective_from <= as_of < self.expires_at:
            raise CostingDataUnavailable("fx_expired")
        if target_currency != self.base_currency:
            raise CostingDataUnavailable("fx_base_currency_mismatch")
        if source_currency == target_currency:
            return amount_minor
        rate = next((item for item in self.rates if item.source_currency == source_currency), None)
        if rate is None:
            raise CostingDataUnavailable("fx_pair_unavailable")
        return _ceil_div(amount_minor * rate.numerator, rate.denominator)


class CostingDataUnavailable(ValueError):
    """Stable, value-free reason that a cost snapshot cannot be trusted."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class TokenizerCounter(Protocol):
    """A pinned implementation that counts text using a frozen binding."""

    def count_text(self, text: str, *, binding: TokenizerBinding) -> int: ...


def validate_costing_snapshots(
    registry: ModelRegistryManifest,
    tokenizer_snapshot: TokenizerSnapshot,
    fx_snapshot: FXSnapshot,
    *,
    target_currency: str,
    as_of: datetime,
    model_id: str | None = None,
) -> None:
    """Fail closed unless each registry model has active tokenizer, price, and FX inputs."""

    as_of = _aware(as_of, "as_of")
    if not _CURRENCY.fullmatch(target_currency):
        raise ValueError("target_currency must be an uppercase three-letter code")
    if fx_snapshot.base_currency != target_currency:
        raise CostingDataUnavailable("fx_base_currency_mismatch")
    model_ids = {model.id for model in registry.models}
    binding_ids = {binding.model_id for binding in tokenizer_snapshot.bindings}
    if binding_ids - model_ids:
        raise CostingDataUnavailable("tokenizer_model_unregistered")
    if model_id is not None and model_id not in model_ids:
        raise CostingDataUnavailable("model_unregistered")
    enabled_providers = {provider.id for provider in registry.providers if provider.enabled}
    selected_models = tuple(
        model
        for model in registry.models
        if model.provider in enabled_providers and (model_id is None or model.id == model_id)
    )
    for model in selected_models:
        binding = tokenizer_snapshot.binding_for(model.id, as_of=as_of)
        if model.price is None:
            if not model.local_zero_cost:
                raise CostingDataUnavailable("price_missing")
            continue
        if model.price.estimator_id != binding.estimator_id:
            raise CostingDataUnavailable("estimator_mismatch")
        if not model.price.effective_from <= as_of < model.price.expires_at:
            raise CostingDataUnavailable("price_expired")
        fx_snapshot.convert_minor(
            0,
            model.price.currency,
            target_currency,
            as_of=as_of,
        )


def estimate_model_cost(
    registry: ModelRegistryManifest,
    tokenizer_snapshot: TokenizerSnapshot,
    fx_snapshot: FXSnapshot,
    *,
    model_id: str,
    input_tokens: int,
    output_tokens: int,
    target_currency: str,
    as_of: datetime,
    reasoning_tokens: int = 0,
    cached_input_tokens: int = 0,
    provider_fee_minor: int = 0,
    tool_fee_minor: int = 0,
) -> CostEstimate:
    """Estimate a conservative fee and retain every frozen input snapshot ID."""

    counts = (input_tokens, output_tokens, reasoning_tokens, cached_input_tokens)
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
        raise ValueError("token counts must be non-negative integers")
    fees = (provider_fee_minor, tool_fee_minor)
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in fees):
        raise ValueError("fees must be non-negative integers")
    validate_costing_snapshots(
        registry,
        tokenizer_snapshot,
        fx_snapshot,
        target_currency=target_currency,
        as_of=as_of,
        model_id=model_id,
    )
    model = next((item for item in registry.models if item.id == model_id), None)
    if model is None:
        raise CostingDataUnavailable("model_unregistered")
    provider = next((item for item in registry.providers if item.id == model.provider), None)
    if provider is None or not provider.enabled:
        raise CostingDataUnavailable("provider_disabled")
    binding = tokenizer_snapshot.binding_for(model_id, as_of=as_of)
    price = model.price
    input_rate = 0 if price is None else price.input_minor_per_million
    output_rate = 0 if price is None else price.output_minor_per_million
    base_currency = target_currency if price is None else price.currency
    if price is not None:
        tool_fee_minor += price.max_tool_cost_minor
    # PriceSpec has no separate cached/reasoning tariff. Charge both at the
    # more conservative output/input rates declared by the frozen manifest.
    source_cost = _ceil_div(
        (input_tokens + cached_input_tokens) * input_rate
        + (output_tokens + reasoning_tokens) * output_rate,
        _MILLION,
    ) + provider_fee_minor + tool_fee_minor
    amount_minor = fx_snapshot.convert_minor(
        source_cost,
        base_currency,
        target_currency,
        as_of=as_of,
    )
    estimate_identity = {
        "fx_snapshot_id": fx_snapshot.snapshot_id,
        "model_id": model_id,
        "price_snapshot_id": registry.content_hash,
        "target_currency": target_currency,
        "tokenizer_binding_id": binding.content_hash,
        "tokenizer_snapshot_id": tokenizer_snapshot.snapshot_id,
    }
    snapshot_id = "sha256:" + hashlib.sha256(
        json.dumps(estimate_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return CostEstimate(
        amount_minor=amount_minor,
        currency=target_currency,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        reasoning_tokens=reasoning_tokens,
        cached_input_tokens=cached_input_tokens,
        provider_fee_minor=provider_fee_minor,
        tool_fee_minor=tool_fee_minor,
        input_price_minor_per_million=input_rate,
        output_price_minor_per_million=output_rate,
        reasoning_price_minor_per_million=output_rate,
        cached_input_price_minor_per_million=input_rate,
        snapshot_id=snapshot_id,
        tokenizer_snapshot_id=tokenizer_snapshot.snapshot_id,
        price_snapshot_id=registry.content_hash,
        estimator_snapshot_id=binding.content_hash,
        fx_snapshot_id=fx_snapshot.snapshot_id,
        price_currency=base_currency,
    )


def _ceil_div(numerator: int, denominator: int) -> int:
    if numerator == 0:
        return 0
    return (numerator + denominator - 1) // denominator


__all__ = [
    "CostingDataUnavailable",
    "ExchangeRate",
    "FXSnapshot",
    "TokenizerBinding",
    "TokenizerCounter",
    "TokenizerSnapshot",
    "estimate_model_cost",
    "validate_costing_snapshots",
]
