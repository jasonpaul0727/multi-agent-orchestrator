"""Validated value objects used by the durable budget ledger.

The budget boundary deliberately deals only in integers.  Currency amounts
are minor units (for example, cents for USD), and token counts are exact
provider-reported or conservatively estimated integers.  Keeping these
objects immutable makes it safe to hand them between the control plane and
workers without accidental in-place accounting changes.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)


Currency = StrictStr
ReservationStatus = Literal["reserved", "committed", "released", "unknown"]


def _currency(value: Any) -> Any:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("currency must be a non-blank string")
    value = value.strip().upper()
    if len(value) != 3 or not value.isascii() or not value.isalpha():
        raise ValueError("currency must be a three-letter ISO currency code")
    return value


def _snapshot_id(value: Any, field_name: str) -> Any:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ValueError(f"{field_name} must not contain lone surrogate characters")
    return value


class _BudgetModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("currency", mode="before", check_fields=False)
    @classmethod
    def validate_currency(cls, value: Any) -> Any:
        return _currency(value)


class RunLimit(_BudgetModel):
    """The immutable cost and token envelope for a Run.

    ``None`` means that a dimension is not separately capped.  A currency is
    always present; the default keeps the small local API pleasant while
    still making every persisted amount explicit.
    """

    max_cost_minor: StrictInt | None = Field(
        default=None,
        ge=0,
        validation_alias=AliasChoices("max_cost_minor", "max_amount_minor", "max_cost"),
    )
    max_tokens: StrictInt | None = Field(
        default=None,
        ge=0,
        validation_alias=AliasChoices(
            "max_tokens", "max_total_tokens", "token_limit"
        ),
    )
    currency: Currency = "USD"
    max_input_tokens: StrictInt | None = Field(default=None, ge=0)
    max_output_tokens: StrictInt | None = Field(default=None, ge=0)
    max_reasoning_tokens: StrictInt | None = Field(default=None, ge=0)
    max_cached_input_tokens: StrictInt | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def require_a_limit(self) -> "RunLimit":
        if self.max_cost_minor is None and self.max_tokens is None:
            raise ValueError("RunLimit must define max_cost_minor or max_tokens")
        return self

    @property
    def max_amount_minor(self) -> int | None:
        return self.max_cost_minor

    @property
    def max_total_tokens(self) -> int | None:
        return self.max_tokens

class CostEstimate(_BudgetModel):
    """A worst-case or actual cost calculation at frozen prices."""

    amount_minor: StrictInt = Field(
        validation_alias=AliasChoices("amount_minor", "cost_minor"), ge=0
    )
    currency: Currency
    token_limit: StrictInt | None = Field(default=None, ge=0)
    input_tokens: StrictInt = Field(default=0, ge=0)
    output_tokens: StrictInt = Field(default=0, ge=0)
    reasoning_tokens: StrictInt = Field(default=0, ge=0)
    cached_input_tokens: StrictInt = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices("cached_input_tokens", "cached_tokens"),
    )
    provider_fee_minor: StrictInt = Field(default=0, ge=0)
    tool_fee_minor: StrictInt = Field(default=0, ge=0)
    input_price_minor_per_million: StrictInt = Field(default=0, ge=0)
    output_price_minor_per_million: StrictInt = Field(default=0, ge=0)
    reasoning_price_minor_per_million: StrictInt = Field(default=0, ge=0)
    cached_input_price_minor_per_million: StrictInt = Field(default=0, ge=0)
    snapshot_id: StrictStr = "unspecified"
    tokenizer_snapshot_id: StrictStr = "unspecified"
    price_snapshot_id: StrictStr = "unspecified"
    estimator_snapshot_id: StrictStr = "unspecified"

    @field_validator(
        "snapshot_id",
        "tokenizer_snapshot_id",
        "price_snapshot_id",
        "estimator_snapshot_id",
        mode="before",
    )
    @classmethod
    def validate_snapshots(cls, value: Any, info: Any) -> Any:
        return _snapshot_id(value, info.field_name)

    @model_validator(mode="after")
    def derive_token_limit(self) -> "CostEstimate":
        if self.token_limit is None:
            object.__setattr__(
                self,
                "token_limit",
                self.input_tokens
                + self.output_tokens
                + self.reasoning_tokens
                + self.cached_input_tokens,
            )
        elif self.token_limit < self.total_tokens:
            raise ValueError("token_limit cannot be lower than the token counts")
        return self

    @property
    def cost_minor(self) -> int:
        return self.amount_minor

    @property
    def total_tokens(self) -> int:
        return (
            self.input_tokens
            + self.output_tokens
            + self.reasoning_tokens
            + self.cached_input_tokens
        )


class BudgetReservation(_BudgetModel):
    """A durable hold against one Run envelope."""

    reservation_id: StrictStr = Field(min_length=1)
    run_id: StrictStr = Field(min_length=1)
    reserved_minor: StrictInt = Field(
        validation_alias=AliasChoices("reserved_minor", "reserved_cost_minor"), ge=0
    )
    reserved_tokens: StrictInt = Field(
        validation_alias=AliasChoices("reserved_tokens", "token_limit"), ge=0
    )
    reserved_input_tokens: StrictInt = Field(default=0, ge=0)
    reserved_output_tokens: StrictInt = Field(default=0, ge=0)
    reserved_reasoning_tokens: StrictInt = Field(default=0, ge=0)
    reserved_cached_input_tokens: StrictInt = Field(default=0, ge=0)
    currency: Currency
    reservation_version: StrictInt = Field(default=1, gt=0)
    status: ReservationStatus = "reserved"
    snapshot_id: StrictStr = "unspecified"
    tokenizer_snapshot_id: StrictStr = "unspecified"
    price_snapshot_id: StrictStr = "unspecified"
    estimator_snapshot_id: StrictStr = "unspecified"
    node_id: StrictStr | None = None
    attempt_id: StrictStr | None = None
    fencing_generation: StrictInt | None = Field(default=None, ge=0)
    correlation_id: StrictStr | None = None
    causation_id: StrictStr | None = None

    @field_validator("reservation_id", "run_id", mode="before")
    @classmethod
    def validate_identifiers(cls, value: Any, info: Any) -> Any:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{info.field_name} must be a non-blank string")
        return value

    @field_validator(
        "node_id", "attempt_id", "correlation_id", "causation_id", mode="before"
    )
    @classmethod
    def validate_optional_execution_ids(cls, value: Any, info: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{info.field_name} must be a non-blank string")
        return value

    @field_validator(
        "snapshot_id",
        "tokenizer_snapshot_id",
        "price_snapshot_id",
        "estimator_snapshot_id",
        mode="before",
    )
    @classmethod
    def validate_snapshots(cls, value: Any, info: Any) -> Any:
        return _snapshot_id(value, info.field_name)

    @property
    def amount_minor(self) -> int:
        return self.reserved_minor

    @property
    def token_limit(self) -> int:
        return self.reserved_tokens

    @property
    def version(self) -> int:
        return self.reservation_version

    @model_validator(mode="after")
    def validate_token_breakdown(self) -> "BudgetReservation":
        total = (
            self.reserved_input_tokens
            + self.reserved_output_tokens
            + self.reserved_reasoning_tokens
            + self.reserved_cached_input_tokens
        )
        if self.reserved_tokens < total:
            raise ValueError(
                "reserved_tokens cannot be lower than the reserved token breakdown"
            )
        return self


class UsageRecord(_BudgetModel):
    """Exact usage observed from a model/tool provider."""

    reservation_id: StrictStr
    run_id: StrictStr
    settlement_key: StrictStr
    currency: Currency
    input_tokens: StrictInt = Field(
        default=0, ge=0, validation_alias=AliasChoices("input_tokens", "input")
    )
    output_tokens: StrictInt = Field(
        default=0, ge=0, validation_alias=AliasChoices("output_tokens", "output")
    )
    reasoning_tokens: StrictInt = Field(
        default=0, ge=0, validation_alias=AliasChoices("reasoning_tokens", "reasoning")
    )
    cached_input_tokens: StrictInt = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices(
            "cached_input_tokens", "cached_tokens", "cached"
        ),
    )
    provider_fee_minor: StrictInt = Field(default=0, ge=0)
    tool_fee_minor: StrictInt = Field(default=0, ge=0)
    cost_minor: StrictInt = Field(
        default=0, ge=0, validation_alias=AliasChoices("cost_minor", "amount_minor", "actual_minor")
    )
    status: Literal["observed", "committed"] = "committed"

    @field_validator("reservation_id", "run_id", "settlement_key", mode="before")
    @classmethod
    def validate_identifiers(cls, value: Any, info: Any) -> Any:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{info.field_name} must be a non-blank string")
        return value

    @property
    def amount_minor(self) -> int:
        return self.cost_minor

    @property
    def actual_minor(self) -> int:
        return self.cost_minor

    @property
    def total_tokens(self) -> int:
        return (
            self.input_tokens
            + self.output_tokens
            + self.reasoning_tokens
            + self.cached_input_tokens
        )


class BudgetBalance(_BudgetModel):
    """A replayed, read-only view of one Run's budget stream."""

    run_id: StrictStr
    currency: Currency
    max_cost_minor: StrictInt | None = Field(default=None, ge=0)
    max_tokens: StrictInt | None = Field(
        default=None,
        ge=0,
        validation_alias=AliasChoices("max_tokens", "max_total_tokens"),
    )
    max_input_tokens: StrictInt | None = Field(default=None, ge=0)
    max_output_tokens: StrictInt | None = Field(default=None, ge=0)
    max_reasoning_tokens: StrictInt | None = Field(default=None, ge=0)
    max_cached_input_tokens: StrictInt | None = Field(default=None, ge=0)
    reserved_minor: StrictInt = Field(default=0, ge=0)
    reserved_tokens: StrictInt = Field(default=0, ge=0)
    used_minor: StrictInt = Field(default=0, ge=0)
    used_tokens: StrictInt = Field(default=0, ge=0)
    released_minor: StrictInt = Field(default=0, ge=0)
    released_tokens: StrictInt = Field(default=0, ge=0)
    unknown_minor: StrictInt = Field(default=0, ge=0)
    unknown_tokens: StrictInt = Field(default=0, ge=0)
    reserved_input_tokens: StrictInt = Field(default=0, ge=0)
    reserved_output_tokens: StrictInt = Field(default=0, ge=0)
    reserved_reasoning_tokens: StrictInt = Field(default=0, ge=0)
    reserved_cached_input_tokens: StrictInt = Field(default=0, ge=0)
    used_input_tokens: StrictInt = Field(default=0, ge=0)
    used_output_tokens: StrictInt = Field(default=0, ge=0)
    used_reasoning_tokens: StrictInt = Field(default=0, ge=0)
    used_cached_input_tokens: StrictInt = Field(default=0, ge=0)
    unknown_input_tokens: StrictInt = Field(default=0, ge=0)
    unknown_output_tokens: StrictInt = Field(default=0, ge=0)
    unknown_reasoning_tokens: StrictInt = Field(default=0, ge=0)
    unknown_cached_input_tokens: StrictInt = Field(default=0, ge=0)
    reservation_version: StrictInt = Field(default=0, ge=0)
    latest_event_id: StrictStr | None = None

    @field_validator("run_id", mode="before")
    @classmethod
    def validate_run_id(cls, value: Any) -> Any:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("run_id must be a non-blank string")
        return value

    @property
    def held_minor(self) -> int:
        return self.reserved_minor + self.unknown_minor

    @property
    def held_tokens(self) -> int:
        return self.reserved_tokens + self.unknown_tokens

    @property
    def max_total_tokens(self) -> int | None:
        return self.max_tokens

    def _available_class(self, name: str, cap: int | None) -> int | None:
        if cap is None:
            return None
        held = getattr(self, f"reserved_{name}") + getattr(self, f"unknown_{name}")
        return max(0, cap - getattr(self, f"used_{name}") - held)

    @property
    def available_input_tokens(self) -> int | None:
        return self._available_class("input_tokens", self.max_input_tokens)

    @property
    def available_output_tokens(self) -> int | None:
        return self._available_class("output_tokens", self.max_output_tokens)

    @property
    def available_reasoning_tokens(self) -> int | None:
        return self._available_class("reasoning_tokens", self.max_reasoning_tokens)

    @property
    def available_cached_input_tokens(self) -> int | None:
        return self._available_class(
            "cached_input_tokens", self.max_cached_input_tokens
        )

    @property
    def available_minor(self) -> int | None:
        if self.max_cost_minor is None:
            return None
        return max(0, self.max_cost_minor - self.used_minor - self.held_minor)

    @property
    def available_tokens(self) -> int | None:
        if self.max_tokens is None:
            return None
        return max(0, self.max_tokens - self.used_tokens - self.held_tokens)

    @property
    def remaining_minor(self) -> int | None:
        return self.available_minor

    @property
    def remaining_tokens(self) -> int | None:
        return self.available_tokens


__all__ = [
    "BudgetBalance",
    "BudgetReservation",
    "CostEstimate",
    "RunLimit",
    "UsageRecord",
]
