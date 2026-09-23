from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from orchestrator.budget.models import CostEstimate
from orchestrator.budget import BudgetLedger, RunLimit
from orchestrator.config.models import (
    ModelRegistryManifest,
    ModelSpec,
    PriceSpec,
    ProviderSpec,
)
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.models import (
    CostingDataUnavailable,
    ExchangeRate,
    FXSnapshot,
    TokenizerBinding,
    TokenizerSnapshot,
    estimate_model_cost,
    validate_costing_snapshots,
)


AS_OF = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)


def registry(
    *, currency: str = "EUR", expires_at: datetime | None = None
) -> ModelRegistryManifest:
    price = PriceSpec(
        currency=currency,
        input_minor_per_million=100,
        output_minor_per_million=200,
        max_tool_cost_minor=2,
        estimator_id="tokens.v1",
        effective_from=AS_OF - timedelta(days=3),
        expires_at=expires_at or AS_OF + timedelta(days=30),
    )
    return ModelRegistryManifest(
        providers=(
            ProviderSpec(
                id="primary",
                adapter="openai_responses",
                secret_ref="env:MODEL_KEY",
                enabled=True,
            ),
        ),
        models=(
            ModelSpec(
                id="model-standard",
                provider="primary",
                remote_model="remote-standard",
                tier="standard",
                capabilities={"text"},
                context_window=32_000,
                max_output_tokens=4_000,
                supported_reasoning_efforts={"low", "medium"},
                price=price,
            ),
        ),
    )


def tokenizer_snapshot(*, expires_at: datetime | None = None) -> TokenizerSnapshot:
    return TokenizerSnapshot(
        bindings=(
            TokenizerBinding(
                model_id="model-standard",
                tokenizer_id="tiktoken.o200k_base",
                tokenizer_version="2026.09",
                estimator_id="tokens.v1",
                estimator_version="1.2.0",
                effective_from=AS_OF - timedelta(days=10),
                expires_at=expires_at or AS_OF + timedelta(days=30),
            ),
        ),
    )


def fx_snapshot(
    *,
    expires_at: datetime | None = None,
    rates: tuple[ExchangeRate, ...] | None = None,
) -> FXSnapshot:
    if rates is None:
        rates = (ExchangeRate(source_currency="EUR", target_currency="USD", numerator=11, denominator=10),)
    return FXSnapshot(
        base_currency="USD",
        source_id="fx.ecb.daily.2026-09-22",
        effective_from=AS_OF - timedelta(days=1),
        expires_at=expires_at or AS_OF + timedelta(days=1),
        rates=rates,
    )


def test_fx_conversion_uses_exact_rational_and_rounds_up():
    fx = fx_snapshot()

    assert fx.convert_minor(101, "EUR", "USD", as_of=AS_OF) == 112
    assert fx.convert_minor(101, "USD", "USD", as_of=AS_OF) == 101
    assert fx.snapshot_id.startswith("sha256:")
    with pytest.raises(CostingDataUnavailable, match="fx_base_currency_mismatch"):
        fx.convert_minor(101, "EUR", "GBP", as_of=AS_OF)


def test_costing_inputs_are_validated_at_event_time_and_fail_closed():
    manifest = registry()
    tokens = tokenizer_snapshot()
    fx = fx_snapshot()
    validate_costing_snapshots(
        manifest,
        tokens,
        fx,
        target_currency="USD",
        as_of=AS_OF,
    )

    with pytest.raises(CostingDataUnavailable, match="fx_expired"):
        validate_costing_snapshots(
            manifest,
            tokens,
            fx_snapshot(expires_at=AS_OF),
            target_currency="USD",
            as_of=AS_OF,
        )
    with pytest.raises(CostingDataUnavailable, match="tokenizer_expired"):
        validate_costing_snapshots(
            manifest,
            tokenizer_snapshot(expires_at=AS_OF),
            fx,
            target_currency="USD",
            as_of=AS_OF,
        )
    with pytest.raises(CostingDataUnavailable, match="fx_pair_unavailable"):
        validate_costing_snapshots(
            manifest,
            tokens,
            fx_snapshot(rates=()),
            target_currency="USD",
            as_of=AS_OF,
        )
    with pytest.raises(CostingDataUnavailable, match="price_expired"):
        validate_costing_snapshots(
            registry(expires_at=AS_OF),
            tokens,
            fx,
            target_currency="USD",
            as_of=AS_OF,
        )
    with pytest.raises(CostingDataUnavailable, match="estimator_mismatch"):
        validate_costing_snapshots(
            manifest,
            TokenizerSnapshot(
                bindings=(
                    tokens.bindings[0].model_copy(update={"estimator_id": "other.v1"}),
                )
            ),
            fx,
            target_currency="USD",
            as_of=AS_OF,
        )
    with pytest.raises(CostingDataUnavailable, match="fx_base_currency_mismatch"):
        validate_costing_snapshots(
            manifest,
            tokens,
            FXSnapshot(
                base_currency="GBP",
                source_id="fx.other",
                effective_from=AS_OF - timedelta(days=1),
                expires_at=AS_OF + timedelta(days=1),
            ),
            target_currency="USD",
            as_of=AS_OF,
        )


def test_cost_estimate_keeps_registry_tokenizer_fx_and_price_currency_refs():
    manifest = registry()
    tokens = tokenizer_snapshot()
    fx = fx_snapshot()

    estimate = estimate_model_cost(
        manifest,
        tokens,
        fx,
        model_id="model-standard",
        input_tokens=1_000,
        output_tokens=1_000,
        target_currency="USD",
        as_of=AS_OF,
        provider_fee_minor=1,
        tool_fee_minor=1,
    )

    assert isinstance(estimate, CostEstimate)
    assert estimate.amount_minor == 6
    assert estimate.currency == "USD"
    assert estimate.price_currency == "EUR"
    assert estimate.fx_snapshot_id == fx.snapshot_id
    assert estimate.tokenizer_snapshot_id == tokens.snapshot_id
    assert estimate.price_snapshot_id == manifest.content_hash
    assert estimate.estimator_snapshot_id == tokens.bindings[0].content_hash


def test_budget_reservation_persists_fx_snapshot_reference(tmp_path):
    manifest = registry()
    tokens = tokenizer_snapshot()
    fx = fx_snapshot()
    estimate = estimate_model_cost(
        manifest,
        tokens,
        fx,
        model_id="model-standard",
        input_tokens=1_000,
        output_tokens=1_000,
        target_currency="USD",
        as_of=AS_OF,
    )
    store = SQLiteEventStore(tmp_path / "fx-budget.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=10_000)},
    )

    reservation = ledger.reserve("run-1", estimate)
    reserved_event = ledger.read("run-1")[0]
    replayed = ledger.get_reservation(reservation.reservation_id)

    assert reservation.fx_snapshot_id == fx.snapshot_id
    assert replayed.fx_snapshot_id == fx.snapshot_id
    assert reserved_event.payload["fx_snapshot_id"] == fx.snapshot_id
    assert reserved_event.payload["price_currency"] == "EUR"
    store.close()


def test_tokenizer_snapshot_hash_ignores_binding_order_and_rejects_duplicate_models():
    first = tokenizer_snapshot()
    one = first.bindings[0]
    other = one.model_copy(update={"model_id": "model-other"})
    left = TokenizerSnapshot(bindings=(one, other))
    right = TokenizerSnapshot(bindings=(other, one))

    assert left.snapshot_id == right.snapshot_id
    with pytest.raises(ValidationError, match="one binding per model"):
        TokenizerSnapshot(bindings=(one, one))


def test_fx_snapshot_rejects_duplicate_and_non_base_quotes():
    rate = ExchangeRate(
        source_currency="EUR", target_currency="USD", numerator=11, denominator=10
    )
    with pytest.raises(ValidationError, match="one direct quote"):
        fx_snapshot(rates=(rate, rate))
    with pytest.raises(ValidationError, match="target the snapshot base"):
        fx_snapshot(
            rates=(
                ExchangeRate(
                    source_currency="EUR",
                    target_currency="GBP",
                    numerator=9,
                    denominator=8,
                ),
            )
        )
