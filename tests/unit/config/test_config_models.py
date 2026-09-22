"""Tests for configuration and model registry boundary objects."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from orchestrator.config import (
    ModelRegistryManifest,
    ModelSpec,
    PolicyEnvelope,
    PriceSpec,
    ProviderSpec,
    tighten_policy_envelopes,
)


NOW = datetime(2026, 9, 22, tzinfo=timezone.utc)


def provider(
    provider_id: str = "primary",
    secret_ref: str = "env:ORCHESTRATOR_TEST_KEY",
) -> ProviderSpec:
    return ProviderSpec(
        id=provider_id,
        adapter="openai_responses",
        secret_ref=secret_ref,
        enabled=True,
    )


def price() -> PriceSpec:
    return PriceSpec(
        currency="usd",
        input_minor_per_million=100,
        output_minor_per_million=200,
        max_tool_cost_minor=0,
        estimator_id="tokenizer-v1",
        effective_from=NOW,
        expires_at=NOW + timedelta(days=30),
    )


def model(model_id: str = "model-a", provider_id: str = "primary") -> ModelSpec:
    return ModelSpec(
        id=model_id,
        provider=provider_id,
        remote_model="provider-model-a",
        tier="standard",
        capabilities={"text", "tools"},
        context_window=32_000,
        max_output_tokens=4_000,
        supported_reasoning_efforts={"low", "high"},
        price=price(),
    )


def test_policy_envelope_is_immutable_and_accepts_only_known_constraints():
    envelope = PolicyEnvelope(
        currency="usd",
        max_cost_minor=500,
        max_total_tokens=20_000,
        allowed_models={"model-a", "model-b"},
        denied_providers={"blocked"},
        require_approval=True,
    )

    assert envelope.currency == "USD"
    assert envelope.allowed_models == frozenset({"model-a", "model-b"})
    with pytest.raises(ValidationError):
        envelope.max_cost_minor = 1_000
    with pytest.raises(ValidationError):
        PolicyEnvelope(allow_models={"model-a"})
    with pytest.raises(ValidationError):
        PolicyEnvelope(max_agents=True)


def test_policy_envelopes_only_tighten_limits_and_sets():
    effective = tighten_policy_envelopes(
        PolicyEnvelope(
            currency="USD",
            max_cost_minor=1_000,
            max_total_tokens=50_000,
            allowed_models={"a", "b"},
            denied_providers={"blocked-a"},
            min_tier="standard",
            require_approval=False,
            max_parallel_candidates=3,
        ),
        PolicyEnvelope(
            max_cost_minor=500,
            max_total_tokens=60_000,
            allowed_models={"b", "c"},
            denied_providers={"blocked-b"},
            min_tier="high",
            require_approval=True,
            max_parallel_candidates=2,
        ),
    )

    assert effective.max_cost_minor == 500
    assert effective.max_total_tokens == 50_000
    assert effective.allowed_models == frozenset({"b"})
    assert effective.denied_providers == frozenset({"blocked-a", "blocked-b"})
    assert effective.min_tier == "high"
    assert effective.require_approval is True
    assert effective.max_parallel_candidates == 2


def test_policy_currency_mismatch_fails_closed_without_frozen_fx_snapshot():
    with pytest.raises(ValueError, match="exchange-rate snapshot"):
        tighten_policy_envelopes(
            PolicyEnvelope(currency="USD"), PolicyEnvelope(currency="EUR")
        )


@pytest.mark.parametrize(
    "secret_ref",
    ["sk-this-is-a-secret-value", "env:", "env:HAS SPACE", "https://example.test/key"],
)
def test_provider_rejects_secret_values_and_unapproved_reference_forms(secret_ref):
    with pytest.raises(ValidationError):
        ProviderSpec(
            id="primary",
            adapter="openai_responses",
            secret_ref=secret_ref,
            enabled=True,
        )


def test_direct_validation_error_hides_rejected_secret_input():
    secret = "sk-not-for-logs"
    with pytest.raises(ValidationError) as raised:
        ProviderSpec(
            id="primary",
            adapter="openai_responses",
            secret_ref=secret,
            enabled=True,
        )

    assert secret not in str(raised.value)


@pytest.mark.parametrize("secret_ref", ["env:API_KEY", "keyring:service/account", "plugin:tool/credential"])
def test_provider_accepts_secret_references(secret_ref):
    assert provider(secret_ref=secret_ref).secret_ref == secret_ref


def test_price_uses_integer_minor_units_and_timezone_aware_window():
    assert price().currency == "USD"
    with pytest.raises(ValidationError):
        PriceSpec(
            currency="USD",
            input_minor_per_million=1.5,
            output_minor_per_million=2,
            max_tool_cost_minor=0,
            estimator_id="tokenizer-v1",
            effective_from=NOW,
            expires_at=NOW + timedelta(days=1),
        )
    with pytest.raises(ValidationError):
        PriceSpec(
            currency="USD",
            input_minor_per_million=1,
            output_minor_per_million=2,
            max_tool_cost_minor=0,
            estimator_id="tokenizer-v1",
            effective_from="2026-09-22T00:00:00",
            expires_at=NOW + timedelta(days=1),
        )


def test_paid_models_require_price_and_output_must_fit_context():
    with pytest.raises(ValidationError, match="paid models require"):
        ModelSpec(
            id="missing-price",
            provider="primary",
            remote_model="remote-a",
            tier="standard",
            capabilities={"text"},
            context_window=10,
            max_output_tokens=5,
            supported_reasoning_efforts={"none"},
        )
    with pytest.raises(ValidationError, match="cannot exceed context_window"):
        ModelSpec(**{**model().model_dump(), "context_window": 1})


def test_manifest_validates_provider_references_and_is_content_addressed():
    first = ModelRegistryManifest(
        providers=(provider("zeta"), provider("alpha")),
        models=(model("model-z", "zeta"), model("model-a", "alpha")),
    )
    reordered = ModelRegistryManifest(
        providers=(provider("alpha"), provider("zeta")),
        models=(model("model-a", "alpha"), model("model-z", "zeta")),
    )

    assert first.content_hash.startswith("sha256:")
    assert len(first.content_hash.removeprefix("sha256:")) == 64
    assert first.content_hash == reordered.content_hash
    assert "ORCHESTRATOR_TEST_KEY" in first.canonical_json()
    assert "sk-" not in first.canonical_json()


def test_manifest_rejects_duplicate_ids_unknown_provider_and_extra_secret_field():
    with pytest.raises(ValidationError, match="provider ids must be unique"):
        ModelRegistryManifest(
            providers=(provider(), provider()),
            models=(model(),),
        )
    with pytest.raises(ValidationError, match="unknown providers"):
        ModelRegistryManifest(providers=(provider(),), models=(model(provider_id="missing"),))
    with pytest.raises(ValidationError):
        ProviderSpec(
            id="primary",
            adapter="openai_responses",
            secret_ref="env:API_KEY",
            enabled=True,
            api_key="sk-plain-text",
        )
