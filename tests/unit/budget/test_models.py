import pytest
from pydantic import ValidationError

from orchestrator.budget import BudgetBalance, BudgetReservation, CostEstimate, RunLimit, UsageRecord


def test_run_limit_requires_at_least_one_nonnegative_limit_and_normalizes_currency():
    with pytest.raises(ValidationError, match="must define"):
        RunLimit()
    with pytest.raises(ValidationError):
        RunLimit(max_cost_minor=-1)
    with pytest.raises(ValidationError, match="three-letter"):
        RunLimit(max_tokens=10, currency="dollars")

    limit = RunLimit(max_amount_minor=20, max_total_tokens=30, currency=" usd ")
    assert limit.currency == "USD"
    assert limit.max_amount_minor == limit.max_cost_minor == 20
    assert limit.max_total_tokens == limit.max_tokens == 30


def test_cost_estimate_aliases_derive_total_tokens_and_validate_prices():
    estimate = CostEstimate(
        cost_minor=4,
        currency="usd",
        input_tokens=2,
        output_tokens=3,
        cached_tokens=1,
        cached_input_price_minor_per_million=5,
    )
    assert estimate.cost_minor == estimate.amount_minor == 4
    assert estimate.total_tokens == estimate.token_limit == 6
    assert estimate.cached_input_tokens == 1

    with pytest.raises(ValidationError, match="cannot be lower"):
        CostEstimate(amount_minor=1, currency="USD", token_limit=1, input_tokens=2)
    with pytest.raises(ValidationError, match="snapshot_id"):
        CostEstimate(amount_minor=1, currency="USD", snapshot_id=" ")
    with pytest.raises(ValidationError):
        CostEstimate(amount_minor=-1, currency="USD")


def test_reservation_optional_context_and_token_breakdown_are_validated():
    reservation = BudgetReservation(
        reservation_id="res-1",
        run_id="run-1",
        reserved_cost_minor=7,
        token_limit=8,
        reserved_input_tokens=5,
        reserved_output_tokens=3,
        currency="USD",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=2,
    )
    assert reservation.amount_minor == 7
    assert reservation.token_limit == reservation.reserved_tokens == 8
    assert reservation.version == reservation.reservation_version == 1
    assert reservation.node_id == "node-1"

    with pytest.raises(ValidationError, match="reserved token breakdown"):
        BudgetReservation(
            reservation_id="res-2",
            run_id="run-1",
            reserved_minor=1,
            reserved_tokens=1,
            reserved_input_tokens=2,
            currency="USD",
        )
    with pytest.raises(ValidationError, match="must be a non-blank"):
        BudgetReservation(
            reservation_id=" ",
            run_id="run-1",
            reserved_minor=1,
            reserved_tokens=0,
            currency="USD",
        )
    with pytest.raises(ValidationError, match="attempt_id"):
        BudgetReservation(
            reservation_id="res-3",
            run_id="run-1",
            reserved_minor=1,
            reserved_tokens=0,
            currency="USD",
            attempt_id=" ",
        )


def test_usage_record_aliases_identity_and_amount_properties():
    usage = UsageRecord(
        reservation_id="res-1",
        run_id="run-1",
        settlement_key="settle-1",
        currency="usd",
        input=2,
        output=3,
        cached=1,
        actual_minor=4,
    )
    assert usage.total_tokens == 6
    assert usage.amount_minor == usage.actual_minor == usage.cost_minor == 4

    with pytest.raises(ValidationError, match="settlement_key"):
        UsageRecord(
            reservation_id="res-1",
            run_id="run-1",
            settlement_key=" ",
            currency="USD",
        )
    with pytest.raises(ValidationError):
        UsageRecord(
            reservation_id="res-1",
            run_id="run-1",
            settlement_key="settle-1",
            currency="USD",
            input_tokens=-1,
        )


def test_budget_balance_exposes_held_and_available_totals_and_aliases():
    balance = BudgetBalance(
        run_id="run-1",
        currency="usd",
        max_cost_minor=100,
        max_total_tokens=100,
        max_input_tokens=20,
        reserved_minor=10,
        unknown_minor=5,
        used_minor=20,
        reserved_tokens=7,
        unknown_tokens=3,
        used_tokens=30,
        reserved_input_tokens=4,
        unknown_input_tokens=2,
        used_input_tokens=5,
    )
    assert balance.max_total_tokens == balance.max_tokens == 100
    assert balance.held_minor == 15
    assert balance.held_tokens == 10
    assert balance.available_minor == balance.remaining_minor == 65
    assert balance.available_tokens == balance.remaining_tokens == 60
    assert balance.available_input_tokens == 9
    assert balance.available_output_tokens is None
    assert balance.available_reasoning_tokens is None
    assert balance.available_cached_input_tokens is None

    uncapped = BudgetBalance(run_id="run-2", currency="USD")
    assert uncapped.available_minor is None
    assert uncapped.available_tokens is None
    assert uncapped.available_input_tokens is None

    with pytest.raises(ValidationError, match="run_id"):
        BudgetBalance(run_id=" ", currency="USD")
