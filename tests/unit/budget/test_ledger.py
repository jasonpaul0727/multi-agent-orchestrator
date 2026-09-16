from __future__ import annotations

import pytest

from orchestrator.budget import (
    BudgetExhausted,
    BudgetLedger,
    BudgetReleasedError,
    CostEstimate,
    CurrencyMismatch,
    RunLimit,
)
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


def estimate_with_worst_case(tokens: int = 10) -> CostEstimate:
    return CostEstimate(
        amount_minor=tokens,
        currency="USD",
        token_limit=tokens,
        snapshot_id="test",
    )


def test_cost_rounds_up_and_reservation_is_atomic(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=200)},
    )
    estimate = ledger.estimate(
        input_tokens=101,
        output_tokens=9,
        input_price_minor_per_million=1,
        output_price_minor_per_million=3,
        currency="USD",
    )

    assert estimate.amount_minor == 1
    reservation = ledger.reserve("run-1", estimate, token_limit=200)
    assert reservation.reserved_minor == 1
    assert reservation.reserved_tokens == 200
    events = ledger.read("run-1")
    assert len(events) == 1
    assert events[0].event_type == "BudgetReserved"
    assert events[0].payload == {
        "amount_minor": 1,
        "cached_input_price_minor_per_million": 0,
        "cached_input_tokens": 0,
        "currency": "USD",
        "input_price_minor_per_million": 1,
        "input_tokens": 101,
        "max_cost_minor": 100,
        "max_tokens": 200,
        "output_price_minor_per_million": 3,
        "output_tokens": 9,
        "price_snapshot_id": "unspecified",
        "provider_fee_minor": 0,
        "reasoning_price_minor_per_million": 0,
        "reasoning_tokens": 0,
        "reservation_id": reservation.reservation_id,
        "reserved_minor": 1,
        "reserved_tokens": 200,
        "run_id": "run-1",
        "snapshot_id": "unspecified",
        "status": "reserved",
        "tokenizer_snapshot_id": "unspecified",
        "tool_fee_minor": 0,
    }


def test_estimate_includes_token_classes_and_fixed_fees_with_one_final_ceiling():
    estimate = BudgetLedger.estimate(
        input_tokens=1,
        output_tokens=1,
        reasoning_tokens=1,
        cached_input_tokens=1,
        input_price_minor_per_million=1_000_000,
        output_price_minor_per_million=2_000_000,
        reasoning_price_minor_per_million=3_000_000,
        cached_input_price_minor_per_million=4_000_000,
        provider_fee_minor=5,
        tool_fee_minor=7,
        currency="USD",
    )
    assert estimate.amount_minor == 22
    assert estimate.token_limit == 4


def test_unknown_result_keeps_worst_case_reservation(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(), token_limit=100)

    unknown = ledger.mark_unknown(reservation.reservation_id)

    assert unknown.status == "unknown"
    assert ledger.available("run-1").unknown_minor == reservation.reserved_minor
    assert ledger.available("run-1").available_minor == 90
    assert ledger.read("run-1")[-1].event_type == "CostAdjusted"


def test_settlement_is_idempotent_and_releases_remainder(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(), token_limit=100)

    first = ledger.commit_usage(
        reservation.reservation_id,
        usage={"input": 2, "output": 3},
        settlement_key="provider-request-1",
    )
    second = ledger.commit_usage(
        reservation.reservation_id,
        usage={"input": 2, "output": 3},
        settlement_key="provider-request-1",
    )

    assert second == first
    assert first.status == "committed"
    assert ledger.available("run-1").released_tokens == 95
    assert ledger.available("run-1").used_minor == first.cost_minor
    assert [event.event_type for event in ledger.read("run-1")] == [
        "BudgetReserved",
        "UsageObserved",
        "CostCommitted",
        "BudgetReleased",
    ]


def test_budget_exhaustion_is_rejected_without_events(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
    )
    ledger.reserve("run-1", estimate_with_worst_case(10), token_limit=10)

    with pytest.raises(BudgetExhausted):
        ledger.reserve("run-1", estimate_with_worst_case(1), token_limit=1)
    assert len(ledger.read("run-1")) == 1


def test_currency_mismatch_is_rejected(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10, currency="USD")},
    )

    with pytest.raises(CurrencyMismatch):
        ledger.reserve(
            "run-1",
            CostEstimate(amount_minor=1, currency="EUR", token_limit=1, snapshot_id="x"),
        )


def test_release_and_reopen_replay_persisted_balance(tmp_path):
    database = tmp_path / "events.db"
    first_store = SQLiteEventStore(database)
    ledger = BudgetLedger(
        first_store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(4), token_limit=4)
    released = ledger.release(reservation.reservation_id)
    first_store.close()

    reopened = SQLiteEventStore(database)
    replayed = BudgetLedger(reopened).available("run-1")
    assert released.status == "released"
    assert replayed.released_minor == 4
    assert replayed.available_minor == 10


def test_unknown_cannot_be_released_implicitly(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(4), token_limit=4)
    ledger.mark_unknown(reservation.reservation_id)

    with pytest.raises(BudgetReleasedError):
        ledger.release(reservation.reservation_id)
