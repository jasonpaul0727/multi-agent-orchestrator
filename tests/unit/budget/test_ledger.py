from __future__ import annotations

import pytest
from concurrent.futures import ThreadPoolExecutor

from orchestrator.budget import (
    AmbiguousReservation,
    BudgetExhausted,
    BudgetLedger,
    BudgetLimitMismatch,
    BudgetReservation,
    BudgetReleasedError,
    CostEstimate,
    CurrencyMismatch,
    IdempotencyConflict,
    ReservedKeyError,
    RunLimit,
    ReservationStateError,
    UsageRecord,
)
from orchestrator.persistence.events import EventDraft
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from pydantic import ValidationError


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
        "estimator_snapshot_id": "unspecified",
        "input_price_minor_per_million": 1,
        "input_tokens": 101,
        "max_cached_input_tokens": None,
        "max_cost_minor": 100,
        "max_input_tokens": None,
        "max_output_tokens": None,
        "max_reasoning_tokens": None,
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
        "reserved_cached_input_tokens": 0,
        "reserved_input_tokens": 101,
        "reserved_output_tokens": 9,
        "reserved_reasoning_tokens": 0,
        "run_id": "run-1",
        "snapshot_id": "unspecified",
        "status": "reserved",
        "tokenizer_snapshot_id": "unspecified",
        "tool_fee_minor": 0,
    }


def test_explicit_idempotency_retry_without_reservation_id_reuses_original(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    estimate = estimate_with_worst_case(7)
    first = ledger.reserve("run-1", estimate, idempotency_key="request-1")
    second = ledger.reserve("run-1", estimate, idempotency_key="request-1")
    assert second == first
    assert len(ledger.read("run-1")) == 1


def test_reserved_token_breakdown_and_override_must_fit_total(tmp_path):
    with pytest.raises(ValidationError):
        BudgetReservation(
            reservation_id="r",
            run_id="run-1",
            reserved_minor=1,
            reserved_tokens=3,
            reserved_input_tokens=2,
            reserved_output_tokens=2,
            currency="USD",
        )
    ledger = BudgetLedger(
        SQLiteEventStore(tmp_path / "events.db"),
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    estimate = CostEstimate(
        amount_minor=1,
        currency="USD",
        token_limit=4,
        input_tokens=3,
        output_tokens=1,
        snapshot_id="test",
    )
    with pytest.raises(ValueError):
        ledger.reserve("run-1", estimate, token_limit=3)


def test_used_class_tokens_exhaust_cap_after_commit(tmp_path):
    ledger = BudgetLedger(
        SQLiteEventStore(tmp_path / "events.db"),
        run_limits={
            "run-1": RunLimit(
                max_cost_minor=100,
                max_tokens=100,
                max_input_tokens=3,
            )
        },
    )
    reservation = ledger.reserve(
        "run-1",
        CostEstimate(
            amount_minor=1,
            currency="USD",
            token_limit=3,
            input_tokens=3,
            snapshot_id="test",
        ),
    )
    ledger.commit_usage(reservation.reservation_id, {"input": 3})
    with pytest.raises(BudgetExhausted):
        ledger.reserve(
            "run-1",
            CostEstimate(
                amount_minor=1,
                currency="USD",
                token_limit=1,
                input_tokens=1,
                snapshot_id="test",
            ),
        )


def test_reservation_run_id_with_separator_is_resolved_from_persisted_stream(tmp_path):
    database = tmp_path / "events.db"
    first_store = SQLiteEventStore(database)
    first = BudgetLedger(
        first_store,
        run_limits={"tenant::run": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = first.reserve("tenant::run", estimate_with_worst_case(5))
    first_store.close()
    reopened_store = SQLiteEventStore(database)
    reopened = BudgetLedger.reopen(reopened_store)
    committed = reopened.commit_usage(
        reservation.reservation_id,
        {"input": 1},
        settlement_key="provider-key",
    )
    assert committed.run_id == "tenant::run"


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


def test_reconcile_unknown_rejects_an_ordinarily_reserved_reservation(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case())
    with pytest.raises(ReservationStateError):
        ledger.reconcile_unknown(
            reservation.reservation_id,
            {"input": 1},
            "provider-reconciliation-1",
        )


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


def test_reconcile_unknown_rejects_prior_ordinary_settlement(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case())
    ledger.commit_usage(
        reservation.reservation_id,
        {"input": 1},
        settlement_key="provider-ordinary",
    )

    with pytest.raises(ReservationStateError):
        ledger.reconcile_unknown(
            reservation.reservation_id,
            {"input": 1},
            "provider-ordinary",
        )


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


def test_unknown_retry_with_changed_reason_conflicts(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(4))
    ledger.mark_unknown(reservation.reservation_id, reason="provider-timeout")
    # The same internal key is deliberately reused, but the reason is part
    # of the CostAdjusted request fingerprint.
    with pytest.raises(IdempotencyConflict):
        ledger.mark_unknown(reservation.reservation_id, reason="worker-crash")


def test_release_key_is_namespaced_away_from_caller_reservation_key(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
    )
    with pytest.raises(ReservedKeyError):
        ledger.reserve(
            "run-1",
            estimate_with_worst_case(4),
            reservation_id="reservation-1",
            idempotency_key="release:reservation-1",
        )
    reservation = ledger.reserve(
        "run-1",
        estimate_with_worst_case(4),
        reservation_id="reservation-1",
        idempotency_key="caller-reservation-1",
    )
    released = ledger.release(reservation.reservation_id)
    assert released.status == "released"
    assert [event.event_type for event in ledger.read("run-1")] == [
        "BudgetReserved",
        "BudgetReleased",
    ]


@pytest.mark.parametrize(
    "prefix",
    ["reserve:", "unknown:", "settle:", "release:", "reconcile:"],
)
def test_caller_reservation_keys_cannot_enter_internal_namespaces(tmp_path, prefix):
    ledger = BudgetLedger(
        SQLiteEventStore(tmp_path / "events.db"),
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    with pytest.raises(ReservedKeyError):
        ledger.reserve(
            "run-1",
            estimate_with_worst_case(1),
            idempotency_key=f"{prefix}caller",
        )


@pytest.mark.parametrize(
    "prefix",
    ["reserve:", "unknown:", "settle:", "release:", "reconcile:"],
)
def test_caller_settlement_keys_cannot_enter_internal_namespaces(tmp_path, prefix):
    ledger = BudgetLedger(
        SQLiteEventStore(tmp_path / "events.db"),
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(1))
    with pytest.raises(ReservedKeyError):
        ledger.commit_usage(
            reservation.reservation_id,
            {"input": 1},
            settlement_key=f"{prefix}caller",
        )


@pytest.mark.parametrize(
    "prefix",
    ["reserve:", "unknown:", "settle:", "release:", "reconcile:"],
)
def test_caller_reconciliation_keys_cannot_enter_internal_namespaces(tmp_path, prefix):
    ledger = BudgetLedger(
        SQLiteEventStore(tmp_path / "events.db"),
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(1))
    ledger.mark_unknown(reservation.reservation_id)
    with pytest.raises(ReservedKeyError):
        ledger.reconcile_unknown(
            reservation.reservation_id,
            {"input": 1},
            f"{prefix}caller",
        )


class _NoStreamIndexStore:
    """A legacy EventStore adapter with no stream enumeration or transactions."""

    def __init__(self, delegate, *, lose_append_response=False):
        self.delegate = delegate
        self.lose_append_response = lose_append_response

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        result = self.delegate.append(
            stream_type, stream_id, expected_version, events, idempotency_key
        )
        return [] if self.lose_append_response else result

    def read_stream(self, stream_type, stream_id, after_version=0):
        return self.delegate.read_stream(stream_type, stream_id, after_version)

    def current_version(self, stream_type, stream_id):
        return self.delegate.current_version(stream_type, stream_id)


def test_reopened_auto_id_lookup_uses_persisted_run_index_without_stream_ids(tmp_path):
    database = tmp_path / "events.db"
    first_store = SQLiteEventStore(database)
    first = BudgetLedger(
        _NoStreamIndexStore(first_store, lose_append_response=True),
        run_limits={"tenant::run": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = first.reserve(
        "tenant::run", estimate_with_worst_case(3), idempotency_key="caller-request"
    )
    assert first.reserve(
        "tenant::run", estimate_with_worst_case(3), idempotency_key="caller-request"
    ) == reservation
    assert reservation.reservation_id.startswith("budget:")
    first_store.close()

    reopened = BudgetLedger.reopen(_NoStreamIndexStore(SQLiteEventStore(database)))
    committed = reopened.commit_usage(reservation.reservation_id, {"input": 1})
    assert committed.run_id == "tenant::run"


def test_omitted_run_id_rejects_ambiguous_indexed_reservation(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    limit = RunLimit(max_cost_minor=100, max_tokens=100)
    estimate = estimate_with_worst_case(2)
    reservation_id = "budget:cnVuLTE:same"
    store.append(
        "budget",
        "run-1",
        0,
        [
            EventDraft(
                "BudgetReserved",
                BudgetLedger._reservation_payload(
                    "run-1", reservation_id, estimate, 2, limit
                ),
            )
        ],
        "fixture-run-1",
    )
    store.append(
        "budget",
        "run-2",
        0,
        [
            EventDraft(
                "BudgetReserved",
                BudgetLedger._reservation_payload(
                    "run-2", reservation_id, estimate, 2, limit
                ),
            )
        ],
        "fixture-run-2",
    )

    with pytest.raises(AmbiguousReservation):
        BudgetLedger(store).get_reservation(reservation_id)


def test_legacy_raw_reservation_key_reuses_event_without_duplicate(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    legacy_payload = {
        "amount_minor": 3,
        "cached_input_price_minor_per_million": 0,
        "cached_input_tokens": 0,
        "currency": "USD",
        "input_price_minor_per_million": 0,
        "input_tokens": 3,
        "max_cost_minor": 100,
        "max_tokens": 100,
        "output_price_minor_per_million": 0,
        "output_tokens": 0,
        "price_snapshot_id": "unspecified",
        "provider_fee_minor": 0,
        "reasoning_price_minor_per_million": 0,
        "reasoning_tokens": 0,
        "reservation_id": "run-1::legacy-id",
        "reserved_minor": 3,
        "reserved_tokens": 3,
        "run_id": "run-1",
        "snapshot_id": "test",
        "status": "reserved",
        "tokenizer_snapshot_id": "unspecified",
        "tool_fee_minor": 0,
    }
    store.append(
        "budget",
        "run-1",
        0,
        [EventDraft("BudgetReserved", legacy_payload)],
        "legacy-request",
    )
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )

    estimate = CostEstimate(
        amount_minor=3,
        currency="USD",
        token_limit=3,
        input_tokens=3,
        snapshot_id="test",
    )
    reservation = ledger.reserve(
        "run-1", estimate, idempotency_key="legacy-request"
    )
    assert reservation.reservation_id == "run-1::legacy-id"
    with pytest.raises(IdempotencyConflict):
        ledger.reserve(
            "run-1",
            CostEstimate(
                amount_minor=4,
                currency="USD",
                token_limit=3,
                input_tokens=3,
                snapshot_id="test",
            ),
            idempotency_key="legacy-request",
        )
    assert len(ledger.read("run-1")) == 1


def test_unknown_cannot_commit_but_explicit_reconciliation_settles(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(), token_limit=100)
    ledger.mark_unknown(reservation.reservation_id)

    with pytest.raises(ReservationStateError) as failure:
        ledger.commit_usage(reservation.reservation_id, {"input": 1})
    assert "reconcile_unknown" in str(failure.value)

    reconciled = ledger.reconcile_unknown(
        reservation.reservation_id,
        {"input": 1, "cost_minor": 3},
        "provider-reconciliation-1",
    )
    retried = ledger.reconcile_unknown(
        reservation.reservation_id,
        {"input": 1, "cost_minor": 3},
        "provider-reconciliation-1",
    )
    assert reconciled.status == "committed"
    assert retried == reconciled
    assert ledger.available("run-1").unknown_minor == 0
    assert [event.event_type for event in ledger.read("run-1")][-4:] == [
        "UsageObserved",
        "CostCommitted",
        "BudgetReleased",
        "CostAdjusted",
    ]


def test_empty_keys_are_rejected_without_fallback(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
    )
    estimate = estimate_with_worst_case(1)
    with pytest.raises(ValueError):
        ledger.reserve("run-1", estimate, idempotency_key="")
    reservation = ledger.reserve("run-1", estimate)
    with pytest.raises(ValueError):
        ledger.commit_usage(reservation.reservation_id, {}, settlement_key="")
    ledger.mark_unknown(reservation.reservation_id)
    with pytest.raises(ValueError):
        ledger.reconcile_unknown(reservation.reservation_id, {}, "")


def test_reservation_key_fingerprint_and_explicit_id_are_unique(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    first = ledger.reserve(
        "run-1",
        estimate_with_worst_case(5),
        reservation_id="reservation-1",
        idempotency_key="request-1",
    )
    assert ledger.reserve(
        "run-1",
        estimate_with_worst_case(5),
        reservation_id="reservation-1",
        idempotency_key="request-1",
    ) == first
    with pytest.raises(IdempotencyConflict):
        ledger.reserve(
            "run-1",
            estimate_with_worst_case(6),
            reservation_id="reservation-1",
            idempotency_key="request-1",
        )
    with pytest.raises(IdempotencyConflict):
        ledger.reserve(
            "run-1",
            estimate_with_worst_case(5),
            reservation_id="reservation-1",
            idempotency_key="request-2",
        )


def test_usage_record_identity_must_match_target(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(5), token_limit=5)
    with pytest.raises(ReservationStateError):
        ledger.commit_usage(
            reservation.reservation_id,
            {"reservation_id": "other", "input": 1},
        )
    with pytest.raises(ReservationStateError):
        ledger.commit_usage(
            reservation.reservation_id,
            {"run_id": "other", "input": 1},
        )
    with pytest.raises(ReservationStateError):
        ledger.commit_usage(
            reservation.reservation_id,
            UsageRecord(
                reservation_id=reservation.reservation_id,
                run_id="run-1",
                settlement_key="other-key",
                currency="USD",
            ),
        )


def test_settlement_key_reuse_on_another_reservation_conflicts(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    first = ledger.reserve(
        "run-1",
        estimate_with_worst_case(5),
        reservation_id="reservation-1",
    )
    second = ledger.reserve(
        "run-1",
        estimate_with_worst_case(5),
        reservation_id="reservation-2",
    )
    ledger.commit_usage(first.reservation_id, {"input": 1}, settlement_key="same-key")
    with pytest.raises(IdempotencyConflict):
        ledger.commit_usage(second.reservation_id, {"input": 1}, settlement_key="same-key")


def test_all_run_token_caps_and_max_total_tokens_alias_are_enforced(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={
            "run-1": RunLimit(
                max_total_tokens=10,
                max_input_tokens=2,
                max_output_tokens=2,
                max_reasoning_tokens=2,
                max_cached_input_tokens=2,
            )
        },
    )
    estimate = CostEstimate(
        amount_minor=1,
        currency="USD",
        token_limit=4,
        input_tokens=3,
        output_tokens=1,
        snapshot_id="test",
    )
    with pytest.raises(BudgetExhausted):
        ledger.reserve("run-1", estimate)


def test_reopen_derives_persisted_envelope_and_rejects_mismatch(tmp_path):
    database = tmp_path / "events.db"
    first_store = SQLiteEventStore(database)
    first = BudgetLedger(
        first_store,
        run_limits={
            "run-1": RunLimit(
                max_cost_minor=10,
                max_total_tokens=10,
                max_input_tokens=4,
                currency="USD",
            )
        },
    )
    first.reserve(
        "run-1",
        CostEstimate(amount_minor=1, currency="USD", token_limit=1, snapshot_id="test"),
    )
    first_store.close()
    reopened_store = SQLiteEventStore(database)
    reopened = BudgetLedger.reopen(reopened_store)
    assert reopened.available("run-1").max_input_tokens == 4
    with pytest.raises(BudgetLimitMismatch):
        BudgetLedger(reopened_store, {"run-1": RunLimit(max_cost_minor=11, max_tokens=10)})


def test_release_after_commit_returns_committed_without_extra_event(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve("run-1", estimate_with_worst_case(5), token_limit=5)
    ledger.commit_usage(reservation.reservation_id, {"input": 1}, settlement_key="s-1")
    event_count = len(ledger.read("run-1"))
    result = ledger.release(reservation.reservation_id)
    assert result.status == "committed"
    assert len(ledger.read("run-1")) == event_count


def test_concurrent_reservations_never_cross_cost_envelope(tmp_path):
    database = tmp_path / "events.db"
    estimate = estimate_with_worst_case(1)

    def attempt(index: int):
        store = SQLiteEventStore(database)
        try:
            ledger = BudgetLedger(
                store,
                run_limits={"run-1": RunLimit(max_cost_minor=3, max_tokens=100)},
            )
            return ledger.reserve(
                "run-1",
                estimate,
                reservation_id=f"r-{index}",
                idempotency_key=f"reserve-{index}",
            )
        except Exception as exc:
            return exc
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(attempt, range(8)))
    assert sum(not isinstance(result, Exception) for result in results) == 3
    check = SQLiteEventStore(database)
    assert check.current_version("budget", "run-1") == 3


def test_concurrent_settlements_cannot_cross_cost_envelope(tmp_path):
    database = tmp_path / "events.db"
    initial = SQLiteEventStore(database)
    setup = BudgetLedger(
        initial,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservations = [
        setup.reserve(
            "run-1",
            CostEstimate(amount_minor=40, currency="USD", token_limit=1, snapshot_id="test"),
            reservation_id=f"r-{index}",
        )
        for index in range(2)
    ]
    initial.close()

    def settle(reservation):
        store = SQLiteEventStore(database)
        try:
            return BudgetLedger(store).commit_usage(
                reservation.reservation_id,
                {"cost_minor": 60},
                settlement_key=f"settle-{reservation.reservation_id}",
            )
        except Exception as exc:
            return exc
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(settle, reservations))
    assert sum(not isinstance(result, Exception) for result in results) == 1
