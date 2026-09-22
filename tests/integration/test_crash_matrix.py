import pytest

from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit
from orchestrator.persistence import EventDraft, SQLiteEventStore


def _context(*, causation_id="event-parent"):
    return {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "fencing_generation": 2,
        "correlation_id": "corr-1",
        "causation_id": causation_id,
    }


def _effect_event(event_type, payload, *, causation_id="event-parent"):
    return EventDraft(event_type, payload, **_context(causation_id=causation_id))


class _CommitThenLoseResponse:
    """Simulate a process losing the reply after a durable append commits."""

    def __init__(self, delegate, key_prefix):
        self.delegate = delegate
        self.key_prefix = key_prefix
        self.lost = False

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def append_checked(self, stream_type, stream_id, idempotency_key, decide):
        result = self.delegate.append_checked(
            stream_type, stream_id, idempotency_key, decide
        )
        if not self.lost and idempotency_key.startswith(self.key_prefix):
            self.lost = True
            raise RuntimeError("simulated process interruption after commit")
        return result


class _CommitThenLoseAppendResponse:
    def __init__(self, delegate, idempotency_key):
        self.delegate = delegate
        self.idempotency_key = idempotency_key
        self.lost = False

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def append(self, stream_type, stream_id, expected_version, events, idempotency_key):
        result = self.delegate.append(
            stream_type, stream_id, expected_version, events, idempotency_key
        )
        if not self.lost and idempotency_key == self.idempotency_key:
            self.lost = True
            raise RuntimeError("simulated response loss after atomic append")
        return result


def test_crash_before_effect_intent_leaves_no_record_or_side_effect(tmp_path):
    database = tmp_path / "before-intent.db"
    store = SQLiteEventStore(database)

    with pytest.raises(RuntimeError, match="injected crash"):
        raise RuntimeError("injected crash before intent append")

    assert store.read_stream("run", "run-1") == []
    store.close()
    reopened = SQLiteEventStore(database)
    assert reopened.read_stream("run", "run-1") == []


def test_crash_after_intent_and_receipt_response_loss_is_idempotent(tmp_path):
    database = tmp_path / "effect-response-loss.db"
    store = SQLiteEventStore(database)
    intent = _effect_event(
        "EffectIntentRecorded",
        {"effect_id": "effect-1", "request_hash": "request-hash"},
    )
    with pytest.raises(RuntimeError, match="response lost"):
        store.append("run", "run-1", 0, [intent], "effect-intent-1")
        raise RuntimeError("response lost after intent commit")
    store.close()

    restarted = SQLiteEventStore(database)
    original_intent = restarted.append(
        "run", "run-1", 0, [intent], "effect-intent-1"
    )[0]
    assert original_intent == restarted.read_stream("run", "run-1")[0]

    # The external operation uses the persisted provider idempotency key, so
    # retrying after restart observes one effect, not a second execution.
    provider_effects = {"provider-key-1": "receipt-1"}
    receipt = _effect_event(
        "EffectReceiptRecorded",
        {"effect_id": "effect-1", "receipt_id": provider_effects["provider-key-1"]},
        causation_id=original_intent.event_id,
    )
    with pytest.raises(RuntimeError, match="response lost"):
        restarted.append("run", "run-1", 1, [receipt], "effect-receipt-1")
        raise RuntimeError("response lost after receipt commit")
    restarted.close()

    recovered = SQLiteEventStore(database)
    original_receipt = recovered.append(
        "run", "run-1", 1, [receipt], "effect-receipt-1"
    )[0]
    events = recovered.read_stream("run", "run-1")
    assert [event.event_type for event in events] == [
        "EffectIntentRecorded",
        "EffectReceiptRecorded",
    ]
    assert original_receipt == events[1]
    assert provider_effects == {"provider-key-1": "receipt-1"}


def test_crash_after_budget_reservation_commit_replays_one_hold(tmp_path):
    database = tmp_path / "budget-response-loss.db"
    initial_store = SQLiteEventStore(database)
    interrupted_store = _CommitThenLoseResponse(initial_store, "reserve:")
    first_ledger = BudgetLedger(
        interrupted_store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    estimate = CostEstimate(amount_minor=12, currency="USD", token_limit=8)

    with pytest.raises(RuntimeError, match="process interruption"):
        first_ledger.reserve(
            "run-1",
            estimate,
            idempotency_key="provider-call-1",
            node_id="node-1",
            attempt_id="attempt-1",
            fencing_generation=2,
            correlation_id="corr-1",
            causation_id="routing-1",
        )
    initial_store.close()

    restarted_store = SQLiteEventStore(database)
    restarted_ledger = BudgetLedger(restarted_store)
    reservation = restarted_ledger.reserve(
        "run-1",
        estimate,
        idempotency_key="provider-call-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=2,
        correlation_id="corr-1",
        causation_id="routing-1",
    )

    assert restarted_store.current_version("budget", "run-1") == 1
    assert reservation.attempt_id == "attempt-1"
    assert restarted_ledger.available("run-1").reserved_minor == 12


@pytest.mark.parametrize("failure_point", ["before_receipt", "after_receipt"])
def test_interruption_around_effect_receipt_keeps_intent_and_single_receipt(
    tmp_path, failure_point
):
    database = tmp_path / f"{failure_point}.db"
    store = SQLiteEventStore(database)
    intent = _effect_event(
        "EffectIntentRecorded",
        {"effect_id": "effect-2", "request_hash": "request-hash"},
    )
    committed_intent = store.append("run", "run-2", 0, [intent], "intent-2")[0]
    store.close()

    recovered = SQLiteEventStore(database)
    receipt = _effect_event(
        "EffectReceiptRecorded",
        {"effect_id": "effect-2", "receipt_id": "receipt-2"},
        causation_id=committed_intent.event_id,
    )
    if failure_point == "before_receipt":
        with pytest.raises(RuntimeError, match="injected crash"):
            raise RuntimeError("injected crash before receipt append")
        assert [event.event_type for event in recovered.read_stream("run", "run-2")] == [
            "EffectIntentRecorded"
        ]
        recovered.append("run", "run-2", 1, [receipt], "receipt-2")
    else:
        with pytest.raises(RuntimeError, match="injected crash"):
            recovered.append("run", "run-2", 1, [receipt], "receipt-2")
            raise RuntimeError("injected crash after receipt append")
        recovered.close()
        recovered = SQLiteEventStore(database)
        recovered.append("run", "run-2", 1, [receipt], "receipt-2")

    events = recovered.read_stream("run", "run-2")
    assert [event.event_type for event in events] == [
        "EffectIntentRecorded",
        "EffectReceiptRecorded",
    ]
    assert events[0] == committed_intent


def test_crash_after_approval_consumption_replays_atomic_pair_once(tmp_path):
    database = tmp_path / "approval-response-loss.db"
    initial = SQLiteEventStore(database)
    interrupted = _CommitThenLoseAppendResponse(initial, "approved-attempt-1")
    context = _context(causation_id="policy-decision-1")
    events = [
        EventDraft(
            "ApprovalGrantConsumed",
            {"approval_grant_id": "grant-1", "effect_id": "effect-1"},
            **context,
        ),
        EventDraft(
            "BudgetReserved",
            {
                "approval_grant_id": "grant-1",
                "reservation_id": "effect-reservation-1",
                "reserved_minor": 5,
                "reserved_tokens": 0,
                "run_id": "run-1",
            },
            **_context(causation_id="approval-consumption-1"),
        ),
    ]
    with pytest.raises(RuntimeError, match="response loss"):
        interrupted.append(
            "run", "run-1", 0, events, "approved-attempt-1"
        )
    initial.close()

    restarted = SQLiteEventStore(database)
    replayed = restarted.append(
        "run", "run-1", 0, events, "approved-attempt-1"
    )
    assert [event.event_type for event in replayed] == [
        "ApprovalGrantConsumed",
        "BudgetReserved",
    ]
    assert restarted.current_version("run", "run-1") == 2


def test_crash_after_settlement_commit_retries_without_double_charge(tmp_path):
    database = tmp_path / "settlement-response-loss.db"
    initial = SQLiteEventStore(database)
    estimate = CostEstimate(amount_minor=10, currency="USD", token_limit=10)
    reservation = BudgetLedger(
        initial,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    ).reserve("run-1", estimate, idempotency_key="reserve-1")
    interrupted = _CommitThenLoseResponse(initial, "settle:")
    with pytest.raises(RuntimeError, match="process interruption"):
        BudgetLedger(interrupted).commit_usage(
            reservation.reservation_id,
            {"input_tokens": 4, "cost_minor": 3},
            settlement_key="provider-settlement-1",
        )
    initial.close()

    restarted = SQLiteEventStore(database)
    ledger = BudgetLedger(restarted)
    first = ledger.commit_usage(
        reservation.reservation_id,
        {"input_tokens": 4, "cost_minor": 3},
        settlement_key="provider-settlement-1",
    )
    second = ledger.commit_usage(
        reservation.reservation_id,
        {"input_tokens": 4, "cost_minor": 3},
        settlement_key="provider-settlement-1",
    )
    assert first == second
    assert ledger.available("run-1").used_minor == 3
    assert [event.event_type for event in restarted.read_stream("budget", "run-1")].count(
        "CostCommitted"
    ) == 1
