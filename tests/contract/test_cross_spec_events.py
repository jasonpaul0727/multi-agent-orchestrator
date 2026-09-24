from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit
from orchestrator.persistence import EventContractError, EventDraft, SQLiteEventStore


def _context(**overrides):
    return {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "fencing_generation": 1,
        "correlation_id": "corr-1",
        "causation_id": "event-parent",
        **overrides,
    }


def _domain_event(event_type, payload, **context_overrides):
    return EventDraft(event_type, payload, **_context(**context_overrides))


@pytest.mark.parametrize(
    "event_type",
    [
        "PolicyDecision",
        "CapabilityGrant",
        "ApprovalGrantConsumed",
        "RoutingDecision",
        "EffectIntentRecorded",
        "EffectReceiptRecorded",
    ],
)
def test_security_and_execution_events_require_complete_causal_identity(event_type):
    with pytest.raises(ValidationError, match="requires complete execution context"):
        EventDraft(event_type, {"effect_id": "effect-1"}, run_id="run-1")


def test_budget_events_allow_standalone_accounting_but_reject_partial_attempt_context():
    EventDraft("BudgetReserved", {"run_id": "run-1", "reservation_id": "r-1"})

    with pytest.raises(ValidationError, match="requires complete execution context"):
        EventDraft(
            "BudgetReserved",
            {"run_id": "run-1", "reservation_id": "r-1"},
            attempt_id="attempt-1",
        )


def test_effect_receipt_requires_a_prior_intent_before_append(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    receipt = _domain_event(
        "EffectReceiptRecorded",
        {"effect_id": "effect-1", "receipt_id": "receipt-1"},
    )

    with pytest.raises(EventContractError, match="prior EffectIntentRecorded"):
        store.append("run", "run-1", 0, [receipt], "receipt-without-intent")

    assert store.current_version("run", "run-1") == 0


@pytest.mark.parametrize(
    ("event_type", "payload", "message"),
    [
        ("EffectIntentRecorded", {}, "requires effect_id"),
        ("ApprovalGrantConsumed", {}, "requires approval_grant_id"),
    ],
)
def test_causal_events_reject_missing_resource_identifiers(
    tmp_path, event_type, payload, message
):
    store = SQLiteEventStore(tmp_path / f"{event_type}.db")
    with pytest.raises(EventContractError, match=message):
        store.append("run", "run-1", 0, [_domain_event(event_type, payload)], event_type)


def test_approval_consumption_without_reservation_is_rejected(tmp_path):
    store = SQLiteEventStore(tmp_path / "unpaired-approval.db")
    approval = _domain_event(
        "ApprovalGrantConsumed", {"grant_id": "grant-1"}
    )
    with pytest.raises(EventContractError, match="must be paired"):
        store.append("run", "run-1", 0, [approval], "approval-only")


def test_approval_consumption_can_be_atomic_with_its_effect_intent(tmp_path):
    store = SQLiteEventStore(tmp_path / "approved-effect.db")
    intent = _domain_event(
        "EffectIntentRecorded",
        {"effect_id": "effect-1", "approval_grant_id": "grant-1", "request_hash": "abc"},
    )
    consumed = _domain_event(
        "ApprovalGrantConsumed",
        {"approval_grant_id": "grant-1", "effect_id": "effect-1"},
    )

    stored = store.append("security", "run-1", 0, [intent, consumed], "approved-effect")

    assert [event.event_type for event in stored] == ["EffectIntentRecorded", "ApprovalGrantConsumed"]


def test_approval_effect_intent_must_precede_consumption_and_match_fencing(tmp_path):
    store = SQLiteEventStore(tmp_path / "approved-effect-order.db")
    consumed = _domain_event(
        "ApprovalGrantConsumed",
        {"approval_grant_id": "grant-1", "effect_id": "effect-1"},
    )
    intent = _domain_event(
        "EffectIntentRecorded",
        {"effect_id": "effect-1", "approval_grant_id": "grant-1"},
    )
    with pytest.raises(EventContractError, match="must precede"):
        store.append("security", "run-1", 0, [consumed, intent], "wrong-order")

    wrong_fence = _domain_event(
        "EffectIntentRecorded",
        {"effect_id": "effect-1", "approval_grant_id": "grant-1"},
        fencing_generation=4,
    )
    with pytest.raises(EventContractError, match="share attempt_id and fencing_generation"):
        store.append("security", "run-1", 0, [wrong_fence, consumed], "wrong-fence")


def test_approval_gated_reservation_requires_consumption_and_matching_fence(tmp_path):
    store = SQLiteEventStore(tmp_path / "approval-fence.db")
    reservation_payload = {
        "approval_grant_id": "grant-1",
        "reservation_id": "reservation-1",
    }
    with pytest.raises(EventContractError, match="requires prior"):
        store.append(
            "run",
            "run-1",
            0,
            [_domain_event("BudgetReserved", reservation_payload)],
            "reservation-only",
        )

    approval = _domain_event(
        "ApprovalGrantConsumed", {"approval_grant_id": "grant-1"}
    )
    reservation = _domain_event(
        "BudgetReserved", reservation_payload, fencing_generation=2
    )
    with pytest.raises(EventContractError, match="share fencing_generation"):
        store.append(
            "run", "run-1", 0, [approval, reservation], "fence-mismatch"
        )


def test_effect_intent_and_receipt_must_share_attempt_and_fencing_generation(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    intent = _domain_event(
        "EffectIntentRecorded",
        {"effect_id": "effect-1", "request_hash": "abc"},
    )
    store.append("run", "run-1", 0, [intent], "intent-1")

    mismatched_receipt = _domain_event(
        "EffectReceiptRecorded",
        {"effect_id": "effect-1", "receipt_id": "receipt-1"},
        attempt_id="attempt-2",
    )
    with pytest.raises(EventContractError, match="must match its intent"):
        store.append("run", "run-1", 1, [mismatched_receipt], "receipt-mismatch")

    receipt = _domain_event(
        "EffectReceiptRecorded",
        {"effect_id": "effect-1", "receipt_id": "receipt-1"},
    )
    stored = store.append("run", "run-1", 1, [receipt], "receipt-1")
    assert stored[0].causation_id == "event-parent"
    assert store.current_version("run", "run-1") == 2


def test_approval_consumption_and_budget_reservation_are_atomic_and_attempt_bound(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    approval = _domain_event(
        "ApprovalGrantConsumed",
        {"approval_grant_id": "grant-1", "effect_id": "effect-1"},
    )
    reservation = _domain_event(
        "BudgetReserved",
        {
            "approval_grant_id": "grant-1",
            "reservation_id": "reservation-1",
            "run_id": "run-1",
        },
    )

    stored = store.append(
        "run", "run-1", 0, [approval, reservation], "approved-reservation"
    )

    assert [event.attempt_id for event in stored] == ["attempt-1", "attempt-1"]
    assert [event.fencing_generation for event in stored] == [1, 1]


def test_approval_and_reservation_attempt_mismatch_rejects_entire_append(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    approval = _domain_event(
        "ApprovalGrantConsumed",
        {"approval_grant_id": "grant-1"},
    )
    reservation = _domain_event(
        "BudgetReserved",
        {
            "approval_grant_id": "grant-1",
            "reservation_id": "reservation-1",
            "run_id": "run-1",
        },
        attempt_id="attempt-2",
    )

    with pytest.raises(EventContractError, match="share attempt_id"):
        store.append("run", "run-1", 0, [approval, reservation], "mismatch")

    assert store.read_stream("run", "run-1") == []


def test_budget_ledger_persists_execution_context_on_reserve_and_settlement(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.db")
    ledger = BudgetLedger(
        store,
        run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
    )
    reservation = ledger.reserve(
        "run-1",
        CostEstimate(amount_minor=10, currency="USD", token_limit=10),
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=4,
        correlation_id="corr-1",
        causation_id="routing-decision-1",
    )
    ledger.commit_usage(
        reservation.reservation_id,
        {"input_tokens": 2, "cost_minor": 3},
        settlement_key="provider-call-1",
    )

    events = store.read_stream("budget", "run-1")
    assert [event.event_type for event in events] == [
        "BudgetReserved",
        "UsageObserved",
        "CostCommitted",
        "BudgetReleased",
    ]
    assert all(event.run_id == "run-1" for event in events)
    assert all(event.node_id == "node-1" for event in events)
    assert all(event.attempt_id == "attempt-1" for event in events)
    assert all(event.fencing_generation == 4 for event in events)
    assert all(event.causation_id == "routing-decision-1" for event in events)


def test_execution_context_survives_store_reopen(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    stored = store.append(
        "run",
        "run-1",
        0,
        [
            _domain_event(
                "RoutingDecision",
                {"provider": "test", "model": "model-1"},
            )
        ],
        "route-1",
    )[0]
    store.close()

    reopened = SQLiteEventStore(database)
    restored = reopened.read_stream("run", "run-1")[0]
    assert restored == stored
    assert restored.correlation_id == "corr-1"
    assert restored.causation_id == "event-parent"
