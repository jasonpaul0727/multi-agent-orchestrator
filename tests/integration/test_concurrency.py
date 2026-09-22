from concurrent.futures import ThreadPoolExecutor
import threading

from orchestrator.budget import BudgetExhausted, BudgetLedger, CostEstimate, RunLimit
from orchestrator.persistence import EventDraft, SQLiteEventStore, StaleStream


def test_simultaneous_store_initialization_is_safe(tmp_path):
    database = tmp_path / "concurrent-init.db"
    barrier = threading.Barrier(6)

    def initialize(_index):
        barrier.wait(timeout=5)
        store = SQLiteEventStore(database)
        try:
            assert store.current_version("run", "run-1") == 0
            return True
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=6) as executor:
        assert list(executor.map(initialize, range(6))) == [True] * 6


def test_two_connections_racing_same_stream_version_have_one_winner(tmp_path):
    database = tmp_path / "cas-race.db"
    barrier = threading.Barrier(2)

    def append(index):
        store = SQLiteEventStore(database)
        try:
            barrier.wait(timeout=5)
            return store.append(
                "run",
                "run-1",
                0,
                [EventDraft("InputAccepted", {"index": index})],
                f"input-{index}",
            )
        except StaleStream as exc:
            return exc
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(append, (1, 2)))

    assert sum(isinstance(result, list) for result in results) == 1
    assert sum(isinstance(result, StaleStream) for result in results) == 1
    check = SQLiteEventStore(database)
    assert check.current_version("run", "run-1") == 1
    assert len(check.read_stream("run", "run-1")) == 1


def test_two_connections_retrying_same_idempotency_key_get_same_event(tmp_path):
    database = tmp_path / "idempotency-race.db"
    barrier = threading.Barrier(2)

    def append():
        store = SQLiteEventStore(database)
        try:
            barrier.wait(timeout=5)
            return store.append(
                "run",
                "run-1",
                0,
                [EventDraft("RunCreated", {"run_id": "run-1"})],
                "create-run-1",
            )
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = list(executor.map(lambda _: append(), range(2)))

    assert first == second
    assert first[0].stream_version == 1
    check = SQLiteEventStore(database)
    assert check.current_version("run", "run-1") == 1


def test_concurrent_reservations_cannot_exceed_run_envelope(tmp_path):
    database = tmp_path / "budget-race.db"
    barrier = threading.Barrier(2)
    estimate = CostEstimate(amount_minor=7, currency="USD", token_limit=5)

    def reserve(index):
        store = SQLiteEventStore(database)
        try:
            ledger = BudgetLedger(
                store,
                run_limits={"run-1": RunLimit(max_cost_minor=10, max_tokens=10)},
            )
            barrier.wait(timeout=5)
            return ledger.reserve(
                "run-1",
                estimate,
                reservation_id=f"reservation-{index}",
                idempotency_key=f"reserve-{index}",
            )
        except BudgetExhausted as exc:
            return exc
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, (1, 2)))

    assert sum(not isinstance(result, BudgetExhausted) for result in results) == 1
    assert sum(isinstance(result, BudgetExhausted) for result in results) == 1
    check = SQLiteEventStore(database)
    balance = BudgetLedger(check).available("run-1")
    assert balance.reserved_minor == 7
    assert balance.available_minor == 3


def test_concurrent_approval_consumption_is_idempotent_and_atomic(tmp_path):
    database = tmp_path / "approval-race.db"
    barrier = threading.Barrier(2)
    context = {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "fencing_generation": 1,
        "correlation_id": "corr-1",
    }
    drafts = [
        EventDraft(
            "ApprovalGrantConsumed",
            {"approval_grant_id": "grant-1", "effect_id": "effect-1"},
            causation_id="policy-1",
            **context,
        ),
        EventDraft(
            "BudgetReserved",
            {
                "approval_grant_id": "grant-1",
                "reservation_id": "reservation-1",
                "reserved_minor": 5,
                "reserved_tokens": 0,
            },
            causation_id="approval-consumption-1",
            **context,
        ),
    ]

    def consume():
        store = SQLiteEventStore(database)
        try:
            barrier.wait(timeout=5)
            return store.append(
                "run", "run-1", 0, drafts, "consume-grant-1"
            )
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = list(executor.map(lambda _: consume(), range(2)))

    assert first == second
    assert [event.event_type for event in first] == [
        "ApprovalGrantConsumed",
        "BudgetReserved",
    ]
    check = SQLiteEventStore(database)
    assert check.current_version("run", "run-1") == 2
