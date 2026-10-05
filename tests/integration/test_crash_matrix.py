import asyncio
import multiprocessing
import os
import signal
import time

import pytest

from orchestrator.budget import BudgetLedger, CostEstimate, RunLimit, UsageRecord
from orchestrator.persistence import EventDraft, SQLiteEventStore
from tests.support.process_crash import block_at_crash_point, kill_at_crash_point


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


def _provider_call_child(database, dispatched_marker, registry, request):
    from pathlib import Path

    from orchestrator.models import (
        ProviderCredential,
        ProviderModelGateway,
        SQLiteProviderCallJournal,
    )

    class _Verifier:
        async def is_accepted(self, _request):
            return True

    class _Broker:
        async def acquire_provider_credential(self, **_values):
            return ProviderCredential(
                "authorization", "test-only-secret", "primary",
                "https://api.openai.com/v1", "model_inference",
            )

    class _HangingTransport:
        async def post_json(self, **_values):
            with Path(dispatched_marker).open("x", encoding="utf-8") as marker:
                marker.write("dispatched\n")
                marker.flush()
                os.fsync(marker.fileno())
            await asyncio.Event().wait()

    store = SQLiteEventStore(database)
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=_Broker(),
        transport=_HangingTransport(),
        provider_call_journal=SQLiteProviderCallJournal(store),
    )
    try:
        asyncio.run(gateway.invoke(request))
    finally:
        store.close()


def _trace_before_commit(pipe, point):
    def trace(statement):
        if statement.strip().upper() == "COMMIT":
            block_at_crash_point(pipe, point)
    return trace


def _budget_reservation_child(database, pipe, boundary):
    with SQLiteEventStore(database) as store:
        ledger = BudgetLedger(
            store,
            run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
        )
        point = f"budget_reservation_{boundary}"
        if boundary == "before_commit":
            store._connection.set_trace_callback(_trace_before_commit(pipe, point))
        _reserve_budget(ledger)
        block_at_crash_point(pipe, point)


def _budget_settlement_child(database, pipe, boundary, reservation_id):
    with SQLiteEventStore(database) as store:
        ledger = BudgetLedger(store)
        point = f"budget_settlement_{boundary}"
        if boundary == "before_commit":
            store._connection.set_trace_callback(_trace_before_commit(pipe, point))
        ledger.commit_usage(
            reservation_id,
            {"input_tokens": 4, "cost_minor": 3},
            settlement_key="settlement-1",
        )
        block_at_crash_point(pipe, point)


def _reserve_budget(ledger):
    return ledger.reserve(
        "run-1",
        CostEstimate(amount_minor=12, currency="USD", token_limit=8),
        idempotency_key="provider-call-1",
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=2,
        correlation_id="corr-1",
        causation_id="routing-1",
    )


@pytest.mark.parametrize("point", ["", "x" * 97, "non-ascii-\N{LATIN SMALL LETTER E WITH ACUTE}"])
def test_crash_barrier_rejects_invalid_point_without_sending(point):
    parent_pipe, child_pipe = multiprocessing.Pipe(duplex=True)
    try:
        with pytest.raises(ValueError, match="non-empty ASCII"):
            block_at_crash_point(child_pipe, point)
        assert not parent_pipe.poll(0)
    finally:
        parent_pipe.close()
        child_pipe.close()


@pytest.mark.parametrize("failure", ["mismatch", "timeout", "oversize"])
def test_crash_barrier_failure_still_kills_and_reaps_child(failure):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("hard process termination requires the fork start method")
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe(duplex=True)

    def blocked_child():
        if failure == "timeout":
            child_pipe.recv_bytes(1)
        else:
            # Oversize exercises the parent's bounded receive failure path.
            child_pipe.send_bytes(b"x" * 97 if failure == "oversize" else b"wrong-point")
            child_pipe.recv_bytes(1)

    child = context.Process(target=blocked_child)
    child.start()
    child_pipe.close()
    try:
        expected_error = OSError if failure == "oversize" else AssertionError
        with pytest.raises(expected_error):
            kill_at_crash_point(
                child, parent_pipe, expected_point="expected-point", timeout_seconds=1
            )
        assert child.exitcode == -signal.SIGKILL
        assert not child.is_alive()
    finally:
        parent_pipe.close()
        child.close()


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


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_budget_reservation_sigkill_replays_one_hold(tmp_path, boundary):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("hard process termination requires the fork start method")
    database = tmp_path / "budget-sigkill.db"
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe(duplex=True)
    child = context.Process(
        target=_budget_reservation_child, args=(database, child_pipe, boundary)
    )
    child.start()
    child_pipe.close()
    try:
        kill_at_crash_point(
            child, parent_pipe, expected_point=f"budget_reservation_{boundary}"
        )
    finally:
        parent_pipe.close()
        child.close()

    with SQLiteEventStore(database) as recovered:
        ledger = BudgetLedger(
            recovered,
            run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
        )
        events = recovered.read_stream("budget", "run-1")
        if boundary == "before_commit":
            assert events == []
            assert ledger.available("run-1").reserved_minor == 0
        else:
            assert [event.event_type for event in events] == ["BudgetReserved"]
            assert ledger.available("run-1").reserved_minor == 12
        reservation = _reserve_budget(ledger)
        if boundary == "after_commit":
            assert reservation.reservation_id == events[0].payload["reservation_id"]
        assert _reserve_budget(ledger) == reservation
        assert [event.event_type for event in recovered.read_stream("budget", "run-1")] == [
            "BudgetReserved"
        ]
        assert reservation.attempt_id == "attempt-1"
        assert ledger.available("run-1").reserved_minor == 12


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


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_budget_settlement_sigkill_retries_without_double_charge(tmp_path, boundary):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("hard process termination requires the fork start method")
    database = tmp_path / "settlement-sigkill.db"
    with SQLiteEventStore(database) as initial:
        reservation = _reserve_budget(BudgetLedger(
            initial,
            run_limits={"run-1": RunLimit(max_cost_minor=100, max_tokens=100)},
        ))
    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe(duplex=True)
    child = context.Process(
        target=_budget_settlement_child,
        args=(database, child_pipe, boundary, reservation.reservation_id),
    )
    child.start()
    child_pipe.close()
    try:
        kill_at_crash_point(
            child, parent_pipe, expected_point=f"budget_settlement_{boundary}"
        )
    finally:
        parent_pipe.close()
        child.close()

    with SQLiteEventStore(database) as recovered:
        ledger = BudgetLedger(recovered)
        events = recovered.read_stream("budget", "run-1")
        balance = ledger.available("run-1")
        if boundary == "before_commit":
            assert [event.event_type for event in events] == ["BudgetReserved"]
            assert balance.reserved_minor == 12
            assert balance.used_minor == 0
        else:
            assert balance.used_minor == 3
            assert balance.reserved_minor == 0
            assert [event.event_type for event in events].count("CostCommitted") == 1
        first = ledger.commit_usage(
            reservation.reservation_id,
            {"input_tokens": 4, "cost_minor": 3},
            settlement_key="settlement-1",
        )
        if boundary == "after_commit":
            committed = next(event for event in events if event.event_type == "CostCommitted")
            assert first == UsageRecord(
                reservation_id=committed.payload["reservation_id"],
                run_id="run-1",
                settlement_key="settlement-1",
                currency="USD",
                input_tokens=4,
                cost_minor=3,
                status="committed",
            )
        assert ledger.commit_usage(
            reservation.reservation_id,
            {"input_tokens": 4, "cost_minor": 3},
            settlement_key="settlement-1",
        ) == first
        assert ledger.available("run-1").used_minor == 3
        assert ledger.available("run-1").reserved_minor == 0
        assert [event.event_type for event in recovered.read_stream("budget", "run-1")].count(
            "CostCommitted"
        ) == 1


def test_gateway_does_not_replay_after_process_dies_during_provider_dispatch(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("hard process termination requires the fork start method")

    from orchestrator.models import (
        ModelGatewayError,
        ProviderModelGateway,
        SQLiteProviderCallJournal,
    )
    from orchestrator.persistence import SQLiteEventStore
    from tests.unit.models.test_provider_call_journal import (
        _Broker,
        _FakeTransport,
        _Verifier,
        _success_response,
        model_registry,
        model_request,
    )

    registry = model_registry()
    request = model_request(registry)
    database = tmp_path / "provider-dispatch-crash.db"
    dispatched_marker = tmp_path / "provider-dispatched"
    child = multiprocessing.get_context("fork").Process(
        target=_provider_call_child,
        args=(database, dispatched_marker, registry, request),
    )
    child.start()
    try:
        deadline = time.monotonic() + 10
        while not dispatched_marker.exists() and child.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)

        assert dispatched_marker.exists(), f"child never reached provider transport (exit={child.exitcode})"
        assert child.is_alive(), "fake provider transport must still be blocked before termination"
        child.kill()
        child.join(timeout=5)
        assert not child.is_alive()
        assert child.exitcode == -signal.SIGKILL
    finally:
        if child.is_alive():
            child.kill()
            child.join(timeout=5)
        child.close()

    assert dispatched_marker.read_text(encoding="utf-8").splitlines() == ["dispatched"]

    recovered_store = SQLiteEventStore(database)
    recovered_journal = SQLiteProviderCallJournal(recovered_store)
    [pending] = recovered_journal.unresolved()
    assert pending.status == "dispatching"
    assert pending.attempt_id == request.attempt_id

    broker = _Broker()
    transport = _FakeTransport(_success_response())
    gateway = ProviderModelGateway(
        registry=registry,
        accepted_route_verifier=_Verifier(),
        secret_broker=broker,
        transport=transport,
        provider_call_journal=recovered_journal,
    )
    with pytest.raises(ModelGatewayError) as replay:
        asyncio.run(gateway.invoke(request))

    assert replay.value.failure.code == "idempotency_conflict"
    assert broker.calls == 0
    assert transport.calls == []
    assert dispatched_marker.read_text(encoding="utf-8").splitlines() == ["dispatched"]
    recovered_store.close()
