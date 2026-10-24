"""Real spawned application owners serialize explicit recovery authority."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import multiprocessing
import os
import sys
from types import SimpleNamespace

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.budget import BudgetLedger
from orchestrator.routing import HealthAggregateKey, HealthController
from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from tests.support.process_crash import reap_process, receive_process_message
from tests.support.recovery_continuation import application_recovery_case, routing_observations
from tests.unit.lifecycle.test_scheduler import NOW, _application_limits


TEXT = "implement a small change"


def _durable_digest(app):
    # Include every durable row, including snapshots, idempotency and stream versions.
    dump = json.dumps(tuple(app._event_store._connection.iterdump())).encode()
    return hashlib.sha256(dump).hexdigest()


def _seed(database):
    case = application_recovery_case(database, routing_budget=True, priced=True)
    with case.app as app:
        opportunity, = app.list_recovery_opportunities(run_id="run-1")
        assert opportunity.status == "ready" and opportunity.successor is None
        # 1000 input + 500 output tokens at 10000 minor/million = 15 minor.
        assert (case.accepted.reservation.reserved_minor,
                case.accepted.reservation.reserved_tokens) == (15, 1500)
        return RecoveryRoutingInputs(**routing_observations(case)), case.plan.authorization.authorization_hash


def _continue(app, observations, text, lease_expires_at):
    return app.continue_recovery(
        run_id="run-1", node_id="node-1", source_attempt_id="attempt-node-1-1",
        source_fencing_generation=1, task_text=text, routing_inputs=observations,
        accepted_at=NOW + timedelta(seconds=7), lease_expires_at=lease_expires_at,
    )


def _continuation_owner(database, pipe, observations, text, lease_expires_at, hold_nested):
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        real_append = app._event_store.append_checked

        def announce(*args, **kwargs):
            if args[2] == "recovery-continuation-snapshot":
                pipe.send(("contending", os.getpid()))
            return real_append(*args, **kwargs)

        app._event_store.append_checked = announce
        if hold_nested:
            real_accept = app._scheduler.accept_routing

            def hold_after_nested(*args, **kwargs):
                accepted = real_accept(*args, **kwargs)
                # This snapshot precedes any possible loser write: the outer lock is held.
                pipe.send(("nested", _durable_digest(app)))
                assert receive_process_message(pipe) == "release"
                return accepted

            app._scheduler.accept_routing = hold_after_nested
        pipe.send(("ready", os.getpid()))
        assert receive_process_message(pipe) == "start"
        try:
            result = _continue(app, observations, text, lease_expires_at)
        except RecoveryContinuationError as error:
            pipe.send(("error", error.code, _durable_digest(app)))
        else:
            pipe.send(("result", result.status,
                       result.accepted_route.model_dump(mode="json") if result.accepted_route else None,
                       result.task.model_dump(mode="json") if result.task else None,
                       _durable_digest(app)))


def _mutate(app, observations, mutation):
    if mutation == "cancel":
        app._scheduler.lifecycle.request_cancel("run-1", reason_code="user_request")
    elif mutation == "budget":
        ledger = app._scheduler._ledger_for_run("run-1")
        ledger.reserve(
            "run-1", BudgetLedger.estimate(currency="USD", input_tokens=1,
                                          input_price_minor_per_million=1_000_000),
            reservation_id="concurrent-budget-hold", idempotency_key="concurrent-budget-hold",
        )
    else:
        snapshot = app._scheduler.lifecycle.config_snapshot("run-1")
        config = snapshot.resolved_config.config
        policy = config.health_policies[config.presets[config.active_preset].health_policy_ref]
        keys = dict(registry_manifest_hash=snapshot.registry_manifest_hash, provider_id="primary")
        HealthController(app._event_store).record_outcome(
            provider_key=HealthAggregateKey(**keys, scope="provider"),
            model_key=HealthAggregateKey(**keys, scope="model", model_id="model-1"),
            policy=policy, event_time=NOW + timedelta(seconds=6),
            idempotency_key="concurrent-health-change", success=True,
        )
        # A benign health update is enough to invalidate the old authority observations.
        case = SimpleNamespace(store=app._event_store, scheduler=app._scheduler,
                               registry=snapshot.registry_manifest, config=config,
                               policy=observations.policy_manifest)
        current = routing_observations(case)["eligibility"].model_health[0]
        previous = observations.eligibility.model_health[0]
        assert current.provider.version > previous.provider.version
        assert current.model.version > previous.model.version


def _mutation_owner(database, pipe, observations, mutation):
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        pipe.send(("ready", os.getpid()))
        assert receive_process_message(pipe) == "start"

        def mutate_under_lock(_events, _version):
            _mutate(app, observations, mutation)
            pipe.send(("mutated", _durable_digest(app)))
            assert receive_process_message(pipe) == "release"
            return None

        app._event_store.append_checked("scheduler", "global", "test-recovery-mutation", mutate_under_lock)
        pipe.send(("committed", _durable_digest(app)))


def _spawn_pair(context, first, second):
    parents, children, processes = [], [], []
    for target, arguments in (first, second):
        parent, child = context.Pipe()
        parents.append(parent)
        children.append(child)
        processes.append(context.Process(target=target, args=(arguments[0], child, *arguments[1:])))
    return parents, children, processes


def _start_pair(parents, children, processes):
    for process, child in zip(processes, children):
        process.start()
        child.close()
    ready = [receive_process_message(pipe) for pipe in parents]
    assert all(message[0] == "ready" for message in ready)
    assert len({message[1] for message in ready} | {os.getpid()}) == 3


def _finish_pair(processes):
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0, "spawned application owner failed or did not exit"


def _close_pair(parents, children, processes):
    try:
        for process in processes:
            reap_process(process)
    finally:
        for pipe in (*parents, *children):
            pipe.close()


def _assert_resources(app, *, admitted, authorization_hash, budget_mutation=False):
    store = app._event_store
    scheduler_events = store.read_stream("scheduler", "global")
    accepted = [event for event in scheduler_events
                if event.event_type == "RoutingDecisionAccepted"]
    assert len(accepted) == (2 if admitted else 1)
    consumed = [event for event in accepted
                if event.payload.get("recovery_authorization_hash") == authorization_hash]
    assert len(consumed) == int(admitted)
    released = {event.payload["attempt_ref"] for event in scheduler_events
                if event.event_type == "AttemptSlotReleased"}
    slots = {event.payload["attempt_ref"] for event in accepted} - released
    assert slots == ({consumed[0].payload["attempt_ref"]} if admitted else set())
    budget_events = store.read_stream("budget", "run-1")
    reservations = [event for event in budget_events if event.event_type == "BudgetReserved"]
    assert len(reservations) == 1 + int(admitted) + int(budget_mutation)
    balance = BudgetLedger(store).available("run-1")
    assert (balance.reserved_minor, balance.reserved_tokens) == (
        (15, 1500) if admitted else (1, 1) if budget_mutation else (0, 0)
    )
    assert (balance.unknown_minor, balance.unknown_tokens, balance.used_minor, balance.used_tokens) == (0, 0, 0, 0)
    assert (balance.held_minor, balance.held_tokens) == (
        (15, 1500) if admitted else (1, 1) if budget_mutation else (0, 0)
    )
    agents = app._scheduler.agents.replay("run-1")
    assert (agents.total_created, agents.active_count) == (1 + int(admitted), int(admitted))
    recovered = app._scheduler.recovery.recover("run-1")
    assert len(recovered.active_attempts) == int(admitted)
    attempts = recovered.lifecycle.node("node-1").attempts
    assert len(attempts) == 1 + int(admitted)
    if admitted:
        route = consumed[0].payload["accepted_route"]
        assert (route["fencing_generation"], route["model_id"], route["provider_id"]) == (2, "model-1", "primary")
        reservation = BudgetLedger(store).get_reservation(route["budget_reservation_id"], run_id="run-1")
        assert (reservation.status, reservation.reserved_minor, reservation.reserved_tokens) == ("reserved", 15, 1500)
        assert recovered.active_attempts[0].attempt_id == route["attempt_id"]
        assert attempts[-1].attempt_id == route["attempt_id"]
        assert agents.for_attempt(route["attempt_id"]).status == "active"
        return route


@pytest.mark.parametrize("conflicting_text", [False, True], ids=["same-text", "conflicting-text"])
@pytest.mark.parametrize("conflicting_observations", [False, True], ids=["same-observations", "conflicting-observations"])
def test_spawned_owners_consume_one_authorization_without_loser_writes(
    tmp_path, conflicting_text, conflicting_observations,
):
    # Catches duplicate consumption/holds, early observation validation on replay,
    # or a conflicting input changing the accepted successor after lock contention.
    assert sys.platform == "linux", "process acceptance requires real Linux processes"
    database = tmp_path / "recovery-race.db"
    observations, authorization_hash = _seed(database)
    changed = observations
    if conflicting_observations:
        changed = observations.model_copy(update={"eligibility": observations.eligibility.model_copy(
            update={"available_tokens": 0, "authorization_revoked": True},
        )})
    lease = datetime.now(timezone.utc) + timedelta(minutes=5)
    parents, children, processes = _spawn_pair(multiprocessing.get_context("spawn"), (
        _continuation_owner, (database, observations, TEXT, lease, True),
    ), (
        _continuation_owner, (database, changed, TEXT + (" changed" if conflicting_text else ""), lease, False),
    ))
    try:
        _start_pair(parents, children, processes)
        parents[0].send("start")
        assert receive_process_message(parents[0])[0] == "contending"
        nested = receive_process_message(parents[0])
        assert nested[0] == "nested"
        parents[1].send("start")
        assert receive_process_message(parents[1])[0] == "contending"
        assert not parents[1].poll(0.1), "contender escaped the held outer transaction"
        parents[0].send("release")
        first, second = [receive_process_message(pipe) for pipe in parents]
        _finish_pair(processes)
        assert first[0:2] == ("result", "prepared")
        assert first[-1] == second[-1] == nested[1]
        if conflicting_text:
            assert second[0:2] == ("error", "recovery_input_mismatch")
        else:
            assert second[0:2] == ("result", "prepared")
            assert second[2:4] == first[2:4]
        assert first[3]["task_text"] == TEXT
    finally:
        _close_pair(parents, children, processes)
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        assert _durable_digest(app) == nested[1]
        assert _assert_resources(app, admitted=True, authorization_hash=authorization_hash) == first[2]
        assert TEXT not in "\n".join(app._event_store._connection.iterdump())


@pytest.mark.parametrize("mutation", ["cancel", "health", "budget"])
def test_spawned_durable_change_wins_outer_lock_without_successor_writes(tmp_path, mutation):
    # Catches checking ready inventory or health/budget observations before acquiring
    # the outer transaction, leaving successor resources after a winning mutation.
    assert sys.platform == "linux", "process acceptance requires real Linux processes"
    database = tmp_path / "mutation-race.db"
    observations, authorization_hash = _seed(database)
    lease = datetime.now(timezone.utc) + timedelta(minutes=5)
    parents, children, processes = _spawn_pair(multiprocessing.get_context("spawn"), (
        _mutation_owner, (database, observations, mutation),
    ), (
        _continuation_owner, (database, observations, TEXT, lease, False),
    ))
    try:
        _start_pair(parents, children, processes)
        parents[0].send("start")
        mutated = receive_process_message(parents[0])
        assert mutated[0] == "mutated"
        parents[1].send("start")
        assert receive_process_message(parents[1])[0] == "contending"
        assert not parents[1].poll(0.1), "continuation escaped the held mutation transaction"
        parents[0].send("release")
        committed, rejected = [receive_process_message(pipe) for pipe in parents]
        _finish_pair(processes)
        assert committed == ("committed", mutated[1])
        assert rejected[-1] == mutated[1]
        if mutation == "cancel":
            assert rejected[0:2] == ("result", "blocked") and rejected[2:4] == (None, None)
        else:
            assert rejected[0:2] == ("error", "recovery_continuation_failed")
    finally:
        _close_pair(parents, children, processes)
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        assert _durable_digest(app) == mutated[1]
        _assert_resources(app, admitted=False, authorization_hash=authorization_hash,
                          budget_mutation=mutation == "budget")
