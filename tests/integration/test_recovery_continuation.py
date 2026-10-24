"""Real spawned application owners serialize explicit recovery authority."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sys
import traceback
from types import SimpleNamespace

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.budget import BudgetLedger
from orchestrator.models import AcceptedModelRoute
from orchestrator.models.gateway import ModelMessage
from orchestrator.models.provider_calls import SQLiteProviderCallJournal
from orchestrator.routing import HealthAggregateKey, HealthController
from orchestrator.runtime.recovery_continuation import RecoveryRoutingInputs
from orchestrator.runtime.recovery_opportunities import RecoveryContinuationError
from tests.support.process_crash import (
    block_at_crash_point, kill_at_crash_point, reap_process, receive_process_message,
)
from tests.support.recovery_continuation import (
    application_recovery_case, forbid_recovery_execution, routing_observations,
)
from tests.unit.lifecycle.test_scheduler import (
    NOW, _application_limits, _provider_call_request_for_crash,
)


TEXT = "implement a small change"
SENTINEL = "private-recovery-process-input-39d6b842"


def _durable_digest(app):
    # Include every durable row, including snapshots, idempotency and stream versions.
    dump = json.dumps(tuple(app._event_store._connection.iterdump())).encode()
    return hashlib.sha256(dump).hexdigest()


def _seed(database, *, task_text=TEXT):
    case = application_recovery_case(database, routing_budget=True, priced=True, task_text=task_text)
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


def _crash_recovery_owner(database, pipe, observations, text, lease, point):
    with forbid_recovery_execution() as attempted:
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            def checkpoint():
                accepted = [event for event in app._event_store.read_stream("scheduler", "global")
                            if event.event_type == "RoutingDecisionAccepted"]
                assert len(accepted) == 2
                assert app._event_store._connection.in_transaction == (point == "nested")
                assert attempted == []
                pipe.send((dict(accepted[-1].payload["accepted_route"]), _durable_digest(app)))
                block_at_crash_point(pipe, "recovery-" + point)

            if point == "nested":
                real_accept = app._scheduler.accept_routing

                def stop_after_nested(*args, **kwargs):
                    accepted = real_accept(*args, **kwargs)
                    checkpoint()
                    return accepted

                app._scheduler.accept_routing = stop_after_nested
            else:
                real_append = app._event_store.append_checked

                def stop_at_boundary(*args, **kwargs):
                    if point == "before-preparation" and args[2] == "recovery-preparation-snapshot":
                        checkpoint()
                    result = real_append(*args, **kwargs)
                    if point == "committed" and args[2] == "recovery-continuation-snapshot":
                        checkpoint()
                    return result

                app._event_store.append_checked = stop_at_boundary
            _continue(app, observations, text, lease)
            raise AssertionError("continuation returned without its crash barrier")


def _kill_recovery_owner(database, observations, lease, point):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_crash_recovery_owner,
                              args=(database, child, observations, SENTINEL, lease, point))
    try:
        process.start()
        child.close()
        checkpoint = receive_process_message(parent)
        kill_at_crash_point(process, parent, expected_point="recovery-" + point, timeout_seconds=15)
        return checkpoint
    finally:
        reap_process(process)
        parent.close()
        child.close()


def _resume_recovery_owner(database, pipe, observations, text, lease):
    with forbid_recovery_execution() as attempted:
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            before = _durable_digest(app)
            opportunity, = app.list_recovery_opportunities(run_id="run-1")
            missing = _continue(app, None, None, lease)
            try:
                _continue(app, observations, text + " changed", lease)
            except RecoveryContinuationError as error:
                changed_code = error.code
                error_report = "".join(traceback.format_exception(error))
            else:
                changed_code, error_report = "accepted", ""
            after_invalid_input = _durable_digest(app)
            # A committed successor must replay without any new routing input.
            routing = observations if opportunity.successor is None else None
            result = _continue(app, routing, text, lease)
            after_exact_input = _durable_digest(app)
            repeated = _continue(app, None, text, lease)
            task = result.task
            report = dict(
                initial_digest=before, after_invalid_input=after_invalid_input,
                initial_status=opportunity.status, initial_reason=opportunity.reason,
                missing_status=missing.status, missing_task=missing.task is None,
                initial_successor=(opportunity.successor.model_dump(mode="json")
                                   if opportunity.successor else None),
                changed_code=changed_code, status=result.status, reason=result.reason,
                route=result.accepted_route.model_dump(mode="json") if result.accepted_route else None,
                context=task.context.model_dump(mode="json") if task else None,
                exact_text=task.task_text == text if task else None,
                role=task.role if task else None,
                repeated_status=repeated.status, same_task=repeated.task == task,
                same_route=repeated.accepted_route == result.accepted_route,
                after_exact_input=after_exact_input, final_digest=_durable_digest(app),
                public_reports=repr((app.startup_report, opportunity, missing, result, task)) + error_report,
                unresolved_calls=app.startup_report.unresolved_provider_calls,
            )
        report["external_attempts"] = list(attempted)
        pipe.send(report)


def _resume_in_fresh_process(database, observations, lease):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_resume_recovery_owner,
                              args=(database, child, observations, SENTINEL, lease))
    try:
        process.start()
        child.close()
        report = receive_process_message(parent)
        process.join(timeout=15)
        assert process.exitcode == 0, "fresh recovery process failed or was not reaped"
        return report
    finally:
        reap_process(process)
        parent.close()
        child.close()


def _assert_private_recovery_surfaces(database, report, *, capfd, caplog):
    assert SENTINEL not in json.dumps(report)
    captured = capfd.readouterr()
    assert SENTINEL not in captured.out + captured.err + caplog.text
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(database) + suffix)
        if path.exists():
            assert SENTINEL.encode() not in path.read_bytes()


@pytest.mark.parametrize("point", ["nested", "committed", "before-preparation"])
def test_sigkill_reopens_atomic_recovery_and_requires_exact_input(tmp_path, point, capfd, caplog):
    # Catches partial nested commits, duplicate lost-reply admission, or a seam
    # retaining input in durable storage instead of requiring exact re-supply.
    assert sys.platform == "linux", "crash acceptance requires parent-issued Linux SIGKILL"
    database = tmp_path / "recovery-crash.db"
    observations, authorization_hash = _seed(database, task_text=SENTINEL)
    lease = datetime.now(timezone.utc) + timedelta(minutes=5)
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        before = _durable_digest(app)
        input_commitment = app._event_store.read_stream("scheduler", "global")[0].payload[
            "verification_admission"]["input_manifest_hash"]
    route_at_crash, digest_at_crash = _kill_recovery_owner(database, observations, lease, point)
    with forbid_recovery_execution() as attempted:
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            reopened = _durable_digest(app)
            if point == "nested":
                assert reopened == before and reopened != digest_at_crash
                _assert_resources(app, admitted=False, authorization_hash=authorization_hash)
            else:
                assert reopened == digest_at_crash
                assert _assert_resources(app, admitted=True, authorization_hash=authorization_hash) == route_at_crash
            _assert_private_recovery_surfaces(database, {}, capfd=capfd, caplog=caplog)
        assert attempted == []
    report = _resume_in_fresh_process(database, observations, lease)
    assert report["initial_digest"] == report["after_invalid_input"] == reopened
    assert report["initial_status"] == ("ready" if point == "nested" else "already_admitted")
    assert report["initial_successor"] == (None if point == "nested" else route_at_crash)
    assert report["missing_status"] == "waiting_for_input"
    assert report["missing_task"]
    assert report["changed_code"] == "recovery_input_mismatch"
    assert report["status"] == report["repeated_status"] == "prepared"
    assert report["same_task"] and report["same_route"] and report["exact_text"]
    assert report["role"] == "coder" and report["external_attempts"] == []
    assert report["route"] == route_at_crash
    assert (report["context"]["attempt_id"], report["context"]["fencing_generation"]) == (route_at_crash["attempt_id"], 2)
    assert report["context"]["input_manifest_hash"] == input_commitment
    assert report["after_exact_input"] == report["final_digest"]
    if point != "nested":
        assert report["final_digest"] == reopened
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        assert _durable_digest(app) == report["final_digest"]
        assert _assert_resources(app, admitted=True, authorization_hash=authorization_hash) == route_at_crash
        _assert_private_recovery_surfaces(database, report, capfd=capfd, caplog=caplog)
    _assert_private_recovery_surfaces(database, report, capfd=capfd, caplog=caplog)


@pytest.mark.parametrize("call_status", ["dispatching", "unknown"])
def test_unresolved_call_blocks_recovery_across_fresh_processes_without_execution(
    tmp_path, call_status, capfd, caplog,
):
    # Catches startup or consumed-successor replay treating a pending Provider
    # call as retry/preparation authority or crossing any external effect boundary.
    assert sys.platform == "linux", "reopen acceptance requires real Linux processes"
    database = tmp_path / "unknown-recovery.db"
    observations, authorization_hash = _seed(database, task_text=SENTINEL)
    lease = datetime.now(timezone.utc) + timedelta(minutes=5)
    route_at_crash, _ = _kill_recovery_owner(database, observations, lease, "before-preparation")
    with forbid_recovery_execution() as attempted:
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            route = AcceptedModelRoute.model_validate(route_at_crash)
            registry = app._scheduler.lifecycle.config_snapshot("run-1").registry_manifest
            request = _provider_call_request_for_crash(SimpleNamespace(accepted_route=route), registry)
            request = request.model_copy(update={"messages": (ModelMessage(role="user", content=SENTINEL),)})
            journal = SQLiteProviderCallJournal(app._event_store)
            journal.record_intent(
                request, provider_id="primary", provider_adapter="openai_responses",
                provider_correlation_id="recovery-unknown-call", request_body=json.dumps({"input": SENTINEL}).encode(),
            )
            if call_status == "unknown":
                journal.record_outcome(request, outcome="unknown", failure_code="timeout")
            call = journal.read(request)
            assert call.status == call_status
            call_id = call.stream_id
            before = _durable_digest(app)
            _assert_resources(app, admitted=True, authorization_hash=authorization_hash)
            _assert_private_recovery_surfaces(database, {}, capfd=capfd, caplog=caplog)
        assert attempted == []
    for _ in range(2):
        report = _resume_in_fresh_process(database, observations, lease)
        assert (report["initial_status"], report["initial_reason"]) == (
            "already_admitted", "recovery_authorization_consumed",
        )
        assert report["missing_status"] == "waiting_for_input" and report["missing_task"]
        assert report["status"] == report["repeated_status"] == "reconciliation_required"
        assert report["reason"] == "provider_outcome_unknown"
        assert report["changed_code"] == "recovery_input_mismatch"
        assert report["route"] == report["initial_successor"] == route_at_crash
        assert report["context"] is None and report["exact_text"] is None
        assert report["same_task"] and report["same_route"]
        assert report["external_attempts"] == [] and report["unresolved_calls"] == (call_id,)
        assert report["initial_digest"] == report["after_invalid_input"] == report["final_digest"] == before
        _assert_private_recovery_surfaces(database, report, capfd=capfd, caplog=caplog)
    with forbid_recovery_execution() as attempted:
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            assert _durable_digest(app) == before
            call = app._journal.read_call(call_id)
            assert call.status == call_status and call.reconciliation is None
            _assert_resources(app, admitted=True, authorization_hash=authorization_hash)
            _assert_private_recovery_surfaces(database, report, capfd=capfd, caplog=caplog)
        assert attempted == []
    _assert_private_recovery_surfaces(database, report, capfd=capfd, caplog=caplog)
