"""Real SQLite serialization and fresh-process SIGKILL admission recovery."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import multiprocessing
from pathlib import Path
import sys
from threading import Event

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.budget import BudgetLedger
from orchestrator.lifecycle.models import NodeSpec
from orchestrator.runtime.worker_input import WorkerInputError
from orchestrator.scheduler import SchedulerError
from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
from tests.support.worker_input import admit_worker_input_case, make_worker_input_case
from tests.unit.lifecycle.test_scheduler import NOW, _application_limits, _resolver_durable_state


SENTINEL = "private-input-crash-sentinel-850afe"


def _snapshot(app):
    return _resolver_durable_state(app._event_store), tuple(app._event_store._connection.iterdump())


def _admit(app, request, decision, text):
    return app.accept_routing_with_input(
        request, decision, task_text=text, accepted_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(minutes=1),
    )


def _assert_one_effect(app):
    assert len([e for e in app._event_store.read_stream("scheduler", "global")
                if e.event_type == "RoutingDecisionAccepted"]) == 1
    assert BudgetLedger(app._event_store).available("run-1").reserved_minor == 20
    assert app._scheduler.agents.replay("run-1").active_count == 1
    assert len(app._scheduler.lifecycle.replay("run-1").node("node-1").attempts) == 1


@pytest.mark.parametrize("conflicting", [False, True], ids=["identical", "conflicting"])
def test_two_owner_thread_connections_serialize_admission(tmp_path, conflicting):
    database = tmp_path / "race.db"
    case = make_worker_input_case(database)
    case.app.close()
    ready = (Event(), Event())
    start = (Event(), Event())
    nested, contender, release = Event(), Event(), Event()

    def owner(index):
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            if index == 0:
                real_accept = app._scheduler.accept_routing

                def hold_after_nested(*args, **kwargs):
                    value = real_accept(*args, **kwargs)
                    nested.set()
                    assert release.wait(10), "parent did not release admission"
                    return value

                app._scheduler.accept_routing = hold_after_nested
            else:
                real_append = app._event_store.append_checked

                def announce_lock_attempt(*args, **kwargs):
                    if args[2] == "worker-input-admission-snapshot":
                        contender.set()
                    return real_append(*args, **kwargs)

                app._event_store.append_checked = announce_lock_attempt
            ready[index].set()
            assert start[index].wait(10)
            try:
                accepted = _admit(app, case.request, case.decision,
                                  case.text + (" changed" if index and conflicting else ""))
                return "accepted", accepted.accepted_route
            except WorkerInputError:
                return "rejected", None

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.submit(owner, 0), pool.submit(owner, 1)
        try:
            assert all(event.wait(10) for event in ready)
            start[0].set()
            assert nested.wait(10)
            start[1].set()
            assert contender.wait(10)
        finally:
            release.set()
            for event in start:
                event.set()
        results = first.result(timeout=15), second.result(timeout=15)
    assert results[0][0] == "accepted"
    assert results[1][0] == ("rejected" if conflicting else "accepted")
    if not conflicting:
        assert results[0][1] == results[1][1]
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        _assert_one_effect(app)


@pytest.mark.parametrize("mutation", ["cancel", "graph"])
def test_durable_mutation_wins_serialization_before_input_admission(tmp_path, mutation):
    database = tmp_path / "mutation-race.db"
    case = make_worker_input_case(database)
    case.app.close()
    ready, start, changed, contender, release = Event(), Event(), Event(), Event(), Event()
    durable_after_mutation = []

    def mutator():
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            ready.set()
            assert start.wait(10)

            def mutate_under_lock(_events, _version):
                if mutation == "cancel":
                    app._scheduler.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
                else:
                    app._scheduler.lifecycle.append_nodes(
                        "run-1", (NodeSpec(node_id="node-2", role="reviewer",
                                           planning_contract_hash="sha256:" + "0" * 64),),
                        expected_graph_version=1, idempotency_key="concurrent-graph",
                    )
                durable_after_mutation.append(_snapshot(app))
                changed.set()
                assert release.wait(10)
                return None

            app._event_store.append_checked("scheduler", "global", "test-mutation", mutate_under_lock)

    admission_ready, admission_start = Event(), Event()

    def admission():
        with ControlPlaneApplication(database, limits=_application_limits()) as app:
            real = app._event_store.append_checked

            def announce(*args, **kwargs):
                if args[2] == "worker-input-admission-snapshot":
                    contender.set()
                return real(*args, **kwargs)

            app._event_store.append_checked = announce
            admission_ready.set()
            assert admission_start.wait(10)
            try:
                _admit(app, case.request, case.decision, case.text)
                return "accepted"
            except WorkerInputError:
                return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        writer, reader = pool.submit(mutator), pool.submit(admission)
        try:
            assert ready.wait(10) and admission_ready.wait(10)
            start.set()
            assert changed.wait(10)
            admission_start.set()
            assert contender.wait(10)
        finally:
            release.set()
            start.set()
            admission_start.set()
        writer.result(timeout=15)
        result = reader.result(timeout=15)
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        if mutation == "cancel":
            assert result == "rejected"
            assert _snapshot(app) == durable_after_mutation[0]
            assert BudgetLedger(app._event_store).available("run-1").reserved_minor == 0
            assert app._scheduler.agents.replay("run-1").active_count == 0
        else:
            assert result == "accepted"
            _assert_one_effect(app)
            bound = app._scheduler.resolve_verification_binding(**case.identity, as_of=case.clock.now)
            assert bound.context.graph_version == 2
            assert app._scheduler.lifecycle.replay("run-1").node("node-1").spec.role == "coder"
            original_hash = bound.context.input_manifest_hash
            original_acceptance = app._event_store.read_stream("scheduler", "global")
            app._scheduler.lifecycle.append_nodes(
                "run-1", (NodeSpec(node_id="node-3", role="reviewer",
                                   planning_contract_hash="sha256:" + "1" * 64),),
                expected_graph_version=2, idempotency_key="later-graph",
            )
            # Existing Scheduler requires the exact accepted graph version.
            with pytest.raises(SchedulerError, match="exact frozen graph"):
                app._scheduler.resolve_verification_binding(**case.identity, as_of=case.clock.now)
            assert app._event_store.read_stream("scheduler", "global") == original_acceptance
            assert original_acceptance[0].payload["verification_context"]["input_manifest_hash"] == original_hash
            assert app._scheduler.lifecycle.replay("run-1").node("node-1").spec.role == "coder"


def _crash_nested_admission(database: str, request, decision, pipe):
    with ControlPlaneApplication(Path(database), limits=_application_limits()) as app:
        real = app._scheduler.accept_routing

        def hold_after_nested(*args, **kwargs):
            value = real(*args, **kwargs)
            block_at_crash_point(pipe, "after-nested-input-admission")
            return value

        app._scheduler.accept_routing = hold_after_nested
        _admit(app, request, decision, SENTINEL)


def _crash_committed_admission(database: str, request, decision, pipe):
    with ControlPlaneApplication(Path(database), limits=_application_limits()) as app:
        _admit(app, request, decision, SENTINEL)
        block_at_crash_point(pipe, "after-input-admission-commit")


@pytest.mark.parametrize("committed", [False, True], ids=["nested-rollback", "committed-retry"])
def test_fresh_process_death_preserves_atomic_admission(tmp_path, committed, caplog):
    assert sys.platform == "linux", "this gate requires real Linux SIGKILL"
    database = tmp_path / "crash.db"
    case = make_worker_input_case(database)
    before = _snapshot(case.app)
    case.text = SENTINEL
    case.app.close()
    context = multiprocessing.get_context("spawn")
    parent_pipe, child_pipe = context.Pipe()
    child = context.Process(
        target=_crash_committed_admission if committed else _crash_nested_admission,
        args=(str(database), case.request, case.decision, child_pipe),
    )
    try:
        child.start()
        child_pipe.close()
        kill_at_crash_point(child, parent_pipe, expected_point=(
            "after-input-admission-commit" if committed else "after-nested-input-admission"),
            timeout_seconds=15,
        )
    finally:
        if child.pid is not None and child.is_alive():
            child.kill()
            child.join(timeout=10)
            assert not child.is_alive(), "child was not reaped"
        parent_pipe.close()
        child_pipe.close()
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        case.app = app
        if committed:
            _assert_one_effect(app)
            after = _snapshot(app)
            original = app._scheduler.resolve_verification_binding(**case.identity, as_of=case.clock.now)
            admit_worker_input_case(case)
            assert _snapshot(app) == after
            assert app._scheduler.resolve_verification_binding(
                **case.identity, as_of=case.clock.now) == original
        else:
            assert _snapshot(app) == before
            assert app._scheduler.agents.replay("run-1").active_count == 0
            assert BudgetLedger(app._event_store).available("run-1").reserved_minor == 0
            assert app._event_store.read_stream("scheduler", "global") == []
            admit_worker_input_case(case)
            _assert_one_effect(app)
        assert SENTINEL not in repr(_snapshot(app))
        assert SENTINEL not in caplog.text
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(database) + suffix)
        if path.exists():
            assert SENTINEL.encode() not in path.read_bytes()
