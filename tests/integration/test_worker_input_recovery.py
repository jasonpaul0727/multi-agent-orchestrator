"""Real SQLite serialization and fresh-process SIGKILL admission recovery."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import multiprocessing
from pathlib import Path
import sys
from threading import Event, Thread

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.budget import BudgetLedger
from orchestrator.lifecycle.models import NodeSpec
from orchestrator.runtime.worker_input import WorkerInputError
from orchestrator.scheduler import SchedulerError
from tests.support.process_crash import block_at_crash_point, kill_at_crash_point
from tests.support.worker_input import (
    admit_worker_input_case, corrupt_worker_input_event, make_worker_input_case,
    prepare_worker_input_case,
)
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


def test_preparation_samples_clock_after_waiting_for_outer_lock(tmp_path):
    database = tmp_path / "expiry-lock.db"
    case = make_worker_input_case(database)
    admit_worker_input_case(case)
    before = _snapshot(case.app)
    ready = (Event(), Event())
    start, held, contender, release, released, sampled = (Event() for _ in range(6))
    results, errors = [], []

    def blocker():
        try:
            with ControlPlaneApplication(database, limits=_application_limits()) as app:
                ready[0].set()
                assert start.wait(10)
                app._event_store._connection.execute("BEGIN IMMEDIATE")
                try:
                    held.set()
                    assert release.wait(10)
                finally:
                    released.set()
                    app._event_store._connection.commit()
        except BaseException as exc:
            errors.append(exc)

    def preparer():
        try:
            with ControlPlaneApplication(database, limits=_application_limits()) as app:
                real = app._event_store.append_checked

                def announce(*args, **kwargs):
                    if args[2] == "worker-input-preparation-snapshot":
                        contender.set()
                    return real(*args, **kwargs)

                def clock():
                    assert released.is_set(), "clock was sampled before lock release"
                    sampled.set()
                    return case.clock.now

                app._event_store.append_checked = announce
                app._worker_inputs._clock = clock
                ready[1].set()
                assert held.wait(10)
                try:
                    results.append(app.prepare_worker_task(**case.identity, task_text=case.text))
                except WorkerInputError:
                    results.append("rejected")
        except BaseException as exc:
            errors.append(exc)

    threads = (Thread(target=blocker), Thread(target=preparer))
    for thread in threads:
        thread.start()
    try:
        assert all(event.wait(10) for event in ready)
        start.set()
        assert held.wait(10) and contender.wait(10)
        assert not sampled.is_set()
        case.clock.now = NOW + timedelta(minutes=1)
    finally:
        release.set()
        start.set()
        for thread in threads:
            thread.join(10)
        case.app.close()
    assert not any(thread.is_alive() for thread in threads)
    assert not errors
    assert sampled.is_set() and results == ["rejected"]
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        assert _snapshot(app) == before


@pytest.mark.parametrize("mutation", ["cancel", "graph"])
@pytest.mark.parametrize("preparation_first", [False, True], ids=["mutation-first", "preparation-first"])
def test_preparation_and_mutation_have_one_serialized_snapshot(tmp_path, mutation, preparation_first):
    database = tmp_path / "preparation-race.db"
    case = make_worker_input_case(database)
    admit_worker_input_case(case)
    before = _snapshot(case.app)
    case.app.close()
    ready = (Event(), Event())
    start = (Event(), Event())
    held, contender, release = Event(), Event(), Event()
    errors, tasks, mutation_state, preparation_state = [], [], [], []

    def owner(index):
        try:
            with ControlPlaneApplication(database, limits=_application_limits()) as app:
                app._worker_inputs._clock = lambda: case.clock.now
                real = app._event_store.append_checked
                own_key = "worker-input-preparation-snapshot" if index == 0 else "test-mutation"
                first = (index == 0) == preparation_first

                def ordered(*args, **kwargs):
                    if args[2] != own_key:
                        return real(*args, **kwargs)
                    if not first:
                        contender.set()
                        return real(*args, **kwargs)
                    callback = args[3]

                    def hold(events, version):
                        value = callback(events, version)
                        if index == 0:
                            preparation_state.append(_snapshot(app))
                        held.set()
                        assert release.wait(10)
                        return value

                    return real(*args[:3], hold)

                app._event_store.append_checked = ordered
                ready[index].set()
                assert start[index].wait(10)
                if index == 0:
                    try:
                        tasks.append(app.prepare_worker_task(**case.identity, task_text=case.text))
                    except WorkerInputError:
                        tasks.append("rejected")
                else:
                    def mutate(_events, _version):
                        if mutation == "cancel":
                            app._scheduler.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
                        else:
                            app._scheduler.lifecycle.append_nodes("run-1", (NodeSpec(
                                node_id="extra", role="reviewer", planning_contract_hash="sha256:" + "0" * 64),),
                                expected_graph_version=1, idempotency_key="concurrent-graph")
                        mutation_state.append(_snapshot(app))
                        return None
                    app._event_store.append_checked("scheduler", "global", own_key, mutate)
        except BaseException as exc:
            errors.append(exc)

    threads = tuple(Thread(target=owner, args=(index,)) for index in (0, 1))
    for thread in threads:
        thread.start()
    first = 0 if preparation_first else 1
    try:
        assert all(event.wait(10) for event in ready)
        start[first].set()
        assert held.wait(10)
        start[1 - first].set()
        assert contender.wait(10)
    finally:
        release.set()
        for event in start:
            event.set()
        for thread in threads:
            thread.join(10)
    assert not any(thread.is_alive() for thread in threads)
    assert not errors
    assert len(tasks) == 1 and len(mutation_state) == 1
    if preparation_first:
        assert tasks[0].context.graph_version == 1
        assert tasks[0].task_text == case.text and tasks[0].output_byte_limit == 16_384
        assert preparation_state == [before]
    else:
        assert tasks == ["rejected"]
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        assert _snapshot(app) == mutation_state[0]
        assert _resolver_durable_state(app._event_store)[1] > before[0][1]
        app._worker_inputs._clock = lambda: case.clock.now
        with pytest.raises(WorkerInputError):
            app.prepare_worker_task(**case.identity, task_text=case.text)
        assert _snapshot(app) == mutation_state[0]


def reopen_and_prepare(database, text_or_none, trusted_fixture_time_iso, pipe):
    """Fresh spawn uses only re-supplied memory input, reports no plaintext."""
    from orchestrator.runtime.worker_process import IsolatedWorkerProcess
    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected Worker execution")
    IsolatedWorkerProcess.execute = forbidden
    try:
        with ControlPlaneApplication(Path(database), limits=_application_limits()) as app:
            before = _snapshot(app)
            app._worker_inputs._clock = lambda: datetime.fromisoformat(trusted_fixture_time_iso)
            identity = dict(run_id="run-1", node_id="node-1", attempt_id="attempt-node-1-1",
                            fencing_generation=1)
            if not hasattr(app, "prepare_worker_task"):
                pipe.send(("missing-api", None, _snapshot(app) == before))
                return
            try:
                kwargs = {} if text_or_none is None else {"task_text": text_or_none}
                task = app.prepare_worker_task(**identity, **kwargs)
                pipe.send(("prepared", (task.context.model_dump(mode="json"), task.output_byte_limit),
                           _snapshot(app) == before))
            except (WorkerInputError, TypeError):
                pipe.send(("rejected", None, _snapshot(app) == before))
    finally:
        pipe.close()


@pytest.mark.parametrize("state", ["active", "missing", "changed", "expired", "cancelled",
                                  "terminal", "stale", "legacy-absent", "legacy-arbitrary"])
def test_fresh_spawn_reopen_requires_exact_still_active_input(tmp_path, state, caplog):
    database = tmp_path / "reopen-preparation.db"
    case = make_worker_input_case(database)
    case.text = SENTINEL
    try:
        if state.startswith("legacy"):
            kwargs = {} if state == "legacy-absent" else dict(
                input_manifest_hash="sha256:" + "0" * 64,
                verification_contract="maestro.artifact-verification/v1",
                required_check_ids=("artifact-integrity",),
            )
            case.app.accept_routing(case.request, case.decision, accepted_at=NOW + timedelta(seconds=1),
                                    lease_expires_at=NOW + timedelta(minutes=1), **kwargs)
        else:
            admit_worker_input_case(case)
        expected_context = (case.app._scheduler.resolve_verification_binding(
            **case.identity, as_of=case.clock.now).context.model_dump(mode="json")
            if state == "active" else None)
        if state == "cancelled":
            case.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
        elif state == "terminal":
            case.app._scheduler.finish_attempt(**case.identity, completed_at=case.clock.now,
                                               outcome="failed", known_no_effect=True)
        elif state == "stale":
            corrupt_worker_input_event(case.app._event_store, ("scheduler", "global"),
                lambda payload: payload["verification_context"].update(fencing_generation=2))
        before = _snapshot(case.app)
    finally:
        case.app.close()
    context = multiprocessing.get_context("spawn")
    parent_pipe, child_pipe = context.Pipe()
    now = NOW + timedelta(minutes=1) if state == "expired" else case.clock.now
    text = None if state == "missing" else SENTINEL + (" changed" if state == "changed" else "")
    child = context.Process(target=reopen_and_prepare,
                            args=(str(database), text, now.isoformat(), child_pipe))
    try:
        child.start()
        child_pipe.close()
        assert parent_pipe.poll(10), "child did not report bounded re-supply outcome"
        result = parent_pipe.recv()
        child.join(10)
        assert not child.is_alive() and child.exitcode == 0
    finally:
        if child.pid is not None and child.is_alive():
            child.kill()
            child.join(10)
        parent_pipe.close()
        child_pipe.close()
    if state == "active":
        assert result == ("prepared", (expected_context, 16_384), True)
    else:
        assert result == ("rejected", None, True)
    assert SENTINEL not in repr(result) + caplog.text
    with ControlPlaneApplication(database, limits=_application_limits()) as app:
        assert _snapshot(app) == before
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(database) + suffix)
        if path.exists():
            assert SENTINEL.encode() not in path.read_bytes()


def test_preparation_uses_frozen_config_after_independent_candidate_reload(tmp_path):
    from orchestrator.config.runtime import ConfigManager
    from tests.unit.config.test_config_effective import registry, runtime_candidate
    case = make_worker_input_case(tmp_path / "frozen-config.db")
    try:
        admit_worker_input_case(case)
        original = case.app._scheduler.resolve_verification_binding(**case.identity, as_of=case.clock.now)
        manager = ConfigManager(runtime_candidate(registry(), 100))
        candidate_file = tmp_path / "candidate-budget.json"
        candidate_file.write_text("300", encoding="utf-8")
        changed = manager.reload(lambda: runtime_candidate(
            registry(), int(candidate_file.read_text(encoding="utf-8"))))
        assert changed.applied
        assert changed.effective_config_hash != original.context.effective_config_hash
        before = _snapshot(case.app)
        task = prepare_worker_input_case(case)
        assert task.context == original.context
        assert _snapshot(case.app) == before
    finally:
        case.app.close()
