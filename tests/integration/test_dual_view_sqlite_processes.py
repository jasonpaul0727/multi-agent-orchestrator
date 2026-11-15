"""Owned /tmp SQLite process evidence; SIGKILL is not a power-loss test."""

from contextlib import contextmanager
from dataclasses import replace
import importlib
import importlib.util
import json
from pathlib import Path
import signal
import sys

import pytest

from tests.support import dual_view_simulation as sim


@contextmanager
def process_group(tmp_path, record_property):
    name = "tests.support.dual_view_sqlite_process"
    assert importlib.util.find_spec(name) is not None, "owned SQLite process harness is missing"
    assert sys.platform == "linux", "this gate requires actual Linux processes and SIGKILL"
    assert tmp_path.resolve().is_relative_to(Path("/tmp")), "process DB must be private local /tmp"
    module = importlib.import_module(name)
    with module.ProcessGroup(tmp_path / "process.sqlite") as group:
        record_property("sqlite_process_environment", json.dumps(group.environment_info, sort_keys=True))
        yield group


def item(source=sim.Source.S0, *, history=None, request=sim.Request.R0,
         version=sim.Version.B, fallback=False, expires=8, eligibility_expires=8):
    own_model = sim.Model.M0 if source is sim.Source.S0 else sim.Model.M1
    own_provider = sim.Provider.P0 if source is sim.Source.S0 else sim.Provider.P1
    evidence = sim.Evidence.E0 if source is sim.Source.S0 else sim.Evidence.E1
    model = (sim.Model.M1 if own_model is sim.Model.M0 else sim.Model.M0) if fallback else own_model
    provider = sim.Provider.P0 if model is sim.Model.M0 else sim.Provider.P1
    eligibility = (sim.Eligibility(source, own_model, own_provider, evidence, sim.Policy.P0,
                                  2, eligibility_expires)
                   if version is sim.Version.B and not fallback else None)
    return sim.Envelope(source, request, version, sim.Entry.RECOVERY,
                        sim.Action.FALLBACK if fallback else sim.Action.SAME,
                        model, provider, (own_model,) if history is None else history,
                        eligibility, 0, 0, expires, sim.Fixture.NORMAL)


def seed(keeper, *, source=sim.Source.S0, history=None):
    history = ((sim.Model.M0 if source is sim.Source.S0 else sim.Model.M1),) if history is None else history
    _, out = keeper.apply(sim.RegisterSource(source, history, sim.Fixture.NORMAL))
    assert out.outcome is sim.Outcome.REGISTERED
    for fact in sim.Fact:
        _, out = keeper.apply(sim.SetFacts(source, ((fact, sim.Value.YES),), 0))
        assert out.outcome is sim.Outcome.RECORDED


def propose(keeper, envelope):
    _, out = keeper.apply(sim.Propose(envelope))
    assert out.outcome is sim.Outcome.PROPOSED


def assert_state(state, *, events, winners):
    assert len(state.events) == events
    assert len(state.winners) == winners
    assert state.pending == ()
    assert state.simulation_only is True and state.non_authoritative is True
    for winner in state.winners:
        assert winner.successor_fencing_generation == 3
        assert winner.reservation.successor_fencing_generation == 3
        assert winner.reservation.envelope == winner.envelope
        assert winner.envelope.source_fencing_generation == 2
    for source in state.sources:
        won = any(winner.envelope.source is source.source for winner in state.winners)
        assert (source.budget, source.attempts, source.retry_level) == ((0, 3, 2) if won else (1, 2, 1))


def result(worker, outcome):
    state, observed = worker.result()
    assert observed == outcome
    assert state.pending == ()
    return state


def retry_busy(worker):
    message = worker.receive()
    if message["kind"] == "busy":
        # This is a new explicit test request after the other result, not a
        # store retry loop. The first SQLite BUSY remains observed evidence.
        worker.run()
        return worker.result()
    assert message["kind"] == "result"
    return worker.decode_result(message)


def test_children_use_a_minimal_environment_and_only_test_model_imports(tmp_path, record_property, monkeypatch):
    monkeypatch.setenv("SYNTHETIC_PARENT_ONLY", "do-not-inherit")
    with process_group(tmp_path, record_property) as group:
        child, = group.launch(("read", None))
        ready = child.ready
        assert ready["environment_keys"] == ["LANG", "PATH", "PYTHONDONTWRITEBYTECODE", "PYTHONPATH"]
        assert ready["production_imported"] is False
        assert ready["diagnostics"]["journal_mode"] == "wal"
        assert ready["diagnostics"]["synchronous"] == 2
        assert ready["diagnostics"]["wal_autocheckpoint"] == 0
        assert ready["diagnostics"]["compile_options"]
        state, _ = group.fresh()
        assert state == sim.ModelState()


@pytest.mark.parametrize("excluded", [True, False])
@pytest.mark.parametrize("first", ["v1", "b"])
def test_real_writer_lock_proves_both_cross_version_winner_orders(
        tmp_path, record_property, excluded, first):
    with process_group(tmp_path, record_property) as group:
        history = (sim.Model.M0,) if excluded else ()
        seed(group.keeper, history=history)
        old = item(history=history, request=sim.Request.R1, version=sim.Version.V1, fallback=excluded)
        new = item(history=history)
        propose(group.keeper, old)
        propose(group.keeper, new)
        before = group.keeper.read()
        old_task, new_task = ("v1_fallback" if excluded else "v1_same"), ("b0" if excluded else "b_clear")
        first_task, second_task = (old_task, new_task) if first == "v1" else (new_task, old_task)
        winner, loser = group.launch((first_task, "after_begin"), (second_task, None))
        winner.run()
        winner.phase("after_begin")
        loser.run()
        assert loser.receive()["kind"] == "busy"
        assert group.keeper.read() == before
        winner.release()
        won = result(winner, "WINNER")
        loser.run()
        assert result(loser, "SUPERSEDED") == won
        state, _ = group.fresh()
        assert state == won
        assert_state(state, events=12, winners=1)
        assert state.sources[0].history == history
        assert state.winners[0].envelope.version is (sim.Version.V1 if first == "v1" else sim.Version.B)
        if excluded:
            assert state.winners[0].envelope.action is (sim.Action.FALLBACK if first == "v1" else sim.Action.SAME)


@pytest.mark.parametrize("excluded", [True, False])
def test_both_ready_workers_are_released_before_collecting_either_result(tmp_path, record_property, excluded):
    with process_group(tmp_path, record_property) as group:
        history = (sim.Model.M0,) if excluded else ()
        seed(group.keeper, history=history)
        propose(group.keeper, item(history=history))
        propose(group.keeper, item(history=history, request=sim.Request.R1,
                                  version=sim.Version.V1, fallback=excluded))
        workers = group.launch(("b0" if excluded else "b_clear", None),
                               ("v1_fallback" if excluded else "v1_same", None))
        for worker in workers:
            worker.run()
        results = [retry_busy(worker) for worker in workers]
        assert sorted(outcome for _, outcome in results) == ["SUPERSEDED", "WINNER"]
        state, _ = group.fresh()
        assert_state(state, events=12, winners=1)
        assert state.winners[0].envelope.request in (sim.Request.R0, sim.Request.R1)


@pytest.mark.parametrize("second_task,expected", [("b0", "WINNER"), ("b_changed", "CONFLICT")])
def test_independent_duplicate_or_changed_envelope_never_duplicates_the_debit(
        tmp_path, record_property, second_task, expected):
    with process_group(tmp_path, record_property) as group:
        seed(group.keeper)
        propose(group.keeper, item())
        first, second = group.launch(("b0", None), (second_task, None))
        first.run()
        second.run()
        first_state, first_outcome = retry_busy(first)
        second_state, second_outcome = retry_busy(second)
        assert first_outcome == "WINNER" and second_outcome == expected
        state, _ = group.fresh()
        assert state == first_state
        if expected == "WINNER":
            assert second_state == state
        assert_state(state, events=11, winners=1)


def test_independent_sources_commit_once_each_in_separate_processes(tmp_path, record_property):
    with process_group(tmp_path, record_property) as group:
        for source in sim.Source:
            seed(group.keeper, source=source)
            propose(group.keeper, item(source))
        workers = group.launch(("b0", None), ("s1", None))
        for worker in workers:
            worker.run()
        assert [retry_busy(worker)[1] for worker in workers] == ["WINNER", "WINNER"]
        state, _ = group.fresh()
        assert_state(state, events=22, winners=2)
        assert {winner.envelope.source for winner in state.winners} == {sim.Source.S0, sim.Source.S1}


@pytest.mark.parametrize("update,expected", [("unknown", "RECONCILIATION"), ("generation", "HELD")])
def test_unknown_or_generation_writer_holds_lock_before_competing_commit(
        tmp_path, record_property, update, expected):
    with process_group(tmp_path, record_property) as group:
        seed(group.keeper)
        propose(group.keeper, item())
        before = group.keeper.read()
        updater, contender = group.launch((update, "after_begin"), ("b0", None))
        updater.run()
        updater.phase("after_begin")
        contender.run()
        assert contender.receive()["kind"] == "busy"
        assert group.keeper.read() == before
        updater.release()
        updated = result(updater, "RECORDED")
        contender.run()
        assert result(contender, expected) == updated
        state, _ = group.fresh()
        assert state == updated
        assert_state(state, events=11, winners=0)
        if update == "unknown":
            assert state.sources[0].invalidations == (sim.Invalidation.UNKNOWN,)
        else:
            assert state.closed_generations == ((sim.Source.S0, 0),)


@pytest.mark.parametrize("phase", [
    "after_begin", "after_events", "after_projection", "after_indexes", "before_commit", "after_commit",
])
def test_verified_owned_sigkill_recovers_a_complete_state_in_a_new_process(
        tmp_path, record_property, phase):
    with process_group(tmp_path, record_property) as group:
        seed(group.keeper)
        propose(group.keeper, item())
        before = group.keeper.read()
        worker, = group.launch(("b0", phase))
        worker.run()
        worker.phase(phase)
        worker.kill_at(phase)
        assert worker.process.returncode == -signal.SIGKILL
        if phase == "after_commit":
            assert_state(group.keeper.read(), events=11, winners=1)
        else:
            assert group.keeper.read() == before
        state, retried = group.fresh(retry="b0" if phase == "after_commit" else None)
        if phase == "after_commit":
            assert_state(state, events=11, winners=1)
            retry_state, outcome = retried
            assert outcome == "WINNER" and retry_state == state
        else:
            assert state == before
            assert_state(state, events=10, winners=0)


@pytest.mark.parametrize("already_won", [False, True])
def test_unknown_persists_before_or_after_winner_and_exact_retry_keeps_history(
        tmp_path, record_property, already_won):
    with process_group(tmp_path, record_property) as group:
        seed(group.keeper)
        propose(group.keeper, item())
        if already_won:
            _, out = group.keeper.commit(item())
            assert out.outcome is sim.Outcome.WINNER
        unknown, exact = group.launch(("unknown", None), ("b0", None))
        unknown.run()
        updated = result(unknown, "RECORDED")
        exact.run()
        assert result(exact, "WINNER" if already_won else "RECONCILIATION") == updated
        state, _ = group.fresh()
        assert state == updated
        assert_state(state, events=12 if already_won else 11, winners=1 if already_won else 0)
        if already_won:
            _, observation = sim.step(state, sim.SimulateDispatchCheck(item()))
            assert observation.outcome is sim.Outcome.RECONCILIATION


@pytest.mark.parametrize("task,expected", [
    ("expired", "HELD"), ("elig_expired", "HELD"),
    ("bad_binding", "HELD_INVALID"), ("bad_fence", "HELD_INVALID"),
    ("bad_request", "HELD_INVALID"), ("external_commit", "REJECTED"),
])
def test_fresh_process_rechecks_expiry_binding_request_and_both_fence_boundaries(
        tmp_path, record_property, task, expected):
    with process_group(tmp_path, record_property) as group:
        seed(group.keeper)
        if task in ("expired", "elig_expired"):
            candidate = item(expires=1 if task == "expired" else 8,
                             eligibility_expires=1 if task == "elig_expired" else 8)
            propose(group.keeper, candidate)
            _, out = group.keeper.apply(sim.SetFacts(sim.Source.S0, ((sim.Fact.HEALTH, sim.Value.YES),), 2))
            assert out.outcome is sim.Outcome.RECORDED
        before = group.keeper.read()
        worker, = group.launch((task, None))
        worker.run()
        assert result(worker, expected) == before
        state, _ = group.fresh()
        assert state == before and not state.winners


@pytest.mark.parametrize("cleanup_fault", [False, True])
def test_pidfd_start_failure_reaps_a_real_term_ignoring_child_and_preserves_primary(
        tmp_path, record_property, monkeypatch, cleanup_fault):
    import os
    import subprocess
    import time

    name = "tests.support.dual_view_sqlite_process"
    module = importlib.import_module(name)
    original_popen, original_pidfd_open = subprocess.Popen, os.pidfd_open
    processes, monitor_fds = [], []
    primary = OSError("synthetic pidfd-open failure")
    child_code = (
        "import json,os,signal,sys;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "print(json.dumps({'ignored_term':True,'pid':os.getpid(),'ppid':os.getppid()}),flush=True);"
        "sys.stdin.buffer.read()"
    )

    def spawn_term_ignoring_child(command, **kwargs):
        child = original_popen([sys.executable, "-c", child_code], **kwargs)
        processes.append(child)
        if cleanup_fault:
            original_terminate = child.terminate

            def terminate_then_report_cleanup_failure():
                original_terminate()
                raise RuntimeError("synthetic secondary cleanup error")

            child.terminate = terminate_then_report_cleanup_failure
        return child

    def fail_pidfd_after_exact_ignore_term_ack(pid, flags):
        child = processes[-1]
        assert child.pid == pid
        acknowledgement = module._Reader(child.stdout).read(time.monotonic() + 3)
        assert acknowledgement == {"ignored_term": True, "pid": pid, "ppid": os.getpid()}
        monitor_fds.append(original_pidfd_open(pid, flags))
        raise primary

    with process_group(tmp_path, record_property) as group:
        monkeypatch.setattr(module.subprocess, "Popen", spawn_term_ignoring_child)
        monkeypatch.setattr(module.os, "pidfd_open", fail_pidfd_after_exact_ignore_term_ack)
        monkeypatch.setattr(module, "_JOIN_SECONDS", 0.2)
        try:
            with pytest.raises(OSError) as caught:
                group.launch(("read", None))
            assert caught.value is primary
            child = processes[0]
            assert child.returncode == -signal.SIGKILL
            assert all(stream.closed for stream in (child.stdin, child.stdout, child.stderr))
            assert len(group.workers) == 1
            assert group.workers[0].process is child
            if cleanup_fault:
                assert primary.__notes__ == ["owned SQLite worker startup cleanup also failed"]
            assert group.keeper.read() == sim.ModelState()
        finally:
            # Preserve RED evidence while reaping this test's verified child
            # even when the implementation under test leaks it.
            for index, child in enumerate(processes):
                monitor_fd = monitor_fds[index] if index < len(monitor_fds) else None
                try:
                    if child.poll() is None:
                        if monitor_fd is None:
                            child.kill()
                        else:
                            signal.pidfd_send_signal(monitor_fd, signal.SIGKILL)
                        child.wait(timeout=3)
                finally:
                    for stream in (child.stdin, child.stdout, child.stderr):
                        if not stream.closed:
                            stream.close()
                    if monitor_fd is not None:
                        os.close(monitor_fd)


def _run_owned_path_probe(module, command, *, cwd, env, timeout):
    import subprocess

    child = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    primary = None
    try:
        stdout, stderr = child.communicate(timeout=timeout)
        return child.pid, child.returncode, stdout, stderr
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            failed = module._cleanup_failed_start(child)
        except BaseException:
            failed = True
        if failed:
            if primary is None:
                raise RuntimeError("owned SQLite path probe cleanup failed")
            primary.add_note("owned SQLite path probe cleanup also failed")


@pytest.mark.parametrize("escape", ["parent", "parent_symlink", "final_symlink"])
def test_child_refuses_tmp_path_escapes_and_final_symlinks_before_opening(tmp_path, escape):
    import subprocess

    module = importlib.import_module("tests.support.dual_view_sqlite_process")
    root = Path(__file__).resolve().parents[2]
    missing_name = "p3-refused-" + tmp_path.name + ".sqlite"
    if escape == "parent":
        path = Path("/tmp/../var/tmp") / missing_name
        assert not path.resolve().exists()
    elif escape == "parent_symlink":
        link = tmp_path / "outside-parent"
        link.symlink_to("/var/tmp", target_is_directory=True)
        path = link / missing_name
        assert not path.resolve().exists()
    else:
        from tests.support.dual_view_sqlite_store import create_store
        target = tmp_path / "owned-target.sqlite"
        create_store(target)
        path = tmp_path / "final-link.sqlite"
        path.symlink_to(target)
    environment = {"PATH": "/usr/bin:/bin", "PYTHONPATH": str(root),
                   "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"}
    pid, returncode, stdout, stderr = _run_owned_path_probe(
        module, [sys.executable, "-m", module.__name__, str(path), "read", "none"],
        cwd=tmp_path, env=environment, timeout=5)
    assert returncode == 1
    assert len(stdout) < 256 and stderr == b"synthetic-sqlite-worker-error\n"
    assert json.loads(stdout) == {"kind": "error", "pid": pid, "code": "validation"}
    if escape != "final_symlink":
        assert not path.resolve().exists()


@pytest.mark.parametrize("cleanup_fault", [False, True])
def test_path_probe_timeout_reaps_a_real_paused_child_without_an_unbounded_wait(
        tmp_path, monkeypatch, cleanup_fault):
    import os
    import subprocess
    import time

    module = importlib.import_module("tests.support.dual_view_sqlite_process")
    original_popen = subprocess.Popen
    owned = []
    timeouts = []
    child_code = (
        "import json,os,signal,sys;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "print(json.dumps({'paused':True,'pid':os.getpid(),'ppid':os.getppid()}),flush=True);"
        "signal.pause()"
    )

    def controlled_hanging_child(command, **kwargs):
        child = original_popen([sys.executable, "-c", child_code], **kwargs)
        owned.append((child, None))
        monitor_fd = os.pidfd_open(child.pid, 0)
        owned[-1] = (child, monitor_fd)
        acknowledgement = module._Reader(child.stdout).read(time.monotonic() + 3)
        assert acknowledgement == {"paused": True, "pid": child.pid, "ppid": os.getpid()}
        original_wait, original_communicate = child.wait, child.communicate

        def require_a_bounded_wait(timeout=None):
            # Prevent the regression test itself from hanging if the Popen
            # context manager tries its implicit wait() after a timeout.
            assert timeout is not None, "owned path probe attempted an unbounded wait"
            return original_wait(timeout=timeout)

        def bounded_communicate(*args, **kwargs):
            kwargs["timeout"] = 0.05
            try:
                return original_communicate(*args, **kwargs)
            except subprocess.TimeoutExpired as error:
                timeouts.append(error)
                raise

        child.wait = require_a_bounded_wait
        child.communicate = bounded_communicate
        if cleanup_fault:
            original_terminate = child.terminate

            def terminate_then_report_cleanup_failure():
                original_terminate()
                raise RuntimeError("synthetic path-probe cleanup failure")

            child.terminate = terminate_then_report_cleanup_failure
        return child

    monkeypatch.setattr(subprocess, "Popen", controlled_hanging_child)
    monkeypatch.setattr(module, "_JOIN_SECONDS", 0.2)
    try:
        with pytest.raises(subprocess.TimeoutExpired) as caught:
            test_child_refuses_tmp_path_escapes_and_final_symlinks_before_opening(tmp_path, "parent")
        assert caught.value is timeouts[0]
        child, _ = owned[0]
        assert child.returncode == -signal.SIGKILL
        assert all(stream.closed for stream in (child.stdin, child.stdout, child.stderr))
        if cleanup_fault:
            assert caught.value.__notes__ == ["owned SQLite path probe cleanup also failed"]
    finally:
        for child, monitor_fd in owned:
            try:
                if child.poll() is None:
                    if monitor_fd is None:
                        child.kill()
                    else:
                        signal.pidfd_send_signal(monitor_fd, signal.SIGKILL)
                    child.wait(timeout=3)
            finally:
                for stream in (child.stdin, child.stdout, child.stderr):
                    if not stream.closed:
                        stream.close()
                if monitor_fd is not None:
                    os.close(monitor_fd)
