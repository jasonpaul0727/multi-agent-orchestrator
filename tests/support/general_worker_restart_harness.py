"""Bounded real-process recovery proofs, with explicitly offline Provider/usage.

Only host-side methods are instrumented, after their durable commits. Fixed
Workers, isolated verifiers, grants, private artifacts and stop receipts are real.
The parent owns all crash/restart sequencing within one pytest invocation.
"""

import asyncio
from dataclasses import asdict
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
import traceback

from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
from orchestrator.persistence import SQLiteEventStore


WAIT_SECONDS = 25
OUTPUT = "candidate café".encode()


def _audit(root, kind, **data):
    # A separate durable database makes offline effects observable across hosts.
    with sqlite3.connect(root / "offline-effects.db", timeout=3) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS effects "
            "(sequence INTEGER PRIMARY KEY, host_pid INTEGER, kind TEXT, data TEXT)")
        connection.execute("INSERT INTO effects(host_pid,kind,data) VALUES(?,?,?)",
            (os.getpid(), kind, json.dumps(data, sort_keys=True)))


def _effects(root):
    with sqlite3.connect(root / "offline-effects.db", timeout=3) as connection:
        return [dict(sequence=row[0], host_pid=row[1], kind=row[2], data=json.loads(row[3]))
            for row in connection.execute("SELECT * FROM effects ORDER BY sequence")]


def _rendezvous(pipe, point, **data):
    pipe.send(dict(point=point, host_pid=os.getpid(), **data))
    if not pipe.poll(WAIT_SECONDS):
        raise TimeoutError("parent did not end test-owned host at crash barrier")
    raise AssertionError("crash barrier must end with parent-issued process death")


def _hold(pipe, point, **data):
    pipe.send(dict(point=point, host_pid=os.getpid(), **data))
    assert pipe.poll(WAIT_SECONDS), "parent did not release test-only authority barrier"
    assert pipe.recv() == "continue"


def _install_matrix_hooks(app, root, pipe, identity, cut, options):
    """Host-only phase observations; no replacement of producer authorities."""
    service = app._general_worker_execution
    if cut == "reserved":
        async def invoke(*args, **kwargs):
            _rendezvous(pipe, cut, identity=identity)
        service._gateway.invoke = invoke
    elif cut in {"unknown", "known_failure"}:
        original = service._gateway.provider_call_journal.record_outcome
        def outcome(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs["outcome"] == cut:
                assert not app._event_store._connection.in_transaction
                _rendezvous(pipe, cut, identity=identity)
            return result
        service._gateway.provider_call_journal.record_outcome = outcome
    elif cut == "no_capture":
        def capture(*args, **kwargs):
            # Real Gateway known_success and validated private publication have
            # committed, but no ResponseCaptured checkpoint was appended.
            _rendezvous(pipe, cut, identity=identity)
        service._journal.capture_response = capture
    elif cut == "captured":
        original = service._turn
        async def turn(*args, phase, **kwargs):
            if phase == "complete":
                _rendezvous(pipe, cut, identity=identity)
            return await original(*args, phase=phase, **kwargs)
        service._turn = turn
    elif cut == "proposed":
        async def accept(**kwargs):
            _rendezvous(pipe, cut, identity=identity)
        app._acceptance_coordinator.accept_general_proposal_async = accept
    elif cut == "rollback":
        complete = app._scheduler.agents.complete_attempt
        def fail(*args, **kwargs):
            result = complete(*args, **kwargs)
            assert app._event_store._connection.in_transaction
            assert any(event.event_type == "GeneralWorkerAccepted" for event in
                app._event_store.read_stream("general_worker", identity.stream_id))
            raise RuntimeError("test-only terminal rollback after real terminal writes")
        app._scheduler.agents.complete_attempt = fail
        original = app._acceptance_coordinator.accept_general_proposal_async
        async def accept(**kwargs):
            try:
                await original(**kwargs)
            except Exception:
                assert not app._event_store._connection.in_transaction
                assert service._journal.read(identity).phase == "proposed"
                _audit(root, "terminal_rollback")
                _rendezvous(pipe, cut, identity=identity)
            raise AssertionError("injected terminal transaction unexpectedly committed")
        app._acceptance_coordinator.accept_general_proposal_async = accept

    if options.get("owner_barrier"):
        from orchestrator.runtime.general_worker_recovery import GeneralWorkerOwnership
        original = GeneralWorkerOwnership.acquire
        def acquire(self):
            acquired = original(self)
            if acquired:
                _hold(pipe, "owner", identity=identity)
            return acquired
        GeneralWorkerOwnership.acquire = acquire
    if options.get("usage_barrier"):
        source = app._scheduler._general_usage_source
        original = source.usage_for
        def usage(snapshot):
            result = original(snapshot)
            assert not app._event_store._connection.in_transaction
            _hold(pipe, "usage", identity=identity, lease_expires_at=snapshot.lease_expires_at.isoformat())
            return result
        source.usage_for = usage


def _host(root, pipe, *, cut=None, identity=None, options=None):
    """Spawn entry: each invocation constructs its own App and SQLite connections."""
    from orchestrator.application import ControlPlaneApplication, GeneralWorkerHostConfig
    from orchestrator.isolation import SystemdReadOnlyLauncher
    from orchestrator.runtime.general_worker_process import GeneralWorkerProcess, GeneralWorkerSession
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.lifecycle.test_scheduler import _application_limits, registry
    from tests.unit.runtime.test_attempt_execution_coordinator import TrustedUsage
    from tests.unit.runtime.test_general_worker_authority import OfflineBroker
    from tests.unit.runtime.test_general_worker_execution import execution_case, OfflineSuccessTransport

    app = None
    options = options or {}
    initial = identity is None
    try:
        if identity is None:
            if options.get("short_lease"):
                from datetime import datetime, timedelta, timezone
                from orchestrator.runtime.worker_input_service import WorkerInputService
                original = WorkerInputService.accept_routing_with_input
                def accept_input(self, *args, **kwargs):
                    kwargs["lease_expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=12)
                    return original(self, *args, **kwargs)
                WorkerInputService.accept_routing_with_input = accept_input
            seed = execution_case(root)
            identity, text = seed.identity, seed.text
            seed.app.close()
        else:
            text = "private exact task café"

        class Broker(OfflineBroker):
            def acquire_provider_credential(self, *args, **kwargs):
                _audit(root, "broker")
                return super().acquire_provider_credential(*args, **kwargs)

        class Transport(OfflineSuccessTransport):
            async def post_json(self, **kwargs):
                _audit(root, "transport")
                if cut == "dispatching":
                    _rendezvous(pipe, cut, identity=identity)
                _audit(root, "provider", request_hash=kwargs["call_binding"].request_hash)
                if cut == "dispatching_effect":
                    _rendezvous(pipe, cut, identity=identity)
                if cut == "unknown":
                    from orchestrator.models.transport import HTTPTransportFailed
                    raise HTTPTransportFailed(may_have_been_sent=True)
                result = await super().post_json(**kwargs)
                if cut == "known_failure":
                    from orchestrator.models.transport import HTTPTransportResponse
                    return HTTPTransportResponse(429, (), b'{"error":{"code":"rate_limited"}}',
                        result.termination_receipt)
                return result

        class Usage(TrustedUsage):
            def usage_for(self, snapshot):
                _audit(root, "usage")
                return super().usage_for(snapshot)

        original_launch = SystemdReadOnlyLauncher.launch
        def launch(self, workspace, command, **kwargs):
            session = original_launch(self, workspace, command, **kwargs)
            kind = "worker" if any(str(arg).endswith("/general_worker_process.py")
                for arg in command) else "verifier"
            _audit(root, kind, unit_name=session.unit_name,
                receipt=asdict(session.launch_receipt) if session.launch_receipt else None,
                command=list(map(str, command)))
            if kind == "verifier":
                original_wait = session.wait
                def wait():
                    result = original_wait()
                    _audit(root, "verifier_stop", unit_name=result.unit_name,
                        receipt=asdict(result.termination_receipt), returncode=result.returncode,
                        input_written=result.input_written, stderr=result.stderr.decode(),
                        evidence=json.loads(result.stdout))
                    return result
                session.wait = wait
            return session
        SystemdReadOnlyLauncher.launch = launch

        original_stop = GeneralWorkerProcess.stop_prepared_launch
        def stop(self, prepared, receipt):
            result = original_stop(self, prepared, receipt)
            _audit(root, "recovery_stop", prepared=asdict(prepared), launch=asdict(receipt), stop=asdict(result))
            return result
        GeneralWorkerProcess.stop_prepared_launch = stop

        app = ControlPlaneApplication(root / "private/events.db", limits=_application_limits(),
            artifact_root=root / "private/artifacts", usage_source=Usage(),
            general_worker_config=GeneralWorkerHostConfig(workspace=root / "workspace", registry=registry(),
                secret_broker=Broker(), transport=Transport()))
        journal = GeneralWorkerJournal(app._event_store)
        stat = (root / "private/events.db").stat()
        _audit(root, "app_open", database_device=stat.st_dev, database_inode=stat.st_ino)
        original_complete = app._general_worker_execution._complete
        async def complete(*args, **kwargs):
            try:
                return await original_complete(*args, **kwargs)
            except Exception:
                _audit(root, "completion_error", traceback=traceback.format_exc())
                raise
        app._general_worker_execution._complete = complete
        _install_matrix_hooks(app, root, pipe, identity, cut, options)

        if cut in {"request", "complete", "complete_timeout"}:
            original_wait = GeneralWorkerSession.wait
            def wait(self):
                if self._turn.phase == ("complete" if cut == "complete_timeout" else cut):
                    # _turn calls this only AFTER ack_turn returned/committed,
                    # and BEFORE sandbox.wait writes a single input byte.
                    _rendezvous(pipe, cut, identity=identity, receipt=asdict(self.launch_receipt))
                return original_wait(self)
            GeneralWorkerSession.wait = wait
        elif cut == "accepted":
            original_accept = app._acceptance_coordinator.accept_general_proposal_async
            async def accept(**kwargs):
                result = await original_accept(**kwargs)
                assert result.status == "accepted"
                assert not app._event_store._connection.in_transaction
                _rendezvous(pipe, "accepted", identity=identity, result=asdict(result))
            app._acceptance_coordinator.accept_general_proposal_async = accept

        historical = journal.read(identity)
        if cut is None and historical.accepted_result is not None:
            # A terminal replay must not resolve active input, verify, use
            # trusted usage, acquire credentials, or launch any new process.
            assert not app._scheduler.recovery.recover_control_state(identity.run_id).active_attempts
            def forbidden(*args, **kwargs):
                _audit(root, "forbidden_historical_effect")
                raise AssertionError("terminal recovery crossed an active/external boundary")
            app._worker_inputs._prepare_general_worker_task = forbidden
            app._scheduler._resolve_general_verification_binding = forbidden
            app._general_worker_execution._worker.prepare_launch = forbidden
            app._general_worker_execution._gateway.invoke = forbidden
            app._scheduler._general_usage_source.usage_for = forbidden
            IsolatedVerifierProcess.verify = forbidden
            IsolatedVerifierProcess.verify_async = forbidden

        method = app.execute_general_worker_task if initial or options.get("attacks") else app.recover_general_worker_task
        if options.get("attacks"):
            from dataclasses import replace
            values = [("same", identity, text), ("input", identity, text + " changed"),
                ("run", replace(identity, run_id="rotated-run"), text),
                ("node", replace(identity, node_id="rotated-node"), text),
                ("attempt", replace(identity, attempt_id="rotated-attempt"), text),
                ("generation", replace(identity, fencing_generation=2), text)]
            results = [(attack, asdict(asyncio.run(method(identity=target, task_text=value))))
                for attack, target, value in values]
            recovered = asyncio.run(app.recover_general_worker_task(identity=identity, task_text=text))
            results.append(("explicit_recover", asdict(recovered)))
            pipe.send(dict(point="matrix_results", host_pid=os.getpid(), results=results))
        else:
            result = asyncio.run(method(identity=identity, task_text=text))
            pipe.send(dict(point="result", host_pid=os.getpid(), result=asdict(result)))
            if options.get("repeat"):
                assert pipe.poll(WAIT_SECONDS), "parent did not request historical contender replay"
                assert pipe.recv() == "recover_again"
                result = asyncio.run(method(identity=identity, task_text=text))
                pipe.send(dict(point="result", host_pid=os.getpid(), result=asdict(result)))
    except BaseException:
        from orchestrator.application import StartupRecoveryFailed
        import sys
        if options.get("startup_blocked") and isinstance(sys.exception(), StartupRecoveryFailed):
            pipe.send(dict(point="startup_blocked", host_pid=os.getpid(), code="startup_recovery_failed"))
            return
        pipe.send(dict(point="error", host_pid=os.getpid(), traceback=traceback.format_exc()))
        raise
    finally:
        if app is not None:
            app.close()
        pipe.close()


def _receive(process, pipe):
    assert pipe.poll(WAIT_SECONDS), f"host {process.pid} failed to rendezvous (exit={process.exitcode})"
    result = pipe.recv()
    assert result["point"] != "error", result.get("traceback")
    assert result["host_pid"] == process.pid
    return result


def _kill_host(process):
    assert process.is_alive(), f"test-owned host {process.pid} exited before SIGKILL"
    os.kill(process.pid, signal.SIGKILL)
    process.join(timeout=5)
    assert process.exitcode == -signal.SIGKILL


def _verified_worker(receipt):
    """Match unit/invocation/cgroup and exact actual MainPID argv before kill."""
    deadline = time.monotonic() + 3
    while True:
        observed = _worker_observation(receipt)
        argv = observed["argv"]
        if (argv[:3] == [b"/usr/bin/python3", b"-I", b"-S"] and len(argv) == 5
            and argv[3].endswith(b"/general_worker_process.py") and argv[4] == b"--child"
            and observed["wchan"].endswith("pipe_read")):
            break
        assert time.monotonic() < deadline, observed
        time.sleep(0.01)
    return dict(pid=observed["pid"], argv=[arg.decode() for arg in argv],
        systemd=observed["systemd"], wchan=observed["wchan"])


def _worker_observation(receipt):
    observed = subprocess.run(["systemctl", "--user", "show", receipt["unit_name"],
        "--property=Id,MainPID,InvocationID,ControlGroup,ActiveState"], capture_output=True,
        text=True, check=True, timeout=3)
    values = dict(line.split("=", 1) for line in observed.stdout.splitlines())
    assert values["Id"] == receipt["unit_name"]
    assert values["InvocationID"] == receipt["invocation_id"]
    assert values["ControlGroup"] == receipt["control_group"]
    assert values["ActiveState"] == "active"
    pid = int(values["MainPID"])
    assert pid > 1
    argv = Path(f"/proc/{pid}/cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    assert receipt["control_group"] in Path(f"/proc/{pid}/cgroup").read_text()
    # The fixed child is waiting for host pipe input. No task was delivered.
    assert Path(f"/proc/{pid}/fd/0").readlink().as_posix().startswith("pipe:")
    return dict(pid=pid, argv=argv, systemd=values, wchan=Path(f"/proc/{pid}/wchan").read_text())


def _wait_real_runtime_timeout(receipt):
    """Observe systemd's actual timer, independently of the Scheduler lease."""
    unit = receipt["unit_name"]
    observed = subprocess.run(["systemctl", "--user", "show", unit,
        "--property=RuntimeMaxUSec,InvocationID,MainPID"], capture_output=True, text=True,
        check=True, timeout=3)
    profile = dict(line.split("=", 1) for line in observed.stdout.splitlines())
    assert profile["InvocationID"] == receipt["invocation_id"]
    assert profile["RuntimeMaxUSec"] == "10s"
    started, deadline = time.monotonic(), time.monotonic()+15
    observations = []
    while time.monotonic() < deadline:
        result = subprocess.run(["systemctl", "--user", "show", unit,
            "--property=LoadState,ActiveState,MainPID,Result,ExecMainCode,ExecMainStatus"],
            capture_output=True, text=True, timeout=3)
        values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if not observations or observations[-1] != values:
            observations.append(values)
        if values.get("Result") == "timeout" and values.get("MainPID") == "0":
            break
        if values.get("LoadState") == "not-found":
            break
        time.sleep(0.02)
    journal = subprocess.run(["journalctl", "--user", "--no-pager", "--output=json", "-u", unit],
        capture_output=True, text=True, check=True, timeout=3)
    records = [json.loads(line) for line in journal.stdout.splitlines() if line]
    # A collected unit can no longer expose Result. Its actual systemd journal
    # must still attest the timer firing; absence/elapsed time alone is no proof.
    timer = [row for row in records if "runtime time limit" in row.get("MESSAGE", "")
        and row.get("USER_UNIT", row.get("UNIT")) == unit]
    assert timer, (profile, observations, records)
    assert any("Failed with result 'timeout'" in row.get("MESSAGE", "") for row in records)
    assert not Path(f"/proc/{profile['MainPID']}").exists() or Path(
        f"/proc/{profile['MainPID']}/stat").read_text().split()[2] == "Z"
    return dict(profile=profile, observations=observations, journal=records,
        parent_wait_seconds=time.monotonic()-started)


def _observe(root, identity):
    # Independent SQLite connection proves both visibility and no host write
    # transaction spanning the test rendezvous, before SIGKILL is issued.
    store = SQLiteEventStore(root / "private/events.db")
    try:
        assert not store._connection.in_transaction
        operation = GeneralWorkerJournal(store).read(identity)
        store._connection.execute("BEGIN IMMEDIATE")
        store._connection.execute("ROLLBACK")
        events = [dict(row) for row in store._connection.execute("SELECT * FROM events ORDER BY rowid")]
        if operation.capture is not None:
            ref = operation.capture.response_artifact
            raw = (root / "private/artifacts" / ref.digest.removeprefix("sha256:")).read_bytes()
            assert len(raw) == ref.size_bytes
            assert "sha256:" + hashlib.sha256(raw).hexdigest() == ref.digest
            response = json.loads(raw)
            assert raw == json.dumps(response, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            assert response["output_text"].encode() == OUTPUT
            assert response["tool_calls"] == [] and response["usage"]["input_tokens"] == 1
        return operation, events
    finally:
        store.close()


def _assert_success(root, identity, result, *, previous_operation):
    operation, events = _observe(root, identity)
    assert result["status"] == "accepted", result
    assert asdict(operation.accepted_result) == result
    assert operation.operation_id == previous_operation.operation_id
    assert operation.input_commitment == previous_operation.input_commitment
    artifact, = operation.accepted_result.artifacts
    assert (root / "private/artifacts" / artifact.digest.removeprefix("sha256:")).read_bytes() == OUTPUT
    assert artifact.digest == "sha256:" + hashlib.sha256(OUTPUT).hexdigest()
    assert all(turn.termination_receipt.cgroup_empty for turn in operation.turns)
    succeeded = [row for row in events if row["event_type"] == "AttemptCompleted"
        and json.loads(row["payload_json"])["outcome"] == "succeeded"]
    assert len(succeeded) == 1
    payload = json.loads(succeeded[0]["payload_json"])
    assert payload["task_sha256"] == result["task_sha256"]
    assert payload["evidence_sha256"] == result["evidence_sha256"]
    return operation, events


def run_restart_scenario(root, *, cut):
    """Prove one crash cut; every process/unit is owned and cleanup is bounded."""
    assert cut in {"request", "complete", "complete_timeout", "accepted"}
    started = time.monotonic()
    context = multiprocessing.get_context("spawn")
    first_pipe, first_child = context.Pipe()
    first = context.Process(target=_host, args=(root, first_child), kwargs=dict(cut=cut))
    processes, pipes, cleanup = [first], [first_pipe, first_child], []
    evidence = dict(cut=cut, parent_pid=os.getpid(), offline_provider_and_usage=True)
    try:
        first.start()
        first_child.close()
        barrier = _receive(first, first_pipe)
        assert barrier["point"] == cut, (barrier, _effects(root))
        identity = barrier["identity"]
        previous, old_events = _observe(root, identity)
        evidence.update(first_host_pid=first.pid, identity=asdict(identity), before=asdict(previous))
        if cut != "accepted":
            turn = previous.turns[-1]
            assert turn.phase == ("complete" if cut == "complete_timeout" else cut)
            assert turn.termination_receipt is None
            assert asdict(turn.launch_receipt) == barrier["receipt"]
            cleanup.append(turn)
            worker = _verified_worker(barrier["receipt"])
            evidence["crashed_worker"] = worker
            if cut in {"complete", "complete_timeout"}:
                assert previous.capture is not None
                assert [row["kind"] for row in _effects(root)].count("provider") == 1
                assert not (root / "private/artifacts" / hashlib.sha256(OUTPUT).hexdigest()).exists()
                # Only this exact MainPID, after all identity/argv predicates.
                if cut == "complete":
                    os.kill(worker["pid"], signal.SIGKILL)
                    evidence["worker_sigkill"] = True
                else:
                    evidence["real_runtime_timeout"] = _wait_real_runtime_timeout(barrier["receipt"])
            else:
                assert previous.capture is None
                assert [row["kind"] for row in _effects(root)].count("provider") == 0
        else:
            assert asdict(previous.accepted_result) == barrier["result"]
            assert previous.acceptance is not None

        before_effects = _effects(root)
        _kill_host(first)
        parent, child = context.Pipe()
        second = context.Process(target=_host, args=(root, child), kwargs=dict(identity=identity))
        processes.append(second)
        pipes.extend((parent, child))
        second.start()
        child.close()
        assert second.pid not in {os.getpid(), first.pid}
        reply = _receive(second, parent)
        assert reply["point"] == "result"
        second.join(timeout=5)
        assert second.exitcode == 0
        assert reply["result"]["status"] == "accepted", (reply, _effects(root))
        operation, events = _assert_success(root, identity, reply["result"], previous_operation=previous)
        effects = _effects(root)
        kinds = [row["kind"] for row in effects]
        opened = [row for row in effects if row["kind"] == "app_open"]
        assert [row["host_pid"] for row in opened] == [first.pid, second.pid]
        assert opened[0]["data"] == opened[1]["data"]
        assert kinds.count("provider") == kinds.count("broker") == kinds.count("usage") == 1
        assert "forbidden_historical_effect" not in kinds
        assert kinds.count("verifier") == 2
        verified = [row for row in effects if row["kind"] == "verifier_stop"]
        assert len(verified) == 2
        assert len({row["data"]["unit_name"] for row in verified}) == 2
        for row in verified:
            assert row["data"]["receipt"]["cgroup_empty"] is True
            assert row["data"]["returncode"] == 0 and row["data"]["input_written"]
            assert row["data"]["stderr"] == ""
            assert row["data"]["evidence"]["outcome"] == "accepted"
            assert row["data"]["evidence"]["context"] == operation.context.model_dump(mode="json")
            assert row["data"]["evidence"]["inspected_digests"] == [operation.capture.output_digest]
            assert row["data"]["evidence"]["checks"] == [{"check_id": "artifact-integrity",
                "evidence_digests": [operation.capture.output_digest], "passed": True}]
        if cut == "accepted":
            assert operation == previous
            assert events == old_events
            assert reply["result"] == barrier["result"]
            assert [row for row in effects if row["kind"] != "app_open"] == [
                row for row in before_effects if row["kind"] != "app_open"]
            assert kinds.count("worker") == 2
        else:
            assert [turn.phase for turn in operation.turns] == (
                ["request", "request", "complete"] if cut == "request" else ["request", "complete", "complete"])
            stopped, = [row for row in effects if row["kind"] == "recovery_stop"]
            assert stopped["host_pid"] == second.pid
            assert stopped["data"]["launch"] == barrier["receipt"]
            assert stopped["data"]["stop"]["cgroup_empty"] is True
            assert stopped["data"]["stop"]["control_group"] == barrier["receipt"]["control_group"]
            assert operation.turns[len(previous.turns)-1].termination_receipt.cgroup_empty
            new_launches = [row for row in effects if row["host_pid"] == second.pid and row["kind"] == "worker"]
            assert new_launches and all(row["sequence"] > stopped["sequence"] for row in new_launches)
            assert kinds.count("worker") == 3
            assert all(row["host_pid"] == second.pid for row in effects if row["kind"] == "verifier")
        evidence.update(second_host_pid=second.pid, after=asdict(operation), effects=effects,
            success_event_count=1, provider_count=kinds.count("provider"))
    finally:
        for process in processes:
            if process.pid is not None:
                if process.is_alive():
                    process.kill()
                process.join(timeout=5)
                assert not process.is_alive(), f"test-owned host {process.pid} survived cleanup"
        # Use only recorded exact acknowledged epochs; never global unit kills.
        from orchestrator.isolation import SystemdReadOnlyLauncher
        for turn in cleanup:
            receipt = SystemdReadOnlyLauncher().stop_prepared_launch(turn.prepared, turn.launch_receipt)
            assert receipt.cgroup_empty
        for pipe in pipes:
            pipe.close()
        evidence["elapsed_seconds"] = time.monotonic() - started
        evidence["cleanup_completed"] = True
        (root / "restart-evidence.json").write_text(json.dumps(evidence, sort_keys=True, indent=2,
            default=lambda value: value.model_dump(mode="json")))
        print(f"real-restart evidence: {root / 'restart-evidence.json'}", flush=True)


class _ProcessMatrix:
    """Test-owned subprocess inventory and retained evidence, even on failure."""

    def __init__(self, root, scenario):
        self.root, self.processes, self.pipes = root, [], []
        self.evidence = dict(scenario=scenario, parent_pid=os.getpid(), offline_provider_and_usage=True)
        self.started = time.monotonic()

    def __enter__(self):
        return self

    def start(self, *, cut=None, identity=None, **options):
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=_host, args=(self.root, child),
            kwargs=dict(cut=cut, identity=identity, options=options))
        self.processes.append(process)
        self.pipes.extend((parent, child))
        process.start()
        child.close()
        assert process.pid not in {os.getpid(), *(p.pid for p in self.processes[:-1])}
        return process, parent

    def receive(self, process, pipe, point):
        message = _receive(process, pipe)
        assert message["point"] == point, (message, _effects(self.root))
        return message

    def finished(self, process):
        process.join(timeout=5)
        assert process.exitcode == 0

    def __exit__(self, error_type, error, tb):
        errors, cleaned = [], []
        for process in self.processes:
            if process.pid is not None:
                if process.is_alive():
                    process.kill()
                process.join(timeout=5)
                if process.is_alive():
                    errors.append(f"test-owned host {process.pid} not reaped")
        try:
            effects = _effects(self.root)
            from orchestrator.isolation import (
                PreparedSandboxLaunch, SandboxLaunchReceipt, SystemdReadOnlyLauncher,
            )
            # Audit entries came only from actual launcher return values. This
            # test cleanup uses exact acknowledged epochs, never invented OS
            # evidence or caller proof. Completed Verifiers already have real
            # terminal receipts; crash barriers never leave one running.
            for row in effects:
                if row["kind"] != "worker":
                    continue
                receipt = SandboxLaunchReceipt(**row["data"]["receipt"])
                prepared = PreparedSandboxLaunch(receipt.launch_id, receipt.unit_name)
                stop = SystemdReadOnlyLauncher().stop_prepared_launch(prepared, receipt)
                assert stop.cgroup_empty
                cleaned.append(asdict(stop))
            self.evidence["effects"] = effects
        except Exception:
            errors.append(traceback.format_exc())
        finally:
            for pipe in self.pipes:
                pipe.close()
            self.evidence.update(host_pids=[p.pid for p in self.processes], cleanup_receipts=cleaned,
                cleanup_errors=errors, cleanup_completed=not errors,
                elapsed_seconds=time.monotonic()-self.started, failure=repr(error) if error else None)
            (self.root / "matrix-evidence.json").write_text(json.dumps(self.evidence, sort_keys=True,
                indent=2, default=lambda value: value.model_dump(mode="json")))
            print(f"real-matrix evidence: {self.root / 'matrix-evidence.json'}", flush=True)
        if errors and error is not None:
            error.add_note("Test-owned cleanup errors: " + repr(errors))
        elif errors:
            raise AssertionError(errors)


def _durable(root):
    from orchestrator.budget import BudgetLedger
    store = SQLiteEventStore(root / "private/events.db")
    try:
        return dict(budget=BudgetLedger(store).available("run-1").model_dump(mode="json"),
            events=[dict(row) for row in store._connection.execute("SELECT * FROM events ORDER BY rowid")])
    finally:
        store.close()


def _counts(root):
    effects = _effects(root)
    return {kind: sum(row["kind"] == kind for row in effects) for kind in
        ("provider", "transport", "broker", "worker", "verifier", "verifier_stop", "usage", "recovery_stop")}


def _mutate_authority_or_capture(root, identity, mutation):
    from orchestrator.application import ControlPlaneApplication
    from tests.unit.lifecycle.test_scheduler import _application_limits
    from tests.support.worker_input import corrupt_worker_input_event
    with ControlPlaneApplication(root / "private/events.db", limits=_application_limits(),
        artifact_root=root / "private/artifacts") as app:
        if mutation in {"pause", "cancel"}:
            method = app._scheduler.lifecycle.pause_run if mutation == "pause" else app._scheduler.lifecycle.request_cancel
            method(identity.run_id, reason_code="user_request")
        elif mutation == "fencing":
            # Explicitly hostile durable route fixture, not a claim that a
            # legitimate new generation was admitted while the old hold exists.
            corrupt_worker_input_event(app._event_store, ("scheduler", "global"),
                lambda payload: payload["accepted_route"].update(fencing_generation=2))
        else:
            operation = GeneralWorkerJournal(app._event_store).read(identity)
            ref = operation.capture.response_artifact
            path = app._artifact_store._path_for_digest(ref.digest)
            if mutation == "bytes":
                path.write_bytes(b"corrupt offline response bytes")
            elif mutation == "missing":
                # Retain bytes as evidence; make only the exact private path
                # unavailable. This is not an authority-store copy/restart.
                path.rename(path.with_name(path.name + ".unavailable-evidence"))
            elif mutation in {"source", "scope"}:
                def corrupt(payload):
                    if mutation == "source":
                        payload["source"]["general_worker_operation_id"] = "other-operation"
                    else:
                        payload["readable_scope"] = ["other-operation"]
                corrupt_worker_input_event(app._event_store, ("artifact", ref.digest), corrupt)
            elif mutation == "capture":
                corrupt_worker_input_event(app._event_store, ("general_worker", identity.stream_id),
                    lambda payload: payload["data"].update(output_byte_count=999))
            else:
                raise AssertionError(mutation)


def run_blocked_restart(root, *, cut, attacks=False, mutation=None):
    with _ProcessMatrix(root, f"blocked:{cut}:{mutation}") as matrix:
        old, old_pipe = matrix.start(cut=cut)
        barrier = matrix.receive(old, old_pipe, cut)
        identity = barrier["identity"]
        operation, _ = _observe(root, identity)
        matrix.evidence.update(before=asdict(operation), identity=asdict(identity))
        if cut == "request":
            matrix.evidence["old_worker"] = _verified_worker(barrier["receipt"])
        else:
            assert all(turn.termination_receipt.cgroup_empty for turn in operation.turns)
        if cut in {"reserved", "dispatching", "dispatching_effect", "unknown", "known_failure", "no_capture"}:
            assert operation.phase == "call_reserved" and operation.capture is None
            from orchestrator.models.provider_calls import SQLiteProviderCallJournal
            store = SQLiteEventStore(root / "private/events.db")
            try:
                call = SQLiteProviderCallJournal(store).read_call(operation.call.provider_call_stream_id)
                expected = {"reserved": None, "dispatching": "dispatching", "dispatching_effect": "dispatching",
                    "unknown": "unknown", "known_failure": "known_failure", "no_capture": "known_success"}[cut]
                assert (call.status if call else None) == expected
                matrix.evidence["provider_status"] = expected
                if cut == "no_capture":
                    publications = [event for stream in store.stream_ids("artifact")
                        for event in store.read_stream("artifact", stream)
                        if event.event_type == "ArtifactPublished"
                        and event.payload.get("source", {}).get("general_worker_kind") == "response"]
                    assert len(publications) == 1
                    path = root / "private/artifacts" / publications[0].payload["digest"].removeprefix("sha256:")
                    raw = path.read_bytes()
                    assert "sha256:" + hashlib.sha256(raw).hexdigest() == publications[0].payload["digest"]
                    assert json.loads(raw)["output_text"].encode() == OUTPUT
                    matrix.evidence["uncheckpointed_response_publication"] = publications[0].payload
            finally:
                store.close()
        _kill_host(old)
        if mutation:
            _mutate_authority_or_capture(root, identity, mutation)
        before = _durable(root)
        counts = _counts(root)
        assert counts["provider"] == (1 if cut in {"dispatching_effect", "unknown", "known_failure", "no_capture", "captured"} else 0)
        assert counts["transport"] == (0 if cut in {"reserved", "request"} else 1)
        assert before["budget"]["reserved_tokens"] + before["budget"]["unknown_tokens"] > 0
        new, pipe = matrix.start(identity=identity, attacks=attacks,
            startup_blocked=mutation in {"bytes", "missing", "source", "scope", "capture"})
        if mutation in {"bytes", "missing", "source", "scope", "capture"}:
            message = _receive(new, pipe)
            assert message["point"] in {"startup_blocked", "result"}, message
            if message["point"] == "result":
                assert message["result"]["status"] == "blocked"
        elif attacks:
            message = matrix.receive(new, pipe, "matrix_results")
            assert len(message["results"]) == 7
            assert all(result["status"] == "blocked" for _, result in message["results"])
        else:
            message = matrix.receive(new, pipe, "result")
            assert message["result"]["status"] == "blocked"
        matrix.finished(new)
        after = _durable(root)
        delta = {key: value-counts[key] for key, value in _counts(root).items()}
        assert {key: value for key, value in delta.items() if key != "recovery_stop"} == {
            key: 0 for key in counts if key != "recovery_stop"}
        assert after["budget"] == before["budget"]
        if cut == "request":
            assert delta["recovery_stop"] == 1
            recovered, _ = _observe(root, identity)
            assert len(recovered.turns) == 1 and recovered.turns[0].termination_receipt.cgroup_empty
        else:
            assert delta["recovery_stop"] == 0 and after == before
        matrix.evidence.update(result=message, baseline_counts=counts, new_effect_delta=delta,
            before_budget=before["budget"], after_budget=after["budget"])


def run_competing_recovery(root):
    with _ProcessMatrix(root, "two-independent-recovered-owners") as matrix:
        old, old_pipe = matrix.start(cut="captured")
        barrier = matrix.receive(old, old_pipe, "captured")
        identity = barrier["identity"]
        previous, _ = _observe(root, identity)
        _kill_host(old)
        winner, winner_pipe = matrix.start(identity=identity, owner_barrier=True)
        matrix.receive(winner, winner_pipe, "owner")
        contender, contender_pipe = matrix.start(identity=identity, repeat=True)
        busy = matrix.receive(contender, contender_pipe, "result")
        assert busy["result"]["status"] == "in_progress"
        assert _counts(root) == dict(provider=1, transport=1, broker=1, worker=1, verifier=0, verifier_stop=0, usage=0, recovery_stop=0)
        winner_pipe.send("continue")
        accepted = matrix.receive(winner, winner_pipe, "result")
        matrix.finished(winner)
        _assert_success(root, identity, accepted["result"], previous_operation=previous)
        before = _durable(root)
        counts = _counts(root)
        contender_pipe.send("recover_again")
        replay = matrix.receive(contender, contender_pipe, "result")
        matrix.finished(contender)
        assert replay["result"] == accepted["result"]
        assert _durable(root) == before and _counts(root) == counts
        assert counts == dict(provider=1, transport=1, broker=1, worker=2, verifier=2, verifier_stop=2, usage=1, recovery_stop=0)
        assert sum(row["event_type"] == "CostCommitted" for row in before["events"]) == 1
        matrix.evidence.update(busy=busy, winner=accepted, historical_contender=replay, counts=counts,
            success_and_settlement_count=1)


def run_late_authority_withdrawal(root, *, withdrawal):
    from datetime import datetime, timezone
    with _ProcessMatrix(root, "late-authority:"+withdrawal) as matrix:
        old, old_pipe = matrix.start(cut="proposed", short_lease=withdrawal == "expiry")
        barrier = matrix.receive(old, old_pipe, "proposed")
        identity = barrier["identity"]
        previous, _ = _observe(root, identity)
        assert previous.phase == "proposed"
        _kill_host(old)
        new, pipe = matrix.start(identity=identity, usage_barrier=True)
        gate = matrix.receive(new, pipe, "usage")
        counts = _counts(root)
        assert counts == dict(provider=1, transport=1, broker=1, worker=2, verifier=2, verifier_stop=2, usage=1, recovery_stop=0)
        if withdrawal == "expiry":
            expires = datetime.fromisoformat(gate["lease_expires_at"])
            deadline = time.monotonic() + 15
            while datetime.now(timezone.utc) <= expires:
                assert time.monotonic() < deadline, "real lease did not expire within bounded test window"
                time.sleep(0.02)
            matrix.evidence["real_expiry"] = dict(lease_expires_at=expires.isoformat(),
                observed_at=datetime.now(timezone.utc).isoformat())
        else:
            _mutate_authority_or_capture(root, identity, withdrawal)
        before = _durable(root)
        pipe.send("continue")
        reply = matrix.receive(new, pipe, "result")
        matrix.finished(new)
        assert reply["result"]["status"] == "blocked"
        after = _durable(root)
        assert before == after and _counts(root) == counts
        assert not any(row["event_type"] in {"GeneralWorkerAccepted", "CostCommitted", "AttemptSlotReleased"}
            for row in after["events"])
        assert after["budget"]["reserved_tokens"] + after["budget"]["unknown_tokens"] > 0
        matrix.evidence.update(result=reply, counts=counts, retained_budget=after["budget"],
            withdrawal_fixture="hostile durable route tamper" if withdrawal == "fencing" else "public lifecycle / real UTC")


def run_rollback_recovery(root):
    with _ProcessMatrix(root, "terminal-rollback-proposed-recovery") as matrix:
        old, old_pipe = matrix.start(cut="rollback")
        barrier = matrix.receive(old, old_pipe, "rollback")
        identity = barrier["identity"]
        previous, _ = _observe(root, identity)
        before = _durable(root)
        assert previous.phase == "proposed" and previous.accepted_result is None
        assert not any(row["event_type"] in {"GeneralWorkerAccepted", "CostCommitted", "AttemptCompleted",
            "AttemptSlotReleased"} for row in before["events"])
        assert _counts(root) == dict(provider=1, transport=1, broker=1, worker=2, verifier=2, verifier_stop=2, usage=1, recovery_stop=0)
        _kill_host(old)
        new, pipe = matrix.start(identity=identity)
        accepted = matrix.receive(new, pipe, "result")
        matrix.finished(new)
        operation, events = _assert_success(root, identity, accepted["result"], previous_operation=previous)
        assert operation.turns == previous.turns and operation.capture == previous.capture
        counts = _counts(root)
        assert counts == dict(provider=1, transport=1, broker=1, worker=2, verifier=3, verifier_stop=3, usage=2, recovery_stop=0)
        assert sum(row["event_type"] == "CostCommitted" for row in events) == 1
        assert sum(row["event_type"] == "GeneralWorkerAccepted" for row in events) == 1
        matrix.evidence.update(result=accepted, counts=counts, success_and_settlement_count=1,
            rollback_before=before["budget"])
