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


def _host(root, pipe, *, cut=None, identity=None):
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
    try:
        if identity is None:
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
                _audit(root, "provider", request_hash=kwargs["call_binding"].request_hash)
                return await super().post_json(**kwargs)

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

        if cut in {"request", "complete"}:
            original_wait = GeneralWorkerSession.wait
            def wait(self):
                if self._turn.phase == cut:
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

        method = app.execute_general_worker_task if cut is not None else app.recover_general_worker_task
        result = asyncio.run(method(identity=identity, task_text=text))
        pipe.send(dict(point="result", host_pid=os.getpid(), result=asdict(result)))
    except BaseException:
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
    assert cut in {"request", "complete", "accepted"}
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
            assert turn.phase == cut and turn.termination_receipt is None
            assert asdict(turn.launch_receipt) == barrier["receipt"]
            cleanup.append(turn)
            worker = _verified_worker(barrier["receipt"])
            evidence["crashed_worker"] = worker
            if cut == "complete":
                assert previous.capture is not None
                assert [row["kind"] for row in _effects(root)].count("provider") == 1
                assert not (root / "private/artifacts" / hashlib.sha256(OUTPUT).hexdigest()).exists()
                # Only this exact MainPID, after all identity/argv predicates.
                os.kill(worker["pid"], signal.SIGKILL)
                evidence["worker_sigkill"] = True
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
