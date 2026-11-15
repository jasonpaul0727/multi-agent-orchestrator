"""Fixed-purpose owned-process harness for synthetic local SQLite tests only."""

from dataclasses import replace
import json
import os
from pathlib import Path
import selectors
import signal
import sqlite3
import subprocess
import sys
import time

from tests.support import dual_view_simulation as sim
from tests.support.dual_view_sqlite_codec import decode, encode
from tests.support.dual_view_sqlite_store import SqliteDualViewStore, create_store

_MAX_MESSAGE = 128 * 1024
_MAX_STDERR = 128
_START_SECONDS = 20
_CHILD_SECONDS = 30
_JOIN_SECONDS = 3
_PHASES = ("after_begin", "after_events", "after_projection",
           "after_indexes", "before_commit", "after_commit")
_TASKS = ("read", "b0", "b_clear", "b_changed", "s1", "v1_same", "v1_fallback",
          "unknown", "generation", "expired", "elig_expired",
          "bad_binding", "bad_fence", "bad_request", "external_commit")


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate protocol key")
        result[key] = value
    return result


class _Reader:
    def __init__(self, stream):
        self.fd = stream.fileno()
        self.buffer = b""

    def read(self, deadline):
        while True:
            newline = self.buffer.find(b"\n")
            if newline >= 0:
                line, self.buffer = self.buffer[:newline], self.buffer[newline + 1:]
                if len(line) > _MAX_MESSAGE:
                    raise ValueError("oversized synthetic protocol message")
                value = json.loads(line, object_pairs_hook=_pairs)
                if type(value) is not dict:
                    raise ValueError("invalid synthetic protocol message")
                return value
            if len(self.buffer) > _MAX_MESSAGE:
                raise ValueError("oversized synthetic protocol message")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("synthetic protocol deadline exceeded")
            with selectors.DefaultSelector() as selector:
                selector.register(self.fd, selectors.EVENT_READ)
                if not selector.select(remaining):
                    raise TimeoutError("synthetic protocol deadline exceeded")
            chunk = os.read(self.fd, min(4096, _MAX_MESSAGE + 1 - len(self.buffer)))
            if not chunk:
                raise EOFError("synthetic protocol ended before acknowledgement")
            self.buffer += chunk


def _send(stream, message):
    payload = json.dumps(message, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    if len(payload) > _MAX_MESSAGE:
        raise ValueError("oversized synthetic protocol message")
    stream.write(payload + b"\n")
    stream.flush()


def _fixture(task):
    source = sim.Source.S1 if task == "s1" else sim.Source.S0
    model = sim.Model.M1 if source is sim.Source.S1 else sim.Model.M0
    provider = sim.Provider.P1 if source is sim.Source.S1 else sim.Provider.P0
    evidence = sim.Evidence.E1 if source is sim.Source.S1 else sim.Evidence.E0
    history = () if task in ("v1_same", "b_clear") else (model,)
    version = sim.Version.V1 if task in ("v1_same", "v1_fallback") else sim.Version.B
    request = sim.Request.R1 if version is sim.Version.V1 else sim.Request.R0
    eligibility = (sim.Eligibility(source, model, provider, evidence, sim.Policy.P0,
                                  2, 1 if task == "elig_expired" else 8)
                   if version is sim.Version.B else None)
    action = sim.Action.SAME
    if task == "v1_fallback":
        action, model, provider = sim.Action.FALLBACK, sim.Model.M1, sim.Provider.P1
    item = sim.Envelope(source, request, version, sim.Entry.RECOVERY, action, model,
                        provider, history, eligibility, 0, 0,
                        1 if task == "expired" else 8, sim.Fixture.NORMAL)
    if task == "b_changed":
        item = replace(item, entry=sim.Entry.IMPORT)
    elif task == "bad_binding":
        item = replace(item, eligibility=replace(item.eligibility, source=sim.Source.S1))
    elif task == "bad_fence":
        item = replace(item, source_fencing_generation=3)
    elif task == "bad_request":
        item = replace(item, request="synthetic-invalid-request")
    return item


def _production_imported():
    return any(name == "orchestrator" or name.startswith("orchestrator.") for name in sys.modules)


def _child(path, task, pause):
    if task not in _TASKS or pause not in (*_PHASES, "none"):
        raise ValueError("unknown synthetic worker preset")
    if not path.resolve().is_relative_to(Path("/tmp")):
        raise ValueError("synthetic worker requires owned local tmp")
    reader = _Reader(sys.stdin.buffer)
    paused = False

    def emit(kind, **fields):
        _send(sys.stdout.buffer, dict(kind=kind, pid=os.getpid(), **fields))

    def hook(phase):
        nonlocal paused
        if not paused and phase == pause:
            paused = True
            emit("phase", phase=phase)
            if reader.read(time.monotonic() + _CHILD_SECONDS) != {"command": "release"}:
                raise ValueError("synthetic cut requires exact release")

    with SqliteDualViewStore(path) as store:
        if _production_imported():
            raise ValueError("production imports are outside this harness")
        emit("ready", ppid=os.getppid(), diagnostics=dict(store.diagnostics),
             environment_keys=sorted(os.environ), production_imported=False)
        while True:
            message = reader.read(time.monotonic() + _CHILD_SECONDS)
            if message == {"command": "stop"}:
                emit("bye")
                return
            if message not in ({"command": "run"}, {"command": "read"}):
                raise ValueError("unknown synthetic worker command")
            try:
                if message["command"] == "read" or task == "read":
                    state, outcome = store.read(), "REPLAYED"
                elif task == "unknown":
                    state, observation = store.apply(
                        sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN), hook=hook)
                    outcome = observation.outcome.name
                elif task == "generation":
                    state, observation = store.apply(sim.CloseGeneration(sim.Source.S0, 0), hook=hook)
                    outcome = observation.outcome.name
                elif task == "external_commit":
                    try:
                        store.apply(sim.Commit(_fixture(task), 2), hook=hook)
                    except ValueError:
                        state, outcome = store.read(), "REJECTED"
                    else:
                        raise ValueError("external commit was not refused")
                else:
                    state, observation = store.commit(_fixture(task), hook=hook)
                    outcome = observation.outcome.name
            except sqlite3.OperationalError as error:
                if getattr(error, "sqlite_errorcode", 0) & 0xff not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                emit("busy")
                continue
            if state.pending or _production_imported():
                raise ValueError("invalid synthetic worker result")
            emit("result", outcome=outcome, state=encode(state))


def _filesystem(path):
    selected, filesystem = -1, "unreported"
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        mount = before.split()[4].replace("\\040", " ").replace("\\134", "\\")
        if str(path) == mount or str(path).startswith(mount.rstrip("/") + "/"):
            if len(mount) > selected:
                selected, filesystem = len(mount), after.split()[0]
    return filesystem


def _cleanup_failed_start(process):
    failed = False
    if process.returncode is None:
        try:
            process.terminate()
        except BaseException:
            failed = True
        try:
            process.wait(timeout=_JOIN_SECONDS)
        except subprocess.TimeoutExpired:
            pass
        except BaseException:
            failed = True
        if process.returncode is None:
            try:
                # Popen owns this unreaped direct child; pidfd acquisition failed.
                process.kill()
            except BaseException:
                failed = True
            try:
                process.wait(timeout=_JOIN_SECONDS)
            except BaseException:
                failed = True
    for stream in (process.stdin, process.stdout, process.stderr):
        try:
            stream.close()
        except BaseException:
            failed = True
    return failed


class _Worker:
    def __init__(self, path, task, pause, deadline, *, owners=None):
        root = Path(__file__).resolve().parents[2]
        environment = {"PATH": "/usr/bin:/bin", "PYTHONPATH": str(root),
                       "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"}
        self.deadline = deadline
        self.pidfd = None
        self.ready = None
        self._pending_phase = None
        self._finished = False
        self.process = subprocess.Popen(
            [sys.executable, "-m", "tests.support.dual_view_sqlite_process",
             str(path), task, pause or "none"], cwd=path.parent, env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=0, close_fds=True,
        )
        try:
            # Register the Popen handle before any later constructor step can
            # fail, so group cleanup can retry an incomplete startup cleanup.
            if owners is not None:
                owners.append(self)
            self.pidfd = os.pidfd_open(self.process.pid, 0)
        except BaseException as error:
            failed = _cleanup_failed_start(self.process)
            self._finished = (self.process.returncode is not None and all(
                stream.closed for stream in (self.process.stdin, self.process.stdout, self.process.stderr)))
            if failed:
                error.add_note("owned SQLite worker startup cleanup also failed")
            raise
        self.reader = _Reader(self.process.stdout)

    def receive(self, expected=None, *, deadline=None):
        message = self.reader.read(self.deadline if deadline is None else deadline)
        schemas = {
            "ready": {"kind", "pid", "ppid", "diagnostics", "environment_keys", "production_imported"},
            "phase": {"kind", "pid", "phase"}, "result": {"kind", "pid", "outcome", "state"},
            "busy": {"kind", "pid"}, "bye": {"kind", "pid"}, "error": {"kind", "pid", "code"},
        }
        kind = message.get("kind")
        if kind not in schemas or set(message) != schemas[kind] or message["pid"] != self.process.pid:
            raise ValueError("invalid owned worker acknowledgement")
        if expected is not None and kind != expected:
            raise AssertionError("owned worker sent an unexpected acknowledgement kind")
        return message

    def verify_ready(self):
        self.ready = self.receive("ready")
        if self.ready["ppid"] != os.getpid() or self.process.poll() is not None:
            raise AssertionError("worker is not this parent's live direct child")
        diagnostics = self.ready["diagnostics"]
        if (diagnostics["journal_mode"] != "wal" or diagnostics["synchronous"] != 2
                or diagnostics["wal_autocheckpoint"] != 0):
            raise AssertionError("worker lacks required SQLite settings")

    def run(self):
        _send(self.process.stdin, {"command": "run"})

    def phase(self, expected):
        message = self.receive("phase")
        if message["phase"] != expected or expected not in _PHASES:
            raise AssertionError("wrong synthetic cut point")
        self._pending_phase = expected

    def release(self):
        if self._pending_phase is None:
            raise AssertionError("release requires an acknowledged phase")
        _send(self.process.stdin, {"command": "release"})
        self._pending_phase = None

    def decode_result(self, message):
        if message["kind"] != "result" or message["outcome"] not in (
                *(outcome.name for outcome in sim.Outcome), "REJECTED"):
            raise ValueError("invalid synthetic worker result")
        state = decode(message["state"])
        if type(state) is not sim.ModelState or state.pending:
            raise ValueError("invalid synthetic worker state")
        return state, message["outcome"]

    def result(self):
        return self.decode_result(self.receive("result"))

    def _reap(self, expected=None):
        returncode = self.process.wait(timeout=_JOIN_SECONDS)
        stderr = self.process.stderr.read(_MAX_STDERR + 1)
        try:
            if expected is not None and (returncode != expected or stderr):
                raise AssertionError("owned worker exit or bounded stderr was unexpected")
            if len(stderr) > _MAX_STDERR:
                raise AssertionError("owned worker stderr exceeded its fixed bound")
        finally:
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                stream.close()
            os.close(self.pidfd)
            self._finished = True

    def kill_at(self, phase):
        if (self.ready is None or self._pending_phase != phase
                or self.process.poll() is not None):
            raise AssertionError("SIGKILL requires an exact live owned phase acknowledgement")
        signal.pidfd_send_signal(self.pidfd, signal.SIGKILL)
        self._reap(-signal.SIGKILL)

    def finish(self):
        if self._finished:
            return
        if self.process.poll() is None:
            if self._pending_phase is not None:
                raise AssertionError("cannot normally finish a paused cut-point worker")
            _send(self.process.stdin, {"command": "stop"})
            self.receive("bye", deadline=time.monotonic() + _JOIN_SECONDS)
        self._reap(0)

    def abort(self):
        if self._finished:
            return
        if self.pidfd is None:
            failed = _cleanup_failed_start(self.process)
            self._finished = (self.process.returncode is not None and all(
                stream.closed for stream in (self.process.stdin, self.process.stdout, self.process.stderr)))
            if failed or not self._finished:
                raise RuntimeError("owned SQLite worker startup cleanup failed")
            return
        if self.process.poll() is None:
            # Cleanup is separate from a claimed crash cut. The pidfd was opened
            # for our own direct Popen child, including a failed startup.
            signal.pidfd_send_signal(self.pidfd, signal.SIGTERM)
            try:
                self.process.wait(timeout=_JOIN_SECONDS)
            except subprocess.TimeoutExpired:
                signal.pidfd_send_signal(self.pidfd, signal.SIGKILL)
        self._reap()


class ProcessGroup:
    """Own the keeper, bounded children, and final independent replay process."""

    def __init__(self, path):
        self.path = path
        self.workers = []
        self._keeper_open = False

    def __enter__(self):
        if sys.platform != "linux" or not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            raise RuntimeError("this process gate requires Linux pidfd support")
        create_store(self.path)
        self.keeper = SqliteDualViewStore(self.path).__enter__()
        self._keeper_open = True
        self.environment_info = dict(self.keeper.diagnostics)
        self.environment_info.update(
            filesystem=_filesystem(self.path), owned_directory="/tmp",
            workload="auto-checkpoints-disabled; no explicit checkpoint with live workers",
            limitation="SIGKILL process death only; no concurrent-checkpoint or power-loss qualification",
        )
        return self

    def _start(self, task, pause, deadline):
        return _Worker(self.path, task, pause, deadline, owners=self.workers)

    def launch(self, *plans):
        if not self._keeper_open:
            raise ValueError("competing workers require the live parent keeper")
        deadline = time.monotonic() + _START_SECONDS
        workers = [self._start(task, pause, deadline) for task, pause in plans]
        for worker in workers:
            worker.verify_ready()
        return workers

    def _close_keeper(self):
        if any(worker.process.poll() is None for worker in self.workers):
            raise AssertionError("keeper must outlive every competing child")
        if self._keeper_open:
            self._keeper_open = False
            self.keeper.__exit__(None, None, None)

    def fresh(self, *, retry=None):
        for worker in self.workers:
            worker.finish()
        self._close_keeper()
        # No competing worker or keeper connection exists while this independent
        # final process performs complete read/replay and optional exact retry.
        worker = self._start(retry or "read", None, time.monotonic() + _START_SECONDS)
        worker.verify_ready()
        _send(worker.process.stdin, {"command": "read"})
        state, outcome = worker.result()
        if outcome != "REPLAYED":
            raise AssertionError("fresh process did not replay")
        retried = None
        if retry is not None:
            worker.run()
            retried = worker.result()
        worker.finish()
        return state, retried

    def __exit__(self, exc_type, exc, traceback):
        failure = None
        for worker in self.workers:
            try:
                worker.abort() if exc_type is not None or failure is not None else worker.finish()
            except BaseException as error:
                if failure is None:
                    failure = error
                try:
                    worker.abort()
                except BaseException:
                    pass
        try:
            self._close_keeper()
        except BaseException as error:
            if failure is None:
                failure = error
        if failure is not None:
            if exc is None:
                raise failure
            exc.add_note("owned SQLite process cleanup also failed")
        return False


def _main():
    try:
        if len(sys.argv) != 4:
            raise ValueError("fixed synthetic worker arguments required")
        _child(Path(sys.argv[1]), sys.argv[2], sys.argv[3])
    except BaseException as error:
        code = "sqlite" if isinstance(error, sqlite3.Error) else "validation" if isinstance(error, ValueError) else "protocol"
        try:
            _send(sys.stdout.buffer, {"kind": "error", "pid": os.getpid(), "code": code})
            sys.stderr.buffer.write(b"synthetic-sqlite-worker-error\n")
            sys.stderr.buffer.flush()
        except BaseException:
            pass
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
