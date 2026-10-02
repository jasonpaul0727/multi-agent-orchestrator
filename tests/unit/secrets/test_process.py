"""Gateway client binding and fail-closed process ownership checks."""
import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import struct

import pytest

from orchestrator.config.models import ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets import SecretAccessRule


def _apis():
    import orchestrator.secrets as secrets
    assert hasattr(secrets, "UnixSocketSecretBroker"), "Gateway client API is absent"
    assert hasattr(secrets, "SecretBrokerProcessManager"), "process manager API is absent"
    return secrets.UnixSocketSecretBroker, secrets.SecretBrokerProcessManager


def _provider_and_rule(run_id):
    provider = ProviderSpec(id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True)
    return provider, SecretAccessRule(provider.id, provider.secret_ref, provider.effective_endpoint,
                                      "model_inference", frozenset({run_id}))


def _context_for(run_id, request_id="request-1"):
    return SecretAccessContext(request_id=request_id, run_id=run_id, node_id="node-1",
                               attempt_id="attempt-1", fencing_generation=1,
                               accepted_route_id="route-1", budget_reservation_id="reservation-1")


def _manager(tmp_path):
    _, manager = _apis()
    events = tmp_path / "events.db"
    with SQLiteEventStore(events):
        pass
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    return manager(runtime_root=runtime, event_store_path=events), runtime


def _acquire(client, provider, request_id="request-1"):
    return asyncio.run(client.acquire_provider_credential(secret_ref=provider.secret_ref,
        provider=provider, endpoint=provider.effective_endpoint, purpose="model_inference",
        context=_context_for("run-1", request_id)))


def test_manager_exposes_client_only_after_ready_and_closes_exact_child(tmp_path, monkeypatch):
    # Inheriting the host environment or returning before ready breaks this boundary.
    monkeypatch.setenv("BROKER_ENV_CANARY", "synthetic-environment-marker")
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    session = manager.start(rules=(rule,), providers={provider.id: provider})
    try:
        assert session.process_pid > 0 and session.is_alive
        assert session.socket_path.stat().st_mode & 0o777 == 0o600
        assert session.socket_path.parent.stat().st_mode & 0o777 == 0o700
        assert b"BROKER_ENV_CANARY" not in Path(f"/proc/{session.process_pid}/environ").read_bytes()
        assert _acquire(session.client, provider) is None
    finally:
        session.close()
    session.close()
    assert not session.is_alive and not session.socket_path.exists()
    assert list(runtime.iterdir()) == []
    with pytest.raises(RuntimeError):
        _ = session.client


@pytest.mark.parametrize("change", [
    {"request_id": "wrong"}, {"provider_id": "wrong"},
    {"endpoint": "https://wrong.example/v1"}, {"purpose": "tool_access"},
    {"header_name": "x-api-key"}, {"status": "unknown"},
])
def test_client_rejects_mismatched_response_binding(tmp_path, change):
    # Each wrong wire field must be rejected before a credential is constructed.
    client_type, _ = _apis()
    provider, _ = _provider_and_rule("run-1")
    path = tmp_path / "fake.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    listener.bind(str(path)); listener.listen(1)
    record = dict(version=1, request_id="request-1", provider_id="primary",
                  endpoint="https://api.openai.com/v1", purpose="model_inference",
                  status="credential", header_name="authorization", credential_value="synthetic-value")
    record.update(change)
    def peer():
        with listener.accept()[0] as connection:
            connection.recv(16384)
            connection.send(json.dumps(record).encode())
    thread = threading.Thread(target=peer)
    thread.start()
    try:
        with pytest.raises(RuntimeError):
            _acquire(client_type(socket_path=path, session_nonce="a" * 64), provider)
    finally:
        listener.close(); thread.join(2)
    assert not thread.is_alive()


@pytest.mark.parametrize("fault", ["host", "permissions", "missing", "symlink", "relative", "long"])
def test_manager_rejects_unsafe_host_paths_before_launch(tmp_path, monkeypatch, fault):
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    if fault == "host":
        monkeypatch.setattr(sys, "platform", "darwin")
    elif fault == "permissions":
        runtime.chmod(0o755)
    elif fault == "missing":
        manager._event_store_path.unlink()
    elif fault == "symlink":
        original = manager._event_store_path
        original.rename(tmp_path / "real.db")
        original.symlink_to(tmp_path / "real.db")
    elif fault == "relative":
        manager._event_store_path = Path("events.db")
    else:
        runtime = runtime / ("long" * 30)
        runtime.mkdir(mode=0o700)
        manager._runtime_root = runtime
    def forbidden(*args, **kwargs):
        pytest.fail("unsafe configuration launched a child")
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises((ValueError, RuntimeError)):
        manager.start(rules=(rule,), providers={provider.id: provider})


def test_oversized_bootstrap_fails_before_child_launch(tmp_path, monkeypatch):
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    huge = replace(rule, allowed_run_ids=frozenset(f"run-{n:06d}" for n in range(30000)))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: pytest.fail("oversized bootstrap launched"))
    with pytest.raises(ValueError):
        manager.start(rules=(huge,), providers={provider.id: provider})
    assert list(runtime.iterdir()) == []


@pytest.mark.parametrize("fault", ["timeout", "exit", "mode", "owner"])
def test_startup_failure_withholds_client_and_reaps_child(tmp_path, monkeypatch, fault):
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    import orchestrator.secrets.process as process
    launched = []
    real_popen = subprocess.Popen
    def launch(*args, **kwargs):
        child = real_popen(*args, **kwargs)
        launched.append(child)
        return child
    monkeypatch.setattr(subprocess, "Popen", launch)
    if fault in {"timeout", "exit"}:
        def not_ready(*args, **kwargs):
            raise RuntimeError("readiness failed") if fault == "exit" else TimeoutError()
        monkeypatch.setattr(process, "_read_frame", not_ready)
    else:
        real_check = process._checked_socket_identity
        def invalid(path):
            if fault == "mode":
                path.chmod(0o644)
                return real_check(path)
            real_uid = os.getuid()
            with monkeypatch.context() as owner_patch:
                owner_patch.setattr(os, "getuid", lambda: real_uid + 1)
                return real_check(path)
        monkeypatch.setattr(process, "_checked_socket_identity", invalid)
    with pytest.raises(RuntimeError):
        manager.start(rules=(rule,), providers={provider.id: provider})
    assert launched and launched[0].poll() is not None
    assert list(runtime.iterdir()) == []


def test_changed_socket_inode_is_retained_after_close(tmp_path):
    # Unlinking a replacement pathname would delete another owner's resource.
    manager, _ = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    session = manager.start(rules=(rule,), providers={provider.id: provider})
    old_path = session.socket_path.with_name("original.sock")
    session.socket_path.rename(old_path)
    session.socket_path.write_text("replacement")
    session.close()
    assert not session.is_alive
    assert session.socket_path.read_text() == "replacement"
    assert old_path.exists()


def test_sigterm_grace_escalates_to_sigkill_for_exact_child(tmp_path):
    # A stopped real child cannot process TERM: only bounded KILL escalation reaps it.
    import signal
    manager, _ = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    session = manager.start(rules=(rule,), providers={provider.id: provider})
    os.kill(session.process_pid, signal.SIGSTOP)
    session.close(timeout_seconds=0.05)
    assert not session.is_alive and session._process.returncode == -signal.SIGKILL
    assert not session.socket_path.exists()


def test_unconfirmed_stop_retains_runtime_path_and_disables_client(tmp_path, monkeypatch):
    # An unconfirmed wait must never authorize path removal or client reuse.
    manager, _ = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    session = manager.start(rules=(rule,), providers={provider.id: provider})
    real_wait = session._process.wait
    def unconfirmed(*args, **kwargs):
        raise subprocess.TimeoutExpired("broker", 0.01)
    monkeypatch.setattr(session._process, "wait", unconfirmed)
    try:
        with pytest.raises(RuntimeError):
            session.close(timeout_seconds=0.01)
        assert session.socket_path.parent.exists()
        with pytest.raises(RuntimeError):
            _ = session.client
    finally:
        monkeypatch.setattr(session._process, "wait", real_wait)
        session.close()


@pytest.mark.parametrize("supervised", [False, True])
def test_server_socket_cleanup_obeys_lifecycle_owner(tmp_path, supervised):
    # Supervised shutdown must retain the socket until process exit is confirmed.
    from orchestrator.secrets.server import UnixSecretBrokerServer
    from orchestrator.secrets.broker import UnavailableSecretValueStore
    provider, rule = _provider_and_rule("run-1")
    server = UnixSecretBrokerServer(socket_path=tmp_path / "broker.sock", session_nonce="a" * 64,
        event_store_path=tmp_path / "events.db", rules=(rule,), providers={provider.id:provider},
        value_store=UnavailableSecretValueStore(), expected_uid=os.getuid(), unlink_on_close=not supervised)
    reader, writer = os.pipe()
    thread = threading.Thread(target=server.serve_forever, kwargs={"readiness_fd": writer})
    thread.start()
    try:
        assert os.read(reader, 100)
        server.close(); thread.join(3)
        assert not thread.is_alive()
        assert server.socket_path.exists() is supervised
    finally:
        os.close(reader); server.close(); thread.join(3)


@pytest.mark.parametrize("record", [b'{"status":"ready","status":"ready"}', b"{}" * 513])
def test_readiness_rejects_duplicate_or_oversized_frames(record):
    # Removing duplicate detection or the frame cap accepts untrusted readiness.
    from orchestrator.secrets.process import _read_frame
    reader, writer = os.pipe()
    try:
        os.write(writer, struct.pack("!I", len(record)) + record)
        with pytest.raises(ValueError):
            _read_frame(reader, deadline=time.monotonic() + 1, max_bytes=1024)
    finally:
        os.close(reader); os.close(writer)


def test_readiness_deadline_bounds_an_incomplete_frame():
    from orchestrator.secrets.process import _read_frame
    reader, writer = os.pipe()
    try:
        os.write(writer, b"\0\0")
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            _read_frame(reader, deadline=started + 0.05, max_bytes=1024)
        assert time.monotonic() - started < 0.5
    finally:
        os.close(reader); os.close(writer)


def test_child_exit_before_readiness_is_reaped(tmp_path, monkeypatch):
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    real_popen = subprocess.Popen
    children = []
    def exiting(args, **kwargs):
        child = real_popen([sys.executable, "-c", "import sys; sys.stdin.buffer.read(); sys.exit(3)"], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(subprocess, "Popen", exiting)
    with pytest.raises(RuntimeError):
        manager.start(rules=(rule,), providers={provider.id:provider})
    assert children[0].returncode == 3
    assert list(runtime.iterdir()) == []


def test_readiness_rejects_changed_private_directory_permissions(tmp_path, monkeypatch):
    # Socket checks alone must not expose a client through a now-public directory.
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    import orchestrator.secrets.process as process
    original = process._checked_socket_identity
    def expose_directory(path):
        path.parent.chmod(0o755)
        return original(path)
    monkeypatch.setattr(process, "_checked_socket_identity", expose_directory)
    with pytest.raises(RuntimeError):
        session = manager.start(rules=(rule,), providers={provider.id:provider})
        try:
            pytest.fail("public runtime directory exposed a client")
        finally:
            session.close()


def test_timeout_after_socket_bind_retains_path_without_attested_identity(tmp_path, monkeypatch):
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    import orchestrator.secrets.process as process
    real_read = process._read_frame
    def lose_readiness(*args, **kwargs):
        real_read(*args, **kwargs)
        raise TimeoutError()
    monkeypatch.setattr(process, "_read_frame", lose_readiness)
    with pytest.raises(RuntimeError):
        manager.start(rules=(rule,), providers={provider.id:provider})
    assert len(list(runtime.iterdir())) == 1
    assert next(runtime.glob("*/broker.sock")).exists()


@pytest.mark.parametrize("replacement_kind", ["socket", "file"])
def test_readiness_replacement_is_never_adopted_for_cleanup(tmp_path, monkeypatch, replacement_kind):
    # The current pathname must never supply the identity used to delete it.
    manager, runtime = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    import orchestrator.secrets.process as process
    real_read = process._read_frame
    replacement = None
    path = None
    replacement_identity = None
    original_identity = None
    def replace_before_validation(*args, **kwargs):
        nonlocal replacement, path, replacement_identity, original_identity
        record = real_read(*args, **kwargs)
        path = next(runtime.glob("*/broker.sock"))
        original_identity = (path.stat().st_dev, path.stat().st_ino)
        path.rename(path.with_name("original.sock"))
        if replacement_kind == "socket":
            replacement = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            replacement.bind(str(path))
        else:
            path.write_text("replacement-owned-by-another-resource")
        path.chmod(0o600)
        replacement_identity = (path.stat().st_dev, path.stat().st_ino)
        assert replacement_identity != original_identity
        return record
    monkeypatch.setattr(process, "_read_frame", replace_before_validation)
    session = None
    try:
        try:
            session = manager.start(rules=(rule,), providers={provider.id:provider})
        except RuntimeError:
            pass
        finally:
            if session is not None:
                session.close()
        assert path is not None and path.exists(), "startup cleanup removed a replacement pathname"
        assert (path.stat().st_dev, path.stat().st_ino) == replacement_identity
        assert path.with_name("original.sock").exists()
        assert session is None, "readiness exposed a client for a replacement socket"
    finally:
        if replacement is not None:
            replacement.close()


def test_manager_rejects_readiness_without_child_socket_identity(tmp_path, monkeypatch):
    manager, _ = _manager(tmp_path)
    provider, rule = _provider_and_rule("run-1")
    import orchestrator.secrets.process as process
    real_read = process._read_frame
    def strip_identity(*args, **kwargs):
        real_read(*args, **kwargs)
        return {"status": "ready"}
    monkeypatch.setattr(process, "_read_frame", strip_identity)
    with pytest.raises(RuntimeError):
        session = manager.start(rules=(rule,), providers={provider.id:provider})
        try:
            pytest.fail("readiness lacking the child's inode exposed a client")
        finally:
            session.close()


def test_server_does_not_emit_readiness_for_a_replaced_bound_socket(tmp_path, monkeypatch):
    from orchestrator.secrets.server import UnixSecretBrokerServer
    from orchestrator.secrets.broker import UnavailableSecretValueStore
    provider, rule = _provider_and_rule("run-1")
    path = tmp_path / "broker.sock"
    server = UnixSecretBrokerServer(socket_path=path, session_nonce="a" * 64,
        event_store_path=tmp_path / "events.db", rules=(rule,), providers={provider.id:provider},
        value_store=UnavailableSecretValueStore(), expected_uid=os.getuid(), unlink_on_close=False)
    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    real_chmod = os.chmod
    def replace_during_self_check(target, mode):
        Path(target).rename(path.with_name("original.sock"))
        replacement.bind(str(path))
        real_chmod(target, mode)
    monkeypatch.setattr(os, "chmod", replace_during_self_check)
    reader, writer = os.pipe()
    def serve():
        try:
            server.serve_forever(readiness_fd=writer)
        except RuntimeError:
            pass
    thread = threading.Thread(target=serve)
    thread.start()
    try:
        assert os.read(reader, 1024) == b"", "server attested readiness for a different inode"
    finally:
        os.close(reader); server.close(); thread.join(3); replacement.close()
    assert not thread.is_alive() and path.exists()


def test_parent_death_exits_child_with_a_blocked_active_store_read(tmp_path):
    # Normal interpreter exit hangs joining this synchronous handler forever.
    import select
    import signal
    manager, _ = _manager(tmp_path)
    child_code = '''
import threading
from pathlib import Path
from orchestrator.secrets import _broker_child as entrypoint
class StalledStore:
    def __init__(self, path): self.path = path
    def read(self, secret_ref):
        self.path.write_text("active")
        threading.Event().wait()
class HeldServer(entrypoint.UnixSecretBrokerServer):
    def __init__(self, **kwargs):
        kwargs["value_store"] = StalledStore(Path(str(kwargs["event_store_path"]) + ".active"))
        super().__init__(**kwargs)
entrypoint.UnixSecretBrokerServer = HeldServer
raise SystemExit(entrypoint.main())
'''
    parent_code = '''
import asyncio, json, os, subprocess, sys, threading, time
from pathlib import Path
from orchestrator.config.models import ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.secrets import SecretAccessRule, SecretBrokerProcessManager
real_launch = subprocess.Popen
def launch(args, **kwargs): return real_launch([sys.executable, "-c", sys.argv[3]], **kwargs)
subprocess.Popen = launch
p = ProviderSpec(id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True)
r = SecretAccessRule(p.id, p.secret_ref, p.effective_endpoint, "model_inference", frozenset({"run-1"}))
s = SecretBrokerProcessManager(runtime_root=Path(sys.argv[1]), event_store_path=Path(sys.argv[2])).start(rules=(r,), providers={p.id:p})
c = SecretAccessContext(request_id="request-1", run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1, accepted_route_id="route-1", budget_reservation_id="reservation-1")
def acquire():
    try: asyncio.run(s.client.acquire_provider_credential(secret_ref=p.secret_ref, provider=p, endpoint=p.effective_endpoint, purpose="model_inference", context=c))
    except Exception: pass
threading.Thread(target=acquire, daemon=True).start()
marker = Path(sys.argv[2] + ".active")
deadline = time.monotonic() + 5
while not marker.exists():
    if time.monotonic() > deadline: raise RuntimeError("handler did not enter")
    time.sleep(0.01)
print(json.dumps({"pid":s.process_pid}), flush=True)
sys.stdin.read(1)
os._exit(0)
'''
    parent = subprocess.Popen([sys.executable, "-c", parent_code, str(manager._runtime_root),
        str(manager._event_store_path), child_code], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(Path(__file__).resolve().parents[3] / "src")})
    child_handle = None
    try:
        assert select.select([parent.stdout], [], [], 10)[0], "supervisor did not become ready"
        record = json.loads(parent.stdout.readline())
        child_handle = os.pidfd_open(record["pid"])
        assert not select.select([child_handle], [], [], 0)[0]
        parent.stdin.write(b"x"); parent.stdin.flush()
        assert parent.wait(timeout=5) == 0
        assert select.select([child_handle], [], [], 5)[0], "blocked handler prevented parent-death exit"
    finally:
        if parent.poll() is None:
            parent.kill(); parent.wait(timeout=5)
        if child_handle is not None:
            try:
                signal.pidfd_send_signal(child_handle, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                os.close(child_handle)

