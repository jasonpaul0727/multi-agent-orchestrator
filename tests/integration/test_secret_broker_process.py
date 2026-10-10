"""Real subprocess replay and Linux parent-death containment."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import select
import signal
import sys
from collections.abc import Callable, Sequence
from dataclasses import replace
from functools import wraps
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import shutil
import socket
import sqlite3
import ssl
import threading
import time
import tempfile

import pytest


from orchestrator.config.models import ModelRegistryManifest, ModelSpec, ProviderSpec
from orchestrator.isolation import SystemdProviderSenderLauncher
from orchestrator.models import (
    AcceptedModelRoute, CostSnapshotRefs, ModelGatewayError, ModelMessage, ModelRequest,
    ProviderModelGateway, SQLiteProviderCallJournal,
)
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.models.transport import SystemdProviderHTTPSTransport
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets import SecretAccessRule, UnixSocketSecretBroker
from orchestrator.secrets.ipc import (
    SecretBrokerResponse, decode_secret_broker_request,
)


def start_test_broker_process(**kwargs):
    child = Path(__file__).resolve().parents[1] / "support" / "secret_broker_child.py"
    spec = importlib.util.spec_from_file_location("maestro_test_broker_child", child)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.start_test_broker_process(**kwargs)


class LoopbackTLSSink:
    """Only the local handler holds observed headers; it never logs them."""
    def __init__(self, tmp_path: Path):
        self.ca_bundle = tmp_path / "test-ca.pem"
        key = tmp_path / "test-tls.key"
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(key), "-out", str(self.ca_bundle), "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ], stdin=subprocess.DEVNULL, capture_output=True, check=True, timeout=15)
        key.chmod(0o600)
        self.authorization_headers: list[str] = []
        self.request_started = threading.Event()
        self.release_response = threading.Event()
        self.intent_checker: Callable[[], bool] = lambda: False
        self.intent_was_durable = False
        self.paths: list[str] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                owner.paths.append(self.path)
                owner.authorization_headers.extend(self.headers.get_all("Authorization", []))
                self.rfile.read(int(self.headers["Content-Length"]))
                owner.intent_was_durable = owner.intent_checker()
                owner.request_started.set()
                if not owner.release_response.wait(15):
                    return
                body = json.dumps({"id": "chatcmpl_1", "choices": [{"message": {
                    "content": "ok"}, "finish_reason": "stop"}], "usage": {
                    "prompt_tokens": 3, "completion_tokens": 1}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.ca_bundle), str(key))
        self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
        self.endpoint = f"https://127.0.0.1:{self._server.server_port}/v1"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_args):
        self.release_response.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def local_provider_request(endpoint: str):
    provider = ProviderSpec(id="primary", adapter="openai_compatible", endpoint=endpoint,
                            secret_ref="env:MAESTRO_TEST_KEY", enabled=True)
    model = ModelSpec(id="model-1", provider="primary", remote_model="local-test-model",
                     tier="standard", capabilities={"text"}, context_window=8192,
                     max_output_tokens=1024, supported_reasoning_efforts={"none"},
                     local_zero_cost=True)
    registry = ModelRegistryManifest(providers=(provider,), models=(model,))
    route = AcceptedModelRoute(decision_id="route-1", run_id="run-1", node_id="node-1",
        attempt_id="attempt-1", fencing_generation=1, budget_reservation_id="reservation-1",
        model_id=model.id, provider_id=provider.id, reasoning_effort="none",
        registry_manifest_hash=registry.content_hash)
    hash_value = "sha256:" + "a" * 64
    request = ModelRequest(request_id="request-1", idempotency_key="idem-1", run_id="run-1",
        node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
        budget_reservation_id="reservation-1", model_id=model.id, accepted_route=route,
        messages=(ModelMessage(role="user", content="Local security acceptance"),),
        max_output_tokens=16, reasoning_effort="none", timeout_ms=10000,
        cost_snapshots=CostSnapshotRefs(registry_manifest_hash=registry.content_hash,
            tokenizer_snapshot_id=hash_value, fx_snapshot_id=hash_value,
            price_snapshot_id=registry.content_hash, estimator_snapshot_id=hash_value))
    rule = SecretAccessRule(provider.id, provider.secret_ref, provider.effective_endpoint,
                            "model_inference", frozenset({request.run_id}))
    return registry, request, provider, rule


class _AcceptedRoutes:
    def __init__(self, request: ModelRequest):
        self._request = request

    async def is_accepted(self, request: ModelRequest) -> bool:
        return request == self._request


def _intent_visible_from_new_connection(db_path: Path, request: ModelRequest) -> bool:
    with SQLiteEventStore(db_path) as store:
        call = SQLiteProviderCallJournal(store).read(request)
        return call is not None and call.status == "dispatching"


def _provider_sender_pids() -> tuple[int, ...]:
    pids = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if b"provider_sender_process.py" in (entry / "cmdline").read_bytes():
                pids.append(int(entry.name))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return tuple(pids)


def assert_no_value_in_process_metadata(sentinel: str, pids: Sequence[int]):
    for pid in pids:
        for name in ("cmdline", "environ"):
            data = (Path("/proc") / str(pid) / name).read_bytes()
            found = sentinel.encode() in data
            assert not found, f"credential in {name} for pid {pid}"


def assert_no_value_in_event_db(sentinel: str, db_path: Path):
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            found = sentinel.encode() in path.read_bytes()
            assert not found, "credential persisted in SQLite"


def assert_no_value_in_test_artifacts(sentinel: str, root: Path):
    for path in root.rglob("*"):
        if path.is_file():
            found = sentinel.encode() in path.read_bytes()
            assert not found, "credential in test artifact/log"


def _new_session(tmp_path, provider, rule, *, event_store_path=None):
    events = event_store_path or tmp_path / "events.db"
    if not events.exists():
        with SQLiteEventStore(events):
            pass
    return start_test_broker_process(runtime_root=tmp_path / "runtime", event_store_path=events,
                                     provider=provider, rule=rule)


def _gateway(registry, request, broker, sink, journal):
    return ProviderModelGateway(registry=registry, accepted_route_verifier=_AcceptedRoutes(request),
        secret_broker=broker, transport=SystemdProviderHTTPSTransport(
            launcher=SystemdProviderSenderLauncher(ca_bundle_path=sink.ca_bundle)),
        provider_call_journal=journal)


def _access_context(request, *, request_id=None):
    return SecretAccessContext(request_id=request_id or request.request_id, run_id=request.run_id,
        node_id=request.node_id, attempt_id=request.attempt_id,
        fencing_generation=request.fencing_generation,
        accepted_route_id=request.accepted_route.decision_id,
        budget_reservation_id=request.budget_reservation_id)


def _live_systemd_available():
    return sys.platform == "linux" and shutil.which("systemd-run") and subprocess.run(
        ["systemctl", "--user", "show-environment"], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, check=False, timeout=5).returncode == 0


def _run_async(test):
    # Follow this repository's asyncio.run convention without a plugin dependency.
    @wraps(test)
    def run(*args, **kwargs):
        return asyncio.run(test(*args, **kwargs))
    return run


@pytest.mark.skipif(not _live_systemd_available(), reason="requires live Linux systemd user manager")
@_run_async
async def test_gateway_uses_process_broker_and_systemd_sender_without_secret_leaks(tmp_path, caplog, capsys):
    # Catches secrets in argv/env/durable output and network-before-intent regressions.
    with LoopbackTLSSink(tmp_path) as sink:
        registry, request, provider, rule = local_provider_request(sink.endpoint)
        session = _new_session(tmp_path, provider, rule)
        with SQLiteEventStore(tmp_path / "e2e-events.db") as events:
            journal = SQLiteProviderCallJournal(events)
            sink.intent_checker = lambda: _intent_visible_from_new_connection(tmp_path / "e2e-events.db", request)
            gateway = _gateway(registry, request, session.client, sink, journal)
            invocation = asyncio.create_task(gateway.invoke(request))
            try:
                assert await asyncio.to_thread(sink.request_started.wait, 8)
                assert len(sink.authorization_headers) == 1
                sentinel = sink.authorization_headers[0].removeprefix("Bearer ")
                assert sentinel.startswith("maestro-test-secret-")
                assert len(sentinel) == len("maestro-test-secret-") + 32
                assert sink.intent_was_durable is True
                pids = _provider_sender_pids()
                assert pids, "sender was not live during the metadata scan"
                assert_no_value_in_process_metadata(sentinel, (os.getpid(), session.process_pid, *pids))
                sink.release_response.set()
                response = await asyncio.wait_for(invocation, 15)
                assert response.output_text == "ok"
                assert response.usage.input_tokens == 3 and response.usage.output_tokens == 1
                call = journal.read(request)
                assert call.status == "known_success"
                assert call.termination_receipt is not None
                assert call.termination_receipt.cgroup_empty is True
                assert sink.paths == ["/v1/chat/completions"]
                # Reuse of the same accepted call cannot initiate a second HTTP request.
                with pytest.raises(ModelGatewayError):
                    await gateway.invoke(request)
                assert len(sink.authorization_headers) == 1
                assert_no_value_in_event_db(sentinel, tmp_path / "events.db")
                assert_no_value_in_event_db(sentinel, tmp_path / "e2e-events.db")
                assert_no_value_in_test_artifacts(sentinel, tmp_path)
                found = sentinel in caplog.text
                assert not found, "credential in captured host log"
                captured = capsys.readouterr()
                found = sentinel in captured.out + captured.err
                assert not found, "credential in captured host output"
            finally:
                sink.release_response.set()
                if not invocation.done():
                    invocation.cancel()
                    try:
                        await invocation
                    except (ModelGatewayError, asyncio.CancelledError):
                        pass
                session.close()


@pytest.mark.parametrize("failure", ["invalid_nonce", "wrong_endpoint", "audit_failure"])
@_run_async
async def test_gateway_secret_failures_never_dispatch_or_persist_success(tmp_path, failure):
    # Catches nonce/audience/audit bypass before Gateway Provider intent/network.
    with LoopbackTLSSink(tmp_path) as sink:
        registry, request, provider, rule = local_provider_request(sink.endpoint)
        if failure == "wrong_endpoint":
            rule = replace(rule, endpoint=sink.endpoint + "/wrong")
        audit_path = tmp_path / "events.db"
        if failure == "audit_failure":
            audit_path.mkdir()  # SQLite cannot open/commit to a directory.
        session = _new_session(tmp_path, provider, rule, event_store_path=audit_path)
        broker = session.client
        if failure == "invalid_nonce":
            broker = UnixSocketSecretBroker(socket_path=session.socket_path, session_nonce="wrong-nonce")
        try:
            with SQLiteEventStore(tmp_path / "e2e-events.db") as events:
                journal = SQLiteProviderCallJournal(events)
                gateway = _gateway(registry, request, broker, sink, journal)
                for _ in range(2):
                    with pytest.raises(ModelGatewayError) as denied:
                        await gateway.invoke(request)
                    assert denied.value.failure.outcome == "not_sent"
                assert journal.read(request) is None
                assert not sink.request_started.is_set()
                assert sink.authorization_headers == []
                assert_no_value_in_process_metadata("maestro-test-secret-", (os.getpid(), session.process_pid))
                assert_no_value_in_test_artifacts("maestro-test-secret-", tmp_path)
        finally:
            session.close()


@_run_async
async def test_test_child_restart_keeps_one_shot_access_and_gateway_blocked(tmp_path):
    # Catches reset of durable secret request consumption on Broker restart.
    with LoopbackTLSSink(tmp_path) as sink:
        registry, request, provider, rule = local_provider_request(sink.endpoint)
        first = _new_session(tmp_path, provider, rule)
        try:
            credential = await first.client.acquire_provider_credential(secret_ref=provider.secret_ref,
                provider=provider, endpoint=provider.effective_endpoint, purpose="model_inference",
                context=_access_context(request))
            sentinel = credential.value
            assert sentinel.startswith("maestro-test-secret-")
        finally:
            first.close()
        second = _new_session(tmp_path, provider, rule)
        try:
            with SQLiteEventStore(tmp_path / "e2e-events.db") as events:
                journal = SQLiteProviderCallJournal(events)
                gateway = _gateway(registry, request, second.client, sink, journal)
                for _ in range(2):
                    with pytest.raises(ModelGatewayError):
                        await gateway.invoke(request)
                assert journal.read(request) is None
                assert sink.authorization_headers == []
            with SQLiteEventStore(tmp_path / "events.db") as events:
                assert [e.event_type for e in events.read_stream("security", request.run_id)] == ["SecretAccessGranted"]
            assert_no_value_in_process_metadata(sentinel, (os.getpid(), second.process_pid))
            assert_no_value_in_test_artifacts(sentinel, tmp_path)
            # The restarted child has a new unknown value; scan its common marker too.
            assert_no_value_in_test_artifacts("maestro-test-secret-", tmp_path)
        finally:
            second.close()


@_run_async
async def test_broker_death_while_gateway_waits_never_dispatches(tmp_path):
    # A real write lock holds the audit before any value read; kill during recv.
    with LoopbackTLSSink(tmp_path) as sink:
        registry, request, provider, rule = local_provider_request(sink.endpoint)
        session = _new_session(tmp_path, provider, rule)
        with SQLiteEventStore(tmp_path / "e2e-events.db") as events:
            journal = SQLiteProviderCallJournal(events)
            gateway = _gateway(registry, request, session.client, sink, journal)
            blocker = sqlite3.connect(tmp_path / "events.db")
            blocker.execute("BEGIN IMMEDIATE")
            invocation = asyncio.create_task(gateway.invoke(request))
            try:
                # Observe the kernel socket's established state instead of sleeping.
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    rows = Path("/proc/net/unix").read_text().splitlines()
                    if any(str(session.socket_path) in row and row.split()[5] == "03" for row in rows):
                        break
                    await asyncio.sleep(0.01)
                else:
                    pytest.fail("Gateway did not connect to Broker before crash injection")
                assert not invocation.done()
                assert_no_value_in_process_metadata("maestro-test-secret-", (os.getpid(), session.process_pid))
                session.terminate_for_test()
                with pytest.raises(ModelGatewayError):
                    await asyncio.wait_for(invocation, 5)
                assert journal.read(request) is None
                assert sink.authorization_headers == []
                with pytest.raises(ModelGatewayError):
                    await gateway.invoke(request)
                assert_no_value_in_test_artifacts("maestro-test-secret-", tmp_path)
            finally:
                blocker.rollback()
                blocker.close()
                session.close()
                if not invocation.done():
                    invocation.cancel()
                    try:
                        await invocation
                    except (ModelGatewayError, asyncio.CancelledError):
                        pass


@pytest.mark.parametrize("mismatch", ["request_id", "provider_id", "endpoint", "purpose", "header_name"])
@_run_async
async def test_invalid_socket_response_binding_never_reaches_sender(tmp_path, mismatch):
    # An untrusted reply on a real packet socket must fail before Provider intent.
    with LoopbackTLSSink(tmp_path) as sink:
        registry, request, provider, _rule = local_provider_request(sink.endpoint)
        socket_path = tmp_path / "forged.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        listener.bind(str(socket_path))
        socket_path.chmod(0o600)
        listener.listen(2)
        listener.settimeout(5)
        sentinel = "maestro-test-secret-forged-binding"
        replies = []

        def send_forged_replies():
            try:
                for _ in range(2):
                    connection, _ = listener.accept()
                    with connection:
                        decoded = decode_secret_broker_request(connection.recv(16384))
                        response = SecretBrokerResponse(1, decoded.context.request_id,
                            decoded.provider_id, decoded.endpoint, decoded.purpose,
                            "credential", "authorization", sentinel)
                        values = {"request_id": "other-request", "provider_id": "other-provider",
                            "endpoint": sink.endpoint + "/wrong", "purpose": "other-purpose",
                            "header_name": "x-api-key"}
                        # Valid JSON, but a scope/header unsuitable for this caller.
                        payload = dict(version=response.version, request_id=response.request_id,
                            provider_id=response.provider_id, endpoint=response.endpoint,
                            purpose=response.purpose, status=response.status,
                            header_name=response.header_name, credential_value=response.credential_value)
                        payload[mismatch] = values[mismatch]
                        connection.send(json.dumps(payload).encode())
                        replies.append(True)
            finally:
                listener.close()

        thread = threading.Thread(target=send_forged_replies, daemon=True)
        thread.start()
        try:
            broker = UnixSocketSecretBroker(socket_path=socket_path, session_nonce="fixture-nonce")
            with SQLiteEventStore(tmp_path / "e2e-events.db") as events:
                journal = SQLiteProviderCallJournal(events)
                gateway = _gateway(registry, request, broker, sink, journal)
                for _ in range(2):
                    with pytest.raises(ModelGatewayError) as denied:
                        await gateway.invoke(request)
                    assert denied.value.failure.outcome == "not_sent"
                assert journal.read(request) is None
                assert sink.authorization_headers == []
                assert_no_value_in_process_metadata(sentinel, (os.getpid(),))
                assert_no_value_in_test_artifacts(sentinel, tmp_path)
        finally:
            thread.join(timeout=5)
            listener.close()
        assert not thread.is_alive()
        assert len(replies) == 2


@pytest.fixture(autouse=True)
def private_manager_runtime_area(monkeypatch):
    with tempfile.TemporaryDirectory(prefix="maestro-broker-test-", dir=f"/run/user/{os.getuid()}") as directory:
        monkeypatch.setattr(sys.modules[__name__], "_MANAGER_RUNTIME_AREA", Path(directory), raising=False)
        yield


def _setup(tmp_path):
    from orchestrator.secrets import SecretBrokerProcessManager
    events = tmp_path / "events.db"
    with SQLiteEventStore(events):
        pass
    runtime = Path(tempfile.mkdtemp(prefix="runtime-", dir=_MANAGER_RUNTIME_AREA))
    provider = ProviderSpec(id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True)
    rule = SecretAccessRule(provider.id, provider.secret_ref, provider.effective_endpoint,
                            "model_inference", frozenset({"run-1"}))
    return SecretBrokerProcessManager(runtime_root=runtime, event_store_path=events), provider, rule, events


@pytest.mark.parametrize("profile", ["overlay", "readonly"])
@pytest.mark.skipif(not _live_systemd_available(), reason="requires live Linux systemd user manager")
def test_actual_manager_broker_socket_is_hidden_and_unreachable_from_sandbox(tmp_path, profile):
    from orchestrator.isolation import SystemdOverlayCandidateLauncher, SystemdReadOnlyLauncher
    manager, provider, rule, events = _setup(tmp_path)
    session = manager.start(rules=(rule,), providers={provider.id: provider})
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    code = r'''
import json, socket, sys
from pathlib import Path
results = []
for path in sys.argv[1:]:
    p = Path(path)
    try:
        p.stat()
        hidden = False
    except OSError:
        hidden = True
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as s:
            s.connect(str(p))
        denied = False
    except OSError:
        denied = True
    results.append({"hidden": hidden, "denied": denied})
print(json.dumps(results))
'''
    try:
        assert session.socket_path.is_relative_to(f"/run/user/{os.getuid()}")
        paths = [session.socket_path]
        wsl_alias = Path("/mnt/wslg/run/user") / session.socket_path.relative_to("/run/user")
        if wsl_alias.exists():
            assert wsl_alias.samefile(session.socket_path)
            paths.append(wsl_alias)
        for path in paths:
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as host:
                host.connect(str(path))
        command = ["/usr/bin/python3", "-c", code, *map(str, paths)]
        if profile == "overlay":
            with SystemdOverlayCandidateLauncher().launch(workspace, command) as candidate:
                result = candidate.wait().execution
        else:
            result = SystemdReadOnlyLauncher().launch(workspace, command).wait()
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        assert result.termination_confirmed
        assert json.loads(result.stdout) == [{"hidden": True, "denied": True}] * len(paths)
        with SQLiteEventStore(events) as store:
            assert store.read_stream("security", "run-1") == []
        assert session.is_alive
    finally:
        session.close()


def test_manager_rejects_actual_source_visible_runtime_root_before_spawn(tmp_path, monkeypatch):
    import orchestrator.secrets.process as process_module
    manager, provider, rule, _events = _setup(tmp_path)
    with tempfile.TemporaryDirectory(prefix=".broker-runtime-test-", dir=Path(process_module.__file__).resolve().parents[2]) as directory:
        manager._runtime_root = Path(directory)
        monkeypatch.setattr(subprocess, "Popen", lambda *_a, **_kw: pytest.fail("source-visible Broker root launched a child"))
        with pytest.raises(ValueError, match="runtime root"):
            manager.start(rules=(rule,), providers={provider.id: provider})


def test_restart_replay_is_denied_after_child_restart(tmp_path):
    # Reusing durable request IDs must not create a second access decision.
    manager, provider, rule, events = _setup(tmp_path)
    paths = []
    for _ in range(2):
        session = manager.start(rules=(rule,), providers={provider.id: provider})
        paths.append(session.socket_path)
        try:
            assert asyncio.run(session.client.acquire_provider_credential(
                secret_ref=provider.secret_ref, provider=provider, endpoint=provider.effective_endpoint,
                purpose="model_inference", context=SecretAccessContext(request_id="request-1",
                run_id="run-1", node_id="node-1", attempt_id="attempt-1", fencing_generation=1,
                accepted_route_id="route-1", budget_reservation_id="reservation-1"))) is None
        finally:
            session.close()
    assert paths[0] != paths[1]
    with SQLiteEventStore(events) as store:
        audit = store.read_stream("security", "run-1")
    assert [event.event_type for event in audit] == ["SecretAccessGranted", "SecretCredentialUnavailable"]


def test_manager_child_exits_when_disposable_parent_exits(tmp_path):
    # Losing the supervisor must terminate the broker without a close() call.
    manager, provider, rule, events = _setup(tmp_path)
    code = '''
import json, os, sys
from pathlib import Path
from orchestrator.config.models import ProviderSpec
from orchestrator.secrets import SecretAccessRule, SecretBrokerProcessManager
p = ProviderSpec(id="primary", adapter="openai_responses", secret_ref="env:MODEL_KEY", enabled=True)
r = SecretAccessRule(p.id, p.secret_ref, p.effective_endpoint, "model_inference", frozenset({"run-1"}))
s = SecretBrokerProcessManager(runtime_root=Path(sys.argv[1]), event_store_path=Path(sys.argv[2])).start(rules=(r,), providers={p.id:p})
print(json.dumps({"pid":s.process_pid, "socket":str(s.socket_path)}), flush=True)
sys.stdin.read(1)
os._exit(0)
'''
    parent = subprocess.Popen([sys.executable, "-c", code, str(manager._runtime_root), str(events)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")})
    child_handle = None
    try:
        record = json.loads(parent.stdout.readline())
        child_pid = record["pid"]
        child_handle = os.pidfd_open(child_pid)
        assert Path(record["socket"]).exists()
        parent.stdin.write(b"x"); parent.stdin.flush()
        assert parent.wait(timeout=5) == 0
        assert select.select([child_handle], [], [], 5)[0], "broker survived supervisor death"
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
