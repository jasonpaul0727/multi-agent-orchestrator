"""Real Linux IPC checks for authorization, durable audit and bounded admission."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
import stat
import struct
import threading
import time

import pytest

from orchestrator.config.models import ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets import (
    SecretAccessRule, SecretBrokerRequest, decode_secret_broker_response,
    encode_secret_broker_request,
)
from orchestrator.secrets.broker import UnavailableSecretValueStore
from orchestrator.secrets.server import UnixSecretBrokerServer


NONCE = "_" + "a" * 63
VALUE = "sentinel-" + "x" * 16
PROVIDER = ProviderSpec(
    id="test-provider", adapter="openai_compatible",
    endpoint="https://provider.example.test/v1",
    secret_ref="env:MAESTRO_TEST_KEY", enabled=True,
)
RULE = SecretAccessRule(
    provider_id=PROVIDER.id, secret_ref=PROVIDER.secret_ref,
    endpoint=PROVIDER.effective_endpoint, purpose="model_inference",
    allowed_run_ids=frozenset({"run-1"}),
)


def request(request_id="request-1"):
    return SecretBrokerRequest(
        version=1, session_nonce=NONCE, secret_ref=PROVIDER.secret_ref,
        provider_id=PROVIDER.id, endpoint=PROVIDER.effective_endpoint,
        purpose="model_inference", context=SecretAccessContext(
            request_id=request_id, run_id="run-1", node_id="node-1",
            attempt_id="attempt-1", fencing_generation=1,
            accepted_route_id="route-1", budget_reservation_id="reservation-1",
        ),
    )


class ObservingValues:
    def __init__(self, path, value=VALUE):
        self.path = path
        self.value = value
        self.observed = []

    def read(self, secret_ref):
        assert secret_ref == "env:MAESTRO_TEST_KEY"
        with SQLiteEventStore(self.path) as reader:
            self.observed.append(reader.read_stream("security", "run-1"))
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


def make_server(tmp_path, **kwargs):
    path = tmp_path / "events.db"
    with SQLiteEventStore(path):
        pass
    values = kwargs.pop("value_store", ObservingValues(path))
    server = UnixSecretBrokerServer(
        socket_path=tmp_path / "broker.sock", session_nonce=NONCE,
        event_store_path=path, rules=(RULE,), providers={PROVIDER.id: PROVIDER},
        value_store=values, expected_uid=kwargs.pop("expected_uid", os.getuid()),
        **kwargs,
    )
    return server, values


def read_exact(fd, size):
    data = b""
    while len(data) < size:
        piece = os.read(fd, size - len(data))
        assert piece, "readiness pipe closed before complete record"
        data += piece
    return data


@contextmanager
def running(server):
    ready_read, ready_write = os.pipe()
    # Threads share fd tables: duplicate the writer so only the server owns it.
    writer = os.dup(ready_write)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"readiness_fd": writer}, daemon=True,
    )
    thread.start()
    os.close(ready_write)
    try:
        length = struct.unpack("!I", read_exact(ready_read, 4))[0]
        assert 0 < length <= 1024
        assert json.loads(read_exact(ready_read, length))["status"] == "ready"
        assert stat.S_IMODE(server.socket_path.stat().st_mode) == 0o600
        yield server
    finally:
        os.close(ready_read)
        server.close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def _exchange_one_frame(socket_path: Path, req: SecretBrokerRequest):
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
        client.settimeout(2)
        client.connect(str(socket_path))
        client.sendall(encode_secret_broker_request(req))
        return decode_secret_broker_response(client.recv(16_385))


def test_server_audits_before_reading_and_rejects_wrong_nonce(tmp_path):
    # Removing audit-before-read or nonce authentication exposes a value here.
    server, values = make_server(tmp_path)
    with running(server):
        result = _exchange_one_frame(server.socket_path, request())
        assert result.status == "credential"
        assert result.credential_value == VALUE
        assert result.header_name == "authorization"
        assert (result.request_id, result.provider_id, result.endpoint, result.purpose) == (
            "request-1", "test-provider", "https://provider.example.test/v1", "model_inference",
        )
        assert values.observed[0][-1].event_type == "SecretAccessGranted"
        assert VALUE not in repr(values.observed)
        assert PROVIDER.secret_ref not in repr(values.observed)
        denied = _exchange_one_frame(server.socket_path, replace(request("wrong-nonce"), session_nonce="0" * 64))
        assert denied.status == "denied"
        assert denied.credential_value is None
        assert len(values.observed) == 1


def test_wrong_peer_uid_never_reads_store(tmp_path):
    server, values = make_server(tmp_path, expected_uid=os.getuid() + 1)
    with running(server):
        assert _exchange_one_frame(server.socket_path, request()).status == "denied"
        assert values.observed == []


@pytest.mark.parametrize("changes", [
    {"provider_id": "unknown"}, {"secret_ref": "env:OTHER_KEY"},
    {"endpoint": "https://other.example.test/v1"},
    {"context": request().context.model_copy(update={"run_id": "run-2"})},
])
def test_scope_mismatch_never_reads_store(tmp_path, changes):
    server, values = make_server(tmp_path)
    with running(server):
        result = _exchange_one_frame(server.socket_path, replace(request(), **changes))
        assert result.status != "credential"
        assert result.credential_value is None
        assert values.observed == []


@pytest.mark.parametrize("mutate", [
    lambda data: data.update(purpose="tool_access"),
    lambda data: data["context"].update(attempt_id=""),
    lambda data: data["context"].update(fencing_generation=True),
    lambda data: data.update(adapter="anthropic_messages"),
])
def test_invalid_wire_request_closes_without_credentials(tmp_path, mutate):
    server, values = make_server(tmp_path)
    data = json.loads(encode_secret_broker_request(request()))
    mutate(data)
    with running(server), socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
        client.settimeout(2)
        client.connect(str(server.socket_path))
        client.sendall(json.dumps(data).encode())
        assert client.recv(16_385) == b""
        assert values.observed == []


def test_audit_write_failure_never_reads_store(tmp_path):
    server, values = make_server(tmp_path)
    # SQLite trigger is a real durable-write failure, preserving the policy path.
    import sqlite3
    with sqlite3.connect(tmp_path / "events.db") as db:
        db.execute("CREATE TRIGGER reject_audit BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'fail'); END")
    with running(server):
        assert _exchange_one_frame(server.socket_path, request()).status == "unavailable"
        assert values.observed == []


@pytest.mark.parametrize("value", [None, "", "bad\r\nheader", "x" * 4097, RuntimeError(VALUE)])
def test_unavailable_or_invalid_store_values_fail_closed(tmp_path, value, capsys):
    values = ObservingValues(tmp_path / "events.db", value)
    server, _ = make_server(tmp_path, value_store=values)
    with running(server):
        result = _exchange_one_frame(server.socket_path, request())
        assert result.status == "unavailable"
        assert result.header_name is None and result.credential_value is None
    assert VALUE not in repr(capsys.readouterr())


def test_replay_is_consumed_durably_and_concurrent_access_is_one_shot(tmp_path):
    server, values = make_server(tmp_path)
    with running(server), ThreadPoolExecutor(max_workers=2) as clients:
        barrier = threading.Barrier(2)
        def exchange():
            barrier.wait()
            return _exchange_one_frame(server.socket_path, request())
        results = list(clients.map(lambda _: exchange(), range(2)))
        assert sorted(result.status for result in results) == ["credential", "denied"]
        assert len(values.observed) == 1
    # A fresh server/process view must still reject the same request ID.
    server, values = make_server(tmp_path)
    with running(server):
        assert _exchange_one_frame(server.socket_path, request()).status == "denied"
        assert values.observed == []


def test_oversized_frame_and_idle_connection_release_handler(tmp_path):
    server, values = make_server(tmp_path, io_timeout_seconds=0.1)
    with running(server):
        for frame in (b"x" * 16_385, None):
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
                client.settimeout(2)
                client.connect(str(server.socket_path))
                if frame:
                    client.sendall(frame)
                assert client.recv(16_385) == b""
        assert _exchange_one_frame(server.socket_path, request()).status == "credential"
        assert len(values.observed) == 1


def test_one_connection_returns_only_one_response(tmp_path):
    server, values = make_server(tmp_path)
    with running(server), socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
        client.settimeout(2)
        client.connect(str(server.socket_path))
        client.sendall(encode_secret_broker_request(request()))
        assert decode_secret_broker_response(client.recv(16_385)).status == "credential"
        assert client.recv(16_385) == b""
        assert len(values.observed) == 1


def test_admission_rejects_twenty_fifth_socket_with_only_eight_handlers(tmp_path):
    entered = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    active = []
    class HeldServer(UnixSecretBrokerServer):
        def _handle_one_connection(self, connection):
            with lock:
                active.append(threading.get_ident())
                if len(active) == 8:
                    entered.set()
            release.wait(5)
    path = tmp_path / "events.db"
    with SQLiteEventStore(path):
        pass
    server = HeldServer(
        socket_path=tmp_path / "broker.sock", session_nonce=NONCE,
        event_store_path=path, rules=(RULE,), providers={PROVIDER.id: PROVIDER},
        value_store=UnavailableSecretValueStore(), expected_uid=os.getuid(),
    )
    sockets = []
    try:
        with running(server):
            for _ in range(24):
                client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                client.settimeout(2)
                deadline = time.monotonic() + 2
                while True:
                    try:
                        client.connect(str(server.socket_path))
                        break
                    except BlockingIOError:
                        # The kernel backlog may briefly fill before accept;
                        # this is separate from the 24 accepted-work bound.
                        assert time.monotonic() < deadline
                        time.sleep(0.001)
                sockets.append(client)
            assert entered.wait(2)
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as rejected:
                rejected.settimeout(2)
                rejected.connect(str(server.socket_path))
                assert rejected.recv(16_385) == b""
            assert len(active) == 8
            with server._lock:
                assert len(server._futures) == 24
                assert len(server._connections) == 24
            release.set()
    finally:
        release.set()
        for client in sockets:
            client.close()


@pytest.mark.parametrize("kwargs", [
    {"max_handlers": 9}, {"max_handlers": 0}, {"backlog": 17}, {"backlog": 0},
    {"io_timeout_seconds": 0}, {"io_timeout_seconds": float("inf")},
    {"io_timeout_seconds": 1e100},
    {"session_nonce": "bad nonce"},
])
def test_constructor_rejects_unbounded_or_invalid_configuration(tmp_path, kwargs):
    config = dict(socket_path=tmp_path / "broker.sock", session_nonce=NONCE,
                  event_store_path=tmp_path / "events.db", rules=(RULE,),
                  providers={PROVIDER.id: PROVIDER}, value_store=UnavailableSecretValueStore(),
                  expected_uid=os.getuid())
    config.update(kwargs)
    with pytest.raises((ValueError, TypeError)):
        UnixSecretBrokerServer(**config)


def test_unavailable_default_never_reads_environment(monkeypatch):
    monkeypatch.setenv("MAESTRO_TEST_KEY", VALUE)
    assert UnavailableSecretValueStore().read("env:MAESTRO_TEST_KEY") is None


@pytest.mark.parametrize("allowed", [False, True])
def test_denied_and_unavailable_decisions_need_no_post_audit_stream_scan(tmp_path, monkeypatch, allowed):
    server, _ = make_server(tmp_path, value_store=UnavailableSecretValueStore())
    history_reads = []

    def no_stream_scan(*_args, **_kwargs):
        history_reads.append(True)
        raise AssertionError("response classification reread the Run history")

    monkeypatch.setattr(SQLiteEventStore, "read_stream", no_stream_scan)
    req = request()
    if not allowed:
        req = replace(req, endpoint="https://other.example.test/v1")
    with running(server):
        assert _exchange_one_frame(server.socket_path, req).status == ("unavailable" if allowed else "denied")
    assert history_reads == []


def test_provider_map_is_frozen_and_trusted_adapter_selects_header(tmp_path):
    provider = ProviderSpec(id="test-provider", adapter="anthropic_messages",
                            secret_ref=PROVIDER.secret_ref, enabled=True)
    rule = replace(RULE, endpoint="https://api.anthropic.com/v1")
    providers = {provider.id: provider}
    values = ObservingValues(tmp_path / "events.db")
    server = UnixSecretBrokerServer(socket_path=tmp_path / "broker.sock", session_nonce=NONCE,
                                   event_store_path=tmp_path / "events.db", rules=(rule,),
                                   providers=providers, value_store=values, expected_uid=os.getuid())
    providers.clear()
    with running(server):
        result = _exchange_one_frame(server.socket_path, replace(request(), endpoint=rule.endpoint))
        assert result.status == "credential"
        assert result.header_name == "x-api-key"


def test_grace_expiry_closes_sockets_and_releases_cancelled_connection_tracking(tmp_path):
    entered = threading.Event()
    release = threading.Event()
    class HeldServer(UnixSecretBrokerServer):
        def _handle_one_connection(self, connection):
            entered.set()
            release.wait(10)
            # A late handler must not deliver after the shutdown grace expires.
            connection.send(b"late-credential")
    server = HeldServer(
        socket_path=tmp_path / "broker.sock", session_nonce=NONCE,
        event_store_path=tmp_path / "events.db", rules=(RULE,),
        providers={PROVIDER.id: PROVIDER}, value_store=UnavailableSecretValueStore(),
        expected_uid=os.getuid(), max_handlers=1,
    )
    clients = []
    try:
        with running(server):
            for _ in range(2):
                client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                client.settimeout(3)
                client.connect(str(server.socket_path))
                clients.append(client)
            assert entered.wait(2)
            deadline = time.monotonic() + 2
            while True:
                with server._lock:
                    if len(server._connections) == 2:
                        break
                assert time.monotonic() < deadline
                time.sleep(0.001)
            started = time.monotonic()
            server.close()
            assert time.monotonic() - started < 3
            assert not server.socket_path.exists()
            for client in clients:
                assert client.recv(16_385) == b""
            with server._lock:
                assert not server._connections
            with pytest.raises(RuntimeError):
                server.serve_forever()
            release.set()
    finally:
        release.set()
        for client in clients:
            client.close()


def test_admission_slot_is_held_until_worker_future_completes(tmp_path):
    finished_connection = threading.Event()
    release = threading.Event()
    class FinishingServer(UnixSecretBrokerServer):
        def _run_connection(self, connection):
            super()._run_connection(connection)
            # Freeze the real scheduling window between closing a connection
            # and completing its executor future.
            finished_connection.set()
            release.wait(5)
    server = FinishingServer(
        socket_path=tmp_path / "broker.sock", session_nonce=NONCE,
        event_store_path=tmp_path / "events.db", rules=(RULE,),
        providers={PROVIDER.id: PROVIDER}, value_store=UnavailableSecretValueStore(),
        expected_uid=os.getuid(), max_handlers=1, backlog=1,
    )
    with running(server):
        try:
            assert _exchange_one_frame(server.socket_path, request()).status == "unavailable"
            assert finished_connection.wait(2)
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as queued:
                queued.settimeout(2)
                queued.connect(str(server.socket_path))
                with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as rejected:
                    rejected.settimeout(1)
                    rejected.connect(str(server.socket_path))
                    assert rejected.recv(16_385) == b""
                with server._lock:
                    assert len(server._futures) == 2
        finally:
            release.set()
