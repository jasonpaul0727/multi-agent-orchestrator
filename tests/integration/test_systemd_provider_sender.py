from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import shutil
import socket
import ssl
import subprocess
import sys
import threading

import pytest

import orchestrator.isolation as isolation_exports


def _usable_systemd_user_manager() -> bool:
    if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
        return False
    import subprocess

    return subprocess.run(
        ["systemctl", "--user", "show-environment"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=5,
    ).returncode == 0


pytestmark = pytest.mark.skipif(
    not _usable_systemd_user_manager(),
    reason="integrated Provider sender tests require Linux and systemd --user",
)


def test_live_provider_sender_trusts_loopback_tls_only_with_host_ca_bundle(tmp_path):
    from orchestrator.runtime.provider_sender_process import encode_provider_sender_request

    ca_key, ca_cert = tmp_path / "ca.key", tmp_path / "ca.pem"
    server_key, server_csr, server_cert = tmp_path / "server.key", tmp_path / "server.csr", tmp_path / "server.pem"
    commands = [
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", str(ca_key), "-out", str(ca_cert), "-subj", "/CN=Maestro test-only CA"],
        ["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(server_key), "-out", str(server_csr), "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1"],
        ["openssl", "x509", "-req", "-in", str(server_csr), "-CA", str(ca_cert), "-CAkey", str(ca_key), "-CAcreateserial", "-out", str(server_cert), "-days", "1", "-copy_extensions", "copy"],
    ]
    for command in commands:
        subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, check=True, timeout=15)

    observed = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            observed.append((self.path, self.headers.get_all("Authorization"), self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(server_cert), str(server_key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    frame = encode_provider_sender_request(
        url=f"https://127.0.0.1:{server.server_port}/v1/responses",
        headers=(("authorization", "Bearer local-sentinel"), ("content-type", "application/json")),
        body=b"{}", timeout_ms=3000, max_response_bytes=1024,
    )
    try:
        untrusted = isolation_exports.SystemdProviderSenderLauncher().launch(frame, timeout_seconds=6, output_bytes=4096).wait()
        assert untrusted.response is None
        assert untrusted.returncode != 0
        assert observed == []
        trusted = isolation_exports.SystemdProviderSenderLauncher(ca_bundle_path=ca_cert).launch(frame, timeout_seconds=6, output_bytes=4096).wait()
        assert trusted.response is not None
        assert trusted.response.status == 200
        assert trusted.response.body == b"{}"
        assert trusted.termination_receipt.cgroup_empty
        assert observed == [("/v1/responses", ["Bearer local-sentinel"], b"{}")]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_live_provider_sender_stops_exact_unit_after_loopback_connection_refusal():
    assert hasattr(isolation_exports, "SystemdProviderSenderLauncher"), (
        "the dedicated systemd Provider sender launcher is not implemented"
    )
    runtime_name = "orchestrator.runtime.provider_sender_process"
    assert importlib.util.find_spec(runtime_name) is not None, (
        "the fixed Provider sender helper module is not implemented"
    )
    from orchestrator.runtime.provider_sender_process import encode_provider_sender_request

    frame = encode_provider_sender_request(
        url="https://127.0.0.1:1/v1/responses",
        headers=(
            ("accept", "application/json"),
            ("authorization", "Bearer test-only"),
            ("content-type", "application/json"),
        ),
        body=b"{}",
        timeout_ms=500,
        max_response_bytes=1_024,
    )

    result = isolation_exports.SystemdProviderSenderLauncher().launch(
        frame, timeout_seconds=4, output_bytes=4_096
    ).wait()

    assert result.response is None
    assert result.returncode != 0
    assert result.termination_receipt is not None
    assert result.termination_receipt.unit_name == result.unit_name
    assert result.termination_receipt.active_state in {"inactive", "failed"}
    assert result.termination_receipt.cgroup_empty is True


def test_live_gateway_transport_cancellation_waits_for_exact_sender_stop():
    import asyncio

    from orchestrator.models.provider_calls import ProviderCallSnapshot

    import orchestrator.models.transport as transport_module

    transport_type = getattr(
        transport_module, "SystemdProviderHTTPSTransport", None
    )
    assert transport_type is not None, "the supervised Gateway transport is not implemented"

    request_body = b'{"input":"integration-cancel"}'
    binding = ProviderCallSnapshot(
        stream_id="call-" + "a" * 64,
        run_id="run-integration",
        node_id="node-integration",
        attempt_id="attempt-integration",
        fencing_generation=1,
        request_id="request-integration",
        idempotency_key_hash="sha256:"
        + hashlib.sha256(b"idem-integration").hexdigest(),
        accepted_route_id="decision-integration",
        budget_reservation_id="reservation-integration",
        provider_id="primary",
        provider_adapter="openai_compatible",
        provider_correlation_id=None,
        model_id="model-integration",
        registry_manifest_hash="sha256:" + "c" * 64,
        request_hash="sha256:" + hashlib.sha256(request_body).hexdigest(),
        status="dispatching",
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(20)
    started = threading.Event()
    stop = threading.Event()
    connection_closed = threading.Event()

    def accept_and_hold_tls():
        try:
            try:
                connection, _address = listener.accept()
            except socket.timeout:
                return
            started.set()
            with connection:
                connection.settimeout(0.1)
                while not stop.is_set():
                    try:
                        if not connection.recv(4096):
                            break
                    except TimeoutError:
                        continue
        finally:
            connection_closed.set()
            listener.close()

    server = threading.Thread(target=accept_and_hold_tls, daemon=True)
    server.start()
    port = listener.getsockname()[1]
    transport = transport_type()

    async def cancel_after_connection_starts():
        invocation = asyncio.create_task(
            transport.post_json(
                url=f"https://127.0.0.1:{port}/v1/chat/completions",
                headers=(
                    ("accept", "application/json"),
                    ("authorization", "Bearer integration-only"),
                    ("content-type", "application/json"),
                    ("idempotency-key", "idem-integration"),
                    ("x-request-id", "request-integration"),
                ),
                body=request_body,
                timeout_ms=30_000,
                cancellation=None,
                max_response_bytes=1_024,
                call_binding=binding,
            )
        )
        if not await asyncio.to_thread(started.wait, 15):
            invocation.cancel()
            with pytest.raises(transport_module.HTTPTransportCancelled):
                await asyncio.wait_for(invocation, timeout=10)
            pytest.fail("sender never connected to local sink")
        invocation.cancel()
        with pytest.raises(transport_module.HTTPTransportCancelled) as failure:
            await asyncio.wait_for(invocation, timeout=10)
        assert failure.value.may_have_been_sent is True
        receipt = failure.value.termination_receipt
        assert receipt is not None
        assert receipt.provider_call_stream_id == binding.stream_id
        assert receipt.attempt_id == binding.attempt_id
        assert receipt.cgroup_empty is True

    try:
        asyncio.run(cancel_after_connection_starts())
    finally:
        stop.set()
        server.join(timeout=5)
    assert connection_closed.is_set()
