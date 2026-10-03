"""Test-only isolated Broker; the child generates and owns its sentinel value.

No production manager injection point is added. Bootstrap contains the same
descriptors as production; a credential enters only the ordinary Broker reply.
"""
from __future__ import annotations

import ctypes
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import time

from orchestrator.config.models import ProviderSpec
from orchestrator.secrets import SecretAccessRule
from orchestrator.secrets._broker_child import _decode_bootstrap
from orchestrator.secrets.process import (
    MAX_BOOTSTRAP_BYTES, SecretBrokerProcessSession, _checked_socket_identity,
    _encode_bootstrap, _identity, _read_frame, _write_frame,
)
from orchestrator.secrets.server import UnixSecretBrokerServer


class InMemorySecretValueStore:
    def __init__(self, secret_ref: str):
        self._secret_ref = secret_ref
        self._value = "maestro-test-secret-" + secrets.token_hex(16)

    def read(self, secret_ref: str) -> str | None:
        return self._value if secret_ref == self._secret_ref else None


class TestBrokerProcessSession(SecretBrokerProcessSession):
    def terminate_for_test(self) -> None:
        # Kill the exact unreaped child while keeping the socket until close().
        signal.pidfd_send_signal(self._pidfd, signal.SIGKILL)
        self._process.wait(timeout=5)


def start_test_broker_process(*, runtime_root: Path, event_store_path: Path,
                              provider: ProviderSpec, rule: SecretAccessRule) -> TestBrokerProcessSession:
    runtime_root.mkdir(mode=0o700, exist_ok=True)
    runtime_root.chmod(0o700)
    directory = Path(tempfile.mkdtemp(prefix="test-broker-", dir=runtime_root))
    nonce = secrets.token_urlsafe(48)
    socket_path = directory / "broker.sock"
    source = Path(__file__).resolve().parents[2] / "src"
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve())], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, cwd=source,
        env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONPATH": str(source)},
        close_fds=True, bufsize=0,
    )
    session = TestBrokerProcessSession(
        process=process, pidfd=os.pidfd_open(process.pid), socket_path=socket_path,
        directory_identity=_identity(directory), session_nonce=nonce,
    )
    try:
        deadline = time.monotonic() + 10
        bootstrap = _encode_bootstrap(
            rules=(rule,), providers={provider.id: provider}, event_store_path=event_store_path,
            socket_path=socket_path, session_nonce=nonce,
        )
        _write_frame(process.stdin.fileno(), bootstrap, deadline=deadline)
        process.stdin.close()
        ready = _read_frame(process.stdout.fileno(), deadline=deadline, max_bytes=1024)
        assert set(ready) == {"status", "socket_dev", "socket_ino"}
        assert ready["status"] == "ready"
        session._socket_identity = (ready["socket_dev"], ready["socket_ino"])
        assert _checked_socket_identity(socket_path) == session._socket_identity
        assert session.is_alive
        session._ready = True
        return session
    except BaseException:
        session.close()
        raise
    finally:
        process.stdout.close()


def main() -> int:
    try:
        if ctypes.CDLL(None).prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
            return 1
        record = _read_frame(0, deadline=time.monotonic() + 10, max_bytes=MAX_BOOTSTRAP_BYTES)
        os.close(0)
        rules, providers = _decode_bootstrap(record)
        if os.getppid() != record["expected_parent_pid"]:
            return 1
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        if os.getppid() != record["expected_parent_pid"]:
            return 1
        server = UnixSecretBrokerServer(
            socket_path=Path(record["socket_path"]), session_nonce=record["session_nonce"],
            event_store_path=Path(record["event_store_path"]), rules=rules, providers=providers,
            value_store=InMemorySecretValueStore(rules[0].secret_ref), expected_uid=os.getuid(),
            unlink_on_close=False,
        )

        def serve():
            try:
                server.serve_forever(readiness_fd=1)
            finally:
                stop.set()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        stop.wait()
        server.close()
        os._exit(0)
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
