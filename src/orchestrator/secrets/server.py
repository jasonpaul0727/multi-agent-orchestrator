"""Bounded Linux Unix-packet boundary for the audited Secret Broker.

Only the trusted Provider registry selects credential adapters. Each handler
owns its SQLite connection; policy and durable replay decisions stay in the
AuditedSecretBroker. No IPC payloads or exception contents are logged.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
import hmac
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import struct
import sys
import threading
from types import MappingProxyType

from orchestrator.config.models import ProviderAdapter, ProviderSpec
from orchestrator.models.transport import ProviderCredential
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets.broker import (
    AuditedSecretBroker, SecretAccessDenied, SecretAccessRule, SecretValueStore,
)
from orchestrator.secrets.ipc import (
    MAX_SECRET_BROKER_FRAME_BYTES, SecretBrokerRequest, SecretBrokerResponse,
    decode_secret_broker_request, encode_secret_broker_response,
)


def _denied_response(request: SecretBrokerRequest) -> SecretBrokerResponse:
    return SecretBrokerResponse(
        1, request.context.request_id, request.provider_id, request.endpoint,
        request.purpose, "denied", None, None,
    )


def _bound_response(
    request: SecretBrokerRequest, provider: ProviderSpec,
    credential: ProviderCredential | None,
) -> SecretBrokerResponse:
    if credential is None:
        return SecretBrokerResponse(
            1, request.context.request_id, request.provider_id, request.endpoint,
            request.purpose, "unavailable", None, None,
        )
    if (
        credential.provider_id != provider.id
        or credential.provider_id != request.provider_id
        or credential.endpoint != provider.effective_endpoint
        or credential.endpoint != request.endpoint
        or credential.purpose != request.purpose
    ):
        raise ValueError("Secret Broker credential binding is invalid")
    return SecretBrokerResponse(
        1, request.context.request_id, provider.id, credential.endpoint,
        credential.purpose, "credential", credential.header_name, credential.value,
    )


def _peer_uid(connection: socket.socket) -> int:
    credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", credentials)
    return uid


def _recv_one_frame(connection: socket.socket) -> bytes:
    frame, _ancillary, flags, _address = connection.recvmsg(MAX_SECRET_BROKER_FRAME_BYTES)
    if not frame or flags & socket.MSG_TRUNC:
        raise ValueError("Secret Broker frame is invalid")
    return frame


class UnixSecretBrokerServer:
    """One-frame connections with eight workers and sixteen pending slots."""

    def __init__(
        self, *, socket_path: Path, session_nonce: str, event_store_path: Path,
        rules: Sequence[SecretAccessRule], providers: Mapping[str, ProviderSpec],
        value_store: SecretValueStore, expected_uid: int, max_handlers: int = 8,
        backlog: int = 16, io_timeout_seconds: float = 10.0,
        unlink_on_close: bool = True,
    ) -> None:
        if sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"):
            raise RuntimeError("Secret Broker requires Linux Unix packet sockets")
        for value, upper in ((max_handlers, 8), (backlog, 16)):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= upper:
                raise ValueError("Secret Broker concurrency bound is invalid")
        if (
            isinstance(io_timeout_seconds, bool)
            or not isinstance(io_timeout_seconds, (int, float))
            or not math.isfinite(io_timeout_seconds) or io_timeout_seconds <= 0
            or io_timeout_seconds > threading.TIMEOUT_MAX
        ):
            raise ValueError("Secret Broker deadline must be finite and positive")
        if not isinstance(session_nonce, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", session_nonce) is None:
            raise ValueError("Secret Broker session nonce is invalid")
        if isinstance(expected_uid, bool) or not isinstance(expected_uid, int) or expected_uid < 0:
            raise ValueError("Secret Broker peer UID is invalid")
        frozen_rules = tuple(rules)
        if not frozen_rules or any(not isinstance(rule, SecretAccessRule) for rule in frozen_rules):
            raise ValueError("Secret Broker requires explicit rules")
        frozen_providers = dict(providers)
        if any(not isinstance(provider, ProviderSpec) or key != provider.id for key, provider in frozen_providers.items()):
            raise ValueError("Secret Broker Provider registry is invalid")
        self._socket_path = Path(socket_path)
        if not isinstance(unlink_on_close, bool):
            raise ValueError("Secret Broker cleanup ownership is invalid")
        self._unlink_on_close = unlink_on_close
        self.session_nonce = session_nonce
        self.event_store_path = Path(event_store_path)
        self.rules = frozen_rules
        self.providers = MappingProxyType(frozen_providers)
        self.value_store = value_store
        self.expected_uid = expected_uid
        self._max_handlers = max_handlers
        self._backlog = backlog
        self._io_timeout_seconds = float(io_timeout_seconds)
        self._admission = threading.BoundedSemaphore(max_handlers + backlog)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._listener: socket.socket | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._connections: set[socket.socket] = set()
        self._futures: set[Future[None]] = set()
        self._socket_identity: tuple[int, int] | None = None
        self._started = False

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    def serve_forever(self, *, readiness_fd: int | None = None) -> None:
        with self._lock:
            if self._started or self._stop.is_set():
                raise RuntimeError("Secret Broker server cannot be restarted")
            self._started = True
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        try:
            # Bind inside a private directory supplied by the supervisor. Never
            # remove an existing pathname: it may belong to another process.
            listener.bind(str(self._socket_path))
            info = self._socket_path.lstat()
            self._socket_identity = (info.st_dev, info.st_ino)
            os.chmod(self._socket_path, 0o600)
            info = self._socket_path.lstat()
            if (not stat.S_ISSOCK(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid() or (info.st_dev, info.st_ino) != self._socket_identity):
                raise RuntimeError("Secret Broker socket self-check failed")
            listener.listen(self._backlog)
            listener.settimeout(0.1)
            with self._lock:
                self._listener = listener
                self._executor = ThreadPoolExecutor(max_workers=self._max_handlers, thread_name_prefix="secret-broker")
            if readiness_fd is not None:
                record = json.dumps({"status": "ready", "socket_dev": self._socket_identity[0],
                                     "socket_ino": self._socket_identity[1]}, separators=(",", ":")).encode("utf-8")
                os.write(readiness_fd, struct.pack("!I", len(record)) + record)
                os.close(readiness_fd)
                readiness_fd = None
            while not self._stop.is_set():
                try:
                    connection, _address = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if self._stop.is_set():
                        break
                    raise
                if not self._admission.acquire(blocking=False):
                    connection.close()
                    continue
                connection.settimeout(self._io_timeout_seconds)
                with self._lock:
                    if self._stop.is_set():
                        connection.close()
                        self._admission.release()
                        break
                    self._connections.add(connection)
                    assert self._executor is not None
                    future = self._executor.submit(self._run_connection, connection)
                    self._futures.add(future)
                future.add_done_callback(self._finished)
        finally:
            if readiness_fd is not None:
                os.close(readiness_fd)
            listener.close()
            self.close()

    def _finished(self, future: Future[None]) -> None:
        with self._lock:
            self._futures.discard(future)
            self._admission.release()

    def _run_connection(self, connection: socket.socket) -> None:
        try:
            self._handle_one_connection(connection)
        except Exception:
            # Boundary failures close the connection without printing sensitive
            # values, exception chains, request objects or unvalidated JSON.
            pass
        finally:
            connection.close()
            with self._lock:
                self._connections.discard(connection)

    def _handle_one_connection(self, connection: socket.socket) -> None:
        uid = _peer_uid(connection)
        request = decode_secret_broker_request(_recv_one_frame(connection))
        if uid != self.expected_uid or not hmac.compare_digest(request.session_nonce, self.session_nonce):
            self._send_result(connection, _denied_response(request))
            return
        provider = self.providers.get(request.provider_id)
        if provider is None:
            self._send_result(connection, _denied_response(request))
            return
        try:
            # Construct, use and close a thread-affine store in this handler.
            with SQLiteEventStore(self.event_store_path) as events:
                broker = AuditedSecretBroker(event_store=events, value_store=self.value_store, rules=self.rules)
                credential = asyncio.run(broker.acquire_provider_credential(
                    secret_ref=request.secret_ref, provider=provider, endpoint=request.endpoint,
                    purpose=request.purpose, context=request.context,
                ))
                response = _bound_response(request, provider, credential)
                if credential is None:
                    # The existing broker returns None for both policy denial
                    # and unavailable values. Its durable decision is authoritative.
                    if any(event.event_type == "SecretAccessDenied" and event.idempotency_key == f"secret-access:{request.context.request_id}"
                           for event in events.read_stream("security", request.context.run_id)):
                        response = _denied_response(request)
        except SecretAccessDenied:
            response = _denied_response(request)
        except Exception:
            response = _bound_response(request, provider, None)
        self._send_result(connection, response, adapter=provider.adapter)

    def _send_result(
        self, connection: socket.socket, response: SecretBrokerResponse,
        *, adapter: ProviderAdapter | None = None,
    ) -> None:
        frame = encode_secret_broker_response(response, adapter=adapter)
        if connection.send(frame) != len(frame):
            raise OSError("Secret Broker response send failed")

    def close(self) -> None:
        self._stop.set()
        with self._lock:
            listener = self._listener
            self._listener = None
            executor = self._executor
            self._executor = None
            futures = tuple(self._futures)
        if listener is not None:
            listener.close()
        if executor is not None:
            # Finite drain attempt. A synchronous backing store cannot be
            # forcibly interrupted, so close its clients when grace expires.
            _done, pending = wait(futures, timeout=2.0)
            if pending:
                with self._lock:
                    connections = tuple(self._connections)
                    self._connections.clear()
                for connection in connections:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    connection.close()
            executor.shutdown(wait=False, cancel_futures=True)
        if self._unlink_on_close and self._socket_identity is not None:
            try:
                info = self._socket_path.lstat()
                if (info.st_dev, info.st_ino) == self._socket_identity:
                    self._socket_path.unlink()
            except FileNotFoundError:
                pass


__all__ = ["UnixSecretBrokerServer"]
