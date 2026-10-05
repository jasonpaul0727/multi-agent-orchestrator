"""Readiness-gated Linux supervisor; bootstrap contains descriptors only."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import math
import os
from pathlib import Path
import secrets
import select
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time

from orchestrator.config.models import ProviderSpec
from .broker import SecretAccessRule, SecretBrokerUnavailable
from .client import UnixSocketSecretBroker
from .ipc import _decode_exact_json_object

MAX_BOOTSTRAP_BYTES = 256 * 1024
_STARTUP_SECONDS = 10.0
_KILL_WAIT_SECONDS = 2.0


def _identity(path: Path) -> tuple[int, int]:
    info = path.lstat()
    return info.st_dev, info.st_ino


def _require_hidden_mount_aliases(runtime: Path, private_area: Path) -> None:
    # A subtree bind preserves inodes but changes the path under which the
    # socket is reachable. Project every mount's filesystem-root mapping; only
    # the barriers hidden by all shipped profiles may expose the runtime tree.
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) < 10 or "-" not in fields or fields.index("-") < 6:
            raise ValueError
        device = fields[2].split(":")
        if len(device) != 2 or not all(part.isdigit() for part in device):
            raise ValueError
        paths = []
        for field in fields[3:5]:
            for encoded, decoded in ((r"\040", " "), (r"\011", "\t"), (r"\012", "\n"), (r"\134", "\\")):
                field = field.replace(encoded, decoded)
            path = Path(field)
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError
            paths.append(path)
        mounts.append((fields[2], *paths))
    containing = [mount for mount in mounts if runtime.is_relative_to(mount[2])]
    if not containing:
        raise ValueError
    device, filesystem_root, mountpoint = max(containing, key=lambda mount: len(mount[2].parts))
    filesystem_path = filesystem_root / runtime.relative_to(mountpoint)
    hidden = (private_area, Path("/mnt/wslg/run/user") / str(os.getuid()))
    for other_device, other_root, other_mountpoint in mounts:
        if device == other_device and filesystem_path.is_relative_to(other_root):
            alias = other_mountpoint / filesystem_path.relative_to(other_root)
            if not any(alias.is_relative_to(barrier) for barrier in hidden):
                raise ValueError


def _checked_socket_identity(path: Path) -> tuple[int, int]:
    info = path.lstat()
    if (not stat.S_ISSOCK(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_uid != os.getuid()):
        raise RuntimeError("Secret Broker socket verification failed")
    return info.st_dev, info.st_ino


def _read_frame(fd: int, *, deadline: float, max_bytes: int) -> dict[str, object]:
    def read_exact(size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
                raise TimeoutError("Secret Broker pipe deadline expired")
            chunk = os.read(fd, size - len(data))
            if not chunk:
                raise RuntimeError("Secret Broker pipe closed")
            data.extend(chunk)
        return bytes(data)
    size = struct.unpack("!I", read_exact(4))[0]
    if not 0 < size <= max_bytes:
        raise ValueError("Secret Broker pipe frame exceeds its bound")
    return _decode_exact_json_object(read_exact(size), max_bytes=max_bytes)


def _write_frame(fd: int, frame: bytes, *, deadline: float) -> None:
    os.set_blocking(fd, False)
    offset = 0
    while offset < len(frame):
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([], [fd], [], remaining)[1]:
            raise TimeoutError("Secret Broker bootstrap deadline expired")
        try:
            offset += os.write(fd, frame[offset:])
        except BlockingIOError:
            continue


def _encode_bootstrap(*, rules: Sequence[SecretAccessRule], providers: Mapping[str, ProviderSpec],
                      event_store_path: Path, socket_path: Path, session_nonce: str) -> bytes:
    if not rules or any(not isinstance(rule, SecretAccessRule) for rule in rules):
        raise ValueError("Secret Broker requires explicit rules")
    if any(not isinstance(provider, ProviderSpec) or key != provider.id for key, provider in providers.items()):
        raise ValueError("Secret Broker Provider registry is invalid")
    payload = dict(version=1, expected_parent_pid=os.getpid(), event_store_path=str(event_store_path),
        socket_path=str(socket_path), session_nonce=session_nonce,
        rules=[dict(provider_id=r.provider_id, secret_ref=r.secret_ref, endpoint=r.endpoint,
                    purpose=r.purpose, allowed_run_ids=sorted(r.allowed_run_ids)) for r in rules],
        providers=[p.model_dump(mode="json") for p in providers.values()])
    encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) > MAX_BOOTSTRAP_BYTES:
        raise ValueError("Secret Broker bootstrap exceeds its bound")
    return struct.pack("!I", len(encoded)) + encoded


class SecretBrokerProcessSession:
    def __init__(self, *, process: subprocess.Popen, pidfd: int, socket_path: Path,
                 directory_identity: tuple[int, int], session_nonce: str):
        self._process = process
        self._pidfd = pidfd
        self._socket_path = socket_path
        self._directory_identity = directory_identity
        self._socket_identity: tuple[int, int] | None = None
        self._client = UnixSocketSecretBroker(socket_path=socket_path, session_nonce=session_nonce)
        self._ready = False
        self._closed = False
        self._lock = threading.Lock()

    @property
    def client(self) -> UnixSocketSecretBroker:
        if not self._ready or self._closed or not self.is_alive:
            raise SecretBrokerUnavailable("Secret Broker session is unavailable")
        return self._client

    @property
    def process_pid(self) -> int:
        return self._process.pid

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    @property
    def is_alive(self) -> bool:
        return self._process.poll() is None

    def close(self, timeout_seconds: float = 5.0) -> None:
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 3600):
            raise ValueError("Secret Broker shutdown deadline is invalid")
        with self._lock:
            self._ready = False
            self._client._available = False
            if self._closed:
                return
            try:
                signal.pidfd_send_signal(self._pidfd, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self._process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                try:
                    signal.pidfd_send_signal(self._pidfd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    self._process.wait(timeout=_KILL_WAIT_SECONDS)
                except subprocess.TimeoutExpired:
                    raise SecretBrokerUnavailable("Secret Broker stop is unconfirmed") from None
            # Removal requires wait-confirmed exit and both original identities.
            self._closed = True
            os.close(self._pidfd)
            self._pidfd = -1
            self._cleanup_paths()

    def _cleanup_paths(self) -> None:
        directory = self.socket_path.parent
        try:
            if _identity(directory) != self._directory_identity:
                return
            if self._socket_identity is not None:
                try:
                    if _identity(self.socket_path) != self._socket_identity:
                        return
                    self.socket_path.unlink()
                except FileNotFoundError:
                    pass
            directory.rmdir()
        except OSError:
            # Unknown/replaced contents are retained; never recursive cleanup.
            pass


class SecretBrokerProcessManager:
    def __init__(self, *, runtime_root: Path, event_store_path: Path,
                 python_executable: Path | None = None):
        self._runtime_root = Path(runtime_root)
        self._event_store_path = Path(event_store_path)
        self._python_executable = Path(python_executable or sys.executable)

    def _validate_host(self) -> None:
        if (sys.platform != "linux" or not hasattr(os, "pidfd_open")
            or not hasattr(signal, "pidfd_send_signal")):
            raise RuntimeError("Secret Broker requires Linux process handles")
        try:
            root = self._runtime_root.lstat()
            event = self._event_store_path.lstat()
        except OSError:
            raise ValueError("Secret Broker paths are unavailable") from None
        if (not self._runtime_root.is_absolute() or not stat.S_ISDIR(root.st_mode)
            or stat.S_IMODE(root.st_mode) != 0o700 or root.st_uid != os.getuid()):
            raise ValueError("Secret Broker runtime root is unsafe")
        try:
            private_area = Path("/run/user") / str(os.getuid())
            resolved = self._runtime_root.resolve(strict=True)
            if (resolved != self._runtime_root or private_area.resolve(strict=True) != private_area
                or not resolved.is_relative_to(private_area)):
                raise ValueError
            # Every ancestor within the private user area must be private and
            # canonical. Environment-selected XDG aliases cannot weaken the
            # /run/user barrier shared by all shipped sandbox profiles.
            private_paths = [resolved]
            while private_paths[-1] != private_area:
                private_paths.append(private_paths[-1].parent)
            for path in private_paths:
                info = path.lstat()
                if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o700):
                    raise ValueError
            source = Path(__file__).resolve().parents[2]
            if resolved.is_relative_to(source):
                raise ValueError
            # Resolve source symlinks and detect bind aliases by inode as well
            # as pathname, including source ancestors mounted into a sandbox.
            source_identities = {_identity(path) for path in (source, *source.parents) if path.exists()}
            if any(_identity(path) in source_identities for path in private_paths):
                raise ValueError
            _require_hidden_mount_aliases(resolved, private_area)
        except (OSError, RuntimeError, ValueError):
            raise ValueError("Secret Broker runtime root is outside the protected private area or aliased into the runtime") from None
        if not self._event_store_path.is_absolute() or not stat.S_ISREG(event.st_mode):
            raise ValueError("Secret Broker EventStore must be an absolute regular file")
        if len(os.fsencode(self._runtime_root / ("broker-" + "x" * 16) / "broker.sock")) > 107:
            raise ValueError("Secret Broker socket path is too long")
        if not self._python_executable.is_absolute() or not self._python_executable.is_file():
            raise ValueError("Secret Broker executable must be an absolute file")

    def start(self, *, rules: Sequence[SecretAccessRule],
              providers: Mapping[str, ProviderSpec]) -> SecretBrokerProcessSession:
        self._validate_host()
        frozen_rules, frozen_providers = tuple(rules), dict(providers)
        nonce = secrets.token_urlsafe(48)
        # Check the entire bootstrap bound before creating paths or children.
        candidate = self._runtime_root / ("broker-" + "x" * 16) / "broker.sock"
        _encode_bootstrap(rules=frozen_rules, providers=frozen_providers,
                         event_store_path=self._event_store_path, socket_path=candidate, session_nonce=nonce)
        directory = Path(tempfile.mkdtemp(prefix="broker-", dir=self._runtime_root))
        directory_identity = _identity(directory)
        socket_path = directory / "broker.sock"
        bootstrap = _encode_bootstrap(rules=frozen_rules, providers=frozen_providers,
            event_store_path=self._event_store_path, socket_path=socket_path, session_nonce=nonce)
        process = None
        session = None
        try:
            code_root = Path(__file__).resolve().parents[2]
            process = subprocess.Popen([str(self._python_executable), "-m", "orchestrator.secrets._broker_child"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONPATH": str(code_root)},
                cwd=code_root, close_fds=True, bufsize=0)
            pidfd = os.pidfd_open(process.pid)
            session = SecretBrokerProcessSession(process=process, pidfd=pidfd, socket_path=socket_path,
                directory_identity=directory_identity, session_nonce=nonce)
            deadline = time.monotonic() + _STARTUP_SECONDS
            assert process.stdin is not None and process.stdout is not None
            _write_frame(process.stdin.fileno(), bootstrap, deadline=deadline)
            process.stdin.close()
            ready = _read_frame(process.stdout.fileno(), deadline=deadline, max_bytes=1024)
            if (set(ready) != {"status", "socket_dev", "socket_ino"} or ready["status"] != "ready"
                or type(ready["socket_dev"]) is not int or not 0 <= ready["socket_dev"] < 2**64
                or type(ready["socket_ino"]) is not int or not 0 < ready["socket_ino"] < 2**64):
                raise RuntimeError("Secret Broker child did not become ready")
            # Only the child's captured bind identity authorizes cleanup. Never
            # adopt whatever now occupies this pathname, even on startup failure.
            session._socket_identity = (ready["socket_dev"], ready["socket_ino"])
            checked = _checked_socket_identity(socket_path)
            directory_info = directory.lstat()
            if (checked != session._socket_identity or _identity(directory) != directory_identity
                or not stat.S_ISDIR(directory_info.st_mode)
                or directory_info.st_uid != os.getuid() or stat.S_IMODE(directory_info.st_mode) != 0o700
                or not session.is_alive):
                raise RuntimeError("Secret Broker readiness changed")
            session._ready = True
            return session
        except Exception:
            if session is not None:
                session.close()
            elif process is not None:
                # pidfd acquisition failed: Popen still owns this unreaped child.
                process.kill()
                try:
                    process.wait(timeout=_KILL_WAIT_SECONDS)
                except subprocess.TimeoutExpired:
                    raise SecretBrokerUnavailable("Secret Broker stop is unconfirmed") from None
                if _identity(directory) == directory_identity:
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
            elif _identity(directory) == directory_identity:
                directory.rmdir()
            raise SecretBrokerUnavailable("Secret Broker startup failed") from None
        finally:
            if process is not None:
                for pipe in (process.stdin, process.stdout):
                    if pipe is not None:
                        pipe.close()
