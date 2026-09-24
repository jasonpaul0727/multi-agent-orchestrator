"""Fail-closed read-only Linux command launcher backed by systemd."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import platform
import selectors
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Sequence
import uuid

from .workspace import WorkspaceBoundaryError, snapshot_workspace


class IsolationUnavailable(RuntimeError):
    """The host cannot prove all required isolation controls for a launch."""


class InvalidSandboxRequest(ValueError):
    """A sandbox request is malformed or outside the supported profile."""


@dataclass(frozen=True)
class SandboxLimits:
    """Hard limits requested for one isolated command attempt."""

    memory_bytes: int = 512 * 1024 * 1024
    tasks: int = 64
    cpu_percent: int = 100
    timeout_seconds: int = 300
    output_bytes: int = 1024 * 1024
    nofile: int = 256
    file_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        bounds = {
            "memory_bytes": (16 * 1024 * 1024, 16 * 1024 * 1024 * 1024),
            "tasks": (1, 4096),
            "cpu_percent": (1, 1000),
            "timeout_seconds": (1, 24 * 60 * 60),
            "output_bytes": (1, 64 * 1024 * 1024),
            "nofile": (16, 65536),
            "file_bytes": (1, 16 * 1024 * 1024 * 1024),
        }
        for name, (minimum, maximum) in bounds.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
                raise InvalidSandboxRequest(f"{name} must be an integer in [{minimum}, {maximum}]")


@dataclass(frozen=True)
class SandboxResult:
    """Bounded output and terminal status from an isolated command."""

    unit_name: str
    returncode: int
    stdout: bytes
    stderr: bytes
    elapsed_seconds: float
    termination_confirmed: bool
    cancelled: bool
    timed_out: bool
    output_limited: bool


class SystemdReadOnlyLauncher:
    """Launch one command with a read-only workspace and no network access.

    This initial backend intentionally does not implement workspace writes or
    change publication.  It refuses to run unless the transient service
    manager and the requested cgroup/rlimit values can be verified in-process.
    """

    def __init__(self, *, systemd_run: str | None = None, systemctl: str | None = None) -> None:
        self._systemd_run = systemd_run or shutil.which("systemd-run")
        self._systemctl = systemctl or shutil.which("systemctl")

    def launch(
        self,
        workspace: str | Path,
        command: Sequence[str],
        *,
        limits: SandboxLimits | None = None,
    ) -> "SandboxSession":
        """Start a command and return a handle supporting wait or cancellation."""

        if not platform.system().lower() == "linux":
            raise IsolationUnavailable("the systemd isolation backend is Linux-only")
        if not self._systemd_run or not self._systemctl:
            raise IsolationUnavailable("systemd-run and systemctl are required")
        _validate_command(command)
        limits = limits or SandboxLimits()
        root = _validated_workspace(workspace)
        client_env = _systemd_client_environment()
        try:
            probe = subprocess.run(
                [self._systemctl, "--user", "show-environment"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=client_env,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise IsolationUnavailable("systemd user manager probe failed") from exc
        if probe.returncode != 0:
            raise IsolationUnavailable("a usable systemd user manager is required")

        staging = _create_staging(root)
        mount_root = Path(staging.name)
        workspace_target = mount_root / "workspace"
        runtime_target = mount_root / "runtime"
        workspace_snapshot = mount_root / "workspace-snapshot"
        try:
            workspace_target.mkdir()
            runtime_target.mkdir()
        except OSError as exc:
            staging.cleanup()
            raise IsolationUnavailable("sandbox mount targets cannot be staged") from exc
        try:
            snapshot_workspace(root, workspace_snapshot)
        except WorkspaceBoundaryError as exc:
            staging.cleanup()
            raise InvalidSandboxRequest("workspace snapshot failed closed") from exc
        runtime_source = Path(__file__).resolve().parents[2]
        if not runtime_source.is_dir():
            staging.cleanup()
            raise IsolationUnavailable("the trusted isolation runtime is unavailable")
        if not _systemd_path_supported(root) or not _systemd_path_supported(runtime_source):
            staging.cleanup()
            raise InvalidSandboxRequest("workspace paths with spaces or systemd separators are unsupported")

        unit_name = f"maestro-attempt-{uuid.uuid4().hex}.service"
        properties = _service_properties(
            root=workspace_snapshot,
            runtime_source=runtime_source,
            workspace_target=workspace_target,
            runtime_target=runtime_target,
            limits=limits,
        )
        env_args = (
            "PATH=/usr/bin:/bin",
            "LANG=C.UTF-8",
            "LC_ALL=C.UTF-8",
            "HOME=/nonexistent",
            "TMPDIR=/tmp",
            f"PYTHONPATH={runtime_target}",
            f"MAESTRO_EXPECT_MEMORY={limits.memory_bytes}",
            f"MAESTRO_EXPECT_TASKS={limits.tasks}",
            f"MAESTRO_EXPECT_CPU={limits.cpu_percent}",
            f"MAESTRO_EXPECT_NOFILE={limits.nofile}",
            f"MAESTRO_EXPECT_FSIZE={limits.file_bytes}",
        )
        exec_command = [
            "/usr/bin/env",
            "-i",
            *env_args,
            "/usr/bin/python3",
            "-m",
            "orchestrator.isolation._exec",
            "--",
            *command,
        ]
        systemd_command = [
            self._systemd_run,
            "--user",
            "--quiet",
            "--wait",
            "--pipe",
            "--collect",
            f"--unit={unit_name.removesuffix('.service')}",
            *(f"--property={value}" for value in properties),
            "--",
            *exec_command,
        ]
        try:
            process = subprocess.Popen(
                systemd_command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=client_env,
                bufsize=0,
                close_fds=True,
            )
        except OSError as exc:
            staging.cleanup()
            raise IsolationUnavailable("systemd transient unit could not be started") from exc
        return SandboxSession(
            process=process,
            unit_name=unit_name,
            systemctl=self._systemctl,
            client_env=client_env,
            output_limit=limits.output_bytes,
            timeout_seconds=limits.timeout_seconds,
            staging=staging,
        )


class SandboxSession:
    """Running transient service; cancellation kills every process in its unit."""

    def __init__(
        self,
        *,
        process: subprocess.Popen[bytes],
        unit_name: str,
        systemctl: str,
        client_env: dict[str, str],
        output_limit: int,
        timeout_seconds: int,
        staging: tempfile.TemporaryDirectory[str],
    ) -> None:
        self.unit_name = unit_name
        self._process = process
        self._systemctl = systemctl
        self._client_env = client_env
        self._output_limit = output_limit
        self._timeout_seconds = timeout_seconds
        self._staging = staging
        self._started = time.monotonic()
        self._cancel_requested = threading.Event()
        self._cancel_signal_accepted = False
        self._cancel_lock = threading.Lock()
        self._wait_lock = threading.Lock()
        self._result: SandboxResult | None = None

    def cancel(self) -> bool:
        """Request SIGKILL for the entire unit; wait() supplies stop confirmation."""

        # The collector retries cancellation while draining the systemd-run
        # pipe. Serialize those retries with the caller's initial signal so the
        # child cannot return a result before the accepted signal is recorded.
        with self._cancel_lock:
            if self._process.poll() is not None:
                return False
            self._cancel_requested.set()
            try:
                result = subprocess.run(
                    [self._systemctl, "--user", "kill", "--kill-whom=all", "--signal=SIGKILL", self.unit_name],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=self._client_env,
                    check=False,
                    timeout=3,
                )
            except (OSError, subprocess.TimeoutExpired):
                return False
            accepted = result.returncode == 0
            if accepted:
                self._cancel_signal_accepted = True
            return accepted

    def wait(self) -> SandboxResult:
        """Drain capped output until exit, runtime deadline, or cancellation."""

        with self._wait_lock:
            if self._result is not None:
                return self._result
            try:
                self._result = self._collect()
                return self._result
            except BaseException:
                self.cancel()
                self._process.wait()
                raise
            finally:
                if self._process.poll() is not None:
                    self._staging.cleanup()

    def _collect(self) -> SandboxResult:
        assert self._process.stdout is not None and self._process.stderr is not None
        selector = selectors.DefaultSelector()
        streams = {self._process.stdout: bytearray(), self._process.stderr: bytearray()}
        for stream in streams:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
        cancelled = False
        timed_out = False
        output_limited = False
        deadline = self._started + self._timeout_seconds
        next_kill_retry = self._started
        try:
            while selector.get_map() or self._process.poll() is None:
                now = time.monotonic()
                if self._cancel_requested.is_set():
                    cancelled = True
                if now >= deadline:
                    timed_out = True
                if (cancelled or timed_out or output_limited) and now >= next_kill_retry:
                    self.cancel()
                    next_kill_retry = now + 1
                for key, _ in selector.select(timeout=0.1):
                    stream = key.fileobj
                    try:
                        chunk = os.read(stream.fileno(), 8192)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(stream)
                        stream.close()
                        continue
                    captured = sum(len(value) for value in streams.values())
                    remaining = max(0, self._output_limit - captured)
                    streams[stream].extend(chunk[:remaining])
                    if len(chunk) > remaining and not output_limited:
                        output_limited = True
                        self.cancel()
                        cancelled = True
            self._process.wait()
        finally:
            selector.close()
            for stream in streams:
                if not stream.closed:
                    stream.close()
        elapsed = max(0.0, time.monotonic() - self._started)
        returncode = int(self._process.returncode or 0)
        if (
            not self._cancel_requested.is_set()
            and not output_limited
            and elapsed >= self._timeout_seconds
        ):
            timed_out = True
        with self._cancel_lock:
            cancel_signal_accepted = self._cancel_signal_accepted
        return SandboxResult(
            unit_name=self.unit_name,
            returncode=returncode,
            stdout=bytes(streams[self._process.stdout]),
            stderr=bytes(streams[self._process.stderr]),
            elapsed_seconds=elapsed,
            termination_confirmed=self._process.returncode is not None,
            cancelled=cancel_signal_accepted and not timed_out and not output_limited,
            timed_out=timed_out,
            output_limited=output_limited,
        )


def _validate_command(command: Sequence[str]) -> None:
    if isinstance(command, (str, bytes)) or not command or len(command) > 128:
        raise InvalidSandboxRequest("command must be a non-empty argument sequence")
    total = 0
    for value in command:
        if not isinstance(value, str) or not value or "\x00" in value:
            raise InvalidSandboxRequest("command arguments must be non-empty strings without NUL")
        total += len(value.encode("utf-8"))
    if total > 32768:
        raise InvalidSandboxRequest("command arguments exceed the supported size")


def _validated_workspace(workspace: str | Path) -> Path:
    path = Path(workspace)
    if not path.is_absolute():
        raise InvalidSandboxRequest("workspace must be an absolute path")
    try:
        if path.is_symlink() or not path.is_dir():
            raise InvalidSandboxRequest("workspace must be an existing real directory")
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise InvalidSandboxRequest("workspace cannot be resolved") from exc
    if not _systemd_path_supported(resolved):
        raise InvalidSandboxRequest("workspace path uses unsupported systemd property characters")
    return resolved


def _systemd_path_supported(path: Path) -> bool:
    rendered = str(path)
    return not any(character in rendered for character in (" ", "\t", "\r", "\n", ":", "\\"))


def _create_staging(workspace: Path) -> tempfile.TemporaryDirectory[str]:
    """Create private staging outside the workspace being snapshotted."""

    candidates = (Path(tempfile.gettempdir()), Path("/var/tmp"))
    for base in candidates:
        try:
            resolved_base = base.resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        try:
            resolved_base.relative_to(workspace)
            continue
        except ValueError:
            pass
        if not _systemd_path_supported(resolved_base):
            continue
        try:
            staging = tempfile.TemporaryDirectory(prefix="maestro-sandbox-", dir=str(resolved_base))
        except OSError:
            continue
        try:
            resolved_staging = Path(staging.name).resolve(strict=True)
        except (OSError, RuntimeError):
            staging.cleanup()
            continue
        try:
            resolved_staging.relative_to(workspace)
        except ValueError:
            return staging
        staging.cleanup()
    raise IsolationUnavailable("no safe staging directory exists outside the workspace")


def _systemd_client_environment() -> dict[str, str]:
    env: dict[str, str] = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    for name in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "SYSTEMD_BUS_ADDRESS"):
        value = os.environ.get(name)
        if value:
            env[name] = value
    if "XDG_RUNTIME_DIR" not in env:
        raise IsolationUnavailable("XDG_RUNTIME_DIR is required for the systemd user manager")
    return env


def _service_properties(
    *,
    root: Path,
    runtime_source: Path,
    workspace_target: Path,
    runtime_target: Path,
    limits: SandboxLimits,
) -> tuple[str, ...]:
    controls = tuple(
        f"InaccessiblePaths=-{workspace_target / name}"
        for name in (".git", ".maestro")
    )
    return (
        "PrivateNetwork=yes",
        "PrivateTmp=yes",
        "PrivateUsers=yes",
        "PrivateDevices=yes",
        "ProtectHome=tmpfs",
        "ProtectSystem=strict",
        "ProtectProc=invisible",
        "ProcSubset=pid",
        "ProtectControlGroups=yes",
        "ProtectKernelTunables=yes",
        "ProtectKernelModules=yes",
        "ProtectKernelLogs=yes",
        "ProtectClock=yes",
        "ProtectHostname=yes",
        "NoNewPrivileges=yes",
        "CapabilityBoundingSet=",
        "RestrictAddressFamilies=AF_UNIX",
        "RestrictRealtime=yes",
        "RestrictSUIDSGID=yes",
        "LockPersonality=yes",
        "MemoryDenyWriteExecute=yes",
        "InaccessiblePaths=-/run/user",
        "InaccessiblePaths=-/run/dbus",
        "InaccessiblePaths=-/etc/shadow",
        "InaccessiblePaths=-/etc/gshadow",
        "InaccessiblePaths=-/etc/sudoers",
        "InaccessiblePaths=-/etc/sudoers.d",
        "InaccessiblePaths=-/etc/ssh",
        "InaccessiblePaths=-/etc/ssl/private",
        "InaccessiblePaths=-/etc/NetworkManager/system-connections",
        "InaccessiblePaths=-/etc/credstore",
        "InaccessiblePaths=-/etc/credstore.encrypted",
        "InaccessiblePaths=-/etc/apt/auth.conf.d",
        f"BindReadOnlyPaths={root}:{workspace_target}",
        f"BindReadOnlyPaths={runtime_source}:{runtime_target}",
        *controls,
        f"WorkingDirectory={workspace_target}",
        f"MemoryMax={limits.memory_bytes}",
        "MemorySwapMax=0",
        f"TasksMax={limits.tasks}",
        f"CPUQuota={limits.cpu_percent}%",
        "CPUQuotaPeriodSec=100ms",
        f"RuntimeMaxSec={limits.timeout_seconds}s",
        "TimeoutStopSec=1s",
        "KillMode=control-group",
        f"LimitNOFILE={limits.nofile}",
        f"LimitFSIZE={limits.file_bytes}",
        "LimitCORE=0",
    )


__all__ = [
    "InvalidSandboxRequest",
    "IsolationUnavailable",
    "SandboxLimits",
    "SandboxResult",
    "SandboxSession",
    "SystemdReadOnlyLauncher",
]
