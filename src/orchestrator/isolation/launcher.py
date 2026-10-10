"""Fail-closed read-only Linux command launcher backed by systemd."""

from __future__ import annotations

from dataclasses import dataclass, replace
import math
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Literal, Sequence
import uuid

from .workspace import WorkspaceBoundaryError, snapshot_workspace


_MAX_INPUT_BYTES = 1_048_576
_RUNTIME_PATH_TOKEN = "@maestro-runtime@/"
_CGROUP_ROOT = Path("/sys/fs/cgroup")
_SYSTEMD_UNIT = re.compile(
    r"^maestro-(?:attempt-[0-9a-f]{32}|candidate-[0-9a-f]{32})\.(?:service|scope)$"
    r"|^maestro-provider-[0-9a-f]{32}\.service$"
)


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
class SandboxTerminationReceipt:
    """Host-observed proof that one exact systemd attempt scope is stopped."""

    unit_name: str
    control_group: str
    active_state: Literal["inactive", "failed"]
    cgroup_empty: bool

    def __post_init__(self) -> None:
        if not isinstance(self.unit_name, str) or not _SYSTEMD_UNIT.fullmatch(self.unit_name):
            raise ValueError("termination receipt unit name is invalid")
        if not isinstance(self.active_state, str) or self.active_state not in {"inactive", "failed"}:
            raise ValueError("termination receipt requires an inactive systemd unit")
        if self.cgroup_empty is not True:
            raise ValueError("termination receipt requires an empty cgroup")
        if not isinstance(self.control_group, str):
            raise ValueError("termination receipt cgroup path is invalid")
        path = Path(self.control_group)
        if (
            not path.is_absolute()
            or path.as_posix() != self.control_group
            or path.name != self.unit_name
            or path.parent.name != "app.slice"
            or ".." in path.parts
        ):
            raise ValueError("termination receipt cgroup path is invalid")


@dataclass(frozen=True)
class SandboxResult:
    """Bounded output and terminal status from an isolated command."""

    unit_name: str
    returncode: int
    stdout: bytes
    stderr: bytes
    elapsed_seconds: float
    termination_receipt: SandboxTerminationReceipt | None
    cancelled: bool
    timed_out: bool
    output_limited: bool
    input_written: bool = True

    def __post_init__(self) -> None:
        if self.termination_receipt is not None:
            if not isinstance(self.termination_receipt, SandboxTerminationReceipt):
                raise ValueError("sandbox termination receipt must be host verified")
            if self.termination_receipt.unit_name != self.unit_name:
                raise ValueError("termination receipt does not match the sandbox unit")

    @property
    def termination_confirmed(self) -> bool:
        """Compatibility view derived solely from verified host receipt data."""

        return self.termination_receipt is not None


class _RetainedStaging:
    """Private staging that only the owning session may explicitly release."""

    def __init__(self, temporary: tempfile.TemporaryDirectory[str]) -> None:
        self._temporary = temporary
        self.path = Path(temporary.name)
        self._released = False
        temporary._finalizer.detach()

    def cleanup(self) -> None:
        return None

    def discard(self) -> None:
        if self._released:
            return
        self._released = True
        self._temporary.cleanup()


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
        input_bytes: bytes | None = None,
        expected_workspace_identity_hash: str | None = None,
    ) -> "SandboxSession":
        """Start a command with optional bounded stdin and return a cancellable handle."""

        if not platform.system().lower() == "linux":
            raise IsolationUnavailable("the systemd isolation backend is Linux-only")
        if not self._systemd_run or not self._systemctl:
            raise IsolationUnavailable("systemd-run and systemctl are required")
        _validate_command(command)
        if input_bytes is not None and (
            not isinstance(input_bytes, bytes) or len(input_bytes) > _MAX_INPUT_BYTES
        ):
            raise InvalidSandboxRequest("stdin payload must be at most 1 MiB of bytes")
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
        cgroup_parent = _systemd_cgroup_parent(self._systemctl, client_env)

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
            snapshot_workspace(
                root,
                workspace_snapshot,
                expected_identity_hash=expected_workspace_identity_hash,
            )
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
        scope_cgroup = cgroup_parent / unit_name
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
        try:
            expanded_command = _expand_runtime_paths(command, runtime_source, runtime_target)
        except InvalidSandboxRequest:
            staging.cleanup()
            raise
        exec_command = [
            "/usr/bin/env",
            "-i",
            *env_args,
            "/usr/bin/python3",
            "-P",
            "-S",
            "-m",
            "orchestrator.isolation._exec",
            "--",
            *expanded_command,
        ]
        systemd_command = [
            self._systemd_run,
            "--user",
            "--slice=app.slice",
            "--quiet",
            "--wait",
            "--pipe",
            "--collect",
            f"--unit={unit_name.removesuffix('.service')}",
            *(f"--property={value}" for value in properties),
            "--",
            *exec_command,
        ]
        retained_staging = _RetainedStaging(staging)
        try:
            process = subprocess.Popen(
                systemd_command,
                stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=client_env,
                bufsize=0,
                close_fds=True,
            )
        except OSError as exc:
            retained_staging.discard()
            raise IsolationUnavailable("systemd transient unit could not be started") from exc
        return SandboxSession(
            process=process,
            unit_name=unit_name,
            systemctl=self._systemctl,
            client_env=client_env,
            output_limit=limits.output_bytes,
            timeout_seconds=limits.timeout_seconds,
            staging=retained_staging,
            input_bytes=input_bytes,
            scope_cgroup=scope_cgroup,
            cleanup_on_termination=True,
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
        input_bytes: bytes | None = None,
        cancel_after_transport_exit: bool = False,
        stop_grace_seconds: float = 2.0,
        scope_cgroup: Path | None = None,
        cleanup_on_termination: bool = True,
    ) -> None:
        if (
            isinstance(stop_grace_seconds, bool)
            or not isinstance(stop_grace_seconds, (int, float))
            or not math.isfinite(stop_grace_seconds)
            or not 0 < stop_grace_seconds <= 30
        ):
            raise InvalidSandboxRequest("sandbox stop grace must be finite and bounded")
        self.unit_name = unit_name
        self._process = process
        self._systemctl = systemctl
        self._client_env = client_env
        self._output_limit = output_limit
        self._timeout_seconds = timeout_seconds
        if scope_cgroup is not None and isinstance(staging, tempfile.TemporaryDirectory):
            staging = _RetainedStaging(staging)
        self._staging = staging
        self._scope_cgroup = scope_cgroup
        self._cleanup_on_termination = cleanup_on_termination
        self._input_bytes = input_bytes
        self._cancel_after_transport_exit = cancel_after_transport_exit
        self._stop_grace_seconds = stop_grace_seconds
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
            if self._process.poll() is not None and not self._cancel_after_transport_exit:
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
                result = self._collect()
                # An absent unit is not a stop witness while systemd-run can
                # still submit it. Retain staging and withhold proof until the
                # owning host launch/transport operation has ended.
                if self._scope_cgroup is not None and self._process.poll() is not None:
                    receipt = _read_termination_receipt(
                        unit_name=self.unit_name,
                        expected_cgroup=self._scope_cgroup,
                        client_env=self._client_env,
                        systemctl=self._systemctl,
                    )
                    result = replace(result, termination_receipt=receipt)
                self._result = result
                return self._result
            except BaseException:
                self.cancel()
                try:
                    self._process.wait(timeout=self._stop_grace_seconds)
                except subprocess.TimeoutExpired:
                    pass  # Caller must retain staging when scope stop is unconfirmed.
                raise
            finally:
                if self._process.poll() is not None:
                    if self._scope_cgroup is None:
                        self._staging.cleanup()
                    elif self._cleanup_on_termination and self._result is not None:
                        if self._result.termination_confirmed:
                            _discard_staging(self._staging)

    def _collect(self) -> SandboxResult:
        assert self._process.stdout is not None and self._process.stderr is not None
        selector = selectors.DefaultSelector()
        streams = {self._process.stdout: bytearray(), self._process.stderr: bytearray()}
        for stream in streams:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
        input_stream = self._process.stdin if self._input_bytes is not None else None
        input_offset = 0
        if input_stream is not None:
            if self._input_bytes:
                os.set_blocking(input_stream.fileno(), False)
                selector.register(input_stream, selectors.EVENT_WRITE)
            else:
                input_stream.close()
        cancelled = False
        timed_out = False
        output_limited = False
        deadline = self._started + self._timeout_seconds
        next_kill_retry = self._started
        stop_deadline = None
        try:
            while selector.get_map() or self._process.poll() is None:
                now = time.monotonic()
                if self._cancel_requested.is_set():
                    cancelled = True
                if now >= deadline:
                    timed_out = True
                if cancelled or timed_out or output_limited:
                    if stop_deadline is None:
                        stop_deadline = now + self._stop_grace_seconds
                    if stop_deadline is not None and now >= stop_deadline:
                        break
                if (cancelled or timed_out or output_limited) and now >= next_kill_retry:
                    self.cancel()
                    next_kill_retry = now + 1
                if input_stream is not None and not input_stream.closed and (
                    cancelled or timed_out or output_limited or self._process.poll() is not None
                ):
                    selector.unregister(input_stream)
                    input_stream.close()
                for key, _ in selector.select(timeout=0.1):
                    stream = key.fileobj
                    if stream is input_stream:
                        assert self._input_bytes is not None
                        try:
                            sent = os.write(stream.fileno(), self._input_bytes[input_offset:input_offset + 8192])
                        except BlockingIOError:
                            continue
                        except OSError:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        input_offset += sent
                        if input_offset == len(self._input_bytes):
                            selector.unregister(stream)
                            stream.close()
                        continue
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
            if stop_deadline is None or time.monotonic() < stop_deadline:
                self._process.wait(timeout=self._stop_grace_seconds)
        finally:
            selector.close()
            if input_stream is not None and not input_stream.closed:
                input_stream.close()
            for stream in streams:
                if not stream.closed:
                    stream.close()
        elapsed = max(0.0, time.monotonic() - self._started)
        returncode = int(self._process.returncode) if self._process.returncode is not None else 125
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
            termination_receipt=None,
            cancelled=cancel_signal_accepted and not timed_out and not output_limited,
            timed_out=timed_out,
            output_limited=output_limited,
            input_written=self._input_bytes is None or input_offset == len(self._input_bytes),
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


def _expand_runtime_paths(
    command: Sequence[str], runtime_source: Path, runtime_target: Path
) -> list[str]:
    """Resolve explicit trusted-runtime arguments to this unit's read-only bind."""

    expanded: list[str] = []
    source_root = runtime_source.resolve(strict=True)
    for value in command:
        if not value.startswith(_RUNTIME_PATH_TOKEN):
            expanded.append(value)
            continue
        relative = Path(value[len(_RUNTIME_PATH_TOKEN):])
        if not relative.parts or relative.is_absolute() or ".." in relative.parts:
            raise InvalidSandboxRequest("trusted runtime path is invalid")
        try:
            source_file = (source_root / relative).resolve(strict=True)
            source_file.relative_to(source_root)
        except (OSError, RuntimeError, ValueError) as exc:
            raise InvalidSandboxRequest("trusted runtime path is invalid") from exc
        if not source_file.is_file():
            raise InvalidSandboxRequest("trusted runtime path must reference a file")
        expanded.append(str(runtime_target / relative))
    return expanded


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


def _discard_staging(staging) -> None:
    discard = getattr(staging, "discard", None)
    if callable(discard):
        discard()
    else:
        staging.cleanup()


def _systemd_cgroup_parent(systemctl: str, client_env: dict[str, str]) -> Path:
    """Resolve the trusted user app.slice below the cgroup-v2 mount."""

    try:
        result = subprocess.run(
            [systemctl, "--user", "show", "app.slice", "--property=ControlGroup"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            env=client_env,
            check=False,
            timeout=3,
        )
        properties = _parse_systemd_properties(result.stdout, {"ControlGroup"})
        control_group = None if properties is None else properties["ControlGroup"]
        if result.returncode != 0 or not control_group or not control_group.startswith("/"):
            raise ValueError("systemd app.slice has no cgroup path")
        components = control_group.removeprefix("/").split("/")
        if any(part in {"", ".", ".."} for part in components) or components[-1] != "app.slice":
            raise ValueError("systemd app.slice cgroup path is invalid")
        parent = _CGROUP_ROOT.joinpath(*components)
        descriptor = _open_cgroup_directory(parent)
        os.close(descriptor)
        return parent
    except (OSError, subprocess.TimeoutExpired, UnicodeError, ValueError) as exc:
        raise IsolationUnavailable("the app.slice cgroup cannot be verified") from exc


def _parse_systemd_properties(
    payload: bytes, required: set[str]
) -> dict[str, str] | None:
    try:
        lines = payload.decode("ascii", errors="strict").splitlines()
    except (AttributeError, UnicodeError):
        return None
    values: dict[str, str] = {}
    for line in lines:
        if not line or "=" not in line:
            return None
        name, value = line.split("=", 1)
        if not name or name in values or name not in required:
            return None
        values[name] = value
    if values.keys() != required:
        return None
    return values


def _read_termination_receipt(
    *,
    unit_name: str,
    expected_cgroup: Path,
    client_env: dict[str, str],
    timeout_seconds: float = 2.0,
    systemctl: str = "systemctl",
) -> SandboxTerminationReceipt | None:
    """Poll systemd and cgroup v2 for a bounded proof of complete stop."""

    if (
        not isinstance(unit_name, str)
        or not _SYSTEMD_UNIT.fullmatch(unit_name)
        or isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds <= 10
    ):
        return None
    try:
        relative = expected_cgroup.relative_to(_CGROUP_ROOT)
        if (
            not relative.parts
            or relative.name != unit_name
            or relative.parent.name != "app.slice"
            or ".." in relative.parts
        ):
            return None
        expected_control_group = "/" + relative.as_posix()
    except (AttributeError, TypeError, ValueError):
        return None

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            result = subprocess.run(
                [
                    systemctl, "--user", "show", unit_name,
                    "--property=ActiveState", "--property=LoadState", "--property=ControlGroup",
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                env=client_env,
                check=False,
                timeout=min(3, max(0.001, deadline - time.monotonic())),
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        properties = _parse_systemd_properties(
            result.stdout, {"ActiveState", "LoadState", "ControlGroup"}
        )
        if result.returncode != 0 or properties is None:
            return None
        active_state = properties["ActiveState"]
        load_state = properties["LoadState"]
        control_group = properties["ControlGroup"]
        if load_state == "loaded":
            if control_group != expected_control_group:
                return None
        elif load_state == "not-found":
            if control_group:
                return None
        else:
            return None
        if active_state in {"inactive", "failed"} and _cgroup_is_empty(expected_cgroup):
            return SandboxTerminationReceipt(
                unit_name=unit_name,
                control_group=expected_control_group,
                active_state=active_state,
                cgroup_empty=True,
            )
        if active_state not in {"active", "activating", "deactivating", "inactive", "failed"}:
            return None
        if time.monotonic() >= deadline:
            return None
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))


def _open_cgroup_directory(path: Path) -> int:
    """Open a cgroup directory component-by-component without following links."""

    try:
        relative = path.relative_to(_CGROUP_ROOT)
    except ValueError as exc:
        raise ValueError("cgroup path escaped the cgroup-v2 mount") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError("cgroup path is malformed")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(_CGROUP_ROOT, flags)
    try:
        for component in relative.parts:
            child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _cgroup_is_empty(path: Path) -> bool:
    """Return true only for a safely opened scope with populated 0, or removal."""

    try:
        descriptor = _open_cgroup_directory(path)
    except FileNotFoundError:
        # systemd may remove the cgroup after it becomes empty.
        return True
    except (OSError, ValueError):
        return False
    try:
        events = os.open(
            "cgroup.events",
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=descriptor,
        )
        with os.fdopen(events, "rb") as stream:
            payload = stream.read(4097)
        if len(payload) > 4096:
            return False
        properties: dict[str, str] = {}
        for line in payload.decode("ascii", errors="strict").splitlines():
            fields = line.split()
            if len(fields) != 2 or fields[0] in properties or fields[1] not in {"0", "1"}:
                return False
            properties[fields[0]] = fields[1]
        return properties.get("populated") == "0"
    except (OSError, UnicodeError):
        return False
    finally:
        os.close(descriptor)



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
        "InaccessiblePaths=-/mnt/wslg/run/user",
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
