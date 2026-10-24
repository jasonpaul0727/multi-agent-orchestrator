"""Fail-closed read-only Linux command launcher backed by systemd."""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
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
import weakref

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


class IsolationNotStarted(IsolationUnavailable):
    """Isolation was rejected before the untrusted launch boundary."""


class IsolationStartUnknown(IsolationUnavailable):
    """Launch may have started; retain staging and unresolved occupancy."""


class IsolationStopUnknown(IsolationUnavailable):
    """A launched unit lacks exact whole-unit stop proof."""


class InvalidSandboxRequest(ValueError, IsolationNotStarted):
    """A sandbox request is malformed or outside the supported profile."""


@dataclass(frozen=True, slots=True)
class PreparedSandboxLaunch:
    """A host-selected launch epoch, allocated before starting any process."""

    launch_id: str
    unit_name: str


@dataclass(frozen=True, slots=True)
class SandboxLaunchReceipt:
    """Observed launch binding, without any claim that the unit has stopped."""

    launch_id: str
    unit_name: str
    control_group: str
    invocation_id: str
    profile_sha256: str


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
        self._prepared: dict[str, PreparedSandboxLaunch] = {}
        self._consumed: set[str] = set()
        self._sessions: dict[str, SandboxSession] = {}
        self._prepare_lock = threading.Lock()

    def prepare_launch(self) -> PreparedSandboxLaunch:
        """Allocate a unique owned identity without crossing the launch boundary."""

        identity = uuid.uuid4()
        prepared = PreparedSandboxLaunch(str(identity), f"maestro-attempt-{identity.hex}.service")
        with self._prepare_lock:
            self._prepared[prepared.launch_id] = prepared
        return prepared

    def _consume_prepared(self, prepared: PreparedSandboxLaunch) -> None:
        valid = False
        try:
            _validate_prepared(prepared)
            with self._prepare_lock:
                if self._prepared.get(prepared.launch_id) == prepared and prepared.launch_id not in self._consumed:
                    self._consumed.add(prepared.launch_id)
                    valid = True
        except Exception:
            pass
        if not valid:
            raise IsolationNotStarted("prepared_launch_invalid_or_consumed")

    def _retire_session(self, session: SandboxSession) -> None:
        """Release launcher-owned buffers after the caller proved exact stop."""

        with self._prepare_lock:
            identity = session._owner_launch_id
            if self._sessions.get(identity) is session:
                self._sessions.pop(identity)
                session._input_bytes = None

    def stop_prepared_launch(
        self, prepared_launch: PreparedSandboxLaunch, launch_receipt: SandboxLaunchReceipt,
    ) -> SandboxTerminationReceipt:
        """Stop an acknowledged binding from the host's validated owned journal.

        A receipt is observational data, not caller authority. The service must
        retrieve it under operation ownership; reconstruction after restart is
        supported without a process-local registry.
        """

        valid = False
        try:
            _validate_launch_receipt(prepared_launch, launch_receipt)
            valid = True
        except Exception:
            pass
        if not valid:
            raise IsolationStartUnknown("prepared_launch_unacknowledged")
        result = None
        try:
            environment = _systemd_client_environment()
            cgroup = _CGROUP_ROOT / launch_receipt.control_group.lstrip("/")
            binding_state = _observe_prepared_binding(prepared_launch, launch_receipt, self._systemctl, environment)
            if binding_state is not None:
                with self._prepare_lock:
                    session = self._sessions.get(prepared_launch.launch_id)
                if session is not None:
                    session.cancel()
                    session.wait()
                    # A live systemd-run can still submit work. Never witness
                    # absence until this owning transport operation has ended.
                    if session._process.poll() is None:
                        raise ValueError("transport remains unresolved")
                elif binding_state == "live":
                    subprocess.run([self._systemctl, "--user", "kill", "--kill-whom=all",
                        "--signal=SIGKILL", prepared_launch.unit_name], stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=environment,
                        check=False, timeout=3)
                result = _read_termination_receipt(unit_name=prepared_launch.unit_name,
                    expected_cgroup=cgroup, client_env=environment, systemctl=self._systemctl)
                if _observe_prepared_binding(prepared_launch, launch_receipt, self._systemctl, environment) != "stopped":
                    result = None
                if result is not None and session is not None:
                    _discard_staging(session._staging)
                    self._retire_session(session)
        except Exception:
            result = None
        if result is None:
            raise IsolationStopUnknown("prepared_launch_stop_unknown")
        return result

    def launch(
        self,
        workspace: str | Path,
        command: Sequence[str],
        *,
        limits: SandboxLimits | None = None,
        input_bytes: bytes | None = None,
        expected_workspace_identity_hash: str | None = None,
        prepared_launch: PreparedSandboxLaunch | None = None,
    ) -> "SandboxSession":
        """Start a command with optional bounded stdin and return a cancellable handle."""

        if prepared_launch is not None:
            self._consume_prepared(prepared_launch)
        if not platform.system().lower() == "linux":
            raise IsolationNotStarted("the systemd isolation backend is Linux-only")
        if not self._systemd_run or not self._systemctl:
            raise IsolationNotStarted("systemd-run and systemctl are required")
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
            raise IsolationNotStarted("systemd user manager probe failed") from exc
        if probe.returncode != 0:
            raise IsolationNotStarted("a usable systemd user manager is required")
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
            raise IsolationNotStarted("sandbox mount targets cannot be staged") from exc
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
            raise IsolationNotStarted("the trusted isolation runtime is unavailable")
        if not _systemd_path_supported(root) or not _systemd_path_supported(runtime_source):
            staging.cleanup()
            raise InvalidSandboxRequest("workspace paths with spaces or systemd separators are unsupported")

        unit_name = prepared_launch.unit_name if prepared_launch is not None else f"maestro-attempt-{uuid.uuid4().hex}.service"
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
        expected_profile = None
        if prepared_launch is not None:
            try:
                expected_profile = _expected_launch_profile(properties, exec_command)
            except Exception:
                staging.cleanup()
            if expected_profile is None:
                raise IsolationNotStarted("prepared_launch_profile_unsupported")
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
            if prepared_launch is None:
                raise IsolationStartUnknown("systemd transient unit could not be started; staging retained") from exc
            process = None
        if process is None:
            raise IsolationStartUnknown("systemd transient unit could not be started; staging retained")
        session = SandboxSession(
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
            cancel_after_transport_exit=prepared_launch is not None,
        )
        if prepared_launch is not None:
            session._owner_ref = weakref.ref(self)
            session._owner_launch_id = prepared_launch.launch_id
            with self._prepare_lock:
                self._sessions[prepared_launch.launch_id] = session
            receipt = _read_launch_receipt(prepared_launch=prepared_launch,
                expected_cgroup=scope_cgroup, expected_profile=expected_profile,
                client_env=client_env, systemctl=self._systemctl)
            if receipt is None:
                raise IsolationStartUnknown("prepared_launch_acknowledgement_unknown")
            session._launch_receipt = receipt
        return session


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
        self._launch_receipt: SandboxLaunchReceipt | None = None
        self._owner_ref: weakref.ReferenceType[SystemdReadOnlyLauncher] | None = None
        self._owner_launch_id: str | None = None

    @property
    def launch_receipt(self) -> SandboxLaunchReceipt | None:
        """Available only after host observation of this exact launch/profile."""

        return self._launch_receipt

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
                    if receipt is not None and self._launch_receipt is not None:
                        prepared = PreparedSandboxLaunch(self._launch_receipt.launch_id, self.unit_name)
                        if _observe_prepared_binding(prepared, self._launch_receipt,
                            self._systemctl, self._client_env) != "stopped":
                            receipt = None
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
                    elif self._result is not None and self._result.termination_confirmed:
                        if self._cleanup_on_termination:
                            _discard_staging(self._staging)
                        owner = self._owner_ref() if self._owner_ref is not None else None
                        if owner is not None:
                            owner._retire_session(self)

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
        raise IsolationNotStarted("the app.slice cgroup cannot be verified") from exc


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


def _validate_prepared(value: PreparedSandboxLaunch) -> None:
    if type(value) is not PreparedSandboxLaunch or type(value.launch_id) is not str or type(value.unit_name) is not str:
        raise ValueError("invalid prepared identity")
    identity = uuid.UUID(value.launch_id)
    if (identity.version != 4 or str(identity) != value.launch_id
        or value.unit_name != f"maestro-attempt-{identity.hex}.service"):
        raise ValueError("invalid prepared binding")


def _validate_launch_receipt(prepared: PreparedSandboxLaunch, receipt: SandboxLaunchReceipt) -> None:
    _validate_prepared(prepared)
    if type(receipt) is not SandboxLaunchReceipt:
        raise ValueError("invalid observed binding")
    if any(type(value) is not str for value in (receipt.launch_id, receipt.unit_name,
        receipt.control_group, receipt.invocation_id, receipt.profile_sha256)):
        raise ValueError("invalid observed binding")
    if (receipt.launch_id != prepared.launch_id or receipt.unit_name != prepared.unit_name
        or not re.fullmatch(r"[0-9a-f]{32}", receipt.invocation_id)
        or receipt.invocation_id == "0" * 32
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", receipt.profile_sha256)):
        raise ValueError("invalid observed binding")
    # Apply the same exact path constraints as termination receipts, without
    # constructing a termination receipt from a launch acknowledgement.
    path = Path(receipt.control_group)
    if (not path.is_absolute() or path.as_posix() != receipt.control_group
        or path.name != prepared.unit_name or path.parent.name != "app.slice"
        or any(part in {".", ".."} for part in path.parts)):
        raise ValueError("invalid observed cgroup")


_DURATION_PROPERTIES = {"CPUQuotaPerSecUSec", "CPUQuotaPeriodUSec", "RuntimeMaxUSec", "TimeoutStopUSec"}
_LIMIT_PROPERTIES = {"MemoryMax", "MemorySwapMax", "TasksMax", "LimitNOFILE", "LimitNOFILESoft",
    "LimitFSIZE", "LimitFSIZESoft", "LimitCORE", "LimitCORESoft"}
_BOOLEAN_PROPERTIES = {"PrivateNetwork", "PrivateTmp", "PrivateUsers", "PrivateDevices",
    "ProtectControlGroups", "ProtectKernelTunables", "ProtectKernelModules", "ProtectKernelLogs",
    "ProtectClock", "ProtectHostname", "NoNewPrivileges", "RestrictRealtime", "RestrictSUIDSGID",
    "LockPersonality", "MemoryDenyWriteExecute"}
_TEXT_PROPERTIES = {"ProtectHome", "ProtectSystem", "ProtectProc", "ProcSubset",
    "CapabilityBoundingSet", "RestrictAddressFamilies", "WorkingDirectory", "KillMode", "Environment", "ExecStart"}
_PROFILE_PROPERTIES = _DURATION_PROPERTIES | _LIMIT_PROPERTIES | _BOOLEAN_PROPERTIES | _TEXT_PROPERTIES | {
    "BindReadOnlyPaths", "InaccessiblePaths"}


def _duration_usec(value: str) -> str:
    # systemctl prints normalized human-readable timespans rather than the
    # numeric microsecond D-Bus value. Convert finite fractions with integer
    # arithmetic: never round an observed limit into the requested profile.
    factors = {"us": 1, "ms": 1000, "s": 1_000_000, "min": 60_000_000,
        "h": 3_600_000_000, "d": 86_400_000_000}
    if type(value) is not str or len(value) > 128:
        raise ValueError("invalid duration")
    chunks = value.split()
    if not chunks:
        raise ValueError("missing duration")
    total = 0
    for chunk in chunks:
        match = re.fullmatch(r"([0-9]+)(?:\.([0-9]{1,6}))?(us|ms|s|min|h|d)", chunk)
        if match is None:
            raise ValueError("unsupported duration")
        fraction = match[2] or ""
        denominator = 10 ** len(fraction)
        numerator = int(match[1]) * denominator + int(fraction or "0")
        microseconds, remainder = divmod(numerator * factors[match[3]], denominator)
        total += microseconds
        # Every requested profile duration is bounded by SandboxLimits' day
        # maximum. Infinity/overflow and sub-microsecond values fail closed.
        if remainder or total > 86_400_000_000:
            raise ValueError("duration outside profile bounds")
    return str(total)


def _normalise_launch_profile(values: dict[str, str]) -> dict[str, object]:
    if set(values) != _PROFILE_PROPERTIES:
        raise ValueError("missing profile properties")
    result: dict[str, object] = {}
    for key, value in values.items():
        if key in _DURATION_PROPERTIES:
            result[key] = _duration_usec(value)
        elif key in _LIMIT_PROPERTIES:
            if not re.fullmatch(r"[0-9]+", value):
                raise ValueError("unbounded resource")
            result[key] = str(int(value))
        elif key in _BOOLEAN_PROPERTIES:
            if value not in {"yes", "no"}:
                raise ValueError("unsupported boolean")
            result[key] = value
        elif key in {"BindReadOnlyPaths", "InaccessiblePaths", "RestrictAddressFamilies"}:
            items = []
            for item in value.split():
                if key == "BindReadOnlyPaths":
                    parts = item.split(":")
                    if len(parts) == 2:
                        parts.append("rbind")
                    if len(parts) != 3 or parts[2] != "rbind":
                        raise ValueError("unsupported bind flags")
                    if not all(part.startswith("/") for part in parts[:2]):
                        raise ValueError("invalid bind")
                    item = ":".join(parts)
                elif key == "InaccessiblePaths":
                    item = item.removeprefix("-")
                    if not item.startswith("/"):
                        raise ValueError("invalid hidden path")
                items.append(item)
            if len(items) != len(set(items)):
                raise ValueError("duplicate profile member")
            result[key] = sorted(items)
        elif key == "ExecStart":
            match = re.fullmatch(r"\{ path=([^;]+) ; argv\[\]=(.*?) ; ignore_errors=no ; .*\}", value)
            if match is None:
                raise ValueError("unsupported exec observation")
            result[key] = [match[1], match[2]]
        else:
            result[key] = value
    return result


def _profile_sha256(profile: dict[str, object]) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(profile, sort_keys=True,
        separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _expected_launch_profile(properties: tuple[str, ...], command: list[str]) -> dict[str, object]:
    if any(not token.isascii() or any(c.isspace() or c in ";{}\\\"'" for c in token) for token in command):
        raise ValueError("ambiguous exec arguments")
    values = {}
    repeated = {"BindReadOnlyPaths": [], "InaccessiblePaths": []}
    renamed = {"RuntimeMaxSec": "RuntimeMaxUSec", "TimeoutStopSec": "TimeoutStopUSec",
        "CPUQuotaPeriodSec": "CPUQuotaPeriodUSec"}
    for property_value in properties:
        key, value = property_value.split("=", 1)
        if key in repeated:
            repeated[key].append(value)
            continue
        if key == "CPUQuota":
            key, value = "CPUQuotaPerSecUSec", str(int(value.removesuffix("%")) * 10_000) + "us"
        key = renamed.get(key, key)
        if key in values:
            raise ValueError("duplicate expected property")
        values[key] = value
        if key in {"LimitNOFILE", "LimitFSIZE", "LimitCORE"}:
            values[key + "Soft"] = value
    values.update({key: " ".join(items) for key, items in repeated.items()})
    values["Environment"] = ""
    values["ExecStart"] = "{ path=" + command[0] + " ; argv[]=" + " ".join(command) + " ; ignore_errors=no ; pending=yes }"
    return _normalise_launch_profile(values)


def _query_launch_properties(unit_name: str, systemctl: str, environment: dict[str, str], timeout: float) -> dict | None:
    identity = {"Id", "LoadState", "ActiveState", "ControlGroup", "InvocationID"}
    required = _PROFILE_PROPERTIES | identity
    try:
        result = subprocess.run([systemctl, "--user", "show", unit_name,
            *(f"--property={name}" for name in sorted(required))], stdin=subprocess.DEVNULL,
            capture_output=True, env=environment, check=False, timeout=timeout)
        if result.returncode == 0:
            values = {}
            for line in result.stdout.decode("ascii", errors="strict").splitlines():
                if "=" not in line:
                    return None
                name, value = line.split("=", 1)
                if name not in required or name in values:
                    return None
                values[name] = value
            # Collected services omit service-only properties such as
            # ExecStart. Only authoritative identity/state is meaningful then;
            # loaded units still require the entire effective profile.
            if identity <= values.keys() and (values["LoadState"] == "not-found" or values.keys() == required):
                return values
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        pass
    return None


def _read_launch_receipt(*, prepared_launch: PreparedSandboxLaunch, expected_cgroup: Path,
    expected_profile: dict[str, object], client_env: dict[str, str],
    timeout_seconds: float = 2.0, systemctl: str = "systemctl") -> SandboxLaunchReceipt | None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            _validate_prepared(prepared_launch)
            control_group = "/" + expected_cgroup.relative_to(_CGROUP_ROOT).as_posix()
            values = _query_launch_properties(prepared_launch.unit_name, systemctl, client_env,
                min(3, max(0.001, deadline - time.monotonic())))
            if values is not None and values["LoadState"] == "loaded":
                if values["Id"] != prepared_launch.unit_name:
                    return None
                pending = (values["ActiveState"] in {"inactive", "activating"}
                    and not values["ControlGroup"] and not values["InvocationID"])
                if not pending:
                    if (values["ControlGroup"] != control_group
                        or values["ActiveState"] not in {"active", "activating"}):
                        return None
                    profile = _normalise_launch_profile({key: values[key] for key in _PROFILE_PROPERTIES})
                    if profile != expected_profile:
                        return None
                    receipt = SandboxLaunchReceipt(prepared_launch.launch_id, prepared_launch.unit_name,
                        control_group, values["InvocationID"], _profile_sha256(profile))
                    _validate_launch_receipt(prepared_launch, receipt)
                    return receipt
        except Exception:
            return None
        if time.monotonic() >= deadline:
            return None
        time.sleep(min(0.02, max(0, deadline - time.monotonic())))


def _observe_prepared_binding(prepared: PreparedSandboxLaunch, receipt: SandboxLaunchReceipt,
    systemctl: str, environment: dict[str, str]) -> str | None:
    values = _query_launch_properties(prepared.unit_name, systemctl, environment, 3)
    if values is None or values["Id"] != prepared.unit_name:
        return None
    if values["LoadState"] == "not-found":
        return "stopped" if (values["ActiveState"] in {"inactive", "failed"}
            and not values["ControlGroup"] and not values["InvocationID"]) else None
    if (values["LoadState"] != "loaded" or values["ControlGroup"] != receipt.control_group
        or values["InvocationID"] != receipt.invocation_id):
        return None
    profile = _normalise_launch_profile({key: values[key] for key in _PROFILE_PROPERTIES})
    if _profile_sha256(profile) != receipt.profile_sha256:
        return None
    if values["ActiveState"] in {"inactive", "failed"}:
        return "stopped"
    return "live" if values["ActiveState"] in {"active", "activating", "deactivating"} else None


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
    raise IsolationNotStarted("no safe staging directory exists outside the workspace")


def _systemd_client_environment() -> dict[str, str]:
    env: dict[str, str] = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    for name in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "SYSTEMD_BUS_ADDRESS"):
        value = os.environ.get(name)
        if value:
            env[name] = value
    if "XDG_RUNTIME_DIR" not in env:
        raise IsolationNotStarted("XDG_RUNTIME_DIR is required for the systemd user manager")
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
        for name in (".git", ".maestro", ".env")
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
    "IsolationNotStarted",
    "IsolationStartUnknown",
    "IsolationStopUnknown",
    "PreparedSandboxLaunch",
    "SandboxLaunchReceipt",
    "SandboxLimits",
    "SandboxResult",
    "SandboxSession",
    "SystemdReadOnlyLauncher",
]
