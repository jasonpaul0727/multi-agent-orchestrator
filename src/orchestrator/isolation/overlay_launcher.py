"""Private candidate execution; the live workspace is never a writable mount."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import threading
import time
import uuid
from typing import Sequence

from .launcher import (
    InvalidSandboxRequest, IsolationUnavailable, SandboxLimits, SandboxResult, SandboxSession,
    _create_staging, _expand_runtime_paths, _systemd_client_environment,
    _systemd_path_supported, _validate_command, _validated_workspace,
)
from .workspace import (
    WorkspaceBoundaryError, WorkspaceDiff, WorkspaceDiffEntry,
    snapshot_workspace, validate_overlay_candidate,
)


_MAX_COMPLETION_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class OverlayCandidateResult:
    execution: SandboxResult
    lower_root: Path
    diff: WorkspaceDiff | None
    candidate_error: str | None = None


class _RetainedStaging:
    def __init__(self, temporary) -> None:
        self.temporary = temporary
        # A host crash/GC must not recursively remove files underneath a
        # potentially running scope. The session alone owns explicit cleanup.
        temporary._finalizer.detach()

    def cleanup(self) -> None:
        pass  # SandboxSession may finish transport before candidate consumption.

    def discard(self) -> None:
        self.temporary.cleanup()


class SystemdOverlayCandidateLauncher:
    """Internal Linux backend producing bounded additions/updates, not publication."""

    def __init__(
        self, *, candidate_bytes: int = 16 * 1024 * 1024,
        candidate_entries: int = 10_000, scratch_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        for name, value, minimum, maximum in (
            ("candidate_bytes", candidate_bytes, 1, 64 * 1024 * 1024),
            ("candidate_entries", candidate_entries, 1, 10_000),
            ("scratch_bytes", scratch_bytes, 1, 64 * 1024 * 1024),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
                raise InvalidSandboxRequest(f"{name} is outside the supported bound")
        self._candidate_bytes = candidate_bytes
        self._candidate_entries = candidate_entries
        self._scratch_bytes = scratch_bytes

    def launch(
        self, workspace: str | Path, command: Sequence[str], *, limits: SandboxLimits | None = None,
        input_bytes: bytes | None = None, expected_workspace_identity_hash: str | None = None,
    ) -> "OverlayCandidateSession":
        if platform.system().lower() != "linux":
            raise IsolationUnavailable("candidate backend is Linux-only")
        for binary in ("systemd-run", "systemctl", "unshare", "mount", "umount"):
            if shutil.which(binary) is None:
                raise IsolationUnavailable("candidate backend requires systemd and namespace tools")
        _validate_command(command)
        if input_bytes is not None and (not isinstance(input_bytes, bytes) or len(input_bytes) > 1_048_576):
            raise InvalidSandboxRequest("stdin payload must be at most 1 MiB of bytes")
        limits = SandboxLimits() if limits is None else limits
        if not isinstance(limits, SandboxLimits):
            raise InvalidSandboxRequest("limits must be a validated SandboxLimits")
        root = _validated_workspace(workspace)
        environment = _systemd_client_environment()
        try:
            manager = subprocess.run(
                ["systemctl", "--user", "show-environment"], env=environment,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise IsolationUnavailable("candidate systemd manager probe failed") from exc
        if manager.returncode != 0:
            raise IsolationUnavailable("candidate backend requires a usable systemd user manager")
        cgroup_parent = _candidate_cgroup_parent(environment)
        runtime = Path(__file__).resolve().parents[2]
        if not runtime.is_dir() or not _systemd_path_supported(runtime):
            raise IsolationUnavailable("candidate runtime path is unavailable or unsupported")
        expanded = _expand_runtime_paths(command, runtime, Path("/runtime"))
        temporary = _create_staging(root)
        stage = Path(temporary.name)
        retained = None
        try:
            snapshot_workspace(
                root, stage / "lower", max_entries=100_000, max_bytes=256 * 1024 * 1024,
                max_depth=64, expected_identity_hash=expected_workspace_identity_hash,
            )
            _enable_overlay_owner_writes(stage / "lower")
            unit = f"maestro-candidate-{uuid.uuid4().hex}.scope"
            scope_cgroup = cgroup_parent / unit
            config = {
                "runtime_source": str(runtime), "command": expanded,
                "expected_cgroup": "/" + str(scope_cgroup.relative_to("/sys/fs/cgroup")),
                "candidate_bytes": self._candidate_bytes,
                "candidate_entries": self._candidate_entries, "scratch_bytes": self._scratch_bytes,
            }
            with (stage / "config.json").open("x", encoding="utf-8") as stream:
                os.chmod(stage / "config.json", 0o600)
                json.dump(config, stream, separators=(",", ":"))
            env_args = ["PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8",
                        f"PYTHONPATH={runtime}", f"MAESTRO_EXPECT_MEMORY={limits.memory_bytes}",
                        f"MAESTRO_EXPECT_TASKS={limits.tasks}", f"MAESTRO_EXPECT_CPU={limits.cpu_percent}",
                        f"MAESTRO_EXPECT_NOFILE={limits.nofile}", f"MAESTRO_EXPECT_FSIZE={limits.file_bytes}"]
            args = [
                "systemd-run", "--user", "--scope", "--slice=app.slice", "--quiet", "--collect", f"--unit={unit}",
                f"--property=MemoryMax={limits.memory_bytes}", "--property=MemorySwapMax=0",
                f"--property=TasksMax={limits.tasks}", f"--property=CPUQuota={limits.cpu_percent}%",
                "--property=CPUQuotaPeriodSec=100ms", f"--property=RuntimeMaxSec={limits.timeout_seconds}s",
                "--property=TimeoutStopSec=1s",
                "--", "/usr/bin/env", "-i", *env_args,
                "/usr/bin/unshare", "--user", "--map-root-user", "--mount", "--net",
                "--pid", "--fork", "--mount-proc=/proc", "/usr/bin/python3", "-P", "-S", "-m",
                "orchestrator.isolation._overlay_bootstrap", str(stage),
            ]
            retained = _RetainedStaging(temporary)
            process = subprocess.Popen(
                args, stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment, bufsize=0, close_fds=True,
            )
        except WorkspaceBoundaryError as exc:
            temporary.cleanup()
            raise InvalidSandboxRequest("candidate snapshot failed closed") from exc
        except BaseException as exc:
            if retained is None:
                temporary.cleanup()
                raise
            raise IsolationUnavailable(
                f"candidate start failed; stop unconfirmed; private staging retained at {stage}; unit {unit}"
            ) from exc
        transport = SandboxSession(
            process=process, unit_name=unit, systemctl="systemctl", client_env=environment,
            output_limit=limits.output_bytes, timeout_seconds=limits.timeout_seconds,
            staging=retained, input_bytes=input_bytes,
            cancel_after_transport_exit=True, stop_grace_seconds=5,
            scope_cgroup=scope_cgroup,
            cleanup_on_termination=False,
        )
        return OverlayCandidateSession(transport, retained, stage, environment,
                                       self._candidate_entries, self._candidate_bytes,
                                       scope_cgroup=scope_cgroup)


def _enable_overlay_owner_writes(root: Path) -> None:
    # Only this private host snapshot is changed. Its bind is remounted read-only
    # before OverlayFS, so writable owner modes enable copy-up without exposing
    # a writable lower filesystem to the command.
    for directory, _, files in os.walk(root, followlinks=False):
        Path(directory).chmod(0o755)
        for name in files:
            path = Path(directory) / name
            info = path.lstat()
            if stat.S_ISREG(info.st_mode):
                path.chmod(stat.S_IMODE(info.st_mode) | 0o200, follow_symlinks=False)


class OverlayCandidateSession:
    def __init__(
        self, transport: SandboxSession, staging: _RetainedStaging, root: Path,
        environment: dict[str, str], max_entries: int, max_bytes: int, *, scope_cgroup: Path,
    ) -> None:
        self._transport = transport
        self._staging = staging
        self._root = root
        self._environment = environment
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._scope_cgroup = scope_cgroup
        self._lock = threading.RLock()
        self._result: OverlayCandidateResult | None = None
        self._closed = False
        self.unit_name = transport.unit_name

    def cancel(self) -> bool:
        return self._transport.cancel()

    def wait(self) -> OverlayCandidateResult:
        with self._lock:
            if self._closed:
                raise RuntimeError("candidate session is closed")
            if self._result is not None:
                return self._result
            execution = self._transport.wait()
            stopped = execution.termination_confirmed and _scope_stopped(self.unit_name, self._environment, self._scope_cgroup)
            if not stopped:
                execution = replace(execution, termination_receipt=None)
            diff = None
            error = "execution_failed"
            if not stopped:
                error = "termination_unconfirmed"
            elif (execution.returncode == 0 and not execution.cancelled and not execution.timed_out
                  and not execution.output_limited and execution.input_written):
                try:
                    diff = _read_candidate(self._root, self._max_entries, self._max_bytes)
                    error = None
                except (OSError, ValueError, TypeError, KeyError, RecursionError, WorkspaceBoundaryError):
                    error = "candidate_invalid"
            self._result = OverlayCandidateResult(execution, self._root / "lower", diff, error)
            return self._result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._result is None:
                self.cancel()
                self.wait()
            if (not self._result.execution.termination_confirmed
                    and not _scope_stopped(self.unit_name, self._environment, self._scope_cgroup)):
                raise IsolationUnavailable("candidate stop is unconfirmed; private staging retained")
            self._staging.discard()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def _candidate_cgroup_parent(environment: dict[str, str]) -> Path:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", "app.slice", "--property=ControlGroup", "--value"],
            stdin=subprocess.DEVNULL, capture_output=True, env=environment, timeout=3, check=False,
        )
        name = result.stdout.decode("ascii").strip()
        relative = name.removeprefix("/").split("/")
        if (result.returncode != 0 or not name.startswith("/") or name == "/"
                or any(part in {"", ".", ".."} for part in relative)
                or relative[-1] != "app.slice"):
            raise ValueError("invalid candidate cgroup parent")
        parent = Path("/sys/fs/cgroup").joinpath(*relative)
        if not parent.is_dir():
            raise ValueError("candidate cgroup parent is absent")
        return parent
    except (OSError, subprocess.TimeoutExpired, UnicodeError, ValueError) as exc:
        raise IsolationUnavailable("candidate cgroup parent cannot be proven") from exc


def _cgroup_is_empty(group: Path) -> bool:
    try:
        descriptor = os.open(group, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return True  # The kernel cannot remove a populated cgroup.
    except OSError:
        return False
    try:
        events = os.open("cgroup.events", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                         dir_fd=descriptor)
        with os.fdopen(events, "rb") as stream:
            content = stream.read(4097)
        if len(content) > 4096:
            return False
        values = content.decode("ascii").splitlines()
        if sum(line.startswith("populated ") for line in values) != 1:
            return False
        return "populated 0" in values
    except (OSError, UnicodeError):
        return False
    finally:
        os.close(descriptor)


def _scope_stopped(unit: str, environment: dict[str, str], group: Path) -> bool:
    deadline = time.monotonic() + 2
    while True:
        try:
            result = subprocess.run(
                ["systemctl", "--user", "show", unit, "--property=ActiveState", "--property=LoadState"],
                stdin=subprocess.DEVNULL, capture_output=True, env=environment, timeout=3, check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        values = dict(line.split("=", 1) for line in result.stdout.decode("ascii", errors="replace").splitlines()
                      if "=" in line)
        if (values.get("ActiveState") in {"inactive", "failed"}
                and values.get("LoadState") in {"loaded", "not-found"}):
            return _cgroup_is_empty(group)
        if result.returncode != 0 or time.monotonic() >= deadline:
            return False
        time.sleep(0.05)


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate completion key")
        result[key] = value
    return result


def _read_candidate(root: Path, max_entries: int, max_bytes: int) -> WorkspaceDiff:
    descriptor = os.open(root / "completion.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > _MAX_COMPLETION_BYTES):
            raise ValueError("completion is not a bounded private record")
        payload = json.loads(stream.read(_MAX_COMPLETION_BYTES + 1), object_pairs_hook=_unique_pairs,
                             parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite")))
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "entries", "total_bytes", "manifest_hash"}:
        raise ValueError("invalid completion fields")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise ValueError("unsupported completion version")
    if not isinstance(payload["entries"], list) or len(payload["entries"]) > max_entries:
        raise ValueError("invalid completion entries")
    diff = WorkspaceDiff(
        candidate_root=root / "candidate",
        entries=tuple(WorkspaceDiffEntry(**entry) for entry in payload["entries"]),
        total_bytes=payload["total_bytes"], manifest_hash=payload["manifest_hash"],
    )
    validate_overlay_candidate(root / "lower", diff, max_entries=max_entries, max_bytes=max_bytes)
    return diff
