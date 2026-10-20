"""Host-only, measured admission for the supported workspace execution profile."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import tempfile
from typing import Literal

from orchestrator.workspace_identity import workspace_identity_hash

from .launcher import IsolationNotStarted, IsolationUnavailable, SandboxLimits, _validated_workspace

_HASH = re.compile(r"sha256:[0-9a-f]{64}")
_CONTROLS = (
    "user-mount-net-pid-namespaces", "private-overlay-final-view",
    "cgroup-v2-memory-swap-tasks-cpu", "rlimit-nofile-fsize-core",
    "landlock", "native-seccomp-default-deny", "no-new-privileges",
    "empty-capabilities", "private-runtime-home-proc", "verified-cgroup-stop",
)
# Fixed trusted code, never request content. Successful exec already requires
# the real bootstrap's cgroup/rlimit, mount, privilege, Landlock and seccomp checks.
_PROBE = """import ctypes, errno, os, resource, socket
from pathlib import Path
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(39, 0, 0, 0, 0) == 1
for family in (socket.AF_INET, socket.AF_UNIX):
    try:
        socket.socket(family, socket.SOCK_STREAM)
    except OSError as exc:
        assert exc.errno == errno.EPERM
    else:
        raise AssertionError('raw sockets available')
assert not Path('/proc/1/root').exists()
assert not Path('/etc/shadow').exists()
try:
    Path('/run/user').stat()
except OSError as exc:
    assert exc.errno in (errno.ENOENT, errno.EACCES, errno.EPERM)
else:
    try:
        list(Path('/run/user').iterdir())
    except OSError as exc:
        assert exc.errno in (errno.EACCES, errno.EPERM)
    else:
        raise AssertionError('host runtime visible')
assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
assert libc.mount(b'none', b'/tmp', b'tmpfs', 0, None) == -1
assert ctypes.get_errno() == errno.EPERM
p = Path('/workspace/admission-probe')
p.write_bytes(b'p')
p.rename('/workspace/admission-renamed')
Path('/workspace/admission-renamed').unlink()
"""


@dataclass(frozen=True)
class WorkspaceIsolationAdmission:
    outcome: Literal["eligible", "blocked"]
    reason_code: Literal["isolation_unavailable"] | None
    profile_hash: str | None
    workspace_identity_hash: str
    observed_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.workspace_identity_hash, str) or not _HASH.fullmatch(self.workspace_identity_hash):
            raise ValueError("workspace identity must be a digest")
        if not isinstance(self.observed_at, datetime) or self.observed_at.utcoffset() is None:
            raise ValueError("observation must be timezone-aware")
        if self.outcome == "eligible":
            if self.reason_code is not None or not isinstance(self.profile_hash, str) or not _HASH.fullmatch(self.profile_hash):
                raise ValueError("eligible admission requires a verified profile")
        elif self.outcome != "blocked" or self.reason_code != "isolation_unavailable" or self.profile_hash is not None:
            raise ValueError("blocked admission must not carry a verified profile")


def _digest(value) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _runtime_root() -> Path:
    from . import overlay_launcher
    return Path(overlay_launcher.__file__).resolve().parents[2]


def _trusted_path(path: Path, *, private: bool = False) -> os.stat_result:
    info = path.lstat()
    if (stat.S_ISLNK(info.st_mode) or info.st_uid not in {0, os.getuid()}
            or info.st_mode & 0o022):
        raise IsolationNotStarted("runtime/control path ownership is unsafe")
    if private and (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o700):
        raise IsolationNotStarted("private root is unsafe")
    for parent in path.parents:
        parent_info = parent.stat()
        if parent_info.st_uid not in {0, os.getuid()}:
            raise IsolationNotStarted("runtime/control parent ownership is unsafe")
        if parent_info.st_mode & 0o022 and not (
            parent_info.st_uid == 0 and parent_info.st_mode & stat.S_ISVTX
        ):
            raise IsolationNotStarted("runtime/control parent is writable")
    return info


def _observe_profile(limits: SandboxLimits, *, candidate_bounds=None) -> dict:
    from .overlay_launcher import _candidate_scope_properties
    if platform.system() != "Linux" or platform.machine().lower() != "x86_64":
        raise IsolationNotStarted("only the measured Linux x86_64 profile is eligible")
    if not isinstance(limits, SandboxLimits):
        raise IsolationNotStarted("limits are not validated")
    binaries = {}
    for name in ("systemd-run", "systemctl", "unshare", "mount", "umount", "python3"):
        executable = shutil.which(name, path="/usr/bin:/bin")
        if executable is None:
            raise IsolationNotStarted("required isolation binary is missing")
        path = Path(executable).resolve(strict=True)
        binaries[name] = _native_identity(path)
    for name in ("libc.so.6", "libseccomp.so.2"):
        path = (Path("/usr/lib/x86_64-linux-gnu") / name).resolve(strict=True)
        binaries[name] = _native_identity(path)
    private_root = Path(os.environ.get("XDG_RUNTIME_DIR", ""))
    if not private_root.is_absolute():
        raise IsolationNotStarted("private runtime root is unavailable")
    info = _trusted_path(private_root, private=True)
    for name in ("DBUS_SESSION_BUS_ADDRESS", "SYSTEMD_BUS_ADDRESS"):
        if os.environ.get(name) not in (None, "", f"unix:path={private_root}/bus"):
            raise IsolationNotStarted("unverified systemd manager address")
    runtime = _runtime_root()
    if not runtime.is_dir():
        raise IsolationNotStarted("trusted runtime is unavailable")
    _trusted_path(runtime)
    sources = {}
    for path in sorted((runtime / "orchestrator").rglob("*")):
        entry = _trusted_path(path)
        if stat.S_ISREG(entry.st_mode):
            sources[str(path.relative_to(runtime))] = hashlib.sha256(path.read_bytes()).hexdigest()
        elif not stat.S_ISDIR(entry.st_mode) and not stat.S_ISREG(entry.st_mode):
            raise IsolationNotStarted("runtime contains an unsafe entry")
    if "orchestrator/isolation/_overlay_bootstrap.py" not in sources:
        raise IsolationNotStarted("trusted bootstrap is missing")
    return {
        "schema": 1, "backend": "systemd-overlay", "native": platform.machine().lower(),
        "kernel": platform.release(), "controls": _CONTROLS, "limits": asdict(limits),
        "scope_properties": _candidate_scope_properties(limits),
        "candidate_bounds": candidate_bounds or (16*1024*1024, 10_000, 8*1024*1024),
        "binaries": binaries, "runtime": [str(runtime), sources],
        "private_root": [str(private_root), info.st_dev, info.st_ino],
        "probe": hashlib.sha256(_PROBE.encode()).hexdigest(),
    }


def _native_identity(path: Path) -> list:
    info = _trusted_path(path)
    if not stat.S_ISREG(info.st_mode) or info.st_size > 64*1024*1024:
        raise IsolationNotStarted("native runtime is not a bounded regular file")
    data = path.read_bytes()
    if (len(data) < 64 or data[:6] != b"\x7fELF\x02\x01"
            or data[18:20] != b"\x3e\x00"):
        raise IsolationNotStarted("native runtime architecture is incompatible")
    return [str(path), info.st_dev, info.st_ino, hashlib.sha256(data).hexdigest()]


def _run_probe(limits: SandboxLimits, *, candidate_bounds=None) -> bool:
    from .overlay_launcher import SystemdOverlayCandidateLauncher
    options = {} if candidate_bounds is None else dict(zip(
        ("candidate_bytes", "candidate_entries", "scratch_bytes"), candidate_bounds
    ))
    # A private empty source excludes project input from the trusted probe.
    # Its lifetime also remains retained if a start/stop cannot be proved.
    temporary = tempfile.TemporaryDirectory(prefix="maestro-admission-")
    temporary._finalizer.detach()
    session = None
    stopped = False
    try:
        session = SystemdOverlayCandidateLauncher(**options)._launch(
            Path(temporary.name), ("/usr/bin/python3", "-I", "-S", "-c", _PROBE), limits=limits,
        )
        result = session.wait()
        stopped = result.execution.termination_confirmed
        return (stopped and result.execution.returncode == 0 and result.diff is not None
                and not result.diff.entries and not result.execution.timed_out
                and not result.execution.output_limited and not result.execution.cancelled)
    except IsolationNotStarted:
        stopped = session is None
        return False
    except Exception:
        return False
    finally:
        if session is not None and stopped:
            session.close()
        if stopped:
            temporary.cleanup()


def verify_workspace_isolation(workspace: str | Path, *, limits: SandboxLimits) -> WorkspaceIsolationAdmission:
    return _verify_workspace_isolation(workspace, limits=limits)


def _verify_workspace_isolation(workspace, *, limits, candidate_bounds=None):
    identity = workspace_identity_hash(workspace)
    profile = None
    try:
        root = _validated_workspace(workspace)
        runtime = _runtime_root()
        if runtime.is_relative_to(root) or root.is_relative_to(runtime):
            raise IsolationNotStarted("workspace and trusted runtime must be disjoint")
        before = _observe_profile(limits, candidate_bounds=candidate_bounds)
        if (_run_probe(limits, candidate_bounds=candidate_bounds)
                and before == _observe_profile(limits, candidate_bounds=candidate_bounds)
                and identity == workspace_identity_hash(workspace)):
            profile = _digest(before)
    except (IsolationUnavailable, OSError, ValueError, RuntimeError):
        pass
    return WorkspaceIsolationAdmission(
        "eligible" if profile else "blocked", None if profile else "isolation_unavailable",
        profile, identity, datetime.now(timezone.utc),
    )


def revalidate_workspace_isolation(workspace, *, limits, admission, candidate_bounds=None) -> None:
    """Recheck deterministic evidence at the last host pre-start boundary."""
    try:
        if (admission.outcome != "eligible" or admission.workspace_identity_hash != workspace_identity_hash(workspace)
                or admission.profile_hash != _digest(_observe_profile(limits, candidate_bounds=candidate_bounds))):
            raise IsolationNotStarted("workspace isolation evidence changed before launch")
    except (OSError, ValueError, RuntimeError) as exc:
        raise IsolationNotStarted("workspace isolation evidence changed before launch") from exc
