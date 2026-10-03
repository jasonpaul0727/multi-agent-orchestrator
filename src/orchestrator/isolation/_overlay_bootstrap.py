"""Trusted namespace init: prepare OverlayFS, supervise, export after stop.

This runs as PID 1 in a new PID namespace. Its host-side paths and completion
record are never visible in the requested command's private filesystem root.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import resource
import signal
import stat
import subprocess
import sys

from ._exec import _verify_limits
from .candidate_security import drop_candidate_privileges
from .workspace import WorkspaceBoundaryError, export_overlay_diff, validate_overlay_candidate


class _MountSetupError(RuntimeError):
    pass


def _mount(*arguments: str) -> None:
    process = subprocess.run(
        ["/usr/bin/mount", *arguments], stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False, timeout=5,
    )
    if process.returncode != 0:
        raise _MountSetupError(f"operation={arguments[0]}, target={Path(arguments[-1]).name}")


def _bind_readonly(source: Path, target: Path) -> None:
    _mount("--bind", str(source), str(target))
    _mount("-o", "remount,bind,ro,nosuid,nodev", str(target))


def _runtime_directories() -> tuple[Path, ...]:
    architecture = {
        "x86_64": "x86_64-linux-gnu", "amd64": "x86_64-linux-gnu",
        "aarch64": "aarch64-linux-gnu", "arm64": "aarch64-linux-gnu",
    }.get(platform.machine().lower())
    if architecture is None:
        raise RuntimeError("unsupported candidate runtime architecture")
    # Bind individual runtime trees: a whole /usr bind on WSL contains locked
    # driver/module submounts, and a recursive bind would expose those host
    # mounts. This initial runtime supports system Python and native libraries.
    paths = (
        Path("/usr/bin"), Path("/usr/sbin"), Path("/usr/lib64"),
        Path("/usr/lib") / architecture,
        Path(f"/usr/lib/python{sys.version_info.major}.{sys.version_info.minor}"),
        Path("/usr/share/locale"), Path("/usr/share/zoneinfo"),
    )
    return tuple(path for path in paths if path.is_dir())


def _prepare_root(root: Path, config: dict) -> Path:
    _mount("--make-rprivate", "/")
    buffer = root / "buffer"
    buffer.mkdir(mode=0o700)
    _mount("-t", "tmpfs", "-o", f"size={config['candidate_bytes']},mode=0700,nosuid,nodev",
           "tmpfs", str(buffer))
    for name in ("upper", "work"):
        (buffer / name).mkdir(mode=0o700)
    view = root / "rootfs"
    view.mkdir(mode=0o700)
    _mount("--bind", str(view), str(view))
    for name in ("usr", "runtime", "workspace", "tmp", "dev", "etc", "proc", "run", "home"):
        (view / name).mkdir(mode=0o755)
    # This is an empty private mount, never a bind of the host runtime. A
    # failed barrier aborts setup before any untrusted command can execute.
    (view / "run/user").mkdir(mode=0o700)
    _mount("-t", "tmpfs", "-o", "size=4096,ro,mode=000,nosuid,nodev,noexec",
           "tmpfs", str(view / "run/user"))
    _bind_readonly(root / "lower", root / "lower")
    _mount("-t", "overlay", "overlay", "-o",
           f"lowerdir={root / 'lower'},upperdir={buffer / 'upper'},workdir={buffer / 'work'},"
           "nosuid,nodev,userxattr,index=off,metacopy=off,redirect_dir=nofollow",
           str(view / "workspace"))
    for source in _runtime_directories():
        target = view / source.relative_to("/")
        target.mkdir(parents=True, exist_ok=True)
        _bind_readonly(source, target)
    _bind_readonly(Path(config["runtime_source"]), view / "runtime")
    for name in ("bin", "sbin", "lib", "lib64"):
        if Path("/" + name).exists():
            target = Path("/" + name).resolve(strict=True)
            target.relative_to("/usr")
            (view / name).symlink_to(str(target))
    _mount("-t", "tmpfs", "-o", f"size={config['scratch_bytes']},mode=0700,nosuid,nodev",
           "tmpfs", str(view / "tmp"))
    for name in ("null", "zero", "random", "urandom"):
        target = view / "dev" / name
        target.touch(mode=0o600)
        _mount("--bind", "/dev/" + name, str(target))
    _mount("-o", "remount,bind,ro,nosuid,nodev", str(view))
    return view


def _enter_command_root(view: Path) -> None:
    os.chroot(view)
    os.chdir("/workspace")
    drop_candidate_privileges()


def _verify_scope_identity(expected: str, *, membership_file: Path = Path("/proc/self/cgroup")) -> None:
    if membership_file.read_text(encoding="ascii").splitlines() != [f"0::{expected}"]:
        raise RuntimeError("candidate cgroup identity does not match host stop proof")


def _terminate_descendants() -> None:
    # kill(-1) is deliberately confined to this new PID namespace, where we
    # are its init. Never use this operation from the host or a non-init PID.
    if os.getpid() != 1:
        raise RuntimeError("candidate supervisor is not namespace init")
    try:
        os.kill(-1, signal.SIGKILL)
    except ProcessLookupError:
        pass
    while True:
        try:
            os.waitpid(-1, 0)
        except InterruptedError:
            continue
        except ChildProcessError:
            return


def _strip_kernel_metadata(upper: Path) -> None:
    """Normalize observed, content-neutral metadata after OverlayFS is unmounted.

    The command cannot access this raw upper or invoke any xattr-setting syscall.
    Metacopy, index and redirect following were disabled at mount time. The
    general exporter still rejects *all* xattrs; no global validation is relaxed.
    """

    def inspect(descriptor: int) -> None:
        for name in os.listxattr(descriptor):
            value = os.getxattr(descriptor, name)
            if not (
                (name == "user.overlay.uuid" and len(value) == 16)
                or (name == "user.overlay.impure" and value == b"y")
                or (name == "user.overlay.origin" and value == b"")
            ):
                raise WorkspaceBoundaryError("unsupported OverlayFS kernel metadata")
            os.removexattr(descriptor, name)

    def directory(descriptor: int) -> None:
        inspect(descriptor)
        for name in os.listdir(descriptor):
            info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                continue  # The exporter separately checks symlinks/special files.
            child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                            dir_fd=descriptor)
            try:
                if stat.S_ISDIR(info.st_mode):
                    directory(child)
                else:
                    inspect(child)
            finally:
                os.close(child)

    descriptor = os.open(upper, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        directory(descriptor)
    finally:
        os.close(descriptor)


def _write_completion(root: Path, diff) -> None:
    payload = {
        "schema_version": 1,
        "entries": [entry.__dict__ for entry in diff.entries],
        "total_bytes": diff.total_bytes,
        "manifest_hash": diff.manifest_hash,
    }
    descriptor = os.open(root / "completion.tmp", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(root / "completion.tmp", root / "completion.json")


def main(arguments: list[str]) -> int:
    if len(arguments) != 1:
        return 64
    phase = "namespace"
    try:
        if os.getpid() != 1:
            raise RuntimeError("candidate supervisor must be namespace init")
        root = Path(arguments[0])
        config = json.loads((root / "config.json").read_text(encoding="utf-8"))
        for name, limit in (("MAESTRO_EXPECT_NOFILE", resource.RLIMIT_NOFILE),
                            ("MAESTRO_EXPECT_FSIZE", resource.RLIMIT_FSIZE)):
            value = int(os.environ[name])
            resource.setrlimit(limit, (value, value))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        phase = "limits"
        _verify_scope_identity(config["expected_cgroup"])
        _verify_limits()
        phase = "mounts"
        view = _prepare_root(root, config)
        environment = {
            "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "PYTHONPATH": "/runtime",
            "MAESTRO_EXPECT_NOFILE": os.environ["MAESTRO_EXPECT_NOFILE"],
            "MAESTRO_EXPECT_FSIZE": os.environ["MAESTRO_EXPECT_FSIZE"],
        }
        phase = "exec"
        process = subprocess.Popen(
            ["/usr/bin/python3", "-P", "-S", "-m", "orchestrator.isolation._candidate_exec", "--",
             *config["command"]], env=environment, close_fds=True,
            preexec_fn=lambda: _enter_command_root(view),
        )
        try:
            returncode = process.wait()
        finally:
            _terminate_descendants()
        if returncode != 0:
            return returncode if 0 < returncode < 126 else 1
        phase = "export"
        unmount = subprocess.run(
            ["/usr/bin/umount", str(view / "workspace")], stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False, timeout=5,
        )
        if unmount.returncode != 0:
            raise RuntimeError("candidate OverlayFS could not be unmounted")
        _strip_kernel_metadata(root / "buffer" / "upper")
        diff = export_overlay_diff(
            root / "lower", root / "buffer" / "upper", root / "candidate",
            max_entries=config["candidate_entries"], max_bytes=config["candidate_bytes"],
            max_depth=64,
        )
        validate_overlay_candidate(
            root / "lower", diff, max_entries=config["candidate_entries"],
            max_bytes=config["candidate_bytes"],
        )
        phase = "completion"
        _write_completion(root, diff)
        return 0
    except Exception as exc:
        detail = str(exc) if isinstance(exc, (_MountSetupError, WorkspaceBoundaryError)) else type(exc).__name__
        print(f"candidate boundary failed at {phase}: {detail}", file=sys.stderr, flush=True)
        return 78


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
