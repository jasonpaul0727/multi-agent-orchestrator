"""Local cgroup capability experiment; it grants no v2 or stop authority."""
import errno
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid

import pytest

from orchestrator.isolation.launcher import _systemd_client_environment


def require_manager():
    def unavailable(reason):
        if os.environ.get("MEDIATION_SANDBOX_REQUIRED") == "1":
            pytest.fail(reason)
        pytest.skip(reason)

    if sys.platform != "linux" or not shutil.which("systemd-run") or not shutil.which("systemctl"):
        unavailable("requires Linux systemd-run and systemctl")
    env = _systemd_client_environment()
    result = subprocess.run(["systemctl", "--user", "show-environment"], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
    if result.returncode:
        unavailable("requires reachable user manager")
    bus = Path(f"/run/user/{os.getuid()}/bus")
    manager = Path(f"/run/user/{os.getuid()}/systemd/private")
    if not bus.exists() or not manager.exists():
        unavailable("requires user bus and private user manager socket")
    for path in (bus, manager):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(2)
            sock.connect(str(path))
    return env, bus, manager


def events(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    return dict(line.split() for line in os.read(fd, 4096).decode("ascii").splitlines())


def mount_id(fd):
    lines = Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
    return next(int(line.split(":", 1)[1]) for line in lines if line.startswith("mnt_id:"))


def is_cgroup_v2_mount(mount):
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, separator, right = line.partition(" - ")
        if separator and int(left.split()[0]) == mount:
            return right.split()[0] == "cgroup2"
    return False

def test_parent_cgroup_identity_and_population_with_controlled_actor():
    env, bus, manager = require_manager()
    nonce = uuid.uuid4().hex
    slice_name = f"maestroprobe{nonce}.slice"
    unit_name = f"maestroprobe{nonce}.service"
    source = '''import json,os,socket,sys
paths=json.loads(sys.stdin.readline())
out={'input_ok':paths.pop('marker') == 'parent-cgroup-fixture'}
for name in ('user_bus','manager_private'):
 s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(1)
 try:s.connect(paths[name]);out[name]=0
 except OSError as e:out[name]=e.errno
 finally:s.close()
for name in ('unit_procs','parent_procs'):
 try:
  fd=os.open(paths[name],os.O_WRONLY);os.close(fd);out[name]=0
 except OSError as e:out[name]=e.errno
print(json.dumps(out,sort_keys=True),flush=True)'''
    properties = ("ProtectControlGroups=yes", "PrivateUsers=yes", "ProtectSystem=strict",
                  "ProtectProc=invisible", "ProcSubset=pid", "InaccessiblePaths=-/run/user",
                  "InaccessiblePaths=-/mnt/wslg/run/user", "InaccessiblePaths=-/run/dbus",
                  "RestrictAddressFamilies=AF_UNIX", "RuntimeMaxSec=30s", "KillMode=control-group")
    command = ["systemd-run", "--user", "--quiet", "--wait", "--pipe", "--collect",
               f"--slice={slice_name}", f"--unit={unit_name.removesuffix('.service')}",
               *(f"--property={prop}" for prop in properties), "--",
               "/usr/bin/python3", "-I", "-S", "-c", source]
    proc = subprocess.Popen(command, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    parent_fd = events_fd = None
    try:
        deadline = time.monotonic() + 10
        while True:
            show = subprocess.run(["systemctl", "--user", "show", unit_name,
                                   "--property=Id", "--property=ActiveState",
                                   "--property=ControlGroup", "--property=InvocationID"],
                                  env=env, capture_output=True, text=True, timeout=3)
            values = dict(line.split("=", 1) for line in show.stdout.splitlines() if "=" in line)
            if show.returncode == 0 and values.get("ActiveState") == "active" and values.get("ControlGroup"):
                break
            if proc.poll() is not None:
                pytest.fail("test-owned actor exited before it became active")
            if time.monotonic() >= deadline:
                pytest.fail("test-owned actor did not become active")
            time.sleep(0.05)
        assert values["Id"] == unit_name and values["InvocationID"]
        relative = Path(values["ControlGroup"].lstrip("/"))
        assert ".." not in relative.parts
        group = Path("/sys/fs/cgroup") / relative
        parent = group.parent
        assert group.name == unit_name and parent.name == slice_name
        assert (group / "cgroup.procs").is_file()
        assert (parent / "cgroup.procs").is_file()
        # O_NOFOLLOW protects the final component only; the host-supplied
        # ancestor path and manager remain trusted test prerequisites.
        parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        events_fd = os.open("cgroup.events", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd)
        before = os.fstat(parent_fd)
        before_mount = mount_id(parent_fd)
        assert is_cgroup_v2_mount(before_mount)
        assert events(events_fd)["populated"] == "1"
        paths = {"user_bus": str(bus), "manager_private": str(manager),
                 "unit_procs": str(group / "cgroup.procs"),
                 "parent_procs": str(parent / "cgroup.procs"), "marker": "parent-cgroup-fixture"}
        stdout, stderr = proc.communicate(json.dumps(paths) + "\n", timeout=15)
        assert proc.returncode == 0 and stderr == ""
        assert json.loads(stdout) == {"input_ok": True, "user_bus": errno.EACCES,
                                     "manager_private": errno.EACCES, "unit_procs": errno.EROFS,
                                     "parent_procs": errno.EROFS}
        assert events(events_fd)["populated"] == "0"
        after = os.stat(parent, follow_symlinks=False)
        assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
        pinned = os.fstat(parent_fd)
        assert (pinned.st_dev, pinned.st_ino, mount_id(parent_fd)) == (before.st_dev, before.st_ino, before_mount)
        reopened = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            fresh = os.fstat(reopened)
            assert (fresh.st_dev, fresh.st_ino, mount_id(reopened)) == (
                before.st_dev, before.st_ino, before_mount)
            assert is_cgroup_v2_mount(mount_id(reopened))
        finally:
            os.close(reopened)
        print(f"parent_device={before.st_dev} parent_inode={before.st_ino} parent_mount_id={before_mount}")
    finally:
        original = sys.exception()
        cleanup_errors = []

        def stop_and_observe(name):
            try:
                stopped = subprocess.run(["systemctl", "--user", "stop", name], env=env,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                shown = subprocess.run(["systemctl", "--user", "show", name,
                                        "--property=LoadState", "--property=ActiveState"],
                                       env=env, capture_output=True, text=True, timeout=5)
                state = dict(line.split("=", 1) for line in shown.stdout.splitlines() if "=" in line)
                if (shown.returncode != 0 or state.get("LoadState") not in {"loaded", "not-found"}
                    or state.get("ActiveState") not in {"inactive", "failed"}):
                    cleanup_errors.append("owned unit cleanup observation unavailable; state unknown")
                elif stopped.returncode and state.get("LoadState") != "not-found":
                    cleanup_errors.append("owned unit stop failed; state unknown")
            except Exception as exc:
                cleanup_errors.append(f"owned unit cleanup unavailable; state unknown: {exc!r}")

        # Both names were allocated solely by this test. A missing or stopped
        # observation is cleanup feedback, never queue or transport proof.
        stop_and_observe(unit_name)
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            cleanup_errors.append("test transport timed out; unit state unknown")
            try:
                proc.kill()
                proc.communicate(timeout=5)
            except Exception as exc:
                cleanup_errors.append(f"test transport cleanup failed: {exc!r}")
        except Exception as exc:
            cleanup_errors.append(f"test transport cleanup failed: {exc!r}")
        if proc.returncode is None:
            cleanup_errors.append("test transport exit unverified; state unknown")
        for fd in (events_fd, parent_fd):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError as exc:
                    cleanup_errors.append(f"closing test FD failed: {exc!r}")
        stop_and_observe(slice_name)
        if cleanup_errors:
            error = AssertionError("; ".join(cleanup_errors))
            if original is None:
                raise error
            original.add_note(str(error))
