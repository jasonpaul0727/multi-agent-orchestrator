"""Local prepared sandbox boundary regression; not v2 host or transport proof."""
import errno
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys

import pytest

from orchestrator.isolation import SandboxLimits, SystemdReadOnlyLauncher
from orchestrator.isolation.launcher import _systemd_cgroup_parent, _systemd_client_environment
from tests.integration.test_general_worker_isolation import cleanup_owned, empty


def unavailable(reason):
    if os.environ.get("MEDIATION_SANDBOX_REQUIRED") == "1":
        pytest.fail(reason)
    pytest.skip(reason)


def require_local_user_manager():
    if sys.platform != "linux":
        unavailable("requires Linux systemd user manager")
    if not shutil.which("systemd-run") or not shutil.which("systemctl"):
        unavailable("requires systemd-run and systemctl")
    result = subprocess.run(["systemctl", "--user", "show-environment"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            timeout=5, check=False, env=_systemd_client_environment())
    if result.returncode:
        unavailable("requires reachable systemd user manager")
    paths = (Path(f"/run/user/{os.getuid()}/bus"),
             Path(f"/run/user/{os.getuid()}/systemd/private"))
    if not all(path.exists() for path in paths):
        unavailable("requires user bus and systemd user manager private sockets")
    for path in paths:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect(str(path))
    return paths


def test_prepared_worker_cannot_reach_host_bus_or_write_own_cgroup(tmp_path):
    bus, manager = require_local_user_manager()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    launcher = SystemdReadOnlyLauncher()
    prepared = launcher.prepare_launch()
    parent = _systemd_cgroup_parent(launcher._systemctl, _systemd_client_environment())
    paths = {"user_bus": str(bus), "manager_private": str(manager),
             "cgroup_unit": str(parent / prepared.unit_name / "cgroup.procs")}
    source = '''import json,os,socket,sys,time
paths=json.loads(PATHS)
out={'input_ok':sys.stdin.buffer.read() == b'prepared-boundary-input'}
for name in ('user_bus','manager_private'):
 s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(1)
 try:s.connect(paths[name]);out[name]=0
 except OSError as e:out[name]=e.errno
 finally:s.close()
try:
 fd=os.open(paths['cgroup_unit'],os.O_WRONLY);os.close(fd);out['cgroup_unit']=0
except OSError as e:out['cgroup_unit']=e.errno
try:open('/proc/self/cgroup').close();out['proc_membership']=0
except OSError as e:out['proc_membership']=e.errno
print(json.dumps(out,sort_keys=True),flush=True)
time.sleep(1)'''.replace("PATHS", repr(json.dumps(paths)))
    # The launch profile deliberately rejects ambiguous argv; encode the probe
    # as the same unambiguous argument form used by existing integration tests.
    token = "exec(bytes([" + ",".join(str(byte) for byte in source.encode()) + "]))"
    try:
        session = launcher.launch(workspace, ["/usr/bin/python3", "-I", "-S", "-c", token],
                                  prepared_launch=prepared, input_bytes=b"prepared-boundary-input",
                                  limits=SandboxLimits(timeout_seconds=10))
        ack = session.launch_receipt
        assert ack is not None
        assert ack.launch_id == prepared.launch_id
        assert ack.unit_name == prepared.unit_name
        assert ack.control_group.endswith("/" + prepared.unit_name)
        assert ack.invocation_id
        cgroup_file = parent / prepared.unit_name / "cgroup.procs"
        assert cgroup_file.is_file()
        try:
            descriptor = os.open(cgroup_file, os.O_WRONLY)
        except OSError as exc:
            host_cgroup_open = f"errno:{exc.errno}"
        else:
            os.close(descriptor)
            host_cgroup_open = "opened_without_write"
        print(f"host_cgroup_open={host_cgroup_open}")
        result = session.wait()
        assert result.returncode == 0
        assert result.stderr == b""
        assert result.input_written
        assert json.loads(result.stdout) == {"input_ok": True, "user_bus": errno.EACCES,
                                             "manager_private": errno.EACCES,
                                             "cgroup_unit": errno.EROFS, "proc_membership": errno.EACCES}
        assert result.termination_receipt is not None
        assert result.termination_receipt.unit_name == ack.unit_name
        assert result.termination_receipt.control_group == ack.control_group
        empty(result.termination_receipt)
    finally:
        original = sys.exception()
        try:
            cleanup_owned(launcher)
        except Exception as exc:
            if original is not None:
                original.add_note(f"owned launcher cleanup failed: {exc!r}")
            else:
                raise


