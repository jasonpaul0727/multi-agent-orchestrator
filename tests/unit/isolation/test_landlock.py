from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

from orchestrator.isolation import landlock_abi


pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Landlock requires Linux")


def test_landlock_restriction_is_enforced_by_kernel_in_child_process(tmp_path):
    if landlock_abi() < 3:
        pytest.skip("Landlock ABI 3 is not available on this kernel")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "allowed.txt").write_text("inside", encoding="utf-8")
    outside = tmp_path / "outside-secret"
    outside.write_text("outside", encoding="utf-8")
    (workspace / "escape-link").symlink_to(outside)
    source_root = Path(__file__).parents[3] / "src"
    code = textwrap.dedent(
        f"""
        import json
        import sys
        from pathlib import Path
        sys.path.insert(0, {str(source_root)!r})
        from orchestrator.isolation import FsAccess, PathGrant, WORKSPACE_WRITE, restrict_current_process

        result = restrict_current_process((
            PathGrant(Path("/"), FsAccess.EXECUTE),
            PathGrant(Path({str(workspace)!r}), WORKSPACE_WRITE),
        ))
        observed = {{"abi": result.abi, "grants": result.grant_count,
                    "inside_read": False, "inside_write": False,
                    "outside_read_blocked": False, "symlink_escape_blocked": False,
                    "outside_write_blocked": False, "etc_read_blocked": False}}
        observed["inside_read"] = Path({str(workspace / "allowed.txt")!r}).read_text(encoding="utf-8") == "inside"
        Path({str(workspace / "new.txt")!r}).write_text("new", encoding="utf-8")
        observed["inside_write"] = True
        for key, path in (("outside_read_blocked", {str(outside)!r}),
                          ("symlink_escape_blocked", {str(workspace / "escape-link")!r}),
                          ("etc_read_blocked", "/etc/passwd")):
            try:
                Path(path).read_text(encoding="utf-8")
            except OSError:
                observed[key] = True
        try:
            Path({str(outside)!r}).write_text("no", encoding="utf-8")
        except OSError:
            observed["outside_write_blocked"] = True
        print(json.dumps(observed, sort_keys=True))
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        text=True,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "abi": landlock_abi(),
        "etc_read_blocked": True,
        "grants": 2,
        "inside_read": True,
        "inside_write": True,
        "outside_read_blocked": True,
        "outside_write_blocked": True,
        "symlink_escape_blocked": True,
    }
    assert (workspace / "new.txt").read_text(encoding="utf-8") == "new"
    assert outside.read_text(encoding="utf-8") == "outside"
