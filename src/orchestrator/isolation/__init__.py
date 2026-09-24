"""Fail-closed operating-system isolation primitives."""

from .workspace import (
    WorkspaceBoundaryError,
    WorkspaceDiff,
    WorkspaceDiffEntry,
    WorkspaceInspection,
    export_overlay_diff,
    inspect_workspace,
    snapshot_workspace,
)
from .launcher import (
    InvalidSandboxRequest,
    IsolationUnavailable,
    SandboxLimits,
    SandboxResult,
    SandboxSession,
    SystemdReadOnlyLauncher,
)
from .landlock import (
    FsAccess,
    LandlockResult,
    LandlockUnavailable,
    PathGrant,
    READ_EXECUTE,
    WORKSPACE_WRITE,
    landlock_abi,
    restrict_current_process,
)

__all__ = [
    "InvalidSandboxRequest",
    "IsolationUnavailable",
    "SandboxLimits",
    "SandboxResult",
    "SandboxSession",
    "SystemdReadOnlyLauncher",
    "FsAccess",
    "LandlockResult",
    "LandlockUnavailable",
    "PathGrant",
    "READ_EXECUTE",
    "WORKSPACE_WRITE",
    "WorkspaceBoundaryError",
    "WorkspaceDiff",
    "WorkspaceDiffEntry",
    "WorkspaceInspection",
    "export_overlay_diff",
    "inspect_workspace",
    "snapshot_workspace",
    "landlock_abi",
    "restrict_current_process",
]
