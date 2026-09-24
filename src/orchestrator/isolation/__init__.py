"""Fail-closed operating-system isolation primitives."""

from .workspace import WorkspaceBoundaryError, WorkspaceInspection, inspect_workspace
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
    "FsAccess",
    "LandlockResult",
    "LandlockUnavailable",
    "PathGrant",
    "READ_EXECUTE",
    "WORKSPACE_WRITE",
    "WorkspaceBoundaryError",
    "WorkspaceInspection",
    "inspect_workspace",
    "landlock_abi",
    "restrict_current_process",
]
