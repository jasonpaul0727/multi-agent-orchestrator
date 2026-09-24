"""Small fail-closed Landlock ABI 3 wrapper for child-process launchers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag
import ctypes
import errno
import os
from pathlib import Path
import platform


class LandlockUnavailable(RuntimeError):
    """The running kernel or process cannot establish the requested ruleset."""


class FsAccess(IntFlag):
    EXECUTE = 1 << 0
    WRITE_FILE = 1 << 1
    READ_FILE = 1 << 2
    READ_DIR = 1 << 3
    REMOVE_DIR = 1 << 4
    REMOVE_FILE = 1 << 5
    MAKE_CHAR = 1 << 6
    MAKE_DIR = 1 << 7
    MAKE_REG = 1 << 8
    MAKE_SOCK = 1 << 9
    MAKE_FIFO = 1 << 10
    MAKE_BLOCK = 1 << 11
    MAKE_SYM = 1 << 12
    REFER = 1 << 13
    TRUNCATE = 1 << 14


READ_EXECUTE = FsAccess.EXECUTE | FsAccess.READ_FILE | FsAccess.READ_DIR
WORKSPACE_WRITE = (
    READ_EXECUTE
    | FsAccess.WRITE_FILE
    | FsAccess.REMOVE_DIR
    | FsAccess.REMOVE_FILE
    | FsAccess.MAKE_DIR
    | FsAccess.MAKE_REG
    | FsAccess.REFER
    | FsAccess.TRUNCATE
)

_SYSCALLS = {
    "x86_64": (444, 445, 446),
    "amd64": (444, 445, 446),
    "aarch64": (444, 445, 446),
    "arm64": (444, 445, 446),
}
_CREATE_RULESET_VERSION = 1
_RULE_PATH_BENEATH = 1
_PR_SET_NO_NEW_PRIVS = 38
_O_PATH = getattr(os, "O_PATH", os.O_RDONLY)


@dataclass(frozen=True)
class PathGrant:
    """Filesystem rights granted below one existing, non-symlink path."""

    path: Path
    access: FsAccess


@dataclass(frozen=True)
class LandlockResult:
    """Evidence returned after restrictions have been applied to this process."""

    abi: int
    grant_count: int


class _RulesetAttr(ctypes.Structure):
    _fields_ = [("handled_access_fs", ctypes.c_uint64)]


class _PathBeneathAttr(ctypes.Structure):
    _fields_ = [("allowed_access", ctypes.c_uint64), ("parent_fd", ctypes.c_int32)]


def landlock_abi() -> int:
    """Return the kernel ABI version, failing closed for unknown architectures."""

    create_syscall, _, _ = _syscalls()
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(create_syscall, None, 0, _CREATE_RULESET_VERSION)
    if result < 1:
        error = ctypes.get_errno()
        raise LandlockUnavailable("Landlock ABI query is unavailable") from OSError(error, os.strerror(error))
    return int(result)


def restrict_current_process(grants: tuple[PathGrant, ...] | list[PathGrant]) -> LandlockResult:
    """Apply mandatory filesystem grants to the current process and descendants.

    Call only in a dedicated launcher process before executing untrusted code.
    Any unsupported path, ABI, syscall, or kernel operation raises instead of
    continuing without the requested restriction.
    """

    abi = landlock_abi()
    if abi < 3:
        raise LandlockUnavailable("Landlock ABI 3 or newer is required")
    if not grants:
        raise LandlockUnavailable("an empty Landlock grant set is not a sandbox")

    handled = int(sum(FsAccess))
    libc = ctypes.CDLL(None, use_errno=True)
    create_syscall, add_rule_syscall, restrict_syscall = _syscalls()
    ruleset_attr = _RulesetAttr(handled_access_fs=handled)
    ruleset_fd = libc.syscall(
        create_syscall,
        ctypes.byref(ruleset_attr),
        ctypes.sizeof(ruleset_attr),
        0,
    )
    if ruleset_fd < 0:
        _raise_syscall("Landlock ruleset creation failed")

    opened_fds: list[int] = []
    try:
        for grant in grants:
            if not isinstance(grant, PathGrant) or not isinstance(grant.access, FsAccess):
                raise LandlockUnavailable("Landlock grants must use typed filesystem rights")
            if int(grant.access) == 0 or int(grant.access) & ~handled:
                raise LandlockUnavailable("Landlock grant has empty or unsupported rights")
            path = Path(grant.path)
            if not path.is_absolute():
                raise LandlockUnavailable("Landlock grant paths must be absolute")
            flags = _O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW
            if path.is_dir():
                flags |= os.O_DIRECTORY
            try:
                parent_fd = os.open(path, flags)
            except OSError as exc:
                raise LandlockUnavailable("Landlock grant path cannot be opened safely") from exc
            opened_fds.append(parent_fd)
            beneath = _PathBeneathAttr(allowed_access=int(grant.access), parent_fd=parent_fd)
            result = libc.syscall(
                add_rule_syscall,
                ruleset_fd,
                _RULE_PATH_BENEATH,
                ctypes.byref(beneath),
                0,
            )
            if result < 0:
                _raise_syscall("Landlock path grant was rejected")

        if libc.prctl(_PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
            _raise_syscall("no_new_privs could not be enabled")
        if libc.syscall(restrict_syscall, ruleset_fd, 0) < 0:
            _raise_syscall("Landlock restrictions could not be applied")
        return LandlockResult(abi=abi, grant_count=len(grants))
    finally:
        for descriptor in opened_fds:
            os.close(descriptor)
        os.close(ruleset_fd)


def _syscalls() -> tuple[int, int, int]:
    try:
        return _SYSCALLS[platform.machine().lower()]
    except KeyError as exc:
        raise LandlockUnavailable("Landlock syscall numbers are unknown on this architecture") from exc


def _raise_syscall(message: str) -> None:
    error = ctypes.get_errno() or errno.EPERM
    raise LandlockUnavailable(message) from OSError(error, os.strerror(error))


__all__ = [
    "FsAccess",
    "LandlockResult",
    "LandlockUnavailable",
    "PathGrant",
    "READ_EXECUTE",
    "WORKSPACE_WRITE",
    "landlock_abi",
    "restrict_current_process",
]
