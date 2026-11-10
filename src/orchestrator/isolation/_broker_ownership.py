"""Conservative Linux broker-lock observation, without launch or stop authority.

All cooperating trusted host processes must use the same configured path and
binding, and must not replace any path component or manipulate these private
descriptor, including closing, duplicating over, unlocking, or re-locking it.
The probe cannot prove continuous ownership or detect arbitrary FD tampering.
The trusted host must defer asynchronous exceptions across complete acquire,
observe and close calls, including result receipt, cleanup and FD transfers. Python cannot
close every asynchronous-interruption window here; if this contract fails, a
descriptor and lock can remain until process exit. Synchronous exceptions are
cleaned up where ownership is known. This is experimental, not dispatch authority.
Validation cannot prevent a malicious same-UID process changing a
path immediately afterwards. Callers must serialize and revalidate around each
effect and publication; this primitive alone cannot make those operations atomic.
"""
from __future__ import annotations

import os
import stat
import sys
import threading
from enum import Enum

try:
    import fcntl
except ImportError:  # Unsupported platforms must fail closed.
    fcntl = None


class OwnershipState(Enum):
    OBSERVED_LOCKED = "observed_locked"
    UNKNOWN = "unknown"


def _identity(fd: int) -> tuple[int, int, int]:
    s = os.fstat(fd)
    with open(f"/proc/self/fdinfo/{fd}", encoding="ascii") as info:
        mounts = [line.split()[1] for line in info if line.startswith("mnt_id:")]
    if len(mounts) != 1:
        raise ValueError("missing mount identity")
    return s.st_dev, s.st_ino, int(mounts[0])


def _directory_ok(fd: int) -> None:
    s = os.fstat(fd)
    if not stat.S_ISDIR(s.st_mode) or s.st_uid not in (0, os.geteuid()):
        raise ValueError("untrusted directory")
    if s.st_mode & 0o022 and not (s.st_uid == 0 and s.st_mode & stat.S_ISVTX):
        raise ValueError("writable directory")


def _lock_ok(fd: int) -> None:
    s = os.fstat(fd)
    if (not stat.S_ISREG(s.st_mode) or s.st_uid != os.geteuid()
            or s.st_nlink != 1 or s.st_mode & 0o077):
        raise ValueError("untrusted lock")


def _close_all(fds: list[int], original: BaseException | None = None) -> None:
    failures = []
    while fds:
        fd = fds.pop()  # Never retry a numeric FD after close has been attempted.
        try:
            os.close(fd)
        except BaseException as exc:
            failures.append(exc)
    if original is not None:
        for failure in failures:
            if isinstance(original, Exception) and not isinstance(failure, Exception):
                failure.add_note("original failure: " + repr(original))
                raise failure from original
            original.add_note("cleanup failure: " + repr(failure))
        raise original
    if failures:
        primary = next((exc for exc in failures if not isinstance(exc, Exception)), failures[0])
        for failure in failures:
            if failure is not primary:
                primary.add_note("additional close failure: " + repr(failure))
        raise primary


def _open_path(path: str) -> tuple[int, tuple[tuple[int, int, int], ...]]:
    parts = path.split("/")
    if not path.startswith("/") or len(parts) < 3 or any(
        not p or p in (".", "..") for p in parts[1:]
    ):
        raise ValueError("canonical absolute path required")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    owned: list[int] = []
    try:
        directory = os.open("/", flags)
        owned.append(directory)
        _directory_ok(directory)
        identities = [_identity(directory)]
        for part in parts[1:-1]:
            child = os.open(part, flags, dir_fd=directory)
            owned.append(child)
            old, directory = directory, child
            owned.remove(old)
            os.close(old)
            _directory_ok(directory)
            identities.append(_identity(directory))
        path_fd = os.open(parts[-1], os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC,
                          dir_fd=directory)
        owned.append(path_fd)
        _lock_ok(path_fd)
        pinned = _identity(path_fd)
        fd = os.open(f"/proc/self/fd/{path_fd}", os.O_RDWR | os.O_CLOEXEC)
        owned.append(fd)
        _lock_ok(fd)
        if _identity(fd) != pinned:
            raise ValueError("pinned lock changed")
        identities.append(pinned)
        if identities[-1][2] != identities[-2][2]:
            raise ValueError("lock mount differs")
        owned.remove(path_fd)
        os.close(path_fd)
        owned.remove(directory)
        os.close(directory)
        result = (fd, tuple(identities))
        owned.remove(fd)
        return result
    except BaseException as exc:
        _close_all(owned, exc)
        raise AssertionError("unreachable")


def _boot_id() -> str:
    with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
        value = source.read().strip()
    if not value:
        raise ValueError("missing boot identity")
    return value


class BrokerOwnership:
    """A live observation only; unknown is sticky and does not prove queue closure."""

    def __init__(self) -> None:
        self._fd = -1
        self._valid = False
        self._mutex = threading.Lock()

    @classmethod
    def acquire(cls, path: os.PathLike[str] | str, *, store_id: str, namespace: str) -> BrokerOwnership:
        obj = cls()
        try:
            if (not sys.platform.startswith("linux") or fcntl is None
                    or not isinstance(store_id, str) or not store_id
                    or not isinstance(namespace, str) or not namespace):
                return obj
            obj._path = os.fspath(path)
            if not isinstance(obj._path, str):
                return obj
            obj.binding = (store_id, namespace, _boot_id())
            obj._pid = os.getpid()
            obj._fd, obj._identities = _open_path(obj._path)
            fcntl.flock(obj._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            obj._valid = True
            if obj.observe() is not OwnershipState.OBSERVED_LOCKED:
                obj.close()
        except BaseException as exc:
            fd, obj._fd = obj._fd, -1
            obj._valid = False
            try:
                _close_all([fd] if fd >= 0 else [], exc)
            except Exception:
                return obj
            raise
        return obj

    def observe(self) -> OwnershipState:
        if getattr(self, "_pid", None) != os.getpid():
            return OwnershipState.UNKNOWN
        with self._mutex:
            if not self._valid:
                return OwnershipState.UNKNOWN
            probe = -1
            try:
                if os.getpid() != self._pid or _boot_id() != self.binding[2]:
                    raise ValueError("ownership continuity lost")
                _lock_ok(self._fd)
                if _identity(self._fd) != self._identities[-1]:
                    raise ValueError("descriptor changed")
                probe, identities = _open_path(self._path)
                if identities != self._identities:
                    raise ValueError("path changed")
                try:
                    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    raise ValueError("lock lost")
                fd, probe = probe, -1
                _close_all([fd])
                return OwnershipState.OBSERVED_LOCKED
            except BaseException as exc:
                self._valid = False
                try:
                    _close_all([probe] if probe >= 0 else [], exc)
                except Exception:
                    return OwnershipState.UNKNOWN
                raise

    def close(self) -> None:
        # A forked child may inherit a permanently locked thread mutex. Its
        # CLOEXEC descriptors remain conservative until exec or process exit.
        if getattr(self, "_pid", None) != os.getpid():
            return
        with self._mutex:
            # The trusted host owns this private descriptor and must not close
            # or reuse it behind this object. Closing is not stop evidence.
            fd, self._fd = self._fd, -1
            self._valid = False
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    # Never retry close: the numeric FD may already be reused.
                    pass
