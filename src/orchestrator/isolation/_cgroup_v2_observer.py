"""Read-only cgroup samples, not authentication, launch fencing, or stop proof."""
import os
from pathlib import Path
import re
import stat
import sys
import threading
import uuid

_ROOT = Path("/sys/fs/cgroup")
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


def _limited(path, limit):
    with open(path, "rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("proc entry too large")
    return data


def _boot_id():
    value = _limited("/proc/sys/kernel/random/boot_id", 64).decode("ascii").strip()
    if str(uuid.UUID(value)) != value:
        raise ValueError("invalid boot id")
    return value


def _mount_id(fd):
    lines = _limited(f"/proc/self/fdinfo/{fd}", 4096).decode("ascii").splitlines()
    found = [line.split(":", 1)[1].strip() for line in lines if line.startswith("mnt_id:")]
    if len(found) != 1 or not found[0].isascii() or not found[0].isdecimal():
        raise ValueError("invalid mount id")
    return int(found[0])


def _cgroup2_mount(mount):
    lines = _limited("/proc/self/mountinfo", 4194304).decode("ascii").splitlines()
    found = []
    for line in lines:
        left, separator, right = line.partition(" - ")
        if not separator:
            raise ValueError("invalid mountinfo")
        fields = left.split()
        if not fields or not fields[0].isdecimal():
            raise ValueError("invalid mountinfo")
        if int(fields[0]) == mount:
            found.append(right.split()[0])
    return found == ["cgroup2"]


def _identity(fd):
    info = os.fstat(fd)
    return info.st_dev, info.st_ino, _mount_id(fd)


def _parts(path):
    if type(path) is not str or not path or len(path) > 4096 or path.startswith("/") or "\x00" in path:
        raise ValueError("invalid path")
    parts = path.split("/")
    if len(parts) > 128 or any(not part or part in {".", ".."} or len(os.fsencode(part)) > 255 for part in parts):
        raise ValueError("invalid path")
    return parts


def _close_owned(*fds):
    failures = []
    for fd in fds:
        if fd is not None:
            try:
                os.close(fd)
            except BaseException as exc:
                failures.append(exc)
    if failures:
        primary = next((exc for exc in failures if not isinstance(exc, Exception)), failures[0])
        for exc in failures:
            if exc is not primary:
                primary.add_note("additional close failure: " + repr(exc))
        raise primary


def _walk(parts, root_identity=None):
    current = os.open(_ROOT, _DIR_FLAGS)
    try:
        root = _identity(current)
        if root_identity is not None and root != root_identity:
            raise ValueError("root changed")
        if not _cgroup2_mount(root[2]):
            raise ValueError("not cgroup2")
        for part in parts:
            nxt = os.open(part, _DIR_FLAGS, dir_fd=current)
            old, current = current, nxt
            os.close(old)
            item = _identity(current)
            if item[0] != root[0] or item[2] != root[2]:
                raise ValueError("crossed mount")
        return current, root
    except BaseException as exc:
        try:
            _close_owned(current)
        except BaseException as cleanup:
            if isinstance(exc, Exception) and not isinstance(cleanup, Exception):
                cleanup.add_note("original directory failure: " + repr(exc))
                raise cleanup from exc
            exc.add_note("directory cleanup failure: " + repr(cleanup))
        raise


def _read_population(fd):
    data = os.pread(fd, 4097, 0)
    if len(data) > 4096:
        raise ValueError("events too large")
    fields = {}
    for line in data.decode("ascii").splitlines():
        parts = line.split()
        if len(parts) != 2 or not re.fullmatch(r"[a-z_]+", parts[0]) or parts[0] in fields:
            raise ValueError("invalid events")
        if not parts[1].isascii() or not parts[1].isdecimal():
            raise ValueError("invalid value")
        fields[parts[0]] = parts[1]
    if set(fields) - {"populated", "frozen"} or ("frozen" in fields and fields["frozen"] not in {"0", "1"}):
        raise ValueError("invalid events fields")
    if fields.get("populated") not in {"0", "1"}:
        raise ValueError("invalid populated")
    return int(fields["populated"])


class CgroupObserver:
    """Pin and sample one path; None means unknown, never stopped."""
    def __init__(self, parts, directory, events, root, boot):
        self._parts = parts
        self._directory = directory
        self._events = events
        self._root = root
        self._boot = boot
        self._dir_identity = _identity(directory)
        self._event_identity = _identity(events)
        self._lock = threading.Lock()

    @classmethod
    def open(cls, path):
        directory = events = None
        try:
            parts = _parts(path)
            boot = _boot_id()
            directory, root = _walk(parts)
            events = os.open("cgroup.events", _FILE_FLAGS, dir_fd=directory)
            if not stat.S_ISREG(os.fstat(events).st_mode) or _identity(events)[2] != root[2]:
                raise ValueError("invalid events file")
            observer = cls(parts, directory, events, root, boot)
            if not observer._consistent():
                raise ValueError("identity changed")
            return observer
        except BaseException as exc:
            try:
                _close_owned(events, directory)
            except BaseException as cleanup:
                if isinstance(exc, Exception) and not isinstance(cleanup, Exception):
                    cleanup.add_note("original open failure: " + repr(exc))
                    raise cleanup from exc
                exc.add_note("open cleanup failure: " + repr(cleanup))
            if isinstance(exc, Exception):
                return None
            raise

    def _consistent(self):
        if (_boot_id() != self._boot or _identity(self._directory) != self._dir_identity
            or _identity(self._events) != self._event_identity):
            return False
        fresh = event = None
        try:
            fresh, _ = _walk(self._parts, self._root)
            if _identity(fresh) != self._dir_identity:
                return False
            event = os.open("cgroup.events", _FILE_FLAGS, dir_fd=fresh)
            return stat.S_ISREG(os.fstat(event).st_mode) and _identity(event) == self._event_identity
        finally:
            original = sys.exception()
            try:
                _close_owned(event, fresh)
            except BaseException as cleanup:
                if original is None:
                    raise
                if isinstance(original, Exception) and not isinstance(cleanup, Exception):
                    cleanup.add_note("original identity failure: " + repr(original))
                    raise cleanup from original
                original.add_note("identity cleanup failure: " + repr(cleanup))

    def observe(self):
        with self._lock:
            if self._directory is None:
                return None
            try:
                if not self._consistent(): return None
                population = _read_population(self._events)
                if not self._consistent(): return None
                return population
            except Exception:
                return None

    def close(self):
        with self._lock:
            if self._directory is not None:
                events, directory = self._events, self._directory
                self._events = self._directory = None
                _close_owned(events, directory)

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, traceback):
        try:
            self.close()
        except BaseException as cleanup:
            if exc is None:
                raise
            if isinstance(exc, Exception) and not isinstance(cleanup, Exception):
                cleanup.add_note("original context failure: " + repr(exc))
                raise cleanup from exc
            exc.add_note("context cleanup failure: " + repr(cleanup))
        return False
