"""Bounded job observation only; caller supplies unauthenticated bus facts.

This module grants no launch, stop, permit, or release authority. A job result
is not a process-exit or cgroup observation. Owner and epoch continuity depend
on the caller monitoring the actual connection without gaps.
"""
from dataclasses import dataclass
from functools import wraps
import re
import threading

_OWNER = re.compile(r"^:[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)+$")
_PATH = re.compile(r"^/org/freedesktop/systemd1/job/([1-9][0-9]{0,9})$")
_UNIT = re.compile(r"^(?:[A-Za-z0-9_.:@-]|\\x[0-9A-Fa-f]{2})+\.(?:service|socket|target|timer|mount|automount|device|swap|path|slice|scope)$")
_RESULT = re.compile(r"^[a-z_]{1,32}$")


def _unit(value):
    return type(value) is str and len(value) <= 255 and bool(_UNIT.fullmatch(value))


def _identity(job_id, path):
    if type(job_id) is not int or not 1 <= job_id <= 2**32 - 1 or type(path) is not str:
        return False
    match = _PATH.fullmatch(path)
    return match is not None and match.group(1) == str(job_id)


def _owner(owner, epoch):
    return (type(owner) is str and len(owner) <= 255 and _OWNER.fullmatch(owner) is not None
            and type(epoch) is int and 0 < epoch <= 2**63 - 1)


def _locked(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper


@dataclass(frozen=True, slots=True)
class JobObservation:
    owner: str
    epoch: int
    job_id: int
    job_path: str
    unit: str
    result: str


class JobCorrelation:
    # ASCII field-byte budget is not a bound on Python memory.
    # Same-unit competing jobs conservatively hold.
    def __init__(self, unit, *, max_events=64, max_bytes=16384):
        if (not _unit(unit) or type(max_events) is not int or not 1 <= max_events <= 256
            or type(max_bytes) is not int or not 1 <= max_bytes <= 65536):
            raise ValueError("invalid correlation limits")
        self._lock = threading.RLock()
        self._unit = unit
        self._max_events = max_events
        self._max_bytes = max_bytes
        self._owner = None
        self._epoch = None
        self._dispatched = False
        self._reply = None
        self._events = []
        self._bytes = 0
        self._observation = None
        self._unknown = False

    @property
    @_locked
    def unknown(self):
        return self._unknown

    @property
    @_locked
    def observation(self):
        return None if self.unknown else self._observation

    def _fail(self):
        self._unknown = True
        self._observation = None
        self._events.clear()
        self._bytes = 0

    def _same_connection(self, owner, epoch):
        if self.unknown:
            return False
        if not _owner(owner, epoch) or (owner, epoch) != (self._owner, self._epoch):
            self._fail()
            return False
        return True

    @_locked
    def subscribe(self, owner, epoch):
        if self.unknown:
            return
        if self._owner is not None or not _owner(owner, epoch):
            self._fail()
            return
        self._owner, self._epoch = owner, epoch

    @_locked
    def dispatch(self, owner, epoch):
        if not self._same_connection(owner, epoch):
            return
        if self._dispatched:
            self._fail()
            return
        self._dispatched = True

    @_locked
    def reply(self, owner, epoch, job_id, path, unit):
        if not self._same_connection(owner, epoch):
            return
        if not self._dispatched or not _identity(job_id, path) or not _unit(unit) or unit != self._unit:
            self._fail()
            return
        current = (job_id, path, unit)
        if self._reply is not None and self._reply != current:
            self._fail()
            return
        self._reply = current
        self._reconcile()

    @_locked
    def signal(self, owner, epoch, job_id, path, unit, result):
        if not self._same_connection(owner, epoch):
            return
        if (not _identity(job_id, path) or not _unit(unit)
            or type(result) is not str or not _RESULT.fullmatch(result)):
            self._fail()
            return
        if not self._dispatched:
            if unit == self._unit:
                self._fail()
            return
        event = (job_id, path, unit, result)
        if event in self._events:
            return
        if any(previous[0] == job_id or previous[1] == path
               or (unit == self._unit and previous[2] == self._unit) for previous in self._events):
            self._fail()
            return
        size = 8 + len(path) + len(unit) + len(result)
        if len(self._events) >= self._max_events or self._bytes + size > self._max_bytes:
            self._fail()
            return
        self._events.append(event)
        self._bytes += size
        self._reconcile()

    def _reconcile(self):
        if self._reply is None:
            return
        job_id, path, unit = self._reply
        for event in self._events:
            if event[0] == job_id or event[1] == path or event[2] == unit:
                if event != (job_id, path, unit, "done"):
                    self._fail()
                    return
                self._observation = JobObservation(self._owner, self._epoch, job_id, path, unit, "done")

    @_locked
    def disconnect(self):
        self._fail()

    @_locked
    def timeout(self):
        self._fail()
