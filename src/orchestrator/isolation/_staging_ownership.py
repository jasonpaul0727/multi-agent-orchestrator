"""Acquisition capabilities and one delegated close path for opaque A1 groups."""
from dataclasses import dataclass
from enum import Enum
import os

from ._staging_a2_contract import AttemptBinding, FailureKind, FailureRecord, NativePhase, OwnerState, Role
from ._staging_contract import _integer, _require
from ._staging_arena import FixtureRoots, SlotArena


class HostCloseState(str, Enum):
    CLOSED = 'CLOSED'
    RETAINED = 'RETAINED'
    POISONED = 'POISONED'


class FDKind(Enum):
    FILE = 'file'
    DIRECTORY = 'directory'
    PIDFD = 'pidfd'
    CONTROL_SOCKET = 'control_socket'
    TERMINAL_SOCKET = 'terminal_socket'


@dataclass(frozen=True, slots=True, eq=False)
class Acquisition:
    """Exact object identity names one acquisition, never a reusable FD number."""
    fd: int
    _owner: object
    _generation: int


@dataclass(slots=True)
class _Record:
    cap: Acquisition
    role: Role
    kind: FDKind
    identity: tuple[int, int] | None
    attempted: bool = False
    closed: bool = False
    group: object = None
    binding: object = None


@dataclass(slots=True)
class _Group:
    object: object
    members: tuple
    attempted: bool = False
    closed: bool = False


class AcquisitionLedger:
    def __init__(self):
        self._records = {}
        self._live = {}
        self._groups = []
        self._generation = 0
        self._debt_phase = None
        self.cause = None
        self.cleanup = None
        self._fixture_roots = None
        self._fixture_caps = None
        self._parents = {}
        self._terminal = None
        self._outside = None

    @property
    def has_debt(self):
        return self._debt_phase is not None

    @property
    def all_closed(self):
        return not self.has_debt and all(record.closed for record in self._records.values())

    @property
    def open_fds(self):
        return tuple(record.cap.fd for record in self._records.values() if not record.closed)

    @property
    def parent(self):
        _require(self._fixture_caps is not None)
        return self._fixture_caps[0]

    def _record(self, cap):
        _require(type(cap) is Acquisition and cap._owner is self and cap in self._records)
        return self._records[cap]

    def require_current(self, cap, *, kind=None, role=None):
        record = self._record(cap)
        _require(not self.has_debt and self._live.get(cap.fd) is cap and not record.attempted
                 and not record.closed and (kind is None or record.kind is kind)
                 and (role is None or record.role is role))
        return record

    def poison_debt(self, phase):
        _require(type(phase) is NativePhase)
        if self._debt_phase is None:
            self._debt_phase = phase

    def record_cause(self, record):
        _require(type(record) is FailureRecord)
        record.__post_init__()
        if self.cause is None:
            self.cause = record

    def record_cleanup(self, role):
        if self.cleanup is None:
            self.cleanup = FailureRecord(role, NativePhase.FD_CLOSE, FailureKind.CLEANUP_UNKNOWN, None)
        return self.cleanup

    def adopt_fd(self, fd, *, role, identity, kind=FDKind.FILE):
        _integer(fd, 2**31 - 1)
        previous = self._live.get(fd)
        _require(previous is None or self._records[previous].closed)
        self._generation += 1
        cap = Acquisition(fd, self, self._generation)
        record = _Record(cap, role, kind, identity)
        self._records[cap] = record
        self._live[fd] = cap
        try:
            _require(type(role) is Role and type(kind) is FDKind)
            if identity is not None:
                self._identity(identity)
        except BaseException:
            self.poison_debt(NativePhase.FD_CLOSE)
            raise
        return cap

    @staticmethod
    def _identity(identity):
        _require(type(identity) is tuple and len(identity) == 2)
        for value in identity:
            _integer(value, 2**64 - 1)

    def bind_identity(self, cap, identity):
        record = self.require_current(cap)
        _require(record.identity is None)
        self._identity(identity)
        record.identity = identity

    def _delegate(self, obj, caps):
        group = _Group(obj, tuple(caps))
        self._groups.append(group)
        for cap in caps:
            record = self.require_current(cap)
            _require(record.group is None)
            record.group = group

    def adopt_returned_arena(self, arena):
        # Retain the opaque object before examining it. Its close is the ONLY
        # path that closes this group's raw descriptors, including uncertainty.
        group = _Group(arena, ())
        self._groups.append(group)
        try:
            _require(type(arena) is SlotArena and not arena._closed)
            caps = []
            for index, fd in enumerate((arena._arena_fd, arena._header_fd, *arena._record_fds)):
                cap = self.adopt_fd(fd, role=Role.HOST, identity=None,
                                    kind=FDKind.DIRECTORY if index == 0 else FDKind.FILE)
                self._records[cap].group = group
                caps.append(cap)
            group.members = tuple(caps)
            for cap in caps:
                info = os.fstat(cap.fd)
                self.bind_identity(cap, (info.st_dev, info.st_ino))
        except BaseException:
            self.poison_debt(NativePhase.REOPEN_ROOTS)
            raise
        return arena

    def bind_fixture_roots(self, roots, caps):
        _require(type(roots) is FixtureRoots and self._fixture_roots is None
                 and type(caps) is tuple and len(caps) == 4)
        for cap, fd, identity in zip(caps, (roots._parent_fd, roots._root_fd, roots.source_fd, roots.arena_fd), roots._identities):
            record = self.require_current(cap, kind=FDKind.DIRECTORY, role=Role.HOST)
            _require(cap.fd == fd and record.identity == identity)
        self._delegate(roots, caps)
        self._fixture_roots, self._fixture_caps = roots, caps

    def require_parent(self, cap):
        _require(self._fixture_caps is not None and cap is self._fixture_caps[0])
        record = self.require_current(cap, kind=FDKind.DIRECTORY, role=Role.HOST)
        _require(record.identity == self._fixture_roots._identities[0])
        info = os.fstat(cap.fd)
        _require((info.st_dev, info.st_ino) == record.identity)
        return cap.fd

    def bind_parent_pidfd(self, cap, *, child, parent_pid, visible_pid):
        _require(type(child) is Role and child is not Role.HOST and child not in self._parents)
        record = self.require_current(cap, kind=FDKind.PIDFD, role=Role(int(child) - 1))
        _integer(parent_pid, 2**31 - 1)
        _integer(visible_pid, 2**31 - 1)
        _require(parent_pid > 0 and (visible_pid == 0 if child is Role.C else visible_pid == parent_pid))
        record.binding = (child, parent_pid, visible_pid)
        self._parents[child] = cap

    def bind_control_socket(self, cap, *, peer, binding):
        record = self.require_current(cap, kind=FDKind.CONTROL_SOCKET, role=Role.C)
        _require(peer is Role.T and type(binding) is AttemptBinding and record.binding is None)
        binding.__post_init__()
        record.binding = (peer, binding)

    def adopt_c_terminal(self, fd, *, source):
        record = self.require_current(source, kind=FDKind.CONTROL_SOCKET, role=Role.C)
        _require(fd == 1 and type(fd) is int and source.fd >= 3 and record.binding is not None
                 and record.identity is not None and self._terminal is None)
        # Called immediately after B's checked dup2. No syscall/Boolean is faked
        # here: this only records the already returned acquisition.
        previous = self._live.get(1)
        if previous is not None:
            old = self._record(previous)
            _require(not old.attempted and not old.closed)
            old.attempted = old.closed = True
        cap = self.adopt_fd(fd, role=Role.C, identity=record.identity, kind=FDKind.TERMINAL_SOCKET)
        self._records[cap].binding = (source, record.binding)
        self._terminal = cap
        return cap

    def bind_outside_directory(self, cap, *, binding):
        record = self.require_current(cap, kind=FDKind.DIRECTORY, role=Role.C)
        _require(self._outside is None and type(binding) is AttemptBinding and record.identity is not None)
        binding.__post_init__()
        record.binding = binding
        self._outside = cap

    def _close_uncertainty(self, role, primary):
        control = None
        try:
            self.poison_debt(NativePhase.FD_CLOSE)
        except BaseException as exc:
            if not isinstance(exc, Exception):
                control = exc
        finally:
            if self._debt_phase is None:
                self._debt_phase = NativePhase.FD_CLOSE
        try:
            self.record_cleanup(role)
        except BaseException as exc:
            if control is None and not isinstance(exc, Exception):
                control = exc
        if not isinstance(primary, Exception):
            raise primary
        if control is not None:
            raise control
        raise primary
    def _close_group(self, group):
        if group.attempted:
            return
        group.attempted = True
        for cap in group.members:
            self._records[cap].attempted = True
        try:
            _require(not group.object._closed)
            for cap in group.members:
                record = self._record(cap)
                _require(self._live.get(cap.fd) is cap)
                info = os.fstat(cap.fd)
                _require(record.identity is not None and (info.st_dev, info.st_ino) == record.identity)
            group.object.close()
            for cap in group.members:
                try:
                    os.fstat(cap.fd)
                except OSError as exc:
                    _require(exc.errno == 9)
                else:
                    _require(False)
            for cap in group.members:
                self._records[cap].closed = True
            group.closed = True
        except BaseException as primary:
            self._close_uncertainty(Role.HOST, primary)

    def close_once(self, cap):
        record = self._record(cap)
        if record.attempted:
            return
        if record.group is not None:
            self._close_group(record.group)
            return
        record.attempted = True
        try:
            _require(self._live.get(cap.fd) is cap)
            if record.identity is not None:
                info = os.fstat(cap.fd)
                _require((info.st_dev, info.st_ino) == record.identity)
            os.close(cap.fd)
            record.closed = True
        except BaseException as primary:
            self._close_uncertainty(record.role, primary)

    def close_all(self, *, primary=None):
        ordinary = control = None
        for cap in reversed(tuple(self._records)):
            try:
                self.close_once(cap)
            except BaseException as exc:
                if isinstance(exc, Exception):
                    if ordinary is None:
                        ordinary = exc
                elif control is None:
                    control = exc
        selected = (primary if primary is not None and not isinstance(primary, Exception)
                    else control if control is not None else primary if primary is not None else ordinary)
        if selected is not None:
            raise selected


class HostState:
    def __init__(self):
        self._entries = [None, None]
        self.busy = False
        self.has_debt = False
        self.lifecycle = OwnerState.BOOTSTRAP
        self._debt_phase = None

    @property
    def entries(self):
        return tuple(self._entries)

    def install_bootstrap(self, owner):
        _require(not self.busy and not self.has_debt and self.lifecycle is OwnerState.BOOTSTRAP
                 and self._entries == [None, None])
        self._entries[0] = owner
        self.busy = True

    def finish_bootstrap(self, owner):
        _require(self.busy and not self.has_debt and self._entries == [owner, None])
        self._entries[0] = None
        self.busy = False
        self.lifecycle = OwnerState.RUNNING

    def poison_debt(self, phase):
        _require(type(phase) is NativePhase)
        if self._debt_phase is None:
            self._debt_phase = phase
        self.has_debt = True
        self.busy = False
        self.lifecycle = OwnerState.POISONED_UNRESERVED_DEBT

    def close_outcome(self):
        if self.busy or self.has_debt or any(entry is not None for entry in self._entries):
            return HostCloseState.RETAINED
        return HostCloseState.CLOSED
