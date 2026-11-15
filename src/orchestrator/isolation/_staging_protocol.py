"""Finite protocol observations. Neither a cursor nor a frame proves OS closure.

B2 must gather registration, exit, targeted-reap and closure facts independently
in its exact retained owner. The private fact view below is only the interface
to those facts; constructing it cannot mint stop, exit or release authority.
"""
from dataclasses import dataclass
from enum import Enum
import fcntl
import os
import select
import socket
import struct
import time

from . import _staging_a2_contract as a
from ._staging_contract import ContractError, _integer, _require
from ._staging_ownership import AcquisitionLedger, FDKind, _Group


class ProtocolFamily(Enum):
    NATIVE = 'native'
    FIXTURE = 'pre_unshare'


class ProtocolClock:
    def __init__(self, started_ns):
        _integer(started_ns)
        self.started_ns = started_ns
        self.deadline_ns = started_ns + 30_000_000_000
        self._cleanup_end = None
        self._budget = None

    @property
    def cleanup_end(self):
        return self._budget._cleanup_end() if self._budget is not None else self._cleanup_end

    def _bind_budget(self, budget):
        _require(type(budget) is ProtocolBudget and self._budget is None and self._cleanup_end is None)
        budget._bind_clock(self.started_ns)
        self._budget = budget

    def begin_cleanup(self, now_ns):
        _integer(now_ns)
        _require(now_ns >= self.started_ns)
        if self._budget is not None:
            return self._budget._begin_cleanup(self.started_ns, now_ns)
        if self._cleanup_end is None:
            self._cleanup_end = min(now_ns + 5_000_000_000, self.deadline_ns + 5_000_000_000)
        return self.cleanup_end

    def check(self, now_ns, *, terminal=False):
        _integer(now_ns)
        _require(type(terminal) is bool and now_ns >= self.started_ns)
        end = self.cleanup_end if terminal else self.deadline_ns
        _require(end is not None and now_ns < end)


class ProtocolBudget:
    """One inherited regular-file counter; no per-channel allowance.

    The entire 2048-byte terminal domain is reserved at creation: eight fixed
    frame slots and 512 history bytes. A claimed slot is charged once, including
    an uncertain send; receiving never charges. An exceeded domain stays failed.
    """
    _STRUCT = struct.Struct('!8s5Q')

    def __init__(self, owner, cap):
        _require(type(owner) is AcquisitionLedger)
        owner.require_current(cap, kind=FDKind.FILE)
        self._owner, self.acquisition = owner, cap

    @classmethod
    def create(cls, owner, cap):
        result = cls(owner, cap)
        try:
            fd = result._fd()
            _require(os.fstat(fd).st_size == 0)
            raw = result._STRUCT.pack(b'SNFBUD2\0', 0, 0, 0, 0, 0)
            _require(os.pwrite(fd, raw, 0) == len(raw))
        except BaseException as primary:
            errors = []
            try:
                _debt(owner, a.NativePhase.CONTROL_IO)
            except BaseException as cleanup:
                errors.append(cleanup)
            raise _selected(primary, errors)
        return result

    @classmethod
    def attach(cls, owner, cap):
        result = cls(owner, cap)
        result.snapshot()
        return result

    def _fd(self):
        record = self._owner.require_current(self.acquisition, kind=FDKind.FILE)
        info = os.fstat(self.acquisition.fd)
        _require(record.identity == (info.st_dev, info.st_ino))
        return self.acquisition.fd

    def _update(self, ordinary=0, slot=None, history=0, started_ns=None, cleanup_now=None):
        fd, primary, errors, result = None, None, [], None
        # POSIX record locks coordinate separate processes sharing the inherited
        # description (flock would share that description's lock ownership).
        try:
            fd = self._fd()
            # Contention refuses rather than waiting outside the absolute
            # protocol deadline. There is no lock-acquisition retry window.
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, self._STRUCT.size, 0)
            _require(os.fstat(fd).st_size == self._STRUCT.size)
            raw = os.pread(fd, self._STRUCT.size, 0)
            _require(len(raw) == self._STRUCT.size)
            magic, used, mask, hist, started_tag, cleanup_end = self._STRUCT.unpack(raw)
            _require(magic == b'SNFBUD2\0' and used <= 14337 and mask <= 255 and hist <= 513)
            _require((started_tag == 0 and cleanup_end == 0)
                     or (started_tag > 0 and (cleanup_end == 0 or started_tag - 1 <= cleanup_end <= started_tag - 1 + 35_000_000_000)))
            if started_ns is not None:
                _require(started_tag in (0, started_ns + 1))
                started_tag = started_ns + 1
            if cleanup_now is not None:
                _require(started_tag > 0 and cleanup_now >= started_tag - 1)
                if cleanup_end == 0:
                    cleanup_end = min(cleanup_now + 5_000_000_000, started_tag - 1 + 35_000_000_000)
            if slot is not None:
                _integer(slot, 7)
                _require(not mask & (1 << slot))
                mask |= 1 << slot
            used, hist = min(14337, used + ordinary), min(513, hist + history)
            if ordinary or history or slot is not None or started_ns is not None or cleanup_now is not None:
                updated = self._STRUCT.pack(magic, used, mask, hist, started_tag, cleanup_end)
                _require(os.pwrite(fd, updated, 0) == len(updated))
            result = used, mask, hist, started_tag, cleanup_end
        except BaseException as exc:
            primary = exc
        if fd is not None:
            try:
                fcntl.lockf(fd, fcntl.LOCK_UN, self._STRUCT.size, 0)
            except BaseException as cleanup:
                errors.append(cleanup)
        if primary is not None or errors:
            try:
                _debt(self._owner, a.NativePhase.CONTROL_IO)
            except BaseException as cleanup:
                errors.append(cleanup)
            raise _selected(primary, errors)
        return result

    def snapshot(self):
        return self._update()[:3]

    def _bind_clock(self, started_ns):
        self._update(started_ns=started_ns)

    def _begin_cleanup(self, started_ns, now_ns):
        return self._update(started_ns=started_ns, cleanup_now=now_ns)[4]

    def _cleanup_end(self):
        return self._update()[4] or None

    def charge_ordinary(self, size):
        _integer(size, 2**64 - 1)
        return self._update(ordinary=size)[0] <= 14336

    def claim_terminal(self, slot):
        self._update(slot=slot)

    def charge_history(self, size):
        _integer(size, 2**64 - 1)
        return self._update(history=size)[2] <= 512


@dataclass(frozen=True, slots=True)
class _Step:
    op: a.Opcode
    src: a.Role
    dst: a.Role
    subject: a.Role | None = None


H, R, S, T, C = a.Role
O = a.Opcode
_ORDINARY = (
    _Step(O.START, H, R), _Step(O.GROUPS, H, R), _Step(O.HELLO, R, H, R),
    _Step(O.ACK_REGISTER, H, R, R), _Step(O.HELLO, S, R, S),
    _Step(O.IDENTITY, R, H, S), _Step(O.ACK_REGISTER, H, R, S),
    _Step(O.ACK_REGISTER, R, S, S), _Step(O.HELLO, T, S, T),
    _Step(O.IDENTITY, S, R, T), _Step(O.IDENTITY, R, H, T),
    _Step(O.ACK_REGISTER, H, R, T), _Step(O.ACK_REGISTER, R, S, T),
    _Step(O.ACK_REGISTER, S, T, T), _Step(O.GO_NATIVE, H, R),
    _Step(O.GO_NATIVE, R, S), _Step(O.GO_NATIVE, S, T),
    _Step(O.HELLO, C, T, C), _Step(O.IDENTITY, T, S, C),
    _Step(O.IDENTITY, S, R, C), _Step(O.IDENTITY, R, H, C),
    _Step(O.ACK_REGISTER, H, R, C), _Step(O.ACK_REGISTER, R, S, C),
    _Step(O.ACK_REGISTER, S, T, C), _Step(O.ACK_REGISTER, T, C, C),
    _Step(O.NS_IDENTITY, C, T), _Step(O.GO_CLOSE_RANGE, T, C),
)
_TERMINAL = {
    a.TerminalFamily.N_C: (
        _Step(O.EXIT_NOTE, C, T, C), _Step(O.REAP_ACK, T, S, C),
        _Step(O.REAP_ACK, S, R, C), _Step(O.REAP_ACK, S, R, T),
        _Step(O.RESULT_CLOSED, R, H), _Step(O.PREFIX_CLOSED, R, H)),
    a.TerminalFamily.R_0: (_Step(O.RESULT_CLOSED, R, H), _Step(O.PREFIX_CLOSED, R, H)),
    a.TerminalFamily.R_1: (_Step(O.RESULT_CLOSED, R, H), _Step(O.PREFIX_CLOSED, R, H)),
    a.TerminalFamily.S_0: (_Step(O.EXIT_NOTE, S, R, S), _Step(O.RESULT_CLOSED, R, H), _Step(O.PREFIX_CLOSED, R, H)),
    a.TerminalFamily.T_0: (_Step(O.EXIT_NOTE, T, S, T), _Step(O.REAP_ACK, S, R, T),
                         _Step(O.RESULT_CLOSED, R, H), _Step(O.PREFIX_CLOSED, R, H)),
    a.TerminalFamily.U: (_Step(O.RESULT_PARKED, R, H),),
}
_FIXTURE_TERMINAL = (
    _Step(O.RESULT_PARKED, R, H), _Step(O.CONVERGE_PREUNSHARE, H, R),
    _Step(O.CONVERGE_PREUNSHARE, R, S), _Step(O.CONVERGE_PREUNSHARE, S, T),
    _Step(O.EXIT_NOTE, T, S, T), _Step(O.REAP_ACK, S, R, T),
    _Step(O.PREFIX_CLOSED, R, H),
)


@dataclass(frozen=True, slots=True)
class _Peer:
    pid: int
    uid: int
    gid: int
    parent: a.Role
    pidfd: object


@dataclass(frozen=True, slots=True)
class _LocalIdentity:
    role: a.Role
    pid: int
    uid: int
    gid: int

    @property
    def parent(self):
        _require(self.role is not H)
        return a.Role(int(self.role) - 1)


class _OwnerFacts:
    """Private inert view of ONE exact owner, never a decoded proof factory.

    Only the later trusted graph may populate these finite observations after
    its own OS checks. Cursor updates do not populate any of them. In particular
    no receive adds a registration, reap, exit, FD closure or namespace check.
    """
    def __init__(self, owner):
        _require(type(owner) is AcquisitionLedger)
        self.owner = owner
        self.peers = {}
        self.endpoints = {}
        self.credentials = {}
        self.prefix = 0
        self.exited = set()
        self.reaped = set()
        self.closed = set()
        self.namespace_checked = False
        self.ambiguous = False
        self.parked = False
        self.terminal_count = 0
        self.state = a.OwnerState.RUNNING
        self.shared_budget = None
        self.maps_verified = False
        self.local = None
        self.created = set()
        self.first_cause = None
        self.first_cleanup = None
        self.targeted_reaps = {}

    def capture_local_failures(self):
        if self.first_cause is None:
            self.first_cause = self.owner.cause
        if self.first_cleanup is None:
            self.first_cleanup = self.owner.cleanup

    def capture_authenticated_failures(self, cause, cleanup):
        # At this event, any already recorded local failure preceded it. Once
        # an authenticated record wins, a later local record cannot displace it.
        self.capture_local_failures()
        if self.first_cause is None:
            self.first_cause = cause
        if self.first_cleanup is None:
            self.first_cleanup = cleanup

    def targeted_reap(self, subject, parent):
        observation = self.targeted_reaps.get(subject)
        _require(type(observation) is tuple and len(observation) == 5)
        target, pid, kind, status, closed = observation
        peer = self.known(subject)
        _require(target is peer.pidfd and peer.parent is parent and type(pid) is int and pid == peer.pid)
        _require(type(kind) is a.ExitKind and type(status) is int and -(2**31) <= status < 2**31)
        _integer(closed, 2**64 - 1)
        return pid, kind, status, closed

    def nonterminal_closed(self, peer):
        tail = self.endpoints.get(peer)
        self.owner.require_current(tail, kind=FDKind.CONTROL_SOCKET)
        allowed = (tail, self.shared_budget)
        return not self.owner.has_debt and all(record.closed or record.cap in allowed
                 for record in self.owner._records.values())

    def peer(self, role):
        _require(type(role) is a.Role and role in self.peers)
        peer = self.peers[role]
        _require(type(peer) is _Peer)
        # A PIDFD's role identifies its exact target; the parent edge and the
        # process performing a later cleanup are separate facts.
        record = self.owner.require_current(peer.pidfd, kind=FDKind.PIDFD, role=role)
        _require(record.identity is not None)
        _require(peer.parent is a.Role(int(role) - 1) and type(peer.pid) is int and peer.pid > 0)
        return peer

    def known(self, role):
        _require(type(role) is a.Role and role in self.peers)
        peer = self.peers[role]
        _require(type(peer) is _Peer)
        record = self.owner._record(peer.pidfd)
        _require(record.kind is FDKind.PIDFD and record.role is role and record.identity is not None)
        if not record.closed:
            self.owner.require_current(peer.pidfd, kind=FDKind.PIDFD, role=role)
        else:
            _require(record.attempted)
        return peer

    def identity(self, role, *, current=True):
        if type(self.local) is _LocalIdentity and role is self.local.role:
            _require(type(self.local.pid) is int and self.local.pid > 0
                     and type(self.local.uid) is int and type(self.local.gid) is int)
            return self.local
        return self.peer(role) if current else self.known(role)


@dataclass(frozen=True, slots=True)
class Transition:
    opcode: a.Opcode
    subject: a.Role | None
    terminal_slot: int | None
    pending_identity: a.IdentityPayload | None = None
    # This is deliberately a protocol observation, with no closure/release bit.


class ProtocolCursor:
    def __init__(self, binding, family, role):
        _require(type(binding) is a.AttemptBinding and type(family) is ProtocolFamily and type(role) is a.Role)
        binding.__post_init__()
        self.binding, self.family, self.role = binding, family, role
        self._facts = None
        self.clock = None
        self._ordinary = 0
        self._terminal = None
        self._terminal_position = 0
        self._sequences = {}
        self._uncertain = set()
        self._result = False
        self._relay = None
        self._claims = {}
        self._cause = self._cleanup = None
        self._terminal_offset = 0
        self._terminal_highest = 0
        self._pending = {}
        self._progress = 0
        self._terminal_prefix = 0
        self._hello = {}
        self._acknowledged = {}

    def _bind_owner(self, facts, clock):
        _require(self._facts is None and type(facts) is _OwnerFacts and type(clock) is ProtocolClock)
        self._facts, self.clock = facts, clock

    def _local_steps(self, steps):
        return tuple((i, step) for i, step in enumerate(steps) if self.role in (step.src, step.dst))

    def _select_terminal(self, family, *, now_ns):
        facts = self._facts
        _require(facts is not None and type(family) is a.TerminalFamily)
        if family is not a.TerminalFamily.U:
            _require(self._terminal is None and not facts.ambiguous)
        else:
            _require(self._terminal is not a.TerminalFamily.U)
        maximum = 14 if self.family is ProtocolFamily.FIXTURE else 27
        if self.family is ProtocolFamily.FIXTURE and family is not a.TerminalFamily.U:
            _require(family is a.TerminalFamily.T_0)
            prefixes = (14,)
            steps = _FIXTURE_TERMINAL
        else:
            allowed = {a.TerminalFamily.N_C: (27,), a.TerminalFamily.R_0: (2,),
                       a.TerminalFamily.R_1: (3, 4), a.TerminalFamily.S_0: (8,),
                       a.TerminalFamily.T_0: (14, 15, 16, 17), a.TerminalFamily.U: tuple(range(maximum + 1))}
            prefixes = allowed[family]
            steps = _TERMINAL[family]
        matches = tuple(prefix for prefix in prefixes
                        if self._ordinary == len(self._local_steps(_ORDINARY[:prefix])))
        _require(bool(matches))
        prefix = max(matches)
        if family is not a.TerminalFamily.U:
            last = {a.TerminalFamily.R_0: R, a.TerminalFamily.R_1: R,
                    a.TerminalFamily.S_0: S, a.TerminalFamily.T_0: T,
                    a.TerminalFamily.N_C: C}[family]
            actual = {role for role in facts.peers if role > self.role} | facts.created
            expected = {a.Role(i) for i in range(int(self.role) + 1, int(last) + 1)}
            _require(actual == expected)
            for role in facts.peers:
                facts.known(role)
        if family is a.TerminalFamily.U:
            # One remaining report, only if no prior RESULT. An ambiguous R->H
            # send never resets its counter or becomes another report.
            steps = () if prefix < 2 or self._result or (R, H) in self._uncertain else steps
            _integer(facts.terminal_count, 8)
            _require(facts.terminal_count >= self._terminal_highest and (not steps or facts.terminal_count < 8))
            self._terminal_offset = facts.terminal_count
        self.clock.begin_cleanup(now_ns)
        self._terminal, self._terminal_steps = family, steps
        self._terminal_prefix = prefix
        self._terminal_position = 0

    def _incoming_terminal(self, frame, now_ns):
        if frame.dst is not self.role or frame.opcode not in (
                O.EXIT_NOTE, O.REAP_ACK, O.RESULT_CLOSED, O.RESULT_PARKED,
                O.PREFIX_CLOSED, O.CONVERGE_PREUNSHARE):
            return
        if self._terminal is not None:
            local = self._local_steps(self._terminal_steps)
            if self._terminal_position < len(local):
                step = local[self._terminal_position][1]
                if (frame.opcode, frame.src, frame.dst) == (step.op, step.src, step.dst):
                    return
            _require(frame.opcode is O.RESULT_PARKED and not self._result
                     and self._terminal is not a.TerminalFamily.U)
        families = ((a.TerminalFamily.T_0, a.TerminalFamily.U) if self.family is ProtocolFamily.FIXTURE
                    else tuple(a.TerminalFamily))
        for family in families:
            steps = (_FIXTURE_TERMINAL if self.family is ProtocolFamily.FIXTURE and family is a.TerminalFamily.T_0
                     else _TERMINAL[family])
            local = self._local_steps(steps)
            if not local:
                continue
            step = local[0][1]
            if (frame.opcode, frame.src, frame.dst) != (step.op, step.src, step.dst):
                continue
            if self.family is ProtocolFamily.FIXTURE and family is a.TerminalFamily.T_0:
                prefixes = (14,)
            else:
                prefixes = {a.TerminalFamily.N_C: (27,), a.TerminalFamily.R_0: (2,),
                            a.TerminalFamily.R_1: (3, 4), a.TerminalFamily.S_0: (8,),
                            a.TerminalFamily.T_0: (14, 15, 16, 17), a.TerminalFamily.U: tuple(range(2, 28))}[family]
            matches = [prefix for prefix in prefixes if prefix <= (14 if self.family is ProtocolFamily.FIXTURE else 27)
                       and self._ordinary == len(self._local_steps(_ORDINARY[:prefix]))]
            if not matches:
                continue
            # This is an authenticated remote observation, not an assertion of
            # receiver-side reaps/closure or knowledge of an unseen decision.
            offset = 0
            if family is a.TerminalFamily.U:
                offset = max(self._terminal_highest, self._facts.terminal_count)
                if self.clock._budget is not None:
                    mask = self.clock._budget.snapshot()[1]
                    _require(mask > 0 and mask == (1 << mask.bit_length()) - 1)
                    offset = mask.bit_length() - 1
                _require(offset < 8 and (R, H) not in self._uncertain)
            self.clock.begin_cleanup(now_ns)
            self._terminal, self._terminal_steps = family, steps
            self._terminal_position, self._terminal_offset = 0, offset
            self._terminal_prefix = max(matches)
            return
        _require(False)

    def mark_uncertain(self, src, dst):
        _require(type(src) is a.Role and type(dst) is a.Role and self.role in (src, dst))
        self._uncertain.add((src, dst))
        if self._facts is not None:
            self._facts.ambiguous = True

    def _grant_eligible(self, frame):
        facts = self._facts
        if self.clock._budget is not None:
            shared = self.clock._budget._update()
            _require(shared[0] <= 14336 and shared[4] == 0)
        else:
            _require(self.clock.cleanup_end is None)
        if frame.opcode is O.GO_NATIVE:
            _require(C not in facts.peers and C not in facts.created)
        required = (R, S, T) if frame.opcode is O.GO_NATIVE else (C,)
        for role in required:
            if role >= self.role:
                facts.identity(role)
        if frame.opcode is O.GO_CLOSE_RANGE:
            _require(facts.namespace_checked)
        # Shared I/O is complete. Re-read local eligibility after that last
        # fallible preparation and before the caller's final time sample.
        _require(not facts.ambiguous and not facts.parked and facts.state is a.OwnerState.RUNNING
                 and not facts.owner.has_debt and facts.owner.cause is None and facts.owner.cleanup is None)

    def _closed_producer_family(self):
        facts = self._facts
        last = {a.TerminalFamily.N_C: C, a.TerminalFamily.R_0: R, a.TerminalFamily.R_1: R,
                a.TerminalFamily.S_0: S, a.TerminalFamily.T_0: T}.get(self._terminal)
        _require(last is not None and not facts.ambiguous)
        expected = {a.Role(i) for i in range(int(self.role) + 1, int(last) + 1)}
        registered = {role for role in facts.peers if role > self.role}
        _require(registered == expected and registered | facts.created == expected)
        for role in expected:
            facts.known(role)

    def _message_credentials(self, frame):
        if frame.src is H:
            if self.role is H:
                local = self._facts.identity(H)
                return (local.pid, local.uid, local.gid)
            return self._facts.credentials.get(H)
        current = self._terminal is None and frame.opcode not in (
            O.EXIT_NOTE, O.REAP_ACK, O.RESULT_PARKED, O.RESULT_CLOSED, O.PREFIX_CLOSED)
        peer = self._facts.identity(frame.src, current=current)
        if (frame.src, frame.dst) == (T, C):
            return (0, 0, 0)
        if (frame.src, frame.dst) == (C, T):
            return (peer.pid, 0, 0)
        if (frame.src, frame.dst) == (S, T) and frame.opcode is O.ACK_REGISTER:
            if frame.payload.subject is C:
                _require(self.role is not T or self._facts.maps_verified)
                return (peer.pid, 0, 0)
            _require(self.role is not T or not self._facts.maps_verified)
        return (peer.pid, peer.uid, peer.gid)

    def _preview(self, frame, credentials, now_ns):
        _require(type(frame) is a.Frame and self._facts is not None and self.clock is not None)
        frame.__post_init__()
        _require(frame.binding == self.binding and self.role in (frame.src, frame.dst))
        edge = (frame.src, frame.dst)
        _require(edge not in self._uncertain and frame.sequence == self._sequences.get(edge, 0))
        _require(frame.sequence < 8)
        facts = self._facts
        _require(not facts.owner.has_debt)
        _require(type(credentials) is tuple and len(credentials) == 3
                 and all(type(item) is int and item >= 0 for item in credentials))
        _require(credentials == self._message_credentials(frame))
        other = frame.dst if frame.src is self.role else frame.src
        endpoint = facts.endpoints.get(other)
        kind = FDKind.TERMINAL_SOCKET if endpoint is facts.owner._terminal else FDKind.CONTROL_SOCKET
        record = facts.owner.require_current(endpoint, kind=kind, role=self.role)
        if kind is FDKind.TERMINAL_SOCKET:
            _require(self.role is C and other is T and endpoint.fd == 1
                     and record.binding is not None and record.binding[1] == (T, self.binding)
                     and facts.owner._record(record.binding[0]).identity == record.identity)
        if frame.src is not H:
            facts.identity(frame.src, current=self._terminal is None and frame.opcode not in (
                O.EXIT_NOTE, O.REAP_ACK, O.RESULT_PARKED, O.RESULT_CLOSED, O.PREFIX_CLOSED))
        if credentials[0] == 0:
            _require((frame.src, frame.dst) == (T, C))
            if self.role is C:
                parent = facts.owner._parents.get(C)
                record = facts.owner.require_current(parent, kind=FDKind.PIDFD, role=T)
                _require(record.binding is not None and record.binding[0] is C
                         and record.binding[1] == facts.known(T).pid and record.binding[2] == 0)
        self._incoming_terminal(frame, now_ns)
        terminal = self._terminal is not None
        self.clock.check(now_ns, terminal=terminal)
        steps = self._terminal_steps if terminal else _ORDINARY[:14 if self.family is ProtocolFamily.FIXTURE else 27]
        local = self._local_steps(steps)
        position = self._terminal_position if terminal else self._ordinary
        _require(position < len(local))
        slot, step = local[position]
        _require((frame.opcode, frame.src, frame.dst) == (step.op, step.src, step.dst))
        payload = frame.payload
        cause = getattr(payload, 'cause', None)
        if cause is not None and cause.phase in (
                a.NativePhase.UNSHARE, a.NativePhase.OPEN_UID_MAP, a.NativePhase.WRITE_UID_MAP,
                a.NativePhase.OPEN_SETGROUPS, a.NativePhase.WRITE_SETGROUPS,
                a.NativePhase.OPEN_GID_MAP, a.NativePhase.WRITE_GID_MAP):
            _require(self.family is ProtocolFamily.NATIVE and max(self._progress, self._terminal_prefix) >= 17)
        pending = None
        if step.subject is not None:
            _require(payload.subject is step.subject)
            if frame.opcode is O.IDENTITY and frame.dst is self.role:
                _require((payload.uid, payload.gid) == facts.credentials[H][1:])
                pending = payload
            if step.subject in facts.peers or pending is None:
                peer = facts.identity(step.subject, current=not terminal)
                _require(payload.pid == peer.pid and payload.parent is peer.parent)
                if type(payload) is a.IdentityPayload:
                    _require((payload.uid, payload.gid) == (peer.uid, peer.gid))
            if frame.opcode in (O.IDENTITY, O.ACK_REGISTER) and step.subject in self._pending:
                identity = self._pending[step.subject]
                _require(payload.pid == identity.pid and payload.parent is identity.parent)
                registered = facts.identity(step.subject)
                _require((registered.uid, registered.gid) == (identity.uid, identity.gid))
                if type(payload) is a.IdentityPayload:
                    _require(payload == identity)
            if frame.opcode is O.ACK_REGISTER:
                hello = self._hello.get(step.subject)
                _require(hello is None or payload == hello)
                previous = self._acknowledged.get(step.subject)
                _require(previous is None or payload == previous)
        if frame.opcode is O.START:
            _require(payload.started_ns == self.clock.started_ns and payload.deadline_ns == self.clock.deadline_ns
                     and (payload.uid, payload.gid) == credentials[1:])
        if frame.opcode in (O.START, O.GROUPS):
            if self.role is H:
                facts.peer(R)
            else:
                parent = facts.owner._parents.get(R)
                record = facts.owner.require_current(parent, kind=FDKind.PIDFD, role=H)
                _require(record.identity is not None and record.binding == (R, credentials[0], credentials[0]))
        if frame.opcode in (O.GO_NATIVE, O.GO_CLOSE_RANGE):
            self._grant_eligible(frame)
        if frame.opcode is O.NS_IDENTITY:
            _require(payload.pid == 1 and payload.ppid == 0)
            facts.identity(C)
        if frame.opcode is O.CONVERGE_PREUNSHARE:
            _require(self.family is ProtocolFamily.FIXTURE and facts.parked and not facts.ambiguous)
        if frame.opcode is O.REAP_ACK:
            if frame.src is self.role:
                _require(payload.subject in (facts.reaped if frame.src is payload.parent else facts.exited))
                if frame.src is payload.parent:
                    _require((payload.pid, payload.exit_kind, payload.status, payload.closed_fds) ==
                             facts.targeted_reap(payload.subject, self.role))
                previous = self._claims.get(payload.subject)
                if previous is not None and frame.src is not payload.parent:
                    _require(payload == previous)
                else:
                    if previous is not None:
                        _require((payload.subject, payload.parent, payload.pid) ==
                                 (previous.subject, previous.parent, previous.pid))
                    # An exit note is a tail claim. The actual targeted-wait
                    # kind/status may differ; it cannot invent a native cause
                    # or erase the authenticated origin/first cleanup.
                    for name, history in (('cause', self._cause), ('cleanup', self._cleanup)):
                        value = getattr(payload, name)
                        prior = getattr(previous, name) if previous is not None else None
                        if prior is not None:
                            _require(value == prior)
                        else:
                            _require(value is None or value == history or value == getattr(facts.owner, name))
            else:
                for value in (payload.cause, payload.cleanup):
                    if value is not None:
                        _require(value.role is frame.src or value.role >= payload.subject and value.role in facts.peers)
            if self.role is S and (frame.src, frame.dst, payload.subject) == (S, R, C):
                _require(self._relay is not None and payload == self._relay)
            if (frame.src, frame.dst, payload.subject) == (S, R, C):
                _require(all(value is None or value.role in (T, C) for value in (payload.cause, payload.cleanup)))
        if frame.opcode is O.EXIT_NOTE:
            _require(all(record is None or record.role is frame.src for record in (payload.cause, payload.cleanup)))
            if frame.src is self.role:
                _require(payload.cause == facts.owner.cause and payload.cleanup == facts.owner.cleanup)
        if frame.src is self.role and self.role in (S, T) and (
                frame.opcode is O.EXIT_NOTE or frame.opcode is O.REAP_ACK and payload.parent is self.role):
            _require(facts.nonterminal_closed(a.Role(int(self.role) - 1)))
        if frame.opcode in (O.RESULT_PARKED, O.RESULT_CLOSED):
            _require(not self._result)
        if frame.opcode is O.RESULT_PARKED:
            _require(payload.state is a.OwnerState.PARKED_UNKNOWN)
            if frame.src is self.role:
                _require(facts.parked and facts.state is a.OwnerState.PARKED_UNKNOWN)
        if frame.opcode in (O.RESULT_PARKED, O.RESULT_CLOSED, O.PREFIX_CLOSED):
            if frame.src is self.role:
                facts.capture_local_failures()
                _require(payload.cause == facts.first_cause and payload.cleanup == facts.first_cleanup)
            else:
                _require((self._cause is None or payload.cause == self._cause)
                         and (self._cleanup is None or payload.cleanup == self._cleanup))
        if frame.opcode in (O.RESULT_CLOSED, O.PREFIX_CLOSED):
            bits = sum(1 << int(role) for role in facts.peers if role in (S, T, C))
            _require(payload.registered == bits)
            if frame.src is self.role:
                self._closed_producer_family()
                _require(all(role in facts.exited for role in facts.peers if role in (S, T, C)))
                _require(not facts.ambiguous and all(role in facts.reaped and role in facts.closed
                         for role in facts.peers if role in (S, T, C)))
                _require(facts.nonterminal_closed(H))
        return Transition(frame.opcode, step.subject, slot + self._terminal_offset if terminal else None, pending)

    def _commit(self, frame, transition):
        edge = (frame.src, frame.dst)
        if transition.pending_identity is not None:
            self._pending[transition.subject] = transition.pending_identity
        if frame.opcode is O.HELLO:
            self._hello[frame.payload.subject] = frame.payload
        if frame.opcode is O.ACK_REGISTER:
            self._acknowledged[frame.payload.subject] = frame.payload
        self._sequences[edge] = frame.sequence + 1
        if transition.terminal_slot is None:
            local = self._local_steps(_ORDINARY[:14 if self.family is ProtocolFamily.FIXTURE else 27])
            self._progress = local[self._ordinary][0] + 1
            self._ordinary += 1
        else:
            self._terminal_position += 1
            self._terminal_highest = max(self._terminal_highest, transition.terminal_slot + 1)
        if frame.opcode in (O.RESULT_PARKED, O.RESULT_CLOSED):
            self._result = True
        if frame.opcode is O.REAP_ACK and frame.payload.subject is C:
            self._relay = frame.payload
        if frame.opcode in (O.EXIT_NOTE, O.REAP_ACK):
            self._claims[frame.payload.subject] = frame.payload
        if frame.opcode in (O.EXIT_NOTE, O.REAP_ACK, O.RESULT_PARKED, O.RESULT_CLOSED, O.PREFIX_CLOSED):
            self._facts.capture_authenticated_failures(frame.payload.cause, frame.payload.cleanup)
            if self._cause is None:
                self._cause = frame.payload.cause
            if self._cleanup is None:
                self._cleanup = frame.payload.cleanup

    def accept(self, frame, credentials, *, now_ns):
        previous = (self._terminal, getattr(self, '_terminal_steps', ()), self._terminal_position,
                    self._terminal_offset, self._terminal_prefix)
        try:
            transition = self._preview(frame, credentials, now_ns)
        except BaseException:
            (self._terminal, self._terminal_steps, self._terminal_position,
             self._terminal_offset, self._terminal_prefix) = previous
            raise
        self._commit(frame, transition)
        return transition


def _selected(primary, cleanup):
    if primary is not None and not isinstance(primary, Exception):
        return primary
    for exc in cleanup:
        if not isinstance(exc, Exception):
            return exc
    return primary if primary is not None else (cleanup[0] if cleanup else None)


def _active(first, later):
    """First active control wins; otherwise preserve the first ordinary fault."""
    if first is None:
        return later
    if not isinstance(first, Exception):
        return first
    return later if not isinstance(later, Exception) else first


def _debt(owner, phase):
    try:
        owner.poison_debt(phase)
    finally:
        if owner._debt_phase is None:
            owner._debt_phase = phase


def _adopt_socket(owner, sock, role):
    _require(type(owner) is AcquisitionLedger and type(sock) is socket.socket and type(role) is a.Role)
    # Retain the already returned wrapper BEFORE fileno/fstat/socket validation.
    # Once delegated it is the sole raw close path, including uncertain close.
    group = _Group(sock, (), actor=role)
    owner._groups.append(group)
    generation = owner._generation
    try:
        cap = owner.adopt_fd(sock.fileno(), role=role, identity=None, kind=FDKind.CONTROL_SOCKET)
        owner._records[cap].group = group
        group.members = (cap,)
        info = os.fstat(cap.fd)
        owner.bind_identity(cap, (info.st_dev, info.st_ino))
        return cap
    except BaseException as primary:
        # If adoption itself failed after recording an acquisition, still bind
        # that record to the retained wrapper; never leave a second raw closer.
        for record in owner._records.values():
            if record.cap._generation > generation:
                record.group = group
                group.members = (record.cap,)
        errors = []
        try:
            _debt(owner, a.NativePhase.CONTROL_IO)
        except BaseException as cleanup:
            errors.append(cleanup)
        raise _selected(primary, errors)


@dataclass(frozen=True, slots=True)
class _CExitContext:
    binding: a.AttemptBinding
    source: object
    source_record: object
    source_identity: tuple
    sender: _LocalIdentity
    credentials: tuple
    sequence: int
    started_ns: int
    end_ns: int
    budget_record: object


class BoundChannel:
    """One exact endpoint, a local cursor, and one shared budget.

    Construct both inherited endpoints before spawning. The channel never uses
    SO_PEERCRED, waits for EOF completion, opens a process or performs C's dup2.
    """
    def __init__(self, sock, owner, cursor, budget, peer):
        self.owner, self._socket = owner, sock
        role = cursor.role
        self.acquisition = _adopt_socket(owner, sock, role)
        self.cursor, self.budget, self.peer = cursor, budget, peer
        self._prepaid = None
        self._c_exit_attempted = False
        _require(type(cursor) is ProtocolCursor and type(budget) is ProtocolBudget
                 and cursor._facts.owner is owner and type(peer) is a.Role
                 and abs(int(role) - int(peer)) == 1)
        cursor._facts.shared_budget = budget.acquisition
        if cursor.clock._budget is None:
            cursor.clock._bind_budget(budget)
        else:
            _require(cursor.clock._budget.acquisition is budget.acquisition
                     and cursor.clock._budget._owner is budget._owner)
        _require(sock.getsockopt(socket.SOL_SOCKET, socket.SO_DOMAIN) == socket.AF_UNIX
                 and sock.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) == socket.SOCK_SEQPACKET
                 and socket.CMSG_SPACE(12) == 32)
        _require(peer not in cursor._facts.endpoints)
        cursor._facts.endpoints[peer] = self.acquisition
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        _require(sock.getsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED) == 1)
        sock.setblocking(False)

    def _endpoint(self):
        _require(self.cursor._facts.endpoints.get(self.peer) is self.acquisition)
        kind = FDKind.TERMINAL_SOCKET if self.acquisition is self.owner._terminal else FDKind.CONTROL_SOCKET
        record = self.owner.require_current(self.acquisition, kind=kind, role=self.cursor.role)
        _require(record.group is not None and record.group.object is self._socket
                 and self._socket.fileno() == self.acquisition.fd)
        info = os.fstat(self.acquisition.fd)
        _require(record.identity == (info.st_dev, info.st_ino)
                 and self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED) == 1)
        return self.acquisition.fd

    def _wait(self, *, writing, frozen=None):
        fd = self._endpoint()
        terminal = self.cursor._terminal is not None
        poller = select.poll()
        poller.register(fd, select.POLLOUT if writing else select.POLLIN)
        while True:
            now = time.monotonic_ns()
            if frozen is not None:
                _require(frozen.started_ns <= now < frozen.end_ns)
                end = frozen.end_ns
            else:
                cleanup_end = self.cursor.clock.cleanup_end
                terminal = terminal or (not writing and cleanup_end is not None)
                self.cursor.clock.check(now, terminal=terminal)
                end = cleanup_end if terminal else self.cursor.clock.deadline_ns
            events = poller.poll((end - now + 999_999) // 1_000_000)
            now = time.monotonic_ns()
            if frozen is not None:
                _require(frozen.started_ns <= now < frozen.end_ns)
            else:
                if not writing and self.cursor.clock.cleanup_end is not None:
                    terminal = True
                self.cursor.clock.check(now, terminal=terminal)
            if events:
                return

    def _failure(self, primary, edge, *, prior_cleanup=None):
        errors = []
        if prior_cleanup is not None:
            errors.append(prior_cleanup)
        try:
            self.cursor.mark_uncertain(*edge)
        except BaseException as cleanup:
            errors.append(cleanup)
            # Retain uncertainty even when its ordinary bookkeeping fails.
            # Debt blocks reuse if inserting the exact edge did not complete.
            self.cursor._facts.ambiguous = True
            try:
                _debt(self.owner, a.NativePhase.CONTROL_IO)
            except BaseException as secondary:
                errors.append(secondary)
        try:
            kind = (a.FailureKind.INTERRUPTED if not isinstance(primary, Exception)
                    else a.FailureKind.PROTOCOL_FAILURE if isinstance(primary, ContractError)
                    else a.FailureKind.NATIVE_OPERATION_FAILURE)
            errno = primary.errno if isinstance(primary, OSError) and type(primary.errno) is int and 0 < primary.errno <= 4095 else None
            self.owner.record_cause(a.FailureRecord(self.cursor.role, a.NativePhase.CONTROL_IO, kind, errno))
        except BaseException as cleanup:
            errors.append(cleanup)
            try:
                _debt(self.owner, a.NativePhase.CONTROL_IO)
            except BaseException as secondary:
                errors.append(secondary)
        raise _selected(primary, errors)

    def send(self, frame):
        edge = (self.cursor.role, self.peer)
        try:
            _require((frame.src, frame.dst) == edge)
            if frame.src is C and frame.opcode is O.EXIT_NOTE:
                return self._send_c_exit(frame)
            transition = self.cursor._preview(frame, self.cursor._message_credentials(frame), time.monotonic_ns())
            self._wait(writing=True)
            if transition.terminal_slot is None:
                _require(self.budget.charge_ordinary(192))
            else:
                self.budget.claim_terminal(transition.terminal_slot)
            raw = frame.to_bytes()
            if frame.opcode in (O.GO_NATIVE, O.GO_CLOSE_RANGE):
                self.cursor._grant_eligible(frame)
            # Resolve every possibly fallible shared read BEFORE sampling the
            # final time. Neither a spent credit nor an old timestamp extends it.
            end = (self.cursor.clock.cleanup_end if self.cursor._terminal is not None
                   else self.cursor.clock.deadline_ns)
            now = time.monotonic_ns()
            _require(end is not None and self.cursor.clock.started_ns <= now < end)
            _require(self._socket.sendmsg([raw], [], socket.MSG_NOSIGNAL | socket.MSG_DONTWAIT) == len(raw))
            self.cursor._commit(frame, transition)
        except BaseException as primary:
            self._failure(primary, edge)

    def _close_delivered_rights(self, ancillary):
        errors, primary = [], None
        known = []
        found = opaque = False
        generation = self.owner._generation
        # Inspect ALL returned cmsgs before validating even the first one.
        try:
            for level, kind, raw in ancillary:
                if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                    found = True
                    for offset in range(0, len(raw) - len(raw) % 4, 4):
                        fd = struct.unpack('@i', raw[offset:offset + 4])[0]
                        try:
                            known.append(self.owner.adopt_fd(fd, role=self.cursor.role, identity=None))
                        except BaseException as exc:
                            primary = _active(primary, exc)
                            opaque = True
        except BaseException as exc:
            # recvmsg already returned resources. An interrupted parser/cmsg
            # iterator cannot establish their complete census, even if no raw
            # number was extracted yet. This is opaque acquisition debt.
            primary, opaque = _active(primary, exc), True
        # Include records made before an adopt_fd exception, not just normally
        # returned capabilities. Known independent closes precede bookkeeping.
        try:
            for record in self.owner._records.values():
                if record.cap._generation > generation and record.cap not in known:
                    known.append(record.cap)
        except BaseException as exc:
            primary, opaque = _active(primary, exc), True
        for cap in known:
            try:
                self.owner.close_once(cap)
            except BaseException as exc:
                errors.append(exc)
        if opaque:
            try:
                _debt(self.owner, a.NativePhase.FD_CLOSE)
            except BaseException as cleanup:
                errors.append(cleanup)
        if primary is not None or errors:
            raise _selected(primary, errors)
        return found

    def receive(self):
        edge = (self.peer, self.cursor.role)
        recv_started = census_complete = False
        try:
            self._wait(writing=False)
            recv_started = True
            raw, ancillary, flags, address = self._socket.recvmsg(161, 32, socket.MSG_CMSG_CLOEXEC | socket.MSG_DONTWAIT)
            rights = self._close_delivered_rights(ancillary)
            census_complete = True
            _require(not rights and len(raw) <= 160 and not flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC))
            _require(len(ancillary) == 1)
            level, kind, data = ancillary[0]
            _require(level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS and len(data) == 12)
            credentials = struct.unpack('@iii', data)
            frame = a.Frame.from_bytes(raw)
            _require((frame.src, frame.dst) == edge)
            self.cursor.accept(frame, credentials, now_ns=time.monotonic_ns())
            return frame
        except BaseException as primary:
            if recv_started and not census_complete:
                try:
                    _debt(self.owner, a.NativePhase.FD_CLOSE)
                except BaseException as cleanup:
                    self._failure(primary, edge, prior_cleanup=cleanup)
            self._failure(primary, edge)

    def prepay_c_exit(self):
        _require(self.cursor.role is C and self.cursor.family is ProtocolFamily.NATIVE
                 and self.cursor._terminal is a.TerminalFamily.N_C and self._prepaid is None and not self._c_exit_attempted
                 and self.cursor._sequences.get((C, T)) == 2)
        self.cursor.clock.check(time.monotonic_ns(), terminal=True)
        self._endpoint()
        record = self.owner.require_current(self.acquisition, kind=FDKind.CONTROL_SOCKET, role=C)
        _require(record.binding == (T, self.cursor.binding))
        local = self.cursor._facts.identity(C)
        _require(type(local) is _LocalIdentity and self.owner.cause is None and self.owner.cleanup is None)
        expected = a.RegistrationPayload(C, local.pid, T)
        _require(self.cursor._hello.get(C) == expected and self.cursor._acknowledged.get(C) == expected)
        candidate = a.Frame(O.EXIT_NOTE, C, T, 2, self.cursor.binding,
            a.ExitPayload(C, T, local.pid, a.ExitKind.EXITED, 0, 0, None, None))
        credentials = self.cursor._message_credentials(candidate)
        self.cursor._preview(candidate, credentials, time.monotonic_ns())
        end = self.cursor.clock.cleanup_end
        context = _CExitContext(self.cursor.binding, self.acquisition, record, record.identity,
                               local, credentials, 2, self.cursor.clock.started_ns, end,
                               self.budget._owner.require_current(self.budget.acquisition, kind=FDKind.FILE))
        self.budget.claim_terminal(0)
        self._terminal_source = self.acquisition
        self._prepaid = context

    def _installed_terminal(self, cap):
        context = self._prepaid
        _require(type(context) is _CExitContext and cap is self.owner._terminal and cap.fd == 1)
        record = self.owner.require_current(cap, kind=FDKind.TERMINAL_SOCKET, role=C)
        _require(self.owner._records.get(context.source) is context.source_record
                 and record.binding == (context.source, (T, context.binding))
                 and record.identity == context.source_identity)
        return record

    def _terminal_ready(self, cap):
        record = self._installed_terminal(cap)
        context = self._prepaid
        _require(not self.owner.has_debt and context.budget_record.attempted and context.budget_record.closed)
        _require(all(item.cap is cap or item.attempted and item.closed for item in self.owner._records.values()))
        return record

    def _send_c_exit(self, frame):
        _require(not self._c_exit_attempted)
        self._c_exit_attempted = True
        context = self._prepaid
        _require(type(context) is _CExitContext and self.cursor.role is C and self.peer is T)
        _require(context.credentials == (context.sender.pid, 0, 0))
        frame.__post_init__()
        payload = frame.payload
        _require((frame.opcode, frame.src, frame.dst, frame.sequence, frame.binding) ==
                 (O.EXIT_NOTE, C, T, context.sequence, context.binding))
        _require((payload.subject, payload.parent, payload.pid) == (C, T, context.sender.pid)
                 and payload.cause == self.owner.cause and payload.cleanup == self.owner.cleanup
                 and all(item is None or item.role is C for item in (payload.cause, payload.cleanup)))
        _require(self.cursor._terminal is a.TerminalFamily.N_C and self.cursor._terminal_position == 0
                 and self.cursor._sequences.get((C, T)) == context.sequence)
        _require((C, T) not in self.cursor._uncertain and not self.cursor._facts.ambiguous)
        self._terminal_ready(self.acquisition)
        self._wait(writing=True, frozen=context)
        raw = frame.to_bytes()
        _require((C, T) not in self.cursor._uncertain and not self.cursor._facts.ambiguous)
        _require(context.started_ns <= time.monotonic_ns() < context.end_ns)
        _require(self._socket.sendmsg([raw], [], socket.MSG_NOSIGNAL | socket.MSG_DONTWAIT) == len(raw))
        self.cursor._commit(frame, Transition(O.EXIT_NOTE, C, 0))

    def _use_terminal(self, sock, cap):
        # B2 alone performs dup2/detach and owns this already-returned wrapper.
        # No same-inode endpoint, integer FD or credit alone can replace it.
        record = self._installed_terminal(cap)
        _require(type(sock) is socket.socket and sock.fileno() == 1
                 and record.group is not None and record.group.object is sock)
        self._socket, self.acquisition = sock, cap
        self.cursor._facts.endpoints[T] = cap

    def close(self):
        self.owner.close_once(self.acquisition)
