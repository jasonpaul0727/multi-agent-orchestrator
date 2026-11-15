"""Finite v2 observations and wire structs. Decoding conveys no ownership rights."""
from dataclasses import dataclass, field
from enum import IntEnum
import json
import struct

from ._staging_contract import (
    ContractError, FixtureStatus, NativePrimitive, NativePrerequisiteReceipt,
    ReasonCode, _dumps, _hex, _integer, _loads, _require,
)


class Role(IntEnum):
    HOST = 0
    R = 1
    S = 2
    T = 3
    C = 4


class NativePhase(IntEnum):
    VALIDATE_REQUEST = 1
    VERIFY_MANIFEST = 2
    REOPEN_ROOTS = 3
    RESERVE_SLOT = 4
    LOAD_LIBC = 5
    SPAWN_R = 6
    REGISTER_R = 7
    SET_SUBREAPER = 8
    VERIFY_SUBREAPER = 9
    SPAWN_S = 10
    REGISTER_S = 11
    SPAWN_T = 12
    REGISTER_T = 13
    GUARD_PARENT = 14
    READ_NAMESPACE_BEFORE = 15
    UNSHARE = 16
    OPEN_UID_MAP = 17
    WRITE_UID_MAP = 18
    OPEN_SETGROUPS = 19
    WRITE_SETGROUPS = 20
    OPEN_GID_MAP = 21
    WRITE_GID_MAP = 22
    VERIFY_MAPS = 23
    VERIFY_USER_MOUNT_AFTER_UNSHARE = 24
    SET_NONDUMPABLE = 25
    VERIFY_NONDUMPABLE = 26
    SPAWN_C = 27
    REGISTER_C = 28
    VERIFY_PID_CHILD_AFTER_REGISTER = 29
    DUP_HIGH_FD = 30
    SET_NOFILE = 31
    INVENTORY_FDS = 32
    CLOSE_RANGE = 33
    VERIFY_HIGH_FDS = 34
    CONTROL_CODEC = 35
    CONTROL_IO = 36
    TARGETED_REAP = 37
    FD_CLOSE = 38
    MARK_UNKNOWN = 39
    OWNER_PARK = 40
    HISTORY_COMMIT = 41
    RELEASE_FIXTURE = 42


class FailureKind(IntEnum):
    NATIVE_REJECTION = 1
    NATIVE_OPERATION_FAILURE = 2
    CONTRACT_FAILURE = 3
    INVARIANT_FAILURE = 4
    PROTOCOL_FAILURE = 5
    INTERRUPTED = 6
    CLEANUP_UNKNOWN = 7


class OwnerState(IntEnum):
    BOOTSTRAP = 1
    RUNNING = 2
    CLEANING = 3
    PARKED_UNKNOWN = 4
    CLOSED = 5
    POISONED = 6
    POISONED_UNRESERVED_DEBT = 7


class TerminalFamily(IntEnum):
    N_C = 1
    R_0 = 2
    R_1 = 3
    S_0 = 4
    T_0 = 5
    U = 6


class Opcode(IntEnum):
    START = 1
    GROUPS = 2
    HELLO = 3
    ACK_REGISTER = 4
    IDENTITY = 5
    NS_IDENTITY = 6
    GO_NATIVE = 7
    GO_CLOSE_RANGE = 8
    RESULT_PARKED = 9
    RESULT_CLOSED = 10
    CONVERGE_PREUNSHARE = 11
    EXIT_NOTE = 12
    REAP_ACK = 13
    PREFIX_CLOSED = 14


class ExitKind(IntEnum):
    EXITED = 1
    SIGNALED = 2


def _typed(value, enum):
    _require(type(value) is enum)


def _named(value, enum):
    _require(type(value) is str)
    try:
        return enum[value]
    except KeyError:
        raise ContractError() from None


def _number(value, enum):
    _require(type(value) is int)
    try:
        return enum(value)
    except ValueError:
        raise ContractError() from None


_REFUSALS = {
    **{phase: frozenset((1, 13, 22, 38)) for phase in (
        NativePhase.UNSHARE, NativePhase.SET_SUBREAPER, NativePhase.VERIFY_SUBREAPER,
        NativePhase.GUARD_PARENT, NativePhase.SET_NONDUMPABLE, NativePhase.VERIFY_NONDUMPABLE,
        NativePhase.REGISTER_R, NativePhase.REGISTER_S, NativePhase.REGISTER_T,
        NativePhase.REGISTER_C, NativePhase.CLOSE_RANGE)},
    **{phase: frozenset((1, 2, 13, 20, 30, 38)) for phase in (
        NativePhase.OPEN_UID_MAP, NativePhase.WRITE_UID_MAP, NativePhase.OPEN_SETGROUPS,
        NativePhase.WRITE_SETGROUPS, NativePhase.OPEN_GID_MAP, NativePhase.WRITE_GID_MAP)},
}


@dataclass(frozen=True, slots=True)
class FailureRecord:
    role: Role
    phase: NativePhase
    kind: FailureKind
    errno: int | None

    def __post_init__(self):
        _typed(self.role, Role)
        _typed(self.phase, NativePhase)
        _typed(self.kind, FailureKind)
        if self.errno is not None:
            _integer(self.errno, 4095)
            _require(self.errno > 0)
        if self.kind is FailureKind.NATIVE_REJECTION:
            _require(self.errno in _REFUSALS.get(self.phase, ()))
        elif self.kind is not FailureKind.NATIVE_OPERATION_FAILURE:
            _require(self.errno is None)

    def to_bytes(self):
        self.__post_init__()
        return _dumps({'role': self.role.name, 'phase': self.phase.name,
                       'kind': self.kind.name, 'errno': self.errno})

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('role', 'phase', 'kind', 'errno'))
        return cls(_named(data['role'], Role), _named(data['phase'], NativePhase),
                   _named(data['kind'], FailureKind), data['errno'])


@dataclass(frozen=True, slots=True)
class AttemptBinding:
    attempt: str
    token: str
    generation: str
    manifest: str

    def __post_init__(self):
        for name in ('attempt', 'token', 'generation'):
            _hex(getattr(self, name), 32)
        _hex(self.manifest, 64)


def derive_status(cause, cleanup, owner_state, primitives):
    _typed(owner_state, OwnerState)
    for record in (cause, cleanup):
        if record is not None:
            _require(type(record) is FailureRecord)
            record.__post_init__()
    _require(type(primitives) is tuple and len(set(primitives)) == len(primitives))
    for primitive in primitives:
        _typed(primitive, NativePrimitive)
    if owner_state is not OwnerState.CLOSED or cleanup is not None:
        return FixtureStatus.UNKNOWN
    if cause is not None:
        if cause.kind in (FailureKind.INTERRUPTED, FailureKind.CLEANUP_UNKNOWN):
            return FixtureStatus.UNKNOWN
        return (FixtureStatus.UNSUPPORTED if cause.kind is FailureKind.NATIVE_REJECTION
                else FixtureStatus.FAILED)
    return FixtureStatus.OBSERVED if set(primitives) == set(NativePrimitive) else FixtureStatus.FAILED


def _failure_roles_registered(cause, cleanup, registered, *, runner_local=False):
    for record in (cause, cleanup):
        if record is not None:
            _require(type(record) is FailureRecord)
            record.__post_init__()
            if record.role is not Role.HOST and not (runner_local and record.role is Role.R):
                _require(bool(registered & (1 << int(record.role))))

@dataclass(frozen=True, slots=True)
class ExecutionEnvelope:
    attempt: str
    token: str
    generation: str
    manifest: str
    receipt_v1: NativePrerequisiteReceipt
    cause: FailureRecord | None
    cleanup: FailureRecord | None
    owner_state: OwnerState
    registered_role_bits: int
    reaped_role_bits: int
    data_grant_released: bool = field(default=False, init=False)
    worker_created: bool = field(default=False, init=False)
    schema: str = field(default='maestro.staging-native-execution.v2', init=False)

    def __post_init__(self):
        AttemptBinding(self.attempt, self.token, self.generation, self.manifest)
        _require(type(self.schema) is str and self.schema == 'maestro.staging-native-execution.v2')
        _require(self.data_grant_released is False and self.worker_created is False)
        _require(type(self.receipt_v1) is NativePrerequisiteReceipt)
        self.receipt_v1.__post_init__()
        _require(self.receipt_v1.profile_digest == self.manifest)
        for value in (self.registered_role_bits, self.reaped_role_bits):
            _integer(value, 30)
            _require(not value & 1)
        _require(self.registered_role_bits in (0, 2, 6, 14, 30)
                 and self.reaped_role_bits & ~self.registered_role_bits == 0)
        if self.receipt_v1.status is FixtureStatus.OBSERVED:
            _require(self.registered_role_bits == self.reaped_role_bits == 30)
        _failure_roles_registered(self.cause, self.cleanup, self.registered_role_bits)
        if self.owner_state is OwnerState.CLOSED:
            _require(self.reaped_role_bits == self.registered_role_bits)
        _require(self.receipt_v1.status is derive_status(
            self.cause, self.cleanup, self.owner_state, self.receipt_v1.completed_primitives))

    def to_bytes(self):
        self.__post_init__()
        data = {name: getattr(self, name) for name in (
            'attempt', 'token', 'generation', 'manifest', 'registered_role_bits',
            'reaped_role_bits', 'data_grant_released', 'worker_created', 'schema')}
        data['owner_state'] = self.owner_state.name
        data['receipt_v1'] = json.loads(self.receipt_v1.to_bytes())
        for name in ('cause', 'cleanup'):
            record = getattr(self, name)
            data[name] = None if record is None else json.loads(record.to_bytes())
        raw = _dumps(data)
        _require(len(raw) <= 1536)
        return raw

    @classmethod
    def from_bytes(cls, raw):
        _require(type(raw) is bytes and len(raw) <= 1536)
        data = _loads(raw, ('schema', 'attempt', 'token', 'generation', 'manifest', 'receipt_v1',
                           'cause', 'cleanup', 'owner_state', 'registered_role_bits', 'reaped_role_bits',
                           'data_grant_released', 'worker_created'))
        _require(data.pop('schema') == 'maestro.staging-native-execution.v2'
                 and data.pop('data_grant_released') is False and data.pop('worker_created') is False)
        data['receipt_v1'] = NativePrerequisiteReceipt.from_bytes(_dumps(data['receipt_v1']))
        for name in ('cause', 'cleanup'):
            if data[name] is not None:
                data[name] = FailureRecord.from_bytes(_dumps(data[name]))
        data['owner_state'] = _named(data['owner_state'], OwnerState)
        return cls(**data)


@dataclass(frozen=True, slots=True)
class StartPayload:
    indices: tuple[int, ...]
    started_ns: int
    deadline_ns: int
    uid: int
    gid: int


@dataclass(frozen=True, slots=True)
class GroupsPayload:
    groups: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class RegistrationPayload:
    subject: Role
    pid: int
    parent: Role


@dataclass(frozen=True, slots=True)
class IdentityPayload:
    subject: Role
    parent: Role
    pid: int
    uid: int
    gid: int


@dataclass(frozen=True, slots=True)
class NamespacePayload:
    identities: tuple[tuple[int, int], ...]
    pid: int
    ppid: int


@dataclass(frozen=True, slots=True)
class ResultPayload:
    status: FixtureStatus
    reason: ReasonCode
    primitives: int
    state: OwnerState
    registered: int
    reaped: int
    cause: FailureRecord | None
    cleanup: FailureRecord | None


@dataclass(frozen=True, slots=True)
class ExitPayload:
    subject: Role
    parent: Role
    pid: int
    exit_kind: ExitKind
    status: int
    closed_fds: int
    cause: FailureRecord | None
    cleanup: FailureRecord | None


@dataclass(frozen=True, slots=True)
class PrefixPayload:
    registered: int
    reaped: int
    closed: int
    cause: FailureRecord | None
    cleanup: FailureRecord | None


def _u32(value):
    _integer(value, 2**32 - 1)


def _u64(value):
    _integer(value, 2**64 - 1)


def _role_parent(subject, parent):
    _typed(subject, Role)
    _typed(parent, Role)
    _require(subject is not Role.HOST and int(parent) == int(subject) - 1)


def _failure_wire(record):
    if record is None:
        return b'\0' * 5
    _require(type(record) is FailureRecord)
    record.__post_init__()
    return struct.pack('!BBBH', record.role, record.phase, record.kind, record.errno or 0)


def _failure_decode(raw):
    if raw == b'\0' * 5:
        return None
    role, phase, kind, err = struct.unpack('!BBBH', raw)
    return FailureRecord(_number(role, Role), _number(phase, NativePhase),
                         _number(kind, FailureKind), err or None)


def _payload(op, value):
    if op in (Opcode.GO_NATIVE, Opcode.GO_CLOSE_RANGE, Opcode.CONVERGE_PREUNSHARE):
        _require(value is None)
        return b''
    if op is Opcode.START:
        _require(type(value) is StartPayload and type(value.indices) is tuple
                 and value.indices == (0, 1, 2, 3, 4, 5)
                 and all(type(item) is int for item in value.indices))
        _u64(value.started_ns)
        _u64(value.deadline_ns)
        _require(value.deadline_ns == value.started_ns + 30_000_000_000)
        _u32(value.uid)
        _u32(value.gid)
        _require(value.uid > 0)
        return struct.pack('!6I2Q2I', *value.indices, value.started_ns, value.deadline_ns, value.uid, value.gid)
    if op is Opcode.GROUPS:
        _require(type(value) is GroupsPayload and type(value.groups) is tuple and len(value.groups) <= 16)
        for group in value.groups:
            _u32(group)
        return struct.pack('!B' + 'I' * len(value.groups), len(value.groups), *value.groups)
    if op in (Opcode.HELLO, Opcode.ACK_REGISTER):
        _require(type(value) is RegistrationPayload)
        _role_parent(value.subject, value.parent)
        _u32(value.pid)
        _require(value.pid > 0)
        return struct.pack('!BIB', value.subject, value.pid, value.parent)
    if op is Opcode.IDENTITY:
        _require(type(value) is IdentityPayload)
        _role_parent(value.subject, value.parent)
        for item in (value.pid, value.uid, value.gid):
            _u32(item)
        _require(value.pid > 0)
        return struct.pack('!BBIII', value.subject, value.parent, value.pid, value.uid, value.gid)
    if op is Opcode.NS_IDENTITY:
        _require(type(value) is NamespacePayload and type(value.identities) is tuple and len(value.identities) == 3)
        for pair in value.identities:
            _require(type(pair) is tuple and len(pair) == 2)
            for item in pair:
                _u64(item)
        _u32(value.pid)
        _u32(value.ppid)
        return struct.pack('!6Q2I', *(item for pair in value.identities for item in pair), value.pid, value.ppid)
    if op in (Opcode.RESULT_PARKED, Opcode.RESULT_CLOSED):
        _require(type(value) is ResultPayload)
        _typed(value.status, FixtureStatus)
        _typed(value.reason, ReasonCode)
        _typed(value.state, OwnerState)
        _integer(value.primitives, 15)
        for item in (value.registered, value.reaped):
            _integer(item, 28)
            _require(not item & 3)
        _require(value.registered in (0, 4, 12, 28) and value.reaped & ~value.registered == 0)
        if value.status is FixtureStatus.OBSERVED:
            _require(value.registered == value.reaped == 28)
        _failure_roles_registered(value.cause, value.cleanup, value.registered, runner_local=True)
        primitives = tuple(primitive for index, primitive in enumerate(NativePrimitive) if value.primitives & (1 << index))
        NativePrerequisiteReceipt(value.status, '00' * 32, primitives, value.reason)
        _require(value.status is derive_status(value.cause, value.cleanup, value.state, primitives))
        _require((op is Opcode.RESULT_CLOSED) == (value.state is OwnerState.CLOSED))
        if op is Opcode.RESULT_CLOSED:
            _require(value.registered == value.reaped)
        return bytes((list(FixtureStatus).index(value.status) + 1, list(ReasonCode).index(value.reason) + 1,
                      value.primitives, value.state, value.registered, value.reaped)) + _failure_wire(value.cause) + _failure_wire(value.cleanup)
    if op in (Opcode.EXIT_NOTE, Opcode.REAP_ACK):
        _require(type(value) is ExitPayload)
        _role_parent(value.subject, value.parent)
        _u32(value.pid)
        _require(value.pid > 0)
        _typed(value.exit_kind, ExitKind)
        _require(type(value.status) is int and -(2**31) <= value.status < 2**31)
        _u64(value.closed_fds)
        return struct.pack('!BBIBiQ', value.subject, value.parent, value.pid, value.exit_kind,
                           value.status, value.closed_fds) + _failure_wire(value.cause) + _failure_wire(value.cleanup)
    _require(op is Opcode.PREFIX_CLOSED and type(value) is PrefixPayload)
    for item in (value.registered, value.reaped, value.closed):
        _integer(item, 28)
        _require(not item & 3)
    _require(value.registered in (0, 4, 12, 28) and value.registered == value.reaped == value.closed)
    _failure_roles_registered(value.cause, value.cleanup, value.registered, runner_local=True)
    return bytes((value.registered, value.reaped, value.closed)) + _failure_wire(value.cause) + _failure_wire(value.cleanup)


def _decode_payload(op, raw):
    try:
        if op in (Opcode.GO_NATIVE, Opcode.GO_CLOSE_RANGE, Opcode.CONVERGE_PREUNSHARE):
            _require(raw == b'')
            return None
        if op is Opcode.START:
            values = struct.unpack('!6I2Q2I', raw)
            return StartPayload(tuple(values[:6]), *values[6:])
        if op is Opcode.GROUPS:
            _require(len(raw) >= 1 and raw[0] <= 16)
            return GroupsPayload(struct.unpack('!' + 'I' * raw[0], raw[1:]))
        if op in (Opcode.HELLO, Opcode.ACK_REGISTER):
            subject, pid, parent = struct.unpack('!BIB', raw)
            return RegistrationPayload(_number(subject, Role), pid, _number(parent, Role))
        if op is Opcode.IDENTITY:
            subject, parent, pid, uid, gid = struct.unpack('!BBIII', raw)
            return IdentityPayload(_number(subject, Role), _number(parent, Role), pid, uid, gid)
        if op is Opcode.NS_IDENTITY:
            values = struct.unpack('!6Q2I', raw)
            return NamespacePayload(tuple(tuple(values[i:i + 2]) for i in (0, 2, 4)), *values[6:])
        if op in (Opcode.RESULT_PARKED, Opcode.RESULT_CLOSED):
            _require(len(raw) == 16 and 1 <= raw[0] <= len(FixtureStatus) and 1 <= raw[1] <= len(ReasonCode))
            return ResultPayload(list(FixtureStatus)[raw[0] - 1], list(ReasonCode)[raw[1] - 1], raw[2],
                                 _number(raw[3], OwnerState), raw[4], raw[5], _failure_decode(raw[6:11]), _failure_decode(raw[11:]))
        if op in (Opcode.EXIT_NOTE, Opcode.REAP_ACK):
            _require(len(raw) == 29)
            subject, parent, pid, kind, status, closed = struct.unpack('!BBIBiQ', raw[:19])
            return ExitPayload(_number(subject, Role), _number(parent, Role), pid, _number(kind, ExitKind),
                               status, closed, _failure_decode(raw[19:24]), _failure_decode(raw[24:]))
        _require(op is Opcode.PREFIX_CLOSED and len(raw) == 13)
        return PrefixPayload(raw[0], raw[1], raw[2], _failure_decode(raw[3:8]), _failure_decode(raw[8:]))
    except (struct.error, IndexError):
        raise ContractError() from None


@dataclass(frozen=True, slots=True)
class Frame:
    opcode: Opcode
    src: Role
    dst: Role
    sequence: int
    binding: AttemptBinding
    payload: object

    def __post_init__(self):
        _typed(self.opcode, Opcode)
        _typed(self.src, Role)
        _typed(self.dst, Role)
        _require(abs(int(self.src) - int(self.dst)) == 1)
        _integer(self.sequence, 7)
        _require(type(self.binding) is AttemptBinding)
        self.binding.__post_init__()
        op = self.opcode
        if op in (Opcode.START, Opcode.GROUPS):
            _require((self.src, self.dst) == (Role.HOST, Role.R))
        elif op in (Opcode.RESULT_PARKED, Opcode.RESULT_CLOSED, Opcode.PREFIX_CLOSED):
            _require((self.src, self.dst) == (Role.R, Role.HOST))
        elif op is Opcode.NS_IDENTITY:
            _require((self.src, self.dst) == (Role.C, Role.T))
        elif op is Opcode.GO_CLOSE_RANGE:
            _require((self.src, self.dst) == (Role.T, Role.C))
        elif op in (Opcode.GO_NATIVE, Opcode.CONVERGE_PREUNSHARE):
            _require(self.src < self.dst and self.dst is not Role.C)
        elif op in (Opcode.HELLO, Opcode.IDENTITY, Opcode.EXIT_NOTE, Opcode.REAP_ACK):
            _require(self.src > self.dst)
        elif op is Opcode.ACK_REGISTER:
            _require(self.src < self.dst)
        _require(len(_payload(op, self.payload)) <= 70)

    def to_bytes(self):
        self.__post_init__()
        binding = self.binding
        return struct.pack('!4sBBBBH16s16s16s32s', b'SNF2', 2, self.opcode, self.src, self.dst, self.sequence,
                           bytes.fromhex(binding.attempt), bytes.fromhex(binding.token),
                           bytes.fromhex(binding.generation), bytes.fromhex(binding.manifest)) + _payload(self.opcode, self.payload)

    @classmethod
    def from_bytes(cls, raw):
        _require(type(raw) is bytes and 90 <= len(raw) <= 160)
        magic, version, op, src, dst, seq, attempt, token, generation, manifest = struct.unpack('!4sBBBBH16s16s16s32s', raw[:90])
        _require(magic == b'SNF2' and version == 2)
        opcode = _number(op, Opcode)
        result = cls(opcode, _number(src, Role), _number(dst, Role), seq,
                     AttemptBinding(attempt.hex(), token.hex(), generation.hex(), manifest.hex()), _decode_payload(opcode, raw[90:]))
        _require(result.to_bytes() == raw)
        return result

@dataclass(frozen=True, slots=True)
class NamespaceSnapshot:
    user: tuple[int, int]
    mount: tuple[int, int]
    pid: tuple[int, int]


@dataclass(frozen=True, slots=True)
class PreChildNamespaces:
    before: NamespaceSnapshot
    after: NamespaceSnapshot


@dataclass(frozen=True, slots=True)
class ChildRegistration:
    """Internal registration snapshot, not a decoder or release capability."""
    role: Role
    parent: Role
    pid: int
    registered_role_bits: int

    def __post_init__(self):
        _role_parent(self.role, self.parent)
        _u32(self.pid)
        _require(self.pid > 0)
        _integer(self.registered_role_bits, 30)
        _require(not self.registered_role_bits & 1)
