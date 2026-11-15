"""Strict local audit values. These bytes can never authorize slot release."""
from dataclasses import dataclass, field
import ctypes
import hashlib
import os
import struct

from . import _staging_arena as arena_v1
from ._staging_a2_contract import AttemptBinding, ExitKind, NativePhase, Role
from ._staging_contract import ContractError, FixtureStatus, _hex, _integer, _require
from ._staging_native import _adopt, _close_owned
from ._staging_ownership import AcquisitionLedger


@dataclass(frozen=True, slots=True)
class Slot0Baseline:
    raw_slot0: bytes
    header0_digest: str
    header0_revision: int
    generation: str
    identities: tuple[tuple[int, int], ...]

    def __post_init__(self):
        _require(type(self.raw_slot0) is bytes and len(self.raw_slot0) == 512)
        _hex(self.header0_digest, 64)
        _integer(self.header0_revision)
        _hex(self.generation, 32)
        _require(hashlib.sha256(self.raw_slot0).hexdigest() == self.header0_digest)
        record = arena_v1._loads(self.raw_slot0, arena_v1._RECORD_FIELDS)
        _require(arena_v1._fixed(record, 512) == self.raw_slot0)
        arena_v1._validate_record(record, self.generation, 0, self.header0_revision)
        _require(record['state'] == 'UNKNOWN')
        _require(type(self.identities) is tuple and len(self.identities) == 9)
        for pair in self.identities:
            _require(type(pair) is tuple and len(pair) == 2)
            for item in pair:
                _integer(item, 2**64 - 1)
        _require(len(set(self.identities)) == 9)

    def to_bytes(self):
        self.__post_init__()
        raw = struct.pack('!4sB512s32sQ16s18Q', b'SBL2', 2, self.raw_slot0,
                          bytes.fromhex(self.header0_digest), self.header0_revision,
                          bytes.fromhex(self.generation), *(n for pair in self.identities for n in pair))
        return raw + b'\0' * (1024 - len(raw))

    @classmethod
    def from_bytes(cls, raw):
        _require(type(raw) is bytes and len(raw) == 1024)
        size = struct.calcsize('!4sB512s32sQ16s18Q')
        magic, version, record, digest, revision, generation, *ids = struct.unpack('!4sB512s32sQ16s18Q', raw[:size])
        _require(magic == b'SBL2' and version == 2 and raw[size:] == b'\0' * (1024 - size))
        return cls(record, digest.hex(), revision, generation.hex(), tuple(tuple(ids[i:i + 2]) for i in range(0, 18, 2)))


@dataclass(frozen=True, slots=True)
class ProcessFact:
    role: Role
    parent: Role
    pid: int
    parent_pid: int
    exit_kind: ExitKind
    status: int
    reaped: bool

    def __post_init__(self):
        _require(type(self.role) is Role and self.role in (Role.R, Role.S, Role.T)
                 and type(self.parent) is Role and int(self.parent) + 1 == int(self.role))
        for pid in (self.pid, self.parent_pid):
            _integer(pid, 2**31 - 1)
            _require(pid > 0)
        _require(type(self.exit_kind) is ExitKind and type(self.reaped) is bool)
        _require(type(self.status) is int and -(2**31) <= self.status < 2**31)


@dataclass(frozen=True, slots=True)
class RetentionHistory:
    binding: AttemptBinding
    baseline_digest: str
    facts: tuple[ProcessFact, ...]
    closed_fd_bits: int
    closure_bits: int
    origin: FixtureStatus = field(default=FixtureStatus.UNKNOWN, init=False)

    def __post_init__(self):
        _require(type(self.binding) is AttemptBinding)
        self.binding.__post_init__()
        _hex(self.baseline_digest, 64)
        _require(self.origin is FixtureStatus.UNKNOWN and type(self.facts) is tuple and len(self.facts) == 3)
        for role, fact in zip((Role.R, Role.S, Role.T), self.facts):
            _require(type(fact) is ProcessFact)
            fact.__post_init__()
            _require(fact.role is role and fact.reaped is True)
        _require(len({fact.pid for fact in self.facts}) == 3
                 and self.facts[1].parent_pid == self.facts[0].pid
                 and self.facts[2].parent_pid == self.facts[1].pid)
        _integer(self.closed_fd_bits, 2**64 - 1)
        _require(type(self.closure_bits) is int and self.closure_bits == 14)

    def to_bytes(self):
        self.__post_init__()
        binding = self.binding
        raw = struct.pack('!4sB16s16s16s32s32sB', b'SRH2', 2, bytes.fromhex(binding.attempt),
                          bytes.fromhex(binding.token), bytes.fromhex(binding.generation),
                          bytes.fromhex(binding.manifest), bytes.fromhex(self.baseline_digest), 4)
        for fact in self.facts:
            raw += struct.pack('!BBIIBiB', fact.role, fact.parent, fact.pid, fact.parent_pid,
                               fact.exit_kind, fact.status, int(fact.reaped))
        raw += struct.pack('!QB', self.closed_fd_bits, self.closure_bits)
        return raw + b'\0' * (512 - len(raw))

    @classmethod
    def from_bytes(cls, raw):
        _require(type(raw) is bytes and len(raw) == 512)
        size = struct.calcsize('!4sB16s16s16s32s32sB')
        magic, version, attempt, token, generation, manifest, baseline, origin = struct.unpack('!4sB16s16s16s32s32sB', raw[:size])
        _require(magic == b'SRH2' and version == 2 and origin == 4)
        facts = []
        try:
            for offset in range(size, size + 48, 16):
                role, parent, pid, parent_pid, kind, status, reaped = struct.unpack('!BBIIBiB', raw[offset:offset + 16])
                _require(reaped in (0, 1))
                facts.append(ProcessFact(Role(role), Role(parent), pid, parent_pid, ExitKind(kind), status, bool(reaped)))
        except ValueError:
            raise ContractError() from None
        closed, closure = struct.unpack('!QB', raw[size + 48:size + 57])
        _require(raw[size + 57:] == b'\0' * (512 - size - 57))
        return cls(AttemptBinding(attempt.hex(), token.hex(), generation.hex(), manifest.hex()), baseline.hex(), tuple(facts), closed, closure)


def _identities(header):
    return tuple(tuple(header[name]) for name in ('parent', 'root', 'source', 'arena', 'header')) + tuple(tuple(pair) for name in ('slots', 'records') for pair in header[name])


def capture_slot0_locked(arena, roots):
    _require(type(arena) is arena_v1.SlotArena and type(roots) is arena_v1.FixtureRoots)
    roots.validate()
    records = arena._read()
    header = arena._header
    _require(records[0]['state'] == 'UNKNOWN' and records[1]['state'] == 'FREE'
             and tuple(roots._identities) == _identities(header)[:4])
    raw = os.pread(arena._record_fds[0], 513, 0)
    return Slot0Baseline(raw, header['record_digests'][0], header['record_revisions'][0],
                         header['generation'], _identities(header))


@dataclass(frozen=True, slots=True, init=False)
class _PendingSlot1Transition:
    """Construct only before intent from A1-validated independent commitments."""
    arena: arena_v1.SlotArena
    token: str | None
    revision: int
    prior_header: bytes
    prior_slot1: bytes

    def __init__(self, *args, **kwargs):
        raise ContractError()


def _prepare_slot1_transition_locked(arena, *, token, revision):
    _require(type(arena) is arena_v1.SlotArena and arena._guard.locked())
    arena._read()
    if token is not None:
        _hex(token, 32)
    _integer(revision)
    _require(revision == arena._header['record_revisions'][1] + 1)
    prior = arena_v1._read_fixed(arena._record_fds[1], 512, arena_v1._RECORD_FIELDS)
    arena_v1._validate_record(prior, arena._header['generation'], 1, arena._header['record_revisions'][1])
    _require(arena_v1._record_digest(prior) == arena._header['record_digests'][1])
    expected = object.__new__(_PendingSlot1Transition)
    for name, value in (('arena', arena), ('token', token), ('revision', revision),
                        ('prior_header', arena_v1._fixed(arena._header, 1024)),
                        ('prior_slot1', arena_v1._fixed(prior, 512))):
        object.__setattr__(expected, name, value)
    return expected


def assert_slot0_unchanged_locked(arena, baseline, *, expected_transition):
    _require(type(arena) is arena_v1.SlotArena and type(baseline) is Slot0Baseline and arena._guard.locked())
    baseline.__post_init__()
    if expected_transition is None:
        arena._read()
        header = arena._header
    else:
        expected = expected_transition
        _require(type(expected) is _PendingSlot1Transition and expected.arena is arena)
        prior = arena_v1._loads(expected.prior_header, arena_v1._HEADER_FIELDS)
        arena_v1._validate_header(prior)
        _require(arena_v1._fixed(prior, 1024) == expected.prior_header
                 and expected.prior_header == arena_v1._fixed(arena._header, 1024))
        _integer(expected.revision)
        _require(expected.revision == prior['record_revisions'][1] + 1)
        if expected.token is not None:
            _hex(expected.token, 32)
        header = arena_v1._read_fixed(arena._header_fd, 1024, arena_v1._HEADER_FIELDS)
        intent = header['pending']
        _require(type(intent) is dict and set(intent) == {'slot_index', 'token', 'revision'}
                 and type(intent['slot_index']) is int and intent['slot_index'] == 1
                 and type(intent['revision']) is int and intent['revision'] == expected.revision
                 and intent['token'] == expected.token)
        _require(header == {**prior, 'pending': {'slot_index': 1, 'token': expected.token, 'revision': expected.revision}})
        arena_v1._validate_header({**header, 'pending': None})
        # At the intent boundary, slot1 MUST still be the independently committed
        # canonical record. An observed digest is never an expected commitment.
        record1 = arena_v1._read_fixed(arena._record_fds[1], 512, arena_v1._RECORD_FIELDS)
        arena_v1._validate_record(record1, prior['generation'], 1, prior['record_revisions'][1])
        _require(arena_v1._fixed(record1, 512) == expected.prior_slot1
                 and arena_v1._record_digest(record1) == prior['record_digests'][1])
        arena_v1._binding((arena._arena_fd, header))
        for fd, identity in ((arena._header_fd, header['header']),
                             (arena._record_fds[0], header['records'][0]), (arena._record_fds[1], header['records'][1])):
            _require(list(arena_v1._identity(os.fstat(fd))) == identity)
        for name, identity in ((arena_v1._HEADER_NAME, header['header']),
                               *((f'slot-{i}.record', header['records'][i]) for i in range(2))):
            info = os.stat(name, dir_fd=arena._arena_fd, follow_symlinks=False)
            arena_v1._check_owned(info, directory=False)
            _require(list(arena_v1._identity(info)) == identity)
        for i in range(2):
            arena_v1._empty_anchor(arena._arena_fd, f'slot-{i}', header['slots'][i])
        _require(arena_v1._directory_matches(arena._arena_fd, arena_v1._ENTRIES))
    _require(header['generation'] == baseline.generation and _identities(header) == baseline.identities
             and header['record_digests'][0] == baseline.header0_digest
             and header['record_revisions'][0] == baseline.header0_revision
             and os.pread(arena._record_fds[0], 513, 0) == baseline.raw_slot0)

class _Statfs(ctypes.Structure):
    _fields_ = [('f_type', ctypes.c_long), ('f_bsize', ctypes.c_long),
                ('f_blocks', ctypes.c_ulong), ('f_bfree', ctypes.c_ulong), ('f_bavail', ctypes.c_ulong),
                ('f_files', ctypes.c_ulong), ('f_ffree', ctypes.c_ulong), ('f_fsid', ctypes.c_int * 2),
                ('f_namelen', ctypes.c_long), ('f_frsize', ctypes.c_long), ('f_flags', ctypes.c_long),
                ('f_spare', ctypes.c_long * 4)]


def _fstatfs_magic(fd):
    libc = ctypes.CDLL('/usr/lib/x86_64-linux-gnu/libc.so.6', use_errno=True)
    libc.fstatfs.argtypes = [ctypes.c_int, ctypes.POINTER(_Statfs)]
    libc.fstatfs.restype = ctypes.c_int
    value = _Statfs()
    result = libc.fstatfs(fd, ctypes.byref(value))
    if result == -1:
        err = ctypes.get_errno()
        raise OSError(err, 'metadata_filesystem_unconfirmed')
    _require(result == 0)
    return value.f_type


def _proc_read(path, limit, owner):
    fd = _adopt(owner, Role.HOST, os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW))
    primary = None
    try:
        raw = os.read(fd.fd, limit + 1)
        _require(len(raw) <= limit)
        return raw
    except BaseException as exc:
        primary = exc
        raise
    finally:
        _close_owned(owner, fd, primary)


def _mount_matches(raw, mount_id, device):
    if type(raw) is not bytes or len(raw) > 262144 or type(mount_id) is not int or mount_id <= 0:
        return False
    seen = 0
    for line in raw.splitlines():
        if len(line) > 4096:
            return False
        fields = line.split()
        if not fields or fields[0] != str(mount_id).encode('ascii'):
            continue
        seen += 1
        if len(fields) < 10 or fields.count(b'-') != 1:
            return False
        separator = fields.index(b'-')
        if separator < 6 or len(fields) != separator + 4:
            return False
        if fields[separator + 1] != b'ext4' or fields[2] != f'{os.major(device)}:{os.minor(device)}'.encode('ascii'):
            return False
    return seen == 1


def verify_ext4_metadata(parent_fd, *, owner):
    _require(type(owner) is AcquisitionLedger)
    try:
        parent_raw = owner.require_parent(parent_fd)
        info = arena_v1._fd_directory(parent_raw)
        _require(_fstatfs_magic(parent_raw) == 0xEF53)
        raw = _proc_read(f'/proc/self/fdinfo/{parent_raw}', 4096, owner)
        ids = [line.split(b':', 1)[1].strip() for line in raw.splitlines() if line.startswith(b'mnt_id:')]
        _require(len(ids) == 1 and ids[0].isdigit() and len(ids[0]) <= 10)
        mount_id = int(ids[0])
        _require(_mount_matches(_proc_read('/proc/self/mountinfo', 262144, owner), mount_id, info.st_dev))
        os.fsync(parent_raw)
    except BaseException:
        owner.poison_debt(NativePhase.HISTORY_COMMIT)
        raise


def _commit(parent_fd, raw, leaf, *, owner, budget):
    _require(type(owner) is AcquisitionLedger)
    active = None
    primary = None
    try:
        parent_raw = owner.require_parent(parent_fd)
        if len(raw) == 1024:
            baseline = Slot0Baseline.from_bytes(raw)
            _require(owner._record(parent_fd).identity == baseline.identities[0])
        verify_ext4_metadata(parent_fd, owner=owner)
        _require(budget.charge(len(raw)) is True)
        fd = os.open(leaf, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent_raw)
        active = owner.adopt_fd(fd, role=Role.HOST, identity=None)
        info = os.fstat(fd)
        owner.bind_identity(active, (info.st_dev, info.st_ino))
        arena_v1._check_owned(info, directory=False)
        _require(info.st_size == 0 and os.write(fd, raw) == len(raw))
        _require(os.fstat(fd).st_size == len(raw))
        os.fsync(fd)
        owner.close_once(active)
        active = None
        os.fsync(parent_raw)
        fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent_raw)
        active = owner.adopt_fd(fd, role=Role.HOST, identity=None)
        after = os.fstat(fd)
        owner.bind_identity(active, (after.st_dev, after.st_ino))
        arena_v1._check_owned(after, directory=False)
        _require((after.st_dev, after.st_ino) == (info.st_dev, info.st_ino) and after.st_size == len(raw)
                 and os.read(fd, len(raw) + 1) == raw)
        (Slot0Baseline if len(raw) == 1024 else RetentionHistory).from_bytes(raw)
    except BaseException as exc:
        owner.poison_debt(NativePhase.HISTORY_COMMIT)
        primary = exc
        raise
    finally:
        if active is not None:
            _close_owned(owner, active, primary)


def commit_baseline(parent_fd, baseline, *, owner, budget):
    _require(type(baseline) is Slot0Baseline)
    _commit(parent_fd, baseline.to_bytes(), 'staging-a2-baseline-v2.bin', owner=owner, budget=budget)


def commit_history(parent_fd, history, *, owner, budget):
    _require(type(history) is RetentionHistory)
    _commit(parent_fd, history.to_bytes(), 'staging-a2-retention-v2.bin', owner=owner, budget=budget)
