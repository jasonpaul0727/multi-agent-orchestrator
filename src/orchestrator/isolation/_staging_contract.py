"""Private finite Stage 0 values; none of these observations convey authority.

The native-prefix manifest covers this increment only. It must be expanded and
rebuilt when new trusted helpers are implemented; it is not a campaign manifest.
"""

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import sys
import time


PROFILE_ID = 'LOCAL_CUSTODY_X64_PY3123_V1'
NATIVE_DOMAIN = 'native_prerequisites_only'
MAX_PROTOCOL_BYTES = 16384
FIXTURE_NS = 30_000_000_000
CLEANUP_NS = 5_000_000_000


class ContractError(ValueError):
    def __init__(self):
        super().__init__('fixture_contract_invalid')


class CaseId(str, Enum):
    BASELINE = 'baseline'
    DENY_PRIVATE_PATHS = 'deny_private_paths'
    DENY_PROC_FD = 'deny_proc_fd'
    DENY_PARENT_MUTATION = 'deny_parent_mutation'
    DENY_PROCESS_CREATION = 'deny_process_creation'
    DENY_PRIVILEGED_SYSCALLS = 'deny_privileged_syscalls'
    BYTE_LIMIT = 'byte_limit'
    INODE_LIMIT = 'inode_limit'
    HELD_FILE_REFERENCE = 'held_file_reference'
    HELD_NAMESPACE_REFERENCE = 'held_namespace_reference'
    ANCHOR_RENAME = 'anchor_rename'
    SLOT_CONTENTION = 'slot_contention'
    KILL_CREATOR = 'kill_creator'
    KILL_CUSTODIAN = 'kill_custodian'
    KILL_SUPERVISOR = 'kill_supervisor'
    REJECT_PROFILE = 'reject_profile'
    OUTPUT_CAP = 'output_cap'
    DEADLINE_CAP = 'deadline_cap'
    FAULT_BEFORE_NAMESPACE = 'fault_before_namespace'
    FAULT_AFTER_NAMESPACE = 'fault_after_namespace'
    FAULT_AFTER_STAGE_MOUNT = 'fault_after_stage_mount'
    FAULT_COPY = 'fault_copy'
    FAULT_BEFORE_WORKER_EXEC = 'fault_before_worker_exec'
    FAULT_AFTER_WORKER_EXEC = 'fault_after_worker_exec'
    FAULT_WRITER_SEAL = 'fault_writer_seal'
    FAULT_REFERENCE_DRAIN = 'fault_reference_drain'
    SPAWN_FAILURE = 'spawn_failure'
    LOST_RESPONSE = 'lost_response'
    CLEANUP_PRIORITY = 'cleanup_priority'


MANDATORY_CASES = tuple(CaseId)


class CheckId(str, Enum):
    PROFILE = 'PROFILE'
    UID_GID_MAP = 'UID_GID_MAP'
    NAMESPACE_MOUNTS = 'NAMESPACE_MOUNTS'
    CAP_LSM_FILTER = 'CAP_LSM_FILTER'
    HIGH_FD_CLOSED = 'HIGH_FD_CLOSED'
    POSITIVE_IO = 'POSITIVE_IO'
    PRIVATE_PATH_DENIED = 'PRIVATE_PATH_DENIED'
    PROC_FD_DENIED = 'PROC_FD_DENIED'
    PARENT_MUTATION_DENIED = 'PARENT_MUTATION_DENIED'
    PROCESS_CREATION_DENIED = 'PROCESS_CREATION_DENIED'
    PRIVILEGED_SYSCALL_DENIED = 'PRIVILEGED_SYSCALL_DENIED'
    BYTE_LIMIT = 'BYTE_LIMIT'
    INODE_LIMIT = 'INODE_LIMIT'
    HELD_REFERENCE = 'HELD_REFERENCE'
    ANCHOR_PRESERVED = 'ANCHOR_PRESERVED'
    SLOT_CAPACITY = 'SLOT_CAPACITY'
    STICKY_OCCUPANCY = 'STICKY_OCCUPANCY'
    FAULT_PRIMARY_PRESERVED = 'FAULT_PRIMARY_PRESERVED'
    UNSUPPORTED_REFUSED = 'UNSUPPORTED_REFUSED'
    OUTPUT_BOUNDED = 'OUTPUT_BOUNDED'
    DEADLINE_BOUNDED = 'DEADLINE_BOUNDED'
    OWNED_GRAPH_CLOSED = 'OWNED_GRAPH_CLOSED'


class ReasonCode(str, Enum):
    CHECKS_OBSERVED = 'checks_observed'
    UNSUPPORTED_RUNTIME = 'unsupported_runtime'
    UNSUPPORTED_NAMESPACES = 'unsupported_namespaces'
    UNSUPPORTED_MOUNT = 'unsupported_mount'
    UNSUPPORTED_PIDFD = 'unsupported_pidfd'
    UNSUPPORTED_FD_CLOSE = 'unsupported_fd_close'
    UNSUPPORTED_LSM = 'unsupported_lsm'
    UNSUPPORTED_SYSCALLS = 'unsupported_syscalls'
    BOUNDARY_VIOLATION = 'boundary_violation'
    INVARIANT_VIOLATION = 'invariant_violation'
    FIXTURE_CONTRACT_INVALID = 'fixture_contract_invalid'
    MALFORMED_PROTOCOL = 'malformed_protocol'
    OUTPUT_LIMIT = 'output_limit'
    DEADLINE = 'deadline'
    CHILD_SETUP_FAILED = 'child_setup_failed'
    CHILD_SPAWN_FAILED = 'child_spawn_failed'
    CONTROL_LOST = 'control_lost'
    LOCAL_CLEANUP_UNKNOWN = 'local_cleanup_unknown'
    SLOT_UNAVAILABLE = 'slot_unavailable'
    INTERRUPTED = 'interrupted'


class FixtureStatus(str, Enum):
    OBSERVED = 'OBSERVED'
    UNSUPPORTED = 'UNSUPPORTED'
    FAILED = 'FAILED'
    UNKNOWN = 'UNKNOWN'


class CampaignStatus(str, Enum):
    PASS = 'PASS'
    UNSUPPORTED = 'UNSUPPORTED'
    FAILED = 'FAILED'
    UNKNOWN = 'UNKNOWN'


class NativePrimitive(str, Enum):
    SUBREAPER = 'subreaper'
    UNSHARE_USER_MOUNT_PID = 'unshare_user_mount_pid'
    PIDFD = 'pidfd'
    CLOSE_RANGE = 'close_range'


def _require(condition):
    if not condition:
        raise ContractError()


def _integer(value, maximum=2**63 - 1):
    _require(type(value) is int and 0 <= value <= maximum)


def _hex(value, length):
    _require(type(value) is str and len(value) == length
             and all(character in '0123456789abcdef' for character in value))


def _enum(value, kind):
    _require(type(value) is kind)


def _enum_tuple(value, kind):
    _require(type(value) is tuple and len(value) <= len(kind))
    for item in value:
        _enum(item, kind)
    _require(len(set(value)) == len(value))


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _reject_constant(value):
    raise ContractError()


def _loads(raw, expected_fields):
    _require(type(raw) is bytes and len(raw) <= MAX_PROTOCOL_BYTES)
    try:
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_reject_constant)
        _require(type(value) is dict and set(value) == set(expected_fields))
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ContractError() from None


def _dumps(value):
    try:
        raw = json.dumps(value, ensure_ascii=True, allow_nan=False,
                         sort_keys=True, separators=(',', ':')).encode('ascii')
        _require(len(raw) <= MAX_PROTOCOL_BYTES)
        return raw
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ContractError() from None


def _decode_enum(value, kind):
    _require(type(value) is str)
    try:
        return kind(value)
    except ValueError:
        raise ContractError() from None


def _decode_enum_tuple(value, kind):
    _require(type(value) is list and len(value) <= len(kind))
    return tuple(_decode_enum(item, kind) for item in value)


def _validate_request(request, schema):
    _require(type(request.schema) is str and request.schema == schema)
    _integer(request.source_fd, 2**31 - 1)
    _integer(request.arena_fd, 2**31 - 1)
    _require(request.source_fd != request.arena_fd)
    _require(type(request.profile_id) is str and request.profile_id == PROFILE_ID)
    _integer(request.started_ns)
    _integer(request.deadline_ns)
    _require(request.deadline_ns == request.started_ns + FIXTURE_NS)


@dataclass(frozen=True, slots=True)
class NativePrerequisiteRequest:
    source_fd: int = field(repr=False)
    arena_fd: int = field(repr=False)
    profile_id: str
    started_ns: int
    deadline_ns: int
    schema: str = field(default='maestro.staging-custody-native-prerequisite.v1', init=False)

    def __post_init__(self):
        _validate_request(self, 'maestro.staging-custody-native-prerequisite.v1')

    def to_bytes(self):
        self.__post_init__()
        return _dumps({name: getattr(self, name) for name in
                       ('schema', 'source_fd', 'arena_fd', 'profile_id', 'started_ns', 'deadline_ns')})

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('schema', 'source_fd', 'arena_fd', 'profile_id', 'started_ns', 'deadline_ns'))
        _require(data.pop('schema') == 'maestro.staging-custody-native-prerequisite.v1')
        return cls(**data)


@dataclass(frozen=True, slots=True)
class GateRequest:
    source_fd: int = field(repr=False)
    arena_fd: int = field(repr=False)
    profile_id: str
    case_id: CaseId
    started_ns: int
    deadline_ns: int
    schema: str = field(default='maestro.staging-custody-local-gate.v1', init=False)

    def __post_init__(self):
        _validate_request(self, 'maestro.staging-custody-local-gate.v1')
        _enum(self.case_id, CaseId)

    def to_bytes(self):
        self.__post_init__()
        return _dumps({name: getattr(self, name) for name in
                       ('schema', 'source_fd', 'arena_fd', 'profile_id', 'case_id', 'started_ns', 'deadline_ns')})

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('schema', 'source_fd', 'arena_fd', 'profile_id', 'case_id', 'started_ns', 'deadline_ns'))
        _require(data.pop('schema') == 'maestro.staging-custody-local-gate.v1')
        data['case_id'] = _decode_enum(data['case_id'], CaseId)
        return cls(**data)


def validate_request(request, *, roots):
    """Revalidate immutable values and the exact still-owned borrowed FD binding."""
    from ._staging_arena import ArenaUnavailable, FixtureRoots
    _require(type(request) in (NativePrerequisiteRequest, GateRequest)
             and type(roots) is FixtureRoots)
    request.__post_init__()
    try:
        roots.validate()
        _require(request.source_fd == roots.source_fd and request.arena_fd == roots.arena_fd)
    except (OSError, ArenaUnavailable):
        raise ContractError() from None


def _observation_fields(observation, schema, scope):
    _require(type(observation.schema) is str and observation.schema == schema)
    _require(type(observation.scope) is str and observation.scope == scope)
    _hex(observation.profile_digest, 64)
    _enum(observation.reason_code, ReasonCode)
    _require(observation.local_observation_only is True and observation.non_authoritative is True)


@dataclass(frozen=True, slots=True)
class NativePrerequisiteReceipt:
    status: FixtureStatus
    profile_digest: str
    completed_primitives: tuple[NativePrimitive, ...]
    reason_code: ReasonCode
    schema: str = field(default='maestro.staging-custody-native-prerequisite-observation.v1', init=False)
    stage: str = field(default=NATIVE_DOMAIN, init=False)
    local_observation_only: bool = field(default=True, init=False)
    non_authoritative: bool = field(default=True, init=False)

    def __post_init__(self):
        _require(self.schema == 'maestro.staging-custody-native-prerequisite-observation.v1'
                 and type(self.schema) is str and type(self.stage) is str and self.stage == NATIVE_DOMAIN)
        _enum(self.status, FixtureStatus)
        _hex(self.profile_digest, 64)
        _enum_tuple(self.completed_primitives, NativePrimitive)
        _enum(self.reason_code, ReasonCode)
        _require(self.local_observation_only is True and self.non_authoritative is True)
        if self.status is FixtureStatus.OBSERVED:
            _require(set(self.completed_primitives) == set(NativePrimitive)
                     and self.reason_code is ReasonCode.CHECKS_OBSERVED)
        else:
            _require(self.reason_code is not ReasonCode.CHECKS_OBSERVED)

    def to_bytes(self):
        self.__post_init__()
        return _dumps({name: getattr(self, name) for name in
                       ('schema', 'stage', 'status', 'profile_digest', 'completed_primitives',
                        'reason_code', 'local_observation_only', 'non_authoritative')})

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('schema', 'stage', 'status', 'profile_digest', 'completed_primitives',
                            'reason_code', 'local_observation_only', 'non_authoritative'))
        _require(data.pop('schema') == 'maestro.staging-custody-native-prerequisite-observation.v1'
                 and data.pop('stage') == NATIVE_DOMAIN
                 and data.pop('local_observation_only') is True
                 and data.pop('non_authoritative') is True)
        data['status'] = _decode_enum(data['status'], FixtureStatus)
        data['reason_code'] = _decode_enum(data['reason_code'], ReasonCode)
        data['completed_primitives'] = _decode_enum_tuple(data['completed_primitives'], NativePrimitive)
        return cls(**data)


_ADDITIONAL_CHECKS = (
    CheckId.POSITIVE_IO, CheckId.PRIVATE_PATH_DENIED, CheckId.PROC_FD_DENIED,
    CheckId.PARENT_MUTATION_DENIED, CheckId.PROCESS_CREATION_DENIED, CheckId.PRIVILEGED_SYSCALL_DENIED,
    CheckId.BYTE_LIMIT, CheckId.INODE_LIMIT, CheckId.HELD_REFERENCE, CheckId.HELD_REFERENCE,
    CheckId.ANCHOR_PRESERVED, CheckId.SLOT_CAPACITY,
    CheckId.STICKY_OCCUPANCY, CheckId.STICKY_OCCUPANCY, CheckId.STICKY_OCCUPANCY,
    CheckId.UNSUPPORTED_REFUSED, CheckId.OUTPUT_BOUNDED, CheckId.DEADLINE_BOUNDED,
    *((CheckId.FAULT_PRIMARY_PRESERVED,) * 11),
)
_WORKER_CASES = frozenset(MANDATORY_CASES[:11]) | {
    CaseId.FAULT_AFTER_WORKER_EXEC, CaseId.FAULT_WRITER_SEAL, CaseId.FAULT_REFERENCE_DRAIN,
}


def required_checks(case_id):
    _enum(case_id, CaseId)
    common = {CheckId.PROFILE, CheckId.OWNED_GRAPH_CLOSED,
              _ADDITIONAL_CHECKS[MANDATORY_CASES.index(case_id)]}
    if case_id in _WORKER_CASES:
        common.update((CheckId.UID_GID_MAP, CheckId.NAMESPACE_MOUNTS,
                       CheckId.CAP_LSM_FILTER, CheckId.HIGH_FD_CLOSED))
    return frozenset(common)


@dataclass(frozen=True, slots=True)
class FixtureObservation:
    case_id: CaseId
    status: FixtureStatus
    profile_digest: str
    completed_checks: tuple[CheckId, ...]
    reason_code: ReasonCode
    schema: str = field(default='maestro.staging-custody-local-observation.v1', init=False)
    scope: str = field(default='fixture', init=False)
    local_observation_only: bool = field(default=True, init=False)
    non_authoritative: bool = field(default=True, init=False)

    def __post_init__(self):
        _observation_fields(self, 'maestro.staging-custody-local-observation.v1', 'fixture')
        _enum(self.case_id, CaseId)
        _enum(self.status, FixtureStatus)
        _enum_tuple(self.completed_checks, CheckId)
        if self.status is FixtureStatus.OBSERVED:
            _require(required_checks(self.case_id) <= set(self.completed_checks)
                     and self.reason_code is ReasonCode.CHECKS_OBSERVED)
        else:
            _require(self.reason_code is not ReasonCode.CHECKS_OBSERVED)

    def to_bytes(self):
        self.__post_init__()
        return _dumps({name: getattr(self, name) for name in
                       ('schema', 'scope', 'case_id', 'status', 'profile_digest', 'completed_checks',
                        'reason_code', 'local_observation_only', 'non_authoritative')})

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('schema', 'scope', 'case_id', 'status', 'profile_digest', 'completed_checks',
                            'reason_code', 'local_observation_only', 'non_authoritative'))
        _require(data.pop('schema') == 'maestro.staging-custody-local-observation.v1'
                 and data.pop('scope') == 'fixture'
                 and data.pop('local_observation_only') is True
                 and data.pop('non_authoritative') is True)
        data['case_id'] = _decode_enum(data['case_id'], CaseId)
        data['status'] = _decode_enum(data['status'], FixtureStatus)
        data['completed_checks'] = _decode_enum_tuple(data['completed_checks'], CheckId)
        data['reason_code'] = _decode_enum(data['reason_code'], ReasonCode)
        return cls(**data)


@dataclass(frozen=True, slots=True)
class CampaignObservation:
    status: CampaignStatus
    profile_digest: str
    results: tuple[FixtureObservation, ...]
    reason_code: ReasonCode
    schema: str = field(default='maestro.staging-custody-local-campaign.v1', init=False)
    scope: str = field(default='campaign', init=False)
    local_observation_only: bool = field(default=True, init=False)
    non_authoritative: bool = field(default=True, init=False)

    def __post_init__(self):
        _observation_fields(self, 'maestro.staging-custody-local-campaign.v1', 'campaign')
        _enum(self.status, CampaignStatus)
        _require(type(self.results) is tuple and len(self.results) == len(MANDATORY_CASES))
        for case, result in zip(MANDATORY_CASES, self.results):
            _require(type(result) is FixtureObservation)
            result.__post_init__()
            _require(result.case_id is case and result.profile_digest == self.profile_digest)
        statuses = {item.status for item in self.results}
        expected = (CampaignStatus.UNKNOWN if FixtureStatus.UNKNOWN in statuses else
                    CampaignStatus.FAILED if FixtureStatus.FAILED in statuses else
                    CampaignStatus.UNSUPPORTED if FixtureStatus.UNSUPPORTED in statuses else CampaignStatus.PASS)
        _require(self.status is expected)
        _require((self.reason_code is ReasonCode.CHECKS_OBSERVED) == (self.status is CampaignStatus.PASS))

    def to_bytes(self):
        self.__post_init__()
        data = {name: getattr(self, name) for name in
                ('schema', 'scope', 'status', 'profile_digest', 'reason_code',
                 'local_observation_only', 'non_authoritative')}
        data['results'] = [json.loads(result.to_bytes()) for result in self.results]
        return _dumps(data)

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('schema', 'scope', 'status', 'profile_digest', 'results', 'reason_code',
                            'local_observation_only', 'non_authoritative'))
        _require(data.pop('schema') == 'maestro.staging-custody-local-campaign.v1'
                 and data.pop('scope') == 'campaign'
                 and data.pop('local_observation_only') is True
                 and data.pop('non_authoritative') is True)
        _require(type(data['results']) is list and len(data['results']) == len(MANDATORY_CASES))
        data['results'] = tuple(FixtureObservation.from_bytes(_dumps(item)) for item in data['results'])
        data['status'] = _decode_enum(data['status'], CampaignStatus)
        data['reason_code'] = _decode_enum(data['reason_code'], ReasonCode)
        return cls(**data)


@dataclass(frozen=True, slots=True)
class SlotReservation:
    token: str
    slot_index: int
    arena_generation: str

    def __post_init__(self):
        _hex(self.token, 32)
        _integer(self.slot_index, 1)
        _hex(self.arena_generation, 32)


@dataclass(frozen=True, slots=True)
class Deadline:
    started_ns: int
    deadline_ns: int
    cleanup_deadline_ns: int

    def __post_init__(self):
        for value in (self.started_ns, self.deadline_ns, self.cleanup_deadline_ns):
            _integer(value)
        _require(self.deadline_ns == self.started_ns + FIXTURE_NS
                 and self.started_ns <= self.cleanup_deadline_ns <= self.deadline_ns + CLEANUP_NS)

    def remaining_seconds(self, *, cleanup=False):
        self.__post_init__()
        _require(type(cleanup) is bool)
        end = self.cleanup_deadline_ns if cleanup else self.deadline_ns
        return max(0, end - time.monotonic_ns()) / 1_000_000_000


class OutputBudget:
    __slots__ = ('_used',)

    def __init__(self, limit=MAX_PROTOCOL_BYTES):
        _require(type(limit) is int and limit == MAX_PROTOCOL_BYTES)
        self._used = 0

    @property
    def used(self):
        return self._used

    def charge(self, size):
        _integer(size, 2**64 - 1)
        self._used = min(MAX_PROTOCOL_BYTES + 1, self._used + size)
        return self._used <= MAX_PROTOCOL_BYTES


_CODE_ROOT = Path(__file__).absolute().parent
_MANIFEST_SOURCES = (
    ('code_contract', str(_CODE_ROOT / '_staging_contract.py'), 'file'),
    ('code_arena', str(_CODE_ROOT / '_staging_arena.py'), 'file'),
    ('code_a2_contract', str(_CODE_ROOT / '_staging_a2_contract.py'), 'file'),
    ('code_ownership', str(_CODE_ROOT / '_staging_ownership.py'), 'file'),
    ('code_native', str(_CODE_ROOT / '_staging_native.py'), 'file'),
    ('code_reopen', str(_CODE_ROOT / '_staging_reopen.py'), 'file'),
    ('code_audit', str(_CODE_ROOT / '_staging_audit.py'), 'file'),
    ('code_gate', str(_CODE_ROOT / '_staging_gate.py'), 'file'),
    ('code_landlock', str(_CODE_ROOT / 'landlock.py'), 'file'),
    ('code_capabilities', str(_CODE_ROOT / 'candidate_security.py'), 'file'),
    ('interpreter', '/usr/bin/python3.12', 'file'),
    ('runtime_stdlib', '/usr/lib/python3.12', 'directory'),
    ('runtime_native', '/usr/lib/x86_64-linux-gnu', 'directory'),
    ('runtime_loader', '/usr/lib64', 'directory'),
    ('runtime_orchestrator', str(_CODE_ROOT.parent), 'directory'),
    ('native_libc', '/usr/lib/x86_64-linux-gnu/libc.so.6', 'file'),
    ('native_seccomp', '/usr/lib/x86_64-linux-gnu/libseccomp.so.2', 'file'),
    *((f'stdlib_{name}', f'/usr/lib/python3.12/{name}.py', 'file') for name in
      ('dataclasses', 'enum', 'hashlib', 'json/__init__', 'pathlib', 'struct',
       'ctypes/__init__', 'ctypes/_endian', 'threading')),
    ('stdlib_ctypes_native', '/usr/lib/python3.12/lib-dynload/_ctypes.cpython-312-x86_64-linux-gnu.so', 'file'),
    ('stdlib_resource', '/usr/lib/python3.12/lib-dynload/resource.cpython-312-x86_64-linux-gnu.so', 'file'),
    ('stdlib_subinterpreters', '/usr/lib/python3.12/lib-dynload/_xxsubinterpreters.cpython-312-x86_64-linux-gnu.so', 'file'),
    # fcntl/_thread/os/stat/sys/time are built-in or frozen in the fixed
    # interpreter for this profile; no invented extension pathname is used.
)


@dataclass(frozen=True, slots=True)
class ManifestObject:
    role: str
    resolved_path: str
    kind: str
    device: int
    inode: int
    size: int
    sha256: str | None

    def __post_init__(self):
        _require(type(self.role) is str and self.role in {item[0] for item in _MANIFEST_SOURCES})
        _require(type(self.resolved_path) is str and self.resolved_path.startswith('/')
                 and len(self.resolved_path) <= 4096 and '..' not in Path(self.resolved_path).parts)
        _require(type(self.kind) is str and self.kind in ('file', 'directory'))
        for value in (self.device, self.inode, self.size):
            _integer(value, 2**64 - 1)
        if self.kind == 'file':
            _hex(self.sha256, 64)
        else:
            _require(self.sha256 is None)

    def _data(self):
        self.__post_init__()
        return {name: getattr(self, name) for name in
                ('role', 'resolved_path', 'kind', 'device', 'inode', 'size', 'sha256')}


@dataclass(frozen=True, slots=True)
class ProfileManifest:
    objects: tuple[ManifestObject, ...]
    digest: str
    profile_id: str = field(default=PROFILE_ID, init=False)
    domain: str = field(default=NATIVE_DOMAIN, init=False)
    architecture: str = field(default='x86_64', init=False)
    interpreter: str = field(default='/usr/bin/python3.12', init=False)
    python_version: str = field(default='3.12.3', init=False)

    def _data(self):
        return {'profile_id': self.profile_id, 'domain': self.domain, 'architecture': self.architecture,
                'interpreter': self.interpreter, 'python_version': self.python_version,
                'objects': [item._data() for item in self.objects]}

    def __post_init__(self):
        for name, expected in (('profile_id', PROFILE_ID), ('domain', NATIVE_DOMAIN),
                               ('architecture', 'x86_64'), ('interpreter', '/usr/bin/python3.12'),
                               ('python_version', '3.12.3')):
            _require(type(getattr(self, name)) is str and getattr(self, name) == expected)
        _require(type(self.objects) is tuple and len(self.objects) == len(_MANIFEST_SOURCES))
        for item, (role, path, kind) in zip(self.objects, _MANIFEST_SOURCES):
            _require(type(item) is ManifestObject)
            item.__post_init__()
            _require(item.role == role and item.kind == kind)
            # Only the explicitly declared native soname is allowed to resolve
            # to a different final name; verify_profile_manifest checks it live.
            if role != 'native_seccomp':
                _require(item.resolved_path == path)
            else:
                _require(Path(item.resolved_path).parent == Path(path).parent)
        _hex(self.digest, 64)
        _require(hashlib.sha256(_dumps(self._data())).hexdigest() == self.digest)

    def to_bytes(self):
        self.__post_init__()
        return _dumps({**self._data(), 'digest': self.digest})

    @classmethod
    def from_bytes(cls, raw):
        data = _loads(raw, ('profile_id', 'domain', 'architecture', 'interpreter', 'python_version', 'objects', 'digest'))
        _require(type(data['objects']) is list and len(data['objects']) == len(_MANIFEST_SOURCES))
        objects = []
        for item in data.pop('objects'):
            _require(type(item) is dict and set(item) ==
                     {'role', 'resolved_path', 'kind', 'device', 'inode', 'size', 'sha256'})
            objects.append(ManifestObject(**item))
        manifest = cls(tuple(objects), data.pop('digest'))
        _require(data == {name: getattr(manifest, name) for name in data})
        return manifest


def _manifest_object(role, path, kind):
    # Resolve only this fixed soname; this is not library discovery.
    if role == 'native_seccomp':
        path = str(Path(path).resolve(strict=True))
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    if kind == 'directory':
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    primary = None
    try:
        before = os.fstat(fd)
        _require((stat.S_ISREG(before.st_mode) if kind == 'file' else stat.S_ISDIR(before.st_mode)))
        digest = None
        if kind == 'file':
            hasher = hashlib.sha256()
            while chunk := os.read(fd, 65536):
                hasher.update(chunk)
            digest = hasher.hexdigest()
        after = os.fstat(fd)
        _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                 (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns))
        return ManifestObject(role, path, kind, before.st_dev, before.st_ino, before.st_size, digest)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            os.close(fd)
        except BaseException:
            if primary is None:
                raise
            primary.add_note('owned_fd_close_unconfirmed')


def build_profile_manifest():
    """Hash the fixed current native prefix without spawning or probing support.

    Directory entries record object identity, not a recursive runtime content
    digest. The later owner must validate every actual bind/setup operation.
    """
    try:
        _require(sys.platform == 'linux' and os.uname().machine == 'x86_64'
                 and struct.calcsize('P') == 8 and sys.implementation.name == 'cpython'
                 and sys.version_info[:3] == (3, 12, 3)
                 and os.path.samefile(sys.executable, '/usr/bin/python3.12'))
        with open('/usr/bin/python3.12', 'rb') as executable:
            elf = executable.read(20)
        _require(len(elf) == 20 and elf[:6] == b'\x7fELF\x02\x01'
                 and elf[18:20] == b'\x3e\x00')
        objects = tuple(_manifest_object(*item) for item in _MANIFEST_SOURCES)
        data = {'profile_id': PROFILE_ID, 'domain': NATIVE_DOMAIN, 'architecture': 'x86_64',
                'interpreter': '/usr/bin/python3.12', 'python_version': '3.12.3',
                'objects': [item._data() for item in objects]}
        return ProfileManifest(objects, hashlib.sha256(_dumps(data)).hexdigest())
    except (OSError, ValueError, TypeError):
        raise ContractError() from None


def verify_profile_manifest(manifest):
    _require(type(manifest) is ProfileManifest)
    manifest.__post_init__()
    _require(manifest == build_profile_manifest())
