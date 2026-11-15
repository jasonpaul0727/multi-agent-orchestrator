"""Two owned fixture slots with durable conservative occupancy, never cleanup.

There is deliberately no release API until a real OwnedGraph can supply closure.
Locks coordinate trusted participants only. This storage cannot detect coherent
rollback/root reinitialization and is not restart-safe global capacity authority.
"""

import _thread
from dataclasses import dataclass, field
import fcntl
import hashlib
import os
from pathlib import Path
import stat

from ._staging_contract import (
    ContractError, ReasonCode, SlotReservation, _dumps, _hex, _integer, _loads, _require,
)


_ROOT_NAME = 'staging-fixture'
_HEADER_NAME = 'arena.header'
_HEADER_SIZE = 1024
_RECORD_SIZE = 512
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_ENTRIES = {_HEADER_NAME, 'slot-0', 'slot-1', 'slot-0.record', 'slot-1.record'}
_HEADER_FIELDS = {'schema', 'generation', 'parent', 'root', 'source', 'arena',
                  'header', 'slots', 'records', 'record_digests', 'record_revisions', 'pending'}
_HEADER_MUTABLE = {'record_digests', 'record_revisions', 'pending'}
_RECORD_FIELDS = {'schema', 'arena_generation', 'slot_index', 'revision', 'token', 'state', 'reason_code'}


class ArenaUnavailable(RuntimeError):
    def __init__(self):
        super().__init__('slot_unavailable')


def _identity(info):
    return (info.st_dev, info.st_ino)


def _check_owned(info, *, directory):
    _require((stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
             and info.st_uid == os.geteuid()
             and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600))
    if not directory:
        _require(info.st_nlink == 1)


def _fd_directory(fd):
    _integer(fd, 2**31 - 1)
    info = os.fstat(fd)
    _check_owned(info, directory=True)
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    _require(flags & os.O_ACCMODE == os.O_RDONLY and not flags & os.O_PATH)
    return info


def _fixed(data, size):
    raw = _dumps(data)
    _require(len(raw) <= size)
    return raw + b' ' * (size - len(raw))


def _read_fixed(fd, size, fields):
    info = os.fstat(fd)
    _check_owned(info, directory=False)
    _require(info.st_size == size)
    raw = os.pread(fd, size + 1, 0)
    _require(len(raw) == size)
    data = _loads(raw, fields)
    _require(_fixed(data, size) == raw)
    return data


def _write_fixed(fd, data, size):
    raw = _fixed(data, size)
    # A short write is uncertainty, not permission to retry a transition.
    _require(os.pwrite(fd, raw, 0) == size)
    os.fsync(fd)


def _close_fds(fds, *, primary=None):
    """Attempt each close once; a body exception always keeps its identity."""
    close_failure = None
    for fd in reversed(fds):
        try:
            os.close(fd)
        except BaseException as exc:
            if close_failure is None or (isinstance(close_failure, Exception) and not isinstance(exc, Exception)):
                close_failure = exc
    if close_failure is not None:
        if primary is not None:
            primary.add_note('owned_fd_close_unconfirmed')
        else:
            raise close_failure


def _safe_close_during_failure(fds):
    # Cleanup never replaces the exception already being propagated.
    for fd in reversed(fds):
        try:
            os.close(fd)
        except BaseException:
            pass


def _open_absolute_directory(path):
    _require(type(path) is Path or isinstance(path, Path))
    _require(path.is_absolute() and '..' not in path.parts)
    current = os.open('/', _DIR_FLAGS)
    try:
        for component in path.parts[1:]:
            following = os.open(component, _DIR_FLAGS, dir_fd=current)
            previous = current
            current = following
            os.close(previous)
        return current
    except BaseException:
        _safe_close_during_failure([current])
        raise


def _directory_matches(fd, expected):
    """Read at most expected+1 names; never materialize an unknown directory."""
    seen = set()
    with os.scandir(fd) as entries:
        for _ in range(len(expected) + 1):
            entry = next(entries, None)
            if entry is None:
                return seen == expected
            if entry.name not in expected or entry.name in seen:
                return False
            seen.add(entry.name)
    return False


def _empty_anchor(arena_fd, name, identity):
    fd = os.open(name, _DIR_FLAGS, dir_fd=arena_fd)
    primary = None
    try:
        _require(list(_identity(_fd_directory(fd))) == identity
                 and _directory_matches(fd, set()))
    except BaseException as exc:
        primary = exc
        raise
    finally:
        _close_fds([fd], primary=primary)


def _binding(header):
    """Validate names through retained directory handles; never delete names."""
    fds = []
    primary = None
    try:
        arena_fd = header[0]
        data = header[1]
        root_fd = os.open('..', _DIR_FLAGS, dir_fd=arena_fd)
        fds.append(root_fd)
        parent_fd = os.open('..', _DIR_FLAGS, dir_fd=root_fd)
        fds.append(parent_fd)
        for fd, identity in ((arena_fd, data['arena']), (root_fd, data['root']),
                             (parent_fd, data['parent'])):
            _require(list(_identity(_fd_directory(fd))) == identity)
        _require(list(_identity(os.stat(_ROOT_NAME, dir_fd=parent_fd, follow_symlinks=False))) == data['root'])
        _require(_directory_matches(root_fd, {'source', 'arena'}))
        for name in ('source', 'arena'):
            info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            _check_owned(info, directory=True)
            _require(list(_identity(info)) == data[name])
    except BaseException as exc:
        primary = exc
        raise
    finally:
        _close_fds(fds, primary=primary)


@dataclass(frozen=True, slots=True)
class FixtureRoots:
    source_fd: int = field(repr=False)
    arena_fd: int = field(repr=False)
    _root_fd: int = field(repr=False)
    _parent_fd: int = field(repr=False)
    _identities: tuple = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def validate(self):
        try:
            _require(not self._closed and type(self._identities) is tuple and len(self._identities) == 4)
            for fd, identity in zip((self._parent_fd, self._root_fd, self.source_fd, self.arena_fd), self._identities):
                _require(_identity(_fd_directory(fd)) == identity)
            _require(_identity(os.stat(_ROOT_NAME, dir_fd=self._parent_fd, follow_symlinks=False)) == self._identities[1])
            _require(_directory_matches(self._root_fd, {'source', 'arena'}))
            for name, identity in (('source', self._identities[2]), ('arena', self._identities[3])):
                _require(_identity(os.stat(name, dir_fd=self._root_fd, follow_symlinks=False)) == identity)
        except (OSError, ContractError):
            raise ArenaUnavailable() from None

    def close(self):
        if self._closed:
            return
        object.__setattr__(self, '_closed', True)
        owned = []
        for fd, identity in zip((self._parent_fd, self._root_fd, self.source_fd, self.arena_fd), self._identities):
            try:
                # A caller violating borrowing must not make us close a reused FD.
                if _identity(os.fstat(fd)) == identity:
                    owned.append(fd)
            except OSError:
                pass
        _close_fds(owned)


def _new_record(generation, index):
    return {'schema': 'maestro.staging-slot.v1', 'arena_generation': generation,
            'slot_index': index, 'revision': 0, 'token': None, 'state': 'FREE', 'reason_code': None}


def _record_digest(record):
    return hashlib.sha256(_fixed(record, _RECORD_SIZE)).hexdigest()


def create_fixture_roots(parent: Path) -> FixtureRoots:
    """Create one fresh private root; failures leave it refused, never repaired."""
    fds = []
    try:
        parent_fd = _open_absolute_directory(parent)
        fds.append(parent_fd)
        _fd_directory(parent_fd)
        os.mkdir(_ROOT_NAME, mode=0o700, dir_fd=parent_fd)
        root_fd = os.open(_ROOT_NAME, _DIR_FLAGS, dir_fd=parent_fd)
        fds.append(root_fd)
        os.mkdir('source', mode=0o700, dir_fd=root_fd)
        source_fd = os.open('source', _DIR_FLAGS, dir_fd=root_fd)
        fds.append(source_fd)
        os.mkdir('arena', mode=0o700, dir_fd=root_fd)
        arena_fd = os.open('arena', _DIR_FLAGS, dir_fd=root_fd)
        fds.append(arena_fd)
        identities = tuple(_identity(_fd_directory(fd)) for fd in fds)
        generation = os.urandom(16).hex()
        metadata_fds = []
        try:
            header_fd = os.open(_HEADER_NAME, _FILE_FLAGS | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=arena_fd)
            metadata_fds.append(header_fd)
            records = []
            record_digests = []
            slots = []
            for index in range(2):
                os.mkdir(f'slot-{index}', mode=0o700, dir_fd=arena_fd)
                slots.append(list(_identity(os.stat(f'slot-{index}', dir_fd=arena_fd, follow_symlinks=False))))
                fd = os.open(f'slot-{index}.record', _FILE_FLAGS | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=arena_fd)
                metadata_fds.append(fd)
                record = _new_record(generation, index)
                _write_fixed(fd, record, _RECORD_SIZE)
                record_digests.append(_record_digest(record))
                records.append(list(_identity(os.fstat(fd))))
            header = {'schema': 'maestro.staging-arena.v1', 'generation': generation,
                      'parent': list(identities[0]), 'root': list(identities[1]),
                      'source': list(identities[2]), 'arena': list(identities[3]),
                      'header': list(_identity(os.fstat(header_fd))), 'slots': slots,
                      'records': records, 'record_digests': record_digests,
                      'record_revisions': [0, 0], 'pending': None}
            _write_fixed(header_fd, header, _HEADER_SIZE)
            for fd in (arena_fd, source_fd, root_fd, parent_fd):
                os.fsync(fd)
        except BaseException:
            _safe_close_during_failure(metadata_fds)
            raise
        else:
            _close_fds(metadata_fds)
        roots = FixtureRoots(source_fd, arena_fd, root_fd, parent_fd, identities)
        roots.validate()
        return roots
    except BaseException as exc:
        _safe_close_during_failure(fds)
        if isinstance(exc, (OSError, ContractError)):
            raise ArenaUnavailable() from None
        raise


def _validate_header(data):
    _require(data['schema'] == 'maestro.staging-arena.v1')
    _hex(data['generation'], 32)
    for name in ('parent', 'root', 'source', 'arena', 'header'):
        _validate_identity(data[name])
    for name in ('slots', 'records'):
        _require(type(data[name]) is list and len(data[name]) == 2)
        for identity in data[name]:
            _validate_identity(identity)
    identities = [tuple(data[name]) for name in ('parent', 'root', 'source', 'arena', 'header')]
    identities += [tuple(item) for name in ('slots', 'records') for item in data[name]]
    _require(len(identities) == len(set(identities)))
    _require(type(data['record_digests']) is list and len(data['record_digests']) == 2
             and type(data['record_revisions']) is list and len(data['record_revisions']) == 2)
    for digest, revision in zip(data['record_digests'], data['record_revisions']):
        _hex(digest, 64)
        _integer(revision)
    # A durable pending transition is uncertainty, never repair permission.
    _require(data['pending'] is None)


def _validate_identity(value):
    _require(type(value) is list and len(value) == 2)
    for item in value:
        _integer(item, 2**64 - 1)


def _validate_record(data, generation, index, revision):
    _require(data['schema'] == 'maestro.staging-slot.v1' and data['arena_generation'] == generation
             and type(data['slot_index']) is int and data['slot_index'] == index
             and type(data['revision']) is int and data['revision'] == revision
             and type(data['state']) is str and data['state'] in ('FREE', 'RESERVED', 'UNKNOWN'))
    if data['state'] == 'FREE':
        _require(data['token'] is None and data['reason_code'] is None)
    else:
        _hex(data['token'], 32)
        if data['state'] == 'RESERVED':
            _require(data['reason_code'] is None)
        else:
            _require(type(data['reason_code']) is str
                     and data['reason_code'] in {item.value for item in ReasonCode if item is not ReasonCode.CHECKS_OBSERVED})


class SlotArena:
    """One process-owned handle; reopen after fork, never reuse an inherited lock.

    Each open gets an independent lock-file description. Same-handle concurrent
    or reentrant use refuses rather than bypassing flock's OFD semantics.
    """
    __slots__ = ('_arena_fd', '_header_fd', '_record_fds', '_header', '_poisoned', '_closed', '_pid', '_guard')

    def __init__(self, arena_fd, header_fd, record_fds):
        self._arena_fd = arena_fd
        self._header_fd = header_fd
        self._record_fds = record_fds
        self._header = None
        self._poisoned = False
        self._closed = False
        self._pid = os.getpid()
        self._guard = _thread.allocate_lock()

    @classmethod
    def open_existing(cls, arena_fd: int):
        fds = []
        try:
            _fd_directory(arena_fd)
            owned_arena_fd = os.open('.', _DIR_FLAGS, dir_fd=arena_fd)
            fds.append(owned_arena_fd)
            # Never dup a lock FD: independent opens must actually contend.
            header_fd = os.open(_HEADER_NAME, _FILE_FLAGS, dir_fd=owned_arena_fd)
            fds.append(header_fd)
            records = []
            for index in range(2):
                fd = os.open(f'slot-{index}.record', _FILE_FLAGS, dir_fd=owned_arena_fd)
                fds.append(fd)
                records.append(fd)
            arena = cls(owned_arena_fd, header_fd, tuple(records))
            arena._enter()
            primary = None
            try:
                arena._read()
            except BaseException as exc:
                primary = exc
                raise
            finally:
                arena._leave(primary)
            return arena
        except BaseException as exc:
            _safe_close_during_failure(fds)
            if isinstance(exc, (OSError, ContractError)):
                raise ArenaUnavailable() from None
            raise

    def _enter(self):
        if self._closed or self._poisoned or self._pid != os.getpid() or not self._guard.acquire(False):
            raise ArenaUnavailable()
        try:
            fcntl.flock(self._header_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._guard.release()
            raise ArenaUnavailable() from None
        except BaseException:
            self._guard.release()
            raise

    def _leave(self, primary=None):
        try:
            fcntl.flock(self._header_fd, fcntl.LOCK_UN)
        except BaseException as exc:
            self._poisoned = True
            if primary is None:
                if isinstance(exc, (OSError, ContractError)):
                    raise ArenaUnavailable() from None
                raise
            primary.add_note('slot_unlock_unconfirmed')
        finally:
            self._guard.release()

    def _read(self):
        _fd_directory(self._arena_fd)
        _require(_directory_matches(self._arena_fd, _ENTRIES))
        header = _read_fixed(self._header_fd, _HEADER_SIZE, _HEADER_FIELDS)
        _validate_header(header)
        if self._header is not None:
            # Other independently locked participants may advance the record
            # commitments, but cannot change bindings or roll back a revision
            # this live handle has already observed.
            _require(all(header[name] == self._header[name] for name in _HEADER_FIELDS - _HEADER_MUTABLE))
            for index in range(2):
                previous = self._header['record_revisions'][index]
                current = header['record_revisions'][index]
                _require(current >= previous)
                if current == previous:
                    _require(header['record_digests'][index] == self._header['record_digests'][index])
        _require(list(_identity(os.fstat(self._header_fd))) == header['header'])
        _binding((self._arena_fd, header))
        for name, identity in ((_HEADER_NAME, header['header']),
                               *((f'slot-{i}', header['slots'][i]) for i in range(2)),
                               *((f'slot-{i}.record', header['records'][i]) for i in range(2))):
            info = os.stat(name, dir_fd=self._arena_fd, follow_symlinks=False)
            _check_owned(info, directory=name in ('slot-0', 'slot-1'))
            _require(list(_identity(info)) == identity)
            if name in ('slot-0', 'slot-1'):
                _empty_anchor(self._arena_fd, name, identity)
        records = []
        for index, fd in enumerate(self._record_fds):
            _require(list(_identity(os.fstat(fd))) == header['records'][index])
            record = _read_fixed(fd, _RECORD_SIZE, _RECORD_FIELDS)
            _validate_record(record, header['generation'], index, header['record_revisions'][index])
            _require(_record_digest(record) == header['record_digests'][index])
            records.append(record)
        occupied = [record['token'] for record in records if record['state'] != 'FREE']
        _require(len(occupied) == len(set(occupied)))
        self._header = header
        return records

    def _transition(self, index, record):
        # Persist intent before changing a slot. After intent is durable, later
        # failures leave pending or occupied state. If no write happens, a fresh
        # opener may still see FREE, but this failed reserve returns no token.
        self._poisoned = True
        revision = self._header['record_revisions'][index] + 1
        _integer(revision)
        record = {**record, 'revision': revision}
        committed = {**self._header, 'record_digests': list(self._header['record_digests']),
                     'record_revisions': list(self._header['record_revisions'])}
        committed['record_digests'][index] = _record_digest(record)
        committed['record_revisions'][index] = revision
        pending = {**self._header, 'pending': {'slot_index': index, 'token': record['token'], 'revision': revision}}
        _write_fixed(self._header_fd, pending, _HEADER_SIZE)
        _write_fixed(self._record_fds[index], record, _RECORD_SIZE)
        _write_fixed(self._header_fd, committed, _HEADER_SIZE)
        self._header = committed
        self._poisoned = False

    def reserve(self) -> SlotReservation | None:
        entered = False
        primary = None
        try:
            self._enter()
            entered = True
            records = self._read()
            for index, record in enumerate(records):
                if record['state'] == 'FREE':
                    token = os.urandom(16).hex()
                    _require(token not in {item['token'] for item in records})
                    reservation = SlotReservation(token, index, self._header['generation'])
                    self._transition(index, {**record, 'state': 'RESERVED', 'token': token})
                    return reservation
            return None
        except BaseException as exc:
            primary = exc
            if isinstance(exc, (OSError, ContractError)):
                self._poisoned = True
                raise ArenaUnavailable() from None
            raise
        finally:
            if entered:
                self._leave(primary)

    def mark_unknown(self, reservation: SlotReservation, reason: ReasonCode) -> None:
        entered = False
        primary = None
        try:
            _require(type(reservation) is SlotReservation and type(reason) is ReasonCode
                     and reason is not ReasonCode.CHECKS_OBSERVED)
            reservation.__post_init__()
            self._enter()
            entered = True
            records = self._read()
            record = records[reservation.slot_index]
            if (reservation.arena_generation != self._header['generation']
                    or reservation.token != record['token'] or record['state'] == 'FREE'):
                raise ArenaUnavailable()
            if record['state'] == 'UNKNOWN':
                return
            self._transition(reservation.slot_index, {**record, 'state': 'UNKNOWN', 'reason_code': reason.value})
        except BaseException as exc:
            primary = exc
            if isinstance(exc, (OSError, ContractError)):
                self._poisoned = True
                raise ArenaUnavailable() from None
            raise
        finally:
            if entered:
                self._leave(primary)

    def close(self):
        if self._closed:
            return
        if not self._guard.acquire(False):
            raise ArenaUnavailable()
        try:
            self._closed = True
            _close_fds([self._arena_fd, self._header_fd, *self._record_fds])
        finally:
            self._guard.release()
