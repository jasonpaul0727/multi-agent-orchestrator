"""Bounded local test-trace replay, without guard or database access.

The private directory constrains input; its selection is not authorization or
an independent trust root. File metadata checks cannot detect every ABA or
same-size modification. A stuck kernel operation may not be promptly killed.
"""

import ctypes
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

from orchestrator.persistence._epoch_model import EpochModel, ModelState


_MAX_INPUT_BYTES = 65536
_MAX_RECORD_BYTES = 4096
_MAX_RECORDS = 128
_MAX_REQUEST_BYTES = 16384
_MAX_RESPONSE_BYTES = 16384
_HELPER_TIMEOUT_SECONDS = 2.0
_CLEANUP_TIMEOUT_SECONDS = 0.5


@dataclass(frozen=True)
class SimulationReport:
    simulation_only: bool
    non_authoritative: bool
    state: str
    transitions: tuple[str, ...]
    reason: str
    registered_digests_rebuild_required_simulated: int | None = None
    registered_digests_rebuilt_simulated: int | None = None


def _failure(reason='input_unavailable'):
    return SimulationReport(True, True, 'UNCERTAIN', (), reason)


def _snapshot(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_uid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _procfs(fd):
    """Require Linux procfs before following its self/fd magic links."""
    info = ctypes.create_string_buffer(256)
    libc = ctypes.CDLL(None, use_errno=True)
    fstatfs = libc.fstatfs
    fstatfs.argtypes = (ctypes.c_int, ctypes.c_void_p)
    fstatfs.restype = ctypes.c_int
    if fstatfs(fd, info) != 0 or ctypes.c_long.from_buffer(info).value != 0x9FA0:
        raise ValueError('procfs unavailable')


def _read_trace(directory, path):
    if (sys.platform != 'linux' or not hasattr(os, 'O_PATH') or not hasattr(os, 'O_NOFOLLOW')
            or not hasattr(os, 'O_DIRECTORY') or os.open not in os.supports_dir_fd
            or os.stat not in os.supports_dir_fd or os.readlink not in os.supports_dir_fd):
        raise ValueError('unsupported')
    root, file = Path(directory), Path(path)
    if (not root.is_absolute() or not file.is_absolute() or '..' in root.parts
            or '..' in file.parts or file.parent != root or file.name in ('', '.', '..')):
        raise ValueError('invalid path')
    directory_fd = None
    file_fd = None
    anchor_fd = None
    proc_fd = None
    proc_files_fd = None
    try:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        directory_fd = os.open('/', flags)
        for component in root.parts[1:]:
            next_fd = os.open(component, flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        root_before = os.fstat(directory_fd)
        if (not stat.S_ISDIR(root_before.st_mode) or root_before.st_uid != os.getuid()
                or root_before.st_mode & 0o077):
            raise ValueError('invalid directory')
        # O_PATH opens only the directory entry: special files do not receive a
        # data open. Reopen the same pinned inode through Linux procfs only
        # after checking its type and the procfs mount and process identity.
        anchor_fd = os.open(file.name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=directory_fd)
        before = os.fstat(anchor_fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_size > _MAX_INPUT_BYTES):
            raise ValueError('invalid file')
        if _snapshot(os.stat(file.name, dir_fd=directory_fd, follow_symlinks=False)) != _snapshot(before):
            raise ValueError('changed file')
        proc_fd = os.open('/proc', os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        _procfs(proc_fd)
        if os.readlink('self', dir_fd=proc_fd) != str(os.getpid()):
            raise ValueError('unexpected procfs process')
        proc_files_fd = os.open('self/fd', os.O_PATH | os.O_DIRECTORY | os.O_CLOEXEC,
                                dir_fd=proc_fd)
        _procfs(proc_files_fd)
        file_fd = os.open(str(anchor_fd), os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC,
                          dir_fd=proc_files_fd)
        if _snapshot(os.fstat(file_fd)) != _snapshot(before):
            raise ValueError('changed file')
        pieces = []
        count = 0
        while True:
            piece = os.read(file_fd, _MAX_INPUT_BYTES + 1 - count)
            if not piece:
                break
            pieces.append(piece)
            count += len(piece)
            if count > _MAX_INPUT_BYTES:
                raise ValueError('oversized')
        after = os.fstat(file_fd)
        named_after = os.stat(file.name, dir_fd=directory_fd, follow_symlinks=False)
        if (_snapshot(before) != _snapshot(after) or _snapshot(after) != _snapshot(named_after)
                or count != before.st_size or _snapshot(root_before) != _snapshot(os.fstat(directory_fd))):
            raise ValueError('changed file')
        return b''.join(pieces)
    finally:
        for fd in (file_fd, proc_files_fd, proc_fd, anchor_fd, directory_fd):
            if fd is not None:
                os.close(fd)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate key')
        result[key] = value
    return result


def _replay(raw):
    try:
        lines = raw.splitlines()
        if not lines or len(lines) > _MAX_RECORDS or any(len(line) > _MAX_RECORD_BYTES for line in lines):
            raise ValueError('invalid trace')
        model = EpochModel()
        transitions = []
        for line in lines:
            record = json.loads(line.decode('utf-8'), object_pairs_hook=_unique_object,
                                parse_constant=lambda value: (_ for _ in ()).throw(ValueError()))
            if type(record) is not dict:
                raise ValueError('invalid record')
            kind = record.get('kind')
            handlers = {
                'guard_prepare': model.start,
                'db_prepared': model.mark_db_prepared,
                'guard_active': model.activate_simulated,
                'admission_denied': model.admission_denied,
                'operation_begin': model.operation_begin,
                'operation_commit': model.operation_commit,
                'db_commit_unknown': model.db_commit_unknown,
                'db_commit_exception': model.db_commit_exception,
                'guard_lost': model.guard_lost,
                'retire': model.retire,
                'guard_release': model.guard_release,
                'guard_release_failed': model.guard_release_failed,
                'guard_release_unknown': model.guard_release_unknown,
                'process_killed': model.process_killed,
                'digest_register': model.digest_register,
                'digest_rebuild_simulated': model.digest_rebuild_simulated,
                'restart': model.restart,
            }
            if type(kind) is not str or kind not in handlers:
                return SimulationReport(True, True, 'UNCERTAIN', tuple(transitions), 'invalid_trace')
            state = handlers[kind](record)
            transitions.append(state.value)
            if state is ModelState.UNCERTAIN:
                return SimulationReport(True, True, state.value, tuple(transitions), model.reason)
        if model.operation_in_flight:
            return SimulationReport(True, True, 'UNCERTAIN', tuple(transitions), 'operation_incomplete')
        if model.state is ModelState.RETIRED:
            if not model.guard_released:
                return SimulationReport(True, True, 'UNCERTAIN', tuple(transitions), 'guard_release_missing')
            return SimulationReport(True, True, model.state.value, tuple(transitions), 'retired',
                                    model.registered_digests_rebuild_required_simulated, model.registered_digests_rebuilt_simulated)
        if model.restart_pending:
            return SimulationReport(True, True, 'UNCERTAIN', tuple(transitions), 'new_epoch_required')
        if model.state is not ModelState.ACTIVE_SIMULATED:
            return SimulationReport(True, True, 'UNCERTAIN', tuple(transitions), 'incomplete_trace')
        return SimulationReport(True, True, model.state.value, tuple(transitions), 'ok',
                                model.registered_digests_rebuild_required_simulated, model.registered_digests_rebuilt_simulated)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        return _failure('invalid_trace')


def _helper_main():
    try:
        raw = sys.stdin.buffer.read(_MAX_REQUEST_BYTES + 1)
        if len(raw) > _MAX_REQUEST_BYTES:
            raise ValueError('request too large')
        request = json.loads(raw, object_pairs_hook=_unique_object)
        if (type(request) is not dict or set(request) != {'directory', 'path'}
                or any(type(request[key]) is not str for key in request)):
            raise ValueError('invalid request')
        report = _replay(_read_trace(request['directory'], request['path']))
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        report = _failure()
    sys.stdout.buffer.write(json.dumps(asdict(report), separators=(',', ':')).encode('utf-8'))


def _terminate_and_reap(child):
    kill_failed = False
    try:
        child.kill()
    except ProcessLookupError:
        pass
    except OSError:
        kill_failed = True
    try:
        child.communicate(timeout=_CLEANUP_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return not kill_failed and child.returncode is not None


def simulate_journal(directory, path):
    """Read and replay in an owned helper with bounded normal-time cleanup."""
    try:
        payload = json.dumps({'directory': os.fspath(directory), 'path': os.fspath(path)}).encode('utf-8')
        if len(payload) > _MAX_REQUEST_BYTES:
            return _failure()
        child = subprocess.Popen(
            [sys.executable, '-m', 'orchestrator.diagnostics._simulation_trace', '--helper'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    except (OSError, TypeError, ValueError, UnicodeError):
        return _failure()
    try:
        output, _ = child.communicate(payload, timeout=_HELPER_TIMEOUT_SECONDS)
    except (subprocess.TimeoutExpired, OSError):
        if not _terminate_and_reap(child):
            return _failure('helper_cleanup_unconfirmed')
        return _failure()
    except BaseException:
        if not _terminate_and_reap(child):
            return _failure('helper_cleanup_unconfirmed')
        raise
    if child.returncode != 0 or len(output) > _MAX_RESPONSE_BYTES:
        return _failure()
    try:
        data = json.loads(output, object_pairs_hook=_unique_object)
        if (type(data) is not dict or set(data) != {'simulation_only', 'non_authoritative', 'state', 'transitions', 'reason',
                                                       'registered_digests_rebuild_required_simulated', 'registered_digests_rebuilt_simulated'}
                or data['simulation_only'] is not True or data['non_authoritative'] is not True
                or data['state'] not in ('ACTIVE_SIMULATED', 'RETIRED', 'UNCERTAIN')
                or type(data['transitions']) is not list or len(data['transitions']) > _MAX_RECORDS
                or any(item not in ('ABSENT', 'ESTABLISHING', 'ACTIVE_SIMULATED', 'RETIRED', 'UNCERTAIN')
                       for item in data['transitions'])
                or data['reason'] not in ('ok', 'retired', 'input_unavailable', 'invalid_trace', 'incomplete_trace',
                                          'helper_cleanup_unconfirmed', 'commit_unknown', 'commit_exception', 'guard_lost',
                                          'operation_incomplete', 'admission_denied', 'no_receipts',
                                          'database_receipt_missing', 'guard_receipt_missing',
                                          'prepared_before_activation', 'matching_prior_receipts',
                                          'receipt_mismatch', 'malformed_receipt', 'guard_release_missing',
                                          'guard_release_failed', 'guard_release_unknown', 'process_killed',
                                          'new_epoch_required')
                or (data['reason'] == 'ok') != (data['state'] == 'ACTIVE_SIMULATED')
                or (data['reason'] == 'retired') != (data['state'] == 'RETIRED')):
            raise ValueError('invalid response')
        counts = (data['registered_digests_rebuild_required_simulated'], data['registered_digests_rebuilt_simulated'])
        if data['state'] == 'UNCERTAIN':
            if counts != (None, None):
                raise ValueError('invalid response')
        elif (any(type(value) is not int or not 0 <= value <= _MAX_RECORDS for value in counts)
              or sum(counts) > _MAX_RECORDS):
            raise ValueError('invalid response')
        return SimulationReport(True, True, data['state'], tuple(data['transitions']), data['reason'], *counts)
    except (ValueError, TypeError, KeyError, UnicodeError):
        return _failure()


if __name__ == '__main__':
    if sys.argv[1:] != ['--helper']:
        raise SystemExit(2)
    _helper_main()
