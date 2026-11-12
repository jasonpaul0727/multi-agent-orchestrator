"""Bounded pathname observations, not SQLite handle or continuity proof.

All filesystem calls run in a supervised private child. The samples can race
with replacement, and sidecars can legitimately be created or removed by SQLite.
"""

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import stat
import subprocess
import sys


_TIMEOUT = 2.0
_CLEANUP_TIMEOUT = 0.5
_MAX_INPUT = 65536
_MAX_OUTPUT = 16384
_ROLES = ('db', 'wal', 'shm')
_STATES = ('present', 'missing', 'symlink_seen', 'nonregular', 'unavailable')


@dataclass(frozen=True)
class StorageObservation:
    findings: tuple[str, ...]
    preflight_ok: bool
    reason: str | None = None
    path_sample: tuple[tuple[int, int, int, int], ...] = field(default=(), repr=False)
    sidecar_sample: tuple = field(default=(), repr=False)


def _failed(reason='database_unavailable'):
    return StorageObservation((), False, reason)


def _observe(path):
    """Helper only: lstat samples names without opening the sampled objects."""
    if not sys.platform.startswith('linux') or not hasattr(os, 'lstat'):
        return StorageObservation(('storage_unsupported',), False, 'database_unavailable')
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        return StorageObservation(('path_invalid',), False, 'database_unavailable')
    if len(path.parents) > 64:
        return StorageObservation(('path_invalid',), False, 'database_unavailable')
    path_samples = []
    sidecar_samples = []
    try:
        for parent in reversed(path.parents):
            info = os.lstat(parent)
            if stat.S_ISLNK(info.st_mode):
                return StorageObservation(('parent_symlink_seen',), False, 'database_unavailable')
            if not stat.S_ISDIR(info.st_mode):
                return StorageObservation(('parent_non_directory',), False, 'database_unavailable')
            # A directory's link count changes when an unrelated child is created.
            path_samples.append((info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode), 0))
    except OSError:
        return StorageObservation(('parent_unavailable',), False, 'database_unavailable')
    findings = []
    preflight_ok = True
    for role, item in zip(_ROLES, (path, Path(str(path) + '-wal'), Path(str(path) + '-shm'))):
        try:
            info = os.lstat(item)
        except FileNotFoundError:
            findings.append(role + '_missing')
            if role != 'db':
                sidecar_samples.append(None)
            if role == 'db':
                preflight_ok = False
            continue
        except OSError:
            findings.append(role + '_unavailable')
            preflight_ok = False
            continue
        if stat.S_ISLNK(info.st_mode):
            findings.append(role + '_symlink_seen')
            preflight_ok = False
        elif not stat.S_ISREG(info.st_mode):
            findings.append(role + '_nonregular')
            preflight_ok = False
        else:
            findings.append(role + '_present')
            if role == 'db':
                path_samples.append((info.st_dev, info.st_ino, info.st_mode, info.st_nlink))
            else:
                sidecar_samples.append((info.st_dev, info.st_ino, info.st_mode, info.st_nlink))
            if info.st_nlink > 1:
                findings.append(role + '_link_count_gt_one')
    return StorageObservation(tuple(findings), preflight_ok, None if preflight_ok else 'database_unavailable',
                              tuple(path_samples) if preflight_ok else (),
                              tuple(sidecar_samples) if preflight_ok else ())


def observe_storage(path):
    """Observe path roles under a deadline; no sampled handle escapes the child."""
    if not isinstance(path, (str, Path)):
        raise TypeError('database path required')
    try:
        payload = json.dumps({'path': str(path)}).encode('utf-8')
        if len(payload) > _MAX_INPUT:
            return _failed()
        child = subprocess.Popen([sys.executable, '-m', __name__, '--helper'],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
    except (OSError, ValueError, UnicodeError):
        return _failed()
    try:
        output, _ = child.communicate(payload, timeout=_TIMEOUT)
    except (subprocess.TimeoutExpired, OSError):
        try:
            child.kill()
        except ProcessLookupError:
            pass
        except OSError:
            return _failed('helper_cleanup_unconfirmed')
        try:
            child.communicate(timeout=_CLEANUP_TIMEOUT)
        except (OSError, subprocess.TimeoutExpired):
            return _failed('helper_cleanup_unconfirmed')
        return _failed()
    except BaseException as exc:
        cleanup_unconfirmed = False
        try:
            child.kill()
        except ProcessLookupError:
            pass
        except OSError:
            cleanup_unconfirmed = True
        try:
            child.communicate(timeout=_CLEANUP_TIMEOUT)
        except (OSError, subprocess.TimeoutExpired):
            cleanup_unconfirmed = True
        if cleanup_unconfirmed:
            exc.add_note('helper_cleanup_unconfirmed')
        raise
    if child.returncode != 0 or len(output) > _MAX_OUTPUT:
        return _failed()
    try:
        data = json.loads(output)
        if set(data) != {'findings', 'preflight_ok', 'reason', 'path_sample', 'sidecar_sample'} or type(data['preflight_ok']) is not bool:
            raise ValueError
        findings = data['findings']
        allowed = {role + '_' + state for role in _ROLES for state in _STATES}
        allowed |= {role + '_link_count_gt_one' for role in _ROLES}
        allowed |= {'path_invalid', 'parent_symlink_seen', 'parent_non_directory',
                    'parent_unavailable', 'storage_unsupported'}
        if (not isinstance(findings, list) or len(findings) > 6
                or any(type(value) is not str or value not in allowed for value in findings)
                or len(findings) != len(set(findings))
                or data['reason'] not in (None, 'database_unavailable')
                or (data['preflight_ok'] != (data['reason'] is None))):
            raise ValueError
        path_sample = data['path_sample']
        if (not isinstance(path_sample, list) or len(path_sample) > 65
                or any(not isinstance(item, list) or len(item) != 4
                       or any(type(value) is not int or value < 0 or value >= 2**64 for value in item)
                       for item in path_sample)):
            raise ValueError
        sidecar_sample = data['sidecar_sample']
        if (not isinstance(sidecar_sample, list) or len(sidecar_sample) not in (0, 2)
                or any(item is not None and (not isinstance(item, list) or len(item) != 4
                       or any(type(value) is not int or value < 0 or value >= 2**64 for value in item)
                       or not stat.S_ISREG(item[2])) for item in sidecar_sample)):
            raise ValueError
        if data['preflight_ok']:
            if (len(sidecar_sample) != 2
                    or any((sidecar_sample[i] is None) != (role + '_missing' in findings)
                           for i, role in enumerate(('wal', 'shm')))
                    or not path_sample or 'db_present' not in findings
                    or any(sum(role + '_' + state in findings for state in _STATES) != 1
                           for role in _ROLES)
                    or any(role + '_' + state in findings for role in _ROLES
                           for state in ('symlink_seen', 'nonregular', 'unavailable'))):
                raise ValueError
        elif path_sample or sidecar_sample:
            raise ValueError
        sample = StorageObservation(tuple(findings), data['preflight_ok'], data['reason'],
                                    tuple(tuple(item) for item in path_sample),
                                    tuple(None if item is None else tuple(item) for item in sidecar_sample))
        _validate_comparison_sample(sample)
        return sample
    except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
        return _failed()


def _helper_main():
    try:
        raw = sys.stdin.buffer.read(_MAX_INPUT + 1)
        if len(raw) > _MAX_INPUT:
            raise ValueError
        data = json.loads(raw)
        if set(data) != {'path'} or not isinstance(data['path'], str):
            raise ValueError
        result = _observe(data['path'])
        output = json.dumps({'findings': result.findings, 'preflight_ok': result.preflight_ok,
                             'reason': result.reason, 'path_sample': result.path_sample,
                             'sidecar_sample': result.sidecar_sample}, separators=(',', ':')).encode('utf-8')
    except (ValueError, TypeError, KeyError, OSError, UnicodeError):
        output = b'{"findings":[],"preflight_ok":false,"reason":"database_unavailable","path_sample":[],"sidecar_sample":[]}'
    sys.stdout.buffer.write(output)


@dataclass(frozen=True)
class StorageComparison:
    findings: tuple[str, ...]
    unknowns: tuple[str, ...]


def _validate_comparison_sample(sample):
    """Validate bounded private data; this never establishes storage authority."""
    if type(sample) is not StorageObservation:
        raise TypeError('storage observation required')
    allowed = {role + '_' + state for role in _ROLES for state in _STATES}
    allowed |= {role + '_link_count_gt_one' for role in _ROLES}
    allowed |= {'path_invalid', 'parent_symlink_seen', 'parent_non_directory',
                'parent_unavailable', 'storage_unsupported'}
    if (type(sample.findings) is not tuple or len(sample.findings) > 6
            or any(type(value) is not str or value not in allowed for value in sample.findings)
            or len(set(sample.findings)) != len(sample.findings)
            or type(sample.preflight_ok) is not bool
            or (sample.reason is not None and type(sample.reason) is not str)
            or type(sample.path_sample) is not tuple or type(sample.sidecar_sample) is not tuple):
        raise ValueError('invalid storage observation')
    if not sample.preflight_ok:
        if (sample.reason not in ('database_unavailable', 'helper_cleanup_unconfirmed')
                or sample.path_sample or sample.sidecar_sample):
            raise ValueError('invalid storage observation')
        if not sample.findings:
            return
        if sample.reason == 'helper_cleanup_unconfirmed':
            raise ValueError('invalid storage observation')
        single_failures = {'path_invalid', 'parent_symlink_seen', 'parent_non_directory',
                           'parent_unavailable', 'storage_unsupported'}
        if len(sample.findings) == 1 and sample.findings[0] in single_failures:
            return
        states = {role: [state for state in _STATES if role + '_' + state in sample.findings]
                  for role in _ROLES}
        if (any(len(values) != 1 for values in states.values())
                or not any(states[role][0] in ('symlink_seen', 'nonregular', 'unavailable')
                           or (role == 'db' and states[role][0] == 'missing') for role in _ROLES)
                or any(role + '_link_count_gt_one' in sample.findings and states[role][0] != 'present'
                       for role in _ROLES)
                or set(sample.findings) & single_failures):
            raise ValueError('invalid storage observation')
        return
    if sample.reason is not None or not 2 <= len(sample.path_sample) <= 65 or len(sample.sidecar_sample) != 2:
        raise ValueError('invalid storage observation')
    def valid_identity(item):
        return (type(item) is tuple and len(item) == 4
                and all(type(v) is int and 0 <= v < 2**64 for v in item))
    if (any(not valid_identity(item) for item in sample.path_sample)
            or any(item[2] != stat.S_IFDIR or item[3] != 0 for item in sample.path_sample[:-1])
            or not stat.S_ISREG(sample.path_sample[-1][2]) or sample.path_sample[-1][3] < 1
            or any(item is not None and (not valid_identity(item) or not stat.S_ISREG(item[2]) or item[3] < 1)
                   for item in sample.sidecar_sample)):
        raise ValueError('invalid storage observation')
    expected = ['db_present']
    values = (sample.path_sample[-1], *sample.sidecar_sample)
    for role, item in zip(_ROLES, values):
        if role != 'db':
            expected.append(role + ('_missing' if item is None else '_present'))
        if item is not None and item[3] > 1:
            expected.append(role + '_link_count_gt_one')
    if set(expected) != set(sample.findings):
        raise ValueError('invalid storage observation')


def compare_storage_observations(before, after):
    """Compare two discrete pathname samples without inferring continuity."""
    _validate_comparison_sample(before)
    _validate_comparison_sample(after)
    unknowns = ('between_samples_unobserved', 'sqlite_handle_binding_unproven',
                'pathname_samples_may_race')
    if not before.preflight_ok or not after.preflight_ok:
        return StorageComparison(('parent_unknown',) + tuple(role + '_unknown' for role in _ROLES), unknowns)
    first = (before.path_sample[-1], *before.sidecar_sample)
    second = (after.path_sample[-1], *after.sidecar_sample)
    parent_status = ('parent_sample_matches' if before.path_sample[:-1] == after.path_sample[:-1]
                     else 'parent_identity_changed')
    findings = [parent_status]
    for role, left, right in zip(_ROLES, first, second):
        if left is None and right is not None:
            status = 'missing_to_present'
        elif left is not None and right is None:
            status = 'present_to_missing'
        elif left is None:
            status = 'sample_matches'
        elif left[:2] != right[:2]:
            status = 'identity_changed'
        elif left[2:] != right[2:]:
            status = 'metadata_changed'
        else:
            status = 'sample_matches'
        findings.append(role + '_' + status)
    return StorageComparison(tuple(findings), unknowns)


if __name__ == '__main__' and sys.argv[1:] == ['--helper']:
    _helper_main()
