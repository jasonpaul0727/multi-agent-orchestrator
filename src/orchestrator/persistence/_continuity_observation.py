"""Bounded SQLite observations for local diagnostics, never authority.

The observer owns a single mode=ro connection and read transaction. SQLite
may use or create WAL/SHM sidecars even for a read-only connection. Path
checks and this snapshot do not attest SQLite's actual handles or other
connections, and neither prove schema continuity nor writer inventory.
Observed regular sidecar changes are unknowns, not evidence of damage; samples
cannot detect intervening changes or bind the handles SQLite actually used.
"""

from dataclasses import asdict, dataclass
from enum import Enum
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
import sqlite3
from urllib.parse import quote


class ObservationReason(str, Enum):
    WRITER_INVENTORY_UNPROVEN = 'writer_inventory_unproven'
    CROSS_CONNECTION_TEMP_UNOBSERVABLE = 'cross_connection_temp_unobservable'
    BUDGET_EXHAUSTED = 'budget_exhausted'
    DATABASE_UNAVAILABLE = 'database_unavailable'
    METADATA_INVALID = 'metadata_invalid'
    HELPER_CLEANUP_UNCONFIRMED = 'helper_cleanup_unconfirmed'
    SIDECAR_CHANGED = 'sidecar_changed'


_UNKNOWNS = (ObservationReason.WRITER_INVENTORY_UNPROVEN,
             ObservationReason.CROSS_CONNECTION_TEMP_UNOBSERVABLE)


class _Exhausted(Exception):
    pass


class _InvalidMetadata(Exception):
    pass


@dataclass(frozen=True)
class ObservationLimits:
    """Immutable limits for fetched rows and metadata bytes.

    These counters do not bound SQLite page I/O or Python object overhead.
    Each private run creates its own counters.
    """

    max_rows: int = 1024
    max_bytes: int = 4 * 1024 * 1024
    max_queries: int = 2048
    max_metadata_bytes: int = 65536

    def __post_init__(self):
        limits = ((self.max_rows, 4096), (self.max_bytes, 8 * 1024 * 1024),
                  (self.max_queries, 8192), (self.max_metadata_bytes, 65536))
        if any(type(value) is not int or value < 0 or value > maximum
               for value, maximum in limits):
            raise ValueError('invalid observation limits')


@dataclass
class _Budget:
    limits: ObservationLimits
    rows: int = 0
    bytes: int = 0
    queries: int = 0

    @property
    def max_rows(self):
        return self.limits.max_rows

    @property
    def max_metadata_bytes(self):
        return self.limits.max_metadata_bytes

    def _charge(self, *, rows=0, bytes_=0, queries=0):
        if (self.rows + rows > self.limits.max_rows or self.bytes + bytes_ > self.limits.max_bytes
                or self.queries + queries > self.limits.max_queries):
            raise _Exhausted
        self.rows += rows
        self.bytes += bytes_
        self.queries += queries


@dataclass(frozen=True)
class SchemaObject:
    kind: str
    name: str
    ddl_identity: str
    unique: bool | None


@dataclass(frozen=True)
class SchemaObservation:
    objects: tuple[SchemaObject, ...]
    recursive_triggers: int | None
    reasons: tuple[ObservationReason, ...]
    temp_objects: tuple[SchemaObject, ...] = ()


def _read_schema(conn, budget, schema):
    if schema not in ('main', 'temp'):
        raise ValueError('invalid schema')
    # Fixed-size metadata comes back before any untrusted text. The query has
    # a remaining+1 sentinel, but we never fetch it without row budget. If no
    # row remains for EOF detection, fail conservatively instead.
    budget._charge(queries=1, bytes_=128)
    cursor = conn.cursor()
    cursor.row_factory = None
    cursor.execute(
        'SELECT rowid, typeof(type), length(CAST(type AS BLOB)), '
        'typeof(name), length(CAST(name AS BLOB)), '
        f'typeof(sql), length(CAST(sql AS BLOB)) FROM {schema}.sqlite_master LIMIT ?',
        (budget.max_rows - budget.rows + 1,),
    )
    metadata = []
    while True:
        budget._charge(rows=1, bytes_=128)
        row = cursor.fetchone()
        if row is None:
            break
        rowid, kt, kl, nt, nl, st, sl = row
        if (type(rowid) is not int or kt != 'text' or nt != 'text'
                or type(kl) is not int or type(nl) is not int
                or st not in ('text', 'null') or (st == 'text' and type(sl) is not int)
                or (st == 'null' and sl is not None)):
            raise _InvalidMetadata
        if max(kl, nl, sl or 0) > budget.max_metadata_bytes:
            raise _Exhausted
        metadata.append((rowid, kl, nl, sl or 0, st))

    objects = []
    for rowid, kl, nl, sl, sql_type in metadata:
        budget._charge(queries=1, rows=1, bytes_=kl + nl + sl + 128)
        cursor.execute(f'SELECT type, name, sql FROM {schema}.sqlite_master WHERE rowid=?', (rowid,))
        row = cursor.fetchone()
        if row is None:
            raise _InvalidMetadata
        kind, name, ddl = row
        if (not isinstance(kind, str) or not isinstance(name, str)
                or (sql_type == 'text' and not isinstance(ddl, str))
                or (sql_type == 'null' and ddl is not None)
                or len(kind.encode('utf-8')) != kl or len(name.encode('utf-8')) != nl
                or (ddl is not None and len(ddl.encode('utf-8')) != sl)):
            raise _InvalidMetadata
        # Null is distinct from every valid DDL string.
        identity = hashlib.sha256(b'null' if ddl is None else b'text:' + ddl.encode('utf-8')).hexdigest()
        unique = None
        if kind == 'index':
            # Index names are already preflighted; this scalar result is bounded.
            budget._charge(queries=1, rows=1, bytes_=128 + nl)
            cursor.execute(
                f'SELECT il."unique" FROM {schema}.sqlite_master AS m, '
                'pragma_index_list(m.tbl_name, ?) AS il WHERE m.rowid=? AND il.name=? LIMIT 1',
                (schema, rowid, name),
            )
            found = cursor.fetchone()
            if found is None or found[0] not in (0, 1):
                raise _InvalidMetadata
            unique = bool(found[0])
        objects.append(SchemaObject(kind, name, identity, unique))
    return tuple(objects)


def _read_recursive(conn, budget):
    budget._charge(queries=1, rows=1, bytes_=128)
    cursor = conn.cursor()
    cursor.row_factory = None
    cursor.execute('PRAGMA recursive_triggers')
    recursive = cursor.fetchone()
    if recursive is None or recursive[0] not in (0, 1):
        raise _InvalidMetadata
    return recursive[0]


_HELPER_TIMEOUT_SECONDS = 2.0
_CLEANUP_TIMEOUT_SECONDS = 0.5
_MAX_INPUT_BYTES = 65536
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def _failed(reason=ObservationReason.DATABASE_UNAVAILABLE):
    return SchemaObservation((), None, _UNKNOWNS + (reason,))


def _path_snapshot(path):
    """Sample names only; does not bind SQLite handles or close TOCTOU gaps."""
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('absolute normalized path required')

    def identity(item, required, directory=False):
        try:
            info = os.lstat(item)
        except FileNotFoundError:
            if required:
                raise
            return None
        if stat.S_ISLNK(info.st_mode):
            raise ValueError('link not supported')
        if directory:
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError('directory required')
        elif not stat.S_ISREG(info.st_mode):
            raise ValueError('regular file required')
        return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink)

    parents = tuple(identity(item, True, directory=True) for item in reversed(path.parents))
    return (parents, identity(path, True),
            identity(Path(str(path) + '-wal'), False),
            identity(Path(str(path) + '-shm'), False))


def _observe_in_process(path: Path, limits: ObservationLimits) -> SchemaObservation:
    """Helper-only synchronous observation; it never accepts a connection."""
    budget = _Budget(limits)
    conn = None
    before = None
    try:
        path = Path(path)
        before = _path_snapshot(path)
        uri = 'file:' + quote(str(path), safe='/') + '?mode=ro'
        conn = sqlite3.connect(uri, uri=True, timeout=1.0, isolation_level=None)
        conn.execute('BEGIN')
        objects = _read_schema(conn, budget, 'main')
        temp_objects = _read_schema(conn, budget, 'temp')
        recursive = _read_recursive(conn, budget)
        result = SchemaObservation(objects, recursive, _UNKNOWNS, temp_objects)
    except _Exhausted:
        result = _failed(ObservationReason.BUDGET_EXHAUSTED)
    except _InvalidMetadata:
        result = _failed(ObservationReason.METADATA_INVALID)
    except (sqlite3.Error, OSError, ValueError, UnicodeError):
        result = _failed()
    finally:
        if conn is not None:
            try:
                conn.rollback()
            except (sqlite3.Error, OSError):
                result = _failed()
            finally:
                try:
                    conn.close()
                except (sqlite3.Error, OSError):
                    result = _failed()
    if before is not None:
        try:
            after = _path_snapshot(path)
            if after[:2] != before[:2]:
                result = _failed()
            elif after[2:] != before[2:]:
                result = SchemaObservation(result.objects, result.recursive_triggers,
                                           result.reasons + (ObservationReason.SIDECAR_CHANGED,),
                                           result.temp_objects)
        except (OSError, ValueError, UnicodeError):
            result = _failed()
    return result


def _run_schema_observation(path: Path, limits: ObservationLimits) -> SchemaObservation:
    """Run one private snapshot in an independently supervised child.

    Path checks are observations, not a handle binding. The deadline bounds
    normal child operation and cleanup; a kernel task stuck in uninterruptible
    I/O may not exit promptly, so cleanup can remain unconfirmed.
    """
    if not isinstance(path, (str, Path)):
        raise TypeError('database path required')
    if not isinstance(limits, ObservationLimits):
        raise TypeError('observation limits required')
    try:
        payload = json.dumps({'path': str(path), 'limits': asdict(limits)}).encode('utf-8')
        if len(payload) > _MAX_INPUT_BYTES:
            return _failed()
        child = subprocess.Popen(
            [sys.executable, '-m', 'orchestrator.persistence._continuity_observation', '--helper'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
    except (OSError, ValueError, UnicodeError):
        return _failed()
    try:
        output, _ = child.communicate(payload, timeout=_HELPER_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            child.kill()
        except ProcessLookupError:
            pass
        except OSError:
            return _failed(ObservationReason.HELPER_CLEANUP_UNCONFIRMED)
        try:
            child.communicate(timeout=_CLEANUP_TIMEOUT_SECONDS)
        except (subprocess.TimeoutExpired, OSError):
            return _failed(ObservationReason.HELPER_CLEANUP_UNCONFIRMED)
        return _failed()
    except OSError:
        try:
            child.kill()
            child.communicate(timeout=_CLEANUP_TIMEOUT_SECONDS)
        except (OSError, subprocess.TimeoutExpired):
            return _failed(ObservationReason.HELPER_CLEANUP_UNCONFIRMED)
        return _failed()
    except BaseException:
        # Preserve interruption while attempting to reap only the child we own.
        try:
            child.kill()
        except OSError:
            pass
        try:
            child.communicate(timeout=_CLEANUP_TIMEOUT_SECONDS)
        except (OSError, subprocess.TimeoutExpired):
            pass
        raise
    if child.returncode != 0 or len(output) > _MAX_RESPONSE_BYTES:
        return _failed()
    try:
        data = json.loads(output)
        if set(data) != {'objects', 'recursive_triggers', 'reasons', 'temp_objects'}:
            raise ValueError
        objects = tuple(SchemaObject(**item) for item in data['objects'])
        temp_objects = tuple(SchemaObject(**item) for item in data['temp_objects'])
        reasons = tuple(ObservationReason(reason) for reason in data['reasons'])
        valid_extras = {ObservationReason.BUDGET_EXHAUSTED,
                        ObservationReason.DATABASE_UNAVAILABLE,
                        ObservationReason.METADATA_INVALID,
                        ObservationReason.SIDECAR_CHANGED}
        if (reasons[:2] != _UNKNOWNS or len(reasons) > 4
                or len(set(reasons)) != len(reasons)
                or not set(reasons[2:]) <= valid_extras
                or len(set(reasons[2:]) - {ObservationReason.SIDECAR_CHANGED}) > 1):
            raise ValueError
        return SchemaObservation(objects, data['recursive_triggers'], reasons, temp_objects)
    except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
        return _failed()


def _helper_main():
    try:
        raw = sys.stdin.buffer.read(_MAX_INPUT_BYTES + 1)
        if len(raw) > _MAX_INPUT_BYTES:
            raise ValueError
        data = json.loads(raw)
        if set(data) != {'path', 'limits'} or not isinstance(data['path'], str):
            raise ValueError
        result = _observe_in_process(Path(data['path']), ObservationLimits(**data['limits']))
        output = json.dumps(asdict(result), separators=(',', ':')).encode('utf-8')
        if len(output) > _MAX_RESPONSE_BYTES:
            raise ValueError
    except (ValueError, TypeError, KeyError, OSError, UnicodeError):
        output = json.dumps(asdict(_failed()), separators=(',', ':')).encode('utf-8')
    sys.stdout.buffer.write(output)


if __name__ == '__main__':
    if sys.argv[1:] != ['--helper']:
        raise SystemExit(2)
    _helper_main()
