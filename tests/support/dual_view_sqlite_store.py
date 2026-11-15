"""Owned local SQLite WAL prototype for finite synthetic test data only.

This database has no production adapter, authority, or provider side effects.
Its trusted parent directory and SQLite/VFS semantics are test assumptions.
"""

import os
from pathlib import Path
import sqlite3
import stat
from types import MappingProxyType

from tests.support import dual_view_simulation as sim
from tests.support.dual_view_sqlite_codec import decode, encode

_EVENT_BYTES = 16 * 1024
_TOTAL_EVENT_BYTES = 1024 * 1024
_PROJECTION_BYTES = 2 * 1024 * 1024
_BUSY_TIMEOUT_MS = 250
_APPLICATION_ID = 0x44565331
_MARKER = "synthetic-dual-view-sqlite-v1"
_TABLES = {
    "prototype_meta": "CREATE TABLE prototype_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, marker TEXT NOT NULL)",
    "events": "CREATE TABLE events(sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
    "projection": "CREATE TABLE projection(singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload TEXT NOT NULL)",
    "winners": "CREATE TABLE winners(source TEXT PRIMARY KEY, payload TEXT NOT NULL)",
    "resources": "CREATE TABLE resources(source TEXT PRIMARY KEY, payload TEXT NOT NULL)",
}
_EXPECTED_SCHEMA = {
    ("table", name, name, sql) for name, sql in _TABLES.items()
} | {
    ("index", "sqlite_autoindex_winners_1", "winners", None),
    ("index", "sqlite_autoindex_resources_1", "resources", None),
}


class UnsupportedStoreError(RuntimeError):
    """The local runtime cannot provide the required WAL configuration."""


def _connect(path):
    return sqlite3.connect(path.absolute().as_uri() + "?mode=rw", uri=True,
                           timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None)


def _rollback_preserving_error(connection):
    try:
        if connection.in_transaction:
            connection.rollback()
    except BaseException:
        # The active validation/control-flow error remains the primary error.
        pass


def _close_preserving_error(connection):
    try:
        connection.close()
    except BaseException:
        pass


def _configure(connection, *, creating=False):
    mode = connection.execute("PRAGMA journal_mode" + ("=WAL" if creating else "")).fetchone()[0]
    if mode.lower() != "wal":
        raise UnsupportedStoreError("private prototype requires SQLite WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    return MappingProxyType({
        "sqlite_version": sqlite3.sqlite_version,
        "compile_options": tuple(row[0] for row in connection.execute("PRAGMA compile_options")),
        "journal_mode": mode.lower(),
        "synchronous": connection.execute("PRAGMA synchronous").fetchone()[0],
        "wal_autocheckpoint": connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0],
        "busy_timeout": connection.execute("PRAGMA busy_timeout").fetchone()[0],
    })


def _verify_schema(connection):
    actual = set(connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema"))
    if actual != _EXPECTED_SCHEMA:
        raise ValueError("unknown private prototype schema")
    if connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID:
        raise ValueError("unknown private prototype application")
    valid_meta = connection.execute(
        "SELECT COUNT(*) FROM prototype_meta WHERE singleton=1 "
        "AND typeof(version)='integer' AND version=1 "
        "AND typeof(marker)='text' AND marker=?", (_MARKER,),
    ).fetchone()[0]
    if valid_meta != 1 or connection.execute("SELECT COUNT(*) FROM prototype_meta").fetchone()[0] != 1:
        raise ValueError("unknown private prototype marker or version")


def create_store(path: Path) -> None:
    """Exclusively create an absent owned file; never overwrite or repair one."""
    path = Path(path)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    connection = _connect(path)
    try:
        _configure(connection, creating=True)
        connection.execute("BEGIN IMMEDIATE")
        for sql in _TABLES.values():
            connection.execute(sql)
        connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
        connection.execute("INSERT INTO prototype_meta VALUES(1,1,?)", (_MARKER,))
        connection.execute("INSERT INTO projection VALUES(1,?)", (encode(sim.initial_state()),))
        connection.commit()
    except BaseException:
        _rollback_preserving_error(connection)
        _close_preserving_error(connection)
        raise
    connection.close()


def _check_payload_bounds(connection, table, max_rows, each_bytes, total_bytes):
    # Table identifiers are fixed local constants, never caller input.
    count, total, largest, nontext = connection.execute(
        f"SELECT COUNT(*),COALESCE(SUM(length(CAST(payload AS BLOB))),0),"
        f"COALESCE(MAX(length(CAST(payload AS BLOB))),0),"
        f"COALESCE(SUM(typeof(payload)!='text'),0) FROM {table}"
    ).fetchone()
    if count > max_rows or total > total_bytes or largest > each_bytes or nontext:
        raise ValueError("private prototype payload bounds or type violated")
    return count


def _read_state(connection):
    _verify_schema(connection)
    event_count = _check_payload_bounds(connection, "events", 64, _EVENT_BYTES, _TOTAL_EVENT_BYTES)
    projection_count = _check_payload_bounds(connection, "projection", 1, _PROJECTION_BYTES, _PROJECTION_BYTES)
    _check_payload_bounds(connection, "winners", 2, _EVENT_BYTES, 2 * _EVENT_BYTES)
    _check_payload_bounds(connection, "resources", 2, _EVENT_BYTES, 2 * _EVENT_BYTES)
    if projection_count != 1:
        raise ValueError("missing private prototype projection")
    events = []
    for expected, (sequence, payload) in enumerate(
            connection.execute("SELECT sequence,payload FROM events ORDER BY sequence"), 1):
        if type(sequence) is not int or sequence != expected:
            raise ValueError("gapped private prototype event sequence")
        event = decode(payload)
        if type(event) is not sim.Event or event.ordinal != sequence:
            raise ValueError("invalid private prototype event")
        events.append(event)
    events = tuple(events)
    rebuilt, observation = sim.step(sim.initial_state(), sim.Replay(events, events, event_count))
    if observation.outcome is not sim.Outcome.REPLAYED or rebuilt.pending:
        raise ValueError("invalid private prototype replay")
    singleton, payload = connection.execute("SELECT singleton,payload FROM projection").fetchone()
    projection = decode(payload)
    if singleton != 1 or type(projection) is not sim.ModelState or projection.pending or projection != rebuilt:
        raise ValueError("inconsistent private prototype projection")
    expected_winners = {item.envelope.source.name: encode(item) for item in rebuilt.winners}
    expected_resources = {item.source.name: encode(item) for item in rebuilt.sources}
    for table, expected_rows, kind in (("winners", expected_winners, sim.Winner),
                                       ("resources", expected_resources, sim.SourceState)):
        actual_rows = {}
        for source, row_payload in connection.execute(f"SELECT source,payload FROM {table}"):
            if type(source) is not str or source not in ("S0", "S1") or source in actual_rows:
                raise ValueError("invalid private prototype source identity")
            item = decode(row_payload)
            if type(item) is not kind:
                raise ValueError("invalid private prototype materialized value")
            actual_rows[source] = row_payload
        if actual_rows != expected_rows:
            raise ValueError("inconsistent private prototype materialized rows")
    return rebuilt


class SqliteDualViewStore:
    """A private connection to one existing synthetic store; no ambient connection."""

    def __init__(self, path: Path):
        path = Path(path)
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
            raise ValueError("private prototype requires an existing regular file")
        connection = _connect(path)
        try:
            # No mutating PRAGMA is issued until this exact owned schema is known.
            connection.execute("BEGIN")
            _read_state(connection)
            connection.commit()
            self._diagnostics = _configure(connection)
        except BaseException:
            _rollback_preserving_error(connection)
            _close_preserving_error(connection)
            raise
        self._connection = connection

    @property
    def diagnostics(self):
        return self._diagnostics

    def __enter__(self):
        if self._connection is None:
            raise ValueError("private prototype store is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        connection, self._connection = self._connection, None
        if connection is not None:
            if exc_type is None:
                connection.close()
            else:
                _close_preserving_error(connection)
        return False

    def read(self) -> sim.ModelState:
        """Replay and cross-check all persistent values within one read snapshot."""
        connection = self._connection
        if connection is None:
            raise ValueError("private prototype store is closed")
        connection.execute("BEGIN")
        try:
            state = _read_state(connection)
            connection.commit()
            return state
        except BaseException:
            _rollback_preserving_error(connection)
            raise
