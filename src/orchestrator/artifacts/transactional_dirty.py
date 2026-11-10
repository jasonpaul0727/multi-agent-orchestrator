"""Opt-in transactional mutation observations; never authorization evidence.

The marker is deliberately separate from checkpoint rows. SQL writers do not
need a registered function or connection-local pragma. This experimental
schema does not authenticate marker resets, schema continuity or storage.
"""


import uuid

from .incremental_checkpoint import _structure


def _require_owned(store):
    if not store._has_owned_write():
        raise RuntimeError('dirty tracking requires an owned write transaction')


def install_dirty_tracking(store):
    """Install atomically inside an owned transaction, without repairing epochs."""
    _require_owned(store)
    conn = store._connection
    # Validate before any DDL. The incremental structure check includes both
    # experimental tables and the existing event immutability triggers.
    if not _structure(conn):
        raise RuntimeError('incompatible incremental checkpoint schema')
    required = {
        'events': {'event_id', 'stream_type', 'stream_id', 'stream_version'},
        'stream_versions': {'stream_type', 'stream_id', 'current_version'},
    }
    for table, columns in required.items():
        actual = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})')}
        if not columns <= actual:
            raise RuntimeError('incompatible event store schema')
    _validate_unique_indexes(conn)
    # A random identifier avoids shadowing a caller's savepoint of the same
    # name. SQLite DDL participates in this savepoint and the outer transaction.
    name = 'artifact_dirty_install_' + uuid.uuid4().hex
    conn.execute(f'SAVEPOINT {name}')
    try:
        _install(conn)
    except BaseException:
        conn.execute(f'ROLLBACK TO SAVEPOINT {name}')
        conn.execute(f'RELEASE SAVEPOINT {name}')
        raise
    conn.execute(f'RELEASE SAVEPOINT {name}')


def _validate_unique_indexes(conn):
    # Every REPLACE conflict source must be known. Non-unique indexes cannot
    # remove a row and are intentionally allowed. Require exact multiplicity,
    # origin, ordered key columns, direction, collation and no partial index.
    expected = {
        'events': [('pk', ('event_id',)), ('u', ('stream_type', 'stream_id', 'stream_version'))],
        'stream_versions': [('pk', ('stream_type', 'stream_id'))],
        'artifact_incremental_checkpoint': [('pk', ('digest',))],
        'artifact_incremental_publication': [('pk', ('digest', 'publication_id')), ('u', ('digest', 'event_id'))],
    }
    for table, allowed in expected.items():
        found = []
        for index in conn.execute(f'PRAGMA index_list("{table}")').fetchall():
            if not index['unique']:
                continue
            if index['partial']:
                raise RuntimeError('unreviewed unique index')
            quoted = index['name'].replace('"', '""')
            keys = [row for row in conn.execute(f'PRAGMA index_xinfo("{quoted}")') if row['key']]
            if any(row['cid'] < 0 or row['name'] is None or row['desc'] != 0 or row['coll'] != 'BINARY' for row in keys):
                raise RuntimeError('unreviewed unique index')
            found.append((index['origin'], tuple(row['name'] for row in keys)))
        if sorted(found) != sorted(allowed):
            raise RuntimeError('unreviewed unique index')


def _install(conn):
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'artifact_dirty_%' LIMIT 1").fetchone():
        raise RuntimeError('dirty tracking already exists or is incomplete')
    conn.execute('CREATE TABLE artifact_dirty_schema (version INTEGER NOT NULL CHECK(version=1))')
    conn.execute('INSERT INTO artifact_dirty_schema VALUES (1)')
    conn.execute('CREATE TABLE artifact_dirty_marker (digest TEXT PRIMARY KEY, dirty INTEGER NOT NULL CHECK(dirty IN (0,1)))')
    for table in ('events', 'stream_versions', 'artifact_incremental_checkpoint', 'artifact_incremental_publication'):
        for operation in ('INSERT', 'UPDATE', 'DELETE'):
            statements = []
            for side in (('NEW',) if operation == 'INSERT' else ('OLD',) if operation == 'DELETE' else ('OLD', 'NEW')):
                scope = f"{side}.stream_type='artifact' AND " if table in ('events', 'stream_versions') else ''
                digest = f'{side}.stream_id' if table in ('events', 'stream_versions') else f'{side}.digest'
                statements.append(f'UPDATE artifact_dirty_marker SET dirty=1 WHERE {scope}digest={digest} AND dirty=0;')
            if table == 'events' and operation in ('INSERT', 'UPDATE'):
                # REPLACE can delete conflicting rows without firing DELETE
                # triggers. Both indexes bound these probes; either can name an
                # artifact digest different from NEW.stream_id.
                statements.append("UPDATE artifact_dirty_marker SET dirty=1 WHERE dirty=0 AND digest IN (SELECT stream_id FROM events WHERE stream_type='artifact' AND event_id=NEW.event_id);")
                statements.append("UPDATE artifact_dirty_marker SET dirty=1 WHERE dirty=0 AND digest IN (SELECT stream_id FROM events WHERE stream_type='artifact' AND stream_type=NEW.stream_type AND stream_id=NEW.stream_id AND stream_version=NEW.stream_version);")
            name = f'artifact_dirty_{table}_{operation.lower()}'
            conn.execute(f'CREATE TRIGGER {name} BEFORE {operation} ON {table} BEGIN ' + ' '.join(statements) + ' END')


def register_digest(store, digest):
    """Register a diagnostic marker, not a validated checkpoint or grant."""
    _require_owned(store)
    conn = store._connection
    if not conn.execute('SELECT 1 FROM artifact_incremental_checkpoint WHERE digest=?', (digest,)).fetchone():
        raise ValueError('incremental checkpoint is absent')
    conn.execute('INSERT INTO artifact_dirty_marker VALUES (?, 0)', (digest,))
