"""Opt-in transactional mutation observations; never authorization evidence.

The marker is deliberately separate from checkpoint rows. SQL writers do not
need a registered function or connection-local pragma. This experimental
schema does not authenticate marker resets, schema continuity or storage.
"""


import uuid

from .incremental_checkpoint import _structure


_DIRTY_TABLES = {
    'artifact_dirty_schema': 'CREATE TABLE artifact_dirty_schema (version INTEGER NOT NULL CHECK(version=1))',
    'artifact_dirty_marker': 'CREATE TABLE artifact_dirty_marker (digest TEXT PRIMARY KEY, dirty INTEGER NOT NULL CHECK(dirty IN (0,1)))',
}


_BASE_TRIGGERS = {'gw_scope_check': "CREATE TRIGGER gw_scope_check BEFORE INSERT ON events\n        WHEN NEW.stream_type='general_worker' BEGIN\n        SELECT CASE WHEN typeof(NEW.run_id) != 'text' OR length(trim(NEW.run_id, char(9)||char(10)||char(13)||' '))=0\n            OR typeof(NEW.node_id) != 'text' OR length(trim(NEW.node_id, char(9)||char(10)||char(13)||' '))=0\n            OR typeof(NEW.attempt_id) != 'text' OR length(trim(NEW.attempt_id, char(9)||char(10)||char(13)||' '))=0\n            OR typeof(NEW.fencing_generation) != 'integer' OR NEW.fencing_generation <= 0\n            THEN RAISE(ABORT, 'general worker scope requires identity') END;\n        SELECT CASE WHEN NEW.stream_version > 1 AND NOT EXISTS (\n            SELECT 1 FROM general_worker_scope s WHERE s.stream_id=NEW.stream_id\n            AND s.run_id=NEW.run_id AND s.node_id=NEW.node_id AND s.attempt_id=NEW.attempt_id\n            AND s.fencing_generation=NEW.fencing_generation)\n            THEN RAISE(ABORT, 'general worker scope mismatch') END;\n        END", 'gw_scope_insert': "CREATE TRIGGER gw_scope_insert AFTER INSERT ON events\n        WHEN NEW.stream_type='general_worker' AND NEW.stream_version=1 BEGIN\n        INSERT INTO general_worker_scope VALUES (NEW.stream_id, NEW.run_id, NEW.node_id,\n            NEW.attempt_id, NEW.fencing_generation); END", 'mediation_scope_check': "CREATE TRIGGER mediation_scope_check BEFORE INSERT ON events\n        WHEN NEW.stream_type='mediation_v2' BEGIN\n        SELECT CASE WHEN typeof(NEW.run_id) != 'text' OR length(trim(NEW.run_id))=0\n            OR typeof(NEW.node_id) != 'text' OR length(trim(NEW.node_id))=0\n            OR typeof(NEW.attempt_id) != 'text' OR length(trim(NEW.attempt_id))=0\n            OR typeof(NEW.fencing_generation) != 'integer' OR NEW.fencing_generation <= 0\n            THEN RAISE(ABORT, 'mediation scope requires identity') END;\n        SELECT CASE WHEN NEW.stream_version > 1 AND NOT EXISTS (\n            SELECT 1 FROM mediation_scope s WHERE s.stream_id=NEW.stream_id\n            AND s.run_id=NEW.run_id AND s.node_id=NEW.node_id AND s.attempt_id=NEW.attempt_id\n            AND s.fencing_generation=NEW.fencing_generation)\n            THEN RAISE(ABORT, 'mediation scope mismatch') END;\n        END", 'mediation_scope_insert': "CREATE TRIGGER mediation_scope_insert AFTER INSERT ON events\n        WHEN NEW.stream_type='mediation_v2' AND NEW.stream_version=1 BEGIN\n        INSERT INTO mediation_scope VALUES (NEW.stream_id, NEW.run_id, NEW.node_id,\n            NEW.attempt_id, NEW.fencing_generation); END"}

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
    _preflight_table_ddls(conn)
    required = {
        'events': {'event_id', 'stream_type', 'stream_id', 'stream_version'},
        'stream_versions': {'stream_type', 'stream_id', 'current_version'},
    }
    for table, columns in required.items():
        actual = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})')}
        if not columns <= actual:
            raise RuntimeError('incompatible event store schema')
    _validate_unique_indexes(conn)
    _validate_triggers(conn, installed=False)
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


def _preflight_table_ddls(conn, budget=None):
    """Bound DDL before PRAGMA exposes column or autoindex names."""
    from .evidence_checkpoint import _charge
    tables = ('events', 'stream_versions', 'artifact_incremental_checkpoint', 'artifact_incremental_publication')
    rows = conn.execute("SELECT name, typeof(sql) AS st, length(CAST(sql AS BLOB)) AS n FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?) LIMIT 5", tables).fetchall()
    if len(rows) != len(tables) or any(row['st'] != 'text' or type(row['n']) is not int or row['n'] > 8192 or (budget is not None and row['n'] > budget.max_row_bytes) for row in rows):
        raise RuntimeError('oversized or missing table DDL')
    _charge(budget, len(rows), sum(row['n'] + len(row['name'].encode()) + 64 for row in rows))
    return {row['name']: row['n'] for row in rows}


def _validate_unique_indexes(conn, budget=None):
    # Every REPLACE conflict source must be known. Non-unique indexes cannot
    # remove a row and are intentionally allowed. Require exact multiplicity,
    # origin, ordered key columns, direction, collation and no partial index.
    expected = {
        'events': [('pk', ('event_id',)), ('u', ('stream_type', 'stream_id', 'stream_version'))],
        'stream_versions': [('pk', ('stream_type', 'stream_id'))],
        'artifact_incremental_checkpoint': [('pk', ('digest',))],
        'artifact_incremental_publication': [('pk', ('digest', 'publication_id')), ('u', ('digest', 'event_id'))],
    }
    from .evidence_checkpoint import _charge
    table_sizes = _preflight_table_ddls(conn, budget)
    for table, allowed in expected.items():
        # Read lengths and types in SQLite before PRAGMA can return any name.
        meta = conn.execute("SELECT typeof(name) AS nt, length(CAST(name AS BLOB)) AS n, typeof(sql) AS st, length(CAST(sql AS BLOB)) AS sn FROM sqlite_master WHERE type='index' AND tbl_name=? LIMIT 129", (table,)).fetchall()
        if len(meta) > 128 or any(r['nt'] != 'text' or type(r['n']) is not int or r['n'] > 4096 or (budget is not None and r['n'] > budget.max_row_bytes) or r['st'] not in ('text', 'null') or (r['st'] == 'text' and (type(r['sn']) is not int or r['sn'] > 8192 or (budget is not None and r['sn'] > budget.max_row_bytes))) for r in meta):
            raise RuntimeError('oversized index metadata')
        _charge(budget, 2 * len(meta), sum(2 * r['n'] + (r['sn'] or 0) + 128 for r in meta))
        found = []
        for number, index in enumerate(conn.execute(f'PRAGMA index_list("{table}")')):
            if number >= 128:
                raise RuntimeError('too many indexes')
            if not index['unique']:
                continue
            if len(found) >= len(allowed):
                raise RuntimeError('unreviewed unique index')
            if index['partial']:
                raise RuntimeError('unreviewed unique index')
            # Reserve the largest permitted result before sqlite returns the
            # first index_xinfo row. SQL text bounds column/collation names.
            max_index_sql = max((r['sn'] or 0 for r in meta), default=0)
            _charge(budget, 32, 32 * (table_sizes[table] + max_index_sql + 512))
            quoted = index['name'].replace('"', '""')
            keys = []
            for number, row in enumerate(conn.execute(f'PRAGMA index_xinfo("{quoted}")')):
                if number >= 32:
                    raise RuntimeError('too many index columns')
                if row['key']:
                    keys.append(row)
            if any(row['cid'] < 0 or row['name'] is None or row['desc'] != 0 or row['coll'] != 'BINARY' for row in keys):
                raise RuntimeError('unreviewed unique index')
            found.append((index['origin'], tuple(row['name'] for row in keys)))
        if sorted(found) != sorted(allowed):
            raise RuntimeError('unreviewed unique index')


def _validate_triggers(conn, installed, budget=None):
    from .evidence_checkpoint import _charge
    from .incremental_checkpoint import _TRIGGERS
    allowed = {**_TRIGGERS, **_BASE_TRIGGERS, **{'gw_scope_immutable_update': "CREATE TRIGGER gw_scope_immutable_update BEFORE UPDATE ON general_worker_scope\n        BEGIN SELECT RAISE(ABORT, 'general worker scope is immutable'); END", 'gw_scope_immutable_delete': "CREATE TRIGGER gw_scope_immutable_delete BEFORE DELETE ON general_worker_scope\n        BEGIN SELECT RAISE(ABORT, 'general worker scope is immutable'); END", 'mediation_scope_immutable_update': "CREATE TRIGGER mediation_scope_immutable_update BEFORE UPDATE ON mediation_scope\n        BEGIN SELECT RAISE(ABORT, 'mediation scope is immutable'); END", 'mediation_scope_immutable_delete': "CREATE TRIGGER mediation_scope_immutable_delete BEFORE DELETE ON mediation_scope\n        BEGIN SELECT RAISE(ABORT, 'mediation scope is immutable'); END"}}
    if installed:
        allowed.update(_trigger_sql())
    # Any trigger may run indirectly during a controlled append.
    tables = ()
    where = "type='trigger'"
    seen = set()
    for schema in ('sqlite_master', 'sqlite_temp_master'):
        meta = conn.execute("SELECT typeof(name) AS nt, length(CAST(name AS BLOB)) AS n, typeof(sql) AS st, length(CAST(sql AS BLOB)) AS sn FROM " + schema + " WHERE " + where + " LIMIT 33", tables).fetchall()
        if len(meta) > 32 or any(r['nt'] != 'text' or r['st'] != 'text' or type(r['n']) is not int or type(r['sn']) is not int or r['n'] > 4096 or r['sn'] > 8192 or (budget is not None and max(r['n'], r['sn']) > budget.max_row_bytes) for r in meta):
            raise RuntimeError('oversized trigger metadata')
        _charge(budget, len(meta) * 2, sum(r['n'] + r['sn'] + 128 for r in meta))
        rows = conn.execute("SELECT name, sql FROM " + schema + " WHERE " + where + " LIMIT 33", tables).fetchall()
        for row in rows:
            if schema == 'sqlite_temp_master' or row['name'] not in allowed or ' '.join(row['sql'].split()) != ' '.join(allowed[row['name']].split()):
                raise RuntimeError('unreviewed trigger')
            seen.add(row['name'])
    if seen != set(allowed):
        raise RuntimeError('missing trigger')



def _install(conn):
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'artifact_dirty_%' LIMIT 1").fetchone():
        raise RuntimeError('dirty tracking already exists or is incomplete')
    conn.execute(_DIRTY_TABLES['artifact_dirty_schema'])
    conn.execute('INSERT INTO artifact_dirty_schema VALUES (1)')
    conn.execute(_DIRTY_TABLES['artifact_dirty_marker'])
    for sql in _trigger_sql().values():
        conn.execute(sql)


def _trigger_sql():
    triggers = {}
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
            triggers[name] = f'CREATE TRIGGER {name} BEFORE {operation} ON {table} BEGIN ' + ' '.join(statements) + ' END'
    return triggers


def register_digest(store, digest):
    """Register DIRTY; only a fresh full rebuild may establish diagnostic clean."""
    _require_owned(store)
    conn = store._connection
    if not conn.execute('SELECT 1 FROM artifact_incremental_checkpoint WHERE digest=?', (digest,)).fetchone():
        raise ValueError('incremental checkpoint is absent')
    conn.execute('INSERT INTO artifact_dirty_marker VALUES (?, 1)', (digest,))


def tracking_present(conn):
    """Any surviving experimental object means opt-in, even if incomplete."""
    return conn.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'artifact_dirty_%' LIMIT 1").fetchone() is not None


def require_clean_marker(conn, digest, budget, *, allow_dirty=False, require_tracking=False):
    """Check diagnostic state only; no schema-continuity or authority claim."""
    import sqlite3
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    from .evidence_checkpoint import _charge

    if not tracking_present(conn):
        if require_tracking:
            raise CancellationHeld('integrity')
        return
    try:
        expected = {**_DIRTY_TABLES, **_trigger_sql()}
        # Bounded schema materialization before fetching the DDL itself.
        _charge(budget, 1024, 1048576)
        meta = conn.execute("SELECT typeof(name) AS nt, length(CAST(name AS BLOB)) AS nn, typeof(sql) AS st, length(CAST(sql AS BLOB)) AS n FROM sqlite_master WHERE name LIKE 'artifact_dirty_%' LIMIT 15").fetchall()
        if len(meta) != len(expected) or any(r['nt'] != 'text' or r['st'] != 'text' or type(r['nn']) is not int or r['nn'] > 4096 or r['nn'] > budget.max_row_bytes or type(r['n']) is not int or r['n'] > 4096 or r['n'] > budget.max_row_bytes for r in meta):
            raise CancellationHeld('integrity')
        rows = conn.execute("SELECT name, sql FROM sqlite_master WHERE name LIKE 'artifact_dirty_%' LIMIT 15").fetchall()
        if {r['name']: ' '.join(r['sql'].split()) for r in rows} != {k: ' '.join(v.split()) for k, v in expected.items()}:
            raise CancellationHeld('integrity')
        _validate_unique_indexes(conn, budget)
        _validate_triggers(conn, installed=True, budget=budget)
        versions = conn.execute("SELECT typeof(version), CASE WHEN typeof(version)='integer' THEN version END FROM artifact_dirty_schema LIMIT 2").fetchall()
        if len(versions) != 1 or tuple(versions[0]) != ('integer', 1):
            raise CancellationHeld('integrity')
        marker = conn.execute("SELECT typeof(dirty), CASE WHEN typeof(dirty)='integer' THEN dirty END FROM artifact_dirty_marker WHERE digest=? LIMIT 2", (digest,)).fetchall()
        allowed_markers = (('integer', 0), ('integer', 1)) if allow_dirty else (('integer', 0),)
        if len(marker) != 1 or tuple(marker[0]) not in allowed_markers:
            raise CancellationHeld('integrity')
    except (sqlite3.DatabaseError, RuntimeError, TypeError, ValueError):
        raise CancellationHeld('integrity') from None
