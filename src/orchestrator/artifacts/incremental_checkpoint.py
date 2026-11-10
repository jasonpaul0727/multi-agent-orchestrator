"""Non-authoritative incremental checkpoint experiment.

These observations assume installation-controlled writers and immutable SQLite
history. They cannot detect arbitrary in-place mutation of older rows or index
entries and must never be used for cancellation, access, permit or release.
"""
import hashlib
import sqlite3

from orchestrator.artifacts.store import ArtifactStore, ArtifactError
from orchestrator.lifecycle.cancel_completion import CancellationHeld, _COLUMNS, _event_storage_types
from .evidence_checkpoint import _charge, _event_window

_TABLES = {
    'artifact_incremental_checkpoint': 'CREATE TABLE artifact_incremental_checkpoint (digest TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, version INTEGER NOT NULL, boundary_id TEXT, chain TEXT NOT NULL)',
    'artifact_incremental_publication': 'CREATE TABLE artifact_incremental_publication (digest TEXT NOT NULL, publication_id TEXT NOT NULL, event_id TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(digest, publication_id), UNIQUE(digest, event_id))',
}


_TRIGGERS = {
    'events_immutable_update': "CREATE TRIGGER events_immutable_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'event rows are immutable'); END",
    'events_immutable_delete': "CREATE TRIGGER events_immutable_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'event rows are immutable'); END",
}


def _structure(conn, budget=None):
    where = "(type='table' AND name IN ('artifact_incremental_checkpoint','artifact_incremental_publication')) OR (type='trigger' AND name IN ('events_immutable_update','events_immutable_delete'))"
    _charge(budget, 4, 1024)
    metadata = conn.execute('SELECT name, length(CAST(sql AS BLOB)) AS n FROM sqlite_master WHERE ' + where + ' LIMIT 5').fetchall()
    if len(metadata) != 4 or any(type(row['n']) is not int or row['n'] > 4096 or (budget is not None and row['n'] > budget.max_row_bytes) for row in metadata):
        return False
    _charge(budget, 4, sum(row['n'] + len(row['name'].encode()) + 64 for row in metadata))
    rows = conn.execute('SELECT name, sql FROM sqlite_master WHERE ' + where + ' LIMIT 5').fetchall()
    expected = {**_TABLES, **_TRIGGERS}
    return len(rows) == 4 and {row['name']: ' '.join(row['sql'].split()) for row in rows} == {key: ' '.join(value.split()) for key, value in expected.items()}

def _step(chain, event):
    value = (str(event.stream_version) + '\0' + event.event_id + '\0' + event.payload_hash + '\0' + event.event_type).encode()
    return hashlib.sha256(bytes.fromhex(chain) + value).hexdigest()


def _validate(event, digest):
    if event.stream_type != 'artifact' or event.stream_id != digest or event.schema_version != 1 or event.event_type not in ('ArtifactPublicationIntent', 'ArtifactPublished'):
        raise CancellationHeld('integrity')
    try:
        return ArtifactStore._record_from_event(event, digest)
    except (ArtifactError, ValueError, TypeError, KeyError):
        raise CancellationHeld('integrity') from None


def initialize_incremental(store, digest, budget):
    budget.bind(store)
    with store._cancellation_proof_write():
        events, version = _event_window(store, digest, budget)
        chain = hashlib.sha256(b'artifact-incremental-v1').hexdigest()
        pubs = []
        ids = set()
        for event in events:
            record = _validate(event, digest)
            chain = _step(chain, event)
            if event.event_type == 'ArtifactPublished':
                if record.publication_id in ids:
                    raise CancellationHeld('integrity')
                ids.add(record.publication_id)
                pubs.append((digest, record.publication_id, event.event_id, event.stream_version))
        _charge(budget, len(pubs) + 4, sum(len(x[0].encode('utf-8')) + len(x[1].encode('utf-8')) + len(x[2].encode('utf-8')) + 192 for x in pubs) + 1024)
        conn = store._connection
        existing = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('artifact_incremental_checkpoint','artifact_incremental_publication') LIMIT 3").fetchall()
        if not existing:
            for sql in _TABLES.values():
                conn.execute(sql)
        if not _structure(conn, budget):
            raise CancellationHeld('integrity')
        # A bounded initial scan only: never silently rebuild prior state.
        if conn.execute('SELECT 1 FROM artifact_incremental_checkpoint WHERE digest=?', (digest,)).fetchone():
            raise CancellationHeld('integrity')
        conn.executemany('INSERT INTO artifact_incremental_publication VALUES (?, ?, ?, ?)', pubs)
        conn.execute('INSERT INTO artifact_incremental_checkpoint VALUES (?, 1, ?, ?, ?)', (digest, version, events[-1].event_id if events else None, chain))


def _head(conn, digest, expected_version):
    meta = conn.execute("""SELECT typeof(schema_version) AS st, typeof(version) AS vt,
        typeof(boundary_id) AS bt, length(CAST(boundary_id AS BLOB)) AS bn,
        typeof(chain) AS ct, length(CAST(chain AS BLOB)) AS cn
        FROM artifact_incremental_checkpoint WHERE digest=?""", (digest,)).fetchone()
    if meta is None:
        return None
    if (meta['st'] != 'integer' or meta['vt'] != 'integer' or meta['ct'] != 'text'
            or meta['cn'] != 64 or meta['bt'] not in ('null', 'text')
            or (meta['bt'] == 'text' and (type(meta['bn']) is not int or meta['bn'] > 128))):
        raise CancellationHeld('integrity')
    row = conn.execute('SELECT schema_version, version, boundary_id, chain FROM artifact_incremental_checkpoint WHERE digest=?', (digest,)).fetchone()
    tail = conn.execute("""SELECT CASE WHEN typeof(stream_version)='integer' THEN stream_version END AS stream_version,
        CASE WHEN typeof(event_id)='text' AND length(CAST(event_id AS BLOB))<=128 THEN event_id END AS event_id
        FROM events WHERE stream_type='artifact' AND stream_id=? ORDER BY stream_version DESC LIMIT 1""", (digest,)).fetchone()
    if (row['schema_version'] != 1 or type(row['version']) is not int or row['version'] != expected_version
            or (tail is None and (row['version'] != 0 or row['boundary_id'] is not None))
            or (tail is not None and (tail['stream_version'] != row['version'] or tail['event_id'] is None or tail['event_id'] != row['boundary_id']))):
        raise CancellationHeld('integrity')
    try:
        if len(bytes.fromhex(row['chain'])) != 32:
            raise ValueError()
    except ValueError:
        raise CancellationHeld('integrity') from None
    return row

def before_append(store, digest, version):
    conn = store._connection
    from .transactional_dirty import tracking_present
    if tracking_present(conn):
        if not store._has_owned_write():
            return None
        from .transactional_dirty import require_clean_marker
        from orchestrator.lifecycle.cancel_completion import CancellationBudget
        diagnostic_budget = CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)
        try:
            require_clean_marker(conn, digest, diagnostic_budget)
            if not _structure(conn, diagnostic_budget):
                return None
            head = _head(conn, digest, version)
            if head is None:
                return None
            return (store._owned_write_token(), head['version'], head['chain'])
        except CancellationHeld:
            return None
    if not _structure(conn):
        return
    if not store._has_owned_write():
        conn.execute('DELETE FROM artifact_incremental_checkpoint WHERE digest=?', (digest,))
        return
    try:
        _head(conn, digest, version)
    except CancellationHeld:
        conn.execute('DELETE FROM artifact_incremental_checkpoint WHERE digest=?', (digest,))


def after_append(store, digest, events, candidate=None):
    conn = store._connection
    from .transactional_dirty import tracking_present
    if tracking_present(conn):
        if candidate is not None:
            _advance_tracked(store, digest, events, candidate)
        return
    if not _structure(conn):
        return
    row = conn.execute('SELECT version, chain FROM artifact_incremental_checkpoint WHERE digest=?', (digest,)).fetchone()
    if row is None:
        return
    chain = row['chain']
    try:
        for offset, event in enumerate(events, 1):
            if event.stream_version != row['version'] + offset:
                raise CancellationHeld('integrity')
            record = _validate(event, digest)
            chain = _step(chain, event)
            if event.event_type == 'ArtifactPublished':
                conn.execute('INSERT INTO artifact_incremental_publication VALUES (?, ?, ?, ?)', (digest, record.publication_id, event.event_id, event.stream_version))
        conn.execute('UPDATE artifact_incremental_checkpoint SET version=?, boundary_id=?, chain=? WHERE digest=?', (events[-1].stream_version, events[-1].event_id, chain, digest))
    except (CancellationHeld, sqlite3.IntegrityError, ValueError, TypeError):
        conn.execute('DELETE FROM artifact_incremental_checkpoint WHERE digest=?', (digest,))


def _advance_tracked(store, digest, events, candidate):
    """Advance one validated owned append; still not authority evidence."""
    import uuid
    conn = store._connection
    token, version, chain = candidate
    if not events or not store._has_owned_write() or store._owned_write_token() is not token:
        return
    # The original append already materializes its batch and may reread all
    # history. Bound only this additional diagnostic maintenance.
    if len(events) > 128:
        return
    total_bytes = 0
    for event in events:
        meta = conn.execute("SELECT typeof(payload_json), length(CAST(payload_json AS BLOB)) FROM events WHERE event_id=? AND stream_type='artifact' AND stream_id=?", (event.event_id, digest)).fetchone()
        if meta is None or meta[0] != 'text' or type(meta[1]) is not int or meta[1] > 65536:
            return
        total_bytes += meta[1]
        if total_bytes > 1024 * 1024:
            return
    # Validate all new envelopes before changing the index. A pre-existing
    # DIRTY marker never produces a candidate, including idempotent replay.
    publications = []
    try:
        for offset, event in enumerate(events, 1):
            if event.stream_version != version + offset:
                raise CancellationHeld('integrity')
            record = _validate(event, digest)
            chain = _step(chain, event)
            if event.event_type == 'ArtifactPublished':
                publications.append((digest, record.publication_id, event.event_id, event.stream_version))
    except (CancellationHeld, ValueError, TypeError):
        return
    # Expected duplicate index keys are an ordinary ineligible diagnostic,
    # not a reason to attempt a write that would abort the legacy append.
    ids = set()
    event_ids = set()
    for _, publication_id, event_id, _ in publications:
        if publication_id in ids or event_id in event_ids:
            return
        ids.add(publication_id)
        event_ids.add(event_id)
        if conn.execute('SELECT 1 FROM artifact_incremental_publication WHERE digest=? AND (publication_id=? OR event_id=?) LIMIT 1', (digest, publication_id, event_id)).fetchone():
            return
    name = 'artifact_dirty_advance_' + uuid.uuid4().hex
    conn.execute(f'SAVEPOINT {name}')
    try:
        marker = conn.execute("SELECT typeof(dirty), dirty FROM artifact_dirty_marker WHERE digest=?", (digest,)).fetchone()
        if marker is None or tuple(marker) != ('integer', 1):
            raise CancellationHeld('integrity')
        conn.executemany('INSERT INTO artifact_incremental_publication VALUES (?, ?, ?, ?)', publications)
        changed = conn.execute('UPDATE artifact_incremental_checkpoint SET version=?, boundary_id=?, chain=? WHERE digest=? AND version=?',
            (events[-1].stream_version, events[-1].event_id, chain, digest, version)).rowcount
        if changed != 1 or _head(conn, digest, events[-1].stream_version) is None:
            raise CancellationHeld('integrity')
        changed = conn.execute('UPDATE artifact_dirty_marker SET dirty=0 WHERE digest=? AND dirty=1', (digest,)).rowcount
        if changed != 1:
            raise CancellationHeld('integrity')
    except (CancellationHeld, ValueError, TypeError) as exc:
        conn.execute(f'ROLLBACK TO SAVEPOINT {name}')
        conn.execute(f'RELEASE SAVEPOINT {name}')
        if not conn.in_transaction or not store._has_owned_write() or store._owned_write_token() is not token:
            raise RuntimeError('diagnostic rollback lost write ownership') from exc
        state = conn.execute("SELECT typeof(dirty), CASE WHEN typeof(dirty)='integer' THEN dirty END FROM artifact_dirty_marker WHERE digest=?", (digest,)).fetchone()
        if state is None or tuple(state) != ('integer', 1):
            raise RuntimeError('diagnostic rollback did not preserve DIRTY') from exc
        return
    except BaseException:
        # SQLite does not identify whether an error originated in this direct
        # statement or a nested marker trigger. Propagate all database errors.
        conn.execute(f'ROLLBACK TO SAVEPOINT {name}')
        conn.execute(f'RELEASE SAVEPOINT {name}')
        raise
    conn.execute(f'RELEASE SAVEPOINT {name}')


def snapshot_incremental(store, digest, publication_id, budget):
    """Diagnostic only: validate head and exact row, not arbitrary old damage."""
    budget.bind(store)
    conn = store._connection
    from .transactional_dirty import require_clean_marker
    require_clean_marker(conn, digest, budget)
    # Conservative fixed accounting for schema, singleton and boundary probes.
    _charge(budget, 12, 16384)
    if not _structure(conn, budget):
        raise CancellationHeld('integrity')
    version = conn.execute("SELECT CASE WHEN typeof(current_version)='integer' THEN current_version END AS current_version FROM stream_versions WHERE stream_type='artifact' AND stream_id=?", (digest,)).fetchone()
    if version is None or type(version['current_version']) is not int:
        raise CancellationHeld('integrity')
    head = _head(conn, digest, version['current_version'])
    if head is None:
        raise CancellationHeld('integrity')
    index_meta = conn.execute("""SELECT typeof(event_id) AS et, length(CAST(event_id AS BLOB)) AS n,
        typeof(version) AS vt FROM artifact_incremental_publication WHERE digest=? AND publication_id=?""", (digest, publication_id)).fetchone()
    if (index_meta is None or index_meta['et'] != 'text' or type(index_meta['n']) is not int
            or index_meta['n'] > 128 or index_meta['vt'] != 'integer'):
        raise CancellationHeld('integrity')
    mapping = conn.execute('SELECT event_id, version FROM artifact_incremental_publication WHERE digest=? AND publication_id=?', (digest, publication_id)).fetchone()
    if mapping is None:
        raise CancellationHeld('integrity')
    lengths = ', '.join('COALESCE(length(CAST(' + col + ' AS BLOB)), 0) AS n' + str(i) for i, col in enumerate(_COLUMNS))
    types = ', '.join('typeof(' + col + ') AS t' + str(i) for i, col in enumerate(_COLUMNS))
    meta = conn.execute('SELECT ' + lengths + ', ' + types + " FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=? AND event_id=?", (digest, mapping['version'], mapping['event_id'])).fetchone()
    if meta is None:
        raise CancellationHeld('integrity')
    sizes = [meta['n' + str(i)] for i in range(len(_COLUMNS))]
    if any(type(n) is not int or n < 0 for n in sizes) or sum(sizes) > budget.max_row_bytes:
        raise CancellationHeld('budget')
    _event_storage_types(meta)
    _charge(budget, 2, sum(sizes) + 512)
    physical = conn.execute("SELECT " + ", ".join(_COLUMNS) + " FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=? AND event_id=?", (digest, mapping['version'], mapping['event_id'])).fetchone()
    if physical is None:
        raise CancellationHeld('integrity')
    try:
        event = store._row_to_event(physical)
        if _validate(event, digest).publication_id != publication_id:
            raise CancellationHeld('integrity')
        return event
    except CancellationHeld:
        raise
    except Exception:
        raise CancellationHeld('integrity') from None
