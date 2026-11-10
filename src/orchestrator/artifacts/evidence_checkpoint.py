"""Experimental artifact evidence checkpoint; never grants artifact access or release.

Eligibility is deliberately invalidated by any later physical append.  This
module is not wired into cancellation until every writer is independently
reviewed. All operations require the store's owned SQLite write transaction.
"""
import hashlib

from orchestrator.lifecycle.cancel_completion import CancellationHeld, read_event_window, read_version
from orchestrator.artifacts.store import ArtifactStore, ArtifactError


def _owned(store, budget):
    budget.bind(store)


# The tables are opt-in diagnostic state, not a trust root.  Exact schema
# recognition is required before attempting any maintenance on normal writes.
_TABLE_SQL = {
    'artifact_evidence_schema': 'CREATE TABLE artifact_evidence_schema (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)',
    'artifact_evidence_checkpoint': 'CREATE TABLE artifact_evidence_checkpoint (digest TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, version INTEGER NOT NULL, boundary_id TEXT, commitment TEXT NOT NULL)',
    'artifact_evidence_publication': 'CREATE TABLE artifact_evidence_publication (digest TEXT NOT NULL, publication_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (digest, publication_id), UNIQUE (digest, event_id))',
}


def _charge(budget, rows, size):
    if budget is not None:
        if budget.rows + rows > budget.max_rows or budget.bytes + size > budget.max_bytes:
            raise CancellationHeld('budget')
        budget.rows += rows
        budget.bytes += size


def _structure_ok(conn, budget=None):
    _charge(budget, 3, 512)
    metadata = conn.execute("SELECT name, length(CAST(sql AS BLOB)) AS n FROM sqlite_master WHERE type='table' AND name IN ('artifact_evidence_schema','artifact_evidence_checkpoint','artifact_evidence_publication') LIMIT 4").fetchall()
    if len(metadata) != 3 or any(type(row['n']) is not int or row['n'] > 4096 or (budget is not None and row['n'] > budget.max_row_bytes) for row in metadata):
        return False
    _charge(budget, 3, sum(row['n'] + len(row['name'].encode()) + 64 for row in metadata))
    rows = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name IN ('artifact_evidence_schema','artifact_evidence_checkpoint','artifact_evidence_publication') LIMIT 4").fetchall()
    return len(rows) == 3 and {row['name']: row['sql'] for row in rows} == _TABLE_SQL


def _event_window(store, digest, budget):
    # The shared reader has sentinel limits on both physical queries.
    _charge(budget, 2, 512)
    return read_event_window(store, 'artifact', digest, budget)


def _index_mapping(store, digest, budget):
    conn = store._connection
    budget.bind(store)
    _charge(budget, 1, 256)
    remaining = budget.max_rows - budget.rows
    rows = conn.execute('SELECT rowid AS r, length(CAST(publication_id AS BLOB)) AS a, length(CAST(event_id AS BLOB)) AS z, typeof(publication_id) AS at, typeof(event_id) AS zt FROM artifact_evidence_publication WHERE digest=? LIMIT ?', (digest, remaining + 1)).fetchall()
    if len(rows) * 2 > remaining or any(type(r['a']) is not int or type(r['z']) is not int or r['a'] + r['z'] > budget.max_row_bytes or r['at'] != 'text' or r['zt'] != 'text' for r in rows):
        raise CancellationHeld('budget')
    size = sum(r['a'] + r['z'] + 64 for r in rows)
    if budget.bytes + size > budget.max_bytes:
        raise CancellationHeld('budget')
    budget.rows += len(rows) * 2
    budget.bytes += size
    actual = conn.execute('SELECT rowid AS r, publication_id, event_id FROM artifact_evidence_publication WHERE digest=? LIMIT ?', (digest, len(rows))).fetchall()
    if [r['r'] for r in rows] != [r['r'] for r in actual]:
        raise CancellationHeld('integrity')
    return {r['publication_id']: r['event_id'] for r in actual}

def _schema(store, budget, *, create=False):
    _owned(store, budget)
    if budget.rows + 3 > budget.max_rows or budget.bytes + 512 > budget.max_bytes:
        raise CancellationHeld('budget')
    budget.rows += 3
    budget.bytes += 512
    conn = store._connection
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('artifact_evidence_schema','artifact_evidence_checkpoint','artifact_evidence_publication')").fetchall()}
    if not names and create:
        _charge(budget, 4, 2048)
        conn.execute('CREATE TABLE artifact_evidence_schema (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)')
        conn.execute('INSERT INTO artifact_evidence_schema VALUES (1, 1)')
        conn.execute('CREATE TABLE artifact_evidence_checkpoint (digest TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, version INTEGER NOT NULL, boundary_id TEXT, commitment TEXT NOT NULL)')
        conn.execute('CREATE TABLE artifact_evidence_publication (digest TEXT NOT NULL, publication_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (digest, publication_id), UNIQUE (digest, event_id))')
        return
    if names != {'artifact_evidence_schema', 'artifact_evidence_checkpoint', 'artifact_evidence_publication'}:
        raise CancellationHeld('integrity')
    if budget.rows + 3 > budget.max_rows or budget.bytes + 512 > budget.max_bytes:
        raise CancellationHeld('budget')
    if not _structure_ok(conn, budget):
        raise CancellationHeld('integrity')
    _charge(budget, 1, 64)
    row = conn.execute("SELECT CASE WHEN typeof(version)='integer' THEN version END AS version, typeof(version) AS t FROM artifact_evidence_schema WHERE id=1").fetchone()
    if row is None or row['t'] != 'integer' or row['version'] != 1:
        raise CancellationHeld('unsupported')


def initialize(store, digest, budget):
    _owned(store, budget)
    with store._cancellation_proof_write():
        _initialize(store, digest, budget)


def _initialize(store, digest, budget):
    _owned(store, budget)
    _schema(store, budget, create=True)
    events, version = _event_window(store, digest, budget)
    publications = {}
    chain = hashlib.sha256(b'artifact-evidence-v1\0')
    try:
        for event in events:
            if event.event_type not in ('ArtifactPublicationIntent', 'ArtifactPublished'):
                raise ValueError()
            record = ArtifactStore._record_from_event(event, digest)
            chain.update((str(event.stream_version) + '\0' + event.event_id + '\0' + event.payload_hash + '\0' + event.event_type + '\0').encode())
            if event.event_type == 'ArtifactPublished':
                if record.publication_id in publications:
                    raise ValueError()
                publications[record.publication_id] = event.event_id
    except (ArtifactError, ValueError, TypeError, KeyError):
        raise CancellationHeld('integrity') from None
    # Account for every inserted row; never allocate an unbounded index.
    added = len(publications) + 1
    size = sum(len(digest.encode()) + len(k.encode()) + len(v.encode()) + 192 for k, v in publications.items()) + 256
    if budget.rows + added > budget.max_rows or budget.bytes + size > budget.max_bytes:
        raise CancellationHeld('budget')
    budget.rows += added
    budget.bytes += size
    conn = store._connection
    _charge(budget, 1, 256)
    remaining = budget.max_rows - budget.rows
    old = conn.execute('SELECT length(CAST(publication_id AS BLOB)) AS a, length(CAST(event_id AS BLOB)) AS z FROM artifact_evidence_publication WHERE digest=? LIMIT ?', (digest, remaining + 1)).fetchall()
    if (len(old) > remaining or any(type(r['a']) is not int or type(r['z']) is not int
            or r['a'] + r['z'] > budget.max_row_bytes for r in old)):
        raise CancellationHeld('budget')
    old_bytes = sum(r['a'] + r['z'] + 64 for r in old)
    if budget.bytes + old_bytes > budget.max_bytes:
        raise CancellationHeld('budget')
    budget.rows += len(old)
    budget.bytes += old_bytes
    conn.execute('DELETE FROM artifact_evidence_publication WHERE digest=?', (digest,))
    conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))
    conn.execute('INSERT INTO artifact_evidence_checkpoint VALUES (?, 1, ?, ?, ?)',
                 (digest, version, events[-1].event_id if events else None, chain.hexdigest()))
    conn.executemany('INSERT INTO artifact_evidence_publication VALUES (?, ?, ?)',
                     [(digest, k, v) for k, v in publications.items()])


def snapshot(store, digest, publication_id, budget):
    """Diagnostic snapshot; still validates the complete bounded stream."""
    _owned(store, budget)
    _schema(store, budget)
    if budget.rows + 4 > budget.max_rows or budget.bytes + 2 * budget.max_row_bytes > budget.max_bytes:
        raise CancellationHeld('budget')
    budget.rows += 4
    budget.bytes += 2 * budget.max_row_bytes
    try:
        conn = store._connection
        bounds = conn.execute("""SELECT length(CAST(digest AS BLOB)) + length(CAST(commitment AS BLOB))
            + COALESCE(length(CAST(boundary_id AS BLOB)), 0) AS n
            FROM artifact_evidence_checkpoint WHERE digest=?""", (digest,)).fetchone()
        mb = conn.execute("""SELECT length(CAST(event_id AS BLOB)) + length(CAST(publication_id AS BLOB)) AS n
            FROM artifact_evidence_publication WHERE digest=? AND publication_id=?""", (digest, publication_id)).fetchone()
        if (bounds is None or mb is None or type(bounds['n']) is not int or type(mb['n']) is not int
                or bounds['n'] > budget.max_row_bytes or mb['n'] > budget.max_row_bytes):
            raise CancellationHeld('integrity')
        checkpoint = conn.execute("SELECT CASE WHEN typeof(schema_version)='integer' THEN schema_version END AS schema_version, CASE WHEN typeof(version)='integer' THEN version END AS version, boundary_id, commitment FROM artifact_evidence_checkpoint WHERE digest=?", (digest,)).fetchone()
        mapping = conn.execute('SELECT event_id FROM artifact_evidence_publication WHERE digest=? AND publication_id=?', (digest, publication_id)).fetchone()
        events, version = _event_window(store, digest, budget)
        if (checkpoint is None or checkpoint['schema_version'] != 1 or mapping is None
                or version != checkpoint['version'] or not events or events[-1].event_id != checkpoint['boundary_id']):
            raise CancellationHeld('integrity')
        chain = hashlib.sha256(b'artifact-evidence-v1\0')
        seen = {}
        for item in events:
            if item.event_type not in ('ArtifactPublicationIntent', 'ArtifactPublished'):
                raise CancellationHeld('integrity')
            rec = ArtifactStore._record_from_event(item, digest)
            chain.update((str(item.stream_version) + '\0' + item.event_id + '\0' + item.payload_hash + '\0' + item.event_type + '\0').encode())
            if item.event_type == 'ArtifactPublished':
                if rec.publication_id in seen:
                    raise CancellationHeld('integrity')
                seen[rec.publication_id] = item.event_id
        if _index_mapping(store, digest, budget) != seen:
            raise CancellationHeld('integrity')
        if checkpoint['commitment'] != chain.hexdigest():
            raise CancellationHeld('integrity')
        matches = [event for event in events if event.event_id == mapping['event_id']]
        if len(matches) != 1 or matches[0].event_type != 'ArtifactPublished':
            raise CancellationHeld('integrity')
        event = matches[0]
        if ArtifactStore._record_from_event(event, digest).publication_id != publication_id:
            raise CancellationHeld('integrity')
        return event
    except CancellationHeld:
        raise
    except Exception:
        raise CancellationHeld('integrity') from None



def _writer_budget():
    from orchestrator.lifecycle.cancel_completion import CancellationBudget
    return CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)


def _writer_charge_or_invalidate(conn, budget, rows, size, digest):
    try:
        _charge(budget, rows, size)
        return True
    except CancellationHeld:
        conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))
        return False


def refresh_after_append(store, digest):
    """Revalidate an opted-in digest in the append transaction or invalidate it."""
    conn = store._connection
    b = _writer_budget()
    if not _structure_ok(conn, b):
        return
    if not _writer_charge_or_invalidate(conn, b, 1, 128, digest):
        return
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='artifact_evidence_checkpoint'").fetchone()
    if not _writer_charge_or_invalidate(conn, b, 1, 128, digest):
        return
    if exists is None or conn.execute('SELECT 1 FROM artifact_evidence_checkpoint WHERE digest=?', (digest,)).fetchone() is None:
        return
    if not store._has_owned_write():
        conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))
        return
    try:
        with store._cancellation_proof_write():
            initialize(store, digest, b)
    except CancellationHeld:
        # No READY state survives an unsupported, damaged or oversized history.
        conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))


def verify_before_append(store, digest):
    """Invalidate, in the caller's transaction, inconsistent old checkpoint state."""
    conn = store._connection
    b = _writer_budget()
    if not _structure_ok(conn, b):
        return
    if not _writer_charge_or_invalidate(conn, b, 1, 128, digest):
        return
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='artifact_evidence_checkpoint'").fetchone() is None:
        return
    if not _writer_charge_or_invalidate(conn, b, 1, 256, digest):
        return
    preflight = conn.execute("""SELECT length(CAST(commitment AS BLOB)) AS a,
        COALESCE(length(CAST(boundary_id AS BLOB)), 0) AS b,
        typeof(schema_version) AS st, typeof(version) AS vt
        FROM artifact_evidence_checkpoint WHERE digest=?""", (digest,)).fetchone()
    if preflight is None:
        return
    if (type(preflight['a']) is not int or type(preflight['b']) is not int
            or preflight['a'] + preflight['b'] > 65536
            or preflight['st'] != 'integer' or preflight['vt'] != 'integer'):
        conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))
        return
    if not _writer_charge_or_invalidate(conn, b, 1, preflight['a'] + preflight['b'] + 128, digest):
        return
    checkpoint = conn.execute("SELECT CASE WHEN typeof(schema_version)='integer' THEN schema_version END AS schema_version, CASE WHEN typeof(version)='integer' THEN version END AS version, boundary_id, commitment FROM artifact_evidence_checkpoint WHERE digest=?", (digest,)).fetchone()
    if not store._has_owned_write():
        conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))
        return
    try:
        _schema(store, b)
        events, version = _event_window(store, digest, b)
        chain = hashlib.sha256(b'artifact-evidence-v1\0')
        expected = {}
        for event in events:
            if event.event_type not in ('ArtifactPublicationIntent', 'ArtifactPublished'):
                raise CancellationHeld('integrity')
            record = ArtifactStore._record_from_event(event, digest)
            chain.update((str(event.stream_version) + '\0' + event.event_id + '\0' + event.payload_hash + '\0' + event.event_type + '\0').encode())
            if event.event_type == 'ArtifactPublished':
                if record.publication_id in expected:
                    raise CancellationHeld('integrity')
                expected[record.publication_id] = event.event_id
        _charge(b, 1, 256)
        remaining = b.max_rows - b.rows
        rows = conn.execute('SELECT length(CAST(publication_id AS BLOB)) AS a, length(CAST(event_id AS BLOB)) AS z FROM artifact_evidence_publication WHERE digest=? LIMIT ?', (digest, remaining + 1)).fetchall()
        if len(rows) * 2 > remaining or any(type(r['a']) is not int or type(r['z']) is not int or r['a'] + r['z'] > b.max_row_bytes for r in rows):
            raise CancellationHeld('budget')
        size = sum(r['a'] + r['z'] + 64 for r in rows)
        if b.bytes + size > b.max_bytes:
            raise CancellationHeld('budget')
        b.rows += len(rows) * 2
        b.bytes += size
        actual = conn.execute('SELECT publication_id, event_id FROM artifact_evidence_publication WHERE digest=? LIMIT ?', (digest, len(rows))).fetchall()
        if (len(actual) != len(rows) or dict((r['publication_id'], r['event_id']) for r in actual) != expected
                or checkpoint['schema_version'] != 1 or checkpoint['version'] != version
                or checkpoint['boundary_id'] != (events[-1].event_id if events else None)
                or checkpoint['commitment'] != chain.hexdigest()):
            raise CancellationHeld('integrity')
    except (CancellationHeld, ArtifactError, ValueError, TypeError, KeyError):
        conn.execute('DELETE FROM artifact_evidence_checkpoint WHERE digest=?', (digest,))















