import sqlite3
import subprocess
import sys

import pytest

from orchestrator.artifacts import ArtifactStore
from orchestrator.artifacts.incremental_checkpoint import initialize_incremental
from orchestrator.lifecycle.cancel_completion import CancellationBudget
from orchestrator.persistence import SQLiteEventStore


def budget():
    return CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)


def marker(conn, digest):
    return conn.execute('SELECT dirty FROM artifact_dirty_marker WHERE digest=?', (digest,)).fetchone()[0]


def setup(store, tmp_path):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking, register_digest
    artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
    records = [artifacts.publish_bytes(x, source={'run_id': 'a'}, readable_scope=('a',)) for x in (b'a', b'b', b'c')]
    with store._cancellation_proof_write():
        for record in records:
            initialize_incremental(store, record.digest, budget())
        install_dirty_tracking(store)
        for record in records:
            register_digest(store, record.digest)
    return records


@pytest.mark.parametrize('recursive', [0, 1])
def test_old_process_replace_event_marks_conflicting_and_new_digest(tmp_path, recursive):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        a, b, c = setup(store, tmp_path)
        # The replacement collides with A's globally unique event_id while moving
        # the row to B. It is a standalone process with no application imports.
        sql = """INSERT OR REPLACE INTO events SELECT event_id, stream_type, ?, 99,
            event_type, schema_version, occurred_at, payload_json, payload_hash,
            idempotency_key, run_id, node_id, attempt_id, fencing_generation,
            correlation_id, causation_id FROM events WHERE stream_type='artifact'
            AND stream_id=? AND stream_version=1"""
        script = "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA recursive_triggers='+sys.argv[2]); c.execute(sys.argv[3],sys.argv[4:]); c.commit()"
        # With recursive triggers on, the existing immutability trigger rejects
        # REPLACE; either a successful replace is marked, or all state rolls back.
        proc = subprocess.run([sys.executable, '-c', script, str(db), str(recursive), sql, b.digest, a.digest], capture_output=True, text=True)
        if recursive:
            assert proc.returncode != 0 and 'immutable' in proc.stderr
            assert [marker(store._connection, r.digest) for r in (a, b, c)] == [0, 0, 0]
        else:
            assert proc.returncode == 0, proc.stderr
            assert [marker(store._connection, r.digest) for r in (a, b, c)] == [1, 1, 0]


def test_dirty_mark_rolls_back_with_nested_savepoint(tmp_path):
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, b, _ = setup(store, tmp_path)
        conn = store._connection
        with store._cancellation_proof_write():
            conn.execute('SAVEPOINT dirty_test')
            conn.execute("UPDATE stream_versions SET current_version=current_version+1 WHERE stream_type='artifact' AND stream_id=?", (a.digest,))
            assert marker(conn, a.digest) == 1
            conn.execute('ROLLBACK TO dirty_test')
            conn.execute('RELEASE dirty_test')
            assert marker(conn, a.digest) == 0
            assert marker(conn, b.digest) == 0


@pytest.mark.parametrize('recursive', [0, 1])
@pytest.mark.parametrize('sql,affected', [
    ("UPDATE stream_versions SET current_version=current_version+1 WHERE stream_type='artifact' AND stream_id=?", (1, 0)),
    ("DELETE FROM stream_versions WHERE stream_type='artifact' AND stream_id=?", (1, 0)),
    ("INSERT OR REPLACE INTO stream_versions VALUES ('artifact', ?, 9)", (1, 0)),
    ("INSERT INTO stream_versions VALUES ('artifact', ?, 9) ON CONFLICT(stream_type,stream_id) DO UPDATE SET current_version=9", (1, 0)),
    ("UPDATE OR REPLACE stream_versions SET stream_id=? WHERE stream_type='artifact' AND stream_id=?", (1, 1)),
    ("UPDATE artifact_incremental_checkpoint SET version=version+1 WHERE digest=?", (1, 0)),
    ("DELETE FROM artifact_incremental_checkpoint WHERE digest=?", (1, 0)),
    ("INSERT OR REPLACE INTO artifact_incremental_checkpoint SELECT digest, schema_version, version, boundary_id, chain FROM artifact_incremental_checkpoint WHERE digest=?", (1, 0)),
    ("UPDATE OR REPLACE artifact_incremental_checkpoint SET digest=? WHERE digest=?", (1, 1)),
    ("UPDATE artifact_incremental_publication SET version=version+1 WHERE digest=?", (1, 0)),
    ("DELETE FROM artifact_incremental_publication WHERE digest=?", (1, 0)),
    ("INSERT OR REPLACE INTO artifact_incremental_publication SELECT digest, publication_id, event_id, version FROM artifact_incremental_publication WHERE digest=?", (1, 0)),
    ("INSERT INTO artifact_incremental_publication SELECT digest, publication_id, event_id, version FROM artifact_incremental_publication WHERE digest=? ON CONFLICT(digest, publication_id) DO UPDATE SET version=excluded.version+1", (1, 0)),
    ("UPDATE OR REPLACE artifact_incremental_publication SET digest=? WHERE digest=?", (1, 1)),
])
def test_old_connection_mutations_mark_only_affected_digests(tmp_path, recursive, sql, affected):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        a, b, c = setup(store, tmp_path)
        args = [b.digest, a.digest] if affected == (1, 1) else [a.digest]
        script = "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA recursive_triggers='+sys.argv[2]); c.execute(sys.argv[3],sys.argv[4:]); c.commit()"
        proc = subprocess.run([sys.executable, '-c', script, str(db), str(recursive), sql, *args], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        assert [marker(store._connection, r.digest) for r in (a, b, c)] == [*affected, 0]


def test_legacy_database_without_opt_in_has_no_marker_schema(tmp_path):
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'legacy', source={'run_id': 'a'}, readable_scope=('a',))
        assert store._connection.execute("SELECT name FROM sqlite_master WHERE name LIKE 'artifact_dirty_%'").fetchall() == []


@pytest.mark.parametrize('commit', [False, True])
def test_old_process_append_and_dirty_commit_or_rollback_together(tmp_path, commit):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        a, b, _ = setup(store, tmp_path)
        script = """import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE')
c.execute(\"UPDATE stream_versions SET current_version=3 WHERE stream_type='artifact' AND stream_id=?\", (sys.argv[2],))
c.execute(\"INSERT INTO events SELECT 'legacy-event', stream_type, stream_id, 3, event_type, schema_version, occurred_at, payload_json, payload_hash, 'legacy-key', run_id, node_id, attempt_id, fencing_generation, correlation_id, causation_id FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=1\", (sys.argv[2],))
c.commit() if sys.argv[3]=='1' else c.rollback()
"""
        proc = subprocess.run([sys.executable, '-c', script, str(db), a.digest, str(int(commit))], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        assert marker(store._connection, a.digest) == int(commit)
        assert marker(store._connection, b.digest) == 0
        assert store._connection.execute("SELECT count(*) FROM events WHERE event_id='legacy-event'").fetchone()[0] == int(commit)
        assert store.current_version('artifact', a.digest) == (3 if commit else 2)


def test_drop_restore_trigger_is_undetectable_by_marker_alone(tmp_path):
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        conn = store._connection
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE name='artifact_dirty_stream_versions_update'").fetchone()[0]
        conn.execute('DROP TRIGGER artifact_dirty_stream_versions_update')
        conn.execute("UPDATE stream_versions SET current_version=9 WHERE stream_type='artifact' AND stream_id=?", (a.digest,))
        conn.execute(ddl)
        assert marker(conn, a.digest) == 0  # No schema-continuity proof exists.


@pytest.mark.parametrize('recursive', [0, 1])
@pytest.mark.parametrize('kind', ['update', 'delete', 'upsert'])
def test_event_mutation_triggers_cover_old_and_new_scope_when_immutability_removed(tmp_path, recursive, kind):
    # Isolate marker coverage; normal installations reject UPDATE/DELETE with
    # their pre-existing immutability triggers, which the REPLACE test checks.
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, b, c = setup(store, tmp_path)
        conn = store._connection
        conn.execute(f'PRAGMA recursive_triggers={recursive}')
        with store._cancellation_proof_write():
            conn.execute('DROP TRIGGER events_immutable_update')
            conn.execute('DROP TRIGGER events_immutable_delete')
            if kind == 'update':
                conn.execute("UPDATE events SET stream_id=?, stream_version=99 WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (b.digest, a.digest))
                expected = [1, 1, 0]
            elif kind == 'delete':
                conn.execute("DELETE FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (a.digest,))
                expected = [1, 0, 0]
            else:
                conn.execute("""INSERT INTO events SELECT event_id, stream_type, stream_id, stream_version,
                    event_type, schema_version, occurred_at, payload_json, payload_hash, idempotency_key,
                    run_id, node_id, attempt_id, fencing_generation, correlation_id, causation_id
                    FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=1
                    ON CONFLICT(event_id) DO UPDATE SET stream_id=?, stream_version=99""", (a.digest, b.digest))
                expected = [1, 1, 0]
            assert [marker(conn, r.digest) for r in (a, b, c)] == expected
            # Roll back DDL and data as well as markers.
            raise_marker = True
            conn.execute('SAVEPOINT undo_example')
            conn.execute('RELEASE undo_example')


@pytest.mark.parametrize('failure', ['missing_schema', 'mid_ddl'])
def test_failed_install_rolls_back_its_schema_but_preserves_outer_work(tmp_path, failure):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        if failure == 'mid_ddl':
            record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
                b'a', source={'run_id': 'a'}, readable_scope=('a',))
            with store._cancellation_proof_write():
                initialize_incremental(store, record.digest, budget())
        conn = store._connection
        with store._cancellation_proof_write():
            conn.execute('CREATE TABLE unrelated_outer_work (value TEXT)')
            conn.execute("INSERT INTO unrelated_outer_work VALUES ('preserved')")
            if failure == 'mid_ddl':
                count = [0]
                def authorizer(action, arg1, arg2, db, source):
                    if action == sqlite3.SQLITE_CREATE_TRIGGER:
                        count[0] += 1
                        if count[0] == 4:
                            return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK
                conn.set_authorizer(authorizer)
            try:
                with pytest.raises((RuntimeError, sqlite3.DatabaseError)):
                    install_dirty_tracking(store)
            finally:
                conn.set_authorizer(None)
        assert conn.execute('SELECT value FROM unrelated_outer_work').fetchone()[0] == 'preserved'
        assert conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'artifact_dirty_%'").fetchall() == []


def test_install_savepoint_does_not_release_same_named_caller_savepoint(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from orchestrator.artifacts import transactional_dirty
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
            conn = store._connection
            monkeypatch.setattr(transactional_dirty.uuid, 'uuid4', lambda: SimpleNamespace(hex='collision'))
            conn.execute('SAVEPOINT artifact_dirty_install_collision')
            transactional_dirty.install_dirty_tracking(store)
            conn.execute('ROLLBACK TO SAVEPOINT artifact_dirty_install_collision')
            conn.execute('RELEASE SAVEPOINT artifact_dirty_install_collision')
            assert conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'artifact_dirty_%'").fetchall() == []


@pytest.mark.parametrize('table,index_sql', [
    ('events', 'CREATE UNIQUE INDEX extra_unique ON events(idempotency_key)'),
    ('events', 'CREATE UNIQUE INDEX extra_unique ON events(lower(event_id))'),
    ('events', 'CREATE UNIQUE INDEX extra_unique ON events(event_id COLLATE NOCASE) WHERE stream_version=1'),
    ('stream_versions', 'CREATE UNIQUE INDEX extra_unique ON stream_versions(current_version) WHERE current_version=999'),
    ('artifact_incremental_checkpoint', 'CREATE UNIQUE INDEX extra_unique ON artifact_incremental_checkpoint(version) WHERE version=999'),
    ('artifact_incremental_publication', 'CREATE UNIQUE INDEX extra_unique ON artifact_incremental_publication(event_id)'),
])
def test_install_rejects_unreviewed_unique_indexes_without_changing_legacy_store(tmp_path, table, index_sql):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
        conn = store._connection
        conn.execute(index_sql)
        with store._cancellation_proof_write():
            with pytest.raises(RuntimeError):
                install_dirty_tracking(store)
        assert conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'artifact_dirty_%'").fetchall() == []
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='extra_unique'").fetchone() is not None
        assert store.current_version('artifact', record.digest) == 2
        other = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'other', source={'run_id': 'b'}, readable_scope=('b',))
        assert store.current_version('artifact', other.digest) == 2


def test_extra_unique_event_conflict_would_replace_another_digest(tmp_path):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        a = artifacts.publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
        b = artifacts.publish_bytes(b'b', source={'run_id': 'b'}, readable_scope=('b',))
        conn = store._connection
        conn.execute('CREATE UNIQUE INDEX extra_unique ON events(idempotency_key)')
        row = conn.execute("SELECT event_id, idempotency_key FROM events WHERE stream_id=? AND stream_version=1", (a.digest,)).fetchone()
        # New event_id and different stream key, but A's idempotency key triggers
        # the extra conflict source and silently replaces A with recursion off.
        conn.execute('PRAGMA recursive_triggers=OFF')
        conn.execute("""INSERT OR REPLACE INTO events SELECT 'new-event', stream_type, ?, 99,
            event_type, schema_version, occurred_at, payload_json, payload_hash,
            idempotency_key, run_id, node_id, attempt_id, fencing_generation,
            correlation_id, causation_id FROM events WHERE event_id=?""", (b.digest, row['event_id']))
        assert conn.execute('SELECT 1 FROM events WHERE event_id=?', (row['event_id'],)).fetchone() is None
        assert conn.execute("SELECT stream_id FROM events WHERE event_id='new-event'").fetchone()[0] == b.digest


def test_install_allows_extra_nonunique_index(tmp_path):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
            store._connection.execute('CREATE INDEX extra_nonunique ON events(event_type)')
            install_dirty_tracking(store)
        assert store._connection.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_dirty_schema'").fetchone()
