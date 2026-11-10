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


def diagnostic(store, record):
    from orchestrator.artifacts.incremental_checkpoint import snapshot_incremental
    with store._cancellation_proof_write():
        return snapshot_incremental(store, record.digest, record.publication_id, budget())


@pytest.mark.parametrize('damage', ['dirty', 'missing_marker', 'missing_trigger', 'bad_version'])
def test_opted_in_diagnostic_holds_for_dirty_or_incomplete_tracking(tmp_path, damage):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, b, _ = setup(store, tmp_path)
        assert diagnostic(store, a).payload['publication_id'] == a.publication_id
        conn = store._connection
        if damage == 'dirty':
            conn.execute('UPDATE artifact_dirty_marker SET dirty=1 WHERE digest=?', (a.digest,))
        elif damage == 'missing_marker':
            conn.execute('DELETE FROM artifact_dirty_marker WHERE digest=?', (a.digest,))
        elif damage == 'missing_trigger':
            conn.execute('DROP TRIGGER artifact_dirty_events_insert')
        else:
            conn.execute('DELETE FROM artifact_dirty_schema')
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)
        if damage in ('dirty', 'missing_marker'):
            assert diagnostic(store, b).payload['publication_id'] == b.publication_id


def test_independent_old_writer_same_value_marks_and_blocks_only_its_diagnostic(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        a, b, _ = setup(store, tmp_path)
        script = "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('UPDATE artifact_incremental_publication SET version=version WHERE digest=?',(sys.argv[2],)); c.commit()"
        proc = subprocess.run([sys.executable, '-c', script, str(db), a.digest], capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)
        assert diagnostic(store, b).payload['publication_id'] == b.publication_id


@pytest.mark.parametrize('ambient', [False, True])
def test_append_does_not_erase_or_advance_opted_in_dirty_checkpoint(tmp_path, ambient):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        conn = store._connection
        original = tuple(conn.execute('SELECT * FROM artifact_incremental_checkpoint WHERE digest=?', (a.digest,)).fetchone())
        conn.execute('UPDATE artifact_dirty_marker SET dirty=1 WHERE digest=?', (a.digest,))
        if ambient:
            conn.execute('BEGIN')
        ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'new'}, readable_scope=('new',))
        if ambient:
            conn.commit()
        assert marker(conn, a.digest) == 1
        assert tuple(conn.execute('SELECT * FROM artifact_incremental_checkpoint WHERE digest=?', (a.digest,)).fetchone()) == original
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)


def test_clean_marker_turns_dirty_on_append_and_rollback_restores_diagnostic(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        with pytest.raises(RuntimeError, match='abort'):
            with store._cancellation_proof_write():
                ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
                    b'a', source={'run_id': 'new'}, readable_scope=('new',))
                assert marker(store._connection, a.digest) == 1
                with pytest.raises(CancellationHeld):
                    diagnostic(store, a)
                raise RuntimeError('abort')
        assert marker(store._connection, a.digest) == 0
        assert diagnostic(store, a).payload['publication_id'] == a.publication_id


def test_idempotent_replay_cannot_clear_dirty(tmp_path):
    from orchestrator.persistence import EventDraft
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        payload = store.read_stream('artifact', a.digest)[-1].payload
        draft = EventDraft('ArtifactPublished', dict(payload, publication_id='replay'))
        store.append('artifact', a.digest, 2, [draft], 'test-replay')
        assert marker(store._connection, a.digest) == 1
        store.append('artifact', a.digest, 2, [draft], 'test-replay')
        assert marker(store._connection, a.digest) == 1
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)


@pytest.mark.parametrize('damage', ['changed_trigger', 'extra_unique', 'duplicate_schema'])
def test_diagnostic_holds_for_changed_tracking_structure(tmp_path, damage):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        conn = store._connection
        if damage == 'changed_trigger':
            conn.execute('DROP TRIGGER artifact_dirty_events_insert')
            conn.execute('CREATE TRIGGER artifact_dirty_events_insert BEFORE INSERT ON events BEGIN SELECT 1; END')
        elif damage == 'extra_unique':
            conn.execute('CREATE UNIQUE INDEX extra_unique ON events(idempotency_key)')
        else:
            conn.execute('INSERT INTO artifact_dirty_schema VALUES (1)')
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)


def test_malformed_marker_fails_closed_without_loading_its_blob(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        conn = store._connection
        conn.execute('PRAGMA ignore_check_constraints=ON')
        conn.execute('UPDATE artifact_dirty_marker SET dirty=? WHERE digest=?', (sqlite3.Binary(b'x' * 1000000), a.digest))
        conn.execute('PRAGMA ignore_check_constraints=OFF')
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)


@pytest.mark.parametrize('target', ['index_install', 'index_read', 'dirty_object_read'])
def test_long_schema_names_are_rejected_before_python_materialization(tmp_path, target):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        if target == 'index_install':
            record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
            with store._cancellation_proof_write():
                initialize_incremental(store, record.digest, budget())
        else:
            record, _, _ = setup(store, tmp_path)
        conn = store._connection
        long_name = ('artifact_dirty_' if target == 'dirty_object_read' else 'long_index_') + 'x' * 70000
        conn.execute(f'CREATE INDEX "{long_name}" ON events(event_type)')
        def guard(cursor, values):
            if any(isinstance(value, str) and len(value) > 60000 for value in values):
                raise AssertionError('oversized schema name materialized')
            return sqlite3.Row(cursor, values)
        conn.row_factory = guard
        try:
            if target == 'index_install':
                with store._cancellation_proof_write():
                    with pytest.raises(RuntimeError):
                        install_dirty_tracking(store)
            else:
                with pytest.raises(CancellationHeld):
                    diagnostic(store, record)
        finally:
            conn.row_factory = sqlite3.Row


@pytest.mark.parametrize('temporary', [False, True])
def test_install_rejects_unreviewed_marker_reset_trigger(tmp_path, temporary):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
            prefix = 'TEMP ' if temporary else ''
            store._connection.execute(f'CREATE {prefix}TRIGGER unexpected AFTER UPDATE ON stream_versions BEGIN SELECT 1; END')
            with pytest.raises(RuntimeError):
                install_dirty_tracking(store)
        assert store._connection.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_dirty_schema'").fetchone() is None


@pytest.mark.parametrize('temporary', [False, True])
def test_reset_trigger_and_old_writer_cannot_make_diagnostic_clean(tmp_path, temporary):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        a, _, _ = setup(store, tmp_path)
        prefix = 'TEMP ' if temporary else ''
        store._connection.execute(f'CREATE {prefix}TRIGGER evil_reset AFTER UPDATE ON artifact_dirty_marker WHEN NEW.dirty=1 BEGIN UPDATE artifact_dirty_marker SET dirty=0 WHERE digest=NEW.digest; END')
        if not temporary:
            script = "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('UPDATE artifact_incremental_publication SET version=version WHERE digest=?',(sys.argv[2],)); c.commit()"
            proc = subprocess.run([sys.executable, '-c', script, str(db), a.digest], capture_output=True, text=True)
            assert proc.returncode == 0, proc.stderr
            assert marker(store._connection, a.digest) == 0
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)


def test_two_process_proof_lock_blocks_old_writer_then_dirty_holds(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        a, _, _ = setup(store, tmp_path)
        script = """import sqlite3,sys
c=sqlite3.connect(sys.argv[1], timeout=10)
print('attempting', flush=True)
c.execute('BEGIN IMMEDIATE')
c.execute('UPDATE artifact_incremental_publication SET version=version WHERE digest=?',(sys.argv[2],))
c.commit()
print('committed', flush=True)
"""
        proc = None
        try:
            with store._cancellation_proof_write():
                assert diagnostic(store, a).payload['publication_id'] == a.publication_id
                proc = subprocess.Popen([sys.executable, '-u', '-c', script, str(db), a.digest], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                assert proc.stdout.readline().strip() == 'attempting'
                with pytest.raises(subprocess.TimeoutExpired):
                    proc.wait(timeout=0.3)
                assert marker(store._connection, a.digest) == 0
            stdout, stderr = proc.communicate(timeout=10)
            assert proc.returncode == 0, stderr
            assert 'committed' in stdout
            assert marker(store._connection, a.digest) == 1
            with pytest.raises(CancellationHeld):
                diagnostic(store, a)
        finally:
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.communicate()


def test_diagnostic_holds_when_expected_event_trigger_is_missing(tmp_path):
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a, _, _ = setup(store, tmp_path)
        store._connection.execute('DROP TRIGGER gw_scope_check')
        with pytest.raises(CancellationHeld):
            diagnostic(store, a)


@pytest.mark.parametrize('target', ['install', 'read'])
@pytest.mark.parametrize('kind', ['column', 'collation'])
def test_long_table_column_or_collation_rejected_before_materialization(tmp_path, target, kind):
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        if target == 'install':
            record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
            with store._cancellation_proof_write():
                initialize_incremental(store, record.digest, budget())
        else:
            record, _, _ = setup(store, tmp_path)
        conn = store._connection
        long_name = 'z' * 70000
        if kind == 'column':
            conn.execute(f'ALTER TABLE events ADD COLUMN "{long_name}" TEXT')
            conn.execute(f'CREATE UNIQUE INDEX novel_idx ON events("{long_name}")')
        else:
            conn.create_collation(long_name, lambda a, b: (a > b) - (a < b))
            conn.execute(f'CREATE UNIQUE INDEX novel_idx ON events(event_id COLLATE "{long_name}")')
        def guard(cursor, values):
            if any(isinstance(value, str) and len(value) > 60000 for value in values):
                raise AssertionError('oversized column or collation materialized')
            return sqlite3.Row(cursor, values)
        conn.row_factory = guard
        try:
            if target == 'install':
                with store._cancellation_proof_write():
                    with pytest.raises(RuntimeError):
                        install_dirty_tracking(store)
            else:
                with pytest.raises(CancellationHeld):
                    diagnostic(store, record)
        finally:
            conn.row_factory = sqlite3.Row


def test_unopted_v1_append_ignores_long_extra_column(tmp_path):
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        name = 'v' * 70000
        store._connection.execute(f'ALTER TABLE events ADD COLUMN "{name}" TEXT')
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'legacy', source={'run_id': 'legacy'}, readable_scope=('legacy',))
        assert store.current_version('artifact', record.digest) == 2
        assert store._connection.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_dirty_schema'").fetchone() is None
