"""Diagnostic incremental experiment; none of these lookups grant access."""
from orchestrator.artifacts import ArtifactStore
from orchestrator.lifecycle.cancel_completion import CancellationBudget
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.artifacts.evidence_checkpoint import initialize


def budget():
    return CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)


def test_same_digest_other_run_exceeds_proof_budget_without_freezing(tmp_path):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as a:
        original = ArtifactStore(tmp_path / 'artifacts', event_store=a).publish_bytes(
            b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        payload = a.read_stream('artifact', original.digest)[-1].payload
        with a._cancellation_proof_write():
            initialize(a, original.digest, budget())
            from orchestrator.artifacts.incremental_checkpoint import initialize_incremental
            initialize_incremental(a, original.digest, budget())
        with SQLiteEventStore(db) as b:
            drafts = [EventDraft('ArtifactPublished', dict(payload, publication_id=f'b-{i}',
                        source={'run_id': 'b'}, readable_scope=['b'])) for i in range(4000)]
            events = b.append('artifact', original.digest, 2, drafts, 'run-b-batch')
            assert len(events) == 4000
            assert b.current_version('artifact', original.digest) == 4002
        # The legacy v1 validator reads the complete physical stream.
        import pytest
        from orchestrator.lifecycle.cancel_completion import CancellationHeld, read_event_window
        with a._cancellation_proof_write():
            with pytest.raises(CancellationHeld) as old:
                read_event_window(a, 'artifact', original.digest, budget())
            assert old.value.reason == 'budget'
        # The future lookup is diagnostic only and carries no read authority.
        from orchestrator.artifacts.incremental_checkpoint import snapshot_incremental
        with a._cancellation_proof_write():
            proof_budget = budget()
            event = snapshot_incremental(a, original.digest, original.publication_id, proof_budget)
            assert event.payload['publication_id'] == original.publication_id
            assert proof_budget.rows < 100
            assert proof_budget.bytes < 1024 * 1024



def test_missing_target_index_and_physical_tail_hold(tmp_path):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
        conn = store._connection
        saved = conn.execute('SELECT digest, publication_id, event_id, version FROM artifact_incremental_publication WHERE digest=? AND publication_id=?', (record.digest, record.publication_id)).fetchone()
        conn.execute('DELETE FROM artifact_incremental_publication WHERE digest=? AND publication_id=?', (record.digest, record.publication_id))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot_incremental(store, record.digest, record.publication_id, budget())
        conn.execute('INSERT INTO artifact_incremental_publication VALUES (?, ?, ?, ?)', tuple(saved))
        conn.execute("""INSERT INTO events SELECT 'physical-tail', stream_type, stream_id, 99999,
            event_type, schema_version, occurred_at, payload_json, payload_hash, 'physical-tail', run_id,
            node_id, attempt_id, fencing_generation, correlation_id, causation_id FROM events
            WHERE stream_type='artifact' AND stream_id=? AND stream_version=1""", (record.digest,))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot_incremental(store, record.digest, record.publication_id, budget())


def test_incremental_advance_rolls_back_with_outer_transaction(tmp_path):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        payload = store.read_stream('artifact', record.digest)[-1].payload
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
        with pytest.raises(RuntimeError):
            with store._cancellation_proof_write():
                store.append('artifact', record.digest, 2,
                    [EventDraft('ArtifactPublished', dict(payload, publication_id='rolled-back'))], 'rolled-back')
                raise RuntimeError('abort')
        with store._cancellation_proof_write():
            event = snapshot_incremental(store, record.digest, record.publication_id, budget())
            assert event.event_type == 'ArtifactPublished'
            assert store.current_version('artifact', record.digest) == 2


def test_out_of_band_old_row_corruption_is_not_proved_by_incremental_snapshot(tmp_path):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
            initialize_incremental(store, record.digest, budget())
        # This explicitly violates the trusted installation boundary. Restore
        # the trigger to demonstrate why schema checks alone cannot detect it.
        conn = store._connection
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'").fetchone()[0]
        conn.execute('DROP TRIGGER events_immutable_update')
        conn.execute("UPDATE events SET payload_json='{}' WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (record.digest,))
        conn.execute(ddl)
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, record.digest, record.publication_id, budget())
            assert snapshot_incremental(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'



def test_missing_immutable_trigger_holds_incremental_snapshot(tmp_path):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
        store._connection.execute('DROP TRIGGER events_immutable_update')
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot_incremental(store, record.digest, record.publication_id, budget())


def test_oversized_checkpoint_is_held_before_materialization(tmp_path):
    import pytest
    import sqlite3
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
        store._connection.execute('UPDATE artifact_incremental_checkpoint SET chain=? WHERE digest=?', ('x' * 1000000, record.digest))
        def guarded(cursor, row):
            if 'chain' in tuple(column[0] for column in cursor.description):
                raise AssertionError('oversized checkpoint was materialized')
            return sqlite3.Row(cursor, row)
        with store._cancellation_proof_write():
            store._connection.row_factory = guarded
            try:
                with pytest.raises(CancellationHeld):
                    snapshot_incremental(store, record.digest, record.publication_id, budget())
            finally:
                store._connection.row_factory = sqlite3.Row


def test_each_real_publication_advances_incremental_checkpoint(tmp_path):
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        original = artifacts.publish_bytes(b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, original.digest, budget())
        for i in range(3):
            publication = artifacts.publish_bytes(b'shared', source={'run_id': f'b{i}'}, readable_scope=(f'b{i}',))
            with store._cancellation_proof_write():
                assert snapshot_incremental(store, original.digest, original.publication_id, budget()).event_type == 'ArtifactPublished'
                assert snapshot_incremental(store, original.digest, publication.publication_id, budget()).event_type == 'ArtifactPublished'


def test_nested_savepoint_rollback_restores_incremental_index(tmp_path):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        payload = store.read_stream('artifact', record.digest)[-1].payload
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
            store._connection.execute('SAVEPOINT incremental_test')
            store.append('artifact', record.digest, 2,
                [EventDraft('ArtifactPublished', dict(payload, publication_id='temporary'))], 'temporary')
            store._connection.execute('ROLLBACK TO SAVEPOINT incremental_test')
            store._connection.execute('RELEASE SAVEPOINT incremental_test')
            assert snapshot_incremental(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'
            with pytest.raises(CancellationHeld):
                snapshot_incremental(store, record.digest, 'temporary', budget())


def test_non_target_index_deletion_is_outside_incremental_snapshot_proof(tmp_path):
    import pytest
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        a = artifacts.publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
        b = artifacts.publish_bytes(b'a', source={'run_id': 'b'}, readable_scope=('b',))
        with store._cancellation_proof_write():
            initialize(store, a.digest, budget())
            initialize_incremental(store, a.digest, budget())
        store._connection.execute('DELETE FROM artifact_incremental_publication WHERE digest=? AND publication_id=?', (a.digest, b.publication_id))
        with store._cancellation_proof_write():
            assert snapshot_incremental(store, a.digest, a.publication_id, budget()).event_type == 'ArtifactPublished'
            with pytest.raises(CancellationHeld):
                snapshot_incremental(store, a.digest, b.publication_id, budget())
            # The old full-scan checkpoint owns a separate complete index.
            assert snapshot(store, a.digest, a.publication_id, budget()).event_type == 'ArtifactPublished'


def test_target_event_hash_damage_is_detected(tmp_path):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        a = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, a.digest, budget())
        conn = store._connection
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'").fetchone()[0]
        conn.execute('DROP TRIGGER events_immutable_update')
        conn.execute("UPDATE events SET payload_json='{}' WHERE stream_type='artifact' AND stream_id=? AND stream_version=2", (a.digest,))
        conn.execute(ddl)
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot_incremental(store, a.digest, a.publication_id, budget())


def test_exact_target_read_ignores_unpreflighted_extra_sql_column(tmp_path):
    import sqlite3
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, snapshot_incremental
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize_incremental(store, record.digest, budget())
        conn = store._connection
        conn.execute('ALTER TABLE events ADD COLUMN extra_blob BLOB')
        ddl = conn.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='events_immutable_update'").fetchone()[0]
        conn.execute('DROP TRIGGER events_immutable_update')
        conn.execute("UPDATE events SET extra_blob=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=2", (b'x' * 2000000, record.digest))
        conn.execute(ddl)
        def guarded(cursor, row):
            if 'extra_blob' in tuple(column[0] for column in cursor.description):
                raise AssertionError('unpreflighted column was materialized')
            return sqlite3.Row(cursor, row)
        with store._cancellation_proof_write():
            conn.row_factory = guarded
            try:
                assert snapshot_incremental(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'
            finally:
                conn.row_factory = sqlite3.Row


def test_unicode_publication_index_bytes_are_charged_before_initialization(tmp_path, monkeypatch):
    import pytest
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental
    from orchestrator.artifacts.evidence_checkpoint import _event_window
    from orchestrator.artifacts.store import ArtifactStore as Parser
    from orchestrator.lifecycle.cancel_completion import CancellationHeld
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        original = store.read_stream('artifact', record.digest)[-1].payload
        unicode_id = '😀' * 1000
        store.append('artifact', record.digest, 2,
            [EventDraft('ArtifactPublished', dict(original, publication_id=unicode_id))], 'unicode')
        with store._cancellation_proof_write():
            measurement = budget()
            events, _ = _event_window(store, record.digest, measurement)
            pubs = [(record.digest, Parser._record_from_event(event, record.digest).publication_id, event.event_id)
                    for event in events if event.event_type == 'ArtifactPublished']
            char_cost = sum(sum(len(field) for field in item) + 192 for item in pubs) + 1024
            byte_cost = sum(sum(len(field.encode('utf-8')) for field in item) + 192 for item in pubs) + 1024
            assert byte_cost > char_cost
            limited = CancellationBudget(4000, measurement.bytes + (char_cost + byte_cost) // 2, 65536, 300, 100)
            import orchestrator.artifacts.incremental_checkpoint as module
            def forbidden(*args, **kwargs):
                raise AssertionError('schema write started before index bytes were reserved')
            monkeypatch.setattr(module, '_structure', forbidden)
            with pytest.raises(CancellationHeld) as error:
                initialize_incremental(store, record.digest, limited)
            assert error.value.reason == 'budget'
            assert store._connection.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_incremental_checkpoint'").fetchone() is None



