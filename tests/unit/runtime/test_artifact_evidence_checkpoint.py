import pytest

from orchestrator.artifacts import ArtifactStore
from orchestrator.lifecycle.cancel_completion import CancellationBudget, CancellationHeld
from orchestrator.persistence import SQLiteEventStore


def budget():
    return CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)


def test_real_publication_checkpoint_tracks_other_run_append(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        record = artifacts.publish_bytes(b'shared', source={'run_id': 'run-a'}, readable_scope=('run-a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
            event = snapshot(store, record.digest, record.publication_id, budget())
            assert event.payload['publication_id'] == record.publication_id
        with SQLiteEventStore(db) as other:
            ArtifactStore(tmp_path / 'artifacts', event_store=other).publish_bytes(
                b'shared', source={'run_id': 'run-b'}, readable_scope=('run-b',))
        with store._cancellation_proof_write():
            assert snapshot(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'


@pytest.mark.parametrize('tail', [3, 999999])
def test_physical_tail_beyond_version_is_held(tmp_path, tail):
    from orchestrator.artifacts.evidence_checkpoint import initialize
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'shared', source={'run_id': 'run-a'}, readable_scope=('run-a',))
        conn = store._connection
        conn.execute("""INSERT INTO events SELECT 'tail-' || ?, stream_type, stream_id, ?, event_type,
            schema_version, occurred_at, payload_json, payload_hash, 'tail-' || ?, run_id, node_id,
            attempt_id, fencing_generation, correlation_id, causation_id FROM events
            WHERE stream_type='artifact' AND stream_id=? AND stream_version=1""",
            (tail, tail, tail, record.digest))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                initialize(store, record.digest, budget())

@pytest.mark.parametrize('position', ['before', 'after'])
def test_real_v1_target_order_and_checkpoint_business_validation(tmp_path, monkeypatch, position):
    import hashlib
    import json
    from orchestrator.artifacts.evidence_checkpoint import initialize
    from orchestrator.lifecycle.cancel_completion import validate_v1_links, validate_v1_terminal
    from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
    from tests.unit.runtime.test_general_worker_acceptance import terminal_case, direct
    from tests.unit.runtime.test_verifier_process import _ChildLauncher
    original = IsolatedVerifierProcess.__init__
    def offline(self, **kwargs):
        kwargs.setdefault('launcher', _ChildLauncher())
        original(self, **kwargs)
    monkeypatch.setattr(IsolatedVerifierProcess, '__init__', offline)
    case = terminal_case(tmp_path)
    try:
        direct(case)
        accepted = case.journal.read(case.operation.identity)
        digest = accepted.capture.response_artifact.digest
        conn = case.store._connection
        if position == 'before':
            conn.execute('DROP TRIGGER events_immutable_update')
            row = conn.execute("SELECT payload_json FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (digest,)).fetchone()
            payload = json.loads(row[0]); payload['size'] = -1
            raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
            conn.execute("UPDATE events SET event_type='ArtifactPublished', payload_json=?, payload_hash=? WHERE stream_type='artifact' AND stream_id=? AND stream_version=1", (raw, hashlib.sha256(raw.encode()).hexdigest(), digest))
        else:
            row = conn.execute("SELECT payload_json FROM events WHERE stream_type='artifact' AND stream_id=? AND stream_version=2", (digest,)).fetchone()
            payload = json.loads(row[0]); payload['size'] = -1; payload['publication_id'] = 'malformed-later'
            raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
            conn.execute("UPDATE stream_versions SET current_version=3 WHERE stream_type='artifact' AND stream_id=?", (digest,))
            conn.execute("""INSERT INTO events SELECT 'malformed-later', stream_type, stream_id, 3,
                'ArtifactPublished', schema_version, occurred_at, ?, ?, 'malformed-later', run_id, node_id,
                attempt_id, fencing_generation, correlation_id, causation_id FROM events
                WHERE stream_type='artifact' AND stream_id=? AND stream_version=2""",
                (raw, hashlib.sha256(raw.encode()).hexdigest(), digest))
        with case.store._cancellation_proof_write():
            b = budget()
            validate_v1_links(case.store, accepted, b)
            if position == 'before':
                with pytest.raises(CancellationHeld):
                    validate_v1_terminal(case.store, accepted, b)
            else:
                validate_v1_terminal(case.store, accepted, b)
        with case.store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                initialize(case.store, digest, budget())
    finally:
        case.store.close()


def test_checkpoint_and_real_append_rollback_are_atomic(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    from orchestrator.persistence import EventDraft
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'shared', source={'run_id': 'run-a'}, readable_scope=('run-a',))
        with pytest.raises(RuntimeError):
            with store._cancellation_proof_write():
                initialize(store, record.digest, budget())
                raise RuntimeError('abort initialization')
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, record.digest, record.publication_id, budget())
            initialize(store, record.digest, budget())
        with pytest.raises(RuntimeError):
            with store._cancellation_proof_write():
                store.append('artifact', record.digest, 2, [EventDraft('unsupported', {})], 'abort')
                raise RuntimeError('abort append')
        with store._cancellation_proof_write():
            assert snapshot(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'


def test_orphan_physical_artifact_rows_hold(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'shared', source={'run_id': 'run-a'}, readable_scope=('run-a',))
        store._connection.execute('PRAGMA foreign_keys=OFF')
        store._connection.execute("DELETE FROM stream_versions WHERE stream_type='artifact' AND stream_id=?", (record.digest,))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                initialize(store, record.digest, budget())



def test_damaged_checkpoint_is_not_laundered_by_later_publication(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        record = artifacts.publish_bytes(b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
        store._connection.execute("UPDATE artifact_evidence_checkpoint SET commitment='bad' WHERE digest=?", (record.digest,))
        artifacts.publish_bytes(b'shared', source={'run_id': 'b'}, readable_scope=('b',))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, record.digest, record.publication_id, budget())


def test_two_connections_concurrent_real_publications_keep_checkpoint_consistent(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    db = tmp_path / 'events.db'
    root = tmp_path / 'artifacts'
    with SQLiteEventStore(db) as initial:
        record = ArtifactStore(root, event_store=initial).publish_bytes(
            b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        with initial._cancellation_proof_write():
            initialize(initial, record.digest, budget())
    start = Barrier(2)
    def publish(run):
        with SQLiteEventStore(db) as store:
            start.wait(timeout=10)
            return ArtifactStore(root, event_store=store).publish_bytes(
                b'shared', source={'run_id': run}, readable_scope=(run,)).publication_id
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publish, ('b', 'c')))
    with SQLiteEventStore(db) as final:
        with final._cancellation_proof_write():
            for publication in [record.publication_id, *results]:
                assert snapshot(final, record.digest, publication, budget()).payload['publication_id'] == publication


def test_failed_checkpoint_rebuild_rolls_back_inside_outer_transaction(tmp_path):
    import sqlite3
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
        with store._cancellation_proof_write():
            store._connection.execute("CREATE TEMP TRIGGER reject_checkpoint BEFORE INSERT ON artifact_evidence_checkpoint BEGIN SELECT RAISE(ABORT, 'injected'); END")
            with pytest.raises(sqlite3.IntegrityError):
                initialize(store, record.digest, budget())
            store._connection.execute('DROP TRIGGER reject_checkpoint')
            assert snapshot(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'


def test_unmodified_v1_publication_works_in_existing_external_transaction(tmp_path):
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        store._connection.execute('BEGIN IMMEDIATE')
        try:
            record = artifacts.publish_bytes(b'legacy', source={'run_id': 'a'}, readable_scope=('a',))
            assert record.digest.startswith('sha256:')
        finally:
            store._connection.rollback()


@pytest.mark.parametrize('commit', [False, True])
def test_external_transaction_invalidates_enabled_checkpoint_atomically(tmp_path, commit):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        record = artifacts.publish_bytes(b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
        store._connection.execute('BEGIN IMMEDIATE')
        artifacts.publish_bytes(b'shared', source={'run_id': 'b'}, readable_scope=('b',))
        if commit:
            store._connection.commit()
        else:
            store._connection.rollback()
        with store._cancellation_proof_write():
            if commit:
                with pytest.raises(CancellationHeld):
                    snapshot(store, record.digest, record.publication_id, budget())
            else:
                assert snapshot(store, record.digest, record.publication_id, budget()).event_type == 'ArtifactPublished'


def test_unknown_checkpoint_schema_version_holds(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
        store._connection.execute('UPDATE artifact_evidence_schema SET version=99')
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, record.digest, record.publication_id, budget())
            with pytest.raises(CancellationHeld):
                initialize(store, record.digest, budget())


def test_unsupported_artifact_event_invalidates_only_its_digest(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    from orchestrator.persistence import EventDraft
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        a = artifacts.publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
        b = artifacts.publish_bytes(b'b', source={'run_id': 'b'}, readable_scope=('b',))
        with store._cancellation_proof_write():
            initialize(store, a.digest, budget())
            initialize(store, b.digest, budget())
        store.append('artifact', a.digest, 2, [EventDraft('UnknownEvent', {})], 'unknown-event')
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, a.digest, a.publication_id, budget())
            assert snapshot(store, b.digest, b.publication_id, budget()).event_type == 'ArtifactPublished'


@pytest.mark.parametrize('damage', ['missing', 'orphan'])
def test_snapshot_holds_when_other_publication_index_is_damaged(tmp_path, damage):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        a = artifacts.publish_bytes(b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        b = artifacts.publish_bytes(b'shared', source={'run_id': 'b'}, readable_scope=('b',))
        with store._cancellation_proof_write():
            initialize(store, a.digest, budget())
        if damage == 'missing':
            store._connection.execute('DELETE FROM artifact_evidence_publication WHERE digest=? AND publication_id=?', (a.digest, b.publication_id))
        else:
            store._connection.execute('INSERT INTO artifact_evidence_publication VALUES (?, ?, ?)', (a.digest, 'orphan', 'missing-event'))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, a.digest, a.publication_id, budget())


def test_malformed_experimental_schema_does_not_block_unrelated_v1_publish(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        a = artifacts.publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, a.digest, budget())
        store._connection.execute('DROP TABLE artifact_evidence_checkpoint')
        store._connection.execute('CREATE TABLE artifact_evidence_checkpoint (other TEXT)')
        b = artifacts.publish_bytes(b'b', source={'run_id': 'b'}, readable_scope=('b',))
        assert b.digest != a.digest
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, a.digest, a.publication_id, budget())


def test_two_independent_processes_race_raw_store_appends(tmp_path):
    import json
    import os
    from pathlib import Path
    import subprocess
    import sys
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'shared', source={'run_id': 'a'}, readable_scope=('a',))
        payload = store.read_stream('artifact', record.digest)[-1].payload
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
    code = '''import json, sys
from orchestrator.persistence import SQLiteEventStore, EventDraft, StaleStream
path, digest, publication, raw = sys.argv[1:]
payload = json.loads(raw); payload['publication_id'] = publication
with SQLiteEventStore(path) as store:
 print('ready', flush=True)
 sys.stdin.readline()
 for i in range(30):
  version = store.current_version('artifact', digest)
  try:
   store.append('artifact', digest, version, [EventDraft('ArtifactPublished', payload)], publication)
   print('ok', flush=True); break
  except StaleStream: continue
 else: raise RuntimeError('no progress')
'''
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3] / 'src'))
    children = [subprocess.Popen([sys.executable, '-c', code, str(db), record.digest, f'process-{i}', json.dumps(payload)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env) for i in range(2)]
    try:
        for child in children:
            assert child.stdout.readline().strip() == 'ready'
        for child in children:
            child.stdin.write('go\n'); child.stdin.flush()
        for child in children:
            output, error = child.communicate(timeout=20)
            assert child.returncode == 0, error
            assert output.strip() == 'ok'
    finally:
        for child in children:
            if child.poll() is None:
                child.kill(); child.wait()
    with SQLiteEventStore(db) as store:
        with store._cancellation_proof_write():
            for publication in (record.publication_id, 'process-0', 'process-1'):
                assert snapshot(store, record.digest, publication, budget()).payload['publication_id'] == publication


def test_initialization_budget_is_checked_before_event_decode(tmp_path, monkeypatch):
    from orchestrator.artifacts.evidence_checkpoint import initialize
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        def forbidden(_row):
            raise AssertionError('decode must not happen')
        monkeypatch.setattr(SQLiteEventStore, '_row_to_event', staticmethod(forbidden))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld) as exc:
                initialize(store, record.digest, CancellationBudget(4, 1000000, 65536, 10, 10))
            assert exc.value.reason == 'budget'



def test_idempotent_replay_does_not_repair_damaged_checkpoint(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    from orchestrator.persistence import EventDraft
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifact = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        event = store.read_stream('artifact', artifact.digest)[-1]
        with store._cancellation_proof_write():
            initialize(store, artifact.digest, budget())
        store._connection.execute("UPDATE artifact_evidence_checkpoint SET commitment='bad' WHERE digest=?", (artifact.digest,))
        replay = store.append('artifact', artifact.digest, 1, [EventDraft(event.event_type, event.payload)], event.idempotency_key)
        assert replay[0].event_id == event.event_id
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                snapshot(store, artifact.digest, artifact.publication_id, budget())


def test_nested_savepoint_rollback_restores_checkpoint(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    from orchestrator.persistence import EventDraft
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifact = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, artifact.digest, budget())
            store._connection.execute('SAVEPOINT test_checkpoint')
            store.append('artifact', artifact.digest, 2, [EventDraft('UnknownEvent', {})], 'unknown')
            store._connection.execute('ROLLBACK TO SAVEPOINT test_checkpoint')
            store._connection.execute('RELEASE SAVEPOINT test_checkpoint')
            assert snapshot(store, artifact.digest, artifact.publication_id, budget()).event_type == 'ArtifactPublished'


@pytest.mark.parametrize('rows,bytes_limit', [(3, 1000000), (100, 64)])
def test_schema_reads_hold_before_exceeding_remaining_budget(tmp_path, rows, bytes_limit, monkeypatch):
    from orchestrator.artifacts.evidence_checkpoint import initialize, snapshot
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
        if rows == 3:
            import orchestrator.artifacts.evidence_checkpoint as module
            def forbidden(*args, **kwargs):
                raise AssertionError('schema structure query exceeded budget')
            monkeypatch.setattr(module, '_structure_ok', forbidden)
        small = CancellationBudget(rows, bytes_limit, 65536, 10, 10)
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld) as exc:
                snapshot(store, record.digest, record.publication_id, small)
            assert exc.value.reason == 'budget'
            assert small.rows <= rows and small.bytes <= bytes_limit



@pytest.mark.parametrize('rows,bytes_limit', [(1, 1000000), (100, 64)])
def test_writer_budget_bounds_checkpoint_preflight(tmp_path, monkeypatch, rows, bytes_limit):
    import orchestrator.artifacts.evidence_checkpoint as module
    from orchestrator.persistence import EventDraft
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        record = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        prior = store.read_stream('artifact', record.digest)[-1]
        with store._cancellation_proof_write():
            module.initialize(store, record.digest, budget())
        monkeypatch.setattr(module, '_writer_budget', lambda: CancellationBudget(rows, bytes_limit, 65536, 10, 10), raising=False)
        with pytest.raises(CancellationHeld) as exc:
            store.append('artifact', record.digest, 2, [EventDraft(prior.event_type, prior.payload)], 'new-publication')
        assert exc.value.reason == 'budget'
        assert store.current_version('artifact', record.digest) == 2



def test_writer_budget_exhaustion_after_schema_check_invalidates_and_preserves_write(tmp_path, monkeypatch):
    import orchestrator.artifacts.evidence_checkpoint as module
    from orchestrator.persistence import EventDraft
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifact = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        event = store.read_stream('artifact', artifact.digest)[-1]
        with store._cancellation_proof_write():
            module.initialize(store, artifact.digest, budget())
        monkeypatch.setattr(module, '_writer_budget', lambda: CancellationBudget(6, 1000000, 65536, 10, 10))
        payload = dict(event.payload, publication_id='new-publication')
        store.append('artifact', artifact.digest, 2, [EventDraft('ArtifactPublished', payload)], 'new-publication')
        assert store.current_version('artifact', artifact.digest) == 3
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                module.snapshot(store, artifact.digest, artifact.publication_id, budget())


def test_index_sentinel_never_returns_more_rows_than_remaining_budget(tmp_path):
    import sqlite3
    from orchestrator.artifacts.evidence_checkpoint import initialize, _index_mapping
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=store)
        record = artifacts.publish_bytes(b'a', source={'run_id': 'a'}, readable_scope=('a',))
        artifacts.publish_bytes(b'a', source={'run_id': 'b'}, readable_scope=('b',))
        with store._cancellation_proof_write():
            initialize(store, record.digest, budget())
        count = [0]
        def row_factory(cursor, row):
            if tuple(column[0] for column in cursor.description) == ('r', 'a', 'z', 'at', 'zt'):
                count[0] += 1
            return sqlite3.Row(cursor, row)
        with store._cancellation_proof_write():
            store._connection.row_factory = row_factory
            try:
                with pytest.raises(CancellationHeld):
                    _index_mapping(store, record.digest, CancellationBudget(1, 100000, 65536, 10, 10))
            finally:
                store._connection.row_factory = sqlite3.Row
            assert count[0] <= 1



def test_artifact_window_reserves_both_query_sentinels(tmp_path):
    from orchestrator.artifacts.evidence_checkpoint import _event_window
    with SQLiteEventStore(tmp_path / 'events.db') as store:
        artifact = ArtifactStore(tmp_path / 'artifacts', event_store=store).publish_bytes(
            b'a', source={'run_id': 'a'}, readable_scope=('a',))
        with store._cancellation_proof_write():
            with pytest.raises(CancellationHeld):
                _event_window(store, artifact.digest, CancellationBudget(6, 1000000, 65536, 10, 10))
            events, version = _event_window(store, artifact.digest, CancellationBudget(7, 1000000, 65536, 10, 10))
            assert version == 2 and len(events) == 2
