"""Real local entry points; schema names and separate handles are not writer proof."""

import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys

import pytest

from orchestrator.artifacts import ArtifactStore
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.persistence.snapshots import SnapshotStore


def cli_report(path):
    proc = subprocess.run([sys.executable, '-m', 'orchestrator.diagnostics.sqlite_continuity',
                           '--database', str(path)], capture_output=True, text=True, timeout=10)
    assert proc.returncode == 0, proc.stderr
    assert str(path) not in proc.stdout + proc.stderr
    report = json.loads(proc.stdout)
    assert report['non_authoritative'] is True
    assert 'writer_inventory_unproven' in report['unknowns']
    assert 'cross_connection_temp_unobservable' in report['unknowns']
    return report


def test_cli_table_names_are_observations_not_writer_inventory(tmp_path):
    known = tmp_path / 'events.db'
    other = tmp_path / 'other.db'
    with SQLiteEventStore(known):
        pass
    with sqlite3.connect(other) as conn:
        conn.execute('CREATE TABLE unrelated (x)')
    present, missing = cli_report(known), cli_report(other)
    for name in ('events', 'stream_versions', 'snapshots'):
        assert f'{name}_table_name_present' in present['findings']
        assert f'{name}_table_name_missing' in missing['findings']
    assert 'independent_connection_state_unobserved' in present['unknowns']
    assert 'independent_connection_state_unobserved' in missing['unknowns']
    assert 'eligible' not in json.dumps(present)


def test_independent_readonly_snapshot_and_raw_connections_are_distinct(tmp_path):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        store.append_checked('run', 'sample', 'checked',
                             lambda events, version: [EventDraft('RunCreated', {'private': 'payload'})])
        with SQLiteEventStore.open_read_only(db) as reader:
            path_snap = SnapshotStore(db)
            shared_snap = SnapshotStore(store)
            raw = sqlite3.connect(db, isolation_level=None)
            raw_snap = SnapshotStore(raw)
            independent = sqlite3.connect(db, isolation_level=None)
            try:
                assert reader._connection is not store._connection
                assert path_snap._connection is not store._connection
                assert shared_snap._connection is store._connection
                assert raw_snap._connection is raw
                assert independent is not raw and independent is not reader._connection
                for index, snap in enumerate((path_snap, shared_snap, raw_snap)):
                    snap.save_snapshot('run', f'snapshot-{index}', 1, {'x': index})
                    assert snap.load_valid('run', f'snapshot-{index}').state == {'x': index}
                assert reader.read_stream('run', 'sample')[0].event_type == 'RunCreated'
                reader._connection.execute('BEGIN')
                assert reader.current_version('run', 'sample') == 1
                store.append('run', 'sample', 1, [EventDraft('InputAccepted', {})], 'later')
                assert store.current_version('run', 'sample') == 2
                assert reader.current_version('run', 'sample') == 1
                independent.execute('BEGIN IMMEDIATE')
                report = cli_report(db)
                assert 'events_table_name_present' in report['findings']
                assert 'snapshots_table_name_present' in report['findings']
                assert 'independent_connection_state_unobserved' in report['unknowns']
                assert 'payload' not in json.dumps(report)
                assert independent.in_transaction
                independent.rollback()
                reader._connection.rollback()
            finally:
                if independent.in_transaction:
                    independent.rollback()
                independent.close()
                raw_snap.close()
                raw.close()
                shared_snap.close()
                path_snap.close()


def test_artifact_two_appends_and_ambient_savepoint_do_not_prove_continuity(tmp_path):
    db = tmp_path / 'events.db'
    with SQLiteEventStore(db) as store:
        class WrappedStore:
            def __init__(self, underlying):
                self.underlying = underlying
                self.append_transactions = []
            def __getattr__(self, name):
                return getattr(self.underlying, name)
            def append(self, *args, **kwargs):
                self.append_transactions.append(self.underlying._connection.in_transaction)
                return self.underlying.append(*args, **kwargs)

        custom = WrappedStore(store)
        artifacts = ArtifactStore(tmp_path / 'artifacts', event_store=custom)
        first = artifacts.publish_bytes(b'one', source={'run_id': 'fixture'}, readable_scope=('fixture',))
        assert [event.event_type for event in store.read_stream('artifact', first.digest)] == [
            'ArtifactPublicationIntent', 'ArtifactPublished']
        assert custom.append_transactions == [False, False]
        savepoints = []

        def authorizer(action, arg1, arg2, database, source):
            if action == sqlite3.SQLITE_SAVEPOINT:
                savepoints.append((arg1, arg2))
            return sqlite3.SQLITE_OK

        store._connection.set_authorizer(authorizer)
        try:
            store._connection.execute('BEGIN IMMEDIATE')
            second = artifacts.publish_bytes(b'two', source={'run_id': 'fixture'}, readable_scope=('fixture',))
            assert custom.append_transactions[-2:] == [True, True]
            assert ('BEGIN', 'orchestrator_append') in savepoints
            assert store.current_version('artifact', second.digest) == 2
            report = cli_report(db)
            assert 'events_table_name_present' in report['findings']
            assert 'independent_connection_state_unobserved' in report['unknowns']
            store._connection.rollback()
            assert store.read_stream('artifact', second.digest) == []
        finally:
            if store._connection.in_transaction:
                store._connection.rollback()
            store._connection.set_authorizer(None)


def test_application_standalone_custom_and_secret_server_are_separate_entries(tmp_path, monkeypatch):
    from dataclasses import replace
    from orchestrator.application import ControlPlaneApplication
    from orchestrator.config.models import ProviderSpec
    from orchestrator.models.gateway import SecretAccessContext
    from orchestrator.scheduler.core import ConcurrencyLimits
    from orchestrator.secrets import SecretAccessRule, SecretBrokerRequest, encode_secret_broker_request, decode_secret_broker_response
    from orchestrator.secrets.broker import UnavailableSecretValueStore
    from orchestrator.secrets import server as server_module

    limits = ConcurrencyLimits(system_active_attempts=2, run_active_attempts=2,
                               provider_active_attempts=2, tool_active_attempts=2)
    db = tmp_path / 'app.db'
    app = ControlPlaneApplication(db, limits=limits)
    standalone = ArtifactStore(tmp_path / 'standalone')
    provider = ProviderSpec(id='fixture', adapter='openai_compatible', endpoint='https://example.invalid/v1',
                            secret_ref='env:NOT_READ_FIXTURE', enabled=True)
    rule = SecretAccessRule(provider.id, provider.secret_ref, provider.effective_endpoint,
                            'model_inference', frozenset({'fixture-run'}))
    server = server_module.UnixSecretBrokerServer(
        socket_path=tmp_path / 'unused.sock', session_nonce='fixture_nonce', event_store_path=db,
        rules=(rule,), providers={provider.id: provider}, value_store=UnavailableSecretValueStore(),
        expected_uid=os.getuid())
    request = SecretBrokerRequest(
        version=1, session_nonce='fixture_nonce', secret_ref=provider.secret_ref,
        provider_id=provider.id, endpoint=provider.effective_endpoint, purpose='model_inference',
        context=SecretAccessContext(request_id='fixture-request', run_id='fixture-run',
                                    node_id='fixture-node', attempt_id='fixture-attempt',
                                    fencing_generation=1, accepted_route_id='fixture-route',
                                    budget_reservation_id='fixture-budget'))
    real_store = server_module.SQLiteEventStore
    opened = []

    def record_store(path):
        instance = real_store(path)
        opened.append(instance._connection)
        return instance

    monkeypatch.setattr(server_module, 'SQLiteEventStore', record_store)
    try:
        assert standalone._owned_event_store
        assert (tmp_path / 'standalone' / '.metadata.db').is_file()
        for number in (1, 2):
            server_socket, client_socket = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            try:
                client_socket.settimeout(3)
                context = request.context.model_copy(update={'request_id': f'fixture-request-{number}'})
                client_socket.sendall(encode_secret_broker_request(replace(request, context=context)))
                server._handle_one_connection(server_socket)
                response = decode_secret_broker_response(client_socket.recv(16385))
                assert response.status in ('unavailable', 'denied')
                assert response.credential_value is None
            finally:
                server_socket.close()
                client_socket.close()
        assert len(opened) == 2
        assert opened[0] is not opened[1]
        assert opened[0] is not app._event_store._connection
        assert standalone._event_store._connection is not app._event_store._connection
        app_report = cli_report(db)
        standalone_report = cli_report(tmp_path / 'standalone' / '.metadata.db')
        assert app_report['findings'] == standalone_report['findings']
        for report in (app_report, standalone_report):
            assert 'events_table_name_present' in report['findings']
            assert 'snapshots_table_name_present' in report['findings']
            assert 'independent_connection_state_unobserved' in report['unknowns']
            assert 'NOT_READ_FIXTURE' not in json.dumps(report)
    finally:
        server.close()
        standalone.close()
        app.close()



def test_table_name_findings_do_not_validate_schema_or_infer_entry(tmp_path):
    db = tmp_path / 'lookalike.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE events (unrelated TEXT)')
        conn.execute('CREATE VIEW snapshots AS SELECT 1 AS x')
    report = cli_report(db)
    assert 'events_table_name_present' in report['findings']
    assert 'stream_versions_table_name_missing' in report['findings']
    assert 'snapshots_table_name_missing' in report['findings']
    assert 'writer_inventory_unproven' in report['unknowns']
    assert 'independent_connection_state_unobserved' in report['unknowns']
    assert 'eligible' not in json.dumps(report)


@pytest.fixture
def diagnostic_case(tmp_path, monkeypatch):
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
        yield case, accepted
    finally:
        case.store.close()


def _diagnostic_publications(case, accepted):
    from orchestrator.artifacts.incremental_checkpoint import initialize_incremental, rebuild_incremental
    from orchestrator.artifacts.transactional_dirty import install_dirty_tracking, register_digest
    from orchestrator.lifecycle.cancel_completion import CancellationBudget

    def budget():
        return CancellationBudget(4000, 4 * 1024 * 1024, 65536, 300, 100)

    digest = accepted.capture.response_artifact.digest
    target = next(ArtifactStore._record_from_event(event, digest)
                  for event in case.store.read_stream('artifact', digest)
                  if event.event_type == 'ArtifactPublished'
                  and event.payload['publication_id'] == accepted.acceptance.response_publication_id)
    raw = case.artifacts._path_for_digest(digest).read_bytes()
    other = case.artifacts.publish_bytes(raw, source={'run_id': 'other'}, readable_scope=('other',))
    assert target.digest == other.digest and target.publication_id != other.publication_id
    with case.store._cancellation_proof_write():
        initialize_incremental(case.store, digest, budget())
        install_dirty_tracking(case.store)
        register_digest(case.store, digest)
        rebuild_incremental(case.store, digest, budget())
    return target, other, budget


def _validate_complete_v1(case, accepted):
    from orchestrator.lifecycle.cancel_completion import CancellationBudget, validate_v1_links, validate_v1_terminal
    with case.store._cancellation_proof_write():
        budget = CancellationBudget(2000, 4_000_000, 200_000, 100, 20)
        assert validate_v1_links(case.store, accepted, budget) is None
        release = validate_v1_terminal(case.store, accepted, budget)
        assert release.event_type == 'AttemptSlotReleased'
        assert ('artifact', accepted.capture.response_artifact.digest) in budget._cache
        return release


def _assert_redacted_schema_comparison(comparison, db, target, diagnostics=None):
    from dataclasses import asdict
    encoded = json.dumps(asdict(comparison))
    assert comparison.status == 'match', f"schema comparison observations: {diagnostics}"
    assert 'schema_continuity_unproven' in comparison.unknowns
    assert str(db) not in encoded and target.digest not in encoded
    assert 'artifact_dirty_' not in encoded and 'eligible' not in encoded


def _assert_empty_temp_observation(observation):
    from orchestrator.persistence._continuity_observation import _comparable_schema
    # An unavailable observation also has an empty temp_objects tuple. Only
    # check TEMP after the one-shot runner returned a complete usable sample.
    reasons = [reason.value for reason in observation.reasons]
    assert _comparable_schema(observation) is not None, f"TEMP observation unavailable: {reasons}"
    assert not observation.temp_objects


@pytest.mark.parametrize("reason", [
    "database_unavailable", "budget_exhausted", "metadata_invalid", "helper_cleanup_unconfirmed",
])
def test_failed_temp_observation_must_not_pass_empty_check(reason):
    from orchestrator.persistence._continuity_observation import ObservationReason, _failed
    failed = _failed(ObservationReason(reason))
    assert not failed.temp_objects
    with pytest.raises(AssertionError):
        _assert_empty_temp_observation(failed)

def test_drop_mutate_restore_identical_schema_cannot_establish_continuity(tmp_path, diagnostic_case):
    from orchestrator.artifacts.incremental_checkpoint import snapshot_incremental
    from orchestrator.persistence._continuity_observation import ObservationLimits, _compare_schema_observations

    db = tmp_path / 'events.db'
    case, accepted = diagnostic_case
    store = case.store
    target, other, budget = _diagnostic_publications(case, accepted)
    baseline = _validate_complete_v1(case, accepted)
    trigger_name = 'artifact_dirty_artifact_incremental_publication_update'
    old_rowid = store._connection.execute(
        "SELECT rowid FROM sqlite_master WHERE type='trigger' AND name=?", (trigger_name,)).fetchone()[0]
    assert store._connection.execute('SELECT count(*) FROM sqlite_master').fetchone()[0] > 1
    script = """import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
c.execute("BEGIN IMMEDIATE")
name="artifact_dirty_artifact_incremental_publication_update"
ddl=c.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)).fetchone()[0]
c.execute("DROP TRIGGER " + name)
assert c.execute("UPDATE artifact_incremental_publication SET version=version+100 WHERE digest=? AND publication_id=?", (sys.argv[2],sys.argv[3])).rowcount == 1
c.execute(ddl)
c.commit()
c.close()
"""
    def attack():
        child = subprocess.run([sys.executable, '-c', script, str(db), target.digest, other.publication_id],
                               capture_output=True, text=True, timeout=10)
        assert child.returncode == 0, child.stderr
    comparison = _compare_schema_observations(db, ObservationLimits(), attack)
    new_rowid = store._connection.execute(
        "SELECT rowid FROM sqlite_master WHERE type='trigger' AND name=?", (trigger_name,)).fetchone()[0]
    assert new_rowid != old_rowid
    assert store._connection.execute('SELECT dirty FROM artifact_dirty_marker WHERE digest=?',
                                     (target.digest,)).fetchone()[0] == 0
    assert store._connection.execute('SELECT version FROM artifact_incremental_publication WHERE digest=? AND publication_id=?',
                                     (target.digest, other.publication_id)).fetchone()[0] > 100
    _assert_redacted_schema_comparison(comparison, db, target)
    assert 'schema_continuity_unproven' in cli_report(db)['unknowns']
    with store._cancellation_proof_write():
        event = snapshot_incremental(store, target.digest, target.publication_id, budget())
    assert event.payload['publication_id'] == target.publication_id
    assert _validate_complete_v1(case, accepted) == baseline


def _compare_with_redacted_diagnostics(db, limits, between, monkeypatch):
    """Test-only probe: report categories and timing, never paths or metadata."""
    import time
    from orchestrator.persistence._continuity_observation import (
        _compare_schema_observations, _comparable_schema,
    )

    samples = []
    active = [None]
    real_popen = subprocess.Popen

    class HelperProbe:
        def __init__(self, child, detail):
            self._child = child
            self._detail = detail
            self._started = time.monotonic()
            self._communications = 0

        def __getattr__(self, name):
            return getattr(self._child, name)

        def kill(self):
            try:
                self._child.kill()
            except ProcessLookupError:
                self._detail["kill_status"] = "not_found"
                raise
            except OSError:
                self._detail["kill_status"] = "error"
                raise
            self._detail["kill_status"] = "requested"

        def communicate(self, *args, **kwargs):
            self._communications += 1
            first = self._communications == 1
            status_key = "helper_status" if first else "cleanup_status"
            try:
                output = self._child.communicate(*args, **kwargs)
            except subprocess.TimeoutExpired:
                self._detail[status_key] = "timeout"
                if first:
                    self._detail["helper_elapsed_ms"] = round(
                        (time.monotonic() - self._started) * 1000, 1
                    )
                raise
            except OSError:
                self._detail[status_key] = "communication_error"
                if first:
                    self._detail["helper_elapsed_ms"] = round(
                        (time.monotonic() - self._started) * 1000, 1
                    )
                raise
            if first:
                self._detail["helper_status"] = "exited"
                self._detail["helper_exit_code"] = self._child.returncode
                self._detail["helper_output_bytes"] = len(output[0] or b"")
                self._detail["helper_elapsed_ms"] = round(
                    (time.monotonic() - self._started) * 1000, 1
                )
            else:
                self._detail["cleanup_status"] = "reaped"
                self._detail["cleanup_exit_code"] = self._child.returncode
            return output

    def traced_popen(*args, **kwargs):
        command = args[0] if args else kwargs.get("args")
        is_helper = (
            active[0] is not None and isinstance(command, (list, tuple))
            and "orchestrator.persistence._continuity_observation" in command
            and "--helper" in command
        )
        try:
            child = real_popen(*args, **kwargs)
        except (OSError, ValueError):
            if is_helper:
                active[0]["helper_status"] = "spawn_error"
            raise
        return HelperProbe(child, active[0]) if is_helper else child

    def begin_sample():
        detail = {"helper_status": "not_started"}
        samples.append(detail)
        active[0] = detail
        detail["_started"] = time.monotonic()
        return True

    def end_sample(observation):
        detail = active[0]
        detail["elapsed_ms"] = round((time.monotonic() - detail.pop("_started")) * 1000, 1)
        detail["reasons"] = [reason.value for reason in observation.reasons]
        detail["complete"] = _comparable_schema(observation) is not None
        detail["temp_count"] = len(observation.temp_objects)
        active[0] = None
        return True  # Instrumentation must not alter the production decision.

    with monkeypatch.context() as patch:
        patch.setattr(subprocess, "Popen", traced_popen)
        comparison = _compare_schema_observations(
            db, limits, between, begin_sample=begin_sample, end_sample=end_sample,
        )
    return comparison, samples

@pytest.mark.parametrize("failed_index", [0, 1])
def test_comparison_diagnostics_identify_failed_side_without_private_data(tmp_path, monkeypatch, failed_index):
    from types import SimpleNamespace
    from orchestrator.persistence import _continuity_observation as observation

    db = tmp_path / "private-database-name.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE fixture (x)")
    original = observation._run_schema_observation
    calls = []

    def inject_failure(path, limits):
        index = len(calls)
        calls.append(index)
        if index == failed_index:
            return observation._failed(observation.ObservationReason.METADATA_INVALID)
        return original(path, limits)

    monkeypatch.setattr(observation, "_run_schema_observation", inject_failure)
    comparison, samples = _compare_with_redacted_diagnostics(
        db, observation.ObservationLimits(), lambda: None, monkeypatch,
    )
    assert comparison.status == "unavailable" and len(samples) == 2
    assert samples[failed_index]["complete"] is False
    assert "metadata_invalid" in samples[failed_index]["reasons"]
    assert samples[1 - failed_index]["complete"] is True
    with pytest.raises(AssertionError) as failure:
        _assert_redacted_schema_comparison(comparison, db, SimpleNamespace(digest="private-digest"), samples)
    message = str(failure.value)
    assert "metadata_invalid" in message
    assert str(db) not in message and "private-digest" not in message
    assert "CREATE TABLE" not in message


def test_comparison_diagnostics_record_helper_timeout_and_cleanup_without_leaks(tmp_path, monkeypatch):
    from orchestrator.persistence import _continuity_observation as observation

    class TimedOutHelper:
        returncode = None
        calls = 0

        def communicate(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired("private-command", kwargs["timeout"])
            return b"", None

        def kill(self):
            self.returncode = -9

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: TimedOutHelper())
    comparison, samples = _compare_with_redacted_diagnostics(
        tmp_path / "private-db-path", observation.ObservationLimits(), lambda: None, monkeypatch,
    )
    assert comparison.status == "unavailable" and len(samples) == 2
    for detail in samples:
        assert detail["helper_status"] == "timeout" and detail["cleanup_exit_code"] == -9
        assert "database_unavailable" in detail["reasons"] and detail["complete"] is False
        assert detail["helper_elapsed_ms"] >= 0 and detail["elapsed_ms"] >= 0
    encoded = json.dumps(samples)
    assert str(tmp_path) not in encoded and "private-command" not in encoded

@pytest.mark.parametrize(("first", "cleanup", "expected_reason"), [
    ("communication_error", "reaped", "database_unavailable"),
    ("timeout", "communication_error", "helper_cleanup_unconfirmed"),
    ("timeout", "timeout", "helper_cleanup_unconfirmed"),
])
def test_comparison_diagnostics_preserve_first_failure_and_cleanup_outcome(
    tmp_path, monkeypatch, first, cleanup, expected_reason,
):
    from orchestrator.persistence import _continuity_observation as observation

    class FaultingHelper:
        returncode = None
        calls = 0

        def communicate(self, *args, **kwargs):
            self.calls += 1
            outcome = first if self.calls == 1 else cleanup
            if outcome == "timeout":
                raise subprocess.TimeoutExpired("private-command", kwargs["timeout"])
            if outcome == "communication_error":
                raise OSError("private-error")
            self.returncode = -9
            return b"", None

        def kill(self):
            pass

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: FaultingHelper())
    comparison, samples = _compare_with_redacted_diagnostics(
        tmp_path / "private-db-path", observation.ObservationLimits(), lambda: None, monkeypatch,
    )
    assert comparison.status == "unavailable" and len(samples) == 2
    for detail in samples:
        assert detail["helper_status"] == first
        assert detail["cleanup_status"] == cleanup
        assert expected_reason in detail["reasons"] and detail["complete"] is False
    encoded = json.dumps(samples)
    assert str(tmp_path) not in encoded and "private-command" not in encoded
    assert "private-error" not in encoded

def test_future_legacy_process_temp_reset_is_invisible_and_report_is_non_authoritative(tmp_path, diagnostic_case, monkeypatch):
    import select
    from orchestrator.artifacts.incremental_checkpoint import snapshot_incremental
    from orchestrator.persistence._continuity_observation import ObservationLimits

    db = tmp_path / 'events.db'
    case, accepted = diagnostic_case
    store = case.store
    target, other, budget = _diagnostic_publications(case, accepted)
    baseline = _validate_complete_v1(case, accepted)
    script = """import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
c.execute("CREATE TEMP TRIGGER reset_marker AFTER UPDATE ON main.artifact_incremental_publication BEGIN UPDATE artifact_dirty_marker SET dirty=0 WHERE digest=NEW.digest; END")
assert c.execute("SELECT count(*) FROM sqlite_temp_master WHERE type='trigger' AND name='reset_marker'").fetchone()[0] == 1
assert c.execute("UPDATE artifact_incremental_publication SET version=version+100 WHERE digest=? AND publication_id=?", (sys.argv[2],sys.argv[3])).rowcount == 1
c.commit()
assert c.execute("SELECT count(*) FROM sqlite_temp_master WHERE type='trigger' AND name='reset_marker'").fetchone()[0] == 1
assert c.execute("SELECT dirty FROM artifact_dirty_marker WHERE digest=?", (sys.argv[2],)).fetchone()[0] == 0
print('ready', flush=True)
sys.stdin.read(1)
c.close()
"""
    child = None
    def attack():
        nonlocal child
        child = subprocess.Popen([sys.executable, '-u', '-c', script, str(db), target.digest, other.publication_id],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert select.select([child.stdout], [], [], 10)[0], 'child did not finish mutation'
        assert child.stdout.readline().strip() == 'ready'
    try:
        comparison, samples = _compare_with_redacted_diagnostics(db, ObservationLimits(), attack, monkeypatch)
        encoded_samples = json.dumps(samples)
        assert str(db) not in encoded_samples and target.digest not in encoded_samples
        assert "artifact_dirty_" not in encoded_samples and "CREATE " not in encoded_samples
        assert store._connection.execute('SELECT dirty FROM artifact_dirty_marker WHERE digest=?', (target.digest,)).fetchone()[0] == 0
        assert store._connection.execute('SELECT version FROM artifact_incremental_publication WHERE digest=? AND publication_id=?', (target.digest, other.publication_id)).fetchone()[0] > 100
        assert store._connection.execute("SELECT count(*) FROM sqlite_temp_master WHERE name='reset_marker'").fetchone()[0] == 0
        _assert_redacted_schema_comparison(comparison, db, target, samples)
        from orchestrator.persistence._continuity_observation import _run_schema_observation
        _assert_empty_temp_observation(_run_schema_observation(db, ObservationLimits()))
        assert 'schema_continuity_unproven' in cli_report(db)['unknowns']
        with store._cancellation_proof_write():
            event = snapshot_incremental(store, target.digest, target.publication_id, budget())
        assert event.payload['publication_id'] == target.publication_id
        assert _validate_complete_v1(case, accepted) == baseline
    finally:
        if child is not None:
            try:
                output, error = child.communicate(input='x', timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate(timeout=5)
                raise
            assert child.returncode == 0, error
