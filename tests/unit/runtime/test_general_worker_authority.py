"""Owned general resolution has no artifact-byte claim or filesystem callback."""

import os
from pathlib import Path

import pytest

from orchestrator.persistence import SQLiteEventStore
from orchestrator.recovery.run import RecoveredControlState, RunRecoveryError
from orchestrator.runtime.general_worker_authority import general_worker_host_scope
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournalError
from tests.unit.runtime.test_attempt_execution_coordinator import prepare_application


@pytest.fixture
def prepared(tmp_path):
    value = prepare_application(tmp_path)
    yield value
    value.app.close()


def test_general_control_projection_never_reads_artifact_files_under_lock(prepared, monkeypatch):
    app = prepared.app
    calls = []
    def forbidden(*args, **kwargs):
        calls.append("artifact")
        raise AssertionError("artifact bytes under SQLite")
    monkeypatch.setattr(app._artifact_store, "verify_run_artifacts", forbidden)
    monkeypatch.setattr(app._artifact_store, "pending_publications_for_run", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    def checked(events, version):
        state = app._scheduler.recovery.recover_control_state("run-1")
        assert type(state) is RecoveredControlState
        assert not hasattr(state, "artifacts")
        binding = app._scheduler._resolve_general_verification_binding(
            **prepared.identity, as_of=prepared.clock.now)
        assert binding.context == prepared.task.context
    app._event_store.append_checked("general_control_test", "run-1", "read", checked)
    assert calls == []


def test_full_recovery_still_rejects_corrupt_artifacts(prepared):
    app = prepared.app
    digest = prepared.task.candidate_artifacts[0].digest
    app._artifact_store._path_for_digest(digest).write_bytes(b"corrupt artifact bytes")
    assert app._scheduler.recovery.recover_control_state("run-1").active_attempts
    with pytest.raises(RunRecoveryError):
        app._scheduler.recovery.recover("run-1")


def test_general_binding_matches_full_frozen_context(prepared):
    scheduler = prepared.app._scheduler
    full = scheduler.resolve_verification_binding(**prepared.identity, as_of=prepared.clock.now)
    general = scheduler._resolve_general_verification_binding(**prepared.identity, as_of=prepared.clock.now)
    assert general == full
    assert scheduler._resolve_general_active_attempt(**prepared.identity,
        as_of=prepared.clock.now) == full.active_attempt


def test_host_scope_is_shared_but_database_copy_differs(tmp_path):
    with SQLiteEventStore(tmp_path / "first.db") as first:
        scope = general_worker_host_scope(first)
        with SQLiteEventStore(tmp_path / "first.db") as second:
            assert general_worker_host_scope(second) == scope
        with SQLiteEventStore(tmp_path / "copy.db") as copied:
            assert general_worker_host_scope(copied) != scope
        os.link(tmp_path / "first.db", tmp_path / "alias.db")
        with pytest.raises(GeneralWorkerJournalError):
            general_worker_host_scope(first)


def test_host_scope_rejects_ambient_transaction_before_file_read(tmp_path, monkeypatch):
    with SQLiteEventStore(tmp_path / "events.db") as store:
        def forbidden(*args, **kwargs):
            raise AssertionError("file access under lock")
        monkeypatch.setattr(Path, "read_text", forbidden)
        def checked(events, version):
            with pytest.raises(GeneralWorkerJournalError):
                general_worker_host_scope(store)
        store.append_checked("host_scope_test", "scope", "read", checked)


def test_operation_binding_uses_exact_frozen_route_and_context(prepared):
    from dataclasses import replace
    from orchestrator.runtime.general_worker_authority import validate_operation_binding
    from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal, metadata_hash
    app = prepared.app
    binding = app._scheduler._resolve_general_verification_binding(**prepared.identity, as_of=prepared.clock.now)
    scope = general_worker_host_scope(app._event_store)
    # This legacy acceptance fixture has no workspace admission; this unit
    # exercises comparison with an independently supplied host commitment.
    workspace = "sha256:" + "c" * 64
    operation = GeneralWorkerJournal(app._event_store).request(context=binding.context,
        input_commitment="sha256:" + "a" * 64,
        accepted_route_sha256=metadata_hash(binding.active_attempt.accepted_route.model_dump(mode="json")),
        workspace_identity_hash=workspace, host_scope_hash=scope, output_byte_limit=16_384)
    validate_operation_binding(operation, binding, workspace_identity_hash=workspace, host_scope_hash=scope)
    for field in ("accepted_route_sha256", "workspace_identity_hash", "host_scope_hash"):
        with pytest.raises(GeneralWorkerJournalError):
            validate_operation_binding(replace(operation, **{field: "sha256:" + "b" * 64}),
                binding, workspace_identity_hash=workspace, host_scope_hash=scope)
