from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from threading import Barrier

import pytest

from orchestrator.artifacts import ArtifactAccessGrant, ArtifactStore
from orchestrator.persistence import EventDraft, SQLiteEventStore
from orchestrator.runtime import (
    ArtifactRef,
    AttemptContext,
    VerificationCheck,
    VerificationEvidence,
    VerificationTask,
    VerifierProposalJournal,
    VerifierProposalJournalError,
)
from orchestrator.runtime import verification_journal


_HASH = "sha256:" + "a" * 64
_OTHER_HASH = "sha256:" + "b" * 64


def _context(**overrides):
    values = {
        "run_id": "run-1",
        "node_id": "node-1",
        "attempt_id": "attempt-1",
        "agent_instance_id": "agent-1",
        "fencing_generation": 2,
        "graph_version": 3,
        "input_manifest_hash": _HASH,
        "effective_config_hash": _HASH,
        "registry_hash": _HASH,
        "policy_manifest_hash": _HASH,
        "routing_decision_hash": _HASH,
        "planning_contract_hash": _HASH,
    }
    values.update(overrides)
    return AttemptContext(**values)


def _task(
    *,
    digest=_HASH,
    attempt_id="attempt-1",
    run_id="run-1",
    checks=None,
    fencing_generation=2,
    graph_version=3,
):
    return VerificationTask(
        context=_context(
            run_id=run_id,
            attempt_id=attempt_id,
            fencing_generation=fencing_generation,
            graph_version=graph_version,
        ),
        candidate_artifacts=(ArtifactRef(
            digest=digest,
            size_bytes=7,
            artifact_type="source",
            media_type="text/plain",
        ),),
        required_check_ids=checks or ("artifact-integrity", "python-syntax"),
        acceptance_contract="maestro.artifact-verification/v1",
    )


def _evidence(task, *, reverse_checks=False):
    digests = tuple(item.digest for item in task.candidate_artifacts)
    checks = tuple(
        VerificationCheck(check_id=check_id, passed=True, evidence_digests=digests)
        for check_id in task.required_check_ids
    )
    if reverse_checks:
        checks = tuple(reversed(checks))
    return VerificationEvidence(
        context=task.context,
        verifier_id="builtin.readonly-v1",
        outcome="accepted",
        checks=checks,
        inspected_digests=digests,
    )


def _canonical_bytes(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _reopened(path):
    store = SQLiteEventStore(path)
    return store, VerifierProposalJournal(store)


def _seed_journal(path):
    store, journal = _reopened(path)
    return store, journal, _task(), _evidence(_task())


def _tamper_event(database, statement, parameters):
    with sqlite3.connect(database) as connection:
        _drop_event_immutability_triggers(connection)
        connection.execute(statement, parameters)


def _drop_event_immutability_triggers(connection):
    triggers = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'events'"
    ).fetchall()
    for (name,) in triggers:
        escaped = name.replace('"', '""')
        connection.execute(f'DROP TRIGGER "{escaped}"')


def test_record_round_trips_from_fresh_store_with_canonical_hashes_and_exact_bindings(tmp_path):
    database = tmp_path / "events.db"
    store, journal, task, evidence = _seed_journal(database)
    record = journal.record(task, evidence)
    event = store.read_stream("verification_proposals", "run-1")[0]
    store.close()

    reopened_store, reopened_journal = _reopened(database)
    replayed = reopened_journal.read_run("run-1")

    expected_task_hash = "sha256:" + hashlib.sha256(_canonical_bytes(task.model_dump(mode="json"))).hexdigest()
    expected_evidence_hash = "sha256:" + hashlib.sha256(_canonical_bytes(evidence.model_dump(mode="json"))).hexdigest()
    assert len(replayed) == 1
    assert replayed[0] == record
    assert replayed[0].task == task
    assert replayed[0].evidence == evidence
    assert replayed[0].task_sha256 == expected_task_hash
    assert replayed[0].evidence_sha256 == expected_evidence_hash
    assert event.event_type == "VerifierProposalRecorded"
    assert event.schema_version == 1
    assert event.stream_type == "verification_proposals"
    assert event.stream_id == "run-1"
    assert event.run_id == "run-1"
    assert event.node_id == "node-1"
    assert event.attempt_id == "attempt-1"
    assert event.fencing_generation == 2
    assert event.correlation_id == "run-1"
    assert event.causation_id == expected_task_hash
    assert event.payload["schema_version"] == 1
    assert event.payload["graph_version"] == 3
    assert event.payload["artifact_digests"] == [_HASH]
    assert len(event.idempotency_key) > 0
    assert reopened_store.read_stream("lifecycle", "run-1") == []
    assert reopened_store.read_stream("scheduler", "global") == []
    reopened_store.close()


def test_read_proposal_validates_entire_stream_before_resolving_hash(tmp_path):
    store, journal, task, evidence = _seed_journal(tmp_path / "lookup.db")
    record = journal.record(task, evidence)
    assert journal.read_proposal("run-1", record.task_sha256) == record
    assert journal.read_proposal("run-1", _OTHER_HASH) is None
    store.append("verification_proposals", "run-1", 1,
        [EventDraft("UnexpectedProposal", {}, run_id="run-1", node_id="node-1",
                    attempt_id="attempt-2", fencing_generation=3, causation_id=_HASH)], "corrupt-tail")
    for digest in (record.task_sha256, _OTHER_HASH):
        with pytest.raises(VerifierProposalJournalError, match="integrity"):
            journal.read_proposal("run-1", digest)


def test_identical_retry_is_idempotent_and_distinct_task_hash_is_a_distinct_proposal(tmp_path):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")

    first = journal.record(task, evidence)
    retry = journal.record(task, evidence)
    other_task = _task(digest=_OTHER_HASH)
    second = journal.record(other_task, _evidence(other_task))

    assert retry == first
    assert second.task != first.task
    assert len(store.read_stream("verification_proposals", "run-1")) == 2
    store.close()


def test_conflicting_valid_evidence_cannot_reuse_proposal_identity(tmp_path):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    journal.record(task, evidence)

    with pytest.raises(VerifierProposalJournalError):
        journal.record(task, _evidence(task, reverse_checks=True))

    assert len(store.read_stream("verification_proposals", "run-1")) == 1
    store.close()


@pytest.mark.parametrize(
    "bad_task,bad_evidence",
    [
        (_task().model_copy(update={"acceptance_contract": "free-form"}), _evidence(_task())),
        (_task(checks=("artifact-integrity", "shell-execution")), _evidence(_task(checks=("artifact-integrity", "shell-execution")))),
        (_task(), _evidence(_task()).model_copy(update={"verifier_id": "untrusted.verifier"})),
        (_task(), _evidence(_task()).model_copy(update={"context": _context(attempt_id="other-attempt")})),
    ],
)
def test_record_revalidates_models_and_rejects_unsupported_or_misbound_proposals(
    tmp_path, bad_task, bad_evidence
):
    store, journal = _reopened(tmp_path / "events.db")

    with pytest.raises(VerifierProposalJournalError):
        journal.record(bad_task, bad_evidence)

    assert store.read_stream("verification_proposals", "run-1") == []
    store.close()


def test_record_rejects_an_oversized_canonical_envelope_before_append(tmp_path, monkeypatch):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    monkeypatch.setattr(verification_journal, "MAX_RECORD_BYTES", 64)

    with pytest.raises(VerifierProposalJournalError):
        journal.record(task, evidence)

    assert store.current_version("verification_proposals", "run-1") == 0
    store.close()


def test_identical_retry_succeeds_at_record_count_cap_but_new_proposal_fails(tmp_path, monkeypatch):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    monkeypatch.setattr(verification_journal, "MAX_PROPOSALS_PER_RUN", 1)
    first = journal.record(task, evidence)

    assert journal.record(task, evidence) == first
    other_task = _task(digest=_OTHER_HASH)
    with pytest.raises(VerifierProposalJournalError):
        journal.record(other_task, _evidence(other_task))
    assert len(store.read_stream("verification_proposals", "run-1")) == 1
    store.close()


def test_identical_retry_succeeds_at_aggregate_byte_cap_but_new_proposal_fails(tmp_path, monkeypatch):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    first = journal.record(task, evidence)
    encoded_size = len(_canonical_bytes(store.read_stream("verification_proposals", "run-1")[0].payload))
    monkeypatch.setattr(verification_journal, "MAX_RUN_PAYLOAD_BYTES", encoded_size)

    assert journal.record(task, evidence) == first
    other_task = _task(digest=_OTHER_HASH)
    with pytest.raises(VerifierProposalJournalError):
        journal.record(other_task, _evidence(other_task))
    assert len(store.read_stream("verification_proposals", "run-1")) == 1
    store.close()


def test_read_run_rejects_a_durable_stream_that_exceeds_replay_count_cap(tmp_path, monkeypatch):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    journal.record(task, evidence)
    other_task = _task(digest=_OTHER_HASH)
    journal.record(other_task, _evidence(other_task))
    monkeypatch.setattr(verification_journal, "MAX_PROPOSALS_PER_RUN", 1)

    with pytest.raises(VerifierProposalJournalError):
        journal.read_run("run-1")
    store.close()


def test_read_run_rejects_a_durable_stream_that_exceeds_replay_byte_cap(tmp_path, monkeypatch):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    journal.record(task, evidence)
    other_task = _task(digest=_OTHER_HASH)
    journal.record(other_task, _evidence(other_task))
    sizes = [
        len(_canonical_bytes(event.payload))
        for event in store.read_stream("verification_proposals", "run-1")
    ]
    monkeypatch.setattr(verification_journal, "MAX_RUN_PAYLOAD_BYTES", sum(sizes) - 1)

    with pytest.raises(VerifierProposalJournalError):
        journal.read_run("run-1")
    store.close()


@pytest.mark.parametrize(
    "column,value",
    [
        ("stream_type", "other_stream"),
        ("stream_id", "another-run"),
        ("run_id", "another-run"),
        ("node_id", "another-node"),
        ("attempt_id", "another-attempt"),
        ("fencing_generation", 9),
        ("correlation_id", "another-run"),
        ("causation_id", "sha256:" + "0" * 64),
        ("idempotency_key", "wrong-key"),
        ("event_type", "OtherEvent"),
        ("schema_version", 2),
    ],
)
def test_read_run_rejects_tampered_event_headers(tmp_path, column, value):
    database = tmp_path / "events.db"
    store, journal, task, evidence = _seed_journal(database)
    journal.record(task, evidence)
    store.close()
    _tamper_event(
        database,
        f"UPDATE events SET {column} = ? WHERE stream_type = 'verification_proposals'",
        (value,),
    )
    reopened_store, reopened_journal = _reopened(database)

    with pytest.raises(VerifierProposalJournalError):
        reopened_journal.read_run("run-1")
    reopened_store.close()


def test_read_run_checks_header_bindings_after_event_store_returns_valid_event(tmp_path, monkeypatch):
    store, journal, task, evidence = _seed_journal(tmp_path / "events.db")
    journal.record(task, evidence)
    read_stream = store.read_stream_with_version

    def return_event_with_wrong_causation(stream_type, stream_id, after_version=0):
        events, version = read_stream(stream_type, stream_id, after_version)
        tampered = [
            event.model_copy(update={"causation_id": "sha256:" + "0" * 64})
            for event in events
        ]
        return tampered, version

    monkeypatch.setattr(store, "read_stream_with_version", return_event_with_wrong_causation)

    with pytest.raises(VerifierProposalJournalError):
        journal.read_run("run-1")
    store.close()


def test_read_run_rejects_valid_run_event_remapped_to_another_stream_id(tmp_path, monkeypatch):
    store, journal = _reopened(tmp_path / "events.db")
    task = _task(run_id="run-a")
    journal.record(task, _evidence(task))
    read_stream = store.read_stream_with_version

    def remap_run_a_event_to_run_b(stream_type, stream_id, after_version=0):
        if stream_type == "verification_proposals" and stream_id == "run-b":
            events, version = read_stream(stream_type, "run-a", after_version)
            return [event.model_copy(update={"stream_id": "run-b"}) for event in events], version
        return read_stream(stream_type, stream_id, after_version)

    monkeypatch.setattr(store, "read_stream_with_version", remap_run_a_event_to_run_b)

    with pytest.raises(VerifierProposalJournalError):
        journal.read_run("run-b")
    store.close()


def test_read_run_rejects_payload_hash_or_binding_corruption_even_with_valid_event_hash(tmp_path):
    database = tmp_path / "events.db"
    store, journal, task, evidence = _seed_journal(database)
    journal.record(task, evidence)
    store.close()
    with sqlite3.connect(database) as connection:
        _drop_event_immutability_triggers(connection)
        row = connection.execute(
            "SELECT payload_json FROM events WHERE stream_type = 'verification_proposals'"
        ).fetchone()
        payload = json.loads(row[0])
        payload["task_sha256"] = "sha256:" + "0" * 64
        encoded = _canonical_bytes(payload)
        connection.execute(
            "UPDATE events SET payload_json = ?, payload_hash = ? WHERE stream_type = 'verification_proposals'",
            (encoded.decode("utf-8"), hashlib.sha256(encoded).hexdigest()),
        )
    reopened_store, reopened_journal = _reopened(database)

    with pytest.raises(VerifierProposalJournalError):
        reopened_journal.read_run("run-1")
    reopened_store.close()


@pytest.mark.parametrize(
    "field,value,generation,graph_version",
    [
        ("graph_version", 3.0, 2, 3),
        ("fencing_generation", 2.0, 2, 3),
        ("fencing_generation", True, 1, 3),
        ("graph_version", True, 2, 1),
    ],
)
def test_replay_rejects_type_changed_bindings_with_a_valid_event_payload_hash(
    tmp_path, field, value, generation, graph_version
):
    database = tmp_path / "events.db"
    store, journal = _reopened(database)
    task = _task(fencing_generation=generation, graph_version=graph_version)
    journal.record(task, _evidence(task))
    store.close()

    with sqlite3.connect(database) as connection:
        _drop_event_immutability_triggers(connection)
        row = connection.execute(
            "SELECT payload_json FROM events WHERE stream_type = 'verification_proposals'"
        ).fetchone()
        payload = json.loads(row[0])
        payload[field] = value
        encoded = _canonical_bytes(payload)
        connection.execute(
            "UPDATE events SET payload_json = ?, payload_hash = ? WHERE stream_type = 'verification_proposals'",
            (encoded.decode("utf-8"), hashlib.sha256(encoded).hexdigest()),
        )

    reopened_store, reopened_journal = _reopened(database)
    with pytest.raises(VerifierProposalJournalError):
        reopened_journal.read_run("run-1")
    reopened_store.close()


def test_concurrent_identical_writes_on_separate_connections_append_once(tmp_path):
    database = tmp_path / "events.db"
    initial = SQLiteEventStore(database)
    initial.close()
    task = _task()
    evidence = _evidence(task)
    barrier = Barrier(2)

    def submit():
        store = SQLiteEventStore(database)
        try:
            journal = VerifierProposalJournal(store)
            barrier.wait(timeout=5)
            return journal.record(task, evidence)
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = [future.result(timeout=15) for future in (executor.submit(submit), executor.submit(submit))]

    store = SQLiteEventStore(database)
    assert first == second
    assert len(store.read_stream("verification_proposals", "run-1")) == 1
    store.close()


def test_record_and_replay_do_not_mutate_lifecycle_scheduler_or_artifact_bytes(tmp_path):
    database = tmp_path / "events.db"
    store = SQLiteEventStore(database)
    store.append("lifecycle", "run-1", 0, [EventDraft("LifecycleMarker", {"value": 1})], "lifecycle-marker")
    store.append("scheduler", "global", 0, [EventDraft("SchedulerMarker", {"value": 2})], "scheduler-marker")
    artifact_store = ArtifactStore(
        tmp_path / "artifact-bytes",
        event_store=store,
        grant_verifier=lambda grant: grant.signature == "valid",
    )
    content = b"candidate bytes"
    artifact = artifact_store.publish_bytes(
        content,
        source={
            "run_id": "run-1",
            "node_id": "node-1",
            "attempt_id": "attempt-1",
            "fencing_generation": "2",
            "agent_instance_id": "agent-1",
        },
        artifact_type="source",
        media_type="text/plain",
        readable_scope=("run-1",),
    )
    task = _task(digest=artifact.digest)
    evidence = _evidence(task)
    grant = ArtifactAccessGrant(
        digest=artifact.digest,
        scope=("run-1",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="test-control-plane",
        signature="valid",
    )
    before_lifecycle = store.read_stream("lifecycle", "run-1")
    before_scheduler = store.read_stream("scheduler", "global")
    before_artifact_events = store.read_stream("artifact", artifact.digest)
    before_bytes = artifact_store.read_bytes(artifact.digest, grant)
    journal = VerifierProposalJournal(store)
    journal.record(task, evidence)
    journal.read_run("run-1")

    assert store.read_stream("lifecycle", "run-1") == before_lifecycle
    assert store.read_stream("scheduler", "global") == before_scheduler
    assert store.read_stream("artifact", artifact.digest) == before_artifact_events
    assert artifact_store.read_bytes(artifact.digest, grant) == before_bytes == content
    store.close()
