import pytest

from orchestrator.persistence.mediation_identity import mediation_stream_id
OP = mediation_stream_id("run-1", "node-1", "attempt-node-1-1", 1)

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.mediation_actors import ActorIntent, MediationOperation
from orchestrator.runtime.mediation_journal import MediationJournal, MediationJournalError

H = "sha256:" + "a" * 64


@pytest.fixture
def journal(tmp_path):
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        yield MediationJournal(case.store)
    finally:
        case.store.close()


from tests.unit.runtime.test_mediation_review_fixes import operation, intent, record_inert
from dataclasses import replace


def test_replay_is_idempotent_and_unacked_permit_remains_unknown_after_close(journal):
    journal.register(operation())
    record_inert(journal, OP, intent())
    record_inert(journal, OP, intent())
    state = journal.close(OP)
    assert state.closed
    assert len(state.actors) == 1
    assert state.actors[0].status == "launch_unknown"
    assert journal.load(OP) == state
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, intent("other", 2))


def test_conflicting_actor_identity_and_sequence_are_rejected(journal):
    journal.register(operation())
    record_inert(journal, OP, intent())
    for conflict in (intent("actor", 2), intent("other", 1)):
        with pytest.raises(MediationJournalError):
            record_inert(journal, OP, conflict)


def test_caller_evidence_is_not_authority(journal):
    journal.register(operation())
    record_inert(journal, OP, intent())
    with pytest.raises(MediationJournalError):
        journal.record_ack(OP, "actor", object())
    with pytest.raises(MediationJournalError):
        journal.record_stop(OP, "actor", object(), object())
    assert journal.load(OP).actors[0].status == "launch_unknown"


def test_unknown_ambient_transaction_is_rejected(journal):
    journal.event_store._connection.execute("BEGIN")
    try:
        with pytest.raises(MediationJournalError):
            journal.register(operation())
    finally:
        journal.event_store._connection.rollback()
    assert journal.load(OP) is None


def test_outer_rollback_does_not_leave_a_permit(journal):
    journal.register(operation())

    def abort(_events, _version):
        record_inert(journal, OP, intent())
        raise RuntimeError("rollback")

    with pytest.raises(RuntimeError):
        journal.event_store.append_checked("scheduler", "global", "rollback-test", abort)
    assert journal.load(OP).actors == ()


def test_capacity_and_bad_digest_fail_closed(journal, monkeypatch):
    import orchestrator.runtime.mediation_journal as module
    monkeypatch.setattr(module, "MAX_ACTORS", 1)
    journal.register(operation())
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, replace(intent("bad"), intent_sha256="bad"))
    record_inert(journal, OP, intent())
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, intent("other", 2))


def test_tampered_causation_fails_replay(journal):
    journal.register(operation())
    record_inert(journal, OP, intent())
    from orchestrator.runtime.mediation_journal import _replay
    events = journal.event_store.read_stream("mediation_v2", OP)
    events[1] = events[1].model_copy(update={"causation_id": "tampered"})
    with pytest.raises(ValueError):
        _replay(OP, events, 2)



def test_load_rejects_unknown_ambient_transaction(journal):
    journal.register(operation())
    journal.event_store._connection.execute("BEGIN")
    try:
        with pytest.raises(MediationJournalError):
            journal.load(OP)
    finally:
        journal.event_store._connection.rollback()


def test_unknown_envelope_version_rejected(journal):
    journal.register(operation())
    conn = journal.event_store._connection
    conn.execute("DROP TRIGGER events_immutable_update")
    conn.execute("UPDATE events SET schema_version=99 WHERE stream_type='mediation_v2'")
    with pytest.raises(MediationJournalError):
        journal.load(OP)


def test_reusing_unit_with_other_actor_rejected(journal):
    journal.register(operation())
    record_inert(journal, OP, intent())
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, intent("other", 2))


def test_version_lag_with_tail_rejected(journal):
    import uuid
    journal.register(operation())
    conn = journal.event_store._connection
    conn.execute("""INSERT INTO events
        SELECT ?, stream_type, stream_id, 2, event_type, schema_version, occurred_at,
        payload_json, payload_hash, ?, run_id, node_id, attempt_id, fencing_generation,
        correlation_id, causation_id FROM events WHERE stream_type='mediation_v2'""",
        (str(uuid.uuid4()), str(uuid.uuid4())))
    with pytest.raises(MediationJournalError):
        journal.close(OP)
