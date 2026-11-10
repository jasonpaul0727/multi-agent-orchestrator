"""Review regressions for bounded, commitment-bound mediation history."""
from dataclasses import asdict, replace
import hashlib
import json
import uuid

import pytest

from orchestrator.persistence.mediation_identity import mediation_stream_id
OP = mediation_stream_id("run-1", "node-1", "attempt-node-1-1", 1)

from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.mediation_actors import ActorIntent, MediationOperation
from orchestrator.runtime.mediation_journal import MediationJournal, MediationJournalError

H = "sha256:" + "a" * 64
J = "sha256:" + "b" * 64


def digest(fields):
    return "sha256:" + hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()


def operation():
    fields = dict(operation_id=OP, run_id="run-1", node_id="node-1", attempt_id="attempt-node-1-1",
                  fencing_generation=1, task_input_sha256=H, workspace_sha256=H,
                  capability_sha256=H, manifest_sha256=H, policy_authority_sha256=H)
    return MediationOperation(**fields, commitment_sha256=digest(fields))


def intent(actor_id="actor", sequence=1, **changes):
    fields = dict(operation_id=OP, fencing_generation=1, actor_id=actor_id,
                  sequence=sequence, kind="worker", host_id="host", boot_id="boot",
                  unit_name="unit.service", launch_id="launch", parameters_sha256=H)
    fields.update(changes)
    return ActorIntent(**fields, intent_sha256=digest(fields))


def record_inert(journal, operation_id, actor):
    """Reducer/census fixture only: never a launch grant or host proof."""
    try:
        return journal._append(operation_id, "Permitted", asdict(actor))
    except Exception:
        raise MediationJournalError() from None

@pytest.fixture
def journal(tmp_path):
    from tests.unit.runtime.test_general_worker_authority import dispatch_case
    case = dispatch_case(tmp_path, register=False)
    try:
        yield MediationJournal(case.store)
    finally:
        case.store.close()


@pytest.mark.parametrize("field", ["operation_id", "run_id", "node_id", "attempt_id",
    "fencing_generation", "task_input_sha256", "workspace_sha256", "capability_sha256",
    "manifest_sha256", "policy_authority_sha256"])
def test_operation_preimage_tamper_rejected(journal, field):
    op = operation()
    value = 2 if field == "fencing_generation" else (J if field.endswith("sha256") else "changed")
    with pytest.raises(MediationJournalError):
        journal.register(replace(op, **{field: value}))


@pytest.mark.parametrize("field", ["operation_id", "fencing_generation", "actor_id",
    "sequence", "kind", "host_id", "boot_id", "unit_name", "launch_id", "parameters_sha256"])
def test_actor_preimage_tamper_rejected(journal, field):
    journal.register(operation())
    item = intent()
    value = 2 if field in ("sequence", "fencing_generation") else (J if field.endswith("sha256") else "changed")
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, replace(item, **{field: value}))


def test_duplicate_unit_rejected_even_when_other_actor_and_parameters_change(journal):
    journal.register(operation())
    record_inert(journal, OP, intent())
    record_inert(journal, OP, intent())
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, intent("other", 2, kind="tool", launch_id="other", parameters_sha256=J))


@pytest.mark.parametrize("action", ["register", "permit", "close", "load"])
@pytest.mark.parametrize("damage", ["extra_tail", "too_many", "large_row", "large_history", "envelope"])
def test_physical_corruption_rejected_before_mutation_or_load(journal, action, damage, monkeypatch):
    journal.register(operation())
    connection = journal.event_store._connection
    connection.execute("DROP TRIGGER events_immutable_update")
    if damage in ("extra_tail", "too_many", "large_history"):
        count = 1 if damage == "extra_tail" else (131 if damage == "too_many" else 17)
        for version in range(2, count + 2):
            connection.execute("""INSERT INTO events
                SELECT ?, stream_type, stream_id, ?, event_type, schema_version, occurred_at,
                       ?, payload_hash, ?, run_id, node_id, attempt_id, fencing_generation,
                       correlation_id, causation_id FROM events
                 WHERE stream_type='mediation_v2' AND stream_version=1""",
                (str(uuid.uuid4()), version, "x" * 16000 if damage == "large_history" else "{}",
                 str(uuid.uuid4())))
        if damage != "extra_tail":
            connection.execute("UPDATE stream_versions SET current_version=? WHERE stream_type='mediation_v2'", (count + 1,))
    elif damage == "large_row":
        connection.execute("UPDATE events SET payload_json=? WHERE stream_type='mediation_v2'", ("x" * 20000,))
    else:
        connection.execute("UPDATE events SET schema_version=99 WHERE stream_type='mediation_v2'")
    before = connection.execute("SELECT count(*) FROM events").fetchone()[0]
    decoded = []
    original = SQLiteEventStore._row_to_event
    def observe(row):
        decoded.append(True)
        return original(row)
    monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
    with pytest.raises(MediationJournalError):
        if action == "register":
            journal.register(operation())
        elif action == "permit":
            record_inert(journal, OP, intent())
        elif action == "close":
            journal.close(OP)
        else:
            journal.load(OP)
    assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == before
    if damage != "envelope":
        assert not decoded


def test_public_error_does_not_leak_sensitive_metadata(journal):
    secret = "task-listing-secret"
    with pytest.raises(MediationJournalError) as error:
        journal.register(replace(operation(), run_id=secret))
    assert secret not in str(error.value)
    assert error.value.__cause__ is None



@pytest.mark.parametrize("target,field", [
    ("operation", field) for field in ("operation_id", "run_id", "node_id", "attempt_id",
        "fencing_generation", "task_input_sha256", "workspace_sha256", "capability_sha256",
        "manifest_sha256", "policy_authority_sha256")
] + [
    ("actor", field) for field in ("operation_id", "fencing_generation", "actor_id",
        "sequence", "kind", "host_id", "boot_id", "unit_name", "launch_id", "parameters_sha256")
])
def test_replay_rejects_changed_preimage_with_unchanged_commitment(journal, target, field):
    journal.register(operation())
    if target == "actor":
        record_inert(journal, OP, intent())
    version = 1 if target == "operation" else 2
    conn = journal.event_store._connection
    conn.execute("DROP TRIGGER events_immutable_update")
    raw = conn.execute("SELECT payload_json FROM events WHERE stream_type='mediation_v2' AND stream_version=?", (version,)).fetchone()[0]
    payload = json.loads(raw)
    payload["data"][field] = (2 if field in ("fencing_generation", "sequence") else
                               J if field.endswith("sha256") else "changed")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    value = hashlib.sha256(encoded.encode()).hexdigest()
    conn.execute("""UPDATE events SET payload_json=?, payload_hash=?, idempotency_key=?
                    WHERE stream_type='mediation_v2' AND stream_version=?""",
                 (encoded, value, "sha256:" + value, version))
    with pytest.raises(MediationJournalError):
        journal.load(OP)


@pytest.mark.parametrize("action", ["register", "permit", "close", "load"])
def test_unknown_payload_version_rejected(journal, action):
    journal.register(operation())
    conn = journal.event_store._connection
    conn.execute("DROP TRIGGER events_immutable_update")
    payload = json.loads(conn.execute("SELECT payload_json FROM events WHERE stream_type='mediation_v2'").fetchone()[0])
    payload["schema_version"] = 99
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    value = hashlib.sha256(encoded.encode()).hexdigest()
    conn.execute("UPDATE events SET payload_json=?, payload_hash=?, idempotency_key=? WHERE stream_type='mediation_v2'",
                 (encoded, value, "sha256:" + value))
    before = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    with pytest.raises(MediationJournalError):
        if action == "register":
            journal.register(operation())
        elif action == "permit":
            record_inert(journal, OP, intent())
        elif action == "close":
            journal.close(OP)
        else:
            journal.load(OP)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == before


@pytest.mark.parametrize("kind", ["worker", "tool", "provider_sender", "verifier"])
def test_supported_actor_kinds_are_accepted(journal, kind):
    journal.register(operation())
    item = intent(kind=kind)
    assert record_inert(journal, OP, item).actors[0].intent == item
    assert record_inert(journal, OP, item) == journal.load(OP)


def test_unknown_actor_kind_is_rejected_even_with_valid_digest(journal):
    journal.register(operation())
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, intent(kind="untrusted_runner"))
    assert journal.load(OP).actors == ()


@pytest.mark.parametrize("action", ["register", "permit", "close", "load"])
def test_unknown_actor_kind_in_history_is_rejected(journal, action):
    journal.register(operation())
    record_inert(journal, OP, intent())
    conn = journal.event_store._connection
    conn.execute("DROP TRIGGER events_immutable_update")
    raw = conn.execute("SELECT payload_json FROM events WHERE stream_type='mediation_v2' AND stream_version=2").fetchone()[0]
    payload = json.loads(raw)
    fields = payload["data"]
    fields["kind"] = "untrusted_runner"
    fields["intent_sha256"] = digest({k: v for k, v in fields.items() if k != "intent_sha256"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    value = hashlib.sha256(encoded.encode()).hexdigest()
    conn.execute("UPDATE events SET payload_json=?, payload_hash=?, idempotency_key=? WHERE stream_type='mediation_v2' AND stream_version=2",
                 (encoded, value, "sha256:" + value))
    before = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    with pytest.raises(MediationJournalError):
        if action == "register":
            journal.register(operation())
        elif action == "permit":
            record_inert(journal, OP, intent())
        elif action == "close":
            journal.close(OP)
        else:
            journal.load(OP)
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == before


@pytest.mark.parametrize("conflict", ["actor_id", "sequence"])
def test_actor_id_and_sequence_conflicts_are_independent_of_unit(journal, conflict):
    journal.register(operation())
    original = intent()
    state = record_inert(journal, OP, original)
    assert record_inert(journal, OP, original) == state
    other = (intent("actor", 2, unit_name="different.service", launch_id="different")
             if conflict == "actor_id" else
             intent("other", 1, unit_name="different.service", launch_id="different"))
    with pytest.raises(MediationJournalError):
        record_inert(journal, OP, other)
    assert journal.load(OP) == state


@pytest.mark.parametrize("action", ["register", "permit", "close", "load"])
def test_remote_tail_version_100_rejected_before_decode(journal, action, monkeypatch):
    journal.register(operation())
    conn = journal.event_store._connection
    conn.execute("""INSERT INTO events
        SELECT ?, stream_type, stream_id, 100, event_type, schema_version, occurred_at,
        payload_json, payload_hash, ?, run_id, node_id, attempt_id, fencing_generation,
        correlation_id, causation_id FROM events WHERE stream_type='mediation_v2' AND stream_version=1""",
        (str(uuid.uuid4()), str(uuid.uuid4())))
    decoded = []
    original = SQLiteEventStore._row_to_event
    def observe(row):
        decoded.append(True)
        return original(row)
    monkeypatch.setattr(SQLiteEventStore, "_row_to_event", staticmethod(observe))
    before = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    with pytest.raises(MediationJournalError):
        if action == "register":
            journal.register(operation())
        elif action == "permit":
            record_inert(journal, OP, intent())
        elif action == "close":
            journal.close(OP)
        else:
            journal.load(OP)
    assert not decoded
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == before


@pytest.mark.parametrize("action", ["permit", "close", "replay"])
def test_sensitive_sentinel_is_not_exposed_by_public_exceptions(journal, action):
    import traceback
    secret = "task-listing-secret"
    journal.register(operation())
    if action == "replay":
        conn = journal.event_store._connection
        conn.execute("DROP TRIGGER events_immutable_update")
        raw = conn.execute("SELECT payload_json FROM events WHERE stream_type='mediation_v2'").fetchone()[0]
        payload = json.loads(raw)
        payload["data"]["run_id"] = secret
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        value = hashlib.sha256(encoded.encode()).hexdigest()
        conn.execute("UPDATE events SET payload_json=?, payload_hash=?, idempotency_key=? WHERE stream_type='mediation_v2'",
                     (encoded, value, "sha256:" + value))
    with pytest.raises(MediationJournalError) as error:
        if action == "permit":
            record_inert(journal, OP, replace(intent(), actor_id=secret))
        elif action == "close":
            journal.close(secret + "!")
        else:
            journal.load(OP)
    assert secret not in "".join(traceback.format_exception(error.value))
    assert error.value.__cause__ is None


def test_exact_registration_and_permit_replay_remain_idempotent_after_close(journal):
    op = operation()
    actor = intent()
    journal.register(op)
    record_inert(journal, OP, actor)
    closed = journal.close(OP)
    assert journal.register(op) == closed
    assert record_inert(journal, OP, actor) == closed
    assert journal.close(OP) == closed
    assert journal.load(OP) == closed


def test_public_permit_is_disabled_without_host_authority(journal):
    journal.register(operation())
    with pytest.raises(MediationJournalError):
        journal.permit(OP, intent())
    assert journal.load(OP).actors == ()
