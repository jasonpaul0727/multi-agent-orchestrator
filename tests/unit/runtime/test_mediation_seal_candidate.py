"""Structural candidates are not host evidence or release authority."""
from dataclasses import FrozenInstanceError, replace
import hashlib
import json

import pytest

from tests.unit.runtime.test_mediation_review_fixes import operation

H = "sha256:" + "a" * 64
I = "sha256:" + "b" * 64


def candidate():
    from orchestrator.runtime.mediation_seal_candidate import ActorCandidate, SealCandidate
    return SealCandidate(operation=operation(), close_sequence=3, registry_version=5, closed=True,
        actors=(ActorCandidate(actor_id="actor-1", sequence=1, kind="worker",
            permit_version=2, ack_version=4, stop_version=5, prepared_sha256=H,
            ack_sha256=H, stop_sha256=H, transport_sha256=H, binding=None),))


def test_candidate_canonical_utf8_and_sha256_are_stable():
    from orchestrator.runtime.mediation_seal_candidate import encode_candidate, candidate_sha256
    value = candidate()
    encoded = encode_candidate(value, expected_registry_version=5)
    data = json.loads(encoded)
    assert data["domain"] == "orchestrator.mediation.seal-candidate.v1"
    assert data["schema_version"] == 1
    assert data["operation"]["operation_id"] == value.operation.operation_id
    assert data["actors"][0]["transport_sha256"] == H
    assert encoded == json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    assert candidate_sha256(value, expected_registry_version=5) == "sha256:" + hashlib.sha256(encoded).hexdigest()
    assert candidate_sha256(value, expected_registry_version=5) == "sha256:eef1cea319e1dfbad90cec1a9a0891d9f37ce1b21c6568399671cefe200414e3"
    with pytest.raises(FrozenInstanceError):
        value.close_sequence = 4


@pytest.mark.parametrize("change", ["stale", "open", "missing", "duplicate", "unsorted", "unknown", "wrong_binding", "after_close", "bad_sequence", "bad_digest", "unknown_event"])
def test_candidate_rejects_invalid_or_incomplete_structure(change):
    from orchestrator.runtime.mediation_seal_candidate import ActorCandidate, encode_candidate
    value = candidate(); actor = value.actors[0]
    expected = 5
    if change == "stale": expected = 6
    elif change == "open": value = replace(value, closed=False)
    elif change == "missing": value = replace(value, actors=(replace(actor, ack_sha256=None),))
    elif change == "duplicate": value = replace(value, actors=(actor, actor), close_sequence=4, registry_version=8); expected = 8
    elif change == "unsorted":
        a = replace(actor, actor_id="actor-2", sequence=2, permit_version=3, ack_version=6, stop_version=7)
        value = replace(value, actors=(a, actor), close_sequence=4, registry_version=8); expected = 8
    elif change == "unknown": value = replace(value, actors=(replace(actor, kind="invented"),))
    elif change == "wrong_binding": value = replace(value, actors=(replace(actor, kind="tool"),))
    elif change == "after_close": value = replace(value, actors=(replace(actor, permit_version=4, ack_version=2),))
    elif change == "bad_sequence": value = replace(value, actors=(replace(actor, sequence=True),))
    elif change == "bad_digest": value = replace(value, actors=(replace(actor, stop_sha256="private-task-secret"),))
    else: value = replace(value, registry_version=6); expected = 6
    with pytest.raises(ValueError) as error:
        encode_candidate(value, expected_registry_version=expected)
    assert "private-task-secret" not in str(error.value)
    assert "private-task-secret" not in repr(value)


def test_candidate_requires_typed_kind_specific_bindings():
    from orchestrator.runtime.mediation_seal_candidate import (ToolCandidate, ProviderCandidate,
        VerifierCandidate, encode_candidate)
    value = candidate(); actor = value.actors[0]
    for kind, binding in (
        ("tool", ToolCandidate(outcome="captured", outcome_sha256=H, publication_id="publication-1", publication_sha256=I)),
        ("provider_sender", ProviderCandidate(outcome="known_success", outcome_sha256=H, receipt_sha256=I)),
        ("verifier", VerifierCandidate(evidence_sha256=H, usage_sha256=I)),
    ):
        result = encode_candidate(replace(value, actors=(replace(actor, kind=kind, binding=binding),)), expected_registry_version=5)
        assert json.loads(result)["actors"][0]["binding"] is not None


def test_capacity_reserves_every_late_ack_and_stop():
    from orchestrator.runtime.mediation_seal_candidate import capacity_for_actors
    capacity = capacity_for_actors(128)
    assert capacity.before_seal == 386
    assert capacity.including_seal == 387
    assert capacity.max_late_evidence == 256
    with pytest.raises(ValueError): capacity_for_actors(129)


def test_candidate_requires_explicit_closed_marker():
    from orchestrator.runtime.mediation_seal_candidate import SealCandidate
    with pytest.raises(TypeError):
        SealCandidate(operation=operation(), close_sequence=2, registry_version=2, actors=())


def test_full_128_actor_candidate_allows_late_ack_and_stop():
    from orchestrator.runtime.mediation_seal_candidate import ActorCandidate, SealCandidate, encode_candidate
    actors = tuple(ActorCandidate(actor_id=f"actor-{n}", sequence=n, kind="worker", permit_version=1+n,
        ack_version=129+2*n, stop_version=130+2*n, prepared_sha256=H, ack_sha256=H,
        stop_sha256=H, transport_sha256=H, binding=None) for n in range(1, 129))
    value = SealCandidate(operation=operation(), close_sequence=130, registry_version=386, actors=actors, closed=True)
    assert len(json.loads(encode_candidate(value, expected_registry_version=386))["actors"]) == 128


@pytest.mark.parametrize("field", ["task_input_sha256", "workspace_sha256", "capability_sha256", "manifest_sha256", "policy_authority_sha256"])
def test_candidate_rejects_changed_operation_commitment_preimage(field):
    from orchestrator.runtime.mediation_seal_candidate import encode_candidate
    value = candidate()
    with pytest.raises(ValueError):
        encode_candidate(replace(value, operation=replace(value.operation, **{field: I})), expected_registry_version=5)


@pytest.mark.parametrize("field", ["prepared_sha256", "ack_sha256", "stop_sha256", "transport_sha256"])
def test_each_actor_digest_changes_structural_hash(field):
    from orchestrator.runtime.mediation_seal_candidate import candidate_sha256
    value = candidate()
    changed = replace(value, actors=(replace(value.actors[0], **{field: I}),))
    assert candidate_sha256(changed, expected_registry_version=5) != candidate_sha256(value, expected_registry_version=5)


def test_candidate_rejects_duplicate_ids_even_with_different_sequences():
    from orchestrator.runtime.mediation_seal_candidate import encode_candidate
    value = candidate(); actor = value.actors[0]
    a = replace(actor, permit_version=2, ack_version=5, stop_version=6)
    b = replace(actor, sequence=2, permit_version=3, ack_version=7, stop_version=8)
    with pytest.raises(ValueError):
        encode_candidate(replace(value, actors=(a, b), close_sequence=4, registry_version=8), expected_registry_version=8)


def test_empty_closed_candidate_is_structural_only():
    from orchestrator.runtime.mediation_seal_candidate import SealCandidate, encode_candidate
    value = SealCandidate(operation=operation(), close_sequence=2, registry_version=2, actors=(), closed=True)
    assert json.loads(encode_candidate(value, expected_registry_version=2))["actors"] == []


@pytest.mark.parametrize("entry", ["permit", "record_ack", "record_stop", "seal"])
def test_public_v2_authorization_entry_remains_disabled(tmp_path, entry):
    from orchestrator.persistence import SQLiteEventStore
    from orchestrator.runtime.mediation_journal import MediationJournal, MediationJournalError
    with SQLiteEventStore(tmp_path / "events.db") as store:
        journal = MediationJournal(store)
        args = {"permit": ("operation", None), "record_ack": ("operation", "actor", None),
                "record_stop": ("operation", "actor", None, None), "seal": ("operation",)}
        with pytest.raises(MediationJournalError):
            getattr(journal, entry)(*args[entry])
        assert not store.read_stream("mediation_v2", "operation")


def test_candidate_rejects_non_string_kind_without_calling_user_equality():
    from orchestrator.runtime.mediation_seal_candidate import encode_candidate
    class Impostor:
        def __eq__(self, other):
            raise AssertionError("caller equality executed")
    value = candidate()
    changed = replace(value, actors=(replace(value.actors[0], kind=Impostor()),))
    with pytest.raises(ValueError):
        encode_candidate(changed, expected_registry_version=5)


def test_unknown_constructor_field_does_not_leak_private_key():
    from orchestrator.runtime.mediation_seal_candidate import SealCandidate
    secret = "private_task_listing_secret"
    with pytest.raises((TypeError, ValueError)) as error:
        SealCandidate(operation=operation(), close_sequence=2, registry_version=2, actors=(), closed=True, **{secret: "value"})
    assert secret not in str(error.value)


@pytest.mark.parametrize("case", ["tool_unknown", "tool_missing_publication", "tool_failure_publication", "provider_unknown", "provider_missing_receipt", "verifier_missing_usage"])
def test_inapplicable_or_incomplete_kind_specific_binding_is_rejected(case):
    from orchestrator.runtime.mediation_seal_candidate import (ToolCandidate, ProviderCandidate,
        VerifierCandidate, encode_candidate)
    values = {
        "tool_unknown": ("tool", ToolCandidate(outcome="unknown", outcome_sha256=H, publication_id=None, publication_sha256=None)),
        "tool_missing_publication": ("tool", ToolCandidate(outcome="captured", outcome_sha256=H, publication_id=None, publication_sha256=I)),
        "tool_failure_publication": ("tool", ToolCandidate(outcome="known_failure", outcome_sha256=H, publication_id="publication-1", publication_sha256=I)),
        "provider_unknown": ("provider_sender", ProviderCandidate(outcome="unknown", outcome_sha256=H, receipt_sha256=I)),
        "provider_missing_receipt": ("provider_sender", ProviderCandidate(outcome="known_success", outcome_sha256=H, receipt_sha256=None)),
        "verifier_missing_usage": ("verifier", VerifierCandidate(evidence_sha256=H, usage_sha256=None)),
    }
    value = candidate(); kind, binding = values[case]
    with pytest.raises(ValueError):
        encode_candidate(replace(value, actors=(replace(value.actors[0], kind=kind, binding=binding),)), expected_registry_version=5)


@pytest.mark.parametrize("outcome", ["known_success", "known_failure", "not_sent"])
def test_provider_outcome_is_only_a_structural_label(outcome):
    from orchestrator.runtime.mediation_seal_candidate import ProviderCandidate, encode_candidate
    value = candidate()
    binding = ProviderCandidate(outcome=outcome, outcome_sha256=H, receipt_sha256=I)
    value = replace(value, actors=(replace(value.actors[0], kind="provider_sender", binding=binding),))
    data = json.loads(encode_candidate(value, expected_registry_version=5))
    assert data["actors"][0]["binding"]["outcome"] == outcome
    assert not any(key in data for key in ("verified", "stopped", "settled", "release", "no_effect"))


def test_tool_known_failure_does_not_invent_a_capture():
    from orchestrator.runtime.mediation_seal_candidate import ToolCandidate, encode_candidate
    value = candidate()
    binding = ToolCandidate(outcome="known_failure", outcome_sha256=H, publication_id=None, publication_sha256=None)
    value = replace(value, actors=(replace(value.actors[0], kind="tool", binding=binding),))
    data = json.loads(encode_candidate(value, expected_registry_version=5))
    assert data["actors"][0]["binding"]["publication_id"] is None
    assert data["actors"][0]["binding"]["publication_sha256"] is None


def test_operation_digest_impostor_cannot_trigger_deepcopy():
    from orchestrator.runtime.mediation_seal_candidate import encode_candidate
    class Impostor:
        def __deepcopy__(self, memo):
            raise AssertionError("caller deepcopy executed")
    value = candidate()
    changed = replace(value, operation=replace(value.operation, task_input_sha256=Impostor()))
    with pytest.raises(ValueError):
        encode_candidate(changed, expected_registry_version=5)
