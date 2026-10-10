"""Acceptance exercises real durable authority; only the OS transport is local."""

from datetime import datetime, timedelta, timezone
import inspect
from types import SimpleNamespace

import pytest

from orchestrator.application import ApplicationNotReady, ControlPlaneApplication
from orchestrator.application import StartupRecoveryFailed
from orchestrator.budget import BudgetLedger, UsageRecord
from orchestrator.persistence import EventDraft
from orchestrator.runtime.acceptance_coordinator import (
    AttemptExecutionCoordinator, ProposalAcceptanceError,
)
from orchestrator.runtime.contracts import ArtifactRef, VerificationTask
from orchestrator.runtime.verification_journal import VerifierProposalJournal
from tests.support.attempt_acceptance import ADMISSION
from tests.unit.lifecycle.test_scheduler import (
    _application_limits, _resolver_durable_state, routed_pair, run_setup_with_frozen_contract,
)
from tests.unit.runtime.test_verifier_process import _ChildLauncher


class TrustedUsage:
    """Deterministic host-only service, never a production Provider backend."""

    def usage_for(self, snapshot):
        return UsageRecord(
            run_id=snapshot.accepted_route.run_id,
            reservation_id=snapshot.reservation.reservation_id,
            settlement_key="test-verified-usage", currency="USD", cost_minor=2,
            input_tokens=1, output_tokens=2,
        )


def prepare_application(tmp_path, *, live=False, usage_source=True, content=b'{"answer":42}',
                        record=True, admission=None):
    """Host-publish one exact Attempt fixture; live=True preserves the default Verifier."""
    app = ControlPlaneApplication(
        tmp_path / "events.db", limits=_application_limits(), artifact_root=tmp_path / "artifacts",
        usage_source=TrustedUsage() if usage_source is True else (None if usage_source is False else usage_source),
    )
    reg, config, lifecycle, manifest, contract = run_setup_with_frozen_contract(app._event_store)
    request, decision = routed_pair(reg, config, manifest, contract_hash=contract.contract_hash)
    clock = SimpleNamespace(now=datetime.now(timezone.utc))
    accepted = app.accept_routing(
        request, decision, accepted_at=clock.now - timedelta(seconds=1),
        lease_expires_at=clock.now + timedelta(minutes=2),
        **(admission or {**ADMISSION, "required_check_ids": ("artifact-integrity", "json")}),
    )
    coordinator = app._acceptance_coordinator
    assert isinstance(coordinator, AttemptExecutionCoordinator)
    if not live:
        coordinator._clock = lambda: clock.now
        coordinator._verifier._launcher = _ChildLauncher()
    binding = app._scheduler.resolve_verification_binding(
        run_id=request.run_id, node_id=request.node_id, attempt_id=request.attempt_id,
        fencing_generation=1, as_of=clock.now,
    )
    artifact = app._artifact_store.publish_bytes(
        content, source={
            "run_id": request.run_id, "node_id": request.node_id,
            "attempt_id": request.attempt_id, "fencing_generation": "1",
            "agent_instance_id": accepted.agent_instance_id,
        }, artifact_type="json-document", media_type="application/json",
        readable_scope=(request.run_id,),
    )
    task = VerificationTask(
        context=binding.context, candidate_artifacts=(ArtifactRef(
            digest=artifact.digest, size_bytes=artifact.size, artifact_type=artifact.artifact_type,
            media_type=artifact.media_type,
        ),), required_check_ids=binding.required_check_ids,
        acceptance_contract=binding.verification_contract,
    )
    journal = VerifierProposalJournal(app._event_store)
    evidence = coordinator._verifier.verify(
        task, app._artifact_store, grant_for_digest=lambda digest: app._artifact_grants.issue(
            digest=digest, run_id=request.run_id, expires_at=accepted.lease_expires_at,
        ),
    )
    proposal = journal.record(task, evidence) if record else None
    return SimpleNamespace(app=app, coordinator=coordinator, clock=clock, journal=journal, live=live,
        task=task, evidence=evidence, proposal=proposal, accepted=accepted, lifecycle=lifecycle,
        identity=dict(run_id=request.run_id, node_id=request.node_id,
                      attempt_id=request.attempt_id, fencing_generation=1))


@pytest.fixture
def prepared(tmp_path):
    value = prepare_application(tmp_path)
    yield value
    value.app.close()


def assert_success(value, returned):
    assert returned == value.proposal
    app = value.app
    state = value.lifecycle.replay("run-1")
    assert state.status == "succeeded"
    assert state.node("node-1").attempts[-1].status == "succeeded"
    assert app._scheduler.agents.replay("run-1").active_count == 0
    balance = BudgetLedger(app._event_store).available("run-1")
    assert balance.used_minor == 2
    assert balance.reserved_minor == 0
    released = app._event_store.read_stream("scheduler", "global")[-1]
    assert released.event_type == "AttemptSlotReleased"
    assert released.payload["task_sha256"] == value.proposal.task_sha256
    assert released.payload["evidence_sha256"] == value.proposal.evidence_sha256
    if value.live:
        completed_at = datetime.fromisoformat(released.payload["completed_at"])
        assert completed_at.utcoffset() == timedelta(0)
        assert value.clock.now <= completed_at <= datetime.now(timezone.utc)
    else:
        assert released.payload["completed_at"] == value.clock.now.isoformat()
    assert app.recover_run("run-1").active_attempts == ()


def test_acceptance_rechecks_artifact_and_atomically_binds_terminal_proof(prepared):
    returned = prepared.app.accept_verifier_proposal(
        **prepared.identity, task_sha256=prepared.proposal.task_sha256,
    )
    assert_success(prepared, returned)


@pytest.mark.parametrize("changes", [
    {"run_id": "other-run"}, {"node_id": "other-node"}, {"attempt_id": "other-attempt"},
    {"agent_instance_id": "other-agent"}, {"fencing_generation": 2}, {"graph_version": 2},
    *({field: "sha256:" + "a" * 64} for field in (
        "input_manifest_hash", "effective_config_hash", "registry_hash", "policy_manifest_hash",
        "routing_decision_hash", "planning_contract_hash")),
])
def test_acceptance_rejects_every_foreign_context_field(prepared, changes):
    context = prepared.task.context.model_copy(update=changes)
    task = prepared.task.model_copy(update={"context": context})
    evidence = prepared.evidence.model_copy(update={"context": context})
    proposal = prepared.journal.record(task, evidence)
    before = _resolver_durable_state(prepared.app._event_store)
    with pytest.raises(ProposalAcceptanceError):
        prepared.app.accept_verifier_proposal(**prepared.identity, task_sha256=proposal.task_sha256)
    assert _resolver_durable_state(prepared.app._event_store) == before


@pytest.mark.parametrize("fault", [
    "missing", "duplicate", "stale", "expired-before", "expired-after-verification",
    "expired-after-usage", "cancel-after-verification", "wrong-checks", "missing-grants",
    "missing-verifier", "missing-store", "missing-journal", "transport", "changed-evidence",
    "changed-evidence-hash", "missing-artifact", "corrupt-journal", "clock-backwards",
    "usage-unavailable", "usage-other-reservation", "usage-invalid", "clock-invalid",
])
def test_acceptance_failure_preserves_all_durable_streams(prepared, fault, monkeypatch):
    app, coordinator = prepared.app, prepared.coordinator
    identity = dict(prepared.identity)
    digest = prepared.proposal.task_sha256
    after_external_change = []
    if fault == "missing":
        digest = "sha256:" + "f" * 64
    elif fault == "duplicate":
        event = app._event_store.read_stream("verification_proposals", "run-1")[0]
        app._event_store.append("verification_proposals", "run-1", 1, [EventDraft(
            event.event_type, event.payload, run_id=event.run_id, node_id=event.node_id,
            attempt_id=event.attempt_id, fencing_generation=event.fencing_generation,
            correlation_id=event.correlation_id, causation_id=event.causation_id,
        )], "duplicate-proposal")
    elif fault == "stale":
        identity["fencing_generation"] = 2
    elif fault == "expired-before":
        prepared.clock.now = prepared.accepted.lease_expires_at
    elif fault in {"expired-after-verification", "cancel-after-verification", "changed-evidence",
                   "changed-evidence-hash", "clock-backwards"}:
        original = coordinator._verifier.verify
        def race(*args, **kwargs):
            evidence = original(*args, **kwargs)
            if fault == "cancel-after-verification":
                prepared.lifecycle.request_cancel("run-1", reason_code="user-request")
                after_external_change.append(_resolver_durable_state(app._event_store))
            elif fault == "expired-after-verification":
                prepared.clock.now = prepared.accepted.lease_expires_at
            elif fault == "clock-backwards":
                prepared.clock.now -= timedelta(microseconds=1)
            elif fault == "changed-evidence-hash":
                evidence = evidence.model_copy(update={"checks": tuple(reversed(evidence.checks))})
            else:
                evidence = evidence.model_copy(update={"verifier_id": "other-verifier"})
            return evidence
        monkeypatch.setattr(coordinator._verifier, "verify", race)
    elif fault == "wrong-checks":
        task = prepared.task.model_copy(update={"required_check_ids": ("artifact-integrity",)})
        evidence = prepared.evidence.model_copy(update={"checks": (prepared.evidence.checks[0],)})
        digest = prepared.journal.record(task, evidence).task_sha256
    elif fault in {"missing-grants", "missing-verifier", "missing-store", "missing-journal"}:
        setattr(coordinator, {"missing-grants": "_grants", "missing-verifier": "_verifier",
                              "missing-store": "_artifact_store", "missing-journal": "_journal"}[fault], None)
    elif fault == "corrupt-journal":
        app._event_store.append("verification_proposals", "run-1", 1,
            [EventDraft("MalformedProposal", {}, **identity, causation_id=digest)], "corrupt-journal")
    elif fault == "transport":
        coordinator._verifier._launcher.receipt = False
    elif fault == "missing-artifact":
        # An independently authorized inventory loss must not accept persisted evidence.
        monkeypatch.setattr(app._artifact_store, "verify_run_artifacts", lambda _run: ())
    elif fault == "clock-invalid":
        prepared.clock.now = prepared.clock.now.replace(tzinfo=None)
    else:
        def usage(snapshot):
            if fault == "usage-unavailable":
                raise RuntimeError("unavailable")
            if fault == "expired-after-usage":
                prepared.clock.now = prepared.accepted.lease_expires_at
            result = TrustedUsage().usage_for(snapshot)
            if fault == "usage-other-reservation":
                return result.model_copy(update={"reservation_id": "other-reservation"})
            if fault == "usage-invalid":
                return result.model_copy(update={"cost_minor": -1})
            return result
        monkeypatch.setattr(coordinator._usage_source, "usage_for", usage)
    before = _resolver_durable_state(app._event_store)
    balance = BudgetLedger(app._event_store).available("run-1")
    with pytest.raises(ProposalAcceptanceError) as error:
        app.accept_verifier_proposal(**identity, task_sha256=digest)
    assert error.value.code == "proposal_acceptance_rejected"
    assert _resolver_durable_state(app._event_store) == (after_external_change[0] if after_external_change else before)
    assert BudgetLedger(app._event_store).available("run-1") == balance
    assert prepared.lifecycle.replay("run-1").node("node-1").status != "succeeded"
    assert app._scheduler.agents.replay("run-1").active_count == 1


@pytest.mark.parametrize("outcome", ["rejected", "inconclusive"])
def test_nonaccepted_proposals_never_release_resources(prepared, outcome):
    evidence = prepared.evidence.model_copy(update={"outcome": outcome,
        "checks": tuple(check.model_copy(update={"passed": False}) for check in prepared.evidence.checks)})
    # A distinct task remains valid for the same admission, but carries no accepted proof.
    task = prepared.task.model_copy(update={"required_check_ids": tuple(reversed(prepared.task.required_check_ids))})
    proposal = prepared.journal.record(task, evidence)
    before = _resolver_durable_state(prepared.app._event_store)
    with pytest.raises(ProposalAcceptanceError):
        prepared.app.accept_verifier_proposal(**prepared.identity, task_sha256=proposal.task_sha256)
    assert _resolver_durable_state(prepared.app._event_store) == before


def test_application_default_usage_source_fails_closed(tmp_path):
    value = prepare_application(tmp_path, usage_source=False)
    try:
        before = _resolver_durable_state(value.app._event_store)
        with pytest.raises(ProposalAcceptanceError):
            value.app.accept_verifier_proposal(**value.identity, task_sha256=value.proposal.task_sha256)
        assert _resolver_durable_state(value.app._event_store) == before
    finally:
        value.app.close()


def test_application_without_artifact_store_fails_closed(tmp_path):
    with ControlPlaneApplication(tmp_path / "empty.db", limits=_application_limits()) as app:
        before = _resolver_durable_state(app._event_store)
        with pytest.raises(ProposalAcceptanceError):
            app.accept_verifier_proposal(run_id="run-1", node_id="node-1", attempt_id="attempt-1",
                fencing_generation=1, task_sha256="sha256:" + "a" * 64)
        assert _resolver_durable_state(app._event_store) == before


def test_application_acceptance_is_identity_only_and_requires_readiness(prepared):
    assert set(inspect.signature(ControlPlaneApplication.accept_verifier_proposal).parameters) == {
        "self", "run_id", "node_id", "attempt_id", "fencing_generation", "task_sha256"}
    for forbidden in ("usage", "result", "evidence", "completed_at", "path", "known_no_effect"):
        with pytest.raises(TypeError):
            prepared.app.accept_verifier_proposal(**prepared.identity,
                task_sha256=prepared.proposal.task_sha256, **{forbidden: object()})
    prepared.app.close()
    with pytest.raises(ApplicationNotReady):
        prepared.app.accept_verifier_proposal(**prepared.identity, task_sha256=prepared.proposal.task_sha256)


def test_acceptance_requires_a_digest_string_not_an_equality_impostor(prepared):
    class Impostor:
        def __eq__(self, other):
            return True

    before = _resolver_durable_state(prepared.app._event_store)
    with pytest.raises(ProposalAcceptanceError):
        prepared.app.accept_verifier_proposal(**prepared.identity, task_sha256=Impostor())
    assert _resolver_durable_state(prepared.app._event_store) == before


@pytest.mark.parametrize("with_artifacts", [True, False])
@pytest.mark.parametrize("service", [lambda snapshot: None, UsageRecord(
    run_id="run-1", reservation_id="reservation-1", settlement_key="usage",
    currency="USD", cost_minor=0)])
def test_application_rejects_records_and_callbacks_as_usage_services(tmp_path, with_artifacts, service):
    with pytest.raises(StartupRecoveryFailed):
        ControlPlaneApplication(tmp_path / "invalid-service.db", limits=_application_limits(),
            artifact_root=tmp_path / "artifacts" if with_artifacts else None, usage_source=service)
