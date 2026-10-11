"""Admission must derive exact inputs while preserving Scheduler authority."""

from datetime import timedelta
import hashlib
import inspect
import json
import traceback

import pytest

from orchestrator.application import ApplicationNotReady, ControlPlaneApplication
from orchestrator.budget import BudgetLedger
from orchestrator.lifecycle.models import NodeSpec
from orchestrator.runtime.worker_input import WorkerInputError
from tests.support.worker_input import admit_worker_input_case, make_worker_input_case
from tests.unit.lifecycle.test_scheduler import (
    NOW, _application_limits, _resolver_durable_state, routed_pair, run_setup,
)


@pytest.fixture
def case(tmp_path):
    value = make_worker_input_case(tmp_path / "input-events.db")
    try:
        yield value
    finally:
        value.app.close()


def durable_state(app):
    """Include events, snapshots, budget, Agents and scheduler slots."""
    return _resolver_durable_state(app._event_store), tuple(app._event_store._connection.iterdump())


def binding(case):
    return case.app._scheduler.resolve_verification_binding(**case.identity, as_of=case.clock.now)


def test_admission_derives_canonical_node_input_and_fixed_checks(case):
    admit_worker_input_case(case)
    value = binding(case)
    # Independently derive the wire frame, rather than reuse the production builder.
    frame = json.dumps({
        "schema": "maestro.worker-input.v1", "run_id": "run-1", "node_id": "node-1",
        "role": "coder", "task_text": "implement a small change", "input_artifacts": [],
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    assert value.context.input_manifest_hash == "sha256:" + hashlib.sha256(
        b"maestro.worker-input.v1\x00" + frame).hexdigest()
    assert value.verification_contract == "maestro.artifact-verification/v1"
    assert value.required_check_ids == ("artifact-integrity",)
    assert case.app._scheduler.lifecycle.replay("run-1").node("node-1").spec.role == "coder"
    assert BudgetLedger(case.app._event_store).available("run-1").reserved_minor == 20
    assert case.app._scheduler.agents.replay("run-1").active_count == 1
    assert len(case.app._scheduler.event_store.read_stream("scheduler", "global")) == 1


def test_exact_retry_is_noop_and_conflicting_text_cannot_retrofit(case):
    original = admit_worker_input_case(case)
    before = durable_state(case.app)
    assert admit_worker_input_case(case) == original
    assert durable_state(case.app) == before
    case.text += " "
    with pytest.raises(WorkerInputError):
        admit_worker_input_case(case)
    assert durable_state(case.app) == before


def test_admission_uses_actual_durable_role_and_identity(tmp_path):
    with ControlPlaneApplication(tmp_path / "reviewer.db", limits=_application_limits()) as app:
        reg, config, lifecycle, policy = run_setup(
            app._event_store, run_id="run-other", freeze_contracts=True,
            nodes=(NodeSpec(node_id="review-node", role="reviewer",
                            planning_contract_hash="sha256:" + "0" * 64),),
        )
        node = lifecycle.replay("run-other").node("review-node")
        request, decision = routed_pair(
            reg, config, policy, run_id="run-other", node_id="review-node", role="reviewer",
            contract_hash=node.spec.planning_contract_hash,
        )
        app.accept_routing_with_input(request, decision, task_text=" review\r\né ",
            accepted_at=NOW + timedelta(seconds=1), lease_expires_at=NOW + timedelta(minutes=1))
        value = app._scheduler.resolve_verification_binding(
            run_id="run-other", node_id="review-node", attempt_id=request.attempt_id,
            fencing_generation=1, as_of=NOW + timedelta(seconds=2),
        )
        frame = json.dumps({
            "schema": "maestro.worker-input.v1", "run_id": "run-other", "node_id": "review-node",
            "role": "reviewer", "task_text": " review\r\né ", "input_artifacts": [],
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        assert value.context.input_manifest_hash == "sha256:" + hashlib.sha256(
            b"maestro.worker-input.v1\x00" + frame).hexdigest()


def test_closed_application_gates_input_admission(case):
    case.app.close()
    with pytest.raises(ApplicationNotReady):
        admit_worker_input_case(case)


@pytest.mark.parametrize("override", [
    "input_manifest_hash", "role", "required_check_ids", "verification_contract",
    "context", "workspace", "path", "usage", "result", "input_artifacts", "clock",
])
def test_public_admission_rejects_authority_overrides(case, override):
    assert set(inspect.signature(case.app.accept_routing_with_input).parameters) == {
        "request", "decision", "task_text", "accepted_at", "lease_expires_at",
    }
    before = durable_state(case.app)
    with pytest.raises(TypeError):
        case.app.accept_routing_with_input(
            case.request, case.decision, task_text=case.text,
            accepted_at=NOW + timedelta(seconds=1), lease_expires_at=NOW + timedelta(minutes=1),
            **{override: "caller-override"},
        )
    assert durable_state(case.app) == before


@pytest.mark.parametrize("text", ["", " \n", 7, "x" * 100_001, "\ud800"],
                         ids=["empty", "blank", "wrong-type", "too-long", "surrogate"])
def test_invalid_text_fails_without_persistent_resource_writes(case, text):
    before = durable_state(case.app)
    case.text = text
    with pytest.raises(WorkerInputError):
        admit_worker_input_case(case)
    assert durable_state(case.app) == before


@pytest.mark.parametrize("kind", [
    "request-type", "request-unsafe", "decision-unsafe", "nested-unsafe",
    "missing-node", "planning-mismatch", "role-mismatch", "config-mismatch",
])
def test_invalid_routing_authority_fails_atomically(case, kind):
    if kind == "request-type":
        case.request = {}
    elif kind == "request-unsafe":
        case.request = case.request.model_copy(update={"fencing_generation": True})
    elif kind == "decision-unsafe":
        case.decision = case.decision.model_copy(update={"outcome": "invented"})
    elif kind == "nested-unsafe":
        item = case.decision.candidate_assessments[0].model_copy(update={"eligible": "yes"})
        case.decision = case.decision.model_copy(update={"candidate_assessments": (item,)})
    elif kind == "missing-node":
        case.request = case.request.model_copy(update={"node_id": "missing"})
    elif kind == "planning-mismatch":
        case.request = case.request.model_copy(update={"planning_contract_hash": "sha256:" + "0" * 64})
    elif kind == "role-mismatch":
        assessment = case.decision.candidate_assessments[0]
        policy = assessment.policy_decision
        policy = policy.model_copy(update={"request": policy.request.model_copy(update={"role": "planner"})})
        case.decision = case.decision.model_copy(update={
            "candidate_assessments": (assessment.model_copy(update={"policy_decision": policy}),),
        })
    else:
        case.decision = case.decision.model_copy(update={"config_hash": "sha256:" + "0" * 64})
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError):
        admit_worker_input_case(case)
    assert durable_state(case.app) == before


def test_legacy_node_without_full_planning_contract_is_rejected(tmp_path):
    with ControlPlaneApplication(tmp_path / "legacy.db", limits=_application_limits()) as app:
        reg, config, _, policy = run_setup(app._event_store)
        request, decision = routed_pair(reg, config, policy)
        before = durable_state(app)
        with pytest.raises(WorkerInputError):
            app.accept_routing_with_input(request, decision, task_text="change",
                accepted_at=NOW + timedelta(seconds=1), lease_expires_at=NOW + timedelta(minutes=1))
        assert durable_state(app) == before


@pytest.mark.parametrize("accepted_at, expires_at", [
    (NOW.replace(tzinfo=None), NOW + timedelta(minutes=1)),
    (NOW + timedelta(seconds=1), NOW.replace(tzinfo=None)),
    (NOW - timedelta(seconds=1), NOW + timedelta(minutes=1)),
    (NOW + timedelta(seconds=1), NOW + timedelta(seconds=1)),
    ("invalid", NOW + timedelta(minutes=1)),
])
def test_invalid_times_do_not_write(case, accepted_at, expires_at):
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError):
        case.app.accept_routing_with_input(case.request, case.decision, task_text=case.text,
                                          accepted_at=accepted_at, lease_expires_at=expires_at)
    assert durable_state(case.app) == before


def test_nested_admission_failure_rolls_back_real_writes_after_reopen(case, monkeypatch, tmp_path):
    before = durable_state(case.app)
    real = case.app._scheduler.accept_routing

    def fail_after_real(*args, **kwargs):
        real(*args, **kwargs)
        raise RuntimeError("private-error-sentinel-991")

    monkeypatch.setattr(case.app._scheduler, "accept_routing", fail_after_real)
    with pytest.raises(WorkerInputError) as caught:
        admit_worker_input_case(case)
    assert "private-error-sentinel-991" not in "".join(traceback.format_exception(caught.value))
    case.app.close()
    with ControlPlaneApplication(tmp_path / "input-events.db", limits=_application_limits()) as reopened:
        assert durable_state(reopened) == before


def test_task_text_absent_from_durable_data_logs_and_errors(case, caplog):
    sentinel = "private-task-sentinel-d751a2"
    case.text = sentinel
    admit_worker_input_case(case)
    case.text += " mismatch"
    with pytest.raises(WorkerInputError) as caught:
        admit_worker_input_case(case)
    assert sentinel not in repr(durable_state(case.app))
    assert sentinel not in caplog.text
    assert sentinel not in "".join(traceback.format_exception(caught.value))
