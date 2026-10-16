"""Admission must derive exact inputs while preserving Scheduler authority."""

from datetime import timedelta, timezone
import hashlib
import inspect
import json
import traceback

import pytest

from orchestrator.application import ApplicationNotReady, ControlPlaneApplication
from orchestrator.budget import BudgetLedger
from orchestrator.lifecycle.models import NodeSpec
from orchestrator.runtime.worker_input import WorkerInputError
from tests.support.worker_input import (
    admit_worker_input_case, corrupt_worker_input_event, make_worker_input_case,
    prepare_worker_input_case,
)
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
        before = durable_state(app)
        app._worker_inputs._clock = lambda: NOW + timedelta(seconds=2)
        task = app.prepare_worker_task(run_id="run-other", node_id="review-node",
            attempt_id=request.attempt_id, fencing_generation=1, task_text=" review\r\né ")
        assert task.role == "reviewer" and task.context == value.context
        assert task.task_text == " review\r\né "
        assert durable_state(app) == before


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


@pytest.mark.parametrize("invalidity", ["surrogate", "validation"])
def test_public_admission_detaches_raw_input_exception_objects(case, invalidity):
    from tests.unit.runtime.test_worker_input import _SENTINEL, _assert_safe_error_object

    case.text = _SENTINEL + "\ud800"
    if invalidity == "validation":
        case.text += "x" * 100_001
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError) as caught:
        admit_worker_input_case(case)
    _assert_safe_error_object(caught.value, "worker_input_invalid")
    assert durable_state(case.app) == before


@pytest.mark.parametrize("fault", ["opaque-code", "code-object", "raw-args", "known-code-cause"])
def test_public_admission_detaches_nested_errors_and_rejects_opaque_codes(case, monkeypatch, fault):
    from tests.unit.runtime.test_worker_input import _SENTINEL, _assert_safe_error_object

    before = durable_state(case.app)
    real = case.app._scheduler.accept_routing

    def fail_after_real(*args, **kwargs):
        real(*args, **kwargs)
        if fault == "known-code-cause":
            raise WorkerInputError("worker_input_too_large") from RuntimeError(_SENTINEL)
        if fault == "raw-args":
            raise RuntimeError(_SENTINEL)
        raise WorkerInputError(_SENTINEL if fault == "opaque-code" else {_SENTINEL: "private"})

    monkeypatch.setattr(case.app._scheduler, "accept_routing", fail_after_real)
    with pytest.raises(WorkerInputError) as caught:
        admit_worker_input_case(case)
    _assert_safe_error_object(caught.value, (
        "worker_input_too_large" if fault == "known-code-cause" else "worker_input_admission_failed"))
    assert durable_state(case.app) == before


def test_preparation_uses_exact_frozen_binding_without_effects(case, monkeypatch):
    from orchestrator.artifacts import ArtifactStore
    from orchestrator.models.gateway import ModelGateway
    from orchestrator.runtime.worker_process import IsolatedWorkerProcess
    from orchestrator.secrets.broker import AuditedSecretBroker
    from orchestrator.tools.gateway import ToolGateway
    from orchestrator.runtime.acceptance_coordinator import AttemptExecutionCoordinator

    case.text = "private-prepared-task-sentinel-238c\r\né"
    admit_worker_input_case(case)
    before = durable_state(case.app)

    def forbidden(*args, **kwargs):
        pytest.fail("preparation invoked an execution or publication effect")

    for target, method in (
        (IsolatedWorkerProcess, "execute"), (ModelGateway, "invoke"),
        (ToolGateway, "execute"), (ArtifactStore, "publish_bytes"),
        (AuditedSecretBroker, "acquire_provider_credential_decision"),
        (AttemptExecutionCoordinator, "accept_proposal"),
    ):
        monkeypatch.setattr(target, method, forbidden)
    task = prepare_worker_input_case(case)
    assert task.context == binding(case).context
    assert task.role == "coder" and task.task_text == case.text
    assert task.input_artifacts == task.tool_capabilities == ()
    assert task.output_byte_limit == 16_384
    assert "private-prepared-task-sentinel-238c" not in repr(task)
    assert task.model_dump(mode="json")["task_text"] == case.text
    assert durable_state(case.app) == before
    assert "private-prepared-task-sentinel-238c" not in repr(before)


def test_preparation_requires_text_each_time_and_rejects_public_overrides(case):
    admit_worker_input_case(case)
    prepare_worker_input_case(case)
    before = durable_state(case.app)
    assert set(inspect.signature(case.app.prepare_worker_task).parameters) == {
        "run_id", "node_id", "attempt_id", "fencing_generation", "task_text",
    }
    with pytest.raises(TypeError):
        case.app.prepare_worker_task(**case.identity)
    for override in ("as_of", "clock", "role", "context", "output_byte_limit", "workspace",
                     "input_manifest_hash", "required_check_ids", "permission", "tool_capabilities"):
        with pytest.raises(TypeError):
            case.app.prepare_worker_task(**case.identity, task_text=case.text,
                                         **{override: "caller-override"})
    assert durable_state(case.app) == before


@pytest.mark.parametrize("change", [
    {"run_id": "other-run"}, {"node_id": "other-node"}, {"attempt_id": "other-attempt"},
    {"fencing_generation": 2}, {"fencing_generation": True}, {"fencing_generation": 0},
    {"fencing_generation": "1"}, {"run_id": "invalid space"}, {"node_id": "é"},
    {"attempt_id": "a" * 129}, {"run_id": 1}, {"node_id": "node-1\n"},
])
def test_preparation_rejects_cross_identity_and_strict_invalid_identity(case, change):
    admit_worker_input_case(case)
    case.identity.update(change)
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


@pytest.mark.parametrize("text", ["changed", "implement a small change ", "", " \n", 7,
                                 "x" * 100_001, "\ud800"],
                         ids=["changed", "whitespace", "empty", "blank", "type", "too-long", "surrogate"])
def test_preparation_rejects_mismatched_and_invalid_text_atomically(case, text):
    admit_worker_input_case(case)
    before = durable_state(case.app)
    case.text = text
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


def test_preparation_does_not_normalize_unicode(case):
    case.text = "é"
    admit_worker_input_case(case)
    assert prepare_worker_input_case(case).task_text == "é"
    before = durable_state(case.app)
    case.text = "e\u0301"
    with pytest.raises(WorkerInputError, match="worker_input_mismatch"):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


@pytest.mark.parametrize("now", [None, "invalid", NOW.replace(tzinfo=None),
                                 NOW.astimezone(timezone(timedelta(hours=1))),
                                 NOW + timedelta(minutes=1)])
def test_preparation_rejects_invalid_clock_or_exact_expiry(case, now):
    admit_worker_input_case(case)
    before = durable_state(case.app)
    case.clock.now = now
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


def test_closed_application_gates_preparation(case):
    admit_worker_input_case(case)
    case.app.close()
    with pytest.raises(ApplicationNotReady):
        prepare_worker_input_case(case)


@pytest.mark.parametrize("legacy", ["absent", "arbitrary"])
def test_preparation_cannot_retrofit_legacy_hash_admission(case, legacy):
    kwargs = {} if legacy == "absent" else dict(
        input_manifest_hash="sha256:" + "0" * 64,
        verification_contract="maestro.artifact-verification/v1",
        required_check_ids=("artifact-integrity",),
    )
    case.app.accept_routing(case.request, case.decision, accepted_at=NOW + timedelta(seconds=1),
                            lease_expires_at=NOW + timedelta(minutes=1), **kwargs)
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


@pytest.mark.parametrize("field", ["run_id", "node_id", "attempt_id", "agent_instance_id",
                                  "fencing_generation", "graph_version", "input_manifest_hash",
                                  "effective_config_hash", "registry_hash", "policy_manifest_hash",
                                  "routing_decision_hash", "planning_contract_hash", "missing"])
def test_preparation_rejects_corrupt_frozen_context(case, field):
    admit_worker_input_case(case)

    def corrupt(payload):
        if field == "missing":
            payload.pop("verification_context")
        else:
            payload["verification_context"][field] = (
                2 if field in {"fencing_generation", "graph_version"} else
                "sha256:" + "0" * 64 if field.endswith("hash") else "other")

    corrupt_worker_input_event(case.app._event_store, ("scheduler", "global"), corrupt)
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


@pytest.mark.parametrize("fault", ["missing-contract", "malformed-contract", "role", "graph", "cancel"])
def test_preparation_rejects_changed_graph_role_and_planning_authority(case, fault):
    admit_worker_input_case(case)
    if fault == "cancel":
        case.lifecycle.request_cancel("run-1", reason_code="user_cancelled")
    elif fault == "graph":
        case.lifecycle.append_nodes("run-1", (NodeSpec(
            node_id="extra", role="reviewer", planning_contract_hash="sha256:" + "0" * 64),),
            expected_graph_version=1, idempotency_key="changed-graph")
    else:
        event = next(e for e in case.app._event_store.read_stream("run_lifecycle", "run-1")
                     if "nodes" in e.payload)
        # Mutate the graph event, not a fake authority resolver.
        def corrupt(payload):
            node = payload["nodes"][0]
            if fault == "missing-contract":
                node["planning_contract"] = None
            elif fault == "malformed-contract":
                node["planning_contract"] = {"invalid": True}
            else:
                node["role"] = "reviewer"
        # The graph append precedes RunStarted, so target its own event explicitly.
        payload = json.loads(json.dumps(event.payload))
        corrupt(payload)
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        store = case.app._event_store
        store._connection.execute("DROP TRIGGER events_immutable_update")
        store._connection.execute("UPDATE events SET payload_json=?, payload_hash=? WHERE event_id=?",
                                 (encoded, hashlib.sha256(encoded.encode()).hexdigest(), event.event_id))
        store._connection.execute(
            "DELETE FROM snapshots WHERE aggregate_type='run_lifecycle' AND aggregate_id='run-1'")
    before = durable_state(case.app)
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


def test_preparation_bounds_actual_serialized_task(case, monkeypatch):
    import orchestrator.runtime.worker_input_service as service
    admit_worker_input_case(case)
    before = durable_state(case.app)
    monkeypatch.setattr(service, "MAX_WORKER_INPUT_FRAME_BYTES", 1)
    with pytest.raises(WorkerInputError, match="worker_input_too_large"):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


@pytest.mark.parametrize("invalidity", ["surrogate", "validation", "mismatch"])
def test_preparation_detaches_raw_text_errors_and_keeps_durable_privacy(case, invalidity, caplog):
    from tests.unit.runtime.test_worker_input import _SENTINEL, _assert_safe_error_object
    case.text = _SENTINEL
    admit_worker_input_case(case)
    before = durable_state(case.app)
    case.text += "\ud800" if invalidity == "surrogate" else (
        "x" * 100_001 if invalidity == "validation" else " mismatch")
    with pytest.raises(WorkerInputError) as caught:
        prepare_worker_input_case(case)
    _assert_safe_error_object(caught.value, "worker_input_mismatch" if invalidity == "mismatch"
                              else "worker_input_invalid")
    assert durable_state(case.app) == before
    assert _SENTINEL not in repr(before) + caplog.text


@pytest.mark.parametrize("fault", ["opaque-code", "code-object", "raw-args", "known-code-cause"])
def test_preparation_detaches_injected_binding_failure_after_read(case, monkeypatch, fault, tmp_path):
    from tests.unit.runtime.test_worker_input import _SENTINEL, _assert_safe_error_object
    admit_worker_input_case(case)
    before = durable_state(case.app)
    real = case.app._scheduler.resolve_verification_binding

    def fail_after_read(**kwargs):
        real(**kwargs)
        if fault == "known-code-cause":
            raise WorkerInputError("worker_input_too_large") from RuntimeError(_SENTINEL)
        if fault == "raw-args":
            raise RuntimeError(_SENTINEL)
        raise WorkerInputError(_SENTINEL if fault == "opaque-code" else {_SENTINEL: "private"})

    monkeypatch.setattr(case.app._scheduler, "resolve_verification_binding", fail_after_read)
    with pytest.raises(WorkerInputError) as caught:
        prepare_worker_input_case(case)
    _assert_safe_error_object(caught.value, "worker_input_too_large" if fault == "known-code-cause"
                              else "worker_input_preparation_failed")
    assert durable_state(case.app) == before
    case.app.close()
    with ControlPlaneApplication(tmp_path / "input-events.db", limits=_application_limits()) as app:
        assert durable_state(app) == before


def test_preparation_rejects_context_agent_not_bound_to_active_attempt(case, monkeypatch):
    admit_worker_input_case(case)
    before = durable_state(case.app)
    real = case.app._scheduler.resolve_verification_binding

    def swapped_agent(**kwargs):
        value = real(**kwargs)
        return value.model_copy(update={
            "context": value.context.model_copy(update={"agent_instance_id": "other-agent"})})

    monkeypatch.setattr(case.app._scheduler, "resolve_verification_binding", swapped_agent)
    with pytest.raises(WorkerInputError):
        prepare_worker_input_case(case)
    assert durable_state(case.app) == before


def test_preparation_validates_identity_before_any_durable_read(case, monkeypatch):
    admit_worker_input_case(case)
    before = durable_state(case.app)
    reads = []
    real = case.app._event_store.append_checked

    def record_read(*args, **kwargs):
        reads.append(args[2])
        return real(*args, **kwargs)

    monkeypatch.setattr(case.app._event_store, "append_checked", record_read)
    case.identity["fencing_generation"] = True
    with pytest.raises(WorkerInputError, match="worker_input_invalid"):
        prepare_worker_input_case(case)
    assert reads == []
    assert durable_state(case.app) == before
