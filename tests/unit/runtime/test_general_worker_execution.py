"""Normal host mediation with offline Provider/usage and real isolated children."""

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.config import ConfigCandidate, ConfigManager
from orchestrator.lifecycle import LifecycleController, NodeSpec
from orchestrator.models import ProviderModelGateway
from orchestrator.models.provider_sender import ProviderSenderTerminationReceipt
from orchestrator.models.transport import HTTPTransportResponse
from orchestrator.routing import compile_node_contract
from orchestrator.runtime.general_worker_authority import SchedulerProviderDispatchAuthority
from orchestrator.runtime.general_worker_journal import GeneralWorkerIdentity, GeneralWorkerJournal
from orchestrator.runtime.general_worker_process import GeneralWorkerProcess
from orchestrator.runtime.verification_journal import VerifierProposalJournal
from orchestrator.security import PolicyAuthority, PolicyManifest
from tests.unit.lifecycle.test_scheduler import registry, effective_config, routed_pair, _application_limits
from tests.unit.runtime.test_attempt_execution_coordinator import TrustedUsage
from tests.unit.runtime.test_general_worker_authority import OfflineBroker


class OfflineSuccessTransport:
    """No network: returns synthetic Provider bytes and a labeled stop fixture."""
    def __init__(self, *, output="candidate café", status="completed", usage=True, stopped=True, tools=False):
        self.output, self.status = output, status
        self.usage, self.stopped, self.tools = usage, stopped, tools
        self.calls = []

    async def post_json(self, **kwargs):
        call = kwargs["call_binding"]
        self.calls.append(kwargs)
        receipt = ProviderSenderTerminationReceipt(provider_call_stream_id=call.stream_id,
            **{name: getattr(call, name) for name in ("run_id", "node_id", "attempt_id", "fencing_generation",
                "accepted_route_id", "budget_reservation_id", "provider_id", "model_id", "registry_manifest_hash")},
            request_hash=call.request_hash, unit_name="maestro-provider-" + "b" * 32 + ".service",
            cgroup_path_hash="sha256:" + "b" * 64, active_state="inactive", cgroup_empty=True,
            observed_at=datetime.now(timezone.utc)) if self.stopped else None
        output = ([{"type": "function_call", "call_id": "tool-1", "name": "shell", "arguments": "{}"}]
            if self.tools else [{"type": "message", "content": [{"type": "output_text", "text": self.output}]}])
        body = {"id": "offline-response", "status": self.status, "output": output}
        if self.usage:
            body["usage"] = {"input_tokens": 1, "output_tokens": 2}
        return HTTPTransportResponse(200, (("request-id", "offline-response"),),
            json.dumps(body).encode(), receipt)


def execution_case(tmp_path, *, transport=None, trusted_usage=True):
    from orchestrator.runtime.general_worker_execution import GeneralWorkerExecutionService
    private = tmp_path / "private"
    private.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "task.txt").write_text("read-only workspace")
    app = ControlPlaneApplication(private / "events.db", limits=_application_limits(),
        artifact_root=private / "artifacts", usage_source=TrustedUsage() if trusted_usage else None)
    reg = registry()
    resolved = effective_config(reg)
    policy = PolicyManifest(authorities=(PolicyAuthority(source="system", max_permission="read-only",
        allowed_actions=("model_invoke",), allowed_tools=("model:model-1",)),))
    text = "private exact task café"
    contract = compile_node_contract(run_id="run-1", node_id="node-1", role="coder", task_text=text,
        config=resolved.config, registry=reg, policy_manifest=policy, context_tokens=1000,
        max_output_tokens=500, required_capabilities=("text",))
    ConfigManager(ConfigCandidate(resolved=resolved, registry=reg)).start_run("run-1", app._event_store, workspace=workspace)
    lifecycle = LifecycleController(app._event_store)
    lifecycle.initialize_run("run-1")
    lifecycle.append_nodes("run-1", (NodeSpec(node_id="node-1", role="coder", planning_contract=contract,
        planning_contract_hash=contract.contract_hash, max_attempts=3),), expected_graph_version=0,
        idempotency_key="graph", policy_manifest=policy)
    lifecycle.start_run("run-1")
    routing, decision = routed_pair(reg, resolved.config, policy, contract_hash=contract.contract_hash)
    assessment, = decision.candidate_assessments
    decision = decision.model_copy(update={"candidate_assessments": (assessment.model_copy(update={
        "estimated_cost": assessment.estimated_cost.model_copy(update={"price_snapshot_id": reg.content_hash})}),)})
    now = datetime.now(timezone.utc)
    app._worker_inputs.accept_routing_with_input(routing, decision, task_text=text,
        accepted_at=now-timedelta(seconds=1), lease_expires_at=now+timedelta(minutes=5))
    journal = GeneralWorkerJournal(app._event_store)
    authority = SchedulerProviderDispatchAuthority(app._scheduler, journal)
    broker, transport = OfflineBroker(), transport or OfflineSuccessTransport()
    gateway = ProviderModelGateway(registry=reg, secret_broker=broker, transport=transport,
        provider_call_journal=authority.provider_journal, dispatch_authority=authority)
    worker = GeneralWorkerProcess()
    service = GeneralWorkerExecutionService(app._scheduler, app._worker_inputs, app._artifact_store,
        app._artifact_grants, VerifierProposalJournal(app._event_store), app._acceptance_coordinator,
        gateway, worker, workspace)
    return SimpleNamespace(app=app, service=service, journal=journal, broker=broker, transport=transport,
        text=text, contract=contract, identity=GeneralWorkerIdentity("run-1", "node-1", routing.attempt_id, 1))


@pytest.fixture
def case(tmp_path):
    value = execution_case(tmp_path)
    try:
        yield value
    finally:
        value.app.close()


def test_real_candidate_bytes_flow_through_independent_acceptance(case):
    result = asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    operation = case.journal.read(case.identity)
    assert [turn.phase for turn in operation.turns] == ["request", "complete"]
    assert all(turn.launch_receipt and turn.termination_receipt.cgroup_empty for turn in operation.turns)
    artifact, = result.artifacts
    grant = case.app._artifact_grants.issue(digest=artifact.digest, run_id="run-1",
        expires_at=datetime.now(timezone.utc)+timedelta(minutes=1))
    assert case.app._artifact_store.read_bytes(artifact.digest, grant=grant) == "candidate café".encode()
    assert artifact.digest == "sha256:" + hashlib.sha256("candidate café".encode()).hexdigest()
    assert len(case.transport.calls) == case.broker.calls == 1
    dumps = "\n".join(case.app._event_store._connection.iterdump())
    assert case.text not in dumps and "candidate café" not in dumps and "offline-secret" not in dumps
    # Canonical FULL response checkpoint, default null/empty fields included.
    capture = operation.capture.response_artifact
    grant = case.app._artifact_grants.issue(digest=capture.digest, run_id=operation.operation_id,
        expires_at=datetime.now(timezone.utc)+timedelta(minutes=1))
    checkpoint = case.app._artifact_store.read_bytes(capture.digest, grant=grant)
    response = json.loads(checkpoint)
    assert response["tool_calls"] == [] and response["usage"]["reasoning_tokens"] is None
    assert checkpoint == json.dumps(response, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def test_host_derives_frozen_request_not_worker_model_or_route(case):
    result = asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    body = json.loads(case.transport.calls[0]["body"])
    assert body["model"] == "remote-model-1"
    assert body["max_output_tokens"] == 500
    assert case.text in json.dumps(body, ensure_ascii=False)
    assert not body.get("tools")


@pytest.mark.parametrize("fault", ["usage", "stop"])
def test_gateway_capture_requires_known_success_usage_and_sender_stop(tmp_path, fault):
    value = execution_case(tmp_path, transport=OfflineSuccessTransport(usage=fault != "usage", stopped=fault != "stop"))
    try:
        result = asyncio.run(value.service.execute(identity=value.identity, task_text=value.text))
        assert result.status == "blocked"
        assert value.journal.read(value.identity).capture is None
        assert len(value.transport.calls) == 1
    finally:
        value.app.close()


@pytest.mark.parametrize("changes", [dict(tools=True), dict(status="incomplete"), dict(output=""), dict(output="x"*16385)])
def test_tool_nonterminal_empty_or_oversized_response_blocks(tmp_path, changes):
    value = execution_case(tmp_path, transport=OfflineSuccessTransport(**changes))
    try:
        assert asyncio.run(value.service.execute(identity=value.identity, task_text=value.text)).status == "blocked"
        assert value.journal.read(value.identity).capture is None
    finally:
        value.app.close()


def test_unavailable_usage_cannot_succeed(tmp_path):
    value = execution_case(tmp_path, trusted_usage=False)
    try:
        assert asyncio.run(value.service.execute(identity=value.identity, task_text=value.text)).status == "blocked"
        assert value.journal.read(value.identity).accepted_result is None
    finally:
        value.app.close()


def test_requested_registration_rolls_back_if_legacy_provider_intent_already_holds(case):
    task = case.app._worker_inputs._prepare_general_worker_task(
        **vars_identity(case.identity), task_text=case.text)
    binding = case.app._scheduler._resolve_general_verification_binding(
        **vars_identity(case.identity), as_of=datetime.now(timezone.utc))
    legacy = SimpleNamespace(identity=case.identity, request_id="legacy-request", idempotency_key="legacy-call",
        context=task.context)
    request = case.service._model_request(legacy, task, binding)
    case.service._authority.provider_journal.record_intent(request, provider_id="primary",
        provider_adapter="openai_responses", request_body=b"legacy-body", provider_correlation_id="legacy")
    result = asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    assert result.status == "blocked"
    assert case.journal.read(case.identity) is None
    assert case.broker.calls == 0 and case.transport.calls == []


def vars_identity(identity):
    return {name: getattr(identity, name) for name in ("run_id", "node_id", "attempt_id", "fencing_generation")}
