"""Explicit durable phase recovery; Provider/usage are labeled offline fixtures.

In-process barriers simulate lost host replies. The full independent-process
SIGKILL matrix belongs to Task 4 and is not claimed by these tests.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.models import ProviderModelGateway
from orchestrator.models.transport import HTTPTransportFailed
from orchestrator.isolation import IsolationStopUnknown
from orchestrator.runtime.general_worker_authority import SchedulerProviderDispatchAuthority, general_worker_host_scope
from orchestrator.runtime.general_worker_execution import GeneralWorkerExecutionService
from orchestrator.runtime.general_worker_journal import GeneralWorkerJournal
from orchestrator.runtime.general_worker_process import GeneralWorkerProcess, GeneralWorkerSession
from orchestrator.runtime.verification_journal import VerifierProposalJournal
from tests.unit.lifecycle.test_scheduler import _application_limits, _resolver_durable_state
from tests.unit.runtime.test_attempt_execution_coordinator import TrustedUsage
from tests.unit.runtime.test_general_worker_authority import OfflineBroker
from tests.unit.runtime.test_general_worker_execution import execution_case, OfflineSuccessTransport
from tests.support.worker_input import corrupt_worker_input_event


@pytest.fixture
def case(tmp_path):
    value = execution_case(tmp_path)
    try:
        yield value
    finally:
        value.app.close()


def checkpoint(case, monkeypatch, phase="response_captured"):
    """Cut normal execution at an owned phase, preserving actual durable data."""
    with monkeypatch.context() as patch:
        if phase == "response_captured":
            original = case.service._turn
            async def cut(operation, task, *, phase, response_text=None):
                if phase == "complete":
                    raise RuntimeError("offline lost completion continuation")
                return await original(operation, task, phase=phase, response_text=response_text)
            patch.setattr(case.service, "_turn", cut)
        elif phase == "proposed":
            async def cut(**kwargs):
                raise RuntimeError("offline lost acceptance continuation")
            patch.setattr(case.service._coordinator, "accept_general_proposal_async", cut)
        elif phase in {"call_reserved", "dispatching", "known_success"}:
            if phase == "call_reserved":
                async def cut(*args, **kwargs):
                    raise RuntimeError("offline reserved-before-intent gap")
                patch.setattr(case.service._gateway, "invoke", cut)
            elif phase == "dispatching":
                class LostHost(BaseException):
                    pass
                async def cut(**kwargs):
                    raise LostHost()
                patch.setattr(case.transport, "post_json", cut)
            else:
                def cut(*args, **kwargs):
                    raise RuntimeError("offline capture checkpoint lost")
                patch.setattr(case.service._journal, "capture_response", cut)
        try:
            asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
        except BaseException as exc:
            assert phase == "dispatching" and type(exc).__name__ == "LostHost"
    operation = case.journal.read(case.identity)
    assert operation.phase == ("call_reserved" if phase in {"dispatching", "known_success"} else phase)
    return operation


def reopen(case):
    """New application/authority/Worker, same physical DB and Linux boot."""
    workspace = case.service._workspace
    private = case.service._owner_root.parent
    registry = case.service._gateway.registry
    case.app.close()
    app = ControlPlaneApplication(private / "events.db", limits=_application_limits(),
        artifact_root=private / "artifacts", usage_source=TrustedUsage())
    journal = GeneralWorkerJournal(app._event_store)
    authority = SchedulerProviderDispatchAuthority(app._scheduler, journal)
    broker, transport = OfflineBroker(), OfflineSuccessTransport()
    gateway = ProviderModelGateway(registry=registry, secret_broker=broker, transport=transport,
        provider_call_journal=authority.provider_journal, dispatch_authority=authority)
    service = GeneralWorkerExecutionService(app._scheduler, app._worker_inputs, app._artifact_store,
        app._artifact_grants, VerifierProposalJournal(app._event_store), app._acceptance_coordinator,
        gateway, GeneralWorkerProcess(), workspace)
    case.app, case.journal, case.service = app, journal, service
    case.broker, case.transport = broker, transport


def no_external_work(case, monkeypatch, *, verifier=False):
    def forbidden(*args, **kwargs):
        pytest.fail("held recovery crossed an external-work boundary")
    monkeypatch.setattr(case.service._worker, "prepare_launch", forbidden)
    monkeypatch.setattr(case.service._worker, "start_turn", forbidden)
    monkeypatch.setattr(case.service._gateway, "invoke", forbidden)
    monkeypatch.setattr(case.broker, "acquire_provider_credential", forbidden)
    monkeypatch.setattr(case.transport, "post_json", forbidden)
    if verifier:
        from orchestrator.runtime.verifier_process import IsolatedVerifierProcess
        monkeypatch.setattr(IsolatedVerifierProcess, "verify_async", forbidden)


@pytest.mark.parametrize("entry", ["execute", "recover"])
def test_captured_success_resumes_without_gateway_or_broker(case, monkeypatch, entry):
    checkpoint(case, monkeypatch)
    reopen(case)
    def forbidden(*args, **kwargs):
        pytest.fail("captured recovery attempted Provider work")
    monkeypatch.setattr(case.service._gateway, "invoke", forbidden)
    monkeypatch.setattr(case.broker, "acquire_provider_credential", forbidden)
    monkeypatch.setattr(case.transport, "post_json", forbidden)
    result = asyncio.run(getattr(case.service, entry)(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    assert [turn.phase for turn in case.journal.read(case.identity).turns] == ["request", "complete"]


@pytest.mark.parametrize("phase", ["call_reserved", "dispatching", "unknown"])
def test_dispatching_unknown_and_reserved_gap_block_every_external_boundary(case, monkeypatch, phase):
    if phase == "unknown":
        async def unknown(**kwargs):
            raise HTTPTransportFailed(may_have_been_sent=True)
        with monkeypatch.context() as patch:
            patch.setattr(case.transport, "post_json", unknown)
            asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    else:
        checkpoint(case, monkeypatch, phase)
    reopen(case)
    no_external_work(case, monkeypatch, verifier=True)
    before = _resolver_durable_state(case.app._event_store)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "blocked"
    assert _resolver_durable_state(case.app._event_store) == before
    assert case.journal.read(case.identity).phase == "call_reserved"


def test_missing_capture_never_regenerates(case, monkeypatch):
    checkpoint(case, monkeypatch, "known_success")
    reopen(case)
    no_external_work(case, monkeypatch, verifier=True)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "blocked" and result.code == "general_worker_capture_missing"
    assert case.journal.read(case.identity).capture is None


def test_verified_orphan_stopped_before_successor(case, monkeypatch):
    original_wait = GeneralWorkerSession.wait
    with monkeypatch.context() as patch:
        def lost_wait(self):
            raise RuntimeError("offline lost host before wait")
        patch.setattr(GeneralWorkerSession, "wait", lost_wait)
        asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    operation = case.journal.read(case.identity)
    old_turn, = operation.turns
    assert old_turn.launch_receipt and old_turn.termination_receipt is None
    reopen(case)
    original_stop = case.service._worker.stop_prepared_launch
    original_prepare = case.service._worker.prepare_launch
    stopped = []
    def stop(prepared, receipt):
        assert prepared == old_turn.prepared and receipt == old_turn.launch_receipt
        result = original_stop(prepared, receipt)
        stopped.append(result)
        return result
    def prepare():
        assert stopped and stopped[0].cgroup_empty
        return original_prepare()
    monkeypatch.setattr(case.service._worker, "stop_prepared_launch", stop)
    monkeypatch.setattr(case.service._worker, "prepare_launch", prepare)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    operation = case.journal.read(case.identity)
    assert operation.turns[0].termination_receipt == stopped[0]
    assert [turn.phase for turn in operation.turns] == ["request", "request", "complete"]


@pytest.mark.parametrize("unacked", [True, False])
def test_unacked_or_stop_unknown_retains_lease(case, monkeypatch, unacked):
    # A metadata-only receipt fixture exercises ambiguous states, with no real
    # orphan process falsely claimed. Real acknowledged orphan is covered above.
    original = case.service._turn
    async def cut(*args, **kwargs):
        raise RuntimeError("offline before launch")
    with monkeypatch.context() as patch:
        patch.setattr(case.service, "_turn", cut)
        asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    from tests.unit.runtime.test_general_worker_journal import epoch
    prepared, ack, _ = epoch()
    operation = case.journal.prepare_turn(case.journal.read(case.identity), phase="request", prepared=prepared)
    if not unacked:
        operation = case.journal.ack_turn(operation, receipt=ack)
    no_external_work(case, monkeypatch, verifier=True)
    def unknown(*args):
        raise IsolationStopUnknown("offline stop unavailable")
    monkeypatch.setattr(case.service._worker, "stop_prepared_launch", unknown)
    before = _resolver_durable_state(case.app._event_store)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.code == ("general_worker_launch_unknown" if unacked else "general_worker_stop_unknown")
    assert _resolver_durable_state(case.app._event_store) == before
    assert case.app._scheduler.recovery.recover_control_state("run-1").active_attempts


@pytest.mark.parametrize("fault", ["bytes", "source", "scope"])
def test_cross_operation_or_corrupt_capture_blocks(case, monkeypatch, fault):
    operation = checkpoint(case, monkeypatch)
    ref = operation.capture.response_artifact
    if fault == "bytes":
        case.app._artifact_store._path_for_digest(ref.digest).write_bytes(b"corrupt")
    else:
        def corrupt(payload):
            if fault == "source":
                payload["source"]["general_worker_operation_id"] = "other-operation"
            else:
                payload["readable_scope"] = ["other-operation"]
        corrupt_worker_input_event(case.app._event_store, ("artifact", ref.digest), corrupt)
    # Legacy bootstrap already rejects corrupt artifacts on reopen. Exercise
    # explicit recovery's own proof here without weakening that startup gate.
    no_external_work(case, monkeypatch, verifier=True)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "blocked"
    assert case.journal.read(case.identity).accepted_result is None


@pytest.mark.parametrize("entry", ["execute", "recover"])
def test_historical_accepted_replay_checks_input_and_bytes_before_active_authority(case, monkeypatch, entry):
    accepted = asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    assert accepted.status == "accepted"
    reopen(case)
    no_external_work(case, monkeypatch, verifier=True)
    def forbidden(*args, **kwargs):
        pytest.fail("historical result resolved fresh active authority or usage")
    monkeypatch.setattr(case.app._worker_inputs, "_prepare_general_worker_task", forbidden)
    monkeypatch.setattr(case.app._scheduler, "_resolve_general_verification_binding", forbidden)
    monkeypatch.setattr(case.app._scheduler._general_usage_source, "usage_for", forbidden)
    before = _resolver_durable_state(case.app._event_store)
    result = asyncio.run(getattr(case.service, entry)(identity=case.identity, task_text=case.text))
    assert result == accepted
    wrong = asyncio.run(getattr(case.service, entry)(identity=case.identity, task_text=case.text + " changed"))
    assert wrong.status == "blocked"
    case.app._artifact_store._path_for_digest(accepted.artifacts[0].digest).write_bytes(b"corrupt")
    missing = asyncio.run(getattr(case.service, entry)(identity=case.identity, task_text=case.text))
    assert missing.status == "blocked" and missing.code == "general_worker_artifact_unavailable"
    assert _resolver_durable_state(case.app._event_store) == before
    assert case.app._scheduler.read_general_worker_result(case.identity) == accepted


@pytest.mark.parametrize("withdrawal", ["pause", "cancel", "expiry", "fencing"])
@pytest.mark.parametrize("phase", ["response_captured", "proposed"])
def test_current_authority_withdrawal_blocks_completion_or_acceptance(case, monkeypatch, withdrawal, phase):
    operation = checkpoint(case, monkeypatch, phase)
    if withdrawal == "pause":
        case.app._scheduler.lifecycle.pause_run("run-1", reason_code="user_request")
    elif withdrawal == "cancel":
        case.app._scheduler.lifecycle.request_cancel("run-1", reason_code="user_request")
    elif withdrawal == "expiry":
        import orchestrator.runtime.general_worker_authority as authority_module
        class Later(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(timezone.utc) + timedelta(hours=1)
        monkeypatch.setattr(authority_module, "datetime", Later)
    else:
        corrupt_worker_input_event(case.app._event_store, ("scheduler", "global"),
            lambda payload: payload["accepted_route"].update(fencing_generation=2))
    no_external_work(case, monkeypatch, verifier=True)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "blocked"
    assert case.journal.read(case.identity).capture == operation.capture
    assert case.journal.read(case.identity).accepted_result is None


def test_changed_host_scope_blocks_before_external_work(case, monkeypatch):
    checkpoint(case, monkeypatch)
    import orchestrator.runtime.general_worker_authority as authority_module
    monkeypatch.setattr(authority_module, "general_worker_host_scope", lambda store: "sha256:" + "e" * 64)
    no_external_work(case, monkeypatch, verifier=True)
    before = _resolver_durable_state(case.app._event_store)
    assert asyncio.run(case.service.recover(identity=case.identity, task_text=case.text)).status == "blocked"
    assert _resolver_durable_state(case.app._event_store) == before


def test_same_boot_new_process_agrees_on_database_scope(case):
    expected = general_worker_host_scope(case.app._event_store)
    database = case.service._owner_root.parent / "events.db"
    code = ("from orchestrator.persistence import SQLiteEventStore; "
        "from orchestrator.runtime.general_worker_authority import general_worker_host_scope; "
        "import sys; s=SQLiteEventStore(sys.argv[1]); print(general_worker_host_scope(s)); s.close()")
    child = subprocess.run([sys.executable, "-c", code, str(database)], capture_output=True, check=True)
    assert child.stdout.decode().strip() == expected


def test_proposed_resume_has_no_worker_or_provider_and_uses_fresh_owned_acceptance(case, monkeypatch):
    checkpoint(case, monkeypatch, "proposed")
    reopen(case)
    no_external_work(case, monkeypatch)
    source = case.app._scheduler._general_usage_source
    original = source.usage_for
    usage_calls = []
    def usage(snapshot):
        usage_calls.append(snapshot)
        return original(snapshot)
    monkeypatch.setattr(source, "usage_for", usage)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    assert len(usage_calls) == 1
    assert len(case.journal.read(case.identity).turns) == 2


def test_busy_owner_returns_in_progress_without_grants_or_external_work(case, monkeypatch):
    from orchestrator.runtime.general_worker_recovery import GeneralWorkerOwnership
    operation = checkpoint(case, monkeypatch)
    ownership = GeneralWorkerOwnership(case.service._owner_root, operation.operation_id)
    assert ownership.acquire()
    no_external_work(case, monkeypatch, verifier=True)
    def forbidden(*args, **kwargs):
        pytest.fail("busy contender accessed private artifacts or active input")
    monkeypatch.setattr(case.app._artifact_grants, "issue", forbidden)
    monkeypatch.setattr(case.app._worker_inputs, "_prepare_general_worker_task", forbidden)
    before = _resolver_durable_state(case.app._event_store)
    try:
        result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
        assert result.status == "in_progress"
        assert _resolver_durable_state(case.app._event_store) == before
    finally:
        ownership.close()


@pytest.mark.parametrize("observed", [False, True])
def test_stopped_request_uses_only_its_allowed_fresh_epoch_or_reservation(case, monkeypatch, observed):
    # Metadata-only already-stopped epoch fixture. Actual new turns remain real.
    async def cut(*args, **kwargs):
        raise RuntimeError("offline before initial turn")
    with monkeypatch.context() as patch:
        patch.setattr(case.service, "_turn", cut)
        asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    from tests.unit.runtime.test_general_worker_journal import epoch
    from orchestrator.runtime.general_worker_protocol import GeneralWorkerRequest
    prepared, ack, stop = epoch()
    operation = case.journal.prepare_turn(case.journal.read(case.identity), phase="request", prepared=prepared)
    operation = case.journal.ack_turn(operation, receipt=ack)
    operation = case.journal.stop_turn(operation, receipt=stop,
        request_proposal=GeneralWorkerRequest(operation.context, prepared.launch_id, operation.input_commitment)
            if observed else None)
    reopen(case)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    phases = [turn.phase for turn in case.journal.read(case.identity).turns]
    assert phases == (["request", "complete"] if observed else ["request", "request", "complete"])
    assert case.broker.calls == 1 and len(case.transport.calls) == 1


@pytest.mark.parametrize("stopped", [False, True])
def test_captured_completion_orphan_or_lost_proposal_replays_only_completion(case, monkeypatch, stopped):
    if stopped:
        original = case.service._publish
        def cut(operation, content, *, kind):
            if kind == "output":
                raise RuntimeError("offline after complete stop before output publication")
            return original(operation, content, kind=kind)
        with monkeypatch.context() as patch:
            patch.setattr(case.service, "_publish", cut)
            asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    else:
        original = GeneralWorkerSession.wait
        def cut(self):
            if self._turn.phase == "complete":
                raise RuntimeError("offline lost complete wait")
            return original(self)
        with monkeypatch.context() as patch:
            patch.setattr(GeneralWorkerSession, "wait", cut)
            asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    operation = case.journal.read(case.identity)
    assert operation.phase == ("completion_stopped" if stopped else "completion_requested")
    reopen(case)
    def forbidden(*args, **kwargs):
        pytest.fail("completion replay attempted Provider invoke")
    monkeypatch.setattr(case.service._gateway, "invoke", forbidden)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "accepted", result
    assert [turn.phase for turn in case.journal.read(case.identity).turns] == ["request", "complete", "complete"]
    assert case.broker.calls == 0 and case.transport.calls == []


def test_paused_orphan_cleanup_stops_old_unit_without_launching_successor(case, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(GeneralWorkerSession, "wait", lambda self: (_ for _ in ()).throw(RuntimeError("offline lost wait")))
        asyncio.run(case.service.execute(identity=case.identity, task_text=case.text))
    old = case.journal.read(case.identity).turns[-1]
    case.app._scheduler.lifecycle.pause_run("run-1", reason_code="user_request")
    no_external_work(case, monkeypatch, verifier=True)
    result = asyncio.run(case.service.recover(identity=case.identity, task_text=case.text))
    assert result.status == "blocked"
    operation = case.journal.read(case.identity)
    assert len(operation.turns) == 1 and operation.turns[0].prepared == old.prepared
    assert operation.turns[0].termination_receipt.cgroup_empty
    assert case.app._scheduler.recovery.recover_control_state("run-1").active_attempts
