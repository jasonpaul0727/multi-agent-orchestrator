import io
from datetime import datetime, timedelta, timezone
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
import orchestrator.runtime as runtime
from orchestrator.runtime import verifier_process

from orchestrator.artifacts import ArtifactAccessGrant, ArtifactStore
from orchestrator.isolation import SandboxResult
from orchestrator.isolation.launcher import SandboxTerminationReceipt
from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime.contracts import (
    ArtifactRef,
    AttemptContext,
    VerificationCheck,
    VerificationEvidence,
    VerificationTask,
)


_DIGEST = "sha256:" + "a" * 64


def _context(*, attempt_id="attempt-1"):
    return AttemptContext(
        run_id="run-1",
        node_id="node-1",
        attempt_id=attempt_id,
        agent_instance_id="agent-1",
        fencing_generation=2,
        graph_version=3,
        input_manifest_hash=_DIGEST,
        effective_config_hash=_DIGEST,
        registry_hash=_DIGEST,
        policy_manifest_hash=_DIGEST,
        routing_decision_hash=_DIGEST,
        planning_contract_hash=_DIGEST,
    )


class _ChildLauncher:
    def __init__(self, *, receipt: bool = True):
        self.receipt = receipt
        self.calls = []

    def launch(self, workspace, command, *, limits, input_bytes):
        from orchestrator.runtime.verifier_process import _verification_child_result

        self.calls.append((Path(workspace), tuple(command), limits, input_bytes))
        payload = _verification_child_result(Path(workspace), input_bytes)
        result = SandboxResult(
            unit_name="maestro-attempt-" + "b" * 32 + ".service", returncode=0, stdout=payload,
            stderr=b"", elapsed_seconds=0.01, termination_receipt=(
                SandboxTerminationReceipt(
                    unit_name="maestro-attempt-" + "b" * 32 + ".service",
                    control_group="/user.slice/user-1000.slice/user@1000.service/app.slice/maestro-attempt-" + "b" * 32 + ".service",
                    active_state="inactive",
                    cgroup_empty=True,
                ) if self.receipt else None
            ),
            cancelled=False, timed_out=False, output_limited=False, input_written=True,
        )
        return type("Session", (), {"wait": lambda _self: result})()


def _artifact_store(
    tmp_path,
    *,
    attempt_id="attempt-1",
    content=b"def answer():\n    return 42\n",
    artifact_type="python-source",
    media_type="text/x-python",
):
    events = SQLiteEventStore(tmp_path / "events.db")
    store = ArtifactStore(
        tmp_path / "artifact-bytes",
        event_store=events,
        grant_verifier=lambda grant: grant.signature == "valid",
    )
    record = store.publish_bytes(
        content,
        source={
            "run_id": "run-1",
            "node_id": "node-1",
            "attempt_id": attempt_id,
            "fencing_generation": "2",
            "agent_instance_id": "agent-1",
        },
        artifact_type=artifact_type,
        media_type=media_type,
        readable_scope=("run-1",),
    )
    return events, store, record


def _task(
    record,
    *,
    attempt_id="attempt-1",
    checks=("artifact-integrity", "python-syntax"),
    acceptance_contract="maestro.artifact-verification/v1",
):
    return VerificationTask(
        context=_context(attempt_id=attempt_id),
        candidate_artifacts=(ArtifactRef(
            digest=record.digest,
            size_bytes=record.size,
            artifact_type=record.artifact_type,
            media_type=record.media_type,
        ),),
        required_check_ids=checks,
        acceptance_contract=acceptance_contract,
    )


def _grant(record):
    return ArtifactAccessGrant(
        digest=record.digest,
        scope=("run-1",),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        issuer="test-control-plane",
        signature="valid",
    )


def _evidence(task):
    digests = tuple(item.digest for item in task.candidate_artifacts)
    return VerificationEvidence(
        context=task.context,
        verifier_id="builtin.readonly-v1",
        outcome="accepted",
        checks=tuple(
            VerificationCheck(check_id=check_id, passed=True, evidence_digests=digests)
            for check_id in task.required_check_ids
        ),
        inspected_digests=digests,
    )


def _sandbox_result(
    stdout: bytes, *, returncode: int = 0, receipt: bool = True, cancelled: bool = False,
    timed_out: bool = False, output_limited: bool = False, input_written: bool = True,
    stderr: bytes = b"",
) -> SandboxResult:
    unit_name = "maestro-attempt-" + "c" * 32 + ".service"
    termination_receipt = SandboxTerminationReceipt(
        unit_name=unit_name,
        control_group="/user.slice/user-1000.slice/user@1000.service/app.slice/" + unit_name,
        active_state="inactive",
        cgroup_empty=True,
    ) if receipt else None
    return SandboxResult(
        unit_name=unit_name,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        elapsed_seconds=0.01,
        termination_receipt=termination_receipt,
        cancelled=cancelled,
        timed_out=timed_out,
        output_limited=output_limited,
        input_written=input_written,
    )


def _fixed_result_launcher(result=None, *, launch_error=None, wait_error=None):
    class FixedLauncher:
        def launch(self, workspace, command, *, limits, input_bytes):
            if launch_error is not None:
                raise launch_error

            class Session:
                def wait(self):
                    if wait_error is not None:
                        raise wait_error
                    return result

            return Session()

    return FixedLauncher()


def test_isolated_verifier_checks_host_published_candidate_in_separate_process_contract(tmp_path):
    assert hasattr(runtime, "IsolatedVerifierProcess"), "runtime must expose an isolated Verifier"
    events, store, record = _artifact_store(tmp_path)
    launcher = _ChildLauncher()
    verifier = runtime.IsolatedVerifierProcess(launcher=launcher)

    evidence = verifier.verify(
        _task(record),
        store,
        grant_for_digest=lambda digest: _grant(record),
    )

    assert evidence.outcome == "accepted"
    assert evidence.verifier_id == "builtin.readonly-v1"
    assert evidence.inspected_digests == (record.digest,)
    assert {check.check_id for check in evidence.checks} == {
        "artifact-integrity", "python-syntax",
    }
    assert all(check.passed and check.evidence_digests == (record.digest,) for check in evidence.checks)
    assert len(launcher.calls) == 1
    assert b"def answer" not in launcher.calls[0][3]
    assert events.read_stream("security", "run-1") == []


def test_verifier_rejects_clean_child_output_without_host_termination_receipt(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    verifier = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher(receipt=False))

    with pytest.raises(runtime.VerifierProcessError, match="transport"):
        verifier.verify(
            _task(record),
            store,
            grant_for_digest=lambda digest: _grant(record),
        )


@pytest.mark.skipif(not hasattr(runtime, "IsolatedVerifierProcess"), reason="isolated Verifier is not implemented")
def test_verifier_rejects_artifact_from_another_attempt_before_launch(tmp_path):
    _events, store, record = _artifact_store(tmp_path, attempt_id="attempt-1")
    launcher = _ChildLauncher()
    verifier = runtime.IsolatedVerifierProcess(launcher=launcher)

    with pytest.raises(runtime.VerifierProcessError, match="exact Attempt publication"):
        verifier.verify(
            _task(record, attempt_id="attempt-2"),
            store,
            grant_for_digest=lambda digest: _grant(record),
        )

    assert launcher.calls == []


@pytest.mark.skipif(not hasattr(runtime, "IsolatedVerifierProcess"), reason="isolated Verifier is not implemented")
def test_verifier_reports_failed_python_syntax_without_accepting_candidate(tmp_path):
    events = SQLiteEventStore(tmp_path / "events.db")
    store = ArtifactStore(
        tmp_path / "artifact-bytes",
        event_store=events,
        grant_verifier=lambda grant: grant.signature == "valid",
    )
    record = store.publish_bytes(
        b"def answer(:\n",
        source={
            "run_id": "run-1",
            "node_id": "node-1",
            "attempt_id": "attempt-1",
            "fencing_generation": "2",
            "agent_instance_id": "agent-1",
        },
        artifact_type="python-source",
        media_type="text/x-python",
        readable_scope=("run-1",),
    )
    verifier = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher())

    evidence = verifier.verify(
        _task(record),
        store,
        grant_for_digest=lambda digest: _grant(record),
    )

    assert evidence.outcome == "rejected"
    checks = {check.check_id: check for check in evidence.checks}
    assert checks["artifact-integrity"].passed is True
    assert checks["python-syntax"].passed is False


def test_verifier_accepts_json_text_and_python_syntax_checks(tmp_path):
    _events, store, record = _artifact_store(
        tmp_path,
        content=b'{"answer": 42}',
        artifact_type="json-document",
        media_type="application/json",
    )
    task = _task(record, checks=("artifact-integrity", "utf8-text", "json", "python-syntax"))

    evidence = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher()).verify(
        task,
        store,
        grant_for_digest=lambda digest: _grant(record),
    )

    assert evidence.outcome == "accepted"
    assert all(check.passed for check in evidence.checks)


def test_verifier_rejects_non_utf8_text_and_invalid_json(tmp_path):
    _events, store, record = _artifact_store(
        tmp_path,
        content=b"\xff\xfe",
        artifact_type="binary",
        media_type="application/octet-stream",
    )
    task = _task(record, checks=("artifact-integrity", "utf8-text", "json", "python-syntax"))

    evidence = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher()).verify(
        task,
        store,
        grant_for_digest=lambda digest: _grant(record),
    )

    assert evidence.outcome == "rejected"
    checks = {check.check_id: check for check in evidence.checks}
    assert checks["artifact-integrity"].passed is True
    assert all(not checks[name].passed for name in ("utf8-text", "json", "python-syntax"))


def test_verifier_rejects_a_staged_byte_tamper(tmp_path):
    _events, store, record = _artifact_store(tmp_path)

    class TamperingLauncher(_ChildLauncher):
        def launch(self, workspace, command, *, limits, input_bytes):
            staged = Path(workspace) / "candidate-000.bin"
            os.chmod(staged, 0o600)
            staged.write_bytes(b"tampered")
            return super().launch(workspace, command, limits=limits, input_bytes=input_bytes)

    task = _task(record, checks=("artifact-integrity",))
    evidence = runtime.IsolatedVerifierProcess(launcher=TamperingLauncher()).verify(
        task,
        store,
        grant_for_digest=lambda digest: _grant(record),
    )

    assert evidence.outcome == "rejected"
    assert evidence.checks[0].passed is False


def test_verifier_rejects_unsupported_or_incomplete_acceptance_contract(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    launcher = _ChildLauncher()
    verifier = runtime.IsolatedVerifierProcess(launcher=launcher)

    for task in (
        _task(record, checks=("unknown-check",)),
        _task(record, checks=("python-syntax",)),
        _task(record, acceptance_contract="arbitrary-acceptance-text"),
    ):
        with pytest.raises(runtime.VerifierProcessError):
            verifier.verify(task, store, grant_for_digest=lambda digest: _grant(record))

    assert launcher.calls == []


def test_verifier_rejects_invalid_digest_grant_and_unavailable_inventory(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    verifier = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher())

    with pytest.raises(runtime.VerifierProcessError, match="not authorized or intact"):
        verifier.verify(
            _task(record), store,
            grant_for_digest=lambda digest: ArtifactAccessGrant(
                digest=_DIGEST,
                scope=("run-1",),
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
                issuer="test-control-plane",
                signature="valid",
            ),
        )

    class NoInventory:
        def verify_run_artifacts(self, run_id):
            raise OSError("inventory unavailable")

    with pytest.raises(runtime.VerifierProcessError, match="inventory is unavailable"):
        verifier.verify(_task(record), NoInventory(), grant_for_digest=lambda digest: _grant(record))


def test_verifier_child_rejects_malformed_frame_without_traceback(monkeypatch):
    stdin = SimpleNamespace(buffer=io.BytesIO(b"{}"))
    stdout_buffer = io.BytesIO()
    monkeypatch.setattr(verifier_process.sys, "stdin", stdin)
    monkeypatch.setattr(verifier_process.sys, "stdout", SimpleNamespace(buffer=stdout_buffer))

    assert verifier_process._child_main() == 64
    assert stdout_buffer.getvalue() == b""


@pytest.mark.parametrize("value", [True, False, 0, 64 * 1024 * 1024 + 1, "large"])
def test_verifier_rejects_invalid_byte_limits(value):
    with pytest.raises(ValueError, match="max_artifact_bytes"):
        runtime.IsolatedVerifierProcess(max_artifact_bytes=value)


def test_verifier_child_emits_a_valid_evidence_frame(monkeypatch, tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    task = _task(record)
    workspace = tmp_path / "child-workspace"
    workspace.mkdir()
    (workspace / "candidate-000.bin").write_bytes(store.read_bytes(record.digest, grant=_grant(record)))
    frame = json.dumps({
        "task": task.model_dump(mode="json"),
        "artifact_files": [{
            "digest": record.digest,
            "name": "candidate-000.bin",
            "size_bytes": record.size,
        }],
    }).encode()
    stdout_buffer = io.BytesIO()
    monkeypatch.setattr(verifier_process.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(frame)))
    monkeypatch.setattr(verifier_process.sys, "stdout", SimpleNamespace(buffer=stdout_buffer))
    monkeypatch.chdir(workspace)

    assert verifier_process._child_main() == 0
    evidence = runtime.decode_verification_evidence(stdout_buffer.getvalue())
    assert evidence.outcome == "accepted"


def test_verifier_fails_closed_on_stale_metadata_and_byte_budget(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    task = _task(record)
    verifier = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher())

    class StaleMetadata:
        def verify_run_artifacts(self, run_id):
            return [record.model_copy(update={"size": record.size + 1})]

    with pytest.raises(runtime.VerifierProcessError, match="metadata conflicts"):
        verifier.verify(task, StaleMetadata(), grant_for_digest=lambda digest: _grant(record))

    too_small = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher(), max_artifact_bytes=1)
    with pytest.raises(runtime.VerifierProcessError, match="byte limit"):
        too_small.verify(task, store, grant_for_digest=lambda digest: _grant(record))


def test_verifier_fails_closed_on_size_change_and_invalid_task_or_grant(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    task = _task(record)
    verifier = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher())

    class SizeChanged:
        def verify_run_artifacts(self, run_id):
            return store.verify_run_artifacts(run_id)

        def read_bytes(self, digest, *, grant):
            return b""

    with pytest.raises(runtime.VerifierProcessError, match="size changed"):
        verifier.verify(task, SizeChanged(), grant_for_digest=lambda digest: _grant(record))
    with pytest.raises(TypeError, match="validated VerificationTask"):
        verifier.verify({}, store, grant_for_digest=lambda digest: _grant(record))
    with pytest.raises(TypeError, match="trusted ArtifactAccessGrant provider"):
        verifier.verify(task, store, grant_for_digest=None)


def test_verifier_rejects_oversized_ipc_before_launch(tmp_path, monkeypatch):
    _events, store, record = _artifact_store(tmp_path)
    launcher = _ChildLauncher()
    monkeypatch.setattr(verifier_process, "_MAX_FRAME_BYTES", 1)

    with pytest.raises(runtime.VerifierProcessError, match="IPC frame limit"):
        runtime.IsolatedVerifierProcess(launcher=launcher).verify(
            _task(record), store, grant_for_digest=lambda digest: _grant(record),
        )

    assert launcher.calls == []


def test_verifier_preserves_verifier_errors_from_launcher(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    verifier = runtime.IsolatedVerifierProcess(
        launcher=_fixed_result_launcher(
            launch_error=runtime.VerifierProcessError("explicit verifier refusal"),
        ),
    )

    with pytest.raises(runtime.VerifierProcessError, match="explicit verifier refusal"):
        verifier.verify(_task(record), store, grant_for_digest=lambda digest: _grant(record))


def test_verifier_rejects_host_generated_staging_name_violation(tmp_path, monkeypatch):
    _events, store, record = _artifact_store(tmp_path)
    monkeypatch.setattr(verifier_process, "_STAGED_NAME", re.compile(r"(?!)"))
    verifier = runtime.IsolatedVerifierProcess(launcher=_ChildLauncher())

    with pytest.raises(runtime.VerifierProcessError, match="host generated"):
        verifier.verify(_task(record), store, grant_for_digest=lambda digest: _grant(record))


def test_verifier_rejects_launch_failure_transport_failure_and_invalid_evidence(tmp_path):
    _events, store, record = _artifact_store(tmp_path)
    task = _task(record)

    launch_failure = runtime.IsolatedVerifierProcess(
        launcher=_fixed_result_launcher(launch_error=OSError("systemd unavailable")),
    )
    with pytest.raises(runtime.VerifierProcessError, match="could not be launched"):
        launch_failure.verify(task, store, grant_for_digest=lambda digest: _grant(record))

    wait_failure = runtime.IsolatedVerifierProcess(
        launcher=_fixed_result_launcher(wait_error=OSError("IPC interrupted")),
    )
    with pytest.raises(runtime.VerifierProcessError, match="could not be launched"):
        wait_failure.verify(task, store, grant_for_digest=lambda digest: _grant(record))

    valid = _evidence(task).model_dump_json().encode()
    transport_results = (
        _sandbox_result(valid, receipt=False),
        _sandbox_result(valid, returncode=1),
        _sandbox_result(valid, cancelled=True),
        _sandbox_result(valid, timed_out=True),
        _sandbox_result(valid, output_limited=True),
        _sandbox_result(valid, input_written=False),
        _sandbox_result(valid, stderr=b"diagnostic"),
    )
    for transport in transport_results:
        verifier = runtime.IsolatedVerifierProcess(launcher=_fixed_result_launcher(transport))
        with pytest.raises(runtime.VerifierProcessError, match="transport did not complete"):
            verifier.verify(task, store, grant_for_digest=lambda digest: _grant(record))

    for output in (b"not-json", b"{}"):
        verifier = runtime.IsolatedVerifierProcess(
            launcher=_fixed_result_launcher(_sandbox_result(output)),
        )
        with pytest.raises(runtime.VerifierProcessError, match="invalid evidence"):
            verifier.verify(task, store, grant_for_digest=lambda digest: _grant(record))

    rejected_claim = _evidence(task).model_copy(update={"inspected_digests": ()})
    verifier = runtime.IsolatedVerifierProcess(
        launcher=_fixed_result_launcher(
            _sandbox_result(rejected_claim.model_dump_json().encode()),
        ),
    )
    with pytest.raises(runtime.VerifierProcessError, match="invalid evidence"):
        verifier.verify(task, store, grant_for_digest=lambda digest: _grant(record))


@pytest.mark.parametrize("frame", [
    b"",
    b"not-json",
    b"\xff",
    b'{"task": {}, "task": {}, "artifact_files": []}',
    b'{"task": {}, "artifact_files": [], "extra": true}',
    b'{"task": {"context": {}, "candidate_artifacts": []}, "artifact_files": []}',
    b'{"task": {"context": {}, "candidate_artifacts": [], "required_check_ids": [], "acceptance_contract": "wrong"}, "artifact_files": []}',
    b'{"task": {"context": {}, "candidate_artifacts": [], "required_check_ids": ["artifact-integrity", "artifact-integrity"], "acceptance_contract": "maestro.artifact-verification/v1"}, "artifact_files": []}',
    b'{"task": {"context": {}, "candidate_artifacts": [], "required_check_ids": ["artifact-integrity"], "acceptance_contract": "maestro.artifact-verification/v1"}, "artifact_files": [], "other": 1}',
    b'{"x": NaN}',
])
def test_verifier_child_rejects_malformed_contract_frames(tmp_path, frame):
    with pytest.raises(runtime.VerifierProcessError):
        verifier_process._verification_child_result(tmp_path, frame)


def test_verifier_child_rejects_candidate_path_and_digest_mismatch(tmp_path):
    _events, _store, record = _artifact_store(tmp_path)
    task = _task(record)
    correct = {
        "task": task.model_dump(mode="json"),
        "artifact_files": [{
            "digest": record.digest,
            "name": "candidate-000.bin",
            "size_bytes": record.size,
        }],
    }
    for mutate in (
        lambda frame: frame["artifact_files"][0].update(name="../escape"),
        lambda frame: frame["artifact_files"][0].update(digest=_DIGEST),
    ):
        frame = json.loads(json.dumps(correct))
        mutate(frame)
        with pytest.raises(runtime.VerifierProcessError):
            verifier_process._verification_child_result(
                tmp_path, json.dumps(frame).encode(),
            )

    with pytest.raises(runtime.VerifierProcessError):
        verifier_process._verification_child_result(tmp_path, json.dumps(correct).encode())


def test_verifier_child_rejects_malformed_artifact_structures(tmp_path):
    minimal_task = {
        "context": {},
        "candidate_artifacts": [],
        "required_check_ids": ["artifact-integrity"],
        "acceptance_contract": "maestro.artifact-verification/v1",
    }
    frames = (
        {"task": {**minimal_task, "context": []}, "artifact_files": []},
        {"task": {**minimal_task, "candidate_artifacts": [{}]}, "artifact_files": []},
        {"task": {**minimal_task, "candidate_artifacts": [None]}, "artifact_files": [{}]},
        {
            "task": {**minimal_task, "candidate_artifacts": [{"digest": "bad"}]},
            "artifact_files": [{"digest": "bad", "name": "candidate-000.bin", "size_bytes": 0}],
        },
        {
            "task": {
                **minimal_task,
                "candidate_artifacts": [{
                    "digest": _DIGEST, "size_bytes": 0,
                    "artifact_type": "document", "media_type": "text/plain",
                }],
            },
            "artifact_files": [{"name": "candidate-000.bin"}],
        },
    )
    for frame in frames:
        with pytest.raises(runtime.VerifierProcessError):
            verifier_process._verification_child_result(tmp_path, json.dumps(frame).encode())


def test_module_entrypoint_rejects_non_child_invocation(monkeypatch):
    import runpy

    monkeypatch.setattr(verifier_process.sys, "argv", [str(verifier_process.__file__), "wrong"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(verifier_process.__file__), run_name="__main__")
    assert exc.value.code == 64
