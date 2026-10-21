"""The explicit one-tool protocol rejects authority substitution and unsafe frames."""

import importlib.util
import hashlib
import json

import pytest

from tests.unit.runtime.test_worker_process import _task, _sandbox_result, _Launcher


def api():
    assert importlib.util.find_spec("orchestrator.runtime.workspace_worker"), "workspace Worker missing"
    from orchestrator.runtime import workspace_worker
    return workspace_worker


def envelope():
    m = api()
    program = m.WorkspaceToolProgram(command=("/usr/bin/printf", "ok"))
    task = _task(text=program.canonical_text)
    return m.WorkspaceWorkerTask.bind(task)


def test_explicit_program_and_bound_reference_round_trip():
    m = api()
    task = envelope()
    proposal = m.decode_workspace_proposal(m.proposal_bytes(task))
    m.validate_workspace_proposal(task, proposal)
    assert proposal.command == ("/usr/bin/printf", "ok")
    assert proposal.context == task.task.context
    assert proposal.tool_reference == task.task.tool_capabilities[0]
    assert len(task.task.tool_capabilities) == 1
    assert json.loads(task.task.task_text) == {
        "schema": 1, "tool_id": "workspace.write-candidate", "command": ["/usr/bin/printf", "ok"],
    }


def test_operation_identity_uses_only_the_accepted_input_commitment():
    m = api()
    context = dict(run_id="run", node_id="node", attempt_id="attempt", fencing_generation=2,
                   input_manifest_hash="sha256:" + "a" * 64, request_id="caller-renaming")
    committed_bytes = (b'maestro.workspace-worker.v1\0{"attempt_id":"attempt","fencing_generation":2,'
        b'"input_manifest_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"node_id":"node","run_id":"run"}')
    expected = "sha256:" + hashlib.sha256(committed_bytes).hexdigest()
    assert m.operation_id(context) == expected
    assert m.operation_id(context | {"request_id": "another-name"}) == expected
    assert m.operation_id(context | {"fencing_generation": 3}) != expected


@pytest.mark.parametrize("raw", [
    b'{"schema":1,"schema":1,"tool_id":"workspace.write-candidate","command":["true"]}',
    b'{"schema":NaN,"tool_id":"workspace.write-candidate","command":["true"]}',
    b'{"schema":true,"tool_id":"workspace.write-candidate","command":["true"]}',
    b'{"schema":1,"tool_id":"other","command":["true"]}',
    b'{"schema":1,"tool_id":"workspace.write-candidate","command":["true"],"workspace":"/tmp"}',
    b"do something", b"\xff", b" " * 1_048_577,
])
def test_program_rejects_noncanonical_or_malformed_input_without_leaking(raw):
    m = api()
    with pytest.raises(m.WorkspaceWorkerError) as caught:
        m.decode_workspace_program(raw)
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("command", [
    (), ("",), ("true", ""), ("true", "\0"), ("true",) * 129,
    ("true", "\\" * 4096), ("true", "\ud800"),
])
def test_program_checks_argument_and_encoded_byte_bounds(command):
    m = api()
    with pytest.raises(ValueError):
        m.WorkspaceToolProgram(command=command)


@pytest.mark.parametrize("field,value", [
    ("command", ("false",)), ("request_id", "renamed"),
    ("workspace", "/private"), ("profile_hash", "sha256:" + "f" * 64),
    ("principal", "admin"), ("approval_grant_id", "forged"),
])
def test_host_rejects_proposal_substitutions(field, value):
    m = api()
    task = envelope()
    payload = json.loads(m.proposal_bytes(task))
    payload[field] = value
    with pytest.raises(m.WorkspaceWorkerError):
        proposal = m.decode_workspace_proposal(json.dumps(payload).encode())
        m.validate_workspace_proposal(task, proposal)


def test_unsafe_constructed_nested_inputs_cannot_cross_boundary(tmp_path):
    m = api()
    task = envelope()
    bad_task = task.model_copy(update={"task": task.task.model_copy(update={"role": True})})
    with pytest.raises(m.WorkspaceWorkerError):
        m.IsolatedWorkspaceWorker().propose(tmp_path, bad_task)
    forged = task.model_copy(update={"workspace": "/private"})
    with pytest.raises(m.WorkspaceWorkerError):
        m.IsolatedWorkspaceWorker().propose(tmp_path, forged)


@pytest.mark.parametrize("change", [
    {"termination_receipt": None}, {"returncode": 1}, {"input_written": False},
    {"cancelled": True}, {"timed_out": True}, {"output_limited": True},
    {"stderr": b"private-error"}, {"stdout": b" " * 16_385},
])
def test_no_proposal_without_clean_transport_and_verified_stop(tmp_path, change):
    m = api()
    task = envelope()
    result = _sandbox_result(**({"stdout": m.proposal_bytes(task)} | change))
    with pytest.raises(m.WorkspaceWorkerError):
        m.IsolatedWorkspaceWorker(launcher=_Launcher(result)).propose(tmp_path, task)


def test_final_serialized_frame_limit_is_checked(tmp_path, monkeypatch):
    m = api()
    task = envelope()
    monkeypatch.setattr(m, "MAX_FRAME_BYTES", 1)
    with pytest.raises(m.WorkspaceWorkerError):
        m.IsolatedWorkspaceWorker().propose(tmp_path, task)


def test_fixed_child_proposes_without_executing_the_program(tmp_path):
    m = api()
    import subprocess
    import sys
    program = m.WorkspaceToolProgram(command=("/usr/bin/touch", str(tmp_path / "forbidden")))
    task = m.WorkspaceWorkerTask.bind(_task(text=program.canonical_text))
    child = subprocess.run([sys.executable, m.__file__, "--child"],
                           input=task.model_dump_json(by_alias=True).encode(), capture_output=True)
    assert child.returncode == 0, child.stderr
    m.validate_workspace_proposal(task, m.decode_workspace_proposal(child.stdout))
    assert not (tmp_path / "forbidden").exists()
