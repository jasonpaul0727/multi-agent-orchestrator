"""Blocked-only isolated Worker IPC boundary tests."""

from __future__ import annotations

import json
import io
from pathlib import Path
import subprocess
import sys

import pytest

from orchestrator.isolation import SandboxResult
from orchestrator.runtime.contracts import AttemptContext, WorkerResult, WorkerTask
from orchestrator.runtime.worker_process import IsolatedWorkerProcess, WorkerProcessError
import orchestrator.runtime.worker_process as worker_module


_DIGEST = "sha256:" + "a" * 64


def _task(*, text: str = "Inspect this task.") -> WorkerTask:
    context = AttemptContext(
        run_id="run-1",
        node_id="node-1",
        attempt_id="attempt-1",
        agent_instance_id="agent-1",
        fencing_generation=1,
        graph_version=2,
        input_manifest_hash=_DIGEST,
        effective_config_hash=_DIGEST,
        registry_hash=_DIGEST,
        policy_manifest_hash=_DIGEST,
        routing_decision_hash=_DIGEST,
        planning_contract_hash=_DIGEST,
    )
    return WorkerTask(
        context=context,
        role="coder",
        task_text=text,
        input_artifacts=(),
        tool_capabilities=(),
        output_byte_limit=1024,
    )


def _sandbox_result(**overrides) -> SandboxResult:
    payload = WorkerResult(
        context=_task().context,
        result_id="blocked-1",
        outcome="blocked",
        artifacts=(),
    ).model_dump_json().encode("utf-8")
    fields = dict(
        unit_name="unit.service", returncode=0, stdout=payload, stderr=b"",
        elapsed_seconds=0.01, termination_confirmed=True, cancelled=False,
        timed_out=False, output_limited=False, input_written=True,
    )
    fields.update(overrides)
    return SandboxResult(**fields)


class _Session:
    def __init__(self, result: SandboxResult) -> None:
        self.result = result

    def wait(self) -> SandboxResult:
        return self.result


class _Launcher:
    def __init__(self, result: SandboxResult) -> None:
        self.result = result
        self.calls = []

    def launch(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return _Session(self.result)


def test_worker_process_passes_task_only_on_stdin_and_accepts_bound_blocked_result(tmp_path: Path) -> None:
    task = _task(text="canary-private-task-text")
    launcher = _Launcher(_sandbox_result())

    result = IsolatedWorkerProcess(launcher=launcher).execute(tmp_path, task)

    assert result.outcome == "blocked"
    (args, kwargs), = launcher.calls
    assert args[0] == tmp_path
    assert args[1] == [
        "/usr/bin/python3", "@maestro-runtime@/orchestrator/runtime/worker_process.py", "--child"
    ]
    assert b"canary-private-task-text" in kwargs["input_bytes"]
    assert "canary-private-task-text" not in repr(args)


@pytest.mark.parametrize(
    "result",
    [
        _sandbox_result(returncode=64),
        _sandbox_result(input_written=False),
        _sandbox_result(termination_confirmed=False),
        _sandbox_result(cancelled=True),
        _sandbox_result(timed_out=True),
        _sandbox_result(output_limited=True),
        _sandbox_result(stderr=b"child error"),
        _sandbox_result(stdout=b"{not-json"),
        _sandbox_result(stdout=b" " * 1_048_577),
        _sandbox_result(stdout=WorkerResult(
            context=_task().context.model_copy(update={"attempt_id": "stale"}),
            result_id="blocked-1", outcome="blocked", artifacts=(),
        ).model_dump_json().encode()),
        _sandbox_result(stdout=WorkerResult(
            context=_task().context, result_id="candidate-1", outcome="candidate",
            artifacts=({"digest": "sha256:" + "b" * 64, "size_bytes": 1,
                        "artifact_type": "source", "media_type": "text/plain"},),
        ).model_dump_json().encode()),
    ],
)
def test_worker_process_fails_closed_on_transport_or_unauthorized_proposal(
    tmp_path: Path, result: SandboxResult
) -> None:
    with pytest.raises(WorkerProcessError):
        IsolatedWorkerProcess(launcher=_Launcher(result)).execute(tmp_path, _task())


def test_worker_process_rejects_unvalidated_task_before_launch(tmp_path: Path) -> None:
    launcher = _Launcher(_sandbox_result())
    with pytest.raises(TypeError, match="validated WorkerTask"):
        IsolatedWorkerProcess(launcher=launcher).execute(tmp_path, {"task_text": "bad"})  # type: ignore[arg-type]
    assert not launcher.calls


def test_worker_process_rejects_oversized_encoded_task_before_launch(
    monkeypatch, tmp_path: Path
) -> None:
    launcher = _Launcher(_sandbox_result())
    monkeypatch.setattr(worker_module, "_MAX_FRAME_BYTES", 10)
    with pytest.raises(WorkerProcessError, match="input exceeds"):
        IsolatedWorkerProcess(launcher=launcher).execute(tmp_path, _task())
    assert not launcher.calls


def _run_child_in_process(monkeypatch, payload: bytes) -> tuple[int, str]:
    captured = io.StringIO()
    with monkeypatch.context() as patch:
        patch.setattr(worker_module.sys, "stdin", io.TextIOWrapper(io.BytesIO(payload), encoding="utf-8"))
        patch.setattr(worker_module.sys, "stdout", captured)
        status = worker_module._child_main()
    return status, captured.getvalue()


def test_child_valid_task_in_process(monkeypatch) -> None:
    task = _task(text="private task text")
    status, output = _run_child_in_process(monkeypatch, task.model_dump_json().encode())
    assert status == 0
    assert "private task text" not in output
    proposal = WorkerResult.model_validate(json.loads(output))
    assert proposal.context == task.context
    assert proposal.outcome == "blocked"


@pytest.mark.parametrize("change", [
    lambda value: [],
    lambda value: {**value, "credential": "forbidden"},
    lambda value: {**value, "context": []},
    lambda value: {**value, "context": {**value["context"], "unexpected": "x"}},
    lambda value: {**value, "context": {**value["context"], "run_id": "bad id"}},
    lambda value: {**value, "context": {**value["context"], "registry_hash": "bad"}},
    lambda value: {**value, "context": {**value["context"], "fencing_generation": 0}},
    lambda value: {**value, "context": {**value["context"], "fencing_generation": True}},
    lambda value: {**value, "context": {**value["context"], "graph_version": -1}},
    lambda value: {**value, "role": "bad role"},
    lambda value: {**value, "task_text": "   "},
    lambda value: {**value, "input_artifacts": "not-an-array"},
    lambda value: {**value, "tool_capabilities": "not-an-array"},
    lambda value: {**value, "output_byte_limit": 0},
    lambda value: {**value, "output_byte_limit": True},
])
def test_child_rejects_invalid_envelope_in_process(monkeypatch, change) -> None:
    value = json.loads(_task().model_dump_json())
    payload = json.dumps(change(value)).encode("utf-8")
    assert _run_child_in_process(monkeypatch, payload) == (64, "")


@pytest.mark.parametrize("payload", [
    b"", b"not-json", b'{"role":"a","role":"b"}', b'{"role":NaN}',
    b"{" + b" " * 1_048_577,
])
def test_child_rejects_unparseable_frame_in_process(monkeypatch, payload: bytes) -> None:
    assert _run_child_in_process(monkeypatch, payload) == (64, "")


def test_child_emits_only_blocked_proposal_and_never_echoes_task_text() -> None:
    task = _task(text="canary-sensitive-task-text")
    script = Path(__file__).resolve().parents[3] / "src/orchestrator/runtime/worker_process.py"
    process = subprocess.run(
        [sys.executable, str(script), "--child"],
        input=task.model_dump_json().encode(),
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert process.returncode == 0
    assert process.stderr == b""
    assert b"canary-sensitive-task-text" not in process.stdout
    proposal = WorkerResult.model_validate(json.loads(process.stdout))
    assert proposal.context == task.context
    assert proposal.outcome == "blocked"
    assert proposal.artifacts == ()


@pytest.mark.parametrize("payload", [
    b"", b"not-json", b"{" + b" " * 1_048_577, b'{"context":{},"context":{}}',
    b'{"context":{},"credential":"secret"}',
], ids=["empty", "malformed", "oversized", "duplicate", "unexpected-field"])
def test_child_rejects_malformed_or_oversized_input_without_leaking(payload: bytes) -> None:
    script = Path(__file__).resolve().parents[3] / "src/orchestrator/runtime/worker_process.py"
    process = subprocess.run(
        [sys.executable, str(script), "--child"],
        input=payload, capture_output=True, check=False, timeout=5,
    )
    assert process.returncode == 64
    assert process.stdout == b""
    assert process.stderr == b""
