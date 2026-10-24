"""Bounded general Worker data, including its real stdlib-only child entry."""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from orchestrator.runtime.contracts import AttemptContext, ToolCapabilityRef, WorkerTask
from orchestrator.runtime.general_worker_protocol import (
    GeneralWorkerProtocolError, GeneralWorkerRequest, GeneralWorkerTurn,
    decode_general_worker_proposal, decode_general_worker_turn, encode_general_worker_turn,
)


LAUNCH = "4dc48974-60de-476b-8828-e6d979729813"
CHILD = Path(__file__).resolve().parents[3] / "src/orchestrator/runtime/general_worker_process.py"


def task(limit=16_384):
    h = "sha256:" + "a" * 64
    context = AttemptContext(run_id="run-1", node_id="node-1", attempt_id="attempt-1",
        agent_instance_id="agent-1", fencing_generation=2, graph_version=3,
        input_manifest_hash=h, effective_config_hash=h, registry_hash=h,
        policy_manifest_hash=h, routing_decision_hash=h, planning_contract_hash=h)
    return WorkerTask(context=context, role="coder", task_text="private-task-sentinel é\r\n",
        input_artifacts=(), tool_capabilities=(), output_byte_limit=limit)


def wire(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def child(payload):
    return subprocess.run(["/usr/bin/python3", "-I", "-S", str(CHILD), "--child"],
        input=payload, capture_output=True, timeout=5, check=False)


def rejected(call):
    with pytest.raises(GeneralWorkerProtocolError) as caught:
        call()
    error = caught.value
    assert error.__context__ is None and error.__cause__ is None
    assert "private-" not in repr(error) + str(error) + repr(error.__dict__)
    return error


def test_request_roundtrip_binds_epoch_and_context():
    turn = GeneralWorkerTurn("request", LAUNCH, task())
    payload = encode_general_worker_turn(turn)
    assert decode_general_worker_turn(payload) == turn
    assert json.loads(payload)["task"]["task_text"] == "private-task-sentinel é\r\n"
    result = child(payload)
    assert (result.returncode, result.stderr) == (0, b"")
    proposal = decode_general_worker_proposal(result.stdout, turn)
    assert type(proposal) is GeneralWorkerRequest
    assert proposal.launch_id == LAUNCH and proposal.context == turn.task.context
    canonical_task = json.dumps(turn.task.model_dump(mode="json"), ensure_ascii=False,
        sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    assert proposal.input_commitment == "sha256:" + hashlib.sha256(
        b"maestro.general-worker-input.v1\0" + canonical_task).hexdigest()
    assert "private-task-sentinel" not in repr(turn) + repr(proposal)


def test_complete_emits_real_utf8_candidate():
    text = "private-response-sentinel é\r\n"
    turn = GeneralWorkerTurn("complete", LAUNCH, task(), text)
    assert decode_general_worker_turn(encode_general_worker_turn(turn)) == turn
    result = child(encode_general_worker_turn(turn))
    assert (result.returncode, result.stderr) == (0, b"")
    decoded = decode_general_worker_proposal(result.stdout, turn)
    assert decoded.result.context == turn.task.context
    assert decoded.result.outcome == "candidate"
    assert len(decoded.blobs) == 1
    blob = decoded.blobs[0]
    assert blob.content == text.encode() and blob.artifact.size_bytes == len(text.encode())
    assert blob.artifact.digest == "sha256:" + hashlib.sha256(text.encode()).hexdigest()
    assert blob.artifact.media_type == "text/plain; charset=utf-8"
    assert "private-response-sentinel" not in repr(turn) + repr(decoded)


@pytest.mark.parametrize("case", ["duplicate", "unknown", "nan", "overflow", "context",
    "epoch", "commitment", "oversize", "surrogate", "bool", "task_unknown", "wrong_phase"])
def test_protocol_rejects_duplicate_unknown_nonfinite_stale_and_oversize(case):
    turn = GeneralWorkerTurn("request", LAUNCH, task())
    raw = encode_general_worker_turn(turn)
    value = json.loads(raw)
    if case == "duplicate": raw = raw[:-1] + b',"phase":"complete"}'
    elif case == "unknown": value["private-secret"] = "private-secret"
    elif case == "nan": raw = raw[:-1] + b',"private-secret":NaN}'
    elif case == "overflow": raw = raw[:-1] + b',"private-secret":1e999}'
    elif case == "oversize": raw = raw + b" " * (1_048_577 - len(raw))
    elif case == "surrogate": value["task"]["task_text"] = "\ud800"
    elif case == "bool": value["task"]["context"]["fencing_generation"] = True
    elif case == "task_unknown": value["task"]["private-secret"] = "private-secret"
    elif case == "wrong_phase": value["response_text"] = "private-response"
    else:
        result = child(raw)
        proposal = json.loads(result.stdout)
        if case == "context": proposal["context"]["graph_version"] += 1
        elif case == "epoch": proposal["launch_id"] = "a552794a-972d-4f80-8c8f-03cbb19fe9e9"
        else: proposal["input_commitment"] = "sha256:" + "b" * 64
        rejected(lambda: decode_general_worker_proposal(wire(proposal), turn))
        return
    if case in {"unknown", "surrogate", "bool", "task_unknown", "wrong_phase"}:
        raw = json.dumps(value).encode()
    rejected(lambda: decode_general_worker_turn(raw))
    result = child(raw)
    assert (result.returncode, result.stdout, result.stderr) == (64, b"", b"")


def test_frame_and_terminal_byte_boundaries():
    turn = GeneralWorkerTurn("request", LAUNCH, task())
    raw = encode_general_worker_turn(turn)
    assert decode_general_worker_turn(raw + b" " * (1_048_576 - len(raw))) == turn
    for limit in (3, 16_384, 20_000):
        cap = min(limit, 16_384)
        complete = GeneralWorkerTurn("complete", LAUNCH, task(limit), "é" * (cap // 2) + "x" * (cap % 2))
        result = child(encode_general_worker_turn(complete))
        assert len(decode_general_worker_proposal(result.stdout, complete).blobs[0].content) == cap
        rejected(lambda: encode_general_worker_turn(replace(complete, response_text=complete.response_text + "x")))
    rejected(lambda: encode_general_worker_turn(replace(turn, phase="complete")))
    rejected(lambda: encode_general_worker_turn(replace(turn, launch_id="private-invalid")))


def test_constructed_subclass_rejected_without_callback():
    called = []
    class HostileTask(WorkerTask):
        def __getattribute__(self, name):
            called.append(name)
            raise RuntimeError("private-task-sentinel")
    hostile = HostileTask.model_construct()
    called.clear()
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, hostile)))
    assert called == []
    unsafe = task().model_copy(update={"task_text": 123})
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    nested = task().model_copy(update={"context": task().context.model_copy(update={"graph_version": True})})
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, nested)))

    class HostileContext(AttemptContext):
        def __getattribute__(self, name):
            called.append(name)
            raise RuntimeError("private-task-sentinel")
    context = HostileContext.model_construct()
    nested = task().model_copy(update={"context": context})
    called.clear()
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, nested)))
    assert called == []
    extra = task().model_copy(update={"private-secret": "private-secret"})
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, extra)))


def test_complete_proposal_cannot_substitute_content_or_context():
    turn = GeneralWorkerTurn("complete", LAUNCH, task(), "actual")
    original = json.loads(child(encode_general_worker_turn(turn)).stdout)
    for change in ("context", "content", "extra", "epoch"):
        value = json.loads(json.dumps(original))
        if change == "context": value["output"]["result"]["context"]["graph_version"] += 1
        elif change == "content": value["output"]["blobs"][0]["content_base64"] = "ZmFrZQ=="
        elif change == "extra": value["private-secret"] = "private-secret"
        else: value["launch_id"] = "a552794a-972d-4f80-8c8f-03cbb19fe9e9"
        rejected(lambda: decode_general_worker_proposal(wire(value), turn))


@pytest.mark.parametrize("phase", ["request", "complete"])
@pytest.mark.parametrize("payload", [b"", b"\xff", b"\xef\xbb\xbf{}", b"null", b"[]",
    b'{"private-secret":NaN}', b'{"private-secret":1e999}',
    b'{"launch_id":"private-secret","launch_id":"private-secret"}',
    b'{"private-secret":"\\ud800"}', b" " * 1_048_577])
def test_proposal_rejects_invalid_json_without_sensitive_error(phase, payload):
    turn = GeneralWorkerTurn(phase, LAUNCH, task(), "text" if phase == "complete" else None)
    rejected(lambda: decode_general_worker_proposal(payload, turn))


def test_constructed_task_cannot_change_fingerprint_on_roundtrip():
    # WorkerTask canonicalizes capability order. Reject a constructor bypass
    # rather than accepting bytes whose fingerprint changes when decoded.
    h = "sha256:" + "a" * 64
    refs = tuple(ToolCapabilityRef(grant_id=name, tool_id="tool", scope_hash=h) for name in ("z", "a"))
    unsafe = task().model_copy(update={"tool_capabilities": refs})
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    value = json.loads(encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, task())))
    value["task"]["tool_capabilities"] = [item.model_dump() for item in refs]
    rejected(lambda: decode_general_worker_turn(wire(value)))
