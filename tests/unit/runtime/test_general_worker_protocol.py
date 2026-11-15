"""Bounded general Worker data, including its real stdlib-only child entry."""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from orchestrator.runtime import general_worker_protocol as protocol
from orchestrator.runtime.contracts import ArtifactRef, AttemptContext, ToolCapabilityRef, WorkerTask
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
    assert str(error) == "general_worker_protocol_invalid"
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


@pytest.mark.parametrize("field, expected, count", [
    ("input_artifacts", ArtifactRef, 129),
    ("tool_capabilities", ToolCapabilityRef, 65),
])
@pytest.mark.parametrize("collection", [list, tuple])
def test_encode_rejects_collection_count_before_converting_any_element(
    monkeypatch, field, expected, count, collection,
):
    # Moving count validation after element conversion spends work on rejected input.
    h = "sha256:" + "a" * 64
    if expected is ArtifactRef:
        refs = [ArtifactRef(digest=f"sha256:{index:064x}", size_bytes=0,
            artifact_type="text", media_type="text/plain") for index in range(count)]
    else:
        refs = [ToolCapabilityRef(grant_id=f"grant-{index:03}", tool_id="tool",
            scope_hash=h) for index in range(count)]
    unsafe = task().model_copy(update={field: collection(refs)})
    converted = []
    original = protocol._model_fields

    def observe_fields(value, contract):
        if contract is expected:
            converted.append(value)
        return original(value, contract)

    monkeypatch.setattr(protocol, "_model_fields", observe_fields)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert len(converted) == 0


@pytest.mark.parametrize("field, value", [
    ("task_text", "x" * 100_001),
    ("task_text", ["private-nested", {"private-key": ["private-value"]}]),
    ("role", {"private-key": ["private-value"]}),
    ("output_byte_limit", [1]),
], ids=["overlong-task", "task-container", "role-container", "limit-container"])
def test_encode_rejects_task_scalar_before_recursive_plain_copy(monkeypatch, field, value):
    # Calling generic copying before schema validation visits rejected scalar data.
    unsafe = task().model_copy(update={field: value})
    visited = []
    original = protocol._plain

    def observe_plain(item):
        if item is value:
            visited.append(item)
        return original(item)

    monkeypatch.setattr(protocol, "_plain", observe_plain)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert len(visited) == 0


@pytest.mark.parametrize("contract, field, value", [
    (AttemptContext, "run_id", "x" * 129),
    (AttemptContext, "graph_version", [3]),
    (ArtifactRef, "media_type", "x" * 129),
    (ArtifactRef, "size_bytes", {"private-key": [0]}),
    (ToolCapabilityRef, "grant_id", "x" * 129),
    (ToolCapabilityRef, "scope_hash", ["private-scope"]),
], ids=["context-long", "context-container", "artifact-long", "artifact-container",
    "capability-long", "capability-container"])
def test_encode_rejects_nested_scalar_before_recursive_plain_copy(
    monkeypatch, contract, field, value,
):
    # Nested schema validation must precede visiting invalid scalar contents.
    h = "sha256:" + "a" * 64
    if contract is AttemptContext:
        model = task().context
        update = {"context": model.model_copy(update={field: value})}
    elif contract is ArtifactRef:
        model = ArtifactRef(digest=h, size_bytes=0, artifact_type="text", media_type="text/plain")
        update = {"input_artifacts": (model.model_copy(update={field: value}),)}
    else:
        model = ToolCapabilityRef(grant_id="grant", tool_id="tool", scope_hash=h)
        update = {"tool_capabilities": (model.model_copy(update={field: value}),)}
    visited = []
    original = protocol._plain

    def observe_plain(item):
        if item is value:
            visited.append(item)
        return original(item)

    monkeypatch.setattr(protocol, "_plain", observe_plain)
    unsafe = task().model_copy(update=update)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert len(visited) == 0


@pytest.mark.parametrize("field, value", [
    ("phase", ["request"]),
    ("launch_id", "x" * 100_001),
], ids=["phase-container", "overlong-launch"])
def test_encode_rejects_turn_scalar_before_observing_task(monkeypatch, field, value):
    # Invalid envelope scalars must fail without inspecting a task contract.
    observed = []
    original = protocol._model_fields

    def observe_fields(model, contract):
        observed.append(contract)
        return original(model, contract)

    monkeypatch.setattr(protocol, "_model_fields", observe_fields)
    turn = replace(GeneralWorkerTurn("request", LAUNCH, task()), **{field: value})
    rejected(lambda: encode_general_worker_turn(turn))
    assert len(observed) == 0


def test_encode_rejects_wrong_model_field_count_before_inspecting_keys(monkeypatch):
    # Removing the size gate would inspect each injected key before rejecting.
    unsafe = task().model_copy(update={f"private-extra-{index}": None for index in range(20)})
    keys = tuple(object.__getattribute__(unsafe, "__dict__"))
    inspected = []

    def observe_type(value):
        if any(value is key for key in keys):
            inspected.append(value)
        return type(value)

    monkeypatch.setattr(protocol, "type", observe_type, raising=False)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert len(inspected) == 0


def test_model_field_snapshot_detaches_from_later_source_dictionary_changes():
    # Returning a live model dictionary lets later host mutations change the observation.
    model = task()
    snapshot = protocol._model_fields(model, WorkerTask)
    object.__getattribute__(model, "__dict__")["task_text"] = ["private-mutated"]
    assert snapshot["task_text"] == "private-task-sentinel é\r\n"


@pytest.mark.parametrize("contract, field", [
    (WorkerTask, "role"), (AttemptContext, "run_id"),
    (ArtifactRef, "artifact_type"), (ToolCapabilityRef, "grant_id"),
])
@pytest.mark.parametrize("string_subclass", [False, True])
def test_same_count_forged_model_keys_never_invoke_hash_or_equality(contract, field, string_subclass):
    # A lookup or set construction before exact-str validation invokes caller code.
    callbacks = []

    class ForgedKey:
        def __hash__(self):
            callbacks.append("hash")
            return hash(field)

        def __eq__(self, other):
            callbacks.append("equality")
            return other == field

    class ForgedString(str):
        __hash__ = ForgedKey.__hash__
        __eq__ = ForgedKey.__eq__

    h = "sha256:" + "a" * 64
    if contract is WorkerTask:
        model = unsafe = task()
    elif contract is AttemptContext:
        model = task().context
        unsafe = task().model_copy(update={"context": model})
    elif contract is ArtifactRef:
        model = ArtifactRef(digest=h, size_bytes=0, artifact_type="text", media_type="text/plain")
        unsafe = task().model_copy(update={"input_artifacts": (model,)})
    else:
        model = ToolCapabilityRef(grant_id="grant", tool_id="tool", scope_hash=h)
        unsafe = task().model_copy(update={"tool_capabilities": (model,)})
    fields = object.__getattribute__(model, "__dict__")
    key = ForgedString(field) if string_subclass else ForgedKey()
    fields[key] = fields.pop(field)
    callbacks.clear()
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert callbacks == []


@pytest.mark.parametrize("mutation", ["grow", "shrink", "replace"])
def test_model_dictionary_mutation_during_key_observation_is_rejected(monkeypatch, mutation):
    # The snapshot must reject detectable size/key changes during bounded observation.
    unsafe = task()
    fields = object.__getattribute__(unsafe, "__dict__")
    keys = tuple(fields)
    inspected = []

    def observe_type(value):
        if any(value is key for key in keys):
            inspected.append(value)
            if len(inspected) == 1:
                if mutation in ("shrink", "replace"):
                    del fields["output_byte_limit"]
                if mutation in ("grow", "replace"):
                    fields["private-added"] = None
        return type(value)

    monkeypatch.setattr(protocol, "type", observe_type, raising=False)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert 1 <= len(inspected) <= len(keys)


@pytest.mark.parametrize("field, contract", [
    ("input_artifacts", ArtifactRef), ("tool_capabilities", ToolCapabilityRef),
])
@pytest.mark.parametrize("mutation", ["grow", "shrink", "finite-growth"])
def test_collection_snapshot_rejects_length_changes_without_following_new_elements(
    monkeypatch, field, contract, mutation,
):
    # A live list iterator follows appended items and silently accepts a shortened list.
    h = "sha256:" + "a" * 64

    def reference(index):
        if contract is ArtifactRef:
            return ArtifactRef(digest=f"sha256:{index:064x}", size_bytes=0,
                artifact_type="text", media_type="text/plain")
        return ToolCapabilityRef(grant_id=f"grant-{index:03}", tool_id="tool", scope_hash=h)

    source = [reference(0), reference(1)]
    unsafe = task().model_copy(update={field: source})
    converted = []
    original = protocol._model_fields

    def observe_fields(model, expected):
        snapshot = original(model, expected)
        if expected is contract:
            converted.append(model)
            if mutation == "shrink" and len(converted) == 1:
                source.pop()
            elif mutation == "grow" and len(converted) == 1:
                source.append(reference(2))
            elif mutation == "finite-growth" and len(converted) <= 4:
                source.append(reference(len(source)))
        return snapshot

    monkeypatch.setattr(protocol, "_model_fields", observe_fields)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert 1 <= len(converted) <= 2


@pytest.mark.parametrize("phase", ["request", "complete"])
def test_encode_builds_valid_outbound_snapshot_without_generic_plain_copy(monkeypatch, phase):
    # Reintroducing generic outbound copying would revisit the entire schema snapshot.
    observed = []
    original = protocol._plain

    def observe_plain(value):
        observed.append(type(value))
        return original(value)

    monkeypatch.setattr(protocol, "_plain", observe_plain)
    turn = GeneralWorkerTurn(phase, LAUNCH, task(), "é" if phase == "complete" else None)
    payload = encode_general_worker_turn(turn)
    assert json.loads(payload)["phase"] == phase
    assert len(observed) == 0


@pytest.mark.parametrize("limit, response", [
    (3, "xxxx"), (20_000, "x" * 16_385), (3, "xxx\ud800"),
], ids=["task-byte-limit", "protocol-byte-limit", "overlong-surrogate"])
def test_completion_character_cap_rejects_before_encoding_response(limit, response):
    # Without the cheap character gate, even already-invalid strings reach UTF-8 encoding.
    encoded = []
    previous = sys.getprofile()

    def observe_encoding(frame, event, function):
        if (event == "c_call" and getattr(function, "__name__", None) == "encode"
            and getattr(function, "__self__", None) is response):
            encoded.append(function)

    try:
        sys.setprofile(observe_encoding)
        rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("complete", LAUNCH,
            task(limit), response)))
    finally:
        sys.setprofile(previous)
    assert len(encoded) == 0


@pytest.mark.parametrize("phase, response", [
    ("complete", "xxxx"), ("complete", ["private-response"]),
    ("complete", "éxx"), ("request", "private-response"),
], ids=["over-character-cap", "wrong-type", "over-byte-cap", "request-response"])
def test_invalid_response_rejects_before_copying_nested_contracts(monkeypatch, phase, response):
    # Response rejection should not spend work snapshotting the task's nested models.
    observed = []
    original = protocol._model_fields

    def observe_fields(model, expected):
        if expected is not WorkerTask:
            observed.append(expected)
        return original(model, expected)

    monkeypatch.setattr(protocol, "_model_fields", observe_fields)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn(phase, LAUNCH, task(3), response)))
    assert len(observed) == 0


@pytest.mark.parametrize("field, contract", [
    ("input_artifacts", ArtifactRef), ("tool_capabilities", ToolCapabilityRef),
])
def test_empty_collection_growth_before_element_snapshot_is_rejected(monkeypatch, field, contract):
    # Re-reading the initial count after context conversion would follow this new item.
    h = "sha256:" + "a" * 64
    if contract is ArtifactRef:
        added = ArtifactRef(digest=h, size_bytes=0, artifact_type="text", media_type="text/plain")
    else:
        added = ToolCapabilityRef(grant_id="grant", tool_id="tool", scope_hash=h)
    source = []
    unsafe = task().model_copy(update={field: source})
    converted = []
    original = protocol._model_fields

    def observe_fields(model, expected):
        snapshot = original(model, expected)
        if expected is AttemptContext:
            source.append(added)
        elif expected is contract:
            converted.append(model)
        return snapshot

    monkeypatch.setattr(protocol, "_model_fields", observe_fields)
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, unsafe)))
    assert len(converted) == 0


@pytest.mark.parametrize("limit, response, content", [
    (1, "", b""), (1, "x", b"x"), (2, "é", b"\xc3\xa9"),
    (3, "éx", b"\xc3\xa9x"), (4, "\U0001f642", b"\xf0\x9f\x99\x82"),
], ids=["empty", "one-byte", "two-byte", "mixed-byte", "four-byte"])
def test_completion_preserves_exact_utf8_content_at_response_limit(limit, response, content):
    # Character admission must still accept exact byte boundaries, including empty output.
    turn = GeneralWorkerTurn("complete", LAUNCH, task(limit), response)
    payload = encode_general_worker_turn(turn)
    assert decode_general_worker_turn(payload) == turn
    result = child(payload)
    assert (result.returncode, result.stderr) == (0, b"")
    decoded = decode_general_worker_proposal(result.stdout, turn)
    assert decoded.blobs[0].content == content


@pytest.mark.parametrize("limit, response", [
    (2, "éx"), (4, "\U0001f642x"), (1, "\ud800"), (3, "x\udfff"),
], ids=["two-byte-over", "four-byte-over", "high-surrogate", "low-surrogate"])
def test_completion_rejects_exact_utf8_overflow_and_short_surrogates(limit, response):
    # Strings within the character cap still require a strict exact UTF-8 byte check.
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("complete", LAUNCH,
        task(limit), response)))


# Handwritten v1 bytes; the commitment below was calculated independently from these bytes.
GOLDEN_TASK = (
    b'{"context":{"agent_instance_id":"agent-1","attempt_id":"attempt-1",'
    b'"effective_config_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"fencing_generation":18446744073709551616,"graph_version":18446744073709551616,'
    b'"input_manifest_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"node_id":"node-1",'
    b'"planning_contract_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"policy_manifest_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"registry_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"routing_decision_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"run_id":"run-1"},"input_artifacts":[{"artifact_type":"text",'
    b'"digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    b'"media_type":"text/plain","size_bytes":18446744073709551616}],'
    b'"output_byte_limit":3,"role":"coder","task_text":"\xc3\xa9\\r\\n",'
    b'"tool_capabilities":[{"grant_id":"grant-a",'
    b'"scope_hash":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
    b'"tool_id":"tool"},{"grant_id":"grant-z",'
    b'"scope_hash":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
    b'"tool_id":"tool"}]}'
)


@pytest.mark.parametrize("collection", [list, tuple])
@pytest.mark.parametrize("phase, response, prefix", [
    ("request", None, b'{"launch_id":"4dc48974-60de-476b-8828-e6d979729813",'
        b'"phase":"request","response_text":null,"schema":"maestro.general-worker-turn.v1","task":'),
    ("complete", "é", b'{"launch_id":"4dc48974-60de-476b-8828-e6d979729813",'
        b'"phase":"complete","response_text":"\xc3\xa9","schema":"maestro.general-worker-turn.v1","task":'),
], ids=["request", "complete"])
def test_v1_literal_canonical_bytes_and_commitment_survive_collection_shape_and_wide_integers(
    collection, phase, response, prefix,
):
    # Capping any formerly unbounded integer or changing v1 bytes/ordering breaks this fixture.
    context = task().context.model_copy(update={"fencing_generation": 18_446_744_073_709_551_616,
        "graph_version": 18_446_744_073_709_551_616})
    artifact = ArtifactRef(digest="sha256:" + "b" * 64, size_bytes=18_446_744_073_709_551_616,
        artifact_type="text", media_type="text/plain")
    refs = [ToolCapabilityRef(grant_id=name, tool_id="tool", scope_hash="sha256:" + "c" * 64)
        for name in ("grant-a", "grant-z")]
    model = task(3).model_copy(update={"context": context, "task_text": "é\r\n",
        "input_artifacts": collection([artifact]), "tool_capabilities": collection(refs)})
    turn = GeneralWorkerTurn(phase, LAUNCH, model, response)
    payload = encode_general_worker_turn(turn)
    assert payload == prefix + GOLDEN_TASK + b"}"
    assert encode_general_worker_turn(decode_general_worker_turn(payload)) == payload
    result = child(payload)
    assert (result.returncode, result.stderr) == (0, b"")
    proposal = decode_general_worker_proposal(result.stdout, turn)
    if phase == "request":
        assert proposal.input_commitment == "sha256:652fd807a87bf53ec9495c3c4ed5e7e4ea9568fe83ef57eeb888b12c6f261aac"
    else:
        assert proposal.blobs[0].content == b"\xc3\xa9"


@pytest.mark.parametrize("contract, field, valid, invalid", [
    pytest.param(WorkerTask, "role", "x" * 64, "x" * 65, id="role-length"),
    pytest.param(WorkerTask, "task_text", "x" * 100_000, "x" * 100_001, id="task-length"),
    pytest.param(WorkerTask, "output_byte_limit", 1, 0, id="output-minimum"),
    pytest.param(WorkerTask, "output_byte_limit", 1_073_741_824, 1_073_741_825, id="output-maximum"),
    *[pytest.param(AttemptContext, name, "x" * 128, "x" * 129, id=name + "-length")
        for name in ("run_id", "node_id", "attempt_id", "agent_instance_id")],
    *[pytest.param(AttemptContext, name, "sha256:" + "a" * 64, "sha256:" + "a" * 65,
        id=name + "-length") for name in ("input_manifest_hash", "effective_config_hash",
        "registry_hash", "policy_manifest_hash", "routing_decision_hash", "planning_contract_hash")],
    pytest.param(AttemptContext, "fencing_generation", 1, 0, id="fencing-minimum"),
    pytest.param(AttemptContext, "graph_version", 0, -1, id="graph-minimum"),
    pytest.param(ArtifactRef, "digest", "sha256:" + "b" * 64, "sha256:" + "b" * 65, id="digest-length"),
    pytest.param(ArtifactRef, "size_bytes", 0, -1, id="artifact-size-minimum"),
    pytest.param(ArtifactRef, "artifact_type", "x" * 64, "x" * 65, id="artifact-type-length"),
    pytest.param(ArtifactRef, "media_type", "x" * 128, "x" * 129, id="media-length"),
    pytest.param(ToolCapabilityRef, "grant_id", "x" * 128, "x" * 129, id="grant-length"),
    pytest.param(ToolCapabilityRef, "tool_id", "x" * 128, "x" * 129, id="tool-length"),
    pytest.param(ToolCapabilityRef, "scope_hash", "sha256:" + "c" * 64, "sha256:" + "c" * 65,
        id="scope-length"),
])
def test_existing_scalar_boundaries_accept_edge_and_reject_first_invalid_value(
    contract, field, valid, invalid,
):
    # Every pair protects an existing admission boundary when model validation is bypassed.
    h = "sha256:" + "a" * 64
    if contract is WorkerTask:
        base, target = task(), None
    elif contract is AttemptContext:
        base, target = task().context, "context"
    elif contract is ArtifactRef:
        base = ArtifactRef(digest=h, size_bytes=0, artifact_type="text", media_type="text/plain")
        target = "input_artifacts"
    else:
        base = ToolCapabilityRef(grant_id="grant", tool_id="tool", scope_hash=h)
        target = "tool_capabilities"
    for scalar, accepted in ((valid, True), (invalid, False)):
        model = base.model_copy(update={field: scalar})
        if target is not None:
            model = task().model_copy(update={target: model if target == "context" else (model,)})
        turn = GeneralWorkerTurn("request", LAUNCH, model)
        if accepted:
            assert decode_general_worker_turn(encode_general_worker_turn(turn)).task == model
        else:
            rejected(lambda: encode_general_worker_turn(turn))


@pytest.mark.parametrize("field, count", [("input_artifacts", 128), ("tool_capabilities", 64)])
@pytest.mark.parametrize("collection", [list, tuple])
def test_existing_collection_count_boundary_remains_accepted(field, count, collection):
    # Enforcing the cap before copying must preserve the exact allowed maximum.
    h = "sha256:" + "a" * 64
    if field == "input_artifacts":
        refs = [ArtifactRef(digest=f"sha256:{index:064x}", size_bytes=0,
            artifact_type="text", media_type="text/plain") for index in range(count)]
    else:
        refs = [ToolCapabilityRef(grant_id=f"grant-{index:03}", tool_id="tool", scope_hash=h)
            for index in range(count)]
    model = task().model_copy(update={field: collection(refs)})
    payload = encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, model))
    assert len(json.loads(payload)["task"][field]) == count
    assert len(getattr(decode_general_worker_turn(payload).task, field)) == count


@pytest.mark.parametrize("control", [KeyboardInterrupt("private-control"),
    SystemExit("private-control"), GeneratorExit("private-control")],
    ids=["keyboard-interrupt", "system-exit", "generator-exit"])
def test_encoding_preserves_baseexception_control_identity(monkeypatch, control):
    # Wrapping BaseException as a protocol error would swallow a host control signal.
    def interrupt(value):
        raise control

    monkeypatch.setattr(protocol, "_canonical", interrupt)
    with pytest.raises(type(control)) as caught:
        encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, task()))
    assert caught.value is control


@pytest.mark.parametrize("launch_id", [LAUNCH.upper(), "4dc48974-60de-576b-8828-e6d979729813",
    "urn:uuid:" + LAUNCH, "{" + LAUNCH + "}", LAUNCH.replace("-", "")],
    ids=["uppercase", "version-five", "urn", "braces", "compact"])
def test_launch_identity_requires_exact_canonical_uuid_four(launch_id):
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", launch_id, task())))


@pytest.mark.parametrize("field", ["input_artifacts", "tool_capabilities"])
def test_constructor_bypass_cannot_admit_duplicate_artifact_or_grant_identity(field):
    h = "sha256:" + "a" * 64
    if field == "input_artifacts":
        ref = ArtifactRef(digest=h, size_bytes=0, artifact_type="text", media_type="text/plain")
    else:
        ref = ToolCapabilityRef(grant_id="grant", tool_id="tool", scope_hash=h)
    model = task().model_copy(update={field: (ref, ref)})
    rejected(lambda: encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, model)))


def test_artifact_input_order_is_preserved_in_wire_bytes():
    refs = tuple(ArtifactRef(digest="sha256:" + digit * 64, size_bytes=0,
        artifact_type="text", media_type="text/plain") for digit in ("b", "a"))
    model = task().model_copy(update={"input_artifacts": refs})
    payload = encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, model))
    assert [ref["digest"] for ref in json.loads(payload)["task"]["input_artifacts"]] == [
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]


def test_turn_schema_version_remains_exact_on_decode():
    value = json.loads(encode_general_worker_turn(GeneralWorkerTurn("request", LAUNCH, task())))
    value["schema"] = "maestro.general-worker-turn.v2"
    rejected(lambda: decode_general_worker_turn(wire(value)))
