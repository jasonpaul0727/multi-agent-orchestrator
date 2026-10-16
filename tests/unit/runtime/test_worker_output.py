"""Pure candidate-byte decoding; no execution or publication authority."""

import base64
import hashlib
import json
import ast
import builtins
import inspect
import os
from pathlib import Path
import socket
import subprocess
import traceback
from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

import orchestrator.runtime.worker_output as output_module

from orchestrator.runtime.contracts import AttemptContext, ArtifactRef, ToolCapabilityRef, WorkerTask
from orchestrator.runtime.worker_output import decode_worker_output, WorkerOutputError

HASH = "sha256:" + "a" * 64
ABC = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def task(limit=16_384):
    context = AttemptContext(
        run_id="run-1", node_id="node-1", attempt_id="attempt-1",
        agent_instance_id="agent-1", fencing_generation=2, graph_version=3,
        input_manifest_hash=HASH, effective_config_hash=HASH, registry_hash=HASH,
        policy_manifest_hash=HASH, routing_decision_hash=HASH, planning_contract_hash=HASH,
    )
    return WorkerTask(context=context, role="coder", task_text="private-task-sentinel",
                      input_artifacts=(), tool_capabilities=(), output_byte_limit=limit)


def envelope(t, contents=(b"abc",)):
    refs, blobs = [], []
    for content in contents:
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        refs.append(dict(digest=digest, size_bytes=len(content),
                         artifact_type="source", media_type="application/octet-stream"))
        blobs.append(dict(digest=digest, content_base64=base64.b64encode(content).decode("ascii")))
    return dict(schema="maestro.worker-output.v1", result=dict(
        context=t.context.model_dump(mode="json"), result_id="result-1",
        outcome="candidate", artifacts=refs, failure_code=None), blobs=blobs)


def wire(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")


def rejected(payload, t, code="worker_output_invalid"):
    with pytest.raises(WorkerOutputError) as caught:
        decode_worker_output(payload, t)
    error = caught.value
    assert error.code == code
    assert error.args == (code,)
    assert error.__cause__ is None and error.__context__ is None
    return error


def test_fixed_vector():
    t = task()
    value = envelope(t)
    assert value["blobs"] == [{"digest": ABC, "content_base64": "YWJj"}]
    assert value["result"]["artifacts"][0]["size_bytes"] == 3
    decoded = decode_worker_output(wire(value), t)
    assert decoded.result.context == t.context
    assert decoded.blobs[0].artifact.digest == ABC
    assert decoded.blobs[0].content == b"abc"
    assert decoded.blobs[0].artifact == decoded.result.artifacts[0]


def test_binary_zero_order_and_immutable_repr():
    t = task()
    contents = (b"", b"\x00\xff\x80", "é\r\n".encode(), b"private-blob-sentinel")
    value = envelope(t, contents)
    value["blobs"].reverse()
    decoded = decode_worker_output(wire(value), t)
    assert tuple(b.artifact.digest for b in decoded.blobs) == tuple(sorted(b["digest"] for b in value["blobs"]))
    assert {b.artifact.digest: b.content for b in decoded.blobs} == {
        "sha256:" + hashlib.sha256(c).hexdigest(): c for c in contents}
    assert tuple(a.digest for a in decoded.result.artifacts) == tuple(a["digest"] for a in value["result"]["artifacts"])
    assert "private-blob-sentinel" not in repr(decoded) + str(decoded)
    assert "private-task-sentinel" not in repr(decoded) + str(decoded)
    with pytest.raises(FrozenInstanceError):
        decoded.blobs = ()
    with pytest.raises(FrozenInstanceError):
        decoded.blobs[0].content = b"changed"


@pytest.mark.parametrize("raw", [b"", b"\xff", b"\xef\xbb\xbf{}", b"[]", b"null",
    b"true", b"{", b'{"schema":1,"schema":2}', b'{"value":NaN}',
    b'{"value":Infinity}', b'{"value":-Infinity}', b'{"value":1e999}',
    b'{"value":"\\ud800"}', b"[" * 2000 + b"]" * 2000])
def test_invalid_frame(raw):
    rejected(raw, task())


class BytesSubclass(bytes):
    pass


@pytest.mark.parametrize("raw", [None, "{}", bytearray(b"{}"), memoryview(b"{}"),
                                  BytesSubclass(b"{}")])
def test_payload_is_exact_bytes(raw):
    rejected(raw, task())


@pytest.mark.parametrize("mutate", [
    lambda v: v.update(schema="other"), lambda v: v.update(extra="raw-sentinel"),
    lambda v: v.pop("schema"), lambda v: v.pop("result"), lambda v: v.pop("blobs"),
    lambda v: v.update(result=[]), lambda v: v.update(blobs={}),
    lambda v: v["result"].update(extra="raw-sentinel"),
    lambda v: v["result"].update(artifacts={}),
    lambda v: v["result"]["context"].update(fencing_generation=True),
    lambda v: v["result"]["context"].update(extra="raw-sentinel"),
    lambda v: v["result"]["artifacts"][0].update(size_bytes=True),
    lambda v: v["result"]["artifacts"][0].update(extra="raw-sentinel"),
    lambda v: v["result"]["artifacts"][0].update(media_type="text/plain\nraw-sentinel"),
    lambda v: v["result"]["artifacts"][0].update(artifact_type="../bad path"),
    lambda v: v["blobs"][0].update(extra="raw-sentinel"),
    lambda v: v["blobs"][0].pop("digest"),
    lambda v: v["blobs"][0].pop("content_base64"),
    lambda v: v["blobs"].__setitem__(0, []),
    lambda v: v["blobs"][0].update(content_base64=3),
])
def test_strict_shape(mutate):
    t = task()
    value = envelope(t)
    mutate(value)
    rejected(wire(value), t)


@pytest.mark.parametrize("path,field,values", [
    ("result", "result_id", [1, True, None, "", "r" * 129, "bad space", "\ud800"]),
    ("context", "run_id", [1, True, None, "", "r" * 129, "bad space", "\ud800"]),
    ("context", "registry_hash", [1, None, "sha256:" + "A" * 64, "sha256:" + "a" * 63]),
    ("artifact", "digest", [1, True, None, "sha256:" + "A" * 64, "md5:" + "a" * 64]),
    ("artifact", "media_type", [1, True, None, "", "m" * 129, "\ud800", "text/plain "]),
    ("artifact", "artifact_type", [1, True, None, "a" * 65]),
    ("blob", "digest", [1, True, None, "sha256:" + "A" * 64, "bad"]),
    ("blob", "content_base64", [True, None, [], "\ud800"]),
])
def test_strict_scalar_and_metadata_types(path, field, values):
    t = task()
    for invalid in values:
        value = envelope(t)
        target = {"result": value["result"], "context": value["result"]["context"],
                  "artifact": value["result"]["artifacts"][0], "blob": value["blobs"][0]}[path]
        target[field] = invalid
        rejected(wire(value), t)


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e999", '"\\ud800"'])
@pytest.mark.parametrize("needle", ['"result_id":"result-1"', '"content_base64":"YWJj"'])
def test_invalid_tree_values_in_valid_shape(token, needle):
    t = task()
    raw = wire(envelope(t)).decode()
    field = needle.split(":", 1)[0]
    rejected(raw.replace(needle, field + ":" + token).encode(), t)


@pytest.mark.parametrize("needle", ['"schema":"maestro.worker-output.v1"',
    '"result_id":"result-1"', '"run_id":"run-1"',
    '"size_bytes":3', '"content_base64":"YWJj"'])
def test_nested_duplicate_keys(needle):
    t = task()
    text = wire(envelope(t)).decode()
    assert text.count(needle) == 1
    rejected(text.replace(needle, needle + "," + needle).encode(), t)


def test_exact_frame_boundary():
    t = task()
    raw = wire(envelope(t))
    for size in (1_048_575, 1_048_576):
        assert decode_worker_output(raw + b" " * (size - len(raw)), t).blobs[0].content == b"abc"
    rejected(raw + b" " * (1_048_577 - len(raw)), t, "worker_output_too_large")


@pytest.mark.parametrize("encoded,content", [("YR==", b"a"), ("YQ", b"a"),
    ("YQ===", b"a"), ("YQ==junk", b"a"), (" YQ==", b"a"), ("YQ==\n", b"a"),
    ("_w==", b"\xff"), ("/w==é", b"\xff"), ("A", b"a"), ("YWJ=", b"ab")])
def test_noncanonical_base64(encoded, content):
    t = task()
    value = envelope(t, (content,))
    value["blobs"][0]["content_base64"] = encoded
    rejected(wire(value), t)


@pytest.mark.parametrize("encoded,content", [("YQ==", b"a"), ("YWI=", b"ab"), ("/w==", b"\xff")])
def test_canonical_padding_accepts(encoded, content):
    t = task()
    value = envelope(t, (content,))
    assert value["blobs"][0]["content_base64"] == encoded
    assert decode_worker_output(wire(value), t).blobs[0].content == content


@pytest.mark.parametrize("limit,n", [(16_384,16_383), (16_384,16_384),
    (1_073_741_824,16_384), (1,1), (5,4), (5,5)])
def test_actual_byte_boundary_accepts(limit,n):
    t = task(limit)
    content = b"x" * n
    assert decode_worker_output(wire(envelope(t,(content,))),t).blobs[0].content == content


@pytest.mark.parametrize("limit,contents", [(16_384,(b"x"*16_385,)),
    (1_073_741_824,(b"x"*16_385,)), (1,(b"xx",)), (5,(b"xxxxxx",)),
    (5,(b"abc",b"def"))])
def test_actual_byte_boundary_rejects(limit,contents):
    t = task(limit)
    rejected(wire(envelope(t,contents)),t,"worker_output_too_large")


@pytest.mark.parametrize("contents", [(b"a", b"bcd"), (b"ab", b"cde")])
def test_aggregate_lower_and_exact_accepts(contents):
    t = task(5)
    decoded = decode_worker_output(wire(envelope(t, contents)), t)
    assert {b.content for b in decoded.blobs} == set(contents)


def test_in_cap_declared_mismatch_is_invalid():
    t = task(5)
    value = envelope(t)
    value["result"]["artifacts"][0]["size_bytes"] = 100
    rejected(wire(value),t)


@pytest.mark.parametrize("huge", [False, True])
def test_deceptive_small_declared_size_cannot_expand_allocation(huge):
    t = task()
    value = envelope(t,(b"x"*16_385,))
    value["result"]["artifacts"][0]["size_bytes"] = 1
    if huge:
        value["blobs"][0]["content_base64"] = "A" * 500_000
    rejected(wire(value),t,"worker_output_too_large")


@pytest.mark.parametrize("outcome,code", [("blocked",None),("failed","worker_failed")])
def test_non_candidate_empty_only(outcome,code):
    t = task()
    value = envelope(t)
    value["result"].update(outcome=outcome,failure_code=code,artifacts=[])
    value["blobs"] = []
    decoded = decode_worker_output(wire(value),t)
    assert decoded.result.outcome == outcome and decoded.blobs == ()
    value["blobs"] = envelope(t)["blobs"]
    rejected(wire(value),t)


@pytest.mark.parametrize("outcome", ["blocked", "failed"])
@pytest.mark.parametrize("with_blobs", [False, True])
def test_non_candidate_references_forbidden(outcome, with_blobs):
    t = task()
    value = envelope(t)
    value["result"].update(outcome=outcome, failure_code="worker_failed" if outcome == "failed" else None)
    if not with_blobs:
        value["blobs"] = []
    rejected(wire(value), t)


@pytest.mark.parametrize("outcome,code", [("failed", None), ("failed", 1),
    ("failed", "bad space"), ("failed", "x" * 65), ("candidate", "worker_failed"),
    ("blocked", "worker_failed")])
def test_failure_code_rules(outcome, code):
    t = task()
    value = envelope(t)
    value["result"].update(outcome=outcome, failure_code=code)
    if outcome != "candidate":
        value["result"]["artifacts"] = []
        value["blobs"] = []
    rejected(wire(value), t)


def test_missing_failed_code():
    t = task()
    value = envelope(t)
    value["result"].update(outcome="failed", artifacts=[])
    value["result"].pop("failure_code")
    value["blobs"] = []
    rejected(wire(value), t)


@pytest.mark.parametrize("case", ["empty", "missing", "extra", "duplicate_blob",
                                  "duplicate_ref", "wrong_bytes", "wrong_hash", "zero_size", "zero_hash"])
def test_candidate_mapping_integrity(case):
    t = task()
    value = envelope(t, (b"",) if case.startswith("zero") else (b"abc",))
    if case == "empty":
        value["result"]["artifacts"] = []
        value["blobs"] = []
    elif case == "missing":
        value["blobs"] = []
    elif case == "extra":
        value["blobs"].extend(envelope(t, (b"extra",))["blobs"])
    elif case == "duplicate_blob":
        value["blobs"] *= 2
    elif case == "duplicate_ref":
        value["result"]["artifacts"] *= 2
    elif case == "wrong_bytes":
        value["blobs"][0]["content_base64"] = "YWJk"
    elif case in ("wrong_hash", "zero_hash"):
        value["result"]["artifacts"][0]["digest"] = HASH
        value["blobs"][0]["digest"] = HASH
    else:
        value["result"]["artifacts"][0]["size_bytes"] = 1
    rejected(wire(value), t)


@pytest.mark.parametrize("count", [128, 129])
def test_unique_blob_count_boundary(count):
    t = task()
    contents = tuple(i.to_bytes(2, "big") for i in range(count))
    raw = wire(envelope(t, contents))
    if count == 129:
        rejected(raw, t)
    else:
        decoded = decode_worker_output(raw, t)
        assert len(decoded.blobs) == 128
        assert {b.content for b in decoded.blobs} == set(contents)


def test_key_and_blob_order_preserves_mapping():
    t = task()
    value = envelope(t, (b"a", b"bc"))
    def reverse_keys(item):
        if isinstance(item, dict):
            return {key: reverse_keys(item[key]) for key in reversed(item)}
        if isinstance(item, list):
            return [reverse_keys(child) for child in item]
        return item
    reordered = reverse_keys(value)
    reordered["blobs"].reverse()
    assert decode_worker_output(wire(reordered), t) == decode_worker_output(wire(value), t)


@pytest.mark.parametrize("reverse", [False,True])
@pytest.mark.parametrize("case,expected", [
    ("malformed_and_actual_overflow","worker_output_invalid"),
    ("declared_mismatch_and_actual_overflow","worker_output_too_large"),
    ("malformed_and_encoded_overflow","worker_output_too_large"),
])
def test_cross_blob_precedence_is_order_independent(reverse,case,expected):
    t = task(1)
    value = envelope(t,(b"a",b"bc"))
    if case == "malformed_and_actual_overflow":
        value["blobs"][0]["content_base64"] = "A"
        assert all(len(b["content_base64"]) <= 4 for b in value["blobs"])
    elif case == "declared_mismatch_and_actual_overflow":
        value["result"]["artifacts"][0]["size_bytes"] = 100
    else:
        value["blobs"][0]["content_base64"] = "A"
        value["blobs"][1]["content_base64"] = "AAAAAAAA"
    if reverse:
        value["blobs"].reverse()
    rejected(wire(value),t,expected)


@pytest.mark.parametrize("field,value", [("output_byte_limit",True), ("output_byte_limit",0),
    ("output_byte_limit",1_073_741_825), ("task_text","\ud800"), ("task_text"," "),
    ("input_artifacts","not-array"), ("tool_capabilities","not-array"),
    ("unexpected_control_handle","private-extra-sentinel")])
def test_unsafe_task_copy(field,value):
    original = task()
    unsafe = original.model_copy(update={field:value})
    rejected(wire(envelope(original)),unsafe)


@pytest.mark.parametrize("field", list(AttemptContext.model_fields))
def test_full_attempt_binding(field):
    t = task()
    value = envelope(t)
    old = value["result"]["context"][field]
    value["result"]["context"][field] = old + 1 if type(old) is int else (
        "sha256:"+"b"*64 if old.startswith("sha256:") else "other-identity")
    rejected(wire(value),t)


def test_input_digest_alias():
    t = task()
    value = envelope(t)
    ref = ArtifactRef.model_validate(value["result"]["artifacts"][0])
    unsafe = t.model_copy(update={"input_artifacts":(ref,)})
    rejected(wire(value),unsafe)


def input_ref(index=0):
    return ArtifactRef(digest="sha256:" + f"{index:064x}", size_bytes=1,
                       artifact_type="input", media_type="application/octet-stream")


def capability(index=0):
    return ToolCapabilityRef(grant_id=f"grant-{index}", tool_id="tool-1", scope_hash=HASH)


@pytest.mark.parametrize("kind", ["missing", "context_type", "none", "mapping", "subclass",
                                  "copied_subclass", "pydantic_extra", "cycle"])
def test_unsafe_task_construction_and_surviving_extras(kind):
    t = task()
    if kind == "missing":
        unsafe = WorkerTask.model_construct(output_byte_limit=16_384)
    elif kind == "context_type":
        unsafe = t.model_copy(update={"context": input_ref()})
    elif kind == "none":
        unsafe = None
    elif kind == "mapping":
        unsafe = t.model_dump()
    elif kind in ("subclass", "copied_subclass"):
        class ExpandedTask(WorkerTask):
            control_handle: str = "private-extra-sentinel"
        unsafe = ExpandedTask(**t.model_dump())
        if kind == "copied_subclass":
            unsafe = unsafe.model_copy(update={"another_handle": "sentinel"})
    elif kind == "pydantic_extra":
        unsafe = t.model_copy()
        object.__setattr__(unsafe, "__pydantic_extra__", {"secret": "private-extra-sentinel"})
    else:
        cycle = []
        cycle.append(cycle)
        unsafe = t.model_copy(update={"input_artifacts": cycle})
    rejected(wire(envelope(t)), unsafe)


@pytest.mark.parametrize("field,value", [("extra", "private-extra-sentinel"),
    ("fencing_generation", True), ("registry_hash", "bad"), ("run_id", "\ud800")])
def test_nested_unsafe_context(field, value):
    t = task()
    unsafe = t.model_copy(update={"context": t.context.model_copy(update={field: value})})
    rejected(wire(envelope(t)), unsafe)


@pytest.mark.parametrize("field,value", [("extra", "private-extra-sentinel"),
    ("size_bytes", True), ("media_type", "bad\n"), ("artifact_type", "../bad"),
    ("media_type", "\ud800")])
def test_nested_unsafe_input_ref(field, value):
    t = task()
    ref = input_ref().model_copy(update={field: value})
    unsafe = t.model_copy(update={"input_artifacts": (ref,)})
    rejected(wire(envelope(t)), unsafe)


@pytest.mark.parametrize("field,value", [("extra", "private-extra-sentinel"),
    ("scope_hash", "bad"), ("tool_id", "\ud800")])
def test_nested_unsafe_capability(field, value):
    t = task()
    ref = capability().model_copy(update={field: value})
    unsafe = t.model_copy(update={"tool_capabilities": (ref,)})
    rejected(wire(envelope(t)), unsafe)


@pytest.mark.parametrize("field,items", [("input_artifacts", (input_ref(),) * 2),
    ("input_artifacts", tuple(input_ref(i) for i in range(129))),
    ("tool_capabilities", (capability(),) * 2),
    ("tool_capabilities", tuple(capability(i) for i in range(65)))])
def test_unsafe_task_collection_bounds_and_uniqueness(field, items):
    t = task()
    rejected(wire(envelope(t)), t.model_copy(update={field: items}))


def test_legitimate_list_normalization_and_fresh_result_links():
    t = task()
    unsafe = t.model_copy(update={"input_artifacts": [input_ref()],
                                "tool_capabilities": [capability(2), capability(1)]})
    decoded = decode_worker_output(wire(envelope(t)), unsafe)
    assert decoded.result.context == t.context
    assert decoded.blobs[0].artifact is decoded.result.artifacts[0]
    assert decoded.result.context is not t.context


def test_discarded_construct_extras_cannot_be_recovered():
    t = task()
    rebuilt = WorkerTask.model_construct(**t.__dict__, discarded="already gone")
    assert "discarded" not in rebuilt.__dict__
    assert decode_worker_output(wire(envelope(t)), rebuilt).blobs[0].content == b"abc"


def test_nested_subclass_declared_extra_rejected():
    class ExpandedContext(AttemptContext):
        control_handle: str = "private-extra-sentinel"
    t = task()
    unsafe = t.model_copy(update={"context": ExpandedContext(**t.context.model_dump())})
    rejected(wire(envelope(t)), unsafe)


def test_untrusted_model_serialization_not_used():
    class NoSerializationTask(WorkerTask):
        def model_dump(self, *args, **kwargs):
            raise AssertionError("unexpected serialization")
        def model_dump_json(self, *args, **kwargs):
            raise AssertionError("unexpected serialization")
    t = task()
    copied = NoSerializationTask(**t.model_dump())
    assert decode_worker_output(wire(envelope(t)), copied).blobs[0].content == b"abc"


def private_error(error, sentinel):
    assert sentinel not in repr(error) + str(error) + str(error.args)
    assert sentinel not in "".join(traceback.format_exception(error))
    assert error.__context__ is None and error.__cause__ is None


@pytest.mark.parametrize("target", ["_revalidate_task","_decode_base64","_digest_bytes"])
def test_owned_raw_exception_is_not_retained(monkeypatch,target):
    sentinel = "raw-secret-sentinel-for-owned-exception"
    class RawError(ValueError):
        code = sentinel
    def fail(*args,**kwargs):
        raise RawError(sentinel)
    t = task()
    raw = wire(envelope(t))
    monkeypatch.setattr(output_module,target,fail)
    private_error(rejected(raw,t), sentinel)


def test_error_constructor_does_not_preserve_opaque_code():
    error = WorkerOutputError("opaque-secret-sentinel")
    assert error.code == "worker_output_invalid"
    assert error.args == ("worker_output_invalid",)


def test_error_constructor_does_not_invoke_hostile_codes():
    calls = []
    class Hostile:
        def __eq__(self,other):
            calls.append("eq")
            raise AssertionError("untrusted equality")
        def __str__(self):
            calls.append("str")
            raise AssertionError("untrusted stringification")
        def __repr__(self):
            calls.append("repr")
            raise AssertionError("untrusted repr")
    class HostileString(str):
        __eq__ = Hostile.__eq__
        __str__ = Hostile.__str__
        __repr__ = Hostile.__repr__
    for code in (Hostile(),HostileString("worker_output_too_large")):
        error = WorkerOutputError(code)
        assert error.code == "worker_output_invalid"
        assert error.args == ("worker_output_invalid",)
    assert calls == []


@pytest.mark.parametrize("code", ["worker_output_invalid", "worker_output_too_large"])
def test_safe_constructor_codes(code):
    error = WorkerOutputError(code)
    assert error.code == code and error.args == (code,)


@pytest.mark.parametrize("source", ["validation", "base64", "hash", "json", "mutated_error"])
def test_injected_exception_paths_detach_raw_details(monkeypatch, source):
    sentinel = "raw-exception-private-sentinel"
    t = task()
    raw = wire(envelope(t))
    if source == "validation":
        def fail(*args, **kwargs):
            AttemptContext.model_validate({"run_id": sentinel})
        owner, name = output_module, "_revalidate_task"
    elif source == "mutated_error":
        error = WorkerOutputError("worker_output_too_large")
        error.code = sentinel
        error.args = (sentinel,)
        def fail(*args, **kwargs):
            raise error
        owner, name = output_module, "_decode_base64"
    else:
        def fail(*args, **kwargs):
            if source == "json":
                raise json.JSONDecodeError(sentinel, sentinel, 0)
            if source == "base64":
                raise base64.binascii.Error(sentinel)
            raise RuntimeError(sentinel)
        owner, name = {"base64": (output_module.base64, "b64decode"),
                       "hash": (output_module.hashlib, "sha256"),
                       "json": (output_module.json, "loads")}[source]
    monkeypatch.setattr(owner, name, fail)
    private_error(rejected(raw, t), sentinel)


@pytest.mark.parametrize("source", ["payload", "nested_task"])
def test_real_invalid_inputs_do_not_retain_secret_details(source):
    sentinel = "real-invalid-private-sentinel"
    t = task()
    if source == "payload":
        raw = ("{" + sentinel).encode()
    else:
        raw = wire(envelope(t))
        t = t.model_copy(update={"context": t.context.model_copy(update={"run_id": sentinel + " space"})})
    private_error(rejected(raw, t), sentinel)


@pytest.mark.parametrize("failure", [KeyboardInterrupt, SystemExit])
def test_process_control_exceptions_propagate(monkeypatch, failure):
    t = task()
    raw = wire(envelope(t))
    def fail(*args):
        raise failure()
    monkeypatch.setattr(output_module, "_revalidate_task", fail)
    with pytest.raises(failure):
        decode_worker_output(raw, t)


def test_direct_blob_privacy_slots_and_nested_model_immutability():
    t = task()
    decoded = decode_worker_output(wire(envelope(t, (b"private-blob-sentinel",))), t)
    blob = decoded.blobs[0]
    assert "private-blob-sentinel" not in repr(blob) + str(blob)
    assert not hasattr(blob, "__dict__") and not hasattr(decoded, "__dict__")
    with pytest.raises(ValidationError):
        blob.artifact.size_bytes = 0
    with pytest.raises(ValidationError):
        decoded.result.outcome = "blocked"


def test_decoding_attempts_no_io(monkeypatch):
    t = task()
    raw = wire(envelope(t))
    invalid = b"malformed-private-sentinel"
    calls = []
    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("decoder attempted I/O")
    with monkeypatch.context() as guarded:
        guarded.setattr(builtins, "open", forbidden)
        for name in ("open", "read_bytes", "read_text", "write_bytes", "write_text"):
            guarded.setattr(Path, name, forbidden)
        guarded.setattr(socket, "socket", forbidden)
        guarded.setattr(subprocess, "Popen", forbidden)
        guarded.setattr(os, "system", forbidden)
        assert decode_worker_output(raw, t).blobs[0].content == b"abc"
        rejected(invalid, t)
        assert calls == []


def test_import_surface_is_data_only():
    tree = ast.parse(inspect.getsource(output_module))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append("." * node.level + (node.module or ""))
    assert set(imports) <= {"__future__", "base64", "hashlib", "json", "math", "re",
                            "dataclasses", ".contracts", "orchestrator.validation"}


def test_valid_frame_still_requires_exact_bytes_and_no_bom():
    t = task()
    raw = wire(envelope(t))
    for invalid in (BytesSubclass(raw), bytearray(raw), memoryview(raw), raw.decode(),
                    b"\xef\xbb\xbf" + raw):
        rejected(invalid, t)


@pytest.mark.parametrize("field", ["context", "result_id", "outcome", "artifacts"])
def test_missing_required_result_field(field):
    t = task()
    value = envelope(t)
    value["result"].pop(field)
    rejected(wire(value), t)


@pytest.mark.parametrize("schema", [None, True, 1, [], {}])
def test_schema_requires_exact_string(schema):
    t = task()
    value = envelope(t)
    value["schema"] = schema
    rejected(wire(value), t)


@pytest.mark.parametrize("kind", ["context", "artifact", "capability"])
def test_nested_pydantic_extras_survive_and_reject(kind):
    t = task()
    nested = {"context": t.context.model_copy(), "artifact": input_ref(),
              "capability": capability()}[kind]
    object.__setattr__(nested, "__pydantic_extra__", {"control": "private-extra-sentinel"})
    update = {"context": nested} if kind == "context" else {
        "input_artifacts" if kind == "artifact" else "tool_capabilities": (nested,)}
    rejected(wire(envelope(t)), t.model_copy(update=update))


def test_legitimate_nested_plain_mappings_are_rebuilt():
    t = task()
    copied = t.model_copy(update={"context": t.context.model_dump(),
                                 "input_artifacts": [input_ref().model_dump()],
                                 "tool_capabilities": [capability().model_dump()]})
    assert decode_worker_output(wire(envelope(t)), copied).blobs[0].content == b"abc"


@pytest.mark.parametrize("kind", ["context_float", "artifact_negative", "artifact_digest",
                                  "artifact_metadata", "capability_scope", "capability_id"])
def test_field_overriding_nested_subclasses_cannot_weaken_task_contracts(kind):
    class RelaxedContext(AttemptContext):
        graph_version: float
    class RelaxedArtifact(ArtifactRef):
        digest: str
        size_bytes: int
        artifact_type: str
    class RelaxedCapability(ToolCapabilityRef):
        scope_hash: str
        tool_id: str
    t = task()
    value = envelope(t)
    value["result"].update(outcome="blocked", artifacts=[])
    value["blobs"] = []
    if kind == "context_float":
        context = RelaxedContext(**t.context.model_dump())
        assert type(context.graph_version) is float
        # Equality alone would accept 3.0 as equal to the required integer 3.
        assert context.graph_version == value["result"]["context"]["graph_version"]
        update = {"context": context}
    elif kind.startswith("artifact"):
        fields = input_ref().model_dump()
        fields.update({"artifact_negative": {"size_bytes": -1},
                       "artifact_digest": {"digest": "not-a-digest"},
                       "artifact_metadata": {"artifact_type": "bad path"}}[kind])
        update = {"input_artifacts": (RelaxedArtifact(**fields),)}
    else:
        fields = capability().model_dump()
        fields.update({"capability_scope": {"scope_hash": "not-a-digest"},
                       "capability_id": {"tool_id": "bad path"}}[kind])
        update = {"tool_capabilities": (RelaxedCapability(**fields),)}
    rejected(wire(value), t.model_copy(update=update))


@pytest.mark.parametrize("container", [tuple, list])
def test_harmless_nested_subclasses_keep_normalization(container):
    class ContextSubclass(AttemptContext):
        pass
    class ArtifactSubclass(ArtifactRef):
        pass
    class CapabilitySubclass(ToolCapabilityRef):
        pass
    t = task()
    copied = t.model_copy(update={
        "context": ContextSubclass(**t.context.model_dump()),
        "input_artifacts": container([ArtifactSubclass(**input_ref().model_dump())]),
        "tool_capabilities": container([
            CapabilitySubclass(**capability(2).model_dump()),
            CapabilitySubclass(**capability(1).model_dump()),
        ]),
    })
    rebuilt = output_module._revalidate_task(copied)
    assert rebuilt.context == t.context
    assert rebuilt.input_artifacts[0] == input_ref()
    assert tuple(item.grant_id for item in rebuilt.tool_capabilities) == ("grant-1", "grant-2")
    assert type(rebuilt.context) is AttemptContext
    assert type(rebuilt.input_artifacts[0]) is ArtifactRef
    assert all(type(item) is ToolCapabilityRef for item in rebuilt.tool_capabilities)
    assert decode_worker_output(wire(envelope(t)), copied).blobs[0].content == b"abc"


@pytest.mark.parametrize("field,invalid", [("graph_version", 3.0),
    ("fencing_generation", -1), ("registry_hash", "not-a-digest")])
def test_context_subclass_fields_are_revalidated_before_binding(field, invalid):
    class RelaxedContext(AttemptContext):
        graph_version: float
        fencing_generation: int
        registry_hash: str
    t = task()
    fields = t.context.model_dump()
    fields[field] = invalid
    copied = t.model_copy(update={"context": RelaxedContext(**fields)})
    with pytest.raises(ValueError):
        output_module._revalidate_task(copied)
