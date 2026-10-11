# Bounded Worker Output Envelope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode bounded untrusted Worker candidate bytes into immutable host data without publishing bytes, granting execution authority or changing the blocked-only Worker.

**Architecture:** Add one pure `orchestrator.runtime.worker_output` module alongside the existing contracts. Strictly parse one JSON/Base64 envelope, rebuild the supplied Task, validate actual content and full result binding, and return immutable sorted blob data. Error exits detach owned raw exception chains; existing contracts and process transport remain unchanged.

**Tech Stack:** Python >=3.12, existing Pydantic >=2.7,<3, standard-library JSON/Base64/SHA-256/dataclasses, pytest and coverage. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-10-p5-worker-output-envelope-design.md` (independently reviewed design delivered at `720f1507c2056424703008674e1c070bd48a5773`).

**Authorization and status:** Prepared under the human's explicit self-plan/review-next-task/proceed mandate, with Subagent-driven already selected; this is not a new written-spec human approval. Independent design and amended-plan reviews completed before implementation. Source implementation, exact-source acceptance and final source review are complete; documentation review and controller delivery verification remain pending. Push each completed reviewed stage to the existing `codex/p3-systemd-termination-receipts` branch; no main merge, PR, deployment or publication authority.

**Whole-slice BASE:** `720f1507c2056424703008674e1c070bd48a5773` (before this plan and all new implementation/fix commits). Separately capture the actual task pre-dispatch BASE after reviewed plan delivery for the task review; neither review uses HEAD~1.

## Recorded execution status (2026-10-11)

The actual task pre-dispatch BASE was `d34f61c7a0d7f074bce1212852f4a1de7b3ba939`. The exact tested and reviewed source HEAD is `2f51e3fa31b5952da1c504b805756c6d64072790`; final full acceptance measured its frozen bytes on 2026-10-11 before the source commit. This subsequent closure changes only plan/spec status, not source or tests. Its separate docs-only commit identity belongs in the documentation closure report and controller delivery record, not the historical source-test identity.

`task-1-report.md` in the plan-owned ignored workspace records the real RED/GREEN rounds and repaired-source acceptance: 218 decoder tests passed, 543 focused/runtime/live tests passed in 20.12s, and 2,292 full-suite tests passed in 453.63s. Total statement coverage was 90.49%; decoder coverage was 100.00%. There were zero test skips or warnings, and required existing live systemd tests executed. Compile, dependency, whitespace, isolated wheel build, packaged-source equality and isolated-checkout module-path checks passed on the same source bytes. Earlier source measurements are historical; unchanged-source evidence is retained without rerunning or redating tests for documentation.

Independent task review completed. Whole-slice source review identified one Important nested-subclass revalidation issue; the source fix above and scoped final fix re-review addressed it with no new Critical/Important breakage. Steps 1–6 are satisfied. Step 7 remains open only for independent documentation review and controller push/actual remote equality/clean-state delivery verification; its exact-source acceptance and final source-review portions are complete. Neither Task 1 delivery nor whole-slice delivery is declared complete before those remaining gates. This records only the pure decoder prerequisite, not publisher/transport/functional Worker, paid-Provider or full V1 acceptance.

## Global Constraints

- Keep existing `WorkerTask`, `WorkerResult`, legacy `decode_worker_result()` and production blocked-only transport unchanged.
- `decode_worker_output(payload: bytes, task: WorkerTask) -> DecodedWorkerOutput` accepts exact bytes, not a bytearray/string/stream/path.
- The payload must be nonempty and at most 1,048,576 bytes before parsing.
- The new wire object has exactly three required fields: `schema`, `result`, `blobs`; schema is `maestro.worker-output.v1`.
- No additional keys are accepted at any new-object or existing-model nesting level. Exact JSON scalar types are required; bool is not an integer. Duplicate keys, non-finite values, invalid UTF-8, UTF-8 BOM, malformed JSON, wrong schema and oversized frames reject.
- For `candidate`, require one to 128 unique ArtifactRefs and one to 128 unique blob objects. The digest sets must match exactly, with neither missing nor extra blobs.
- For `blocked` or `failed`, both `result.artifacts` and `blobs` must be empty. Preserve the existing result contract's failure-code rules.
- Base64 is standard RFC 4648 with required padding where applicable, ASCII only, no whitespace, URL-safe characters or ignored suffix.
- Bound every encoded string before decoding: canonical encoded length cannot exceed `4 * ceil(min(task.output_byte_limit, 16_384) / 3)`.
- Enforce the sum of all decoded bytes at most `min(task.output_byte_limit, 16_384)` as well as existing declared-size validation.
- Compare the actual decoded size with the corresponding ArtifactRef size and SHA-256 digest computed by the host.
- Revalidate the supplied task recursively before use, including unsafe `model_construct`/`model_copy` values. Reject surviving unknown fields before a field-only rebuild can discard them. Do not modify the shared revalidation helper or existing contracts for this task.
- Well-typed payloads exceeding an actual byte cap must yield `worker_output_too_large`. A declared/actual size mismatch within byte caps yields `worker_output_invalid`; malformed encoding remains invalid unless a preliminary frame/encoded-length cap has already rejected it.
- Sort blobs by digest for deterministic mapping; preserve the result's validated reference sequence. Content bytes are excluded from incidental repr/str. Do not include task text in returned data.
- `WorkerOutputError` exposes only trusted literal `worker_output_invalid` or `worker_output_too_large`. Raise the sanitized error after leaving owned exception handlers; `from None` alone does not detach context.
- It reads no files and performs no I/O. No convenience method publishes, runs Verifier, records a proposal, completes an Attempt, settles usage, writes an event or dispatches a child.
- Full suite passes with at least 90% total coverage and no skipped required existing real systemd isolation test; compileall/pip check/wheel/diff checks, focused and full exact-source evidence, independent task and whole-slice reviews, then stage push with actual remote/clean verification.
- No credentials, paid Provider calls, permission expansion, main-workspace edits, destructive cleanup, shared-venv installation, deployment or merge. Preserve this plan's ignored ledger/reports for restart as requested by the user.

## Review Focus

1. A copied Task with an extra nested object field must reject before field-only rebuilding drops that field; Step 4 tests all Task nesting levels.
2. A legitimate Task whose output limit is greater than 16,384 must not expand this decoder's fixed profile; Step 3 pins 16,383/16,384/16,385 and smaller Task caps.
3. JSON numeric overflow `1e999` and escaped lone surrogates must reject rather than bypass strict UTF-8/non-finite validation; Step 2 tests raw numeric and escaped string vectors.
4. An internal exception with sentinel args and a malicious `code` attribute must not survive through public error objects or chains; Step 5 injects validation/hash/Base64 failures and inspects raw chains as well as formatted traces.
5. Two equal bytes encoded with alternate Base64 pad bits must not produce two accepted wire forms; Step 3 rejects `YR==` versus canonical `YQ==` and `YWJ=` versus `YWI=`.

All five are owned by the single task below; review-focus entries are not deferred tests.

## File map, task boundary and environment

One cohesive task owns the pure decoder, its security tests and its user-facing contract documentation. Parser, byte validation, Task revalidation and safe errors are not independently shippable permissive states; each RED/GREEN round below is an internal task step, not a separately approved feature.

Create `src/orchestrator/runtime/worker_output.py` and `tests/unit/runtime/test_worker_output.py`. Modify only `docs/security/worker-runtime.md` to document this data-only API. Do not change runtime `__init__.py`, legacy contracts, `validation.py`, ArtifactStore, application/Scheduler, sender/Worker scripts, launcher budgets or existing tests to hide regressions. Import the new API directly from its module. Tracked plan/spec status bookkeeping may be updated only after the associated evidence exists.

Existing isolated checkout (Windows): `C:/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`.
WSL checkout: `/mnt/c/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`.
Run WSL Git and Python with the checkout as cwd. Existing Python: `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`. Prefix Python commands with `env PYTHONPATH=src:.` so probes/build verification do not resolve the editable main checkout. Pytest additionally has `pythonpath = ["src"]` in pyproject.

Ledger: `.superpowers/sdd/2026-10-10-p5-worker-output-envelope/progress.md`. Resolve this exact workspace using the SDD script and check live children before dispatch; the previous input plan is complete and must not be replayed.

### Task 1: Strict candidate-byte decoder and acceptance evidence

**Files:**
- Create: `src/orchestrator/runtime/worker_output.py`.
- Test: `tests/unit/runtime/test_worker_output.py`.
- Modify: `docs/security/worker-runtime.md` (new pure-output contract section; retain historical measurements as historical).

**Interfaces:**
- Consumes unchanged `AttemptContext`, `ArtifactRef`, `WorkerTask`, `WorkerResult`, `validate_worker_result(task: WorkerTask, result: WorkerResult) -> None` from `orchestrator.runtime.contracts` and `revalidate_model(model_type, value)` from `orchestrator.validation` only after extra-field rejection.
- Produces `decode_worker_output(payload: bytes, task: WorkerTask) -> DecodedWorkerOutput`.
- Produces frozen, slotted dataclasses `DecodedCandidateBlob(artifact: ArtifactRef, content: bytes)` with `content = field(repr=False)`, and `DecodedWorkerOutput(result: WorkerResult, blobs: tuple[DecodedCandidateBlob, ...])`.
- Produces `WorkerOutputError(ValueError)` with safe `code` and args; no raw validation detail. Any unsupported constructor code maps to `worker_output_invalid`; check exact string type before comparing allowed literal values, without invoking arbitrary input equality/stringification.
- Private helper names for fault-injection tests: `_revalidate_task(task: WorkerTask) -> WorkerTask`, `_decode_base64(encoded: str) -> bytes`, `_digest_bytes(content: bytes) -> str`. They grant no authority and are not public adapters.

- [x] **Step 1: Write fixed-vector and immutable-data RED tests, then establish the new API.**

Use these complete fixture helpers in the new test file; do not import another test module's private fixtures:

```python
import base64
import hashlib
import json
from dataclasses import FrozenInstanceError
import pytest
from orchestrator.runtime.contracts import AttemptContext, ArtifactRef, WorkerTask
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
```

Run `env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/runtime/test_worker_output.py -x`. Initially expect missing-module collection failure, not a failure in fixture construction. Record exact command, failure reason and source state. Introduce the dataclasses/API and minimal decoding for the pinned vectors, then run these tests GREEN; this intermediate implementation is not committed or approved for delivery. Every later round first adds its rejecting/boundary tests and records their genuine behavior failures before implementing them.

- [x] **Step 2: RED/GREEN strict frame/JSON/model validation.**

Add tests using these vectors and mutation examples:

```python
@pytest.mark.parametrize("raw", [b"", b"\xff", b"\xef\xbb\xbf{}", b"[]", b"null",
    b"true", b"{", b'{"schema":1,"schema":2}', b'{"value":NaN}',
    b'{"value":Infinity}', b'{"value":-Infinity}', b'{"value":1e999}',
    b'{"value":"\\ud800"}', b"[" * 2000 + b"]" * 2000])
def test_invalid_frame(raw):
    rejected(raw, task())

@pytest.mark.parametrize("raw", [None, "{}", bytearray(b"{}"), memoryview(b"{}")])
def test_payload_is_exact_bytes(raw):
    rejected(raw, task())

@pytest.mark.parametrize("mutate", [
    lambda v: v.update(schema="other"), lambda v: v.update(extra="raw-sentinel"),
    lambda v: v.pop("blobs"), lambda v: v.update(blobs={}),
    lambda v: v["result"].update(extra="raw-sentinel"),
    lambda v: v["result"]["context"].update(fencing_generation=True),
    lambda v: v["result"]["context"].update(extra="raw-sentinel"),
    lambda v: v["result"]["artifacts"][0].update(size_bytes=True),
    lambda v: v["result"]["artifacts"][0].update(extra="raw-sentinel"),
    lambda v: v["result"]["artifacts"][0].update(media_type="text/plain\nraw-sentinel"),
    lambda v: v["result"]["artifacts"][0].update(artifact_type="../bad path"),
    lambda v: v["blobs"][0].update(extra="raw-sentinel"),
    lambda v: v["blobs"][0].update(content_base64=3),
])
def test_strict_shape(mutate):
    t = task(); value = envelope(t); mutate(value)
    rejected(wire(value), t)

@pytest.mark.parametrize("needle", ['"schema":"maestro.worker-output.v1"',
    '"result_id":"result-1"', '"run_id":"run-1"',
    '"size_bytes":3', '"content_base64":"YWJj"'])
def test_nested_duplicate_keys(needle):
    t = task(); text = wire(envelope(t)).decode()
    assert text.count(needle) == 1
    rejected(text.replace(needle, needle + "," + needle).encode(), t)

def test_exact_frame_boundary():
    t = task(); raw = wire(envelope(t))
    for size in (1_048_575, 1_048_576):
        assert decode_worker_output(raw + b" " * (size - len(raw)), t).blobs[0].content == b"abc"
    rejected(raw + b" " * (1_048_577 - len(raw)), t, "worker_output_too_large")
```

Repeat shape vectors for missing required root/blob fields, root `result` not an object, `artifacts` not an array, strict IDs/digest/media string types, invalid/lowercase hash syntax, oversized metadata and non-finite floats or escaped surrogates inside otherwise valid result/blob objects. Each added parametrization must make an assertion through `rejected` or validated exact output; no assertion-free fuzz loops. Add a bytes subclass to the exact-type test.

Implementation order: check `type(payload) is bytes`, nonempty and frame length before parsing; decode strict UTF-8 and reject BOM; `json.loads` uses duplicate-rejecting `object_pairs_hook` and rejecting `parse_constant`. Traverse the parsed JSON tree to reject non-finite floats (including exponent overflow) and unencodable strings before schema rebuilding. Require exact root keys and schema, exact blob dict keys and strict strings; validate fresh `WorkerResult.model_validate(result_mapping)` rather than legacy decoder exception behavior. Do not turn dictionary insertion order or JSON whitespace into a wire canonicality requirement.

- [x] **Step 3: RED/GREEN canonical content, outcome shapes, matching sets and exact caps.**

```python
@pytest.mark.parametrize("encoded", ["YR==", "YQ", "YQ===", "YQ==junk", " YQ==",
    "YQ==\n", "_w==", "/w==é", "A", "YWJ="])
def test_noncanonical_base64(encoded):
    t = task(); value = envelope(t, (b"a",))
    value["blobs"][0]["content_base64"] = encoded
    rejected(wire(value), t)

@pytest.mark.parametrize("limit,n", [(16_384,16_383), (16_384,16_384),
    (1_073_741_824,16_384), (1,1), (5,4), (5,5)])
def test_actual_byte_boundary_accepts(limit,n):
    t = task(limit); content = b"x" * n
    assert decode_worker_output(wire(envelope(t,(content,))),t).blobs[0].content == content

@pytest.mark.parametrize("limit,contents", [(16_384,(b"x"*16_385,)),
    (1_073_741_824,(b"x"*16_385,)), (1,(b"xx",)), (5,(b"xxxxxx",)),
    (5,(b"abc",b"def"))])
def test_actual_byte_boundary_rejects(limit,contents):
    t = task(limit)
    rejected(wire(envelope(t,contents)),t,"worker_output_too_large")

def test_in_cap_declared_mismatch_is_invalid():
    t = task(5); value = envelope(t)
    value["result"]["artifacts"][0]["size_bytes"] = 100
    rejected(wire(value),t)

def test_deceptive_small_declared_size_cannot_expand_allocation():
    t = task(); value = envelope(t,(b"x"*16_385,))
    value["result"]["artifacts"][0]["size_bytes"] = 1
    rejected(wire(value),t,"worker_output_too_large")

@pytest.mark.parametrize("outcome,code", [("blocked",None),("failed","worker_failed")])
def test_non_candidate_empty_only(outcome,code):
    t = task(); value = envelope(t)
    value["result"].update(outcome=outcome,failure_code=code,artifacts=[])
    value["blobs"] = []
    decoded = decode_worker_output(wire(value),t)
    assert decoded.result.outcome == outcome and decoded.blobs == ()
    value["blobs"] = envelope(t)["blobs"]
    rejected(wire(value),t)

@pytest.mark.parametrize("reverse", [False,True])
@pytest.mark.parametrize("case,expected", [
    ("malformed_and_actual_overflow","worker_output_invalid"),
    ("declared_mismatch_and_actual_overflow","worker_output_too_large"),
    ("malformed_and_encoded_overflow","worker_output_too_large"),
])
def test_cross_blob_precedence_is_order_independent(reverse,case,expected):
    t = task(1); value = envelope(t,(b"a",b"bc"))
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
```

Add explicit cases: empty candidate, blocked/failed with references but no blobs, both nonempty, wrong/missing/nonstring failed code and forbidden nonfailed code; missing/extra/duplicate blobs and duplicated ArtifactRefs; wrong real bytes/hash; zero-byte digest and size mismatch; 128 unique one- or two-byte contents accepted and 129 rejected (use `i.to_bytes(2,"big")` for distinct contents); reverse root/key/blob order preserves mapping. Pin canonical `YQ==`, `YWI=` and `/w==` acceptance separately so bad-pad-bit rejection is not explained merely by size/digest mismatch. For `YWJ=` use reference/content `b"ab"`; for `YR==` use `b"a"` to demonstrate equal decoded bytes with bad pad bits. Pin aggregate exact/lower/overflow using distinct small contents and a huge encoded string with a tiny declared size. No reduced production caps are necessary for the natural frame/candidate boundaries above.

Implementation phases are global across the matched blob set, never per-blob early integrity/cap exits: (1) check candidate/noncandidate shape and 1..128 unique blob/ref sets, derive `cap = min(valid_task.output_byte_limit, 16_384)` and encoded cap `4*((cap+2)//3)`, and check every encoded-string length before decoding any blob; (2) validate canonical encoding for every blob; (3) classify actual individual/aggregate byte overflow; (4) compare declared sizes and host digests; (5) run shared result binding. `_decode_base64` encodes ASCII strictly, uses `base64.b64decode(..., validate=True)` and requires `base64.b64encode(content).decode("ascii") == encoded`. During phase2 an implementation may count actual bytes and discard content after detecting overflow to avoid retaining oversized aggregate data, but it must still finish remaining encoding validation before raising actual overflow. Malformed encoding within all preliminary bounds takes invalid priority even if another blob has actual overflow. If all encodings are canonical, actual overflow takes too-large priority over any declared-size/digest mismatch. Preliminary frame or encoded-length overflow may reject as too-large before encoding validation. Only after byte integrity and caps, call `validate_worker_result(valid_task,result)` for context, input-alias and declared aggregate. Return a tuple sorted by digest, with references linked to the freshly validated result; result artifact ordering is not rewritten. Do not trust any metadata as host provenance.

- [x] **Step 4: RED/GREEN unsafe Task and exact binding checks.**

```python
@pytest.mark.parametrize("field,value", [("output_byte_limit",True), ("output_byte_limit",0),
    ("output_byte_limit",1_073_741_825), ("task_text","\ud800"), ("task_text"," "),
    ("input_artifacts","not-array"), ("tool_capabilities","not-array"),
    ("unexpected_control_handle","private-extra-sentinel")])
def test_unsafe_task_copy(field,value):
    original = task(); unsafe = original.model_copy(update={field:value})
    rejected(wire(envelope(original)),unsafe)

@pytest.mark.parametrize("field", list(AttemptContext.model_fields))
def test_full_attempt_binding(field):
    t = task(); value = envelope(t)
    old = value["result"]["context"][field]
    value["result"]["context"][field] = old + 1 if type(old) is int else (
        "sha256:"+"b"*64 if old.startswith("sha256:") else "other-identity")
    rejected(wire(value),t)

def test_input_digest_alias():
    t = task(); value = envelope(t)
    ref = ArtifactRef.model_validate(value["result"]["artifacts"][0])
    unsafe = t.model_copy(update={"input_artifacts":(ref,)})
    rejected(wire(value),unsafe)
```

Add unsafe `model_construct` with missing required fields and type-substituted context; nested copied context extra/boolean/hash/surrogate; nested ArtifactRef extra/boolean/invalid metadata; nested ToolCapabilityRef extra/invalid scope/duplicate grants; more than 128 input refs or 64 capabilities; duplicate input digests. Supply wrong Task types including mapping/None and copied subclasses with surviving extras. For extras already discarded by unsafe Pydantic construction, assert no promise of recovering discarded input: test surviving `__dict__`/`__pydantic_extra__` fields instead. Explicitly check legitimate Task list normalization remains consistent with existing validators; do not invent a new tuple-only contract.

Implement `_revalidate_task` as recursive preflight over model fields and container members: reject surviving `__dict__` names outside the corresponding expected contract class's `model_fields` and nonempty `__pydantic_extra__` before `revalidate_model(WorkerTask,task)`. Expected schemas are the existing WorkerTask, AttemptContext, ArtifactRef and ToolCapabilityRef; never use an untrusted subclass's expanded field set as the allowlist. Add a subclass with an extra declared field to the rejecting tests as well as unsafe model_copy extras. Reject unencodable nested strings and field-only rebuild invalid types; catch cycles/recursion as invalid. Avoid serialization methods on the untrusted model, and do not amend the shared helper. Fresh result models are validated from plain parsed JSON, not accepted model instances.

- [x] **Step 5: RED/GREEN detached errors, purity and legacy compatibility.**

```python
import traceback
import orchestrator.runtime.worker_output as output_module

@pytest.mark.parametrize("target", ["_revalidate_task","_decode_base64","_digest_bytes"])
def test_owned_raw_exception_is_not_retained(monkeypatch,target):
    sentinel = "raw-secret-sentinel-for-owned-exception"
    class RawError(ValueError):
        code = sentinel
    def fail(*args,**kwargs):
        raise RawError(sentinel)
    t = task(); raw = wire(envelope(t))
    monkeypatch.setattr(output_module,target,fail)
    error = rejected(raw,t)
    assert sentinel not in repr(error) + str(error) + str(error.args)
    assert sentinel not in "".join(traceback.format_exception(error))
    assert error.__context__ is None and error.__cause__ is None

def test_error_constructor_does_not_preserve_opaque_code():
    error = WorkerOutputError("opaque-secret-sentinel")
    assert error.code == "worker_output_invalid"
    assert error.args == ("worker_output_invalid",)

def test_error_constructor_does_not_invoke_hostile_codes():
    calls = []
    class Hostile:
        def __eq__(self,other):
            calls.append("eq"); raise AssertionError("untrusted equality")
        def __str__(self):
            calls.append("str"); raise AssertionError("untrusted stringification")
        def __repr__(self):
            calls.append("repr"); raise AssertionError("untrusted repr")
    class HostileString(str):
        __eq__ = Hostile.__eq__
        __str__ = Hostile.__str__
        __repr__ = Hostile.__repr__
    for code in (Hostile(),HostileString("worker_output_too_large")):
        error = WorkerOutputError(code)
        assert error.code == "worker_output_invalid"
        assert error.args == ("worker_output_invalid",)
    assert calls == []
```

Inject a genuine Pydantic ValidationError containing sentinel input through `_revalidate_task`, a Base64 decoding exception, a hash exception, a JSON parsing exception and an internal `WorkerOutputError` with unsafe copied/mutated code/args. The public wrapper must not inspect/trust arbitrary caught `exc.code`. Assert sentinel absence in error raw args, cause/context objects and formatted trace for malformed payload, invalid nested Task and all injected paths. Protect repr/str of each `DecodedCandidateBlob` directly, not only the parent object; frozen Pydantic result/ref mutation must reject too. Treat explicit content access and traceback frame locals as intentionally outside confidentiality guarantees.

Use private `_TooLarge` only for trusted frame/encoded/actual aggregate overflow. Public flow: initialize a trusted invalid literal; `try: return _decode(...)`; `except _TooLarge: code = "worker_output_too_large"`; `except Exception: code = "worker_output_invalid"`; after the handlers, `raise WorkerOutputError(code)`. Do not catch `BaseException` (KeyboardInterrupt/SystemExit are not data failures), retain raw exceptions or use `raise ... from exc`. Exceptions owned by parsing/revalidation helpers must terminate at the wrapper. An untrusted existing WorkerOutputError also maps to invalid.

For purity, construct inputs/import module before the guarded call. Inside `monkeypatch.context()` replace `builtins.open`, Path read/write/open methods, `socket.socket`, `subprocess.Popen` and `os.system` with sentinels that record every attempted call before raising, then decode a valid vector and a rejected vector and assert the recorded-call list is empty. Raising alone is insufficient because the decoder may sanitize the exception. Audit the new module AST/import surface: only stdlib data operations, existing contracts and validation; no ArtifactStore, Scheduler/application, Gateway/Broker, Verifier, time/clock, event or process service import/call. Restoring the monkeypatch context precedes pytest/report I/O. This is a pure-function check, not a systemd security proof.

Run the growing focused file after each RED/GREEN change. Run existing `tests/unit/runtime/test_contracts.py`, `test_worker_process.py`, `test_artifact_admission.py`, `test_worker_input.py`, `test_worker_input_service.py` and `tests/integration/test_isolated_worker_process.py` from the real WSL user systemd environment. Required live tests must execute with zero skips; unavailable environment is a failed delivery gate, not permission to loosen marks/limits or substitute a pure test.

- [x] **Step 6: Document, self-review, commit and independently review the task.**

Add a pure-output section to `docs/security/worker-runtime.md` showing the exact direct-module import/signature, schema keys, sorted immutable blob mapping, two safe error codes, exact frame/raw/Task caps, canonical Base64 and strict new outcome shape. State explicitly: no publication, origin attestation, active authority, Node success, settlement or Worker execution; current transport remains blocked-only. Describe surviving-extra Task revalidation and detached owned errors without promising memory erasure. Document that 16,384 bytes encode to 21,848 characters before metadata and cannot fit the current default 16,384-byte stdout budget; future integration needs a separately reviewed encoded-frame limit. Preserve publisher digest-lock-before-SQLite/intent/recovery gates in the existing spec; do not claim those solved here. Link focused tests and retain historical test numbers as historical, not new evidence.

Self-review requirements against all steps/spec, exact new file scope, no production transport/shared-validator/legacy edits, no raw diagnostics or added dependencies, real RED/GREEN evidence, no unsupported performance/isolation claim. Run focused tests and `compileall`/`git diff --check`; report actual commands, timings, counts, skip/warnings, source paths and any deviations. Commit only the three task-owned files with `feat: decode bounded Worker candidate bytes`; implementer never pushes or dispatches agents. Controller records actual pre-dispatch BASE, generates a complete BASE..HEAD review-package file via SDD scripts, and dispatches a fresh task reviewer with the task brief, full report, binding constraints and package. Require both spec-compliance and quality verdicts; resolve findings through original-implementer fix/scoped review, never a controller product patch. Record each declined-to-judge item disposition, not just the approval.

- [ ] **Step 7: Exact-source full acceptance, final whole-slice review and push.**

On the final reviewed source SHA run these commands from the WSL checkout, retaining raw outputs in this plan's workspace via normal command capture/report tooling (no shell write shortcuts for hand-authored reports):

```bash
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest tests/unit/runtime/test_worker_output.py tests/unit/runtime/test_contracts.py tests/unit/runtime/test_worker_process.py tests/unit/runtime/test_artifact_admission.py tests/unit/runtime/test_worker_input.py tests/unit/runtime/test_worker_input_service.py tests/integration/test_isolated_worker_process.py
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage run -m pytest
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage report
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m compileall -q src tests
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pip check
git diff --check 720f1507c2056424703008674e1c070bd48a5773..HEAD
```

Build with these additional commands; the task-specific variable is not a system option and the temporary directory is never deleted:

```bash
maestro_output_wheel_dir=$(mktemp -d /tmp/maestro-worker-output-wheel.XXXXXX)
env PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m build --wheel --outdir "$maestro_output_wheel_dir"
```

Record actual task BASE captured before implementation; do not use HEAD~1 to truncate fixes. Verify packaged `orchestrator/runtime/worker_output.py` bytes equal the tested source using `zipfile` and SHA-256/read_bytes in a read-only check, and confirm module `__file__` resolves this isolated checkout. Build artifacts are not deployed/published. A dependency/network/build problem is a reported gate failure, not authority to install into shared venv or change dependencies.

Require full pass, total coverage >=90%, zero skipped required live systemd tests, clean compile/pip/wheel/whitespace checks. Any source changes after measurements invalidate that source evidence and need covering tests plus new final full gate. Run the full suite once per final source candidate, not once per reviewer. Old 2,074/90.41% evidence is not current acceptance. Report new-module coverage separately from total; no paid Provider or cost/token/rework improvement claim.

Controller packages the entire new slice's fixed `720f1507c2056424703008674e1c070bd48a5773..final HEAD`, including this plan and every implementation/fix, for one fresh most-capable whole-slice reviewer. Prior completed input slice has its own complete final review; do not repeat it. The whole-slice reviewer receives current spec, task report, full acceptance evidence, package and all ledger rulings/deferred observations. Review is read-only with no nested agents/test duplication. Handle any final findings with one fix wave and scoped re-review per SDD; residual load-bearing safety issues prevent delivery.

Only after task/final review and fresh gates are satisfied, update associated plan/spec status as a scoped documentation change without changing requirements or falsely dating earlier tests. Review that exact documentation candidate before its push. Controller pushes the existing codex branch, verifies HEAD/upstream/actual `git ls-remote` equality and clean porcelain status, and records actual SHA/outcome in the durable ledger. No merge or PR. Do not mark Task 1 complete before its task review, or whole slice complete before delivery gates; plan status must distinguish source-tested SHA from any later docs-only SHA.

## Next-stage handoff and delivery boundary

This decoder is only a prerequisite. Future trusted publication must resolve durable active Attempt authority afresh, derive metadata/provenance outside the proposal, avoid digest-lock/SQLite inversion, and specify publication intent, races and uncertain crash recovery before connecting ArtifactStore. Future transport must separately bound encoded frames; actual Worker/Gateway/Approval/Broker/tool calls and Verifier/usage/acceptance wiring remain distinct execution work. Do not grant any of those through this plan.

Functional seven-role orchestration, semantic/project-test/Final Review, CLI/MCP, full security/recovery E2E and genuine reproducible paid-Provider benchmarks remain V1 acceptance gates. Credential source/fees/destructive operations/deployment/permission expansion require explicit human authority. Offline contract tests or previous isolation smoke tests never stand in for those results.
