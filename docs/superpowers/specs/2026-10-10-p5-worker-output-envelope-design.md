# P5 Bounded Worker Candidate Byte Envelope

**Date:** 2026-10-10 (America/Los_Angeles)
**Status:** Architectural specification prepared under the user's explicit instruction to self-plan/review the next task and proceed when sound; independent specification review required before an implementation plan. This is not a claim of written-spec human approval.
**Source BASE:** `d274eae71bec4895b744594b3dad4a0c0853b721`
**Delivery:** A pure output-data prerequisite for trusted publication and functional Worker execution, not either feature or V1 delivery.

## Intent, authorized scope and assumptions

The user wants Maestro's remaining V1 implemented in the existing isolated codex branch with tested, independently reviewed stages pushed individually. The user explicitly authorized self-planning the next task and proceeding after self-review; this is the basis for autonomous design preparation, not silence or a heartbeat. Subagent-driven execution remains selected. No real credentials, paid Provider, deployment, destructive cleanup, main merge or permission expansion is authorized.

The prior input slice supplies a bounded `WorkerTask` from durable authority. The production `IsolatedWorkerProcess` still returns only `blocked`, and `WorkerResult` carries references rather than bytes. `admit_candidate_artifacts()` can only verify bytes already published by the host. There is no bounded way for a future Worker to submit new candidate bytes without passing a path or control-directory handle. This slice closes that data-contract gap before execution or publication is enabled.

The assumption is that one response can carry at most the existing narrow task's 16,384 output bytes. Large outputs and streaming need a separate design. This module must never label decoded data as authorized, host-published, process-origin-attested, verified, or successful.

## Alternatives and selection

1. **One strict JSON frame with canonical Base64 blobs (selected):** uses the existing single-response stdin/stdout framing pattern; supports arbitrary bytes and deterministic validation without Worker-controlled paths. Encoding overhead is bounded, but this is not compatibility evidence for the current transport's operational stdout budget. No execution transport is changed in this slice.
2. **Child export directory:** could avoid encoding overhead, but adds file-descriptor/path ownership, links, concurrent writes, mount export and cleanup questions. Defer until such a filesystem boundary is needed and reviewed.
3. **Chunked duplex stream:** scales beyond one response, but introduces session framing, partial-output recovery and backpressure. Defer; do not silently enlarge the present output cap.

## Wire schema and strict decoding

Add a separate `orchestrator.runtime.worker_output` module. Keep existing `WorkerTask`, `WorkerResult`, legacy `decode_worker_result()` and production blocked-only transport unchanged.

The new wire object has exactly three required fields:

```json
{
  "schema": "maestro.worker-output.v1",
  "result": {"context": "full existing AttemptContext object", "result_id": "result-1", "outcome": "candidate", "artifacts": [], "failure_code": null},
  "blobs": [{"digest": "sha256:<64 lowercase hex digits>", "content_base64": "<canonical padded RFC 4648 standard Base64>"}]
}
```

The example illustrates fields, not a valid candidate: a real candidate must include at least one complete existing `ArtifactRef` and matching blob. A full valid fixed vector will be pinned in tests. No path, workspace, secret, store handle, capability body or settlement data is accepted. No additional keys are accepted at any new-object or existing-model nesting level. Exact JSON scalar types are required; bool is not an integer. Duplicate keys, non-finite values, invalid UTF-8, UTF-8 BOM, malformed JSON, wrong schema and oversized frames reject.

`decode_worker_output(payload: bytes, task: WorkerTask) -> DecodedWorkerOutput` accepts exact bytes, not a bytearray/string/stream/path. It reads no files and performs no I/O. The payload must be nonempty and at most 1,048,576 bytes before parsing. Catch recursion/decode/validation failures and expose only sanitized stable errors.

Revalidate the supplied task recursively before use, including unsafe `model_construct`/`model_copy` values. Reject surviving unknown fields before a field-only rebuild can discard them. Do not modify the shared revalidation helper or existing contracts for this task. Result/context/ArtifactRef models must likewise be freshly validated from the strict JSON object, not trusted because they are model instances.

Apply `validate_worker_result()` to the newly validated result and task for exact full AttemptContext equality, aggregate declared-size limits and prohibition on relabeling input artifact digests. Equality is a binding check, not current authority. There is no Scheduler, clock, lease or permission resolution here. Sequence actual-byte validation and this shared validator deliberately: well-typed payloads exceeding an actual byte cap must yield `worker_output_too_large`, not a generic invalid error from the shared declared-size check. A declared/actual size mismatch within byte caps yields `worker_output_invalid`; malformed encoding remains invalid unless a preliminary frame/encoded-length cap has already rejected it. Pin these classifications in tests.

## Bytes, metadata and outcomes

For `candidate`, require one to 128 unique ArtifactRefs and one to 128 unique blob objects. The digest sets must match exactly, with neither missing nor extra blobs. Blob order may differ from reference order; validation is independent of dictionary/key order. A blob has exactly `digest` and `content_base64`, both strict strings; digest syntax is the existing lowercase SHA-256 convention. Reject duplicate digests even if their bytes are identical.

For `blocked` or `failed`, both `result.artifacts` and `blobs` must be empty. Preserve the existing result contract's failure-code rules; this tighter byte-envelope shape does not change the legacy result contract. Do not reinterpret failed/blocked data as a successful empty candidate.

Base64 is standard RFC 4648 with required padding where applicable, ASCII only, no whitespace, URL-safe characters or ignored suffix. Decode with strict validation, then require re-encoding to equal the input, rejecting alternate pad bits and noncanonical encodings. Empty Base64 is valid only for a genuinely zero-byte artifact whose declared digest/size match; a candidate still requires at least one artifact. Bound every encoded string before decoding: canonical encoded length cannot exceed `4 * ceil(min(task.output_byte_limit, 16_384) / 3)`, and the whole frame cap bounds parser allocations. Compare the actual decoded size with the corresponding ArtifactRef size and SHA-256 digest computed by the host. Enforce the sum of all decoded bytes at most `min(task.output_byte_limit, 16_384)` as well as existing declared-size validation. Do not trust a small declared size to permit a large allocation. An untrusted task cannot raise the fixed 16,384-byte narrow-profile cap.

Validated result metadata remains a child's proposal. `artifact_type`/`media_type` continue through existing strict bounded ArtifactRef validation; they do not choose publication permissions, redaction classification, scope, lifecycle state or source provenance. A future publisher must derive or separately authorize those fields. No metadata or byte content is persisted by this module.

## Return objects, errors and privacy

Return immutable host-side data: `DecodedWorkerOutput` contains the freshly validated WorkerResult and a tuple of `DecodedCandidateBlob` objects, each holding its corresponding ArtifactRef and exact immutable bytes. Sort blobs by digest for deterministic mapping; preserve the result's validated reference sequence. Content bytes are excluded from incidental repr/str. Do not include task text in returned data. Explicit caller access to candidate bytes is intentional, not a confidentiality guarantee.

Return objects are data, not capabilities or proof. Ordinary constructors, unsafe construction or caller mutation attempts must not become an origin/authorization guarantee; future trust crossings must revalidate. No convenience method publishes, runs Verifier, records a proposal, completes an Attempt, settles usage, writes an event or dispatches a child.

`WorkerOutputError` exposes only trusted literal `worker_output_invalid` or `worker_output_too_large`. Wrong input/task/context/schema/outcome/metadata/digest/Base64/invariant uses invalid; actual fixed-frame or byte-cap overflow uses too-large. Raw payload, decoded bytes, task text, arbitrary validation messages, opaque exception codes and original exceptions must not appear in public args, code, repr, formatted output or owned context/cause chains. Raise the sanitized error after leaving owned exception handlers; `from None` alone does not detach context. Injected internal errors fail closed. This does not erase Python traceback locals, caller-owned prior errors, host memory/swap/core dumps or explicit byte access. Hashes are integrity checks, not confidentiality or same-UID-host protection.

## Required verification

1. Real RED/GREEN for the missing decoder and each introduced validation behavior; use a full valid fixed candidate vector and verify exact bytes/reference mapping, SHA-256 and zero-byte artifact behavior.
2. Exact frame/output boundaries and limit-minus/at/plus cases; fixed 16,384 cap even for a larger task limit; smaller task limit; declared/actual mismatch; multi-blob aggregate overflow. Test-only reduced caps must be disclosed and restored, not described as natural oversized valid envelopes.
3. Arbitrary binary and UTF-8 bytes, key/blob order changes, canonical padding, pad-bit aliases, junk suffix, whitespace, non-ASCII, URL-safe encodings, invalid length and huge encoded string with deceptively small declared size.
4. Duplicate root/nested keys, wrong schema/types/fields, missing/extra/duplicate blobs, wrong digests, artifact metadata rejection, cross-Attempt/hash/generation bindings and input/output digest aliasing. Unsafe task construction/copy with mutated nested context, tuple bounds, extra fields, bool and surrogates must fail.
5. Candidate/blocked/failed shape tests, immutable data and content-free repr/str, direct sentinel checks on owned error args/context/cause and formatted traces, including injected validation/hash/Base64 errors with arbitrary raw args/codes. No raw input/payload logging.
6. Legacy Worker/result contracts and blocked-only real systemd behavior stay unchanged. Assert no filesystem, ArtifactStore, Scheduler, Gateway, Broker, child or Verifier call is made by decoder; pure tests are not isolation/functional-execution evidence.
7. Full suite passes with at least 90% total coverage and no skipped required existing real systemd isolation test; compileall/pip check/wheel/diff checks, focused and full exact-source evidence, independent task and whole-slice reviews, then stage push with actual remote/clean verification. Do not reassert older 2,074/90.41% results as new evidence.

## Publication and execution handoff (not implemented here)

The next publisher design must use a trusted host boundary and fresh durable Attempt resolution, never grant publication or Node success merely because this decoder accepted a frame. Existing ArtifactStore acquires a digest lock before writing SQLite intent/metadata; an outer Scheduler SQLite lock held across `publish_bytes()` would reverse that order and risk deadlock. Keep filesystem work outside long Scheduler transactions, and design the precise authority checks, publication-intent/metadata guards and crash semantics before connecting it. Partial or stale output must not become accepted success; uncertain publication and existing blobs must not be automatically deleted or re-executed after restart. These are preserved next-stage requirements, not solved by this contract.

The current production `IsolatedWorkerProcess` defaults to a 16,384-byte stdout budget. A 16,384-byte artifact alone needs 21,848 Base64 characters before JSON/context/metadata, so maximum-size decoded content cannot traverse that budget. Future transport integration must independently budget and bound the encoded frame plus metadata, distinct from the 16,384-byte decoded-content cap; it cannot treat decoder acceptance as a launch/transport proof. This slice does not change launcher limits, relax isolation or connect the decoder to the child.

Actual Worker generation, Gateway/Tool/Approval/Broker mediation, candidate publication, production usage, dynamic seven-role orchestration, semantic/project-test/Final Review, CLI/MCP, complete recovery/security E2E and genuine paid-Provider benchmarks remain incomplete. Production Worker must remain blocked-only until those specific execution changes are reviewed and tested.

## Design/process delivery

Use the existing isolated worktree and `codex/p3-systemd-termination-receipts`; preserve main's unrelated changes. First self-review this spec for placeholders, contradictory bounds, ambiguous types, input retention and premature origin/permission claims; obtain independent architectural review. Resolve findings before a reviewed implementation plan, then use the selected Subagent-driven workflow, TDD and independent reviews. The user's explicit self-plan/proceed mandate permits this safe autonomous workflow; it does not approve secret/payment/deployment/permission changes. Record that provenance without inventing a written-spec approval message. Push each completed reviewed stage; retain its plan-specific ledger and reports for restart.
