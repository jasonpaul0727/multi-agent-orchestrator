# P5 Worker Input Re-supply and Task Construction Design

**Status:** Specification and reviewed implementation plan explicitly approved for execution by the user on 2026-10-10. Source implementation independently reviewed and pushed. Final documentation closure is conditional on independent review and controller push/remote verification recorded in the durable delivery ledger.
**Date:** 2026-10-09 (America/Los_Angeles)
**Base:** `e521885ba37997f97dc9609ecc960ea961e33bc0`
**Delivery:** A prerequisite for functional Worker execution, not functional Worker or V1 delivery.

## Intent and scope

The user wants Maestro's remaining V1 implementation completed in the existing isolated codex branch, with tested, independently reviewed stages pushed individually. The selected direction preserves the existing no-task-text-persistence rule: a trusted caller supplies task text again after restart, and the control plane checks it against the original accepted input commitment.

This specification covers a versioned input commitment, host-owned routing admission with that commitment, and construction of a bounded `WorkerTask` from durable Attempt authority. It does not dispatch the Worker, enable candidate production, change settlement authority, or add a secret/input store. Missing inputs never trigger automatic execution. The construction result is data, not a capability to execute or complete an Attempt.

## Current evidence and limitation

- `GraphPlanningService` consumes `NodeProposal.task_text` but does not persist it. The frozen planning contract does not contain a digest of that original text.
- The compatible low-level `ControlPlaneApplication.accept_routing()` accepts a caller-provided `input_manifest_hash`; the implemented `accept_routing_with_input()` derives the canonical commitment from durable Node authority and exact caller text. Scheduler freezes it together with full Attempt context and the supported verification contract/check set.
- `Scheduler.resolve_verification_binding()` re-resolves active, current-generation, unexpired Attempt authority from durable state. It is the authority source for this slice.
- `WorkerTask` has strict bounded context, text, artifact-reference, capability-reference, and output-limit fields. The implemented `ControlPlaneApplication.prepare_worker_task()` constructs task data from one serialized frozen Attempt snapshot; it does not execute a Worker.
- The production `IsolatedWorkerProcess` remains blocked-only. `AttemptExecutionCoordinator` currently re-verifies an existing proposal and uses trusted usage before proof-gated completion; it does not execute tasks.

Matching a new commitment proves equality with text accepted at routing admission, not equality with text previously consumed by the planner. This slice must not claim to recover original planner text, authenticate caller identity, or prove semantic equivalence. Adding original-planning-input commitments is separate work; raw-text re-supply cannot silently repair missing historical evidence.

## Alternatives and selected boundary

1. **Re-supply and compare a frozen commitment (selected):** no plaintext input store, no encryption keys, and deterministic mismatch rejection. The caller remains responsible for retaining or reconstructing the exact text outside Maestro. A restarted Run cannot continue when that caller cannot provide it.
2. **Protected durable input storage:** improves unattended restart but introduces retention, access control, encryption/key management, migration, and recovery policy. It is not authorized by this direction and is excluded from this slice.
3. **Process-local input cache:** convenient during one process lifetime but not restart-safe. Excluded as an authority source; there is no cache-based fallback when input is absent.

The trusted host boundary remains the repository's single-host model. Hashes are consistency checks, not protection against a compromised same-UID host. Low-entropy task text may be guessable from its digest; this design makes no confidentiality claim for hashes.

## 1. Versioned canonical input commitment

The caller supplies only task text to the new input-specific interface. The host resolves Run, Node, and role from durable graph/config state; the caller cannot override them through an input body.

The manifest has exactly these fields:

```json
{
  "schema": "maestro.worker-input.v1",
  "run_id": "run-1",
  "node_id": "node-1",
  "role": "coder",
  "task_text": "exact caller-supplied text",
  "input_artifacts": []
}
```

The example role is illustrative; the actual value must be the Node's frozen role. V1 of this narrow interface permits no input artifacts or tool capabilities. Nonempty arrays, caller-controlled paths, credentials, context hashes, or additional fields are rejected rather than silently discarded. A later artifact-delivery design must precede support for artifact inputs.

Canonical bytes use UTF-8 strict encoding of JSON with `ensure_ascii=False`, sorted keys, compact separators, and `allow_nan=False`. The text is not trimmed, case-folded, newline-normalized, or Unicode-normalized. Blank text is rejected; otherwise byte-significant differences remain different inputs. Strings containing unencodable surrogates fail closed.

The commitment is `sha256:` followed by the lowercase SHA-256 of `b"maestro.worker-input.v1\x00" + canonical_bytes`. The domain prefix and schema prevent confusion with another JSON/hash contract. Run/Node/role are included so identical text cannot authorize a different node by itself.

Text retains the existing 100,000-character maximum; canonical manifest bytes and serialized `WorkerTask` must each fit the existing 1,048,576-byte IPC envelope. Models and nested fields are revalidated at trust crossings, including values created via unsafe model construction. Hash calculation and validation failures expose stable sanitized errors, not rejected text, validation input, or chained arbitrary exceptions.

## 2. Input-bound admission

Add a distinct host application entry point, conceptually `accept_routing_with_input(...)`, accepting the existing routing request/decision/timestamps plus task text. It does not accept `input_manifest_hash`, Attempt context, reservation ID, role, verification evidence, or settlement usage supplied as input authority.

The host derives the canonical commitment from the durable Node identity/role and supplied text and passes it, with the fixed supported built-in verification contract and required checks, into the existing Scheduler admission transaction. Required checks come from trusted composition, include `artifact-integrity`, and cannot be selected by task text. The existing Scheduler still owns readiness, routing/config/Registry/planning binding, reservation, Agent instance, fencing, and lease creation.

The Node role and identity used to calculate the commitment must be checked against the same authoritative node accepted by Scheduler, under its serialized admission authority. A graph mutation between preliminary lookup and admission must reject or restart the host calculation; it must not produce a commitment for a different node/role. Do not hold a database transaction open during future process execution.

Admission idempotency retains existing exact-binding behavior: identical inputs and authority may replay the original admission; different text/hash/contract for the same admission cannot mutate or retrofit it. The existing low-level hash admission API remains compatible for prior failure/unknown paths and tests. It does not become evidence that a canonical input was retained or can be reconstructed.

## 3. Task preparation from durable authority

Add a distinct application operation, conceptually `prepare_worker_task(run_id, node_id, attempt_id, fencing_generation, task_text)`. The application must be ready and all identity fields strictly valid. There is no default text, process cache lookup, ambient file read, or startup call to this operation.

Preparation uses a host-owned aware UTC clock, calls `resolve_verification_binding()` for the exact current Attempt, and resolves the Node's role from its accepted frozen graph/planning authority. The trusted time is sampled after acquiring the outer preparation serialization lock, not before waiting for it. That sample defines the preparation validity point; the resolver's `as_of` must use it, and a lease that expired during lock acquisition must reject. Preparation recomputes the versioned input commitment and requires an exact match to the admission's `input_manifest_hash` before constructing any `WorkerTask`.

The task contains only the binding's immutable `AttemptContext`, the authoritative role, validated caller text, empty artifact/capability tuples, and a host-fixed output limit of 16,384 bytes. The caller cannot select context/config hashes, workspace, paths, output limit, tools, Provider, or model. The role also must match the role used when the canonical commitment was admitted. Configuration reload must not change a running Attempt's task/context authority.

The same authoritative snapshot must cover role, graph version, context, and active lease; preliminary reads are not sufficient. Concurrent cancellation, lease expiry, fencing replacement, or conflicting graph state must reject preparation rather than returning a mixed snapshot. The eventual dispatch layer must independently re-resolve authority immediately before launch and before acceptance: a previously prepared task is not lasting permission to execute.

This slice returns task data only. It does not call `IsolatedWorkerProcess.execute()`, append a success/proposal event, create output artifacts, invoke Gateway/Tool/Approval/Broker, settle usage, or release a lease. Empty/incorrect input leaves the existing durable state unchanged and returns a stable error; it does not introduce a new lifecycle status called waiting.

## 4. Restart, failure, and compatibility

- Startup retains the completed acceptance-foundation proof/proposal validation. It never reads task text from storage or invokes preparation/Worker execution.
- After restart the caller may provide exact text for an Attempt that is still active and unexpired. Expired/stale/cancelled/terminal Attempts reject even when text matches. A new Attempt follows existing Scheduler authority rules, not automatic resurrection.
- Missing or mismatched input is recoverable only by supplying the correct text while authority remains valid. No fallback to planner classification, current config, another node's text, or an older generation is permitted.
- Historical admissions with no input commitment cannot prepare a task. Arbitrary old hash values are not converted or overwritten. An existing hash authorizes preparation only if it exactly matches this domain-separated canonical manifest and all normal authority checks pass; collision resistance is assumed, not a migration guarantee.
- Inputs remain in host/returned task memory for the request lifetime. The child IPC boundary, when later connected, will necessarily receive task text. No claim is made that Python strings can be securely zeroed.
- Event payloads, persistent projection rows, audit, logs, exceptions, argv, environment, and ArtifactStore must not gain raw task text from this path. Existing unrelated state is not rewritten or deleted.

## 5. Verification and delivery gates

Required new tests, with RED/GREEN where behavior is added:

1. Known canonical vectors; dictionary-order independence; exact whitespace/newline/Unicode distinction; domain/schema/Run/Node/role separation; no unexpected normalization.
2. Empty, blank, excessive-character, excessive-byte, surrogate, extra-field, wrong-type, and unsafe-constructed inputs fail with sanitized errors and no persistent writes.
3. Application admission computes its own hash and freezes the required verifier binding. Exact retries match; conflicting inputs fail without a new reservation, Agent, or slot.
4. Preparation builds the exact task from real SQLite authority and does not dispatch any child, Gateway, tool, or secret request. Caller input cannot override any authority field.
5. Cancellation, lease expiry, stale generation, swapped role/context, and concurrent graph/admission/preparation races fail closed. Include a lock wait that crosses lease expiry, proving time is sampled under the acquired lock. Reads must not produce a mixed-authority task.
6. Fresh-process reopen accepts exact re-supply only for still-valid authority. Missing/mismatched/legacy inputs never automatically run or repair durable state. Config-file changes do not replace frozen Run hashes.
7. Sentinel task text does not occur in SQLite event/projection payloads, audit records, captured logs/errors, artifacts, argv, or environment; explicit in-memory task data is the documented exception. No live secrets or paid Provider are needed.
8. Full repository tests pass with total coverage at least 90%; required existing real systemd isolation tests are not skipped. A task-construction-only test must not be labeled functional Worker execution.

Each approved implementation task uses the retained subagent-driven workflow and independent scoped review. Source stages were committed, reviewed and pushed to `codex/p3-systemd-termination-receipts`; the main checkout's unrelated changes remain untouched. Final documentation bookkeeping requires independent documentation review, diff checks, and verified push; its candidate does not reassert source test results as a fresh run.

### Recorded source acceptance and delivery — 2026-10-10

Tested source is `d5436c25aab182170da6634ca068bf06c0f55a17`. Its scoped Task 3 review was Approved; the complete six-commit/fifteen-file range `fd5a9351d2b75f71b72e28442f6143f8f946c9fa..d5436c25aab182170da6634ca068bf06c0f55a17` passed independent whole-branch review with no Critical, Important or new Minor findings. The technical approval did not authorize a merge. The controller pushed that source and verified HEAD, tracking ref and actual remote SHA all equal to the tested source, with a clean checkout. This records the source push only; the later documentation candidate's review and remote equality remain pending.

Source gates in the isolated WSL worktree used `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`, with explicit `PYTHONPATH=src:.`: prescribed focused **552 passed in 98.58s**, affected final-test recheck **10 passed in 21.17s**, and `python -m coverage run -m pytest` **2,074 passed in 401.16s**, zero test skips/warnings. `coverage report` measured **90.41%** total (17,134 statements/1,643 missing); report elision of 28 completely covered files is not a test skip. Required existing real systemd isolation tests ran. Compileall, pip check and whitespace gates exited 0. A normal isolated PEP517 wheel built successfully; its application, contracts and input-service files matched source byte-for-byte. Actual runtime was Python **3.12.3** / Pydantic **2.13.5**; no minimum-version matrix or child-process coverage certification is implied. Detailed command/RED/GREEN provenance remains in the task recovery reports and the [implementation plan](../plans/2026-10-10-p5-worker-input-resupply.md). No source tests or build are rerun for documentation-only bookkeeping.

## Boundaries and next work

This slice deliberately prepares data before enabling execution. Functional Worker logic, bounded candidate-byte IPC and trusted publication, Model/Tool Gateway integration, production usage/effect receipts, complete interruption recovery, seven-role delegation, semantic/project-test verification, CLI/MCP, and product-wide V1 acceptance remain separate follow-up gates. No offline test substantiates the advertised paid-Provider cost/token/rework improvement numbers.

The human's explicit "继续开发" response after the linked reviewed plan authorized this source implementation; silence was not approval. The source slice is complete. Final documentation delivery becomes complete only when the controller's durable delivery ledger records independent approval of this exact candidate, its push, actual remote equality and clean checkout; those gates are pending at candidate creation, and their later ledger confirmation does not require another tracked-document patch solely to close status. Functional execution and the other follow-up gates above require their own implementation and evidence; this delivery grants no new data-retention or external-effect authority.
