# P3 Cross-Process Crash Matrix Design

**Status:** User-approved design (including the ApprovalGrantBound amendment)
**Date:** 2026-10-05
**Target:** P3 offline process-death coverage for already implemented durable control-plane and publication boundaries

## Goal

Replace response-loss-only coverage at remaining durable boundaries with deterministic real-process termination and restart assertions. The acceptance must show that a killed process leaves either a fully rolled-back transaction, one idempotently replayable committed result, or an explicitly unresolved effect whose budget/concurrency holds remain and which cannot be dispatched again.

This is a test-and-repair slice for existing P3 primitives, not an implementation of the functional Worker or a claim that P3 is complete.

## Existing behavior and evidence

The current branch already contains substantial process-death tests:

- `tests/unit/lifecycle/test_scheduler.py` kills child processes before/after Scheduler acceptance and reconciliation commits, after EffectIntent/EffectReceipt records, and at Artifact intent/blob/publication boundaries, then calls `RunRecoveryCoordinator` on a reopened store.
- `tests/integration/test_crash_matrix.py` kills a process after a fake Provider transport has begun dispatch; restart finds the call unresolved and the Gateway refuses same-identity replay before requesting credentials or calling transport.
- `tests/unit/isolation/test_workspace_publish.py` uses separate Python processes that exit during workspace publication, then checks journal-driven rollback or committed-publication recovery.
- `tests/integration/test_secret_broker_process.py` covers Broker child termination and one-shot request/replay behavior.
- `tests/integration/test_systemd_launcher.py` checks host-created termination receipts against the exact inactive systemd unit and empty cgroup.

There is also response-loss-only coverage in `tests/integration/test_crash_matrix.py`: `RuntimeError` wrappers model losing the return after budget reservation, ApprovalGrant consumption, settlement, EffectIntent, or EffectReceipt writes. Those tests establish idempotent API behavior but are not equivalent to killing the process. The Scheduler EffectIntent recovery tests persist control-plane events directly and do not execute a separate external-effect receiver. Existing isolated Worker IPC always returns `blocked`; there is no functional Worker whose complete dispatch/cancel/recovery lifecycle could be tested.

The approved-Tool path has another separately committed window. `ToolGateway.execute()` persists `ApprovalGrantBound` in the security stream before `ApprovalService.consume_and_intend()` atomically persists `EffectIntentRecorded`, `ApprovalGrantConsumed`, and `BudgetReserved` in the budget stream. If the host dies after the binding commit but before that atomic consume commit, the grant remains bound to the old Attempt; current `ApprovalService.bind_to_attempt()` rejects an already-bound grant, so it cannot be rebound even to that same Attempt. No external effect has launched and no effect reservation exists at this point, but the previous approval cannot be reused.

## Goals and non-goals

### Goals

1. Add true process-death coverage for the currently response-loss-only durable commit points where recovery correctness matters: budget reservation/settlement and atomic ApprovalGrant-plus-budget consumption.
2. Kill between the separately committed `ApprovalGrantBound` and atomic consumption. Prove the grant remains fail-closed and unused, with no effect or budget reservation; do not add automatic unbind/rebind.
3. Model an external effect with a test-only durable receiver separate from the Maestro EventStore. Kill the caller after the receiver applies the idempotency key but before the receipt is committed; prove restart leaves the effect unresolved and does not invoke the receiver again.
4. Exercise Artifact publication death windows after the durable publication intent and after the content-addressed blob is installed but before its Published event. Reopen the store and prove the candidate remains pending and is neither accepted nor automatically deleted.
5. Retain the existing real-kill Provider dispatch case as part of the matrix and verify unresolved calls still fail before credentials or transport on attempted replay.
6. Keep every failure point deterministic, bounded, and observable by a new process and a fresh database connection.

### Explicit non-goals

- Configuring a production Provider authority, validating real Provider signatures/receipts, settling live Provider usage, or making paid/public Provider requests. The current evidence verifier remains unavailable; unresolved Provider calls remain held.
- A functional Worker, whole Worker lifecycle/application-service stop coordination, automatic recovery dispatch, CLI/MCP, production Secret Broker backend, or V1 release approval. The current Worker remains blocked-only; existing systemd stop receipts are evidence for the launcher boundary only.
- Simulating host power loss, filesystem/controller failure, or loss of already committed storage. `SIGKILL` tests process death while using the current SQLite durability configuration; it is not a disk-failure guarantee.
- Changing event schemas, idempotency semantics, budget accounting, workspace publication policy, systemd isolation, or Provider retention behavior unless a failing regression demonstrates a required bug.

## Design decisions

### 1. Process-death harness

Use the repository's existing Python subprocess/multiprocessing patterns and a real temporary SQLite database. Each child reports arrival at one named fault point through a bounded parent/child pipe or equivalent local synchronization channel, then blocks. The parent waits against a monotonic deadline, sends `SIGKILL`, joins/reaps the child within a finite timeout, and opens new service/store instances for recovery. No assertion may depend on sleeps as the mechanism that places a process at a commit boundary.

For an uncommitted SQLite transaction, use a test-only trace callback on the child's SQLite connection to signal immediately before the `COMMIT` statement executes; kill at that barrier and verify SQLite rolls back the whole transaction. For the committed-but-response-lost case, use a test wrapper that delegates to the real operation, signals only after it returns from its durable commit, and blocks before replying. These barriers remain test-local: no public API, environment-controlled production failpoint, or runtime behavior change is introduced.

For file-backed ArtifactStore boundaries, use its existing internal `_record_intent` and `_record_metadata` methods as test-local synchronization points. The production publish sequence remains intent → object installation → Published metadata. Recovery is always performed from a fresh `SQLiteEventStore`, `ArtifactStore`, or owning coordinator, not from the child’s in-memory objects.

### 2. Durable and external-effect boundaries

The matrix has four assertion classes:

| Boundary | Kill point(s) | Required restart invariant |
|---|---|---|
| Approval binding before consumption | After `ApprovalGrantBound` commits in the security stream; before `consume_and_intend()` commits the budget-stream transaction | No `EffectIntentRecorded`, `ApprovalGrantConsumed`, or `BudgetReserved` event exists and no launcher/receiver ran. The grant remains bound to its original Attempt and is rejected for reuse/rebinding. Recovery never removes the binding; another execution requires a new approval request/grant. |
| Budget and Approval transactions | Before/after Budget reserve commit; before/after Budget settlement commit; before/after the atomic ApprovalGrant-plus-budget append commit | Pre-commit death leaves no partial events or changed projection. Post-commit death is idempotently replayed with exactly one reservation, settlement, or grant-consumption pair. |
| External effect and receipt | After durable EffectIntent; after a test-only durable receiver applies the exact idempotency key but before EffectReceipt commits | Recovery reports an unresolved/unknown effect and preserves budget/slot holds. No startup/recovery operation calls the receiver or replays the action. A later explicit operator/reconciliation path is not implemented by this slice. |
| Artifact publication | After `ArtifactPublicationIntent`; after blob installation but before Published metadata; existing complete-publication case | Recovery lists the exact Attempt-bound artifact as pending with `missing` or `orphaned_blob` content state as appropriate. It does not accept the candidate or automatically delete the object. A completed publication remains digest/size/provenance verified. |
| Provider dispatch | After the hanging fake transport confirms dispatch began; existing case retained | Recovery continues to report `dispatching`/unresolved. Same-identity replay is rejected before Broker access and before another transport call. No usage or no-effect fact is fabricated. |

The external-effect receiver is a test fixture with its own durable idempotency ledger, not a Provider emulator and not an evidence authority. Its counter/receipt makes duplicate invocation observable across process restart. A missing Maestro receipt after the action remains ambiguous even if the fixture can show that one test action occurred; only the configured production reconciliation authority could resolve that state.

### 3. Failure handling and resource invariants

- Every wait for a child, marker, database lock, or systemd operation is bounded. A timeout is a test failure; cleanup kills and joins the child and reports the exact fault point.
- A killed process before a durable transaction commit must not leave a partial cross-stream state. A process killed after commit but before response must not create a duplicate event on retry.
- A crash after an ApprovalGrant binding but before its atomic consume transaction leaves a one-use grant stranded on the bound Attempt. This slice proves no action or budget reservation occurred and preserves fail-closed semantics; it does not silently unbind, transfer, or revive the grant. A fresh approval flow is required to proceed.
- An external action without a durable EffectReceipt stays `outcome_unknown`; its budget reservation and Scheduler slot remain held. The test must prove no recovery path silently executes it again.
- A pending ArtifactPublicationIntent never becomes an accepted result solely because a digest-shaped blob exists. Existing global orphan inventory remains read-only and no crash test invokes cleanup/delete behavior.
- A Provider dispatch without terminal outcome or authoritative evidence stays unresolved; sender termination evidence alone does not prove Provider acceptance, billing, or no effect.
- Test errors and diagnostic output must not print credentials, prompts, raw Provider bodies, or secret fixture contents.

## Files expected to change

The implementation plan will begin with these current owners and add a focused helper only if existing fixtures cannot express a needed barrier:

- `tests/integration/test_crash_matrix.py` — convert/add response-loss cases into actual subprocess hard-kill and reopen cases; retain the Provider dispatch regression.
- `tests/unit/lifecycle/test_scheduler.py` — reuse the existing Run recovery, effect, and Artifact fixtures; avoid duplicate coverage where the existing real-process cases already assert the specified invariant.
- `tests/unit/isolation/test_workspace_publish.py` — retain the existing true process-death/rollback matrix; extend only if the end-to-end Run recovery matrix reveals a missing P3 boundary.
- `tests/integration/test_systemd_launcher.py` and `tests/integration/test_isolated_worker_process.py` — retain measured launcher/blocked-Worker evidence. Do not mark whole Worker coordination covered while no functional Worker/application service exists.
- `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`, `README.md`, and the relevant security note — update only after tests establish the new evidence, preserving P3/P5/V1 unchecked status.

Production source changes are conditional on a failing test demonstrating incorrect recovery or an inability to place a precise boundary through existing test-local hooks. Any such change must preserve the global rule that uncertain side effects are held and never silently replayed.

## Acceptance

1. A designated Linux test run covers every newly claimed hard-kill point without skips. Unsupported hosts may skip only live systemd checks according to existing platform policy; such a run is not evidence that the Linux/systemd acceptance passed.
2. Recovery uses fresh processes/connections and the product's normal journal, ledger, ArtifactStore, Gateway, or Run Recovery APIs.
3. The assertions in the matrix above pass, including the stranded-but-unconsumed ApprovalGrant case, exact event counts, reservation/slot state, pending artifact state, receiver invocation count, and no second Provider credential/transport call.
4. Existing cancellation/termination receipt tests continue to prove that the host receipt is returned only after the exact transient systemd unit is inactive and its cgroup is empty. This remains launcher evidence, not whole Worker lifecycle acceptance.
5. Run affected suites and the complete project gate: test coverage at least 90%, `compileall`, `pip check`, wheel build, and `git diff --check`. Report actual skips and failures; do not round a sub-90% result up.
6. Push the reviewed change to `codex/p3-systemd-termination-receipts`, verify the remote tip and clean tracked worktree, and do not merge to `main`.

## Remaining P3/V1 blockers after this slice

Passing this matrix closes only the offline process-death cases described here. It does not close the P3 checklist item for production Provider authority/signature verification or full Worker lifecycle coordination, and it does not provide a functional Worker, automatic recovery dispatcher, CLI/MCP, production Secret Broker backend, or paid Provider benchmark evidence. Unknown external effects remain unknown until a separately approved authoritative reconciliation path is configured. The bound-but-unconsumed ApprovalGrant remains a known liveness limitation: automatic safe recovery would require a separately designed append-only revoke/abandon-and-reapprove flow.
