# P3 Cross-Process Crash Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic real-`SIGKILL` restart coverage for the remaining durable P3 approval, budget, effect, and artifact boundaries without resolving or replaying unknown side effects.

**Architecture:** Reuse current SQLiteEventStore, BudgetLedger, ApprovalService, ToolGateway, ArtifactStore, and RunRecoveryCoordinator APIs; add a test-only pipe barrier that lets the parent kill a child at a named durable boundary. Recovery assertions always reopen a new process/store, and the existing fake Provider dispatch and systemd termination tests remain the authority for those separate boundaries.

**Tech Stack:** Python 3.12, `multiprocessing`/POSIX `SIGKILL`, SQLite WAL, pytest, existing coverage/build tooling; no new product dependency.

**Spec:** [2026-10-05-p3-cross-process-crash-matrix-design.md](../specs/2026-10-05-p3-cross-process-crash-matrix-design.md)

## Global Constraints

- No live/paid Provider request or production Provider authority/signature is introduced; the evidence verifier remains unavailable and unknown Provider calls remain held.
- The current Worker remains blocked-only; this plan does not claim full Worker lifecycle or automatic recovery-dispatch acceptance.
- Use real temporary SQLite databases and fresh recovery services/connections after child death.
- Use explicit bounded parent/child barriers, send `SIGKILL` from the parent, and join/reap every child under a finite timeout.
- A missing EffectReceipt remains unknown and retains budget/concurrency holds; recovery must not invoke an effect receiver or replay an action.
- A bound-but-unconsumed ApprovalGrant remains bound and unusable; this plan does not unbind, transfer, or revive it.
- Keep the P3 and V1 checklist items open until production Provider evidence and whole Worker lifecycle coordination are separately implemented and verified.
- After each independently reviewed task, commit and push to `codex/p3-systemd-termination-receipts`; never merge to `main`.

## Review Focus

1. A pre-COMMIT SQLite trace callback must stop only the intended operation after fixture setup; killing it must leave no partial transaction. Task 1 tests the armed boundary and verifies rollback.
2. A post-COMMIT barrier must fire only after durable commit but before the caller continues; replay must return the original result without adding events. Tasks 1 and 2 assert exact event counts.
3. A process death after ApprovalGrantBound but before consumption must leave no budget/effect records while refusing reuse or rebinding of the old grant. Task 2 covers same- and newer-Attempt attempts.
4. A receiver action followed by death before EffectReceipt must remain unknown and must not be invoked by recovery. Task 3 uses a separate durable test sink and checks its invocation count after recovery.
5. Artifact intent-only and blob-without-metadata deaths must remain pending with exact `missing`/`orphaned_blob` classification; recovery may not accept or delete either candidate. Task 3 checks both states through RunRecoveryCoordinator.

---

### Task 1: Reusable SIGKILL Barrier and Budget Boundaries

**Files:**
- Create: `tests/support/process_crash.py`
- Modify: `tests/integration/test_crash_matrix.py`
- Test: `tests/integration/test_crash_matrix.py`

**Interfaces:**
- Produces `block_at_crash_point(pipe: Connection, point: str) -> None` and `kill_at_crash_point(process: BaseProcess, pipe: Connection, *, expected_point: str, timeout_seconds: float = 10.0) -> None`, importing those types from `multiprocessing.connection` and `multiprocessing.process`. The child sends the validated ASCII point name with `send_bytes` then blocks on `recv_bytes(1)`; the parent polls for at most the timeout, reads at most 96 bytes, requires the exact point, sends `SIGKILL`, joins within the timeout, and requires exit code `-signal.SIGKILL`. On any assertion/error it still kills and joins a live child before raising.
- Consumes the current `SQLiteEventStore`, `BudgetLedger.reserve`, `BudgetLedger.commit_usage`, and `BudgetLedger.available` contracts.

- [x] **Step 1: Write the real-kill reservation and settlement tests before helper extraction**

Replace the response-loss-only assertion in `test_crash_after_budget_reservation_commit_replays_one_hold` with parameterized `before_commit` and `after_commit` children. Add the same two boundaries for `BudgetLedger.commit_usage`. Use `multiprocessing.get_context("fork")`, a duplex pipe, an actual SQLite path, `RunLimit(max_cost_minor=100, max_tokens=100)`, and `CostEstimate(amount_minor=12, currency="USD", token_limit=8)`. For settlement, seed the reservation in the parent before closing its store and let the child commit usage `{"input_tokens": 4, "cost_minor": 3}` with settlement key `"settlement-1"`. Use small inline pipe barriers in this test file for the first pass; do not import the not-yet-created shared helper. This is characterization of existing product behavior, so a valid crash-boundary test is expected to pass without production-code changes.

The child arms `SQLiteEventStore._connection.set_trace_callback()` only after opening the database and completing setup. At the pre-commit case, the callback blocks only when the normalized SQL is `COMMIT`; at the post-commit case, call the real Ledger method and block immediately after it returns. The inline child barrier sends the point name with `pipe.send_bytes(point.encode("ascii"))`, then blocks on `pipe.recv_bytes(1)`:

```python
def _trace_before_commit(pipe, point):
    def trace(statement):
        if statement.strip().upper() == "COMMIT":
            pipe.send_bytes(point.encode("ascii"))
            pipe.recv_bytes(1)
    return trace
```

Every test parent must issue `SIGKILL` after receiving the exact point name; it must not use `RuntimeError`, `os._exit`, or a sleep to simulate death.

- [x] **Step 2: Run the new characterization tests before adding the shared helper**

Run: `python3 -m pytest tests/integration/test_crash_matrix.py -k 'budget_reservation_sigkill or budget_settlement_sigkill' -q`

Expected: the four crash-boundary cases execute and pass against the existing ledger/recovery implementation using the inline test-only barriers. A collection/import failure is not an acceptable RED result; if the semantic assertions fail, investigate that durable-state defect before continuing.

- [x] **Step 3: Extract the test-only process barrier helper without changing product behavior**

After the inline tests pass, create `tests/support/process_crash.py` with the two signatures above and replace the local pipe/kill code with this shared helper. Validate point names as non-empty ASCII up to 96 bytes. `block_at_crash_point` sends exactly that name and then blocks on `recv_bytes(1)`. `kill_at_crash_point` polls for at most `timeout_seconds`, rejects a mismatched point, sends `os.kill(process.pid, signal.SIGKILL)`, joins for at most `timeout_seconds`, and checks `process.exitcode == -signal.SIGKILL`. In a `finally` block, kill and join any still-live process. Do not include payloads, credentials, or arbitrary exception text in barrier messages; keep all existing assertions green during this test-only refactor.

- [x] **Step 4: Verify budget recovery from a fresh EventStore**

For a reserve killed before COMMIT, reopen and assert no BudgetReserved event and zero reserved minor units; retry the same reserve key and assert exactly one reservation. For a reserve killed after COMMIT, assert the same reservation ID is returned on retry and there is exactly one BudgetReserved event. For settlement killed before COMMIT, assert the reservation remains held and no CostCommitted event exists, then retry once. For settlement killed after COMMIT, assert `used_minor == 3`, `reserved_minor == 0`, exactly one CostCommitted event, and an idempotent retry returns the original UsageRecord.

Run: `python3 -m pytest tests/integration/test_crash_matrix.py -k 'budget_reservation_sigkill or budget_settlement_sigkill' -q`

Expected: PASS with no skip on the designated Linux test host.

- [x] **Step 5: Run the existing crash/provider cases and commit/push**

Run: `python3 -m pytest tests/integration/test_crash_matrix.py -q`

Expected: PASS; the existing hanging fake Provider transport case still proves the dispatching call is unresolved and replay is blocked before Broker/transport.

Commit and push:

```bash
git add tests/support/process_crash.py tests/integration/test_crash_matrix.py
git commit -m "test: hard-kill budget operations at commit boundaries"
git push origin codex/p3-systemd-termination-receipts
```

### Task 2: Approval Binding, Consumption, and Grant Reuse

**Files:**
- Modify: `tests/integration/test_approval_tool_gateway.py`
- Test: `tests/integration/test_approval_tool_gateway.py`
- Consume: `tests/support/process_crash.py` from Task 1

**Interfaces:**
- Consumes `_approval_fixture(tmp_path, approval_attempt_authority=..., launcher=...)`, `_tool_request(...)`, `ApprovalService.approve`, `ApprovalService.bind_to_attempt`, `ApprovalService.consume_and_intend`, and `ToolGateway.execute`; extend the fixture with these optional parameters while retaining `_ApprovalAuthority` and `_Launcher` as their defaults.
- The child reopens the same database, recreates the real host service fixture, and injects only a test-local barrier around the selected method/SQLite COMMIT. The normal ToolGateway/ApprovalService logic still produces all persisted records.

- [x] **Step 1: Write the bound-but-unconsumed process-death regression**

In a new integration test, use `_approval_fixture(tmp_path)`. First call `gateway.execute` with origin Attempt `attempt-1`/generation 2, require `awaiting_approval`, and call `approvals.approve(..., "human-1", reason="approve exact read")`. Close the parent store. In the child, reopen `_approval_fixture` with a marker-writing launcher whose `launch(...)` creates the marker exclusively, wrap `bind_to_attempt` to call the real method and then block at `approval.bound-before-consume`, and call `ToolGateway.execute` for `attempt-2`/generation 3 with the issued grant. This is a characterization test for the approved fail-closed behavior; no product behavior change is intended.

The parent hard-kills at the named boundary and asserts one ApprovalGrantBound, zero EffectIntentRecorded, zero ApprovalGrantConsumed, zero BudgetReserved, no ToolExecutionStarted event, and no launcher marker file. Then assert binding the same grant to attempt-2 again raises `ApprovalInvalid`; create a reopened ApprovalService using the fixture's optional test authority that considers attempt-3/generation 4 current and assert that rebinding there also raises `ApprovalInvalid`. No code may remove the binding or manufacture a replacement grant.

- [x] **Step 2: Run the binding characterization test**

Run: `python3 -m pytest tests/integration/test_approval_tool_gateway.py -k 'sigkill_after_binding_before_consume' -q`

Expected: PASS: the existing ApprovalService/Gateway path leaves the grant bound but unconsumed after a real parent-issued `SIGKILL`. Do not replace the process death with an in-memory exception.

- [x] **Step 3: Add consume pre/post-COMMIT barriers and tests**

Add test-local consume barriers without changing production methods. For the consume pre-COMMIT case, arm the SQLite trace callback only after the `approval-bind:<grant>` security-stream append has returned, then block at `approval.consume-before-commit` when the later budget-stream transaction traces COMMIT. For the consume post-COMMIT case, wrap the real `consume_and_intend`, call it, then report and block at `approval.consume-after-commit` before returning to ToolGateway. Each independent case starts from a fresh temporary database with equivalent Gateway request IDs, grant scope, policy, and Attempt identities as the existing approval success test. Use the marker-writing test launcher plus persisted `ToolExecutionStarted` checks to prove neither case reaches launch; valid characterization tests are expected to pass with current product code.

Expected pre-COMMIT state: the binding exists, but no EffectIntentRecorded, ApprovalGrantConsumed, or BudgetReserved exists; no ToolExecutionStarted event or launcher marker exists. Expected post-COMMIT state: exactly one EffectIntentRecorded, one ApprovalGrantConsumed, and one BudgetReserved exist; no EffectReceiptRecorded, ToolExecutionStarted event, or launcher marker exists because the child was killed before launch. Replaying the same ToolRequest must raise `ToolRequestAlreadyUsed` before the launcher and must not add events.

- [x] **Step 4: Run focused Approval and budget tests**

Run: `python3 -m pytest tests/integration/test_approval_tool_gateway.py tests/unit/approvals tests/unit/budget -q`

Expected: PASS with the pre-consumption liveness limitation explicit in the test name and assertions; existing successful Approval execution remains unchanged.

- [x] **Step 5: Commit/push the reviewed Approval test task**

```bash
git add tests/integration/test_approval_tool_gateway.py
git commit -m "test: cover approval binding and consumption process death"
git push origin codex/p3-systemd-termination-receipts
```

### Task 3: Unknown External Effect and Artifact Publication Windows

**Files:**
- Modify: `tests/unit/lifecycle/test_scheduler.py`
- Test: `tests/unit/lifecycle/test_scheduler.py`
- Consume: `tests/support/process_crash.py` from Task 1

**Interfaces:**
- Consumes the module fixtures `run_setup`, `scheduler`, `routed_pair`, and `accept`; the production `RunRecoveryCoordinator.recover(run_id)` API; `ArtifactStore.publish_bytes`; and the current internal test hooks `_record_intent`/`_record_metadata`.
- Adds a test-local durable receiver with `apply(effect_id: str) -> None` backed by a SQLite file separate from the Maestro EventStore. Create `invocations(effect_id TEXT NOT NULL)` and `effects(effect_id TEXT PRIMARY KEY)` tables; each `apply` call uses `BEGIN IMMEDIATE`, inserts one invocation row, inserts into `effects` with `ON CONFLICT DO NOTHING`, then commits. This lets the test distinguish a repeated call from the receiver's idempotent final state across process restart.

- [x] **Step 1: Write the external-action-before-receipt process-death test**

Seed a real Run, accepted Attempt, and budget reservation in the parent using `run_setup`, `routed_pair`, `scheduler`, and `accept`. In the child, reopen the EventStore and receiver database, append one exact Attempt-bound EffectIntentRecorded event to the budget stream, commit one call to the separate durable receiver, report and block at `effect.applied-before-receipt`, and omit EffectReceiptRecorded. The parent hard-kills, opens new EventStore and receiver connections, calls `RunRecoveryCoordinator.recover("run-1")`, closes and reopens the EventStore, and calls recovery again so both reads use fresh connections. This characterizes already-implemented no-replay semantics; no product behavior change is intended.

Assert the recovered effect is `outcome_unknown`, the accepted Attempt still occupies one active slot, `recovered.budget.reserved_minor` still includes the accepted Attempt's reservation, no EffectReceiptRecorded exists, and the receiver has exactly one invocation row and one effect row. Repeating read-only recovery must leave both sink counts unchanged. This test does not claim to prove a real Provider effect or supply authority to settle it.

- [x] **Step 2: Run the external-effect characterization test**

Run: `python3 -m pytest tests/unit/lifecycle/test_scheduler.py -k 'external_action_before_receipt_sigkill' -q`

Expected: PASS: after the fake receiver action commits but before a receipt is written, recovery keeps the effect unknown and does not invoke the receiver again. The durable receiver fixture is test-only, not a Provider emulator or evidence authority. A test that directly appends EffectReceiptRecorded or runs recovery in the child does not satisfy this case.

- [x] **Step 3: Convert Artifact recovery points to parent-issued SIGKILL**

Modify `test_run_recovery_attributes_interrupted_artifact_publications` so each crash fixture first seeds an accepted Attempt and its budget reservation. Its child wrapper calls `block_at_crash_point` after `_record_intent(record)` and after the blob has been installed but before `_record_metadata(record)`. The publication source and recovered pending intent must preserve exact Run/Node/Attempt/fencing-generation provenance from that accepted Attempt. The parent uses `kill_at_crash_point` at `artifact.after-intent` and `artifact.after-blob`; require `-SIGKILL` rather than `os._exit` return codes. Retain assertions for `pending_artifacts`, exact `missing`/`orphaned_blob` state, Run/Node/Attempt/fencing provenance, and read-only orphan inventory. Do not call candidate admission or artifact deletion.

- [x] **Step 4: Verify fresh-process recovery for effects and artifacts**

Run:

```bash
python3 -m pytest -o addopts= tests/unit/lifecycle/test_scheduler.py -k 'external_action_before_receipt_sigkill or interrupted_artifact_publications or replays_effect_state_after_process_death' -q
```

Expected: PASS with no skip on the designated Linux host; each child exits from parent-issued SIGKILL, and recovery assertions run through newly opened stores/coordinators.

- [x] **Step 5: Commit/push the reviewed recovery-boundary tests**

```bash
git add tests/unit/lifecycle/test_scheduler.py
git commit -m "test: hard-kill external effect and artifact recovery windows"
git push origin codex/p3-systemd-termination-receipts
```

### Task 4: Evidence Documentation and Full Acceptance

**Files:**
- Modify: `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`
- Modify: `README.md`
- Modify: `docs/security/run-recovery.md`
- Modify: `docs/superpowers/plans/2026-10-05-p3-cross-process-crash-matrix.md` (selector/provenance corrections and completion tracking)
- Test: full repository acceptance commands

**Interfaces:**
- Consumes exact test results and commit hashes from Tasks 1–3.
- Produces an evidence note limited to measured SIGKILL/reopen cases. The P3 checklist item for production Provider authority and full Worker lifecycle remains unchecked.

- [x] **Step 1: Run all affected focused tests**

Run:

```bash
python3 -m pytest -o addopts= tests/integration/test_crash_matrix.py tests/integration/test_approval_tool_gateway.py tests/unit/approvals tests/unit/budget tests/unit/lifecycle/test_scheduler.py tests/unit/isolation/test_workspace_publish.py tests/integration/test_systemd_launcher.py tests/integration/test_isolated_worker_process.py -q
```

Expected: PASS on the designated Linux/systemd acceptance host with no skips for required SIGKILL cases; report any systemd-only skip accurately on unsupported hosts.

- [x] **Step 2: Document measured evidence without closing P3/V1**

Add a dated note to the full V1 plan and README with the exact focused/full test commands, results, platform, and precise crash points proven. State that the bound-but-unconsumed grant requires a fresh approval, external effects without receipts stay unknown, Provider authority remains unavailable, the Worker is blocked-only, and P3/V1 stay incomplete.

- [x] **Step 3: Run the exact full quality gate**

Run:

```bash
python3 -m pytest --cov=orchestrator --cov-report=term-missing --cov-fail-under=90
python3 -m compileall -q src
python3 -m pip check
python3 -m pip wheel . --no-deps --wheel-dir /tmp/maestro-p3-crash-wheel
git diff --check
```

Expected: every command exits 0; coverage is at least 90.00%; the designated Linux run has no skips for new crash tests. Record exact test counts, coverage, and platform in the evidence docs; report the wheel build result without claiming it proves runtime deployment compatibility.

- [x] **Step 4: Review the branch, commit docs/evidence, push, and verify remote state**

Inspect `git diff --stat` and the full diff. Confirm there are no production failpoints, no live Provider calls, no new dependency, no changes to secret policy/idempotency semantics, and the P3/V1 completion boxes remain unchecked. Commit only the documentation/evidence files for this task, push to `origin codex/p3-systemd-termination-receipts`, compare local HEAD with `git ls-remote`, and require a clean tracked worktree.

```bash
git add README.md docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md docs/security/run-recovery.md docs/superpowers/plans/2026-10-05-p3-cross-process-crash-matrix.md
git commit -m "docs: record P3 crash matrix acceptance evidence"
git push origin codex/p3-systemd-termination-receipts
```

## Execution and review protocol

The user previously selected **Subagent-driven** execution. Use a fresh implementer and independent reviewer for each task, sequentially, because each task consumes tested APIs from the preceding task and a missed crash invariant could release resources or duplicate an external action. Keep reviewer scope limited to the task diff plus its tests; address all Important findings before proceeding. After Task 4, run one independent whole-slice review and close its findings before reporting completion.

If a regression demonstrates a product-semantic defect, implement only a correction that preserves this approved spec's fail-closed invariants. If a fix would automatically unbind/rebind approvals, settle Provider evidence, release an unknown hold, or introduce new Worker/recovery semantics, stop and request a design amendment before making that change.
