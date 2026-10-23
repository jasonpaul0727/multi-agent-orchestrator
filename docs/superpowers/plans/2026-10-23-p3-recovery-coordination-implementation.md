# P3 Recovery Coordination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover durable same-node retry opportunities after restart, admit one successor under Scheduler authority, and prepare its WorkerTask only after explicit exact input re-supply.

**Architecture:** Project existing Scheduler recovery/classification/admission events instead of adding a second recovery state machine. A trusted host service derives the successor request, routes from typed observations, and nests existing Scheduler admission inside one SQLite transaction. Task preparation is a separate post-commit authority check; construction and queries execute no Worker or Provider request.

**Tech Stack:** Python >=3.12, Pydantic 2, SQLiteEventStore checked transactions, existing ModelRouter, WorkerInputService and pytest/coverage; existing systemd integration backend for the final regression gate.

**Spec:** `docs/superpowers/specs/2026-10-16-p3-runtime-recovery-coordination-design.md`, explicitly approved 2026-10-23. Implementation base: `621cacd7931725ef7c7bf97132192925bc32e0f8` after P0 closure; the approved-spec recording commit follows that base.

## Global Constraints

- Preserve explicit input re-supply. No plaintext task persistence, prompt-derived identifiers, task text in events/errors/logs, or host callbacks inside a database lock.
- Only persisted `same_model_retry`, `same_tier_fallback`, and `capability_escalation` authorizations may admit a successor. Report new-child/repair planning without creating nodes.
- One checked transaction validates current source/input/contract/outcomes, routes, and admits through Scheduler; rollback leaves no authorization consumption, reservation, Agent, or slot.
- Unknown Provider/effect outcomes retain resource holds and never cause a new call or successor admission. Recheck current Provider journal state inside the admission transaction.
- Existing Scheduler, lifecycle and proof-gated success remain authoritative. No new scheduler link event or automatic effect replay/cleanup; no Worker/process launch in this slice.
- Use the existing isolated `codex/p3-systemd-termination-receipts` worktree only. Ordinary push is authorized; merge, PR, deploy, destructive cleanup, shared dependency installs, credentials and paid Provider requests are not.
- Each task has three independently useful feature groups, each verified, committed and ordinarily pushed. Task review gates completion. Never use empty commits to satisfy cadence.
- At most five completed items per America/Los_Angeles day; count the P0 Task 6 close-out already completed on 2026-10-23. Count this planning/preflight as one item. Stop when the observable account window has <=50% remaining; check before dispatch and after each feature push. An account window is the available meter, not a fabricated total-token balance.
- Final slice gate: full suite, >=90% aggregate statement coverage, necessary live isolation zero skips, compile/dependency/package checks, task review and whole-slice review. This prerequisite slice cannot close the full P3 checklist.

## Review Focus

- Lost post-commit reply with new observations/times must resolve the existing successor without recomputing admission or reserving again (Tasks 2/4).
- A known-failure plan followed by a currently unknown Provider call on the same node must remain reconciliation-required even with a new Attempt identity (Tasks 1/2/4).
- A copied unsafe model, malformed history, or alternate source/input identity must fail with detached bounded errors and zero persistent changes (Tasks 1/2/3).
- Cancellation, expiry or newer descendants between admission and preparation must preserve committed admission while withholding stale WorkerTask data (Tasks 3/4).
- Health/budget observations that change before the SQLite lock is acquired must not bypass current durable checks; no host observation callback may execute while that lock is held (Tasks 2/4).

## File and interface map

- `runtime/recovery_opportunities.py`: strict public metadata/result types, sanitized errors, and validated read-only projection of existing recovery records. No routing or execution.
- `runtime/recovery_continuation.py`: data-only routing input envelope, deterministic successor IDs, atomic route/admit, and post-commit preparation orchestration.
- `application.py`: construct the service and expose ready-gated host methods; startup remains recovery-only.
- `tests/support/recovery_continuation.py`: real SQLite/frozen-contract/known-failure/health/costing fixtures, snapshot and corruption utilities. Test-only crash hooks stay in test helpers.
- `tests/unit/runtime/test_recovery_opportunities.py`, `test_recovery_continuation.py`: projection, admission, validation/privacy and preparation behavior.
- `tests/integration/test_recovery_continuation.py`: independent processes, contention and parent-issued SIGKILL at concrete durable seams.
- `docs/security/run-recovery.md`, `control-plane-startup.md`, `README.md`, `PROJECT_CONTEXT.md`: current explicit-host continuation capability and remaining P3 boundaries.

### Task 0: Approved design, concrete plan and reviewed preflight

- [x] **Step 1 / feature A:** Record the human's specific design approval, historical versus current source baseline, and ordinary push of the approved spec. Commit: `c3564a0`.
- [x] **Step 2 / feature B:** Write this concrete implementation plan; self-review coverage/types/task interfaces and ordinarily push it. Commit `aae52e4`. User explicitly requested planning followed by development, so no additional plan-confirmation wait is inserted.
- [x] **Step 3 / feature C:** Independently review the plan against the approved spec and current APIs; resolve findings, run the focused unchanged-source baseline, record execution preflight/conflict table and cadence/usage ledger, then ordinarily push the reviewed plan/baseline record. The review's missing atomic post-commit preparation gate was corrected in Task 3 and scoped re-review marked it addressed with no new breakage. Fresh baseline: 149 passed, 181 deselected, 0 failed/errors/skipped in 54.39s. No old P0 result is counted as a fresh P3 run.

### Task 1: Validated recovery opportunity inventory

**Files:** Create `src/orchestrator/runtime/recovery_opportunities.py`, `tests/unit/runtime/test_recovery_opportunities.py`, `tests/support/recovery_continuation.py`.

**Interfaces:**
- `RecoveryContinuationError(code: str)`: safe owned error; public codes `recovery_continuation_invalid`, `recovery_continuation_failed`, `recovery_input_invalid`, `recovery_input_mismatch`; detach raw causes/contexts/arguments.
- `RecoveryOpportunity`: frozen strict metadata with `run_id`, `node_id`, `source_attempt_id`, positive `source_fencing_generation`, `source_decision_hash`, optional `authorization_hash`/`evidence_hash`/`action`, `status`, `reason`, optional successor `AcceptedModelRoute`. Status is `ready`, `already_admitted`, `reconciliation_required`, `new_child_required`, or `blocked`. No task text or observation payload.
- Internal frozen `RecoverySourceRecord`: opportunity, validated RecoveryPlan/FailureClassification, source StoredEvent, optional successor StoredEvent.
- `load_recovery_records(scheduler: Scheduler, *, run_id: str, events: list[StoredEvent]) -> tuple[RecoverySourceRecord, ...]`: internal, called only inside a checked transaction; recovery consistency first, then validate/join exact source/classification/plan/consumption and historical verification commitment.
- `RecoveryOpportunityService(scheduler: Scheduler).list_opportunities(*, run_id: str) -> tuple[RecoveryOpportunity, ...]`: sorted by node/source generation/source Attempt, one current snapshot, outer checked callback returns `None`.

- [ ] **Step 1 / feature A RED:** Add `test_reopen_lists_known_failure_without_writes`, `test_unknown_and_new_child_plans_report_reasons`, `test_query_executes_no_worker_or_provider`; assert exact literal status/source/hash bindings, unchanged SQLite dump/budget/Agent/slot state and no process/request activity. Run missing API tests and retain expected RED output.
- [ ] **Step 2 / feature A GREEN + push:** Project existing validated recovery events; known unconsumed latest failed source is ready; unknown source or unresolved Provider call on the same Run/node is reconciliation-required. Block cancelled/paused/terminal Runs and stale unconsumed sources. Verify focused tests and push `feat: discover durable recovery opportunities`.
- [ ] **Step 3 / feature B RED/GREEN + push:** Add consumed-successor/reopen/older-source tests. Join existing `RoutingDecisionAccepted.recovery_authorization_hash`, return its exact accepted route without synthesizing a RoutingDecision or writing an event. Classify blocked and new-child plans without losing their durable reason. Verify and push `feat: resolve consumed recovery successor identities`.
- [ ] **Step 4 / feature C RED/GREEN + push:** Add malformed ID/bool generation, unsafe nested model, orphan/duplicate/corrupt history, missing historical input commitment, unknown journal and privacy tests. Revalidate serialized history, source AttemptContext and frozen contract; generic detached errors hide payloads. A legacy source with no verification commitment is `blocked` with `reason="source_input_unavailable"`; malformed/inconsistent committed data raises the sanitized owned error. All rejected cases preserve durable state. Run focused tests, the full project suite once on final task state, self-review, and push `test: harden recovery opportunity trust boundaries`.
- [ ] **Step 5:** Independent task spec/quality review of all three feature commits, fix/re-review any blocking findings, then mark complete in the plan ledger.

### Task 2: Atomic deterministic successor admission

**Files:** Create `src/orchestrator/runtime/recovery_continuation.py`, `tests/unit/runtime/test_recovery_continuation.py`; extend `tests/support/recovery_continuation.py`.

**Interfaces:**
- Consume Task 1 record projection and errors; do not alter its event authority.
- `RecoveryRoutingInputs`: strict frozen Pydantic envelope of `eligibility: EligibilitySnapshot`, `policy_manifest: PolicyManifest`, `tokenizer_snapshot: TokenizerSnapshot`, `fx_snapshot: FXSnapshot`; data only, no provider/Broker/route callback.
- `RecoveryContinuationResult`: strict frozen `status`, bounded owned `reason: str`, `opportunity`, optional `accepted_route: AcceptedModelRoute`, optional `task: WorkerTask` (`repr=False`). Status adds `waiting_for_input`, `waiting_for_routing`, `admitted`, `prepared`, `successor_inactive` to the query outcomes. A task is present only for `prepared`; admitted/already-admitted/inactive/prepared results carry the exact accepted route. Never invent a complete historic RoutingDecision.
- `RecoveryContinuationService(scheduler: Scheduler, worker_inputs: WorkerInputService)` and `.admit(*, run_id: str, node_id: str, source_attempt_id: str, source_fencing_generation: int, task_text: str | None = None, routing_inputs: RecoveryRoutingInputs | None = None, accepted_at: datetime, lease_expires_at: datetime) -> RecoveryContinuationResult`.
- Deterministic digest: SHA-256 of UTF-8 canonical JSON (sorted keys, compact separators) over `{schema: "maestro.recovery-successor.v1", run_id, node_id, source_attempt_id, source_fencing_generation, authorization_hash}`. IDs: `recovery-attempt-<64 hex>` and `recovery-request-<64 hex>`; next generation is source+1. IDs contain no input text.

- [ ] **Step 1 / feature A RED:** Add `test_known_failure_admits_derived_successor_once`, `test_changed_input_cannot_admit`, `test_missing_input_or_routing_waits_without_effects`, and exact counter/frozen-source tests. Assert one new acceptance/authorization consumption/reservation/Agent/slot, canonical source input equality, deterministic IDs and zero prompt persistence.
- [ ] **Step 2 / feature A GREEN + push:** In outer `append_checked("scheduler", "global", "recovery-continuation-snapshot", callback)`, reload Task 1 records; validate exact source/contract/input, derive request and counters from persisted evidence, call real ModelRouter then nested Scheduler with canonical input commitment and inherited verification admission. Outer callback returns `None`. Verify and push `feat: admit deterministic recovery successors atomically`.
- [ ] **Step 3 / feature B RED/GREEN + push:** Add fallback/escalation and replay tests with real multi-model registry/Router. After committed response loss, resolve the consumed authorization first: new observations, acceptance times or missing routing must never readmit. A valid prior consumption by existing Scheduler APIs also returns its actual identity even if it predates the new deterministic ID scheme. Re-supplied changed input is still rejected. Missing text returns waiting-for-input plus exact existing identity. Inactive/expired/newer successor returns successor-inactive rather than stale preparation. Verify and push `feat: replay recovery admissions without duplicate holds`.
- [ ] **Step 4 / feature C RED/GREEN + push:** Add current Provider/effect unknown, cancellation, budget/Agent/depth/concurrency/attempt caps, revoked/denied policy, expired tokenizer/FX, stale policy/budget/health refs, and unsafe-model/privacy tests. Under the transaction compare eligibility budget currency/version/available cost/tokens to BudgetLedger; compare each provider/model HealthAggregateRef to current HealthController state using the frozen preset health policy. Frozen config/registry/contract come from lifecycle. Policy/secret availability/revocation metadata are trusted host facts evaluated by ModelRouter, not independently fetched credentials or an MCP identity claim. Recheck unresolved Provider calls before any admission. Run focused and full suites on final state, self-review, and push `fix: enforce current recovery admission safety gates`.
- [ ] **Step 5:** Independent task review and covering fixes/re-review before downstream work.

### Task 3: Ready-gated explicit continuation and WorkerTask preparation

**Files:** Modify `src/orchestrator/runtime/recovery_continuation.py`, `src/orchestrator/application.py`; extend unit tests and security docs.

**Interfaces:**
- `RecoveryContinuationService.continue_recovery(...) -> RecoveryContinuationResult`: same keyword signature as `.admit`; after its admission transaction commits, enter a second `append_checked("scheduler", "global", "recovery-preparation-snapshot", callback)` transaction. Recheck the Run is running, the exact successor is latest/active/unexpired, and no current same-node Provider/effect outcome is unknown; only then nest existing `WorkerInputService.prepare_worker_task` with exact identity/text. The outer callback returns `None`. No host observation callback executes under either lock.
- `ControlPlaneApplication.list_recovery_opportunities(*, run_id: str) -> tuple[RecoveryOpportunity, ...]` and `.continue_recovery(...) -> RecoveryContinuationResult`: same service keyword signatures, always `_require_ready()` first. Construct services on the same existing Scheduler/store; no launch during construction/bootstrap/query/continuation.
- A preparation failure returns committed identity with `status="admitted"`, no task, and bounded `reason="task_preparation_failed"`; it does not roll back or repeat admission. A later explicit call can prepare the same still-active successor. Unknown/inactive/paused authority returns reconciliation-required/successor-inactive/blocked without task data, retaining any committed route and resources in the result.

- [ ] **Step 1 / feature A RED/GREEN + push:** Add ready/closed/startup/no-dispatch query API tests; construct shared-store services and expose the ready-gated inventory. No Worker/process/transport object is created by the new service. Push `feat: expose ready-gated recovery opportunity host API`.
- [ ] **Step 2 / feature B RED/GREEN + push:** Add `test_reopen_recovery_prepares_exact_successor_task`, `test_preparation_failure_retains_committed_admission`, and retry-after-failure tests. Expose explicit application continuation and return a real WorkerTask only inside the second checked authority transaction. Inject failure after real admission to prove durable resources remain single and subsequent retry resumes preparation. Push `feat: prepare recovery tasks after committed admission`.
- [ ] **Step 3 / feature C RED/GREEN + push:** Add cancel/pause/expiry/newer-descendant and newly unknown Provider/effect outcomes between admission/preparation tests, supplied-input mismatch on replay, safe result/error repr and invalid public arguments. Assert no task under stale/unknown authority, no additional admission, retained route/resources and no plaintext in DB/WAL/log/error surfaces. Update host API docs with typed observation trust sources, input re-supply and remaining full-P3 gates. Run focused/full suites on final task state, self-review, and push `test: protect resumed task preparation authority`.
- [ ] **Step 4:** Independent task review and fixes/re-review; mark task complete only after both verdicts pass.

### Task 4: Real process races, crash recovery and final acceptance

**Files:** Create `tests/integration/test_recovery_continuation.py`, `docs/validation/2026-10-23-p3-recovery-coordination.md`; extend support fixture and README/PROJECT_CONTEXT/run-recovery documentation.

- [ ] **Step 1 / feature A RED/GREEN + push:** Two spawned processes with independent application/store connections contend for the same authorization using deterministic barriers. Test same and conflicting text/observations, plus cancellation or health/budget change winning the outer lock. Assert one successor/consumption/reservation/Agent/slot, exact held usage and no loser writes. Reuse `tests/support/process_crash.py`; bounded waits and child reaping are mandatory. Push `test: prove recovery continuation process serialization`.
- [ ] **Step 2 / feature B RED/GREEN + push:** Parent-issued SIGKILL after nested admission but before outer commit leaves no successor/resources; kill after outer commit before response reopens to exact same successor. Kill between admission/preparation and retry with exact input. Unknown-call case forbids Broker/transport/Worker activity across fresh reopen. Scan durable DB/WAL and reports for sentinel input. Hooks live in test helpers/monkeypatches only. Push `test: prove recovery continuation crash and lost-reply semantics`.
- [ ] **Step 3 / feature C verification + push:** Run full coverage and required real isolation matrix; compileall, pip check, wheel build/content check using existing available build dependencies; inspect all new security branches. Record exact source SHA, commands, pass/fail/skip counts, coverage, hashes, environment and review decisions in validation docs. Clearly label this recovery-to-admission/preparation slice as incomplete full P3. Push `docs: record recovery coordination acceptance evidence`.
- [ ] **Step 4:** Independent task review then broad whole-slice review from implementation base through tested final head. Fix important findings via one implementer and scoped re-review; record final local/remote SHA. Preserve evidence ledger for recurring continuation; no automatic transition to the deferred P5 plan or credential/paid Provider gate.

## Verification commands and execution accounting

Run from the isolated checkout, using existing `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python` as `PY` and `PYTHONPATH=src:.`.

- Baseline: `$PY -m pytest -o addopts= tests/unit/runtime/test_worker_input_service.py tests/integration/test_worker_input_recovery.py tests/unit/lifecycle/test_scheduler.py -k 'worker or recovery' -q`.
- Task-focused: `$PY -m pytest -o addopts= tests/unit/runtime/test_recovery_opportunities.py tests/unit/runtime/test_recovery_continuation.py -q` (only name files once created); integration: `$PY -m pytest -o addopts= tests/integration/test_recovery_continuation.py -q`.
- Full: `$PY -m coverage run -m pytest -o addopts= -q`; `$PY -m coverage report --fail-under=90` (retain JUnit/logs and separate coverage file under this plan's ignored workspace).
- Isolation: `$PY -m pytest -o addopts= tests/integration/test_p0_workspace_acceptance.py tests/integration/test_overlay_candidate_launcher.py tests/integration/test_secret_broker_process.py tests/integration/test_systemd_provider_sender.py tests/integration/test_systemd_launcher.py tests/integration/test_workspace_worker_execution.py tests/integration/test_workspace_isolation_admission.py tests/integration/test_workspace_removal_recovery.py -q`; required cases must have zero skips.
- Health/package: `$PY -m compileall -q src tests`; `$PY -m pip check`; existing build-capable Python `-m build --wheel --no-isolation --outdir <private-artifact-dir>`; verify tracked Python module inventory and no private artifacts; `git diff --check`.
- Root owns ordinary push and usage/daily accounting after each group; implementers commit a verified feature and hand back its SHA so root can push before the next feature. Task completion requires >=3 successful non-empty pushes plus independent spec/quality review. Preserve all failed RED/verification evidence and report it honestly.

## Planning decisions and handoff

The explicit 2026-10-23 instruction authorizes this plan followed immediately by development using the preserved subagent method. No second approval is needed. Deterministic existing-event joins replace a new link event because Scheduler already persists authorization consumption and nested same-stream append versions would otherwise compete. Only exact accepted-route identity is replayed because complete historical RoutingDecision objects are not stored.

The new task ledger records preflight shared-file/interface checks, reviewed tasks, each feature push and the LA-day completion count. P0's final 2,611-test run is prior evidence for unchanged baseline source only; this plan records its own fresh baseline and final tests.

Planning/preflight evidence (2026-10-23): focused baseline command above completed with 149 passed / 181 deselected / 0 failed / 0 errors / 0 skipped, 54.39s. Retained in this plan's ignored workspace as `baseline.log` (SHA-256 `a6f7ad751cb6ea3bd5a99f8c5c435e4658e928d04586621f6bce23e0009f7be2`) and `baseline.xml` (`cf9695868a8e88380864f171a4a3f3b79fc66d5612cbcda877ead60eed2e97c6`). The independent architecture review found one blocking plan omission: post-admission preparation needed its own atomic running/active/unknown-outcome gate. Task 3 now specifies that transaction and races; scoped re-review returned ADDRESSED with no new breakage. Root's preflight shared-interface/self-consistency table is recorded in the plan ledger.
