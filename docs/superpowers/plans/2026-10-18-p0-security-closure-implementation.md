# P0 Security Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete both remaining P0 checklist entries with an actual isolated tool Worker-to-Gateway workspace-write path, recoverable create/update/delete/rename, fail-closed platform admission, reproducible security/recovery evidence, independent review and ordinary push.

**Architecture:** Reuse the frozen Attempt, policy, systemd launchers, exclusive workspace lease and durable Gateway publication streams. Export a versioned final-view change manifest after untrusted descendants stop; independently validate it and apply it through retained, recoverable journals. A separate single-tool Worker proposes a bounded request over stdin/stdout; the ready-gated host derives authority and routes the request, never trusting Worker claims as authorization.

**Tech Stack:** Existing Python >=3.12, Pydantic 2, SQLite, Linux namespaces/systemd/OverlayFS/Landlock/seccomp and pytest/coverage. No new production dependency or shared installation.

**Spec:** `docs/superpowers/specs/2026-10-18-p0-security-closure-design.md` (human approved); also the approved `2026-09-13-permissions-security-isolation-approval-design.md` and original V1 plan P0.

Status: human confirmed execution plan on 2026-10-18 ("确认"); implementation starting, no source task completed.
Base: `ae12d40458e6c8cf41c925ca52cf01a5c965098f`, existing isolated
`codex/p3-systemd-termination-receipts` branch. Preserve the untracked P3 proposal.

## Global Constraints

- Existing isolated worktree only; ordinary push existing branch only after all P0 gates. No PR, merge or deployment.
- Complete reversible create/update/delete and rename semantics within declared bounds.
- Define atomic application as per-path atomic replacement inside a durable, serialized, all-or-rollback transaction, NOT a single reader-visible atomic swap of a whole arbitrary directory.
- Unknown commit or stop status retains journals, staging and unresolved intent; no optimistic cleanup.
- No write into `.git`, control/config/secret roots, external paths or protected names becomes permitted.
- Worker receives only prepared input and opaque tool references; no EventStore, host paths, workspace lease, Broker socket/nonce, credentials or final-state authority.
- Approval cannot revive the originating Attempt; a newer accepted Attempt is required.
- No Worker message can bypass the existing verifier/acceptance proof chain.
- General model execution, P3 auto-recovery, deferred P5 Artifact publisher and P6 MCP implementation stay outside this plan.
- Production credential store remains unavailable; no environment/argv/file credential delivery or public/paid Provider calls.
- Full suite >=90% total coverage; required isolation tests zero skips; independent specification/quality review per task and final tested-range review.
- Fixtures may mutate only test-created private workspaces; no real project data is deleted to demonstrate the feature.

## Review Focus

1. Lost response after filesystem commit must replay a receipt, not repeat a command (Tasks 2, 4, 5).
2. A directory removal must not delete concurrent/untracked descendants absent from its verified preimage (Tasks 1, 2).
3. A generic failed launch may have started a process: only proven pre-start failure is a safe block (Tasks 3, 4, 5).
4. Worker-created context/capability/profile/identity claims must not authorize another Attempt or workspace (Tasks 3, 4).
5. Kernel/runtime/environment drift must fail actual isolation checks; a mocked probe or skipped positive live test cannot close P0 (Tasks 3, 5, 6).

## Delivery boundaries and commands

Execute Tasks 1-6 serially: each produces an independently reviewable deliverable.
Record initial and per-task Git SHAs, RED/GREEN commands and reviews in this plan's
own `.superpowers/sdd/` ledger. Do not use the P3 or P5 ledger for implementation.
Task implementers must read their extracted brief plus Global Constraints, not
recreate requirements from a summary. No parallel source implementers.

Commands below run INSIDE WSL Ubuntu-24.04 with cwd
`/mnt/c/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator`.
Use `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python` as `PY` after
checking it exists; `PYTHONPATH=src:.` ensures tests import this checkout rather
than the main checkout's editable installation. Do not install missing tools in
the shared environment. Report an unavailable prerequisite instead.

`PYTHONPATH=src:. "$PY" -m pytest -o addopts= <named tests> -q` is the focused
test command. RED means the named missing behavior fails, not an unrelated import
or setup error. GREEN requires all named tests pass. Retain real subprocess/live
test evidence; ordinary unit-test skips on unsupported development hosts do not
satisfy the mandatory supported-profile acceptance run.

### Task 1: Canonical final-view manifests with safe reversible removals

**Files:**
- Modify: `src/orchestrator/isolation/workspace.py` (diff model/validation/conflicts).
- Create: `src/orchestrator/isolation/workspace_view.py` (bounded no-follow final-view inventory/export).
- Modify: `src/orchestrator/isolation/_overlay_bootstrap.py`, `overlay_launcher.py`, `__init__.py`.
- Test: `tests/unit/isolation/test_workspace_view.py`, existing `test_overlay_diff.py`, `test_overlay_launcher.py`, `test_overlay_bootstrap.py`, `test_workspace_snapshot.py`.
- Live test: `tests/integration/test_overlay_candidate_launcher.py`.

**Interfaces:**
- Preserve `export_overlay_diff(lower_root, upper_root, candidate_root, *, max_entries=100_000, max_bytes=1_073_741_824, max_depth=256) -> WorkspaceDiff` as a legacy add/modify-only API; it must still reject raw whiteouts.
- Extend `WorkspaceDiff` with trailing `schema_version: int = 1`; schema 1 hashes/fields remain compatible. Schema 2 adds `delete` to `WorkspaceDiffEntry.operation`; deletion has `mode=0`, `size=0`, `digest=sha256(empty bytes)`, a required `baseline_digest`, and no candidate payload at that path.
- Produce `export_workspace_view_diff(lower_root: str|Path, view_root: str|Path, candidate_root: str|Path, *, max_entries: int=100_000, max_bytes: int=1_073_741_824, max_depth: int=256) -> WorkspaceDiff`, schema 2. It scans final visible view, not raw upper metadata.
- Existing `validate_overlay_candidate(...) -> None` and `check_workspace_publish_conflicts(...) -> WorkspaceConflictReport` accept schema 1/2 and reject unknown versions.

- [ ] **Step 1: Write failing unit tests** for unchanged view/no-op, file/directory/symlink create/modify/remove, file and subtree rename, rename overwrite, all-descendant deletion manifests and forged tombstones. Assert exact operations, changed payload byte accounting, unchanged lower, deterministic sorted manifest and independent revalidation.
  The deletion assertion must include `operation == "delete"`, `baseline_digest is not None`, `size == 0` and absent candidate payload. Directory preimage hashes include sorted descendant relative names/types/content/targets, so an extra live child conflicts. Existing snapshot normalization does not become a grant to change protected paths; capture actual live metadata separately in Task 2 backups.
  Parameterize traversal/backslash/control paths, case-colliding names, hardlinks, special files, mounts, invalid/escaping symlinks, xattrs, root swap, mutation during inventory, malformed/constructed models and depth/entry/byte bounds. Case-colliding exports fail closed even on case-sensitive hosts.
- [ ] **Step 2: Run the new unit tests RED** with the focused command; retain the missing-interface/behavior failure.
- [ ] **Step 3: Implement the interfaces** with descriptor-relative no-follow inventory, inode/mount checks and bounded reads. Domain-separate canonical schema-2 manifest hashes with `maestro.workspace-diff.v2\0`; never reinterpret schema-1 hashes. Include every removed descendant; directory removal is not an implicit recursive delete. Reject incompatible type replacements rather than inventing ambiguous ordering; rename-overwrite of compatible files/symlinks uses modify destination plus delete source, existing destination subtree replacement is rejected unless explicitly represented and independently validated.
  After `_terminate_descendants()` succeeds, the trusted namespace supervisor exports from the still-mounted merged view into private candidate storage, then unmounts before writing its completion record. Export failure or unmount failure yields no accepted diff. Host still waits for verified complete scope stop before reading/admitting it. Schema-2 completion frames carry version 2; legacy schema-1 frames remain strictly decoded.
- [ ] **Step 4: Run focused regressions GREEN and live deletion/rename candidate tests.** Assert only private view changes before host publication, exact exported removals after stop, no raw whiteout acceptance and no live-test skip. No source workspace deletion occurs in the launcher.
- [ ] **Step 5: Commit the named source/tests** as `feat: export bounded workspace removals from stopped final views`; independent task review before Task 2.

### Task 2: Complete preimages, reversible deletion and retained commit receipts

**Files:**
- Modify: `src/orchestrator/isolation/workspace_publish.py`, `tools/workspace_write.py`, `tools/gateway.py`, `isolation/__init__.py`.
- Test: `tests/unit/isolation/test_workspace_publish.py`, `tests/unit/tools/test_workspace_write_gateway.py`, new `tests/integration/test_workspace_removal_recovery.py`.

**Interfaces:**
- Consume schema-2 `WorkspaceDiff` and Task 1 validation/conflict APIs.
- Extend `publish_workspace_diff(..., *, existing_bounds_and_callbacks, retain_committed: bool=False) -> WorkspacePublishReceipt` without changing legacy default behavior.
- Produce `acknowledge_workspace_publication(workspace_root: str|Path, lease: WorkspaceWriteLease, journal_root: str|Path, receipt: WorkspacePublishReceipt) -> None`: verify exact durable committed journal/workspace/manifest/generation before cleanup, idempotent after successful cleanup; never roll back a committed transaction.
- Produce `acknowledge_workspace_recovery(workspace_root: str|Path, lease: WorkspaceWriteLease, journal_root: str|Path, recovery: WorkspacePublicationRecovery) -> None` for proven retained `not_started`/`rolled_back` records after durable abort acknowledgement; wrong outcome/binding fails. Committed recovery uses the publication acknowledgement API instead. Neither public primitive creates approval authority; only trusted host composition calls it.
- Preserve both recovery APIs; `recover_workspace_publications_with_outcomes(...)` reports committed outcomes needed for trusted receipt reconciliation and does not silently discard a retained committed/rollback outcome journal. New schema-2 journals persist `retain_committed: bool`; schema-1 journals have the old cleanup policy. Schema-2 terminal rollback/not-started evidence remains until the associated durable abort receipt is acknowledged, too.
- Add trusted `retain_publication: bool=False` to `WorkspaceWriteGateway` construction; normal direct callers acknowledge after their durable Gateway receipts, while Task 4's service sets it true and owns final acknowledgement after its operation-terminal event. Add optional trailing `publication_receipt: WorkspacePublishReceipt|None=None` to `ToolExecutionResult` in `tools/gateway.py`; only a completed actual publication populates it.

- [ ] **Step 1: Write failing tests** for removing files, symlinks, nested nonempty directories; rename with existing compatible destination; full rollback including modes and symlink targets; preimage corruption/missing backup/overflow before first write; new live child appearing before directory removal. Assert unrelated/untracked files survive and conflicts retain evidence.
  Add retained commit tests: commit survives response loss; acknowledgement follows durable Gateway receipts; crash before acknowledgement reopens to committed contents; no duplicate mutation. Legacy schema-1 prepared/committed fixtures recover with original semantics. Unknown versions/duplicate JSON keys/nonfinite or boolean integers/overlapping records fail closed.
- [ ] **Step 2: Run RED** against the focused unit and new integration tests, record behavior failures.
- [ ] **Step 3: Implement schema-2 journals** with strict versioned parsing, complete original file bytes, exact live modes, symlink targets and directory topology backed up/fsynced before `prepared`. Preserve schema-1 reader and bounds. Build deterministic application order: added parent directories first, payload additions/modifications next, deleted non-directories deepest-first, deleted directories deepest-first. Reverse the actual applied sequence for rollback, restoring deleted parents before children. Recheck expected live state/lease/authority before every mutation; use descriptor-relative unlink/rmdir of enumerated paths, never path-string recursive deletion. Fsync touched parent directories before commit.
  Gateway uses `retain_committed=True`; append Run completion receipt, workspace resolution and consumed-approval EffectReceipt before acknowledgement. If audit fails after commit, keep the committed journal and unresolved stream. For retained mode, write a schema-2 `preparing` record binding workspace/transaction/manifest before backups; no mutation precedes durable `prepared`. Recovery persists `rolled_back` or `not_started` terminal evidence before returning it, and retains that evidence until durable abort receipt acknowledgement. Unbound/corrupt staging is not evidence and remains blocked. Legacy non-retained cleanup is unchanged. In service-owned retention mode, Gateway must not acknowledge before Task 5 appends its terminal operation event.
- [ ] **Step 4: Run GREEN** focused publisher/Gateway tests plus real subprocess-death matrix at backup, prepared, each apply, parent fsync, commit and acknowledgement. Assert exact old state before commit, exact new state after commit, idempotent repeat recovery, and safe conflict on external interference. Expose narrow no-op crash seams only where existing seams cannot observe a required boundary; no production fault-injection flags.
- [ ] **Step 5: Commit** as `feat: journal reversible workspace removals and retained commits`; independent task review.

### Task 3: Verified isolation admission and proven-not-started failures

**Files:**
- Create: `src/orchestrator/isolation/admission.py`.
- Modify: `isolation/launcher.py`, `isolation/overlay_launcher.py`, `isolation/__init__.py`, `tools/workspace_write.py`.
- Test: new `tests/unit/isolation/test_admission.py`, existing launcher/Gateway tests; new `tests/integration/test_workspace_isolation_admission.py`.

**Interfaces:**
- Produce immutable strict `WorkspaceIsolationAdmission` with `outcome: Literal["eligible","blocked"]`, `reason_code: Literal["isolation_unavailable"]|None`, `profile_hash: str|None`, `workspace_identity_hash: str`, `observed_at: datetime`; eligible requires verified hash, blocked forbids it.
- `verify_workspace_isolation(workspace: str|Path, *, limits: SandboxLimits) -> WorkspaceIsolationAdmission` uses fixed trusted probes/host observations, never Worker-provided profile data. The profile hash covers schema/backend/native architecture, verified required controls, effective limits and runtime identity; exclude timestamps/random unit names from its deterministic material.
- Add `IsolationNotStarted(IsolationUnavailable)` for failures proven before untrusted launch and `IsolationStartUnknown(IsolationUnavailable)` for uncertain start. Existing callers catching base `IsolationUnavailable` remain safe. Generic exceptions are never assumed not-started.

- [ ] **Step 1: Write failing tests** for Windows/macOS/unmeasured profile, missing binary/manager, incompatible architecture, unsafe runtime/private roots and mismatched workspace/control fingerprint. Assert a blocked `isolation_unavailable` result and zero Worker/tool/Provider/Broker invocation. Separate counters for trusted preflight probes versus untrusted command launches.
  Add a launch that creates a process then raises: it must remain unknown, with no cleanup or fabricated not-applied receipt. Revalidate eligibility immediately before the actual launch; policy may demand a stronger profile than the host proves.
- [ ] **Step 2: Run RED** on new unit tests.
- [ ] **Step 3: Implement admission** using existing trusted namespace/cgroup/rlimit verification paths; an OS label/systemd executable is not positive evidence. A bounded fixed harmless probe may verify kernel/filesystem controls in private staging; it cannot execute request content or publish workspace changes. Required probe stop must be verified. Check source workspace identity and runtime/control ownership; a stale probe is no exemption from per-launch in-child checks.
  Classify existing failures at their actual start boundary. Gateway maps only `IsolationNotStarted` to an audited safe block, without launch/publication intent when caught by preflight. After capability/approval consumption, record a definite not-applied receipt only when non-start is proven; retain consumed grants, never reuse them. `IsolationStartUnknown` retains all existing unknown semantics.
- [ ] **Step 4: Run GREEN** unit regressions and real positive controls under documented WSL2/systemd; simulate one required control missing and prove the actual command marker never appears. Inject drift between probe/launch and ensure no publication. A skipped positive run is a failed acceptance prerequisite, not a pass.
- [ ] **Step 5: Commit** as `feat: fail closed on unverified workspace execution profiles`; independent task review.

### Task 4: Actual isolated tool Worker and ready-gated host composition

**Files:**
- Create: `runtime/workspace_worker.py`, `runtime/workspace_execution.py` under `src/orchestrator/`.
- Modify: `application.py`, `runtime/__init__.py`, `tools/authority.py` only where exact durable binding requires it.
- Test: new `tests/unit/runtime/test_workspace_worker.py`, `test_workspace_execution.py`, `tests/integration/test_workspace_worker_execution.py`; extend application startup tests.

**Interfaces:**
- `WorkspaceToolProgram`: strict schema 1, `tool_id="workspace.write-candidate"`, command array <=128 arguments satisfying existing ToolRequest validation, additionally canonical JSON command encoding <=8,192 bytes. Canonical program JSON is the explicit tool-task text supplied at input admission; ordinary natural-language WorkerTask is not reinterpreted as a command.
- `WorkspaceWorkerTask`: strict schema-1 envelope with `task: WorkerTask`; its fully revalidated task has exactly one `ToolCapabilityRef` bound to this context/program hash. No host filesystem paths or grant body. Input total <= existing 1,048,576-byte Worker frame limit and task-text limit; proposal <=16,384 bytes. Both final serialized sizes are checked, not estimated from raw characters.
- `WorkspaceWorkerProposal`: strict schema 1, exact `AttemptContext`, request identity, tool reference and command array; no workspace/profile/principal/approval credential fields.
- `IsolatedWorkspaceWorker.propose(workspace: str|Path, task: WorkspaceWorkerTask) -> WorkspaceWorkerProposal` launches a fixed stdlib-only child through `SystemdReadOnlyLauncher` and decodes bounded stdout only after clean input/exit and complete verified stop. Preserve existing blocked-only `IsolatedWorkerProcess` behavior.
- `WorkspaceExecutionResult`: strict typed outcome `published|blocked|awaiting_approval|failed|outcome_unknown`, stable reason, operation ID, optional approval request ID and publication receipt metadata. Include `publication_atomicity="per_path_crash_recoverable"`; no Node success, secret, task text or internal staging path.
- `WorkspaceExecutionService.execute(*, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, task_text: str, approval_grant_id: str|None=None) -> WorkspaceExecutionResult` owns composition. `ControlPlaneApplication.execute_workspace_task` exposes the same keyword signature after `_require_ready()`.
- `WorkspaceExecutionHost` is immutable trusted configuration with `workspace: Path`, `lease_root: Path`, `journal_root: Path`, `limits: SandboxLimits`, `policy_state: Callable[[ToolRequest],PolicyState]`, `clock: Callable[[],datetime]` (default aware UTC), optional `approval_factory: Callable[[SQLiteEventStore],ApprovalService]` and `approval_requester: Callable[[ToolRequest],str]`. A configured factory requires a requester and must return a service using the application's exact EventStore. Only the host can supply this configuration; it is never parsed from request JSON. Add optional `workspace_execution_host: WorkspaceExecutionHost|None=None` to application construction; absent config safely blocks execution. Derive frozen PolicyManifest from the Run's persisted graph policy and validate its hash, not from Worker configuration.

- [ ] **Step 1: Write RED contract/service tests** proving canonical program input must match durable admitted input; full context/role/graph/policy/tool/fence binding; Worker cannot substitute command or tool reference; malformed/duplicate/oversized/nonfinite/constructed inputs fail without leaking task text. Test closed/not-ready application, missing host configuration and forged scope/profile/principal. No child runs inside a SQLite transaction.
  Test exact replay before/after published receipt and concurrent duplicate requests: one Worker invocation, one Gateway invocation, one publication. The deterministic host operation ID is SHA-256 of domain `maestro.workspace-worker.v1\0` and canonical Run/Node/Attempt/generation/input commitment; caller renaming cannot create a second operation for the same accepted input.
- [ ] **Step 2: Run RED** focused new tests.
- [ ] **Step 3: Implement actual isolated proposal and host flow.** The fixed child interprets only the explicit tool program, emits a proposal over stdin/stdout and exits; it has no raw socket or host control handles. This is a real one-tool Worker, not an LLM simulator or injected host callback. Host reconstructs `WorkspaceWriteRequest` with actual workspace, durable causation event ID and role; never copies authorization fields from the proposal.
  Use WorkerInputService and Scheduler binding under one checked snapshot to derive input/context; issue one opaque reference and append a hash-only `WorkspaceWorkerOperationRequested` before untrusted launch. Request/terminal events belong to a dedicated operation stream linked to the Run/Attempt; strict replay rejects conflicting bindings. No callback or child execution under a write lock. Recheck current durable authority outside/at each Gateway gate and after Worker return. Validate operation ownership through SQLite CAS, not memory locks.
  Construct Gateway only from verified profile (Task 3), frozen PolicyManifest, DurableAttemptAuthority, shared EventStore and trusted host config. The proposal process must stop before Gateway launches the separate tool process. Publication needs both exact stop proofs; requests proposing control-plane/Broker reads remain denied by OS and host. Persist terminal operation metadata after Gateway receipt; resource/Attempt success is not inferred from a tool receipt.
- [ ] **Step 4: Run GREEN** contract/service/application tests and real accepted-Attempt -> isolated Worker -> real Gateway -> real Overlay -> workspace publication, including create/update/delete/rename. Assert durable receipt and byte/mode/target correctness, lower unchanged, distinct supervised units stopped, Worker has no host paths/keys, and zero public Provider requests.
- [ ] **Step 5: Commit** as `feat: mediate isolated workspace tool workers through host authority`; independent task review.

### Task 5: Explicit recovery, approval, cancellation and full P0 live matrix

**Files:**
- Modify: `runtime/workspace_execution.py`, `application.py`, `tools/workspace_write.py` and existing recovery integration only as needed for strict operation auditing.
- Create: `tests/integration/test_p0_workspace_acceptance.py`, `tests/support/workspace_execution.py`.
- Extend: existing Broker isolation, approval, crash and WorkspaceWriteGateway tests.

**Interfaces:**
- `WorkspaceExecutionService.reconcile_publication(*, run_id: str, operation_id: str) -> WorkspaceExecutionResult`, exposed by ready-gated `ControlPlaneApplication.reconcile_workspace_publication` with identical keywords. Explicit trusted host action, no automatic command dispatch on construction/recovery/query.
- Consume Task 2 retained journal outcomes, Task 3 verified profile and Task 4 operation binding. Replay checks Run receipt, workspace stream, operation and exact journal manifest/transaction/workspace/lease identity consistently. Unknown Worker/tool stop cannot be reconciled from a publication receipt alone.

- [ ] **Step 1: Write failing real-process tests** for Worker proposal death, post-proposal/pre-tool death, tool start/death, backup/prepared/apply/fsync/commit, commit-before-Run-receipt, Run-receipt-before-workspace-resolution, resolution-before-operation-terminal and acknowledgement cleanup. Kill the actual child/controller at a concrete seam; reopen in a fresh process. Assert rollback or exact committed tree and at-most-once execution, not just event counts.
  Approval test must use real ApprovalService, durable accepted Attempts and both real systemd processes together. Request on generation N returns awaiting approval without a workspace effect; explicit trusted approval plus a newly accepted generation N+1 executes exactly once. Old Attempt, revoked/expired/duplicate grant, wrong principal/command and concurrent consumption never execute.
  Cancel/pause/revoke/lease-loss races must close Gateway authority before stopping process trees. Assert unknown stop retains budget/concurrency occupancy and prevents publication/cleanup; stale results cannot finalize another Attempt. Do not mutate the production lifecycle to invent `blocked` as a Run/Node enum: block is the typed host operation outcome.
- [ ] **Step 2: Run RED** named missing recovery/approval tests; record concrete seams.
- [ ] **Step 3: Implement explicit reconciliation** under the exact workspace lease. A verified committed journal reconstructs only the missing bound publication receipts, never reruns Worker/command. A verified prepared rollback records abort only after restoration. Missing evidence or inconsistent receipt/hash/provenance remains unknown/quarantined; no cleanup. Append Run receipt before workspace resolution, then operation terminal, then acknowledge retained journal. Startup validates operation histories but does not replay effects or perform filesystem cleanup automatically.
  Losing the host after launch means operation unknown unless durable exact evidence proves both process and publication state. Do not release scheduler slots based solely on the one-tool result; complete P3 Attempt reconciliation remains a separate proof-gated path.
- [ ] **Step 4: Run GREEN mandatory live matrix** including raw network, Unix Broker socket, protected/home/control paths, hardlink/mount/symlink escapes, injection of fake approval text, detached/forked children, SIGTERM resistance, disk/output/input/resource limits, audit failure and competing writers. Sentinel exists only in the Broker test process; scan Worker/Shell argv/environment, output, events, errors, approval previews, logs/artifacts; reuse actual existing Broker/sender tests without claiming memory erasure or paid Provider evidence. Mandatory cases must execute with zero skips.
- [ ] **Step 5: Commit** as `test: verify P0 worker write safety and crash recovery end to end`; independent task review.

### Task 6: Full verification, truthful completion records and ordinary push

**Files:**
- Modify: `README.md`, `PROJECT_CONTEXT.md` if present, `docs/security/platform-support.md`, `workspace-write.md`, `secrets.md`, `approvals.md`, `control-plane-startup.md`, original V1 plan P0 only.
- Create: `docs/validation/2026-10-18-p0-security-closure.md` with actual final validation date/environment (filename is plan identity, not fabricated run time).
- Test: existing full suite plus new P0 tests; package contents/clean-environment smoke.

**Interfaces:** no new product API. Both original P0 checkboxes map to exact current evidence; all incomplete P3/P4/P5/P6/P7 statuses remain truthful.

- [ ] **Step 1: Establish acceptance evidence before editing checkboxes.** Full supported-host command: `PYTHONPATH=src:. "$PY" -m coverage run -m pytest -o addopts= -q`; then `"$PY" -m coverage report --fail-under=90`. Capture actual pass/fail/skip totals and coverage; inspect any missed new security branch. Fail if required P0 live cases skipped. Existing unrelated skips must be enumerated rather than hidden.
- [ ] **Step 2: Run packaging/health checks:** `"$PY" -m compileall -q src tests`, `"$PY" -m pip check`, `"$PY" -m build --wheel --no-isolation`, `git diff --check`; inspect wheel contents for intended modules and no private artifacts. Use a private temporary validation environment only if already available tooling permits; no shared installs. CLI/MCP remain unfinished, so do not claim their installation/startup gates passed.
- [ ] **Step 3: Reproduce from a clean temporary checkout of the exact candidate commit**, using existing dependencies and real systemd backend. Run the documented P0 smoke with accepted input/approval/restart, not an injected fake service. Record command, commit, Python/kernel/systemd/WSL/native arch, observed profile hashes, known unsupported paths/platforms, resource limits, counts and artifact digests. Never include credential values or private request text. No real Provider measurements; label local numbers as correctness/smoke data, not paid-model improvement.
- [ ] **Step 4: Update docs and checklist only from that evidence.** Explain per-path atomicity vs reader-visible batches, reversible deletion/rename, same-user cooperating-writer limitation, unknown journal retention/manual reconciliation, Worker tool-only scope and failclosed missing capability. Record Broker unavailable production source, private endpoint/peer+nonce/lifetime and trusted Gateway/sender-only credential exposure. Record MCP non-implemented authenticated caller/Authority Envelope boundary; no JSON identity field grants authority. Product platform matrix is per-profile and explicitly not general V1 support. Include a requirement-to-command/evidence table for both P0 entries; unmet row keeps the checkbox open.
- [ ] **Step 5: Commit completion docs and obtain final whole-P0-range independent review**, including deferred findings and actual test evidence. Fix Critical/Important issues through reviewed implementer loops; rerun covering tests on each amended code state. Final delivered source must be the verified/reviewed state; doc-only evidence commits do not change tested code identity.
- [ ] **Step 6: Ordinary push** `git push origin codex/p3-systemd-termination-receipts`; verify local HEAD equals `git ls-remote origin refs/heads/codex/p3-systemd-termination-receipts`. No force push, merge, PR or deployment. Report P0 capability, exact tests/coverage/zero-required-skips/crash/security/review evidence, pushed SHA and remaining V1 boundaries. If any gate fails, report incomplete and do not claim the requested P0 delivery.

## Self-review and handoff

- Scope mapping: item 1 -> Tasks 1/2/4/5/6; item 2 -> Tasks 3/4/5/6.
- Approved atomicity, reversible preimages, Worker/no-secrets boundary, platform
  failclosed behavior, current identity/Broker limits and all evidence gates each
  have a task. Publication is not Node success and not the P5 Artifact publisher.
- Interfaces flow 1 -> 2 -> 3 -> 4 -> 5 -> 6; source writers are serialized.
  Shared files: workspace models 1/2; Gateway 2/3/4/5; application 4/5; docs 6.
- Current human approval covers the design and existing subagent-driven method.
  This newly written execution plan still requires the writing-plans handoff
  review before dispatching source implementation. No source task is pre-completed.
