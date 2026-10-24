# P3 General Worker Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete accepted text work after real Worker SIGKILL/restart without replaying unknown Provider effects.

**Architecture:** New explicit host-owned executor composes bounded isolated Worker turns, a same-store operation journal, existing Provider Gateway and independent artifact acceptance. Startup never dispatches. Unknown send/launch/stop/capture gaps stay held, not retried.

**Tech Stack:** Existing Python 3.12, Pydantic, SQLite and Linux systemd isolation; no installs.

**Spec:** docs/superpowers/specs/2026-10-24-p3-general-worker-recovery-design.md

## Global Constraints

- Existing Python 3.12/Pydantic/SQLite/systemd architecture; no dependency installation.
- One 1 MiB frame; output capped at min(task.output_byte_limit, 16,384 bytes).
- Terminal model text only; tool calls, arbitrary shell execution and workspace publication fail closed here.
- Worker has no network, secrets, SQLite, Scheduler or host filesystem paths.
- EventStore stores identifiers, phase, hashes, counts, exact context and host stop metadata only.
- Exact input is explicitly re-supplied against the existing durable commitment.
- Startup/inventory never launches Worker or contacts Provider.
- Same concrete SQLiteEventStore owns Scheduler, general operation journal and Provider journal.
- Missing trusted usage, stop proof, response checkpoint or dispatch authority blocks; no caller-zero-cost success.
- Unknown Provider effects never authorize invoke, credentials, rotated IDs or changed-attempt retry.
- No callback, filesystem, systemd, broker or transport under SQLite checked callbacks; trusted UTC after lock acquisition.
- Preserve existing blocked-only Worker, non-general Attempt paths and unavailable production usage defaults.
- Work only in the existing codex/p3-systemd-termination-receipts worktree; no PR, deployment, main integration, cleanup, force-push, real credentials or paid Provider calls.
- TDD, independent task spec/quality review and final whole-slice review; retain all evidence.
- Each task has at least three substantive commits/ordinary pushes, no empty commits; controller owns pushes and usage checks.
- LA daily reviewed completion maximum5; Oct24 starts1/5. Stop dispatch/development at observable used>=50%, or unavailable usage.

## Review Focus

1. A delayed credential broker outlives pause/expiry/fencing: authorization before the irreversible send must reject it (Task2).
2. This operation's captured known_success is unsettled: allow completion only, retaining every unrelated hold and preventing another invoke (Tasks2–3).
3. A host dies between prepared launch and verified ack, or stop proof is missing: retain holds and prohibit successor/success (Tasks1–3).
4. Scheduler terminal commits but API reply is lost: historical replay returns identical proof/artifact without active lease, fresh usage/time or dispatch (Tasks2–4).
5. Hostile constructed/subclass IPC and corrupt or cross-operation capture: reject with detached errors before callbacks, secrets or process launch (Tasks1–3).

## File and interface map

- `runtime/general_worker_protocol.py`: pure stdlib framed request/complete turn codecs.
- `runtime/general_worker_process.py`: fixed child entry point and host process wrapper.
- `isolation/launcher.py`: prepared host unit identity, verified launch acknowledgement, exact orphan stop.
- `runtime/general_worker_journal.py`: strict metadata-only operation projection/transitions, terminal linkage.
- `runtime/general_worker_authority.py`: phase eligibility and owned Scheduler Provider dispatch authority.
- `runtime/general_worker_execution.py`: host orchestration, capture, candidate and verification composition.
- `runtime/general_worker_recovery.py`: explicit resume decisions and per-operation advisory ownership.
- `application.py`: optional trusted composition, explicit ready-gated APIs and startup validation.
- `models/transport.py`: optional concrete post-credential intent authority; existing callers preserved.
- `scheduler/core.py`: operation-specific stop/capture gates, atomic accepted terminal linkage and validation.
- Existing artifact, input, Provider journal and verifier APIs are reused. Avoid copying their validators/acceptance logic.

Signatures below define cross-task contracts. Additional private helpers are
allowed; report a needed public contract change before building a dependent
task. All dataclasses are frozen; sensitive fields use repr=False. Revalidate
exact builtin/Pydantic types before any subclass/property/callback traversal.

## Verification and commit cadence

Use existing interpreter `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`
with `PYTHONPATH=src:.` from the isolated worktree. Focused RED/GREEN loops
first; retain exact commands/output. Run one full suite before each source
task's final C commit, not after every edit. Documentation Task0 uses its
existing focused compatibility baseline; do not rerun the old completed plan.
Controller pushes A, checks usage, authorizes B; repeats for C. Never keep a
worker running development across a failed quota gate. Reviewer consumes the
packaged task diff/report without rerunning suites absent a concrete doubt.

### Task 0: Reviewed design, complete plan and preflight

**Files:** this plan; design spec; `docs/superpowers/specs/2026-10-24-p3-general-worker-recovery-preflight.md`.
**Interfaces:** consumes existing acceptance/Provider/isolation contracts; produces the concrete contracts below and a recorded architecture gate.

- [ ] Read independent design-review.md, resolve its three important questions in the spec and obtain scoped reviewer verification.
- [ ] Commit/push A: reviewed design and its precise authority/replay decisions.
- [ ] Complete this plan, scan every shared-file/interface pair and each task for contradictions, record the table and rulings in this plan's ledger.
- [ ] Commit/push B: complete implementation plan after self-review against the spec.
- [ ] Record focused baseline345/0, source tree identity, environment and retained diagnostic limitations in the preflight document; no product edits.
- [ ] Commit/push C: durable preflight/acceptance map. Package the Task0 diff and run one task-scoped spec/quality gate; close only after findings resolved.

### Task 1: Bounded useful isolated Worker turns and prepared launch lifecycle

**Files:** create `src/orchestrator/runtime/general_worker_protocol.py`,
`src/orchestrator/runtime/general_worker_process.py`,
`tests/unit/runtime/test_general_worker_protocol.py`,
`tests/unit/runtime/test_general_worker_process.py`,
`tests/integration/test_general_worker_isolation.py`;
modify `src/orchestrator/isolation/launcher.py`;
extend existing launcher tests as required.

**Interfaces:**
- `GeneralWorkerTurn(phase: Literal["request","complete"], launch_id: str, task: WorkerTask, response_text: str|None=None)` in protocol module; exact task/context and terminal text bounds.
- `encode_general_worker_turn(turn: GeneralWorkerTurn) -> bytes` and `decode_general_worker_turn(payload: bytes) -> GeneralWorkerTurn`.
- `GeneralWorkerRequest(context: AttemptContext, launch_id: str, input_commitment: str)` is a non-authoritative request proposal, not a ModelRequest.
- `decode_general_worker_proposal(payload: bytes, turn: GeneralWorkerTurn) -> GeneralWorkerRequest|DecodedWorkerOutput` validates the echoed launch identity and uses existing candidate decoder.
- `PreparedSandboxLaunch(launch_id: str, unit_name: str)`; `SystemdReadOnlyLauncher.prepare_launch() -> PreparedSandboxLaunch` generates unique owned UUID identities without launch.
- Extend launcher `launch(..., prepared_launch: PreparedSandboxLaunch|None=None)`; preserve omitted default behavior. `SandboxSession.launch_receipt` is available only after exact systemd identity/profile observation. Its data contains prepared identity plus verified control_group, never a claimed stop.
- `SystemdReadOnlyLauncher.stop_prepared_launch(prepared_launch, launch_receipt) -> SandboxTerminationReceipt` requires exact verified owned binding; unacknowledged launch raises detached ambiguous-start error.
- `GeneralWorkerProcess.run_turn(workspace: Path, turn: GeneralWorkerTurn, *, prepared_launch: PreparedSandboxLaunch, cancellation=None) -> GeneralWorkerTurnResult` synchronously supervises a turn, exposing host launch receipt before wait via a separate `start_turn(...) -> GeneralWorkerSession`; the session's `wait()` returns result plus termination receipt. The service can persist ack before wait without an injected callback under a DB lock.

**Feature A — pure protocol (first substantive commit/push):**
- [ ] Write failing tests `test_request_roundtrip_binds_epoch_and_context`, `test_complete_emits_real_utf8_candidate`, `test_protocol_rejects_duplicate_unknown_nonfinite_stale_and_oversize`, `test_constructed_subclass_rejected_without_callback`. Assert1MiB frame and16,384 output cap, exact context, SHA256 of actual content, no sensitive error text.
- [ ] Run focused protocol tests and retain expected missing-interface RED.
- [ ] Implement exact pure stdlib codecs and fixed request/complete child behavior; no Pydantic import in isolated child startup.
- [ ] Run same tests GREEN; self-review; commit A, then wait for controller push/quota gate.

**Feature B — supervised lifecycle (second substantive commit/push):**
- [ ] Add failing tests `test_prepared_identity_precedes_launch`, `test_ack_requires_observed_identity`, `test_epoch_cannot_be_reused_or_substituted`, `test_missing_stop_never_yields_candidate`, `test_old_worker_remains_blocked`.
- [ ] Implement prepared launcher extension, start/wait session and exact whole-unit stop using owned existing primitives. No receipt constructed from returncode or Provider sender stop. Detached no-start/start-unknown/stop-unknown states remain distinct.
- [ ] Run focused new and existing launcher/Worker suites GREEN; commit B, wait for gate.

**Feature C — required real isolation (third substantive commit/push):**
- [ ] Add required-isolation tests `test_real_request_and_complete_turns_are_readonly_nonetwork`, `test_real_sigkill_worker_proves_empty_cgroup`, `test_prepared_unacked_launch_never_retries`. A real MainPID is killed with SIGKILL; no mocked receipt substitutes for it. No runtime test-hook API.
- [ ] Execute real isolation with bounded waits/cleanup; required cases must have zero skips. Run full suite once, retain JUnit/log, self-review compatibility diff; commit C.
- [ ] Controller packages/reviews this task, resolves findings via implementer, then marks complete.

### Task 2: Durable mediation, dispatch and atomic acceptance authority

**Files:** create `src/orchestrator/runtime/general_worker_journal.py`,
`src/orchestrator/runtime/general_worker_authority.py`,
`tests/unit/runtime/test_general_worker_journal.py`,
`tests/unit/runtime/test_general_worker_authority.py`;
modify `models/transport.py`, `scheduler/core.py` and their focused tests.

**Interfaces:**
- `GeneralWorkerIdentity(run_id: str,node_id: str,attempt_id: str,fencing_generation: int)`.
- `GeneralWorkerOperation`: immutable projection of identity, operation_id, input/config/registry/route/workspace commitments, phase, epochs/acks/stops, stable call identity, capture metadata and optional accepted result. Contains no task/response text.
- `GeneralWorkerJournal(event_store: SQLiteEventStore)`; `read(identity) -> GeneralWorkerOperation|None`, `validate_operations() -> None`; concrete checked methods for request, prepare/ack/stop turn, reserve call, capture response and proposal. Strict versioned events, bounded history, exact context/causation/idempotency replay; no generic caller event append or success setter.
- `GeneralWorkerResult(identity, operation_id, status: Literal["accepted","blocked","in_progress"], code: str, artifacts: tuple[ArtifactRef,...], task_sha256: str|None, evidence_sha256: str|None)`; ArtifactRef is the existing runtime contract; accepted fields reflect historical proof.
- `SchedulerProviderDispatchAuthority(scheduler: Scheduler, journal: GeneralWorkerJournal)` with async `is_accepted(request: ModelRequest)->bool` and synchronous `record_intent(request: ModelRequest, *, provider_id, provider_adapter, request_body, provider_correlation_id)->str`. The concrete authority owns existing Provider journal and stable operation request matching.
- Add optional `dispatch_authority` to ProviderModelGateway; new service requires exact owned class/same store. After credential validation and encoding it calls this authority instead of plain journal.record_intent. Legacy omitted path stays unchanged. Authority validation and nested record_intent share one append_checked transaction and post-lock trusted UTC.
- Phase eligibility helper accepts exact operation and purpose request/dispatch/completion/acceptance; only completion/acceptance exempt own captured known_success with exact usage/receipt/capture bindings. No invoke exemption.
- Scheduler internal concrete journal acceptance transaction records accepted linkage; public success/failure entry points consult operation state when present. validate_success_proofs includes reciprocal general checkpoint validation.

**Feature A — durable projection (commit/push A):**
- [ ] RED tests `test_metadata_replay_rejects_missing_cross_epoch_transition`, `test_operation_binds_all_frozen_commitments`, `test_db_wal_repr_and_errors_exclude_payload`, `test_constructed_operation_rejected_before_callback`.
- [ ] Implement journal/projection and stable operation/request IDs derived from trusted identity/commitments; strict exact-shape bounded event schemas and causation. No new network/process/storage effects in validation.
- [ ] GREEN focused tests; commit A, wait for gate.

**Feature B — send authority (commit/push B):**
- [ ] RED tests `test_delayed_broker_pause_before_intent_sends_nothing`, parameterize expiry/fencing/new same-node hold; `test_intent_committed_before_withdrawal_retains_inflight_hold`; `test_two_dispatch_authorities_claim_once`; `test_plain_verifier_or_mismatched_store_cannot_authorize_general_dispatch`.
- [ ] Add concrete authority/Gateway integration; no awaits under locks. Recheck cancellation before authority where possible; durable authority is the serialized send boundary, not a promise that subsequent pause erases delivery.
- [ ] Test exact own captured success allowed only for completion, unrelated unknown/knownsuccess/effect holds block, request rotation cannot bypass. Existing Gateway behavior preserved.
- [ ] GREEN focused transport/authority tests; commit B, wait for gate.

**Feature C — terminal transaction (commit/push C):**
- [ ] RED tests `test_direct_acceptance_cannot_bypass_general_stop`, `test_failure_cannot_release_unproved_worker`, `test_accepted_terminal_and_usage_commit_roll_back_together`, `test_lost_reply_replays_historical_result_without_active_lease`, `test_bootstrap_rejects_unpaired_success`; include existing non-general success regression.
- [ ] Extend Scheduler success path with concrete journal eligibility, accepted metadata in same SQLite transaction; private ArtifactPublished stays earlier. Rollback includes general accepted, Scheduler terminal, usage, agent and slot writes. Historical reads validate exact original completed_at/usagehash/settlement/proposal/artifact references, never resample/reaccept.
- [ ] GREEN focused Scheduler/acceptance/journal tests, one full suite, commit C; independent task review.

### Task 3: Explicit useful host execution and restart recovery

**Files:** create `src/orchestrator/runtime/general_worker_execution.py`,
`src/orchestrator/runtime/general_worker_recovery.py`,
`tests/unit/runtime/test_general_worker_execution.py`,
`tests/unit/runtime/test_general_worker_recovery.py`,
`tests/integration/test_general_worker_application.py`;
modify `src/orchestrator/application.py` and public runtime exports as needed.

**Interfaces:**
- `GeneralWorkerExecutionService(scheduler, input_service, artifact_store, grant_authority, proposal_journal, acceptance_coordinator, provider_gateway, worker_process, workspace: Path)` requires exact owned dependencies and store/registry binding; unavailable usage remains default.
- async `execute(*, identity: GeneralWorkerIdentity, task_text: str) -> GeneralWorkerResult` and `recover(*, identity: GeneralWorkerIdentity, task_text: str) -> GeneralWorkerResult`.
- `recover` is explicit after bootstrap. `execute` also recognizes an existing operation and uses the same recovery decision table rather than a second implementation.
- `GeneralWorkerOwnership` private nonblocking OS advisory lease keyed by canonical DB authority and operation ID; released on crash/exit. It is never stop proof. Busy contender returns in_progress with no external work.
- `GeneralWorkerHostConfig(workspace: Path, registry, secret_broker=None, transport=None)` is trusted constructor-only configuration; None uses existing unavailable broker/default isolated transport. `ControlPlaneApplication.__init__` gains optional `general_worker_config: GeneralWorkerHostConfig|None=None`, then composes its own service from its existing concrete Scheduler/store/artifact/usage authority. It never accepts an already-bound foreign service. Public async `execute_general_worker_task(...)` / `recover_general_worker_task(...)` accept identity and exact task_text only. Missing configuration returns detached unavailable error. Startup validates journal even when execution unconfigured and never launches.

**Feature A — normal host mediation (commit/push A):**
- [ ] RED `test_real_candidate_bytes_flow_through_independent_acceptance`, `test_host_derives_frozen_request_not_worker_model_or_route`, `test_gateway_capture_requires_known_success_usage_and_sender_stop`, `test_tool_nonterminal_empty_or_oversized_response_blocks`, `test_unavailable_usage_cannot_succeed`.
- [ ] Compose prepare exact WorkerTask, durable prepared intent/ack/request stop, one stable host ModelRequest and call_reserved, owned Gateway invoke and private canonical capture. Publish response with operation-specific readable scope/source/type; access on resume uses a signed digest-bound grant and verifies matching metadata, not just digest. Candidate output uses existing decoder, attested publication, frozen VerificationTask, real verifier/proposal/coordinator.
- [ ] Canonical response bytes include needed validated output/usage/request identity; no raw provider HTTP or credentials. DB/WAL only metadata. Run blocking process supervision outside event-loop critical sections; no thread shares SQLite connection or callback under transaction.
- [ ] GREEN focused service tests; commit A, wait for gate.

**Feature B — restart decision table (commit/push B):**
- [ ] RED `test_captured_success_resumes_without_gateway_or_broker`, `test_dispatching_unknown_and_reserved_gap_block_every_external_boundary`, `test_missing_capture_never_regenerates`, `test_verified_orphan_stopped_before_successor`, `test_unacked_or_stop_unknown_retains_lease`, `test_cross_operation_or_corrupt_capture_blocks`.
- [ ] Implement single-owner explicit resume with trusted current phase checks. Before ackless launch return launch_unknown; verified orphan gets exact stop/empty proof before new epoch. Captured response resume never invokes; proposed resume accepts only under current authority; accepted replay validates historical result before active-lease lookup.
- [ ] Test pause/cancel/expiry/new generation between phases blocks launch/acceptance and retains unresolved effects. No automated reconciliation/retry/replan.
- [ ] GREEN focused recovery/service tests; commit B, wait for gate.

**Feature C — public ready gates/privacy (commit/push C):**
- [ ] RED `test_startup_validates_without_dispatch`, `test_explicit_application_reopen_requires_exact_input`, `test_public_methods_fail_before_ready_or_unconfigured`, `test_same_node_unknown_other_attempt_blocks`, `test_recovered_owner_contention_has_one_winner`.
- [ ] Wire application explicit methods/startup validation; validate all same-store dependencies and immutable commitments on reopen. No caller clock, usage, ModelRequest, success flag or arbitrary continuation cursor.
- [ ] GREEN focused application/recovery tests, one full suite, commit C; independent task review. Daily cap may require Task4 on next LA date.

### Task 4: Independent-process SIGKILL matrix and final acceptance

**Files:** create `tests/integration/test_general_worker_restart_process.py`,
`tests/integration/test_general_worker_recovery_matrix.py`,
`tests/support/general_worker_restart_harness.py`,
`docs/superpowers/specs/2026-10-24-p3-general-worker-recovery-acceptance.md`;
amend touched modules only for demonstrated fixes via TDD.

**Interfaces:** consumes real application APIs, exact operation history, fixed child, actual systemd receipts and private artifacts. Harness is test-only and records actual PIDs/units/invocations, never runtime test hooks or fake stop receipts. Offline deterministic transport and usage are explicitly labeled; no paid calls.

**Feature A — primary actual crash (commit/push A):**
- [ ] Add failing real process test `test_sigkill_completion_then_new_process_completes_with_one_call`: capture validated response, wait for ack of real completion Worker blocked on stdin, SIGKILL its actual MainPID, terminate old host, open entirely new Python process/app on same stores, re-supply exact input, obtain actual artifact and independently verified AttemptSucceeded, Provider count exactly1. Test-only host instrumentation may pause immediately after durable ack; production source has no crash-hook API.
- [ ] Also kill host after verified request launch; new owner must stop old exact unit/prove empty then continue. Exercise lost acceptance reply with identical historical task/evidence/artifacts and unchanged counts.
- [ ] Run required live tests zero skips and exact bounded cleanup; commit A, wait for gate.

**Feature B — no-resend and competition matrix (commit/push B):**
- [ ] Add `test_unknown_provider_survives_new_process_without_any_dispatch`, cover dispatching/unknown/reserved/no-capture cuts and changed IDs/attempt/generation attacks; assert broker, transport and Worker counts stayzero on blocked restart.
- [ ] Two independent recovered hosts, lease expiry/pause/cancel/fence races, capture corruption and terminal rollback; require exactly one accepted result/call/settlement and no released unknown occupancy. Preserve failing logs before fixes.
- [ ] Run focused real matrix and privacy checks GREEN, commit B, wait for gate.

**Feature C — full evidence and independent whole slice (commit/push C):**
- [ ] Run one fresh full suite with JUnit and statement coverage>=90%; required live isolation0skips. Run compileall, pip check, private wheel build/inventory without dependency install, and source hashes. Persist exact counts, versions, commands, durations and unresolved limitations; no claiming offline evidence as online delivery/charging proof.
- [ ] Commit/push acceptance document and tested code. Independent task review, then one broad whole-branch review of base36423cf..HEAD, one fix wave/scoped re-review if needed; all residual load-bearing findings surfaced with rulings.
- [ ] Mark this slice complete only after gates. Final report once: useful capability, real crash/no-resend proof, test coverage, commit/push state, exhaustive ledger rulings and cost-if-wrong, remaining full-P3 gates. Keep evidence/worktree/branch. No main integration or P5 execution.

## Self-review and authorization

Every design component maps to Tasks1–3; real restart/cross-process evidence
belongs to Task4. The five Review Focus risks each have named tests. Shared
interface scan is recorded in progress.md before Task1 dispatch. This plan
does not claim every kill cut resumes: ambiguous launch/send/capture still
blocks intentionally. Human explicitly authorized reviewer then direct
development and skipped questions; preserve subagent-driven execution without
a new plan-approval pause.
