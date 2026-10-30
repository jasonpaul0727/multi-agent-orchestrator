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
- In new general-Worker authority paths, no callback, filesystem, systemd, broker or transport under SQLite checked callbacks; trusted UTC after lock acquisition. Existing legacy/bootstrap artifact recovery retains its prior file checks and is not retroactively labelled IO-free.
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
- `recovery/run.py`: explicit metadata-only control projection sharing control-state consistency validation with full recovery; full recovery still verifies artifact files.
- `runtime/acceptance_coordinator.py`: chooses registered general operation's metadata binding itself, with independent byte verification outside terminal transaction.
- `runtime/attempt_usage.py`: cycle-free shared trusted usage protocol/unavailable default/acceptance error; preserve original imports as aliases.
- `runtime/proposal_verification.py`: shared out-of-lock fresh isolated proof verification, preserving legacy injected verifier and owned fixed general verifier paths without duplicated blocks.
- `runtime/worker_input_service.py`: owned general preparation with metadata binding and locked stdlib UTC; shared exact-input validator, existing legacy path preserved.
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

- [x] Read independent design-review.md, resolve its three important questions in the spec and obtain scoped reviewer verification.
- [x] Commit/push A: reviewed design and its precise authority/replay decisions (a5bfd34).
- [x] Complete this plan, scan every shared-file/interface pair and each task for contradictions, record the table and rulings in this plan's ledger.
- [x] Commit/push B: complete implementation plan after self-review against the spec (c319f3f).
- [x] Record focused baseline345/0, source tree identity, environment and retained diagnostic limitations in the preflight document; no product edits.
- [x] Commit/push C: durable preflight/acceptance map (f6d6f72). Task0 spec/quality gate Approved, zero Critical/Important/Minor; ledger records closure and LAcount2/5.

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
- `GeneralWorkerRequest(context: AttemptContext, launch_id: str, input_commitment: str)` is a non-authoritative request proposal, not a ModelRequest. Its input_commitment is SHA256 of `b"maestro.general-worker-input.v1\0" + canonical_full_WorkerTask_JSON`, distinct from existing admission `task.context.input_manifest_hash`; bind both independently.
- `decode_general_worker_proposal(payload: bytes, turn: GeneralWorkerTurn) -> GeneralWorkerRequest|DecodedWorkerOutput` validates the echoed launch identity and uses existing candidate decoder.
- `PreparedSandboxLaunch(launch_id: str, unit_name: str)`; `SystemdReadOnlyLauncher.prepare_launch() -> PreparedSandboxLaunch` generates unique owned UUID identities without launch.
- Extend launcher `launch(..., prepared_launch: PreparedSandboxLaunch|None=None)`; preserve omitted default behavior. `SandboxSession.launch_receipt` is available only after exact systemd identity/profile observation. `SandboxLaunchReceipt(launch_id, unit_name, control_group, invocation_id, profile_sha256)` contains observed canonical32hex systemd InvocationID and a commitment to all effective security/resource properties emitted by existing _service_properties, including normalized RO binds/inaccessible path sets and assumed sanitized environment/exec binding; unrelated systemd defaults need not match. No raw filesystem profile paths in the durable journal and never a claimed stop.
- `SystemdReadOnlyLauncher.stop_prepared_launch(prepared_launch, launch_receipt) -> SandboxTerminationReceipt` accepts exact reconstructed receipts from the validated same-store journal after host restart; new launches still require in-process prepare/consumption ownership. Loaded/running unit Id/cgroup/InvocationID must agree before stop. A collected unit needs the previously verified ack plus authoritative stopped/empty exact retained cgroup proof; unacknowledged launch raises detached ambiguous-start error. Application never accepts caller receipts; process-local sets cannot be the recovery authority.
- `GeneralWorkerProcess.run_turn(workspace: Path, turn: GeneralWorkerTurn, *, prepared_launch: PreparedSandboxLaunch, cancellation=None) -> GeneralWorkerTurnResult` synchronously supervises a turn, exposing host launch receipt before wait via a separate `start_turn(...) -> GeneralWorkerSession`; the session's `wait()` returns result plus termination receipt. `GeneralWorkerTurnResult` has proposal, launch_receipt, termination_receipt, returncode, cancelled, timed_out, output_limited and input_written; confirmed stopped invalid/cancelled transport returns proposal=None, absent/mismatched stop raises detached IsolationStopUnknown. The service can persist ack before wait without an injected callback under a DB lock.

**Feature A — pure protocol (first substantive commit/push):**
- [x] Write failing tests `test_request_roundtrip_binds_epoch_and_context`, `test_complete_emits_real_utf8_candidate`, `test_protocol_rejects_duplicate_unknown_nonfinite_stale_and_oversize`, `test_constructed_subclass_rejected_without_callback`. Assert1MiB frame and16,384 output cap, exact context, SHA256 of actual content, no sensitive error text.
- [x] Run focused protocol tests and retain expected missing-interface RED.
- [x] Implement exact pure stdlib codecs and fixed request/complete child behavior; no Pydantic import in isolated child startup.
- [x] Run same tests GREEN; self-review; commit A, then wait for controller push/quota gate.

**Feature B — supervised lifecycle (second substantive commit/push):**
- [x] Add failing tests `test_prepared_identity_precedes_launch`, `test_ack_requires_observed_identity`, `test_epoch_cannot_be_reused_or_substituted`, `test_missing_stop_never_yields_candidate`, `test_old_worker_remains_blocked`.
- [x] Implement prepared launcher extension, start/wait session and exact whole-unit stop using owned existing primitives. No receipt constructed from returncode or Provider sender stop. Detached no-start/start-unknown/stop-unknown states remain distinct.
- [x] Run focused new and existing launcher/Worker suites GREEN; commit B, wait for gate.

**Feature C — required real isolation (third substantive commit/push):**
- [x] Add required-isolation tests `test_real_request_and_complete_turns_are_readonly_nonetwork`, `test_real_sigkill_worker_proves_empty_cgroup`, `test_prepared_unacked_launch_never_retries`. A real MainPID is killed with SIGKILL; no mocked receipt substitutes for it. No runtime test-hook API.
- [x] Execute real isolation with bounded waits/cleanup; required cases must have zero skips. Run full suite once, retain JUnit/log, self-review compatibility diff; commit C.
- [x] Controller packages/reviews this task, resolves findings via implementer, then marks complete.

### Task 2: Durable mediation, dispatch and atomic acceptance authority

**Files:** create `src/orchestrator/runtime/general_worker_journal.py`,
`src/orchestrator/runtime/general_worker_authority.py`,
`tests/unit/runtime/test_general_worker_journal.py`,
`tests/unit/runtime/test_general_worker_authority.py`;
modify `src/orchestrator/models/transport.py`, `src/orchestrator/scheduler/core.py`,
`src/orchestrator/recovery/run.py`, `src/orchestrator/runtime/acceptance_coordinator.py`
and `src/orchestrator/application.py`; create `src/orchestrator/runtime/attempt_usage.py`
and `src/orchestrator/runtime/proposal_verification.py`; extend their focused tests.

**Interfaces:**
- `GeneralWorkerIdentity(run_id: str,node_id: str,attempt_id: str,fencing_generation: int)`.
- `GeneralWorkerOperation`: immutable projection of identity, operation_id, input/config/registry/route/workspace/host-scope commitments, phase, epochs/acks/stops, stable call identity, capture metadata and optional accepted result. Contains no task/response text. Host scope is SHA256 of domain-separated canonical trusted Linux boot UUID, effective UID and concrete SQLite canonical-path commitment/device/inode, read before DB locks by owned authority; same-store same-boot processes agree, different scope (including same-host DB copy) blocks external work/terminal acceptance. Require regular file-backed DB, reject hardlinked DB aliases/in-memory execution so canonical lease authority is unique. Store only hash, never raw host/path identity. This slice supports same-host-boot restart, not OS-boot/host/store migration.
- `GeneralWorkerJournal(event_store: SQLiteEventStore)`; `read(identity) -> GeneralWorkerOperation|None`, `validate_operations() -> None`; concrete checked methods for request, prepare/ack/stop turn, reserve call, capture response and proposal. Strict versioned events, bounded history, exact context/causation/idempotency replay; no generic caller event append or success setter.
- `GeneralWorkerResult(identity, operation_id, status: Literal["accepted","blocked","in_progress"], code: str, artifacts: tuple[ArtifactRef,...], task_sha256: str|None, evidence_sha256: str|None)`; ArtifactRef is the existing runtime contract; accepted fields reflect historical proof.
- `SchedulerProviderDispatchAuthority(scheduler: Scheduler, journal: GeneralWorkerJournal)` with async `is_accepted(request: ModelRequest)->bool` and synchronous `record_intent(request: ModelRequest, *, provider_id, provider_adapter, request_body, provider_correlation_id)->str`. The concrete authority owns existing Provider journal and stable operation request matching.
- Its owned `reserve_call(operation, *, request: ModelRequest, request_body: bytes) -> GeneralWorkerOperation` atomically requires a NEW eligible worker_stopped -> call_reserved transition, using the journal's same signature (exact body bytes length1..1048576, separate model-request/body hashes). After commit, mint a private process-local one-use dispatch permit bound to the exact reservation event/request/body. Replayed reservation or newly constructed authority has no permit and fails before broker; no caller flag or durable cursor can reconstruct it. `is_accepted` claims that permit for one Gateway invocation before awaiting credentials; `record_intent` consumes/rechecks it at the durable post-credential boundary. Permit loss leaves the conservative reserved/no-intent gap held; it is never a stop proof.
- Add optional `dispatch_authority` to ProviderModelGateway; new service requires exact owned class/same store. After credential validation and encoding it calls this authority instead of plain journal.record_intent. Gateway detects registered general identity from its concrete journal before route-verifier/broker callbacks, and that operation always requires the owned same-store authority; omitted/foreign authority cannot fall through legacy behavior. Non-general omitted path stays unchanged. Authority validation and nested record_intent share one append_checked transaction and post-lock trusted UTC.
- Phase eligibility helper accepts exact operation and purpose request/dispatch/completion/acceptance; request admits initial requested, or worker_stopped with no call/capture and every prior request turn exactly acknowledged/stopped, so an interrupted pure request can run a fresh epoch. Prepared/unacknowledged/unstopped turns and any call reservation never admit request retry. Only completion/acceptance exempt own captured known_success with exact usage/receipt/capture bindings. No invoke exemption.
- Scheduler internal concrete journal acceptance transaction records accepted linkage; public success/failure entry points consult operation state when present. validate_success_proofs includes reciprocal general checkpoint validation.
- General known releases also preserve Provider cost/unknown holds: stopped reserved/no-intent and dispatching/unknown calls cannot be released by caller zero usage or no-effect flags. Reconcile consumes exact durable Provider reconciliation usage/effect/time with separate all-Worker-stop proof and no startup callbacks. Known-cost finish/cancel samples Scheduler trusted usage outside SQL and rechecks exact Provider token/receipt/hold bindings under lock. Pure no-call/no-intent work may release as no-effect after all prepared epochs stop; exact historical release replay does not resample usage. Ordinary non-general contracts remain unchanged.
- Scheduler constructor gains optional `usage_source: AttemptUsageSource|None=None`, default Unavailable. Application resolves one trusted host source for Scheduler and Coordinator. Shared protocol/default/error module uses TYPE_CHECKING snapshot types and old module reexports preserve identity. General direct success derives trusted UsageRecord outside transactions and rejects caller-record/captured-token mismatches. Coordinator delegates general success to an owned no-usage-input Scheduler finish entry sharing the same preflight/commit, sampling service once; no public skip flag/prevalidated-proof token. Non-general semantics unchanged; general locked stdUTC validates actual lease/fence regardless of caller timestamp.
- Scheduler also gains optional trusted `artifact_grants: EphemeralArtifactGrantAuthority|None=None`, with Application wiring its existing same-store ArtifactStore grant authority. General preflight invokes owned concrete IsolatedVerifierProcess once OUTSIDE SQL, issues scoped digest grants, revalidates fresh evidence and requires its hash to equal stored proposal. Coordinator delegates so no duplicate fresh launch. Missing grants or forged/mismatched durable accepted evidence blocks; preserve old aliases/injected nongeneral verifier behavior via shared helper.
- `RecoveredControlState(lifecycle, agents, budget, active_attempts, effects)` is a frozen projection with no artifact-byte claim; `RunRecoveryCoordinator.recover_control_state(run_id: str) -> RecoveredControlState` shares the existing control-consistency algorithm, while ordinary recover still checks actual artifacts/pending files. Never use a fake ArtifactStore, copied validation algorithm or public skip flag.
- Scheduler owned `_resolve_general_active_attempt(...) -> ActiveAttemptSnapshot` and `_resolve_general_verification_binding(...) -> VerificationBinding` take the existing identity/as_of parameters and use metadata-only control replay. Share frozen binding checks with existing resolvers; ordinary resolvers preserve full recovery. New general dispatch/continuation/terminal uses owned path; Coordinator selects it from registered same-store operation, never caller flag. Direct general terminal entry verifies required private files before acquiring its checked transaction and rejects an ambient caller transaction before any file read; final locked decision rechecks exact byte commitments/metadata/proposal/stop identities.

**Feature A — durable projection (commit/push A):**
- [x] RED tests `test_metadata_replay_rejects_missing_cross_epoch_transition`, `test_operation_binds_all_frozen_commitments`, `test_db_wal_repr_and_errors_exclude_payload`, `test_constructed_operation_rejected_before_callback`.
- [x] Implement journal/projection and stable operation/request IDs derived from trusted identity/commitments; strict exact-shape bounded event schemas and causation. Extract shared control-state replay plus typed metadata projection/owned Scheduler resolvers. RED/GREEN `test_general_control_projection_never_reads_artifact_files_under_lock`, `test_full_recovery_still_rejects_corrupt_artifacts`, and `test_general_binding_matches_full_frozen_context`; no fake ArtifactStore or validation-copy fork.
- [x] GREEN focused tests; commit A, wait for gate.

**Feature B — send authority (commit/push B):**
- [x] RED tests `test_delayed_broker_pause_before_intent_sends_nothing`, parameterize expiry/fencing/new same-node hold; `test_intent_committed_before_withdrawal_retains_inflight_hold`; `test_two_dispatch_authorities_claim_once`; `test_plain_verifier_or_mismatched_store_cannot_authorize_general_dispatch`; `test_reopened_reserved_call_cannot_remint_dispatch_permit` (zero broker/transport) and `test_one_live_permit_cannot_start_two_gateway_invocations`.
- [x] Add concrete authority/Gateway integration; no awaits under locks. Recheck cancellation before authority where possible; durable authority is the serialized send boundary, not a promise that subsequent pause erases delivery.
- [x] Test exact own captured success allowed only for completion, unrelated unknown/knownsuccess/effect holds block, request rotation cannot bypass. Add `test_registered_general_gateway_cannot_use_legacy_authority_path` with zero callback/broker/transport, and `test_host_scope_mismatch_blocks_before_external_work`; non-general Gateway behavior preserved.
- [x] GREEN focused transport/authority tests; commit B, wait for gate.

**Feature C — terminal transaction (commit/push C):**
- [x] RED tests `test_direct_acceptance_cannot_bypass_general_stop`, `test_failure_cannot_release_unproved_worker` (finish_attempt/reconcile_attempt/startup Provider settlement; sender-stop alone insufficient), `test_accepted_terminal_and_usage_commit_roll_back_together`, `test_lost_reply_replays_historical_result_without_active_lease`, `test_bootstrap_rejects_unpaired_success`; include existing non-general success regression.
- [x] Pin the dependent recovery contract with `test_interrupted_request_with_exact_stop_can_claim_fresh_pure_request_epoch` and negative reserved/unacknowledged/unstopped cases. Same-node Provider/effect holds, running/latest/active/unexpired context and scope checks still apply; no prior call can regain dispatch permission.
- [x] Extend Scheduler/Coordinator success paths with concrete journal eligibility, independent bytes outside transaction and metadata binding inside, accepted metadata in same SQLite transaction; private ArtifactPublished stays earlier. RED/GREEN `test_direct_general_terminal_verifies_files_before_lock`, `test_ambient_transaction_cannot_trigger_general_file_preflight`, and a tripwire on filesystem/process/broker callbacks during every new general checked claim. Rollback includes general accepted, Scheduler terminal, usage, agent and slot writes. Historical reads validate exact original completed_at/usagehash/settlement/proposal/artifact references, never resample/reaccept.
- [x] RED/GREEN `test_general_direct_success_rejects_caller_usage_mismatch`, `test_general_success_requires_scheduler_owned_usage_source`, `test_captured_tokens_must_match_trusted_usage`, `test_general_coordinator_samples_trusted_usage_once_outside_transaction`, application same-source wiring and non-general compatibility; defaults remain unavailable.
- [x] RED/GREEN forged durable accepted evidence for bytes failing a frozen check, fresh evidence-hash mismatch, missing trusted grants, one fresh verifier/one usage service call and ambient zero external callbacks. Real end-to-end verification later must use actual isolated verifier, not fixture evidence.
- [x] GREEN focused Scheduler/acceptance/journal tests, one full suite, commit C; independent task review.

Task2 closed at `28c3c30` after independent specification/quality review and
scoped fix-round1 review: I1 addressed, zero remaining findings. A/B/C and
review fix ordinarily pushed. Full3181/90.70% belongs to pre-review-fix
`f200580`; current fix has26 focused and587 compatibility passes. Task4 still
requires its fresh final full suite and real independent-process matrix.

### Task 3: Explicit useful host execution and restart recovery

**Files:** create `src/orchestrator/runtime/general_worker_execution.py`,
`src/orchestrator/runtime/general_worker_recovery.py`,
`tests/unit/runtime/test_general_worker_execution.py`,
`tests/unit/runtime/test_general_worker_recovery.py`,
`tests/integration/test_general_worker_application.py`;
modify `src/orchestrator/application.py`, `src/orchestrator/runtime/worker_input_service.py`
and `src/orchestrator/runtime/general_worker_process.py`; extend their focused tests
and public runtime exports as needed. The async ownership seam also permits
narrow shared changes to verifier_process.py, proposal_verification.py,
Scheduler general-acceptance internals and Coordinator, plus focused tests;
no duplicated acceptance algorithm or public prevalidated-proof parameter.
Task3B also permits a narrow launcher.py recovered-stop observation fix and its
tests: after exact ACK and a fresh stopped binding, bounded reread of the same
standard termination/cgroup proof plus final stopped-binding recheck. Global
collector/legacy strictness stays; no synthesized receipt or new-work retry.

**Interfaces:**
- `GeneralWorkerExecutionService(scheduler, input_service, artifact_store, grant_authority, proposal_journal, acceptance_coordinator, provider_gateway, worker_process, workspace: Path)` requires exact owned dependencies and store/registry binding; unavailable usage remains default.
- async `execute(*, identity: GeneralWorkerIdentity, task_text: str) -> GeneralWorkerResult` and `recover(*, identity: GeneralWorkerIdentity, task_text: str) -> GeneralWorkerResult`.
- `recover` is explicit after bootstrap. `execute` also recognizes an existing operation and uses the same recovery decision table rather than a second implementation.
- `GeneralWorkerOwnership` private nonblocking OS advisory lease keyed by canonical DB authority and operation ID; released on crash/exit. It is never stop proof. Busy contender returns in_progress with no external work.
- Constructor validates canonical SQLite/ArtifactStore/owner-lease roots outside both readable workspace and runtime-source tree and outside bootstrap ambient read grants (`/usr`, canonical `/bin`/`/lib`/`/lib64`, `/etc`, `/dev`), before process/Provider work and outside SQLite locks. Reject overlapping/ambient-readable layouts with detached unavailable error; keep legacy constructors unchanged. Do not infer secrecy from a directory name or allow a symlink to bypass the canonical-root check; inspect `_exec._runtime_grants` and prove the selected out-of-tree layout unreadable with a real child.
- `WorkerInputService._prepare_general_worker_task(*, run_id, node_id, attempt_id, fencing_generation, task_text) -> WorkerTask` uses the owned general metadata-only Scheduler binding and locked stdlib UTC, sharing existing exact-input validation. Both execute/recover use it; existing `_prepare_recovery_worker_task` still transitively calls full file recovery and is insufficient for this new no-IO claim.
- Add thin owned-process facade methods `GeneralWorkerProcess.prepare_launch() -> PreparedSandboxLaunch` and `stop_prepared_launch(prepared_launch, launch_receipt) -> SandboxTerminationReceipt`; delegate to its same launcher without exposing private backend state. Extend start_turn/run_turn with optional `expected_workspace_identity_hash: str|None=None`, forwarding to actual launcher snapshot verification. Legacy omitted behavior preserved; the new service always supplies its immutable operation workspace hash, closing the precheck/snapshot mutation gap.
- `GeneralWorkerHostConfig(workspace: Path, registry, secret_broker=None, transport=None)` is trusted constructor-only configuration; None uses existing unavailable broker/default isolated transport. `ControlPlaneApplication.__init__` gains optional `general_worker_config: GeneralWorkerHostConfig|None=None`, then composes its own service from its existing concrete Scheduler/store/artifact/usage authority. It never accepts an already-bound foreign service. Public async `execute_general_worker_task(...)` / `recover_general_worker_task(...)` accept identity and exact task_text only. Missing configuration returns detached unavailable error. Startup validates journal even when execution unconfigured and never launches.

**Feature A — normal host mediation (commit/push A):**
- [x] RED `test_real_candidate_bytes_flow_through_independent_acceptance`, `test_host_derives_frozen_request_not_worker_model_or_route`, `test_gateway_capture_requires_known_success_usage_and_sender_stop`, `test_tool_nonterminal_empty_or_oversized_response_blocks`, `test_unavailable_usage_cannot_succeed`.
- [x] RED/GREEN owned facade identity/recovered-stop delegation and `test_workspace_changed_after_precheck_cannot_launch_general_turn`: replace/move the bound workspace directory after precheck, require actual path/device/inode snapshot rejection before Popen/credentials/transport. Same-directory byte edits are not a content-freeze claim; preserve existing bounded no-follow snapshots. Forwarding-only mocks do not replace this behavioral check.
- [x] Compose prepare exact WorkerTask through owned general input method (RED `test_general_input_preparation_uses_control_binding_without_files_or_caller_clock_under_lock`), durable prepared intent/ack/request stop, one stable host ModelRequest and call_reserved, owned Gateway invoke and private canonical capture. Publish response with operation-specific readable scope/source/type; access on resume uses a signed digest-bound grant and verifies matching metadata, not just digest. Candidate output uses existing decoder, attested publication, frozen VerificationTask, real verifier/proposal/coordinator.
- [x] Canonical response bytes include needed validated output/usage/request identity; no raw provider HTTP or credentials. DB/WAL only metadata. Run blocking process supervision outside event-loop critical sections; no thread shares SQLite connection or callback under transaction.
- [x] Split verifier metadata/grant/byte preparation on the SQLite owner thread from DB-free immutable staged-byte/process execution off the event loop. Share sync/async owned general acceptance preparation, fresh evidence validation and terminal commit; no caller proof/skip capability. RED/GREEN owner-thread metadata tripwire and event-loop heartbeat during verifier wait, preserving sync/legacy proof, rollback and final-hold rechecks.
- [x] GREEN focused service tests; commit A, wait for gate.

**Feature B — restart decision table (commit/push B):**
- [x] RED `test_captured_success_resumes_without_gateway_or_broker`, `test_dispatching_unknown_and_reserved_gap_block_every_external_boundary`, `test_missing_capture_never_regenerates`, `test_verified_orphan_stopped_before_successor`, `test_unacked_or_stop_unknown_retains_lease`, `test_cross_operation_or_corrupt_capture_blocks`.
- [x] Implement single-owner explicit resume with trusted current phase checks. Before ackless launch return launch_unknown; verified orphan gets exact stop/empty proof before new epoch. Captured response resume never invokes; proposed resume accepts only under current authority; accepted replay validates historical result before active-lease lookup.
- [x] Exercise a real acknowledged orphan through a NEW launcher with no old backend session. Pin the loaded/blank-cgroup collection transition with focused RED/GREEN; permit only bounded proof resampling after observed exact stopped binding, retaining mismatch/unacknowledged/unresolved/populated failures.
- [x] Test pause/cancel/expiry/new generation between phases blocks launch/acceptance and retains unresolved effects; changed host-scope/boot blocks before credentials/Worker/terminal, while new same-boot application process agrees. No automated reconciliation/retry/replan or host migration.
- [x] GREEN focused recovery/service tests; commit B, wait for gate.

**Feature C — public ready gates/privacy (commit/push C):**
- [x] RED `test_startup_validates_without_dispatch`, `test_explicit_application_reopen_requires_exact_input`, `test_public_methods_fail_before_ready_or_unconfigured`, `test_same_node_unknown_other_attempt_blocks`, `test_recovered_owner_contention_has_one_winner`, `test_private_storage_layout_rejected_if_visible_to_worker` (workspace/runtime overlap and symlink alias, zero launch/broker/transport), plus real child denial of out-of-tree SQLite/artifact roots.
- [x] Wire application explicit methods/startup validation; validate all same-store dependencies and immutable commitments on reopen. No caller clock, usage, ModelRequest, success flag or arbitrary continuation cursor.
- [x] GREEN focused application/recovery tests, one full suite, commit C; independent task review. At Task3 closure, Task4 remained subject to next-LA-day daily-cap accounting.

Task3 closed at `43e51ff` after independent specification/quality review and
scoped fix-round1 review: cancellation I1 addressed, zero remaining findings.
A/B/C/fix ordinarily pushed. Full3295/90.76% belongs to pre-review-fix
`0c4fb64`; current fix has6 focused cases included in520 compatibility passes.
At Task3 closure, Task4's remaining gates were the final current-source full
run and independent-host matrix; both are now satisfied as documented below.

### Task 4: Independent-process SIGKILL matrix and final acceptance

**Files:** create `tests/integration/test_general_worker_restart_process.py`,
`tests/integration/test_general_worker_recovery_matrix.py`,
`tests/support/general_worker_restart_harness.py`,
`docs/superpowers/specs/2026-10-24-p3-general-worker-recovery-acceptance.md`;
amend touched modules only for demonstrated fixes via TDD.

**Interfaces:** consumes real application APIs, exact operation history, fixed child, actual systemd receipts and private artifacts. Harness is test-only and records actual PIDs/units/invocations, never runtime test hooks or fake stop receipts. Offline deterministic transport and usage are explicitly labeled; no paid calls.

**Feature A — primary actual crash (commit/push A):**
- [x] Add failing real process test `test_sigkill_completion_then_new_process_completes_with_one_call`: capture validated response, wait for ack of real completion Worker blocked on stdin, SIGKILL its actual MainPID, terminate old host, open entirely new Python process/app on same stores, re-supply exact input, obtain actual artifact and independently verified AttemptCompleted(outcome=succeeded), Provider count exactly1. Test-only host instrumentation may pause immediately after durable ack; production source has no crash-hook API.
- [x] Also kill host after verified request launch; new owner must stop old exact unit/prove empty then continue. Exercise lost acceptance reply with identical historical task/evidence/artifacts and unchanged counts.
- [x] Run required live tests zero skips and exact bounded cleanup; commit A, wait for gate.

**Feature B — no-resend and competition matrix (commit/push B):**
- [x] Add `test_unknown_provider_survives_new_process_without_any_dispatch`, cover dispatching/unknown/reserved/no-capture cuts and changed IDs/attempt/generation attacks; assert broker, transport and Worker counts stayzero on blocked restart.
- [x] Two independent recovered hosts, lease expiry/pause/cancel/fence races, capture corruption and terminal rollback; require exactly one accepted result/call/settlement and no released unknown occupancy. Preserve failing logs before fixes.
- [x] Run focused real matrix and privacy checks GREEN, commit B, wait for gate.

**Feature C — full evidence and independent whole slice (commit/push C):**
- [x] Run one fresh full suite with JUnit and statement coverage>=90%; required live isolation0skips. Run compileall, pip check, private wheel build/inventory using builder-only tooling isolated from shared/product environments, and source hashes. Persist exact counts, versions, commands, durations and unresolved limitations; no claiming offline evidence as online delivery/charging proof.
- [x] Commit/push acceptance document and tested code. Independent task review, then one broad whole-branch review of base36423cf..HEAD, one fix wave/scoped re-review if needed; all residual load-bearing findings surfaced with rulings.
- [x] Mark this slice complete only after gates. Final report once: useful capability, real crash/no-resend proof, test coverage, commit/push state, exhaustive ledger rulings and cost-if-wrong, remaining full-P3 gates. Keep evidence/worktree/branch. No main integration or P5 execution.

**Post-review fixes and final evidence:** The whole-slice review found one
Important cross-generation authority-fallback issue, resolved by
280036a42f83d3c0bb54643558c0248ea0a9da70; scoped re-review found no remaining
findings. The post-fix full run then exposed a legacy SQLite migration-order
failure, resolved by d41d229bcb222851cb8a5f08d6cc9917b6dbb7be; its scoped
re-review also found no findings. Final HEAD full suite: 3326 passed,
0 failures/errors/skips, 90.79% same-run statement coverage. Package tooling
was installed only in a new private mode-0700 builder venv; the final wheel,
106 cold imports, frozen hashes and refreshed 23-store privacy scan passed.
Full review/failure/fix history and the remaining offline/production boundaries
are recorded in the acceptance document and this plan's ignored progress ledger.
## Self-review and authorization

Every design component maps to Tasks1–3; real restart/cross-process evidence
belongs to Task4. The five Review Focus risks each have named tests. Shared
interface scan is recorded in progress.md before Task1 dispatch. This plan
does not claim every kill cut resumes: ambiguous launch/send/capture still
blocks intentionally. Human explicitly authorized reviewer then direct
development and skipped questions; preserve subagent-driven execution without
a new plan-approval pause.
