# P3 Native Custody Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct A2 failure/custody handling and validate ONE fixed real PRE_UNSHARE retention/release fixture, without performing ordinary native acceptance.

**Architecture:** Build strict finite contracts and constructive ownership first, then the canonical host/runner protocol, then the separate explicit real fixture. The strong host root precedes fallible acquisitions; exact live ownership and current-execution proof, not a decoded receipt, control the narrow new-slot release. Archived A2 is reference evidence only.

**Tech Stack:** Existing Python/pytest environment; fixed `/usr/bin/python3.12 -B -I -S` helpers; Linux x86_64; stdlib/ctypes with explicit libc path. No installations.

**Spec:** `docs/superpowers/specs/2026-11-15-p3-staging-native-failure-custody-addendum.md`, SHA256 `d3bb4ea73facfe4ac066619844245fe672c790e323769904d815555f7046e693`, final scoped review PASS. Original Stage0 boundary: `docs/superpowers/specs/2026-11-15-p3-staging-custody-cleanup-design.md`. Read both, but implement ONLY the correction and fixed fixture below.

## Global Constraints

- Baseline HEAD1e1ff3d is docs-only after verified sourcee030f29. Existing A1 remains accepted; no blind restoration of the seven parked A2 files and no conversion of the old required native failure to skip/xfail/pass.
- No base/overlay/Provider/GeneralWorker/session-cleanup edits, namespace custodian/Worker/data, credentials, network, installation, deployment, main merge, v2 activation or unknown resend. Ordinary native acceptance is OUTSIDE this plan even if its code is unit-tested.
- Keep the sole existing ledger `.superpowers/sdd/2026-11-07-p3-worker-host-mediation-stop-proof`; no new ledger/root to regain capacity. Original slot0 stays UNKNOWN forever under this release family.
- Profile and control semantics are exactly the reviewed addendum. Helpers always `-B -I -S`; do not rely on environment to disable pyc. Fixed explicit loader/stdlib/native paths; no PATH/find_library/ldconfig/shell/compiler/mount helpers or source-code payload.
- At most9 processes; shared16384B: ordinary14336, terminal2048=8*192+512 history. Normal native27-frame and closed-abort/U alternatives are exact tables, not optional implementation choices. Max8 frames/direction; no repeated RESULT or invented ACK/grant.
- One original30s deadline and one cleanup_end=min(now+5s, original_end+5s). Parked owner may remain alive; no refreshed budget, EOF cleanup inference, new native retry or forced capacity reset.
- Host pytest/app process never changes prctl/rlimits. Only dedicated R owns subreaper; actual helpers are forbidden in A/B validation. Real fixture is C-only and explicitly dispatched once.
- Preserve A1 opaque-helper body-primary cleanup semantics/tests. NEW A2 cleanup follows its distinct selected-control-flow rule; opaque construction/close uncertainty retains poisoned unreserved debt, not a fabricated exact FD ledger.
- Fixed original parent `/tmp/maestro-staging-a2-native-20261115`; strict existing-root reopen only. Audit needs the addendum's ext4/mount-ID+file/parent-fsync support. 9p/unknown support refuses release; no alternative arena.
- A/B/C each require TDD, scoped independent review, real commit/ordinary feature push and immediate quota refresh. Quota was37% at dispatch; apply existing stop/refresh rules. Whole review + one fresh final-head full suite precede bounded-task completion.

## Review Focus

1. Constructor/helper failure before a reservation token retains debt and prevents another admission; the root must exist before controlled imports/manifest/reopen (A).
2. A1 body-primary exception behavior cannot silently change while A2 preserves its own selected custom BaseException identity (A/B).
3. Correct namespace order never reads pid_for_children before C and never moves T's own PID namespace; incomplete native evidence cannot become UNSUPPORTED/OBSERVED (A/B pure tests only).
4. Credentials, sequence and abort substitutions retain exact known ownership through early return instead of exiting R or guessing a cause (B).
5. A real current PRE_UNSHARE fixture alone may free new slot1 after host reap/all attempt-FD closure/history/baseline proof; no old receipt/fake bool/9p persistence/native-success release (C).

---

## Frozen file map

| Increment | Source files | Responsibility |
| --- | --- | --- |
| A | New `_staging_a2_contract.py`, `_staging_ownership.py`, `_staging_native.py`, `_staging_reopen.py`, `_staging_audit.py`, `_staging_gate.py` in `src/orchestrator/isolation/` | v2 codecs/status; finite FD/debt records; checked native wrappers; existing-root acquisition; baseline/audit codec; metadata-only canonical host root, NO submission methods yet. |
| A/B/C | Modify `_staging_contract.py` ONLY its explicit manifest list/declared dependencies as actual new files appear | Keep v1 request/receipt semantics and A1 cleanup unchanged. Never manifest nonexistent/unimplemented entry files. |
| B | New `_staging_protocol.py`, `_staging_supervisor.py`, `_staging_entry.py`, `_staging_retention_entry.py` | Fixed framed channels/cursor; real owner/process graph; separate fixed native versus fixture entry programs. Add host submission/observation methods to the A root. |
| C | New `_staging_release.py`; narrow `SlotArena.release_current_fixture` addition to `_staging_arena.py`; new `tools/manual_checks/staging_preunshare_retention.py` | Private one-shot evidence/release and explicit one-shot real fixture driver. No schema migration. |

Tests are specified per task below; no test file is restored from the parked increment. Entry modules have no import-time execution. Helper loader reads only exact manifested files; no production package initialization, user sys.path/PATH or arbitrary entry selector. Module dependency order is contract -> ownership/native -> reopen/audit -> gate; protocol/supervisor are added in B; release is added/lazily imported only in C. TYPE_CHECKING/postponed annotations avoid audit/arena cycles.

## Shared interfaces (names are fixed across increments)

- `_staging_a2_contract.py`: `Role`, `NativePhase`, `FailureKind`, `OwnerState`, `TerminalFamily`, `Opcode` finite enums EXACTLY from addendum; frozen `FailureRecord(role, phase, kind, errno)` and `AttemptBinding(attempt, token, generation, manifest)`; `ExecutionEnvelope` has exact v2 fields/schema. `Frame.to_bytes()/from_bytes()` implements the90B header + <=70B typed payload, not a generic dictionary. `Slot0Baseline` and `RetentionHistory` strict binary records are owned by audit module.
- `_staging_ownership.py`: `AcquisitionLedger.adopt_fd(fd, *, role, identity)`, `.adopt_returned_arena(arena)`, `.close_once(fd)`, `.close_all()`, `.poison_debt(phase)`; `HostState` is the pure two-entry/busy/debt/lifecycle state used by root. No public release factory, callback/witness decoder or real child is needed in A. Pure HostState instances support isolated unit tests without resetting the canonical live singleton.
- `_staging_native.py`: `NativeCalls(owner, *, role)`; checked `set_subreaper()`, `guard_parent(parent_pidfd, expected_pid)`, `set_nondumpable()`, `unshare_and_map(uid,gid,groups) -> PreChildNamespaces`, `verify_registered_c(pre, c_registration, c_identity)`, `close_high_fds(outside_fd)`. Roles/owner identities are internal, never public request fields. Explicit libc `/usr/lib/x86_64-linux-gnu/libc.so.6`, declared ABIs, immediate errno capture; tests substitute private syscall backend, not public support flags.
- `_staging_reopen.py`: `open_fixture_roots_existing(parent: Path, *, owner: AcquisitionLedger) -> FixtureRoots`; owner-aware returned FD acquisition, existing names only. `SlotArena.open_existing` remains an opaque accepted-A1 call; raising/close uncertainty poisons host debt.
- `_staging_audit.py`: `capture_slot0_locked(arena, roots) -> Slot0Baseline`; `assert_slot0_unchanged_locked(arena, baseline, *, expected_transition) -> None`; `verify_ext4_metadata(parent_fd, *, owner: AcquisitionLedger) -> None`; `commit_baseline(parent_fd, baseline, *, owner, budget) -> None`; `commit_history(parent_fd, history, *, owner, budget) -> None`. expected_transition is an internal exact committed/pending object, not caller permission to ignore pending state.
- `_staging_gate.py`: canonical `NativeHostRoot.open_preserved() -> NativeHostRoot`, `.close() -> CloseOutcome{CLOSED,RETAINED,POISONED}`. A adds metadata/root/debt only. B adds `.probe_native_prerequisites(request: NativePrerequisiteRequest) -> ExecutionEnvelope` (NOT executed here), `.begin_retention_fixture() -> AttemptHandle`, `.observe_attempt(handle) -> ExecutionEnvelope`, `.converge_retention_fixture(handle) -> ExecutionEnvelope`. Root stores handles strongly; handles have no from_bytes or rights supplied by callers.
- `_staging_protocol.py`: `ProtocolCursor(binding, family, role).accept(frame, credentials, *, now_ns) -> Transition`; `BoundChannel.send(frame)/receive()`. Private family is fixed by entry file, not a field/options on public NativePrerequisiteRequest.
- `_staging_supervisor.py`: `OwnedGraph` owns exact pidfds/parent edges/reap and FD records; `RunnerOwner` owns graph for process lifetime; `run_native_prefix(...)`, `run_retention_prefix(...)` implement distinct fixed paths. Prefix functions propagate selected local control objects after bounded cleanup; entries convert only at process boundary.
- C adds root `.release_closed_retention_fixture(handle) -> RetentionOutcome`, `_staging_release.mint_current_fixture_evidence(root, owner) -> _ClosureEvidence` and `_staging_arena.SlotArena.release_current_fixture(reservation,evidence)`. Evidence is privately registered by exact identity, single-use and current-root/owner/fixture/token/index/generation bound; no constructor/from_bytes/caller-bool route.

All commands run from this isolated Linux worktree with the EXISTING interpreter
`/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`, `PYTHONPATH="$PWD/src:$PWD"`, `-B -m pytest -o addopts= -p no:cacheprovider`. Helper interpreter remains fixed system Python. Do not run any command in this planning task.

### Increment A — Finite failures, constructive acquisition and existing-root baseline

**Files:** A source map; new `tests/unit/isolation/test_staging_a2_contract.py`, `test_staging_ownership.py`, `test_staging_native_calls.py`, `test_staging_reopen_audit.py`, `test_staging_host_root.py`; retain original A1 tests unchanged.

**Consumes:** A1 strict v1 models, SlotArena and original preserved evidence. **Produces:** all A interfaces above, actual manifest entries for A files only, no Popen/fork/runner API.

- [x] Write requirement assertions first: exact v2 unknown/extra/bool-errno/schema rejection; ContractError after successful unshare is FAILED with errno null; checked -1 captures only that call's errno; cleanup ambiguity overrides while preserving cause. Missing-import/interface errors are setup evidence, not behavior RED; do not deliberately weaken code to manufacture RED.
- [x] Write ordering REDs using private inert syscall doubles: no pid_for_children access pre-C, T user/mnt changes but own pid unchanged, registered C/PID1 validation only afterward. Any accidental real unshare/prctl/setrlimit/fork in these unit tests fails immediately. Do not restore archived broad exception catches.
- [x] Implement strict enums/codecs and single-call checked native operations, including explicit native ABI and refusal errno table. Keep A1 NativePrerequisiteReceipt/from_bytes unchanged. New A2 exception cleanup does not claim suppressed A1 helper signals.
- [x] Implement the canonical module's built-in-only boot sentinel/_HOST_ROOT/lock/plain placeholder BEFORE lazy controlled imports and FD acquisitions. Then implement checks, HostState, fixed eight administration handles, no-token debt and close/tombstone rules. Add REDs for alias/reload/fork/interpreter/thread/reentry refusal and constructor-not-returning/close uncertainty. Test pure HostState/isolated unit contexts; never reset a live canonical root to run another case.
- [x] Implement strict owner-aware existing-root reopen and locked slot0 capture. A unit test uses ONLY its own temporary A1 fixture, never the original arena. Test raw512/header0 identity/digest/revision checks at every stage, bounded names, no mkdir/truncate/repair, changed inode/header/slot0 refusal, ext4 mount-ID agreement, 9p refusal and one-shot baseline file+directory fsync/readback. Opaque A1 failures retain debt. Preserve existing A1 body-primary tests.
- [x] Run A files plus `tests/unit/isolation/test_staging_contract.py tests/unit/isolation/test_staging_arena.py -q`. Assert no runner submission can occur and manifest names only existing A files. Capture genuine behavior RED/GREEN and truthful setup failures separately.
- [ ] Obtain independent scoped review, fix new findings via RED/GREEN, commit `feat: add strict native failure and existing-root ownership contracts`, ordinary push, immediate quota read. A is not native acceptance and starts no helper.

### Increment B — Canonical retained owners and exact supervised protocol

**Files:** B source map; modify A private modules as consumed; new `tests/unit/isolation/test_staging_protocol.py`, `test_staging_supervisor.py`, `test_staging_gate_ownership.py`. Add B files to manifest only once implemented; no C release import/API yet.

**Consumes:** A contracts/acquisition/debt/root/audit/native wrappers. **Produces:** B interfaces above with real implementation but validation limited to pure faults/own local sockets; no actual preserved-arena helper attempt.

- [ ] Write REDs for RESULT-not-EOF, original local custom BaseException identity, returned UNKNOWN leaving a strong host owner + actual runner ledger reference, root.close RETAINED, no token/no returned constructor losing debt, and forbidding normal R exit when ledger incomplete. Use finite fake process/syscall backends; Popen/fork/native control sentinels prevent actual submission.
- [ ] Implement exact OwnedGraph/RunnerOwner and canonical root state. Own each returned FD before another fallible operation, register exact roles/parent edges, use pidfd exit + trusted targeted waitid(P_PIDFD) evidence, never wait-any/scan/ECHILD inference. Dedicated R alone performs subreaper; caller process stays unchanged. Preserve selected local control identity; remote reports carry only finite interrupted/unknown.
- [ ] Implement the90B header, typed payloads, sequence/state table, per-message SO_PASSCRED/SCM_CREDENTIALS bounded recvmsg, exact peer/namespace view checks and no arbitrary ancillary FD forwarding. Test own socketpairs and pure malformed ancillary fixtures: reject rights/truncation/wrong credential/role/edge/seq/attempt, close delivered unexpected descriptors once, poison uncertainty. A local-socket unit success is not a native custody proof.
- [ ] Implement complete27-frame ordinary trace, normal N_C terminal, R_0/R_1/S_0/T_0 closed aborts, U retained alternatives and the separate14-step fixture prefix. Add table-driven REDs for unshare refusal before C preserving T EXIT_NOTE->actual-reap ACK; S/R early abort sequences; prebinding/partial registration using UNKNOWN/control_lost without invented cause/PREFIX/exit. Freeze exactly the reviewed grants/abort sequence counters.
- [ ] Implement shared 16KiB counters/2048B reserve and original absolute cleanup end; no heartbeat/extra diagnostic allowance. Implement PARKED owner and one timely fixture CONVERGE, rejecting late/peer-lost/native-family command, with passive-only later observation. Test worst-case arithmetic, uncertain-send no seq reset, original-deadline waits and retained owner after saturation.
- [ ] Implement both fixed loader/entry files without CLI selector: native entry calls native prefix; retention entry calls ONLY fixed PRE_UNSHARE path. Fixed interpreter `-B -I -S`, fixed environment FD roles, manifest-bound imports, no package init/PATH/discovery/pyc. Implement C's control dup2 to FD1/old wrapper detach and preassigned terminal credit in native code, verified with inert FD/syscall doubles only. No real close_range/prctl/namespace execution in B validation.
- [ ] Run B unit files and all A files with the documented existing pytest interpreter and `-q`. Include positive same-process socket credential checks and negative source/loader guards. Do not invoke `_staging_entry.py`, `_staging_retention_entry.py`, root.probe/begin against real preserved roots, old native test, or any one-shot script.
- [ ] Independent scoped review must inspect actual native entry bodies/retention guards as well as unit evidence. Fix findings; commit `fix: retain native runner custody through bounded failure`, ordinary push and immediate quota read. Unit PASS does not establish namespace support or real retention.

### Increment C — One explicit real PRE_UNSHARE fixture and private release

**Files:** C source map; modify gate/ownership/audit/arena/protocol only as required; new `tests/unit/isolation/test_staging_release.py`, `test_staging_retention_oracle.py`; new `tools/manual_checks/staging_preunshare_retention.py` (NOT `test_*.py`, outside tests/).

**Consumes:** B fixed fixture entry/retained root/graph and A existing-root/baseline. **Produces:** current-execution private evidence/release and finite RetentionOutcome audit; no ordinary native acceptance.

- [ ] Write REDs for fake caller bool/v1 receipt/token-decoded evidence, stale owner/token/generation, wrong family, original slot0, uncertain spawn/reap/FD/history, replay evidence, post-unshare/C-created record and root debt all refusing release. Positive unit evidence remains synthetic and is not the real acceptance receipt.
- [ ] Implement private evidence registry and one-shot claim under root lock; no release-capable object from any decoder. Require continuously owned current fixture, dual R/S/T identities/parent edges, no unshare/C/data/Worker/grant, exact trusted reaps + actual host R reap and all attempt-FD closure. Only the eight separately owned composition administration FDs remain for persistence/release.
- [ ] Implement exact512B typed history at the original owned parent and commit/readback/file+parent-fsync protocol. History is audit only. Add narrow arena release with existing v1 intent/record/header ordering, increasing revision, no schema/generation reset. Check raw slot0/header0 before/after reopen,reserve,UNKNOWN,intent,release readback; pending check validates exact own intent without weakening A1 committed-only reads. On any uncertainty poison, do not retry/repair/free. Keep old512B baseline honest: preservation since NEW observation, not invented historical bytes.
- [ ] Run reusable C/A/B unit suites only, obtain pre-execution scoped review of release predicates/history filesystem support and fixed fixture code. Unit/default-suite success cannot waive the next required real check.
- [ ] Keep manual driver separate from default pytest discovery. It has no arguments/path/code selector, uses only fixed original root, refuses prior run/unexpected artifact/capacity/binding, and performs ONE current R/S/T PRE_UNSHARE fixture. It MUST NOT call ordinary `.probe_native_prerequisites`, unshare, fork C, data or Worker. It first observes early UNKNOWN with R alive + root/control handles retained, then uses the one original-window CONVERGE; it independently observes/reaps exact R and verifies all current proof, immutable slot0 and committed history before explicit slot1 release. Emit only a fixed bounded charged summary; history remains the non-authoritative512B artifact. No new arena/reset, no old-receipt replay and no skip/xfail path.
- [ ] ONLY after coordinator explicitly dispatches this reviewed one-shot step, run once:
  `PYTHONPATH="$PWD/src:$PWD" /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -B tools/manual_checks/staging_preunshare_retention.py`.
  Exit0 requires real retained lifetime AND actual full current fixture closure/history/private release: original slot0 still same UNKNOWN bytes/header0; new slot1 FREE at higher revision after distinct UNKNOWN history. It is retention-fixture acceptance, NOT native OBSERVED/Stage0 PASS. Any unavailable/unknown condition exits nonzero, preserves owner/artifacts/slot, blocks task completion and is not retried here. If resources remain live, the strong host/root lifetime must continue rather than driver normal exit; coordinator keeps that running session and receives the bounded UNKNOWN snapshot.
- [ ] Preserve exact actual bounded receipt/history/baseline facts and failure distinction in the sole ledger; original failed receipt/archive/arena slot0 stay unchanged. The manual driver is never imported or silently executed by default pytest and no known failing default test is removed to obtain green.
- [ ] Scoped independent review of real evidence + unit regressions; fix findings without a native retry/root reset. Commit `fix: verify and release only the current pre-unshare retention fixture`, ordinary push and immediate quota read. Any second real fixture attempt requires a new explicit coordinator decision, not this plan.

## Final acceptance and self-review

- [ ] Whole independent review against final addendum: A1 semantics unchanged; every manifested file exists; no slot0/receipt-derived release; no unlisted protocol/grant/frame; strong owner/debt survives failure; default-suite/manual boundary truthful. Audit that native4-prerequisite acceptance is not executed or implied by fixture result.
- [ ] Run one fresh final-head full suite using the existing interpreter, explicit worktree PYTHONPATH, persistent log and JUnit in the SOLE ledger. Never duplicate a live run or edit source/tests during it. Inspect failures, existing strict xfail and warnings honestly; a green default suite alone does not satisfy C's real fixture.
- [ ] Only after three meaningful reviewed ordinary pushes, actual C acceptance, whole review and final full pass mark THIS bounded correction task complete. Do not mark ordinary A2 native acceptance, Stage0, original async cancellation proof or P3 complete.
- [ ] Handoff exact root/owner lifetime and capacity state. After a future separately authorized native attempt, even native OBSERVED/closed leaves slot1 UNKNOWN while slot0 is held: no capacity/permission for Stage0 B/C is produced. Their complete-prefix release consumer needs its own reviewed plan. Do not promise reuse after host/root lifetime loss or infer it from an audit receipt.

Self-review completed for this plan: all five design fixes map to A/B/C; normal,
abort and U traces are assigned to B, real host closure/history/release to C.
Constructive ownership precedes every consumer; no import requires an absent
future file. A manifest grows only with implemented files. All shared signatures
above match their later consumers. Current work is plan only: no source restored,
no runtime imported/test/helper/native invoked, no baseline/history/slot mutated.

## Increment A review clarifications (2026-11-15, before B consumers)

The initial A review found ten blocking implementation defects. The binding design remains unchanged. Private interface parameters named `fd` above identify acquisition capabilities, not reusable integers: close, identity binding, parent guards, terminal endpoints and artifact parents must retain the exact generation, role, kind and provenance. Raw numbers may be extracted only for the corresponding immediate syscall. Returned A1 SlotArena objects have one delegated group close path; their raw descriptors cannot also be independently closed by the ledger. A1 close semantics remain intact.

Root admission/closure consults sticky debt in the actual retained owner, including partially constructed bootstrap objects. Canonical supported installation and main native thread checks precede controlled loading. Exact pending slot1 validation uses independently committed prior bytes and strict canonical types. Audit writes use the original parent acquisition, not any owned directory. Host OBSERVED diagnostics require the complete R/S/T/C registration and host reap; the live R result contains descendant reaps only. Ordinary close uncertainty is a finite FD_CLOSE failure and cannot become a checked namespace refusal. The initial report, frozen twelve-file snapshot and each scoped fix review remain in the sole ledger. No new helper/native attempt or release follows from unit review.

## Execution granularity for B

After A is independently accepted, B may be implemented in two sequential reviewed increments: B1 is the exact protocol cursor/bound channel and its tests; B2 is the owner graph, canonical-root submission bodies and both fixed entries with their tests. B2 consumes accepted B1; C consumes completed B2. Neither B1 nor B2 runs a helper or completes the bounded task. Each accepted increment has a substantive ordinary feature push and immediate quota check, so this split adds a real implementation checkpoint without reducing the required three substantive pushes or changing the one-shot C fixture. The prepared B1 brief remains inactive until the coordinator dispatches.

The approved raw design SHA256 91055b5945bf33c9eaf79743d9d4e18569a11b68b02d60711e668fec8f317395 is retained in the sole ledger. Before the A commit, only eight CRLF line endings were normalized to LF; the design lines and decisions are identical. The Spec hash above identifies the normalized committed document.
