# P3 Local Staging Custody Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one bounded, real local Stage 0 custody/Worker-denial campaign whose observations cannot authorize production work.

**Architecture:** A facade starts a dedicated isolated runner R, which owns the fixed two-slot arena and the registered S/T/C/W process graph. A first native prerequisite probe runs before the private custodian and full campaign; unsupported actual primitives block later stages. The custodian builds a finite private snapshot/root, executes one non-spawning Worker under private restrictions, and returns only finite non-authoritative observations.

**Tech Stack:** Existing Python test environment; fixed `/usr/bin/python3.12 -B -I -S` CPython 3.12.3 helper; Linux x86_64; Python standard library/ctypes; existing Landlock and capability-drop wrappers; explicitly bound libc/libseccomp. No installations.

**Spec:** `docs/superpowers/specs/2026-11-15-p3-staging-custody-cleanup-design.md`, reviewed Stage 0 only, including its final C-registered/W-not-created kill predicate. Read it with this plan.

## Global Constraints

- Only the Stage 0 gate is implemented. Do not modify base/overlay/Provider launchers, admission, shared candidate syscall policy, GeneralWorker execution/journals, SQLite authority or the preserved snapshot patch.
- Full v1 validation and every prepared/launch_unknown/outcome_unknown hold remain unchanged; no Provider, credentials, network, installation, deployment, main merge or v2 activation.
- Profile is `LOCAL_CUSTODY_X64_PY3123_V1`: Linux native x86_64, 64-bit CPython 3.12.3; fixed runtime bind sources and exact mappings/order from the spec. Missing/mismatched support refuses; no dynamic compatibility fallback.
- Two fixed slots; source <=64 ordinary entries, depth 8, <=1 MiB logical copied bytes; staging tmpfs `size=4MiB,nr_inodes=128`, scratch tmpfs `size=1MiB,nr_inodes=64`; nofile=32, fsize=1MiB, core=0.
- R is a new dedicated helper. Set/verify subreaper, dumpability and limits only in helpers, never pytest/app/agent's main process. Graph is R plus at most two S/T/C/W copies (9 processes); no threads, executors or incidental external helpers.
- Explicit native library paths: `/usr/lib/x86_64-linux-gnu/libc.so.6` and `/usr/lib/x86_64-linux-gnu/libseccomp.so.2`; manifest and read-only native-library bind cover their resolved objects. No `ctypes.util.find_library`, ldconfig, PATH search, shell, mount/umount binaries, compiler invocation or subprocess-producing library discovery.
- Single W cannot fork/vfork/clone/clone3: each is EPERM, including clone3. Do not call `_candidate_exec` or `restrict_candidate_syscalls`; they have different workspace/fork policy.
- All fixture stdout/stderr/control/detail share a cumulative 16 KiB budget; campaign compact result has a separate 16 KiB ceiling. One monotonic 30 s fixture deadline plus one shared <=5 s cleanup budget; no reset per step and no unbounded context-manager cleanup.
- R owns exact pidfds, parent edges, FD/mount/reference ledger and reservation before native probing or helper submission. Native probe is subject to the same capacity/registration/cleanup contract.
- Only registered pre-data R/S/T/C closure can explicitly release the kill fixture after reopen proves no automatic reuse. W does not exist at that cut. Post-data/post-W-fork crash or unknown spawn stays UNKNOWN/charged; kernel PID-namespace reaping is not a trusted targeted reap.
- Unsupported actual support is a valid truthful diagnostic, never denial PASS. No mock/skip/xfail substitutes for the required real campaign. On early failure do not expand B/C or fabricate three completed pushes.
- Three meaningful stages for this one task: each TDD -> scoped independent review -> real commit -> ordinary feature-branch push -> immediately refresh quota. Apply existing user quota/stop rules; do not infer a stale reading is permission. Whole independent review and one fresh final-head full suite are required before task completion.

## Review Focus

1. Unsupported unshare/pidfd/close_range fails before unowned work, and real unsupported cannot become campaign PASS (A's required native test).
2. Hidden subprocess/native-library discovery or R's process-wide changes leak into unrelated pytest/app children (A's isolated-runner and loader tests).
3. FD >4096 survives nofile=32, or proc/cwd/parent aliases give W an outside capability (B's actual high-FD/proc checks).
4. Dead PID-namespace init leads the implementation to invent W reap/cleanup evidence (C's pre-data kill and post-W-fork negative controls).
5. Separate output/deadline allowances, or root/slot reset after a fault, evade finite retention (C's shared-budget, reopen and two-slot campaign tests).

---

## Files and interfaces shared by the stages

All listed files are new except the later stages modifying their own earlier files. Keep the private modules out of public `__init__` exports.

| File | Responsibility |
| --- | --- |
| `src/orchestrator/isolation/_staging_contract.py` | Frozen exact types, case/check/reason enums copied from spec, strict codecs, fixed profile/manifest and finite results. |
| `src/orchestrator/isolation/_staging_native.py` | Explicit libc ABI, pidfd/unshare/maps/close_range/prctl/mount wrappers; no process discovery. |
| `src/orchestrator/isolation/_staging_arena.py` | Two fixed slot records, reserve-before-submit, sticky uncertainty and evidence-checked explicit local release. |
| `src/orchestrator/isolation/_staging_supervisor.py` | Dedicated R ownership graph, bounded native-child launch, reference ledger, output/deadline and reap/cleanup. |
| `src/orchestrator/isolation/_staging_entry.py` | Fixed absolute helper entry; roles `--runner`, `--supervisor`, `--creator`, `--custodian` only, never arbitrary code/path arguments. |
| `src/orchestrator/isolation/_staging_gate.py` | Trusted local facade; prerequisite, fixture and campaign entrypoints. |
| `src/orchestrator/isolation/_staging_snapshot.py` | Small fd-only snapshot writer into C-owned data namespace; no host pathname cleanup. |
| `src/orchestrator/isolation/_staging_restrictions.py` | Private fixed single-W seccomp and read-only workspace Landlock profile. |
| `src/orchestrator/isolation/_staging_custodian.py` | C namespace/mount/root/snapshot/one-W setup and normal closure. |
| `src/orchestrator/isolation/_staging_worker.py` | Fixed trusted positive/denial probes after restriction boundary; finite case IDs only. |
| `src/orchestrator/isolation/_staging_campaign.py` | Exact mandatory case oracles, scheduling and aggregate result. |
| `tests/support/staging_gate.py` | Creates only finite owned fixture roots/source data, writes bounded receipts under explicit test output path. No authority fixtures. |

Freeze these types in A; B/C consume them without alternate spelling:

- `CaseId`, `CheckId`, `ReasonCode`: exact finite enums from the spec; `MANDATORY_CASES: tuple[CaseId, ...]` in spec result order. `FixtureStatus={OBSERVED,UNSUPPORTED,FAILED,UNKNOWN}`; `CampaignStatus={PASS,UNSUPPORTED,FAILED,UNKNOWN}`.
- `FixtureRoots`: retained source/arena FDs and exact owned binding; `create_fixture_roots(parent: Path) -> FixtureRoots` creates one private fresh fixture root/two slots, no fallback/reinitialization. Existing/unsafe/mismatched root fails; no recursive pathname teardown of a replacement. Tests own the finite source explicitly.
- `GateRequest(source_fd: int, arena_fd: int, profile_id: str, case_id: CaseId, started_ns: int, deadline_ns: int)`: exact schema literal from spec, exact builtin fields, frozen slots; hidden repr for FDs; deadline exactly start+30 s, parent-created. No supplied support Boolean/receipt.
- `ProfileManifest`: fixed architecture/interpreter/code/runtime/native-lib identities plus SHA-256 digest; `build_profile_manifest() -> ProfileManifest` hashes exact declared trusted code and explicit runtime objects before helper submission; no environment-controlled import/bind list.
- `NativePrerequisiteReceipt`: internal `stage='native_prerequisites_only'`, finite status OBSERVED/UNSUPPORTED/FAILED/UNKNOWN, finite primitive observations `{subreaper,unshare_user_mount_pid,pidfd,close_range}`, manifest digest, existing `ReasonCode`, `local_observation_only=True`, `non_authoritative=True`. It is neither a fixture OBSERVED nor a campaign PASS and has no release token.
- `FixtureObservation` and `CampaignObservation`: exact schemas/fields/status semantics in the spec; typed frozen slot objects; strict byte codecs reject extra/duplicate/nonfinite/oversized fields. History/cleanup observations are finite, redacted and remain within the shared budget.
- `SlotReservation(token: str, slot_index: int, arena_generation: str)`, `OwnedProcess(role, pidfd, parent_role)`, `ClosureEvidence`: private immutable identities; evidence is produced only by `OwnedGraph`, never decoded from a caller's result.
- `Deadline(started_ns: int, deadline_ns: int, cleanup_deadline_ns: int)`; `remaining_seconds(*, cleanup: bool=False) -> float`; `OutputBudget(limit: int=16384).charge(size: int) -> bool` saturates at 16385, never stores overflowing payload.

Run commands below from this isolated Linux worktree with the same existing pytest environment used by the completed suite and `PYTHONPATH="$PWD/src"`. The helper interpreter remains the profile's fixed `/usr/bin/python3.12`, independently of pytest's interpreter.

### Stage A: Consumed finite contracts, reservation and real native prerequisites

**Files:** Create `_staging_contract.py`, `_staging_native.py`, `_staging_arena.py`, `_staging_supervisor.py`, `_staging_entry.py`, `_staging_gate.py`, `tests/support/staging_gate.py`; create `tests/unit/isolation/test_staging_contract.py`, `test_staging_arena.py`, `test_staging_supervisor.py`, and `tests/integration/test_staging_native_prerequisites.py`.

**Interfaces:**

- `probe_native_prerequisites(request: NativePrerequisiteRequest, *, roots: FixtureRoots) -> NativePrerequisiteReceipt` in `_staging_gate.py` validates, reserves, then starts independent R. It actually consumes the contracts/arena/supervisor.
- `SlotArena.open_existing(arena_fd: int) -> SlotArena`; `.reserve() -> SlotReservation | None`; `.mark_unknown(reservation, reason: ReasonCode) -> None`; `.release_predata(reservation, evidence: ClosureEvidence) -> None`. Two fixed index records and fixed-size header; O_NOFOLLOW/dirfd operations, finite strict codec, `flock` + fsync for transitions; no append-only unbounded history. Missing/corrupt/replaced records refuse; lock availability never frees a slot.
- `OwnedGraph.register(role, pidfd: int, *, parent_role) -> None`; `.confirm_exit_and_reap(role) -> None`; `.close_owned_fds() -> None`; `.predata_closure() -> ClosureEvidence | None`; `.cleanup(deadline: Deadline) -> bool`. Own every FD before another raising operation; no fabricated evidence from PID scan/EOF.
- `run_native_prerequisite(request, roots, manifest, reservation) -> NativePrerequisiteReceipt` executes only fixed pre-data R/S/T/C graph, no W/data mounts. R is subreaper before S, dual-register T/C and source/control FD bindings; wrapper result and explicit release follow the spec's complete pre-data predicate.

- [ ] Write failing finite-contract tests: unknown/duplicate/nonfinite/mutated inputs reject before `reserve`/submit; malformed manifest profile mismatch fails; wrong FD owner/binding refuses; fixture observation cannot decode as native receipt/campaign PASS. Assert `receipt.stage == 'native_prerequisites_only'` and no authority/release field.
- [ ] Run `PYTHONPATH="$PWD/src" python -m pytest tests/unit/isolation/test_staging_contract.py -q`; capture real RED before implementation.
- [ ] Implement strict immutable contracts/codec/manifest and two-slot reserve-before-submit path. Map all native errors to fixed existing reason enum without raw messages/tracebacks.
- [ ] Write failing supervisor tests for spawn/pidfd-registration failure, repeated cleanup/control-flow priority, third-reservation refusal, shared 16384-byte saturation, one absolute deadline, FD-constructor failure and root reopen preserving UNKNOWN. Verify main pytest PID never changes subreaper/rlimits/dumpability.
- [ ] Implement explicit wrappers: `ctypes.CDLL('/usr/lib/x86_64-linux-gnu/libc.so.6', use_errno=True)` with declared ABI for prctl/close_range/mount/umount2; `os.unshare(CLONE_NEWUSER|CLONE_NEWNS|CLONE_NEWPID)`; `os.fork()` only trusted fixed setup edges; `os.pidfd_open`, `signal.pidfd_send_signal`, `os.waitid(os.P_PIDFD, fd, os.WEXITED|os.WNOHANG)`. R uses PR_SET_CHILD_SUBREAPER=36/PR_GET_CHILD_SUBREAPER=37, PR_SET_DUMPABLE=4/PR_GET_DUMPABLE=3. No flag/support fallback.
- [ ] Implement the earliest real probe: reserve before creating R/S/T/C; actual namespace unshare and exact UID/setgroups/GID-map sequence; actual owned pidfd exit/targeted reap; actual `close_range(3,UINT_MAX,CLOSE_RANGE_UNSHARE=2)` in isolated C with known outside FD duplicated >=4096, before nofile=32 would hide it. Keep result on stdio; confirm EBADF, every graph exit/reap and surviving close. No tmpfs/Worker/campaign claim yet.
- [ ] Run the mandatory real test command once: `PYTHONPATH="$PWD/src" python -m pytest tests/integration/test_staging_native_prerequisites.py -q -rs`. `test_native_prerequisites_required_on_this_host` must assert actual OBSERVED and exact primitive set, no mock/skip/xfail. A separate diagnostic test asserts an actual unsupported receipt is truthful/closed or UNKNOWN+charged. Write the finite receipt to the explicit test artifact path.
- [ ] If native prerequisites are UNSUPPORTED/FAILED/UNKNOWN, stop B/C execution. Preserve reason/receipt/owned resources and ask coordinator to record the technical blocker; do not install, weaken the profile or count the bounded task complete. A reviewed diagnostic foundation may be committed as incomplete work, never as three completed stages.
- [ ] On supported real primitives, run all A unit/integration files, obtain scoped independent review of actual receipt/graph/FD teardown, fix findings with new RED, then commit `feat: add bounded local staging native prerequisite gate`, ordinary push and immediate quota reading. Only reviewed A interfaces pass to B.

### Stage B: Real private custodian, finite snapshot and single restricted Worker

**Files:** Create `_staging_snapshot.py`, `_staging_restrictions.py`, `_staging_custodian.py`, `_staging_worker.py`; modify the A private modules/manifest; create `tests/unit/isolation/test_staging_snapshot.py`, `test_staging_restrictions.py`, `tests/integration/test_staging_custodian.py`.

**Consumes:** A contracts/arena/graph/native receipt. A receipt is an execution prerequisite for this local task, not reusable production authority; every actual setup operation is checked again.

**Produces:**

- `run_fixture(request: GateRequest, *, roots: FixtureRoots) -> FixtureObservation` in `_staging_gate.py`, initially supporting only the fixed baseline case; other cases return fixed unavailable/contract error, never placeholder OBSERVED.
- `copy_owned_snapshot(source_fd: int, destination_fd: int) -> SnapshotSummary` in `_staging_snapshot.py`; fixed bounds 64/1MiB/8, no source-controlled callbacks or host pathname cleanup.
- `run_custodian(request, manifest, reservation, graph) -> FixtureObservation` in `_staging_custodian.py`; grant/mount/snapshot/seal/fork/register/exec/close protocol.
- `restrict_single_worker() -> None` in `_staging_restrictions.py`; `run_worker_case(case_id: CaseId, public_fixture: bytes) -> int` in `_staging_worker.py`; code and attack bytes are fixed trusted program, not arbitrary supplied Python.

- [ ] Write/run genuine failing tests for useful baseline read + scratch write, read-only workspace, outside FD >=4096, invalidated anchor avoiding replacement deletion, and private loader path. Use only own finite roots; assert real baseline cannot report OBSERVED before W exists/restrictions are checked.
- [ ] Implement the C boundary with direct libc `mount`/`umount2` (no binaries): rprivate propagation, exact two finite tmpfs mounts and fixed read-only runtime bind set from spec; no mounts outside its isolated namespace. Check parsed own mount/namespace identity/flags, exact maps and the non-dumpable controls. Never hand a data/ns FD to a parent/manager.
- [ ] Implement fd-only snapshot copying in C: borrowed source FD, owned destination FD, O_NOFOLLOW/dirfd descend, precharged entries/logical bytes/depth, protected-root inode prepass and exclusions, no protected hardlink aliases, same-mount/regular-file/relative-symlink/identity rechecks and read-only normalization. Adapt existing snapshot checks without editing the legacy function or calling its pathname cleanup. The finite fixture source is the input domain; no global protected-name/RSS guarantee.
- [ ] After snapshot sealing, C forks its sole W, registers exact W pidfd+parent edge in both R/S and waits for acknowledgement before restriction/exec. Chroot/chdir, set limits, no-new-privileges, bounding/ambient/effective/permitted/inheritable cap drop and verification in the spec's order. Apply Landlock read/execute grants to fixed runtime and `/workspace`; only `/tmp` is writable, plus four explicit device files. Reuse existing Landlock/capability wrappers without changing them.
- [ ] Load private seccomp only through explicit trusted libseccomp path with all ctypes signatures set, context released once, default EPERM. Freeze its allowlist in `_staging_restrictions.py`: read/write/readv/writev/pread64/pwrite64, open/openat/openat2, close, stat/fstat/lstat/newfstatat/statx, statfs/fstatfs, access/faccessat/faccessat2, lseek/getdents/getdents64, getcwd/chdir/fchdir/readlink/readlinkat, unlink/unlinkat/mkdir/mkdirat/rmdir/link/linkat/symlink/symlinkat/rename/renameat/renameat2, chmod/fchmod/fchmodat, truncate/ftruncate/fallocate/fsync/fdatasync, mmap/mprotect/munmap/mremap/madvise/brk, fcntl/flock, poll/ppoll/select/pselect6, ioctl, getuid/geteuid/getgid/getegid/getgroups/getpid/getppid/gettid, clock_gettime/clock_getres/gettimeofday/nanosleep/clock_nanosleep, getrandom/uname, rt_sigaction/rt_sigprocmask/rt_sigreturn/sigaltstack, prlimit64/getrlimit/getrusage, futex/set_tid_address/set_robust_list/rseq/arch_prctl, execve/exit/exit_group, wait4/waitid. Everything else is EPERM, explicitly including all four process-creation syscalls, namespace/mount/ptrace/process_vm/pidfd/handle-open and socket IPC; no clone3 ENOSYS fallback. No permissive retry on a denied syscall.
- [ ] Close construction FDs then invoke actual high-FD close_range primitive; prove only stdio inherited and known high outside FD EBADF; install final private filter and exec the fixed Python leaf. Give W only fixed public fixture identifiers/known attack paths, never source/arena/data/namespace descriptors.
- [ ] Implement normal C -> T -> S -> R trusted cleanup: W exact exit plus C targeted reap, C writer sealing/FD+mount closure ACK, C/T exits and trusted targeted reaps, surviving ledgers closed. Fault or missing acknowledgement after W creation stays UNKNOWN. Retain actual native-call uncertainty; no kernel-init-reap substitution.
- [ ] Run `PYTHONPATH="$PWD/src" python -m pytest tests/unit/isolation/test_staging_snapshot.py tests/unit/isolation/test_staging_restrictions.py tests/integration/test_staging_custodian.py -q -rs`, plus A files. The real baseline must OBSERVE useful I/O and all required map/mount/FD/filter/closure checks; unsupported actual runtime blocks C campaign acceptance. Obtain scoped independent review, fix findings via RED/GREEN, commit `feat: isolate local snapshot custody in a restricted namespace`, ordinary push and immediate quota reading.

### Stage C: Fixed real denial/fault campaign and two-slot closure

**Files:** Create `_staging_campaign.py`, `tests/unit/isolation/test_staging_campaign.py`, `tests/integration/test_staging_campaign.py`; modify the earlier private modules/tests and fixture support only.

**Consumes:** B `run_fixture`, A slot/graph/native ownership; exact spec enums/case table.

**Produces:** `run_campaign(*, roots: FixtureRoots) -> CampaignObservation` in `_staging_gate.py`, starting dedicated R; `_staging_campaign.run_mandatory_campaign(roots, manifest) -> CampaignObservation`; no arbitrary case-list or code API. Extend `run_fixture` to every exact mandatory case with one finite oracle each.

- [ ] First write/run a genuine RED: all real mandatory results except one absent check/case must not aggregate to PASS; fixture OBSERVED cannot be decoded/coerced into campaign PASS; UNKNOWN cleanup takes precedence over FAILED/UNSUPPORTED. Add fixed enum/ordering/count tests and exact 16KiB compact-summary gate.
- [ ] Implement spec scheduling once per case, <=32 fixtures, no retries/new roots; serialize in spec order. Run three kill cases last, creator/custodian/supervisor. Limit slot contention to two S. No next allocation while an actual previous graph remains unconfirmed/charged.
- [ ] Implement real W denial probes for exact stage/arena/source paths, supplied parent aliases, proc root/cwd/fd/ns and high FD, parent rename/unlink/replacement, read-only workspace, forbidden privileged syscalls and all four creation syscalls. For unsafe-to-return child syscalls (vfork/clone), use a fixed reviewed x86_64 native trampoline stored in trusted `_staging_native.py`: raw child branch immediately performs SYS_exit before any return to Python; unexpected parent success reaps that one fixture child and reports violation. No dynamic assembler/compiler/helpers or arbitrary executable bytes. Trampoline identity is in the code manifest; mock errno is never kernel-denial evidence.
- [ ] Add actual tmpfs byte/inode refusal and held-file/namespace reference controls; UNKNOWN/non-reusable observation precedes deliberate owner release, remains in bounded history, then normal graph closure can complete the fixture. Add anchor rename/replacement barriers proving the host never recursively touches substitutes.
- [ ] Implement 3 real pre-data kill cases exactly: R is verified subreaper before S; complete R/S/T/C prefix dual registration, W not created, data grant withheld. Kill only recorded pidfd. Reopen same arena and prove no automatic reuse. Require exact exits and trusted direct-parent or R-adopter targeted reaps plus every surviving FD close; no dead-C ACK is required at this pre-data cut. Only then explicit trusted-owner release, preserving earlier UNKNOWN. Post-W-fork/data-mounted crash, unregistered spawn, unknown cut, missing reap/FD evidence remain UNKNOWN+charged and prevent campaign PASS. Kernel namespace reaping never satisfies trusted reap. Release first two kill fixtures before the next; final history-only metadata marker can remain after explicit release without reoccupying a slot.
- [ ] Implement fixed source/control/spawn/copy/exec/seal/drain faults, primary/control-flow identity matrix and output/deadline cases. Central output budget charges every captured control/stdout/stderr/detail byte, saturates at 16385 and retains no further payload. All waits derive from the original 30s deadline; cleanup shares one additional 5s deadline, no Popen context-manager implicit wait. Reaching either budget with uncertain closure returns UNKNOWN and leaves slot/process-root owner intact.
- [ ] Run `PYTHONPATH="$PWD/src" python -m pytest tests/unit/isolation/test_staging_campaign.py tests/integration/test_staging_campaign.py -q -rs` and all A/B files. Required integration assertion is real `CampaignStatus.PASS`, every exact mandatory case OBSERVED with its required check set; actual unsupported is an explicit technical blocker, not skipped/passing denial. Preserve bounded native/prerequisite/fixture/campaign receipts in the single ledger's task report path; no public authority fields.
- [ ] Obtain scoped independent review of actual denial observations, finite case count/bytes/deadlines, subreaper/reaps and durable sticky slots; fix findings using RED/GREEN. Commit `test: verify real local staging custody denial and crash closure` with meaningful campaign/runtime implementation, ordinary push and immediate quota reading.

## Whole-task acceptance and handoff

- [ ] Independent whole-slice review checks spec coverage and ensures no imports from production authority/Provider/launcher paths consume these observations; no src files outside the private gate and unchanged reused wrappers were edited.
- [ ] On final reviewed HEAD run one full pytest suite with explicit isolated `PYTHONPATH`, persistent log and JUnit in the unique ledger. Do not duplicate a live suite or edit source during it. Inspect full result and known strict xfail separately; a partial/interrupted/unsupported run is not all-pass.
- [ ] Mark this bounded task complete only after three meaningful reviewed pushes, real early prerequisites, real complete mandatory campaign, whole review and fresh full-suite success. Record the profile/manifest digest, exact runtime, native and campaign receipts, unsupported/fault distinctions, retained metadata/history and quota in the sole ledger. No root cleanup claim extends beyond the exact owned graph.
- [ ] Handoff receipt to the coordinator as local evidence only. Until this gate passes, base/overlay custody integration, restoration of the blocked snapshot streaming work, and the async-handoff Stage 0 resource claim remain blocked. Even PASS does not unblock them automatically: each still needs its reviewed consumed design. This plan never releases existing prepared/unknown operations, proves manager queue/stop/settlement or completes P3 overall.

## Current execution boundary

Implement/review Stage A first. Native prerequisite observations have their own exact request/check domain and no campaign PASS. Stage A is an incomplete milestone of this one Stage 0 task, not completion of P3; unsupported actual native primitives block B/C and are preserved as technical evidence. Within A, save meaningful reviewed increments for contracts/arena, supervised native execution, and real fault/receipt tests; do not fabricate RED or successful kernel evidence. Fixed helpers always use explicit -B -I -S and no external library discovery.
