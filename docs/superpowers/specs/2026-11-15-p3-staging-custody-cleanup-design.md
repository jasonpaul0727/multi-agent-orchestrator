# Draft: staged local staging custody and cleanup capability gate

Status: DRAFT FOR INDEPENDENT REVIEW. No capability experiment described here has
been executed. No runtime or test implementation is authorized by this document
alone. Drafting and this revision respected the src/tests freeze for suite 97856;
the coordinator has since reported that suite passed. Only this document changed.

## Decision and first deliverable

A local design is feasible without knowing the eventual production deployment.
The first deliverable is a real, bounded Linux capability and Worker-denial gate
for a controlled custodian, using only owned fixtures and children. It must show
that a useful snapshot can be built in an isolated staging namespace, read by an
actually restricted child, and accounted for through successful and interrupted
cleanup. This is a prerequisite experiment with production candidate code, not
an offline state model and not immediate replacement of both launchers.

Prefer staging data in a custodian-owned private mount/PID namespace with finite
tmpfs mounts over trying to prove a mutable host pathname still names our tree.
The host must never recursively remove that data by pathname. A supported future
launcher must establish the tested boundary itself before requested code runs;
a caller Boolean, a previous probe, or an inode sample cannot confer support.
An unsupported profile starts no requested code or production stage; bounded probe setup remains charged until its own cleanup is confirmed.

Gate success permits a second, separately reviewed design for one consumed base
launcher integration. Overlay integration follows only after its additional
trusted writers are accounted for. Gate failure is a concrete unsupported result,
not a reason to restore the old snapshot patch, add an unused registry, silently
fall back to a pathname backend, or accumulate quarantined trees without a cap.

The current code has not proved safe staging cleanup. That fact does not prove
that the new local custodian design is impossible. Conversely, the local gate
cannot prove production stop, manager-queue fencing, old-writer exclusion, or
rollback-resistant global capacity.

## Source basis and unchanged authority

The only progress ledger used is
.superpowers/sdd/2026-11-07-p3-worker-host-mediation-stop-proof/progress.md.
Relevant existing inputs are its v1-async-cancel-handoff-draft.md and
v1-async-cancel-handoff-plan-draft.md, plus
2026-11-13-p3-snapshot-ordinary-entry-bound-design.md. The preserved snapshot
patch remains unapproved and must not be reapplied.

User-authorized scope is local design, independent review, then bounded local
implementation after the active suite exits. Other same-UID host peers are
trusted; Workers and inputs remain untrusted. Trusted peers must use the declared
coordination protocol; their accidental crashes and ordinary application races
still need conservative handling. Trusting peers does not make a Worker trusted,
prove existing namespaces, make a process listing complete, or make a lock a
stop witness. No network, Provider request, credentials, installation, deployment,
main merge, v2 activation, unknown resend, or bypass of complete v1 validation.

The new mechanism supplies no journal ACK, unit stop, settlement, publication,
release, cross-version source winner, or old SQLite writer authority. A late
session, a dead direct fixture child, a closed pipe, an empty directory, or local
namespace cleanup must never be promoted into any of those facts.

## Existing staging and writer map

| Component and source | Current owner/writers and exposure | Current unresolved edge |
| --- | --- | --- |
| workspace.py:81-336 snapshot_workspace | Host caller opens source FD; creates destination by pathname; recursively writes/chmods descendants. Source names and contents are untrusted. | Error paths at 170/325 use pathname rmtree. Source anchoring does not anchor destination ownership. Final chmod uses paths. |
| launcher.py:328-416 base preparation | Host creates TemporaryDirectory, mount targets, workspace snapshot; builds systemd properties. Staging precedes service namespace and Worker creation. | Early cleanup at 337/346/350/353/381/400 and TemporaryDirectory finalizer re-resolve names. A new scan error can reach outer cleanup. |
| launcher.py:416-457 start and ACK | RetainedStaging detaches finalizer; Popen has stdio pipes, close_fds=True. Session enters launcher-local _sessions before receipt observation. | Popen/ACK unknown can retain staging without returning a session. Per-launcher dictionaries are not process/root-wide capacity. |
| launcher.py:226-284, 543-585 teardown | Owned acknowledged recovery/session wait can invoke discard and retirement after existing v1 checks. | These checks must remain intact. discard still uses TemporaryDirectory pathname cleanup; weak owner reference/launcher loss does not transfer custody. |
| overlay_launcher.py:144-208 | Host writes lower snapshot and config; flips lower modes; starts trusted bootstrap through systemd-run scope and unshare. | Same pathname cleanup before launch; detached staging after ambiguous start. Error at 195-197 includes stage/unit names. |
| _overlay_bootstrap.py:62-102, 176-188, 191-249 | Trusted PID-namespace init sees whole stage before chroot; writes buffer/rootfs, mounts tmpfs/overlay/runtime, exports candidate and completion. Requested command gets a chroot later. | Bootstrap, its mount subprocesses, export, completion writer and namespace descendants are real writers/references until finished. Command exit alone does not seal them. |
| overlay_launcher.py:224-279 | Host retains lower/candidate/completion for validation/consumption and later discard. | Candidate consumers are extra readers. A cleanup design cannot discard on transport exit alone or race a consumer. |
| admission.py:169-198 | Admission itself creates a detached temporary source and invokes the real internal overlay launcher. | Admission is also an allocation/launch ingress. Moving a gate after this probe is too late; unknown source lifetime needs capacity too. |
| workspace.py:339-626; workspace_view.py:308-363 | Exporters create candidate trees and have independent pathname failure cleanup. | New overlay integration must cover them; fixing only snapshot rmtree is insufficient. |
| provider_sender.py:19-30, 192-296 | Separate Provider backend imports shared RetainedStaging and SandboxSession; stages runtime/CA data and has its own cleanup behavior. | No Provider integration is proposed. Avoid accidentally changing it by replacing shared helpers; any later change requires separate review without real sends. |

Base unit properties request PrivateTmp, ProtectProc=invisible, ProcSubset=pid,
read-only source/runtime binds, and inaccessible runtime-manager paths
(launcher.py:1207-1268). _exec.py:46-63 grants the service-private /tmp broad
scratch rights. These requests are not a demonstrated denial of host staging
paths or /proc magic-link aliases on every runtime. ProtectProc's name is not a
same-UID security proof. A read-only descendant bind does not by itself prove
its parent is inaccessible in another namespace.

Overlay's chroot, dropped capabilities, native seccomp allowlist and Landlock
are promising building blocks. Its current probe checks selected controls but
not this complete staging-custody claim. A trusted bootstrap being allowed to
write stage does not authorize the requested command to inherit its root,
current directory, namespace handles or other descriptors.

## Proposed local custodian boundary

Stage 0 uses the single non-spawning Worker profile LOCAL_CUSTODY_X64_PY3123_V1.
It starts only owned local helpers, with no production manager or backend change.
The Worker cannot fork, vfork, clone or clone3. A new private seccomp wrapper
must reject all four unconditionally with EPERM, including clone3: do not reuse
candidate_security.restrict_candidate_syscalls, whose ordinary fork/clone policy
is intentionally different. Do not change that shared candidate policy.

Freeze the following support contract. A missing required file/control, a changed
code identity or a different runtime is UNSUPPORTED, with no dynamic substitute.
This freezes a local experiment profile; it does not claim it is already present.

| Item | Exact first-profile choice and enforcement |
| --- | --- |
| OS/architecture/interpreter | Linux native x86_64, 64-bit CPython 3.12.3; exec only /usr/bin/python3.12 with -I -S and a fixed trusted gate entrypoint. Verify actual executable target/version, native ELF architecture and trusted-code manifest before submitting helpers. Reject other patches/architectures; do not locate Python through PATH. |
| Trusted code | New private staging-custodian module and fixed gate/attack fixture entrypoints under orchestrator/isolation, plus their explicitly declared standard-library imports. Manifest records exact bytes/digests of those modules and reused landlock.py/candidate_security.py capability wrappers; import no workspace code, site customization, plugins or user-selected modules. Only cap drop/verification may be reused from candidate_security. Freeze the final manifest at the reviewed implementation HEAD. |
| Read-only runtime binds | Exact sources /usr/bin/python3.12 (file), /usr/lib/python3.12 (directory), /usr/lib/x86_64-linux-gnu (directory), /usr/lib64 (directory), and the exact reviewed orchestrator runtime directory at /runtime. Targets keep the first four absolute names; target /usr/bin contains no other executable. Every source must exist, resolve to an explicitly recorded trusted object and be bind-remounted read-only,nosuid,nodev. No conditional exists-based additions. Controlled /lib -> /usr/lib and /lib64 -> /usr/lib64 symlinks support the loader; no whole-host /usr,/etc,/home,/run or root bind. |
| Data/root/device mounts | One private stage tmpfs size=4MiB,nr_inodes=128; one Worker scratch tmpfs size=1MiB,nr_inodes=64; read-only prepared snapshot at /workspace; empty /proc,/run,/home,/etc; no /dev/fd. Bind only /dev/null,/dev/zero,/dev/random,/dev/urandom as individual device files. No namespace/open-tree FD is exported to the Worker or another process. |
| Trusted setup process graph | One campaign runner R; before starting any S, R must set PR_SET_CHILD_SUBREAPER and verify PR_GET_CHILD_SUBREAPER=1. Failure refuses the campaign before S/data allocation. R tracks/reaps only its ledger-registered exact pidfd identities, including adopted T/C/W; no wait-any or process-scan inference. At most two fixture supervisors S concurrently, only in slot_contention. Each S has exactly one namespace creator T, T exactly one PID-namespace init/custodian C; C may fork its sole W only after snapshot sealing. W does not exist at the pre-data kill barrier. No threads, executors, mount/umount subprocesses, shell, external helper or additional fork. Setup uses direct reviewed libc/syscall wrappers. Maximum graph is R plus two copies of S/T/C/W (9 processes). R does not start the next fixture before the current owned graph is closed or recorded UNKNOWN. |
| Namespace creation and registration | Single-threaded T uses user+mount+PID namespace unshare, then one trusted fork to C, which is PID1. Before any data mount/copy, C is dual-registered in R/S with exact pidfd and trusted parent edge; W has not been created. The data-allocation grant requires complete registration of the current R/S/T/C prefix (R owns S identity, R/S both own T/C identities and edges), not a W registration. After snapshot sealing C performs its one fixed fork to W. Before W may restrict/exec, R/S must both register W exact pidfd and parent edge through the bounded handshake. All allocation/exec grants are explicit and recorded. T/C use PR_SET_PDEATHSIG(SIGKILL) and a before/after parent-identity check. These supplement exact pidfd tracking; parent death is not cleanup proof. Unsupported pidfd creation/transfer refuses the phase, with slot held on ambiguity. |
| UID/GID mapping | Require non-root host ruid=euid=U>0 and rgid=egid=G. Snapshot host supplementary groups (at most 16) before submission. After user-namespace unshare and before any mount: write uid_map exactly `0 U 1`; write setgroups exactly `deny`; then gid_map exactly `0 G 1`, with no helper or extra mapping. Verify all three kernel records before C may allocate. No setgroups call follows deny. Inherited supplementary groups are NOT claimed cleared: record the exact host list and expected namespace-visible mapping (G becomes 0; unmapped groups use the observed kernel overflowgid, which must be 65534 for this profile). Deny any unexpected change. No security claim relies on those groups losing host DAC rights; mount/chroot/FD/LSM boundaries must deny outside access regardless. |
| Pre-exec privilege order | After complete R/S/T/C prefix registration and the explicit allocation grant, C makes propagation rprivate and builds finite mounts/snapshot/root. Only after snapshot sealing does C fork its sole W; W waits for exact R/S dual pidfd/parent-edge registration before restriction/exec. Trusted W setup chroots and chdirs to /workspace; sets hard/soft nofile=32,fsize=1MiB,core=0; sets PR_SET_NO_NEW_PRIVS; drops every present bounding capability while it still can; clears ambient capabilities; capset clears effective/permitted/inheritable; verifies all empty. Establish read-only Landlock runtime and /workspace grants, writable scratch-only /tmp plus the four device files, then close all high FDs as specified below and install the private default-deny seccomp filter. Never invoke _candidate_exec: it grants workspace writes. |
| High-FD closure | After all LSM/filter construction FDs have been closed, trusted W setup calls native close_range(3,UINT_MAX,CLOSE_RANGE_UNSHARE) through a checked libc wrapper, before loading the final Worker filter and exec. Missing primitive/flag support is UNSUPPORTED; no RLIMIT-based loop or CLOEXEC-only fallback. No syscall after that point may create a non-stdio FD except the fixed exec loader/runtime's own allowed opens inside the command root. The trusted graph has no concurrent FD creator. |
| FD confirmation | During setup, inventory exact owned FDs through trusted host proc with a strict finite count, closing the enumerator itself. Include a known fixture-only outside-directory FD duplicated above 4096, above nofile=32; after close_range, fcntl(F_GETFD) must report EBADF for it and every known non-stdio FD. Only 0/1/2 survive the pre-exec boundary. After chroot, proc remains empty. Subsequent Worker file opens are confined by mounts/Landlock; no outside descriptor is inherited. |
| Custodian/control protection | Dedicated S,T,C are non-dumpable after mapping, and have exact parent/child identities recorded. Worker cannot access their proc root/cwd/fd/ns, obtain pidfd_getfd/process_vm/ptrace or request FD forwarding. Actual denial, not the prctl request alone, is mandatory. |

The private Worker seccomp allowlist is frozen in the implementation brief, must
allow the fixed stdlib probe's needed file/I/O/exit operations and must default
deny unreviewed syscalls. Besides unconditional process-creation denial, reject
mount/umount, unshare/setns, chroot, ptrace, process_vm, pidfd access, handle-based
reopen and socket/SCM_RIGHTS IPC. The trusted setup forks only at the two fixed
pre-filter boundaries; no Worker-controlled request can create another node.
The positive control must prove a real single Worker still reads its read-only
snapshot, writes scratch and exits under this actual filter.

The custodian may receive a read-only source FD through the fixed bounded
trusted setup channel. That capability never reaches W. Input cannot select
helper code, mount options, roots, mappings, FD forwarding or recovery actions.
The data stage is created only in C's isolated finite mount namespace. A host
anchor, if needed, is a fixed precreated empty slot directory; the host never
recursively deletes or repopulates it. A normal private host /tmp directory is
not a fallback. Every namespace/LSM/map/mount setup step is checked before W exec;
setup uncertainty is UNKNOWN and remains charged.

## All writers, references and cleanup ownership

Every stage has exactly one trusted custodian writer owner. The parent owns the
control connection, child process identity and capacity reservation; the
custodian owns source/data/mount FDs. A Worker may write only its dedicated
scratch/view. No Worker has a mutation capability for the stage parent/control
namespace. Other participating trusted host code must serialize arena metadata
operations and must not mutate custodian data behind its back.

Use an explicit state protocol: RESERVED -> SETTING_UP -> SNAPSHOTTING ->
SEALED_FOR_CHILD -> CHILD_RUNNING -> EXPORTING (later overlay only) ->
WRITERS_SEALED -> DRAINING_REFERENCES -> CLEAN_LOCAL, with sticky UNKNOWN on
uncertainty. Child execution is impossible before SEALED_FOR_CHILD. Sealed input
is not a durable Worker ACK or execution authorization. Never use a caller's
supplied state/receipt as evidence of a transition.

WRITERS_SEALED requires the exact trusted bootstrap to have finished every
copy/export/completion task and its mount subprocesses, and the namespace
supervisor to have ended/reaped all requested descendants. No input task may
remain queued. A Worker exit code or systemd-run exit alone is insufficient.
An interrupted/unresponsive custodian remains UNKNOWN and charged.

DRAINING_REFERENCES accounts for:

- custodian and descendant tasks holding the mount namespace or a cwd;
- source/data/namespace/open-tree FDs in custodian, Worker, parent or callbacks;
- bind mounts and open files held by another namespace, including a manager;
- transport/control endpoints, late results, candidate consumers and returned
  snapshot handles;
- trusted bootstrap mount/umount subprocesses and export/completion activity.

No reference to a governed data mount may be passed to an external manager in
Stage 0. This makes the local test graph finite and inspectable. If a deliberately
held reference keeps data alive, retain its reservation and demonstrate the
negative control. Namespace leader exit, a successful umount, lazy unmount,
empty proc output or parent pipe EOF is not by itself CLEAN_LOCAL. Namespace
teardown may release unreferenced mounts, but this design does not promise that
all bytes instantly disappear or physical storage is reclaimed.

For Stage 0, CLEAN_LOCAL has a finite owned-graph predicate rather than a general
kernel scan. On the normal W-created path, S's ledger contains exactly T,C,W pidfds, their trusted direct-parent
reap/exit observations, one bounded setup/control channel per edge, its result
pipes, the source/arena/slot FDs, and the fixed stage/scratch mount identities.
C alone owns data/root/bind-mount handles; nothing exports a data/namespace FD.
An intentional held-reference case adds exactly one named custodian-owned file
or namespace FD to that same finite ledger and records it until closed.

C seals snapshot writes before W starts; after W's pidfd reports exit, C reaps
its sole child, closes source/data/held/control FDs except its final bounded
acknowledgement channel once, removes its fixed binds
and mounts without lazy detach, leaves no child/export task, and acknowledges
its exact fixed cleanup sequence, closes that final channel and exits. T then reaps C; S confirms C,T,W exact pidfd
exit and the required trusted reaps, closes every own control/result/fixture FD
except its fixed arena bookkeeping root, and checks its ledger has no data or
namespace reference. S has no data mount to unmount and performs no pathname
rmtree. Ordinary cleanup requires both this complete ledger closure and exact
owned-process termination; EOF, one umount or a process scan never substitutes.
There is one narrower closure predicate for the registered pre-data kill cases.
R's subreaper status must already be verified before any S starts. Before the
kill, the current R/S/T/C prefix must be completely registered, including R's S identity and R/S dual exact T/C pidfds and trusted parent
edges, and the independent surviving coordinator's ledger must show that the
data-allocation grant was never released: no data mount/copy began and W has not been forked. The three kill cases occur only at this C-dual-registered, W-not-created barrier.
After the kill, every registered exact S/T/C process belonging to the killed/terminating prefix must report exit through its
pidfd and have a confirmed targeted reap by its trusted direct parent or by R
as adopter. R uses only registered identities (for example targeted
waitid(P_PIDFD,WEXITED|WNOHANG) within the existing budget); ECHILD without a
recorded trusted-parent/adopter reap is not confirmation. Surviving R/S must
close and confirm every non-metadata fixture/control/pidfd FD in their ledgers.
This conjunction closes the finite registered pre-data graph without requiring
an ACK from a dead C: W never existed, no governed data mount/copy had begun and no data reference was transferred. It does not
weaken ordinary data-allocated cleanup or infer cleanup from subreaper status,
process death, adoption, lock release or absence in a scan.

An unregistered spawn, ambiguous cut/grant, crash after any data mount/copy or at/after W fork,
missing exact exit/reap, or missing surviving-FD closure evidence stays UNKNOWN
and charged. It cannot use the pre-data exception. Kernel PID-namespace termination/reaping is never a trusted-parent/adopter reap observation. In particular a C death after W fork cannot supply W's required trusted targeted reap. No automatic release follows.
A metadata-only occupied marker may remain as a finite negative-control artifact
only after the corresponding independent graph closure is proved; that marker
is not an untracked data/mount reference or permission to reuse an allocation.
This predicate applies only to the declared Stage 0 graph, whose W cannot spawn
and whose fixed trusted code cannot export references.

Local cleanup operates only in the custodian's anchored data namespace after
writers are sealed. It must never reopen a source/destination through the host
pathname to recover a failed handle. The host never rmtrees the arena, slot or
replacement name. No lstat/fstat sequence is presented as atomic protection
against rename/ABA. A parent/slot rename injected by a trusted fixture cannot
make cleanup touch the replacement. Underlying fixed slot directories remain
fixed overhead; they are not recursively deleted as part of ordinary teardown.

## Finite capacity and restart boundary

The proposed integration needs a fixed arena with N precreated slots and fixed
size metadata. Reserve one slot durably before submitting any prepare helper,
manager probe, start operation or other governed allocation. Reserve conservatively
for all required tmpfs mounts, pipes, owned process handles and control buffers.
At most one allocation generation may be active per reserved slot. Queueing,
Popen failure, missing ACK, callback failure and late session all keep the slot
charged. Refuse new work on capacity exhaustion; do not evict unknown entries,
create another arena, append unbounded history, or silently fall back to /tmp.

A slot record is updated through an owned locked descriptor, with a fixed codec
and finite byte limit, and identifies its arena generation and allocation token.
The arena lock serializes participating trusted writers; it is not actor stop,
manager fencing, writer inventory or storage authority. Process death releases
flock but must NOT clear occupied slots. Marker corruption, an unexpected slot,
missing root/record, sync failure, unknown commit, replacement or unsupported
filesystem makes the arena unavailable. A retired process cannot be inferred
from a PID number or stale record; use exact owned process identities locally.

An existing occupied slot is never automatically reinitialized on restart.
Never recover capacity from marker age, host reboot, missing lock, empty host
mountpoint or absence of a process found by a scan. Reuse needs independently
established closure of every possible queued start/writer/mount reference for
that allocation. A successful local direct-helper fixture can establish that
narrow fact for its own graph; a manager-backed production allocation has extra
closure requirements and remains held until those are designed and proved.

Important trust limit: file checksums and fsync do not detect coherent rollback
of arena header, slot records and data together. A fresh process cannot infer
that an apparently new/reverted root never held a stage. The initial capability
gate therefore uses one freshly created fixture arena and does not claim
restart-safe/global capacity. Tests may characterize rollback refusal while a
live parent remembers the generation, but cannot promote that into durable
rollback protection. Production root activation/recovery needs an independently
reviewed continuity anchor and controlled writer/root-reinitialization protocol.
No such authority is assumed here, and the tests-only SQLite prototype supplies
none. Changing root/configuration to regain slots is a capacity reset requiring
that protocol, not an automatic supported operation.

For the local gate, fixture limits are deliberately small and finite, chosen in
the subsequent implementation brief: fixed child/FD counts, a small fixed slot
count, bounded protocol/output, and per-mount byte/inode settings. Record the
exact numbers in results; they are not proposed production defaults. Exhaustion
must fail before another helper/mount/copy, with all previous reservations
retained. No loop creates arbitrarily many new roots or test fixtures.

A tmpfs size option and nr_inodes limit constrain the tested mount's allocation
semantics. Copied logical bytes and created objects must also be precharged by
trusted code before writing/creating. Neither is a claim about total RSS, every
kernel metadata/xattr/page-table allocation, host physical disk/RAM, unrelated
processes, all mount references, or all filesystems. Multiple mounts per slot
need separate fixed limits and accounting; an unknown old mount prevents reuse.
The existing protected-name enumeration gap and broader async handoff resource
gate remain separate: moving data into tmpfs does not bound their Python
allocations or make that Stage 0 PASS.

## Error, cancellation and teardown contract

Preserve the primary control-flow exception object (BaseException excluding
Exception). If the primary is ordinary or absent, propagate the first cleanup
control-flow exception after attempting all owned cleanup. Otherwise preserve
the primary ordinary error and use fixed redacted codes for secondary failures.
Do not include paths, mount arguments, input, raw exception text, tracebacks or
credentials in long-lived status. Best-effort fixed notes cannot replace the
selected exception. Do not store raw Future exceptions/tracebacks indefinitely.

All applicable cleanup attempts run even if one close/signal/wait/umount fails.
Attempt a numeric FD close at most once; never retry a reused FD number. Do not
kill arbitrary names/PIDs or wait unboundedly in a destructor/context manager.
Use exact owned child identities and bounded host waits; native calls may still
lack hard interruption, in which case the helper/slot stays unknown. A cancelled
caller receives cancellation, while a retained process-root owner keeps the
late helper/result/slot. No callback uses the thread-affine journal connection.
No finalizer or garbage collection recursively deletes data or releases slots.

The direct local gate supervisor may terminate and reap only its own fixture
children. That does not grant a production kill-by-name API. Killing its PID
namespace init is a local teardown action, not a systemd/Provider stop witness.

## Stage 0: mandatory real local capability and denial gate

Only after the active suite has completed and this draft is independently
reviewed, implement/run a bounded gate using self-created data and owned real
children. Do not modify either backend first. Candidate reusable implementation
is a private custodian module plus a local owned-snapshot consumer. Freeze its
interface only after review; do not add a public fake support Boolean.

### Exact local gate input, finite campaign and output

The trusted constructor request has exact fields: schema literal
maestro.staging-custody-local-gate.v1; owned read-only source directory FD; owned
fixture-arena directory FD; profile_id=LOCAL_CUSTODY_X64_PY3123_V1; one case_id
from the finite table below; and a parent-created monotonic start/deadline token.
No arbitrary executable, Python/shell text, mount option, path, UID mapping,
forwarding target, production identity, credential or supplied evidence. Reject
unknown/duplicate fields and wrong FD bindings before helper submission. Child
setup consumes only this fixed protocol; descriptor numbers are private data.

Limits are two slots, source fixture at most 64 ordinary entries/depth 8/1 MiB
logical file bytes, the two exact tmpfs sizes/inode counts in the frozen profile,
fixed R/S/T/C/W graph, nofile=32, fsize=1MiB and one shared 16 KiB output budget
per fixture. Source fixtures themselves are finite and self-created; arbitrary
production workspace input and protected-name memory bounds are outside Stage 0.
No copying of runtime into staging and no dynamic bind additions.

Finite check_id enum (no extensible strings):
PROFILE, UID_GID_MAP, NAMESPACE_MOUNTS, CAP_LSM_FILTER, HIGH_FD_CLOSED,
POSITIVE_IO, PRIVATE_PATH_DENIED, PROC_FD_DENIED, PARENT_MUTATION_DENIED,
PROCESS_CREATION_DENIED, PRIVILEGED_SYSCALL_DENIED, BYTE_LIMIT, INODE_LIMIT,
HELD_REFERENCE, ANCHOR_PRESERVED, SLOT_CAPACITY, STICKY_OCCUPANCY,
FAULT_PRIMARY_PRESERVED, UNSUPPORTED_REFUSED, OUTPUT_BOUNDED, DEADLINE_BOUNDED,
OWNED_GRAPH_CLOSED.

The following table freezes case_id and its additional required check IDs.
Every fixture has PROFILE and OWNED_GRAPH_CLOSED as common requirements; a
pre-submission rejection fixture establishes OWNED_GRAPH_CLOSED by demonstrating
that no owned child/data mount was created. The three pre-data kill cases have no W and do not claim W registration, W FD/filter checks or a W reap; their common closure check is exactly the registered R/S/T/C-prefix predicate above. Cases whose fixed oracle reaches W exec also require
UID_GID_MAP,NAMESPACE_MOUNTS,CAP_LSM_FILTER,HIGH_FD_CLOSED. A check cannot be marked
complete from a property string or because its code path was skipped.

| case_id literal(s), each a separate mandatory fixture | Additional required checks and expected observation |
| --- | --- |
| baseline | POSITIVE_IO; actual restricted W reads snapshot, writes scratch, exits. |
| deny_private_paths | PRIVATE_PATH_DENIED; all fixed stage/arena/source path attacks denied. |
| deny_proc_fd | PROC_FD_DENIED; fixed proc aliases and known high outside FD denied/closed. |
| deny_parent_mutation | PARENT_MUTATION_DENIED; fixture-only parent rename/unlink/replacement attacks denied. |
| deny_process_creation | PROCESS_CREATION_DENIED; actual fork,vfork,clone,clone3 calls each return EPERM; no child is created. |
| deny_privileged_syscalls | PRIVILEGED_SYSCALL_DENIED; all fixed mount/namespace/chroot/ptrace/process_vm/pidfd/handle/IPC attacks denied. |
| byte_limit | BYTE_LIMIT; actual finite data limit refusal, no over-limit trusted logical write. |
| inode_limit | INODE_LIMIT; actual finite mount inode refusal and bounded logical create count. |
| held_file_reference, held_namespace_reference | HELD_REFERENCE; one declared held reference produces an earlier UNKNOWN/non-reusable observation; owner releases it explicitly, then closes graph. Preserve the earlier observation. |
| anchor_rename | ANCHOR_PRESERVED; deterministic fixture rename/replacement never causes substitute deletion. |
| slot_contention | SLOT_CAPACITY; two S contend for the same two-slot arena and a third reservation is refused without another process/mount. |
| kill_creator, kill_custodian, kill_supervisor | STICKY_OCCUPANCY; kill only the exact registered identity at the complete R/S/T/C-prefix pre-data barrier: C is dual-registered, W has not been created, and the data-allocation grant has not been released. First reopen the same arena and verify the original occupied token is not automatically reused, even with flock available. Then the trusted owner may explicitly release only after the independent pre-data closure predicate proves every registered exit/reap and surviving FD close. Preserve the earlier UNKNOWN observation. Missing closure evidence stays UNKNOWN/charged. |
| reject_profile | UNSUPPORTED_REFUSED; deliberately mismatched fixed profile is rejected before child/data allocation. This tests refusal; baseline still must run on the real supported profile. |
| output_cap | OUTPUT_BOUNDED; oversized own fixture output triggers cap with no extra retained output. |
| deadline_cap | DEADLINE_BOUNDED; deliberate own-helper pre-data-allocation barrier consumes the one deadline, then bounded cleanup/UNKNOWN retention behavior is observed. |
| fault_before_namespace, fault_after_namespace, fault_after_stage_mount, fault_copy, fault_before_worker_exec, fault_after_worker_exec, fault_writer_seal, fault_reference_drain | FAULT_PRIMARY_PRESERVED; inject the named fixed boundary failure, preserve exact chosen exception and perform every eligible owned cleanup. |
| spawn_failure, lost_response, cleanup_priority | FAULT_PRIMARY_PRESERVED; fixed setup spawn failure, lost control response at the registered pre-data-allocation barrier, or primary plus secondary cleanup-control-flow matrix. No raw errors or new graphs. |

Schedule the three kill cases last, in creator/custodian/supervisor order; result
serialization still uses table order. Each uses one of the same two fixed slots.
First verify reopen cannot auto-reuse the killed allocation. Only afterwards may
the trusted fixture owner perform a separate explicit release using the complete
registered pre-data closure evidence, never merely to regain capacity. Release
and earlier UNKNOWN are separate bounded observations; release does not overwrite
history, change root/generation, or reinterpret a dead C ACK. Complete that
closure/release for the first two kill cases before starting the next. For the
last kill case, demonstrate the same explicit-release precondition and operation;
the final bounded report may keep its original metadata-only UNKNOWN observation as
an audit marker after all mandatory fixtures, without reoccupying the released slot, with no live data/reference and
no subsequent allocation. A missing closure stops the campaign UNKNOWN, with
the slot retained; do not create a third slot/root or bypass capacity. Thus two
slots suffice for the fixed finite campaign without automatic crash reclamation.

case_id consists exactly of the literals in this table. The campaign runs each
literal once, at most 32 fixtures total, no automatic retries, no caller-specified
case list and no unbounded root-creation loop. Each literal is fixed trusted code;
case selection cannot inject an operation. Timing/errno/message strings never
become new case/check labels. Fault injection results are labeled as such and
cannot substitute for actual kernel-denial fixtures.

Finite reason_code enum:
checks_observed, unsupported_runtime, unsupported_namespaces, unsupported_mount,
unsupported_pidfd, unsupported_fd_close, unsupported_lsm, unsupported_syscalls,
boundary_violation, invariant_violation, fixture_contract_invalid,
malformed_protocol, output_limit, deadline, child_setup_failed,
child_spawn_failed, control_lost, local_cleanup_unknown, slot_unavailable,
interrupted. No arbitrary message/detail field.

A single-fixture result has exact fields: schema literal
maestro.staging-custody-local-observation.v1; scope=fixture; case_id; status
OBSERVED/UNSUPPORTED/FAILED/UNKNOWN; profile_digest; completed_checks (unique IDs
from the finite enum); reason_code; local_observation_only=true;
non_authoritative=true. OBSERVED means only this case's complete fixed oracle
matched, with its owned graph confirmed closed. It does not mean campaign PASS.
An intentionally retained metadata-only sticky slot marker is an expected finite
negative-control artifact, not permission to reuse it. A held-reference fixture
must record its earlier operational UNKNOWN before deliberate reference release;
its final OBSERVED cannot rewrite that earlier event.

The separate campaign result has schema literal
maestro.staging-custody-local-campaign.v1, scope=campaign, status
PASS/UNSUPPORTED/FAILED/UNKNOWN, profile_digest, exactly one bounded result per
mandatory case_id in table order, reason_code, local_observation_only=true and
non_authoritative=true. PASS requires every mandatory case OBSERVED and all
required real checks/positive controls, with no unresolved actual process/data/
namespace references. A missing, unsupported, failed, unknown or skipped case
prevents PASS. The negative-control expected refusals are assertions inside an
OBSERVED case, not a waived failure. Aggregate UNKNOWN takes precedence when
any cleanup/reference fact is uncertain; otherwise FAILED precedes UNSUPPORTED.

The 16 KiB limit is cumulative across ALL fixture stdout, stderr, trusted control
observations and any namespace/uid/FD/mount detail report, including intermediate
results. It is not 16 KiB per stream/message/report. No separate unmetered debug
file or traceback capture. Parent nonblocking collection precharges bytes into
the shared budget; at the next byte, saturate the counter, retain only fixed
output_limit status, request owned cleanup and discard further bytes without
retaining them. If draining cannot finish within cleanup budget, return UNKNOWN
and retain ownership/slot. The final campaign serialized summary has its own
16 KiB ceiling; it may contain only the fixed compact case/check/reason results,
never copies of fixture streams/reports. A compact-summary overflow is UNKNOWN,
not a reason to truncate a mandatory case into PASS.

Use one monotonic 30-second fixture budget beginning before reservation/helper
submission: all map/setup/mount/copy/handshake/Worker/read/exit phases consume the
same absolute deadline. No phase/pipe/case step resets it. Cleanup then has at
most one further 5-second monotonic budget shared across all close/signal/wait/
unmount/reap attempts; total parent waiting is at most 35 seconds per fixture.
No destructor, Popen context-manager exit or exception handler performs an extra
unbounded wait. R keeps the fixed owned-process/slot ledger beyond this return.
An unresolved native call or process after that cleanup budget is UNKNOWN and
charged, with no new fixture/helper/root allocation to regain capacity. The
budget bounds parent waiting, not every native/kernel operation or physical
reclamation. No elapsed-time conversion into CLEAN_LOCAL.

UNSUPPORTED means a required real operation/profile is unavailable before W exec
and all local setup references are confirmed closed. FAILED is a wrong boundary
or invariant with confirmed cleanup. Missing/malformed/truncated output, unknown
spawn/response/commit, output/deadline with unconfirmed cleanup, or any unclosed
reference is UNKNOWN. Cancellation preserves its exact selected control-flow
exception after eligible cleanup; only fixed bounded observations are retained.
No fixture/campaign result supplies ACK, stop, Provider evidence, slot-release
token or production-support capability.

Required positive control: create a small source tree, enter the actual isolated
custodian profile, create the finite staging mount, snapshot valid bytes, run an
actual restricted child that can read exactly its intended snapshot and write
its scratch, and return a finite redacted observation. Then seal all writers,
close every reference and complete local teardown. Existing snapshot acceptance
checks remain; no artificial success if the platform lacks a feature.

Required negative/fault controls:

1. The restricted child tries the known exact stage/arena/slot paths, source
   parent, custodian PID root/cwd/fd/ns, parent PID aliases, /proc/self/fd,
   /dev/fd, /proc/1/root, existing symlinks and a supplied outside-directory FD.
   Deny mutation and reading of private control data. Never expose a real
   private host directory as the test target. The outside-FD attempt must show
   it was closed, not merely that its guessed number was absent.
2. Actual rename/unlink/replacement attempts against own source/stage parent
   fixtures, including an independently held parent FD. A successful attack
   blocks the profile. Rename the host anchor at deterministic barriers and
   verify teardown never removes a substitute. Do not fake ENOENT or repair
   an invalidated descriptor by pathname.
3. Observe actual user/mount/PID namespace identities, uid/gid maps, mount
   propagation, mount flags and file descriptor ownership. Check forbidden
   mount/setns/unshare/chroot/ptrace/process_vm/pidfd_getfd/handle-reopen paths
   after the restriction boundary. A static property string is insufficient.
4. Exercise finite byte/inode limits with small own objects; refuse before
   logical overflow. A held file/namespace reference must prevent local
   completion/reuse. Release it deliberately and observe only the specific
   tested lifecycle. No RSS or physical-space conclusion from sample size.
5. Interrupt before/after namespace creation, tmpfs mount, source copy, child
   exec, writer sealing and reference draining. Include child spawn failure,
   lost response and unknown control connection. Confirm no substitute deletion,
   no uncharged helper, fixed bounded retained status and primary-error identity.
6. Two cooperating parent processes contend for the same fixture arena; exactly
   its fixed capacity can reserve. Kill one exact owned helper/parent at a
   barrier; persistent occupied marker survives, and restart refuses to reuse
   it merely because flock is available. A second root is a labeled separate
   domain and must not be described as enforcing a global cap.
7. Unsupported namespace/proc/FD/LSM/syscall behavior returns unavailable with
   no weaker fallback. Stable source/snapshot controls must also pass, so that
   an unavailable profile is not mistaken for a useful secure implementation.

Use deterministic barriers and finite supervisor deadlines, not sleeps as
proof. An attack test double is useful for error ordering but does not establish
kernel denial. Record exactly which checks used real processes/files/mounts,
which were fault injections, and all unsupported/unknown results.

Campaign PASS means only that this controlled profile can supply the stated local
custody boundary on the measured runtime. Independent review must decide whether
that evidence supports a useful consumed product slice. Gate work is not a
completed cleanup correction, cannot clear old unknown stages and does not
satisfy the broader v1 async-cancellation resource proof.

## Later stages, with separate review boundaries

A. Base-only integration design after Stage 0 PASS. Run the custodian inside the
new owned supervised service, with a read-only source binding; prepare snapshot
inside that service before allowing the fixed Worker to enter its private root.
Do not hand a /proc/<pid>/root or /proc/<pid>/fd staging pathname to the manager:
PID reuse and a manager-held mount would add unproved ownership edges. Preserve
old v1 receipt/recovery validation for historical operations and define a new
exact profile for fresh launches; never reinterpret old receipts. Capacity must
be reserved before GeneralWorker prepare submission and continuously bind to the
prepared launch. Until persistent continuity, manager queue and exact local
reference closure are solved, production activation is unavailable. Refusal may
be the reviewed behavior; no staging is allocated to disguise unavailable work.

B. Overlay design after A's consumed boundary is proven. Replace both the host
lower-snapshot staging and trusted bootstrap export/completion storage with the
same custody protocol. Add overlay upper/work/scratch mounts, bootstrap/export
writer sealing, candidate-consumer leases and mount-reference accounting. Its
admission probe must consume capacity before allocating its temporary source or
starting the internal launcher. Keep the existing bootstrap trusted, requested
command untrusted, and full v1/isolated-workspace validation intact. No shared
RetainedStaging replacement may silently alter the Provider backend.

C. Snapshot streaming addendum only after destination ownership is resolved for
all its new failure paths and both actual callers. Revisit the old narrow spec;
do not apply its preserved patch. Preserve root protected-name exemptions,
no-follow/hardlink/mount checks and exact root revalidation. Standalone snapshot
or exporter APIs lacking owned destination custody must be explicitly out of
scope or refuse before destination creation; they cannot be called globally
safe because an internal caller migrated.

Each stage needs a concrete reviewed scope, genuine failing evidence for changed
behavior, independent review and appropriate fresh verification. Do not invent
three superficial changes to meet a push count. This docs-only revision changes no src/tests. Any implementation requires
independent review and coordinator dispatch. The coordinator records results and any substantive
push in the single ledger.

## Local versus external gates

Locally testable now, after review and suite completion: actual namespace
creation, finite mounts, child restrictions/FD/proc denial, anchored local
snapshot handling, exact owned-child lifecycle, finite sticky fixture slots,
error priority and refusal on unsupported behavior. These do not require
production credentials, a chosen production path, or a production service.

Still required before production activation: exact deployed profile/runtime,
complete relevant actor/writer inventory and exclusion of old ingress paths,
a continuity root resistant to supported rollback/root reinitialization,
capacity across participating processes/restarts, manager queued-start fencing,
exact acknowledged session custody and mount/reference closure, and integration
with existing authoritative admission/stop rules. The tests-only durable model
and successful local helper runs cannot supply those facts. P3 overall remains
incomplete until its actual authority/effect/settlement requirements are met.

Stage A interface clarification: native prerequisites use a separate exact NativePrerequisiteRequest schema with source_fd, arena_fd, fixed profile_id, started_ns and deadline_ns only; no case_id. Its receipt carries native_prerequisites_only, finite native primitive observations, fixed reason and non-authoritative/local-only labels; never fixture/campaign checks or PASS. Helpers execute the fixed interpreter with explicit -B -I -S. This separates initial primitive evidence from the full Stage 0 campaign and does not enable any backend.
