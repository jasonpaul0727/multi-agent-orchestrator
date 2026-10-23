# Workspace Write Boundary

The repository now contains an internal, attempt-fenced workspace-write
vertical slice, but product workspace-write remains disabled. Its
`WorkspaceWriteGateway` exposes only `workspace.write-candidate`, evaluates the
frozen `reversible_workspace_write` policy action, runs against a private
OverlayFS candidate, and publishes validated additions, updates, and deletions
through a lease-bound journaled host publisher. Renames are represented as a
source deletion plus a destination addition. Approval-required policy is wired to the
existing `ApprovalService`; it binds the exact command/workspace/policy/profile
scope to a newer accepted Attempt and records an effect receipt. See
[`tool-gateway.md`](tool-gateway.md) and [`approvals.md`](approvals.md) for the
adjacent read-only surface and identity limitations.

Publication intents and receipts are persisted both in the Run security stream
and in a workspace-identity-scoped stream, so unresolved writes block later
writes even when a different Run targets the same directory. The writer holds
the exclusive workspace lease from before candidate snapshot/execution through
publication, rechecks Attempt and policy authority while the command runs and
before each publish step, and recovers prepared journals while holding that
lease. Recovery distinguishes a transaction that never reached its prepared
journal, a confirmed rollback, and a durable commit. Only the first two get a
`WorkspacePublicationAborted` receipt; a durable commit gets a completion
receipt. Run-level receipts are written before the workspace-scoped stream is
resolved, so a missing Run audit record continues to block every writer for
that workspace. An intent without a durable completion/abort receipt remains
unresolved and fail-closes future writes; it is never replayed automatically.

## Private candidate execution contract

`SystemdOverlayCandidateLauncher.launch(workspace, command, limits=...,
input_bytes=..., expected_workspace_identity_hash=...)` freezes a no-follow
snapshot outside the source workspace, excluding Git/control directories. It
launches a systemd user scope with exact cgroup memory/swap/task/CPU bounds,
short stop timeout and new user/mount/network/PID namespaces. The trusted PID 1
checks both its host-frozen cgroup identity and effective limits before mounts
or command execution. It creates a bounded tmpfs upper (16 MiB by default),
scratch tmpfs (8 MiB), a private OverlayFS workspace and read-only runtime root.
The source workspace itself is never mounted into that root.

Before executing the requested command, the child chroots, clears all process,
ambient and bounding capabilities, sets no-new-privileges, installs Landlock
and native-architecture default-deny seccomp. Socket creation, namespace/mount
changes, chroot, xattr mutation, BPF and io_uring are not allowed. Ordinary
fork/threads stay in the same namespaces/cgroup. The root is read-only except
the Overlay workspace and scratch; `/proc`, home, control paths and raw upper
are unavailable. The command gets only a fixed environment and bounded stdin,
not credentials or host control-plane handles. Both trusted Python startup
stages use `-P -S` with an explicit trusted PYTHONPATH, preventing workspace
packages and site-startup code from replacing security setup. The existing
read-only startup now has the same import protection.

After leader exit, namespace PID 1 kills and reaps every remaining descendant,
then unmounts the workspace Overlay. Only then does it normalize three observed,
content-neutral kernel xattrs (`user.overlay.uuid` with a 16-byte value,
`user.overlay.impure=y`, and empty `user.overlay.origin`). Every other xattr is
rejected; index/metacopy/redirect-following are disabled and the command cannot
set xattrs. The existing exporter still rejects all remaining xattrs,
hardlinks, escaping symlinks, protected paths and bounds.

`session.wait()` returns `OverlayCandidateResult(execution, lower_root, diff,
candidate_error)`. A diff is available only after clean execution, complete
input, confirmed transport exit, inactive/failed/missing unit state **and**
kernel evidence that the frozen expected scope cgroup is empty or gone.
`failed` alone is not stop evidence. A private no-follow/nonblocking completion
record is size/schema bounded and duplicate-key safe; candidate contents and
lower baselines are revalidated independently of stdout or the supplied hash.

Use the session as a context manager. Candidate/lower paths remain valid only
until `close()`; consume or copy the candidate via trusted host gates first.
Closing before waiting cancels the scope. Unknown stop prevents admission and
cleanup; later `close()` may clean only after fresh stop proof. An uncertain
transport start always retains staging, even if the unit is not yet visible,
because delayed scope registration is possible. A host crash or lost session
also leaves staging: there is no automatic startup cleanup/recovery service.
Operators must prove the reported scope and cgroup stopped before removing it.
Transport stop attempts have a bounded grace; an unknown outcome never becomes
an accepted candidate.

This runtime is measured only on the documented WSL2/systemd host. It
binds system Python/native libraries explicitly, not the entire `/usr` tree
with WSL driver submounts; arbitrary installed packages, networking and project
test environments are not promised. Missing kernel/runtime controls fail
closed. The launcher itself does not create Attempt/approval/audit events,
authorize publication, supply model/tool mediation or accept lifecycle state.
The fixed-tool application path now carries an accepted Attempt through an
isolated Worker to the host `WorkspaceWriteGateway`, publication audit and
journaled publisher. It is a narrow, explicit internal workflow, not a general
dynamic Worker/Scheduler execution service or enabled product capability.

## Lease contract

`acquire_workspace_write_lease(workspace_root, lease_root)` requires a real
workspace directory and a separate lease directory owned by the current user
with mode `0700`. It rejects overlapping roots, follows no final symlink, and
uses a no-follow regular lock file with mode `0600` under that private
directory. The lease directory is caller-provisioned; acquisition never
creates or repairs the directory itself.

Acquisition takes a non-blocking cross-process exclusive `flock`, verifies the
workspace and lock-file identities, and appends a bounded JSONL record before
returning. The record includes a monotonically increasing generation, a random
lease ID, and the workspace device/inode. It is fsynced while the lock is held.
An incomplete trailing record from a process crash is truncated under the
exclusive lock; malformed complete records fail closed. If persistence reports
an uncertain outcome after a complete append, the next successful acquisition
consumes the next generation rather than reusing it.

The returned lease holds both lock and directory descriptors until `close()`
or context-manager exit. `assert_current()` rechecks the private directory,
named lock file, workspace identity, and latest durable fence. It is intended
to be checked immediately before a future publisher acts. An expired or closed
lease cannot be used as evidence that a write is safe.

## Journaled publication contract

`publish_workspace_diff(lower, workspace, diff, lease, journal_root)` requires
the current lease for that exact workspace and a separate current-user-owned
mode-`0700` journal root. It revalidates the candidate, checks live-path
conflicts, rechecks modified file/symlink lower digests while taking bounded
rollback copies, fsyncs backups and a `prepared` journal, then applies entries
using descriptor-relative no-follow path traversal. Individual file and
symlink replacements are atomic. New files are installed without replacing an
unexpected occupied name; directory changes are limited to owner-accessible
modes so recovery can still traverse them.

A durable `committed` marker is written only after workspace entries and the
workspace root have been fsynced. After restart, acquire the workspace lease
and call `recover_workspace_publications` before any new publish: an incomplete
`prepared` transaction is rolled back idempotently, while `committed` records
are cleaned without undoing the new contents. If a target no longer matches
either its recorded original or candidate state, recovery fails closed and
retains the journal for inspection. Publishing is blocked while any pending
transaction directory exists.

The optional host authorization callback is checked before journaling, before
each entry, and immediately before commit; loss raises a distinct authorization
error so the caller can roll back under the same lease. A caller may supply a
pre-audited 128-bit transaction ID to bind the intent to the journal
transaction.

## Gateway wiring and explicit limitations

- `WorkspaceWriteGateway` holds the exclusive lease from before candidate
  snapshot/execution through publication, so another cooperative writer blocks
  before launching its candidate. It also checks a workspace-identity-scoped
  unresolved-intent stream, including intents created by other Runs targeting
  the same directory.
- A definite publisher error triggers journal recovery. A prepared transaction
  is recorded aborted only after rollback is confirmed; a durable committed
  journal is reconciled as completed. Run-level completion/abort receipts are
  appended before the workspace-wide stream is resolved, leaving failures
  fail-closed. A process crash after a committed publication but before its
  durable receipt still needs operator reconciliation.
- Real Ubuntu 24.04/WSL2 systemd tests exercise the fixed accepted-Attempt
  Worker path through `SystemdOverlayCandidateLauncher`,
  `WorkspaceWriteGateway`, SQLite audit, and journaled publisher. A clean
  checkout smoke also covers serial and concurrent replay and a fresh-Attempt
  ApprovalService path. This does not prove a deployed general Worker/Scheduler
  service or enable product workspace-write.
- Isolation profile hash, AttemptAuthority, policy-state provider, policy
  snapshot, authenticated approval requester, event store, and private
  lease/journal roots are host-injected. Production provisioning, a separate
  authority service, Secret Broker, Worker IPC and Scheduler lifecycle
  reconciliation remain unimplemented.
- The conflict report by itself remains advisory. The publisher holds a lease
  and repeats checks, but the lock only serializes cooperating writers; it is
  not an OS lock against arbitrary same-user edits.
- Validated schema-2 diffs represent deletion explicitly; a rename is exposed
  as a source deletion plus destination addition. Publication records retain
  preimages so rollback can restore both sides. This does not make a multi-path
  batch globally atomic to readers: each file/symlink replacement is atomic,
  while a concurrent reader may observe an intermediate tree.
- The narrow fixed-tool Worker path invokes the internal Gateway with
  host-injected Attempt authority, policy, optional ApprovalService, one-use
  capability events and durable publication audit. It is not the general
  Scheduler/Worker application service and does not enable product
  workspace-write.
- The publisher remains a host-side primitive and no Worker receives this
  lease API. The Gateway is opt-in and does not enable workspace-write in the
  product.

Tests include subprocess termination after partial publication and before the
first write, termination around the commit/cleanup boundary, symlink and
directory rollback, and the hard-link/temp-unlink window. Lease/conflict tests
cover cross-process exclusion, generation replay, partial journal-tail
recovery, corrupt-journal rejection, root/file symlink boundaries,
permissions, root replacement, mount identity, and uncertain persistence.
The live
`test_real_worker_gateway_publication_and_replay` integration test exercises
the fixed accepted-Attempt Worker/Gateway/audit/publisher path through a real
systemd/OverlayFS scope, in serial and concurrent modes. It demonstrates
add/modify/delete/rename-as-delete-plus-add/symlink publication and idempotent
replay, not a general Worker/Scheduler service or product platform support.
The full Task 6 environment, profile hashes, limits, command, and evidence
digests are recorded in
[`2026-10-18-p0-security-closure.md`](../validation/2026-10-18-p0-security-closure.md).
`tests/integration/test_overlay_candidate_launcher.py` now exercises the
actual launcher API, including source immutability, hidden host paths, syscall
denials, trusted import shadowing, detached descendants, SIGTERM resistance,
stdin, cancellation, bounds, export rejection and explicit host publication.
Unit tests cover malformed/FIFO completion, cgroup proof, retained staging,
privilege/seccomp setup failures and post-stop export ordering. These tests do
not replace end-to-end service wiring or a kernel-vulnerability assessment.
