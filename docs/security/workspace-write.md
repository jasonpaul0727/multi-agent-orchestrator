# Workspace Write Boundary

The workspace-write path is not enabled. The repository currently contains
an internal `SystemdOverlayCandidateLauncher`, a bounded Overlay upper-layer exporter, a
read-only candidate validator, and a read-only comparison of touched live paths
against the frozen lower snapshot. `acquire_workspace_write_lease` adds a
fourth primitive for serializing host writers. `publish_workspace_diff` now
composes the candidate validator, conflict check, and lease into a bounded
host-side publisher with a durable rollback journal. `recover_workspace_publications`
rolls back prepared transactions or cleans committed transaction records after
restart.

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
deletions/whiteouts, hardlinks, escaping symlinks, protected paths and bounds.

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

This initial runtime is measured only on the documented WSL2/systemd host. It
binds system Python/native libraries explicitly, not the entire `/usr` tree
with WSL driver submounts; arbitrary installed packages, networking and project
test environments are not promised. Missing kernel/runtime controls fail
closed. The launcher does not create Attempt/approval/audit events, authorize
publication, supply model/tool mediation or accept lifecycle state. It is an
internal backend, not a functional Worker or enabled product workspace-write.

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

## Explicit limitations

- The lease is not yet bound to Scheduler attempt ownership, policy or approval
  grants, Gateway capabilities, or audit events.
- The conflict report by itself remains advisory. The publisher holds a lease
  and repeats checks, but the lock only serializes cooperating writers; it is
  not an OS lock against arbitrary same-user edits.
- Current diff export rejects deletions/whiteouts and therefore does not
  implement complete rename or deletion semantics.
- A batch is crash-recoverable, not atomically visible as one change to
  unrelated readers: each file/symlink replacement is atomic, but a reader
  racing a multi-entry publish may observe an intermediate tree.
- This publisher is a host-side primitive only. It is not connected to
  ApprovalService, Scheduler/Worker attempt ownership, Tool Gateway
  capabilities, Secret Broker, or a durable security audit event. It does not
  enable workspace-write in the product.
- No Worker receives this lease API. The Tool Gateway still does not enable
  workspace-write.

Tests include subprocess termination after partial publication and before the
first write, termination around the commit/cleanup boundary, symlink and
directory rollback, and the hard-link/temp-unlink window. Lease/conflict tests
cover cross-process exclusion, generation replay, partial journal-tail
recovery, corrupt-journal rejection, root/file symlink boundaries,
permissions, root replacement, mount identity, and uncertain persistence.
The live `test_systemd_scope_contains_preexec_user_mount_overlay_and_cgroup_limits`
integration test additionally creates and validates a candidate inside a real
systemd/OverlayFS scope, then invokes the host-side publisher and checks the
published workspace plus unchanged lower tree. This is still not
end-to-end Approval/Gateway/Worker integration or a product platform-support
claim.
`tests/integration/test_overlay_candidate_launcher.py` now exercises the
actual launcher API, including source immutability, hidden host paths, syscall
denials, trusted import shadowing, detached descendants, SIGTERM resistance,
stdin, cancellation, bounds, export rejection and explicit host publication.
Unit tests cover malformed/FIFO completion, cgroup proof, retained staging,
privilege/seccomp setup failures and post-stop export ordering. These tests do
not replace end-to-end service wiring or a kernel-vulnerability assessment.
