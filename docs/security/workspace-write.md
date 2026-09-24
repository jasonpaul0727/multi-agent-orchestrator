# Workspace Write Boundary

The workspace-write path is not enabled. The repository currently contains
three separate primitives: a bounded Overlay upper-layer exporter, a
read-only candidate validator, and a read-only comparison of touched live paths
against the frozen lower snapshot. `acquire_workspace_write_lease` adds a
fourth primitive for serializing host writers, but they are not yet composed
into a publisher.

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

## Explicit limitations

- The lease is not yet bound to Scheduler attempt ownership, policy or approval
  grants, Gateway capabilities, or audit events.
- `check_workspace_publish_conflicts` does not accept or hold a lease; its
  report is advisory and can become stale as soon as it returns.
- No host publisher currently applies a candidate, backs up overwritten
  content, implements deletion/rename semantics, or recovers a partially
  applied transaction after process restart.
- No Worker receives this lease API. The Tool Gateway still does not enable
  workspace-write.

The lease and conflict-report tests cover cross-process exclusion, generation
replay, partial journal-tail recovery, corrupt-journal rejection, root/file
symlink boundaries, permissions, root replacement, mount identity, and
uncertain journal persistence. They are unit evidence for these primitives,
not an end-to-end publication or platform-support claim.
