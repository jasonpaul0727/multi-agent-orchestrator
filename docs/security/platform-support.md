# Isolation backend support matrix

This matrix distinguishes a measured backend profile from a product platform
being supported. A row only becomes a supported product target after the
Approval/secrets path, workspace change publication, Worker integration, and the
full security acceptance suite are integrated and pass.

| Platform | Observed runtime | Read-only command profile | Workspace-write profile | Product support |
| --- | --- | --- | --- | --- |
| Ubuntu 24.04 under WSL2 | Microsoft kernel `6.6.87.2-microsoft-standard-WSL2`, systemd `255.4-1ubuntu8.17` | Live-tested through `SystemdReadOnlyLauncher`; each launch uses a descriptor-anchored no-follow snapshot, requires exact cgroup/rlimit values, Landlock, private network/tmp/home, and read-only bind mounts. Any mismatch fails closed. | Still unsupported. A bounded Overlay upper-to-private-candidate exporter and read-only validator have unit coverage and a live systemd-scope/OverlayFS probe. A lease-bound host publisher now writes a durable rollback journal and has subprocess interruption/recovery tests; file/symlink replacements are individually atomic, while multi-entry visibility is not. The publisher has not been tested as a live Systemd/OverlayFS→host publication path and is not connected to Approval/Gateway/Worker. Deletions/whiteouts remain unsupported. | Candidate/publisher primitives only; not a complete V1 execution platform. |
| Other Linux distributions | Not measured | Unverified; do not infer support from the presence of systemd. | Unsupported | Unsupported/unverified. |
| Windows and macOS | Not measured | Unsupported by this backend | Unsupported | Unsupported. |

The backend currently accepts absolute workspace paths without spaces, colon,
backslash, or line breaks because those characters require a separately
verified systemd property-escaping path. The trusted runtime installation path
has the same constraint. This is an explicit fail-closed limitation, not a
path rewrite.

`SystemdReadOnlyLauncher` and `orchestrator.tools.ToolGateway` now form an
internal, opt-in read-only command path. A separate `export_overlay_diff`
primitive can copy a bounded upper layer into a private candidate tree, and
`validate_overlay_candidate` rechecks private-only modes, no-follow inventory,
bytes, manifest, and lower snapshot baselines; `check_workspace_publish_conflicts`
reports stale/occupied touched paths without mutation; and
`acquire_workspace_write_lease` offers a private cross-process lock and
monotonic fence. `publish_workspace_diff` binds these primitives in a
lease-held host transaction with backups, a durable journal, per-entry atomic
replacement, and explicit restart rollback. It is unit/subprocess-tested but
not yet live-tested through the Systemd/OverlayFS path or wired into
approval/audit/Worker services. Its multi-entry changes are not a single
reader-visible atomic swap. These primitives do not enable workspace-write.
The Tool Gateway evaluates a frozen
`PolicyManifest`, records `PolicyDecision` and a one-use capability in the
security event stream, checks an injected attempt/fencing authority before and
during execution, and logs only output digests and lengths. A live integration
test covers that full path through the actual systemd unit. This is not yet
connected to a Worker or Scheduler application service. The Tool Gateway does
not implement workspace writes, candidate validation or artifact publication,
approval-grant consumption, or a Secret Broker; requests needing approval
remain blocked.
Model and managed-web network access are not provided inside the command unit;
future network access must go through their own Gateway.
