# Isolation backend support matrix

This matrix distinguishes a measured backend profile from a product platform
being supported. A row only becomes a supported product target after the
Approval/secrets path, workspace change publication, Worker integration, and the
full security acceptance suite are integrated and pass.

| Platform | Observed runtime | Read-only command profile | Workspace-write profile | Product support |
| --- | --- | --- | --- | --- |
| Ubuntu 24.04 under WSL2 | Microsoft kernel `6.6.87.2-microsoft-standard-WSL2`, systemd `255.4-1ubuntu8.17` | Live-tested through `SystemdReadOnlyLauncher`; each launch uses a descriptor-anchored no-follow snapshot, requires exact cgroup/rlimit values, Landlock, private network/tmp/home, and read-only bind mounts. Any mismatch fails closed. | Still unsupported. A bounded Overlay upper-to-private-candidate exporter now has unit coverage and a live systemd-scope/OverlayFS probe. It rejects deletions/whiteouts, xattrs, special files, hard links, protected paths, lower symlink traversal, and bound violations. It does not validate or atomically publish changes to the host workspace. | Candidate only; not a complete V1 execution platform. |
| Other Linux distributions | Not measured | Unverified; do not infer support from the presence of systemd. | Unsupported | Unsupported/unverified. |
| Windows and macOS | Not measured | Unsupported by this backend | Unsupported | Unsupported. |

The backend currently accepts absolute workspace paths without spaces, colon,
backslash, or line breaks because those characters require a separately
verified systemd property-escaping path. The trusted runtime installation path
has the same constraint. This is an explicit fail-closed limitation, not a
path rewrite.

`SystemdReadOnlyLauncher` and `orchestrator.tools.ToolGateway` now form an
internal, opt-in read-only command path. A separate `export_overlay_diff`
primitive can copy a bounded, validated upper layer into a private candidate
tree; a live namespace test confirms this works with the measured WSL OverlayFS
metadata. It is not an approval, validation, diff-review, or host-publication
path, and does not enable workspace-write. The Tool Gateway evaluates a frozen
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
