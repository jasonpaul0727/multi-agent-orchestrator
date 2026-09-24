# Isolation backend support matrix

This matrix distinguishes a measured backend profile from a product platform
being supported. A row only becomes a supported product target after the
Tool Gateway, approval/secrets path, workspace change publication, and the full
security acceptance suite are integrated and pass.

| Platform | Observed runtime | Read-only command profile | Workspace-write profile | Product support |
| --- | --- | --- | --- | --- |
| Ubuntu 24.04 under WSL2 | Microsoft kernel `6.6.87.2-microsoft-standard-WSL2`, systemd `255.4-1ubuntu8.17` | Live-tested through `SystemdReadOnlyLauncher`; each launch uses a descriptor-anchored no-follow snapshot, requires exact cgroup/rlimit values, Landlock, private network/tmp/home, and read-only bind mounts. Any mismatch fails closed. | Unsupported; no Overlay diff export, validation, or atomic host application yet. | Candidate only; not a complete V1 execution platform. |
| Other Linux distributions | Not measured | Unverified; do not infer support from the presence of systemd. | Unsupported | Unsupported/unverified. |
| Windows and macOS | Not measured | Unsupported by this backend | Unsupported | Unsupported. |

The backend currently accepts absolute workspace paths without spaces, colon,
backslash, or line breaks because those characters require a separately
verified systemd property-escaping path. The trusted runtime installation path
has the same constraint. This is an explicit fail-closed limitation, not a
path rewrite.

`SystemdReadOnlyLauncher` is an internal isolation component, not yet a public
CLI/MCP feature. It does not authorize tools, call a Secret Broker, request
approval, publish worktree changes, or connect to the lifecycle scheduler.
Model and managed-web network access are also not provided inside the command
unit; any future network access must go through the relevant Gateway.
