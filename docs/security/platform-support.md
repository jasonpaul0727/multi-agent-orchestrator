# Isolation backend support matrix

This matrix distinguishes a measured backend profile from a product platform
being supported. A row only becomes a supported product target after the
Approval/secrets path, workspace change publication, Worker integration, and the
full security acceptance suite are integrated and pass.

| Platform | Observed runtime | Read-only command profile | Workspace-write profile | Product support |
| --- | --- | --- | --- | --- |
| Ubuntu 24.04 under WSL2 | Microsoft kernel `6.6.87.2-microsoft-standard-WSL2`, systemd `255.4-1ubuntu8.17` | Live-tested through `SystemdReadOnlyLauncher`; no-follow snapshot, exact cgroup/rlimits, Landlock, private network/tmp/home and read-only binds. Trusted Python startup rejects workspace import shadowing. | `SystemdOverlayCandidateLauncher` and `WorkspaceWriteGateway` live-test private candidate execution, Attempt/policy checks, SQLite publication intent/receipt, exclusive lease and journaled host publication; optional ApprovalService path has unit integration tests. Product workspace-write remains disabled: not invoked by Worker/Scheduler, host injects profile/authority/identity, and no automatic cross-stream reconciliation. Deletions/whiteouts unsupported; multi-entry publication not reader-atomic. | Measured internal backends only, not a complete V1 execution platform. |
| Other Linux distributions | Not measured | Unverified; do not infer support from the presence of systemd. | Unsupported | Unsupported/unverified. |
| Windows and macOS | Not measured | Unsupported by this backend | Unsupported | Unsupported. |

Provider HTTPS dispatch has a separate measured profile on the same Ubuntu
24.04/WSL2/systemd host. `ProviderModelGateway` defaults to a fixed bounded
helper in a per-call systemd service; the helper receives the credential and
request only through stdin, and the host creates a call-bound termination
receipt only after verifying the exact unit/cgroup stopped. Cancellation waits
for that proof, while an unverifiable stop remains unresolved. This profile
intentionally has `PrivateNetwork=no` to permit HTTPS: systemd does not impose
an egress-host allowlist here, so trusted Registry endpoint configuration is
part of the boundary. The live test uses only a local TLS sink and proves
transport/cancellation behavior, not connectivity, authorization, billing, or
reconciliation against a real Provider. This sender profile does not make the
platform a supported V1 execution target.

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
replacement, and explicit restart rollback. A live systemd/OverlayFS probe now
exports a candidate and publishes it through the host transaction, in addition
to unit/subprocess interruption tests. The actual candidate launcher now has
live API tests for source immutability, hidden host paths, import shadowing,
syscall denial, detached children, SIGTERM resistance, input, cancellation,
time/output/disk/file bounds, export rejection and explicit host publication.
Its frozen cgroup identity is checked before command start and its kernel
emptiness after transport exit; unknown start/stop retains private staging.
The curated runtime does not promise arbitrary project packages or test
environments. See `workspace-write.md` for lifetime and cleanup obligations.
The launcher itself does not make policy, Attempt, or approval decisions; the
new opt-in `WorkspaceWriteGateway` composes those checks and durable audit with
the publisher, but is not connected to the application service. Its multi-entry
changes are not a single reader-visible atomic swap, and the product capability
remains disabled.
The Tool Gateway evaluates a frozen
`PolicyManifest`, records `PolicyDecision` and a one-use capability in the
security event stream, checks an injected attempt/fencing authority before and
during execution, and logs only output digests and lengths. A live integration
test covers that full path through the actual systemd unit. This is not yet
connected to a Worker or Scheduler application service. The Tool Gateway does
not implement workspace writes, candidate validation or artifact publication,
workspace-write approval-grant consumption, or a Secret Broker. Its fixed
read-only command has an internal ApprovalService path; reversible workspace
changes use the separate `WorkspaceWriteGateway` ApprovalService path. Other
effects remain blocked, and no functional Worker application service is
connected.
Model and managed-web network access are not provided inside the command unit;
future network access must go through their own Gateway.
