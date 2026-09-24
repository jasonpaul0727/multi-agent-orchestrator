# Read-only Tool Gateway (internal slice)

`orchestrator.tools.ToolGateway` is a narrow, opt-in path from one accepted
attempt to one isolated command. It currently exposes only the fixed tool id
`system.readonly-command`; it is not a general plugin registry and does not
provide workspace-write, Git mutation, web access, secret use, or external
effects.

## Enforcement sequence

1. The caller uses a Gateway instance bound to the Run's immutable policy
   snapshot and supplies a typed `ToolRequest` for that exact Run. Its command is an argument array
   (never shell-concatenated), bounded to 128 arguments / 32 KiB, and its
   workspace must be absolute.
2. A trusted `AttemptAuthority` validates the accepted attempt, fencing
   generation, and `causation_id`. A trusted policy-state provider supplies
   the current revocation and emergency-deny versions. Provider errors fail
   closed.
3. The frozen `PolicyManifest` is evaluated for exactly `safe_read`,
   `read-only`, and `system.readonly-command`. The raw command, workspace path,
   and process output are excluded from event payloads; the request hash binds
   all execution inputs, limits, and policy manifest hash.
4. `ToolRequestReceived`, `PolicyDecision`, and, only for `allow`, a one-use
   `CapabilityGrant` are appended atomically to the per-Run security stream.
   An approval-required decision records `ApprovalRequested` and returns
   `awaiting_approval`; this slice intentionally has no approval-consumption
   service, so it never treats the event as authorization.
5. Immediately before launch the Gateway rechecks attempt and policy
   authority, atomically consumes the one-use grant, and appends
   `ToolExecutionStarted`. The candidate Linux/systemd launcher provides the
   actual read-only filesystem/network/resource boundary.
6. Authority is polled while the process runs. Loss requests process-tree
   cancellation. Completion records output hashes/lengths and termination
   status. If authority is lost, output is withheld. If termination is not
   confirmed or the wait channel fails, the Gateway records an unknown outcome
   and withholds output; the request id cannot be implicitly replayed.

Audit append failures prevent a not-yet-started command from launching. A
failure to record the terminal outcome raises `ToolAuditUnavailable` and the
request remains at-most-once/unknown. The application layer must preserve its
attempt slot until reconciliation; this Gateway does not perform that
cross-stream Scheduler reconciliation yet.

## Trust boundary and limits

`AttemptAuthority`, the policy-state provider, `PolicyManifest`, event store,
and launcher are trusted control-plane dependencies. The authority callback
must verify the request's causal event against durable Scheduler state; this
module cannot establish that relationship from a caller-provided string by
itself. Revocation is rechecked just before launch and polled during execution.

Only the Ubuntu 24.04/WSL2 systemd read-only profile in
[`platform-support.md`](platform-support.md) has live evidence. There is no
enabled workspace-write support, Approval service, Secret Broker, Worker, CLI,
or MCP wiring. A host-side lease-bound Overlay candidate publisher and
crash-recovery journal now exist with unit/subprocess-crash evidence. The live
`test_systemd_scope_contains_preexec_user_mount_overlay_and_cgroup_limits`
probe also sends a candidate created in a real systemd/OverlayFS scope through
the host publisher. This remains a primitive-only path: it is not integrated
with this Gateway, approval, audit, or attempt ownership, and it does not run
an untrusted Worker through the publisher. Thus these primitives are not
evidence that the complete P4/P5 execution path or the V1 product is
deliverable.

## Verification

The offline unit tests cover deny, approval-required, stale fencing, policy
version changes, audit failure, duplicate request ids, live revocation,
unconfirmed termination, and wait-channel failure. The live integration test
`tests/integration/test_tool_gateway.py` sends a command through the real
`SystemdReadOnlyLauncher` and verifies that the workspace snapshot is visible
while a sibling secret file and raw output are absent from the durable audit.
