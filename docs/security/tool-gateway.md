# Read-only Tool Gateway (internal slice)

`orchestrator.tools.ToolGateway` is a narrow, opt-in path from one accepted
attempt to one isolated command. It currently exposes only the fixed tool id
`system.readonly-command`; it is not a general plugin registry and does not
provide workspace-write, Git mutation, web access, secret use, or external
effects.

## Enforcement sequence

1. The trusted host creates a Gateway instance bound to a Run ID, immutable
   policy snapshot, and one existing canonical workspace directory. A
   `ToolRequest` cannot select another directory. Its command is an argument
   array (never shell-concatenated), bounded to 128 arguments / 32 KiB.
2. A trusted `AttemptAuthority` validates the accepted attempt, fencing
   generation, and `causation_id`. A trusted policy-state provider supplies
   the current revocation and emergency-deny versions. Provider errors fail
   closed.
3. The frozen `PolicyManifest` is evaluated for exactly `safe_read`,
   `read-only`, and `system.readonly-command`. The raw command, workspace path,
   and process output are excluded from event payloads; the request hash binds
   all execution inputs, limits, and policy manifest hash.
4. `ToolRequestReceived` and `PolicyDecision` are appended atomically. An
   `allow` decision adds a one-use `CapabilityGrant`. If approval is required
   and the host has configured `ApprovalService`, the Gateway creates a
   hash-only `ApprovalRequest` bound to this Attempt, command scope, policy
   versions, expiry, and the trusted requester identity; without the service
   it returns `awaiting_approval` and never treats the request event as
   authorization.
5. An approver must use the configured authenticator. The grant cannot resume
   its origin Attempt: the host must present a newer accepted Attempt. Before
   any grant binding is written, the Gateway compares the new command and
   isolation/policy scope to the exact approved hashes. It then binds and
   consumes the one-shot grant with `EffectIntentRecorded` and a zero-cost
   budget reservation, records `ToolApprovalConsumed`/`ToolExecutionStarted`,
   and rechecks attempt and policy authority immediately before launch. The
   Linux/systemd launcher provides the actual read-only
   filesystem/network/resource boundary.
6. Authority is polled while the process runs. Loss requests process-tree
   cancellation. Completion records output hashes/lengths and termination
status. If authority is lost, output is withheld. For an ApprovalService-backed
call, a confirmed process termination gets a hash-only `EffectReceiptRecorded`
result; if the result or receipt cannot be durably recorded, the effect remains
unknown and output is withheld. If termination is not confirmed or the wait
channel fails, the Gateway records an unknown outcome and withholds output; the
request id cannot be implicitly replayed.

Audit append failures prevent a not-yet-started command from launching. A
failure to record the terminal outcome raises `ToolAuditUnavailable` and the
request remains at-most-once/unknown. The application layer must preserve its
attempt slot until reconciliation; this Gateway does not perform that
cross-stream Scheduler reconciliation yet.

## Trust boundary and limits

`AttemptAuthority`, the policy-state provider, `PolicyManifest`, event store,
workspace binding, and launcher are trusted control-plane dependencies.
`DurableAttemptAuthority` is an opt-in adapter that reads Run lifecycle,
Scheduler, and Agent streams from one SQLite snapshot on a fresh read-only
connection per check. It requires the accepted-attempt event ID as causation,
matching role/tool/fence/policy/Agent identity, an unexpired lease, and an
active Run. This avoids using the Gateway's thread-affine writer connection
from its revocation monitor. `ConfigManager.start_run(..., workspace=...)`
freezes a privacy-preserving path/device/inode hash into `RunCreated`;
`LifecycleController.initialize_run` copies it into lifecycle history;
recovery rejects a mismatch. Before each authorization check the durable
adapter rechecks that the configured directory still has the same identity.
The launcher then compares the frozen hash against device/inode values from
the same opened workspace directory descriptor used to build its snapshot,
closing the authorization-to-snapshot replacement window.
Runs without a frozen workspace binding fail closed for this adapter. The
injected authority interface remains trusted and could be misconfigured by a
future caller.

Only the Ubuntu 24.04/WSL2 systemd read-only profile in
[`platform-support.md`](platform-support.md) has live evidence. There is no
enabled workspace-write support, general-purpose Approval/Secret Broker
process boundary, functional Worker, CLI, or MCP wiring. The new ApprovalService
connection covers only this fixed read-only command path and relies on the host
to supply an authenticated requester, an accepted fresh Attempt, and a measured
profile hash; it is not a user-facing approval queue or proof of production
identity. A host-side lease-bound Overlay candidate publisher and
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
version changes, host workspace spoofing, transaction-time authority loss,
audit failure, duplicate request ids, live revocation, unconfirmed
termination, and wait-channel failure. The live integration test
`tests/integration/test_tool_gateway.py` sends a command through the real
`SystemdReadOnlyLauncher` and verifies that the workspace snapshot is visible
while a sibling secret file and raw output are absent from the durable audit.
Unit tests also replace the bound directory at the same path and verify that
the existing Attempt loses authority, and verify that the binding survives
Run snapshot restoration after process restart.
