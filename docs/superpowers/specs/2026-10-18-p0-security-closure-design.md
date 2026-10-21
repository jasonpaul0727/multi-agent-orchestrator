# P0 security closure — proposed implementation boundary

Status: approved by the human on 2026-10-18 ("批准"); not P0 completion or an execution plan.

## Authority and goal

The human explicitly requested completing BOTH remaining P0 checklist items
and then pushing the existing isolated codex branch. This supersedes P3-first
ordering for the current task; it does not approve the separate pending P3
recovery design, deferred P5 Artifact publisher, credentials, paid requests,
deployment, merges, PRs, shared dependency installation or destructive host work.

Binding sources are the approved
`2026-09-13-permissions-security-isolation-approval-design.md` and P0 in
`../plans/2026-09-22-full-v1-product-implementation-plan.md`.
Source base is `ae12d40458e6c8cf41c925ca52cf01a5c965098f`, branch
`codex/p3-systemd-termination-receipts`, existing isolated worktree only.

Deliver safe workspace changes through an actual isolated Worker proposal and
host Gateway, and deny unsupported isolation without fallback. Record the
actual Broker/credential and MCP caller trust boundaries. Do not complete P0
merely by documenting all platforms as unsupported or checking existing unit
tests; the required positive workspace-write path must work under real isolation.

## Verified existing pieces and gaps

- Descriptor-relative snapshot/diff validation excludes protected control/Git
  paths, bounds traversal and bytes, rejects unsafe symlinks, mounts and hardlinks.
- `SystemdOverlayCandidateLauncher` runs untrusted commands in private
  namespaces/OverlayFS with resource limits, Landlock/seccomp, no raw network,
  bounded output, trusted startup and host-verified stop evidence.
- `WorkspaceWriteGateway` already joins policy, durable Attempt authority,
  exclusive workspace lease, approval/capability consumption, publication
  intent/receipt and journaled publication. Profile/identity remain host-injected.
- Publication currently supports additions/updates, per-entry atomic replacement
  and crash rollback, NOT deletion/complete rename or reader-atomic tree swaps.
- `IsolatedWorkerProcess` is blocked-only. `WorkerInputService` supplies no tool
  capabilities. `ControlPlaneApplication` exposes no workspace-write execution.
- Broker process/IPC and local sentinel tests exist, but the production backend
  remains unavailable. MCP has no implemented authenticated product entrypoint.

## P0 completion map

P0 item 1 requires ALL of: safe path operations; bounded Overlay diff export and
independent validation; recoverable publication with defined atomicity; actual
Worker-to-Gateway/application connection; positive and adversarial live evidence.

P0 item 2 requires ALL of: a typed `blocked` result with stable reason
`isolation_unavailable` at the host entry for unsupported platform/capability;
zero unisolated fallback; truthful per-profile support matrix; precise credential
source/lifetime/endpoint and caller-identity boundary documentation and tests.

## Approved choices (originally builder recommendations)

### Workspace transaction semantics

Preserve the declared workspace root identity and current cooperating-writer
lease model. Define atomic application as per-path atomic replacement inside a
durable, serialized, all-or-rollback transaction, NOT a single reader-visible
atomic swap of a whole arbitrary directory. During multi-entry publication,
uncoordinated same-user readers may observe an intermediate tree. Describe this
in the public result/docs, not only an internal comment. Unknown commit or stop
status retains journals, staging and unresolved intent; no optimistic cleanup.

Complete reversible create/update/delete and rename semantics within declared
bounds. Rename is represented as a validated add/update plus source deletion in
one journaled transaction, including directory descendants where necessary;
it is not promised as a reader-atomic rename operation for an entire batch.
The complete preimage (including descendants, modes and symlink targets) must
be safely bounded, verified and durably backed up BEFORE live mutation. Missing
or conflicting preimages reject before mutation. No broad recursive operation
is derived solely from a Worker path or an Overlay whiteout name.

Export a canonical change manifest that represents removals explicitly; do not
trust device files/xattrs supplied by candidate code as deletion authority.
Trusted supervision must derive removals from frozen baseline versus the final
bounded visible candidate tree after all untrusted descendants have stopped.
Independent host validation binds every removal to the same lower baseline.
The implementation plan must pin directory ordering, rename-overwrite behavior,
old journal schema compatibility and each crash seam before source changes.

No write into `.git`, control/config/secret roots, external paths or protected
names becomes permitted. Non-recoverable deletions remain approval-gated and
unsupported unless their exact safe execution contract is separately implemented.
Fixtures may mutate only test-created private workspaces; no real project data
is deleted to demonstrate the feature.

### Worker and host boundary

Implement a bounded actual isolated Worker tool-proposal path, not a callback
or fake executor presented as Worker integration. Worker receives only prepared
input and opaque tool references; it never receives the EventStore, host paths,
workspace lease, Broker socket/nonce, credentials or final-state authority.
It may propose workspace actions but cannot directly publish the real workspace.

The ready-gated host/application boundary derives Run/node/Attempt/generation,
workspace identity, tool allowlist, policy and current authority from durable
accepted state. Worker-supplied identities/profile hashes do not authorize work.
Each request is bounded and strictly decoded, exact-context bound and one-use.
The host rechecks cancellation, expiry, revocation and resource authority before
launch and each publication boundary through the existing Gateway. Approval
cannot revive the originating Attempt; a newer accepted Attempt is required.

The concrete IPC layout must keep the command sandbox's raw-socket denial:
use supervised bounded pipe messages or an equivalently verified dedicated
mediator; do not expose host Unix sockets or broaden network syscall access.
Every untrusted process stays under a verifiable unit/cgroup, including detached
descendants. No response or zero exit code alone constitutes stop proof.

Publish only after valid candidate and complete stop proof; report workspace
publication receipt separately from Node success. No Worker message can bypass
the existing verifier/acceptance proof chain. General LLM execution, all Agent
roles, automatic recovery dispatch and Artifact candidate publication are not
required to prove this P0 tool boundary and remain unfinished P3/P5 work.

### Platform and identity admission

Only the measured Linux/systemd profile can become eligible after live P0 tests;
presence of systemd or a matching OS name is not sufficient. Probe required
controls and bind the actual verified fingerprint to the requested contract.
Missing/mismatched controls, unsupported architecture/paths, insecure private
roots or unverifiable stop fail closed. Host entry translates unavailable
isolation into `blocked/isolation_unavailable` without invoking an alternative
local subprocess, Gateway effect, Broker or Provider sender. Audit write failure
is a distinct fatal/quarantine error, not successful persistence of a block.
This translation applies only to proven pre-start rejection. A failed launch
whose process may have started remains unknown with retained occupancy/staging;
never relabel generic launch exceptions as safe pre-start blocks.

Windows/macOS and unmeasured Linux profiles remain unsupported. Report tested
capabilities independently; passing P0 does not imply all V1 platform/security
or P4/P7 gates passed. Do not silently reclassify unsupported writes as reads.

The initial authority is an explicitly trusted local host session under the
approved single-user threat model. Approval principals come only from a trusted
host authenticator; no Worker/project/request field creates approval authority.
MCP remains unavailable for execution/approval until P6 supplies authenticated
caller identity and a caller Authority Envelope bounded by system authority.
An MCP JSON request, transport connection or claimed local UID is not itself
an authenticated approver. Document what a future adapter must prove.

Document actual Broker provisioning, one-use request lifetime, private runtime
socket permissions/peer+nonce checks, scope and exact endpoint binding, shutdown
and unknown-stop retention. Production credential store remains unavailable;
environment/argv/file credential delivery is not enabled. Credential values
remain limited to trusted Broker/Gateway/fixed sender IPC, never Worker/Shell.
This does not defend against a compromised same-UID host account or promise
Python string erasure. No real secret is needed for P0 boundary documentation
and sentinel leakage tests; no live paid Provider call is authorized.

## Verification and delivery gates

Use test-first per-task implementation and independent specification/quality
review. Reuse existing tests as regression evidence, not fresh claims until run.

- Positive real Worker proposal -> accepted host context -> Gateway -> systemd
  Overlay -> validated create/update/delete/rename -> durable publication receipt.
  Reopen the database and verify exact events, workspace content and no duplicate
  mutation. Test approval-required execution using the real ApprovalService and
  actual isolated runtime together, including a fresh accepted Attempt.
- Negative paths: traversal, symlink/hardlink/mount/control-dir escape, type and
  case collisions, unsafe models/duplicate keys/oversized frames, stale fences,
  cross-Run/workspace requests, forged tool/profile/principal/Grant, audit failure,
  changed policy/expired lease/revocation, preimage conflicts and backup overflow.
- Live isolation: raw network, Broker socket/home/secret/control-path reads,
  resource/output/disk/input bounds, forked/detached children and resistant cancel.
  Unknown stop must prevent publication and cleanup.
- Real subprocess death at backup/prepared/each mutation/fsync/commit/receipt
  boundaries, restart rollback or committed reconciliation, duplicate delivery,
  competing writers and retained unknown publication. No implicit replay.
- Unsupported-platform/capability matrix asserts the stable blocked reason and
  zero launch/effect/Broker/Provider activity; measured profile positive test must
  run, not be skipped or replaced with a fake.
- Sentinel credential audit over Worker/Shell environment, argv, output, events,
  logs, artifacts and error/approval previews; no public Provider requests.
- Full suite >=90% total coverage, required isolation tests zero skips, compile,
  dependency/package checks, exact tested committed-range independent review.
  Record commands, environment, counts, coverage and limits reproducibly.
- Update P0 checkboxes only when EACH mapped obligation is proven. Update README,
  platform/write/Broker/identity docs without marking P3/P4/P5/P6/P7 complete.
  Ordinary push existing branch only after all P0 gates; verify local and remote
  commit identity. No PR, merge or deployment.

## Approval and next step

The human approved this full P0 closure boundary, including the explicitly
crash-recoverable/per-path atomic publication model (not whole-tree reader
atomicity) and isolated tool Worker boundary without general model execution.
Write and review the concrete test-first implementation plan,
then implement all tasks continuously with independent task reviews and push
only after the complete P0 acceptance gates pass.
