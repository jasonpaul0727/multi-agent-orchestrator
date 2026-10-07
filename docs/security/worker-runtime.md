# Worker and Verifier Boundary Contracts

`orchestrator.runtime.contracts` defines bounded host-to-Worker inputs and
Worker/Verifier proposals. `IsolatedWorkerProcess` now exercises that input
and result contract through a real systemd-isolated child process, but the
child deliberately returns only `blocked`: it never executes task text,
calls a model or tool, or authorizes a state transition.

## Contract guarantees

- Every message is bound to the exact Run, Node, Attempt, Agent instance,
  fencing generation, graph version, input manifest, EffectiveConfig,
  Registry, policy, routing decision, and frozen planning contract hashes.
- Worker inputs contain task text, opaque content digests, output byte limits,
  and scoped Tool capability references only. They contain no filesystem path,
  EventStore handle, control-directory handle, provider credential, or durable
  state mutation capability.
- A Worker response is a candidate, failure, or blocked proposal. A candidate
  must name at least one artifact, cannot relabel an input digest as a new
  output, and the aggregate declared size must fit the frozen byte limit.
- A Verifier receives an exact candidate artifact set and required check IDs.
  Its result must echo the same attempt, inspect exactly that artifact set,
  report every required check once, and cite only candidate digests. A passing
  proposal is not itself accepted execution state; each passing check must
  cite at least one candidate digest, while an inconclusive result may report
  only the candidate subset it managed to inspect.
- Inbound JSON is size-bounded, UTF-8, rejects duplicate object keys and
  non-finite constants, then validates against strict schemas with unknown
  fields forbidden. Errors do not echo payload values.

These checks establish message shape and causal binding, not truth.
`admit_candidate_artifacts` is a host-only gate that re-hashes ArtifactStore
bytes and requires each candidate digest, type, size, media type, Run, Node,
Attempt, fencing generation, and Agent ID to match a host publication. It
does not promote a candidate to success or attest to an independent check.

The blocked-only Worker child receives at most 1 MiB through stdin, never via a
command argument or environment variable. It runs from a read-only trusted
runtime bind under the measured systemd profile; the host rejects incomplete
input writes, malformed output, child errors, unconfirmed termination, and
any unauthorized candidate. This is a process-boundary smoke test, not a
functional Agent; the Worker cannot yet publish output.

## Host-verified termination and staging

For the systemd launcher, child/transport exit and an accepted `systemctl kill`
are not termination proof. `SandboxResult.termination_confirmed` is derived
only from an immutable host-side `SandboxTerminationReceipt` binding the
generated unit to its exact systemd `ControlGroup` below `app.slice`, an
`inactive`/`failed` unit state, and a kernel cgroup-v2 `cgroup.events` witness
with `populated 0` (or a cgroup already removed after stop). Cgroup paths are
opened beneath `/sys/fs/cgroup` without following symlinks; malformed,
mismatched, timed-out, unavailable, or still-populated evidence fails closed.
Receipt issuance additionally requires the owning host launch/transport process
to have exited. An absent unit/cgroup while `systemd-run` is still alive is an
unconfirmed launch, not proof of stop: that process can submit the unit later.
Grace expiry remains bounded, with no receipt and retained staging.

The session caches its first result. Private staging is explicitly discarded
only after a valid receipt; if stop cannot be verified, staging is retained
and Worker/Verifier transport checks reject the proposal. The live
`tests/integration/test_systemd_launcher.py` suite exercised clean exit,
output-limit stop, runtime timeout, cancellation of a descendant tree, and a
gated real unit submission after an unconfirmed wait on
Ubuntu 24.04 / WSL2 with systemd 255. This establishes only the launcher stop
boundary: it does not complete the full Worker interruption/recovery matrix,
Provider reconciliation, or V1 activation.

`IsolatedVerifierProcess` is a separate host-to-child path. The host first
requires each candidate digest, size, type, media type, Run, Node, Attempt,
fencing generation, and Agent ID to match a verified ArtifactStore
publication. It obtains bytes only through a trusted digest-bound
`ArtifactAccessGrant`, applies a configurable aggregate byte limit (8 MiB by
default), and stages them under generated filenames in a private temporary
tree. The child sees that tree read-only under the systemd profile and receives
only bounded metadata over stdin. Credentials and control-plane handles are
not passed to it.

The built-in acceptance contract `maestro.artifact-verification/v1` supports
`artifact-integrity` (required), `utf8-text`, `json`, and `python-syntax` checks.
The Python check parses source but does not import or execute it. A result is
validated against the exact Attempt and candidate digest set in the host and
remains a proposal: the process module does not persist verification events,
accept a Node, or notify the Scheduler. The separate host journal described
below persists proposals only. The checks prove byte/format properties only;
they are not semantic code review, test-suite execution, documentation
acceptance, or Final Review.

## Durable Verifier proposal replay

`VerifierProposalJournal` records validated task/evidence pairs in the existing
`SQLiteEventStore`, on a dedicated per-Run `verification_proposals` stream as
`VerifierProposalRecorded` events. It supports durable replay after restart,
not durable acceptance. It accepts only the fixed built-in verifier and
`maestro.artifact-verification/v1` contract with the supported check set;
arbitrary acceptance-contract text and artifact bytes are not stored.

The journal reparses caller-supplied models and revalidates their exact
Attempt/artifact bindings before append and during replay. Replay treats
durable payloads and event metadata as untrusted: it verifies canonical
task/evidence hashes, JSON types, Run/Node/Attempt/fencing generation/graph
bindings, artifact digests, schema and stream versions, event headers,
correlation/causation IDs, and the derived idempotency key. EventStore
CAS/idempotency makes identical retries a no-op and rejects conflicting
same-identity proposals. A record is limited to 1 MiB before append; a Run
is limited to 1,024 proposals and 16 MiB of aggregate canonical payload bytes
under the EventStore write lock. Replay enforces the same limits.

This is a trusted-host caller boundary. The caller must obtain evidence
through the isolated Verifier path before recording it; matching verifier IDs,
hashes and bindings do not attest to the process that produced the evidence.
The journal does not re-read or modify ArtifactStore bytes, re-run checks,
accept a Node, write lifecycle state, or affect the Scheduler. Proposal replay
does not provide Worker/Run crash recovery or control-plane acceptance.

Worker model/tool execution, artifact publication from Worker, Gateway /
Approval / Secret Broker mediation, durable evidence acceptance, crash
recovery, and CLI/MCP application services remain unimplemented. No
production Worker profile is enabled by this slice.

The new `SystemdOverlayCandidateLauncher` is a separate internal command
backend, not a change to the blocked-only Worker. It can produce private,
host-validated workspace diffs without modifying source, but no Worker IPC,
Attempt acceptance, ArtifactStore publication, ToolGateway/approval or model
mediation is connected to it. See `workspace-write.md`; callers must not treat
a successful command/diff as a verified Node result. Trusted read-only Python
bootstrap imports now use `-P -S`, so workspace packages/site hooks cannot
replace Worker/Verifier security setup.

## Local tests

`tests/unit/runtime/test_contracts.py` covers schema bounds, duplicate and
stale attempt bindings, input/output aliasing, aggregate size limits, exact
Verifier check/artifact sets, contradictory verdicts, duplicate JSON keys,
malformed UTF-8/JSON, and oversized frames. These are contract tests, not a
security proof for a process boundary. `test_worker_process.py`,
`test_artifact_admission.py`, and the live
`tests/integration/test_isolated_worker_process.py` cover the blocked-only
Worker IPC and host artifact admission boundaries. The Verifier has focused
unit tests plus `tests/integration/test_isolated_verifier_process.py`, which
exercises the real systemd child when systemd --user is available. Termination
receipt requirements and staging retention also have focused tests in
`tests/unit/isolation/test_launcher.py`. This is platform-specific integration
evidence, not a general platform certification.

`tests/unit/runtime/test_verification_journal.py` covers real SQLite restart,
separate-connection concurrent writes, idempotency at the caps, conflicting
proposals, payload/header tampering (including JSON numeric/boolean type
substitution), and unchanged lifecycle/Scheduler streams and ArtifactStore
bytes. The reviewed 2026-10-05 implementation gate recorded 1,567 passed,
zero skipped, and 90.12% total coverage; journal-module coverage was 87.73%.
Focused runtime/live Verifier integration (211 passed, zero skipped),
`compileall`, `pip check`, wheel build, and branch whitespace checks passed.
These measurements cover this proposal-only slice; P5 and V1 remain incomplete.
