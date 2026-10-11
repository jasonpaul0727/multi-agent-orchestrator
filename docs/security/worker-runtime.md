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

## Pure Worker input commitment

`orchestrator.runtime.worker_input` defines the frozen, strict
`WorkerInputManifest` and `build_worker_input_manifest` data API. Its fields
are schema `maestro.worker-input.v1`, Run and Node identifiers with the same
1–128-character ASCII shape as `AttemptContext`, the existing authoritative
`RoleName`, nonblank task text of at most 100,000 characters, and an empty
input-artifact tuple. Unknown fields, unsupported schema/roles, and nonempty
artifact arrays are rejected. The builder receives the host-selected Run,
Node and role; this pure module does not resolve their durable authority.

Only `canonical_bytes` defines this commitment's wire protocol: JSON uses the
`schema` alias, sorted keys, compact separators, `ensure_ascii=False` and
`allow_nan=False`, encoded as strict UTF-8. Native Pydantic dump methods are
model inspection/explicit serialization interfaces; their default output
must not replace these canonical bytes. `content_hash` is `sha256:` followed
by the lowercase digest of `b"maestro.worker-input.v1\x00" + canonical_bytes`.
Both properties rebuild and validate unsafe copied/constructed models,
reject surviving unknown object fields/extras before rebuilding, and enforce
the 1,048,576-byte manifest envelope. An unknown key already discarded by
Pydantic's unsafe `model_construct` cannot be recovered; raw mappings forbid
unknown keys. Public builder/property errors expose only stable
`WorkerInputError.code` values (`worker_input_invalid` or
`worker_input_too_large`). Guarded builder/property exits retain only trusted
literal codes and raise a fresh error after leaving their handlers, detaching
raw underlying exception args, context and cause rather than merely hiding
their formatted output. Opaque internal codes map to `worker_input_invalid`.
Task text is omitted from model repr.

Text is compared exactly: it is never trimmed, case-folded, newline-normalized,
or Unicode-normalized. Leading/trailing whitespace, CRLF/LF, and composed/
decomposed Unicode change the commitment. Unencodable surrogates fail closed.
Run, Node and role are also committed, so identical text alone cannot bind
another identity. This hash is an equality check, with no confidentiality,
caller-authentication or original-planner-authenticity claim: low-entropy text
may be guessable, and matching an admission commitment proves only
equality with text accepted at that routing admission.

The pure manifest supplies in-memory data only. It adds no persistence,
logging, input cache, reference resolution or production dispatch.
`WORKER_INPUT_OUTPUT_BYTES` fixes the later task output
bound at 16,384 bytes; it is not a caller option or an execution capability.
The production Worker remains blocked-only. The host preparation API below
constructs bounded `WorkerTask` data from frozen Attempt authority.

## Atomic input-bound routing admission

`ControlPlaneApplication.accept_routing_with_input(request, decision, *,
task_text, accepted_at, lease_expires_at)` is available after startup recovery
and rejects calls on a closed application. The application privately owns
`WorkerInputService`; callers cannot supply a role, input hash, verifier
contract/check set, context, workspace/path, usage, result or clock through
this API. The service revalidates routing models, requires the Node's full
frozen planning contract to agree with its Run, identity, role and routing
contract hashes, and derives `manifest.content_hash` from durable Node role
and exact caller text.

One outer `SQLiteEventStore.append_checked` callback serializes that Node
lookup, commitment calculation and existing `Scheduler.accept_routing` call
on the same connection. Scheduler retains its readiness, frozen config,
Registry/policy, budget, Agent, fencing, lease and slot authority. The trusted
verification binding is fixed to `maestro.artifact-verification/v1` with
`artifact-integrity` as its only required check. Nested Scheduler savepoints
commit or roll back together with the outer transaction. No input manifest,
task text, cache, extra snapshot event or readiness event is persisted.
Exact retries produce one durable acceptance/resource effect; changed text
or changed admission authority cannot retrofit an existing commitment.
The older hash-based admission API remains available with its existing rules.

Concurrent cancellation can reject admission. Graph append retains existing
Scheduler semantics: admission commits against the graph it reads under the
lock, and later graph changes do not alter an accepted Node's role/hash.
Verification resolution still requires the exact accepted graph version.
Service errors expose only `WorkerInputError.code`: input validation retains
`worker_input_invalid`/`worker_input_too_large`; routing, time or durable-state
failures and opaque internal codes use `worker_input_admission_failed`.
Admission likewise raises after leaving its handlers, with no owned raw
exception args, context or cause linked to the public error. These boundaries
do not clear Python traceback frame locals or host memory, or sanitize arbitrary
exceptions a caller was already handling. Task text remains in request-lifetime
host memory.

This commitment proves equality with text supplied at admission, not with
the original planner input. Canonical admission and explicit task preparation
are implemented. Automatic execution and a functional Worker remain unavailable.
The real SQLite tests exercise
separate owner-thread connections and fresh spawned-process SIGKILL before
outer commit and after commit; retries do not create partial or duplicate
reservations, Agents, slots or acceptances.

## Frozen task preparation and explicit re-supply

`ControlPlaneApplication.prepare_worker_task(*, run_id, node_id, attempt_id,
fencing_generation, task_text)` requires a ready application and exact caller
text on every call. Identities use `AttemptContext`'s strict ASCII bounds and
a positive integer fencing generation; bool is rejected. A single outer
checked-append transaction samples the private host clock after acquiring the
SQLite lock, validates aware UTC, and resolves the current active, unexpired
verification binding on the same connection. The frozen Node role, planning
contract, graph version, config, Registry and policy identities must agree.
Preparation compares the canonical manifest `content_hash` with the frozen
input commitment, using constant-time digest comparison.

The returned task carries that immutable context, authoritative role, exact
text, empty artifact/capability tuples and a fixed 16,384-byte output limit.
Its strict UTF-8 JSON frame must fit 1,048,576 bytes. Task repr omits plaintext;
explicit JSON serialization deliberately retains it. Preparation creates no
event, idempotency record, reservation, Agent, slot, artifact or execution
effect. Errors expose only trusted literal codes: `worker_input_invalid`,
`worker_input_too_large`, `worker_input_mismatch`, or the generic
`worker_input_preparation_failed`. Owned underlying args/context/cause are
detached after leaving exception handlers, subject to the same frame-local,
host-memory and existing-caller-exception limits described above.

Fresh-process restart permits exact re-supply only while the original authority
is active and unexpired. Missing/changed input, absent/arbitrary legacy hashes,
cancelled/terminal/stale authority and graph changes fail closed; no commitment
is retrofitted. Independent config reload cannot replace the Run's frozen
hashes. Inputs remain in host/returned-task memory for the request lifetime,
with no cache or durable text store. Startup never prepares or executes tasks.
The validity point is the clock sample under the serialization lock. A returned
task is data, not continuing permission to execute: future dispatch and result
acceptance must independently recheck authority.

The real SQLite tests cover lock waiting across the exact lease deadline,
both cancellation/graph serialization orders, fresh spawned-process re-supply,
frozen config reload, injected errors after the binding read, detached error
objects and sentinel absence from durable payloads/logs/database bytes. These
are task-construction checks, not functional Worker or semantic acceptance.
See the [approved design](../superpowers/specs/2026-10-09-p5-worker-input-resupply-design.md)
and [implementation plan](../superpowers/plans/2026-10-10-p5-worker-input-resupply.md).

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
below persists proposals only; acceptance uses the additional host gate below.
The checks prove byte/format properties only;
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

## Proof-gated Attempt acceptance

The acceptance foundation adds a private `AttemptExecutionCoordinator` and
`ControlPlaneApplication.accept_verifier_proposal(*, run_id: str, node_id: str,
attempt_id: str, fencing_generation: int, task_sha256: str) ->
VerifierProposalRecord`. This ready-gated application API accepts only the
exact Attempt identity and proposal task hash. Evidence, usage, completion
time, Worker results, paths, and no-effect claims are not request inputs.
Missing artifacts/dependencies, unavailable usage/audit/store, mismatched
proof, ambiguous settlement, stale generation, cancellation or lease expiry
fail closed without accepting the Attempt or releasing its resource holds.

Trusted host routing admission may supply `input_manifest_hash`,
`verification_contract`, and `required_check_ids`, all together or all absent.
For success, a full frozen PlanningNodeContract and this original admission
binding are required. Admission captures the graph version and constructs the
complete AttemptContext independently of the proposal, including Run, Node,
Attempt, generation, Agent, input manifest, EffectiveConfig, Registry, policy,
routing, and planning commitments. A missing binding cannot be retrofitted;
admission retries must match the original binding. The input hash commits to
host input identity, not to the content of undisclosed inputs.

The coordinator resolves that durable active state and its accepted check
contract, then loads the exact proposal through full-stream journal validation.
It compares the proposal to the independently frozen context, requires an
accepted `builtin.readonly-v1` result for `maestro.artifact-verification/v1`,
and re-runs `IsolatedVerifierProcess`. That path rechecks candidate bytes and
exact publication provenance, not Worker-supplied paths, before staging. Only
the supported deterministic checks, including required `artifact-integrity`,
are admitted. Fresh canonical evidence must match the durable evidence hash.

`EphemeralArtifactGrantAuthority` issues a fresh opaque token for exactly one
digest and one concrete Run, with issuer and aware expiry bound in host memory.
Valid verification consumes it once; replay, mutation, wrong issuer, wrong
scope/digest, or expiry cannot authorize another read. Tokens do not survive
authority replacement/restart and are not signing keys or durable credentials.
Consumed bindings are removed under the same lock; issuance and verification
prune expired unused bindings while retaining live grants. This bounds retention
of obsolete grants, not the number of simultaneously live grants.
The coordinator uses the active lease expiry for these per-artifact grants;
exact Attempt/generation/Agent provenance is checked separately by the
Verifier's ArtifactStore admission gate.

Usage comes from the trusted composition-time `AttemptUsageSource` service,
whose `usage_for(ActiveAttemptSnapshot) -> UsageRecord` binds settlement to the
accepted reservation. `ControlPlaneApplication(..., usage_source=None)` uses
`UnavailableAttemptUsageSource`: default usage is unavailable and acceptance
fails closed. There is no production Provider usage backend in this slice;
deterministic sources are test fixtures. The internal UTC clock supplies both
initial validation and completion time, so callers cannot backdate completion.

`Scheduler.finish_verified_attempt` reloads the exact durable proposal inside
the existing SQLite `append_checked` transaction. It rechecks original
admission, context/check contract, accepted evidence, cancellation, fencing,
lease validity, and reservation/usage before committing lifecycle success,
Agent completion, budget settlement, and slot release together. The success
event records `task_sha256` and `evidence_sha256`; identical retries are
idempotent and conflicting decisions or nested-write faults leave no partial
settlement. Ordinary finish, Provider reconciliation, and public Lifecycle
completion APIs cannot write `succeeded` without this proof-bearing path.

Before readiness or startup settlement, `Scheduler.validate_success_proofs()`
audits all historical Scheduler and lifecycle successes, including earlier
Nodes and events hidden by current snapshots. It reloads each exact proposal
and checks original admission, historical graph/context (including independent
Attempt policy commitment), lease/completion bindings, and recorded hashes.
Later graph expansion does not invalidate an earlier valid success. Missing,
duplicated, corrupted, stale, mismatched, or proofless legacy success raises
`StartupRecoveryFailed`. This audit is read-only: it never promotes a
proposal-only crash to success or rewrites legacy history. The authentic
archived BASE Provider fixture/generator intentionally retain their proofless
success; startup rejects it, with no migration or grandfathering policy.

Independently of successful releases, startup replays every durable Verifier
proposal stream through the journal's schema, integrity and size/count checks,
and requires its Run to have a valid frozen config and recovered lifecycle.
This check shares the startup transaction with pending Provider settlement;
failure rolls back settlement writes and prevents readiness. Historical
proposals do not require a currently active lease and are never promoted.

The acceptance foundation, canonical input-bound admission and frozen task preparation are implemented;
final whole-slice review and push remain controller gates. Worker model/tool
execution, task-input retention,
a Worker candidate-byte publisher, Gateway / Approval /
Secret Broker integration, functional Worker crash recovery, CLI/MCP, full
end-to-end security acceptance, and cost/token/rework benchmarks remain open.
`NodeProposal.task_text` is consumed but not persisted; explicit caller re-supply
can construct bounded `WorkerTask` data. The production Worker remains blocked-only and
V1 is not complete.

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

The 2026-10-09 acceptance-foundation gate, including the recovered Attempt
policy-commitment fix, recorded 391 focused acceptance tests and 1,786 full
suite tests passed, with zero skips and 90.29% total coverage (16,946 statements,
1,645 missing). The full suite ran the required live systemd integration cases;
coordinator coverage was 98.55%, Scheduler core 90.43%, and application 97.16%.
Compilation, dependency consistency, wheel build and complete branch-range
whitespace checks passed. Coverage was measured with `coverage run -m pytest`
and `coverage report` using the existing 90% threshold; pytest-cov is absent.
Tests cover grant reuse, frozen-context/proof mismatch, unavailable usage,
cancellation/expiry races, atomic rollback, real isolated re-verification,
proposal-only restart non-promotion, historical/legacy corruption rejection,
and committed-success response loss. Final whole-slice review and documentation
push remain controller gates; these results do not complete functional Worker,
P5, or V1.
