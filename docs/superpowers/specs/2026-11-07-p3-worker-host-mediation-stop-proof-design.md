# P3 Worker host mediation and operation stop proof

Status: revised design for independent review, 2026-11-07.

## Purpose and baseline

The completed general Worker slice at feature commit
96889b8fc55a881d704c7590c8a9d36bfcbd6e3d was merged into main as
5e5a609ca5b1b20af6dae592bb9b2e1feb2c0b1f. It supports bounded terminal-text
work, one Provider call, private response checkpoints and per-turn Worker
start/stop evidence. Unknown Provider outcomes block redispatch. Recovery is
limited to the same host boot and concrete event store.

The next slice proves one Worker-to-host read-only tool mediation and an
operation-wide stopped-actor seal. It is opt-in, versioned and disabled by
default. It does not claim full P3, general tool dialogue, model-driven tool
selection, production Provider delivery/billing proof, or a new autonomous
model round.

Current ToolGateway has policy decisions and audit events, but not a durable
prelaunch identity and exact ACK/stop binding for its command. Its output
digest and stop boolean are not a recovered result or an operation-wide proof.
The Provider sender and Verifier similarly require owned, durable launch
protocols for the new operation. Existing v1 Worker stop receipts remain valid
only for their original scope. In particular a stopped sender proves no remote
delivery, billing, or cessation of unrelated actors.

## User-visible first slice

The caller explicitly requests a terminal text task plus the fixed action
workspace.list_root as exact input. The caller cannot provide command, path,
arguments, environment, execution identity, policy decision or stop evidence.

A fixed real Worker requests the existing one Provider call using the original
task input. The host applies the existing reservation, Provider journal and
unknown-outcome rules. Only after a durable known Provider response, another
fixed Worker turn emits a strictly validated proposal for workspace.list_root.
The host verifies operation identity and an allow decision, starts one owned
isolated tool unit, durably captures its bounded output, and gives that captured
output to a fixed completion Worker. The completion result is a deterministic,
bounded composite containing the Provider text and the listing as separately
identified untrusted data. The model does not receive the listing and no second
Provider call occurs. A supervised Verifier checks the result under the existing
verification contract. Any unsupported Worker output or malformed proposal
fails closed.

The completion Worker cannot establish artifact provenance. The trusted host
loads the exact scoped, immutable Provider-response and authorized-tool capture
outside the write transaction, verifies their operation IDs and digests, and
reconstructs canonical JSON with exactly three string fields: listing,
provider_text and schema, where schema is p3.mediation.composite.v1. Encode
UTF-8 with sorted keys, ensure_ascii=false, separators comma and colon with
no spaces, JSON standard escaping and no trailing newline. No float, alternate
keys, duplicate keys or non-string values are allowed. The completion Worker's
bytes must equal the reconstructed bytes byte-for-byte. Enforce the operation's
output byte limit and existing 16384-byte verifier maximum after encoding;
encoding expansion, invalid UTF-8 or overflow blocks rather than truncates.
The host binds operation identity, all three immutable publication IDs and
digests (Provider response, tool capture, composite), Worker evidence and
Verifier evidence in the proposal, seal and acceptance as applicable. It
rechecks the immutable scoped publication bindings under the final owned
transaction after outside-transaction reads. The existing v1 rule that final
bytes equal Provider response remains unchanged. Verify empty listing, forged,
swapped, duplicated, truncated and expanded encodings.

The listing cannot become host instructions,
change the tool action, grant capabilities, or cause a network action. Empty
listing is a successful value, not a missing checkpoint. The raw Provider
response and listing remain in access-controlled private artifacts; events
contain bounded references, hashes and lengths. Hashes and lengths, especially
of filenames, are sensitive metadata and readers of journal, artifacts and
logs must have the same authorization boundary; hashes are not a privacy
guarantee. Artifact publication is immutable, identified and digest checked;
the acceptance event must reference exactly the published composite.

This slice intentionally returns the listing to the caller, not to a model.
Existing pricing accepts caller-supplied token counts and has no trusted
full-request tokenizer; current attempt-usage production backend may be
unavailable. Neither the original reservation nor a policy label proves that
an added model round is authorized, affordable or settleable. Any future
model continuation requires a separate design for trusted bounds over the
full encoded request and provider framing, authoritative pricing, per-call
reservation, usage settlement, unknown outcomes and data disclosure. Missing
evidence blocks before sending; no synthetic token bound or fake usage is used.

## Exact tool and policy boundary

The host maps workspace.list_root to the existing
system.readonly-command capability, with fixed argv
/usr/bin/ls, -1b, --sort=name, --, . and launcher-owned LC_ALL=C.UTF-8.
No shell, caller-supplied argv, path, environment, network, workspace mutation,
or dynamic executable selection is allowed. The launcher snapshots the entire
workspace to its sandbox and binds the snapshot as cwd. Root .git, .maestro and
.env are excluded by snapshot policy; ls without -a omits all hidden root names.
The command reports snapshot root entries, not an atomic view of the live
workspace. Escaping and ordering are GNU ls under the stated locale. Empty
output is valid. Missing supported executable/locale, failed or over-limit
snapshot, invalid UTF-8 or listing exceeding 4096 bytes blocks the result; no
truncation is represented as a complete listing. Names and command output are
untrusted. Tests cover names containing control characters and non-UTF-8
bytes, hidden names, empty output, overflow and snapshot failures. Platform
support is claimed only after testing the actual isolated launcher.

The first slice admits only an explicit policy allow for this exact capability,
role, manifest, operation, workspace and fixed action. Deny, needs_approval,
unknown state, unavailable policy authority or inability to prove current
revocation state blocks before any tool launch permit; it does not consume an
approval or claim approval recovery. Worker and the single Provider call may
already have occurred; they remain independently represented and settled. Preserve the existing policy and grant
restrictions: an old allow decision, historical audit event, default version
zero or Worker assertion is not current authority. Existing injected callbacks
cannot be assumed atomic or current while a SQLite transaction is held.

A trusted owner must provide a durable, restart-readable, monotonic policy and
revocation authority co-located in the same SQLite store as the Scheduler
fence and operation journal. Its protocol must serialize policy changes and
permit creation under the same owned SQLite write lock; a revocation is not
acknowledged effective until committed there. Its authenticated bootstrap,
version, authority identity, manifest, scope and failure modes are explicit.
An external source that can change independently without that serialization
cannot establish current authority and therefore leaves this slice disabled.
Policy evaluation from the locked durable snapshot must be deterministic,
bounded and free of I/O and injected callbacks. Where the existing gateway has
additional controls that cannot be represented and checked under that same
authority, block rather than bypass them. A single-use grant, where required
by that existing allow path, is bound to the operation and action and consumed
in the same transaction as its launch permit. ToolGateway may be factored to
share these checks and its capability rules; a parallel path must not weaken
its restrictions. The existing needs_approval flow and its crash recovery are
outside this slice.

Task text, Provider request/response payload, credentials, listing content and
composite content must not appear in exceptions, reprs, logs or event payloads.
Use redacted bounded error codes and private authorized artifact references.
Tests use distinctive sentinel values to detect accidental disclosure.

Sample real UTC once after acquiring the owned write lock; use that sample
consistently for all current lease, authority and expiry checks in that
transaction. Do not invoke an injectable clock or other I/O callback within
the transaction.

After tool launch, supervise attempt, cancellation and revocation state while
the unit runs; on loss of authority request cancellation and collect exact
stop evidence. Before captured bytes become an authorized result, revalidate
attempt fence, policy authority, revocation and applicable version in the same
owned checked transaction that authorizes the immutable private publication. Private file I/O finishes before the database transaction; the transaction rechecks immutable publication identity and digest. If authorization fails, the private orphan is inaccessible to readers and cannot be reused.
If revocation wins first, discard output, never return or reuse it, and still
prove the unit and transport stopped. Revalidate those conditions atomically
again at final acceptance; a private publication without current authorization
cannot be resumed, disclosed or accepted. A revocation committed after a
valid terminal acceptance cannot retroactively undo that acceptance. Add
two-connection ordering tests for revocation against launch permit, capture
authorization and acceptance, as well as cancellation while running.

## Unified operation identity and transaction interlock

The versioned operation binds run, node, attempt, Scheduler generation/fence,
exact task input, workspace, capability, manifest and policy authority. The
same concrete SQLite store is the authoritative source for Scheduler, v1
Worker operations, this v2 operation, permits and evidence. Actor IDs and
sequence numbers are unique within the operation. The registry is bounded
and queries are indexed; overflow or corruption fails closed.

Every creation, registration, prepare, launch-permit, acceptance, terminal
failure, cancel, reconcile, release and successor-attempt path must use one
common in-transaction census across both v1 and v2 and across all attempts
and generations of the same run/node. An unresolved prior actor or operation
blocks incompatible creation, release, terminal acceptance and successor
execution. v1 and v2 cannot both become active while the other has unresolved
work. These rules apply to existing public v1 journal entry points and all
Scheduler paths, not merely to the new service. v1 continues to use its
original receipt rules; neither a boolean nor a v1 receipt is upgraded into
a v2 seal.

Cross-stream atomicity means one SQLite connection in its owning thread under
an owned BEGIN IMMEDIATE, including nested savepoints on that connection.
A different connection or thread waits for the lock and is not part of that
transaction. Reject unknown ambient transactions unless ownership and the
write lock are provable; never claim that sequential independent commits are
atomic. Registration, current Scheduler fence, current policy authority,
single-use grant and durable launch permit are checked and committed together.
No process launch, network or other external I/O occurs inside the transaction.

The existing v1 execution path resolves eligibility before a separate
prepare_turn append. Whether a Scheduler release can actually win between
those operations is a hypothesis, not an established defect. First write a
deterministic two-connection barrier characterization that exercises the
public path. If reachable, close the gap before enabling v2. Independently
make all affected public v1 entry points honor the unified atomic gate.
Test races across v1/v2 and attempts/generations, prepare versus release,
cancel versus launch and acceptance versus successor; prove that at most
one incompatible decision commits.

## Owned actor launch and stop protocol

The host owns all externally acting units in this operation: every request,
proposal and completion Worker turn; the tool; the Provider sender; and the
Verifier. Model service activity after a request is delivered is not a local
actor and is not represented as stopped by a sender receipt.

Each concrete launcher is refactored to the same staged protocol:
  1. Prepare an unguessable unique actor identity and exact operation/fence,
     actor kind, unit name, launch parameters and intent digest.
  2. Under the owned database write lock validate the unified fence, policy
     as applicable and open operation, and durably register a unique permit
     and prepared identity before any Popen, thread transport or other launch.
  3. The owned launcher starts the unit and durably binds the authentic ACK to
     the prepared identity, operation and unit/invocation/cgroup evidence.
  4. On completion, cancellation, timeout or recovery, collect and persist
     authoritative stop evidence for that exact ACKed unit. Verify boot,
     unit/invocation identity, cgroup membership and empty/inactive state.
     In addition, prove the exact launcher transport is terminated or
     irreversibly fenced so it cannot submit the unit again. A live local
     systemd-run transport must be observed exited; recovered ACK alone,
     empty cgroup, unit absence or loss of the in-memory session does not
     prove transport quiescence after host crash. If the exact transport
     cannot be identified and proved ended/fenced, hold. Reused or
     mismatched identity fails closed.

The Provider sender's current launch-generated identity and the Verifier's
direct launch/boolean result are insufficient for v2: refactor the concrete
owned systemd launcher and sender/verifier integrations to persist prelaunch,
ACK and exact stop evidence. Injected test doubles can exercise state machines
but cannot supply production stop authority. No caller or Worker may submit
an arbitrary receipt, boolean, unit identity or no-start assertion. Provider
outcome/usage settlement remains separately authoritative; local sender stop
does not resolve outcome_unknown or authorize resend.

Closing is a durable close-intent under the same write lock as permit creation.
It freezes the permit set and prevents any new permit or launch that was not
already permitted. It must not reject a late authentic ACK or stop record for
a pre-close permit. A permitted launcher can pause before Popen or after Popen
before ACK, including in a thread that outlives async cancellation. Therefore
missing ACK, a not-found unit, released owner lock, elapsed timeout, process
absence observed once or unchanged journal version cannot prove no start.
Every unACKed permitted actor remains launch_unknown and blocks the seal,
completion, release and retries, even after close. This first slice has no
general externally submitted no-start proof: unless the exact authentic ACK
is recovered and the actor proved stopped, it stays held. An eventual
transport-fencing/no-start protocol requires its own design and evidence.

All pre-close permits, including async/background work, remain in the census.
Close and permit transactions serialize so none can appear outside the
frozen set. Recovery can append a late ACK and matching stop to that set,
but may not invent either. Unknown or ambiguous evidence fails closed.

## Tool result durability and recovery

The tool executes at most once for a given committed intent. Its private
output is captured, size/UTF-8 validated and durably published with immutable
identity, digest and length before a checkpoint claims it exists. A recorded
start or hash alone is not recoverable output. Existing publish_bytes emits
ArtifactPublished immediately and existing reads can authorize any same-digest
publication in scope; neither is sufficient to hide an unauthorized orphan.
Use a truly inaccessible staging namespace plus an authorized publication
transition, or exact-publication-ID v2 read checks with current policy and
revocation. Existing digest/scope grants must never acquire new read or metadata rights
from unauthorized v2 publications; read_bytes, get_record and list_publications
must not disclose v2 publication/operation association. Independently
previously authorized identical bytes (including empty bytes) remain readable
under their own legacy authority; no rule revokes that existing access. Test
that v2 orphan or revocation adds no rights or association leakage through real
artifact APIs, including same-digest multiple publications. Crash after tool ACK while
output remains only in a host pipe can lose it: recover the exact stop if
possible, then persist result_missing/unknown, prohibit automatic re-execution,
do not send the listing elsewhere, and keep the operation and resource release
blocked. A stopped tool with missing output is not success.

A crash after durable capture can recover the exact validated artifact and
continue the fixed completion Worker and Verifier once all prior actor
requirements are met. Never rerun an already permitted tool merely because a
result is missing. Repeated recovery or duplicate events must be idempotent
without accepting conflicting identity, output or receipt. A Provider
outcome_unknown holds regardless of local sender stop and is never resent.
The existing same-boot/same-store restriction remains; missing boot or store
identity evidence, corrupted or unavailable artifacts and unresolvable actor
identities hold rather than falling back to new work.

## Operation seal and acceptance

An operation seal is not a timeout, application callback or aggregate boolean.
It is created only after close, when the authoritative census proves every
registered permit is resolved by its own exact ACK and authoritative stop,
every required tool result is durably captured, and the existing Provider
outcome/usage and Scheduler settlement conditions permit the terminal action.
A missing capture blocks successful acceptance, and blocks all settlement
where an already-started tool's outcome is unknown. Any launch_unknown,
mismatched/missing ACK or stop, unknown Provider outcome, unresolved settlement,
duplicate/conflicting history or unverifiable authority blocks seal and release. A denial before any permit
may use a no-effect terminal path only when the authoritative registry proves
the set empty and existing settlement requirements are met. Known failures
after permits still require all corresponding stop and known usage/no-effect
settlement; no fabricated stops or usage are allowed.

The release matrix is explicit:
- Tool not permitted, including deny/needs_approval: no tool capture is due.
  A no-tool terminal failure may settle only when the registry proves no tool
  permit and all earlier Worker/sender actors and known Provider charges are
  resolved under existing authority.
- Tool permitted without authentic ACK: launch_unknown; never release.
- Tool ACKed, revoked/cancelled or confirmed failed: discard any unauthorized
  bytes. A terminal failure may settle only after exact unit AND transport
  stop, authoritative known failure/no replay ambiguity, and known Provider
  usage settlement. Failure to prove any of these holds.
- Tool succeeded and capture authorized: successful acceptance requires the
  exact capture, composite checks, Verifier evidence and complete seal.
- Tool might have succeeded but output was lost or authorization is ambiguous:
  result_missing/unknown; no automatic retry, no success or release.
- Provider provably not sent: only existing authoritative no-send and no-charge
  rules permit failure settlement; an absent local ACK is not no-send proof.
  Provider known_success requires known actual usage settlement and local actor
  stop. Provider outcome_unknown always holds and never resends.

The versioned canonical seal input is UTF-8 JSON with sorted object keys,
no floating-point values and explicit domain/schema version. It binds operation
and run/node/attempt/generation, close sequence and registry version, and a
sequence-sorted list of unique actors. Each entry binds actor kind and
prepared identity/digest, ACK binding hash, exact unit stop receipt hash, and independently verified transport quiescence evidence hash.
It also binds tool capture publication ID/digest, Provider journal outcome
and receipt digest, and Verifier evidence/usage digest as applicable. Missing
required fields are not represented as success. No raw response, filenames or
tool output are included. Hash the canonical bytes with the existing approved
digest primitive and version the encoding. Acceptance, settlement and resource
release must atomically recheck the same closed registry and matching seal
under the unified owned transaction. A stale seal cannot authorize actors added
or changed by a competing transaction.

## Verification and independent review

Implement test-first with independent task/spec and code review, then an
independent whole-slice review. Keep every existing v1 fail-closed behavior.
Unit and real two-connection concurrency tests cover the atomic gate,
cross-version and cross-generation races, stale authority, grant consumption,
policy deny/needs_approval, unknown ambient transactions, seal canonicality,
conflicting identities, delayed ACK after close and artifact authorization.
Barrier tests stop execution immediately before Popen and between Popen and
ACK, close concurrently, then resume: no late launch can disappear from the
census or lead to release. Include cancellation while a background thread
still owns a permit.

Real isolated Linux/systemd tests—not fake receipts—cover exact start/stop
for Worker, tool, sender and Verifier, cancellation/timeout and host SIGKILL.
The SIGKILL matrix distinguishes (a) host killed after tool ACK but before
durable capture: stopped if provable, result_missing held, no rerun, and
(b) host killed after durable capture: restart consumes that exact capture and
finishes with one tool invocation and one Provider call; the success barrier
is after both exact tool/transport stop and authorized capture are durable.
A separate host-kill test leaves launcher transport alive and proves recovery
cannot use transient cgroup emptiness or lost local sessions as stop evidence.
A genuine owned
Provider sender against a local loopback TLS fixture provides transport and
stop evidence; it does not prove production delivery or billing. Test unknown
Provider outcome separately with no resend. Verify the snapshot/listing
semantics and hostile filenames against the real launcher, not only mocks.
Capture full suite and coverage after implementation, and run independent
whole-slice review before claiming completion. No paid Provider or real
credentials are needed for these local tests.

This slice does not authorize production Provider calls, new model rounds,
host-independent recovery, arbitrary tools, workspace publication, full
approval recovery, deployment or future main integration. Any environment
that cannot supply real systemd supervision, authoritative same-store policy,
the exact executable/locale or trusted required settlement fails closed and
cannot be reported as validated.
