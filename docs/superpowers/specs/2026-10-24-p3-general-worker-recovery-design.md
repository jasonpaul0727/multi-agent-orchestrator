# P3 general Worker recovery — useful execution after real SIGKILL

## Authority and intent

On LA 2026-10-24 the human explicitly said: skip questions, draft the new
general Worker recovery design, prioritize real SIGKILL followed by restart
and actual task completion, never resend outcome_unknown Provider calls,
have a reviewer examine the design, then develop directly. This authorizes
the design/review/plan/implementation sequence without another confirmation.
It does not authorize real credentials, paid calls, PRs, deployment, cleanup,
force-push, or another main merge. The earlier Task4/main integration is done.

Source base is codex `36423cfdf9f7d4c6a9e755bc555d3a9999b0953d`, whose tree
equals main merge `2ab247db388d326cae60b2a3d58ef22117dbec3c`.
Use the existing Python 3.12/Pydantic/SQLite/systemd architecture and isolated
worktree; no dependency installation. Existing blocked-only Worker stays
blocked-only. A new explicit host API owns useful execution and recovery.

## Goals and honest scope

1. Execute arbitrary accepted text tasks through a bounded isolated Worker
   and host-mediated existing ModelGateway; produce an actual UTF-8 artifact.
   The first supported completion contract is terminal model text. Tool calls,
   arbitrary shell execution and workspace publication fail closed here.
2. Parent-issued SIGKILL of a genuinely running Worker, followed by a new
   application/process and explicit exact-input re-supply, must complete the
   task from durable host checkpoints without inventing a Worker result.
3. A host crash after a verified launch or a captured response must recover
   safely. Ambiguous launch/send gaps must retain holds and block rather than
   pretending the system can resume every crash cut.
4. Existing Provider intents are never resent under a fresh request ID,
   changed attempt, changed generation or another process. Unknown effect or
   missing stop/response/usage proof is a reconciliation boundary.
5. Success means real stored artifact bytes, independently verified under the
   frozen contract, durable proof, and Scheduler-owned Attempt terminal and
   settlement. Merely starting a process or returning `prepared` is not success.

This is the first useful general text-Worker executor, not an arbitrary
model/tool agent loop, a semantic verifier, full P3, or online Provider
acceptance. Production delivery/charge authority and the broader lifecycle
matrix remain explicit full-P3 gates. Existing later same-model retry versus
Router exhaustion mismatch remains safely blocked; do not bypass it.

## State and trust boundaries

- Worker is untrusted, isolated read-only/no network, no secrets, SQLite,
  Scheduler, host filesystem paths or grants beyond exact bounded input.
- Trusted host derives context, route, reservation, frozen config/registry and
  stable call identity from current durable admission, not Worker assertions.
- Constructor/startup/inventory never launches Worker or contacts Provider.
  Startup validates the new journal and may apply existing settlement proofs
  as before; execution/recovery are explicit ready-gated calls only.
- EventStore stores identifiers, phase, hashes, counts, exact context and host
  stop metadata only. No task text, raw Provider request/response, credentials
  or candidate bytes in DB/WAL, exceptions, repr, log or test names.
- Exact input text is re-supplied and checked against existing commitment.
  Validated response/candidate bytes may live only in explicitly configured
  private ArtifactStore content, never a new ad-hoc plaintext journal.
- The new service rejects private SQLite/ArtifactStore/owner-lease roots that
  resolve inside either readable workspace or trusted runtime-source tree.
  Also reject roots within the bootstrap's ambient read grants (`/usr`,
  canonical `/bin`/`/lib`/`/lib64`, `/etc` and `/dev`); merely being outside
  workspace/runtime is insufficient when Landlock deliberately grants those.
  Validate this host storage layout before any launch, outside SQLite locks;
  keep state outside those readable trees rather than relying on a private
  directory name. Legacy callers remain unchanged. Real isolation tests must
  show that out-of-tree private state cannot be read by the child.
- In the new general-Worker authority paths, host callbacks, Gateway,
  filesystem, systemd and clocks with caller code never run inside SQLite
  checked callbacks. Use owned concrete validation and trusted stdlib UTC
  sampled after authority-lock acquisition. Existing legacy/bootstrap artifact
  recovery still performs its prior file verification; this design does not
  retroactively label that existing path callback/IO-free or refactor it all.

Existing Scheduler active/binding resolution calls full Run recovery, which
reads ArtifactStore files. Add an owned metadata-only control projection and
internal Scheduler active/verification binding resolvers sharing the same
lifecycle/agent/budget/effect consistency algorithm. The control projection
has an explicit type that does not claim artifact-byte verification; never
fake an ArtifactStore or silently set a public skip-verification flag. New
general preparation, dispatch, continuation and terminal locked checks use
that path. WorkerInputService gains an owned general preparation entry using
the same exact-input validator and locked stdlib UTC. Existing non-general
resolution keeps full recovery. AcceptanceCoordinator detects a registered
general operation itself and uses the control binding, then independently
reads/verifies artifacts outside the terminal transaction as before; callers
cannot select a weaker resolver. Scheduler's direct general terminal entry
also verifies required private artifact bytes before acquiring its own checked
transaction, then atomically rechecks exact metadata/proposal/stop bindings.
It rejects an ambient caller transaction before doing that file preflight;
new general APIs never nest file verification inside a held SQLite lock.

## Components

### 1. Pure bounded turn protocol and new fixed Worker

`general_worker_protocol.py` defines versioned exact-shape messages, bounded
decode/encode and detached literal errors. One 1 MiB frame, task output capped
at min(existing task output limit, 16,384 bytes), no duplicate JSON keys,
nonfinite values, unknown fields, unsafe constructed/subclass data or stale
AttemptContext. A host-owned turn number/launch epoch binds each proposal.

`general_worker_process.py` is a fixed stdlib child, staged through existing
SystemdReadOnlyLauncher. Turn `request` consumes WorkerTask and emits a
non-authoritative inference request bound to context/input commitment. Turn
`complete` consumes that task plus the host's validated terminal text response
and emits the existing maestro.worker-output.v1 candidate envelope with real
blob bytes. It never calls a Provider, writes a path, claims stop, settles
usage or emits durable success. Nonterminal/tool-call/empty/oversize responses
are rejected, not silently flattened into a successful answer.

Host accepts candidate bytes only via existing decode_worker_output and exact
context validation. The old IsolatedWorkerProcess contract remains unchanged.

### 2. Durable single-owner execution journal

`general_worker_journal.py` uses the SAME SQLiteEventStore as Scheduler and
ProviderCallJournal. Stable operation identity includes Run/Node/Attempt/
generation/input commitment; immutable config/registry/route/workspace binding
must match on every replay. Event version, causation, phase transitions,
unique launch epochs and capped stream size are validated fail-closed.
Each operation also binds a host-scope hash derived outside DB locks from
trusted Linux boot identity, effective user identity and the concrete SQLite
file authority (canonical-path commitment plus device/inode). The new runtime
requires a regular file-backed DB and rejects hardlinked DB aliases, keeping
one canonical ownership-lease authority; in-memory DB execution is unsupported.
Copying into another DB file/path even within the same boot changes this
authority. Restoring/rewinding a journal in place is outside this recovery
contract and requires separate restore authority. This slice resumes
within the same host boot only. A copied database or changed host/boot scope
blocks before credentials, Worker launch or terminal acceptance; missing
local units cannot prove that a different host's old Worker stopped. OS-boot
and host migration resolution remain future authority work. Store the scope
commitment only, not raw host identity; process-local ownership is still not
a stop proof.

Phases: `requested -> worker_requested -> worker_stopped -> call_reserved ->
response_captured -> completion_requested -> completion_stopped -> proposed ->
accepted`. Failure/unknown states retain exact identities, holds and evidence.
No caller can supply a success flag, usage or a free-form continuation cursor.
Repeated completion returns the same durable artifact/proof without dispatch.

Use a trusted private per-operation advisory lease to serialize active hosts;
lease is not settlement or stop proof. A lost owner never authorizes launch
until current phase, Worker stop and Provider state have been checked. Each
checked claim revalidates running/latest/active/unexpired context and all
same-node unresolved Provider/effect holds, not just the current call.
Eligibility is phase-specific: request launch and dispatch admit NO unsettled
call. Completion launch, verification and acceptance may consume exactly this
operation's known_success call while its reservation remains reserved, only
when its immutable call/route/request/reservation/context bindings, TokenUsage,
sender termination receipt and privately stored response capture all agree.
This exception never authorizes invoke or credentials, never exempts a second
call on that node, and never exempts workspace/effect holds. Reuse the existing
hold definitions but add a concrete exact-call continuation predicate; do not
remove every known_success node from the hold set or settle its budget early.

### 3. Real launch identity and stop proof

Extend the owned launcher with a prepared launch binding: unique host-issued
maestro-attempt UUID unit/cgroup intent is durable BEFORE Popen; exact launch
ack is durable only AFTER verified systemd identity/profile observation. The
default launcher API/behavior is preserved. Child cannot choose the unit.
Never reuse unit names across launches or derive them solely from caller data.

The frozen workspace identity reuses the existing canonical path/device/inode
binding. Forward it to the actual launcher snapshot boundary and reject a
directory replacement or move after precheck, before Popen. It is not a tree
content commitment: same-directory byte edits do not change this identity.
Existing bounded no-follow snapshot checks remain; no new content-admission
schema or promise of immutable workspace contents is introduced.

Worker stop is host-observed inactive/failed AND empty exact cgroup, with
unit identity tied to journal launch epoch, not process returncode, a child
message or Provider sender receipt. During normal wait, cancellation, timeout,
lease expiry/loss and output overflow, kill the whole unit and require stop
proof. Missing proof means outcome_unknown and no successor/release/success.

Explicit recovery of a verified launched orphan first stops that exact owned
unit and proves emptiness before relaunch. A `worker_requested` intent without
verified launch ack is an ambiguous-start gap: do NOT treat a temporarily
not-found unit as non-start or automatically retry; return launch_unknown,
retain occupancy and require authoritative host resolution. Tests pin this
conservative boundary. Do not fabricate persistence of a process-local receipt.

### 4. Host mediation and response checkpoint

`GeneralWorkerExecutionService` is composed with the application's concrete
Scheduler, input service, ArtifactStore, grants, existing verifier/proposal
journal, Provider journal and trusted host configuration. Explicit async
execute/recover methods take identity and exact task_text only. Dependencies
must share the same durable store/authority; no Worker-supplied route or URL.

Async execution keeps SQLite, ArtifactStore metadata/grant reads and final
acceptance on the owning thread. The verifier separates preparation from its
DB-free staged-byte/process supervision, which runs outside the event loop.
Only immutable bounded validated data crosses that thread boundary, never a
store, connection or grant callback. Owned synchronous/asynchronous acceptance
share preparation, evidence validation and terminal commit; both independently
run the fixed fresh verifier and trusted usage, without a public skip flag or
prevalidated-proof capability. Existing synchronous/non-general APIs remain.

After the request Worker has stopped, host derives ONE stable ModelRequest
from accepted routing and frozen limits/snapshots and records `call_reserved`
before invoking the existing ProviderModelGateway. Preserve the Gateway's own
intent-before-send and no-existing-intent guard; do not create a retrying
Gateway or accept an unaudited injected callable as production authority.

Add an owned SchedulerProviderDispatchAuthority, sharing this exact concrete
Scheduler, journal and operation store. It supplies the route verifier and a
checked intent claim used by ProviderModelGateway AFTER credential acquisition
and request encoding. In one append_checked transaction it revalidates current
running/latest/active/unexpired authority, frozen request bindings, operation
phase, request-Worker stop proof and every same-node hold, then appends the
Provider intent (dispatching). No await, broker, transport or caller callback
runs under that lock. The committed intent is the send authorization point:
pause/cancel/fencing/expiry committed before it prevents sending; a later
withdrawal can cancel an already-authorized in-flight operation and blocks
acceptance, but cannot retroactively revoke delivery or release held usage.
The journal's same-request single-writer claim remains mandatory. The service
requires this owned authority; the Gateway's existing optional integration
preserves other callers, and a plain is_accepted callback is insufficient here.
Gateway also detects a registered general operation in its concrete same-store
journal before invoking a route verifier/broker: that operation requires the
owned dispatch authority even through the existing Gateway API. Omission or
foreign authority cannot fall through to the legacy path. Non-general calls
retain their prior behavior.

Before ANY invoke on reopen, inspect all same-node Provider/effect history.
Any dispatching/unknown/unrelated-unsettled outcome blocks credentials,
transport and new Worker activity; the exact captured known-success exception
above allows completion work only. A reserved call with no durable intent after
owner loss is still ambiguous at this layer: fail closed rather than race an
old owner into a second dispatch. No silent request-ID/attempt rotation.

After invoke, require a matching known_success journal record and validated
response binding/usage. Store canonical bounded response bytes privately by
digest, then commit `response_captured` metadata bound to the exact call and
artifact. Reopen loads it through scoped verified ArtifactStore access and
does NOT invoke again. If Provider known_success exists but response capture
was lost, return response_checkpoint_missing, retain holds; known delivery is
not a license to regenerate output. Response mismatch/corruption is blocked.

An explicit no-effect Provider proof may eventually authorize a Scheduler
recovery plan under existing authority, but this executor does not manufacture
that proof, clear a journal or silently re-plan from unknown state.

### 5. Candidate acceptance and completion

After completion Worker stop, decode/validate output, publish only attested
bytes to ArtifactStore, derive VerificationTask from frozen Scheduler binding,
run the existing isolated verifier, record VerifierProposalJournal evidence,
then use existing AttemptExecutionCoordinator/Scheduler success gates. Artifact
checks prove only requested integrity/format checks, not semantic correctness.
Missing trusted usage or unavailable production authority remains blocked.

No new caller-supplied zero-cost success path. Test-only deterministic usage
authority and Provider fixtures are explicitly labeled offline. Production
default UnavailableAttemptUsageSource stays unavailable. Scheduler gains the
same optional constructor-owned trusted usage service; Application resolves
one service object and wires Scheduler and Coordinator consistently. Shared
protocol/default/error definitions live in a cycle-free module with old import
aliases preserved. A general direct terminal call samples that source outside
transactions and rejects supplied UsageRecord mismatches and captured Provider
token mismatches. Coordinator's general path delegates to the same owned
Scheduler preflight/commit with no caller usage or prevalidated-proof token,
so the trusted service is sampled once. General Scheduler preflight also runs
one NEW owned concrete isolated verifier and compares its validated evidence
hash with the durable proposal: byte integrity alone cannot authenticate an
untrusted claimed accepted proof or its frozen format checks. Application
supplies Scheduler the same trusted artifact-grant authority used by its
ArtifactStore/Coordinator; missing authority blocks. A shared out-of-lock
proposal-verification helper avoids duplicated logic; general Coordinator
delegation avoids launching a second fresh verifier. No public skip flag or
prevalidated-proof token is added. Non-general paths retain their prior
contract. General current-time authority is standard-library UTC after locking,
so a caller completion timestamp cannot revive an expired/fenced Attempt.
Private artifact
publication (ArtifactPublished) is an earlier durable prerequisite, not part
of the terminal transaction and not workspace publication. Its bytes and
provenance are reverified before acceptance; unaccepted private bytes confer
no success or release authority.

Extend Scheduler.finish_verified_attempt so an operation for this Attempt
must have exact completion-Worker stop, capture and proposal bindings before
success, even when reached through the existing public acceptance API. Inside
the SAME SQLite terminal transaction append the journal's accepted checkpoint
with immutable task/evidence/artifact/usage hashes, settlement key and exact
completed_at, alongside AttemptSucceeded, reservation commit and slot release.
Use concrete owned journal methods, not injected callbacks under the lock.
Any exception rolls every terminal write back. Bootstrap validates both sides
of these references; a general operation's historical success missing the
paired accepted record is corruption, not a synthesized replay.

Accepted replay reads and validates that historical checkpoint and matching
Scheduler/proposal/settlement evidence before resolving an active lease or
sampling usage/time; it returns the exact durable result without a second
acceptance/invoke. Thus a kill after terminal commit but before API reply
does not need the released active binding. A kill after proposal but before
terminal commit still requires current authority. Existing terminal gates for
failed outcomes also prohibit releasing a general operation with an unproved
Worker stop, including finish_attempt and reconcile_attempt and startup
settlement paths; a Provider sender receipt cannot discharge this gate.
outcome_unknown keeps its holds. Cancellation/fencing/expiry
committed before acceptance wins. Existing non-general Attempts are unchanged.

## Recovery truth table

| Durable cut after real kill | Explicit restart behavior |
| --- | --- |
| Before request/claim | Fresh current-authority claim; no prior effects |
| Prepared unit intent, launch ack absent | launch_unknown; no relaunch or released holds |
| Verified request Worker running | Stop exact old unit, prove empty, fresh epoch; one owner |
| Request Worker stopped, no call reservation | Resume host mediation under current authority |
| Call reserved / intent dispatching / outcome unknown | reconciliation_required; zero resend/credential/Worker activity |
| Known success but response checkpoint absent | response_checkpoint_missing; no resend |
| Captured response, completion Worker killed | Prove old Worker stopped, reload response, render/verify/accept without invoke |
| Candidate/proof durable | Revalidate current authority; idempotently accept existing proof |
| Atomic accepted + terminal committed, reply lost | Validate paired historical records and return exact result; no active lease/new usage/time/invoke |

Success-path priority test kills the completion Worker with actual SIGKILL
after the host has captured a real bounded response, then starts a genuinely
new application process with the same SQLite/ArtifactStore, re-supplies input
and proves exact output bytes + independently verified AttemptSucceeded.
Provider invocation count across both processes stays ONE. A second scenario
kills the host after a verified request-Worker launch, recovers the owned unit
and completes. Unknown-call scenarios use actual journal states and guard
every credential/Gateway/transport boundary; count remains zero on restart.

## Acceptance and rollout

Separate reviewable tasks: (0) design/reviewer/plan/preflight, (1) bounded
protocol + useful isolated Worker, (2) host mediation/journal/stop/capture/
acceptance composition, (3) explicit restart recovery and application gates,
(4) independent-process SIGKILL/contention/matrix/final slice acceptance.
Every task has at least three substantive ordinary feature pushes and an
independent spec/quality gate; whole-slice final review is separate. All source
changes follow TDD; existing-behavior characterizations are labeled honestly.

Final full suite and statement coverage >=90%, required live isolation zero
skips, compileall/pip check, private wheel/module inventory, source hashes,
privacy scans and retained failures are mandatory. Test cancellation, pause,
expiry while waiting, newer generation, corrupt checkpoints, oversized/hostile
IPC, missing usage, unknown stop, two competing recovered hosts, lost reply,
Provider request ID rotation and response replay. Real test processes/services
must have bounded waits and be reaped; no harness PID/receipt fakes substitute
for the primary SIGKILL/stop tests.

Daily reviewed completion count starts at 1/5 on LA 2026-10-24. New Task0
counts after design/plan review and its third substantive push. Once daily
count reaches5, preserve progress and wait for next LA date; never bypass the
quota floor (remaining must exceed50%). Root owns pushes and quota checks.
Only this design's corresponding .superpowers/sdd ledger is the recovery map.
Do not read sibling-plan scratch, repeat the completed prerequisite or delete
any evidence. Main integration is not authorized for this new work.

## Builder decisions, not human assertions

- First useful executor is terminal-text model work, not arbitrary tools:
  provides verifiable useful output without executing deferred P5 publication;
  cost is that tool-demanding tasks explicitly block instead of completing.
- Use bounded restartable pure child turns, not long-lived bidirectional
  sockets: existing isolation can supervise each turn; cost is extra process
  launches and conservative ambiguity gaps, not claimed universal liveness.
- Private response capture enables no-resend continuation after Worker death;
  cost is sensitive output retention in the explicitly configured private
  ArtifactStore and a blocked gap if capture did not commit.
- No paid/credentialed Provider acceptance is executed under this authority;
  offline transport evidence is labeled offline and cannot close that P3 gate.
