# P5 Attempt Execution Coordinator and Verified Acceptance Design

**Status:** Proposed architecture; the conversational direction is approved, but this written specification requires user review before an implementation plan is written
**Date:** 2026-10-07
**Target:** Connect an accepted Scheduler Attempt to an isolated Worker proposal, host-published candidate artifacts, an independent isolated Verifier, durable evidence, and a proof-gated control-plane terminal decision

## Goal

Add one control-plane-owned execution path that turns an accepted Attempt into either a safely held/failed result or a success backed by exact, independently verified candidate evidence. Worker and Verifier messages remain untrusted. Only the trusted host control plane may publish candidate bytes, accept evidence, settle the Attempt, and release its Scheduler lease.

This is a staged P5 design, not a claim that the current Worker is functional. The first implementation slice closes the acceptance and decision path using bounded test adapters and already host-published fixtures. The existing `IsolatedWorkerProcess` remains blocked-only until a later slice implements real task execution and candidate publication. Production success must remain unreachable while the production Worker cannot produce a candidate.

## Current boundary

- `ControlPlaneApplication` composes startup recovery, Scheduler, ArtifactStore, and Provider-call reconciliation, but has no execution coordinator.
- `IsolatedWorkerProcess.execute()` returns only a `blocked` proposal and rejects candidate output.
- `WorkerResult` contains artifact references, not candidate bytes. No trusted candidate-byte publisher is connected to the Worker IPC path.
- `admit_candidate_artifacts()` verifies that ArtifactStore records carry the exact Run/Node/Attempt/generation/Agent provenance.
- `IsolatedVerifierProcess` re-reads those bytes under read-only grants and produces bounded deterministic evidence; it is not a semantic reviewer or project test runner.
- `VerifierProposalJournal` durably records validated proposals but does not accept a Node or affect Scheduler state.
- `Scheduler.finish_attempt(outcome="succeeded")` currently has no verifier-proof parameter. Because it accepts a known terminal usage settlement, callers can currently mark lifecycle success without evidence.

## Design decisions

### 1. One coordinator owns the execution sequence

Add a host-only `AttemptExecutionCoordinator`, composed by `ControlPlaneApplication` from the same SQLite connection, Scheduler, ArtifactStore, Worker runtime, Verifier runtime, and proposal journal. It owns sequencing and failure classification; it does not own a second lifecycle or budget implementation.

The application-facing entry point identifies a durable accepted Attempt. The coordinator resolves the accepted route, active lease, Agent instance, current fencing generation, and frozen Run snapshots from durable control-plane state. It must not accept caller-supplied `WorkerResult`, `VerificationEvidence`, lifecycle outcome, reservation ID, or a self-asserted no-effect claim as authority. A returned `AcceptedAttempt` object is a convenience value, not proof by itself; the coordinator rechecks it against Scheduler/EventStore state before dispatch and before committing a terminal result.

Construct `WorkerTask` only from that resolved Attempt and the frozen graph/input/config/Registry/policy/routing/planning context. Keep EventStore, Scheduler, ArtifactStore handles, filesystem paths, secret material, and ambient credentials out of Worker task data.

### 2. Candidate publication is a trusted host operation

For a candidate result, first strictly revalidate the exact `AttemptContext`, outcome shape, output limits, and non-reuse of input digests. Worker-supplied paths are never accepted.

The eventual functional Worker slice must define a bounded candidate-byte transport and a host `CandidateArtifactPublisher`. The publisher, not Worker code, writes bytes to ArtifactStore with immutable source metadata for the exact Run/Node/Attempt/fencing generation/Agent. Only after host publication may `admit_candidate_artifacts()` compare each opaque Worker reference to a fresh verified ArtifactStore inventory. A missing or mismatched publication fails closed.

The first control-acceptance slice may use test-only publisher/runtime adapters and pre-published fixtures to exercise this path. Such tests prove coordinator behavior only; they do not make the production Worker functional or permit a public API to inject fake results.

### 3. Verification is independent and narrow

Create a `VerificationTask` from the admitted, exact candidate set and the same immutable Attempt context. Run `IsolatedVerifierProcess` read-only over those host-published bytes. Revalidate returned evidence and require the fixed supported verifier/contract, exact candidate digests, exact required-check set, and matching Attempt context.

Only `outcome == "accepted"` with every required check passing can enter the verified-success path. `rejected` and `inconclusive` evidence never means success. The built-in checks remain limited to byte integrity and supported deterministic formats; they do not establish semantic correctness, run the project test suite, or substitute for Final Review.

### 4. Durable proposal precedes, but does not itself perform, acceptance

Record the exact validated task/evidence pair through `VerifierProposalJournal` before the final Scheduler decision. The proposal journal remains append-only and non-authoritative. A proposal may therefore survive a crash even when the Attempt was not accepted.

The Scheduler's proof-gated terminal operation must re-read and validate the exact durable proposal, match it to the still-active accepted route/lease and frozen Attempt context, and include its proposal/task/evidence digests in the durable terminal event. A caller-provided hash alone is insufficient. Replays verify that every successful Attempt has exactly one matching accepted proposal and reject missing, conflicting, stale, or mismatched proof; replay never promotes a proposal to success by itself.

The acceptance event and existing terminal bookkeeping—lifecycle transition, budget settlement, Agent completion, and Scheduler slot release—must commit atomically using the existing SQLite `append_checked` transaction/savepoint mechanism. If that invariant cannot be implemented without weakening EventStore ownership or adding a second source of truth, stop and revise this spec rather than splitting success across transactions.

### 5. Every success-authorizing write path is proof-gated

Add a distinct Scheduler operation for verified success (or an equivalent mandatory typed proof parameter) and make ordinary `finish_attempt(outcome="succeeded")` and `reconcile_attempt(outcome="succeeded")` fail closed without that proof. Lifecycle success-writing methods must likewise be internal to the Scheduler transaction or require the exact proof reference; callers must not be able to write `AttemptCompleted`/`AttemptReconciled` with `succeeded` directly. This protects against future application callers bypassing `AttemptExecutionCoordinator`.

The proof must bind at least: Run, Node, Attempt, fencing generation, graph version, input manifest, effective-config hash, Registry hash, policy/routing/planning hashes, exact candidate artifact digests, verifier/contract/check-set identity, evidence digest, and the accepted route/reservation identity. The Scheduler must resolve those values against authoritative durable records and reject stale leases, expired results, cancellation races, and duplicate/conflicting idempotency keys.

Rejected evidence is not success. A terminal failure may release the lease only with valid usage settlement or an independently validated no-effect receipt under existing Scheduler rules. If a Worker/Provider outcome is ambiguous or the required settlement evidence is unavailable, retain the Attempt/budget hold as unknown for explicit reconciliation; do not infer zero usage from a local process exit.

### 6. Recovery is explicit and non-promoting

On restart, `ControlPlaneApplication` validates the proposal stream and any terminal proof references before exposing readiness. A proposal without a matching terminal acceptance remains only a proposal; startup does not re-run the Worker or Verifier and does not mark the Node succeeded.

The initial recovery behavior for a crash after proposal persistence but before terminal acceptance is to keep the Attempt held/awaiting explicit reconciliation or a future idempotent coordinator resume operation. Any resume operation must re-check the durable active lease, cancellation state, expiry, ArtifactStore bytes/provenance, and exact proposal before it can commit success. Automatic re-execution and automatic promotion are out of scope for this slice.

## Execution and state sequence

1. The caller requests work through `ControlPlaneApplication`; the coordinator resolves a current durable accepted Attempt.
2. The isolated Worker receives only bounded task/context data. A blocked/failed/transport-unknown result cannot become success.
3. For `candidate`, the host publisher stores bytes with exact Attempt provenance; candidate admission verifies the published objects.
4. The isolated Verifier inspects the exact published bytes and returns an Attempt-bound proposal.
5. The host revalidates and records the proposal in `VerifierProposalJournal`.
6. The proof-gated Scheduler operation atomically records acceptance, settles known usage, transitions lifecycle/Agent projections, and releases the lease; rejected/unknown cases follow explicit failure/reconciliation rules.
7. Startup replay verifies proof references and reconstructs state, but never authorizes work from projections or proposals alone.

## Security and consistency invariants

- Worker, Verifier, Tool, and candidate code never receive EventStore/Scheduler handles or write lifecycle, Registry, budget, or Scheduler streams.
- Worker output and Verifier evidence are untrusted proposals until exact schema, provenance, and current durable Attempt state are rechecked by the host.
- Artifact paths are host-generated; ArtifactStore publication metadata is the provenance authority.
- No Provider secret, raw credential, environment dump, arbitrary exception text, or local protected path enters IPC, proposal events, acceptance events, or logs.
- No Worker or Verifier cancellation/timeout is treated as confirmed process termination unless the isolation runtime returns its existing termination proof.
- One stale fencing generation, cancelled Run, expired lease, unavailable audit/EventStore, missing artifact, or conflicting idempotency key fails closed and cannot release or succeed the Attempt.
- A local SQLite/hash-consistent proposal is not cryptographic proof against a compromised same-UID host process; this design preserves the repository's existing single-host trust model.

## Failure, concurrency, and crash cases

Tests must cover at minimum:

- duplicate coordinator requests for one accepted Attempt, with a single terminal decision and no duplicate budget settlement;
- two simultaneous candidate completions racing on the same fencing generation;
- cancellation and lease expiry racing Worker return, publication, verification, and final acceptance;
- Worker result bound to another Attempt/Agent/context, duplicated JSON keys, malformed/oversized frames, and input-artifact relabeling;
- candidate reference absent from ArtifactStore, wrong source provenance, wrong type/size/media type, changed bytes, and no-follow publisher failures;
- Verifier wrong context/check set/digest, rejected/inconclusive result, malformed output, timeout, cancellation, and unconfirmed termination;
- failure injected after candidate publication, after proposal append, during atomic success settlement, and after commit but before response;
- fresh-process restart at each durable boundary, proving no proposal-only success and exact idempotent replay after an already committed success;
- direct attempt completion cannot record `succeeded` without accepted verifier proof; tampered or mismatched proof references fail startup recovery;
- known usage/no-effect settlement versus ambiguous Provider outcome, proving budgets/slots remain held when reconciliation is required.

## Phased delivery

1. **Acceptance foundation:** coordinator composition, strict accepted-Attempt resolution, artifact admission, isolated Verifier invocation, durable proposal use, proof-gated atomic Scheduler success/failure path, restart/concurrency/cancellation tests. Use test-only adapters; production Worker remains blocked-only, and end-to-end product execution is still blocked.
2. **Functional Worker and publisher:** define bounded candidate-byte transport, implement Worker task execution, host publication, and failure/effect receipts without exposing storage or credentials to the Worker. Keep model/tool calls disabled until their trusted gateways are integrated.
3. **Gateway and effect integration:** connect Model/Tool Gateway, Approval, Secret Broker, usage accounting, Provider reconciliation, and cancellation/unknown-outcome handling to the coordinator. No real paid Provider is required for offline acceptance tests.
4. **Product acceptance:** add project tests/semantic review/Final Review, shared CLI/MCP entry points, end-to-end security and recovery acceptance, and measured cost/token/rework benchmarks. Only these later gates can support a V1 delivery claim.

## Non-goals

- Enabling actual Worker task execution in the acceptance-foundation slice.
- Provider/model/tool access, production Secret Broker backend, caller identity provider, or paid Provider requests.
- Treating deterministic format checks as semantic correctness or full project validation.
- Graph expansion/Agent delegation, Final Review, CLI/MCP, workspace-write isolation, or V1 certification.
- Automatic recovery that replays proposals into success or silently re-executes an ambiguous Attempt.

## Acceptance criteria for the first implementation slice

1. No public application API accepts raw Worker/Verifier output as authority; accepted Attempt identity and frozen inputs are resolved/rechecked from durable state.
2. Candidate admission requires exact host ArtifactStore source provenance; the real Worker remains blocked-only until the separate publisher/functional Worker phase.
3. Only validated accepted evidence for the exact active Attempt can authorize success; ordinary finish, reconciliation, and lifecycle success-writing APIs cannot bypass the gate.
4. Acceptance proof and all existing Scheduler/lifecycle/budget/Agent terminal bookkeeping are atomic; every durable success replays with exact proof binding.
5. Proposal-only crash recovery never succeeds a Node; ambiguous usage/effect outcomes retain their hold for reconciliation.
6. The test matrix above covers race, cancellation, stale generation, corruption, process restart, idempotency, and crash boundaries using real SQLite and the measured systemd process boundary where applicable.
7. Full repository acceptance gates still pass, including total coverage at least 90%; no skips may silently replace the required isolation integration tests.
8. Documentation and the P5/V1 implementation plan state plainly that this slice does not make the Worker functional or establish V1 delivery readiness.

## Alternatives considered

- **Put orchestration in `ControlPlaneApplication`:** rejected because it would mix composition/readiness with process sequencing, artifact publication, verification, and terminal decision logic.
- **Let Worker or Verifier write state directly:** rejected because untrusted child output would become authority and would bypass existing Scheduler atomicity, budget, lifecycle, and recovery rules.
- **Implement Worker, Gateway, publisher, acceptance, CLI/MCP, and end-to-end security in one slice:** rejected because it entangles unrelated trust boundaries, makes failure attribution difficult, and cannot be meaningfully validated as one reviewable change.

## Open implementation constraints

- Before implementation, inspect `SQLiteEventStore.append_checked` nested-write and rollback behavior and design the proof reference so the Scheduler terminal event and acceptance record have one atomic commit point.
- Define the candidate-byte IPC framing and host publisher in the functional Worker phase; do not extend `WorkerResult` with filesystem paths or silently place artifact bytes in an unbounded JSON message.
- Preserve existing recovery of historical Run state while ensuring newly recorded success requires proof; any compatibility rule for pre-gate success events must be explicit and fail closed for new writes.
