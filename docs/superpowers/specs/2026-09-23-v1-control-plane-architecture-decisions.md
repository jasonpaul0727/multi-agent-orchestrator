# V1 Control-Plane Architecture Decisions

Date: 2026-09-23

Status: implementation contract for the approved single-user local V1 scope.
This record does not declare a product-supported execution platform or relax
the approved security specification.

## Context

The repository has implemented configuration, policy/routing, SQLite event
storage, lifecycle, Scheduler, Agent Registry, budgets, artifacts, Gateway
primitives, and a measured Linux/systemd read-only command profile. Their
interfaces must converge on one control plane before an isolated Worker,
Verifier, CLI, or MCP entry point is allowed to change durable Run state.

## Decisions

### Identity and execution context

- `run_id` identifies one durable execution aggregate. IDs are opaque bounded
  strings at module boundaries; new IDs are UUIDs unless an aggregate has an
  explicit deterministic derivation rule.
- `node_id` identifies one immutable node identity within a Run. A graph
  version changes only when the control plane accepts a validated append.
- `attempt_id` identifies one execution generation. `fencing_generation` is
  monotonically increasing for a node and is required on accepted routes,
  Worker messages, Gateway requests, receipts, and verifier evidence.
- Every durable execution event carries its available run/node/attempt/generation
  context. Consumers reject mismatched context rather than repair or infer it.

### Event ownership and transaction boundaries

| Authority | Durable event family | Decision boundary |
| --- | --- | --- |
| Lifecycle controller | Run initialization/config snapshot, graph append, start/pause/wait/cancel, Attempt state | Per-Run stream CAS on expected graph/state version |
| Scheduler | Accepted route, terminal/unknown lease, failure classification, recovery plan | Global Scheduler stream CAS; admission rechecks Run, policy, budget, Agent and capacity state |
| Agent Registry | Agent creation and status transitions | Per-Run Registry stream; cumulative count/depth never refunded |
| Budget ledger | Reservation, settlement, release, unknown and reconciliation | Per-Run ledger stream; unknown holds remain reserved until explicit reconciliation |
| Health controller | Provider/model health and probe leases | Aggregate-specific CAS with event-time replay |
| Tool/Approval/Secret services | Policy decision, capability/grant consumption, effect intent/receipt, secret decision | Security/effect streams in the same SQLite event store; failure to audit denies execution |
| Artifact store | Publication metadata keyed by digest | Digest stream and content-addressed blob; bytes are verified against metadata |

SQLite is the current single-host transaction coordinator. Scheduler admission
uses `append_checked` on the global Scheduler stream and nested savepoints for
the lifecycle, Registry, and budget drafts so an Attempt is accepted in one
database transaction. This is not a distributed transaction guarantee; any
future multi-process or remote store must preserve the same atomic decision
boundary or expose an explicit saga with `OutcomeUnknown` semantics.

Event rows are authoritative. Snapshots, in-memory objects, CLI/MCP responses,
metrics, and logs are projections only. Snapshot mismatch falls back to event
replay; projections never authorize work.

### Proposals and accepted decisions

- Graph expansion is an untrusted proposal. A control-plane Graph Manager must
  validate schema, dependencies, graph version, node count/depth, planning
  contract, policy scope, and budget before appending the graph event. Workers
  cannot append graph events or mutate lifecycle state.
- `PolicyDecision` and `RoutingDecision` are immutable decision evidence over a
  frozen Run config/Registry/policy/health snapshot. A route is not executable
  until Scheduler acceptance binds it to one Attempt, fencing generation,
  budget reservation, Agent instance, lease, and event-stream version.
- Tool requests are untrusted typed proposals. Only Tool Gateway may turn a
  request into an effect after rechecking action/target/parameter hashes,
  capability/grant, policy version, fencing, cancellation, isolation profile,
  and audit availability. A denied or stale request must not start a process or
  external action.
- Worker output and `CandidateResult` are untrusted data, never a success event.
  Outputs are size/type checked, redacted as required, stored as candidate
  artifacts with Attempt provenance, then independently verified.
- `VerificationEvidence` must identify the exact node, Attempt generation,
  accepted graph version/input manifest, and artifact digests it inspected.
  Only the control plane may accept evidence and transition node/Run state. A
  Worker cannot verify its own completion; Final Review is a separate frozen
  decision over required outputs and evidence.

Graph Manager, Worker, CandidateResult, VerificationEvidence, and Final Review
application services are contracts to implement; their absence must remain an
explicit blocker, not be filled by a direct Worker-to-EventStore shortcut.

### Errors, unknown outcomes, and retries

- Errors exposed across module/process boundaries use stable error codes and
  safe structured context; raw prompts, credentials, provider bodies, local
  protected paths, and exception strings do not enter durable audit payloads.
- Policy denial, missing/invalid approval, budget exhaustion, stale fencing,
  unsupported isolation, and unavailable dependencies are terminal for the
  requested operation and do not trigger implicit fallback.
- Gateway failures distinguish known preflight/known failure/known success
  settlement failure/unknown transport outcome. Only deterministic persisted
  failure evidence can authorize bounded retries. Unknown provider outcomes,
  external effects, and unconfirmed process termination retain their lease and
  budget until reconciliation.
- Reconciliation is a trusted host-side operation today. Existing tests prove
  SQLite atomicity/idempotency across process death, not the authenticity of a
  provider lookup receipt. No Worker or MCP caller may treat a self-asserted
  result as provider evidence; Provider lookup/signature verification remains
  a P3/P5 implementation requirement.

### Application and platform boundary

- CLI and MCP must invoke the same application services and receive the same
  redacted projections. Neither surface may implement its own scheduler,
  approval, budget, or recovery logic.
- MCP high-risk calls require an authenticated caller and an Authority Envelope
  that binds actor, action, target, expiry, and one-use grant. The current
  injected identity hook is a testable primitive, not a production identity
  provider.
- Ubuntu 24.04 under the measured WSL2/systemd 255 environment is a candidate
  for the opt-in read-only command profile only. No product execution platform
  is supported until workspace-write isolation/publication, Worker/Gateway/
  Approval/Secret wiring, and the complete security acceptance suite pass.
  Unsupported capabilities return a stable blocked result; there is no
  unsandboxed fallback.

## Consequences

This contract preserves the current modular implementation while making the
missing execution path explicit. It prevents interface primitives from being
mistaken for product integration, and it makes crash/unknown states part of the
API rather than an exception-handling detail. P3–P7 remain incomplete until
the contracts above are implemented and exercised through one offline
end-to-end path plus platform-specific negative security tests.

## Required follow-up

1. Add Graph Manager and Worker IPC contracts without giving Worker durable
   store handles.
2. Complete a bounded, no-follow workspace-write Overlay profile, diff
   validation, concurrency fencing, and crash-safe host publication.
3. Connect the audited Secret Broker and ApprovalService to trusted Gateway
   and Worker processes; define production caller identity and provider-result
   reconciliation.
4. Implement independent Verifier/Final Review, then one shared CLI/MCP
   application-service surface.
5. Run offline end-to-end, crash-injection, negative security, packaging, and
   reproducible performance benchmarks before changing the delivery status.
