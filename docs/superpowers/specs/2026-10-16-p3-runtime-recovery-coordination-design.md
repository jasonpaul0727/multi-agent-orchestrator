# P3 runtime recovery coordination — approved design

Status: explicitly approved by the human on 2026-10-23, with instructions to
write the implementation plan and continue development. This is not completed
P3. The historical starting point below is retained; implementation starts from
`621cacd7931725ef7c7bf97132192925bc32e0f8` after the separate P0 closure.
P0 added a fixed tool-only workspace Worker/Gateway path, but no general model
Worker or automatic recovery-plan execution. That does not expand this slice.

## Intent and authority

The human approved prioritizing P3 over the deferred P5 candidate publisher,
with work starting no earlier than 2026-10-16 01:10 America/Los_Angeles.
The intended demonstration is restart after interruption, continuation of
unambiguously recoverable work, and no automatic resend of an unknown Provider
call. P3 completion additionally needs whole-Worker termination coordination,
the lifecycle interruption matrix, and authoritative Provider reconciliation.

This proposal chooses a prerequisite slice: durable recovery-to-admission
coordination. It does NOT redefine that full demonstration or P3 as complete.
The choice to build this slice before functional Worker mediation is the
builder's recommendation, not an already approved human design decision.

## Verified starting point

Source base: ae12d40458e6c8cf41c925ca52cf01a5c965098f on the existing isolated
codex/p3-systemd-termination-receipts branch. The base contains the reviewed
P5 publication PLAN, not its implementation. Do not execute that plan here.

- Scheduler.plan_attempt_recovery persists classified failures and bounded
  RecoveryPlanCreated records after durable failure/unknown state.
- Scheduler.accept_routing consumes persisted recovery authorization once,
  and owns budget, Agent, concurrency, fencing and lifecycle admission.
- ControlPlaneApplication startup validates/rebuilds durable state and applies
  persisted Provider settlement proofs atomically, without starting work.
- WorkerInputService commits a canonical input hash; task text is not stored.
  Explicit re-supply prepares a bounded WorkerTask under current authority.
- IsolatedWorkerProcess is intentionally blocked-only. It is not a functional
  model/tool executor. The output decoder is pure; candidate publication is
  still absent. Neither component can truthfully demonstrate resumed useful
  Agent execution yet.
- Provider journal replay blocks unresolved calls. Gateway sender stop proof
  is not proof that an entire Worker stopped or that Provider cost is known.

Authoritative existing context: the original V1 P3 checklist in
docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md;
docs/security/run-recovery.md; docs/security/worker-runtime.md;
src/orchestrator/application.py; src/orchestrator/scheduler/core.py;
src/orchestrator/routing/recovery.py;
src/orchestrator/runtime/worker_input_service.py.

## Goals of the prerequisite slice

1. Enumerate durable, unconsumed same-node recovery opportunities after reopen,
   with exact source Attempt/generation, decision and evidence identities.
2. Let an explicit trusted host continuation call supply the original task
   text and fresh routing observations. Derive retry counters and authorization
   from history, not caller assertions; bind input equality to source admission.
3. Atomically accept at most one successor per persisted authorization through
   the existing Scheduler. Repeated calls/restarts return the same durable
   successor identity rather than spending another authorization or budget.
4. Prepare the successor WorkerTask only under fresh active authority, using
   the existing input service; return data to the host without implying launch.
5. Preserve all unresolved outcome/resource holds and refuse unknown-call
   replay, including after competing controllers or ambiguous response loss.

## Non-goals and anti-goals

No functional Worker implementation, model/tool execution, automatic OS process
launch, candidate publication, success acceptance, CLI/MCP, new storage backend,
or production Provider authority is delivered by this prerequisite slice.
No persistent task-text store, credential access, paid calls, shared dependency
installation, main merge, PR, deployment or destructive cleanup is authorized.
Never mark a blocked-only IPC roundtrip or injected executor as functional
Worker recovery. Never convert uncertainty into zero usage or a no-effect claim.

## Host interaction and restart semantics

Construction remains recovery-only. An explicit ready-gated application API
lists continuation opportunities and their reasons: ready for input/routing,
already admitted, reconciliation required, new-child planning required, or
blocked. Exact public names and model types belong in the implementation plan.

The first implementation accepts only existing same-node authorized actions
(same-model retry, same-tier fallback, capability escalation). New-role/repair
nodes are reported, never fabricated. Each continuation handles one opportunity;
there is no unbounded retry loop or background polling in an application object.

Host re-supply is deliberate: durable input hashes cannot reconstruct task
text. Missing input is a waiting condition, not an invalid recovered Run.
The host can re-supply after restart, or a later separately reviewed trusted
input source can automate that. Do not introduce plaintext persistence merely
to make the demonstration appear automatic. The complete future demonstration
must disclose this requirement.

Fresh routing facts include the existing policy, health, costing and eligibility
inputs; their trusted composition sources and validation must be enumerated in
the plan. Stale facts must fail existing admission checks. A caller cannot
choose a different role, recovery evidence, exhausted-model counters, source
generation, or permission level. No host callback runs inside a database lock.

## Durable ownership and atomicity

Use existing event-store transactions and Scheduler authority, not an in-memory
lock as interprocess ownership. A deterministic successor identity is derived
from the source authorization and domain-separated identity, with collision and
binding validation on replay. The exact identity encoding is left to the plan.
Do not include prompt text in that identity or in any new event/log field.

One checked transaction must resolve the latest source, verify its original
input commitment and frozen contract, reject cancellation/unknown/inconsistent
state, route/admit through Scheduler and bind the chosen successor. If an extra
link event is needed, it shares the admission transaction. No partial durable
link, authorization consumption, reservation, Agent or slot may remain after
rollback. Existing Scheduler events are authoritative; avoid a second competing
state machine. Do not relax existing admission or proof-gated success APIs.

After commit but lost response, replay resolves the committed successor. A
successor that has already run/expired/ended is reported as such; replay cannot
prepare stale task data or create another successor from the old authorization.
Task preparation after admission is a separate validity point. Failure there
does not pretend the committed admission rolled back, and must not double-admit.
No launch is attempted by this slice, so a returned task is not execution proof.

## Unknown and side-effect boundaries

An active/expired/unknown source cannot be declared safely failed by this API.
Unresolved Provider calls and external effects retain existing recovery gates,
reservations and slots. Admission must inspect durable current state, not merely
trust a historically authorized plan. Reconciliation remains explicit and
proof-gated. Provider journal protection must remain effective even if a caller
tries a new successor identity to bypass same-call replay rejection.

Cancellation wins when committed before continuation admission. If admission
wins first, cancellation must still prevent subsequent task preparation under
the existing authority gates. This slice never releases a running Worker slot:
whole-process termination evidence is a later P3 prerequisite.

## Evidence and test gates

Use TDD and independent task-level specification and quality review. Tests must
cover actual SQLite state, not only mocked Scheduler calls:

- restart discovers an authorized known-failure plan and re-supply binds exactly;
- absent/changed input, malformed identities, unsafe copied models and corrupt
  historical events fail without leaking task text or validation internals;
- two independent connections/processes racing continuation yield one successor,
  one consumed authorization, one reservation, one Agent and one slot;
- parent-issued process death before outer commit rolls back everything; death
  after commit/lost reply reopens to exactly the committed successor;
- pause/cancel, expired/newer successor, revoked/stale routing facts, exhausted
  retries/budget/Agent/depth/concurrency gates are not bypassed;
- unknown Provider/effect outcomes never produce successor admission or calls;
  journal/transport/Broker spies show zero activity for blocked continuation;
- preparation failure after successful admission remains recoverable without
  duplicate admission; no task text appears in events or diagnostic surfaces.

The plan must name concrete fixtures, durable boundaries, public errors and
exact test commands before implementation. Final slice verification includes
full suite and at least 90% total coverage, required live isolation zero skips,
compilation/dependency/package checks proportionate to changed code, and review
of the tested committed range before an ordinary push of the existing branch.
Historical 2292 tests / 90.49% are not fresh results for this proposal.

## Remaining route to P3, not hidden inside this slice

After this slice, the original requested useful recovery demonstration still
needs a separately described functional Worker/host Gateway mediation path,
whole-Worker stop binding, and runtime dispatch crash semantics. Successful
Worker output cannot become Node success without the existing verified
acceptance chain and a reviewed publication producer; bypassing it is forbidden.
That dependency must be resolved explicitly rather than silently executing the
deferred P5 publisher plan or calling fake execution a production success.

Authoritative production Provider usage/receipt verification and its online
acceptance remain a distinct full-P3 gate. Ask for human direction before any
real credential or paid request. Offline/loopback tests never satisfy it.

## Approval recorded

The human approved delivering durable recovery-to-admission coordination first
on 2026-10-23, preserving explicit input re-supply and no execution at startup,
and requested implementation planning followed by development. Functional
Worker/publication dependencies and the full recovery demonstration remain the
later P3 work described above.
