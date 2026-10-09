# P5 Verified Attempt Acceptance Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a successful Attempt impossible unless its exact active lease is bound to a durable, independently revalidated accepted Verifier proposal, with one atomic Scheduler/lifecycle/budget/Agent decision.

**Architecture:** Add a private `AttemptExecutionCoordinator` that resolves a durable active Attempt, loads a proposal by its task hash, re-runs the isolated Verifier over the exact host-published artifacts, and asks Scheduler to commit proof-bound success. Scheduler and Lifecycle reject all unproved success paths; startup validates every success proof before readiness. This plan is only the acceptance foundation: it does not dispatch a Worker or make the blocked-only production Worker functional.

**Tech Stack:** Python 3.12+, Pydantic v2, SQLite `append_checked`, existing `ArtifactStore`, `IsolatedVerifierProcess`, `VerifierProposalJournal`, systemd read-only isolation, pytest and coverage.py.

**Spec:** [`docs/superpowers/specs/2026-10-07-p5-attempt-execution-coordinator-design.md`](../specs/2026-10-07-p5-attempt-execution-coordinator-design.md)

**Implementation status (2026-10-09):** Tasks 1–5 are implemented, independently reviewed and pushed, including the startup Attempt-policy commitment fix. The acceptance foundation is implemented; Task 6 documentation and measured final gates are tracked below. Fresh whole-slice review, documentation push and remote-tip verification remain controller responsibilities. Production Worker remains blocked-only; broader P5/V1 delivery is incomplete.

## Global Constraints

- Worker and Verifier messages remain untrusted; only trusted control-plane code may change durable Run state.
- The production `IsolatedWorkerProcess` remains blocked-only in this slice; tests may use internal fake adapters and host-published fixtures, but no public API may accept raw `WorkerResult` or `VerificationEvidence` as authority.
- Candidate artifacts must be content-verified and bound to exact Run/Node/Attempt/fencing generation/Agent provenance; Worker-supplied paths are forbidden.
- Only `builtin.readonly-v1` with `maestro.artifact-verification/v1` and the supported deterministic check set can authorize this success path.
- Proposal persistence does not itself accept a Node; final proof, lifecycle, Agent, budget, and Scheduler-slot writes use the existing SQLite `append_checked` transaction.
- Do not add database tables, schema migrations, Provider calls, production Secret Broker backends, or new runtime dependencies.
- `NodeProposal.task_text` is intentionally consumed but not persisted today. This plan does not add task-text persistence or construct production `WorkerTask` values; functional Worker dispatch needs a separate approved task-input retention/recovery design.
- A proposal-only crash, ambiguous usage, stale generation, cancellation race, expired lease, missing artifact, or unavailable audit/store fails closed and never marks success.
- The full test suite must pass with at least 90% total coverage; required systemd integration tests must run without skips.
- No V1 delivery claim is permitted after this plan alone; Worker, Gateway/effect integration, CLI/MCP, full security acceptance, and real improvement benchmarks remain open.

## Review Focus

- A proposal copied from another Run/Node/Attempt or a stale fencing generation must be rejected; pin this in `tests/unit/runtime/test_attempt_execution_coordinator.py::test_rejects_proposal_from_different_attempt`.
- A direct Scheduler, reconciliation, or Lifecycle success call without proof must fail without changing any stream; pin this in `tests/unit/lifecycle/test_scheduler.py::test_unproved_success_paths_are_rejected_atomically`.
- Cancellation or lease expiry between re-verification and final commit must not accept success or release a budget hold; pin this in `tests/unit/runtime/test_attempt_execution_coordinator.py::test_rechecks_active_lease_before_acceptance`.
- A crash after proposal persistence but before terminal commit must replay as an active/held Attempt, never a succeeded Node; pin this in `tests/integration/test_recovery.py::test_proposal_without_acceptance_never_promotes_on_restart`.
- A replayed, expired, wrong-digest, wrong-scope, or already-consumed ArtifactAccessGrant must not authorize a second artifact read; pin this in `tests/unit/artifacts/test_grants.py::test_grant_is_attempt_scoped_expiring_and_one_use`.

---

## Scope Note Discovered During Plan Review

`GraphPlanningService` consumes `NodeProposal.task_text`, while `NodeProposal` explicitly says raw text is never persisted and `PlanningNodeContract` contains no task text. Therefore this first slice starts from a durable `VerificationTask` proposal and does not construct or dispatch a `WorkerTask`. The later Functional Worker plan must first settle how task input is retained or resupplied across restart without weakening the existing data-minimization rule. This limitation is deliberate and keeps this plan within the approved acceptance-foundation phase.

## File Map

- Create `src/orchestrator/artifacts/grants.py`: process-local one-use ArtifactAccessGrant issuer/verifier.
- Create `src/orchestrator/runtime/acceptance_coordinator.py`: proposal lookup, active-attempt binding, isolated re-verification, usage-source call, and proof-gated Scheduler handoff.
- Modify `src/orchestrator/application.py`: own the grant authority and acceptance coordinator using its existing EventStore/ArtifactStore; expose only identity/hash-based acceptance, never raw IPC output.
- Modify `src/orchestrator/scheduler/core.py`: resolve durable active Attempts, commit accepted proposal references atomically, and reject unproved success/reconciliation.
- Modify `src/orchestrator/lifecycle/controller.py`: remove direct public success writes; provide only the internal proof-bearing success transition called from Scheduler.
- Modify `src/orchestrator/runtime/verification_journal.py`: provide exact proposal-by-task-hash lookup while retaining full-stream validation and append-only behavior.
- Modify `src/orchestrator/artifacts/__init__.py`: export the stable host-facing grant authority type.
- Create `tests/unit/artifacts/test_grants.py` and `tests/unit/runtime/test_attempt_execution_coordinator.py`.
- Modify `tests/unit/lifecycle/test_scheduler.py`, `tests/integration/test_recovery.py`, and `tests/integration/test_isolated_verifier_process.py` to use proof-bound success fixtures and restart assertions.
- Create `tests/support/attempt_acceptance.py`: test-only helper that builds Attempt-bound verification fixtures, journals them, and calls the proof-gated Scheduler operation.
- Modify `tests/unit/models/test_legacy_provider_upgrade.py`: authorized Task 5 compatibility assertion requires startup rejection of authentic archived BASE proofless success; direct legacy Provider journal/budget tests remain supported.
- Preserve `tests/support/generate_base_provider_fixture.py` and its archived BASE fixture unchanged: they intentionally reproduce historical proofless success; no migration or rewritten history.
- Modify `docs/security/worker-runtime.md` and `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md` to document the narrow acceptance gate and preserve the broader P5/V1 blockers.

## Task 1: Add one-use host ArtifactAccessGrant authority

**Files:**
- Create: `src/orchestrator/artifacts/grants.py`
- Modify: `src/orchestrator/artifacts/__init__.py`
- Create: `tests/unit/artifacts/test_grants.py`
- Modify: `tests/unit/lifecycle/test_scheduler.py` (ArtifactStore application construction helper)

**Interfaces:**
- Produces: `EphemeralArtifactGrantAuthority.issue(*, digest: str, run_id: str, expires_at: datetime) -> ArtifactAccessGrant` and `EphemeralArtifactGrantAuthority.verify(grant: ArtifactAccessGrant) -> bool`.
- `issue` creates an opaque random token held only in process memory and binds it to exactly one digest, one Run scope, one issuer, and one timezone-aware expiry. `verify` consumes the token once; expired, replayed, mutated, or scope-mismatched grants return `False`.
- `ControlPlaneApplication` constructs the authority before `ArtifactStore` and passes `authority.verify` as its grant verifier. No signature or signing key is persisted or sent to Worker/Verifier.

- [x] **Step 1: Write failing grant-scope and one-use tests**

Add tests that issue a grant for `sha256:` plus 64 lowercase hex characters and assert it verifies once; a second verification, a different digest, a different Run scope, an expired grant, and a naive expiry must not authorize access.

```python
def test_grant_is_attempt_scoped_expiring_and_one_use():
    authority = EphemeralArtifactGrantAuthority()
    expires = datetime.now(timezone.utc) + timedelta(minutes=1)
    grant = authority.issue(digest=DIGEST, run_id="run-a", expires_at=expires)

    assert grant.digest == DIGEST
    assert grant.scope == ("run-a",)
    assert authority.verify(grant) is True
    assert authority.verify(grant) is False
```

- [x] **Step 2: Run the focused test and verify the missing service fails**

Run: `python3 -m pytest tests/unit/artifacts/test_grants.py -q`
Expected: collection fails because `EphemeralArtifactGrantAuthority` does not exist.

- [x] **Step 3: Implement the process-local issuer/verifier**

Use `secrets.token_urlsafe(32)` as the opaque capability, store only its exact digest/scope/expiry/consumed state in a private dict, compare all grant fields, and consume only a valid unexpired grant. Do not accept an injected issuer string or token from callers.

- [x] **Step 4: Configure ArtifactStore from the application and run focused suites**

Update the application construction helper to pass the authority verifier; run `python3 -m pytest tests/unit/artifacts/test_grants.py tests/unit/artifacts/test_store.py tests/integration/test_isolated_verifier_process.py -q`.
Expected: all grant, ArtifactStore authorization, and live Verifier tests pass with zero skips.

- [x] **Step 5: Commit and push this task**

```bash
git add src/orchestrator/artifacts/grants.py src/orchestrator/artifacts/__init__.py src/orchestrator/application.py tests/unit/artifacts/test_grants.py tests/unit/lifecycle/test_scheduler.py
git commit -m "feat: issue one-use artifact read grants"
git push origin codex/p3-systemd-termination-receipts
```

## Task 2: Resolve active accepted Attempt state from durable Scheduler evidence

**Files:**
- Modify: `src/orchestrator/scheduler/core.py`
- Modify: `tests/unit/lifecycle/test_scheduler.py`

**Interfaces:**
- Produces: frozen `ActiveAttemptSnapshot(accepted_route: AcceptedModelRoute, reservation: BudgetReservation, agent_instance_id: str, accepted_at: datetime, lease_expires_at: datetime)`.
- Produces: `Scheduler.resolve_active_attempt(*, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, as_of: datetime) -> ActiveAttemptSnapshot`.
- The resolver reads `RoutingDecisionAccepted` from the global Scheduler stream, validates its complete route/event binding, confirms recovery still projects that exact Attempt as active, reloads its budget reservation, and rejects unknown/released/cancelled/stale/expired Attempts relative to trusted `as_of`. It is read-only.

- [x] **Step 1: Add tests for exact active resolution and stale terminal states**

Test a currently accepted Attempt resolves to the route/reservation/Agent/lease from durable state. Test wrong Run, Node, Attempt, generation, released Attempt, unknown Attempt, expired lease, corrupt event route, and cancelled Run all raise `SchedulerError` without appending events.

- [x] **Step 2: Run the focused test and verify it fails**

Run: `python3 -m pytest tests/unit/lifecycle/test_scheduler.py -k resolve_active_attempt -q`
Expected: FAIL because the resolver and snapshot model are not defined.

- [x] **Step 3: Add the immutable snapshot and resolver**

Define the snapshot with `ConfigDict(frozen=True, extra="forbid", strict=True)`. Parse the accepted route from the event payload, load the exact reservation using the existing `BudgetLedger`, and require a matching active lease from `RunRecoveryCoordinator.recover(run_id)` that remains unexpired at `as_of` before returning.

- [x] **Step 4: Verify resolution is read-only and transactionally consistent**

Run: `python3 -m pytest tests/unit/lifecycle/test_scheduler.py -k resolve_active_attempt -q`.
Expected: all resolver tests pass and Scheduler/EventStore stream versions remain unchanged.

- [x] **Step 5: Commit and push this task**

```bash
git add src/orchestrator/scheduler/core.py tests/unit/lifecycle/test_scheduler.py
git commit -m "feat: resolve active attempts from scheduler journal"
git push origin codex/p3-systemd-termination-receipts
```

## Task 3: Make success require a durable accepted Verifier proposal

Execution clarification: frozen input/graph/check binding is absent from the
current Scheduler record. Extend trusted host `Scheduler.accept_routing` with
optional keyword-only `input_manifest_hash`, `verification_contract`, and
`required_check_ids`, accepted only all together or all absent. Validate the
fixed supported contract/checks including `artifact-integrity` before writes,
capture graph version and construct complete Attempt context internally, and
persist it with the accepted check contract. Missing binding cannot succeed or
be retrofitted. Canonical binding must match on admission retries. Require full
PlanningNodeContract for success. Add tests for absent/partial/invalid binding,
input/check mismatch, graph drift, and changed/omitted retry bindings. The input
hash is a host commitment, not proof of undisclosed input content. No task-text
retention or Worker dispatch is added. Task 4 forwards this optional host
admission binding and consumes the shared canonical binding resolver.

**Files:**
- Modify: `src/orchestrator/scheduler/core.py`
- Modify: `src/orchestrator/lifecycle/controller.py`
- Modify: `src/orchestrator/runtime/verification_journal.py`
- Modify: `tests/unit/lifecycle/test_scheduler.py`
- Create: `tests/support/attempt_acceptance.py`
- Preserve unchanged: `tests/support/generate_base_provider_fixture.py` and the archived BASE fixture. Execution clarification: authentic historical proofless success is intentional compatibility evidence. Current startup rejects it; ordinary current success fixtures use the test-only proof-backed helper. No fixture regeneration, migration, grandfathering or history rewrite is authorized.

**Interfaces:**
- Produces: `VerifierProposalJournal.read_proposal(run_id: str, task_sha256: str) -> VerifierProposalRecord | None`, which first validates the complete Run proposal stream using the existing replay gate.
- Produces: `Scheduler.finish_verified_attempt(*, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, task_sha256: str, completed_at: datetime, usage: UsageRecord) -> None`.
- Ordinary `Scheduler.finish_attempt(... outcome="succeeded")`, `Scheduler.reconcile_attempt(... outcome="succeeded")`, and direct public Lifecycle success writers fail closed. `finish_verified_attempt` reloads the proposal from EventStore inside its `append_checked` callback, validates all context and accepted-check bindings, and records the task/evidence hashes in the terminal Scheduler event.
- Lifecycle success transition is performed only through the private Scheduler-owned proof-bearing writer after the proposal is validated. Failed/unknown and Provider failure-reconciliation behavior keep their existing settlement rules.

- [x] **Step 1: Test all unproved success bypasses and atomic no-write behavior**

Add `test_unproved_success_paths_are_rejected_atomically`: invoke the ordinary finish, Provider reconciliation, and direct Lifecycle success methods; assert each raises a stable error and that Scheduler, Lifecycle, Agent Registry, and budget stream versions are unchanged.

- [x] **Step 2: Run the focused regression test and confirm current bypasses**

Run: `python3 -m pytest tests/unit/lifecycle/test_scheduler.py -k unproved_success_paths -q`
Expected: FAIL because current Scheduler accepts a `succeeded` outcome without a verifier proof.

- [x] **Step 3: Add proposal lookup and a proof-bound terminal operation**

Make `finish_verified_attempt` idempotent by the existing Attempt reference. Inside `append_checked("scheduler", "global", ...)`, reload the exact proposal, require `evidence.outcome == "accepted"`, match Run/Node/Attempt/generation/graph and all frozen hashes to the accepted route/node contract, validate usage against the accepted reservation, and include `task_sha256` and `evidence_sha256` in the `AttemptSlotReleased` success payload. Call lifecycle, budget, and Agent nested appends before the outer transaction commits.

- [x] **Step 4: Close all direct success paths and update success-dependent fixtures**

Remove `"succeeded"` from unproved completion/reconciliation APIs. Route only the new Scheduler proof-bound operation to the internal Lifecycle success writer. Replace success setup in scheduler tests with a test helper that constructs exact Attempt-bound `VerificationTask`/`VerificationEvidence` fixtures, records a valid accepted `VerifierProposalRecord`, and calls `finish_verified_attempt`; do not use the helper in production code. Real ArtifactStore publication and isolated process behavior are tested separately in Task 4.

- [x] **Step 5: Test exact proof binding, duplicate decisions, corruption, and rollback**

Add tests for wrong task hash, wrong evidence outcome, mismatch in each Attempt/frozen hash, accepted-route/reservation mismatch, expired lease, stale generation, cancellation race, duplicate identical completion, conflicting duplicate, and a fault injected after nested lifecycle/budget writes. The fault case must leave every stream version and balance unchanged.

- [x] **Step 6: Run scheduler, journal, Provider reconciliation, and budget suites**

Run: `python3 -m pytest tests/unit/lifecycle/test_scheduler.py tests/unit/runtime/test_verification_journal.py tests/unit/test_provider_reconciliation.py tests/unit/budget -q`.
Expected: all tests pass; failed Provider reconciliation remains supported while Provider reconciliation cannot manufacture task success.

- [x] **Step 7: Commit and push this task**

```bash
git add src/orchestrator/scheduler/core.py src/orchestrator/lifecycle/controller.py src/orchestrator/runtime/verification_journal.py tests/unit/lifecycle/test_scheduler.py tests/support/attempt_acceptance.py
git commit -m "feat: require verifier proof for attempt success"
git push origin codex/p3-systemd-termination-receipts
```

## Task 4: Add the proposal acceptance coordinator and compose it in the application

Execution clarification: use Task 3's shared
`Scheduler.resolve_verification_binding(*, run_id, node_id, attempt_id,
fencing_generation, as_of) -> VerificationBinding` rather than reconstructing
frozen authority from the proposal. Forward the optional all-or-none host
admission binding through `ControlPlaneApplication.accept_routing`; do not add
raw-result or per-Attempt usage inputs. Include a real systemd coordinator test
with real SQLite, host-published ArtifactStore bytes, durable proposal, default
isolated Verifier, and test-only trusted usage source; it must reach the
proof-bound terminal/budget/Agent/slot result. A standalone Verifier test or fake
coordinator verifier does not replace this integration assertion.

**Files:**
- Create: `src/orchestrator/runtime/acceptance_coordinator.py`
- Modify: `src/orchestrator/application.py`
- Create: `tests/unit/runtime/test_attempt_execution_coordinator.py`
- Modify: `tests/unit/lifecycle/test_scheduler.py`
- Modify: `tests/integration/test_isolated_verifier_process.py`

**Interfaces:**
- Produces a private `AttemptExecutionCoordinator` with `accept_proposal(*, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, task_sha256: str) -> VerifierProposalRecord`.
- The coordinator depends on `Scheduler`, `ArtifactStore`, `IsolatedVerifierProcess`, `VerifierProposalJournal`, `EphemeralArtifactGrantAuthority`, a trusted UTC clock, and a host-only `AttemptUsageSource` protocol whose `usage_for(snapshot: ActiveAttemptSnapshot) -> UsageRecord` returns validated settlement evidence. `ControlPlaneApplication` may be composed with this trusted service dependency; when omitted, an unavailable source fails closed. This slice has no production Provider usage backend; tests inject a deterministic source.
- The coordinator uses a trusted internal UTC clock for initial lease validation and final completion time; callers cannot backdate a result. `ControlPlaneApplication.accept_verifier_proposal(...)` exposes only the exact Attempt identity and task hash. It does not accept `WorkerResult`, `VerificationEvidence`, `UsageRecord`, completion time, filesystem paths, or a no-effect claim.
- Exact application signature: `accept_verifier_proposal(*, run_id: str, node_id: str, attempt_id: str, fencing_generation: int, task_sha256: str) -> VerifierProposalRecord`.
- Exact composition change: add optional `usage_source: AttemptUsageSource | None = None` to `ControlPlaneApplication.__init__`; accept only the trusted service interface, never a usage record or caller callback per Attempt.
- The coordinator re-resolves the Attempt, loads exactly one matching durable proposal, compares every `AttemptContext` field to the resolved frozen state, re-runs the isolated Verifier (which rechecks ArtifactStore source provenance and bytes) using one-use per-artifact grants, compares canonical evidence hashes, obtains host usage, then calls `Scheduler.finish_verified_attempt`.

- [x] **Step 1: Write coordinator tests for accepted, missing, duplicate, and cross-Attempt proposals**

Build a coordinator fixture with real SQLite, ArtifactStore, journal, and test-only usage source. Use a pre-published JSON artifact whose source metadata exactly binds the active Attempt. Assert the coordinator accepts only the matching task hash and rejects a missing, duplicate, stale, or cross-Attempt proposal. Advance the fake clock past lease expiry and race Run cancellation after verification; neither case may commit success.

- [x] **Step 2: Run the focused test and confirm the coordinator is absent**

Run: `python3 -m pytest tests/unit/runtime/test_attempt_execution_coordinator.py -q`
Expected: collection fails because `AttemptExecutionCoordinator` is not implemented.

- [x] **Step 3: Implement proposal resolution and exact active-context comparison**

Read trusted UTC time and resolve the Scheduler snapshot at that time; then require exactly one proposal whose task hash matches and whose `AttemptContext` fields match the accepted route, Agent, graph, input manifest, effective config, Registry, policy, routing, and planning snapshots. Do not accept evidence or a completion timestamp as method arguments.

- [x] **Step 4: Re-run isolated verification and bind host usage**

Call `IsolatedVerifierProcess.verify(record.task, artifact_store, grant_for_digest=...)`; issue one-use grants scoped to the exact Run and digest. Recompute canonical evidence bytes and require the resulting evidence hash to equal the persisted proposal's hash. Ask the internal usage source for the active reservation, then read trusted UTC completion time after verification/settlement lookup and call Scheduler; Scheduler rechecks cancellation, fencing, and lease expiry inside the terminal transaction.

- [x] **Step 5: Compose the coordinator and add the application method**

Retain the private ArtifactStore on `ControlPlaneApplication`; create the verifier, grant authority, journal, and coordinator only when that store is configured. Add the optional trusted `usage_source` constructor dependency and `accept_verifier_proposal` as an identity/hash-only wrapper guarded by `_require_ready()`. Missing ArtifactStore, grant authority, usage source, proposal, or live verifier must return a stable fail-closed error and leave all streams unchanged.

- [x] **Step 6: Run focused unit and live process tests**

Run: `python3 -m pytest tests/unit/runtime/test_attempt_execution_coordinator.py tests/unit/runtime/test_artifact_admission.py tests/unit/runtime/test_verification_journal.py tests/integration/test_isolated_verifier_process.py -q`.
Expected: all required systemd tests execute with zero skips; accepted evidence is durably bound and rejected/inconclusive/transport failure never advances lifecycle.

- [x] **Step 7: Commit and push this task**

```bash
git add src/orchestrator/runtime/acceptance_coordinator.py src/orchestrator/application.py tests/unit/runtime/test_attempt_execution_coordinator.py tests/unit/lifecycle/test_scheduler.py
git commit -m "feat: coordinate verified proposal acceptance"
git push origin codex/p3-systemd-termination-receipts
```

## Task 5: Validate proof references during startup and restart replay

**Files:**
- Modify: `src/orchestrator/application.py`
- Modify: `src/orchestrator/scheduler/core.py`
- Modify: `tests/integration/test_recovery.py`
- Modify: `tests/unit/lifecycle/test_scheduler.py`
- Modify: `tests/unit/models/test_legacy_provider_upgrade.py` (authorized extra scope: archived BASE proofless success must fail startup without rewriting the fixture; independent legacy Provider behavior remains covered)

**Interfaces:**
- Produces: `Scheduler.validate_success_proofs() -> None`, a read-only full-stream audit that finds every successful `AttemptSlotReleased`, reloads its exact durable proposal, and verifies the recorded task/evidence hashes, outcome, route, generation, and frozen context.
- `ControlPlaneApplication._bootstrap()` runs this validator within the existing startup transaction before setting `_ready = True`. Missing, duplicated, corrupted, stale, or mismatched proof raises `StartupRecoveryFailed`; startup never promotes a proposal.
- Compatibility rule: historical succeeded Attempts without an exact proposal fail startup closed. This plan does not silently grandfather or rewrite old success events; any migration policy requires a separate approved design.

- [x] **Step 1: Add fresh-process proposal-only and committed-success replay tests**

Add one real-SQLite test that crashes/reopens after proposal append but before Scheduler acceptance and proves the Run is not succeeded. Add another that closes/reopens after commit and proves the success event and proposal hashes replay exactly once.

- [x] **Step 2: Add startup corruption and legacy-success rejection tests**

Tamper with or omit the proposal event, change the terminal event's task/evidence digest, bind it to another fencing generation, and seed an old `succeeded` terminal event without proof. Each new application construction must raise `StartupRecoveryFailed` and expose no readiness handle.

- [x] **Step 3: Run the recovery tests and confirm proof validation is missing**

Run: `python3 -m pytest tests/integration/test_recovery.py tests/unit/lifecycle/test_scheduler.py -k 'proposal_only or success_proof or legacy_success' -q`.
Expected: the proposal-only case demonstrates current non-promotion, while proof/legacy checks fail until bootstrap validation is added.

- [x] **Step 4: Implement full-stream proof audit and call it before readiness**

Validate all Scheduler stream terminal rows against the full validated proposal streams before `_bootstrap` completes. Do not write events during validation; retain current fail-closed `StartupRecoveryFailed` wrapping.

- [x] **Step 5: Run recovery, startup, concurrency, and crash suites**

Run: `python3 -m pytest tests/integration/test_recovery.py tests/integration/test_concurrency.py tests/integration/test_crash_matrix.py tests/unit/lifecycle/test_scheduler.py tests/unit/models/test_legacy_provider_upgrade.py -q`.
Expected: every test passes with zero skips, including response loss after atomic success commit and restart from proposal-only state.

- [x] **Step 6: Commit and push this task**

```bash
git add src/orchestrator/application.py src/orchestrator/scheduler/core.py tests/integration/test_recovery.py tests/unit/lifecycle/test_scheduler.py tests/unit/models/test_legacy_provider_upgrade.py
git commit -m "feat: validate verified attempt proofs on recovery"
git push origin codex/p3-systemd-termination-receipts
```

## Task 6: Document the boundary, run the full acceptance gate, and close this slice

**Files:**
- Modify: `docs/security/worker-runtime.md`
- Modify: `docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md`
- Modify: this plan

- [x] **Step 1: Document proof-gated acceptance and its limits**

State that a durable proposal is not an accepted Node; explain success proof binding, atomic Scheduler settlement, startup rejection on missing proof, the ArtifactAccessGrant one-use boundary, and the current absence of functional Worker task input/publisher/Gateway integration.

- [x] **Step 2: Keep broader P5/V1 work unchecked**

Record only this acceptance-foundation slice as complete. Leave functional Worker, task-input retention/recovery design, candidate byte publisher, Gateway/Approval/Secret Broker wiring, CLI/MCP, E2E security, and cost/token/rework benchmarks incomplete.

- [x] **Step 3: Run the focused acceptance suite**

Run: `python3 -m pytest tests/unit/artifacts/test_grants.py tests/unit/runtime/test_attempt_execution_coordinator.py tests/unit/runtime/test_verification_journal.py tests/unit/runtime/test_verifier_process.py tests/unit/lifecycle/test_scheduler.py tests/integration/test_isolated_verifier_process.py tests/integration/test_recovery.py -q`.
Expected: all required tests pass, zero skips, and each new gate has direct failure-path coverage.

- [x] **Step 4: Run full tests, coverage, build, and static checks**

Run:

```bash
python3 -m coverage run -m pytest --override-ini=addopts= -q
python3 -m coverage report
python3 -m compileall -q src tests
python3 -m pip check
python3 -m build --wheel
git diff --check "$(git merge-base origin/main HEAD)" HEAD
```

The existing interpreter is `/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python`; use it in place of `python3` for this gate. The environment has coverage.py but no pytest-cov. `coverage report` uses the repository's `fail_under = 90` and `precision = 2` configuration, preserving the same coverage threshold without installing dependencies.

Expected: all tests pass with zero skips, coverage is at least 90.00%, compile/dependency/wheel checks pass, and the complete branch-range diff is whitespace-clean. Report measured results; do not round a failing coverage result up.

Measured 2026-10-09 on source/test commit `04d5d2d380509d5179db84e6bc8fed5731d294c9`, including the startup policy-commitment fix: focused acceptance command (with `--override-ini=addopts=`) passed 391 tests in 91.92s; full `coverage run -m pytest --override-ini=addopts= -q` passed 1,786 tests in 308.42s. Both had zero skips and no warnings. `coverage report` passed: 16,946 statements, 1,645 missing, 90.29% total; coordinator 98.55%, Scheduler core 90.43%, application 97.16%. The 26 fully covered files suppressed by the report are not skipped tests. Required systemd integration ran. Compileall, pip check (`No broken requirements found.`), wheel build (`dist/multi_agent_orchestrator-0.1.0-py3-none-any.whl`) and complete branch-range diff check passed; dirty documentation whitespace is checked before the local commit. No production/test code or shared dependencies changed for Task 6.

### Delivery record — 2026-10-09

The preceding 1,786-test / 391-focused measurement is the original Task 6 gate on `04d5d2d380509d5179db84e6bc8fed5731d294c9`, retained as dated historical evidence. The controller subsequently completed the whole-slice review of `5bb9e58699b8f86ddb54a497fde273a19af35bac..3348abec05804e9d3054e8cea20a70533f2cbb8c`, covering all 88/88 changed files and the named existing integration boundaries. The single final fix commit `e3e7e1f1b38b4a04f16a0b1403f5fbec946c287d` addressed I1 (independent transactional startup validation of every proposal stream), M1 (consumed/expired grant cleanup), and M2 (README boundary accuracy). Independent scoped re-review closed I1/M1/M2 with no new Critical/Important breakage or residual fix finding.

The newer final exact-code gate for `e3e7e1f1b38b4a04f16a0b1403f5fbec946c287d` passed 1,797 full tests with zero skips and 90.29% total coverage (16,958 statements, 1,647 missing), plus 497 focused affected tests. Required real systemd isolation tests ran. `compileall -q src tests`, `pip check`, the normal isolated wheel build, and the complete branch-range whitespace check passed. No source/test change followed that full gate; the suppressed fully covered file rows are not skipped tests, and parent coverage does not imply sanitized child-process instrumentation.

The controller completed the documentation/fix push and remote verification: the tracked worktree was clean and local HEAD equaled `origin/codex/p3-systemd-termination-receipts` at `e3e7e1f1b38b4a04f16a0b1403f5fbec946c287d`. Steps 5–7 below record those completed acceptance-foundation delivery gates. This later bookkeeping update receives its own local documentation commit and controller review/push; the recorded equality describes the verified delivery tip before that update.

Only the acceptance foundation is delivered. Production `IsolatedWorkerProcess` remains blocked-only and default usage remains unavailable/fail-closed. Functional Worker execution, task-input retention/recovery, candidate byte publisher, Gateway/effect integration, CLI/MCP, full security acceptance, semantic/project-test verification and real cost/token/rework benchmarks remain open. No paid Provider calls, merge, deployment or V1 completion claim is established by this delivery record.

- [x] **Step 5: Perform a fresh whole-slice review before pushing the final documentation**

Review the complete diff from `git merge-base origin/main HEAD`, including tests and existing acceptance/reconciliation paths. Verify every `succeeded` writer is gated, every startup proof is reloaded from the journal, and no test-only adapter is exposed as a production application input. Fix findings and re-run the owning tests before proceeding.

- [x] **Step 6: Commit and push the documentation/gate results**

```bash
git add docs/security/worker-runtime.md docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md docs/superpowers/plans/2026-10-07-p5-verified-attempt-acceptance.md
git commit -m "docs: record verified attempt acceptance boundary"
git push origin codex/p3-systemd-termination-receipts
```

- [x] **Step 7: Verify remote tip and clean worktree**

Run: `git fetch origin && git status --short --branch && git rev-parse HEAD && git rev-parse origin/codex/p3-systemd-termination-receipts`.
Expected: local and remote commit IDs match and the tracked worktree is clean. Report that production Worker execution remains blocked and V1 is not complete.

## Execution Notes

- Work one task at a time. After each commit/push, independently inspect the exact task diff before starting the next task; do not carry an unresolved proof or recovery finding forward.
- The previous conversational choice was subagent-driven execution. Preserve that method for implementation if it still applies; do not create a Worker/Gateway subtask outside the six tasks without updating this plan.
- The acceptance foundation can be complete while the production Worker remains blocked-only. Do not remove its blocked-only guard as part of this plan.
