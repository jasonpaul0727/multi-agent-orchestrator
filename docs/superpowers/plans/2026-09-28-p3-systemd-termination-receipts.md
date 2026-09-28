# P3 Slice: systemd-verified termination receipts

## Goal

Make SystemdReadOnlyLauncher report successful termination only after the host has observed both an inactive systemd unit and an empty attempt cgroup. An accepted systemctl kill, a returned systemd-run process, or an exited main PID alone must not be treated as proof that descendants stopped. If the host cannot verify the exact cgroup is empty, the result remains unconfirmed, Worker and Verifier proposals are rejected, and private staging is retained.

This is a P3 recovery-control-plane prerequisite, not an enabled Worker, a full interruption matrix, or a claim that P3/V1 is complete. It preserves the current OpenAI store=false behavior; unknown Provider outcomes without authoritative evidence remain held and are never replayed.

## Architecture

- Bind each read-only transient service to a validated app.slice cgroup and record the expected cgroup path before start. Derive that path from the trusted systemd parent rather than from Worker output.
- Add a host-created immutable termination receipt to SandboxResult. It must bind the exact unit and cgroup and represent the observed inactive unit plus cgroup.events populated 0 result. Keep termination_confirmed as a compatibility property derived only from a valid receipt.
- Reuse the cgroup-v2 validation and bounded inactive/empty polling pattern already used by the Overlay candidate launcher; do not trust child exit, accepted kill, or caller-supplied booleans as a receipt.
- Keep the private staging owner detached from automatic TemporaryDirectory cleanup until a verified stop permits explicit cleanup. An unverified stop leaves the staging retained and cannot yield a usable Worker/Verifier result.
- Keep the launcher's failure mode fail-closed: malformed unit state, an escaped or unexpected cgroup path, populated cgroup, timeout, or unavailable systemd evidence all produce no receipt.

## Tech Stack

Python 3.12, systemd user transient services, cgroup v2, pytest, existing SystemdReadOnlyLauncher and SystemdOverlayCandidateLauncher implementation.

## Spec Links

- docs/superpowers/plans/2026-09-22-full-v1-product-implementation-plan.md (P0/P3 isolation and crash-recovery gates)
- docs/security/platform-support.md
- docs/security/worker-runtime.md
- docs/superpowers/specs/2026-09-23-v1-control-plane-architecture-decisions.md

## Global Constraints

- Worker/Verifier stdout remains an untrusted proposal; only the host launcher may create a termination receipt.
- No result, slot, budget, or staging is released on an unverified stop.
- Cancellation acceptance is a request, not a receipt.
- Do not change Provider retention, automatically retry calls, or settle an outcome_unknown attempt as part of this slice.
- Do not weaken existing process limits, workspace boundaries, network block, or systemd failure-closed behavior.
- Do not mark all of P3 complete: Provider-side authoritative reconciliation, Scheduler/Worker cancellation wiring, complete crash matrix, and automatic recovery driving remain open.

## Review Focus

1. A main process exits while a descendant remains in the unit: there must be no receipt and no Worker/Verifier result admission.
2. systemctl kill returns success but the cgroup is still populated: the result must remain unconfirmed.
3. Unit inspection fails, returns malformed properties, or reports an unexpected cgroup: fail closed without path traversal or false receipt.
4. The unit is inactive but cgroup.events is unavailable or says populated: do not release staging or claim termination.
5. The unit/cgroup is empty but the command timed out, was cancelled, exceeded output limits, or returned invalid IPC: termination may be proven, but its Worker/Verifier proposal must still be rejected.

## Implementation Tasks

### Task 1: Specify and test the trusted termination receipt

- Add focused tests in tests/unit/isolation/test_launcher.py for valid receipt binding, invalid unit/cgroup values, non-empty cgroups, missing cgroup.events, malformed systemctl show output, and bounded polling.
- Add Worker and Verifier tests proving a result without the host-created receipt is rejected, even if a fake child reports clean stdout/exit status.
- Run the new tests first and confirm they fail for the missing receipt proof.

### Task 2: Bind and verify the read-only service cgroup

- In src/orchestrator/isolation/launcher.py, resolve and validate the app.slice cgroup parent before creating the transient service; bind the expected per-attempt unit cgroup and pass the identity into SandboxSession.
- Implement bounded unit-state and cgroup-empty verification, following the existing Overlay helper's no-follow cgroup access and strict populated 0 parsing.
- Add a private immutable receipt type and derive SandboxResult.termination_confirmed only from the verified receipt.
- Detach staging cleanup from garbage collection while the unit may still be alive; explicitly release staging only after verified stop. Preserve uncertain staging on stop failure.
- Ensure repeated wait() returns the same result/receipt and cannot release retained staging a second time.

### Task 3: Validate real process-tree stop receipts

- Extend tests/integration/test_systemd_launcher.py to assert a receipt for a normal clean exit and for a killed process tree, and verify the exact attempt cgroup is empty before the receipt is returned.
- Cover timeout, cancellation, and output-limit termination. Confirm these cases may have a valid stop receipt while still not yielding an accepted Worker proposal.
- Run the full test suite, coverage gate, compile, pip check, and wheel build.
- Update docs/security/worker-runtime.md and the V1 plan with only the verified scope; leave full Worker activation and P3 completion unchecked.

## Acceptance

- A real descendant process cannot outlive an accepted termination receipt.
- Every missing, malformed, mismatched, timed-out, or non-empty OS witness fails closed and leaves the result/staging unavailable for admission.
- Existing supported-platform integration tests pass on the measured Ubuntu 24.04 / WSL2 / systemd profile; environments without systemd skip only the live integration layer and still pass deterministic unit tests.
- No Provider request is replayed and no unknown Provider budget is settled.
