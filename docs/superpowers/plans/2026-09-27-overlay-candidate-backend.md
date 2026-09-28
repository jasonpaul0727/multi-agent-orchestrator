# Overlay candidate execution backend

Approved direction: the user requested continuation on 2026-09-27 after the
private OverlayFS backend proposal. Execute inline under the existing V1 and
permissions/security specifications; verify and push the completed slice.

## Contract

`SystemdOverlayCandidateLauncher.launch(workspace, command, *, limits,
input_bytes, expected_workspace_identity_hash)` returns a host-owned
`OverlayCandidateSession`. Its `wait()` returns an `OverlayCandidateResult`
containing the bounded `SandboxResult`, frozen lower snapshot and, only after
confirmed termination and host validation, a `WorkspaceDiff`. `close()` removes
private staging only after confirmed termination. No workspace publication or
node acceptance occurs in this API.

The trusted bootstrap runs as PID 1 inside new user/mount/network/PID
namespaces under a systemd user scope. It mounts a bounded tmpfs upper layer,
a private OverlayFS workspace, a minimal filesystem root and read-only runtime
binds. The command enters that root with all Linux capabilities dropped,
no-new-privileges, Landlock and a seccomp filter. Its environment contains no
host credentials. All descendants are killed and reaped before upper export.
The host verifies the private completion record and revalidates the candidate.

## Constraints and test cases

- Preserve the existing read-only launcher and its callers.
- Do not write the live workspace, publish changes, or consume approval grants.
- Fail closed if Linux, systemd, namespaces, OverlayFS, Landlock, libseccomp,
  capability dropping, resource limits or stop confirmation are unavailable.
- Retain staging if stopping cannot be confirmed; never infer candidate
  acceptance from stdout, process exit alone, or a worker-supplied manifest.
- Unsupported deletions/whiteouts, unsafe symlinks, hard links, control-directory
  changes and candidate bounds continue to fail closed in the existing exporter.
- No provider/network call, key or paid-model test is needed for this slice.

## Tasks

- [x] Write and run live failing tests for private candidate writes, unchanged
  source, hidden controls/home/proc, socket/mount restrictions, clean environment
  and candidate lifetime.
- [x] Implement the namespace bootstrap and command hardening; confirm exact
  cgroup/rlimit values before starting the requested command.
- [x] Implement the host launcher/session, termination checks and no-follow
  bounded completion decoding, then run the live success test.
- [x] Add cancellation, time/output/disk limits, lingering descendants, failed
  execution, malformed completion, invalid snapshots and exporter rejection
  tests; fix each observed failure at its cause.
- [x] Update security/platform/README context with the measured internal backend
  and remaining application-service integration gaps.
- [x] Run the full suite with coverage >= 90%, compile, dependency check, wheel
  build and diff review; commit and push, verify remote synchronization.

## Verification

Run targeted tests with `.venv/bin/python -m pytest
tests/unit/isolation/test_overlay_launcher.py
tests/unit/isolation/test_candidate_exec.py
tests/unit/isolation/test_overlay_bootstrap.py
tests/integration/test_overlay_candidate_launcher.py`.

The final gate is `.venv/bin/python -m coverage run -m pytest` followed by
`.venv/bin/python -m coverage report`; all live tests on this WSL/systemd host
must execute. Review every required failure boundary, not only total coverage.

This backend completes an internal candidate-execution slice. Product
workspace-write remains gated on Worker, Scheduler, Approval/Gateway/audit,
Secret Broker, publication and recovery wiring.

Final local gate (2026-09-27): 1095 tests passed, no skips on this WSL/systemd
host; total coverage 90.76% with the unchanged 90% threshold. `compileall`,
`pip check`, wheel build and diff checks passed. Independent read-only review
found trusted-import injection, failure-state stop proof, transport-exit
cancellation, uncertain-start registration and FIFO decoding issues; fixes
and regression tests are included. Scope identity is verified before command
start; emptiness proof is checked independently after execution. No paid
provider was invoked and no benchmark improvement number is validated here.
