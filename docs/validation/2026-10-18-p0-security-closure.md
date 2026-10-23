# P0 security closure validation evidence

Observed on 2026-10-23 in an Ubuntu 24.04 WSL2 environment. This is evidence
for the candidate source commit `687209fcc7ef4f2c1548298a58cec4b56d052b87`;
the report and later documentation-only commits do not alter that tested
source identity. **P0 remains incomplete.** The required wheel build and wheel
contents inspection could not run because the existing environments do not
contain the `wheel` build dependency. No dependency was installed.

## Requirement-to-evidence map

| Original P0 requirement | Evidence in this validation | Status |
| --- | --- | --- |
| Safe workspace path operations, validated Overlay diff, reversible publication, and the fixed Worker-to-Gateway path | Full suite; clean-checkout live systemd smoke below; Task 1–5 reports and tests | Implementation evidence is present, but P0 remains open until packaging and the final whole-P0 review pass. This is one fixed tool path, not a general Worker or product capability. |
| Unsupported isolation/platform paths fail closed as `Blocked(isolation_unavailable)`; document Broker source/lifetime/endpoint and MCP caller trust | Existing admission negative tests and security-boundary docs; full suite has zero skips | Boundary is documented and tested for the existing API. MCP authenticated-caller/Authority Envelope integration is not implemented; P0 remains open until packaging and the final whole-P0 review pass. |

The original P0 checkboxes remain unchecked. No product platform is declared
supported, and no P3/P4/P5/P6/P7 status is advanced by this evidence.

## Full verification

Commands, run from the isolated candidate worktree:

```bash
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage run -m pytest -o addopts= -q --junitxml=.superpowers/sdd/2026-10-18-p0-security-closure-implementation/task-6-evidence/full-pytest.xml
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m coverage report --fail-under=90
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m compileall -q src tests
/home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pip check
git diff --check 687209fcc7ef4f2c1548298a58cec4b56d052b87
```

Results: **2,610 passed, 0 failed, 0 errors, 0 skipped** in 635.05 seconds;
coverage **90.30%** (19,125 statements, 1,855 missed). The required live P0
cases therefore had zero skips in this run. `compileall`, `pip check`, and the
pre-documentation `git diff --check` passed. Evidence logs and JUnit XML are
retained under the ignored Task 6 evidence directory. The reported percentage
is aggregate statement coverage; branch coverage was not separately measured.

SHA-256 evidence digests:

| Artifact | Digest |
| --- | --- |
| `full-pytest.log` | `e9564b0b248d4929914a8633f893b683067ce050169733aaeaca8602c2193334` |
| `full-pytest.xml` | `8b83aa7c373ecb0e7ac219d9a56f84a761281515db3256993361cbee7c3498f2` |
| `coverage-report.log` | `e22e9f1dfea482f7ffd3b01d3fe76d2b2e8c2794549e8bec82fffc7701873807` |
| `compileall.log` (empty success output) | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `pip-check.log` | `9261363b733079a641c2e4cc9bc46ffa1d8336945a87f807b6cf68847dbc9b09` |
| `diff-check-pre-docs.log` (empty success output) | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `build.log` | `0b8b80f3017b4299a16fc3e62d188407dcf5e3a5cab65b258d2e6fe8ea8e7471` |
| `build-system-python.log` | `7e9c5f86d42fd38017bcd25ac3278fc5f70e3c90cb448227e47cbefb0390ca50` |
| `clean-checkout-smoke.log` | `09cb038a9555597216a757db71ccfe4af46f9564814cedbb036642b400e6bf7e` |
| `clean-checkout-smoke.xml` | `1d4c6c4313b018c18aeef91d7c53538f196feab2bdcfa702a9e97b7ea154a8d8` |

The wheel gate did **not** pass:

- The project virtualenv's `python -m build --wheel --no-isolation` failed
  because that environment has no `setuptools.build_meta` backend.
- Existing WSL Python 3.12.3's `python3 -m build --wheel --no-isolation`
  reached `egg_info`, then reported the missing `wheel` dependency.
- Neither the checked project virtualenv nor WSL system Python can import
  `wheel`; no `uv`, `hatch`, or `poetry` command is available. The pip cache contains only a previously built
  `multi_agent_orchestrator-0.1.0-py3-none-any.whl`, not a build tool for this
  candidate. That stale artifact was not substituted for a candidate build.
- Consequently wheel contents were not inspected. Installing a shared or new
  dependency is outside the approved scope; this gate remains blocked pending
  an already-available build environment or an explicit scope decision.

## Clean-checkout live systemd smoke

The exact candidate commit above was cloned to a fresh temporary checkout at
`/tmp/maestro-p0-task6-FEc4mS/repo`; the checkout began clean. Using the
existing project test environment, this command exercised the actual WSL2
systemd backend (not an injected fake service):

```bash
cd /tmp/maestro-p0-task6-FEc4mS/repo
PYTHONPATH=src:. /home/paul2/workspace/multi-agent-orchestrator/.venv/bin/python -m pytest -o addopts= -q \
  tests/integration/test_workspace_worker_execution.py::test_real_worker_gateway_publication_and_replay \
  tests/integration/test_p0_workspace_acceptance.py::test_real_approval_requires_fresh_attempt_and_reconciles_committed_write \
  --junitxml=/mnt/c/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator/.superpowers/sdd/2026-10-18-p0-security-closure-implementation/task-6-evidence/clean-checkout-smoke.xml \
  > /mnt/c/Users/paul2/.codex/worktrees/p3-provider-reconciliation/multi-agent-orchestrator/.superpowers/sdd/2026-10-18-p0-security-closure-implementation/task-6-evidence/clean-checkout-smoke.log 2>&1
```

Result: **3 passed in 9.54 seconds**. The first test covers serial and
concurrent invocation of the fixed workspace tool path, including add, modify,
mode change, delete, rename-as-delete-plus-add, symlink publication, replay,
and exactly one candidate/Worker/Gateway operation. The second covers a fresh
accepted Attempt for ApprovalService and reconciliation of a committed write.
The tests require the real systemd Overlay/cgroup backend; the Worker path
rejects any Provider invocation. This is local correctness evidence only, not
a Provider measurement or production-readiness result.

Observed environment and profile evidence:

- Ubuntu 24.04 WSL2, kernel `6.6.87.2-microsoft-standard-WSL2`, native
  `x86_64`, Python 3.12.3, systemd `255.4-1ubuntu8.18`; PID 1 is systemd.
- Workspace Worker/publication profile:
  `sha256:80da707fee925face2fdb287a2c8e84bec0455b638aca0ddac3f9b7d97c3e718`.
- Approval/fresh-Attempt profile:
  `sha256:e5180cbb66e018c735b1496e2736bb362c07dcdea8492e687d9e7d84c8c01378`.
- Default command limits: memory 512 MiB, 64 tasks, CPU quota 100%, 300-second
  timeout, 1 MiB combined output, 256 file descriptors, and 64 MiB per file;
  candidate upper 16 MiB, scratch 8 MiB, and 10,000 entries.
- The measured backend is not a general platform guarantee. Other Linux
  distributions, Windows, macOS, arbitrary project package/test environments,
  and workspace/runtime paths containing spaces, colon, backslash, or line
  breaks remain unsupported or unverified. Missing required isolation blocks
  with `isolation_unavailable`; there is no unisolated fallback.

No credentials or private task text are recorded here. No real or paid Provider
request was made. A Worker stop receipt does not prove Provider delivery,
charge, or authoritative reconciliation.

## Remaining Task 6 gates

1. Obtain an already-available clean wheel build environment without installing
   dependencies, or request a decision to change that constraint; inspect the
   resulting candidate wheel contents.
2. Obtain independent review of the complete P0 range, address any Critical or
   Important findings, and verify the final local branch matches the ordinary
   remote push. Until then, both original P0 checklist entries stay open.
