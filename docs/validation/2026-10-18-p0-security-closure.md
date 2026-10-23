# P0 security closure validation evidence

Observed on 2026-10-23 in an Ubuntu 24.04 WSL2 environment. This is evidence
for the candidate source commit `687209fcc7ef4f2c1548298a58cec4b56d052b87`;
the report and later documentation-only commits do not alter that tested
source identity. The final full run reports **2,611 passed, 0 failed/errors/
skipped and 90.29% statement coverage**; the clean-checkout systemd smoke passed
3 cases. The candidate wheel built in a one-off private environment and its
contents were independently checked against all 96 tracked Python modules;
wheel SHA-256 is
`92b6e9e99b0b08dca6805c7d21c5b1d417d78640903afd603ebc60e379adf6be`.
No shared dependency environment changed. The final whole-P0 independent review
concluded that both original P0 acceptance items are met by the implementation,
tests, and boundary documentation; the original checklist is closed below.
This is P0 closure only, not full V1 or production readiness.

## Requirement-to-evidence map

| Original P0 requirement | Evidence in this validation | Status |
| --- | --- | --- |
| Safe workspace path operations, validated Overlay diff, reversible publication, and the fixed Worker-to-Gateway path | Full suite (2,611 passed, 0 skipped; 90.29% statement coverage); clean-checkout live systemd smoke (3 passed); Task 1–5 reports/tests; exact wheel inventory and hash below | **Met for the fixed tool-only path.** The independent final review found the bounded reversible publication and evidence sufficient for this P0 item. This is not a general Worker or product capability. |
| Unsupported isolation/platform paths fail closed as `Blocked(isolation_unavailable)`; document Broker source/lifetime/endpoint and MCP caller trust | Existing admission negative tests, live missing-control test, full suite (0 skipped), and security-boundary docs | **Met as a fail-closed behavior and documented trust boundary.** Authenticated MCP caller/Authority Envelope integration is not implemented and remains deferred; this P0 item requires documenting that boundary, not shipping MCP authentication. |

Both original P0 checkboxes are checked in the full V1 plan based on the
evidence and final independent-review conclusions recorded here. No product
platform is declared supported, and no P3/P4/P5/P6/P7 status is advanced by
this P0 closure.

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

## Post-review security-branch regression run (2026-10-23)

After the whole-P0 review, inspection of the coverage report identified an
uncovered fail-closed guard in `WorkspaceView` for a symlink chain that
continues through a non-directory target. Added
`test_symlink_chain_rejects_non_directory_intermediate` to
`tests/unit/isolation/test_workspace_view.py`. The test passed against the
current implementation, failed when the guard was temporarily disabled, and
passed again after restoring the guard. The production source tree was not
changed. The focused file run reported **74 passed**; the subsequent full run
reported **2,611 passed, 0 failed, 0 errors, 0 skipped** in 594.02 seconds.
Aggregate statement coverage was **90.29%** (19,125 statements, 1,857 missed).

The full run used the same isolated candidate worktree and existing test
environment, with a separate coverage data file so the earlier evidence was
preserved. Its logs and XML are under the ignored Task 6 evidence directory.
The final whole-P0 review of the earlier documentation range found no
release-blocking findings. A follow-up independent review of commit
`4bacdf9ecb2fdad24d9f65a5e28eedeed9a2c08b` also found no release-blocking
findings: it confirmed the test reaches the non-directory intermediate guard,
the source implementation is unchanged, the reported totals and hashes match
the retained artifacts, and both original P0 checkboxes remain unchecked. The
ordinary push was verified with local and remote branch SHA both equal to
`4bacdf9ecb2fdad24d9f65a5e28eedeed9a2c08b`. The wheel build/content gate
remains blocked, so the P0 checkboxes remain open.

SHA-256 evidence digests for the post-review run:

| Artifact | Digest |
| --- | --- |
| `full-pytest-after-review.log` | `d3bf5702ee3bf51d80b15a808b573a276bc3a7140649e14b3faa3e6987313696` |
| `full-pytest-after-review.xml` | `3d3dbd934203e1e07d3f933f702be21d5388720700cd17a5039cdeeab97d5bb8` |
| `coverage-report-after-review.log` | `a9b26266384035c252d4394ea0ab3d374ce790497b1f9b58506c4a69d3c00cf4` |

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

Initial wheel attempts were blocked by missing tooling in the project virtual
environment and missing `wheel` in the WSL system Python. The stale wheel in
the pip cache was not substituted. The gate was subsequently completed with a
one-off private environment, without changing any shared dependency:

```bash
python3 -m venv --system-site-packages /tmp/maestro-p0-task6-wheel.qDhql9/venv
/tmp/maestro-p0-task6-wheel.qDhql9/venv/bin/python -m pip install --no-deps wheel
/tmp/maestro-p0-task6-wheel.qDhql9/venv/bin/python -m build --wheel --no-isolation \
  --outdir /tmp/maestro-p0-task6-wheel.qDhql9/dist
```

The temporary environment installed only `wheel==0.48.0`; it reused existing
system `setuptools==68.1.2` and `build==1.4.0`. The build succeeded. Content
inspection compared archived Python modules to `git ls-files src/orchestrator`:
all **96 tracked Python modules** were present exactly, with no extras. The
wheel contained no tests, docs, `.superpowers`, `.maestro`, `.git`, bytecode,
or other private artifacts; runtime metadata named only the expected Pydantic
and PyYAML requirements. SHA-256:
`92b6e9e99b0b08dca6805c7d21c5b1d417d78640903afd603ebc60e379adf6be`.

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

## Task 6 review and push closure

The wheel-evidence delta in commit
`4b27417509e39da26e7760c2cb5dd68f85aa7db0..72f1f68bd36bdee3bac98228500bef2335d917d0`
received an independent read-only review with no Critical, Important, or Minor
findings. The reviewer independently confirmed the wheel SHA-256, exact match
between the 96 wheel modules and tracked module paths/bytes, expected runtime
metadata, no extra package/test/doc/internal artifacts, and no shared
dependency change. The ordinary push of `72f1f68bd36bdee3bac98228500bef2335d917d0`
was verified against `git ls-remote`; local and remote SHA matched.

The final whole-P0-range independent review of
`ae12d40458e6c8cf41c925ca52cf01a5c965098f..8d317c3cbc18894004e24ab94b4ca40e32d86296`
found no source-level P0 security finding blocking the fixed internal scope and
concluded that both original P0 items are technically justified as complete.
The reviewer independently confirmed the final full-suite counts and coverage,
3 clean-checkout systemd smokes, wheel SHA/module inventory/runtime metadata,
and the distinction between P0 closure and deferred P3/P4/P6/P7/production work.
The ordinary push of `8d317c3cbc18894004e24ab94b4ca40e32d86296` was verified
against `git ls-remote`; local and remote SHA matched. This closes the P0
acceptance rows only; it does not establish full V1 or production readiness.
