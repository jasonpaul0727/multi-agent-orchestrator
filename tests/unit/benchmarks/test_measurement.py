"""Synthetic traces test arithmetic and rejection; they are not benchmark evidence."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from orchestrator.benchmarks import BenchmarkError, evaluate_manifest
from orchestrator.benchmarks.__main__ import main


_HASH = "a" * 64


def _evidence_bytes(run_id: str, kind: str) -> bytes:
    return f"synthetic {run_id} {kind}\n".encode("utf-8")


def _run(
    run_id: str,
    *,
    cost: int,
    input_tokens: int,
    output_tokens: int,
    repeated_keys: tuple[str, ...],
) -> dict:
    return {
        "schema_version": 1,
        "feature_id": "feature-one",
        "workload_sha256": _HASH,
        "protocol_sha256": "b" * 64,
        "run_id": run_id,
        "currency": "USD",
        "status": "succeeded",
        "verification": {
            "verifier_id": "verifier-v1",
            "evidence_file": f"{run_id}-verification.txt",
            "evidence_sha256": hashlib.sha256(
                _evidence_bytes(run_id, "verification")
            ).hexdigest(),
            "accepted": True,
        },
        "attempts": [
            {
                "attempt_id": f"{run_id}-failed",
                "sequence": 1,
                "outcome": "failed",
                "completed_work_keys": ["unit-a", "unit-b"],
            },
            {
                "attempt_id": f"{run_id}-success",
                "sequence": 2,
                "outcome": "succeeded",
                "completed_work_keys": list(repeated_keys),
            },
        ],
        "model_calls": [
            {
                "call_id": f"{run_id}-call",
                "attempt_id": f"{run_id}-success",
                "usage_record_id": f"{run_id}-usage-record",
                "usage_evidence_file": f"{run_id}-usage.txt",
                "usage_evidence_sha256": hashlib.sha256(
                    _evidence_bytes(run_id, "usage")
                ).hexdigest(),
                "cost_record_id": f"{run_id}-cost-record",
                "cost_evidence_file": f"{run_id}-cost.txt",
                "cost_evidence_sha256": hashlib.sha256(
                    _evidence_bytes(run_id, "cost")
                ).hexdigest(),
                "usage_source": "provider_reported",
                "cost_source": "settled_ledger",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "reasoning_tokens": 5,
                "cached_input_tokens": 3,
                "cost_minor": cost,
            }
        ],
    }


def _files(tmp_path: Path) -> tuple[Path, dict, dict]:
    baseline = _run(
        "baseline-run", cost=1800, input_tokens=60, output_tokens=30,
        repeated_keys=("unit-a", "unit-b"),
    )
    candidate = _run(
        "candidate-run", cost=700, input_tokens=25, output_tokens=15,
        repeated_keys=("unit-a",),
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "schema_version": 1,
        "pairs": [{
            "feature_id": "feature-one",
            "baseline": "baseline.json",
            "candidate": "candidate.json",
        }],
    }), encoding="utf-8")
    _write_runs(tmp_path, baseline, candidate)
    return manifest, baseline, candidate


def _write_runs(root: Path, baseline: dict, candidate: dict) -> None:
    (root / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
    (root / "candidate.json").write_text(json.dumps(candidate), encoding="utf-8")
    _write_evidence(root, baseline)
    _write_evidence(root, candidate)


def _write_evidence(root: Path, run: dict) -> None:
    run_id = run["run_id"]
    for kind in ("verification", "usage", "cost"):
        (root / f"{run_id}-{kind}.txt").write_bytes(_evidence_bytes(run_id, kind))


def test_measures_exact_paired_observations_without_double_counting_breakdowns(
    tmp_path: Path,
) -> None:
    manifest, _, _ = _files(tmp_path)

    report = evaluate_manifest(manifest)

    assert report.feature_count == 1
    assert report.currency == "USD"
    assert (report.baseline_cost_minor, report.candidate_cost_minor) == (1800, 700)
    assert report.cost_reduction_percent == "61.11"
    assert (report.baseline_tokens, report.candidate_tokens) == (90, 40)
    assert report.token_reduction_percent == "55.56"
    assert (
        report.baseline_failure_reexecuted_work_units,
        report.candidate_failure_reexecuted_work_units,
    ) == (2, 1)
    assert report.failure_rework_reduction_percent == "50.00"
    assert report.manifest_sha256 == hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert report.pairs[0].baseline.source_sha256 == hashlib.sha256(
        (tmp_path / "baseline.json").read_bytes()
    ).hexdigest()


@pytest.mark.parametrize(
    "side,change,reason",
    [
        ("candidate", {"workload_sha256": "f" * 64}, "workloads"),
        ("candidate", {"protocol_sha256": "f" * 64}, "protocols"),
        ("candidate", {"currency": "EUR"}, "currency"),
        ("candidate", {"feature_id": "different"}, "feature"),
        ("candidate", {"run_id": "baseline-run"}, "run IDs"),
        ("baseline", {"status": "unknown"}, "verification"),
        ("candidate", {"model_calls": []}, "trace is invalid"),
    ],
)
def test_rejects_incomparable_or_unmeasured_runs(
    tmp_path: Path, side: str, change: dict, reason: str
) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    target = baseline if side == "baseline" else candidate
    target.update(change)
    _write_runs(tmp_path, baseline, candidate)

    with pytest.raises(BenchmarkError, match=reason):
        evaluate_manifest(manifest)


def test_requires_same_verifier_and_accepted_result(tmp_path: Path) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    candidate["verification"]["verifier_id"] = "verifier-v2"
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="verifier versions"):
        evaluate_manifest(manifest)

    candidate["verification"]["verifier_id"] = "verifier-v1"
    candidate["verification"]["accepted"] = False
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="verification"):
        evaluate_manifest(manifest)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda run: run["attempts"][1].update(sequence=1),
        lambda run: run["attempts"][1].update(attempt_id="baseline-run-failed"),
        lambda run: run["attempts"][0].update(completed_work_keys=["x", "x"]),
        lambda run: run["attempts"][0].update(completed_work_keys=[]),
        lambda run: run["model_calls"][0].update(attempt_id="missing"),
        lambda run: run["model_calls"][0].update(input_tokens=True),
        lambda run: run["model_calls"][0].update(usage_source="estimated"),
        lambda run: run["model_calls"][0].update(cost_source="measured_local"),
        lambda run: run["model_calls"][0].update(usage_evidence_sha256="not-a-hash"),
    ],
)
def test_rejects_invalid_trace_events(tmp_path: Path, mutate) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    mutate(baseline)
    if baseline["attempts"][0]["completed_work_keys"] == []:
        baseline["attempts"][1]["completed_work_keys"] = []
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="trace is invalid"):
        evaluate_manifest(manifest)


def test_rejects_duplicate_model_calls_and_unknown_fields(tmp_path: Path) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    baseline["model_calls"].append(deepcopy(baseline["model_calls"][0]))
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="trace is invalid"):
        evaluate_manifest(manifest)

    baseline["model_calls"].pop()
    baseline["fabricated_saving"] = "60%"
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="trace is invalid"):
        evaluate_manifest(manifest)


@pytest.mark.parametrize("record_field", ["usage_record_id", "cost_record_id"])
def test_rejects_duplicate_accounting_records(
    tmp_path: Path, record_field: str
) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    duplicate = deepcopy(baseline["model_calls"][0])
    duplicate["call_id"] = "different-call"
    other_field = (
        "cost_record_id" if record_field == "usage_record_id" else "usage_record_id"
    )
    duplicate[other_field] = "different-record"
    baseline["model_calls"].append(duplicate)
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="trace is invalid"):
        evaluate_manifest(manifest)


def test_requires_positive_baselines_for_all_three_claims(tmp_path: Path) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    baseline["model_calls"][0]["cost_minor"] = 0
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="baseline cost"):
        evaluate_manifest(manifest)

    baseline["model_calls"][0]["cost_minor"] = 1
    baseline["model_calls"][0].update(input_tokens=0, output_tokens=0)
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="baseline tokens"):
        evaluate_manifest(manifest)

    baseline["model_calls"][0].update(input_tokens=1)
    baseline["attempts"][1]["completed_work_keys"] = ["new-unit"]
    _write_runs(tmp_path, baseline, candidate)
    with pytest.raises(BenchmarkError, match="baseline failure rework"):
        evaluate_manifest(manifest)


def test_preserves_negative_reduction_when_candidate_is_worse(tmp_path: Path) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    candidate["model_calls"][0]["cost_minor"] = 3600
    candidate["model_calls"][0].update(input_tokens=120, output_tokens=60)
    candidate["attempts"][1]["completed_work_keys"] = ["unit-a", "unit-b"]
    candidate["attempts"].append({
        "attempt_id": "candidate-third",
        "sequence": 3,
        "outcome": "succeeded",
        "completed_work_keys": ["unit-a", "unit-b"],
    })
    _write_runs(tmp_path, baseline, candidate)
    report = evaluate_manifest(manifest)
    assert report.cost_reduction_percent == "-100.00"
    assert report.token_reduction_percent == "-100.00"
    assert report.failure_rework_reduction_percent == "-100.00"


def test_aggregates_multiple_pairs_and_skips_nonfailure_reexecution(
    tmp_path: Path,
) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    candidate["attempts"][0]["outcome"] = "cancelled"
    _write_runs(tmp_path, baseline, candidate)

    second_baseline = _run(
        "baseline-two", cost=100, input_tokens=6, output_tokens=4,
        repeated_keys=("unit-a",),
    )
    second_candidate = _run(
        "candidate-two", cost=50, input_tokens=5, output_tokens=3,
        repeated_keys=(),
    )
    second_baseline["feature_id"] = "feature-two"
    second_candidate["feature_id"] = "feature-two"
    (tmp_path / "baseline-two.json").write_text(
        json.dumps(second_baseline), encoding="utf-8"
    )
    (tmp_path / "candidate-two.json").write_text(
        json.dumps(second_candidate), encoding="utf-8"
    )
    _write_evidence(tmp_path, second_baseline)
    _write_evidence(tmp_path, second_candidate)
    content = json.loads(manifest.read_text(encoding="utf-8"))
    content["pairs"].append({
        "feature_id": "feature-two",
        "baseline": "baseline-two.json",
        "candidate": "candidate-two.json",
    })
    manifest.write_text(json.dumps(content), encoding="utf-8")

    report = evaluate_manifest(manifest)

    assert report.feature_count == 2
    assert (report.baseline_cost_minor, report.candidate_cost_minor) == (1900, 750)
    assert (report.baseline_tokens, report.candidate_tokens) == (100, 48)
    assert (
        report.baseline_failure_reexecuted_work_units,
        report.candidate_failure_reexecuted_work_units,
    ) == (3, 0)
    assert report.failure_rework_reduction_percent == "100.00"


def test_rejects_duplicate_feature_pairs(tmp_path: Path) -> None:
    manifest, _, _ = _files(tmp_path)
    content = json.loads(manifest.read_text(encoding="utf-8"))
    content["pairs"].append(deepcopy(content["pairs"][0]))
    manifest.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(BenchmarkError, match="manifest is invalid"):
        evaluate_manifest(manifest)


@pytest.mark.parametrize("bad", ["../outside.json", "/tmp/outside.json", "C:/tmp/a", "sub\\a", "./baseline.json"])
def test_rejects_unsafe_trace_paths(tmp_path: Path, bad: str) -> None:
    manifest, _, _ = _files(tmp_path)
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["pairs"][0]["baseline"] = bad
    manifest.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(BenchmarkError, match="trace paths|trace path"):
        evaluate_manifest(manifest)


def test_rejects_symlink_escape(tmp_path: Path) -> None:
    manifest, _, _ = _files(tmp_path)
    outside = tmp_path.parent / "external-trace.json"
    outside.write_text("{}", encoding="utf-8")
    (tmp_path / "baseline.json").unlink()
    (tmp_path / "baseline.json").symlink_to(outside)
    with pytest.raises(BenchmarkError, match="escapes"):
        evaluate_manifest(manifest)


def test_rejects_empty_duplicate_and_malformed_manifest(tmp_path: Path) -> None:
    manifest, _, _ = _files(tmp_path)
    manifest.write_text('{"schema_version":1,"pairs":[]}', encoding="utf-8")
    with pytest.raises(BenchmarkError, match="manifest is invalid"):
        evaluate_manifest(manifest)

    manifest.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    with pytest.raises(BenchmarkError, match="duplicate keys"):
        evaluate_manifest(manifest)

    manifest.write_text("{", encoding="utf-8")
    with pytest.raises(BenchmarkError, match="strict JSON"):
        evaluate_manifest(manifest)


def test_rejects_duplicate_trace_keys(tmp_path: Path) -> None:
    manifest, _, _ = _files(tmp_path)
    (tmp_path / "baseline.json").write_text(
        '{"schema_version":1,"schema_version":1}', encoding="utf-8"
    )
    with pytest.raises(BenchmarkError, match="duplicate keys"):
        evaluate_manifest(manifest)


def test_rejects_missing_or_tampered_evidence(tmp_path: Path) -> None:
    manifest, baseline, candidate = _files(tmp_path)
    verification_file = tmp_path / baseline["verification"]["evidence_file"]
    verification_file.unlink()
    with pytest.raises(BenchmarkError, match="evidence is unavailable"):
        evaluate_manifest(manifest)

    verification_file.write_bytes(_evidence_bytes("baseline-run", "verification"))
    usage_file = tmp_path / candidate["model_calls"][0]["usage_evidence_file"]
    usage_file.write_bytes(b"changed after trace export")
    with pytest.raises(BenchmarkError, match="hash mismatch"):
        evaluate_manifest(manifest)


def test_rejects_oversized_evidence(tmp_path: Path) -> None:
    manifest, baseline, _ = _files(tmp_path)
    evidence_file = tmp_path / baseline["verification"]["evidence_file"]
    evidence_file.write_bytes(b"x" * (16 * 1024 * 1024 + 1))
    with pytest.raises(BenchmarkError, match="evidence exceeds size limit"):
        evaluate_manifest(manifest)


def test_rejects_nonfinite_and_oversized_input(tmp_path: Path) -> None:
    manifest, _, _ = _files(tmp_path)
    manifest.write_text('{"schema_version":1,"pairs":NaN}', encoding="utf-8")
    with pytest.raises(BenchmarkError, match="non-finite"):
        evaluate_manifest(manifest)

    manifest.write_bytes(b" " * (4 * 1024 * 1024 + 1))
    with pytest.raises(BenchmarkError, match="size limit"):
        evaluate_manifest(manifest)


def test_missing_trace_fails_closed(tmp_path: Path) -> None:
    manifest, _, _ = _files(tmp_path)
    (tmp_path / "candidate.json").unlink()
    with pytest.raises(BenchmarkError, match="unavailable"):
        evaluate_manifest(manifest)


def test_module_entrypoint_prints_report_or_rejection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest, _, _ = _files(tmp_path)
    assert main([str(manifest)]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["feature_count"] == 1
    assert output.err == ""

    assert main([str(tmp_path / "missing.json")]) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert "benchmark rejected" in output.err
