"""Strict offline evaluation of paired recorded runs.

This module measures supplied observations.  It neither creates provider
traffic nor asserts that a trace was produced by a trusted exporter.  Source
digests make a report reproducible; provenance and experimental controls must
still be reviewed before publishing performance claims.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator


MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
MAX_EVIDENCE_BYTES = 16 * 1024 * 1024
_PERCENT = Decimal("0.01")


class BenchmarkError(ValueError):
    """The recorded input cannot support a comparable performance claim."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Verification(_StrictModel):
    verifier_id: StrictStr = Field(min_length=1)
    evidence_file: StrictStr = Field(min_length=1)
    evidence_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    accepted: StrictBool


class AttemptObservation(_StrictModel):
    attempt_id: StrictStr = Field(min_length=1)
    sequence: StrictInt = Field(ge=0)
    outcome: Literal["succeeded", "failed", "cancelled", "unknown"]
    completed_work_keys: tuple[StrictStr, ...] = Field(strict=False)

    @model_validator(mode="after")
    def unique_work_keys(self) -> "AttemptObservation":
        if any(not key.strip() for key in self.completed_work_keys):
            raise ValueError("completed work keys must be non-blank")
        if len(self.completed_work_keys) != len(set(self.completed_work_keys)):
            raise ValueError("completed work keys must be unique within an attempt")
        return self


class ModelCallObservation(_StrictModel):
    call_id: StrictStr = Field(min_length=1)
    attempt_id: StrictStr = Field(min_length=1)
    usage_record_id: StrictStr = Field(min_length=1)
    usage_evidence_file: StrictStr = Field(min_length=1)
    usage_evidence_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    cost_record_id: StrictStr = Field(min_length=1)
    cost_evidence_file: StrictStr = Field(min_length=1)
    cost_evidence_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    usage_source: Literal["provider_reported", "local_measured"]
    cost_source: Literal["provider_invoice", "settled_ledger", "measured_local"]
    input_tokens: StrictInt = Field(ge=0)
    output_tokens: StrictInt = Field(ge=0)
    reasoning_tokens: StrictInt | None = Field(default=None, ge=0)
    cached_input_tokens: StrictInt | None = Field(default=None, ge=0)
    cost_minor: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def source_pair(self) -> "ModelCallObservation":
        if (self.usage_source == "local_measured") != (self.cost_source == "measured_local"):
            raise ValueError("local usage and local cost must be paired")
        return self


class RecordedRun(_StrictModel):
    schema_version: Literal[1]
    feature_id: StrictStr = Field(min_length=1)
    workload_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    protocol_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    run_id: StrictStr = Field(min_length=1)
    currency: StrictStr = Field(pattern=r"^[A-Z]{3}$")
    status: Literal["succeeded", "failed", "cancelled", "unknown"]
    verification: Verification
    attempts: tuple[AttemptObservation, ...] = Field(min_length=1, strict=False)
    model_calls: tuple[ModelCallObservation, ...] = Field(min_length=1, strict=False)

    @model_validator(mode="after")
    def consistent_events(self) -> "RecordedRun":
        attempt_ids = [attempt.attempt_id for attempt in self.attempts]
        if len(attempt_ids) != len(set(attempt_ids)):
            raise ValueError("attempt IDs must be unique")
        sequences = [attempt.sequence for attempt in self.attempts]
        if sequences != sorted(set(sequences)):
            raise ValueError("attempt sequences must be strictly increasing")
        if len({call.call_id for call in self.model_calls}) != len(self.model_calls):
            raise ValueError("model call IDs must be unique")
        if len({call.usage_record_id for call in self.model_calls}) != len(self.model_calls):
            raise ValueError("usage record IDs must be unique")
        if len({call.cost_record_id for call in self.model_calls}) != len(self.model_calls):
            raise ValueError("cost record IDs must be unique")
        known_attempts = set(attempt_ids)
        if any(call.attempt_id not in known_attempts for call in self.model_calls):
            raise ValueError("model calls must reference a recorded attempt")
        if not any(attempt.completed_work_keys for attempt in self.attempts):
            raise ValueError("a run must contain measured completed work")
        return self


class PairSpec(_StrictModel):
    feature_id: StrictStr = Field(min_length=1)
    baseline: StrictStr = Field(min_length=1)
    candidate: StrictStr = Field(min_length=1)


class BenchmarkManifest(_StrictModel):
    schema_version: Literal[1]
    pairs: tuple[PairSpec, ...] = Field(min_length=1, max_length=1000, strict=False)

    @model_validator(mode="after")
    def unique_features(self) -> "BenchmarkManifest":
        features = [pair.feature_id for pair in self.pairs]
        if len(features) != len(set(features)):
            raise ValueError("feature IDs must be unique")
        return self


class RunMeasurement(_StrictModel):
    run_id: StrictStr
    source_sha256: StrictStr
    cost_minor: StrictInt
    tokens: StrictInt
    failure_reexecuted_work_units: StrictInt


class PairMeasurement(_StrictModel):
    feature_id: StrictStr
    baseline: RunMeasurement
    candidate: RunMeasurement


class BenchmarkReport(_StrictModel):
    manifest_sha256: StrictStr
    currency: StrictStr
    feature_count: StrictInt
    baseline_cost_minor: StrictInt
    candidate_cost_minor: StrictInt
    cost_reduction_percent: StrictStr
    baseline_tokens: StrictInt
    candidate_tokens: StrictInt
    token_reduction_percent: StrictStr
    baseline_failure_reexecuted_work_units: StrictInt
    candidate_failure_reexecuted_work_units: StrictInt
    failure_rework_reduction_percent: StrictStr
    pairs: tuple[PairMeasurement, ...]


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BenchmarkError("JSON contains duplicate keys")
        result[key] = value
    return result


def _reject_constant(_: str) -> None:
    raise BenchmarkError("non-finite JSON number")


def _load_json(path: Path) -> tuple[Any, str]:
    try:
        with path.open("rb") as stream:
            data = stream.read(MAX_DOCUMENT_BYTES + 1)
    except OSError as error:
        raise BenchmarkError("benchmark source is unavailable") from error
    if len(data) > MAX_DOCUMENT_BYTES:
        raise BenchmarkError("benchmark source exceeds size limit")
    try:
        parsed = json.loads(
            data,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except BenchmarkError:
        raise
    except (ValueError, UnicodeDecodeError) as error:
        raise BenchmarkError("benchmark source is not strict JSON") from error
    return parsed, hashlib.sha256(data).hexdigest()


def _trace_path(root: Path, name: str) -> Path:
    if "\\" in name or ":" in name:
        raise BenchmarkError("trace paths must be relative POSIX paths")
    relative = PurePosixPath(name)
    if relative.is_absolute() or any(part in ("", ".", "..") for part in name.split("/")):
        raise BenchmarkError("trace paths must remain within the manifest directory")
    path = root.joinpath(*relative.parts)
    if not path.resolve().is_relative_to(root.resolve()):
        raise BenchmarkError("trace path escapes the manifest directory")
    return path


def _verify_evidence(root: Path, name: str, expected_sha256: str) -> None:
    path = _trace_path(root, name)
    digest = hashlib.sha256()
    total = 0
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(64 * 1024):
                total += len(chunk)
                if total > MAX_EVIDENCE_BYTES:
                    raise BenchmarkError("benchmark evidence exceeds size limit")
                digest.update(chunk)
    except OSError as error:
        raise BenchmarkError("benchmark evidence is unavailable") from error
    if digest.hexdigest() != expected_sha256:
        raise BenchmarkError("benchmark evidence hash mismatch")


def _verify_run_evidence(root: Path, run: RecordedRun) -> None:
    _verify_evidence(
        root, run.verification.evidence_file, run.verification.evidence_sha256
    )
    for call in run.model_calls:
        _verify_evidence(root, call.usage_evidence_file, call.usage_evidence_sha256)
        _verify_evidence(root, call.cost_evidence_file, call.cost_evidence_sha256)


def _measure(run: RecordedRun, source_sha256: str) -> RunMeasurement:
    failed_completed: set[str] = set()
    repeated = 0
    for attempt in run.attempts:
        repeated += sum(key in failed_completed for key in attempt.completed_work_keys)
        if attempt.outcome == "failed":
            failed_completed.update(attempt.completed_work_keys)
    return RunMeasurement(
        run_id=run.run_id,
        source_sha256=source_sha256,
        cost_minor=sum(call.cost_minor for call in run.model_calls),
        # Provider reasoning is generally a subset of output and cached input
        # a subset of input.  Never add these breakdowns a second time.
        tokens=sum(call.input_tokens + call.output_tokens for call in run.model_calls),
        failure_reexecuted_work_units=repeated,
    )


def _reduction(baseline: int, candidate: int, label: str) -> str:
    if baseline <= 0:
        raise BenchmarkError(f"baseline {label} must be positive for a reduction claim")
    percent = (Decimal(baseline - candidate) * 100 / Decimal(baseline)).quantize(
        _PERCENT, rounding=ROUND_HALF_UP
    )
    return format(percent, ".2f")


def evaluate_manifest(path: str | Path) -> BenchmarkReport:
    """Measure paired recorded traces; reject missing or incomparable evidence.

    The returned source hashes are SHA-256 of the exact input file bytes.
    Authenticity, workload selection, and independent verification are outside
    this arithmetic utility and must be established before public claims.
    """

    manifest_path = Path(path)
    try:
        manifest_data, manifest_digest = _load_json(manifest_path)
        manifest = BenchmarkManifest.model_validate(manifest_data)
    except BenchmarkError:
        raise
    except ValueError as error:
        raise BenchmarkError("benchmark manifest is invalid") from error

    measurements: list[PairMeasurement] = []
    run_ids: set[str] = set()
    currency: str | None = None
    for pair in manifest.pairs:
        loaded: list[tuple[RecordedRun, str]] = []
        for name in (pair.baseline, pair.candidate):
            data, digest = _load_json(_trace_path(manifest_path.parent, name))
            try:
                run = RecordedRun.model_validate(data)
            except ValueError as error:
                raise BenchmarkError("benchmark trace is invalid") from error
            if run.feature_id != pair.feature_id:
                raise BenchmarkError("trace feature does not match the manifest")
            if run.run_id in run_ids:
                raise BenchmarkError("run IDs must be unique across the benchmark")
            run_ids.add(run.run_id)
            if run.status != "succeeded" or not run.verification.accepted:
                raise BenchmarkError("all runs require accepted verification")
            _verify_run_evidence(manifest_path.parent, run)
            if currency is None:
                currency = run.currency
            elif run.currency != currency:
                raise BenchmarkError("all runs must use the same currency")
            loaded.append((run, digest))
        baseline, candidate = (loaded[0][0], loaded[1][0])
        if baseline.workload_sha256 != candidate.workload_sha256:
            raise BenchmarkError("paired runs have different workloads")
        if baseline.protocol_sha256 != candidate.protocol_sha256:
            raise BenchmarkError("paired runs have different benchmark protocols")
        if baseline.verification.verifier_id != candidate.verification.verifier_id:
            raise BenchmarkError("paired runs use different verifier versions")
        measurements.append(
            PairMeasurement(
                feature_id=pair.feature_id,
                baseline=_measure(*loaded[0]),
                candidate=_measure(*loaded[1]),
            )
        )

    baseline_cost = sum(pair.baseline.cost_minor for pair in measurements)
    candidate_cost = sum(pair.candidate.cost_minor for pair in measurements)
    baseline_tokens = sum(pair.baseline.tokens for pair in measurements)
    candidate_tokens = sum(pair.candidate.tokens for pair in measurements)
    baseline_rework = sum(
        pair.baseline.failure_reexecuted_work_units for pair in measurements
    )
    candidate_rework = sum(
        pair.candidate.failure_reexecuted_work_units for pair in measurements
    )
    return BenchmarkReport(
        manifest_sha256=manifest_digest,
        currency=currency or "",
        feature_count=len(measurements),
        baseline_cost_minor=baseline_cost,
        candidate_cost_minor=candidate_cost,
        cost_reduction_percent=_reduction(baseline_cost, candidate_cost, "cost"),
        baseline_tokens=baseline_tokens,
        candidate_tokens=candidate_tokens,
        token_reduction_percent=_reduction(baseline_tokens, candidate_tokens, "tokens"),
        baseline_failure_reexecuted_work_units=baseline_rework,
        candidate_failure_reexecuted_work_units=candidate_rework,
        failure_rework_reduction_percent=_reduction(
            baseline_rework, candidate_rework, "failure rework"
        ),
        pairs=tuple(measurements),
    )


__all__ = ["BenchmarkError", "BenchmarkReport", "evaluate_manifest"]
