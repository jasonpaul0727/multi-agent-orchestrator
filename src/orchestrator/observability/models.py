"""Structured observation objects accepted by the ObservationSink.

The sink only accepts these validated objects; it never accepts loose dicts.
Free-form content lives in the ``fields`` / ``attributes`` / ``labels`` maps and
in ``message`` / ``error_type`` — those are the parts the sink runs through the
redactor.  Structural identifiers (levels, names, ids) are developer-controlled
constants and are stored as given.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _Observation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class LogRecord(_Observation):
    """A structured log line.  Never a source of truth, only an observation."""

    level: LogLevel
    message: StrictStr = Field(min_length=1)
    component: StrictStr = Field(default="orchestrator", min_length=1)
    occurred_at: datetime = Field(default_factory=_utcnow)

    run_id: StrictStr | None = None
    node_id: StrictStr | None = None
    attempt_id: StrictStr | None = None
    agent_id: StrictStr | None = None

    provider: StrictStr | None = None
    model: StrictStr | None = None
    role: StrictStr | None = None
    preset: StrictStr | None = None
    task_class: StrictStr | None = None

    correlation_id: StrictStr | None = None
    causation_id: StrictStr | None = None
    fencing_generation: int | None = None
    event_version: int | None = None

    error_type: StrictStr | None = None
    external_request_id: StrictStr | None = None

    fields: dict[str, Any] = Field(default_factory=dict)

    @field_validator("fencing_generation", "event_version")
    @classmethod
    def _non_negative(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("version and generation counters cannot be negative")
        return value


class TraceSpan(_Observation):
    """A span in a Run's trace.  Holds only low-sensitivity metadata."""

    name: StrictStr = Field(min_length=1)
    span_id: StrictStr = Field(min_length=1)
    trace_id: StrictStr = Field(min_length=1)
    parent_span_id: StrictStr | None = None

    run_id: StrictStr | None = None
    node_id: StrictStr | None = None
    attempt_id: StrictStr | None = None

    started_at: datetime = Field(default_factory=_utcnow)
    ended_at: datetime | None = None

    attributes: dict[str, Any] = Field(default_factory=dict)
    event_refs: list[str] = Field(default_factory=list)


class MetricSample(_Observation):
    """A single metric sample derived from events or runtime records."""

    name: StrictStr = Field(min_length=1)
    value: float
    unit: StrictStr | None = None
    occurred_at: datetime = Field(default_factory=_utcnow)
    labels: dict[str, Any] = Field(default_factory=dict)


__all__ = ["LogLevel", "LogRecord", "MetricSample", "TraceSpan"]
