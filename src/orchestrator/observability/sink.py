"""The ObservationSink: the only intake for logs, spans, and metrics.

The sink accepts validated :class:`LogRecord`, :class:`TraceSpan`, and
:class:`MetricSample` objects, redacts their free-form content, and retains the
redacted copies.  It is fail closed: if redaction raises, the observation is
dropped and the error propagates so the caller aborts the write.  The sink is a
derived view — it holds no event-store handle and cannot influence scheduling.
"""

from __future__ import annotations

from typing import Any

from .models import LogRecord, MetricSample, TraceSpan
from .redaction import RedactionError, Redactor


class ObservationSink:
    """Collects redacted observations in memory behind read-only accessors."""

    def __init__(self, redactor: Redactor | None = None) -> None:
        self._redactor = redactor if redactor is not None else Redactor()
        self._logs: list[LogRecord] = []
        self._spans: list[TraceSpan] = []
        self._metrics: list[MetricSample] = []

    @property
    def logs(self) -> tuple[LogRecord, ...]:
        return tuple(self._logs)

    @property
    def spans(self) -> tuple[TraceSpan, ...]:
        return tuple(self._spans)

    @property
    def metrics(self) -> tuple[MetricSample, ...]:
        return tuple(self._metrics)

    def emit_log(self, record: LogRecord) -> LogRecord:
        if not isinstance(record, LogRecord):
            raise TypeError("emit_log requires a LogRecord")
        cleaned = record.model_copy(
            update={
                "message": self._redactor.clean_text(record.message),
                "error_type": self._clean_optional_text(record.error_type),
                "fields": self._clean_mapping(record.fields),
            }
        )
        self._logs.append(cleaned)
        return cleaned

    def emit_span(self, span: TraceSpan) -> TraceSpan:
        if not isinstance(span, TraceSpan):
            raise TypeError("emit_span requires a TraceSpan")
        cleaned = span.model_copy(
            update={"attributes": self._clean_mapping(span.attributes)}
        )
        self._spans.append(cleaned)
        return cleaned

    def emit_metric(self, sample: MetricSample) -> MetricSample:
        if not isinstance(sample, MetricSample):
            raise TypeError("emit_metric requires a MetricSample")
        cleaned = sample.model_copy(
            update={"labels": self._clean_mapping(sample.labels)}
        )
        self._metrics.append(cleaned)
        return cleaned

    # -- internals ------------------------------------------------------------

    def _clean_mapping(self, mapping: dict[str, Any]) -> dict[str, Any]:
        cleaned = self._redactor.clean(mapping)
        if not isinstance(cleaned, dict):  # pragma: no cover - defensive
            raise RedactionError("redacting a mapping did not yield a mapping")
        return cleaned

    def _clean_optional_text(self, text: str | None) -> str | None:
        if text is None:
            return None
        return self._redactor.clean_text(text)


__all__ = ["ObservationSink"]
