"""Redacted observations and read-only, version-aware projections.

The observability plane is entirely derived from the authoritative event
stream.  Nothing here writes events or makes scheduling decisions; the sink and
projections only redact, retain, and expose observations.
"""

from .models import LogLevel, LogRecord, MetricSample, TraceSpan
from .projections import (
    ApprovalProjection,
    ApprovalView,
    AuditProjection,
    AuditView,
    BudgetProjection,
    BudgetView,
    CostProjection,
    CostView,
    ProjectionError,
    RunProjection,
    RunView,
)
from .redaction import (
    PROTECTED_PATH_MARKER,
    REDACTED_MARKER,
    REDACTION_VERSION,
    RedactionError,
    Redactor,
)
from .sink import ObservationSink

__all__ = [
    "ApprovalProjection",
    "ApprovalView",
    "AuditProjection",
    "AuditView",
    "BudgetProjection",
    "BudgetView",
    "CostProjection",
    "CostView",
    "LogLevel",
    "LogRecord",
    "MetricSample",
    "ObservationSink",
    "PROTECTED_PATH_MARKER",
    "ProjectionError",
    "REDACTED_MARKER",
    "REDACTION_VERSION",
    "RedactionError",
    "Redactor",
    "RunProjection",
    "RunView",
    "TraceSpan",
]
