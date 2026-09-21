"""Redaction must be deterministic and fail closed."""

import pytest

from orchestrator.observability.redaction import (
    PROTECTED_PATH_MARKER,
    REDACTED_MARKER,
    RedactionError,
    Redactor,
)


def test_redactor_removes_secret_values_and_protected_paths():
    redacted = Redactor(secret_values=["sk-secret"], protected_paths=["/workspace/private"])
    result = redacted.clean({"token": "sk-secret", "path": "/workspace/private/a.txt"})
    assert result == {"token": "[REDACTED]", "path": "[PROTECTED_PATH]"}


def test_markers_have_expected_text():
    assert REDACTED_MARKER == "[REDACTED]"
    assert PROTECTED_PATH_MARKER == "[PROTECTED_PATH]"


def test_secret_value_is_replaced_inside_longer_text():
    redactor = Redactor(secret_values=["sk-secret"])
    cleaned = redactor.clean("call failed using sk-secret at the gateway")
    assert "sk-secret" not in cleaned
    assert REDACTED_MARKER in cleaned


def test_redaction_recurses_into_lists_and_nested_dicts():
    redactor = Redactor(secret_values=["sk-secret"], protected_paths=["/workspace/private"])
    payload = {
        "items": ["sk-secret", {"nested": "/workspace/private/x"}],
        "count": 3,
        "flag": True,
        "empty": None,
    }
    result = redactor.clean(payload)
    assert result == {
        "items": ["[REDACTED]", {"nested": "[PROTECTED_PATH]"}],
        "count": 3,
        "flag": True,
        "empty": None,
    }


def test_sensitive_key_names_are_redacted_regardless_of_value():
    redactor = Redactor()
    result = redactor.clean(
        {
            "Authorization": "Bearer abc.def",
            "X-Api-Key": "anything",
            "password": "hunter2",
            "cookie": "session=1",
        }
    )
    assert result == {
        "Authorization": "[REDACTED]",
        "X-Api-Key": "[REDACTED]",
        "password": "[REDACTED]",
        "cookie": "[REDACTED]",
    }


def test_token_count_keys_are_not_treated_as_secrets():
    redactor = Redactor()
    result = redactor.clean({"reserved_tokens": 100, "input_tokens": 42})
    assert result == {"reserved_tokens": 100, "input_tokens": 42}


def test_exception_text_is_redacted():
    redactor = Redactor(secret_values=["sk-secret"])
    cleaned = redactor.clean(RuntimeError("boom with sk-secret token"))
    assert "sk-secret" not in cleaned
    assert cleaned.startswith("RuntimeError")


def test_unsupported_type_raises_redaction_error():
    redactor = Redactor()

    class Opaque:
        pass

    with pytest.raises(RedactionError):
        redactor.clean({"obj": Opaque()})


def test_redactor_exposes_a_version():
    assert isinstance(Redactor().redaction_version, int)
    assert Redactor().redaction_version >= 1


# --- ObservationSink: redacted, structured-only intake -----------------------

from orchestrator.observability.models import LogRecord, MetricSample, TraceSpan
from orchestrator.observability.sink import ObservationSink


def _sink():
    redactor = Redactor(secret_values=["sk-secret"], protected_paths=["/workspace/private"])
    return ObservationSink(redactor=redactor)


def test_sink_redacts_log_fields_and_message():
    sink = _sink()
    sink.emit_log(
        LogRecord(
            level="INFO",
            message="dispatch used sk-secret",
            component="gateway",
            fields={"authorization": "Bearer x", "path": "/workspace/private/a"},
        )
    )
    stored = sink.logs[0]
    blob = repr(stored.model_dump())
    assert "sk-secret" not in blob
    assert "/workspace/private" not in blob
    assert stored.fields["authorization"] == "[REDACTED]"


def test_sink_redacts_span_attributes():
    sink = _sink()
    sink.emit_span(
        TraceSpan(
            name="model_gateway",
            span_id="s1",
            trace_id="t1",
            attributes={"api_key": "sk-secret", "note": "/workspace/private/x"},
        )
    )
    blob = repr(sink.spans[0].model_dump())
    assert "sk-secret" not in blob
    assert "/workspace/private" not in blob


def test_sink_redacts_metric_labels():
    sink = _sink()
    sink.emit_metric(
        MetricSample(name="model.calls", value=1, labels={"secret": "sk-secret"})
    )
    assert sink.metrics[0].labels["secret"] == "[REDACTED]"


def test_sink_rejects_untyped_observations():
    sink = _sink()
    with pytest.raises(TypeError):
        sink.emit_log({"level": "INFO", "message": "not a LogRecord"})


def test_sink_fails_closed_when_redaction_fails_and_stores_nothing():
    sink = _sink()

    class Opaque:
        pass

    with pytest.raises(RedactionError):
        sink.emit_log(
            LogRecord(level="INFO", message="x", component="c", fields={"bad": Opaque()})
        )
    assert sink.logs == ()
