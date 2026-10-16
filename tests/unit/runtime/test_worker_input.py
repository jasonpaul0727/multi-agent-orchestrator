"""Pure input commitment behavior; no admission or Worker execution."""

from __future__ import annotations

import hashlib
import json
import traceback

import pytest
from pydantic import ValidationError

import orchestrator.runtime.worker_input as worker_input
from orchestrator.runtime.worker_input import (
    WorkerInputError,
    WorkerInputManifest,
    build_worker_input_manifest,
)
from orchestrator.validation import revalidate_model


_CANONICAL = (
    b'{"input_artifacts":[],"node_id":"node-1","role":"coder",'
    b'"run_id":"run-1","schema":"maestro.worker-input.v1",'
    b'"task_text":"implement a small change"}'
)
_HASH = "sha256:c024f0eefb7b0362f5783d560036a89c3564b1f4a656e108c655a799fbd37e2c"
_SENTINEL = "PRIVATE_INPUT_SENTINEL_9f38"


def _values(**overrides: object) -> dict[str, object]:
    values = {
        "run_id": "run-1",
        "node_id": "node-1",
        "role": "coder",
        "task_text": "implement a small change",
    }
    values.update(overrides)
    return values


def test_known_canonical_vector() -> None:
    value = build_worker_input_manifest(**_values())
    assert value.canonical_bytes == _CANONICAL
    assert value.content_hash == _HASH
    # Omitting the domain prefix would commit to a different protocol.
    assert value.content_hash != "sha256:" + hashlib.sha256(_CANONICAL).hexdigest()


@pytest.mark.parametrize("schema_key", ["schema", "schema_id"])
def test_schema_alias_and_python_name_revalidate_to_canonical_key(schema_key: str) -> None:
    value = WorkerInputManifest(**_values(**{schema_key: "maestro.worker-input.v1"}))
    for candidate in (
        value,
        revalidate_model(WorkerInputManifest, value),
        value.model_copy(update={"task_text": "implement a small change"}),
        WorkerInputManifest.model_construct(**_values(schema_id="maestro.worker-input.v1")),
    ):
        assert candidate.canonical_bytes == _CANONICAL
        assert candidate.content_hash == _HASH
        assert json.loads(candidate.canonical_bytes)["schema"] == "maestro.worker-input.v1"
        assert "schema_id" not in json.loads(candidate.canonical_bytes)


def test_reordered_input_mapping_has_same_commitment() -> None:
    values = _values(schema="maestro.worker-input.v1", input_artifacts=[])
    value = WorkerInputManifest.model_validate(dict(reversed(tuple(values.items()))))
    assert value.canonical_bytes == _CANONICAL
    assert value.content_hash == _HASH


@pytest.mark.parametrize(
    "overrides",
    [
        {"task_text": ""}, {"task_text": " \t\r\n"}, {"task_text": "\u2003"},
        {"task_text": None}, {"task_text": 1}, {"task_text": True}, {"task_text": b"text"},
        {"task_text": "x" * 100_001}, {"task_text": "\ud800"},
        *[
            {field: value}
            for field in ("run_id", "node_id")
            for value in ("", "bad id", "_bad", "é", "a\n", "a" * 129, None, 1, True, b"id")
        ],
        {"role": "unknown"}, {"role": "Coder"}, {"role": None},
        {"role": 1}, {"role": b"coder"},
    ],
)
def test_builder_rejects_invalid_inputs_with_stable_error(overrides: dict[str, object]) -> None:
    with pytest.raises(WorkerInputError) as caught:
        build_worker_input_manifest(**_values(**overrides))
    assert caught.value.code == "worker_input_invalid"
    assert str(caught.value) == "worker_input_invalid"


@pytest.mark.parametrize("property_name", ["canonical_bytes", "content_hash"])
@pytest.mark.parametrize("construction", ["copy", "construct"])
@pytest.mark.parametrize(
    "overrides",
    [
        {"task_text": ""}, {"task_text": " \n"}, {"task_text": None},
        {"task_text": "x" * 100_001}, {"task_text": "\ud800"},
        {"run_id": "bad id"}, {"node_id": 1}, {"role": "unknown"},
        {"schema_id": "maestro.worker-input.v2"}, {"schema_id": None},
        {"input_artifacts": ({"digest": "not-permitted"},)},
    ],
)
def test_properties_revalidate_unsafe_models(
    property_name: str, construction: str, overrides: dict[str, object]
) -> None:
    if construction == "copy":
        value = WorkerInputManifest(**_values()).model_copy(update=overrides)
    else:
        value = WorkerInputManifest.model_construct(**_values(**overrides))
    with pytest.raises(WorkerInputError) as caught:
        getattr(value, property_name)
    assert caught.value.code == "worker_input_invalid"


@pytest.mark.parametrize("property_name", ["canonical_bytes", "content_hash"])
@pytest.mark.parametrize("extra_location", ["copy", "model_extra", "malformed_extra"])
def test_properties_reject_surviving_unsafe_extra_fields(
    property_name: str, extra_location: str
) -> None:
    value = WorkerInputManifest(**_values())
    if extra_location == "copy":
        value = value.model_copy(update={"unknown_field": _SENTINEL})
    elif extra_location == "model_extra":
        object.__setattr__(value, "__pydantic_extra__", {"unknown_field": _SENTINEL})
    else:
        object.__setattr__(value, "__pydantic_extra__", [_SENTINEL])
    with pytest.raises(WorkerInputError) as caught:
        getattr(value, property_name)
    assert caught.value.code == "worker_input_invalid"
    assert _SENTINEL not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    "overrides",
    [
        {"input_artifacts": ["artifact"]}, {"input_artifacts": ("artifact",)},
        {"input_artifacts": None}, {"input_artifacts": ""},
        {"schema": "maestro.worker-input.v2"}, {"schema_id": "maestro.worker-input.v2"},
        {"schema": None}, {"schema": 1}, {"schema": b"maestro.worker-input.v1"},
        {"unknown_field": _SENTINEL}, {"tool_capabilities": ()},
        {"schema": "maestro.worker-input.v1", "schema_id": "maestro.worker-input.v1"},
    ],
)
def test_raw_manifest_rejects_schema_artifacts_and_extra_fields(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        WorkerInputManifest.model_validate(_values(**overrides))


@pytest.mark.parametrize("role", [
    "planner", "coder", "document_analyst", "researcher", "tester", "reviewer", "director"
])
def test_all_authoritative_roles_are_committed(role: str) -> None:
    value = build_worker_input_manifest(**_values(role=role))
    assert json.loads(value.canonical_bytes)["role"] == role


@pytest.mark.parametrize("field", ["run_id", "node_id"])
def test_identifier_boundaries_and_ascii_punctuation_are_valid(field: str) -> None:
    for identifier in ("a", "a" * 128, "A0._:/@+-"):
        value = build_worker_input_manifest(**_values(**{field: identifier}))
        assert json.loads(value.canonical_bytes)[field] == identifier


@pytest.mark.parametrize(
    "left,right",
    [
        ("text", " text"), ("text", "text "), ("Text", "text"),
        ("line\r\nnext", "line\nnext"), ("é", "e\u0301"),
    ],
)
def test_exact_text_differences_change_commitment(left: str, right: str) -> None:
    first = build_worker_input_manifest(**_values(task_text=left))
    second = build_worker_input_manifest(**_values(task_text=right))
    assert first.content_hash != second.content_hash
    assert json.loads(first.canonical_bytes)["task_text"] == left
    assert json.loads(second.canonical_bytes)["task_text"] == right


@pytest.mark.parametrize("overrides", [
    {"run_id": "run-2"}, {"node_id": "node-2"}, {"role": "reviewer"},
])
def test_identity_and_role_changes_separate_commitments(overrides: dict[str, object]) -> None:
    assert build_worker_input_manifest(**_values(**overrides)).content_hash != _HASH


def test_maximum_valid_text_is_encoded_as_exact_utf8() -> None:
    text = "🙂" * 100_000
    value = build_worker_input_manifest(**_values(task_text=text))
    assert json.loads(value.canonical_bytes)["task_text"] == text
    assert text.encode("utf-8") in value.canonical_bytes
    assert len(value.canonical_bytes) < 1_048_576


@pytest.mark.parametrize("operation", ["builder", "canonical_bytes", "content_hash"])
def test_frame_bound_rejects_one_byte_over_and_accepts_exact_bound(monkeypatch, operation: str) -> None:
    value = build_worker_input_manifest(**_values())
    monkeypatch.setattr(worker_input, "MAX_WORKER_INPUT_FRAME_BYTES", len(_CANONICAL))
    assert value.canonical_bytes == _CANONICAL
    assert value.content_hash == _HASH
    monkeypatch.setattr(worker_input, "MAX_WORKER_INPUT_FRAME_BYTES", len(_CANONICAL) - 1)
    with pytest.raises(WorkerInputError) as caught:
        if operation == "builder":
            build_worker_input_manifest(**_values())
        else:
            getattr(value, operation)
    assert caught.value.code == "worker_input_too_large"


def test_manifest_repr_omits_task_text_and_fields_are_frozen() -> None:
    value = build_worker_input_manifest(**_values(task_text=_SENTINEL))
    assert _SENTINEL not in repr(value)
    assert _SENTINEL not in str(value)
    with pytest.raises(ValidationError):
        value.task_text = "replacement"


@pytest.mark.parametrize("operation", ["builder", "canonical_bytes", "content_hash"])
def test_public_errors_omit_rejected_text_and_exception_chain(operation: str) -> None:
    with pytest.raises(WorkerInputError) as caught:
        if operation == "builder":
            build_worker_input_manifest(**_values(run_id=_SENTINEL + " "))
        else:
            value = WorkerInputManifest(**_values()).model_copy(update={"role": _SENTINEL})
            getattr(value, operation)
    assert _SENTINEL not in repr(caught.value)
    assert _SENTINEL not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__


def test_hash_failure_is_sanitized(monkeypatch) -> None:
    value = build_worker_input_manifest(**_values())

    def unavailable_digest(_payload: bytes):
        raise RuntimeError(_SENTINEL)

    # Fault injection into a pure digest has no side effects to preserve.
    monkeypatch.setattr(worker_input.hashlib, "sha256", unavailable_digest)
    with pytest.raises(WorkerInputError) as caught:
        value.content_hash
    assert caught.value.code == "worker_input_invalid"
    assert _SENTINEL not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__


def test_runtime_exports_the_pure_manifest_api() -> None:
    from orchestrator.runtime import WorkerInputError as exported_error
    from orchestrator.runtime import WorkerInputManifest as exported_manifest
    from orchestrator.runtime import build_worker_input_manifest as exported_builder

    assert exported_error is WorkerInputError
    assert exported_manifest is WorkerInputManifest
    assert exported_builder(**_values()).canonical_bytes == _CANONICAL


def _assert_safe_error_object(error: WorkerInputError, code: str) -> None:
    """Inspect error data itself; formatted suppression does not detach context."""
    assert error.code == code
    assert error.args == (code,)
    seen = set()
    pending = [error]
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        assert _SENTINEL not in repr(item.args)
        assert _SENTINEL not in repr(vars(item))
        if isinstance(item, ValidationError):
            assert _SENTINEL not in repr(item.errors())
        pending.extend(link for link in (item.__context__, item.__cause__) if link is not None)
    assert error.__context__ is None
    assert error.__cause__ is None
    assert str(error) == code
    assert _SENTINEL not in "".join(traceback.format_exception(error))


@pytest.mark.parametrize("operation", ["builder", "canonical_bytes", "content_hash"])
@pytest.mark.parametrize("construction", ["copy", "construct"])
@pytest.mark.parametrize("invalidity", ["surrogate", "validation"])
def test_guarded_errors_detach_raw_validation_and_encoding_context(
    operation, construction, invalidity,
):
    text = _SENTINEL + "\ud800"
    if invalidity == "validation":
        text += "x" * 100_001
    with pytest.raises(WorkerInputError) as caught:
        if operation == "builder":
            build_worker_input_manifest(**_values(task_text=text))
        else:
            value = (WorkerInputManifest(**_values()).model_copy(update={"task_text": text})
                     if construction == "copy" else
                     WorkerInputManifest.model_construct(**_values(task_text=text)))
            getattr(value, operation)
    _assert_safe_error_object(caught.value, "worker_input_invalid")


@pytest.mark.parametrize("operation", ["builder", "canonical_bytes", "content_hash"])
@pytest.mark.parametrize("fault", ["opaque-code", "code-object", "raw-args", "known-code-cause"])
def test_guarded_errors_allowlist_internal_codes_and_detach_injected_faults(
    monkeypatch, operation, fault,
):
    value = WorkerInputManifest(**_values())

    def fail(_value):
        if fault == "known-code-cause":
            raise WorkerInputError("worker_input_too_large") from RuntimeError(_SENTINEL)
        if fault == "raw-args":
            raise RuntimeError(_SENTINEL)
        raise WorkerInputError(_SENTINEL if fault == "opaque-code" else {_SENTINEL: "private"})

    if operation == "builder":
        monkeypatch.setattr(WorkerInputManifest, "canonical_bytes", property(fail))
    elif operation == "canonical_bytes":
        def fail_revalidation(_model, _value):
            return fail(_value)
        monkeypatch.setattr(worker_input, "revalidate_model", fail_revalidation)
    else:
        monkeypatch.setattr(worker_input.hashlib, "sha256", fail)
    with pytest.raises(WorkerInputError) as caught:
        build_worker_input_manifest(**_values()) if operation == "builder" else getattr(value, operation)
    _assert_safe_error_object(caught.value, (
        "worker_input_too_large" if fault == "known-code-cause" else "worker_input_invalid"))


@pytest.mark.parametrize("operation", ["builder", "canonical_bytes", "content_hash"])
def test_too_large_guard_errors_remain_safe_detached_and_stable(monkeypatch, operation):
    value = WorkerInputManifest(**_values())
    monkeypatch.setattr(worker_input, "MAX_WORKER_INPUT_FRAME_BYTES", 1)
    with pytest.raises(WorkerInputError) as caught:
        build_worker_input_manifest(**_values()) if operation == "builder" else getattr(value, operation)
    _assert_safe_error_object(caught.value, "worker_input_too_large")
