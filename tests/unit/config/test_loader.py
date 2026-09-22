import traceback

import pytest

from orchestrator.config import (
    ConfigurationLoadError,
    load_model_registry_yaml,
    model_registry_json_schema,
)


VALID_REGISTRY = """
schema_version: 1
providers:
  - id: primary
    adapter: openai_responses
    secret_ref: env:ORCHESTRATOR_KEY
    enabled: true
models:
  - id: model-a
    provider: primary
    remote_model: example-model
    tier: standard
    capabilities: [text, tools]
    context_window: 32000
    max_output_tokens: 4000
    supported_reasoning_efforts: [low, high]
    price:
      currency: USD
      input_minor_per_million: 100
      output_minor_per_million: 200
      max_tool_cost_minor: 0
      estimator_id: tokenizer-v1
      effective_from: 2026-09-22T00:00:00Z
      expires_at: 2026-10-22T00:00:00Z
"""


def test_loads_yaml_and_produces_stable_registry_hash():
    manifest = load_model_registry_yaml(VALID_REGISTRY)

    assert manifest.providers[0].secret_ref == "env:ORCHESTRATOR_KEY"
    assert manifest.models[0].capabilities == frozenset({"text", "tools"})
    assert manifest.content_hash.startswith("sha256:")


def test_yaml_duplicate_keys_are_rejected():
    with pytest.raises(ConfigurationLoadError, match="duplicate_key"):
        load_model_registry_yaml(
            VALID_REGISTRY.replace(
                "schema_version: 1", "schema_version: 1\nschema_version: 1"
            )
        )


def test_yaml_aliases_and_unsafe_python_tags_are_rejected():
    with pytest.raises(ConfigurationLoadError, match="invalid_yaml"):
        load_model_registry_yaml("schema_version: &version 1\ncopy: *version\n")
    with pytest.raises(ConfigurationLoadError, match="invalid_yaml"):
        load_model_registry_yaml("!!python/object/apply:os.system ['echo unsafe']")


def test_validation_error_does_not_echo_a_rejected_secret_value():
    secret = "sk-super-secret-value"
    source = VALID_REGISTRY.replace("env:ORCHESTRATOR_KEY", secret)

    with pytest.raises(ConfigurationLoadError) as raised:
        load_model_registry_yaml(source)

    assert secret not in str(raised.value)
    assert secret not in "".join(traceback.format_exception(raised.value))
    assert raised.value.issues[0].path == "providers.0.secret_ref"


def test_unknown_field_names_are_not_echoed_in_validation_paths():
    secret = "sk-secret-looking-field"
    source = VALID_REGISTRY.replace(
        "    enabled: true", f"    enabled: true\n    {secret}: value"
    )

    with pytest.raises(ConfigurationLoadError) as raised:
        load_model_registry_yaml(source)

    assert secret not in str(raised.value)
    assert raised.value.issues[0].path == "providers.0.<field>"


def test_loader_rejects_multiple_documents_oversize_and_non_utf8():
    with pytest.raises(ConfigurationLoadError, match="invalid_yaml"):
        load_model_registry_yaml(VALID_REGISTRY + "\n---\nschema_version: 1\n")
    with pytest.raises(ConfigurationLoadError, match="document_too_large"):
        load_model_registry_yaml(b"a" * 1_048_577)
    with pytest.raises(ConfigurationLoadError, match="invalid_utf8"):
        load_model_registry_yaml(b"\xff")


def test_json_schema_is_available_and_forbids_unknown_fields():
    schema = model_registry_json_schema()

    assert schema["properties"]["schema_version"]["const"] == 1
    assert schema["additionalProperties"] is False
