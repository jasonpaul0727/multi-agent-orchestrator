"""Safe YAML parsing helpers for configuration boundary objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml
from pydantic import ValidationError
from yaml.events import AliasEvent
from yaml.nodes import MappingNode

from orchestrator.config.effective import (
    ConfigOverlay,
    EffectiveConfig,
    config_overlay_json_schema,
    effective_config_json_schema,
)
from orchestrator.config.models import ModelRegistryManifest


MAX_CONFIG_BYTES = 1_048_576
_SAFE_FIELD_NAMES = frozenset(
    {
        "schema_version",
        "providers",
        "models",
        "id",
        "adapter",
        "endpoint",
        "secret_ref",
        "enabled",
        "remote_model",
        "provider",
        "tier",
        "capabilities",
        "context_window",
        "max_output_tokens",
        "supported_reasoning_efforts",
        "price",
        "local_zero_cost",
        "currency",
        "input_minor_per_million",
        "output_minor_per_million",
        "max_tool_cost_minor",
        "estimator_id",
        "effective_from",
        "expires_at",
        "active_preset",
        "registry_manifest_ref",
        "policy_envelope",
        "mandatory_guard_rules",
        "presets",
        "classifier",
        "health_policies",
        "requested_budget",
        "max_cost_minor",
        "max_total_tokens",
        "max_agents",
        "max_depth",
        "max_concurrency",
        "max_parallel_candidates",
        "allowed_models",
        "allowed_providers",
        "allowed_capabilities",
        "denied_models",
        "denied_providers",
        "min_tier",
        "require_independent_review",
        "require_approval",
        "roles",
        "candidates",
        "reasoning_effort",
        "retries",
        "escalate_to",
        "selector_rules",
        "guard_rules",
        "health_policy_ref",
        "priority",
        "when",
        "select",
        "model",
        "fallback",
        "allow_degraded",
        "disabled",
        "task_class",
        "complexity",
        "risk",
        "failure_category",
        "authorized_recovery_action",
        "health_state",
        "required_capabilities_all",
        "context_tokens",
        "output_tokens",
        "retry_level",
        "minimum",
        "maximum",
        "constraints",
        "failure_window_ms",
        "degrade_after",
        "open_after",
        "recovery_successes",
        "cooldown_ms",
        "max_probe_permits",
        "version",
        "normalization_version",
        "taxonomy_version",
    }
)


@dataclass(frozen=True)
class ConfigIssue:
    """A sanitized configuration error; it never carries the rejected value."""

    path: str
    code: str


class ConfigurationLoadError(ValueError):
    def __init__(self, issues: tuple[ConfigIssue, ...]) -> None:
        self.issues = issues
        summary = "; ".join(f"{issue.path}:{issue.code}" for issue in issues)
        super().__init__(f"configuration rejected ({summary})")


class _StrictSafeLoader(yaml.SafeLoader):
    """SafeLoader variant rejecting aliases and duplicate mapping keys."""

    def compose_node(self, parent: Any, index: Any) -> Any:
        if self.check_event(AliasEvent):
            event = self.get_event()
            raise yaml.YAMLError(
                f"YAML aliases are not supported at line {event.start_mark.line + 1}"
            )
        return super().compose_node(parent, index)

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[Any, Any]:
        if not isinstance(node, MappingNode):
            raise yaml.constructor.ConstructorError(
                None, None, "expected a mapping", node.start_mark
            )
        self.flatten_mapping(node)
        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as error:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "mapping keys must be hashable",
                    key_node.start_mark,
                ) from error
            if duplicate:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "duplicate mapping key",
                    key_node.start_mark,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _source_text(source: str | bytes) -> str:
    if isinstance(source, bytes):
        if len(source) > MAX_CONFIG_BYTES:
            raise ConfigurationLoadError((ConfigIssue("$", "document_too_large"),))
        try:
            return source.decode("utf-8")
        except UnicodeDecodeError:
            raise ConfigurationLoadError((ConfigIssue("$", "invalid_utf8"),)) from None
    if not isinstance(source, str):
        raise TypeError("configuration source must be str or UTF-8 bytes")
    if len(source.encode("utf-8")) > MAX_CONFIG_BYTES:
        raise ConfigurationLoadError((ConfigIssue("$", "document_too_large"),))
    return source


def _safe_path(location: tuple[object, ...]) -> str:
    """Keep schema field names and indexes; hide arbitrary user-supplied keys."""
    parts = []
    for part in location:
        if isinstance(part, int):
            parts.append(str(part))
        elif isinstance(part, str) and part in _SAFE_FIELD_NAMES:
            parts.append(part)
        else:
            parts.append("<field>")
    return ".".join(parts) or "$"


def load_model_registry_yaml(source: str | bytes) -> ModelRegistryManifest:
    """Parse one strict, de-keyed Model Registry YAML document.

    Parser and validation diagnostics include only paths and error codes, not
    rejected input values that could accidentally contain credentials.
    """
    data = _load_yaml_data(source)
    return _validate_model(data, ModelRegistryManifest)


def load_effective_config_yaml(source: str | bytes) -> EffectiveConfig:
    """Parse and validate a complete EffectiveConfig YAML document."""
    return _validate_model(_load_yaml_data(source), EffectiveConfig)


def load_config_overlay_yaml(source: str | bytes) -> ConfigOverlay:
    """Parse a strict partial overlay for the user, project, or Run layer."""
    return _validate_model(_load_yaml_data(source), ConfigOverlay)


def _load_yaml_data(source: str | bytes) -> Any:
    text = _source_text(source)
    try:
        data = yaml.load(text, Loader=_StrictSafeLoader)
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        problem = getattr(error, "problem", None)
        path = (
            f"line:{mark.line + 1}:column:{mark.column + 1}"
            if mark is not None
            else "$"
        )
        code = "duplicate_key" if problem == "duplicate mapping key" else "invalid_yaml"
        raise ConfigurationLoadError((ConfigIssue(path, code),)) from None
    return data


def _validate_model(data: Any, model_type: Any) -> Any:
    try:
        return model_type.model_validate(data)
    except ValidationError as error:
        issues = tuple(
            ConfigIssue(
                _safe_path(item["loc"]),
                str(item["type"]),
            )
            for item in error.errors(include_input=False, include_context=False)
        )
        raise ConfigurationLoadError(issues) from None


def model_registry_json_schema() -> dict[str, Any]:
    """Return the JSON Schema for the strict Model Registry manifest."""
    return ModelRegistryManifest.model_json_schema()


def effective_config_schema() -> dict[str, Any]:
    """Return the JSON Schema for a complete effective manifest."""
    return effective_config_json_schema()


def config_overlay_schema() -> dict[str, Any]:
    """Return the JSON Schema for a partial config overlay."""
    return config_overlay_json_schema()


__all__ = [
    "ConfigIssue",
    "ConfigurationLoadError",
    "MAX_CONFIG_BYTES",
    "config_overlay_schema",
    "effective_config_schema",
    "load_config_overlay_yaml",
    "load_effective_config_yaml",
    "load_model_registry_yaml",
    "model_registry_json_schema",
]
