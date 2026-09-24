"""Attempt-bound, endpoint-bound and fail-closed provider credential broker.

The broker resolves only explicit secret references and records a redacted
access decision before returning an ephemeral credential to Model Gateway.
Workers and tools do not receive this object or the value it contains.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
import os
import re
from typing import Protocol

from orchestrator.config.models import ProviderSpec
from orchestrator.models.gateway import SecretAccessContext
from orchestrator.models.transport import ProviderCredential
from orchestrator.persistence.events import EventDraft, StoredEvent
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$")
_ENV_REF = re.compile(r"^env:([A-Za-z_][A-Za-z0-9_]*)$")
_HEADER_BY_ADAPTER = {
    "openai_responses": "authorization",
    "anthropic_messages": "x-api-key",
    "openai_compatible": "authorization",
}


@dataclass(frozen=True, slots=True)
class SecretAccessRule:
    """Immutable permit for one provider/ref/endpoint/purpose and Run set."""

    provider_id: str
    secret_ref: str
    endpoint: str
    purpose: str
    allowed_run_ids: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.provider_id, str) or not _IDENTIFIER.fullmatch(self.provider_id):
            raise ValueError("SecretAccessRule requires a valid provider_id")
        if not isinstance(self.secret_ref, str) or not self.secret_ref.startswith(("env:", "keyring:", "plugin:")):
            raise ValueError("SecretAccessRule requires a secret reference, not secret material")
        if not isinstance(self.endpoint, str) or not self.endpoint.startswith("https://"):
            raise ValueError("SecretAccessRule requires an HTTPS endpoint")
        if self.purpose != "model_inference":
            raise ValueError("only the model_inference secret purpose is supported")
        if not isinstance(self.allowed_run_ids, frozenset) or not self.allowed_run_ids:
            raise ValueError("SecretAccessRule must name one or more allowed Runs")
        if any(not isinstance(run_id, str) or not _IDENTIFIER.fullmatch(run_id) for run_id in self.allowed_run_ids):
            raise ValueError("SecretAccessRule contains an invalid Run id")


class SecretValueStore(Protocol):
    """Trusted host secret source; values must not be logged or serialized."""

    def read(self, secret_ref: str) -> str | None: ...


class EnvironmentSecretStore:
    """Resolve an explicit env reference only when allowlisted by the host."""

    def __init__(
        self,
        *,
        allowed_secret_refs: Iterable[str],
        environ: Mapping[str, str] | None = None,
    ) -> None:
        refs = frozenset(allowed_secret_refs)
        if any(not isinstance(ref, str) or _ENV_REF.fullmatch(ref) is None for ref in refs):
            raise ValueError("EnvironmentSecretStore accepts only explicit env: references")
        self._allowed_refs = refs
        self._environ = os.environ if environ is None else environ

    def read(self, secret_ref: str) -> str | None:
        if secret_ref not in self._allowed_refs:
            return None
        match = _ENV_REF.fullmatch(secret_ref)
        if match is None:
            return None
        value = self._environ.get(match.group(1))
        return value if isinstance(value, str) and value else None

    def __repr__(self) -> str:
        return f"EnvironmentSecretStore(allowlisted_refs={len(self._allowed_refs)})"


class SecretAccessDenied(RuntimeError):
    """Secret access was rejected without exposing its value or source detail."""


class SecretBrokerUnavailable(RuntimeError):
    """Audit or backing-store failure prevented a safe secret decision."""


class AuditedSecretBroker:
    """Return a scoped credential only after allowlist and durable audit checks."""

    def __init__(
        self,
        *,
        event_store: SQLiteEventStore,
        value_store: SecretValueStore,
        rules: Iterable[SecretAccessRule],
    ) -> None:
        materialized_rules = tuple(rules)
        if not materialized_rules:
            raise ValueError("Secret Broker requires at least one explicit access rule")
        if any(not isinstance(rule, SecretAccessRule) for rule in materialized_rules):
            raise TypeError("Secret Broker rules must be SecretAccessRule values")
        self._events = event_store
        self._values = value_store
        self._rules = materialized_rules

    async def acquire_provider_credential(
        self,
        *,
        secret_ref: str,
        provider: ProviderSpec,
        endpoint: str,
        purpose: str,
        context: SecretAccessContext,
    ) -> ProviderCredential | None:
        if not isinstance(context, SecretAccessContext):
            raise SecretAccessDenied("a validated Model Gateway access context is required")
        valid_request = (
            isinstance(provider, ProviderSpec)
            and provider.enabled
            and provider.secret_ref == secret_ref
            and provider.effective_endpoint == endpoint
            and context.fencing_generation >= 0
            and _IDENTIFIER.fullmatch(context.accepted_route_id) is not None
            and purpose == "model_inference"
        )
        rule = next(
            (
                candidate
                for candidate in self._rules
                if valid_request
                and candidate.provider_id == provider.id
                and candidate.secret_ref == secret_ref
                and candidate.endpoint == endpoint
                and candidate.purpose == purpose
                and context.run_id in candidate.allowed_run_ids
            ),
            None,
        )
        self._audit(
            context,
            provider_id=getattr(provider, "id", "unknown"),
            secret_ref=secret_ref,
            endpoint=endpoint,
            purpose=purpose,
            allowed=rule is not None,
            reason="authorized" if rule is not None else "scope_denied",
        )
        if rule is None:
            return None
        assert isinstance(provider, ProviderSpec)
        try:
            secret = self._values.read(secret_ref)
        except Exception as exc:
            self._audit_outcome(
                context,
                provider_id=provider.id,
                secret_ref=secret_ref,
                endpoint=endpoint,
                purpose=purpose,
                reason="credential_unavailable",
            )
            raise SecretBrokerUnavailable("credential store unavailable") from exc
        if not isinstance(secret, str) or not secret or len(secret) > 4_096 or _has_header_controls(secret):
            self._audit_outcome(
                context,
                provider_id=provider.id,
                secret_ref=secret_ref,
                endpoint=endpoint,
                purpose=purpose,
                reason="credential_unavailable",
            )
            return None
        return ProviderCredential(
            header_name=_HEADER_BY_ADAPTER[provider.adapter],
            value=secret,
            provider_id=provider.id,
            endpoint=endpoint,
            purpose=purpose,
        )

    def _audit(
        self,
        context: SecretAccessContext,
        *,
        provider_id: str,
        secret_ref: str,
        endpoint: str,
        purpose: str,
        allowed: bool,
        reason: str,
    ) -> None:
        event_type = "SecretAccessGranted" if allowed else "SecretAccessDenied"
        self._append_audit(
            context,
            event_type=event_type,
            idempotency_key=f"secret-access:{context.request_id}",
            provider_id=provider_id,
            secret_ref=secret_ref,
            endpoint=endpoint,
            purpose=purpose,
            reason=reason,
        )

    def _audit_outcome(
        self,
        context: SecretAccessContext,
        *,
        provider_id: str,
        secret_ref: str,
        endpoint: str,
        purpose: str,
        reason: str,
    ) -> None:
        self._append_audit(
            context,
            event_type="SecretCredentialUnavailable",
            idempotency_key=f"secret-result:{context.request_id}",
            provider_id=provider_id,
            secret_ref=secret_ref,
            endpoint=endpoint,
            purpose=purpose,
            reason=reason,
        )

    def _append_audit(
        self,
        context: SecretAccessContext,
        *,
        event_type: str,
        idempotency_key: str,
        provider_id: str,
        secret_ref: str,
        endpoint: str,
        purpose: str,
        reason: str,
    ) -> None:
        payload = {
            "request_id": context.request_id,
            "provider_id": provider_id if isinstance(provider_id, str) and _IDENTIFIER.fullmatch(provider_id) else "unknown",
            "secret_ref_hash": _hash(secret_ref),
            "endpoint_hash": _hash(endpoint),
            "purpose": purpose if purpose == "model_inference" else "unsupported",
            "accepted_route_id": context.accepted_route_id,
            "budget_reservation_id": context.budget_reservation_id,
            "reason_code": reason,
        }

        def decide(events: list[StoredEvent], _version: int) -> list[EventDraft]:
            if any(event.idempotency_key == idempotency_key for event in events):
                raise SecretAccessDenied("secret access request id was already used")
            return [
                EventDraft(
                    event_type,
                    payload,
                    run_id=context.run_id,
                    node_id=context.node_id,
                    attempt_id=context.attempt_id,
                    fencing_generation=context.fencing_generation,
                    causation_id=context.accepted_route_id,
                )
            ]

        try:
            self._events.append_checked(
                "security",
                context.run_id,
                idempotency_key,
                decide,
            )
        except SecretAccessDenied:
            raise
        except Exception as exc:
            raise SecretBrokerUnavailable("secret access could not be durably audited") from exc


def _hash(value: object) -> str:
    encoded = value.encode("utf-8") if isinstance(value, str) else b"<invalid>"
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _has_header_controls(value: str) -> bool:
    return not value.isascii() or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)


__all__ = [
    "AuditedSecretBroker",
    "EnvironmentSecretStore",
    "SecretAccessDenied",
    "SecretAccessRule",
    "SecretBrokerUnavailable",
    "SecretValueStore",
]
