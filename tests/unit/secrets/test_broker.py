from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from orchestrator.config.models import ProviderSpec
from orchestrator.models import SecretAccessContext
from orchestrator.persistence.sqlite_event_store import SQLiteEventStore
from orchestrator.secrets import (
    AuditedSecretBroker,
    EnvironmentSecretStore,
    SecretAccessDenied,
    SecretAccessRule,
    SecretBrokerUnavailable,
)


def _provider(*, enabled: bool = True, secret_ref: str = "env:MODEL_KEY") -> ProviderSpec:
    return ProviderSpec(
        id="primary",
        adapter="openai_responses",
        endpoint=None,
        secret_ref=secret_ref,
        enabled=enabled,
    )


def _context(request_id: str = "request-1", *, run_id: str = "run-1") -> SecretAccessContext:
    return SecretAccessContext(
        request_id=request_id,
        run_id=run_id,
        node_id="node-1",
        attempt_id="attempt-1",
        fencing_generation=1,
        accepted_route_id="decision-1",
        budget_reservation_id="reservation-1",
    )


class _ValueStore:
    def __init__(self, value: str | None = "secret-value", *, error: Exception | None = None) -> None:
        self.value = value
        self.error = error
        self.reads: list[str] = []

    def read(self, secret_ref: str) -> str | None:
        self.reads.append(secret_ref)
        if self.error is not None:
            raise self.error
        return self.value


def _broker(tmp_path, value_store: _ValueStore | None = None, *, allowed_runs=frozenset({"run-1"})):
    provider = _provider()
    events = SQLiteEventStore(tmp_path / "secrets.db")
    values = value_store or _ValueStore()
    broker = AuditedSecretBroker(
        event_store=events,
        value_store=values,
        rules=(
            SecretAccessRule(
                provider_id=provider.id,
                secret_ref=provider.secret_ref,
                endpoint=provider.effective_endpoint,
                purpose="model_inference",
                allowed_run_ids=allowed_runs,
            ),
        ),
    )
    return broker, events, values, provider


def _acquire(broker, provider, context, *, endpoint=None, purpose="model_inference"):
    return asyncio.run(
        broker.acquire_provider_credential(
            secret_ref=provider.secret_ref,
            provider=provider,
            endpoint=endpoint or provider.effective_endpoint,
            purpose=purpose,
            context=context,
        )
    )


def test_broker_returns_only_exact_attempt_scoped_credential_after_hash_audit(tmp_path):
    broker, events, values, provider = _broker(tmp_path)
    credential = _acquire(broker, provider, _context())

    assert credential is not None
    assert credential.value == "secret-value"
    assert (credential.provider_id, credential.endpoint, credential.purpose) == (
        "primary", "https://api.openai.com/v1", "model_inference"
    )
    assert "secret-value" not in repr(credential)
    assert values.reads == ["env:MODEL_KEY"]
    [event] = events.read_stream("security", "run-1")
    assert event.event_type == "SecretAccessGranted"
    assert event.attempt_id == "attempt-1"
    assert event.fencing_generation == 1
    assert event.payload["accepted_route_id"] == "decision-1"
    assert event.payload["secret_ref_hash"].startswith("sha256:")
    assert "secret-value" not in repr(event.payload)
    assert "env:MODEL_KEY" not in repr(event.payload)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"run_id": "run-2"},
        {"endpoint": "https://attacker.example/v1"},
        {"purpose": "tool_execution"},
        {"disabled": True},
    ],
)
def test_broker_denies_wrong_audience_run_endpoint_purpose_or_provider_state(tmp_path, kwargs):
    broker, events, values, provider = _broker(tmp_path)
    context = _context(run_id=kwargs.get("run_id", "run-1"))
    if kwargs.get("disabled"):
        provider = _provider(enabled=False)
    credential = _acquire(
        broker,
        provider,
        context,
        endpoint=kwargs.get("endpoint"),
        purpose=kwargs.get("purpose", "model_inference"),
    )

    assert credential is None
    assert values.reads == []
    event = events.read_stream("security", context.run_id)[0]
    assert event.event_type == "SecretAccessDenied"
    assert event.payload["reason_code"] == "scope_denied"


@pytest.mark.parametrize("secret", [None, "", "bad\r\nheader", "non-ascii-秘密"])
def test_broker_audits_missing_or_invalid_secret_and_returns_none(tmp_path, secret):
    broker, events, _values, provider = _broker(tmp_path, _ValueStore(secret))

    assert _acquire(broker, provider, _context()) is None
    records = events.read_stream("security", "run-1")
    assert [event.event_type for event in records] == ["SecretAccessGranted", "SecretCredentialUnavailable"]
    assert records[-1].payload["reason_code"] == "credential_unavailable"
    assert not secret or secret not in repr([event.payload for event in records])


def test_broker_store_errors_are_audited_and_fail_closed(tmp_path):
    broker, events, _values, provider = _broker(tmp_path, _ValueStore(error=OSError("private path")))

    with pytest.raises(SecretBrokerUnavailable, match="credential store unavailable"):
        _acquire(broker, provider, _context())
    records = events.read_stream("security", "run-1")
    assert [event.event_type for event in records] == ["SecretAccessGranted", "SecretCredentialUnavailable"]
    assert records[-1].payload["reason_code"] == "credential_unavailable"
    assert "private path" not in repr([event.payload for event in records])


def test_broker_request_id_is_one_shot_and_duplicate_does_not_read_again(tmp_path):
    broker, events, values, provider = _broker(tmp_path)
    assert _acquire(broker, provider, _context()) is not None

    with pytest.raises(SecretAccessDenied, match="already used"):
        _acquire(broker, provider, _context())
    assert values.reads == ["env:MODEL_KEY"]
    assert len(events.read_stream("security", "run-1")) == 1


def test_concurrent_broker_requests_issue_at_most_one_credential(tmp_path):
    database = tmp_path / "shared.db"
    values = _ValueStore()
    context = _context("concurrent-request")
    provider = _provider()

    def access() -> str:
        event_store = SQLiteEventStore(database)
        broker = AuditedSecretBroker(
            event_store=event_store,
            value_store=values,
            rules=(
                SecretAccessRule(
                    provider_id=provider.id,
                    secret_ref=provider.secret_ref,
                    endpoint=provider.effective_endpoint,
                    purpose="model_inference",
                    allowed_run_ids=frozenset({"run-1"}),
                ),
            ),
        )
        try:
            credential = _acquire(broker, provider, context)
            return "granted" if credential is not None else "empty"
        except SecretAccessDenied:
            return "denied"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: access(), range(2)))
    assert sorted(outcomes) == ["denied", "granted"]
    assert values.reads == ["env:MODEL_KEY"]
    audit = SQLiteEventStore(database).read_stream("security", "run-1")
    assert [event.event_type for event in audit] == ["SecretAccessGranted"]


def test_broker_does_not_resolve_a_secret_when_audit_is_unavailable(tmp_path, monkeypatch):
    broker, events, values, provider = _broker(tmp_path)

    def fail_append(*args, **kwargs):
        raise OSError("audit store private detail")

    monkeypatch.setattr(events, "append_checked", fail_append)
    with pytest.raises(SecretBrokerUnavailable, match="durably audited"):
        _acquire(broker, provider, _context())
    assert values.reads == []


def test_environment_store_reads_only_explicit_env_refs(monkeypatch):
    monkeypatch.setenv("MODEL_KEY", "api-key")
    monkeypatch.setenv("OTHER_KEY", "other-secret")
    store = EnvironmentSecretStore(allowed_secret_refs={"env:MODEL_KEY"})

    assert store.read("env:MODEL_KEY") == "api-key"
    assert store.read("env:OTHER_KEY") is None
    assert "api-key" not in repr(store)
    with pytest.raises(ValueError, match="only explicit env"):
        EnvironmentSecretStore(allowed_secret_refs={"keyring:provider/model"})

    store._allowed_refs = frozenset({"env:INVALID NAME"})
    assert store.read("env:INVALID NAME") is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"provider_id": ""},
        {"secret_ref": "plaintext-secret"},
        {"endpoint": "http://provider.invalid"},
        {"purpose": "arbitrary"},
        {"allowed_run_ids": frozenset()},
        {"allowed_run_ids": frozenset({"bad run id"})},
    ],
)
def test_secret_access_rule_rejects_broad_or_unsafe_configuration(kwargs):
    values = {
        "provider_id": "primary",
        "secret_ref": "env:MODEL_KEY",
        "endpoint": "https://api.openai.com/v1",
        "purpose": "model_inference",
        "allowed_run_ids": frozenset({"run-1"}),
    }
    values.update(kwargs)
    with pytest.raises((TypeError, ValueError)):
        SecretAccessRule(**values)


def test_broker_rejects_missing_or_untyped_policy_rules_and_unvalidated_context(tmp_path):
    event_store = SQLiteEventStore(tmp_path / "invalid-rules.db")
    with pytest.raises(ValueError, match="at least one"):
        AuditedSecretBroker(event_store=event_store, value_store=_ValueStore(), rules=())
    with pytest.raises(TypeError, match="SecretAccessRule values"):
        AuditedSecretBroker(event_store=event_store, value_store=_ValueStore(), rules=("bad",))

    broker, _events, _values, provider = _broker(tmp_path)
    with pytest.raises(SecretAccessDenied, match="validated Model Gateway"):
        asyncio.run(
            broker.acquire_provider_credential(
                secret_ref=provider.secret_ref,
                provider=provider,
                endpoint=provider.effective_endpoint,
                purpose="model_inference",
                context=object(),
            )
        )
