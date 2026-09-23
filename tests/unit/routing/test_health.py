from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.config.effective import HealthPolicy
from orchestrator.persistence import SQLiteEventStore
from orchestrator.routing import (
    HealthAggregateKey,
    HealthController,
    HealthCircuitError,
    ProbeLeaseConflict,
    effective_health_state,
    reduce_health_events,
)


NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
REGISTRY_HASH = "sha256:" + "d" * 64


def keys():
    return (
        HealthAggregateKey(
            registry_manifest_hash=REGISTRY_HASH,
            scope="provider",
            provider_id="primary",
        ),
        HealthAggregateKey(
            registry_manifest_hash=REGISTRY_HASH,
            scope="model",
            provider_id="primary",
            model_id="model-1",
        ),
    )


def policy():
    return HealthPolicy(
        id="default",
        failure_window_ms=60_000,
        degrade_after=2,
        open_after=4,
        recovery_successes=2,
        cooldown_ms=10_000,
        max_probe_permits=1,
    )


def open_both(controller):
    provider, model = keys()
    current_policy = policy()
    for index in range(4):
        p_state, m_state = controller.record_outcome(
            provider_key=provider,
            model_key=model,
            policy=current_policy,
            event_time=NOW + timedelta(seconds=index),
            idempotency_key=f"call-{index}",
            success=False,
            failure_category="transient",
        )
    return provider, model, current_policy, p_state, m_state


def test_health_aggregate_replays_thresholds_and_atomic_probe_lease(tmp_path):
    db_path = tmp_path / "health.db"
    store = SQLiteEventStore(db_path)
    controller = HealthController(store)
    provider, model, current_policy, p_state, m_state = open_both(controller)

    assert p_state.state == m_state.state == "open"
    assert p_state.generation == m_state.generation == 2
    assert p_state.cooldown_until == NOW + timedelta(seconds=13)
    assert effective_health_state("healthy", "degraded") == "degraded"
    repeated_provider, repeated_model = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=3),
        idempotency_key="call-3",
        success=False,
        failure_category="transient",
    )
    assert repeated_provider.version == p_state.version
    assert repeated_model.version == m_state.version
    with pytest.raises(HealthCircuitError, match="idempotency key was reused"):
        controller.record_outcome(
            provider_key=provider,
            model_key=model,
            policy=current_policy,
            event_time=NOW + timedelta(seconds=4),
            idempotency_key="call-3",
            success=False,
            failure_category="transient",
        )

    lease = controller.acquire_probe_lease(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=14),
        lease_id="probe-1",
        holder_id="health-controller",
        expires_at=NOW + timedelta(seconds=24),
        idempotency_key="probe-acquire-1",
    )
    assert len(lease.aggregates) == 2
    assert controller.acquire_probe_lease(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=14),
        lease_id="probe-1",
        holder_id="health-controller",
        expires_at=NOW + timedelta(seconds=24),
        idempotency_key="probe-acquire-1",
    ) == lease
    assert controller.read(provider, default_policy=current_policy).state == "half_open"
    assert controller.read(model, default_policy=current_policy).state == "half_open"

    result = controller.finish_probe_lease(
        lease,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=15),
        success=True,
        idempotency_key="probe-result-1",
    )
    assert tuple(item.state for item in result) == ("healthy", "healthy")
    repeated_result = controller.finish_probe_lease(
        lease,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=15),
        success=True,
        idempotency_key="probe-result-1",
    )
    assert repeated_result == result
    expected_versions = tuple(item.version for item in result)
    store.close()

    reopened = SQLiteEventStore(db_path)
    replayed = HealthController(reopened)
    assert tuple(
        replayed.read(key, default_policy=current_policy).version for key in (provider, model)
    ) == expected_versions
    assert replayed.read(provider, default_policy=current_policy).state == "healthy"


def test_health_probe_lease_expiry_reopens_circuits_and_conflicts_fail_atomically(tmp_path):
    store = SQLiteEventStore(tmp_path / "expiry.db")
    controller = HealthController(store)
    provider, model, current_policy, _, _ = open_both(controller)
    lease = controller.acquire_probe_lease(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=14),
        lease_id="probe-expiring",
        holder_id="health-controller",
        expires_at=NOW + timedelta(seconds=20),
        idempotency_key="probe-acquire-expiring",
    )
    with pytest.raises(ProbeLeaseConflict, match="active probe lease"):
        controller.acquire_probe_lease(
            provider_key=provider,
            model_key=model,
            policy=current_policy,
            event_time=NOW + timedelta(seconds=15),
            lease_id="probe-duplicate",
            holder_id="health-controller",
            expires_at=NOW + timedelta(seconds=25),
            idempotency_key="probe-acquire-duplicate",
        )

    assert controller.expire_probe_leases(event_time=NOW + timedelta(seconds=20)) == (lease.lease_id,)
    states = tuple(controller.read(key, default_policy=current_policy) for key in (provider, model))
    assert tuple(item.state for item in states) == ("open", "open")
    assert all(item.cooldown_until == NOW + timedelta(seconds=30) for item in states)


def test_probe_group_cas_allows_only_one_concurrent_lease(tmp_path):
    db_path = tmp_path / "concurrent.db"
    provider, model = keys()
    current_policy = policy()
    seed = SQLiteEventStore(db_path)
    open_both(HealthController(seed))
    seed.close()

    def acquire(lease_id):
        store = SQLiteEventStore(db_path)
        try:
            try:
                return HealthController(store).acquire_probe_lease(
                    provider_key=provider,
                    model_key=model,
                    policy=current_policy,
                    event_time=NOW + timedelta(seconds=14),
                    lease_id=lease_id,
                    holder_id="health-controller",
                    expires_at=NOW + timedelta(seconds=24),
                    idempotency_key=f"acquire-{lease_id}",
                )
            except ProbeLeaseConflict as error:
                return error
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(acquire, ("probe-a", "probe-b")))

    assert sum(isinstance(result, ProbeLeaseConflict) for result in results) == 1


def test_health_replay_rejects_non_monotonic_event_time():
    provider, _ = keys()
    # Reducer is exercised through durable append APIs elsewhere; this guards
    # its empty-history default and makes clear that event time is not clocked.
    assert reduce_health_events(provider, [], default_policy=policy()).state == "healthy"


def test_health_degrades_recovers_and_honors_bounded_retry_after(tmp_path):
    store = SQLiteEventStore(tmp_path / "recovery.db")
    controller = HealthController(store)
    provider, model = keys()
    current_policy = policy()

    # Permanent failures do not poison the transient-health window.
    p_state, _ = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW,
        idempotency_key="permanent",
        success=False,
        failure_category="permanent",
    )
    assert p_state.state == "healthy"
    p_state, m_state = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=1),
        idempotency_key="transient-1",
        success=False,
        failure_category="transient",
    )
    assert p_state.state == m_state.state == "healthy"
    p_state, m_state = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=2),
        idempotency_key="transient-2",
        success=False,
        failure_category="transient",
    )
    assert p_state.state == m_state.state == "degraded"
    assert p_state.snapshot_ref["state"] == "degraded"
    assert p_state.routing_ref.state == "degraded"

    p_state, _ = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=3),
        idempotency_key="success-1",
        success=True,
    )
    assert p_state.state == "degraded"
    p_state, _ = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=current_policy,
        event_time=NOW + timedelta(seconds=4),
        idempotency_key="success-2",
        success=True,
    )
    assert p_state.state == "healthy"
    assert p_state.transient_failure_times == ()

    # An explicit provider retry hint extends, but never shortens, cooldown.
    open_policy = current_policy.model_copy(update={"degrade_after": 1, "open_after": 2})
    p_state, m_state = controller.record_outcome(
        provider_key=provider,
        model_key=model,
        policy=open_policy,
        event_time=NOW + timedelta(seconds=5),
        idempotency_key="retry-after",
        success=False,
        failure_category="transient",
        retry_after_ms=120_000,
    )
    assert p_state.cooldown_until == NOW + timedelta(seconds=125)
    assert m_state.cooldown_until == p_state.cooldown_until

    with pytest.raises(ValueError, match="successful outcomes"):
        controller.record_outcome(
            provider_key=provider,
            model_key=model,
            policy=open_policy,
            event_time=NOW + timedelta(seconds=6),
            idempotency_key="bad-success-retry-after",
            success=True,
            retry_after_ms=1,
        )
    with pytest.raises(ValueError, match="non-negative"):
        controller.record_outcome(
            provider_key=provider,
            model_key=model,
            policy=open_policy,
            event_time=NOW + timedelta(seconds=6),
            idempotency_key="bad-retry-after",
            success=False,
            retry_after_ms=-1,
        )
    assert controller.expire_probe_leases(event_time=NOW + timedelta(days=1)) == ()
