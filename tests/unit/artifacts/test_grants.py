from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest

from orchestrator.artifacts import EphemeralArtifactGrantAuthority


DIGEST = "sha256:" + "a" * 64


def test_grant_is_run_scoped_expiring_and_one_use():
    authority = EphemeralArtifactGrantAuthority()
    expires = datetime.now(timezone.utc) + timedelta(minutes=1)
    grant = authority.issue(digest=DIGEST, run_id="run-a", expires_at=expires)

    assert grant.digest == DIGEST
    assert grant.scope == ("run-a",)
    assert grant.expires_at == expires
    assert authority.verify(grant) is True
    assert authority.verify(grant) is False


@pytest.mark.parametrize(
    "update",
    [
        {"digest": "sha256:" + "b" * 64},
        {"scope": ("run-b",)},
        {"scope": ("run-a", "run-b")},
        {"issuer": "other-control-plane"},
        {"signature": "caller-chosen-token"},
        {"signature": []},
        {"expires_at": datetime(2099, 1, 1, tzinfo=timezone.utc)},
        {"expires_at": datetime(2099, 1, 1)},
    ],
)
def test_mutated_grant_is_rejected_without_consuming_original(update):
    authority = EphemeralArtifactGrantAuthority()
    grant = authority.issue(
        digest=DIGEST, run_id="run-a",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
    )

    assert authority.verify(grant.model_copy(update=update)) is False
    assert authority.verify(grant) is True


def test_expired_grant_cannot_authorize_access():
    authority = EphemeralArtifactGrantAuthority()
    grant = authority.issue(
        digest=DIGEST, run_id="run-a",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    assert authority.verify(grant) is False


def test_naive_expiry_cannot_be_issued():
    with pytest.raises(ValueError, match="timezone-aware"):
        EphemeralArtifactGrantAuthority().issue(
            digest=DIGEST, run_id="run-a", expires_at=datetime(2099, 1, 1),
        )


@pytest.mark.parametrize(
    "digest,run_id", [("not-a-digest", "run-a"), (DIGEST, ""), (DIGEST, "*")],
)
def test_invalid_binding_cannot_be_issued(digest, run_id):
    with pytest.raises(ValueError):
        EphemeralArtifactGrantAuthority().issue(
            digest=digest, run_id=run_id,
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        )


def test_other_authority_cannot_verify_grant():
    authority = EphemeralArtifactGrantAuthority()
    grant = authority.issue(
        digest=DIGEST, run_id="run-a",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
    )

    assert EphemeralArtifactGrantAuthority().verify(grant) is False
    assert authority.verify(grant) is True


def test_non_grant_cannot_authorize_access():
    assert EphemeralArtifactGrantAuthority().verify(None) is False


def test_concurrent_verification_consumes_grant_once():
    authority = EphemeralArtifactGrantAuthority()
    grant = authority.issue(
        digest=DIGEST, run_id="run-a",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
    )
    barrier = Barrier(4)

    def verify():
        barrier.wait(timeout=5)
        return authority.verify(grant)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: verify(), range(4)))

    assert results.count(True) == 1
    assert results.count(False) == 3
    assert authority._bindings == {}


def test_consumed_grant_binding_is_released_while_live_grant_is_retained():
    authority = EphemeralArtifactGrantAuthority()
    expires = datetime.now(timezone.utc) + timedelta(minutes=1)
    consumed = authority.issue(digest=DIGEST, run_id="run-a", expires_at=expires)
    live = authority.issue(digest=DIGEST, run_id="run-b", expires_at=expires)

    assert authority.verify(consumed) is True
    assert consumed.signature not in authority._bindings
    assert live.signature in authority._bindings
    assert authority.verify(consumed) is False
    assert authority.verify(live) is True
    assert authority._bindings == {}


@pytest.mark.parametrize("operation", ["issue", "verify"])
def test_expired_unused_bindings_are_pruned_without_losing_live_grants(monkeypatch, operation):
    import orchestrator.artifacts.grants as grants_module

    authority = EphemeralArtifactGrantAuthority()
    now = datetime.now(timezone.utc)
    expired = authority.issue(digest=DIGEST, run_id="run-a", expires_at=now + timedelta(seconds=1))
    live = authority.issue(digest=DIGEST, run_id="run-b", expires_at=now + timedelta(minutes=1))

    class LaterClock:
        @staticmethod
        def now(_timezone):
            return now + timedelta(seconds=2)

    monkeypatch.setattr(grants_module, "datetime", LaterClock)
    if operation == "issue":
        fresh = authority.issue(digest=DIGEST, run_id="run-c", expires_at=now + timedelta(minutes=1))
        assert fresh.signature in authority._bindings
    else:
        assert authority.verify(live.model_copy(update={"issuer": "wrong"})) is False
    assert expired.signature not in authority._bindings
    assert live.signature in authority._bindings
    assert authority.verify(expired) is False
    assert authority.verify(live) is True
