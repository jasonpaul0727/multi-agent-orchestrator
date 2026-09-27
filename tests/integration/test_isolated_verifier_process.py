"""Exercise independent Verifier checks under the real systemd boundary."""

from datetime import datetime, timedelta, timezone
import shutil
import subprocess
import sys

import pytest

from orchestrator.artifacts import ArtifactAccessGrant, ArtifactStore
from orchestrator.persistence import SQLiteEventStore
from orchestrator.runtime import (
    ArtifactRef,
    AttemptContext,
    IsolatedVerifierProcess,
    VerificationTask,
)


def _usable_systemd_user_manager() -> bool:
    if not sys.platform.startswith("linux") or not shutil.which("systemd-run"):
        return False
    return subprocess.run(
        ["systemctl", "--user", "show-environment"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=5,
    ).returncode == 0


pytestmark = pytest.mark.skipif(
    not _usable_systemd_user_manager(),
    reason="integrated Verifier test requires Linux and systemd --user",
)


def test_real_isolated_verifier_checks_exact_host_artifact(tmp_path):
    events = SQLiteEventStore(tmp_path / "events.db")
    artifacts = ArtifactStore(
        tmp_path / "artifact-bytes",
        event_store=events,
        grant_verifier=lambda grant: grant.signature == "valid",
    )
    record = artifacts.publish_bytes(
        b'{"answer": 42}',
        source={
            "run_id": "run-live",
            "node_id": "node-live",
            "attempt_id": "attempt-live",
            "fencing_generation": "1",
            "agent_instance_id": "agent-live",
        },
        artifact_type="json-document",
        media_type="application/json",
        readable_scope=("run-live",),
    )
    digest = "sha256:" + "a" * 64
    task = VerificationTask(
        context=AttemptContext(
            run_id="run-live",
            node_id="node-live",
            attempt_id="attempt-live",
            agent_instance_id="agent-live",
            fencing_generation=1,
            graph_version=1,
            input_manifest_hash=digest,
            effective_config_hash=digest,
            registry_hash=digest,
            policy_manifest_hash=digest,
            routing_decision_hash=digest,
            planning_contract_hash=digest,
        ),
        candidate_artifacts=(ArtifactRef(
            digest=record.digest,
            size_bytes=record.size,
            artifact_type=record.artifact_type,
            media_type=record.media_type,
        ),),
        required_check_ids=("artifact-integrity", "json", "python-syntax"),
        acceptance_contract="maestro.artifact-verification/v1",
    )

    evidence = IsolatedVerifierProcess().verify(
        task,
        artifacts,
        grant_for_digest=lambda artifact_digest: ArtifactAccessGrant(
            digest=artifact_digest,
            scope=("run-live",),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            issuer="test-control-plane",
            signature="valid",
        ),
    )

    assert evidence.outcome == "accepted"
    assert evidence.inspected_digests == (record.digest,)
    assert all(item.passed for item in evidence.checks)
