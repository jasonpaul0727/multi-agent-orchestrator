"""Upgrade real serialized BASE events without changing their identity/history."""
import asyncio
import json
from pathlib import Path
from datetime import datetime, timezone

import pytest

from orchestrator.application import ControlPlaneApplication
from orchestrator.models import ModelGatewayError, ModelRequest, ProviderModelGateway, SQLiteProviderCallJournal
from orchestrator.persistence import EventContractError, EventDraft, SQLiteEventStore
from orchestrator.persistence.events import validate_event_contract
from orchestrator.provider_reconciliation import ProviderEvidenceUnsupported
from orchestrator.scheduler import ConcurrencyLimits


FIXTURE = Path(__file__).parents[2] / "fixtures/provider_calls/base_v1_events.json"


@pytest.fixture
def base_database(tmp_path):
    fixture = json.loads(FIXTURE.read_text())
    path = tmp_path / "base.db"
    with SQLiteEventStore(path) as store:
        for table in ("stream_versions", "events", "idempotency_records"):
            rows = fixture["tables"][table]
            columns = tuple(rows[0])
            statement = "INSERT INTO " + table + " (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")"
            store._connection.executemany(statement, [tuple(row[column] for column in columns) for row in rows])
        store._connection.commit()
    return path, {state: ModelRequest.model_validate(data) for state, data in fixture["requests"].items()}, fixture


def test_base_v1_terminal_and_unresolved_calls_replay_without_invented_metadata(base_database):
    path, requests, fixture = base_database
    with SQLiteEventStore(path) as store:
        journal = SQLiteProviderCallJournal(store)
        for state, request in requests.items():
            call = journal.read(request)
            assert call.status == state
            assert call.provider_adapter is None and call.provider_correlation_id is None
            assert call.termination_receipt is None and call.reconciliation is None
            events = store.read_stream("provider_call", call.stream_id)
            assert events[0].schema_version == 1
            assert "provider_adapter" not in events[0].payload
            validate_event_contract(events)
        assert {call.status for call in journal.unresolved()} == {"dispatching", "unknown"}
        assert [dict(row) for row in store._connection.execute("SELECT * FROM events")] == fixture["tables"]["events"]


def test_base_upgrade_bootstrap_retains_holds_replay_block_and_evidence_rejection(base_database):
    path, requests, fixture = base_database
    limits = ConcurrencyLimits(system_active_attempts=8, run_active_attempts=8, provider_active_attempts=8, tool_active_attempts=8)
    class NoVerifierCalls:
        def verify(self, *_args):
            pytest.fail("legacy evidence was sent to an unsupported verifier")
        def verify_stopped(self, *_args):
            pytest.fail("legacy call reached termination verification")
    with ControlPlaneApplication(path, limits=limits, evidence_verifier=NoVerifierCalls(), termination_verifier=NoVerifierCalls()) as app:
        assert app.startup_report.held_attempts == 2
        assert app.startup_report.unknown_attempts == 2
        assert len(app.startup_report.unresolved_provider_calls) == 2
        recovered = app.recover_run("run-1")
        assert len(recovered.active_attempts) == 2
        assert recovered.budget.unknown_minor == 40
        journal = SQLiteProviderCallJournal(app._event_store)
        for state in ("dispatching", "unknown"):
            request = requests[state]
            call = journal.read(request)
            with pytest.raises(ProviderEvidenceUnsupported):
                app.reconcile_provider_call(call.stream_id, raw_evidence=b"no historical correlation",
                    termination_receipt=object(), reconciled_at=datetime(2026, 10, 4, tzinfo=timezone.utc))
            class Accepted:
                async def is_accepted(self, _request):
                    return True
            class NoBrokerAccess:
                async def acquire_provider_credential(self, **_kwargs):
                    pytest.fail("legacy replay reached credential acquisition")
            gateway = ProviderModelGateway(registry=app._scheduler.lifecycle.config_snapshot("run-1").registry_manifest,
                accepted_route_verifier=Accepted(), secret_broker=NoBrokerAccess(), provider_call_journal=journal)
            with pytest.raises(ModelGatewayError) as failure:
                asyncio.run(gateway.invoke(request))
            assert failure.value.failure.code == "idempotency_conflict"
        assert [dict(row) for row in app._event_store._connection.execute("SELECT * FROM events")] == fixture["tables"]["events"]


def test_new_outcome_can_append_to_authentic_legacy_intent_without_rewriting_it(base_database):
    path, requests, _fixture = base_database
    with SQLiteEventStore(path) as store:
        journal = SQLiteProviderCallJournal(store)
        request = requests["dispatching"]
        call = journal.read(request)
        before = store.read_stream("provider_call", call.stream_id)[0]
        journal.record_outcome(request, outcome="unknown", failure_code="timeout")
        events = store.read_stream("provider_call", call.stream_id)
        assert events[0] == before
        assert events[1].schema_version == 2
        assert journal.read(request).status == "unknown"
        assert journal.read(request).provider_correlation_id is None


def test_new_provider_writes_use_v2_and_require_full_binding(base_database):
    path, requests, _fixture = base_database
    with SQLiteEventStore(path) as store:
        journal = SQLiteProviderCallJournal(store)
        request = requests["dispatching"]
        intent = store.read_stream("provider_call", next(iter(store.stream_ids("provider_call"))))[0]
        with pytest.raises(EventContractError, match="IntentRecorded"):
            store.append("provider_call", "new-legacy-shaped-call", expected_version=0,
                events=[EventDraft("ProviderCallIntentRecorded", intent.payload,
                    run_id=intent.run_id, node_id=intent.node_id, attempt_id=intent.attempt_id,
                    fencing_generation=intent.fencing_generation, causation_id=intent.causation_id)],
                idempotency_key="strict-new-write")
    with SQLiteEventStore(path.with_name("new.db")) as store:
        journal = SQLiteProviderCallJournal(store)
        stream_id = journal.record_intent(request, provider_id="primary", provider_adapter="openai_responses",
                                         request_body=b"{}", provider_correlation_id="maestro-new-write")
        assert store.read_stream("provider_call", stream_id)[0].schema_version == 2


@pytest.mark.parametrize("mutation", ["unsupported-version", "partial-legacy"])
def test_legacy_projection_rejects_unknown_schema_and_partial_historical_binding(base_database, mutation):
    path, _requests, _fixture = base_database
    with SQLiteEventStore(path) as store:
        stream_id = store.stream_ids("provider_call")[0]
        event = store.read_stream("provider_call", stream_id)[0]
        if mutation == "unsupported-version":
            changed = event.model_copy(update={"schema_version": 3})
        else:
            payload = dict(event.payload, provider_adapter="openai_responses")
            changed = event.model_copy(update={"payload": payload})
        with pytest.raises(EventContractError):
            SQLiteProviderCallJournal(store)._project_call([changed])
