"""Run lifecycle commands and deterministic replay of lifecycle event streams."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Literal

from orchestrator.config.runtime import RunConfigSnapshot
from orchestrator.persistence import EventDraft, SQLiteEventStore, SnapshotStore, StoredEvent
from orchestrator.security import PolicyManifest

from .graph import GraphError, validate_graph_append
from .models import AttemptState, NodeSpec, NodeState, RunLifecycleState


_LIFECYCLE_STREAM = "run_lifecycle"
_RUN_STREAM = "run"
_HASH = r"^sha256:[0-9a-f]{64}$"
_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$"


class LifecycleError(RuntimeError):
    """The requested lifecycle transition is stale or invalid."""


class LifecycleConflict(LifecycleError):
    """The expected aggregate or graph version no longer matches."""


def _hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _valid_identifier(value: str) -> bool:
    return isinstance(value, str) and re.fullmatch(_IDENTIFIER, value) is not None


class LifecycleController:
    """Event-backed Run/Node/Attempt lifecycle with append-only graph changes."""

    def __init__(self, event_store: SQLiteEventStore) -> None:
        self.event_store = event_store
        self.snapshots = SnapshotStore(event_store)

    def initialize_run(self, run_id: str) -> RunLifecycleState:
        snapshot = self.config_snapshot(run_id)
        payload = {
            "run_id": run_id,
            "config_hash": snapshot.effective_config_hash,
            "registry_hash": snapshot.registry_manifest_hash,
            "max_nodes": _node_limit(snapshot),
            "max_depth": _depth_limit(snapshot),
        }
        key = f"lifecycle-init:{run_id}"

        def decide(events: list[StoredEvent], version: int):
            if events:
                _check_idempotent(events, key, "RunInitialized", payload)
                return None
            return [EventDraft("RunInitialized", payload)]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def checkpoint(self, run_id: str) -> RunLifecycleState:
        """Persist the current validated projection, anchored to its final event."""

        state = self.replay(run_id)
        events = self.event_store.read_stream(_LIFECYCLE_STREAM, run_id)
        if not events or events[-1].stream_version != state.event_version:
            raise LifecycleConflict("lifecycle checkpoint does not match its event stream")
        self.snapshots.save_snapshot(
            _LIFECYCLE_STREAM,
            run_id,
            state.event_version,
            state.model_dump(mode="json"),
            source_event_id=events[-1].event_id,
        )
        return state

    def config_snapshot(self, run_id: str) -> RunConfigSnapshot:
        events, version = self.event_store.read_stream_with_version(_RUN_STREAM, run_id)
        if version < 1 or not events or events[0].event_type != "RunCreated":
            raise LifecycleError("RunConfigSnapshot is missing")
        try:
            value = events[0].payload["config_snapshot"]
            return RunConfigSnapshot.model_validate_json(json.dumps(value))
        except (KeyError, TypeError, ValueError) as exc:
            raise LifecycleError("RunConfigSnapshot failed validation") from exc

    def replay(self, run_id: str) -> RunLifecycleState:
        events, version = self.event_store.read_stream_with_version(_LIFECYCLE_STREAM, run_id)
        if not events:
            raise LifecycleError("Run lifecycle has not been initialized")
        state: RunLifecycleState | None = None
        offset = 0
        try:
            snapshot = self.snapshots.load_valid(
                _LIFECYCLE_STREAM, run_id, expected_schema_version=1
            )
        except (TypeError, ValueError):
            snapshot = None
        if snapshot is not None and snapshot.event_version <= version:
            anchor = events[snapshot.event_version - 1]
            try:
                candidate = RunLifecycleState.model_validate(snapshot.state)
            except (TypeError, ValueError):
                candidate = None
            if (
                candidate is not None
                and candidate.run_id == run_id
                and candidate.event_version == snapshot.event_version
                and snapshot.source_event_id == anchor.event_id
                and candidate.config_hash == events[0].payload.get("config_hash")
                and candidate.registry_hash == events[0].payload.get("registry_hash")
            ):
                state = candidate
                offset = snapshot.event_version
        for event in events[offset:]:
            state = apply_lifecycle_event(run_id, state, event)
        if state is None:
            raise LifecycleError("Run lifecycle replay produced no state")
        if state.event_version != version:
            raise LifecycleError("lifecycle stream version disagrees with its projection")
        return state

    def append_nodes(
        self,
        run_id: str,
        nodes: tuple[NodeSpec, ...],
        *,
        expected_graph_version: int,
        idempotency_key: str,
        policy_manifest: PolicyManifest | None = None,
    ) -> RunLifecycleState:
        if not nodes:
            raise ValueError("nodes must not be empty")
        snapshot = self.config_snapshot(run_id)
        payload_nodes = tuple(sorted(nodes, key=lambda item: item.node_id))
        payload = {
            "graph_version": expected_graph_version + 1,
            "nodes": [node.model_dump(mode="json") for node in payload_nodes],
        }
        if policy_manifest is not None:
            if not isinstance(policy_manifest, PolicyManifest):
                raise TypeError("policy_manifest must be a validated PolicyManifest")
            payload["policy_manifest"] = policy_manifest.model_dump(mode="json")
            payload["policy_manifest_hash"] = policy_manifest.content_hash
        elif any(node.planning_contract is not None for node in payload_nodes):
            raise LifecycleError("planned graph append requires its frozen PolicyManifest")
        key = f"graph:{idempotency_key}"

        def decide(events: list[StoredEvent], version: int):
            for event in events:
                if event.idempotency_key == key:
                    _check_idempotent(events, key, "GraphNodesAppended", payload)
                    return None
            state = reduce_lifecycle(run_id, events)
            if state.status not in {"created", "running"}:
                raise LifecycleError("nodes can only be appended to a non-terminal Run")
            if state.graph_version != expected_graph_version:
                raise LifecycleConflict("graph version changed before append")
            proposed_policy_hash = payload.get("policy_manifest_hash")
            if (
                proposed_policy_hash is not None
                and state.policy_manifest_hash is not None
                and proposed_policy_hash != state.policy_manifest_hash
            ):
                raise LifecycleError("PolicyManifest changed after it was frozen for this Run")
            for spec in payload_nodes:
                contract = spec.planning_contract
                if contract is not None and (
                    contract.run_id != run_id
                    or contract.config_hash != snapshot.effective_config_hash
                    or contract.registry_hash != snapshot.registry_manifest_hash
                    or contract.policy_manifest_hash != proposed_policy_hash
                ):
                    raise LifecycleError("frozen node contract does not match its Run snapshot")
            max_nodes = _node_limit(snapshot)
            max_depth = _depth_limit(snapshot)
            try:
                validated = validate_graph_append(
                    tuple(item.spec for item in state.nodes),
                    payload_nodes,
                    max_nodes=max_nodes,
                    max_depth=max_depth,
                )
            except GraphError as exc:
                raise LifecycleError(str(exc)) from exc
            if validated != payload_nodes:
                raise LifecycleError("graph append ordering is not canonical")
            return [EventDraft("GraphNodesAppended", payload)]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def start_run(self, run_id: str) -> RunLifecycleState:
        key = f"run-start:{run_id}"
        payload = {"run_id": run_id}

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "RunStarted", payload)
                return None
            state = reduce_lifecycle(run_id, events)
            if state.status != "created":
                raise LifecycleError("only a created Run can be started")
            if not state.nodes:
                raise LifecycleError("Run cannot start before its initial graph is appended")
            return [EventDraft("RunStarted", payload)]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def pause_run(self, run_id: str, *, reason_code: str) -> RunLifecycleState:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason_code):
            raise ValueError("reason_code must be a stable lowercase code")
        return self._run_transition(run_id, "RunPaused", "pause", reason_code=reason_code)

    def resume_run(self, run_id: str) -> RunLifecycleState:
        return self._run_transition(run_id, "RunResumed", "resume", reason_code=None)

    def await_user(
        self,
        run_id: str,
        *,
        request_id: str,
        reason_code: str,
        context_hash: str,
    ) -> RunLifecycleState:
        if not _valid_identifier(request_id):
            raise ValueError("request_id must be a stable identifier")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason_code):
            raise ValueError("reason_code must be a stable lowercase code")
        if not re.fullmatch(_HASH, context_hash):
            raise ValueError("context_hash must be a sha256 content hash")
        payload = {
            "run_id": run_id,
            "request_id": request_id,
            "reason_code": reason_code,
            "context_hash": context_hash,
        }
        key = f"run-awaiting-user:{run_id}:{request_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "RunAwaitingUser", payload)
                return None
            state = reduce_lifecycle(run_id, events)
            if state.status != "running":
                raise LifecycleError("only a running Run can await user input")
            return [
                EventDraft(
                    "RunAwaitingUser", payload, run_id=run_id,
                    correlation_id=run_id, causation_id=_hash(payload),
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def record_user_response(
        self, run_id: str, *, request_id: str, response_hash: str
    ) -> RunLifecycleState:
        if not _valid_identifier(request_id):
            raise ValueError("request_id must be a stable identifier")
        if not re.fullmatch(_HASH, response_hash):
            raise ValueError("response_hash must be a sha256 content hash")
        payload = {"run_id": run_id, "request_id": request_id, "response_hash": response_hash}
        key = f"run-user-response:{run_id}:{request_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "RunUserResponseReceived", payload)
                return None
            state = reduce_lifecycle(run_id, events)
            if state.status != "awaiting_user" or state.awaiting_user_request_id != request_id:
                raise LifecycleConflict("user response does not match the outstanding request")
            return [
                EventDraft(
                    "RunUserResponseReceived", payload, run_id=run_id,
                    correlation_id=run_id, causation_id=response_hash,
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def request_cancel(self, run_id: str, *, reason_code: str) -> RunLifecycleState:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason_code):
            raise ValueError("reason_code must be a stable lowercase code")
        payload = {"run_id": run_id, "reason_code": reason_code}
        key = f"run-cancel-request:{run_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "RunCancellationRequested", payload)
                return None
            state = reduce_lifecycle(run_id, events)
            if state.status not in {"created", "running", "paused", "awaiting_user"}:
                raise LifecycleError("only a non-terminal Run can request cancellation")
            return [
                EventDraft(
                    "RunCancellationRequested", payload, run_id=run_id,
                    correlation_id=run_id, causation_id=_hash(payload),
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        self.complete_cancellation_if_idle(run_id)
        return self.checkpoint(run_id)

    def complete_cancellation_if_idle(self, run_id: str) -> RunLifecycleState:
        state = self.replay(run_id)
        if state.status != "cancelling" or any(
            node.status in {"running", "awaiting_reconciliation"} for node in state.nodes
        ):
            return state
        request_event = next(
            event for event in reversed(self.event_store.read_stream(_LIFECYCLE_STREAM, run_id))
            if event.event_type == "RunCancellationRequested"
        )
        payload = {"run_id": run_id, "cancellation_request_id": request_event.event_id}
        key = f"run-cancel-complete:{run_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "RunCancelled", payload)
                return None
            current = reduce_lifecycle(run_id, events)
            if current.status != "cancelling" or any(
                node.status in {"running", "awaiting_reconciliation"} for node in current.nodes
            ):
                raise LifecycleConflict("Run still has attempts requiring termination or reconciliation")
            return [
                EventDraft(
                    "RunCancelled", payload, run_id=run_id,
                    correlation_id=run_id, causation_id=request_event.event_id,
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def _run_transition(
        self, run_id: str, event_type: str, operation: str, *, reason_code: str | None
    ) -> RunLifecycleState:
        payload = {"run_id": run_id, "reason_code": reason_code}
        key = f"run-{operation}:{run_id}:{_hash(payload)}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, event_type, payload)
                return None
            state = reduce_lifecycle(run_id, events)
            expected = "running" if operation == "pause" else "paused"
            if state.status != expected:
                raise LifecycleError(f"only a {expected} Run can {operation}")
            return [EventDraft(event_type, payload)]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def record_attempt_accepted(
        self,
        run_id: str,
        *,
        node_id: str,
        attempt: AttemptState,
        decision_hash: str,
        causation_id: str,
    ) -> RunLifecycleState:
        if attempt.status != "accepted":
            raise LifecycleError("newly accepted attempts must start in accepted state")
        payload = {
            "node_id": node_id,
            "attempt": attempt.model_dump(mode="json"),
        }
        key = f"attempt-accepted:{attempt.attempt_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "AttemptAccepted", payload)
                return None
            state = reduce_lifecycle(run_id, events)
            node = state.node(node_id)
            if state.status != "running" or node.status != "ready":
                raise LifecycleError("attempt acceptance requires a ready node in a running Run")
            expected_generation = len(node.attempts) + 1
            if attempt.fencing_generation != expected_generation:
                raise LifecycleConflict("attempt fencing generation is not the next generation")
            if any(item.attempt_id == attempt.attempt_id for item in node.attempts):
                raise LifecycleConflict("attempt ID has already been used")
            if any(
                attempt.attempt_id == prior.attempt_id
                for other in state.nodes
                for prior in other.attempts
            ):
                raise LifecycleConflict("attempt ID must be unique within its Run")
            if len(node.attempts) >= node.spec.max_attempts:
                raise LifecycleError("node has exhausted its configured attempt limit")
            return [
                EventDraft(
                    "AttemptAccepted",
                    payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt.attempt_id,
                    fencing_generation=attempt.fencing_generation,
                    correlation_id=run_id,
                    causation_id=causation_id or decision_hash,
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def record_attempt_completed(
        self,
        run_id: str,
        *,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        outcome: Literal["succeeded", "failed", "outcome_unknown"],
        causation_id: str,
    ) -> RunLifecycleState:
        return self._record_attempt_end(
            run_id,
            event_type="AttemptCompleted",
            node_id=node_id,
            attempt_id=attempt_id,
            fencing_generation=fencing_generation,
            outcome=outcome,
            causation_id=causation_id,
        )

    def record_attempt_reconciled(
        self,
        run_id: str,
        *,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        outcome: Literal["succeeded", "failed"],
        causation_id: str,
    ) -> RunLifecycleState:
        return self._record_attempt_end(
            run_id,
            event_type="AttemptReconciled",
            node_id=node_id,
            attempt_id=attempt_id,
            fencing_generation=fencing_generation,
            outcome=outcome,
            causation_id=causation_id,
        )

    def record_attempt_cancelled(
        self,
        run_id: str,
        *,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        stop_receipt_hash: str,
        causation_id: str,
    ) -> RunLifecycleState:
        if not re.fullmatch(_HASH, stop_receipt_hash):
            raise ValueError("stop_receipt_hash must be a sha256 content hash")
        payload = {
            "node_id": node_id,
            "attempt_id": attempt_id,
            "outcome": "cancelled",
            "stop_receipt_hash": stop_receipt_hash,
        }
        key = f"attempt-cancelled:{attempt_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, "AttemptCancelled", payload)
                return None
            state = reduce_lifecycle(run_id, events)
            if state.status != "cancelling":
                raise LifecycleError("attempt cancellation requires a cancelling Run")
            node = state.node(node_id)
            active = _active_attempt(node)
            if (
                active is None or active.attempt_id != attempt_id
                or active.fencing_generation != fencing_generation
                or active.status != "accepted"
            ):
                raise LifecycleConflict("cancellation receipt is stale or attempt is not active")
            return [
                EventDraft(
                    "AttemptCancelled", payload, run_id=run_id,
                    node_id=node_id, attempt_id=attempt_id,
                    fencing_generation=fencing_generation, correlation_id=run_id,
                    causation_id=causation_id,
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)

    def _record_attempt_end(
        self,
        run_id: str,
        *,
        event_type: str,
        node_id: str,
        attempt_id: str,
        fencing_generation: int,
        outcome: str,
        causation_id: str,
    ) -> RunLifecycleState:
        payload = {"node_id": node_id, "attempt_id": attempt_id, "outcome": outcome}
        key = f"{event_type.lower()}:{attempt_id}"

        def decide(events: list[StoredEvent], version: int):
            if any(event.idempotency_key == key for event in events):
                _check_idempotent(events, key, event_type, payload)
                return None
            state = reduce_lifecycle(run_id, events)
            node = state.node(node_id)
            active = _active_attempt(node)
            expected_status = "outcome_unknown" if event_type == "AttemptReconciled" else "accepted"
            if (
                active is None
                or active.attempt_id != attempt_id
                or active.fencing_generation != fencing_generation
                or active.status != expected_status
            ):
                raise LifecycleConflict("attempt result is stale or not the active attempt")
            if event_type == "AttemptReconciled" and outcome == "outcome_unknown":
                raise LifecycleError("reconciliation must resolve the unknown outcome")
            return [
                EventDraft(
                    event_type,
                    payload,
                    run_id=run_id,
                    node_id=node_id,
                    attempt_id=attempt_id,
                    fencing_generation=fencing_generation,
                    correlation_id=run_id,
                    causation_id=causation_id,
                )
            ]

        self.event_store.append_checked(_LIFECYCLE_STREAM, run_id, key, decide)
        return self.checkpoint(run_id)


def reduce_lifecycle(run_id: str, events: list[StoredEvent]) -> RunLifecycleState:
    """Rebuild a lifecycle aggregate deterministically from its event sequence."""

    state: RunLifecycleState | None = None
    for event in events:
        state = apply_lifecycle_event(run_id, state, event)
    if state is None:
        raise LifecycleError("lifecycle stream must start with RunInitialized")
    return state


def apply_lifecycle_event(
    run_id: str, state: RunLifecycleState | None, event: StoredEvent
) -> RunLifecycleState:
    """Apply one validated lifecycle event; shared by snapshots and full replay."""

    payload = event.payload
    if event.run_id is not None and event.run_id != run_id:
        raise LifecycleError("lifecycle event identity does not match its stream")
    if state is None:
        if event.event_type != "RunInitialized":
            raise LifecycleError("lifecycle stream must start with RunInitialized")
        max_nodes = payload.get("max_nodes")
        max_depth = payload.get("max_depth")
        if payload.get("run_id") != run_id:
            raise LifecycleError("RunInitialized identity does not match its stream")
        if (
            isinstance(max_nodes, bool) or not isinstance(max_nodes, int) or max_nodes <= 0
            or isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0
        ):
            raise LifecycleError("RunInitialized is missing valid frozen graph limits")
        if event.stream_version != 1:
            raise LifecycleConflict("RunInitialized must be lifecycle stream version one")
        return RunLifecycleState(
            run_id=run_id,
            status="created",
            config_hash=payload.get("config_hash"),
            registry_hash=payload.get("registry_hash"),
            max_nodes=max_nodes,
            max_depth=max_depth,
            graph_version=0,
            event_version=event.stream_version,
            nodes=(),
        )

    if event.stream_version != state.event_version + 1:
        raise LifecycleConflict("lifecycle event versions are not contiguous")
    status = state.status
    awaiting_user_request_id = state.awaiting_user_request_id
    cancellation_request_event_id = state.cancellation_request_event_id
    policy_manifest_hash = state.policy_manifest_hash
    graph_version = state.graph_version
    nodes = {item.spec.node_id: item for item in state.nodes}

    if event.event_type == "GraphNodesAppended":
        if status not in {"created", "running"}:
            raise LifecycleError("graph mutation occurred after Run termination")
        next_version = payload.get("graph_version")
        if next_version != graph_version + 1:
            raise LifecycleConflict("graph version is not monotonic")
        manifest_payload = payload.get("policy_manifest")
        proposed_policy_hash = payload.get("policy_manifest_hash")
        if manifest_payload is not None:
            try:
                policy_manifest = PolicyManifest.model_validate(manifest_payload)
            except (TypeError, ValueError) as exc:
                raise LifecycleError("persisted PolicyManifest failed validation") from exc
            if policy_manifest.content_hash != proposed_policy_hash:
                raise LifecycleError("persisted PolicyManifest hash is invalid")
            if policy_manifest_hash is not None and policy_manifest_hash != proposed_policy_hash:
                raise LifecycleError("persisted PolicyManifest changed after Run planning")
            policy_manifest_hash = proposed_policy_hash
        elif proposed_policy_hash is not None and proposed_policy_hash != policy_manifest_hash:
            raise LifecycleError("graph append references an unfrozen PolicyManifest")
        specs = tuple(NodeSpec.model_validate(item) for item in payload.get("nodes", []))
        for spec in specs:
            contract = spec.planning_contract
            if contract is not None and (
                contract.run_id != run_id
                or contract.config_hash != state.config_hash
                or contract.registry_hash != state.registry_hash
                or contract.policy_manifest_hash != policy_manifest_hash
            ):
                raise LifecycleError("persisted node contract does not match its Run snapshot")
        try:
            specs = validate_graph_append(
                tuple(item.spec for item in nodes.values()),
                specs,
                max_nodes=state.max_nodes,
                max_depth=state.max_depth,
            )
        except GraphError as exc:
            raise LifecycleError("persisted graph append violates its frozen limits") from exc
        for spec in specs:
            nodes[spec.node_id] = NodeState(spec=spec, status="blocked")
        for spec in specs:
            ready = not spec.depends_on or all(
                nodes[item].status == "succeeded" for item in spec.depends_on
            )
            if ready:
                nodes[spec.node_id] = nodes[spec.node_id].model_copy(update={"status": "ready"})
        graph_version = next_version
    elif event.event_type == "RunStarted":
        if status != "created" or not nodes:
            raise LifecycleError("invalid RunStarted transition")
        status = "running"
    elif event.event_type == "RunPaused":
        if status != "running":
            raise LifecycleError("invalid RunPaused transition")
        status = "paused"
    elif event.event_type == "RunResumed":
        if status != "paused":
            raise LifecycleError("invalid RunResumed transition")
        status = "running"
    elif event.event_type == "RunAwaitingUser":
        if status != "running" or event.run_id != run_id:
            raise LifecycleError("invalid RunAwaitingUser transition or event identity")
        request_id = payload.get("request_id")
        if not _valid_identifier(request_id) or not re.fullmatch(
            r"[a-z][a-z0-9_]{0,63}", str(payload.get("reason_code", ""))
        ) or not re.fullmatch(_HASH, str(payload.get("context_hash", ""))):
            raise LifecycleError("RunAwaitingUser request metadata is invalid")
        if event.causation_id != _hash(payload):
            raise LifecycleError("RunAwaitingUser causal hash is invalid")
        status = "awaiting_user"
        awaiting_user_request_id = request_id
    elif event.event_type == "RunUserResponseReceived":
        if (
            status != "awaiting_user"
            or event.run_id != run_id
            or payload.get("request_id") != awaiting_user_request_id
            or not re.fullmatch(_HASH, str(payload.get("response_hash", "")))
            or event.causation_id != payload.get("response_hash")
        ):
            raise LifecycleConflict("RunUserResponseReceived does not match the outstanding request")
        status = "running"
        awaiting_user_request_id = None
    elif event.event_type == "RunCancellationRequested":
        if status not in {"created", "running", "paused", "awaiting_user"}:
            raise LifecycleError("invalid RunCancellationRequested transition")
        if (
            event.run_id != run_id
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", str(payload.get("reason_code", "")))
            or event.causation_id != _hash(payload)
        ):
            raise LifecycleError("RunCancellationRequested metadata is invalid")
        status = "cancelling"
        awaiting_user_request_id = None
        cancellation_request_event_id = event.event_id
    elif event.event_type == "RunCancelled":
        request_event_id = payload.get("cancellation_request_id")
        if (
            status != "cancelling"
            or event.run_id != run_id
            or request_event_id != cancellation_request_event_id
            or event.causation_id != request_event_id
        ):
            raise LifecycleError("invalid RunCancelled transition or request reference")
        if any(node.status in {"running", "awaiting_reconciliation"} for node in nodes.values()):
            raise LifecycleError("RunCancelled cannot abandon an active or unknown attempt")
        nodes = {
            node_id: node.model_copy(update={
                "status": "cancelled" if node.status in {"blocked", "ready"} else node.status
            })
            for node_id, node in nodes.items()
        }
        status = "cancelled"
    elif event.event_type == "AttemptAccepted":
        if status != "running":
            raise LifecycleError("attempt accepted while Run is not running")
        node_id = payload.get("node_id")
        node = nodes.get(node_id)
        if node is None or node.status != "ready":
            raise LifecycleError("attempt accepted for a non-ready node")
        attempt = AttemptState.model_validate(payload.get("attempt"))
        if (
            event.run_id != run_id
            or event.node_id != node_id
            or event.attempt_id != attempt.attempt_id
            or event.fencing_generation != attempt.fencing_generation
        ):
            raise LifecycleError("AttemptAccepted event context disagrees with its payload")
        if attempt.fencing_generation != len(node.attempts) + 1:
            raise LifecycleError("attempt generation is not monotonic")
        if len(node.attempts) >= node.spec.max_attempts:
            raise LifecycleError("attempt count exceeds the frozen node limit")
        if any(
            attempt.attempt_id == prior.attempt_id
            for other in nodes.values()
            for prior in other.attempts
        ):
            raise LifecycleError("persisted attempt ID is duplicated in the Run")
        nodes[node_id] = node.model_copy(update={"status": "running", "attempts": (*node.attempts, attempt)})
    elif event.event_type in {"AttemptCompleted", "AttemptReconciled", "AttemptCancelled"}:
        node_id = payload.get("node_id")
        node = nodes.get(node_id)
        if node is None:
            raise LifecycleError("attempt result references an unknown node")
        active = _active_attempt(node)
        if active is None or active.attempt_id != payload.get("attempt_id"):
            raise LifecycleError("attempt result is not for the active generation")
        if event.fencing_generation != active.fencing_generation or event.attempt_id != active.attempt_id:
            raise LifecycleError("attempt result context does not match the active generation")
        outcome = payload.get("outcome")
        if event.run_id != run_id or event.node_id != node_id:
            raise LifecycleError("attempt result event context disagrees with its payload")
        if event.event_type in {"AttemptCompleted", "AttemptCancelled"} and active.status != "accepted":
            raise LifecycleError("only an accepted attempt may complete")
        if event.event_type == "AttemptReconciled" and active.status != "outcome_unknown":
            raise LifecycleError("only an unknown outcome may be reconciled")
        if event.event_type == "AttemptCompleted" and outcome not in {
            "succeeded", "failed", "outcome_unknown"
        }:
            raise LifecycleError("AttemptCompleted has an invalid outcome")
        if event.event_type == "AttemptReconciled" and outcome not in {"succeeded", "failed"}:
            raise LifecycleError("AttemptReconciled has an invalid outcome")
        if event.event_type == "AttemptCancelled" and (
            status != "cancelling"
            or outcome != "cancelled"
            or not re.fullmatch(_HASH, str(payload.get("stop_receipt_hash", "")))
        ):
            raise LifecycleError("AttemptCancelled requires a valid stop receipt during cancellation")
        end_status = "outcome_unknown" if outcome == "outcome_unknown" else outcome
        updated_attempt = active.model_copy(update={"status": end_status})
        attempts = (*node.attempts[:-1], updated_attempt)
        if outcome == "outcome_unknown":
            node_status = "awaiting_reconciliation"
        elif outcome == "succeeded":
            node_status = "succeeded"
        elif outcome == "cancelled":
            node_status = "cancelled"
        elif len(attempts) < node.spec.max_attempts:
            node_status = "ready"
        else:
            node_status = "failed"
        nodes[node_id] = node.model_copy(update={"status": node_status, "attempts": attempts})
        if outcome == "succeeded":
            for child_id, child in tuple(nodes.items()):
                if child.status == "blocked" and all(
                    nodes[parent].status == "succeeded" for parent in child.spec.depends_on
                ):
                    nodes[child_id] = child.model_copy(update={"status": "ready"})
    else:
        raise LifecycleError("unsupported lifecycle event")

    if status == "running" and nodes:
        active_or_ready = any(
            node.status in {"ready", "running", "awaiting_reconciliation"}
            for node in nodes.values()
        )
        if any(node.status == "failed" for node in nodes.values()) and not active_or_ready:
            status = "failed"
        elif all(node.status == "succeeded" for node in nodes.values()):
            status = "succeeded"

    return RunLifecycleState(
        run_id=run_id,
        status=status,
        awaiting_user_request_id=awaiting_user_request_id,
        cancellation_request_event_id=cancellation_request_event_id,
        config_hash=state.config_hash,
        registry_hash=state.registry_hash,
        policy_manifest_hash=policy_manifest_hash,
        max_nodes=state.max_nodes,
        max_depth=state.max_depth,
        graph_version=graph_version,
        event_version=event.stream_version,
        nodes=tuple(nodes[node_id] for node_id in sorted(nodes)),
    )


def _active_attempt(node: NodeState) -> AttemptState | None:
    if node.status not in {"running", "awaiting_reconciliation"}:
        return None
    return next(
        (item for item in reversed(node.attempts) if item.status in {"accepted", "outcome_unknown"}),
        None,
    )


def _check_idempotent(
    events: list[StoredEvent], key: str, event_type: str, payload: dict[str, object]
) -> None:
    existing = [event for event in events if event.idempotency_key == key]
    if len(existing) != 1 or existing[0].event_type != event_type or dict(existing[0].payload) != payload:
        raise LifecycleConflict("lifecycle idempotency key was reused with different input")


def _node_limit(snapshot: RunConfigSnapshot) -> int:
    config = snapshot.resolved_config.config
    requested = config.presets[config.active_preset].requested_budget.max_agents
    envelope = config.policy_envelope.max_agents
    return min(requested, envelope)


def _depth_limit(snapshot: RunConfigSnapshot) -> int:
    config = snapshot.resolved_config.config
    requested = config.presets[config.active_preset].requested_budget.max_depth
    envelope = config.policy_envelope.max_depth
    return min(requested, envelope)


__all__ = ["LifecycleConflict", "LifecycleController", "LifecycleError", "reduce_lifecycle"]
