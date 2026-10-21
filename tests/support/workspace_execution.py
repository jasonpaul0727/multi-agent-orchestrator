"""Real durable acceptance and controller-crash helpers for workspace tests."""

from datetime import timedelta
import multiprocessing


def accept_fresh_generation(case, generation: int):
    from tests.unit.lifecycle.test_scheduler import NOW, routed_pair

    request, decision = routed_pair(
        case.registry,
        case.config,
        case.policy,
        attempt=generation,
        contract_hash=case.contract.contract_hash,
    )
    case.app.accept_routing_with_input(
        request,
        decision,
        task_text=case.text,
        accepted_at=NOW + timedelta(seconds=generation),
        lease_expires_at=NOW + timedelta(hours=1),
    )
    return {
        "run_id": request.run_id,
        "node_id": request.node_id,
        "attempt_id": request.attempt_id,
        "fencing_generation": request.fencing_generation,
    }


def finish_failed_attempt_without_effect(case, identity, *, completed_at):
    case.app._scheduler.finish_attempt(
        **identity,
        completed_at=completed_at,
        outcome="failed",
        known_no_effect=True,
    )


def kill_controller_at_point(run_child, *, expected_point: str, timeout_seconds: int = 30):
    from tests.support.process_crash import kill_at_crash_point

    context = multiprocessing.get_context("fork")
    parent_pipe, child_pipe = context.Pipe()
    process = context.Process(target=run_child, args=(child_pipe,))
    process.start()
    child_pipe.close()
    try:
        kill_at_crash_point(
            process,
            parent_pipe,
            expected_point=expected_point,
            timeout_seconds=timeout_seconds,
        )
    finally:
        parent_pipe.close()
    return process.exitcode


class DurableAcceptedAttemptAuthority:
    """Approval authority backed by the committed Lifecycle event stream."""

    def __init__(self, database):
        self._database = database

    def is_current(self, attempt):
        from orchestrator.lifecycle.controller import reduce_lifecycle
        from orchestrator.persistence import SQLiteEventStore

        try:
            events = SQLiteEventStore.open_read_only(self._database)
            try:
                state = reduce_lifecycle(
                    attempt.run_id,
                    events.read_stream("run_lifecycle", attempt.run_id),
                )
                node = state.node(attempt.node_id)
                current = node.attempts[-1]
                return (
                    node.status == "running"
                    and current.status == "accepted"
                    and current.attempt_id == attempt.attempt_id
                    and current.fencing_generation == attempt.fencing_generation
                )
            finally:
                events.close()
        except Exception:
            return False
