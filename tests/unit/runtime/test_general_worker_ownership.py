"""Real advisory ownership survives cancellation before descriptor transfer."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import errno
import fcntl
import os
import threading

import pytest

from orchestrator.runtime.general_worker_recovery import GeneralWorkerOwnership
from tests.unit.lifecycle.test_scheduler import _resolver_durable_state
from tests.unit.runtime.test_general_worker_execution import execution_case
from tests.unit.runtime.test_general_worker_recovery import checkpoint, no_external_work


class RecordingExecutor(ThreadPoolExecutor):
    """Retain the actual worker Future even when asyncio cancels its wrapper."""
    def __init__(self):
        super().__init__(max_workers=2)
        self.submitted = []

    def submit(self, function, /, *args, **kwargs):
        future = super().submit(function, *args, **kwargs)
        self.submitted.append(future)
        return future


def hold_actual_flock(monkeypatch, target):
    """Pause AFTER real Linux flock succeeds, BEFORE acquire can assign its FD."""
    main_thread = threading.get_ident()
    entered, release = threading.Event(), threading.Event()
    descriptors = []
    original = fcntl.flock
    def flock(descriptor, operation):
        result = original(descriptor, operation)
        if (threading.get_ident() != main_thread and operation == fcntl.LOCK_EX | fcntl.LOCK_NB
            and os.readlink(f"/proc/self/fd/{descriptor}") == str(target)):
            descriptors.append(descriptor)
            entered.set()
            assert release.wait(5), "real acquisition barrier timed out"
        return result
    monkeypatch.setattr(fcntl, "flock", flock)
    return entered, release, descriptors


def descriptor_closed(descriptor):
    try:
        os.fstat(descriptor)
    except OSError as error:
        assert error.errno == errno.EBADF
        return True
    return False


async def wait_until(predicate):
    while not predicate():
        await asyncio.sleep(0.001)


@pytest.mark.parametrize("cancel_count", [1, 2, 5])
def test_actual_service_cancelled_claim_closes_eventual_descriptor(tmp_path, monkeypatch, cancel_count):
    from orchestrator.runtime import general_worker_execution as module
    case = execution_case(tmp_path)
    owners = []
    executor = RecordingExecutor()
    release = None
    try:
        operation = checkpoint(case, monkeypatch)
        root = case.service._owner_root
        entered, release, descriptors = hold_actual_flock(monkeypatch, root / (operation.operation_id + ".lock"))
        # Record real instances; neither acquire nor close is substituted.
        def construct(root, operation_id):
            owner = GeneralWorkerOwnership(root, operation_id)
            owners.append(owner)
            return owner
        monkeypatch.setattr(module, "GeneralWorkerOwnership", construct)
        no_external_work(case, monkeypatch, verifier=True)
        def forbidden(*args, **kwargs):
            pytest.fail("cancelled claim continued into input/artifact work")
        monkeypatch.setattr(case.app._worker_inputs, "_restore_general_worker_task", forbidden)
        monkeypatch.setattr(case.app._artifact_grants, "issue", forbidden)
        before = _resolver_durable_state(case.app._event_store)
        async def run():
            asyncio.get_running_loop().set_default_executor(executor)
            task = asyncio.create_task(case.service.recover(identity=case.identity, task_text=case.text))
            try:
                await asyncio.wait_for(wait_until(entered.is_set), timeout=2)
                assert owners[0]._descriptor is None
                contender = GeneralWorkerOwnership(root, operation.operation_id)
                try:
                    assert contender.acquire() is False  # Actual old flock is already held.
                finally:
                    contender.close()
                for _ in range(cancel_count):
                    task.cancel()
                    await asyncio.sleep(0)
                if cancel_count > 1:
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task, timeout=1)
                    assert not executor.submitted[0].done()
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                await asyncio.wait_for(wait_until(executor.submitted[0].done), timeout=2)
                executor.submitted[0].result()  # Real acquire has returned, including its FD cleanup.
                was_closed = descriptor_closed(descriptors[0])  # Check before a new open may reuse the FD number.
                fresh = GeneralWorkerOwnership(root, operation.operation_id)
                try:
                    assert fresh.acquire(), "abandoned real acquisition stranded the operation flock"
                    assert was_closed, "abandoned descriptor remained open after real worker completion"
                    assert owners[0]._descriptor is None
                finally:
                    fresh.close()
                assert _resolver_durable_state(case.app._event_store) == before
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                await asyncio.wait_for(wait_until(lambda: all(future.done() for future in executor.submitted)), timeout=2)
                for owner in owners:
                    owner.close()
        asyncio.run(run())
    finally:
        if release is not None:
            release.set()
        executor.shutdown(wait=True)
        for owner in owners:
            owner.close()
        case.app.close()


def test_close_before_real_acquisition_assignment_is_terminal_without_event_loop(tmp_path, monkeypatch):
    owner = GeneralWorkerOwnership(tmp_path, "operation")
    entered, release, descriptors = hold_actual_flock(monkeypatch, tmp_path / "operation.lock")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(owner.acquire)
        try:
            assert entered.wait(2)
            assert owner._descriptor is None
            owner.close()
            owner.close()
            release.set()
            acquired = future.result(timeout=2)
            was_closed = descriptor_closed(descriptors[0])
            fresh = GeneralWorkerOwnership(tmp_path, "operation")
            try:
                assert fresh.acquire(), "close-before-transfer leaked a real flock without an event loop"
                assert acquired is False and was_closed and owner._descriptor is None
            finally:
                fresh.close()
        finally:
            release.set()
            future.result(timeout=2)
            owner.close()


def test_uncancelled_claim_holds_only_its_operation_until_close(tmp_path):
    owner = GeneralWorkerOwnership(tmp_path, "one")
    contender = GeneralWorkerOwnership(tmp_path, "one")
    unrelated = GeneralWorkerOwnership(tmp_path, "two")
    fresh = GeneralWorkerOwnership(tmp_path, "one")
    try:
        assert owner.acquire() is True
        assert contender.acquire() is False
        assert unrelated.acquire() is True
        descriptor = owner._descriptor
        owner.close()
        owner.close()
        assert descriptor_closed(descriptor)
        assert fresh.acquire() is True
    finally:
        owner.close()
        contender.close()
        unrelated.close()
        fresh.close()


def test_closed_claim_never_opens_again(tmp_path, monkeypatch):
    owner = GeneralWorkerOwnership(tmp_path, "closed")
    owner.close()
    def forbidden(*args, **kwargs):
        pytest.fail("terminally closed ownership opened a new descriptor")
    monkeypatch.setattr(os, "open", forbidden)
    assert owner.acquire() is False
