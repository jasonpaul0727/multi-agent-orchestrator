"""Pure correlation tests; input provenance and unit lifecycle are external."""
import pytest

from orchestrator.isolation._systemd_job_correlation import JobCorrelation

OWNER, EPOCH, UNIT, PATH = ":1.42", 3, "own.service", "/org/freedesktop/systemd1/job/19"


def ready(**limits):
    state = JobCorrelation(UNIT, **limits)
    state.subscribe(OWNER, EPOCH)
    state.dispatch(OWNER, EPOCH)
    return state


def test_signal_before_reply_and_duplicate_are_correlated():
    state = ready()
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert state.observation is None
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    assert state.observation.job_id == 19
    assert not state.unknown


def test_reply_before_signal_and_unrelated_noise():
    state = ready()
    state.signal(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", "other.service", "failed")
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    assert state.observation is None
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert state.observation.job_path == PATH


@pytest.mark.parametrize("scenario", ["dispatch_first", "signal_first", "reply_first", "bad_owner", "epoch_change", "disconnect", "timeout"])
def test_unknown_is_sticky(scenario):
    state = JobCorrelation(UNIT)
    if scenario == "dispatch_first": state.dispatch(OWNER, EPOCH)
    elif scenario == "signal_first": state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    elif scenario == "reply_first": state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    else:
        state.subscribe(OWNER, EPOCH)
        state.dispatch(OWNER, EPOCH)
        if scenario == "bad_owner": state.signal(":1.43", EPOCH, 19, PATH, UNIT, "done")
        elif scenario == "epoch_change": state.signal(OWNER, EPOCH + 1, 19, PATH, UNIT, "done")
        elif scenario == "disconnect": state.disconnect()
        else: state.timeout()
    assert state.unknown
    state.subscribe(OWNER, EPOCH)
    state.dispatch(OWNER, EPOCH)
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert state.unknown and state.observation is None


@pytest.mark.parametrize("event", [
    (20, PATH, UNIT, "done"), (19, PATH, "other.service", "done"),
    (19, PATH, UNIT, "failed"), (True, PATH, UNIT, "done"),
    (19, PATH, UNIT, ""), (19, PATH, UNIT, "done\x00"),
])
def test_malformed_or_conflicting_signal_is_unknown(event):
    state = ready()
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    state.signal(OWNER, EPOCH, *event)
    assert state.unknown and state.observation is None


@pytest.mark.parametrize("reply", [(20, PATH, UNIT), (19, PATH, "other.service"),
    (19, "/org/freedesktop/systemd1/job/019", UNIT)])
def test_reply_identity_mismatch_is_unknown(reply):
    state = ready()
    state.reply(OWNER, EPOCH, *reply)
    assert state.unknown


def test_noise_related_by_job_id_conflicts_and_duplicates_conflict():
    state = ready()
    state.signal(OWNER, EPOCH, 19, PATH, "other.service", "done")
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    assert state.unknown
    state = ready()
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "failed")
    assert state.unknown


def test_bounded_events_and_bytes_fail_closed():
    state = ready(max_events=1, max_bytes=1000)
    state.signal(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", "other.service", "done")
    state.signal(OWNER, EPOCH, 21, "/org/freedesktop/systemd1/job/21", "third.service", "done")
    assert state.unknown
    state = ready(max_events=8, max_bytes=10)
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert state.unknown


def test_exact_duplicate_at_limit_and_post_completion_disconnect():
    size = 8 + len(PATH) + len(UNIT) + len("done")
    state = ready(max_events=1, max_bytes=size)
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    assert state.observation is not None and not state.unknown
    state.disconnect()
    assert state.unknown and state.observation is None


def test_same_unit_different_job_and_conflicting_reply_fail_closed():
    state = ready()
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    state.signal(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", UNIT, "done")
    assert state.unknown
    state = ready()
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    state.reply(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", UNIT)
    assert state.unknown


@pytest.mark.parametrize("owner,epoch", [("org.freedesktop.systemd1", 3), (":1.2", True), (":1.2", 0)])
def test_bad_subscription_identity_fails_closed(owner, epoch):
    state = JobCorrelation(UNIT)
    state.subscribe(owner, epoch)
    assert state.unknown


def test_unknown_is_not_a_public_resettable_flag():
    state = JobCorrelation(UNIT)
    state.timeout()
    with pytest.raises(AttributeError):
        state.unknown = False
    assert state.unknown


@pytest.mark.parametrize("noise", ["dbus.socket", "multi-user.target", "nightly.timer",
    "mnt-data.mount", "dev-disk.device", r"my\x2dunit.service"])
def test_systemd_noise_types_and_escaped_units_do_not_block_matching(noise):
    state = ready()
    state.signal(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", noise, "done")
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert state.observation is not None and not state.unknown


def test_pre_dispatch_noise_is_discarded_and_target_conflict_holds():
    state = JobCorrelation(UNIT)
    state.subscribe(OWNER, EPOCH)
    for i in range(300):
        state.signal(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", "other.socket", "done")
    state.dispatch(OWNER, EPOCH)
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert state.observation is not None
    target = JobCorrelation(UNIT)
    target.subscribe(OWNER, EPOCH)
    target.signal(OWNER, EPOCH, 19, PATH, UNIT, "done")
    assert target.unknown
    bad = JobCorrelation(UNIT)
    bad.subscribe(OWNER, EPOCH)
    bad.signal(":1.43", EPOCH, 20, "/org/freedesktop/systemd1/job/20", "other.socket", "done")
    assert bad.unknown


def test_unique_owner_alphanumeric_and_hyphen_segments():
    owner = ":_client.A-b9"
    state = JobCorrelation(UNIT)
    state.subscribe(owner, EPOCH)
    state.dispatch(owner, EPOCH)
    state.reply(owner, EPOCH, 19, PATH, UNIT)
    state.signal(owner, EPOCH, 19, PATH, UNIT, "done")
    assert state.observation is not None


def test_concurrent_events_do_not_exceed_budget():
    import threading
    state = ready(max_events=1, max_bytes=1000)
    barrier = threading.Barrier(3)
    def send(i):
        barrier.wait()
        state.signal(OWNER, EPOCH, i, "/org/freedesktop/systemd1/job/" + str(i), "noise" + str(i) + ".socket", "done")
    threads = [threading.Thread(target=send, args=(i,)) for i in (20, 21)]
    for thread in threads: thread.start()
    barrier.wait()
    for thread in threads: thread.join(5)
    assert all(not thread.is_alive() for thread in threads)
    assert state.unknown and state.observation is None


def test_concurrent_disconnect_cannot_be_undone_by_signal_or_reply():
    import threading
    state = ready()
    barrier = threading.Barrier(4)
    def invoke(fn):
        barrier.wait()
        fn()
    threads = [threading.Thread(target=invoke, args=(fn,)) for fn in (
        lambda: state.reply(OWNER, EPOCH, 19, PATH, UNIT),
        lambda: state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done"),
        state.disconnect)]
    for thread in threads: thread.start()
    barrier.wait()
    for thread in threads: thread.join(5)
    assert all(not thread.is_alive() for thread in threads)
    assert state.unknown and state.observation is None


def test_disconnect_waits_for_inflight_update_and_leaves_unknown(monkeypatch):
    import threading
    state = ready()
    state.reply(OWNER, EPOCH, 19, PATH, UNIT)
    entered, release, disconnected = threading.Event(), threading.Event(), threading.Event()
    reconcile = state._reconcile
    def pause():
        entered.set()
        assert release.wait(5)
        reconcile()
    monkeypatch.setattr(state, "_reconcile", pause)
    first = threading.Thread(target=lambda: state.signal(OWNER, EPOCH, 19, PATH, UNIT, "done"))
    second = threading.Thread(target=lambda: (state.disconnect(), disconnected.set()))
    first.start()
    try:
        assert entered.wait(5)
        second.start()
        assert not disconnected.wait(0.05)
    finally:
        release.set()
        first.join(5)
        second.join(5)
    assert not first.is_alive() and not second.is_alive()
    assert state.unknown and state.observation is None


@pytest.mark.parametrize("owner", [":one", ":.two", ":one.", ":ab..cd", ":ab.+cd", ":" + "a" * 254 + ".z"])
def test_malformed_or_oversize_unique_owner_is_unknown(owner):
    state = JobCorrelation(UNIT)
    state.subscribe(owner, EPOCH)
    assert state.unknown


@pytest.mark.parametrize("unit", [r"bad\x2.service", r"bad\xzz.service", "bad/device.service", "a" * 250 + ".service"])
def test_malformed_or_oversize_noise_is_unknown(unit):
    state = ready()
    state.signal(OWNER, EPOCH, 20, "/org/freedesktop/systemd1/job/20", unit, "done")
    assert state.unknown
