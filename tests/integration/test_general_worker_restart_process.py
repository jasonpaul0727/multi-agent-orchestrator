"""Real independent-host crashes; Provider and trusted usage are offline fixtures.

These catch replay that resends a captured call, resumes an orphan without an
exact stop, or re-verifies/settles a terminal operation after losing its reply.
"""


def test_sigkill_completion_then_new_process_completes_with_one_call(tmp_path):
    from tests.support.general_worker_restart_harness import run_restart_scenario

    run_restart_scenario(tmp_path, cut="complete")


def test_host_death_after_request_ack_stops_exact_unit_before_fresh_request(tmp_path):
    from tests.support.general_worker_restart_harness import run_restart_scenario

    run_restart_scenario(tmp_path, cut="request")


def test_lost_terminal_reply_returns_identical_history_without_new_work(tmp_path):
    from tests.support.general_worker_restart_harness import run_restart_scenario

    run_restart_scenario(tmp_path, cut="accepted")
