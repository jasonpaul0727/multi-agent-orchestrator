"""Independent-process no-resend/authority matrix; Provider/usage are offline.

Breaks caught: replaying ambiguous effects; bypassing holds with changed public
identity/input; losing ownership or current authority; accepting corrupt private
capture; or persisting only part of the terminal transaction.
"""

import pytest

from tests.support.general_worker_restart_harness import (
    run_blocked_restart, run_competing_recovery, run_late_authority_withdrawal,
    run_rollback_recovery,
)


@pytest.mark.parametrize("cut", ["reserved", "dispatching", "dispatching_effect", "unknown", "known_failure", "no_capture"])
def test_unknown_provider_survives_new_process_without_any_dispatch(tmp_path, cut):
    run_blocked_restart(tmp_path, cut=cut, attacks=True)


def test_two_independent_recovered_hosts_have_one_acceptance_and_settlement(tmp_path):
    run_competing_recovery(tmp_path)


@pytest.mark.parametrize("withdrawal", ["pause", "cancel", "expiry", "fencing"])
def test_current_authority_withdrawn_after_fresh_verifier_blocks_terminal_commit(tmp_path, withdrawal):
    run_late_authority_withdrawal(tmp_path, withdrawal=withdrawal)


@pytest.mark.parametrize("withdrawal", ["pause", "cancel"])
def test_withdrawn_request_orphan_is_stopped_without_a_successor(tmp_path, withdrawal):
    run_blocked_restart(tmp_path, cut="request", mutation=withdrawal)


@pytest.mark.parametrize("fault", ["bytes", "missing", "source", "scope", "capture"])
def test_corrupt_or_unavailable_capture_fails_closed_on_new_host(tmp_path, fault):
    run_blocked_restart(tmp_path, cut="captured", mutation=fault)


def test_terminal_rollback_then_new_process_accepts_proposal_without_provider_reinvoke(tmp_path):
    run_rollback_recovery(tmp_path)
