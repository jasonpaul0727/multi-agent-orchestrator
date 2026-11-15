"""Finite synthetic model only: no production authorization or provider effects."""

import importlib
import importlib.util
from dataclasses import FrozenInstanceError, replace

import pytest


@pytest.fixture
def sim():
    assert importlib.util.find_spec("tests.support.dual_view_simulation") is not None
    return importlib.import_module("tests.support.dual_view_simulation")


def register(sim, history=None):
    history = (sim.Model.M0,) if history is None else history
    state, observation = sim.step(sim.initial_state(), sim.RegisterSource(sim.Source.S0, history, sim.Fixture.NORMAL))
    assert observation.outcome is sim.Outcome.REGISTERED
    return state


def fact(sim, state, name, value, clock=0):
    return sim.step(state, sim.SetFacts(sim.Source.S0, ((name, value),), clock))


def ready(sim, history=None):
    state = register(sim, history)
    for name in (sim.Fact.FAILED, sim.Fact.LATEST, sim.Fact.INPUT, sim.Fact.CLOSED,
                 sim.Fact.NO_EFFECT, sim.Fact.POLICY, sim.Fact.HEALTH, sim.Fact.RESOURCE):
        state, result = fact(sim, state, name, sim.Value.YES)
        assert result.outcome is sim.Outcome.RECORDED
    return state


def proposal(sim, version, *, model=None, provider=None, eligibility=None):
    source = sim.Source.S0
    model = sim.Model.M0 if model is None else model
    provider = sim.Provider.P0 if provider is None else provider
    if eligibility is None and version is sim.Version.B:
        eligibility = sim.Eligibility(source, sim.Model.M0, sim.Provider.P0,
                                      sim.Evidence.E0, sim.Policy.P0, 2, 8)
    return sim.Propose(sim.Envelope(source, sim.Request.R0, version, sim.Entry.RECOVERY,
                                   sim.Action.SAME, model, provider, (sim.Model.M0,),
                                   eligibility, 0, 0, 8, sim.Fixture.NORMAL))


def test_source_registration_preserves_history_and_exact_repetition_is_idempotent():
    assert importlib.util.find_spec("tests.support.dual_view_simulation") is not None
    sim = importlib.import_module("tests.support.dual_view_simulation")
    state = register(sim, (sim.Model.M1, sim.Model.M0))
    same, out = sim.step(state, sim.RegisterSource(sim.Source.S0, (sim.Model.M1, sim.Model.M0), sim.Fixture.NORMAL))
    assert same == state
    assert out.outcome is sim.Outcome.REGISTERED
    changed, out = sim.step(state, sim.RegisterSource(sim.Source.S0, (sim.Model.M0, sim.Model.M1), sim.Fixture.NORMAL))
    assert changed == state
    assert out.outcome is sim.Outcome.HELD_INVALID
    assert state.sources[0].history == (sim.Model.M1, sim.Model.M0)
    assert out.simulation_only and out.non_authoritative


def test_nested_mutable_and_duplicate_histories_are_rejected_without_changes(sim):
    state = sim.initial_state()
    for history in ([sim.Model.M0], (sim.Model.M0, sim.Model.M0), ("M0",)):
        result, out = sim.step(state, sim.RegisterSource(sim.Source.S0, history, sim.Fixture.NORMAL))
        assert result == state
        assert out.outcome is sim.Outcome.HELD_INVALID


def test_mutable_command_and_boolean_clock_are_rejected(sim):
    state = register(sim)
    for command in (sim.SetFacts(sim.Source.S0, [(sim.Fact.CLOSED, sim.Value.YES)], 0),
                    sim.SetFacts(sim.Source.S0, ((sim.Fact.CLOSED, sim.Value.YES),), True)):
        result, out = sim.step(state, command)
        assert result == state
        assert out.outcome is sim.Outcome.HELD_INVALID


def test_atomic_stop_and_effect_same_command_rejected(sim):
    state = register(sim)
    after, result = sim.step(state, sim.SetFacts(sim.Source.S0, ((sim.Fact.CLOSED, sim.Value.YES),
                                                               (sim.Fact.NO_EFFECT, sim.Value.YES)), 0))
    assert after == state
    assert result.outcome is sim.Outcome.HELD_INVALID


def test_effect_before_stop_then_repeated_effect_cannot_gain_eligibility(sim):
    state = register(sim)
    state, _ = fact(sim, state, sim.Fact.NO_EFFECT, sim.Value.YES)
    state, _ = fact(sim, state, sim.Fact.CLOSED, sim.Value.YES)
    state, _ = fact(sim, state, sim.Fact.NO_EFFECT, sim.Value.YES)
    for name in (sim.Fact.FAILED, sim.Fact.LATEST, sim.Fact.INPUT,
                 sim.Fact.POLICY, sim.Fact.HEALTH, sim.Fact.RESOURCE):
        state, _ = fact(sim, state, name, sim.Value.YES)
    before = state
    after, out = sim.step(state, proposal(sim, sim.Version.B))
    assert after == before
    assert out.outcome is sim.Outcome.HELD


def test_eligible_b_keeps_history_and_v1_remains_excluded(sim):
    state = ready(sim)
    held, out = sim.step(state, proposal(sim, sim.Version.V1))
    assert held == state
    assert out.outcome is sim.Outcome.HELD
    allowed, out = sim.step(state, proposal(sim, sim.Version.B))
    assert out.outcome is sim.Outcome.PROPOSED
    assert allowed.sources[0].history == (sim.Model.M0,)
    assert out.simulation_only and out.non_authoritative


def test_eligibility_must_bind_exact_source_model_and_provider(sim):
    state = ready(sim)
    valid = proposal(sim, sim.Version.B)
    for bad in (replace(valid.envelope.eligibility, source=sim.Source.S1),
                replace(valid.envelope.eligibility, model=sim.Model.M1),
                replace(valid.envelope.eligibility, provider=sim.Provider.P1),
                replace(valid.envelope.eligibility, evidence=sim.Evidence.E1),
                replace(valid.envelope.eligibility, retry_level=True)):
        after, out = sim.step(state, sim.Propose(replace(valid.envelope, eligibility=bad)))
        assert after == state
        assert out.outcome is sim.Outcome.HELD_INVALID


def test_unknown_source_outcome_never_becomes_yes(sim):
    state = register(sim)
    state, _ = fact(sim, state, sim.Fact.FAILED, sim.Value.UNKNOWN)
    same, out = fact(sim, state, sim.Fact.FAILED, sim.Value.YES)
    assert same == state
    assert out.outcome is sim.Outcome.HELD
    after, out = sim.step(state, proposal(sim, sim.Version.B))
    assert after == state
    assert out.outcome is sim.Outcome.RECONCILIATION


def test_state_is_frozen_and_source_fact_cannot_recover_after_no(sim):
    state = register(sim)
    with pytest.raises(FrozenInstanceError):
        state.sources = ()
    state, _ = fact(sim, state, sim.Fact.CLOSED, sim.Value.NO)
    same, out = fact(sim, state, sim.Fact.CLOSED, sim.Value.YES)
    assert same == state
    assert out.outcome is sim.Outcome.HELD


def test_dynamic_no_may_recover_but_clock_cannot_go_back(sim):
    state = register(sim)
    state, _ = fact(sim, state, sim.Fact.HEALTH, sim.Value.NO, 2)
    state, out = fact(sim, state, sim.Fact.HEALTH, sim.Value.YES, 3)
    assert out.outcome is sim.Outcome.RECORDED
    same, out = fact(sim, state, sim.Fact.HEALTH, sim.Value.YES, 2)
    assert same == state
    assert out.outcome is sim.Outcome.HELD_INVALID



def test_proposal_replay_is_historical_even_after_facts_change(sim):
    state = ready(sim)
    command = proposal(sim, sim.Version.B)
    state, out = sim.step(state, command)
    assert out.outcome is sim.Outcome.PROPOSED
    state, _ = fact(sim, state, sim.Fact.CLOSED, sim.Value.NO)
    same, out = sim.step(state, command)
    assert same == state
    assert out.outcome is sim.Outcome.PROPOSED
    altered = sim.Propose(replace(command.envelope, entry=sim.Entry.INITIAL))
    same, out = sim.step(state, altered)
    assert same == state
    assert out.outcome is sim.Outcome.CONFLICT


def test_eligibility_expiry_is_independent_from_request_expiry(sim):
    state = ready(sim)
    command = proposal(sim, sim.Version.B)
    eligibility = replace(command.envelope.eligibility, expires=2)
    command = sim.Propose(replace(command.envelope, eligibility=eligibility))
    state, out = sim.step(state, command)
    assert out.outcome is sim.Outcome.PROPOSED
    state, _ = fact(sim, state, sim.Fact.HEALTH, sim.Value.YES, 3)
    later = sim.Propose(replace(command.envelope, request=sim.Request.R1))
    same, out = sim.step(state, later)
    assert same == state
    assert out.outcome is sim.Outcome.HELD


def test_nested_mutable_history_in_forged_event_is_rejected(sim):
    state = register(sim)
    bad_event = replace(state.events[0], command=sim.RegisterSource(sim.Source.S0, [sim.Model.M0], sim.Fixture.NORMAL))
    malformed = replace(state, events=(bad_event,))
    same, out = sim.step(malformed, sim.RegisterSource(sim.Source.S0, (sim.Model.M0,), sim.Fixture.NORMAL))
    assert same is malformed
    assert out.outcome is sim.Outcome.HELD_INVALID


def test_initial_state_is_frozen_and_explicitly_non_authoritative(sim):
    state = sim.initial_state()
    assert getattr(state, "simulation_only", None) is True
    assert getattr(state, "non_authoritative", None) is True
    with pytest.raises(FrozenInstanceError):
        state.simulation_only = False


def test_transitions_keep_both_non_authoritative_state_marks(sim):
    state = register(sim)
    assert state.simulation_only is True
    assert state.non_authoritative is True
    state, _ = fact(sim, state, sim.Fact.CLOSED, sim.Value.YES)
    assert state.simulation_only is True
    assert state.non_authoritative is True


@pytest.mark.parametrize("changes", [
    {"simulation_only": False}, {"non_authoritative": False},
    {"simulation_only": 1}, {"non_authoritative": 1},
])
def test_forged_state_marks_hold_without_any_transition(sim, changes):
    forged = replace(sim.initial_state(), **changes)
    after, out = sim.step(forged, sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL))
    assert after is forged
    assert out.outcome is sim.Outcome.HELD_INVALID
    assert out.simulation_only is True
    assert out.non_authoritative is True


def _pair(sim, excluded):
    history = (sim.Model.M0,) if excluded else ()
    state = ready(sim, history)
    b = proposal(sim, sim.Version.B).envelope
    b = replace(b, history=history, request=sim.Request.R0)
    old = sim.Envelope(sim.Source.S0, sim.Request.R1, sim.Version.V1, sim.Entry.INITIAL,
                       sim.Action.FALLBACK if excluded else sim.Action.SAME,
                       sim.Model.M1 if excluded else sim.Model.M0,
                       sim.Provider.P1 if excluded else sim.Provider.P0,
                       history, None, 0, 0, 8, sim.Fixture.NORMAL)
    return state, b, old


@pytest.mark.parametrize("excluded", [False, True])
@pytest.mark.parametrize("b_first", [False, True])
@pytest.mark.parametrize("b_entry", ["INITIAL", "PROBE", "RECOVERY", "IMPORT"])
@pytest.mark.parametrize("old_entry", ["INITIAL", "PROBE", "RECOVERY", "IMPORT"])
def test_competing_versions_have_one_winner_and_reservation(sim, excluded, b_first, b_entry, old_entry):
    state, b, old = _pair(sim, excluded)
    b, old = replace(b, entry=sim.Entry[b_entry]), replace(old, entry=sim.Entry[old_entry])
    for envelope in (b, old):
        state, out = sim.step(state, sim.Propose(envelope))
        assert out.outcome is sim.Outcome.PROPOSED
        state, out = sim.step(state, sim.PrepareCommit(envelope))
        assert out.outcome is sim.Outcome.PREPARED
    first, second = (b, old) if b_first else (old, b)
    state, out = sim.step(state, sim.Commit(first))
    assert out.outcome is sim.Outcome.WINNER
    assert len(state.winners) == 1
    assert state.winners[0].envelope == first
    assert state.sources[0].budget == 0
    assert state.sources[0].attempts == 3
    same, out = sim.step(state, sim.Commit(first))
    assert same == state
    assert out.outcome is sim.Outcome.WINNER
    same, out = sim.step(state, sim.Commit(second))
    assert same == state
    assert out.outcome is sim.Outcome.SUPERSEDED
    same, out = sim.step(state, sim.SimulateDispatchCheck(first))
    assert same == state
    assert out.outcome is sim.Outcome.PRECONDITIONS_MET_NON_AUTHORITY
    assert same.sources[0].budget == 0
    same, out = sim.step(state, sim.SimulateDispatchCheck(second))
    assert same == state
    assert out.outcome is sim.Outcome.HELD


def test_prepare_requires_exact_proposal_and_commit_requires_pending(sim):
    state, b, _ = _pair(sim, True)
    same, out = sim.step(state, sim.PrepareCommit(b))
    assert same == state and out.outcome is sim.Outcome.HELD
    state, _ = sim.step(state, sim.Propose(b))
    same, out = sim.step(state, sim.Commit(b))
    assert same == state and out.outcome is sim.Outcome.HELD
    state, out = sim.step(state, sim.PrepareCommit(b))
    assert out.outcome is sim.Outcome.PREPARED
    same, out = sim.step(state, sim.PrepareCommit(b))
    assert same == state and out.outcome is sim.Outcome.PREPARED
    altered = replace(b, entry=sim.Entry.IMPORT)
    same, out = sim.step(state, sim.Commit(altered))
    assert same == state and out.outcome is sim.Outcome.CONFLICT


@pytest.mark.parametrize("fixture", ["ATTEMPTS_FULL", "RETRY_FULL", "BUDGET_EMPTY"])
def test_exhausted_synthetic_resource_cannot_create_winner(sim, fixture):
    source = sim.Source.S0
    setting = sim.Fixture[fixture]
    state, _ = sim.step(sim.initial_state(), sim.RegisterSource(source, (sim.Model.M0,), setting))
    for name in (sim.Fact.FAILED, sim.Fact.LATEST, sim.Fact.INPUT, sim.Fact.CLOSED,
                 sim.Fact.NO_EFFECT, sim.Fact.POLICY, sim.Fact.HEALTH, sim.Fact.RESOURCE):
        state, _ = fact(sim, state, name, sim.Value.YES)
    b = replace(proposal(sim, sim.Version.B).envelope, fixture=setting)
    state, out = sim.step(state, sim.Propose(b))
    if out.outcome is sim.Outcome.PROPOSED:
        state, out = sim.step(state, sim.PrepareCommit(b))
        if out.outcome is sim.Outcome.PREPARED:
            state, out = sim.step(state, sim.Commit(b))
    assert out.outcome is sim.Outcome.HELD
    assert not state.winners


def test_closed_generation_rejects_late_commit_and_new_generation_must_revalidate(sim):
    state, b, _ = _pair(sim, True)
    state, _ = sim.step(state, sim.Propose(b))
    state, _ = sim.step(state, sim.PrepareCommit(b))
    state, out = sim.step(state, sim.CloseGeneration(sim.Source.S0, 0))
    assert out.outcome is sim.Outcome.RECORDED
    same, out = sim.step(state, sim.Commit(b))
    assert same == state and out.outcome is sim.Outcome.HELD
    generation1 = replace(b, request=sim.Request.R1, generation=1)
    state, out = sim.step(state, sim.Propose(generation1))
    assert out.outcome is sim.Outcome.PROPOSED
    state, out = sim.step(state, sim.PrepareCommit(generation1))
    assert out.outcome is sim.Outcome.PREPARED
    state, _ = fact(sim, state, sim.Fact.CLOSED, sim.Value.NO)
    same, out = sim.step(state, sim.Commit(generation1))
    assert same == state and out.outcome is sim.Outcome.HELD
    assert not state.winners


@pytest.mark.parametrize("point", ["before_propose", "before_prepare", "before_commit", "after_winner"])
def test_unknown_at_every_phase_requires_reconciliation_without_new_spend(sim, point):
    state, b, _ = _pair(sim, True)
    if point == "before_propose":
        state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
        same, out = sim.step(state, sim.Propose(b))
    else:
        state, _ = sim.step(state, sim.Propose(b))
        if point == "before_prepare":
            state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
            same, out = sim.step(state, sim.PrepareCommit(b))
        else:
            state, _ = sim.step(state, sim.PrepareCommit(b))
            if point == "before_commit":
                state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
                same, out = sim.step(state, sim.Commit(b))
            else:
                state, _ = sim.step(state, sim.Commit(b))
                state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
                same, out = sim.step(state, sim.SimulateDispatchCheck(b))
    assert same == state
    assert out.outcome is sim.Outcome.RECONCILIATION
    assert len(state.winners) == (1 if point == "after_winner" else 0)
    assert state.sources[0].budget == (0 if point == "after_winner" else 1)


def test_revoked_reservation_cannot_dispatch_or_reverse_a_historical_winner(sim):
    state, b, _ = _pair(sim, True)
    state, _ = sim.step(state, sim.Propose(b))
    state, _ = sim.step(state, sim.PrepareCommit(b))
    state, _ = sim.step(state, sim.Commit(b))
    state, out = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.RESERVATION))
    assert out.outcome is sim.Outcome.RECORDED
    same, out = sim.step(state, sim.SimulateDispatchCheck(b))
    assert same == state and out.outcome is sim.Outcome.HELD
    same, out = sim.step(state, sim.Commit(b))
    assert same == state and out.outcome is sim.Outcome.WINNER
    assert state.sources[0].budget == 0 and len(state.winners) == 1


def test_unknown_outcome_remains_reconciliation_even_if_reservation_is_revoked(sim):
    state, b, _ = _pair(sim, True)
    state, _ = sim.step(state, sim.Propose(b))
    state, _ = sim.step(state, sim.PrepareCommit(b))
    state, _ = sim.step(state, sim.Commit(b))
    state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
    state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.RESERVATION))
    same, out = sim.step(state, sim.SimulateDispatchCheck(b))
    assert same == state
    assert out.outcome is sim.Outcome.RECONCILIATION
    assert state.sources[0].budget == 0 and len(state.winners) == 1


def test_two_distinct_synthetic_sources_do_not_share_resources(sim):
    state = ready(sim)
    state, out = sim.step(state, sim.RegisterSource(sim.Source.S1, (sim.Model.M1,), sim.Fixture.NORMAL))
    assert out.outcome is sim.Outcome.REGISTERED
    for name in (sim.Fact.FAILED, sim.Fact.LATEST, sim.Fact.INPUT, sim.Fact.CLOSED,
                 sim.Fact.NO_EFFECT, sim.Fact.POLICY, sim.Fact.HEALTH, sim.Fact.RESOURCE):
        state, out = sim.step(state, sim.SetFacts(sim.Source.S1, ((name, sim.Value.YES),), 0))
        assert out.outcome is sim.Outcome.RECORDED
    b0 = proposal(sim, sim.Version.B).envelope
    b1 = sim.Envelope(sim.Source.S1, sim.Request.R0, sim.Version.B, sim.Entry.RECOVERY,
                      sim.Action.SAME, sim.Model.M1, sim.Provider.P1, (sim.Model.M1,),
                      sim.Eligibility(sim.Source.S1, sim.Model.M1, sim.Provider.P1,
                                      sim.Evidence.E1, sim.Policy.P0, 2, 8),
                      0, 0, 8, sim.Fixture.NORMAL)
    for envelope in (b0, b1):
        state, out = sim.step(state, sim.Propose(envelope))
        assert out.outcome is sim.Outcome.PROPOSED
        state, out = sim.step(state, sim.PrepareCommit(envelope))
        assert out.outcome is sim.Outcome.PREPARED
        state, out = sim.step(state, sim.Commit(envelope))
        assert out.outcome is sim.Outcome.WINNER
    assert len(state.winners) == 2
    assert [source.budget for source in state.sources] == [0, 0]
    for envelope in (b0, b1):
        same, out = sim.step(state, sim.SimulateDispatchCheck(envelope))
        assert same == state and out.outcome is sim.Outcome.PRECONDITIONS_MET_NON_AUTHORITY


@pytest.mark.parametrize("reason", ["REVOKED", "STALE", "INPUT", "EFFECT", "OLD_SEND", "RESERVATION"])
@pytest.mark.parametrize("point", ["before_propose", "before_prepare", "before_commit", "after_winner"])
def test_each_monotone_invalidation_blocks_at_every_phase(sim, reason, point):
    state, b, _ = _pair(sim, True)
    if point != "before_propose":
        state, _ = sim.step(state, sim.Propose(b))
    if point in ("before_commit", "after_winner"):
        state, _ = sim.step(state, sim.PrepareCommit(b))
    if point == "after_winner":
        state, _ = sim.step(state, sim.Commit(b))
    state, out = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation[reason]))
    assert out.outcome is sim.Outcome.RECORDED
    command = {"before_propose": sim.Propose, "before_prepare": sim.PrepareCommit,
               "before_commit": sim.Commit, "after_winner": sim.SimulateDispatchCheck}[point]
    same, out = sim.step(state, command(b))
    assert same == state and out.outcome is sim.Outcome.HELD
    assert len(state.winners) == (1 if point == "after_winner" else 0)
    assert state.sources[0].budget == (0 if point == "after_winner" else 1)


def test_expired_old_generation_can_close_but_late_commit_cannot_win(sim):
    state, b, _ = _pair(sim, True)
    b = replace(b, expires=1, eligibility=replace(b.eligibility, expires=1))
    state, out = sim.step(state, sim.Propose(b))
    assert out.outcome is sim.Outcome.PROPOSED
    state, out = sim.step(state, sim.PrepareCommit(b))
    assert out.outcome is sim.Outcome.PREPARED
    state, out = fact(sim, state, sim.Fact.HEALTH, sim.Value.YES, 2)
    assert out.outcome is sim.Outcome.RECORDED
    state, out = sim.step(state, sim.CloseGeneration(sim.Source.S0, 0))
    assert out.outcome is sim.Outcome.RECORDED
    same, out = sim.step(state, sim.Commit(b))
    assert same == state and out.outcome is sim.Outcome.HELD
    b1 = replace(b, request=sim.Request.R1, generation=1, created=2, expires=8,
                 eligibility=replace(b.eligibility, expires=8))
    state, out = sim.step(state, sim.Propose(b1))
    assert out.outcome is sim.Outcome.PROPOSED
    state, out = sim.step(state, sim.PrepareCommit(b1))
    assert out.outcome is sim.Outcome.PREPARED
    state, out = sim.step(state, sim.Commit(b1))
    assert out.outcome is sim.Outcome.WINNER
    assert state.winners[0].envelope == b1 and state.sources[0].budget == 0


@pytest.mark.parametrize("condition", ["unknown", "winner", "policy_unavailable"])
def test_expiry_does_not_relax_generation_close_guards(sim, condition):
    state, b, _ = _pair(sim, True)
    b = replace(b, expires=1, eligibility=replace(b.eligibility, expires=1))
    state, _ = sim.step(state, sim.Propose(b))
    state, _ = sim.step(state, sim.PrepareCommit(b))
    if condition == "winner":
        state, _ = sim.step(state, sim.Commit(b))
    state, _ = fact(sim, state, sim.Fact.HEALTH, sim.Value.YES, 2)
    if condition == "unknown":
        state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
    if condition == "policy_unavailable":
        state, _ = fact(sim, state, sim.Fact.POLICY, sim.Value.NO, 2)
    same, out = sim.step(state, sim.CloseGeneration(sim.Source.S0, 0))
    assert same == state
    assert out.outcome is (sim.Outcome.RECONCILIATION if condition == "unknown" else sim.Outcome.HELD)
    assert not state.closed_generations


@pytest.mark.parametrize("version", ["V1", "B"])
@pytest.mark.parametrize("action", ["FALLBACK", "ESCALATION"])
def test_non_same_action_cannot_select_source_model_when_history_omits_it(sim, version, action):
    state = ready(sim, ())
    envelope = sim.Envelope(sim.Source.S0, sim.Request.R0, sim.Version[version], sim.Entry.INITIAL,
                            sim.Action[action], sim.Model.M0, sim.Provider.P0, (), None,
                            0, 0, 8, sim.Fixture.NORMAL)
    same, out = sim.step(state, sim.Propose(envelope))
    assert same == state and out.outcome is sim.Outcome.HELD
    same, out = sim.step(state, sim.PrepareCommit(envelope))
    assert same == state and out.outcome is sim.Outcome.HELD
    same, out = sim.step(state, sim.Commit(envelope))
    assert same == state and out.outcome is sim.Outcome.HELD
    assert state.sources[0].budget == 1 and not state.winners


@pytest.mark.parametrize("command_name", ["PrepareCommit", "Commit", "SimulateDispatchCheck"])
def test_unknown_without_prior_proposal_requires_reconciliation(sim, command_name):
    state, b, _ = _pair(sim, True)
    state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
    same, out = sim.step(state, getattr(sim, command_name)(b))
    assert same == state and out.outcome is sim.Outcome.RECONCILIATION
    assert state.sources[0].budget == 1 and not state.winners


def test_unknown_without_winner_dispatch_requires_reconciliation(sim):
    state, b, _ = _pair(sim, True)
    state, _ = sim.step(state, sim.Propose(b))
    state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
    same, out = sim.step(state, sim.SimulateDispatchCheck(b))
    assert same == state and out.outcome is sim.Outcome.RECONCILIATION
    assert state.sources[0].budget == 1 and not state.winners


def test_unknown_keeps_historical_winner_replay_and_conflict_precedence(sim):
    state, b, _ = _pair(sim, True)
    state, _ = sim.step(state, sim.Propose(b))
    state, _ = sim.step(state, sim.PrepareCommit(b))
    state, _ = sim.step(state, sim.Commit(b))
    state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
    for command in (sim.PrepareCommit(b), sim.Commit(b)):
        same, out = sim.step(state, command)
        assert same == state and out.outcome is sim.Outcome.WINNER
    altered = replace(b, entry=sim.Entry.IMPORT)
    for command_type in (sim.PrepareCommit, sim.Commit, sim.SimulateDispatchCheck):
        same, out = sim.step(state, command_type(altered))
        assert same == state and out.outcome is sim.Outcome.CONFLICT
        same, out = sim.step(state, command_type(replace(b, source="S0")))
        assert same == state and out.outcome is sim.Outcome.HELD_INVALID
    same, out = sim.step(state, sim.SimulateDispatchCheck(b))
    assert same == state and out.outcome is sim.Outcome.RECONCILIATION
    assert state.sources[0].budget == 0 and len(state.winners) == 1


@pytest.mark.parametrize("command_name", ["PrepareCommit", "Commit", "SimulateDispatchCheck"])
def test_unregistered_source_is_invalid_without_resource_mutation(sim, command_name):
    state = sim.initial_state()
    b = proposal(sim, sim.Version.B).envelope
    same, out = sim.step(state, getattr(sim, command_name)(b))
    assert same is state and out.outcome is sim.Outcome.HELD_INVALID
    assert not state.sources and not state.winners and not state.events


@pytest.mark.parametrize("command_name", ["PrepareCommit", "Commit", "SimulateDispatchCheck"])
def test_registered_source_without_proposal_remains_held(sim, command_name):
    state, b, _ = _pair(sim, True)
    same, out = sim.step(state, getattr(sim, command_name)(b))
    assert same is state and out.outcome is sim.Outcome.HELD
    assert state.sources[0].budget == 1 and not state.winners


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("command_name", ["PrepareCommit", "Commit", "SimulateDispatchCheck"])
@pytest.mark.parametrize("mismatch", ["history", "fixture", "provider", "same_model", "elig_source",
                                      "elig_model", "elig_provider", "elig_evidence", "elig_policy", "elig_retry"])
def test_unproposed_request_must_match_static_source_bindings(sim, unknown, command_name, mismatch):
    state, b, _ = _pair(sim, True)
    if unknown:
        state, _ = sim.step(state, sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN))
    changes = {
        "history": {"history": ()},
        "fixture": {"fixture": sim.Fixture.BUDGET_EMPTY},
        "provider": {"provider": sim.Provider.P1},
        "same_model": {"model": sim.Model.M1, "provider": sim.Provider.P1},
        "elig_source": {"eligibility": replace(b.eligibility, source=sim.Source.S1)},
        "elig_model": {"eligibility": replace(b.eligibility, model=sim.Model.M1)},
        "elig_provider": {"eligibility": replace(b.eligibility, provider=sim.Provider.P1)},
        "elig_evidence": {"eligibility": replace(b.eligibility, evidence=sim.Evidence.E1)},
        "elig_policy": {"eligibility": replace(b.eligibility, policy="P0")},
        "elig_retry": {"eligibility": replace(b.eligibility, retry_level=1)},
    }
    invalid = replace(b, **changes[mismatch])
    same, out = sim.step(state, getattr(sim, command_name)(invalid))
    assert same is state and out.outcome is sim.Outcome.HELD_INVALID
    assert state.sources[0].budget == 1 and not state.winners
