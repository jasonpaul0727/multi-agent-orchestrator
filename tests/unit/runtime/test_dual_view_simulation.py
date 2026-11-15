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
