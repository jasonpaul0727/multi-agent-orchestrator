"""A finite, synthetic thought experiment, not an admission or dispatch system.

Everything here is non-authoritative and used only by offline tests.  No value
is a production identifier, authorization, reservation or proof.
"""

from dataclasses import dataclass, replace
from enum import Enum, auto


class Source(Enum):
    S0 = auto()
    S1 = auto()


class Model(Enum):
    M0 = auto()
    M1 = auto()


class Provider(Enum):
    P0 = auto()
    P1 = auto()


class Evidence(Enum):
    E0 = auto()
    E1 = auto()


class Policy(Enum):
    P0 = auto()


class Fixture(Enum):
    NORMAL = auto()
    ATTEMPTS_FULL = auto()
    RETRY_FULL = auto()
    BUDGET_EMPTY = auto()


class Request(Enum):
    R0 = auto()
    R1 = auto()


class Version(Enum):
    V1 = auto()
    B = auto()


class Entry(Enum):
    INITIAL = auto()
    PROBE = auto()
    RECOVERY = auto()
    IMPORT = auto()


class Action(Enum):
    SAME = auto()
    FALLBACK = auto()
    ESCALATION = auto()


class Fact(Enum):
    FAILED = auto()
    LATEST = auto()
    INPUT = auto()
    CLOSED = auto()
    NO_EFFECT = auto()
    POLICY = auto()
    HEALTH = auto()
    RESOURCE = auto()


class Value(Enum):
    UNSET = auto()
    YES = auto()
    NO = auto()
    UNKNOWN = auto()


class Outcome(Enum):
    REGISTERED = auto()
    RECORDED = auto()
    PROPOSED = auto()
    HELD = auto()
    HELD_INVALID = auto()
    RECONCILIATION = auto()
    CONFLICT = auto()


@dataclass(frozen=True)
class Observation:
    outcome: Outcome
    simulation_only: bool = True
    non_authoritative: bool = True


@dataclass(frozen=True)
class Eligibility:
    source: Source
    model: Model
    provider: Provider
    evidence: Evidence
    policy: Policy
    retry_level: int
    expires: int


@dataclass(frozen=True)
class Envelope:
    source: Source
    request: Request
    version: Version
    entry: Entry
    action: Action
    model: Model
    provider: Provider
    history: tuple[Model, ...]
    eligibility: Eligibility | None
    generation: int
    created: int
    expires: int
    fixture: Fixture


@dataclass(frozen=True)
class RegisterSource:
    source: Source
    history: tuple[Model, ...]
    fixture: Fixture


@dataclass(frozen=True)
class SetFacts:
    source: Source
    changes: tuple[tuple[Fact, Value], ...]
    clock: int


@dataclass(frozen=True)
class Propose:
    envelope: Envelope


@dataclass(frozen=True)
class Event:
    ordinal: int
    command: RegisterSource | SetFacts | Propose


@dataclass(frozen=True)
class SourceState:
    source: Source
    history: tuple[Model, ...]
    fixture: Fixture
    facts: tuple[Value, ...]
    first_yes: tuple[int | None, ...]


@dataclass(frozen=True)
class ModelState:
    sources: tuple[SourceState, ...] = ()
    events: tuple[Event, ...] = ()
    proposals: tuple[Envelope, ...] = ()
    clock: int = 0
    simulation_only: bool = True
    non_authoritative: bool = True


def initial_state() -> ModelState:
    return ModelState()


def _out(state, outcome):
    return state, Observation(outcome)


def _index(fact):
    return list(Fact).index(fact)


def _identity(source):
    return {
        Source.S0: (Model.M0, Provider.P0, Evidence.E0),
        Source.S1: (Model.M1, Provider.P1, Evidence.E1),
    }[source]


def _history_ok(history):
    return (type(history) is tuple and len(history) <= 2
            and all(type(model) is Model for model in history)
            and len(set(history)) == len(history))


def _clock_ok(clock):
    return type(clock) is int and 0 <= clock <= 8


def _command_shape_ok(command):
    if type(command) is RegisterSource:
        return (type(command.source) is Source and type(command.fixture) is Fixture
                and _history_ok(command.history))
    if type(command) is SetFacts:
        changes = command.changes
        return (type(command.source) is Source and _clock_ok(command.clock)
                and type(changes) is tuple and 0 < len(changes) <= len(Fact)
                and all(type(pair) is tuple and len(pair) == 2
                        and type(pair[0]) is Fact and type(pair[1]) is Value
                        and pair[1] is not Value.UNSET for pair in changes)
                and len({pair[0] for pair in changes}) == len(changes))
    return type(command) is Propose and _envelope_ok(command.envelope)

def _state_ok(state):
    if (type(state) is not ModelState or state.simulation_only is not True
            or state.non_authoritative is not True
            or type(state.sources) is not tuple or len(state.sources) > 2):
        return False
    if type(state.events) is not tuple or len(state.events) > 64 or type(state.proposals) is not tuple or len(state.proposals) > 4 or not _clock_ok(state.clock):
        return False
    if not all(type(event) is Event and type(event.ordinal) is int and event.ordinal == i
               and _command_shape_ok(event.command)
               for i, event in enumerate(state.events, 1)):
        return False
    if not all(type(item) is SourceState and type(item.source) is Source
               and _history_ok(item.history) and type(item.fixture) is Fixture
               and type(item.facts) is tuple and len(item.facts) == len(Fact)
               and all(type(value) is Value for value in item.facts)
               and type(item.first_yes) is tuple and len(item.first_yes) == len(Fact)
               and all(ordinal is None or (type(ordinal) is int and 1 <= ordinal <= len(state.events))
                       for ordinal in item.first_yes) for item in state.sources):
        return False
    return (len({item.source for item in state.sources}) == len(state.sources)
            and all(_envelope_ok(proposal) for proposal in state.proposals))


def _find(state, source):
    return next((item for item in state.sources if item.source is source), None)


def _append(state, command, **updates):
    return replace(state, events=state.events + (Event(len(state.events) + 1, command),), **updates)


def _envelope_ok(envelope):
    if type(envelope) is not Envelope:
        return False
    for value, kind in ((envelope.source, Source), (envelope.request, Request),
                        (envelope.version, Version), (envelope.entry, Entry),
                        (envelope.action, Action), (envelope.model, Model),
                        (envelope.provider, Provider), (envelope.fixture, Fixture)):
        if type(value) is not kind:
            return False
    if not (_history_ok(envelope.history) and type(envelope.generation) is int
            and envelope.generation in (0, 1) and _clock_ok(envelope.created)
            and _clock_ok(envelope.expires) and envelope.created <= envelope.expires):
        return False
    eligibility = envelope.eligibility
    if eligibility is None:
        return True
    return (type(eligibility) is Eligibility and type(eligibility.source) is Source
            and type(eligibility.model) is Model and type(eligibility.provider) is Provider
            and type(eligibility.evidence) is Evidence and type(eligibility.policy) is Policy
            and type(eligibility.retry_level) is int and 0 <= eligibility.retry_level <= 2
            and _clock_ok(eligibility.expires))


def step(state: ModelState, command: object) -> tuple[ModelState, Observation]:
    """Perform one bounded synthetic transition, holding malformed inputs."""
    if not _state_ok(state):
        return _out(state, Outcome.HELD_INVALID)
    if type(command) is RegisterSource:
        if type(command.source) is not Source or type(command.fixture) is not Fixture or not _history_ok(command.history):
            return _out(state, Outcome.HELD_INVALID)
        found = _find(state, command.source)
        if found is not None:
            return _out(state, Outcome.REGISTERED if (found.history, found.fixture) == (command.history, command.fixture) else Outcome.HELD_INVALID)
        if len(state.events) == 64:
            return _out(state, Outcome.HELD_INVALID)
        source = SourceState(command.source, command.history, command.fixture,
                             (Value.UNSET,) * len(Fact), (None,) * len(Fact))
        return _out(_append(state, command, sources=state.sources + (source,)), Outcome.REGISTERED)
    if type(command) is SetFacts:
        if type(command.source) is not Source or not _clock_ok(command.clock) or command.clock < state.clock:
            return _out(state, Outcome.HELD_INVALID)
        found = _find(state, command.source)
        changes = command.changes
        if (found is None or type(changes) is not tuple or not changes or len(changes) > len(Fact)
                or any(type(pair) is not tuple or len(pair) != 2 or type(pair[0]) is not Fact
                       or type(pair[1]) is not Value or pair[1] is Value.UNSET for pair in changes)
                or len({pair[0] for pair in changes}) != len(changes) or len(state.events) == 64):
            return _out(state, Outcome.HELD_INVALID)
        changes_map = dict(changes)
        if changes_map.get(Fact.CLOSED) is Value.YES and changes_map.get(Fact.NO_EFFECT) is Value.YES:
            return _out(state, Outcome.HELD_INVALID)
        facts = list(found.facts)
        first = list(found.first_yes)
        dynamic = {Fact.POLICY, Fact.HEALTH, Fact.RESOURCE}
        for name, value in changes:
            i = _index(name)
            old = facts[i]
            if old is Value.UNKNOWN and value is not Value.UNKNOWN:
                return _out(state, Outcome.HELD)
            if name not in dynamic and old is Value.NO and value is not Value.NO:
                return _out(state, Outcome.HELD)
            if value is Value.YES and first[i] is None:
                first[i] = len(state.events) + 1
            facts[i] = value
        updated = replace(found, facts=tuple(facts), first_yes=tuple(first))
        sources = tuple(updated if item.source is command.source else item for item in state.sources)
        return _out(_append(state, command, sources=sources, clock=command.clock), Outcome.RECORDED)
    if type(command) is Propose:
        envelope = command.envelope
        if not _envelope_ok(envelope):
            return _out(state, Outcome.HELD_INVALID)
        existing = next((item for item in state.proposals if item.source is envelope.source and item.request is envelope.request), None)
        if existing is not None:
            return _out(state, Outcome.PROPOSED if existing == envelope else Outcome.CONFLICT)
        found = _find(state, envelope.source)
        if found is None or found.history != envelope.history or found.fixture is not envelope.fixture:
            return _out(state, Outcome.HELD_INVALID)
        source_model, source_provider, evidence = _identity(envelope.source)
        expected_provider = Provider.P0 if envelope.model is Model.M0 else Provider.P1
        if envelope.provider is not expected_provider or (envelope.action is Action.SAME and envelope.model is not source_model):
            return _out(state, Outcome.HELD_INVALID)
        if envelope.version is Version.B and envelope.action is Action.SAME:
            eligibility = envelope.eligibility
            if (eligibility is None or eligibility.source is not envelope.source
                    or eligibility.model is not source_model or eligibility.provider is not source_provider
                    or eligibility.evidence is not evidence or eligibility.policy is not Policy.P0
                    or eligibility.retry_level != 2 or eligibility.expires < envelope.created):
                return _out(state, Outcome.HELD_INVALID)
        elif envelope.eligibility is not None:
            return _out(state, Outcome.HELD_INVALID)
        if found.facts[_index(Fact.FAILED)] is Value.UNKNOWN:
            return _out(state, Outcome.RECONCILIATION)
        if any(value is not Value.YES for value in found.facts):
            return _out(state, Outcome.HELD)
        if (envelope.created > state.clock or state.clock > envelope.expires
                or (envelope.eligibility is not None and state.clock > envelope.eligibility.expires)):
            return _out(state, Outcome.HELD)
        closed, effect = found.first_yes[_index(Fact.CLOSED)], found.first_yes[_index(Fact.NO_EFFECT)]
        if closed is None or effect is None or closed >= effect:
            return _out(state, Outcome.HELD)
        if envelope.model in found.history and not (envelope.version is Version.B and envelope.action is Action.SAME):
            return _out(state, Outcome.HELD)
        if len(state.events) == 64 or len(state.proposals) == 4:
            return _out(state, Outcome.HELD_INVALID)
        return _out(_append(state, command, proposals=state.proposals + (envelope,)), Outcome.PROPOSED)
    return _out(state, Outcome.HELD_INVALID)
