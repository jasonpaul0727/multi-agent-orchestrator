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
    PREPARED = auto()
    WINNER = auto()
    SUPERSEDED = auto()
    PRECONDITIONS_MET_NON_AUTHORITY = auto()
    CRASHED = auto()
    REPLAYED = auto()


class Invalidation(Enum):
    UNKNOWN = auto()
    REVOKED = auto()
    STALE = auto()
    INPUT = auto()
    EFFECT = auto()
    OLD_SEND = auto()
    RESERVATION = auto()


@dataclass(frozen=True, slots=True)
class Observation:
    outcome: Outcome
    simulation_only: bool = True
    non_authoritative: bool = True


@dataclass(frozen=True, slots=True)
class Eligibility:
    source: Source
    model: Model
    provider: Provider
    evidence: Evidence
    policy: Policy
    retry_level: int
    expires: int


@dataclass(frozen=True, slots=True)
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


@dataclass(frozen=True, slots=True)
class RegisterSource:
    source: Source
    history: tuple[Model, ...]
    fixture: Fixture


@dataclass(frozen=True, slots=True)
class SetFacts:
    source: Source
    changes: tuple[tuple[Fact, Value], ...]
    clock: int


@dataclass(frozen=True, slots=True)
class Propose:
    envelope: Envelope


@dataclass(frozen=True, slots=True)
class PrepareCommit:
    envelope: Envelope


@dataclass(frozen=True, slots=True)
class Commit:
    envelope: Envelope


@dataclass(frozen=True, slots=True)
class CloseGeneration:
    source: Source
    generation: int


@dataclass(frozen=True, slots=True)
class Invalidate:
    source: Source
    reason: Invalidation


@dataclass(frozen=True, slots=True)
class SimulateDispatchCheck:
    envelope: Envelope


@dataclass(frozen=True, slots=True)
class Crash:
    pass


@dataclass(frozen=True, slots=True)
class Replay:
    events: tuple["Event", ...]
    anchor: tuple["Event", ...]
    anchor_count: int


@dataclass(frozen=True, slots=True)
class Reservation:
    envelope: Envelope
    revoked: bool = False


@dataclass(frozen=True, slots=True)
class Winner:
    envelope: Envelope
    reservation: Reservation


@dataclass(frozen=True, slots=True)
class Event:
    ordinal: int
    command: RegisterSource | SetFacts | Propose | Commit | CloseGeneration | Invalidate


@dataclass(frozen=True, slots=True)
class SourceState:
    source: Source
    history: tuple[Model, ...]
    fixture: Fixture
    facts: tuple[Value, ...]
    first_yes: tuple[int | None, ...]
    budget: int
    attempts: int
    max_attempts: int
    retry_level: int
    max_retries: int
    invalidations: tuple[Invalidation, ...] = ()


@dataclass(frozen=True, slots=True)
class ModelState:
    sources: tuple[SourceState, ...] = ()
    events: tuple[Event, ...] = ()
    proposals: tuple[Envelope, ...] = ()
    pending: tuple[Envelope, ...] = ()
    winners: tuple[Winner, ...] = ()
    closed_generations: tuple[tuple[Source, int], ...] = ()
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
    if type(command) in (Propose, PrepareCommit, Commit, SimulateDispatchCheck):
        return _envelope_ok(command.envelope)
    if type(command) is CloseGeneration:
        return type(command.source) is Source and type(command.generation) is int and command.generation == 0
    return (type(command) is Invalidate and type(command.source) is Source
            and type(command.reason) is Invalidation)

def _event_ok(event, ordinal):
    # Pending, crash, replay and simulated dispatch are never durable events.
    return (type(event) is Event and type(event.ordinal) is int and event.ordinal == ordinal
            and type(event.command) in (RegisterSource, SetFacts, Propose, Commit, CloseGeneration, Invalidate)
            and _command_shape_ok(event.command))


def _state_ok(state):
    if (type(state) is not ModelState or state.simulation_only is not True
            or state.non_authoritative is not True
            or type(state.sources) is not tuple or len(state.sources) > 2):
        return False
    if (type(state.events) is not tuple or len(state.events) > 64
            or type(state.proposals) is not tuple or len(state.proposals) > 4
            or type(state.pending) is not tuple or len(state.pending) > 4
            or type(state.winners) is not tuple or len(state.winners) > 2
            or type(state.closed_generations) is not tuple or len(state.closed_generations) > 2
            or not _clock_ok(state.clock)):
        return False
    if not all(_event_ok(event, i) for i, event in enumerate(state.events, 1)):
        return False
    if not all(type(item) is SourceState and type(item.source) is Source
               and _history_ok(item.history) and type(item.fixture) is Fixture
               and type(item.facts) is tuple and len(item.facts) == len(Fact)
               and all(type(value) is Value for value in item.facts)
               and type(item.first_yes) is tuple and len(item.first_yes) == len(Fact)
               and all(ordinal is None or (type(ordinal) is int and 1 <= ordinal <= len(state.events))
                       for ordinal in item.first_yes)
               and type(item.budget) is int and 0 <= item.budget <= 1
               and type(item.attempts) is int and 2 <= item.attempts <= 3
               and type(item.max_attempts) is int and item.max_attempts in (2, 3)
               and type(item.retry_level) is int and item.retry_level in (1, 2)
               and type(item.max_retries) is int and item.max_retries == 2
               and type(item.invalidations) is tuple
               and all(type(reason) is Invalidation for reason in item.invalidations)
               and len(set(item.invalidations)) == len(item.invalidations)
               for item in state.sources):
        return False
    return (len({item.source for item in state.sources}) == len(state.sources)
            and all(_envelope_ok(proposal) for proposal in state.proposals)
            and all(_envelope_ok(item) for item in state.pending)
            and len({(item.source, item.request) for item in state.pending}) == len(state.pending)
            and all(type(item) is Winner and _envelope_ok(item.envelope)
                    and type(item.reservation) is Reservation
                    and item.reservation.envelope == item.envelope
                    and type(item.reservation.revoked) is bool for item in state.winners)
            and len({item.envelope.source for item in state.winners}) == len(state.winners)
            and all(type(item) is tuple and len(item) == 2 and type(item[0]) is Source
                    and type(item[1]) is int and item[1] == 0 for item in state.closed_generations)
            and len(set(state.closed_generations)) == len(state.closed_generations))


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


def _binding_ok(envelope, source):
    if source.history != envelope.history or source.fixture is not envelope.fixture:
        return False
    source_model, source_provider, evidence = _identity(envelope.source)
    expected_provider = Provider.P0 if envelope.model is Model.M0 else Provider.P1
    if envelope.provider is not expected_provider or (envelope.action is Action.SAME and envelope.model is not source_model):
        return False
    if envelope.version is Version.B and envelope.action is Action.SAME:
        eligibility = envelope.eligibility
        return (eligibility is not None and eligibility.source is envelope.source
                and eligibility.model is source_model and eligibility.provider is source_provider
                and eligibility.evidence is evidence and eligibility.policy is Policy.P0
                and eligibility.retry_level == 2 and eligibility.expires >= envelope.created)
    return envelope.eligibility is None


def _live(state, envelope, source, *, check_resources, check_expiry=True):
    if Invalidation.UNKNOWN in source.invalidations or source.facts[_index(Fact.FAILED)] is Value.UNKNOWN:
        return Outcome.RECONCILIATION
    if source.invalidations or any(value is not Value.YES for value in source.facts):
        return Outcome.HELD
    if (envelope.created > state.clock or (check_expiry and (state.clock > envelope.expires
            or (envelope.eligibility is not None and state.clock > envelope.eligibility.expires)))):
        return Outcome.HELD
    closed, effect = source.first_yes[_index(Fact.CLOSED)], source.first_yes[_index(Fact.NO_EFFECT)]
    if closed is None or effect is None or closed >= effect:
        return Outcome.HELD
    if ((envelope.source, 0) in state.closed_generations) != (envelope.generation == 1):
        return Outcome.HELD
    if check_resources and (source.budget < 1 or source.attempts >= source.max_attempts
                            or source.retry_level >= source.max_retries):
        return Outcome.HELD
    return None


def _proposal(state, envelope):
    return next((item for item in state.proposals
                 if item.source is envelope.source and item.request is envelope.request), None)


def _winner(state, source):
    return next((item for item in state.winners if item.envelope.source is source), None)


def _replay(state, command):
    events, anchor, count = command.events, command.anchor, command.anchor_count
    if (type(events) is not tuple or type(anchor) is not tuple
            or type(count) is not int or not 0 <= count <= 64
            or len(events) != count or len(anchor) != count
            or not all(_event_ok(event, i) for i, event in enumerate(events, 1))
            or not all(_event_ok(event, i) for i, event in enumerate(anchor, 1))
            or events != anchor):
        return _out(state, Outcome.HELD_INVALID)
    rebuilt = initial_state()
    for event in events:
        if type(event.command) is Commit:
            # Preparation is volatile. Reconstruct its effect locally solely
            # to reuse the live commit checks; the event does not prove that
            # preparation occurred. Only durable preconditions can validate it.
            candidate = replace(rebuilt, pending=(event.command.envelope,))
        else:
            candidate = rebuilt
        after, observation = step(candidate, event.command)
        if (observation.outcome not in (Outcome.REGISTERED, Outcome.RECORDED,
                                       Outcome.PROPOSED, Outcome.WINNER)
                or len(after.events) != event.ordinal or after.events[-1] != event):
            return _out(state, Outcome.HELD_INVALID)
        rebuilt = replace(after, pending=())
    return _out(rebuilt, Outcome.REPLAYED)


def _step(state: ModelState, command: object) -> tuple[ModelState, Observation]:
    """Perform one bounded synthetic transition, holding malformed inputs."""
    if not _state_ok(state):
        return _out(state, Outcome.HELD_INVALID)
    if type(command) is Crash:
        return _out(replace(state, pending=()), Outcome.CRASHED)
    if type(command) is Replay:
        return _replay(state, command)
    if type(command) is RegisterSource:
        if type(command.source) is not Source or type(command.fixture) is not Fixture or not _history_ok(command.history):
            return _out(state, Outcome.HELD_INVALID)
        found = _find(state, command.source)
        if found is not None:
            return _out(state, Outcome.REGISTERED if (found.history, found.fixture) == (command.history, command.fixture) else Outcome.HELD_INVALID)
        if len(state.events) == 64:
            return _out(state, Outcome.HELD_INVALID)
        budget = 0 if command.fixture is Fixture.BUDGET_EMPTY else 1
        max_attempts = 2 if command.fixture is Fixture.ATTEMPTS_FULL else 3
        retry_level = 2 if command.fixture is Fixture.RETRY_FULL else 1
        source = SourceState(command.source, command.history, command.fixture,
                             (Value.UNSET,) * len(Fact), (None,) * len(Fact),
                             budget, 2, max_attempts, retry_level, 2)
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
        if found is None or not _binding_ok(envelope, found):
            return _out(state, Outcome.HELD_INVALID)
        source_model, _, _ = _identity(envelope.source)
        blocked = _live(state, envelope, found, check_resources=True)
        if blocked is not None:
            return _out(state, blocked)
        # A fallback or escalation cannot target its own source model. This
        # synthetic model does not simulate tier or capability qualification.
        if envelope.action is not Action.SAME and envelope.model is source_model:
            return _out(state, Outcome.HELD)
        if envelope.model in found.history and not (envelope.version is Version.B and envelope.action is Action.SAME):
            return _out(state, Outcome.HELD)
        if len(state.events) == 64 or len(state.proposals) == 4:
            return _out(state, Outcome.HELD_INVALID)
        return _out(_append(state, command, proposals=state.proposals + (envelope,)), Outcome.PROPOSED)
    if type(command) in (PrepareCommit, Commit, SimulateDispatchCheck):
        if not _command_shape_ok(command):
            return _out(state, Outcome.HELD_INVALID)
        envelope = command.envelope
        proposed = _proposal(state, envelope)
        if proposed is not None and proposed != envelope:
            return _out(state, Outcome.CONFLICT)
        winner = _winner(state, envelope.source)
        source = _find(state, envelope.source)
        if source is None:
            return _out(state, Outcome.HELD_INVALID)
        if proposed is None and not _binding_ok(envelope, source):
            return _out(state, Outcome.HELD_INVALID)
        if (type(command) is not SimulateDispatchCheck and winner is not None
                and winner.envelope == envelope and proposed is not None):
            return _out(state, Outcome.WINNER)
        if (Invalidation.UNKNOWN in source.invalidations
                or source.facts[_index(Fact.FAILED)] is Value.UNKNOWN):
            return _out(state, Outcome.RECONCILIATION)
        if proposed is None:
            return _out(state, Outcome.HELD)
        if type(command) is SimulateDispatchCheck:
            if winner is None or winner.envelope != envelope:
                return _out(state, Outcome.HELD)
            blocked = _live(state, envelope, source, check_resources=False)
            if blocked is not None:
                return _out(state, blocked)
            if winner.reservation.envelope != envelope or winner.reservation.revoked:
                return _out(state, Outcome.HELD)
            return _out(state, Outcome.PRECONDITIONS_MET_NON_AUTHORITY)
        if winner is not None:
            return _out(state, Outcome.WINNER if winner.envelope == envelope else Outcome.SUPERSEDED)
        blocked = _live(state, envelope, source, check_resources=True)
        if blocked is not None:
            return _out(state, blocked)
        if type(command) is PrepareCommit:
            if envelope in state.pending:
                return _out(state, Outcome.PREPARED)
            if sum(item.source is envelope.source for item in state.pending) >= 2 or len(state.pending) >= 4:
                return _out(state, Outcome.HELD_INVALID)
            return _out(replace(state, pending=state.pending + (envelope,)), Outcome.PREPARED)
        if envelope not in state.pending:
            return _out(state, Outcome.HELD)
        if len(state.events) == 64:
            return _out(state, Outcome.HELD_INVALID)
        updated = replace(source, budget=source.budget - 1, attempts=source.attempts + 1,
                          retry_level=source.retry_level + 1)
        sources = tuple(updated if item.source is envelope.source else item for item in state.sources)
        winner = Winner(envelope, Reservation(envelope))
        pending = tuple(item for item in state.pending if item != envelope)
        return _out(_append(state, command, sources=sources, pending=pending,
                            winners=state.winners + (winner,)), Outcome.WINNER)
    if type(command) is CloseGeneration:
        if not _command_shape_ok(command):
            return _out(state, Outcome.HELD_INVALID)
        source = _find(state, command.source)
        if source is None:
            return _out(state, Outcome.HELD_INVALID)
        marker = (command.source, command.generation)
        if marker in state.closed_generations:
            return _out(state, Outcome.RECORDED)
        if _winner(state, command.source) is not None:
            return _out(state, Outcome.HELD)
        proposals = [item for item in state.proposals if item.source is command.source and item.generation == 0]
        if not proposals:
            return _out(state, Outcome.HELD)
        blocked = _live(state, proposals[0], source, check_resources=True, check_expiry=False)
        if blocked is not None:
            return _out(state, blocked)
        if len(state.events) == 64:
            return _out(state, Outcome.HELD_INVALID)
        return _out(_append(state, command, closed_generations=state.closed_generations + (marker,)), Outcome.RECORDED)
    if type(command) is Invalidate:
        if not _command_shape_ok(command):
            return _out(state, Outcome.HELD_INVALID)
        source = _find(state, command.source)
        if source is None:
            return _out(state, Outcome.HELD_INVALID)
        if command.reason in source.invalidations:
            return _out(state, Outcome.RECORDED)
        if len(state.events) == 64:
            return _out(state, Outcome.HELD_INVALID)
        updated = replace(source, invalidations=source.invalidations + (command.reason,))
        sources = tuple(updated if item.source is command.source else item for item in state.sources)
        winners = tuple(replace(item, reservation=replace(item.reservation, revoked=True))
                        if item.envelope.source is command.source and command.reason is Invalidation.RESERVATION
                        else item for item in state.winners)
        return _out(_append(state, command, sources=sources, winners=winners), Outcome.RECORDED)
    return _out(state, Outcome.HELD_INVALID)



def _copy_envelope(envelope):
    eligibility = envelope.eligibility
    if eligibility is not None:
        eligibility = Eligibility(eligibility.source, eligibility.model, eligibility.provider,
                                  eligibility.evidence, eligibility.policy,
                                  eligibility.retry_level, eligibility.expires)
    return Envelope(envelope.source, envelope.request, envelope.version, envelope.entry,
                    envelope.action, envelope.model, envelope.provider, tuple(envelope.history),
                    eligibility, envelope.generation, envelope.created, envelope.expires,
                    envelope.fixture)


def _copy_event_command(command):
    if type(command) is RegisterSource:
        return RegisterSource(command.source, tuple(command.history), command.fixture)
    if type(command) is SetFacts:
        return SetFacts(command.source, tuple((fact, value) for fact, value in command.changes), command.clock)
    if type(command) is Propose:
        return Propose(_copy_envelope(command.envelope))
    if type(command) is Commit:
        return Commit(_copy_envelope(command.envelope))
    if type(command) is CloseGeneration:
        return CloseGeneration(command.source, command.generation)
    if type(command) is Invalidate:
        return Invalidate(command.source, command.reason)
    raise ValueError("invalid synthetic event command")


def _copy_state(state):
    """Detach trusted model values, not a defense against reflective mutation of outputs."""
    sources = tuple(SourceState(item.source, tuple(item.history), item.fixture, tuple(item.facts),
                                tuple(item.first_yes), item.budget, item.attempts,
                                item.max_attempts, item.retry_level, item.max_retries,
                                tuple(item.invalidations)) for item in state.sources)
    events = tuple(Event(item.ordinal, _copy_event_command(item.command)) for item in state.events)
    proposals = tuple(_copy_envelope(item) for item in state.proposals)
    pending = tuple(_copy_envelope(item) for item in state.pending)
    winners = tuple(Winner(_copy_envelope(item.envelope),
                           Reservation(_copy_envelope(item.reservation.envelope),
                                       item.reservation.revoked)) for item in state.winners)
    return ModelState(sources=sources, events=events, proposals=proposals, pending=pending,
                      winners=winners, closed_generations=tuple((source, generation)
                                                               for source, generation in state.closed_generations),
                      clock=state.clock)


def step(state: ModelState, command: object) -> tuple[ModelState, Observation]:
    """Return detached synthetic outputs; invalid inputs remain unchanged."""
    result, observation = _step(state, command)
    if result is state:
        return result, observation
    return _copy_state(result), observation
