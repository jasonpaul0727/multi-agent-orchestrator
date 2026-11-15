"""Exact finite-model JSON for offline tests; roundtrips confer no authority."""

import json

from tests.support import dual_view_simulation as sim

MAX_TEXT_BYTES = 2 * 1024 * 1024
MAX_DEPTH = 32

_ENUMS = {kind.__name__: kind for kind in (
    sim.Source, sim.Model, sim.Provider, sim.Evidence, sim.Policy, sim.Fixture,
    sim.Request, sim.Version, sim.Entry, sim.Action, sim.Fact, sim.Value,
    sim.Outcome, sim.Invalidation,
)}


def _exact(kind):
    return lambda value: type(value) is kind


def _integer(low, high):
    return lambda value: type(value) is int and low <= value <= high


def _fixed(value):
    return lambda candidate: type(candidate) is type(value) and candidate == value


def _optional(check):
    return lambda value: value is None or check(value)


def _tuple(check, minimum=0, maximum=64, *, unique=False):
    def valid(value):
        return (type(value) is tuple and minimum <= len(value) <= maximum
                and all(check(item) for item in value)
                and (not unique or len(set(value)) == len(value)))
    return valid


def _pair(left, right):
    return lambda value: (type(value) is tuple and len(value) == 2
                          and left(value[0]) and right(value[1]))


_HISTORY = _tuple(_exact(sim.Model), maximum=2, unique=True)
_CLOCK = _integer(0, 8)
_SOURCE = _exact(sim.Source)
_ENVELOPE = _exact(sim.Envelope)
_EVENT_COMMAND = lambda value: type(value) in (
    sim.RegisterSource, sim.SetFacts, sim.Propose, sim.Commit,
    sim.CloseGeneration, sim.Invalidate,
)
_SCHEMAS = {
    sim.Observation: dict(outcome=_exact(sim.Outcome), simulation_only=_fixed(True),
                          non_authoritative=_fixed(True)),
    sim.Eligibility: dict(source=_SOURCE, model=_exact(sim.Model),
                          provider=_exact(sim.Provider), evidence=_exact(sim.Evidence),
                          policy=_exact(sim.Policy), retry_level=_integer(0, 2),
                          expires=_CLOCK, source_fencing_generation=_fixed(2)),
    sim.Envelope: dict(source=_SOURCE, request=_exact(sim.Request),
                       version=_exact(sim.Version), entry=_exact(sim.Entry),
                       action=_exact(sim.Action), model=_exact(sim.Model),
                       provider=_exact(sim.Provider), history=_HISTORY,
                       eligibility=_optional(_exact(sim.Eligibility)),
                       generation=_integer(0, 1), created=_CLOCK, expires=_CLOCK,
                       fixture=_exact(sim.Fixture), source_fencing_generation=_fixed(2)),
    sim.RegisterSource: dict(source=_SOURCE, history=_HISTORY,
                             fixture=_exact(sim.Fixture), source_fencing_generation=_fixed(2)),
    sim.SetFacts: dict(source=_SOURCE, changes=_tuple(
        _pair(_exact(sim.Fact), lambda value: type(value) is sim.Value and value is not sim.Value.UNSET),
        minimum=1, maximum=8), clock=_CLOCK),
    sim.Propose: dict(envelope=_ENVELOPE),
    sim.PrepareCommit: dict(envelope=_ENVELOPE),
    sim.Commit: dict(envelope=_ENVELOPE, successor_fencing_generation=_fixed(3)),
    sim.CloseGeneration: dict(source=_SOURCE, generation=_fixed(0)),
    sim.Invalidate: dict(source=_SOURCE, reason=_exact(sim.Invalidation)),
    sim.SimulateDispatchCheck: dict(envelope=_ENVELOPE),
    sim.Crash: {},
    sim.Replay: dict(events=_tuple(_exact(sim.Event)), anchor=_tuple(_exact(sim.Event)),
                     anchor_count=_integer(0, 64)),
    sim.Reservation: dict(envelope=_ENVELOPE, revoked=_exact(bool),
                          successor_fencing_generation=_fixed(3)),
    sim.Winner: dict(envelope=_ENVELOPE, reservation=_exact(sim.Reservation),
                     successor_fencing_generation=_fixed(3)),
    sim.Event: dict(ordinal=_integer(1, 64), command=_EVENT_COMMAND,
                    source_fencing_generation=_fixed(2)),
    sim.SourceState: dict(source=_SOURCE, history=_HISTORY, fixture=_exact(sim.Fixture),
                          facts=_tuple(_exact(sim.Value), minimum=8, maximum=8),
                          first_yes=_tuple(_optional(_integer(1, 64)), minimum=8, maximum=8),
                          budget=_integer(0, 1), attempts=_integer(2, 3),
                          max_attempts=_integer(2, 3), retry_level=_integer(1, 2),
                          max_retries=_fixed(2),
                          invalidations=_tuple(_exact(sim.Invalidation), maximum=7, unique=True),
                          source_fencing_generation=_fixed(2)),
    sim.ModelState: dict(sources=_tuple(_exact(sim.SourceState), maximum=2),
                         events=_tuple(_exact(sim.Event)),
                         proposals=_tuple(_ENVELOPE, maximum=4),
                         pending=_tuple(_ENVELOPE, maximum=4),
                         winners=_tuple(_exact(sim.Winner), maximum=2),
                         closed_generations=_tuple(_pair(_SOURCE, _fixed(0)), maximum=2, unique=True),
                         clock=_CLOCK, simulation_only=_fixed(True),
                         non_authoritative=_fixed(True)),
}
_TYPES = {kind.__name__: kind for kind in _SCHEMAS}


def _validate(value):
    schema = _SCHEMAS[type(value)]
    if not all(check(getattr(value, name)) for name, check in schema.items()):
        raise ValueError("invalid finite-model field")
    if type(value) is sim.Envelope and value.created > value.expires:
        raise ValueError("invalid finite-model expiry")
    if type(value) is sim.SetFacts and len({pair[0] for pair in value.changes}) != len(value.changes):
        raise ValueError("duplicate finite-model fact")


def _root(value, depth=0):
    if depth > MAX_DEPTH:
        raise ValueError("finite-model value is too deep")
    if type(value) in _SCHEMAS or type(value) in _ENUMS.values():
        return
    if type(value) is tuple and len(value) <= 64:
        for item in value:
            _root(item, depth + 1)
        return
    raise ValueError("unregistered finite-model value")


def _pack(value, depth=0):
    if depth > MAX_DEPTH:
        raise ValueError("finite-model value is too deep")
    kind = type(value)
    if kind in _ENUMS.values():
        return {"enum": kind.__name__, "name": value.name}
    if kind in _SCHEMAS:
        _validate(value)
        return {"type": kind.__name__, "fields": {
            name: _pack(getattr(value, name), depth + 1) for name in _SCHEMAS[kind]}}
    if kind is tuple and len(value) <= 64:
        return {"tuple": [_pack(item, depth + 1) for item in value]}
    if value is None or kind in (bool, int):
        return value
    raise ValueError("unregistered finite-model value")


def _canonical(raw):
    return json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def encode(value) -> str:
    """Encode exact registered immutable values, defensively traversing all fields."""
    try:
        _root(value)
        text = _canonical({"version": 1, "value": _pack(value)})
        if len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            raise ValueError("finite-model text is too large")
        return text
    except (TypeError, AttributeError, RecursionError, UnicodeError) as error:
        raise ValueError("invalid finite-model value") from error


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_number(value):
    raise ValueError("only finite-model integers are supported")


def _unpack(raw, depth=0):
    if depth > MAX_DEPTH:
        raise ValueError("finite-model value is too deep")
    if raw is None or type(raw) in (int, bool):
        return raw
    if type(raw) is not dict:
        raise ValueError("invalid finite-model node")
    if set(raw) == {"enum", "name"}:
        if type(raw["enum"]) is not str or type(raw["name"]) is not str:
            raise ValueError("invalid finite-model enum")
        kind = _ENUMS.get(raw["enum"])
        if kind is None or raw["name"] not in kind.__members__:
            raise ValueError("unknown finite-model enum")
        return kind[raw["name"]]
    if set(raw) == {"tuple"}:
        items = raw["tuple"]
        if type(items) is not list or len(items) > 64:
            raise ValueError("invalid finite-model tuple")
        return tuple(_unpack(item, depth + 1) for item in items)
    if set(raw) == {"type", "fields"}:
        if type(raw["type"]) is not str:
            raise ValueError("unknown finite-model type")
        kind = _TYPES.get(raw["type"])
        fields = raw["fields"]
        if kind is None or type(fields) is not dict or set(fields) != set(_SCHEMAS[kind]):
            raise ValueError("unknown finite-model type or fields")
        value = kind(**{name: _unpack(item, depth + 1) for name, item in fields.items()})
        _validate(value)
        return value
    raise ValueError("unknown finite-model node")


def decode(text: str):
    """Refuse unknown or noncanonical JSON; construct only fixed local classes."""
    try:
        if type(text) is not str or len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            raise ValueError("invalid finite-model text size or type")
        raw = json.loads(text, object_pairs_hook=_object,
                         parse_float=_reject_number, parse_constant=_reject_number)
        if (type(raw) is not dict or set(raw) != {"version", "value"}
                or type(raw["version"]) is not int or raw["version"] != 1):
            raise ValueError("unknown finite-model format")
        value = _unpack(raw["value"])
        _root(value)
        if encode(value) != text:
            raise ValueError("noncanonical finite-model text")
        return value
    except (TypeError, KeyError, AttributeError, RecursionError, UnicodeError) as error:
        raise ValueError("invalid finite-model text") from error
