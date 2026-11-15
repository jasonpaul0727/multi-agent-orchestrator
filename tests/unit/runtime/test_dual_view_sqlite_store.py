"""Private finite SQLite prototype: synthetic values only, never authorization."""

import importlib
import importlib.util
import json
from dataclasses import replace
from enum import Enum

import pytest

from tests.support import dual_view_simulation as sim


def codec():
    name = "tests.support.dual_view_sqlite_codec"
    assert importlib.util.find_spec(name) is not None, "finite codec is not implemented"
    return importlib.import_module(name)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def envelope():
    eligibility = sim.Eligibility(sim.Source.S0, sim.Model.M0, sim.Provider.P0,
                                  sim.Evidence.E0, sim.Policy.P0, 2, 8)
    return sim.Envelope(sim.Source.S0, sim.Request.R0, sim.Version.B,
                        sim.Entry.RECOVERY, sim.Action.SAME, sim.Model.M0,
                        sim.Provider.P0, (sim.Model.M0,), eligibility, 0, 0, 8,
                        sim.Fixture.NORMAL)


def sample_values():
    item = envelope()
    register = sim.RegisterSource(sim.Source.S0, (sim.Model.M0,), sim.Fixture.NORMAL)
    state, _ = sim.step(sim.initial_state(), register)
    reservation = sim.Reservation(item)
    return (
        sim.Source.S0, sim.Model.M0, sim.Provider.P0, sim.Evidence.E0,
        sim.Policy.P0, sim.Fixture.NORMAL, sim.Request.R0, sim.Version.B,
        sim.Entry.RECOVERY, sim.Action.SAME, sim.Fact.CLOSED, sim.Value.YES,
        sim.Outcome.WINNER, sim.Invalidation.UNKNOWN,
        sim.Observation(sim.Outcome.WINNER), item.eligibility, item, register,
        sim.SetFacts(sim.Source.S0, ((sim.Fact.CLOSED, sim.Value.YES),), 0),
        sim.Propose(item), sim.PrepareCommit(item), sim.Commit(item),
        sim.CloseGeneration(sim.Source.S0, 0),
        sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN),
        sim.SimulateDispatchCheck(item), sim.Crash(),
        sim.Replay(state.events, state.events, 1), reservation,
        sim.Winner(item, reservation), state.events[0], state.sources[0], state,
        (sim.Source.S0, sim.Source.S1),
    )


def test_codec_roundtrips_only_registered_finite_values():
    module = codec()
    for value in sample_values():
        decoded = module.decode(module.encode(value))
        assert type(decoded) is type(value)
        assert decoded == value


def test_codec_register_source_has_a_canonical_versioned_representation():
    module = codec()
    command = sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL)
    assert module.encode(command) == (
        '{"value":{"fields":{"fixture":{"enum":"Fixture","name":"NORMAL"},'
        '"history":{"tuple":[]},"source":{"enum":"Source","name":"S0"},'
        '"source_fencing_generation":2},"type":"RegisterSource"},"version":1}'
    )


def test_codec_detaches_nested_caller_owned_dataclasses_on_both_directions():
    module = codec()
    caller = envelope()
    text = module.encode(caller)
    decoded = module.decode(text)
    assert decoded is not caller
    assert decoded.eligibility is not caller.eligibility
    object.__setattr__(caller.eligibility, "expires", 0)
    assert decoded.eligibility.expires == 8
    object.__setattr__(decoded.eligibility, "expires", 1)
    assert module.decode(text).eligibility.expires == 8


@pytest.mark.parametrize("value", [
    [], {}, "production-id", 1, True, None, 1.5, float("inf"),
    sim.RegisterSource(sim.Source.S0, [sim.Model.M0], sim.Fixture.NORMAL),
    sim.RegisterSource(sim.Source.S0, (sim.Model.M0, sim.Model.M0), sim.Fixture.NORMAL),
    sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL, True),
    sim.SetFacts(sim.Source.S0, ((sim.Fact.CLOSED, sim.Value.YES),), True),
    sim.SetFacts(sim.Source.S0, ((sim.Fact.CLOSED, sim.Value.UNSET),), 0),
    sim.CloseGeneration(sim.Source.S0, 2),
    replace(envelope(), generation=2),
    replace(envelope(), created=-1),
    replace(envelope(), expires=9),
    replace(envelope(), history=(sim.Model.M0,) * 3),
    replace(envelope(), source_fencing_generation=3),
    sim.Commit(envelope(), True),
    sim.ModelState(simulation_only=False),
    sim.ModelState(non_authoritative=False),
])
def test_codec_rejects_mutable_arbitrary_or_out_of_domain_values(value):
    with pytest.raises(ValueError):
        codec().encode(value)


def test_codec_rejects_subclasses_and_unregistered_types():
    class OtherSource(Enum):
        S0 = 1

    class Child(sim.RegisterSource):
        pass

    for value in (OtherSource.S0, Child(sim.Source.S0, (), sim.Fixture.NORMAL)):
        with pytest.raises(ValueError):
            codec().encode(value)


@pytest.mark.parametrize("text", [
    '{"version":1,"version":1,"value":{"enum":"Source","name":"S0"}}',
    '{"value":{"enum":"Source","name":"S0","name":"S1"},"version":1}',
    '{"value":{"enum":"Source","name":"S0"},"version":true}',
    '{"value":{"enum":"Source","name":"S0"},"version":2}',
    '{"value":{"enum":"Source","name":"S0"},"version":1,"extra":0}',
    '{"value":{"enum":"Unknown","name":"S0"},"version":1}',
    '{"value":{"enum":"Source","name":"S2"},"version":1}',
    '{"value":{"type":"os.system","fields":{}},"version":1}',
    '{"value":{"type":"Crash","fields":{"extra":null}},"version":1}',
    '{"value":{"type":"RegisterSource","fields":{}},"version":1}',
    '{"value":NaN,"version":1}',
    '{"value":Infinity,"version":1}',
    '{"value":1.0,"version":1}',
    '{"value":{"enum":"Source","name":"S0"},"version":1.0}',
    '{"value":{"enum":"Source","name":"S0"},"version":1} ',
    '{ "value":{"enum":"Source","name":"S0"},"version":1}',
    '{"version":1,"value":{"enum":"Source","name":"S0"}}',
    '{"value":{"enum":"Source","name":"\\u00530"},"version":1}',
    '{"value":{"tuple":[]},"version":01}',
])
def test_codec_refuses_unknown_duplicate_noncanonical_or_noninteger_json(text):
    with pytest.raises(ValueError):
        codec().decode(text)


def test_codec_decoding_enforces_field_types_not_just_json_types():
    module = codec()
    raw = json.loads(module.encode(envelope()))
    for field, bad in (("created", True), ("created", 1.0), ("created", 9),
                       ("source", {"enum": "Request", "name": "R0"}),
                       ("history", []), ("eligibility", False)):
        changed = json.loads(canonical(raw))
        changed["value"]["fields"][field] = bad
        with pytest.raises(ValueError):
            module.decode(canonical(changed))


def test_codec_bounds_depth_input_size_and_tuple_workload():
    module = codec()
    nested = sim.Source.S0
    for _ in range(80):
        nested = (nested,)
    with pytest.raises(ValueError):
        module.encode(nested)
    text = '{"value":' + '{"tuple":[' * 80 + '{"enum":"Source","name":"S0"}' + ']}' * 80 + ',"version":1}'
    with pytest.raises(ValueError):
        module.decode(text)
    with pytest.raises(ValueError):
        module.decode(" " * (2 * 1024 * 1024 + 1))
    with pytest.raises(ValueError):
        module.encode((sim.Source.S0,) * 65)
    with pytest.raises(ValueError):
        module.decode(canonical({"version": 1, "value": {"tuple": [
            {"enum": "Source", "name": "S0"}] * 65}}))


@pytest.mark.parametrize("value", [b"{}", None, [], 1])
def test_codec_decode_requires_exact_text(value):
    with pytest.raises(ValueError):
        codec().decode(value)

# Each lifecycle test exercises real local SQLite, including malformed fixtures.
def store_module():
    name = "tests.support.dual_view_sqlite_store"
    assert importlib.util.find_spec(name) is not None, "private SQLite store is not implemented"
    return importlib.import_module(name)


def test_create_exclusively_initializes_a_reopenable_complete_empty_store(tmp_path):
    import os
    import sqlite3
    import stat

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    with module.SqliteDualViewStore(path) as store:
        state = store.read()
        assert state == sim.ModelState()
        assert state.pending == ()
        assert state.simulation_only is True
        assert state.non_authoritative is True
        diagnostics = store.diagnostics
        assert diagnostics["sqlite_version"] == sqlite3.sqlite_version
        assert type(diagnostics["compile_options"]) is tuple
        assert diagnostics["compile_options"]
        assert diagnostics["journal_mode"] == "wal"
        assert diagnostics["synchronous"] == 2
        assert diagnostics["wal_autocheckpoint"] == 0
        assert 0 < diagnostics["busy_timeout"] <= 1000
        object.__setattr__(state, "clock", 8)
        assert store.read().clock == 0
    with module.SqliteDualViewStore(path) as reopened:
        assert reopened.read() == sim.ModelState()
        assert reopened.diagnostics["synchronous"] == 2
        assert reopened.diagnostics["wal_autocheckpoint"] == 0
    with pytest.raises(ValueError):
        store.read()


def test_create_refuses_existing_destinations_without_overwriting(tmp_path):
    import sqlite3

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path) as connection:
        before = connection.iterdump()
        before = tuple(before)
    with pytest.raises(FileExistsError):
        module.create_store(path)
    with sqlite3.connect(path) as connection:
        assert tuple(connection.iterdump()) == before
    other = tmp_path / "existing"
    other.write_bytes(b"caller-owned")
    with pytest.raises(FileExistsError):
        module.create_store(other)
    assert other.read_bytes() == b"caller-owned"


def test_open_missing_and_final_symlink_never_creates_or_overwrites(tmp_path):
    module = store_module()
    missing = tmp_path / "missing.sqlite"
    with pytest.raises((ValueError, OSError)):
        with module.SqliteDualViewStore(missing):
            pass
    assert not missing.exists()
    target = tmp_path / "target.sqlite"
    module.create_store(target)
    link = tmp_path / "link.sqlite"
    link.symlink_to(target)
    with pytest.raises(ValueError):
        with module.SqliteDualViewStore(link):
            pass
    with pytest.raises(FileExistsError):
        module.create_store(link)
    with module.SqliteDualViewStore(target) as store:
        assert store.read() == sim.ModelState()


def test_foreign_database_is_refused_before_wal_configuration(tmp_path):
    import sqlite3

    module = store_module()
    path = tmp_path / "foreign.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE foreign_data(value TEXT)")
        connection.execute("INSERT INTO foreign_data VALUES ('keep')")
    with pytest.raises(ValueError):
        with module.SqliteDualViewStore(path):
            pass
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        assert connection.execute("SELECT value FROM foreign_data").fetchall() == [("keep",)]
        assert connection.execute("SELECT name FROM sqlite_schema").fetchall() == [("foreign_data",)]


@pytest.mark.parametrize("sql", [
    "CREATE TABLE alien(value TEXT)",
    "CREATE INDEX alien ON events(payload)",
    "CREATE TRIGGER alien AFTER INSERT ON events BEGIN SELECT 1; END",
    "ALTER TABLE events ADD COLUMN alien INTEGER",
    "UPDATE prototype_meta SET version=2",
    "UPDATE prototype_meta SET marker='foreign'",
    "DELETE FROM prototype_meta",
    "PRAGMA application_id=0",
])
def test_unknown_schema_or_marker_refuses_without_logical_repair(tmp_path, sql):
    import sqlite3

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path) as connection:
        connection.execute(sql)
    with sqlite3.connect(path) as connection:
        before = tuple(connection.iterdump())
    with pytest.raises(ValueError):
        with module.SqliteDualViewStore(path) as store:
            store.read()
    with sqlite3.connect(path) as connection:
        assert tuple(connection.iterdump()) == before


@pytest.mark.parametrize("sql,parameters", [
    ("DELETE FROM projection", ()),
    ("UPDATE projection SET payload=?", ('{"value":{"tuple":[]},"version":1}',)),
    ("UPDATE projection SET payload=?", ("not-json",)),
    ("UPDATE projection SET payload=?", (" " * (2 * 1024 * 1024 + 1),)),
    ("UPDATE projection SET payload=?", (b"blob",)),
    ("INSERT INTO events(sequence,payload) VALUES(2,?)", ("not-json",)),
    ("INSERT INTO events(sequence,payload) VALUES(1,?)", (" " * (16 * 1024 + 1),)),
    ("INSERT INTO winners(source,payload) VALUES('S0',?)", ("not-json",)),
    ("INSERT INTO resources(source,payload) VALUES('S2',?)", ("not-json",)),
])
def test_read_refuses_absent_oversized_wrong_type_or_inconsistent_rows(tmp_path, sql, parameters):
    import sqlite3

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path) as connection:
        connection.execute(sql, parameters)
    with sqlite3.connect(path) as connection:
        before = tuple(connection.iterdump())
    with pytest.raises(ValueError):
        with module.SqliteDualViewStore(path) as store:
            store.read()
    with sqlite3.connect(path) as connection:
        assert tuple(connection.iterdump()) == before


def test_read_refuses_nonempty_events_with_empty_projection(tmp_path):
    import sqlite3

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    event = sim.Event(1, sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL))
    with sqlite3.connect(path) as connection:
        connection.execute("INSERT INTO events VALUES(1,?)", (codec().encode(event),))
    with pytest.raises(ValueError):
        with module.SqliteDualViewStore(path) as store:
            store.read()


def test_read_proceeds_while_a_wal_writer_holds_the_lock(tmp_path):
    import sqlite3
    import time

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path, isolation_level=None) as locker:
        locker.execute("BEGIN EXCLUSIVE")
        # SQLite WAL readers may proceed. Opening must neither hang nor spin
        # while confirming the existing database's schema and configuration.
        started = time.monotonic()
        with module.SqliteDualViewStore(path) as store:
            assert store.read() == sim.ModelState()
        assert time.monotonic() - started < 3
        locker.execute("ROLLBACK")

@pytest.mark.parametrize("sql,parameters", [
    ("DELETE FROM projection", ()),
    ("UPDATE projection SET payload=?", ("not-json",)),
    ("UPDATE projection SET payload=?", (sim.ModelState(clock=1),)),
])
def test_open_refuses_missing_or_invalid_projection_before_returning_store(tmp_path, sql, parameters):
    import sqlite3

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path) as connection:
        parameters = tuple(codec().encode(value) if type(value) is sim.ModelState else value
                           for value in parameters)
        connection.execute(sql, parameters)
    with pytest.raises(ValueError):
        with module.SqliteDualViewStore(path):
            pass


def test_existing_store_requires_wal_without_converting_a_delete_journal(tmp_path):
    import sqlite3

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
    with pytest.raises(module.UnsupportedStoreError):
        with module.SqliteDualViewStore(path):
            pass
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)


def test_open_stops_within_bounded_busy_timeout_under_real_exclusive_lock(tmp_path):
    import sqlite3
    import time

    module = store_module()
    path = tmp_path / "owned.sqlite"
    module.create_store(path)
    with sqlite3.connect(path, isolation_level=None) as locker:
        locker.execute("PRAGMA wal_autocheckpoint=0")
        locker.execute("PRAGMA locking_mode=EXCLUSIVE")
        locker.execute("BEGIN EXCLUSIVE")
        locker.execute("UPDATE projection SET payload=payload")
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            with module.SqliteDualViewStore(path):
                pass
        assert time.monotonic() - started < 3
        locker.execute("ROLLBACK")


def finite_envelope(source=sim.Source.S0, *, request=sim.Request.R0, history=None,
                    version=sim.Version.B, action=sim.Action.SAME, generation=0,
                    expires=8, eligibility_expires=8, fixture=sim.Fixture.NORMAL):
    own_model = sim.Model.M0 if source is sim.Source.S0 else sim.Model.M1
    own_provider = sim.Provider.P0 if source is sim.Source.S0 else sim.Provider.P1
    evidence = sim.Evidence.E0 if source is sim.Source.S0 else sim.Evidence.E1
    model = own_model
    if action is not sim.Action.SAME:
        model = sim.Model.M1 if own_model is sim.Model.M0 else sim.Model.M0
    provider = sim.Provider.P0 if model is sim.Model.M0 else sim.Provider.P1
    eligibility = None
    if version is sim.Version.B and action is sim.Action.SAME:
        eligibility = sim.Eligibility(source, own_model, own_provider, evidence,
                                      sim.Policy.P0, 2, eligibility_expires)
    return sim.Envelope(source, request, version, sim.Entry.RECOVERY, action,
                        model, provider, (own_model,) if history is None else history,
                        eligibility, generation, 0, expires, fixture)


@pytest.fixture
def writable_store(tmp_path):
    module = store_module()
    path = tmp_path / "transaction.sqlite"
    module.create_store(path)
    with module.SqliteDualViewStore(path) as store:
        yield path, store


def assert_synthetic(result, outcome):
    state, observation = result
    assert observation.outcome is outcome
    assert state.pending == ()
    assert state.simulation_only is True and state.non_authoritative is True
    assert observation.simulation_only is True and observation.non_authoritative is True
    return state


def ready_source(store, source=sim.Source.S0, *, history=None, fixture=sim.Fixture.NORMAL):
    model = sim.Model.M0 if source is sim.Source.S0 else sim.Model.M1
    history = (model,) if history is None else history
    state = assert_synthetic(store.apply(sim.RegisterSource(source, history, fixture)),
                             sim.Outcome.REGISTERED)
    for fact in sim.Fact:
        state = assert_synthetic(store.apply(sim.SetFacts(source, ((fact, sim.Value.YES),), 0)),
                                 sim.Outcome.RECORDED)
    return state


def logical_rows(path):
    from contextlib import closing
    import sqlite3

    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA wal_autocheckpoint=0")
        return tuple((table, tuple(connection.execute(f"SELECT * FROM {table} ORDER BY 1")))
                     for table in ("prototype_meta", "events", "projection", "winners", "resources"))


def test_apply_persists_fixed_commands_and_commit_atomically_debits_once(writable_store):
    path, store = writable_store
    ready = ready_source(store)
    assert len(ready.events) == 9
    item = finite_envelope()
    proposed = assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    assert len(proposed.events) == 10
    committed = assert_synthetic(store.commit(item), sim.Outcome.WINNER)
    assert len(committed.events) == 11
    assert committed.events[-1] == sim.Event(11, sim.Commit(item))
    assert len(committed.winners) == 1
    assert committed.winners[0] == sim.Winner(item, sim.Reservation(item, False, 3), 3)
    assert committed.sources[0].history == (sim.Model.M0,)
    assert (committed.sources[0].budget, committed.sources[0].attempts,
            committed.sources[0].retry_level) == (0, 3, 2)
    assert store.read() == committed
    stored = dict(logical_rows(path))
    assert stored["winners"] == (("S0", codec().encode(committed.winners[0])),)
    assert stored["resources"] == (("S0", codec().encode(committed.sources[0])),)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(item), sim.Outcome.WINNER) == committed
    assert logical_rows(path) == before
    with store_module().SqliteDualViewStore(path) as reopened:
        assert reopened.read() == committed
        assert assert_synthetic(reopened.commit(item), sim.Outcome.WINNER) == committed
    assert logical_rows(path) == before


@pytest.mark.parametrize("winner_version", [sim.Version.V1, sim.Version.B])
@pytest.mark.parametrize("historically_excluded", [True, False])
def test_cross_version_winner_is_unique_without_widening_v1_rules(
        writable_store, winner_version, historically_excluded):
    path, store = writable_store
    history = (sim.Model.M0,) if historically_excluded else ()
    ready_source(store, history=history)
    old = finite_envelope(version=sim.Version.V1, history=history,
                          action=sim.Action.FALLBACK if historically_excluded else sim.Action.SAME)
    new = finite_envelope(request=sim.Request.R1, history=history)
    if historically_excluded:
        held = store.read()
        assert assert_synthetic(store.apply(sim.Propose(replace(old, action=sim.Action.SAME,
            model=sim.Model.M0, provider=sim.Provider.P0))), sim.Outcome.HELD) == held
    assert_synthetic(store.apply(sim.Propose(old)), sim.Outcome.PROPOSED)
    assert_synthetic(store.apply(sim.Propose(new)), sim.Outcome.PROPOSED)
    first, second = (old, new) if winner_version is sim.Version.V1 else (new, old)
    won = assert_synthetic(store.commit(first), sim.Outcome.WINNER)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(second), sim.Outcome.SUPERSEDED) == won
    assert logical_rows(path) == before
    assert len(won.winners) == 1
    assert won.winners[0].envelope == first
    assert won.sources[0].history == history
    assert (won.sources[0].budget, won.sources[0].attempts, won.sources[0].retry_level) == (0, 3, 2)


def test_same_request_changed_frozen_envelope_conflicts_before_and_after_winner(writable_store):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    proposed = assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    changed = replace(item, entry=sim.Entry.IMPORT)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(changed), sim.Outcome.CONFLICT) == proposed
    assert assert_synthetic(store.apply(sim.Propose(changed)), sim.Outcome.CONFLICT) == proposed
    assert logical_rows(path) == before
    committed = assert_synthetic(store.commit(item), sim.Outcome.WINNER)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(changed), sim.Outcome.CONFLICT) == committed
    assert logical_rows(path) == before


def test_two_sources_keep_independent_winners_full_bindings_and_debits(writable_store):
    path, store = writable_store
    for source in sim.Source:
        ready_source(store, source)
        assert_synthetic(store.apply(sim.Propose(finite_envelope(source))), sim.Outcome.PROPOSED)
    first = assert_synthetic(store.commit(finite_envelope(sim.Source.S0)), sim.Outcome.WINNER)
    assert (first.sources[0].budget, first.sources[1].budget) == (0, 1)
    second = assert_synthetic(store.commit(finite_envelope(sim.Source.S1)), sim.Outcome.WINNER)
    assert len(second.winners) == 2 and len(second.events) == 22
    assert [(item.budget, item.attempts, item.retry_level) for item in second.sources] == [(0, 3, 2), (0, 3, 2)]
    before = logical_rows(path)
    for source in sim.Source:
        assert assert_synthetic(store.commit(finite_envelope(source)), sim.Outcome.WINNER) == second
    assert logical_rows(path) == before


@pytest.mark.parametrize("command", [
    sim.PrepareCommit(envelope()), sim.Commit(envelope()), sim.Crash(),
    sim.Replay((), (), 0), sim.SimulateDispatchCheck(envelope()), sim.ModelState(),
    (), "production-id", object(),
])
def test_apply_refuses_external_volatile_commands_states_and_arbitrary_values(writable_store, command):
    path, store = writable_store
    before = logical_rows(path)
    with pytest.raises(ValueError):
        store.apply(command)
    assert store.read() == sim.ModelState()
    assert logical_rows(path) == before


@pytest.mark.parametrize("item", ["production-id", sim.PrepareCommit(envelope()), sim.ModelState()])
def test_commit_requires_an_exact_finite_envelope(writable_store, item):
    path, store = writable_store
    before = logical_rows(path)
    with pytest.raises(ValueError):
        store.commit(item)
    assert logical_rows(path) == before
    assert store.read() == sim.ModelState()


@pytest.mark.parametrize("reason,outcome", [
    (sim.Invalidation.UNKNOWN, sim.Outcome.RECONCILIATION),
    (sim.Invalidation.REVOKED, sim.Outcome.HELD),
    (sim.Invalidation.STALE, sim.Outcome.HELD),
    (sim.Invalidation.INPUT, sim.Outcome.HELD),
    (sim.Invalidation.EFFECT, sim.Outcome.HELD),
    (sim.Invalidation.OLD_SEND, sim.Outcome.HELD),
    (sim.Invalidation.RESERVATION, sim.Outcome.HELD),
])
def test_commit_rechecks_persisted_invalidation_after_proposal(writable_store, reason, outcome):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    blocked = assert_synthetic(store.apply(sim.Invalidate(sim.Source.S0, reason)), sim.Outcome.RECORDED)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(item), outcome) == blocked
    assert logical_rows(path) == before
    assert not blocked.winners
    assert (blocked.sources[0].budget, blocked.sources[0].attempts, blocked.sources[0].retry_level) == (1, 2, 1)


@pytest.mark.parametrize("unknown_first", [True, False])
def test_unknown_before_or_after_winner_preserves_exact_history_once(writable_store, unknown_first):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    rival = finite_envelope(request=sim.Request.R1)
    for candidate in (item, rival):
        assert_synthetic(store.apply(sim.Propose(candidate)), sim.Outcome.PROPOSED)
    if not unknown_first:
        assert_synthetic(store.commit(item), sim.Outcome.WINNER)
    unknown = assert_synthetic(store.apply(sim.SetFacts(sim.Source.S0,
        ((sim.Fact.FAILED, sim.Value.UNKNOWN),), 0)), sim.Outcome.RECORDED)
    before = logical_rows(path)
    expected = sim.Outcome.RECONCILIATION if unknown_first else sim.Outcome.WINNER
    assert assert_synthetic(store.commit(item), expected) == unknown
    assert assert_synthetic(store.commit(rival), sim.Outcome.RECONCILIATION) == unknown
    assert logical_rows(path) == before
    assert len(unknown.winners) == (0 if unknown_first else 1)
    if not unknown_first:
        _, observation = sim.step(unknown, sim.SimulateDispatchCheck(item))
        assert observation.outcome is sim.Outcome.RECONCILIATION


@pytest.mark.parametrize("fixture", [
    sim.Fixture.ATTEMPTS_FULL, sim.Fixture.RETRY_FULL, sim.Fixture.BUDGET_EMPTY,
])
def test_resource_fixture_never_creates_a_proposal_winner_or_debit(writable_store, fixture):
    path, store = writable_store
    state = ready_source(store, fixture=fixture)
    item = finite_envelope(fixture=fixture)
    before = logical_rows(path)
    assert assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.HELD) == state
    assert assert_synthetic(store.commit(item), sim.Outcome.HELD) == state
    assert logical_rows(path) == before


@pytest.mark.parametrize("expiry_field", ["expires", "eligibility"])
def test_commit_rechecks_expiry_after_proposal(writable_store, expiry_field):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope(expires=1 if expiry_field == "expires" else 8,
                           eligibility_expires=1 if expiry_field == "eligibility" else 8)
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    advanced = assert_synthetic(store.apply(sim.SetFacts(sim.Source.S0,
        ((sim.Fact.HEALTH, sim.Value.YES),), 2)), sim.Outcome.RECORDED)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(item), sim.Outcome.HELD) == advanced
    assert logical_rows(path) == before


def test_generation_closure_blocks_old_plan_and_allows_only_new_generation(writable_store):
    path, store = writable_store
    ready_source(store)
    old = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(old)), sim.Outcome.PROPOSED)
    closed = assert_synthetic(store.apply(sim.CloseGeneration(sim.Source.S0, 0)), sim.Outcome.RECORDED)
    before = logical_rows(path)
    assert assert_synthetic(store.commit(old), sim.Outcome.HELD) == closed
    assert logical_rows(path) == before
    new = finite_envelope(request=sim.Request.R1, generation=1)
    assert_synthetic(store.apply(sim.Propose(new)), sim.Outcome.PROPOSED)
    won = assert_synthetic(store.commit(new), sim.Outcome.WINNER)
    assert won.closed_generations == ((sim.Source.S0, 0),)
    assert len(won.winners) == 1


@pytest.mark.parametrize("alteration", [
    {"source_fencing_generation": 1},
    {"source_fencing_generation": 3},
    {"source_fencing_generation": True},
    {"eligibility": replace(envelope().eligibility, source=sim.Source.S1)},
    {"eligibility": replace(envelope().eligibility, evidence=sim.Evidence.E1)},
    {"eligibility": replace(envelope().eligibility, source_fencing_generation=3)},
    {"provider": sim.Provider.P1},
    {"history": (sim.Model.M1,)},
    {"request": "production-id"},
    {"generation": True},
])
def test_wrong_fence_source_binding_or_request_cannot_mutate_store(writable_store, alteration):
    path, store = writable_store
    state = ready_source(store)
    item = replace(finite_envelope(), **alteration)
    before = logical_rows(path)
    assert assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.HELD_INVALID) == state
    # A valid shape with a mismatched binding is invalid even without a proposal.
    assert assert_synthetic(store.commit(item), sim.Outcome.HELD_INVALID) == state
    assert logical_rows(path) == before


_PHASES = ("after_begin", "after_events", "after_projection",
           "after_indexes", "before_commit", "after_commit")


def test_commit_hook_phases_keep_all_rows_invisible_until_commit(writable_store):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    before = logical_rows(path)
    phases = []

    def observe(phase):
        phases.append(phase)
        visible = dict(logical_rows(path))
        if phase == "after_commit":
            assert len(visible["events"]) == 11
            assert len(visible["winners"]) == 1
            resource = codec().decode(visible["resources"][0][1])
            assert (resource.budget, resource.attempts, resource.retry_level) == (0, 3, 2)
        else:
            assert logical_rows(path) == before

    assert_synthetic(store.commit(item, hook=observe), sim.Outcome.WINNER)
    assert tuple(phases) == _PHASES


@pytest.mark.parametrize("operation", ["apply", "commit"])
@pytest.mark.parametrize("phase", _PHASES[:-1])
@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_precommit_hook_error_rolls_back_every_owned_table_and_clears_transaction(
        writable_store, operation, phase, error_type):
    path, store = writable_store
    if operation == "commit":
        ready_source(store)
        item = finite_envelope()
        assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
        invoke = lambda hook: store.commit(item, hook=hook)
    else:
        invoke = lambda hook: store.apply(
            sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL), hook=hook)
    before_state, before_rows = store.read(), logical_rows(path)
    error = error_type("owned cut point")

    def cut(current):
        if current == phase:
            raise error

    with pytest.raises(error_type) as caught:
        invoke(cut)
    assert caught.value is error
    assert store.read() == before_state
    assert logical_rows(path) == before_rows
    with store_module().SqliteDualViewStore(path) as reopened:
        assert reopened.read() == before_state


@pytest.mark.parametrize("operation", ["apply", "commit"])
def test_after_commit_error_preserves_committed_state_and_exact_retry(writable_store, operation):
    path, store = writable_store
    if operation == "commit":
        ready_source(store)
        item = finite_envelope()
        assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
        invoke = lambda hook: store.commit(item, hook=hook)
        expected_outcome, expected_count = sim.Outcome.WINNER, 11
    else:
        command = sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL)
        invoke = lambda hook: store.apply(command, hook=hook)
        expected_outcome, expected_count = sim.Outcome.REGISTERED, 1
    error = RuntimeError("response lost after commit")

    def lose_response(phase):
        if phase == "after_commit":
            raise error

    with pytest.raises(RuntimeError) as caught:
        invoke(lose_response)
    assert caught.value is error
    committed = store.read()
    assert len(committed.events) == expected_count and committed.pending == ()
    before_rows = logical_rows(path)
    assert assert_synthetic(invoke(None), expected_outcome) == committed
    assert logical_rows(path) == before_rows
    with store_module().SqliteDualViewStore(path) as reopened:
        assert reopened.read() == committed


def test_hook_primary_error_survives_a_real_closed_connection_rollback_error(tmp_path):
    module = store_module()
    path = tmp_path / "cleanup.sqlite"
    module.create_store(path)
    error = KeyboardInterrupt("preserve this primary control-flow error")
    with module.SqliteDualViewStore(path) as store:
        def close_and_fail(phase):
            if phase == "after_begin":
                store._connection.close()
                raise error

        with pytest.raises(KeyboardInterrupt) as caught:
            store.apply(sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL), hook=close_and_fail)
        assert caught.value is error
    with module.SqliteDualViewStore(path) as reopened:
        assert reopened.read() == sim.ModelState()


def test_failed_rollback_closes_owned_connection_and_releases_real_sqlite_lock(writable_store):
    import sqlite3

    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    before = store.read()
    error = RuntimeError("primary hook error survives rollback denial")

    def deny_rollback(action, argument, database, trigger, context):
        if action == sqlite3.SQLITE_TRANSACTION and argument == "ROLLBACK":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def fail_with_rollback_denied(phase):
        if phase == "after_events":
            store._connection.set_authorizer(deny_rollback)
            raise error

    with pytest.raises(RuntimeError) as caught:
        store.commit(item, hook=fail_with_rollback_denied)
    assert caught.value is error
    with store_module().SqliteDualViewStore(path) as reopened:
        assert reopened.read() == before
        assert_synthetic(reopened.commit(item), sim.Outcome.WINNER)
    with pytest.raises(ValueError, match="closed"):
        store.read()


@pytest.mark.parametrize("event_count", [63, 64])
def test_event_cap_never_persists_or_returns_volatile_pending(writable_store, event_count):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    while len(store.read().events) < event_count:
        assert_synthetic(store.apply(sim.SetFacts(sim.Source.S0,
            ((sim.Fact.HEALTH, sim.Value.YES),), 0)), sim.Outcome.RECORDED)
    before = store.read()
    rows = logical_rows(path)
    if event_count == 64:
        assert assert_synthetic(store.commit(item), sim.Outcome.HELD_INVALID) == before
        assert logical_rows(path) == rows
        assert assert_synthetic(store.apply(sim.SetFacts(sim.Source.S0,
            ((sim.Fact.HEALTH, sim.Value.YES),), 0)), sim.Outcome.HELD_INVALID) == before
        assert not before.winners and before.sources[0].budget == 1
    else:
        committed = assert_synthetic(store.commit(item), sim.Outcome.WINNER)
        assert len(committed.events) == 64
        assert assert_synthetic(store.commit(item), sim.Outcome.WINNER) == committed
        assert committed.sources[0].budget == 0
    assert store.read().pending == ()
    with store_module().SqliteDualViewStore(path) as reopened:
        assert reopened.read() == store.read()
        assert reopened.read().pending == ()


@pytest.mark.parametrize("reason", list(sim.Invalidation))
def test_winner_history_replays_after_invalidation_without_authorizing_dispatch(writable_store, reason):
    path, store = writable_store
    ready_source(store)
    item, rival = finite_envelope(), finite_envelope(request=sim.Request.R1)
    for candidate in (item, rival):
        assert_synthetic(store.apply(sim.Propose(candidate)), sim.Outcome.PROPOSED)
    assert_synthetic(store.commit(item), sim.Outcome.WINNER)
    invalid = assert_synthetic(store.apply(sim.Invalidate(sim.Source.S0, reason)), sim.Outcome.RECORDED)
    rows = logical_rows(path)
    assert assert_synthetic(store.commit(item), sim.Outcome.WINNER) == invalid
    expected = sim.Outcome.RECONCILIATION if reason is sim.Invalidation.UNKNOWN else sim.Outcome.SUPERSEDED
    assert assert_synthetic(store.commit(rival), expected) == invalid
    assert logical_rows(path) == rows
    assert invalid.winners[0].reservation.revoked is (reason is sim.Invalidation.RESERVATION)
    _, observation = sim.step(invalid, sim.SimulateDispatchCheck(item))
    assert observation.outcome is (sim.Outcome.RECONCILIATION if reason is sim.Invalidation.UNKNOWN else sim.Outcome.HELD)


@pytest.mark.parametrize("update", ["unknown", "generation"])
def test_open_connection_reloads_changes_committed_by_another_writer(writable_store, update):
    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    with store_module().SqliteDualViewStore(path) as second:
        command = (sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN) if update == "unknown"
                   else sim.CloseGeneration(sim.Source.S0, 0))
        updated = assert_synthetic(second.apply(command), sim.Outcome.RECORDED)
    rows = logical_rows(path)
    outcome = sim.Outcome.RECONCILIATION if update == "unknown" else sim.Outcome.HELD
    assert assert_synthetic(store.commit(item), outcome) == updated
    assert logical_rows(path) == rows


def test_after_begin_hook_observes_exclusive_writer_lock_already_acquired(writable_store):
    from contextlib import closing
    import sqlite3

    path, store = writable_store
    checked = []

    def verify_locked(phase):
        if phase == "after_begin":
            with closing(sqlite3.connect(path, isolation_level=None, timeout=0.01)) as competitor:
                competitor.execute("PRAGMA wal_autocheckpoint=0")
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    competitor.execute("BEGIN IMMEDIATE")
                checked.append(True)

    assert_synthetic(store.apply(sim.RegisterSource(sim.Source.S0, (), sim.Fixture.NORMAL),
                                  hook=verify_locked), sim.Outcome.REGISTERED)
    assert checked == [True]


@pytest.mark.parametrize("tamper", [
    "missing_event", "event_gap", "wrong_ordinal", "invalid_transition", "noncanonical_event",
    "invalid_codec_event", "projection_clock", "projection_pending", "missing_projection",
    "winner_revoked", "missing_winner", "winner_wrong_source", "extra_winner",
    "resource_budget", "missing_resource", "resource_wrong_source", "extra_resource",
    "extra_schema_trigger",
])
def test_nonempty_store_refuses_every_inconsistent_view_without_logical_repair(writable_store, tamper):
    from contextlib import closing
    import sqlite3

    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    committed = assert_synthetic(store.commit(item), sim.Outcome.WINNER)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA wal_autocheckpoint=0")
        if tamper == "missing_event":
            connection.execute("DELETE FROM events WHERE sequence=11")
        elif tamper == "event_gap":
            connection.execute("DELETE FROM events WHERE sequence=2")
        elif tamper == "wrong_ordinal":
            connection.execute("UPDATE events SET payload=? WHERE sequence=11",
                (codec().encode(replace(committed.events[-1], ordinal=12)),))
        elif tamper == "invalid_transition":
            command = replace(committed.events[0].command, history=())
            connection.execute("UPDATE events SET payload=? WHERE sequence=1",
                (codec().encode(replace(committed.events[0], command=command)),))
        elif tamper == "noncanonical_event":
            connection.execute("UPDATE events SET payload=payload||' ' WHERE sequence=11")
        elif tamper == "invalid_codec_event":
            connection.execute("UPDATE events SET payload=? WHERE sequence=11",
                ('{"value":{"type":"Unknown","fields":{}},"version":1}',))
        elif tamper == "projection_clock":
            connection.execute("UPDATE projection SET payload=?",
                (codec().encode(replace(committed, clock=1)),))
        elif tamper == "projection_pending":
            connection.execute("UPDATE projection SET payload=?",
                (codec().encode(replace(committed, pending=(item,))),))
        elif tamper == "missing_projection":
            connection.execute("DELETE FROM projection")
        elif tamper == "winner_revoked":
            winner = replace(committed.winners[0],
                reservation=replace(committed.winners[0].reservation, revoked=True))
            connection.execute("UPDATE winners SET payload=?", (codec().encode(winner),))
        elif tamper == "missing_winner":
            connection.execute("DELETE FROM winners")
        elif tamper == "winner_wrong_source":
            connection.execute("UPDATE winners SET source='S1'")
        elif tamper == "extra_winner":
            connection.execute("INSERT INTO winners VALUES('S1',?)",
                (codec().encode(committed.winners[0]),))
        elif tamper == "resource_budget":
            source = replace(committed.sources[0], budget=1)
            connection.execute("UPDATE resources SET payload=?", (codec().encode(source),))
        elif tamper == "missing_resource":
            connection.execute("DELETE FROM resources")
        elif tamper == "resource_wrong_source":
            connection.execute("UPDATE resources SET source='S1'")
        elif tamper == "extra_resource":
            connection.execute("INSERT INTO resources VALUES('S1',?)",
                (codec().encode(committed.sources[0]),))
        elif tamper == "extra_schema_trigger":
            connection.execute("CREATE TRIGGER alien AFTER INSERT ON events BEGIN SELECT 1; END")
        connection.commit()
        schema_before = tuple(connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name"))
    rows = logical_rows(path)
    for operation in (store.read, lambda: store.apply(sim.Invalidate(sim.Source.S0, sim.Invalidation.UNKNOWN)),
                      lambda: store.commit(item)):
        with pytest.raises(ValueError):
            operation()
        assert logical_rows(path) == rows
    with pytest.raises(ValueError):
        with store_module().SqliteDualViewStore(path):
            pass
    with closing(sqlite3.connect(path)) as connection:
        assert tuple(connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name")) == schema_before
    assert logical_rows(path) == rows


@pytest.mark.parametrize("action,table", [
    ("insert", "events"), ("update", "projection"), ("insert", "winners"), ("insert", "resources"),
])
def test_real_sqlite_write_errors_rollback_partial_rows_and_allow_a_clean_retry(
        writable_store, action, table):
    import sqlite3

    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    before, rows = store.read(), logical_rows(path)
    denied_action = sqlite3.SQLITE_INSERT if action == "insert" else sqlite3.SQLITE_UPDATE

    def reject_write(code, argument, column, database, trigger):
        return sqlite3.SQLITE_DENY if code == denied_action and argument == table else sqlite3.SQLITE_OK

    store._connection.set_authorizer(reject_write)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="authorized"):
            store.commit(item)
    finally:
        store._connection.set_authorizer(None)
    assert store.read() == before
    assert logical_rows(path) == rows
    assert_synthetic(store.commit(item), sim.Outcome.WINNER)


def test_busy_write_refuses_without_retry_or_partial_mutation_and_can_be_retried(writable_store):
    from contextlib import closing
    import sqlite3
    import time

    path, store = writable_store
    ready_source(store)
    item = finite_envelope()
    assert_synthetic(store.apply(sim.Propose(item)), sim.Outcome.PROPOSED)
    before, rows = store.read(), logical_rows(path)
    with closing(sqlite3.connect(path, isolation_level=None)) as locker:
        locker.execute("PRAGMA wal_autocheckpoint=0")
        locker.execute("BEGIN IMMEDIATE")
        try:
            started = time.monotonic()
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                store.commit(item)
            assert time.monotonic() - started < 3
            assert store.read() == before
            assert logical_rows(path) == rows
        finally:
            locker.execute("ROLLBACK")
    assert_synthetic(store.commit(item), sim.Outcome.WINNER)
