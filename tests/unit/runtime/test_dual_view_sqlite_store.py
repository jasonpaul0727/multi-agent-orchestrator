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
