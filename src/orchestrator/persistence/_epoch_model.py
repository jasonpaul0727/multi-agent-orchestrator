"""Deterministic epoch lifecycle simulation; never an authorization capability.

These records are untrusted test input, not durable guard or database evidence.
Restart cannot resume an old simulated active state.
"""

from enum import Enum


class ModelState(str, Enum):
    ABSENT = 'ABSENT'
    ESTABLISHING = 'ESTABLISHING'
    ACTIVE_SIMULATED = 'ACTIVE_SIMULATED'
    UNCERTAIN = 'UNCERTAIN'
    RETIRED = 'RETIRED'


_FIELDS = {'version', 'kind', 'instance', 'epoch', 'generation'}
_MAX_ID_BYTES = 128
_MAX_GENERATION = (1 << 63) - 1


def _valid_record(record, kind):
    if type(record) is not dict or set(record) != _FIELDS:
        return False
    if type(record['version']) is not int or record['version'] != 1 or record['kind'] != kind:
        return False
    if type(record['generation']) is not int or not 1 <= record['generation'] <= _MAX_GENERATION:
        return False
    for key in ('instance', 'epoch'):
        value = record[key]
        if type(value) is not str or not value:
            return False
        try:
            if len(value.encode('utf-8')) > _MAX_ID_BYTES:
                return False
        except UnicodeError:
            return False
    return True


class EpochModel:
    """Ephemeral simulation only; its state cannot establish live continuity."""

    def __init__(self):
        self.state = ModelState.ABSENT
        self._phase = 0
        self._identity = None

    def _step(self, record, kind, expected_phase, new_state):
        if (self.state is ModelState.UNCERTAIN or self._phase != expected_phase
                or not _valid_record(record, kind)):
            self.state = ModelState.UNCERTAIN
            return self.state
        identity = (record['instance'], record['epoch'], record['generation'])
        if expected_phase and identity != self._identity:
            self.state = ModelState.UNCERTAIN
            return self.state
        self._identity = identity
        self._phase += 1
        self.state = new_state
        return self.state

    def start(self, record):
        return self._step(record, 'guard_prepare', 0, ModelState.ESTABLISHING)

    def mark_db_prepared(self, record):
        return self._step(record, 'db_prepared', 1, ModelState.ESTABLISHING)

    def activate_simulated(self, record):
        return self._step(record, 'guard_active', 2, ModelState.ACTIVE_SIMULATED)


def recover_model(guard_record, db_record):
    """A new process cannot resume simulated ACTIVE from old records."""
    return ModelState.UNCERTAIN
