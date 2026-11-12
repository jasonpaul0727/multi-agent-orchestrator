"""Deterministic epoch lifecycle simulation; never an authorization capability.

These records are untrusted test input, not durable guard or database evidence.
Restart cannot resume an old simulated active state.
"""

from dataclasses import dataclass
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


def _identity(record):
    return record['instance'], record['epoch'], record['generation']


@dataclass(frozen=True)
class RecoveryAssessment:
    """Classification of simulated receipts; not a recovery authorization."""

    state: ModelState
    reason: str
    restart_requires_new_epoch: bool = True


def assess_recovery(guard_record, db_record):
    """Classify a simulated crash cut without reusing a prior epoch."""
    guard_valid = (guard_record is None or _valid_record(guard_record, 'guard_prepare')
                   or _valid_record(guard_record, 'guard_active'))
    db_valid = db_record is None or _valid_record(db_record, 'db_prepared')
    if not guard_valid or not db_valid:
        reason = 'malformed_receipt'
    elif guard_record is None and db_record is None:
        reason = 'no_receipts'
    elif guard_record is None:
        reason = 'guard_receipt_missing'
    elif db_record is None:
        reason = 'database_receipt_missing'
    elif _identity(guard_record) != _identity(db_record):
        reason = 'receipt_mismatch'
    elif guard_record['kind'] == 'guard_prepare':
        reason = 'prepared_before_activation'
    else:
        reason = 'matching_prior_receipts'
    return RecoveryAssessment(ModelState.UNCERTAIN, reason)


def recover_model(guard_record, db_record):
    """A new process cannot resume simulated ACTIVE from old records."""
    return assess_recovery(guard_record, db_record).state


class EpochModel:
    """Ephemeral simulation only; its state cannot establish live continuity."""

    def __init__(self):
        self.state = ModelState.ABSENT
        self.reason = 'ok'
        self._phase = 0
        self._identity = None
        self._guard_record = None
        self._db_record = None
        self._operation_in_flight = False

    @property
    def operation_in_flight(self):
        return self._operation_in_flight

    def _uncertain(self, reason='invalid_trace'):
        self.state = ModelState.UNCERTAIN
        self.reason = reason
        return self.state

    def _matches(self, record, kind):
        return _valid_record(record, kind) and _identity(record) == self._identity

    def _step(self, record, kind, expected_phase, new_state):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if self._phase != expected_phase or not _valid_record(record, kind):
            return self._uncertain()
        identity = _identity(record)
        if expected_phase and identity != self._identity:
            return self._uncertain()
        self._identity = identity
        self._phase += 1
        self.state = new_state
        if kind in ('guard_prepare', 'guard_active'):
            self._guard_record = dict(record)
        elif kind == 'db_prepared':
            self._db_record = dict(record)
        return self.state

    def start(self, record):
        return self._step(record, 'guard_prepare', 0, ModelState.ESTABLISHING)

    def mark_db_prepared(self, record):
        return self._step(record, 'db_prepared', 1, ModelState.ESTABLISHING)

    def activate_simulated(self, record):
        return self._step(record, 'guard_active', 2, ModelState.ACTIVE_SIMULATED)

    def admission_denied(self, record):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if self._phase != 0 or not _valid_record(record, 'admission_denied'):
            return self._uncertain()
        return self._uncertain('admission_denied')

    def operation_begin(self, record):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if (self.state is not ModelState.ACTIVE_SIMULATED or self._operation_in_flight
                or not self._matches(record, 'operation_begin')):
            return self._uncertain()
        self._operation_in_flight = True
        return self.state

    def operation_commit(self, record):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if (self.state is not ModelState.ACTIVE_SIMULATED or not self._operation_in_flight
                or not self._matches(record, 'operation_commit')):
            return self._uncertain()
        self._operation_in_flight = False
        return self.state

    def _commit_failed(self, record, kind, reason):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if (not self._matches(record, kind)
                or not (self._phase in (1, 2) or (self._phase == 3 and self._operation_in_flight))):
            return self._uncertain()
        return self._uncertain(reason)

    def db_commit_unknown(self, record):
        return self._commit_failed(record, 'db_commit_unknown', 'commit_unknown')

    def db_commit_exception(self, record):
        return self._commit_failed(record, 'db_commit_exception', 'commit_exception')

    def guard_lost(self, record):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if self._phase not in (1, 2, 3) or not self._matches(record, 'guard_lost'):
            return self._uncertain()
        return self._uncertain('guard_lost')

    def restart(self, record):
        if self.state is ModelState.UNCERTAIN:
            return self.state
        if self._phase not in (1, 2, 3) or not self._matches(record, 'restart'):
            return self._uncertain()
        if self._operation_in_flight:
            return self._uncertain('operation_incomplete')
        return self._uncertain(assess_recovery(self._guard_record, self._db_record).reason)
