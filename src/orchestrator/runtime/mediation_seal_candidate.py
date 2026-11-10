"""Pure structural encoding; candidate hashes are not authenticated seals.

This module reads no registry and grants no launch, stop, seal or release
permission. Future authenticated evidence may need a different schema.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import re

from orchestrator.persistence.mediation_identity import mediation_stream_id
from .mediation_actors import MediationOperation

_DOMAIN = "orchestrator.mediation.seal-candidate.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_ACTORS = 128
_MAX_INTEGER = 2**63 - 1


class _CandidateRecord:
    __slots__ = ()

    def __init__(self, **values):
        fields = type(self).__dataclass_fields__
        if values.keys() != fields.keys():
            raise TypeError("invalid candidate")
        for name in fields:
            object.__setattr__(self, name, values[name])


@dataclass(frozen=True, slots=True, repr=False, init=False)
class ToolCandidate(_CandidateRecord):
    outcome: str
    outcome_sha256: str
    publication_id: str | None
    publication_sha256: str | None


@dataclass(frozen=True, slots=True, repr=False, init=False)
class ProviderCandidate(_CandidateRecord):
    outcome: str
    outcome_sha256: str
    receipt_sha256: str


@dataclass(frozen=True, slots=True, repr=False, init=False)
class VerifierCandidate(_CandidateRecord):
    evidence_sha256: str
    usage_sha256: str


@dataclass(frozen=True, slots=True, repr=False, init=False)
class ActorCandidate(_CandidateRecord):
    actor_id: str
    sequence: int
    kind: str
    permit_version: int
    ack_version: int
    stop_version: int
    prepared_sha256: str
    ack_sha256: str
    stop_sha256: str
    transport_sha256: str
    binding: ToolCandidate | ProviderCandidate | VerifierCandidate | None


@dataclass(frozen=True, slots=True, repr=False, init=False)
class SealCandidate(_CandidateRecord):
    operation: MediationOperation
    close_sequence: int
    registry_version: int
    actors: tuple[ActorCandidate, ...]
    closed: bool


@dataclass(frozen=True, slots=True)
class CandidateCapacity:
    before_seal: int
    including_seal: int
    max_late_evidence: int


def capacity_for_actors(count: int) -> CandidateCapacity:
    if type(count) is not int or not 0 <= count <= _MAX_ACTORS:
        raise ValueError("invalid candidate")
    # One registration, each permit/ACK/stop, one close, one future seal.
    before_seal = 2 + 3 * count
    return CandidateCapacity(before_seal, before_seal + 1, 2 * count)


def _identifier(value):
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError()


def _digest(value):
    if type(value) is not str or _DIGEST.fullmatch(value) is None:
        raise ValueError()


def _positive(value):
    if type(value) is not int or not 1 <= value <= _MAX_INTEGER:
        raise ValueError()


def _validate_operation(operation):
    if type(operation) is not MediationOperation:
        raise ValueError()
    for value in (operation.operation_id, operation.run_id, operation.node_id, operation.attempt_id):
        _identifier(value)
    _positive(operation.fencing_generation)
    if operation.operation_id != mediation_stream_id(operation.run_id, operation.node_id,
                                                      operation.attempt_id, operation.fencing_generation):
        raise ValueError()
    for name in ("task_input_sha256", "workspace_sha256", "capability_sha256", "manifest_sha256",
                 "policy_authority_sha256", "commitment_sha256"):
        _digest(getattr(operation, name))
    data = asdict(operation)
    commitment = data.pop("commitment_sha256")
    raw = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    if commitment != "sha256:" + hashlib.sha256(raw).hexdigest():
        raise ValueError()


def _validate_binding(actor):
    binding = actor.binding
    if type(actor.kind) is not str:
        raise ValueError()
    if actor.kind == "worker":
        if binding is not None:
            raise ValueError()
    elif actor.kind == "tool":
        if (type(binding) is not ToolCandidate or type(binding.outcome) is not str
                or binding.outcome not in ("captured", "known_failure")):
            raise ValueError()
        _digest(binding.outcome_sha256)
        if binding.outcome == "captured":
            _identifier(binding.publication_id)
            _digest(binding.publication_sha256)
        elif binding.publication_id is not None or binding.publication_sha256 is not None:
            raise ValueError()
    elif actor.kind == "provider_sender":
        if (type(binding) is not ProviderCandidate or type(binding.outcome) is not str
                or binding.outcome not in ("known_success", "known_failure", "not_sent")):
            raise ValueError()
        _digest(binding.outcome_sha256)
        _digest(binding.receipt_sha256)
    elif actor.kind == "verifier":
        if type(binding) is not VerifierCandidate:
            raise ValueError()
        _digest(binding.evidence_sha256)
        _digest(binding.usage_sha256)
    else:
        raise ValueError()


def _validate(candidate, expected_registry_version):
    if type(candidate) is not SealCandidate or candidate.closed is not True or type(candidate.actors) is not tuple:
        raise ValueError()
    _validate_operation(candidate.operation)
    _positive(candidate.close_sequence)
    _positive(candidate.registry_version)
    _positive(expected_registry_version)
    if candidate.registry_version != expected_registry_version:
        raise ValueError()
    count = len(candidate.actors)
    if count > _MAX_ACTORS or candidate.registry_version != capacity_for_actors(count).before_seal:
        raise ValueError()
    versions = {1, candidate.close_sequence}
    if len(versions) != 2 or candidate.close_sequence > candidate.registry_version:
        raise ValueError()
    actor_ids = set()
    previous_sequence = 0
    for actor in candidate.actors:
        if type(actor) is not ActorCandidate:
            raise ValueError()
        _identifier(actor.actor_id)
        _positive(actor.sequence)
        if actor.actor_id in actor_ids or actor.sequence <= previous_sequence:
            raise ValueError()
        actor_ids.add(actor.actor_id)
        previous_sequence = actor.sequence
        for value in (actor.permit_version, actor.ack_version, actor.stop_version):
            _positive(value)
            if value > candidate.registry_version or value in versions:
                raise ValueError()
            versions.add(value)
        if not (1 < actor.permit_version < candidate.close_sequence and
                actor.permit_version < actor.ack_version < actor.stop_version):
            raise ValueError()
        for digest in (actor.prepared_sha256, actor.ack_sha256, actor.stop_sha256, actor.transport_sha256):
            _digest(digest)
        _validate_binding(actor)
    if versions != set(range(1, candidate.registry_version + 1)):
        raise ValueError()


def encode_candidate(candidate: SealCandidate, *, expected_registry_version: int) -> bytes:
    """Return canonical structural bytes; input values are not authenticated."""
    try:
        _validate(candidate, expected_registry_version)
        data = {"domain": _DOMAIN, "schema_version": 1, **asdict(candidate)}
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, AttributeError, OverflowError):
        raise ValueError("invalid candidate") from None


def candidate_sha256(candidate: SealCandidate, *, expected_registry_version: int) -> str:
    """Hash structural bytes; this does not establish authority."""
    return "sha256:" + hashlib.sha256(encode_candidate(candidate,
        expected_registry_version=expected_registry_version)).hexdigest()
