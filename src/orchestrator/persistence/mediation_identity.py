"""Stable v2 stream identity derived from the admitted Scheduler tuple."""

import hashlib
import json


def mediation_stream_id(run_id: str, node_id: str, attempt_id: str, fencing_generation: int) -> str:
    if (any(type(value) is not str or not value for value in (run_id, node_id, attempt_id))
            or type(fencing_generation) is not int or fencing_generation < 1):
        raise ValueError("invalid mediation identity")
    data = dict(domain="orchestrator.mediation.operation.v2", run_id=run_id,
                node_id=node_id, attempt_id=attempt_id, fencing_generation=fencing_generation)
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    return "mediation-v2-" + hashlib.sha256(encoded).hexdigest()
