"""Fixed useful Worker child: bounded private pipes, no execution authority."""

from pathlib import Path
import sys


def _child_main() -> int:
    # The fixed child is invoked by absolute path under isolated system Python.
    # Import its trusted sibling directly, bypassing package/host startup imports.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from general_worker_protocol import MAX_FRAME_BYTES, _proposal_bytes

    try:
        raw = sys.stdin.buffer.read(MAX_FRAME_BYTES + 1)
        proposal = _proposal_bytes(raw)
        sys.stdout.buffer.write(proposal)
        sys.stdout.buffer.flush()
        return 0
    except Exception:
        return 64


if __name__ == "__main__":
    raise SystemExit(_child_main() if sys.argv[1:] == ["--child"] else 64)
