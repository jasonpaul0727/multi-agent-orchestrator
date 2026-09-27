"""Run the offline paired-trace evaluator without provider access."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from orchestrator.benchmarks import BenchmarkError, evaluate_manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate recorded Maestro benchmark pairs")
    parser.add_argument("manifest", help="Path to a strict JSON benchmark manifest")
    args = parser.parse_args(argv)
    try:
        report = evaluate_manifest(args.manifest)
    except BenchmarkError as error:
        print(f"benchmark rejected: {error}", file=sys.stderr)
        return 2
    print(report.model_dump_json(indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
