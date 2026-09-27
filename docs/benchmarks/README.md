# Offline paired-run measurement

This evaluator is a measurement tool, **not** evidence that Maestro has met
the résumé targets. The repository currently contains no genuine paired
provider runs, independently verified workload corpus, or provider invoice
reconciliation. Unit-test traces are synthetic and must never be reported as
product results.

Run it with `PYTHONPATH=src python -m orchestrator.benchmarks /path/to/manifest.json`.
It reads local JSON only; it cannot call a model, access a credential, or spend
money. Invalid or missing input exits with code 2. A successful run prints a
machine-readable report with exact file SHA-256 digests, aggregate counts, and
percentage reductions. Archive the manifest, traces, verifier evidence,
provider usage records, billing/ledger evidence, and report together.

The manifest is strict JSON:

```json
{
  "schema_version": 1,
  "pairs": [
    {
      "feature_id": "feature-001",
      "baseline": "baseline/feature-001.json",
      "candidate": "candidate/feature-001.json"
    }
  ]
}
```

Each referenced trace must be a successful run of that feature. Its required
fields are `schema_version`, `feature_id`, `workload_sha256`,
`protocol_sha256`, `run_id`, `currency`, `status`, `verification`, `attempts`,
and `model_calls`. `verification` contains the frozen `verifier_id`, an
`evidence_file`, its `evidence_sha256`, and `accepted: true`. Each ordered attempt has a unique
`attempt_id`, increasing `sequence`, an `outcome` (`succeeded`, `failed`,
`cancelled`, or `unknown`), and `completed_work_keys`. A work key identifies
one *logical completed unit* and remains stable across retry/restart; it may
appear at most once per attempt. Each model call has a unique `call_id`, a
recorded `attempt_id`, `usage_record_id` and `cost_record_id`, and relative
`usage_evidence_file` and `cost_evidence_file` paths with their SHA-256 digests,
`usage_source`, `cost_source`, exact input/output token counts, optional
reasoning/cached breakdowns, and integer `cost_minor`. Accepted sources are
provider-reported or locally measured usage, paired with provider invoice,
settled ledger, or measured local cost as appropriate. Estimates and missing
usage cannot enter a report. Failed attempts' model charges **must** be
included. The trace exporter must record every call, including retries and
unknown-charge calls; if any usage remains unknown, do not publish a cost or
token improvement claim.

Pairing requires the same feature ID, workload hash, benchmark protocol hash,
verifier version, and currency. Every run ID must be unique. Each trace must
have model calls and completed work. Empty corpora, unverified runs, zero
baseline cost/tokens, or a zero baseline of failure-induced reexecution are
rejected. The latter means a recovery improvement cannot be inferred without
an actual failure/recovery workload.

The evaluator's token count is the sum of input and output tokens. Reasoning
and cached-input fields are reported breakdowns, not added again. The
failure-reexecution count increments when a work key completed in a failed
attempt is completed again in a later attempt. It excludes reexecution after
cancellation or ordinary parallel duplication. Percent reduction is
`(baseline - candidate) / baseline × 100`, aggregated over the paired corpus,
rounded only for display to two decimals. Negative values are preserved.
All monetary amounts are integer minor units; no FX conversion occurs in the
evaluator. Compare like-for-like USD settlement or first reconcile both
cohorts into one frozen currency basis.

The evaluator checks that each evidence file exists inside the manifest
directory, is at most 16 MiB, and matches its recorded SHA-256. It does not
parse provider-specific evidence formats or prove that an evidence document
actually belongs to a model call. File hashes show exactly which bytes were
measured, **not** that those bytes are authentic. Before using any metric publicly, independently audit the
corpus selection and quality threshold, trace/export completeness, verifier
independence, evidence-hash targets, provider billing reconciliation,
currency/price window, statistical uncertainty, and environmental parity.
This utility does not establish those controls and does not assert `$18 → $7`,
`90M → 40M`, `~65%` less repeated work, or `15%/85%` routing split.
