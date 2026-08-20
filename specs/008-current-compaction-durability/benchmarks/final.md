# Final Performance Gate Template

Status: not evaluated.

## Capture validity

Record the harness/environment match and all six raw CSV checksums here.

## Per-cell results

For every matrix cell, report baseline and candidate median operations per
second, aggregate p95 latency, throughput ratio, latency ratio, and verdict.

## Inclusive thresholds

- One-worker throughput ratio: at least `0.90`.
- Eight-worker distinct-key throughput ratio: at least `0.85`.
- Every p95 latency ratio: at most `1.25`.

All cells must pass; results cannot offset one another.
