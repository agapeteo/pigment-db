# Post-Ordering Performance Snapshot

Captured on 2026-08-06 after the ordering, WAL rejection, and quality changes.
The environment matches the immutable baseline metadata in `pre-feature.md`.

## Command

```bash
cargo test --release --test mutation_ordering performance::paired_candidate -- --exact --ignored --nocapture --test-threads=1
```

The raw 36-cell snapshot and paired ratios are in `post-ordering.csv`. This is
an honest failing snapshot, not the required final GREEN artifact: 9 throughput
cells and 6 latency cells missed their independent limits. Exploratory repeats
failed different sets of cells, including unchanged paths, with ratios ranging
from 0.397 to 1.651 for throughput and 0.476 to 9.023 for p95. The recorded run
uses the same harness as the immutable baseline. It times only 1, 4, or 8
operations per worker, so many samples are a few microseconds and are highly
sensitive to host scheduling and CPU state. Thresholds were not weakened and
failing cells were not averaged away.

The paired retained-memory gate passed separately. Candidate RSS deltas were
589,824 bytes after 1,000 create/delete cycles and 138,039,296 bytes after
1,000,000 cycles. After subtracting the immutable baseline, added ordering
retention was 360,448 and 0 bytes, respectively, within the 110% limit with no
retained live keys.

`final.csv` and `final.md` are intentionally absent until all performance cells
pass their specified thresholds in a trustworthy run.
