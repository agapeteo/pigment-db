# Durability Benchmark Evidence

This directory holds immutable performance evidence for issue #5. The files are
created during implementation, not during planning.

Expected outputs:

| File | Timing | Contents |
|---|---|---|
| `baseline.csv` / `baseline.md` | before production edits | 36 buffered comparator cells and provenance |
| `reference.csv` / `reference.md` | before production edits | 18 minimal append-plus-barrier comparator cells and provenance |
| `final.csv` / `final.md` | after correctness gates and quiet-machine approval | one complete passing protocol-v5 paired CSV (1,188 rows); the report displays 36 buffered comparisons plus 18 physical and 18 reference rows, with 54 per-cell verdicts |

All captures use the same explicit real-filesystem root, release profile,
toolchain, machine, payloads, warmups, sampling floor, and worker counts. The
normative matrix, CSV schema, and thresholds are defined in
[the performance contract](../contracts/performance.md).

Baseline and reference captures are write-once evidence. If environment or
protocol drift makes them invalid, document why and recapture the complete
affected matrix from the pre-change commit; never replace individual failing
cells selectively.

Protocol v1 used only a round-start rendezvous; its physical/reference comparison
was invalid because the minimal reference could monopolize consecutive mutex
acquisitions. Protocol v2 added a rendezvous before every timed eight-worker call;
its physical matrix passed, but the barrier introduced unstable scheduler-tail
noise into microsecond buffered calls. Protocol v3 selected scheduling by policy,
but an old buffered baseline captured under uncontrolled heterogeneous-core
placement made the complete retry fail 14 buffered cells even though a
contemporaneous pre-feature diagnostic passed against the candidate.

Protocol v4 fixed affinity but its separate processes passed 50 of 54 comparisons;
the four failures were high-throughput eight-worker vector cells. T263 alternated
those four comparator/candidate pairs inside one pinned process and every aggregate
comparison passed unchanged thresholds, isolating residual scheduler/frequency
noise rather than a stable candidate-only regression.

Protocol v5 is normative. One release process links the pre-feature and candidate
crates, is invoked through `taskset -c 12-19`, and captures all 36 buffered plus
18 physical comparisons as counterbalanced AB/BA pairs. Buffered pairs use
start-only scheduling; physical/reference pairs use per-operation scheduling.
Five warmup and eleven measured pairs per comparison produce 1,188 data rows in
one write-once CSV. Protocol-v1 through protocol-v4 and T263 files remain immutable
historical/diagnostic evidence and are not protocol-v5 comparators. Thresholds and
dimensions are unchanged; any affinity drift invalidates the complete attempt.

## Capture provenance template

Every capture report records:

- capture identifier and UTC timestamp;
- repository commit and dirty-state hash;
- exact command and benchmark root;
- Rust toolchain and target;
- operating system, CPU, filesystem source/type, and mount options;
- payload, warmup, sample-count, duration, and operation floors;
- raw CSV SHA-256 checksum; and
- notes about machine load or protocol deviations.

Comparator reports contain provenance only until their complete raw capture has
finished. Result rows are generated from, and must remain reproducible from, the
immutable CSV.
