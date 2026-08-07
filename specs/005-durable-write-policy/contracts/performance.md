# Contract: Durability Performance Acceptance

## Purpose

The performance gate protects the existing buffered path and limits avoidable
overhead in physical mode without comparing physical storage latency directly to
buffered latency. Every cell is evaluated independently; faster cells never
compensate for a failure elsewhere.

## Immutable Evidence and Capture Order

1. Preserve every historical comparator and candidate capture byte-for-byte with
   its protocol version, provenance, checksum, and verdicts.
2. Protocol v5 links pre-feature commit
   `6d7edc7c29a60a94c59effeeb2b78d8b95038135` and the candidate as distinct
   packages in one release process. That process captures each of the 36
   buffered baseline/candidate comparisons and 18 append-plus-barrier
   reference/physical-candidate comparisons as counterbalanced AB/BA pairs.
3. Invoke the paired process through `taskset -c 12-19`.
   Verify that CPUs 12–19 still map to eight distinct physical cores with no SMT
   siblings before capture. Any affinity or topology mismatch invalidates the
   complete protocol-v5 attempt.
4. After correctness and quality gates pass, ask the user for a new quiet-machine
   window. Capture all 54 paired comparisons in one process and one unique,
   write-once CSV. Never reuse, concatenate, average, or otherwise merge samples
   from protocol v1–v4 or a focused diagnostic into protocol-v5 acceptance.

Baseline or reference data must not be regenerated after implementation to make a
candidate pass. A protocol or environment change invalidates the affected capture
and requires an explicitly documented full recapture from the pre-change commit.
Every candidate attempt is also retained unchanged; samples from different
attempts are never merged.

If a captured attempt proves that the benchmark protocol itself gives one side a
different scheduling opportunity, preserve all affected raw evidence and amend
the protocol explicitly. A comparator may be reused only when its policy matrix,
schedule, environment, pre-feature commit, and schema exactly match the amended
protocol; otherwise recapture the complete affected comparator matrix. Never
selectively regenerate only failing cells.

## Benchmark Root and Storage

The runner requires an explicit `PIGMENT_DB_V5_BENCH_ROOT` pointing to a
real filesystem. It rejects an unset path, a non-directory, or an in-memory/
synthetic backing. Both linked implementations in the paired process use the
same root, filesystem, release profile, toolchain, machine configuration,
process, and CPU affinity.

Each sample creates isolated files under that root, excludes setup/recovery from
the timed region, syncs as required by its policy, and removes sample artifacts
outside the timed region.

## Fixed Matrix

Common dimensions:

| Dimension | Values |
|---|---|
| store family | key/value, key/set, key/sorted-map |
| workload | ordinary write, successful removal, minimal callback/compute |
| workers | 1, 8 |
| payload | fixed 32-byte keys/search keys/values as applicable |
| warmups | 5 AB/BA pairs per comparison |
| measured samples | 11 AB/BA pairs per comparison |
| sample floor | at least 100 ms and at least 1,024 successful operations |

Storage/policy dimensions produce exactly:

- **Buffered baseline/candidate: 36 cells** = 3 families x 2 storage modes
  (`Vec<u8>`, file) x 3 workloads x 2 worker counts.
- **Physical candidate: 18 cells** = 3 families x file only x 3 workloads x 2
  worker counts.
- **Reference: 18 cells** = matching file record/group size x 3 families x 3
  workloads x 2 worker counts.

The paired benchmark process measures 108 cell-sides: 36 buffered candidates and
their 36 pre-feature comparators, plus 18 physical candidates and their 18
append-plus-barrier references. Eleven measured AB/BA pairs produce 1,188 raw
rows. Acceptance evaluates 54 comparisons. The final report displays exactly 72
rows: 36 buffered comparison rows containing candidate and baseline columns, 18
physical candidate rows, and 18 matching reference rows.

### Protocol-v5 one-process pairing, affinity, and scheduling

The single paired invocation is pinned with `taskset -c 12-19`.
Before capture, those logical CPUs must identify distinct cores 6–13, each with a
3,800 MHz advertised maximum, and no SMT sibling in the selected set. The test
process and every worker inherit that affinity. A changed CPU list, topology,
maximum-frequency class, or process affinity invalidates the complete protocol-v5
capture; selectively recapturing only an affected matrix is forbidden.

Each comparison runs five untimed warmup AB/BA pairs followed by eleven measured
AB/BA pairs in the same process. Even-numbered pairs run comparator then candidate;
odd-numbered pairs run candidate then comparator. Each side receives a fresh store,
and setup/removal occurs outside timing. There is no sleep or cooldown between the
two matched sides. All 54 comparisons must complete before the process writes its
single CSV.

Every eight-worker sample uses persistent workers within a round. Scheduling is
fixed by policy and is never selected per cell or per result:

- **Buffered baseline and candidate**: all workers and the timing coordinator
  rendezvous once before the round starts. Workers then execute the fixed round
  independently. This is the exact protocol-v1 buffered schedule.
- **Append-plus-barrier reference and physical candidate**: the same round-start
  rendezvous is followed by an all-worker rendezvous immediately before each
  timed public call, after completing the preceding call. This is the exact
  protocol-v2 physical schedule.

For physical/reference cells, the per-call timer starts after the operation
rendezvous, so synchronization wait is not attributed to a public call; no worker
may begin operation `n + 1` until every worker has returned from operation `n`.
For buffered cells, the call timer begins immediately before the public call and
there is no per-operation barrier. Round wall time includes the selected schedule
for throughput. One-worker cells need no rendezvous. Each candidate/comparator
pair therefore has identical scheduling even though the two policy matrices use
different schedules.

## Minimal Append-Plus-Barrier Reference

The reference measures the unavoidable serialized file persistence boundary, not
Pigment DB logic. For each matching cell it uses one shared `Mutex<File>` and,
inside that lock, performs:

```text
write_all(exact matching logical-mutation byte count)
  -> flush
  -> sync_data
```

One logical multi-record workload performs all matching writes followed by one
barrier. Exact matching reference bytes are preallocated outside the timed region;
the timed call includes lock acquisition, write, flush, and data synchronization.
Per-operation rendezvous makes concurrent lock-queue opportunity consistent with
the physical candidate. The reference adds no batching or group commit.

## Metrics

Each sample records:

- successful operations;
- elapsed nanoseconds;
- operations per second;
- per-public-call latency observations needed for p95;
- failed operations, which must be zero for a valid performance sample.

For each sample, p95 is computed across every public-call observation in that
sample. For each cell the report publishes median operations/second and the median
of its eleven independently measured sample p95 values. Raw sample summaries are
retained so aggregation can be reproduced.

## Acceptance Thresholds

For every buffered cell, compare the protocol-v5 candidate side with its paired
pre-feature buffered comparator side from the same process:

- 1 worker median throughput / matching baseline >= 0.90;
- 8 worker median throughput / matching baseline >= 0.85; and
- candidate p95 latency / matching baseline p95 <= 1.25.

For every physical cell, compare the protocol-v5 candidate side with its paired
append-plus-barrier reference side from the same process:

- 1 worker median throughput / reference >= 0.90;
- 8 worker median throughput / reference >= 0.85; and
- candidate p95 latency / reference p95 <= 1.25.

A missing/invalid cell, a nonzero operation failure count, or any failed ratio
fails the gate. Acceptance uses each side's aggregate median across its eleven
samples. Pair-by-pair ratios, aggregate means, and overall totals are diagnostic
only.

## CSV and Report Schema

Raw CSV rows contain at least:

```text
capture_id,baseline_commit,baseline_dirty_hash,candidate_commit,
candidate_dirty_hash,toolchain,target,os,cpu,filesystem,benchmark_root,affinity,
variant,implementation,policy,comparator,pair_index,position,store_family,
storage_mode,workload,workers,payload_bytes,warmup_pair_count,sample_index,
operations,elapsed_ns,ops_per_second,p95_latency_ns,failed_operations
```

The Markdown report adds comparator values, throughput ratio, latency ratio, and
`PASS`/`FAIL` for each cell. It also states commands, capture time, environmental
notes, and whether the quiet-machine window was confirmed.

If the benchmark process cannot write the repository worktree, it may write the
complete CSV once to a unique local staging path outside the timed benchmark root.
Before evaluation, require exactly 1,189 lines/1,188 data rows, 54 unique
comparison cells, 11 pair groups and both variants per cell, the exact alternating
AB/BA order, expected capture ID, zero failed operations, and SHA-256; then copy
once into the absent repository destination using an explicitly authorized
filesystem operation. `cmp` and matching source/destination SHA-256 must prove
byte identity.
Retain the staged source until evaluation and any final promotion are complete.
Failure to persist the in-memory rows is an execution failure with no benchmark
verdict and requires a new quiet-machine approval before retry.

## Performance-Failure Response

Correctness and thresholds are fixed. If direct per-mutation barriers fail a
physical cell, first determine whether a focused runtime RED reproduces avoidable
production overhead or a comparator/scheduling mismatch. For production overhead,
remove only avoidable allocation, encoding, or lock-path work without weakening
ordering. For a proven protocol mismatch, preserve all evidence, amend the
protocol, and fully recapture affected comparators from the pre-feature commit.
Group commit, delayed acknowledgement, a new global mutex, or relaxed thresholds
require a separately approved specification and may not be introduced as an
unplanned benchmark fix.

A failed candidate attempt follows this mandatory retry loop:

1. Preserve all 1,188 raw rows for 54 comparisons under
   `benchmarks/attempts/<capture-id>.csv` and its provenance plus failed verdicts
   under `benchmarks/attempts/<capture-id>.md`.
2. Add one focused runtime performance test and observe the expected RED.
3. Make the minimum optimization and run that test plus affected matrices GREEN.
4. Rerun formatting, strict Clippy, all-target, doc, compatibility, and durability
   quality gates after the code change.
5. Ask the user for a new quiet-machine window; prior approval does not authorize
   a new attempt.
6. Capture all 54 paired comparisons in one process/file under a new attempt ID
   on the same benchmark root and reevaluate every cell independently.

Repeat the loop for every failed attempt. Only one complete passing paired CSV may
be copied byte-for-byte to `benchmarks/final.csv` and used by `final.md`; the
report lists every retained failed attempt and the passing capture ID.
