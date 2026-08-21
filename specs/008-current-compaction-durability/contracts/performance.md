# Contract: Inactive-Compaction Performance Gate

## Purpose

The performance gate measures file-backed mutation paths after they gain a
per-store coordinator and vector-backed paths as unchanged controls. It does
not measure compaction speed. A matching feature-specific baseline must be
frozen before the first production hot-path edit.

## Matrix

Every complete run covers:

- families: key/value, key/set, key/map;
- backing: vector and file-backed buffered storage;
- profile: ordinary write, successful remove, minimal successful compute callback where supported;
- workers: one worker and eight workers using distinct keys for the eight-worker throughput cell;
- payload: fixed 32-byte keys and values/members/map values;
- five warmups and eleven measured samples per cell;
- at least 100 ms and 1,024 public operations per sample;
- median operations/second and aggregate public-call p95 latency.

The harness must consume results enough to prevent optimization and keep setup/teardown outside timed regions.

## Baseline and candidate capture

1. Add/freeze the harness and its source digest before production path edits.
2. Record the baseline commit and dirty-tree digest from the pre-feature implementation.
3. Use the same release toolchain, host, CPU affinity, power policy, filesystem, build flags, harness source, and data placement for baseline and candidate.
4. On a quiet pinned host, run three complete baseline matrices and three complete candidate matrices in counterbalanced order where practical.
5. Store raw CSV and metadata with SHA-256 checksums. Metadata includes commit, dirty state, Rust/Cargo versions, OS/kernel, CPU, filesystem, command, affinity, sample rules, and capture time.
6. Compare each matrix cell independently using the median metric across complete runs. Never drop a run or cell after observing its result without documenting a protocol-invalidating cause and recapturing the entire pair.

Final quiet-machine capture is an explicit acceptance step; preparation and diagnostic runs may occur earlier, but the final gate is evaluated only from protocol-complete captures.

## Thresholds

For every applicable cell:

- one-worker median throughput: `candidate / baseline >= 0.90`;
- eight-worker distinct-key median throughput: `candidate / baseline >= 0.85`;
- p95 latency: `candidate / baseline <= 1.25`.

Every threshold is inclusive. A passing family/profile cannot offset a failing cell. A zero/invalid sample, incomplete matrix, changed harness digest, or mismatched environment makes the capture invalid rather than passing.

## Deterministic structural evidence

Separate tests prove performance-relevant invariants independent of noisy timing:

- normal reads do not touch maintenance coordination;
- inactive delta recording allocates/clones no payload and performs only a branch under existing WAL state;
- one constant-size coordinator exists per store; no per-key/global coordination is introduced;
- staging encoding and both staging validations execute after the exclusive gate is released;
- unrelated stores and families do not share gates;
- mutation callbacks run after maintenance/shard guards are dropped.

## Gate result

The gate passes only when all structural tests pass and all valid performance cells satisfy their thresholds. The report includes baseline and candidate writes/operations per second, p95 values, ratios, environment metadata, CSV checksums, and any protocol retries. If it fails, implementation remains incomplete until optimized and recaptured under the same protocol.
