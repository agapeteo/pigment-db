# Candidate Capture

Status: **ATTEMPT 1 FAILED — preserved; optimized retry candidate pending**.

## Attempt 1 provenance

| Field | Value |
|---|---|
| Capture IDs | `candidate-1`, `candidate-2`, `candidate-3` |
| Accepted implementation commit | `9cb2411351c8e6c86e412a79c02b5c5a3bdc9cb0` |
| Dirty-path listing SHA-256 | `5f4545757014d2f6e8193e59a0de24b72dfe9367e2fa8f1d90c74e08b08f9a94` |
| Harness SHA-256 | `c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c` |
| Rust/Cargo/target | `rustc 1.97.1 (8bab26f4f 2026-07-14)` / `cargo 1.97.1 (c980f4866 2026-06-30)` / `x86_64-unknown-linux-gnu` |
| OS/kernel/CPU | `Linux 7.0.11-76070011-generic x86_64` / `Intel(R) Core(TM) Ultra 7 155H` |
| Filesystem/data placement | `/dev/nvme0n1p4`, Btrfs project subvolume / `/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp` |
| CPU affinity | CPUs `12-19` |
| Approved capture window | `2026-08-21T02:05:50-05:00` through `2026-08-21T02:11:28-05:00` |
| Matrix result | 30/36 cells passed; six one-worker throughput cells failed |

The three complete matrices were run sequentially and paired ordinally with the
three immutable pre-feature matrices. Temporal counterbalancing was not
practical because the required pre-edit baseline had already been frozen and
captured. No observed run or cell was dropped. Each per-run test wrote its
complete CSV and metadata before its immediate ordinal assertion reported a
failure; the protocol verdict is the median across all three complete runs.

## Attempt 1 raw artifacts

| Artifact | SHA-256 |
|---|---|
| `candidate/candidate-1.csv` | `62c05f986ed464eb5e7d3476b613c754dd982fe58d67b65f1f9243f2086a5b9a` |
| `candidate/candidate-1.csv.metadata` | `e4c908c36a5e0c1b4b80fed2b1f4e7f97a8e64d1115ca61d370b32d850d0ac60` |
| `candidate/candidate-2.csv` | `f37abd2c60c037deff913e27b34a09866c6c3b6128b15f16b5da2f126b0232a5` |
| `candidate/candidate-2.csv.metadata` | `8881d72b3256e88493d20b3fd402db48d53c832bb345f8ff93bd2f0b41663baf` |
| `candidate/candidate-3.csv` | `26390074b313400e2ee1454e037b481f991a4e284f1d56cdefa5ccb403344220` |
| `candidate/candidate-3.csv.metadata` | `9229206c0861369de8d60182d9e6d9de15fdb27746bc3664fb5cbe8ad5a07d21` |

## RED and optimization

Attempt 1 is the performance RED. It showed a fixed uncontended-maintenance
cost rather than a contention collapse: every eight-worker throughput cell and
every median p95 cell passed, while six one-worker throughput cells failed.

The retry candidate keeps full per-instance coordination for file-backed
stores, replaces only that maintenance gate with a compact userspace
read/write lock, and makes vector-backed stores bypass filesystem-maintenance
coordination because they expose neither storage inspection nor online
compaction. A new deterministic RED–GREEN test proves the vector behavior;
the existing all-mutation ordering tests now exercise the file-backed stores
to preserve the required maintenance → logical key/shard → WAL ordering.

Several diagnostic matrices were intentionally excluded from acceptance. They
confirmed all file-backed cells pass after the optimization, but the host load
rose to approximately 7 and unrelated vector/p95 cells varied sharply by run
order. Those `/tmp` diagnostics neither replace nor modify the immutable raw
attempt-1 evidence.

## Optimized retry candidate provenance

| Field | Value |
|---|---|
| Accepted source commit | `19ecb1d7efaceecf2199f0458b50d3faebc0b3da` |
| Pre-capture dirty-tree digest | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (empty/clean) |
| Harness SHA-256 | `c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c` |
| Rust/Cargo/target | `rustc 1.97.1 (8bab26f4f 2026-07-14)` / `cargo 1.97.1 (c980f4866 2026-06-30)` / `x86_64-unknown-linux-gnu` |
| Resolved maintenance-lock dependency | `parking_lot 0.12.5` |
| Environment contract | Same filesystem, temporary-data root, CPU set `12-19`, workload matrix, and frozen baseline contract recorded in `README.md` |
| Build verification | Complete debug/release quality gates, Windows GNU cross-check, and byte-identical 36-cell baseline/candidate smoke traversals GREEN |
| Acceptance capture IDs | Pending: `candidate-retry-1`, `candidate-retry-2`, `candidate-retry-3` |

The accepted source commit contains the optimization and immutable attempt-1
evidence. This documentation-only provenance update does not change its release
binary. Native platform CI and a fresh explicit quiet-host confirmation remain
required before the three optimized acceptance matrices are captured.
