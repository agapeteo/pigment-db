# Buffered Baseline Capture — Protocol V1 Historical

Status: immutable protocol-v1 evidence; invalid for protocol-v5 acceptance

The start-only eight-worker schedule intentionally permits independent buffered
workers after the round begins. This capture is preserved byte-for-byte at
`attempts/protocol-v1-baseline-20260807.csv`. Protocol v3 selected it for its
matching start-only buffered candidate matrix, and protocol v4 recaptured a pinned
copy. Protocol v5 links its own pre-feature comparator into the paired process, so
this historical file is not a comparator.

| Field | Value |
|---|---|
| Capture ID | `baseline-pre-feature` |
| Captured at (UTC) | 2026-08-07T09:25:01Z |
| Commit | `6d7edc7c29a60a94c59effeeb2b78d8b95038135` |
| Dirty-state hash | `default-hasher:f4a984dc06afd081` |
| Command | `CARGO_TARGET_DIR=/tmp/pigment-db-005-target PIGMENT_DB_DURABILITY_BENCH_ROOT=/tmp/pigment-db-durability-bench PIGMENT_DB_DURABILITY_OUTPUT=/tmp/pigment-db-baseline.csv cargo test --release --test durable_write_policy performance::capture_buffered_baseline -- --exact --ignored --nocapture --test-threads=1` |
| Benchmark root | `/tmp/pigment-db-durability-bench` |
| Rust toolchain / target | `rustc 1.97.0 (2d8144b78 2026-07-07)` / `x86_64-unknown-linux-gnu` |
| OS / CPU | `Linux 7.0.11-76070011-generic x86_64` / `Intel(R) Core(TM) Ultra 7 155H` |
| Filesystem | `/dev/nvme0n1p3[/tmp]`, ext4, `rw,nosuid,nodev,noatime,errors=remount-ro` |
| Protocol | 32-byte payload; 5 warmups; 11 samples; >=100 ms and >=1,024 operations per sample |
| Expected cells | 36 buffered |
| CSV SHA-256 | `6a4ca0b81f504459462c3870f0da1ce244a08313fa3afbf35284d104db3a3196` |
| Notes | 36 unique cells and 396 measured rows. The generated file was imported byte-for-byte because this worktree mount rejects ordinary process writes; `cmp` and SHA-256 verified identity. An earlier identical run produced no artifact when its final direct workspace write was rejected. |

The immutable raw sample rows are in `baseline.csv`.

Frozen issue #1–#4 fixture checksums at capture time:

| Fixture | SHA-256 |
|---|---|
| `kv.wal.dat` | `e48dee8c4a07db010778d08037ac96a6cd16ca5fb323ea40145bd1fa36cb75f2` |
| `set.wal.dat` | `d81d058ae3eabff04e08a8f12cad339e223f05f1fe532c82766b0565611cb653` |
| `map.wal.dat` | `4612530c4b7b95ef8cb557c0306b2f11a5598a053b31dffec9f9aedb9477e84e` |
