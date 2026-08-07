# Append-Plus-Barrier Reference Capture — Protocol V1 Invalidated

Status: immutable protocol-v1 evidence; invalid for protocol-v5 acceptance

The start-only eight-worker schedule let a reference worker repeatedly reacquire
the mutex and hide serialized queue wait from p95. This capture is preserved
byte-for-byte at `attempts/protocol-v1-reference-20260807.csv`. Protocol v3 used
the protocol-v2 physical reference instead, and protocol v4 recaptured a pinned
same-window reference. Protocol v5 measures its own reference side inside the
paired process; samples are never merged.

| Field | Value |
|---|---|
| Capture ID | `reference-pre-feature` |
| Captured at (UTC) | 2026-08-07T09:31:14Z |
| Commit | `6d7edc7c29a60a94c59effeeb2b78d8b95038135` |
| Dirty-state hash | `default-hasher:ed7c19232d71b0cf` |
| Command | `CARGO_TARGET_DIR=/tmp/pigment-db-005-target PIGMENT_DB_DURABILITY_BENCH_ROOT=/tmp/pigment-db-durability-bench PIGMENT_DB_DURABILITY_OUTPUT=/tmp/pigment-db-reference.csv cargo test --release --test durable_write_policy performance::capture_physical_reference -- --exact --ignored --nocapture --test-threads=1` |
| Benchmark root | `/tmp/pigment-db-durability-bench` |
| Rust toolchain / target | `rustc 1.97.0 (2d8144b78 2026-07-07)` / `x86_64-unknown-linux-gnu` |
| OS / CPU | `Linux 7.0.11-76070011-generic x86_64` / `Intel(R) Core(TM) Ultra 7 155H` |
| Filesystem | `/dev/nvme0n1p3[/tmp]`, ext4, `rw,nosuid,nodev,noatime,errors=remount-ro` |
| Protocol | 32-byte payload; 5 warmups; 11 samples; >=100 ms and >=1,024 operations per sample |
| Expected cells | 18 direct `write_all -> flush -> sync_data` reference |
| CSV SHA-256 | `ad16a5c090541cba4a19c64ae3e6444891996d04a5118a1af4141ebaa9233b26` |
| Notes | 18 unique cells and 198 measured rows. Every operation used one shared `Mutex<File>` and `write_all -> flush -> sync_data`; the generated CSV was imported byte-for-byte and verified with `cmp` and SHA-256. |

The immutable raw sample rows are in `reference.csv`.

Frozen issue #1–#4 fixture checksums at capture time:

| Fixture | SHA-256 |
|---|---|
| `kv.wal.dat` | `e48dee8c4a07db010778d08037ac96a6cd16ca5fb323ea40145bd1fa36cb75f2` |
| `set.wal.dat` | `d81d058ae3eabff04e08a8f12cad339e223f05f1fe532c82766b0565611cb653` |
| `map.wal.dat` | `4612530c4b7b95ef8cb557c0306b2f11a5598a053b31dffec9f9aedb9477e84e` |
