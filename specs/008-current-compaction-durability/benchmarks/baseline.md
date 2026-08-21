# Immutable Pre-Production Baseline

Status: **VALID — FROZEN**.

The user approved the quiet-machine window on 2026-08-20. Three complete
pre-production matrices then ran sequentially on CPUs `12-19`; no candidate or
production-feature measurement ran during this window.

## Provenance

| Field | Value |
|---|---|
| Capture IDs | `baseline-1`, `baseline-2`, `baseline-3` |
| Pre-feature commit | `a7c8281f72e25c177a142be99285faead7335e01` |
| Dirty-state listing SHA-256 | `cb72a95fc5d3cf49d1d6c1af6cd9c1181d6581ba3535001fb4dfe77107f7979c` |
| Harness SHA-256 | `c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c` |
| Rust/Cargo | `rustc 1.97.1 (8bab26f4f 2026-07-14)` / `cargo 1.97.1 (c980f4866 2026-06-30)` |
| Target | `x86_64-unknown-linux-gnu` |
| OS/kernel | `Linux 7.0.11-76070011-generic x86_64` |
| CPU | `Intel(R) Core(TM) Ultra 7 155H` |
| Filesystem | `/dev/nvme0n1p4`, Btrfs project subvolume |
| Temporary data root | `/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp` |
| CPU affinity | `12-19` from allowed set `0-21` |
| Exact command | Frozen parameterized command in [README.md](./README.md) |
| Capture completion times | `11:52:54`, `11:55:13`, `11:57:18` on 2026-08-20 (`-05:00`) |

## Raw artifacts

| Capture | Rows | CSV SHA-256 | Metadata SHA-256 |
|---|---:|---|---|
| `baseline/baseline-1.csv` | 36 | `ccaea6e78fb3981ae082ce896ee555be4401c34a6a4f28d87b3d2e4b197f36d8` | `9fb7b2d4b9e955996846b0be97ac81c5d83e296dfd29980bd2002c018c547a3f` |
| `baseline/baseline-2.csv` | 36 | `62a35d50b253d8c8f26cb1653747a47f402d47bb014d98b0805895125c1f0333` | `9bbbd2a5971f2884c0c623e50b17d736434cfb467711e52425e011fb8b9b35b4` |
| `baseline/baseline-3.csv` | 36 | `ae5fc3718cb661fc83b6dd914a54f43e1e857087488add28685e5da5614eb97c` | `4b54aa3d112a706cfa7b411e8f10bde571f7148b61d66836362271aab99438fc` |

Each CSV has exactly 36 unique cells, eleven measured samples per cell, at
least 1,024 operations per measured sample, finite positive throughput, and
positive aggregate p95 latency. All metadata agrees on source, harness,
toolchain, host, filesystem, affinity, payload, warmups, samples, and sampling
floors. These files are immutable comparator evidence; later results must not
replace or selectively recapture them.

## Same-window reconstructed baseline for final gate

The original artifacts above remain frozen lineage evidence. After two
candidate-only comparisons showed material day-to-day one-worker drift, the
pre-feature source was reconstructed at the same commit with the byte-identical
harness and recaptured in the user's approved 2026-08-21 quiet window. This is
the matching same-window baseline required by the counterbalanced performance
contract; it does not alter or replace the original files.

| Field | Value |
|---|---|
| Capture IDs | `baseline-retry-p2-1`, `baseline-retry-p2-2`, `baseline-retry-p2-3` |
| Pre-feature commit | `a7c8281f72e25c177a142be99285faead7335e01` |
| Reconstructed-worktree dirty digest | `8d4777e6137aefc871028d975c2649d481f3178970c7c2b0dae88537a2adca60` |
| Harness SHA-256 | `c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c` |
| Pair order | `B1-C1 / C2-B2 / B3-C3` |
| Capture completion times | `10:34:43`, `10:40:14`, `10:42:04` on 2026-08-21 (`-05:00`) |

| Capture | Rows | CSV SHA-256 | Metadata SHA-256 |
|---|---:|---|---|
| `baseline/baseline-retry-p2-1.csv` | 36 | `7bf93e03b7bf095c4d2d1f37cd29b1bda0bae00c26b1d289cdaed94656a619fc` | `09ff25cd9cb116ec28b41c31eaa0d8af0ea33840910a224131c2feccae9b1550` |
| `baseline/baseline-retry-p2-2.csv` | 36 | `ebd8f35c5d44b9380337e25b3111fa78fb69889b75b1a97823592ecdd4901cf0` | `9972736b7321dbfcd1c528ffc6c99ff64bbed90102245279bbc06c33e05083d9` |
| `baseline/baseline-retry-p2-3.csv` | 36 | `04c53c3f97a79ede8cac9b2c8cef0ed35880d8ddc162ff8657407a62ce75db8c` | `61a834283310448da492ddf282afa9cefca8625928d670cc111d472a7c8d8bd8` |
