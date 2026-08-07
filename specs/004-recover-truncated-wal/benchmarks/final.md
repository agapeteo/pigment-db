# V1 WAL Final Performance Report

Captured on 2026-08-06 after explicit quiet-machine confirmation. The final
steady-state matrix, startup gate, and retained-ordering-memory gate pass.

## Acceptance summary

- Steady-state cells: **36/36 PASS**.
- One-worker throughput: minimum ratio `0.950083` (required `>=0.90`).
- Eight-worker throughput: minimum ratio `0.974522` (required `>=0.85`).
- p95 latency: maximum ratio `1.179561` (required `<=1.25`).
- Complete-versus-torn startup median ratio: `0.7451` (required `<=1.25`).
- Retained live keys after 1,000 and 1,000,000 create/delete cycles: `0`.

The raw candidate values and every paired threshold are in `final.csv`. The
first threshold-failing attempt is preserved as
`candidate-failed-pre-optimization.csv`; the immutable inputs remain
`baseline.csv` and `startup-baseline.csv`.

## T265 RED–GREEN optimization

The first complete matrix exposed one isolated failure:
`key_map/vector/ordinary_write/8` delivered `232,934.596` writes/s
(`6.896339x` baseline), but its `27,328 ns` p95 was `1.683381x` baseline.
The focused release-only threshold test reproduced RED at `31,885 ns`
(`1.964088x`).

The minimum production change serializes and checksums a map-put payload before
entering shared WAL acceptance. It does not change record bytes, acceptance
order, per-key publication, rollback, or the public API. The focused test became
GREEN at `264,310.466` writes/s and `17,412 ns` p95. In the final full matrix the
cell remained GREEN at `264,192.031` writes/s (`7.821758x`) and `19,149 ns` p95
(`1.179561x`). The relevant 87 WAL tests, four key-map ordering tests, and 29
non-ignored truncation integration tests remained GREEN.

## Steady-state cells

Throughput is operations/writes per second. Each cell used five warmups and 11
measured samples. Thresholds are evaluated independently.

| Store | Mode | Profile | Workers | Baseline ops/s | Candidate ops/s | Throughput ratio | Baseline p95 ns | Candidate p95 ns | Latency ratio | Result |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| key_value | vector | ordinary_write | 1 | 1,865,070.035 | 1,911,738.206 | 1.025022 | 535 | 518 | 0.968224 | PASS |
| key_value | vector | ordinary_write | 8 | 223,248.876 | 237,649.961 | 1.064507 | 24,600 | 24,856 | 1.010407 | PASS |
| key_value | vector | successful_remove | 1 | 1,464,670.896 | 1,476,257.927 | 1.007911 | 529 | 514 | 0.971645 | PASS |
| key_value | vector | successful_remove | 8 | 30,958.343 | 34,212.847 | 1.105125 | 22,604 | 22,520 | 0.996284 | PASS |
| key_value | vector | minimal_callback | 1 | 1,784,964.247 | 1,771,557.821 | 0.992489 | 569 | 567 | 0.996485 | PASS |
| key_value | vector | minimal_callback | 8 | 117,005.741 | 134,120.232 | 1.146271 | 23,427 | 24,497 | 1.045674 | PASS |
| key_value | file | ordinary_write | 1 | 195,025.053 | 472,012.002 | 2.420263 | 6,347 | 2,661 | 0.419253 | PASS |
| key_value | file | ordinary_write | 8 | 96,482.213 | 143,293.899 | 1.485185 | 146,701 | 111,786 | 0.761999 | PASS |
| key_value | file | successful_remove | 1 | 187,289.671 | 539,494.476 | 2.880535 | 7,251 | 2,023 | 0.278996 | PASS |
| key_value | file | successful_remove | 8 | 21,077.373 | 37,960.006 | 1.800984 | 147,905 | 103,340 | 0.698692 | PASS |
| key_value | file | minimal_callback | 1 | 144,615.167 | 476,874.120 | 3.297539 | 7,475 | 2,467 | 0.330033 | PASS |
| key_value | file | minimal_callback | 8 | 56,191.940 | 102,056.499 | 1.816212 | 170,718 | 114,496 | 0.670673 | PASS |
| key_set | vector | ordinary_write | 1 | 1,071,752.476 | 1,269,259.730 | 1.184284 | 912 | 752 | 0.824561 | PASS |
| key_set | vector | ordinary_write | 8 | 201,501.617 | 238,876.888 | 1.185484 | 28,396 | 27,030 | 0.951895 | PASS |
| key_set | vector | successful_remove | 1 | 1,191,184.580 | 1,208,226.073 | 1.014306 | 647 | 625 | 0.965997 | PASS |
| key_set | vector | successful_remove | 8 | 34,357.055 | 37,184.817 | 1.082305 | 34,714 | 35,474 | 1.021893 | PASS |
| key_set | vector | minimal_callback | 1 | 821,933.131 | 880,668.754 | 1.071460 | 1,412 | 1,316 | 0.932011 | PASS |
| key_set | vector | minimal_callback | 8 | 113,120.659 | 118,301.856 | 1.045802 | 81,371 | 80,667 | 0.991348 | PASS |
| key_set | file | ordinary_write | 1 | 168,770.372 | 411,679.374 | 2.439287 | 7,223 | 2,913 | 0.403295 | PASS |
| key_set | file | ordinary_write | 8 | 65,434.107 | 129,424.470 | 1.977936 | 188,026 | 115,748 | 0.615596 | PASS |
| key_set | file | successful_remove | 1 | 170,982.097 | 452,843.112 | 2.648483 | 8,067 | 2,604 | 0.322797 | PASS |
| key_set | file | successful_remove | 8 | 8,297.934 | 36,546.311 | 4.404266 | 418,583 | 119,596 | 0.285716 | PASS |
| key_set | file | minimal_callback | 1 | 332,167.049 | 417,148.166 | 1.255838 | 3,573 | 3,022 | 0.845788 | PASS |
| key_set | file | minimal_callback | 8 | 12,051.518 | 91,843.034 | 7.620869 | 869,172 | 125,672 | 0.144588 | PASS |
| key_map | vector | ordinary_write | 1 | 1,123,252.065 | 1,439,515.494 | 1.281561 | 1,060 | 690 | 0.650943 | PASS |
| key_map | vector | ordinary_write | 8 | 33,776.556 | 264,192.031 | 7.821758 | 16,234 | 19,149 | 1.179561 | PASS |
| key_map | vector | successful_remove | 1 | 1,036,641.966 | 1,091,799.289 | 1.053208 | 1,008 | 961 | 0.953373 | PASS |
| key_map | vector | successful_remove | 8 | 23,571.617 | 36,623.496 | 1.553712 | 36,901 | 36,138 | 0.979323 | PASS |
| key_map | vector | minimal_callback | 1 | 616,311.926 | 611,442.665 | 0.992099 | 1,764 | 1,830 | 1.037415 | PASS |
| key_map | vector | minimal_callback | 8 | 111,558.250 | 108,716.011 | 0.974522 | 85,838 | 88,226 | 1.027820 | PASS |
| key_map | file | ordinary_write | 1 | 192,143.134 | 484,678.032 | 2.522484 | 7,028 | 2,941 | 0.418469 | PASS |
| key_map | file | ordinary_write | 8 | 91,321.762 | 133,633.105 | 1.463322 | 161,158 | 103,573 | 0.642680 | PASS |
| key_map | file | successful_remove | 1 | 187,765.728 | 404,032.085 | 2.151788 | 6,004 | 2,971 | 0.494837 | PASS |
| key_map | file | successful_remove | 8 | 31,359.661 | 37,386.395 | 1.192181 | 148,178 | 113,227 | 0.764128 | PASS |
| key_map | file | minimal_callback | 1 | 363,074.202 | 344,950.748 | 0.950083 | 3,492 | 4,083 | 1.169244 | PASS |
| key_map | file | minimal_callback | 8 | 82,235.950 | 87,115.624 | 1.059337 | 136,190 | 131,876 | 0.968324 | PASS |

## Startup gate

Each mode used 11 samples over a history containing 1,000,000 accepted
operations.

- Immutable complete-history baseline median: `8,044,284,168 ns`.
- Candidate complete-history median: `3,836,356,572 ns` (`0.4769x` baseline).
- Candidate terminally torn-history median: `2,858,484,996 ns`.
- Torn/complete median ratio: `0.7451`; threshold `<=1.25`: **PASS**.

## Retained-memory evidence

The clean candidate-file validation process reported:

| Cycles | Baseline RSS delta | Candidate RSS delta | Candidate live keys |
|---:|---:|---:|---:|
| 1,000 | 229,376 B | 520,192 B | 0 |
| 1,000,000 | 138,092,544 B | 138,043,392 B | 0 |

The matrix-generation process ran the memory sentinel after 96 seconds of
allocation-heavy measurements and observed a 1.72 MiB RSS allocator-state
variance. The same retained-memory target passed in an isolated fresh process,
and the full 36-cell candidate-file validation plus memory target passed in a
second clean process. No historical keys were retained in any run.

## Provenance

- Branch/HEAD: `main` / `fae6d38e70fb8bd1aff2841243eda8811ab26cc5`.
- Worktree: dirty with the feature implementation; benchmarked WAL source SHA-256:
  `d89d8a5eff91f2acb2e6c7c94f8b5048cdcea5c044d22262c205332f05978960`.
- Post-quality-gate WAL source SHA-256:
  `892145d55f42d9278138fd38574ad7031c6894b4de767f4e8770f53a4c3a3e3e`;
  the post-benchmark WAL-module delta only scopes two unused test helpers behind
  `cfg(test)` for strict Clippy and does not change the measured acceptance path.
- Benchmark driver SHA-256:
  `54fe51b48c60580ab456a8043d68bfe451b0656f95f9c9ce3ee73ac815eeb374`.
- Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`.
- Cargo: `cargo 1.97.0 (c980f4866 2026-06-30)`.
- Build: optimized `release`.
- OS: Linux `7.0.11-76070011-generic`, x86-64.
- CPU: Intel Core Ultra 7 155H, 22 logical CPUs online.
- Filesystem: btrfs on NVMe.
- Containment: transient user service, `MemoryMax=8G`, `MemorySwapMax=0`;
  observed final-matrix peak `149,794,816 B`, swap peak `0 B`.
- Matrix test body: `96.00 s`.
- Immutable baseline SHA-256:
  `d756820b4e863de7ce45ef61e31e7d13d7d16d9119bfe89709c15dbb26650733`.
- Final CSV SHA-256 after adding paired columns:
  `3c21459554a73d8b1ab3e355fe3f7f503ce88cccb525ea604df82e5de6fe1806`.
- Startup final CSV SHA-256:
  `fd9e6deb7519319c3ea78986a1de3bca0f0c0949d17389746c4aa40124ee7bb1`.

## Commands

The matrix-generation Cargo command was run by `systemd-run --user` with the
containment and environment above:

```bash
PIGMENT_DB_BENCHMARK_OUTPUT=specs/004-recover-truncated-wal/benchmarks/final.csv \
PIGMENT_DB_BENCHMARK_BASELINE=specs/004-recover-truncated-wal/benchmarks/baseline.csv \
  cargo test --release --test truncated_wal \
  performance::issue3::paired_candidate -- \
  --ignored --exact --nocapture --test-threads=1
```

The saved matrix and retained-memory result were then validated in a clean
process:

```bash
PIGMENT_DB_BENCHMARK_BASELINE=specs/004-recover-truncated-wal/benchmarks/baseline.csv \
PIGMENT_DB_BENCHMARK_CANDIDATE=specs/004-recover-truncated-wal/benchmarks/final.csv \
  cargo test --release --test truncated_wal \
  performance::issue3::paired_candidate -- \
  --ignored --exact --nocapture --test-threads=1
```
