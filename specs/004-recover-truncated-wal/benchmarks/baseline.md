# Immutable Pre-V1 Performance Baseline

Captured on 2026-08-06 before V1 header or record behavior was connected to
startup or steady-state writes. These rows are immutable candidate inputs.

## Provenance

- Git branch: `main`
- HEAD: `fae6d38e70fb8bd1aff2841243eda8811ab26cc5`
- Worktree: dirty with the completed issue #1–#3 changes and feature-004 test
  scaffolding; production diff SHA-256 at measurement time:
  `8bfcc306329ac24f94d64b8a9e0ee7fb8cd883f7f7c4956fe965d8a8e29db55a`
- Startup driver SHA-256:
  `9e42427db0bc281af0e2135022fbefa17399bf4b35c5de102ebbc399a8b377a7`
- Reused issue #3 driver SHA-256:
  `dbb428ddc60164e5d6654336b692e84a554ccb6d7da8bd33a03f0c1d3ed0735a`
- Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`
- Cargo: `cargo 1.97.0 (c980f4866 2026-06-30)`
- Build profile: `release` (optimized)
- OS: Linux `7.0.11-76070011-generic`, x86-64
- CPU: Intel Core Ultra 7 155H, 22 logical CPUs online
- Filesystem: btrfs
- Matrix configuration: 5 warmups, 11 measured samples per steady-state
  cell; 11 measured complete-history startup samples
- Process containment: both aligned final baseline runs used a temporary user
  service with `MemoryMax=8G` and `MemorySwapMax=0`. Candidate measurements must
  use the same containment. The final candidate additionally requires explicit
  user confirmation that the machine is quiet.

The initial startup attempt exposed quadratic replay retention: each accepted
legacy frame cloned and retained the full logical snapshot. A 10,000-operation
diagnostic peaked at 14,492,016 KiB and took 35.07 seconds. The behavior-focused
RED `wal::replay::tests::complete_replay_does_not_retain_a_snapshot_per_frame`
failed with 32 retained snapshots. Streaming exact-prefix matching made it GREEN
while all 18 recovery integration tests, including the newer-active-prefix case,
remained GREEN. The same diagnostic then peaked at 249,348 KiB and completed the
test body in 0.11 seconds. Both immutable baseline artifacts below were generated
after that prerequisite fix and before V1 production activation.

## Commands

```bash
PIGMENT_DB_STARTUP_OUTPUT=specs/004-recover-truncated-wal/benchmarks/startup-baseline.csv \
  cargo test --release --test truncated_wal \
  performance::complete_startup_million_operations_baseline -- \
  --ignored --exact --nocapture --test-threads=1

PIGMENT_DB_BENCHMARK_OUTPUT=specs/004-recover-truncated-wal/benchmarks/baseline.csv \
  cargo test --release --test truncated_wal \
  performance::issue3::paired_baseline -- \
  --ignored --exact --nocapture --test-threads=1
```

The commands were supervised by `systemd-run --user` only to survive desktop-app
restarts and apply the recorded memory/swap containment; the executed Cargo/test
arguments and workload were unchanged.

## Results

### Steady state

- Raw artifact: `baseline.csv`
- SHA-256: `d756820b4e863de7ce45ef61e31e7d13d7d16d9119bfe89709c15dbb26650733`
- Completed cells: 36/36 (18 one-worker, 18 eight-worker)
- Median-throughput range: 8,297.934–1,865,070.035 operations/second
- Test result: PASS in 94.02 seconds

Every cell remains independently authoritative; the range above is descriptive
and must not be used to average away a failing candidate ratio.

### Complete-history startup

- Raw artifact: `startup-baseline.csv`
- SHA-256: `2e1ea73db8f2b11148a8a5f35c2be89f5b1891372f5382f8e3f3db91a0ee1db4`
- History size: 1,000,000 accepted operations
- Completed samples: 11/11
- Median: 8,044,284,168 ns (8.044284168 s)
- Minimum: 7,849,767,349 ns
- p95/max (nearest-rank over 11): 9,299,454,493 ns
- Mean: 8,244,492,374 ns
- Corrected-run observed memory peak: approximately 1.08 GB
- Test result: PASS in 102.31 seconds
