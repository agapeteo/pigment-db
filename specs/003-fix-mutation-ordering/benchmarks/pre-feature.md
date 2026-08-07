# Pre-Feature Mutation Ordering Baseline

Captured before any issue #3 release mutation-path change.

## Environment

- Timestamp: `2026-08-06T04:01:48-05:00`
- Git commit: `fae6d38e70fb8bd1aff2841243eda8811ab26cc5`
- Binary tracked working-diff SHA-256: `3f1045674f66f360dda5bccef93e607f11b734d8034e87e5f89fe955a6db7b9f`
- Cargo source/test tree SHA-256: `d410743cf54e6853497be563f54ce5768379286c4ade2d9ad71befdcfbd4faa4`
- Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`, LLVM 22.1.6, `x86_64-unknown-linux-gnu`
- Kernel: `Linux 7.0.11-76070011-generic x86_64`
- CPU: `Intel(R) Core(TM) Ultra 7 155H`, 22 logical CPUs, 16 cores, 2 threads/core, 400–4800 MHz
- Repository filesystem: Btrfs on `/dev/nvme0n1p4`
- Temporary file filesystem: ext4 on `/dev/nvme0n1p3`
- `TMPDIR`: `/home/emix/.local/state/codex-desktop/tmp`
- Release overrides: `RUSTFLAGS` empty; `CARGO_PROFILE_RELEASE_LTO` empty

`git diff --binary` does not include untracked feature #1/#2 files, so the
additional source/test tree digest fingerprints every file Cargo compiles.

## Validation

- Benchmark test: 1 passed, 0 failed.
- Matrix: 36 rows, 36 unique `(store, mode, profile, workers)` keys.
- Warmups: 5; measured samples: 11 per cell.
- Memory rows: 2; both completed with zero retained live keys.
- Raw performance data: [pre-feature.csv](pre-feature.csv)
- Raw memory data: [pre-feature-memory.csv](pre-feature-memory.csv)

The million-cycle RSS delta includes the existing vector-backed WAL and
allocator retention. It is a paired baseline, not an assertion that all of the
reported bytes are ordering state.
