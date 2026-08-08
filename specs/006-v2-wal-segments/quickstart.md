# Quickstart: Validate V2 WAL Segments

Run from the repository root on branch `codex/009-v2-wal-segments`.

## 1. Focused V2 lifecycle

```bash
cargo test --test v2_wal_segments -- --test-threads=1
```

Expected: configuration, fresh V2 creation, rotation, oversized mutation, compute-group atomicity, interrupted rotation, active-tail repair, granularity inheritance, and >4 GiB handoff scenarios pass.

## 2. Recovery and corruption matrix

```bash
cargo test --test truncated_wal -- --test-threads=1
cargo test --test recovery -- --test-threads=1
```

Expected: every terminal cut recovers the exact accepted prefix, protected corruption fails closed, and repeated reopen state is stable.

## 3. Offline conversion

```bash
cargo test --test migration_cli -- --test-threads=1
```

Expected: legacy, V1, recoverable V1 tail, and segmented V2 inputs emit V2; source bytes remain unchanged and crash/failure artifacts follow the CLI contract.

## 4. Full quality gate

```bash
cargo fmt --all -- --check
cargo test --all-features -- --test-threads=1
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
```

Expected: zero failures and zero diagnostics treated as warnings.

## 5. Performance gate

Use the pre-feature commit `f5bf40e` as baseline and the current branch as candidate. Build both with the same release toolchain, pin each matched pair to the same CPU, alternate baseline/candidate order, and capture on a quiet machine.

Acceptance from [spec.md](spec.md): candidate median non-rotating write throughput is at least 90% of baseline and every matched pair is at least 85%.

The immutable final capture is `protocol-v5-20260808-124401`. It pins the coordinator to CPU 11 and worker `n` to CPU `12 + n`, verifies every affinity before timing, and retains five warmup plus eleven measured AB/BA pairs for all six family/worker cells.

Expected final evidence:

- all six candidate/baseline median ratios are at least `0.90`;
- all 66 same-pair ratios are at least `0.85`;
- the observed minimum pair ratio is `0.896`;
- the runner exits 0 with all 132 measured rows;
- raw CSV SHA-256 is `acf7b9354a7238a591f8a56edfb16859f834277d967d183dcad68c3bad47250a`.

See `benchmarks/results/protocol-v5-20260808-124401.md` for the decision and `benchmarks/attempts/protocol-v5-20260808-124401.md` for provenance. Protocols V1–V4 and Diagnostics V2–V3 remain preserved as immutable failed or diagnostic evidence.
