# Compaction Performance Evidence

This directory holds the immutable inactive-compaction performance evidence for
feature 008. The benchmark measures ordinary mutations only; it does not measure
compaction speed.

## Protocol

Each complete matrix contains every combination of three store families, vector
and buffered file backing, ordinary write/successful remove/minimal successful
compute, and one/eight distinct-key workers. Every cell uses 32-byte inputs, five
warmups, eleven measured samples, and a minimum of both 100 ms and 1,024 public
operations per sample.

The final gate uses three complete pre-feature matrices and three complete
candidate matrices, captured as sequential counterbalanced pairs on the same
quiet pinned host. No observed cell may be dropped or selectively recaptured.

## Capture procedure

The harness was frozen before any production edit with this identity:

| Field | Frozen value |
|---|---|
| Pre-feature implementation commit | `a7c8281f72e25c177a142be99285faead7335e01` |
| Branch used to add test infrastructure | `codex/008-current-compaction-durability` |
| Harness source | `tests/mutation_ordering/performance.rs` |
| Harness SHA-256 | `c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c` |
| Pre-capture dirty-state listing SHA-256 | `cb72a95fc5d3cf49d1d6c1af6cd9c1181d6581ba3535001fb4dfe77107f7979c` |
| Rust/Cargo | `rustc 1.97.1 (8bab26f4f 2026-07-14)` / `cargo 1.97.1 (c980f4866 2026-06-30)` |
| Target | `x86_64-unknown-linux-gnu` |
| Data filesystem | `/dev/nvme0n1p4`, Btrfs project subvolume |
| Allowed CPUs / capture affinity | `0-21` / `12-19` |
| Temporary data root | `/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp` |

The dirty-state digest is the SHA-256 of
`git status --porcelain=v1 --untracked-files=all`; it identifies the complete
pre-production dirty path set. The harness digest is content-addressed
separately and is the normative benchmark-source identity.

The successful result-free smoke command was:

```text
TMPDIR=/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp PIGMENT_DB_COMPACTION_BENCHMARK_SMOKE=1 taskset -c 12-19 cargo test --release --test mutation_ordering performance::paired_baseline -- --exact --ignored --nocapture --test-threads=1
```

Smoke mode traverses all 36 feature cells once, writes no acceptance artifact,
and skips the unrelated retained-memory report. It is never valid acceptance
evidence.

Each immutable baseline matrix uses the following command, replacing `<N>` with
`1`, `2`, or `3` and setting the two frozen digests exactly as shown above:

```text
TMPDIR=/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp PIGMENT_DB_COMPACTION_CAPTURE_ID=baseline-<N> PIGMENT_DB_COMPACTION_DIRTY_SHA256=cb72a95fc5d3cf49d1d6c1af6cd9c1181d6581ba3535001fb4dfe77107f7979c PIGMENT_DB_COMPACTION_HARNESS_SHA256=c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c PIGMENT_DB_BENCHMARK_OUTPUT=/work/@projects/penpack-projects/pigment-db/specs/008-current-compaction-durability/benchmarks/baseline/baseline-<N>.csv taskset -c 12-19 cargo test --release --test mutation_ordering performance::paired_baseline -- --exact --ignored --nocapture --test-threads=1
```

To reconstruct an isolated baseline after production code changes:

1. Create a detached worktree at
   `/work/@projects/penpack-projects/pigment-db-008-baseline` from
   `a7c8281f72e25c177a142be99285faead7335e01`.
2. Copy only the frozen `tests/mutation_ordering/performance.rs` from this
   feature worktree into the same path in the detached worktree.
3. Verify its SHA-256 is
   `c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c`.
4. Create that worktree's ignored `target/compaction-benchmark-tmp` directory
   and run the exact release command from its root, changing only absolute root
   and output paths while retaining CPUs `12-19` and every protocol parameter.

This reconstruction keeps production code at the pre-feature commit while
using byte-identical test infrastructure. No specification, integration-test,
or candidate production file is copied.

## Platform prerequisites

- Final performance acceptance requires this Linux host to be quiet, CPUs
  `12-19` available for `taskset`, the recorded Btrfs data placement, the
  recorded Rust/Cargo toolchain, and no concurrent baseline/candidate runs.
- Real Windows physical-durability evidence requires a Windows worker with an
  NTFS same-directory test location. Linux cross-compilation validates the
  target-specific source boundary but cannot execute `MoveFileExW`, sharing
  violations, or write-through namespace publication.
- Linux/macOS compatibility jobs retain their existing rename plus directory
  synchronization behavior and must be green in CI before release.

## Preparation evidence (2026-08-20)

The three immutable quiet-host baseline captures remain available in
[baseline.md](./baseline.md) and
[`baseline/`](./baseline/). Their source commit, dirty-state digest, harness
digest, toolchain, filesystem, affinity, row counts, and artifact checksums all
match the frozen protocol above.

The pre-feature implementation was reconstructed at
`/work/@projects/penpack-projects/pigment-db-008-baseline` from commit
`a7c8281f72e25c177a142be99285faead7335e01`. Only the frozen performance
harness was copied into that worktree. Both copies hashed to
`c8ca6c94e6f38d54e456462bce2e8fad0d3cffa2b2457f4c2818129b1c62006c`.
Baseline and candidate release binaries each completed a result-free 36-cell
smoke traversal with the same CPUs, temporary directory, filesystem, payload,
workload, and toolchain.

From the reconstructed baseline worktree:

```text
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/work/@projects/penpack-projects/pigment-db/target/008-baseline-build TMPDIR=/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp PIGMENT_DB_COMPACTION_BENCHMARK_SMOKE=1 taskset -c 12-19 cargo test --release --test mutation_ordering performance::paired_baseline -- --exact --ignored --nocapture --test-threads=1
```

From the candidate worktree:

```text
TMPDIR=/work/@projects/penpack-projects/pigment-db/target/compaction-benchmark-tmp PIGMENT_DB_COMPACTION_BENCHMARK_SMOKE=1 taskset -c 12-19 cargo test --release --test mutation_ordering performance::paired_baseline -- --exact --ignored --nocapture --test-threads=1
```

A non-acceptance diagnostic compared a newly reconstructed pre-feature matrix
with the candidate under the current machine state. The reconstructed baseline
itself was substantially slower than the immutable quiet baseline in several
one-worker cells, proving that direct comparison with the earlier quiet capture
was invalid for development diagnosis. Deterministic tests found no structural
failure: every mutation still holds the maintenance gate through WAL acceptance
and live publication, reads bypass it, inactive delta recording builds no
payload, and staging encode/validation occurs outside exclusive maintenance.

Two speculative optimizations were rejected rather than carried into the
candidate. Bypassing maintenance for vector stores violated the all-mutation
gate invariant and failed three deterministic ordering tests. A custom atomic
reader gate first exposed a lost-wakeup deadlock and, after that defect was
corrected, was materially slower than the standard-library `RwLock`. The
attempt-1 candidate therefore retained the simpler, fully GREEN standard gate.
Diagnostic measurements are not acceptance evidence; only the approved
counterbalanced quiet-host matrices may decide the thresholds.

## Attempt-1 failure and retry preparation (2026-08-21)

The first complete candidate capture preserved all three matrices but failed
six one-worker throughput cells. All eight-worker throughput and median p95
cells passed, identifying fixed uncontended gate cost rather than contention
collapse. The full result and checksums are recorded in `candidate.md` and
`final.md`.

The RED–GREEN retry narrows the coordination invariant to stores that can
actually perform file maintenance. Vector-backed stores expose neither storage
inspection nor online compaction and now bypass the maintenance gate. The
existing all-mutation coordination tests were moved to file-backed stores, and
a new deterministic test proves vector mutations progress while a file-only
maintenance probe is held. The file-backed gate now uses `parking_lot::RwLock`;
the WAL retains its original standard-library lock after a diagnostic userspace
WAL-lock experiment was rejected for worse eight-worker p95 latency.

After the optimization, the complete debug and release suites, formatting,
Clippy with warnings denied, rustdoc with warnings denied, Windows GNU
cross-check, and byte-identical baseline/candidate 36-cell smoke traversals all
passed. Diagnostic timing runs were not accepted because host load rose to
approximately 7 and results varied materially by run order. A new acceptance
capture requires a new committed candidate and fresh quiet-host confirmation.

## Artifact policy

- `baseline/` contains immutable raw pre-feature CSV and metadata.
- `candidate/` contains immutable raw candidate CSV and metadata.
- `baseline.md` and `candidate.md` summarize provenance and capture validity.
- `final.md` pairs every cell, reports absolute values and ratios, and records
  the inclusive threshold verdict.

An incomplete matrix, invalid sample, harness mismatch, environment mismatch, or
missing checksum invalidates a complete capture; it never counts as a pass.
