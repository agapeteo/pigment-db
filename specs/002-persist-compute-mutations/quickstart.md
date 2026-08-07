# Quickstart: Validate Compute Mutation Persistence

## Prerequisites

- Rust toolchain compatible with the crate's 2021 edition.
- Repository root as the working directory.
- Feature 001 recovery implementation and frozen fixtures present.
- Design references: [data-model.md](data-model.md) and
  [compute-persistence-api.md](contracts/compute-persistence-api.md).

## RED-GREEN workflow

Implement one behavior at a time:

1. Add one behavior-focused test.
2. Run only that test and confirm it fails for the expected missing behavior
   (RED).
3. Add the minimum production change needed for that behavior.
4. Re-run the targeted test and confirm it passes (GREEN).
5. Run the accumulated feature tests.
6. Refactor only while GREEN, then run compatibility and full-suite checks.

Do not write the complete matrix before production work. Each subsection below
is one RED-GREEN cycle unless it explicitly says to repeat the cycle per case.
Every successful SC-001/SC-002 case must finish by using one shared helper to
drop and reopen the file-backed store three consecutive times, asserting exact
logical contents and outer-key presence after each cycle.

## Validation order

### 1. Additive fallible API contract

Compile one set `try_compute` call returning `std::io::Result<()>` while keeping
the existing `compute` call unit-typed. Add only that fallible method and wrapper
delegation, then repeat one method at a time for the remaining six pairs.

```bash
cargo test --test compute_persistence contract::fallible_and_compatibility_signatures -- --exact --nocapture
```

### 2. Atomic WAL batch rejection

Using the private deterministic fault writer, test one checkpoint at a time:

- partial `write_all` rejection restores the exact prior bytes and offset;
- `flush` rejection restores the exact prior bytes and offset;
- a successful batch advances the offset only once and flushes once.

```bash
cargo test wal::tests::compute_batch_write_failure_restores_prefix -- --exact --nocapture
cargo test wal::tests::compute_batch_flush_failure_restores_prefix -- --exact --nocapture
```

Confirm each test is RED before implementing its checkpoint or rollback path.

Validation on 2026-08-05: all three success/write-rejection/flush-rejection
checkpoints pass. The RED failures were respectively missing batch APIs, a
five-byte torn suffix, and a complete but unflushed suffix.

### 3. Set compute on a present key

- Start with multiple members and another independent outer key.
- Call fallible synchronous compute to add one member and remove another.
- Assert immediate state, then use the shared three-reopen helper to assert exact
  membership plus other-key isolation after every cycle.
- Confirm the compatibility wrapper produces the same successful result.

```bash
cargo test --test compute_persistence key_set::try_compute_persists_mixed_delta -- --exact --nocapture
```

### 4. Set absent and conditional variants

Repeat RED-GREEN separately for:

- unconditional compute creating a non-empty absent key;
- `try_compute_if_present` matching and skipped cases;
- `try_compute_if_absent` matching and skipped cases;
- callback counts of one when eligible and zero when skipped.

Every successfully invoked result must pass the shared three-reopen assertion;
every skipped call returns `Ok(())` and leaves the WAL and live state unchanged.

### 5. Asynchronous set compute

- Use `try_compute_async` with a ready callback performing a mixed change.
- Assert exactly one invocation and `Ok(())`.
- Compare immediate state and all three consecutive reopened states.
- Confirm `compute_async` remains a unit-returning compatibility wrapper.

Use a small test-only standard-library executor. Do not add an async runtime or
change lock-across-await behavior in this feature.

### 6. Sorted-map compute on a present key

- Begin with multiple ordered entries and another outer key.
- In one fallible callback, insert one search key, replace one value, remove one
  entry, and preserve another.
- Assert immediate ordering and values, then compare exactly after each of three
  consecutive reopenings.

```bash
cargo test --test compute_persistence key_map::try_compute_persists_mixed_delta -- --exact --nocapture
```

### 7. Sorted-map conditional variants

Repeat RED-GREEN separately for matching and skipped
`try_compute_if_present`/`try_compute_if_absent` cases, then verify the existing
wrappers delegate with unchanged callback counts.

### 8. Store publication after persistence failure

For each store type, use a private writer fault after its original prefix:

- fallible compute returns `Err`;
- the callback runs exactly once;
- immediate reads retain the original collection;
- replaying the restored prefix yields the original collection;
- the compatibility wrapper panics and retains the same original state.

Repeat the RED-GREEN cycle first for write rejection and then for flush
rejection. Do not combine both failure tests before implementing either.

### 9. Empty and no-op outcomes

Repeat RED-GREEN separately for both store types:

- present to empty removes the outer key immediately and after reopening;
- absent to empty creates no phantom outer key or WAL frame;
- exact no-op writes nothing;
- duplicate set insertion and remove-then-reinsert write nothing;
- unchanged map value writes nothing;
- empty binary values remain distinct from missing entries.

After every successful outcome above, reopen three consecutive times and verify
identical key presence and contents after each cycle.

### 10. Deterministic history matrix

Generate 100 reproducible multi-item histories per store without a new random
dependency. Include additions, map replacements, removals, no-ops, empty
results, and another outer key. Compare the expected model with immediate state
and each of three consecutive reopened states after every history.

Result on 2026-08-05: set histories 100/100 passed and sorted-map histories
100/100 passed; each history matched immediately and after all three reopen
cycles, including the unrelated outer key.

### 11. Compatibility regression

Frozen fixtures must remain unchanged and readable. Run feature 001's public and
internal recovery suites:

```bash
cargo test --test recovery -- --test-threads=1
cargo test wal::recovery::tests -- --test-threads=1
```

Pre-change result on 2026-08-05: 18/18 public recovery tests and 4/4 internal
recovery tests passed. Frozen fixture SHA-256 values were unchanged:

- `kv.wal.dat`: `e48dee8c4a07db010778d08037ac96a6cd16ca5fb323ea40145bd1fa36cb75f2`
- `set.wal.dat`: `d81d058ae3eabff04e08a8f12cad339e223f05f1fe532c82766b0565611cb653`
- `map.wal.dat`: `4612530c4b7b95ef8cb557c0306b2f11a5598a053b31dffec9f9aedb9477e84e`

### 12. Performance report

Capture the pre-feature non-durable compute baseline before production changes.
After implementation, use the identical setup-excluded workload with at least
11 release-mode samples. Report medians for the corrected fallible durable
compute path, equivalent existing durable operations, and the pre-feature
baseline for all six store/profile combinations:

| Profile | Set workload | Sorted-map workload |
|---|---|---|
| Sparse | Add one member to 10,000 | Replace one value among 10,000 |
| Mixed | Remove 500, add 500 | Remove 250, add 250, replace 500 |
| Full | Replace all 10,000 members | Replace all 10,000 entries |

```bash
cargo test --release --test compute_persistence performance::compute_10k_medians -- --exact --ignored --nocapture --test-threads=1
```

Record all medians. There is no ratio pass/fail threshold.

### Pre-feature benchmark (2026-08-05)

Release-mode, 11-sample medians on the implementation host; setup and result
inspection are excluded from each timed region. Times are informational only.

| Profile | Set non-durable | Set equivalent operations | Map non-durable | Map equivalent operations |
|---|---:|---:|---:|---:|
| Sparse | 294 ns | 683 ns | 2.193 us | 1.762 us |
| Mixed | 79.107 us | 383.661 us | 282.345 us | 738.432 us |
| Full | 1.269861 ms | 8.041252 ms | 3.232415 ms | 10.293219 ms |

### Final benchmark (2026-08-05)

The identical release workload was repeated after implementation with 11
setup-excluded samples per cell. No ratio threshold is applied.

| Profile | Set corrected durable | Set equivalent operations | Set pre-feature | Map corrected durable | Map equivalent operations | Map pre-feature |
|---|---:|---:|---:|---:|---:|---:|
| Sparse | 3.609964 ms | 606 ns | 246 ns | 9.460786 ms | 1.664 us | 1.088 us |
| Mixed | 3.766394 ms | 395.627 us | 86.007 us | 9.686064 ms | 746.052 us | 297.746 us |
| Full | 17.120105 ms | 8.31082 ms | 1.274433 ms | 21.853062 ms | 7.610477 ms | 3.326771 ms |

## Feature-level commands

```bash
cargo test --test compute_persistence -- --test-threads=1
cargo test --test recovery -- --test-threads=1
cargo test wal::recovery::tests -- --test-threads=1
cargo test --all-targets --all-features -- --test-threads=1
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Record pre-existing Clippy failures separately; the feature must introduce no
new diagnostic.

### Final validation (2026-08-05)

- `compute_persistence`: 22 passed, 1 ignored release benchmark.
- Public recovery target: 18 passed.
- Internal recovery group: 4 passed.
- Full all-target/all-feature suite: 75 passed, 4 intentionally ignored.
- `cargo fmt --check`: passed.
- Frozen fixture SHA-256 values: identical to the pre-change values above.
- Strict Clippy: feature-introduced diagnostics were corrected. The remaining
  baseline diagnostics are confined to existing key-value APIs/implementation
  (`result_unit_err`, `implicit_saturating_sub`, `needless_borrow`), existing
  `SearchKey` ordering (`non_canonical_partial_ord_impl`), legacy WAL open/write/
  parsing style (`ineffective_open_options`, `let_unit_value`, `question_mark`),
  and legacy unit-test assertion/ownership/formatting style.

Audit result: every successful SC-001/SC-002 integration case that accepts a
persistent mutation calls the shared three-consecutive-reopen assertion; fault
rejection cases instead compare the restored prefix and unpublished live state.

## Cross-platform validation

Run the compute-persistence integration target and unit fault checkpoints on
Linux, macOS, and Windows in the existing recovery workflow. Each file-backed
store must use its own temporary directory and be dropped before reopening.

## Expected result

- Seven fallible methods return `std::io::Result<()>`; seven existing methods
  retain their signatures and panic-on-persistence-error behavior.
- Every successful acceptance result is identical immediately and after each of
  three consecutive reopenings.
- Rejected compute writes restore the prior WAL prefix and publish no live
  callback state.
- Empty results leave no outer key; skipped and no-op results write nothing.
- One hundred histories per store and three restart cycles have zero mismatch.
- Frozen fixtures and feature 001 recovery remain green.
- The fixed benchmark prints every required median without enforcing a ratio.
