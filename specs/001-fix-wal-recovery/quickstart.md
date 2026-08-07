# Quickstart: Validate Crash-Safe WAL Recovery

## Prerequisites

- Rust toolchain compatible with the crate's 2021 edition.
- Repository root as the working directory.
- Feature design in [data-model.md](data-model.md) and [recovery-api.md](contracts/recovery-api.md).

## RED–GREEN workflow

Implement one observable recovery behavior at a time:

1. Add one test through a public initializer or read API.
2. Run only that test and confirm it fails for the expected recovery reason.
3. Add the minimum production behavior needed for the test.
4. Re-run the targeted test and confirm it passes.
5. Run the recovery test group, then the full relevant suite.
6. Refactor only while GREEN and rerun tests after each refactor.

Do not write the entire fault matrix before implementation. Each row below is a separate RED–GREEN tracer bullet.

## Recommended validation order

### 1. Public initialization contract

Validate in this order:

- New empty store returns `Normal`.
- Existing ordinary store returns `Normal` with identical logical contents.
- Existing `init_new(&str) -> Self` call sites remain valid.
- Fallible filesystem failures return structured errors rather than panicking.

Targeted command pattern:

```bash
cargo test <exact_test_name> -- --exact --nocapture
```

### 2. Legacy recovery tracer bullets

Use a fresh temporary directory for every case and preserve the current filenames:

- legacy recovery exists and active is missing;
- legacy recovery exists and active is zero-length;
- active is a frame-complete partial snapshot of legacy recovery;
- active completed the snapshot and contains later overwrite/delete operations;
- active and legacy candidates are genuinely ambiguous.

For successful cases, assert `Recovered` and compare public logical reads with the pre-interruption snapshot. For the conflict case, capture every recognized filename and byte sequence before initialization and assert exact equality afterward.

### 3. New publication interruption matrix

For each store type, interrupt independently after:

- artifact inspection;
- staging creation;
- first and middle staging records;
- staging validation;
- staging synchronization;
- publication rename;
- before cleanup;
- cleanup failure;
- cleanup success.

Reopen through `try_init_new`, then verify all acknowledged pre-startup data is present. A leftover staging artifact never wins over active.

### 4. Store-specific logical fixtures

Use mutation histories that detect ordering mistakes:

- Key/value: multiple keys, overwrite one, delete another, and retain an empty value.
- Key/set: duplicate append, member removal, multiple outer keys, and removal of a final member.
- Key/sorted-map: multiple search keys, overwrite one, remove another, and use multiple outer keys.
- Every store: intentionally empty active state.

Compare logical snapshots rather than compacted WAL byte order because map/set iteration order is not a compatibility contract.

### 5. Idempotence and compatibility

- Interrupt startup ten consecutive times at deterministic transition points, then allow one complete startup; assert the original logical state is fully recovered.
- After one successful recovery, perform three ordinary restarts; assert identical logical contents and `Normal` on the restarts after cleanup.
- Open frozen pre-feature fixtures for all three store types; do not generate compatibility fixtures with the new writer.
- If cleanup is forced to fail, assert the store remains usable, status is `Recovered`, and the compatibility initializer logs recovery.

## Feature-level commands

```bash
cargo test recovery -- --test-threads=1
cargo test --all-targets --all-features -- --test-threads=1
cargo fmt --check
```

Run Clippy as a diagnostic during implementation:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

The repository has pre-existing Clippy failures, so distinguish baseline warnings from feature-introduced warnings until the baseline is repaired.

## Cross-platform validation

Run the recovery group on Linux, macOS, and Windows before release. All staging files must share the active WAL's directory, and all mappings/file handles must be dropped before publication or cleanup. Platform tests must demonstrate that a completed replacement rename leaves either the old or new complete active state under the supported local filesystem; power-loss and portable directory synchronization are outside this feature.

## Expected result

- All interruption points recover 100% of acknowledged pre-startup logical data.
- Ambiguous authority returns a structured error without byte changes.
- Every store type reports the same `Normal`/`Recovered` semantics.
- Existing callers and pre-feature artifacts remain compatible.

## Validation record — 2026-08-05

Validation was run on Linux with the repository root as the working directory.

| Command | Result |
|---|---|
| `cargo test --test recovery -- --test-threads=1` | PASS: all 18 public recovery scenarios passed |
| `cargo test wal::recovery::tests -- --test-threads=1` | PASS: all 4 publication/classification fault tests passed |
| `cargo test recovery -- --test-threads=1` | PASS: 8 recovery-named tests passed across unit and integration targets |
| `cargo test --all-targets --all-features -- --test-threads=1` | PASS: 42 passed, 3 pre-existing ignored, 0 failed |
| `cargo fmt --check` | PASS |
| `cargo clippy --all-targets --all-features -- -D warnings` | Baseline-only failure; no feature-introduced diagnostic remains |

The public suite covers fresh and frozen active WALs, stale staging, all
publication checkpoints, ten interrupted-startup retries, legacy-only and
incomplete-active recovery, equal/history/prefix provenance, byte-preserving
conflicts, cleanup and required-I/O faults, all three store adapters, empty
states, and cross-store isolation. Frozen fixtures were read in place and were
not regenerated by the new writer.

Strict Clippy still reports 25 pre-existing diagnostics in code present before
this feature: three legacy key/value API/style diagnostics, one `SearchKey`
ordering implementation diagnostic, four legacy WAL implementation
diagnostics, ten existing key/set boolean-assert diagnostics, and seven
existing WAL-test ownership/formatting diagnostics. During this validation,
the new recovery modules initially reported naming, type-complexity, sorting,
and test-only dead-code diagnostics; those feature-introduced findings were
fixed before the final run.

The same two recovery commands are configured in
`.github/workflows/recovery.yml` for Linux, macOS, and Windows. Linux is
validated above; macOS and Windows execution will occur in CI because those
operating systems are not available in this workspace.
