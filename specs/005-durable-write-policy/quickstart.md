# Quickstart: Implementing Explicit Durable Write Acknowledgements

This is the execution order for `/speckit-tasks` and `/speckit-implement`. Commands
shown for new tests are intended targets; they become runnable as their RED slices
are created.

## 1. Establish Provenance and Baselines

Before editing production code:

```bash
git status --short --branch
git rev-parse HEAD
rustc --version --verbose
cargo test --all-targets
```

Choose an explicit quiet, real-filesystem benchmark directory outside the
repository's source tree. Comparator captures are write-once; never overwrite
them after production edits.

### Protocol-v5 paired complete capture (design approved 2026-08-07)

- Commit: `6d7edc7c29a60a94c59effeeb2b78d8b95038135`
- Toolchain: `rustc 1.97.0 (2d8144b78 2026-07-07)`, target
  `x86_64-unknown-linux-gnu`
- Benchmark filesystem: `/tmp/pigment-db-durability-bench` on ext4
- CPU affinity: the paired release process is invoked through
  `taskset -c 12-19`; CPUs 12–19 were verified as distinct core IDs
  6–13, each with a 3,800 MHz advertised maximum and no SMT sibling in the set
- Linked implementations: the pre-feature crate and candidate crate have distinct
  package names but execute inside the same release process
- Buffered matrix: all 36 pre-feature/candidate comparisons use start-only
  scheduling
- Physical matrix: all 18 append-plus-barrier/physical-candidate comparisons use
  per-operation scheduling
- Pairing: 5 warmup and 11 measured AB/BA pairs per comparison, alternating
  comparator/candidate then candidate/comparator
- `cargo fmt --all -- --check`: GREEN
- `cargo test --all-targets`: GREEN
- `cargo test --doc`: GREEN

Protocol v5 does not reuse protocol-v1 through protocol-v4 or T263 performance
values. Those files remain immutable historical/diagnostic evidence. Capture all
54 comparisons, 1,188 measured rows, in one process and one write-once file;
never concatenate, average, or selectively regenerate samples.

The clean worktree initially could not resolve the declared migration binary
because a broad ignore pattern had omitted `src/bin/pigment-db-migrate.rs`; the
tracked entry point and root-only `/bin/` ignore were restored before testing. Two
existing fresh-publication tests then reproduced a parallel-only RED caused by
one global cleanup-fault guard clearing another test's fault. The minimum
test-only GREEN changed the registry to path-scoped guarded entries; the focused,
unit, and all-target suites passed afterward. No issue #5 production durability
behavior existed during either comparator capture.

### Preserved failed protocol history

Protocol v1's physical/reference comparison was invalid because start-only
scheduling let the minimal reference monopolize its mutex. Protocol v2 fixed all
18 physical cells, but its per-operation barrier added unstable scheduler-tail
noise to microsecond buffered p95: 34/36 buffered cells passed and an isolated
unchanged path crossed the threshold in both directions. All raw comparator and
candidate captures remain immutable under `benchmarks/attempts/`; neither failed
candidate is promotable.

The policy scheduler selector was introduced through RED–GREEN evidence:
buffered policy selects `StartOnly`, while physical and reference policies select
`PerOperation`. Protocol v3 then passed all physical cells but only 22 of 36
buffered cells against its old baseline. A contemporaneous clean pre-feature run
showed the old baseline itself had drifted while candidate-versus-pre-feature
ratios passed. Protocol v4 therefore fixes process affinity without retaining a
production optimization or changing any threshold. Its separate-process capture
passed 50 of 54 comparisons; all failures were high-throughput eight-worker vector
cells. T263 alternated those four pairs in one pinned process, and all aggregate
ratios passed unchanged thresholds. Protocol v5 extends that same-process AB/BA
control to the complete matrix.

## 2. Follow One RED-GREEN Cycle Per New Behavior

For every new behavior:

1. Add one behavior-focused runtime test.
2. Run only that test and confirm it fails for the expected behavioral reason.
3. Make the smallest production change that satisfies it.
4. Run the focused test and confirm GREEN.
5. Run the relevant module/integration suite before the next behavior.

Compilation failure, a placeholder assertion, or deliberately incorrect adapter
does not count as RED. Existing buffered and issue #4 recovery behavior is
characterization: its tests must be GREEN on first execution, recorded before the
related physical slice, and rerun afterward. Do not manufacture a RED for behavior
the implementation already provides.

Recommended slice order:

```bash
# Private configuration default and explicit selection
cargo test config::durability_tests -- --nocapture

# Existing buffered zero-barrier/zero-probe compatibility (first-run GREEN)
cargo test wal::durability_tests::buffered -- --nocapture

# Existing pending-callback async cancellation boundary (first-run GREEN)
cargo test wal::durability_tests::async_cancellation -- --nocapture

# Private memory rejection, one family and one construction path at a time
cargo test --test durable_write_policy contract::physical_memory_is_rejected -- --nocapture

# One direct physical barrier: single record, set group, then map group independently
cargo test wal::durability_tests::physical_single_acceptance -- --nocapture
cargo test wal::durability_tests::physical_set_acceptance -- --nocapture
cargo test wal::durability_tests::physical_map_acceptance -- --nocapture

# One original failure and rollback transition per slice
cargo test wal::durability_tests::write_rejection -- --nocapture
cargo test wal::durability_tests::flush_rejection -- --nocapture
cargo test wal::durability_tests::data_barrier_rejection -- --nocapture
cargo test wal::durability_tests::truncate_indeterminate -- --nocapture
cargo test wal::durability_tests::rollback_sync_indeterminate -- --nocapture

# Blocking barrier and publication visibility
cargo test wal::durability_tests::publication_order -- --nocapture
```

Keep public durability configuration, construction, and fallible mutation
adapters unexposed during these slices. Rerun buffered characterization after
each physical slice.

## 3. Implement Startup Capability and Publication Vertically

Keep physical construction private and use one RED-GREEN slice for each behavior
in this order:

1. compile-time unsupported target;
2. non-mutating active/recovery authority inspection;
3. parent-directory preflight success;
4. parent-directory preflight failure classified as
   `RequiredBarrierUnavailable`, with byte and namespace identity;
5. existing selected-file `sync_all` content preflight success/failure before
   stale-staging cleanup or repair;
6. missing-store parent preflight before staging creation;
7. missing-store staging create/write/flush/validate;
8. staging `sync_all` content-preflight failure, no authority, and deterministic
   cleanup or diagnosed non-authoritative staging;
9. fresh same-handle preparation, rename, and publication-directory barrier;
10. active-to-recovery rename and its authority directory barrier;
11. replacement-to-active rename and its publication directory barrier;
12. reopen/validation before cleanup;
13. cleanup removal and cleanup directory barrier;
14. recovery-authority replacement without an unnecessary pre-publication
    directory barrier; and
15. volatile/durable byte and namespace crash restoration at every checkpoint.

Every preflight open/synchronization failure is a support error regardless of
error kind. Every failure after both required preflights are successful is an
ordinary path-aware recovery I/O error.

Run the focused recovery contract after each slice:

```bash
cargo test --test durable_write_policy recovery:: -- --nocapture
```

Treat existing complete/incomplete issue #4 reopen behavior as first-execution-
GREEN conformance unless a concrete gap supplies a focused RED. Simulate power
loss at every new checkpoint by discarding volatile byte and namespace state and
reopening the restored model through normal initialization behavior.

Run the complete private capability, fresh, active-authority,
recovery-authority, cleanup, crash/reopen, and three-family matrix. This is the
public physical-construction exposure gate; there is no releasable capability-only
MVP.

### Direct-barrier buffered regression checkpoint (T044)

After the private direct-barrier slices, the buffered WAL characterization remained
GREEN: one write plus one flush for single- and multi-record commits, zero data/full
barriers, and zero durability preflights. Public buffered byte preservation,
exact-no-op callback counts, and no-options reopen behavior also remained GREEN for
all three store families. Commands:

```bash
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --lib wal::durability_tests::buffered_
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --test durable_write_policy compatibility::buffered_
```

### Durable rejection checkpoint (T065)

The accumulated 27-test private matrix is GREEN for write, flush, data-barrier,
truncate, rollback `sync_all`, typed rejection/indeterminate/failed-closed
classification, caller-owned barriers, and complete/incomplete/corrupt V1 reopen
classification:

```bash
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --lib wal::durability_tests
```

### Visibility and interruption checkpoint (T076)

The expanded 33-test private matrix is GREEN for blocked-barrier visibility,
post-acceptance set/map callbacks, deferred removal results, successful-barrier
interruption before publication, rejection unwind, and callback counts. The full
issue #4 truncation/recovery integration suite is also GREEN (29 passed, 6
release-only benchmarks ignored):

```bash
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --lib wal::durability_tests
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --test truncated_wal
```

### Private fallible-core checkpoint (T090)

All 39 private durability tests are GREEN after routing key/value, key/set, and
key/map simple, callback, numeric, compute, pop, and ordered mutations through
fallible cores. The existing panic/cancellation guard-release test is GREEN, a
normal library build succeeds, and generated public rustdoc contains none of the
private policy, support, mutation-failure, or core symbols:

```bash
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --lib wal::durability_tests
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --lib compute_panic_and_async_cancellation_preserve_state_and_progress
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo check --lib
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo doc --no-deps
```

## 4. Promote and Verify Public Adapters

Only after the private exposure gate is GREEN:

1. Promote the policy, support/failure types, and file/vector construction
   adapters one store family at a time. Each public construction contract is a
   first-execution-GREEN adapter over proven internals.
2. Promote one fallible mutator and its compatibility panic wrapper at a time.
   Verify returned values, callbacks, public reads, typed error classification,
   guard unwind, and retry behavior before the next method or family.
3. Confirm each physical call has exactly one direct barrier for its own complete
   logical mutation and no barrier shared across calls.

Then run public correctness and compatibility gates:

```bash
cargo test --test durable_write_policy -- --nocapture
cargo test --all-targets
cargo test --doc
```

Required public coverage includes all three store families, every mutator/result/
callback shape, single- and multi-record mutations, same- and different-shard
concurrency, pending-callback async cancellation with no WAL/barrier/publication
and prompt same-key guard release, post-callback non-yielding persistence,
complete/incomplete indeterminate reopen, legacy/V1 fixtures, and issue #1 through
#4 recovery regressions.

Platform CI expectations:

- Linux: physical file and directory capability success plus full physical suite.
- macOS: runtime-probed physical file and directory success plus full physical
  suite.
- Windows: buffered suite GREEN and explicit physical unsupported result; no
  downgrade and no physical success test.

### Physical capability, publication, and public-adapter checkpoint (T180–T236)

The private durability matrix is GREEN for unsupported targets; parent,
selected-file, and fresh-staging preflights; deterministic staging cleanup;
fresh publication; active-to-recovery backup publication; replacement
publication; recovery-authority publication; and cleanup uncertainty. Public
physical construction and fallible mutations are GREEN for key/value, key/set,
and key/map, followed by buffered no-options reopen. The accumulated unit suite
reported 251 passed and 9 release-only tests ignored; all public integration,
issue #1–#4 recovery, migration, compute, and ordering targets passed.

```bash
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --lib wal::durability_tests
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --test durable_write_policy public_
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --all-targets
```

## 5. Run Quality Gates

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo test --doc
```

All gates must be GREEN before final performance measurement.

The pre-capture quality gate is GREEN on 2026-08-07:

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo clippy --all-targets --all-features -- -D warnings
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --all-targets
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo test --doc
CARGO_TARGET_DIR=/tmp/pigment-db-005-target cargo doc --no-deps
git diff --check
```

## 6. Pause Before Complete Protocol-v5 Acceptance Capture

Stop and ask the user to confirm a new quiet-machine window. Do not begin the
benchmark until they explicitly approve this protocol-v5 capture window. Design
approval does not authorize timed capture. Before execution, verify that
`taskset -c 12-19` still selects the documented eight physical cores and that all
output paths are absent.

After quiet-machine approval, run the prepared paired release executable once:

```bash
PIGMENT_DB_V5_BENCH_ROOT=/tmp/pigment-db-durability-bench \
PIGMENT_DB_V5_CAPTURE_ID=protocol-v5-20260807-220443 \
PIGMENT_DB_V5_OUTPUT=/tmp/protocol-v5-20260807-220443.csv \
PIGMENT_DB_V5_BASELINE_ROOT=/tmp/pigment-db-005-protocol-v5-prefeature \
PIGMENT_DB_V5_CANDIDATE_ROOT=/work/@projects/penpack-projects/pigment-db-005-durable-write-policy \
taskset -c 12-19 \
  /tmp/pigment-db-005-protocol-v5-target/release/pigment-db-protocol-v5-runner
```

The runner keeps results in memory until all 54 comparisons finish and then
persists one write-once CSV under `/tmp`. Expect approximately 16 minutes. After
the command succeeds, require 1,189 lines/1,188 data rows, 54 unique comparisons,
11 AB/BA pair groups and both variants per comparison, exact alternating order,
one capture ID, and zero failed operations; then compute SHA-256. Copy the file
once to the absent
`specs/005-durable-write-policy/benchmarks/attempts/protocol-v5-20260807-220443.csv`
destination, prove identity with `cmp` plus matching SHA-256, and retain the staged
source through evaluation and final promotion.

The run captures 36 baseline/candidate and 18 reference/candidate pairs. Verify all
54 per-cell thresholds from [performance.md](contracts/performance.md). A
failed cell blocks acceptance: keep the complete failed attempt, diagnose one
focused behavior without weakening correctness or thresholds, rerun every
quality gate, ask the user for a new quiet-machine window, and recapture the
complete paired matrix under a new ID. Never merge samples across attempts.

Only after one complete attempt passes may its paired CSV be copied byte-for-byte
to `benchmarks/final.csv`. Generate `final.md` from that capture, include the
passing capture ID and every failed attempt, and
produce the specified 72-row report without averaging away or weakening any
threshold.

## 7. Final Protocol-v5 Result

Capture `protocol-v5-20260807-220443` completed in the user-approved quiet-machine
window and passed **54/54** independent comparisons: `36/36` buffered and `18/18`
physical. The staged, imported-attempt, and promoted `final.csv` copies are
byte-identical and share SHA-256
`d3292e8d2dc73f4185ed1ec917a29bc23bcc627238f7a8effa0ee0be5183016e`.

Validation confirmed one exact header plus 1,188 data rows, 54 unique cells, 594
AB/BA pair groups, exact counterbalanced order, both variants in every pair, and
zero failed operations. The lowest throughput ratio was `0.949319` against an
eight-worker floor of `0.85`; the highest p95 ratio was `1.065339` against the
`1.25` ceiling. No threshold was weakened and no prior samples were reused.

After promotion, the complete quality sequence was rerun from fresh target
`/tmp/pigment-db-005-final-target-20260807-1749`:

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/tmp/pigment-db-005-final-target-20260807-1749 \
  cargo clippy --all-targets --all-features -- -D warnings
CARGO_TARGET_DIR=/tmp/pigment-db-005-final-target-20260807-1749 \
  cargo test --all-targets
CARGO_TARGET_DIR=/tmp/pigment-db-005-final-target-20260807-1749 cargo test --doc
CARGO_TARGET_DIR=/tmp/pigment-db-005-final-target-20260807-1749 cargo doc --no-deps
git diff --check
```

All commands passed. The library unit result was `254` passed and `9` ignored
release-only tests; every integration target, doc test, and rustdoc build passed.
The final scope audit found no Cargo dependency change, new unsafe block, production
mutation mutex, persisted-format change, group commit, silent downgrade, or
threshold change. Detailed evidence is in
`benchmarks/attempts/protocol-v5-20260807-220443.md`; the required 72-row report is
in `benchmarks/final.md`.
