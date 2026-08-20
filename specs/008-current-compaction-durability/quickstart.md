# Validation Quickstart: Current-Format Compaction and Windows Physical Durability

This guide is for implementing and validating the feature. It does not replace the behavior contracts.

## 1. Preserve a baseline before hot-path edits

Freeze the feature-specific performance harness and capture the current implementation before adding the maintenance coordinator to store structs. Record the exact commit/dirty digest and environment described in [contracts/performance.md](./contracts/performance.md). Diagnostic captures may run during development; reserve the final acceptance capture for a quiet pinned machine.

## 2. Work one RED–GREEN behavior at a time

For each inspection, publication, recovery, online interleaving, or Windows behavior:

1. Add one behavior-focused test.
2. Run only that test and record the expected failure (RED).
3. Implement the smallest production change.
4. Run the focused test and record success (GREEN).
5. Run the relevant family/fault module before refactoring.

Tests should use deterministic maintenance checkpoints, durability fault injection, and watchdogs. Do not treat a sleep-only concurrency test as evidence.

## 3. Recommended validation slices

### Inspection

Validate empty, active-only, every family, mixed families, multiple contiguous sealed segments, safe terminal tail, older recognized data, malformed/missing/wrong-family/corrupt artifacts, any unexpected directory entry, ambiguous authority, checked totals, and a byte-for-byte before/after directory snapshot proving no writes.

### Closed compaction and recovery

For every family and mixed directories, prove one active current-V2 segment, exact state/timestamp metadata, source-change rejection, repeated compaction, cleanup pending, and three reopenings. Inject interruption at every staging, manifest, namespace, reopen, and cleanup boundary under buffered and physical policies.

### Online compaction

Use scheduling hooks to pause capture, encoding, validation, cutover, and cleanup. Prove read/write progress where promised; same/distinct-key and compute ordering; rejected/no-op/rollback/cancel exclusion; exact and exceeded delta bounds; immediate second-attempt rejection; writer/rotation handoff; panic cleanup; pending cleanup with continued writes; and failed-closed indeterminate publication.

### Windows

On Windows CI, replace unsupported-policy assertions with successful physical construction/open/mutation coverage. Exercise no-replace and replacement write-through moves, preflight failures, sharing violations, content faults, rotation/recovery/compaction, Unicode, supported long paths, and buffered compatibility.

### Compatibility

Hash frozen migration fixtures before and after testing. Runtime open/inspection/compaction must return migration guidance without conversion; the external migration tool's established results remain unchanged.

The pre-production file-byte SHA-256 values frozen on 2026-08-20 are:

| Fixture | SHA-256 |
|---|---|
| `tests/fixtures/legacy/kv.wal.dat` | `e48dee8c4a07db010778d08037ac96a6cd16ca5fb323ea40145bd1fa36cb75f2` |
| `tests/fixtures/legacy/set.wal.dat` | `d81d058ae3eabff04e08a8f12cad339e223f05f1fe532c82766b0565611cb653` |
| `tests/fixtures/legacy/map.wal.dat` | `4612530c4b7b95ef8cb557c0306b2f11a5598a053b31dffec9f9aedb9477e84e` |
| `tests/fixtures/i128_key/legacy-map.hex` | `513710051c8d10925dbd3cadb44988e2d6626dbc9a0ad8ed44e4880fdf2d1829` |
| `tests/fixtures/i128_key/v1-map.hex` | `9685830d06416c5240f9ef31136bfe6c504d5a4851f26d742ef8b8bf8a22b725` |
| `tests/fixtures/i128_key/earlier-v2-map.hex` | `2010b57e0320a26212aa174e16939598e001abb4911ffba3e964657b95089e04` |

## 4. Focused commands during development

Use the test targets introduced by implementation tasks, for example:

```text
cargo test --test maintenance_api -- --test-threads=1
cargo test --test storage_inspection -- --test-threads=1
cargo test --test closed_compaction -- --test-threads=1
cargo test --test compaction_recovery -- --test-threads=1
cargo test --test online_compaction -- --test-threads=1
cargo test --test migration_compatibility -- --test-threads=1
```

Run Windows-only physical tests on a Windows worker; Linux cannot establish the real Win32 namespace evidence.

## 5. Performance acceptance

Run the ignored release benchmark using the harness's documented invocation, CPU affinity, and CSV output. Capture three complete baseline/candidate matrices and evaluate every cell against:

- one-worker throughput at least 90% of baseline;
- eight-worker distinct-key throughput at least 85% of baseline;
- p95 latency no more than 125% of baseline.

Include absolute operations/writes per second, not only ratios, in the final report.

## 6. Required completion gates

Run exactly:

```text
cargo test --all-targets --all-features -- --test-threads=1
cargo test --release --all-targets --all-features -- --test-threads=1
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

Also require the Windows CI matrix, all crash/fault matrices, fixture hashes, unsafe-boundary check, and the performance gate. Completion evidence should link raw benchmark artifacts and identify any cleanup-pending scenario that was deliberately injected and then converged.

### Pre-production GREEN checkpoint (2026-08-20)

Before any library production behavior changed, the following all completed
without failures on commit `a7c8281f72e25c177a142be99285faead7335e01` plus
feature-only test/specification scaffolding:

- `cargo test --all-targets --all-features -- --test-threads=1`;
- `cargo fmt --all -- --check`;
- `cargo test --test migration_cli --test i128_key -- --test-threads=1`;
- all six frozen fixture file-byte SHA-256 checks listed above.

Release-only and historical performance tests remained intentionally ignored.
The feature matrix was exercised separately in result-free smoke mode.

### Foundational infrastructure GREEN checkpoint (2026-08-20)

After T009–T017, `cargo test --all-targets --all-features --
--test-threads=1` completed with zero failures. This checkpoint includes the
maintenance scheduling, durability namespace, current-V2 fixture, subprocess
crash, lock-rank/watchdog, recovery, durability, mutation-ordering, migration,
and 104-row requirement-manifest tests. Normal builds still expose no
maintenance API or test hook.

### Inspection arithmetic characterization (2026-08-20)

The T033 synthetic family/directory overflow characterization was
first-execution GREEN. Checked additions had already been introduced while
implementing the earlier exact-byte inspection slices, so the implementation
retained the GREEN behavior and extracted it into focused helpers rather than
deliberately regressing arithmetic to manufacture a RED result.

### User Story 1 storage-inspection GREEN checkpoint (2026-08-20)

SC-001 is GREEN. Empty, active-only, segmented, mixed-family, recoverable-tail,
legacy, corrupt, malformed, missing-segment, wrong-family, unexpected-entry,
ambiguous-generation, and open-family fixtures all returned the specified
exact statistics or structured classification. Every public fixture compared
native `OsString`-preserving relative names, file/directory type, length,
permissions, modification metadata, and file bytes before and after inspection.

The checkpoint completed with zero failures:

- `cargo test --lib compaction::inspection -- --test-threads=1` (9 passed);
- `cargo test --test maintenance_api --test storage_inspection -- --test-threads=1` (9 passed);
- `cargo test --doc -- --test-threads=1` (3 compile-fail specialization checks passed);
- `cargo fmt --all -- --check`;
- `cargo clippy --all-targets --all-features -- -D warnings`.

### User Story 2 closed-compaction GREEN checkpoint (2026-08-20)

SC-002 is GREEN. Public buffered and supported physical compaction reduced
active-only, segmented, recoverable-tail, and mixed key/value, key/set, and
key/map directories to exactly one active V2 segment per family. Exact logical
state, family identity, timestamp granularity, and the last accepted timestamp
bucket remained stable through three consecutive reopenings. Repeated
compaction was byte-idempotent for unchanged active data, and a deferred
`CleanupPending` generation converged on the next explicit compaction.

SC-003 is GREEN. The deterministic fault model covered 13 staging,
manifest, move, reopen, phase-rewrite, and cleanup cuts for all three families
under buffered process-interruption and physical power-loss semantics. The
subprocess matrix then terminated real buffered compaction at 20 exact
phase/cut points for every family (60 child processes). Each public initializer
either recovered exact old/new state or returned a structured
`AuthorityUndetermined`/`InvalidArtifact` result without changing any evidence;
no cut removed the last complete authority.

The checkpoint completed with zero failures:

- `cargo test --lib compaction:: -- --test-threads=1` (38 passed, including the
  complete child-process matrix);
- `cargo test --test closed_compaction -- --test-threads=1` (10 passed);
- `cargo test --test recovery --test migration_cli -- --test-threads=1` (33
  passed);
- `cargo test --all-targets --all-features -- --test-threads=1` (all debug
  library, binary, integration, crash, durability-model, compatibility, and
  documentation test targets passed; release-only benchmarks remained
  intentionally ignored);
- `cargo fmt --all -- --check`;
- `cargo clippy --all-targets --all-features -- -D warnings`.

## 7. Expected caller usage

Callers inspect and choose when to compact; Pigment DB schedules nothing. Closed callers drop every same-process store before `compact_directory_in_place`. Online callers invoke `try_compact_online` on exactly one file-backed store and may choose a delta bound; policy is inherited. `CleanupStatus::Pending` means replacement publication succeeded and ordinary use may continue, with cleanup retried at reopen or a later explicit compaction. `MigrationRequired` means the caller must run `pigment-db-migrate` separately.
