# Implementation Plan: Persist Compute Mutations

**Branch**: `002-persist-compute-mutations` | **Date**: 2026-08-05 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-persist-compute-mutations/spec.md`

## Summary

Persist all set and sorted-map callback mutations through a failure-atomic,
existing-format WAL batch before publishing live state. Add seven fallible
`try_compute*` methods returning `std::io::Result<()>`; retain the seven existing
methods as source-compatible unit-returning wrappers that panic on persistence
error. Each eligible callback runs once on a guarded working copy, deterministic
net actions are prepared in memory, and a rejected write/flush truncates back to
the pre-commit WAL checkpoint. Empty results delete the outer key; absent-empty,
skipped, and no-op outcomes write nothing.

## Technical Context

**Language/Version**: Rust 2021 edition; current validation toolchain Rust 1.97.0; crate declares no MSRV

**Primary Dependencies**: Existing `dashmap` 3.11.10 guards, `bincode` 1.3.3 encoding, `crc32fast`, `log`, standard collections and I/O; development-only `tempfile` 3.23.0; no new dependency

**Storage**: Existing local per-store WAL files and action grammar from feature 001; private writer checkpoint/rollback function added to WAL state; no new action identifier or migration

**Testing**: `cargo test`; public file-backed `compute_persistence` integration target with one shared three-consecutive-reopen assertion applied to every successful SC-001/SC-002 case; private deterministic write/flush fault injection; replay of restored prefixes; recovery/frozen-fixture regressions; deterministic 100-history models; standard-library async executor; ignored release-mode median report; formatting and Clippy diagnostics

**Target Platform**: Rust library on Linux, macOS, and Windows; one process per store directory under current ownership assumptions

**Project Type**: Single Rust library crate

**Performance Goals**: O(d) encoded actions for logical difference d; one WAL lock, `write_all`, and flush per non-empty compute delta; report at least 11 setup-excluded medians for sparse, mixed, and full 10,000-item profiles against equivalent durable operations and the pre-feature baseline; no ratio gate

**Constraints**: Preserve every existing method signature and callback condition; add exactly seven fallible counterparts; invoke eligible callbacks once; retain existing WAL grammar and acknowledgement policy; restore the prior prefix after rejected compute writes when rollback remains operational; stable present outer keys are non-empty; no feature-introduced Clippy diagnostics

**Scale/Scope**: Four set and three sorted-map operation pairs; collections of 10,000 items for performance reporting; 100 histories per store; three consecutive reopen checks after every successful acceptance-matrix result; compute-specific rollback included; general ordering, ordinary-mutation partial recovery, stronger synchronization, and async lock duration remain separate issues

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

### Pre-research gate

- **Project constitution**: PASS — `.specify/memory/constitution.md` is an unratified placeholder with no enforceable project principle.
- **RED-GREEN TDD**: PASS — root `AGENTS.md` requires one behavior-focused failing test before each production change. Delivery is split into method, failure checkpoint, and outcome tracer bullets.
- **Source compatibility**: PASS — existing seven signatures remain unchanged; the seven `try_compute*` methods are additive.
- **Format compatibility**: PASS — compute batches use only action types already accepted by checked replay and frozen fixtures.
- **Scope discipline**: PASS — only compute-specific rollback is included. General ordering, ordinary partial-write recovery, stronger sync, and async lock-duration work remain explicitly excluded.
- **Clarification gate**: PASS — the user selected additive fallible APIs plus compute-specific failure atomicity; spec FR-011, FR-013, and FR-016 are consistent.

### Post-design gate

- **RED-GREEN TDD**: PASS — [quickstart.md](quickstart.md) requires targeted RED evidence and minimal GREEN implementation for one behavior before the next test.
- **Public contract**: PASS — [compute-persistence-api.md](contracts/compute-persistence-api.md) defines all seven API pairs, callback eligibility, successful results, and failure behavior.
- **Failure atomicity**: PASS — [research.md](research.md) selects prepare/checkpoint/write-and-flush/rollback/publish sequencing without a public storage trait or WAL grammar change.
- **State consistency**: PASS — [data-model.md](data-model.md) retains original live state and durable prefix after a rejected compute commit with successful rollback.
- **Compatibility**: PASS — existing wrappers, existing actions, checked replay, and frozen fixtures remain supported without migration.
- **Restart idempotence**: PASS — [quickstart.md](quickstart.md) applies a shared three-consecutive-reopen assertion to every successful SC-001/SC-002 acceptance case, not only representative histories.
- **Performance requirement**: PASS — the exact sparse, mixed, and full profiles report all three medians with no obsolete ratio threshold.
- **Research completeness**: PASS — all technical-context decisions are resolved; no `NEEDS CLARIFICATION` marker remains.

## Project Structure

### Documentation (this feature)

```text
specs/002-persist-compute-mutations/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── compute-persistence-api.md
├── checklists/
│   └── requirements.md
└── tasks.md                         # Regenerated by $speckit-tasks
```

### Source Code (repository root)

```text
src/
├── key_set_store.rs                 # Four fallible methods, wrappers, set commit, unit failure tests
├── key_map_store.rs                 # Three fallible methods, wrappers, map commit, unit failure tests
└── wal/
    ├── mod.rs                       # Batch preparation, checkpoint/rollback, deterministic fault tests
    ├── model/
    │   └── mod.rs                   # Existing action constructors; unchanged format surface
    ├── replay.rs                    # Existing replay plus restored-prefix regression surface
    └── recovery.rs                  # Existing startup recovery regression surface

tests/
├── compute_persistence.rs           # Feature integration-test root
├── compute_persistence/
│   ├── support.rs                   # Three-reopen, model, callback-count, async helpers
│   ├── contract.rs                  # Seven fallible and compatibility signatures
│   ├── key_set.rs                   # Successful four-variant set persistence
│   ├── key_map.rs                   # Successful three-variant map persistence
│   ├── outcomes.rs                  # Empty/no-op/binary/isolation/reopen cases
│   ├── histories.rs                 # Deterministic 100-history validation
│   └── performance.rs               # Ignored exact-profile 10k median report
├── recovery.rs                      # Feature 001 compatibility target
└── fixtures/
    └── legacy/                      # Frozen pre-feature WAL fixtures; never regenerated

.github/workflows/
└── recovery.yml                     # Compute target and fault tests on three platforms
```

**Structure Decision**: Keep the single-crate layout. Store modules own
collection snapshots, callback invocation, delta derivation, and publication.
`wal/mod.rs` owns existing-frame preparation and rollback-capable batch commit.
Replay and recovery remain unchanged production paths and mandatory regression
surfaces. Successful public behavior is covered by integration tests; private
fault writers stay in unit tests so no testing seam enters the public API.

## Design Overview

### Additive public API

Each existing method gains a same-parameter fallible counterpart:

| Store | Compatibility wrapper | Fallible implementation |
|---|---|---|
| Set | `compute` | `try_compute` |
| Set | `compute_async` | `try_compute_async` |
| Set | `compute_if_present` | `try_compute_if_present` |
| Set | `compute_if_absent` | `try_compute_if_absent` |
| Sorted map | `compute` | `try_compute` |
| Sorted map | `compute_if_present` | `try_compute_if_present` |
| Sorted map | `compute_if_absent` | `try_compute_if_absent` |

Fallible methods return `std::io::Result<()>`. Skipped/no-op calls return
`Ok(())`. Existing wrappers call their fallible counterpart and panic on `Err`,
retaining current unit return types and failure surface.

### Guarded working-copy lifecycle

For each operation:

1. Acquire the outer-key entry guard and evaluate its presence condition.
2. Return success immediately on a conditional mismatch.
3. Clone a present collection into original and working values, or create empty
   values for an absent key.
4. Invoke the callback exactly once on the working value. The asynchronous set
   method awaits once without changing today's guard lifetime.
5. Derive the deterministic net difference. Return success without WAL access
   for a no-op or absent-empty result.
6. Prepare all existing-format action bytes for the delta.
7. Commit the batch through checkpoint/write_all/flush; rollback on rejection.
8. Only after commit acceptance, replace or insert a non-empty result or remove
   a present key whose result is empty.

Callback panic occurs before a WAL commit or live publication and propagates.

### Set difference

- Non-empty result: sorted `after - before` appends, then sorted
  `before - after` removals.
- Present to empty: one outer-key delete.
- Absent to empty or equal states: no batch.

### Sorted-map difference

- New and changed values: map puts in `BTreeMap` order.
- Removed search keys: map removals in order after all puts.
- Replacement: one map-put action.
- Present to empty: one outer-key delete.
- Absent to empty or equal states: no batch.

### Failure-atomic compute batch

`WalState<W>` stores its current offset, writer, and a private rollback function
selected by the existing File or Vec constructor. Under one WAL write lock:

1. Treat the current offset as the authoritative checkpoint.
2. Encode consecutive frames into memory using that starting offset.
3. Call `write_all` on the complete byte buffer and call `flush` once.
4. On either error, truncate the writer to the checkpoint, retain the old
   in-memory offset, and return the persistence error.
5. On success, advance the offset to the end of the batch and return `Ok(())`.

File rollback uses `set_len(checkpoint)` under append semantics; Vec rollback
uses `truncate(checkpoint)`. Test-only writers provide the same operation while
injecting a partial write or flush error. A rollback failure is returned and
live state is not published; general repair when the medium rejects rollback is
still issue #4.

### Recovery compatibility

An accepted batch contains only ordinary existing frames, so feature 001 replay
and startup compaction need no production change. A rejected batch with
successful rollback leaves the exact previously validated prefix. Frozen
fixtures and all recovery classification tests remain mandatory regression
gates. Every successful SC-001/SC-002 acceptance case uses the same helper to
drop and reopen the file-backed store three consecutive times, asserting exact
logical contents and outer-key presence after each cycle.

### Performance validation

Correct observation of arbitrary changes through concrete mutable collection
callbacks requires O(n) snapshot comparison for a present n-item collection.
Encoded WAL work remains O(d). With setup excluded and at least 11 release-mode
samples, report corrected durable compute, equivalent ordinary durable
operations, and the pre-feature non-durable baseline for:

- sparse: add one set member / replace one map value;
- mixed: set remove 500/add 500; map remove 250/add 250/replace 500;
- full: replace all 10,000 set members / map entries.

The benchmark is ignored during normal test runs and has no pass/fail ratio.

## TDD Delivery Strategy

Every item is its own RED-GREEN cycle; repeat subcases one at a time rather than
writing a batch of tests before implementation.

1. Compile one `try_compute` result type while the existing wrapper remains
   unit-typed; minimally add that pair, then repeat per remaining method.
2. Reject a partial batch write and restore bytes/offset; add minimal checkpoint
   and File/Vec/test rollback support.
3. Reject a batch flush and restore bytes/offset; add only the flush rejection
   path.
4. Accept one batch with one lock/flush and advance offset after acceptance.
5. Persist a present-key synchronous set mixed delta through `try_compute` and
   pass three consecutive reopen assertions; add the minimal set working-copy/delta path.
6. Persist absent-key set creation; then set if-present, if-absent, and async
   variants one at a time with matching/skipped callback counts and three
   consecutive reopen assertions after every successful case.
7. Inject set write failure: verify `Err`, callback once, unchanged live state,
   and replayed prior prefix; then separately test flush failure and wrapper
   panic.
8. Persist a present-key sorted-map insert/replace/remove result through three
   consecutive reopen assertions; add the minimal map path.
9. Add map if-present and if-absent matching/skipped behavior one method at a
   time, applying the same three-reopen assertion after every successful case.
10. Inject map write failure, then flush failure, then wrapper panic as separate
    cycles with unchanged live and replayed state.
11. Normalize present-to-empty and absent-to-empty for set, then map, one case at
    a time, verifying each successful result through three reopenings.
12. Add exact no-op, duplicate/reinsert, unchanged value, binary/empty value,
    and cross-key isolation cases as separate cycles with three reopenings per
    successful case.
13. Add three-restart idempotence and 100 deterministic histories per store.
14. Run frozen fixture, recovery, all-target/all-feature, formatting, and
    feature-only Clippy regression checks.
15. Capture and report all exact-profile release medians without a threshold.
16. Add the compute target and relevant unit fault tests to the three-platform
    recovery workflow.

## Complexity Tracking

No constitution violation remains. Two compatibility costs are intentional:

| Choice | Why required | Simpler alternative rejected |
|---|---|---|
| O(n) original/working snapshots | Existing callbacks expose concrete mutable collections and may make arbitrary changes | A tracking callback type would break existing callers |
| Private writer rollback function | `Write` alone cannot restore bytes after a partial `write_all`, while public generic bounds must remain stable | A public rollback trait would expand and constrain the storage API |

Compute-specific rollback expands the original issue #2 scope by explicit user
decision. It remains bounded to compute batches; ordinary mutation partial-write
recovery is not reimplemented here.
