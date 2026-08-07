# Implementation Plan: Crash-Safe WAL Recovery

**Branch**: `001-fix-wal-recovery` | **Date**: 2026-08-05 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-fix-wal-recovery/spec.md`

## Summary

Replace the crash-unsafe startup sequence with one shared recovery coordinator. It keeps the active WAL authoritative while writing and validating a same-directory staged snapshot, synchronizes and closes staging, then publishes it with one replacement rename. Checked replay classifies old hidden recovery artifacts and proves whether an active WAL is an interrupted snapshot or a completed snapshot followed by later mutations. All three file-backed stores gain the same fallible initialization contract while their existing initializer signatures remain compatible.

## Technical Context

**Language/Version**: Rust 2021 edition; current validation toolchain is Rust 1.97.0, and the crate declares no MSRV

**Primary Dependencies**: Rust standard filesystem/I/O APIs; existing `bincode`, `crc32fast`, `memmap`, `dashmap`, and `log`; new development-only `tempfile` for isolated real-filesystem tests

**Storage**: Local per-store WAL files (`kv.wal.dat`, `set.wal.dat`, `map.wal.dat`), legacy hidden recovery files, and same-directory staging files

**Testing**: `cargo test`; public-interface integration tests plus focused state-classification unit tests; deterministic fault checkpoints; `cargo fmt --check`; Clippy as a diagnostic until its pre-existing baseline is repaired

**Target Platform**: Rust library on Linux, macOS, and Windows local filesystems that support same-directory replacement rename; single process per store directory

**Project Type**: Single Rust library crate

**Performance Goals**: No asymptotic regression from current startup compaction: at most one linear replay of the source plus one linear staged snapshot write and validation; normal read/write performance unchanged after initialization

**Constraints**: Preserve the existing WAL frame format and `init_new(&str) -> Self`; never select staging as authority; never mutate candidate bytes on a conflict; close mmap/file handles before rename; power-loss durability, partial normal-write recovery, and multi-process coordination remain out of scope

**Scale/Scope**: One shared state machine applied independently to three store types; up to three recognized artifacts per store kind; logical state limited by the existing in-memory replay model

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

### Pre-research gate

- **Project constitution**: PASS — `.specify/memory/constitution.md` is still an unratified placeholder and defines no enforceable principles.
- **RED–GREEN TDD**: PASS — root `AGENTS.md` requires one behavior-focused test, observed RED failure, minimal GREEN implementation, then relevant/full-suite verification. The design decomposes recovery states into separate tracer bullets.
- **Public-interface testing**: PASS — end-to-end assertions use `try_init_new` and public store reads; internal tests are limited to pure classification and deterministic transition seams.
- **Scope discipline**: PASS — separate review findings (normal-write durability, mutation ordering, callback persistence, offset width, and corruption repair) are explicitly excluded.

### Post-design gate

- **RED–GREEN TDD**: PASS — [quickstart.md](quickstart.md) defines a vertical test order and forbids writing the complete matrix before implementation.
- **Compatibility**: PASS — [recovery-api.md](contracts/recovery-api.md) retains all existing initializer and vector-backed constructor signatures while adding a fallible path.
- **Data preservation**: PASS — [data-model.md](data-model.md) has an authoritative source in every mutating transition and a preserve-all conflict outcome.
- **Cross-store consistency**: PASS — one internal coordinator owns filesystem transitions; store modules provide only filenames and logical snapshot adapters.
- **Research completeness**: PASS — [research.md](research.md) contains no unresolved `NEEDS CLARIFICATION` items.

## Project Structure

### Documentation (this feature)

```text
specs/001-fix-wal-recovery/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── recovery-api.md
├── checklists/
│   └── requirements.md
└── tasks.md                 # Created later by $speckit-tasks
```

### Source Code (repository root)

```text
src/
├── lib.rs                   # Export shared recovery contract types
├── recovery.rs              # Public outcome/status/error types
├── key_value_store.rs       # KV codec adapter + initializer pair
├── key_set_store.rs         # Set codec adapter + initializer pair
├── key_map_store.rs         # Sorted-map codec adapter + initializer pair
├── model.rs
└── wal/
    ├── mod.rs               # Fallible WAL create/open-for-append primitives
    ├── replay.rs            # Checked frames, validation, prefix snapshots
    ├── recovery.rs          # Artifact inspection and recovery coordinator
    └── model/
        └── mod.rs

tests/
├── recovery.rs              # Public API and cross-store recovery matrix
└── fixtures/
    └── legacy/              # Frozen pre-feature WAL compatibility fixtures
```

**Structure Decision**: Keep the existing single-crate layout. Public contract types receive a small root module, while WAL-specific parsing and recovery stay behind the crate-private `wal` module. Integration tests are added at the crate boundary because recovery behavior is observable through public initializers and reads.

## Design Overview

### Safe publication

1. Inspect and classify active, legacy recovery, and staging artifacts without mutation.
2. Select active, legacy recovery, empty initialization, or conflict using the decision table in [data-model.md](data-model.md).
3. Materialize the selected logical snapshot in memory.
4. Exclusively create staging beside active and write a canonical snapshot.
5. Close/synchronize staging, replay it through the checked adapter, and compare logical contents with the selected source.
6. Drop all source mappings and file handles, then rename staging over active.
7. Reopen active for append at its validated byte length and construct the store.
8. Remove stale legacy/staging artifacts only after authority is established; cleanup failure logs a warning and still yields `Recovered`.

A restart before step 6 keeps old active authoritative. A restart after step 6 observes the complete replacement at the active name. Leftover staging is never promoted.

### Legacy provenance

The old hidden backup requires special handling because failed cleanup may leave either an interrupted replay or a completed replay followed by later writes. Checked replay records logical state at every active frame boundary. If a prefix reaches the backup state, active is proven to contain a completed replay and is authoritative. If active is only a proper compacted-snapshot prefix, backup is authoritative. Anything else is a structured conflict.

When active is proven newer but stale-backup removal fails, initialization skips compaction and opens active directly. This preserves the proof prefix for the next retry and avoids publishing a new compacted snapshot that could become indistinguishable from an interrupted old replay.

### Fallible initialization

The internal coordinator returns a logical snapshot, append-ready WAL, and status. Each store adapter constructs its `DashMap` shape and wraps it in `RecoveryOutcome<Self>`. The compatibility initializer delegates, logs `Recovered`, returns the store on success, and panics with the structured diagnostic on error.

## TDD Delivery Strategy

Each item is its own RED–GREEN cycle; do not batch all tests before implementation.

1. Shared public outcome/status/error types and a fresh-store `Normal` contract test.
2. Checked frame iteration for one valid and one truncated key/value artifact.
3. Pure artifact classification for active-only and active-plus-staging states.
4. Legacy recovery-only key/value startup returning `Recovered`.
5. Interrupted legacy key/value snapshot selects recovery.
6. Completed legacy key/value replay plus later mutation selects active.
7. Ambiguous key/value candidates return a structured error with unchanged bytes.
8. Safe staged publication checkpoints for key/value.
9. Apply the proven adapter/state-machine path to key/set, one behavior at a time.
10. Apply it to key/sorted-map, one behavior at a time.
11. Cleanup-failure success/logging and compatibility-wrapper behavior.
12. Ten-interruption idempotence, three-restart stability, frozen-fixture compatibility, and cross-platform suite.

## Complexity Tracking

No constitution violations or complexity exceptions require justification. The additional modules separate public contracts, checked replay, and filesystem state transitions; they replace three duplicated unsafe initializer sequences rather than adding a parallel architecture.
