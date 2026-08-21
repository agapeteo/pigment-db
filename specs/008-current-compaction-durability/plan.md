# Implementation Plan: Current-Format Compaction and Windows Physical Durability

**Branch**: `codex/008-current-compaction-durability` | **Date**: 2026-08-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/008-current-compaction-durability/spec.md`

## Summary

Add read-only storage statistics, closed directory compaction, and per-family online compaction for the current V2 WAL format, backed by one checksummed maintenance manifest and a recoverable old/staging/replacement publication protocol. Online compaction prefixes the existing shard-to-WAL mutation order with a per-store reader/writer maintenance gate, records accepted logical mutations inside the WAL acceptance boundary, performs expensive staging work outside exclusive coordination, and atomically hands the active writer to the replacement. Windows physical durability is enabled through one target-specific `windows-sys` module that performs lossless UTF-16 write-through namespace moves and same-directory barrier preflight. Runtime code classifies legacy formats but never migrates them or changes the current record format.

## Technical Context

**Language/Version**: Rust 2021 edition on stable Rust; planning toolchain `rustc 1.97.1` (the crate does not currently declare an MSRV)

**Primary Dependencies**: Existing `dashmap`, `crc32fast`, `serde`, `bincode`,
and `memmap`; add `parking_lot = 0.12.5` for the compact non-poisoning
file-maintenance gate after the standard-library gate failed the approved
inactive-throughput threshold, and add target-specific `windows-sys = 0.61.2`
with only `Win32_Storage_FileSystem`. WAL synchronization retains its existing
standard-library locks.

**Storage**: Current V2 append-only file WALs with active and sealed segments; temporary custom-binary, CRC32-checksummed compaction manifest; no record-format change

**Testing**: `cargo test` unit/integration/fault-injection suites, deterministic scheduling hooks, subprocess crash checkpoints, Windows CI, Clippy, rustdoc, and the existing mutation performance harness extended with a feature-specific baseline/candidate protocol

**Target Platform**: Linux, macOS, and Windows filesystems supported by existing buffered durability; explicit physical durability on all three, subject to target-filesystem preflight

**Project Type**: Rust library with an external migration binary retained as a separate compatibility boundary

**Performance Goals**: With compaction inactive, every measured cell must retain at least 90% of baseline one-worker median mutation throughput and 85% of baseline eight-worker distinct-key throughput; mutation p95 must not exceed 125% of baseline

**Constraints**: Strict RED–GREEN TDD; exact logical and timestamp preservation; no implicit migration; no background scheduling; no cross-process or cross-family coordination; reads never use the maintenance gate; delta memory bounded by an 8 MiB default; disk-heavy staging outside exclusive coordination; never delete the last complete authority; no unsafe code outside one Windows durability module; no silent physical-to-buffered fallback

**Scale/Scope**: All three file-backed families (`KeyValue`, `KeySet`, `KeyMap`), directory-level mixed-family inspection/closed compaction, per-instance online compaction, four manifest phases, every publication/fault cut point, and current V2 active plus arbitrarily many contiguous sealed segments

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design.*

| Principle | Pre-research gate | Post-design gate |
|-----------|-------------------|------------------|
| I. RED–GREEN TDD | PASS — each behavior begins with a focused failing test, then minimal implementation, then the relevant suite. | PASS — contracts and quickstart define deterministic seams and required RED/GREEN evidence for inspection, recovery, concurrency, Windows, and performance. |
| II. Durable authority and live-state integrity | PASS — the design must preserve one provable authority and publish live state only after durability acceptance. | PASS — the manifest state machine retains old authority through replacement validation, distinguishes pending cleanup from publication failure, and fails writes closed when authority is indeterminate. |
| III. Explicit compatibility boundaries | PASS — runtime accepts only current format; external migration remains the sole converter. | PASS — inspection uses shallow legacy recognition only for `MigrationRequired`; compaction uses current V2 replay/encoding and never calls migration conversion. |
| IV. Bounded concurrency and measured performance | PASS — no global mutation mutex, unbounded per-key state, or background maintenance. | PASS — one constant-size per-store gate and one bounded WAL delta recorder are used; unrelated instances remain independent; a pinned baseline/candidate protocol enforces every performance threshold. |
| V. Public evidence and controlled scope | PASS — new public behavior needs documented types, structured errors, deterministic tests, and platform evidence. | PASS — public contracts, authority tables, test hooks, Windows CI coverage, static unsafe checks, and final quality commands are explicit. |

The target-specific dependency and bounded unsafe module are approved by the feature specification and remain within the constitution: the dependency exposes the required Windows primitive, and unsafe code is isolated behind a safe internal interface with platform tests. There are no constitution violations requiring an exception.

## Design Overview

### Shared maintenance architecture

`src/maintenance.rs` owns the public API and delegates to internal compaction modules. `src/compaction/inspection.rs` rejects every unexpected directory entry and discovers and validates current-format artifacts without mutation. `src/compaction/manifest.rs` owns bounded manifest parsing and the recovery state machine. `src/compaction/publication.rs` implements the publication protocol shared by closed and online compaction. `src/maintenance_coordination.rs` owns constant-size per-store gates, online-attempt RAII, and the same-process directory-open registry.

Current-V2 snapshot and delta encoders live beside current replay code in `src/wal/replay.rs`; they may share neutral V2 encoding helpers with the migration tool, but runtime maintenance never calls legacy migration probes or converters. `WalState` remains the total-order boundary for accepted mutations and gains a bounded optional delta recorder plus a detachable writer seam for Windows-safe cutover.

### Closed publication sequence

1. Resolve any existing maintenance manifest before ordinary WAL recovery.
2. Acquire an exclusive same-process directory lease and capture exact current-format source inventories, bytes, logical states, and timestamp metadata.
3. Atomically publish `Prepared`, construct a same-parent staging directory, synchronize as required, reopen it, and compare every family with the capture.
4. Re-read every source name, length, and byte before publication; reject any difference.
5. Move the source to the previous-generation location, establish `PreviousPublished`, then publish the replacement and establish `ReplacementPublished`.
6. Reopen and confirm the replacement, enter `CleanupPending`, delete only manifest-owned checksum-matching obsolete artifacts, and remove the manifest last. Cleanup failure returns a successful outcome with pending cleanup.

### Online coordination sequence

1. Atomically claim the per-instance attempt flag and resolve retryable cleanup.
2. Acquire the exclusive maintenance gate, capture logical state and timestamp metadata, and activate one WAL-state delta recorder without a gap.
3. Release exclusivity; encode, synchronize, reopen, and validate staging while reads and writes continue.
4. Reacquire exclusivity, detach the recorder, reject bounded overflow or failed WAL health, and snapshot current live state.
5. Apply accepted logical mutation groups in WAL acceptance order, synchronize, reopen, and compare staging to current live state.
6. Freeze the final original-WAL inventory, publish with the shared family-scoped manifest protocol, close the old Windows handle before namespace moves, install the replacement writer/rotation state, and release exclusivity.
7. Retry exact cleanup outside exclusive coordination; leave the replacement readable and writable when cleanup remains pending.

For file-backed instances that expose online compaction, normal mutation lock
order is always `maintenance shared -> DashMap key/shard -> WAL state`, held
through WAL acceptance and live publication. Vector-backed instances cannot
perform storage maintenance and bypass this gate. Reads keep the existing
direct DashMap path. User callbacks and post-publication callbacks execute
after maintenance and shard guards are released.

### Windows durability boundary

`src/durability/windows.rs` is the sole unsafe module and exposes safe no-replace and replace-existing write-through move operations. It constructs null-terminated UTF-16 paths without lossy conversion, calls `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` (and `MOVEFILE_REPLACE_EXISTING` only when replacement is intended), captures `io::Error::last_os_error()` immediately, and never enables cross-volume copy fallback. Physical store initialization preflights file synchronization and disposable same-directory namespace publication before exposing a store. Existing Linux/macOS publication retains rename-plus-directory-sync behavior; buffered Windows behavior remains unchanged.

## Project Structure

### Documentation (this feature)

```text
specs/008-current-compaction-durability/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── public-api.md
│   ├── compaction-authority.md
│   ├── online-coordination.md
│   ├── windows-physical-durability.md
│   └── performance.md
└── tasks.md                 # generated later by $speckit-tasks
```

### Source Code (repository root)

```text
src/
├── lib.rs
├── config.rs
├── durability.rs
├── durability/
│   └── windows.rs           # sole unsafe/Win32 boundary
├── maintenance.rs           # public API types and directory operations
├── maintenance_coordination.rs
├── compaction/
│   ├── mod.rs
│   ├── inspection.rs
│   ├── manifest.rs
│   ├── publication.rs
│   └── recovery.rs
├── key_value_store.rs
├── key_set_store.rs
├── key_map_store.rs
├── wal/
│   ├── mod.rs               # accepted-delta recording and writer handoff
│   ├── replay.rs            # deterministic current-V2 snapshot/delta encoding
│   └── recovery.rs
└── test_support/
    ├── durability_snapshot.rs
    └── maintenance_schedule.rs

tests/
├── maintenance_api.rs
├── storage_inspection/
├── closed_compaction/
├── compaction_recovery/
├── online_compaction/
├── windows_physical_durability/
├── migration_compatibility/
└── mutation_ordering/
    └── performance.rs

.github/workflows/
└── recovery.yml
```

**Structure Decision**: Keep one library crate and place the shared authority machinery in private `compaction` modules. Family modules remain thin adapters; WAL ordering logic stays at its existing serialization boundary; all Win32 FFI stays under `durability/windows.rs`. This avoids duplicating the recovery protocol across families or turning the external migration tool into a runtime dependency.

## Implementation Strategy

Implementation proceeds one behavior at a time under RED–GREEN. The first production-path edit is preceded by a frozen, feature-specific performance baseline. The vertical order is: public types and read-only inspection; same-process ownership registry; bounded manifest codec; closed staging and publication; interruption recovery before ordinary WAL recovery; per-store maintenance coordination; WAL-ordered delta recording; online staging/cutover/writer handoff; Windows content and namespace preflight/publication; complete cross-family fault matrices; performance candidate capture; final quality gates.

Every filesystem fault test snapshots names and bytes before the call and verifies either an exact complete old state, an exact complete replacement, or preserved ambiguous evidence. Every online concurrency test uses deterministic hooks and watchdogs rather than sleep-only scheduling. Migration fixtures are hashed before and after the suite.

## Complexity Tracking

No constitution violations require justification. The target-specific Windows
dependency, process-local ownership registry, custom maintenance manifest, and
one unsafe Windows module are directly required by approved functional
requirements and bounded by the feature contracts. The cross-platform
`parking_lot` dependency is limited to the per-instance file-maintenance gate;
it replaces a standard-library gate only after immutable performance evidence
failed the approved threshold, and it is not used for WAL state or vector-only
stores.
