# Implementation Plan: Full-Range Signed I128 Keys

**Branch**: `codex/010-fix-i128-key` | **Date**: 2026-08-08 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/007-fix-i128-key/spec.md`

## Summary

Correct the public `Key::I128` payload from `u64` to `i128`, then separate historical and current sorted-map WAL payloads with explicit V2 action identifiers. Historical legacy, V1, and earlier V2 records decode through immutable unsigned wire models and normalize by exact widening; new V2 put/remove records carry the signed model. Mixed V2 histories replay in order, while offline migration and compaction emit only the current contract.

## Technical Context

**Language/Version**: Rust 2021 edition; package MSRV remains the repository's existing supported toolchain

**Primary Dependencies**: Existing `serde`, `bincode`, `crc32fast`, `dashmap`, and standard library; no new production dependency

**Storage**: Existing legacy, V1, and segmented V2 local WAL files; only sorted-map record payloads contain `SearchKey`

**Testing**: Focused Rust unit and integration tests under `cargo test`, immutable byte fixtures, reopen/recovery tests, and offline migration CLI tests

**Target Platform**: Existing supported Rust targets; historical native-endian legacy fixtures remain covered on the repository's supported fixture platforms

**Project Type**: Rust library crate plus existing offline migration CLI

**Performance Goals**: No additional serialization pass, lock, allocation layer, or dependency on the live write path beyond the required wider 8-byte `I128` payload; existing write and reopen suites remain green

**Constraints**: Strict RED-GREEN behavior slices; valid historical bytes remain immutable; no heuristic payload detection; no in-place migration; exact unsigned-to-signed widening; no unrelated key or store-family changes

**Scale/Scope**: Full `i128` domain, composite sorted-map keys, put/remove/compute mutations, mixed V2 segments, frozen legacy/V1/earlier-V2 fixtures, and all existing unaffected-family regression suites

## Constitution Check

*GATE: Passed before design and rechecked after contracts.*

- **I. RED-GREEN TDD**: Signed decode, current put, current remove, current compute, current tail recovery, historical replay, and migration output each receive an observed behavioral RED before their minimal production change. Mixed-history and failure-matrix tests are regression composition after their component behaviors are GREEN.
- **II. Durable/live integrity**: A record action selects exactly one payload contract before replay mutates a snapshot. Unknown or invalid actions fail validation; migration validates the complete source and destination before publication.
- **III. Explicit compatibility**: Historical and current map records use distinct action identifiers. Frozen historical payloads decode only through immutable legacy models, and migration is offline/source-preserving.
- **IV. Bounded concurrency/performance**: No lock or coordination boundary changes. Encoding retains one serialization and the existing WAL critical section; the wider payload is the only required data-size increase.
- **V. Public evidence/scope**: Assertions use public key construction, reads, ordering, reopen results, migration output, and source hashes. Internal codec tests cover only the otherwise hidden action/payload grammar.
- **Project constraints**: No dependency, unsafe code, or platform behavior is added. Key/value and key/set are demonstrably unaffected because their payload models contain byte keys rather than `SearchKey`.

## Architecture and State Authority

1. Public live state always uses `Key::I128(i128)`.
2. Legacy records, V1 map actions `4`/`5`, and earlier V2 map actions `4`/`5` are authoritative historical inputs whose enum discriminant `10` contains one `u64`.
3. Current V2 map actions `6`/`7` are authoritative signed inputs whose enum discriminant `10` contains one `i128`.
4. Historical decoders normalize each unsigned value with exact widening before applying the mutation to live or migration snapshot state.
5. Runtime V2 writes and offline V2 snapshot output use only actions `6`/`7`. Mixed histories remain ordered by their enclosing V2 frames; no record is rewritten at startup.
6. Unknown actions, a historical payload under a current action, a current payload under a historical action, and malformed values fail before snapshot publication.

## Test-Driven Slices

1. **Public signed domain**: hard-coded current-wire bytes decode to negative and boundary values (RED under the old unsigned model); change the public payload type; add public ordering/size regressions (GREEN).
2. **Current V2 put**: a public durable reopen test requires full-range values and signed put action `6` (RED while ordinary writes still use historical action `4`); add only the current put path and confirm GREEN.
3. **Current V2 remove**: a separate public remove/reopen test requires action `7` (RED while remove still uses historical action `5`); add the remove mapping and confirm GREEN.
4. **Current V2 compute**: a separate compute/reopen test requires current actions (RED while the grouped writer bypasses V2 mapping); route grouped records through the mapping and confirm GREEN.
5. **Historical replay**: frozen old `I128(u64)` payloads fail after the type correction (RED); add private immutable historical wire models and exact normalization (GREEN).
6. **Tail and migration**: current-action truncation and historical offline output each receive focused REDs before extending tail classification and current snapshot output. Mixed histories then compose the GREEN paths without another production edit.
7. **Failure matrix and unaffected stores**: reject mismatched/unknown/truncated payloads, prove source immutability, and run all three store-family regression suites before full gates.

## Project Structure

### Documentation (this feature)

```text
specs/007-fix-i128-key/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── public-api.md
│   └── sorted-map-key-wire.md
├── checklists/requirements.md
└── tasks.md
```

### Source Code (repository root)

```text
src/
├── model.rs
├── key_map_store.rs
├── migration.rs
└── wal/
    ├── format.rs
    ├── model/
    │   ├── mod.rs
    │   └── historical.rs
    ├── mod.rs
    ├── recovery.rs
    └── replay.rs

tests/
├── i128_key.rs
├── fixtures/i128_key/
├── migration_cli/
└── recovery/
```

**Structure Decision**: Keep the corrected public model in `model.rs`; isolate historical wire-only types under the private WAL model rather than exposing them; extend the existing V2 action grammar and writer mapping; reuse the existing offline migration engine and public sorted-map integration seams.

## Complexity Tracking

No constitution exception is required. Two additional V2 action identifiers are the smallest unambiguous compatibility discriminator. Heuristic byte inspection, a second segment-header version, custom global `Key` serialization, and a new dependency are rejected as broader and less safe.
