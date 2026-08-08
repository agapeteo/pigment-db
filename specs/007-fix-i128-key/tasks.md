# Tasks: Full-Range Signed I128 Keys

**Input**: Design documents from `specs/007-fix-i128-key/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Required. Every production behavior follows an observed RED → minimal GREEN cycle, one behavior at a time.

**Organization**: Tasks are grouped by user story and ordered so no production edit precedes its behavior-focused failing test.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run independently in a different file after its stated prerequisites
- **[Story]**: User story traced from `spec.md`

## Phase 1: Setup and Immutable Evidence

**Purpose**: Establish independent historical inputs and a clean pre-change checkpoint.

- [x] T001 Run the pre-change focused model/WAL/migration tests and record the passing checkpoint in `specs/007-fix-i128-key/quickstart.md`
- [x] T002 Freeze complete legacy, V1, and earlier-V2 sorted-map byte fixtures containing historical `I128(u64::MAX)` put/remove records under `tests/fixtures/i128_key/`
- [x] T003 Verify fixture lengths/digests independently and record them in `tests/fixtures/i128_key/README.md`

---

## Phase 2: Foundational Contract Checks

**Purpose**: Lock the compatibility design before production changes.

- [x] T004 Validate action assignments, historical enum ordering, normalization, and source-level compatibility across `specs/007-fix-i128-key/contracts/sorted-map-key-wire.md` and `specs/007-fix-i128-key/contracts/public-api.md`
- [x] T005 Confirm no unresolved clarification or constitution gate remains in `specs/007-fix-i128-key/spec.md` and `specs/007-fix-i128-key/plan.md`

**Checkpoint**: Frozen inputs and the old/current wire contract are stable; TDD user-story slices may begin.

---

## Phase 3: User Story 1 - Use the full signed 128-bit range (Priority: P1) 🎯 MVP

**Goal**: Accept, compare, and size the complete signed 128-bit public key domain.

**Independent Test**: Decode hard-coded signed boundary bytes, then compare all boundary classes and verify 16-byte size accounting.

### RED tests for User Story 1

- [x] T006 [US1] Add a runtime assertion that hard-coded signed `i128::MIN` key bytes decode to the exact signed value in `tests/i128_key.rs`
- [x] T007 [US1] Run only the signed decode test and confirm the old unsigned model fails for the expected value mismatch in `tests/i128_key.rs`

### GREEN implementation for User Story 1

- [x] T008 [US1] Change the public `Key::I128` payload to `i128` with no other variant changes in `src/model.rs`
- [x] T009 [US1] Run the focused signed decode test and confirm GREEN in `tests/i128_key.rs`
- [x] T010 [US1] Add boundary, composite signed-ordering, and 16-byte size regression assertions in `tests/i128_key.rs`
- [x] T011 [US1] Run the complete `i128_key` target and existing model tests, keeping all tests GREEN

**Checkpoint**: User Story 1 independently exposes the full signed domain in live/public model behavior.

---

## Phase 4: User Story 2 - Reopen signed keys exactly (Priority: P1)

**Goal**: Persist full-range signed keys under an explicit current V2 record contract and reopen them exactly.

**Independent Test**: Persist all signed boundary classes through public sorted-map operations, assert current action identifiers in the WAL, and verify public reads/order across three reopen cycles.

### RED tests for User Story 2

- [x] T012 [US2] Add a public V2 sorted-map boundary round-trip test that also asserts signed put action `6` in `tests/i128_key.rs`
- [x] T013 [US2] Run the focused V2 round-trip test and confirm RED because the writer still emits historical map action `4` in `tests/i128_key.rs`

### GREEN implementation for User Story 2

- [x] T014 [US2] Define the current map action identifiers and V2-only put mapping in `src/wal/model/mod.rs`
- [x] T015 [US2] Extend V2 action/store-family and payload validation for current signed put records in `src/wal/format.rs`
- [x] T016 [US2] Route current signed put replay without changing historical action handling in `src/wal/replay.rs`
- [x] T017 [US2] Emit current action `6` for ordinary V2 map puts while retaining legacy/V1 action `4` in `src/wal/mod.rs`
- [x] T018 [US2] Run the focused V2 boundary round-trip test and confirm GREEN in `tests/i128_key.rs`

### RED-GREEN remove, compute, and failure behaviors

- [x] T019 [US2] Add a current signed remove/action/reopen test in `tests/i128_key.rs`
- [x] T020 [US2] Run the focused remove test and confirm RED because remove still emits historical action `5` in `tests/i128_key.rs`
- [x] T021 [US2] Map current V2 remove to action `7`, route its validation/replay, and confirm the focused test GREEN across `src/wal/model/mod.rs`, `src/wal/format.rs`, and `src/wal/replay.rs`
- [x] T022 [US2] Add a separate current signed compute/action/reopen test in `tests/i128_key.rs`
- [x] T023 [US2] Run the focused compute test and confirm RED because the grouped V2 writer still emits historical action `4` in `tests/i128_key.rs`
- [x] T024 [US2] Route grouped V2 map actions through the current mapping in `src/wal/mod.rs`
- [x] T025 [US2] Run the focused compute test and confirm GREEN in `tests/i128_key.rs`
- [x] T026 [US2] Add and run a truncated-current-action tail test RED before extending V2 tail action matching in `src/wal/replay.rs`, then confirm GREEN in `tests/i128_key.rs`
- [x] T027 [US2] Add strict mismatched-width, unknown-action, and wrong-family validation tests in `tests/i128_key.rs`
- [x] T028 [US2] Run all current signed V2 tests and existing V2 recovery tests, keeping the story GREEN

**Checkpoint**: User Story 2 independently persists and reopens full-range signed values with explicit current V2 actions.

---

## Phase 5: User Story 3 - Preserve historical unsigned I128 data (Priority: P1)

**Goal**: Replay and migrate frozen historical unsigned payloads by exact widening while preserving source artifacts.

**Independent Test**: Feed frozen legacy, V1, and earlier-V2 fixtures through replay/migration, assert `u64::MAX` becomes the numerically equal positive `i128`, and prove output/current actions and unchanged source digests.

### RED tests for User Story 3

- [x] T029 [US3] Add frozen earlier-V2 historical put replay assertions through public reopen in `tests/i128_key.rs`
- [x] T030 [US3] Run the focused historical replay test and confirm RED because the corrected public model cannot decode the old 8-byte payload in `tests/i128_key.rs`

### GREEN historical replay

- [x] T031 [US3] Add private immutable historical key/search-key/put/remove wire models and exact normalization in `src/wal/model/historical.rs`
- [x] T032 [US3] Route legacy, V1, and earlier-V2 actions `4`/`5` through historical decoding in `src/wal/format.rs` and `src/wal/replay.rs`
- [x] T033 [US3] Run the focused earlier-V2 historical put test and confirm GREEN in `tests/i128_key.rs`
- [x] T034 [US3] Cover frozen historical action `5`, zero, `u64::MAX`, and current composite-key ordering across `tests/i128_key.rs` and `tests/migration_cli/i128_key.rs`

### RED-GREEN mixed histories and migration

- [x] T035 [US3] Add historical-put/current-remove and current-put/historical-remove mixed V2 tests across active and sealed segments in `tests/i128_key.rs`
- [x] T036 [US3] Run the mixed-history composition tests while both component paths are GREEN in `tests/i128_key.rs`
- [x] T037 [US3] Add frozen legacy, V1, and earlier-V2 offline migration tests with pre/post source bytes and public destination reads in `tests/migration_cli/i128_key.rs`
- [x] T038 [US3] Run the focused migration test and confirm RED because map snapshot output still uses historical action `4` in `src/migration.rs`
- [x] T039 [US3] Emit only current signed map put action `6` from offline migration and V2 compaction in `src/migration.rs`
- [x] T040 [US3] Run historical and current segmented-I128 migration/compaction tests and confirm GREEN with source-byte identity in `tests/migration_cli/i128_key.rs`

**Checkpoint**: User Story 3 independently preserves all valid historical unsigned values and produces only current V2 output.

---

## Phase 6: Polish and Cross-Cutting Gates

**Purpose**: Align contracts, prove unaffected families, and complete repository quality gates.

- [x] T041 Update the canonical V2 action registry and compatibility wording in `specs/006-v2-wal-segments/contracts/wal-v2.md`
- [x] T042 [P] Document the source-level `Key::I128(i128)` contract in `src/model.rs` and managed-WAL migration behavior in `specs/007-fix-i128-key/contracts/public-api.md`
- [x] T043 Run focused key/value and key/set reopen/recovery suites to confirm the unaffected-family claim from `specs/007-fix-i128-key/spec.md`
- [x] T044 Run every command and expected outcome in `specs/007-fix-i128-key/quickstart.md`
- [x] T045 Run `cargo test --all-features -- --test-threads=1` and record zero failures in `specs/007-fix-i128-key/quickstart.md`
- [x] T046 Run formatting, warning-denying Clippy, and documentation-test gates and record results in `specs/007-fix-i128-key/quickstart.md`
- [x] T047 Reconcile requirement/task coverage and mark only completed evidence in `specs/007-fix-i128-key/tasks.md`
- [x] T048 Run final cross-artifact SpecKit analysis and resolve every HIGH/MEDIUM inconsistency in `specs/007-fix-i128-key/`
- [x] T049 Review `git diff`, confirm the branch contains only Issues #9 and #10, and commit the complete Issue #10 change

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: Starts from committed Issue #9 checkpoint `27d5a6a`.
- **Foundational (Phase 2)**: Depends on immutable fixture capture; blocks production edits.
- **US1 (Phase 3)**: Depends on Phase 2 and establishes the corrected public model.
- **US2 (Phase 4)**: Depends on US1 because current V2 payloads serialize the corrected model.
- **US3 (Phase 5)**: Depends on US1 and US2 so old/current action routing can be tested together.
- **Polish (Phase 6)**: Depends on all three user stories.

### User Story Dependencies

- **US1**: Independent public model slice and MVP.
- **US2**: Requires US1's corrected public payload but is independently demonstrated by current V2 reopen.
- **US3**: Requires current public state and action routing but is independently demonstrated by frozen source migration/replay.

### Within Each User Story

- Add exactly one behavior-focused test, run it, and observe the expected behavioral RED.
- Make only the minimal production change needed for GREEN.
- Rerun the focused test, then the relevant story suite.
- Refactor only while GREEN and rerun after each refactor.
- A compilation error or unrelated failure is not RED evidence.

### Parallel Opportunities

- T042 may run in parallel with focused unaffected-family verification after implementation.
- Documentation-only reconciliation can be prepared independently once all contracts are stable.
- Production tasks intentionally remain sequential because they share the persisted grammar and must preserve auditable RED-GREEN ordering.

---

## Parallel Example: Final Documentation

```text
Task: "Update the canonical V2 action registry in specs/006-v2-wal-segments/contracts/wal-v2.md"
Task: "Document the public source-level change in src/model.rs and the public API contract"
```

---

## Implementation Strategy

### MVP First: User Story 1

1. Freeze independent old-wire evidence.
2. Observe the signed boundary decode RED.
3. Correct only the public payload and confirm GREEN.
4. Validate signed ordering and size accounting.

### Incremental Delivery

1. Add explicit current V2 actions and prove current signed reopen.
2. Add immutable historical models and prove exact earlier-record normalization.
3. Combine both paths in mixed histories.
4. Route migration/compaction output to current actions.
5. Run unaffected-family and full repository gates.

## Notes

- Frozen fixture content is captured before T008 and never regenerated by production code under test.
- Actions `4`/`5` never change meaning; actions `6`/`7` are V2 sorted-map-only.
- No global serde override is introduced for `Key`.
- Commit after the complete Issue #10 behavior and evidence are green.
