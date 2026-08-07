# Tasks: Persist Compute Mutations

**Input**: Design documents from `specs/002-persist-compute-mutations/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [compute-persistence-api.md](contracts/compute-persistence-api.md), [quickstart.md](quickstart.md)

**Tests**: Required by root `AGENTS.md`. For every production change, write one behavior-focused test, run it and confirm the expected RED failure, implement only the paired minimum change, then confirm GREEN and run the accumulated relevant group.

**Organization**: Tasks are grouped by user story. User-story tasks carry `[US1]`, `[US2]`, or `[US3]`; setup, foundation, and cross-cutting tasks do not.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel after its stated prerequisites because it uses different files and has no dependency on another incomplete task in that stream.
- **[Story]**: Maps to the numbered user story in [spec.md](spec.md).
- Every task names its exact working file or files.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish the integration harness and capture the immutable pre-production performance baseline.

- [X] T001 Create the `compute_persistence` integration root, empty planned module files, and isolated-directory, shared three-consecutive-reopen assertion, callback-count, logical-snapshot, WAL-byte, and ready-future helpers in `tests/compute_persistence.rs`, `tests/compute_persistence/support.rs`, `tests/compute_persistence/contract.rs`, `tests/compute_persistence/key_set.rs`, `tests/compute_persistence/key_map.rs`, `tests/compute_persistence/outcomes.rs`, `tests/compute_persistence/histories.rs`, and `tests/compute_persistence/performance.rs`
- [X] T002 Add an ignored release benchmark with at least 11 setup-excluded samples for every sparse, mixed, and full 10,000-item set/map profile in `tests/compute_persistence/performance.rs`, capture pre-feature non-durable and equivalent existing durable-operation medians before production changes, and record them in `specs/002-persist-compute-mutations/quickstart.md`

**Checkpoint**: The integration target compiles without production changes, and all required pre-feature medians are recorded.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build and verify the existing-format, rollback-capable compute WAL commit shared by both stores.

**⚠️ CRITICAL**: Complete this phase before either user-story implementation stream.

- [X] T003 Write one failing test for a successful compute batch proving existing action bytes, consecutive offsets, one WAL lock, one `write_all`, one flush, and offset advancement only after acceptance in `src/wal/mod.rs`; run it and record RED
- [X] T004 Implement the minimal in-memory existing-frame encoder and shared one-lock compute batch commit needed for T003 in `src/wal/mod.rs`; rerun T003 for GREEN and the existing WAL unit group
- [X] T005 Write one failing partial-write test proving a rejected compute batch restores the exact prior bytes and WAL offset in `src/wal/mod.rs`; run it and record RED
- [X] T006 Add private File/Vec rollback functions plus a deterministic test-only writer and constructor needed for T005 in `src/wal/mod.rs`; rerun T005 for GREEN and the accumulated compute-batch tests
- [X] T007 Write one failing flush-rejection test proving a fully written but rejected compute batch restores the exact prior bytes and WAL offset in `src/wal/mod.rs`; run it and record RED
- [X] T008 Implement the minimal flush-error rollback and error propagation needed for T007 in `src/wal/mod.rs`; rerun T007 for GREEN and the accumulated compute-batch tests
- [X] T009 [P] Run the frozen-fixture and feature 001 recovery baseline commands, verify fixture checksums without regenerating files under `tests/fixtures/legacy/`, and record results in `specs/002-persist-compute-mutations/quickstart.md`

**Checkpoint**: A compute batch either extends the existing WAL prefix completely or restores the exact checkpoint, and feature 001 compatibility is green.

---

## Phase 3: User Story 1 — Set Compute Results Survive Restart (Priority: P1) 🎯 MVP

**Goal**: All four set compute operation pairs retain callback behavior, expose additive fallible APIs, and preserve accepted membership across reopening.

**Independent Test**: Exercise fallible and compatibility forms of synchronous, asynchronous, if-present, and if-absent set compute against eligible and skipped keys; compare exact immediate membership and callback counts, run every successful case through the shared three-reopen assertion, and inject write/flush rejection to prove original state remains authoritative.

### RED-GREEN tracer bullets

- [X] T010 [US1] Write one failing integration test that present-key set `try_compute` returns `std::io::Result<()>`, compatibility `compute` remains unit-returning, and both persist the same mixed add/remove result through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_set.rs`; run it and record RED
- [X] T011 [US1] Add the set `try_compute` API, make `compute` its panic-on-error wrapper, and implement only the occupied original/working-copy, deterministic delta, atomic commit, and post-commit publication path needed for T010 in `src/key_set_store.rs`; rerun T010 for GREEN
- [X] T012 [US1] Write one failing integration test that fallible and compatibility set compute on an absent key create the same non-empty set through the shared three-reopen assertion in `tests/compute_persistence/key_set.rs`; run it and record RED
- [X] T013 [US1] Implement only the vacant set `try_compute` path needed for T012 in `src/key_set_store.rs`; rerun T012 for GREEN and the accumulated synchronous set group
- [X] T014 [US1] Write one failing integration test that set `try_compute_if_present` and its compatibility wrapper invoke once and persist for a present key, invoke zero times for an absent key, return their documented result types, and pass every successful case through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_set.rs`; run it and record RED
- [X] T015 [US1] Add set `try_compute_if_present`, make the existing method its panic-on-error wrapper, route only its occupied branch through the proven set commit path, and retain the skipped vacant branch in `src/key_set_store.rs`; rerun T014 for GREEN
- [X] T016 [US1] Write one failing integration test that set `try_compute_if_absent` and its compatibility wrapper invoke once and persist for an absent key, invoke zero times for a present key, return their documented result types, and pass every successful case through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_set.rs`; run it and record RED
- [X] T017 [US1] Add set `try_compute_if_absent`, make the existing method its panic-on-error wrapper, route only its vacant branch through the proven set commit path, and retain the skipped occupied branch in `src/key_set_store.rs`; rerun T016 for GREEN
- [X] T018 [US1] Write one failing integration test that present-key set `try_compute_async` resolves to `std::io::Result<()>`, compatibility `compute_async` remains unit-returning, invokes once, and persists a mixed result through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_set.rs`; run it with the ready-future helper and record RED
- [X] T019 [US1] Add set `try_compute_async`, make `compute_async` its panic-on-error wrapper, and implement only the occupied async working-copy and post-await commit path without changing guard lifetime in `src/key_set_store.rs`; rerun T018 for GREEN
- [X] T020 [US1] Write one failing integration test that fallible and compatibility asynchronous set compute create the same non-empty absent-key result through the shared three-reopen assertion in `tests/compute_persistence/key_set.rs`; run it and record RED
- [X] T021 [US1] Implement only the vacant set `try_compute_async` path needed for T020 in `src/key_set_store.rs`; rerun T020 for GREEN and the complete successful US1 group
- [X] T022 [US1] Add a unit regression proving an injected partial-write rejection makes set `try_compute` return `Err`, invokes the callback once, retains original live state, and replays the restored original prefix in `src/key_set_store.rs`; run it with the foundational batch-failure group
- [X] T023 [US1] Add a unit regression proving an injected flush rejection makes set `try_compute` return `Err`, makes compatibility `compute` panic, and retains the same original live/replayed state in `src/key_set_store.rs`; run it with the foundational flush-failure group
- [X] T024 [US1] Add unit regressions proving injected persistence rejection returns `Err` from set `try_compute_if_present` and `try_compute_if_absent`, panics through their compatibility wrappers, preserves callback eligibility/count, and retains original state in `src/key_set_store.rs`
- [X] T025 [US1] Add an async unit regression proving injected persistence rejection resolves set `try_compute_async` to `Err`, makes compatibility `compute_async` panic when awaited, invokes once, and retains original state in `src/key_set_store.rs`; run the complete US1 group

**Checkpoint**: User Story 1 is independently usable as the MVP; all set operation pairs preserve signatures, callback conditions, successful durability, and rejected-commit state.

---

## Phase 4: User Story 2 — Sorted-Map Compute Results Survive Restart (Priority: P1)

**Goal**: All three sorted-map compute operation pairs preserve ordered insert, replacement, and removal results across reopening with additive fallible APIs.

**Independent Test**: Exercise fallible and compatibility forms against present, absent, eligible, and skipped keys; compare complete ordered maps immediately and through the shared three-reopen assertion after every successful case, and inject write/flush rejection to prove original state remains authoritative.

### Parallel RED-GREEN stream

- [X] T026 [P] [US2] Write one failing integration test that present-key map `try_compute` returns `std::io::Result<()>`, compatibility `compute` remains unit-returning, and both persist the same insert/replace/remove result and order through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_map.rs`; run it and record RED
- [X] T027 [US2] Add map `try_compute`, make `compute` its panic-on-error wrapper, and implement only the occupied original/working-copy, deterministic delta, atomic commit, and post-commit publication path needed for T026 in `src/key_map_store.rs`; rerun T026 for GREEN
- [X] T028 [US2] Write one failing integration test that fallible and compatibility map compute on an absent outer key create the same non-empty ordered result through the shared three-reopen assertion in `tests/compute_persistence/key_map.rs`; run it and record RED
- [X] T029 [US2] Implement only the vacant map `try_compute` path needed for T028 in `src/key_map_store.rs`; rerun T028 for GREEN and the accumulated unconditional map group
- [X] T030 [US2] Write one failing integration test that map `try_compute_if_present` and its compatibility wrapper invoke once and persist for a present key, invoke zero times for an absent key, return their documented result types, and pass every successful case through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_map.rs`; run it and record RED
- [X] T031 [US2] Add map `try_compute_if_present`, make the existing method its panic-on-error wrapper, route only its occupied branch through the proven map commit path, and retain the skipped vacant branch in `src/key_map_store.rs`; rerun T030 for GREEN
- [X] T032 [US2] Write one failing integration test that map `try_compute_if_absent` and its compatibility wrapper invoke once and persist for an absent key, invoke zero times for a present key, return their documented result types, and pass every successful case through the shared three-reopen assertion in `tests/compute_persistence/contract.rs` and `tests/compute_persistence/key_map.rs`; run it and record RED
- [X] T033 [US2] Add map `try_compute_if_absent`, make the existing method its panic-on-error wrapper, route only its vacant branch through the proven map commit path, and retain the skipped occupied branch in `src/key_map_store.rs`; rerun T032 for GREEN and the successful map group
- [X] T034 [US2] Add a unit regression proving an injected partial-write rejection makes map `try_compute` return `Err`, invokes the callback once, retains original live state, and replays the restored original prefix in `src/key_map_store.rs`; run it with the foundational batch-failure group
- [X] T035 [US2] Add a unit regression proving an injected flush rejection makes map `try_compute` return `Err`, makes compatibility `compute` panic, and retains the same original live/replayed state in `src/key_map_store.rs`; run it with the foundational flush-failure group
- [X] T036 [US2] Add unit regressions proving injected persistence rejection returns `Err` from map `try_compute_if_present` and `try_compute_if_absent`, preserves callback eligibility/count, and retains original state in `src/key_map_store.rs`
- [X] T037 [US2] Add unit regressions proving map compatibility `compute_if_present` and `compute_if_absent` panic on their fallible counterparts' injected persistence errors without publishing callback state in `src/key_map_store.rs`; run the complete US2 group

**Checkpoint**: User Story 2 is independently usable; every map operation pair preserves order, callback conditions, successful durability, and rejected-commit state.

---

## Phase 5: User Story 3 — Empty and Conditional Results Stay Consistent (Priority: P2)

**Goal**: Empty, skipped, and no-op outcomes have identical immediate and reopened meaning across both stores, with no phantom key, unnecessary WAL change, or unrelated-key mutation.

**Independent Test**: Drive present-to-empty, absent-to-empty, exact no-op, duplicate/reinsert, unchanged-value, binary-value, and skipped outcomes; compare key presence, WAL bytes, callback counts, other keys, and deterministic models, and pass every successful case through the shared three-reopen assertion.

### Empty-result streams and convergence

- [X] T038 [US3] Write one failing set integration test that an eligible present-to-empty result emits one logical outer-key deletion and leaves the key absent immediately and through the shared three-reopen assertion in `tests/compute_persistence/outcomes.rs`; run it and record RED
- [X] T039 [US3] Implement one-delete present-to-empty publication for every eligible set compute path needed for T038 in `src/key_set_store.rs`; rerun T038 for GREEN
- [X] T040 [US3] Write one failing set integration test that an eligible absent-to-empty result returns success without creating a live key or changing WAL bytes and remains absent through the shared three-reopen assertion in `tests/compute_persistence/outcomes.rs`; run it and record RED
- [X] T041 [US3] Implement no-write absent-to-empty handling for every eligible set compute path needed for T040 in `src/key_set_store.rs`; rerun T040 for GREEN
- [X] T042 [US3] Write one failing map integration test that an eligible present-to-empty result emits one logical outer-key deletion and leaves the key absent immediately and through the shared three-reopen assertion in `tests/compute_persistence/outcomes.rs`; run it and record RED
- [X] T043 [US3] Implement one-delete present-to-empty publication for every eligible map compute path needed for T042 in `src/key_map_store.rs`; rerun T042 for GREEN
- [X] T044 [US3] Write one failing map integration test that an eligible absent-to-empty result returns success without creating a live key or changing WAL bytes and remains absent through the shared three-reopen assertion in `tests/compute_persistence/outcomes.rs`; run it and record RED
- [X] T045 [US3] Implement no-write absent-to-empty handling for every eligible map compute path needed for T044 in `src/key_map_store.rs`; rerun T044 for GREEN
- [X] T046 [US3] Write one failing set integration test that an exact logical no-op returns success, preserves callback count and membership, leaves WAL bytes unchanged, and passes the shared three-reopen assertion in `tests/compute_persistence/outcomes.rs`; run it and record RED
- [X] T047 [US3] Add exact-equality result classification that bypasses set WAL commit and publication needed for T046 in `src/key_set_store.rs`; rerun T046 for GREEN
- [X] T048 [US3] Write one failing map integration test that an exact logical no-op returns success, preserves callback count, ordering, and values, leaves WAL bytes unchanged, and passes the shared three-reopen assertion in `tests/compute_persistence/outcomes.rs`; run it and record RED
- [X] T049 [US3] Add exact-equality result classification that bypasses map WAL commit and publication needed for T048 in `src/key_map_store.rs`; rerun T048 for GREEN
- [X] T050 [US3] Add set regression cases for duplicate insertion, remove-then-reinsert, empty/binary members, skipped callback counts, and unrelated-key isolation, applying the shared three-reopen assertion after every successful case in `tests/compute_persistence/outcomes.rs`; if any case is RED, stop and add a new explicit test/implementation GREEN task pair before changing `src/key_set_store.rs`
- [X] T051 [US3] Add map regression cases for unchanged replacement, empty/binary values and search keys, skipped callback counts, and unrelated-key isolation, applying the shared three-reopen assertion after every successful case in `tests/compute_persistence/outcomes.rs`; if any case is RED, stop and add a new explicit test/implementation GREEN task pair before changing `src/key_map_store.rs`
- [X] T052 [US3] Add 100 deterministic multi-item set histories with exact immediate models and three consecutive reopen comparisons in `tests/compute_persistence/histories.rs`; run the set history group and record results in `specs/002-persist-compute-mutations/quickstart.md`
- [X] T053 [US3] Add 100 deterministic multi-item map histories with insert/replace/remove models and three consecutive reopen comparisons in `tests/compute_persistence/histories.rs`; run the map history group and record results in `specs/002-persist-compute-mutations/quickstart.md`

**Checkpoint**: All user stories are GREEN; both stores share empty/no-op/conditional semantics, and repeated reopening preserves every accepted result.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Document the public contract, automate platforms, report performance, and complete compatibility and quality validation.

- [X] T054 [P] Add Rustdoc for all seven fallible counterparts, compatibility wrappers, callback-once behavior, empty results, persistence errors, rollback limits, and concurrency/async boundaries in `src/key_set_store.rs` and `src/key_map_store.rs`
- [X] T055 [P] Add the single-threaded `compute_persistence` target and compute fault-test filters to the Linux, macOS, and Windows matrix in `.github/workflows/recovery.yml`
- [X] T056 Repeat the exact T002 release workloads after implementation, record corrected durable compute, equivalent durable-operation, and pre-feature medians for every sparse/mixed/full set/map profile in `specs/002-persist-compute-mutations/quickstart.md`, and report results without a ratio pass/fail threshold
- [X] T057 Run every command in `specs/002-persist-compute-mutations/quickstart.md`, audit that every successful SC-001/SC-002 case uses the shared three-reopen assertion, verify frozen fixture checksums and no unintended action/replay changes, run formatting and the full relevant test suite, classify baseline versus feature-introduced Clippy diagnostics, and record final results in `specs/002-persist-compute-mutations/quickstart.md`

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: Starts immediately; T002 must finish before production edits.
- **Foundation (Phase 2)**: Depends on Setup and blocks all user-story work.
- **US1 and US2 (Phases 3–4)**: Depend on Foundation; set and map streams can proceed in parallel.
- **US3 (Phase 5)**: Depends on the relevant completed US1/US2 operation pairs.
- **Polish (Phase 6)**: Depends on all desired stories being GREEN.

### User story dependency graph

```text
Setup → Foundation ─┬→ US1: set operation pairs (P1) ─────┐
                    └→ US2: map operation pairs (P1) ─────┤
                                                           └→ US3: shared outcomes (P2) → Polish
```

### Within every RED-GREEN pair

1. Complete the RED task and run only its named behavior.
2. Confirm failure is caused by the missing contract behavior, not harness setup.
3. Complete only the paired GREEN task.
4. Re-run the targeted test, then the accumulated story group.
5. Refactor only while GREEN and rerun affected tests after each refactor.

### Parallel opportunities

- T009 can run beside T003–T008 after T002 because it touches compatibility fixtures and documentation, not WAL production code.
- After T008, the T010–T025 set stream and T026–T037 map stream can proceed in parallel; each stream is internally sequential.
- After US1/US2, T038–T041 and T042–T045 are logically separate set/map streams but are sequenced because they share `tests/compute_persistence/outcomes.rs`.
- T054 and T055 touch source documentation and CI configuration respectively and can run in parallel.

## Parallel Example: User Story 1

```text
Set RED-GREEN stream: T010 → T011 → T012 → T013 → T014 → T015 → T016 → T017 → T018 → T019 → T020 → T021
Then failure regressions: T022 → T023 → T024 → T025
Parallel opportunity: start independent US2 at T026 after Foundation.
```

## Parallel Example: User Story 2

```text
Map RED-GREEN stream: T026 → T027 → T028 → T029 → T030 → T031 → T032 → T033
Then failure regressions: T034 → T035 → T036 → T037
Parallel opportunity: continue the independent US1 stream in `src/key_set_store.rs`.
```

## Parallel Example: User Story 3

```text
Set empty stream: T038 RED → T039 GREEN → T040 RED → T041 GREEN
Then map empty stream: T042 RED → T043 GREEN → T044 RED → T045 GREEN
Converge: T046 → T049 no-op cycles → T050 → T051 regressions → T052 → T053 histories
```

## Implementation Strategy

### MVP first

1. Complete Setup and Foundation.
2. Complete US1 through T025.
3. Stop and run the complete independent set-store criterion.
4. Deliver the set durability slice only if its fallible and compatibility APIs, failure rollback, and reopen behavior are all GREEN.

### Incremental delivery

1. **US1**: All set operation pairs persist or reject atomically.
2. **US2**: All sorted-map operation pairs persist or reject atomically.
3. **US3**: Empty, skipped, and no-op outcomes converge; add deterministic history/restart coverage.
4. **Polish**: Document, automate, report medians, and run full compatibility validation.

### Parallel team strategy

After Foundation, one developer can own the set source/tests while another owns the map source/tests. Merge both GREEN streams before the shared outcomes and history phase.

## Notes

- `[P]` never permits skipping a task's own prerequisite or combining multiple RED tests before its GREEN change.
- Keep `src/wal/model/mod.rs`, `src/wal/replay.rs`, and `src/wal/recovery.rs` unchanged unless a failing compatibility test disproves the design assumption.
- Do not regenerate frozen fixtures.
- Compute-specific rollback is in scope; general ordering, ordinary-mutation partial recovery, stronger synchronization, and async lock duration remain out of scope.
- A commit-plus-rollback medium failure returns the rollback error and leaves live state unpublished; general artifact repair remains issue #4.
- Performance work reports all required medians and never applies the removed two-times threshold.
- Every successful SC-001/SC-002 acceptance case must call the shared three-reopen assertion; rejected operations use their dedicated restored-prefix checks instead.
