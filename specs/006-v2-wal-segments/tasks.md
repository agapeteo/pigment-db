# Tasks: V2 WAL Segments

**Input**: Design documents from `specs/006-v2-wal-segments/`

**Tests**: Required by the project constitution and root `AGENTS.md`; every production behavior follows RED then GREEN.

## Phase 1: Setup

- [X] T001 Create the Issue #9 branch/worktree and preserve the pre-feature baseline commit in `.git` metadata
- [X] T002 Create feature artifacts and contracts under `specs/006-v2-wal-segments/`
- [X] T003 Verify Rust ignore rules retain `target/`, editor files, logs, and environment files in `.gitignore`

---

## Phase 2: Foundational V2 Grammar

- [X] T004 Write RED header and record boundary tests for 64-byte V2 headers and `u64` fields in `src/wal/truncation_tests.rs`
- [X] T005 Implement checked V2 header/record encoding and validation with CRC32 in `src/wal/format.rs`
- [X] T006 Run T004 GREEN and the accumulated WAL unit suite in `src/wal/truncation_tests.rs`
- [X] T007 Write a RED public fresh-store test asserting V2 creation/reopen in `tests/v2_wal_segments.rs`
- [X] T008 Promote fresh file-backed creation to V2 across `src/wal/recovery.rs` and `src/key_value_store.rs`, `src/key_set_store.rs`, `src/key_map_store.rs`
- [X] T009 Run T007 GREEN for all three family startup paths in `tests/truncated_wal/`

---

## Phase 3: User Story 1 - Long-running stores beyond 4 GiB (Priority: P1)

**Goal**: Remove narrowing and checked-arithmetic defects from V2 write, repair, and replay boundaries.

**Independent Test**: A sparse prepared V2 file above 4 GiB retains its exact offset and all regular mutation/reopen tests remain green.

- [X] T010 [US1] Write and run a RED sparse-file repair handoff above 4 GiB in `src/wal/durability_tests.rs`
- [X] T011 [US1] Widen V2 runtime, prepared-file, frame-length, physical, mutation, and segment-base arithmetic in `src/wal/mod.rs`, `src/wal/recovery.rs`, and `src/wal/replay.rs`
- [X] T012 [US1] Remove cached `u32` payload sizes and add checked legacy-only narrowing in `src/wal/model/mod.rs` and `src/wal/mod.rs`
- [X] T013 [US1] Run T010 GREEN and all all-features library tests

---

## Phase 4: User Story 2 - Immutable runtime rotation (Priority: P1)

**Goal**: Bound active growth without splitting a logical mutation or introducing a global operation lock.

**Independent Test**: A small target creates consecutive sealed segments; oversized and grouped mutations stay intact; the complete chain reopens.

- [X] T014 [US2] Write and run a RED validated `WalSegmentSize` default/builder test in `tests/v2_wal_segments.rs`
- [X] T015 [US2] Add public segment-size configuration in `src/config.rs` and re-export it from `src/lib.rs`
- [X] T016 [US2] Write and run a RED numbered-segment rotation/reopen test in `tests/v2_wal_segments.rs`
- [X] T017 [US2] Implement deterministic staging, sealing, next-header publication, and chain bases in `src/wal/mod.rs`
- [X] T018 [US2] Write RED oversized-mutation and compute-group boundary tests in `tests/v2_wal_segments.rs`
- [X] T019 [US2] Encode each logical mutation before rotation and keep groups in one segment in `src/wal/mod.rs`
- [X] T020 [US2] Run T016 and T018 GREEN across key/value and compute-group paths

---

## Phase 5: User Story 2 - Segment crash recovery (Priority: P1)

**Goal**: Recover the only provable segment authority and reject ambiguous or corrupt chains.

**Independent Test**: Interrupted next-header publication and every active-tail/group cut recover the exact accepted chain.

- [X] T021 [US2] Write and run a RED interrupted-rotation promotion test in `tests/v2_wal_segments.rs`
- [X] T022 [US2] Implement sealed discovery, consecutive chain validation, and staging promotion in `src/wal/recovery.rs` and `src/wal/replay.rs`
- [X] T023 [US2] Write and run RED V2 single-record and group tail tests in `tests/v2_wal_segments.rs`
- [X] T024 [US2] Implement V2 terminal record/group classification and staged repair in `src/wal/replay.rs` and `src/wal/recovery.rs`
- [X] T025 [US2] Write and run a RED segmented active-tail test in `tests/v2_wal_segments.rs`
- [X] T026 [US2] Extend staged tail repair across a sealed chain and preserve recovery evidence in `src/wal/recovery.rs`
- [X] T027 [US2] Write and run RED exact-between-group-member cuts for set/map in `tests/truncated_wal/key_set.rs` and `tests/truncated_wal/key_map.rs`
- [X] T028 [US2] Recognize a complete non-final V2 group prefix as a recoverable tail in `src/wal/replay.rs`
- [X] T029 [US2] Run the complete truncation/recovery matrix GREEN in `tests/truncated_wal/` and `tests/recovery/`

---

## Phase 6: User Story 3 - Offline V2 migration and compaction (Priority: P2)

**Goal**: Convert supported sources into a new, validated, source-preserving V2 destination.

**Independent Test**: Frozen legacy, complete V1, recoverable V1 tail, and segmented V2 sources all produce equivalent V2 output with unchanged sources.

- [X] T030 [US3] Update migration compatibility tests to require V2 output in `tests/migration_cli/compatibility.rs`
- [X] T031 [US3] Implement legacy-to-V2 output in `src/migration.rs`
- [X] T032 [US3] Write and run a RED complete V1 conversion test in `tests/migration_cli/compatibility.rs`
- [X] T033 [US3] Accept complete V1 only through the offline CLI in `src/migration.rs` and `src/wal/recovery.rs`
- [X] T034 [US3] Write and run RED recoverable-V1-tail and segmented-V2 compaction tests in `tests/migration_cli/compatibility.rs`
- [X] T035 [US3] Capture numbered source artifacts, replay the combined chain, and emit one V2 active segment in `src/migration.rs`
- [X] T036 [US3] Preserve source bytes and nonexistent-destination semantics through CLI crash/failure tests in `tests/migration_cli/`
- [X] T037 [US3] Detect source segment additions/removals as well as byte changes during final stability verification in `src/migration.rs` and `tests/migration_cli/`

---

## Phase 7: User Story 4 - Timestamp continuity (Priority: P2)

**Goal**: Apply only explicit granularity changes and preserve monotonic timestamp state across every lifecycle transition.

**Independent Test**: A changed active granularity survives a no-override reopen/rotation, and migration preserves the last accepted bucket.

- [X] T038 [US4] Write and run a RED active-segment granularity inheritance test in `tests/v2_wal_segments.rs`
- [X] T039 [US4] Represent timestamp override intent separately from default configuration in `src/config.rs` and all store constructors
- [X] T040 [US4] Carry the active header's persisted granularity into runtime rotation in `src/wal/mod.rs`
- [X] T041 [US4] Write and run a RED V1 migration timestamp-bucket preservation test in `tests/migration_cli/compatibility.rs`
- [X] T042 [US4] Preserve the replayed last bucket in compacted V2 headers and records in `src/migration.rs`
- [X] T043 [US4] Run timestamp history and repeated reopen tests GREEN for all three families in `tests/truncated_wal/`

---

## Phase 8: Polish and Cross-Cutting Gates

- [X] T044 Update public Rust documentation from V1 startup semantics to V2/rotation/migration semantics in `src/config.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, `src/key_map_store.rs`, and `src/wal/recovery.rs`
- [X] T045 Rename stale V1-only migration helper/test descriptions where they now emit V2 in `src/migration.rs` and `tests/migration_cli/`
- [X] T046 Update integration-test frame boundaries and compatibility assertions for V2 in `tests/compute_persistence/`, `tests/recovery/`, and `tests/truncated_wal/`
- [X] T047 Run `cargo test --all-features -- --test-threads=1` GREEN across every target
- [x] T048 Run `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo doc --no-deps --all-features`
- [x] T049 Prepare matched baseline/candidate release benchmark binaries and protocol notes under `specs/006-v2-wal-segments/benchmarks/`
- [x] T050 Pause for quiet-machine approval, run alternating pinned matched pairs, and record the SC-005 decision under `specs/006-v2-wal-segments/benchmarks/`
- [x] T051 Add and smoke-test the longer eight-worker diagnostic runner with CPU/I/O pressure evidence in `specs/006-v2-wal-segments/benchmarks/runner/diagnostic_v2.rs`
- [x] T052 Pause for quiet-machine approval, run the focused diagnostic, and preserve its raw result under `specs/006-v2-wal-segments/benchmarks/results/`
- [x] T053 Evaluate pair ratios against pressure evidence, document whether the candidate lower tail reproduces under `specs/006-v2-wal-segments/benchmarks/results/`, and request the remediation decision
- [x] T054 Add and smoke-test per-worker CPU/context-switch accounting in `specs/006-v2-wal-segments/benchmarks/runner/diagnostic_v3.rs`
- [x] T055 Pause for quiet-machine approval, run Diagnostic V3, and preserve its raw result under `specs/006-v2-wal-segments/benchmarks/results/`
- [x] T056 Evaluate CPU ticks, context switches, wall throughput, and pressure evidence under `specs/006-v2-wal-segments/benchmarks/results/`, then request the remediation decision
- [x] T057 Apply the explicitly approved remediation with RED-GREEN evidence in `src/wal/mod.rs` or `specs/006-v2-wal-segments/benchmarks/README.md`
- [x] T058 Prepare and smoke-test a fresh complete six-cell SC-005 acceptance capture under `specs/006-v2-wal-segments/benchmarks/`
- [x] T059 Pause for quiet-machine approval, run the fresh acceptance capture, and record the immutable SC-005 decision under `specs/006-v2-wal-segments/benchmarks/results/`
- [x] T060 Write and run a deterministic RED progress test proving slow V2 action preparation does not own the exclusive WAL append lock in `src/wal/ordering_tests.rs`
- [x] T061 Build V2 actions before taking the exclusive WAL-state lock, revalidate health after lock acquisition, and preserve legacy/V1 offset-dependent persisted semantics in `src/wal/mod.rs`
- [x] T062 Run focused ordering/concurrency tests and the complete Rust quality suite GREEN
- [x] T063 Prepare and smoke-test a source-identified fixed-affinity Protocol V3 acceptance retry with unchanged SC-005 thresholds under `specs/006-v2-wal-segments/benchmarks/`
- [x] T064 Run the approved quiet-machine Protocol V3 capture and record its immutable SC-005 decision under `specs/006-v2-wal-segments/benchmarks/results/`
- [x] T065 Remove the per-mutation read/write lock sequence as a GREEN refactor by preparing format-independent actions before the single write guard while preserving the legacy footer checkpoint in `src/wal/mod.rs` and `src/wal/model/mod.rs`
- [x] T066 Re-run deterministic progress, frozen-format compatibility, full tests, formatting, Clippy, and documentation GREEN
- [x] T067 Prepare, smoke-test, and run a source-identified fixed-affinity Protocol V4 retry with unchanged SC-005 thresholds
- [x] T068 Record the immutable Protocol V4 decision under `specs/006-v2-wal-segments/benchmarks/results/`
- [x] T069 Reuse a WAL-owned V2 frame buffer, encode without a zero-fill pass, and defer payload-only CRC work to legacy writes in `src/wal/format.rs`, `src/wal/model/mod.rs`, and `src/wal/mod.rs`
- [x] T070 Re-run deterministic progress, frozen-format compatibility, full tests, formatting, Clippy, and documentation GREEN
- [x] T071 Prepare, smoke-test, and run a source-identified fixed-affinity Protocol V5 retry with unchanged SC-005 thresholds
- [x] T072 Record the immutable Protocol V5 decision under `specs/006-v2-wal-segments/benchmarks/results/`
- [x] T073 Re-run Spec Kit analysis, mark all completed tasks, and record final validation evidence in `specs/006-v2-wal-segments/quickstart.md`

## Dependencies

- Foundational grammar (T004-T009) blocks every user story.
- US1 width safety (T010-T013) blocks rotation and migration output promotion.
- US2 runtime rotation (T014-T020) blocks segment recovery (T021-T029) and segmented-source compaction (T034-T037).
- US3 and US4 may proceed independently after the V2 grammar and replay snapshot metadata exist.
- Quality and performance gates (T044-T051) require all behavior phases complete.

## Parallel Opportunities

- Public configuration tests in `tests/v2_wal_segments.rs` and migration fixtures in `tests/migration_cli/` can be prepared independently before their production slices.
- Documentation files under `specs/006-v2-wal-segments/` can be reviewed while focused Rust suites run.
- Baseline release compilation can run independently of candidate static analysis, but final measurements must be sequential and matched on the same quiet machine.

## Implementation Strategy

1. Deliver the V2 grammar and fresh-store reopen tracer bullet.
2. Prove `u64` boundaries, then add rotation without changing public mutation signatures.
3. Extend recovery one crash boundary at a time.
4. Promote the offline CLI to the sole V1 upgrade and V2 compaction path.
5. Preserve timestamp state, run complete compatibility gates, and finish with the matched performance decision.
