# Tasks: Boundary-Aware WAL Recovery and Offline Migration

**Input**: Design documents from `specs/004-recover-truncated-wal/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `quickstart.md`, and every file in `contracts/`

**Tests**: Mandatory. Root `AGENTS.md` and the constitution require one behavior-focused test, an expected runtime RED, minimum GREEN implementation, and the accumulated relevant suite. Compilation failure is not RED. New internal components begin behind `cfg(test)` probes so tests compile without changing release behavior; promotion and public/binary adapters occur only over GREEN behavior and must pass on first exposure.

**Organization**: Both safety and recovery are P1. US2 precedes US1 because corruption rules and the staged repair publisher must be proven before tail recovery may use them. US1 remains independently testable through its public cut/reopen matrix. US3 follows both P1 stories.

## Format: `[ID] [P?] [Story] Description`

- **[P]** means different files and no dependency on unfinished work in the lane.
- Story labels appear only in user-story phases.
- Every task names an exact file path.

---

## Phase 1: Setup and Immutable Baseline

**Purpose**: Establish test-only probes and freeze compatibility/performance evidence before production bytes change.

- [X] T001 Create integration roots in `tests/truncated_wal.rs`, `tests/truncated_wal/{support,contract,key_value,key_set,key_map,compatibility,performance}.rs`, `tests/migration_cli.rs`, and `tests/migration_cli/{support,contract,compatibility,failures,process}.rs`
- [X] T002 [P] Create the immutable benchmark schema and quiet-machine protocol in `specs/004-recover-truncated-wal/benchmarks/README.md`
- [X] T003 Register only `cfg(test)` V1 codec/publication/migration probes and fault checkpoints in `src/wal/mod.rs`, `src/wal/format.rs`, `src/wal/recovery.rs`, `src/migration.rs`, `src/migration_cli.rs`, and `src/test_support/mod.rs`, proving release behavior is unchanged
- [X] T004 Implement ignored paired baseline/candidate drivers with existing public APIs in `tests/truncated_wal/performance.rs`
- [X] T005 Run all 36 issue #3 steady-state baseline cells and complete-startup million-operation cells, saving raw rows and provenance in `specs/004-recover-truncated-wal/benchmarks/baseline.csv` and `specs/004-recover-truncated-wal/benchmarks/baseline.md`
- [X] T006 Hash and document immutable legacy fixtures in `tests/fixtures/legacy/README.md`
- [X] T007 Run the unmodified full suite and formatting gate, recording the pre-production checkpoint in `specs/004-recover-truncated-wal/quickstart.md`

**Checkpoint**: Baselines, fixtures, and test probes exist; release behavior is unchanged.

---

## Phase 2: Foundational V1 Grammar, Fresh Publication, and Legacy Boundary

**Purpose**: Prove private codecs and source-less publication before activating V1, then establish explicit legacy startup behavior.

### Private Header Codec — One Invariant per RED–GREEN Pair

- [X] T008 Write and run a runtime RED for exact V1 magic in `src/wal/truncation_tests.rs`
- [X] T009 Implement only magic encoding/validation in the `cfg(test)` probe in `src/wal/format.rs`, then run T008 GREEN
- [X] T010 Write and run a runtime RED for header version in `src/wal/truncation_tests.rs`
- [X] T011 Implement only version encoding/validation in `src/wal/format.rs`, then run T010 GREEN
- [X] T012 Write and run a runtime RED for fixed header length in `src/wal/truncation_tests.rs`
- [X] T013 Implement only header-length encoding/validation in `src/wal/format.rs`, then run T012 GREEN
- [X] T014 Write and run a runtime RED for store kind in `src/wal/truncation_tests.rs`
- [X] T015 Implement only store-kind encoding/validation in `src/wal/format.rs`, then run T014 GREEN
- [X] T016 Write and run a runtime RED for timestamp unit in `src/wal/truncation_tests.rs`
- [X] T017 Implement only timestamp-unit encoding/validation in `src/wal/format.rs`, then run T016 GREEN
- [X] T018 Write and run a runtime RED for nonzero `u64` granularity in `src/wal/truncation_tests.rs`
- [X] T019 Implement only granularity encoding/validation in `src/wal/format.rs`, then run T018 GREEN
- [X] T020 Write and run a runtime RED for base-bucket round-trip in `src/wal/truncation_tests.rs`
- [X] T021 Implement only base-bucket encoding/decoding in `src/wal/format.rs`, then run T020 GREEN
- [X] T022 Write and run a runtime RED for zero flags in `src/wal/truncation_tests.rs`
- [X] T023 Implement only strict flags validation in `src/wal/format.rs`, then run T022 GREEN
- [X] T024 Write and run a runtime RED for zero reserved bytes in `src/wal/truncation_tests.rs`
- [X] T025 Implement only strict reserved-byte validation in `src/wal/format.rs`, then run T024 GREEN
- [X] T026 Write and run a runtime RED for full header CRC coverage in `src/wal/truncation_tests.rs`
- [X] T027 Implement only header CRC encoding/validation in `src/wal/format.rs`, then run T026 GREEN
- [X] T028 Write and run every 1–39-byte header prefix as runtime RED for invalid-header classification in `src/wal/truncation_tests.rs`
- [X] T029 Implement only strict partial-header rejection in `src/wal/format.rs`, then run T028 and accumulated header tests GREEN

### Private Action Record Codec — One Invariant per RED–GREEN Pair

- [X] T030 Write and run a runtime RED for record marker bytes in `src/wal/truncation_tests.rs`
- [X] T031 Implement only marker encoding/validation in the `cfg(test)` probe in `src/wal/format.rs`, then run T030 GREEN
- [X] T032 Write and run a runtime RED for record version in `src/wal/truncation_tests.rs`
- [X] T033 Implement only record-version encoding/validation in `src/wal/format.rs`, then run T032 GREEN
- [X] T034 Write and run a runtime RED for supported action identifiers in `src/wal/truncation_tests.rs`
- [X] T035 Implement only action validation in `src/wal/format.rs`, then run T034 GREEN
- [X] T036 Write and run a runtime RED for fixed record-header length in `src/wal/truncation_tests.rs`
- [X] T037 Implement only record-header-length validation in `src/wal/format.rs`, then run T036 GREEN
- [X] T038 Write and run a runtime RED for payload length/complement in `src/wal/truncation_tests.rs`
- [X] T039 Implement only length/complement validation in `src/wal/format.rs`, then run T038 GREEN
- [X] T040 Write and run a runtime RED for checked frame-end and `u32` overflow in `src/wal/truncation_tests.rs`
- [X] T041 Implement only checked bounds/overflow handling in `src/wal/format.rs`, then run T040 GREEN
- [X] T042 Write and run a runtime RED for physical start and repeated footer in `src/wal/truncation_tests.rs`
- [X] T043 Implement only physical-position/footer validation in `src/wal/format.rs`, then run T042 GREEN
- [X] T044 Write and run a runtime RED for mutation-start metadata in `src/wal/truncation_tests.rs`
- [X] T045 Implement only mutation-start encoding/validation in `src/wal/format.rs`, then run T044 GREEN
- [X] T046 Write and run a runtime RED for index/count metadata in `src/wal/truncation_tests.rs`
- [X] T047 Implement only nonzero-count and index-range validation in `src/wal/format.rs`, then run T046 GREEN
- [X] T048 Write and run a runtime RED for timestamp-bucket encoding in `src/wal/truncation_tests.rs`
- [X] T049 Implement only timestamp encoding/decoding in `src/wal/format.rs`, then run T048 GREEN
- [X] T050 Write and run a runtime RED for store-specific payload validation in `src/wal/truncation_tests.rs`
- [X] T051 Implement only payload decode validation using existing action meanings in `src/wal/model/mod.rs` and `src/wal/format.rs`, then run T050 GREEN
- [X] T052 Write and run a runtime RED for full-envelope record CRC in `src/wal/truncation_tests.rs`
- [X] T053 Implement only record CRC encoding/validation in `src/wal/format.rs`, then run T052 GREEN
- [X] T054 Write and run an exact complete single-record golden-frame runtime RED in `src/wal/truncation_tests.rs`
- [X] T055 Complete only private frame composition in `src/wal/format.rs`, then run T054 and every codec invariant GREEN
- [X] T056 Promote the fully GREEN codec from test-only registration to crate-private release code in `src/wal/mod.rs` and `src/wal/format.rs`, requiring all codec tests to pass on first promoted execution without connecting startup or writes

### Fresh V1 Publication — One Checkpoint per RED–GREEN Pair

- [X] T057 Write and run invalid-options/candidate-inspection runtime RED asserting zero mutation in `src/wal/truncation_tests.rs`
- [X] T058 Implement only pre-I/O validation and missing/existing candidate classification in the `cfg(test)` fresh publisher in `src/wal/recovery.rs`, then run T057 GREEN
- [X] T059 Write and run role-bounded staging cleanup runtime RED covering successful removal and injected removal failure with active absent, exact target attempts, and exact diagnostic leftovers in `src/wal/truncation_tests.rs`
- [X] T060 Implement only safe staging-role resolution plus the invocation-local cleanup transition in `src/wal/recovery.rs`, then run T059 GREEN before introducing any later pre-commit failure handler
- [X] T061 Write and run exclusive staging create-and-register runtime RED proving failed creation leaves active absent and the cleanup registry empty, while successful creation registers exactly the created staging path as the sole invocation-owned target in `src/wal/truncation_tests.rs`
- [X] T062 Implement only exclusive same-directory staging creation and exact invocation-owned path registration in `src/wal/recovery.rs`, then run T061 GREEN
- [X] T063 Write and run every 0–39-byte partial header write runtime RED asserting active absence and the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T064 Implement only complete-write/short-write handling routed through the GREEN cleanup transition in `src/wal/recovery.rs`, then run T063 GREEN
- [X] T065 Write and run staged-header flush failure runtime RED asserting the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T066 Implement only fresh-header flush handling routed through the GREEN cleanup transition in `src/wal/recovery.rs`, then run T065 GREEN
- [X] T067 Write and run persisted-byte read/length failure runtime RED asserting the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T068 Implement only staged readback and exact-length checking routed through the GREEN cleanup transition in `src/wal/recovery.rs`, then run T067 GREEN
- [X] T069 Write and run strict staged-header validation failure runtime RED asserting the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T070 Implement only persisted-header/config/CRC comparison routed through the GREEN cleanup transition in `src/wal/recovery.rs`, then run T069 GREEN
- [X] T071 Write and run startup synchronization failure runtime RED asserting the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T072 Implement only startup sync and fail-closed handling routed through the GREEN cleanup transition in `src/wal/recovery.rs`, then run T071 GREEN
- [X] T073 Write and run append-handle preparation failure runtime RED asserting the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T074 Implement only pre-publication append positioning/capability preparation routed through the GREEN cleanup transition in `src/wal/recovery.rs`, then run T073 GREEN
- [X] T075 Write and run publish/rename failure runtime RED asserting active absence and the exact already-proven cleanup outcome in `src/wal/truncation_tests.rs`
- [X] T076 Implement only rename-to-absent-active publication in `src/wal/recovery.rs`, run T075 GREEN, then add a post-commit interruption regression requiring exactly 40 valid active bytes and deterministic next startup and require its first run GREEN
- [X] T077 Write and run successful current-initialization prepared-handle handoff runtime RED requiring the same append-capable handle at offset 40, no post-commit filesystem checkpoint, and a successful first append in `src/wal/truncation_tests.rs`
- [X] T078 Implement only infallible ownership transfer of the already-prepared handle after the publication commit point in `src/wal/recovery.rs`, then run T077, the post-commit interruption regression, and all fresh checkpoints GREEN
- [X] T079 Promote the GREEN fresh publisher from test-only registration to crate-private release code in `src/wal/recovery.rs`, requiring all checkpoint tests to pass on first promoted execution
- [X] T080 Write and run public missing-file initialization runtime RED requiring staged V1 publication and normal startup in `tests/truncated_wal/contract.rs`
- [X] T081 Connect only missing-file startup to the GREEN fresh publisher in `src/key_value_store.rs`, then run T080 and every fresh checkpoint GREEN
- [X] T082 Write and run key/set missing-file initialization runtime RED in `tests/truncated_wal/key_set.rs`
- [X] T083 Connect only key/set initialization to the GREEN fresh publisher in `src/key_set_store.rs`, then run T082 GREEN
- [X] T084 Write and run key/map missing-file initialization runtime RED in `tests/truncated_wal/key_map.rs`
- [X] T085 Connect only key/map initialization to the GREEN fresh publisher in `src/key_map_store.rs`, then run T084 GREEN
- [X] T086 Write and run vector-backed complete-header exposure runtime RED in `src/wal/truncation_tests.rs`
- [X] T087 Expose vector storage only after complete codec validation in `src/wal/mod.rs`, then run T086 GREEN

### V1 Write Activation Without Mixed Grammar

- [X] T088 Write and run first value-store single-action V1 frame and one-flush runtime RED in `src/wal/truncation_tests.rs`
- [X] T089 Connect only single-action acceptance to the GREEN V1 encoder in `src/wal/mod.rs`, then run T088 and mutation-ordering tests GREEN
- [X] T090 Write and run key/set multi-action compute-group metadata and one-flush runtime RED in `src/wal/truncation_tests.rs`
- [X] T091 Connect only prepared set compute actions to contiguous V1 group encoding in `src/key_set_store.rs` and `src/wal/mod.rs`, then run T090 GREEN
- [X] T092 Write and run key/map multi-action compute-group metadata and one-flush runtime RED in `src/wal/truncation_tests.rs`
- [X] T093 Connect only prepared map compute actions to the GREEN group encoder in `src/key_map_store.rs`, then run T092 GREEN
- [X] T094 Add all-action no-mixed-grammar append/reopen regression in `src/wal/truncation_tests.rs`
- [X] T095 Run value/set/map compute, callback, rollback, one-flush, and mutation-ordering suites after V1 write activation, recording results in `specs/004-recover-truncated-wal/quickstart.md`

### Explicit Legacy and Existing-Header Startup Boundary

- [X] T096 Write and run complete frozen legacy startup runtime RED for structured `MigrationRequired` and byte identity in `tests/truncated_wal/compatibility.rs`
- [X] T097 Implement only complete-legacy startup rejection and actionable compatibility panic in `src/recovery.rs`, `src/wal/replay.rs`, and `src/wal/recovery.rs`, then run T096 GREEN
- [X] T098 Write and run existing zero-byte active runtime RED for empty-legacy `MigrationRequired` in `tests/truncated_wal/contract.rs`
- [X] T099 Distinguish only missing from existing zero-byte input in `src/wal/replay.rs` and `src/wal/recovery.rs`, then run T098 GREEN
- [X] T100 Write and run truncated legacy runtime RED for `InvalidArtifact` and byte identity in `tests/truncated_wal/compatibility.rs`
- [X] T101 Implement only legacy incomplete-versus-complete classification in `src/wal/replay.rs`, then run T100 GREEN
- [X] T102 Write and run existing partial/corrupt V1 active preservation runtime RED in `tests/truncated_wal/contract.rs`
- [X] T103 Route existing invalid headers only to structured failure with no fresh fallback in `src/wal/recovery.rs`, then run T102 GREEN
- [X] T104 Write and run cross-family V1 kind-mismatch runtime RED in `tests/truncated_wal/contract.rs`
- [X] T105 Reject only requested/canonical store-kind mismatch before authority selection in `src/wal/replay.rs`, then run T104 GREEN
- [X] T106 Add frozen collision/no-mixed-grammar and all-family fresh/legacy regressions in `tests/truncated_wal/compatibility.rs`
- [X] T107 Run all foundational codec, publication, fixture, recovery, compute, and ordering targets and record the GREEN checkpoint in `specs/004-recover-truncated-wal/quickstart.md`

**Checkpoint**: Private codecs and every fresh-publication transition are proven before activation; active never exposes a partial new header; existing invalid/legacy artifacts remain explicit and immutable.

---

## Phase 3: User Story 2 — Reject Corruption and Migrate Explicitly (Priority: P1)

**Goal**: Reject complete corruption, prove repair fail-closed behavior, and provide a source-preserving no-overwrite migration CLI.

**Independent Test**: Corrupt every protected field at first/middle/final positions and inject every repair/migration checkpoint; no writable store or false success escapes, authority/source bytes remain protected, and complete legacy directories migrate to validated equivalent V1 destinations.

### Complete Corruption and Authority Safety

- [X] T108 [US2] Add marker/version/action/header-length first/middle/final corruption regression in `src/wal/truncation_tests.rs`
- [X] T109 [US2] Add length/complement/checked-end first/middle/final corruption regression in `src/wal/truncation_tests.rs`
- [X] T110 [US2] Add physical-start/footer first/middle/final corruption regression in `src/wal/truncation_tests.rs`
- [X] T111 [US2] Add mutation-start/index/count first/middle/final corruption regression in `src/wal/truncation_tests.rs`
- [X] T112 [US2] Add timestamp and store-payload first/middle/final corruption regression in `src/wal/truncation_tests.rs`
- [X] T113 [US2] Add full-envelope CRC first/middle/final corruption regression for all actions in `src/wal/truncation_tests.rs`
- [X] T114 [US2] Write and run earlier-complete-corruption-before-terminal-fragment runtime RED in `tests/truncated_wal/contract.rs`
- [X] T115 [US2] Make first complete-region error win over later tail classification in `src/wal/replay.rs`, then run T114 GREEN
- [X] T116 [US2] Save the full protected-field position manifest and public preservation assertions in `tests/truncated_wal/contract.rs`

### Repair Snapshot and Checkpoints

- [X] T117 [US2] Write and run pure accepted-logical-snapshot encoding runtime RED in `src/wal/truncation_tests.rs`
- [X] T118 [US2] Implement only snapshot/header-only replacement encoding in the `cfg(test)` repair probe in `src/wal/recovery.rs`, then run T117 GREEN
- [X] T119 [US2] Write and run repair staging-create failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T120 [US2] Implement only repair exclusive-create failure handling in `src/wal/recovery.rs`, then run T119 GREEN
- [X] T121 [US2] Write and run repair partial-write failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T122 [US2] Implement only repair complete-write handling in `src/wal/recovery.rs`, then run T121 GREEN
- [X] T123 [US2] Write and run repair flush failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T124 [US2] Implement only repair flush handling in `src/wal/recovery.rs`, then run T123 GREEN
- [X] T125 [US2] Write and run repair staged-validation failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T126 [US2] Implement only exact logical/config staged validation in `src/wal/recovery.rs`, then run T125 GREEN
- [X] T127 [US2] Write and run repair synchronization failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T128 [US2] Implement only repair startup-sync handling in `src/wal/recovery.rs`, then run T127 GREEN
- [X] T129 [US2] Write and run repair publish/rename failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T130 [US2] Implement only repair publish handling without source truncation in `src/wal/recovery.rs`, then run T129 GREEN
- [X] T131 [US2] Write and run repair exact-length reopen failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T132 [US2] Implement only exact reopen/complete-V1 validation before writable handoff in `src/wal/recovery.rs`, then run T131 GREEN
- [X] T133 [US2] Write and run blocking pre-publication cleanup failure runtime RED in `src/wal/truncation_tests.rs`
- [X] T134 [US2] Implement only fail-closed blocking cleanup after exclusivity proof in `src/wal/recovery.rs`, then run T133 GREEN
- [X] T135 [US2] Write and run deferrable post-publication cleanup runtime RED in `src/wal/truncation_tests.rs`
- [X] T136 [US2] Implement only deferred cleanup after new active is validated authoritative in `src/wal/recovery.rs`, then run T135 GREEN
- [X] T137 [US2] Promote the fully GREEN repair publisher to crate-private release behavior in `src/wal/recovery.rs`, requiring all repair checkpoint tests to pass on first promoted execution

### Pure Migration Conversion and Source Checkpoints

- [X] T138 [US2] Write and run pure one-family legacy-snapshot-to-V1-bytes runtime RED in `src/migration.rs`
- [X] T139 [US2] Implement only in-memory V1 snapshot conversion with bucket zero/default granularity in the `cfg(test)` migration probe in `src/migration.rs`, then run T138 GREEN
- [X] T140 [US2] Write and run pure selected-granularity conversion runtime RED in `src/migration.rs`
- [X] T141 [US2] Implement only selected-granularity conversion in `src/migration.rs`, then run T140 GREEN
- [X] T142 [US2] Write and run initial canonical source open/read failure runtime RED in `src/migration.rs`
- [X] T143 [US2] Implement only read-only initial capture and source-I/O error detail in `src/migration.rs`, then run T142 GREEN
- [X] T144 [US2] Write and run no-canonical-file preflight runtime RED in `src/migration.rs`
- [X] T145 [US2] Implement only canonical discovery/at-least-one validation in `src/migration.rs`, then run T144 GREEN
- [X] T146 [US2] Write and run recognized recovery/staging artifact runtime RED in `src/migration.rs`
- [X] T147 [US2] Implement only unresolved-artifact rejection without cleanup in `src/migration.rs`, then run T146 GREEN
- [X] T148 [US2] Write and run V1 source rejection runtime RED in `src/migration.rs`
- [X] T149 [US2] Implement only V1-as-nonmigratable classification in `src/migration.rs`, then run T148 GREEN
- [X] T150 [US2] Write and run truncated legacy source rejection runtime RED in `src/migration.rs`
- [X] T151 [US2] Implement only truncated-legacy rejection in `src/migration.rs`, then run T150 GREEN
- [X] T152 [US2] Write and run corrupt legacy source rejection runtime RED in `src/migration.rs`
- [X] T153 [US2] Implement only corrupt-legacy rejection in `src/migration.rs`, then run T152 GREEN
- [X] T154 [US2] Write and run wrong-family payload rejection runtime RED in `src/migration.rs`
- [X] T155 [US2] Implement only canonical-family compatibility validation in `src/migration.rs`, then run T154 GREEN

### Migration Destination Checkpoints

- [X] T156 [US2] Write and run existing destination file/directory/symlink runtime RED in `src/migration.rs`
- [X] T157 [US2] Implement only no-overwrite destination inspection in `src/migration.rs`, then run T156 GREEN
- [X] T158 [US2] Write and run source-equals/destination-inside-source path runtime RED in `src/migration.rs`
- [X] T159 [US2] Implement only source/destination relation validation in `src/migration.rs`, then run T158 GREEN
- [X] T160 [US2] Write and run destination directory create-and-register runtime RED proving failed creation leaves no destination artifact or owned path, while successful creation registers exactly the created directory as the sole invocation-owned path in `src/migration.rs`
- [X] T161 [US2] Implement only exclusive destination-directory creation and registration of that successfully created directory as the sole owned path in `src/migration.rs`, then run T160 GREEN
- [X] T162 [US2] Write and run invocation-owned cleanup success runtime RED with test-owned registered file/directory paths, asserting immutable source bytes, reverse-order exact target attempts, no leftovers, and no later checkpoint in `src/migration.rs`
- [X] T163 [US2] Implement only successful reverse-order file-then-directory cleanup over the existing owned-path registry in `src/migration.rs`, then run T162 GREEN
- [X] T164 [US2] Write and run cleanup removal failure runtime RED asserting the original synthetic checkpoint, exact cleanup operation/path, immutable source bytes, exact diagnostic leftovers, and no broader target attempt in `src/migration.rs`
- [X] T165 [US2] Implement only cleanup-failure propagation and diagnostic leftover reporting in `src/migration.rs`, then run T164 GREEN before introducing output creation or any later post-creation failure handler
- [X] T166 [US2] Write and run output-file create-and-register runtime RED proving failed creation leaves only the directory registered before the GREEN cleanup transition runs, while successful creation registers exactly the output before an injected downstream failure routes both owned paths through cleanup in `src/migration.rs`
- [X] T167 [US2] Implement only canonical create-new output handling, successful output-path registration, and failure routing through the GREEN cleanup transition in `src/migration.rs`, then run T166 GREEN
- [X] T168 [US2] Write and run partial header/body write failure runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T169 [US2] Implement only complete-write/short-write handling routed through the GREEN cleanup transition in `src/migration.rs`, then run T168 GREEN
- [X] T170 [US2] Write and run output flush failure runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T171 [US2] Implement only migration output flush handling routed through the GREEN cleanup transition in `src/migration.rs`, then run T170 GREEN
- [X] T172 [US2] Write and run output synchronization failure runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T173 [US2] Implement only migration output sync handling routed through the GREEN cleanup transition in `src/migration.rs`, then run T172 GREEN
- [X] T174 [US2] Write and run output reopen/read failure runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T175 [US2] Implement only ordinary drop then strict reopen/read handling routed through the GREEN cleanup transition in `src/migration.rs`, then run T174 GREEN
- [X] T176 [US2] Write and run output V1/config/logical-parity validation failure runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T177 [US2] Implement only strict reopened-output validation routed through the GREEN cleanup transition in `src/migration.rs`, then run T176 GREEN
- [X] T178 [US2] Write and run final source reread I/O failure runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T179 [US2] Implement only final reread source-I/O handling routed through the GREEN cleanup transition in `src/migration.rs`, then run T178 GREEN
- [X] T180 [US2] Write and run successful-reread changed-source runtime RED asserting the exact already-proven cleanup outcome in `src/migration.rs`
- [X] T181 [US2] Implement only exact source-byte stability comparison routed through the GREEN cleanup transition in `src/migration.rs`, then run T180 GREEN
- [X] T182 [US2] Write and run complete single-family migration success runtime RED after all checkpoints are GREEN in `src/migration.rs`
- [X] T183 [US2] Compose only the proven transitions into complete single-family success in `src/migration.rs`, then run T182 and every failure checkpoint GREEN
- [X] T184 [US2] Write and run all-present-family/empty/delete-only migration success runtime RED in `src/migration.rs`
- [X] T185 [US2] Implement only all-family orchestration over the GREEN per-family pipeline in `src/migration.rs`, then run T184 GREEN
- [X] T186 [US2] Promote the complete GREEN migration engine from test-only registration to crate-private release code in `src/lib.rs` and `src/migration.rs`, requiring all engine tests to pass on first promoted execution

### Private CLI Runner, Thin Binary, and Process Evidence

- [X] T187 [US2] Write and run valid required-option parsing runtime RED in `src/migration_cli.rs`
- [X] T188 [US2] Implement only `args_os` source/destination parsing in the `cfg(test)` runner in `src/migration_cli.rs`, then run T187 GREEN
- [X] T189 [US2] Write and run optional granularity parsing runtime RED in `src/migration_cli.rs`
- [X] T190 [US2] Implement only nonzero `u64` granularity parsing in `src/migration_cli.rs`, then run T189 GREEN
- [X] T191 [US2] Write and run unknown-option runtime RED in `src/migration_cli.rs`
- [X] T192 [US2] Implement only unknown-option usage rejection in `src/migration_cli.rs`, then run T191 GREEN
- [X] T193 [US2] Write and run duplicate-option runtime RED in `src/migration_cli.rs`
- [X] T194 [US2] Implement only duplicate-option usage rejection in `src/migration_cli.rs`, then run T193 GREEN
- [X] T195 [US2] Write and run missing-value runtime RED in `src/migration_cli.rs`
- [X] T196 [US2] Implement only missing-value usage rejection in `src/migration_cli.rs`, then run T195 GREEN
- [X] T197 [US2] Write and run non-UTF-8 path parsing runtime RED where supported in `src/migration_cli.rs`
- [X] T198 [US2] Preserve only OS-string paths through the runner in `src/migration_cli.rs`, then run T197 GREEN
- [X] T199 [US2] Write and run help-output runtime RED in `src/migration_cli.rs`
- [X] T200 [US2] Implement only help output/exit 0 with no mutation in `src/migration_cli.rs`, then run T199 GREEN
- [X] T201 [US2] Write and run version-output runtime RED in `src/migration_cli.rs`
- [X] T202 [US2] Implement only version output/exit 0 with no mutation in `src/migration_cli.rs`, then run T201 GREEN
- [X] T203 [US2] Write and run one-final-success-summary runtime RED in `src/migration_cli.rs`
- [X] T204 [US2] Implement only success stdout and empty stderr in `src/migration_cli.rs`, then run T203 GREEN
- [X] T205 [US2] Write and run deterministic internal-outcome-to-exit/diagnostic mapping runtime RED in `src/migration_cli.rs`
- [X] T206 [US2] Implement only exits 2–7 and stderr mapping in `src/migration_cli.rs`, then run T205 GREEN
- [X] T207 [US2] Promote the GREEN private runner from test-only registration in `src/lib.rs` and `src/migration_cli.rs`, requiring all runner tests to pass on first promoted execution
- [X] T208 [US2] Add explicit `[[bin]]`, one doc-hidden zero-argument bridge, and the thin executable in `Cargo.toml`, `src/lib.rs`, and `src/bin/pigment-db-migrate.rs`; add its first smoke contract in `tests/migration_cli/contract.rs` and require first execution GREEN
- [X] T209 [US2] Add child termination after destination creation, partial/complete write, validation, and pre-success output in `tests/migration_cli/process.rs`
- [X] T210 [P] [US2] Add frozen fixture, source identity, no-overwrite, append, public V1 startup, and three-reopen executable matrices in `tests/migration_cli/{compatibility,contract}.rs`
- [X] T211 [P] [US2] Add executable filesystem/exit/output/failure matrices in `tests/migration_cli/failures.rs`
- [X] T212 [US2] Run all corruption, repair, legacy, migration, CLI, recovery, compute, and ordering suites and record hashes/checkpoint traces in `specs/004-recover-truncated-wal/quickstart.md`

**Checkpoint**: US2 proves complete corruption is never shortened, repair failures never expose writable state, and migration succeeds only after every source/destination proof.

---

## Phase 4: User Story 1 — Reopen the Last Accepted State (Priority: P1)

**Goal**: Recover exactly the accepted V1 logical-mutation prefix, exclude pending effects, persist an equivalent repair, and accept stable future writes.

**Independent Test**: Cut every action and compute group at every byte/member boundary; startup returns `Recovered`, exposes only accepted state, appends successfully, and produces `Normal` with identical state for three later reopens.

- [X] T213 [US1] Write and run incomplete action-header tail classification runtime RED in `src/wal/truncation_tests.rs`
- [X] T214 [US1] Implement only constant-matching header-prefix `RecoverableTail` after valid V1 prefix in `src/wal/replay.rs`, then run T213 GREEN
- [X] T215 [US1] Write and run incomplete payload tail classification runtime RED in `src/wal/truncation_tests.rs`
- [X] T216 [US1] Implement only incomplete-payload `RecoverableTail` classification in `src/wal/replay.rs`, then run T215 GREEN
- [X] T217 [US1] Write and run incomplete footer tail classification runtime RED in `src/wal/truncation_tests.rs`
- [X] T218 [US1] Implement only incomplete-footer `RecoverableTail` classification in `src/wal/replay.rs`, then run T217 GREEN
- [X] T219 [US1] Write and run incomplete first-action empty-prefix runtime RED in `src/wal/truncation_tests.rs`
- [X] T220 [US1] Preserve only header configuration/empty accepted state for first-action tail in `src/wal/replay.rs`, then run T219 GREEN
- [X] T221 [US1] Write and run all-six-action terminal-fragment runtime RED in `src/wal/truncation_tests.rs`
- [X] T222 [US1] Generalize only terminal-fragment classification across existing action payload shapes in `src/wal/replay.rs`, then run T221 GREEN
- [X] T223 [US1] Add shared start/count/timestamp and single-flush grouped-write regression in `src/wal/truncation_tests.rs`
- [X] T224 [US1] Add deterministic same-shard waiting and different-shard progress regression for grouped writes in `src/mutation_ordering_tests/{key_set,key_map}.rs`
- [X] T225 [US1] Write and run EOF-between-complete-nonfinal-members runtime RED in `src/wal/truncation_tests.rs`
- [X] T226 [US1] Buffer only constituent effects until declared final member validates in `src/wal/replay.rs`, then run T225 GREEN
- [X] T227 [US1] Write and run every-byte/every-member group-cut runtime RED in `src/wal/truncation_tests.rs`
- [X] T228 [US1] Roll recoverable open groups back only to mutation start in `src/wal/replay.rs`, then run T227 GREEN
- [X] T229 [US1] Write and run public selected-tail staged-repair runtime RED in `tests/truncated_wal/key_value.rs`
- [X] T230 [US1] Connect only selected `RecoverableTail` to the GREEN repair publisher in `src/wal/recovery.rs`, then run T229 and every repair failure test GREEN
- [X] T231 [US1] Add exact-boundary, zero-length, rollback-failed, callback/outcome, post-repair append, and three-reopen regressions in `tests/truncated_wal/contract.rs`
- [X] T232 [P] [US1] Add complete key/value action-cut and public-state matrix in `tests/truncated_wal/key_value.rs`
- [X] T233 [P] [US1] Add complete key/set action/group-cut and membership matrix in `tests/truncated_wal/key_set.rs`
- [X] T234 [P] [US1] Add complete key/map action/group-cut and ordered-entry matrix in `tests/truncated_wal/key_map.rs`
- [X] T235 [US1] Run every cut, public store, repair, recovery, compute, and ordering target and record US1 GREEN evidence in `specs/004-recover-truncated-wal/quickstart.md`

**Checkpoint**: US1 independently recovers exactly the last accepted logical state for every store and action shape.

---

## Phase 5: User Story 3 — Consistent Recovery and Timestamp Configuration (Priority: P2)

**Goal**: Apply identical authority, status, repeatability, and timestamp rules across all stores and restarts.

**Independent Test**: Reopen equivalent tailed histories three times across all stores, exercise artifact authority, and verify default/non-default granularity, clock rollback, compaction, and options without changing ordering or public state.

- [X] T236 [US3] Write and run accepted-logical-snapshot authority comparison runtime RED in `tests/truncated_wal/contract.rs`
- [X] T237 [US3] Compare only accepted V1 logical snapshots/prefixes before repair in `src/wal/recovery.rs`, then run T236 and issue #1 authority tests GREEN
- [X] T238 [US3] Write and run ambiguous complete/recoverable candidate runtime RED in `tests/truncated_wal/contract.rs`
- [X] T239 [US3] Return only `AuthorityUndetermined` with unchanged candidates when proof is absent in `src/wal/recovery.rs`, then run T238 GREEN
- [X] T240 [US3] Add public `Recovered`-once, later-`Normal`, and compatibility notification/status regressions in `tests/truncated_wal/contract.rs`
- [X] T241 [US3] Write and run default one-minute bucket runtime RED with deterministic clock in `src/wal/truncation_tests.rs`
- [X] T242 [US3] Implement only default floor/acceptance under the existing WAL lock in `src/wal/mod.rs`, then run T241 GREEN
- [X] T243 [US3] Write and run persisted non-default granularity reopen runtime RED in `src/wal/truncation_tests.rs`
- [X] T244 [US3] Restore only header granularity/base and accepted bucket maxima in `src/wal/replay.rs` and `src/wal/mod.rs`, then run T243 GREEN
- [X] T245 [US3] Write and run forward/equal/backward clock runtime RED across restart in `src/wal/truncation_tests.rs`
- [X] T246 [US3] Clamp only requested buckets to last accepted under the WAL lock in `src/wal/mod.rs`, then run T245 and ordering tests GREEN
- [X] T247 [US3] Write and run failed write/flush bucket rollback runtime RED in `src/wal/truncation_tests.rs`
- [X] T248 [US3] Restore only pre-mutation bucket state on rejection in `src/wal/mod.rs`, then run T247 GREEN
- [X] T249 [US3] Write and run nonempty/header-only compaction metadata runtime RED in `src/wal/truncation_tests.rs`
- [X] T250 [US3] Preserve only last bucket/granularity through compaction and repair in `src/wal/recovery.rs`, then run T249 GREEN
- [X] T251 [US3] Write and run crate-private explicit-options/config-change runtime RED in `src/wal/truncation_tests.rs`
- [X] T252 [US3] Implement only validated internal options and staged configuration change in `src/config.rs`, `src/wal/recovery.rs`, and `src/wal/mod.rs`, then run T251 GREEN
- [X] T253 [US3] Expose correct `TimestampGranularity`, `DurableStoreOptions`, file/vector option adapters over GREEN internals in `src/config.rs`, `src/lib.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, and `src/key_map_store.rs`; add public contracts in `tests/truncated_wal/contract.rs` and require first execution GREEN
- [X] T254 [P] [US3] Add key/value repeatability/timestamp matrix in `tests/truncated_wal/key_value.rs`
- [X] T255 [P] [US3] Add key/set repeatability/timestamp matrix in `tests/truncated_wal/key_set.rs`
- [X] T256 [P] [US3] Add key/map repeatability/timestamp matrix in `tests/truncated_wal/key_map.rs`
- [X] T257 [US3] Run cross-store, authority, status, options, timestamp, migration, recovery, compute, and ordering targets and record US3 GREEN evidence in `specs/004-recover-truncated-wal/quickstart.md`

**Checkpoint**: US3 independently proves repeatable authority and timestamp behavior across all durable stores.

---

## Phase 6: Polish and Cross-Cutting Gates

- [X] T258 [P] Add fast V1, fresh publication, truncation, repair, migration CLI, options, and recovery targets for Linux/macOS/Windows in `.github/workflows/recovery.yml`
- [X] T259 [P] Add rustdoc for V1/legacy boundaries, fresh publication, recovery, options, and CLI-only migration in `src/config.rs`, `src/recovery.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, and `src/key_map_store.rs`
- [X] T260 Complete executable FR-001–FR-037 and SC-001–SC-018 traceability in `tests/truncated_wal/contract.rs`
- [X] T261 Run frozen fixtures and all issue #1 recovery, issue #2 compute, and issue #3 deterministic progress/deadlock/ordering suites, recording compatibility evidence in `specs/004-recover-truncated-wal/quickstart.md`
- [X] T262 Refactor duplicate codec/publication/migration code only while narrow and accumulated tests stay GREEN in `src/wal/format.rs`, `src/wal/replay.rs`, `src/wal/recovery.rs`, `src/migration.rs`, `src/migration_cli.rs`, and `src/wal/mod.rs`
- [X] T263 Run million-operation complete-versus-torn startup for at least 11 paired samples per mode in `tests/truncated_wal/performance.rs`, requiring every median ratio <=1.25
- [X] T264 Pause for the user's quiet-machine confirmation, then run all 36 steady-state candidate cells against `specs/004-recover-truncated-wal/benchmarks/baseline.csv` with per-cell throughput/latency thresholds
- [X] T265 If T263/T264 fails, write one focused threshold RED in `tests/truncated_wal/performance.rs`, make the minimum optimization in `src/wal/format.rs` or `src/wal/mod.rs`, and rerun affected/full matrices GREEN
- [X] T266 Save raw candidate cells, paired ratios, writes/second, latency, startup results, and provenance in `specs/004-recover-truncated-wal/benchmarks/final.csv` and `specs/004-recover-truncated-wal/benchmarks/final.md`
- [X] T267 Run full tests, format, strict Clippy, docs, and whitespace checks, recording results in `specs/004-recover-truncated-wal/quickstart.md`
- [X] T268 Audit `specs/004-recover-truncated-wal/{spec,plan,research,data-model,quickstart,tasks}.md` and `specs/004-recover-truncated-wal/contracts/*.md` against implementation, confirming every exclusion and no transparent migration/global lock/new dependency/direct truncation/partial fresh active file

---

## Dependencies and Execution Order

### Phase Dependencies

- Phase 1 freezes baselines and provides test-only probes.
- Phase 2 is blocking: private header/frame invariants precede promotion; fresh checkpoint pairs precede public V1 activation; explicit legacy behavior follows activation.
- Phase 3 (US2, P1) depends on Phase 2. Corruption regressions precede repair; repair checkpoints precede promotion; pure migration conversion and every source/destination checkpoint precede complete success; private runner precedes thin binary; process tests follow binary creation.
- Phase 4 (US1, P1) depends on Phase 3's GREEN repair publisher.
- Phase 5 (US3, P2) depends on both P1 stories; internal timestamp/options behavior precedes public adapters.
- Phase 6 depends on all selected stories; final steady-state benchmarking pauses for explicit quiet-machine confirmation.

### Safe MVP

The safe MVP is Phases 1–4. Both P1 stories are required: recovery without corruption/failure/migration safety is not releasable.

### RED–GREEN Rule

For every adjacent pair: write and run the named test, observe the expected runtime RED at the named checkpoint, implement only the next GREEN behavior, then run the exact and accumulated relevant suites. A generic earlier failure is not valid checkpoint RED. Promotion tasks and public/binary adapters expose already-GREEN behavior and must pass on first execution.

---

## Parallel Execution Examples

```text
Setup: T002 can run beside T001/T003.
US2 after core GREEN: T210 || T211, then T212.
US1 after T231: T232 || T233 || T234, then T235.
US3 after T253: T254 || T255 || T256, then T257.
Polish: T258 || T259, then T260 onward.
```

## Implementation Strategy

1. Freeze baseline and fixtures.
2. Complete private codec invariants, fresh publication checkpoints, and legacy boundary.
3. Complete US2 vertically; do not enable migration success before all checkpoints.
4. Complete US1 using the proven repair publisher and validate the safe P1 MVP.
5. Complete US3 internal behavior before exposing public options.
6. Run compatibility/quality/startup gates, obtain quiet-machine confirmation, then run final steady-state gates.

## Completion Rules

- No release behavior before its runtime RED; no checkpoint implementation before its named RED.
- No public/binary adapter exposes incomplete behavior.
- No source or selected authority is modified before a validated replacement is committed.
- Every existing issue #1/#2/#3 target and every individual performance cell must pass.
