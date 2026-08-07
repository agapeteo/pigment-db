# Tasks: Explicit Durable Write Acknowledgements

**Input**: Design documents from `specs/005-durable-write-policy/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`,
`quickstart.md`, and every file in `contracts/`

**Tests**: Mandatory. Root `AGENTS.md` and the constitution require one
behavior-focused runtime RED, the minimum GREEN implementation, and the
accumulated relevant suite for every new production behavior. Compilation failure
is not valid RED. Existing buffered and issue #4 recovery behavior is
first-execution-GREEN characterization and must never be forced to fail.

**Organization**: Phase 2 builds the complete private physical system shared by
US1, US2, and US4. Public durability types, constructors, and fallible mutators
remain unavailable until T180 proves the full capability, fresh, active-authority,
recovery-authority, cleanup, crash/reopen, and three-family exposure gate GREEN.
The four user-story phases then promote and verify public adapters in specification
priority order.

## Format: `[ID] [P?] [Story] Description`

- **[P]** means the task changes different files and has no dependency on an
  unfinished task in the same lane.
- Story labels appear only in user-story phases.
- Every task names the exact file or artifact path it changes or validates.

---

## Phase 1: Setup and Immutable Performance Evidence

**Purpose**: Create test/benchmark roots and freeze both approved comparator
datasets before any production durability behavior changes.

- [X] T001 Create the integration test root and module tree in `tests/durable_write_policy.rs` and `tests/durable_write_policy/{support,contract,key_value,key_set,key_map,compatibility,recovery,performance}.rs`
- [X] T002 [P] Finalize immutable capture/provenance templates without result rows in `specs/005-durable-write-policy/benchmarks/README.md`, `specs/005-durable-write-policy/benchmarks/baseline.md`, and `specs/005-durable-write-policy/benchmarks/reference.md`
- [X] T003 Implement fixed-payload, warmup, sampling, percentile, provenance, CSV, and explicit-real-filesystem-root helpers in `tests/durable_write_policy/support.rs`
- [X] T004 Implement the ignored 36-cell buffered baseline driver using only pre-feature public APIs in `tests/durable_write_policy/performance.rs`
- [X] T005 Implement the ignored 18-cell minimal `Mutex<File>` write-all/flush/sync-data reference driver with matching logical-mutation byte counts in `tests/durable_write_policy/performance.rs`
- [X] T006 Run all 36 pre-change buffered cells and save immutable raw results plus environment/commit/dirty-state provenance in `specs/005-durable-write-policy/benchmarks/baseline.csv` and `specs/005-durable-write-policy/benchmarks/baseline.md`
- [X] T007 Run all 18 pre-change append-plus-barrier reference cells on the same benchmark root and save immutable raw results plus provenance in `specs/005-durable-write-policy/benchmarks/reference.csv` and `specs/005-durable-write-policy/benchmarks/reference.md`
- [X] T008 Record comparator CSV and frozen issue #1–#4 fixture checksums in `specs/005-durable-write-policy/benchmarks/{baseline,reference}.md` and `tests/fixtures/legacy/README.md`
- [X] T009 Run the unmodified all-target, doc-test, and formatting gates and record the pre-production GREEN checkpoint in `specs/005-durable-write-policy/quickstart.md`

**Checkpoint**: Comparator evidence is immutable and release behavior is
unchanged.

---

## Phase 2: Foundational Private Physical System and Exposure Gate

**Purpose**: Build and prove the complete durability system behind private test
entry points. This phase blocks every public user-story adapter.

**Critical**: No task in a user-story phase may begin before T180 is GREEN.

### Deterministic Durability Harness

- [X] T010 Register only `cfg(test)` durability modules and private probe constructors in `src/wal/mod.rs`, `src/wal/durability_tests.rs`, `src/config.rs`, and `src/test_support/mod.rs`, proving a normal library build is unchanged
- [X] T011 Extend scripted scheduling with distinct write, flush, truncate, data-barrier, and full-barrier calls, failures, and exact counters in `src/test_support/fault_writer.rs`
- [X] T012 Add deterministic block/release checkpoints for data and full barriers without sleeps in `src/test_support/fault_writer.rs`
- [X] T013 Add separate volatile and durable byte images where successful barriers alone advance durable state and simulated power loss discards volatile state in `src/test_support/fault_writer.rs`
- [X] T014 [P] Add a test-only volatile/durable file-and-directory namespace snapshot with rename/remove/barrier scheduling in `src/test_support/durability_snapshot.rs` and register it in `src/test_support/mod.rs`
- [X] T015 Add and run harness self-tests for ordering, failure, blocking, rollback, and volatile-byte loss in `src/test_support/fault_writer.rs`
- [X] T016 Add and run namespace-model self-tests for file sync, directory sync, rename, removal, and crash restoration in `src/test_support/durability_snapshot.rs`
- [X] T017 Add scratch-directory, durable-image restore, process/reopen, callback-counter, and cross-shard helpers for later public tests in `tests/durable_write_policy/support.rs`
- [X] T018 Create the FR-001–FR-030 and SC-001–SC-015 executable coverage manifest with planned test names in `tests/durable_write_policy/contract.rs`

### Private Policy, Buffered Characterization, and Memory Rejection

- [X] T019 Write and run a runtime RED for private `Buffered` default and explicit `Physical` selection in `src/wal/durability_tests.rs`
- [X] T020 Implement only the private runtime policy and option selection in `src/config.rs`, then run T019 GREEN and the accumulated config suite
- [X] T021 Write and run a runtime RED proving policy is per-open and absent from legacy/V1 bytes in `src/wal/durability_tests.rs`
- [X] T022 Pass only the selected private runtime policy through construction without encoding it in `src/config.rs` and `src/wal/mod.rs`, then run T021 GREEN and format/reopen suites
- [X] T023 Add and run first-execution-GREEN buffered single- and multi-record characterization proving exact existing write/flush counts and zero data/full barriers in `src/wal/durability_tests.rs`
- [X] T024 Add and run first-execution-GREEN buffered construction characterization proving zero content/directory preflights in `src/wal/durability_tests.rs`
- [X] T025 Add and run first-execution-GREEN buffered byte, no-op, callback, and no-options reopen characterization in `tests/durable_write_policy/compatibility.rs`
- [X] T026 Write and run a runtime RED rejecting a private key/value memory-backed physical request without exposing a store in `src/wal/durability_tests.rs`
- [X] T027 Implement only private backing validation and `NoPhysicalBacking` classification in `src/durability.rs`, then run T026 GREEN and T023–T025
- [X] T028 Write and run a runtime RED routing key/set memory-backed physical construction to the GREEN rejection in `src/wal/durability_tests.rs`
- [X] T029 Connect only the private key/set construction path to backing validation in `src/key_set_store.rs`, then run T028 GREEN and key/set compatibility tests
- [X] T030 Write and run a runtime RED routing key/map memory-backed physical construction to the GREEN rejection in `src/wal/durability_tests.rs`
- [X] T031 Connect only the private key/map construction path to backing validation in `src/key_map_store.rs`, then run T030 GREEN and T023–T025

### Direct Mutation Barrier RED–GREEN Slices

- [X] T032 Write and run a runtime RED for the private file data-barrier operation and scripted barrier dispatch in `src/wal/durability_tests.rs`
- [X] T033 Implement only the private `sync_data` operation/function-pointer seam in `src/durability.rs` and `src/wal/mod.rs`, then run T032 GREEN
- [X] T034 Write and run a single-record physical acceptance RED requiring write, flush, then exactly one direct data barrier under the existing WAL guard in `src/wal/durability_tests.rs`
- [X] T035 Add only the data barrier after the complete encoded mutation and before acceptance in `src/wal/mod.rs`, then run T034 GREEN and T023–T025
- [X] T036 Add and run runtime coverage proving WAL offset and timestamp bucket remain unaccepted until the direct barrier succeeds in `src/wal/durability_tests.rs` (the assertion was already GREEN because T035 placed acceptance after the barrier)
- [X] T037 Advance offset and timestamp only after the GREEN barrier transition in `src/wal/mod.rs`, then run T036 GREEN and the WAL suite
- [X] T038 Write and run a key/set multi-record RED requiring one direct barrier after the final declared member and no constituent acceptance in `src/wal/durability_tests.rs`
- [X] T039 Route only the complete prepared set group through one WAL physical commit in `src/key_set_store.rs` and `src/wal/mod.rs`, then run T038 GREEN
- [X] T040 Write and run a key/map multi-record RED requiring one direct barrier after the final declared member and no constituent acceptance in `src/wal/durability_tests.rs`
- [X] T041 Route only the complete prepared map group through one WAL physical commit in `src/key_map_store.rs` and `src/wal/mod.rs`, then run T040 GREEN
- [X] T042 Add and run first-execution-GREEN physical exact-no-op tests requiring zero WAL writes, flushes, and barriers in `src/wal/durability_tests.rs`
- [X] T043 Add and run first-execution-GREEN concurrent direct-barrier conformance proving one completed barrier per successful call and no barrier shared across calls in `src/wal/durability_tests.rs`
- [X] T044 Rerun buffered write/flush/barrier/probe characterization after all direct-barrier slices and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Durable Rejection and Failed-Closed RED–GREEN Slices

- [X] T045 Write and run a runtime RED for private typed confirmed-rejection classification carried as an `io::Error` source with the original error kind in `src/wal/durability_tests.rs`
- [X] T046 Implement only private rejected/indeterminate/failed-closed values and source recovery in `src/wal/mod.rs`, then run T045 GREEN
- [X] T047 Write and run a failed-data-barrier RED requiring checkpoint truncate followed by one full rollback barrier in `src/wal/durability_tests.rs`
- [X] T048 Implement only truncate plus `sync_all` rollback for data-barrier failure under the WAL guard in `src/wal/mod.rs`, then run T047 GREEN
- [X] T049 Add and run partial/failed-write coverage requiring durable rollback and `Rejected(Write)` in `src/wal/durability_tests.rs` (the shared T048 rollback path was already GREEN)
- [X] T050 Route only write failure through durable rollback in `src/wal/mod.rs`, then run T049 GREEN and T047
- [X] T051 Add and run flush-failure coverage requiring durable rollback and `Rejected(Flush)` in `src/wal/durability_tests.rs` (the shared T048 rollback path was already GREEN)
- [X] T052 Route only flush failure through durable rollback in `src/wal/mod.rs`, then run T051 GREEN and T047–T050
- [X] T053 Add and run confirmed-rejection state coverage proving offset, timestamp, live state, callbacks, and next mutation retain the preceding checkpoint in `src/wal/durability_tests.rs`
- [X] T054 Restore only checkpoint state after both truncate and rollback `sync_all` succeed in `src/wal/mod.rs`, then run T053 GREEN
- [X] T055 Add and run truncate-failure coverage requiring `Indeterminate`, preserved bytes, and failed-closed health without attempting rollback sync in `src/wal/durability_tests.rs`
- [X] T056 Enter failed-closed health only on failed checkpoint truncate and preserve both diagnostics in `src/wal/mod.rs`, then run T055 GREEN
- [X] T057 Add and run rollback-sync-failure coverage requiring `Indeterminate` and failed-closed health after successful truncate in `src/wal/durability_tests.rs`
- [X] T058 Enter failed-closed health only when rollback `sync_all` cannot be confirmed in `src/wal/mod.rs`, then run T057 GREEN
- [X] T059 Add and run later-mutation coverage requiring `FailedClosed` before any writer, flush, truncate, or barrier access in `src/wal/durability_tests.rs`
- [X] T060 Reject later mutations from WAL health while retaining original/rollback diagnostics in `src/wal/mod.rs`, then run T059 GREEN
- [X] T061 Add and run first-execution-GREEN conformance proving complete valid indeterminate bytes replay through existing V1 authority in `src/wal/durability_tests.rs`
- [X] T062 Add and run first-execution-GREEN conformance proving incomplete terminal indeterminate bytes use issue #4 accepted-prefix repair in `src/wal/durability_tests.rs`
- [X] T063 Add and run first-execution-GREEN conformance proving complete structural corruption remains preserved and rejected in `src/wal/durability_tests.rs`
- [X] T064 Add and run first-execution-GREEN caller-isolation conformance proving a confirmed rejection does not alter another caller's result or barrier ownership in `src/wal/durability_tests.rs`
- [X] T065 Run the accumulated write/flush/data-barrier/truncate/rollback-sync/fail-closed matrix and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Visibility, Callback, and Interruption RED–GREEN Slices

- [X] T066 Add and run blocked-barrier coverage proving key/value reads cannot observe new state until barrier release in `src/wal/durability_tests.rs`
- [X] T067 Preserve key/value publication strictly after physical acceptance in `src/key_value_store.rs`; the existing publication ordering was already GREEN once the WAL barrier was added
- [X] T068 Add and run blocked-barrier coverage proving key/set final-member callbacks remain ineligible before acceptance in `src/wal/durability_tests.rs`
- [X] T069 Preserve key/set state and callbacks strictly after physical acceptance in `src/key_set_store.rs`; existing ordering was already GREEN
- [X] T070 Add and run blocked-barrier coverage proving key/map callbacks remain ineligible before acceptance in `src/wal/durability_tests.rs`
- [X] T071 Preserve key/map state and callbacks strictly after physical acceptance in `src/key_map_store.rs`; existing ordering was already GREEN
- [X] T072 Add and run blocked-barrier coverage proving key/map removal return values remain unobservable before acceptance in `src/wal/durability_tests.rs`
- [X] T073 Preserve key/map ordered mutation results strictly after physical acceptance in `src/key_map_store.rs`; existing ordering was already GREEN
- [X] T074 Add and run first-execution-GREEN interruption conformance after a successful barrier but before live publication, requiring complete issue #4 replay in `src/wal/durability_tests.rs`
- [X] T075 Add and run first-execution-GREEN guard-unwind and callback-count conformance after rejection and panic in `src/wal/durability_tests.rs`
- [X] T076 Run the accumulated visibility, callback, interruption, and issue #4 reopen suite and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Private Fallible Mutation Cores

- [X] T077 Write and run a runtime RED for a private key/value fallible simple-mutation core returning typed persistence failures in `src/wal/durability_tests.rs`
- [X] T078 Move only key/value put/set/remove persistence into the private fallible core in `src/key_value_store.rs`, then run T077 GREEN
- [X] T079 Write and run a runtime RED for private key/value callback and nested numeric-result shapes preserving publication/error ordering in `src/wal/durability_tests.rs`
- [X] T080 Move only key/value compute/increment/decrement persistence into GREEN private fallible cores in `src/key_value_store.rs`, then run T079 GREEN
- [X] T081 Write and run a runtime RED for private key/set append/remove/key-removal fallible shapes in `src/wal/durability_tests.rs`
- [X] T082 Move only key/set simple persistence into GREEN private fallible cores in `src/key_set_store.rs`, then run T081 GREEN
- [X] T083 Write and run a runtime RED for private key/set callback and existing sync/async compute failure propagation in `src/wal/durability_tests.rs`
- [X] T084 Route only key/set callback and compute persistence through GREEN private failure propagation in `src/key_set_store.rs`, then run T083 GREEN
- [X] T085 Write and run a runtime RED for private key/map simple and optional-result fallible shapes in `src/wal/durability_tests.rs`
- [X] T086 Move only key/map put/remove/key-removal persistence into GREEN private fallible cores in `src/key_map_store.rs`, then run T085 GREEN
- [X] T087 Write and run a runtime RED for private key/map callback, pop, ordered-append, and compute failure propagation in `src/wal/durability_tests.rs`
- [X] T088 Route only key/map callback/ordered/compute persistence through GREEN private failure propagation in `src/key_map_store.rs`, then run T087 GREEN
- [X] T089 Add and run first-execution-GREEN private compatibility-wrapper panic/cause, guard-unwind, and pending-callback async cancellation conformance proving no WAL/barrier/live change and prompt same-key guard release in `src/wal/durability_tests.rs`
- [X] T090 Run all private fallible result, callback, panic, async-cancellation, and no-public-symbol checks and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Phase-Based Capability Preflights

- [X] T091 Write and run an unsupported-target RED requiring `UnsupportedPlatform` before authority inspection or artifact mutation in `src/wal/durability_tests.rs`
- [X] T092 Implement only the safe standard-library compile-target gate in `src/durability.rs`, then run T091 GREEN
- [X] T093 Write and run a RED requiring active/recovery authority inspection without cleanup, repair, staging, or namespace mutation in `src/wal/durability_tests.rs`
- [X] T094 Add only non-mutating authority inspection before physical capability work in `src/wal/recovery.rs`, then run T093 GREEN
- [X] T095 Write and run a parent-directory preflight-success RED before any physical startup mutation in `src/wal/durability_tests.rs`
- [X] T096 Implement only parent-directory open and `sync_all` preflight in `src/durability.rs` and call it after T094 in `src/wal/recovery.rs`, then run T095 GREEN
- [X] T097 Write and run a parent-preflight-failure RED requiring `RequiredBarrierUnavailable(DirectoryEntry)` with path/source and byte/namespace identity for every error kind in `src/wal/durability_tests.rs`
- [X] T098 Map every parent preflight open/sync failure to the structured support error without artifact mutation in `src/durability.rs` and `src/recovery.rs`, then run T097 GREEN
- [X] T099 Write and run an existing-authority content-preflight-success RED requiring `sync_all` on the selected active/recovery file before stale-staging cleanup or repair in `src/wal/durability_tests.rs`
- [X] T100 Implement only selected-file full synchronization after parent preflight in `src/durability.rs` and `src/wal/recovery.rs`, then run T099 GREEN
- [X] T101 Write and run an existing-content-preflight-failure RED requiring `RequiredBarrierUnavailable(FileContent)` with selected authority and namespace unchanged in `src/wal/durability_tests.rs`
- [X] T102 Map every selected-file preflight open/sync failure to the structured support error before cleanup/repair in `src/durability.rs` and `src/wal/recovery.rs`, then run T101 GREEN
- [X] T103 Write and run a missing-store RED proving parent-directory preflight completes before staging creation in `src/wal/durability_tests.rs`
- [X] T104 Order only missing-store staging creation after GREEN parent preflight in `src/wal/recovery.rs`, then run T103 GREEN
- [X] T105 Write and run a missing-store content-preflight-success RED requiring complete staging write/flush/validation then `sync_all(staging)` before rename in `src/wal/durability_tests.rs`
- [X] T106 Use only validated staging full synchronization as the missing-store content preflight in `src/wal/recovery.rs`, then run T105 GREEN
- [X] T107 Write and run a staging-content-preflight-failure RED requiring `RequiredBarrierUnavailable(FileContent)`, no authority, and successful deterministic staging cleanup in `src/wal/durability_tests.rs`
- [X] T108 Return the support error and remove only non-authoritative staging after failed content preflight in `src/wal/recovery.rs`, then run T107 GREEN
- [X] T109 Write and run a failed-staging-cleanup RED requiring the support error to diagnose the sole remaining non-authoritative staging artifact in `src/wal/durability_tests.rs`
- [X] T110 Preserve and diagnose only staging when cleanup after failed content preflight also fails in `src/wal/recovery.rs`, then run T109 GREEN
- [X] T111 Write and run a post-preflight operation-failure RED requiring path-aware `RecoveryError::Io` rather than a support error in `src/wal/durability_tests.rs`
- [X] T112 Classify only failures after both required preflights as ordinary operation/path-aware I/O in `src/recovery.rs` and `src/wal/recovery.rs`, then run T111 GREEN
- [X] T113 Rerun first-execution-GREEN buffered construction characterization proving zero new preflights, staging, or directory barriers in `src/wal/durability_tests.rs`
- [X] T114 Run the complete unsupported/parent/existing/missing/error-kind capability matrix and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Fresh Physical Publication RED–GREEN Slices

- [X] T115 Add and run first-execution-GREEN issue #4 conformance for complete staging create/write/flush/validation before physical publication in `src/wal/durability_tests.rs`
- [X] T116 Write and run a fresh-publication RED requiring an append-capable handle prepared before authority rename in `src/wal/durability_tests.rs`
- [X] T117 Prepare only the append handle before rename and retain same-inode ownership transfer in `src/wal/recovery.rs`, then run T116 GREEN
- [X] T118 Write and run a fresh-publication RED requiring staging-to-active rename followed by parent-directory barrier before store exposure in `src/wal/durability_tests.rs`
- [X] T119 Add only the post-rename directory barrier and private handle exposure after success in `src/wal/recovery.rs`, then run T118 GREEN
- [X] T120 Write and run a fresh-rename-failure RED requiring no claimed store and only non-authoritative staging in `src/wal/durability_tests.rs`
- [X] T121 Preserve staging without claiming authority after failed fresh rename in `src/wal/recovery.rs`, then run T120 GREEN
- [X] T122 Write and run a fresh-directory-barrier-failure RED requiring no store, no inverse rename, and a complete active artifact for deterministic reopen in `src/wal/durability_tests.rs`
- [X] T123 Preserve the complete active artifact and return ordinary path-aware I/O without compensating rename in `src/wal/recovery.rs`, then run T122 GREEN
- [X] T124 Write and run a volatile/durable namespace RED proving fresh success survives discarded unbarriered state and every failure exposes no store in `src/wal/durability_tests.rs`
- [X] T125 Connect fresh checkpoints to the test-only durable namespace model in `src/wal/recovery.rs` and `src/test_support/durability_snapshot.rs`, then run T124 GREEN
- [X] T126 Run the complete private fresh capability/publication/failure/crash matrix and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Active-Authority Replacement RED–GREEN Slices

- [X] T127 Write and run an active-authority staging RED requiring validated/full-synchronized staging while active remains untouched after successful preflight in `src/wal/durability_tests.rs`
- [X] T128 Add only post-preflight replacement staging preparation in `src/wal/recovery.rs`, then run T127 GREEN
- [X] T129 Write and run an active-to-recovery rename RED requiring an immediate parent-directory barrier before replacement publication in `src/wal/durability_tests.rs`
- [X] T130 Add only the backup rename and authority-establishing directory barrier in `src/wal/recovery.rs`, then run T129 GREEN
- [X] T131 Write and run a backup-directory-barrier-failure RED requiring publication stop, no replacement rename, and preserved diagnostic artifacts in `src/wal/durability_tests.rs`
- [X] T132 Stop at the failed backup checkpoint without inverse rename in `src/wal/recovery.rs`, then run T131 GREEN
- [X] T133 Write and run a staging-to-active rename RED requiring an immediate publication-directory barrier while recovery remains untouched in `src/wal/durability_tests.rs`
- [X] T134 Add only replacement rename and publication barrier after GREEN backup authority in `src/wal/recovery.rs`, then run T133 GREEN
- [X] T135 Write and run a replacement-publication-barrier-failure RED requiring no exposed store and retained recovery authority in `src/wal/durability_tests.rs`
- [X] T136 Preserve recovery and available active without cleanup or compensation after publication uncertainty in `src/wal/recovery.rs`, then run T135 GREEN
- [X] T137 Write and run a post-publication reopen RED requiring recovery remain until active is reopened in `src/wal/durability_tests.rs`
- [X] T138 Reopen active only after its publication barrier and before cleanup in `src/wal/recovery.rs`, then run T137 GREEN
- [X] T139 Write and run a post-publication validation-failure RED requiring recovery remain authoritative and cleanup remain untouched in `src/wal/durability_tests.rs`
- [X] T140 Validate active before changing recovery and preserve recovery on failure in `src/wal/recovery.rs`, then run T139 GREEN
- [X] T141 Write and run a recovery-removal RED requiring cleanup only after durable validated active authority in `src/wal/durability_tests.rs`
- [X] T142 Remove only obsolete recovery after GREEN validation in `src/wal/recovery.rs`, then run T141 GREEN
- [X] T143 Write and run a cleanup-removal-failure RED requiring active remain authoritative and cleanup be deferred/diagnostic in `src/wal/durability_tests.rs`
- [X] T144 Preserve active and report/defer only obsolete recovery removal in `src/wal/recovery.rs`, then run T143 GREEN
- [X] T145 Write and run a cleanup-directory-barrier RED requiring a parent sync only after successful recovery removal in `src/wal/durability_tests.rs`
- [X] T146 Add only post-removal cleanup directory synchronization in `src/wal/recovery.rs`, then run T145 GREEN
- [X] T147 Write and run a cleanup-directory-barrier-failure RED requiring active authority and indeterminate cleanup without compensation in `src/wal/durability_tests.rs`
- [X] T148 Preserve active and diagnose only cleanup uncertainty in `src/wal/recovery.rs`, then run T147 GREEN
- [X] T149 Write and run an active-authority volatile/durable crash RED at every rename, reopen, validation, removal, and directory barrier checkpoint in `src/wal/durability_tests.rs`
- [X] T150 Connect active-authority checkpoints to the durable namespace model in `src/wal/recovery.rs`, then run T149 GREEN
- [X] T151 Run the complete private active-authority publication/cleanup/failure/crash matrix and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Recovery-Authority Replacement RED–GREEN Slices

- [X] T152 Write and run a recovery-authority staging RED requiring recovery remain untouched through candidate preparation and full synchronization in `src/wal/durability_tests.rs`
- [X] T153 Add only recovery-preserving candidate preparation after successful preflight in `src/wal/recovery.rs`, then run T152 GREEN
- [X] T154 Write and run an obsolete-active-removal RED proving recovery remains authority and no unnecessary intermediate directory barrier is issued in `src/wal/durability_tests.rs`
- [X] T155 Remove only proven-obsolete active while keeping recovery untouched in `src/wal/recovery.rs`, then run T154 GREEN
- [X] T156 Write and run an obsolete-active-removal-failure RED requiring recovery and namespace authority remain usable in `src/wal/durability_tests.rs`
- [X] T157 Preserve recovery and stop before replacement rename after obsolete-active removal failure in `src/wal/recovery.rs`, then run T156 GREEN
- [X] T158 Write and run a recovery-authority staging-to-active RED requiring rename plus parent-directory publication barrier in `src/wal/durability_tests.rs`
- [X] T159 Add only replacement rename and publication barrier while recovery remains available in `src/wal/recovery.rs`, then run T158 GREEN
- [X] T160 Write and run a recovery-authority publication-barrier-failure RED requiring no store and retained recovery authority in `src/wal/durability_tests.rs`
- [X] T161 Preserve recovery without cleanup or compensation after publication uncertainty in `src/wal/recovery.rs`, then run T160 GREEN
- [X] T162 Write and run a recovery-authority active-reopen RED before recovery cleanup in `src/wal/durability_tests.rs`
- [X] T163 Reopen active only after successful publication while leaving recovery untouched in `src/wal/recovery.rs`, then run T162 GREEN
- [X] T164 Write and run a recovery-authority validation-failure RED requiring recovery remain authoritative in `src/wal/durability_tests.rs`
- [X] T165 Validate active before changing recovery and preserve recovery on failure in `src/wal/recovery.rs`, then run T164 GREEN
- [X] T166 Write and run a recovery-cleanup-removal RED requiring durable validated active authority first in `src/wal/durability_tests.rs`
- [X] T167 Remove recovery only after GREEN active validation in `src/wal/recovery.rs`, then run T166 GREEN
- [X] T168 Write and run a recovery-cleanup-removal-failure RED requiring active remain authoritative and later startup deterministic in `src/wal/durability_tests.rs`
- [X] T169 Preserve active and diagnose only obsolete recovery after failed removal in `src/wal/recovery.rs`, then run T168 GREEN
- [X] T170 Write and run a recovery-cleanup-directory-barrier-failure RED requiring active authority and indeterminate cleanup in `src/wal/durability_tests.rs`
- [X] T171 Synchronize cleanup only after removal and preserve active on barrier failure in `src/wal/recovery.rs`, then run T170 GREEN
- [X] T172 Write and run a recovery-authority volatile/durable crash RED at every removal, rename, reopen, validation, and directory barrier checkpoint in `src/wal/durability_tests.rs`
- [X] T173 Connect recovery-authority checkpoints to the durable namespace model in `src/wal/recovery.rs`, then run T172 GREEN
- [X] T174 Run the complete private recovery-authority publication/cleanup/failure/crash matrix and record GREEN evidence in `specs/005-durable-write-policy/quickstart.md`

### Blocking Private Exposure Gate

- [X] T175 Complete the private key/value mutation/failure/capability/publication matrix in `src/wal/durability_tests.rs`
- [X] T176 Complete the private key/set single/group/async/callback/capability/publication matrix in `src/wal/durability_tests.rs`
- [X] T177 Complete the private key/map single/group/pop/order/callback/capability/publication matrix in `src/wal/durability_tests.rs`
- [X] T178 Run first-execution-GREEN issue #1–#4 authority, complete/incomplete tail, fixture, and corruption conformance in `src/wal/durability_tests.rs`
- [X] T179 Run normal-build symbol checks proving no public physical policy, construction, support error, or fallible mutation adapter is exposed yet and record results in `specs/005-durable-write-policy/quickstart.md`
- [X] T180 Run the complete private three-family capability, fresh, active-authority, recovery-authority, cleanup, crash/reopen, mutation, and failure suite and record the public-exposure gate GREEN in `specs/005-durable-write-policy/quickstart.md`

**Checkpoint**: The entire physical system is GREEN but still private. Public
promotion may now begin one adapter at a time.

---

## Phase 3: User Story 1 — Choose Power-Loss-Safe Acknowledgements (Priority: P1)

**Goal**: Let callers opt file-backed stores into physical durability and receive
success only after one direct barrier covers the complete logical mutation.

**Independent Test**: Use public physical construction for each family, execute
every mutation shape, discard unbarriered bytes, reopen normally, and verify every
acknowledged state through public reads.

- [X] T181 [US1] Promote the GREEN private policy/support boundary to public non-exhaustive `DurabilityPolicy`, `DurabilitySupportError`, `DurabilityCapability`, `RecoveryError::UnsupportedDurability`, `DurableStoreOptions::with_durability_policy`, and crate-root exports with a first-execution-GREEN contract in `src/config.rs`, `src/durability.rs`, `src/recovery.rs`, `src/lib.rs`, and `tests/durable_write_policy/contract.rs`
- [X] T182 [US1] Promote key/value file options construction over the GREEN physical startup path and require its first public success/reopen execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T183 [US1] Promote key/set file options construction over the GREEN physical startup path and require its first public success/reopen execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T184 [US1] Promote key/map file options construction over the GREEN physical startup path and require its first public success/reopen execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T185 [P] [US1] Add the key/value physical-success, first-mutation, numeric, compute, delete, actual-file close, and public reopen matrix in `tests/durable_write_policy/key_value.rs`
- [X] T186 [P] [US1] Add the key/set physical-success, single/group, async compute, callback, delete, actual-file close, and public reopen matrix in `tests/durable_write_policy/key_set.rs`
- [X] T187 [P] [US1] Add the key/map physical-success, single/group, callback, pop/order, delete, actual-file close, and public reopen matrix in `tests/durable_write_policy/key_map.rs`
- [X] T188 [US1] Add public same-shard waiting, different-shard preparation progress, WAL ordering, and no-deadlock conformance without private counters in `tests/durable_write_policy/contract.rs`
- [X] T189 [US1] Use private interruption/power-loss scheduling while asserting only public operation results, reads, and normal reopen outcomes for all three families in `src/wal/durability_tests.rs`
- [X] T190 [US1] Run all US1 public physical-success, ordering, callback, private-scheduled durable-image, and reopen targets and record the GREEN checkpoint in `specs/005-durable-write-policy/quickstart.md`

**Checkpoint**: Public successful physical mutations are directly acknowledged,
fully grouped, and replayable after loss.

---

## Phase 4: User Story 2 — Receive Honest Storage-Failure Outcomes (Priority: P1)

**Goal**: Expose typed support and mutation failures, durable rejection,
failed-closed outcomes, and additive fallible APIs without changing compatibility
wrappers.

**Independent Test**: Through public construction and mutation APIs, inject each
write, flush, data-barrier, truncate, rollback-sync, and preflight failure; assert
typed results, unchanged public state/callbacks, future health, artifacts, and
complete/incomplete reopen behavior.

- [X] T191 [US2] Promote GREEN mutation classifications to public non-exhaustive `MutationFailure` and `PersistenceOperation` contracts in `src/durability.rs`, `src/lib.rs`, and `tests/durable_write_policy/contract.rs`
- [X] T192 [US2] Promote key/value `try_new_vec_based_with_options` plus compatibility panic delegation over GREEN backing validation and require first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T193 [US2] Promote key/set `try_new_vec_based_with_options` plus compatibility panic delegation over GREEN backing validation and require first execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T194 [US2] Promote key/map `try_new_vec_based_with_options` plus compatibility panic delegation over GREEN backing validation and require first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T195 [US2] Expose key/value `try_put`, make `put` its cause-preserving panic wrapper, and require success/rejection/indeterminate first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T196 [US2] Expose key/value `try_compute`, make `compute` its compatibility wrapper, and require callback/publication first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T197 [US2] Expose key/value `try_increment_or_init` with nested domain result and compatibility wrapper, requiring first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T198 [US2] Expose key/value `try_decrement` with nested optional/domain result and compatibility wrapper, requiring first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T199 [US2] Expose key/value `try_set_number` and compatibility wrapper, requiring first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T200 [US2] Expose key/value `try_remove` and compatibility wrapper, requiring first execution GREEN in `src/key_value_store.rs` and `tests/durable_write_policy/key_value.rs`
- [X] T201 [US2] Expose key/set `try_append` and compatibility wrapper, requiring first execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T202 [US2] Expose key/set `try_remove_from_set` and compatibility wrapper, requiring first execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T203 [US2] Expose key/set `try_remove_from_set_callback` with post-publication callback eligibility and compatibility wrapper, requiring first execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T204 [US2] Expose key/set `try_remove_key` and compatibility wrapper, requiring first execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T205 [US2] Route existing key/set `try_compute`, `try_compute_if_present`, and `try_compute_if_absent` through typed physical failures without changing signatures, requiring first execution GREEN in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T206 [US2] Route existing key/set `try_compute_async` through typed physical failure/publication rules and require first-execution-GREEN public conformance that pending-callback cancellation publishes nothing and releases the same-key guard while post-callback persistence has no yield in `src/key_set_store.rs` and `tests/durable_write_policy/key_set.rs`
- [X] T207 [US2] Expose key/map `try_put` and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T208 [US2] Expose key/map `try_remove_from_sorted_map` with preserved optional result and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T209 [US2] Expose key/map `try_remove_from_sorted_map_callback` with post-publication callback eligibility and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T210 [US2] Expose key/map `try_remove_key` and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T211 [US2] Expose key/map `try_pop_first` with preserved ordered optional result and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T212 [US2] Expose key/map `try_pop_last` with preserved ordered optional result and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T213 [US2] Expose key/map `try_append_ordered_element` and compatibility wrapper, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T214 [US2] Route existing key/map `try_compute`, `try_compute_if_present`, and `try_compute_if_absent` through typed physical failures without changing signatures, requiring first execution GREEN in `src/key_map_store.rs` and `tests/durable_write_policy/key_map.rs`
- [X] T215 [US2] Use private fault scheduling but assert only public key/value results and reads for every write/flush/data-sync/truncate/rollback-sync/fail-closed/result/panic failure in `src/wal/durability_tests.rs`
- [X] T216 [US2] Use private fault scheduling but assert only public key/set results, reads, callback counts, and pending-callback cancellation state for every single/group/async failure in `src/wal/durability_tests.rs`
- [X] T217 [US2] Use private fault scheduling but assert only public key/map results, reads, and callback counts for every single/group/pop/order failure in `src/wal/durability_tests.rs`
- [X] T218 [US2] Use private preflight scheduling but assert public construction errors, artifact identity, no downgrade, and post-preflight ordinary-I/O classification in `src/wal/durability_tests.rs`
- [X] T219 [US2] Assert public cause chains, panic diagnostics, failed-closed no-I/O, and normal complete/incomplete reopen outcomes around private scheduling in `src/wal/durability_tests.rs`
- [X] T220 [US2] Run all US2 public support/fault/API/recovery/fixture/ordering targets and record the GREEN checkpoint in `specs/005-durable-write-policy/quickstart.md`

**Checkpoint**: Public failures are honest and classifiable; confirmed rollback is
durable, uncertain authority fails closed, and compatibility wrappers retain their
established behavior.

---

## Phase 5: User Story 3 — Preserve Existing Fast Buffered Behavior (Priority: P2)

**Goal**: Preserve every existing constructor, result, callback, byte format, and
fast buffered path while documenting its weaker acknowledgement guarantee.

**Independent Test**: Use only pre-feature APIs across memory/file and all three
families; prove zero capability/barrier work, unchanged public behavior and bytes,
and later pass every immutable buffered benchmark cell.

- [X] T221 [US3] Add a first-execution-GREEN public contract proving all no-options constructors and reopen paths select buffered durability in `tests/durable_write_policy/compatibility.rs`
- [X] T222 [US3] Call public buffered single/group mutations from crate-unit tests and use private counters only to prove exact write/flush and zero data/full barrier/preflight behavior in `src/wal/durability_tests.rs`
- [X] T223 [US3] Add first-execution-GREEN byte-for-byte legacy/V1, action-count, result, callback, and exact-no-op compatibility regressions in `tests/durable_write_policy/compatibility.rs`
- [X] T224 [US3] Add first-execution-GREEN reopen tests proving policy is runtime-only and no-options reopen returns to buffered behavior in `tests/durable_write_policy/compatibility.rs`
- [X] T225 [P] [US3] Add complete legacy key/value signature, nested-result, panic, callback, and key-existence coverage in `tests/durable_write_policy/key_value.rs`
- [X] T226 [P] [US3] Add complete legacy key/set signature, no-op, async, callback, and membership coverage in `tests/durable_write_policy/key_set.rs`
- [X] T227 [P] [US3] Add complete legacy key/map signature, ordered-result, no-op, callback, and membership coverage in `tests/durable_write_policy/key_map.rs`
- [X] T228 [US3] Run immutable fixtures and issue #1–#4 public compatibility/reopen matrices, verifying no fixture or persisted-format changes in `tests/durable_write_policy/compatibility.rs`
- [X] T229 [US3] Document buffered success, physical opt-in, runtime-only reopen selection, and panic-versus-error compatibility in `src/config.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, `src/key_map_store.rs`, and `src/lib.rs`
- [X] T230 [US3] Run all US3 public compatibility, format, fixture, compute, and ordering targets and record the GREEN checkpoint in `specs/005-durable-write-policy/quickstart.md`

**Checkpoint**: Existing users retain buffered semantics and release-path cost;
no compatibility RED was fabricated.

---

## Phase 6: User Story 4 — Durably Publish Store Files (Priority: P2)

**Goal**: Prove through public reopen behavior that successful physical creation,
startup replacement, and cleanup make contents and namespace authority durable.

**Independent Test**: Fail or interrupt every public startup checkpoint, discard
unbarriered byte/namespace state, reopen normally, and verify the last completely
published authority and cleanup ordering.

- [X] T231 [US4] Use private namespace scheduling but assert only public key/value reopen results at every fresh/active/recovery/cleanup checkpoint in `src/wal/durability_tests.rs`
- [X] T232 [US4] Use private namespace scheduling but assert only public key/set reopen results at every fresh/active/recovery/cleanup checkpoint in `src/wal/durability_tests.rs`
- [X] T233 [US4] Use private namespace scheduling but assert only public key/map reopen results at every fresh/active/recovery/cleanup checkpoint in `src/wal/durability_tests.rs`
- [X] T234 [US4] Assert public fresh, active-authority, and recovery-authority construction/reopen outcomes after every scheduled failure/crash in `src/wal/durability_tests.rs`
- [X] T235 [US4] Assert public cleanup outcomes after removal/barrier failures while the scheduled durable image retains new active authority in `src/wal/durability_tests.rs`
- [X] T236 [US4] Run all US4 public publication, authority, cleanup, capability, issue #1 recovery, and issue #4 repair/reopen targets and record the GREEN checkpoint in `specs/005-durable-write-policy/quickstart.md`

**Checkpoint**: Public physical startup success survives loss of unbarriered
namespace state and every failure preserves a complete authority.

---

## Phase 7: Polish and Cross-Cutting Acceptance Gates

**Purpose**: Complete platform evidence, traceability, quality gates, and the
approved quiet-machine performance acceptance run.

- [X] T237 [P] Add Linux/macOS physical capability/full durability lanes plus Windows explicit-unsupported/buffered lanes in `.github/workflows/recovery.yml`
- [X] T238 [P] Complete rustdoc for guarantees, platform/filesystem rejection, failure classification, fallible mutators, reopen outcomes, and publication authority in `src/config.rs`, `src/durability.rs`, `src/recovery.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, `src/key_map_store.rs`, and `src/lib.rs`
- [X] T239 Complete executable FR-001–FR-030 and SC-001–SC-015 traceability with public evidence references in `tests/durable_write_policy/contract.rs`
- [X] T240 Run frozen fixtures and all issue #1 recovery, issue #2 compute, issue #3 ordering/performance-harness, and issue #4 truncation/migration/reopen suites, recording evidence in `specs/005-durable-write-policy/quickstart.md`
- [X] T241 Run deterministic same/different-shard progress, WAL order, blocked barrier, pending-callback async cancellation with zero persistence/publication and prompt same-key guard release, post-callback non-yielding persistence, callback, and deadlock conformance in `src/wal/durability_tests.rs`
- [X] T242 Refactor duplicate durability/error/publication code only while focused and accumulated tests remain GREEN in `src/durability.rs`, `src/wal/mod.rs`, `src/wal/recovery.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, and `src/key_map_store.rs`
- [X] T243 Run formatting, strict Clippy, all targets, doc tests, normal-build no-test-seam checks, and `git diff --check`, recording results in `specs/005-durable-write-policy/quickstart.md`
- [X] T244 Choose a unique candidate capture ID, pause for explicit user confirmation of a quiet-machine window, and record approval/provenance in `specs/005-durable-write-policy/benchmarks/attempts/<capture-id>.md`
- [X] T245 After T244 approval, run all 54 candidate cells on the same explicit filesystem root and save untouched samples in `specs/005-durable-write-policy/benchmarks/attempts/<capture-id>.csv`
- [X] T246 Evaluate every cell and record verdicts in `specs/005-durable-write-policy/benchmarks/attempts/<capture-id>.md`; if any gate fails, preserve the attempt and write/run one focused runtime performance RED in `tests/durable_write_policy/performance.rs`
- [X] T247 After the focused physical RED remained failing under a same-scope mutex experiment, revert that experiment, write/run a scheduler RED proving a fast worker can lap slower workers, add per-operation eight-worker rendezvous in `tests/durable_write_policy/performance.rs`, and rerun the focused physical comparison GREEN without production changes or threshold relaxation
- [X] T248 Amend `spec.md`, `research.md`, `plan.md`, `contracts/performance.md`, `quickstart.md`, benchmark provenance, and this task plan to define protocol-v2 scheduling, invalidate but preserve protocol-v1 evidence, and require complete affected comparator recapture from the pre-feature commit
- [X] T249 Preserve protocol-v1 comparator files/reports, prepare and verify a clean protocol-v2 comparator runner over commit `6d7edc7c29a60a94c59effeeb2b78d8b95038135`, choose unique comparator/candidate capture IDs, then pause for one new explicit quiet-machine approval covering the complete protocol-v2 capture window
- [X] T250 After T249 approval, capture all 36 buffered and 18 direct-barrier reference cells from the pre-feature tree plus all 54 candidate cells from this worktree on the same root/toolchain/machine; preserve every raw file and return to per-cell T246 evaluation without merging protocol versions
- [X] T251 Evaluate the complete protocol-v2 attempt, preserve its `52/54` verdict and focused buffered reproducibility diagnostics, obtain approval for protocol v3, then RED–GREEN a policy-selected scheduler in `tests/durable_write_policy/benchmark_protocol.rs`: start-only for buffered and per-operation for physical/reference
- [X] T252 Amend `spec.md`, `research.md`, `plan.md`, `contracts/performance.md`, `quickstart.md`, benchmark provenance, and this task plan for protocol v3; verify and select the immutable protocol-v1 buffered baseline plus protocol-v2 physical reference, choose `candidate-protocol-v3-20260807-132706`, and pause for a new explicit quiet-machine approval without starting timed capture
- [X] T253 After T252 approval, reverify selected comparator and runner checksums and execute all 54 policy-scheduled cells; preserve the `ReadOnlyFilesystem` final-output failure and absence of raw/verdict evidence in `benchmarks/attempts/protocol-v3-20260807-132706.md` without reconstructing lost rows
- [X] T254 Prepare `candidate-protocol-v3-20260807-145653` to write first to a unique `/tmp` CSV, require row/checksum validation before one authorized repository copy, verify identity after import, and pause for a new explicit quiet-machine approval without rerunning timed cells
- [X] T255 After T254 approval, reverify both output paths plus selected comparator/runner checksums, capture all 54 cells, preserve/import the raw attempt byte-for-byte, evaluate every cell independently, and retain the `40/54` failed verdict without merging comparator samples
- [X] T256 Write/run the focused worst buffered RED, confirm it across clean processes, revert the unsuccessful force-inline experiment, and run the identical current-machine pre-feature diagnostic proving the immutable start-only baseline drifted while candidate-versus-contemporaneous-pre-feature ratios pass
- [X] T257 Resolve the buffered acceptance protocol using the preserved candidate and clean contemporaneous pre-feature evidence; obtain explicit approval for protocol v4's fixed `taskset -c 12-19` affinity, same-window complete recapture, unchanged policy-selected scheduling, and unchanged thresholds without tuning production code
- [X] T258 Amend `spec.md`, `research.md`, `plan.md`, `contracts/performance.md`, `quickstart.md`, benchmark provenance, and this task plan for protocol v4; prepare and validate a clean pre-feature comparator runner, choose `20260807-155736` write-once IDs and `/tmp` outputs, then pause for a distinct quiet-machine approval without starting timed capture
- [X] T259 After T258 quiet-machine approval, reverify topology/affinity, absent outputs, checksums, benchmark root, toolchain, and machine state; capture all 36 pinned pre-feature buffered cells, all 18 pinned pre-feature reference cells, and all 54 pinned candidate cells in that order; validate/import each raw file byte-for-byte and evaluate every cell without merging attempts, preserving the resulting `50/54` failed verdict
- [X] T260 After one complete attempt passes, copy its paired CSV byte-for-byte to `specs/005-durable-write-policy/benchmarks/final.csv` and generate the 72-row writes/second, p95, ratio, verdict, provenance, checksum, passing-ID, and failed-attempt report in `specs/005-durable-write-policy/benchmarks/final.md`
- [X] T261 Execute every command/scenario in `specs/005-durable-write-policy/quickstart.md` from a clean build state and record final pass/fail evidence in that file
- [X] T262 Audit `specs/005-durable-write-policy/{spec,plan,research,data-model,quickstart,tasks}.md`, `specs/005-durable-write-policy/contracts/*.md`, and `specs/005-durable-write-policy/benchmarks/*.md` against implementation, confirming no format change, downgrade, global mutation mutex, unsafe code, dependency, group commit, or weakened threshold
- [X] T263 RED–GREEN the AB/BA diagnostic protocol, link pre-feature and candidate crates in one pinned process, measure the four protocol-v4 eight-worker vector failures with per-operation CPU and paired sample-distribution evidence, and preserve the conclusion that no stable candidate-only regression was proven without changing production code or thresholds
- [X] T264 Resolve the acceptance protocol using T263 evidence and obtain explicit user approval before specifying or preparing a protocol-v5 complete same-process counterbalanced capture; do not promote focused diagnostic rows or reuse protocol-v4 comparators
- [X] T265 RED–GREEN the complete 54-comparison protocol-v5 matrix, prepare and validate one dual-version release runner with five warmup plus eleven measured AB/BA pairs per comparison, amend all specifications/contracts/provenance, reserve `protocol-v5-20260807-220443`, verify absent write-once outputs and immutable checksums, then pause for a distinct quiet-machine approval without starting timed capture
- [X] T266 After T265 quiet-machine approval, reverify topology/affinity, absent outputs, checksums, benchmark root, toolchain, and machine state; run the one-process 1,188-row paired capture once, validate/import it byte-for-byte, and evaluate all 54 comparisons independently without reusing prior protocol samples

---

## Dependencies and Execution Order

### Phase Dependencies

- **Phase 1** has no dependencies and freezes comparator evidence before production edits.
- **Phase 2** depends on Phase 1 and is the blocking private exposure gate. Its subsections are sequential; only tasks explicitly marked `[P]` may overlap.
- **US1 (Phase 3)** depends on T180 and promotes successful physical acknowledgement.
- **US2 (Phase 4)** depends on T180 and US1 public construction; it promotes support/failure types and fallible mutation adapters.
- **US3 (Phase 5)** depends on the public adapters it characterizes, while its private buffered characterization already ran before physical implementation in T023–T025.
- **US4 (Phase 6)** depends on T180 and US1 public construction; it supplies public evidence over the already-GREEN private publication system.
- **Phase 7** depends on all four user stories. T244/T245 form the initial user-controlled capture gate. Protocol-v1 and protocol-v2 failures proceed through T246–T251. T252 approval gates the failed T253 execution; T254 approval gates T255; T256 diagnoses start-only environment drift; T257 records protocol-v4 design approval; T258 preparation pauses for distinct quiet-machine approval before T259 capture. The failed T259 attempt proceeds through completed T263 diagnosis and T264 protocol approval to T265 preparation, which must pause for distinct quiet-machine approval before T266 or T260–T262 finalization.

### Within Every RED–GREEN Lane

1. Write and run the named behavior test and observe its expected runtime RED.
2. Implement only the immediately following GREEN task.
3. Run the focused test and accumulated relevant module/integration suite.
4. Refactor only while GREEN.

A compilation error, placeholder assertion, unrelated failure, or test failing
before the named checkpoint is not valid RED. Characterization and public adapter
tasks are explicitly first-execution-GREEN and must not be made to fail.

### User Story Dependencies and Smallest Safe Increment

- **US1** supplies public physical success after the complete private gate.
- **US2** depends on US1 construction to exercise public failures.
- **US3** is independently verifiable with pre-feature APIs but remains a release gate.
- **US4** depends on US1 construction to exercise public startup/reopen behavior.

There is no releasable US1-only MVP. The smallest safe functional increment is
Phase 1 + Phase 2 + US1 + US2 + US4; US3 compatibility and all Phase 7 quality/
performance gates are still mandatory before release acceptance.

## Parallel Execution Examples

```text
Setup after T001: T002 can proceed while T003–T005 build test drivers; T006 and T007 remain serialized on one benchmark machine.
Foundation: T014 can proceed beside T011–T013; otherwise preserve the written RED → immediate GREEN sequence.
Private gate after T174: T175–T180 remain sequential because they share `src/wal/durability_tests.rs`.
US1 after T184: T185 || T186 || T187, then T188–T190.
US2 after T214: T215–T220 remain sequential because deterministic fault scheduling stays in crate-unit tests.
US3 after T224: T225 || T226 || T227, then T228–T230.
US4: T231–T236 remain sequential because deterministic namespace scheduling stays in crate-unit tests.
Polish: T237 || T238, then T239–T243; T244 approval gates T245; the preserved protocol-v1 failure proceeds through T246–T248; T249 approval gates T250; protocol-v2 diagnosis and protocol-v3 preparation proceed through T251–T252; T252 approval gates T253; T254 approval gates T255; T256 evidence leads to T257 protocol-v4 approval and T258 preparation; T258 pauses for quiet-machine approval before T259; completed T263 diagnosis and T264 approval lead to T265 protocol-v5 preparation, which pauses for quiet-machine approval before T266 and T260–T262.
```

## Implementation Strategy

1. Freeze buffered and reference performance evidence.
2. Build test-only durable byte/namespace models.
3. Characterize existing buffered behavior GREEN before physical production work.
4. Complete private direct barriers, rollback/fail-closed behavior, visibility,
   fallible cores, capability preflights, publication, cleanup, and crash models.
5. Pass T180 before exposing any public physical configuration or construction.
6. Promote public success, failure, compatibility, and publication evidence one
   adapter/family at a time.
7. Run platform, compatibility, quality, and user-approved performance gates.

## Completion Rules

- No new production behavior precedes its valid runtime RED.
- Existing compatibility and issue #4 behavior uses first-execution-GREEN evidence.
- No public physical adapter exists before T180.
- One logical physical mutation receives exactly one direct barrier; no barrier is shared across calls.
- Only successful truncate plus rollback `sync_all` produces `Rejected`; either rollback-step failure produces `Indeterminate` and fail-closed health.
- Parent-directory preflight precedes staging; every preflight failure is a structured support error; later failures are ordinary path-aware I/O.
- Cleanup never removes the last complete authority.
- Buffered paths perform no new barrier or capability probe.
- Every one of the 54 performance comparisons must pass independently.
