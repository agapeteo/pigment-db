# Tasks: Consistent Concurrent Mutation Ordering

**Input**: Design documents from `specs/003-fix-mutation-ordering/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/concurrent-mutation-ordering.md`, `quickstart.md`

**Tests**: Mandatory. Root `AGENTS.md` requires RED–GREEN TDD. Every release-code change below is preceded by one focused failing test, followed by the minimum implementation and a targeted GREEN run.

**Organization**: Tasks are grouped by user story. Private deterministic controls stay in crate-unit tests; external integration tests use only the exported public API.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel because it uses different files and has no dependency on an incomplete task in the same lane.
- **[Story]**: Maps the task to User Story 1, 2, or 3.
- Every task names at least one exact file that it creates, edits, validates, or uses to record results.

---

## Phase 1: Setup (Shared Test and Baseline Infrastructure)

**Purpose**: Create the test layout and capture an immutable pre-feature performance/memory baseline before release mutation paths change.

- [X] T001 Enable DashMap 3.11.10 `raw-api` for test builds only by adding a matching dev dependency without changing the runtime dependency in `Cargo.toml`
- [X] T002 [P] Create the crate-unit support/module files `src/test_support/{mod,mutation_schedule,shard_keys,fault_writer}.rs`, `src/mutation_ordering_tests/{key_value,key_set,key_map,cross_shard}.rs`, and `src/wal/ordering_tests.rs`
- [X] T003 Create the public-only integration root and module files `tests/mutation_ordering.rs` and `tests/mutation_ordering/{support,key_value,key_set,key_map,compatibility,traceability,conformance,performance}.rs`
- [X] T004 Implement an ignored standard-library paired benchmark harness with 5 warmups, at least 11 measured samples, fixed 32-byte data, median throughput, per-call p95 latency, all 36 SC-004 cells, and 1k/1m create-delete memory profiles in `tests/mutation_ordering/performance.rs`
- [X] T005 Run the release benchmark harness against the unmodified release code, assert exactly 36 uniquely keyed performance rows plus memory rows, and capture environment, `rustc -Vv`, commit, binary working-diff checksum, OS/CPU/filesystem metadata, raw rows, and output schema in `specs/003-fix-mutation-ordering/benchmarks/{README,pre-feature}.md` and `specs/003-fix-mutation-ordering/benchmarks/pre-feature.csv`
- [X] T006 Run `cargo test --all-targets --all-features -- --test-threads=1` and `cargo fmt --check`, then record the unchanged baseline or discovered variance in `specs/003-fix-mutation-ordering/quickstart.md`

**Checkpoint**: The baseline is immutable and reproducible before ordering work begins.

---

## Phase 2: Foundational (Blocking Deterministic Test Support)

**Purpose**: Add private semantic scheduling, fault injection, and public-state assertions needed by every story.

**CRITICAL**: No user-story release-code change begins until test support compiles only under `cfg(test)` and the original suite remains GREEN.

- [X] T007 Implement labeled one-shot RAII gates for `AcceptanceEntered`, `AcceptedBeforePublication`, and `Published`, plus bounded watchdog and child-process checkpoint primitives, in `src/test_support/mutation_schedule.rs`
- [X] T008 [P] Implement opaque deterministic same-shard/different-shard key selection with DashMap `determine_map`, without exposing shard numbers or hash formulas, in `src/test_support/shard_keys.rs`
- [X] T009 [P] Implement scripted explicit write/flush errors, exact-byte checkpoints, rollback errors, and writer-call counters in `src/test_support/fault_writer.rs`
- [X] T010 Register `src/test_support/mod.rs` only under `cfg(test)` and keep every helper crate-private in `src/lib.rs`
- [X] T011 [P] Implement public snapshot, key-existence, bounded-channel watchdog, and three-consecutive-reopen assertion helpers without private imports in `tests/mutation_ordering/support.rs`
- [X] T012 [P] Add a test-only observer field/defaults, semantic lifecycle notifications, and the child test-module include without widening visibility in `src/key_value_store.rs`
- [X] T013 [P] Add a test-only observer field/defaults, semantic lifecycle notifications, and the child test-module include without widening visibility in `src/key_set_store.rs`
- [X] T014 [P] Add a test-only observer field/defaults, semantic lifecycle notifications, and the child test-module include without widening visibility in `src/key_map_store.rs`
- [X] T015 Register each `src/mutation_ordering_tests/{key_value,key_set,key_map}.rs` file as a child of its owning store module, expose shared schedules from `src/mutation_ordering_tests/cross_shard.rs` only inside those child modules, and register `src/wal/ordering_tests.rs` from `src/wal/mod.rs`
- [X] T016 Run all unit, integration, compute-persistence, and recovery targets to prove the private seams have zero release effect, recording the foundation checkpoint in `specs/003-fix-mutation-ordering/quickstart.md`

**Checkpoint**: Deterministic controls are crate-unit-only; public integration tests depend exclusively on exported behavior.

---

## Phase 3: User Story 1 - Same-Key Mutations Stay Consistent (Priority: P1) - Core Correctness Checkpoint

**Goal**: Give every successful same-key mutation one order shared by durable acceptance, live publication, and all reopenings.

**Independent Test**: Force ordinary/ordinary and ordinary/compute overlaps for every store, then require exact live state to equal each of three consecutive reopened states. Completion-before-invocation order is preserved; overlapping calls may use either internally consistent order.

### Key/Value RED-GREEN Lane

- [X] T017 [US1] Write and run a deterministic file-backed `put` versus `put` test that parks A after acceptance, proves the current live/reopen inversion, and maps to CMO-ORDER-2 in `src/mutation_ordering_tests/key_value.rs`
- [X] T018 [US1] Make `put` acquire and retain the DashMap entry/shard before WAL acceptance, publish through that entry, then run T017 and accumulated key/value tests GREEN in `src/key_value_store.rs`
- [X] T019 [US1] Write and run a same-key `set_number` versus `put` test that fails for the current WAL-first path and maps to CMO-ORDER-2 in `src/mutation_ordering_tests/key_value.rs`
- [X] T020 [US1] Convert `set_number` to the shard-first acceptance/publication lifecycle, then run T019 and accumulated key/value tests GREEN in `src/key_value_store.rs`
- [X] T021 [US1] Write and run an absent `remove` versus `put` deletion/recreation test that exposes delete-acceptance/live-removal inversion and maps to CMO-ORDER-2 in `src/mutation_ordering_tests/key_value.rs`
- [X] T022 [US1] Convert key/value `remove` to occupied/vacant Entry coordination while preserving absent-delete and panic compatibility, then run T021 and accumulated key/value tests GREEN in `src/key_value_store.rs`
- [X] T023 [US1] Add and run public-state/reopen regression cases for `put→compute`, `compute→put`, `set_number→increment_or_init`, and `increment_or_init→decrement` without changing numeric/compute semantics in `tests/mutation_ordering/key_value.rs`

### Key/Set RED-GREEN Lane

- [X] T024 [P] [US1] Write and run a deterministic file-backed `append` versus removal of the same existing member test that records the current WAL/live inversion as CMO-ORDER-2 RED in `src/mutation_ordering_tests/key_set.rs`
- [X] T025 [US1] Convert set `append` to occupied/vacant Entry-first acceptance and publication, then run T024 and accumulated set tests GREEN in `src/key_set_store.rs`
- [X] T026 [US1] Write and run a `remove_from_set` versus compute test covering a final-member transition and recording split removal/delete ordering as CMO-ORDER-2 RED in `src/mutation_ordering_tests/key_set.rs`
- [X] T027 [US1] Guard before presence inspection, accept one outer-key delete for a final member or one member removal otherwise, publish afterward, then run T026 GREEN in `src/key_set_store.rs`
- [X] T028 [US1] Write and run a callback-removal ordering/progress test that fails if acceptance is WAL-first or the callback runs while the shard is guarded, covering CMO-CALL-3 in `src/mutation_ordering_tests/key_set.rs`
- [X] T029 [US1] Convert `remove_from_set_callback` to entry-first acceptance and invoke the existing callback only after entry removal releases the guard, then run T028 GREEN in `src/key_set_store.rs`
- [X] T030 [US1] Write and run a `remove_key` versus `append` recreation test that exposes durable/live divergence as CMO-ORDER-2 RED in `src/mutation_ordering_tests/key_set.rs`
- [X] T031 [US1] Convert set `remove_key` to occupied/vacant Entry coordination with durable delete before live removal, then run T030 and accumulated set tests GREEN in `src/key_set_store.rs`
- [X] T032 [US1] Add and run deterministic set cases for ordinary versus sync compute in both directions, eligible conditional variants, sync versus async compute, skipped/no-op outcomes, and an indivisible multi-action batch for CMO-ORDER-3 in `src/mutation_ordering_tests/key_set.rs`

### Key/Sorted-Map RED-GREEN Lane

- [X] T033 [P] [US1] Write and run a deterministic file-backed replacement `put` versus `put` test that records current map WAL/live inversion as CMO-ORDER-2 RED in `src/mutation_ordering_tests/key_map.rs`
- [X] T034 [US1] Convert map `put` to occupied/vacant Entry-first acceptance and publication, then run T033 and accumulated map tests GREEN in `src/key_map_store.rs`
- [X] T035 [US1] Write and run a `remove_from_sorted_map` versus compute final-entry test that records split removal/delete behavior as CMO-ORDER-2 RED in `src/mutation_ordering_tests/key_map.rs`
- [X] T036 [US1] Accept one outer-key delete for a final entry or one map removal otherwise and publish only afterward, then run T035 GREEN in `src/key_map_store.rs`
- [X] T037 [US1] Write and run a callback-removal ordering/progress test that records RED if callback or durable/live order escapes the guarded lifecycle, covering CMO-CALL-3 in `src/mutation_ordering_tests/key_map.rs`
- [X] T038 [US1] Convert map callback removal to entry-first acceptance and callback-after-guard-release behavior, then run T037 GREEN in `src/key_map_store.rs`
- [X] T039 [US1] Write and run a `remove_key` versus `put` recreation test that exposes current durable/live divergence as CMO-ORDER-2 RED in `src/mutation_ordering_tests/key_map.rs`
- [X] T040 [US1] Convert map `remove_key` to occupied/vacant Entry coordination with delete accepted before live removal, then run T039 and accumulated map tests GREEN in `src/key_map_store.rs`
- [X] T041 [US1] Write and run a `pop_first` versus `put` ordering test that fails because live state changes before acceptance, asserting contents only and excluding issue #8 return semantics, in `src/mutation_ordering_tests/key_map.rs`
- [X] T042 [US1] Make `pop_first` identify/clone its candidate, accept removal or one final outer delete, and mutate live state afterward while preserving return semantics, then run T041 GREEN in `src/key_map_store.rs`
- [X] T043 [US1] Write and run the symmetric `pop_last` versus `put` ordering test as CMO-ORDER-2 RED without asserting issue #8 return semantics in `src/mutation_ordering_tests/key_map.rs`
- [X] T044 [US1] Apply accepted-before-publication sequencing to `pop_last`, preserve its current return semantics, then run T043 and accumulated map tests GREEN in `src/key_map_store.rs`
- [X] T045 [US1] Add and run deterministic map cases for ordinary versus compute in both directions, compute versus compute, conditional variants, `append_ordered_element`, skipped/no-op results, and an indivisible multi-action batch for CMO-ORDER-3 in `src/mutation_ordering_tests/key_map.rs`

### User Story 1 Public Integration

- [X] T046 [P] [US1] Add CMO-ORDER-1 completion-before-invocation and CMO-ORDER-2 overlapping-either-order cases for key/value, requiring live state and three reopenings to agree and rejecting FIFO assumptions, in `tests/mutation_ordering/key_value.rs`
- [X] T047 [P] [US1] Add CMO-ORDER-1 completion-before-invocation and CMO-ORDER-2 overlapping-either-order cases for key/set, requiring live state and three reopenings to agree and rejecting FIFO assumptions, in `tests/mutation_ordering/key_set.rs`
- [X] T048 [P] [US1] Add CMO-ORDER-1 completion-before-invocation and CMO-ORDER-2 overlapping-either-order cases for key/sorted-map, requiring live state and three reopenings to agree and rejecting FIFO assumptions, in `tests/mutation_ordering/key_map.rs`
- [X] T049 [US1] Complete the public fast family matrix so every exported mutator named in `contracts/concurrent-mutation-ordering.md` participates in at least one public-state/three-reopen case in `tests/mutation_ordering/{key_value,key_set,key_map}.rs`
- [X] T050 [US1] Run the complete fast same-key matrix and record CMO-ORDER-1 through CMO-ORDER-3 coverage, exact pass counts, and no public signature or pop-return change in `specs/003-fix-mutation-ordering/quickstart.md`

**Checkpoint**: User Story 1 independently proves one same-key durable/live/reopen order for every mutation family.

---

## Phase 4: User Story 2 - Unrelated Keys Remain Concurrent (Priority: P1)

**Goal**: Prove the fix reuses existing DashMap shard coordination rather than globally serializing mutations, and enforce the clarified performance/memory limits.

**Independent Test**: Park key A during preparation and accepted-before-publication; key B in another shard still reaches the shared WAL interval, while a same-shard control may wait. During WAL acceptance, other keys may prepare but cannot complete acceptance.

- [X] T051 [P] [US2] Add key/value different-shard progress tests at preparation and accepted-before-publication for CMO-CROSS-1 and CMO-CROSS-3 using opaque selected keys and bounded watchdogs in `src/mutation_ordering_tests/key_value.rs`
- [X] T052 [P] [US2] Add key/set different-shard progress tests at sync/async preparation and accepted-before-publication for CMO-CROSS-1 and CMO-CROSS-3 in `src/mutation_ordering_tests/key_set.rs`
- [X] T053 [P] [US2] Add key/sorted-map different-shard progress tests at preparation and accepted-before-publication for CMO-CROSS-1 and CMO-CROSS-3 in `src/mutation_ordering_tests/key_map.rs`
- [X] T054 [US2] Add all-store crate-unit cases proving a second different-shard mutation may prepare but cannot complete acceptance while the first scripted writer owns the WAL interval, covering CMO-CROSS-2 in `src/mutation_ordering_tests/{key_value,key_set,key_map}.rs`
- [X] T055 [US2] Add all-store same-shard/different-key controls proving waiting is permitted but state remains independent for CMO-CROSS-4, using shared schedules from `src/mutation_ordering_tests/cross_shard.rs` in `src/mutation_ordering_tests/{key_value,key_set,key_map}.rs`
- [X] T056 [US2] Run the fast cross-shard crate-unit target with `--test-threads=1` and record zero deadlocks/timeouts plus three-reopen parity in `specs/003-fix-mutation-ordering/quickstart.md`
- [X] T057 [US2] Implement and run the ignored deterministic 1,000-schedule key/value different-shard conformance lane with three reopenings after each accepted history in `src/mutation_ordering_tests/key_value.rs`
- [X] T058 [P] [US2] Implement and run the ignored deterministic 1,000-schedule key/set different-shard conformance lane with three reopenings after each accepted history in `src/mutation_ordering_tests/key_set.rs`
- [X] T059 [P] [US2] Implement and run the ignored deterministic 1,000-schedule key/sorted-map different-shard conformance lane with three reopenings after each accepted history in `src/mutation_ordering_tests/key_map.rs`
- [X] T060 [US2] Add a public-only normal concurrent-history smoke matrix that makes no exact-shard claim and checks live/three-reopen parity for every store in `tests/mutation_ordering/conformance.rs`
- [X] T061 [US2] Run the paired 1k/1m unique-key create-delete memory profile and fail if added retained ordering memory exceeds 110% of the immutable baseline in `tests/mutation_ordering/performance.rs`
- [X] T062 [US2] Run all 36 paired candidate cells against the stable reconstructed pre-feature baseline, failing each cell independently below 90% one-worker throughput, below 85% eight-worker throughput, or above 125% p95 latency in `tests/mutation_ordering/performance.rs`
- [X] T063 [US2] If and only if T061 or T062 fails, optimize measured key cloning, action preparation, or shard-guard scope without a lock registry or weaker thresholds, rerunning each affected RED cell GREEN in `src/key_value_store.rs`, `src/key_set_store.rs`, or `src/key_map_store.rs`
- [X] T064 [US2] Save post-ordering performance/memory rows, paired ratios, and host metadata in `specs/003-fix-mutation-ordering/benchmarks/post-ordering.csv` and `specs/003-fix-mutation-ordering/benchmarks/post-ordering.md`
- [X] T065 [US2] Record CMO-CROSS-1 through CMO-CROSS-4 results, 3,000 controlled schedules, public smoke results, and all performance/memory thresholds in `specs/003-fix-mutation-ordering/quickstart.md`

**Checkpoint**: User Story 2 proves different-shard progress, bounded shared-WAL serialization, bounded added memory, and acceptable performance.

---

## Phase 5: User Story 3 - Failures and Boundary Transitions Remain Safe (Priority: P2)

**Goal**: Rejected, abandoned, skipped, deleted, and recreated mutations publish no inconsistent state, release coordination, and leave the WAL usable unless rollback itself fails.

**Independent Test**: Inject write/flush/rollback failures, callback panic/cancellation, reads during each semantic boundary, and process exits at each prefix checkpoint; verify the specified prior or accepted prefix state and subsequent progress.

### WAL Rejection and Fail-Closed RED-GREEN Lane

- [X] T066 [US3] Write and run an explicit write-`Err` test after earlier record segments have progressed, asserting exact pre-record bytes/offset, unlocked WAL, and CMO-FAIL-1 RED in `src/wal/ordering_tests.rs`
- [X] T067 [US3] Implement a fallible segmented-record helper that restores the pre-record checkpoint and advances offsets only after complete acceptance, then run T066 and accumulated WAL tests GREEN in `src/wal/mod.rs`
- [X] T068 [US3] Write and run a distinct flush-`Err` checkpoint test asserting exact bytes/offset, no accepted action, and CMO-FAIL-2 RED in `src/wal/ordering_tests.rs`
- [X] T069 [US3] Add flush-error rollback to the fallible helper, return the rejection only after the WAL guard drops, then run T068 and accumulated WAL tests GREEN in `src/wal/mod.rs`
- [X] T070 [US3] Write and run a rollback-failure test that expects terminal WAL health, a composite error, no publication, and zero subsequent writer calls, recording CMO-FAIL-3 RED in `src/wal/ordering_tests.rs`
- [X] T071 [US3] Add the minimal constant-space `Ready`/`FailedRollback` WAL health state, preserve original and rollback causes, and fail closed before later writer access, then run T070 and WAL tests GREEN in `src/wal/mod.rs`
- [X] T072 [US3] Run accumulated write/flush/rollback rejection tests and record exact byte, offset, writer-call, unlock, and WAL-health evidence for CMO-FAIL-1 through CMO-FAIL-3 in `specs/003-fix-mutation-ordering/quickstart.md`

### Store Rejection RED-GREEN Lanes

- [X] T073 [US3] Write and run rejected key/value `put` and `remove` tests that catch compatibility panic, preserve prior live/reopened state, and prove later same-key progress, recording CMO-FAIL-4 RED in `src/mutation_ordering_tests/key_value.rs`
- [X] T074 [US3] Migrate key/value ordinary event calls to ownership-preserving fallible WAL helpers and panic only after all guards drop, then run T073 and accumulated key/value tests GREEN in `src/key_value_store.rs`
- [X] T075 [US3] Write and run key/set write- and flush-rejection tests covering final-member deletion, original live/reopened state, and later same-key progress, recording CMO-FAIL-4 RED in `src/mutation_ordering_tests/key_set.rs`
- [X] T076 [US3] Migrate set ordinary event calls to rollback-capable fallible helpers without changing compute result APIs, then run T075 and accumulated set/compute tests GREEN in `src/key_set_store.rs`
- [X] T077 [US3] Write and run key/sorted-map rejection tests covering `put`, final-entry removal, and pre-publication pops while excluding issue #8 return semantics, recording CMO-FAIL-4 RED in `src/mutation_ordering_tests/key_map.rs`
- [X] T078 [US3] Migrate map ordinary event calls to rollback-capable fallible helpers and keep pop live mutation after acceptance, then run T077 and accumulated map tests GREEN in `src/key_map_store.rs`

### Callback, Read-Visibility, and Process-Prefix Coverage

- [X] T079 [P] [US3] Add public integration cases asserting exact eligible/ineligible callback invocation counts and ordinary-versus-callback ordering for CMO-CALL-1 and CMO-CALL-2 across all stores in `tests/mutation_ordering/compatibility.rs`
- [X] T080 [P] [US3] Add a synchronous key/value compute panic case proving no publication, guard release, and later same-key progress for CMO-CALL-3 in `src/mutation_ordering_tests/key_value.rs`
- [X] T081 [P] [US3] Add synchronous panic and controlled async-cancellation cases proving no publication, guard release, and later same-key progress for CMO-CALL-3 in `src/mutation_ordering_tests/key_set.rs`
- [X] T082 [P] [US3] Add a synchronous key/sorted-map compute panic case proving no publication, guard release, and later same-key progress for CMO-CALL-3 in `src/mutation_ordering_tests/key_map.rs`
- [X] T083 [US3] Add callback-removal reentrancy/progress cases proving callback invocation occurs after shard-guard release without changing invocation counts in `src/mutation_ordering_tests/{key_set,key_map}.rs`
- [X] T084 [P] [US3] Add public-only set cases proving overlapping reads never expose compute working state, covering CMO-READ-1 in `tests/mutation_ordering/key_set.rs`
- [X] T085 [P] [US3] Add public-only sorted-map cases proving overlapping sync/async reads never expose compute working state, covering CMO-READ-1 in `tests/mutation_ordering/key_map.rs`
- [X] T086 [P] [US3] Add a key/value unit case proving a read initiated during accepted-before-publication may wait and then returns one complete published state for CMO-READ-2 in `src/mutation_ordering_tests/key_value.rs`
- [X] T087 [P] [US3] Add a key/set unit case proving a read initiated during accepted-before-publication may wait and then returns one complete published state for CMO-READ-2 in `src/mutation_ordering_tests/key_set.rs`
- [X] T088 [P] [US3] Add a key/sorted-map unit case proving a read initiated during accepted-before-publication may wait and then returns one complete published state for CMO-READ-2 in `src/mutation_ordering_tests/key_map.rs`
- [X] T089 [US3] Complete the subprocess runner, explicit child modes, bounded waits, exact file syncing, and parent-side reopen assertions used by every interruption case in `src/test_support/mutation_schedule.rs`
- [X] T090 [US3] Add a table-driven all-store exit-before-acceptance case asserting the prior durable prefix for CMO-PREFIX-1 in `src/mutation_ordering_tests/cross_shard.rs`
- [X] T091 [US3] Add a table-driven all-store exit-after-acceptance/before-publication case including a multi-action batch and asserting the accepted durable prefix for CMO-PREFIX-2 in `src/mutation_ordering_tests/cross_shard.rs`
- [X] T092 [US3] Add a table-driven all-store exit-after-publication case asserting accepted live/reopened state for CMO-PREFIX-3 in `src/mutation_ordering_tests/cross_shard.rs`
- [X] T093 [US3] Add a table-driven all-store exit-with-blocked-contender case asserting only the accepted prefix survives for CMO-PREFIX-4 in `src/mutation_ordering_tests/cross_shard.rs`
- [X] T094 [US3] Add and run public create/delete/recreate, absent removal, final-item deletion, conditional skip, exact no-op, binary-key, and compatibility edge cases one at a time, then record CMO-CALL, CMO-READ, CMO-PREFIX, and CMO-FAIL results in `tests/mutation_ordering/compatibility.rs` and `specs/003-fix-mutation-ordering/quickstart.md`

**Checkpoint**: User Story 3 independently proves failure-atomic publication, read visibility, interruption-prefix safety, and fail-closed rollback behavior.

---

## Phase 6: Polish and Cross-Cutting Validation

**Purpose**: Complete expensive conformance, traceability, documentation, compatibility, CI, and final performance gates after all behavior is GREEN.

- [X] T095 [P] Implement the ignored deterministic 10,000-controlled-history same-key key/value conformance lane with three reopenings after every accepted history in `src/mutation_ordering_tests/key_value.rs`
- [X] T096 [P] Implement the ignored deterministic 10,000-controlled-history same-key key/set conformance lane with three reopenings after every accepted history in `src/mutation_ordering_tests/key_set.rs`
- [X] T097 [P] Implement the ignored deterministic 10,000-controlled-history same-key key/sorted-map conformance lane with three reopenings after every accepted history in `src/mutation_ordering_tests/key_map.rs`
- [X] T098 Run all three release conformance lanes and record 30,000/30,000 successful histories, reopen counts, and duration in `specs/003-fix-mutation-ordering/quickstart.md`
- [X] T099 Add a public traceability manifest mapping every exported mutation family and every CMO identifier exactly once, rejecting missing or duplicate mappings, in `tests/mutation_ordering/traceability.rs`
- [X] T100 [P] Add the exact one-sentence public concurrency contract plus same-shard contention, lock-across-await/recursive-access, panic/cancellation, and unchanged-signature details in `src/key_value_store.rs`, `src/key_set_store.rs`, and `src/key_map_store.rs`
- [X] T101 Refactor duplicated entry-first preparation/publication and WAL error handling only while targeted suites remain GREEN, rerunning the affected target after each refactor in `src/key_value_store.rs`, `src/key_set_store.rs`, `src/key_map_store.rs`, and `src/wal/mod.rs`
- [X] T102 [P] Add the fast `mutation_ordering` and WAL rejection targets to Linux, macOS, and Windows CI without enabling ignored conformance/benchmarks in `.github/workflows/recovery.yml`
- [X] T103 Run frozen-fixture checksum verification, public recovery, WAL recovery, and compute-persistence suites and record unchanged compatibility results in `specs/003-fix-mutation-ordering/quickstart.md`
- [X] T104 Run `cargo test --all-targets --all-features -- --test-threads=1`, `cargo fmt --check`, strict Clippy, and documentation builds; distinguish pre-existing diagnostics and fix every feature-introduced diagnostic in `specs/003-fix-mutation-ordering/quickstart.md`
- [X] T105 Rerun all 36 paired performance cells and retained-memory gates after WAL changes, require every threshold GREEN, and save final rows and ratios in `specs/003-fix-mutation-ordering/benchmarks/final.csv` and `specs/003-fix-mutation-ordering/benchmarks/final.md`
- [X] T106 Audit `contracts/concurrent-mutation-ordering.md`, `data-model.md`, and `quickstart.md` against implementation and tests; confirm no public test hooks, global mutation lock, per-key registry, new WAL action, async conflict redesign, or issue #8 return fix in `specs/003-fix-mutation-ordering/quickstart.md`

---

## Dependencies and Execution Order

### Phase Dependencies

- **Phase 1 - Setup**: Starts immediately. T004 depends on T002-T003; T005-T006 finish before release mutation edits.
- **Phase 2 - Foundational**: Depends on Phase 1. T008-T009 and T011-T014 may run in parallel after T007; T015-T016 complete the gate.
- **Phase 3 - User Story 1**: Depends on Phase 2. The key/value, key/set, and key/map lanes may proceed in parallel, but each RED-GREEN pair within a lane is sequential.
- **Phase 4 - User Story 2**: Depends on User Story 1 because it validates final guard scope. T051-T053 and T057-T059 may run in parallel; controlled performance work is serial.
- **Phase 5 - User Story 3**: Depends on User Story 1's accepted-before-publication lifecycle. T066-T071 are blocking; the three store rejection lanes then proceed independently; callback/read lanes may run in parallel.
- **Phase 6 - Polish**: Depends on all selected stories. Conformance lanes, documentation, and CI can be prepared in parallel; final test/performance runs are serial.

### User Story Dependencies

- **User Story 1 (P1)**: No story dependency after Foundational; independently delivers same-key ordering correctness.
- **User Story 2 (P1)**: Requires User Story 1's final shard scope; independently proves the absence of global serialization and enforces performance/memory gates.
- **User Story 3 (P2)**: Requires User Story 1's publish-after-accept lifecycle; independently validates rejection, abandonment, read boundaries, and interrupted prefixes.

### RED-GREEN Discipline

1. Write one behavior-focused test for the next production behavior.
2. Run that exact test and verify failure for the intended invariant, not unrelated compilation noise.
3. Make the minimum release-code change needed for that test.
4. Run the exact test and accumulated affected-store suite GREEN.
5. Refactor only while GREEN; never begin the next RED with a failing suite.

---

## Parallel Execution Examples

### User Story 1 Store Lanes

```text
Lane A: T017 → T018 → T019 → T020 → T021 → T022 → T023
Lane B: T024 → T025 → T026 → T027 → T028 → T029 → T030 → T031 → T032
Lane C: T033 → T034 → T035 → T036 → T037 → T038 → T039 → T040 → T041 → T042 → T043 → T044 → T045
Join:   T046 || T047 || T048 → T049 → T050
```

### User Story 2 Progress and Conformance

```text
Progress:    T051 || T052 || T053 → T054 → T055 → T056
Conformance: T057 || T058 || T059 → T060 → T061 → T062 → T063 → T064 → T065
```

### User Story 3 Store and Visibility Lanes

```text
WAL:        T066 → T067 → T068 → T069 → T070 → T071 → T072
Stores:     T073 → T074 || T075 → T076 || T077 → T078
Callbacks:  T079 || T080 || T081 || T082 → T083
Reads:      T084 || T085 || T086 || T087 || T088
Prefixes:   T089 → T090 → T091 → T092 → T093 → T094
```

---

## Implementation Strategy

### Suggested MVP

The first useful correctness checkpoint is Setup + Foundational + User Story 1. Because avoiding global serialization is also P1, the safe release MVP is Setup + Foundational + User Stories 1 and 2, including their performance and memory gates. Stop for review there before P2 failure-boundary hardening.

### Incremental Delivery

1. **Setup + Foundation**: Capture the baseline and enable private deterministic scheduling.
2. **User Story 1**: Converge same-key live and durable order for every mutation family.
3. **User Story 2**: Prove unrelated-key progress and protect throughput, latency, and memory.
4. **User Story 3**: Make failure, read, callback, and interruption boundaries safe.
5. **Polish**: Complete conformance, traceability, CI, compatibility, docs, and final benchmarks.

### Completion Rule

- A story is independently complete only when its stated independent test passes without relying on later stories.
- An implementation task is complete only with its preceding RED evidence and its targeted plus accumulated GREEN evidence.
- Ignored conformance and performance targets remain explicit release gates; they are not part of the default fast suite.
- Any task that reveals a new contract decision returns to `spec.md` and `plan.md` before implementation continues.
