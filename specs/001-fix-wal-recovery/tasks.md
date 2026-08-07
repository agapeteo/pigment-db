# Tasks: Crash-Safe WAL Recovery

**Input**: Design documents from `specs/001-fix-wal-recovery/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [recovery-api.md](contracts/recovery-api.md), [quickstart.md](quickstart.md)

**Tests**: Required by root `AGENTS.md`. Execute one behavior-focused RED test followed by its minimal GREEN implementation; do not batch all tests before implementation.

**Organization**: Tasks are grouped by user story. Every user-story task includes its story label and an exact file path.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with the other indicated stream after its own prerequisites are complete
- **[Story]**: Maps work to US1, US2, or US3 from [spec.md](spec.md)
- Test tasks explicitly require observing the expected RED failure before the paired implementation task begins

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare isolated real-filesystem tests and the planned module layout without changing recovery behavior.

- [X] T001 Add `tempfile = "3"` under `[dev-dependencies]` in `Cargo.toml` and verify the dependency resolves with `cargo test --no-run`
- [X] T002 [P] Capture frozen pre-feature WAL fixtures and expected logical contents in `tests/fixtures/legacy/kv.wal.dat`, `tests/fixtures/legacy/set.wal.dat`, `tests/fixtures/legacy/map.wal.dat`, and `tests/fixtures/legacy/README.md` before changing the production writer
- [X] T003 Create the integration-test harness and isolated directory/log/file-snapshot helpers in `tests/recovery.rs` and `tests/recovery/support.rs`
- [X] T004 [P] Create the planned module skeletons and declarations in `src/recovery.rs`, `src/wal/replay.rs`, `src/wal/recovery.rs`, `src/lib.rs`, and `src/wal/mod.rs` without adding recovery behavior

**Checkpoint**: Fixtures are frozen, temporary directories are isolated, and source/test modules compile.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Establish shared contracts, checked frame parsing, append-ready WAL primitives, and pure artifact classification required by every story.

**⚠️ CRITICAL**: Complete this phase before any user-story work.

- [X] T005 Write one failing public contract test for `RecoveryStatus`, `RecoveryOutcome` accessors, structured error matching, and unchanged `init_new(&str) -> Self` signatures in `tests/recovery/contract.rs`; run it and record the expected RED compile/test failure
- [X] T006 Implement only the shared public recovery types, error/display/source behavior, accessors, and crate-root exports needed to make T005 GREEN in `src/recovery.rs` and `src/lib.rs`, then run the targeted and full relevant tests
- [X] T007 [P] Write one failing checked-frame test covering a valid frozen KV frame and a truncated frame without panic in `src/wal/replay.rs`; run it and record RED
- [X] T008 [P] Implement the minimal bounds-checked frame iterator, CRC/action validation, and typed validation error needed to make T007 GREEN in `src/wal/replay.rs`, then rerun parser tests
- [X] T009 [P] Write one failing test for fallible file creation, synchronization, and opening an existing WAL for append at its validated byte length in `src/wal/mod.rs`; run it and record RED
- [X] T010 [P] Implement the minimal fallible `WalStorage<File>` create/sync/open-for-append primitives needed to make T009 GREEN in `src/wal/mod.rs`, preserving vector-backed behavior
- [X] T011 Write one failing pure decision-table test for per-store artifact paths, active-only authority, zero-length active completeness, and staging never being authoritative in `src/wal/recovery.rs`; run it and record RED
- [X] T012 Implement `StoreKind`, `ArtifactPaths`, artifact observations, recovery decisions, and side-effect-free classification needed to make T011 GREEN in `src/wal/recovery.rs`

**Checkpoint**: Shared types and fallible low-level primitives are GREEN; no store initializer has changed yet.

---

## Phase 3: User Story 1 — Reopen After Interrupted Startup (Priority: P1) 🎯 MVP

**Goal**: A key/value store reopens with all acknowledged data when startup is interrupted before or after staged publication, while reporting `Normal` or `Recovered` correctly.

**Independent Test**: Populate a KV store, interrupt each new publication transition, reopen through `try_init_new`, and verify every expected key/value plus status; verify the compatibility initializer still returns the store and logs recovery.

### RED–GREEN tracer bullets

- [X] T013 [US1] Write one failing test that a fresh key/value `try_init_new` returns an empty usable store with `RecoveryStatus::Normal` in `tests/recovery/key_value.rs`; run it and record RED
- [X] T014 [US1] Implement the minimal fresh-store fallible KV initializer and outcome construction needed to make T013 GREEN in `src/key_value_store.rs` and `src/wal/recovery.rs`
- [X] T015 [US1] Write one failing test that the frozen pre-feature KV fixture opens with `Normal` and identical logical contents in `tests/recovery/key_value.rs`; run it and record RED
- [X] T016 [US1] Implement checked KV replay/snapshot adaptation and ordinary active-WAL initialization needed to make T015 GREEN in `src/key_value_store.rs` and `src/wal/replay.rs`
- [X] T017 [US1] Write one failing test that active KV data wins over an empty or frame-complete partial `.kv.wal.dat.next`, returns `Recovered`, and never promotes staging in `tests/recovery/key_value.rs`; run it and record RED
- [X] T018 [US1] Implement stale-staging inspection, non-destructive cleanup/defer behavior, and append-ready active opening needed to make T017 GREEN in `src/wal/recovery.rs` and `src/key_value_store.rs`
- [X] T019 [US1] Write one failing checkpoint matrix for interruption after staging creation, first/middle snapshot records, validation, synchronization, and publication rename in `src/wal/recovery.rs`; assert the next public KV open preserves the source snapshot and record RED
- [X] T020 [US1] Implement the minimal same-directory staging writer, logical validation, `sync_all`, handle closure, replacement rename, and deterministic recovery observer needed to make T019 GREEN in `src/wal/recovery.rs`, `src/wal/mod.rs`, and `src/key_value_store.rs`
- [X] T021 [US1] Write one failing test that `init_new` logs exactly on `Recovered`, remains source-compatible, and delegates errors to its compatibility panic behavior in `tests/recovery/key_value.rs`; run it and record RED
- [X] T022 [US1] Implement the KV compatibility wrapper and recovery logging needed to make T021 GREEN in `src/key_value_store.rs`
- [X] T023 [US1] Write one failing test for ten deterministic interrupted KV startups followed by one completed startup and three stable ordinary restarts in `tests/recovery/key_value.rs`; run it and record RED
- [X] T024 [US1] Implement the minimal idempotent staging retry and cleanup behavior needed to make T023 GREEN in `src/wal/recovery.rs`, then run the complete US1 test group

**Checkpoint**: US1 is independently usable as the MVP for key/value stores and all its tests are GREEN.

---

## Phase 4: User Story 2 — Resolve Multiple Recovery Candidates Safely (Priority: P2)

**Goal**: Legacy hidden backups and conflicting candidates resolve by replay provenance, or fail with a structured error while preserving every candidate byte.

**Independent Test**: Build each active/legacy artifact combination for KV, invoke `try_init_new`, and verify the selected logical state and status; ambiguous states must return `AuthorityUndetermined` with identical pre/post filenames and bytes.

### RED–GREEN tracer bullets

- [X] T025 [US2] Write one failing test that a valid `.kv.wal.dat` with no active WAL is recovered completely with `Recovered` in `tests/recovery/key_value.rs`; run it and record RED
- [X] T026 [US2] Implement legacy-recovery-only source selection and safe staged publication needed to make T025 GREEN in `src/wal/recovery.rs` and `src/key_value_store.rs`
- [X] T027 [US2] Write one failing test that a valid legacy KV recovery artifact beats a zero-length or physically truncated active artifact in `tests/recovery/key_value.rs`; run it and record RED
- [X] T028 [US2] Implement the minimal incomplete-active classification and legacy selection needed to make T027 GREEN in `src/wal/replay.rs` and `src/wal/recovery.rs`
- [X] T029 [US2] Write one failing test that logically equal active and legacy KV artifacts select active, clean or defer the stale legacy file, and return `Recovered` in `tests/recovery/key_value.rs`; run it and record RED
- [X] T030 [US2] Implement logical snapshot equality classification and post-authority legacy cleanup needed to make T029 GREEN in `src/wal/recovery.rs`
- [X] T031 [US2] Write one failing test that active KV replay reaching the legacy state at a frame boundary and then applying later overwrite/delete actions selects active in `tests/recovery/key_value.rs`; run it and record RED
- [X] T032 [US2] Implement frame-boundary logical history and completed-replay provenance detection needed to make T031 GREEN in `src/wal/replay.rs` and `src/wal/recovery.rs`
- [X] T033 [US2] Write one failing test that a valid compacted-snapshot proper prefix of the legacy KV state selects legacy in `tests/recovery/key_value.rs`; run it and record RED
- [X] T034 [US2] Implement snapshot-action prefix validation and interrupted-replay selection needed to make T033 GREEN in `src/wal/recovery.rs`
- [X] T035 [US2] Write one failing test that two valid but unprovable KV candidates return `RecoveryError::AuthorityUndetermined` and preserve a sorted filename-to-bytes snapshot exactly in `tests/recovery/key_value.rs`; run it and record RED
- [X] T036 [US2] Implement the preserve-all conflict path and path-rich structured error needed to make T035 GREEN in `src/wal/recovery.rs` and `src/recovery.rs`
- [X] T037 [US2] Write one failing test that stale-legacy cleanup failure still returns a usable `Recovered` active store, logs a warning, and skips compaction so provenance remains available in `src/wal/recovery.rs`; run it and record RED
- [X] T038 [US2] Implement the narrow cleanup fault seam, non-fatal cleanup handling, and open-without-compaction branch needed to make T037 GREEN in `src/wal/recovery.rs` and `src/key_value_store.rs`
- [X] T039 [US2] Write one failing test that required inspect/open/create/publish filesystem failures return `RecoveryError::Io` with the correct `RecoveryOperation`, path, and source in `tests/recovery/key_value.rs`; run it and record RED
- [X] T040 [US2] Propagate and map required filesystem failures without panic to make T039 GREEN in `src/wal/recovery.rs`, `src/wal/mod.rs`, and `src/recovery.rs`, then run the complete US2 test group

**Checkpoint**: US2 is independently verifiable through the KV artifact decision table, including safe failure and byte preservation.

---

## Phase 5: User Story 3 — Consistent Recovery Across Store Types (Priority: P3)

**Goal**: Key/set and key/sorted-map stores use the same recovery state machine, statuses, conflicts, and compatibility guarantees as KV without cross-store interference.

**Independent Test**: Run meaningful set and sorted-map histories through the legacy and staged-interruption matrices, compare logical snapshots, and show that a conflict for one store kind does not block another kind in the same directory.

### Parallel RED–GREEN adapter streams

- [X] T041 [P] [US3] Write one failing set-store parity test using `tests/fixtures/legacy/set.wal.dat`, duplicate append/removal history, and the legacy decision matrix in `tests/recovery/key_set.rs`; run it and record RED
- [X] T042 [P] [US3] Implement only the set replay/snapshot adapter and fallible/compatibility initializer pair needed to make T041 GREEN in `src/key_set_store.rs`
- [X] T043 [P] [US3] Write one failing set-store staged-publication interruption matrix using meaningful multi-key membership state in `tests/recovery/key_set.rs`; run it and record RED
- [X] T044 [P] [US3] Route set snapshot writing and recovery through the shared coordinator to make T043 GREEN in `src/key_set_store.rs`
- [X] T045 [P] [US3] Write one failing sorted-map parity test using `tests/fixtures/legacy/map.wal.dat`, overwrite/removal history, and the legacy decision matrix in `tests/recovery/key_map.rs`; run it and record RED
- [X] T046 [P] [US3] Implement only the sorted-map replay/snapshot adapter and fallible/compatibility initializer pair needed to make T045 GREEN in `src/key_map_store.rs`
- [X] T047 [P] [US3] Write one failing sorted-map staged-publication interruption matrix using multiple outer and search keys in `tests/recovery/key_map.rs`; run it and record RED
- [X] T048 [P] [US3] Route sorted-map snapshot writing and recovery through the shared coordinator to make T047 GREEN in `src/key_map_store.rs`
- [X] T049 [US3] Write one failing cross-store test for intentionally empty active WALs and for a KV conflict not blocking independent set/map initialization in `tests/recovery/cross_store.rs`; run it and record RED
- [X] T050 [US3] Implement per-`StoreKind` path isolation and consistent empty-state handling needed to make T049 GREEN in `src/wal/recovery.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, and `src/key_map_store.rs`, then run the complete US3 test group

**Checkpoint**: All three user stories and all three durable store types are independently GREEN.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final documentation, cross-platform automation, and whole-feature validation after all desired stories are GREEN.

- [X] T051 [P] Add Rustdoc for recovery statuses, error preservation guarantees, fallible initializers, compatibility wrappers, and scope boundaries in `src/recovery.rs`, `src/key_value_store.rs`, `src/key_set_store.rs`, and `src/key_map_store.rs`
- [X] T052 [P] Add Linux, macOS, and Windows recovery-test jobs with the documented single-threaded fault-test command in `.github/workflows/recovery.yml`
- [X] T053 Run every command and scenario in `specs/001-fix-wal-recovery/quickstart.md`, record validation results there, run `cargo fmt --check` and the full test suite, and document only pre-existing versus feature-introduced Clippy findings in `specs/001-fix-wal-recovery/quickstart.md`

---

## Dependencies & Execution Order

### Phase dependencies

- **Phase 1 — Setup**: Starts immediately.
- **Phase 2 — Foundational**: Depends on T001–T004 and blocks all user stories.
- **Phase 3 — US1**: Depends on T005–T012 and supplies the safe publication tracer bullet.
- **Phase 4 — US2**: Depends on US1's publication path because recovered legacy state must be published safely.
- **Phase 5 — US3**: Adapter work can start after US1; the full legacy parity matrix depends on US2's provenance rules.
- **Phase 6 — Polish**: Depends on every desired user story being GREEN.

### User story dependency graph

```text
Setup → Foundation → US1 (MVP) → US2 → US3 → Polish
                              ↘ set adapter stream  ┐
                               ↘ map adapter stream ┴→ cross-store parity
```

### Within every RED–GREEN pair

1. Complete the RED task and run only that test.
2. Confirm failure is caused by the missing behavior, not fixture/setup failure.
3. Complete only the paired GREEN task.
4. Run the targeted test, then the story's accumulated tests.
5. Refactor only after GREEN and rerun tests after each refactor.

## Parallel Opportunities

- T002 fixture capture and T004 module scaffolding can proceed independently of T003 after dependency setup.
- T007→T008 checked replay and T009→T010 fallible WAL I/O are separate foundational streams and may proceed in parallel while preserving RED before GREEN within each stream.
- US1 and US2 intentionally remain sequential because each tracer bullet changes the same KV recovery coordinator and builds on the prior invariant.
- In US3, the set stream T041→T044 and sorted-map stream T045→T048 can run in parallel; each stream must preserve its own RED–GREEN order.
- T051 documentation and T052 CI automation can run in parallel after all story code is GREEN.

### Parallel example: User Story 1

US1 has no safe intra-story parallel code tasks: run T013→T024 sequentially so each KV recovery invariant drives only its minimal implementation.

### Parallel example: User Story 2

US2 has no safe intra-story parallel code tasks: run T025→T040 sequentially because provenance cases share the frame-history classifier and must remain independently attributable.

### Parallel example: User Story 3

```text
Stream A: T041 RED → T042 GREEN → T043 RED → T044 GREEN
Stream B: T045 RED → T046 GREEN → T047 RED → T048 GREEN
Join:     T049 RED → T050 GREEN
```

## Implementation Strategy

### MVP first

1. Complete Setup and Foundational phases.
2. Complete US1 through T024.
3. Stop and run the independent US1 test criterion.
4. Ship/demo only if KV crash-safe startup is an acceptable first release slice.

### Incremental delivery

1. **US1**: Safe staged publication and recovery status for KV.
2. **US2**: Legacy backup provenance, structured conflicts, and non-fatal cleanup.
3. **US3**: Reuse the proven coordinator for set/map stores and verify isolation.
4. **Polish**: Document, automate cross-platform validation, and run the quickstart.

## Notes

- `[P]` marks different files or independent adapter streams, not permission to violate a task's own RED prerequisite.
- Frozen fixtures must be captured before writer changes and must not be regenerated by the completed implementation.
- Compare set/map logical snapshots, not WAL byte order.
- Preserve all candidate bytes on `AuthorityUndetermined` or `InvalidArtifact` errors.
- Keep power-loss durability, normal-write tail repair, concurrent writer ordering, and callback persistence out of this feature.
