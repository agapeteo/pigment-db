# Online attempt recovery: verification

All runs on Linux x86-64 (22 cores), rustc and clippy 1.97.1 (clippy 0.1.97), single-threaded as
CI runs them (`-- --test-threads=1`), with the target directory outside the repository.

## Baseline at `9dff848`, before any edit
`git rev-parse HEAD`: `9dff8484cd3b6a1243bfd51daa13d0bb5e66b487` (on `ce7f0f9`, spec 015; the
tree held only this spec's `spec.md` untracked). `cargo test --locked --all-targets
--all-features -- --test-threads=1`: 736 passed, 0 failed, 28 ignored across 32 binaries (the
library's unit tests: 465 passed, 9 ignored), as spec 015 recorded at `ce7f0f9`; 144 s. Doc tests
(`cargo test --locked --doc`): 9 passed.

## Principle VI
Scan of the whole change (spec.md's Status line and Amendments, plan.md, tasks.md, this record,
the code, the tests, the workflow, and the dated notes in specs/008, specs/011 and specs/015) for
consumer and application vocabulary: the motivating consumer, penpack, and its finding id V415
appear only in spec.md's marked Motivation note, plan.md's Motivation note and Constitution check,
and this record. No consumer key format, prefix or limit appears; test data are opaque bytes. The
spec directory is `016-online-attempt-recovery`. No public API, type, error variant or limit is
added: the new refusal is `RecoveryError::Io` of kind `WouldBlock`, specs/011's shape. Crate-private
additions: `OwnershipState::open_families`, `OpenDirectoryLease::family` and `family_writers`,
`FamilyWriters`, a `family` parameter on `acquire_open_lease_with`, a closure parameter on
`resolve_store_maintenance`, `DeadAttempts`, `resolve_online_maintenance`,
`discard_dead_first_publication`, `restore_split_online_source`, `held_file_is_at`; test-only,
`manifest_publication_faults::Fault::Exit` and `online_source_move_exit`. It lands on `main` before
any consumer pins it; the revision is recorded here once published (tasks.md, T012).

## Over-reach controls at `9dff848`
Written before any production change, with only T002's infrastructure in place (the exit seams,
the module, the Windows-safe namespace snapshot), which changes no behaviour. Each control opens
the store and requires its refusal, then compares the parent directory's namespace (directories,
files with their bytes, symlinks with their targets) with the one before, apart from the two lock
files the open itself takes. All passed, each family:

| Control | Error at `9dff848` |
|---|---|
| a lone family temporary beside family staging; beside a previous directory; beside a corrupt family; beside no family at all; beside a corrupt family manifest; a temporary that is a directory; one that is a symlink to a regular file; one beside a family manifest linking to nothing | `AuthorityUndetermined` |
| a split whose moved artifact changed; whose moved artifact is also in the store; with a foreign file, or a subdirectory, in the previous directory; whose unmoved artifact changed, or is missing | `AuthorityUndetermined` |
| a split beside an unfinalized `Prepared` (a process killed inside the finalize rewrite, one artifact then moved) | `AuthorityUndetermined` |
| a previous directory that is a symlink to the moved artifact's directory | `AuthorityUndetermined` |
| a previous directory that cannot be read (Unix; skips as root) | `AuthorityUndetermined` |
| a dead attempt's lone temporary, or its split, beside a lone closed `.manifest.next` | `InvalidArtifact` naming the store (directory recovery) |
| a dead attempt's lone temporary, or its split, opened with both lock files skipped (`Unsupported`) | `AuthorityUndetermined` |

The first draft of the unfinalized control rewrote a finalized manifest as unfinalized, which the
codec refuses (`InvalidBody`); it was replaced by a real kill inside the finalize rewrite before any
production change. A first draft of a test expecting an open to recover a family's leftovers beside
directory-level debris was RED for a reason no rule here could change: directory recovery refuses
first (`InvalidArtifact` naming the store, every family and both states). It is the control in the
second-to-last row, and the reason plan.md's gate decision does not move family recovery.

## RED
Every RED below is an assertion failure observed before the production change that satisfies it.

- **T003, FR-1** (infrastructure only):
  - `tests/one_family_instance.rs`: five tests, each "KeyValue at <store>: a second instance of the
    family opened (Normal)", and for `the_refusal_comes_before_any_recovery` "(Recovered)": the
    second open had removed the planted closed temporary.
  - `family_hold_tests::an_open_behind_a_pending_entry_is_refused_once_the_family_is_held`: "a
    second open of the family must be refused, got Ok(Normal)";
    `an_open_still_recovering_holds_its_family_and_a_second_open_does_not_wait_for_it`: "... got
    Ok(Normal)".
  - `a_second_open_during_a_live_online_attempt_is_refused_and_the_attempt_completes` (H3): "a
    second open during a live online attempt must be refused, got Ok(Recovered)".
  - Spec 015's tests rewritten to expect FR-1's refusal of the second instance:
    `a_second_open_during_a_live_cutover_is_refused_and_the_cutover_completes` ("got
    Err(AuthorityUndetermined { .. })"), `a_later_instance_cannot_open_while_an_open_of_its_family_is_still_recovering`
    ("got Ok(Normal)"), `a_compaction_over_a_lone_family_temporary_keeps_its_error_and_its_bytes`
    ("KeyValue: a second instance of the family must be refused, got Ok(())"), and the lone
    temporary test's held-family case ("expected its current refusal, got
    Err(AuthorityUndetermined { .. })").
  - `an_open_that_panics_after_admission_leaves_its_family_free` passed there, as the control it is.
- **T004, CI pin:** `recovery_workflow_runs_every_dedicated_issue_regression_target`: "recovery
  workflow must run `cargo test --test one_family_instance -- --test-threads=1`".
- **T005 and T006, FR-2** (FR-1 in place):
  - `a_process_killed_inside_its_first_online_publication_reopens_recovered`: all nine cases
    (three families, killed at `Created`, `Written`, `Flushed`) `AuthorityUndetermined { active_path:
    store, recovery_path: <active>.pigment-compact.next }`.
  - `a_process_killed_inside_its_cutovers_source_moves_reopens_recovered`: all 37 cases
    `AuthorityUndetermined`: key/value killed after each of its 12 moves, key/set and key/sorted-map
    after each of their 11, and each family inside its `PreviousPublished` publication.
  - With T005's rule alone the first test passed and the second was still RED (both runs
    recorded); then T006's rule made it pass.
  - Spec 015's tests rewritten to FR-2's expectation, observed RED with both rules removed
    (restored from a byte copy, sha256 verified): `a_finalized_prepared_split_by_its_source_move_restores_the_source`
    ("after the first move: a split source kept its store closed: AuthorityUndetermined { .. }")
    and `a_lone_online_manifest_temporary_is_removed_at_open_unless_its_family_is_open` ("KeyValue,
    with None held open: ... Err(AuthorityUndetermined { .. })").
- **T007, FR-3** (T005 and T006 in place, the gate not yet asking `locked`):
  `without_locks_a_dead_attempts_leftovers_keep_their_errors_and_their_bytes`: "KeyValue killed
  FirstPublication(Written), opened without locks: expected its error from 1eb9de5, got
  Ok(Recovered)"; spec 015's `a_process_without_locks_opening_during_a_live_cutover_leaves_it_alone`:
  the lock-less child exited 3 (`CHILD_OPEN_SUCCEEDED`): it restored the parent's live split, the
  third review's hazard.
- **T008, CI pin:** `online_attempt_recovery_tests_run_on_every_operating_system`: "recovery
  workflow must run the online attempt recovery unit tests on every OS"; and
  `a_pinned_step_gated_to_one_operating_system_fails_its_pin`: "Online attempt recovery".

## GREEN
- **T003:** the registry entry's `open_families`; `admit` refuses a held family with
  `WouldBlock`; the lease releases it. The FR-1 tests above passed. The full suite then ran 756
  passed and 2 failed, the failures being the two FR-2 kill tests, which were RED by design: no
  other test in the crate opened one family twice in one process.
- **T004:** the workflow line; `ci_workflow`: 13 passed.
- **T005, T006:** `discard_dead_first_publication` and `restore_split_online_source`, gated on
  `OpenDirectoryLease::family_writers`; 10 of the module's 11 tests then passed, the FR-3 one
  failing (T007's RED).
- **T007:** the gate requires `lock.locked`; the module's 11 and spec 015's lock-less child test
  passed.
- **T008:** the "Online attempt recovery" step; `ci_workflow`: 14 passed; the step's command ran
  the module's tests.
- The full suite after T008: 761 passed, 0 failed, 28 ignored across 33 binaries.

## Tests whose expectation changed
- `unpublished_attempt_tests::a_lone_online_manifest_temporary_keeps_its_error_and_its_bytes` is now
  `a_lone_online_manifest_temporary_is_removed_at_open_unless_its_family_is_open`: alone and beside
  an open instance of another family the open reports `Recovered`, removes only the temporary, and
  every family reopens three times; beside an open instance of the same family it is refused as a
  second instance and changes nothing. At `9dff848` every case answered `AuthorityUndetermined`.
- `online_tests::a_finalized_prepared_split_by_its_source_move_keeps_its_error_and_its_bytes` is
  now `..._restores_the_source`: `Recovered`, no debris, every key.
- `online_tests::an_open_still_recovering_when_a_later_instance_starts_its_cutover_leaves_that_cutover_alone`
  is now `a_later_instance_cannot_open_while_an_open_of_its_family_is_still_recovering`: the later
  instance is refused before any recovery and changes nothing, so no cutover can start; the first
  open completes, compacts, and a write survives a reopen.
- `online_tests::a_second_open_during_a_live_cutover_is_refused_and_the_cutover_completes`: the
  refusal is FR-1's (`Io`, `WouldBlock`, naming `key/value`), not `AuthorityUndetermined`.
- `online_tests::a_compaction_over_a_lone_family_temporary_keeps_its_error_and_its_bytes`: its
  second half asserts that a second instance of the family is refused, where it used to open one;
  the compaction keeps its error in both halves.
- `online_tests::a_process_without_locks_opening_during_a_live_cutover_leaves_it_alone`: unchanged
  assertions, its doc comment now names specs/016 FR-3.
- `unpublished_attempt_tests::namespace` records a held lock file by its presence on Windows, where
  its bytes cannot be read (found by reading: the lone-temporary test snapshots a directory while an
  instance holds its lock, in a module CI runs on Windows).

## Pins added after GREEN
Each was observed passing on the final tree and failing under the probe named in the table below:
`dropping_the_instance_ends_the_hold_while_another_family_keeps_the_directory_open`, the five
`family_writers_tests`, `a_restore_interrupted_part_way_is_completed_by_the_next_open` (each family:
a split from three moves with one moved back, and with all moved back),
`a_live_cutover_is_left_to_complete_whoever_else_opens_or_compacts_its_family` (each family: a
second open refused by FR-1, a second compaction of the instance refused by its attempt token,
nothing changed, the cutover completing), and `a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked`
(each family and state: `current -> one` retargeted to `two/` while the open is parked before
family recovery; neither directory changes and the open answers `AuthorityUndetermined`).

## Neutralization probes
Each probe changed the final tree in place, rebuilt every target, ran the library's
`maintenance_coordination::`, `compaction::online_attempt_recovery_tests::`,
`compaction::online_tests::` and spec 015's two online `unpublished_attempt_tests`, and the
`one_family_instance` integration test, and restored each file from a byte copy verified by sha256
(both production files matched their recorded sha256 after every probe).

| Probe | Caught by |
|---|---|
| P01 FR-1's refusal removed | 13 tests: every FR-1 test, the live-cutover and H3 tests, spec 015's rewritten tests |
| P02 the hold not released on drop | `dropping_the_instance_ends_the_hold_while_another_family_keeps_the_directory_open` (added for it: the first run caught nothing, because an entry with no lease left is removed whole) |
| P03 the hold per directory, not per family | 4 tests, `other_families_and_other_directories_are_unaffected` among them |
| P04 the hold taken only once the inner lock is taken, after recovery | 8 tests, both "still recovering" tests among them |
| P05 a skipped lock counted as held (FR-3) | the in-process FR-3 test, the lock-less child, `a_skipped_inner_lock_excludes_no_other_writer` |
| P06 the held lock not compared with the file at its path | `a_held_inner_lock_no_longer_at_its_path_excludes_no_other_writer` |
| P07 the gate ignores the family hold | not caught, by construction: a lease always holds its family (FR-1) |
| P08 the gate ignores `takes_locks` | not caught, by construction: an entry that takes no locks never holds an inner lock |
| P09 the rules run without the gate | the in-process FR-3 test and the lock-less child |
| P10 the rules run at an online compaction's start | `a_compaction_over_a_lone_family_temporary_keeps_its_error_and_its_bytes` |
| P11 D4 acts outside the locked directory | `a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked` |
| P12 D4 ignores family staging; P13 a previous directory; P14 an entry at the manifest's path; P15 the family's validity; P16 the temporary's kind | each by `a_family_temporary_that_is_not_provably_lone_keeps_its_error_and_its_bytes` and spec 015's `online_debris_that_is_not_a_lone_unpublished_temporary_keeps_its_error_and_its_bytes` |
| P17 D6 acts outside the locked directory | `a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked` |
| P18 D6 moves back an artifact that does not verify; P19 over one present in the store; P20 past a foreign entry; P21 ignoring the unmoved artifacts | each by `a_split_the_manifest_cannot_account_for_keeps_its_error_and_its_bytes` and spec 015's KV twin |
| P22 D6 also for an unfinalized `Prepared` | both unfinalized-split controls |
| P23 D6 follows a previous directory that is a symlink | not caught alone: `verify_descriptor` refuses any symlink on an artifact's path, so the moved artifact never verifies through the link |
| P23b P23, with the moved artifacts verified through the link | `a_previous_directory_that_is_a_symlink_keeps_its_error_and_its_bytes` |
| P24 D4 removed | the first-publication kill test and the lone-temporary test |
| P25 D6 removed | the source-move kill test, the interrupted-restore test, the split test |
| P26 FR-1's refusal of another kind than `WouldBlock` | 13 tests |

P23 and `verify_descriptor`'s own check are two checks of one property, each masking the other, as
spec 015's R03 and R03d were; both stay.

The runner that produced this table matched per-test `FAILED` lines, which the killed children's
output interleaves in the module's runs, so its saved results record P09, P11 and P17 as catching
nothing and P18 to P21 as caught only by spec 015's twin, though the logs show the failures above.
"Review 1 (final)" re-ran those seven with a runner that reads libtest's closing `failures:` list.

## Measured
plan.md IV's threshold, set before measuring: a clean open within 5% of its time at `9dff848`,
and its peak kept. Spec 015's harness (`examples/open_with_debris.rs`) was built against a
`git archive` of `9dff848` (with the working tree's `Cargo.lock`) and against this tree, release
build, and run alternately, one process per open, a fresh copy of the same data per run, two runs
per tree, the slower run reported; load average 5 to 7 on 22 cores.

| Workload (family opened) | `9dff848` | now | Change |
|---|---|---|---|
| kv, 1,000,000 keys (kv) | 5.93 s, 891,616 KiB | 5.69 s, 891,388 KiB | -4.0%, -0.03% |
| multi (kv) | 3.08 s, 509,932 KiB | 3.09 s, 510,452 KiB | +0.3%, +0.1% |
| multi (set) | 1.50 s, 510,052 KiB | 1.48 s, 510,044 KiB | -1.3%, -0.0% |

Every row is within the threshold. Debris opens were not measured: D4 and D6 run only when their
states exist, and each reads what the abandonment that follows it reads.

The first candidate build reused the baseline's artifact (cargo keys a path package's metadata
without its path, and its fingerprint found the repository's sources older than the baseline
build): the two binaries had one sha256. The candidate was rebuilt after touching `src/lib.rs`,
and its binary was checked to hold the new refusal's text, which the baseline's does not.

## Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 768 passed,
  0 failed, 28 ignored across 33 binaries, in 151 s. That is the baseline's 736, plus 22 library
  tests (14 in `compaction::online_attempt_recovery_tests`, one of them the killed child, which does
  nothing unless a parent test starts it; 3 in `maintenance_coordination::family_hold_tests`;
  5 in `maintenance_coordination::family_writers_tests`), the new `one_family_instance` binary's 9,
  and one CI pin. Library tests: 487 passed, 9 ignored. Rewritten tests keep their count.
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features` after `cargo clean -p pigment-db`: no warnings, once the three test enums named
  for the families were allowed `clippy::enum_variant_names`, as the crate's `InspectedFamily` is
  (it reported those three first). `cargo build --locked --release` and `--all-targets`: no
  warnings.
- The CI steps' commands: `cargo test --test one_family_instance` ran 9;
  `cargo test compaction::online_attempt_recovery_tests::` ran 14; `cargo test
  maintenance_coordination::` ran 17.
- macOS and Windows: not run (tasks.md, T012). This change adds to what CI runs on every operating
  system: the integration test (symlink and relative spellings that skip where the platform refuses
  them or shares no root with the working directory), and the unit module, whose tests kill child
  processes inside online compaction, rename files back, and retarget a directory symlink (skipped
  where the platform refuses it).

## Deviations and amendments
- spec.md's Status line names one return-semantics change for the approver's sign-off: an I/O error
  from a removal or move that fails after FR-2 proved its state (`RecoveryError::Io`, `Inspect`),
  where `9dff848` returned `AuthorityUndetermined`.
- spec.md, Amendments (dated 2026-10-03): FR-1's error shape and message; what "the only live
  writer" means, decided after directory recovery; FR-2 at an open only, not at an online
  compaction's start (Decisions); the rules act only in the directory the open locked.
- The FR-2 gate does not hold while an open recovers directory-level maintenance under the
  replacement lock alone; directory recovery refuses there first, which a control pins.
- Dated notes where this spec changes earlier statements: specs/008's contract (the online
  `Prepared` paragraph, the classification bullet and its note), specs/011 FR-10, specs/015's
  spec.md (an amendment), plan.md (D4, D6 and two "Not done here" items) and verification.md (the
  renamed tests).

## Not done here
- macOS and Windows (T012).
- plan.md, "Not done here": an online compaction's start keeps both errors; specs/011's limits
  bound the exclusion (a process of a version before specs/011 takes no lock); a rename of real
  directories on the resolved path; what the abandonment does through the caller's spelling; no
  directory barrier after D4's removal.

## Review 1 (final) (2026-10-03)
The review reported no confirmed blocker or major finding, and sixteen others: from the authority
lens four minor findings (two confirmed) and a nit; from the probes lens five minor findings; from
the documents lens two minor findings and four nits. Each was checked here against the tree. Two
need a production change to be closed (the authority lens's first and fourth); they are recorded
in plan.md's "Not done here" with their evidence and proposed fix, and not made. No production
behaviour changed: the production-file edits are a test-only pause point in `recovery.rs`
(`#[cfg(test)]`, absent from normal builds, Principle V) and the rustdoc of each family's open.
Each new test was observed passing on the tree as handed over, with the tests added, and failing,
by an assertion, under a probe of the property it pins. Same environment as above, with every
build in a target directory outside the repository.

### Principle VI, first
Scan of this round's changes (the tests, the pause point, the rustdoc, spec.md, plan.md, tasks.md,
this section, and the notes in specs/011 and specs/015) for consumer and application vocabulary:
none. penpack and its finding id appear only where the Principle VI record above says. Test data
are opaque bytes. No API, type, error, limit or format is added, and no production behaviour
changes.

### Starting point
The tree as handed over: every one of its 22 changed and new paths had the sha256 the review's
documents lens recorded before it ran (`review016-r1-documents-live.sha`, compared file by file:
identical). A byte copy of them was kept, with each file's sha256, and every comparison and
restore below is against it.

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| authority, minor (confirmed) | D6 moves the source back before checking the abandonment's staging precondition: a split beside a staging that is a directory or a symlink keeps its error, but the directory changes | Confirmed by reading: `restore_split_online_source` checks no staging, and `abandon_prepared_online` removes the empty previous directory before it refuses a staging that is not a regular file. Not closed: the check is a production change, which this pass may not make. Recorded in plan.md "Not done here" with the review's measurement (each family, both kinds: `unchanged=false still-moved=0`) and the fix (require `PathEntry::read(&at.staging).is_absent_or(is_regular_file)` before the first move, then two over-reach cases, RED first). Documented meanwhile in spec.md's Status line and plan.md II (D6) and III. No test pins the current behaviour, which the over-reach standard calls a defect. |
| authority, minor (confirmed) | D6's Physical branch and its directory barriers are exercised by no test | Fixed in tests. `under_physical_durability_a_killed_attempt_reopens_recovered` (each family, killed inside its first publication and after one and two source moves, the child's open and so the manifest under `Physical`; asserts the manifest's durability, `Recovered`, exact state, no debris, the reopens) and `a_restore_whose_directory_barrier_fails_keeps_its_manifest_for_the_next_open` (not Windows; each family, the previous directory's and the store's barrier each failed once through `fail_directory_barrier_for` at the next call: the open returns that error at the store's path with every artifact moved back and the finalized manifest still in place, and the next open reports `Recovered` with the exact state). Caught by R07 (both barriers removed) and R08 (the store's barrier left to the abandonment, after the manifest's removal). |
| authority, minor (not confirmed) | The kill tests open at once after the child exits; Windows releases a dead process's locks asynchronously | Not reproducible here (Linux). Answered in the test module without a production change: `kill_an_online_attempt` now waits, on Windows only and for at most 5 s, until this process can take and release each lock file the child may have held, as `tests/directory_lock.rs` allows a killed owner; elsewhere it returns at once, so a lock left behind still fails the test that meets it. It is type-checked on every target (a `cfg!` branch), run only on Windows, which was not run (T012). |
| authority, minor (not confirmed) | The gate answers `OnlyThis` where specs/011 takes a lock that does not exclude (Solaris; mixed toolchains on illumos, AIX, GNU/Hurd) | Confirmed by reading specs/011's Known limitations and `family_writers`; Solaris was not run. Not closed: answering `NotExcluded` under `cfg(target_os = "solaris")` is a production change. plan.md "Not done here" names both places, and what FR-2 can do there. |
| authority, nit | The probe record's parser misses failures in the kill-test module | Confirmed: the first-run log of P09 holds the failure `results.json` records as `failed: []`. A new runner (`impl016/review1/probes/run.py`) reads libtest's closing `failures:` list of each run; on that same old log it finds the two failures the old parser missed (positive control), and on a green run of the same commands it finds none (negative control). P09, P11 and P17 to P21 were re-run with it; the table below replaces their rows in "Neutralization probes" above. |
| probes, minor | The return-semantics change awaiting sign-off is pinned by no test | Fixed in tests. `a_proved_lone_temporary_that_cannot_be_removed_fails_with_the_removal_error` (Unix; the store directory read-only after a first-publication kill: `Io`, `Inspect`, at the temporary, `PermissionDenied`, nothing changed, then `Recovered` with the exact state once writable) and `a_proved_split_that_cannot_be_moved_back_fails_with_the_moves_error` (Unix; the previous directory read-only after one move: `Io` at `store/<moved artifact>`, nothing changed, then `Recovered`). Both skip where the process may write into a read-only directory; here (uid 1000) both ran. Caught by R01, R02 and R03 (the review's D4h, D6j and the documents lens's Q1). |
| probes, minor | The amendment's "resolved once" half is unpinned for D4 and D6 | Fixed: a test-only pause point, `recovery_pause::Point::DeadAttemptIdentified`, reached by each rule right after it finds the caller's path naming the locked directory, and `every_read_and_write_of_the_rules_is_in_the_directory_the_open_locked` (each family, each leftover: `current` pointed from `one/` to `two/` at that pause; `two/`, holding a dead attempt of its own, must not change). Caught by R04 and R05 (the review's D4g, D6i). |
| probes, minor | FR-1's hold belonging to its own lease is unpinned | Fixed: `tests/one_family_instance.rs::a_familys_hold_is_its_own_and_a_refusal_leaves_nothing_behind` (each family: its lease joins an entry another family created; refused; a third family's instance opened and dropped; refused again). Caught by R10 and R11 (the review's F08, F09b). |
| probes, minor | Nothing checks that a refused open leaves the registry unchanged | Fixed by the same test's tail: once every instance is dropped, `compact_directory_in_place` succeeds, which this process refuses while any open of the directory is counted, and the family reopens with its write. Caught by R09 (the review's F07). |
| probes, minor | D6 running only after the manifest's binding is validated is unpinned | Fixed: `a_split_named_by_a_manifest_bound_to_another_family_keeps_its_error_and_its_bytes` (the key/set split's finalized manifest at the key/value manifest's path: `InvalidArtifact` at that path, every byte kept). Caught by R06 (the review's D6k). |
| documents, minor | The return-semantics change awaiting sign-off is pinned by no test | The same finding as the probes lens's first; the same two tests, caught by R03, which is this lens's Q1. |
| documents, minor | After D6 restores, the abandonment can still refuse or fail; the Status line does not say so | Confirmed. spec.md's Status line now states the item awaiting sign-off in full (a failed directory barrier after D6's moves, and what the abandonment does once a split is restored: its failures return their I/O error, and a state it refuses keeps `AuthorityUndetermined` with the source moved back); its first line, the approval, is unchanged and no sign-off is recorded; an amendment says so. plan.md II (D6: "everything D6 itself checks", and what the abandonment then checks) and III item 3 say the same. The check that would keep the bytes is the authority lens's first finding, recorded, not made. |
| documents, nit | specs/011's FR-10 note and spec FR-2 state the condition less precisely than the code | Fixed: the FR-10 note names the family hold, the lock still being the file at the lock path, and the reading after directory recovery, marked as corrected; spec.md has an amendment that a lone temporary also has no family staging beside it. |
| documents, nit | The rustdoc and the refusal message say "open" where the hold starts at admission | The rustdoc of each family's `try_init_new` now says "while one is open, or still being opened", and that a failed open's hold ends with it. The message is production text: recorded in plan.md "Not done here" with the wording it would take, not changed. |
| documents, nit | The pending-entry progress test does not pin the interleaving its name claims | plan.md IV now says what it pins (a deterministic outcome; the wait behind the `Pending` entry left to the scheduler, reached in 70 of 70 of the review's runs); the counting seam that would force it is in "Not done here". |
| documents, nit | Spec 015 texts that 016 supersedes are partly left without notes | Fixed: a dated note in spec 015's plan.md IV naming specs/016 as the specification it called for; the renamed test at its D6 paragraph and in its tasks.md; and, since spec 015 used "spec 016" for the planned path-spelling work, a note in its plan.md "Not done here" and verification.md that that work is the deferred Draft A. The key/sorted-map row of the clean-open measurement was not added (optional; the measured rows are within the threshold). |

### Probes
Each probe changed the tree in place, rebuilt every target, ran the library's
`maintenance_coordination::`, `compaction::online_attempt_recovery_tests::`,
`compaction::online_tests::` and `compaction::unpublished_attempt_tests::`, and the
`one_family_instance` integration test, and restored each file from a byte copy verified by
sha256 (every probed file matched its sha256 after every probe). The failing tests are read from
libtest's closing `failures:` list.

| Probe | The edit | Caught by |
|---|---|---|
| P09 (re-run) | the rules run where the gate answers `NotExcluded` | `without_locks_a_dead_attempts_leftovers_keep_their_errors_and_their_bytes` ("KeyValue killed FirstPublication(Written), opened without locks: expected its error from 1eb9de5, got Ok(Recovered)") and spec 015's lock-less child (`left: Some(3)`, `right: Some(0)`) |
| P11 (re-run) | D4 ignores `directory != locked` | `a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked` ("a rule acted in two/ ... (Ok(Recovered))") |
| P17 (re-run) | D6 ignores `directory != locked` | the same test, `AfterMoves(1)` |
| P18 to P21 (re-run) | D6 moves back an artifact that does not verify; over one present in the store; past a foreign entry; ignoring the unmoved artifacts | each by `a_split_the_manifest_cannot_account_for_keeps_its_error_and_its_bytes` and spec 015's `a_split_source_the_manifest_cannot_account_for_keeps_its_error_and_its_bytes` ("a refused open changed the directory", or for P19 `Ok(Recovered)`) |
| R01 (the review's D4h) | D4's removal error read as "not proved" | `a_proved_lone_temporary_that_cannot_be_removed_fails_with_the_removal_error` ("expected the removal's error, got Err(AuthorityUndetermined { .. })") |
| R02 (D6j) | D6's move error ignored, the abandonment run | `a_proved_split_that_cannot_be_moved_back_fails_with_the_moves_error` ("expected the move's error, got Err(AuthorityUndetermined { .. })") |
| R03 (the documents lens's Q1) | D4's removal error and D6's move error fall through to the `9dff848` error; D6's barrier errors ignored | both tests above and `a_restore_whose_directory_barrier_fails_keeps_its_manifest_for_the_next_open` ("expected the barrier's error, got Ok(Recovered)") |
| R04 (D4g) | D4's paths built from the caller's path after the identity check | `every_read_and_write_of_the_rules_is_in_the_directory_the_open_locked` ("KeyValue killed FirstPublication(Written): a rule read or acted in two/ ... (Ok(Recovered))") |
| R05 (D6i) | D6's paths built from the caller's path after the identity check | the same test, `AfterMoves(1)` |
| R06 (D6k) | D6 run before the manifest's binding is validated | `a_split_named_by_a_manifest_bound_to_another_family_keeps_its_error_and_its_bytes` ("an open of key/value restored the key/set split its manifest path named") |
| R07 (the authority lens's Q3) | both of D6's barriers removed | `a_restore_whose_directory_barrier_fails_keeps_its_manifest_for_the_next_open` ("the previous directory's barrier failing: expected the barrier's error, got Ok(Recovered)") |
| R08 | D6's store barrier removed, so the store's first barrier is the abandonment's, after the manifest's removal | the same test ("the store directory's barrier failing: the finalized manifest was removed before the restore was durable") |
| R09 (F07) | a refused open counts its lease first | `a_familys_hold_is_its_own_and_a_refusal_leaves_nothing_behind` ("a refused open left the directory owned: FailedClosed { .. already owns this directory }") |
| R10 (F08) | dropping an instance clears every family of the directory | the same test ("a second instance of the family opened (Normal)") |
| R11 (F09b) | only the lease that creates an entry holds its family, and the gate ignores the hold | the same test, at its first refusal |

Each probe's failures were assertion failures of the tests named, and no other test in those runs
failed. The first R03 did not compile (the review's edit was written for its copy) and was
corrected and re-run; a probe that does not build is not counted. The Physical kill test catches
none of R07 or R08, as expected: a barrier's absence is visible only through a power loss or a
failed barrier.

### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 775
  passed, 0 failed, 28 ignored across 33 binaries, in 155 s: the 768 above plus 6 library tests
  in `compaction::online_attempt_recovery_tests` (library: 493 passed, 9 ignored) and one in
  `one_family_instance` (10). The three tests that skip where they cannot act (two read-only, one
  symlink) ran here, each checked with `--nocapture`.
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features` after `cargo clean -p pigment-db`: no warnings. `cargo build --locked --release`
  and `--all-targets --all-features`: no warnings.
- The CI steps' commands: `cargo test --test one_family_instance` ran 10;
  `cargo test compaction::online_attempt_recovery_tests::` ran 20; `cargo test
  maintenance_coordination::` ran 17. The workflow is unchanged: both new library tests and the
  integration test are inside targets it already runs on every operating system.

### Not done here
- The two production changes above (D6's staging precondition; `NotExcluded` on Solaris), the
  refusal message's wording, and the counting seam for the pending-entry test (plan.md, "Not done
  here").
- macOS and Windows (T012): the new tests run there through the existing steps, and the Windows
  wait for a dead child's locks, the Physical kill test (where a Physical move is write-through)
  and the read-only tests on macOS have not run.

## CI on the published revision
 GitHub Actions run 37166904585 on `96415c4` (published 2026-10-04): Minimum supported Rust,
and Recovery on ubuntu-latest, macos-latest and windows-latest, all green. The `one_family_instance` target and the "Online attempt recovery" step run on every operating
system, and spec 015's Windows failure is gone. penpack pins `96415c4`. Closes T012.
