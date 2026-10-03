# Unpublished maintenance attempts: verification

All runs on Linux x86-64, rustc and clippy 1.97 (clippy 0.1.97), single-threaded as CI runs them
(`-- --test-threads=1`), with the target directory outside the repository.

## Baseline at `1eb9de5`, before any edit
`cargo test --locked --all-targets --all-features -- --test-threads=1`: 665 passed, 0 failed,
28 ignored across 31 binaries (the library's unit tests: 395 passed, 9 ignored), as spec 014
recorded at `1afb66e`. `1eb9de5` changed only 014's verification record.

## Principle VI
Scan of the whole change (the diff, the new test module, spec.md, plan.md, tasks.md and this
record) for consumer and application vocabulary: the motivating consumer, penpack, appears only in
the marked Motivation notes of spec.md and plan.md, in plan.md's Constitution check and in this
record, as Principle VI permits; no consumer key format, prefix or limit appears. Test data are the
existing opaque fixtures. The change adds no API, type, error or limit. It lands on `main` before
any consumer pins it; the revision is recorded here once it is published (tasks.md, T011).

## Over-reach controls at `1eb9de5`
Written before any production change and run with only the T002 test infrastructure in place (the
seam, the relocated `StagingWrite` cut, the new cuts and the module), which changes no behaviour.
Each case opens the store and requires its current refusal, then compares the parent directory's
namespace (directories, files with their bytes, symlinks with their targets) with the one before,
apart from the two lock files the open itself takes (`.store.pigment-lock`, `store/.pigment-lock`).
All passed, with these errors:

| Control | Error at `1eb9de5` |
|---|---|
| closed: canonical missing, complete staging | `AuthorityUndetermined` |
| closed: canonical corrupt | `AuthorityUndetermined` |
| closed: previous generation present | `AuthorityUndetermined` |
| closed: staging holds a foreign file | `InvalidArtifact` (staging) |
| closed: staging holds `.pigment-lock` | `AuthorityUndetermined` |
| closed: corrupt main manifest | `AuthorityUndetermined` |
| closed: staging holds a family the canonical directory lacks | `AuthorityUndetermined` |
| closed: `.manifest.next` is a directory | `InvalidArtifact` (`.manifest.next`) |
| closed: staging is a symlink to a valid copy (link and target kept) | `InvalidArtifact` (staging) |
| online: lone temporary beside family staging | `AuthorityUndetermined` |
| online: lone temporary beside a previous directory | `AuthorityUndetermined` |
| online: lone temporary beside a corrupt family | `AuthorityUndetermined` |
| online: temporary is a directory | `AuthorityUndetermined` |

The FR-4 and FR-5 controls were written before their GREEN and passed there:
`AuthorityUndetermined` for each. Two cases added later (FR-4's damaged previous generation, FR-5's
unmoved artifact that does not verify) were observed passing with their rule removed (probes P21
and P17 below). On Windows the symlink case is skipped, with a printed note, if the platform
refuses to create the link.

## RED
Every RED below is an assertion failure observed before the production change that satisfies it.

- **T003, FR-1** (the guard first landed as a stub whose `Drop` did nothing):
  - `a_compaction_that_fails_validation_removes_its_staging` (all three families; a paused child
    at `StagingSync` loses its staged key/value file, so validation fails and the child exits 90):
    "a compaction that failed validation left its staging directory".
  - `a_compaction_whose_first_manifest_publication_fails_or_panics_removes_its_staging`: "a
    compaction whose first manifest publication failed left its staging: [Error, Panic]".
  - `an_unpublished_staging_is_removed_beside_a_manifest_the_attempt_did_not_change`: "an
    unpublished staging beside an unchanged manifest was left".
  - The control `a_staging_named_by_a_published_prepared_is_left_to_its_recovery` passed.
- **T004, FR-2:**
  - `a_failed_manifest_publication_removes_the_temporary_it_created`: "failed publications
    (rewrite, stage, panicked) left their temporary:" all twelve combinations of first
    publication or rewrite, `Created`/`Written`/`Flushed`, error or panic.
  - `a_compaction_whose_first_manifest_publication_fails_leaves_no_temporary`: "compactions whose
    first manifest publication failed left its temporary: [(Created, Error), (Written, Error),
    (Flushed, Error), (Written, Panic)]".
  - `online_tests::an_online_manifest_publication_that_fails_leaves_no_temporary`: "online
    publications that failed before their rename left their temporary: [1, 2]" (the first
    `Prepared` and its finalize rewrite).
  - The rewritten `manifest::tests::failed_temp_publication_preserves_main_phase_and_removes_its_temporary`:
    "assertion failed: !paths.manifest_next.exists()".
  - The control `a_publication_leaves_a_temporary_it_did_not_create` passed.
- **T005, FR-3** (FR-1 and FR-2 in place):
  - `every_pre_prepared_cut_reopens_the_untouched_source`: all 24 cases (each family alone and all
    three together, at `StagingCreate`, `StagingWrite`, `StagingSync`, `StagingValidate`,
    `ManifestWrite`, `ManifestSync`) refused: `InvalidArtifact` (staging) at the first two cuts,
    `AuthorityUndetermined { store, staging }` at the other four.
  - The tightened `recovery_tests::every_closed_checkpoint_process_exit_reopens_exact_state_or_preserves_explicit_evidence`:
    "KeyValue Prepared StagingCreate: a cut before Prepared must reopen the untouched source, not
    InvalidArtifact { path: \".../.store.pigment-compact.next\" }".
  - `a_compaction_started_over_unpublished_debris_removes_it_and_succeeds`: "StagingWrite:
    InvalidArtifact { .../.store.pigment-compact.next }", "ManifestSync: AuthorityUndetermined {
    paths: [store, staging] }".
  - `a_lone_online_manifest_temporary_is_removed_at_open`: `AuthorityUndetermined` for all three
    families.
  - The rewritten staging-copy case of
    `recovery_tests::untrusted_manifest_evidence_distinguishes_ambiguity_from_invalid_debris_without_mutation`:
    `unwrap()` on `Err(AuthorityUndetermined { store, staging })`.
  - `a_lone_closed_manifest_temporary_is_removed_at_open` was added after GREEN and observed RED
    with the closed discard call removed (restored afterwards, sha256 verified): "a lone closed
    manifest temporary kept its store closed: Err(InvalidArtifact { .../.store.pigment-compact.manifest.next })".
- **T006, FR-4:**
  - `an_interrupted_closed_rollback_completes_when_run_again`, killed at `RollbackRestore`:
    `AuthorityUndetermined { store, staging }` for all three families.
  - The same test extended to `RollbackCleanup` (after the rollback removed staging), against the
    first GREEN, which required staging: "KeyValue/KeySet/KeyMap RollbackCleanup:
    Err(AuthorityUndetermined { ... })"; the `RollbackRestore` cases passed.
- **T007, FR-5:** `online_tests::a_finalized_prepared_split_by_its_source_move_restores_the_source`:
  "after the first move: AuthorityUndetermined { ... kv.wal.dat.pigment-compact.next }", "after
  every move: AuthorityUndetermined { ... }".
- **T009, CI pin:** `unpublished_attempt_tests_run_on_every_operating_system`: "recovery workflow
  must run the unpublished maintenance attempt unit tests on every OS"; and
  `a_pinned_step_gated_to_one_operating_system_fails_its_pin` failed on the unchanged workflow
  ("Unpublished maintenance attempts").

## GREEN
- **T003:** `UnpublishedClosedStaging` (`src/compaction/mod.rs`), armed by
  `prepare_closed_staging_guarded` right after `create_dir`, disarmed after
  `publish_closed_prepared`, removing staging when the main manifest is byte-identical to what it
  was at staging creation. The write-error `remove_dir_all` is gone. Library tests: 401 passed.
- **T004:** `UnpublishedManifestTemporary` (`src/compaction/publication.rs`), armed after
  `create_new`, disarmed after the rename. 405 passed.
- **T005:** `discard_unpublished_closed_attempt` and `discard_lone_online_manifest_temp`
  (`src/compaction/recovery.rs`), each returning `Recovered` without the classifier's second read
  of the canonical directory. 408 passed.
- **T006:** `finish_closed_rollback`, shared by the rollback and by the new branch for its
  interrupted state; the branch then dropped its staging requirement. 410, later 412, passed.
- **T007:** `restore_split_online_source`. 412 passed.
- **T009:** the `Unpublished maintenance attempts` step in `recovery.yml`; `ci_workflow`: 13
  passed. `cargo test compaction::unpublished_attempt_tests:: -- --test-threads=1`, as the step
  runs it: 15 passed.
- The new and changed tests (with the cut-identifier test), three runs in a row on the final tree:
  22 passed each time (about 3 s).

## Tests whose expectation changed
- `recovery_tests::every_closed_checkpoint_process_exit_reopens_exact_state_or_preserves_explicit_evidence`:
  its `Err` branch, which accepted `AuthorityUndetermined`/`InvalidArtifact` with an unchanged
  directory at every cut, now refuses at the six pre-`Prepared` cuts; later cuts keep the
  contract's allowance.
- `recovery_tests::untrusted_manifest_evidence_distinguishes_ambiguity_from_invalid_debris_without_mutation`:
  its manifest-less staging copy asserted `AuthorityUndetermined` from the private classifier; it
  now asserts the public open: `Recovered`, staging gone, canonical files byte-identical. The other
  cases are unchanged.
- `manifest::tests::failed_temp_publication_preserves_main_phase_and_unpublished_evidence`, renamed
  `..._and_removes_its_temporary`: the temporary no longer survives, and a later publication
  succeeds instead of failing.
- `fault_checkpoint::tests::every_maintenance_phase_and_cut_has_a_stable_process_identifier`: 13
  cuts, not 11.
- `StagingWrite` now fires once the first family's file is created and before its bytes; at
  `1eb9de5` it fired at the same point as `StagingSync`.

## Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 684
  passed, 0 failed, 28 ignored across 31 binaries, in 72 s (65 s at the baseline). That is the
  baseline's 665, plus 18 library tests (15 in `compaction::unpublished_attempt_tests`, 3 in
  `online_tests`) and the CI pin. Library tests: 413 passed, 9 ignored.
- Doc tests (`cargo test --locked --doc`): 9 passed.
- `cargo fmt --check`: clean.
- `cargo clippy --locked --all-targets --all-features`: no warnings, on a target directory that
  had never checked this crate.
- `cargo build --locked --release` and `cargo build --locked --all-targets`: no warnings.

## Neutralization probes
Each probe changed one condition in the final tree, rebuilt, ran the named tests, and restored the
file from a copy, verified by sha256 (all thirteen files matched afterwards). P17, P18, P19 and P21
remove a rule entirely, which is the code before this change for that rule: its controls must
still pass.

| Probe | Caught by |
|---|---|
| P1 D3 ignores a previous generation | closed control "previous present": `Ok(Recovered)` |
| P2 D3 ignores the staged names | "staging holds a foreign file": `Ok(Recovered)` |
| P3 D3 never consults the canonical directory | "canonical missing": the only complete copy was discarded, then the open failed (`Io`) |
| P4 D3 also runs beside a corrupt main manifest | "corrupt main manifest": `Ok(Recovered)` |
| P5 D3 follows a staging symlink | "staging is a symlink": `Ok(Recovered)` |
| P6 D3 accepts a temporary that is not a file | "manifest temporary is a directory": `Io`, not `InvalidArtifact` |
| P7 D4 ignores family staging and previous | "temporary beside family staging": `Ok(Recovered)` |
| P8 D4 never inspects the family | "temporary beside a corrupt family": `InvalidArtifact` |
| P9 D1 removes staging after `Prepared` is in place | "a published staging was removed" |
| P10 D1 removes only while no manifest exists | "an unpublished staging beside an unchanged manifest was left" |
| P11 D2 owns a temporary it did not create | the other publication's temporary was deleted |
| P12 D5 completes without the source matching | "a canonical directory that no longer matches the source" |
| P20 D5 completes beside a previous generation | "a damaged previous generation beside the restored source": `Ok(Recovered)` |
| P13 D6 restores over a canonical artifact | `PresentAtBothLocations`: opened `Recovered` |
| P14 D6 restores an unverified artifact | `MovedArtifactChanged`: the refused open changed the directory |
| P15 D6 skips foreign files | `ForeignFileInPrevious`: the refused open changed the directory |
| P16 D6 ignores the unmoved artifacts | `UnmovedArtifactChanged`: the refused open changed the directory |
| P17 D6 removed | FR-5 controls passed |
| P18 D3 removed | closed controls passed |
| P19 D4 removed | online controls passed |
| P21 D5 removed | FR-4 controls passed |

Not probed, because no test can see it: the order of D1's guard and the claim (only another
process could observe the gap). This paragraph also named D2 disarming before rather than after
the rename, saying no seam fails a rename; the first review showed that a directory at the main
manifest's path does, and that probe is R07 under "Review".

## Deviations and amendments
- spec.md, Amendments: FR-1's "published" means renamed into place; FR-1's `.manifest.next` half is
  met by FR-2.
- spec.md, Amendments: FR-4 also covers a rollback killed after its staging removal, measured RED
  above.
- The tests that drive online compaction (FR-2's online publications, FR-5) are in
  `online_tests.rs`, which CI runs on Linux only, like every other online-compaction test. The
  closed-compaction and planted-state tests are in the module CI now runs on every operating
  system.
- Spec 008's contract and plan were corrected as FR-6 asks, each edit with a dated note.

## Not done here
- macOS and Windows were not run (tasks.md, T011): the new CI step and the changed recovery tests
  need a CI run of the published revision.
- plan.md, "Not done here": relative store paths (spec 016); the cost of reading the whole
  canonical directory at every open; an online finalize rewrite that fails after its rename; a
  closed `CleanupPending` recovery that leaves its manifest and lets a compaction proceed; and,
  from the first review, a second live instance of one family in one process, no-replace moves
  outside Windows Physical, and Physical barriers in closed recovery.

## Review (2026-10-03)
The first review reported 15 findings and 7 nits. Each was checked here against the tree, and
each behaviour change was made test-first: RED is an assertion failure observed before the
production change. Same environment as above.

### Principle VI, first
Scan of the review's changes (the code, the tests, spec.md's Amendments, plan.md, tasks.md, spec
008's contract and plan, and this section) for consumer and application vocabulary: none. penpack
appears only where the Principle VI record above says. No API, type, error, limit or format is
added.

### Starting point
Before any review edit: 684 passed, 0 failed, 28 ignored, as "Final" records.

### Measured at `1eb9de5`
On a copy of `1eb9de5` with a probe module that is not part of the change:
- **H1.** Two key/value instances open on one directory in one process: the second open answers
  `Ok(Normal)`, both accept writes, and the next open refuses the WAL (`InvalidArtifact`,
  `kv.wal.dat`). No maintenance is involved.
- **H2.** A write by the second instance after the first instance's online compaction returns
  `Ok(())`, and is missing after a reopen (`Ok(Normal)`).
- **H3.** A second open while the first instance's online attempt has published its unfinalized
  `Prepared` and written its staging answers `Ok(Recovered)`. It removes that live attempt's
  manifest and staging, and the attempt's cutover then fails (`Io`, `WriteStaging`, `NotFound`).
- **H4.** Debris an open cannot remove (a complete staging copy and a `.manifest.next`, staging
  made read-only): `AuthorityUndetermined { store, staging }`. With the change, the open returns
  `Io { Inspect, staging, PermissionDenied }` and removes nothing.

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| A1 major | D1 removes the only intact copy when the source changed under the claim | Confirmed and fixed; **design change**. The guard also requires the canonical directory to be exactly the captured generation (`generation_matches` against the capture), so a staging it removes is always a compaction of what the canonical directory still holds. A staging it leaves is decided by recovery as after a kill. plan.md D1 and Decisions; spec.md Amendments (FR-1). |
| A2 major | D4 and D6 run without exclusion from a same-process live cutover | (Its premise was wrong and the finding is fixed: see "Review 2".) Confirmed as stated: plan.md said D3 to D6 ran under the replacement lock or the claim, which was false for D4 and D6, and the interleavings are real. **Not fixed here.** Their precondition is a second live instance of one family in one process, which corrupts the WAL at `1eb9de5` with no maintenance (H1), loses acknowledged writes across an online compaction (H2), and lets recovery abandon a live attempt (H3). Closing it means refusing such an open or recording live online attempts in the registry: a new public refusal and new coordination state (Principles III and IV), so it needs its own specification. plan.md D4, D6, IV and "Not done here", the D6 doc comment and spec 008's contract now state the exclusion as it is. |
| A3 minor | D6's no-replace move replaces outside Windows Physical | Confirmed (`durability::move_namespace` is `std::fs::rename` there). The no-replace claim is removed from plan.md (D6, Decisions) and the doc comment. A true no-replace move would change the function publication shares, so it is in "Not done here". |
| A4 minor, unconfirmed | No Physical barrier between the rollback's restore rename and its manifest removal | Verified by reading only. It predates this spec, and closed `Prepared` recovery and closed `CleanupPending` recovery share it. D5 adds no rename of its own. The outcome reasoned from the code is a refusal with every generation present. Recorded in "Not done here". |
| P1 minor | FR-2's failed rename is untested, and this record said no test could see it | Confirmed. `a_publication_whose_rename_fails_removes_its_temporary` (a directory holding a file at the main manifest's path); probe R07. The "Not probed" paragraph is corrected. |
| P2 minor | D4's "family has no manifest" is untested | Confirmed. `online_tests::a_family_manifest_temporary_beside_a_published_manifest_is_recovered_with_it`; probe R08. After N1, D4 itself refuses beside any entry at the manifest's path, so moving its call above the manifest read alone changes nothing (R08a); that check is pinned by R06. |
| P3 minor | Subdirectory or symlink inside staging untested | Confirmed. Two closed controls; probes R03 and R03b. |
| P4 minor | D3's "at least one family" untested | Confirmed. Closed control, empty canonical directory beside a lone temporary; probe R04. |
| D1 major | Same as P3, plus a sealed-segment name | Confirmed. The three controls are in `closed_debris_that_is_not_provably_unpublished_keeps_its_error_and_its_bytes` and tasks.md T012. |
| D2 minor | A staging write failure is untested | Confirmed. `a_compaction_whose_staging_write_fails_removes_its_staging`: at the `StagingCreate` pause a directory takes each family file's name, so the first `create_new` fails. This works as root and on Windows, where the review's read-only directory would not. Probe R01. |
| D3 minor | D1's unreadable-manifest rule is untested | Confirmed. `a_main_manifest_that_is_neither_readable_nor_absent_leaves_the_staging`; probe R02. |
| D4 minor | An open that cannot remove debris returns `Io`, not `AuthorityUndetermined` | Confirmed (H4). Stated in spec.md Amendments (FR-7) and plan.md III, and pinned on Unix by `debris_that_cannot_be_removed_fails_the_open_with_the_removal_error`, which skips when the process can write into a read-only directory. |
| D5 minor | D6's rustdoc misstates the cutover order | Confirmed (`finalize_online_prepared` precedes `take_online_writer`); reworded. |
| D6 minor | Lock statements in plan.md and the contract | Confirmed. plan.md D1, D3, D4 and IV, and the contract bullet, now name the lock each rule runs under. |
| D7 minor | No CI run on macOS or Windows | Confirmed and still open: tasks.md T011. Nothing here was run off Linux. |
| N1 | A symlink to nothing at the main manifest reads as absent | Confirmed and fixed in D1, D3 and D4. Nothing may be at the manifest's path. spec.md Amendments; probes R05, R06, R13. |
| N2 | D6 doc comment | As D5. |
| N3 | "Staging first" is not pinned | Confirmed. Pinned by the removal-failure test; probe R09. |
| N4 | D1's unreadable-manifest rule | As D3. |
| N5 | (a) D6's "finalized" scope; (b) `keep_staging` after `Prepared` is redundant | (a) `online_tests::a_split_beside_an_unfinalized_prepared_keeps_its_error_and_its_bytes`, probe R10. (b) Confirmed by R14; plan.md D1 says so. |
| N6 | The contract's correction note misstates old step 5 | Corrected. In the contract and in spec 008's plan the note also moved below the list it had split. |
| N7 | Small inaccuracies here and in plan.md | Corrected: P17 to P19 and P21; the two VI records; the revision is recorded after publication. |

### RED
- `a_compaction_whose_source_was_damaged_under_its_claim_keeps_its_staging`: "a compaction whose
  source was damaged under its claim removed its staging, the only intact copy of the captured
  state". GREEN: the open then answers `AuthorityUndetermined { store, staging }` and changes
  nothing.
- `a_compaction_whose_source_was_replaced_by_a_valid_generation_leaves_its_staging_to_the_next_open`:
  "the attempt must leave a staging it cannot prove redundant". Under D3's precondition, the
  review's other proposal, it stays RED (probe R12).
- `a_main_manifest_that_is_neither_readable_nor_absent_leaves_the_staging`: "the guard removed
  staging beside a manifest it could not read: [\"a dangling symlink at the manifest path\"]".
  The directory case passed.
- `closed_debris_...`, its new case: "main manifest is a symlink to nothing: expected its current
  refusal, got Ok(Recovered)".
- `online_debris_...`, its new case: "temporary beside a family manifest that is a symlink to
  nothing: expected its current refusal, got Ok(Recovered)".

### Controls at `1eb9de5`
Both control tests, with every new case, and the two new online controls were copied, with their
helpers, onto the `1eb9de5` copy. All four passed there with the same errors:
- staging holding a subdirectory, a symlink entry or a sealed-segment name: `InvalidArtifact`
  (staging);
- an empty canonical directory beside a lone temporary: `InvalidArtifact` (`.manifest.next`);
- a main manifest that is a symlink to nothing: `AuthorityUndetermined`;
- a family manifest that is a symlink to nothing: `AuthorityUndetermined`;
- a temporary beside a published family manifest: `Recovered`;
- a split beside an unfinalized `Prepared`: `AuthorityUndetermined`.

The tests that pin existing behaviour without changing it (R01, R02, R07, R09) were added after
the code they pin, and each was observed RED under its probe.

### Probes
Each probe changed the final tree in place, ran the library's `compaction::` tests (103 in all),
and restored the file from a byte copy verified by sha256.

| Probe | Review id | Caught by |
|---|---|---|
| R01 D1's guard armed after the staging writes | PE | "a compaction whose staging write failed left its staging directory" |
| R02 D1 ignores an unreadable manifest | PD, P05 | "...: [\"a directory at the manifest path\", \"a dangling symlink at the manifest path\"]" |
| R03 D3 accepts any staging entry type | PA, P13 | "staging holds a subdirectory named for an active segment": `Ok(Recovered)` |
| R03b D3 accepts symlink entries only | | "staging holds a symlink named for an active segment": `Ok(Recovered)` |
| R04 D3 accepts a canonical directory with no family | P11 | "an empty canonical directory beside a lone temporary": `Ok(Recovered)` |
| R05 D3 ignores an entry at the manifest's path | N1 | "main manifest is a symlink to nothing": `Ok(Recovered)` |
| R06 D4 ignores an entry at the manifest's path | | "temporary beside a family manifest that is a symlink to nothing": `Ok(Recovered)` |
| R07 D2 disarmed before the rename | P08 | "a failed rename left its temporary (... IsADirectory ...)" |
| R08 D4 before the manifest read, and R06 | P23 | "an open reported success and left .../kv.wal.dat.pigment-compact.manifest", and R06's control |
| R08a D4 before the manifest read only | | not caught: D4's own entry check refuses beside the manifest (R06 pins it) |
| R09 D3 removes the temporary before staging | P18 | "the temporary was removed before staging" |
| R10 D6 also for an unfinalized `Prepared` | P32 | "a split beside an unfinalized Prepared opened Recovered" |
| R11 D1 without the canonical-generation check | | both FR-1 review tests |
| R12 D1 with D3's precondition instead | | "the attempt must leave a staging it cannot prove redundant" |
| R13 `ManifestBytes` reads a symlink to nothing as absent | | "...: [\"a dangling symlink at the manifest path\"]" |
| R14 no `keep_staging` after `Prepared` | P04 | not caught, as plan.md D1 now says: the manifest comparison keeps a published staging |
| P4b P4 and R05 together | | "corrupt main manifest": `Ok(Recovered)` |

P1 to P21 (table above) were run again on the final tree, against all 103 `compaction::` tests;
P1's needle was moved past the new manifest check. Each was caught by the same control as before,
with three differences:
- P3 is now also caught by the damaged-source test: the discard removes the only intact copy, and
  the open then fails (`InvalidArtifact`, `store/kv.wal.dat`).
- P4 is no longer caught. D3 now refuses beside any entry at the manifest's path, a corrupt
  manifest included, so the arm it is called from no longer matters. P4b removes that check too
  and is caught.
- P17, P18, P19 and P21 remove a rule entirely. Only behaviour tests fail; every control passes.
  P18 also fails the removal-failure test, which expects the amended `Io`.

### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 692 passed,
  0 failed, 28 ignored across 31 binaries. That is 684 plus eight library tests: six in
  `compaction::unpublished_attempt_tests` (now 21) and two in `online_tests` (now five for this
  spec). Library tests: 421 passed, 9 ignored. The `compaction::` tests passed three runs in a row
  (103 each), and the CI step's command ran 21.
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features`: no warnings. `cargo build --locked --release` and `--all-targets`: no warnings.
- macOS and Windows: not run (T011).

## Review 2 (2026-10-03)
The second review reported eleven findings and four nits. Each was checked here against the tree,
and each behaviour change was made test-first: RED is an assertion failure observed before the
production change. Same environment as above, with the target directory outside the repository.
Every measurement "at `1eb9de5`" ran on a `git archive` copy of that revision with a test module
that is not part of the change.

### Principle VI, first
Scan of this round's changes (the code, the tests, spec.md's Amendments, plan.md, tasks.md, spec
008's contract and plan, and this section) for consumer and application vocabulary: none. penpack
appears only where the Principle VI record above says, and V415 only as provenance. No API, type,
error, limit or format is added; the registry's lease count gains a per-family split, which is
crate-private.

### Starting point
Before any edit: 692 passed, 0 failed, 28 ignored across 31 binaries, as Review's "Final" records.

### Measured
- **The first review's A2 premise.** H1 above measured two instances that both write. The second
  review measured a second instance that only opens and reads, and this round reproduced its shape
  as a test with a live cutover parked after its source moves (a new test-only pause): at
  `1eb9de5` the second open is refused (`AuthorityUndetermined`), changes nothing, and the cutover
  completes with every key readable after a reopen; before this round's fix the second open
  answered `Ok(Recovered)` (D6 restored the live cutover's source). So D6 introduced the
  interference; the configuration was harmless for a reader.
- **Prefixes of a staged file.** At `1eb9de5`, for each family, plain and segmented, every prefix
  of a staged file was replayed: 0 bytes and the 64-byte header alone replay as an empty state,
  1 to 63 bytes are rejected (`Invalid`), and every longer prefix replays as a complete state or a
  torn tail. So a kill while a staged file's header is written leaves a file the replay rejects and
  that holds no fact, which is why D3 reads a rejected staged file as holding nothing.
- **Concurrent first opens over D3's debris** (finding D-10 below), on a scratch copy of the final
  tree: three threads opening the three families behind a barrier, 300 runs, 900 opens: 300
  `Recovered`, 581 `Normal`, 19 `RecoveryError::Io` with `NotFound` (15 removing
  `.manifest.next`, 4 removing staging); no run left debris.

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| A-1 major | A2 was declined on a false premise; a second instance that only opens lets D6 destroy source data | (Superseded by "Review 3": the gate below did not hold while D4 and D6 acted, nor see another process where locks are skipped, and both rules are withdrawn.) Confirmed and fixed. D4 and D6 run only when the open, or the compacting instance at an online compaction's start, is its process's only open instance of the family (`FamilyOpens::Sole`). The registry entry already counted open leases, and the lease is admitted before recovery runs; it now counts them per family. Per family rather than per directory, as the review proposed, because an instance of another family cannot own the family's artifacts, and a process keeping all three families open would otherwise never run D4 or D6 for two of them (`an_open_instance_of_another_family_does_not_keep_a_familys_debris`). No new lock, wait or refusal. plan.md D6 (whose "unsupported, destroys data anyway" text was wrong), D4, IV, Decisions and "Not done here", the D6 doc comment, the A2 row above, spec.md Amendments and spec 008's contract are corrected. |
| A-2 major | D3 deletes the only complete copy when the changed canonical directory still validates | (Superseded by "Review 3": containment missed lost deletes, and D3 now requires equality.) Confirmed (truncation at a record boundary under the claim, and a rollback to an older generation) and fixed: D3 removes a staging only when it holds nothing the canonical directory lacks (`staging_holds_nothing_the_canonical_directory_lacks`). Containment rather than the review's equality, which would refuse an interrupted attempt's prefix and a canonical directory that gained a write. |
| A-3 minor | D1's precondition fails FR-1 for a relative store path, with a false warning | Confirmed and fixed with the review's alternative: the guard compares at the path resolved when staging was created (`fs::canonicalize`, as the claim resolves it), which also covers a symlinked store path (D-8). A path that does not resolve leaves the staging with its own warning. |
| A-4 minor | D3 and D6 turn an inspection error into a new error | Confirmed for both measured states (an unreadable staging directory; an unreadable previous directory) and fixed: a read that fails while a rule checks a state proves nothing (`PathEntry`, `regular_file_names`), so the classifier and the existing checks answer as at `1eb9de5`. The other reads in D3, D4 and D6 (metadata of entries in the parent or the store directory) could not fail here without the manifest read in the same directory failing first, so they were made non-propagating too with no observable change. FR-7's I/O error now comes only from a removal or a move. |
| P-5 minor | D1's "bytes, not existence" is unpinned (Q14) | Confirmed. A third case in `a_staging_named_by_a_published_prepared_is_left_to_its_recovery`: a `Prepared` renamed over an earlier manifest keeps the staging. Probe Q14. |
| P-6 minor | No symlink control for `.manifest.next`, closed or online | Confirmed. One case in each control test; probes Q24b, Q24, Q31c and Q31. |
| P-7 minor | D1's "never through a symlink" is untested (Q17b) | Confirmed. `the_guard_never_removes_through_a_staging_symlink`; probe Q17b. |
| D-8 minor | Amended FR-1 protects the captured state for one open only | Same defect as A-2; fixed by the code, and spec.md's Amendments supersede the first review's FR-1 wording. |
| D-9 minor | FR-1 depends on the path spelling | Same as A-3; fixed in the code, so FR-1 now holds for both spellings. plan.md "Not done here" records what spec 016 still owns: the relative compaction itself fails, and a symlinked path's artifacts sit where no open looks. |
| D-10 minor | Concurrent first opens of two families race inside D3 | Confirmed (measured above). Not fixed: it is the defect specs/011 records under "Found during review, not fixed here", which needs recovery serialized per registry entry. Treating `NotFound` as already removed, the review's other option, would remove the 19 failures measured but not the window: an open that lists a staging another thread is removing finds it incomplete and refuses. Recorded in plan.md IV, D3 and "Not done here". |
| D-11 minor | CI on macOS and Windows unverified | Still open: tasks.md T011. |
| N-1, N-3 | D4 and D6 run under the replacement lock alone when directory-level maintenance was in progress at the open | Confirmed by reading (`try_init_new_configured` runs recovery before `ensure_inner_lock`; `open_locks` returns only the replacement lock then). Reworded in plan.md D4, D6 and IV and in the contract. |
| N-2 | FR-3's "staging first" is pinned on Unix only | Accepted and recorded in plan.md "Not done here". |
| N-4 | Spec 008's texts and D3's doc comment omit "at least one family" | Corrected in the contract bullet, spec 008's plan note (with "no previous generation") and the D3 doc comment. |

### RED
- `online_tests::a_second_open_during_a_live_cutover_is_refused_and_the_cutover_completes`: "a
  second open during a live cutover must be refused, got Ok(Recovered)".
- `a_lone_family_temporary_is_left_while_another_instance_of_the_family_is_open`: "KeyValue: a
  second open beside a live instance of the family: expected its current refusal, got
  Ok(Recovered)".
- `online_tests::a_compaction_beside_another_instance_of_its_family_leaves_a_lone_temporary`: "a
  compaction beside another open instance of its family removed a temporary it cannot prove is
  debris: Ok(())".
- `a_source_truncated_under_the_claim_keeps_the_staging_that_holds_what_it_lost`: "a source
  truncated at a record boundary under the claim: expected its current refusal, got
  Ok(Recovered)".
- `a_source_rolled_back_under_the_claim_keeps_the_staging_that_holds_what_it_lost`: "... got
  Ok(Recovered)".
- `a_staging_holding_a_fact_the_canonical_directory_lacks_keeps_its_error_and_its_bytes`: "KeyValue
  (changed value: false): staging holds a fact: expected its current refusal, got Ok(Recovered)".
- `a_staging_directory_that_cannot_be_read_keeps_its_error_and_its_bytes` (Unix): "got Err(Io {
  operation: Inspect, path: \".../.store.pigment-compact.next\", ... PermissionDenied })".
- `online_tests::a_previous_directory_that_cannot_be_read_keeps_its_error_and_its_bytes` (Unix):
  "got Err(Io { operation: Inspect, path: \".../kv.wal.dat.pigment-compact.previous\", ...
  PermissionDenied })".
- `a_compaction_given_a_relative_store_path_that_fails_removes_its_staging` (the child printed
  `Err(InvalidArtifact { path: ".store.pigment-compact.next/kv.wal.dat" })`, the motivating
  defect's own failure):
  "a compaction given a relative store path left its staging".
- `a_compaction_given_a_symlinked_store_path_that_fails_removes_its_staging`: "a compaction given a
  symlinked store path left its staging beside the link".

The control `an_open_instance_of_another_family_does_not_keep_a_familys_debris` passed before the
gate and after it, and `a_staging_holding_nothing_the_canonical_directory_lacks_is_removed` before
the containment check and after it.

### Controls at `1eb9de5`
Copied, with their helpers (and, for the live cutover, the test-only pause), onto the `1eb9de5`
copy. All passed there:
- the live cutover: the second open refused with `AuthorityUndetermined`, the directory unchanged,
  the cutover completing;
- a lone family temporary beside another open instance of the family, each family:
  `AuthorityUndetermined`; an online compaction beside one: `AuthorityUndetermined`, nothing
  changed (the refusal halves of the two gate tests; their second halves need D4);
- a staging holding a fact the canonical directory lacks (each family, and a changed key/value
  value), a source truncated under the claim, a source rolled back under the claim:
  `AuthorityUndetermined`;
- an unreadable staging directory: `InvalidArtifact` (staging); an unreadable previous directory:
  `AuthorityUndetermined`;
- a `.manifest.next` that is a symlink to a regular file: closed `InvalidArtifact`
  (`.manifest.next`), online `AuthorityUndetermined`.

The remaining new tests pin rules `1eb9de5` does not have (the guard, D3's removals, the gate's
second halves, spelling), and each was observed failing under its probe below.

### Probes
Each probe changed the tree in place, ran the library's 117 `compaction::` tests, and restored the file from a byte copy verified by sha256; the source files
matched their recorded sha256 after every run.

| Probe | Review id | Caught by |
|---|---|---|
| S01 D6 runs beside another open instance of the family | A-1 | "a second open during a live cutover must be refused, got Ok(Recovered)" |
| S02 D4 runs beside another open instance | A-1 | both D4 gate tests |
| S03 the gate counts the directory's leases, not the family's | | "KeyValue beside an open KeySet: the lone temporary kept its store closed" |
| S04 a closed lease is not taken off its family's count | | "alone, the compaction removes the temporary and proceeds: AuthorityUndetermined" |
| S05 a compaction start takes itself to be the sole instance | A-1 | "a compaction beside another open instance of its family removed a temporary ..." |
| S06 D3 without the containment check | A-2, D-8 | the three D3 RED tests |
| S07 containment replaced by equality | | the containment control, and the first review's replaced-generation test |
| S08a/b/c a set member, a map entry, a key/value value not compared | | the KeySet, KeyMap and changed-value cases of the planted test |
| S09 a staged file the replay rejects counts as unproven | | not caught by the first 117; then "stagings holding nothing the canonical directory lacks kept their stores closed" (the header case, added for it) |
| S11 D3 returns a staging listing error | A-4 | the unreadable-staging test |
| S12 D6 returns a previous-directory listing error | A-4 | the unreadable-previous test |
| S13 D1 compares at the caller's spelling | A-3, D-9 | both spelling tests |
| Q14 D1 compares the manifest's existence | P-5 | "a staging named by a Prepared renamed over an earlier manifest was removed" |
| Q24b, Q24 D3 follows, or accepts, a temporary symlink | P-6 | "manifest temporary is a symlink to a regular file: ... got Ok(Recovered)" |
| Q31c, Q31 D4 follows, or accepts, a temporary symlink | P-6 | "temporary is a symlink to a regular file: ... got Ok(Recovered)" |
| Q17b D1 follows a staging symlink and removes its target | P-7 | "the guard removed files through a staging symlink" |

The first review's probes were run again on the final tree, rewritten where this round changed
their needle (P1, P3, P5, P6, P7, P13, P14, P17, P19, R03, R03b, R05, R06, R08, R08a, R09, R10,
R11, R12, P4b). Each was caught as before, with these exceptions:
- P2 (D3 ignores the staged names) is no longer caught alone, nor is P2c (the containment check's
  own lookup of a staged name answers `continue`): each check refuses what the other would. P2 and
  P2c together are caught ("staging holds a foreign file: ... got Ok(Recovered)"). Both checks
  stay: the containment check cannot compare a file it cannot place.
- P3 no longer compiles as written; rewritten to remove the debris whenever the canonical
  directory does not validate, it is caught by "canonical missing" and "source damaged under the
  claim".
- P4, R08a and R14 are not caught, as the first review recorded; P4b and R08 are.
- R11 and R12 are now also caught by the two D3 RED tests.

### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 706 passed,
  0 failed, 28 ignored across 31 binaries. That is 692 plus fourteen library tests: eleven in
  `compaction::unpublished_attempt_tests` (now 32, one of them the relative-path child, which does
  nothing unless a parent test starts it) and three in `online_tests`. Library tests: 435 passed,
  9 ignored. The `compaction::` tests passed three runs in a row (117 each), and the CI step's
  command ran 32.
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features`: no warnings, after `begin_online_capture` took an eighth parameter and the
  `too_many_arguments` allow this file already uses for `capture_artifact`. `cargo build --locked
  --release` and `--all-targets`: no warnings.
- macOS and Windows: not run (T011). This round adds to the module CI runs on every operating
  system: a child process that changes its working directory, symlink cases that skip where the
  platform refuses the link, and a Unix-only unreadable-staging test that skips as root (its online
  twin is in `online_tests`, which CI runs on Linux).

## Review 3 (2026-10-03)
The third review reported five confirmed major findings (two pairs of them the same defect seen by
two lenses), ten minor findings and five nits. Each was checked here against the tree, and each
behaviour change was made test-first: RED is an assertion failure observed before the production
change. Same environment as above. Every measurement "at `1eb9de5`" ran on a `git archive` copy of
that revision with this round's tests and the two test-only pauses copied in, none of it part of
the change.

The rows for D4 and D6 in the sections above, and their probes, record rules this round withdrew.
They are kept as the record of what was built and why it was not enough.

### Principle VI, first
Scan of this round's changes (the code, the tests, spec.md's Amendments, plan.md, tasks.md, spec
008's contract and plan, and this section) for consumer and application vocabulary: none. penpack
appears only where the Principle VI record above says, and V415 only as provenance. No API, type,
error, limit or format is added. The crate-private `FamilyOpens` and the registry's per-family
lease count, added by the second review, are removed: `maintenance_coordination.rs` and the three
store files are byte-identical to `1eb9de5`.

### Starting point
Before any edit: 706 passed, 0 failed, 28 ignored across 31 binaries, as "Review 2"'s "Final"
records.

### Design changes
This round changed design; it did not only fix code:
- **D3 compares states for equality, not containment.** Each staged file must recover exactly what
  its canonical family recovers, or be shorter than a file header (64 bytes) and so hold nothing.
  A staged file of at least a header that the replay rejects refuses. Every refused state keeps
  its `1eb9de5` error.
- **D4 (FR-3 online) and D6 (FR-5) are withdrawn**, with the per-family lease count that gated
  them. Their states keep their `1eb9de5` errors. This is the narrower refusal: making them sound
  needs coordination state, which needs its own specification (plan.md, Decisions).
- **D1 resolves every path it acts on when staging is created**, not only the canonical
  directory.
- Added: a test-only pause between directory and family recovery
  (`recovery::family_recovery_pause`).

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| authority, major | D3 containment ignores deletions | Confirmed: the KeyValue case of the new real-compaction test, and the planted cases, opened `Recovered` with the deleted fact back. Fixed: equality (design change above). |
| documents, major (b) | Containment treats an absence as no fact | The same defect. spec.md (FR-3 closed amendment, superseding the second review's), plan.md D1, D3 and Decisions, the contract bullet, spec 008's plan note and the D1 and D3 doc comments corrected. |
| authority, major | D6 and D4 run beside another process's live attempt where locks are skipped (FR-11) | Confirmed (RED below). Fixed by withdrawing D4 and D6. |
| probes, major | A `Sole` answer is stale for the whole of directory recovery | Confirmed (RED below). Fixed by withdrawing D4 and D6. |
| documents, major (a) | The same, and the documents claim it cannot happen | The same defect. The rustdoc went with the code. plan.md D4, D6, IV, Decisions and "Not done here", spec.md (FR-3 online and FR-5 withdrawn), the contract and the Review 2 rows above are corrected. |
| authority, minor | `Sole` read before directory recovery | The same as the probes major. |
| authority, minor | D1 checks one directory and removes another | Confirmed (RED below). Fixed: the canonical directory, the main manifest and the staging directory are resolved when staging is created, and the guard checks and removes only through them. |
| authority, minor | D3 doubles open time and raises peak memory before the store is usable | Confirmed: the staged bytes stayed alive through the canonical replay. Fixed by dropping them first. Measured below: 883 MiB against the review's 1072, on 786 for a clean open. The doubled time at an open that removes debris stays, once; plan.md IV states the bound and the thresholds. |
| authority, minor | A rejected staged file reads as holding nothing even when damaged mid-file | Confirmed (RED below). Fixed: only a staged file shorter than a header holds nothing. |
| probes, minor (A5) | Which family's count a closing lease decrements is unpinned | Moot: the per-family count is removed. |
| probes, minor (A8) | The compaction-start gate is pinned for KeyValue only | Moot for the gate. The rewritten compaction control covers all three families, with and without another instance open. |
| probes, minor (B4) | Map entry values are not checked by any test | Confirmed. A changed KeyMap value case in `a_staging_holding_a_fact_the_canonical_directory_lacks_keeps_its_error_and_its_bytes`; probe B4. |
| probes, minor (B10) | Only the first staged file needs to be compared | Confirmed. `every_staged_family_is_compared`; probe B10. |
| probes, minor (B7, D5) | An unreadable staged file is unpinned | Confirmed. `a_staged_file_that_cannot_be_read_keeps_its_error_and_its_bytes` (Unix, skips as root); probe B7. D5 (the read failure mapped to `Io`) is not expressible: the comparison returns a `bool`, so no read error can leave it. |
| probes, minor (D6 read) | An unreadable moved artifact is unpinned for D6 | Moot: D6 is withdrawn, and no rule reads a moved artifact. |
| probes, nit (C2) | Resolving at drop time instead of creation is unobservable | Addressed by the D1 fix: everything is resolved at creation, and the working-directory test catches a removal through the caller's spelling (probe C1). |
| documents, minor | The per-family count is in-process coordination standing in for a deferred exclusion; the 2026-10-03 amendments have no recorded approval | Confirmed. The count is removed with D4 and D6. spec.md now states that the review amendments narrow or withdraw approved requirements and await the approver's sign-off; this implementation cannot record that sign-off. |
| documents, nit (d) | Two non-removal I/O paths | D6's barriers: moot. The rollback's second read of the staging path: confirmed by reading. At `1eb9de5` the same recovery's first read of that path already returned `Io` when it failed, so the second can fail only if the path became unreadable in between. spec.md's FR-7 amendment and plan.md III now say so. |
| documents, nit (b) | "Rejected means empty" equates an open's view with the file's content | The same as the authority minor above. |
| documents, nit (c) | D1 compares at the resolved path but removes at the caller's spelling | The same as the D1 minor above. |

### RED
- `a_source_that_lost_a_delete_under_the_claim_keeps_the_staging_that_holds_it`: "KeyValue
  TruncatedAtBoundary: a source that lost a delete under the claim: expected its current refusal,
  got Ok(Recovered)".
- `a_staging_not_proved_to_hold_the_canonical_state_keeps_its_error_and_its_bytes`: "KeyValue
  CanonicalGained: expected its current refusal, got Ok(Recovered)".
- `a_compaction_whose_source_was_replaced_by_another_valid_generation_keeps_its_staging_and_its_error`
  (the first review's control, rewritten to the equality expectation): "a source replaced by a
  valid generation holding one more key: expected its current refusal, got Ok(Recovered)".
- With equality in place, `a_staged_file_the_replay_rejects_past_its_header_keeps_its_error_and_its_bytes`:
  "KeyValue: a staged file damaged in its header: expected its current refusal, got Ok(Recovered)".
- `a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging`: "a
  compaction of one/store changed two/, whose staging may be the only copy of a fact" (the child
  printed `Err(InvalidArtifact { path: ".store.pigment-compact.next/kv.wal.dat" })`).
- `online_tests::an_open_still_recovering_when_a_later_instance_starts_its_cutover_leaves_that_cutover_alone`:
  "an open still recovering when a later instance's cutover moved its source must be refused, got
  Ok(Recovered)".
- `online_tests::a_process_without_locks_opening_during_a_live_cutover_leaves_it_alone`: the child
  printed `Ok(Recovered)`, and "a process without locks opening during a live cutover must be
  refused".

Each RED test stops at its first failing case. The probes below show which cases each one needs.

### Tests whose expectation changed
- `a_compaction_whose_source_was_replaced_by_a_valid_generation_leaves_its_staging_to_the_next_open`
  is now `..._by_another_valid_generation_keeps_its_staging_and_its_error`: the next open keeps
  `AuthorityUndetermined` and both copies. Under equality a canonical directory holding one more
  key cannot be told from one that lost a delete.
- `a_staging_holding_nothing_the_canonical_directory_lacks_is_removed` is now
  `a_staging_that_holds_the_canonical_state_or_nothing_is_removed`, with the removal cases a byte
  copy, a compacted copy, an empty file and a file cut inside its header. Its former cases "the
  canonical directory gained a write" and "a staged record torn" now refuse, in
  `a_staging_not_proved_to_hold_the_canonical_state_keeps_its_error_and_its_bytes`.
- `a_lone_online_manifest_temporary_is_removed_at_open` is now
  `a_lone_online_manifest_temporary_keeps_its_error_and_its_bytes`. It runs alone, beside another
  open instance of the family, and beside an open instance of another family, so it absorbs
  `a_lone_family_temporary_is_left_while_another_instance_of_the_family_is_open` and
  `an_open_instance_of_another_family_does_not_keep_a_familys_debris`, which are removed.
- `online_tests::a_finalized_prepared_split_by_its_source_move_restores_the_source` is now
  `..._keeps_its_error_and_its_bytes`.
- `online_tests::a_compaction_beside_another_instance_of_its_family_leaves_a_lone_temporary` is now
  `a_compaction_over_a_lone_family_temporary_keeps_its_error_and_its_bytes`, for each family, with
  and without another instance open.
- `online_tests::online_prepared_recovery_accepts_append_rotation_and_requires_finalization` calls
  `recover_prepared_online` with its `1eb9de5` arguments again.

### Controls at `1eb9de5`
Copied onto the `1eb9de5` copy, with their helpers and the two pauses. All passed there:
- a source that lost a delete under the claim, each family and each way, through a real
  compaction: `AuthorityUndetermined { store, staging }`, nothing changed;
- each planted state equality refuses (a canonical directory that gained a write, a staged
  delete, a torn staged record, a header-only staged file), each family: `AuthorityUndetermined`;
- a staged file damaged in its header, its first record or its last record, each family:
  `InvalidArtifact` (staging);
- a staging holding a fact the canonical directory lacks, now with a changed map entry value:
  `AuthorityUndetermined`; every staged family compared: `AuthorityUndetermined`; an unreadable
  staged file: `InvalidArtifact` (staging);
- a source replaced by a valid generation holding one more key: `AuthorityUndetermined`;
- a lone online temporary alone, beside another instance of the family, and beside an instance of
  another family: `AuthorityUndetermined`;
- a compaction over a lone family temporary, each family, with and without another instance open:
  `AuthorityUndetermined`, nothing changed;
- a finalized `Prepared` split after the first move and after every move: `AuthorityUndetermined`;
- the open still recovering while a later instance's cutover is parked between its moves: refused
  with `AuthorityUndetermined`, nothing changed, the cutover completing and its store accepting a
  write;
- a second process with its locks skipped during a live cutover: the child printed
  `Err(AuthorityUndetermined { .. })`, nothing changed, the cutover completing.

The rule tests that `1eb9de5` cannot pass (the removal control, and the working-directory test's
removal of the attempt's own staging) were each caught by a probe below.

### Probes
Each probe changed the final tree in place, ran the library's 125 `compaction::` tests, and
restored the file from a byte copy verified by sha256. Every source file matched its recorded
sha256 after the run.

| Probe | Review id | Caught by |
|---|---|---|
| T1 containment instead of equality | A, docs (b) | the lost-delete test, the not-proved test and the replaced-generation test (3 failed) |
| T2 a staged file rejected past its header holds nothing | | the rejected-file test |
| T3 no rule for a file shorter than a header (every staged file replayed) | | the removal control, the pre-`Prepared` cut tests (`StagingWrite`), the cut matrix and the compaction over debris (4 failed) |
| T4 a header-only file holds nothing | | the not-proved test, "KeyValue HeaderOnly" |
| B4 map entry values not compared | B4 | the changed KeyMap value case |
| S08a set members not compared | | 4 tests, KeySet cases |
| S08c key/value values not compared | | the changed KeyValue value case |
| B7 an unreadable staged file holds nothing | B7 | the unreadable staged-file test |
| B10 only the first staged file compared | B10 | `every_staged_family_is_compared` |
| S06 no comparison at all | | 9 tests |
| C1 the guard removes through the caller's spelling | C2, docs (c) | the working-directory test |

The withdrawal of D4 and D6 has no probe of its own: the rules are gone, and the RED above is
the two tests failing while they were present.

### Measured
D3's cost at open (debug build, Linux), with the review's workload: one key/value family of
1,000,000 keys in a 197,000,064-byte snapshot, and for the debris case a byte-identical staged
copy. Each open ran in its own process, twice, on a fresh copy of the same data, with the same
harness on the `1eb9de5` copy:

| Tree | Open | Result | Time | VmHWM |
|---|---|---|---|---|
| `1eb9de5` | no debris | `Normal` | 11.55 s, 11.81 s | 786 MiB, 786 MiB |
| `1eb9de5` | debris | `AuthorityUndetermined` | 10.53 s, 10.64 s | 811 MiB, 800 MiB |
| this change | no debris | `Normal` | 12.02 s, 12.13 s | 786 MiB, 786 MiB |
| this change | debris | `Recovered` | 24.73 s, 25.34 s | 883 MiB, 883 MiB |

The review measured 26.19 s and 1072 MiB for the debris open before the staged bytes were
dropped. Against plan.md IV's thresholds: a clean open keeps its peak (786 MiB) and is within 5%
of its `1eb9de5` time (12.0 and 12.1 s against 11.6 and 11.8 s, +3% on the means). The debris
open peaks at 1.12 times the clean open's, under the 1.25 threshold.


### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 714 passed,
  0 failed, 28 ignored across 31 binaries. That is 706 plus eight library tests: in
  `compaction::unpublished_attempt_tests` seven added (one of them the working-directory child,
  which does nothing unless a parent test starts it) and two removed, now 37; in `online_tests`
  three added (one of them a child), with the rewritten tests keeping their count. Library tests:
  443 passed, 9 ignored. The `compaction::` tests passed three runs in a row (125 each), and the CI
  step's command ran 37.
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features` after `cargo clean -p pigment-db`: no warnings. `cargo build --locked --release`
  and `--all-targets`: no warnings.
- macOS and Windows: not run (T011). This round adds to the module CI runs on every operating
  system a child process whose working directory changes while its compaction is parked, and a
  Unix-only unreadable staged-file test that skips as root; its two new process and race tests
  for the withdrawn rules are in `online_tests`, which CI runs on Linux.

## Review 4 (2026-10-03)
The fourth review reported three confirmed major findings, seven minor findings and nine nits (two
of them unconfirmed, and one recording what it checked and found sound). Each was checked here
against the tree, and each behaviour change was made test-first: RED is an assertion failure
observed before the production change. Same environment as above, with every build in a target
directory outside the repository. Every measurement "at `1eb9de5`" ran on a `git archive` copy of
that revision; its control tests ran there with the new tests and their helpers copied in, none of
it part of the change.

### Principle VI, first
Scan of this round's changes (the code, the tests, spec.md's Status line and Amendments, plan.md,
tasks.md, spec 008's contract and plan, and this section) for consumer and application
vocabulary: none. penpack appears only where the Principle VI record above says. The consumer's
finding id, which spec.md's FR-1 amendment, plan.md's D1 and a test's doc comment cited outside a
marked Motivation note (documents nit), is replaced there by "the motivating defect"; it now
appears only in the Motivation notes and in this record. No API, type, error, limit or format is
added. Crate-private only: `OpenDirectoryLease::identity` and `ClosedDirectoryClaim::identity`,
and a `locked` parameter on directory recovery, so `maintenance_coordination.rs` and the three
store files are no longer byte-identical to `1eb9de5` (the review's (a) record): each store passes
its lease's identity to recovery, one line each.

### Starting point
Before any edit: 714 passed, 0 failed, 28 ignored across 31 binaries, as "Review 3"'s "Final"
records.

### Design changes
This round changed design; it did not only fix code:
- **D3 compares bytes, not states.** A staged file is removed only when it is byte for byte what a
  closed compaction of the canonical directory stages for its family (the canonical state encoded
  as compaction encodes it, with the chain's timestamp metadata), or when it is shorter than a
  header and the replay rejects it or recovers no key. A canonical key with an empty set or map
  refuses. This narrows the third review's equality of states; each newly refused state keeps its
  `1eb9de5` error. It is what brought D3's peak back to the open's own.
- **D3 acts only in the directory its open or compaction locked.** It resolves the caller's path
  once, requires the result to equal the lease's or claim's identity, and reads and removes only
  through paths built from it. Directory recovery takes that identity as a parameter.
- **D3 checks the staged names before it validates the canonical directory.**
- **D1 resolves before it creates; D2 resolves once and creates, renames and removes through it.**
- Added, test-only: `recovery::recovery_pause` (generalized from `family_recovery_pause`) with the
  points `DiscardEntry` and `DiscardProved`; the `StagingCreate` cut moved to between the staging
  directory's creation and the guard's.

Withdrawing (a) or (b), the documents lens's alternative, was not needed: (a), the per-family
lease count, was already gone since the third review, and (b), D3's comparison, is kept in the
narrower byte form, which the spec's amendments now state and which awaits the same sign-off as
the earlier amendments.

### Baseline, before any production edit
Thresholds were written into plan.md IV first. The harness (a public-API program, release build,
one process per open, VmHWM from `/proc/self/status`) then measured `1eb9de5` and the third
round's tree on five workloads, each clean, with debris a closed compaction stages, and with that
debris cut by one byte. The third round's tree reproduced the review's figures: a map of
2,000,000 single-entry keys peaked at 3,724,816 kB against a clean 2,443,548 kB (1.52x), a set
of 2,000,000 single-member keys at 1.29x, a map of 200,000 x 10 entries at 1.20x, key/value at
1.08x; and four refusal rows exceeded the new time bound. plan.md IV has the whole table.

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| authority, major | D3 proves one directory and removes another's staging through the caller's spelling | Confirmed (RED below, an open and a compaction's start). Fixed: **design change**, D3 resolves once from the lease's or claim's identity. The review's fix as worded (canonicalize at D3's entry) was extended with the identity check, because a path changed before D3 starts would otherwise let it act, consistently but under no lock, in another directory (pinned by its own test). The device-and-inode check was declined: a rename of real directories on the resolved path defeats the lock files too, and is specs/011's to state; plan.md "Not done here". |
| authority, major | D3's memory threshold breached: 1.52x (map) and 1.29x (set) | Confirmed (baseline above). Fixed: **design change**, byte equality with what compaction stages; no staged state is built. Measured: every debris and refusal open within 1.07x of its clean peak, and within the time bound (plan.md IV). Rejected alternatives, each with its reason, in plan.md Decisions (streaming against a staged state, digests, a byte-identical shortcut, replaying into the open's structure). |
| documents, major | Return-semantics changes approved only by amendments awaiting sign-off | Confirmed. Not closable here: the sign-off is the approver's, and this agent cannot record it. spec.md's Status line now names the return-semantics changes (FR-7's `Io`, and the refusal after a staging write that fails over a changed source), says the amendments await sign-off, and says the change is not to be merged or released until the sign-off is recorded there. |
| authority, minor | A short headerless legacy file replays to a fact, and D3 deleted it | Confirmed (RED below). Fixed: a short file holds nothing only when the replay rejects it or recovers no key; at `1eb9de5` it is `InvalidArtifact` (staging), and so it is again. |
| authority, nit | Empty set and map keys count as no fact, though the open publishes them | Confirmed (RED below, the set case; the map case added). Fixed fail-closed: a canonical key with an empty collection refuses, with `1eb9de5`'s `AuthorityUndetermined`. |
| authority, nit (unconfirmed) | D1 resolves after `create_dir`; D2 creates and removes through the caller's spelling | Verified and reproduced (RED below for both, child processes). Fixed: D1 resolves before it creates and creates through the resolution; D2 resolves the temporary and the main manifest once and creates, renames and removes through them. |
| authority, nit (unconfirmed) | Under FR-11, D3 beside a live closed attempt is worse than plan.md said, and has no Physical barrier | Confirmed by reading; not reproduced. plan.md D3's "worst case" sentence replaced by the two residuals, and both added to "Not done here". No barrier added: directory recovery takes no durability policy, and the outcome without one is a refusal with every generation present, as for the other closed-recovery branches already listed. |
| authority, nit | Checked and sound: (a), (b) on deletions and torn canonical tails, (d) | Noted. (a)'s byte-identity record no longer holds for four files (Principle VI above), by one-line parameter changes. (b) now compares bytes; a canonical directory with a torn tail beside staging written from its accepted state is still removed (the capture and D3 replay it alike), and a staging "carrying the same torn byte" (a byte copy) now keeps its error. |
| probes, minor | D1's manifest-path resolution unpinned (C2) | Confirmed. `the_guard_checks_the_main_manifest_it_resolved_when_staging_was_created` (a child process, after the review's demonstrator); probe Q16. |
| probes, minor | Canonical sealed-segment order unpinned (B9c) | Confirmed. `a_key_rewritten_across_sealed_segments_does_not_keep_its_staging`; probe Q04. |
| probes, minor | The short-file boundary pinned only at 10 bytes (B8a) | Confirmed. A `HEADER_LEN - 1` case in the removal control; probe Q05. |
| probes, nit | The empty-collection clauses unpinned | Superseded: the clauses are gone, and the refusal that replaces them is pinned (probes Q09, Q09b, Q09c). |
| probes, nit | `PathEntry::Unknown` never reached | Accepted; plan.md III says where it is reachable. |
| documents, minor | A staging write failing over a changed source now refuses where `1eb9de5` opened | Confirmed: at `1eb9de5` the staging was removed and the next open answered `Ok(Normal)` without the family the source lost (measured here, below). Recorded in spec.md's Status line and FR-7 amendment and in plan.md III; pinned by `a_staging_write_that_fails_over_a_changed_source_keeps_the_staging_and_its_error`; probe Q20. |
| documents, minor | Thresholds set after measuring, on one workload; "roughly doubles" wrong | Confirmed. Thresholds rewritten before this round's measurements (memory kept at 1.25, a time bound added), six workloads including a two-family store and refusals, "roughly doubles" removed in both places. The third round's tree fails them; this round's passes (plan.md IV). |
| documents, minor | No deterministic test for two first opens racing through D3 | Confirmed. `two_first_opens_racing_through_the_discard_lose_nothing_and_leave_no_debris`, through the `DiscardProved` pause; probe Q21 shows it pins the documented answers. |
| documents, nit | Stale prose after FR-3 online and FR-5 were withdrawn | Corrected: FR-7's body, the first review's "move/restore" wording marked superseded, T005 and T007 annotated. |
| documents, nit | The consumer's finding id outside Motivation notes | Reworded in the three places (Principle VI above). |
| documents, nit | D3 validates the canonical directory before its cheap name check | Fixed for names: they are checked first, against a listing of regular files. A staged file that differs still pays D3's validation and the classification's; plan.md IV and "Not done here" state it. Probe Q22, which removes the check, is caught by nothing, as expected for a cost-only change. |

### RED
Observed on the tree before this round's production changes, with only the test infrastructure
above in place:
- `a_discard_removes_only_what_it_proved_in_the_directory_it_locked`: "Open: a discard that proved
  one/'s debris changed two/ (Ok)", `two/`'s staging gone.
- `a_discard_leaves_a_directory_its_open_or_claim_did_not_lock`: "Open: a discard acted in two/,
  which its open or claim did not lock (Ok)".
- `a_staging_not_proved_to_hold_the_canonical_state_keeps_its_error_and_its_bytes`, its new case:
  "KeyValue ByteCopy: expected its current refusal, got Ok(Recovered)".
- `a_short_staged_file_that_replays_to_a_fact_keeps_its_error_and_its_bytes`: "KeyValue: a 32-byte
  legacy staged file: expected its current refusal, got Ok(Recovered)".
- `a_canonical_key_left_with_no_members_keeps_the_staging_that_records_its_absence`: "KeySet: a
  canonical key with no members beside a staging without it: expected its current refusal, got
  Ok(Recovered)".
- `a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging`, its new
  `StagingCreate` case: "StagingCreate: a compaction of one/store changed two/, whose staging may be
  the only copy of a fact".
- `a_working_directory_change_during_a_publication_never_removes_another_directorys_temporary`: "a
  publication in one/ removed two/'s temporary" (left `None`).

The pins of existing behaviour passed there and are each caught by a probe below:
`the_guard_checks_the_main_manifest_it_resolved_when_staging_was_created`,
`a_key_rewritten_across_sealed_segments_does_not_keep_its_staging`, the one-byte-short case,
`a_staging_write_that_fails_over_a_changed_source_keeps_the_staging_and_its_error`,
`two_first_opens_racing_through_the_discard_lose_nothing_and_leave_no_debris`.

### Tests whose expectation or fixture changed
- Staged files that were byte copies of canonical files now hold what a compaction stages
  (`what_compaction_stages`, a compaction of a copy), in every control of
  `closed_debris_that_is_not_provably_unpublished_keeps_its_error_and_its_bytes`, in the removal
  failure test, the unreadable staging and staged-file tests, `every_staged_family_is_compared`,
  `a_staging_holding_a_fact_the_canonical_directory_lacks_keeps_its_error_and_its_bytes` and
  `a_staged_file_the_replay_rejects_past_its_header_keeps_its_error_and_its_bytes`, and in the
  `NotProved` cases. A byte copy would now refuse for its bytes, whatever each control names, so
  each control would no longer test its own condition. The symlink-entry control's target lives
  under a path longer than a header, so the link's own length cannot make it a short file.
- `a_staging_that_holds_the_canonical_state_or_nothing_is_removed` is now
  `a_staging_that_is_what_compaction_stages_or_holds_nothing_is_removed`: its byte-copy case moved
  to the refusals (`NotProved::ByteCopy`), and a one-byte-short-of-a-header case was added.
- `recovery_tests::untrusted_manifest_evidence_distinguishes_ambiguity_from_invalid_debris_without_mutation`:
  its manifest-less staging case, a byte copy, is back to its `1eb9de5` text (the classifier answers
  `AuthorityUndetermined` and changes nothing); a byte copy is no longer debris D3 removes.
- `a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging` runs at
  `StagingCreate` as well as `StagingSync`.

### Controls at `1eb9de5`
Copied, with their helpers, onto the `1eb9de5` copy. Passing there, with these errors:
- a byte-copy staged file, and every other `NotProved` case: `AuthorityUndetermined`;
- a 32-byte legacy staged file, key/value and key/set: `InvalidArtifact` (staging);
- a canonical key with no members or entries beside a staging without it, set and map:
  `AuthorityUndetermined`;
- every closed control with its staging rewritten as above, `every_staged_family_is_compared`, the
  fact-the-canonical-directory-lacks test, the rejected-past-its-header test, and the unreadable
  staging and staged-file tests: their recorded errors.

`a_staging_write_that_fails_over_a_changed_source_keeps_the_staging_and_its_error` fails there, as
it must: "the failed attempt removed the only copy of the family its source lost". Instrumented in
that copy, the next open answered `Ok(Normal)` with no staging left: the behaviour change the
documents lens recorded.

### Probes
Each probe changed a scratch copy of the final tree, ran the library's 136 `compaction::` tests,
and restored the file from a byte copy verified by sha256; the copy matched its recorded sha256
after the run. The probes ran first on the tree before `cargo fmt` and the explicit type check
below, then all again on the final tree (R03c and R03d there only); every probe present in both
runs gave the same result.

| Probe | Review id | Caught by |
|---|---|---|
| Q01 D3 removes through the caller's spelling | authority major | `a_discard_removes_only_what_it_proved_in_the_directory_it_locked` |
| Q02 D3 not compared with the lock's identity | | `a_discard_leaves_a_directory_its_open_or_claim_did_not_lock` |
| Q03 D3 reads and removes through the caller's spelling | | the removes-only test |
| Q04 sealed segments replayed in reverse | B9c | `a_key_rewritten_across_sealed_segments_does_not_keep_its_staging` |
| Q05 header threshold halved | B8a | the removal control (one byte short of a header) |
| Q06 every short file holds nothing | authority minor | the short legacy-file test |
| Q07 a short file the replay rejects proves nothing | | the removal control |
| Q08 a short file that replays holds nothing | | the short legacy-file test |
| Q09, Q09b, Q09c empty keys not refused (both, map only, set only) | authority nit | the empty-key test |
| Q10 a staged file shorter than the encoding passes | | 3 tests (lost delete, replaced generation, not proved) |
| Q11 a staged file longer than the encoding passes | | 5 tests |
| Q12 only the length compared | | the byte-copy case and the unreadable staged file |
| Q13 only the first staged file compared | B10 | `every_staged_family_is_compared` |
| Q14 an unreadable staged file holds nothing | B7 | the unreadable staged-file test |
| Q15 D1 resolved after `create_dir` | authority nit | the working-directory test at `StagingCreate` |
| Q16 D1 reads the manifest through the caller's spelling at drop | C2 | the guard manifest test |
| Q17 D1 removes through the caller's spelling | C1 | the working-directory test |
| Q18 D2 removes through the caller's spelling | authority nit | the publication test |
| Q20 D1 without the generation check | F2 | 6 tests, the staging-write test among them |
| Q21 a removal that finds nothing counts as removed | documents minor | the race test |
| Q22 staged names not checked first | documents nit | not caught: cost only |
| P01, P02 (all three name checks), P05, R04, R05, S11 | earlier rounds | the closed controls (S11 also two others) |
| R03 regular-file check in the staging listing removed | earlier rounds | not caught alone |
| R03d regular-file check in the comparison removed | | not caught alone |
| R03c both removed | | "staging holds a symlink named for an active segment: ... got Ok(Recovered)" |

R03 and R03d are two checks of one property, each masking the other; the comparison's check was
made explicit in this round because its reads follow links, where before the property held only
by accident of a short link's length and a directory's read failing. R03c pins the pair.

### Measured
plan.md IV records the table. In short, against thresholds written before measuring: clean opens
keep their peak (within 0.2%) and their time (-10% to +3.3% against `1eb9de5`); debris and
refusal opens peak at 1.00x to 1.07x of the clean open (the third round's tree: up to 1.52x); the
slowest open against its time bound is the map refusal, 24.6 s against 25.5 s. The measured tree
differs from the final one only by `cargo fmt`, the comparison's explicit regular-file check, test
fixtures and comments.

### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 725 passed,
  0 failed, 28 ignored across 31 binaries. That is 714 plus eleven library tests, all in
  `compaction::unpublished_attempt_tests` (now 48, four of them children that do nothing unless a
  parent test starts them, two added here). Library tests: 454 passed, 9 ignored. The `compaction::` tests passed
  three runs in a row (136 each), and the CI step's command ran 48.
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features` on a target directory that had never checked this crate: no warnings. `cargo
  build --locked --release` and `--all-targets`: no warnings.
- macOS and Windows: not run (T011). This round adds to the module CI runs on every operating
  system: two child processes that change their working directory, two tests that retarget a
  directory symlink (they skip where the platform refuses the link), and a publication that now
  creates, renames and removes through canonicalized paths, which on Windows are verbatim (`\\?\`)
  paths; the D1 guard already removed through those since the second review.

## Review 5 (2026-10-03)
The fifth review reported three confirmed major findings (two from the probes lens, one from the
documents lens) and thirteen others: from the authority lens two minor findings, one nit and two
records of what it checked and found sound; from the probes lens two minor findings and a nit;
from the documents lens three minor findings and three nits. Each was checked here against the
tree. Each behaviour change was made test-first, and RED is an assertion failure observed before
the production change. Each pin of existing behaviour was observed passing before any production
edit of this round and is caught by a probe of the rule it pins. Same environment as above, with
every build in a target directory outside the repository. The states behind "at `1eb9de5`" were
planted by this round's tests in a scratch copy of the final tree, exported, and opened by a
scratch program built against a `git archive` copy of `1eb9de5`; the CRC-32 control ran there as a
test copied in. None of that is part of the change.

### Principle VI, first
Scan of this round's changes (the code, the tests, the frozen fixture and its README, the example
harness, spec.md, plan.md, tasks.md, spec 008's contract and plan, and this section) for consumer
and application vocabulary: none. penpack appears only where the Principle VI record above says.
The fixture's keys and values (`alpha`, `group`, `book`, ...) are opaque test data. No public API,
type, error, limit or format is added; `CompactionOperation::Cleanup`'s rustdoc is widened. The
crate-private additions are `V2CodecProbe::record_encoded_len`, D3's `ends_inside_a_record`, D1's
`generation_is_exactly`, and `CapturedGeneration::source_bytes` becoming an `Arc`; test-only, the
cut `StagingWriteTorn`, `fault_checkpoint::maintenance_fault_requested` and the pause point
`DiscardIdentified`.

### Starting point
Before any edit: 725 passed, 0 failed, 28 ignored across 31 binaries, as "Review 4"'s "Final"
records (re-run here on the tree as handed over).

### Design changes
This round changed design; it did not only add tests:
- **D3 removes a torn write of what compaction stages.** A staged file is removed when it is byte
  for byte what a closed compaction of the canonical directory stages, or the first bytes of that
  encoding ending inside its header or inside one of its records; one ending right after the
  header or on a record boundary keeps its `1eb9de5` error, as does any other file. This replaces
  the fourth review's short-file rule (a file under 64 bytes that the replay rejects or that
  recovers no key), so D3 no longer replays a staged file at all. It removes one more kind of state
  than before (a staging write killed inside its bytes, `AuthorityUndetermined` at `1eb9de5`) and
  refuses one more (short headerless files holding records, which the fourth review's rule
  removed; `InvalidArtifact` at `1eb9de5`).
- **D1 compares the capture's bytes**, through the capture shared with the guard, not each file's
  length and CRC-32.
- **Not changed, documented:** the label of D3's removal error at an open (`Inspect`), the encoder
  dependency (now pinned by a frozen fixture), `PathEntry::Unknown`'s reachability, the residual
  of a rename of real directories (now this spec's, not specs/011's).
- Withdrawing (a) or (b) was not needed: (a), the per-family lease count, has been gone since the
  third review (the authority lens confirms it: `maintenance_coordination.rs` differs from
  `1eb9de5` only by the two crate-private `identity()` getters); (b), D3's comparison, is kept in
  its byte form, extended to torn writes, which the spec's amendments state and which awaits the
  same sign-off as the earlier ones.

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| probes, major | D3's byte comparison unchecked past the header and the first block (B2, B3) | Confirmed: every same-length fixture differed in length or inside the header, and none spanned a second block. Pinned: `a_staging_differing_in_one_fact_of_the_same_length_keeps_its_error_and_its_bytes`, each family, a changed value, member or entry value, a renamed key and (sorted map) a changed entry key, in a one-fact store and in a store whose staged file spans many blocks, the first difference past 64 KiB there. Probes B2 and B3 caught. |
| probes, major | No staged file larger than one block anywhere (B4) | Confirmed. The removal control now runs every case on a store of 3,001 records per family as well (staged files of about 0.9 MB), and `every_pre_prepared_cut_of_a_store_larger_than_a_block_reopens_the_untouched_source` kills a compaction of such a store at every pre-`Prepared` cut, each family. Probe B4 caught by both. |
| documents, major | A kill inside the staging write's bytes leaves the store unopenable; the plan's reasons for declining were stale | Confirmed: the `StagingWriteTorn` kill (RED below) answered `AuthorityUndetermined` for every family. Fixed: **design change**, the torn-write rule above, test-first. The plan's "no cut produces" reasoning is replaced (Decisions), spec.md's Acceptance and amendments say what the rule covers, and spec 008's contract states its two exceptions (a kill exactly on a record boundary or right after the header, and a canonical key with an empty set or map). The review's probe B was sound but builds a staged state; the tear is read from the expected encoding's record lengths instead. |
| authority, minor | D1 trusts length and CRC-32 where the attempt has just proved the bytes changed | Confirmed (RED below, every family). Fixed: `generation_is_exactly`; the capture's bytes are shared with the guard through an `Arc`, no copy. Rejected: keeping the staging whenever revalidation failed, which misses CRC-preserving damage when the attempt fails elsewhere. At `1eb9de5` the staging was kept and the next open named both, as now. |
| authority, minor | D3's short-file rule removed short legacy files holding intact records | Confirmed (RED below: four of five cases removed, key/value, key/set and key/sorted-map). Fixed by the torn-write rule: a short file is removed only when it is the first bytes of what compaction stages. All five keep `1eb9de5`'s `InvalidArtifact` naming the staging. |
| authority, nit | FR-7's removal error is labelled `Inspect` at an open | Confirmed by reading `map_compaction_recovery_error`. Documented, not remapped: spec.md's FR-7 amendment and plan.md III state the labels (`Cleanup` from a compaction's start, `Inspect` from an open, as for every compaction-side I/O error directory recovery returns to an open since before this spec). Remapping D3's alone needs a second error path for one label and leaves the older removals saying `Inspect`. |
| authority, nit | Checked and sound: (a) withdrawn; exclusion across spellings; no leaked lease | Noted. |
| authority, nit | Checked and sound: (b) loses nothing for staged files of at least a header | Noted; this round adds the torn prefixes, whose soundness plan.md's Decisions argue, and the probes below test both directions (T1, T2, T4). |
| probes, minor | D3's proof reads could move to the caller's spelling (E2, E3, E6) | Confirmed. `every_read_of_the_discards_proof_is_in_the_directory_it_locked`, three fixtures (an unproved staging, a previous generation, a canonical directory elsewhere that matches the staging), for an open and a compaction's start, through a new test-only pause `DiscardIdentified` right after the identity check. Probes E2, E3 and E6 caught. |
| probes, minor | D2's rename half unpinned (C7, C8) | Confirmed. `a_working_directory_change_during_a_publication_renames_only_into_its_own_directory` (a child process; the working directory moves after `Written`; the publication succeeds; `two/` byte-identical, `one/`'s manifest is the encoded bytes, no temporary left). Probes C7 and C8 caught. |
| probes, nit | D1's manifest read at creation unpinned (C4) | Confirmed. The `StagingCreate` case of the working-directory test plants a main manifest in `one/` while the child is parked, before the working directory moves; the guard must remove `one/`'s staging. Probe C4 caught. |
| documents, minor | The rename-of-real-directories residual cited to specs/011, which does not state it | Confirmed (specs/011's Known limitations name symlink repointing, inode paths and mount views). The citation is dropped in plan.md D1, D3, Decisions and "Not done here", and the residual is recorded as this spec's. |
| documents, minor | The performance harness was never checked in | Confirmed. `examples/open_with_debris.rs` (std and the public API only), with its commands and workloads in its documentation and in plan.md IV, and this round's measurements made with it. |
| documents, minor | D3 depends on the encoder's bytes across releases, unstated and unpinned | Confirmed. Stated in plan.md III, Decisions and "Not done here", spec.md's amendments and spec 008's contract. Pinned by the frozen fixture `tests/fixtures/unpublished_closed_staging` (a store of all three families and what `1eb9de5`'s closed compaction staged for it, both written by `1eb9de5`) and `debris_staged_by_an_earlier_revision_is_removed_at_open`; probe EN2, an encoder change that keeps snapshots valid, is caught by it. |
| documents, nit | `PathEntry::Unknown` is reachable, contrary to plan III | Confirmed. plan.md III corrected; pinned by `a_canonical_directory_that_cannot_be_searched_keeps_its_error_and_its_bytes` (Unix, skips where the process may search such a directory); probe U1 shows the test reaches that arm. |
| documents, nit | `CompactionOperation::Cleanup`'s rustdoc does not cover D3's removal | Confirmed. The rustdoc is widened (doc only). |
| documents, nit | The contract's "every pre-`Prepared` cut reopens" has exceptions | Confirmed. spec 008's fault-evidence paragraph now states both exceptions, and the checkpoints it lists include a cut inside a staging write's bytes. |

### RED
Observed on the tree with this round's test infrastructure in place (the `StagingWriteTorn` cut,
the `DiscardIdentified` pause, the helpers) and before the production change named:
- Torn-write rule:
  - `every_pre_prepared_cut_reopens_the_untouched_source`: "[KeyValue] StagingWriteTorn:
    Err(AuthorityUndetermined { .. })", and the same for `[KeySet]`, `[KeyMap]` and all three.
  - `every_pre_prepared_cut_of_a_store_larger_than_a_block_reopens_the_untouched_source`:
    "KeyValue StagingWriteTorn: Err(AuthorityUndetermined { .. })", and the same for the two
    other families.
  - `every_closed_checkpoint_process_exit_reopens_exact_state_or_preserves_explicit_evidence`:
    "KeyValue Prepared StagingWriteTorn: a cut before Prepared must reopen the untouched source,
    not AuthorityUndetermined".
  - the removal control, all twelve torn cases (three families, two sizes, `TornInsideARecord`
    and `TornShortOfItsLastByte`): `Err(AuthorityUndetermined { .. })`.
  - `a_short_staged_file_holding_records_compaction_never_writes_keeps_its_error_and_its_bytes`:
    `Ok(Recovered)` for a key/value put and its delete (47 bytes, the replay recovers no key), a
    key/value put before a rejected record (62 bytes), a key/set append before a rejected record
    (61 bytes) and a key/sorted-map entry before a rejected record (62 bytes); a key/set append and
    its removal (62 bytes, which the replay keeps as a key with no members) kept its error there
    too.
- D1: `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging`: "KeyValue:
  a compaction whose source changed in its bytes, but not in its length or its CRC-32, removed its
  staging, the only intact copy of the captured state".

The pins passed there before any production edit of this round, and each is caught by its probe
below.

### Tests whose expectation or fixture changed
- `NotProved::TornRecord` (refused) is now the removal control's `TornShortOfItsLastByte`, beside a
  new `TornInsideARecord`; the control is renamed
  `a_staging_that_is_what_compaction_stages_or_a_torn_write_of_it_is_removed` and runs on both
  store sizes. `NotProved::CutAtARecordBoundary` is new (a complete snapshot of fewer facts).
- `PRE_PREPARED_CUTS` has seven cuts, the `recovery_tests` matrix one more point, and the
  fault-checkpoint identifier test fourteen cuts.
- The `StagingCreate` working-directory case plants a main manifest in `one/`.
- Doc comments of the short-file and replay-rejects tests now state the torn-write rule.

### Controls at `1eb9de5`
Each state planted by this round's tests, opened with `1eb9de5`, left the parent directory
byte-identical apart from the open's lock files, with these errors:

| State | At `1eb9de5` | Now |
|---|---|---|
| one same-length fact changed, 14 cases (each family; fixture and large store) | `AuthorityUndetermined` | the same |
| staged file cut at a record boundary (each family; fixture, and large past 128 KiB) | `AuthorityUndetermined` | the same |
| a canonical directory that cannot be searched (each family) | `AuthorityUndetermined` | the same |
| five short legacy staged files | `InvalidArtifact` (staging) | the same |
| a torn write of what compaction stages, 12 cases | `AuthorityUndetermined` | `Recovered`, staging removed |
| the frozen fixture, opened as each family | `AuthorityUndetermined` | `Recovered`, staging removed |
| CRC-32-preserving damage under the claim (each family, a test copied in) | staging kept; next open `AuthorityUndetermined` naming both | the same |

### Probes
Each probe changed a scratch copy of the final tree, ran the library's `compaction::` and
`test_support::` tests (171), and restored the file from a byte copy verified by sha256; the copy's
sources matched their recorded sha256 after the run. Every probe was caught.

| Probe | Review id | Caught by |
|---|---|---|
| B2 only the header and the length compared | probes major | the same-length test, and two others (a lost delete, not proved) |
| B3 only the first 64 KiB block compared | probes major | the same-length test |
| B4 every block compared with the encoding's first block | probes major | the removal control (large store) and the large-store kill test |
| E2 the staging compared at the caller's spelling | probes minor | `every_read_of_the_discards_proof_is_in_the_directory_it_locked` |
| E3 the previous generation checked at the caller's spelling | probes minor | the same |
| E6 the expected bytes from the caller's spelling | probes minor | the same |
| C4 the guard's manifest at creation through the caller's spelling | probes nit | the working-directory test (`StagingCreate`) |
| C7 the rename from the caller's spelling | probes minor | `a_working_directory_change_during_a_publication_renames_only_into_its_own_directory` |
| C8 the rename to the caller's spelling | probes minor | the same |
| T1 any prefix of the encoding accepted | documents major (the review's probe A) | 4 tests: a replaced generation, a lost delete, the record-boundary test, not proved |
| T2 torn writes refused again | documents major | 5 tests: both kill matrices, the large-store kill test, the compaction-start test, the removal control |
| T3 the end of the header read as torn | | not proved (`HeaderOnly`) |
| T4 record boundaries read as torn | | the same 4 tests as T1 |
| T5 record lengths read without their payload | | 9 tests |
| T6 every short file holds nothing | authority minor | both short-file tests |
| G1 the guard back to length and CRC-32 | authority minor | `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging` |
| G2 the captured bytes not compared | authority minor | the same |
| EN1 the snapshot header's segment id changed | documents minor | 90 tests (the change makes snapshots invalid; too wide to say anything about the fixture) |
| EN2 key/value snapshot records written in reverse order | documents minor | `debris_staged_by_an_earlier_revision_is_removed_at_open`, and the same-length test (its last-sorting key moves) |
| U1 `PathEntry::Unknown` panics | documents nit | `a_canonical_directory_that_cannot_be_searched_keeps_its_error_and_its_bytes` |
| B5, B5b, B6, B6d, B7, B9, B10 (earlier rounds' D3 probes, on the final comparison) | earlier rounds | B5 10 tests, B5b 6, B6 5, B6d the sealed-segments test, B7 the empty-key test, B9 `every_staged_family_is_compared`, B10 4 tests |

B2, B3 and B4 were also run on the fourth round's comparison, and E2, E3, E6, C4, C7 and C8 on
code this round does not change, before any production edit of this round, over the module's
tests: each was caught there by the test named above, and by nothing else.

### Measured
plan.md IV records the table and the harness's commands. In short, against the thresholds set in
the fourth round and not changed: every debris, torn and refusal open is within its time bound and
peaks at most 1.07 times its clean open; a torn staging (the fourth round's "refusal" shape), which
the baseline refused, is now removed at a removed debris's cost. One clean row was 7.4% slower than
`1eb9de5` in one slow run of two, on a code path this round does not change; re-measured with four
interleaved runs per tree, every clean row is within 5% of `1eb9de5` by the slower run and by the
median, and every clean peak within 0.2%. The measured final binary was built before the comparison
function's rename, the doc comments and the compaction-start test's extra cut, which change no
release code.

### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 735 passed,
  0 failed, 28 ignored across 32 binaries. That is 725 plus ten library tests, all in
  `compaction::unpublished_attempt_tests` (now 58, five of them children that do nothing unless a
  parent test starts them, one added here); the 32nd binary is the new example, which has no
  tests. Library tests: 464 passed, 9 ignored. The CI step's command ran the module's 58; the
  `compaction::` tests passed three runs in a row (146 each).
- Doc tests: 9 passed. `cargo fmt --check`: clean. `cargo clippy --locked --all-targets
  --all-features` on a target directory that had never checked this crate: no warnings. `cargo
  build --locked --release` and `--all-targets`: no warnings.
- macOS and Windows: not run (T011). This round adds to the module CI runs on every operating
  system: stores of 3,001 records per family (a few seconds each), a child process that changes
  its working directory during a publication that then succeeds, a symlink retargeted at the new
  pause (it skips where the platform refuses the link), a frozen fixture read with
  `include_bytes!`, and one Unix-only control. The example builds everywhere and reads
  `/proc/self/status` only where it exists.

## Review 6 (final) (2026-10-03)
The final review reported no confirmed blocker or major finding, and sixteen others: from the
authority lens two minor findings and three nits (two of them records of what it checked and found
sound); from the probes lens two minor findings and two nits; from the documents lens two minor
findings and five nits. Each was checked here against the tree. None needed a production change
to be dispositioned. One needs a production change to be closed (authority, minor: real kills stop
on page or folio boundaries); it is recorded in plan.md's "Not done here" with its evidence and
proposed fix, and not made. No production behaviour changed: the only production-file edit is D3's
rustdoc, comments only, and the example's documentation. Each new pin was observed passing on the
tree as handed over, with the tests added, and caught by a probe of the property it pins; the new
planted states were opened by `1eb9de5`. Same environment as above, with every build in a target
directory outside the repository.

### Principle VI, first
Scan of this round's changes (the tests, the regenerated fixture and its README, the example's
documentation, D3's rustdoc, spec.md, plan.md, tasks.md, spec 008's contract, and this section)
for consumer and application vocabulary: none. penpack appears only where the Principle VI record
above says. The consumer's finding id, which "Review 2"'s RED evidence named outside a marked
note (documents nit), is replaced there by "the motivating defect". The fixture's keys and values
(`aardvark`, `zz`, `a-long-member`, ...) and the page-boundary test's keys are opaque test data.
No API, type, error, limit or format is added, and no production code changes.

### Starting point
The tree as handed over: every one of its 35 changed and new paths had the sha256 the final
review's documents lens recorded before it ran the full suite there (735 passed, 0 failed,
28 ignored across 32 binaries). A byte copy of it was kept, and every comparison below is against
it.

### Dispositions
| # | Finding | Disposition |
|---|---|---|
| authority, minor | D1's exact-byte comparison of sealed segments covered by no test (G3) | Confirmed. `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging` now also damages the first sealed segment of a segmented store (`CrcPreservingDamage::SealedSegment`, each family). G3 caught by exactly those three cases. |
| authority, minor | Real kills stop at page or folio boundaries, so the record-boundary exception is reached at a fixed rate (one in three for 96-byte records) | Confirmed by reading the rule and the review's measurements; not reproduced here with a real kill. Not closed: it needs a production change, which this round may not make. Recorded in plan.md's "Not done here" with the review's evidence and its proposed fix (`set_len` before the staging write, and D3 removing the encoding's first bytes followed by zeros), for its own RED, measurement and sign-off. Documented in spec.md (Acceptance, a final-review amendment), plan.md (III, Decisions, the fifth review's residual corrected) and spec 008's fault-evidence paragraph. Pinned: `a_staging_write_killed_on_a_page_boundary_keeps_its_error_only_where_a_record_ends` (a store of 290 records of 96 bytes; of the six 4 KiB multiples of its staged file, the two on a record boundary keep `AuthorityUndetermined` and every byte, the four inside a record are removed). Probes T4 and T2 caught it. |
| authority, nit | D3's rustdoc gives the wrong reason why a torn prefix is safe | Confirmed. Corrected in D3's rustdoc, plan.md's D3 and Decisions, and in place in spec.md's fifth-review amendment, marked there: the prefix records the absence of every key sorting between its complete records, and it is the byte comparison with the current encoding that makes those absences the canonical directory's; the cut record or header records nothing beyond the cut. |
| authority, nit | Checked and sound: the torn-write rule; expected bytes deterministic | Noted. |
| authority, nit | Checked and sound: D1's exact comparison keeps nothing alive longer, raises no peak | Noted. The optional block-wise comparison is recorded in plan.md's "Not done here" (not a regression). |
| probes, minor | The frozen fixture pins no key or member order (EN3, EN4, EN5 survive the suite) | Confirmed. The fixture is regenerated with the README's procedure by a scratch program built against a `git archive` of `1eb9de5` (copied `Cargo.lock`), before it is first committed: every earlier shape kept, plus keys whose order by length differs from their byte order in each family, sets of several members of different lengths in two keys, and maps of several entries in three keys. The README's table and the test's contents are updated. EN3, EN4, EN5 and EN6 now fail the fixture test, and EN2 still does. Opened as debris by `1eb9de5`: `AuthorityUndetermined`, staging kept, each family. |
| probes, minor | D1 pinned only on the active file of a one-family store (G3, G4) | Confirmed. The same test now runs each family's active file damaged in a three-family store (`CrcPreservingDamage::OneOfThreeFamilies`); G4 caught by the key/set and key/sorted-map cases, G3 as above. |
| probes, nit | Nothing pins that the guard compares the capture's own bytes (G6) | Confirmed. The same test runs the damage while the attempt is parked at `StagingCreate`, before its guard exists (`CrcPreservingDamage::ActiveFileBeforeTheGuard`, each family); G6 caught by exactly those three cases. The test now reports every case that removed its staging instead of stopping at the first. |
| probes, nit | D3's comparison unpinned in the middle of a multi-block file (P5c) | Confirmed. The same-length test adds `SameLength::MiddleFact` for large stores: the value of the middle-sorting key, its last member, or its last entry's value, with the difference asserted to lie neither in the first two nor in the last two 64 KiB blocks. P5c caught there (KeyValue: `Ok(Recovered)`). |
| documents, minor | The Status line's sign-off gate omits the FR-4 widening (2026-10-02), which changes return semantics | Confirmed: the approved text has no Amendments section. Documented, not in the Status line, which is the requester's: spec.md's closing amendment now says the two 2026-10-02 amendments were made during implementation too, names FR-4's second window (`AuthorityUndetermined` at `1eb9de5`; now `Recovered`, or the manifest removal's `Io`) and says the Status line does not yet name it; plan.md III says the same. **The requester should add it to the Status line when recording the sign-off.** |
| documents, minor | The empty-key refusal is documented for the whole directory; the code applies it per staged family | Confirmed by reading `staged_bytes_for`, which runs per staged file. The documents are narrowed to the implemented rule: spec 008's classification bullet and fault-evidence exception, plan.md D3 and III (three places), spec.md's fourth-review amendment (marked) and a final-review amendment. No code change. |
| documents, nit | "Every pre-`Prepared` cut recovers" statements omit the exceptions | Confirmed. spec.md's fourth-review amendment (marked), spec.md's Acceptance and plan.md III now name both exceptions, the first narrowed as above, and the page granularity. |
| documents, nit | Three test doc comments state the pre-round-5 rule | Confirmed. The doc comments of `a_staging_not_proved_...`, the same-length test and the fixture test, and `NotProved::ByteCopy`'s, now include the torn-write case. |
| documents, nit | V415 named outside a marked note | Confirmed. "Review 2"'s RED line now says "the motivating defect's own failure". |
| documents, nit | The harness's build command fails on a fresh checkout (`Cargo.lock` untracked) | Confirmed (`.gitignore` lists it). The example's documentation and plan.md IV say to copy the working tree's `Cargo.lock` into the other revision's copy; the base build of this round was made so. |
| documents, nit | IV's clean-time pass rests on a re-measurement with a changed procedure | Confirmed. plan.md IV states the change as a decision with its reason. The original procedure (two runs per tree, the slower run) was run once more here, `1eb9de5`'s harness binary against this tree's, with nothing else running but a desktop: every clean row within 5% (map1 +2.2%, set1 -1.4%, map10 +0.8%, kv -0.2%, multi (set) -0.7%, multi (kv) -1.0%), every peak within 0.05%. |

### Tests added or changed
- `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging`: twelve cases
  (each family: the active file, a sealed segment, the family's active file in a three-family
  store, the active file before the guard exists), through a new helper
  `change_the_source_under_the_claim_at`; it reports every case that removed its staging.
- `a_staging_differing_in_one_fact_of_the_same_length_keeps_its_error_and_its_bytes`:
  `SameLength::MiddleFact` in each large store, the middle key chosen in sorted order
  (`middle_sorting_key`; the snapshot maps are `HashMap`s, so their own order is no order). The
  first cut took the map's iteration order and put the key/sorted-map difference in the last
  block, which the test's own assertion refused.
- `a_staging_write_killed_on_a_page_boundary_keeps_its_error_only_where_a_record_ends`: new.
- `debris_staged_by_an_earlier_revision_is_removed_at_open`: the regenerated fixture (ten files,
  the key/set family with two sealed segments) and its contents.
- Doc comments, as in the table.

### Controls at `1eb9de5`
| State | At `1eb9de5` | Now |
|---|---|---|
| the regenerated frozen fixture, opened as each family | `AuthorityUndetermined`, staging kept | `Recovered`, staging removed |
| the six page-multiple cuts of the 96-byte-record store's staged file | `AuthorityUndetermined` for all six, staging and store unchanged (a scratch program built against `1eb9de5`) | the two on a record boundary the same; the four inside a record `Recovered`, staging removed |
| CRC-32-preserving damage: sealed segment, three families, before the guard | not run: at `1eb9de5` only a staging write error removed a staging (plan.md, Technical context), so a validation failure always kept it | staging kept; the next open `AuthorityUndetermined` naming both |
| a same-length difference in a middle block | not run: the shape of the fifth review's fourteen same-length cases, all `AuthorityUndetermined` there | `AuthorityUndetermined`, unchanged |

### Probes
Each probe is the final review's (its script's definitions, imported unchanged), applied to a
scratch copy of this tree with these tests, run over the library's `compaction::` tests (147
unprobed, all passing), and restored from a byte copy verified by sha256; every source file of the
copy matched its recorded sha256 after each batch. The copy's `mod.rs` predates the rustdoc
correction, which changes comments only. The four G probes were run again over the D1 test alone
once it reported every case; the case lists below are from that run.

| Probe | Review id | Caught by |
|---|---|---|
| G1 the guard back to length and CRC-32 | fifth review | the D1 test, all twelve cases |
| G3 exact comparison only for active files | authority minor, probes minor | the D1 test: `SealedSegment`, each family |
| G4 exact comparison only for the first captured file | probes minor | the D1 test: `OneOfThreeFamilies` for key/set and key/sorted-map |
| G6 the guard re-reads the source when armed | probes nit | the D1 test: `ActiveFileBeforeTheGuard`, each family |
| P5c only the first and last 64 KiB compared | probes nit | the same-length test, at `KeyValue Large MiddleFact` (`Ok(Recovered)`) |
| B3 only the first block compared | fifth review | the same-length test |
| EN2 key/value records reversed | fifth review | the fixture test and the same-length test |
| EN3 keys sorted by length, then bytes | probes minor | the fixture test |
| EN4 set members reversed within a key | probes minor | the fixture test |
| EN5 set members sorted by length, then bytes | probes minor | the fixture test |
| EN6 map entries reversed within a key | probes minor | the fixture test |
| T4 record boundaries read as torn | authority minor | 5 tests, the page-boundary test among them (the cut at 4096, a record boundary: `Ok(Recovered)`) |
| T2 torn writes refused again | authority minor | 6 tests, the page-boundary test among them (the cut at 8192, inside a record: `AuthorityUndetermined`) |

In the final review's runs G3, G4, G6 and P5c survived every `compaction::` test, and EN3, EN4
and EN5 the whole suite; each now fails a test written for it.

### Measured
plan.md IV records the decision and the re-measurement. In short: the clean opens, by the original
procedure (two runs per tree, trees alternating, the slower run), `1eb9de5`'s harness binary
against this tree's, are within 5% in every row and their peaks within 0.05%. Debris, torn and
refusal opens were not re-measured: no production code changed.

### Final
- Full suite (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 736 passed,
  0 failed, 28 ignored across 32 binaries. That is 735 plus the page-boundary test. Library tests:
  465 passed, 9 ignored. The CI step's command ran the module's 59; the `compaction::` tests
  passed three runs in a row (147 each), and once more unprobed in the probe copy.
- Doc tests: 9 passed. `cargo fmt --check`: clean (after `cargo fmt` reformatted two of this
  round's test statements). `cargo clippy --locked --all-targets --all-features` on a target
  directory that had never checked this crate: no warnings. `cargo build --locked --release` and
  `--all-targets`: no warnings.
- macOS and Windows: not run (T011). This round adds to the module CI runs on every operating
  system: nine more paused child compactions (the D1 test's new cases), one more large-store
  same-length case per family, and a store of 290 records opened six times.
