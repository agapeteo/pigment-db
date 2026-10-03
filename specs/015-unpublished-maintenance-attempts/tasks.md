# Unpublished maintenance attempts: tasks

Each task observes RED for its behaviour, as an assertion failure, before the production change
that satisfies it. Test infrastructure (T002) precedes the first RED. The over-reach controls are
written first and must pass at `1eb9de5` and after every task: each keeps its current error and
leaves the parent directory byte-identical apart from the lock files the open itself takes.

- [x] T001 Write the plan and tasks. Record the suite baseline at `1eb9de5`.
- [x] T002 Test infrastructure, no behaviour change:
  - a manifest-publication fault seam (`publication::manifest_publication_faults`), keyed by the
    temporary's path and the publication's ordinal, failing or panicking at a stage;
  - `StagingWrite` fires once the first family's active file is created, before its bytes;
  - `RollbackRestore` and `RollbackCleanup` cuts in the closed rollback, after its restore rename
    and after its staging removal;
  - the module `compaction::unpublished_attempt_tests`, with the over-reach controls for D3 and D4
    observed passing at `1eb9de5`:
    - closed: canonical missing; canonical corrupt; previous present; staging holding a foreign
      file; staging holding the inner lock file; a corrupt main manifest; staging holding a family
      the canonical directory lacks; a `.manifest.next` that is a directory; staging as a symlink
      to a directory;
    - online: a lone family `.manifest.next` beside family staging, beside a previous directory,
      beside a corrupt canonical family, and a `.manifest.next` that is a directory.
- [x] T003 FR-1 (P1), D1:
  - RED: a closed compaction failing validation (a paused child whose staging loses a file)
    leaves staging; one failing, and one panicking, in its first manifest publication leaves
    staging; the guard over a manifest the attempt did not write leaves staging.
  - GREEN: `UnpublishedClosedStaging`, armed after `create_dir`, disarmed after `Prepared`.
  - Control: the guard leaves staging once the main manifest has changed, directly and through a
    compaction whose `Prepared` rename succeeded and whose next step failed.
- [x] T004 FR-2, D2:
  - RED: `publish_manifest_buffered_with_checkpoint` failing or panicking at `Created`, `Written`
    and `Flushed`, as a first publication and as a rewrite, leaves `.manifest.next`; so do a closed
    compaction's first publication, an online compaction's first publication and its finalize
    rewrite.
  - GREEN: the temporary is owned from `create_new` to the rename.
  - Rewrite `failed_temp_publication_preserves_main_phase_and_unpublished_evidence` to the new
    expectation (`..._and_removes_its_temporary`). Control: a `.manifest.next` the call did not
    create is left byte-identical.
- [x] T005 FR-3 (P1), D3 and D4 (D4 withdrawn by T014; D3's comparison replaced by T014 and T015):
  - RED: a process killed at each pre-`Prepared` cut, for each family and for all three together,
    does not reopen `Recovered` with its exact state, no debris and a following compaction; the
    existing cut matrix tightened the same way; a compaction started over such debris; a planted
    lone online `.manifest.next`.
  - GREEN: the closed discard in `resolve_directory_maintenance_for_compaction` and the online one
    in `resolve_online_maintenance_for_compaction`.
  - Rewrite `untrusted_manifest_evidence_distinguishes_ambiguity_from_invalid_debris_without_mutation`'s
    manifest-less staging copy to the public expectation; `inspect_storage` still reports the debris
    and changes nothing.
  - Added after GREEN, observed RED with the closed discard removed: a lone closed
    `.manifest.next`.
- [x] T006 FR-4, D5:
  - RED: a closed rollback killed after its restore rename (`RollbackRestore`) does not reopen.
  - GREEN: recovery completes the rollback.
  - RED, then GREEN: one killed after its staging removal (`RollbackCleanup`), which spec.md did
    not name (Amendments).
  - Controls: the interrupted state beside a previous generation, beside a damaged previous
    generation, with staging as a file, and with a canonical directory that no longer matches the
    source, keep their errors.
- [x] T007 FR-5, D6 (withdrawn by T014; its tests now pin the `1eb9de5` refusal):
  - RED: a finalized online `Prepared` with its first source artifact, or every one, moved to the
    previous directory does not reopen.
  - GREEN: the moved artifacts are restored.
  - Controls: a moved artifact that does not verify, one present at both locations, a foreign file
    in the previous directory, and an unmoved artifact that does not verify keep their errors.
- [x] T008 FR-6: spec 008's contract and plan state the implemented order, the classification and
  the fault-evidence rule (minimal edits, dated notes).
- [x] T009 Run the new module on every operating system: RED the pin in `tests/ci_workflow.rs`,
  then the step in `recovery.yml`.
- [x] T010 Full suite, doc tests, `cargo fmt --check`, Clippy, neutralization probes. Record
  everything in verification.md.
- [ ] T011 macOS and Windows: the new CI step and the changed recovery tests on a CI run of the
  published revision (not run here; Linux only).
- [x] T012 First review (2026-10-03). Each behaviour change observed RED as an assertion failure
  first; each new control observed passing at `1eb9de5` and caught by a probe of the rule it pins:
  - D1, FR-1 amended: RED `a_compaction_whose_source_was_damaged_under_its_claim_keeps_its_staging`
    and `a_compaction_whose_source_was_replaced_by_a_valid_generation_leaves_its_staging_to_the_next_open`;
    GREEN: the guard also requires `generation_matches` against the capture.
  - D1, "absent": RED `a_main_manifest_that_is_neither_readable_nor_absent_leaves_the_staging`
    (a symlink to nothing); GREEN: `ManifestBytes::read` asks `symlink_metadata` first.
  - D3 and D4, "absent": RED the controls "main manifest is a symlink to nothing" and "temporary
    beside a family manifest that is a symlink to nothing"; GREEN: both discards require nothing at
    the manifest's path.
  - Controls added: staging holding a subdirectory, a symlink entry and a sealed-segment name; an
    empty canonical directory beside a lone temporary; a failed manifest rename; a staging write
    that fails; a lone family temporary beside a published family manifest; a split beside an
    unfinalized online `Prepared`; debris an open cannot remove (Unix; pins FR-7's amended
    `Io` and FR-3's "staging first").
  - Documents: plan.md D1, D3, D4, D6, III, IV, Decisions and "Not done here"; the D6 doc comment;
    spec.md Amendments; spec 008's contract and plan; verification.md, "Review".
- [x] T013 Second review (2026-10-03). Each behaviour change observed RED as an assertion failure
  first; each new control observed passing at `1eb9de5` where its rule existed there, and caught
  by a probe of the rule it pins:
  - D4 and D6, sole open instance of the family: RED
    `a_second_open_during_a_live_cutover_is_refused_and_the_cutover_completes` (a live cutover
    parked after its source moves by a new test-only pause), `a_lone_family_temporary_is_left_while_another_instance_of_the_family_is_open`
    and `a_compaction_beside_another_instance_of_its_family_leaves_a_lone_temporary`; GREEN: the
    registry counts open leases per family, and family recovery runs D4 and D6 only for
    `FamilyOpens::Sole`. Control: `an_open_instance_of_another_family_does_not_keep_a_familys_debris`.
  - D3, containment: RED `a_source_truncated_under_the_claim_keeps_the_staging_that_holds_what_it_lost`,
    `a_source_rolled_back_under_the_claim_keeps_the_staging_that_holds_what_it_lost` and
    `a_staging_holding_a_fact_the_canonical_directory_lacks_keeps_its_error_and_its_bytes`; GREEN:
    `staging_holds_nothing_the_canonical_directory_lacks`. Controls:
    `a_staging_holding_nothing_the_canonical_directory_lacks_is_removed` (a canonical directory that
    gained a write, a staged file torn mid-record, a staged file cut inside its header).
  - D3 and D6, unreadable paths: RED `a_staging_directory_that_cannot_be_read_keeps_its_error_and_its_bytes`
    and `a_previous_directory_that_cannot_be_read_keeps_its_error_and_its_bytes` (Unix; skip as
    root); GREEN: `PathEntry` and `regular_file_names` answer "not proved" for a failed read.
  - D1, path spelling: RED `a_compaction_given_a_relative_store_path_that_fails_removes_its_staging`
    (a child process) and `a_compaction_given_a_symlinked_store_path_that_fails_removes_its_staging`;
    GREEN: the guard compares at the path resolved when staging was created.
  - Controls added: a `.manifest.next` that is a symlink to a regular file, closed and online; a
    `Prepared` renamed over an earlier manifest; a staging path replaced by a symlink before the
    guard drops (`the_guard_never_removes_through_a_staging_symlink`).
  - Documents: plan.md D1, D3, D4, D6, III, IV, V, Decisions and "Not done here"; the D3, D4 and D6
    doc comments; spec.md Amendments; spec 008's contract and plan; verification.md, "Review 2".
- [x] T014 Third review (2026-10-03). Each behaviour change observed RED as an assertion failure
  first; each new control observed passing at `1eb9de5`, and caught by a probe of the rule it pins:
  - D3, equality: RED `a_source_that_lost_a_delete_under_the_claim_keeps_the_staging_that_holds_it`
    (each family; truncated before, torn inside, rolled back before a delete),
    `a_staging_not_proved_to_hold_the_canonical_state_keeps_its_error_and_its_bytes` (each family;
    canonical gained a write, a staged delete, a torn staged record, a header-only staged file) and
    the rewritten `a_compaction_whose_source_was_replaced_by_another_valid_generation_keeps_its_staging_and_its_error`;
    GREEN: `staging_holds_the_canonical_state_or_nothing` (equality; a staged file shorter than a
    header holds nothing). Control rewritten: `a_staging_that_holds_the_canonical_state_or_nothing_is_removed`
    (a copy, a compacted copy, an empty file, a file cut inside its header).
  - D3, a rejected staged file: RED `a_staged_file_the_replay_rejects_past_its_header_keeps_its_error_and_its_bytes`;
    GREEN: only a file shorter than a header holds nothing.
  - D3 controls added: a changed map entry value, every staged family compared
    (`every_staged_family_is_compared`), an unreadable staged file (Unix).
  - D1, paths: RED `a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging`
    (a child process whose working directory changes while its compaction is parked); GREEN: the
    canonical directory, the manifest and the staging are resolved when staging is created.
  - D4 and D6 withdrawn: RED `an_open_still_recovering_when_a_later_instance_starts_its_cutover_leaves_that_cutover_alone`
    (a new test-only pause between directory and family recovery) and
    `a_process_without_locks_opening_during_a_live_cutover_leaves_it_alone` (a child process with
    its locks skipped); GREEN: both rules and the registry's per-family lease count removed.
    Tests that pinned the rules rewritten to their `1eb9de5` refusals:
    `a_lone_online_manifest_temporary_keeps_its_error_and_its_bytes` (alone, beside another
    instance of the family, beside an instance of another family; it absorbs the two gate tests,
    which are removed), `a_finalized_prepared_split_by_its_source_move_keeps_its_error_and_its_bytes`
    and `a_compaction_over_a_lone_family_temporary_keeps_its_error_and_its_bytes` (each family).
  - Documents: plan.md D1, D3, D4, D6, III, IV (with a measured bound for D3), V, Decisions and
    "Not done here"; the D1 and D3 doc comments; spec.md Amendments (FR-3 closed and FR-1 amended,
    FR-3 online and FR-5 withdrawn, FR-7 worded, sign-off of the review amendments pending); spec
    008's contract and plan; verification.md, "Review 3".
- [x] T015 Fourth review (2026-10-03). Each behaviour change observed RED as an assertion failure
  first; each pin of existing behaviour caught by a probe of the rule it pins:
  - Thresholds for D3's memory and time set in plan.md IV before any measurement; baseline
    measured on the third-round tree and on `1eb9de5` before the production change.
  - D3, the directory it acts in: RED `a_discard_removes_only_what_it_proved_in_the_directory_it_locked`
    (a symlinked parent retargeted while D3 is parked after its proof) and
    `a_discard_leaves_a_directory_its_open_or_claim_did_not_lock` (retargeted before it starts);
    GREEN: D3 resolves the caller's path once, requires the lease's or claim's identity, and reads
    and removes only through paths built from it. Infrastructure: `recovery::recovery_pause`
    (generalized from `family_recovery_pause`) with `DiscardEntry` and `DiscardProved`.
  - D3, what it compares: RED `NotProved::ByteCopy`, `a_short_staged_file_that_replays_to_a_fact_keeps_its_error_and_its_bytes`
    and `a_canonical_key_left_with_no_members_keeps_the_staging_that_records_its_absence`; GREEN:
    `staging_is_what_compaction_stages_or_holds_nothing` (byte equality with what compaction
    stages for the canonical state; a short file holds nothing only when the replay rejects it or
    recovers no key; a canonical key with nothing in it refuses), and the staged names checked
    before the canonical directory is validated. Tests whose staged files were byte copies now
    stage what a compaction stages (`what_compaction_stages`); the removal control is
    `a_staging_that_is_what_compaction_stages_or_holds_nothing_is_removed`; recovery_tests' byte-copy
    case is back to its `1eb9de5` text.
  - D1, before `create_dir`: RED `a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging`
    at `StagingCreate` (the cut moved between the creation and the guard); GREEN: resolution before
    creation, creation through it.
  - D2: RED `a_working_directory_change_during_a_publication_never_removes_another_directorys_temporary`;
    GREEN: one resolution for the temporary and the main manifest, used to create, rename and remove.
  - Pins of existing behaviour, each caught by its probe: `the_guard_checks_the_main_manifest_it_resolved_when_staging_was_created`,
    `a_key_rewritten_across_sealed_segments_does_not_keep_its_staging`, the one-byte-short-of-a-header
    case, `a_staging_write_that_fails_over_a_changed_source_keeps_the_staging_and_its_error`,
    `two_first_opens_racing_through_the_discard_lose_nothing_and_leave_no_debris`.
  - Documents: spec.md (Status line, FR-7 body, the first review's "move/restore" wording marked
    superseded, four amendments), plan.md (D1, D2, D3, III, IV thresholds and measurements, V,
    Decisions, "Not done here"), this list, spec 008's contract and plan, verification.md,
    "Review 4".
- [x] T016 Fifth review (2026-10-03). Each behaviour change observed RED as an assertion failure
  first; each pin of existing behaviour observed passing before any production edit and caught by
  a probe of the rule it pins; each new control observed with its error at `1eb9de5`:
  - D3, a torn write of what compaction stages: infrastructure, the test-only cut
    `StagingWriteTorn` (all of the first family's staged bytes but the last, then a process exit)
    in both cut matrices; RED `every_pre_prepared_cut_reopens_the_untouched_source`,
    `every_pre_prepared_cut_of_a_store_larger_than_a_block_reopens_the_untouched_source` and
    `every_closed_checkpoint_process_exit_reopens_exact_state_or_preserves_explicit_evidence` at
    that cut, and the removal control's torn cases (renamed
    `a_staging_that_is_what_compaction_stages_or_a_torn_write_of_it_is_removed`; the old
    `NotProved::TornRecord` moved there); RED
    `a_short_staged_file_holding_records_compaction_never_writes_keeps_its_error_and_its_bytes`;
    GREEN: a staged file is removed when it is what compaction stages or its first bytes ending
    inside the header or a record (`ends_inside_a_record`, `V2CodecProbe::record_encoded_len`),
    replacing the short-file rule. Controls: `NotProved::CutAtARecordBoundary`,
    `a_staging_cut_at_a_record_boundary_past_the_first_block_keeps_its_error_and_its_bytes`.
  - D1, exact bytes: RED `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging`
    (each family); GREEN: `generation_is_exactly`, the capture's bytes shared with the guard.
  - Pins: `a_staging_differing_in_one_fact_of_the_same_length_keeps_its_error_and_its_bytes`
    (probes B2, B3); the removal control and a kill-cut test on stores whose staged file spans many
    64 KiB blocks (probe B4); `every_read_of_the_discards_proof_is_in_the_directory_it_locked`
    through a new test-only pause `DiscardIdentified` (probes E2, E3, E6);
    `a_working_directory_change_during_a_publication_renames_only_into_its_own_directory`
    (probes C7, C8); a main manifest beside `one/store` in the `StagingCreate` working-directory
    case (probe C4); `debris_staged_by_an_earlier_revision_is_removed_at_open`, over the frozen
    fixture `tests/fixtures/unpublished_closed_staging` written by `1eb9de5`;
    `a_canonical_directory_that_cannot_be_searched_keeps_its_error_and_its_bytes` (Unix).
  - Documents: spec.md (Acceptance's mid-write line, four amendments), plan.md (D1, D3, III, IV
    with the checked-in harness `examples/open_with_debris.rs` and its measurements, V,
    Decisions, "Not done here"), spec 008's contract (classification bullet, fault-evidence rule
    with its two exceptions) and plan, `CompactionOperation::Cleanup`'s rustdoc, this list,
    verification.md, "Review 5".
- [x] T017 Final review (2026-10-03). No confirmed blocker or major; no production behaviour
  changes. Each new pin observed passing on the tree as handed over and caught by a probe of the
  property it pins; the regenerated fixture's control observed at `1eb9de5`:
  - D1, exact bytes: `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging`
    now damages a sealed segment, each family's active file in a three-family store, and the
    active file while the attempt is parked at `StagingCreate`, before its guard exists, and
    reports every case that removed its staging (probes G3, G4, G6; G1 still caught).
  - D3, the byte comparison: a same-length case whose difference lies in a middle 64 KiB block
    (`SameLength::MiddleFact`, large stores; probe P5c).
  - D3, the residual: `a_staging_write_killed_on_a_page_boundary_keeps_its_error_only_where_a_record_ends`
    (96-byte records: the page multiples on a record boundary keep their error, the others are
    removed; probes T4 and T2).
  - The frozen fixture regenerated with the same `1eb9de5` procedure before its first commit, with
    keys whose length order differs from their byte order and sets and maps of several members or
    entries in more than one key (probes EN3, EN4, EN5; EN2 and EN6 still caught).
  - Documents: spec.md (Acceptance, the fourth and fifth reviews' amendments where they overstated
    or misreasoned, the fifth review's fixture amendment, a final-review amendment, and the closing
    amendment naming FR-4's 2026-10-02 widening for the sign-off; not the Status line), plan.md
    (D3's reasoning and empty-key scope, III, IV's procedure decision and the harness's
    `Cargo.lock` step, Decisions, "Not done here" with the page-granularity residual and its
    proposed production fix), D3's rustdoc, three test doc comments, the example's documentation,
    the fixture's README, spec 008's contract, this list, verification.md ("Review 6 (final)").
