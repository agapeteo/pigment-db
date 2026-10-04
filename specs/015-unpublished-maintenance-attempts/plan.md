# Unpublished maintenance attempts: plan

## Technical context
Rust 2021 library crate, MSRV 1.91. At `1eb9de5` the closed compaction (`compact_closed_directory`,
`src/compaction/mod.rs`) runs in this order, all under its claim (specs/011: the inner lock
`<store>/.pigment-lock` and the replacement lock `<parent>/.<store>.pigment-lock`):

1. resolve earlier maintenance (`resolve_directory_maintenance_for_compaction`), take the inner
   lock, inspect;
2. `prepare_closed_staging`: capture the source, `create_dir(staging)` (cut `StagingCreate`), write
   one active file per family (cut `StagingWrite`), synchronize (cut `StagingSync`);
3. `validate_closed_staging`: verify descriptors, re-capture, reopen each family without locks;
4. retire the inner lock (cut `StagingValidate`);
5. `publish_closed_prepared`: validate staging again, re-read the source, write
   `.<store>.pigment-compact.manifest.next` (cuts `ManifestWrite`, `ManifestSync`), rename it to
   the main manifest (cut `ManifestPublish`);
6. move the source to the previous generation, `PreviousPublished`, staging to canonical,
   `ReplacementPublished`, `CleanupPending`, cleanup, manifest last.

So staging exists, with no manifest, from step 2 until the rename in step 5. Only a write error
inside step 2 removes it (`let _ = fs::remove_dir_all`). A validation failure, a failed source
re-read, a failed first manifest publication, a panic, or a process kill at any of the six cuts
before step 5's rename leaves it. Every later open, `inspect_storage` and compaction then reads it
in `classify_untrusted_closed_authority` (`src/compaction/recovery.rs`), which was written for
spec 008's order (`Prepared` first, then staging): a complete staging generation is a competing
authority (`AuthorityUndetermined`), an incomplete one is `InvalidArtifact`, and a lone
`.manifest.next` is `InvalidArtifact`. The directory is then unopenable until an operator deletes
staging by hand. The investigation behind spec.md measured this at six of the eleven closed cuts
and after an in-process validation failure; the compaction code has not changed since that
measurement (`git diff 9ba8871 1eb9de5 -- src/compaction` is empty).

`publish_manifest_with_checkpoint` (`src/compaction/publication.rs`) creates `.manifest.next` with
`create_new`, writes, flushes, optionally syncs, and renames it over the main manifest. Nothing
removes the temporary when a step before the rename fails. With a main manifest present that is
harmless, because recovery removes `.manifest.next` first (`remove_unpublished_manifest_temp`).
With none it is terminal: closed `InvalidArtifact`, online `AuthorityUndetermined`. Online compaction
is exposed twice: its first `Prepared` publication runs before `StagingGenerationGuard` exists, and
when its finalize rewrite fails before the rename, the guard removes staging and the manifest but
not `.manifest.next`. An existing unit test pins the temporary's survival
(`failed_temp_publication_preserves_main_phase_and_unpublished_evidence`).

Two recovery branches are not idempotent:
- **Closed `PreviousPublished` rollback** (`recover_previous_published_closed`) renames the previous
  generation back to canonical, then removes staging, then the manifest. Interrupted after the
  rename, the re-run finds canonical equal to the source, no previous and staging present, and no
  branch matches: `AuthorityUndetermined`. Interrupted after the staging removal, it finds the same
  with no staging, and returns the same error.
- **Online finalized `Prepared`** (`abandon_prepared_online`) requires every source artifact at its
  canonical location and an empty previous directory. `publish_online_previous` moves the source
  artifacts one at a time, so a failure or kill between moves leaves them split, and recovery
  returns `AuthorityUndetermined`. The contract's `Prepared` row says "Restore split old
  artifacts"; closed recovery does, online does not.

An open finding any directory-level artifact takes only the replacement lock, runs recovery, then
takes the inner lock (specs/011 FR-5). A live claim holds the replacement lock throughout, and in
one process the registry refuses an open while a claim exists and a claim while an open exists. So
recovery at open or at the start of a compaction never runs beside a live closed attempt, within
the exclusion specs/011 states (its Compatibility section and FR-11).

**Motivation (provenance):** penpack finding V415: a closed compaction of a copy of penpack's
production store failed validation and left the store unopenable. The trigger was a relative store
path, which spec 016 handles; this spec handles what the failure left behind.

## Constitution check
- **VI (first).** The change is stated in the library's terms: closed and online compaction,
  staging, the manifest and its temporary, the previous generation, recovery, opens. Scan of the
  whole change (spec, plan, tasks, verification, code, tests) for consumer vocabulary: the
  motivating consumer, penpack, is named only in the marked Motivation notes of spec.md and this
  plan, in this Constitution check, and in verification.md's Principle VI record; test data are
  the existing opaque fixtures. The spec directory is `015-unpublished-maintenance-attempts`. No
  API, type, error, limit or key format is added. The change lands on this repository's `main`
  before any consumer pins it; verification.md records the revision once it is published (T011).
- **II. Every deletion, and why it cannot destroy the last complete authoritative state.** The
  shared fact: the closed canonical directory is moved only by
  `publish_closed_previous_with_checkpoint`, which refuses without a finalized `Prepared` manifest
  in hand, and which `compact_closed_directory` reaches only after `publish_closed_prepared`
  returned `Ok`, i.e. after that manifest was renamed into place and, under Physical, synchronized.
  The only recovery branch that promotes staging needs a `PreviousPublished` manifest, and recovery
  discards any staging in `Prepared` (FR-049). Online source artifacts are moved only by
  `publish_online_previous`, which requires a finalized `Prepared` main manifest, and only after
  the writer was detached under exclusive coordination. And spec 008's contract already
  says a `.manifest.next` alone "does not advance the durable phase". Each rule below deletes
  nothing the canonical generation holds.
  - **D1, FR-1: the compaction's own staging, on its own failure.** A guard
    (`UnpublishedClosedStaging`) is armed by `prepare_closed_staging_guarded` right after `create_dir`
    succeeds and disarmed when `publish_closed_prepared` returns `Ok`. On drop while armed it
    removes the staging directory (a real directory, never through a symlink), but only while two
    things hold:
    - the main manifest is byte-for-byte what it was when staging was created (nothing at its path
      both times, or the same readable bytes): if `Prepared` was renamed into place and a later step
      failed (a Physical directory sync after the rename), the manifest changed, `Prepared` recovery
      owns the staging, and the guard leaves it; anything else at the path (a directory, a symlink
      to nothing) leaves it too;
    - the canonical directory is still exactly the generation the attempt captured: the same
      names (the inner lock file is not part of a generation), and in each file the very bytes
      captured (`generation_is_exactly`: `generation_matches` against the capture's inventory,
      then each file compared with the capture's bytes, which the guard shares with the attempt
      through an `Arc`). Then the staging is a compaction of what the canonical directory holds,
      and removing it loses nothing. Until the fifth review the check was `generation_matches`
      alone, which compares each file's length and CRC-32: damage under the claim that kept both
      made the attempt's own revalidation, which compares exact bytes, fail, and then the guard
      read the source as unchanged and removed the staging, the only intact copy of the captured
      state (measured, every family:
      `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging`). Added after the first review: a source that changed under the
      claim (damaged by the medium, or written by a process outside specs/011's exclusion) made the
      attempt fail its revalidation, and the guard then removed the staging although the canonical
      directory no longer validated, so the staging was the only complete copy of the captured
      state (measured: `a_compaction_whose_source_was_damaged_under_its_claim_keeps_its_staging`).
      The canonical directory is compared where the store path resolved to when staging was
      created (`fs::canonicalize`, as the claim resolves it), not at the caller's spelling (added
      after the second review: `generation_matches` refuses a symlinked location and cannot
      resolve a one-component relative path, whose parent is `""`, so the guard left the staging
      for the motivating defect's own trigger and for a symlinked store path; measured:
      `a_compaction_given_a_relative_store_path_that_fails_removes_its_staging`, which drives the
      relative spelling in a child process, and `..._symlinked_store_path_...`). A path that does
      not resolve leaves the staging. Every path the guard acts on -- the canonical directory, the
      main manifest and the staging directory -- is resolved there, once, and the guard checks and
      removes only through those (added after the third review: it had resolved only the canonical
      directory, and read the manifest and removed the staging through the caller's spelling when
      it dropped, so with a relative store path a change of the process's working directory in
      between made it check one directory and remove another directory's staging, which may be
      the only copy of what that staging holds; measured:
      `a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging`).
      The resolution is made before the staging directory is created, and the directory is
      created through it (added after the fourth review: the resolution ran just after
      `create_dir`, through the caller's spelling again, so a working-directory change between the
      two bound the guard to another directory whose store was byte-identical, and the guard
      removed that directory's staging; measured RED by the same test parked at `StagingCreate`,
      a cut that now fires between the creation and the guard). What the guard cannot see is a
      rename of real directories on the resolved path while the attempt runs, which would also
      leave the claim's lock files covering another directory. specs/011 does not state that
      limitation (its Known limitations name symlink repointing and inode paths, not renames), so
      it is this spec's residual ("Not done here"), and it is not handled here.

    While both hold the attempt has published nothing, the canonical directory has not moved, and
    nothing names the staging. It is dropped before the claim, so while the claim still holds the
    replacement lock, and the inner lock too until the claim retires it before the source is
    revalidated. A cleanup failure is logged and the original error returned; D3 removes the debris
    at the next open. A staging the guard leaves is decided by recovery as after a kill: D3 removes
    it when the canonical directory is complete and every staged file is what a closed compaction
    of the canonical directory stages for its family, and otherwise the open returns its
    `1eb9de5` error, `AuthorityUndetermined` naming both for a complete staging.
  - **D2, FR-2: the manifest temporary a publication created.** A guard in
    `publish_manifest_with_checkpoint` is armed only after `create_new` succeeded, so it never owns a
    temporary it did not create, and disarmed once the rename has returned `Ok`. On a failure or
    panic before that it removes `.manifest.next` (`NotFound` ignored: a rename that failed after
    moving the file leaves nothing to remove). The main manifest is not touched, so the durable
    phase is unchanged. This also discharges FR-1's "any `.manifest.next` the attempt created": the
    attempt's only temporary is its publication's, and D1 does not remove a `.manifest.next` it
    cannot prove it created. The publication resolves the temporary's and the main manifest's paths
    once, before it creates the temporary (`resolved_beside`), and creates, writes, renames and
    removes through them (added after the fourth review: it created and removed the temporary
    through the caller's spelling, so a working-directory change during a failing publication
    removed another directory's temporary, which may belong to that directory's live publication;
    measured: `a_working_directory_change_during_a_publication_never_removes_another_directorys_temporary`).
    A path whose parent does not resolve is used as spelled, and then creating the temporary fails.
  - **D3, FR-3 closed: unpublished-attempt debris at open and at compaction start.** In
    `resolve_directory_maintenance_for_compaction`, only when reading the main manifest returned
    `NotFound` (never for a corrupt one) and nothing at all is at its path (a symlink to nothing is
    not an absent manifest; added after the first review), and only when all of these hold:
    - nothing exists at the previous-generation path;
    - the staging path, if present, is a real directory whose entries are all regular files named
      for the active segment of a family the canonical directory holds (empty is allowed);
    - `.manifest.next`, if present, is a regular file;
    - each staged name is checked first, before the canonical directory is validated: it must be
      a family's active-segment name with a regular file of that name in the canonical directory
      (added after the fourth review, so that a staging holding a foreign name refuses without a
      whole validation that the classification then repeats);
    - the canonical directory passes `inspect_generation` with at least one family;
    - each staged file is byte for byte what a closed compaction of the canonical directory stages
      for its family (`staging_is_what_compaction_stages_or_a_torn_write_of_it`): the state the
      family's canonical chain recovers -- sealed segments first and in order, then the active
      file, as `capture_family` reads them -- encoded by the encoder compaction uses
      (`encode_captured_state`, which writes keys, members and entries in sorted order) with the
      timestamp metadata that chain carries. A staged file of exactly those bytes recovers exactly
      what the canonical directory recovers, so removing it loses nothing. Or the staged file is a
      torn write of those bytes: their first bytes, ending inside the file header (64 bytes) or
      inside one of the records (`ends_inside_a_record`, which walks the record lengths the
      expected encoding declares). That is what a staging write killed before its first byte
      leaves (an empty file, the `StagingWrite` cut), and what one killed inside its bytes leaves
      unless the kill falls exactly on a record boundary (the `StagingWriteTorn` cut; added after
      the fifth review). Removing such a file loses nothing, for two reasons that need each other
      (wording corrected by the final review, which found "it records no deletion" false). Its
      bytes, the gaps between its complete records included, are the current encoding's, so every
      absence it records -- a key sorting between two of its complete records -- is an absence in
      the canonical state too; the byte comparison, not the tear, makes that so, and a comparison
      of only the facts in its complete records would lose a delete. And its last record or its
      header is cut, so it is no complete snapshot of any state and records nothing about the keys
      beyond the cut. A file of the encoding's first
      bytes that ends right after the header or on a record boundary is a complete snapshot of a
      state with fewer facts, which records the rest as deleted, and cannot be told from a write
      stopped there: it proves nothing, and keeps its error. The encoding compared is the current
      encoder's, so debris staged by an earlier release is removed only while the encoder writes
      the bytes that release wrote; otherwise it keeps its error (III);
    - no canonical key of a family whose file is staged has an empty member set or entry map: an
      open publishes such a key (`contains_key`, `size`), while its encoding is the same as the
      key's absence, which is how a staged snapshot records a delete (added after the fourth
      review; no current writer leaves such a key, so only legacy or planted data reaches this
      refusal). The check is made per staged file, in `staged_bytes_for`, so it does not apply to
      a family with no staged file, of which nothing is compared or removed (an empty staging, a
      lone `.manifest.next`, or another family's staged file): such a key there neither refuses
      nor is at risk. The documents stated it for every canonical family until the final review.

    History of that comparison, each step observed RED first. Added after the second review as
    containment ("every staged fact is in the canonical directory"); made equality of recovered
    states after the third, because a staged state records a deletion only by a fact's absence, so
    a canonical directory that lost a delete -- truncated at the record boundary before it, torn
    inside it, or rolled back -- held every staged fact and one more, and the open removed the only
    copy of the state in which the fact was deleted and reported `Recovered` with it back
    (measured, all three families:
    `a_source_that_lost_a_delete_under_the_claim_keeps_the_staging_that_holds_it`). Made byte
    equality with what compaction stages after the fourth, for two reasons. Equality of states held
    the staged state, the canonical chain and the canonical state at once, and an open finding
    debris in a store of many small keys peaked at 1.52 times a clean open's peak, past IV's
    threshold; byte equality never builds a staged state (IV, measured there). And the short-file
    rule read every file under 64 bytes as holding nothing, while a headerless legacy file of 32
    bytes replays to a key, which the open then removed and reported `Recovered` without
    (`a_short_staged_file_that_replays_to_a_fact_keeps_its_error_and_its_bytes`). Made "a torn
    write of what compaction stages" after the fifth review, replacing the fourth review's
    short-file rule (a file shorter than a header that the replay rejects or that recovers no key),
    for two reasons, each observed RED. A process killed inside a staging write's bytes left a file
    torn inside a record, which the open refused with `1eb9de5`'s error, so the P1 requirement
    held only for kills at the test cuts (every family, real kills:
    `every_pre_prepared_cut_reopens_the_untouched_source` and
    `every_pre_prepared_cut_of_a_store_larger_than_a_block_reopens_the_untouched_source` at
    `StagingWriteTorn`, and the removal control's two torn cases). And the short-file rule decided
    from the replay's answer, so short legacy-format files holding intact records -- a put and its
    delete, which replay to no key, or a put before a record the replay rejects -- were removed and
    the open reported `Recovered`
    (`a_short_staged_file_holding_records_compaction_never_writes_keeps_its_error_and_its_bytes`,
    key/value, key/set and key/sorted-map). A staged file that is neither what compaction stages
    nor a write of it stopped inside its header or a record proves nothing, whether the replay
    rejects it (it may hold intact records the canonical directory lacks:
    `a_staged_file_the_replay_rejects_past_its_header_keeps_its_error_and_its_bytes`), it is cut on
    a record boundary (`NotProved::CutAtARecordBoundary`,
    `a_staging_cut_at_a_record_boundary_past_the_first_block_keeps_its_error_and_its_bytes`), it
    recovers the canonical state in other bytes (a byte copy of a canonical file,
    `NotProved::ByteCopy`), or it differs in a single fact of the same length, anywhere in the file
    (`a_staging_differing_in_one_fact_of_the_same_length_keeps_its_error_and_its_bytes`, which
    since the fifth review pins every byte, past the first 64 KiB block included).

    Every path the rule reads or removes is resolved once, when it starts, from the directory that
    the open's lease or the compaction's claim locked (its specs/011 identity), and the rule runs
    only while the caller's path still names that directory (`fs::canonicalize` equal to the
    identity); otherwise it leaves everything to the classification, which answers as at
    `1eb9de5`. Added after the fourth review: the rule read and removed through the caller's
    spelling, read again at every system call, so a change of the working directory (a relative
    path) or of a symlink on the path while the rule ran made it prove one directory's staging
    redundant and remove another's, never compared and possibly the only copy of a fact, in a
    directory whose locks the open had not taken (measured for an open and for a compaction's
    start, with a symlinked parent retargeted while the rule is parked after its proof:
    `a_discard_removes_only_what_it_proved_in_the_directory_it_locked`; and retargeted before it
    starts, beside redundant debris in the other directory:
    `a_discard_leaves_a_directory_its_open_or_claim_did_not_lock`). Errors still name the caller's
    spelling. Every read of the proof -- the staging it compares, the canonical directory it
    compares it with, whether a previous generation exists -- is in that directory, which a test
    pins by retargeting the caller's path right after the identity check
    (`every_read_of_the_discards_proof_is_in_the_directory_it_locked`, through the test-only pause
    `DiscardIdentified`; added after the fifth review, whose probes moved each of those reads to
    the caller's spelling with every test green). What the open or compaction does after the rule,
    through the caller's spelling, is spec 016's. A rename of real directories on the resolved path
    while the rule runs would make it act in whatever the path then names, and would leave the
    lock files covering another directory; specs/011 does not state that limitation, so it is this
    spec's residual ("Not done here"), and it is not handled here.

    Every check reads; a path a check cannot read proves nothing, and the state keeps its current
    error (added after the second review, which found the staging listing returning its
    `PermissionDenied` where `1eb9de5` returned `InvalidArtifact`). It then removes staging, then
    `.manifest.next`, and the open reports `Recovered`. Authority:
    with no main manifest and no previous generation, the canonical directory has never been moved,
    since a move needs a durable `Prepared`, and recovery of every phase ends by removing the
    manifest after staging and previous are gone. So the complete canonical generation is the
    authority, within the protocol. Staging written by compaction holds only active files of
    captured families, and no manifest names it; `.manifest.next` alone never advances the phase.
    Outside the protocol -- the canonical directory truncated at a record boundary or rolled back
    to an older generation by something specs/011 does not exclude, while the attempt held its
    claim or after a kill -- the canonical directory can still validate while it lacks what the
    staging holds, or holds what the staging records as deleted; the comparison is what keeps that
    staging, since it may then be the only copy (the second review measured a truncated source
    losing its last key at the next open:
    `a_source_truncated_under_the_claim_keeps_the_staging_that_holds_what_it_lost`, and the third
    a lost delete coming back). Anything else at those paths is not provably compaction's, and
    keeps its current error. The rule runs under the claim (a compaction) or, at an open, under the
    replacement lock that the process's first open of the directory takes because directory-level
    artifacts exist (specs/011 FR-5), or under the locks a process that already has the directory
    open holds. Two first opens of different families in one process can run it at the same time:
    nothing is lost and no debris is left, and the open that loses the race to remove fails with a
    raw `NotFound` (`RecoveryError::Io`), which
    `two_first_opens_racing_through_the_discard_lose_nothing_and_leave_no_debris` pins at a fixed
    interleaving (see "Not done here"). A live closed attempt holds the replacement lock and, in
    its own process, refuses every open, so the rule cannot remove a live attempt's staging.

    Where specs/011's exclusion does not hold (a skipped lock, FR-11), the rule can run beside
    another process's live closed attempt, and two residuals remain (after the fourth review, read
    from the code, not reproduced; this paragraph said until then that the worst case was the live
    attempt failing). The removal can race that attempt's own rename of staging to canonical:
    `remove_dir_all` unlinks through a handle it opened on the staging directory, so after the
    rename it keeps unlinking entries of what is now the published replacement, and if the attempt
    has meanwhile validated the replacement and removed the previous generation, neither generation
    survives. That needs the removing process to stall for seconds between opening the directory
    and unlinking, on a platform or filesystem where locks are skipped. And the rule's removals are
    not followed by a Physical directory barrier, so after a power loss a removed staging can come
    back beside a canonical directory that has since been written; the next open then refuses, with
    every generation present (the other closed-recovery branches lack the same barrier; "Not done
    here"). The canonical directory is never written.
  - **D4, FR-3 online: withdrawn after the third review.** (Note, 2026-10-03: restored by specs/016
    FR-2 at an open that is the only live writer of the family, under the exclusion this
    paragraph says it lacked; see specs/016's plan.) A lone family `.manifest.next` keeps its
    error from `1eb9de5`, and nothing is removed. The rule removed it at an open, or at an online
    compaction's start, when the family had no manifest, staging or previous directory and a valid
    canonical family. But another open instance of the family writes exactly that temporary during
    its first publication, and nothing excludes that instance from the rule: in this process, the
    second review's gate ("the open is its process's only open instance of the family") was read
    when the open was admitted and used after directory recovery, so an instance admitted later
    could reach its first publication while the gated open was still recovering; and where locks
    are not supported (specs/011 FR-11) another process's instance is excluded by nothing at all.
    Removing that temporary fails the other instance's compaction. Closing it needs coordination
    this spec does not add (see Decisions and "Not done here"). FR-2 still removes the temporary of
    every online publication that fails; only a process killed inside one leaves it.
  - **D5, FR-4: completing an interrupted closed rollback.** In `recover_previous_published_closed`,
    only when the canonical directory matches the manifest's source inventory exactly
    (`generation_matches`) and no previous generation exists: any staging is removed (refusing, as
    the rollback already does, a staging path that is not a real directory), then the manifest.
    Authority: in `PreviousPublished` the forward path leaves the canonical path empty or holding
    the replacement until cleanup, and cleanup runs only after
    `ReplacementPublished`; so canonical equal to the source with no previous can only be the
    rollback's restore rename, which recovery takes only when the replacement did not validate. The
    canonical directory is then the verified last complete authority; staging is what the rollback
    already decided to remove.
  - **D6, FR-5: withdrawn after the third review.** (Note, 2026-10-03: restored by specs/016
    FR-2, as for D4.) A finalized online `Prepared` whose source is
    split between the canonical directory and the previous directory keeps its error from
    `1eb9de5`, `AuthorityUndetermined`, and nothing is moved. The rule moved each verified artifact
    back. A live cutover of another open instance of the family leaves exactly that split between
    its source moves and `PreviousPublished`, and restoring it fails the cutover and leaves the
    store refusing to open, where `1eb9de5` refused only the opener (measured by the second review
    for an instance already open, and closed then by the gate). The third review measured the
    gate failing in two ways, and both were observed RED here before the rule was withdrawn:
    - an open admitted alone, parked after its directory-level recovery while a second instance
      opened, compacted and parked between its moves, then restored that live split
      (`an_open_still_recovering_when_a_later_instance_starts_its_cutover_leaves_that_cutover_alone`,
      through a new test-only pause between directory and family recovery; renamed by specs/016 to
      `a_later_instance_cannot_open_while_an_open_of_its_family_is_still_recovering`, since its FR-1
      refuses the later instance);
    - a second process whose locks are skipped (specs/011 FR-11, simulated by injecting
      `Unsupported`) restored a live split
      (`a_process_without_locks_opening_during_a_live_cutover_leaves_it_alone`).

    Re-reading the gate where the rule acts narrows the first window without closing it, and a
    lock-held check closes only the second. The split is reachable only by a process killed inside
    a cutover's source moves.
  - **Over-reach controls.** Each rule has controls that keep their current error and leave the
    directory byte-identical (tasks.md lists them; verification.md records each at `1eb9de5` and
    after the change).
- **III.** No persisted format and no public signature changes. Behaviour changes only where an open
  or a compaction returned `AuthorityUndetermined` or `InvalidArtifact` for a state D3 or D5
  covers; those now succeed (an open reports `Recovered`), or, when the removal itself fails,
  return its I/O error (`RecoveryError::Io`, `CompactionError::Io`) instead (spec.md, Amendments,
  added after the first review; the same state returned `AuthorityUndetermined` at `1eb9de5`).
  Only a removal returns its I/O error: a read that fails while a rule checks a state proves
  nothing, and the state keeps its current error (after the second review). Closed
  `PreviousPublished` recovery reads the staging path before any rule runs and returns that
  read's error as at `1eb9de5`; D5 reads it once more as it removes it, which fails only if the
  path became unreadable in between (third review, nit). `PathEntry::Unknown`, a path whose
  metadata cannot be read for a reason other than its absence, proves nothing either. This plan
  said until the fifth review that no test reaches it, since in the checked directories it needed
  a failure the manifest read beside it would meet first; that is false since the fourth review
  moved the staged names' check before the canonical directory's validation, because that check
  reads inside the canonical directory, not beside the manifest. A canonical directory the
  process cannot search reaches it, and the open keeps `1eb9de5`'s `AuthorityUndetermined`
  (`a_canonical_directory_that_cannot_be_searched_keeps_its_error_and_its_bytes`, Unix, skipped
  where the process may search such a directory; a probe making that arm panic turns it red).

  The I/O error FR-7 promises for a removal that fails after D3 proved the state carries its
  operation as every compaction-side I/O error does where it reaches the caller: from a
  compaction's start, `CompactionError::Io` with `operation: CompactionOperation::Cleanup`; from an
  open, `RecoveryError::Io` with `operation: RecoveryOperation::Inspect`, since
  `map_compaction_recovery_error` labels every `CompactionError::Io` that directory recovery
  returns to an open `Inspect`, as it did at `1eb9de5` for the removals that recovery already
  made (fifth review, nit). Mapping D3's removal alone to `RecoveryOperation::Cleanup` would be
  more exact, and was declined: it needs a second error path out of directory recovery for one
  label, and the pre-existing removals would still say `Inspect`. spec.md's FR-7 amendment states
  the labels, and `CompactionOperation::Cleanup`'s rustdoc now names unpublished debris too.

  D3's proof depends on the snapshot encoder: it compares a staged file with what the current
  encoder writes for the canonical state. Debris staged by an earlier release is removed only
  while the encoder still writes the bytes that release wrote; a change to the encoder's output
  (a header field, the record framing, the order) returns such debris to its `1eb9de5` error,
  which is fail-closed, and narrows FR-3 for the commonest sequence of all: a crash, an upgrade,
  a restart. A frozen fixture makes such a change face the rule, when the fixture's data can show
  it: `tests/fixtures/unpublished_closed_staging` holds a store of all three families and what
  `1eb9de5`'s closed compaction staged for it, both written by `1eb9de5` through its public API,
  and `debris_staged_by_an_earlier_revision_is_removed_at_open` requires each family's first open
  to remove it and report `Recovered` with the exact contents (fifth review). An encoder change
  must then either keep the bytes or say, in its own specification, what becomes of such debris.
  The fixture can only show an order its data distinguish. The fifth review's version could not:
  every key of a family had one length and every set one member, so an encoder writing keys by
  length, or set members in reverse or by length, passed every test while it returned real debris
  staged by `1eb9de5` to its error (the final review's probes EN3, EN4 and EN5). The final review
  regenerated it, with the same `1eb9de5` procedure and before it was first committed, so that in
  each family the keys' order by length differs from their byte order, and a set and a map hold
  several members or entries of different lengths in more than one key; those three probes, and
  the fifth review's EN2 and the final review's EN6, now fail it. A change the data still cannot
  show (an order among search keys of other types, say, or a record kind the fixture lacks)
  passes it.

  One more return-semantics change, recorded after the fourth review: D1 leaves its staging when
  the source changed under the claim (first review), so a staging write that fails over a changed
  source -- a family file deleted under the claim, say -- now leaves the staging, which may hold
  the only copy of what the source lost, and the next open refuses with `InvalidArtifact` naming
  it. At `1eb9de5` the failed write removed the staging and the next open succeeded without the
  lost family (measured on a `1eb9de5` copy by the fourth review; pinned by
  `a_staging_write_that_fails_over_a_changed_source_keeps_the_staging_and_its_error`). The safer
  direction under Principle II, and listed in spec.md's FR-7 amendment and Status line with FR-7's
  `Io`, for the approver's sign-off.

  Since the fourth review D3 also refuses some states the third review's rule removed, each with
  its `1eb9de5` error: a staged file recovering the canonical state in other bytes than a
  compaction writes, a canonical key with an empty set or map in a family whose file is staged,
  and a file shorter than a header that recovers a key. Since the fifth review it removes one more kind of state, which returned
  `AuthorityUndetermined` at `1eb9de5` and now recovers: a staged file torn inside its header or a
  record of what compaction stages (a kill inside a staging write's bytes). And it refuses one
  more, with its `1eb9de5` error (`InvalidArtifact`, naming the staging): a short headerless file
  that is not the first bytes of what compaction stages, whatever its replay recovers. Every
  pre-`Prepared` cut still recovers, with the two exceptions spec 008's fault-evidence paragraph
  states: a kill inside a staging write whose bytes stop on a record boundary or right after the
  header, and a cut at which a family's file is staged while that family holds a canonical key
  with no members or entries. A real kill stops a large write at a page or page-cache folio
  boundary, so the first exception is reached by a fixed share of kills in a store whose staged
  records all have one length (final review; "Not done here").

  FR-4's second window, the rollback killed after its staging removal (spec.md's 2026-10-02
  amendment, made during implementation after the approved text), is a return-semantics change
  as well: `AuthorityUndetermined` at `1eb9de5`, now `Recovered`, or the manifest removal's `Io`.
  spec.md's closing amendment names it for the sign-off; the Status line, which the requester
  keeps, does not yet (final review).
  `inspect_storage` stays read-only (spec 008 FR-031) and keeps reporting such debris; a test says
  so. Correcting the contract text rather than
  the code's order is a Principle III decision: the code cannot publish `Prepared` before staging
  without an unfinalized closed manifest, which the codec refuses (`manifest.rs`,
  `validate_manifest`), so the contract's order would need a persisted-format change.
- **IV.** No new lock or coordination layer, and no coordination state. D1 and D2 run inside the
  operation that created the artifacts, under its claim or attempt ownership. D3 and D5 run inside
  directory recovery: under the claim at a compaction's start, and at an open under the replacement
  lock the process's first open takes because directory-level artifacts exist, or under the locks
  a process that already has the directory open holds. Both exclude other processes within
  specs/011's exclusion, and in this process the registry refuses an open while a closed claim
  exists, so neither runs beside a live closed attempt. D4 and D6 ran inside family recovery,
  whose artifacts an online attempt of another open instance of the family owns while it is live;
  the second review gated them on the registry's lease count, kept per family, and the third
  review showed that gate neither held while the rules acted nor saw another process where locks
  are skipped. Both rules are withdrawn and the per-family count is removed with them: the
  registry is as at `1eb9de5`. A sound version needs a family's recovery excluded from its other
  instances' online attempts, which is new coordination state (Principles III and IV) and its own
  specification. Lock order is unchanged. (Note, 2026-10-03: specs/016 is that specification. Its
  FR-1 keeps the families each open holds in the registry entry, so one instance of a family is
  open per directory per process, and its gate reads that and specs/011's inner lock once
  directory recovery is done; its plan's II and IV record the state, the lock order and the
  progress tests.)

  Deterministic tests for D3's concurrency (added after the fourth review, which found the race
  between two first opens of different families pinned only by 300 random runs): a test-only
  pause parks D3 once it has proved the debris and before its first removal, and
  `two_first_opens_racing_through_the_discard_lose_nothing_and_leave_no_debris` runs a second
  family's first open through the same D3 while the first is parked. The second removes the debris
  and reports `Recovered`, the first then fails with its removal's `NotFound`, nothing is lost, no
  debris is left, and every family reopens exactly. The same pause, and one at D3's entry, drive
  the path-resolution tests (D3 above).

  Performance. D1's `generation_is_exactly` (a read of the canonical directory, then of each of
  its files, one at a time, compared with the capture the attempt already holds) runs only when an
  attempt fails before `Prepared`. D3's extra `inspect_generation` runs only when staging or
  `.manifest.next` exists and every staged name has passed its check, and a discard returns before
  `classify_untrusted_closed_authority` would inspect the canonical directory again; a refusal
  then pays that inspection twice ("Not done here"). D3's comparison runs only when staging holds
  a staged file (since the fifth review an empty or short one too, which the fourth review's rule
  replayed on its own). For each such family it reads the canonical chain, replays it, drops the
  chain, encodes the state, drops the state, and reads the staged file a block at a time against
  the encoding, walking the encoding's record headers when the file ends early: at most one chain
  and its state, or one state and its encoding, are resident, never a staged state. The third review's equality of states held the staged state,
  the chain and the canonical state at once (and, until that review, the staged bytes too).

  The thresholds, set in the fourth round **before** the measurements recorded with them (the
  third round set them after measuring, which Principle IV forbids; its memory figure is kept,
  not relaxed, and the time figure is new):
  - an open with no debris keeps its peak and stays within 5% of its time at `1eb9de5`;
  - an open that finds closed debris -- whether D3 removes it or refuses -- peaks at no more than
    1.25 times the peak of a clean open of the same store and family;
  - such an open takes no longer than a clean open of the same family plus 1.5 times the sum of
    the clean-open times of every family that has a staged file (D3 reads each such family's
    canonical chain once more, whichever family is opened);
  - measured for every family with a many-small-key shape, a key/value shape, and a two-family
    store whose staged families differ in size, each with the debris a closed compaction stages
    and with that debris cut by one byte (a refusal); release build, one process per open, a
    fresh copy of the same data per run.

  Measured (release build, Linux x86-64, one process per open, two runs each, a fresh copy of the
  same data per run, VmHWM from `/proc/self/status`; the same public-API harness on a `1eb9de5`
  copy, on the third round's tree before any production edit of this round, and on this round's
  tree). Debris is what a closed compaction stages for every family of the store (the active
  files of a compacted copy); the refusal shape is that debris with the largest staged file cut by
  one byte. Peaks in MiB, times in seconds (the slower run); the threshold is the time bound above.

  | Workload | Clean open, `1eb9de5` / third round / now | Debris, third round | Debris, now | Refusal, third round | Refusal, now |
  |---|---|---|---|---|---|
  | map, 2,000,000 keys x 1 entry | 10.1 / 10.3 / 10.2 s, 2386 MiB | 23.4 s, 3638 MiB (1.52x) | 18.3 s, 2415 MiB (1.01x); bound 25.5 s | 27.6 s (over 25.8), 1.52x | 24.6 s, 1.01x |
  | set, 2,000,000 keys x 1 member | 10.1 / 9.0 / 9.0 s, 1579 MiB | 21.0 s, 1.29x | 16.5 s, 1.00x; bound 22.6 s | 24.4 s (over 22.6), 1.29x | 20.2 s, 1.00x |
  | map, 200,000 keys x 10 entries | 4.9 / 4.7 / 5.1 s, 1187 MiB | 11.8 s, 1.20x | 8.0 s, 1.01x; bound 12.7 s | 14.0 s (over 11.9), 1.20x | 11.7 s, 1.02x |
  | key/value, 1,000,000 keys | 3.7 / 3.3 / 3.4 s, 871 MiB | 7.3 s, 1.08x | 6.3 s, 1.00x; bound 8.5 s | 8.9 s (over 8.3), 1.22x | 6.9 s, 1.07x |
  | two families, opening the set (key/value 600,000 keys, set 1,000 keys) | 0.9 / 0.9 / 0.9 s, 498 MiB | 3.2 s, 1.04x | 2.2 s, 1.00x; bound 4.8 s | 4.7 s, 1.20x | 4.0 s, 1.03x |
  | two families, opening the key/value family | 1.9 / 1.8 / 1.8 s, 498 MiB | 4.9 s, 1.04x | 3.2 s, 1.00x; bound 5.7 s | 4.8 s, 1.20x | 3.9 s, 1.07x |

  At `1eb9de5` every debris and refusal open answered `AuthorityUndetermined` at about a clean
  open's time and peak. Every threshold holds now: clean opens keep their peak (within 0.2%) and
  their time (between -10% and +3.3% against `1eb9de5`), debris and refusal opens peak at most
  1.07 times the clean peak, and the slowest open against its bound is the map refusal, 24.6 s
  against 25.5 s.
  The third round's tree failed the memory threshold twice and the time threshold in four refusal
  rows, which is what this round's change of comparison fixed, not the thresholds.

  Fifth round (the thresholds above, unchanged). The harness is now part of the repository,
  `examples/open_with_debris.rs` (std and the public API only); its documentation gives the
  commands, and the runs below are, per workload SHAPE (`map1`, `set1`, `map10`, `kv`, `multi`):

  ```text
  cp WORKING_TREE/Cargo.lock .   # in another revision's copy: Cargo.lock is not tracked
  cargo build --release --locked --example open_with_debris
  open_with_debris build SHAPE DATA/SHAPE && open_with_debris stage DATA/SHAPE
  # per variant (clean, debris, torn, refusal), run and tree, alternating the trees:
  rm -rf work && cp -a DATA/SHAPE/VARIANT work && sync && open_with_debris open FAMILY work
  ```

  `stage` writes three variants beside the clean store: the debris a closed compaction stages
  (`debris`), that debris with its largest file cut by one byte (`torn`: the fourth round's
  "refusal" shape, which is a staging write killed inside its last record and which D3 now
  removes), and that debris with the last byte of its largest file changed (`refusal`, which D3
  must read to its end and then refuse). Built against `1eb9de5`, against the fourth round's tree
  before any production edit of this round (the baseline), and against this round's tree, and
  run on one machine, two runs each, trees alternating, a fresh copy per run. Peaks in MiB, times
  in seconds, the slower run; "bound" is the time threshold above, from this round's clean open
  and `1eb9de5`'s clean opens of the staged families.

  | Workload (family opened) | Clean, `1eb9de5` / baseline / now | Debris, baseline / now | Torn, baseline / now | Refusal, baseline / now | Bound |
  |---|---|---|---|---|---|
  | map1 (map) | 10.6 / 10.6 / 10.4 s, 2386 MiB | 19.3 / 19.0 s, 1.01x | 23.6 s refused / 18.9 s removed, 1.01x | 20.0 / 18.7 s, 1.01x | 26.3 s |
  | set1 (set) | 9.1 / 9.4 / 9.3 s, 1579 MiB | 14.9 / 15.5 s, 1.00x | 18.7 s refused (1.06x) / 16.8 s removed, 1.00x | 15.3 (1.06x) / 15.1 s, 1.04x | 23.0 s |
  | map10 (map) | 5.1 / 4.8 / 5.5 s, 1187 MiB | 9.3 / 7.7 s, 1.01x | 11.8 s refused / 7.8 s removed, 1.01x | 8.3 / 8.3 s, 1.01x | 13.2 s |
  | kv (kv) | 3.6 / 3.4 / 3.5 s, 871 MiB | 5.8 / 5.9 s, 1.00x | 7.0 s refused (1.07x) / 6.1 s removed, 1.00x | 5.8 / 5.8 s, 1.07x | 9.0 s |
  | multi (set) | 0.9 / 0.9 / 0.9 s, 498 MiB | 2.2 / 3.0 s, 1.00x | 3.9 s refused (1.07x) / 2.2 s removed, 1.00x | 3.2 / 3.3 s, 1.07x | 5.0 s |
  | multi (kv) | 1.9 / 1.8 / 1.9 s, 498 MiB | 3.1 / 3.1 s, 1.00x | 3.8 s refused / 3.1 s removed, 1.00x | 3.2 / 3.2 s, 1.05x | 6.1 s |

  At `1eb9de5` every debris and torn open answered `AuthorityUndetermined`, and every refusal
  `InvalidArtifact` (the changed last byte breaks a record's CRC-32), at about a clean open's time
  and peak. Every debris, torn and refusal open of this round is within its time bound and peaks at
  most 1.07 times its clean open. A torn staging, which the baseline refused after its comparison
  and the classification's second validation, now costs what a removed debris costs. One clean
  row read past the 5% time threshold in that batch: map10, 5.49 s against `1eb9de5`'s 5.11 s
  (+7.4%), one slow run of two (the other 4.90 s), on a code path this round does not change (an
  open with no debris leaves D3 before its comparison, and the release build of everything it runs
  is the baseline's; the baseline measured -6.8% there). Re-measured in a second batch with four
  runs per tree, trees alternating, every clean row is within the threshold against `1eb9de5`, by
  the slower run and by the median: map1 -0.7% / +0.7%, set1 -1.2% / -1.1%, map10 +2.3% / +2.7%,
  kv -4.9% / -3.0%, multi (set) -1.9% / -2.8%, multi (kv) -6.2% / -0.6%, every peak within 0.2%.
  The machine was more loaded during that batch (map1's clean open took about 19 s for all three
  trees), so its absolute times are not comparable with the table's, only its trees with one
  another.

  That re-measurement changed the procedure after a failing result, and is recorded as a decision
  (final review, which found it stated only as a re-measurement). The procedure set above is two
  runs per tree, the slower run; by it, map10's clean open failed the 5% time threshold in the
  first batch. The second batch used four runs per tree and reported the median beside the slower
  run. Principle IV requires a failed threshold to be fixed in the implementation, not weakened
  after measurement; the threshold was not changed, but how a threshold is measured is part of it,
  so the change is stated here with its reason rather than left implicit. The reason: the failing
  figure was one run of two, the other run of the same binary was 4.1% faster than `1eb9de5`'s
  slower run, the code an open with no debris runs is unchanged by this round, and two runs on a
  shared machine could not separate a regression from noise. Four runs per tree with the trees
  alternating could, and their slower run -- the original statistic, over more samples -- is
  within the threshold in every row. The final review's own runs, under load, found no clean-open
  regression either (map10 +2.7%, the other rows between -21% and -5%, peaks within 0.3%). And
  the clean opens were measured once more by the original procedure after the final review --
  two runs per tree, trees alternating, the slower run, nothing else running but a desktop (load
  average 3.6 on 22 cores), `1eb9de5`'s harness binary against this tree's -- with every row
  within the threshold: map1 +2.2%, set1 -1.4%, map10 +0.8%, kv -0.2%, multi (set) -0.7%, multi
  (kv) -1.0%, every peak within 0.05%. Absolute times were about 1.9 times the table's (map1
  19.5 s), as in the second batch, so again only the trees compare.
- **V.** Acceptance uses public opens (`try_init_new`, `assert_three_reopens`), public compaction
  results, `inspect_storage`, and byte snapshots of the parent directory. Private seams schedule
  what is otherwise unobservable, and are `cfg(test)` only:
  - the existing process-exit and pause checkpoints (`fault_checkpoint.rs`), with `StagingWrite`
    moved to where its name says (below) and two new cuts in the closed rollback: `RollbackRestore`
    after its restore rename and `RollbackCleanup` after its staging removal; and, added after the
    fifth review, `StagingWriteTorn`, which writes all of the first family's staged bytes but the
    last and then exits, so that a real process kill inside a staging write's bytes is in both
    cut matrices (a kill inside a large `write_all` leaves a prefix of the bytes);
  - a new manifest-publication fault seam (`publication::manifest_publication_faults`), keyed by the
    temporary's path and the publication's ordinal, which makes a given publication fail or panic at
    a given stage, so the real closed and online paths can be driven through their first
    publication and the online finalize rewrite;
  - a pause in an online cutover after its source moves and before `PreviousPublished`
    (`publication::online_source_move_pause`, keyed by the previous directory; added after the
    second review), so that a test can open the family while the cutover is live;
  - pauses of recovery at a point (`recovery::recovery_pause`, keyed by the canonical store
    directory and the point, first arrival only): between an open's directory-level recovery and
    its family recovery (`FamilyRecovery`, added after the third review as
    `family_recovery_pause`), so that a test can open the family again and start a cutover while
    the first open is still recovering; at D3's entry and between its proof and its first
    removal (`DiscardEntry`, `DiscardProved`, added after the fourth review), so that a test can
    change what the caller's path names, or run a second open through D3; and right after D3's
    identity check, before its first read (`DiscardIdentified`, added after the fifth review), so
    that a test can change what the caller's path names while D3 proves;
  - the `StagingCreate` cut fires between the staging directory's creation and the guard's
    construction (moved there by the fourth review; a kill there leaves the same directory as
    before, so its kill tests keep their meaning), so that a test can change the working directory
    between the two.

## Decisions
- **Correct the contract, not the order.** Spec 008 says "Publish `Prepared` and build ... staging"
  (contract step 3, plan line 62). The code builds and validates staging first, and the codec
  refuses an unfinalized closed manifest. Reordering is the persisted-format change Principle III
  forbids without a migration; FR-6 corrects the two documents instead (minimal edits, dated note).
- **D1 compares the manifest's bytes, not its existence.** The simpler rule, "leave everything
  while any main manifest exists" (the investigation's prototype), fails FR-1 in one state: a
  closed `CleanupPending` recovery whose final manifest removal failed (a file held open without
  delete sharing on Windows, say) returns `Ok` with the manifest still there, and the compaction
  then proceeds (`inspect_storage` looks only at the staging and previous paths).
  Staging left beside that manifest makes the next `CleanupPending` recovery refuse
  (`AuthorityUndetermined`, staging present). Comparing the bytes captured at staging creation
  removes exactly the attempt's own unpublished staging in that state too; `Prepared` renamed
  into place always changes the bytes (a fresh operation id and phase). An unreadable manifest at
  either moment leaves everything, and so does anything at the path that is not a manifest (a
  symlink to nothing reads as unreadable, not absent). The comparison, not the `keep_staging` call
  after `publish_closed_prepared`, is what keeps a published staging: that call is redundant today
  (a probe removing it leaves every test green) and stays as the explicit hand-off to `Prepared`
  recovery. spec.md's Amendments line records that "published" means
  renamed into place, which this rule implements. That state cannot be produced through a public
  call, so the rule is tested on the guard itself, in both directions, and the published direction
  also through a compaction whose `Prepared` rename succeeded and whose next step failed.
- **D1 asks whether the canonical directory is exactly the capture, not whether it is complete**
  (after the first review). The review proposed D3's precondition (a complete canonical generation)
  or, stricter, exact equality with the capture. Equality is chosen: it makes D1's argument
  self-contained (the staging is a compaction of exactly what the canonical directory holds), and
  it is at least as strict as D3's canonical condition, since a canonical directory equal to the
  capture is complete and holds every staged family. A canonical directory that changed but still
  validates keeps the staging for the next open, whose D3 removes it only if every staged file
  recovers exactly what the canonical directory recovers, and otherwise refuses as `1eb9de5` did
  (`a_compaction_whose_source_was_replaced_by_another_valid_generation_keeps_its_staging_and_its_error`,
  `a_source_truncated_under_the_claim_keeps_the_staging_that_holds_what_it_lost`,
  `a_source_rolled_back_under_the_claim_...`,
  `a_source_that_lost_a_delete_under_the_claim_keeps_the_staging_that_holds_it`). This Decision
  said that open's D3 removes it in every case, which the second review showed loses what only the
  staging held, and then that it removes it when the canonical directory holds everything the
  staging holds, which the third review showed loses a delete. Rejected: D3's precondition, which
  would also remove a staging beside a canonical directory that lost a family but still validates.
- **D3 proves the staging redundant by equality of states** (after the third review; the second
  review's version compared containment). With no capture to compare against, D3 cannot tell a
  canonical directory that gained a write from one that lost a delete, since a staged state records
  a deletion only by a fact's absence, so only equality proves that removing the staging loses
  nothing. What equality refuses keeps its error from `1eb9de5`, which Principle II accepts: a
  canonical directory that gained a write (which within the protocol cannot happen before D3 runs:
  the claim excludes every writer, and an open recovers before it writes), and a staged file torn
  past its header (a process killed inside a staging write, which no cut stops at). The acceptance
  cuts are all covered: `StagingCreate` leaves an empty staging, `StagingWrite` an empty file
  (shorter than a header), and every later cut complete files equal to the untouched canonical
  directory. Rejected:
  - containment, the second review's rule, which resurrected deleted keys, set members and map
    entries (measured above);
  - the review's alternative for a torn staged file, equality with the canonical state restricted
    to keys up to the staged file's last complete record: it depends on the snapshot encoding's key
    order and adds a third comparison for a state no cut produces, where refusing is simple and
    keeps `1eb9de5`'s error;
  - reading every staged file the replay rejects as holding nothing (the second review's rule):
    only a file shorter than a header is that, which is what the second review measured (1 to 63
    bytes are rejected, and hold no record); anything longer may hold intact records;
  - narrowing FR-1's amendment to changes that leave the canonical directory invalid (the second
    review's other alternative), which would keep the loss.

  Superseded in part by the next decision: the states are now compared as the bytes compaction
  stages, which is stricter still. And superseded for a torn staged file by the fifth review's
  decision below ("D3 removes a torn write of what compaction stages"): "a state no cut
  produces" took the test cuts for the kills they stand for, and a process killed inside a staging
  write's bytes produces exactly that state.
- **D3 compares a staged file with what compaction stages for the canonical state, byte for
  byte** (after the fourth review, which measured equality of states at 1.52 times a clean open's
  peak for a map of 2,000,000 single-entry keys, and 1.29 times for a set of as many single-member
  keys, past IV's 1.25). Compaction encodes a family's state deterministically -- keys, members and
  entries sorted, with the chain's timestamp metadata -- so the bytes it would stage now are
  computable from the canonical chain alone, and a staged file holding exactly them recovers
  exactly the canonical state. D3 replays each staged family's canonical chain once, encodes it,
  drops the state, and reads the staged file a block at a time against the encoding: no staged
  state is ever built, and the peak is the canonical chain and its state, which the open's own
  replay already holds. Every cut before `Prepared` stages exactly these bytes, so every one
  still recovers. It refuses more than equality of states, each with its `1eb9de5` error: a staged
  file that recovers the canonical state in other bytes (a byte copy of a canonical file, which no
  compaction writes), and a canonical key with an empty set or map in a family whose file is
  staged, which the encoding cannot record and which the third review's rule wrongly treated as
  no fact (fourth review, nit).
  Rejected:
  - streaming the canonical replay against the staged state, removing matched facts from a
    working copy (the review's first proposal): it still builds the staged state, and needs a
    replay that applies records to a structure it does not own;
  - comparing order-independent digests of the two states: a digest match is not a proof, and a
    collision removes the only copy of a state, which Principle II forbids;
  - skipping D3's replay only when the staged file is byte-identical to a canonical file: compaction
    re-encodes, so it would cover a planted copy and not a single real cut;
  - replaying the staged state into the structure the open keeps: D3 runs in directory recovery,
    for every family with a staged file, while the open builds only its own family.
- **D3 removes a torn write of what compaction stages** (after the fifth review, whose documents
  lens found that a kill inside a staging write's bytes, which a plain process kill reaches, left
  the store unopenable). A staged file whose bytes are the first bytes of what compaction stages,
  ending inside the header or inside a record, is removed; one that ends right after the header or
  on a record boundary keeps its error. The soundness argument has two halves that need each other
  (corrected by the final review: this said the file "records no deletion", which is false). Every
  byte of such a file, the gaps between its complete records included, is a byte of the canonical
  directory's own current encoding, so every absence it records -- a key sorting between two of
  its complete records -- is an absence in the canonical state too, and removing it loses no fact
  and no delete the canonical directory does not hold; the byte comparison is what makes that so,
  and a comparison of only the facts in its complete records would lose a delete. And since its
  header or its last record is cut, it is no complete snapshot of any state and records nothing
  about the keys beyond the cut, which a whole snapshot records as deleted. A complete snapshot
  ends after its header or after a record, and a file ending there cannot be told from a write
  stopped there, so it keeps its error. A real kill does not stop a write at any byte: it leaves a
  length that is a multiple of the page size or of a larger page-cache folio, so in a store whose
  staged records all have one length a fixed share of kills stop on a record boundary (one in
  three for 96-byte records), not a rare one; this decision leaves those with their error, and the
  final review's way to remove them is recorded under "Not done here". The file the
  `StagingWrite` cut leaves (empty) and a cut inside the header are torn writes too, so this
  replaces the fourth review's short-file rule, which decided from the replay's answer and
  removed short legacy files holding intact records (fifth review, authority minor). The tear is
  read from the expected encoding's own record lengths (`V2CodecProbe::record_encoded_len`), not
  from the staged bytes, which are known only to be a prefix of it. Rejected:
  - stating the refusal as a residual (the review's other option): it leaves FR-3, the P1
    requirement, unmet for any process killed inside a staging write of a store whose staged file
    takes more than one write system call, and for an operating-system crash that keeps a
    page-granular prefix of an unsynchronized file. The reasons this plan gave for declining no
    longer held under byte comparison: "a state no cut produces" (the cuts stand for kills),
    "depends on the snapshot encoding's key order" (the byte rule depends on the whole encoding
    already), and "a third comparison" (the tear is one more answer of the same block comparison);
  - accepting any prefix of the expected encoding (the review's probe A): a prefix ending on a
    record boundary is a complete snapshot of fewer facts, which records the rest as deleted;
    three existing tests fail with it (a lost delete truncated at the boundary, a replaced
    generation, a canonical directory that gained a write);
  - accepting a prefix whose own replay ends in a recoverable tail (the review's probe B): sound,
    but it builds the staged state, which IV's peak bound excludes;
  - keeping the fourth review's short-file rule beside it, with the review's fix ("a prefix of
    what compaction stages"): that fix is this rule's case for a cut inside the header.
- **D1 compares the capture's bytes, not their CRC-32** (after the fifth review). The capture's
  bytes stay resident for the whole attempt, since its source revalidation compares them, so the
  guard shares them through an `Arc` (no copy) and compares each canonical file with them, one file
  read at a time, only when the attempt fails before `Prepared`. Rejected: keeping the staging
  whenever the attempt failed in its source revalidation (the review's other proposal): it covers
  the measured failure and not damage that keeps a file's length and CRC-32 while the attempt
  fails elsewhere (in its staging validation, say), which the guard would still read as the
  capture; and any digest, which is not a proof (the decision above).
- **The encoder dependency is pinned, not removed** (after the fifth review). D3's comparison is
  with the current encoder's output, so a change to that output narrows FR-3 for debris staged
  before an upgrade (III). A frozen fixture written by `1eb9de5` makes such a change fail a test
  when its data can show it: since the final review, which regenerated it with order-revealing
  shapes, a change in the order of keys, set members or map entries fails it (III).
  Rejected: recording in the staged files which encoder wrote them, which is a persisted-format
  change (Principle III), for a rule that already fails closed.
- **D3 acts only in the directory its open or compaction locked** (after the fourth review). It
  resolves the caller's path once and requires the result to be the identity the lease or claim
  locked, then reads and removes only through paths built from it. Rejected: resolving once
  without comparing with the identity (the review's proposal as worded), which keeps the proof and
  the removal in one directory but can act in a directory whose locks were never taken if the path
  changed between the open's admission and D3 (pinned by
  `a_discard_leaves_a_directory_its_open_or_claim_did_not_lock`); and comparing device and inode
  numbers at the proof and before the removal, against a rename of real directories on the
  resolved path. That rename would also leave the lock files covering another directory, so a
  check here would close the window for this rule alone; specs/011 does not state the limitation
  (this plan said until the fifth review that it did), so it is recorded as this spec's residual
  ("Not done here").
- **D1 and D2 resolve before they create** (after the fourth review). The guard's paths are
  resolved before `create_dir` and the staging directory is created through them; a publication
  resolves its temporary and main manifest once and creates, renames and removes through them.
  Rejected: resolving the publication's temporary alone, which keeps the rename on the caller's
  spelling and lets it move another directory's temporary into place after a working-directory
  change -- a hazard `1eb9de5` already had, which this rule would otherwise keep beside a removal
  that no longer shares it.
- **The review amendments are kept, narrowed, and gated on sign-off** (after the fourth review,
  whose documents lens found the code shipping return semantics that only the unsigned 2026-10-03
  amendments approve). The per-family lease count, (a), was already withdrawn by the third review.
  D3's comparison, (b), is kept in the byte form above rather than replaced by a refusal: a
  refusal in every state would return every pre-`Prepared` cut to `1eb9de5`'s error, which is
  withdrawing FR-3 (closed), the P1 requirement this specification exists for. FR-7's `Io` and the
  refusal after a staging write over a changed source are kept for the same reason as before
  (Principle II: neither removes anything the old behaviour kept). spec.md's Status line names
  them and bars merge and release until the approver's sign-off is recorded there; this plan
  cannot record it.
- **D4 and D6 are withdrawn, not gated** (after the third review; the second review gated them on
  the family's lease count, which this round removes). A rule that removes or moves what a live
  online attempt owns needs every attempt that could own it excluded while it acts. Nothing in the
  library gives that: the registry's lease count is a snapshot taken at admission, and specs/011's
  locks are skipped where FR-11 says. Rejected, each against the governing requirement (Principles
  II and IV):
  - re-reading the count immediately before each action: it narrows the window between the read
    and the action, and does not close it;
  - acting only when the open holds a real lock: it closes the FR-11 case and not the in-process
    one;
  - a per-family "recovering" mark in the registry entry that an online attempt's start waits on
    or refuses, or recovery serialized per registry entry (specs/011's deferred item): new
    coordination state, with its own lock ownership, ordering and deterministic progress tests,
    which is a specification of its own;
  - refusing a second open of a family already open in the process: a new public refusal.

  Withdrawing returns both states to `1eb9de5`'s error, which is fail-closed; neither is reachable
  without a process kill inside an online publication or a cutover's moves, since FR-2 removes the
  temporary of every online publication that fails.
- **One guard from `create_dir` to `Prepared`.** `prepare_closed_staging` keeps its signature and
  disarms its guard before returning (the helper tests call it and keep staging);
  `prepare_closed_staging_guarded` returns the staging with its armed guard to
  `compact_closed_directory`, which disarms it after `publish_closed_prepared`. The ad-hoc
  `remove_dir_all` on a write error is replaced by the guard, which also covers a panic during the
  writes. Rejected: arming in `compact_closed_directory` after `prepare_closed_staging` returns (the
  prototype), which leaves a panic in the staging writes uncovered.
- **D3 returns at once.** After a discard the open reports `Recovered` without calling
  `classify_untrusted_closed_authority`: the discard's preconditions are exactly that classifier's
  "everything missing" early return once staging and the temporary are gone, and calling it would
  read and validate every WAL chain of the canonical directory a second time.
- **D3 is strict.** A `.pigment-lock`, a sealed-segment name, a subdirectory, a symlink or a family
  the canonical directory lacks inside staging keeps today's error. Compaction never writes any of
  them (the staging reopen takes no lock, `ProcessLockPolicy::CoveredByClosedClaim`), so their
  presence is not evidence of compaction's own debris.
- **D5's condition is the state, not a marker.** No new manifest field or phase: the rollback is
  recognised from canonical equal to the source inventory and previous absent.
- **D5 covers both windows of the rollback's tail, which spec.md did not.** FR-4 names the state
  "staging present"; a kill between the rollback's staging removal and its manifest removal leaves
  the same state without staging, which the re-run refused in the same way (measured: a new cut,
  `RollbackCleanup`, after the staging removal). FR-4's title, "a closed rollback can be repeated",
  needs both, and the authority argument does not depend on staging. spec.md's Amendments line
  records the extension. Rejected: handling only the named state, which leaves the rollback
  unrepeatable in its last window.
- **`StagingWrite` moves to where its name says.** At `1eb9de5` it fired after
  `write_staging_families` returned, at the same point as `StagingSync`, so no cut killed a staging
  write in progress. It now
  fires once the first family's active file has been created and before any byte is written to it,
  leaving a staging directory with one empty file: spec.md's "mid-write". Every test naming the cut
  keeps its meaning (the cut matrix accepted either state).
- **Tests that pinned the old behaviour are rewritten, not deleted**, and verification.md records
  each: the cut matrix's acceptance of `AuthorityUndetermined`/`InvalidArtifact` at pre-`Prepared`
  cuts; `untrusted_manifest_evidence_distinguishes_ambiguity_from_invalid_debris_without_mutation`'s
  manifest-less staging copy; `failed_temp_publication_preserves_main_phase_and_unpublished_evidence`'s
  surviving temporary.
- **CI.** The new unit-test module `compaction::unpublished_attempt_tests` holds the closed-compaction
  and planted-state tests and runs on every operating system (a pinned `recovery.yml` step, as 014
  did): the deletions depend on filesystem semantics (symlink metadata, directory removal, Windows
  sharing) that differ by platform. The tests that drive online compaction (D2's online paths, and
  the controls for the withdrawn D4 and D6) live in `online_tests.rs`, which CI runs on Linux like every other online test; online compaction
  has no macOS or Windows CI run, and adding one is outside this spec.

## Not done here
- Relative store paths whose parent is `""` (spec 016). Since the second review FR-1 holds for them
  and for a symlinked store path (D1); the compaction itself still fails for a relative path, and
  for a symlinked path its artifacts are named beside the link, where an open, which looks beside
  the directory the link names, never sees them. Spec 016 owns both. (Note, 2026-10-03: "spec 016"
  here, in the Motivation note and in D3's paragraph names the planned path-spelling work, now the
  deferred Draft A in `reviews/opus-5.5-maintenance-2026-10-02.md`; the number went to
  specs/016-online-attempt-recovery.)
- The cost of `classify_untrusted_closed_authority` reading and validating every WAL chain of the
  canonical directory at every open, even when no maintenance sibling exists (found by the same
  investigation; a separate change with its own measurement).
- An online finalize rewrite that fails after its rename under Physical while the writer stays
  attached (reading only; no reproduction).
- A closed `CleanupPending` recovery that returns `Ok` with its manifest still present and lets the
  compaction proceed (D1's decision handles its consequence for this spec; the behaviour itself is
  unchanged).
- From the first review, each found to predate this spec and to need its own specification:
  - **A second live instance of one family on one directory in one process.** (Note, 2026-10-03:
    closed by specs/016 FR-1, which refuses such an open before any recovery.) The process registry
    counts opens and refuses only against a closed claim (specs/011 FR-10), so a second
    `try_init_new` of a family already open in the process succeeds. Measured at `1eb9de5`, with no
    change from this spec involved: two such key/value instances writing leave a WAL the next open
    refuses (`InvalidArtifact`); a write by the second after the first's online compaction is lost;
    and the second's open during the first's live online attempt, after its unfinalized
    `Prepared` and before its finalize, abandons that attempt's manifest and staging (`Recovered`),
    so the attempt fails. Those three predate this spec and are unchanged by it. A second instance
    that only opens and reads is harmless at `1eb9de5` (measured by the second review), and with D4
    and D6 withdrawn this spec leaves it so: no rule here acts on a family's online artifacts.
    Closing the three means refusing such an open, or recording live online attempts in the
    registry, which is a new public refusal and new coordination state (Principles III and IV).
- From the second review:
  - **Two first opens of different families in one process can run D3 at once.** Both take the
    directory's recovery path, and one can remove the staging or the temporary while the other
    lists or removes it; the loser's open then fails with a raw `NotFound` (`RecoveryError::Io`),
    and the next open is clean. Nothing is lost and no debris is left. Measured on the final tree,
    three first opens of the three families racing over a staging copy and a temporary, 300 runs:
    300 `Recovered`, 581 `Normal`, 19 `NotFound` (15 removing the temporary, 4 removing staging),
    no debris left in any run. This is the defect specs/011
    records under "Found during review, not fixed here", which needs recovery serialized per
    registry entry; D3 makes it reachable in a state `1eb9de5` refused outright. Treating
    `NotFound` as already removed would narrow the window, not close it: an open that lists a
    staging another thread is removing finds it incomplete, and refuses. Not changed here. Since
    the fourth review one interleaving is pinned deterministically (IV): the second open removes,
    the parked one fails with `NotFound`, nothing is lost and no debris is left.
  - **FR-3's "staging first" is pinned on Unix only**, by the removal-failure test, which skips
    where the process may write into a read-only directory (root). A reorder would leave a
    temporary or a staging the next open removes; no Windows seam fails a staging removal.
  - **No-replace moves outside Windows Physical.** `durability::move_namespace` is
    `std::fs::rename` there, for publication's moves (D6, which also used it, is withdrawn).
  - **Physical directory barriers in closed recovery.** The closed `Prepared` rollback, the closed
    `PreviousPublished` rollback (and D5, which completes it) and closed `CleanupPending` recovery
    remove the manifest with no barrier after their renames and removals; a power loss could keep
    the removal and lose a rename. Read only, not reproduced; the outcome reasoned from the code is a
    refusal with every generation still present, not a loss.

- From the third review:
  - **FR-3 online and FR-5 (withdrawn, D4 and D6).** (Note, 2026-10-03: closed for opens by
    specs/016; an online compaction's start keeps both errors.) A process killed inside an online
    first
    publication leaves a lone family `.manifest.next`, and one killed inside a cutover's source
    moves leaves a split source; both keep `AuthorityUndetermined` as at `1eb9de5`. Recovering them
    safely needs a family's recovery excluded from its other instances' online attempts in this
    process, and, where locks are skipped (specs/011 FR-11), from other processes'; that is new
    coordination state and its own specification.
  - **A staged file torn past its header** (a process killed inside a staging write's bytes) kept
    its error from `1eb9de5`. Withdrawn from this list by the fifth review, which found the reason
    false (a kill produces it) and D3 now removes it ("D3 removes a torn write of what compaction
    stages"); what remains is the next list's record-boundary case.
- From the fourth review:
  - **D3's cost.** An open that finds closed debris replays each staged family's canonical chain
    once more, whichever family it opens (IV, measured); an open that refuses pays that comparison
    and then the classification's validation of the canonical directory, which `inspect_generation`
    in D3 has already done once. Checking the staged names before that inspection removes the
    double validation for a foreign name, not for a staged file that differs.
  - **A rename of real directories on a resolved path** while D1's guard or D3 runs makes them act
    in whatever the path names then, as it leaves the specs/011 lock files covering another
    directory. This said until the fifth review that specs/011's Known limitations own it; they do
    not (they name a symlink repointed on the store path, inode paths and mount views, not a
    rename), so it is this spec's residual. Closing it needs device-and-inode checks at the proof
    and at the removal, or specs/011 stating the limitation through its own amendment.
  - **Where locks are skipped (specs/011 FR-11), D3 beside another process's live closed
    attempt** can race that attempt's rename of staging to canonical, and its removals have no
    Physical directory barrier (D3, last paragraph). Read from the code, not reproduced. A barrier
    would need the open's durability policy in directory recovery, which does not take one today;
    the outcome without it is a refusal with every generation present, as for the other
    closed-recovery branches above.
  - **A byte copy of a canonical file planted as staging** keeps its `1eb9de5` error, though it
    recovers the canonical state (D3's byte comparison; no compaction writes it).
- From the fifth review:
  - **A staging write killed exactly on a record boundary, or right after its header,** keeps its
    `1eb9de5` error, `AuthorityUndetermined` with every artifact: the file is a complete snapshot
    of fewer facts, which records the rest as deleted, and cannot be told from a write stopped
    there. This said a kill lands there only when the bytes written so far happen to end a record;
    the final review measured that a real kill lands there for a fixed share of kills (next
    list). An operating-system crash that keeps an unsynchronized staged file's length with
    unwritten blocks (zeros, say) is not a prefix of what compaction stages and keeps its error
    too.
  - **An encoder whose output changes** returns debris staged before the upgrade to its
    `1eb9de5` error (III); the frozen fixture makes such a change fail a test rather than pass
    unnoticed. It does not decide what the change should do instead.
  - **The removal error's label at an open** is `RecoveryOperation::Inspect` (III), as for every
    compaction-side I/O error directory recovery returns to an open.
- From the final review:
  - **Real kills inside a staging write stop at page or folio boundaries**, so the record-boundary
    exception above is not rare for every store. A process killed inside one large `write(2)`
    leaves a file whose length is a multiple of the page size, or of the page cache's folio: six
    of six SIGKILLs of a 1 GiB write left multiples of 2 MiB on the reviewer's Linux ext4. When
    the staged records all have one length `r`, the share of those offsets that are record
    boundaries is fixed: gcd(r, folio)/r, when that gcd divides 64 (the header's length), which is
    one in three for 96-byte records. Measured by the review: a key/value store of 50,000 records
    of 96 bytes had 391 of its 1,171 4 KiB multiples on record boundaries; in nine end-to-end runs
    of a release `compact_directory_in_place` over 2,000,000 such records, SIGKILLed while its
    staged file grew, three stopped on a record boundary and their opens returned
    `AuthorityUndetermined` with the staging left, and six recovered every key. For the shapes
    plan IV measures the rate is lower (key/value records of 229 bytes, about one folio boundary
    in 229; key/set records of 102 bytes, about 2%). Not a Principle II violation: both copies are
    kept, as at `1eb9de5`. It is the liveness defect FR-3 exists to fix, reached at that rate.
    Pinned, with both halves of today's rule, by
    `a_staging_write_killed_on_a_page_boundary_keeps_its_error_only_where_a_record_ends`. Closing
    it needs a production change, which this round does not make (a production change needs its
    own RED, measurement and the approver's sign-off, since a state that now keeps its error would
    recover): the review proposes setting the staged file's length to the encoding's (`set_len`)
    before writing it, so that a write torn at any point leaves the full length with a zero tail,
    and letting D3 remove "the encoding's first `k` bytes followed by zeros to its length" for any
    `k`. No complete snapshot ends in zeros (a record starts with its marker and ends with a
    nonzero footer), and `1eb9de5` already refuses every such file, so older binaries would treat
    it no differently; debris staged without it would still follow today's rule.
  - **D1 reads each canonical file whole** (`exact_artifact_bytes_match`, `fs::read`) when it
    compares it with the capture, one file at a time and only when the attempt fails before
    `Prepared`. That is the pattern and the peak of the source revalidation in
    `publish_closed_prepared` that precedes it, not a regression (the final review measured a
    failing compaction of a 229 MB store: 917 to 938 MiB peak at `1eb9de5`, 917 MiB now). A
    block-wise comparison would lower both, and is left to a change that does both.

## Release
The change lands on `main`; a consumer pins that tested revision.
