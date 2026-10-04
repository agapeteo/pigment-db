# Online attempt recovery: tasks

Each task observes RED for its behaviour, as an assertion failure, before the production change
that satisfies it. Test infrastructure (T002) precedes the first RED. The over-reach controls are
written first and must pass at `9dff848` and after every task: each keeps its error and leaves the
parent directory byte-identical apart from the lock files the open itself takes.

- [x] T001 Write the plan and tasks. Record the suite baseline at `9dff848`.
- [x] T002 Test infrastructure, no behaviour change:
  - `manifest_publication_faults::Fault::Exit` (a process ended inside a manifest publication) and
    `online_source_move_exit` (a process ended after a cutover's `n`-th source move);
  - spec 015's namespace snapshot records a held lock file by its presence on Windows, as
    `tests/directory_lock.rs` does (a held lock's bytes cannot be read there);
  - the module `compaction::online_attempt_recovery_tests`, with its killed child
    (`killed_online_attempt_child`) and its over-reach controls, observed passing at `9dff848`,
    each family:
    - a family temporary beside family staging, beside a previous directory, beside a corrupt
      family, beside no family, beside a corrupt family manifest, a temporary that is a directory,
      a symlink to a regular file, and beside a family manifest that links to nothing;
    - a split whose moved artifact changed, whose moved artifact is also in the store, with a
      foreign file or a subdirectory in the previous directory, whose unmoved artifact changed or
      is missing; a split beside an unfinalized `Prepared`; a previous directory that is a symlink;
      a previous directory that cannot be read (Unix);
    - a dead attempt's leftovers beside directory-level maintenance (a lone closed
      `.manifest.next`): directory recovery's `InvalidArtifact`, every byte kept;
    - FR-3: a dead attempt's leftovers opened with both lock files skipped (`Unsupported`).
- [x] T003 FR-1 (P1):
  - RED: `tests/one_family_instance.rs` (a second open through six spellings, every entry point,
    before recovery, H1, H2); `family_hold_tests` (behind a `Pending` entry; while the first open is
    parked in recovery); H3 (`a_second_open_during_a_live_online_attempt_is_refused_and_the_attempt_completes`);
    spec 015's live-cutover tests rewritten to expect FR-1's refusal of the second instance
    (`a_second_open_during_a_live_cutover_is_refused_and_the_cutover_completes`,
    `a_later_instance_cannot_open_while_an_open_of_its_family_is_still_recovering`, the second
    half of `a_compaction_over_a_lone_family_temporary_keeps_its_error_and_its_bytes`, and the
    held-family case of the lone-temporary test).
  - GREEN: the registry entry holds each open's family; `admit` refuses a family it holds; the
    lease releases it. Controls passing before and after: dropping ends the hold, a failed open
    holds nothing, other families and directories open, a panic after admission leaves the family
    free.
  - Full suite: no other test opened one family twice.
- [x] T004 CI: RED the dedicated-target pin for `one_family_instance` in `tests/ci_workflow.rs`,
  then the workflow line.
- [x] T005 FR-2 (P1), a dead first publication's lone temporary (D4):
  - RED: `a_process_killed_inside_its_first_online_publication_reopens_recovered` (each family,
    killed at `Created`, `Written` and `Flushed`).
  - GREEN: `discard_dead_first_publication`, gated on `OpenDirectoryLease::family_writers`, read
    after directory recovery (FR-3's `locked` condition not yet in the gate).
- [x] T006 FR-2 (P1), a dead cutover's split source (D6):
  - RED: `a_process_killed_inside_its_cutovers_source_moves_reopens_recovered` (each family,
    killed after every one of its source moves and inside its `PreviousPublished` publication;
    still RED with T005's rule alone).
  - GREEN: `restore_split_online_source` before the abandonment, for a finalized `Prepared`.
  - Spec 015's tests that pinned the withdrawn rules' refusals, rewritten to FR-2's expectation and
    observed RED with both rules removed: `a_lone_online_manifest_temporary_is_removed_at_open_unless_its_family_is_open`,
    `a_finalized_prepared_split_by_its_source_move_restores_the_source`.
- [x] T007 FR-3:
  - RED (with T005 and T006 in place): `without_locks_a_dead_attempts_leftovers_keep_their_errors_and_their_bytes`
    and spec 015's `a_process_without_locks_opening_during_a_live_cutover_leaves_it_alone` (the
    lock-less child restored the live split).
  - GREEN: the gate requires the inner lock to be really locked.
- [x] T008 CI: RED the pin `online_attempt_recovery_tests_run_on_every_operating_system` (and its
  gated-step control), then the "Online attempt recovery" step.
- [x] T009 Pins added after GREEN, each caught by a probe of the property it pins:
  `a_restore_interrupted_part_way_is_completed_by_the_next_open`,
  `a_live_cutover_is_left_to_complete_whoever_else_opens_or_compacts_its_family`,
  `a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked`, `family_writers_tests`
  (each condition of the gate), and `dropping_the_instance_ends_the_hold_while_another_family_keeps_the_directory_open`.
- [x] T010 Documents: the open rustdoc of each family; spec 008's contract (the `Prepared` online
  paragraph and the classification bullet); dated notes in specs/011 and specs/015 where spec 016
  changes their statements; spec.md's Status line and Amendments.
- [x] T011 Full suite, doc tests, `cargo fmt --check`, Clippy, both builds, the clean-open
  measurement, neutralization probes. Record everything in verification.md.
- [ ] T012 macOS and Windows: the two new CI targets on a CI run of the published revision (not run
  here; Linux only).
- [x] T013 First review (verification.md, "Review 1 (final)"). No production behaviour changed;
  each new test observed passing on the tree and failing under a probe of the property it pins:
  - tests: the removal's and the move's I/O error after FR-2 proved its state (Unix), every read
    and write of the rules in the directory the open locked (through the test-only pause
    `DeadAttemptIdentified`), D6 only for a manifest bound to the family, FR-2 under Physical
    durability, D6's directory barriers before the manifest is removed (not Windows), and FR-1's
    hold belonging to its own instance with a refusal leaving the registry unchanged;
  - the killed child under Physical durability, and on Windows a bounded wait for its locks;
  - the probe runner reads libtest's closing `failures:` list; P09, P11 and P17 to P21 re-run;
  - documents: spec.md's Status line states the return-semantics change in full (no sign-off
    recorded) and one wording amendment; plan.md II, III, IV, V and "Not done here" (two
    production changes recorded, not made); the families' open rustdoc; notes in specs/011 FR-10
    and specs/015's plan, tasks and verification.
