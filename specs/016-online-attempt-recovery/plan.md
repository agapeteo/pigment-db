# Online attempt recovery: plan

## Technical context
Rust 2021 library crate, MSRV 1.91. The baseline is `9dff848`, whose code is `ce7f0f9`'s (spec 015);
`9dff848` changed only a review file.

**The process registry** (`src/maintenance_coordination.rs`) keeps one entry per store directory,
keyed by the directory's identity (its canonical path, specs/011 FR-2). An entry counts open leases
and holds the directory's lock files. An open is refused in the process only while a closed claim
exists, and a claim only while an open exists (specs/011 FR-10). So a second `try_init_new` of a
family already open in the process is admitted, shares the entry's locks, and runs recovery like
any first open. Spec 015's reviews measured what that allows, with no compaction rule involved
(verification.md of spec 015, "Review", H1 to H3): two key/value instances writing leave a WAL the
next open refuses; a write by the second instance after the first's online compaction is
acknowledged and lost; and a second open during the first instance's live online attempt removes
that attempt's manifest and staging, so the attempt fails. Its third review measured how the same
admission defeated the withdrawn online rules (D4, D6): an open admitted alone and still
recovering restored the live split of an instance admitted after it, and a process whose locks
are skipped (specs/011 FR-11) restored another process's live split.

**Family recovery** (`resolve_online_maintenance_for_compaction`, `src/compaction/recovery.rs`)
runs in an open after directory recovery and before `ensure_inner_lock`, and at an online
compaction's start after the instance's attempt token is claimed. At `9dff848`:
- a lone `<active>.pigment-compact.manifest.next` (no family manifest) returns
  `AuthorityUndetermined`;
- a finalized online `Prepared` whose source the cutover's moves split between the store and
  `<active>.pigment-compact.previous` returns `AuthorityUndetermined` from
  `abandon_prepared_online`, which requires every source artifact in the store.

Only a process killed inside an online attempt leaves either state: the first publication writes
the temporary before its rename (a failed publication removes it, spec 015 FR-2), and
`publish_online_previous` moves the source one artifact at a time.

**How the code knows whether specs/011 took a lock**: a held `LockFile` has `locked`, false only
where std reported `Unsupported` and the lock was skipped (FR-11); an entry has `takes_locks`,
false only for the staging reopen a closed claim covers; and the entry's `InnerLock` is `Held`,
`Absent` (while directory-level maintenance is recovered, before `ensure_inner_lock`, specs/011
FR-5) or `Acquiring`.

**Directory recovery refuses before family recovery could meet either state**: every
directory-recovery branch validates the canonical directory, by `inspect_generation` (which refuses
any name that is not a family's active or sealed segment) or by `generation_matches` (exact names),
and a family's online artifacts are in the canonical directory. So an open that finds
directory-level maintenance and either state is refused by directory recovery (measured:
`InvalidArtifact` naming the store, for every family and both states; pinned below).

**Motivation (provenance):** penpack finding V415, whose fix compacts its stores online (spec.md).

## Constitution check
- **VI (first).** The change is stated in the library's terms: store directories, families,
  instances, opens, the process registry, lock files, online compaction, its manifest, temporary,
  staging, previous directory and cutover, recovery. Scan of the whole change (spec.md's Status
  and Amendments, this plan, tasks.md, verification.md, the code, the tests, the workflow, and the
  dated notes in specs/008, specs/011 and specs/015) for consumer and application vocabulary: the
  motivating consumer, penpack, and its finding id appear only in spec.md's marked Motivation note,
  this plan's Motivation note and this Constitution check, and verification.md's Principle VI
  record. Test data are opaque bytes (`key-0`, `group-1`, `book-2`, ...). The spec directory is
  `016-online-attempt-recovery`. No public API, type or error variant is added. The new refusal is
  `RecoveryError::Io` of kind `WouldBlock`, the shape specs/011 gives another process's open. It
  lands on this repository's `main` before any consumer pins it; verification.md records the
  revision once it is published (tasks.md).
- **II. Each rule, and why it cannot destroy the last complete authoritative state.**
  - **FR-1, the per-family hold.** It refuses an open and changes nothing, so it deletes nothing.
    It is what makes FR-2 sound in this process, and it closes H1 to H3, each of which loses or
    corrupts accepted writes without any compaction rule.
  - **The gate (`OpenDirectoryLease::family_writers`).** FR-2 runs only when it answers
    `OnlyThis { locked }`: this open's process holds the family in the directory (FR-1), and the
    entry holds the directory's inner lock, really locked (`locked`, FR-3), and still the file at
    `<store>/.pigment-lock` (compared by device and inode where the platform has them, as
    `ensure_inner_lock` compares). It is read once directory recovery is done, just before family
    recovery. Why that excludes every live online attempt of the family on the directory:
    - an online attempt is made by an open instance, through `try_compact_online`;
    - in this process, FR-1 admits no second instance of the family while this open's lease
      exists, from its admission (before any recovery) until it is dropped;
    - this open's own instance does not exist yet, so it has no attempt;
    - every other process's open instance holds the directory's inner lock for as long as it is
      open (specs/011 FR-1, FR-5), so it cannot be open while this open holds that lock; within
      the limits specs/011 states (its Compatibility section and FR-11).

    While an open holds only the replacement lock (it found directory-level maintenance), another
    process holding only the inner lock is not yet excluded, so the gate answers `NotExcluded`
    there. That costs nothing reachable: directory recovery refuses a canonical directory holding
    a family's online artifacts first (Technical context).
  - **D4, a dead first publication's lone temporary** (`discard_dead_first_publication`). Removed
    only when: the gate holds; the caller's path still names the locked directory
    (`fs::canonicalize` equal to the lease's identity), every path being built from that identity;
    the temporary is a regular file; nothing at all is at the family manifest's path (a symlink to
    nothing is not absent); nothing is at the family staging or previous path; and the canonical
    family inspects as valid (`inspect_open_family`). Authority: online publication moves a source
    artifact only under a finalized `Prepared` main manifest (`publish_online_previous` calls
    `online_publication_ready` and verifies the source first), and writes its staging only after
    its first `Prepared` was renamed into place. With nothing at the manifest's path and no staging
    or previous directory, no online attempt has published anything, the canonical family is the
    authority, and a `.manifest.next` alone never advances a phase (spec 008's contract). The gate
    excludes the one writer that could own the temporary, a live first publication. A path it
    cannot read proves nothing: the state keeps its error. A removal that fails after the proof
    returns its I/O error (III).
  - **D6, a dead cutover's split source** (`restore_split_online_source`). Runs only for a
    finalized online `Prepared` (the manifest's binding to the family was validated just before,
    in `resolve_online_maintenance`), when the gate holds and the caller's path still names the
    locked directory. Everything D6 itself checks is checked before its first move, through paths
    built from the identity: the previous directory is a real directory (not a symlink); each of
    its entries is a regular file named for exactly one source artifact, verifies against that
    artifact's descriptor (length and CRC-32, `verify_descriptor`, as closed recovery verifies a
    moved source), and is absent from the store; every source artifact not moved verifies in the
    store.
    Then each moved artifact is moved back (`move_namespace`, no-replace where the platform honours
    it), and both directories are synchronized under the manifest's durability. Authority: a
    finalized `Prepared` is written under exclusive coordination, and the cutover detaches the
    WAL's writer before its first move (`complete_online_cutover`: `finalize_online_prepared`,
    `take_online_writer`, then `publish_online_previous`), so the source is frozen and its
    descriptors are exact; restored, it is exactly the finalized source, every accepted mutation
    included. The existing abandonment then removes the empty previous directory, the staging and
    the manifest, as it did for a cutover killed before its first move. Interruption: each move is
    one rename, so a kill inside the restore leaves a split of another shape (some artifacts back),
    which the next open restores the same way; a kill after the last move and before the
    abandonment leaves a finalized `Prepared` with its whole source in place and an empty previous
    directory, which recovery at `9dff848` already abandons. Both are tested. A path it cannot read
    proves nothing; a move or barrier that fails after the proof returns its I/O error (III), with
    the finalized manifest still in place, so the next open restores what is left. The
    abandonment's own preconditions are checked only after the restore (first review): a split
    beside a family staging that is not a regular file is moved back, its empty previous directory
    removed, and then refused with its `9dff848` error. No dead cutover leaves such a staging and
    nothing is lost, since the moves go toward the canonical location and the manifest stays, but
    the directory does not keep its bytes, which the over-reach standard below requires ("Not done
    here").
  - **FR-3.** Where the gate does not hold, neither rule runs, and both states keep their errors
    and their bytes: fail-closed, as at `9dff848`.
  - **Over-reach controls** (tasks.md lists them; verification.md records each, with its error at
    `9dff848` and after the change, and the probe that each one catches). Each keeps its error and
    leaves the parent directory byte-identical apart from the open's own lock files.
- **III. Return-semantics changes, each listed in spec.md's Status line.**
  1. FR-1, approved: an open of a family this process already holds open in the directory, in
     `try_init_new`, `try_init_new_with_options` and `init_new` of every family, was admitted and
     returned what its recovery returned; it now fails before any recovery with
     `RecoveryError::Io { operation: Inspect, path: <the caller's path>, source }`,
     `source.kind() == WouldBlock`, the message naming the directory by its identity and the
     family as `key/value`, `key/set` or `key/sorted-map`. `init_new` panics with that message.
  2. FR-2, approved: an open over a dead first publication's lone temporary, or over a dead
     cutover's split source, returned `AuthorityUndetermined` and now reports `Recovered`.
  3. Not named in the approved text, so listed for sign-off: when D4 or D6 has proved its state
     and its removal or a move fails (permissions, a read-only mount, a file in use on Windows),
     the open returns that I/O error, `RecoveryError::Io { operation: Inspect, path, source }`,
     where `9dff848` returned `AuthorityUndetermined`. The path is the temporary for D4, and for
     D6 the artifact's path in the store or, for a failed directory barrier, the store. This is
     spec 015's FR-7 shape for its own rules; `map_compaction_recovery_error` labels every I/O
     error directory and family recovery return to an open `Inspect`. Once D6 has restored a
     split, the abandonment that follows runs where `9dff848` stopped before it, so its own
     failures (removing the empty previous directory, the staging or the manifest, or its
     barrier) return their I/O error in the same way, and a state it refuses keeps
     `AuthorityUndetermined` with the source moved back (II, D6). Pinned since the first review:
     `a_proved_lone_temporary_that_cannot_be_removed_fails_with_the_removal_error`,
     `a_proved_split_that_cannot_be_moved_back_fails_with_the_moves_error` (both Unix, skipping
     where the process may write into a read-only directory) and
     `a_restore_whose_directory_barrier_fails_keeps_its_manifest_for_the_next_open` (not Windows,
     where the barrier does nothing).

  Unchanged: no persisted format, no public signature, no lock-file name or location, and which
  lock files an open leaves after a refusal (specs/011, Intended behaviour change 4). An online
  compaction's start keeps both states' errors (Decisions). `inspect_storage` stays read-only.
- **IV.** No new lock and no wait. FR-1's hold is state in the existing registry entry, a set of
  at most three families, read and written under the registry mutex inside `admit`, where the
  closed-claim refusal already is. Lock order is unchanged (only the registry mutex is taken), and
  the check does no I/O, so specs/011 FR-13 holds: no lock-file I/O under the mutex. A refused open
  does not wait: it returns from `admit`. The one wait that already existed, behind another
  thread's `Pending` entry for the same directory, is unchanged. The gate reads the registry under
  its mutex and then makes one `symlink_metadata` call outside it. Progress tests: a second open of
  a family started while the first open's entry is `Pending` is refused once the first is
  admitted, and another family's open goes on
  (`an_open_behind_a_pending_entry_is_refused_once_the_family_is_held`; its outcome is
  deterministic, but nothing makes either open reach the `Pending` poll before the stall is
  released, so whether they wait behind the entry is the scheduler's: a probe panicking on that
  poll failed it in 70 of 70 runs, first review);
  a second open while the first is parked inside its recovery is refused within 5 s without
  waiting for it (`an_open_still_recovering_holds_its_family_and_a_second_open_does_not_wait_for_it`);
  an open that panics after admission leaves the family free
  (`an_open_that_panics_after_admission_leaves_its_family_free`).

  Performance. An open now inserts into and later removes from a three-element set under a mutex
  it already takes, and makes one more registry lookup and one `lstat`; D4 and D6 run only when
  their states exist. The threshold, set before measuring: a clean open stays within 5% of its
  time at `9dff848`, and keeps its peak, measured with spec 015's harness
  (`examples/open_with_debris.rs`, release build, one process per open, a fresh copy of the data
  per run, two runs per tree, trees alternating, the slower run). verification.md records it.
- **V.** Acceptance uses public opens, reads, writes and compactions, `try_init_new`'s status, and
  byte snapshots of the parent directory. Private seams, `cfg(test)` only, schedule what is
  otherwise unobservable:
  - `manifest_publication_faults::Fault::Exit` ends the process inside a given manifest
    publication of a given temporary, at a stage before its rename, running no destructor: a
    process killed inside the attempt's first publication, its finalize rewrite, or its
    `PreviousPublished` publication;
  - `online_source_move_exit` ends the process once a cutover has moved a given number of source
    artifacts;
  - the existing `recovery_pause` (`FamilyRecovery`), `online_source_move_pause` and specs/011's
    `lock_seams` (`Stall`, `inject_lock_error`, `inject_panic`);
  - the gate's own unit tests (`family_writers_tests`) call it directly, to pin each condition;
  - since the first review: `recovery_pause::Point::DeadAttemptIdentified`, reached by D4 and D6
    once the caller's path is found naming the locked directory, before either reads anything
    there; the killed child's open under `DurabilityPolicy::Physical` (an environment variable);
    and `durability::fail_directory_barrier_for` and `directory_barrier_calls`, which existed.

  The killed processes are the unit-test binary re-executed with an exact test name, as spec 015's
  kill tests are. On Windows, which releases a dead process's locks asynchronously (specs/011,
  Compatibility), the parent then waits at most 5 s until it can take each lock file the child
  may have held, as `tests/directory_lock.rs` does; elsewhere it does not wait (first review).

## Decisions
- **FR-1's refusal is made where an open is admitted** (`acquire_open_lease_with`'s `admit`, next
  to the closed-claim refusal), keyed by the directory's identity and the family, before any lock
  I/O for an existing entry and before any recovery. It has the shape of specs/011's refusal of
  another process's open (`RecoveryError::Io`, `Inspect`, `WouldBlock`), so a consumer handles one
  refusal; the message names the directory by its identity (the canonical path every spelling
  resolves to) and the family; `path` is the caller's spelling, as for every open error. Rejected:
  - a new `RecoveryError` variant: a public enum change, not approved, and a second refusal for
    consumers to handle;
  - returning the open instance, or waiting for it to be dropped: an API change, and a wait that
    deadlocks a thread holding the first instance (specs/011 puts waiting out of scope);
  - a separate per-family registry: a second coordination structure (IV) that would have to
    resolve identities and stay consistent with the first.
- **The gate is the inner lock, read after directory recovery, not any lock.** Every other
  process's open instance holds the inner lock for as long as it is open; the replacement lock it
  holds only in some cases. Rejected:
  - the second review's lease count: read at admission, stale while recovery ran, blind to other
    processes;
  - the replacement lock alone: an open holding only it does not exclude a process holding only the
    inner lock (specs/011 FR-5 refuses that open only after recovery);
  - taking the inner lock before family recovery (moving family recovery after
    `ensure_inner_lock`): unnecessary, since directory recovery already refuses whenever family
    recovery could meet either state in that path (pinned by
    `online_leftovers_beside_directory_maintenance_keep_the_opens_error_and_their_bytes`), and it
    would change which lock files a refused open leaves (specs/011 Intended behaviour change 4) and
    specs/011 FR-5's order, for nothing.
- **FR-2 runs at an open, not at an online compaction's start.** spec.md's FR-2 says "an open
  recovers", and its acceptance is a reopen. At an online compaction's start the states are reached
  only when planted, or by the compacting instance's own cutover failing after its moves; that
  failure marks the instance's WAL indeterminate, so a second compaction fails at its capture
  anyway (`online_capture_metadata`), and the instance must be reopened, which recovers. Running
  there would add a return-semantics change the approval does not cover (`AuthorityUndetermined`
  to another error or `Ok`). Rejected: running there too, which the exclusion would allow (the
  compacting instance is the family's only one, and its attempt token excludes its own second
  attempt).
- **Every path D4 and D6 act on is resolved once from the lease's identity, and they act only
  while the caller's path still names it**, as spec 015's closed discard (D3) does after its
  fourth review: a path read again at each step names another directory once the working
  directory or a symlink on the path changes. What the abandonment does after D6, through the
  caller's spelling, is as at `9dff848` (Not done here).
- **D6 runs before the abandonment, in `resolve_online_maintenance`'s `Prepared` arm**, so
  `recover_prepared_online` keeps the arguments spec 015's third review returned it to, and an
  online attempt's own abandonment (`abandon_online_prepublication`, an unfinalized manifest)
  never restores anything.
- **Tests**: FR-1's public acceptance is an integration test, `tests/one_family_instance.rs`, run
  on every operating system by the "Dedicated issue regression targets" step; its deterministic
  progress tests and the gate's tests are `maintenance_coordination::` unit tests, which the
  ownership step already runs everywhere. FR-2 and FR-3 are the unit module
  `compaction::online_attempt_recovery_tests`, which kills child processes through test seams, run
  on every operating system by a new pinned step, because D4 and D6 remove and move files, which
  each platform does in its own way. Spec 015's tests that pinned the withdrawn rules' refusals are
  rewritten in place where spec 016 makes them recover (verification.md lists each).

## Not done here
- macOS and Windows were not run (tasks.md): the new step and the integration test need a CI run
  of the published revision.
- An online compaction's start keeps both states' errors (Decisions).
- The exclusion is bounded by what specs/011 states: a process that forks without exec, network
  filesystems, a process of a version that takes no lock (the first upgrade), and the other Known
  limitations there. In particular, a process running a version before specs/011 holds no lock,
  so an open of this version can hold the inner lock beside that process's live online attempt and
  recover it; specs/011 already requires every process using a directory to run a version that
  locks.
- A rename of real directories on the resolved path while D4 or D6 runs makes them act in whatever
  the path then names, and leaves the lock files covering another directory: spec 015's residual,
  shared here.
- After D6, the abandonment and the open go on through the caller's spelling, as at `9dff848`;
  directory maintenance through any path spelling is a separate, deferred draft
  (`reviews/opus-5.5-maintenance-2026-10-02.md`, "Draft A").
- D4's removal has no directory barrier: family recovery takes no durability policy, and a
  temporary that a power loss brings back is removed again by the next open. D6's moves are
  synchronized under the manifest's durability, as publication's are.
- **D6 does not check the abandonment's staging precondition before its first move** (first
  review, confirmed; a production change, so not made in that review's pass). For a split whose
  `<active>.pigment-compact.next` is a directory or a symlink, the open keeps its `9dff848` error
  (`AuthorityUndetermined`), but every moved artifact is back in the store and the empty previous
  directory is removed: the review measured `unchanged=false still-moved=0` for each family and
  both kinds, where `9dff848` refuses the same state in `source_descriptors_match` with nothing
  changed. Nothing is destroyed, and no dead cutover leaves such a staging, but it breaks the
  over-reach standard (II). The fix: before the first move, also require
  `PathEntry::read(&at.staging).is_absent_or(is_regular_file)`, the abandonment's own staging
  precondition, and return `Ok(())` otherwise; then add "staging is a directory" and "staging is
  a symlink" to `a_split_the_manifest_cannot_account_for_keeps_its_error_and_its_bytes`, observed
  failing before the check and caught by a probe that removes it. spec.md's Status line names the
  behaviour meanwhile.
- **Where specs/011 takes a lock that does not exclude, the gate answers `OnlyThis`** (first
  review, not run on those targets; a production change). `family_writers` detects only a lock
  this process skipped (FR-11). specs/011's Known limitations name two places where a lock is
  taken and still does not exclude, and FR-2 can act beside another instance's live online attempt
  in both:
  - Solaris: std takes process-owned `fcntl` locks there, and cross-process ownership is not
    supported. A second identity of one directory in one process (a lofs or bind mount) also
    locks, where Linux's `flock` refused it in the review's bind-mount run, so FR-1's per-identity
    hold does not exclude it either. The fix would answer `NotExcluded` under
    `cfg(target_os = "solaris")`.
  - Two builds of one revision made with different toolchains on illumos, AIX or GNU/Hurd, where
    one build locks and the other skips: the locking build's gate answers `OnlyThis` beside the
    skipping build's live cutover. specs/011 states that limitation; nothing here closes it.
- **FR-1's refusal message says "is already open in this process"**, though the hold is taken when
  an open is admitted, before its recovery, so an open still recovering, which may yet fail,
  already refuses (first review; the message is production text, so not changed in that review's
  pass). The rustdoc of each family's open now says "open, or still being opened"; the message
  would say "is already open, or being opened, in this process".
- **The pending-entry progress test does not force the interleaving its name describes** (first
  review): a test seam counting the opens polling a `Pending` entry, waited on before the stall is
  released, would make it deterministic. IV states what it does pin.

## Release
The change lands on `main`; a consumer pins that tested revision.
