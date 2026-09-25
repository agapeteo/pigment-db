# Cross-process directory ownership: implementation plan

## Technical context

Ownership lives in `src/maintenance_coordination.rs`. Its process-static registry is keyed by
canonical directory, and every file-backed open and every closed-maintenance claim goes through it:
- the three stores' `try_init_new_configured`, through `acquire_open_lease_with`;
- `compact_closed_directory`, through `try_claim_closed`.

A registry entry gains the directory's inner and replacement locks, and a `Pending` state that
keeps lock-file I/O outside the mutex. The other changes are small:
- `compaction/inspection.rs`, `compaction/recovery.rs`, `compaction/publication.rs` and
  `compaction/mod.rs`: every exact inventory of a store generation skips a regular-file
  `.pigment-lock`, and both cleanups of `.previous` delete it (FR-9).
- `compaction/mod.rs`: the closed compactor retires the inner lock before revalidating and
  publishing (FR-4). The staging-validation reopen takes no lock, because the compactor's claim
  covers it.
- The three stores: after `resolve_store_maintenance`, the lease takes the inner lock if the open
  recovered directory-level maintenance (FR-5).

## Constitution check

**VI (settled first).**
- The change is stated in library terms: store directory, process, lock file, open, closed
  maintenance, recovery.
- Consumer vocabulary appears only in the spec's marked Motivation note.
- The branch and the spec directory carry no consumer name.
- The motivating consumer is penpack. It pins the `main` revision this change merges into, once
  published, and only then.
- No consumer key format, limit or workflow is encoded. Any embedder gets identical behaviour.

**I.** RED before GREEN, one behaviour at a time, in the order of tasks.md. For each test:
- **A1, A2, A7, X1, X2**, and the `init_new` panic: RED at the pre-change revision, where the second
  open succeeds.
- **A4:** RED because the lock file does not exist.
- **A6:** RED at the pre-change revision, and again against the step before its fallback.
- **A8:** RED against the step before the explicit unlock. The 010 prototype measured 343 of 400
  reopens refused.
- **A3:** RED at the pre-change revision (the compaction runs).
- **R1:** written after the claim step had landed, so probed by neutralization rather than
  observed failing in sequence (see verification.md).
- **The progress and `Unsupported` unit tests:** RED against the step before their fix.
- **The existing inspection and compaction tests:** they go RED when opens start creating
  `.pigment-lock`, which is what drives FR-9.
- **A5 and X3** are guards. A5 fails under any existence-based lock, and X3 under a lock that trusts
  its contents.

**II.** WAL, replay, recovery and publication formats are unchanged.
- An open refused for ownership opens, creates and writes no store artifact.
- Closed compaction's authority transitions are unchanged. The claim deletes its inner lock file
  before any manifest is published or the source is revalidated. The exact-content checks on the
  source, the canonical directory and `.previous` compare store artifacts. A lock file that a
  refused open creates in a generation is ownership state, so those checks skip it and cleanup
  deletes it (FR-9).
- The fault-checkpoint suite runs across every cut.

**III.** The spec lists the behaviour changes and the migration: the first upgrade, and the
unwritable store directory. It declares the lock files a contract. There are no signature or type
changes. `rust-version = "1.91"` is declared, and a CI job checks every target on it.

**IV.**
- **Lock ordering.** The registry mutex is taken, the entry is marked `Pending`, and the mutex is
  released. The locks are then taken with `try_lock`, which never blocks. Finally the mutex is taken
  again and the entry installed.
- **Waiting.** A second thread opening the same directory waits by polling until the entry is
  installed. Nothing waits under the mutex.
- **Release.** The entry is removed and each lock explicitly unlocked, under the mutex. The file is
  closed after the mutex is released.
- **What can block.** Only opens of the same directory, while its lock files are being opened. A
  progress test stalls one directory's lock-file open, and requires another directory's open and a
  third's drop to complete.
- **Post-recovery.** The inner lock taken after recovery (FR-5) is set once per entry, through an
  `Acquiring` marker. Its lock-file I/O also runs outside the mutex, and has its own progress test.
  Every other open of the directory waits on that marker before it goes live, and takes the lock
  itself if the attempt failed. A guard clears the marker if the attempt unwinds.
- **No entry without a lock.** An open that finds no directory asks again whether maintenance is
  in progress, because a claim may have moved the directory aside in between. If none is, the
  open fails at once (FR-8). An entry that held no lock could neither recover maintenance nor go
  live safely. The first version of this plan let such an entry proceed, and a re-review measured
  it recovering, and wedging, another process's live compaction.
- **Unwinding.** A panic while an entry is `Pending` removes the entry, through a guard that is
  disarmed once creation returns. Otherwise every later open of that directory would wait forever.

**V.** The acceptance tests observe public results, errors, and file bytes and permissions. Private
seams are `#[cfg(test)]` only: a stall hook, an injected lock error, an injected error from the
read-write open, a one-shot panic, and a maintenance pause. Each
schedules an interleaving that is otherwise unobservable. No dependency and no API is added.

## Decisions

- **Mechanism.** `std::fs::File::try_lock`. It needs no `unsafe` code and no dependency. What it
  does is decided per target by std and changes between toolchains (the spec's Platform coverage):
  - `flock(LOCK_EX|LOCK_NB)` on Linux, Apple platforms, FreeBSD, NetBSD, OpenBSD and Fuchsia;
  - `LockFileEx` over the whole file on Windows;
  - `fcntl` on Solaris;
  - no lock at all on some other targets.
- **Why two lock files.** They cover different ground:
  - **The inner lock** is inside the directory, so it is shared by every view of it: bind mounts,
    container volumes, symlinks. It needs no write access outside the store.
  - **Its gap:** the directory is replaced by rename during closed compaction and its recovery, and
    an inner lock does not survive that.
  - **The replacement lock** is keyed by the parent entry's name, so it is stable across that swap.
    It is taken only when the directory is being replaced, or checked (never created) otherwise. So
    ordinary opens create nothing outside the store directory.
- **Rejected alternatives:**
  - **Replacement lock only (spec 010):** measured to exclude nothing for a store directory that is
    a mount point, and to refuse every open under a read-only parent.
  - **A lock on the directory's own descriptor:** unavailable on Windows, and lost on the swap.
  - **Locking the WAL files:** each rotation and compaction replaces them.
- **Retiring the inner lock during compaction.** The claim deletes its own lock file before the
  manifest, so `.previous` normally holds exactly the captured artifacts.
  - This plan first called that sufficient. It argued that from staging onward, every other
    same-parent open finds maintenance artifacts and is refused by the replacement lock before it
    touches the inner file.
  - That misses an open that checked before staging began, and reaches its inner lock after the
    retirement. Such an open creates a new file in the source. The review measured it with the
    stall seam: the compaction failed revalidation and left the directory undetermined.
  - The same exact checks also broke on the canonical directory. An open whose cleanup stays
    pending takes the inner lock there, and the next recovery then refused the directory.
  - So the lock file is ownership state wherever a generation is compared exactly, not only in
    inspection (FR-9).
- **Explicit unlock at release (FR-12).** Measured: closing without unlocking left the lock held by
  children in the middle of being spawned. Immediate reopens were refused 343 of 400 times in the
  spec 010 prototype, and 312 of 400 times on this branch. With the unlock, none were refused.
- **Refusal.** An `io::Error` of kind `WouldBlock`, which callers already map to
  `RecoveryError::Io` or `FailedClosed`. No new error variant is added.
- **A lock path that is not a regular file** (a symlink, a directory or a FIFO): a required lock is
  refused, and an optional check skips it. The pid write would otherwise overwrite a symlink's
  target. The file is checked before it is opened; the race that leaves is documented.

## Performance

The locks cost a few `open`, `flock` and small writes per directory, per process, on first open.
There is nothing on read or mutation paths, so no benchmark gate applies.

## Persisted data

No format changes. The new files hold a diagnostic process id and are never read for authority.

## Constitution amendment (MINOR, 1.1.0 -> 1.2.0)

Replace the Project Constraint "The existing single-process-per-store-directory ownership model
remains the default until an approved specification defines cross-process coordination" with:

> Every file-backed open of a store directory, and every closed-maintenance claim, MUST take the
> lock files specs/011 defines and MUST refuse while another process holds them. Their names,
> locations and lock semantics are a compatibility contract, and changing them requires a
> specification with a migration. The exclusion this gives is bounded by what specs/011 states in
> its Compatibility section (Known limitations, the first upgrade, and what is out of scope) and
> in FR-11, among them: targets and filesystems where the standard library takes no lock, network
> filesystems, closed maintenance while another mount view of the directory exists, a process
> that forks without exec, the first upgrade from a version that takes no lock, and the
> destination of `pigment-db-migrate`. Within those limits, single-process ownership is enforced,
> not a convention.

This makes enforcement mandatory rather than conventional, which is materially expanded mandatory
guidance, so the amendment is MINOR. The first wording stated the constraint descriptively, and
listed as exceptions only cases where a lock cannot be taken. It left out cases where the locks are
taken but exclusion still fails, which the spec itself lists. The review corrected both.
