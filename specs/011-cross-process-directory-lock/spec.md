# Cross-process ownership of store directories
Status: approved for implementation, 2026-09-25.

## Motivation (provenance)

A penpack deployment was restarted while its previous process was still alive: that process's
HTTP shutdown never completed, but its listener had already closed. The replacement opened the same
store directory. For about twelve hours both processes appended to
`kv.wal.dat`. Each V2 record carries its writer's own view of the file length (the physical-start
and mutation-start fields), so the next open refused the WAL as `InvalidArtifact`.

Every record was intact, and recovery meant rewriting the position fields of 18,979 records by
hand. The library's open lease (`maintenance_coordination.rs`) excludes owners only within one
process, and the Project Constraints record single-process ownership as a convention, "the default
until an approved specification defines cross-process coordination". This is that specification.

penpack's container image keeps its store at a volume path, `/opt/db`, so the lock must also hold
when the store directory is a mount point.

## Numbering

This is spec 011. Earlier work used the number 010 three times:
- `specs/010`, the refused bounded key-set snapshot change, is cited under that number in the
  constitution's Sync Impact Report. It never reached `main`.
- The `010-cross-process-directory-lock` branch was this feature's first prototype. It used a lock
  beside the directory only, and the verification record calls it "the spec 010 prototype". It
  never reached `main`.
- The branch `codex/010-fix-i128-key` reached `main` as `specs/007-fix-i128-key`.

## Requirements

- **FR-1 One process owns a directory.** A file-backed store directory is owned by at most one live
  process.
  - A process takes the directory's locks the first time it opens the directory (any family) or
    claims it for closed maintenance.
  - Further opens of the same directory in the same process share those locks.
  - Every open that takes locks holds at least one of them before it recovers anything or goes
    live.
  - The locks are released when the process's last owner of the directory is dropped. The operating
    system releases them when the process exits for any reason, including a kill that runs no
    destructor.
- **FR-2 Two lock files.**
  - **The inner lock, `<store directory>/.pigment-lock`**, is the steady-state lock. It lives inside
    the directory, so every view of the directory reaches the same file: symlink aliases, bind
    mounts, and a container volume mounted at the store path.
    - Every open and every closed-maintenance claim takes it whenever the directory exists and no
      directory-level maintenance is in progress.
  - **The replacement lock, `<canonical parent>/.<directory name>.pigment-lock`**, guards the phases
    in which the directory itself is replaced: closed compaction, and recovery from an interrupted
    one.
    - A closed-maintenance claim takes it, creating it if absent.
    - So does an open that finds directory-level maintenance artifacts for the directory.
    - Every other open checks it only if it already exists, and never creates it.
  - **Identity.** A directory is known by its canonical path, which also locates both lock files.
    - A symlink alias whose target does not exist, because a compaction has moved the directory
      aside, is followed by hand, up to 40 links. So it still names the directory it points to.
    - Every path is read lexically first (a trailing separator or `.` removed) before it is
      asked whether it is a symlink. The system follows the link for `alias/` or `alias/.`, and
      would hide it.
    - Recovery at open reads the canonical directory's maintenance artifacts whenever the path's
      last component is not the directory's own name. That covers a symlink however spelled, a
      path ending in `..`, and on Windows a name the system maps to another (`store.`, an 8.3
      short name). For any other path it uses the path given, read lexically, so errors name it.
  - **A held inner lock must still be the directory's.** An open stalled after taking the inner
    lock can end up holding the lock file of a directory that a compaction then retired. Before
    an open or a claim goes on, a held inner lock is checked against the file now at the lock
    path, by device and inode, and re-taken if they differ. The check runs outside the registry's
    mutex. Two threads that both find it stale compare holdings, not inodes, because the retired
    file's inode can be given to the new one. A lock path that cannot be read for a reason other
    than absence keeps the held lock. Windows std exposes no stable file identity, so there a
    held lock is kept (Known limitations).
- **FR-3 Refusal.**
  - An open of a directory another live process owns fails before any store artifact is opened,
    created or written. This covers `try_init_new`, `try_init_new_with_options` and `init_new` of
    every family.
  - The error is `RecoveryError::Io { operation: Inspect, path: <store dir>, source }`, with
    `source.kind() == ErrorKind::WouldBlock`.
  - The message names the lock file that is held. It adds "its last recorded owner is process N"
    when that record can be read.
  - `init_new` panics with that message.
  - Closed maintenance of such a directory fails with `CompactionError::FailedClosed` naming the
    lock file, and changes nothing.
- **FR-4 Closed compaction.**
  - The claim holds both locks. The replacement lock excludes every other process until the claim
    ends.
  - After validating its staging directory, and before revalidating the source or publishing any
    manifest, the compactor unlocks and deletes the inner lock file.
  - An open in another process can still reach the directory afterwards: one that checked for
    maintenance before staging began, and reaches its inner lock only after that deletion.
    - Where the directory is present, the open creates a lock file there and is refused by the
      replacement lock. The file it leaves is not a store artifact (FR-9).
    - While the directory is moved aside, the open finds no directory. It asks again whether
      maintenance is in progress, finds that it is, and is refused by the replacement lock (FR-8).
    - An open that has already reached its lock-file open when the directory is moved aside fails
      with `NotFound` naming the lock file, and creates nothing.
  - The next open creates an inner lock file in the replacement directory.
- **FR-5 Recovery at open.** When directory-level maintenance artifacts exist, an open does the
  following:
  1. It takes the replacement lock, not the inner one.
  2. It runs recovery.
  3. It takes the inner lock of the directory that recovery leaves in place. If another process
     already holds that lock, the open is refused.
  - Other opens of the same directory in the same process wait for that attempt before they go
    live. If it failed, each takes the lock itself, and is refused in the same way.
- **FR-6 Lock-file contents.**
  - After locking through a writable descriptor, the holder replaces the file's contents with its
    process id in ASCII decimal followed by a newline.
  - The contents are diagnostics only and never decide ownership. A reader reads at most 64 bytes
    and ignores contents it cannot parse.
  - The record can be stale: a holder that locked read-only cannot rewrite it, and process ids are
    relative to a pid namespace. So it is reported as the *last recorded* owner.
- **FR-7 Permissions.**
  - **The read-write open fails with `PermissionDenied` or `ReadOnlyFilesystem`:** the file is
    opened read-only and locked, and nothing is recorded.
  - **An inner lock, or a required replacement lock, cannot be opened at all:** the open is
    refused, naming the file. This covers an absent file that cannot be created, and an unreadable
    file.
  - **The file consulted by the replacement check cannot be opened:** the check is skipped,
    unless directory-level maintenance is in progress once the inner lock is held. A claim
    retires its inner lock after staging, so from then on the replacement lock is what excludes
    the open, and it is then required.
- **FR-8 A missing directory.**
  - When the store directory does not exist and no directory-level maintenance artifacts exist,
    the open fails at once and creates nothing. The error is
    `RecoveryError::Io { operation: Inspect, path: <store dir>, source }`, with
    `source.kind() == ErrorKind::NotFound`.
  - A closed compaction moves the directory aside while its claim holds the replacement lock. So an
    open that finds no directory asks again whether maintenance is in progress. If it is, the open
    takes the maintenance path of FR-5, and a live claim refuses it.
  - An open that finds the directory changing on each of three attempts is refused with
    `WouldBlock`.
- **FR-9 The inner lock file is not a store artifact.**
  - A regular file named `.pigment-lock` in a store generation belongs to no family, and its bytes
    count toward no total.
  - These checks ignore it: `inspect_storage`, closed compaction's revalidation of its source, and
    every recovery check that compares a generation's exact inventory.
  - The cleanup that deletes a replaced generation deletes the file with it.
  - Anything else of that name, such as a directory, is foreign.
- **FR-10 In-process behaviour is unchanged.** One process opens all three families of a directory
  under one set of locks, and a closed claim still excludes open leases in the same process.
- **FR-11 Locking support.**
  - Where std reports a lock as `ErrorKind::Unsupported`, that lock is skipped and a warning is
    logged. std reports it for these:
    - targets whose std takes no lock (see Platform coverage);
    - `ENOSYS` and `EOPNOTSUPP` on Unix;
    - `ERROR_CALL_NOT_IMPLEMENTED` on Windows.
  - Any other lock error refuses the open. That includes the codes other filesystems use to
    decline a lock, which std does not classify as unsupported: `ENOTSUP` on Apple platforms,
    `ENOLCK`, `ERROR_NOT_SUPPORTED` and `ERROR_INVALID_FUNCTION`.
- **FR-12 Release.** A lock is released by an explicit unlock, then the file is closed. A child
  process spawned by any thread keeps a copy of the descriptor until it execs, and closing alone
  would leave the lock held for that long.
- **FR-13 Progress.**
  - Lock-file I/O runs outside the process-wide ownership registry's mutex. A stalled lock-file
    open for one directory does not block opens, claims or drops of other directories.
  - Unlocking at release stays under the mutex, so the same process's next open of that directory
    cannot see its own stale lock.
  - A panic while a directory's locks are being taken leaves no entry behind, so the process's
    next open of that directory proceeds.

## Compatibility

- **What does not change:** WAL, snapshot and manifest formats, public signatures and types, and
  dependencies (none added).
- **The contract:** the lock files' names, locations and lock semantics are a compatibility
  contract. The semantics are whatever std's `File::try_lock` takes on each target (see Platform
  coverage): on the targets CI covers, a whole-file `flock` or `LockFileEx`. Changing them needs a specification
  with a migration, because processes that disagree exclude nothing. Lock-file contents and message
  text are not contract.
- **What is new:**
  - The inner lock file appears in every opened store directory.
  - The replacement lock file appears beside a directory once closed compaction or recovery has run.
  - `rust-version = "1.91"` is declared. `File::try_lock` needs 1.89, and an existing
    `PathBuf == String` comparison needs 1.91.
- **Intended behaviour changes:**
  1. A second process's open of an owned directory is refused.
  2. Cross-process closed maintenance of an owned directory is refused.
  3. A store directory that is not writable and has no `.pigment-lock` is refused. Creating the file
     once, readable, restores it.
  4. An open or claim refused for any other reason, such as `MigrationRequired` or
     `InvalidArtifact`, leaves behind the lock files it took:
     - the inner one, for an open;
     - both, for a claim;
     - for an open or claim in a maintenance state, the replacement one alone when recovery
       itself refuses, and both when recovery completed before the refusal.
  5. An open of a directory that does not exist now fails before it takes a lock or touches any
     artifact, with `Inspect` naming the directory (FR-8). It used to fail creating the WAL's
     staging file (`CreateStaging`, naming `<dir>/.kv.wal.dat.next` for the key/value family).
- **The first upgrade:** protection begins only when every process using a directory runs this
  version. An older process takes no lock, so drain it before the first restart onto this version.
- **Windows:**
  - While `.pigment-lock` is held, no handle can read it except the one that locked it. That
    includes the holder's own other handles.
    - A hot copy or backup of a live store directory fails on that file.
    - A refused open cannot read the owner record, so its refusal names only the lock file.
  - A terminated owner's locks are released asynchronously. An immediate restart can therefore be
    refused with `WouldBlock`. The library does not wait, so a supervisor should retry the open.
- **Known limitations:**
  - A process that forks without exec shares its parent's locks.
  - Closed compaction while another mount view of the directory exists is unsupported: a
    bind-mounted view keeps the old directory, and a mount point cannot be renamed.
  - Network filesystems are not verified. On NFS an exclusive lock may need a writable descriptor.
  - On Solaris, std takes `fcntl` record locks, which belong to the process rather than to a
    descriptor. Cross-process ownership is not supported there. This is inferred from std's
    source and was not run.
    - The read-only path of FR-7 is unavailable, because `F_WRLCK` needs a descriptor open for
      writing.
    - Closing any descriptor of a lock file drops the process's lock on it. So FR-12's close
      outside the mutex can drop a lock that the same process's next owner of the directory has
      just taken.
  - Which targets lock depends on the toolchain the crate is built with (see Platform coverage).
    Two builds of one revision made with different toolchains may not exclude each other on
    illumos, AIX or GNU/Hurd, where one build locks and the other skips.
  - Closed compaction through a symlinked store path replaces the symlink rather than the directory.
    That defect predates this spec and is tracked separately.
  - A path that reaches the directory through its inode rather than its name, such as
    `/proc/self/fd/N` or a working directory inside it, follows the directory when a compaction
    moves it aside. An open through such a path during a compaction is not excluded, and is
    unsupported.
  - A store's files are opened through the path given, while its ownership is keyed by the
    directory that path named when it was opened. So repointing a symlink on the store path
    while a store is open, or while it is being opened, is unsupported: the open store would
    write into a directory it does not own. This predates this spec.
  - On Windows a held inner lock is not checked against the file at the lock path (FR-2), since
    std exposes no stable file identity there. An open stalled across a whole compaction can
    therefore keep the retired directory's lock, and a view of the directory created afterwards
    is not excluded by it. Opens through the same parent are still excluded by the replacement
    lock. It is not known whether Windows lets a compaction rename a directory holding an open
    lock file.
- **Out of scope:**
  - locking the destination of `pigment-db-migrate`;
  - waiting for a lock;
  - repairing a WAL that two processes wrote;
  - the pre-existing defect recorded under "Found during review".

## Platform coverage

std decides what `File::try_lock` does on each target. Read from its source at 1.91.0, the declared
minimum, and at 1.97.1, the toolchain this was verified with:

| Target | 1.91 | 1.97.1 |
|---|---|---|
| Linux, Apple platforms, FreeBSD, NetBSD, OpenBSD, Fuchsia, Cygwin | `flock(LOCK_EX \| LOCK_NB)` | the same |
| Windows | `LockFileEx`, exclusive and fail-immediately, over offset 0, length 2^64 − 1 | the same |
| Solaris | `fcntl(F_SETLK)` with `F_WRLCK`, whole file | the same |
| illumos, AIX, GNU/Hurd | no lock: `Unsupported`, skipped with a warning (FR-11) | `flock` |
| DragonFly BSD and every other Unix target | no lock: skipped with a warning | no lock: skipped with a warning |

Only Linux was run. CI covers macOS and Windows once the branch is pushed, and no run on either
is recorded here. The minimum-toolchain job checks Linux only.

## Found during review, not fixed here

**Two families of one process recovering at once can refuse one of them.** When the kv and key-set
opens of one process start together on an interrupted compaction, both run directory recovery.
- Measured once in 120 runs: one open failed with a raw `NotFound`, reported under `Inspect`.
- The next open was clean in every run. Other processes stay excluded throughout, because both
  threads share the entry's replacement lock.
- This predates this spec. It needs recovery serialized per registry entry, as FR-5 now does for
  the inner lock.

**A write after an open whose closed cleanup stays pending makes the next open fail.** This defect
is present at `af25792`.
- CleanupPending recovery re-verifies the canonical directory against the replacement inventory,
  byte for byte.
- An open whose cleanup stays Pending still goes live.
- Any accepted write breaks that match, and so does a second family's first open, which creates its
  WAL. The next open then fails with `AuthorityUndetermined`.

This spec's own lock file broke the same match with no write at all, and that part is fixed (FR-9).
The pre-existing part needs a specification of its own. One remedy: once a ReplacementPublished or
CleanupPending manifest is durable, verify only what cleanup deletes (`.previous` against the source
inventory). The canonical directory would then only have to inspect as a valid generation.

## Acceptance

Most of these tests re-execute their own test binary as a child process. X3, A7 and Symlink run
in one process. A8 spawns `true`, not the test binary. X2, A6, A7, A8 and Symlink are Unix-only.
All but R1 use only the public API. R1 parks its child with a private pause seam.

| Test | Scenario | Expected |
|---|---|---|
| A1 | The parent holds one family. A child opens every family, the first twice. | Each open is refused (`Inspect`, `WouldBlock`), and the message names the inner lock file. A recursive snapshot of the store's parent, lock files included, is unchanged. |
| A2 | A child holds the directory. The parent opens it. | Refused, naming the lock file, and on Unix the child's process id. |
| A3 | The parent holds the directory. A child runs closed compaction. | `FailedClosed`, naming the lock file, and nothing changes. |
| A4 (control) | One process opens all three families, then drops them. | A child then opens them, and the inner lock file still exists. |
| A5 (control) | A child holding the directory is killed and reaped. | The parent opens it. |
| A6 (Unix) | The store directory is not writable, and a read-only `.pigment-lock` exists. | The open succeeds and records nothing. A second process is refused. |
| A7 (Unix) | The store directory is not writable, and there is no lock file. | The open is refused (`PermissionDenied`), naming the lock file, and nothing is created. |
| A8 | Another thread spawns children continuously while this process drops and reopens a directory 400 times. | No reopen is refused. |
| X1 | Open two families, then drop one. | A child is still refused. After the other is dropped too, the child opens. |
| X2 | The parent holds the directory. A child opens it through a symlink alias. | Refused. |
| X3 | The lock file holds a foreign process id or unparsable bytes, and no one holds it. | The open succeeds. |
| R1 | A child parks mid closed-compaction, holding its claim, at three points. | The parent's open is refused by the replacement lock, and opens once the compaction finishes. |
| Symlink | `.pigment-lock` is a symlink. | The open is refused, and the target is untouched. |

These unit tests use private seams or fixtures:

| Test | Scenario | Expected |
|---|---|---|
| Cleanup left pending | An open recovers a compaction whose cleanup stays pending. Cleanup is then allowed to proceed. | The next open, a second family opened in the same process, and a compaction retry each finish cleanup. |
| Stalled opener | An open is stalled at its lock-file open. Meanwhile another process's compaction reaches StagingValidate, PreviousPublish or ReopenValidation. | The open is refused: `WouldBlock` naming the replacement lock, or `NotFound` while the directory is moved aside. The compaction completes and the directory reopens. |
| Opener stalled earlier | The same, stalled just after its maintenance check, with PreviousPublished/ManifestPublish added. | Refused by the replacement lock at every point, including those at which the directory is moved aside. |
| Inner lock in flight | A second family opens while the first is taking the inner lock after recovery, and another holder has that lock; or the first attempt panics. | The second family waits. It is refused while the other holder has the lock, and takes the lock itself after a panic. |
| A directory named like the lock file | It appears in the source after the claim retired its lock. | The compaction fails closed and publishes nothing. |
| Alias, moved aside (Unix) | An open through a symlink alias, spelled `alias`, `alias/`, `alias/.` or through a link whose target ends in `/`, while a compaction has moved the directory aside. | Refused by the claim's replacement lock. |
| Alias, recovery (Unix) | An open through the same spellings after a compaction was interrupted. | It recovers the directory it names, and its writes survive a later open of the real path. |
| Retired inner lock (Unix) | An open stalled after taking the inner lock while a compaction replaces the directory; the lock path left empty, or holding another file. | Before going live it holds the lock of the directory in place. |
| Replaced lock, later family (Unix) | The held lock file is replaced at its path, then another family opens. | The later family holds the current file's lock. |
| Retired lock, claim (Unix) | The same interleaving for a closed-maintenance claim. | After `ensure_inner_lock` the claim holds the current file's lock, and retires it. |
| Two stale verdicts (Unix) | Two families find the lock stale; one is parked while the other re-takes it. | The parked one keeps the re-taken lock. |
| Held-lock check stalled | The check for one directory is stalled. | Other directories open and drop (FR-13). |
| Replacement check unreadable | An open that cannot open the replacement lock file, stalled until a claim has published its manifest. | Refused; the claim completes. |
| A path ending in `.` | `store/.` over an interrupted compaction. | Recovered. |
| Lock file in `.previous` | A replaced generation holds `.pigment-lock`. | Both cleanups delete it. A directory of that name keeps cleanup pending. |
| Recovering owners | An open, or a claim paused while staging, recovers an interrupted compaction. | Each holds the inner lock and records its process id. |
| Progress | Lock-file I/O for one directory is stalled, at entry creation and again after recovery. | Other directories open and drop. |
| Lock errors | An injected `Unsupported`, another lock error, or `ReadOnlyFilesystem` from the read-write open. | Skipped, with a warning naming the lock file and the directory; refused; locked read-only with nothing recorded. |
| Panic | The lock-file open panics. | The next open of the directory proceeds. |

A mount-namespace run (`unshare -rm`), with the store directory bind-mounted as a volume under a
private parent, is recorded as verification evidence.

The existing suites pass. Tests that assert a namespace is unchanged exclude exactly the lock files
this spec creates, and assert that those files exist. `cargo fmt --check` is clean, and Clippy
reports no new diagnostics. These run on every CI operating system: `tests/directory_lock.rs`, the
library's `maintenance_coordination::` tests, and `compaction::recovery_tests::ownership`. A CI job
checks every target on the declared minimum toolchain.
