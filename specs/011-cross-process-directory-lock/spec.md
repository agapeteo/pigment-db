# Cross-process ownership of store directories
Status: approved for implementation, 2026-09-25.

**Motivation (provenance):** a penpack deployment was restarted while its previous process was
still alive: that process's HTTP shutdown never completed, but its listener had already closed. The
replacement opened the same store directory. For about twelve hours both processes appended to
`kv.wal.dat`. Each V2 record carries its writer's own view of the file length (the physical-start
and mutation-start fields), so the next open refused the WAL as `InvalidArtifact`.

Every record was intact, and recovery meant rewriting the position fields of 18,979 records by
hand. The library's open lease (`maintenance_coordination.rs`) excludes owners only within one
process, and the Project Constraints record single-process ownership as a convention, "the default
until an approved specification defines cross-process coordination". This is that specification.

penpack's container image keeps its store at a volume path, `/opt/db`, so the lock must also hold
when the store directory is a mount point.

**Numbering.** This is spec 011. `specs/010` is the refused bounded key-set snapshot change, cited
under that number in the constitution's Sync Impact Report, and it never reached `main`.

## Requirements

- **FR-1 One process owns a directory.** A file-backed store directory is owned by at most one live
  process.
  - A process takes the directory's locks the first time it opens the directory (any family) or
    claims it for closed maintenance.
  - Further opens of the same directory in the same process share those locks.
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
    manifest, the compactor unlocks and deletes the inner lock file. The replaced directory then
    holds exactly the store files it captured.
  - The next open creates an inner lock file in the replacement directory.
- **FR-5 Recovery at open.** When directory-level maintenance artifacts exist, an open does the
  following:
  1. It takes the replacement lock, not the inner one.
  2. It runs recovery.
  3. It takes the inner lock of the directory that recovery leaves in place. If another process
     already holds that lock, the open is refused.
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
  - **The file consulted by the replacement check cannot be opened:** the check is skipped.
- **FR-8 A missing directory.** When the store directory does not exist and no directory-level
  maintenance artifacts exist, no lock is taken and nothing is created. The open fails as it does
  today.
- **FR-9 Inspection ignores the inner lock file.** `inspect_storage` and closed compaction ignore
  `.pigment-lock` inside a store directory, and its bytes count toward no family.
- **FR-10 In-process behaviour is unchanged.** One process opens all three families of a directory
  under one set of locks, and a closed claim still excludes open leases in the same process.
- **FR-11 Locking support.**
  - Where the standard library cannot lock (`ErrorKind::Unsupported`, from the platform or the
    filesystem), that lock is skipped and a warning is logged.
  - Any other lock error refuses the open.
- **FR-12 Release.** A lock is released by an explicit unlock, then the file is closed. A child
  process spawned by any thread keeps a copy of the descriptor until it execs, and closing alone
  would leave the lock held for that long.
- **FR-13 Progress.**
  - Lock-file I/O runs outside the process-wide ownership registry's mutex. A stalled lock-file
    open for one directory does not block opens, claims or drops of other directories.
  - Unlocking at release stays under the mutex, so the same process's next open of that directory
    cannot see its own stale lock.

## Compatibility

- **What does not change:** WAL, snapshot and manifest formats, public signatures and types, and
  dependencies (none added).
- **The contract:** the lock files' names, locations and lock semantics (whole-file `flock` on
  Unix, `LockFileEx` on Windows) are a compatibility contract. Changing them needs a specification
  with a migration, because processes that disagree exclude nothing. Lock-file contents and message
  text are not contract.
- **What is new:**
  - The inner lock file appears in every opened store directory.
  - The replacement lock file appears beside a directory once closed compaction or recovery has run.
  - `rust-version = "1.89"` is declared, for `File::try_lock`.
- **Intended behaviour changes:**
  1. A second process's open of an owned directory is refused.
  2. Cross-process closed maintenance of an owned directory is refused.
  3. A store directory that is not writable and has no `.pigment-lock` is refused. Creating the file
     once, readable, restores it.
  4. An open or claim refused for any other reason, such as `MigrationRequired` or
     `InvalidArtifact`, leaves the lock files it took behind: the inner one, or the replacement one
     for a claim or a maintenance-state open.
- **The first upgrade:** protection begins only when every process using a directory runs this
  version. An older process takes no lock, so drain it before the first restart onto this version.
- **Known limitations:**
  - A process that forks without exec shares its parent's locks.
  - Closed compaction while another mount view of the directory exists is unsupported: a
    bind-mounted view keeps the old directory, and a mount point cannot be renamed.
  - Network filesystems are not verified. On NFS an exclusive lock may need a writable descriptor.
  - On Solaris and illumos, std locks with `fcntl(F_WRLCK)`, so the read-only path of FR-7 is
    unavailable there.
  - Closed compaction through a symlinked store path replaces the symlink rather than the directory.
    That defect predates this spec and is tracked separately.
- **Out of scope:** locking the destination of `pigment-db-migrate`, waiting for a lock, and
  repairing a WAL that two processes wrote.

## Acceptance

These tests use only the public API. Each re-executes its own test binary as a child process.

| Test | Scenario | Expected |
|---|---|---|
| A1 | The parent holds one family. A child opens every family, the first twice. | Each open is refused (`Inspect`, `WouldBlock`), and the message names the inner lock file. A recursive snapshot of the store's parent, lock files included, is unchanged. |
| A2 (Unix) | A child holds the directory. The parent opens it. | Refused, naming the lock file and the child's process id. On Windows, the lock file only. |
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

The following are unit tests that use private seams:
- lock-file I/O stalled for one directory while other directories open and drop;
- an injected `Unsupported` lock result;
- `init_new`'s panic naming the lock file.

A mount-namespace run (`unshare -rm`), with the store directory bind-mounted as a volume under a
private parent, is recorded as verification evidence.

The existing suites pass. Tests that assert a namespace is unchanged exclude exactly the lock files
this spec creates, and assert that those files exist. `cargo fmt --check` is clean, and Clippy
reports no new diagnostics. `tests/directory_lock.rs` runs on every CI operating system.
