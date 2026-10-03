# Pigment DB maintenance review: storage inspection under concurrent rotation

- Reviewed: 2026-10-02
- Branch: `main`
- Commit: `9ba8871c6d303a8d94bcb9cc4443fdec990e16ec`, with specs/014's uncommitted implementation
  (tracked storage stats) in the tree
- Scope: the two defects specs/014 found and deferred (its plan.md, Decisions, "Not done here").
  Neither is introduced by specs/014, and specs/014's method does not depend on either.

## Principle VI

Both findings are stated in the library's terms: storage stats, the WAL, segments, rotation,
online compaction, cleanup. They were found while implementing specs/014, whose motivating
consumer is named in that spec's Motivation note; nothing here depends on it.

## Summary

Both defects have one cause: storage inspection lists the store directory and then reads the files
it listed, with nothing excluding a rotation of the same store between the two. A rotation renames
the active segment to a sealed name and a staged segment to the active name, so an inspection that
overlaps one sees a chain that is not a chain.

| # | Severity | Defect |
|---|---|---|
| 1 | Medium | `storage_stats()` fails spuriously while the store rotates |
| 2 | Medium | `try_compact_online` reports `AuthorityUndetermined` after a successful publication when the store rotates during its cleanup |

## Measurement

A scratch program (not committed) against the tree above, release build, on a Linux x86-64
machine shared with other work. Each trial opens a fresh key/value store in a fresh directory with
the stated segment target and Buffered durability, seeds it, and starts writer threads that each
make a fixed number of 64-byte puts.

## 1. `storage_stats()` fails spuriously while the store rotates

Location: `src/maintenance.rs` (`public_file_family_storage_stats`),
`src/compaction/inspection.rs` (`inspect_open_family`, `validate_current_chain`).

`storage_stats()` documents "exact storage usage for this open generation" and takes no lock: it
lists the directory, then reads every listed segment and replays the chain. A rotation between the
listing and the reads makes the listed active file a new segment whose base does not continue the
listed chain (`InvalidArtifact`), or removes a listed name (`Io { operation: Inspect, NotFound }`).

A reading in a loop for as long as the writers run, 20 trials per row:

| Segment target | Writers | Readings | `InvalidArtifact` | `Io(Inspect, NotFound)` | `Ok` |
|---|---|---:|---:|---:|---:|
| 256 B (every write rotates) | 1 × 500 puts | 1,328 | 1,150 | 130 | 48 (3.6%) |
| 256 B | 4 × 500 puts | 2,611 | 2,375 | 197 | 39 (1.5%) |
| 4 KiB (a rotation every ~50 writes) | 4 × 2,000 puts | 1,924 | 1,260 | 102 | 562 (29%) |

Every reading taken after the writers stopped succeeded (0 of 60 failed), so the store is intact:
the failures are the inspection's, not the data's. A caller that reads `storage_stats()`
periodically on a busy store therefore sees `InvalidArtifact`, which names a real WAL file as
invalid, for most readings.

Fix direction: make the inspection of an open family consistent with the writer, for example by
retrying the inspection when the writer's rotation count (specs/014's tracked figures) changed
while it ran, or by taking the WAL state lock's read side only across the listing and the
`open` of each listed file and reading through the open handles. Either needs its own
specification: Principle III keeps `storage_stats()`'s validation, and Principle IV asks what may
block.

## 2. Online compaction's cleanup reports `AuthorityUndetermined` under concurrent rotation

Location: `src/compaction/recovery.rs` (`recover_online_cleanup_with_checkpoint`,
`online_replacement_prefix_matches`), reached from `try_compact_online` after the replacement is
published.

Once the replacement is published and the writer reinstalled, cleanup confirms that the published
generation still begins with the replacement prefix: `online_replacement_prefix_matches` runs
`inspect_open_family` and then hashes the listed segments. Cleanup runs after the maintenance
coordinator's exclusive section ends (`complete_online_cutover`, `src/compaction/mod.rs`), so
writers are admitted again by then, and a rotation in between fails the check, and cleanup returns `AuthorityUndetermined` although the
publication succeeded. The manifest and the previous generation's directory are left behind.

One `try_compact_online` per trial, run while the writers write:

| Segment target | Writers | Trials | `Ok(Complete)` | `AuthorityUndetermined` |
|---|---|---:|---:|---:|
| 256 B | 4 × 300 puts | 100 | 50 | 50 |
| 256 B (second run) | 4 × 300 puts | 100 | 55 | 45 |
| 4 KiB | 4 × 1,000 puts | 100 | 33 | 67 |
| 4 KiB (second run) | 4 × 1,000 puts | 100 | 29 | 71 |
| 256 B, no writers | — | 20 | 20 | 0 |

After each `AuthorityUndetermined` (second runs): the directory held `kv.wal.dat.pigment-compact.manifest`
and `kv.wal.dat.pigment-compact.previous` besides the WAL and its lock file; the store accepted a
write; `storage_stats()` succeeded once the writers had stopped; a second `try_compact_online`
with no writers returned `Ok(Complete)` in all 116 cases; and the directory reopened. So no data is
lost, but a caller that compacts a busy store is told, about half the time or more, that
"available evidence cannot prove one authoritative generation" (the variant's documentation), after
a compaction that took effect.

Fix direction: confirm the prefix while writers are still excluded (before the writer is
reinstalled), or confirm it against the writer's tracked chain rather than a fresh listing, and
treat a later mismatch caused only by rotation past the prefix as `Pending` rather than
`AuthorityUndetermined`. This changes a recovery decision, so it needs its own specification and
the recovery fault matrix (Principle II).

## Deferred: two drafted specifications (2026-10-03)

Found by the same investigation as specs/014 and specs/015, drafted, and deferred by the maintainer
on 2026-10-03: once the motivating consumer compacts its stores online, neither blocks it. Each is
recorded here with its evidence so it can be approved and numbered later. Neither has been reviewed.

### Draft A: directory maintenance through any path spelling

#### Directory maintenance through any spelling

**Motivation (provenance):** found while measuring penpack finding V415. penpack opens its stores
through a one-component relative path (`-d db`). A closed compaction given such a path (`"db"`)
fails validation every time, with `InvalidArtifact` naming the staging copy of the key/value WAL,
and open-time recovery of any closed-compaction manifest through such a path cannot verify anything
and returns `AuthorityUndetermined`, even in the phases it recovers through `"./db"`.

The cause: for a one-component relative path, `Path::parent()` is the empty path, not `"."`.
Directory-level maintenance (closed compaction, `inspect_storage`, and manifest-driven recovery at
open) places and verifies its sibling artifacts beside that parent, and verification canonicalizes
it, which fails. Spec 011's lock layer and every open already read the directory through one rule
(`maintenance_path_for`, which reads paths lexically and switches to the directory's identity where
the spelling does not name it). Closed compaction and inspection never adopted that rule, so they
also mishandle `"db/."` and an absolute path ending in `"/."` (the rename after `Prepared` fails with
`EBUSY`), refuse `"."`, and compact a symlink's own name instead of the directory it names.

##### User scenarios and requirements

- **FR-1 (P1): one path rule for directory-level maintenance.**
  - `compact_directory_in_place` and `inspect_storage` derive every artifact location from the same
    maintenance path an open derives, for every spelling an open accepts: a one-component relative
    path, a trailing separator, a trailing `"."`, `"./"` and `"../"` forms, and absolute paths.
  - Recovery of a closed-compaction manifest at open succeeds through every such spelling in every
    phase it succeeds through an absolute path.
- **FR-2: no empty anchor.**
  - Wherever maintenance verifies or synchronizes a parent directory, an empty parent means the
    current directory. No future caller can reintroduce the empty-anchor failure.
- **FR-3: the current directory and symlinks.**
  - A spelling whose directory is the process's current directory (`"."`) is refused with an error
    naming it, before any file is created.
  - A spelling through a symlink compacts and inspects the directory the symlink names, beside that
    directory's replacement lock, and leaves the symlink in place. Inspection and compaction through
    the symlink see the directory's own maintenance evidence.
- **FR-4: errors keep naming what the caller gave.**
  - Error paths name the caller's spelling where today they do, as spec 011 requires; only the
    location of artifacts and the identity used for verification change.
- **FR-5: compatibility.**
  - Manifests record leaf names only, so no manifest or persisted format changes, and binaries on
    either side of this change recover each other's manifests for every lexical spelling.
  - A symlink spelling's artifacts move from beside the link to beside the directory. Debris an
    older version left beside a link (`.<link>.pigment-compact.*`) is no longer seen by closed
    compaction, as opens already do not see it; this is stated in the rustdoc and release notes.
  - Spec 011's Known limitation about closed maintenance through a symlink is retired.

##### Acceptance

- In a child process whose current directory is the store's parent, `compact_directory_in_place`
  succeeds through `"store"`, `"store/"` and `"store/."`, under both durability policies, leaving no
  maintenance artifact, and the store reopens with its state.
- Through an absolute path ending in `"/."`, compaction succeeds and leaves no manifest or staging.
- After a closed compaction is interrupted at `Prepared`, `PreviousPublished`,
  `ReplacementPublished` and `CleanupPending`, an open through `"store"` in a child process recovers
  (`Recovered`), with the expected state, and a later write survives a reopen through the absolute
  path.
- `"."` is refused by compaction and inspection with no file created.
- Through a symlink (same directory and another directory, Unix): compaction succeeds with cleanup
  complete, the link is still a link, the directory is compacted in place, no artifact is named for
  the link, and a write through either spelling reads back through the other after reopening. With
  stranded staging beside the directory, inspection and compaction through the link return the
  directory's error and change nothing.
- Full suites and builds pass on every operating system CI runs, including Windows' verbatim and
  short-name spellings.

### Draft B: replay and inspection memory

#### Replay and inspection memory

**Motivation (provenance):** penpack finding V415. A key/value WAL of 2.67 GB holding 27 MB of live
data (two 1 GiB sealed segments and a 0.49 GiB active segment) cost, measured:
- opening the store: 13 s and a 3.7 GB peak;
- `inspect_storage` and `storage_stats()`: a 3.2 GB peak;
- online compaction: a 3.3 GB peak above the store's resident size, and writers stalled for
  seconds;
- closed compaction: a 5.8 GB peak;
- opening the key/set or key/sorted-map store of the same directory, whose WALs are 10 and 17 MB:
  about 7 s each.

The open's peak alone was within 0.2 GB of the machine's memory. A crash that tears the last record
is worse: tail repair holds about twice the chain plus four copies of the active segment, which for
that store projects to about 6.9 GiB, so the store could not have been reopened on that machine.

The causes, all read at `9ba8871`:
- Every reader loads the whole chain into one buffer, because the codecs take one slice, and several
  keep the buffers the chain was built from as well.
- Replay indexes every record before applying any, and keeps a set of every key ever written to
  compute a flag that is usually already false.
- Validation-only readers build the full logical state and drop it.
- Every open of any family first inspects the whole directory, every family's chain, and discards
  the result unless closed-compaction evidence exists (`classify_untrusted_closed_authority`
  computes the canonical directory's evidence before checking whether any sibling evidence exists).
- Online compaction reads and replays the whole WAL twice at capture and twice at cutover, all while
  writers are excluded; closed compaction makes four full passes.

##### User scenarios and requirements

- **FR-1 (P1): an open reads only its own family, and only when it must.**
  - Opening a family reads no other family's WAL when the directory holds no maintenance evidence.
- **FR-2 (P1): peak memory follows records, not WAL size.**
  - Opening a store, recovering a torn tail, `inspect_storage`, `storage_stats()` and online
    compaction hold, beyond the store's logical state, at most the largest record group in the chain
    plus a fixed buffer. A record's declared length is checked against the bytes remaining in its
    artifact before anything is allocated for it.
  - Replay keeps no per-key history beyond what its result needs.
- **FR-3: fewer passes while writers wait.**
  - Online compaction reads the source chain once at capture and once at cutover.
- **FR-4: closed compaction.**
  - Closed compaction keeps spec 008's exact-byte source recheck (FR-039) and therefore still holds
    the source bytes; it drops its redundant passes. Bounding it further needs a change to that
    requirement and is recorded, not done here.
- **FR-5: outcomes do not change.**
  - Every classification, offset, recovery status, authority decision, error and repaired byte is
    exactly what it is today. No persisted format and no public signature changes. Frozen fixtures
    remain inputs.

##### Acceptance

- Opening a key/set store beside a large key/value chain reads none of the key/value chain
  (structural count), and its contents and recovery status are unchanged.
- Release-only, ignored-by-default gates in a child process, by `VmHWM` delta, over the same live
  state written with 64 MiB and 512 MiB of history:
  - open, torn-tail open, `storage_stats()` and `inspect_storage`: the 512 MiB delta is at most
    1.10 times the 64 MiB delta;
  - an open after 1M historical keys (churned to 1k live) peaks at most 1.10 times the peak after
    1k.
  Each gate was observed failing at `9ba8871` before the change.
- Online compaction takes one full source pass at capture and one at cutover while writers are
  excluded (structural count).
- Every existing recovery, truncated-WAL, segment, inspection and compaction suite passes
  unchanged, and spec 004's startup timing gate still holds.
- Full suites and builds pass on every operating system CI runs.
