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
