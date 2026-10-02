# Tracked storage stats
Status: approved for implementation, 2026-10-02.

**Motivation (provenance):** penpack finding V415 (open). penpack never compacts its stores, so a
key/value WAL holding 27 MB of live data reached 2.67 GB, and opening it peaked at 3.7 GB of memory.
Its fix compacts a store online once the WAL has grown well past its size after the last
compaction, and so needs to read the WAL's size about once a minute. The only reading today,
`storage_stats()`, reads and replays every byte of the WAL: 6.6 s and a 3.25 GB peak on that store.

## User scenarios and requirements

- **FR-1 (P1): read an open store's WAL size from the writer's own accounting.**
  - `tracked_storage_stats(&self) -> Result<FamilyStorageStats, CompactionError>` on
    `DurableKeyValueStore<File>`, `DurableKeySetStore<File>` and `DurableKeyMapStore<File>`.
  - It reports the active segment's length, the sealed segments' total length and count, and their
    sum: the same five figures `storage_stats()` reports for the same generation.
  - It lists, stats, opens and reads no file, and allocates nothing beyond its result.
  - It never waits for an online compaction's capture or cutover. It waits at most for one
    in-progress write, rotation or rollback of the same store.
  - It may be called from a compute callback of the same store.
  - A reading never makes a write wait. An observation of the store must not delay the writes it
    observes, whatever thread takes it and however that thread is scheduled: a monitoring thread
    of low priority, preempted at the wrong moment, must not stall the writers it watches.
  - *Why a new coordination layer (Principle IV).* The only existing boundary that orders the
    figures is the WAL state's lock. Every write holds it for its whole I/O, including
    `sync_data` under Physical durability, and a rotation holds it across staging, two renames, a
    reopen and a directory sync. Where the platform's lock lets waiting writers go first, as
    Linux's does, a reading that takes it waits behind every write queued for it, for as long as
    writes keep arriving: one reading under four Physical writers took 286 s while 45,429 writes
    completed (verification.md, Measurements). The maintenance coordinator cannot serve either:
    online compaction holds it exclusively across whole-WAL passes, which this requirement forbids
    waiting for. So the figures are copied, as the WAL state lock's write side is released, into a
    small cell of their own that a reading consults without that lock. plan.md records what the
    cell is, who owns it, and the alternative it was weighed against.
- **FR-2: what the figures mean.**
  - The figures are those of the store's WAL at one instant during the call. Appends and rotations
    either happened before that instant or not at all; the call never observes half of a rotation.
  - While an online compaction is installing its replacement (the writer is detached), the figures
    are those of the generation being replaced.
  - Like `storage_stats()`, the figures exclude maintenance residue, staging, lock files and
    anything outside the authoritative generation.
  - They are not a validation. Damage to the files after the open is invisible to them;
    `storage_stats()` remains the validating reading.
- **FR-3: failing closed.**
  - When the store's WAL is failed closed, because a rejected write or rotation could not be
    rolled back, an online compaction's publication is indeterminate, or a panic unwound out of a
    write that held the WAL's lock, poisoning it, the call returns `CompactionError::FailedClosed`
    and no figures. In those states the figures need not match the files. (The third cause was
    added by the review: every later mutation of such a WAL panics on the poisoned lock, so it is
    failed closed in all but name. A panic that began before the write took the lock poisons
    nothing and is not this cause.)
- **FR-4: compatibility.**
  - `storage_stats()` and `inspect_storage` keep their signatures and semantics.
  - No persisted format changes. Memory stores gain no method (`compile_fail` doctests).

## Acceptance

- After at least three rotations in each family, `tracked_storage_stats()` equals
  `storage_stats()`, before and after a reopen.
- After `try_compact_online`, the two readings agree, the sealed count is 0 and the total equals the
  outcome's `after_bytes()`. They agree again after a further rotation.
- With the store directory made unreadable, `storage_stats()` fails and `tracked_storage_stats()`
  still returns the earlier figures (Unix only).
- Under concurrent writers that rotate, every reading succeeds, the totals and counts never
  decrease, active plus sealed equals the total, and each reading's sealed bytes equal the lengths of
  the sealed segment files it counts.
- A reading taken while an online compaction holds its capture returns at once with the figures
  from before the attempt. A reading taken while the writer is detached returns the replaced
  generation's figures.
- A failed-closed WAL returns `FailedClosed`, for each of the three causes.
- A reading inside a compute callback completes while another thread compacts the store online.
- Full suites and builds pass on every operating system CI runs.

## Amendments
- 2026-10-02, after the implementation review: FR-3's third cause (a panic that unwound out of a
  write holding the WAL's lock) and FR-1's "Why a new coordination layer" note were added, and the
  Acceptance line on failed-closed WALs was extended from two causes to all three.
- 2026-10-02, maintainer decision after the second review: FR-1 gains "A reading never makes a
  write wait". The review found that no requirement chose the sequence lock over a `Mutex`
  holding a copy of the figures, which also meets the bound on a reading's wait. This one does:
  under the `Mutex`, a reading preempted inside its critical section delays the write releasing
  the WAL's lock and every write queued behind it. The motivating consumer reads the figures from
  a background thread while request-path writes proceed.
