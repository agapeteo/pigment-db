# Entry enumeration and bounded set capacity: plan

## Technical context
Rust 2021 library crate, MSRV 1.91, `dashmap` 3.11. Both stores keep their data in one
`DashMap`. The key/value store orders batches against ordinary operations with a `transaction`
gate; the key/set store has none.

**Motivation (provenance):** penpack finding V263. penpack bounds how many bytes each key prefix
holds, recounts the prefixes when it opens a store, and charges each set member a fixed amount. It
needs FR-1/FR-2 to recount, and FR-3 so that the fixed charge bounds a set's table.

## Constitution check
- **VI.** The API and the capacity rule are stated in the library's own terms: entries, sets,
  capacity. Any consumer can use them unchanged. The motivating consumer is penpack (finding V263).
  It is named in the Motivation notes, in this record and in the release line below. No
  identifier, type, error, limit, test, branch or spec title carries it. The change lands on
  `main` before penpack pins it.
- **V.** Assertions use public reads: the enumeration, and `HashSet::capacity()` of the sets it
  visits. No private seam is needed.
- **IV.**
  - The enumeration takes no new lock. It uses the map's own shard read guards. `visit` always runs
    under exactly one, but the iterator keeps the previous shard's guard until it has taken the
    next, so at most two are held, and the second only while `next()` waits.
  - The `transaction` gate is deliberately not taken. Holding it for the whole visit would make
    every batch, and everything queued behind one on the fair lock, wait for the whole visit. Not
    taking it lets batches on other parts proceed, and a batch may then be observed in part, which
    FR-1 states.
  - The stall FR-1 states remains. A batch that writes to the guarded part, or one queued behind a
    writer waiting on it, holds the gate until the visit leaves that part.
  - The capacity release is O(1) to decide. A release that resizes rehashes the set's members
    while that shard's write guard is held, O(len). The hysteresis bounds how often that happens,
    and the cost is smaller than the growth rehash the same table already made on append. Baseline
    below.
- **III.** Two methods are added, and nothing else changes signature, semantics or format.
- **II.** No write path changes what is accepted, persisted or published. Only a set's spare
  capacity changes. It is released after WAL acceptance and before publication, so it never runs on
  an error path. For compute it is released on the private working copy. For removals it is released
  on the live set, under the same entry guard.

## Decisions
- **Enumeration.** Iterate `self.store.iter()` and pass `(key, value)` borrowed from each guard.
  - The callback runs under that shard's read guard, the same rule compute callbacks already
    carry.
  - The visit is not a snapshot. That is enough for a caller recounting before it serves, or
    sampling while it serves.
- **Capacity rule.** One `pub(crate)` helper in `key_set_store.rs`,
  `release_spare_capacity(&mut HashSet<Vec<u8>>)`, which calls `shrink_to(2 * len())`
  unconditionally.
  - `shrink_to` compares the bucket count that `2 * len` needs with the table's real bucket count.
    It resizes only when the real count is larger, and is O(1) otherwise.
  - Gating it on `capacity()` would be wrong. `capacity()` is `len + growth_left`, and a removal that
    leaves a tombstone does not return its slot to `growth_left`.
    - Measured at `d5ad6ee`: a set cut from 10,000 members to 10 reported capacity of 12,843 to
      13,228 across the recorded runs, in a table sized for 14,336. The figure varies with the hash
      seed; verification.md T004 has the per-path ranges.
    - A table filled before the cut leaves most removed slots as tombstones. There, a
      `capacity()`-gated trigger never fires while the real table reaches six to seven times the
      length (the 012 review measured this).
  - Right after the call, the power-of-two sizing gives a capacity below `4 * len()` for every
    `len >= 1`. Insertion keeps it at most `4 * len()`: a table with no growth left doubles once
    half its usable slots hold members, even on an append of an existing member.
  - Resizing is amortised:
    - after a release that resizes, the table has room for at least `len` more insertions;
    - removal one member at a time shrinks it again only when the length falls to about a quarter
      of its capacity;
    - a set released at an arbitrary length (by a compute, or at open) may sit just above a
      threshold and shrink on its next removal.
- **Where the rule is applied.** Every place a set that stays published loses members:
  - `try_remove_from_set_core`, on both of its non-final paths;
  - `try_remove_from_set_callback_core`, on its non-final path;
  - the working set of every compute variant (`try_compute`, `try_compute_async`,
    `try_compute_if_present`, `try_compute_if_absent`), before publication, on both the occupied
    and vacant paths. A callback may reserve capacity as well as remove members;
  - each set loaded at open (`for (key, values) in initialized.snapshot`). Replay builds sets in a
    private map, so one place covers every recovery path.
- **Not done here.** The outer `DashMap` keeps its own peak capacity across keys. It is bounded by
  the peak number of keys, it is not per-set state, and shrinking it needs every shard's write
  lock.

## Performance and release
Baseline at `d5ad6ee`, recorded before production edits. Release build, a standalone crate
(path dependency) outside the repository, five runs per workload:

- **W1, remove-heavy.** One set with 200,000 members is cut to one member by `remove_from_set`.
  - Medians of three sets of runs: 0.064270, 0.060173, 0.056211 s. Baseline 0.060173 s.
  - Gate: median <= 0.078225 s.
- **W2, churn.** 2,000 keys each get 64 appends then 48 removals.
  - Medians: 0.039138, 0.039924, 0.038418 s. Baseline 0.039138 s.
  - Gate: median <= 0.050879 s.

The candidate is measured the same way. The change lands on `main`, and penpack pins that tested
revision.
