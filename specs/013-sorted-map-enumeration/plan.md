# Sorted-map enumeration: plan

## Technical context
Rust 2021 library crate, MSRV 1.91, `dashmap` 3.11. The key/sorted-map store keeps its data in one
`DashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>`. It has no batch gate. Mutations of a file-backed
store take the maintenance coordinator's shared side and then the shard's write guard; reads take
only the shard's read guard.

**Motivation (provenance):** penpack finding V414 (open). penpack's VFS tag values and tag-name
records in this store are charged to no budget. Its fix counts them into penpack's per-host data
budget, which is recounted when the stores open, and needs FR-1 to recount them.

## Constitution check
- **VI.** The API is stated in the library's own terms: keys and sorted maps. Any consumer can use
  it unchanged. The motivating consumer is penpack (finding V414). It is named in the Motivation
  notes, in this record and in the Release line below; no other prose uses its terms. No
  identifier, type, error, limit, test, branch or spec title carries it. The change lands on `main`
  before penpack pins it, and verification.md records the published revision.
- **V.** Assertions use public reads: the enumeration, `contains_key`, `size()`, `get_sorted_map`
  and a file-backed store's `storage_stats()`. No private seam is needed.
- **IV.**
  - The enumeration takes no new lock. It uses the map's own shard read guards, as 012's
    `for_each_set` does: `visit` always runs under exactly one, and the iterator keeps the previous
    shard's guard until it has taken the next, so at most two are held, the second only while
    `next()` waits.
  - This store has no batch gate, so no batch can stall behind the visit. The maintenance
    coordinator is not taken either, so a compaction's capture, which takes only shard read
    guards, proceeds during a visit. A compaction that queues behind a writer waiting on the
    guarded part makes every writer wait with it, which FR-1 states.
  - A shard's lock is a spin lock, so a writer waiting on the guarded part spins on its thread
    rather than sleeping, for as long as `visit` stays in that part. FR-1 states this, and the
    rustdoc tells callers to keep `visit` short and non-blocking.
  - The visitor rule is wider than "call no method of the store". A visitor waiting on another
    thread's write deadlocks once a compaction queues: the other thread's write parks behind the
    compaction, the compaction waits for a writer spinning on the guarded part, and that writer
    waits for the visit (measured by the review).
- **III.** One method is added, and nothing else changes signature, semantics or format.
- **II.** No write path changes.

## Decisions
- **Enumeration.** Iterate `self.store.iter()` and pass `(key, value)` borrowed from each guard,
  exactly as `for_each_set`. The callback runs under that shard's read guard, the same rule compute
  callbacks already carry.
- **Name.** `for_each_sorted_map`, after `get_sorted_map` and `sorted_map_size`.
- **Shape.** One call per key with its whole map, like `for_each_set`, not one call per entry. A
  caller that wants entries iterates the map; a caller that wants per-key facts (`len()`, whether it
  is empty) gets them without a second pass.
- **Empty maps.** No public write or open publishes an empty map. Removing a map's last entry writes
  a delete event and removes its key, and a compute that leaves its map empty publishes nothing.
  Legacy and V1 WAL input is refused at open with `MigrationRequired`, and the migrator rebuilds from
  the replayed snapshot, emitting only put frames, so it drops an empty key. The visit filters
  nothing all the same, so it agrees with `contains_key` and `size()` whatever the store holds.
- **Not done here.** No capacity rule, unlike 012's sets: a `BTreeMap` frees its nodes as entries
  are removed. The outer `DashMap` keeps its own peak capacity across keys, as 012 records for the
  other two stores.

## Release
The change lands on `main`, and penpack pins that tested revision.
