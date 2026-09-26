# Entry enumeration and bounded set capacity
Status: approved for implementation, 2026-09-26.

**Motivation (provenance):** penpack finding V263. penpack bounds how many bytes each key prefix
may hold in its key/value and key/set stores. It cannot learn what a store already holds when it
opens one, because no public API visits the entries. A set that grows and is then cut down by
removals also keeps its largest table, so a bound charged per live member does not bound the
memory a set holds. Measured at `d5ad6ee`: a set grown to 760,000 members and cut to one member
still held a 26,214,464-byte table.

## User scenarios and requirements

- **FR-1 (P1): enumerate key/value entries.**
  - `DurableKeyValueStore::for_each_entry(&self, visit: impl FnMut(&[u8], &[u8]))` calls `visit`
    once for each published key and its value.
  - The visit is not a snapshot. An entry that is present and unchanged for the whole call is
    visited exactly once. An entry inserted, replaced or removed during the call may be visited
    before or after that change, or not at all. This includes an entry written by a
    compare-exchange batch.
  - `visit` runs while part of the store is guarded, so it MUST NOT call any method of the same
    store. That may deadlock, as for compute callbacks.
  - A part stays guarded from its first entry until the visit moves on. Reads of any key, and
    writes to other parts, proceed. Writes to the guarded part wait until the visit leaves it.
  - The batch gate is not taken, so batches on other parts proceed. A compare-exchange batch that
    writes to the guarded part, or one queued behind a writer waiting on it, holds the gate while it
    waits. Every read and write of the store then waits until the visit leaves the part.
  - Nothing is copied, and nothing is written to the WAL.
- **FR-2 (P1): enumerate key/set entries.**
  - `DurableKeySetStore::for_each_set(&self, visit: impl FnMut(&[u8], &HashSet<Vec<u8>>))` has the
    same contract, over each published set key and its set.
- **FR-3 (P1): a set's capacity follows its length.**
  - Every published set, after every completed public operation and after every open, has a table
    whose capacity is at most four times its length. Through the public API that reads
    `capacity() <= 4 * len()`.
  - The operations are append, `remove_from_set` and its callback form, `try_compute`,
    `try_compute_async`, `try_compute_if_present`, `try_compute_if_absent`, and their compatibility
    wrappers.
  - When an operation removes members, or its compute callback leaves spare capacity, the set's
    table is shrunk to the smallest one that holds twice its length. The decision is made on the
    table's real size. `capacity()` can understate that size after removals, because a removed slot
    is not always counted as free again, so it cannot be the trigger.
  - Resizing is amortised, so alternating appends and removals near any length do not reallocate
    repeatedly.
  - Set contents, key existence, return values, WAL records and error behaviour are unchanged.
- **FR-4: compatibility.**
  - No existing signature changes, and no persisted format changes.
  - A store written before this change opens with FR-3 holding for every set.
  - The key/sorted-map store gains no enumeration here. It is not needed by the motivating bound,
    and adding it is separate work.

## Acceptance

- Each FR is tested through public reads: the enumeration itself, and `capacity()` of the sets it
  visits.
- `capacity()` cannot see a table's tombstoned slots. FR-3's real-size clause and its hysteresis
  are therefore also tested by measuring set-table allocations in a test binary with its own
  allocator.
- Concurrent-writer tests show unchanged entries visited exactly once.
- Reads of every key make progress while `visit` is blocked and no batch touches the guarded
  part. Writers held by it proceed once it returns.
- Reopening a file-backed store whose WAL grew a set and cut it down gives a set within FR-3.
- Set-removal workloads keep median time within +30% of the baseline recorded before production
  edits (plan.md).
- Full suites and builds pass. Unit tests use memory stores; reopen tests use isolated temporary
  directories.
