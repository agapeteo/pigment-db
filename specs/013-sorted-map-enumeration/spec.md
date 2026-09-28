# Sorted-map enumeration
Status: approved for implementation, 2026-09-28.

**Motivation (provenance):** penpack finding V414. penpack bounds how many bytes of certain
sorted-map entries each of its tenants may hold. It must learn what a store already holds when it
opens one, and it cannot: no public API visits the key/sorted-map store's keys. Spec 012 added the
same enumeration to the key/value and key/set stores and left this store out (012 FR-4).

## User scenarios and requirements

- **FR-1 (P1): enumerate key/sorted-map entries.**
  - `DurableKeyMapStore::for_each_sorted_map(&self, visit: impl FnMut(&[u8], &BTreeMap<SearchKey, Vec<u8>>))`
    calls `visit` once for each published key and its sorted map.
  - Every published key is visited, whatever its map holds. A key whose map is empty is visited
    with the empty map, so the visit agrees with `contains_key` and `size()`.
  - The visit is not a snapshot. A key present and unchanged for the whole call is visited exactly
    once. A key inserted, changed or removed during the call may be visited before or after that
    change, or not at all.
  - `visit` runs while part of the store is guarded, so it MUST NOT call any method of the same
    store. That may deadlock, as for compute callbacks.
  - A part stays guarded from its first key until the visit moves on. Reads of any key, and writes
    to other parts, proceed. Writes to the guarded part wait until the visit leaves it. On a
    file-backed store, a compaction queued behind such a writer makes every writer wait with it.
  - Nothing is copied, and nothing is written to the WAL.
- **FR-2: compatibility.**
  - No existing signature changes, and no persisted format changes.

## Acceptance

- FR-1 is tested through the enumeration itself, and through `contains_key`, `size()` and
  `get_sorted_map` for comparison.
- A concurrent-writer test shows unchanged keys visited exactly once while other keys are inserted
  and removed.
- Reads of every key make progress while `visit` is blocked. Writers held by it proceed once it
  returns.
- A reopened file-backed store is visited exactly as it was written.
- A file-backed store's WAL is the same size after a visit as before it.
- The new test target runs on every operating system in CI.
- Full suites and builds pass. Unit tests use memory stores; reopen tests use isolated temporary
  directories.
