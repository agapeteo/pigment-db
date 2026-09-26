# Entry enumeration and bounded set capacity: tasks

Each task observes RED for its behaviour before the production change that satisfies it. A new
method first lands as a stub that visits nothing, so its RED is a failed assertion and not a
compile error.

- [x] T001 Write the spec and plan. Record the performance baseline (plan.md) and the suite baseline
  at `d5ad6ee`.
- [x] T002 FR-1:
  - RED: `for_each_entry` over a stub visits nothing.
  - GREEN: iterate the map.
  - Then: an unchanged entry is visited exactly once under concurrent writers, and reads progress
    while `visit` blocks.
- [x] T003 FR-2: the same, for `for_each_set`.
- [x] T004 FR-3, the live paths:
  - RED: a set cut down by `remove_from_set`, by its callback form, and through each compute variant
    (including a callback that reserves) keeps its peak capacity.
  - GREEN: `release_spare_capacity` at each site.
  - Then: alternating appends and removals near the rule's own thresholds do not reallocate on
    every call. This is counted in table allocations, because `capacity()` moves without a resize.
    Probed with a shrink-to-fit rule and with `shrink_to(len + 1)`.
  - The trigger changed before any production edit: RED showed `capacity()` understating the table,
    so the rule calls `shrink_to(2 * len)` unconditionally (plan.md).
- [x] T005 FR-3, open:
  - RED: a file-backed store whose WAL grew a set and cut it down reopens with the peak table.
  - GREEN: release at load.
- [x] T006 Performance gate (plan.md), full suite, `cargo fmt --check` and Clippy. Record
  everything in verification.md.
- [x] T007 Review fixes (verification.md, "Review"): corrected contracts, a real-table-size test, the
  hysteresis sweep, a large cut, and churn that removes what it inserts.
- [ ] T008 MSRV 1.91, macOS and Windows: CI after publication, recorded with the run id.
