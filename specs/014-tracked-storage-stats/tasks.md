# Tracked storage stats: tasks

Each task observes RED for its behaviour before the production change that satisfies it. The method
first lands as a stub returning all-zero figures, so its RED is a failed assertion and not a compile
error.

- [x] T001 Write the spec and plan. Record the suite baseline at `9ba8871`.
- [x] T002 FR-1, FR-2 (P1):
  - RED: against the stub, the equality with `storage_stats()` after rotations fails, per family.
  - GREEN: read the writer's figures under the WAL state lock.
  - Then: agreement after online compaction and a further rotation; the unreadable-directory test;
    the concurrent-rotation test; the capture and detached-writer readings; the compute-callback
    progress test; `compile_fail` doctests for memory stores.
- [x] T003 FR-3: RED with a body that ignores the WAL's health; GREEN by mapping both failed-closed
  states to `FailedClosed`.
- [x] T004 Add the new target to `recovery.yml` and `tests/ci_workflow.rs`.
- [x] T005 Full suite, `cargo fmt --check` and Clippy. Neutralization probes and an adversarial
  review. Record everything in verification.md.
  - The review's fixes, each RED first: the reading takes no lock (FR-1, published figures); the
    concurrent test sees a torn reading; damage after the open, allocation and the key/value
    transaction lock have tests; a poisoned lock fails closed; the unit tests run on every OS.
  - Review 2 (verification.md), each RED first where it changes behaviour or closes a test gap:
    the Principle IV record (plan.md: the sequence lock named as a new coordination layer and its
    owner, the `Mutex` alternative weighed and recommended, the memory-ordering argument and
    aarch64 instructions; spec.md: FR-1's "Why a new coordination layer" note and Amendments);
    the performance gate (threshold, `examples/put_cost.rs`, baseline at `9ba8871`, the skip of
    unchanged publications after the first series failed); a failed-closed reading allocates only
    its detail; Acceptance covers three causes; the CI pins refuse an `if:` anywhere in the step;
    a test for a panic that began before the write side was taken; the FR-1 test's timing
    recorded; the nits, and the deferred defects filed in
    `reviews/opus-5.5-maintenance-2026-10-02.md`.
- [ ] T006 MSRV 1.91, macOS and Windows: CI run on the published revision.
