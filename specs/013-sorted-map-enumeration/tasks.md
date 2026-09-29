# Sorted-map enumeration: tasks

Each task observes RED for its behaviour before the production change that satisfies it. The new
method first lands as a stub that visits nothing, so its RED is a failed assertion and not a
compile error.

- [x] T001 Write the spec and plan. Record the suite baseline at `37db5bb`.
- [x] T002 FR-1:
  - RED: `for_each_sorted_map` over a stub visits nothing.
  - GREEN: iterate the map.
  - Then: an unchanged key is visited exactly once under concurrent writers; reads progress while
    `visit` blocks and writers held by it finish after it; a reopened file-backed store is visited
    as written; a visit leaves the WAL's size unchanged.
- [x] T003 Run the new target on every operating system: `recovery.yml` and `tests/ci_workflow.rs`.
- [x] T004 Full suite, `cargo fmt --check` and Clippy. Neutralization probes and an adversarial
  review. Record everything in verification.md.
- [x] T005 MSRV 1.91, macOS and Windows: CI run 36510511873 on `82aaf6b`, all four jobs green.
