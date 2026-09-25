# Cross-process directory ownership: tasks

Each task observes RED before any production change for its behaviour. The order below is the
planned one. The claims step (T007) landed together with T003-T005 in `2579342`, and R1 was then
probed rather than observed failing in sequence (see verification.md).

- [x] T001 Write the spec and plan. Record the baseline: 568 passed, 0 failed, 27 ignored across 25
  test binaries at `af25792`. `cargo fmt --check` is clean and Clippy reports nothing.
- [x] T002 The test harness, in `tests/directory_lock.rs`:
  - One ignored child-entry test.
  - The child reports atomically through a file and exits with a fixed code.
  - The handshake directory lives outside the snapshot root, and a child guard reaps on drop.
  - A permission fixture that fails loudly where permissions are not enforced.
- [x] T003 The inner lock on opens:
  - RED: A1, A2, A4, A7, X1, X2 and the `init_new` panic, at the pre-change revision.
  - GREEN: an entry takes `<store>/.pigment-lock`, records its process id, and refuses a held lock.
  - Then FR-9: the existing inspection and compaction tests turn RED, and `inspect_generation`
    skipping the file turns them GREEN.
  - Then the existing namespace assertions: exclude exactly the lock files, and assert they exist.
- [x] T004 A6, the read-only fallback:
  - RED against T003.
  - GREEN: open read-only on `PermissionDenied` or `ReadOnlyFilesystem`.
- [x] T005 A8, explicit unlock:
  - RED against T003's close-only release.
  - GREEN: unlock under the mutex, and close outside it.
- [x] T006 A lock path that is not a regular file:
  - RED: a symlinked `.pigment-lock` whose target the pid write overwrites.
  - GREEN: refuse it, with the check made before the open.
- [x] T007 Claims and the replacement lock:
  - RED: A3 at the pre-change revision, and R1 against T006.
  - GREEN: claims take both locks and retire the inner one before publication. Maintenance-state
    opens take the replacement lock and then the inner one after recovery. Other opens check an
    existing replacement lock.
  - The fault-checkpoint and closed-compaction suites stay green.
- [x] T008 Progress:
  - RED: a stalled lock-file open for one directory blocks another directory's open and drop.
  - GREEN: the `Pending` state keeps the I/O outside the mutex.
- [x] T009 Unsupported locking:
  - RED: an injected `Unsupported` lock result refuses the open.
  - GREEN: skip that lock and warn.
- [x] T010 Contract and CI:
  - Declare `rust-version = "1.89"`.
  - Update the rustdoc of `try_init_new` and `compact_directory_in_place`.
  - Add `tests/directory_lock.rs` to every CI operating system, with its pin in `ci_workflow.rs`,
    whose RED comes first.
- [x] T011 Verify:
  - Run the full suite, `cargo fmt --check` and Clippy.
  - Record the mount-namespace run.
  - Apply the constitution amendment.
  - Write verification.md.
