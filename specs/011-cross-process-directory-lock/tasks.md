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
  - RED: A3 at the pre-change revision. R1 was written after this step landed, so it was probed by
    neutralization, not observed failing first.
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
  - Declare `rust-version = "1.89"`. This was wrong: an existing comparison needs 1.91, and T016
    corrects it.
  - Update the rustdoc of `try_init_new` and `compact_directory_in_place`.
  - Add `tests/directory_lock.rs` to every CI operating system, with its pin in `ci_workflow.rs`,
    whose RED comes first.
- [x] T011 Verify:
  - Run the full suite, `cargo fmt --check` and Clippy.
  - Record the mount-namespace run.
  - Apply the constitution amendment.
  - Write verification.md.

Review fixes, after the review of `df1fef1`:

- [x] T012 Exact inventories skip the inner lock:
  - RED: the reopen, the second family and the compaction retry after a pending cleanup; the stalled
    opener at StagingValidate.
  - GREEN: `generation_matches`, source revalidation and both `.previous` cleanups skip a regular
    `.pigment-lock`, and the cleanups delete it (`1b79628`).
- [x] T013 Pin the recovering owners' inner lock and its progress, by neutralization (`7da7807`).
- [x] T014 The `Pending` slot guard:
  - RED: a panic while taking locks wedged the directory.
  - GREEN: the guard. The `ReadOnlyFilesystem` fallback is pinned by neutralization (`1012318`).
- [x] T015 The Windows-run tests, the open-each report length, and the lock-file existence
  assertions (`a0310da`, `b6194a7`).
- [x] T016 rust-version 1.91, a CI job on it, and the ownership tests on every OS. Both pins were
  RED first (`edc0afa`).
- [x] T017 Pin the stalled opener while the directory is moved aside (`f57bdde`).
- [x] T018 Correct the spec, plan, constitution and this record: FR-4, FR-9, FR-11, Platform
  coverage, the Windows semantics, Solaris, and the pre-existing defect.

Fixes after the re-review of `c026816`:

- [x] T019 No lock-free entry, and no open going live on another thread's unfinished attempt:
  - RED: an opener stalled before its directory check recovered a live compaction; a second family
    went live while another holder had the inner lock; after a panic, it never took the lock.
  - GREEN: open_locks asks again and refuses a missing directory at once; ensure_inner_lock waits,
    behind an unwinding guard. FR-5 step 3 is pinned for every family (`01f0200`).
- [x] T020 Harden the ownership tests: the FR-11 warning, foreign entries at revalidation and in
  `.previous`, the claim's retirement, the inspection assertions, CRLF-safe pins, and Windows
  lock release (`a00e12b`). A failing checkpoint child exits with its own code (`583ddbb`).
- [x] T021 Correct the documents after the re-review.

Fixes after the check of `2170d23`:

- [x] T022 An alias names its directory, recovery through an alias reads that directory, and a
  held inner lock must still be the directory's:
  - RED: an alias open during a move answered `NotFound`; an alias open skipped recovery
    (`Normal`); a stalled open went live on a retired lock.
  - GREEN: the three fixes (`6a5b20b`).
- [x] T023 Correct the documents after the check.

Fixes after the check of `68be84f`:

- [x] T024 Read paths lexically before any lstat; recover through the identity for `.`- and
  `..`-terminated paths; identify a held lock by holding, not inode; keep it when its path cannot
  be read:
  - RED: the alias tests extended with `alias/`, `alias/.` and a slash-terminated chain.
  - Pins, confirmed by neutralization: identity unit tests, two stale verdicts, a file left at the
    lock path, a later family, a claim, and the held-lock check under FR-13 (`cfc8cb5`).
- [x] T025 Correct the documents after the check.
