# Cross-process directory ownership: verification

Measured on Linux 7.1 with rustc 1.97.1, on branch `011-cross-process-directory-lock`, from
`af25792` to `e03c2a1`. A review of that revision found three defects and several gaps. They were
fixed from `1b79628` to `f57bdde`, and "Review fixes" below records that evidence.

## Suites

| Revision | Passed | Failed | Ignored | Test binaries |
|---|---:|---:|---:|---:|
| `af25792` (baseline) | 568 | 0 | 27 | 25 |
| `e03c2a1` | 585 | 0 | 28 | 26 |
| `f57bdde` | 597 | 0 | 28 | 26 |

The extra ignored test is `directory_lock::child_entry`, the child-process role runner.

- `cargo fmt --check` is clean.
- `cargo clippy --offline --all-targets` reports nothing at either revision.
- `cargo doc --no-deps` builds with no warnings.
- The complete suite ran with `--no-fail-fast`.
- CI runs these on Linux, macOS and Windows:
  - `tests/directory_lock.rs`;
  - the library's `maintenance_coordination::` tests;
  - `compaction::recovery_tests::ownership`.
- A CI job checks every target on rust-version 1.91.
- Only Linux was run for this record.

## RED evidence, in order

Each test was observed failing for the reason stated before the production change that makes it
pass.

**At `af25792`:**

| Test | Failure |
|---|---|
| A1 | `kv opened` |
| A2 | "a directory another process holds must not open" |
| A4 | The lock file did not exist after release |
| A7 | The open succeeded |
| X1, X2 | `kv opened` |
| `init_new` panic | `["opened"]` |
| A3 | `["compacted"]`: a child's closed compaction ran against a held directory |

**Existing tests, once opens created `.pigment-lock`:** 34 inspection and compaction tests went RED
on the new entry. `inspect_generation` skipping it fixed 7 of them. The rest were fixed by claims
retiring the inner lock, by fixtures standing in for that step, and by assertions scoped to name
exactly the lock files.

**Explicit unlock:**
- A8 refused 312 of 400 reopens against a close-only release.
- The same race refused
  `key_set_store::mutation_ordering_tests::append_and_remove_keep_live_and_reopened_order` from its
  own process.

**The remaining behaviours:**

| Test | Failure |
|---|---|
| A6 | `PermissionDenied … cannot open lock file`, before the read-only fallback |
| Symlinked lock path | The target `keep me` was overwritten with the owner record |
| Progress (FR-13) | Another directory's open and a third directory's store drop timed out after 5 s behind a stalled lock-file open |
| `Unsupported` (FR-11) | The open was refused ("cannot lock … unsupported") |
| CI pin | "recovery workflow must run `cargo test --test directory_lock -- --test-threads=1`" |

**R1** was written after the claim step had landed, so it was probed instead: with the claim taking
no replacement lock, it failed. The file was then restored and checked against its sha256.

## Container views, measured

`unshare -rm` emulated two containers. In each namespace a private tmpfs was mounted at the parent
path, and one shared "volume" was bind-mounted at the identical store path beneath it. The first
process held all three families; the second opened.

| Library | Second container |
|---|---|
| `af25792` | Opened: a second writer |
| this branch | Refused: "… already open in another process (lock file …/opt/db/.pigment-lock is held; its last recorded owner is process …)" |

The spec 010 prototype excluded nothing in the same layout (measured by its reviewer). Its lock
lived in each namespace's private parent.

**A read-only parent with a writable volume at the store path**, emulating `docker run --read-only
-v vol:/opt/db`:
- `af25792` and this branch both open.
- This branch creates only `.pigment-lock` inside the volume.
- The spec 010 prototype refused every open with `ReadOnlyFilesystem`.

## Not verified

- macOS and Windows: CI covers them from this revision on, and this record ran neither.
- Network filesystems.
- Solaris and illumos.
- The pid-namespace wording, beyond the documented limitation.

## Review fixes

An adversarial review of `df1fef1` approved it with changes. Each fix below was either observed
failing before it was made, or confirmed by neutralization. For neutralization, the site was
broken, the suite run, and the file restored and checked against its sha256.

### Defects

**Exact inventories counted the inner lock file (`1b79628`).** RED at `df1fef1`:

| Test | Failure |
|---|---|
| `a_reopen_finishes_a_cleanup_that_an_earlier_open_left_pending` | `AuthorityUndetermined` |
| `a_second_family_finishes_a_cleanup_the_first_left_pending` | `AuthorityUndetermined` |
| `a_compaction_retry_finishes_a_cleanup_that_an_earlier_open_left_pending` | `AuthorityUndetermined` |
| `an_open_stalled_across_another_processs_compaction_neither_opens_nor_breaks_it` | At StagingValidate, the compaction child exited 101: `FailedClosed`, "closed source changed before publication" |

At `af25792` the three cleanup sequences finish cleanup. `a_lock_file_in_the_replaced_generation_is_deleted_with_it`
was written after the fix. Neutralizing each site failed exactly the named tests:

| Site neutralized | Failing test |
|---|---|
| `generation_matches` skip | the three cleanup-pending tests |
| Source-revalidation skip | the stalled opener |
| Either cleanup's listing skip | deleted-with-it |
| Either cleanup's removal | deleted-with-it |
| The file-type check (name only) | deleted-with-it's directory control |

**A panic while taking a directory's locks wedged it (`1012318`).**
`a_panic_while_taking_locks_does_not_wedge_the_directory` was RED before the guard: the reopen
timed out after 5 s. With the guard left disarmed, it fails the same way.

### Gaps: no test failed when the mechanism was removed

| Mechanism | Pinned by | Neutralization that fails it |
|---|---|---|
| The lease's `ensure_inner_lock` (FR-5 step 3) | `an_open_that_recovered_holds_the_inner_lock` | A no-op, which also fails the cleanup-pending tests' check that the open holds the lock |
| The claim's `ensure_inner_lock` | `a_claim_that_recovered_holds_the_inner_lock_while_it_stages` | A no-op, failing this test only |
| Post-recovery lock I/O outside the mutex (FR-13) | `a_stalled_inner_lock_after_recovery_holds_up_no_other_directory` | The acquire moved under the mutex, which compiles warning-free |
| The optional replacement-lock check on ordinary opens | the stalled-opener test, at ReopenValidation | `check_existing(..).ok().flatten()` |
| The `ReadOnlyFilesystem` read-only fallback (FR-7) | `a_read_only_filesystem_opens_the_lock_file_read_only_and_records_nothing`, through an injected read-write-open error | The arm dropped |

The stalled-opener test also covers PreviousPublish, where the directory is moved aside. The
opener fails there with `NotFound`, creates nothing, and the compaction completes. This was
measured with a temporary print before it was pinned.

### Tests that would have failed on Windows, or passed vacuously (`a0310da`, `b6194a7`)

- **Windows listings.** Four tests in `tests/windows_physical_durability` asserted listings that
  left out the lock file. The Windows modules were copied, without their cfg line, into a throwaway
  Linux test target:
  - before the change, 4 tests failed;
  - after it, 8 of 9 pass;
  - the ninth, `physical_rotation_conflict_preserves_original_os_error_and_writable_authority`,
    fails at `af25792` too, for a Windows-specific reason.
- **Reads of a held lock file.** A1 and A3 read `.pigment-lock` while holding it. This was emulated
  on Linux by panicking whenever a fresh descriptor is refused the lock:
  - without the Windows skip, A1 and A3 fail;
  - with it, the suite passes.
- **Short reports.** An "open-each" child writing an empty report now fails A1, A4, X1, X2 and A6.
  Before, all 13 tests passed.
- **Refused reopens.** In `every_closed_checkpoint_...`, 18 cuts refuse the reopen, and 9 of them
  carry a claim's inner lock file. That file is now compared byte for byte, and the replacement
  lock is asserted to exist.
- **Migration compatibility.** These tests assert that each lock file an operation takes exists,
  and that none present before has gone.

### Toolchain (`edc0afa`)

rust-version 1.89 was wrong, because `compaction/recovery.rs` compares `PathBuf == String`, which
is stable from 1.91. Measured with the installed 1.88 toolchain and `--ignore-rust-version`, in a
scratch worktree:
- The library fails on `file_lock` (1.89) and that comparison (1.91), and on nothing else.
- With `file_lock` enabled per crate root through `RUSTC_BOOTSTRAP`, and the comparison written
  through `Path`, every target compiles.

rust-version is now 1.91. Toolchains 1.89 to 1.91 are not installed, and the CI job is what runs
1.91. Both CI pins were RED against the previous workflow.

### Other checks

- `cargo clippy --all-targets --target x86_64-pc-windows-gnu` compiles every target for Windows.
  Its warnings all predate the branch.
- The std source at 1.89.0, 1.90.0, 1.91.0 and 1.97.1 was read for the spec's Platform coverage.
- The error-code mappings behind FR-11 were read from the 1.97.1 std source.

### Container views, re-measured at `f57bdde`

This repeats the `unshare -rm` layout above. The second container was refused: "… already open in
another process (lock file …/opt/db/.pigment-lock is held; its last recorded owner is process …)".
Once the holder was killed, it opened.

### Found, not fixed here

The pre-existing defect recorded in the spec: a write after an open whose closed cleanup stays
pending makes the next open fail. It is present at `af25792` and needs its own specification.

### Not verified

- macOS and Windows: CI covers them, and no run of either is recorded here.
- Solaris behaviour, inferred from std's source only.
- A delete-pending lock file at retirement on Windows, when another handle to it is open.
