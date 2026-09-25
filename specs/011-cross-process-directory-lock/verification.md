# Cross-process directory ownership: verification

Measured on Linux 7.1 with rustc 1.97.1, on branch `011-cross-process-directory-lock`, from
`af25792` to `e03c2a1`.

## Suites

| Revision | Passed | Failed | Ignored | Test binaries |
|---|---:|---:|---:|---:|
| `af25792` (baseline) | 568 | 0 | 27 | 25 |
| `e03c2a1` | 585 | 0 | 28 | 26 |

The extra ignored test is `directory_lock::child_entry`, the child-process role runner.

- `cargo fmt --check` is clean.
- `cargo clippy --offline --all-targets` reports nothing at either revision.
- `cargo doc --no-deps` builds with no warnings.
- The complete suite ran with `--no-fail-fast`.
- CI now runs `tests/directory_lock.rs` on Linux, macOS and Windows. Only Linux was run for this
  record.

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
