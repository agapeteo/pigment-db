# Contract: Offline Legacy-to-V1 Migration CLI

## Supported interface

```text
pigment-db-migrate --source <LEGACY_DIR> --destination <V1_DIR>
                    [--timestamp-granularity-nanos <NONZERO_U64>]
pigment-db-migrate --help
pigment-db-migrate --version
```

Only these long options are accepted. Arguments are parsed as operating-system
strings so non-UTF-8 paths work where supported. Unknown/duplicate options,
missing values, zero granularity, and invalid numeric values are usage errors. The
default granularity is 60,000,000,000 ns.

The supported migration interface is the executable. There is no supported Rust
migration API. The binary may use a doc-hidden package bridge to private logic.

## Source contract

- `LEGACY_DIR` exists, is a readable directory, and remains offline/quiescent.
- Canonical files are `kv.wal.dat`, `set.wal.dat`, and `map.wal.dat`; at least one
  must exist and every one present is migrated in the same invocation.
- Canonical names determine store family, including empty/delete-only histories.
- Each input must be complete legacy format and validate with existing native-
  endian, payload-only CRC/action semantics.
- V1, truncated/corrupt legacy, wrong file type, or store-incompatible payload is
  non-migratable.
- Recognized dot-prefixed recovery or `.next` staging artifacts reject the source;
  the CLI never selects authority or cleans source artifacts.
- Every source file is opened read-only, captured exactly before destination
  creation, and reread before success. Initial open/read or final reread I/O failure
  causes exit 3; a successful final reread with changed bytes causes exit 7.
- Legacy endian is assumed to match the migration host. No alternate-endian guess
  is attempted.

## Destination contract

- `V1_DIR` is explicit, distinct from the source, and must not exist as a file,
  directory, or symlink. Its parent must already exist.
- The CLI uses exclusive directory creation; it has no force, overwrite, merge,
  resume, or in-place mode.
- One wholly V1 active file is create-new for each source family. No output mixes
  formats or contains recovery/staging artifacts.
- Each output uses the requested/default granularity and base bucket zero, and is
  written, flushed, synchronized, closed, reopened, strictly replay-validated,
  and compared with the legacy logical snapshot.
- Exit 0 is emitted only after every family and the final source-stability check
  pass. A failed/interrupted destination is never reported successful.
- Handled failures best-effort remove only exact files/directory created by this
  invocation. If cleanup fails or the process dies, leftovers remain diagnostic
  artifacts and every later invocation refuses to overwrite them.

### Cleanup ownership and sequencing

The migration engine maintains an invocation-local ordered registry of destination
paths. It registers the destination directory only after exclusive directory
creation succeeds and registers each canonical output only after its create-new
operation succeeds. A path that was merely proposed, pre-existing, unresolved, or
not successfully created by this invocation is never registered.

Immediately after exclusive destination-directory creation and ownership
registration are GREEN—and before output-create failure, partial-write, flush,
sync, reopen/read, validation, final source reread, or changed-source handling is
implemented—focused runtime RED–GREEN pairs use test-owned registered paths to
prove:

1. successful cleanup removes registered files in reverse creation order and then
   removes the registered directory only when empty; and
2. an injected cleanup removal failure stops broadening cleanup, preserves the
   exact remaining registered paths as diagnostic artifacts, and reports the
   original checkpoint plus the cleanup operation/path.

Every later handled failure routes through these already-GREEN cleanup transitions
and asserts the precise removed and remaining path sets. Source paths and
unregistered destination entries are never cleanup targets.

## Output and exits

| Situation | stdout | stderr | Exit |
|---|---|---|---:|
| migration success | One final summary with families, destination, and byte/entry counts | Empty | 0 |
| help/version | Help/version text | Empty | 0 |
| invalid arguments/configuration/path relation | Empty | `error:` plus one usage line | 2 |
| source unavailable/I/O | Empty | Actionable path diagnostic | 3 |
| invalid, truncated, corrupt, V1, or unresolved source | Empty | Actionable non-migratable diagnostic | 4 |
| destination already exists | Empty | No-overwrite diagnostic | 5 |
| destination write/flush/sync/reopen/validation/cleanup failure | Empty | Exact operation/path diagnostic | 6 |
| source changed during migration | Empty | Offline-contract diagnostic | 7 |

Operator input and expected filesystem failures never panic. No progress is sent
to stdout; a migration success has exactly one final success record.

## Required process and failure evidence

- All three frozen fixtures individually and together; empty and delete-only input.
- Exact source hashes/bytes before and after every successful and failed invocation.
- Public V1 startup, exact logical parity, append, and three reopenings.
- Existing destination as empty/nonempty file, directory, and symlink.
- No source file, hidden recovery/staging artifacts, V1 input, every legacy byte
  truncation, CRC/action/offset/payload corruption, and wrong-family payload.
- Failures at initial source open/read, destination directory creation, successful
  cleanup, cleanup removal, output-file creation, header/body write, flush, sync,
  reopen/read/validation, and final source reread. Every handled failure after
  destination creation asserts its exact cleanup result. Process interruption
  covers the close boundary because ordinary Rust file drop has no reportable close
  result.
- Child termination after destination creation, partial header/body, complete write,
  validation, and immediately before success output.
- Unknown, duplicate, missing, and non-UTF-8 arguments where supported.
- Linux, macOS, and Windows without assumptions about rename-overwrite, directory
  fsync, hard links, advisory locks, or Unix-only exit signals.

## Delivery invariant

The pure in-memory conversion codec is proven before destination filesystem work.
Each source and destination checkpoint receives a focused runtime RED before its
production transition exists. The test must prove checkpoint entry, exact
operation/path error, immutable source bytes, no later checkpoint, and the precise
created leftovers or cleanup result. Destination-directory ownership registration,
successful cleanup, and cleanup-removal failure MUST all be GREEN before output
creation or any later post-creation failure transition is introduced. Each later
failure composes the proven cleanup transition rather than adding cleanup behavior
opportunistically.
Complete single-/multi-family success and exit 0 are implemented only after every
earlier checkpoint pair is GREEN. The private runner is GREEN before the binary
bridge; the first binary contract passes on its first execution, and child-process
interruption regressions follow binary creation.
