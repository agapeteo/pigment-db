# Phase 0 Research: Boundary-Aware WAL Recovery

## Decision 1: File-level V1 discrimination and explicit legacy rejection

**Decision**: Prefix every new artifact with `PIGWAL\r\n`, version, store kind,
granularity, base bucket, reserved fields, and header CRC. New integers are
little-endian. Byte zero selects V1 or legacy grammar once. Normal startup rejects
complete legacy format with `MigrationRequired`, rejects truncated/corrupt legacy
as invalid, preserves every source byte, and never mixes or converts formats.

**Rationale**: Valid legacy first bytes are action IDs 0–5, so the magic cannot
collide. Explicit rejection honors the approved offline migration boundary and
prevents startup from modifying an old authority.

**Alternatives considered**: Transparent startup conversion was rejected by
clarification; mixed frames, high-bit actions, and sidecar version metadata leave
ambiguous or separately atomic states.

## Decision 2: Grouped physical records commit one logical mutation

**Decision**: Each action remains a physical record but carries mutation start,
index, count, and one shared timestamp. Replay buffers records and applies them
only after contiguous indices `0..count-1` validate. Count 1 covers ordinary
mutations; the final valid member commits a compute batch. One flush remains.

**Rationale**: This preserves action payload meanings and issue #3 ordering,
avoids a second commit record/flush, and prevents complete early batch members
from becoming visible after a torn later member.

**Alternatives considered**: Separate BEGIN/COMMIT records add overhead; an outer
container needs a nested grammar; in-place commit bits require rewriting.

## Decision 3: Full-envelope CRC32 plus redundant length

**Decision**: Retain four-byte `crc32fast`. V1 CRC covers every frame byte except
itself, including payload and repeated footer offset. Store payload length and its
bitwise complement.

**Rationale**: CRC32 is the selected compact accidental-corruption detector. The
complement prevents upward length corruption from masquerading as torn payload
before the CRC location is known.

**Alternatives considered**: Payload-only CRC leaves metadata unprotected;
BLAKE3 adds bytes/CPU without authentication requirements; treating every
oversized length as truncation can silently discard corruption.

## Decision 4: Checked replay has format-specific outcomes

**Decision**: V1 replay yields complete, recoverable terminal tail, or corruption.
A recoverable result includes accepted snapshot/prefixes, accepted byte length,
pending mutation start, and last bucket. Only partial action-record bytes or an
open group after a complete valid V1 file header are recoverable. A partial/corrupt
file header is invalid. Legacy replay yields complete legacy or invalid/truncated,
never recoverable tail.

**Rationale**: Recovery needs an exact accepted logical boundary without exposing
a pending group. File configuration cannot be reconstructed safely from a partial
header, and legacy has no commit boundary proof.

**Alternatives considered**: Panics, a generic parse error, frame-level prefixes,
and reconstructing a file header from caller assumptions were rejected.

## Decision 5: Repair accepted logical state through staged publication

**Decision**: Select V1 authority first, encode the accepted logical state as one
complete snapshot group, validate/flush/synchronize `.next`, rename it over active,
and only then open writable. Physical accepted records may change; their logical
effects must match exactly. Any failure preserves the selected source and errors.

**Rationale**: This meets clarified FR-007 and reuses issue #1's crash-safe state
machine. Direct truncation changes the only authority before a replacement is
proven.

**Alternatives considered**: `set_len`, raw-prefix copy, and repair before
authority selection offer weaker proofs or preserve unnecessary physical history.

## Decision 6: Reserve “legacy” for format, not artifact role

**Decision**: Call the three issue #1 roles active, recovery (dot-prefixed), and
staging. Compare only accepted V1 group snapshots. If any potentially authoritative
candidate is complete legacy format, normal startup returns `MigrationRequired`
without cleanup; truncated/corrupt legacy is invalid. V1 ambiguity remains
`AuthorityUndetermined`.

**Rationale**: The existing code name “legacy” describes a recovery-copy role and
would otherwise collide with the new byte-format meaning. Startup cannot silently
choose or delete a legacy-format candidate after explicit migration was required.

**Alternatives considered**: Comparing legacy and V1 snapshots during startup
would reintroduce implicit migration/cleanup; renaming persisted files is needless.

## Decision 7: Additive timestamp options after an internal GREEN path

**Decision**: Add validated `TimestampGranularity` and `DurableStoreOptions`, plus
fallible options initializers and vector constructors. Zero or durations exceeding
`u64` nanoseconds fail before I/O. Test default/non-default/persisted behavior
through a crate-private initializer first; expose correct public adapters only once
that path is GREEN, and require public tests to pass on first exposure.

**Rationale**: This supplies one-minute defaults and persisted configuration
without intentionally exposing a wrong adapter or using compilation failure as RED.

**Alternatives considered**: Changing existing signatures is breaking; dummy
public adapters violate the constitution; storing granularity only in memory makes
reopen inconsistent.

## Decision 8: Clamp timestamps under the existing WAL lock

**Decision**: Compute `floor(unix_nanos / granularity) * granularity`, then take
the maximum with the prior accepted bucket while holding the WAL acceptance lock.
Advance only after successful write/flush. Replay/header state restores the bucket;
physical group offset orders equal buckets. A private clock exists only in tests.

**Rationale**: This preserves nondecreasing time across concurrency, rollback,
clock reversal, and restart without another lock or write rejection.

**Alternatives considered**: A separate timestamp mutex creates ordering risk;
rejecting backward time harms availability; wall time alone is not ordered.

## Decision 9: Compaction and migration preserve metadata, not history

**Decision**: V1 compaction writes the last bucket in the header and one complete
snapshot group using that bucket; header-only empty snapshots retain it. Offline
legacy migration writes bucket zero with the requested/default granularity. Full
historical versions remain discarded as today.

**Rationale**: Timestamp semantics survive reopening while migrated legacy has an
explicit unknown historical time. Point-in-time retention remains separate.

**Alternatives considered**: Synthesizing historical timestamps is misleading;
retaining every version expands the approved scope.

## Decision 10: Reuse fixed performance gates and adjacent boundaries

**Decision**: Capture the issue #3 36-cell baseline before edits and retain its
thresholds, plus the one-million-operation startup gate. No steady-state physical
sync, offset-width migration, authenticated hash, or time-point API is added.

**Rationale**: Group metadata touches every mutation, so per-cell evidence is
mandatory; unrelated format/durability work would invalidate the comparison.

**Alternatives considered**: Aggregate averages hide regressions; changing
thresholds after measurement violates the constitution.

## Decision 11: Provide a directory-level, no-overwrite offline migration CLI

**Decision**: Ship `pigment-db-migrate --source <LEGACY_DIR> --destination
<V1_DIR>` with optional `--timestamp-granularity-nanos`, plus `--help`/`--version`.
Use `args_os` and no parser dependency. Discover canonical active filenames and
migrate every found family; reject a source with no recognized file, V1 input,
invalid/truncated legacy, or any recognized recovery/staging artifact. Create the
destination directory exclusively and never overwrite it or modify source bytes.

**Rationale**: A database directory is the library's initialization unit, canonical
filenames unambiguously identify even empty/delete-only store families, and
directory-level migration avoids a partially selected family set. Long explicit
flags and non-UTF-8 paths are supported without another dependency.

**Alternatives considered**: Per-file `--kind` migration permits incomplete
database sets; filename/payload guessing outside canonical paths is unsafe;
`clap`, in-place conversion, `--force`, and automatic startup conversion were
rejected.

## Decision 12: Validate every output and source-stability boundary

**Decision**: Read and strictly validate every legacy source before destination
creation. Create-new each V1 output, write/flush/sync/close, reopen, strictly replay,
and compare exact logical snapshots. Reread the source bytes before exit 0. Register
only successfully created destination paths. Prove reverse-order cleanup success and
cleanup-removal failure immediately after destination-directory registration and
before output creation or later checkpoint handlers compose that cleanup. Handled
failure best-effort removes only exact outputs/directory created by the invocation;
cleanup failure or process death may
leave a destination that future runs refuse to overwrite. Use exit codes 0 success,
2 usage/configuration, 3 source I/O, 4
invalid source, 5 existing destination, 6 destination/validation/cleanup failure,
and 7 source changed.

**Rationale**: Exclusive creation prevents overwrite races portably. Source remains
the rollback authority. Validation and source reread make success meaningful even
though portable process locking is out of scope.

**Alternatives considered**: `std::fs::rename` can replace a target and differs by
platform; hard-link publication is not portable; advisory locks need new policy or
dependencies; deleting arbitrary destination trees is too destructive.

## Decision 13: Use one hidden binary bridge, not a migration library API

**Decision**: Keep the migration engine in crate-private `src/migration.rs` and
the argument/output/exit runner in crate-private `src/migration_cli.rs`. Declare
the `pigment-db-migrate` target explicitly in `Cargo.toml`, then add one
`#[doc(hidden)]` zero-argument library symbol used only by
`src/bin/pigment-db-migrate.rs` to enter the private runner. Do not expose supported
migration options, report, error, or callable API types.

**Rationale**: Cargo binary targets are separate crates and cannot call
`pub(crate)` code. The hidden bridge preserves a single implementation and honors
the user's CLI-only choice without a second crate or copied safety-critical parser.

**Alternatives considered**: A public migration API was explicitly not selected;
a private core crate violates the single-library structure; source inclusion or
copied parsers can drift.

## Decision 14: Make every failure checkpoint its own RED–GREEN slice

**Decision**: Test then implement repair failures separately for staging create,
partial write, flush, staged validation, sync, publish/rename, exact reopen,
blocking cleanup, and deferrable post-publication cleanup. Migration receives the
same vertical treatment: pure in-memory conversion is proven first; initial source
read, strict preflight, and destination directory creation establish initial exact
path ownership. Cleanup success and cleanup-removal failure each receive a focused
RED–GREEN pair before output creation, partial write, flush, sync, reopen/read, strict output
validation, final source reread, or changed-source handlers compose cleanup. Each
later transition still receives its own focused RED. Complete single-/multi-family
migration success and exit 0 are the final transitions, never the implementation
used to enable later fault tests.
Binary/public adapters are added only over GREEN private behavior and must pass
their first contract tests; executable process-interruption regressions follow the
thin binary adapter.

**Rationale**: The constitution requires one behavior at a time and authority
proof at each failure transition. Implementing the happy path first would also
change untested failure behavior.

**Alternatives considered**: One test for all checkpoints obscures the failing
transition; late failure matrices and intentionally wrong adapters violate the
constitution.

## Decision 15: Publish a missing-store header through staging

**Decision**: Keep the pure V1 header/frame codecs disconnected from startup and
steady-state writing until each structural, bounds, CRC, and round-trip invariant
is GREEN. For a missing store, exclusively create staging, write and flush the
complete 40-byte header, strictly validate it, perform startup synchronization,
prepare the same append-capable handle at offset 40, and publish by rename before
returning writable without another fallible filesystem operation. The
active-path rename is the commit point: every reported pre-publication failure
leaves active absent; an interruption after publication observes a complete valid
active header and therefore represents committed creation, not a partial header.
A partial/corrupt header already present when startup begins remains preserved
invalid and is never cleaned as a failed new-store attempt.

**Rationale**: Directly writing the active file can expose a partial discriminator
that blocks every later startup. Staging makes the visible state transition atomic
without weakening the rule that pre-existing invalid evidence is preserved.

**Alternatives considered**: Preserving a partial newly created active file makes
routine creation failure operationally sticky; best-effort deletion after direct
write cannot cover process death; close-rename-reopen introduces a fallible step
after the active path is visible; reconstructing a partial header guesses metadata.
