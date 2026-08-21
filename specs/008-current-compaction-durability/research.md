# Phase 0 Research: Current-Format Compaction and Windows Physical Durability

## 1. Current-format discovery and compatibility classification

**Decision**: Treat the store directory as a dedicated Pigment DB namespace and reject every unexpected entry as `InvalidArtifact` without mutation. Validate exact native-path names for canonical active, sealed-segment, recovery, and compaction artifacts, contiguous segment identity, current V2 envelopes, family identity, record integrity, and maintenance authority. Use only a shallow header/envelope recognition probe for known older formats so runtime can return `MigrationRequired`; do not decode legacy application data or call the migration engine.

**Rationale**: Closed compaction publishes a replacement directory and later removes the previous generation, so ignoring an unrelated entry could delete caller data. Rejecting unexpected entries before mutation makes inspection and publication deterministic and preserves all evidence. A bounded recognition probe preserves clear migration guidance without moving compatibility code into runtime maintenance.

**Alternatives considered**: Copying unrelated entries into staging was rejected because compaction would silently assume ownership of non-database data and complicate source-stability checks. Ignoring unrelated or malformed entries was rejected because later directory cleanup could delete them or hide authority evidence. Reusing migration conversion was rejected by the external-only boundary.

## 2. Manifest encoding and artifact ownership

**Decision**: Introduce one custom binary manifest with fixed magic, version, bounded body length, phase/body fields, and trailing CRC32. Descriptors contain only relative artifact names, roles/families, lengths, and CRC32 checksums—never application data. Reject absolute paths, `..`, duplicate names, aliases, excessive counts/lengths, or entries outside the anchor.

Directory-scoped artifacts live beside the database directory as `.<directory>.pigment-compact.{manifest,manifest.next,next,previous}`. Family-scoped artifacts live inside the store directory and append equivalent suffixes to the canonical active name. Paths use native `OsString` component appends.

**Rationale**: Existing `crc32fast` is sufficient for torn/corrupt metadata and avoids an unapproved dependency. Relative bounded descriptors make cleanup invocation-owned and prevent arbitrary path targeting.

**Alternatives considered**: JSON was rejected in favor of a bounded native-path-aware parser. Cryptographic checksums were rejected because this is fault integrity, not adversarial authentication. Filename-only phase inference was rejected because partial transitions can be contradictory.

## 3. Recoverable publication protocol

**Decision**: Use `Prepared`, `PreviousPublished`, `ReplacementPublished`, and `CleanupPending`. Publish every revision through a synchronized temporary and atomic namespace transition. Never remove the last verified generation. Capture source bytes/state, validate same-parent replacement, re-read source byte-for-byte, publish source as previous, publish replacement, reopen, then clean exact obsolete artifacts. An online manifest identifies its mode and initially treats the `Prepared` source descriptor as a verified prefix because the old WAL may advance; an atomic same-phase rewrite freezes the exact final inventory and marks it finalized before any publication.

**Rationale**: The manifest states intent while complete old/replacement generations provide recovery evidence. Source revalidation closes the capture-to-publication race.

**Alternatives considered**: In-place segment deletion and file-by-file overwrite were rejected because interruption can expose incomplete mixed generations. Post-publication cleanup failure remains a successful publication with pending cleanup.

## 4. Recovery authority rules

**Decision**:

| Durable phase | Recovery action |
|---------------|-----------------|
| `Prepared` | Verified old generation wins; restore split source artifacts and discard only provably incomplete owned staging. |
| `PreviousPublished` | Prefer a fully validated replacement; otherwise restore verified previous; if neither is provable, preserve all evidence and return `AuthorityUndetermined`. |
| `ReplacementPublished` | Validate and select canonical replacement; retain previous until authority is confirmed. |
| `CleanupPending` | Use replacement and retry only exact safe cleanup. |

A valid main manifest outranks an unpublished `.manifest.next`. Without a valid manifest, ambiguous complete generations yield `AuthorityUndetermined`; malformed debris that cannot compete with a proven authority yields `InvalidArtifact`. Recovery of an online `Prepared` manifest selects the canonical old WAL even when it has valid bytes beyond the recorded prefix. Online cleanup validates the immutable published replacement prefix and permits valid later appends/rotations.

**Rationale**: These rules implement the clarified preference without guessing and keep online stores writable after cleanup is deferred.

**Alternatives considered**: Always restoring old state, trusting a canonical path without validating content, and requiring replacement checksum equality forever were rejected.

## 5. Same-process closed-store ownership

**Decision**: Use a process-local registry keyed by stable canonical directory identity. File stores acquire an RAII open lease before maintenance recovery and hold it for their lifetime. Closed compaction atomically acquires an exclusive lease only at open count zero, before artifact creation. If canonical directory is absent during recovery, resolve the nearest existing parent and append the exact leaf.

**Rationale**: This detects forbidden same-process overlap without global mutation coordination or cross-process locking.

**Alternatives considered**: Caller discipline alone fails the clarified requirement; OS locking is out of scope; a global mutation gate is a throughput hazard.

## 6. Online maintenance coordination

**Decision**: Add a constant-size coordinator per store: `RwLock<()>` plus `AtomicBool`. Mutations hold shared maintenance coordination through `DashMap shard -> WAL acceptance -> live publication`. Reads remain unchanged. Async user work occurs outside the gate; final acceptance reacquires `maintenance -> shard -> WAL`; callbacks run after guards drop.

**Rationale**: Prefixing the existing shard-to-WAL order lets exclusive cutover wait for both durability acceptance and live publication without per-key state or unrelated-instance contention.

**Alternatives considered**: Global/directory gates, unbounded per-key maintenance locks, reversed lock order, and holding locks across async callbacks were rejected.

## 7. WAL-ordered concurrent delta

**Decision**: Store `Option<DeltaRecorder>` in `WalState`. After write, flush, and required synchronization succeed—but before releasing WAL order—record one logical group with timestamp bucket, current-V2 action/payload frames, and encoded length. Compute batches stay atomic. Inactive recording is only an option branch and clones nothing.

A group equal to the limit is accepted. The first group that would exceed it marks overflow, clears retained entries, records no partial group, and makes later mutations skip recording while the original WAL continues normally.

**Rationale**: WAL state is the cross-shard acceptance order. This excludes rejected, rolled-back, cancelled, and no-op work while bounding memory.

**Alternatives considered**: Store-layer recording can reverse order; raw-byte copying cannot reuse offsets; state diff loses mutation groups; rejecting an overflowing ordinary mutation violates availability.

## 8. Online snapshot, validation, and cutover

**Decision**: Under exclusive maintenance coordination, clone state/metadata and activate the recorder without a gap. Encode/synchronize/reopen a deterministic current-V2 staging segment outside exclusivity. At cutover, reacquire exclusivity, detach delta, capture current state, append regenerated V2 groups, synchronize/reopen/compare, freeze final source inventory, publish, and install a fresh writer/rotation state before release.

**Rationale**: Expensive work stays concurrent while exact comparisons prevent authority publication from hiding ordering or encoding defects.

**Alternatives considered**: Exclusive encoding blocks writes; initial-only source hashes reject legitimate WAL growth; unlocked delta application creates a gap; historical unversioned snapshot encoders write the wrong format.

## 9. Writer handoff and failure behavior

**Decision**: Make the writer privately detachable. Cutover verifies WAL health, drops the old file before Windows moves, publishes, reopens canonical replacement, and installs preserved durability/granularity plus fresh V2 offset/buffer/rotation state. RAII clears the matching recorder and attempt flag on success, error, or panic.

Pre-publication failure retains writable old authority. Proven rollback restores its writer. Cleanup-pending replacement stays writable. Indeterminate authority or authoritative replacement reopen failure keeps live reads but fails subsequent writes closed until reopen recovery.

**Rationale**: An old open handle is unsafe for Windows publication, and existing failed-closed WAL semantics already provide the correct external contract.

**Alternatives considered**: Keeping the handle open, reconstructing the public store, or deleting evidence from `Drop` after publication begins were rejected.

## 10. Windows physical durability

**Decision**: Add target-specific `windows-sys 0.61.2` with `Win32_Storage_FileSystem`. Keep all unsafe code in `src/durability/windows.rs`. Expose safe no-replace/replace-existing moves using `MoveFileExW`, always `MOVEFILE_WRITE_THROUGH`, plus `MOVEFILE_REPLACE_EXISTING` only when intended, never `MOVEFILE_COPY_ALLOWED`.

Use lossless `encode_wide`, reject interior NUL, append terminators, keep buffers alive, and capture `last_os_error()` immediately. Close owned handles before moves. Preflight actual target directory using disposable `create_new` artifacts: sentinel write plus `sync_all`, close, same-directory write-through move, replacement probe, reopen/validate, and safe cleanup. Failure maps to the existing content or directory-entry barrier error and never touches authoritative WALs.

**Rationale**: Standard Rust rename does not expose Windows write-through namespace flags, and filesystem capability must be proven where the store lives.

**Alternatives considered**: Standard rename in physical mode, silent fallback, broad Windows feature sets, and authoritative-artifact probing were rejected.

Primary references: [Microsoft `MoveFileExW`](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw), [Microsoft `FlushFileBuffers`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers), [Microsoft `CreateFileW`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew), and [windows-rs](https://github.com/microsoft/windows-rs).

## 11. Deterministic evidence and performance

**Decision**: Add private semantic scheduling hooks, lock-rank assertions (`Maintenance < Shard < WAL`), subprocess crash checkpoints, and full cross-family fault matrices. Before production hot-path edits, freeze a feature-specific baseline; compare three complete pinned quiet-host baseline/candidate runs cell-by-cell with five warmups and eleven samples.

The matrix covers every family, vector and file buffered storage, ordinary write/remove/minimal compute, one worker and eight distinct-key workers, at least 100 ms and 1,024 operations per sample. Record source digest, commit/dirty state, toolchain/OS/CPU/filesystem, command, affinity, and CSV checksum. Passing cells cannot offset failures.

**Rationale**: Semantic hooks prove interleavings and gate release for
file-backed stores that expose online maintenance. Vector stores remain in the
matrix as unchanged controls and deterministically prove they bypass file-only
coordination.

**Alternatives considered**: Historical baselines, timing-only tests, final-state-only checks, and aggregate ratios were rejected.
