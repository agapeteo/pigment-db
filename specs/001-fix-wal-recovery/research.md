# Research: Crash-Safe WAL Recovery

## Decision 1: Preserve the active WAL until publication

**Decision**: Build each compacted replacement in a same-directory staging file with atomic exclusive creation. Replay the completed staging file to validate its logical contents, synchronize the closed staging file, and then publish it with one replacement rename over the active WAL. A staging file is never authoritative during recovery; if startup finds an active WAL plus staging, it keeps active and discards or rebuilds staging.

**Rationale**: The current failure begins by renaming away the only complete WAL before its replacement exists. Keeping active untouched means interruption before publication leaves the original source intact; interruption after the rename sees the already-complete replacement at the active name. Same-directory staging avoids cross-filesystem rename failures. Closing mappings and file handles before rename supports Windows as well as Unix-like systems.

**Alternatives considered**:

- Rename active to backup, publish staging, and persist a recovery journal: rejected for new transactions because it creates extra authority states and cleanup hazards that a single replacement rename avoids.
- Promote a complete-looking staging file after restart: rejected because retrying from active is safer and a frame-complete stage may still be an incomplete logical snapshot.
- Stop compacting on startup: rejected because it changes current WAL growth behavior rather than repairing publication.

**References**: [Rust `rename`](https://doc.rust-lang.org/std/fs/fn.rename.html), [`OpenOptions::create_new`](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new), [`File::sync_all`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all).

## Decision 2: Classify artifacts by role, not timestamps or size

**Decision**: Recognize per-store active, legacy recovery, and new staging names. An active WAL alone is authoritative, including a zero-length active WAL representing an intentional empty store. Staging is always non-authoritative. Legacy active/recovery pairs are classified by checked logical replay and provenance rules; size, modification time, and entry count are never authority signals.

**Rationale**: Deletes and overwrites can make newer state smaller, and an interrupted compacted snapshot can end at a valid frame boundary. Artifact names plus replay provenance encode the only trustworthy history available without changing the existing WAL format.

**Alternatives considered**:

- Prefer the newest timestamp: rejected because filesystem timestamp resolution and clock behavior do not prove write order.
- Prefer the larger file or the state with more entries: rejected because valid later mutations can reduce either measure.
- Add a new WAL header/footer or generation manifest: rejected for this feature because the simpler active-preserving protocol needs no new-format metadata, and backward readability is an explicit constraint.

## Decision 3: Resolve legacy hidden backups with replay provenance

**Decision**: For the pre-feature hidden recovery file, replay both candidates frame-by-frame:

1. Recovery only, or recovery plus empty/truncated active: use recovery.
2. Active and recovery with equal logical contents: use active.
3. Active reaches the recovery state at any frame boundary and then continues: use active; replay completed and later mutations followed.
4. Active contains only a valid compacted-snapshot prefix that is a proper subset of recovery: use recovery; replay was interrupted.
5. Otherwise: return `AuthorityUndetermined` and preserve all candidates byte-for-byte.

If active is proven newer but the stale legacy artifact cannot be removed, open active without compacting it. This retains the prefix evidence for the next startup. Once legacy cleanup succeeds, normal safe compaction may resume.

**Rationale**: Existing compaction writes exactly one snapshot action per live entry. A completed replay necessarily reaches the recovery state at a frame boundary before any later writes. This distinguishes completed replay plus later deletes/updates from a frame-complete partial replay.

**Alternatives considered**:

- Always use recovery: rejected because cleanup may have failed after a completed replay and active may contain later acknowledged writes.
- Always use parse-valid active: rejected because a partial snapshot may be parse-valid.
- Automatically merge candidates: rejected because merge order cannot be recovered safely.

## Decision 4: Add checked replay for classification, not tail repair

**Decision**: Introduce a checked frame iterator and store-specific replay adapters that return structured validation errors rather than slicing, decoding, or CRC panics. Full-artifact validation and frame-boundary logical snapshots are used only to classify startup artifacts and prove legacy provenance.

**Rationale**: A fallible initializer cannot safely call the current panic-based readers. Classification also needs intermediate logical states. General repair of a partial normal-mutation tail or interior corruption remains outside this feature.

**Alternatives considered**:

- Catch panics from existing readers: rejected because panic payloads are not a stable error contract and bounds panics obscure the failing artifact.
- Expand scope to truncate partial tails: rejected because that is review issue #4 and has different data-loss decisions.

## Decision 5: Share recovery infrastructure across store types

**Decision**: Add public recovery result/error types in `src/recovery.rs`; add checked replay and the artifact state machine under `src/wal/`; keep store-specific codecs/adapters in the three store modules. The state machine accepts store paths plus callbacks or a small internal trait for replaying and writing a logical snapshot.

**Rationale**: Filesystem transitions are identical for key/value, key/set, and key/sorted-map stores, while their logical replay types differ. One coordinator prevents behavioral drift without forcing their data models into one type.

**Alternatives considered**:

- Duplicate the corrected startup sequence three times: rejected because it recreates the current maintenance and consistency risk.
- Make all stores implement a large public trait: rejected because recovery polymorphism is internal and should not expand the public API.

## Decision 6: Preserve the initializer and add a structured fallible path

**Decision**: Each file-backed store gains `try_init_new`, returning `Result<RecoveryOutcome<Self>, RecoveryError>`. `RecoveryOutcome` exposes `Normal` or `Recovered`. Existing `init_new(&str) -> Self` remains unchanged, delegates to the fallible path, logs successful automatic recovery, and retains its historical panic-on-failure behavior.

**Rationale**: This directly implements the accepted clarifications, provides a non-panicking path for new callers, and does not break current source compatibility. The vector-backed constructors do not participate in filesystem recovery and remain unchanged.

**Alternatives considered**:

- Change `init_new` to return `Result`: rejected as a breaking API change contrary to the clarification.
- Return `(Self, RecoveryStatus)`: rejected because a named outcome is clearer and can evolve without exposing tuple layout.
- Separate outcome/error types for each store: rejected as unnecessary duplication.

## Decision 7: Test with real files and deterministic interruption fixtures

**Decision**: Add `tempfile` as a development-only dependency. Use one `TempDir` per test, public fallible initializers for assertions, table-driven on-disk fixtures for every artifact state, and a narrow internal recovery observer/fault point only where needed to prove production transitions leave the expected artifacts. Compare logical store contents; compare raw bytes only for conflict-preservation assertions.

**Rationale**: Real files exercise create, sync, rename, cleanup, mmap closure, and reopen behavior. Deterministic checkpoint errors leave artifacts like a terminated process without poisoning the test process. Temporary directories keep renames on one filesystem and isolate parallel cases.

**Alternatives considered**:

- Mock the entire filesystem: rejected because it would not exercise the behavior being fixed.
- Use shared names under the system temp directory: rejected because parallel tests can collide.
- Spawn/abort a subprocess at every checkpoint: rejected as slow and difficult to diagnose; one optional abort smoke test may supplement deterministic coverage.

**Reference**: [`tempfile::TempDir`](https://docs.rs/tempfile/latest/tempfile/struct.TempDir.html).

## Decision 8: Keep power-loss durability and other review findings out of scope

**Decision**: Synchronize the newly created staging file before publication, but do not claim portable directory-fsync or strengthen every normal WAL append in this feature. Do not address concurrent mutation ordering, set/map callback persistence, offset width, or general corruption repair.

**Rationale**: The specification targets process interruption during startup maintenance. Power-loss durability, short writes, mutation ordering, and partial normal-write recovery are independent review findings with broader API and format implications.

**Alternatives considered**:

- Bundle all WAL durability fixes: rejected because it prevents a focused, independently testable tracer-bullet delivery.
