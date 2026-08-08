# Feature Specification: V2 WAL Segments

**Feature Branch**: `codex/009-v2-wal-segments`

**Created**: 2026-08-07

**Status**: Approved

**Input**: Fix review Issue #9: prevent WAL frame-size and offset overflow after 4 GiB, add bounded runtime rotation, preserve configurable timestamp metadata, and provide an explicit offline migration path.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Keep long-running stores recoverable (Priority: P1)

As a library user, I can continue writing after the cumulative durable history exceeds 4 GiB without offset wrap, panic, or replay following the wrong record boundary.

**Why this priority**: Wrapped record boundaries can make accepted data unrecoverable and are the core defect in review Issue #9.

**Independent Test**: Open a validated store at an offset above 4 GiB, append and reopen it, and verify the accepted logical state and record boundary remain exact.

**Acceptance Scenarios**:

1. **Given** a validated durable history whose cumulative offset exceeds 4 GiB, **When** a mutation is accepted, **Then** its full length and boundaries are represented without narrowing or wrap.
2. **Given** a mutation whose encoded size cannot be represented safely by the running platform, **When** it is submitted, **Then** the operation fails before any partial record is accepted.

---

### User Story 2 - Bound active WAL growth safely (Priority: P1)

As an operator, I can configure a target active-history size so the store rotates between complete logical mutations while retaining a replayable immutable segment chain.

**Why this priority**: Wider offsets remove wrap but do not bound an indefinitely growing active file or provide a practical long-running storage lifecycle.

**Independent Test**: Use a small target, write through multiple rotations, restart, and verify every accepted mutation replays in order from numbered immutable segments plus the active segment.

**Acceptance Scenarios**:

1. **Given** a mutation would cross the configured target, **When** an earlier complete mutation already exists in the active segment, **Then** rotation completes before the new mutation begins.
2. **Given** one mutation is larger than the target, **When** the active segment contains only its header, **Then** that mutation remains intact in one oversized segment and the next mutation rotates first.
3. **Given** a multi-record compute mutation, **When** rotation is needed, **Then** no member of the group is split across segments.
4. **Given** interruption during rotation or a torn final active mutation, **When** startup can prove one accepted chain, **Then** it restores that chain; otherwise it fails without deleting evidence.

---

### User Story 3 - Upgrade and compact offline (Priority: P2)

As an operator, I can migrate legacy or V1 data and compact segmented V2 data into a new V2 destination without modifying the source.

**Why this priority**: A versioned format needs an explicit, recoverable compatibility boundary rather than silently reinterpreting existing bytes.

**Independent Test**: Run the migration command against frozen legacy, complete V1, recoverable V1-tail, and segmented V2 sources; open the output and compare source bytes before and after.

**Acceptance Scenarios**:

1. **Given** a complete legacy or V1 source, **When** migration succeeds, **Then** the new destination contains equivalent V2 state and every source artifact is byte-identical.
2. **Given** a V1 source with only a recoverable terminal tail, **When** migration succeeds, **Then** output represents its last complete logical prefix.
3. **Given** a valid segmented V2 source, **When** offline compaction succeeds, **Then** output is one valid V2 active segment with equivalent state.
4. **Given** a destination already exists or the source changes during conversion, **When** migration detects it, **Then** it fails without overwriting the destination or source.

---

### User Story 4 - Preserve timestamp configuration (Priority: P2)

As a library user, I can choose timestamp granularity while the default remains one minute, and later openings inherit the active segment's persisted setting unless I explicitly request a change.

**Why this priority**: Timestamp metadata is part of the new persisted contract and must remain monotonic across rotation and migration for future time-based startup capabilities.

**Independent Test**: Write with one granularity, explicitly change it, rotate, reopen with unrelated options, rotate again, and verify the active setting and last accepted timestamp do not regress.

**Acceptance Scenarios**:

1. **Given** no explicit timestamp option, **When** a new store is created, **Then** it uses one-minute granularity.
2. **Given** an existing V2 store and an explicit different granularity, **When** the next mutation is accepted, **Then** rotation publishes the new setting without rewriting an immutable segment.
3. **Given** an existing store and options that do not set timestamp granularity, **When** it reopens, **Then** the persisted active setting is retained.
4. **Given** offline migration or compaction, **When** output is created, **Then** the last accepted timestamp bucket is preserved.

### Edge Cases

- The configured segment target is zero.
- One complete logical mutation is larger than the segment target.
- A crash leaves only sealed segments plus a complete next-segment staging artifact.
- A crash occurs exactly between complete members of a multi-record group.
- The final active record or group is torn while earlier sealed segments are valid.
- Segment identifiers, segment bases, record lengths, or cumulative offsets overflow.
- Segment numbers are missing, duplicated, malformed, or have inconsistent base offsets.
- A V1 header is corrupt, partial, or belongs to another store family.
- A migration source gains, loses, or changes an artifact during conversion.
- Cleanup of an obsolete recovery or staging artifact fails after authority is established.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: New file-backed stores MUST use a versioned format capable of representing record lengths, physical offsets, mutation offsets, and cumulative segment bases beyond 4 GiB.
- **FR-002**: Length and offset arithmetic MUST be checked before bytes are accepted; overflow MUST return an error without advancing durable or live state.
- **FR-003**: Record integrity MUST retain a checksum and must fail closed for protected-field, payload, footer, or checksum corruption.
- **FR-004**: The public segment target MUST reject zero and default to 1 GiB.
- **FR-005**: Rotation MUST occur only between complete logical mutations and MUST never split a compute group.
- **FR-006**: An oversized mutation MUST remain intact in one segment, even when it exceeds the configured target.
- **FR-007**: Sealed segments MUST be immutable, deterministically numbered, and retained until explicit offline compaction.
- **FR-008**: Startup MUST validate segment ordering, identifiers, base offsets, store family, and record boundaries before exposing state.
- **FR-009**: Startup MUST recover a complete interrupted-rotation staging segment when it is the provable next segment and the active segment is absent.
- **FR-010**: Startup MUST discard only a recoverable torn final mutation or entire torn final group, including when earlier sealed segments exist.
- **FR-011**: Startup MUST preserve artifacts and return a structured error when authority cannot be proven.
- **FR-012**: Complete or recoverable V1 startup input MUST require the offline migration command; corrupt, partial, or wrong-family V1 input MUST remain invalid.
- **FR-013**: Offline conversion MUST accept frozen legacy, complete V1, recoverable V1 terminal-tail, complete V2, and segmented V2 sources.
- **FR-014**: Offline conversion MUST always emit V2, require a nonexistent destination, preserve source artifacts byte-for-byte, and reject a source that changes during conversion.
- **FR-015**: Offline V2 compaction MUST emit one active segment with state equivalent to the complete source chain.
- **FR-016**: Timestamp granularity MUST be configurable, nonzero, persisted per segment, and default to one minute for new stores.
- **FR-017**: Options unrelated to timestamps MUST NOT reset an existing persisted timestamp granularity.
- **FR-018**: An explicit granularity change MUST take effect through the next immutable-segment rotation rather than rewriting an existing segment during open.
- **FR-019**: Rotation and migration MUST preserve the last accepted timestamp bucket so subsequent accepted timestamps never move backward.
- **FR-020**: All requirements MUST apply consistently to key/value, key/set, and key/sorted-map file-backed stores.
- **FR-021**: Point-in-time startup, automatic online deletion of sealed segments, cross-process coordination, and changing the checksum algorithm are outside this feature.

### Key Entities

- **V2 Segment**: One immutable or active portion of durable history, identified by store family, segment number, cumulative base, timestamp settings, and integrity metadata.
- **Logical Mutation**: One atomic accepted operation, represented by one record or an ordered multi-record group that cannot cross a segment boundary.
- **Segment Chain**: Consecutive sealed segments followed by one active segment whose identifiers and bases form one replay authority.
- **Migration Source Capture**: The complete ordered set of source artifacts and bytes used to prove offline input stability.
- **Timestamp State**: Persisted granularity and last accepted bucket carried across reopen, rotation, repair, and migration.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A validated handoff above 4 GiB preserves its exact offset and accepts no narrowing to 32 bits.
- **SC-002**: Every cut point of each record type and multi-record group either recovers the exact accepted prefix or fails closed for corruption; no cut exposes partial logical state.
- **SC-003**: After at least two rotations, three consecutive reopen cycles return the same complete logical state for all three store families.
- **SC-004**: Every supported migration class produces an openable V2 destination while source files remain byte-identical; an existing destination is never overwritten.
- **SC-005**: Under a matched quiet-machine workload that does not trigger rotation, candidate median write throughput is at least 90% of the pre-feature baseline and no paired run is below 85%.
- **SC-006**: The full relevant test suite, formatting check, and warning-denying static analysis complete with zero failures.

## Assumptions

- One process owns a store directory while it is open; cross-process writers remain out of scope.
- Segment-size configuration is runtime-only and is supplied on each open where a non-default target is desired.
- Sealed segments are deliberately retained online; reclaiming them requires the explicit offline migration/compaction command.
- Existing CRC32 integrity remains sufficient for accidental-corruption detection; cryptographic authentication is not introduced.
- Point-in-time startup will use the timestamp metadata in a future feature and is not exposed here.
