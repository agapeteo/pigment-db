# Feature Specification: Full-Range Signed I128 Keys

**Feature Branch**: `codex/010-fix-i128-key`

**Created**: 2026-08-08

**Status**: Approved

**Input**: Fix review Issue #10: make the public `I128` key represent the complete signed 128-bit range and provide an explicit persisted-data compatibility and migration path.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Use the full signed 128-bit key range (Priority: P1)

As a library user, I can use negative and positive signed 128-bit values, including both extrema, as sorted-map search-key components.

**Why this priority**: The current public key variant cannot represent most of the domain promised by its name and reported size.

**Independent Test**: Store entries at the signed minimum, negative one, zero, a value above the unsigned 64-bit maximum, and the signed maximum; then read and range-order them through the public sorted-map API.

**Acceptance Scenarios**:

1. **Given** any signed 128-bit value, **When** it is supplied as an `I128` key component, **Then** the value is accepted without narrowing or reinterpretation.
2. **Given** signed 128-bit key components spanning negative and positive values, **When** they are ordered or queried, **Then** their order follows signed numeric order.
3. **Given** an `I128` key component, **When** its logical byte size is reported, **Then** the result is 16 bytes.

---

### User Story 2 - Reopen signed keys exactly (Priority: P1)

As a user of a durable sorted-map store, I can close and reopen a store containing full-range signed 128-bit keys without changing their values, order, or associated entries.

**Why this priority**: A key that works only in live memory would violate the store's durability contract.

**Independent Test**: Persist boundary signed values, reopen the store repeatedly, and verify exact public reads and signed ordering after each reopen.

**Acceptance Scenarios**:

1. **Given** accepted entries keyed by signed 128-bit boundary values, **When** the store is reopened, **Then** every key and value is recovered exactly.
2. **Given** old and new key-record encodings in one valid V2 history, **When** the store is reopened, **Then** both are replayed in their original mutation order.
3. **Given** a corrupt or unknown key-record encoding, **When** startup validates it, **Then** startup fails explicitly without exposing partial state.

---

### User Story 3 - Preserve historical unsigned I128 data (Priority: P1)

As an operator, I can open or migrate historical records whose `I128` payload was stored as an unsigned 64-bit value, and each value is preserved as the numerically equal nonnegative signed 128-bit value.

**Why this priority**: Changing the payload width without an explicit compatibility rule would silently reinterpret or reject valid persisted data.

**Independent Test**: Replay immutable legacy, V1, and earlier V2 fixtures containing zero, the unsigned 64-bit maximum, put, remove, and mixed-history records; then migrate them and verify exact public state while proving source bytes are unchanged.

**Acceptance Scenarios**:

1. **Given** a historical `I128` payload in the inclusive range zero through the unsigned 64-bit maximum, **When** it is replayed, **Then** it becomes the numerically equal nonnegative signed 128-bit key.
2. **Given** a supported historical source, **When** offline migration succeeds, **Then** the destination uses the current signed-key encoding and the source remains byte-identical.
3. **Given** an earlier V2 history followed by current writes, **When** it is reopened or compacted, **Then** historical and current key records produce one equivalent logical state.

### Edge Cases

- Signed minimum and maximum values.
- Negative one, zero, and one.
- The unsigned 64-bit maximum and the next greater signed value.
- Composite search keys containing an `I128` component before or after another key type.
- Historical put followed by current remove, and current put following a historical remove.
- Mixed historical/current records across sealed and active V2 segments.
- A record identifier that is valid for another store family but invalid for a sorted map.
- Truncated, corrupt, or unknown-version key payloads.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The public `I128` key variant MUST accept and preserve every value in the signed 128-bit integer domain.
- **FR-002**: `I128` key comparison MUST follow signed numeric ordering, including within composite search keys.
- **FR-003**: `I128` key size accounting MUST remain 16 bytes.
- **FR-004**: New durable sorted-map records MUST identify the signed-key payload contract explicitly and MUST encode the complete signed 128-bit value without narrowing.
- **FR-005**: Legacy and V1 sorted-map records MUST retain their historical payload interpretation in which the `I128` variant contains an unsigned 64-bit value.
- **FR-006**: Earlier V2 sorted-map records using the historical payload interpretation MUST remain replayable alongside current signed-key records.
- **FR-007**: Historical unsigned `I128` values MUST normalize exactly to numerically equal nonnegative signed values; they MUST NOT be sign-extended, truncated, or reinterpreted.
- **FR-008**: Put and remove records MUST use the same version distinction and normalization rules.
- **FR-009**: Offline migration and V2 compaction MUST emit only the current signed-key record contract, preserve logical state, and leave every source artifact byte-identical.
- **FR-010**: Startup and migration MUST reject corrupt, truncated, unknown, or store-family-incompatible key-record encodings before exposing destination or live state.
- **FR-011**: All other public key variants, their comparison behavior, and their persisted interpretations MUST remain unchanged.
- **FR-012**: The accepted API correction from an unsigned payload parameter to a signed payload parameter MUST be documented as a source-level compatibility change; callers passing an unsigned variable MUST convert it explicitly.
- **FR-013**: Key/value and key/set durable stores MUST remain byte- and behavior-compatible because they do not serialize `SearchKey` values; their existing reopen suites MUST show no regression.

### Key Entities

- **Signed I128 Key**: A public sorted-map key component covering the complete signed 128-bit domain.
- **Historical I128 Key**: An immutable persisted key component whose payload covers only the unsigned 64-bit domain.
- **Key Record Contract**: The explicit record identifier that determines whether a sorted-map payload uses the historical or current key representation.
- **Normalized Search Key**: A public search key produced after historical values are widened exactly into the signed domain.
- **Mixed V2 History**: A valid ordered segment chain containing both historical and current sorted-map key-record contracts.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All five representative boundary classes—signed minimum, negative, zero, above unsigned 64-bit maximum, and signed maximum—round-trip exactly through public durable sorted-map operations.
- **SC-002**: Signed ordering is correct for 100% of the boundary and composite-key cases in the acceptance suite before and after three reopen cycles.
- **SC-003**: Every frozen historical fixture replays to the numerically identical nonnegative key, and every migrated source is byte-identical before and after conversion.
- **SC-004**: Histories covering historical put/current remove, current put/historical remove, and mixed records across segment boundaries recover one exact accepted state.
- **SC-005**: Corrupt, truncated, unknown, and wrong-family key-record cases fail explicitly in 100% of the defined validation cases.
- **SC-006**: The full relevant test suite, formatting check, documentation tests, and warning-denying static analysis complete with zero failures.

## Assumptions

- Review Issue #10 explicitly approves the source-level correction of the `I128` variant payload type; no alias retaining the misleading unsigned constructor is required.
- The historical unsigned payload has only nonnegative values, so widening it into the signed 128-bit domain is exact and cannot overflow.
- The V2 WAL record namespace can distinguish historical and current sorted-map payload contracts without changing unrelated record types or the segment header version.
- Generic serialized `Key` values outside pigment-db's managed WAL files do not carry enough format context for automatic migration; the compatibility guarantee in this feature applies to supported pigment-db persisted stores and the documented public type change applies to direct serialization consumers.
- No new production dependency, coordination primitive, unsafe code, or performance benchmark is required for this representation and compatibility correction.
