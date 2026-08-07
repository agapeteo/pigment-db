# Feature Specification: Truncated WAL Recovery

**Feature Branch**: `not-created`

**Created**: 2026-08-06

**Status**: Draft

**Input**: User description: "Fix review issue #4: a normal crash that leaves a partial final record prevents the database from reopening."

## Clarifications

### Session 2026-08-06

- Q: How should startup handle an incomplete terminal record in a legacy WAL—one written before explicit logical-mutation boundaries existed? → A: Fail safely; automatically recover only newer boundary-aware WALs.
- Q: Should issue #4 store timestamp metadata now but leave full point-in-time startup and history retention to a separate feature? → A: Add timestamp metadata now with configurable granularity and a one-minute default; implement point-in-time startup and retention separately.
- Q: What integrity guarantee should the new WAL record format optimize for? → A: Keep SIMD CRC32 for fast, compact accidental-corruption detection, but cover the complete boundary-aware record rather than only its payload.
- Q: Should the added mutation boundaries, timestamps, and full-record CRC coverage use the same steady-state performance gate as issue #3? → A: Yes; require one-worker throughput of at least 90%, eight-worker throughput of at least 85%, and p95 latency no more than 125% of the matching baseline.
- Q: How should stored timestamp buckets behave when the system clock moves backward? → A: Keep buckets nondecreasing by clamping each new bucket to at least the previous accepted bucket; use accepted WAL sequence order within a bucket.
- Q: When repairing a torn V1 tail, must recovery preserve earlier accepted physical WAL records, or only their exact logical state? → A: Preserve every accepted logical effect; physical records may be replaced by an equivalent validated snapshot.
- Q: What should “no migration required” mean when a complete legacy database is opened for writing? → A: Reject legacy startup and require an external migration tool.
- Q: Who should provide the external migration tool required before a legacy database can open as V1? → A: This project provides and tests a standalone legacy-to-V1 migration CLI as part of this feature.
- Q: How should the migration CLI publish the converted V1 database? → A: Write to a new explicit destination, refuse overwrite, validate it fully, and never modify the legacy source.
- Q: Which header truncations should automatic startup recovery repair? → A: Recover partial action-record headers after a valid file header; reject partial or corrupt file headers unchanged.
- Q: If creating a brand-new V1 store is interrupted while writing its 40-byte header, what artifact should the next startup observe? → A: Publish the active file only after a complete staged header is written and validated; failure leaves no active file.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reopen the Last Complete State (Priority: P1)

As an application operator, I need a store whose final durable-history logical
mutation was interrupted to reopen at its last accepted logical-mutation
boundary, so a routine process crash does not make all earlier accepted data
unavailable.

**Why this priority**: Earlier accepted logical mutations remain the only
trustworthy state after an interrupted final write. Refusing to open that state
turns a single unfinished operation into a full database outage.

**Independent Test**: Populate each durable store family using a history with
explicit logical-mutation boundaries, cut the final record at every byte
position, reopen the store, and verify that the accepted prefix is available
while the interrupted operation is absent.

**Acceptance Scenarios**:

1. **Given** a boundary-aware history with a valid V1 file header and one or more accepted mutations followed by an incomplete action-record header, **When** the store reopens, **Then** it exposes exactly the accepted-prefix state and excludes the interrupted operation.
2. **Given** a boundary-aware history with one or more accepted mutations followed by an incomplete payload, **When** the store reopens, **Then** it exposes exactly the accepted-prefix state and excludes the interrupted operation.
3. **Given** a boundary-aware history with one or more accepted mutations followed by an incomplete footer, **When** the store reopens, **Then** it exposes exactly the accepted-prefix state and excludes the interrupted operation.
4. **Given** a boundary-aware history with no accepted mutations and only an incomplete first record, **When** the store reopens, **Then** it opens as an empty store and does not invent a value, member, or map entry.
5. **Given** a recovered store, **When** a caller performs a new mutation and reopens it again, **Then** the recovered prefix and new mutation are both present.
6. **Given** an interrupted multi-record logical mutation has one or more complete constituent records before its incomplete terminal record, **When** the store reopens, **Then** none of that unaccepted logical mutation is exposed and the preceding accepted state is preserved.

---

### User Story 2 - Reject Corruption Without Data Loss (Priority: P1)

As an application operator, I need startup to distinguish an interrupted tail
from corruption in a complete record, so automatic recovery never hides damage
or discards data merely to make a store open.

**Why this priority**: Treating arbitrary corruption as an unfinished write
could silently remove an accepted operation and make a damaged history appear
healthy.

**Independent Test**: Corrupt each validated field and payload in otherwise
complete records at the beginning, middle, and end of a history, attempt startup,
and verify that startup reports failure while retaining all recoverable artifacts.

**Acceptance Scenarios**:

1. **Given** a complete record with an invalid checksum, **When** startup validates the history, **Then** it reports corruption and does not classify the record as an interrupted tail.
2. **Given** an unsupported action, invalid recorded position, or invalid complete payload, **When** startup validates the history, **Then** it reports corruption and preserves the available artifacts for diagnosis or retry.
3. **Given** corruption before a later incomplete suffix, **When** startup validates the history, **Then** it reports the earlier corruption rather than recovering only the prefix before it.
4. **Given** storage refuses the required repair, **When** startup runs, **Then** the store does not become writable and the failure is reported without accepting further mutations.
5. **Given** a legacy history without explicit logical-mutation boundaries and an incomplete terminal record, **When** startup validates it, **Then** startup fails safely and preserves the legacy artifact rather than guessing a repair boundary.
6. **Given** a complete legacy history, **When** normal startup validates it, **Then** startup reports that explicit migration is required and preserves every legacy byte.
7. **Given** a complete legacy history and a new explicit destination, **When** an operator runs the project-provided migration CLI, **Then** it exclusively creates and fully validates a V1 database with identical logical state while leaving the legacy source unchanged.

---

### User Story 3 - Recovery Is Consistent and Repeatable (Priority: P2)

As a library user, I need terminal-record recovery to behave identically for
key/value, key/set, and key/sorted-map stores and across repeated restarts, so
the selected data model does not change crash safety.

**Why this priority**: All durable store families share the same persistence
promise. Repeatability prevents a successful first recovery from becoming a
later startup failure.

**Independent Test**: Run the same valid-prefix and interrupted-tail matrix for
all three durable store families, then reopen each recovered store three times
and compare every public value, membership result, map entry, and key-existence
result with the expected complete-prefix state.

**Acceptance Scenarios**:

1. **Given** equivalent interrupted boundary-aware histories for all durable store families, **When** they reopen, **Then** each family recovers the exact logical state of its accepted prefix.
2. **Given** a terminal tail was recovered once, **When** the store reopens three more times, **Then** every reopening produces the same state without repeating a visible repair.
3. **Given** both active and recovery artifacts from interrupted startup maintenance, **When** the store reopens, **Then** existing authority-selection rules protect the most complete trustworthy state before terminal-tail repair is considered.
4. **Given** automatic terminal-tail recovery, **When** a caller uses the error-reporting startup entry point, **Then** it receives a recovered outcome; the compatibility startup entry point records that recovery through its existing notification behavior.

### Edge Cases

- The history is empty and contains no tail.
- The first record is truncated after each possible byte, leaving no complete
  prefix.
- The 40-byte V1 file header is partial or corrupt and must be rejected unchanged
  rather than reconstructed or treated as an empty database.
- Creation of a brand-new V1 store is interrupted while writing its file header;
  the active path remains absent because only a complete validated header may be
  published. This does not authorize removing a partial header already observed
  at an existing active path.
- The last record is truncated at the boundary between its header, payload, and
  footer.
- The interrupted record represents deletion, replacement, collection creation,
  final-member removal, or one action in a multi-action logical mutation.
- A complete zero-length payload is valid for its action and must not be confused
  with truncation.
- Trailing bytes begin at a valid frame boundary but are too short to establish
  a complete record.
- A complete final record has a bad checksum or recorded position; it is
  corruption, not an interrupted tail.
- A corrupt complete record is followed by an incomplete tail.
- The available history ends exactly at a complete record boundary and requires
  no repair.
- Repair succeeds but later cleanup or reopening is interrupted.
- Repair cannot be completed because the storage medium returns an error.
- A recovery artifact contains more trustworthy complete data than an active
  artifact with a terminal fragment.
- A prior in-process rollback failed and the next process finds a valid complete
  prefix followed by an incomplete terminal record.
- A legacy history has a valid physical-record prefix followed by an incomplete
  terminal record but contains no explicit logical-mutation boundary.
- A complete legacy history is presented to normal startup before explicit
  migration to V1.
- Legacy migration is interrupted or its explicit destination already exists.
- The system wall clock moves backward before a mutation or between process
  restarts.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Startup MUST validate the selected authoritative artifact from byte zero according to its file grammar. For V1, after validating the complete file header at byte zero, startup MUST identify the longest contiguous sequence of complete, valid action records beginning immediately after that header.
- **FR-002**: Startup MUST recognize an incomplete terminal record only when its bytes begin immediately after that complete, valid prefix and do not contain enough bytes to form a structurally complete record.
- **FR-003**: After a complete valid V1 file header, startup MUST recover a boundary-aware history from an incomplete terminal record at every possible truncation point in its action-record header, payload, and footer. A partial or corrupt V1 file header already present at startup MUST instead be rejected and preserved unchanged. Creating a missing V1 store MUST make its active path visible only after the complete file header is written and validated; an interrupted creation MUST leave the active path absent.
- **FR-004**: Recovery MUST use exactly the logical state represented by the longest accepted logical-mutation prefix; a physical record boundary alone is insufficient when multiple records belong to one logical mutation.
- **FR-005**: Recovery MUST NOT apply any part of the incomplete terminal operation.
- **FR-006**: Recovery MUST exclude every constituent effect of an interrupted multi-record logical mutation, including constituent records that were completely written before its terminal fragment.
- **FR-007**: Recovery MUST preserve every logical effect of each earlier accepted mutation without omission, alteration, or duplication; it MAY replace their physical records with one equivalent validated snapshot through staged publication.
- **FR-008**: The persisted history MUST make accepted logical-mutation boundaries unambiguous for every newly written single-record and multi-record mutation that can be interrupted.
- **FR-009**: Before the store becomes writable, recovery MUST persistently remove the incomplete terminal bytes and every complete constituent record of the same unaccepted mutation according to the existing durability policy.
- **FR-010**: After successful recovery, newly accepted operations MUST follow the repaired prefix and survive subsequent reopening under the existing durability policy.
- **FR-011**: A structurally complete record with an invalid checksum, unsupported action, inconsistent recorded position, or invalid payload MUST be reported as corruption rather than treated as an interrupted tail.
- **FR-012**: Corruption within the complete-record region MUST prevent automatic tail recovery, even when incomplete bytes appear later.
- **FR-013**: Validation of malformed or truncated input MUST report a structured failure through the error-reporting startup entry point rather than terminate unexpectedly.
- **FR-014**: If persistent tail repair cannot be completed, startup MUST report failure, MUST NOT expose the store as writable, and MUST preserve every artifact not already affected by the failed storage operation.
- **FR-015**: Successful automatic tail recovery MUST be reported as a recovered startup outcome and MUST use the existing compatibility notification behavior.
- **FR-016**: Recovery behavior MUST be idempotent: repeated startup after a successful repair MUST produce the same logical state without further data removal.
- **FR-017**: All requirements MUST apply consistently to the key/value, key/set, and key/sorted-map durable store families.
- **FR-018**: Existing multi-artifact authority selection MUST take precedence over repairing a less trustworthy candidate; recovery MUST NOT choose a truncated active artifact when another artifact preserves a more complete authoritative state.
- **FR-019**: Normal startup MUST reject every complete legacy history without changing its bytes and report that explicit external migration is required; after migration to V1, the database MUST open with logical state identical to the complete legacy input. Truncated legacy histories remain non-migratable failures under FR-023.
- **FR-020**: Existing public startup signatures, successful-operation results, key-existence semantics, and panic-versus-error compatibility behavior MUST remain unchanged except for the explicit migration-required outcome when normal startup receives a legacy history.
- **FR-021**: Any persisted-representation change needed to identify logical mutation boundaries MUST be explicitly distinguishable from existing valid histories and MUST preserve the meanings of existing actions.
- **FR-022**: Recovery tests MUST cover every byte truncation boundary for every boundary-aware persisted action shape and every position within a multi-record logical mutation used by each affected store family.
- **FR-023**: Startup MUST NOT automatically shorten a truncated legacy history that lacks explicit logical-mutation boundaries; it MUST report failure and preserve the artifact for diagnosis or manual recovery.
- **FR-024**: Every newly written logical-mutation boundary MUST include a timestamp bucket that can support future time-based history selection.
- **FR-025**: Timestamp granularity MUST be configurable, and existing startup entry points MUST use a one-minute default without changing their signatures.
- **FR-026**: Selecting a non-default timestamp granularity MUST be available through an additive configuration path that does not break existing callers.
- **FR-027**: Timestamp metadata MUST remain unambiguous across single-record mutations, multi-record mutations, repaired tails, and compacted histories produced by this feature.
- **FR-028**: Each boundary-aware physical record MUST use a four-byte CRC32 integrity value covering its format version, action, declared length, payload, logical position, timestamp metadata, and mutation-boundary metadata while excluding the integrity field itself.
- **FR-029**: Any change to a CRC32-protected field in a structurally complete boundary-aware record MUST be classified as corruption rather than truncation.
- **FR-030**: Existing legacy records MUST retain their existing payload-only CRC32 validation semantics and MUST NOT be reinterpreted as boundary-aware records.
- **FR-031**: The integrity mechanism is intended to detect accidental corruption, not to authenticate records against an attacker capable of rewriting both data and integrity values.
- **FR-032**: Before production record-format behavior changes, the feature MUST capture a reproducible steady-state baseline using the same store-family, storage-mode, workload-profile, concurrency, warmup, and sampling methodology established for issue #3.
- **FR-033**: Every steady-state performance cell MUST pass independently against its matching baseline; results from faster cells MUST NOT offset a failing cell.
- **FR-034**: Each timestamp bucket MUST begin with the system wall-clock time rounded down to the configured granularity and MUST be clamped to no earlier than the previous accepted logical mutation's bucket.
- **FR-035**: Reopening MUST recover the last accepted timestamp bucket so the nondecreasing rule continues across process restarts.
- **FR-036**: Mutations sharing one timestamp bucket MUST remain deterministically ordered by their existing accepted WAL sequence; timestamp granularity MUST NOT replace mutation ordering.
- **FR-037**: The project MUST provide and test a standalone offline command-line tool that accepts a complete legacy source and an explicit new destination, exclusively creates and fully validates an equivalent V1 database, refuses to overwrite any destination, and never modifies the legacy source.

### Key Entities

- **Complete Record**: One persisted operation whose action, declared length,
  payload, integrity value, and recorded position are all present and valid.
- **Protected Record Envelope**: Every boundary-aware structural field, timestamp,
  mutation-boundary field, and payload covered together by one four-byte CRC32
  integrity value.
- **Recoverable Prefix**: For V1, the longest contiguous sequence of complete
  action records beginning immediately after the complete valid file header and
  ending at an accepted logical-mutation boundary whose logical result is safe
  to expose.
- **Incomplete Terminal Record**: Bytes beginning at the next record boundary
  after a recoverable prefix that do not contain a complete record.
- **Corrupt Record**: A structurally complete record, or an earlier record in the
  history, whose integrity, action, payload, or position is invalid.
- **Repair Boundary**: The accepted logical-mutation boundary represented by the
  recoverable prefix and used to derive an equivalent replacement snapshot. It
  identifies the logical state to preserve, not a required physical output
  offset or direct-truncation position.
- **Logical Mutation Boundary**: Evidence that every constituent record of one
  caller-visible mutation was accepted as a complete unit; recovery may expose
  state only through such a boundary.
- **Timestamp Bucket**: Time metadata attached to one logical mutation boundary,
  derived from rounded wall-clock time, clamped against the preceding accepted
  bucket, and ordered with that boundary's accepted WAL position.
- **Recovery Outcome**: The startup result distinguishing normal opening,
  successful automatic recovery, and an explicit unrecoverable error.
- **Migration CLI**: The project-provided offline command that validates a
  complete legacy source and produces an equivalent validated V1 database at a
  new explicit destination without modifying the source.

### Scope Boundaries

- This feature repairs incomplete terminal records already present when a store
  starts in boundary-aware histories; it does not promise automatic recovery of
  truncated legacy histories, arbitrary corruption, or missing bytes inside a
  complete-record region.
- This feature does not strengthen buffered completion into physical-storage
  synchronization; that durability-policy change remains review issue #5.
- This feature does not change successful short-write handling, record-size or
  offset width, or the meanings of existing action identifiers; those remain
  independent compatibility concerns. A distinguishable, backward-compatible
  representation of logical mutation boundaries is in scope if required for
  safe recovery.
- This feature retains CRC32 for accidental-corruption detection and does not add
  cryptographic hashing, record authentication, or integrity-key management.
- Normal library startup does not migrate legacy histories; operators must run
  the project-provided standalone migration CLI before a legacy database can
  open as V1.
- This feature does not change concurrent mutation ordering, callback contracts,
  pop return values, numeric overflow behavior, or key representations.
- This feature records timestamp metadata but does not retain historical versions,
  expose time-based startup, define point-in-time branching, or change historical
  compaction policy. Those capabilities require a separate feature.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For 100% of byte truncation positions across every boundary-aware persisted action shape and all three durable store families, startup exposes exactly the state of the accepted prefix without terminating unexpectedly.
- **SC-002**: Across the full truncation matrix, zero incomplete terminal operations appear partially or completely in public reads after recovery.
- **SC-003**: At every interruption position within a multi-record logical mutation, zero constituent effects of that unaccepted mutation appear after recovery, while 100% of the preceding accepted state remains.
- **SC-004**: Every recovered case produces identical public state across three consecutive reopenings, and a new post-recovery mutation remains visible on the third reopening.
- **SC-005**: For 100% of tested checksum, action, payload, and recorded-position corruptions in complete records, startup reports corruption and does not silently shorten the history.
- **SC-006**: For 100% of simulated repair failures, the store never becomes writable and no later mutation is accepted through that failed startup result.
- **SC-007**: For 100% of complete legacy histories and frozen fixtures, normal startup reports that migration is required and leaves every source byte unchanged.
- **SC-008**: A history containing 1,000,000 complete operations plus one interrupted terminal operation recovers in no more than 125% of the median startup time for the matching complete history on the same machine and storage mode, using at least 11 measured samples.
- **SC-009**: Every successful automatic tail repair is distinguishable from a normal startup through the existing recovery outcome or compatibility notification behavior.
- **SC-010**: For 100% of truncated legacy-history cases without explicit logical-mutation boundaries, startup reports failure and leaves the legacy artifact available rather than shortening it automatically.
- **SC-011**: Every newly accepted logical mutation contains exactly one timestamp bucket; unchanged callers use one-minute buckets, and each tested supported non-default granularity produces the configured bucket boundaries.
- **SC-012**: Changing any one protected structural, timestamp, boundary, or payload field in a complete boundary-aware record causes 100% of the corruption matrix to be rejected without shortening the history.
- **SC-013**: After explicit external migration, every frozen legacy fixture opens as V1 with the same logical replay result as its complete legacy input.
- **SC-014**: In every matching steady-state benchmark cell, one-worker median throughput is at least 90% of baseline, eight-worker median throughput is at least 85% of baseline, and p95 public-call latency is no more than 125% of baseline.
- **SC-015**: The final performance report contains every required baseline and candidate cell, records no threshold exceptions, and reports an overall pass only when every individual cell passes.
- **SC-016**: Across forward, equal, and backward wall-clock test sequences before and after reopening, 100% of accepted timestamp buckets are nondecreasing and no mutation is rejected solely because the clock moved backward.
- **SC-017**: For every tested set of mutations sharing a timestamp bucket, replay reproduces their accepted WAL sequence and final public state exactly across three consecutive reopenings.
- **SC-018**: For 100% of successful and failed migration attempts, the legacy source remains byte-for-byte unchanged; a successful destination passes complete V1 validation before normal startup, and a pre-existing or incomplete destination is never overwritten or reported as successful.

## Assumptions

- The bytes available when startup begins are evaluated under the project's
  existing durability policy; guaranteeing that acknowledged bytes reached
  physical media belongs to review issue #5.
- In a boundary-aware history, a terminal fragment immediately following a fully
  valid prefix represents an uncommitted logical mutation and may be excluded by
  staged publication of an equivalent snapshot at the last accepted logical
  boundary. A legacy history without such a boundary fails safely, and a
  structurally complete but invalid final record is corruption that may not be
  removed automatically.
- Existing issue #1 recovery rules remain authoritative when active, recovery,
  and staging artifacts coexist. Tail repair applies only after the trustworthy
  candidate has been selected.
- The store directory remains under single-process ownership during startup.
- Existing error-reporting and compatibility startup entry points remain
  available and retain their established caller-facing behavior except for the
  explicit migration-required legacy outcome.
- Timestamp metadata is a forward-compatible foundation only; this feature does
  not yet let callers open or restore a database at a specified historical time.
- Wall-clock values are interpreted independently of local time zone; accepted
  WAL sequence remains authoritative when multiple mutations share one bucket.
- Storage can report failure while inspecting or repairing the history; such a
  failure leaves the store unavailable for writes until a later successful
  startup or operator intervention.
