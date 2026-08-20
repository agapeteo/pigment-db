# Feature Specification: Current-Format Compaction and Windows Physical Durability

**Feature Branch**: `codex/008-current-compaction-durability`

**Created**: 2026-08-19

**Status**: Draft

**Input**: Add caller-triggered storage statistics, closed and online current-format compaction, crash-safe compaction recovery, and strict Windows physical durability for all durable store families while keeping legacy conversion exclusively in `pigment-db-migrate`.

## Clarifications

### Session 2026-08-19

- Q: If closed compaction is called while a store from that directory remains open in the same process, what must the library do? → A: Detect the open store and return `FailedClosed` before creating or changing any artifact; callers remain responsible for other processes.
- Q: If two callers invoke online compaction concurrently on the same store instance, how should the second call behave? → A: Reject the second attempt immediately with `FailedClosed`; the first attempt and ordinary reads and writes continue.
- Q: During recovery from `PreviousPublished`, when both the previous generation and a fully validated replacement are available, which generation should recovery select? → A: Prefer the fully validated replacement; if it cannot be validated, restore the verified previous generation.
- Q: After online compaction publishes the replacement but obsolete-artifact cleanup remains pending, should the open store continue accepting writes, and when should cleanup be retried? → A: Continue reads and writes; retry cleanup on reopen or the next explicit compaction.
- Q: When maintenance artifacts exist but the manifest is missing or corrupt, how should the library distinguish `AuthorityUndetermined` from `InvalidArtifact`? → A: Use `AuthorityUndetermined` when evidence prevents proving one authoritative generation; otherwise use `InvalidArtifact`.
- Q: How should inspection and compaction handle an unexpected non-Pigment entry inside the store directory? → A: Reject it as `InvalidArtifact` without changing any entry; closed compaction must never ignore, copy, move, or delete an unrecognized entry.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Inspect Current Storage Without Mutation (Priority: P1)

As a library consumer, I can measure the current on-disk footprint and segment count of a database or an open store family without changing any artifact or recovery state.

**Why this priority**: Operators need trustworthy measurements before deciding whether maintenance is worthwhile, and inspection is the smallest independently useful part of the feature.

**Independent Test**: Inspect an empty directory, each individual current-format family, and a directory containing all three families; compare the reported counts and byte totals with the canonical artifacts and prove every directory entry and byte remains unchanged.

**Acceptance Scenarios**:

1. **Given** a directory containing valid current-format active and sealed segments, **When** a caller inspects storage, **Then** each discovered family reports exact active, sealed, and total byte counts plus its sealed-segment count.
2. **Given** a valid unsegmented current-format family, **When** it is inspected, **Then** sealed bytes and sealed-segment count are both zero.
3. **Given** an empty directory, **When** it is inspected, **Then** the result contains no families and a total of zero without creating any artifact.
4. **Given** an older recognized format, **When** it is inspected, **Then** the caller receives migration-required guidance naming the affected path and the external migration tool.
5. **Given** corrupt, incomplete, incorrectly named, wrong-family, unrecognized, unexpected, or authority-ambiguous evidence, **When** it is inspected, **Then** inspection fails explicitly and preserves every directory entry byte-for-byte.

---

### User Story 2 - Compact a Closed Database In Place (Priority: P1)

As a desktop application, I can compact a database that I have closed, in place, and recover space without managing a destination directory or risking the last complete database state.

**Why this priority**: Closed compaction provides the safest production maintenance path and establishes the publication and recovery protocol needed by online compaction.

**Independent Test**: Create segmented current-format data for all three families, close every instance, compact the directory, and verify one active segment per family, exact state through three reopenings, preserved timestamps, and safe repeated compaction.

**Acceptance Scenarios**:

1. **Given** a closed directory with one or more valid current-format families, **When** compaction succeeds, **Then** each family has one current-format active segment, exact logical state, preserved timestamp metadata, and no sealed segments.
2. **Given** a current-format terminal tail already classified as safely recoverable by normal recovery rules, **When** closed compaction runs, **Then** the accepted complete prefix is compacted without accepting incomplete state.
3. **Given** any source artifact addition, removal, rename, length change, or content change after capture, **When** publication is attempted, **Then** publication is rejected and the original database remains authoritative.
4. **Given** interruption at any publication phase, **When** the directory is next opened or compacted, **Then** exactly one complete old or new generation is selected, or the operation fails closed while preserving ambiguous evidence.
5. **Given** replacement authority is established but obsolete-artifact cleanup fails, **When** compaction returns, **Then** it reports successful publication with cleanup pending and a later retry can complete cleanup safely.
6. **Given** a successfully compacted directory, **When** compaction is repeated without intervening mutations, **Then** logical state is unchanged and the repeated operation succeeds safely.

---

### User Story 3 - Compact an Open Store While Work Continues (Priority: P1)

As a long-running application, I can explicitly compact one open store family while reads remain available and mutations continue during the disk-heavy staging work.

**Why this priority**: Long-running applications cannot routinely close their stores, but must retain the existing concurrency and durability guarantees during maintenance.

**Independent Test**: Pause staging construction after a consistent snapshot, perform reads and ordered concurrent mutations, resume compaction, and verify that every accepted mutation appears exactly once in acceptance order after cutover and three reopenings.

**Acceptance Scenarios**:

1. **Given** online compaction is encoding or validating staging data, **When** callers read or mutate the open family, **Then** reads make progress and writes can complete against the original authoritative log.
2. **Given** accepted concurrent mutations involving the same key, independent keys, compute groups, removal, and recreation, **When** cutover succeeds, **Then** the compacted store contains each mutation exactly once in durable acceptance order.
3. **Given** rejected, rolled-back, cancelled, or no-op work, **When** online compaction cuts over, **Then** that work is absent from the concurrent delta and from reopened state.
4. **Given** the encoded concurrent delta exceeds the caller's configured limit, **When** cutover is reached, **Then** staging is abandoned, the limit error is returned, and the original store remains authoritative, writable, and recoverable.
5. **Given** a pre-publication staging or validation failure, **When** online compaction returns, **Then** the original store remains authoritative and subsequent operations proceed normally.
6. **Given** publication authority becomes indeterminate, **When** the operation fails, **Then** current readable state stays available, subsequent writes are rejected, all evidence is preserved, and reopening is required before mutation resumes.
7. **Given** one family is being compacted online, **When** another family or unrelated store instance is used, **Then** it remains independent and does not acquire the compacting store's maintenance coordination.

---

### User Story 4 - Request Physical Durability on Windows (Priority: P1)

As a Windows application, I can request physical durability and receive either the same explicit acknowledgement contract available on other supported platforms or a structured refusal before the store is exposed.

**Why this priority**: Returning success without durable file contents and namespace publication would violate the library's strongest durability contract, while unconditional rejection prevents Windows production use.

**Independent Test**: On supported and fault-injected Windows filesystems, construct and reopen stores for every family, exercise mutations, rotation, recovery, and both compaction modes, then verify acknowledged state survives the durability fault model and unavailable barriers cause a structured refusal without fallback.

**Acceptance Scenarios**:

1. **Given** the target filesystem supports required content and namespace barriers, **When** physical durability is requested, **Then** construction succeeds only after disposable same-directory preflight proves both barriers.
2. **Given** either required barrier cannot be established, **When** physical durability is requested, **Then** the request fails with `RequiredBarrierUnavailable` and is never downgraded to buffered durability.
3. **Given** an acknowledged physical mutation or maintenance publication, **When** the durability fault model is applied, **Then** the complete acknowledged representation and authoritative namespace transition survive.
4. **Given** a content synchronization or write-through namespace failure, **When** an operation is in progress, **Then** the existing rollback or failed-closed contract applies and incomplete live state is not published.
5. **Given** Unicode or supported long paths, **When** physical construction, mutation, rotation, recovery, or compaction runs, **Then** path identity is preserved without lossy conversion.
6. **Given** buffered durability on Windows, **When** existing operations run, **Then** their established compatibility and behavior remain unchanged.

---

### User Story 5 - Keep Legacy Conversion Explicit (Priority: P2)

As an operator with an older Pigment DB database, I receive clear external-migration guidance instead of an implicit runtime conversion or a partially compacted database.

**Why this priority**: Explicit format boundaries prevent maintenance from silently becoming a compatibility layer and protect immutable historical behavior.

**Independent Test**: Present every recognized older frozen fixture to runtime opening, inspection, and compaction; verify the same migration-required classification and unchanged source bytes, then confirm existing external migration tests and fixtures remain unchanged.

**Acceptance Scenarios**:

1. **Given** an older recognized format, **When** runtime opening, inspection, or compaction encounters it, **Then** the operation returns `MigrationRequired` with the affected path and external-tool guidance.
2. **Given** an older recognized format, **When** maintenance rejects it, **Then** no automatic conversion, repair, rename, truncation, staging publication, or public format-version value occurs.
3. **Given** existing frozen migration fixtures, **When** the feature's compatibility suite runs, **Then** fixture bytes and established external migration outcomes remain unchanged.

### Edge Cases

- An empty directory and a directory containing only one of the three store families.
- Active-only storage, multiple contiguous sealed segments, a missing middle segment, and a missing leading segment.
- A safely recoverable terminal current-format tail versus corruption before the terminal tail.
- A canonical filename containing wrong-family data, a malformed segment name, any unexpected directory entry, or incomplete compaction evidence; every unexpected entry is rejected without mutation.
- A source file that keeps the same length but changes bytes between capture and publication.
- Source additions, removals, and renames during closed compaction.
- Interruption before or after each manifest phase becomes durable.
- Missing, corrupt, contradictory, stale, or checksummed-but-inconsistent manifest evidence.
- A `PreviousPublished` recovery with both a verified previous generation and a fully validated replacement present.
- Cleanup failure for one obsolete artifact while other evidence remains necessary for recovery.
- Repeated recovery or compaction after cleanup was deferred.
- Same-key and independent-key concurrent mutations during online staging.
- Put/remove, delete/recreate, compute/ordinary, and multi-change compute overlap.
- A no-op compute, rejected log write, failed rollback, panic, or cancelled asynchronous compute while delta recording is active.
- A delta exactly at the configured limit and one encoded mutation beyond it.
- A staging image that reopens successfully but does not match captured or current logical state.
- Publication failure after replacement namespace movement but before authority confirmation.
- Concurrent use of unrelated store families and unrelated store instances.
- Windows preflight failure, content-barrier failure, write-through move failure, open-handle conflict, Unicode path, and supported long path.
- A physical-durability cleanup failure after authoritative publication.
- Online cleanup pending while the authoritative replacement continues serving reads and writes until cleanup is retried on reopen or explicit compaction.
- Closed compaction requested while one or more same-process store instances for the target directory remain open.
- Two concurrent online-compaction requests for the same store instance.

## Requirements *(mandatory)*

### Functional Requirements

#### Compatibility and Scope

- **FR-001**: Runtime opening, inspection, and compaction MUST accept only the current Pigment DB format plus a current-format terminal tail already classified as safely recoverable by normal recovery rules.
- **FR-002**: A recognized older format MUST produce `MigrationRequired` with the affected path and guidance to use `pigment-db-migrate`.
- **FR-003**: Corrupt, incomplete, wrong-family, unrecognized, unexpected, or malformed data in the store directory MUST produce `InvalidArtifact` when it cannot plausibly identify a competing complete generation; no unexpected entry may be ignored or removed.
- **FR-004**: Missing, corrupt, or contradictory maintenance metadata MUST produce `AuthorityUndetermined` whenever the available evidence prevents proving exactly one authoritative generation; the library MUST preserve all available evidence and MUST NOT guess through the conflict.
- **FR-005**: Runtime maintenance MUST NOT invoke the migration engine, decode legacy application data for conversion, or perform automatic format conversion.
- **FR-006**: Compaction MUST write the same current database format it reads and MUST NOT change the current database record format.
- **FR-007**: The public maintenance API MUST NOT expose a database format-version enumeration or other internal format-version identifier.
- **FR-008**: Existing external migration behavior and frozen fixtures MUST remain unchanged.
- **FR-009**: Inspection and compaction MUST apply equivalently to key/value, key/set, and key/sorted-map families.
- **FR-010**: Maintenance MUST remain explicitly caller-triggered; the library MUST NOT schedule background compaction or choose application-specific thresholds.

#### Public Maintenance Contract

- **FR-011**: The public API MUST identify store families as `KeyValue`, `KeySet`, or `KeyMap` through a future-extensible `StoreFamily` value.
- **FR-012**: Per-family statistics MUST expose the family, active bytes, sealed-segment bytes, sealed-segment count, and total bytes.
- **FR-013**: Directory statistics MUST expose the discovered per-family statistics and their aggregate total bytes.
- **FR-014**: Closed-compaction options MUST expose a durability policy and default to buffered durability unless physical durability is explicitly selected.
- **FR-015**: Online-compaction options MUST expose a maximum encoded delta size and default that limit to 8 MiB.
- **FR-016**: Cleanup status MUST distinguish complete cleanup from pending cleanup.
- **FR-017**: Per-family compaction outcomes MUST expose the family, bytes before and after, sealed segments removed, concurrent mutations replayed, and cleanup status.
- **FR-018**: Directory compaction outcomes MUST expose one outcome for each compacted family.
- **FR-019**: Compaction errors MUST distinguish migration required, invalid artifact, authority undetermined, concurrent delta overflow, unsupported durability, operation-specific I/O failure, and failed-closed state.
- **FR-020**: Operation-specific I/O errors MUST identify the affected path and one of these future-extensible operation categories: inspection, capture, staging write, staging validation, manifest write, previous publication, replacement publication, replacement reopen, or cleanup.
- **FR-021**: All newly exposed maintenance types MUST be public, documented, future-extensible, and provide getters and option builders consistent with the existing public API style.
- **FR-022**: The library MUST provide directory-level inspection and closed in-place compaction operations matching the supplied `inspect_storage` and `compact_directory_in_place` contracts.
- **FR-023**: Every file-backed store family MUST provide per-family `storage_stats` and `try_compact_online` operations matching the supplied contracts.
- **FR-024**: Online compaction MUST inherit the durability policy of the already-open store and MUST NOT accept a policy downgrade or independent override.

#### Storage Inspection

- **FR-025**: Directory inspection MUST discover every current-format family represented by canonical active or sealed-segment artifacts.
- **FR-026**: Inspection MUST validate that every store-directory entry is a canonical current-format database artifact or recognized maintenance artifact, then validate active names, sealed-segment names, segment continuity, family identity, record integrity, and maintenance authority before reporting statistics.
- **FR-027**: Reported byte counts MUST equal the lengths of authoritative canonical current-format active and sealed segments; temporary maintenance metadata and obsolete generations MUST NOT be counted as active storage.
- **FR-028**: An unsegmented family MUST report zero sealed bytes and zero sealed segments.
- **FR-029**: An empty directory MUST report an empty family list and zero total bytes.
- **FR-030**: Directory and per-family totals MUST be arithmetically consistent and checked for overflow before returning success.
- **FR-031**: Inspection MUST NOT repair, recover, create, rename, delete, truncate, synchronize, or otherwise modify any filesystem object or recovery state.
- **FR-032**: Per-store statistics MUST describe only the open family's current authoritative generation.

#### Closed In-Place Compaction

- **FR-033**: Closed compaction MUST detect any same-process store instance still open for the target directory and return `FailedClosed` before creating or changing any filesystem artifact; callers remain responsible for excluding other processes, and the single-owning-process contract remains unchanged.
- **FR-034**: An empty directory MUST compact successfully as a no-op with an empty family outcome and without creating database artifacts.
- **FR-035**: Closed compaction MUST capture and replay every discovered current-format family into exactly one active current-format segment per family.
- **FR-036**: The replacement MUST preserve exact logical state, family identity, timestamp granularity, and the last accepted timestamp bucket for each family.
- **FR-037**: Replacement construction MUST occur in a new same-parent staging location and MUST leave the source authoritative before publication.
- **FR-038**: Before publication, the complete staging database MUST reopen successfully and its public logical state and metadata MUST match the captured source.
- **FR-039**: Immediately before publication, compaction MUST re-read the source inventory, lengths, and contents and reject every addition, removal, rename, length change, or byte change.
- **FR-040**: Publication MUST transition through recoverable old and replacement generations without deleting the last complete authoritative database.
- **FR-041**: Obsolete source storage MUST be removed only after replacement authority is established.
- **FR-042**: Cleanup failure after successful publication MUST return a successful family outcome with `CleanupStatus::Pending`, retain sufficient recovery evidence, and MUST NOT report publication failure.
- **FR-043**: Repeated compaction and cleanup retry MUST be safe, converge on one current generation, and preserve logical state.

#### Compaction Manifest and Recovery

- **FR-044**: Compaction MUST use one small, versioned, checksummed maintenance manifest that exists only during active compaction or deferred cleanup and does not contain application data.
- **FR-045**: The manifest MUST identify its version, directory or family scope, current phase, source artifact names with lengths and checksums, staging location, previous-generation location, and requested durability policy.
- **FR-046**: The manifest MUST distinguish `Prepared`, `PreviousPublished`, `ReplacementPublished`, and `CleanupPending` phases.
- **FR-047**: Each phase transition MUST be atomically published; physical durability MUST synchronize manifest contents and namespace publication before advancing the phase.
- **FR-048**: Normal file-backed store initialization MUST resolve interrupted compaction before ordinary log recovery.
- **FR-049**: In `Prepared`, the old database MUST remain authoritative and incomplete staging MUST be discarded only when doing so cannot remove recovery evidence.
- **FR-050**: In `PreviousPublished`, recovery MUST finish publishing the replacement when it fully validates against the manifest and captured source; otherwise it MUST restore the verified previous generation, and if neither decision is provable it MUST preserve all evidence and return `AuthorityUndetermined`.
- **FR-051**: In `ReplacementPublished`, recovery MUST validate and select the replacement while retaining the previous generation until authority is confirmed.
- **FR-052**: In `CleanupPending`, recovery MUST use the replacement and retry only cleanup proven safe by the manifest evidence.
- **FR-053**: Missing, corrupt, contradictory, or mismatched required artifacts MUST fail closed and preserve every available old, staging, replacement, manifest, and previous-generation artifact.
- **FR-054**: Recovery and cleanup MUST be idempotent across repeated interruption at every manifest and publication boundary.

#### Online Compaction

- **FR-055**: Online compaction MUST operate on exactly one open store-family instance and permit at most one active compaction attempt for that instance; a concurrent second attempt MUST return `FailedClosed` immediately while the first attempt and ordinary reads and writes continue, and other families and unrelated instances MUST remain independent.
- **FR-056**: Each store instance MUST have a bounded maintenance coordination gate whose acquisition order is maintenance coordination, then logical key or existing shard, then log state.
- **FR-057**: Normal mutations MUST participate in shared maintenance coordination for their complete durable-acceptance and live-publication interval.
- **FR-058**: Normal reads MUST retain their existing direct read path and MUST NOT acquire maintenance coordination.
- **FR-059**: Initial snapshot capture and final cutover MAY briefly exclude mutations; disk-heavy snapshot encoding and staging validation MUST occur without exclusive maintenance coordination.
- **FR-060**: Online compaction MUST capture one consistent logical snapshot and activate exactly one ordered delta recorder inside the existing durable acceptance-order boundary before allowing mutations to resume.
- **FR-061**: Every successfully accepted concurrent mutation MUST be recorded exactly once in durable acceptance order while continuing to use the original authoritative log.
- **FR-062**: Rejected, rolled-back, cancelled, panicked, or logical no-op mutations MUST NOT be recorded in the delta.
- **FR-063**: Delta entries MUST preserve logical mutation boundaries, and multi-change compute operations MUST remain atomic and ordered.
- **FR-064**: Online compaction MUST count encoded delta bytes and permanently mark the attempt aborted once the configured limit is exceeded, without preventing mutations from completing normally.
- **FR-065**: A limit-aborted attempt MUST discard staging at cutover, return `ConcurrentDeltaLimitExceeded` with the configured limit, and leave the original log authoritative, writable, and recoverable.
- **FR-066**: At cutover, compaction MUST apply the ordered delta to staging, reopen staging, and prove that it reconstructs the current public live state before publication.
- **FR-067**: Successful cutover MUST use the same manifest authority protocol, replace the store's active log writer and segment-rotation state, and retain obsolete artifacts until replacement authority is confirmed.
- **FR-068**: Pre-publication failure MUST leave the original log authoritative and writable.
- **FR-069**: Cleanup failure after publication MUST leave the replacement authoritative, report cleanup pending, and keep the open store readable and writable; cleanup MUST be retried on the next reopen or caller-triggered compaction and MUST NOT be scheduled in the background.
- **FR-070**: Indeterminate publication authority MUST preserve readable live state, reject subsequent writes, preserve old and new evidence, and require successful reopen or recovery before mutations resume.
- **FR-071**: Delta state and exclusive maintenance coordination MUST be cleared on success, ordinary abort, error, cancellation, or panic unwinding.
- **FR-072**: The coordination design MUST NOT introduce cross-family coordination, cross-process coordination, unbounded per-key maintenance state, or a gate shared by unrelated store instances.

#### Windows Physical Durability

- **FR-073**: Windows MUST support explicit physical durability for construction, opening, normal mutations, compute batches, rollback, segment rotation, recovery publication, closed compaction, and online compaction.
- **FR-074**: Before exposing a physical store, the library MUST preflight content synchronization on the actual target filesystem and namespace publication with disposable same-directory, same-volume artifacts.
- **FR-075**: Preflight MUST NOT modify authoritative log artifacts, MUST clean disposable artifacts when safe, and MUST return `RequiredBarrierUnavailable` if either barrier cannot be established.
- **FR-076**: A requested physical policy MUST never silently downgrade to buffered durability.
- **FR-077**: Physical mutations MUST synchronize their complete persisted representation before publishing corresponding live state.
- **FR-078**: Physical rollback, staging, and manifest contents MUST use the same file-content durability contract as normal log contents.
- **FR-079**: Physical fresh-store publication, rotation, recovery, compaction cutover, manifest transition, and previous-generation transition MUST use a write-through same-volume namespace operation and MUST NOT silently fall back to a weaker rename.
- **FR-080**: Namespace publication whose destination must be absent MUST preserve that no-replacement condition.
- **FR-081**: Handles that would prevent a Windows namespace transition MUST be closed or replaced before attempting publication.
- **FR-082**: Windows paths MUST be passed to platform operations without lossy conversion and MUST support Unicode and platform-supported long paths.
- **FR-083**: Platform failures MUST retain their operating-system error information and be mapped to the applicable structured durability or operation-specific I/O error.
- **FR-084**: All Windows platform calls and unsafe behavior MUST remain within one narrowly bounded durability boundary with documented safety rationale and only the minimum platform capabilities required.
- **FR-085**: A failed content barrier or write-through namespace operation MUST retain existing rollback and failed-closed semantics and MUST NOT expose incomplete acknowledged state.
- **FR-086**: Linux and macOS behavior, existing public signatures, and Windows buffered behavior MUST remain unchanged.

#### Verification and Quality

- **FR-087**: Every behavior MUST follow the project's RED-GREEN test sequence before production code changes, with one behavior-focused failing test followed by the minimum passing implementation.
- **FR-088**: Compaction interruption and error injection MUST cover staging creation, write, synchronization, validation, manifest write and synchronization, previous publication, replacement publication, reopened validation, and cleanup for every family and applicable durability policy.
- **FR-089**: Every interruption case MUST prove exact reopened state, selection of a complete old or new authority, retention of the last complete authority, failed-closed ambiguity handling, and safe cleanup retry.
- **FR-090**: Online tests MUST cover read and write progress, same-key and independent-key ordering, all specified overlap and rejection cases, delta overflow, staging and publication failures, pending cleanup, three reopenings, deterministic lock ordering, and deadlock freedom.
- **FR-091**: Windows tests MUST cover every physical operation, barrier failure, write-through failure, Unicode and long paths, and buffered compatibility.
- **FR-092**: Deterministic evidence MUST prove staging encoding and validation occur outside exclusive maintenance coordination.
- **FR-093**: With compaction inactive, median one-worker mutation throughput MUST remain at least 90% of its matching baseline, eight-worker distinct-key throughput at least 85%, and mutation p95 latency no more than 125% of baseline.
- **FR-094**: Before completion, debug and optimized all-target/all-feature tests, formatting, warning-denying static analysis, and warning-denying documentation generation MUST complete successfully.

### Key Entities

- **Store Family**: One durable key/value, key/set, or key/sorted-map namespace with independent active and sealed segments.
- **Storage Statistics**: A read-only measurement of authoritative active and sealed storage for one family or their directory aggregate.
- **Compaction Generation**: The captured source, staged replacement, previous authoritative generation, or current replacement involved in a maintenance transition.
- **Compaction Manifest**: Temporary checksummed authority metadata describing scope, phase, source identity, generation locations, and durability policy without containing application data.
- **Concurrent Delta**: A bounded, ordered sequence of successfully accepted mutations occurring after an online snapshot and before cutover.
- **Maintenance Coordination**: Per-store bounded coordination that excludes mutation acceptance only during snapshot capture and cutover while leaving reads outside the gate.
- **Cleanup Status**: The distinction between a fully cleaned authoritative replacement and a successful replacement awaiting safe obsolete-artifact cleanup.
- **Durability Policy**: The caller-selected buffered or physical acknowledgement contract governing content and namespace barriers.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Inspection reports exact byte totals and segment counts for 100% of empty, single-family, mixed-family, active-only, and segmented acceptance fixtures while a before/after filesystem comparison shows zero changes.
- **SC-002**: Closed compaction reduces every valid segmented family to one active segment and preserves 100% of public logical state, timestamp granularity, and last accepted timestamp bucket through three consecutive reopenings.
- **SC-003**: Every injected interruption and failure point for all three families selects a complete old or new authority or fails closed with all evidence preserved; no case deletes the last complete authority.
- **SC-004**: Online compaction preserves 100% of accepted concurrent mutations exactly once and in acceptance order while excluded, rejected, cancelled, rolled-back, and no-op work appears zero times after three reopenings.
- **SC-005**: During deliberately paused staging construction, reads and mutations both demonstrate progress; deterministic tests show disk-heavy encoding and validation hold exclusive maintenance coordination for zero operations.
- **SC-006**: Delta-limit overflow leaves the original store writable and logically unchanged in 100% of boundary and over-limit cases, and every success, abort, error, cancellation, and panic case leaves no active delta recorder.
- **SC-007**: All recognized older-format fixtures return migration-required guidance and remain byte-identical; all corrupt, wrong-family, malformed, and ambiguous fixtures return their specified structured classification without mutation.
- **SC-008**: On Windows filesystems that satisfy preflight, 100% of acknowledged physical construction, mutation, rotation, recovery, and compaction cases survive the defined durability fault model; unavailable barriers produce zero buffered fallbacks.
- **SC-009**: With maintenance inactive, one-worker median mutation throughput is at least 90% of baseline, eight-worker distinct-key throughput is at least 85% of baseline, and mutation p95 latency is at most 125% of baseline under matched workloads.
- **SC-010**: The complete required verification matrix finishes with zero test, formatting, static-analysis, or documentation-warning failures on every supported platform applicable to the change.

## Assumptions

- Production callers use the current Pigment DB format; older development data can be discarded or converted explicitly with `pigment-db-migrate`.
- Closed-compaction callers guarantee that no other process has the target directory open; the library detects same-process open instances, but this feature does not add cross-process ownership or locking.
- The current single-process-per-directory ownership model remains sufficient.
- Online compaction is always explicitly invoked for one already-open file-backed family.
- Superseded mutation history may be discarded only after replacement authority is established and cleanup is proven safe.
- The maintenance manifest is temporary recovery metadata, not a database-format compatibility layer.
- A target-specific Windows platform dependency and one bounded unsafe durability boundary are approved solely for strict physical durability; no other unsafe or production dependency expansion is implied.
- The exact manifest filenames, encoding layout, internal staging names, and platform wrapper structure are planning decisions provided they meet every observable authority, durability, compatibility, and recovery requirement above.
- Store directories are dedicated to Pigment DB artifacts; any unexpected entry is rejected before inspection or compaction can mutate the namespace.
- The existing external migration tool, normal recovery classifications, durability policies, WAL acceptance-order boundary, and segment rotation behavior are dependencies of this feature and remain authoritative where this specification does not explicitly extend them.

## Out of Scope

- Runtime support for older database formats or implicit format migration.
- Moving legacy conversion logic into normal store initialization, inspection, or compaction.
- Automatic or background compaction scheduling and application-specific threshold selection.
- Cross-process locking, cross-family transactions, or a gate shared across independent store instances.
- SQL, query languages, secondary indexes, encryption, or replication.
- Changing the current database record format or removing the external migration tool.
