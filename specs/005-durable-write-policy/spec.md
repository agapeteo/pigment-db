# Feature Specification: Explicit Durable Write Acknowledgements

**Feature Branch**: `codex/005-durable-write-policy`

**Created**: 2026-08-07

**Status**: Draft

**Input**: User description: "Fix review issue #5: successful mutations are not guaranteed to be durable on disk."

## Clarifications

### Session 2026-08-07

- Q: What durability policy should existing no-options file-backed constructors use by default? → A: Keep buffered acknowledgement as the compatibility default; users explicitly enable physical durability.
- Q: How should the new physical-durability mode be performance-gated before acceptance? → A: Compare each cell with a matching append-plus-barrier reference: at least 90% one-worker throughput, at least 85% eight-worker throughput, and no more than 125% p95 latency.
- Q: How should eight-worker p95 include serialized queueing without adding scheduler-tail noise to microsecond buffered operations? → A: Protocol v5 remains policy-specific: buffered comparator/candidate samples use the established start-only worker rendezvous, while append-plus-barrier reference and physical candidate samples rendezvous before every timed call after completing the preceding call. Each candidate is compared only with a comparator using the same schedule.
- Q: How should protocol v5 prevent process order, heterogeneous-core placement, and frequency drift from favoring one implementation? → A: Link the pre-feature and candidate crates into one process invoked with `taskset -c 12-19`, then counterbalance every matching pair as comparator/candidate for even pairs and candidate/comparator for odd pairs. Reverify topology and inherited affinity before capture and invalidate the complete attempt if either changes.
- Q: Which comparator captures may protocol v5 use? → A: Capture all 36 pre-feature buffered comparator cells, all 18 append-plus-barrier reference cells, and their 54 matching candidates as eleven AB/BA pairs in one write-once process/file. Protocol-v1 through protocol-v4 and focused T263 files remain historical or diagnostic and are not v5 comparators.
- Q: How should an in-memory store respond when a caller requests physical durability? → A: Reject physical durability explicitly as unsupported for in-memory storage.
- Q: After a durability barrier and its rollback both cannot be confirmed, what should reopening do if the attempted mutation is complete and structurally valid on disk? → A: Replay the complete valid mutation; recover an incomplete mutation using existing tail rules.
- Q: How should physical-durability configuration behave when the host platform or filesystem cannot provide every required file-content and directory-entry barrier? → A: Reject physical mode when every required barrier cannot be provided.
- Q: When a physical data barrier fails after complete mutation bytes may have reached storage, should a subsequent successfully synchronized rollback convert the outcome into a confirmed rejection? → A: Return `Rejected` after successful truncate plus rollback synchronization; otherwise return `Indeterminate` and fail closed.
- Q: When a preflight file-content or parent-directory durability probe fails, how should the library classify that failure? → A: Any failed preflight barrier is `RequiredBarrierUnavailable` with operation, path, and original source; later operational failures remain ordinary I/O errors.
- Q: Should issue #5 require one direct durability barrier per logical mutation, or must it implement shared barriers across concurrent calls? → A: Require one direct barrier per logical mutation; shared barriers and group commit are out of scope.
- Q: When may the public physical-durability construction option be exposed as supported? → A: Only after both capability probes and every fresh, active-authority, recovery-authority, and cleanup publication path are GREEN.
- Q: For a missing store with no existing WAL file, what should serve as the file-content capability preflight? → A: Probe the parent directory first, then use the validated staging file's required full synchronization as the content probe before any authority rename.
- Q: What happens if the future returned by the existing key/set `try_compute_async` API is cancelled? → A: Cancellation while the callback future is pending releases the per-key guard and discards its private working copy without WAL I/O, a durability barrier, accepted-state advance, or live publication. Once the callback completes, persistence is a synchronous non-yielding segment of that poll and cannot be cancelled before its success or failure is returned.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Choose Power-Loss-Safe Acknowledgements (Priority: P1)

As an application operator, I need to opt a file-backed store into physical
durability so that a mutation reported as successful is still present after an
operating-system crash or power loss.

**Why this priority**: The current buffered acknowledgement can return before
the storage system has made the mutation durable. Callers that treat success as
a durable commit can therefore lose already-acknowledged data.

**Independent Test**: Open each file-backed store family with physical
durability selected, perform every supported mutation shape, discard all writes
not covered by a completed physical durability barrier, reopen the store, and
verify the acknowledged logical state through public reads.

**Acceptance Scenarios**:

1. **Given** a file-backed store configured for physical durability, **When** a mutation returns success and power is interrupted immediately afterward, **Then** reopening exposes that complete mutation.
2. **Given** one logical mutation represented by multiple persisted records, **When** it returns success in physical-durability mode, **Then** the durability barrier covers the entire logical mutation rather than only one constituent record.
3. **Given** multiple concurrent mutations, **When** each call returns success after its own completed durability barrier, **Then** reopening exposes every accepted mutation in WAL order and no partial logical mutation.

---

### User Story 2 - Receive Honest Storage-Failure Outcomes (Priority: P1)

As an application developer, I need storage write and durability failures to be
reported before the mutation is published as live state, so I never mistake an
unconfirmed commit for a successful durable commit.

**Why this priority**: A physical durability guarantee is unsafe if a failed
barrier is hidden, or if live memory changes even though the call reports that
durability could not be established.

**Independent Test**: Inject failures before, during, and after persisted-record
writing and at the durability barrier, then verify the result, live public state,
store health, and reopened state for every store family.

**Acceptance Scenarios**:

1. **Given** an error-reporting mutation entry point in physical-durability mode, **When** the storage system rejects the write or durability barrier, **Then** the call reports a non-success outcome and does not publish the mutation into live public state.
2. **Given** a durability failure whose persisted outcome cannot be confirmed or safely rolled back, **When** another mutation is attempted on the same instance, **Then** the store rejects further writes until its authority is re-established by a successful reopen or repair.
3. **Given** a compatibility mutation entry point that historically terminates on a persistence error, **When** physical durability fails, **Then** it retains that established failure behavior while clearly identifying the persistence failure.
4. **Given** a barrier failure after complete mutation bytes may have reached storage, **When** truncate plus rollback synchronization succeeds, **Then** the outcome is a confirmed rejection and the store may continue; when either rollback step cannot be confirmed, the outcome is indeterminate and the store fails closed.
5. **Given** an in-memory store is constructed with physical durability requested, **When** a caller uses the error-reporting construction path, **Then** construction returns an unsupported-policy error and exposes no store; the compatibility construction path retains its established panic behavior with an actionable diagnostic.
6. **Given** a barrier and durable rollback were both unconfirmed, **When** reopening finds the attempted mutation complete and structurally valid, **Then** it replays that mutation; when it finds an incomplete mutation, it applies the existing interrupted-tail recovery rules.
7. **Given** either required content or directory-entry durability preflight fails, **When** physical durability is requested, **Then** creation or opening returns `RequiredBarrierUnavailable` with the operation, path, and original source before authority-changing cleanup, repair, publication, or store exposure and does not fall back to buffered behavior; a missing store may create only non-authoritative staging before its content preflight, while failures after successful preflight remain ordinary operational I/O errors.
8. **Given** a key/set `try_compute_async` callback is still pending, **When** its future is cancelled, **Then** the private working copy is discarded, the per-key guard is released, no WAL or barrier call occurs, and public state remains unchanged; once the callback completes, the synchronous persistence segment reaches its normal success or failure result without a cancellation point.

---

### User Story 3 - Preserve Existing Fast Buffered Behavior (Priority: P2)

As an existing user, I need unchanged constructors and mutation calls to retain
their current buffered behavior and performance, while documentation states
exactly what that success does and does not guarantee.

**Why this priority**: Making every existing write wait for physical media would
silently impose a severe workload-dependent latency and throughput change.
Compatibility requires the stronger guarantee to be an explicit choice.

**Independent Test**: Run existing callers without new configuration, verify
their public behavior and persisted-format compatibility, and compare the full
steady-state benchmark matrix with the pre-change baseline.

**Acceptance Scenarios**:

1. **Given** an existing caller that uses no new durability configuration, **When** it creates, opens, and mutates a store, **Then** its signatures, return behavior, and buffered acknowledgement policy remain unchanged.
2. **Given** documentation for a buffered success, **When** a user reads the durability contract, **Then** it clearly states that an operating-system crash or power loss may lose that mutation.
3. **Given** the matching pre-change and candidate buffered benchmark cells, **When** results are evaluated, **Then** every cell independently satisfies the approved throughput and latency thresholds.

---

### User Story 4 - Durably Publish Store Files (Priority: P2)

As an operator using physical-durability mode, I need successful store creation
and startup maintenance to make both the selected file contents and its visible
directory entry durable, so a power loss cannot resurrect an older authority or
remove the newly acknowledged store.

**Why this priority**: Durable mutation bytes are insufficient if the filesystem
name selecting those bytes can still be lost or reverted after success.

**Independent Test**: Interrupt new-store creation and every startup publication
boundary, discard unbarriered file contents and namespace changes, and verify
that each reported success reopens the published authority while every failure
preserves the last complete authoritative artifact.

**Acceptance Scenarios**:

1. **Given** a new file-backed store in physical-durability mode, **When** creation reports success and power is interrupted, **Then** the complete store remains discoverable and opens successfully.
2. **Given** startup maintenance publishes a replacement artifact in physical-durability mode, **When** startup reports success and power is interrupted, **Then** the selected replacement remains authoritative on the next startup.
3. **Given** publication or cleanup is interrupted, **When** the store next opens, **Then** authority selection preserves the last completely published state and cleanup never destroys its only recoverable artifact.

### Edge Cases

- The first mutation in a newly created store is followed immediately by power
  loss.
- A durability barrier succeeds but the process exits before live publication or
  before the caller observes the return value.
- A durability barrier reports failure after the storage device may already have
  persisted some or all bytes.
- A rollback truncate succeeds but the rollback durability barrier fails.
- Reopening after an indeterminate failure finds either a complete valid logical
  mutation or an incomplete terminal mutation.
- Several concurrent callers contend at the existing WAL acceptance boundary;
  one caller fails or panics before its own barrier while the others continue.
- A key/set asynchronous compute future is cancelled while its callback is
  pending; its working copy is discarded and another caller can acquire the key.
- A multi-record mutation fails between constituent records or before its direct
  barrier completes.
- A deletion or final-member removal is the mutation awaiting durable
  acknowledgement.
- Startup maintenance publishes a complete replacement but directory visibility
  is not yet durable when interruption occurs.
- Physical-durability configuration is requested for storage that has no physical
  backing; construction rejects the unsupported policy rather than silently
  downgrading it.
- A missing store has no file on which to run a content preflight; the parent
  directory is probed first, then validated staging is fully synchronized as the
  content probe while active and recovery authority remain unchanged.
- The underlying storage stack claims a completed barrier but hardware does not
  honor the platform durability contract.
- The host platform or filesystem does not expose one of the barriers required
  to guarantee durable file contents and namespace publication.
- A store is reopened with a different runtime durability policy than the prior
  process used.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: File-backed stores MUST support two explicit acknowledgement policies: the existing buffered policy and a physical-durability policy.
- **FR-002**: Existing constructors and opening paths that do not receive new durability configuration MUST continue to select buffered acknowledgement.
- **FR-003**: The physical-durability policy MUST be selectable through an additive configuration path without removing or changing existing public signatures. Public physical construction MUST NOT be exposed as supported until content and directory capability checks plus fresh, active-authority, recovery-authority, and cleanup publication behavior are complete across all three store families.
- **FR-004**: The public contract MUST define buffered success as logical acceptance into the process and operating-system-managed persistence path, without claiming survival of an operating-system crash or power loss.
- **FR-005**: The public contract MUST define physically durable success as completion of a storage durability barrier covering the entire logical mutation before the call reports success.
- **FR-006**: In physical-durability mode, a mutation MUST NOT be published into live public state before its complete persisted representation is covered by a successful durability barrier.
- **FR-007**: Each physical logical mutation MUST receive exactly one direct durability barrier covering its complete persisted representation; a barrier MUST NOT be shared between concurrent calls in this feature.
- **FR-008**: Direct per-mutation barriers MUST preserve accepted WAL order, MUST NOT expose a partial multi-record mutation, and MUST NOT allow one failed mutation to be reported as successful or affect another caller's result.
- **FR-009**: Every persistence write, buffering, and physical durability failure MUST be observable through an error-reporting mutation path for each store family.
- **FR-010**: Existing compatibility mutation paths MUST retain their established panic-versus-return behavior when persistence fails and MUST include the persistence cause in their diagnostic.
- **FR-011**: A mutation whose physical durability cannot be confirmed MUST NOT be reported as successful and MUST NOT become visible in that store instance's live public state.
- **FR-012**: When a failed mutation can be rolled back to the preceding accepted checkpoint, the rollback MUST itself be durably established before the store accepts another mutation.
- **FR-013**: When neither the attempted mutation nor a rollback can be confirmed durable, the store MUST enter a fail-closed state that rejects further mutations and preserves available artifacts for reopen, recovery, or diagnosis.
- **FR-014**: After a physical-barrier failure, successful truncate plus rollback synchronization MUST produce a confirmed rejection and permit later mutations. If either rollback step cannot be confirmed, the outcome MUST be reported as indeterminate and the store MUST fail closed. On reopening an indeterminate image, a complete structurally valid logical mutation MUST be replayed as authoritative, while an incomplete mutation MUST be handled by the existing interrupted-tail recovery rules.
- **FR-015**: A new file-backed store opened under physical durability MUST NOT report successful creation until both its complete initial contents and the namespace entry used to discover it satisfy the platform's durability contract.
- **FR-016**: Startup maintenance under physical durability MUST NOT report successful publication until the selected complete artifact and the namespace transition making it authoritative satisfy the platform's durability contract.
- **FR-017**: Cleanup after durable publication MUST occur only after the replacement is authoritative and MUST never remove the last complete authoritative artifact.
- **FR-018**: FR-001 through FR-017 MUST apply consistently to key/value, key/set, and key/sorted-map file-backed stores and to every supported mutation shape.
- **FR-019**: In-memory stores without a physical-durability request MUST retain their existing behavior. When physical durability is requested, an error-reporting construction path MUST reject it as unsupported without exposing a store, and the compatibility construction path MUST retain its established panic behavior with an actionable diagnostic; no path may silently ignore or downgrade the request.
- **FR-020**: Durability policy MUST be a property of the currently opened store instance; reopening without explicit configuration MUST use the compatibility default rather than infer a policy from historical records.
- **FR-021**: This feature MUST NOT require a persisted-record format change or reinterpret valid legacy or V1 records.
- **FR-022**: Existing public read semantics, callbacks, key-existence behavior, logical mutation order, and successful mutation results MUST remain unchanged apart from the newly documented acknowledgement scope.
- **FR-023**: Physical-durability tests MUST model loss of every write and namespace change not covered by a successful barrier and MUST exercise write, barrier, rollback, publication, and cleanup failures deterministically.
- **FR-024**: Before production behavior changes, the feature MUST capture both a reproducible buffered baseline and a minimal correct append-plus-barrier reference baseline using the same store family, storage mode, workload profile, concurrency, warmup, and sampling methodology established for issues #3 and #4.
- **FR-025**: Every buffered performance cell MUST pass independently against its matching baseline; faster cells MUST NOT compensate for a failing cell.
- **FR-026**: Every physically durable performance cell MUST pass independently against its matching append-plus-barrier reference; faster cells MUST NOT compensate for a failing cell.
- **FR-027**: The final performance report MUST publish throughput and tail latency for both acknowledgement policies under matching file-backed workloads, clearly separating buffered, physically durable, and append-plus-barrier reference results.
- **FR-028**: Physical durability MAY coordinate callers only at the shared persistence-ordering and durability boundary; it MUST NOT introduce new whole-operation global serialization into the buffered path.
- **FR-029**: Before authority-changing cleanup, repair, publication, or exposure of a store configured for physical durability, creation and opening MUST successfully execute the required parent-directory and file-content durability preflights. For an existing selected file, its non-destructive synchronization is the content preflight. For a missing store, the parent directory MUST pass preflight first, then the validated non-authoritative staging file's required full synchronization MUST serve as the content preflight before any authority rename. Any failed preflight MUST return `RequiredBarrierUnavailable` with the operation, path, and original source; the compatibility path MUST retain its established panic behavior with an actionable diagnostic. A failed directory preflight MUST leave startup artifacts unchanged; a failed missing-store content preflight MAY leave only non-authoritative staging if deterministic cleanup also fails. No failed preflight may change active/recovery authority, expose a usable store, or fall back to buffered acknowledgement. Failures after successful preflight MUST remain ordinary operation/path-aware I/O errors.
- **FR-030**: Cancelling the future returned by key/set `try_compute_async` while its callback future is pending MUST release the per-key guard, discard the private working copy, perform no WAL write/flush/barrier or accepted-state advance, and leave public state unchanged. After the callback future completes, WAL persistence MUST execute synchronously without an intervening cancellation point and MUST reach the same success, rejection, or indeterminate outcome as the corresponding synchronous mutation path.

### Key Entities

- **Acknowledgement Policy**: The runtime choice defining what persistence work
  must finish before a mutation call may report success.
- **Buffered Acknowledgement**: Success after logical acceptance into the current
  process and operating-system-managed persistence path, without a power-loss
  survival guarantee.
- **Physical Durability Acknowledgement**: Success only after a completed storage
  barrier covers every persisted record belonging to the logical mutation.
- **Durability Barrier**: The platform operation that establishes the required
  physical persistence ordering for file contents or namespace changes.
- **Barrier Coverage**: Every persisted record belonging to exactly one logical
  mutation and covered by that mutation's single completed durability barrier.
- **Indeterminate Persistence Outcome**: A reported storage failure for which the
  system cannot prove whether complete bytes reached durable storage; it is
  neither a successful acknowledgement nor a confirmed rollback.
- **Store Health State**: Whether the current instance can continue accepting
  mutations or must fail closed until authority is re-established.
- **Authoritative Artifact**: The complete store file selected by startup and
  protected through publication and cleanup sequencing.

### Scope Boundaries

- This feature defines acknowledgement and failure contracts for physical
  durability; it does not guarantee correctness from storage hardware that
  falsely reports completion or violates the host platform's durability rules.
- This feature does not add replication, cross-device redundancy, distributed
  consensus, multi-store transactions, or protection from filesystem or media
  corruption after a successful barrier.
- This feature does not change WAL record structure, timestamps, checksums,
  action meanings, or recovery classification introduced by issue #4.
- This feature does not change the existing single-process-per-store-directory
  ownership model.
- Physical durability is available only where the complete platform durability
  contract can be enforced; best-effort operation and automatic downgrade are
  outside this policy.
- Partial public physical support is outside this issue: internal mutation and
  failure behavior may be developed behind test-only probes, but the public
  construction option is exposed only when every startup path satisfies the full
  capability and namespace-publication contract.
- Shared barriers, group commit, waiter coordination, and shared-failure
  propagation are outside this issue and require a separately approved feature.
- Historical retention, point-in-time startup, new asynchronous completion
  notification APIs, and caller-selected delayed flush schedules remain outside
  this issue. FR-030 only fixes the cancellation boundary of the existing
  key/set async compute API.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Across every supported mutation shape and all three file-backed store families, 100% of mutations reported successful in physical-durability mode remain present after a fault harness discards every unbarriered content and namespace change.
- **SC-002**: At every write and barrier failure point, zero mutations with unconfirmed physical durability are reported as successful or published into the current instance's live public state.
- **SC-003**: For 100% of simulated rollback-barrier failures, the current store instance rejects every subsequent mutation until successful reopen or repair re-establishes authority.
- **SC-004**: For every tested set of concurrent mutations, each successful call has exactly one completed direct barrier, all successful calls replay in accepted WAL order, no partial logical mutation appears, and no failed call changes another caller's result or is counted as successful.
- **SC-005**: After successful new-store creation and every successful startup-publication checkpoint in physical-durability mode, the published store reopens correctly after all unbarriered namespace changes are discarded.
- **SC-006**: Existing frozen histories and public compatibility tests pass without persisted-format migration or changed read, callback, key-existence, and successful-result behavior.
- **SC-007**: Under protocol v5's one-process AB/BA pairing, fixed `12-19` CPU affinity, and start-only worker schedule, in every matching buffered benchmark cell, one-worker median throughput is at least 90% of the paired pre-feature comparator, eight-worker median throughput is at least 85% of that comparator, and p95 public-call latency is no more than 125% of that comparator.
- **SC-008**: Under protocol v5's same-process AB/BA pairing, fixed affinity, and per-operation worker-rendezvous schedule, against the matching append-plus-barrier reference, every physically durable benchmark cell achieves at least 90% one-worker median throughput, at least 85% eight-worker median throughput, and p95 public-call latency no more than 125% of that reference.
- **SC-009**: The final performance report contains every required buffered, physically durable, and append-plus-barrier reference file-backed benchmark cell, reports writes per second and p95 latency separately, and declares acceptance only when every individual buffered and physically durable cell passes its threshold.
- **SC-010**: For every injected persistence failure, the error-reporting path returns a persistence error and the compatibility path exhibits its established failure behavior with a diagnostic identifying the persistence cause.
- **SC-011**: Documentation for every file-backed creation/opening path identifies its default acknowledgement policy, the exact guarantee of success, the effect of reopening with different configuration, and the deterministic complete-versus-incomplete reopen outcome after a reported barrier failure.
- **SC-012**: For all three in-memory store families, 100% of error-reporting construction attempts that request physical durability return an unsupported-policy error without exposing a store, and every matching compatibility construction attempt terminates with an actionable unsupported-policy diagnostic.
- **SC-013**: Across all three file-backed store families, 100% of indeterminate-failure reopen cases replay a complete structurally valid logical mutation and apply existing interrupted-tail recovery rules to an incomplete mutation.
- **SC-014**: For every simulated platform or filesystem capability matrix whose required content or directory-entry preflight fails, 100% of physical-durability creation and opening attempts return `RequiredBarrierUnavailable` with operation, path, and source without changing active/recovery authority or exposing a store, and zero attempts downgrade to buffered acknowledgement; failed missing-store content preflight leaves no artifact other than explicitly diagnosed non-authoritative staging when its deterministic cleanup also fails.
- **SC-015**: For 100% of deterministic key/set async-compute cancellations while the callback is pending, public state and WAL/barrier counters remain unchanged and a waiting same-key caller proceeds after guard release; after callback completion, 100% of calls reach their normal synchronous persistence outcome without an observable cancellation checkpoint.

## Assumptions

- Compatibility and the existing performance contract require current no-options
  constructors to remain buffered by default; callers that require power-loss
  survival will select physical durability explicitly.
- The operating system and storage hardware honor their documented durability
  barrier semantics. A device that acknowledges but ignores a barrier is outside
  the guarantee the library can provide; a platform that cannot expose every
  required barrier is rejected as unsupported for physical durability.
- The issue #1 and issue #4 authority-selection, staged-publication, logical-
  mutation-boundary, and cleanup rules remain authoritative; this feature adds
  the physical barriers required by the selected acknowledgement policy.
- Physical durability is intentionally slower than buffered acknowledgement.
  Candidate measurements quantify that cost, the buffered regression gate
  protects existing users, and the append-plus-barrier reference gate limits
  avoidable overhead in the new physical mode.
- A storage error may be inherently indeterminate. The safe contract is to avoid
  success, avoid live publication, and fail closed when durable rollback cannot
  be established rather than claim an outcome the storage system did not prove.
  On reopening, the complete valid bytes actually present are authoritative;
  incomplete bytes remain subject to the existing interrupted-tail rules.
