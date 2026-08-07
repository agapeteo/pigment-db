# Feature Specification: Consistent Concurrent Mutation Ordering

**Feature Branch**: `not-created`

**Created**: 2026-08-05

**Status**: Draft

**Input**: User description: "Fix review issue #3: prevent durable mutation order and live in-memory order from diverging under concurrency, without a global mutex across all put operations."

## Clarifications

### Session 2026-08-06

- Q: If a synchronous compute callback panics or an asynchronous compute callback is cancelled before its result is accepted, what state should remain? → A: Publish nothing; preserve the last accepted state and release coordination.
- Q: Outside the brief shared durable-history append, may distinct keys occasionally wait because they share a bounded coordination slot, or must coordination be exact per key? → A: Reuse the existing data-map shards; keys sharing a shard may block.
- Q: Which storage modes must satisfy the throughput and latency limits in SC-004? → A: Both vector-backed and file-backed stores must meet every performance limit.
- Q: How should same-key mutations be ordered when their calls overlap or occur one after another? → A: Preserve completion-before-invocation order for non-overlapping calls; overlapping calls may be accepted in either order.
- Q: Which mutation workload must satisfy the SC-004 performance limits? → A: Benchmark ordinary writes, ordinary removals, and minimal callback mutations separately; every profile must pass.
- Q: Which persistence failures must this feature treat as a rejected mutation and roll back to the previous WAL checkpoint? → A: Roll back explicit write or flush errors, including errors after partial byte progress; successful short writes remain outside this feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Same-Key Mutations Stay Consistent (Priority: P1)

As a library user, I need concurrent mutations of the same logical key to have one accepted order, so the value I observe after the calls finish is exactly the value reconstructed after reopening the store.

**Why this priority**: A live state that changes after restart is a silent consistency failure. It can make a successful write appear to be undone or make a completed removal reappear.

**Independent Test**: Force two or more mutations of one key to pause at controlled points around durable acceptance and live publication, complete them in every relevant order, and compare the final live state with the state after three consecutive reopenings.

**Acceptance Scenarios**:

1. **Given** one key/value key and two concurrent writes with different values, **When** both calls complete successfully, **Then** the live value and the value after each reopening reflect the same one of the two accepted orders.
2. **Given** one set key and concurrent append and removal operations, **When** both calls complete successfully, **Then** membership is identical immediately and after each reopening.
3. **Given** one sorted-map outer key and concurrent insert, replacement, removal, or pop operations, **When** the calls complete successfully, **Then** the complete ordered map is identical immediately and after each reopening.
4. **Given** ordinary, callback-based, conditional, and numeric mutations aimed at the same logical key, **When** their execution overlaps, **Then** all successful logical mutations participate in the same per-key order regardless of which public operation initiated them.
5. **Given** one logical mutation represented by several durable changes, **When** another mutation of the same key overlaps it, **Then** the second mutation cannot be ordered inside the first mutation's change set.
6. **Given** mutation A finishes before mutation B is invoked for the same key, **When** B completes, **Then** A precedes B; if their calls overlap, either accepted order is valid provided live and durable state use the same order.

---

### User Story 2 - Unrelated Keys Remain Concurrent (Priority: P1)

As a library user, I need operations on unrelated keys to keep making progress independently, so correcting same-key ordering does not turn the store into a globally serialized database.

**Why this priority**: A single global whole-operation lock would correct ordering at the cost of the parallel performance expected from this library, especially when a callback is slow.

**Independent Test**: Pause a mutation of key A before, during, and after its brief durable-history acceptance step, then verify that a mutation of key B assigned to a different data-map shard can prepare and publish independently and is delayed only while the shared durable history is actively accepting another change.

**Acceptance Scenarios**:

1. **Given** a mutation of key A paused while user-provided computation is in progress and key B assigned to a different data-map shard, **When** another thread mutates key B, **Then** key B can complete without waiting for key A's computation.
2. **Given** mutations of many distinct keys, **When** they run concurrently, **Then** the correction does not impose one exclusive critical section spanning every complete mutation.
3. **Given** two keys whose internal coordination may share a finite resource, **When** their operations overlap, **Then** any incidental delay affects performance only and cannot violate either key's durable/live ordering.
4. **Given** a workload containing many one-time keys, **When** all operations finish, **Then** ordering support does not retain unbounded per-key state indefinitely.

---

### User Story 3 - Failures and Boundary Transitions Remain Safe (Priority: P2)

As a library user, I need failed, skipped, deleted, and recreated mutations to preserve the same ordering rules, so unusual outcomes cannot leave live state ahead of or contradictory to durable state.

**Why this priority**: Correct ordering on the common write path is insufficient if an absent-key race, persistence failure, or conditional no-op bypasses the ordering boundary.

**Independent Test**: Exercise absent-key creation, deletion, recreation, conditional no-ops, empty-collection deletion, callback failure boundaries, and injected durable-write rejection while concurrent same-key work is waiting; verify exact immediate and reopened state and continued progress afterward.

**Acceptance Scenarios**:

1. **Given** an absent key and overlapping create, delete, and recreate operations, **When** successful calls finish, **Then** live and reopened state reflect one identical accepted order.
2. **Given** a collection mutation that removes its final item, **When** another mutation of that outer key overlaps it, **Then** deletion and recreation are ordered as complete logical mutations rather than as unrelated absent and present entries.
3. **Given** a conditional operation whose condition is not satisfied, **When** it returns, **Then** it makes no durable or live change and does not disturb the order of successful same-key mutations.
4. **Given** durable acceptance rejects a mutation, **When** the operation reports failure or follows its existing compatibility behavior, **Then** no result from that mutation is published live and later same-key operations continue from the last accepted state.
5. **Given** a read overlaps an in-progress mutation, **When** it observes the key, **Then** it sees a complete state from before or after publication and never a callback's working state or a partially applied logical mutation.
6. **Given** a synchronous callback panics or an asynchronous callback is cancelled before acceptance, **When** the operation ends abnormally, **Then** no callback result is published, the last accepted state remains authoritative, and later same-key operations can proceed.

### Edge Cases

- Two key/value writes target an initially absent key.
- A removal races with a write, numeric update, or callback-based replacement of the same key.
- A set append races with removal of the same member, removal of a different member, or removal of the outer key.
- A sorted-map update races with removal, `pop_first`, `pop_last`, ordered append, or removal of the outer key.
- An ordinary mutation overlaps a synchronous, asynchronous, presence-conditional, or absence-conditional compute mutation of the same outer key.
- A multi-change compute result is empty and therefore deletes its outer collection key.
- A mutation has no net logical effect, including duplicate set insertion and replacement with an equal value.
- Durable acceptance fails while other operations on the same key and different keys are waiting.
- User-provided computation is slow or attempts nested access; ordering support must release owned resources according to the operation's existing failure contract.
- A synchronous callback panics or an asynchronous callback is cancelled before its result is accepted; its working state must be discarded without changing live or durable state.
- Binary keys are empty, very large, or share long prefixes but remain distinct logical keys.
- A process stops after a logical mutation is durably accepted but before its caller observes completion; reopening may include that accepted mutation but must never reconstruct a conflicting per-key order.
- Many unique transient keys are mutated once, ensuring coordination state does not grow without bound.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Every successfully accepted mutation MUST occupy one unambiguous position in the order for its logical outer key.
- **FR-002**: Durable history and live state MUST apply successful mutations of the same logical key in exactly the same order.
- **FR-003**: A logical mutation that requires several durable changes MUST be ordered as one indivisible unit relative to every other mutation of the same key.
- **FR-004**: The ordering guarantee MUST cover every public mutation operation in the key/value, key/set, and key/sorted-map stores, including ordinary writes and removals, numeric updates, callback removals, pops, ordered appends, synchronous and asynchronous compute operations, conditional compute operations, and empty-result deletion.
- **FR-005**: Different mutation forms targeting the same logical key MUST share one ordering domain; no mutation family may maintain an independent same-key order.
- **FR-006**: Creation, deletion, and recreation of a key MUST remain in the same ordering domain even while the key is absent from live state.
- **FR-007**: Once a mutation reports success, every later non-overlapping read of that key MUST observe a state containing that accepted mutation and all earlier same-key mutations.
- **FR-008**: After successful operations finish, reopening the store under its existing durability policy MUST reconstruct the exact logical state that was visible before close, including key existence, collection membership, search-key order, values, and counters.
- **FR-009**: A read overlapping a mutation MUST observe either the complete previously accepted state or the complete newly published state; it MUST NOT observe a callback's working copy or a partially published logical mutation.
- **FR-010**: If durable acceptance rejects a mutation because writing or flushing returns an explicit error, including an error after partial byte progress, the mutation MUST roll back to its previous WAL checkpoint, MUST publish no live change, MUST preserve the last accepted state as authoritative, and MUST release its ordering ownership so later operations can proceed. A successful short write that returns fewer bytes than requested remains outside this rejection rule.
- **FR-011**: Conditional operations whose condition is not met and mutations with no accepted logical change MUST preserve their documented callback behavior while leaving live and durable state consistent.
- **FR-012**: Each user callback MUST retain its existing at-most-once invocation behavior; fixing ordering MUST NOT introduce callback retries.
- **FR-013**: The store MUST NOT use one exclusive coordination boundary that spans the complete work of every mutation across all keys.
- **FR-014**: A paused or slow mutation of one key MUST NOT prevent an unrelated key assigned to a different data-map shard from preparing its mutation, running user-provided computation, or publishing its accepted result, except for the bounded interval in which the shared durable history is actively accepting a change.
- **FR-015**: Any unavoidable serialization used to accept changes into one shared durable history MUST be limited to that acceptance work and MUST NOT encompass unrelated user computation or live-state work.
- **FR-016**: Ordering support MUST remain correct for distinct keys that contend for an internal finite coordination resource; such contention may delay an operation but MUST NOT merge the keys' logical state or accepted order.
- **FR-017**: Completed operations on transient keys MUST NOT cause ordering-related memory usage to grow permanently in direct proportion to every key ever seen.
- **FR-018**: Existing public method signatures, return values, panic-versus-error compatibility behavior, key-existence rules, and callback presence conditions MUST remain source-compatible.
- **FR-019**: Deterministic concurrency tests MUST force the previously failing gap between durable acceptance and live publication rather than relying only on probabilistic stress timing.
- **FR-020**: Subject to the existing crash-recovery and durability policy, a process interruption during an in-flight mutation MUST leave a recoverable prefix of the same per-key accepted order and MUST NOT create an alternative ordering on reopen.
- **FR-021**: If a synchronous compute callback panics or an asynchronous compute callback is cancelled before acceptance, the operation MUST publish no callback result, preserve the last accepted live and durable state, and release same-key coordination.
- **FR-022**: Mutations MUST reuse the existing data map's shard-level coordination rather than adding an exact-key lock registry or a separate striped coordination layer; distinct keys assigned to the same shard are permitted to block one another.
- **FR-023**: If mutation A of a key completes before mutation B of that key is invoked, A MUST precede B in the accepted per-key order. Mutations whose calls overlap MAY be accepted in either order, and no invocation-start FIFO guarantee is required, but durable and live state MUST use the same chosen order.

### Scope Boundaries

- This feature covers the ordering relationship between durable acceptance and live publication for concurrent mutations in all three store types.
- Correctness is defined per logical outer key. A new user-visible total order across unrelated keys and multi-key atomic transactions are outside scope.
- Serializing the brief append/acceptance step required by one shared durable history is allowed; serializing complete mutation operations globally is prohibited.
- Exact-key lock registries and separate striped coordination layers are rejected alternatives for this feature; the existing data-map shards define the available cross-key concurrency.
- The asynchronous compute operation participates in the same-key ordering guarantee, but redesigning its public callback contract, retry semantics, or same-key conflict policy is part of review issue #7 and outside this feature.
- Repair of truncated records, successful short-write handling, stronger storage synchronization, and offset-width changes remain separately tracked review issues #4, #5, #6, and #9.
- This feature does not change the on-disk format or the single-process store ownership model.

### Key Entities

- **Logical Outer Key**: The key that owns one value, set, or sorted map and defines the unit of mutation ordering. All entries inside one set or sorted map share their outer key's order.
- **Logical Mutation**: One public operation's accepted state transition, which may contain no durable change, one durable change, or a contiguous group of changes.
- **Accepted Per-Key Order**: The single sequence in which successful logical mutations of one outer key become authoritative for both durable history and live state.
- **Working State**: A private, incomplete result being prepared by an operation or callback; it is never directly observable as accepted state.
- **Live State**: The complete logical contents visible to readers in the running process.
- **Durable State**: The logical contents reconstructed from the accepted history when the store is reopened.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Across at least 10,000 controlled same-key interleavings for each store type, 100% of completed histories have identical live state and reopened state after each of three consecutive reopenings.
- **SC-002**: The deterministic matrix covers ordinary-versus-ordinary, ordinary-versus-compute, compute-versus-ordinary, compute-versus-compute, and delete-versus-recreate overlaps wherever those forms exist, with zero durable/live ordering mismatches.
- **SC-003**: Across 1,000 controlled independent-key schedules per store type, an operation on key B assigned to a different data-map shard completes while key A is deliberately paused outside the shared durable-acceptance interval in 100% of runs.
- **SC-004**: For each store type in both vector-backed and file-backed modes, a reproducible benchmark MUST measure ordinary writes, ordinary successful removals, and minimal callback-based mutations as three separate profiles, with setup excluded and at least 11 samples per case. Every profile, mode, and store-type case MUST independently show median single-worker same-key mutation throughput of at least 90% of its matching pre-feature baseline, median eight-worker distinct-key throughput of at least 85% of its matching baseline, and 95th-percentile mutation latency no greater than 125% of its matching baseline.
- **SC-005**: After create-and-delete cycles for 1,000,000 unique keys have completed and no mutation remains active, retained memory attributable to ordering is no more than 110% of the retained amount measured after 1,000 otherwise identical cycles.
- **SC-006**: Across injected durable-acceptance failures for all three store types, 100% of rejected mutations leave the prior live state authoritative, release same-key progress, and reopen to the prior accepted state.
- **SC-007**: All existing public mutation compatibility tests pass without source changes, and the corrected behavior introduces zero public signature changes.
- **SC-008**: The public concurrency documentation states in one testable sentence that mutations are ordered per logical key while unrelated keys remain concurrent, and every acceptance test can be traced to that contract.
- **SC-009**: Deterministic tests preserve completion-before-invocation order in 100% of non-overlapping same-key cases and accept either order for overlapping cases only when live state and all three reopenings agree on that order.

## Assumptions

- The outer key, rather than an individual set member or sorted-map search key, is the safest ordering boundary because one public operation can replace or delete the complete collection.
- The store continues to have one owning process. Coordination between multiple processes writing the same store directory is outside scope.
- A single shared durable history may require brief global serialization while accepting a record or logical change set; the user's performance constraint applies to the rest of each mutation.
- Distinct keys assigned to the same existing data-map shard may block one another; this accepted tradeoff avoids an additional lock registry or coordination layer.
- The benchmark baseline is the project state immediately before implementation of review issue #3, using identical hardware, data shape, and build settings; vector-backed and file-backed results are compared only with their matching baseline mode.
- The minimal callback benchmark performs one deterministic logical change without intentional delay, so it measures coordination overhead rather than user computation time.
- Existing durability acknowledgements and crash-recovery behavior remain unchanged; this feature aligns accepted ordering but does not strengthen when bytes are guaranteed to reach physical storage.
- Unsupported recursive callback access retains the operation's existing public failure contract and must not permanently block later operations.
- Features 001 and 002 provide the current recovery and compute-persistence foundations on which this ordering guarantee depends.
