# Feature Specification: Persist Compute Mutations

**Feature Branch**: `not-created`

**Created**: 2026-08-05

**Status**: Draft

**Input**: User description: "Fix review issue #2: set/map `compute*` mutations are never written to durable history, so changes disappear after restart and empty outer keys cannot be reconstructed."

## Clarifications

### Session 2026-08-05

- Q: Which baseline should determine whether the 10,000-item compute performance criterion passes? → A: Remove the two-times pass/fail gate and record both durable and non-durable medians for review.
- Q: Which fixed mutation workloads should the 10,000-item performance report measure? → A: Measure one-item sparse, 10% mixed add/remove/replace, and full-collection replacement profiles.
- Q: If persistence fails after a compute callback returns normally, what state should remain visible in the running store? → A: Preserve the original live state and return an error.
- Q: How should fallible persistence be exposed without breaking existing compute callers? → A: Add fallible `try_compute*` counterparts and make compute-specific WAL commits failure-atomic, while preserving existing methods as panic-on-error compatibility wrappers.
- Q: Which successful compute test cases must complete three consecutive reopenings? → A: Every successful acceptance case must pass three consecutive reopenings.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Set Compute Results Survive Restart (Priority: P1)

As a library user, I need every successfully completed callback-based set mutation to remain visible after the store is reopened, so using a compute operation does not silently weaken durability compared with ordinary set operations.

**Why this priority**: A successful mutation that disappears after restart violates the primary expectation of a durable store and can cause silent data loss.

**Independent Test**: Apply each existing callback-based set operation to absent and present keys, add and remove multiple members, reopen the store, and compare the complete logical set state before and after reopening.

**Acceptance Scenarios**:

1. **Given** an existing set, **When** a synchronous compute callback adds and removes members and returns normally, **Then** the same final membership is visible immediately and after reopening.
2. **Given** an existing set, **When** an asynchronous compute callback changes its membership and completes normally, **Then** the same final membership is visible immediately and after reopening.
3. **Given** an absent set key, **When** a compute-if-absent callback creates a non-empty set, **Then** the new key and every member remain available after reopening.
4. **Given** a present set key, **When** a compute-if-present callback changes the set, **Then** the changes remain available after reopening.

---

### User Story 2 - Sorted-Map Compute Results Survive Restart (Priority: P1)

As a library user, I need every successfully completed callback-based sorted-map mutation to remain visible after the store is reopened, including inserted, replaced, and removed entries.

**Why this priority**: Sorted-map compute operations have the same silent-loss behavior as set compute operations and are part of the same durability defect.

**Independent Test**: Apply each existing callback-based sorted-map operation to absent and present outer keys, insert, replace, and remove multiple ordered entries, reopen the store, and compare the complete logical map state before and after reopening.

**Acceptance Scenarios**:

1. **Given** an existing sorted map, **When** a compute callback inserts, replaces, and removes entries and returns normally, **Then** the same keys, values, and order are visible immediately and after reopening.
2. **Given** an absent outer key, **When** a compute-if-absent callback creates a non-empty sorted map, **Then** the outer key and every entry remain available after reopening.
3. **Given** a present outer key, **When** a compute-if-present callback changes the sorted map, **Then** the resulting entries remain available after reopening.

---

### User Story 3 - Empty and Conditional Results Stay Consistent (Priority: P2)

As a library user, I need empty and skipped compute outcomes to have the same meaning before and after restart, so conditional callbacks do not create phantom keys or lose deletion intent.

**Why this priority**: Empty outer keys currently have no reconstructable durable meaning. Defining them as absent removes the ambiguity and aligns live and reopened behavior.

**Independent Test**: Exercise callbacks that produce an empty collection, make no change, or are skipped by their presence condition, then verify identical key existence and contents before and after multiple reopenings.

**Acceptance Scenarios**:

1. **Given** a present set or sorted-map key, **When** a successful callback removes its final member or entry, **Then** the outer key is absent immediately and remains absent after reopening.
2. **Given** an absent outer key, **When** a successful callback leaves the collection empty, **Then** no outer key is created either immediately or after reopening.
3. **Given** a conditional compute operation whose presence condition is not met, **When** the operation returns, **Then** the callback is not invoked and the durable state remains unchanged.
4. **Given** a callback that leaves the logical collection unchanged, **When** the store is reopened repeatedly, **Then** its contents remain unchanged and no phantom outer key appears.
5. **Given** a callback that computes a changed collection, **When** persistence rejects the mutation, **Then** the fallible compute operation returns an error, the compatibility operation panics, and the original logical state remains visible immediately and after reopening.

### Edge Cases

- A callback removes every member or entry from a previously non-empty collection.
- A callback on an absent key returns normally without adding any member or entry.
- A callback removes an item and adds it again, producing no net logical change.
- A set callback inserts the same member more than once.
- A sorted-map callback replaces a value without changing its search key.
- A callback changes several items in one invocation, including a mixture of additions, replacements, and removals.
- The collection contains empty byte values or binary keys.
- A conditional callback is skipped because the outer key is unexpectedly present or absent.
- Persistence rejects a computed mutation after its callback returns normally.
- The store is reopened several times after a successful compute operation.
- A different outer key is mutated before reopening; its state must not be changed by persistence of the computed key.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Every existing callback-based mutation operation for durable key/set and key/sorted-map stores MUST preserve its successful logical result across store reopening.
- **FR-002**: The guarantee in FR-001 MUST cover synchronous set compute, asynchronous set compute, set compute-if-present, set compute-if-absent, sorted-map compute, sorted-map compute-if-present, and sorted-map compute-if-absent operations.
- **FR-003**: A successful callback result MUST preserve the complete logical difference from the collection state observed by the callback, including additions, removals, and value replacements.
- **FR-004**: The logical state visible when a compute operation returns normally MUST equal the logical state reconstructed after reopening.
- **FR-005**: When a callback changes an absent key into a non-empty collection, the new outer key and all resulting members or entries MUST persist.
- **FR-006**: When a callback leaves a collection empty, the outer key MUST be treated as absent both immediately and after reopening.
- **FR-007**: When a callback produces no net logical change, reopening MUST preserve the original state without creating an empty or duplicate logical entry.
- **FR-008**: Presence-conditional operations MUST invoke their callback only when their documented condition is satisfied and MUST leave both live and durable state unchanged when it is not satisfied.
- **FR-009**: Each callback MUST be invoked no more than once per compute operation.
- **FR-010**: Changes to one outer key MUST NOT alter the logical contents of any other outer key during immediate reads or subsequent reopening.
- **FR-011**: A compute operation MUST NOT report successful completion unless its resulting logical state has been accepted under the store's existing durability policy. If a compute-specific WAL commit is rejected or only partly written, a fallible compute operation MUST return an error, an existing compatibility operation MUST preserve its panic-on-persistence-error behavior, and the original pre-callback logical state and durable WAL prefix MUST remain authoritative immediately and after reopening.
- **FR-012**: Existing valid stores created before this feature MUST remain openable with identical logical contents.
- **FR-013**: Existing callers of the callback-based mutation operations MUST continue to compile and retain their current callback invocation and presence-condition behavior.
- **FR-014**: The key/set and key/sorted-map stores MUST follow the same empty-collection and restart-consistency rules.
- **FR-015**: Reopening a store repeatedly after a successful callback mutation MUST be idempotent and MUST NOT add, remove, or change logical data.
- **FR-016**: Each of the seven existing callback-based mutation operations MUST have an additive fallible `try_compute*` counterpart that returns a persistence error without changing the existing operation's signature.

### Scope Boundaries

- This feature covers persistence of successful callback-based set and sorted-map mutations and consistent handling of empty results.
- General ordering between concurrent mutations is addressed by review issue #3 and is outside this feature.
- Recovery from a partially written ordinary mutation remains addressed by review issue #4; this feature includes rollback of a rejected or partially written compute-specific WAL commit only.
- Stronger flush-to-storage guarantees are addressed by review issue #5 and are outside this feature.
- Lock duration across asynchronous callbacks is addressed by review issue #7 and is outside this feature.
- Key/value compute behavior and public mutation operations other than the seven fallible compute counterparts are outside this feature.

### Key Entities

- **Outer Key**: The key identifying one set or sorted-map collection. An outer key with no members or entries is treated as absent.
- **Set State**: The unique members associated with one outer key before and after a callback.
- **Sorted-Map State**: The ordered search keys and values associated with one outer key before and after a callback.
- **Compute Result**: The final logical collection state after a callback returns normally.
- **Logical Mutation Difference**: The additions, removals, and replacements needed to transform the pre-callback collection into the compute result.
- **Durable Store State**: The logical state reconstructed when the store is reopened through its existing initialization path.
- **Compute Commit**: The contiguous WAL change set for one compute result, which either becomes fully authoritative before live publication or leaves the prior durable prefix authoritative.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In the acceptance test matrix, 100% of successful callback-based set and sorted-map mutations produce identical logical state immediately after the call and after reopening.
- **SC-002**: The test matrix covers all seven existing callback-based set/map mutation variants, with absent-key, present-key, add, replace, remove, no-change, and empty-result cases and zero state mismatches.
- **SC-003**: Across 100 generated multi-item callback histories per store type, reopening reconstructs 100% of the expected members, search keys, and values with no extras.
- **SC-004**: Every successful case in the SC-001 and SC-002 acceptance matrix MUST complete three consecutive reopenings, with identical logical contents and key-existence behavior after each reopening.
- **SC-005**: In 100% of empty-result cases, the outer key is absent immediately and after every tested reopening.
- **SC-006**: All pre-feature compatibility fixtures reopen with 100% of their prior logical contents unchanged.
- **SC-007**: Existing callback-based mutation call sites require zero source changes to adopt the corrected persistence behavior.
- **SC-008**: With setup excluded from timing and at least 11 samples per case, a reproducible 10,000-item benchmark MUST report median completion times for the corrected durable compute operation, the equivalent sequence of existing durable mutations, and the pre-feature non-durable compute operation across three fixed profiles: (a) sparse—add one new set member or replace one existing map value; (b) mixed—remove 500 and add 500 set members, or remove 250, add 250, and replace 500 map entries; and (c) full—replace all 10,000 set members or all 10,000 map entries. Results are reviewed without a fixed ratio pass/fail threshold.
- **SC-009**: Across injected write and flush failures for both store types, 100% of fallible compute operations return an error, existing compatibility operations panic, immediate reads retain the pre-callback state, and reopening reconstructs the complete pre-commit WAL prefix without any compute delta becoming visible.

## Assumptions

- A “successful” callback is one that returns normally and whose containing fallible compute operation returns `Ok` or compatibility operation returns normally; callback panic, cancellation, and process termination during a callback are outside this feature.
- Durability uses the store's existing acknowledgement policy. This feature ensures compute operations participate in that policy but does not strengthen the policy itself.
- Treating an empty collection as an absent outer key is the intended public meaning because an empty outer key has no reconstructable member or entry state.
- The callback observes and mutates one logical collection as it does today; callbacks are not retried or invoked solely to calculate persistence changes.
- Existing single-process ownership assumptions remain unchanged.
- The crash-safe startup recovery behavior from feature 001 remains available and is a dependency for reliable reopening tests.
- Compute-specific failure atomicity assumes the storage writer can restore its prior checkpoint after rejecting a write or flush; failure of both commit and rollback remains part of the general partial-write recovery scope in review issue #4.
