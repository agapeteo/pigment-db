# Feature Specification: Crash-Safe WAL Recovery

**Feature Branch**: `not-created`

**Created**: 2026-08-05

**Status**: Draft

**Input**: User description: "Fix review issue #1: a crash during startup compaction can make the next startup discard the only complete WAL."

## Clarifications

### Session 2026-08-05

- Q: How should an unresolved recovery conflict be reported to library callers? → A: Add a fallible initializer returning a structured recovery error, while retaining the current initializer as a compatibility wrapper.
- Q: When startup automatically recovers from interrupted maintenance, how should callers learn that recovery occurred? → A: Return a structured Normal or Recovered status from the fallible initializer, and log recovery when using the compatibility wrapper.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reopen After Interrupted Startup (Priority: P1)

As an application operator, I need a durable store to reopen with all previously committed data after its prior startup was interrupted, so a restart does not turn a temporary maintenance state into permanent data loss.

**Why this priority**: Preserving committed data is the primary promise of a durable store. A store that silently reopens with missing data cannot be trusted in production.

**Independent Test**: Create a populated store, interrupt startup at each transition where persistent files are moved, rebuilt, published, or cleaned up, then reopen it and verify that every committed entry is available with its original value.

**Acceptance Scenarios**:

1. **Given** a store with committed data, **When** startup is interrupted immediately after the existing active log is preserved for recovery, **Then** the next startup restores every committed entry.
2. **Given** a store with committed data, **When** startup is interrupted while a replacement log is being built, **Then** the next startup uses a complete recoverable state and does not expose the partial replacement as authoritative.
3. **Given** a store with committed data, **When** startup is interrupted after a complete replacement is available but before cleanup finishes, **Then** the next startup opens the complete state and safely finishes or defers cleanup.
4. **Given** startup completes an automatic recovery, **When** a caller uses the fallible initializer, **Then** it receives a Recovered status; when it uses the compatibility initializer, the recovery event is logged.

---

### User Story 2 - Resolve Multiple Recovery Candidates Safely (Priority: P2)

As an application operator, I need startup to handle both active and recovery artifacts without guessing destructively, so repeated restarts converge on one complete store state.

**Why this priority**: Interrupted maintenance commonly leaves more than one candidate. Safe, repeatable selection prevents a subsequent restart from amplifying an interruption into data loss.

**Independent Test**: Prepare each supported combination of complete, empty, partial, and stale active/recovery artifacts, open the store repeatedly, and verify that startup either selects the complete authoritative state or stops without destroying recoverable data.

**Acceptance Scenarios**:

1. **Given** a complete recovery artifact and an empty or incomplete active artifact, **When** the store starts, **Then** the complete recovery artifact is retained as the source of truth.
2. **Given** a complete active artifact and a stale recovery artifact from completed maintenance, **When** the store starts, **Then** the active state opens successfully and stale cleanup cannot remove active data.
3. **Given** multiple artifacts whose authority cannot be determined safely, **When** a caller uses the fallible initializer, **Then** it receives a structured recovery error and all potentially recoverable artifacts remain unchanged.
4. **Given** artifacts left by an interrupted startup, **When** startup is interrupted repeatedly and retried, **Then** each retry remains safe and eventually converges once a startup completes.

---

### User Story 3 - Consistent Recovery Across Store Types (Priority: P3)

As a library user, I need the key/value, key/set, and key/sorted-map stores to provide the same recovery guarantees, so choosing a data model does not change the risk of startup data loss.

**Why this priority**: All three public durable stores use the same maintenance pattern and must honor a consistent durability contract.

**Independent Test**: Run the same interruption matrix against populated instances of all three store types and compare the recovered logical contents with their pre-interruption contents.

**Acceptance Scenarios**:

1. **Given** equivalent committed data in each durable store type, **When** startup is interrupted at the same maintenance transition, **Then** all three stores recover their complete logical contents.
2. **Given** an unresolved recovery conflict for any store type, **When** startup is attempted, **Then** that store fails safely and leaves its recovery candidates available for diagnosis or retry.

### Edge Cases

- Startup finds only the recovery artifact because interruption occurred before a new active artifact was published.
- Startup finds a zero-length active artifact alongside a complete recovery artifact.
- Startup finds a partially rebuilt active artifact alongside a complete recovery artifact.
- Startup finds both artifacts complete after interruption occurred before cleanup.
- Cleanup of a stale artifact fails because of a transient file-system error.
- Startup is interrupted multiple times at different recovery transitions.
- A store contains no logical entries but still has valid maintenance artifacts; an intentionally empty store must not be confused with an incomplete replacement.
- Recovery succeeds for one store type while another store type in the same directory has an unresolved recovery conflict.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Each durable store MUST preserve all data committed before startup maintenance begins when that startup is interrupted at any maintenance transition.
- **FR-002**: Startup MUST examine every recognized active and recovery artifact for the requested store before treating the store as new or empty.
- **FR-003**: Startup MUST distinguish a complete usable store state from an empty, incomplete, or stale maintenance artifact before selecting an authoritative state.
- **FR-004**: Startup MUST prefer a complete recoverable state over an empty or incomplete replacement created by an interrupted startup.
- **FR-005**: The system MUST NOT delete or supersede the last complete recoverable state until a complete replacement has been made authoritative.
- **FR-006**: If startup cannot determine an authoritative state without risking data loss, it MUST fail explicitly and preserve all potentially recoverable artifacts unchanged.
- **FR-007**: Recovery MUST be idempotent: retrying startup from the same persistent state MUST not reduce the amount of recoverable data.
- **FR-008**: Recovery MUST tolerate repeated interruptions and remain recoverable at every transition it creates.
- **FR-009**: After recovery completes, subsequent normal startups MUST expose the same logical data without requiring manual intervention.
- **FR-010**: Safe cleanup of obsolete maintenance artifacts MUST occur only after the authoritative state is established; cleanup failure MUST NOT invalidate an otherwise recoverable store.
- **FR-011**: The recovery guarantees MUST apply to the key/value, key/set, and key/sorted-map durable stores.
- **FR-012**: Existing complete store artifacts created before this feature MUST remain openable without data migration by the user.
- **FR-013**: The library MUST provide a fallible initialization path that returns the opened store with a structured Normal or Recovered status on success and a structured recovery error when authority cannot be established; the current initializer MUST remain as a compatibility wrapper and log successful automatic recovery.
- **FR-014**: An intentionally empty but complete store MUST be distinguishable from a replacement that is empty because startup was interrupted.

### Key Entities

- **Store State**: The complete logical contents of one durable store type at a committed point in time.
- **Active Artifact**: The persistent candidate normally used to open a store.
- **Recovery Artifact**: A preserved candidate retained while startup maintenance is incomplete.
- **Replacement State**: A reconstructed candidate intended to become authoritative only after it is complete.
- **Recovery Outcome**: The result of startup classification: normal open, recovered open, or safe failure requiring retry or intervention.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Across fault-injection tests at every identified startup maintenance transition, 100% of acknowledged pre-startup entries remain available after the next completed startup.
- **SC-002**: The full interruption test matrix passes for all three durable store types with no logical differences between pre-interruption and recovered contents.
- **SC-003**: Ten consecutive interrupted startup attempts followed by one completed attempt recover 100% of the original committed data for every tested store type.
- **SC-004**: In 100% of ambiguous-authority test cases, startup stops with a clear recovery failure and leaves every potentially recoverable artifact intact.
- **SC-005**: In 100% of existing valid-store compatibility fixtures, startup succeeds and returns contents identical to those produced before this feature.
- **SC-006**: After a successful recovery, three consecutive normal restarts return identical logical contents and require no manual cleanup.

## Assumptions

- “Committed data” means data acknowledged according to the project’s existing durability policy before startup maintenance begins; strengthening power-loss synchronization is covered by a separate review issue.
- This feature addresses artifacts created or left by interrupted startup maintenance. General recovery from arbitrary interior log corruption or an application crash during a normal mutation is outside this feature’s scope.
- Only one process manages a given store directory at a time; cross-process writer coordination is outside this feature’s scope.
- The existing logical data model and public read/write behavior remain unchanged.
- When authority is genuinely ambiguous, preserving artifacts and failing clearly is safer than automatically choosing a candidate.
