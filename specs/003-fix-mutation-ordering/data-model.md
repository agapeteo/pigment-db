# Data Model: Consistent Concurrent Mutation Ordering

This feature adds no public persisted entity or WAL format. The model describes
the transient lifecycle that keeps one logical mutation ordered across existing
live and durable representations.

## Entities

### Logical Outer Key

The binary key that defines one ordering domain.

| Attribute | Meaning | Validation |
|---|---|---|
| Bytes | Existing `Vec<u8>` key | Empty and arbitrary binary keys remain valid |
| Store kind | Key/value, key/set, or key/sorted-map | Orders mutations only within the owning store instance |
| Coordination shard | Existing DashMap shard selected by the map's hasher | Fixed for the key within one map; not public identity |
| Presence | Occupied or vacant while guarded | Checked only after the shard guard is acquired |

All members of a set and all search-key/value pairs of a sorted map share their
outer key's ordering domain. Distinct keys in the same shard may block each
other but never share logical state.

### Shard Mutation Guard

The operation-scoped occupied or vacant DashMap entry that owns a shard write
guard.

| Attribute | Meaning | Validation |
|---|---|---|
| Entry state | Occupied or vacant | Returned while the shard remains write-locked |
| Guarded original | Last published complete state or absence | Never mutated during callback preparation |
| Lifetime | Entry acquisition through publication/rejection | Must include WAL acceptance |
| Release | Normal return, error, panic unwind, or future drop | No retained per-key coordination object remains |

The guard is never exposed publicly and never acquired while the WAL lock is
held.

### Logical Mutation

One invocation's proposed state transition for one outer key.

| Attribute | Meaning | Validation |
|---|---|---|
| Operation family | Ordinary write/remove, numeric, pop, ordered append, or compute variant | Every public mutator participates |
| Original state | State observed after guard acquisition | Complete accepted state only |
| Candidate state | Complete result to publish | Prepared without changing original live state |
| Return candidate | Value/callback outcome returned after publication | Existing public semantics retained |
| Eligibility | Accepted, skipped, no-op, rejected, or abandoned | Callback conditions and at-most-once behavior unchanged |

A logical mutation owns exactly one outer key. No multi-key transaction is
introduced.

### Working State

A private compute result or pre-publication candidate.

| Store | Working representation | Failure behavior |
|---|---|---|
| Key/value | Owned replacement value | Dropped on callback panic or WAL rejection |
| Key/set | Owned `HashSet<Vec<u8>>` copied from guarded original | Dropped on panic, cancellation, no-op, or rejection |
| Key/sorted-map | Owned `BTreeMap<SearchKey, Vec<u8>>` copied from guarded original | Dropped on panic, no-op, or rejection |

Working state is never visible through a public read.

### Durable Acceptance

The existing WAL action or compute action batch that gives a mutation its
accepted durable position.

| Attribute | Meaning | Validation |
|---|---|---|
| Checkpoint | WAL offset before acceptance | Retained until write and flush succeed |
| Action set | One ordinary frame or one existing compute batch | Uses existing action identifiers only |
| Lock scope | Encode/write/flush/rollback bookkeeping | Contains no callback or DashMap acquisition |
| Outcome | Accepted or rejected | Offset advances only on acceptance |
| Rejection trigger | Explicit write or flush `Err`, including after earlier byte progress | Successful short writes without a later `Err` remain issue #6 |
| Rollback | Restore exact bytes and offset after explicit rejection | Success preserves the original error; failure makes the WAL fail closed |

### WAL Health

The constant-sized state controlling whether further acceptance is safe.

| State | Meaning | Allowed behavior |
|---|---|---|
| Ready | WAL prefix and logical offset agree | New acceptance may begin |
| FailedRollback | An acceptance error was followed by rollback failure or rollback was unavailable | Public reads remain available; every later mutation fails before another writer call |

`FailedRollback` retains compact summaries of both the primary acceptance error
and rollback error. It adds one state per WAL, never one coordinator per key.
Repair or authoritative reopening of the uncertain artifact remains issue #4.

### Live Publication

The single change made through the guarded entry after durable acceptance.

| Candidate | Publication |
|---|---|
| KV replacement or numeric value | Replace/insert the value |
| Set/map changed non-empty result | Replace/insert the complete collection |
| Ordinary set/map member change | Apply the accepted single change under the guard |
| Final member/entry removed | Remove the occupied outer key after one delete action |
| Direct outer-key delete | Remove occupied entry; vacant state remains absent |
| Skipped/no-op/rejected/abandoned | No publication |

## Relationships

```text
Logical Outer Key
  └── selects one existing Coordination Shard
          └── yields one Shard Mutation Guard
                  ├── protects Original Live State
                  ├── contains one Logical Mutation
                  │       └── may own one private Working State
                  ├── spans one Durable Acceptance
                  └── permits one Live Publication after acceptance

Durable Acceptance order for one key
  == Live Publication order for that key
  == Reopened State transition order for that key
```

## State Transitions

### Successful mutation

```text
Requested
  → ShardGuarded
  → Prepared
  → DurablyAccepted
  → LivePublished
  → GuardReleased
  → Completed
```

Rules:

1. A later same-shard mutation cannot reach `ShardGuarded` until the guard is released.
2. `DurablyAccepted` cannot occur before `ShardGuarded`.
3. `LivePublished` cannot occur before `DurablyAccepted`.
4. A successful return cannot occur before `LivePublished` and guard release.

### Skipped or exact no-op

```text
Requested
  → ShardGuarded
  → Eligibility/EqualityChecked
  → GuardReleased
  → CompletedWithoutMutation
```

No WAL action or live publication occurs. Conditional callbacks remain
uninvoked when ineligible.

### WAL rejection with successful rollback

```text
Requested
  → ShardGuarded
  → Prepared
  → AcceptanceRejected
  → WALCheckpointRestored
  → OriginalLiveStateRetained
  → GuardReleased
  → ErrorOrCompatibilityPanic
```

The compatibility panic occurs only after the WAL lock is released. A caller
that catches the panic can perform a later same-key operation without a poisoned
WAL lock.

### WAL rejection with failed rollback

```text
Requested
  → ShardGuarded
  → Prepared
  → AcceptanceRejected
  → RollbackFailed
  → LiveCandidateDiscarded
  → WALMarkedFailedRollback
  → GuardReleased
  → CompositeErrorOrCompatibilityPanic
```

The WAL offset does not advance, but the artifact is not claimed to match it.
Later mutations fail before writing; public reads continue to expose the last
published live state. No restart-parity guarantee is made for the uncertain
artifact in this feature.

### Callback panic or async cancellation before acceptance

```text
Requested
  → ShardGuarded
  → WorkingStatePrepared
  → CallbackPanics OR FutureDropped
  → WorkingStateDiscarded
  → GuardReleased
  → OriginalLiveAndDurableStateRetained
```

Cancellation releases coordination when the future is actually dropped. A
never-dropped pending future continues to own its shard guard, consistent with
the existing issue-#7 boundary.

### Final-item removal

```text
Occupied non-empty collection
  → Guarded candidate proves target is final item
  → One outer-key delete accepted
  → Occupied entry removed
  → Outer key vacant
```

No separately accepted member/entry removal precedes the delete.

### Process interruption at semantic boundaries

```text
BeforeAcceptance → reopen previous accepted prefix
DurablyAcceptedBeforePublication → reopen prefix including the complete mutation
LivePublishedBeforeReturn → reopen the same newly published state
```

A same-key contender blocked behind the interrupted shard guard contributes no
WAL action. Interruption during a partial frame write remains issue #4 rather
than an accepted semantic boundary for this feature.

## Ordering Rules

### Non-overlapping calls

If mutation A completes before mutation B is invoked for the same outer key,
the guard acquired by B observes A's published state and B is ordered after A.

### Overlapping calls

Either operation may acquire the shard first. The acquired order is valid as
long as durable acceptance and live publication use that same order. Invocation
start order is not a FIFO guarantee.

### Different keys

- Different shards may prepare and run callbacks concurrently.
- The shared WAL briefly serializes acceptance.
- The accepted clarification permits different keys in the same shard to wait.
- Cross-key acceptance order has no new user-visible transaction meaning.

## Invariants

1. Every successful mutation owns its shard before its WAL position is accepted.
2. A mutation publishes exactly once and only after acceptance.
3. The WAL lock is never held while acquiring a shard or executing user code.
4. Present and absent states use the same entry/shard coordination domain.
5. One key's durable and live transitions have the same order.
6. A reader sees the complete state before or after publication, never working state.
7. A rejected or abandoned mutation publishes nothing.
8. Callback eligibility and at-most-once invocation remain unchanged.
9. A final-item removal is represented by one outer-key delete.
10. Entry guards and working values are operation-scoped and leave no per-key lock state.
11. Replaying accepted actions produces the state visible after successful calls finish.
12. Existing public signatures, WAL action identifiers, and frozen artifacts remain compatible.
13. Rollback failure makes the WAL terminal; later mutation attempts perform no writer call.
14. An interruption around a complete acceptance yields one recoverable prefix and never an alternative same-key order.
