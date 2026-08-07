# Data Model: Persist Compute Mutations

## Overview

This feature adds no persistent entity and does not change the WAL grammar. It
defines the in-memory states and failure-atomic commit lifecycle that map one
callback result to existing durable actions.

## Entities

### Outer Key

Identifies one collection in either durable store.

| Attribute | Meaning | Validation |
|---|---|---|
| Key bytes | Binary collection identity | Preserved byte-for-byte |
| Store kind | Key/set or key/sorted-map | Selects member or ordered-entry semantics |
| Presence | Absent or present | A stable present collection is non-empty |

An empty callback result is transient. Successful publication normalizes it to
an absent outer key.

### Set State

| Attribute | Meaning | Validation |
|---|---|---|
| Members | Unique binary byte strings | Duplicate insertion has no logical effect |
| Cardinality | Unique member count | Greater than zero while the outer key is present |

### Sorted-Map State

| Attribute | Meaning | Validation |
|---|---|---|
| Search key | Existing ordered public key type | Unique within one outer key |
| Value | Binary value | May be empty |
| Cardinality | Search-key count | Greater than zero while the outer key is present |

### Compute Invocation

One eligible callback execution for one outer key.

| Attribute | Meaning | Validation |
|---|---|---|
| API mode | Compatibility or fallible | Existing method returns `()`; counterpart returns `std::io::Result<()>` |
| Operation | Unconditional, if-present, or if-absent; synchronous or supported asynchronous form | One of seven operation pairs |
| Starting presence | Guarded outer-key state | Determines conditional eligibility |
| Original state | Owned pre-callback snapshot | Empty for an absent key |
| Working state | Owned mutable callback value | Callback is invoked at most once |
| Result state | Working state after normal callback completion | Published only after commit succeeds |

### Logical Mutation Difference

The minimal existing actions that transform original state into result state.

#### Set difference

- `added = result − original`
- `removed = original − result`
- Both lists are sorted before encoding.

#### Sorted-map difference

- `puts`: new search keys plus existing keys whose values changed.
- `removed`: original search keys absent from the result.
- Existing sorted-map order determines action order.

#### Empty result

- Original present and result empty: one outer-key deletion.
- Original absent and result empty: no action.

### Compute Commit

The failure-atomic WAL change set for one compute result.

| Attribute | Meaning | Validation |
|---|---|---|
| Checkpoint | Authoritative WAL byte offset before the commit | Equals the complete prior prefix length |
| Encoded batch | Consecutive existing-format action frames | Fully prepared before writer mutation |
| Publication state | Prepared, accepted, or rejected | Live result publishes only when accepted |
| Rollback function | Writer-specific truncation operation | Restores the checkpoint after write/flush rejection |

| Logical result | Encoded batch |
|---|---|
| Changed non-empty set | Sorted appends, then sorted removals |
| Changed non-empty sorted map | Ordered puts/replacements, then ordered removals |
| Present to empty | One outer-key delete |
| No-op or absent to empty | Empty batch; no writer call |
| Conditional mismatch | No commit is created |

No transaction identifier or new replay action is introduced.

## Relationships

```text
Outer Key
  └── owns zero-or-one stable Collection State
          ├── Set State
          └── Sorted-Map State

Compute Invocation
  ├── reads Original State
  ├── gives one Working State to one callback
  ├── derives Logical Mutation Difference
  ├── prepares one Compute Commit
  └── publishes Result State only after commit acceptance

Compute Commit
  ├── starts at one WAL Checkpoint
  ├── writes one Encoded Batch
  └── either advances authority or restores the Checkpoint
```

## State Transitions

### Eligibility

| Operation | Starting absent | Starting present |
|---|---|---|
| Unconditional compute | Invoke once | Invoke once |
| Compute if present | Return success without callback | Invoke once |
| Compute if absent | Invoke once | Return success without callback |

A skipped transition does not allocate a working result, invoke the callback,
write the WAL, or change live state.

### Successful invoked transition

```text
Guard outer key
  → snapshot original and create working copy
  → invoke callback once
  → derive deterministic logical difference
  → encode complete batch in memory
  → checkpoint WAL
  → write_all and flush
  → advance WAL offset
  → publish non-empty working result or remove empty outer key
  → release guard and return Ok / compatibility unit
```

### Rejected compute commit

```text
Prepared batch
  → write_all or flush rejects
  → truncate writer to checkpoint
  → retain original WAL offset
  → retain original live collection
  → return Err from try_compute* / panic from compatibility wrapper
```

### Failure boundaries

| Failure point | Live state | Durable authority | API outcome |
|---|---|---|---|
| Callback panics | Original retained | No compute batch is attempted | Panic propagates |
| Write or flush fails; rollback succeeds | Original retained | Prior prefix restored | Fallible error / wrapper panic |
| Commit and rollback both fail | Original retained; further use is unsafe | Requires retained-artifact recovery | Rollback error; issue #4 boundary |
| Commit succeeds | Result published | Complete batch extends authority | `Ok(())` / normal unit return |
| Process ends during callback | Original retained in surviving process only | Prior prefix authoritative | Outside successful outcome |

## Invariants

1. A stable present outer key owns a non-empty collection.
2. An eligible callback is invoked exactly once; an ineligible callback is not
   invoked.
3. Live result publication occurs only after write and flush acceptance.
4. A rejected compute commit with successful rollback restores the exact prior
   WAL prefix and offset.
5. A logical no-op emits no durable action.
6. A present-to-empty result emits one outer-key deletion.
7. Non-empty deltas write additions/puts before removals.
8. One outer key's compute result contains no action for another outer key.
9. Replaying an accepted batch transforms the original state into exactly the
   published result.
10. Repeated startup compaction and reopening do not change the accepted result.
11. Existing methods and fallible counterparts share callback eligibility,
    invocation count, delta derivation, commit, and publication behavior.
12. Every successful SC-001/SC-002 acceptance case retains identical logical
    contents and outer-key presence through three consecutive reopenings.
