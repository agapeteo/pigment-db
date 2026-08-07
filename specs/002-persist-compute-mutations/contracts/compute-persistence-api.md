# Public Contract: Callback Mutation Persistence

## Scope

This contract strengthens callback-based key/set and key/sorted-map mutations
and adds fallible counterparts. Existing method signatures remain unchanged.

## API Compatibility and Additive Surface

| Store | Existing compatibility method | New fallible counterpart | Eligibility |
|---|---|---|---|
| Key/set | `compute` | `try_compute` | Always |
| Key/set | `compute_async` | `try_compute_async` | Always |
| Key/set | `compute_if_present` | `try_compute_if_present` | Outer key present |
| Key/set | `compute_if_absent` | `try_compute_if_absent` | Outer key absent |
| Key/sorted-map | `compute` | `try_compute` | Always |
| Key/sorted-map | `compute_if_present` | `try_compute_if_present` | Outer key present |
| Key/sorted-map | `compute_if_absent` | `try_compute_if_absent` | Outer key absent |

Fallible methods use the same key and callback parameter types as their
counterparts and return `std::io::Result<()>`; the asynchronous fallible method
resolves to that result. Existing methods retain `()` and delegate to the
fallible behavior, panicking if persistence returns an error. Existing callers
require no source change.

## Invocation Contract

For an eligible operation:

1. The callback receives one owned mutable logical collection initialized from
   the guarded outer key.
2. The callback is invoked exactly once.
3. Callback changes are not published to live state until persistence accepts
   the complete compute commit.

For an ineligible conditional operation:

1. The callback is not invoked.
2. No WAL write or flush occurs.
3. A fallible method returns `Ok(())`; a compatibility method returns normally.

Callback panic, asynchronous cancellation, and process termination during the
callback are not successful outcomes.

## Result Contract

| Starting state | Callback result | Immediate state after success | State after reopening |
|---|---|---|---|
| Absent | Empty | Outer key absent | Outer key absent |
| Absent | Non-empty | Outer key present with result | Identical result |
| Present | Unchanged | Original collection retained | Original collection retained |
| Present | Changed, non-empty | Outer key present with result | Identical result |
| Present | Empty | Outer key absent | Outer key absent |

A set result is equal when it contains exactly the same unique binary members.
A sorted-map result is equal when it contains exactly the same ordered search
keys and binary values. An empty value is distinct from an absent entry.

## Persistence Contract

- Every successful addition, removal, and map replacement participates in the
  existing durability policy.
- A multi-item callback persists its net logical result as one contiguous,
  failure-atomic compute batch using existing WAL frames.
- A no-op callback and a skipped conditional call write nothing.
- An empty final collection is represented as outer-key absence.
- Changes for one outer key do not alter another outer key.
- Every successful acceptance-matrix result preserves identical logical contents
  and outer-key presence through three consecutive reopenings.
- Existing valid pre-feature artifacts remain readable without migration.

## Failure Contract

When a compute batch write or flush is rejected:

1. The writer is truncated to the checkpoint preceding the batch.
2. The in-memory WAL offset remains at that checkpoint.
3. The original live collection remains published.
4. Reopening reconstructs the original logical state from the restored prefix.
5. A fallible method returns `Err(std::io::Error)`.
6. Its existing compatibility method panics on that error.

If the storage medium rejects both commit and rollback, live state remains
unpublished and the rollback error is reported. Repair of a medium that cannot
restore its prior prefix remains within the general partial-write recovery scope
of review issue #4.

## Concurrency and Asynchrony Boundaries

- Existing presence-condition behavior is unchanged.
- The asynchronous set callback retains today's guarded-entry lifetime.
- This feature adds no ordering guarantee between concurrent outer-key
  mutations and does not resolve holding a shard guard across an arbitrary
  await.
- Stronger synchronization than the current write-and-flush acknowledgement
  policy is not introduced.
