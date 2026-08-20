# Contract: Online Compaction Coordination and Delta Replay

## Instance scope

Online compaction acts on one open file-backed family. Its maintenance gate, attempt flag, delta recorder, staging generation, and publication scope are not shared with other families or store instances. A compare/exchange winner owns the only attempt; a loser returns `FailedClosed` immediately before creating artifacts or installing a recorder.

## Lock order and guard lifetime

```text
maintenance shared/exclusive
    -> logical key or DashMap shard
    -> WAL state
```

Normal reads acquire none of these new maintenance guards. Every normal mutation acquires shared maintenance coordination before the existing shard/key operation and retains it through:

1. logical precondition validation;
2. WAL write, flush, and required physical barrier;
3. delta recording at the acceptance boundary, when active;
4. live DashMap publication or removal.

Mutation and shard guards are dropped before user-visible post-publication callbacks. Async compute performs its user future outside maintenance and shard guards, then reacquires the complete ordered sequence for conflict validation and acceptance. Poisoned maintenance locks recover their guard rather than changing existing public panic compatibility.

## Delta acceptance contract

The recorder is accessed only under the WAL write guard. A delta group is created only after the complete WAL representation is accepted, including required physical synchronization. A logical single action is one group; every frame of a compute batch is one atomic group. The group stores its accepted timestamp bucket and current-V2 logical frames, not raw file offsets.

The recorder excludes:

- WAL write, flush, or synchronization failure;
- rejected mutation or conflict;
- successful rollback of a rejected mutation;
- failed rollback (and the attempt subsequently aborts failed closed);
- logical no-op compute;
- callback panic before acceptance;
- cancelled/dropped async compute;
- an async computation invalidated before acceptance.

Acceptance order is precisely insertion order under `WalState`; store-method return order is irrelevant.

## Encoded bound

`used_bytes` is the checked sum of bytes that the groups will append in current V2 framing. An exact-limit group is retained. If the next complete group would exceed the limit, or arithmetic overflows:

1. set `overflowed`;
2. clear all retained groups;
3. retain none of the overflowing group;
4. allow that mutation and later mutations to proceed normally against the old WAL;
5. skip all later recorder payload work;
6. return `ConcurrentDeltaLimitExceeded { limit }` only when compaction reaches cutover.

The original WAL remains authoritative and writable after the ordinary overflow abort.

## Lifecycle

1. Claim the attempt flag and prepare owned metadata.
2. Acquire exclusive maintenance coordination.
3. Clone one consistent logical state and timestamp metadata.
4. Activate one token-bound recorder inside WAL state and durably publish online `Prepared` with a verified old-WAL prefix and `source_finalized = false`.
5. Release exclusivity.
6. Encode, content-synchronize, reopen, and validate staging without exclusive coordination. Deterministic tests pause both encoding and validation while reads/writes complete.
7. Reacquire exclusivity. Existing shared mutation guards must finish both WAL acceptance and live publication first.
8. Detach the matching recorder and capture current live state.
9. On overflow or unhealthy WAL, abandon pre-publication staging without changing authority.
10. Apply ordered complete groups using regenerated V2 framing and their accepted timestamp buckets.
11. Synchronize, reopen, and compare exact current logical state, family, granularity, and last bucket.
12. Freeze exact source descriptors and refresh `Prepared` atomically under exclusivity with `source_finalized = true`; no namespace publication is legal before this durable rewrite.
13. Close/detach the old writer, publish through the shared manifest protocol, open/install the replacement writer and rotation state.
14. Release exclusivity only after the store can direct the next mutation to the replacement or has entered an explicit failed-closed state.
15. Clean exact obsolete artifacts outside exclusivity.

## Writer handoff

Handoff preserves the opened durability policy, timestamp granularity, final last bucket, and rotation configuration. The replacement starts as one active current-V2 segment with correct offset/active length, a fresh frame buffer, initial rotation id/base, and no stale force-rotation flag. The old `File` is dropped before Windows namespace movement.

If publication failure proves old authority restored, reopen/install the old writer. If replacement authority is established but writer reopen fails, return operation-specific I/O and reject future mutations. If authority cannot be determined, retain readable in-memory state, preserve all files, reject writes before I/O, and require reopen recovery.

## RAII and unwind safety

`OnlineAttemptGuard` owns the attempt token, recorder token, and flag. On drop it acquires exclusive maintenance coordination, clears only the matching recorder, then resets the attempt flag with release ordering. Lock guards are declared inside its scope so they unwind first. `StagingGenerationGuard` may remove only owned pre-manifest staging; after durable manifest publication it preserves evidence and defers to phase recovery.

User callback panic behavior is unchanged. Panic or cancellation cannot leave a recorder installed or the instance permanently marked as compacting.

## Deterministic acceptance evidence

Tests must prove:

- direct reads progress during initial capture, staging encode/validate, cutover waiting, and cleanup;
- writes block only during snapshot/cutover and complete during paused staging work;
- same-key, distinct-key, put/remove, delete/recreate, compute/ordinary, and batch overlaps replay exactly once in WAL order;
- every excluded-work category above produces no delta entry;
- exact bound and first-over-bound behavior;
- first attempt continues while second fails immediately;
- unrelated store/family progress;
- lock rank and watchdog-based no-deadlock behavior;
- panic/error clears attempt and recorder;
- successful cutover followed by immediate mutation and forced rotation targets replacement;
- cleanup pending stays writable and converges on reopen/next compaction;
- three consecutive reopenings preserve exact final state.
