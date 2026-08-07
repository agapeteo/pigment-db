# Contract: Durable Mutation Acknowledgement

## Buffered Contract

For existing/default construction, success means:

1. The complete logical mutation was accepted by the current WAL writer and its
   `flush` operation.
2. The WAL offset/timestamp state advanced.
3. Live logical state and eligible callbacks were published.

It does **not** mean the operating system or storage device completed a physical
barrier. Operating-system crash or power loss may discard a buffered success.
Buffered mode performs no new capability probe, data barrier, directory barrier,
or lock acquisition.

## Physical Contract

For an explicitly configured and capability-approved file store, success means:

1. Every record of one logical mutation was written in accepted order.
2. The writer flush completed.
3. One `sync_data` barrier covering the complete logical mutation completed.
4. The WAL offset and timestamp bucket advanced.
5. Live state and eligible callbacks were published.
6. The operation returned its existing successful result.

The guarantee is conditioned on the host platform/filesystem and device honoring
their successful barrier contract. A device that falsely acknowledges a barrier is
outside library control.

## Lock Ownership and Ordering

```text
per-key DashMap entry/shard guard
  → WalStorage.wal_state write guard
      → encode complete logical mutation
      → write_all
      → flush
      → physical only: sync_data
      → offset/timestamp commit
  → release WAL guard
  → live publication/callback
  → release key/shard guard
```

- No WAL operation acquires a DashMap guard.
- Buffered lock duration and I/O remain unchanged except for one predictable enum
  branch.
- Physical callers on any key may wait at the existing single-WAL acceptance
  boundary while a barrier completes.
- Different shards may still prepare candidates concurrently.
- Callbacks continue to run under their existing shard contract but outside the
  WAL guard.
- No whole-operation store mutex, unbounded per-key state, or group coordinator is
  introduced.

## Async Cancellation Boundary

For the existing key/set `try_compute_async` API, awaiting the user callback is
the only cancellation point. If the returned future is dropped while that
callback is pending, the per-key guard is released, the callback's private
working copy is discarded, and the database performs no WAL write, flush,
barrier, accepted-state advance, or live publication.
A waiting operation for the same key can then acquire the guard and proceed.

After the callback returns `Ready`, persistence, acceptance, and live publication
execute synchronously in the same poll without another yield. Cancellation cannot
then detach a background commit or leave a shared barrier waiter; the operation
reaches its normal success or typed persistence-failure result. External side
effects performed by user callback code are not database state and cannot be
rolled back by dropping its future.

## Direct Barrier Coverage

- One single-record logical mutation receives one barrier.
- One multi-record set/map compute mutation receives one barrier after its final
  declared member.
- No constituent record may advance the accepted offset or live state alone.
- The barrier belongs only to that call and is never shared with another logical
  mutation. Issue #5 introduces no group-commit queue, leader, epoch, or waiter
  state.
- Any future shared-barrier/group-commit design is a separate feature requiring a
  new approved specification.

## Confirmed Rejection

On write, flush, or data-barrier failure:

1. Keep accepted offset and timestamp at the captured checkpoint.
2. Truncate the file to the checkpoint.
3. Call `sync_all` to make the rollback, including file length, durable.
4. Only after both truncate and `sync_all` succeed, return a
   `MutationFailure::Rejected` through `io::Error`.
5. Publish no live state and invoke no post-publication callback.
6. Permit later mutations.

The error operation identifies the original failing write, flush, or data barrier.

## Indeterminate Failure and Failed-Closed Health

If either truncate fails or truncate succeeds but rollback synchronization fails:

1. Return `MutationFailure::Indeterminate` with original and rollback diagnostics.
2. Publish no attempted state in the current instance.
3. Change WAL health to failed closed.
4. Preserve the available artifact and bytes.
5. Refuse every later mutation with `MutationFailure::FailedClosed` before any
   writer, flush, truncate, or barrier call.

The library does not claim that the attempted mutation was accepted or rejected.
On later reopen:

- complete structurally valid group present: replay it;
- incomplete terminal group: use existing issue #4 accepted-prefix repair;
- structurally complete invalid group: use existing corruption failure.

## Interruption After a Successful Barrier

If the data barrier succeeds and the process exits before live publication or
before the caller observes return, the complete persisted group is authoritative
and replays. This is not a rejected operation: the persistence commit occurred
even though process-level observation did not.

## Deterministic Evidence Matrix

Each case is independently testable for single-record and multi-record mutation:

| Injected boundary | Required result | Live state | Later current-instance write | Reopen image |
|---|---|---|---|---|
| partial/failed write + durable rollback | `Rejected(Write)` | old | allowed | old |
| failed flush + durable rollback | `Rejected(Flush)` | old | allowed | old |
| failed data barrier + durable rollback | `Rejected(SynchronizeData)` | old | allowed | old |
| failed truncate | `Indeterminate` | old | refused | complete replays; incomplete repairs |
| failed rollback full barrier | `Indeterminate` | old | refused | complete replays; incomplete repairs |
| barrier blocked | call incomplete | old | waits at WAL boundary | not applicable until release |
| barrier succeeds, process stops before publication | no observed return | old before stop | not applicable | new complete state |
| successful physical call, volatile cache discarded | success | new | allowed | new complete state |

Assertions use returned errors, `MutationFailure::from_io_error`, public reads,
callback counts, writer/barrier call counts, durable-byte images, and public reopen
results. Private observers only arrange the checkpoint.
