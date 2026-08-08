# Public Contract: Concurrent Mutation Ordering

## Scope

This contract strengthens ordering for existing mutations in the key/value,
key/set, and key/sorted-map stores. It adds no public method, parameter, return
type, trait bound, on-disk action, or multi-key transaction.

**Public concurrency guarantee**: Mutations are ordered per logical outer key,
while mutations of keys in different data-map shards remain concurrent except
during shared WAL acceptance.

## Covered Mutation Surface

| Store | Mutation families |
|---|---|
| Key/value | `put`, `compute`, `increment_or_init`, `decrement`, `set_number`, `remove` |
| Key/set | `append`, `remove_from_set`, `remove_from_set_callback`, `remove_key`, all synchronous/async/conditional compute and `try_compute*` variants |
| Key/sorted-map | `put`, `remove_from_sorted_map`, removal callback, `remove_key`, `pop_first`, `pop_last`, `append_ordered_element`, all compute and `try_compute*` variants |

Compatibility wrappers retain their existing panic behavior. Fallible compute
methods retain `std::io::Result<()>`. Existing numeric and optional results are
unchanged.

## Per-Key Ordering Contract

1. Every successful mutation occupies one accepted position for its logical outer key.
2. The durable history and live store apply same-key mutations in the same accepted order.
3. A call that starts after an earlier same-key call completes is ordered after the earlier call.
4. Calls that overlap may be accepted in either order; invocation-start FIFO is not promised.
5. Once an overlapping order is accepted, immediate live reads and every reopening reflect that same order.
6. A multi-action compute result is one indivisible same-key ordering unit.

The ordering unit for a set or sorted map is the outer key, not an individual
member or search key.

## Read Contract

- A read concurrent with a mutation observes the complete previously published state or the complete newly published state.
- A read never observes a callback's private working collection or a partially applied logical mutation.
- After a mutation reports success, every later non-overlapping read includes that mutation and all earlier accepted same-key mutations.

Readers of another key in the same underlying data-map shard may wait; this is
the accepted coordination tradeoff, not a state-consistency failure.

## Cross-Key Progress Contract

- No global exclusive lock spans callbacks, mutation preparation, WAL acceptance, and publication for every key.
- A paused operation on key A does not prevent key B in a different data-map shard from preparing or publishing, except while the shared WAL is actively accepting a change.
- Keys in the same data-map shard are permitted to block each other for the full guarded mutation.
- The shared WAL may serialize only its existing encode/write/flush/rollback acceptance interval.
- No total transaction order or atomicity is added across different outer keys.

## Presence and Collection Boundary Contract

| Starting state | Mutation | Accepted durable/live result |
|---|---|---|
| Absent | Create non-empty value/collection | Key becomes present after acceptance |
| Present | Replace/update | Complete accepted replacement/update |
| Present collection, non-final removal | Remove member/entry | Outer key remains present |
| Present collection, final removal | Remove member/entry or pop | One outer-key delete; key becomes absent |
| Present | Direct outer-key removal | Key becomes absent |
| Absent | Removal | Remains absent with existing compatibility outcome |
| Conditional mismatch | Conditional compute | Callback not invoked; no durable/live change |
| Exact no-op | Compute or duplicate logical operation | No accepted logical change |

Creation, deletion, and recreation remain in the same shard coordination domain
even while no live entry exists.

## Callback Contract

- Eligible callbacks retain their existing at-most-once invocation behavior.
- Ineligible conditional callbacks are not invoked.
- Compute callbacks work on a private candidate and do not publish before WAL acceptance.
- If a synchronous callback panics before acceptance, the panic propagates and no callback result is published or persisted.
- If an asynchronous callback future is dropped before acceptance, its candidate is discarded and no callback result is published or persisted.
- Post-removal callbacks run only after live deletion and shard-guard release, with their existing argument and invocation condition.
- Recursive access to the same data map/shard from a synchronous callback remains unsupported and may deadlock.

The asynchronous set callback runs once against a private snapshot without a
DashMap guard held across `.await`. Same-key and other-key mutations may proceed
while it is pending. After the callback completes, publication reacquires the
entry guard and compares the accepted value with the snapshot. A mismatch
returns `io::ErrorKind::WouldBlock` without retrying the callback or writing or
publishing its candidate; a match commits through the normal WAL-before-live
path.

## Persistence Failure Contract

When a storage write or flush returns an explicit `Err`, including after earlier
record bytes were written, and rollback succeeds:

1. No candidate live state is published.
2. The WAL offset and bytes return to the pre-mutation checkpoint.
3. The prior live state remains authoritative.
4. The shard and WAL coordination resources are released.
5. A fallible compute returns its existing I/O error.
6. An ordinary or compatibility method follows its existing panic behavior after the WAL lock is released.
7. A later same-key operation can proceed from the prior accepted state.

If rollback fails or is unavailable, live state remains unpublished, the shard
is released, and the WAL enters a terminal fail-closed state. Later mutations
fail before another writer call; public reads remain available. Durable repair
or reopening of the uncertain artifact belongs to issue #4.

A successful `Write::write` result shorter than its requested buffer, without a
later explicit error, does not enter this rejection contract and remains issue
#6. This contract also does not strengthen `flush` to physical-storage
synchronization; issue #5 remains separate.

## Restart Contract

After successful concurrent operations finish and the store is closed normally:

- three consecutive reopenings reproduce exact key existence and contents;
- set membership is exact with no extras;
- sorted-map search-key order and values are exact;
- key/value bytes and numeric values are exact;
- accepted deletion remains absent;
- no reopening chooses an order different from the completed live state.

An operation durably accepted before process termination but whose caller did
not observe completion may appear after restart; it must still occupy the same
per-key order as the accepted history.

## Compatibility Boundaries

- Existing valid WAL files and frozen fixtures remain readable without migration.
- Existing WAL action identifiers and frame grammar remain unchanged.
- Existing public method signatures and callback shapes remain unchanged.
- Pop ordering and state are corrected, but the separate review issue #8 return-value defect is not changed by this feature.
- Partial-tail parsing, successful short-write repair, rollback-failure recovery, physical sync, and offset width remain separate findings.

## Performance Contract

Every performance cell is compared with its matching pre-feature baseline on
the same host and storage mode:

| Dimension | Required cases |
|---|---|
| Stores | Key/value, key/set, key/sorted-map |
| Modes | Vector-backed, file-backed |
| Profiles | Ordinary write, successful ordinary removal, minimal callback mutation |
| Concurrency | One worker on one key, eight workers on distinct keys |

Each cell uses at least 11 measured samples and must independently satisfy:

- one-worker median same-key throughput ≥ 90% of baseline;
- eight-worker median distinct-key throughput ≥ 85% of baseline;
- p95 public-call latency ≤ 125% of baseline.

Added ordering state must not grow with historical unique keys after operations
finish. Existing DashMap bucket-capacity retention is compared against the same
baseline rather than attributed to the ordering feature.

## Acceptance Test Ownership and Traceability

Private lifecycle gates, exact shard selection, fault writers, and process-exit
checkpoints are available only to crate unit tests. External integration tests
use exported public APIs only. In both layers, private mechanisms control timing
or failure; public reads, results, callback counts, and reopen behavior are the
contract assertions.

| Contract ID | Requirement | Planned exact test | Layer | Public assertion |
|---|---|---|---|---|
| CMO-ORDER-1 | FR-001, FR-002, FR-023 | `contract_order_nonoverlap` per store | Integration | Completion-before-invocation order survives three reopenings |
| CMO-ORDER-2 | FR-002, FR-023, SC-009 | `overlap_uses_one_live_and_reopened_order` per store | Unit | Either overlap order is accepted once and matches public reopen state |
| CMO-ORDER-3 | FR-003, SC-002 | `multi_action_batch_is_indivisible` for set/map | Unit | Public collection never reflects a split batch |
| CMO-READ-1 | FR-009 | `callback_working_state_is_invisible` for set/map | Integration | Public read is old/blocked, then complete new state |
| CMO-READ-2 | FR-009 | `accepted_before_publication_read_is_atomic` per store | Unit | Public read never returns a partial candidate |
| CMO-CALL-1 | FR-012 | `eligible_callback_runs_once` family matrix | Integration | Atomic invocation counter is exactly one |
| CMO-CALL-2 | FR-011, FR-012 | `ineligible_callback_is_not_invoked` family matrix | Integration | Counter is zero and state is unchanged |
| CMO-CALL-3 | FR-021 | `panic_or_cancel_discards_candidate` | Integration/Unit | Counter is one; prior state and later progress remain valid |
| CMO-PREFIX-1 | FR-020 | `interrupt_before_acceptance_reopens_prior_prefix` | Unit subprocess | Three public reopenings show prior prefix |
| CMO-PREFIX-2 | FR-020 | `interrupt_after_acceptance_reopens_complete_mutation` | Unit subprocess | Complete accepted action/batch appears once |
| CMO-PREFIX-3 | FR-020 | `interrupt_after_publication_reopens_published_state` | Unit subprocess | Reopen matches published state |
| CMO-PREFIX-4 | FR-020 | `interrupted_contender_contributes_no_action` | Unit subprocess | Reopen includes A and excludes blocked B |
| CMO-CROSS-1 | FR-013, FR-014 | `different_shard_progresses_during_preparation` per store | Unit | Key B completes through public API |
| CMO-CROSS-2 | FR-014, FR-015 | `different_shard_waits_only_for_wal_acceptance` per store | Unit | B prepares, then completes after WAL release |
| CMO-CROSS-3 | FR-014 | `different_shard_progresses_before_publication` per store | Unit | B completes while accepted A is parked |
| CMO-CROSS-4 | FR-016, FR-022 | `same_shard_contention_preserves_independent_state` per store | Unit | Delay is allowed; public states/reopens stay independent |
| CMO-FAIL-1 | FR-010, SC-006 | `explicit_write_error_restores_checkpoint` | WAL/store unit | Prior public state/reopen and later progress |
| CMO-FAIL-2 | FR-010, SC-006 | `flush_error_restores_checkpoint` | WAL/store unit | Prior public state/reopen and later progress |
| CMO-FAIL-3 | Constitution II | `rollback_failure_marks_wal_fail_closed` | WAL/store unit | No publication and no later writer call |
| CMO-FAIL-4 | FR-018 | `compatibility_panic_occurs_after_guard_release` | Store unit | Panic is catchable; rollback-success path remains usable |

The implementation manifest must additionally map every mutation family in
Covered Mutation Surface to at least one unique case ID and reject duplicate IDs
or case names. Documentation and tests use these IDs in names or adjacent case
metadata so the mapping can be audited mechanically.
