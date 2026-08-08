# Research: Consistent Concurrent Mutation Ordering

## Decision 1: Use the existing DashMap entry guard as the mutation boundary

**Decision**: Begin every logical mutation with `DashMap::entry(outer_key)` and
retain its occupied or vacant entry guard through state preparation, WAL
acceptance, and live publication. Publish or delete through that same entry.

**Rationale**: In pinned DashMap 3.11.10, both `OccupiedEntry` and
`VacantEntry` own the selected shard's write guard. The shard is locked before
presence is checked, so absence, creation, deletion, and recreation stay in one
coordination order. The fixed shard array adds no per-key retained lock state.
Reads of any key in that shard wait and then observe a complete old or new state.

Exact-source evidence:

- Default shards are a fixed boxed array of shard `RwLock`s; the default count is `(num_cpus * 4).next_power_of_two()`: pinned `dashmap-3.11.10/src/lib.rs:52-57,69-73,174-192`.
- `entry` hashes and write-locks the shard before returning occupied/vacant state: `dashmap-3.11.10/src/lib.rs:632-639,807-824`.
- Both entry variants own the shard write guard and perform insert/remove under it: `dashmap-3.11.10/src/mapref/entry.rs:93-127,139-203`.
- `get_mut` returns `None` after dropping the shard lock for an absent key, so `get_mut → insert` alone cannot continuously coordinate creation: `dashmap-3.11.10/src/lib.rs:747-769`.
- The current authoritative DashMap docs likewise state that `entry`/`get_mut` hold shard write locks and warn about deadlock while another map reference is held: <https://github.com/xacrimon/dashmap/blob/master/_autodocs/api-reference/dashmap.md>.

**Alternatives considered**:

- Exact-key `LockableHashMap`/lock registry: rejected by clarified FR-022; it adds lifecycle, allocation, and retained-key risks.
- A second fixed striped mutex array: rejected because it duplicates the map's existing shards and was explicitly declined.
- One global mutation mutex: rejected because slow callbacks and live publication would serialize all keys.
- WAL-first then shard: rejected because it is the reported divergence.
- `get_mut` followed by top-level `insert/remove`: rejected as the universal rule because the absent path loses the guard and top-level calls can self-deadlock while guarded.

## Decision 2: Enforce one lock order for all mutation families

**Decision**: Use `data shard → prepare/callback → WAL → publish → release
shard` everywhere. No code holding the WAL write lock may acquire a DashMap
reference or invoke user code.

**Rationale**: Current ordinary methods often do `WAL → release → shard`, while
compute, numeric, ordered-append, and pop paths begin with the shard. The mixed
orders permit accepted `A,B` but published `B,A`; changing only some paths to
hold WAL while waiting for a shard would instead create a `WAL↔shard` deadlock.
One shard-first order makes every same-shard mutation wait before recording its
durable position.

Current inventory:

| Store | WAL-first paths to migrate | Already shard-first paths |
|---|---|---|
| Key/value | `put`, `set_number`, `remove` | `compute`, `increment_or_init`, `decrement` |
| Key/set | `append`, member removal, callback removal, outer-key removal | all four compute variants |
| Key/sorted-map | `put`, entry removal, callback removal, outer-key removal | pops (but publish too early), ordered append, all three compute variants |

**Alternatives considered**:

- Keep mixed orders and assign WAL sequence numbers: rejected because live publication can still reverse unless publication waits in sequence, effectively adding a global coordinator.
- Release the shard before WAL and validate/retry: rejected because callbacks are at-most-once and no conflict/retry contract exists.
- Hold WAL during callback preparation: rejected as a global whole-mutation lock and a severe performance regression.

## Decision 3: Represent final-item removal with one outer-key delete

**Decision**: While the occupied entry is guarded, determine whether a set
member or sorted-map entry is the final item. If so, accept one existing
outer-key delete action and then remove the occupied entry. Otherwise accept one
member/entry removal and then apply it. Pops identify the candidate before WAL
acceptance and mutate live state only afterward.

**Rationale**: The current remove-then-delete sequence takes two separate WAL
locks and can be split by another accepted record. One delete already has the
correct replay meaning and is the canonical empty-result representation used by
feature 002. It also avoids a partially accepted two-action ordinary mutation.

**Alternatives considered**:

- Keep two independently accepted actions under the shard guard: same-key order would be correct, but a second write/flush failure could leave a partial logical mutation.
- Add a new transaction frame: rejected because existing delete semantics are sufficient and a format change is unnecessary.
- Mutate the collection first and compensate on WAL failure: rejected because readers or panic recovery could observe unaccepted state.

## Decision 4: Keep existing WAL grammar and add a safe single-action acceptance boundary

**Decision**: Preserve the ownership-returning ordinary WAL helper surface, but
make its existing segmented single-action writes and flush return a result after
releasing the WAL guard. Advance the offset only after every segment and flush
returns success. An explicit write or flush `Err`, including one after earlier
record bytes were written, triggers rollback to the checkpoint. Existing ordinary
store methods retain panic compatibility by panicking outside the WAL helper.
Compute methods retain their contiguous `write_all` batch path.

**Rationale**: FR-010 and SC-006 require rejected acceptance to publish nothing,
leave the prior prefix authoritative when rollback works, and permit later
operations. A panic while owning `std::sync::RwLock` poisons the global WAL lock;
returning the internal error first prevents that. The frame types, encoding, and
replay grammar remain unchanged. A successful short write with no later explicit
`Err` does not enter this rejection rule and remains issue #6.

When rollback succeeds, preserve the original write/flush error, restore exact
bytes and offset, release both guards, and permit later mutations. When rollback
fails or is unavailable, publish nothing, retain readable live state, mark the
single WAL state terminal with both error summaries, and reject later mutations
without another writer call. Repair or reopening of that uncertain artifact is
issue #4; continuing to append at the old logical offset is unsafe.

**Alternatives considered**:

- Keep `unwrap` inside the WAL lock: rejected because caught compatibility panics poison the WAL and block later progress.
- Route every one-action mutation through an allocated multi-action byte buffer: rejected as avoidable overhead against strict SC-004 gates.
- Claim full truncated-tail recovery: rejected; rollback failure, pre-existing partial tails, and corrupt prefixes remain issue #4.
- Generalize this change into successful-short-write repair: rejected by the clarified scope; ordinary `Ok(n < requested)` handling remains issue #6.
- Change ordinary public methods to return `Result`: rejected as source-incompatible.

## Decision 5: Preserve callback failure atomicity with private working values

**Decision**: Continue invoking set/map compute callbacks once on a private
working collection while the entry guard protects the original. KV compute
continues producing an owned replacement without mutating the original. A sync
panic or dropped async future before acceptance discards working state and the
guard. Removal callbacks run only after the entry guard is released.

**Rationale**: DashMap 3.11.10's lock is non-poisoning, so unwind/drop releases
the shard without making it unusable. Because no live state changes before WAL
acceptance, panic/cancellation leaves the original state. Moving async work
outside the guard would need versioning and conflict semantics forbidden by the
current scope.

**Alternatives considered**:

- Catch and suppress callback panic: rejected because it changes public panic behavior.
- Mutate live state and restore on unwind: rejected because partial state could be observed and async cancellation cleanup is unreliable.
- Retry callbacks after conflict: rejected by at-most-once FR-012.
- Release the async guard across `.await`: deferred by this feature and later
  implemented by issue #7 with one-shot optimistic snapshot validation and a
  `WouldBlock` conflict result.

## Decision 6: Use semantic test hooks only for scheduling

**Decision**: Add a private per-store `cfg(test)` lifecycle observer with
semantic phases (`AcceptanceEntered`, `AcceptedBeforePublication`, `Published`)
and one-shot labeled gates. Enable DashMap 3.11.10 `raw-api` only in test builds
to select opaque same/different-shard keys. Seam-driven schedules, exact shard
selection, fault writers, and child-process exit checkpoints live in crate unit
tests. Assertions use public reads, public mutations, and three public reopenings;
they never assert hook counts, shard numbers, or guard identity.

**Rationale**: The existing public API has no controllable pause between WAL
acceptance and live publication. Without a seam, ordinary-versus-ordinary RED
reproduction depends on scheduler luck. Semantic lifecycle gates force the
reported gap, while opaque shard selection makes cross-key progress tests
deterministic despite randomized hashing. Cargo integration targets compile the
library as a normal dependency, so library `cfg(test)` items are absent and
`pub(crate)` remains inaccessible. Keeping forced schedules in child unit modules
is therefore both technically necessary and required by the constitution.

**Alternatives considered**:

- Sleeps and high-volume stress only: rejected as nondeterministic and low-signal.
- Inspect raw WAL frame order as the assertion: rejected because behavior is live/reopen equality, not an internal byte sequence.
- Expose a public testing feature or lock API: rejected as unnecessary surface area.
- Drive private seams from `tests/mutation_ordering`: rejected because integration tests are separate crates and cannot access those items without widening visibility.
- Add Loom models only: useful for model checking but insufficient proof that actual store methods follow the model.

## Decision 7: Benchmark paired baseline/candidate matrices

**Decision**: Before production mutation changes, capture 36 baseline cells:
three stores × vector/file modes × ordinary-write/successful-remove/minimal-
callback profiles × one-worker same-key/eight-worker distinct-key shapes. Use
fixed inputs, five warmups, at least 11 measured samples, setup outside timing,
barrier start, sample throughput, and per-call p95. Rerun the same harness after
implementation on the same host/session and gate each cell independently.

**Rationale**: File I/O can hide lock overhead; vector mode exposes it. One
operation family can hide regressions in another, so the clarified profiles are
separate. Paired ratios are more stable than absolute cross-host timing.

**Alternatives considered**:

- File-only benchmark: rejected because storage latency masks coordination overhead.
- Put-only benchmark: rejected because removal and callback guard lifetimes differ.
- Every public method as a hard gate: rejected as noisy and excessive; correctness covers every family while three representative profiles gate performance.
- Criterion dependency: not required; the existing standard-library ignored-test approach can compute medians, p95, throughput, and ratios.

## Decision 8: Isolate ordering memory from existing map capacity

**Decision**: Compare baseline and corrected quiescent memory after 1,000 and
1,000,000 unique-key create/delete cycles, recording only candidate-minus-
baseline retained growth attributable to ordering. Also assert structurally that
no per-key coordinator/tombstone registry exists after operations finish.

**Rationale**: DashMap's underlying hash tables may retain bucket capacity after
deletion in both versions. Counting that existing behavior as new ordering state
would make SC-005 meaningless. Entry guards and callback working copies are
operation-scoped; the selected design adds no persistent per-key object.

**Alternatives considered**:

- Require process RSS to return to its initial value: rejected because allocators and the existing DashMap retain capacity.
- Skip memory validation: rejected by SC-005.
- Add explicit per-key lock cleanup metrics: rejected because no per-key lock registry is selected.

## Decision 9: Preserve adjacent defect boundaries

**Decision**: Do not alter public pop return semantics, async retry/conflict
behavior, physical sync policy, WAL offsets, or recovery grammar while fixing
ordering. Assert map contents and restart parity for pops; leave review issue #8
to correct the returned element separately.

**Rationale**: These findings have independent contracts and migration risks.
Combining them would obscure RED evidence and violate the feature's focused
scope. Narrow rollback-safe acceptance supports this feature but does not claim
complete issue #4/#6 resolution.

**Alternatives considered**:

- Fix pop return values while moving mutation after WAL: rejected because it is a distinct observable behavior change.
- Move async callbacks outside shards now: rejected because it requires a user-selected conflict policy.
- Strengthen flush to physical synchronization: rejected because it is issue #5 and changes performance/durability policy.

## Decision 10: Split private scheduling from public contract evidence

**Decision**: Store child unit-test modules own deterministic schedules and use
private helpers only to choose timing, shard relationships, fault outcomes, or
process exit points. External integration tests use exported constructors,
mutations, reads, callbacks, and reopen APIs only. A stable traceability manifest
maps every contract ID to its FR/SC, exact test, layer, store/family, schedule,
and public assertion.

The unit layer covers:

- accepted-before-publication RED tracers and overlapping reads;
- exact same/different-shard progress and WAL-acceptance parking;
- explicit write/flush errors, rollback success/failure, and offset mechanics;
- ignored 10,000 same-key and 1,000 different-shard controlled schedules;
- child-process exits before acceptance, after acceptance/before publication,
  and after publication, including a blocked same-key contender.

The integration layer covers:

- public compatibility, normal concurrent histories, and three reopenings;
- callback working-state invisibility and atomic at-most-once counters;
- traceability-manifest completeness and every public mutation family;
- paired performance and retained-memory reports.

**Rationale**: This split preserves deterministic access to private boundaries
without exporting test APIs, while ensuring acceptance outcomes remain public
behavior. A real child process is required for at least one interruption case so
FR-020 is not reduced to ordinary destructor cleanup. Contract IDs make SC-008
objective rather than relying on a final prose audit.

**Alternatives considered**:

- Put every test in unit modules: rejected because public-crate compatibility and
  external caller behavior still need independent integration coverage.
- Put every test in integration targets: rejected because private `cfg(test)`
  seams and exact shard/WAL state are unavailable there.
- Use only panic/drop as process-interruption evidence: rejected because it runs
  destructors and cannot prove the no-cleanup boundary required by FR-020.
- Trace requirements only in task prose: rejected because SC-008 requires a
  durable, testable contract-to-acceptance mapping.
