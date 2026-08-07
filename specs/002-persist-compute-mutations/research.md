# Research: Persist Compute Mutations

## Decision 1: Reuse the existing WAL action vocabulary

**Decision**: Encode compute results with the existing set-append, set-remove,
sorted-map-put, sorted-map-remove, and outer-key-delete actions. Do not add an
action type or change the on-disk frame grammar.

**Rationale**: Feature 001's checked replay already validates and applies every
required action. Existing actions express the complete logical difference, and
outer-key deletion is already the canonical representation of an empty result.
Frozen pre-feature fixtures therefore remain readable without migration.

**Alternatives considered**:

- Add whole-collection or transaction actions: rejected because they expand the
  format and replay surface when existing actions are sufficient.
- Delete and rewrite every result: rejected because sparse and no-op callbacks
  would create unnecessary linear WAL growth.
- Persist callbacks: rejected because callbacks are neither serializable nor
  safe to invoke during replay.

## Decision 2: Add fallible counterparts without changing existing methods

**Decision**: Add seven public fallible methods mirroring the existing callback
operations:

- Key/set: `try_compute`, `try_compute_async`, `try_compute_if_present`, and
  `try_compute_if_absent`.
- Key/sorted-map: `try_compute`, `try_compute_if_present`, and
  `try_compute_if_absent`.

Each returns `std::io::Result<()>`. Existing methods retain their current names,
parameters, and `()` return types and delegate to the corresponding fallible
method, panicking on persistence error as they do today.

**Rationale**: Returning `Result` directly from an existing `()` method is a
source-breaking signature change. Additive fallible methods expose persistence
failures while preserving existing function-item, unit-return, and statement
call sites. `std::io::Error` matches the underlying write, flush, and rollback
failure domain and avoids an unnecessary public error wrapper.

**Alternatives considered**:

- Change existing methods to return `Result`: rejected because it violates
  FR-013 and can break typed call sites and `must_use` policies.
- Keep only panic-based failure reporting: rejected by the clarified FR-011 and
  FR-016 contract.
- Add one generic transaction API: rejected because callers would have to adopt
  a new callback shape and existing compute operations would remain non-fallible.

## Decision 3: Invoke callbacks once on an owned working copy

**Decision**: Retain the current outer-key entry guard, copy the starting
collection into an original snapshot and working value, and invoke the callback
exactly once on the working value. For an absent key, both logical starting
states are empty. Publish the working result only after its WAL commit succeeds.

**Rationale**: The callback receives a concrete mutable `HashSet` or `BTreeMap`,
so arbitrary removals and replacements cannot be observed without comparing an
original snapshot. The working copy prevents callback or persistence failure
from mutating live state and keeps the presence decision stable.

**Alternatives considered**:

- Mutate live state and restore it on failure: rejected because readers could
  observe an unaccepted state and restoration complicates panic paths.
- Invoke the callback twice: rejected because callbacks may have side effects
  and FR-009 permits only one invocation.
- Pass a change-tracking wrapper: rejected because it breaks existing callback
  types.
- Release the guard around `await`: rejected because conflict policy and async
  lock duration belong to review issues #3 and #7.

## Decision 4: Persist deterministic net differences

**Decision**: For sets, emit sorted additions followed by sorted removals. For
sorted maps, emit new or changed puts in search-key order followed by ordered
removals. A changed map value is one put. Equal starting and result states emit
nothing.

**Rationale**: Net differences keep action volume proportional to logical
change. Deterministic ordering makes histories reproducible. Additions before
removals keep every complete prefix non-empty when both endpoint collections are
non-empty.

**Alternatives considered**:

- Removals before additions: rejected because an intermediate complete prefix
  can create an empty outer collection.
- Emit all final items: rejected because it produces redundant history.
- Use hash iteration order: rejected because equivalent operations would create
  unstable histories.

## Decision 5: Normalize an empty result to outer-key absence

**Decision**: A present key ending empty emits exactly one outer-key delete and
is removed from live state after commit. An absent key ending empty writes
nothing and remains absent.

**Rationale**: Ordinary last-item removal already removes the outer key. Member
or map-entry removal alone does not reconstruct an absent key consistently, and
snapshot encoding has no stable representation for an empty outer collection.

**Alternatives considered**:

- Preserve empty outer keys: rejected because immediate and reopened states
  would differ.
- Add an explicit empty-collection action: rejected as an unnecessary format
  and data-model expansion.
- Emit every old item removal: rejected because it is larger and may replay to
  an empty-but-present collection.

## Decision 6: Make a compute WAL batch rollback-capable

**Decision**: Encode every existing-format frame for one compute delta into a
memory buffer while holding the WAL state lock. Record the authoritative byte
offset, call `write_all` once, then call the existing `flush` policy once. If
write or flush fails, invoke a writer-specific rollback function that truncates
the writer to the recorded offset before returning `Err`. Advance the WAL offset
only after write and flush succeed.

`WalState<W>` stores a private rollback function selected by its existing
specialized constructors:

- File-backed WAL: truncate the file to the recorded byte offset and retain
  append position semantics.
- Vector-backed WAL: truncate the vector to the recorded length.
- Test-only fault writers: truncate their retained byte buffer, enabling
  deterministic failures during both write and flush.

No new public trait bound is required because the rollback function is stored
inside the private WAL state when it is constructed.

**Rationale**: Preparing the batch first prevents encoding failures from
touching durable state. A checkpoint and truncate restore the complete prior WAL
prefix after a partial write, satisfying the compute-specific atomicity decision
without transaction markers or changes to feature 001 replay. One lock and one
flush also avoid per-item overhead.

**Alternatives considered**:

- Rely on `write_all` alone: rejected because it may write a prefix before
  returning an error.
- Add public rollback/seek bounds to store generics: rejected because it leaks a
  new storage trait into the public API.
- Add begin/commit frames: rejected because incomplete frames would still need
  recovery changes and the WAL grammar would change.
- Rewrite and atomically replace the whole WAL for every compute: rejected
  because cost grows with complete history instead of the compute delta.

**Residual failure boundary**: If both the commit and its rollback operation
fail, the method returns the rollback error and must not publish live state. The
storage medium may then require issue #4 recovery; no software design can
guarantee prefix restoration when the medium rejects restoration itself. SC-009
injects commit failures with an operational rollback path.

## Decision 7: Preserve conditional and asynchronous behavior

**Decision**: Determine `if-present` and `if-absent` eligibility from the guarded
entry exactly as today. A skipped branch returns `Ok(())`, invokes no callback,
and writes nothing. `try_compute_async` awaits its callback once on the working
set while retaining today's guard lifetime, then uses the same atomic batch
commit path.

**Rationale**: This changes only durability and failure reporting. It does not
silently absorb separate ordering or lock-across-await work.

**Alternatives considered**:

- Return a special skipped outcome: rejected because the requested fallible
  surface only distinguishes success from persistence failure.
- Add an async runtime: rejected; a ready-only standard-library executor is
  sufficient for deterministic tests.

## Decision 8: Test public behavior and private fault checkpoints

**Decision**: Use public file-backed integration tests for successful compute,
reopen, compatibility wrappers, and fallible signatures. Use focused unit tests
with a deterministic fault writer for write/flush rejection, rollback, retained
live state, and replay of the restored byte prefix. Run each behavior through a
separate RED-GREEN cycle. Keep feature 001 recovery tests and frozen fixtures as
compatibility gates. Route every successful SC-001/SC-002 acceptance case through
one shared helper that performs three consecutive drop/reopen/assert cycles.

**Rationale**: Reopen integration tests prove the user-visible defect is fixed,
and a shared three-cycle helper makes SC-004 uniform without duplicating test
logic. Private fault injection avoids a public test seam and unreliable platform
permission tricks while still exercising the real batch and store publication
paths. Deterministic histories cover breadth without a new dependency.

**Alternatives considered**:

- Use filesystem permissions to induce failures: rejected because behavior is
  platform- and environment-dependent.
- Expose a public test writer constructor: rejected because it adds unsupported
  production API solely for tests.
- Inspect bytes without replay: rejected because it would not prove the restored
  prefix remains logically authoritative.

## Decision 9: Report fixed performance profiles without a timing gate

**Decision**: With setup excluded and at least 11 release-mode samples, report
medians for corrected durable compute, equivalent existing durable operations,
and a captured pre-feature non-durable compute baseline across exactly:

- Sparse: one new set member or one replaced map value.
- Mixed: set remove 500/add 500; map remove 250/add 250/replace 500.
- Full: replace all 10,000 set members or all 10,000 map entries.

The report has no ratio pass/fail threshold and is not a normal CI assertion.

**Rationale**: Concrete collection callbacks require an O(n) snapshot to detect
arbitrary changes, while WAL output remains O(d). Reporting all three baselines
exposes this tradeoff without a flaky or misleading delivery gate.

**Alternatives considered**:

- Enforce the obsolete two-times threshold: rejected by the clarified SC-008.
- Benchmark only one mixed workload: rejected because it hides sparse snapshot
  cost and full replacement behavior.
- Run timing assertions in normal CI: rejected because filesystem and runner
  noise make them unreliable.

## Scope decisions

- Compute-specific write/flush rollback is included.
- General concurrent WAL/live ordering remains review issue #3.
- Partial ordinary-mutation recovery remains review issue #4.
- Stronger flush-to-storage guarantees remain review issue #5.
- General short-write repair remains review issue #6; this feature uses
  `write_all` plus compute-batch rollback.
- Async lock duration remains review issue #7.
- No new dependency, WAL action, migration, or unresolved clarification remains.
