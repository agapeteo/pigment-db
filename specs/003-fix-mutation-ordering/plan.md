# Implementation Plan: Consistent Concurrent Mutation Ordering

**Branch**: `003-fix-mutation-ordering` | **Date**: 2026-08-06 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/003-fix-mutation-ordering/spec.md`

## Summary

Make the existing DashMap shard guard the ordering boundary for every mutation:
acquire the outer-key entry before durable acceptance, retain the same guard while
preparing the result and writing the WAL, publish through that guard only after
acceptance, then release it. This establishes one shard-first lock order for
present, absent, deleted, and recreated keys without a global whole-mutation
mutex or an additional key-lock registry. Existing compute paths already follow
the core pattern; ordinary WAL-first paths are migrated one RED-GREEN tracer at
a time. Final-item set/map removals use one outer-key delete action, while
multi-item compute results retain their existing failure-atomic WAL batch.

## Technical Context

**Language/Version**: Rust 2021 edition; current validation toolchain Rust 1.97.0; crate declares no MSRV

**Primary Dependencies**: Existing `dashmap` 3.11.10 entry/shard guards, `bincode` 1.3.3 WAL encoding, `crc32fast`, `log`, standard collections and synchronization; development-only `tempfile` and DashMap `raw-api` feature for opaque same/different-shard test-key selection; no new runtime crate

**Storage**: Existing generic `WalStorage<W: Write>` backed by `Vec<u8>` or one local file per store; existing WAL frame grammar and feature-002 rollback-capable compute batches; no on-disk action identifier or migration

**Testing**: `cargo test`; crate-unit tests own private `cfg(test)` semantic observers, opaque shard selection, WAL fault writers, and child-process interruption checkpoints; external integration tests use only public APIs for compatibility, normal concurrent histories, callback counts, traceability, three consecutive file-backed reopenings, and ignored release performance/memory reports; formatting and Clippy diagnostics

**Target Platform**: Rust library on Linux, macOS, and Windows under the existing one-process-per-store-directory assumption

**Project Type**: Single Rust library crate

**Performance Goals**: For each key/value, key/set, and key/sorted-map store in vector-backed and file-backed modes, separately benchmark ordinary write, successful removal, and minimal callback profiles. Every cell's median one-worker same-key throughput MUST remain at least 90% of the matching baseline; median eight-worker distinct-key throughput MUST remain at least 85%; per-call p95 mutation latency MUST remain at most 125%. Each cell uses at least 11 measured samples.

**Constraints**: Preserve all public signatures, return types, callback eligibility, and panic-versus-result behavior; use existing DashMap shards rather than a global mutation mutex, exact-key registry, or second striped layer; permit same-shard unrelated-key blocking; use one lock order `data shard → WAL → live publication`; do not expose guards or test hooks publicly; callbacks are never retried; explicit write/flush `Err` after zero or partial record progress rolls back, but successful short writes remain issue #6; rollback failure makes the WAL fail closed; no WAL format change; issue #7 async conflict redesign and review issues #4/#5/#6/#8/#9 remain independently tracked

**Scale/Scope**: Three store types and every public mutator; at least 10,000 controlled same-key histories per store in ignored conformance; 1,000 controlled different-shard schedules per store; three reopenings after every accepted conformance history; 36 independently gated performance cells (3 stores × 2 storage modes × 3 profiles × 2 concurrency shapes); 1,000,000 unique-key create/delete cycles for retained-ordering-memory validation

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

### Pre-research gate

- **I. RED-GREEN Test-Driven Development**: PASS — every production mutation or WAL behavior change follows one behavior-focused failing test, expected RED confirmation, minimum GREEN implementation, and the affected suite before the next behavior.
- **II. Durable and Live State Integrity**: PASS — the design names requested, accepted, published, rejected, interrupted, and rollback-failed authority transitions. It includes deterministic overlapping-read, rejection, and child-process interruption evidence.
- **III. Compatibility Is an Explicit Contract**: PASS — public signatures, callback eligibility, panic-versus-result behavior, WAL grammar, and frozen fixtures remain unchanged. Successful short-write repair, physical sync, and format migration remain separate work.
- **IV. Bounded Concurrency and Measured Performance**: PASS — existing DashMap shards are the only data coordination layer; no whole-operation global mutex or retained key registry is added. The 36 paired performance cells and retained-memory gate are fixed before production edits.
- **V. Public Evidence and Scope Discipline**: PASS — private seams schedule only crate-unit tests; state assertions use public reads, results, and reopen APIs. External integration targets consume no `cfg(test)` internals.
- **Project constraints**: PASS — the plan covers all three store families, adds no production dependency, retains the one-process ownership model, and keeps platform-specific interruption behavior in a test-only subprocess harness.
- **Clarification gate**: PASS — all six decisions from the 2026-08-06 clarification sessions are reflected; no unresolved technical-context marker remains.

### Post-design gate

- **RED-GREEN TDD**: PASS — [quickstart.md](quickstart.md) separates each production change into an exact RED command and minimum GREEN step; regression matrices follow rather than precede the relevant tracer.
- **Public evidence and Rust test boundaries**: PASS — [research.md](research.md) assigns private observers, shard discovery, fault writers, and exit checkpoints to crate-unit tests. [concurrent-mutation-ordering.md](contracts/concurrent-mutation-ordering.md) assigns public-only integration evidence and stable traceability IDs.
- **State authority**: PASS — [data-model.md](data-model.md) defines successful, rejected-with-rollback, rollback-failed/fail-closed, callback-abandoned, and process-interrupted transitions without inventing another durable order.
- **Compatibility**: PASS — existing public APIs, action identifiers, frame grammar, callback counts, frozen fixtures, and adjacent issue boundaries are explicit test gates.
- **Bounded concurrency**: PASS — entry guards establish one shard-first order; no WAL-held path acquires a shard, no global whole-mutation lock is introduced, and same/different-shard progress is deterministic in unit tests.
- **Failure and interruption evidence**: PASS — explicit write/flush errors, rollback success/failure, overlapping reads, and child-process exits before acceptance, after acceptance/before publication, and after publication are specified.
- **Performance validation**: PASS — the fixed 36-cell paired baseline/candidate matrix includes medians and p95, and the memory comparison isolates added ordering state from pre-existing DashMap capacity retention.
- **Research completeness**: PASS — all technical-context choices are resolved across Phase 0 and Phase 1 artifacts.

## Project Structure

### Documentation (this feature)

```text
specs/003-fix-mutation-ordering/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── concurrent-mutation-ordering.md
├── checklists/
│   └── requirements.md
└── tasks.md                          # Generated by $speckit-tasks
```

### Source Code (repository root)

```text
Cargo.toml                            # Test-only DashMap raw-api feature

src/
├── test_support/                     # Entire module compiled only with cfg(test)
│   ├── mod.rs                        # Shared unit-test support exports
│   ├── mutation_schedule.rs          # Semantic one-shot gates and RAII release
│   ├── shard_keys.rs                 # Opaque same/different-shard selection
│   └── fault_writer.rs               # Explicit-error and rollback fault writers
├── mutation_ordering_tests/
│   ├── key_value.rs                  # KV seam-driven unit tracers/conformance
│   ├── key_set.rs                    # Set seam-driven unit tracers/conformance
│   ├── key_map.rs                    # Map seam-driven unit tracers/conformance
│   └── cross_shard.rs                # Deterministic shard/WAL progress schedules
├── key_value_store.rs               # Entry-first KV mutations; includes child tests
├── key_set_store.rs                 # Entry-first set mutations; includes child tests
├── key_map_store.rs                 # Entry-first map mutations; includes child tests
└── wal/
    ├── mod.rs                       # Accepted-before-publish WAL boundary and fault tests
    ├── ordering_tests.rs            # Offset/rollback/fail-closed unit assertions
    ├── model/
    │   └── mod.rs                   # Existing action grammar; unchanged
    ├── replay.rs                    # Restart parity regression surface
    └── recovery.rs                  # Existing recovery regression surface

tests/
├── mutation_ordering.rs             # Feature integration-test root
├── mutation_ordering/
│   ├── support.rs                   # Barriers, snapshots, three-reopen helpers
│   ├── key_value.rs                 # KV ordering histories
│   ├── key_set.rs                   # Set ordering histories
│   ├── key_map.rs                   # Sorted-map ordering histories
│   ├── compatibility.rs             # Public signatures/results/callback counts
│   ├── traceability.rs              # Contract-ID manifest and family coverage
│   ├── conformance.rs               # Public-only concurrent/reopen histories
│   └── performance.rs               # Ignored paired performance/memory gates
├── compute_persistence.rs            # Feature 002 regression target
├── recovery.rs                       # Feature 001 regression target
└── fixtures/
    └── legacy/                       # Frozen fixtures; never regenerated

.github/workflows/
└── recovery.yml                     # Fast ordering target on three platforms
```

**Structure Decision**: Keep the single-crate layout. Each store module includes
its seam-driven tests as child unit modules, so they can access private map/WAL
state without widening visibility. Shared scheduling, shard-key, and fault-writer
support exists only under crate `cfg(test)`. External `tests/mutation_ordering`
targets use exported APIs only; they cannot consume library `cfg(test)` items or
`pub(crate)` helpers. WAL code owns only acceptance/rollback/fail-closed state and
never acquires a data-map guard. The corrected runtime adds no test API, second
coordination structure, or per-key retained state.

## Design Overview

### Universal mutation lifecycle

Every mutation follows this order:

1. Acquire `DashMap::entry(outer_key)` before observing presence or touching the WAL.
2. Read the accepted value through `OccupiedEntry`, or retain the guarded absence through `VacantEntry`.
3. Prepare the candidate result and callback return data without mutating the live entry. Compute callbacks operate once on a private working value.
4. Return under the documented no-op/eligibility rules when no accepted mutation is required.
5. Accept the single action or existing compute batch while the shard guard remains owned. The WAL lock is held only during encode/write/flush/rollback bookkeeping.
6. After acceptance, publish through `OccupiedEntry::insert/get_mut/remove` or `VacantEntry::insert`; never call top-level `DashMap::insert/remove/get/get_mut` while an entry guard is live.
7. Release the entry guard before invoking post-removal callbacks or returning.

The invariant is `shard → prepare/callback → WAL acceptance → publication →
release shard`. A WAL-held function never calls back into a store or DashMap.

### Mutation family migration

- **Key/value**: migrate `put`, `set_number`, and `remove`; retain the already shard-first `compute`, `increment_or_init`, and `decrement` paths.
- **Key/set**: migrate `append`, member removal, callback removal, and outer-key removal; retain and regression-test all four compute variants.
- **Key/sorted-map**: migrate `put`, entry removal, callback removal, outer-key removal, and pre-acceptance pop behavior; retain and regression-test ordered append and all three compute variants.
- **Final collection member/entry**: accept one outer-key delete action and then remove the occupied entry. Do not emit a separately accepted member/entry removal followed by delete.
- **Missing member/entry and absent outer key**: preserve existing logical and callback outcomes. The vacant/occupied entry still establishes order before any compatibility WAL action.
- **Pops**: identify and clone the candidate before acceptance, persist removal/delete, then mutate live state. Preserve the existing signature and leave review issue #8's returned-value defect untouched.

### WAL acceptance boundary

Retain existing frame types and ownership-returning store-event helpers, but
make their internal segmented-write/flush/rollback path return a result and
release the WAL guard before a compatibility panic. Single-action acceptance
advances the logical offset only after every existing segment write and flush
returns success. An explicit write or flush `Err`, including one after earlier
record bytes were written, restores the checkpoint when rollback succeeds.
Successful short writes without a later `Err` retain existing behavior and
remain issue #6. Compute batches retain their existing contiguous `write_all`
path while sharing rollback and WAL-health handling. No live entry changes until
the helper returns success.

On rollback failure, publish nothing, release the shard, mark the single WAL
state fail-closed with both error summaries, and reject later mutations without
additional writes. Reads remain available; repair or reopening of the retained
uncertain artifact remains issue #4. This narrow change is required by FR-010
and SC-006 and does not define successful-short-write repair, corrupt-prefix
recovery, or physical power-loss durability.

### Callback and async boundaries

- Synchronous compute panic unwinds before WAL acceptance or publication; the private working value and non-poisoning entry guard drop.
- Dropping an asynchronous compute future before acceptance drops its private working value and entry guard, leaving the accepted state unchanged.
- Removal callbacks run only after the occupied entry is consumed and its shard guard is released.
- Recursive callback access to the same map/shard remains unsupported and documented as a possible self-deadlock.
- Historical issue #3 scope kept the `compute_async` shard guard across `.await`.
  Follow-up issue #7 supersedes that boundary with one-shot optimistic snapshot
  validation: no guard across `.await`, no callback retry, and `WouldBlock` on a
  changed same-key value.

### Deterministic concurrency seam

Public operations expose no pause point between durable acceptance and live
publication, so ordinary-versus-ordinary RED reproduction needs a private,
per-store `cfg(test)` lifecycle observer. It reports semantic phases such as
`AcceptanceEntered`, `AcceptedBeforePublication`, and `Published` and can park a
single labeled mutation with RAII release on panic. The observer and its tests
compile only in crate unit-test builds. Tests use it only to schedule; assertions
compare public live reads, operation results, and three public reopenings.

DashMap's `raw-api` feature is enabled only for test builds to choose keys in
the same or different coordination shards without depending on hash formulas or
shard counts. Tests assert progress and final state, never shard numbers or guard
identity. Store child unit modules pass closures over their private maps to the
opaque selector. External integration tests do not use observer, shard, or WAL
fault internals. A bounded channel timeout is only a deadlock watchdog; no sleep
is used for scheduling.

### Read, interruption, and traceability evidence

- **Overlapping reads**: public callback-controlled integration tests prove
  private working collections are invisible. Store unit tests additionally park
  at `AcceptedBeforePublication`; a public read may block or return the complete
  old state, then must return the complete new state after release.
- **Process interruption**: a crate-unit child-process harness exits without
  destructors before acceptance, after acceptance/before publication, and after
  publication. The parent reopens the artifact three times through public
  `try_init_new`; a same-key contender contributes no action while blocked.
- **Callback counts**: public integration cases use atomic invocation counters
  for eligible, ineligible, panic, cancellation, overlap, and rejection paths.
- **Traceability**: contract IDs (`CMO-ORDER`, `CMO-READ`, `CMO-CALL`,
  `CMO-PREFIX`, `CMO-CROSS`, `CMO-FAIL`) map every acceptance case to its FR/SC,
  exact test, layer, schedule, store family, and public assertion.

### Performance and retained-memory validation

Capture the complete paired baseline before production changes, including the
working-diff checksum because feature 002 is currently uncommitted. Each of the
36 cells uses fixed data, warmups, at least 11 measured samples, setup outside
the timed window, barrier-synchronized workers, per-call latencies, and sample
wall throughput. Report raw measurements and exact ratios; fail the specific
cell that violates SC-004.

For SC-005, compare corrected and pre-feature quiescent memory after 1,000 and
1,000,000 unique-key create/delete cycles. Subtract or pair the pre-existing
DashMap bucket-capacity behavior so only added ordering memory is judged. The
selected design has no per-key coordinator, so added retained coordination
state should remain zero after guards drop.

## TDD Delivery Strategy

Each numbered item is one RED-GREEN tracer; do not write all listed tests before
production work.

1. Add the private semantic observer and one file-backed key/value `put↔put` tracer that forces accepted-before-published inversion and proves live/three-reopen mismatch (RED); acquire the KV entry before acceptance and publish through it (GREEN).
2. Add `set_number↔put`, then `remove→put`, as separate KV cycles; migrate only the failing method each time and preserve numeric/error outcomes.
3. Add one key/set `append↔same-member removal` tracer (RED) and migrate only append (GREEN); then add a final-member removal tracer (RED) and migrate removal with one delete action (GREEN). Two appends are intentionally not the tracer because set insertion is commutative and cannot expose an order inversion through public state.
4. Repeat separately for set callback removal and outer-key removal, invoking callbacks only after guard release.
5. Add one sorted-map replacement `put↔put` tracer (RED) and migrate only put (GREEN); then add a final-entry removal tracer (RED) and migrate removal/delete (GREEN).
6. Add pop-first, pop-last, removal-callback, outer-key removal, and ordered-append pairings one at a time. Move pop live mutation after WAL acceptance without changing its separately reviewed return semantics.
7. Pair ordinary mutations with synchronous/conditional compute methods one direction at a time for all stores; count every eligible/ineligible callback and preserve at-most-once eligibility and contiguous compute batches.
8. Add asynchronous set compute cancellation and synchronous callback panic tests (RED where missing), then make only the minimum working-copy/guard cleanup correction needed for GREEN.
9. Inject an explicit write `Err` after earlier record bytes, then a flush `Err`, as separate WAL/store cycles; verify exact rollback, no publication, released progress, and prior reopened prefix. Add rollback-failure fail-closed behavior separately. Do not add a successful-short-write acceptance test.
10. Add non-overlapping completion-before-invocation tests, then overlapping either-order tests, requiring live and three reopened states to agree.
11. Add callback-controlled and accepted-before-publication overlapping-read tests for all stores, proving only complete public state is observable.
12. Enable test-only opaque shard selection in crate unit tests. Add different-shard progress during callback preparation, WAL acceptance, and accepted-before-publication as separate cycles; also prove same-shard contention remains correct.
13. Add child-process interruption tests before acceptance, after acceptance/before publication, and after publication; include a blocked same-key contender and three public reopenings.
14. Grow the fast mixed-family matrix until every public mutation family participates at least once and every case has a stable contract ID, running accumulated feature tests after every GREEN.
15. Capture the 36-cell pre-feature performance baseline before the first production mutation change; after all GREEN cycles, run the identical candidate matrix and resolve any failing cell without weakening thresholds.
16. Run ignored 10,000 same-key histories and 1,000 different-shard schedules per store in crate unit tests, with three reopenings after each accepted history; run the public integration conformance matrix separately.
17. Run the paired 1,000/1,000,000-key retained-memory validation.
18. Refactor duplicated entry-first logic only while GREEN, rerunning targeted, feature, compute-persistence, and recovery tests after each refactor.
19. Run formatting, full all-target/all-feature tests, feature-level Clippy comparison, frozen fixtures, contract traceability, Rustdoc, and the three-platform fast ordering workflow.

## Complexity Tracking

No constitution violation remains. Two compatibility costs are intentional:

| Choice | Why required | Simpler alternative rejected |
|---|---|---|
| Private unit-test lifecycle observer and child-process exit seam | The failing interval and no-destructor interruption have no public pause point; deterministic RED/FR-020 evidence cannot otherwise be forced | Sleep/stress-only tests and in-process drops do not prove the required semantic boundary |
| Existing shard guard held through WAL I/O and async callback | The user selected DashMap shard coordination and at-most-once callbacks with no retry/conflict API | Dropping/reacquiring the guard recreates ordering races; exact-key or striped side locks violate FR-022 |
| Constant-sized fail-closed WAL health after rollback failure | Constitution II forbids further writes to an artifact whose bytes no longer provably match its offset | Continuing at the old offset risks compounding an uncertain durable prefix; repairing that artifact remains issue #4 |
