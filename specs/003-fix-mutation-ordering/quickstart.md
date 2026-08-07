# Quickstart: Validate Concurrent Mutation Ordering

## Prerequisites

- Rust toolchain compatible with the crate's 2021 edition.
- Repository root as the working directory.
- Features 001 and 002 recovery/compute-persistence code and tests present.
- Design references: [data-model.md](data-model.md), [research.md](research.md), and [concurrent-mutation-ordering.md](contracts/concurrent-mutation-ordering.md).
- The pre-feature performance baseline captured before changing production mutation paths.

## RED-GREEN Workflow

Implement one observable behavior at a time:

1. Add one behavior-focused test through a public store operation.
2. When the behavior needs a private semantic gate, exact shard selection, fault
   writer, or process-exit checkpoint, place it in a crate unit-test child module.
   External integration tests use exported APIs only.
3. Run that exact test and confirm it fails for the expected live/reopen mismatch (RED).
4. Change only the mutation path required by that test.
5. Re-run the exact test and confirm it passes (GREEN).
6. Run the affected store's unit tests plus the public-only `mutation_ordering` integration target.
7. Refactor only while GREEN, then rerun compute-persistence and recovery regressions.

Do not create the complete matrix before production work. Each subsection below
is a separate RED-GREEN cycle unless it explicitly describes an ignored final
conformance/report run.

For every RED record:

- exact test command;
- expected failing invariant;
- actual failure showing live state differs from reopened state or progress contract;
- confirmation that the test passes only after the minimal shard-first change.

## 1. Capture the Immutable Performance Baseline First

Create the feature's ignored benchmark harness before production changes, then
run it against the current worktree. Record the current commit plus a checksum
of the working diff because feature 002 is not represented by the bare commit.

```bash
git rev-parse HEAD
git diff --binary | sha256sum
rustc -Vv
cargo test --release --test mutation_ordering performance::paired_baseline -- --exact --ignored --nocapture --test-threads=1
```

Record OS/kernel, CPU model, filesystem/temp directory, release flags, command,
raw cell rows, and timestamp. Keep baseline rows machine-readable with:

```text
store,mode,profile,workers,samples,ops_per_sample,median_throughput,p95_latency
```

Required 36 cells:

| Dimension | Values |
|---|---|
| Store | key/value, key/set, key/sorted-map |
| Mode | vector, file |
| Profile | ordinary write, successful remove, minimal callback |
| Workers | 1 same-key, 8 distinct-key |

Use five untimed warmups and at least 11 measured samples. Prepopulate and reset
outside timed windows; synchronize worker starts with barriers; record both wall
throughput and per-public-call latency.

## 2. Key/Value Tracer Bullet

Start with one file-backed same-key `put↔put` interleaving:

1. Seed an accepted value.
2. Park mutation A at `AcceptedBeforePublication` through the private observer.
3. Invoke mutation B for the same key and confirm the buggy implementation can accept/publish it out of order.
4. Release A, join both calls, read live state, drop all store handles, and reopen three times.
5. Assert live and every reopened value are identical.

```bash
cargo test key_value_store::mutation_ordering_tests::concurrent_puts_keep_live_and_reopened_order -- --exact --nocapture
```

Expected RED: the current WAL-first path accepts A then B but can publish B then
A, so live state disagrees with reopening. GREEN requires acquiring the entry
before A can be accepted and retaining it through publication.

Observed T017 RED on 2026-08-06 with the exact command above: the reopened value
was `second` while the live value was `first`, and the test failed at the public
live/reopen equality assertion. This is the intended CMO-ORDER-2 failure.

Observed T019 RED with `set_number(42)` parked after acceptance: reopening
returned `second` while live state contained the native-endian bytes for `42`.
The failure disappeared only after `set_number` reused the entry-first put path.

Observed T021 RED with an absent-key delete parked after acceptance: the later
put was present after reopen while the live store was absent after the parked
delete published. Entry coordination for both vacant and occupied removal is
therefore required.

Repeat as separate cycles for:

- `set_number↔put`;
- `remove→put` deletion/recreation;
- `put→compute` and `compute→put`;
- `set_number→increment_or_init` and `increment_or_init→decrement`.

## 3. Key/Set Tracer Bullet

Use `append→remove_from_set` on one outer key/member, then compare exact live
membership and outer-key presence after three reopenings.

```bash
cargo test key_set_store::mutation_ordering_tests::append_and_remove_keep_live_and_reopened_order -- --exact --nocapture
```

GREEN requires entry-first append/removal. When the removed member is final,
accept one outer-key delete before removing the occupied entry.

Repeat one cycle at a time for:

- non-final and final member removal;
- removal callback, including callback-after-guard-release progress;
- outer-key removal followed by recreation;
- ordinary↔sync compute in both directions;
- eligible conditional compute variants;
- sync compute↔async compute;
- a multi-action compute result conflicting with an ordinary mutation.

Skipped conditional and exact no-op cases must not create a durable/live change.

Observed T024 RED with duplicate append parked after acceptance and same-member
removal completing first: reopening contained only the sentinel while live state
also contained the reinserted member. The non-commutative pairing replaces the
infeasible append/append draft tracer, since two set appends commute publicly.

Observed T026 RED with a final-member removal parked after its first accepted
record: a same-key compute completed inside the logical removal before the outer
delete decision. The corrected path must guard first and represent a final
removal with one outer-key delete.

Observed T028 RED on callback removal: the overlapping compute changed finality
inside the WAL-first gap, so the required final-removal callback ran zero times.
The corrected path must decide and publish under the shard, then invoke the
callback after consuming the entry guard.

Observed T030 RED with direct set deletion parked after acceptance: live state
was absent after the parked delete published, while reopening retained the later
recreation. Vacant and occupied outer-key removal now use the entry guard.

## 4. Key/Sorted-Map Tracer Bullet

Use a same-outer-key `put→remove_from_sorted_map` interleaving and assert the
complete ordered map through three reopenings.

```bash
cargo test key_map_store::mutation_ordering_tests::put_and_remove_keep_live_and_reopened_order -- --exact --nocapture
```

Repeat separately for:

- replacement `put↔put`;
- non-final and final entry removal;
- removal callback and outer-key recreation;
- ordinary↔compute in both directions and compute↔compute;
- `pop_first` and `pop_last` paired with a same-key mutation;
- `append_ordered_element` paired with a same-key mutation.

Pop tests assert state/order and restart parity. Do not change or add assertions
for review issue #8's returned-value defect in this feature.

Observed T033 RED for replacement puts of one search key: reopening contained
`second` while live state contained `first`. This is the sorted-map
CMO-ORDER-2 WAL-first/publication inversion.

## 5. Real-Time and Overlap Contract

Test completion-before-invocation without concurrency: complete A, invoke B,
then require live state and all reopenings to reflect A before B.

For overlapping calls, force both acquisition possibilities. Either result is
valid only when live state and all three reopenings agree.

```bash
cargo test --test mutation_ordering contract_order_nonoverlap -- --nocapture --test-threads=1
cargo test mutation_ordering_tests::overlap_uses_one_live_and_reopened_order -- --nocapture --test-threads=1
```

No FIFO assertion is permitted for overlapping invocation start order.

## 6. Different-Shard Progress

Use the test-only opaque key selector; do not hardcode shard counts or hash
formulas. For each store, select key A and key B in different shards and test:

1. **During callback preparation**: park A's compute callback; B must complete.
2. **During durable acceptance**: B may prepare on its shard but waits only for the WAL acceptance interval.
3. **After acceptance, before publication**: B must complete while A remains parked.
4. **Same-shard control**: two different keys may wait, but their final states remain independent and restart-consistent.

```bash
cargo test mutation_ordering_tests::different_shard -- --nocapture --test-threads=1
cargo test mutation_ordering_tests::same_shard -- --nocapture --test-threads=1
```

Use channel handshakes for positive progress and a generous `recv_timeout` only
as a deadlock/starvation watchdog. These are crate unit tests because external
integration targets cannot access private `cfg(test)` observers, map fields, or
WAL fault writers. Do not schedule with sleeps.

## 7. Panic, Cancellation, and Persistence Rejection

Repeat RED-GREEN per failure boundary:

- synchronous callback panic caught by the test: original live state, prior reopened state, later same-key mutation succeeds;
- async callback reaches a pending point and its future is dropped: same outcomes;
- one injected explicit write `Err` after earlier record bytes, with successful rollback;
- one injected flush rejection with successful rollback;
- one rollback failure that publishes nothing, marks the WAL fail-closed, and rejects later mutations without another writer call;
- existing compatibility panic occurs after WAL lock release;
- removal callbacks never run while the shard remains guarded;
- every eligible callback counter is exactly one and every ineligible counter is zero.

```bash
cargo test mutation_ordering_tests::callback_panic_publishes_nothing -- --exact --nocapture
cargo test mutation_ordering_tests::dropped_async_compute_publishes_nothing -- --exact --nocapture
cargo test mutation_ordering_tests::rejected_acceptance_releases_same_key -- --exact --nocapture
```

Each failure assertion uses the public store state and, where the prefix is
recoverable, three reopenings. Rollback-failure repair remains outside this
feature. Do not add a test that treats a successful `Ok(n < requested)` write as
FR-010 rejection; successful short-write repair remains issue #6.

## 8. Overlapping Reads and Process Interruption

For FR-009, use two complementary layers:

1. Public integration callbacks pause after changing only their private set/map
   candidate. A concurrent public read may return the complete old state or
   block, but never returns the candidate or a partial result.
2. Store unit tests park at `AcceptedBeforePublication`. A public read may block
   or return complete old state; after release it returns complete new state and
   three reopenings agree.

```bash
cargo test --test mutation_ordering callback_working_state_is_invisible -- --nocapture --test-threads=1
cargo test mutation_ordering_tests::accepted_before_publication_read_is_atomic -- --nocapture --test-threads=1
```

For FR-020, the crate-unit test executable spawns itself as a child and exits
without destructors at semantic checkpoints. The parent expects the dedicated
exit code and performs three public reopenings:

- before acceptance: prior prefix only;
- after complete acceptance/before publication: complete mutation appears once;
- after publication/before return: reopened state matches publication;
- same-key B blocked behind A: B contributes no WAL action.

```bash
cargo test mutation_ordering_tests::process_interruption -- --nocapture --test-threads=1
```

Do not terminate during frame writing or flush; partial-tail recovery remains
issue #4.

## 9. Fast Family Matrix and Traceability

Grow `tests/mutation_ordering/{key_value,key_set,key_map}.rs` one pairing at a
time until every public mutation family appears in at least one same-key
ordering test. Assign every acceptance case a stable `CMO-*` contract ID and
record requirement, layer, store/family, schedule, exact test name, and public
assertion in `tests/mutation_ordering/traceability.rs`. Run after every GREEN:

```bash
cargo test --test mutation_ordering -- --nocapture --test-threads=1
cargo test --test compute_persistence -- --test-threads=1
cargo test --test recovery -- --test-threads=1
```

The traceability manifest must reject duplicate IDs/names and prove that every
mutation family in the contract has at least one mapped case.

## 10. Ignored Conformance Runs

After the fast matrix is GREEN, run the expensive deterministic requirements:

```bash
cargo test --release mutation_ordering_tests::conformance_same_key_10k -- --ignored --nocapture --test-threads=1
cargo test --release mutation_ordering_tests::conformance_different_shard_1k -- --ignored --nocapture --test-threads=1
cargo test --release --test mutation_ordering conformance::public_histories -- --exact --ignored --nocapture --test-threads=1
```

- Rotate deterministically through the family matrix.
- Run at least 10,000 controlled same-key histories for each store.
- Run at least 1,000 controlled different-shard schedules for each store.
- Compare public live snapshots with three consecutive reopenings after every accepted history.
- Report zero durable/live mismatches.

The controlled 10k/1k schedules are crate unit tests because they need private
semantic gates and exact shard selection. The integration conformance target is
public-only and does not claim control of hidden lifecycle phases.

Observed final same-key conformance run on 2026-08-06: all three release lanes
completed 30,000/30,000 controlled histories in 109.14 seconds. Every accepted
history matched the live state across three consecutive reopenings, for 90,000
successful reopen assertions and zero durable/live mismatches.

Final compatibility verification on 2026-08-06 preserved all three frozen
fixture SHA-256 values from `tests/fixtures/legacy/README.md`. The public
recovery suite passed 18/18, WAL recovery passed 4/4, and compute persistence
passed 22/22 active cases (one release benchmark remained intentionally
ignored).

Final quality verification on 2026-08-06 passed `cargo fmt --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, and
`cargo doc --all-features --no-deps`. Strict Clippy initially exposed the new
duplicate inclusion of the shared cross-shard helper plus two unused new test
helpers; those were consolidated or removed. Its pre-existing API/style
diagnostics were repaired without changing public signatures. The complete
all-target/all-feature test run passed 144 active cases with 14 explicitly
ignored release/manual cases and zero failures.

## 11. Candidate Performance Gates

Run the identical harness and environment used for the pre-feature baseline:

```bash
cargo test --release --test mutation_ordering performance::paired_candidate -- --exact --ignored --nocapture --test-threads=1
```

Every cell passes independently:

- median one-worker same-key throughput ≥ 90% of matching baseline;
- median eight-worker distinct-key throughput ≥ 85%;
- p95 public-call latency ≤ 125%.

Write profiles alternate fixed 32-byte values/members/entries. Successful
removal profiles prepare a present target and retain a sentinel for set/map so
the timed operation measures ordinary removal rather than final-key deletion.
Minimal callbacks perform one deterministic change with no intentional work.

Do not average passing cells with failing cells or weaken a threshold after
seeing results.

## 12. Retained Ordering Memory

Compare the same baseline and candidate process after 1,000 and 1,000,000
unique-key create/delete cycles, with no active operation:

```bash
cargo test --release --test mutation_ordering performance::retained_ordering_memory -- --exact --ignored --nocapture --test-threads=1
```

Pair/subtract pre-existing DashMap capacity retention. Added memory attributable
to ordering after 1,000,000 cycles must be no more than 110% of the added amount
after 1,000 cycles. The design must contain no retained per-key lock/tombstone
registry.

## 13. Full Regression and Quality Gates

```bash
cargo test --all-targets --all-features -- --test-threads=1
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Also run the feature-001 recovery internals and verify frozen fixture checksums
remain unchanged. Run the traceability manifest and audit every `CMO-*` row
against the contract. Record pre-existing Clippy diagnostics separately; this
feature introduces none.

Current pre-change validation on 2026-08-06:

- full all-target/all-feature suite: 75 passed, 4 intentionally ignored;
- zero test failures;
- production mutation paths still contain the issue #3 WAL-first order and therefore provide the expected RED starting point.

Implementation baseline on 2026-08-06:

- `cargo fmt --check` and the new release benchmark target pass;
- the first serialized all-target/all-feature run stalled in
  `model::tests::test_dashmap_compute` for more than three minutes and was
  interrupted with exit status 130;
- the same test passed immediately when rerun alone under a 15-second bound,
  confirming a schedule-dependent existing DashMap lock-order test;
- with approval, that test was made deterministic by dropping each first guard
  before acquiring another and synchronizing the two threads at a barrier;
- the repeated all-target/all-feature run passed with 75 tests passed and 7
  ignored (the prior 4 plus 3 new ignored benchmark/report targets);
- no issue #3 release mutation path changed before this baseline completed.

Foundation checkpoint on 2026-08-06:

- the private test observer, shard-key selector, fault writer, watchdog, and
  public-only reopen helpers compile without changing release-visible APIs;
- the serialized all-target/all-feature suite passed with 75 tests passed and
  7 intentionally ignored after registering the private seams.

Key/map RED evidence on 2026-08-06:

- T033 demonstrated replacement `put` operations choosing different live and
  durable orders before entry-first coordination;
- T035 demonstrated a final-entry removal allowing a same-key compute to
  complete between durable acceptance and live publication.
- T037 demonstrated callback removal losing its one-shot callback when an
  overlapping compute changed map finality before live publication.
- T039 demonstrated an outer-key delete and recreation selecting opposite live
  and durable orders before entry-first delete coordination.
- T041 recorded `pop_first` publishing its live removal before entering durable
  acceptance; its state assertions intentionally exclude issue #8 return data.
- T043 recorded the symmetric `pop_last` publication-before-acceptance defect,
  likewise without asserting the separate return-value behavior.

User Story 1 fast checkpoint on 2026-08-06:

- `cargo test --test mutation_ordering -- --test-threads=1` passed 13 tests
  with 3 release-only performance tests intentionally ignored;
- CMO-ORDER-1 is covered by completion-before-invocation cases, CMO-ORDER-2
  by controlled and public overlapping cases that permit either accepted order,
  and CMO-ORDER-3 by indivisible set/map compute batches;
- every public mutation family in the contract participates in a live-state and
  three-reopen matrix case;
- public signatures are unchanged, and pop return values remain the separate
  issue #8 behavior while their state ordering is corrected.

User Story 2 fast cross-shard checkpoint on 2026-08-06:

- the accumulated private mutation-ordering target passed 24/24 tests with
  `--test-threads=1`, followed by the three file-backed cross-shard controls;
- CMO-CROSS-1 and CMO-CROSS-3 allow different-shard completion during
  preparation and accepted-before-publication; the async set preparation case
  behaves the same;
- CMO-CROSS-2 permits another shard to prepare but blocks its acceptance while
  a scripted writer owns the WAL, and CMO-CROSS-4 permits same-shard waiting;
- every bounded wait completed without deadlock or timeout, and live state
  matched three consecutive reopenings for all three stores.
- all three ignored different-shard release lanes passed 1,000 schedules each
  (3,000/3,000 total) in 12.02 seconds, with three reopenings after every
  accepted history;
- the public-only concurrent-history smoke matrix passed for key/value, set,
  and sorted-map stores without making an exact-shard claim.
- retained-memory candidate rows were 589,824 bytes at 1,000 cycles and
  138,039,296 bytes at 1,000,000 cycles; after subtracting the immutable
  baseline, added ordering retention was 360,448 and 0 bytes respectively,
  passing the 110% growth gate with zero retained live keys;
- preliminary 1–8-operation samples varied drastically under host contention
  and are preserved only as the failing `post-ordering` snapshot. The stable
  harness instead required at least 1,024 operations and 100 ms per sample,
  then used per-cell medians across three complete reconstructed-baseline and
  candidate matrices;
- final performance passed 36/36 independent cells without weaker thresholds:
  the lowest one-worker throughput ratio was 0.929685 (minimum 0.90), the
  lowest eight-worker ratio was 0.923004 (minimum 0.85), and the highest p95
  ratio was 1.083975 (maximum 1.25);
- final retained-memory deltas were 393,216 bytes at 1,000 cycles and
  138,043,392 bytes at 1,000,000 cycles. After immutable-baseline subtraction,
  added ordering retention was 163,840 and 0 bytes with zero retained keys;
- the key/set append optimization uses borrowed-key serialization proven
  byte-identical to the legacy WAL frame. Full rows and reconstruction details
  are recorded in `benchmarks/final.csv` and `benchmarks/final.md`.

WAL rejection checkpoint on 2026-08-06:

- the accumulated WAL target passed 13 tests with 2 historical tests ignored;
- CMO-FAIL-1 restored exact pre-record bytes and offset after a third-segment
  write error, released the WAL guard, and accepted a later operation;
- CMO-FAIL-2 did the same for a flush error and recorded exactly two rejected
  flush calls before later progress;
- CMO-FAIL-3 preserved both original-write and rollback causes in one error,
  transitioned the constant-space health state to `FailedRollback`, and made a
  later mutation fail before any additional writer call.

Callback, read, and interruption checkpoint on 2026-08-06:

- eligible callbacks ran exactly once, conditional callbacks remained at zero,
  panic and dropped async candidates published nothing, and later same-key
  operations progressed;
- public reads launched against callback working state waited at the existing
  shard guard and then observed only the complete published set/map;
- subprocess exits before acceptance reopened the prior prefix, exits after
  acceptance or publication reopened the complete accepted prefix (including
  multi-action set/map batches), and a blocked contender contributed no WAL
  action; all child waits were bounded and all three stores reopened cleanly.

Final design audit on 2026-08-06 found that every mutation uses the existing
DashMap shard/entry coordination plus the existing shared WAL interval. There
is no global mutation lock, retained per-key registry, new WAL action, public
test hook, async retry/conflict redesign, or change to issue #8 pop return
semantics. Mutation observers, shard selection, fault writers, and process
checkpoints remain crate-private under `cfg(test)`. The contract, data model,
traceability manifest, and implementation agree on all 20 CMO identifiers.

## Expected Result

- Every mutation acquires its existing DashMap shard before durable acceptance.
- Live publication occurs only after acceptance and through the same entry guard.
- Same-key live and reopened state always use one order.
- Different-shard keys remain concurrent outside the brief WAL interval.
- Same-shard unrelated keys may block as explicitly accepted.
- Panic, cancellation, and rejected acceptance publish nothing and release coordination.
- Overlapping reads observe only complete public state; callback working state is invisible.
- Semantic-boundary process interruption reopens one accepted prefix with no blocked-contender action.
- Rollback failure publishes nothing and makes the WAL fail closed without another write.
- No public signature, WAL action identifier, or frozen artifact changes.
- Every acceptance case is mapped to a stable `CMO-*` contract ID.
- All 36 performance cells and retained-memory validation pass their clarified gates.
