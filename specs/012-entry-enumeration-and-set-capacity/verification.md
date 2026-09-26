# Entry enumeration and bounded set capacity: verification

## Baselines at `d5ad6ee`, before production edits
- **Suite:** `cargo test --all-targets --all-features -- --test-threads=1` gave 607 passed,
  0 failed, 28 ignored across 25 binaries.
- **Performance:** plan.md records W1 and W2.

## RED evidence, in task order
- **T002/T003.**
  - Both methods first landed as stubs that visit nothing.
  - All seven FR-1/FR-2 tests failed on their assertions, for example
    `for_each_entry_visits_every_published_entry_once` at `assert_eq!(seen, expected)`.
  - The FR-3 tests also failed at the stubs, in the helper that reads a set through the
    enumeration. That is not a RED for FR-3, so it was not counted.
- **T004.** With the enumeration real and no capacity rule, every FR-3 test failed on its bound:
  - `try_remove_from_set`: 10 members, capacity 12,983 to 13,228 across runs;
  - `try_remove_from_set_callback`: 10 members, capacity 13,019 to 13,099;
  - `try_compute`: 10 members, capacity 12,843 to 13,097;
  - a reserving compute callback on an occupied key: 2 members, capacity 114,688.
- **T004, `capacity()` is not the trigger.** Those figures show `capacity()` understating a table
  sized for 14,336. The first plan triggered on `4 * len < capacity()`, and was replaced by an
  unconditional `shrink_to(2 * len)` before any production edit.
- **T005.** With T004 in place, only `a_reopened_store_holds_every_set_within_its_capacity_bound`
  still failed: 10 members, capacity 13,157 after reopen.
- **Hysteresis, as first committed (`b04220a`).** `alternating_appends_and_removals_do_not_resize_every_time`
  counted 16-byte-aligned allocations on the test's own thread, at the one length 112.
  - Against a shrink-to-fit rule it reported 400 table allocations in 400 alternating calls.
  - Against `shrink_to(2 * len)` it reported at most 2.
  - The review found this too narrow (see "Review"). The binary now calibrates a set table's
    alignment and sizes at run time and sweeps the rule's own thresholds.

## GREEN
- **`tests/entry_enumeration.rs`:** 12 passed.
- **`tests/set_capacity_hysteresis.rs`:** 3 passed.
- **Full suite:** 622 passed, 0 failed, 28 ignored across 27 binaries. That is the baseline plus
  the 15 new tests and their 2 binaries.
- **`cargo fmt --check`:** clean.
- **`cargo clippy --all-targets --all-features`:** no warnings.

## Neutralization probes
Each probe removed one `release_spare_capacity` call, ran both new test files, and restored the file
(sha256 verified).

| Probe | Site | Caught by |
|---|---|---|
| P10 | set loaded at open | `a_reopened_store_holds_every_set_within_its_capacity_bound` |
| P1 | `try_remove_from_set_core`, fast path | `a_set_cut_down_by_remove_from_set_releases_its_spare_capacity` |
| P2 | `try_remove_from_set_core`, entry path | not caught (see below) |
| P3 | `try_compute`, occupied | the reserving-callback test and the compute-variant test |
| P4 | `try_compute`, vacant | the reserving-callback test (`vacant`) |
| P5 | `try_compute_async`, occupied | the compute-variant test (`try_compute_async`) |
| P6 | `try_compute_async`, vacant | the reserving-callback test (`async`) |
| P7 | `try_compute_if_present` | both compute tests |
| P8 | `try_compute_if_absent` | the reserving-callback test (`absent`) |
| P9 | `try_remove_from_set_callback_core` | `a_set_cut_down_by_the_callback_removal_releases_its_spare_capacity` |

**Why P2 is not caught.** The entry path's non-final branch runs only under a race. The fast path
saw either no key or a final-member removal, and a concurrent append or compute on the same key
changed that before the entry was taken. The set
then arrives from an operation that already holds FR-3, so without the release it could exceed
`4 * len` by at most three slots after the removal. It is kept so that every removal path applies
the same rule.

## Review
An adversarial review of `b04220a` ran four lenses: Principle VI and scope, enumeration, capacity,
and tests. Every finding was then put to a refuter. None was a code defect; all were upheld except
the one noted below, and all are fixed in the follow-up commit.

- **The enumeration contract was false (MEDIUM, three reviewers).** "Readers do not wait" does not
  hold once a compare-exchange batch writes to the guarded part: it holds the store-wide gate while
  it waits, so every read waits too. A batch queued behind a writer waiting on that part does the
  same, and on a file-backed store so does a queued compaction. The guard is also held for the whole
  part, not for one `visit`, and the iterator holds the previous part's guard while it takes the
  next. The locking stays as it is. Taking the gate for the whole visit would stall every batch, and
  everything behind one, for the whole visit. The docs, FR-1 and plan IV now state the real
  contract.
- **Capacity prose (LOW).**
  - `shrink_to(2 * len)` gives the smallest table holding `2 * len`, not "at most twice".
  - Insertion keeps capacity at most `4 * len`, not below it. A duplicate append on a half-tombstoned
    table can double it to exactly `4 * len`.
  - The hysteresis is amortised, not an exact doubling in each direction.
  - The release runs after WAL acceptance and before publication, not after publication.
- **The tests did not tell the rule from its likeliest regressions (LOW).** M1 to M3 below passed all
  14 tests; M4 was already caught at length 112. Each is now caught:

  | Mutant | Caught by |
  |---|---|
  | M1: trigger gated on `capacity() > 4 * len` | `a_full_table_cut_down_is_released_by_its_real_size` |
  | M2: `shrink_to(len + 1)` | `alternating_appends_and_removals_do_not_resize_every_time` (sweep) |
  | M3: release only below 64 members | the real-size test, and the large cut in `a_set_cut_down_by_remove_from_set_releases_its_spare_capacity` |
  | M4: shrink to fit | the sweep |

  - M1 is the rule FR-3 excludes. A table filled before the cut leaves most removed slots as
    tombstones, so `capacity()` follows `len` down while the table stays at six to seven times the
    length. Only an allocation measurement can see that.
- **The allocation test assumed x86 (LOW).** It counted 16-byte-aligned allocations. On NEON and the
  generic 64-bit hashbrown, a set table is 8-aligned, so the positive control would have failed
  there and the test would have passed vacuously. It now calibrates alignment and sizes from sets of
  known bucket counts.
- **The exactly-once churn never removed anything (LOW).** Every removal targeted a key that was never
  inserted, because `index % 500` keeps the parity of `index`. The key is now taken from `index / 2`.
- **Records (LOW).**
  - The plan's capacity range disagreed with this file. It is corrected here; the `b04220a` commit
    message keeps the narrower range.
  - T006 claimed an MSRV build that was not run. It is now T008, open until CI.
  - The P2 explanation above is corrected.
  - The Principle VI record now names every place the consumer appears.
- **Refuted.** A removal that resizes holds its shard's write guard for O(len): 62 ms at a million
  members. The refuter found the cost bounded by the hysteresis and smaller than the growth rehash
  the same table made on append. The plan now states it.

## Performance gate (plan.md)
Candidate, same crate and machine, three sets of five release runs:

| Workload | Medians (s) | Candidate | Gate | Change |
|---|---|---|---|---|
| W1 | 0.063544, 0.065238, 0.066026 | 0.065238 | 0.078225 | +8% |
| W2 | 0.040662, 0.040376, 0.041352 | 0.040662 | 0.050879 | +4% |

## Not verified locally
Rust 1.91 (the MSRV), macOS and Windows are covered by CI. No toolchain of that version is
installed here. The new code uses APIs stable by then:
- `HashSet::shrink_to` (1.56);
- `Waker::noop` (1.85);
- async closures (1.85);
- `is_multiple_of` (1.87);
- `std::iter::repeat_n` (1.82).
