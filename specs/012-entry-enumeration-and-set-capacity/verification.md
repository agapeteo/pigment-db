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
- **Hysteresis.** `alternating_appends_and_removals_do_not_resize_every_time` counts a set's table
  allocations, which are 16-byte aligned, on the test's own thread.
  - Against a shrink-to-fit rule it reported 400 table allocations in 400 alternating calls at the
    full-table length 112.
  - Against `shrink_to(2 * len)` it reports at most 2.
  - `growing_a_set_is_counted` is its positive control.

## GREEN
- **`tests/entry_enumeration.rs`:** 12 passed.
- **`tests/set_capacity_hysteresis.rs`:** 2 passed.
- **Full suite:** 621 passed, 0 failed, 28 ignored across 27 binaries. That is the baseline plus the
  14 new tests and their 2 binaries.
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

**Why P2 is not caught.** The entry path's non-final branch runs only when the fast path saw a
final-member removal and a concurrent append made it non-final before the entry was taken. The set
then arrives from an operation that already holds FR-3, so without the release it could exceed
`4 * len` by at most three slots after the removal. It is kept so that every removal path applies
the same rule.

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
- `is_multiple_of` (1.87).
