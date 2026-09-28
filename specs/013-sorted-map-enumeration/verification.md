# Sorted-map enumeration: verification

## Baseline at `37db5bb`, before production edits
`cargo test --all-targets --all-features -- --test-threads=1` gave 622 passed, 0 failed, 28 ignored
across 27 binaries.

## RED
- **T002.** `for_each_sorted_map` first landed as a stub that visits nothing. All five tests in
  `tests/sorted_map_enumeration.rs` failed on an assertion, not at compile time:
  - `for_each_sorted_map_visits_every_published_map_once` at the visited-keys `assert_eq!`;
  - `an_unchanged_map_is_visited_exactly_once_while_other_keys_change` at its progress check,
    because a visit that reads nothing finishes before the writer moves;
  - `reads_progress_while_a_map_visit_is_blocked_and_writers_finish_after_it` at "the visit
    started";
  - `a_reopened_store_is_visited_as_it_was_written` at `!written.is_empty()`;
  - `a_visit_writes_nothing_to_the_wal` at "the visit read the store".
- **T003.** With `sorted_map_enumeration` added to the target list in `tests/ci_workflow.rs`,
  `recovery_workflow_runs_every_dedicated_issue_regression_target` failed: "recovery workflow must
  run `cargo test --test sorted_map_enumeration -- --test-threads=1`". Adding the line to
  `recovery.yml` turned it green.

## GREEN
- **`tests/sorted_map_enumeration.rs`:** 5 passed, three runs in a row.
- **Full suite:** 627 passed, 0 failed, 28 ignored across 28 binaries. That is the baseline plus
  the 5 new tests and their binary.
- **`cargo fmt --check`:** clean.
- **`cargo clippy --all-targets --all-features`:** no warnings.

## Neutralization probes
Each probe replaced the method body, ran `tests/sorted_map_enumeration.rs`, and restored the file
from a backup (sha256 verified).

| Probe | Change | Caught by |
|---|---|---|
| P1 | visit only the first key | all five tests |
| P2 | visit every key twice | the visits-every-map test and the exactly-once test |
| P3 | skip keys whose map is empty | not caught (see below) |
| P4 | take write guards instead of read guards | the reads-progress test |
| P5 | copy every map out, then visit with no guard held | the reads-progress test (its anti-vacuity check) |
| P6 | skip keys whose last byte is even | four tests |

**Why P3 is not caught.** No public write publishes an empty map. Removing a map's last entry writes
a delete event and removes the key, and a compute that leaves its map empty publishes nothing. Only
replay of WAL bytes written by an older revision, whose final removal was a plain removal event,
can load one. The spec states that such a key is visited, so the enumeration agrees with
`contains_key` and `size()`. The implementation filters nothing.

**What no test sees.** "Nothing is copied" is observable only through allocation counting. P5 copies
and is caught, but only because it also drops the guards. A copy made while the guard is held would
pass every test here.
