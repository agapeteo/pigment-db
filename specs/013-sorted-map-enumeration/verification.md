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
from a backup (sha256 verified). The table is the final run, over the tests as the review left them;
P7 to P9 were added by the review.

| Probe | Change | Caught by |
|---|---|---|
| P1 | visit only the first key | all six tests |
| P2 | visit every key twice | the visits-every-map test and the exactly-once test |
| P3 | skip keys whose map is empty | not caught (see below) |
| P4 | take write guards instead of read guards | the reads-progress test and the file-backed test |
| P5 | copy every map out, then visit with no guard held | the reads-progress test (its anti-vacuity check) |
| P6 | skip keys whose last byte is even | five tests |
| P7 | hold every part's guard for the whole visit | the exactly-once, reads-progress and file-backed tests |
| P8 | take the maintenance gate exclusively | the file-backed test |
| P9 | take the maintenance gate shared | the file-backed test |

**Why P3 is not caught.** No public write or open publishes an empty map. Removing a map's last entry
writes a delete event and removes the key, and a compute that leaves its map empty publishes
nothing. Legacy and V1 WAL input is refused at open with `MigrationRequired`, and the migrator
rebuilds from the replayed snapshot, emitting only put frames, so it drops an empty key (the review
measured this with a crafted legacy WAL). The implementation filters nothing all the same.

**What no test sees.** "Nothing is copied" is observable only through allocation counting. P5 copies
and is caught, but only because it also drops the guards. A copy made while the guard is held would
pass every test here.

## Review
Three lenses reviewed `eb183ad` (Principle VI first, then the contract, then the tests), and two
refuters checked each of the ten findings. Nine survived, all LOW or MEDIUM; none was CRITICAL.
Fixed here:
- **Busy-waiting (MEDIUM, LOW).** A writer to the guarded part spins on dashmap's lock and never
  sleeps; the review measured one core busy for the whole of a one-second visit. The contract said
  only "waits", and the plan justified the cost by one consumer's usage. The rustdoc and FR-1 now say
  it in the library's terms and tell callers to keep `visit` short and non-blocking.
- **The visitor rule (MEDIUM).** "Call no method of the store" did not prevent a deadlock: a visitor
  waiting on another thread's write to a different part deadlocks once a compaction queues behind a
  writer on the guarded part (measured). The rule now also forbids waiting on anything that waits on
  a write to the store.
- **The empty-map claim (LOW).** "Replay of older WAL bytes can load an empty map" was false for
  every public open; plan.md and this record are corrected. The review also found that a crafted V2
  WAL holding an empty map makes compaction fail closed. No revision writes such bytes, so it is
  recorded here and not filed.
- **Tests (MEDIUM, LOW).**
  - "Writes to other parts proceed" had no test, so a visit holding every part's guard passed all
    five. `writers_to_other_parts_proceed` now asserts that one of 64 single-key writers finishes
    while the visit is blocked.
  - Every concurrency test used a memory store, whose maintenance gate is disabled, so a visit that
    took the gate went unseen. A file-backed test now asserts that a compaction completes, and that
    writes to other parts proceed, while a visit is blocked.
  - The exactly-once test's progress check counted writes outside the visit; it now reads the
    counter inside the visitor, at the first and last stable key.
  - The WAL-size check now also runs over the reopen test's maps of one to five entries.
- **Principle VI record (LOW).** The Motivation notes now name the consumer feature (VFS tag values
  and tag-name records, penpack V414, open), and plan.md's record lists every place the consumer is
  named. Refuted (two of two): that the notes had to present the directory-quota alternative, which
  the consumer had already declined.

### Principle VI
Every added line of code, rustdoc, test, test name, CI target and spec title was scanned for the
consumer's vocabulary. The consumer is named only in the Motivation notes of spec.md and plan.md,
in plan.md's Principle VI record and in its Release line, and in this record.

## GREEN after the review
- **`tests/sorted_map_enumeration.rs`:** 6 passed.
- **Full suite:** 628 passed, 0 failed, 28 ignored across 28 binaries.
- **`cargo fmt --check`:** clean. **`cargo clippy --all-targets --all-features`:** no warnings.

## CI after publication
`82aaf6b` was pushed to `main` on 2026-09-28. GitHub Actions run 36510511873 passed all four jobs:
Minimum supported Rust (1.91), Recovery (ubuntu-latest), Recovery (macos-latest) and Recovery
(windows-latest). The step running `tests/sorted_map_enumeration.rs`, whose command
`tests/ci_workflow.rs` requires, carries no platform condition, so the enumeration's tests ran on
all three platforms.
