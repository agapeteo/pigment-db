# Tracked storage stats: verification

## Baseline at `9ba8871`, before production edits
The full suite (`cargo test --all-targets --all-features -- --test-threads=1`), run at `9ba8871`
before the first production edit, gave 628 passed, 0 failed, 28 ignored across 28 binaries. Those
are spec 013's figures at `82aaf6b`, as expected: `9ba8871` changed only 013's verification record.

## RED
- **T002.** `tracked_storage_stats` first landed on all three file-backed stores as a stub returning
  all-zero figures (`Ok`), with the crate-private conversion it delegates to stubbed the same way.
  Every new test failed on an assertion, not at compile time.
  - `tests/tracked_storage_stats.rs`, all 15 (five scenarios, once per family):
    - `tracked_stats_equal_storage_stats_after_rotations_and_a_reopen` at "the tracked figures
      differ from storage_stats() after rotations" (key/value: zeros against 11 sealed segments,
      2,652 bytes);
    - `tracked_stats_agree_after_online_compaction_and_a_further_rotation` at "the tracked figures
      differ from storage_stats() after an online compaction";
    - `tracked_stats_read_no_file` at "the tracked figures need the store directory";
    - `tracked_stats_are_consistent_under_concurrent_rotation` at its anti-vacuity check, "the
      readings saw no rotation, so nothing was tested: {0}". Zeros satisfy every other property it
      checks. The test was revised after GREEN (below), and the revision was observed RED against
      the stub put back for that run: "the readings saw too few rotations, so little was tested:
      {0}". `src/maintenance.rs` was restored from a copy afterwards, sha256 verified;
    - `a_reading_inside_a_compute_callback_completes_while_a_compaction_waits` at "the reading
      inside the callback lost bytes" (key/sorted-map: zeros after 2,892 bytes). A stub never
      waits, so this RED shows only the figures; whether the test catches a reading that waits for
      the maintenance gate is for the neutralization probes.
  - `src/tracked_storage_stats_tests.rs`, private seams, assertions on the public result:
    - `a_reading_during_an_online_capture_returns_at_once_with_the_figures_from_before_it` at "a
      reading at SnapshotCaptured is not the figures from before the attempt";
    - `a_reading_while_the_writer_is_detached_returns_the_replaced_generations_figures` at "a
      reading while the writer is detached is not the replaced generation's figures";
    - `figures_whose_total_overflows_fail_closed` at "an overflowing total must answer
      FailedClosed, not Ok(...)";
    - the two failed-closed tests failed against the stub too; their RED for T003 is below.
- **T003.** Against T002's GREEN body, which read the figures without looking at the WAL's health,
  both failed-closed tests failed:
  - `a_wal_failed_closed_by_an_indeterminate_publication_answers_failed_closed`: "a WAL failed
    closed by an indeterminate publication must answer FailedClosed, not Ok(FamilyStorageStats {
    family: KeyValue, active_bytes: 216, sealed_segment_bytes: 2376, sealed_segment_count: 11,
    total_bytes: 2592 })". The replacement reopen is scripted to fail through
    `complete_online_cutover_with_reopen_probe`.
  - `a_wal_failed_closed_by_an_unconfirmed_rollback_answers_failed_closed`: "a WAL failed closed by
    an unconfirmed rollback must answer FailedClosed, not Ok(FamilyStorageStats { family: KeyValue,
    active_bytes: 64, ... total_bytes: 64 })". The WAL is a file-backed V2 segment with rotation
    enabled, whose data barrier and rollback are scripted to fail
    (`new_v2_with_physical_probe`); it runs once per family. The 64 bytes are the header: the
    rejected write's bytes stayed in the file, so the figures and the file disagree, which is why
    FR-3 returns none.
- **T004.** With `tracked_storage_stats` added to the target list in `tests/ci_workflow.rs`,
  `recovery_workflow_runs_every_dedicated_issue_regression_target` failed: "recovery workflow must
  run `cargo test --test tracked_storage_stats -- --test-threads=1`". Adding the line to
  `recovery.yml` turned it green.

## GREEN
- **T002.** `WalStorage::tracked_lengths` reads `active_len` and the rotation state's
  `segment_base` and `segment_id` under one `wal_state` read guard, through a poisoned lock, and a
  WAL without rotation state answers an error. `tracked_family_storage_stats` maps that error to
  `FailedClosed` and builds the result through `FamilyInspection -> FamilyStorageStats`, with a
  checked total and `usize::try_from` for the count, each overflow `FailedClosed`. Nothing takes the
  maintenance coordinator. `tests/tracked_storage_stats.rs`: 15 passed; the capture, detached-writer
  and overflow unit tests passed.
- **T003.** The accessor now refuses every health but `Ready` and `WriterDetached` through the
  WAL's existing `ensure_ready`, so `MaintenanceIndeterminate` and `FailedRollback` answer
  `FailedClosed` with that function's detail. The five unit tests passed.
- **The new tests, three runs in a row:** 15 and 5 passed each time.
- **Full suite** (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 648
  passed, 0 failed, 28 ignored across 29 binaries. That is 628, 28 and 28 binaries, as spec 013
  recorded at `82aaf6b` (`9ba8871` changed only its verification record), plus the 15 integration
  tests and their binary and the 5 unit tests.
- **Doc tests** (`cargo test --locked --doc`): 9 passed, among them the three new `compile_fail`
  examples showing that memory stores have no `tracked_storage_stats`.
- **`cargo fmt --check`:** clean.
- **`cargo clippy --locked --all-targets --all-features`:** no warnings (clippy 0.1.97, measured on
  a target directory that had not checked this crate before).

## Found while implementing
- **FR-1's wait bound does not hold as written.** FR-1 says the call "waits at most for one
  in-progress write, rotation or rollback". `wal_state` is a `std::sync::RwLock`; on Linux its
  futex implementation lets no reader in while a writer waits (`is_read_lockable`,
  `library/std/src/sys/sync/rwlock/futex.rs`, Rust 1.97.1), so a reading waits behind every writer
  queued ahead of it, and std promises no policy on other platforms. Measured with four writers
  rotating at almost every write: the median reading took 1.8 µs, the 99th percentile 0.65 ms and
  the slowest 44 ms, and successive readings that differed were about 20 sealed segments apart. The
  rustdoc was changed to state a weaker bound and spec.md was left unchanged, for the review. The
  review rejected that, and the reading no longer takes the lock (Review, item 1).
- **The concurrent-rotation test** first stopped once a reading saw 40 sealed segments. Its first
  reading came back already past 40 (`{422}`), so it saw one count and tested nothing. It now reads
  until it has seen ten different sealed counts.

## Review
Three adversarial reviewers (the contract, concurrency and neutralization-probe lenses) reported 16
findings: 3 blockers, 6 majors and 7 minors. They are 10 distinct defects, because all three
lenses reported the wait bound and two or three reported the tear, the poisoned lock, file access
and allocation. Each was checked against the tree and confirmed; none was refuted. Every lens
classified the change as passing Principle VI, and this record's own scan agrees (below). Each
fix was observed RED first: against the reviewed code for a behaviour change, and against the
probe it exists to catch for a test that closes a gap.

1. **FR-1's wait bound did not hold (blocker; all three lenses).** Confirmed. The reading took
   `wal_state` for reading, a writer-preferring lock on Linux, so it waited behind every write
   queued for it. Fixed in the implementation, as Principle IV requires of a failed threshold;
   FR-1 and spec.md are unchanged.
   - Each WAL publishes what the method reports, a status and three lengths, to a cell of atomics
     (a sequence lock) whenever `wal_state`'s write side is released, from that guard's `Drop`.
     The reading copies the cell and takes no lock. plan.md's IV record is amended: one piece of new state, no new lock, and
     the reasons and rejected alternatives.
   - The rustdoc on all three methods says what holds now. The "can make it wait behind several of
     them" sentence is gone.
   - A failed-closed reading's detail is now a fixed sentence per cause, because the reading no
     longer sees the WAL's state; FR-3 asks only for `FailedClosed`.
   - RED: `a_reading_waits_at_most_for_the_write_in_progress` (unit, private seams) holds write A
     inside its data barrier, queues further writes, calls the reading, and releases A alone.
     Against the reviewed code: "the reading was still waiting 10s after write A finished, while
     writes that began after it went first (2 writes had entered their data barrier)". GREEN with
     the cell. The neutralization probes then showed that a reading queueing for the write side
     (P10) passed that first version: the reading was the first writer waiting, so it was the one
     woken when A finished. The test now queues write B before the reading as well, and P10 is
     caught. The reviewed reading
     (OLDREAD) is caught by the final version too.
   - Measured before and after under load: see Measurements.
2. **No test could see a torn reading (major; all three lenses).** Confirmed. With 256-byte
   segments every write rotated, so every reading's active length was the same constant, and the
   reviewers measured their split-guard probes passing 30 of 30 family runs.
   - `tracked_stats_are_consistent_under_concurrent_rotation` now uses 4 KiB segments and values
     of 16 to 215 bytes. It reads until it has seen 10 sealed counts and 50 active lengths, and
     checks each reading's active length against the length its segment reached.
   - RED against the reviewed code's split reads, with the final test, 5 runs each, every run
     failing: P5a (active length under one guard, sealed figures under another) failed 14 of 15
     family runs and P5b (the other order) 13 of 15, each at "a later reading went backwards"; P5c
     (count and bytes under different guards) failed 15 of 15. An earlier run of the test, before
     each writer was capped (item 8), also once reported "the active length exceeds the length its
     segment reached (3955)". The unprobed reviewed code passed 15 of 15.
   - `published_figures_are_never_read_in_part` (unit) drives the cell itself: one publisher, a
     reader checking that three linked words come from one publication. It catches the cell's own
     defects (P5b to P5d below), which the store-level test does not.
3. **File access through the open writer, and damage after the open, were unseen (major;
   contract, probes).** Confirmed: an `fstat` of the writer's file (P1fd) passed every test.
   `tracked_stats_do_not_see_damage_made_after_the_open` appends 100 bytes to the active segment
   and cuts a sealed segment short, then requires `storage_stats()` to change and the tracked
   figures not to. RED against P1fd on the reviewed code, all three families: "the tracked
   figures moved with damage made after the open" (key/value 321 active bytes against 221).
4. **"Allocates nothing beyond its result" was unseen (major; contract, probes).** Confirmed (P14
   passed everything). `tests/tracked_storage_stats_allocation.rs` is its own binary with a
   counting global allocator; it counts one reading's allocations on the calling thread, per
   family, after a first reading. A fourth test shows the counter counts. RED against P14 on the
   reviewed code: "a reading allocated", 3 against 0, all three families. Added to
   `recovery.yml`'s dedicated targets and to `tests/ci_workflow.rs`, whose test went RED first.
5. **The compute-callback test did not cover the key/value transaction lock (minor; probes).**
   Confirmed (P12 passed everything). `key_value_reading_inside_a_compute_callback_completes_while_a_batch_waits`
   queues a compare-exchange batch behind the callback, then reads inside it. RED against P12 on
   the reviewed code: "a reading inside a compute callback did not complete while a batch waited
   for the callback: Timeout".
6. **The unit tests ran in CI on Linux only (minor; contract).** Confirmed. A "Tracked storage
   stats seams" step runs `cargo test maintenance::tracked_storage_stats_tests:: --
   --test-threads=1` on every OS. `tracked_storage_stats_seam_tests_run_on_every_operating_system`
   in `tests/ci_workflow.rs` pins it the way the directory-ownership step is pinned; it failed
   before the step was added.
7. **A poisoned lock answered figures that disagree with the file (minor; contract,
   concurrency).** Confirmed: 64 bytes reported for a 213-byte active file. The figures now fail
   closed when a write panics while it holds the write side, and stay so while the lock is
   poisoned, including after a write-side holder that reads through the poison and completes.
   plan.md's decision is reversed. RED: `a_wal_whose_lock_a_panicking_write_poisoned_answers_failed_closed`
   failed against the cell before poison was handled: "must answer FailedClosed, not
   Ok(FamilyStorageStats { family: KeyValue, active_bytes: 64, ... })". It was then moved from the
   store to the WAL, so that it can also run a write-side holder that reads through the poison;
   that version fails against the reviewed reading (OLDREAD) and against P15, P15b and P15c.
8. **The concurrent test's readings were unbounded (minor; probes).** Confirmed (1.2 GB under a
   stub). Readings are kept only when they change, at most 100,000 of them, and each writer stops
   after 25,000 writes. Under the stub probe the integration binary now peaks at 86 MB.
9. **The Principle VI record understated where the consumer is named (minor; contract).**
   Confirmed. plan.md's record and this one now list the Release line too. The revision a
   consumer pins is recorded once T006 publishes it.
10. **The baseline was a placeholder (minor; contract).** Confirmed; recorded above, and T001 is
    ticked.

Three things the probes found in the review's own tests, each fixed before the final run:
- The rsync-based probe runner kept source modification times older than the last build, so
  cargo did not rebuild after a probe was undone, and three `BASE` runs measured the previous
  probe's build. The runner now touches every source after copying.
- The cell test asserted inside its thread scope, so a failing reading left the publisher running
  and the scope waited for it for ever (P5b and P5c timed out at 300 s). It records the first bad
  reading and asserts after the scope.
- The first FR-1 test let a reading that queues for the write side pass (item 1).

## Neutralization probes
Each probe changes one thing in a copy of the tree (the working tree is never edited), rebuilds
it, and runs `tests/tracked_storage_stats.rs`, `tests/tracked_storage_stats_allocation.rs` and the
unit tests. The table is the final run, over the code and tests as they stand. Most are the probes
reviewer's. P5a to P5c, P10, P14 and P15 are restated against the published figures, because the
code they changed is gone; OLDREAD, P5d, P10r, P15b, P15c, P16 and P19 were added here.

"All" is every scenario in all three families. "Unit" names a test in
`src/tracked_storage_stats_tests.rs`.

| Probe | Change | Caught by |
|---|---|---|
| STUB | all-zero figures | all 19 integration tests, the allocation tests (their setup) and 7 of 8 unit tests |
| OLDREAD | the reviewed reading: `wal_state`'s read side, reading through the poison | unit: the FR-1 wait and poison tests |
| P1 | figures from the directory (stat, read_dir) | damage, unreadable-directory and allocation tests (all); unit: FR-1 wait, detached writer |
| P1fd | active length from an fstat of the open writer | damage and concurrent tests (all); unit: FR-1 wait |
| P2 | the maintenance coordinator taken shared | compute callback with a compaction (all); unit: capture, detached writer |
| P2x | the coordinator taken exclusively | as P2, plus the batch test and unit: FR-1 wait |
| P3 | health ignored | unit: both failed-closed tests |
| P3a | an indeterminate publication answered with figures | unit: its failed-closed test |
| P3b | an unconfirmed rollback answered with figures | unit: its failed-closed test |
| P4 | a detached writer fails closed | unit: detached writer |
| P4wait | the reading waits while the writer is detached | unit: detached writer, FR-1 wait |
| P5a | active length and sealed figures from two readings of the cell | concurrent test (all; 14 of 15 family runs in 5 more runs) |
| P5b | the cell's reader without its second sequence check | unit: the cell test (5 of 5 runs) |
| P5c | the cell's reader without the odd-sequence check | unit: the cell test (5 of 5); the concurrent test once (key/set) |
| P5d | the cell's publisher without the odd mark | unit: the cell test (5 of 5); the concurrent test (key/set, key/sorted-map) |
| P6 | figures fixed at construction | every test but the overflow and cell tests |
| P6ttl | the first reading cached for one second | equality, compaction, concurrent and compute-callback tests (all), the batch test; unit: FR-1 wait, detached writer |
| P7a | sealed count plus one | every integration test; unit: capture, detached writer |
| P7b | sealed count minus one, saturating | five scenarios (all), the batch test; unit: capture, detached writer |
| P7c | active and sealed bytes swapped | every integration test; unit: capture, detached writer |
| P8a | wrapping total | unit: overflow |
| P8b | saturating total | unit: overflow |
| P8c | unchecked count cast | not caught: identical on a 64-bit target |
| P9 | delegate to `storage_stats()` | concurrent, damage, unreadable-directory and allocation tests (all); unit: FR-1 wait, detached writer, both failed-closed tests |
| P9h | health check, then `storage_stats()` | as P9, except the indeterminate-publication test |
| P10 | the reading queues for `wal_state`'s write side | unit: FR-1 wait |
| P10r | the reading takes `wal_state`'s read side first | unit: FR-1 wait |
| P12 | the key/value transaction lock taken shared | the batch test |
| P12x | the transaction lock taken exclusively | the batch test, key/value compute callback with a compaction; unit: FR-1 wait |
| P13 | every map shard touched (`store.len()`) | compute callback with a compaction (all), the batch test; unit: FR-1 wait |
| P14 | a 4 KiB allocation per reading | the allocation tests (all three families) |
| P15 | poison ignored | unit: poison, at the first reading |
| P15b | poison seen only while unwinding | unit: poison, after the write-side holder that reads through it |
| P15c | poison seen only once the lock records it | unit: poison, at the first reading |
| P16 | published only while the WAL is ready | unit: both failed-closed tests |
| P17 | failure mapped to another `CompactionError` variant | unit: the three failed-closed tests |
| P18 | family always key/value | six scenarios for key/set and key/sorted-map |
| P19 | published when the write side is taken, not when released | five scenarios (all), the batch test; unit: capture, FR-1 wait, unconfirmed rollback, poison |

The peak resident size of the integration binary was 34 MB unprobed and 86 MB under STUB, whose
concurrent test runs to its deadline.

## Measurements
Release builds of the reviewed code and of the final code, driven by the same harness (kept
outside the repository): a file-backed key/value store with 4 KiB segments and 100-byte values,
one reading every 5 ms for 8 s after 300 ms of writes. The machine was shared with other work
throughout (load average 6 to 40).

| Workload | Reviewed code: median / p99 / slowest reading, most writes during one | Final code |
|---|---|---|
| 4 writers, Buffered | 81 µs / 618 µs / 2.2 ms, 72 writes | 0.74 µs / 1.4 µs / 17 µs, 2 writes |
| 4 writers, Physical | one reading in the 8 s, taking 286 s while 45,429 writes completed; a second run 185 s and 29,503 | 1.08 µs / 2.5 µs / 38 µs, 0 writes; a second run 0.93 µs / 2.3 µs / 82 µs, 0 |
| 8 writers, Physical | no reading answered within the 900 s limit | 1.01 µs / 2.1 µs / 7.6 µs, 1 write |
| 4 writers, Physical, reading inside a compute callback (the reading alone timed) | 329 ms / 591 ms / 2.9 s, 476 writes | 0.96 µs / 1.4 µs / 1.8 µs, 1 write |

Put cost, one thread, best of five runs of 200,000 puts (memory store) or 100,000 (file store,
Buffered), eight runs of each build interleaved:

| Store | Reviewed code: median (fastest) | Final code |
|---|---|---|
| memory | 1,019 ns (932) | 1,023 ns (952) |
| file, Buffered | 2,169 ns (2,112) | 2,213 ns (2,172) |

The file store's 2% is within the spread of either build's runs. A first comparison, with an
earlier build of the harness, put the final code's memory puts 12% behind. That did not reproduce
once both sides were rebuilt from the same harness source, and builds of the final code with the
guard's `Drop` returning at once, or with no `Drop` and no panic check at all, measured the same
as the final code (fastest 946 ns and 928 ns, against 973 ns for the reviewed code in that run).
What each release of the write side gains is six atomic stores and three atomic loads. (Superseded
by Review 2's performance gate, which compares against `9ba8871` with a harness kept in the
repository; since then an unchanged publication stores nothing.)

## GREEN after the review
- **spec 014's targets**, three runs in a row: `tests/tracked_storage_stats.rs` 19 passed,
  `tests/tracked_storage_stats_allocation.rs` 4 passed, the unit tests 8 passed, and
  `tests/ci_workflow.rs` 11 passed.
- **Full suite** (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 660
  passed, 0 failed, 28 ignored across 30 binaries. That is the 648 recorded above, plus 4
  integration tests (the damage scenario per family and the batch test), the allocation binary
  and its 4 tests, 3 unit tests (the FR-1 wait, poison and cell tests) and 1 CI pin.
- **Doc tests** (`cargo test --locked --doc`): 9 passed.
- **`cargo fmt --check`:** clean.
- **`cargo clippy --locked --all-targets --all-features`:** no warnings (clippy 0.1.97, on a target
  directory that had not checked this crate before). The two `manual_is_multiple_of` warnings it
  first raised on the new code are fixed; `is_multiple_of` is stable since Rust 1.87, below the
  1.91 minimum.
- **Builds:** `cargo build --release --locked` and `cargo build --locked --all-targets
  --all-features` emit no warnings; `cargo doc --no-deps` none either.

## Review 2
A second adversarial review, on 2026-10-02, verified the cell's memory ordering (the aarch64
instructions included) and that the publication invariant held over the full suite, and asked for
two majors, five minors and a set of nits. Each is done or answered below. Every behaviour change
and every test that closes a gap was observed RED first: a behaviour change against the code
before it, a new test against the variant it exists to catch, run in a copy of the tree (the
working tree was never used for a probe).

1. **MAJOR: the Principle IV record.** Done.
   - plan.md now calls the cell what it is, a sequence lock and a new coordination layer, owned by
     the holder of `wal_state`'s write side (`publish` is private to the `wal` module; its one
     production caller is the write guard's `Drop`). It states lock order, what may block, that a
     reading's progress depends on the publisher being scheduled, and that a reading must not be
     taken from a signal handler. The first review's record ("one piece of new state, no new lock",
     item 1 above) understated it.
   - spec.md has the demonstration Principle IV asks for, a "Why a new coordination layer" note
     under FR-1, and an Amendments line recording that it and FR-3's third cause were added on
     2026-10-02 after the implementation review.
   - plan.md's Memory ordering section records the argument (nothing older, nothing newer, never
     backwards, one publisher, the skip), its correspondence with Boehm's fence-based seqlock and
     crossbeam-utils 0.8.23's `SeqLock` (read from the registry source), and the instructions. The
     aarch64 and x86-64 listings were produced here from the cell's two functions extracted by
     script from `src/wal/mod.rs` (`RUSTC_BOOTSTRAP=1 cargo rustc --release --target
     aarch64-unknown-linux-gnu -Zbuild-std=core -- --emit asm`, rustc 1.97.1): publish `ldr`/`cmp`
     ×4 (the skip), `ldr`, `str` odd, `dmb ish`, `str` ×4, `stlr` even; read `ldar`, `tbnz`,
     `ldr` ×4, `dmb ishld`, `ldr`, `cmp`, `b.ne`. On x86-64 every access is a plain `mov` and both
     fences are `#MEMBARRIER` (compiler only).
   - **Would a `Mutex` have met FR-1? Yes.** A `Mutex` holding a copy of the figures, taken by the
     publisher only to copy them in and by a reading only to copy them out, bounds a reading's wait
     by one critical section of a few stores, never by I/O, and depends on the holder being
     scheduled exactly as the sequence lock's retry does. No requirement in spec.md decides
     against it: what the sequence lock adds is that a write never waits for a reading and has no
     read-modify-write, which are preferences. plan.md says so, and does not rationalise the
     choice. Measured (below): a plain `Mutex` fails the performance gate on key/value memory
     stores in all five series that ran it (+5.8% to +9.3%), so it needs the same skip of
     unchanged figures; with the skip its memory-store path is the same work as the sequence
     lock's, and the harness could not separate the two. The recommendation is in item 2's last
     bullet and in the report: replace the cell with the `Mutex` and the skip, re-measured against
     the gate. The implementation was not switched here.
2. **MAJOR: the performance gate.** Done; the first measurement failed and the implementation was
   fixed.
   - Threshold (plan.md, Performance gate): for each family, on a memory store and on a
     file-backed store, the median put cost may regress by at most 5% against `9ba8871`.
   - Harness: `examples/put_cost.rs`, no new dependency (`tempfile` is an existing
     dev-dependency); it builds under `--all-targets` and passes Clippy. Command:
     `cargo run --release --locked --example put_cost -- 5 100000`.
   - Baseline: a copy of the tree at `9ba8871` (every tracked file compared by sha256 with
     `git show 9ba8871:<path>`; only `examples/put_cost.rs` added), built with its own target
     directory. Both sides run the same harness source.
   - The first series (unpinned, eight rounds, the reviewed cell) failed on key/value memory
     stores: +9.7%. The cost there could not be the publication's six stores alone, but they were
     the work a memory store never needs, since its figures never change. Fix: `publish` compares
     the new words with the published ones (relaxed loads under the write side's exclusion) and
     stores nothing when they are equal. Probe S2 (the skip compares only the active length)
     fails 13 integration tests and 2 unit tests, so the comparison's width is covered.
   - Ten rounds did not resolve 5%: the same put path came out at -0.4% and at +6.8% for
     key/sorted-map memory stores in two ten-round pinned series. The gate therefore runs twenty
     rounds with a byte-identical copy of the baseline binary as an A/A control, pinned to one
     performance core and unpinned (the processor is hybrid: 1.4 GHz performance cores beside
     0.9 GHz and 0.7 GHz efficiency cores). The threshold is unchanged.
   - Results, twenty rounds, nanoseconds per put, median over rounds (range of the round medians),
     and the median over rounds of each round's ratio to the baseline:

     | Family, store | Pinned: `9ba8871` | Pinned: change | Pinned: change / base (A/A) | Unpinned: `9ba8871` | Unpinned: change | Unpinned: change / base (A/A) |
     |---|---|---|---|---|---|---|
     | key/value, memory | 590 (543–640) | 598 (562–737) | +3.1% (−2.7%) | 575 (546–724) | 584 (548–698) | +0.1% (−0.1%) |
     | key/value, file | 1,776 (1,737–1,831) | 1,818 (1,769–1,956) | +4.0% (−0.1%) | 1,792 (1,733–2,070) | 1,841 (1,770–2,057) | +1.7% (+0.5%) |
     | key/set, memory | 724 (694–791) | 716 (688–806) | −0.6% (+0.0%) | 726 (702–988) | 736 (698–827) | +1.6% (+1.8%) |
     | key/set, file | 1,950 (1,910–2,111) | 1,992 (1,915–2,220) | +2.0% (+0.5%) | 1,988 (1,911–2,363) | 2,010 (1,968–2,223) | +1.5% (+0.8%) |
     | key/sorted-map, memory | 759 (683–906) | 758 (695–1,033) | +2.2% (−0.7%) | 781 (668–904) | 751 (724–862) | −2.3% (+0.2%) |
     | key/sorted-map, file | 1,939 (1,864–2,026) | 1,972 (1,912–2,182) | +2.6% (+0.5%) | 1,980 (1,893–2,124) | 2,007 (1,952–2,151) | +1.3% (+0.4%) |

     The gate holds in both series; the largest figure is +4.0% (key/value, file, pinned). Single
     rounds vary much more: the A/A control's own round ratios ranged from −28% to +31%.
   - Every earlier series, ten rounds unless stated, median of round ratios against the baseline
     (`final` is the change as recorded; `skip` is the change with the skip before the
     failed-closed fix, whose put path is the same):

     | Series | key/value mem | key/value file | key/set mem | key/set file | key/sorted-map mem | key/sorted-map file |
     |---|---|---|---|---|---|---|
     | 1, unpinned, 8 rounds: reviewed cell | +9.7% | +3.9% | +3.8% | +3.7% | −0.3% | +2.3% |
     | 1: plain `Mutex` | +6.8% | +2.3% | +4.0% | +2.2% | +4.6% | +3.0% |
     | 2, pinned: reviewed cell | +1.0% | +1.1% | +2.8% | +3.0% | −4.3% | +2.0% |
     | 2: skip | +1.0% | +0.3% | −0.1% | +0.3% | −0.4% | +3.7% |
     | 2: plain `Mutex` | +9.3% | +1.4% | +4.0% | +2.8% | +3.8% | +2.7% |
     | 3, unpinned: final | +2.4% | +3.8% | +1.9% | +3.2% | +2.6% | +5.1% |
     | 3: plain `Mutex` | +8.3% | +2.6% | +4.1% | +1.3% | +3.1% | +4.6% |
     | 3: `Mutex` with the skip | +2.6% | +4.3% | +1.2% | +1.9% | +4.4% | +2.8% |
     | 3, pinned: final | +2.9% | +2.7% | +2.8% | +1.8% | +6.8% | +3.6% |
     | 3: plain `Mutex` | +5.8% | +2.0% | +7.1% | +3.3% | +5.9% | +2.7% |
     | 3: `Mutex` with the skip | +3.9% | +3.1% | +1.0% | +3.4% | +2.3% | +3.6% |
     | 5, pinned, 20 rounds: plain `Mutex` (A/A +1.0% on key/value memory) | +9.3% | +2.5% | +4.1% | +1.1% | +4.6% | +2.6% |
     | 5: `Mutex` with the skip | +5.8% | +4.4% | −1.2% | +1.8% | +0.8% | +2.8% |

     The 3-unpinned +5.1% and 3-pinned +6.8% for the change are the ten-round series that showed
     ten rounds were not enough; on key/sorted-map memory the change does the same work as in
     series 2's skip column (−0.4%).
   - The measurement is x86-64 only. On aarch64 each publication adds a `dmb ish` and each reading
     a `dmb ishld` (item 1's listing); no aarch64 machine was available.
   - Recommendation on the cell: the `Mutex` variant with the skip (`Mutex<TrackedFigures>` plus
     the publisher's record of its last publication, kept in atomics read and written only under
     the write side) passed every ten-round series and measured +5.8% on key/value memory stores
     in its one twenty-round series, on work identical to the sequence lock's there, which
     measured +3.1%; the difference is the harness's resolution, not the design. Given FR-1 is met
     either way and no requirement asks for a write path that never waits for a reading, the
     simpler design is recommended, to land with its own run of this gate.
3. **MINOR: a failed-closed reading allocated.** Fixed. `WalStorage::tracked_lengths` now returns
   the cause (`UnreadableFigures`), and `tracked_family_storage_stats` builds
   `CompactionError::FailedClosed { detail: cause.detail().to_owned() }` once, the only
   allocation. RED: `a_failed_closed_{key_value,key_set,key_map}_reading_allocates_only_its_detail`
   (`tests/tracked_storage_stats_allocation.rs`, Unix) failed in all three families, "a
   failed-closed reading allocated more than its detail", left 4, right 1 (an `io::Error` built
   from the detail, its box, its custom box, and `to_string`). The WAL is failed closed through
   the public API: under Physical durability with 256-byte segments, the store directory is left
   with write and search permission only (0o300), so a rotation's moves succeed and its directory
   synchronisation, which opens the directory for reading, fails, and the WAL is failed closed
   (an unconfirmed rollback). The test refuses to run where permissions are not enforced (root).
   GREEN: 7 passed.
4. **MINOR: spec.md's Acceptance said "for both causes".** It now says each of the three.
5. **MINOR: the CI pins could be evaded.** Fixed, for both pins (the directory-ownership pin had
   the same weakness). RED: both pins were moved onto one predicate, still the old `contains`
   match, and `a_pinned_step_gated_to_one_operating_system_fails_its_pin` plants
   `if: runner.os == 'Linux'` in copies of the workflow after each pinned step's commands, between
   its name and `run:`, and on its first line; it failed: "the pin accepted Directory ownership
   seams and cross-process claims with an `if: runner.os == 'Linux'` after its commands". GREEN:
   the predicate now parses the step (from its `- name:` line to the next line indented six spaces
   or fewer) and requires its only eight-space key to be `run` and its script to be exactly the
   pinned commands. `tests/ci_workflow.rs`: 12 passed. Not covered: an `if:` on the job, or a
   matrix that names fewer operating systems.
6. **MINOR: no test needed `panicking_on_entry`.** Fixed.
   `a_panic_that_began_before_the_write_side_was_taken_leaves_the_figures_readable` (unit) makes
   an online capture panic at its `RecorderActivated` checkpoint; while the thread unwinds, the
   attempt's guard takes the write side to clear the delta recorder. RED against the variant
   without the field (`poisoned = panicking() || is_poisoned()`), in a probe copy: "a panic that
   unwound out of no write must leave the figures readable and unchanged: Err(FailedClosed {
   detail: "the WAL is failed closed: a panic unwound out of a write that held the WAL's lock,
   poisoning it, ..." }), before it FamilyStorageStats { ..., total_bytes: 2592 }". GREEN on the
   working tree; a later write also succeeds, so the lock is not poisoned.
7. **MINOR: the FR-1 wait test's timing.** Recorded here.
   `a_reading_waits_at_most_for_the_write_in_progress` orders its threads with 200 ms sleeps
   (`SETTLE`), three of them: for write B to queue for the WAL state lock before the reading
   starts, for the reading to start, and for the four later writes to queue before write A is
   released. Only A's entry into its barrier is observed (a counter, under the 10 s watchdog);
   that B and the later writes are queued is assumed from the sleeps, because a waiter on the
   standard lock cannot be observed from outside it. On a runner too loaded to schedule a thread
   within 200 ms the test loses power, not correctness: a reading that queued for the write side
   (P10) or took the read side (P10r) could then answer ahead of writes that had not yet queued,
   and pass. A correct reading never waits, so the sleeps cannot fail it.
8. **NITS.** Done.
   - FR-3's wording is "a panic unwound out of a write that held the WAL's lock, poisoning it" in
     spec.md, plan.md, the three methods' rustdoc and the failed-closed detail. spec.md adds that a
     panic which began before the write took the lock poisons nothing and is not this cause.
   - The three methods' rustdoc and `PublishedFigures`'s say that a reading's progress depends on
     the publishing writer being scheduled, and that it must not be called from a signal handler.
   - `PublishedFigures::publish` is private to the `wal` module; the cell test calls a
     `#[cfg(test)]` `publish_probe`.
   - The two deferred defects are filed, with measurements, in
     `reviews/opus-5.5-maintenance-2026-10-02.md`, cited from plan.md: `storage_stats()` failed
     in 96% to 98.5% of readings taken while every write rotated, and in 71% with 4 KiB segments,
     and never once the writers stopped; online compaction returned `AuthorityUndetermined` after
     publishing in 45 to 71 of 100 attempts under rotating writers, and a quiescent retry then
     completed in all 116 cases.

## GREEN after Review 2
- **spec 014's targets**, three runs in a row: `tests/tracked_storage_stats.rs` 19 passed,
  `tests/tracked_storage_stats_allocation.rs` 7 passed, the unit tests 9 passed, and
  `tests/ci_workflow.rs` 12 passed.
- **Full suite** (`cargo test --locked --all-targets --all-features -- --test-threads=1`): 665
  passed, 0 failed, 28 ignored across 31 binaries. That is the 660 recorded above, plus the three
  failed-closed allocation tests, the unit test of item 6 and the CI control of item 5; the 31st
  binary is `examples/put_cost.rs`, which `--all-targets` builds and runs with no tests.
- **Doc tests** (`cargo test --locked --doc`): 9 passed.
- **`cargo fmt --check`:** clean.
- **`cargo clippy --locked --all-targets --all-features`:** no warnings (clippy 0.1.97, on a target
  directory that had not checked this crate before), the example included.
- **Builds:** `cargo build --release --locked` and `cargo build --locked --all-targets
  --all-features` emit no warnings; `cargo doc --no-deps` none either.

## What no test sees
- The publication cell's memory ordering on weakly ordered processors. x86-64 orders ordinary
  loads and stores strongly, so the cell test catches a missing sequence check (P5b to P5d) but
  could not catch a missing fence. The fences follow the standard sequence-lock pattern for
  C++11-model atomics; plan.md's Memory ordering section records the argument and the aarch64
  instructions they compile to (Review 2, item 1), which is evidence by inspection, not a test.
- The new macOS and Windows behaviour of Review 2's tests: the failed-closed allocation tests are
  Unix-only and have run on Linux alone so far (T006).
- How long a reading retries. It retries only while a publication overlaps its copy. A publisher
  doing nothing between publications slowed a debug-build reader to 978 readings in one second
  (the first version of the cell test). A WAL publishes once per release of its write side, after
  the work done under it, so the cell test paces its publisher the same way.
- A WAL with no rotation state, which no public open produces; it answers `FailedClosed`.
- A sealed count that does not fit `usize`, reachable only on a 32-bit target (P8c).

### Principle VI
Every added line of code, rustdoc, test, test name, CI step and target was scanned for the
consumer's vocabulary, including after both reviews (Review 2 added `examples/put_cost.rs` and
`reviews/opus-5.5-maintenance-2026-10-02.md`); none carries it. The consumer is named only in
the Motivation notes of spec.md and plan.md, and in plan.md's Principle VI record and its Release
line.

## Not done here
T006: MSRV 1.91, macOS and Windows in CI on the published revision, and the `main` revision a
consumer pins.
