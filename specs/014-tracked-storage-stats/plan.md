# Tracked storage stats: plan

## Technical context
Rust 2021 library crate, MSRV 1.91. Each file-backed store owns a `WalStorage<File>` whose state is
behind one `std::sync::RwLock` (`wal_state`). Every change to the active segment's length
(`active_len`) and to the rotation state (`segment_id`, the sealed count; `segment_base`, the sealed
bytes) happens under that lock's write side: appends, rotation, rollback, and the online-compaction
writer take and install. `storage_stats()` takes no lock at all, lists the directory, and reads and
replays the whole chain (`compaction/inspection.rs`, `validate_current_chain`).

A write holds `wal_state`'s write side for its whole I/O, including `sync_data` under Physical
durability, and a rotation holds it across staging, two renames, a reopen and a directory sync.
On Linux the standard `RwLock` admits no reader while a writer waits and wakes a waiting writer
before any reader (`is_read_lockable` and `wake_writer_or_readers`,
`library/std/src/sys/sync/rwlock/futex.rs`, Rust 1.97.1); other platforms promise no policy. So a
reader that takes `wal_state` waits behind every write that queues for it, for as long as writes
keep arriving. The review measured 6 writes, 5 of which began after the reading, before one
reading answered, and readings of 18 s to 125 s under four writers at Physical durability.

**Motivation (provenance):** penpack finding V415 (open): penpack's fix decides when to compact an
open store from its WAL's size, read about once a minute.

## Constitution check
- **VI.** The method is stated in the library's terms: storage stats, the WAL, segments, the writer,
  online compaction. Any consumer can use it unchanged, with no parameters. The motivating consumer
  is penpack (finding V415); it is named only in the Motivation notes of spec.md and this plan, in
  this record and in the Release line. The change lands on `main` before penpack pins it, and
  verification.md records the revision.
- **III.** One method per family is added. `storage_stats()` is not changed: making it metadata-only
  would turn its validation errors into `Ok` and drop spec 008's FR-026 integrity check, which
  Principle III forbids without an approved breaking change. No new public type: the result reuses
  `FamilyStorageStats`, whose five fields are exactly the writer's figures.
- **IV. A new coordination layer: a sequence lock, owned by the holder of `wal_state`'s write
  side.** spec.md's FR-1 note is the demonstration Principle IV asks for: `wal_state` makes a
  reading wait behind every write queued for it (Technical context), and the maintenance
  coordinator is held exclusively across whole-WAL passes. So the change adds coordination, and
  this record says what, who owns it, and what it was weighed against.
  - *What it is.* Each WAL keeps a publication cell (`PublishedFigures`, `src/wal/mod.rs`): a
    sequence lock over five atomics, a sequence number and four words (a status, then the three
    lengths the method reports). It is not a lock anyone waits on, but it is a new coordination
    layer, with its own protocol and its own memory-ordering argument (below).
  - *Who owns it.* Only the holder of `wal_state`'s write side publishes: `publish` is private to
    the `wal` module, and its one production caller is `WalStateWriteGuard`'s `Drop`, which runs
    before the standard guard inside it releases the lock. So publications are serialised by
    `wal_state` and never overlap. That exclusion is the sequence lock's precondition, inherited
    from the existing lock rather than established by the cell, and it is why the sequence is
    advanced by a plain load and store rather than a read-modify-write. A publication whose four
    words equal the published ones stores nothing (Performance gate).
  - *Readers.* A reading takes nothing: it copies the four words between an acquire load of an
    even sequence and a relaxed reload after an acquire fence, and retries if the sequence was odd
    or has changed, yielding to the scheduler every 64 attempts.
  - *Lock order.* Unchanged: publishing takes nothing more, while `wal_state`'s write side is
    already held, and a reading takes nothing at all.
  - *What may block.* A reading waits for no lock and no I/O. It retries while a publication, six
    stores at the end of a write, overlaps its copy. Its progress therefore depends on the
    publisher being scheduled: a publisher preempted between its first and last store keeps every
    reading retrying until it runs again. For the same reason a reading must not be taken from a
    signal handler, which could interrupt a publication on its own thread and would then retry for
    ever. A publisher never waits for a reading.
  - The method does not take the maintenance coordinator: online compaction holds it exclusively
    across whole-WAL passes, and a reader re-taking it from a compute callback could deadlock
    behind a queued exclusive. It takes no store transaction or map lock either.
  - *Cost on the write path.* Each release of `wal_state`'s write side gains a panic-state check
    (and one when the write side is taken), a poison check, the figures built from the state, and
    four relaxed loads comparing them with the published words; only when they differ, a load and
    two stores of the sequence, four stores of the words, and a release fence (a `dmb ish` on
    aarch64, a compiler barrier only on x86-64). No read-modify-write. Measured under Performance
    gate.
  - *The simpler alternative, weighed honestly: a `Mutex` holding a copy of the figures*, taken
    by the publisher in the same `Drop` only to copy the figures in, and by a reading only to copy
    them out, never across I/O.
    - **It meets FR-1.** A reading then waits at most for one critical section of a few stores,
      the tail of the write in progress and never its I/O; with the standard mutex spinning before
      it parks, and a critical section shorter than the spin, a reading almost never parks. Like
      the sequence lock, the bound holds only while the holder is scheduled: a publisher preempted
      inside its critical section makes a reading wait, as it makes the sequence lock's reading
      retry. It allocates nothing, and nothing in either critical section can panic, so it is
      never poisoned.
    - It needs no memory-ordering argument of its own, no encoding to words, no retry loop and no
      test of the cell for torn readings (`published_figures_are_never_read_in_part`, and probes
      P5b to P5d, exist only because of the sequence lock).
    - What it costs is a blocking edge the sequence lock does not have: a write releasing
      `wal_state` can wait for a reading inside its critical section, so a reading preempted there
      delays that write, and every write queued behind it, by up to a scheduling quantum. And it
      adds two atomic read-modify-writes per publication: measured as is, it failed this plan's
      performance gate on key/value memory stores in every series (+5.8% to +9.3%; verification.md,
      Review 2). So a `Mutex` needs the same skip of unchanged figures, which needs a record of
      the last publication kept by the publisher. With it, a memory store's write does the same
      work as under the sequence lock, and a file store's differs by two read-modify-writes
      against six plain stores; the harness did not separate the two designs (key/value memory
      stores: +5.8% for the `Mutex` with the skip in one twenty-round pinned series, +3.1% for the
      sequence lock in another, on identical work).
    - **FR-1's last bullet decides for the sequence lock.** "A reading never makes a write wait"
      was added after the second review (spec.md, Amendments), which found that nothing in the
      spec chose between the two and recommended the `Mutex` as the simpler design. Under the
      `Mutex`, a reading preempted inside its critical section delays the write releasing
      `wal_state`, and every write queued behind it, by up to a scheduling quantum: a priority
      inversion that a low-priority monitoring thread makes routine. The sequence lock's
      publisher never waits for a reading, by construction: a reading takes no lock and writes
      nothing. The maintainer kept the sequence lock for that requirement on 2026-10-02; the
      `Mutex` with the skip is the rejected alternative, and the review's comparison stays in
      verification.md (Review 2).
  - Rejected: amending FR-1 to the reader's measured wait under `wal_state`, which Principle IV
    forbids for a threshold that failed.
- **II.** Nothing is written. The two failed-closed WAL states, and a WAL whose state lock a panic
  poisoned by unwinding out of a write that held it, return `FailedClosed`.
- **V.** Acceptance uses public readings (`storage_stats()`, `try_compact_online`'s outcome, file
  lengths of sealed segments, reopen). Private schedule seams that already exist
  (`begin_online_capture_with_checkpoint_probe`, `complete_online_cutover_with_reopen_probe`,
  `new_v2_with_physical_probe`) place a reading inside the capture, inside the detached-writer
  window and behind a write held in its data barrier, and make a capture panic; they assert only
  the result of the method or of the function it delegates to. `PublishedFigures::publish_probe`
  (`cfg(test)`) lets the cell's own test play the write side.

## Decisions
- **Name.** `tracked_storage_stats`: the figures the writer tracks, beside the validating
  `storage_stats`.
- **Detached writer.** Return the replaced generation's figures rather than an error or a wait. The
  window covers a full re-read of the source, so a waiting reader would stall for seconds, and an
  error would make an ordinary periodic reading fail whenever it coincides with a compaction.
- **Poisoned lock.** A panic that unwinds out of a write holding `wal_state`'s write side poisons
  the lock and fails the figures closed, and they stay so for as long as the lock is poisoned. The
  panicking write's bytes can be in the file uncounted, and every later mutation panics on the
  poisoned lock, so the WAL is failed closed in all but name and the figures need not match the
  files: FR-3's reason. (The plan first read through the poison. The review measured that
  answering 64 bytes for a 213-byte file.) A holder that takes the write side while its thread is
  already unwinding poisons nothing, as the standard lock decides, and publishes the state it
  leaves; the guard records whether its thread was panicking when it took the lock for this.
- **Failed-closed detail.** The WAL reports the cause, and the method copies that cause's fixed
  sentence into `CompactionError::FailedClosed` once: the only allocation a failed-closed reading
  makes, as FR-1 allows ("beyond its result").
- **Arithmetic.** The total is `checked_add(segment_base, active_len)`, and the count is
  `usize::try_from(segment_id)`; an overflow is `FailedClosed`.
- **Not done here.** `storage_stats()` fails spuriously under concurrent rotation, and online
  compaction's cleanup runs the same unsynchronised inspection. Both are filed, with their
  measured evidence, in `reviews/opus-5.5-maintenance-2026-10-02.md` for a separate change;
  neither is needed by this method.

## Memory ordering
The cell is the fence-based sequence lock in its standard form. A publication, under `wal_state`'s
write side, does `seq.store(s + 1, Relaxed)`, `fence(Release)`, the four word stores `Relaxed`, and
`seq.store(s + 2, Release)`. A reading does `s0 = seq.load(Acquire)` and retries if `s0` is odd,
then the four word loads `Relaxed`, `fence(Acquire)`, and `s1 = seq.load(Relaxed)`, accepting the
words only if `s1 == s0`.

- **Nothing older than the publication `s0` names.** `s0` was written by the release store that
  ended publication `k`, so that store synchronizes with the acquire load, the publication's word
  stores happen before the reading's word loads, and by coherence each load returns that
  publication's value or a later one.
- **Nothing newer.** If a word load returned a value stored by a later publication `j`, that store
  is sequenced after `j`'s release fence and the load is sequenced before the reading's acquire
  fence, so the two fences synchronize (the fence rules of the C++ model, which Rust's follows).
  Then `j`'s odd store happens before the reload, which by coherence cannot return `s0` again, and
  the reading retries. So an accepted reading is one publication whole.
- **Never backwards.** Successive readings on one thread load the sequence in its modification
  order (read-read coherence), so a later reading never accepts an earlier publication.
- **One publisher.** The sequence is advanced by a relaxed load and a store, not a read-modify-
  write. That is sound only because every publication happens before the next through
  `wal_state`'s release and acquire, so each publisher reads the last publisher's final value. The
  check that skips unchanged figures reads the words under the same exclusion, so it compares
  with the latest publication, and when it stores nothing readers see what a publication would
  have written.
- **Correspondence.** This is the reader with a trailing acquire fence that Hans Boehm gives in
  "Can seqlocks get along with programming language memory models?" (MSPC 2012), and the protocol
  of crossbeam-utils 0.8.23's `atomic/seq_lock.rs`: `optimistic_read` is `state.load(Acquire)`,
  `validate_read` is `fence(Acquire)` then `state.load(Relaxed) == stamp`, `write` is
  `state.swap(1, Acquire)` then `fence(Release)`, and the guard's drop is
  `state.store(stamp + 2, Release)`. Two differences: crossbeam's swap is a spin lock between
  writers, which `wal_state` makes unnecessary here; and `AtomicCell` copies its data with
  volatile non-atomic reads, which the language model calls a data race, where every word here is
  an atomic, so a racing read is a defined relaxed load.
- **Instructions.** The cell's two functions, extracted from `src/wal/mod.rs` by script into a
  `no_std` crate (the reading's every-64th yield becomes a spin hint, since `core` has no
  scheduler) and compiled with rustc 1.97.1 at `opt-level=3` for `aarch64-unknown-linux-gnu`
  (`RUSTC_BOOTSTRAP=1 cargo rustc --release --target aarch64-unknown-linux-gnu -Zbuild-std=core
  -- --emit asm`), give: publish four `ldr`/`cmp` against the new words, returning if all are
  equal, else `ldr` (sequence), `str` (odd), `dmb ish`, four `str`, `stlr` (even); read `ldar`,
  `tbnz` (odd: retry), four `ldr`, `dmb ishld`, `ldr`, `cmp`, `b.ne` (changed: retry). That is the
  standard C/C++11-to-ARMv8 mapping: release fence `dmb ish`, acquire fence `dmb ishld`, release
  store `stlr`, acquire load `ldar`. On x86-64 the same source gives plain `mov`s, and both fences
  compile to a compiler barrier only, which TSO makes sufficient; that is also why the cell test,
  run on x86-64, catches a missing sequence check (P5b to P5d) but cannot catch a missing fence.

## Performance gate
Principle IV's threshold for this change: **for each family (key/value, key/set, key/sorted-map),
on a memory store and on a file-backed store (default options: Buffered, one segment), the median
put cost may regress by at most 5% against `9ba8871`.**

- Harness: `examples/put_cost.rs`, no new dependency. `cargo run --release --locked --example
  put_cost -- 5 100000` times five runs of 100,000 writes per configuration, interleaved, and
  prints each configuration's median, fastest and slowest run in nanoseconds per write.
- Method: the same harness source built against `9ba8871` (a copy of the tree at that commit) and
  against the change, plus a byte-identical copy of the baseline binary as an A/A control; the
  three run alternately, rotating the order each round, for twenty rounds on one machine. The
  figure compared is the median over rounds of the per-round ratio of the change's median to the
  baseline's; the control's own figure shows how much of a difference is the machine's.
- The machine's processor is hybrid (performance cores at up to 1.4 GHz, efficiency cores at
  0.9 GHz and 0.7 GHz), so an unpinned run's cost depends on where the scheduler put it. Both an
  unpinned series and one pinned to a single performance core (`taskset -c 10`) are recorded, and
  the gate must hold in both.
- Twenty rounds, not ten, because ten did not resolve 5%: the same put path, measured in two
  ten-round pinned series, came out at -0.4% and at +6.8% against the baseline for key/sorted-map
  memory stores, and single rounds of the A/A control differ by up to 30%. Every series is recorded
  in verification.md, the ten-round ones included.
- The measurement is x86-64 only. On aarch64 each publication also executes a `dmb ish`, and a
  reading a `dmb ishld`; no aarch64 machine was available to measure them.
- The first unpinned measurement of the reviewed cell failed the gate on key/value memory stores
  (+9.7%). The fix is in the implementation, not the threshold: a publication is skipped when the
  figures have not changed, which removes every store from a memory store's writes, whose figures
  never change. With it, both twenty-round series pass, the largest figure +4.0% (key/value, file,
  pinned): verification.md, Review 2.

## Release
The change lands on `main`, and penpack pins that tested revision.
