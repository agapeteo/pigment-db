//! specs/014: tracked storage stats at the schedules only private seams reach.
//!
//! The seams place a reading inside an online compaction's capture and inside its
//! detached-writer window, hold a write inside the WAL's lock while others queue behind it, put a
//! WAL into each failed-closed state, and hand the figures' arithmetic values no WAL can reach.
//! Every assertion is on the public result of `tracked_storage_stats()` or of the function it
//! delegates to.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::compaction::inspection::InspectedFamily;
use crate::compaction::OnlineCaptureStage;
use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::model::SearchKey;
use crate::test_support::maintenance_schedule::MaintenanceObserver;
use crate::test_support::mutation_schedule::MutationObserver;
use crate::wal::{PublishedFigures, TrackedFigures, TrackedWalLengths, WalStorage};
use crate::{CompactionError, DurableStoreOptions, FamilyStorageStats, MutationFailure};

/// How long a test waits for another thread before calling it stuck.
const WATCHDOG: Duration = Duration::from_secs(10);
/// How long a test lets other threads reach the lock they are about to wait for.
const SETTLE: Duration = Duration::from_millis(200);

/// A segment target that a single record fills, so nearly every write rotates.
fn rotating() -> DurableStoreOptions {
    DurableStoreOptions::default()
        .with_wal_segment_size(crate::WalSegmentSize::try_from(256_u64).unwrap())
}

fn rotated_key_value_store(directory: &Path) -> DurableKeyValueStore<File> {
    let store = DurableKeyValueStore::try_init_new_with_options(directory, rotating())
        .unwrap()
        .into_store();
    for index in 0..12 {
        store.put(format!("key-{index:02}").into_bytes(), vec![b'v'; 64]);
    }
    assert!(store.storage_stats().unwrap().sealed_segment_count() >= 3);
    store
}

fn assert_failed_closed(result: Result<FamilyStorageStats, CompactionError>, cause: &str) {
    assert!(
        matches!(result, Err(CompactionError::FailedClosed { .. })),
        "a WAL failed closed by {cause} must answer FailedClosed, not {result:?}"
    );
}

#[test]
fn a_reading_during_an_online_capture_returns_at_once_with_the_figures_from_before_it() {
    let directory = tempfile::tempdir().unwrap();
    let store = rotated_key_value_store(directory.path());
    let before = store.storage_stats().unwrap();
    let (ask_tx, ask_rx) = mpsc::channel::<()>();
    let (answer_tx, answer_rx) = mpsc::channel();

    let during = std::thread::scope(|scope| {
        let reader = &store;
        scope.spawn(move || {
            while ask_rx.recv().is_ok() {
                answer_tx.send(reader.tracked_storage_stats()).unwrap();
            }
        });
        let mut during = Vec::new();
        let (recorded, answers) = (&mut during, &answer_rx);
        // The checkpoint owns the asking end, so the reader stops once the capture returns.
        let capture = store
            .begin_online_capture_with_checkpoint_probe(u64::MAX, move |stage| {
                // Every stage runs while the capture holds the store's maintenance gate.
                ask_tx.send(()).unwrap();
                recorded.push((stage, answers.recv_timeout(WATCHDOG)));
            })
            .unwrap();
        drop(capture);
        during
    });

    assert_eq!(
        during.iter().map(|(stage, _)| *stage).collect::<Vec<_>>(),
        [
            OnlineCaptureStage::SnapshotCaptured,
            OnlineCaptureStage::RecorderActivated,
            OnlineCaptureStage::ManifestPrepared,
        ]
    );
    for (stage, answer) in during {
        let reading = answer
            .unwrap_or_else(|_| panic!("a reading at {stage:?} waited for the online capture"));
        assert_eq!(
            reading.unwrap(),
            before,
            "a reading at {stage:?} is not the figures from before the attempt"
        );
    }
    assert_eq!(
        store.tracked_storage_stats().unwrap(),
        store.storage_stats().unwrap()
    );
}

#[test]
fn a_reading_while_the_writer_is_detached_returns_the_replaced_generations_figures() {
    let directory = tempfile::tempdir().unwrap();
    let store = rotated_key_value_store(directory.path());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = crate::compaction::prepare_online_staging(capture, |_| Ok(())).unwrap();
    store.put(b"delta".to_vec(), vec![b'd'; 64]);
    let replaced = store.storage_stats().unwrap();
    let (ask_tx, ask_rx) = mpsc::channel::<()>();
    let (answer_tx, answer_rx) = mpsc::channel();

    let (completed, during) = std::thread::scope(|scope| {
        let reader = &store;
        scope.spawn(move || {
            if ask_rx.recv().is_ok() {
                answer_tx.send(reader.tracked_storage_stats()).unwrap();
            }
        });
        let mut during = None;
        let (recorded, answers) = (&mut during, &answer_rx);
        // The reopen owns the asking end, so the reader stops once the cutover returns.
        let completed = store
            .complete_online_cutover_with_reopen_probe(staged, move |active_path| {
                // The replacement is published and the writer is detached until this returns.
                ask_tx.send(()).unwrap();
                *recorded = Some(answers.recv_timeout(WATCHDOG));
                OpenOptions::new().append(true).open(active_path)
            })
            .unwrap();
        (completed, during)
    });

    let reading = during
        .expect("the cutover reopened its replacement")
        .expect("a reading waited for the online cutover");
    assert_eq!(
        reading.unwrap(),
        replaced,
        "a reading while the writer is detached is not the replaced generation's figures"
    );
    let installed = store.tracked_storage_stats().unwrap();
    assert_eq!(installed, store.storage_stats().unwrap());
    assert_eq!(installed.sealed_segment_count(), 0);
    assert_eq!(installed.total_bytes(), completed.after_bytes);
}

/// Permits for [`gated_data_barrier`]. Only
/// [`a_reading_waits_at_most_for_the_write_in_progress`] uses it.
static BARRIER_PERMITS: Mutex<usize> = Mutex::new(0);
static BARRIER_PERMITTED: Condvar = Condvar::new();
static BARRIERS_ENTERED: AtomicUsize = AtomicUsize::new(0);

/// A data barrier that holds its write, and so the WAL's lock, until the test grants a permit.
fn gated_data_barrier(_: &mut File) -> std::io::Result<()> {
    BARRIERS_ENTERED.fetch_add(1, Ordering::SeqCst);
    let mut permits = BARRIER_PERMITS.lock().unwrap();
    while *permits == 0 {
        permits = BARRIER_PERMITTED.wait(permits).unwrap();
    }
    *permits -= 1;
    Ok(())
}

fn grant_barrier_permits(count: usize) {
    *BARRIER_PERMITS.lock().unwrap() += count;
    BARRIER_PERMITTED.notify_all();
}

fn truncating_rollback(file: &mut File, checkpoint: usize) -> std::io::Result<()> {
    file.set_len(checkpoint as u64)
}

/// FR-1: a reading waits at most for the one write, rotation or rollback in progress. Write A
/// holds the WAL's lock inside its data barrier. Write B queues for the lock before the reading
/// is called, and four more writes begin after the reading and queue too. Only A is in progress:
/// once it finishes, the reading must answer while every other write is still held. (A reading
/// that queued for the write side would wait for B; one that took the read side, for them all.)
#[test]
fn a_reading_waits_at_most_for_the_write_in_progress() {
    const LATER_WRITES: usize = 4;
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let active = directory
        .path()
        .join(InspectedFamily::KeyValue.active_name());
    let writer = OpenOptions::new().append(true).open(&active).unwrap();
    let wal =
        WalStorage::new_v2_with_physical_probe(writer, truncating_rollback, gated_data_barrier);
    wal.enable_file_rotation(active, u64::MAX, None).unwrap();
    let store = Arc::new(DurableKeyValueStore::from_probe_parts(
        [],
        wal,
        MutationObserver::default(),
    ));
    let before = store.tracked_storage_stats().unwrap();

    let write = |key: &str| {
        let (store, key) = (store.clone(), key.as_bytes().to_vec());
        std::thread::spawn(move || store.try_put(key, vec![b'v'; 64]).unwrap())
    };
    let mut writes = vec![write("held")];
    let deadline = Instant::now() + WATCHDOG;
    while BARRIERS_ENTERED.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "write A never reached its barrier"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    writes.push(write("queued"));
    std::thread::sleep(SETTLE);

    let (reading_tx, reading_rx) = mpsc::channel();
    let reader = {
        let store = store.clone();
        std::thread::spawn(move || reading_tx.send(store.tracked_storage_stats()).unwrap())
    };
    std::thread::sleep(SETTLE);
    for later in 0..LATER_WRITES {
        writes.push(write(&format!("later-{later}")));
    }
    std::thread::sleep(SETTLE);

    grant_barrier_permits(1);
    let answer = reading_rx.recv_timeout(WATCHDOG);
    let entered = BARRIERS_ENTERED.load(Ordering::SeqCst);
    // Release every other write before asserting, so a failure leaves no thread parked.
    grant_barrier_permits(1 + LATER_WRITES);
    for write in writes {
        write.join().unwrap();
    }
    reader.join().unwrap();

    let reading = answer
        .unwrap_or_else(|_| {
            panic!(
                "the reading was still waiting {WATCHDOG:?} after write A finished, while writes \
                 that were not in progress went first ({entered} writes had entered their data \
                 barrier)"
            )
        })
        .unwrap();
    let after = store.tracked_storage_stats().unwrap();
    assert_eq!(
        reading.sealed_segment_count(),
        before.sealed_segment_count()
    );
    assert!(
        reading.total_bytes() >= before.total_bytes()
            && reading.total_bytes() < after.total_bytes(),
        "the reading saw a later write, or lost bytes: {reading:?} between {before:?} and \
         {after:?}"
    );
}

#[test]
fn a_wal_failed_closed_by_an_indeterminate_publication_answers_failed_closed() {
    let directory = tempfile::tempdir().unwrap();
    let store = rotated_key_value_store(directory.path());
    store.tracked_storage_stats().expect("a ready WAL is read");
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = crate::compaction::prepare_online_staging(capture, |_| Ok(())).unwrap();

    let failed = store.complete_online_cutover_with_reopen_probe(staged, |_| {
        Err(std::io::Error::other("scripted replacement reopen failure"))
    });
    assert!(
        failed.is_err(),
        "the scripted replacement reopen did not fail"
    );
    assert!(
        store
            .try_put(b"refused".to_vec(), b"value".to_vec())
            .is_err(),
        "the WAL is not failed closed"
    );

    assert_failed_closed(
        store.tracked_storage_stats(),
        "an indeterminate publication",
    );
}

fn failing_data_barrier(_: &mut File) -> std::io::Result<()> {
    Err(std::io::Error::other("scripted data barrier failure"))
}

fn failing_rollback(_: &mut File, _: usize) -> std::io::Result<()> {
    Err(std::io::Error::other("scripted rollback failure"))
}

/// A file-backed WAL over a freshly published segment whose next write fails its data barrier
/// and then its rollback.
fn wal_whose_rollback_fails(directory: &Path, family: InspectedFamily) -> WalStorage<File> {
    let active = directory.join(family.active_name());
    let writer = OpenOptions::new().append(true).open(&active).unwrap();
    let wal =
        WalStorage::new_v2_with_physical_probe(writer, failing_rollback, failing_data_barrier);
    wal.enable_file_rotation(active, u64::MAX, None).unwrap();
    wal
}

fn assert_unconfirmed_rollback(result: std::io::Result<()>) {
    let error = result.expect_err("the scripted data barrier did not fail the write");
    assert!(
        matches!(
            MutationFailure::from_io_error(&error),
            Some(MutationFailure::Indeterminate { .. })
        ),
        "the write was not left indeterminate: {error}"
    );
}

#[test]
fn a_wal_failed_closed_by_an_unconfirmed_rollback_answers_failed_closed() {
    let cause = "an unconfirmed rollback";

    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let store = DurableKeyValueStore::from_probe_parts(
        [],
        wal_whose_rollback_fails(directory.path(), InspectedFamily::KeyValue),
        MutationObserver::default(),
    );
    store.tracked_storage_stats().expect("a ready WAL is read");
    assert_unconfirmed_rollback(store.try_put(b"key".to_vec(), b"value".to_vec()));
    assert_failed_closed(store.tracked_storage_stats(), cause);

    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeySetStore::try_init_new(directory.path()).unwrap());
    let store = DurableKeySetStore::from_probe_parts(
        [],
        wal_whose_rollback_fails(directory.path(), InspectedFamily::KeySet),
        MutationObserver::default(),
    );
    store.tracked_storage_stats().expect("a ready WAL is read");
    assert_unconfirmed_rollback(store.try_append(b"key".to_vec(), b"member".to_vec()));
    assert_failed_closed(store.tracked_storage_stats(), cause);

    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyMapStore::try_init_new(directory.path()).unwrap());
    let store = DurableKeyMapStore::from_probe_parts(
        [],
        wal_whose_rollback_fails(directory.path(), InspectedFamily::KeyMap),
        MutationObserver::default(),
    );
    store.tracked_storage_stats().expect("a ready WAL is read");
    assert_unconfirmed_rollback(store.try_put(
        b"key".to_vec(),
        SearchKey::from(1_usize),
        b"value".to_vec(),
    ));
    assert_failed_closed(store.tracked_storage_stats(), cause);
}

fn panicking_data_barrier(_: &mut File) -> std::io::Result<()> {
    panic!("scripted panic inside the WAL's lock")
}

/// FR-3: a write that panics inside the WAL's lock leaves bytes in the file that the figures do
/// not count, and poisons the lock, so every later mutation panics. The WAL is failed closed, and
/// stays so: a later write meets the poisoned lock, and the online-maintenance paths that hold
/// the write side through the poison and complete leave it failed closed too.
#[test]
fn a_wal_whose_lock_a_panicking_write_poisoned_answers_failed_closed() {
    let cause = "a write that panicked inside its lock";
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let active = directory
        .path()
        .join(InspectedFamily::KeyValue.active_name());
    let writer = OpenOptions::new().append(true).open(&active).unwrap();
    let wal =
        WalStorage::new_v2_with_physical_probe(writer, truncating_rollback, panicking_data_barrier);
    wal.enable_file_rotation(active, u64::MAX, None).unwrap();
    let reading =
        || crate::maintenance::tracked_family_storage_stats(&wal, InspectedFamily::KeyValue);
    reading().expect("a ready WAL is read");

    let write = |key: &[u8]| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = wal.try_store_put_event(key.to_vec(), vec![b'v'; 64]);
        }))
    };
    assert!(
        write(b"first").is_err(),
        "the scripted barrier did not panic"
    );
    assert_failed_closed(reading(), cause);
    assert!(
        write(b"second").is_err(),
        "a write after the panic did not meet the poisoned lock"
    );
    assert_failed_closed(reading(), cause);

    assert!(wal.activate_delta_recorder(7, 1 << 20).is_ok());
    wal.clear_delta_recorder(7);
    assert_failed_closed(reading(), cause);
}

/// FR-3's third cause is a panic that unwinds out of a write holding the WAL's lock. A panic that
/// began before the write side was taken poisons nothing: here an online capture panics at its
/// checkpoint, and while the thread unwinds the attempt's guard takes the write side to clear the
/// WAL's delta recorder. The figures must stay readable, as the lock does.
#[test]
fn a_panic_that_began_before_the_write_side_was_taken_leaves_the_figures_readable() {
    let directory = tempfile::tempdir().unwrap();
    let store = rotated_key_value_store(directory.path());
    let before = store.storage_stats().unwrap();

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = store.begin_online_capture_with_checkpoint_probe(u64::MAX, |stage| {
            if stage == OnlineCaptureStage::RecorderActivated {
                panic!("scripted panic inside an online capture");
            }
        });
    }));
    assert!(unwound.is_err(), "the scripted checkpoint did not panic");

    let reading = store.tracked_storage_stats();
    assert!(
        matches!(&reading, Ok(figures) if *figures == before),
        "a panic that unwound out of no write must leave the figures readable and unchanged: \
         {reading:?}, before it {before:?}"
    );
    store
        .try_put(b"after".to_vec(), vec![b'a'; 64])
        .expect("the panic poisoned the WAL's lock");
    assert_eq!(
        store.tracked_storage_stats().unwrap(),
        store.storage_stats().unwrap()
    );
}

/// Figures whose three lengths determine one another, so a reading that mixes two publications
/// shows.
fn linked_figures(publication: u64) -> TrackedFigures {
    TrackedFigures::Readable(TrackedWalLengths {
        active_len: publication,
        sealed_bytes: publication.wrapping_mul(3),
        sealed_count: publication.wrapping_mul(7),
    })
}

/// FR-2 at the cell the writer publishes through: a reading taken while a publisher replaces the
/// figures sees one publication whole, never words from two, and never an earlier one after a
/// later. The publisher pauses for a varying few spins between publications, as a writer does
/// for its I/O, so readings land both between publications and across them.
#[test]
fn published_figures_are_never_read_in_part() {
    let cell = PublishedFigures::new(linked_figures(0));
    let stop = AtomicBool::new(false);
    // The reader records the first bad reading rather than asserting, so a failure still stops
    // the publisher and the scope can join it.
    let (readings, distinct, bad) = std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut publication = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                publication += 1;
                cell.publish_probe(linked_figures(publication));
                for _ in 0..publication % 64 {
                    std::hint::spin_loop();
                }
            }
        });
        let (mut readings, mut distinct, mut last, mut bad) = (0_u64, 0_u64, 0_u64, None);
        let deadline = Instant::now() + Duration::from_secs(1);
        while bad.is_none() && distinct < 100_000 && Instant::now() < deadline {
            let figures = cell.read();
            bad = match figures {
                TrackedFigures::Readable(lengths)
                    if figures != linked_figures(lengths.active_len) =>
                {
                    Some(format!("a reading mixed two publications: {figures:?}"))
                }
                TrackedFigures::Readable(lengths) if lengths.active_len < last => Some(format!(
                    "a reading went back from publication {last} to {}",
                    lengths.active_len
                )),
                TrackedFigures::Readable(lengths) => {
                    distinct += u64::from(lengths.active_len != last);
                    last = lengths.active_len;
                    None
                }
                TrackedFigures::FailedClosed(_) => {
                    Some(format!("a readable publication was read as {figures:?}"))
                }
            };
            readings += 1;
        }
        stop.store(true, Ordering::Relaxed);
        (readings, distinct, bad)
    });
    assert_eq!(bad, None, "after {readings} readings");
    assert!(
        distinct >= 1_000,
        "{readings} readings saw only {distinct} publications, so little was tested"
    );
}

#[test]
fn figures_whose_total_overflows_fail_closed() {
    let overflowing = crate::maintenance::family_storage_stats_from_tracked(
        InspectedFamily::KeyValue,
        TrackedWalLengths {
            active_len: u64::MAX,
            sealed_bytes: 1,
            sealed_count: 1,
        },
    );
    assert!(
        matches!(overflowing, Err(CompactionError::FailedClosed { .. })),
        "an overflowing total must answer FailedClosed, not {overflowing:?}"
    );

    let fitting = crate::maintenance::family_storage_stats_from_tracked(
        InspectedFamily::KeyValue,
        TrackedWalLengths {
            active_len: u64::MAX - 1,
            sealed_bytes: 1,
            sealed_count: 1,
        },
    )
    .unwrap();
    assert_eq!(fitting.total_bytes(), u64::MAX);
    assert_eq!(fitting.sealed_segment_count(), 1);
}
