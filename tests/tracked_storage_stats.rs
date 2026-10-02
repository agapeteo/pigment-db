//! specs/014: an open store's WAL size from its writer's own accounting (FR-1, FR-2).
//!
//! Every assertion reads through the public API: `tracked_storage_stats()` itself, and
//! `storage_stats()`, `try_compact_online`'s outcome, the lengths of the sealed segment files and a
//! reopen to compare it with. Each scenario runs once per store family.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::{CompareExchangeEntry, DurableKeyValueStore};
use pigment_db::model::SearchKey;
use pigment_db::{
    CompactionError, DurableStoreOptions, FamilyCompactionOutcome, FamilyStorageStats,
    OnlineCompactionOptions, WalSegmentSize,
};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a test waits for another thread before calling it stuck.
const WATCHDOG: Duration = Duration::from_secs(10);
/// How long a write may take before a test treats it as held.
const HELD: Duration = Duration::from_millis(200);
/// The key a compute callback runs on; ordinary writes use small indices.
const COMPUTED: usize = 1_000_000;

/// A segment target that a single record fills, so nearly every write rotates.
fn rotating() -> DurableStoreOptions {
    DurableStoreOptions::default().with_wal_segment_size(WalSegmentSize::try_from(256_u64).unwrap())
}

/// A segment target that holds several records, so the active segment grows between rotations.
fn several_records_per_segment() -> DurableStoreOptions {
    DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(4096_u64).unwrap())
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:07}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("{index:064}").into_bytes()
}

/// A value whose length varies with `index`, so segments seal at different lengths.
fn varied_value(index: usize) -> Vec<u8> {
    vec![b'v'; 16 + (index * 37) % 200]
}

/// The path of sealed segment `segment` beside the active segment `active`.
fn sealed_segment(directory: &Path, active: &str, segment: usize) -> std::path::PathBuf {
    directory.join(format!("{active}.segment-{segment:020}"))
}

/// One store family, opened with [`rotating`] options unless a scenario names others.
trait Store: Send + Sync + Sized + 'static {
    const ACTIVE: &'static str;
    fn open_with(directory: &Path, options: DurableStoreOptions) -> Self;
    fn write_value(&self, index: usize, value: Vec<u8>);

    fn open(directory: &Path) -> Self {
        Self::open_with(directory, rotating())
    }

    fn write(&self, index: usize) {
        self.write_value(index, value(index));
    }

    fn compute_with(&self, index: usize, inside: impl FnOnce());
    fn validated(&self) -> Result<FamilyStorageStats, CompactionError>;
    fn tracked(&self) -> Result<FamilyStorageStats, CompactionError>;
    fn compact(&self) -> FamilyCompactionOutcome;
}

impl Store for DurableKeyValueStore<File> {
    const ACTIVE: &'static str = "kv.wal.dat";

    fn open_with(directory: &Path, options: DurableStoreOptions) -> Self {
        DurableKeyValueStore::try_init_new_with_options(directory, options)
            .unwrap()
            .into_store()
    }

    fn write_value(&self, index: usize, value: Vec<u8>) {
        self.put(key(index), value);
    }

    fn compute_with(&self, index: usize, inside: impl FnOnce()) {
        self.try_compute(key(index), |_| {
            inside();
            value(index)
        })
        .unwrap();
    }

    fn validated(&self) -> Result<FamilyStorageStats, CompactionError> {
        self.storage_stats()
    }

    fn tracked(&self) -> Result<FamilyStorageStats, CompactionError> {
        self.tracked_storage_stats()
    }

    fn compact(&self) -> FamilyCompactionOutcome {
        self.try_compact_online(OnlineCompactionOptions::default())
            .unwrap()
    }
}

impl Store for DurableKeySetStore<File> {
    const ACTIVE: &'static str = "set.wal.dat";

    fn open_with(directory: &Path, options: DurableStoreOptions) -> Self {
        DurableKeySetStore::try_init_new_with_options(directory, options)
            .unwrap()
            .into_store()
    }

    fn write_value(&self, index: usize, value: Vec<u8>) {
        self.append(key(index), value);
    }

    fn compute_with(&self, index: usize, inside: impl FnOnce()) {
        self.try_compute(key(index), |set| {
            inside();
            set.insert(value(index));
        })
        .unwrap();
    }

    fn validated(&self) -> Result<FamilyStorageStats, CompactionError> {
        self.storage_stats()
    }

    fn tracked(&self) -> Result<FamilyStorageStats, CompactionError> {
        self.tracked_storage_stats()
    }

    fn compact(&self) -> FamilyCompactionOutcome {
        self.try_compact_online(OnlineCompactionOptions::default())
            .unwrap()
    }
}

impl Store for DurableKeyMapStore<File> {
    const ACTIVE: &'static str = "map.wal.dat";

    fn open_with(directory: &Path, options: DurableStoreOptions) -> Self {
        DurableKeyMapStore::try_init_new_with_options(directory, options)
            .unwrap()
            .into_store()
    }

    fn write_value(&self, index: usize, value: Vec<u8>) {
        self.put(key(index), SearchKey::from(index), value);
    }

    fn compute_with(&self, index: usize, inside: impl FnOnce()) {
        self.try_compute(key(index), |map| {
            inside();
            map.insert(SearchKey::from(index), value(index));
        })
        .unwrap();
    }

    fn validated(&self) -> Result<FamilyStorageStats, CompactionError> {
        self.storage_stats()
    }

    fn tracked(&self) -> Result<FamilyStorageStats, CompactionError> {
        self.tracked_storage_stats()
    }

    fn compact(&self) -> FamilyCompactionOutcome {
        self.try_compact_online(OnlineCompactionOptions::default())
            .unwrap()
    }
}

fn tracked_stats_equal_storage_stats_after_rotations_and_a_reopen<S: Store>() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = S::open(directory.path());
        for index in 0..12 {
            store.write(index);
        }
        let validated = store.validated().unwrap();
        assert!(
            validated.sealed_segment_count() >= 3,
            "the writes rotated fewer than three times: {validated:?}"
        );
        assert_eq!(
            store.tracked().unwrap(),
            validated,
            "the tracked figures differ from storage_stats() after rotations"
        );
    }

    let reopened = S::open(directory.path());
    let validated = reopened.validated().unwrap();
    assert!(validated.sealed_segment_count() >= 3);
    assert_eq!(
        reopened.tracked().unwrap(),
        validated,
        "the tracked figures differ from storage_stats() after a reopen"
    );
    reopened.write(12);
    assert_eq!(
        reopened.tracked().unwrap(),
        reopened.validated().unwrap(),
        "the tracked figures differ from storage_stats() after a write to the reopened store"
    );
}

fn tracked_stats_agree_after_online_compaction_and_a_further_rotation<S: Store>() {
    let directory = tempfile::tempdir().unwrap();
    let store = S::open(directory.path());
    for index in 0..12 {
        store.write(index);
    }
    assert!(store.validated().unwrap().sealed_segment_count() >= 3);

    let outcome = store.compact();
    let compacted = store.tracked().unwrap();
    assert_eq!(
        compacted,
        store.validated().unwrap(),
        "the tracked figures differ from storage_stats() after an online compaction"
    );
    assert_eq!(compacted.sealed_segment_count(), 0);
    assert_eq!(compacted.total_bytes(), outcome.after_bytes());

    // The replacement already exceeds the segment target, so the next write rotates.
    store.write(12);
    let rotated = store.validated().unwrap();
    assert_eq!(
        rotated.sealed_segment_count(),
        1,
        "the write did not rotate"
    );
    assert_eq!(
        store.tracked().unwrap(),
        rotated,
        "the tracked figures differ from storage_stats() after a rotation of the replacement"
    );
}

/// Makes a directory unreadable for the duration of a test, and restores it on drop. Fails the
/// test where permissions are not enforced for this user (for example root), because the
/// scenario would otherwise pass without reaching what it tests.
#[cfg(unix)]
struct UnreadableDirectory(std::path::PathBuf);

#[cfg(unix)]
impl UnreadableDirectory {
    fn new(path: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000))
            .expect("make the directory unreadable");
        let guard = Self(path.to_path_buf());
        assert!(
            std::fs::read_dir(path).is_err(),
            "permissions are not enforced for this user, so this scenario cannot be staged; run \
             the suite as an unprivileged user"
        );
        guard
    }
}

#[cfg(unix)]
impl Drop for UnreadableDirectory {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

#[cfg(unix)]
fn tracked_stats_read_no_file<S: Store>() {
    let directory = tempfile::tempdir().unwrap();
    let store = S::open(directory.path());
    for index in 0..12 {
        store.write(index);
    }
    let validated = store.validated().unwrap();

    let unreadable = UnreadableDirectory::new(directory.path());
    let refused = store.validated();
    let tracked = store.tracked();
    drop(unreadable);

    assert!(
        refused.is_err(),
        "storage_stats() read an unreadable directory: {refused:?}"
    );
    assert_eq!(
        tracked.unwrap(),
        validated,
        "the tracked figures need the store directory"
    );
}

/// FR-2: a reading is the WAL at one instant. Segments hold several records of varying length, so
/// the active length grows between rotations, segments seal at different lengths, and a reading
/// that took the active length and the sealed figures at two instants around a rotation shows: it
/// goes backwards against the next reading, or its active length exceeds what its segment ever
/// reached.
fn tracked_stats_are_consistent_under_concurrent_rotation<S: Store>() {
    const WRITERS: usize = 4;
    /// Distinct readings kept, and writes per writer. Both stop there, so an implementation whose
    /// figures never reach the anti-vacuity thresholds cannot exhaust memory before the check
    /// reports it.
    const MAX_READINGS: usize = 100_000;
    const MAX_WRITES_PER_WRITER: usize = 25_000;
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(S::open_with(
        directory.path(),
        several_records_per_segment(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let (store, stop) = (store.clone(), stop.clone());
            std::thread::spawn(move || {
                for write in 0..MAX_WRITES_PER_WRITER {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let index = write * WRITERS + writer;
                    store.write_value(index, varied_value(index));
                }
            })
        })
        .collect();

    let mut readings: Vec<FamilyStorageStats> = Vec::new();
    let mut counts = BTreeSet::new();
    let mut active_lengths = BTreeSet::new();
    let deadline = Instant::now() + WATCHDOG;
    while (counts.len() < 10 || active_lengths.len() < 50)
        && readings.len() < MAX_READINGS
        && Instant::now() < deadline
    {
        let reading = store
            .tracked()
            .expect("a reading under concurrent rotation failed");
        if readings.last() != Some(&reading) {
            counts.insert(reading.sealed_segment_count());
            active_lengths.insert(reading.active_bytes());
            readings.push(reading);
        }
    }
    stop.store(true, Ordering::Release);
    for writer in writers {
        writer.join().unwrap();
    }

    assert!(
        counts.len() >= 10,
        "the readings saw too few rotations, so little was tested: {counts:?}"
    );
    assert!(
        active_lengths.len() >= 50,
        "the readings saw too few active lengths for a torn reading to show: {active_lengths:?}"
    );
    for pair in readings.windows(2) {
        assert!(
            pair[1].total_bytes() >= pair[0].total_bytes()
                && pair[1].sealed_segment_count() >= pair[0].sealed_segment_count(),
            "a later reading went backwards: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    // Sealed segments are immutable, so their lengths now are their lengths at every reading;
    // the segment still active has its final length now that the writers have stopped.
    let last = store.tracked().unwrap();
    let mut segment_lengths: Vec<u64> = (0..last.sealed_segment_count())
        .map(|segment| {
            std::fs::metadata(sealed_segment(directory.path(), S::ACTIVE, segment))
                .unwrap()
                .len()
        })
        .collect();
    segment_lengths.push(last.active_bytes());
    let mut sealed_prefix = vec![0_u64];
    for length in &segment_lengths {
        sealed_prefix.push(sealed_prefix.last().unwrap() + length);
    }
    for reading in &readings {
        let count = reading.sealed_segment_count();
        assert_eq!(
            reading.active_bytes() + reading.sealed_segment_bytes(),
            reading.total_bytes(),
            "the parts do not sum to the total: {reading:?}"
        );
        assert_eq!(
            reading.sealed_segment_bytes(),
            sealed_prefix[count],
            "the sealed bytes are not the lengths of the sealed segments counted: {reading:?}"
        );
        assert!(
            reading.active_bytes() <= segment_lengths[count],
            "the active length exceeds the length its segment reached ({}): {reading:?}",
            segment_lengths[count]
        );
    }
    assert_eq!(
        last,
        store.validated().unwrap(),
        "the tracked figures differ from storage_stats() once the writers stop"
    );
}

/// FR-2: the figures are not a validation. Bytes appended to the active segment and a sealed
/// segment cut short after the open do not move them, while `storage_stats()` sees the damage.
fn tracked_stats_do_not_see_damage_made_after_the_open<S: Store>() {
    let directory = tempfile::tempdir().unwrap();
    let store = S::open(directory.path());
    for index in 0..12 {
        store.write(index);
    }
    let before = store.tracked().unwrap();
    assert_eq!(before, store.validated().unwrap());
    assert!(before.sealed_segment_count() >= 1);

    OpenOptions::new()
        .append(true)
        .open(directory.path().join(S::ACTIVE))
        .unwrap()
        .write_all(&[0xAB; 100])
        .unwrap();
    let sealed = OpenOptions::new()
        .write(true)
        .open(sealed_segment(directory.path(), S::ACTIVE, 0))
        .unwrap();
    let sealed_len = sealed.metadata().unwrap().len();
    sealed.set_len(sealed_len - 1).unwrap();
    drop(sealed);

    let validated = store.validated();
    assert_ne!(
        validated.as_ref().ok(),
        Some(&before),
        "storage_stats() did not see the damage, so nothing was tested"
    );
    assert_eq!(
        store.tracked().unwrap(),
        before,
        "the tracked figures moved with damage made after the open"
    );
}

/// Spawns a write of `index` and reports whether it finished within [`HELD`]. The handle is
/// returned either way, so a held write is joined once it is released.
fn write_within_held<S: Store>(store: &Arc<S>, index: usize) -> (bool, JoinHandle<()>) {
    let (done_tx, done_rx) = mpsc::channel();
    let writer = {
        let store = store.clone();
        std::thread::spawn(move || {
            store.write(index);
            let _ = done_tx.send(());
        })
    };
    (done_rx.recv_timeout(HELD).is_ok(), writer)
}

fn a_reading_inside_a_compute_callback_completes_while_a_compaction_waits<S: Store>() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(S::open(directory.path()));
    for index in 0..12 {
        store.write(index);
    }
    let before = store.validated().unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (read_tx, read_rx) = mpsc::channel::<()>();
    let (reading_tx, reading_rx) = mpsc::channel();
    let computer = {
        let store = store.clone();
        std::thread::spawn(move || {
            let inner = store.clone();
            store.compute_with(COMPUTED, move || {
                entered_tx.send(()).unwrap();
                read_rx.recv().unwrap();
                reading_tx.send(inner.tracked()).unwrap();
            });
        })
    };
    entered_rx
        .recv_timeout(WATCHDOG)
        .expect("the compute callback was not entered");

    // A key whose writes the callback's own part of the store does not hold.
    let mut released = Vec::new();
    let mut probe = 0_usize;
    loop {
        probe += 1;
        assert!(probe < 64, "every probe key shares the callback's part");
        let (finished, writer) = write_within_held(&store, 100 + probe);
        released.push(writer);
        if finished {
            break;
        }
    }

    let compactor = {
        let store = store.clone();
        std::thread::spawn(move || store.compact())
    };
    // Once the compaction waits behind the callback, a write to the free key waits behind it.
    let deadline = Instant::now() + WATCHDOG;
    loop {
        assert!(
            Instant::now() < deadline,
            "the compaction never waited behind the compute callback"
        );
        let (finished, writer) = write_within_held(&store, 100 + probe);
        released.push(writer);
        if !finished {
            break;
        }
    }

    read_tx.send(()).unwrap();
    let reading = reading_rx.recv_timeout(WATCHDOG).expect(
        "a reading inside a compute callback did not complete while a compaction waited for the \
         callback",
    );
    let reading = reading.unwrap();
    computer.join().unwrap();
    compactor.join().unwrap();
    for writer in released {
        writer.join().unwrap();
    }

    assert!(
        reading.total_bytes() >= before.total_bytes(),
        "the reading inside the callback lost bytes: {reading:?} after {before:?}"
    );
    assert_eq!(
        reading.active_bytes() + reading.sealed_segment_bytes(),
        reading.total_bytes()
    );
    assert_eq!(store.tracked().unwrap(), store.validated().unwrap());
}

/// A key/value compare-exchange batch takes the store's transaction lock exclusively, and a
/// compute callback holds it shared. A reading that took that lock inside the callback would wait
/// behind the queued batch, which waits for the callback.
#[test]
fn key_value_reading_inside_a_compute_callback_completes_while_a_batch_waits() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(<DurableKeyValueStore<File> as Store>::open(
        directory.path(),
    ));
    for index in 0..12 {
        store.write(index);
    }
    let before = store.validated().unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (read_tx, read_rx) = mpsc::channel::<()>();
    let (reading_tx, reading_rx) = mpsc::channel();
    let computer = {
        let store = store.clone();
        std::thread::spawn(move || {
            let inner = store.clone();
            store.compute_with(COMPUTED, move || {
                entered_tx.send(()).unwrap();
                read_rx.recv().unwrap();
                reading_tx.send(inner.tracked()).unwrap();
            });
        })
    };
    entered_rx
        .recv_timeout(WATCHDOG)
        .expect("the compute callback was not entered");

    let (batch_tx, batch_rx) = mpsc::channel();
    let batcher = {
        let store = store.clone();
        std::thread::spawn(move || {
            let result = store.try_compare_exchange_batch(&[CompareExchangeEntry {
                key: key(COMPUTED + 1),
                expected: None,
                replacement: Some(value(COMPUTED + 1)),
            }]);
            batch_tx.send(result.is_ok()).unwrap();
        })
    };
    assert!(
        batch_rx.recv_timeout(HELD).is_err(),
        "the batch did not wait behind the compute callback, so nothing was tested"
    );

    read_tx.send(()).unwrap();
    let reading = reading_rx.recv_timeout(WATCHDOG).expect(
        "a reading inside a compute callback did not complete while a batch waited for the \
         callback",
    );
    computer.join().unwrap();
    assert!(
        batch_rx.recv_timeout(WATCHDOG).unwrap(),
        "the queued batch failed"
    );
    batcher.join().unwrap();

    let reading = reading.unwrap();
    assert!(
        reading.total_bytes() >= before.total_bytes(),
        "the reading inside the callback lost bytes: {reading:?} after {before:?}"
    );
    assert_eq!(store.tracked().unwrap(), store.validated().unwrap());
}

macro_rules! family_tests {
    ($family:ident, $store:ty) => {
        mod $family {
            use super::*;

            #[test]
            fn tracked_stats_equal_storage_stats_after_rotations_and_a_reopen() {
                super::tracked_stats_equal_storage_stats_after_rotations_and_a_reopen::<$store>();
            }

            #[test]
            fn tracked_stats_agree_after_online_compaction_and_a_further_rotation() {
                super::tracked_stats_agree_after_online_compaction_and_a_further_rotation::<$store>(
                );
            }

            #[cfg(unix)]
            #[test]
            fn tracked_stats_read_no_file() {
                super::tracked_stats_read_no_file::<$store>();
            }

            #[test]
            fn tracked_stats_are_consistent_under_concurrent_rotation() {
                super::tracked_stats_are_consistent_under_concurrent_rotation::<$store>();
            }

            #[test]
            fn tracked_stats_do_not_see_damage_made_after_the_open() {
                super::tracked_stats_do_not_see_damage_made_after_the_open::<$store>();
            }

            #[test]
            fn a_reading_inside_a_compute_callback_completes_while_a_compaction_waits() {
                super::a_reading_inside_a_compute_callback_completes_while_a_compaction_waits::<
                    $store,
                >();
            }
        }
    };
}

family_tests!(key_value, DurableKeyValueStore<File>);
family_tests!(key_set, DurableKeySetStore<File>);
family_tests!(key_map, DurableKeyMapStore<File>);
