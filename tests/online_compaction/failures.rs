//! Online-compaction failure tests.

use crate::support::{
    assert_map_reopens, assert_set_reopens, assert_value_reopens, large_snapshot_value,
    run_until_finished, signals,
};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{CompactionError, FamilyCompactionOutcome, OnlineCompactionOptions};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};

#[test]
fn public_delta_overflow_aborts_without_disabling_any_family() {
    assert_key_value_overflow();
    assert_key_set_overflow();
    assert_key_map_overflow();
}

#[test]
fn public_overlapping_attempts_reject_exactly_one_caller_for_every_family() {
    let value_directory = tempfile::tempdir().unwrap();
    let value_store = Arc::new(
        DurableKeyValueStore::try_init_new(value_directory.path())
            .unwrap()
            .into_store(),
    );
    value_store.put(b"snapshot".to_vec(), large_snapshot_value());
    assert_one_attempt_wins(Arc::new({
        let store = Arc::clone(&value_store);
        move || store.try_compact_online(OnlineCompactionOptions::default())
    }));
    assert!(value_store.contains(b"snapshot"));

    let set_directory = tempfile::tempdir().unwrap();
    let set_store = Arc::new(
        DurableKeySetStore::try_init_new(set_directory.path())
            .unwrap()
            .into_store(),
    );
    set_store.append(b"snapshot".to_vec(), large_snapshot_value());
    assert_one_attempt_wins(Arc::new({
        let store = Arc::clone(&set_store);
        move || store.try_compact_online(OnlineCompactionOptions::default())
    }));
    assert!(set_store.contains_key(b"snapshot"));

    let map_directory = tempfile::tempdir().unwrap();
    let map_store = Arc::new(
        DurableKeyMapStore::try_init_new(map_directory.path())
            .unwrap()
            .into_store(),
    );
    map_store.put(
        b"snapshot".to_vec(),
        SearchKey::from(0),
        large_snapshot_value(),
    );
    assert_one_attempt_wins(Arc::new({
        let store = Arc::clone(&map_store);
        move || store.try_compact_online(OnlineCompactionOptions::default())
    }));
    assert!(map_store.contains_key(b"snapshot"));
}

fn assert_one_attempt_wins(
    compact: Arc<
        dyn Fn() -> Result<FamilyCompactionOutcome, CompactionError> + Send + Sync + 'static,
    >,
) {
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let worker_barrier = Arc::clone(&barrier);
        let worker_compact = Arc::clone(&compact);
        workers.push(std::thread::spawn(move || {
            worker_barrier.wait();
            worker_compact()
        }));
    }
    barrier.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(CompactionError::FailedClosed { .. })))
            .count(),
        1
    );
}

fn assert_key_value_overflow() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    store.put(b"snapshot".to_vec(), large_snapshot_value());
    let (started, finished) = signals();
    let compaction_store = Arc::clone(&store);
    let compaction_started = Arc::clone(&started);
    let compaction_finished = Arc::clone(&finished);
    let compaction = std::thread::spawn(move || {
        compaction_started.store(true, Ordering::Release);
        let result = compaction_store
            .try_compact_online(OnlineCompactionOptions::default().with_max_delta_bytes(0));
        compaction_finished.store(true, Ordering::Release);
        result
    });
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&started, &finished, |index| {
            mutation_store.put(b"live".to_vec(), index.to_le_bytes().to_vec());
        })
    });

    assert!(matches!(
        compaction.join().unwrap(),
        Err(CompactionError::ConcurrentDeltaLimitExceeded { limit: 0 })
    ));
    assert!(mutation.join().unwrap() > 0);
    store.put(b"live".to_vec(), b"still-writable".to_vec());
    let expected = store.get(b"live");
    drop(store);
    assert_value_reopens(directory.path(), &expected);
}

fn assert_key_set_overflow() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    store.append(b"snapshot".to_vec(), large_snapshot_value());
    let (started, finished) = signals();
    let compaction_store = Arc::clone(&store);
    let compaction_started = Arc::clone(&started);
    let compaction_finished = Arc::clone(&finished);
    let compaction = std::thread::spawn(move || {
        compaction_started.store(true, Ordering::Release);
        let result = compaction_store
            .try_compact_online(OnlineCompactionOptions::default().with_max_delta_bytes(0));
        compaction_finished.store(true, Ordering::Release);
        result
    });
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&started, &finished, |index| {
            mutation_store.append(b"live".to_vec(), index.to_le_bytes().to_vec());
        })
    });

    assert!(matches!(
        compaction.join().unwrap(),
        Err(CompactionError::ConcurrentDeltaLimitExceeded { limit: 0 })
    ));
    assert!(mutation.join().unwrap() > 0);
    store.append(b"live".to_vec(), b"still-writable".to_vec());
    let expected = store.get_hashset(b"live");
    drop(store);
    assert_set_reopens(directory.path(), &expected);
}

fn assert_key_map_overflow() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    store.put(
        b"snapshot".to_vec(),
        SearchKey::from(0),
        large_snapshot_value(),
    );
    let (started, finished) = signals();
    let compaction_store = Arc::clone(&store);
    let compaction_started = Arc::clone(&started);
    let compaction_finished = Arc::clone(&finished);
    let compaction = std::thread::spawn(move || {
        compaction_started.store(true, Ordering::Release);
        let result = compaction_store
            .try_compact_online(OnlineCompactionOptions::default().with_max_delta_bytes(0));
        compaction_finished.store(true, Ordering::Release);
        result
    });
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&started, &finished, |index| {
            mutation_store.put(
                b"live".to_vec(),
                SearchKey::from(0),
                index.to_le_bytes().to_vec(),
            );
        })
    });

    assert!(matches!(
        compaction.join().unwrap(),
        Err(CompactionError::ConcurrentDeltaLimitExceeded { limit: 0 })
    ));
    assert!(mutation.join().unwrap() > 0);
    store.put(
        b"live".to_vec(),
        SearchKey::from(0),
        b"still-writable".to_vec(),
    );
    let expected = store.get_sorted_map(b"live");
    drop(store);
    assert_map_reopens(directory.path(), &expected);
}
