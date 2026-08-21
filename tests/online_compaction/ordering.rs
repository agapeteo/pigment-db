//! Online-compaction mutation-ordering tests.

use crate::support::{
    assert_map_reopens, assert_set_reopens, assert_value_reopens, large_snapshot_value,
    run_until_finished, signals,
};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::OnlineCompactionOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[test]
fn accepted_ordered_mutation_sequences_replay_exactly_for_every_family() {
    assert_key_value_ordering();
    assert_key_set_ordering();
    assert_key_map_ordering();
}

fn assert_key_value_ordering() {
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
        let result = compaction_store.try_compact_online(OnlineCompactionOptions::default());
        compaction_finished.store(true, Ordering::Release);
        result
    });
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&started, &finished, |index| {
            mutation_store.put(b"live".to_vec(), b"ordinary".to_vec());
            mutation_store.remove(b"live");
            mutation_store.compute(b"live".to_vec(), |_| index.to_le_bytes().to_vec());
            mutation_store.put(
                format!("independent-{index}").into_bytes(),
                b"present".to_vec(),
            );
        })
    });

    let outcome = compaction.join().unwrap().unwrap();
    assert!(mutation.join().unwrap() > 0);
    assert!(outcome.concurrent_mutations_replayed() > 0);
    let expected = store.get(b"live");
    assert!(expected.is_some());
    drop(store);
    assert_value_reopens(directory.path(), &expected);
}

fn assert_key_set_ordering() {
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
        let result = compaction_store.try_compact_online(OnlineCompactionOptions::default());
        compaction_finished.store(true, Ordering::Release);
        result
    });
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&started, &finished, |index| {
            mutation_store.append(b"live".to_vec(), b"ordinary".to_vec());
            mutation_store.remove_from_set(b"live".to_vec(), b"ordinary".to_vec());
            mutation_store.compute(b"live".to_vec(), |set| {
                set.insert(index.to_le_bytes().to_vec());
            });
            mutation_store.append(
                format!("independent-{index}").into_bytes(),
                b"present".to_vec(),
            );
        })
    });

    let outcome = compaction.join().unwrap().unwrap();
    assert!(mutation.join().unwrap() > 0);
    assert!(outcome.concurrent_mutations_replayed() > 0);
    let expected = store.get_hashset(b"live");
    assert!(expected.is_some());
    drop(store);
    assert_set_reopens(directory.path(), &expected);
}

fn assert_key_map_ordering() {
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
        let result = compaction_store.try_compact_online(OnlineCompactionOptions::default());
        compaction_finished.store(true, Ordering::Release);
        result
    });
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&started, &finished, |index| {
            mutation_store.put(b"live".to_vec(), SearchKey::from(0), b"ordinary".to_vec());
            mutation_store.remove_from_sorted_map(b"live".to_vec(), SearchKey::from(0));
            mutation_store.compute(b"live".to_vec(), |map| {
                map.insert(SearchKey::from(0), index.to_le_bytes().to_vec());
            });
            mutation_store.put(
                format!("independent-{index}").into_bytes(),
                SearchKey::from(0),
                b"present".to_vec(),
            );
        })
    });

    let outcome = compaction.join().unwrap().unwrap();
    assert!(mutation.join().unwrap() > 0);
    assert!(outcome.concurrent_mutations_replayed() > 0);
    let expected = store.get_sorted_map(b"live");
    assert!(expected.is_some());
    drop(store);
    assert_map_reopens(directory.path(), &expected);
}
