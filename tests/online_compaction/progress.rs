//! Online-compaction availability tests.

use crate::support::{large_snapshot_value, run_until_finished, signals, wait_until_started};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::OnlineCompactionOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[test]
fn public_reads_and_writes_progress_during_staging_for_every_family() {
    assert_key_value_progress();
    assert_key_set_progress();
    assert_key_map_progress();
}

fn assert_key_value_progress() {
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
    let mutation_started = Arc::clone(&started);
    let mutation_finished = Arc::clone(&finished);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&mutation_started, &mutation_finished, |index| {
            mutation_store.put(b"live".to_vec(), index.to_le_bytes().to_vec());
        })
    });
    let read_store = Arc::clone(&store);
    let read_started = Arc::clone(&started);
    let read_finished = Arc::clone(&finished);
    let reader = std::thread::spawn(move || {
        wait_until_started(&read_started);
        let mut reads = 0;
        while !read_finished.load(Ordering::Acquire) {
            assert!(read_store.contains(b"snapshot"));
            reads += 1;
            std::thread::yield_now();
        }
        reads
    });

    let outcome = compaction.join().unwrap().unwrap();
    let mutations = mutation.join().unwrap();
    let reads = reader.join().unwrap();
    assert!(mutations > 0);
    assert!(reads > 0);
    assert!(outcome.concurrent_mutations_replayed() > 0);
}

fn assert_key_set_progress() {
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
    let mutation_started = Arc::clone(&started);
    let mutation_finished = Arc::clone(&finished);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&mutation_started, &mutation_finished, |index| {
            mutation_store.append(b"live".to_vec(), index.to_le_bytes().to_vec());
        })
    });
    let read_store = Arc::clone(&store);
    let read_started = Arc::clone(&started);
    let read_finished = Arc::clone(&finished);
    let reader = std::thread::spawn(move || {
        wait_until_started(&read_started);
        let mut reads = 0;
        while !read_finished.load(Ordering::Acquire) {
            assert!(read_store.contains_key(b"snapshot"));
            reads += 1;
            std::thread::yield_now();
        }
        reads
    });

    let outcome = compaction.join().unwrap().unwrap();
    let mutations = mutation.join().unwrap();
    let reads = reader.join().unwrap();
    assert!(mutations > 0);
    assert!(reads > 0);
    assert!(outcome.concurrent_mutations_replayed() > 0);
}

fn assert_key_map_progress() {
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
    let mutation_started = Arc::clone(&started);
    let mutation_finished = Arc::clone(&finished);
    let mutation = std::thread::spawn(move || {
        run_until_finished(&mutation_started, &mutation_finished, |index| {
            mutation_store.put(
                b"live".to_vec(),
                SearchKey::from(0),
                index.to_le_bytes().to_vec(),
            );
        })
    });
    let read_store = Arc::clone(&store);
    let read_started = Arc::clone(&started);
    let read_finished = Arc::clone(&finished);
    let reader = std::thread::spawn(move || {
        wait_until_started(&read_started);
        let mut reads = 0;
        while !read_finished.load(Ordering::Acquire) {
            assert!(read_store.contains_key(b"snapshot"));
            reads += 1;
            std::thread::yield_now();
        }
        reads
    });

    let outcome = compaction.join().unwrap().unwrap();
    let mutations = mutation.join().unwrap();
    let reads = reader.join().unwrap();
    assert!(mutations > 0);
    assert!(reads > 0);
    assert!(outcome.concurrent_mutations_replayed() > 0);
}
