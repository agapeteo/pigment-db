//! Public key/sorted-map mutation-ordering contract tests.

use crate::support::assert_key_map_reopens;
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier};

#[test]
fn completion_before_invocation_orders_map_mutations() {
    let directory = tempfile::tempdir().expect("create map completion-order directory");
    let key = b"ordered".to_vec();
    let search_key = SearchKey::from(1);
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize map completion-order store")
        .into_store();
    store.put(key.clone(), search_key.clone(), b"first".to_vec());
    store.put(key.clone(), search_key, b"second".to_vec());
    let expected = store.get_sorted_map(&key);
    assert_eq!(
        expected.as_ref().and_then(|map| map.values().next()),
        Some(&b"second".to_vec())
    );
    drop(store);
    assert_key_map_reopens(directory.path(), &key, &expected);
}

#[test]
fn overlapping_map_mutations_accept_either_order() {
    let directory = tempfile::tempdir().expect("create map overlap directory");
    let key = b"overlap".to_vec();
    let search_key = SearchKey::from(1);
    let store = Arc::new(
        DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize map overlap store")
            .into_store(),
    );
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for value in [b"left".to_vec(), b"right".to_vec()] {
        let worker_store = Arc::clone(&store);
        let worker_barrier = Arc::clone(&barrier);
        let worker_key = key.clone();
        let worker_search_key = search_key.clone();
        workers.push(std::thread::spawn(move || {
            worker_barrier.wait();
            worker_store.put(worker_key, worker_search_key, value);
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().expect("overlapping map put must join");
    }
    let expected = store.get_sorted_map(&key);
    let value = expected.as_ref().and_then(|map| map.get(&search_key));
    assert!(matches!(
        value.map(Vec::as_slice),
        Some(b"left") | Some(b"right")
    ));
    drop(store);
    assert_key_map_reopens(directory.path(), &key, &expected);
}

#[test]
fn all_map_mutators_participate_in_public_family_matrix() {
    let directory = tempfile::tempdir().expect("create map family matrix directory");
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize map family matrix store")
        .into_store();
    let main = b"main".to_vec();
    let deleted = b"deleted".to_vec();

    store.put(main.clone(), SearchKey::from(1), b"put".to_vec());
    store.remove_from_sorted_map(main.clone(), SearchKey::from(1));
    store.put(main.clone(), SearchKey::from(2), b"callback".to_vec());
    store.remove_from_sorted_map_callback(main.clone(), SearchKey::from(2), |_| {});
    store
        .try_compute(main.clone(), |map| {
            map.insert(SearchKey::from(3), b"try-compute".to_vec());
        })
        .unwrap();
    store.compute(main.clone(), |map| {
        map.insert(SearchKey::from(4), b"compute".to_vec());
    });
    store
        .try_compute_if_present(main.clone(), |map| {
            map.insert(SearchKey::from(5), b"try-present".to_vec());
        })
        .unwrap();
    store.compute_if_present(main.clone(), |map| {
        map.insert(SearchKey::from(6), b"present".to_vec());
    });
    store
        .try_compute_if_absent(b"try-absent".to_vec(), |map| {
            map.insert(SearchKey::from(1), b"created".to_vec());
        })
        .unwrap();
    store.compute_if_absent(b"absent".to_vec(), |map| {
        map.insert(SearchKey::from(1), b"created".to_vec());
    });
    store.put(b"pop-first".to_vec(), SearchKey::from(1), b"one".to_vec());
    let _ = store.pop_first(b"pop-first".to_vec());
    store.put(b"pop-last".to_vec(), SearchKey::from(1), b"one".to_vec());
    let _ = store.pop_last(b"pop-last".to_vec());
    store.append_ordered_element(b"ordered-elements".to_vec(), b"zero".to_vec());
    store.put(deleted.clone(), SearchKey::from(1), b"gone".to_vec());
    store.remove_key(&deleted);

    let keys = [
        main,
        b"try-absent".to_vec(),
        b"absent".to_vec(),
        b"pop-first".to_vec(),
        b"pop-last".to_vec(),
        b"ordered-elements".to_vec(),
        deleted,
    ];
    let snapshots: Vec<_> = keys
        .iter()
        .map(|key| (key.clone(), store.get_sorted_map(key)))
        .collect();
    drop(store);
    for (key, expected) in snapshots {
        assert_key_map_reopens(directory.path(), &key, &expected);
    }
}

#[test]
fn overlapping_reads_never_observe_map_working_state() {
    let directory = tempfile::tempdir().expect("create map read-visibility directory");
    let key = b"key".to_vec();
    let store = Arc::new(
        DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize map read-visibility store")
            .into_store(),
    );
    store.put(key.clone(), SearchKey::from(1), b"before".to_vec());
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let compute_store = Arc::clone(&store);
    let compute_key = key.clone();
    let compute = std::thread::spawn(move || {
        compute_store.compute(compute_key, move |map| {
            map.insert(SearchKey::from(2), b"new-a".to_vec());
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            map.insert(SearchKey::from(3), b"new-b".to_vec());
        });
    });
    entered_rx.recv().unwrap();

    let (read_tx, read_rx) = mpsc::sync_channel(0);
    let read_store = Arc::clone(&store);
    let read_key = key.clone();
    let read = std::thread::spawn(move || {
        read_tx.send(read_store.get_sorted_map(&read_key)).unwrap();
    });
    assert!(matches!(
        read_rx.recv_timeout(std::time::Duration::from_millis(250)),
        Err(RecvTimeoutError::Timeout)
    ));
    release_tx.send(()).unwrap();
    compute.join().unwrap();
    let observed = read_rx.recv().unwrap();
    read.join().unwrap();
    assert_eq!(observed.as_ref().map(|map| map.len()), Some(3));
    drop(store);
    assert_key_map_reopens(directory.path(), &key, &observed);
}
