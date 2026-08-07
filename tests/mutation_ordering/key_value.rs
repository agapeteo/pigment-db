//! Public key/value mutation-ordering contract tests.

use crate::support::assert_key_value_reopens;
use pigment_db::key_value_store::DurableKeyValueStore;
use std::sync::{Arc, Barrier};

#[test]
fn completion_before_invocation_orders_key_value_mutations() {
    let directory = tempfile::tempdir().expect("create key/value completion-order directory");
    let key = b"ordered".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize completion-order store")
        .into_store();
    store.put(key.clone(), b"first".to_vec());
    store.put(key.clone(), b"second".to_vec());
    let expected = Some(b"second".to_vec());
    assert_eq!(store.get(&key), expected);
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn overlapping_key_value_mutations_accept_either_order() {
    let directory = tempfile::tempdir().expect("create key/value overlap directory");
    let key = b"overlap".to_vec();
    let store = Arc::new(
        DurableKeyValueStore::try_init_new(directory.path())
            .expect("initialize overlap store")
            .into_store(),
    );
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for value in [b"left".to_vec(), b"right".to_vec()] {
        let worker_store = Arc::clone(&store);
        let worker_barrier = Arc::clone(&barrier);
        let worker_key = key.clone();
        workers.push(std::thread::spawn(move || {
            worker_barrier.wait();
            worker_store.put(worker_key, value);
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().expect("overlapping key/value put must join");
    }
    let expected = store.get(&key);
    assert!(matches!(
        expected.as_deref(),
        Some(b"left") | Some(b"right")
    ));
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn remove_participates_in_public_family_matrix() {
    let directory = tempfile::tempdir().expect("create key/value removal directory");
    let key = b"remove".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize key/value removal store")
        .into_store();
    store.put(key.clone(), b"value".to_vec());
    store.remove(&key);
    let expected = None;
    assert_eq!(store.get(&key), expected);
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn put_then_compute_preserves_public_and_reopened_state() {
    let directory = tempfile::tempdir().expect("create put/compute directory");
    let key = b"key".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize put/compute store")
        .into_store();
    store.put(key.clone(), b"ordinary".to_vec());
    store.compute(key.clone(), |_| b"computed".to_vec());
    let expected = Some(b"computed".to_vec());
    assert_eq!(store.get(&key), expected);
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn compute_then_put_preserves_public_and_reopened_state() {
    let directory = tempfile::tempdir().expect("create compute/put directory");
    let key = b"key".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize compute/put store")
        .into_store();
    store.compute(key.clone(), |_| b"computed".to_vec());
    store.put(key.clone(), b"ordinary".to_vec());
    let expected = Some(b"ordinary".to_vec());
    assert_eq!(store.get(&key), expected);
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn set_number_then_increment_preserves_public_and_reopened_state() {
    let directory = tempfile::tempdir().expect("create numeric increment directory");
    let key = b"number".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize numeric increment store")
        .into_store();
    store.set_number(key.clone(), 10);
    assert_eq!(store.increment_or_init(key.clone(), 5), Ok(15));
    let expected = Some(15_u64.to_ne_bytes().to_vec());
    assert_eq!(store.get(&key), expected);
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn increment_then_decrement_preserves_public_and_reopened_state() {
    let directory = tempfile::tempdir().expect("create numeric decrement directory");
    let key = b"number".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize numeric decrement store")
        .into_store();
    assert_eq!(store.increment_or_init(key.clone(), 10), Ok(10));
    assert_eq!(store.decrement(key.clone(), 3), Some(Ok(7)));
    let expected = Some(7_u64.to_ne_bytes().to_vec());
    assert_eq!(store.get(&key), expected);
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}
