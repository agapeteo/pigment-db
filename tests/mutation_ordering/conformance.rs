//! Public-only concurrent-history conformance tests.

use crate::support::{assert_key_map_reopens, assert_key_set_reopens, assert_key_value_reopens};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use std::sync::{Arc, Barrier};

#[test]
#[ignore = "release public-only concurrent-history smoke matrix"]
fn public_histories() {
    let kv_directory = tempfile::tempdir().expect("create public key/value history directory");
    let kv_key = b"key".to_vec();
    let kv = Arc::new(
        DurableKeyValueStore::try_init_new(kv_directory.path())
            .expect("initialize public key/value history")
            .into_store(),
    );
    run_pair(
        {
            let store = Arc::clone(&kv);
            let key = kv_key.clone();
            move || store.put(key, b"left".to_vec())
        },
        {
            let store = Arc::clone(&kv);
            let key = kv_key.clone();
            move || store.put(key, b"right".to_vec())
        },
    );
    let kv_expected = kv.get(&kv_key);
    drop(kv);
    assert_key_value_reopens(kv_directory.path(), &kv_key, &kv_expected);

    let set_directory = tempfile::tempdir().expect("create public set history directory");
    let set_key = b"key".to_vec();
    let set = Arc::new(
        DurableKeySetStore::try_init_new(set_directory.path())
            .expect("initialize public set history")
            .into_store(),
    );
    run_pair(
        {
            let store = Arc::clone(&set);
            let key = set_key.clone();
            move || store.append(key, b"left".to_vec())
        },
        {
            let store = Arc::clone(&set);
            let key = set_key.clone();
            move || store.append(key, b"right".to_vec())
        },
    );
    let set_expected = set.get_hashset(&set_key);
    drop(set);
    assert_key_set_reopens(set_directory.path(), &set_key, &set_expected);

    let map_directory = tempfile::tempdir().expect("create public map history directory");
    let map_key = b"key".to_vec();
    let map_search_key = SearchKey::from(1);
    let map = Arc::new(
        DurableKeyMapStore::try_init_new(map_directory.path())
            .expect("initialize public map history")
            .into_store(),
    );
    run_pair(
        {
            let store = Arc::clone(&map);
            let key = map_key.clone();
            let search_key = map_search_key.clone();
            move || store.put(key, search_key, b"left".to_vec())
        },
        {
            let store = Arc::clone(&map);
            let key = map_key.clone();
            let search_key = map_search_key.clone();
            move || store.put(key, search_key, b"right".to_vec())
        },
    );
    let map_expected = map.get_sorted_map(&map_key);
    drop(map);
    assert_key_map_reopens(map_directory.path(), &map_key, &map_expected);
}

fn run_pair<F, G>(left: F, right: G)
where
    F: FnOnce() + Send + 'static,
    G: FnOnce() + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(3));
    let left_barrier = Arc::clone(&barrier);
    let left = std::thread::spawn(move || {
        left_barrier.wait();
        left();
    });
    let right_barrier = Arc::clone(&barrier);
    let right = std::thread::spawn(move || {
        right_barrier.wait();
        right();
    });
    barrier.wait();
    left.join().expect("left public history must join");
    right.join().expect("right public history must join");
}
