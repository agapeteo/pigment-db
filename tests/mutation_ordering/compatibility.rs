//! Public mutation compatibility and callback-count tests.

use crate::support::{assert_key_map_reopens, assert_key_set_reopens, assert_key_value_reopens};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn key_value_compute_callback_runs_once_in_ordinary_order() {
    let directory = tempfile::tempdir().expect("create callback key/value directory");
    let key = b"key".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize callback key/value store")
        .into_store();
    store.put(key.clone(), b"ordinary".to_vec());
    let calls = AtomicUsize::new(0);
    store.compute(key.clone(), |prior| {
        calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(prior, Some(b"ordinary".as_slice()));
        b"callback".to_vec()
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let expected = Some(b"callback".to_vec());
    drop(store);
    assert_key_value_reopens(directory.path(), &key, &expected);
}

#[test]
fn set_callbacks_keep_eligible_and_ineligible_counts() {
    let directory = tempfile::tempdir().expect("create callback set directory");
    let key = b"key".to_vec();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize callback set store")
        .into_store();
    store.append(key.clone(), b"ordinary".to_vec());
    let eligible = AtomicUsize::new(0);
    store.compute_if_present(key.clone(), |set| {
        eligible.fetch_add(1, Ordering::SeqCst);
        set.insert(b"callback".to_vec());
    });
    let ineligible = AtomicUsize::new(0);
    store.compute_if_present(b"absent".to_vec(), |_| {
        ineligible.fetch_add(1, Ordering::SeqCst);
    });
    let removal = AtomicUsize::new(0);
    store.remove_from_set_callback(key.clone(), b"ordinary".to_vec(), |_| {
        removal.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(eligible.load(Ordering::SeqCst), 1);
    assert_eq!(ineligible.load(Ordering::SeqCst), 0);
    assert_eq!(
        removal.load(Ordering::SeqCst),
        0,
        "outer key remains present"
    );
    let expected = store.get_hashset(&key);
    drop(store);
    assert_key_set_reopens(directory.path(), &key, &expected);
}

#[test]
fn map_callbacks_keep_eligible_and_ineligible_counts() {
    let directory = tempfile::tempdir().expect("create callback map directory");
    let key = b"key".to_vec();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize callback map store")
        .into_store();
    store.put(key.clone(), SearchKey::from(1), b"ordinary".to_vec());
    let eligible = AtomicUsize::new(0);
    store.compute_if_present(key.clone(), |map| {
        eligible.fetch_add(1, Ordering::SeqCst);
        map.insert(SearchKey::from(2), b"callback".to_vec());
    });
    let ineligible = AtomicUsize::new(0);
    store.compute_if_present(b"absent".to_vec(), |_| {
        ineligible.fetch_add(1, Ordering::SeqCst);
    });
    let removal = AtomicUsize::new(0);
    store.remove_from_sorted_map_callback(key.clone(), SearchKey::from(1), |_| {
        removal.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(eligible.load(Ordering::SeqCst), 1);
    assert_eq!(ineligible.load(Ordering::SeqCst), 0);
    assert_eq!(
        removal.load(Ordering::SeqCst),
        0,
        "outer key remains present"
    );
    let expected = store.get_sorted_map(&key);
    drop(store);
    assert_key_map_reopens(directory.path(), &key, &expected);
}

#[test]
fn public_presence_noop_and_binary_key_edges_remain_compatible() {
    let binary_key = vec![0, 255, 0, 128];

    let kv_dir = tempfile::tempdir().unwrap();
    let kv = DurableKeyValueStore::try_init_new(kv_dir.path())
        .unwrap()
        .into_store();
    kv.remove(&binary_key);
    kv.put(binary_key.clone(), b"created".to_vec());
    kv.remove(&binary_key);
    kv.put(binary_key.clone(), b"recreated".to_vec());
    let kv_expected = kv.get(&binary_key);
    drop(kv);
    assert_key_value_reopens(kv_dir.path(), &binary_key, &kv_expected);

    let set_dir = tempfile::tempdir().unwrap();
    let set = DurableKeySetStore::try_init_new(set_dir.path())
        .unwrap()
        .into_store();
    set.remove_from_set(binary_key.clone(), b"absent".to_vec());
    set.append(binary_key.clone(), b"member".to_vec());
    set.compute(binary_key.clone(), |_| {});
    set.remove_from_set(binary_key.clone(), b"member".to_vec());
    set.compute_if_absent(binary_key.clone(), |values| {
        values.insert(b"recreated".to_vec());
    });
    let set_expected = set.get_hashset(&binary_key);
    drop(set);
    assert_key_set_reopens(set_dir.path(), &binary_key, &set_expected);

    let map_dir = tempfile::tempdir().unwrap();
    let map = DurableKeyMapStore::try_init_new(map_dir.path())
        .unwrap()
        .into_store();
    map.remove_from_sorted_map(binary_key.clone(), SearchKey::from(1));
    map.put(binary_key.clone(), SearchKey::from(1), b"entry".to_vec());
    map.compute(binary_key.clone(), |_| {});
    map.remove_from_sorted_map(binary_key.clone(), SearchKey::from(1));
    map.compute_if_absent(binary_key.clone(), |values| {
        values.insert(SearchKey::from(2), b"recreated".to_vec());
    });
    let map_expected = map.get_sorted_map(&binary_key);
    drop(map);
    assert_key_map_reopens(map_dir.path(), &binary_key, &map_expected);
}
