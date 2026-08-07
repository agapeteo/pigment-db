//! Key/sorted-map durability-policy integration coverage.

use std::cell::Cell;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;
use pigment_db::{DurabilityPolicy, DurableStoreOptions};

use super::support::scratch_directory;

#[test]
fn public_physical_key_map_mutations_reopen_as_acknowledged() {
    let directory = scratch_directory("pigment-physical-map-");
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeyMapStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();

    store
        .try_put(b"key".to_vec(), SearchKey::from(1_usize), b"one".to_vec())
        .unwrap();
    store
        .try_compute(b"key".to_vec(), |map| {
            map.insert(SearchKey::from(2_usize), b"two".to_vec());
        })
        .unwrap();
    assert_eq!(
        store
            .try_remove_from_sorted_map(b"key".to_vec(), SearchKey::from(1_usize))
            .unwrap(),
        Some(b"one".to_vec())
    );
    store
        .try_append_ordered_element(b"ordered".to_vec(), b"first".to_vec())
        .unwrap();
    let callbacks = Cell::new(0);
    store
        .try_remove_from_sorted_map_callback(b"key".to_vec(), SearchKey::from(2_usize), |_| {
            callbacks.set(callbacks.get() + 1)
        })
        .unwrap();
    assert_eq!(callbacks.get(), 1);
    drop(store);

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert!(!reopened.contains_key(b"key"));
    assert_eq!(
        reopened.get_element(b"ordered", &SearchKey::from(0_usize)),
        Some(b"first".to_vec())
    );
}
