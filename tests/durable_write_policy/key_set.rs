//! Key/set durability-policy integration coverage.

use std::cell::Cell;

use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::{DurabilityPolicy, DurableStoreOptions};

use super::support::scratch_directory;

#[test]
fn public_physical_key_set_mutations_reopen_as_acknowledged() {
    let directory = scratch_directory("pigment-physical-set-");
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();

    store.try_append(b"key".to_vec(), b"one".to_vec()).unwrap();
    store
        .try_compute(b"key".to_vec(), |set| {
            set.insert(b"two".to_vec());
            set.insert(b"three".to_vec());
        })
        .unwrap();
    store
        .try_remove_from_set(b"key".to_vec(), b"one".to_vec())
        .unwrap();
    let callbacks = Cell::new(0);
    store
        .try_remove_from_set_callback(b"key".to_vec(), b"two".to_vec(), |_| {
            callbacks.set(callbacks.get() + 1);
        })
        .unwrap();
    assert_eq!(callbacks.get(), 0);
    drop(store);

    let reopened = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert!(!reopened.contains_in_set(b"key", b"one"));
    assert!(!reopened.contains_in_set(b"key", b"two"));
    assert!(reopened.contains_in_set(b"key", b"three"));
}
