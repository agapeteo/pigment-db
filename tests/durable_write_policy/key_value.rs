//! Key/value durability-policy integration coverage.

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{DurabilityPolicy, DurableStoreOptions};

use super::support::scratch_directory;

#[test]
fn public_physical_key_value_mutations_reopen_as_acknowledged() {
    let directory = scratch_directory("pigment-physical-value-");
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();

    store.try_put(b"plain".to_vec(), b"value".to_vec()).unwrap();
    store
        .try_compute(b"computed".to_vec(), |current| {
            assert!(current.is_none());
            b"result".to_vec()
        })
        .unwrap();
    assert_eq!(
        store.try_increment_or_init(b"number".to_vec(), 4).unwrap(),
        Ok(4)
    );
    assert_eq!(
        store.try_decrement(b"number".to_vec(), 1).unwrap(),
        Some(Ok(3))
    );
    store.try_set_number(b"number".to_vec(), 9).unwrap();
    store
        .try_put(b"removed".to_vec(), b"gone".to_vec())
        .unwrap();
    store.try_remove(b"removed").unwrap();
    drop(store);

    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"plain"), Some(b"value".to_vec()));
    assert_eq!(reopened.get(b"computed"), Some(b"result".to_vec()));
    assert_eq!(reopened.read_number(b"number"), Some(Ok(9)));
    assert!(!reopened.contains(b"removed"));
}
