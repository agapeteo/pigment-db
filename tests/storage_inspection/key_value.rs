//! Key/value storage-inspection tests.

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{DurableStoreOptions, StoreFamily, WalSegmentSize};

use crate::maintenance_support::namespace_snapshot;

#[test]
fn open_key_value_stats_match_current_segment_files() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"one".to_vec(), b"1".to_vec());
    store.put(b"two".to_vec(), b"2".to_vec());

    let before = namespace_snapshot(directory.path()).unwrap();
    let stats = store.storage_stats().unwrap();

    assert_eq!(stats.family(), StoreFamily::KeyValue);
    assert_eq!(
        stats.active_bytes(),
        std::fs::metadata(directory.path().join("kv.wal.dat"))
            .unwrap()
            .len()
    );
    assert!(stats.sealed_segment_count() >= 1);
    assert_eq!(
        stats.total_bytes(),
        stats.active_bytes() + stats.sealed_segment_bytes()
    );
    assert_eq!(namespace_snapshot(directory.path()).unwrap(), before);
}
