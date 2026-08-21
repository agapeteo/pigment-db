//! Windows buffered-durability compatibility tests.

#![cfg(target_os = "windows")]

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{DurabilityPolicy, DurableStoreOptions, OnlineCompactionOptions, WalSegmentSize};

#[test]
fn buffered_all_family_bytes_results_rotation_and_online_compaction_remain_compatible() {
    let options = DurableStoreOptions::default()
        .with_durability_policy(DurabilityPolicy::Buffered)
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());

    let value_directory = tempfile::tempdir().unwrap();
    let values = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    values.put(b"one".to_vec(), vec![1; 100]);
    values.put(b"two".to_vec(), vec![2; 100]);
    values
        .try_compact_online(OnlineCompactionOptions::default())
        .unwrap();
    drop(values);
    let values = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(values.get(b"one"), Some(vec![1; 100]));
    assert_eq!(values.get(b"two"), Some(vec![2; 100]));

    let set_directory = tempfile::tempdir().unwrap();
    let sets = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    sets.append(b"group".to_vec(), b"one".to_vec());
    sets.append(b"group".to_vec(), b"two".to_vec());
    drop(sets);
    let sets = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    assert!(sets.contains_in_set(b"group", &b"one".to_vec()));
    assert!(sets.contains_in_set(b"group", &b"two".to_vec()));

    let map_directory = tempfile::tempdir().unwrap();
    let maps = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    maps.put(b"book".to_vec(), SearchKey::from(1), b"one".to_vec());
    maps.put(b"book".to_vec(), SearchKey::from(2), b"two".to_vec());
    drop(maps);
    let maps = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(
        maps.get_element(b"book", &SearchKey::from(1)),
        Some(b"one".to_vec())
    );
    assert_eq!(
        maps.get_element(b"book", &SearchKey::from(2)),
        Some(b"two".to_vec())
    );
}
