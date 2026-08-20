//! Directory storage-inspection contract tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{inspect_storage, DurableStoreOptions, StoreFamily, WalSegmentSize};

#[test]
fn public_inspection_and_open_store_methods_report_exact_deterministic_totals() {
    let empty = tempfile::tempdir().unwrap();
    let empty_stats = inspect_storage(empty.path()).unwrap();
    assert!(empty_stats.families().is_empty());
    assert_eq!(empty_stats.total_bytes(), 0);

    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let value = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    value.put(b"alpha".to_vec(), b"one".to_vec());
    value.put(b"beta".to_vec(), b"two".to_vec());
    let set = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    set.append(b"group".to_vec(), b"red".to_vec());
    set.append(b"group".to_vec(), b"blue".to_vec());
    let map = DurableKeyMapStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    map.put(b"book".to_vec(), 1_usize.into(), b"one".to_vec());
    map.put(b"book".to_vec(), 2_usize.into(), b"two".to_vec());

    let directory_stats = inspect_storage(directory.path()).unwrap();
    assert_eq!(
        directory_stats
            .families()
            .iter()
            .map(|family| family.family())
            .collect::<Vec<_>>(),
        [
            StoreFamily::KeyValue,
            StoreFamily::KeySet,
            StoreFamily::KeyMap,
        ]
    );
    assert_eq!(
        directory_stats.total_bytes(),
        directory_stats
            .families()
            .iter()
            .map(|family| family.total_bytes())
            .sum()
    );

    for (actual, family) in [
        (value.storage_stats().unwrap(), StoreFamily::KeyValue),
        (set.storage_stats().unwrap(), StoreFamily::KeySet),
        (map.storage_stats().unwrap(), StoreFamily::KeyMap),
    ] {
        assert_eq!(actual.family(), family);
        assert_eq!(
            actual.total_bytes(),
            actual.active_bytes() + actual.sealed_segment_bytes()
        );
        assert!(actual.sealed_segment_count() >= 1);
    }
}
