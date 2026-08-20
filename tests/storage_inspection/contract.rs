//! Directory storage-inspection contract tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{inspect_storage, DurableStoreOptions, StoreFamily, WalSegmentSize};
use std::io::Write as _;

use crate::maintenance_support::namespace_snapshot;

#[test]
fn public_inspection_and_open_store_methods_report_exact_deterministic_totals() {
    let empty = tempfile::tempdir().unwrap();
    let empty_before = namespace_snapshot(empty.path()).unwrap();
    let empty_stats = inspect_storage(empty.path()).unwrap();
    assert!(empty_stats.families().is_empty());
    assert_eq!(empty_stats.total_bytes(), 0);
    assert_eq!(namespace_snapshot(empty.path()).unwrap(), empty_before);

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

    let before = namespace_snapshot(directory.path()).unwrap();
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
    assert_eq!(namespace_snapshot(directory.path()).unwrap(), before);
}

#[test]
fn public_inspection_measures_a_recoverable_tail_without_repair() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), b"value".to_vec());
    drop(store);
    let path = directory.path().join("kv.wal.dat");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[0xa7]).unwrap();
    file.flush().unwrap();
    drop(file);
    let before = std::fs::read(&path).unwrap();
    let namespace_before = namespace_snapshot(directory.path()).unwrap();

    let stats = inspect_storage(directory.path()).unwrap();

    assert_eq!(stats.families().len(), 1);
    assert_eq!(stats.total_bytes(), u64::try_from(before.len()).unwrap());
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(
        namespace_snapshot(directory.path()).unwrap(),
        namespace_before
    );
}
