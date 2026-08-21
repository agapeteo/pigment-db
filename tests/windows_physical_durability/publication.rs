//! Windows write-through publication tests.

#![cfg(target_os = "windows")]

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{DurabilityPolicy, DurableStoreOptions, WalSegmentSize};

#[test]
fn fresh_physical_publication_exposes_only_canonical_files_for_every_family() {
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let value_directory = tempfile::tempdir().unwrap();
    drop(
        DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
            .unwrap()
            .into_store(),
    );
    assert_only_active(value_directory.path(), "kv.wal.dat");

    let set_directory = tempfile::tempdir().unwrap();
    drop(
        DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
            .unwrap()
            .into_store(),
    );
    assert_only_active(set_directory.path(), "set.wal.dat");

    let map_directory = tempfile::tempdir().unwrap();
    drop(
        DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
            .unwrap()
            .into_store(),
    );
    assert_only_active(map_directory.path(), "map.wal.dat");
}

fn assert_only_active(directory: &std::path::Path, active_name: &str) {
    let names = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(names, [std::ffi::OsString::from(active_name)]);
}

#[test]
fn physical_rotation_publishes_sealed_and_next_active_for_every_family() {
    let options = DurableStoreOptions::default()
        .with_durability_policy(DurabilityPolicy::Physical)
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());

    let value_directory = tempfile::tempdir().unwrap();
    let values = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    values.put(b"one".to_vec(), vec![1; 100]);
    values.put(b"two".to_vec(), vec![2; 100]);
    drop(values);
    assert_rotated(value_directory.path(), "kv.wal.dat");
    let values = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(values.get(b"two"), Some(vec![2; 100]));

    let set_directory = tempfile::tempdir().unwrap();
    let sets = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    sets.append(b"one".to_vec(), vec![1; 100]);
    sets.append(b"two".to_vec(), vec![2; 100]);
    drop(sets);
    assert_rotated(set_directory.path(), "set.wal.dat");
    let sets = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    assert!(sets.contains_in_set(b"two", &vec![2; 100]));

    let map_directory = tempfile::tempdir().unwrap();
    let maps = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    maps.put(b"one".to_vec(), SearchKey::from(1), vec![1; 100]);
    maps.put(b"two".to_vec(), SearchKey::from(2), vec![2; 100]);
    drop(maps);
    assert_rotated(map_directory.path(), "map.wal.dat");
    let maps = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(
        maps.get_element(b"two", &SearchKey::from(2)),
        Some(vec![2; 100])
    );
}

fn assert_rotated(directory: &std::path::Path, active_name: &str) {
    assert!(directory.join(active_name).is_file());
    assert!(directory
        .join(format!("{active_name}.segment-{:020}", 0))
        .is_file());
}
