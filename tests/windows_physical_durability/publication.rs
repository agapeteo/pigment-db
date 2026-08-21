//! Windows write-through publication tests.

#![cfg(target_os = "windows")]

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{DurabilityPolicy, DurableStoreOptions};

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
