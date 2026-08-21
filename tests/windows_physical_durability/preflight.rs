//! Windows physical-durability preflight tests.

#![cfg(target_os = "windows")]

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{DurabilityPolicy, DurableStoreOptions};

#[test]
fn physical_construction_preflights_actual_directory_content_without_residue() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);

    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();

    assert!(store.get(b"absent").is_none());
    drop(store);
    let mut names = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [std::ffi::OsString::from("kv.wal.dat")]);
}

#[test]
fn physical_open_preflight_preserves_existing_authority_byte_for_byte() {
    let directory = tempfile::tempdir().unwrap();
    let buffered = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    buffered.put(b"key".to_vec(), b"value".to_vec());
    drop(buffered);
    let active = directory.path().join("kv.wal.dat");
    let before = std::fs::read(&active).unwrap();
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);

    let physical = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();

    assert_eq!(physical.get(b"key"), Some(b"value".to_vec()));
    drop(physical);
    assert_eq!(std::fs::read(&active).unwrap(), before);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
