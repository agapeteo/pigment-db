//! Windows physical-durability contract tests.

#![cfg(target_os = "windows")]

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    CompactionError, CompactionOperation, DurabilityPolicy, DurableStoreOptions,
    OnlineCompactionOptions,
};
use std::os::windows::fs::OpenOptionsExt;
use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

#[test]
fn external_non_delete_sharing_handle_blocks_cutover_without_fallback() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), b"authority".to_vec());
    let active = directory.path().join("kv.wal.dat");
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&active)
        .unwrap();

    let error = store
        .try_compact_online(OnlineCompactionOptions::default())
        .expect_err("an external handle denying delete-sharing must block namespace movement");

    assert!(matches!(
        error,
        CompactionError::Io {
            operation: CompactionOperation::PublishPrevious,
            ref path,
            ref source,
        } if path.ends_with("kv.wal.dat") && source.raw_os_error() == Some(32)
    ));
    assert_eq!(store.get(b"stable"), Some(b"authority".to_vec()));
    assert!(store
        .try_put(b"blocked".to_vec(), b"not-written".to_vec())
        .is_err());
    drop(held);
    drop(store);

    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("reopen must resolve the preserved pre-publication authority");
    assert_eq!(reopened.store().get(b"stable"), Some(b"authority".to_vec()));
    reopened
        .store()
        .try_put(b"after-reopen".to_vec(), b"accepted".to_vec())
        .unwrap();
}
