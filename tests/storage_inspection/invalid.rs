//! Invalid storage-inspection artifact tests.

use std::error::Error as _;

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{inspect_storage, CompactionError};

use crate::maintenance_support::namespace_snapshot;

fn current_value_directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"key".to_vec(), b"value".to_vec());
    drop(store);
    directory
}

#[test]
fn public_errors_distinguish_legacy_invalid_unexpected_ambiguous_and_io_without_mutation() {
    let legacy = tempfile::tempdir().unwrap();
    let legacy_path = legacy.path().join("kv.wal.dat");
    let legacy_bytes = include_bytes!("../fixtures/legacy/kv.wal.dat");
    std::fs::write(&legacy_path, legacy_bytes).unwrap();
    let legacy_before = namespace_snapshot(legacy.path()).unwrap();
    let legacy_error = inspect_storage(legacy.path()).unwrap_err();
    assert!(matches!(
        legacy_error,
        CompactionError::MigrationRequired { ref path } if path == &legacy_path
    ));
    assert!(legacy_error.to_string().contains("pigment-db-migrate"));
    assert_eq!(std::fs::read(&legacy_path).unwrap(), legacy_bytes);
    assert_eq!(namespace_snapshot(legacy.path()).unwrap(), legacy_before);

    let corrupt = current_value_directory();
    let corrupt_path = corrupt.path().join("kv.wal.dat");
    let mut corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
    corrupt_bytes[0] ^= 0xff;
    std::fs::write(&corrupt_path, &corrupt_bytes).unwrap();
    let corrupt_before = namespace_snapshot(corrupt.path()).unwrap();
    assert!(matches!(
        inspect_storage(corrupt.path()),
        Err(CompactionError::InvalidArtifact { path }) if path == corrupt_path
    ));
    assert_eq!(std::fs::read(&corrupt_path).unwrap(), corrupt_bytes);
    assert_eq!(namespace_snapshot(corrupt.path()).unwrap(), corrupt_before);

    let unexpected = current_value_directory();
    let unexpected_path = unexpected.path().join("notes.txt");
    std::fs::write(&unexpected_path, b"caller data").unwrap();
    let unexpected_before = namespace_snapshot(unexpected.path()).unwrap();
    assert!(matches!(
        inspect_storage(unexpected.path()),
        Err(CompactionError::InvalidArtifact { path }) if path == unexpected_path
    ));
    assert_eq!(std::fs::read(&unexpected_path).unwrap(), b"caller data");
    assert_eq!(
        namespace_snapshot(unexpected.path()).unwrap(),
        unexpected_before
    );

    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    let store = DurableKeyValueStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();
    store.put(b"old".to_vec(), b"authority".to_vec());
    drop(store);
    let previous = root.path().join(".store.pigment-compact.previous");
    std::fs::create_dir(&previous).unwrap();
    let candidate = DurableKeyValueStore::try_init_new(&previous)
        .unwrap()
        .into_store();
    candidate.put(b"new".to_vec(), b"candidate".to_vec());
    drop(candidate);
    let old_bytes = std::fs::read(store_dir.join("kv.wal.dat")).unwrap();
    let new_bytes = std::fs::read(previous.join("kv.wal.dat")).unwrap();
    let ambiguous_before = namespace_snapshot(root.path()).unwrap();
    let ambiguous = inspect_storage(&store_dir).unwrap_err();
    assert!(matches!(
        ambiguous,
        CompactionError::AuthorityUndetermined { ref paths }
            if paths == &vec![store_dir.clone(), previous.clone()]
    ));
    assert_eq!(
        std::fs::read(store_dir.join("kv.wal.dat")).unwrap(),
        old_bytes
    );
    assert_eq!(
        std::fs::read(previous.join("kv.wal.dat")).unwrap(),
        new_bytes
    );
    assert_eq!(namespace_snapshot(root.path()).unwrap(), ambiguous_before);

    let missing = root.path().join("missing");
    let io_error = inspect_storage(&missing).unwrap_err();
    assert!(matches!(
        io_error,
        CompactionError::Io { ref path, .. } if path == &missing
    ));
    assert!(io_error.source().is_some());
}
