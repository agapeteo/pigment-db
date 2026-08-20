//! Closed-compaction contract tests.

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    compact_directory_in_place, CleanupStatus, ClosedCompactionOptions, CompactionError,
    CompactionOperation, DurabilityPolicy, DurableStoreOptions, StoreFamily, WalSegmentSize,
};

#[test]
fn public_closed_compaction_defaults_builds_and_compacts_buffered_storage() {
    assert_eq!(
        ClosedCompactionOptions::default().durability_policy(),
        DurabilityPolicy::Buffered
    );
    assert_eq!(
        ClosedCompactionOptions::default()
            .with_durability_policy(DurabilityPolicy::Physical)
            .durability_policy(),
        DurabilityPolicy::Physical
    );

    let empty = tempfile::tempdir().unwrap();
    assert!(
        compact_directory_in_place(empty.path(), ClosedCompactionOptions::default())
            .unwrap()
            .families()
            .is_empty()
    );

    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"alpha".to_vec(), b"one".to_vec());
    store.put(b"beta".to_vec(), b"two".to_vec());
    drop(store);

    let outcome =
        compact_directory_in_place(directory.path(), ClosedCompactionOptions::default()).unwrap();
    let family = &outcome.families()[0];
    assert_eq!(family.family(), StoreFamily::KeyValue);
    assert_eq!(family.sealed_segments_removed(), 1);
    assert_eq!(family.concurrent_mutations_replayed(), 0);
    assert_eq!(family.cleanup(), CleanupStatus::Complete);
    assert!(family.after_bytes() < family.before_bytes());

    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
        assert_eq!(reopened.get(b"beta"), Some(b"two".to_vec()));
    }
}

#[test]
fn public_closed_compaction_rejects_a_same_process_open_store_without_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"alpha".to_vec(), b"one".to_vec());
    let before = crate::support::namespace_snapshot(directory.path()).unwrap();

    let error = compact_directory_in_place(directory.path(), ClosedCompactionOptions::default())
        .unwrap_err();

    assert!(matches!(error, CompactionError::FailedClosed { .. }));
    assert_eq!(
        crate::support::namespace_snapshot(directory.path()).unwrap(),
        before
    );
}

#[test]
fn public_closed_compaction_io_error_names_the_operation_and_exact_path() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("missing-store");

    let error =
        compact_directory_in_place(&missing, ClosedCompactionOptions::default()).unwrap_err();

    assert!(matches!(
        error,
        CompactionError::Io {
            operation: CompactionOperation::Inspect,
            ref path,
            ..
        } if path == &missing
    ));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn public_physical_closed_compaction_publishes_and_reopens_on_supported_targets() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"alpha".to_vec(), b"one".to_vec());
    store.put(b"beta".to_vec(), b"two".to_vec());
    drop(store);

    let outcome = compact_directory_in_place(
        directory.path(),
        ClosedCompactionOptions::default().with_durability_policy(DurabilityPolicy::Physical),
    )
    .unwrap();

    assert_eq!(outcome.families()[0].cleanup(), CleanupStatus::Complete);
    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
    assert_eq!(reopened.get(b"beta"), Some(b"two".to_vec()));
}
