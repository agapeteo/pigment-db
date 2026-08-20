//! Closed-compaction contract tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
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

fn assert_mixed_directory_compaction(policy: DurabilityPolicy) {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let values = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    values.put(b"alpha".to_vec(), b"one".to_vec());
    values.put(b"beta".to_vec(), b"two".to_vec());
    drop(values);
    let sets = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    sets.append(b"group".to_vec(), b"red".to_vec());
    sets.append(b"group".to_vec(), b"blue".to_vec());
    drop(sets);
    let maps = DurableKeyMapStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    maps.put(b"book".to_vec(), SearchKey::from(1), b"one".to_vec());
    maps.put(b"book".to_vec(), SearchKey::from(2), b"two".to_vec());
    drop(maps);

    let outcome = compact_directory_in_place(
        directory.path(),
        ClosedCompactionOptions::default().with_durability_policy(policy),
    )
    .unwrap();

    assert_eq!(
        outcome
            .families()
            .iter()
            .map(|family| family.family())
            .collect::<Vec<_>>(),
        [
            StoreFamily::KeyValue,
            StoreFamily::KeySet,
            StoreFamily::KeyMap
        ]
    );
    assert!(outcome
        .families()
        .iter()
        .all(|family| family.sealed_segments_removed() >= 1
            && family.cleanup() == CleanupStatus::Complete));
    let snapshot = crate::support::namespace_snapshot(directory.path()).unwrap();
    assert_eq!(snapshot.len(), 3);
    for _ in 0..3 {
        let values = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        assert_eq!(values.get(b"alpha"), Some(b"one".to_vec()));
        drop(values);
        let sets = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        assert!(sets
            .get_hashset(b"group")
            .unwrap()
            .contains(b"blue".as_slice()));
        drop(sets);
        let maps = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        assert_eq!(
            maps.get_element(b"book", &SearchKey::from(2)),
            Some(b"two".to_vec())
        );
    }
}

#[test]
fn public_buffered_closed_compaction_handles_a_mixed_directory() {
    assert_mixed_directory_compaction(DurabilityPolicy::Buffered);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn public_physical_closed_compaction_handles_a_mixed_directory() {
    assert_mixed_directory_compaction(DurabilityPolicy::Physical);
}
