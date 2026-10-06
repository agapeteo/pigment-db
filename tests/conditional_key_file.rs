//! Actual temporary files: replay and maintenance compatibility, not power-loss proof.
use pigment_db::key_value_store::{
    ConditionalAction as Action, ConditionalResult as Result, DurableKeyValueStore,
};
use pigment_db::{DurabilityPolicy, DurableStoreOptions, OnlineCompactionOptions, WalSegmentSize};

#[test]
fn conditional_changes_survive_rotation_online_compaction_and_reopen() {
    for policy in [DurabilityPolicy::Buffered, DurabilityPolicy::Physical] {
        let directory = tempfile::tempdir().unwrap();
        let options = DurableStoreOptions::default()
            .with_durability_policy(policy)
            .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
        let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), None, Action::Put(vec![1; 80]))
                .unwrap(),
            Result::Applied
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), Some(&[1; 80]), Action::Put(vec![2; 80]))
                .unwrap(),
            Result::Applied
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"gone".to_vec(), None, Action::Put(vec![3; 80]))
                .unwrap(),
            Result::Applied
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"gone".to_vec(), Some(&[3; 80]), Action::Delete)
                .unwrap(),
            Result::Applied
        );
        let paths: Vec<_> = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(
            paths
                .iter()
                .any(|p| p.file_name().unwrap().to_string_lossy().contains("segment")),
            "fixture must rotate: {paths:?}"
        );
        store
            .try_compact_online(OnlineCompactionOptions::default())
            .unwrap();
        assert_eq!(
            store
                .try_compare_exchange_one(b"after".to_vec(), None, Action::Put(vec![]))
                .unwrap(),
            Result::Applied
        );
        drop(store);
        let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"key"), Some(vec![2; 80]));
        assert_eq!(reopened.get(b"gone"), None);
        assert_eq!(reopened.get(b"after"), Some(vec![]));
        assert_eq!(reopened.size(), 2);
    }
}

#[test]
fn equal_absent_and_conflict_actions_leave_real_wal_bytes_unchanged() {
    for policy in [DurabilityPolicy::Buffered, DurabilityPolicy::Physical] {
        let directory = tempfile::tempdir().unwrap();
        let options = DurableStoreOptions::default().with_durability_policy(policy);
        let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        store
            .try_compare_exchange_one(b"key".to_vec(), None, Action::Put(b"value".to_vec()))
            .unwrap();
        let before = std::fs::read(directory.path().join("kv.wal.dat")).unwrap();
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), Some(b"value"), Action::Keep)
                .unwrap(),
            Result::Unchanged
        );
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"key".to_vec(),
                    Some(b"value"),
                    Action::Put(b"value".to_vec())
                )
                .unwrap(),
            Result::Unchanged
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"missing".to_vec(), None, Action::Delete)
                .unwrap(),
            Result::Unchanged
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), None, Action::Delete)
                .unwrap(),
            Result::Conflict
        );
        assert_eq!(
            std::fs::read(directory.path().join("kv.wal.dat")).unwrap(),
            before
        );
    }
}

#[test]
fn every_synthetic_torn_single_event_preserves_the_previous_prefix() {
    for delete in [false, true] {
        let source = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(source.path())
            .unwrap()
            .into_store();
        store
            .try_compare_exchange_one(b"key".to_vec(), None, Action::Put(b"old".to_vec()))
            .unwrap();
        let path = source.path().join("kv.wal.dat");
        let prefix = std::fs::metadata(&path).unwrap().len() as usize;
        store
            .try_compare_exchange_one(
                b"key".to_vec(),
                Some(b"old"),
                if delete {
                    Action::Delete
                } else {
                    Action::Put(b"new".to_vec())
                },
            )
            .unwrap();
        drop(store);
        let bytes = std::fs::read(path).unwrap();
        for cut in prefix..=bytes.len() {
            let directory = tempfile::tempdir().unwrap();
            std::fs::write(directory.path().join("kv.wal.dat"), &bytes[..cut]).unwrap();
            let reopened = DurableKeyValueStore::try_init_new(directory.path())
                .unwrap()
                .into_store();
            let expected = if cut < bytes.len() {
                Some(b"old".to_vec())
            } else if delete {
                None
            } else {
                Some(b"new".to_vec())
            };
            assert_eq!(reopened.get(b"key"), expected, "delete={delete}, cut={cut}");
        }
        eprintln!(
            "single-event synthetic cuts: delete={delete}, cases={}",
            bytes.len() - prefix + 1
        );
    }
}
