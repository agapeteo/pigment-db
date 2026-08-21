//! Online-compaction contract tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{
    CleanupStatus, CompactionError, FamilyCompactionOutcome, OnlineCompactionOptions, StoreFamily,
};

#[test]
fn public_online_options_and_all_file_family_entry_points_execute() {
    assert_eq!(
        OnlineCompactionOptions::default().max_delta_bytes(),
        8 * 1024 * 1024
    );
    let _: fn(
        &DurableKeyValueStore<std::fs::File>,
        OnlineCompactionOptions,
    ) -> Result<FamilyCompactionOutcome, CompactionError> =
        DurableKeyValueStore::<std::fs::File>::try_compact_online;
    let _: fn(
        &DurableKeySetStore<std::fs::File>,
        OnlineCompactionOptions,
    ) -> Result<FamilyCompactionOutcome, CompactionError> =
        DurableKeySetStore::<std::fs::File>::try_compact_online;
    let _: fn(
        &DurableKeyMapStore<std::fs::File>,
        OnlineCompactionOptions,
    ) -> Result<FamilyCompactionOutcome, CompactionError> =
        DurableKeyMapStore::<std::fs::File>::try_compact_online;

    let value_dir = tempfile::tempdir().unwrap();
    let value = DurableKeyValueStore::try_init_new(value_dir.path())
        .unwrap()
        .into_store();
    value.put(b"key".to_vec(), b"value".to_vec());
    assert_outcome(
        value
            .try_compact_online(OnlineCompactionOptions::default())
            .unwrap(),
        StoreFamily::KeyValue,
    );
    assert_eq!(value.get(b"key"), Some(b"value".to_vec()));

    let set_dir = tempfile::tempdir().unwrap();
    let set = DurableKeySetStore::try_init_new(set_dir.path())
        .unwrap()
        .into_store();
    set.append(b"group".to_vec(), b"member".to_vec());
    assert_outcome(
        set.try_compact_online(OnlineCompactionOptions::default())
            .unwrap(),
        StoreFamily::KeySet,
    );
    assert!(set
        .get_hashset(b"group")
        .unwrap()
        .contains(b"member".as_slice()));

    let map_dir = tempfile::tempdir().unwrap();
    let map = DurableKeyMapStore::try_init_new(map_dir.path())
        .unwrap()
        .into_store();
    map.put(b"book".to_vec(), SearchKey::from(7), b"entry".to_vec());
    assert_outcome(
        map.try_compact_online(OnlineCompactionOptions::default())
            .unwrap(),
        StoreFamily::KeyMap,
    );
    assert_eq!(
        map.get_element(b"book", &SearchKey::from(7)),
        Some(b"entry".to_vec())
    );
}

fn assert_outcome(outcome: FamilyCompactionOutcome, family: StoreFamily) {
    assert_eq!(outcome.family(), family);
    assert!(outcome.before_bytes() > 0);
    assert!(outcome.after_bytes() > 0);
    assert_eq!(outcome.concurrent_mutations_replayed(), 0);
    assert_eq!(outcome.cleanup(), CleanupStatus::Complete);
}
