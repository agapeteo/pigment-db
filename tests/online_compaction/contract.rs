//! Online-compaction contract tests.

use crate::support::{assert_map_reopens, assert_set_reopens, assert_value_reopens};
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
    value.put(b"live".to_vec(), b"value".to_vec());
    let value_expected = value.get(b"live");
    drop(value);
    assert_value_reopens(value_dir.path(), &value_expected);

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
    set.append(b"live".to_vec(), b"member".to_vec());
    let set_expected = set.get_hashset(b"live");
    drop(set);
    assert_set_reopens(set_dir.path(), &set_expected);

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
    map.put(b"live".to_vec(), SearchKey::from(7), b"entry".to_vec());
    let map_expected = map.get_sorted_map(b"live");
    drop(map);
    assert_map_reopens(map_dir.path(), &map_expected);
}

fn assert_outcome(outcome: FamilyCompactionOutcome, family: StoreFamily) {
    assert_eq!(outcome.family(), family);
    assert!(outcome.before_bytes() > 0);
    assert!(outcome.after_bytes() > 0);
    assert_eq!(outcome.concurrent_mutations_replayed(), 0);
    assert_eq!(outcome.cleanup(), CleanupStatus::Complete);
}
