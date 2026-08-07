//! Key/sorted-map truncation and recovery matrix.

use super::support::assert_v1_timestamp_contract;
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;
use pigment_db::{DurableStoreOptions, RecoveryStatus, TimestampGranularity};
use std::fs;
use std::time::Duration;

#[test]
fn fresh_key_map_uses_v1_header() {
    let directory = tempfile::tempdir().unwrap();

    let outcome = DurableKeyMapStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Normal);
    drop(outcome);

    let bytes = fs::read(directory.path().join("map.wal.dat")).unwrap();
    assert_eq!(bytes.len(), 40);
    assert_eq!(&bytes[..8], b"PIGWAL\r\n");
    assert_eq!(bytes[12], 3);
    assert_eq!(
        u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        crc32fast::hash(&bytes[..36])
    );
    assert!(!directory.path().join(".map.wal.dat.next").exists());
}

fn assert_map_prefix_for_every_cut(label: &str, accepted: &[u8], final_group: &[u8]) {
    for group_cut in 1..final_group.len() {
        let directory = tempfile::tempdir().unwrap();
        let active = directory.path().join("map.wal.dat");
        let mut interrupted = accepted.to_vec();
        interrupted.extend_from_slice(&final_group[..group_cut]);
        fs::write(active, interrupted).unwrap();

        let outcome = DurableKeyMapStore::try_init_new(directory.path()).unwrap();
        assert_eq!(
            outcome.status(),
            RecoveryStatus::Recovered,
            "{label} cut={group_cut}"
        );
        assert_eq!(
            outcome.store().get_element(b"stable", &SearchKey::from(1)),
            Some(b"accepted".to_vec()),
            "{label} cut={group_cut}"
        );
        let target = outcome.store().get_sorted_map(b"target").unwrap();
        assert_eq!(
            target.keys().cloned().collect::<Vec<_>>(),
            vec![SearchKey::from(1), SearchKey::from(9)],
            "{label} cut={group_cut}"
        );
        assert_eq!(target.get(&SearchKey::from(1)), Some(&b"before".to_vec()));
        assert_eq!(target.get(&SearchKey::from(9)), Some(&b"retained".to_vec()));
        assert!(!target.contains_key(&SearchKey::from(2)));
        assert!(!target.contains_key(&SearchKey::from(3)));
    }
}

#[test]
fn every_key_map_action_and_group_cut_preserves_ordered_prefix() {
    for action in ["put", "remove", "delete"] {
        let reference = tempfile::tempdir().unwrap();
        let active = reference.path().join("map.wal.dat");
        let store = DurableKeyMapStore::try_init_new(reference.path())
            .unwrap()
            .into_store();
        store.put(b"stable".to_vec(), SearchKey::from(1), b"accepted".to_vec());
        store.put(b"target".to_vec(), SearchKey::from(1), b"before".to_vec());
        store.put(b"target".to_vec(), SearchKey::from(9), b"retained".to_vec());
        let accepted = fs::read(&active).unwrap();
        match action {
            "put" => store.put(b"target".to_vec(), SearchKey::from(1), b"after".to_vec()),
            "remove" => {
                store.remove_from_sorted_map(b"target".to_vec(), SearchKey::from(1));
            }
            "delete" => store.remove_key(b"target"),
            _ => unreachable!(),
        }
        drop(store);
        let complete = fs::read(active).unwrap();
        assert_map_prefix_for_every_cut(action, &accepted, &complete[accepted.len()..]);
    }

    let reference = tempfile::tempdir().unwrap();
    let active = reference.path().join("map.wal.dat");
    let store = DurableKeyMapStore::try_init_new(reference.path())
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), SearchKey::from(1), b"accepted".to_vec());
    store.put(b"target".to_vec(), SearchKey::from(1), b"before".to_vec());
    store.put(b"target".to_vec(), SearchKey::from(9), b"retained".to_vec());
    let accepted = fs::read(&active).unwrap();
    store.compute(b"target".to_vec(), |map| {
        map.remove(&SearchKey::from(1));
        map.insert(SearchKey::from(2), b"group-a".to_vec());
        map.insert(SearchKey::from(3), b"group-b".to_vec());
    });
    drop(store);
    let complete = fs::read(active).unwrap();
    assert_map_prefix_for_every_cut("compute-group", &accepted, &complete[accepted.len()..]);
}

#[test]
fn key_map_timestamp_groups_are_repeatable_across_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let granularity = 1_000_000_u64;
    let options = DurableStoreOptions::default().with_timestamp_granularity(
        TimestampGranularity::try_from(Duration::from_nanos(granularity)).unwrap(),
    );
    let store = DurableKeyMapStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"map".to_vec(), SearchKey::from(1), b"discard".to_vec());
    store.remove_from_sorted_map(b"map".to_vec(), SearchKey::from(1));
    store.compute(b"map".to_vec(), |map| {
        map.insert(SearchKey::from(2), b"alpha".to_vec());
        map.insert(SearchKey::from(3), b"beta".to_vec());
    });
    drop(store);

    assert_v1_timestamp_contract(
        &fs::read(directory.path().join("map.wal.dat")).unwrap(),
        granularity,
    );
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path()).unwrap();
        assert_eq!(reopened.status(), RecoveryStatus::Normal);
        let map = reopened.store().get_sorted_map(b"map").unwrap();
        assert_eq!(map.get(&SearchKey::from(2)), Some(&b"alpha".to_vec()));
        assert_eq!(map.get(&SearchKey::from(3)), Some(&b"beta".to_vec()));
        assert!(!map.contains_key(&SearchKey::from(1)));
        drop(reopened);
    }
}
