//! Key/value truncation and recovery matrix.

use super::support::assert_v1_timestamp_contract;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{DurableStoreOptions, RecoveryStatus, TimestampGranularity};
use std::time::Duration;

#[test]
fn selected_active_tail_is_repaired_through_staging() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let recovery = directory.path().join(".kv.wal.dat");
    let staging = directory.path().join(".kv.wal.dat.next");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), b"accepted".to_vec());
    store.put(b"torn".to_vec(), b"unaccepted".to_vec());
    drop(store);

    let mut interrupted = std::fs::read(&active).unwrap();
    interrupted.pop();
    std::fs::write(&active, &interrupted).unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path())
        .expect("a selected V1 terminal tail must be staged and repaired");

    assert_eq!(outcome.status(), RecoveryStatus::Recovered);
    assert_eq!(outcome.store().get(b"stable"), Some(b"accepted".to_vec()));
    assert_eq!(outcome.store().get(b"torn"), None);
    let repaired = std::fs::read(&active).unwrap();
    assert_ne!(repaired, interrupted);
    assert!(repaired.starts_with(b"PIGWAL\r\n"));
    assert!(!recovery.exists());
    assert!(!staging.exists());
}

#[test]
fn every_key_value_action_cut_recovers_the_public_prefix() {
    for action in ["put", "delete"] {
        let reference = tempfile::tempdir().unwrap();
        let reference_active = reference.path().join("kv.wal.dat");
        let store = DurableKeyValueStore::try_init_new(reference.path())
            .unwrap()
            .into_store();
        store.put(b"stable".to_vec(), b"accepted".to_vec());
        store.put(b"target".to_vec(), b"before".to_vec());
        let accepted = std::fs::read(&reference_active).unwrap();
        match action {
            "put" => store.put(b"target".to_vec(), b"after".to_vec()),
            "delete" => store.remove(b"target"),
            _ => unreachable!(),
        }
        drop(store);
        let complete = std::fs::read(&reference_active).unwrap();
        let final_record = &complete[accepted.len()..];

        for record_cut in 1..final_record.len() {
            let directory = tempfile::tempdir().unwrap();
            let active = directory.path().join("kv.wal.dat");
            let mut interrupted = accepted.clone();
            interrupted.extend_from_slice(&final_record[..record_cut]);
            std::fs::write(&active, interrupted).unwrap();

            let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
            assert_eq!(
                outcome.status(),
                RecoveryStatus::Recovered,
                "action={action} cut={record_cut}"
            );
            assert_eq!(
                outcome.store().get(b"stable"),
                Some(b"accepted".to_vec()),
                "action={action} cut={record_cut}"
            );
            assert_eq!(
                outcome.store().get(b"target"),
                Some(b"before".to_vec()),
                "action={action} cut={record_cut}"
            );
            assert!(outcome.store().contains(b"target"));
        }
    }
}

#[test]
fn key_value_timestamp_history_is_repeatable_across_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let granularity = 1_000_000_u64;
    let options = DurableStoreOptions::default().with_timestamp_granularity(
        TimestampGranularity::try_from(Duration::from_nanos(granularity)).unwrap(),
    );
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"a".to_vec(), b"one".to_vec());
    store.put(b"b".to_vec(), b"discard".to_vec());
    store.remove(b"b");
    store.compute(b"a".to_vec(), |_| b"final".to_vec());
    drop(store);

    assert_v1_timestamp_contract(
        &std::fs::read(directory.path().join("kv.wal.dat")).unwrap(),
        granularity,
    );
    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
        assert_eq!(reopened.status(), RecoveryStatus::Normal);
        assert_eq!(reopened.store().get(b"a"), Some(b"final".to_vec()));
        assert_eq!(reopened.store().get(b"b"), None);
        drop(reopened);
    }
}
