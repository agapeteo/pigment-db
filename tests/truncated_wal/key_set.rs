//! Key/set truncation and recovery matrix.

use super::support::assert_v1_timestamp_contract;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::{DurableStoreOptions, RecoveryStatus, TimestampGranularity};
use std::fs;
use std::time::Duration;

#[test]
fn fresh_key_set_uses_v1_header() {
    let directory = tempfile::tempdir().unwrap();

    let outcome = DurableKeySetStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Normal);
    drop(outcome);

    let bytes = fs::read(directory.path().join("set.wal.dat")).unwrap();
    assert_eq!(bytes.len(), 40);
    assert_eq!(&bytes[..8], b"PIGWAL\r\n");
    assert_eq!(bytes[12], 2);
    assert_eq!(
        u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        crc32fast::hash(&bytes[..36])
    );
    assert!(!directory.path().join(".set.wal.dat.next").exists());
}

fn assert_set_prefix_for_every_cut(label: &str, accepted: &[u8], final_group: &[u8]) {
    for group_cut in 1..final_group.len() {
        let directory = tempfile::tempdir().unwrap();
        let active = directory.path().join("set.wal.dat");
        let mut interrupted = accepted.to_vec();
        interrupted.extend_from_slice(&final_group[..group_cut]);
        fs::write(active, interrupted).unwrap();

        let outcome = DurableKeySetStore::try_init_new(directory.path()).unwrap();
        assert_eq!(
            outcome.status(),
            RecoveryStatus::Recovered,
            "{label} cut={group_cut}"
        );
        assert!(
            outcome.store().contains_in_set(b"stable", b"accepted"),
            "{label} cut={group_cut}"
        );
        assert!(
            outcome.store().contains_in_set(b"target", b"before"),
            "{label} cut={group_cut}"
        );
        for absent in [
            b"after".as_slice(),
            b"group-a".as_slice(),
            b"group-b".as_slice(),
        ] {
            assert!(
                !outcome.store().contains_in_set(b"target", absent),
                "{label} cut={group_cut}"
            );
        }
    }
}

#[test]
fn every_key_set_action_and_group_cut_preserves_membership_prefix() {
    for action in ["append", "remove", "delete"] {
        let reference = tempfile::tempdir().unwrap();
        let active = reference.path().join("set.wal.dat");
        let store = DurableKeySetStore::try_init_new(reference.path())
            .unwrap()
            .into_store();
        store.append(b"stable".to_vec(), b"accepted".to_vec());
        store.append(b"target".to_vec(), b"before".to_vec());
        let accepted = fs::read(&active).unwrap();
        match action {
            "append" => store.append(b"target".to_vec(), b"after".to_vec()),
            "remove" => store.remove_from_set(b"target".to_vec(), b"before".to_vec()),
            "delete" => store.remove_key(b"target"),
            _ => unreachable!(),
        }
        drop(store);
        let complete = fs::read(active).unwrap();
        assert_set_prefix_for_every_cut(action, &accepted, &complete[accepted.len()..]);
    }

    let reference = tempfile::tempdir().unwrap();
    let active = reference.path().join("set.wal.dat");
    let store = DurableKeySetStore::try_init_new(reference.path())
        .unwrap()
        .into_store();
    store.append(b"stable".to_vec(), b"accepted".to_vec());
    store.append(b"target".to_vec(), b"before".to_vec());
    let accepted = fs::read(&active).unwrap();
    store.compute(b"target".to_vec(), |set| {
        set.remove(b"before".as_slice());
        set.insert(b"group-a".to_vec());
        set.insert(b"group-b".to_vec());
    });
    drop(store);
    let complete = fs::read(active).unwrap();
    assert_set_prefix_for_every_cut("compute-group", &accepted, &complete[accepted.len()..]);
}

#[test]
fn key_set_timestamp_groups_are_repeatable_across_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let granularity = 1_000_000_u64;
    let options = DurableStoreOptions::default().with_timestamp_granularity(
        TimestampGranularity::try_from(Duration::from_nanos(granularity)).unwrap(),
    );
    let store = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.append(b"set".to_vec(), b"discard".to_vec());
    store.remove_from_set(b"set".to_vec(), b"discard".to_vec());
    store.compute(b"set".to_vec(), |set| {
        set.insert(b"alpha".to_vec());
        set.insert(b"beta".to_vec());
    });
    drop(store);

    assert_v1_timestamp_contract(
        &fs::read(directory.path().join("set.wal.dat")).unwrap(),
        granularity,
    );
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory.path()).unwrap();
        assert_eq!(reopened.status(), RecoveryStatus::Normal);
        assert!(reopened.store().contains_in_set(b"set", b"alpha"));
        assert!(reopened.store().contains_in_set(b"set", b"beta"));
        assert!(!reopened.store().contains_in_set(b"set", b"discard"));
        drop(reopened);
    }
}
