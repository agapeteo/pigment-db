use std::collections::HashSet;

use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::{RecoveryError, RecoveryStatus};

use super::support::copy_fixture;

#[test]
fn frozen_set_fixture_has_legacy_recovery_parity() {
    for case in ["active", "legacy", "empty-active-with-legacy"] {
        let directory = tempfile::tempdir().unwrap();
        match case {
            "active" => {
                copy_fixture("set.wal.dat", directory.path(), "set.wal.dat");
            }
            "legacy" => {
                copy_fixture("set.wal.dat", directory.path(), ".set.wal.dat");
            }
            "empty-active-with-legacy" => {
                copy_fixture("set.wal.dat", directory.path(), ".set.wal.dat");
                std::fs::File::create(directory.path().join("set.wal.dat")).unwrap();
            }
            _ => unreachable!(),
        }

        let names = ["set.wal.dat", ".set.wal.dat", ".set.wal.dat.next"];
        let before = super::support::snapshot_files(directory.path(), &names);
        assert!(matches!(
            DurableKeySetStore::try_init_new(directory.path()),
            Err(RecoveryError::MigrationRequired { .. })
        ));
        assert_eq!(
            super::support::snapshot_files(directory.path(), &names),
            before
        );
    }
}

#[test]
fn set_store_ignores_interrupted_staging_and_preserves_membership() {
    for complete_staging in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"group".to_vec(), b"red".to_vec());
        store.append(b"group".to_vec(), b"green".to_vec());
        store.append(b"group".to_vec(), b"red".to_vec());
        store.remove_from_set(b"group".to_vec(), b"green".to_vec());
        store.append(b"other".to_vec(), b"blue".to_vec());
        drop(store);

        let staging = directory.path().join(".set.wal.dat.next");
        if complete_staging {
            let other = tempfile::tempdir().unwrap();
            let wrong = DurableKeySetStore::try_init_new(other.path())
                .unwrap()
                .into_store();
            wrong.append(b"stage-only".to_vec(), b"wrong".to_vec());
            drop(wrong);
            std::fs::copy(other.path().join("set.wal.dat"), &staging).unwrap();
        } else {
            let active = std::fs::read(directory.path().join("set.wal.dat")).unwrap();
            std::fs::write(&staging, &active[..active.len() - 1]).unwrap();
        }

        let outcome = DurableKeySetStore::try_init_new(directory.path()).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Recovered);
        assert_eq!(
            outcome.store().get_hashset(b"group"),
            Some(HashSet::from([b"red".to_vec()]))
        );
        assert_eq!(
            outcome.store().get_hashset(b"other"),
            Some(HashSet::from([b"blue".to_vec()]))
        );
        assert!(!outcome.store().contains_key(b"stage-only"));
        assert!(!staging.exists());
    }
}
