use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::{RecoveryError, RecoveryStatus};

use super::support::copy_fixture;

#[test]
fn frozen_sorted_map_fixture_has_legacy_recovery_parity() {
    for case in ["active", "legacy", "empty-active-with-legacy"] {
        let directory = tempfile::tempdir().unwrap();
        match case {
            "active" => {
                copy_fixture("map.wal.dat", directory.path(), "map.wal.dat");
            }
            "legacy" => {
                copy_fixture("map.wal.dat", directory.path(), ".map.wal.dat");
            }
            "empty-active-with-legacy" => {
                copy_fixture("map.wal.dat", directory.path(), ".map.wal.dat");
                std::fs::File::create(directory.path().join("map.wal.dat")).unwrap();
            }
            _ => unreachable!(),
        }

        let names = ["map.wal.dat", ".map.wal.dat", ".map.wal.dat.next"];
        let before = super::support::snapshot_files(directory.path(), &names);
        assert!(matches!(
            DurableKeyMapStore::try_init_new(directory.path()),
            Err(RecoveryError::MigrationRequired { .. })
        ));
        assert_eq!(
            super::support::snapshot_files(directory.path(), &names),
            before
        );
    }
}

#[test]
fn sorted_map_ignores_interrupted_staging_and_preserves_ordered_entries() {
    for complete_staging in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"book".to_vec(), 1usize.into(), b"a".to_vec());
        store.put(b"book".to_vec(), 2usize.into(), b"b".to_vec());
        store.put(b"book".to_vec(), 2usize.into(), b"b2".to_vec());
        store.remove_from_sorted_map(b"book".to_vec(), 1usize.into());
        store.put(b"other".to_vec(), 3usize.into(), b"c".to_vec());
        drop(store);

        let staging = directory.path().join(".map.wal.dat.next");
        if complete_staging {
            let other = tempfile::tempdir().unwrap();
            let wrong = DurableKeyMapStore::try_init_new(other.path())
                .unwrap()
                .into_store();
            wrong.put(b"stage-only".to_vec(), 9usize.into(), b"wrong".to_vec());
            drop(wrong);
            std::fs::copy(other.path().join("map.wal.dat"), &staging).unwrap();
        } else {
            let active = std::fs::read(directory.path().join("map.wal.dat")).unwrap();
            std::fs::write(&staging, &active[..active.len() - 1]).unwrap();
        }

        let outcome = DurableKeyMapStore::try_init_new(directory.path()).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Recovered);
        assert_eq!(
            outcome.store().get_element(b"book", &2usize.into()),
            Some(b"b2".to_vec())
        );
        assert_eq!(outcome.store().get_element(b"book", &1usize.into()), None);
        assert_eq!(
            outcome.store().get_element(b"other", &3usize.into()),
            Some(b"c".to_vec())
        );
        assert!(!outcome.store().contains_key(b"stage-only"));
        assert!(!staging.exists());
    }
}
