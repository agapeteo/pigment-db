//! Windows write-through publication tests.

#![cfg(target_os = "windows")]

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{compact_directory_in_place, CleanupStatus, ClosedCompactionOptions};
use pigment_db::{DurabilityPolicy, DurableStoreOptions, RecoveryStatus, WalSegmentSize};

#[test]
fn fresh_physical_publication_exposes_only_canonical_files_for_every_family() {
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let value_directory = tempfile::tempdir().unwrap();
    drop(
        DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
            .unwrap()
            .into_store(),
    );
    assert_only_active(value_directory.path(), "kv.wal.dat");

    let set_directory = tempfile::tempdir().unwrap();
    drop(
        DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
            .unwrap()
            .into_store(),
    );
    assert_only_active(set_directory.path(), "set.wal.dat");

    let map_directory = tempfile::tempdir().unwrap();
    drop(
        DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
            .unwrap()
            .into_store(),
    );
    assert_only_active(map_directory.path(), "map.wal.dat");
}

fn assert_only_active(directory: &std::path::Path, active_name: &str) {
    let names = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(names, [std::ffi::OsString::from(active_name)]);
}

#[test]
fn physical_rotation_publishes_sealed_and_next_active_for_every_family() {
    let options = DurableStoreOptions::default()
        .with_durability_policy(DurabilityPolicy::Physical)
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());

    let value_directory = tempfile::tempdir().unwrap();
    let values = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    values.put(b"one".to_vec(), vec![1; 100]);
    values.put(b"two".to_vec(), vec![2; 100]);
    drop(values);
    assert_rotated(value_directory.path(), "kv.wal.dat");
    let values = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(values.get(b"two"), Some(vec![2; 100]));

    let set_directory = tempfile::tempdir().unwrap();
    let sets = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    sets.append(b"one".to_vec(), vec![1; 100]);
    sets.append(b"two".to_vec(), vec![2; 100]);
    drop(sets);
    assert_rotated(set_directory.path(), "set.wal.dat");
    let sets = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    assert!(sets.contains_in_set(b"two", &vec![2; 100]));

    let map_directory = tempfile::tempdir().unwrap();
    let maps = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    maps.put(b"one".to_vec(), SearchKey::from(1), vec![1; 100]);
    maps.put(b"two".to_vec(), SearchKey::from(2), vec![2; 100]);
    drop(maps);
    assert_rotated(map_directory.path(), "map.wal.dat");
    let maps = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(
        maps.get_element(b"two", &SearchKey::from(2)),
        Some(vec![2; 100])
    );
}

fn assert_rotated(directory: &std::path::Path, active_name: &str) {
    assert!(directory.join(active_name).is_file());
    assert!(directory
        .join(format!("{active_name}.segment-{:020}", 0))
        .is_file());
}

#[test]
fn physical_recovery_repairs_a_terminal_tail_before_exposing_the_store() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), b"accepted".to_vec());
    store.put(b"torn".to_vec(), b"unaccepted".to_vec());
    drop(store);

    let active = directory.path().join("kv.wal.dat");
    let mut interrupted = std::fs::read(&active).unwrap();
    interrupted.pop();
    std::fs::write(&active, interrupted).unwrap();

    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("physical recovery must publish a synchronized repaired WAL");

    assert_eq!(reopened.status(), RecoveryStatus::Recovered);
    assert_eq!(reopened.store().get(b"stable"), Some(b"accepted".to_vec()));
    assert_eq!(reopened.store().get(b"torn"), None);
    assert_only_active(directory.path(), "kv.wal.dat");
}

#[test]
fn physical_recovery_promotes_complete_rotation_staging_write_through() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_durability_policy(DurabilityPolicy::Physical)
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    store.put(b"second".to_vec(), b"two".to_vec());
    drop(store);

    let active = directory.path().join("kv.wal.dat");
    let active_bytes = std::fs::read(&active).unwrap();
    let sealed_one = directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000001");
    std::fs::rename(&active, sealed_one).unwrap();
    let mut next_header: [u8; 64] = active_bytes[..64].try_into().unwrap();
    next_header[32..40].copy_from_slice(&2_u64.to_le_bytes());
    let next_base =
        u64::from_le_bytes(active_bytes[40..48].try_into().unwrap()) + active_bytes.len() as u64;
    next_header[40..48].copy_from_slice(&next_base.to_le_bytes());
    let crc = crc32fast::hash(&next_header[..60]);
    next_header[60..64].copy_from_slice(&crc.to_le_bytes());
    let staging = directory.path().join(".kv.wal.dat.next");
    std::fs::write(&staging, next_header).unwrap();

    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("physical recovery must promote synchronized rotation staging");

    assert_eq!(reopened.status(), RecoveryStatus::Recovered);
    assert_eq!(reopened.store().get(b"first"), Some(b"one".to_vec()));
    assert_eq!(reopened.store().get(b"second"), Some(b"two".to_vec()));
    assert!(active.is_file());
    assert!(!staging.exists());
}

#[test]
fn physical_closed_compaction_publishes_manifest_previous_and_replacement_write_through() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_durability_policy(DurabilityPolicy::Physical)
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"first".to_vec(), vec![1; 100]);
    store.put(b"second".to_vec(), vec![2; 100]);
    drop(store);

    let outcome = compact_directory_in_place(
        directory.path(),
        ClosedCompactionOptions::default().with_durability_policy(DurabilityPolicy::Physical),
    )
    .expect("physical compaction must durably publish each authority phase");

    assert_eq!(outcome.families().len(), 1);
    assert_eq!(outcome.families()[0].cleanup(), CleanupStatus::Complete);
    assert_only_active(directory.path(), "kv.wal.dat");
    for _ in 0..3 {
        let reopened =
            DurableKeyValueStore::try_init_new_with_options(directory.path(), options).unwrap();
        assert_eq!(reopened.store().get(b"first"), Some(vec![1; 100]));
        assert_eq!(reopened.store().get(b"second"), Some(vec![2; 100]));
    }
}
