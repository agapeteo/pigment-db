use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::RecoveryError;

#[test]
fn empty_states_are_valid_and_kv_conflict_is_store_local() {
    let empty_directory = tempfile::tempdir().unwrap();
    for name in ["kv.wal.dat", "set.wal.dat", "map.wal.dat"] {
        std::fs::File::create(empty_directory.path().join(name)).unwrap();
    }
    assert!(matches!(
        DurableKeyValueStore::try_init_new(empty_directory.path()),
        Err(RecoveryError::MigrationRequired { .. })
    ));
    assert!(matches!(
        DurableKeySetStore::try_init_new(empty_directory.path()),
        Err(RecoveryError::MigrationRequired { .. })
    ));
    assert!(matches!(
        DurableKeyMapStore::try_init_new(empty_directory.path()),
        Err(RecoveryError::MigrationRequired { .. })
    ));

    let directory = tempfile::tempdir().unwrap();
    let set = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    set.append(b"set-key".to_vec(), b"member".to_vec());
    drop(set);
    let map = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    map.put(b"map-key".to_vec(), 7usize.into(), b"value".to_vec());
    drop(map);

    fn kv_wal(key: &[u8]) -> Vec<u8> {
        let source = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(source.path())
            .unwrap()
            .into_store();
        store.put(key.to_vec(), b"value".to_vec());
        drop(store);
        std::fs::read(source.path().join("kv.wal.dat")).unwrap()
    }
    std::fs::write(directory.path().join("kv.wal.dat"), kv_wal(b"active")).unwrap();
    std::fs::write(directory.path().join(".kv.wal.dat"), kv_wal(b"legacy")).unwrap();
    assert!(matches!(
        DurableKeyValueStore::try_init_new(directory.path()),
        Err(RecoveryError::AuthorityUndetermined { .. })
    ));

    let set = DurableKeySetStore::try_init_new(directory.path()).unwrap();
    assert!(set.store().contains_in_set(b"set-key", b"member"));
    drop(set);
    let map = DurableKeyMapStore::try_init_new(directory.path()).unwrap();
    assert_eq!(
        map.store().get_element(b"map-key", &7usize.into()),
        Some(b"value".to_vec())
    );
}
