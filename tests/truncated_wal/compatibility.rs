//! Frozen legacy and V1 compatibility tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::RecoveryError;
use std::fs;

#[test]
fn complete_legacy_startup_requires_migration_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let fixture = include_bytes!("../fixtures/legacy/kv.wal.dat");
    fs::write(&active, fixture).unwrap();

    let error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("complete legacy startup must require explicit migration"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        RecoveryError::MigrationRequired { path } if path == active
    ));
    assert_eq!(fs::read(active).unwrap(), fixture);
}

#[test]
fn legacy_compatibility_panic_names_the_migration_tool() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let fixture = include_bytes!("../fixtures/legacy/kv.wal.dat");
    fs::write(&active, fixture).unwrap();
    let directory_text = directory.path().to_str().unwrap().to_owned();

    let panic = match std::panic::catch_unwind(|| DurableKeyValueStore::init_new(&directory_text)) {
        Ok(_) => panic!("compatibility initializer must panic for legacy input"),
        Err(panic) => panic,
    };
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap();

    assert!(message.contains("pigment-db-migrate"));
    assert!(message.contains(active.to_str().unwrap()));
    assert_eq!(fs::read(active).unwrap(), fixture);
}

#[test]
fn truncated_legacy_startup_is_invalid_and_preserves_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let fixture = include_bytes!("../fixtures/legacy/kv.wal.dat");
    let truncated = &fixture[..fixture.len() - 1];
    fs::write(&active, truncated).unwrap();

    let error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("truncated legacy input must fail closed"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        RecoveryError::InvalidArtifact { path } if path == active
    ));
    assert_eq!(fs::read(active).unwrap(), truncated);
}

#[test]
fn frozen_all_family_and_no_mixed_grammar_regressions() {
    let directory = tempfile::tempdir().unwrap();
    for (name, fixture) in [
        (
            "kv.wal.dat",
            include_bytes!("../fixtures/legacy/kv.wal.dat").as_slice(),
        ),
        (
            "set.wal.dat",
            include_bytes!("../fixtures/legacy/set.wal.dat").as_slice(),
        ),
        (
            "map.wal.dat",
            include_bytes!("../fixtures/legacy/map.wal.dat").as_slice(),
        ),
    ] {
        fs::write(directory.path().join(name), fixture).unwrap();
    }
    let kv_error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("frozen key/value fixture must require migration"),
        Err(error) => error,
    };
    let set_error = match DurableKeySetStore::try_init_new(directory.path()) {
        Ok(_) => panic!("frozen key/set fixture must require migration"),
        Err(error) => error,
    };
    let map_error = match DurableKeyMapStore::try_init_new(directory.path()) {
        Ok(_) => panic!("frozen key/map fixture must require migration"),
        Err(error) => error,
    };
    assert!(matches!(kv_error, RecoveryError::MigrationRequired { .. }));
    assert!(matches!(set_error, RecoveryError::MigrationRequired { .. }));
    assert!(matches!(map_error, RecoveryError::MigrationRequired { .. }));
    assert_eq!(
        fs::read(directory.path().join("kv.wal.dat")).unwrap(),
        include_bytes!("../fixtures/legacy/kv.wal.dat")
    );
    assert_eq!(
        fs::read(directory.path().join("set.wal.dat")).unwrap(),
        include_bytes!("../fixtures/legacy/set.wal.dat")
    );
    assert_eq!(
        fs::read(directory.path().join("map.wal.dat")).unwrap(),
        include_bytes!("../fixtures/legacy/map.wal.dat")
    );

    let fresh = tempfile::tempdir().unwrap();
    let value = DurableKeyValueStore::try_init_new(fresh.path())
        .unwrap()
        .into_store();
    value.put(b"key".to_vec(), b"value".to_vec());
    let set = DurableKeySetStore::try_init_new(fresh.path())
        .unwrap()
        .into_store();
    set.append(b"key".to_vec(), b"member".to_vec());
    let map = DurableKeyMapStore::try_init_new(fresh.path())
        .unwrap()
        .into_store();
    map.put(b"key".to_vec(), 1_usize.into(), b"value".to_vec());
    drop((value, set, map));
    for name in ["kv.wal.dat", "set.wal.dat", "map.wal.dat"] {
        let bytes = fs::read(fresh.path().join(name)).unwrap();
        assert_eq!(&bytes[..8], b"PIGWAL\r\n");
        assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 2);
        assert_eq!(&bytes[64..66], &[0xa7, 0xd1]);
        assert!(!matches!(bytes[64], 0..=5));
    }
    let _ = DurableKeyValueStore::try_init_new(fresh.path()).unwrap();
    let _ = DurableKeySetStore::try_init_new(fresh.path()).unwrap();
    let _ = DurableKeyMapStore::try_init_new(fresh.path()).unwrap();

    let collision = tempfile::tempdir().unwrap();
    let collision_path = collision.path().join("kv.wal.dat");
    let collision_bytes = b"PIGWAL\rX";
    fs::write(&collision_path, collision_bytes).unwrap();
    let collision_error = match DurableKeyValueStore::try_init_new(collision.path()) {
        Ok(_) => panic!("near-magic collision must remain invalid"),
        Err(error) => error,
    };
    assert!(matches!(
        collision_error,
        RecoveryError::InvalidArtifact { path } if path == collision_path
    ));
    assert_eq!(fs::read(collision_path).unwrap(), collision_bytes);
}
