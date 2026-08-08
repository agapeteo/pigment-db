use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{RecoveryError, RecoveryOperation, RecoveryStatus};

use super::support::{captured_logs, copy_fixture, start_log_capture};

#[test]
fn fresh_key_value_store_returns_normal_and_is_usable() {
    let directory = tempfile::tempdir().unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path())
        .expect("fresh store initialization should be fallible, not panicking");
    assert_eq!(outcome.status(), RecoveryStatus::Normal);
    assert_eq!(outcome.store().size(), 0);

    let store = outcome.into_store();
    store.put(b"key".to_vec(), b"value".to_vec());
    assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
}

#[test]
fn frozen_pre_feature_key_value_wal_requires_migration() {
    let directory = tempfile::tempdir().unwrap();
    copy_fixture("kv.wal.dat", directory.path(), "kv.wal.dat");
    let active = directory.path().join("kv.wal.dat");
    let before = std::fs::read(&active).unwrap();

    assert!(matches!(
        DurableKeyValueStore::try_init_new(directory.path()),
        Err(RecoveryError::MigrationRequired { path }) if path == active
    ));
    assert_eq!(std::fs::read(active).unwrap(), before);
}

#[test]
fn active_key_value_wal_wins_over_stale_staging() {
    for complete_staging in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        copy_fixture("kv.wal.dat", directory.path(), "kv.wal.dat");
        let staging = directory.path().join(".kv.wal.dat.next");

        if complete_staging {
            let staging_source = tempfile::tempdir().unwrap();
            let stage_store = DurableKeyValueStore::try_init_new(staging_source.path())
                .unwrap()
                .into_store();
            stage_store.put(b"stage-only".to_vec(), b"wrong".to_vec());
            drop(stage_store);
            std::fs::copy(staging_source.path().join("kv.wal.dat"), &staging).unwrap();
        } else {
            std::fs::File::create(&staging).unwrap();
        }

        let active = directory.path().join("kv.wal.dat");
        let active_before = std::fs::read(&active).unwrap();
        let staging_before = std::fs::read(&staging).unwrap();
        assert!(matches!(
            DurableKeyValueStore::try_init_new(directory.path()),
            Err(RecoveryError::MigrationRequired { path }) if path == active
        ));
        assert_eq!(std::fs::read(active).unwrap(), active_before);
        assert_eq!(std::fs::read(staging).unwrap(), staging_before);
    }
}

#[test]
fn compatibility_initializer_logs_recovery_once_and_panics_with_diagnostic() {
    const RECOVERY_EVENT: &str = "pigment-db recovered key/value WAL";
    start_log_capture();

    let normal_directory = tempfile::tempdir().unwrap();
    let normal = DurableKeyValueStore::init_new(normal_directory.path().to_str().unwrap());
    drop(normal);
    assert_eq!(
        captured_logs()
            .iter()
            .filter(|message| message.contains(RECOVERY_EVENT))
            .count(),
        0
    );

    let recovered_directory = tempfile::tempdir().unwrap();
    let recovered_seed = DurableKeyValueStore::try_init_new(recovered_directory.path())
        .unwrap()
        .into_store();
    recovered_seed.put(b"alpha".to_vec(), b"uno".to_vec());
    drop(recovered_seed);
    std::fs::File::create(recovered_directory.path().join(".kv.wal.dat.next")).unwrap();
    start_log_capture();
    let recovered = DurableKeyValueStore::init_new(recovered_directory.path().to_str().unwrap());
    assert_eq!(recovered.get(b"alpha"), Some(b"uno".to_vec()));
    assert_eq!(
        captured_logs()
            .iter()
            .filter(|message| message.contains(RECOVERY_EVENT))
            .count(),
        1
    );
    drop(recovered);

    let invalid_directory = tempfile::tempdir().unwrap();
    std::fs::write(invalid_directory.path().join("kv.wal.dat"), [1u8]).unwrap();
    let panic = match std::panic::catch_unwind(|| {
        DurableKeyValueStore::init_new(invalid_directory.path().to_str().unwrap())
    }) {
        Ok(_) => panic!("compatibility initializer must retain panic-on-error behavior"),
        Err(panic) => panic,
    };
    let diagnostic = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            panic
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_default();
    assert!(diagnostic.contains("invalid WAL artifact"), "{diagnostic}");
}

#[test]
fn repeated_interrupted_startups_converge_and_stay_stable() {
    let directory = tempfile::tempdir().unwrap();
    let initial = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    initial.put(b"original".to_vec(), b"value".to_vec());
    drop(initial);

    for attempt in 0..10u8 {
        let active = std::fs::read(directory.path().join("kv.wal.dat")).unwrap();
        let partial_len = active.len().saturating_sub(1).max(1);
        std::fs::write(
            directory.path().join(".kv.wal.dat.next"),
            &active[..partial_len],
        )
        .unwrap();

        let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Recovered);
        assert_eq!(outcome.store().get(b"original"), Some(b"value".to_vec()));
        for prior in 0..attempt {
            assert_eq!(outcome.store().get(&[b'k', prior]), Some(vec![b'v', prior]));
        }
        let store = outcome.into_store();
        store.put(vec![b'k', attempt], vec![b'v', attempt]);
        drop(store);
    }

    let completed = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(completed.status(), RecoveryStatus::Normal);
    assert_eq!(completed.store().get(b"original"), Some(b"value".to_vec()));
    for attempt in 0..10u8 {
        assert_eq!(
            completed.store().get(&[b'k', attempt]),
            Some(vec![b'v', attempt])
        );
    }
    drop(completed);

    for _ in 0..3 {
        let stable = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
        assert_eq!(stable.status(), RecoveryStatus::Normal);
        assert_eq!(stable.store().size(), 11);
        drop(stable);
    }
}

#[test]
fn legacy_only_key_value_artifact_is_recovered() {
    let directory = tempfile::tempdir().unwrap();
    copy_fixture("kv.wal.dat", directory.path(), ".kv.wal.dat");

    let legacy = directory.path().join(".kv.wal.dat");
    let before = std::fs::read(&legacy).unwrap();
    assert!(matches!(
        DurableKeyValueStore::try_init_new(directory.path()),
        Err(RecoveryError::MigrationRequired { path }) if path == legacy
    ));
    assert!(!directory.path().join("kv.wal.dat").exists());
    assert_eq!(std::fs::read(legacy).unwrap(), before);
}

#[test]
fn legacy_beats_zero_length_or_truncated_active_wal() {
    for truncated in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        copy_fixture("kv.wal.dat", directory.path(), ".kv.wal.dat");
        if truncated {
            let fixture = std::fs::read(super::support::fixture_path("kv.wal.dat")).unwrap();
            std::fs::write(
                directory.path().join("kv.wal.dat"),
                &fixture[..fixture.len() - 1],
            )
            .unwrap();
        } else {
            std::fs::File::create(directory.path().join("kv.wal.dat")).unwrap();
        }

        let names = ["kv.wal.dat", ".kv.wal.dat", ".kv.wal.dat.next"];
        let before = super::support::snapshot_files(directory.path(), &names);
        assert!(matches!(
            DurableKeyValueStore::try_init_new(directory.path()),
            Err(RecoveryError::MigrationRequired { .. })
        ));
        assert_eq!(
            super::support::snapshot_files(directory.path(), &names),
            before
        );
    }
}

#[test]
fn logically_equal_active_and_legacy_select_active() {
    let canonical_source = tempfile::tempdir().unwrap();
    let canonical_store = DurableKeyValueStore::try_init_new(canonical_source.path())
        .unwrap()
        .into_store();
    canonical_store.put(b"alpha".to_vec(), b"temporary".to_vec());
    canonical_store.put(b"alpha".to_vec(), b"uno".to_vec());
    canonical_store.put(b"empty".to_vec(), Vec::new());
    drop(canonical_store);
    let canonical = std::fs::read(canonical_source.path().join("kv.wal.dat")).unwrap();
    let equivalent_source = tempfile::tempdir().unwrap();
    let equivalent_store = DurableKeyValueStore::try_init_new(equivalent_source.path())
        .unwrap()
        .into_store();
    equivalent_store.put(b"alpha".to_vec(), b"uno".to_vec());
    equivalent_store.put(b"empty".to_vec(), Vec::new());
    drop(equivalent_store);
    let frozen = std::fs::read(equivalent_source.path().join("kv.wal.dat")).unwrap();
    assert_ne!(
        canonical, frozen,
        "fixture history should differ from compacted bytes"
    );

    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("kv.wal.dat"), canonical).unwrap();
    std::fs::write(directory.path().join(".kv.wal.dat"), frozen).unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Recovered);
    assert_eq!(outcome.store().get(b"alpha"), Some(b"uno".to_vec()));
    assert_eq!(outcome.store().get(b"empty"), Some(Vec::new()));
    assert!(!directory.path().join(".kv.wal.dat").exists());
}

#[test]
fn active_replay_that_reaches_legacy_then_continues_is_newer() {
    let legacy_source = tempfile::tempdir().unwrap();
    let legacy_store = DurableKeyValueStore::try_init_new(legacy_source.path())
        .unwrap()
        .into_store();
    legacy_store.put(b"alpha".to_vec(), b"uno".to_vec());
    legacy_store.put(b"empty".to_vec(), Vec::new());
    drop(legacy_store);
    drop(
        DurableKeyValueStore::try_init_new(legacy_source.path())
            .unwrap()
            .into_store(),
    );
    let legacy = std::fs::read(legacy_source.path().join("kv.wal.dat")).unwrap();

    let active_source = tempfile::tempdir().unwrap();
    std::fs::write(active_source.path().join("kv.wal.dat"), &legacy).unwrap();
    let active_store = DurableKeyValueStore::try_init_new(active_source.path())
        .unwrap()
        .into_store();
    active_store.put(b"alpha".to_vec(), b"newer".to_vec());
    active_store.remove(b"empty");
    active_store.put(b"later".to_vec(), b"write".to_vec());
    drop(active_store);
    let active = std::fs::read(active_source.path().join("kv.wal.dat")).unwrap();

    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("kv.wal.dat"), active).unwrap();
    std::fs::write(directory.path().join(".kv.wal.dat"), legacy).unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Recovered);
    assert_eq!(outcome.store().get(b"alpha"), Some(b"newer".to_vec()));
    assert_eq!(outcome.store().get(b"empty"), None);
    assert_eq!(outcome.store().get(b"later"), Some(b"write".to_vec()));
}

#[test]
fn proper_compacted_snapshot_prefix_selects_legacy() {
    let legacy_source = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(legacy_source.path())
        .unwrap()
        .into_store();
    store.put(b"a".to_vec(), b"one".to_vec());
    store.put(b"b".to_vec(), b"two".to_vec());
    store.put(b"c".to_vec(), b"three".to_vec());
    drop(store);
    drop(
        DurableKeyValueStore::try_init_new(legacy_source.path())
            .unwrap()
            .into_store(),
    );
    let legacy = std::fs::read(legacy_source.path().join("kv.wal.dat")).unwrap();
    let first_start = 64;
    let first_data_len = usize::try_from(u64::from_le_bytes(
        legacy[first_start + 6..first_start + 14]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let first_frame_end = first_start + 66 + first_data_len;
    let second_data_len = usize::try_from(u64::from_le_bytes(
        legacy[first_frame_end + 6..first_frame_end + 14]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let second_frame_end = first_frame_end + 66 + second_data_len;

    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("kv.wal.dat"),
        &legacy[..second_frame_end],
    )
    .unwrap();
    std::fs::write(directory.path().join(".kv.wal.dat"), legacy).unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Recovered);
    assert_eq!(outcome.store().get(b"a"), Some(b"one".to_vec()));
    assert_eq!(outcome.store().get(b"b"), Some(b"two".to_vec()));
    assert_eq!(outcome.store().get(b"c"), Some(b"three".to_vec()));
}

#[test]
fn ambiguous_candidates_return_structured_error_without_byte_changes() {
    fn one_entry_wal(key: &[u8], value: &[u8]) -> Vec<u8> {
        let source = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(source.path())
            .unwrap()
            .into_store();
        store.put(key.to_vec(), value.to_vec());
        drop(store);
        std::fs::read(source.path().join("kv.wal.dat")).unwrap()
    }

    let directory = tempfile::tempdir().unwrap();
    let active_path = directory.path().join("kv.wal.dat");
    let legacy_path = directory.path().join(".kv.wal.dat");
    std::fs::write(&active_path, one_entry_wal(b"active", b"one")).unwrap();
    std::fs::write(&legacy_path, one_entry_wal(b"legacy", b"two")).unwrap();
    let names = ["kv.wal.dat", ".kv.wal.dat", ".kv.wal.dat.next"];
    let before = super::support::snapshot_files(directory.path(), &names);

    let error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("unrelated valid candidates must not be guessed between"),
        Err(error) => error,
    };
    match error {
        RecoveryError::AuthorityUndetermined {
            active_path: found_active,
            recovery_path: found_legacy,
        } => {
            assert_eq!(found_active, Some(active_path));
            assert_eq!(found_legacy, Some(legacy_path));
        }
        other => panic!("unexpected error: {other}"),
    }
    assert_eq!(
        super::support::snapshot_files(directory.path(), &names),
        before
    );
}

#[test]
fn filesystem_failures_include_operation_path_and_source() {
    fn assert_io(
        result: Result<
            pigment_db::RecoveryOutcome<DurableKeyValueStore<std::fs::File>>,
            RecoveryError,
        >,
        expected_operation: RecoveryOperation,
        expected_path: &std::path::Path,
    ) {
        match result {
            Err(RecoveryError::Io {
                operation,
                path,
                source,
            }) => {
                assert_eq!(operation, expected_operation);
                assert_eq!(path, expected_path);
                assert!(!source.to_string().is_empty());
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("filesystem fault unexpectedly succeeded"),
        }
    }

    let parent = tempfile::tempdir().unwrap();
    let not_a_directory = parent.path().join("store-file");
    std::fs::write(&not_a_directory, b"not a directory").unwrap();
    assert_io(
        DurableKeyValueStore::try_init_new(&not_a_directory),
        RecoveryOperation::Inspect,
        &not_a_directory.join("kv.wal.dat"),
    );

    let open_directory = tempfile::tempdir().unwrap();
    let active_directory = open_directory.path().join("kv.wal.dat");
    std::fs::create_dir(&active_directory).unwrap();
    assert_io(
        DurableKeyValueStore::try_init_new(open_directory.path()),
        RecoveryOperation::Open,
        &active_directory,
    );

    let missing_parent = parent.path().join("missing");
    assert_io(
        DurableKeyValueStore::try_init_new(&missing_parent),
        RecoveryOperation::CreateStaging,
        &missing_parent.join(".kv.wal.dat.next"),
    );
}
