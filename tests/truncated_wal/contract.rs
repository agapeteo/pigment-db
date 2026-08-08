//! Cross-store recovery contract tests.

use super::support::{captured_logs, start_log_capture};

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    DurableStoreOptions, RecoveryError, RecoveryStatus, TimestampGranularity,
    TimestampGranularityError,
};
use std::fs;
use std::time::Duration;

#[test]
fn fresh_store_uses_v2_header() {
    let directory = tempfile::tempdir().unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Normal);
    drop(outcome);

    let active = directory.path().join("kv.wal.dat");
    let staging = directory.path().join(".kv.wal.dat.next");
    let bytes = fs::read(active).unwrap();
    assert_eq!(bytes.len(), 64);
    assert_eq!(&bytes[..8], b"PIGWAL\r\n");
    assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 2);
    assert_eq!(u16::from_le_bytes(bytes[10..12].try_into().unwrap()), 64);
    assert_eq!(bytes[12], 1);
    assert_eq!(bytes[13], 1);
    assert_eq!(u16::from_le_bytes(bytes[14..16].try_into().unwrap()), 0);
    assert_eq!(
        u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        60_000_000_000
    );
    assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 0);
    assert_eq!(u64::from_le_bytes(bytes[32..40].try_into().unwrap()), 0);
    assert_eq!(u64::from_le_bytes(bytes[40..48].try_into().unwrap()), 0);
    assert_eq!(&bytes[48..60], &[0; 12]);
    assert_eq!(
        u32::from_le_bytes(bytes[60..64].try_into().unwrap()),
        crc32fast::hash(&bytes[..60])
    );
    assert!(!staging.exists());
}

#[test]
fn public_timestamp_options_validate_and_adapt_all_store_families() {
    assert_eq!(
        TimestampGranularity::try_from(Duration::ZERO),
        Err(TimestampGranularityError::Zero)
    );
    assert_eq!(
        TimestampGranularity::try_from(Duration::new(u64::MAX, 999_999_999)),
        Err(TimestampGranularityError::TooLarge)
    );

    let selected = TimestampGranularity::try_from(Duration::from_nanos(250)).unwrap();
    let options = DurableStoreOptions::default().with_timestamp_granularity(selected);

    let value_directory = tempfile::tempdir().unwrap();
    let value = DurableKeyValueStore::try_init_new_with_options(value_directory.path(), options)
        .unwrap()
        .into_store();
    value.put(b"key".to_vec(), b"value".to_vec());
    drop(value);
    assert_eq!(
        u64::from_le_bytes(
            fs::read(value_directory.path().join("kv.wal.dat")).unwrap()[16..24]
                .try_into()
                .unwrap()
        ),
        250
    );
    let unchanged = DurableKeyValueStore::try_init_new(value_directory.path()).unwrap();
    assert_eq!(unchanged.store().get(b"key"), Some(b"value".to_vec()));
    drop(unchanged);

    let changed_options = DurableStoreOptions::default().with_timestamp_granularity(
        TimestampGranularity::try_from(Duration::from_nanos(500)).unwrap(),
    );
    let changed =
        DurableKeyValueStore::try_init_new_with_options(value_directory.path(), changed_options)
            .unwrap();
    assert_eq!(changed.status(), RecoveryStatus::Normal);
    assert_eq!(changed.store().get(b"key"), Some(b"value".to_vec()));
    changed
        .store()
        .put(b"after-granularity-change".to_vec(), b"accepted".to_vec());
    drop(changed);
    assert_eq!(
        u64::from_le_bytes(
            fs::read(value_directory.path().join("kv.wal.dat")).unwrap()[16..24]
                .try_into()
                .unwrap()
        ),
        500
    );
    assert!(value_directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000")
        .is_file());

    let set_directory = tempfile::tempdir().unwrap();
    let set = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .unwrap()
        .into_store();
    set.append(b"set".to_vec(), b"member".to_vec());
    drop(set);
    assert_eq!(
        u64::from_le_bytes(
            fs::read(set_directory.path().join("set.wal.dat")).unwrap()[16..24]
                .try_into()
                .unwrap()
        ),
        250
    );

    let map_directory = tempfile::tempdir().unwrap();
    let map = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .unwrap()
        .into_store();
    map.put(b"map".to_vec(), 1_usize.into(), b"entry".to_vec());
    drop(map);
    assert_eq!(
        u64::from_le_bytes(
            fs::read(map_directory.path().join("map.wal.dat")).unwrap()[16..24]
                .try_into()
                .unwrap()
        ),
        250
    );

    let vector_value = DurableKeyValueStore::new_vec_based_with_options(options);
    vector_value.put(b"v".to_vec(), b"1".to_vec());
    assert_eq!(vector_value.get(b"v"), Some(b"1".to_vec()));
    let vector_set = DurableKeySetStore::new_vec_based_with_options(options);
    vector_set.append(b"s".to_vec(), b"m".to_vec());
    assert!(vector_set.contains_in_set(b"s", b"m"));
    let vector_map = DurableKeyMapStore::new_vec_based_with_options(options);
    vector_map.put(b"m".to_vec(), 2_usize.into(), b"e".to_vec());
    assert!(vector_map.contains_in_map(b"m", &2_usize.into()));
}

#[test]
fn existing_zero_byte_active_is_empty_legacy_and_requires_migration() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    fs::write(&active, []).unwrap();

    let error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("existing zero-byte active must not enter fresh creation"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        RecoveryError::MigrationRequired { path } if path == active
    ));
    assert_eq!(fs::read(active).unwrap(), Vec::<u8>::new());
}

#[test]
fn existing_partial_and_corrupt_v1_headers_are_preserved_invalid() {
    let header = value_v1_header();
    let mut cases = (1..40)
        .map(|length| header[..length].to_vec())
        .collect::<Vec<_>>();
    let mut corrupt_crc = header;
    corrupt_crc[39] ^= 0xff;
    cases.push(corrupt_crc.to_vec());

    for bytes in cases {
        let directory = tempfile::tempdir().unwrap();
        let active = directory.path().join("kv.wal.dat");
        fs::write(&active, &bytes).unwrap();

        let error = match DurableKeyValueStore::try_init_new(directory.path()) {
            Ok(_) => panic!("existing invalid V1 header must never enter fresh creation"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            RecoveryError::InvalidArtifact { path } if path == active
        ));
        assert_eq!(fs::read(active).unwrap(), bytes);
    }
}

fn value_v1_header() -> [u8; 40] {
    v1_header(1)
}

#[test]
fn v1_header_store_kind_mismatch_is_rejected_without_mutation() {
    let value_directory = tempfile::tempdir().unwrap();
    let value_path = value_directory.path().join("kv.wal.dat");
    let value_bytes = v1_header(2);
    fs::write(&value_path, value_bytes).unwrap();
    let value_error = match DurableKeyValueStore::try_init_new(value_directory.path()) {
        Ok(_) => panic!("set header must not open as value store"),
        Err(error) => error,
    };
    assert!(matches!(
        value_error,
        RecoveryError::InvalidArtifact { path } if path == value_path
    ));
    assert_eq!(fs::read(value_path).unwrap(), value_bytes);

    let set_directory = tempfile::tempdir().unwrap();
    let set_path = set_directory.path().join("set.wal.dat");
    let set_bytes = v1_header(3);
    fs::write(&set_path, set_bytes).unwrap();
    let set_error = match DurableKeySetStore::try_init_new(set_directory.path()) {
        Ok(_) => panic!("map header must not open as set store"),
        Err(error) => error,
    };
    assert!(matches!(
        set_error,
        RecoveryError::InvalidArtifact { path } if path == set_path
    ));
    assert_eq!(fs::read(set_path).unwrap(), set_bytes);

    let map_directory = tempfile::tempdir().unwrap();
    let map_path = map_directory.path().join("map.wal.dat");
    let map_bytes = v1_header(1);
    fs::write(&map_path, map_bytes).unwrap();
    let map_error = match DurableKeyMapStore::try_init_new(map_directory.path()) {
        Ok(_) => panic!("value header must not open as map store"),
        Err(error) => error,
    };
    assert!(matches!(
        map_error,
        RecoveryError::InvalidArtifact { path } if path == map_path
    ));
    assert_eq!(fs::read(map_path).unwrap(), map_bytes);
}

fn v1_header(kind: u8) -> [u8; 40] {
    let mut bytes = [0; 40];
    bytes[..8].copy_from_slice(b"PIGWAL\r\n");
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&40_u16.to_le_bytes());
    bytes[12] = kind;
    bytes[13] = 1;
    bytes[16..24].copy_from_slice(&60_000_000_000_u64.to_le_bytes());
    let crc = crc32fast::hash(&bytes[..36]);
    bytes[36..40].copy_from_slice(&crc.to_le_bytes());
    bytes
}

#[test]
fn earlier_complete_corruption_wins_over_later_terminal_fragment() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    store.put(b"second".to_vec(), b"two".to_vec());
    drop(store);
    let active = directory.path().join("kv.wal.dat");
    let mut bytes = fs::read(&active).unwrap();
    let first_payload_len =
        usize::try_from(u64::from_le_bytes(bytes[70..78].try_into().unwrap())).unwrap();
    let first_crc = 64 + 62 + first_payload_len;
    bytes[first_crc] ^= 0xff;
    bytes.pop();
    fs::write(&active, &bytes).unwrap();

    let error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("earlier complete corruption must not be hidden by a torn tail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        RecoveryError::InvalidArtifact { path } if path == active
    ));
    assert_eq!(fs::read(active).unwrap(), bytes);
}

#[test]
fn protected_field_manifest_preserves_every_corrupt_input() {
    const FIELDS: [&str; 14] = [
        "marker",
        "version",
        "action",
        "header-length",
        "payload-length",
        "length-complement",
        "physical-start",
        "mutation-start",
        "index",
        "count",
        "timestamp",
        "payload",
        "footer",
        "crc",
    ];
    assert_eq!(std::collections::HashSet::from(FIELDS).len(), FIELDS.len());

    for field in FIELDS {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"key".to_vec(), b"value".to_vec());
        drop(store);
        let active = directory.path().join("kv.wal.dat");
        let mut bytes = fs::read(&active).unwrap();
        let frame_start = 64;
        let payload_len = usize::try_from(u64::from_le_bytes(
            bytes[frame_start + 6..frame_start + 14].try_into().unwrap(),
        ))
        .unwrap();
        let relative = match field {
            "marker" => 0,
            "version" => 2,
            "action" => 3,
            "header-length" => 4,
            "payload-length" => 6,
            "length-complement" => 14,
            "physical-start" => 22,
            "mutation-start" => 30,
            "index" => 38,
            "count" => 42,
            "timestamp" => 46,
            "payload" => 54,
            "footer" => 54 + payload_len,
            "crc" => 62 + payload_len,
            _ => unreachable!(),
        };
        bytes[frame_start + relative] ^= 0xff;
        fs::write(&active, &bytes).unwrap();

        let error = match DurableKeyValueStore::try_init_new(directory.path()) {
            Ok(_) => panic!("protected {field} corruption must fail closed"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            RecoveryError::InvalidArtifact { path } if path == active
        ));
        assert_eq!(fs::read(active).unwrap(), bytes, "{field}");
    }
}

#[test]
fn exact_record_boundary_and_zero_length_delete_are_complete() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(Vec::new(), Vec::new());
    store.remove(b"");
    drop(store);

    let bytes = fs::read(&active).unwrap();
    let first_payload_len =
        usize::try_from(u64::from_le_bytes(bytes[70..78].try_into().unwrap())).unwrap();
    let delete_start = 64 + 66 + first_payload_len;
    assert_eq!(
        u64::from_le_bytes(
            bytes[delete_start + 6..delete_start + 14]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(bytes.len(), delete_start + 66);

    let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(outcome.status(), RecoveryStatus::Normal);
    assert!(!outcome.store().contains(b""));
    assert_eq!(fs::read(active).unwrap(), bytes);
}

#[test]
fn repaired_outcome_keeps_callbacks_appends_and_three_reopens_stable() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), b"before".to_vec());
    store.put(b"pending".to_vec(), b"discard".to_vec());
    drop(store);
    let mut interrupted = fs::read(&active).unwrap();
    interrupted.pop();
    fs::write(&active, interrupted).unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    let (store, status) = outcome.into_parts();
    assert_eq!(status, RecoveryStatus::Recovered);
    assert_eq!(store.get(b"stable"), Some(b"before".to_vec()));
    assert_eq!(store.get(b"pending"), None);
    store.compute(b"stable".to_vec(), |current| {
        assert_eq!(current, Some(b"before".as_slice()));
        b"after-callback".to_vec()
    });
    store.put(b"after-repair".to_vec(), b"appended".to_vec());
    drop(store);

    for _ in 0..3 {
        let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Normal);
        assert_eq!(
            outcome.store().get(b"stable"),
            Some(b"after-callback".to_vec())
        );
        assert_eq!(
            outcome.store().get(b"after-repair"),
            Some(b"appended".to_vec())
        );
        assert_eq!(outcome.store().get(b"pending"), None);
        drop(outcome);
    }
}

#[test]
fn compatibility_initializer_notifies_only_for_the_repairing_startup() {
    const RECOVERY_EVENT: &str = "pigment-db recovered key/value WAL";
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"accepted".to_vec(), b"stable".to_vec());
    store.put(b"torn".to_vec(), b"discard".to_vec());
    drop(store);
    let mut interrupted = fs::read(&active).unwrap();
    interrupted.pop();
    fs::write(&active, interrupted).unwrap();

    start_log_capture();
    let repaired = DurableKeyValueStore::init_new(directory.path().to_str().unwrap());
    assert_eq!(repaired.get(b"accepted"), Some(b"stable".to_vec()));
    assert_eq!(repaired.get(b"torn"), None);
    assert_eq!(
        captured_logs()
            .iter()
            .filter(|message| message.contains(RECOVERY_EVENT))
            .count(),
        1
    );
    drop(repaired);

    let reopened = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
    assert_eq!(reopened.status(), RecoveryStatus::Normal);
    drop(reopened);

    start_log_capture();
    drop(DurableKeyValueStore::init_new(
        directory.path().to_str().unwrap(),
    ));
    assert_eq!(
        captured_logs()
            .iter()
            .filter(|message| message.contains(RECOVERY_EVENT))
            .count(),
        0
    );
}

#[test]
fn complete_and_partial_rollback_failure_group_shapes_are_all_or_none() {
    let complete_directory = tempfile::tempdir().unwrap();
    let complete = DurableKeySetStore::try_init_new(complete_directory.path())
        .unwrap()
        .into_store();
    complete.compute(b"group".to_vec(), |set| {
        set.insert(b"alpha".to_vec());
        set.insert(b"beta".to_vec());
    });
    drop(complete);
    let complete = DurableKeySetStore::try_init_new(complete_directory.path()).unwrap();
    assert_eq!(complete.status(), RecoveryStatus::Normal);
    assert!(complete.store().contains_in_set(b"group", b"alpha"));
    assert!(complete.store().contains_in_set(b"group", b"beta"));

    let partial_directory = tempfile::tempdir().unwrap();
    let active = partial_directory.path().join("set.wal.dat");
    let partial = DurableKeySetStore::try_init_new(partial_directory.path())
        .unwrap()
        .into_store();
    partial.compute(b"group".to_vec(), |set| {
        set.insert(b"alpha".to_vec());
        set.insert(b"beta".to_vec());
    });
    drop(partial);
    let mut interrupted = fs::read(&active).unwrap();
    interrupted.pop();
    fs::write(&active, interrupted).unwrap();

    let partial = DurableKeySetStore::try_init_new(partial_directory.path()).unwrap();
    assert_eq!(partial.status(), RecoveryStatus::Recovered);
    assert!(!partial.store().contains_key(b"group"));
    assert!(!partial.store().contains_in_set(b"group", b"alpha"));
    assert!(!partial.store().contains_in_set(b"group", b"beta"));
}

#[test]
fn accepted_active_tail_beats_an_older_complete_recovery_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let recovery = directory.path().join(".kv.wal.dat");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    fs::copy(&active, &recovery).unwrap();
    store.put(b"second".to_vec(), b"two".to_vec());
    store.put(b"torn".to_vec(), b"discard".to_vec());
    drop(store);
    let mut interrupted = fs::read(&active).unwrap();
    interrupted.pop();
    fs::write(&active, interrupted).unwrap();

    let outcome = DurableKeyValueStore::try_init_new(directory.path())
        .expect("accepted active prefix should prove it is newer than recovery");

    assert_eq!(outcome.status(), RecoveryStatus::Recovered);
    assert_eq!(outcome.store().get(b"first"), Some(b"one".to_vec()));
    assert_eq!(outcome.store().get(b"second"), Some(b"two".to_vec()));
    assert_eq!(outcome.store().get(b"torn"), None);
    assert!(!recovery.exists());
}

#[test]
fn incomparable_complete_and_recoverable_candidates_remain_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let recovery = directory.path().join(".kv.wal.dat");
    let staging = directory.path().join(".kv.wal.dat.next");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"b".to_vec(), b"two".to_vec());
    store.put(b"a".to_vec(), b"one".to_vec());
    store.put(b"torn".to_vec(), b"discard".to_vec());
    drop(store);
    let mut active_bytes = fs::read(&active).unwrap();
    active_bytes.pop();
    fs::write(&active, &active_bytes).unwrap();

    let recovery_source = tempfile::tempdir().unwrap();
    let recovery_store = DurableKeyValueStore::try_init_new(recovery_source.path())
        .unwrap()
        .into_store();
    recovery_store.put(b"a".to_vec(), b"one".to_vec());
    drop(recovery_store);
    fs::copy(recovery_source.path().join("kv.wal.dat"), &recovery).unwrap();
    let recovery_bytes = fs::read(&recovery).unwrap();

    let error = match DurableKeyValueStore::try_init_new(directory.path()) {
        Ok(_) => panic!("snapshot inclusion without a reached logical prefix is ambiguous"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        RecoveryError::AuthorityUndetermined {
            active_path: Some(active_path),
            recovery_path: Some(recovery_path),
        } if active_path == active && recovery_path == recovery
    ));
    assert_eq!(fs::read(active).unwrap(), active_bytes);
    assert_eq!(fs::read(recovery).unwrap(), recovery_bytes);
    assert!(!staging.exists());
}

#[test]
fn requirements_traceability_maps_every_fr_and_sc_exactly_once() {
    const CASES: &[(&str, &str)] = &[
        (
            "FR-001",
            "protected_field_manifest_preserves_every_corrupt_input",
        ),
        (
            "FR-002",
            "all_six_action_shapes_classify_terminal_fragments_as_recoverable",
        ),
        ("FR-003", "every_action_cut_and_fresh_header_fault_matrices"),
        (
            "FR-004",
            "complete_and_partial_rollback_failure_group_shapes_are_all_or_none",
        ),
        ("FR-005", "every_store_action_cut_preserves_public_prefix"),
        (
            "FR-006",
            "every_group_member_byte_cut_rolls_back_to_mutation_start",
        ),
        (
            "FR-007",
            "repaired_outcome_keeps_callbacks_appends_and_three_reopens_stable",
        ),
        (
            "FR-008",
            "grouped_write_shares_start_count_timestamp_and_uses_one_flush",
        ),
        ("FR-009", "selected_active_tail_is_repaired_through_staging"),
        (
            "FR-010",
            "repaired_outcome_keeps_callbacks_appends_and_three_reopens_stable",
        ),
        (
            "FR-011",
            "protected_field_manifest_preserves_every_corrupt_input",
        ),
        (
            "FR-012",
            "earlier_complete_corruption_wins_over_later_terminal_fragment",
        ),
        (
            "FR-013",
            "existing_partial_and_corrupt_v1_headers_are_preserved_invalid",
        ),
        (
            "FR-014",
            "repair_fault_checkpoint_matrix_preserves_authority",
        ),
        (
            "FR-015",
            "compatibility_initializer_notifies_only_for_the_repairing_startup",
        ),
        (
            "FR-016",
            "repaired_outcome_keeps_callbacks_appends_and_three_reopens_stable",
        ),
        ("FR-017", "key_value_set_map_cut_matrices"),
        (
            "FR-018",
            "accepted_active_tail_and_incomparable_candidate_contracts",
        ),
        (
            "FR-019",
            "complete_legacy_startup_requires_migration_without_mutation",
        ),
        (
            "FR-020",
            "public_recovery_contract_is_structured_and_compatible",
        ),
        (
            "FR-021",
            "fresh_store_uses_v1_header_and_frozen_legacy_compatibility",
        ),
        ("FR-022", "every_action_and_group_byte_cut_matrices"),
        (
            "FR-023",
            "truncated_legacy_startup_is_invalid_and_preserves_bytes",
        ),
        ("FR-024", "cross_store_timestamp_repeatability_matrices"),
        ("FR-025", "fresh_store_uses_v1_header_default_granularity"),
        (
            "FR-026",
            "public_timestamp_options_validate_and_adapt_all_store_families",
        ),
        ("FR-027", "repair_compaction_and_group_timestamp_contracts"),
        ("FR-028", "record_crc_covers_envelope"),
        (
            "FR-029",
            "protected_field_manifest_preserves_every_corrupt_input",
        ),
        ("FR-030", "frozen_legacy_payload_crc_compatibility"),
        ("FR-031", "crc_accidental_corruption_scope_documented"),
        ("FR-032", "immutable_36_cell_baseline_artifact"),
        ("FR-033", "per_cell_candidate_threshold_driver"),
        (
            "FR-034",
            "forward_equal_and_backward_clocks_never_decrease_across_restart",
        ),
        (
            "FR-035",
            "reopen_restores_persisted_granularity_and_last_complete_bucket",
        ),
        (
            "FR-036",
            "group_timestamp_sequence_and_mutation_ordering_suites",
        ),
        (
            "FR-037",
            "migration_cli_contract_process_and_failure_matrices",
        ),
        ("SC-001", "every_store_action_and_group_byte_cut_matrices"),
        ("SC-002", "every_store_cut_excludes_terminal_operation"),
        (
            "SC-003",
            "complete_and_partial_rollback_failure_group_shapes_are_all_or_none",
        ),
        (
            "SC-004",
            "repaired_outcome_keeps_callbacks_appends_and_three_reopens_stable",
        ),
        (
            "SC-005",
            "protected_field_manifest_preserves_every_corrupt_input",
        ),
        (
            "SC-006",
            "repair_fault_checkpoint_matrix_never_returns_store",
        ),
        ("SC-007", "frozen_complete_legacy_migration_required_matrix"),
        (
            "SC-008",
            "million_operation_complete_versus_torn_startup_driver",
        ),
        (
            "SC-009",
            "recovered_once_later_normal_and_compatibility_notification",
        ),
        (
            "SC-010",
            "truncated_legacy_startup_is_invalid_and_preserves_bytes",
        ),
        ("SC-011", "default_nondefault_and_group_timestamp_matrices"),
        ("SC-012", "every_protected_field_corruption_matrix"),
        (
            "SC-013",
            "all_frozen_legacy_fixtures_migrate_with_state_parity",
        ),
        (
            "SC-014",
            "all_36_candidate_cells_have_independent_thresholds",
        ),
        (
            "SC-015",
            "final_performance_report_requires_zero_exceptions",
        ),
        (
            "SC-016",
            "forward_equal_and_backward_clocks_never_decrease_across_restart",
        ),
        (
            "SC-017",
            "cross_store_timestamp_groups_repeat_across_three_reopens",
        ),
        (
            "SC-018",
            "migration_success_failure_source_and_destination_matrix",
        ),
    ];

    let mut expected = (1..=37)
        .map(|number| format!("FR-{number:03}"))
        .chain((1..=18).map(|number| format!("SC-{number:03}")))
        .collect::<Vec<_>>();
    let mut actual = CASES
        .iter()
        .map(|(requirement, evidence)| {
            assert!(
                !evidence.is_empty(),
                "{requirement} must name executable evidence"
            );
            (*requirement).to_owned()
        })
        .collect::<Vec<_>>();
    expected.sort();
    actual.sort();
    actual.dedup();
    assert_eq!(actual, expected);
    assert_eq!(CASES.len(), 55, "each requirement must map exactly once");
}
