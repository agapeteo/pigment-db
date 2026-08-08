//! Runtime RED–GREEN tracers for V1 format, publication, repair, and recovery.

use super::format::{
    HeaderProbeClassification, RecordBoundsError, RecordProbeFields, V1CodecProbe, V2CodecProbe,
    V2HeaderProbeFields, V2RecordProbeFields,
};
use super::model::{
    KeyValueData, DELETE_ACT, MAP_PUT_ACT, MAP_REMOVE_ACT, PUT_ACT, SET_APPEND_ACT, SET_REMOVE_ACT,
};
use super::recovery::{
    cleanup_after_validated_repair, cleanup_blocking_repair_active, create_fresh_staging,
    create_repair_staging, encode_key_value_repair_snapshot, fail_cleanup_for, flush_fresh_header,
    flush_repair_snapshot, handoff_fresh_handle, initialize_snapshot, inspect_fresh_candidate,
    prepare_fresh_append, publish_fresh_header, publish_repair_snapshot, publish_validated_repair,
    readback_fresh_header, reopen_key_value_repair_snapshot, sync_fresh_header,
    sync_repair_snapshot, validate_fresh_header, validate_key_value_repair_snapshot,
    write_fresh_header_prefix, write_repair_snapshot_prefix, ArtifactPaths, FreshCandidateRole,
    FreshCleanupRegistry, FreshInspection, FreshOptionsProbe, RepairAuthority, StoreKind,
};
use super::replay::{
    encode_key_value_snapshot, key_value_is_proper_snapshot_prefix, replay_key_value,
    replay_key_value_against, replay_key_value_tail,
};
use super::{ComputeAction, WalStorage};
use crate::model::{SearchKey, SortedMapEntry, SortedMapKey};
use crate::test_support::fault_writer::{rollback_scripted, ScriptedWriter, WriterFault};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static DETERMINISTIC_CLOCK_NANOS: AtomicU64 = AtomicU64::new(0);

fn deterministic_clock_nanos() -> u64 {
    DETERMINISTIC_CLOCK_NANOS.load(Ordering::SeqCst)
}

#[test]
fn header_magic_is_strict() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(&encoded[..8], b"PIGWAL\r\n");
    assert!(V1CodecProbe::magic_is_valid(&encoded));

    for index in 0..8 {
        let mut corrupted = encoded;
        corrupted[index] ^= 0xff;
        assert!(
            !V1CodecProbe::magic_is_valid(&corrupted),
            "magic corruption at byte {index} must be rejected"
        );
    }
}

#[test]
fn header_version_is_strict() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(u16::from_le_bytes(encoded[8..10].try_into().unwrap()), 1);
    assert!(V1CodecProbe::version_is_valid(&encoded));

    let mut unsupported = encoded;
    unsupported[8..10].copy_from_slice(&2_u16.to_le_bytes());
    assert!(!V1CodecProbe::version_is_valid(&unsupported));
}

#[test]
fn v2_header_carries_segment_identity_and_global_base() {
    let segment_base = u64::from(u32::MAX) + 100;
    let encoded = V2CodecProbe::encode_header(V2HeaderProbeFields {
        kind: 2,
        granularity_nanos: 7,
        base_bucket: 11,
        segment_id: 3,
        segment_base,
    });

    assert_eq!(encoded.len(), V2CodecProbe::HEADER_LEN);
    assert!(V2CodecProbe::header_is_valid(&encoded));
    assert_eq!(V2CodecProbe::header_kind(&encoded), Some(2));
    assert_eq!(V2CodecProbe::header_granularity(&encoded), Some(7));
    assert_eq!(V2CodecProbe::header_base_bucket(&encoded), Some(11));
    assert_eq!(V2CodecProbe::header_segment_id(&encoded), Some(3));
    assert_eq!(
        V2CodecProbe::header_segment_base(&encoded),
        Some(segment_base)
    );
}

#[test]
fn header_length_is_strict() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(u16::from_le_bytes(encoded[10..12].try_into().unwrap()), 40);
    assert!(V1CodecProbe::header_length_is_valid(&encoded));

    let mut wrong_length = encoded;
    wrong_length[10..12].copy_from_slice(&39_u16.to_le_bytes());
    assert!(!V1CodecProbe::header_length_is_valid(&wrong_length));
}

#[test]
fn header_kind_is_strict() {
    for kind in 1_u8..=3 {
        let encoded = V1CodecProbe::encode_header_with_kind(kind);
        assert_eq!(encoded[12], kind);
        assert!(V1CodecProbe::kind_is_valid(&encoded));
    }

    for unsupported in [0_u8, 4, u8::MAX] {
        let mut encoded = V1CodecProbe::encode_header();
        encoded[12] = unsupported;
        assert!(!V1CodecProbe::kind_is_valid(&encoded));
    }
}

#[test]
fn header_timestamp_unit_is_strict() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(encoded[13], 1);
    assert!(V1CodecProbe::timestamp_unit_is_valid(&encoded));

    for unsupported in [0_u8, 2, u8::MAX] {
        let mut encoded = encoded;
        encoded[13] = unsupported;
        assert!(!V1CodecProbe::timestamp_unit_is_valid(&encoded));
    }
}

#[test]
fn header_granularity_is_nonzero() {
    for granularity in [1_u64, 60_000_000_000, u64::MAX] {
        let encoded = V1CodecProbe::encode_header_with_granularity(granularity);
        assert_eq!(
            u64::from_le_bytes(encoded[16..24].try_into().unwrap()),
            granularity
        );
        assert!(V1CodecProbe::granularity_is_valid(&encoded));
    }

    let zero = V1CodecProbe::encode_header_with_granularity(0);
    assert!(!V1CodecProbe::granularity_is_valid(&zero));
}

#[test]
fn default_clock_is_floored_to_one_minute_for_each_accepted_mutation() {
    let header = V1CodecProbe::encode_header();
    DETERMINISTIC_CLOCK_NANOS.store(179_999_999_999, Ordering::SeqCst);
    let wal = WalStorage::new_vec_based_v1_with_clock(&header, deterministic_clock_nanos);

    wal.store_put_event(b"first".to_vec(), b"one".to_vec());
    DETERMINISTIC_CLOCK_NANOS.store(180_000_000_001, Ordering::SeqCst);
    wal.store_put_event(b"second".to_vec(), b"two".to_vec());

    let state = wal.wal_state.read().unwrap();
    let first = &state.writer[V1CodecProbe::HEADER_LEN..];
    let first_length = V1CodecProbe::EMPTY_RECORD_LEN
        + u32::from_le_bytes(first[6..10].try_into().unwrap()) as usize;
    let second = &first[first_length..];
    assert_eq!(
        V1CodecProbe::record_timestamp_bucket(first),
        Some(120_000_000_000)
    );
    assert_eq!(
        V1CodecProbe::record_timestamp_bucket(second),
        Some(180_000_000_000)
    );
}

#[test]
fn reopen_restores_persisted_granularity_and_last_complete_bucket() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header_with_granularity(100);
    let payload =
        bincode::serialize(&KeyValueData::new(b"persisted".to_vec(), b"value".to_vec())).unwrap();
    let record = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: PUT_ACT,
        payload: &payload,
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 700,
    });
    let mut persisted = header.to_vec();
    persisted.extend_from_slice(&record);
    fs::write(&paths.active, persisted).unwrap();

    let initialized = initialize_snapshot(
        &paths,
        replay_key_value,
        replay_key_value_tail,
        replay_key_value_against,
        encode_key_value_snapshot,
        encode_key_value_repair_snapshot,
        key_value_is_proper_snapshot_prefix,
        Some(V1CodecProbe::encode_header()),
        None,
    )
    .unwrap();
    let state = initialized.wal.wal_state.read().unwrap();
    assert_eq!(state.granularity_nanos, 100);
    assert_eq!(state.last_bucket, 700);
}

#[test]
fn forward_equal_and_backward_clocks_never_decrease_across_restart() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header_with_granularity(100);
    fs::write(&paths.active, header).unwrap();

    let initialized = initialize_snapshot(
        &paths,
        replay_key_value,
        replay_key_value_tail,
        replay_key_value_against,
        encode_key_value_snapshot,
        encode_key_value_repair_snapshot,
        key_value_is_proper_snapshot_prefix,
        Some(V1CodecProbe::encode_header()),
        None,
    )
    .unwrap();
    let wal = initialized.wal;
    wal.wal_state.write().unwrap().clock = deterministic_clock_nanos;
    for (clock, key) in [
        (250, b"forward".as_slice()),
        (250, b"equal".as_slice()),
        (150, b"backward".as_slice()),
    ] {
        DETERMINISTIC_CLOCK_NANOS.store(clock, Ordering::SeqCst);
        wal.store_put_event(key.to_vec(), key.to_vec());
    }
    drop(wal);

    let reopened = initialize_snapshot(
        &paths,
        replay_key_value,
        replay_key_value_tail,
        replay_key_value_against,
        encode_key_value_snapshot,
        encode_key_value_repair_snapshot,
        key_value_is_proper_snapshot_prefix,
        Some(V1CodecProbe::encode_header()),
        None,
    )
    .unwrap();
    let wal = reopened.wal;
    wal.wal_state.write().unwrap().clock = deterministic_clock_nanos;
    for (clock, key) in [
        (99, b"restart-backward".as_slice()),
        (350, b"restart-forward".as_slice()),
    ] {
        DETERMINISTIC_CLOCK_NANOS.store(clock, Ordering::SeqCst);
        wal.store_put_event(key.to_vec(), key.to_vec());
    }
    drop(wal);

    let bytes = fs::read(&paths.active).unwrap();
    let mut cursor = V1CodecProbe::HEADER_LEN;
    let mut buckets = Vec::new();
    while cursor < bytes.len() {
        let frame = &bytes[cursor..];
        buckets.push(V1CodecProbe::record_timestamp_bucket(frame).unwrap());
        cursor += V1CodecProbe::EMPTY_RECORD_LEN
            + u32::from_le_bytes(frame[6..10].try_into().unwrap()) as usize;
    }
    assert_eq!(buckets, vec![200, 200, 200, 200, 300]);
}

#[test]
fn rejected_write_or_flush_does_not_advance_the_accepted_bucket() {
    for fault in [WriterFault::WriteCall(1), WriterFault::FlushCall(1)] {
        let (writer, _observed) = ScriptedWriter::new(fault, false);
        let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);
        {
            let mut state = wal.wal_state.write().unwrap();
            state.granularity_nanos = 100;
            state.last_bucket = 100;
            state.clock = deterministic_clock_nanos;
        }
        DETERMINISTIC_CLOCK_NANOS.store(250, Ordering::SeqCst);

        assert!(wal
            .try_store_put_event(b"rejected".to_vec(), b"value".to_vec())
            .is_err());
        assert_eq!(wal.wal_state.read().unwrap().last_bucket, 100, "{fault:?}");
    }
}

#[test]
fn repair_compaction_preserves_nonempty_and_header_only_timestamp_metadata() {
    for (base_bucket, accepted_bucket) in [(0_u64, Some(700_u64)), (500, None)] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let mut header = V1CodecProbe::encode_header_with_granularity(100);
        header[24..32].copy_from_slice(&base_bucket.to_le_bytes());
        let header_crc = crc32fast::hash(&header[..36]);
        header[36..40].copy_from_slice(&header_crc.to_le_bytes());
        let mut interrupted = header.to_vec();

        if let Some(timestamp_bucket) = accepted_bucket {
            let accepted_payload =
                bincode::serialize(&KeyValueData::new(b"accepted".to_vec(), b"value".to_vec()))
                    .unwrap();
            interrupted.extend_from_slice(&V1CodecProbe::encode_complete_record(
                RecordProbeFields {
                    action: PUT_ACT,
                    payload: &accepted_payload,
                    physical_start: interrupted.len() as u32,
                    mutation_start: interrupted.len() as u32,
                    index: 0,
                    count: 1,
                    timestamp_bucket,
                },
            ));
        }

        let torn_payload =
            bincode::serialize(&KeyValueData::new(b"torn".to_vec(), b"discard".to_vec())).unwrap();
        let torn = V1CodecProbe::encode_complete_record(RecordProbeFields {
            action: PUT_ACT,
            payload: &torn_payload,
            physical_start: interrupted.len() as u32,
            mutation_start: interrupted.len() as u32,
            index: 0,
            count: 1,
            timestamp_bucket: accepted_bucket.unwrap_or(base_bucket) + 100,
        });
        interrupted.extend_from_slice(&torn[..torn.len() - 1]);
        fs::write(&paths.active, interrupted).unwrap();

        let repaired = initialize_snapshot(
            &paths,
            replay_key_value,
            replay_key_value_tail,
            replay_key_value_against,
            encode_key_value_snapshot,
            encode_key_value_repair_snapshot,
            key_value_is_proper_snapshot_prefix,
            Some(V1CodecProbe::encode_header()),
            None,
        )
        .unwrap();
        assert_eq!(repaired.status, crate::RecoveryStatus::Recovered);
        drop(repaired);

        let persisted = fs::read(&paths.active).unwrap();
        let replayed = replay_key_value(&persisted).unwrap();
        let expected_last = accepted_bucket.unwrap_or(base_bucket);
        assert_eq!(replayed.granularity_nanos, 100);
        assert_eq!(replayed.last_bucket, expected_last);
        assert_eq!(
            u64::from_le_bytes(persisted[24..32].try_into().unwrap()),
            expected_last
        );
    }
}

#[test]
fn explicit_internal_granularity_change_compacts_without_losing_state_or_time() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header_with_granularity(100);
    let payload =
        bincode::serialize(&KeyValueData::new(b"persisted".to_vec(), b"value".to_vec())).unwrap();
    let record = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: PUT_ACT,
        payload: &payload,
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 700,
    });
    let mut original = header.to_vec();
    original.extend_from_slice(&record);
    fs::write(&paths.active, &original).unwrap();

    let changed = initialize_snapshot(
        &paths,
        replay_key_value,
        replay_key_value_tail,
        replay_key_value_against,
        encode_key_value_snapshot,
        encode_key_value_repair_snapshot,
        key_value_is_proper_snapshot_prefix,
        Some(V1CodecProbe::encode_header()),
        Some(250),
    )
    .unwrap();
    assert_eq!(
        changed.snapshot.get(b"persisted".as_slice()),
        Some(&b"value".to_vec())
    );
    drop(changed);

    let persisted = fs::read(&paths.active).unwrap();
    assert_ne!(persisted, original);
    assert!(!paths.staging.exists());
    let replayed = replay_key_value(&persisted).unwrap();
    assert_eq!(replayed.granularity_nanos, 250);
    assert_eq!(replayed.last_bucket, 700);
    assert_eq!(
        replayed.snapshot.get(b"persisted".as_slice()),
        Some(&b"value".to_vec())
    );
    assert_eq!(
        u64::from_le_bytes(persisted[24..32].try_into().unwrap()),
        700
    );
}

#[test]
fn header_base_bucket_round_trips() {
    for base_bucket in [0_u64, 1, 60_000_000_000, u64::MAX] {
        let encoded = V1CodecProbe::encode_header_with_base_bucket(base_bucket);
        assert_eq!(
            u64::from_le_bytes(encoded[24..32].try_into().unwrap()),
            base_bucket
        );
        assert_eq!(V1CodecProbe::base_bucket(&encoded), Some(base_bucket));
    }
}

#[test]
fn header_flags_are_strict() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(u16::from_le_bytes(encoded[14..16].try_into().unwrap()), 0);
    assert!(V1CodecProbe::flags_are_valid(&encoded));

    for unsupported in [1_u16, u16::MAX] {
        let mut encoded = encoded;
        encoded[14..16].copy_from_slice(&unsupported.to_le_bytes());
        assert!(!V1CodecProbe::flags_are_valid(&encoded));
    }
}

#[test]
fn header_reserved_is_strict() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(&encoded[32..36], &[0; 4]);
    assert!(V1CodecProbe::reserved_is_valid(&encoded));

    for index in 32..36 {
        let mut corrupted = encoded;
        corrupted[index] = 1;
        assert!(
            !V1CodecProbe::reserved_is_valid(&corrupted),
            "reserved byte {index} must remain zero"
        );
    }
}

#[test]
fn header_crc_covers_prefix() {
    let encoded_variants = [
        V1CodecProbe::encode_header(),
        V1CodecProbe::encode_header_with_kind(3),
        V1CodecProbe::encode_header_with_granularity(1),
        V1CodecProbe::encode_header_with_base_bucket(u64::MAX),
    ];

    for encoded in encoded_variants {
        let stored = u32::from_le_bytes(encoded[36..40].try_into().unwrap());
        assert_eq!(stored, crc32fast::hash(&encoded[..36]));
        assert!(V1CodecProbe::header_crc_is_valid(&encoded));

        for index in 0..40 {
            let mut corrupted = encoded;
            corrupted[index] ^= 0x01;
            assert!(
                !V1CodecProbe::header_crc_is_valid(&corrupted),
                "CRC must cover header byte {index}"
            );
        }
    }
}

#[test]
fn partial_file_header_is_invalid() {
    let encoded = V1CodecProbe::encode_header();
    assert_eq!(
        V1CodecProbe::classify_header(&encoded),
        HeaderProbeClassification::Valid
    );

    for available in 1..V1CodecProbe::HEADER_LEN {
        assert_eq!(
            V1CodecProbe::classify_header(&encoded[..available]),
            HeaderProbeClassification::Invalid,
            "a {available}-byte V1 header prefix must be preserved as invalid"
        );
    }
}

#[test]
fn record_marker_is_strict() {
    let encoded = V1CodecProbe::encode_record();
    assert_eq!(&encoded[..2], &[0xa7, 0xd1]);
    assert!(V1CodecProbe::record_marker_is_valid(&encoded));

    for index in 0..2 {
        let mut corrupted = encoded.clone();
        corrupted[index] ^= 0xff;
        assert!(!V1CodecProbe::record_marker_is_valid(&corrupted));
    }
}

#[test]
fn record_version_is_strict() {
    let encoded = V1CodecProbe::encode_record();
    assert_eq!(encoded[2], 1);
    assert!(V1CodecProbe::record_version_is_valid(&encoded));

    for unsupported in [0_u8, 2, u8::MAX] {
        let mut encoded = encoded.clone();
        encoded[2] = unsupported;
        assert!(!V1CodecProbe::record_version_is_valid(&encoded));
    }
}

#[test]
fn record_action_is_strict() {
    for action in 0_u8..=5 {
        let encoded = V1CodecProbe::encode_record_with_action(action);
        assert_eq!(encoded[3], action);
        assert!(V1CodecProbe::record_action_is_valid(&encoded));
    }

    for unsupported in [6_u8, u8::MAX] {
        let mut encoded = V1CodecProbe::encode_record();
        encoded[3] = unsupported;
        assert!(!V1CodecProbe::record_action_is_valid(&encoded));
    }
}

#[test]
fn record_header_length_is_strict() {
    let encoded = V1CodecProbe::encode_record();
    assert_eq!(u16::from_le_bytes(encoded[4..6].try_into().unwrap()), 38);
    assert!(V1CodecProbe::record_header_length_is_valid(&encoded));

    for unsupported in [0_u16, 37, 39, u16::MAX] {
        let mut encoded = encoded.clone();
        encoded[4..6].copy_from_slice(&unsupported.to_le_bytes());
        assert!(!V1CodecProbe::record_header_length_is_valid(&encoded));
    }
}

#[test]
fn record_length_complement_and_bounds_are_checked() {
    for payload in [Vec::new(), vec![1, 2, 3], vec![0x5a; 257]] {
        let encoded = V1CodecProbe::encode_record_with_payload(&payload);
        assert_eq!(
            encoded.len(),
            V1CodecProbe::EMPTY_RECORD_LEN + payload.len()
        );
        let length = u32::from_le_bytes(encoded[6..10].try_into().unwrap());
        let complement = u32::from_le_bytes(encoded[10..14].try_into().unwrap());
        assert_eq!(length, payload.len() as u32);
        assert_eq!(complement, !length);
        assert!(V1CodecProbe::record_length_complement_is_valid(&encoded));
    }

    let mut contradictory = V1CodecProbe::encode_record_with_payload(b"payload");
    contradictory[10] ^= 1;
    assert!(!V1CodecProbe::record_length_complement_is_valid(
        &contradictory
    ));
}

#[test]
fn record_offset_overflow_is_explicit() {
    assert_eq!(V1CodecProbe::checked_record_end(40, 3, 49), Ok(89));
    assert_eq!(
        V1CodecProbe::checked_record_end(40, 3, 48),
        Err(RecordBoundsError::Truncated)
    );
    assert_eq!(
        V1CodecProbe::checked_record_end(u32::MAX - 45, 0, 46),
        Err(RecordBoundsError::Overflow)
    );
    assert_eq!(
        V1CodecProbe::checked_record_end(u32::MAX - 46, 0, 46),
        Ok(u32::MAX)
    );
}

#[test]
fn v2_record_offsets_extend_beyond_the_v1_u32_boundary() {
    let physical_start = u64::from(u32::MAX) + 17;
    let payload = b"beyond-v1";
    let encoded = V2CodecProbe::encode_complete_record(V2RecordProbeFields {
        action: PUT_ACT,
        payload,
        physical_start,
        mutation_start: physical_start,
        index: 0,
        count: 1,
        timestamp_bucket: 9,
    });

    assert_eq!(
        encoded.len(),
        V2CodecProbe::EMPTY_RECORD_LEN + payload.len()
    );
    assert!(V2CodecProbe::record_physical_start_is_valid(
        &encoded,
        physical_start
    ));
    assert_eq!(
        V2CodecProbe::checked_record_end(physical_start, payload.len() as u64, encoded.len()),
        Ok(physical_start + encoded.len() as u64)
    );
}

#[test]
fn record_physical_start_and_footer_match() {
    let physical_start = 1234_u32;
    let payload = b"positioned";
    let encoded = V1CodecProbe::encode_record_at(physical_start, payload);
    let footer_start = 38 + payload.len();

    assert_eq!(
        u32::from_le_bytes(encoded[14..18].try_into().unwrap()),
        physical_start
    );
    assert_eq!(
        u32::from_le_bytes(encoded[footer_start..footer_start + 4].try_into().unwrap()),
        physical_start
    );
    assert!(V1CodecProbe::record_physical_start_is_valid(
        &encoded,
        physical_start
    ));
    assert!(!V1CodecProbe::record_physical_start_is_valid(
        &encoded,
        physical_start + 1
    ));

    for field_start in [14, footer_start] {
        let mut corrupted = encoded.clone();
        corrupted[field_start] ^= 1;
        assert!(!V1CodecProbe::record_physical_start_is_valid(
            &corrupted,
            physical_start
        ));
    }
}

#[test]
fn record_mutation_start_is_strict() {
    let first = V1CodecProbe::encode_record_with_mutation_start(40, 40, b"first");
    assert_eq!(u32::from_le_bytes(first[18..22].try_into().unwrap()), 40);
    assert!(V1CodecProbe::record_mutation_start_is_valid(&first));

    let continuation = V1CodecProbe::encode_record_with_mutation_start(100, 40, b"next");
    assert_eq!(
        u32::from_le_bytes(continuation[18..22].try_into().unwrap()),
        40
    );
    assert!(V1CodecProbe::record_mutation_start_is_valid(&continuation));

    for invalid in [0_u32, 39, 101] {
        let mut encoded = continuation.clone();
        encoded[18..22].copy_from_slice(&invalid.to_le_bytes());
        assert!(!V1CodecProbe::record_mutation_start_is_valid(&encoded));
    }
}

#[test]
fn record_index_count_are_strict() {
    for (index, count) in [(0_u32, 1_u32), (0, 3), (1, 3), (2, 3)] {
        let encoded = V1CodecProbe::encode_record_with_group(index, count);
        assert_eq!(
            u32::from_le_bytes(encoded[22..26].try_into().unwrap()),
            index
        );
        assert_eq!(
            u32::from_le_bytes(encoded[26..30].try_into().unwrap()),
            count
        );
        assert!(V1CodecProbe::record_index_count_are_valid(&encoded));
    }

    for (index, count) in [(0_u32, 0_u32), (1, 1), (3, 3), (u32::MAX, 2)] {
        let mut encoded = V1CodecProbe::encode_record();
        encoded[22..26].copy_from_slice(&index.to_le_bytes());
        encoded[26..30].copy_from_slice(&count.to_le_bytes());
        assert!(!V1CodecProbe::record_index_count_are_valid(&encoded));
    }
}

#[test]
fn record_timestamp_is_strict() {
    for timestamp_bucket in [0_u64, 1, 60_000_000_000, u64::MAX] {
        let encoded = V1CodecProbe::encode_record_with_timestamp(timestamp_bucket);
        assert_eq!(
            u64::from_le_bytes(encoded[30..38].try_into().unwrap()),
            timestamp_bucket
        );
        assert_eq!(
            V1CodecProbe::record_timestamp_bucket(&encoded),
            Some(timestamp_bucket)
        );
    }
}

#[test]
fn record_payload_is_strict() {
    let key_value = bincode::serialize(&KeyValueData::new(b"key".to_vec(), b"value".to_vec()))
        .expect("serialize key/value payload");
    let map_put = bincode::serialize(&SortedMapEntry::new(
        b"key".to_vec(),
        7_usize.into(),
        b"value".to_vec(),
    ))
    .expect("serialize map put payload");
    let map_remove = bincode::serialize(&SortedMapKey::new(b"key".to_vec(), 7_usize.into()))
        .expect("serialize map remove payload");

    for (kind, action, payload) in [
        (1_u8, 0_u8, b"raw-key".as_slice()),
        (1, 1, key_value.as_slice()),
        (2, 0, b"raw-key".as_slice()),
        (2, 2, key_value.as_slice()),
        (2, 3, key_value.as_slice()),
        (3, 0, b"raw-key".as_slice()),
        (3, 4, map_put.as_slice()),
        (3, 5, map_remove.as_slice()),
    ] {
        assert!(
            V1CodecProbe::payload_is_valid(kind, action, payload),
            "kind {kind}, action {action} should accept its legacy payload"
        );
    }

    for (kind, action, payload) in [
        (1_u8, 2_u8, key_value.as_slice()),
        (2, 1, key_value.as_slice()),
        (3, 1, key_value.as_slice()),
        (1, 1, b"malformed".as_slice()),
        (2, 2, b"malformed".as_slice()),
        (3, 4, b"malformed".as_slice()),
        (3, 5, b"malformed".as_slice()),
        (0, 0, b"raw-key".as_slice()),
        (4, 0, b"raw-key".as_slice()),
        (1, 6, b"raw-key".as_slice()),
    ] {
        assert!(!V1CodecProbe::payload_is_valid(kind, action, payload));
    }
}

#[test]
fn record_crc_covers_envelope() {
    let variants = [
        V1CodecProbe::encode_record(),
        V1CodecProbe::encode_record_with_action(5),
        V1CodecProbe::encode_record_with_payload(b"payload"),
        V1CodecProbe::encode_record_at(1234, b"positioned"),
        V1CodecProbe::encode_record_with_mutation_start(1234, 40, b"group"),
        V1CodecProbe::encode_record_with_group(2, 3),
        V1CodecProbe::encode_record_with_timestamp(u64::MAX),
    ];

    for encoded in variants {
        let payload_len = u32::from_le_bytes(encoded[6..10].try_into().unwrap()) as usize;
        let crc_start = 42 + payload_len;
        let stored = u32::from_le_bytes(encoded[crc_start..crc_start + 4].try_into().unwrap());
        assert_eq!(stored, crc32fast::hash(&encoded[..crc_start]));
        assert!(V1CodecProbe::record_crc_is_valid(&encoded));

        for index in 0..encoded.len() {
            let mut corrupted = encoded.clone();
            corrupted[index] ^= 1;
            assert!(
                !V1CodecProbe::record_crc_is_valid(&corrupted),
                "record CRC must cover byte {index}"
            );
        }
    }
}

#[test]
fn complete_single_record_matches_golden_frame() {
    let encoded = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 0,
        payload: b"k",
        physical_start: 40,
        mutation_start: 40,
        index: 0,
        count: 1,
        timestamp_bucket: 0x0102_0304_0506_0708,
    });
    let golden = [
        0xa7, 0xd1, 0x01, 0x00, 0x26, 0x00, 0x01, 0x00, 0x00, 0x00, 0xfe, 0xff, 0xff, 0xff, 0x28,
        0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x6b, 0x28, 0x00, 0x00, 0x00, 0xed, 0x4c,
        0xf9, 0xc9,
    ];

    assert_eq!(encoded, golden);
}

#[test]
fn fresh_invalid_options_and_candidates_do_not_mutate() {
    let missing_directory = tempfile::tempdir().unwrap().path().join("not-created");
    let missing_paths = ArtifactPaths::new(&missing_directory, StoreKind::Value);
    let invalid = inspect_fresh_candidate(
        &missing_paths,
        FreshOptionsProbe {
            kind: StoreKind::Value,
            granularity_nanos: 0,
        },
    )
    .unwrap();
    assert_eq!(invalid, FreshInspection::InvalidOptions);
    assert!(!missing_directory.exists(), "validation must precede I/O");

    for role in [
        FreshCandidateRole::Active,
        FreshCandidateRole::Recovery,
        FreshCandidateRole::Staging,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let candidate = match role {
            FreshCandidateRole::Active => paths.active.clone(),
            FreshCandidateRole::Recovery => paths.legacy.clone(),
            FreshCandidateRole::Staging => paths.staging.clone(),
        };
        let evidence = format!("{role:?}-evidence").into_bytes();
        fs::write(&candidate, &evidence).unwrap();

        let inspected = inspect_fresh_candidate(
            &paths,
            FreshOptionsProbe {
                kind: StoreKind::Value,
                granularity_nanos: 60_000_000_000,
            },
        )
        .unwrap();

        assert_eq!(
            inspected,
            FreshInspection::Existing {
                role,
                path: candidate.clone(),
            }
        );
        assert_eq!(fs::read(candidate).unwrap(), evidence);
    }

    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Map);
    assert_eq!(
        inspect_fresh_candidate(
            &paths,
            FreshOptionsProbe {
                kind: StoreKind::Map,
                granularity_nanos: 1,
            },
        )
        .unwrap(),
        FreshInspection::Ready
    );
}

#[test]
fn fresh_staging_cleanup_is_role_bounded() {
    for inject_failure in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let unrelated = directory.path().join("unrelated.evidence");
        let staging_bytes = b"invocation-owned-staging";
        fs::write(&paths.staging, staging_bytes).unwrap();
        fs::write(&unrelated, b"untouched").unwrap();

        let mut registry = FreshCleanupRegistry::default();
        assert!(!registry.register_staging(&paths, &paths.active));
        assert!(!registry.register_staging(&paths, &paths.legacy));
        assert!(!registry.register_staging(&paths, &unrelated));
        assert!(registry.register_staging(&paths, &paths.staging));
        assert_eq!(registry.registered(), std::slice::from_ref(&paths.staging));

        let fault = inject_failure.then(|| fail_cleanup_for(paths.staging.clone()));
        let result = registry.cleanup();
        drop(fault);

        assert!(!paths.active.exists());
        assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
        assert_eq!(fs::read(&unrelated).unwrap(), b"untouched");
        if inject_failure {
            match result {
                Err(crate::RecoveryError::Io {
                    operation,
                    path,
                    source: _,
                }) => {
                    assert_eq!(operation, crate::RecoveryOperation::Cleanup);
                    assert_eq!(path, paths.staging);
                }
                other => panic!("unexpected cleanup result: {other:?}"),
            }
            assert_eq!(fs::read(&paths.staging).unwrap(), staging_bytes);
            assert_eq!(registry.registered(), std::slice::from_ref(&paths.staging));
        } else {
            result.unwrap();
            assert!(!paths.staging.exists());
            assert!(registry.registered().is_empty());
        }
    }
}

#[test]
fn fresh_staging_create_registration_is_atomic() {
    let blocked_directory = tempfile::tempdir().unwrap();
    let blocked_paths = ArtifactPaths::new(blocked_directory.path(), StoreKind::Value);
    fs::write(&blocked_paths.staging, b"pre-existing").unwrap();
    let mut blocked_registry = FreshCleanupRegistry::default();
    match create_fresh_staging(&blocked_paths, &mut blocked_registry) {
        Err(crate::RecoveryError::Io {
            operation,
            path,
            source: _,
        }) => {
            assert_eq!(operation, crate::RecoveryOperation::CreateStaging);
            assert_eq!(path, blocked_paths.staging);
        }
        other => panic!("unexpected create result: {other:?}"),
    }
    assert!(!blocked_paths.active.exists());
    assert_eq!(fs::read(&blocked_paths.staging).unwrap(), b"pre-existing");
    assert!(blocked_registry.registered().is_empty());
    assert!(blocked_registry.attempted().is_empty());

    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let mut registry = FreshCleanupRegistry::default();
    let handle = create_fresh_staging(&paths, &mut registry).expect("exclusive staging create");
    assert!(!paths.active.exists());
    assert!(paths.staging.is_file());
    assert_eq!(handle.metadata().unwrap().len(), 0);
    assert_eq!(registry.registered(), std::slice::from_ref(&paths.staging));
    assert!(registry.attempted().is_empty());
    drop(handle);
    registry.cleanup().unwrap();
}

#[test]
fn fresh_header_each_write_cut_leaves_active_absent() {
    let header = V1CodecProbe::encode_header();
    for written_len in 0..V1CodecProbe::HEADER_LEN {
        for inject_cleanup_failure in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
            let mut registry = FreshCleanupRegistry::default();
            let staging = create_fresh_staging(&paths, &mut registry).unwrap();
            let cleanup_fault =
                inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

            let failure = write_fresh_header_prefix(staging, &header, written_len, &mut registry)
                .expect_err("every short header write must fail before publication");
            drop(cleanup_fault);

            assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
            assert_eq!(failure.path, paths.staging);
            assert_eq!(
                failure.cleanup_path,
                inject_cleanup_failure.then(|| paths.staging.clone())
            );
            assert!(!paths.active.exists());
            assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
            if inject_cleanup_failure {
                assert_eq!(
                    fs::read(&paths.staging).unwrap(),
                    header[..written_len],
                    "diagnostic staging bytes must match cut {written_len}"
                );
                assert_eq!(registry.registered(), std::slice::from_ref(&paths.staging));
            } else {
                assert!(!paths.staging.exists());
                assert!(registry.registered().is_empty());
            }
        }
    }
}

#[test]
fn fresh_header_flush_failure_leaves_active_absent() {
    let header = V1CodecProbe::encode_header();
    for inject_cleanup_failure in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let mut registry = FreshCleanupRegistry::default();
        let staging = create_fresh_staging(&paths, &mut registry).unwrap();
        let staging = write_fresh_header_prefix(staging, &header, header.len(), &mut registry)
            .expect("complete header write");
        let cleanup_fault = inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

        let failure = flush_fresh_header(staging, true, &mut registry)
            .expect_err("injected flush failure must fail before publication");
        drop(cleanup_fault);

        assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
        assert_eq!(failure.path, paths.staging);
        assert_eq!(
            failure.cleanup_path,
            inject_cleanup_failure.then(|| paths.staging.clone())
        );
        assert!(!paths.active.exists());
        assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
        if inject_cleanup_failure {
            assert_eq!(fs::read(&paths.staging).unwrap(), header);
        } else {
            assert!(!paths.staging.exists());
        }
    }
}

#[test]
fn fresh_header_read_failure_leaves_active_absent() {
    let header = V1CodecProbe::encode_header();
    for (inject_read_failure, truncated_len) in [(true, None), (false, Some(39_u64))] {
        for inject_cleanup_failure in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
            let mut registry = FreshCleanupRegistry::default();
            let staging = create_fresh_staging(&paths, &mut registry).unwrap();
            let staging =
                write_fresh_header_prefix(staging, &header, header.len(), &mut registry).unwrap();
            let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
            if let Some(length) = truncated_len {
                staging.set_len(length).unwrap();
            }
            let cleanup_fault =
                inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

            let failure = readback_fresh_header(staging, inject_read_failure, &mut registry)
                .expect_err("read or exact-length fault must fail before publication");
            drop(cleanup_fault);

            assert_eq!(failure.operation, crate::RecoveryOperation::Open);
            assert_eq!(failure.path, paths.staging);
            assert_eq!(
                failure.cleanup_path,
                inject_cleanup_failure.then(|| paths.staging.clone())
            );
            assert!(!paths.active.exists());
            assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
            if inject_cleanup_failure {
                let expected =
                    truncated_len.map_or(header.as_slice(), |length| &header[..length as usize]);
                assert_eq!(fs::read(&paths.staging).unwrap(), expected);
            } else {
                assert!(!paths.staging.exists());
            }
        }
    }
}

#[test]
fn fresh_header_validation_failure_leaves_active_absent() {
    let expected = V1CodecProbe::encode_header();
    let mut invalid_magic = expected;
    invalid_magic[0] ^= 0xff;
    let mut invalid_version = expected;
    invalid_version[8] ^= 0xff;
    let mut invalid_length = expected;
    invalid_length[10] = 39;
    let mut invalid_unit = expected;
    invalid_unit[13] = 2;
    let mut invalid_flags = expected;
    invalid_flags[14] = 1;
    let mut invalid_reserved = expected;
    invalid_reserved[32] = 1;
    let mut invalid_crc = expected;
    invalid_crc[39] ^= 0xff;
    let cases = [
        ("magic", invalid_magic),
        ("version", invalid_version),
        ("length", invalid_length),
        (
            "kind configuration",
            V1CodecProbe::encode_header_with_kind(2),
        ),
        ("timestamp unit", invalid_unit),
        ("flags", invalid_flags),
        (
            "zero granularity",
            V1CodecProbe::encode_header_with_granularity(0),
        ),
        (
            "granularity configuration",
            V1CodecProbe::encode_header_with_granularity(1),
        ),
        (
            "base-bucket configuration",
            V1CodecProbe::encode_header_with_base_bucket(1),
        ),
        ("reserved", invalid_reserved),
        ("crc", invalid_crc),
    ];

    for (case, persisted) in cases {
        for inject_cleanup_failure in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
            let mut registry = FreshCleanupRegistry::default();
            let staging = create_fresh_staging(&paths, &mut registry).unwrap();
            let staging =
                write_fresh_header_prefix(staging, &persisted, persisted.len(), &mut registry)
                    .unwrap();
            let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
            let (staging, readback) = readback_fresh_header(staging, false, &mut registry).unwrap();
            let cleanup_fault =
                inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

            let failure = match validate_fresh_header(staging, &readback, &expected, &mut registry)
            {
                Ok(_) => panic!("{case} mismatch must fail before publication"),
                Err(failure) => failure,
            };
            drop(cleanup_fault);

            assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
            assert_eq!(failure.path, paths.staging);
            assert_eq!(
                failure.cleanup_path,
                inject_cleanup_failure.then(|| paths.staging.clone())
            );
            assert!(!paths.active.exists());
            assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
            if inject_cleanup_failure {
                assert_eq!(fs::read(&paths.staging).unwrap(), persisted, "{case}");
            } else {
                assert!(!paths.staging.exists(), "{case}");
            }
        }
    }
}

#[test]
fn fresh_header_sync_failure_leaves_active_absent() {
    let header = V1CodecProbe::encode_header();
    for inject_cleanup_failure in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let mut registry = FreshCleanupRegistry::default();
        let staging = create_fresh_staging(&paths, &mut registry).unwrap();
        let staging =
            write_fresh_header_prefix(staging, &header, header.len(), &mut registry).unwrap();
        let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
        let (staging, persisted) = readback_fresh_header(staging, false, &mut registry).unwrap();
        let staging = validate_fresh_header(staging, &persisted, &header, &mut registry).unwrap();
        let cleanup_fault = inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

        let failure = match sync_fresh_header(staging, true, &mut registry) {
            Ok(_) => panic!("synchronization fault must fail before publication"),
            Err(failure) => failure,
        };
        drop(cleanup_fault);

        assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
        assert_eq!(failure.path, paths.staging);
        assert_eq!(
            failure.cleanup_path,
            inject_cleanup_failure.then(|| paths.staging.clone())
        );
        assert!(!paths.active.exists());
        assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
        if inject_cleanup_failure {
            assert_eq!(fs::read(&paths.staging).unwrap(), header);
        } else {
            assert!(!paths.staging.exists());
        }
    }
}

#[test]
fn fresh_append_handoff_failure_leaves_active_absent() {
    let header = V1CodecProbe::encode_header();
    for inject_cleanup_failure in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let mut registry = FreshCleanupRegistry::default();
        let staging = create_fresh_staging(&paths, &mut registry).unwrap();
        let staging =
            write_fresh_header_prefix(staging, &header, header.len(), &mut registry).unwrap();
        let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
        let (staging, persisted) = readback_fresh_header(staging, false, &mut registry).unwrap();
        let staging = validate_fresh_header(staging, &persisted, &header, &mut registry).unwrap();
        let staging = sync_fresh_header(staging, false, &mut registry).unwrap();
        let cleanup_fault = inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

        let failure = match prepare_fresh_append(staging, true, &mut registry) {
            Ok(_) => panic!("append-handle preparation fault must fail before publication"),
            Err(failure) => failure,
        };
        drop(cleanup_fault);

        assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
        assert_eq!(failure.path, paths.staging);
        assert_eq!(
            failure.cleanup_path,
            inject_cleanup_failure.then(|| paths.staging.clone())
        );
        assert!(!paths.active.exists());
        assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
        if inject_cleanup_failure {
            assert_eq!(fs::read(&paths.staging).unwrap(), header);
        } else {
            assert!(!paths.staging.exists());
        }
    }
}

#[test]
fn fresh_header_publish_failure_leaves_active_absent() {
    let header = V1CodecProbe::encode_header();
    for inject_cleanup_failure in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let mut registry = FreshCleanupRegistry::default();
        let staging = create_fresh_staging(&paths, &mut registry).unwrap();
        let staging =
            write_fresh_header_prefix(staging, &header, header.len(), &mut registry).unwrap();
        let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
        let (staging, persisted) = readback_fresh_header(staging, false, &mut registry).unwrap();
        let staging = validate_fresh_header(staging, &persisted, &header, &mut registry).unwrap();
        let staging = sync_fresh_header(staging, false, &mut registry).unwrap();
        let staging = prepare_fresh_append(staging, false, &mut registry).unwrap();
        let cleanup_fault = inject_cleanup_failure.then(|| fail_cleanup_for(paths.staging.clone()));

        let failure = match publish_fresh_header(staging, &paths, true, &mut registry) {
            Ok(_) => panic!("publish fault must fail before the active commit point"),
            Err(failure) => failure,
        };
        drop(cleanup_fault);

        assert_eq!(failure.operation, crate::RecoveryOperation::Publish);
        assert_eq!(failure.path, paths.active);
        assert_eq!(
            failure.cleanup_path,
            inject_cleanup_failure.then(|| paths.staging.clone())
        );
        assert!(!paths.active.exists());
        assert_eq!(registry.attempted(), std::slice::from_ref(&paths.staging));
        if inject_cleanup_failure {
            assert_eq!(fs::read(&paths.staging).unwrap(), header);
        } else {
            assert!(!paths.staging.exists());
        }
    }
}

#[test]
fn fresh_post_commit_interruption_is_valid() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header();
    let mut registry = FreshCleanupRegistry::default();
    let staging = create_fresh_staging(&paths, &mut registry).unwrap();
    let staging = write_fresh_header_prefix(staging, &header, header.len(), &mut registry).unwrap();
    let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
    let (staging, persisted) = readback_fresh_header(staging, false, &mut registry).unwrap();
    let staging = validate_fresh_header(staging, &persisted, &header, &mut registry).unwrap();
    let staging = sync_fresh_header(staging, false, &mut registry).unwrap();
    let staging = prepare_fresh_append(staging, false, &mut registry).unwrap();

    let published = publish_fresh_header(staging, &paths, false, &mut registry).unwrap();
    assert!(!paths.staging.exists());
    assert!(registry.registered().is_empty());
    assert!(registry.attempted().is_empty());

    drop(published);

    let active = fs::read(&paths.active).unwrap();
    assert_eq!(active, header);
    assert!(V1CodecProbe::magic_is_valid(&active));
    assert!(V1CodecProbe::version_is_valid(&active));
    assert!(V1CodecProbe::header_length_is_valid(&active));
    assert!(V1CodecProbe::kind_is_valid(&active));
    assert!(V1CodecProbe::timestamp_unit_is_valid(&active));
    assert!(V1CodecProbe::flags_are_valid(&active));
    assert!(V1CodecProbe::granularity_is_valid(&active));
    assert!(V1CodecProbe::reserved_is_valid(&active));
    assert!(V1CodecProbe::header_crc_is_valid(&active));

    let mut next_start = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.active)
        .expect("next startup can open the committed active artifact");
    assert_eq!(
        next_start.seek(SeekFrom::End(0)).unwrap(),
        V1CodecProbe::HEADER_LEN as u64
    );
}

#[test]
fn fresh_prepared_handle_handoff_is_infallible() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let moved_active = directory.path().join("moved-active.wal");
    let header = V1CodecProbe::encode_header();
    let mut registry = FreshCleanupRegistry::default();
    let staging = create_fresh_staging(&paths, &mut registry).unwrap();
    let staging = write_fresh_header_prefix(staging, &header, header.len(), &mut registry).unwrap();
    let staging = flush_fresh_header(staging, false, &mut registry).unwrap();
    let (staging, persisted) = readback_fresh_header(staging, false, &mut registry).unwrap();
    let staging = validate_fresh_header(staging, &persisted, &header, &mut registry).unwrap();
    let staging = sync_fresh_header(staging, false, &mut registry).unwrap();
    let staging = prepare_fresh_append(staging, false, &mut registry).unwrap();
    let published = publish_fresh_header(staging, &paths, false, &mut registry).unwrap();

    fs::rename(&paths.active, &moved_active).unwrap();
    let mut handle = match handoff_fresh_handle(published) {
        Ok(handle) => handle,
        Err(_) => panic!("published prepared handle must transfer without filesystem I/O"),
    };
    assert_eq!(
        handle.stream_position().unwrap(),
        V1CodecProbe::HEADER_LEN as u64
    );

    let first_record = V1CodecProbe::encode_record();
    handle.write_all(&first_record).unwrap();
    handle.flush().unwrap();
    let mut expected = header.to_vec();
    expected.extend_from_slice(&first_record);
    assert_eq!(fs::read(moved_active).unwrap(), expected);
}

#[test]
fn vector_backed_storage_exposes_only_a_complete_v1_header() {
    let header = V1CodecProbe::encode_header();

    let wal = WalStorage::new_vec_based_v1(&header);
    let state = wal.wal_state.read().unwrap();

    assert_eq!(state.writer, header);
    assert_eq!(state.offset, V1CodecProbe::HEADER_LEN as u64);
    assert!(V1CodecProbe::header_crc_is_valid(&state.writer));
}

#[test]
fn value_single_action_uses_one_complete_v1_frame_and_flush() {
    let (writer, observed) = ScriptedWriter::new(WriterFault::WriteCall(usize::MAX), false);
    let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);
    let key = b"key".to_vec();
    let value = b"value".to_vec();
    let payload = bincode::serialize(&KeyValueData::new(key.clone(), value.clone())).unwrap();
    let expected = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 1,
        payload: &payload,
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 0,
    });

    wal.try_store_put_event(key, value).unwrap();

    assert_eq!(observed.bytes(), expected);
    assert_eq!(observed.flush_calls(), 1);
}

#[test]
fn set_compute_batch_uses_contiguous_v1_group_and_one_flush() {
    let (writer, observed) = ScriptedWriter::new(WriterFault::WriteCall(usize::MAX), false);
    let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);
    let first_payload =
        bincode::serialize(&KeyValueData::new(b"key".to_vec(), b"add".to_vec())).unwrap();
    let second_payload =
        bincode::serialize(&KeyValueData::new(b"key".to_vec(), b"remove".to_vec())).unwrap();
    let first = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 2,
        payload: &first_payload,
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 2,
        timestamp_bucket: 0,
    });
    let second_start = V1CodecProbe::HEADER_LEN as u32 + first.len() as u32;
    let second = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 3,
        payload: &second_payload,
        physical_start: second_start,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 1,
        count: 2,
        timestamp_bucket: 0,
    });
    let mut expected = first;
    expected.extend_from_slice(&second);

    wal.commit_set_compute_batch(vec![
        ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"add".to_vec(),
        },
        ComputeAction::SetRemove {
            key: b"key".to_vec(),
            value: b"remove".to_vec(),
        },
    ])
    .unwrap();

    assert_eq!(observed.bytes(), expected);
    assert_eq!(observed.flush_calls(), 1);
}

#[test]
fn map_compute_batch_uses_contiguous_v1_group_and_one_flush() {
    let (writer, observed) = ScriptedWriter::new(WriterFault::WriteCall(usize::MAX), false);
    let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);
    let first_payload = bincode::serialize(&SortedMapEntry::new(
        b"key".to_vec(),
        7_usize.into(),
        b"value".to_vec(),
    ))
    .unwrap();
    let second_payload =
        bincode::serialize(&SortedMapKey::new(b"key".to_vec(), 7_usize.into())).unwrap();
    let first = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 4,
        payload: &first_payload,
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 2,
        timestamp_bucket: 0,
    });
    let second_start = V1CodecProbe::HEADER_LEN as u32 + first.len() as u32;
    let second = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 5,
        payload: &second_payload,
        physical_start: second_start,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 1,
        count: 2,
        timestamp_bucket: 0,
    });
    let mut expected = first;
    expected.extend_from_slice(&second);

    wal.commit_map_compute_batch(vec![
        ComputeAction::MapPut {
            key: b"key".to_vec(),
            search_key: 7_usize.into(),
            value: b"value".to_vec(),
        },
        ComputeAction::MapRemove {
            key: b"key".to_vec(),
            search_key: 7_usize.into(),
        },
    ])
    .unwrap();

    assert_eq!(observed.bytes(), expected);
    assert_eq!(observed.flush_calls(), 1);
}

#[test]
fn grouped_write_shares_start_count_timestamp_and_uses_one_flush() {
    let (writer, observed) = ScriptedWriter::new(WriterFault::WriteCall(usize::MAX), false);
    let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);

    wal.commit_set_compute_batch(vec![
        ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"first".to_vec(),
        },
        ComputeAction::SetRemove {
            key: b"key".to_vec(),
            value: b"old".to_vec(),
        },
        ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"second".to_vec(),
        },
    ])
    .unwrap();

    let bytes = observed.bytes();
    let mutation_start = V1CodecProbe::HEADER_LEN as u32;
    let mut cursor = 0;
    for index in 0..3_u32 {
        let payload_len =
            u32::from_le_bytes(bytes[cursor + 6..cursor + 10].try_into().unwrap()) as usize;
        let frame_len = V1CodecProbe::EMPTY_RECORD_LEN + payload_len;
        let frame = &bytes[cursor..cursor + frame_len];
        assert_eq!(
            u32::from_le_bytes(frame[14..18].try_into().unwrap()),
            mutation_start + cursor as u32
        );
        assert_eq!(
            u32::from_le_bytes(frame[18..22].try_into().unwrap()),
            mutation_start
        );
        assert_eq!(u32::from_le_bytes(frame[22..26].try_into().unwrap()), index);
        assert_eq!(u32::from_le_bytes(frame[26..30].try_into().unwrap()), 3);
        assert_eq!(u64::from_le_bytes(frame[30..38].try_into().unwrap()), 0);
        assert!(V1CodecProbe::record_crc_is_valid(frame));
        cursor += frame_len;
    }
    assert_eq!(cursor, bytes.len());
    assert_eq!(observed.write_calls(), 1);
    assert_eq!(observed.flush_calls(), 1);
}

#[test]
fn every_action_remains_v1_across_append_and_reopen() {
    let directory = tempfile::tempdir().unwrap();

    let value_path = directory.path().join("value.wal");
    let value = create_v1_test_wal(&value_path, 1);
    value
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap();
    value.try_store_delete_event(b"key").unwrap();
    drop(value);
    let value = reopen_v1_test_wal(&value_path);
    value
        .try_store_put_event(b"again".to_vec(), b"value".to_vec())
        .unwrap();
    drop(value);
    assert_v1_action_stream(&value_path, 1, &[1, 0, 1]);

    let set_path = directory.path().join("set.wal");
    let set = create_v1_test_wal(&set_path, 2);
    set.try_store_append_to_set_event(b"key".to_vec(), b"member".to_vec())
        .unwrap();
    set.try_store_remove_from_set_event(b"key".to_vec(), b"member".to_vec())
        .unwrap();
    drop(set);
    let set = reopen_v1_test_wal(&set_path);
    set.try_store_delete_event(b"key").unwrap();
    drop(set);
    assert_v1_action_stream(&set_path, 2, &[2, 3, 0]);

    let map_path = directory.path().join("map.wal");
    let map = create_v1_test_wal(&map_path, 3);
    map.try_store_put_to_map_event(b"key".to_vec(), 7_usize.into(), b"value".to_vec())
        .unwrap();
    map.try_store_remove_from_sorted_map_event(b"key".to_vec(), 7_usize.into())
        .unwrap();
    drop(map);
    let map = reopen_v1_test_wal(&map_path);
    map.try_store_delete_event(b"key").unwrap();
    drop(map);
    assert_v1_action_stream(&map_path, 3, &[4, 5, 0]);
}

fn create_v1_test_wal(path: &Path, kind: u8) -> WalStorage<std::fs::File> {
    fs::write(path, V1CodecProbe::encode_header_with_kind(kind)).unwrap();
    reopen_v1_test_wal(path)
}

fn reopen_v1_test_wal(path: &Path) -> WalStorage<std::fs::File> {
    let offset = u32::try_from(fs::metadata(path).unwrap().len()).unwrap();
    let file = OpenOptions::new().append(true).open(path).unwrap();
    WalStorage::from_prepared_file(file, offset)
}

fn assert_v1_action_stream(path: &Path, kind: u8, expected_actions: &[u8]) {
    let bytes = fs::read(path).unwrap();
    assert_eq!(
        &bytes[..V1CodecProbe::HEADER_LEN],
        &V1CodecProbe::encode_header_with_kind(kind)
    );
    let mut offset = V1CodecProbe::HEADER_LEN;
    let mut actions = Vec::new();
    while offset < bytes.len() {
        let payload_len =
            u32::from_le_bytes(bytes[offset + 6..offset + 10].try_into().unwrap()) as usize;
        let end = offset + V1CodecProbe::EMPTY_RECORD_LEN + payload_len;
        let frame = &bytes[offset..end];
        let action = frame[3];
        assert!(V1CodecProbe::record_marker_is_valid(frame));
        assert!(V1CodecProbe::record_version_is_valid(frame));
        assert!(V1CodecProbe::record_action_is_valid(frame));
        assert!(V1CodecProbe::record_header_length_is_valid(frame));
        assert!(V1CodecProbe::record_length_complement_is_valid(frame));
        assert!(V1CodecProbe::record_physical_start_is_valid(
            frame,
            offset as u32
        ));
        assert!(V1CodecProbe::record_mutation_start_is_valid(frame));
        assert!(V1CodecProbe::record_index_count_are_valid(frame));
        assert!(V1CodecProbe::payload_is_valid(
            kind,
            action,
            &frame[38..38 + payload_len]
        ));
        assert!(V1CodecProbe::record_crc_is_valid(frame));
        actions.push(action);
        offset = end;
    }
    assert_eq!(actions, expected_actions);
}

#[test]
fn record_identity_fields_reject_corruption_at_every_position() {
    for position in 0..3 {
        for field in 0..4 {
            let (mut bytes, ranges) = three_value_record_stream();
            let frame = &mut bytes[ranges[position].clone()];
            match field {
                0 => frame[0] ^= 0xff,
                1 => frame[2] = 2,
                2 => frame[3] = 6,
                3 => frame[4..6].copy_from_slice(&37_u16.to_le_bytes()),
                _ => unreachable!(),
            }
            rewrite_test_record_crc(frame);
            assert!(
                replay_key_value(&bytes).is_err(),
                "position={position} field={field}"
            );
        }
    }
}

#[test]
fn record_length_fields_reject_corruption_at_every_position() {
    for position in 0..3 {
        let (mut complement_bytes, ranges) = three_value_record_stream();
        let frame = &mut complement_bytes[ranges[position].clone()];
        frame[10] ^= 0x01;
        rewrite_test_record_crc(frame);
        assert!(replay_key_value(&complement_bytes).is_err());

        let (mut overflow_bytes, ranges) = three_value_record_stream();
        let frame = &mut overflow_bytes[ranges[position].clone()];
        frame[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
        frame[10..14].copy_from_slice(&(!u32::MAX).to_le_bytes());
        assert!(replay_key_value(&overflow_bytes).is_err());
    }
}

#[test]
fn record_physical_fields_reject_corruption_at_every_position() {
    for position in 0..3 {
        for footer in [false, true] {
            let (mut bytes, ranges) = three_value_record_stream();
            let frame = &mut bytes[ranges[position].clone()];
            let payload_len = u32::from_le_bytes(frame[6..10].try_into().unwrap()) as usize;
            if footer {
                frame[38 + payload_len..42 + payload_len].copy_from_slice(&u32::MAX.to_le_bytes());
            } else {
                frame[14..18].copy_from_slice(&u32::MAX.to_le_bytes());
            }
            rewrite_test_record_crc(frame);
            assert!(replay_key_value(&bytes).is_err());
        }
    }
}

#[test]
fn record_group_fields_reject_corruption_at_every_position() {
    for position in 0..3 {
        for field in 0..3 {
            let (mut bytes, ranges) = three_value_record_stream();
            let frame = &mut bytes[ranges[position].clone()];
            match field {
                0 => frame[18..22].copy_from_slice(&0_u32.to_le_bytes()),
                1 => frame[22..26].copy_from_slice(&1_u32.to_le_bytes()),
                2 => frame[26..30].copy_from_slice(&0_u32.to_le_bytes()),
                _ => unreachable!(),
            }
            rewrite_test_record_crc(frame);
            assert!(replay_key_value(&bytes).is_err());
        }
    }
}

#[test]
fn record_timestamp_and_payload_reject_corruption_at_every_position() {
    for position in 0..3 {
        let (mut timestamp_bytes, ranges) = three_value_record_stream();
        timestamp_bytes[ranges[position].start + 30] ^= 0x01;
        assert!(replay_key_value(&timestamp_bytes).is_err());

        let (mut payload_bytes, ranges) = three_value_record_stream();
        let frame = &mut payload_bytes[ranges[position].clone()];
        frame[38..46].copy_from_slice(&u64::MAX.to_le_bytes());
        rewrite_test_record_crc(frame);
        assert!(replay_key_value(&payload_bytes).is_err());
    }
}

#[test]
fn every_action_crc_rejects_first_middle_and_final_corruption() {
    let key_value =
        bincode::serialize(&KeyValueData::new(b"key".to_vec(), b"value".to_vec())).unwrap();
    let map_put = bincode::serialize(&SortedMapEntry::new(
        b"key".to_vec(),
        7_usize.into(),
        b"value".to_vec(),
    ))
    .unwrap();
    let map_remove =
        bincode::serialize(&SortedMapKey::new(b"key".to_vec(), 7_usize.into())).unwrap();
    for (action, payload) in [
        (0, b"key".as_slice()),
        (1, key_value.as_slice()),
        (2, key_value.as_slice()),
        (3, key_value.as_slice()),
        (4, map_put.as_slice()),
        (5, map_remove.as_slice()),
    ] {
        for position in 0..3 {
            let mut offset = V1CodecProbe::HEADER_LEN as u32;
            let mut frames = Vec::new();
            for _ in 0..3 {
                let frame = V1CodecProbe::encode_complete_record(RecordProbeFields {
                    action,
                    payload,
                    physical_start: offset,
                    mutation_start: offset,
                    index: 0,
                    count: 1,
                    timestamp_bucket: 0,
                });
                offset += frame.len() as u32;
                frames.push(frame);
            }
            let corrupt = &mut frames[position];
            let crc_index = corrupt.len() - 1;
            corrupt[crc_index] ^= 0xff;
            for (index, frame) in frames.iter().enumerate() {
                assert_eq!(
                    V1CodecProbe::record_crc_is_valid(frame),
                    index != position,
                    "action={action} position={position}"
                );
            }
        }
    }
}

fn three_value_record_stream() -> (Vec<u8>, Vec<std::ops::Range<usize>>) {
    let mut bytes = V1CodecProbe::encode_header().to_vec();
    let mut ranges = Vec::new();
    for index in 0..3 {
        let payload = bincode::serialize(&KeyValueData::new(
            format!("key-{index}").into_bytes(),
            format!("value-{index}").into_bytes(),
        ))
        .unwrap();
        let start = bytes.len();
        let frame = V1CodecProbe::encode_complete_record(RecordProbeFields {
            action: 1,
            payload: &payload,
            physical_start: start as u32,
            mutation_start: start as u32,
            index: 0,
            count: 1,
            timestamp_bucket: index as u64,
        });
        bytes.extend_from_slice(&frame);
        ranges.push(start..bytes.len());
    }
    (bytes, ranges)
}

#[test]
fn incomplete_constant_matching_action_header_is_a_recoverable_tail() {
    use crate::wal::replay::{replay_key_value_tail, TailReplay};

    let (bytes, ranges) = three_value_record_stream();
    let accepted_end = ranges[0].end;
    let cut = ranges[1].start + 5;
    let classified = replay_key_value_tail(&bytes[..cut]);

    let TailReplay::RecoverableTail {
        replay,
        tail_offset,
        ..
    } = classified
    else {
        panic!("matching incomplete action header must be recoverable");
    };
    assert_eq!(tail_offset, accepted_end);
    assert_eq!(replay.byte_len, accepted_end as u64);
    assert_eq!(replay.snapshot.len(), 1);
    assert_eq!(
        replay.snapshot.get(b"key-0".as_slice()),
        Some(&b"value-0".to_vec())
    );

    let mut mismatched = bytes[..cut].to_vec();
    mismatched[ranges[1].start] ^= 0xff;
    assert!(matches!(
        replay_key_value_tail(&mismatched),
        TailReplay::Invalid(_)
    ));
}

#[test]
fn incomplete_action_payload_is_a_recoverable_tail() {
    use crate::wal::replay::{replay_key_value_tail, TailReplay};

    let (bytes, ranges) = three_value_record_stream();
    let accepted_end = ranges[0].end;
    let payload_start = ranges[1].start + 38;
    let cut = payload_start + 1;

    let TailReplay::RecoverableTail {
        replay,
        tail_offset,
        ..
    } = replay_key_value_tail(&bytes[..cut])
    else {
        panic!("incomplete action payload must be recoverable");
    };
    assert_eq!(tail_offset, accepted_end);
    assert_eq!(replay.byte_len, accepted_end as u64);
    assert_eq!(replay.snapshot.len(), 1);
    assert_eq!(
        replay.snapshot.get(b"key-0".as_slice()),
        Some(&b"value-0".to_vec())
    );
}

#[test]
fn incomplete_action_footer_is_a_recoverable_tail() {
    use crate::wal::replay::{replay_key_value_tail, TailReplay};

    let (bytes, ranges) = three_value_record_stream();
    let accepted_end = ranges[0].end;
    let second = &bytes[ranges[1].clone()];
    let payload_len = u32::from_le_bytes(second[6..10].try_into().unwrap()) as usize;
    let footer_start = ranges[1].start + 38 + payload_len;
    let cut = footer_start + 1;

    let TailReplay::RecoverableTail {
        replay,
        tail_offset,
        ..
    } = replay_key_value_tail(&bytes[..cut])
    else {
        panic!("incomplete action footer must be recoverable");
    };
    assert_eq!(tail_offset, accepted_end);
    assert_eq!(replay.byte_len, accepted_end as u64);
    assert_eq!(replay.snapshot.len(), 1);
}

#[test]
fn incomplete_first_action_preserves_header_and_empty_accepted_state() {
    use crate::wal::replay::{replay_key_value_tail, TailReplay};

    let header = V1CodecProbe::encode_header_with_granularity(7_500_000_000);
    let payload = bincode::serialize(&KeyValueData::new(
        b"unaccepted-key".to_vec(),
        b"unaccepted-value".to_vec(),
    ))
    .unwrap();
    let first_record = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 1,
        payload: &payload,
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 0,
    });
    let mut cut = header.to_vec();
    cut.extend_from_slice(&first_record[..5]);

    let TailReplay::RecoverableTail {
        replay,
        tail_offset,
        accepted_header,
    } = replay_key_value_tail(&cut)
    else {
        panic!("an incomplete first action after a valid header must be recoverable");
    };
    assert_eq!(tail_offset, V1CodecProbe::HEADER_LEN);
    assert_eq!(replay.byte_len, V1CodecProbe::HEADER_LEN as u64);
    assert!(replay.snapshot.is_empty());
    assert_eq!(accepted_header, Some(header.to_vec()));
}

fn two_record_stream(
    kind: u8,
    accepted_action: u8,
    accepted_payload: &[u8],
    tail_action: u8,
    tail_payload: &[u8],
) -> (Vec<u8>, usize) {
    let mut bytes = V1CodecProbe::encode_header_with_kind(kind).to_vec();
    let accepted_start = bytes.len();
    bytes.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: accepted_action,
        payload: accepted_payload,
        physical_start: accepted_start as u32,
        mutation_start: accepted_start as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 1,
    }));
    let accepted_end = bytes.len();
    let tail = V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: tail_action,
        payload: tail_payload,
        physical_start: accepted_end as u32,
        mutation_start: accepted_end as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 2,
    });
    bytes.extend_from_slice(&tail[..tail.len() - 1]);
    (bytes, accepted_end)
}

fn assert_recoverable_terminal_fragment<S>(
    classified: crate::wal::replay::TailReplay<S>,
    accepted_end: usize,
) {
    let crate::wal::replay::TailReplay::RecoverableTail {
        replay,
        tail_offset,
        ..
    } = classified
    else {
        panic!("valid terminal action fragment must be recoverable");
    };
    assert_eq!(tail_offset, accepted_end);
    assert_eq!(replay.byte_len, accepted_end as u64);
}

#[test]
fn all_six_action_shapes_classify_terminal_fragments_as_recoverable() {
    use crate::wal::replay::{replay_key_map_tail, replay_key_set_tail, replay_key_value_tail};

    let value = bincode::serialize(&KeyValueData::new(b"key".to_vec(), b"value".to_vec())).unwrap();
    for (action, payload) in [(DELETE_ACT, b"deleted-key".as_slice()), (PUT_ACT, &value)] {
        let (bytes, accepted_end) = two_record_stream(1, PUT_ACT, &value, action, payload);
        assert_recoverable_terminal_fragment(replay_key_value_tail(&bytes), accepted_end);
    }

    for action in [SET_APPEND_ACT, SET_REMOVE_ACT] {
        let (bytes, accepted_end) = two_record_stream(2, SET_APPEND_ACT, &value, action, &value);
        assert_recoverable_terminal_fragment(replay_key_set_tail(&bytes), accepted_end);
    }

    let map_put = bincode::serialize(&SortedMapEntry::new(
        b"key".to_vec(),
        SearchKey::from(1_usize),
        b"value".to_vec(),
    ))
    .unwrap();
    let map_remove = bincode::serialize(&SortedMapKey::new(
        b"key".to_vec(),
        SearchKey::from(1_usize),
    ))
    .unwrap();
    for (action, payload) in [
        (MAP_PUT_ACT, map_put.as_slice()),
        (MAP_REMOVE_ACT, &map_remove),
    ] {
        let (bytes, accepted_end) = two_record_stream(3, MAP_PUT_ACT, &map_put, action, payload);
        assert_recoverable_terminal_fragment(replay_key_map_tail(&bytes), accepted_end);
    }
}

#[test]
fn eof_after_complete_nonfinal_group_member_is_recoverable() {
    use crate::wal::replay::{replay_key_set_tail, TailReplay};

    let mut bytes = V1CodecProbe::encode_header_with_kind(2).to_vec();
    let stable_payload =
        bincode::serialize(&KeyValueData::new(b"stable".to_vec(), b"seed".to_vec())).unwrap();
    let stable_start = bytes.len();
    bytes.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: SET_APPEND_ACT,
        payload: &stable_payload,
        physical_start: stable_start as u32,
        mutation_start: stable_start as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 1,
    }));
    let group_start = bytes.len();
    let pending_payload = bincode::serialize(&KeyValueData::new(
        b"pending".to_vec(),
        b"must-not-appear".to_vec(),
    ))
    .unwrap();
    bytes.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: SET_APPEND_ACT,
        payload: &pending_payload,
        physical_start: group_start as u32,
        mutation_start: group_start as u32,
        index: 0,
        count: 2,
        timestamp_bucket: 2,
    }));

    let TailReplay::RecoverableTail {
        replay,
        tail_offset,
        ..
    } = replay_key_set_tail(&bytes)
    else {
        panic!("EOF after a complete nonfinal group member must be recoverable");
    };
    assert_eq!(tail_offset, group_start);
    assert_eq!(replay.byte_len, group_start as u64);
    assert_eq!(
        replay.snapshot.get(b"stable".as_slice()),
        Some(&std::collections::HashSet::from([b"seed".to_vec()]))
    );
    assert!(!replay.snapshot.contains_key(b"pending".as_slice()));
}

#[test]
fn every_group_member_byte_cut_rolls_back_to_mutation_start() {
    use crate::wal::replay::{replay_key_set_tail, TailReplay};

    let mut accepted = V1CodecProbe::encode_header_with_kind(2).to_vec();
    let stable_payload =
        bincode::serialize(&KeyValueData::new(b"stable".to_vec(), b"seed".to_vec())).unwrap();
    let stable_start = accepted.len();
    accepted.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: SET_APPEND_ACT,
        payload: &stable_payload,
        physical_start: stable_start as u32,
        mutation_start: stable_start as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 1,
    }));
    let group_start = accepted.len();
    let group_actions = [
        (SET_APPEND_ACT, b"pending-a".as_slice()),
        (SET_REMOVE_ACT, b"seed".as_slice()),
        (SET_APPEND_ACT, b"pending-b".as_slice()),
    ];
    let mut group = Vec::new();
    for (index, (action, value)) in group_actions.into_iter().enumerate() {
        let payload =
            bincode::serialize(&KeyValueData::new(b"stable".to_vec(), value.to_vec())).unwrap();
        let physical_start = group_start + group.len();
        group.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
            action,
            payload: &payload,
            physical_start: physical_start as u32,
            mutation_start: group_start as u32,
            index: index as u32,
            count: 3,
            timestamp_bucket: 2,
        }));
    }

    for group_cut in 1..group.len() {
        let mut bytes = accepted.clone();
        bytes.extend_from_slice(&group[..group_cut]);
        let TailReplay::RecoverableTail {
            replay,
            tail_offset,
            ..
        } = replay_key_set_tail(&bytes)
        else {
            panic!("group cut {group_cut} must be recoverable");
        };
        assert_eq!(tail_offset, group_start, "group cut {group_cut}");
        assert_eq!(replay.byte_len, group_start as u64, "group cut {group_cut}");
        assert_eq!(
            replay.snapshot.get(b"stable".as_slice()),
            Some(&std::collections::HashSet::from([b"seed".to_vec()])),
            "group cut {group_cut}"
        );
    }
}

fn rewrite_test_record_crc(frame: &mut [u8]) {
    let payload_len = u32::from_le_bytes(frame[6..10].try_into().unwrap()) as usize;
    let crc_start = 42 + payload_len;
    let crc = crc32fast::hash(&frame[..crc_start]);
    frame[crc_start..crc_start + 4].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn accepted_logical_snapshot_encodes_as_complete_v1() {
    let snapshot = std::collections::HashMap::from([
        (b"beta".to_vec(), b"two".to_vec()),
        (b"alpha".to_vec(), b"one".to_vec()),
    ]);
    let header = V1CodecProbe::encode_header();

    let encoded = encode_key_value_repair_snapshot(&snapshot, &header);

    assert_eq!(&encoded[..V1CodecProbe::HEADER_LEN], &header);
    let replayed = replay_key_value(&encoded).unwrap();
    assert_eq!(replayed.snapshot, snapshot);
    assert_eq!(replayed.byte_len, encoded.len() as u64);
}

#[test]
fn repair_staging_create_failure_preserves_source_and_existing_staging() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let source = V1CodecProbe::encode_header();
    let existing_staging = b"unowned-staging-evidence";
    fs::write(&paths.active, source).unwrap();
    fs::write(&paths.staging, existing_staging).unwrap();

    let failure = match create_repair_staging(&paths) {
        Ok(_) => panic!("exclusive repair staging create must reject an existing path"),
        Err(failure) => failure,
    };

    assert_eq!(failure.operation, crate::RecoveryOperation::CreateStaging);
    assert_eq!(failure.path, paths.staging);
    assert_eq!(fs::read(paths.active).unwrap(), source);
    assert_eq!(fs::read(paths.staging).unwrap(), existing_staging);
}

#[test]
fn repair_partial_write_failure_preserves_source_authority() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let source = V1CodecProbe::encode_header();
    fs::write(&paths.active, source).unwrap();
    let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let replacement = encode_key_value_repair_snapshot(&snapshot, &source);
    let cut = replacement.len() - 1;
    let staging = create_repair_staging(&paths).unwrap();

    let failure = match write_repair_snapshot_prefix(staging, &replacement, cut, &paths) {
        Ok(_) => panic!("partial repair write must fail before later checkpoints"),
        Err(failure) => failure,
    };

    assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
    assert_eq!(failure.path, paths.staging);
    assert_eq!(fs::read(paths.active).unwrap(), source);
    assert_eq!(fs::read(paths.staging).unwrap(), replacement[..cut]);
}

#[test]
fn repair_flush_failure_preserves_source_authority() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let source = V1CodecProbe::encode_header();
    fs::write(&paths.active, source).unwrap();
    let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let replacement = encode_key_value_repair_snapshot(&snapshot, &source);
    let staging = create_repair_staging(&paths).unwrap();
    let staging =
        write_repair_snapshot_prefix(staging, &replacement, replacement.len(), &paths).unwrap();

    let failure = match flush_repair_snapshot(staging, true, &paths) {
        Ok(_) => panic!("repair flush failure must stop before staged validation"),
        Err(failure) => failure,
    };

    assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
    assert_eq!(failure.path, paths.staging);
    assert_eq!(fs::read(paths.active).unwrap(), source);
    assert_eq!(fs::read(paths.staging).unwrap(), replacement);
}

#[test]
fn repair_staged_validation_requires_exact_logical_state_and_configuration() {
    let expected_header = V1CodecProbe::encode_header_with_granularity(60_000_000_000);
    let expected_snapshot =
        std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let wrong_snapshot =
        std::collections::HashMap::from([(b"key".to_vec(), b"different-value".to_vec())]);
    let wrong_config = V1CodecProbe::encode_header_with_granularity(1);
    let candidates = [
        (
            "logical state",
            encode_key_value_repair_snapshot(&wrong_snapshot, &expected_header),
        ),
        (
            "configuration",
            encode_key_value_repair_snapshot(&expected_snapshot, &wrong_config),
        ),
    ];

    for (difference, candidate) in candidates {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let source = encode_key_value_repair_snapshot(&expected_snapshot, &expected_header);
        fs::write(&paths.active, &source).unwrap();
        let staging = create_repair_staging(&paths).unwrap();
        let staging =
            write_repair_snapshot_prefix(staging, &candidate, candidate.len(), &paths).unwrap();
        let staging = flush_repair_snapshot(staging, false, &paths).unwrap();

        let failure = match validate_key_value_repair_snapshot(
            staging,
            &expected_snapshot,
            &expected_header,
            &paths,
        ) {
            Ok(_) => panic!("staged repair with wrong {difference} must fail validation"),
            Err(failure) => failure,
        };

        assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
        assert_eq!(failure.path, paths.staging);
        assert_eq!(fs::read(paths.active).unwrap(), source);
        assert_eq!(fs::read(paths.staging).unwrap(), candidate);
    }
}

#[test]
fn repair_sync_failure_preserves_source_authority() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header();
    let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let source = encode_key_value_repair_snapshot(&snapshot, &header);
    fs::write(&paths.active, &source).unwrap();
    let staging = create_repair_staging(&paths).unwrap();
    let staging = write_repair_snapshot_prefix(staging, &source, source.len(), &paths).unwrap();
    let staging = flush_repair_snapshot(staging, false, &paths).unwrap();
    let staging = validate_key_value_repair_snapshot(staging, &snapshot, &header, &paths).unwrap();

    let failure = match sync_repair_snapshot(staging, true, &paths) {
        Ok(_) => panic!("repair synchronization failure must stop before publication"),
        Err(failure) => failure,
    };

    assert_eq!(failure.operation, crate::RecoveryOperation::WriteStaging);
    assert_eq!(failure.path, paths.staging);
    assert_eq!(fs::read(paths.active).unwrap(), source);
    assert_eq!(fs::read(paths.staging).unwrap(), source);
}

#[test]
fn repair_publish_failure_preserves_source_without_truncation() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header();
    let source = header.to_vec();
    fs::write(&paths.active, &source).unwrap();
    let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let replacement = encode_key_value_repair_snapshot(&snapshot, &header);
    let staging = create_repair_staging(&paths).unwrap();
    let staging =
        write_repair_snapshot_prefix(staging, &replacement, replacement.len(), &paths).unwrap();
    let staging = flush_repair_snapshot(staging, false, &paths).unwrap();
    let staging = validate_key_value_repair_snapshot(staging, &snapshot, &header, &paths).unwrap();
    let staging = sync_repair_snapshot(staging, false, &paths).unwrap();

    let failure = publish_repair_snapshot(staging, &paths, true).unwrap_err();

    assert_eq!(failure.operation, crate::RecoveryOperation::Publish);
    assert_eq!(failure.path, paths.active);
    assert_eq!(fs::read(paths.active).unwrap(), source);
    assert_eq!(fs::read(paths.staging).unwrap(), replacement);
}

#[test]
fn repair_reopen_rejects_wrong_length_and_incomplete_v1() {
    for fault in ["extra byte", "complete corruption"] {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let header = V1CodecProbe::encode_header();
        fs::write(&paths.active, header).unwrap();
        let snapshot =
            std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
        let replacement = encode_key_value_repair_snapshot(&snapshot, &header);
        let staging = create_repair_staging(&paths).unwrap();
        let staging =
            write_repair_snapshot_prefix(staging, &replacement, replacement.len(), &paths).unwrap();
        let staging = flush_repair_snapshot(staging, false, &paths).unwrap();
        let staging =
            validate_key_value_repair_snapshot(staging, &snapshot, &header, &paths).unwrap();
        let staging = sync_repair_snapshot(staging, false, &paths).unwrap();
        let expected_len = publish_repair_snapshot(staging, &paths, false).unwrap();
        let mut invalid_published = replacement.clone();
        if fault == "extra byte" {
            invalid_published.push(0xa5);
        } else {
            let last = invalid_published.len() - 1;
            invalid_published[last] ^= 0xff;
        }
        fs::write(&paths.active, &invalid_published).unwrap();

        let failure =
            match reopen_key_value_repair_snapshot(&paths, expected_len, &snapshot, &header) {
                Ok(_) => panic!("repair reopen must reject {fault}"),
                Err(failure) => failure,
            };

        assert_eq!(failure.operation, crate::RecoveryOperation::Open);
        assert_eq!(failure.path, paths.active);
        assert_eq!(fs::read(paths.active).unwrap(), invalid_published);
        assert!(!paths.staging.exists());
    }
}

#[test]
fn blocking_repair_cleanup_failure_preserves_selected_authority() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let selected = V1CodecProbe::encode_header().to_vec();
    let obsolete = b"proven-obsolete-active".to_vec();
    let staged = b"validated-repair-staging".to_vec();
    fs::write(&paths.legacy, &selected).unwrap();
    fs::write(&paths.active, &obsolete).unwrap();
    fs::write(&paths.staging, &staged).unwrap();

    let failure =
        cleanup_blocking_repair_active(&paths, &paths.legacy, &obsolete, true).unwrap_err();

    assert_eq!(failure.operation, crate::RecoveryOperation::Cleanup);
    assert_eq!(failure.path, paths.active);
    assert_eq!(fs::read(paths.legacy).unwrap(), selected);
    assert_eq!(fs::read(paths.active).unwrap(), obsolete);
    assert_eq!(fs::read(paths.staging).unwrap(), staged);
}

#[test]
fn post_publish_cleanup_failure_defers_after_validated_authority() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header();
    fs::write(&paths.active, header).unwrap();
    let obsolete_recovery = b"obsolete-recovery-evidence";
    fs::write(&paths.legacy, obsolete_recovery).unwrap();
    let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let replacement = encode_key_value_repair_snapshot(&snapshot, &header);
    let staging = create_repair_staging(&paths).unwrap();
    let staging =
        write_repair_snapshot_prefix(staging, &replacement, replacement.len(), &paths).unwrap();
    let staging = flush_repair_snapshot(staging, false, &paths).unwrap();
    let staging = validate_key_value_repair_snapshot(staging, &snapshot, &header, &paths).unwrap();
    let staging = sync_repair_snapshot(staging, false, &paths).unwrap();
    let expected_len = publish_repair_snapshot(staging, &paths, false).unwrap();
    let validated =
        reopen_key_value_repair_snapshot(&paths, expected_len, &snapshot, &header).unwrap();

    let completed = cleanup_after_validated_repair(validated, &paths, true)
        .expect("cleanup after validated publication may be deferred");

    assert_eq!(completed.status, crate::RecoveryStatus::Recovered);
    assert!(completed.cleanup_deferred);
    assert_eq!(completed.handle.metadata().unwrap().len(), expected_len);
    assert_eq!(fs::read(paths.active).unwrap(), replacement);
    assert_eq!(fs::read(paths.legacy).unwrap(), obsolete_recovery);
}

#[test]
fn promoted_repair_publisher_returns_validated_append_handle() {
    let directory = tempfile::tempdir().unwrap();
    let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
    let header = V1CodecProbe::encode_header();
    fs::write(&paths.active, header).unwrap();
    let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"accepted-value".to_vec())]);
    let replacement = encode_key_value_repair_snapshot(&snapshot, &header);

    let mut completed = publish_validated_repair(
        &paths,
        RepairAuthority::Active {
            obsolete_recovery: None,
        },
        &replacement,
        |persisted| {
            persisted.get(..V1CodecProbe::HEADER_LEN) == Some(header.as_slice())
                && replay_key_value(persisted).is_ok_and(|replayed| replayed.snapshot == snapshot)
        },
    )
    .unwrap();

    assert_eq!(completed.status, crate::RecoveryStatus::Recovered);
    assert!(!completed.cleanup_deferred);
    assert_eq!(
        completed.handle.stream_position().unwrap(),
        replacement.len() as u64
    );
    assert_eq!(fs::read(paths.active).unwrap(), replacement);
    assert!(!paths.legacy.exists());
    assert!(!paths.staging.exists());
}
