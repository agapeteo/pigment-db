//! Private WAL maintenance behavior tests.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use super::format::V2CodecProbe;
use super::model::{
    KeyValueData, StoredAction, DELETE_ACT, MAP_PUT_V2_ACT, MAP_REMOVE_V2_ACT, PUT_ACT,
    SET_APPEND_ACT, SET_REMOVE_ACT,
};
use super::replay::{
    encode_current_key_map_snapshot, encode_current_key_map_snapshot_with_metadata,
    encode_current_key_set_snapshot, encode_current_key_set_snapshot_with_metadata,
    encode_current_key_value_snapshot, encode_current_key_value_snapshot_with_metadata,
    replay_key_map, replay_key_set, replay_key_value, KeyMapSnapshot, KeySetSnapshot,
    KeyValueSnapshot,
};
use super::{
    checked_current_v2_group_encoded_len, DeltaRecordResult, DeltaRecorder, RecordedFrame,
    RecordedMutation, WalStorage,
};
use crate::model::{Key, SearchKey};
use crate::test_support::fault_writer::{
    rollback_scripted, sync_data_scripted, BarrierKind, ScriptedWriter,
};

#[test]
fn delta_recorder_is_token_bound_exactly_bounded_and_terminal_on_overflow() {
    let storage = WalStorage::new_vec_based();
    let exact = group_encoded_len(&[3, 5]);
    let mut state = storage.wal_state.write().unwrap();
    assert!(state.activate_delta(11, exact).is_ok());
    assert!(state.activate_delta(12, exact).is_err());
    assert!(state.detach_delta(12).is_none());

    let built = Cell::new(false);
    assert_eq!(
        state
            .delta_recorder
            .as_mut()
            .unwrap()
            .record_group([3, 5], || {
                built.set(true);
                mutation(7, &[3, 5])
            }),
        DeltaRecordResult::Recorded
    );
    assert!(built.get());
    let exact_recorder = state.detach_delta(11).unwrap();
    assert_eq!(exact_recorder.used_bytes, exact);
    assert_eq!(exact_recorder.groups, vec![mutation(7, &[3, 5])]);
    assert!(!exact_recorder.overflowed);

    state.activate_delta(21, exact + 1).unwrap();
    let recorder = state.delta_recorder.as_mut().unwrap();
    assert_eq!(
        recorder.record_group([0], || mutation(8, &[0])),
        DeltaRecordResult::Recorded
    );
    let over_limit_build_ran = Cell::new(false);
    assert_eq!(
        recorder.record_group([3, 5], || {
            over_limit_build_ran.set(true);
            mutation(9, &[3, 5])
        }),
        DeltaRecordResult::Overflowed
    );
    assert!(!over_limit_build_ran.get());
    assert!(recorder.overflowed);
    assert_eq!(recorder.used_bytes, 0);
    assert!(recorder.groups.is_empty());
    assert_eq!(recorder.groups.capacity(), 0);

    let later_build_ran = Cell::new(false);
    assert_eq!(
        recorder.record_group([0], || {
            later_build_ran.set(true);
            mutation(10, &[0])
        }),
        DeltaRecordResult::AlreadyOverflowed
    );
    assert!(!later_build_ran.get());
    assert!(recorder.groups.is_empty());

    let mut arithmetic_overflow = DeltaRecorder::new(31, u64::MAX);
    arithmetic_overflow.used_bytes = u64::MAX;
    let overflow_build_ran = Cell::new(false);
    assert_eq!(
        arithmetic_overflow.record_group([0], || {
            overflow_build_ran.set(true);
            mutation(11, &[0])
        }),
        DeltaRecordResult::Overflowed
    );
    assert!(!overflow_build_ran.get());
    assert!(arithmetic_overflow.overflowed);
    assert_eq!(arithmetic_overflow.used_bytes, 0);
    assert_eq!(arithmetic_overflow.groups.capacity(), 0);

    let mut first_group_over = DeltaRecorder::new(41, exact - 1);
    let first_build_ran = Cell::new(false);
    assert_eq!(
        first_group_over.record_group([3, 5], || {
            first_build_ran.set(true);
            mutation(12, &[3, 5])
        }),
        DeltaRecordResult::Overflowed
    );
    assert!(!first_build_ran.get());
    assert!(first_group_over.groups.is_empty());
    assert_eq!(first_group_over.groups.capacity(), 0);
}

#[test]
fn successful_single_actions_record_after_physical_acceptance_in_wal_order() {
    let header = V2CodecProbe::encode_header(super::format::V2HeaderProbeFields {
        kind: 1,
        granularity_nanos: 60_000_000_000,
        base_bucket: 0,
        segment_id: 0,
        segment_base: 0,
    });
    let (writer, handle) =
        ScriptedWriter::scripted_with_bytes(None, false, Some(BarrierKind::Data), header.to_vec());
    let wal = Arc::new(WalStorage::new_v2_with_physical_probe(
        writer,
        rollback_scripted,
        sync_data_scripted,
    ));
    wal.wal_state
        .write()
        .unwrap()
        .activate_delta(101, u64::MAX)
        .unwrap();

    let first_wal = Arc::clone(&wal);
    let first = std::thread::spawn(move || {
        first_wal
            .try_store_put_event(b"first".to_vec(), b"one".to_vec())
            .unwrap();
    });
    handle.wait_until_barrier_blocked(BarrierKind::Data);
    let (second_tx, second_rx) = mpsc::sync_channel(0);
    let second_wal = Arc::clone(&wal);
    let second = std::thread::spawn(move || {
        second_wal
            .try_store_put_event(b"second".to_vec(), b"two".to_vec())
            .unwrap();
        second_tx.send(()).unwrap();
    });
    assert!(second_rx.recv_timeout(Duration::from_millis(100)).is_err());
    handle.release_barrier();
    first.join().unwrap();
    second_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    second.join().unwrap();

    wal.try_store_delete_event(b"deleted").unwrap();
    wal.try_store_append_to_set_event_borrowed(b"set", b"added".to_vec())
        .unwrap();
    wal.try_store_remove_from_set_event(b"set".to_vec(), b"removed".to_vec())
        .unwrap();
    wal.try_store_put_to_map_event(b"map".to_vec(), SearchKey::from(1), b"mapped".to_vec())
        .unwrap();
    wal.try_store_remove_from_sorted_map_event(b"map".to_vec(), SearchKey::from(1))
        .unwrap();

    let recorder = wal.wal_state.write().unwrap().detach_delta(101).unwrap();
    assert_eq!(recorder.groups.len(), 7);
    assert_eq!(
        recorder
            .groups
            .iter()
            .map(|group| group.frames[0].action)
            .collect::<Vec<_>>(),
        vec![
            PUT_ACT,
            PUT_ACT,
            DELETE_ACT,
            SET_APPEND_ACT,
            SET_REMOVE_ACT,
            MAP_PUT_V2_ACT,
            MAP_REMOVE_V2_ACT,
        ]
    );
    assert!(recorder
        .groups
        .iter()
        .all(|group| group.timestamp_bucket == 0 && group.frames.len() == 1));
    let first_payload =
        StoredAction::prepare_put(&0, &KeyValueData::new(b"first".to_vec(), b"one".to_vec()));
    let second_payload =
        StoredAction::prepare_put(&0, &KeyValueData::new(b"second".to_vec(), b"two".to_vec()));
    assert_eq!(recorder.groups[0].frames[0].payload, first_payload.data());
    assert_eq!(recorder.groups[1].frames[0].payload, second_payload.data());
    let exact_used = checked_current_v2_group_encoded_len(
        recorder
            .groups
            .iter()
            .flat_map(|group| group.frames.iter().map(|frame| frame.payload.len())),
    )
    .unwrap();
    assert_eq!(recorder.used_bytes, exact_used);
}

fn group_encoded_len(payload_lengths: &[usize]) -> u64 {
    checked_current_v2_group_encoded_len(payload_lengths.iter().copied()).unwrap()
}

fn mutation(timestamp_bucket: u64, payload_lengths: &[usize]) -> RecordedMutation {
    RecordedMutation {
        timestamp_bucket,
        frames: payload_lengths
            .iter()
            .enumerate()
            .map(|(index, payload_len)| RecordedFrame {
                action: index as u8 + 1,
                payload: vec![index as u8; *payload_len],
            })
            .collect(),
    }
}

#[test]
fn key_value_snapshot_encodes_as_one_deterministic_current_v2_segment() {
    let snapshot = KeyValueSnapshot::from([
        (b"zeta".to_vec(), b"last".to_vec()),
        (b"alpha".to_vec(), b"first".to_vec()),
    ]);
    let reversed = HashMap::from([
        (b"alpha".to_vec(), b"first".to_vec()),
        (b"zeta".to_vec(), b"last".to_vec()),
    ]);

    let encoded = encode_current_key_value_snapshot(&snapshot).unwrap();
    let encoded_reversed = encode_current_key_value_snapshot(&reversed).unwrap();

    assert_eq!(encoded, encoded_reversed);
    assert_eq!(
        encoded
            .windows(b"PIGWAL\r\n".len())
            .filter(|window| *window == b"PIGWAL\r\n")
            .count(),
        1
    );
    assert!(V2CodecProbe::header_is_valid(
        &encoded[..V2CodecProbe::HEADER_LEN]
    ));
    assert_eq!(replay_key_value(&encoded).unwrap().snapshot, snapshot);
}

#[test]
fn key_set_snapshot_encodes_deterministically_with_exact_membership() {
    let snapshot = KeySetSnapshot::from([
        (
            b"zeta".to_vec(),
            HashSet::from([b"blue".to_vec(), b"amber".to_vec()]),
        ),
        (b"alpha".to_vec(), HashSet::from([b"red".to_vec()])),
    ]);
    let reordered = HashMap::from([
        (b"alpha".to_vec(), HashSet::from([b"red".to_vec()])),
        (
            b"zeta".to_vec(),
            HashSet::from([b"amber".to_vec(), b"blue".to_vec()]),
        ),
    ]);

    let encoded = encode_current_key_set_snapshot(&snapshot).unwrap();

    assert_eq!(
        encoded,
        encode_current_key_set_snapshot(&reordered).unwrap()
    );
    assert_eq!(
        encoded
            .windows(b"PIGWAL\r\n".len())
            .filter(|window| *window == b"PIGWAL\r\n")
            .count(),
        1
    );
    assert_eq!(replay_key_set(&encoded).unwrap().snapshot, snapshot);
}

#[test]
fn key_map_snapshot_encodes_deterministically_with_exact_current_keys_and_values() {
    let zeta = BTreeMap::from([
        (SearchKey::from(vec![Key::I128(9)]), b"nine".to_vec()),
        (SearchKey::from(vec![Key::I128(-1)]), b"negative".to_vec()),
    ]);
    let alpha = BTreeMap::from([(SearchKey::from(vec![Key::I128(2)]), b"two".to_vec())]);
    let snapshot = KeyMapSnapshot::from([
        (b"zeta".to_vec(), zeta.clone()),
        (b"alpha".to_vec(), alpha.clone()),
    ]);
    let reordered = HashMap::from([(b"alpha".to_vec(), alpha), (b"zeta".to_vec(), zeta)]);

    let encoded = encode_current_key_map_snapshot(&snapshot).unwrap();

    assert_eq!(
        encoded,
        encode_current_key_map_snapshot(&reordered).unwrap()
    );
    assert_eq!(
        encoded
            .windows(b"PIGWAL\r\n".len())
            .filter(|window| *window == b"PIGWAL\r\n")
            .count(),
        1
    );
    assert_eq!(replay_key_map(&encoded).unwrap().snapshot, snapshot);
}

#[test]
fn every_current_snapshot_encoder_preserves_family_time_and_segment_metadata() {
    const GRANULARITY: u64 = 250;
    const LAST_BUCKET: u64 = 42;

    let value = encode_current_key_value_snapshot_with_metadata(
        &KeyValueSnapshot::from([(b"key".to_vec(), b"value".to_vec())]),
        GRANULARITY,
        LAST_BUCKET,
    )
    .unwrap();
    let set = encode_current_key_set_snapshot_with_metadata(
        &KeySetSnapshot::from([(b"set".to_vec(), HashSet::from([b"member".to_vec()]))]),
        GRANULARITY,
        LAST_BUCKET,
    )
    .unwrap();
    let map = encode_current_key_map_snapshot_with_metadata(
        &KeyMapSnapshot::from([(
            b"map".to_vec(),
            BTreeMap::from([(SearchKey::from(1_usize), b"entry".to_vec())]),
        )]),
        GRANULARITY,
        LAST_BUCKET,
    )
    .unwrap();

    for (kind, encoded) in [(1, &value), (2, &set), (3, &map)] {
        let header = &encoded[..V2CodecProbe::HEADER_LEN];
        assert!(V2CodecProbe::header_is_valid(header));
        assert_eq!(V2CodecProbe::header_kind(header), Some(kind));
        assert_eq!(V2CodecProbe::header_granularity(header), Some(GRANULARITY));
        assert_eq!(V2CodecProbe::header_base_bucket(header), Some(LAST_BUCKET));
        assert_eq!(V2CodecProbe::header_segment_id(header), Some(0));
        assert_eq!(V2CodecProbe::header_segment_base(header), Some(0));
        assert_eq!(
            encoded
                .windows(b"PIGWAL\r\n".len())
                .filter(|window| *window == b"PIGWAL\r\n")
                .count(),
            1
        );
    }
    let value_replay = replay_key_value(&value).unwrap();
    let set_replay = replay_key_set(&set).unwrap();
    let map_replay = replay_key_map(&map).unwrap();
    for (granularity, last_bucket) in [
        (value_replay.granularity_nanos, value_replay.last_bucket),
        (set_replay.granularity_nanos, set_replay.last_bucket),
        (map_replay.granularity_nanos, map_replay.last_bucket),
    ] {
        assert_eq!(granularity, GRANULARITY);
        assert_eq!(last_bucket, LAST_BUCKET);
    }
}
