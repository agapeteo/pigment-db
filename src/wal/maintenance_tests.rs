//! Private WAL maintenance behavior tests.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::format::V2CodecProbe;
use super::replay::{
    encode_current_key_map_snapshot, encode_current_key_map_snapshot_with_metadata,
    encode_current_key_set_snapshot, encode_current_key_set_snapshot_with_metadata,
    encode_current_key_value_snapshot, encode_current_key_value_snapshot_with_metadata,
    replay_key_map, replay_key_set, replay_key_value, KeyMapSnapshot, KeySetSnapshot,
    KeyValueSnapshot,
};
use crate::model::{Key, SearchKey};

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
