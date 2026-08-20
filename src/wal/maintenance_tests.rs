//! Private WAL maintenance behavior tests.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::format::V2CodecProbe;
use super::replay::{
    encode_current_key_map_snapshot, encode_current_key_set_snapshot,
    encode_current_key_value_snapshot, replay_key_map, replay_key_set, replay_key_value,
    KeyMapSnapshot, KeySetSnapshot, KeyValueSnapshot,
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
