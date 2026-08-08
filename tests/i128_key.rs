use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::{BytesLen, Key, SearchKey};
use pigment_db::{DurableStoreOptions, RecoveryError, RecoveryStatus, WalSegmentSize};
use std::ops::Bound::Unbounded;

#[test]
fn signed_i128_minimum_decodes_without_unsigned_reinterpretation() {
    const SIGNED_I128_MIN_WIRE: [u8; 20] = [
        10, 0, 0, 0, // Key enum discriminant: I128
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, // i128::MIN, little-endian
    ];

    let decoded: Key =
        bincode::deserialize(&SIGNED_I128_MIN_WIRE).expect("signed I128 key must deserialize");
    let Key::I128(value) = decoded else {
        panic!("wire discriminant 10 must remain Key::I128");
    };
    let signed_value: i128 = value;

    assert_eq!(signed_value, i128::MIN);
}

#[test]
fn signed_i128_boundaries_use_signed_order_and_sixteen_bytes() {
    let values = [i128::MIN, -1, 0, i128::from(u64::MAX) + 1, i128::MAX];
    let keys = values
        .into_iter()
        .map(|value| SearchKey::from(vec![Key::I128(value)]))
        .collect::<Vec<_>>();

    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(keys.iter().all(|key| key.bytes_len() == 16));

    let composite_negative = SearchKey::from(vec![Key::I128(-1), Key::U8(255)]);
    let composite_zero = SearchKey::from(vec![Key::I128(0), Key::U8(0)]);
    assert!(composite_negative < composite_zero);
}

#[test]
fn signed_i128_keys_round_trip_through_v2_reopen() {
    let directory = tempfile::tempdir().expect("create signed I128 store directory");
    let cases = [
        (i128::MIN, b"minimum".to_vec()),
        (-1, b"negative".to_vec()),
        (0, b"zero".to_vec()),
        (i128::from(u64::MAX) + 1, b"above-u64".to_vec()),
        (i128::MAX, b"maximum".to_vec()),
    ];

    {
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize signed I128 map")
            .into_store();
        for (value, expected) in &cases {
            store.put(
                b"signed".to_vec(),
                SearchKey::from(vec![Key::I128(*value)]),
                expected.clone(),
            );
        }
    }

    let wal = std::fs::read(directory.path().join("map.wal.dat")).expect("read signed I128 V2 WAL");
    assert_eq!(v2_actions(&wal), vec![6; cases.len()]);

    for reopening in 1..=3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap_or_else(|error| panic!("reopen {reopening} failed: {error}"))
            .into_store();
        for (value, expected) in &cases {
            assert_eq!(
                reopened.get_element(b"signed", &SearchKey::from(vec![Key::I128(*value)]),),
                Some(expected.clone()),
                "reopen {reopening} value {value}",
            );
        }
        let ordered = reopened
            .range_entries(b"signed", Unbounded, Unbounded)
            .expect("signed map must exist")
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        let expected_order = cases
            .iter()
            .map(|(value, _)| SearchKey::from(vec![Key::I128(*value)]))
            .collect::<Vec<_>>();
        assert_eq!(ordered, expected_order, "reopen {reopening} order");
    }
}

#[test]
fn frozen_earlier_v2_historical_i128_reopens_as_equal_nonnegative_value() {
    let directory = tempfile::tempdir().expect("create historical I128 V2 directory");
    let frozen = decode_hex(include_str!("fixtures/i128_key/earlier-v2-map.hex"));
    std::fs::write(directory.path().join("map.wal.dat"), frozen)
        .expect("install frozen earlier-V2 map fixture");

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("frozen earlier-V2 I128 map must reopen")
        .into_store();
    let historical_max = SearchKey::from(vec![Key::I128(i128::from(u64::MAX))]);

    assert_eq!(
        reopened.get_element(b"retained", &historical_max),
        Some(b"old-max".to_vec())
    );
}

#[test]
fn current_signed_remove_uses_current_action_and_reopens() {
    let directory = tempfile::tempdir().expect("create signed remove directory");
    let removed = SearchKey::from(vec![Key::I128(-9)]);
    let retained = SearchKey::from(vec![Key::I128(9)]);
    {
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize signed remove map")
            .into_store();
        store.put(b"map".to_vec(), removed.clone(), b"remove".to_vec());
        store.put(b"map".to_vec(), retained.clone(), b"retain".to_vec());
        store.remove_from_sorted_map(b"map".to_vec(), removed.clone());
    }

    let wal = std::fs::read(directory.path().join("map.wal.dat")).unwrap();
    assert_eq!(v2_actions(&wal), vec![6, 6, 7]);

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("reopen signed remove map")
        .into_store();
    assert_eq!(reopened.get_element(b"map", &removed), None);
    assert_eq!(
        reopened.get_element(b"map", &retained),
        Some(b"retain".to_vec())
    );
}

#[test]
fn current_signed_compute_uses_current_action_and_reopens() {
    let directory = tempfile::tempdir().expect("create signed compute directory");
    let computed = SearchKey::from(vec![Key::I128(i128::MAX)]);
    {
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize signed compute map")
            .into_store();
        store.compute(b"computed".to_vec(), |map| {
            map.insert(computed.clone(), b"computed".to_vec());
        });
    }

    let wal = std::fs::read(directory.path().join("map.wal.dat")).unwrap();
    assert_eq!(v2_actions(&wal), vec![6]);

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("reopen signed compute map")
        .into_store();
    assert_eq!(
        reopened.get_element(b"computed", &computed),
        Some(b"computed".to_vec())
    );
}

#[test]
fn truncated_current_signed_record_recovers_the_complete_prefix() {
    let directory = tempfile::tempdir().expect("create signed tail-recovery directory");
    let accepted = SearchKey::from(vec![Key::I128(-1)]);
    let torn = SearchKey::from(vec![Key::I128(i128::MAX)]);
    {
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize signed tail-recovery map")
            .into_store();
        store.put(b"map".to_vec(), accepted.clone(), b"accepted".to_vec());
        store.put(b"map".to_vec(), torn.clone(), b"torn".to_vec());
    }
    let path = directory.path().join("map.wal.dat");
    let mut wal = std::fs::read(&path).unwrap();
    wal.pop();
    std::fs::write(&path, wal).unwrap();

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("truncated current signed tail must recover");

    assert_eq!(reopened.status(), RecoveryStatus::Recovered);
    assert_eq!(
        reopened.store().get_element(b"map", &accepted),
        Some(b"accepted".to_vec())
    );
    assert_eq!(reopened.store().get_element(b"map", &torn), None);
}

#[test]
fn mixed_historical_and_current_i128_records_replay_across_segments() {
    let directory = tempfile::tempdir().expect("create mixed I128 segment directory");
    let frozen = decode_hex(include_str!("fixtures/i128_key/earlier-v2-map.hex"));
    std::fs::write(directory.path().join("map.wal.dat"), frozen).unwrap();
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(400_u64).expect("small nonzero segment target"),
    );
    let historical_max = SearchKey::from(vec![Key::I128(i128::from(u64::MAX))]);
    let historical_zero = SearchKey::from(vec![Key::I128(0)]);
    let current_retained = SearchKey::from(vec![Key::I128(-1)]);
    {
        let store = DurableKeyMapStore::try_init_new_with_options(directory.path(), options)
            .expect("open earlier-V2 map for current writes")
            .into_store();
        store.put(
            b"missing".to_vec(),
            historical_zero.clone(),
            b"current-zero".to_vec(),
        );
        store.put(
            b"retained".to_vec(),
            current_retained.clone(),
            b"current-negative".to_vec(),
        );
        store.remove_from_sorted_map(b"retained".to_vec(), historical_max.clone());
    }

    let sealed_historical = std::fs::read(
        directory
            .path()
            .join("map.wal.dat.segment-00000000000000000000"),
    )
    .expect("historical segment must seal before the current write");
    assert_eq!(v2_actions(&sealed_historical), vec![4, 5]);
    let sealed_current = std::fs::read(
        directory
            .path()
            .join("map.wal.dat.segment-00000000000000000001"),
    )
    .expect("current put segment must seal before current remove");
    assert_eq!(v2_actions(&sealed_current), vec![6, 6]);
    let active = std::fs::read(directory.path().join("map.wal.dat")).unwrap();
    assert_eq!(v2_actions(&active), vec![7]);

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("mixed historical/current V2 chain must reopen")
        .into_store();
    assert_eq!(reopened.get_element(b"retained", &historical_max), None);
    assert_eq!(
        reopened.get_element(b"retained", &current_retained),
        Some(b"current-negative".to_vec())
    );
    assert_eq!(
        reopened.get_element(b"missing", &historical_zero),
        Some(b"current-zero".to_vec())
    );
}

#[test]
fn mismatched_unknown_and_wrong_family_current_records_fail_closed() {
    let seed = tempfile::tempdir().unwrap();
    {
        let store = DurableKeyMapStore::try_init_new(seed.path())
            .unwrap()
            .into_store();
        store.put(
            b"map".to_vec(),
            SearchKey::from(vec![Key::I128(-1)]),
            b"value".to_vec(),
        );
    }
    let current = std::fs::read(seed.path().join("map.wal.dat")).unwrap();

    for (case, action) in [("current-payload-as-historical", 4_u8), ("unknown", 8)] {
        let directory = tempfile::tempdir().unwrap();
        let mut corrupted = current.clone();
        rewrite_first_v2_action(&mut corrupted, action);
        std::fs::write(directory.path().join("map.wal.dat"), corrupted).unwrap();
        assert!(
            matches!(
                DurableKeyMapStore::try_init_new(directory.path()),
                Err(RecoveryError::InvalidArtifact { .. })
            ),
            "{case}",
        );
    }

    let historical = decode_hex(include_str!("fixtures/i128_key/earlier-v2-map.hex"));
    let directory = tempfile::tempdir().unwrap();
    let mut mismatched = historical;
    rewrite_first_v2_action(&mut mismatched, 6);
    std::fs::write(directory.path().join("map.wal.dat"), mismatched).unwrap();
    assert!(matches!(
        DurableKeyMapStore::try_init_new(directory.path()),
        Err(RecoveryError::InvalidArtifact { .. })
    ));

    let directory = tempfile::tempdir().unwrap();
    let mut wrong_family = current;
    wrong_family[12] = 1;
    let header_crc = crc32fast::hash(&wrong_family[..60]);
    wrong_family[60..64].copy_from_slice(&header_crc.to_le_bytes());
    std::fs::write(directory.path().join("kv.wal.dat"), wrong_family).unwrap();
    assert!(matches!(
        DurableKeyValueStore::try_init_new(directory.path()),
        Err(RecoveryError::InvalidArtifact { .. })
    ));
}

fn v2_actions(bytes: &[u8]) -> Vec<u8> {
    assert_eq!(&bytes[..8], b"PIGWAL\r\n");
    assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 2);
    let mut actions = Vec::new();
    let mut offset = 64_usize;
    while offset < bytes.len() {
        actions.push(bytes[offset + 3]);
        let payload_len =
            u64::from_le_bytes(bytes[offset + 6..offset + 14].try_into().unwrap()) as usize;
        offset += 66 + payload_len;
    }
    assert_eq!(offset, bytes.len());
    actions
}

fn rewrite_first_v2_action(bytes: &mut [u8], action: u8) {
    let record_start = 64_usize;
    let payload_len = u64::from_le_bytes(
        bytes[record_start + 6..record_start + 14]
            .try_into()
            .unwrap(),
    ) as usize;
    bytes[record_start + 3] = action;
    let crc_start = record_start + 62 + payload_len;
    let crc = crc32fast::hash(&bytes[record_start..crc_start]);
    bytes[crc_start..crc_start + 4].copy_from_slice(&crc.to_le_bytes());
}

fn decode_hex(text: &str) -> Vec<u8> {
    let digits = text.trim().as_bytes();
    assert_eq!(digits.len() % 2, 0);
    digits
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("fixture contains a non-lowercase-hex byte"),
    }
}
