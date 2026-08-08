use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{DurableStoreOptions, TimestampGranularity, WalSegmentSize};
use std::time::Duration;

fn v2_record_len(bytes: &[u8], start: usize) -> usize {
    let payload_len = u64::from_le_bytes(bytes[start + 6..start + 14].try_into().unwrap()) as usize;
    66 + payload_len
}

fn rewrite_v2_record_timestamp(bytes: &mut [u8], start: usize, timestamp_bucket: u64) {
    let record_len = v2_record_len(bytes, start);
    bytes[start + 46..start + 54].copy_from_slice(&timestamp_bucket.to_le_bytes());
    let crc_start = start + record_len - 4;
    let crc = crc32fast::hash(&bytes[start..crc_start]);
    bytes[crc_start..start + record_len].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn wal_segment_size_is_validated_and_defaults_to_one_gibibyte() {
    assert_eq!(WalSegmentSize::default().as_bytes(), 1024 * 1024 * 1024);
    assert!(WalSegmentSize::try_from(0_u64).is_err());

    let configured = WalSegmentSize::try_from(4096_u64).expect("nonzero segment size");
    let options = DurableStoreOptions::default().with_wal_segment_size(configured);
    assert_eq!(options.wal_segment_size(), configured);
}

#[test]
fn fresh_key_value_store_writes_v2_and_reopens() {
    let directory = tempfile::tempdir().expect("create V2 key/value directory");
    let key = b"key".to_vec();
    let value = b"value".to_vec();

    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize fresh V2 key/value store")
        .into_store();
    store.put(key.clone(), value.clone());
    drop(store);

    let wal = std::fs::read(directory.path().join("kv.wal.dat")).expect("read V2 WAL");
    assert_eq!(u16::from_le_bytes(wal[8..10].try_into().unwrap()), 2);

    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .expect("reopen V2 key/value store")
        .into_store();
    assert_eq!(reopened.get(&key), Some(value));
}

#[test]
fn rotation_seals_a_numbered_segment_and_reopens_the_complete_chain() {
    let directory = tempfile::tempdir().expect("create segmented V2 directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );

    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("initialize segmented V2 store")
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    store.put(b"second".to_vec(), b"two".to_vec());
    drop(store);

    let sealed = directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000");
    assert!(
        sealed.is_file(),
        "rotation must retain immutable segment zero"
    );
    assert!(directory.path().join("kv.wal.dat").is_file());

    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("reopen complete segment chain")
        .into_store();
    assert_eq!(reopened.get(b"first"), Some(b"one".to_vec()));
    assert_eq!(reopened.get(b"second"), Some(b"two".to_vec()));
}

#[test]
fn startup_rejects_a_v2_history_missing_its_leading_segment() {
    let directory = tempfile::tempdir().expect("create incomplete V2 directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );

    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("initialize segmented V2 store")
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    store.put(b"second".to_vec(), b"two".to_vec());
    drop(store);

    let missing = directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000");
    std::fs::remove_file(&missing).expect("remove leading segment");
    let active_path = directory.path().join("kv.wal.dat");
    let active_before = std::fs::read(&active_path).expect("capture surviving active segment");

    let result = DurableKeyValueStore::try_init_new_with_options(directory.path(), options);
    assert!(result.is_err(), "an incomplete V2 history must not open");
    assert_eq!(
        std::fs::read(active_path).expect("read preserved active segment"),
        active_before,
        "rejection must preserve the surviving evidence"
    );
}

#[test]
fn set_and_map_startup_reject_v2_histories_missing_their_leading_segment() {
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );

    let set_directory = tempfile::tempdir().expect("create incomplete V2 set directory");
    let set = DurableKeySetStore::try_init_new_with_options(set_directory.path(), options)
        .expect("initialize segmented V2 set")
        .into_store();
    set.append(b"set".to_vec(), b"first".to_vec());
    set.append(b"set".to_vec(), b"second".to_vec());
    drop(set);
    std::fs::remove_file(
        set_directory
            .path()
            .join("set.wal.dat.segment-00000000000000000000"),
    )
    .expect("remove leading set segment");
    assert!(
        DurableKeySetStore::try_init_new_with_options(set_directory.path(), options).is_err(),
        "an incomplete set history must not open"
    );

    let map_directory = tempfile::tempdir().expect("create incomplete V2 map directory");
    let map = DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options)
        .expect("initialize segmented V2 map")
        .into_store();
    map.put(b"map".to_vec(), SearchKey::from(0_usize), b"first".to_vec());
    map.put(
        b"map".to_vec(),
        SearchKey::from(1_usize),
        b"second".to_vec(),
    );
    drop(map);
    std::fs::remove_file(
        map_directory
            .path()
            .join("map.wal.dat.segment-00000000000000000000"),
    )
    .expect("remove leading map segment");
    assert!(
        DurableKeyMapStore::try_init_new_with_options(map_directory.path(), options).is_err(),
        "an incomplete map history must not open"
    );
}

#[test]
fn startup_rejects_a_gap_in_a_v2_segment_chain() {
    let directory = tempfile::tempdir().expect("create gapped V2 directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("initialize segmented V2 store")
        .into_store();
    for index in 0..3 {
        store.put(format!("key-{index}").into_bytes(), vec![index; 8]);
    }
    drop(store);

    let missing = directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000001");
    assert!(missing.is_file(), "three writes must create segment one");
    std::fs::remove_file(missing).expect("remove middle segment");

    assert!(
        DurableKeyValueStore::try_init_new_with_options(directory.path(), options).is_err(),
        "a gapped V2 history must not open"
    );
}

#[test]
fn oversized_mutation_stays_intact_and_rotates_before_the_following_mutation() {
    let directory = tempfile::tempdir().expect("create oversized V2 directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(80_u64).expect("small nonzero segment target"),
    );
    let large_value = vec![0x5a; 4096];

    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("initialize oversized V2 store")
        .into_store();
    store.put(b"large".to_vec(), large_value.clone());
    assert!(
        !directory
            .path()
            .join("kv.wal.dat.segment-00000000000000000000")
            .exists(),
        "an empty segment must accept one oversized logical mutation"
    );
    store.put(b"next".to_vec(), b"value".to_vec());
    drop(store);

    assert!(directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000")
        .is_file());
    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("reopen oversized V2 chain")
        .into_store();
    assert_eq!(reopened.get(b"large"), Some(large_value));
    assert_eq!(reopened.get(b"next"), Some(b"value".to_vec()));
}

#[test]
fn rotation_never_splits_an_atomic_compute_group() {
    let directory = tempfile::tempdir().expect("create compute-group V2 directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(180_u64).expect("small nonzero segment target"),
    );

    let store = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .expect("initialize compute-group V2 store")
        .into_store();
    store.append(b"set".to_vec(), b"existing".to_vec());
    store.compute(b"set".to_vec(), |values| {
        values.insert(b"first".to_vec());
        values.insert(b"second".to_vec());
    });
    drop(store);

    let sealed = std::fs::read(
        directory
            .path()
            .join("set.wal.dat.segment-00000000000000000000"),
    )
    .expect("read sealed pre-compute segment");
    let active =
        std::fs::read(directory.path().join("set.wal.dat")).expect("read active compute segment");
    assert_eq!(
        u64::from_le_bytes(active[40..48].try_into().unwrap()),
        sealed.len() as u64
    );

    let reopened = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .expect("reopen compute-group V2 chain")
        .into_store();
    let values = reopened.get_hashset(b"set").expect("set must survive");
    assert!(values.contains(b"existing".as_slice()));
    assert!(values.contains(b"first".as_slice()));
    assert!(values.contains(b"second".as_slice()));
}

#[test]
fn startup_rejects_mismatched_timestamps_inside_an_atomic_v2_group() {
    let directory = tempfile::tempdir().expect("create timestamp-corruption directory");
    let store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize V2 set store")
        .into_store();
    store.compute(b"set".to_vec(), |values| {
        values.insert(b"one".to_vec());
        values.insert(b"two".to_vec());
    });
    drop(store);

    let path = directory.path().join("set.wal.dat");
    let mut bytes = std::fs::read(&path).expect("read V2 group");
    let first_start = 64_usize;
    let second_start = first_start + v2_record_len(&bytes, first_start);
    rewrite_v2_record_timestamp(&mut bytes, second_start, 1);
    std::fs::write(&path, &bytes).expect("write CRC-valid timestamp corruption");

    assert!(
        DurableKeySetStore::try_init_new(directory.path()).is_err(),
        "one atomic group must carry one timestamp"
    );
    assert_eq!(
        std::fs::read(path).expect("read preserved corrupt artifact"),
        bytes,
        "invalid timestamp evidence must remain byte-identical"
    );
}

#[test]
fn startup_rejects_a_v2_group_whose_timestamp_moves_backward() {
    let directory = tempfile::tempdir().expect("create timestamp-regression directory");
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize V2 key/value store")
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    store.put(b"second".to_vec(), b"two".to_vec());
    drop(store);

    let path = directory.path().join("kv.wal.dat");
    let mut bytes = std::fs::read(&path).expect("read V2 groups");
    let first_start = 64_usize;
    let second_start = first_start + v2_record_len(&bytes, first_start);
    let first_timestamp = u64::from_le_bytes(
        bytes[first_start + 46..first_start + 54]
            .try_into()
            .unwrap(),
    );
    rewrite_v2_record_timestamp(
        &mut bytes,
        second_start,
        first_timestamp
            .checked_sub(1)
            .expect("system timestamp bucket must be positive"),
    );
    std::fs::write(&path, &bytes).expect("write CRC-valid timestamp regression");

    assert!(
        DurableKeyValueStore::try_init_new(directory.path()).is_err(),
        "accepted timestamp buckets must not move backward"
    );
    assert_eq!(
        std::fs::read(path).expect("read preserved corrupt artifact"),
        bytes,
        "invalid timestamp evidence must remain byte-identical"
    );
}

#[test]
fn startup_rejects_a_segment_base_bucket_that_breaks_timestamp_continuity() {
    let directory = tempfile::tempdir().expect("create base-bucket-corruption directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("initialize segmented V2 store")
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    store.put(b"second".to_vec(), b"two".to_vec());
    drop(store);

    let active_path = directory.path().join("kv.wal.dat");
    let mut active = std::fs::read(&active_path).expect("read active V2 segment");
    let base_bucket = u64::from_le_bytes(active[24..32].try_into().unwrap());
    active[24..32].copy_from_slice(&(base_bucket ^ 1).to_le_bytes());
    let header_crc = crc32fast::hash(&active[..60]);
    active[60..64].copy_from_slice(&header_crc.to_le_bytes());
    std::fs::write(&active_path, &active).expect("write CRC-valid base-bucket corruption");

    assert!(
        DurableKeyValueStore::try_init_new_with_options(directory.path(), options).is_err(),
        "each segment base bucket must equal the preceding accepted bucket"
    );
    assert_eq!(
        std::fs::read(active_path).expect("read preserved corrupt segment"),
        active,
        "invalid timestamp evidence must remain byte-identical"
    );
}

#[test]
fn startup_promotes_a_complete_rotation_staging_segment_when_active_is_absent() {
    let directory = tempfile::tempdir().expect("create interrupted-rotation directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );
    {
        let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        store.put(b"first".to_vec(), b"one".to_vec());
        store.put(b"second".to_vec(), b"two".to_vec());
    }

    let active_path = directory.path().join("kv.wal.dat");
    let active = std::fs::read(&active_path).unwrap();
    let sealed_one = directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000001");
    std::fs::rename(&active_path, &sealed_one).unwrap();
    let mut next_header: [u8; 64] = active[..64].try_into().unwrap();
    next_header[32..40].copy_from_slice(&2_u64.to_le_bytes());
    let next_base = u64::from_le_bytes(active[40..48].try_into().unwrap()) + active.len() as u64;
    next_header[40..48].copy_from_slice(&next_base.to_le_bytes());
    let crc = crc32fast::hash(&next_header[..60]);
    next_header[60..64].copy_from_slice(&crc.to_le_bytes());
    let staging_path = directory.path().join(".kv.wal.dat.next");
    std::fs::write(&staging_path, next_header).unwrap();

    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("complete rotation staging must restore the missing active segment");

    assert_eq!(reopened.status(), pigment_db::RecoveryStatus::Recovered);
    assert_eq!(reopened.store().get(b"first"), Some(b"one".to_vec()));
    assert_eq!(reopened.store().get(b"second"), Some(b"two".to_vec()));
    assert!(active_path.is_file());
    assert!(!staging_path.exists());
}

#[test]
fn startup_discards_only_a_truncated_final_v2_mutation() {
    let directory = tempfile::tempdir().expect("create truncated V2 directory");
    {
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"accepted".to_vec(), b"value".to_vec());
        store.put(b"torn".to_vec(), b"discard".to_vec());
    }
    let active_path = directory.path().join("kv.wal.dat");
    let mut bytes = std::fs::read(&active_path).unwrap();
    bytes.pop();
    std::fs::write(&active_path, bytes).unwrap();

    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .expect("a truncated final V2 mutation must recover its complete prefix");

    assert_eq!(reopened.status(), pigment_db::RecoveryStatus::Recovered);
    assert_eq!(reopened.store().get(b"accepted"), Some(b"value".to_vec()));
    assert_eq!(reopened.store().get(b"torn"), None);
}

#[test]
fn startup_discards_an_entire_truncated_v2_compute_group() {
    let directory = tempfile::tempdir().expect("create truncated V2 group directory");
    {
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"set".to_vec(), b"existing".to_vec());
        store.compute(b"set".to_vec(), |values| {
            values.insert(b"first".to_vec());
            values.insert(b"second".to_vec());
        });
    }
    let active_path = directory.path().join("set.wal.dat");
    let mut bytes = std::fs::read(&active_path).unwrap();
    bytes.pop();
    std::fs::write(&active_path, bytes).unwrap();

    let reopened = DurableKeySetStore::try_init_new(directory.path())
        .expect("a truncated V2 group must recover before its mutation start");

    let values = reopened.store().get_hashset(b"set").unwrap();
    assert_eq!(
        values,
        std::collections::HashSet::from([b"existing".to_vec()])
    );
}

#[test]
fn segmented_startup_discards_only_a_truncated_final_active_mutation() {
    let directory = tempfile::tempdir().expect("create truncated segmented V2 directory");
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );
    {
        let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        store.put(b"sealed".to_vec(), b"accepted".to_vec());
        store.put(b"active".to_vec(), b"discard".to_vec());
    }

    let active_path = directory.path().join("kv.wal.dat");
    let mut bytes = std::fs::read(&active_path).unwrap();
    bytes.pop();
    std::fs::write(&active_path, bytes).unwrap();

    let reopened = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .expect("a truncated active mutation must preserve the sealed segment chain");

    assert_eq!(reopened.status(), pigment_db::RecoveryStatus::Recovered);
    assert_eq!(reopened.store().get(b"sealed"), Some(b"accepted".to_vec()));
    assert_eq!(reopened.store().get(b"active"), None);
}

#[test]
fn reopen_without_override_inherits_the_active_segments_granularity() {
    let directory = tempfile::tempdir().expect("create granularity-chain directory");
    let segment_size = WalSegmentSize::try_from(170_u64).unwrap();
    let default_options = DurableStoreOptions::default().with_wal_segment_size(segment_size);
    {
        let store =
            DurableKeyValueStore::try_init_new_with_options(directory.path(), default_options)
                .unwrap()
                .into_store();
        store.put(b"first".to_vec(), b"one".to_vec());
    }

    let one_second = TimestampGranularity::try_from(Duration::from_secs(1)).unwrap();
    let changed_options = default_options.with_timestamp_granularity(one_second);
    {
        let store =
            DurableKeyValueStore::try_init_new_with_options(directory.path(), changed_options)
                .unwrap()
                .into_store();
        store.put(b"second".to_vec(), b"two".to_vec());
    }

    {
        let store =
            DurableKeyValueStore::try_init_new_with_options(directory.path(), default_options)
                .unwrap()
                .into_store();
        store.put(b"third".to_vec(), b"three".to_vec());
    }

    let active = std::fs::read(directory.path().join("kv.wal.dat")).unwrap();
    assert_eq!(
        u64::from_le_bytes(active[16..24].try_into().unwrap()),
        1_000_000_000
    );
}
