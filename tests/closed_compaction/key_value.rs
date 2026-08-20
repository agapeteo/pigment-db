//! Key/value closed-compaction tests.

use std::io::Write as _;
use std::time::Duration;

use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    compact_directory_in_place, CleanupStatus, ClosedCompactionOptions, DurableStoreOptions,
    StoreFamily, TimestampGranularity,
};

fn last_v2_bucket(bytes: &[u8]) -> u64 {
    let mut last = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let mut cursor = 64_usize;
    while cursor < bytes.len() {
        let frame = &bytes[cursor..];
        let payload_len =
            usize::try_from(u64::from_le_bytes(frame[6..14].try_into().unwrap())).unwrap();
        last = u64::from_le_bytes(frame[46..54].try_into().unwrap());
        cursor += 66 + payload_len;
    }
    assert_eq!(cursor, bytes.len());
    last
}

#[test]
fn active_key_value_compaction_preserves_timestamp_metadata_and_repeats_idempotently() {
    let directory = tempfile::tempdir().unwrap();
    let granularity_nanos = 1_000_000_u64;
    let options = DurableStoreOptions::default().with_timestamp_granularity(
        TimestampGranularity::try_from(Duration::from_nanos(granularity_nanos)).unwrap(),
    );
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"alpha".to_vec(), b"one".to_vec());
    store.put(b"beta".to_vec(), b"two".to_vec());
    drop(store);
    let active = directory.path().join("kv.wal.dat");
    let before_bucket = last_v2_bucket(&std::fs::read(&active).unwrap());

    let first =
        compact_directory_in_place(directory.path(), ClosedCompactionOptions::default()).unwrap();
    let compacted = std::fs::read(&active).unwrap();
    assert_eq!(first.families()[0].family(), StoreFamily::KeyValue);
    assert_eq!(first.families()[0].sealed_segments_removed(), 0);
    assert_eq!(first.families()[0].cleanup(), CleanupStatus::Complete);
    assert_eq!(
        u64::from_le_bytes(compacted[16..24].try_into().unwrap()),
        granularity_nanos
    );
    assert_eq!(
        u64::from_le_bytes(compacted[24..32].try_into().unwrap()),
        before_bucket
    );

    let second =
        compact_directory_in_place(directory.path(), ClosedCompactionOptions::default()).unwrap();
    assert_eq!(std::fs::read(&active).unwrap(), compacted);
    assert_eq!(second.families()[0].sealed_segments_removed(), 0);
    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
        assert_eq!(reopened.get(b"beta"), Some(b"two".to_vec()));
    }
}

#[test]
fn recoverable_key_value_tail_compacts_only_the_accepted_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"alpha".to_vec(), b"one".to_vec());
    drop(store);
    let active = directory.path().join("kv.wal.dat");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&active)
        .unwrap()
        .write_all(&[0xa7])
        .unwrap();

    compact_directory_in_place(directory.path(), ClosedCompactionOptions::default()).unwrap();

    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
}
