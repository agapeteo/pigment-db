use pigment_db::key_value_store::{
    CompareExchangeEntry, CompareExchangeResult, DurableKeyValueStore,
};
use pigment_db::{DurableStoreOptions, OnlineCompactionOptions, WalSegmentSize};

fn batch() -> Vec<CompareExchangeEntry> {
    ["book", "usage", "receipt"]
        .into_iter()
        .map(|key| CompareExchangeEntry {
            key: key.as_bytes().to_vec(),
            expected: None,
            replacement: Some(b"committed".to_vec()),
        })
        .collect()
}

#[test]
fn batch_survives_rotation_compaction_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"prefix".to_vec(), vec![0; 80]);
    assert_eq!(
        store.try_compare_exchange_batch(&batch()).unwrap(),
        CompareExchangeResult::Applied
    );
    store.put(b"suffix".to_vec(), vec![1; 80]);
    store
        .try_compact_online(OnlineCompactionOptions::default())
        .unwrap();
    drop(store);
    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    for item in batch() {
        assert_eq!(reopened.get(&item.key), item.replacement);
    }
    assert_eq!(reopened.size(), 5);
}

#[test]
fn every_truncated_v2_batch_recovers_only_its_previous_prefix() {
    let source = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(source.path())
        .unwrap()
        .into_store();
    store.put(b"prefix".to_vec(), b"safe".to_vec());
    let wal_path = source.path().join("kv.wal.dat");
    let prefix_len = std::fs::metadata(&wal_path).unwrap().len() as usize;
    store.try_compare_exchange_batch(&batch()).unwrap();
    drop(store);
    let wal = std::fs::read(wal_path).unwrap();
    for cut in prefix_len..=wal.len() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("kv.wal.dat"), &wal[..cut]).unwrap();
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"prefix"), Some(b"safe".to_vec()));
        for item in batch() {
            assert_eq!(
                reopened.get(&item.key),
                if cut == wal.len() {
                    item.replacement
                } else {
                    None
                },
                "cut {cut}"
            );
        }
    }
}
