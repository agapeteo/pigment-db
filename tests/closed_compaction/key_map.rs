//! Key/map closed-compaction tests.

use std::io::Write as _;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;
use pigment_db::{
    compact_directory_in_place, ClosedCompactionOptions, DurableStoreOptions, StoreFamily,
    WalSegmentSize,
};

#[test]
fn segmented_and_recoverable_tail_key_map_compaction_preserves_sorted_entries() {
    for recoverable_tail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let options = DurableStoreOptions::default()
            .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
        let store = DurableKeyMapStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        store.put(b"book".to_vec(), SearchKey::from(1), b"one".to_vec());
        store.put(b"book".to_vec(), SearchKey::from(2), b"two".to_vec());
        drop(store);
        let active = directory.path().join("map.wal.dat");
        if recoverable_tail {
            std::fs::OpenOptions::new()
                .append(true)
                .open(&active)
                .unwrap()
                .write_all(&[0xa7])
                .unwrap();
        }

        let outcome =
            compact_directory_in_place(directory.path(), ClosedCompactionOptions::default())
                .unwrap();

        assert_eq!(outcome.families()[0].family(), StoreFamily::KeyMap);
        assert_eq!(outcome.families()[0].sealed_segments_removed(), 1);
        assert!(!directory
            .path()
            .join("map.wal.dat.segment-00000000000000000000")
            .exists());
        for _ in 0..3 {
            let reopened = DurableKeyMapStore::try_init_new(directory.path())
                .unwrap()
                .into_store();
            assert_eq!(
                reopened.get_element(b"book", &SearchKey::from(1)),
                Some(b"one".to_vec())
            );
            assert_eq!(
                reopened.get_element(b"book", &SearchKey::from(2)),
                Some(b"two".to_vec())
            );
        }
    }
}
