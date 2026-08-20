//! Key/set closed-compaction tests.

use std::io::Write as _;

use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::{
    compact_directory_in_place, ClosedCompactionOptions, DurableStoreOptions, StoreFamily,
    WalSegmentSize,
};

#[test]
fn segmented_and_recoverable_tail_key_set_compaction_preserves_membership() {
    for recoverable_tail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let options = DurableStoreOptions::default()
            .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
        let store = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
            .unwrap()
            .into_store();
        store.append(b"group".to_vec(), b"red".to_vec());
        store.append(b"group".to_vec(), b"blue".to_vec());
        drop(store);
        let active = directory.path().join("set.wal.dat");
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

        assert_eq!(outcome.families()[0].family(), StoreFamily::KeySet);
        assert_eq!(outcome.families()[0].sealed_segments_removed(), 1);
        assert!(!directory
            .path()
            .join("set.wal.dat.segment-00000000000000000000")
            .exists());
        for _ in 0..3 {
            let reopened = DurableKeySetStore::try_init_new(directory.path())
                .unwrap()
                .into_store();
            let values = reopened.get_hashset(b"group").unwrap();
            assert!(values.contains(b"red".as_slice()));
            assert!(values.contains(b"blue".as_slice()));
        }
    }
}
