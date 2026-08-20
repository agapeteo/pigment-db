//! Key/set storage-inspection tests.

use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::StoreFamily;

#[test]
fn open_key_set_stats_are_family_specialized() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.append(b"set".to_vec(), b"member".to_vec());

    let stats = store.storage_stats().unwrap();

    assert_eq!(stats.family(), StoreFamily::KeySet);
    assert_eq!(stats.sealed_segment_count(), 0);
    assert_eq!(stats.total_bytes(), stats.active_bytes());
}
