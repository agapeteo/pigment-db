//! Key/map storage-inspection tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::StoreFamily;

use crate::maintenance_support::namespace_snapshot;

#[test]
fn open_key_map_stats_are_family_specialized() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"map".to_vec(), 1_usize.into(), b"value".to_vec());

    let before = namespace_snapshot(directory.path()).unwrap();
    let stats = store.storage_stats().unwrap();

    assert_eq!(stats.family(), StoreFamily::KeyMap);
    assert_eq!(stats.sealed_segment_count(), 0);
    assert_eq!(stats.total_bytes(), stats.active_bytes());
    assert_eq!(namespace_snapshot(directory.path()).unwrap(), before);
}
