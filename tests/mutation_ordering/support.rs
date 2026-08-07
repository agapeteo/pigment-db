//! Public-API-only helpers for mutation-ordering integration tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use std::collections::{BTreeMap, HashSet};
use std::sync::mpsc::Receiver;
use std::time::Duration;

pub const WATCHDOG: Duration = Duration::from_secs(10);
pub type SetSnapshot = Option<HashSet<Vec<u8>>>;
pub type MapSnapshot = Option<BTreeMap<SearchKey, Vec<u8>>>;

pub fn recv_with_watchdog<T>(receiver: &Receiver<T>, context: &str) -> T {
    receiver
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|error| panic!("{context}: {error}"))
}

pub fn assert_key_value_reopens(
    directory: &std::path::Path,
    key: &[u8],
    expected: &Option<Vec<u8>>,
) {
    for _ in 0..3 {
        let store = DurableKeyValueStore::try_init_new(directory)
            .expect("reopen key/value store")
            .into_store();
        assert_eq!(&store.get(key), expected);
        assert_eq!(store.contains(key), expected.is_some());
        drop(store);
    }
}

pub fn assert_key_set_reopens(directory: &std::path::Path, key: &[u8], expected: &SetSnapshot) {
    for _ in 0..3 {
        let store = DurableKeySetStore::try_init_new(directory)
            .expect("reopen key/set store")
            .into_store();
        assert_eq!(&store.get_hashset(key), expected);
        assert_eq!(store.contains_key(key), expected.is_some());
        drop(store);
    }
}

pub fn assert_key_map_reopens(directory: &std::path::Path, key: &[u8], expected: &MapSnapshot) {
    for _ in 0..3 {
        let store = DurableKeyMapStore::try_init_new(directory)
            .expect("reopen key/sorted-map store")
            .into_store();
        assert_eq!(&store.get_sorted_map(key), expected);
        assert_eq!(store.contains_key(key), expected.is_some());
        drop(store);
    }
}
