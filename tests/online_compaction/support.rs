//! Shared online-compaction test support.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const LARGE_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

pub fn large_snapshot_value() -> Vec<u8> {
    vec![0x5a; LARGE_SNAPSHOT_BYTES]
}

pub fn wait_until_started(started: &AtomicBool) {
    while !started.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
}

pub fn run_until_finished<F>(started: &AtomicBool, finished: &AtomicBool, mut operation: F) -> usize
where
    F: FnMut(usize),
{
    wait_until_started(started);
    let mut completed = 0;
    while !finished.load(Ordering::Acquire) && completed < 10_000 {
        operation(completed);
        completed += 1;
        std::thread::yield_now();
    }
    completed
}

pub fn signals() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    )
}

pub fn assert_value_reopens(directory: &Path, expected: &Option<Vec<u8>>) {
    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory)
            .expect("reopen compacted key/value store")
            .into_store();
        assert_eq!(&reopened.get(b"live"), expected);
    }
}

pub fn assert_set_reopens(directory: &Path, expected: &Option<HashSet<Vec<u8>>>) {
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory)
            .expect("reopen compacted key/set store")
            .into_store();
        assert_eq!(&reopened.get_hashset(b"live"), expected);
    }
}

pub fn assert_map_reopens(directory: &Path, expected: &Option<BTreeMap<SearchKey, Vec<u8>>>) {
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory)
            .expect("reopen compacted key/map store")
            .into_store();
        assert_eq!(&reopened.get_sorted_map(b"live"), expected);
    }
}
