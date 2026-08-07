use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::model::SearchKey;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub type SetSnapshot = Option<HashSet<Vec<u8>>>;
pub type MapSnapshot = Option<BTreeMap<SearchKey, Vec<u8>>>;

pub fn callback_count() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

pub fn increment(counter: &Arc<AtomicUsize>) {
    counter.fetch_add(1, Ordering::SeqCst);
}

pub fn count(counter: &Arc<AtomicUsize>) -> usize {
    counter.load(Ordering::SeqCst)
}

pub fn wal_bytes(directory: &Path, file_name: &str) -> Vec<u8> {
    std::fs::read(directory.join(file_name)).unwrap_or_default()
}

pub fn assert_set_reopens(directory: &Path, key: &[u8], expected: &SetSnapshot) {
    for _ in 0..3 {
        let store = DurableKeySetStore::try_init_new(directory)
            .expect("reopen key/set store")
            .into_store();
        assert_eq!(&store.get_hashset(key), expected);
        assert_eq!(store.contains_key(key), expected.is_some());
        drop(store);
    }
}

pub fn assert_map_reopens(directory: &Path, key: &[u8], expected: &MapSnapshot) {
    for _ in 0..3 {
        let store = DurableKeyMapStore::try_init_new(directory)
            .expect("reopen key/map store")
            .into_store();
        assert_eq!(&store.get_sorted_map(key), expected);
        assert_eq!(store.contains_key(key), expected.is_some());
        drop(store);
    }
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
