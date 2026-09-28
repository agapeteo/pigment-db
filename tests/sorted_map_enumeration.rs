//! specs/013: enumeration of the key/sorted-map store (FR-1).
//!
//! Every assertion reads through the public API: the enumeration itself, and `contains_key`,
//! `size()`, `get_sorted_map` and `storage_stats()` to compare it with.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;
use pigment_db::OnlineCompactionOptions;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

/// How long a test waits for another thread before calling it stuck.
const WATCHDOG: Duration = Duration::from_secs(10);
/// Enough keys that a writer touching all of them reaches every part of the map.
const KEYS: usize = 2_048;

/// Each visited key: its map, and how many times it was visited.
type VisitedMaps = HashMap<Vec<u8>, (BTreeMap<SearchKey, Vec<u8>>, usize)>;

fn maps<W: std::io::Write>(store: &DurableKeyMapStore<W>) -> VisitedMaps {
    let mut seen = VisitedMaps::new();
    store.for_each_sorted_map(|key, map| {
        seen.entry(key.to_vec())
            .and_modify(|(_, count)| *count += 1)
            .or_insert((map.clone(), 1));
    });
    seen
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:04}").into_bytes()
}

#[test]
fn for_each_sorted_map_visits_every_published_map_once() {
    let store = DurableKeyMapStore::new_vec_based();
    assert!(maps(&store).is_empty(), "an empty store visits nothing");

    store.put(b"a".to_vec(), SearchKey::from(1), b"one".to_vec());
    store.put(b"a".to_vec(), SearchKey::from(2), b"two".to_vec());
    store.put(b"b".to_vec(), SearchKey::from("only"), b"x".to_vec());
    store.remove_from_sorted_map(b"b".to_vec(), SearchKey::from("only"));
    store.compute(b"c".to_vec(), |map| {
        map.insert(SearchKey::from(b"k".as_slice()), Vec::new());
    });
    store.compute(b"d".to_vec(), |map| map.clear());
    store.put(b"e".to_vec(), SearchKey::from(7), b"gone".to_vec());
    store.remove_key(b"e");

    let seen = maps(&store);
    let mut visited: Vec<_> = seen.keys().cloned().collect();
    visited.sort();
    assert_eq!(
        visited,
        vec![b"a".to_vec(), b"c".to_vec()],
        "a map emptied by its last removal, a compute that leaves nothing, and a removed key are \
         not published"
    );
    assert_eq!(visited.len(), store.size(), "the visit agrees with size()");
    for key in &visited {
        assert!(store.contains_key(key));
        assert_eq!(
            Some(&seen[key].0),
            store.get_sorted_map(key).as_ref(),
            "the visited map is the published map"
        );
    }
    assert!(seen.values().all(|(_, count)| *count == 1));
    assert_eq!(seen[b"a".as_slice()].0.len(), 2);
}

#[test]
fn an_unchanged_map_is_visited_exactly_once_while_other_keys_change() {
    let store = Arc::new(DurableKeyMapStore::new_vec_based());
    for index in 0..2_000 {
        store.put(
            format!("stable-{index:05}").into_bytes(),
            SearchKey::from(index),
            b"m".to_vec(),
        );
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (store, stop, writes) = (store.clone(), stop.clone(), writes.clone());
        std::thread::spawn(move || {
            let mut index = 0_usize;
            while !stop.load(Ordering::Acquire) {
                // A put and then a removal of the same key, so the churn both inserts and removes.
                let key = format!("churn-{:05}", (index / 2) % 500).into_bytes();
                if index.is_multiple_of(2) {
                    store.put(key, SearchKey::from(index), b"m".to_vec());
                } else {
                    store.remove_key(&key);
                }
                writes.fetch_add(1, Ordering::Release);
                index += 1;
            }
        })
    };
    while writes.load(Ordering::Acquire) < 100 {
        std::thread::yield_now();
    }

    let mut stable: HashMap<Vec<u8>, usize> = HashMap::new();
    let (mut first, mut last) = (None, 0);
    store.for_each_sorted_map(|key, _| {
        if key.starts_with(b"stable-") {
            // Read the counter, not the store: this is how many writes landed between the first
            // stable key visited and the last.
            let now = writes.load(Ordering::Acquire);
            first.get_or_insert(now);
            last = now;
            *stable.entry(key.to_vec()).or_default() += 1;
            std::thread::yield_now();
        }
    });
    stop.store(true, Ordering::Release);
    writer.join().unwrap();

    assert!(
        last > first.expect("a stable key was visited"),
        "no write interleaved with the visit, so nothing was tested"
    );
    assert_eq!(stable.len(), 2_000);
    assert!(
        stable.values().all(|count| *count == 1),
        "an unchanged map was visited twice"
    );
}

#[test]
fn reads_progress_while_a_map_visit_is_blocked_and_writers_finish_after_it() {
    let store = Arc::new(DurableKeyMapStore::new_vec_based());
    for index in 0..KEYS {
        store.put(key(index), SearchKey::from(1), b"m".to_vec());
    }
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let visitor = {
        let store = store.clone();
        std::thread::spawn(move || {
            let mut first = true;
            store.for_each_sorted_map(|_, _| {
                if first {
                    first = false;
                    entered_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(WATCHDOG)
                        .expect("the test released the visit");
                }
            });
        })
    };
    entered_rx
        .recv_timeout(WATCHDOG)
        .expect("the visit started");

    let (read_tx, read_rx) = mpsc::channel();
    let reader = {
        let store = store.clone();
        std::thread::spawn(move || {
            let found = (0..KEYS)
                .filter(|index| store.contains_in_map(&key(*index), &SearchKey::from(1)))
                .count();
            read_tx.send(found).unwrap();
        })
    };
    assert_eq!(
        read_rx.recv_timeout(WATCHDOG),
        Ok(KEYS),
        "a read waited on the blocked visit"
    );
    reader.join().unwrap();

    let other_parts = writers_to_other_parts_proceed(&store);

    let (wrote_tx, wrote_rx) = mpsc::channel();
    let writer = {
        let store = store.clone();
        std::thread::spawn(move || {
            for index in 0..KEYS {
                store.put(key(index), SearchKey::from(2), b"n".to_vec());
            }
            wrote_tx.send(()).unwrap();
        })
    };
    // Anti-vacuity: with this many keys the writer reaches the part of the map the blocked visit
    // holds, so it must still be waiting. A visit that held nothing would let it finish here.
    assert!(
        wrote_rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "the writer finished while the visit was blocked, so the visit guarded nothing"
    );
    release_tx.send(()).unwrap();
    visitor.join().unwrap();
    wrote_rx
        .recv_timeout(WATCHDOG)
        .expect("the writer finished after the visit returned");
    writer.join().unwrap();
    for handle in other_parts {
        handle.join().unwrap();
    }
    let seen = maps(&*store);
    assert_eq!(seen.len(), KEYS);
    assert!(seen.values().all(|(map, _)| map.len() == 2));
}

/// While a visit is blocked, 64 writers each rewrite one entry of a different key. A writer waits only
/// when its key shares the guarded part, so at least one finishes unless the visit guards every part.
/// Returns the writers, some of which may still be waiting, to be joined after the visit returns.
fn writers_to_other_parts_proceed<W: std::io::Write + Send + 'static>(
    store: &Arc<DurableKeyMapStore<W>>,
) -> Vec<std::thread::JoinHandle<()>>
where
    DurableKeyMapStore<W>: Send + Sync,
{
    let finished = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..64)
        .map(|index| {
            let (store, finished) = (store.clone(), finished.clone());
            std::thread::spawn(move || {
                store.put(key(index), SearchKey::from(1), b"m".to_vec());
                finished.fetch_add(1, Ordering::Release);
            })
        })
        .collect();
    let deadline = std::time::Instant::now() + WATCHDOG;
    while finished.load(Ordering::Acquire) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "no write to another part finished while the visit was blocked"
        );
        std::thread::yield_now();
    }
    handles
}

#[test]
fn on_a_file_backed_store_compaction_and_other_parts_proceed_while_a_visit_is_blocked() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    for index in 0..KEYS {
        store.put(key(index), SearchKey::from(1), b"m".to_vec());
    }
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let visitor = {
        let store = store.clone();
        std::thread::spawn(move || {
            let mut first = true;
            store.for_each_sorted_map(|_, _| {
                if first {
                    first = false;
                    entered_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(WATCHDOG)
                        .expect("the test released the visit");
                }
            });
        })
    };
    entered_rx
        .recv_timeout(WATCHDOG)
        .expect("the visit started");

    // A compaction first, while no writer waits on the guarded part: its capture takes only read
    // guards, so it completes during the visit.
    let (compacted_tx, compacted_rx) = mpsc::channel();
    let compactor = {
        let store = store.clone();
        std::thread::spawn(move || {
            let outcome = store.try_compact_online(OnlineCompactionOptions::default());
            compacted_tx.send(outcome.is_ok()).unwrap();
        })
    };
    assert_eq!(
        compacted_rx.recv_timeout(WATCHDOG),
        Ok(true),
        "a compaction did not complete while the visit was blocked"
    );
    compactor.join().unwrap();

    let other_parts = writers_to_other_parts_proceed(&store);
    release_tx.send(()).unwrap();
    visitor.join().unwrap();
    for handle in other_parts {
        handle.join().unwrap();
    }
    assert_eq!(maps(&*store).len(), KEYS);
}

#[test]
fn a_reopened_store_is_visited_as_it_was_written() {
    let directory = tempfile::tempdir().unwrap();
    let written = {
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        for index in 0..300 {
            for entry in 0..(index % 5) + 1 {
                store.put(
                    key(index),
                    SearchKey::from(entry),
                    format!("{index}/{entry}").into_bytes(),
                );
            }
        }
        for index in (0..300).step_by(7) {
            store.remove_key(&key(index));
        }
        for index in (3..300).step_by(11) {
            store.remove_from_sorted_map(key(index), SearchKey::from(0));
        }
        let before = store.storage_stats().unwrap().total_bytes();
        let written = maps(&store);
        assert_eq!(
            store.storage_stats().unwrap().total_bytes(),
            before,
            "the visit wrote to the WAL"
        );
        written
    };
    assert!(!written.is_empty());
    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(
        maps(&reopened),
        written,
        "the reopened store is visited as it was written"
    );
    assert_eq!(written.len(), reopened.size());
}

#[test]
fn a_visit_writes_nothing_to_the_wal() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    for index in 0..500 {
        store.put(key(index), SearchKey::from(index), vec![b'v'; 32]);
    }
    let before = store.storage_stats().unwrap().total_bytes();
    let visited = maps(&store).len();
    let after = store.storage_stats().unwrap().total_bytes();
    // Anti-vacuity: the stats see a write, so an unchanged figure is not an unread one.
    store.put(key(0), SearchKey::from(9_999), vec![b'v'; 32]);
    let grown = store.storage_stats().unwrap().total_bytes();

    assert_eq!(visited, 500, "the visit read the store");
    assert_eq!(before, after, "the visit wrote to the WAL");
    assert!(grown > after, "the WAL figure did not see a write");
}
