//! specs/012: entry enumeration (FR-1, FR-2) and a set's capacity following its length (FR-3).
//!
//! Every assertion reads through the public API: the enumeration itself, and `capacity()` of the
//! sets it visits.

use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::{
    CompareExchangeEntry, CompareExchangeResult, DurableKeyValueStore,
};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// How long a test waits for another thread before calling it stuck.
const WATCHDOG: Duration = Duration::from_secs(10);
/// Enough keys that a writer touching all of them reaches every part of the map.
const KEYS: usize = 2_048;

fn block_on<F: Future>(future: F) -> F::Output {
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

fn entries(store: &DurableKeyValueStore<Vec<u8>>) -> HashMap<Vec<u8>, (Vec<u8>, usize)> {
    let mut seen: HashMap<Vec<u8>, (Vec<u8>, usize)> = HashMap::new();
    store.for_each_entry(|key, value| {
        seen.entry(key.to_vec())
            .and_modify(|(_, count)| *count += 1)
            .or_insert((value.to_vec(), 1));
    });
    seen
}

/// Each visited set key: its members, its capacity, and how many times it was visited.
type VisitedSets = HashMap<Vec<u8>, (HashSet<Vec<u8>>, usize, usize)>;

fn sets(store: &DurableKeySetStore<Vec<u8>>) -> VisitedSets {
    let mut seen = VisitedSets::new();
    store.for_each_set(|key, set| {
        seen.entry(key.to_vec())
            .and_modify(|(_, _, count)| *count += 1)
            .or_insert((set.clone(), set.capacity(), 1));
    });
    seen
}

fn capacity_of<W: std::io::Write>(store: &DurableKeySetStore<W>, key: &[u8]) -> (usize, usize) {
    let mut found = None;
    store.for_each_set(|visited, set| {
        if visited == key {
            found = Some((set.len(), set.capacity()));
        }
    });
    found.expect("the set is published")
}

fn assert_within_fr3(len: usize, capacity: usize, how: &str) {
    assert!(
        capacity <= 4 * len,
        "{how}: a set of {len} members holds capacity {capacity}, above 4 x len"
    );
}

fn member(index: usize) -> Vec<u8> {
    format!("member-{index:06}").into_bytes()
}

// ---- FR-1 -------------------------------------------------------------------------------------

#[test]
fn for_each_entry_visits_every_published_entry_once() {
    let store = DurableKeyValueStore::new_vec_based();
    store.put(b"alpha".to_vec(), b"one".to_vec());
    store.put(b"beta".to_vec(), Vec::new());
    store.put(b"gamma".to_vec(), b"three".to_vec());
    store.put(b"alpha".to_vec(), b"replaced".to_vec());
    store.remove(b"gamma");
    store.increment_or_init(b"counter".to_vec(), 7).unwrap();

    let seen = entries(&store);
    let expected: HashMap<Vec<u8>, (Vec<u8>, usize)> = [
        (b"alpha".to_vec(), (b"replaced".to_vec(), 1)),
        (b"beta".to_vec(), (Vec::new(), 1)),
        (b"counter".to_vec(), (7_u64.to_ne_bytes().to_vec(), 1)),
    ]
    .into_iter()
    .collect();
    assert_eq!(seen, expected);
}

#[test]
fn for_each_entry_sees_a_completed_batch_and_an_empty_store() {
    let store = DurableKeyValueStore::new_vec_based_with_options(Default::default());
    assert!(entries(&store).is_empty());
    let batch: Vec<CompareExchangeEntry> = [b"x".as_slice(), b"y".as_slice()]
        .into_iter()
        .map(|key| CompareExchangeEntry {
            key: key.to_vec(),
            expected: None,
            replacement: Some(key.to_vec()),
        })
        .collect();
    assert_eq!(
        store.try_compare_exchange_batch(&batch).unwrap(),
        CompareExchangeResult::Applied
    );
    assert_eq!(entries(&store).len(), 2);
}

#[test]
fn an_unchanged_entry_is_visited_exactly_once_while_other_keys_change() {
    let store = Arc::new(DurableKeyValueStore::new_vec_based());
    for index in 0..2_000 {
        store.put(format!("stable-{index:05}").into_bytes(), b"v".to_vec());
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (store, stop, writes) = (store.clone(), stop.clone(), writes.clone());
        std::thread::spawn(move || {
            let mut index = 0_usize;
            while !stop.load(Ordering::Acquire) {
                let key = format!("churn-{:05}", index % 500).into_bytes();
                if index.is_multiple_of(2) {
                    store.put(key, vec![0; index % 64]);
                } else {
                    store.remove(&key);
                }
                writes.fetch_add(1, Ordering::Release);
                index += 1;
            }
        })
    };
    while writes.load(Ordering::Acquire) < 100 {
        std::thread::yield_now();
    }

    let before = writes.load(Ordering::Acquire);
    let mut stable: HashMap<Vec<u8>, usize> = HashMap::new();
    store.for_each_entry(|key, _| {
        if key.starts_with(b"stable-") {
            *stable.entry(key.to_vec()).or_default() += 1;
            // Give the writer room to interleave with the visit.
            std::thread::yield_now();
        }
    });
    let during = writes.load(Ordering::Acquire) - before;
    stop.store(true, Ordering::Release);
    writer.join().unwrap();

    assert!(
        during > 0,
        "the writer made no progress during the visit, so nothing was tested"
    );
    assert_eq!(stable.len(), 2_000);
    assert!(
        stable.values().all(|count| *count == 1),
        "an unchanged entry was visited twice"
    );
}

/// A visit blocked inside `visit` holds one part of the map. Reads of every key still answer, and a
/// writer that had to wait for that part finishes once the visit returns.
#[test]
fn reads_progress_while_an_entry_visit_is_blocked_and_writers_finish_after_it() {
    let store = Arc::new(DurableKeyValueStore::new_vec_based());
    for index in 0..KEYS {
        store.put(format!("key-{index:04}").into_bytes(), vec![index as u8]);
    }
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let visitor = {
        let store = store.clone();
        std::thread::spawn(move || {
            let mut first = true;
            store.for_each_entry(|_, _| {
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
                .filter(|index| store.get(format!("key-{index:04}").as_bytes()).is_some())
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

    let (wrote_tx, wrote_rx) = mpsc::channel();
    let writer = {
        let store = store.clone();
        std::thread::spawn(move || {
            for index in 0..KEYS {
                store.put(format!("key-{index:04}").into_bytes(), b"new".to_vec());
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
    assert!(entries(&store).values().all(|(value, _)| value == b"new"));
}

// ---- FR-2 -------------------------------------------------------------------------------------

#[test]
fn for_each_set_visits_every_published_set_once() {
    let store = DurableKeySetStore::new_vec_based();
    store.append(b"a".to_vec(), b"1".to_vec());
    store.append(b"a".to_vec(), b"2".to_vec());
    store.append(b"b".to_vec(), b"only".to_vec());
    store.remove_from_set(b"b".to_vec(), b"only".to_vec());
    store.append(b"c".to_vec(), Vec::new());

    let seen = sets(&store);
    assert_eq!(
        seen.len(),
        2,
        "a set emptied by its last removal is not published"
    );
    assert_eq!(
        seen[b"a".as_slice()].0,
        [b"1".to_vec(), b"2".to_vec()].into_iter().collect()
    );
    assert_eq!(seen[b"c".as_slice()].0, [Vec::new()].into_iter().collect());
    assert!(seen.values().all(|(_, _, count)| *count == 1));
}

#[test]
fn an_unchanged_set_is_visited_exactly_once_while_other_keys_change() {
    let store = Arc::new(DurableKeySetStore::new_vec_based());
    for index in 0..2_000 {
        store.append(format!("stable-{index:05}").into_bytes(), b"m".to_vec());
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (store, stop, writes) = (store.clone(), stop.clone(), writes.clone());
        std::thread::spawn(move || {
            let mut index = 0_usize;
            while !stop.load(Ordering::Acquire) {
                let key = format!("churn-{:05}", index % 500).into_bytes();
                if index.is_multiple_of(2) {
                    store.append(key, b"m".to_vec());
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

    let before = writes.load(Ordering::Acquire);
    let mut stable: HashMap<Vec<u8>, usize> = HashMap::new();
    store.for_each_set(|key, _| {
        if key.starts_with(b"stable-") {
            *stable.entry(key.to_vec()).or_default() += 1;
            std::thread::yield_now();
        }
    });
    let during = writes.load(Ordering::Acquire) - before;
    stop.store(true, Ordering::Release);
    writer.join().unwrap();

    assert!(
        during > 0,
        "the writer made no progress during the visit, so nothing was tested"
    );
    assert_eq!(stable.len(), 2_000);
    assert!(
        stable.values().all(|count| *count == 1),
        "an unchanged set was visited twice"
    );
}

#[test]
fn reads_progress_while_a_set_visit_is_blocked_and_writers_finish_after_it() {
    let store = Arc::new(DurableKeySetStore::new_vec_based());
    for index in 0..KEYS {
        store.append(format!("key-{index:04}").into_bytes(), b"m".to_vec());
    }
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let visitor = {
        let store = store.clone();
        std::thread::spawn(move || {
            let mut first = true;
            store.for_each_set(|_, _| {
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
                .filter(|index| store.contains_in_set(format!("key-{index:04}").as_bytes(), b"m"))
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

    let (wrote_tx, wrote_rx) = mpsc::channel();
    let writer = {
        let store = store.clone();
        std::thread::spawn(move || {
            for index in 0..KEYS {
                store.append(format!("key-{index:04}").into_bytes(), b"n".to_vec());
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
    assert!(sets(&store).values().all(|(set, _, _)| set.len() == 2));
}

// ---- FR-3 -------------------------------------------------------------------------------------

/// A set grown to `peak` members and then cut to `keep` by `cut`.
fn grown_then_cut(
    peak: usize,
    keep: usize,
    cut: impl Fn(&DurableKeySetStore<Vec<u8>>, &[u8], Vec<u8>),
) -> (usize, usize) {
    let store = DurableKeySetStore::new_vec_based();
    for index in 0..peak {
        store.append(b"k".to_vec(), member(index));
    }
    let (_, peak_capacity) = capacity_of(&store, b"k");
    assert!(peak_capacity >= peak, "fixture: the set did not grow");
    for index in keep..peak {
        cut(&store, b"k", member(index));
    }
    capacity_of(&store, b"k")
}

#[test]
fn a_set_cut_down_by_remove_from_set_releases_its_spare_capacity() {
    let (len, capacity) = grown_then_cut(10_000, 10, |store, key, member| {
        store.try_remove_from_set(key.to_vec(), member).unwrap()
    });
    assert_eq!(len, 10);
    assert_within_fr3(len, capacity, "try_remove_from_set");

    let (len, capacity) = grown_then_cut(10_000, 10, |store, key, member| {
        store.remove_from_set(key.to_vec(), member)
    });
    assert_within_fr3(len, capacity, "remove_from_set");
}

#[test]
fn a_set_cut_down_by_the_callback_removal_releases_its_spare_capacity() {
    let (len, capacity) = grown_then_cut(10_000, 10, |store, key, member| {
        store
            .try_remove_from_set_callback(key.to_vec(), member, |_| panic!("the key stays"))
            .unwrap()
    });
    assert_eq!(len, 10);
    assert_within_fr3(len, capacity, "try_remove_from_set_callback");
}

#[test]
fn a_set_cut_down_through_each_compute_variant_releases_its_spare_capacity() {
    let cut = |set: &mut HashSet<Vec<u8>>| set.retain(|member| member < &member_boundary());
    let fresh = || {
        let store = DurableKeySetStore::new_vec_based();
        for index in 0..10_000 {
            store.append(b"k".to_vec(), member(index));
        }
        store
    };

    let store = fresh();
    store.try_compute(b"k".to_vec(), cut).unwrap();
    let (len, capacity) = capacity_of(&store, b"k");
    assert_eq!(len, 10);
    assert_within_fr3(len, capacity, "try_compute");

    let store = fresh();
    store.try_compute_if_present(b"k".to_vec(), cut).unwrap();
    let (len, capacity) = capacity_of(&store, b"k");
    assert_within_fr3(len, capacity, "try_compute_if_present");

    let store = fresh();
    block_on(store.try_compute_async(b"k".to_vec(), async |set: &mut HashSet<Vec<u8>>| cut(set)))
        .unwrap();
    let (len, capacity) = capacity_of(&store, b"k");
    assert_within_fr3(len, capacity, "try_compute_async");
}

fn member_boundary() -> Vec<u8> {
    member(10)
}

/// A callback may reserve capacity as well as change members; the published set is held to FR-3
/// either way, on the occupied path and the vacant one.
#[test]
fn a_compute_callback_that_reserves_does_not_publish_its_reservation() {
    let reserve_and_add = |set: &mut HashSet<Vec<u8>>| {
        set.reserve(100_000);
        set.insert(b"added".to_vec());
    };

    let store = DurableKeySetStore::new_vec_based();
    store.append(b"occupied".to_vec(), b"m".to_vec());
    store
        .try_compute(b"occupied".to_vec(), reserve_and_add)
        .unwrap();
    store
        .try_compute(b"vacant".to_vec(), reserve_and_add)
        .unwrap();
    store
        .try_compute_if_absent(b"absent".to_vec(), reserve_and_add)
        .unwrap();
    store.append(b"present".to_vec(), b"m".to_vec());
    store
        .try_compute_if_present(b"present".to_vec(), reserve_and_add)
        .unwrap();
    block_on(
        store.try_compute_async(b"async".to_vec(), async |set: &mut HashSet<Vec<u8>>| {
            reserve_and_add(set)
        }),
    )
    .unwrap();

    for key in [
        b"occupied".as_slice(),
        b"vacant",
        b"absent",
        b"present",
        b"async",
    ] {
        let (len, capacity) = capacity_of(&store, key);
        assert_within_fr3(len, capacity, &String::from_utf8_lossy(key));
    }
}

#[test]
fn a_reopened_store_holds_every_set_within_its_capacity_bound() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        for index in 0..10_000 {
            store.append(b"k".to_vec(), member(index));
        }
        for index in 10..10_000 {
            store.remove_from_set(b"k".to_vec(), member(index));
        }
    }
    let reopened = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    let (len, capacity) = capacity_of(&reopened, b"k");
    assert_eq!(len, 10);
    assert_within_fr3(len, capacity, "reopen");
}
