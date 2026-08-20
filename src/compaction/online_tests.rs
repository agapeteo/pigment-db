//! Private online-compaction behavior tests.

use std::future::Future;
use std::pin::pin;
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::maintenance_coordination::MaintenanceCoordinator;
use crate::model::SearchKey;

#[test]
fn coordinator_is_constant_per_instance_exclusive_and_immediately_single_attempt() {
    assert!(std::mem::size_of::<MaintenanceCoordinator>() <= 64);
    let primary = Arc::new(MaintenanceCoordinator::default());
    let unrelated = Arc::new(MaintenanceCoordinator::default());

    let exclusive = primary.exclusive();
    let (acquired_tx, acquired_rx) = mpsc::sync_channel(0);
    let waiting = Arc::clone(&primary);
    let waiter = std::thread::spawn(move || {
        let _shared = waiting.shared();
        acquired_tx.send(()).unwrap();
    });
    assert!(acquired_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    let unrelated_shared = unrelated.shared();
    drop(unrelated_shared);
    drop(exclusive);
    acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();

    let poisoned = Arc::clone(&primary);
    assert!(std::thread::spawn(move || {
        let _exclusive = poisoned.exclusive();
        panic!("scripted maintenance panic");
    })
    .join()
    .is_err());
    drop(primary.shared());

    let first = primary.try_begin_online().unwrap();
    assert_ne!(first.id(), 0);
    assert!(primary.try_begin_online().is_err());
    assert!(unrelated.try_begin_online().is_ok());
    drop(first);
    assert!(primary.try_begin_online().is_ok());

    let key_value = DurableKeyValueStore::new_vec_based();
    let unrelated_key_value = DurableKeyValueStore::new_vec_based();
    let key_set = DurableKeySetStore::new_vec_based();
    let key_map = DurableKeyMapStore::new_vec_based();
    let key_value_attempt = key_value.maintenance_probe().try_begin_online().unwrap();
    assert!(key_value.maintenance_probe().try_begin_online().is_err());
    assert!(unrelated_key_value
        .maintenance_probe()
        .try_begin_online()
        .is_ok());
    assert!(key_set.maintenance_probe().try_begin_online().is_ok());
    assert!(key_map.maintenance_probe().try_begin_online().is_ok());
    drop(key_value_attempt);
    assert!(key_value.maintenance_probe().try_begin_online().is_ok());
}

#[test]
fn reads_bypass_maintenance_and_post_publication_callbacks_hold_no_store_guard() {
    let key_value = Arc::new(DurableKeyValueStore::new_vec_based());
    key_value.put(b"value".to_vec(), 7_u64.to_ne_bytes().to_vec());
    let key_set = Arc::new(DurableKeySetStore::new_vec_based());
    key_set.append(b"set".to_vec(), b"member".to_vec());
    let key_map = Arc::new(DurableKeyMapStore::new_vec_based());
    key_map.put(b"map".to_vec(), SearchKey::from(1), b"element".to_vec());

    let value_exclusive = key_value.maintenance_probe().exclusive();
    let set_exclusive = key_set.maintenance_probe().exclusive();
    let map_exclusive = key_map.maintenance_probe().exclusive();
    let (read_tx, read_rx) = mpsc::channel();
    let value_reader = Arc::clone(&key_value);
    let value_tx = read_tx.clone();
    let value_read = std::thread::spawn(move || {
        assert_eq!(
            value_reader.get(b"value"),
            Some(7_u64.to_ne_bytes().to_vec())
        );
        assert_eq!(value_reader.read_number(b"value"), Some(Ok(7)));
        assert!(value_reader.contains(b"value"));
        assert_eq!(value_reader.size(), 1);
        value_tx.send(()).unwrap();
    });
    let set_reader = Arc::clone(&key_set);
    let set_tx = read_tx.clone();
    let set_read = std::thread::spawn(move || {
        assert!(set_reader.contains_key(b"set"));
        assert!(set_reader.contains_in_set(b"set", b"member"));
        assert!(set_reader.get_hashset(b"set").is_some());
        assert_eq!(set_reader.size(), 1);
        set_tx.send(()).unwrap();
    });
    let map_reader = Arc::clone(&key_map);
    let map_read = std::thread::spawn(move || {
        assert!(map_reader.contains_key(b"map"));
        assert!(map_reader.contains_search_key(b"map", &SearchKey::from(1)));
        assert!(map_reader.contains_in_map(b"map", &SearchKey::from(1)));
        assert_eq!(
            map_reader.get_element(b"map", &SearchKey::from(1)),
            Some(b"element".to_vec())
        );
        assert!(map_reader.get_sorted_map(b"map").is_some());
        assert_eq!(map_reader.first(b"map").unwrap().0, SearchKey::from(1));
        assert_eq!(map_reader.last(b"map").unwrap().0, SearchKey::from(1));
        assert_eq!(map_reader.sorted_map_size(b"map"), Some(1));
        assert_eq!(map_reader.size(), 1);
        read_tx.send(()).unwrap();
    });
    for _ in 0..3 {
        read_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    value_read.join().unwrap();
    set_read.join().unwrap();
    map_read.join().unwrap();
    drop(map_exclusive);
    drop(set_exclusive);
    drop(value_exclusive);

    assert_callback_releases_set_guards();
    assert_callback_releases_map_guards();
}

fn assert_callback_releases_set_guards() {
    let store = Arc::new(DurableKeySetStore::new_vec_based());
    store.append(b"callback-set".to_vec(), b"member".to_vec());
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let callback_store = Arc::clone(&store);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        worker_store.remove_from_set_callback(
            b"callback-set".to_vec(),
            b"member".to_vec(),
            move |_| {
                assert!(!callback_store.contains_key(b"callback-set"));
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            },
        );
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (exclusive_tx, exclusive_rx) = mpsc::sync_channel(0);
    let exclusive_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let _exclusive = exclusive_store.maintenance_probe().exclusive();
        exclusive_tx.send(()).unwrap();
    });
    let acquired_while_callback_paused = exclusive_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    if acquired_while_callback_paused.is_err() {
        exclusive_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    waiter.join().unwrap();
    assert!(acquired_while_callback_paused.is_ok());
}

fn assert_callback_releases_map_guards() {
    let store = Arc::new(DurableKeyMapStore::new_vec_based());
    store.put(
        b"callback-map".to_vec(),
        SearchKey::from(1),
        b"element".to_vec(),
    );
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let callback_store = Arc::clone(&store);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        worker_store.remove_from_sorted_map_callback(
            b"callback-map".to_vec(),
            SearchKey::from(1),
            move |_| {
                assert!(!callback_store.contains_key(b"callback-map"));
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            },
        );
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (exclusive_tx, exclusive_rx) = mpsc::sync_channel(0);
    let exclusive_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let _exclusive = exclusive_store.maintenance_probe().exclusive();
        exclusive_tx.send(()).unwrap();
    });
    let acquired_while_callback_paused = exclusive_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    if acquired_while_callback_paused.is_err() {
        exclusive_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    waiter.join().unwrap();
    assert!(acquired_while_callback_paused.is_ok());
}

#[test]
fn async_conflict_cancellation_and_callback_panic_leave_no_delta_or_coordination() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    let key = b"conflict".to_vec();
    store.append(key.clone(), b"seed".to_vec());
    let attempt = store.begin_online_probe(u64::MAX).unwrap();
    assert_ne!(attempt.token(), 0);
    assert!(store.begin_online_probe(u64::MAX).is_err());

    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let conflict_store = Arc::clone(&store);
    let conflict_key = key.clone();
    let conflict = std::thread::spawn(move || {
        block_on_online(
            conflict_store.try_compute_async(conflict_key, async move |working| {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                working.insert(b"stale-candidate".to_vec());
            }),
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    store.append(key.clone(), b"concurrent".to_vec());
    release_tx.send(()).unwrap();
    let conflict_error = conflict.join().unwrap().unwrap_err();
    assert_eq!(conflict_error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(store.delta_group_count_probe(), 1);

    store.try_compute(key.clone(), |_| {}).unwrap();
    assert_eq!(store.delta_group_count_probe(), 1);

    let callback_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store
            .try_compute(b"panic".to_vec(), |_| {
                panic!("scripted sync callback panic")
            })
            .unwrap();
    }));
    assert!(callback_panic.is_err());
    assert_eq!(store.delta_group_count_probe(), 1);
    drop(store.maintenance_probe().exclusive());

    {
        let future = store.try_compute_async(b"cancelled".to_vec(), async |working| {
            working.insert(b"discarded".to_vec());
            std::future::pending::<()>().await;
        });
        let mut future = pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }
    assert_eq!(store.delta_group_count_probe(), 1);
    drop(store.maintenance_probe().exclusive());

    let async_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_online(store.try_compute_async(b"async-panic".to_vec(), async |_| {
            panic!("scripted async callback panic");
        }))
    }));
    assert!(async_panic.is_err());
    assert_eq!(store.delta_group_count_probe(), 1);
    drop(store.maintenance_probe().exclusive());

    drop(attempt);
    assert!(!store.has_delta_recorder_probe());
    let next = store.begin_online_probe(u64::MAX).unwrap();
    drop(next);
    assert!(!store.has_delta_recorder_probe());
}

fn block_on_online<F: Future>(future: F) -> F::Output {
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
