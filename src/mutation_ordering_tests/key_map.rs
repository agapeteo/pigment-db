//! Deterministic key/sorted-map mutation-ordering tests.

use super::DurableKeyMapStore;
use crate::model::SearchKey;
use crate::test_support::cross_shard;
use crate::test_support::fault_writer::{
    rollback_blocking, rollback_scripted, BlockingWriter, ScriptedWriter, WriterFault,
};
use crate::test_support::mutation_schedule::{
    run_checkpoint_child, PROCESS_CHECKPOINT_ENV, PROCESS_CHILD_MODE_ENV, PROCESS_STORE_DIR_ENV,
};
use crate::test_support::mutation_schedule::{MutationObserver, MutationPhase, WATCHDOG};
use crate::test_support::shard_keys::select_shard_keys;
use crate::wal::WalStorage;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;

/// CMO-CROSS-1/3/4: map mutations retain only their selected data-map shard.
#[test]
fn different_shard_progress_and_same_shard_waiting() {
    for phase in [
        MutationPhase::AcceptanceEntered,
        MutationPhase::AcceptedBeforePublication,
    ] {
        let directory = tempfile::tempdir().expect("create map cross-shard directory");
        let mut store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize map cross-shard store")
            .into_store();
        let keys = select_shard_keys(&store.store);
        let (observer, gate) = MutationObserver::one_shot(keys.anchor.clone(), phase);
        store.mutation_observer = observer;
        let store = Arc::new(store);

        let anchor_store = Arc::clone(&store);
        let anchor_key = keys.anchor.clone();
        let anchor = std::thread::spawn(move || {
            anchor_store.put(anchor_key, SearchKey::from(1), b"anchor".to_vec());
        });
        gate.wait_until_reached();

        let (different_tx, different_rx) = mpsc::sync_channel(0);
        let different_store = Arc::clone(&store);
        let different_key = keys.different_shard.clone();
        let different = std::thread::spawn(move || {
            different_store.put(different_key, SearchKey::from(1), b"different".to_vec());
            different_tx.send(()).unwrap();
        });
        assert!(cross_shard::completes_within(
            &different_rx,
            std::time::Duration::from_millis(500)
        ));

        let (same_tx, same_rx) = mpsc::sync_channel(0);
        let same_store = Arc::clone(&store);
        let same_key = keys.same_shard.clone();
        let same = std::thread::spawn(move || {
            same_store.put(same_key, SearchKey::from(1), b"same".to_vec());
            same_tx.send(()).unwrap();
        });
        assert!(!cross_shard::completes_within(
            &same_rx,
            std::time::Duration::from_millis(250)
        ));

        gate.release();
        anchor.join().unwrap();
        different.join().unwrap();
        same_rx.recv_timeout(WATCHDOG).unwrap();
        same.join().unwrap();
        assert_eq!(
            store.get_element(&keys.anchor, &SearchKey::from(1)),
            Some(b"anchor".to_vec())
        );
        assert_eq!(
            store.get_element(&keys.different_shard, &SearchKey::from(1)),
            Some(b"different".to_vec())
        );
        assert_eq!(
            store.get_element(&keys.same_shard, &SearchKey::from(1)),
            Some(b"same".to_vec())
        );
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeyMapStore::try_init_new(directory.path())
                .expect("reopen map cross-shard store")
                .into_store();
            assert_eq!(
                reopened.get_element(&keys.anchor, &SearchKey::from(1)),
                Some(b"anchor".to_vec())
            );
            assert_eq!(
                reopened.get_element(&keys.different_shard, &SearchKey::from(1)),
                Some(b"different".to_vec())
            );
            assert_eq!(
                reopened.get_element(&keys.same_shard, &SearchKey::from(1)),
                Some(b"same".to_vec())
            );
            drop(reopened);
        }
    }
}

#[test]
fn grouped_compute_preserves_shard_progress_contract() {
    for phase in [
        MutationPhase::AcceptanceEntered,
        MutationPhase::AcceptedBeforePublication,
    ] {
        let mut store = DurableKeyMapStore::new_vec_based();
        let keys = select_shard_keys(&store.store);
        store.put(keys.anchor.clone(), SearchKey::from(1), b"old".to_vec());
        let (observer, gate) = MutationObserver::one_shot(keys.anchor.clone(), phase);
        store.mutation_observer = observer;
        let store = Arc::new(store);

        let grouped_store = Arc::clone(&store);
        let grouped_key = keys.anchor.clone();
        let grouped = std::thread::spawn(move || {
            grouped_store.compute(grouped_key, |map| {
                map.remove(&SearchKey::from(1));
                map.insert(SearchKey::from(2), b"new-a".to_vec());
                map.insert(SearchKey::from(3), b"new-b".to_vec());
            });
        });
        gate.wait_until_reached();

        let (different_tx, different_rx) = mpsc::sync_channel(0);
        let different_store = Arc::clone(&store);
        let different_key = keys.different_shard.clone();
        let different = std::thread::spawn(move || {
            different_store.put(different_key, SearchKey::from(1), b"different".to_vec());
            different_tx.send(()).unwrap();
        });
        assert!(cross_shard::completes_within(
            &different_rx,
            std::time::Duration::from_millis(500)
        ));

        let (same_tx, same_rx) = mpsc::sync_channel(0);
        let same_store = Arc::clone(&store);
        let same_key = keys.same_shard.clone();
        let same = std::thread::spawn(move || {
            same_store.put(same_key, SearchKey::from(1), b"same".to_vec());
            same_tx.send(()).unwrap();
        });
        assert!(!cross_shard::completes_within(
            &same_rx,
            std::time::Duration::from_millis(250)
        ));

        gate.release();
        grouped.join().unwrap();
        different.join().unwrap();
        same_rx.recv_timeout(WATCHDOG).unwrap();
        same.join().unwrap();
        assert_eq!(store.get_element(&keys.anchor, &SearchKey::from(1)), None);
        assert_eq!(
            store.get_element(&keys.anchor, &SearchKey::from(2)),
            Some(b"new-a".to_vec())
        );
        assert_eq!(
            store.get_element(&keys.anchor, &SearchKey::from(3)),
            Some(b"new-b".to_vec())
        );
    }
}

/// CMO-CROSS-2: a map mutation on another shard waits only at the WAL.
#[test]
fn different_shard_prepares_but_waits_for_busy_wal() {
    let (writer, writer_gate) = BlockingWriter::new(1);
    let mut store = DurableKeyMapStore {
        store: DashMap::new(),
        wal: WalStorage::new_with_rollback(writer, rollback_blocking),
        file_backing: None,
        _open_lease: None,
        mutation_observer: MutationObserver::default(),
    };
    let keys = select_shard_keys(&store.store);
    let (observer, preparation_gate) = MutationObserver::one_shot(
        keys.different_shard.clone(),
        MutationPhase::AcceptanceEntered,
    );
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let anchor_store = Arc::clone(&store);
    let anchor_key = keys.anchor.clone();
    let anchor = std::thread::spawn(move || {
        anchor_store.put(anchor_key, SearchKey::from(1), b"anchor".to_vec());
    });
    writer_gate.wait_until_blocked();

    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let different_store = Arc::clone(&store);
    let different_key = keys.different_shard.clone();
    let different = std::thread::spawn(move || {
        different_store.put(different_key, SearchKey::from(1), b"different".to_vec());
        completed_tx.send(()).unwrap();
    });
    preparation_gate.wait_until_reached();
    preparation_gate.release();
    assert!(!cross_shard::completes_within(
        &completed_rx,
        std::time::Duration::from_millis(250)
    ));

    writer_gate.release();
    anchor.join().unwrap();
    completed_rx.recv_timeout(WATCHDOG).unwrap();
    different.join().unwrap();
}

#[test]
#[ignore = "release-only 1,000-schedule different-shard conformance"]
fn conformance_different_shard_1k() {
    for schedule in 0..1_000 {
        let directory = tempfile::tempdir().expect("create map conformance directory");
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize map conformance store")
            .into_store();
        let keys = select_shard_keys(&store.store);
        cross_shard::run_concurrently(
            || {
                store.put(
                    keys.anchor.clone(),
                    SearchKey::from(schedule),
                    b"anchor".to_vec(),
                )
            },
            || {
                store.put(
                    keys.different_shard.clone(),
                    SearchKey::from(schedule + 1),
                    b"different".to_vec(),
                )
            },
        );
        let anchor = store.get_sorted_map(&keys.anchor);
        let different = store.get_sorted_map(&keys.different_shard);
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeyMapStore::try_init_new(directory.path())
                .expect("reopen map conformance store")
                .into_store();
            assert_eq!(reopened.get_sorted_map(&keys.anchor), anchor);
            assert_eq!(reopened.get_sorted_map(&keys.different_shard), different);
            drop(reopened);
        }
    }
}

#[test]
#[ignore = "release-only 10,000-history same-key conformance"]
fn conformance_same_key_10k() {
    for history in 0_usize..10_000 {
        let directory = tempfile::tempdir().expect("create map same-key directory");
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .expect("initialize map same-key store")
            .into_store();
        let key = b"same-key".to_vec();
        let search_key = SearchKey::from(1);
        cross_shard::run_concurrently(
            || {
                store.put(
                    key.clone(),
                    search_key.clone(),
                    history.to_ne_bytes().to_vec(),
                )
            },
            || {
                store.put(
                    key.clone(),
                    search_key.clone(),
                    (history + 1).to_ne_bytes().to_vec(),
                )
            },
        );
        let expected = store.get_sorted_map(&key);
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeyMapStore::try_init_new(directory.path())
                .expect("reopen map same-key store")
                .into_store();
            assert_eq!(reopened.get_sorted_map(&key), expected);
            drop(reopened);
        }
    }
}

/// CMO-FAIL-4: rejected map puts, final removals, and pops publish nothing.
#[test]
fn rejected_map_mutations_preserve_state_and_allow_progress() {
    for operation in 0..4 {
        let (writer, handle) = ScriptedWriter::new(WriterFault::WriteCall(8), false);
        let store = DurableKeyMapStore {
            store: DashMap::new(),
            wal: WalStorage::new_with_rollback(writer, rollback_scripted),
            file_backing: None,
            _open_lease: None,
            mutation_observer: MutationObserver::default(),
        };
        let key = b"key".to_vec();
        let target = SearchKey::from(1);
        store.put(key.clone(), target.clone(), b"before".to_vec());
        let checkpoint = handle.bytes();

        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match operation {
            0 => store.put(key.clone(), target.clone(), b"rejected".to_vec()),
            1 => {
                store.remove_from_sorted_map(key.clone(), target.clone());
            }
            2 => {
                let _ = store.pop_first(key.clone());
            }
            3 => {
                let _ = store.pop_last(key.clone());
            }
            _ => unreachable!(),
        }));
        assert!(rejected.is_err());
        assert_eq!(store.get_element(&key, &target), Some(b"before".to_vec()));
        assert_eq!(handle.bytes(), checkpoint);
        assert_eq!(
            crate::wal::read_for_map(&handle.bytes())
                .get(&key)
                .and_then(|map| map.get(&target)),
            Some(&b"before".to_vec())
        );

        let later = SearchKey::from(2);
        store.put(key.clone(), later.clone(), b"later".to_vec());
        assert_eq!(store.get_element(&key, &later), Some(b"later".to_vec()));
        assert_eq!(
            crate::wal::read_for_map(&handle.bytes())
                .get(&key)
                .and_then(|map| map.get(&later)),
            Some(&b"later".to_vec())
        );
    }
}

/// CMO-CALL-3: a panicking map compute publishes nothing and releases its shard.
#[test]
fn compute_panic_preserves_state_and_allows_progress() {
    let store = DurableKeyMapStore::new_vec_based();
    let key = b"panic".to_vec();
    let before = SearchKey::from(1);
    store.put(key.clone(), before.clone(), b"before".to_vec());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.compute(key.clone(), |map| {
            map.insert(SearchKey::from(2), b"transient".to_vec());
            panic!("scripted map callback panic");
        });
    }));
    assert!(panic.is_err());
    assert_eq!(store.sorted_map_size(&key), Some(1));
    assert_eq!(store.get_element(&key, &before), Some(b"before".to_vec()));
    store.put(key.clone(), SearchKey::from(3), b"later".to_vec());
    assert_eq!(store.sorted_map_size(&key), Some(2));
}

/// CMO-READ-2: a read cannot pass an accepted-but-unpublished same-key map put.
#[test]
fn read_at_accepted_boundary_returns_complete_published_map() {
    let mut store = DurableKeyMapStore::new_vec_based();
    let key = b"read-boundary".to_vec();
    let search_key = SearchKey::from(1);
    store.put(key.clone(), search_key.clone(), b"before".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);
    let put_store = Arc::clone(&store);
    let put_key = key.clone();
    let put_search_key = search_key.clone();
    let put = std::thread::spawn(move || {
        put_store.put(put_key, put_search_key, b"after".to_vec());
    });
    gate.wait_until_reached();

    let (read_tx, read_rx) = mpsc::sync_channel(0);
    let read_store = Arc::clone(&store);
    let read_key = key.clone();
    let read =
        std::thread::spawn(move || read_tx.send(read_store.get_sorted_map(&read_key)).unwrap());
    assert!(matches!(
        read_rx.recv_timeout(std::time::Duration::from_millis(250)),
        Err(RecvTimeoutError::Timeout)
    ));
    gate.release();
    put.join().unwrap();
    let observed = read_rx.recv_timeout(WATCHDOG).unwrap().unwrap();
    assert_eq!(observed.get(&search_key), Some(&b"after".to_vec()));
    read.join().unwrap();
}

#[test]
fn process_prefix_child() {
    let Some(mode) = std::env::var_os(PROCESS_CHILD_MODE_ENV) else {
        return;
    };
    let directory = std::path::PathBuf::from(
        std::env::var_os(PROCESS_STORE_DIR_ENV).expect("checkpoint child store directory"),
    );
    let checkpoint = std::env::var_os(PROCESS_CHECKPOINT_ENV);
    std::env::remove_var(PROCESS_CHECKPOINT_ENV);
    let mut store = DurableKeyMapStore::try_init_new(&directory)
        .expect("initialize map checkpoint child")
        .into_store();
    let key = b"prefix".to_vec();
    let search_key = SearchKey::from(1);
    store.put(key.clone(), search_key.clone(), b"before".to_vec());
    store.wal.sync_all().unwrap();

    if mode == "blocked-contender" {
        let (observer, gate) =
            MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
        store.mutation_observer = observer;
        let store = Arc::new(store);
        let first_store = Arc::clone(&store);
        let first_key = key.clone();
        let first_search_key = search_key.clone();
        let _first = std::thread::spawn(move || {
            first_store.put(first_key, first_search_key, b"first".to_vec());
        });
        gate.wait_until_reached();
        store.wal.sync_all().unwrap();
        let contender_store = Arc::clone(&store);
        let contender_key = key.clone();
        let contender_search_key = search_key.clone();
        let _contender = std::thread::spawn(move || {
            contender_store.put(contender_key, contender_search_key, b"contender".to_vec());
        });
        std::process::exit(crate::test_support::mutation_schedule::PROCESS_CHECKPOINT_EXIT_CODE);
    }

    std::env::set_var(
        PROCESS_CHECKPOINT_ENV,
        checkpoint.expect("checkpoint child phase"),
    );
    store.compute(key, |map| {
        map.clear();
        map.insert(SearchKey::from(2), b"new-a".to_vec());
        map.insert(SearchKey::from(3), b"new-b".to_vec());
    });
    unreachable!("checkpoint child must exit from observer notification");
}

#[test]
fn process_prefixes_reopen_one_accepted_map_history() {
    for (mode, checkpoint, expected) in [
        ("before", Some("acceptance-entered"), 0_u8),
        ("accepted", Some("accepted-before-publication"), 1_u8),
        ("published", Some("published"), 1_u8),
        ("blocked-contender", None, 2_u8),
    ] {
        let directory = tempfile::tempdir().expect("create map checkpoint directory");
        run_checkpoint_child(
            "key_map_store::mutation_ordering_tests::process_prefix_child",
            directory.path(),
            mode,
            checkpoint,
        );
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen map checkpoint store")
            .into_store();
        let map = reopened.get_sorted_map(b"prefix").unwrap();
        match expected {
            0 => assert_eq!(map.get(&SearchKey::from(1)), Some(&b"before".to_vec())),
            1 => {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get(&SearchKey::from(2)), Some(&b"new-a".to_vec()));
                assert_eq!(map.get(&SearchKey::from(3)), Some(&b"new-b".to_vec()));
            }
            2 => assert_eq!(map.get(&SearchKey::from(1)), Some(&b"first".to_vec())),
            _ => unreachable!(),
        }
    }
}

/// CMO-ORDER-2: replacement puts choose one live and reopened order.
#[test]
fn replacement_puts_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create key/map ordering directory");
    let key = b"same-map".to_vec();
    let search_key = SearchKey::from(1);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize key/map ordering store")
        .into_store();
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first_search_key = search_key.clone();
    let first = std::thread::spawn(move || {
        first_store.put(first_key, first_search_key, b"first".to_vec());
    });
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second_search_key = search_key.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal replacement put start");
        second_store.put(second_key, second_search_key, b"second".to_vec());
        completed_tx
            .send(())
            .expect("signal replacement put completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("replacement put thread must start");
    let second_completed_while_first_was_parked =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("replacement put disconnected"),
        };

    gate.release();
    first.join().expect("first replacement put must complete");
    if !second_completed_while_first_was_parked {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("second replacement put must complete after publication");
    }
    second.join().expect("second replacement put must join");

    let live = store.get_sorted_map(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen key/map ordering store")
            .into_store();
        assert_eq!(
            reopened.get_sorted_map(&key),
            live,
            "replacement puts selected different live and durable orders"
        );
        drop(reopened);
    }
}

/// CMO-ORDER-2: final-entry removal is one guarded ordering unit.
#[test]
fn final_entry_remove_blocks_same_key_compute_until_publication() {
    let directory = tempfile::tempdir().expect("create final-entry map directory");
    let key = b"final-map".to_vec();
    let target = SearchKey::from(1);
    let replacement = SearchKey::from(2);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize final-entry map store")
        .into_store();
    store.put(key.clone(), target.clone(), b"target".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let remove_store = Arc::clone(&store);
    let remove_key = key.clone();
    let remove_target = target.clone();
    let remove = std::thread::spawn(move || {
        remove_store.remove_from_sorted_map(remove_key, remove_target);
    });
    gate.wait_until_reached();

    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let compute_store = Arc::clone(&store);
    let compute_key = key.clone();
    let compute_replacement = replacement.clone();
    let compute = std::thread::spawn(move || {
        compute_store.compute(compute_key, |map| {
            map.insert(compute_replacement, b"replacement".to_vec());
        });
        completed_tx
            .send(())
            .expect("signal map compute completion");
    });
    let compute_completed_inside_removal =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("map compute disconnected"),
        };

    gate.release();
    remove.join().expect("final-entry removal must join");
    if !compute_completed_inside_removal {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("map compute must finish after removal");
    }
    compute.join().expect("map compute must join");

    let live = store.get_sorted_map(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen final-entry map")
            .into_store();
        assert_eq!(reopened.get_sorted_map(&key), live);
        drop(reopened);
    }
    assert!(
        !compute_completed_inside_removal,
        "same-key compute completed inside final-entry removal"
    );
}

/// CMO-CALL-3: final-removal callbacks run once after shard release.
#[test]
fn callback_removal_is_guarded_and_callback_runs_after_release() {
    let directory = tempfile::tempdir().expect("create map callback-removal directory");
    let key = b"callback-map".to_vec();
    let target = SearchKey::from(1);
    let replacement = SearchKey::from(2);
    let callback_search_key = SearchKey::from(3);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize map callback-removal store")
        .into_store();
    store.put(key.clone(), target.clone(), b"target".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);
    let callback_count = Arc::new(AtomicUsize::new(0));

    let remove_store = Arc::clone(&store);
    let callback_store = Arc::clone(&store);
    let remove_key = key.clone();
    let callback_key = key.clone();
    let remove_target = target.clone();
    let callback_entry = callback_search_key.clone();
    let remove_count = Arc::clone(&callback_count);
    let remove = std::thread::spawn(move || {
        remove_store.remove_from_sorted_map_callback(remove_key, remove_target, move |_| {
            remove_count.fetch_add(1, Ordering::SeqCst);
            callback_store.put(callback_key, callback_entry, b"from-callback".to_vec());
        });
    });
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let compute_store = Arc::clone(&store);
    let compute_key = key.clone();
    let compute_replacement = replacement.clone();
    let compute = std::thread::spawn(move || {
        started_tx
            .send(())
            .expect("signal map callback overlap start");
        compute_store.compute(compute_key, |map| {
            map.insert(compute_replacement, b"replacement".to_vec());
        });
        completed_tx
            .send(())
            .expect("signal map callback overlap completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("map callback overlap thread must start");
    let compute_completed_inside_removal =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("map callback overlap disconnected"),
        };

    gate.release();
    remove.join().expect("map callback removal must complete");
    if !compute_completed_inside_removal {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("map overlap compute must complete after callback removal");
    }
    compute.join().expect("map overlap compute must join");

    assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    let live = store.get_sorted_map(&key);
    assert!(live.as_ref().is_some_and(|map| {
        map.contains_key(&replacement) && map.contains_key(&callback_search_key)
    }));
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen map callback-removal store")
            .into_store();
        assert_eq!(reopened.get_sorted_map(&key), live);
        drop(reopened);
    }
    assert!(
        !compute_completed_inside_removal,
        "same-key compute completed inside map callback removal"
    );
}

/// CMO-ORDER-2: outer-key deletion and recreation share the map shard order.
#[test]
fn remove_key_and_put_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create map recreation directory");
    let key = b"recreated-map".to_vec();
    let original = SearchKey::from(1);
    let recreated = SearchKey::from(2);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize map recreation store")
        .into_store();
    store.put(key.clone(), original, b"original".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let remove_store = Arc::clone(&store);
    let remove_key = key.clone();
    let remove = std::thread::spawn(move || remove_store.remove_key(&remove_key));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let put_store = Arc::clone(&store);
    let put_key = key.clone();
    let put_recreated = recreated.clone();
    let put = std::thread::spawn(move || {
        started_tx.send(()).expect("signal map recreation start");
        put_store.put(put_key, put_recreated, b"recreated".to_vec());
        completed_tx
            .send(())
            .expect("signal map recreation completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("map recreation thread must start");
    let put_completed_inside_delete =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("map recreation disconnected"),
        };

    gate.release();
    remove.join().expect("map delete must complete");
    if !put_completed_inside_delete {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("map recreation must complete after delete");
    }
    put.join().expect("map recreation must join");

    let live = store.get_sorted_map(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen recreated map")
            .into_store();
        assert_eq!(
            reopened.get_sorted_map(&key),
            live,
            "map deletion and recreation selected different orders"
        );
        drop(reopened);
    }
}

/// CMO-ORDER-1/2: pop-first acceptance precedes publication and a later put.
#[test]
fn pop_first_accepts_before_publication_and_later_put() {
    let directory = tempfile::tempdir().expect("create pop-first ordering directory");
    let key = b"pop-first-map".to_vec();
    let first = SearchKey::from(1);
    let retained = SearchKey::from(2);
    let later = SearchKey::from(3);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize pop-first ordering store")
        .into_store();
    store.put(key.clone(), first.clone(), b"first".to_vec());
    store.put(key.clone(), retained.clone(), b"retained".to_vec());
    let (observer, phases) = MutationObserver::recording(key.clone());
    store.mutation_observer = observer;

    let _ = store.pop_first(key.clone());
    store.put(key.clone(), later.clone(), b"later".to_vec());

    let observed = phases.lock().unwrap().clone();
    assert_eq!(
        &observed[..3],
        &[
            MutationPhase::AcceptanceEntered,
            MutationPhase::AcceptedBeforePublication,
            MutationPhase::Published,
        ],
        "pop-first published before durable acceptance"
    );
    let live = store.get_sorted_map(&key);
    assert!(live.as_ref().is_some_and(|map| {
        !map.contains_key(&first) && map.contains_key(&retained) && map.contains_key(&later)
    }));
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen pop-first ordering store")
            .into_store();
        assert_eq!(reopened.get_sorted_map(&key), live);
        drop(reopened);
    }
}

/// CMO-ORDER-1/2: pop-last acceptance precedes publication and a later put.
#[test]
fn pop_last_accepts_before_publication_and_later_put() {
    let directory = tempfile::tempdir().expect("create pop-last ordering directory");
    let key = b"pop-last-map".to_vec();
    let retained = SearchKey::from(1);
    let last = SearchKey::from(2);
    let later = SearchKey::from(3);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize pop-last ordering store")
        .into_store();
    store.put(key.clone(), retained.clone(), b"retained".to_vec());
    store.put(key.clone(), last.clone(), b"last".to_vec());
    let (observer, phases) = MutationObserver::recording(key.clone());
    store.mutation_observer = observer;

    let _ = store.pop_last(key.clone());
    store.put(key.clone(), later.clone(), b"later".to_vec());

    let observed = phases.lock().unwrap().clone();
    assert_eq!(
        &observed[..3],
        &[
            MutationPhase::AcceptanceEntered,
            MutationPhase::AcceptedBeforePublication,
            MutationPhase::Published,
        ],
        "pop-last published before durable acceptance"
    );
    let live = store.get_sorted_map(&key);
    assert!(live.as_ref().is_some_and(|map| {
        map.contains_key(&retained) && !map.contains_key(&last) && map.contains_key(&later)
    }));
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen pop-last ordering store")
            .into_store();
        assert_eq!(reopened.get_sorted_map(&key), live);
        drop(reopened);
    }
}

/// CMO-ORDER-3: a multi-action map compute cannot be split by a put.
#[test]
fn multi_action_batch_is_indivisible() {
    let directory = tempfile::tempdir().expect("create multi-action map directory");
    let key = b"batch-map".to_vec();
    let old_a = SearchKey::from(1);
    let old_b = SearchKey::from(2);
    let new_a = SearchKey::from(3);
    let new_b = SearchKey::from(4);
    let ordinary = SearchKey::from(5);
    let mut store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize multi-action map store")
        .into_store();
    store.put(key.clone(), old_a.clone(), b"old-a".to_vec());
    store.put(key.clone(), old_b.clone(), b"old-b".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let compute_store = Arc::clone(&store);
    let compute_key = key.clone();
    let compute_new_a = new_a.clone();
    let compute_new_b = new_b.clone();
    let compute = std::thread::spawn(move || {
        compute_store.compute(compute_key, |map| {
            map.clear();
            map.insert(compute_new_a, b"new-a".to_vec());
            map.insert(compute_new_b, b"new-b".to_vec());
        });
    });
    gate.wait_until_reached();

    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let put_store = Arc::clone(&store);
    let put_key = key.clone();
    let put_ordinary = ordinary.clone();
    let put = std::thread::spawn(move || {
        put_store.put(put_key, put_ordinary, b"ordinary".to_vec());
        completed_tx
            .send(())
            .expect("signal map batch overlap completion");
    });
    let put_completed_inside_batch =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("map batch overlap disconnected"),
        };
    gate.release();
    compute.join().expect("multi-action map compute must join");
    if !put_completed_inside_batch {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("map put must complete after batch publication");
    }
    put.join().expect("map batch overlap put must join");

    assert!(!put_completed_inside_batch);
    let live = store.get_sorted_map(&key).expect("batch map must exist");
    assert_eq!(live.len(), 3);
    assert_eq!(live.get(&new_a), Some(&b"new-a".to_vec()));
    assert_eq!(live.get(&new_b), Some(&b"new-b".to_vec()));
    assert_eq!(live.get(&ordinary), Some(&b"ordinary".to_vec()));
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen multi-action map store")
            .into_store();
        assert_eq!(reopened.get_sorted_map(&key), Some(live.clone()));
        drop(reopened);
    }
}

#[test]
fn ordinary_conditional_noop_and_ordered_append_matrix() {
    let directory = tempfile::tempdir().expect("create map family matrix directory");
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize map family matrix store")
        .into_store();
    let put_then_compute = b"put-then-compute".to_vec();
    let compute_then_put = b"compute-then-put".to_vec();
    let compute_twice = b"compute-twice".to_vec();
    let ordered = b"ordered".to_vec();
    let search_key = SearchKey::from(1);

    store.put(
        put_then_compute.clone(),
        search_key.clone(),
        b"put".to_vec(),
    );
    store.compute(put_then_compute.clone(), |map| {
        map.insert(search_key.clone(), b"compute".to_vec());
    });
    store.compute(compute_then_put.clone(), |map| {
        map.insert(search_key.clone(), b"compute".to_vec());
    });
    store.put(
        compute_then_put.clone(),
        search_key.clone(),
        b"put".to_vec(),
    );
    store.compute(compute_twice.clone(), |map| {
        map.insert(search_key.clone(), b"first".to_vec());
    });
    store.compute(compute_twice.clone(), |map| {
        map.insert(search_key.clone(), b"second".to_vec());
    });

    let conditional_calls = AtomicUsize::new(0);
    store.compute_if_present(b"absent".to_vec(), |_| {
        conditional_calls.fetch_add(1, Ordering::SeqCst);
    });
    store.compute_if_absent(put_then_compute.clone(), |_| {
        conditional_calls.fetch_add(1, Ordering::SeqCst);
    });
    let before_noop = store.get_sorted_map(&compute_twice);
    store.compute(compute_twice.clone(), |_| {});
    store.append_ordered_element(ordered.clone(), b"zero".to_vec());
    store.append_ordered_element(ordered.clone(), b"one".to_vec());

    assert_eq!(conditional_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.get_sorted_map(&compute_twice), before_noop);
    assert_eq!(
        store.get_element(&put_then_compute, &search_key),
        Some(b"compute".to_vec())
    );
    assert_eq!(
        store.get_element(&compute_then_put, &search_key),
        Some(b"put".to_vec())
    );
    assert_eq!(
        store.get_element(&compute_twice, &search_key),
        Some(b"second".to_vec())
    );
    assert_eq!(store.sorted_map_size(&ordered), Some(2));

    let snapshots = [
        (
            put_then_compute.clone(),
            store.get_sorted_map(&put_then_compute),
        ),
        (
            compute_then_put.clone(),
            store.get_sorted_map(&compute_then_put),
        ),
        (compute_twice.clone(), store.get_sorted_map(&compute_twice)),
        (ordered.clone(), store.get_sorted_map(&ordered)),
    ];
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyMapStore::try_init_new(directory.path())
            .expect("reopen map family matrix store")
            .into_store();
        for (key, expected) in &snapshots {
            assert_eq!(&reopened.get_sorted_map(key), expected);
        }
        drop(reopened);
    }
}
