//! Deterministic key/set mutation-ordering tests.

use super::DurableKeySetStore;
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
use std::collections::HashSet;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// CMO-CROSS-1/3/4: set mutations retain only their selected data-map shard.
#[test]
fn different_shard_progress_and_same_shard_waiting() {
    for phase in [
        MutationPhase::AcceptanceEntered,
        MutationPhase::AcceptedBeforePublication,
    ] {
        let directory = tempfile::tempdir().expect("create set cross-shard directory");
        let mut store = DurableKeySetStore::try_init_new(directory.path())
            .expect("initialize set cross-shard store")
            .into_store();
        let keys = select_shard_keys(&store.store);
        let (observer, gate) = MutationObserver::one_shot(keys.anchor.clone(), phase);
        store.mutation_observer = observer;
        let store = Arc::new(store);

        let anchor_store = Arc::clone(&store);
        let anchor_key = keys.anchor.clone();
        let anchor =
            std::thread::spawn(move || anchor_store.append(anchor_key, b"anchor".to_vec()));
        gate.wait_until_reached();

        let (different_tx, different_rx) = mpsc::sync_channel(0);
        let different_store = Arc::clone(&store);
        let different_key = keys.different_shard.clone();
        let different = std::thread::spawn(move || {
            different_store.append(different_key, b"different".to_vec());
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
            same_store.append(same_key, b"same".to_vec());
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
        assert!(store.contains_in_set(&keys.anchor, b"anchor"));
        assert!(store.contains_in_set(&keys.different_shard, b"different"));
        assert!(store.contains_in_set(&keys.same_shard, b"same"));
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeySetStore::try_init_new(directory.path())
                .expect("reopen set cross-shard store")
                .into_store();
            assert!(reopened.contains_in_set(&keys.anchor, b"anchor"));
            assert!(reopened.contains_in_set(&keys.different_shard, b"different"));
            assert!(reopened.contains_in_set(&keys.same_shard, b"same"));
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
        let mut store = DurableKeySetStore::new_vec_based();
        let keys = select_shard_keys(&store.store);
        store.append(keys.anchor.clone(), b"old".to_vec());
        let (observer, gate) = MutationObserver::one_shot(keys.anchor.clone(), phase);
        store.mutation_observer = observer;
        let store = Arc::new(store);

        let grouped_store = Arc::clone(&store);
        let grouped_key = keys.anchor.clone();
        let grouped = std::thread::spawn(move || {
            grouped_store.compute(grouped_key, |set| {
                set.remove(b"old".as_slice());
                set.insert(b"new-a".to_vec());
                set.insert(b"new-b".to_vec());
            });
        });
        gate.wait_until_reached();

        let (different_tx, different_rx) = mpsc::sync_channel(0);
        let different_store = Arc::clone(&store);
        let different_key = keys.different_shard.clone();
        let different = std::thread::spawn(move || {
            different_store.append(different_key, b"different".to_vec());
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
            same_store.append(same_key, b"same".to_vec());
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
        assert!(!store.contains_in_set(&keys.anchor, b"old"));
        assert!(store.contains_in_set(&keys.anchor, b"new-a"));
        assert!(store.contains_in_set(&keys.anchor, b"new-b"));
    }
}

/// CMO-CROSS-1/4: async callback preparation holds no DashMap shard lock.
#[test]
fn async_preparation_allows_same_and_different_shard_progress() {
    let store = DurableKeySetStore::new_vec_based();
    let keys = select_shard_keys(&store.store);
    store.append(keys.anchor.clone(), b"seed".to_vec());
    let store = Arc::new(store);
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let async_store = Arc::clone(&store);
    let async_key = keys.anchor.clone();
    let worker = std::thread::spawn(move || {
        block_on(async_store.compute_async(async_key, async move |set| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(WATCHDOG).unwrap();
            set.insert(b"async".to_vec());
        }));
    });
    entered_rx.recv_timeout(WATCHDOG).unwrap();

    let (different_tx, different_rx) = mpsc::sync_channel(0);
    let different_store = Arc::clone(&store);
    let different_key = keys.different_shard.clone();
    let different = std::thread::spawn(move || {
        different_store.append(different_key, b"different".to_vec());
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
        same_store.append(same_key, b"same".to_vec());
        same_tx.send(()).unwrap();
    });
    assert!(cross_shard::completes_within(
        &same_rx,
        std::time::Duration::from_millis(500)
    ));

    release_tx.send(()).unwrap();
    worker.join().unwrap();
    different.join().unwrap();
    same.join().unwrap();
}

/// CMO-CROSS-2: a set mutation on another shard waits only at the WAL.
#[test]
fn different_shard_prepares_but_waits_for_busy_wal() {
    let (writer, writer_gate) = BlockingWriter::new(1);
    let mut store = DurableKeySetStore {
        store: DashMap::new(),
        wal: WalStorage::new_with_rollback(writer, rollback_blocking),
        file_backing: None,
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
    let anchor = std::thread::spawn(move || anchor_store.append(anchor_key, b"anchor".to_vec()));
    writer_gate.wait_until_blocked();

    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let different_store = Arc::clone(&store);
    let different_key = keys.different_shard.clone();
    let different = std::thread::spawn(move || {
        different_store.append(different_key, b"different".to_vec());
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
    for schedule in 0_usize..1_000 {
        let directory = tempfile::tempdir().expect("create set conformance directory");
        let store = DurableKeySetStore::try_init_new(directory.path())
            .expect("initialize set conformance store")
            .into_store();
        let keys = select_shard_keys(&store.store);
        cross_shard::run_concurrently(
            || store.append(keys.anchor.clone(), schedule.to_ne_bytes().to_vec()),
            || {
                store.append(
                    keys.different_shard.clone(),
                    (schedule + 1).to_ne_bytes().to_vec(),
                )
            },
        );
        let anchor = store.get_hashset(&keys.anchor);
        let different = store.get_hashset(&keys.different_shard);
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeySetStore::try_init_new(directory.path())
                .expect("reopen set conformance store")
                .into_store();
            assert_eq!(reopened.get_hashset(&keys.anchor), anchor);
            assert_eq!(reopened.get_hashset(&keys.different_shard), different);
            drop(reopened);
        }
    }
}

#[test]
#[ignore = "release-only 10,000-history same-key conformance"]
fn conformance_same_key_10k() {
    for _history in 0..10_000 {
        let directory = tempfile::tempdir().expect("create set same-key directory");
        let store = DurableKeySetStore::try_init_new(directory.path())
            .expect("initialize set same-key store")
            .into_store();
        let key = b"same-key".to_vec();
        let member = b"member".to_vec();
        store.append(key.clone(), member.clone());
        cross_shard::run_concurrently(
            || store.append(key.clone(), member.clone()),
            || store.remove_from_set(key.clone(), member.clone()),
        );
        let expected = store.get_hashset(&key);
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeySetStore::try_init_new(directory.path())
                .expect("reopen set same-key store")
                .into_store();
            assert_eq!(reopened.get_hashset(&key), expected);
            drop(reopened);
        }
    }
}

/// CMO-FAIL-4: rejected final-member deletion leaves set state and WAL unchanged.
#[test]
fn rejected_final_member_removal_preserves_state_and_allows_progress() {
    for fault in [WriterFault::WriteCall(8), WriterFault::FlushCall(2)] {
        let (writer, handle) = ScriptedWriter::new(fault, false);
        let store = DurableKeySetStore {
            store: DashMap::new(),
            wal: WalStorage::new_with_rollback(writer, rollback_scripted),
            file_backing: None,
            mutation_observer: MutationObserver::default(),
        };
        let key = b"key".to_vec();
        let member = b"before".to_vec();
        store.append(key.clone(), member.clone());
        let checkpoint = handle.bytes();

        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.remove_from_set(key.clone(), member.clone());
        }));
        assert!(rejected.is_err());
        assert!(store.contains_in_set(&key, &member));
        assert_eq!(handle.bytes(), checkpoint);
        assert!(crate::wal::read_for_set(&handle.bytes())
            .get(&key)
            .is_some_and(|set| set.contains(&member)));

        store.append(key.clone(), b"later".to_vec());
        assert!(store.contains_in_set(&key, b"later"));
        assert!(crate::wal::read_for_set(&handle.bytes())
            .get(&key)
            .is_some_and(|set| set.contains(b"later".as_slice())));
    }
}

/// CMO-CALL-3: panic and async cancellation discard private set candidates.
#[test]
fn compute_panic_and_async_cancellation_preserve_state_and_progress() {
    let store = DurableKeySetStore::new_vec_based();
    let key = b"panic-cancel".to_vec();
    store.append(key.clone(), b"before".to_vec());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.compute(key.clone(), |set| {
            set.insert(b"transient-panic".to_vec());
            panic!("scripted set callback panic");
        });
    }));
    assert!(panic.is_err());
    assert!(!store.contains_in_set(&key, b"transient-panic"));

    {
        let future = store.compute_async(key.clone(), async |set| {
            set.insert(b"transient-cancel".to_vec());
            std::future::pending::<()>().await;
        });
        let mut future = pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }
    assert!(!store.contains_in_set(&key, b"transient-cancel"));
    assert!(store.contains_in_set(&key, b"before"));
    store.append(key.clone(), b"later".to_vec());
    assert!(store.contains_in_set(&key, b"later"));
}

/// CMO-READ-2: a read cannot pass an accepted-but-unpublished same-key append.
#[test]
fn read_at_accepted_boundary_returns_complete_published_set() {
    let mut store = DurableKeySetStore::new_vec_based();
    let key = b"read-boundary".to_vec();
    store.append(key.clone(), b"before".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);
    let append_store = Arc::clone(&store);
    let append_key = key.clone();
    let append = std::thread::spawn(move || append_store.append(append_key, b"after".to_vec()));
    gate.wait_until_reached();

    let (read_tx, read_rx) = mpsc::sync_channel(0);
    let read_store = Arc::clone(&store);
    let read_key = key.clone();
    let read = std::thread::spawn(move || read_tx.send(read_store.get_hashset(&read_key)).unwrap());
    assert!(matches!(
        read_rx.recv_timeout(std::time::Duration::from_millis(250)),
        Err(RecvTimeoutError::Timeout)
    ));
    gate.release();
    append.join().unwrap();
    let observed = read_rx.recv_timeout(WATCHDOG).unwrap().unwrap();
    assert!(observed.contains(b"before".as_slice()) && observed.contains(b"after".as_slice()));
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
    let mut store = DurableKeySetStore::try_init_new(&directory)
        .expect("initialize set checkpoint child")
        .into_store();
    let key = b"prefix".to_vec();
    store.append(key.clone(), b"before".to_vec());
    store.wal.sync_all().unwrap();

    if mode == "blocked-contender" {
        let (observer, gate) =
            MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
        store.mutation_observer = observer;
        let store = Arc::new(store);
        let first_store = Arc::clone(&store);
        let first_key = key.clone();
        let _first = std::thread::spawn(move || first_store.append(first_key, b"first".to_vec()));
        gate.wait_until_reached();
        store.wal.sync_all().unwrap();
        let contender_store = Arc::clone(&store);
        let contender_key = key.clone();
        let _contender = std::thread::spawn(move || {
            contender_store.append(contender_key, b"contender".to_vec())
        });
        std::process::exit(crate::test_support::mutation_schedule::PROCESS_CHECKPOINT_EXIT_CODE);
    }

    std::env::set_var(
        PROCESS_CHECKPOINT_ENV,
        checkpoint.expect("checkpoint child phase"),
    );
    store.compute(key, |set| {
        set.clear();
        set.insert(b"new-a".to_vec());
        set.insert(b"new-b".to_vec());
    });
    unreachable!("checkpoint child must exit from observer notification");
}

#[test]
fn process_prefixes_reopen_one_accepted_set_history() {
    for (mode, checkpoint, expected) in [
        ("before", Some("acceptance-entered"), 0_u8),
        ("accepted", Some("accepted-before-publication"), 1_u8),
        ("published", Some("published"), 1_u8),
        ("blocked-contender", None, 2_u8),
    ] {
        let directory = tempfile::tempdir().expect("create set checkpoint directory");
        run_checkpoint_child(
            "key_set_store::mutation_ordering_tests::process_prefix_child",
            directory.path(),
            mode,
            checkpoint,
        );
        let reopened = DurableKeySetStore::try_init_new(directory.path())
            .expect("reopen set checkpoint store")
            .into_store();
        let set = reopened.get_hashset(b"prefix").unwrap();
        match expected {
            0 => assert_eq!(set, HashSet::from([b"before".to_vec()])),
            1 => assert_eq!(set, HashSet::from([b"new-a".to_vec(), b"new-b".to_vec()])),
            2 => assert_eq!(set, HashSet::from([b"before".to_vec(), b"first".to_vec()])),
            _ => unreachable!(),
        }
    }
}

/// CMO-ORDER-2: append and removal of the same member choose one order.
#[test]
fn append_and_remove_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create key/set ordering directory");
    let key = b"same-set".to_vec();
    let member = b"member".to_vec();
    let sentinel = b"sentinel".to_vec();
    let mut store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize key/set ordering store")
        .into_store();
    store.append(key.clone(), member.clone());
    store.append(key.clone(), sentinel);
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first_member = member.clone();
    let first = std::thread::spawn(move || first_store.append(first_key, first_member));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second_member = member.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal set removal start");
        second_store.remove_from_set(second_key, second_member);
        completed_tx
            .send(())
            .expect("signal set removal completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("set removal thread must start");
    let remove_completed_while_append_was_parked =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("set removal thread disconnected"),
        };

    gate.release();
    first.join().expect("append must complete");
    if !remove_completed_while_append_was_parked {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("set removal must complete after append publication");
    }
    second.join().expect("set removal must join");

    let live = store.get_hashset(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory.path())
            .expect("reopen key/set ordering store")
            .into_store();
        assert_eq!(
            reopened.get_hashset(&key),
            live,
            "append and removal selected different live and durable orders"
        );
        drop(reopened);
    }
}

/// CMO-ORDER-2: a final-member removal is one guarded ordering unit.
#[test]
fn final_member_remove_blocks_same_key_compute_until_publication() {
    let directory = tempfile::tempdir().expect("create final-member ordering directory");
    let key = b"final-set".to_vec();
    let member = b"member".to_vec();
    let replacement = b"replacement".to_vec();
    let mut store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize final-member ordering store")
        .into_store();
    store.append(key.clone(), member.clone());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first_member = member.clone();
    let first = std::thread::spawn(move || first_store.remove_from_set(first_key, first_member));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second_replacement = replacement.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal set compute start");
        second_store.compute(second_key, |set| {
            set.insert(second_replacement);
        });
        completed_tx
            .send(())
            .expect("signal set compute completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("set compute thread must start");
    let compute_completed_inside_removal =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("set compute thread disconnected"),
        };

    gate.release();
    first.join().expect("final-member removal must complete");
    if !compute_completed_inside_removal {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("set compute must complete after final removal");
    }
    second.join().expect("set compute must join");

    let live = store.get_hashset(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory.path())
            .expect("reopen final-member ordering store")
            .into_store();
        assert_eq!(reopened.get_hashset(&key), live);
        drop(reopened);
    }
    assert!(
        !compute_completed_inside_removal,
        "same-key compute completed between removal acceptance and publication"
    );
}

/// CMO-CALL-3: final-removal callbacks run once after shard release.
#[test]
fn callback_removal_is_guarded_and_callback_runs_after_release() {
    let directory = tempfile::tempdir().expect("create callback-removal directory");
    let key = b"callback-set".to_vec();
    let member = b"member".to_vec();
    let replacement = b"replacement".to_vec();
    let callback_member = b"from-callback".to_vec();
    let mut store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize callback-removal store")
        .into_store();
    store.append(key.clone(), member.clone());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);
    let callback_count = Arc::new(AtomicUsize::new(0));

    let first_store = Arc::clone(&store);
    let callback_store = Arc::clone(&store);
    let first_key = key.clone();
    let callback_key = key.clone();
    let first_member = member.clone();
    let callback_value = callback_member.clone();
    let first_count = Arc::clone(&callback_count);
    let first = std::thread::spawn(move || {
        first_store.remove_from_set_callback(first_key, first_member, move |_| {
            first_count.fetch_add(1, Ordering::SeqCst);
            callback_store.append(callback_key, callback_value);
        });
    });
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second_replacement = replacement.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal callback overlap start");
        second_store.compute(second_key, |set| {
            set.insert(second_replacement);
        });
        completed_tx
            .send(())
            .expect("signal callback overlap completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("callback overlap thread must start");
    let compute_completed_inside_removal =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("callback overlap disconnected"),
        };

    gate.release();
    first.join().expect("callback removal must complete");
    if !compute_completed_inside_removal {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("overlap compute must complete after callback removal");
    }
    second.join().expect("overlap compute must join");

    assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    let live = store.get_hashset(&key);
    assert!(live
        .as_ref()
        .is_some_and(|set| { set.contains(&replacement) && set.contains(&callback_member) }));
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory.path())
            .expect("reopen callback-removal store")
            .into_store();
        assert_eq!(reopened.get_hashset(&key), live);
        drop(reopened);
    }
    assert!(
        !compute_completed_inside_removal,
        "same-key compute completed inside callback removal"
    );
}

/// CMO-ORDER-2: outer-key deletion and recreation share the set shard order.
#[test]
fn remove_key_and_append_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create set recreation directory");
    let key = b"recreated-set".to_vec();
    let mut store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize set recreation store")
        .into_store();
    store.append(key.clone(), b"original".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first = std::thread::spawn(move || first_store.remove_key(&first_key));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal set recreation start");
        second_store.append(second_key, b"recreated".to_vec());
        completed_tx
            .send(())
            .expect("signal set recreation completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("set recreation thread must start");
    let append_completed_while_delete_was_parked =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("set recreation disconnected"),
        };

    gate.release();
    first.join().expect("set delete must complete");
    if !append_completed_while_delete_was_parked {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("set recreation must complete after delete");
    }
    second.join().expect("set recreation must join");

    let live = store.get_hashset(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory.path())
            .expect("reopen recreated set")
            .into_store();
        assert_eq!(
            reopened.get_hashset(&key),
            live,
            "set deletion and recreation selected different orders"
        );
        drop(reopened);
    }
}

/// CMO-ORDER-3: a multi-action set compute cannot be split by append.
#[test]
fn multi_action_batch_is_indivisible() {
    let directory = tempfile::tempdir().expect("create multi-action set directory");
    let key = b"batch-set".to_vec();
    let mut store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize multi-action set store")
        .into_store();
    store.append(key.clone(), b"old-a".to_vec());
    store.append(key.clone(), b"old-b".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let compute_store = Arc::clone(&store);
    let compute_key = key.clone();
    let compute = std::thread::spawn(move || {
        compute_store.compute(compute_key, |set| {
            set.clear();
            set.insert(b"new-a".to_vec());
            set.insert(b"new-b".to_vec());
        });
    });
    gate.wait_until_reached();

    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let append_store = Arc::clone(&store);
    let append_key = key.clone();
    let append = std::thread::spawn(move || {
        append_store.append(append_key, b"ordinary".to_vec());
        completed_tx
            .send(())
            .expect("signal batch overlap completion");
    });
    let append_completed_inside_batch =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("batch overlap disconnected"),
        };
    gate.release();
    compute.join().expect("multi-action compute must join");
    if !append_completed_inside_batch {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("append must complete after batch publication");
    }
    append.join().expect("batch overlap append must join");

    assert!(!append_completed_inside_batch);
    let expected = Some(HashSet::from([
        b"new-a".to_vec(),
        b"new-b".to_vec(),
        b"ordinary".to_vec(),
    ]));
    assert_eq!(store.get_hashset(&key), expected);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeySetStore::try_init_new(directory.path())
            .expect("reopen multi-action set store")
            .into_store();
        assert_eq!(reopened.get_hashset(&key), expected);
        drop(reopened);
    }
}

#[test]
fn conditional_noop_and_sync_async_compute_keep_documented_outcomes() {
    let store = DurableKeySetStore::new_vec_based();
    let present = b"present".to_vec();
    store.append(present.clone(), b"seed".to_vec());
    let present_calls = AtomicUsize::new(0);
    store.compute_if_present(present.clone(), |set| {
        present_calls.fetch_add(1, Ordering::SeqCst);
        set.insert(b"sync".to_vec());
    });
    let absent_calls = AtomicUsize::new(0);
    store.compute_if_present(b"absent".to_vec(), |_| {
        absent_calls.fetch_add(1, Ordering::SeqCst);
    });
    store.compute_if_absent(present.clone(), |_| {
        absent_calls.fetch_add(1, Ordering::SeqCst);
    });
    block_on(store.compute_async(present.clone(), async |set| {
        set.insert(b"async".to_vec());
    }));
    let before_noop = store.get_hashset(&present);
    store.compute(present.clone(), |_| {});

    assert_eq!(present_calls.load(Ordering::SeqCst), 1);
    assert_eq!(absent_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.get_hashset(&present), before_noop);
    assert!(store.contains_in_set(&present, b"sync"));
    assert!(store.contains_in_set(&present, b"async"));
}

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
