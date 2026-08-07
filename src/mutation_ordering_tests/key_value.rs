//! Deterministic key/value mutation-ordering tests.

use super::DurableKeyValueStore;
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
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;

/// CMO-CROSS-1/3/4: only the occupied DashMap shard is held across a mutation.
#[test]
fn different_shard_progress_and_same_shard_waiting() {
    for phase in [
        MutationPhase::AcceptanceEntered,
        MutationPhase::AcceptedBeforePublication,
    ] {
        let directory = tempfile::tempdir().expect("create key/value cross-shard directory");
        let mut store = DurableKeyValueStore::try_init_new(directory.path())
            .expect("initialize key/value cross-shard store")
            .into_store();
        let keys = select_shard_keys(&store.store);
        let (observer, gate) = MutationObserver::one_shot(keys.anchor.clone(), phase);
        store.mutation_observer = observer;
        let store = Arc::new(store);

        let anchor_store = Arc::clone(&store);
        let anchor_key = keys.anchor.clone();
        let anchor = std::thread::spawn(move || anchor_store.put(anchor_key, b"anchor".to_vec()));
        gate.wait_until_reached();

        let (different_tx, different_rx) = mpsc::sync_channel(0);
        let different_store = Arc::clone(&store);
        let different_key = keys.different_shard.clone();
        let different = std::thread::spawn(move || {
            different_store.put(different_key, b"different".to_vec());
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
            same_store.put(same_key, b"same".to_vec());
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
        assert_eq!(store.get(&keys.anchor), Some(b"anchor".to_vec()));
        assert_eq!(
            store.get(&keys.different_shard),
            Some(b"different".to_vec())
        );
        assert_eq!(store.get(&keys.same_shard), Some(b"same".to_vec()));
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeyValueStore::try_init_new(directory.path())
                .expect("reopen key/value cross-shard store")
                .into_store();
            assert_eq!(reopened.get(&keys.anchor), Some(b"anchor".to_vec()));
            assert_eq!(
                reopened.get(&keys.different_shard),
                Some(b"different".to_vec())
            );
            assert_eq!(reopened.get(&keys.same_shard), Some(b"same".to_vec()));
            drop(reopened);
        }
    }
}

/// CMO-CROSS-2: another shard may prepare but waits for WAL acceptance.
#[test]
fn different_shard_prepares_but_waits_for_busy_wal() {
    let (writer, writer_gate) = BlockingWriter::new(1);
    let mut store = DurableKeyValueStore {
        store: DashMap::new(),
        wal: WalStorage::new_with_rollback(writer, rollback_blocking),
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
    let anchor = std::thread::spawn(move || anchor_store.put(anchor_key, b"anchor".to_vec()));
    writer_gate.wait_until_blocked();

    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let different_store = Arc::clone(&store);
    let different_key = keys.different_shard.clone();
    let different = std::thread::spawn(move || {
        different_store.put(different_key, b"different".to_vec());
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
        let directory = tempfile::tempdir().expect("create key/value conformance directory");
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .expect("initialize key/value conformance store")
            .into_store();
        let keys = select_shard_keys(&store.store);
        cross_shard::run_concurrently(
            || store.put(keys.anchor.clone(), schedule.to_ne_bytes().to_vec()),
            || {
                store.put(
                    keys.different_shard.clone(),
                    (schedule + 1).to_ne_bytes().to_vec(),
                )
            },
        );
        let anchor = store.get(&keys.anchor);
        let different = store.get(&keys.different_shard);
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeyValueStore::try_init_new(directory.path())
                .expect("reopen key/value conformance store")
                .into_store();
            assert_eq!(reopened.get(&keys.anchor), anchor);
            assert_eq!(reopened.get(&keys.different_shard), different);
            drop(reopened);
        }
    }
}

#[test]
#[ignore = "release-only 10,000-history same-key conformance"]
fn conformance_same_key_10k() {
    for history in 0_usize..10_000 {
        let directory = tempfile::tempdir().expect("create key/value same-key directory");
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .expect("initialize key/value same-key store")
            .into_store();
        let key = b"same-key".to_vec();
        cross_shard::run_concurrently(
            || store.put(key.clone(), history.to_ne_bytes().to_vec()),
            || store.put(key.clone(), (history + 1).to_ne_bytes().to_vec()),
        );
        let expected = store.get(&key);
        drop(store);
        for _ in 0..3 {
            let reopened = DurableKeyValueStore::try_init_new(directory.path())
                .expect("reopen key/value same-key store")
                .into_store();
            assert_eq!(reopened.get(&key), expected);
            drop(reopened);
        }
    }
}

/// CMO-FAIL-4: rejected ordinary key/value changes publish nothing and release the shard.
#[test]
fn rejected_put_and_remove_preserve_state_and_allow_progress() {
    for rejects_remove in [false, true] {
        let (writer, handle) = ScriptedWriter::new(WriterFault::WriteCall(8), false);
        let store = DurableKeyValueStore {
            store: DashMap::new(),
            wal: WalStorage::new_with_rollback(writer, rollback_scripted),
            mutation_observer: MutationObserver::default(),
        };
        let key = b"key".to_vec();
        store.put(key.clone(), b"before".to_vec());
        let checkpoint = handle.bytes();

        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if rejects_remove {
                store.remove(&key);
            } else {
                store.put(key.clone(), b"rejected".to_vec());
            }
        }));
        assert!(
            rejected.is_err(),
            "compatibility method must retain panic behavior"
        );
        assert_eq!(store.get(&key), Some(b"before".to_vec()));
        assert_eq!(handle.bytes(), checkpoint);
        assert_eq!(
            crate::wal::read_forward(&handle.bytes()).get(&key),
            Some(&b"before".to_vec())
        );

        store.put(key.clone(), b"later".to_vec());
        assert_eq!(store.get(&key), Some(b"later".to_vec()));
        assert_eq!(
            crate::wal::read_forward(&handle.bytes()).get(&key),
            Some(&b"later".to_vec())
        );
    }
}

/// CMO-CALL-3: a panicking compute callback publishes nothing and releases its shard.
#[test]
fn compute_panic_preserves_state_and_allows_progress() {
    let store = DurableKeyValueStore::new_vec_based();
    let key = b"panic".to_vec();
    store.put(key.clone(), b"before".to_vec());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.compute(key.clone(), |_| panic!("scripted callback panic"));
    }));
    assert!(panic.is_err());
    assert_eq!(store.get(&key), Some(b"before".to_vec()));
    store.put(key.clone(), b"later".to_vec());
    assert_eq!(store.get(&key), Some(b"later".to_vec()));
}

/// CMO-READ-2: a read cannot pass an accepted-but-unpublished same-key put.
#[test]
fn read_at_accepted_boundary_returns_complete_published_value() {
    let mut store = DurableKeyValueStore::new_vec_based();
    let key = b"read-boundary".to_vec();
    store.put(key.clone(), b"before".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);
    let put_store = Arc::clone(&store);
    let put_key = key.clone();
    let put = std::thread::spawn(move || put_store.put(put_key, b"after".to_vec()));
    gate.wait_until_reached();

    let (read_tx, read_rx) = mpsc::sync_channel(0);
    let read_store = Arc::clone(&store);
    let read_key = key.clone();
    let read = std::thread::spawn(move || read_tx.send(read_store.get(&read_key)).unwrap());
    assert!(matches!(
        read_rx.recv_timeout(std::time::Duration::from_millis(250)),
        Err(RecvTimeoutError::Timeout)
    ));
    gate.release();
    put.join().unwrap();
    assert_eq!(
        read_rx.recv_timeout(WATCHDOG).unwrap(),
        Some(b"after".to_vec())
    );
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
    let mut store = DurableKeyValueStore::try_init_new(&directory)
        .expect("initialize key/value checkpoint child")
        .into_store();
    let key = b"prefix".to_vec();
    store.put(key.clone(), b"before".to_vec());
    store.wal.sync_all().unwrap();

    if mode == "blocked-contender" {
        let (observer, gate) =
            MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
        store.mutation_observer = observer;
        let store = Arc::new(store);
        let first_store = Arc::clone(&store);
        let first_key = key.clone();
        let _first = std::thread::spawn(move || first_store.put(first_key, b"first".to_vec()));
        gate.wait_until_reached();
        store.wal.sync_all().unwrap();
        let contender_store = Arc::clone(&store);
        let contender_key = key.clone();
        let _contender =
            std::thread::spawn(move || contender_store.put(contender_key, b"contender".to_vec()));
        std::process::exit(crate::test_support::mutation_schedule::PROCESS_CHECKPOINT_EXIT_CODE);
    }

    std::env::set_var(
        PROCESS_CHECKPOINT_ENV,
        checkpoint.expect("checkpoint child phase"),
    );
    store.put(key, b"after".to_vec());
    unreachable!("checkpoint child must exit from observer notification");
}

#[test]
fn process_prefixes_reopen_one_accepted_key_value_history() {
    for (mode, checkpoint, expected) in [
        ("before", Some("acceptance-entered"), b"before".as_slice()),
        (
            "accepted",
            Some("accepted-before-publication"),
            b"after".as_slice(),
        ),
        ("published", Some("published"), b"after".as_slice()),
        ("blocked-contender", None, b"first".as_slice()),
    ] {
        let directory = tempfile::tempdir().expect("create key/value checkpoint directory");
        run_checkpoint_child(
            "key_value_store::mutation_ordering_tests::process_prefix_child",
            directory.path(),
            mode,
            checkpoint,
        );
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .expect("reopen key/value checkpoint store")
            .into_store();
        assert_eq!(reopened.get(b"prefix"), Some(expected.to_vec()));
    }
}

/// CMO-ORDER-2: overlapping puts must choose one live and reopened order.
#[test]
fn concurrent_puts_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create key/value ordering directory");
    let key = b"same-key".to_vec();
    let mut store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize key/value ordering store")
        .into_store();
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first = std::thread::spawn(move || first_store.put(first_key, b"first".to_vec()));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal second put start");
        second_store.put(second_key, b"second".to_vec());
        completed_tx.send(()).expect("signal second put completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("second put thread must start");

    let second_completed_while_first_was_parked =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("second put thread disconnected"),
        };

    gate.release();
    first.join().expect("first put must complete");
    if !second_completed_while_first_was_parked {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("second put must complete after first publication");
    }
    second.join().expect("second put must join");

    let live = store.get(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .expect("reopen key/value ordering store")
            .into_store();
        assert_eq!(
            reopened.get(&key),
            live,
            "overlapping puts selected different live and durable orders"
        );
        drop(reopened);
    }
}

/// CMO-ORDER-2: numeric replacement and ordinary put share one order.
#[test]
fn set_number_and_put_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create numeric ordering directory");
    let key = b"numeric-key".to_vec();
    let mut store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize numeric ordering store")
        .into_store();
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first = std::thread::spawn(move || first_store.set_number(first_key, 42));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal ordinary put start");
        second_store.put(second_key, b"second".to_vec());
        completed_tx
            .send(())
            .expect("signal ordinary put completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("ordinary put thread must start");
    let second_completed_while_number_was_parked =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("ordinary put thread disconnected"),
        };

    gate.release();
    first.join().expect("set_number must complete");
    if !second_completed_while_number_was_parked {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("ordinary put must complete after numeric publication");
    }
    second.join().expect("ordinary put must join");

    let live = store.get(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .expect("reopen numeric ordering store")
            .into_store();
        assert_eq!(
            reopened.get(&key),
            live,
            "set_number and put selected different live and durable orders"
        );
        drop(reopened);
    }
}

/// CMO-ORDER-2: absent deletion and recreation remain in one key order.
#[test]
fn absent_remove_and_put_keep_live_and_reopened_order() {
    let directory = tempfile::tempdir().expect("create deletion ordering directory");
    let key = b"deleted-key".to_vec();
    let mut store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize deletion ordering store")
        .into_store();
    let (observer, gate) =
        MutationObserver::one_shot(key.clone(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);

    let first_store = Arc::clone(&store);
    let first_key = key.clone();
    let first = std::thread::spawn(move || first_store.remove(&first_key));
    gate.wait_until_reached();

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (completed_tx, completed_rx) = mpsc::sync_channel(0);
    let second_store = Arc::clone(&store);
    let second_key = key.clone();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal recreating put start");
        second_store.put(second_key, b"recreated".to_vec());
        completed_tx
            .send(())
            .expect("signal recreating put completion");
    });
    started_rx
        .recv_timeout(WATCHDOG)
        .expect("recreating put thread must start");
    let put_completed_while_remove_was_parked =
        match completed_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => panic!("recreating put thread disconnected"),
        };

    gate.release();
    first.join().expect("absent remove must complete");
    if !put_completed_while_remove_was_parked {
        completed_rx
            .recv_timeout(WATCHDOG)
            .expect("recreating put must complete after remove publication");
    }
    second.join().expect("recreating put must join");

    let live = store.get(&key);
    drop(store);
    for _ in 0..3 {
        let reopened = DurableKeyValueStore::try_init_new(directory.path())
            .expect("reopen deletion ordering store")
            .into_store();
        assert_eq!(
            reopened.get(&key),
            live,
            "absent remove and recreation selected different orders"
        );
        drop(reopened);
    }
}
