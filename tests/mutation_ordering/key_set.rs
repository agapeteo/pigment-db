//! Public key/set mutation-ordering contract tests.

use crate::support::assert_key_set_reopens;
use pigment_db::key_set_store::DurableKeySetStore;
use std::future::Future;
use std::pin::pin;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Waker};

#[test]
fn completion_before_invocation_orders_set_mutations() {
    let directory = tempfile::tempdir().expect("create set completion-order directory");
    let key = b"ordered".to_vec();
    let member = b"member".to_vec();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize set completion-order store")
        .into_store();
    store.append(key.clone(), member.clone());
    store.remove_from_set(key.clone(), member);
    let expected = None;
    assert_eq!(store.get_hashset(&key), expected);
    drop(store);
    assert_key_set_reopens(directory.path(), &key, &expected);
}

#[test]
fn overlapping_set_mutations_accept_either_order() {
    let directory = tempfile::tempdir().expect("create set overlap directory");
    let key = b"overlap".to_vec();
    let member = b"member".to_vec();
    let store = Arc::new(
        DurableKeySetStore::try_init_new(directory.path())
            .expect("initialize set overlap store")
            .into_store(),
    );
    store.append(key.clone(), member.clone());
    let barrier = Arc::new(Barrier::new(3));
    let append_store = Arc::clone(&store);
    let append_barrier = Arc::clone(&barrier);
    let append_key = key.clone();
    let append_member = member.clone();
    let append = std::thread::spawn(move || {
        append_barrier.wait();
        append_store.append(append_key, append_member);
    });
    let remove_store = Arc::clone(&store);
    let remove_barrier = Arc::clone(&barrier);
    let remove_key = key.clone();
    let remove_member = member.clone();
    let remove = std::thread::spawn(move || {
        remove_barrier.wait();
        remove_store.remove_from_set(remove_key, remove_member);
    });
    barrier.wait();
    append.join().expect("overlapping append must join");
    remove.join().expect("overlapping removal must join");
    let expected = store.get_hashset(&key);
    assert!(expected.is_none() || expected.as_ref().is_some_and(|set| set.contains(&member)));
    drop(store);
    assert_key_set_reopens(directory.path(), &key, &expected);
}

#[test]
fn all_set_mutators_participate_in_public_family_matrix() {
    let directory = tempfile::tempdir().expect("create set family matrix directory");
    let store = DurableKeySetStore::try_init_new(directory.path())
        .expect("initialize set family matrix store")
        .into_store();
    let main = b"main".to_vec();
    let deleted = b"deleted".to_vec();

    store.append(main.clone(), b"append".to_vec());
    store.remove_from_set(main.clone(), b"append".to_vec());
    store.append(main.clone(), b"callback-target".to_vec());
    store.remove_from_set_callback(main.clone(), b"callback-target".to_vec(), |_| {});
    store
        .try_compute(main.clone(), |set| {
            set.insert(b"try-compute".to_vec());
        })
        .unwrap();
    store.compute(main.clone(), |set| {
        set.insert(b"compute".to_vec());
    });
    block_on(store.try_compute_async(main.clone(), async |set| {
        set.insert(b"try-async".to_vec());
    }))
    .unwrap();
    block_on(store.compute_async(main.clone(), async |set| {
        set.insert(b"async".to_vec());
    }));
    store
        .try_compute_if_present(main.clone(), |set| {
            set.insert(b"try-present".to_vec());
        })
        .unwrap();
    store.compute_if_present(main.clone(), |set| {
        set.insert(b"present".to_vec());
    });
    store
        .try_compute_if_absent(b"try-absent".to_vec(), |set| {
            set.insert(b"created".to_vec());
        })
        .unwrap();
    store.compute_if_absent(b"absent".to_vec(), |set| {
        set.insert(b"created".to_vec());
    });
    store.append(deleted.clone(), b"gone".to_vec());
    store.remove_key(&deleted);

    let snapshots = [
        (main.clone(), store.get_hashset(&main)),
        (b"try-absent".to_vec(), store.get_hashset(b"try-absent")),
        (b"absent".to_vec(), store.get_hashset(b"absent")),
        (deleted.clone(), store.get_hashset(&deleted)),
    ];
    drop(store);
    for (key, expected) in snapshots {
        assert_key_set_reopens(directory.path(), &key, &expected);
    }
}

#[test]
fn overlapping_reads_never_observe_sync_or_async_working_sets() {
    for asynchronous in [false, true] {
        let directory = tempfile::tempdir().expect("create set read-visibility directory");
        let key = b"key".to_vec();
        let store = Arc::new(
            DurableKeySetStore::try_init_new(directory.path())
                .expect("initialize set read-visibility store")
                .into_store(),
        );
        store.append(key.clone(), b"before".to_vec());
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let compute_store = Arc::clone(&store);
        let compute_key = key.clone();
        let compute = std::thread::spawn(move || {
            if asynchronous {
                block_on(compute_store.compute_async(compute_key, async move |set| {
                    set.insert(b"new-a".to_vec());
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    set.insert(b"new-b".to_vec());
                }));
            } else {
                compute_store.compute(compute_key, move |set| {
                    set.insert(b"new-a".to_vec());
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    set.insert(b"new-b".to_vec());
                });
            }
        });
        entered_rx.recv().unwrap();

        let (read_tx, read_rx) = mpsc::sync_channel(0);
        let read_store = Arc::clone(&store);
        let read_key = key.clone();
        let read = std::thread::spawn(move || {
            read_tx.send(read_store.get_hashset(&read_key)).unwrap();
        });
        assert!(matches!(
            read_rx.recv_timeout(std::time::Duration::from_millis(250)),
            Err(RecvTimeoutError::Timeout)
        ));
        release_tx.send(()).unwrap();
        compute.join().unwrap();
        let observed = read_rx.recv().unwrap();
        read.join().unwrap();
        assert!(observed.as_ref().is_some_and(|set| {
            set.contains(b"before".as_slice())
                && set.contains(b"new-a".as_slice())
                && set.contains(b"new-b".as_slice())
        }));
        drop(store);
        assert_key_set_reopens(directory.path(), &key, &observed);
    }
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
