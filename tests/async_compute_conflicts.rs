use pigment_db::key_set_store::DurableKeySetStore;
use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

const WATCHDOG: Duration = Duration::from_secs(1);

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

struct ReleaseGate {
    entered: Option<mpsc::SyncSender<()>>,
    released: Arc<AtomicBool>,
}

impl Future for ReleaseGate {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).expect("report callback entry");
        }
        if self.released.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[test]
fn changed_same_key_progresses_and_rejects_the_pending_async_candidate() {
    let directory = tempfile::tempdir().expect("create async conflict directory");
    let store = Arc::new(
        DurableKeySetStore::try_init_new(directory.path())
            .expect("initialize async conflict store")
            .into_store(),
    );
    let key = b"shared".to_vec();
    store.append(key.clone(), b"original".to_vec());

    let released = Arc::new(AtomicBool::new(false));
    let callback_count = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let compute_store = Arc::clone(&store);
    let compute_key = key.clone();
    let compute_released = Arc::clone(&released);
    let compute_callback_count = Arc::clone(&callback_count);
    let compute = std::thread::spawn(move || {
        block_on(
            compute_store.try_compute_async(compute_key, async move |set| {
                compute_callback_count.fetch_add(1, Ordering::SeqCst);
                ReleaseGate {
                    entered: Some(entered_tx),
                    released: compute_released,
                }
                .await;
                set.insert(b"async-candidate".to_vec());
            }),
        )
    });

    entered_rx
        .recv_timeout(WATCHDOG)
        .expect("async callback must become pending");
    let append_store = Arc::clone(&store);
    let append_key = key.clone();
    let (append_done_tx, append_done_rx) = mpsc::channel();
    let append = std::thread::spawn(move || {
        append_store.append(append_key, b"concurrent".to_vec());
        append_done_tx.send(()).expect("report append completion");
    });

    let append_progressed = append_done_rx.recv_timeout(WATCHDOG).is_ok();
    released.store(true, Ordering::Release);
    let compute_result = compute.join().expect("join async compute");
    append.join().expect("join concurrent append");

    assert!(
        append_progressed,
        "same-key writes must not wait for a pending async callback"
    );
    let conflict = compute_result.expect_err("changed snapshots must reject async publication");
    assert_eq!(conflict.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    assert!(store.contains_in_set(&key, b"original"));
    assert!(store.contains_in_set(&key, b"concurrent"));
    assert!(!store.contains_in_set(&key, b"async-candidate"));
    drop(store);

    let reopened = DurableKeySetStore::try_init_new(directory.path())
        .expect("reopen async conflict store")
        .into_store();
    assert!(reopened.contains_in_set(&key, b"original"));
    assert!(reopened.contains_in_set(&key, b"concurrent"));
    assert!(!reopened.contains_in_set(&key, b"async-candidate"));
}
