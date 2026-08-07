//! Shared deterministic cross-shard schedules for store unit tests.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Barrier};
use std::time::Duration;

pub(crate) fn completes_within(receiver: &Receiver<()>, duration: Duration) -> bool {
    match receiver.recv_timeout(duration) {
        Ok(()) => true,
        Err(RecvTimeoutError::Timeout) => false,
        Err(RecvTimeoutError::Disconnected) => panic!("cross-shard worker disconnected"),
    }
}

pub(crate) fn run_concurrently<F, G>(left: F, right: G)
where
    F: FnOnce() + Send,
    G: FnOnce() + Send,
{
    std::thread::scope(|scope| {
        let barrier = Arc::new(Barrier::new(3));
        let left_barrier = Arc::clone(&barrier);
        let left = scope.spawn(move || {
            left_barrier.wait();
            left();
        });
        let right_barrier = Arc::clone(&barrier);
        let right = scope.spawn(move || {
            right_barrier.wait();
            right();
        });
        barrier.wait();
        left.join().expect("left controlled mutation must join");
        right.join().expect("right controlled mutation must join");
    });
}
