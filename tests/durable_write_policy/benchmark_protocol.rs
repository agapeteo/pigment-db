//! Shared policy-selected sampling; CPU affinity is fixed by the capture command.

use crate::support::{
    p95_nanos, CaptureRow, CellKey, Implementation, Policy, Workload, MIN_OPERATIONS,
    MIN_SAMPLE_DURATION, SAMPLE_COUNT, WARMUP_COUNT,
};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

pub const OPERATIONS_PER_ROUND: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerSchedule {
    StartOnly,
    PerOperation,
}

fn worker_schedule(policy: Policy) -> WorkerSchedule {
    match policy {
        Policy::Buffered => WorkerSchedule::StartOnly,
        Policy::Physical | Policy::Reference => WorkerSchedule::PerOperation,
    }
}

pub trait BenchStore: Sync {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize);
    fn operate(&self, workload: Workload, worker: usize, operation: usize);
}

pub fn measure_cell<S: BenchStore>(
    key: CellKey,
    implementation: Implementation,
    policy: Policy,
    mut build: impl FnMut() -> S,
) -> Vec<CaptureRow> {
    let schedule = worker_schedule(policy);
    for _ in 0..WARMUP_COUNT {
        let store = build();
        black_box(run_sample(&store, key, schedule));
    }

    let mut rows = Vec::with_capacity(SAMPLE_COUNT);
    for sample_index in 0..SAMPLE_COUNT {
        let store = build();
        let (elapsed, mut latencies) = run_sample(&store, key, schedule);
        let operations = latencies.len();
        assert!(operations >= MIN_OPERATIONS);
        assert!(elapsed >= MIN_SAMPLE_DURATION);
        rows.push(CaptureRow {
            sample_index,
            key,
            implementation,
            policy,
            operations,
            elapsed,
            p95_latency_ns: p95_nanos(&mut latencies),
            failed_operations: 0,
        });
    }
    rows
}

fn run_sample<S: BenchStore>(
    store: &S,
    key: CellKey,
    schedule: WorkerSchedule,
) -> (Duration, Vec<Duration>) {
    let mut elapsed = Duration::ZERO;
    let mut latencies = Vec::new();
    let mut base = 0;
    while elapsed < MIN_SAMPLE_DURATION || latencies.len() < MIN_OPERATIONS {
        for worker in 0..key.workers {
            store.prepare(key.workload, worker, base, OPERATIONS_PER_ROUND);
        }
        let (round_elapsed, mut round_latencies) = run_round(store, key, base, schedule);
        elapsed += round_elapsed;
        latencies.append(&mut round_latencies);
        base += OPERATIONS_PER_ROUND;
    }
    (elapsed, latencies)
}

fn run_round<S: BenchStore>(
    store: &S,
    key: CellKey,
    base: usize,
    schedule: WorkerSchedule,
) -> (Duration, Vec<Duration>) {
    if key.workers == 1 {
        let mut latencies = Vec::with_capacity(OPERATIONS_PER_ROUND);
        let wall_started = Instant::now();
        for operation in base..base + OPERATIONS_PER_ROUND {
            let call_started = Instant::now();
            store.operate(key.workload, 0, operation);
            latencies.push(call_started.elapsed());
        }
        return (wall_started.elapsed(), latencies);
    }

    std::thread::scope(|scope| {
        let start_barrier = Arc::new(Barrier::new(key.workers + 1));
        let operation_barrier = match schedule {
            WorkerSchedule::StartOnly => None,
            WorkerSchedule::PerOperation => Some(Arc::new(Barrier::new(key.workers))),
        };
        let mut handles = Vec::with_capacity(key.workers);
        for worker in 0..key.workers {
            let start_barrier = Arc::clone(&start_barrier);
            let operation_barrier = operation_barrier.clone();
            handles.push(scope.spawn(move || {
                let mut latencies = Vec::with_capacity(OPERATIONS_PER_ROUND);
                start_barrier.wait();
                for operation in base..base + OPERATIONS_PER_ROUND {
                    if let Some(operation_barrier) = &operation_barrier {
                        operation_barrier.wait();
                    }
                    let call_started = Instant::now();
                    store.operate(key.workload, worker, operation);
                    latencies.push(call_started.elapsed());
                }
                latencies
            }));
        }

        start_barrier.wait();
        let wall_started = Instant::now();
        let mut latencies = Vec::with_capacity(key.workers * OPERATIONS_PER_ROUND);
        for handle in handles {
            latencies.extend(handle.join().expect("benchmark worker panicked"));
        }
        (wall_started.elapsed(), latencies)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::{StorageMode, StoreFamily};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct RoundRendezvousProbe {
        first_calls_completed: AtomicUsize,
        fast_worker_lapped: AtomicBool,
    }

    impl RoundRendezvousProbe {
        fn new() -> Self {
            Self {
                first_calls_completed: AtomicUsize::new(0),
                fast_worker_lapped: AtomicBool::new(false),
            }
        }
    }

    impl BenchStore for RoundRendezvousProbe {
        fn prepare(&self, _workload: Workload, _worker: usize, _base: usize, _count: usize) {}

        fn operate(&self, _workload: Workload, worker: usize, operation: usize) {
            if operation == 0 {
                if worker != 0 {
                    std::thread::sleep(Duration::from_millis(50));
                }
                self.first_calls_completed.fetch_add(1, Ordering::SeqCst);
            } else if self.first_calls_completed.load(Ordering::SeqCst) < 8 {
                self.fast_worker_lapped.store(true, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn eight_worker_round_rendezvous_prevents_a_fast_worker_from_lapping() {
        let key = CellKey {
            family: StoreFamily::KeyValue,
            storage: StorageMode::Vector,
            workload: Workload::Write,
            workers: 8,
        };
        let probe = RoundRendezvousProbe::new();

        black_box(run_round(&probe, key, 0, WorkerSchedule::PerOperation));

        assert_eq!(probe.first_calls_completed.load(Ordering::SeqCst), 8);
        assert!(
            !probe.fast_worker_lapped.load(Ordering::SeqCst),
            "every worker must finish operation n before any worker starts operation n+1"
        );
    }

    #[test]
    fn hybrid_protocol_selects_scheduling_by_durability_policy() {
        assert_eq!(worker_schedule(Policy::Buffered), WorkerSchedule::StartOnly);
        assert_eq!(
            worker_schedule(Policy::Physical),
            WorkerSchedule::PerOperation
        );
        assert_eq!(
            worker_schedule(Policy::Reference),
            WorkerSchedule::PerOperation
        );
    }
}
