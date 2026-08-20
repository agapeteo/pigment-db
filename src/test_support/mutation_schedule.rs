//! Semantic mutation scheduling primitives used by crate unit tests.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

pub(crate) const WATCHDOG: Duration = Duration::from_secs(10);
pub(crate) const PROCESS_CHECKPOINT_ENV: &str = "PIGMENT_DB_MUTATION_CHECKPOINT";
pub(crate) const PROCESS_CHECKPOINT_EXIT_CODE: i32 = 86;
pub(crate) const PROCESS_CHILD_MODE_ENV: &str = "PIGMENT_DB_MUTATION_CHILD_MODE";
pub(crate) const PROCESS_STORE_DIR_ENV: &str = "PIGMENT_DB_MUTATION_STORE_DIR";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum LockRank {
    Maintenance,
    Shard,
    Wal,
}

thread_local! {
    static HELD_LOCK_RANKS: RefCell<Vec<(LockRank, &'static str)>> = const { RefCell::new(Vec::new()) };
}

pub(crate) struct LockRankGuard {
    rank: LockRank,
    label: &'static str,
}

pub(crate) struct LockRankWatchdog {
    started: std::time::Instant,
    timeout: Duration,
    context: &'static str,
}

impl LockRankGuard {
    pub(crate) fn enter(rank: LockRank, label: &'static str) -> Self {
        HELD_LOCK_RANKS.with(|held| {
            let mut held = held.borrow_mut();
            if let Some((current, current_label)) = held.last().copied() {
                assert!(
                    rank >= current,
                    "lock-rank inversion: attempted {rank:?} ({label}) while holding {current:?} ({current_label}); required Maintenance < Shard < WAL"
                );
            }
            held.push((rank, label));
        });
        Self { rank, label }
    }
}

impl Drop for LockRankGuard {
    fn drop(&mut self) {
        HELD_LOCK_RANKS.with(|held| {
            let popped = held.borrow_mut().pop();
            assert_eq!(
                popped,
                Some((self.rank, self.label)),
                "lock-rank guards must drop in LIFO order"
            );
        });
    }
}

impl LockRankWatchdog {
    pub(crate) fn new(timeout: Duration, context: &'static str) -> Self {
        Self {
            started: std::time::Instant::now(),
            timeout,
            context,
        }
    }

    pub(crate) fn assert_progress(&self) {
        if self.started.elapsed() >= self.timeout {
            let held = HELD_LOCK_RANKS.with(|held| held.borrow().clone());
            panic!(
                "lock-order watchdog expired in {} after {:?}; held ranks: {held:?}",
                self.context, self.timeout
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationPhase {
    AcceptanceEntered,
    AcceptedBeforePublication,
    Published,
}

#[derive(Clone, Default)]
pub(crate) struct MutationObserver {
    gate: Option<Arc<OneShotGate>>,
    trace: Option<PhaseTrace>,
}

#[derive(Clone)]
struct PhaseTrace {
    label: Vec<u8>,
    phases: Arc<Mutex<Vec<MutationPhase>>>,
}

struct OneShotGate {
    label: Vec<u8>,
    phase: MutationPhase,
    claimed: AtomicBool,
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    reached: bool,
    released: bool,
}

pub(crate) struct GateController {
    gate: Arc<OneShotGate>,
}

impl MutationObserver {
    pub(crate) fn one_shot(label: Vec<u8>, phase: MutationPhase) -> (Self, GateController) {
        let gate = Arc::new(OneShotGate {
            label,
            phase,
            claimed: AtomicBool::new(false),
            state: Mutex::new(GateState::default()),
            changed: Condvar::new(),
        });
        (
            Self {
                gate: Some(Arc::clone(&gate)),
                trace: None,
            },
            GateController { gate },
        )
    }

    pub(crate) fn recording(label: Vec<u8>) -> (Self, Arc<Mutex<Vec<MutationPhase>>>) {
        let phases = Arc::new(Mutex::new(Vec::new()));
        let trace = PhaseTrace {
            label,
            phases: Arc::clone(&phases),
        };
        (
            Self {
                gate: None,
                trace: Some(trace),
            },
            phases,
        )
    }

    pub(crate) fn notify(&self, label: &[u8], phase: MutationPhase) {
        exit_at_requested_checkpoint(match phase {
            MutationPhase::AcceptanceEntered => "acceptance-entered",
            MutationPhase::AcceptedBeforePublication => "accepted-before-publication",
            MutationPhase::Published => "published",
        });
        if let Some(trace) = self.trace.as_ref() {
            if trace.label == label {
                lock(&trace.phases).push(phase);
            }
        }
        let Some(gate) = self.gate.as_ref() else {
            return;
        };
        if gate.label != label || gate.phase != phase {
            return;
        }
        if gate
            .claimed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let mut state = lock(&gate.state);
        state.reached = true;
        gate.changed.notify_all();
        while !state.released {
            state = gate
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl GateController {
    pub(crate) fn wait_until_reached(&self) {
        let state = lock(&self.gate.state);
        let (state, timeout) = self
            .gate
            .changed
            .wait_timeout_while(state, WATCHDOG, |state| !state.reached)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.reached,
            "mutation did not reach the armed semantic gate"
        );
        assert!(!timeout.timed_out(), "semantic gate watchdog expired");
    }

    pub(crate) fn release(&self) {
        let mut state = lock(&self.gate.state);
        state.released = true;
        self.gate.changed.notify_all();
    }
}

impl Drop for GateController {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) fn exit_at_requested_checkpoint(checkpoint: &str) {
    if std::env::var_os(PROCESS_CHECKPOINT_ENV).as_deref() == Some(checkpoint.as_ref()) {
        std::process::exit(PROCESS_CHECKPOINT_EXIT_CODE);
    }
}

pub(crate) fn run_checkpoint_child(
    exact_test_name: &str,
    store_dir: &std::path::Path,
    mode: &str,
    checkpoint: Option<&str>,
) {
    let executable = std::env::current_exe().expect("locate unit-test executable");
    let mut command = std::process::Command::new(executable);
    command
        .arg(exact_test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(PROCESS_CHILD_MODE_ENV, mode)
        .env(PROCESS_STORE_DIR_ENV, store_dir);
    if let Some(checkpoint) = checkpoint {
        command.env(PROCESS_CHECKPOINT_ENV, checkpoint);
    }
    let mut child = command.spawn().expect("spawn mutation checkpoint child");
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("poll checkpoint child") {
            assert_eq!(
                status.code(),
                Some(PROCESS_CHECKPOINT_EXIT_CODE),
                "checkpoint child exited unexpectedly"
            );
            return;
        }
        if started.elapsed() >= WATCHDOG {
            let _ = child.kill();
            let _ = child.wait();
            panic!("mutation checkpoint child timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_gate_releases_a_matching_notification() {
        let (observer, controller) =
            MutationObserver::one_shot(b"key".to_vec(), MutationPhase::AcceptanceEntered);
        let worker = std::thread::spawn(move || {
            observer.notify(b"other", MutationPhase::AcceptanceEntered);
            observer.notify(b"key", MutationPhase::AcceptanceEntered);
        });

        controller.wait_until_reached();
        controller.release();
        worker.join().unwrap();
    }

    #[test]
    fn maintenance_shard_wal_rank_order_is_accepted() {
        let _maintenance = LockRankGuard::enter(LockRank::Maintenance, "maintenance");
        let _shard = LockRankGuard::enter(LockRank::Shard, "shard");
        let _wal = LockRankGuard::enter(LockRank::Wal, "wal");
    }

    #[test]
    fn reverse_lock_rank_panics_with_actionable_diagnostics() {
        let result = std::panic::catch_unwind(|| {
            let _wal = LockRankGuard::enter(LockRank::Wal, "wal");
            let _maintenance = LockRankGuard::enter(LockRank::Maintenance, "maintenance");
        });
        let panic = result.expect_err("reverse lock order must panic");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(message.contains("required Maintenance < Shard < WAL"));
    }

    #[test]
    fn panic_unwinding_clears_the_thread_lock_rank_stack() {
        let _ = std::panic::catch_unwind(|| {
            let _maintenance = LockRankGuard::enter(LockRank::Maintenance, "maintenance");
            panic!("injected callback panic");
        });
        let _maintenance = LockRankGuard::enter(LockRank::Maintenance, "next-maintenance");
    }

    #[test]
    fn watchdog_diagnostic_lists_the_current_lock_path() {
        let _maintenance = LockRankGuard::enter(LockRank::Maintenance, "maintenance-gate");
        let result = std::panic::catch_unwind(|| {
            LockRankWatchdog::new(Duration::ZERO, "cutover").assert_progress();
        });
        let panic = result.expect_err("zero-duration watchdog must expire");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(message.contains("cutover"));
        assert!(message.contains("Maintenance"));
        assert!(message.contains("maintenance-gate"));
    }
}
