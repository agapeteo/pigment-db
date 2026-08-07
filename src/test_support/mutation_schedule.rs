//! Semantic mutation scheduling primitives used by crate unit tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

pub(crate) const WATCHDOG: Duration = Duration::from_secs(10);
pub(crate) const PROCESS_CHECKPOINT_ENV: &str = "PIGMENT_DB_MUTATION_CHECKPOINT";
pub(crate) const PROCESS_CHECKPOINT_EXIT_CODE: i32 = 86;
pub(crate) const PROCESS_CHILD_MODE_ENV: &str = "PIGMENT_DB_MUTATION_CHILD_MODE";
pub(crate) const PROCESS_STORE_DIR_ENV: &str = "PIGMENT_DB_MUTATION_STORE_DIR";

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
}
