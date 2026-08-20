//! Semantic scheduling checkpoints for deterministic maintenance tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

const WATCHDOG: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum MaintenanceCheckpoint {
    SnapshotCapture,
    RecorderActivation,
    StagingEncode,
    StagingValidation,
    Cutover,
    WriterHandoff,
    ManifestPrepared,
    ManifestPreviousPublished,
    ManifestReplacementPublished,
    ManifestCleanupPending,
    Cleanup,
}

#[derive(Clone, Default)]
pub(crate) struct MaintenanceObserver {
    state: Option<Arc<ScheduleState>>,
}

pub(crate) struct MaintenanceController {
    state: Arc<ScheduleState>,
}

struct ScheduleState {
    book: Mutex<ScheduleBook>,
    changed: Condvar,
}

#[derive(Default)]
struct ScheduleBook {
    gates: BTreeMap<MaintenanceCheckpoint, GateState>,
    trace: Vec<MaintenanceCheckpoint>,
}

#[derive(Default)]
struct GateState {
    reached: bool,
    released: bool,
}

impl MaintenanceObserver {
    pub(crate) fn controlled(
        paused: impl IntoIterator<Item = MaintenanceCheckpoint>,
    ) -> (Self, MaintenanceController) {
        let gates = paused
            .into_iter()
            .map(|checkpoint| (checkpoint, GateState::default()))
            .collect();
        let state = Arc::new(ScheduleState {
            book: Mutex::new(ScheduleBook {
                gates,
                trace: Vec::new(),
            }),
            changed: Condvar::new(),
        });
        (
            Self {
                state: Some(Arc::clone(&state)),
            },
            MaintenanceController { state },
        )
    }

    pub(crate) fn checkpoint(&self, checkpoint: MaintenanceCheckpoint) {
        let Some(state) = self.state.as_ref() else {
            return;
        };
        let mut book = lock(&state.book);
        book.trace.push(checkpoint);
        let Some(gate) = book.gates.get_mut(&checkpoint) else {
            return;
        };
        gate.reached = true;
        state.changed.notify_all();
        while !book
            .gates
            .get(&checkpoint)
            .is_some_and(|gate| gate.released)
        {
            book = state
                .changed
                .wait(book)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl MaintenanceController {
    pub(crate) fn wait_until_reached(&self, checkpoint: MaintenanceCheckpoint) {
        let book = lock(&self.state.book);
        assert!(
            book.gates.contains_key(&checkpoint),
            "maintenance checkpoint was not armed: {checkpoint:?}"
        );
        let (book, timeout) = self
            .state
            .changed
            .wait_timeout_while(book, WATCHDOG, |book| {
                !book.gates.get(&checkpoint).is_some_and(|gate| gate.reached)
            })
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            book.gates.get(&checkpoint).is_some_and(|gate| gate.reached),
            "maintenance checkpoint was not reached: {checkpoint:?}"
        );
        assert!(!timeout.timed_out(), "maintenance checkpoint timed out");
    }

    pub(crate) fn release(&self, checkpoint: MaintenanceCheckpoint) {
        let mut book = lock(&self.state.book);
        let gate = book
            .gates
            .get_mut(&checkpoint)
            .unwrap_or_else(|| panic!("maintenance checkpoint was not armed: {checkpoint:?}"));
        gate.released = true;
        self.state.changed.notify_all();
    }

    pub(crate) fn trace(&self) -> Vec<MaintenanceCheckpoint> {
        lock(&self.state.book).trace.clone()
    }

    fn release_all(&self) {
        let mut book = lock(&self.state.book);
        for gate in book.gates.values_mut() {
            gate.released = true;
        }
        self.state.changed.notify_all();
    }
}

impl Drop for MaintenanceController {
    fn drop(&mut self) {
        self.release_all();
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
    fn checkpoints_block_until_their_exact_release() {
        let (observer, controller) =
            MaintenanceObserver::controlled([MaintenanceCheckpoint::StagingEncode]);
        let worker = std::thread::spawn(move || {
            observer.checkpoint(MaintenanceCheckpoint::SnapshotCapture);
            observer.checkpoint(MaintenanceCheckpoint::StagingEncode);
            observer.checkpoint(MaintenanceCheckpoint::StagingValidation);
        });

        controller.wait_until_reached(MaintenanceCheckpoint::StagingEncode);
        assert!(!worker.is_finished());
        controller.release(MaintenanceCheckpoint::StagingEncode);
        worker.join().expect("maintenance worker must finish");
    }

    #[test]
    fn trace_preserves_semantic_checkpoint_order() {
        let (observer, controller) = MaintenanceObserver::controlled([]);
        for checkpoint in [
            MaintenanceCheckpoint::SnapshotCapture,
            MaintenanceCheckpoint::RecorderActivation,
            MaintenanceCheckpoint::StagingEncode,
            MaintenanceCheckpoint::StagingValidation,
            MaintenanceCheckpoint::Cutover,
            MaintenanceCheckpoint::WriterHandoff,
            MaintenanceCheckpoint::ManifestPrepared,
            MaintenanceCheckpoint::ManifestPreviousPublished,
            MaintenanceCheckpoint::ManifestReplacementPublished,
            MaintenanceCheckpoint::ManifestCleanupPending,
            MaintenanceCheckpoint::Cleanup,
        ] {
            observer.checkpoint(checkpoint);
        }
        assert_eq!(
            controller.trace(),
            [
                MaintenanceCheckpoint::SnapshotCapture,
                MaintenanceCheckpoint::RecorderActivation,
                MaintenanceCheckpoint::StagingEncode,
                MaintenanceCheckpoint::StagingValidation,
                MaintenanceCheckpoint::Cutover,
                MaintenanceCheckpoint::WriterHandoff,
                MaintenanceCheckpoint::ManifestPrepared,
                MaintenanceCheckpoint::ManifestPreviousPublished,
                MaintenanceCheckpoint::ManifestReplacementPublished,
                MaintenanceCheckpoint::ManifestCleanupPending,
                MaintenanceCheckpoint::Cleanup,
            ]
        );
    }

    #[test]
    fn controller_drop_releases_a_blocked_unwinding_worker() {
        let (observer, controller) =
            MaintenanceObserver::controlled([MaintenanceCheckpoint::Cutover]);
        let worker = std::thread::spawn(move || {
            observer.checkpoint(MaintenanceCheckpoint::Cutover);
            panic!("injected maintenance panic");
        });

        controller.wait_until_reached(MaintenanceCheckpoint::Cutover);
        drop(controller);
        assert!(worker.join().is_err());
    }

    #[test]
    fn maintenance_schedule_is_registered_only_below_the_test_only_crate_module() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let crate_root = std::fs::read_to_string(root.join("src/lib.rs")).expect("read lib.rs");
        assert!(crate_root.contains("#[cfg(test)]\nmod test_support;"));
        assert!(!crate_root.contains("maintenance_schedule"));

        let support = std::fs::read_to_string(root.join("src/test_support/mod.rs"))
            .expect("read test-support module");
        assert!(support.contains("pub(crate) mod maintenance_schedule;"));
    }
}
