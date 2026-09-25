//! Deterministic fault checkpoint recording for unit-test-only pipelines.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::maintenance_fixtures::{snapshot_directory, DirectoryByteSnapshot};

const MAINTENANCE_CHILD_MODE_ENV: &str = "PIGMENT_DB_MAINTENANCE_CHILD_MODE";
const MAINTENANCE_STORE_DIR_ENV: &str = "PIGMENT_DB_MAINTENANCE_STORE_DIR";
const MAINTENANCE_PHASE_ENV: &str = "PIGMENT_DB_MAINTENANCE_PHASE";
const MAINTENANCE_CUT_ENV: &str = "PIGMENT_DB_MAINTENANCE_CUT";
const MAINTENANCE_EXIT_CODE: i32 = 87;
/// When set, a child reaching its requested cut parks there instead of exiting: it writes
/// `paused` into this directory and continues once `resume` appears.
const MAINTENANCE_PAUSE_ENV: &str = "PIGMENT_DB_MAINTENANCE_PAUSE_DIR";
/// A paused child whose `resume` never came.
const MAINTENANCE_PAUSE_TIMEOUT_CODE: i32 = 88;
/// A paused child that resumed and completed its maintenance.
pub(crate) const MAINTENANCE_PAUSED_CHILD_COMPLETED: i32 = 89;
const WATCHDOG: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FaultCheckpoint {
    FreshPublication,
    RepairPublication,
    Migration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenancePhase {
    Prepared,
    PreviousPublished,
    ReplacementPublished,
    CleanupPending,
}

impl MaintenancePhase {
    fn name(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::PreviousPublished => "previous-published",
            Self::ReplacementPublished => "replacement-published",
            Self::CleanupPending => "cleanup-pending",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceCut {
    StagingCreate,
    StagingWrite,
    StagingSync,
    StagingValidate,
    ManifestWrite,
    ManifestSync,
    ManifestPublish,
    PreviousPublish,
    ReplacementPublish,
    ReopenValidation,
    Cleanup,
}

impl MaintenanceCut {
    fn name(self) -> &'static str {
        match self {
            Self::StagingCreate => "staging-create",
            Self::StagingWrite => "staging-write",
            Self::StagingSync => "staging-sync",
            Self::StagingValidate => "staging-validate",
            Self::ManifestWrite => "manifest-write",
            Self::ManifestSync => "manifest-sync",
            Self::ManifestPublish => "manifest-publish",
            Self::PreviousPublish => "previous-publish",
            Self::ReplacementPublish => "replacement-publish",
            Self::ReopenValidation => "reopen-validation",
            Self::Cleanup => "cleanup",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceFaultPoint {
    pub(crate) phase: MaintenancePhase,
    pub(crate) cut: MaintenanceCut,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceChildEvidence {
    pub(crate) before: DirectoryByteSnapshot,
    pub(crate) after: DirectoryByteSnapshot,
}

#[derive(Default)]
pub(crate) struct FaultCheckpointLog {
    reached: Mutex<Vec<FaultCheckpoint>>,
}

impl FaultCheckpointLog {
    pub(crate) fn record(&self, checkpoint: FaultCheckpoint) {
        self.reached.lock().unwrap().push(checkpoint);
    }

    pub(crate) fn reached(&self) -> Vec<FaultCheckpoint> {
        self.reached.lock().unwrap().clone()
    }
}

pub(crate) fn exit_at_maintenance_fault(point: MaintenanceFaultPoint) {
    if std::env::var_os(MAINTENANCE_CHILD_MODE_ENV).is_none() {
        return;
    }
    if std::env::var(MAINTENANCE_PHASE_ENV).as_deref() == Ok(point.phase.name())
        && std::env::var(MAINTENANCE_CUT_ENV).as_deref() == Ok(point.cut.name())
    {
        let Some(pause_dir) = std::env::var_os(MAINTENANCE_PAUSE_ENV).map(PathBuf::from) else {
            std::process::exit(MAINTENANCE_EXIT_CODE);
        };
        std::fs::write(pause_dir.join("paused"), b"").expect("signal paused");
        let started = Instant::now();
        while !pause_dir.join("resume").exists() {
            if started.elapsed() >= WATCHDOG {
                std::process::exit(MAINTENANCE_PAUSE_TIMEOUT_CODE);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Whether this process is a maintenance child asked to park at its cut rather than exit.
pub(crate) fn maintenance_child_pauses() -> bool {
    std::env::var_os(MAINTENANCE_PAUSE_ENV).is_some()
}

/// A maintenance child parked at a cut, holding whatever it holds there. Killed on drop.
pub(crate) struct PausedMaintenanceChild {
    child: Option<std::process::Child>,
    pause_dir: PathBuf,
}

impl PausedMaintenanceChild {
    /// The parked child's process id.
    pub(crate) fn id(&self) -> u32 {
        self.child.as_ref().expect("paused child").id()
    }

    /// Lets the child continue past its cut and returns its exit code.
    pub(crate) fn resume(mut self) -> i32 {
        std::fs::write(self.pause_dir.join("resume"), b"").expect("signal resume");
        let mut child = self.child.take().expect("paused child");
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait().expect("poll paused child") {
                return status.code().unwrap_or(-1);
            }
            if started.elapsed() >= WATCHDOG {
                let _ = child.kill();
                let _ = child.wait();
                panic!("resumed maintenance child did not finish");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for PausedMaintenanceChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Starts `exact_test_name` as a maintenance child on `store_dir` and returns once it is parked
/// at `point`.
pub(crate) fn pause_maintenance_child(
    exact_test_name: &str,
    store_dir: &Path,
    pause_dir: &Path,
    point: MaintenanceFaultPoint,
) -> PausedMaintenanceChild {
    let executable = std::env::current_exe().expect("locate unit-test executable");
    let child = std::process::Command::new(executable)
        .arg(exact_test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(MAINTENANCE_CHILD_MODE_ENV, "1")
        .env(MAINTENANCE_STORE_DIR_ENV, store_dir)
        .env(MAINTENANCE_PHASE_ENV, point.phase.name())
        .env(MAINTENANCE_CUT_ENV, point.cut.name())
        .env(MAINTENANCE_PAUSE_ENV, pause_dir)
        .spawn()
        .expect("spawn maintenance pause child");
    let mut paused = PausedMaintenanceChild {
        child: Some(child),
        pause_dir: pause_dir.to_path_buf(),
    };
    let started = Instant::now();
    while !pause_dir.join("paused").exists() {
        let child = paused.child.as_mut().expect("paused child");
        if let Some(status) = child.try_wait().expect("poll pause child") {
            panic!("maintenance child exited before its cut ({status})");
        }
        assert!(
            started.elapsed() < WATCHDOG,
            "maintenance child never paused"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    paused
}

pub(crate) fn maintenance_child_store_dir() -> Option<PathBuf> {
    std::env::var_os(MAINTENANCE_STORE_DIR_ENV).map(PathBuf::from)
}

pub(crate) fn run_maintenance_checkpoint_child(
    exact_test_name: &str,
    store_dir: &Path,
    point: MaintenanceFaultPoint,
) -> MaintenanceChildEvidence {
    run_maintenance_checkpoint_child_with_evidence_root(
        exact_test_name,
        store_dir,
        store_dir,
        point,
    )
}

pub(crate) fn run_maintenance_checkpoint_child_with_evidence_root(
    exact_test_name: &str,
    store_dir: &Path,
    evidence_root: &Path,
    point: MaintenanceFaultPoint,
) -> MaintenanceChildEvidence {
    let before = snapshot_directory(evidence_root).expect("snapshot evidence before child");
    let executable = std::env::current_exe().expect("locate unit-test executable");
    let mut child = std::process::Command::new(executable)
        .arg(exact_test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(MAINTENANCE_CHILD_MODE_ENV, "1")
        .env(MAINTENANCE_STORE_DIR_ENV, store_dir)
        .env(MAINTENANCE_PHASE_ENV, point.phase.name())
        .env(MAINTENANCE_CUT_ENV, point.cut.name())
        .spawn()
        .expect("spawn maintenance checkpoint child");
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("poll maintenance child") {
            assert_eq!(
                status.code(),
                Some(MAINTENANCE_EXIT_CODE),
                "maintenance child did not terminate at the requested checkpoint"
            );
            break;
        }
        if started.elapsed() >= WATCHDOG {
            let _ = child.kill();
            let _ = child.wait();
            panic!("maintenance checkpoint child timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    #[cfg(windows)]
    wait_for_released_lock_files(evidence_root);
    let after = snapshot_directory(evidence_root).expect("snapshot evidence after child");
    MaintenanceChildEvidence { before, after }
}

/// Waits until no lock file under `root` is still held by an exited child. Windows releases a
/// terminated process's locks asynchronously, and refuses reads of a held one; elsewhere a lock
/// is gone once its owner is reaped.
#[cfg(windows)]
fn wait_for_released_lock_files(root: &Path) {
    fn lock_files(directory: &Path, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => lock_files(&path, found),
                Ok(kind)
                    if kind.is_file()
                        && entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| name.ends_with(".pigment-lock")) =>
                {
                    found.push(path)
                }
                _ => {}
            }
        }
    }

    let mut pending = Vec::new();
    lock_files(root, &mut pending);
    let started = Instant::now();
    while let Some(path) = pending.last() {
        let released = match std::fs::File::open(path) {
            Ok(file) => !matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
            Err(_) => true,
        };
        if released {
            pending.pop();
            continue;
        }
        assert!(
            started.elapsed() < WATCHDOG,
            "{} was still held after its owner exited",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROBE: MaintenanceFaultPoint = MaintenanceFaultPoint {
        phase: MaintenancePhase::Prepared,
        cut: MaintenanceCut::ManifestSync,
    };

    #[test]
    fn maintenance_checkpoint_child_probe() {
        if maintenance_child_store_dir().is_some() {
            exit_at_maintenance_fault(PROBE);
        }
    }

    #[test]
    fn maintenance_child_exit_is_exact_and_preserves_artifact_evidence() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("authority"), b"complete").unwrap();
        let evidence = run_maintenance_checkpoint_child(
            "test_support::fault_checkpoint::tests::maintenance_checkpoint_child_probe",
            directory.path(),
            PROBE,
        );
        assert_eq!(evidence.before, evidence.after);
        assert_eq!(
            evidence.after.get(Path::new("authority")),
            Some(&b"complete".to_vec())
        );
    }

    #[test]
    fn every_maintenance_phase_and_cut_has_a_stable_process_identifier() {
        let phases = [
            MaintenancePhase::Prepared,
            MaintenancePhase::PreviousPublished,
            MaintenancePhase::ReplacementPublished,
            MaintenancePhase::CleanupPending,
        ];
        let cuts = [
            MaintenanceCut::StagingCreate,
            MaintenanceCut::StagingWrite,
            MaintenanceCut::StagingSync,
            MaintenanceCut::StagingValidate,
            MaintenanceCut::ManifestWrite,
            MaintenanceCut::ManifestSync,
            MaintenanceCut::ManifestPublish,
            MaintenanceCut::PreviousPublish,
            MaintenanceCut::ReplacementPublish,
            MaintenanceCut::ReopenValidation,
            MaintenanceCut::Cleanup,
        ];
        assert_eq!(phases.map(MaintenancePhase::name).len(), 4);
        assert_eq!(cuts.map(MaintenanceCut::name).len(), 11);
    }
}
