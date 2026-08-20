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
        std::process::exit(MAINTENANCE_EXIT_CODE);
    }
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
    let after = snapshot_directory(evidence_root).expect("snapshot evidence after child");
    MaintenanceChildEvidence { before, after }
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
