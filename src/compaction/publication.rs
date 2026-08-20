//! Recoverable compaction-publication internals.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::manifest::{
    decode_manifest, encode_manifest, verify_descriptor, ArtifactRole, CompactionManifest,
    ManifestMode, ManifestPhase, ManifestScope,
};
use super::{
    revalidate_closed_source_inventory, validate_closed_staging,
    validate_published_closed_replacement, PreparedClosedStaging,
};
use crate::{CompactionError, CompactionOperation, DurabilityPolicy};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceArtifactPaths {
    pub(crate) manifest: PathBuf,
    pub(crate) manifest_next: PathBuf,
    pub(crate) staging: PathBuf,
    pub(crate) previous: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestPublishStage {
    Created,
    Written,
    Flushed,
    Renamed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClosedPreviousStage {
    SourceMoved,
    PhasePublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClosedReplacementStage {
    ReplacementMoved,
    ReplacementReopened,
    PhasePublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClosedCleanupStage {
    CleanupPendingPublished,
    BeforePreviousArtifact(usize),
    BeforePreviousDirectory,
    BeforeManifest,
}

pub(crate) fn cleanup_closed_with_checkpoint(
    prepared: &PreparedClosedStaging,
    manifest: &mut CompactionManifest,
    mut checkpoint: impl FnMut(ClosedCleanupStage) -> io::Result<()>,
) -> Result<crate::CleanupStatus, CompactionError> {
    if manifest.phase != ManifestPhase::ReplacementPublished {
        return Err(CompactionError::FailedClosed {
            detail: "cleanup requires ReplacementPublished authority".to_owned(),
        });
    }
    validate_published_closed_replacement(prepared)?;
    let mut next = manifest.clone();
    next.phase = ManifestPhase::CleanupPending;
    publish_manifest_for_policy(&prepared.paths, &next, manifest.durability)?;
    *manifest = next;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::CleanupPending,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::Cleanup,
        },
    );
    if checkpoint(ClosedCleanupStage::CleanupPendingPublished).is_err() {
        return Ok(crate::CleanupStatus::Pending);
    }

    let anchor = match prepared.paths.previous.parent() {
        Some(anchor) => anchor,
        None => return Ok(crate::CleanupStatus::Pending),
    };
    let previous_name = match prepared.paths.previous.file_name() {
        Some(name) => name,
        None => return Ok(crate::CleanupStatus::Pending),
    };
    let mut owned = Vec::with_capacity(prepared.capture.inventory.len());
    for source in &prepared.capture.inventory {
        let Some(file_name) = source.relative_path.file_name() else {
            return Ok(crate::CleanupStatus::Pending);
        };
        let mut previous = source.clone();
        previous.relative_path = PathBuf::from(previous_name).join(file_name);
        previous.role = ArtifactRole::PreviousGeneration;
        if verify_descriptor(anchor, &previous).is_err() {
            return Ok(crate::CleanupStatus::Pending);
        }
        owned.push((previous, prepared.paths.previous.join(file_name)));
    }
    let expected_names = owned
        .iter()
        .filter_map(|(_, path)| path.file_name().map(OsString::from))
        .collect::<BTreeSet<_>>();
    let actual_names = match fs::read_dir(&prepared.paths.previous) {
        Ok(entries) => {
            let mut names = BTreeSet::new();
            for entry in entries {
                let Ok(entry) = entry else {
                    return Ok(crate::CleanupStatus::Pending);
                };
                let Ok(file_type) = entry.file_type() else {
                    return Ok(crate::CleanupStatus::Pending);
                };
                if !file_type.is_file() {
                    return Ok(crate::CleanupStatus::Pending);
                }
                names.insert(entry.file_name());
            }
            names
        }
        Err(_) => return Ok(crate::CleanupStatus::Pending),
    };
    if actual_names != expected_names {
        return Ok(crate::CleanupStatus::Pending);
    }

    for (index, (_, path)) in owned.iter().enumerate() {
        if checkpoint(ClosedCleanupStage::BeforePreviousArtifact(index)).is_err()
            || fs::remove_file(path).is_err()
        {
            return Ok(crate::CleanupStatus::Pending);
        }
    }
    if checkpoint(ClosedCleanupStage::BeforePreviousDirectory).is_err()
        || fs::remove_dir(&prepared.paths.previous).is_err()
    {
        return Ok(crate::CleanupStatus::Pending);
    }
    if manifest.durability == DurabilityPolicy::Physical
        && prepared
            .paths
            .previous
            .parent()
            .is_none_or(|parent| crate::durability::synchronize_directory(parent).is_err())
    {
        return Ok(crate::CleanupStatus::Pending);
    }
    if checkpoint(ClosedCleanupStage::BeforeManifest).is_err()
        || fs::remove_file(&prepared.paths.manifest).is_err()
    {
        return Ok(crate::CleanupStatus::Pending);
    }
    if manifest.durability == DurabilityPolicy::Physical
        && prepared
            .paths
            .manifest
            .parent()
            .is_none_or(|parent| crate::durability::synchronize_directory(parent).is_err())
    {
        return Ok(crate::CleanupStatus::Pending);
    }
    Ok(crate::CleanupStatus::Complete)
}

pub(crate) fn publish_closed_replacement_with_checkpoint(
    prepared: &PreparedClosedStaging,
    manifest: &mut CompactionManifest,
    mut checkpoint: impl FnMut(ClosedReplacementStage) -> io::Result<()>,
) -> Result<(), CompactionError> {
    if manifest.phase != ManifestPhase::PreviousPublished {
        return Err(CompactionError::FailedClosed {
            detail: "replacement publication requires PreviousPublished authority".to_owned(),
        });
    }
    validate_closed_staging(prepared)?;
    match fs::symlink_metadata(&prepared.capture.source_dir) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::PublishReplacement,
                path: prepared.capture.source_dir.clone(),
                source,
            });
        }
        Ok(_) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::PublishReplacement,
                path: prepared.capture.source_dir.clone(),
                source: io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "canonical replacement path already exists",
                ),
            });
        }
    }
    fs::rename(&prepared.paths.staging, &prepared.capture.source_dir).map_err(|source| {
        CompactionError::Io {
            operation: CompactionOperation::PublishReplacement,
            path: prepared.capture.source_dir.clone(),
            source,
        }
    })?;
    synchronize_publication_parent(
        &prepared.capture.source_dir,
        manifest.durability,
        CompactionOperation::PublishReplacement,
    )?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::ReplacementPublished,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::ReplacementPublish,
        },
    );
    checkpoint(ClosedReplacementStage::ReplacementMoved).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::PublishReplacement,
        path: prepared.capture.source_dir.clone(),
        source,
    })?;
    validate_published_closed_replacement(prepared)?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::ReplacementPublished,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::ReopenValidation,
        },
    );
    checkpoint(ClosedReplacementStage::ReplacementReopened).map_err(|source| {
        CompactionError::Io {
            operation: CompactionOperation::ReopenReplacement,
            path: prepared.capture.source_dir.clone(),
            source,
        }
    })?;
    let mut next = manifest.clone();
    next.phase = ManifestPhase::ReplacementPublished;
    publish_manifest_for_policy(&prepared.paths, &next, manifest.durability)?;
    *manifest = next;
    checkpoint(ClosedReplacementStage::PhasePublished).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::WriteManifest,
        path: prepared.paths.manifest.clone(),
        source,
    })?;
    Ok(())
}

pub(crate) fn publish_closed_prepared(
    prepared: &PreparedClosedStaging,
    durability: DurabilityPolicy,
) -> Result<CompactionManifest, CompactionError> {
    validate_closed_staging(prepared)?;
    revalidate_closed_source_inventory(prepared)?;
    let manifest = CompactionManifest {
        operation_id: next_operation_id(),
        mode: ManifestMode::ClosedDirectory,
        scope: ManifestScope::Directory,
        phase: ManifestPhase::Prepared,
        source_finalized: true,
        durability,
        source_inventory: prepared.capture.inventory.clone(),
        staging_location: native_leaf(&prepared.paths.staging)?,
        previous_location: native_leaf(&prepared.paths.previous)?,
        replacement_inventory: prepared.replacement_inventory.clone(),
    };
    publish_manifest_for_policy(&prepared.paths, &manifest, durability)?;
    Ok(manifest)
}

pub(crate) fn publish_closed_previous_with_checkpoint(
    prepared: &PreparedClosedStaging,
    manifest: &mut CompactionManifest,
    mut checkpoint: impl FnMut(ClosedPreviousStage) -> io::Result<()>,
) -> Result<(), CompactionError> {
    if manifest.phase != ManifestPhase::Prepared || !manifest.source_finalized {
        return Err(CompactionError::FailedClosed {
            detail: "old-to-previous publication requires finalized Prepared authority".to_owned(),
        });
    }
    revalidate_closed_source_inventory(prepared)?;
    match fs::symlink_metadata(&prepared.paths.previous) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::PublishPrevious,
                path: prepared.paths.previous.clone(),
                source,
            });
        }
        Ok(_) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::PublishPrevious,
                path: prepared.paths.previous.clone(),
                source: io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "previous generation path already exists",
                ),
            });
        }
    }
    fs::rename(&prepared.capture.source_dir, &prepared.paths.previous).map_err(|source| {
        CompactionError::Io {
            operation: CompactionOperation::PublishPrevious,
            path: prepared.paths.previous.clone(),
            source,
        }
    })?;
    synchronize_publication_parent(
        &prepared.paths.previous,
        manifest.durability,
        CompactionOperation::PublishPrevious,
    )?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::PreviousPublished,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::PreviousPublish,
        },
    );
    checkpoint(ClosedPreviousStage::SourceMoved).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::PublishPrevious,
        path: prepared.paths.previous.clone(),
        source,
    })?;
    let mut next = manifest.clone();
    next.phase = ManifestPhase::PreviousPublished;
    publish_manifest_for_policy(&prepared.paths, &next, manifest.durability)?;
    *manifest = next;
    checkpoint(ClosedPreviousStage::PhasePublished).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::WriteManifest,
        path: prepared.paths.manifest.clone(),
        source,
    })?;
    Ok(())
}

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

fn next_operation_id() -> [u8; 16] {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or_default();
    let sequence = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&time.to_le_bytes());
    id[8..].copy_from_slice(&sequence.to_le_bytes());
    id
}

fn native_leaf(path: &Path) -> Result<PathBuf, CompactionError> {
    path.file_name()
        .map(PathBuf::from)
        .ok_or_else(|| CompactionError::Io {
            operation: CompactionOperation::WriteManifest,
            path: path.to_path_buf(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "maintenance artifact has no native file name",
            ),
        })
}

pub(crate) fn publish_manifest_for_policy(
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
    durability: DurabilityPolicy,
) -> Result<(), CompactionError> {
    publish_manifest_with_checkpoint(paths, manifest, durability, |_| Ok(())).map_err(|source| {
        CompactionError::Io {
            operation: CompactionOperation::WriteManifest,
            path: paths.manifest.clone(),
            source,
        }
    })
}

pub(crate) fn directory_artifact_paths(store_dir: &Path) -> io::Result<MaintenanceArtifactPaths> {
    artifact_paths(store_dir, true)
}

pub(crate) fn family_artifact_paths(active_path: &Path) -> io::Result<MaintenanceArtifactPaths> {
    artifact_paths(active_path, false)
}

fn artifact_paths(base: &Path, hidden: bool) -> io::Result<MaintenanceArtifactPaths> {
    let parent = base.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "maintenance scope has no parent directory",
        )
    })?;
    let base_name = base.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "maintenance scope has no native file name",
        )
    })?;
    let sibling = |suffix: &str| {
        let mut name = OsString::new();
        if hidden {
            name.push(".");
        }
        name.push(base_name);
        name.push(".pigment-compact.");
        name.push(suffix);
        parent.join(name)
    };
    Ok(MaintenanceArtifactPaths {
        manifest: sibling("manifest"),
        manifest_next: sibling("manifest.next"),
        staging: sibling("next"),
        previous: sibling("previous"),
    })
}

pub(crate) fn publish_manifest_buffered(
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> io::Result<()> {
    publish_manifest_with_checkpoint(paths, manifest, DurabilityPolicy::Buffered, |_| Ok(()))
}

pub(crate) fn publish_manifest_buffered_with_checkpoint(
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
    checkpoint: impl FnMut(ManifestPublishStage) -> io::Result<()>,
) -> io::Result<()> {
    publish_manifest_with_checkpoint(paths, manifest, DurabilityPolicy::Buffered, checkpoint)
}

fn publish_manifest_with_checkpoint(
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
    durability: DurabilityPolicy,
    mut checkpoint: impl FnMut(ManifestPublishStage) -> io::Result<()>,
) -> io::Result<()> {
    let encoded = encode_manifest(manifest)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))?;
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.manifest_next)?;
    checkpoint(ManifestPublishStage::Created)?;
    temporary.write_all(&encoded)?;
    checkpoint(ManifestPublishStage::Written)?;
    #[cfg(test)]
    exit_at_manifest_fault(
        manifest,
        crate::test_support::fault_checkpoint::MaintenanceCut::ManifestWrite,
    );
    temporary.flush()?;
    if durability == DurabilityPolicy::Physical {
        temporary.sync_all()?;
    }
    checkpoint(ManifestPublishStage::Flushed)?;
    #[cfg(test)]
    exit_at_manifest_fault(
        manifest,
        crate::test_support::fault_checkpoint::MaintenanceCut::ManifestSync,
    );
    drop(temporary);
    fs::rename(&paths.manifest_next, &paths.manifest)?;
    if durability == DurabilityPolicy::Physical {
        let parent = paths.manifest.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "compaction manifest has no parent directory",
            )
        })?;
        crate::durability::synchronize_directory(parent)?;
    }
    checkpoint(ManifestPublishStage::Renamed)?;
    #[cfg(test)]
    exit_at_manifest_fault(
        manifest,
        crate::test_support::fault_checkpoint::MaintenanceCut::ManifestPublish,
    );
    Ok(())
}

fn synchronize_publication_parent(
    published: &Path,
    durability: DurabilityPolicy,
    operation: CompactionOperation,
) -> Result<(), CompactionError> {
    if durability == DurabilityPolicy::Buffered {
        return Ok(());
    }
    let parent = published.parent().ok_or_else(|| CompactionError::Io {
        operation,
        path: published.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            "published artifact has no parent directory",
        ),
    })?;
    crate::durability::synchronize_directory(parent).map_err(|source| CompactionError::Io {
        operation,
        path: parent.to_path_buf(),
        source,
    })
}

#[cfg(test)]
fn exit_at_manifest_fault(
    manifest: &CompactionManifest,
    cut: crate::test_support::fault_checkpoint::MaintenanceCut,
) {
    use crate::test_support::fault_checkpoint::{
        exit_at_maintenance_fault, MaintenanceFaultPoint, MaintenancePhase,
    };

    let phase = match manifest.phase {
        ManifestPhase::Prepared => MaintenancePhase::Prepared,
        ManifestPhase::PreviousPublished => MaintenancePhase::PreviousPublished,
        ManifestPhase::ReplacementPublished => MaintenancePhase::ReplacementPublished,
        ManifestPhase::CleanupPending => MaintenancePhase::CleanupPending,
    };
    exit_at_maintenance_fault(MaintenanceFaultPoint { phase, cut });
}

pub(crate) fn read_published_manifest(
    paths: &MaintenanceArtifactPaths,
) -> io::Result<Option<CompactionManifest>> {
    let encoded = match fs::read(&paths.manifest) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    decode_manifest(&encoded)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))
}

#[cfg(test)]
pub(crate) fn test_sentinel() {}
