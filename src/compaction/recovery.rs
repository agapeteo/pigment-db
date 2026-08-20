//! Interrupted-compaction recovery internals.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::manifest::{
    verify_descriptor, ArtifactDescriptor, CompactionManifest, ManifestMode, ManifestPhase,
    ManifestScope,
};
use super::publication::{
    directory_artifact_paths, publish_manifest_for_policy, read_published_manifest,
    MaintenanceArtifactPaths,
};
use crate::wal::replay::{
    classify_key_map_read_only, classify_key_set_read_only, classify_key_value_read_only,
};
use crate::{CompactionError, CompactionOperation, RecoveryError, RecoveryOperation, StoreFamily};

pub(crate) fn resolve_directory_maintenance(store_dir: &Path) -> Result<bool, RecoveryError> {
    resolve_directory_maintenance_for_compaction(store_dir)
        .map_err(|error| map_compaction_recovery_error(store_dir, error))
}

pub(crate) fn resolve_directory_maintenance_for_compaction(
    store_dir: &Path,
) -> Result<bool, CompactionError> {
    let paths = directory_artifact_paths(store_dir).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Inspect,
        path: store_dir.to_path_buf(),
        source,
    })?;
    let mut manifest = match read_published_manifest(&paths) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            return classify_untrusted_closed_authority(store_dir, &paths).map(|()| false);
        }
        Err(_) => {
            return classify_untrusted_closed_authority(store_dir, &paths).map(|()| false);
        }
    };
    remove_unpublished_manifest_temp(&paths)?;
    validate_directory_manifest_binding(store_dir, &paths, &manifest)?;

    loop {
        match manifest.phase {
            ManifestPhase::Prepared => {
                recover_prepared_closed(store_dir, &paths, &manifest)
                    .and_then(|()| finish_prepared_abort(store_dir, &paths, &manifest))?;
                return Ok(true);
            }
            ManifestPhase::PreviousPublished => {
                let authority =
                    recover_previous_published_closed(store_dir, &paths, &mut manifest)?;
                if authority == RecoveredAuthority::Previous {
                    return Ok(true);
                }
            }
            ManifestPhase::ReplacementPublished => {
                recover_replacement_published_closed(store_dir, &paths, &mut manifest)?;
            }
            ManifestPhase::CleanupPending => {
                let _ = recover_cleanup_pending_closed(store_dir, &paths, &manifest)?;
                return Ok(true);
            }
        }
    }
}

fn remove_unpublished_manifest_temp(
    paths: &MaintenanceArtifactPaths,
) -> Result<(), CompactionError> {
    let metadata = match fs::symlink_metadata(&paths.manifest_next) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.manifest_next.clone(),
                source,
            });
        }
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(CompactionError::InvalidArtifact {
            path: paths.manifest_next.clone(),
        });
    }
    fs::remove_file(&paths.manifest_next).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Cleanup,
        path: paths.manifest_next.clone(),
        source,
    })
}

fn validate_directory_manifest_binding(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    let expected_staging = paths.staging.file_name().map(PathBuf::from);
    let expected_previous = paths.previous.file_name().map(PathBuf::from);
    let source_name = store_dir.file_name();
    let paths_bound = expected_staging.as_ref() == Some(&manifest.staging_location)
        && expected_previous.as_ref() == Some(&manifest.previous_location)
        && manifest.source_inventory.iter().all(|descriptor| {
            descriptor.relative_path.components().next().is_some_and(|component| {
                matches!(component, std::path::Component::Normal(name) if Some(name) == source_name)
            })
        })
        && manifest.replacement_inventory.iter().all(|descriptor| {
            descriptor.relative_path.components().next().is_some_and(|component| {
                matches!(component, std::path::Component::Normal(name) if Some(name) == paths.staging.file_name())
            })
        });
    if manifest.mode != ManifestMode::ClosedDirectory
        || manifest.scope != ManifestScope::Directory
        || !manifest.source_finalized
        || !paths_bound
    {
        return Err(CompactionError::InvalidArtifact {
            path: paths.manifest.clone(),
        });
    }
    Ok(())
}

fn finish_prepared_abort(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    if path_exists(&paths.staging)? {
        if !generation_matches(&paths.staging, &manifest.replacement_inventory) {
            return Err(authority_undetermined(store_dir, paths));
        }
        fs::remove_dir_all(&paths.staging).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.staging.clone(),
            source,
        })?;
    }
    match fs::remove_file(&paths.manifest) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.manifest.clone(),
            source,
        }),
    }
}

fn map_compaction_recovery_error(store_dir: &Path, error: CompactionError) -> RecoveryError {
    match error {
        CompactionError::MigrationRequired { path } => RecoveryError::MigrationRequired { path },
        CompactionError::InvalidArtifact { path } => RecoveryError::InvalidArtifact { path },
        CompactionError::AuthorityUndetermined { paths } => RecoveryError::AuthorityUndetermined {
            active_path: paths
                .iter()
                .find(|path| path.as_path() == store_dir)
                .cloned(),
            recovery_path: paths.into_iter().find(|path| path.as_path() != store_dir),
        },
        CompactionError::UnsupportedDurability { source } => {
            RecoveryError::UnsupportedDurability { source }
        }
        CompactionError::Io { path, source, .. } => RecoveryError::Io {
            operation: RecoveryOperation::Inspect,
            path,
            source,
        },
        CompactionError::FailedClosed { detail } => RecoveryError::Io {
            operation: RecoveryOperation::Inspect,
            path: store_dir.to_path_buf(),
            source: io::Error::other(detail),
        },
        CompactionError::ConcurrentDeltaLimitExceeded { limit } => RecoveryError::Io {
            operation: RecoveryOperation::Inspect,
            path: store_dir.to_path_buf(),
            source: io::Error::other(format!("unexpected recovery delta limit {limit}")),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveredAuthority {
    Previous,
    Replacement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryCleanupStage {
    Artifact(usize),
    Directory,
    Manifest,
}

pub(crate) fn classify_untrusted_closed_authority(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
) -> Result<(), CompactionError> {
    let canonical = evidence_state(store_dir, true)?;
    let staging = evidence_state(&paths.staging, false)?;
    let previous = evidence_state(&paths.previous, false)?;
    let manifest_state = match read_published_manifest(paths) {
        Ok(Some(_)) => EvidenceState::Complete,
        Ok(None) => EvidenceState::Missing,
        Err(_) => EvidenceState::Invalid,
    };
    let manifest_next = if path_exists(&paths.manifest_next)? {
        EvidenceState::Invalid
    } else {
        EvidenceState::Missing
    };

    if staging == EvidenceState::Missing
        && previous == EvidenceState::Missing
        && manifest_state == EvidenceState::Missing
        && manifest_next == EvidenceState::Missing
    {
        return Ok(());
    }

    let complete_siblings = [(&paths.staging, staging), (&paths.previous, previous)]
        .into_iter()
        .filter(|(_, state)| *state == EvidenceState::Complete)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    if !complete_siblings.is_empty() {
        let mut evidence = Vec::new();
        if canonical != EvidenceState::Missing {
            evidence.push(store_dir.to_path_buf());
        }
        evidence.extend(complete_siblings);
        if manifest_state != EvidenceState::Missing {
            evidence.push(paths.manifest.clone());
        }
        return Err(CompactionError::AuthorityUndetermined { paths: evidence });
    }

    if canonical == EvidenceState::Invalid {
        return Err(CompactionError::InvalidArtifact {
            path: store_dir.to_path_buf(),
        });
    }
    for (path, state) in [
        (&paths.staging, staging),
        (&paths.previous, previous),
        (&paths.manifest_next, manifest_next),
        (&paths.manifest, manifest_state),
    ] {
        if state == EvidenceState::Invalid {
            return Err(CompactionError::InvalidArtifact { path: path.clone() });
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvidenceState {
    Missing,
    Complete,
    Invalid,
}

fn evidence_state(path: &Path, allow_empty: bool) -> Result<EvidenceState, CompactionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(EvidenceState::Missing);
        }
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::Inspect,
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(EvidenceState::Invalid);
    }
    match super::inspection::inspect_generation(path) {
        Ok(generation) if allow_empty || !generation.families.is_empty() => {
            Ok(EvidenceState::Complete)
        }
        _ => Ok(EvidenceState::Invalid),
    }
}

pub(crate) fn recover_cleanup_pending_closed(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<crate::CleanupStatus, CompactionError> {
    recover_cleanup_pending_closed_with_checkpoint(store_dir, paths, manifest, |_| Ok(()))
}

pub(crate) fn recover_cleanup_pending_closed_with_checkpoint(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
    mut checkpoint: impl FnMut(RecoveryCleanupStage) -> io::Result<()>,
) -> Result<crate::CleanupStatus, CompactionError> {
    if manifest.phase != ManifestPhase::CleanupPending
        || manifest.mode != ManifestMode::ClosedDirectory
        || manifest.scope != ManifestScope::Directory
        || !manifest.source_finalized
    {
        return Err(CompactionError::FailedClosed {
            detail: "closed CleanupPending recovery received contradictory manifest state"
                .to_owned(),
        });
    }
    if !path_exists(store_dir)?
        || !generation_matches(store_dir, &manifest.replacement_inventory)
        || path_exists(&paths.staging)?
    {
        return Err(authority_undetermined(store_dir, paths));
    }
    if !path_exists(&paths.previous)? {
        return remove_manifest_last(paths, &mut checkpoint);
    }
    let metadata = fs::symlink_metadata(&paths.previous).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Cleanup,
        path: paths.previous.clone(),
        source,
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(crate::CleanupStatus::Pending);
    }
    let Some(parent) = paths.previous.parent() else {
        return Ok(crate::CleanupStatus::Pending);
    };
    let Some(previous_name) = paths.previous.file_name() else {
        return Ok(crate::CleanupStatus::Pending);
    };
    let mut by_name = std::collections::BTreeMap::new();
    for source in &manifest.source_inventory {
        let Some(file_name) = source.relative_path.file_name() else {
            return Ok(crate::CleanupStatus::Pending);
        };
        if by_name.insert(OsString::from(file_name), source).is_some() {
            return Ok(crate::CleanupStatus::Pending);
        }
    }
    let entries = match fs::read_dir(&paths.previous) {
        Ok(entries) => entries,
        Err(_) => return Ok(crate::CleanupStatus::Pending),
    };
    let mut remaining = Vec::new();
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
        let Some(source) = by_name.get(&entry.file_name()) else {
            return Ok(crate::CleanupStatus::Pending);
        };
        let mut translated = (*source).clone();
        translated.relative_path = PathBuf::from(previous_name).join(entry.file_name());
        if verify_descriptor(parent, &translated).is_err() {
            return Ok(crate::CleanupStatus::Pending);
        }
        remaining.push(entry.path());
    }
    remaining.sort();
    for (index, path) in remaining.iter().enumerate() {
        if checkpoint(RecoveryCleanupStage::Artifact(index)).is_err()
            || fs::remove_file(path).is_err()
        {
            return Ok(crate::CleanupStatus::Pending);
        }
    }
    if checkpoint(RecoveryCleanupStage::Directory).is_err()
        || fs::remove_dir(&paths.previous).is_err()
    {
        return Ok(crate::CleanupStatus::Pending);
    }
    remove_manifest_last(paths, &mut checkpoint)
}

fn remove_manifest_last(
    paths: &MaintenanceArtifactPaths,
    checkpoint: &mut impl FnMut(RecoveryCleanupStage) -> io::Result<()>,
) -> Result<crate::CleanupStatus, CompactionError> {
    if checkpoint(RecoveryCleanupStage::Manifest).is_err() {
        return Ok(crate::CleanupStatus::Pending);
    }
    match fs::remove_file(&paths.manifest) {
        Ok(()) => Ok(crate::CleanupStatus::Complete),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(crate::CleanupStatus::Complete),
        Err(_) => Ok(crate::CleanupStatus::Pending),
    }
}

pub(crate) fn recover_previous_published_closed(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &mut CompactionManifest,
) -> Result<RecoveredAuthority, CompactionError> {
    if manifest.phase != ManifestPhase::PreviousPublished
        || manifest.mode != ManifestMode::ClosedDirectory
        || manifest.scope != ManifestScope::Directory
        || !manifest.source_finalized
    {
        return Err(CompactionError::FailedClosed {
            detail: "closed PreviousPublished recovery received contradictory manifest state"
                .to_owned(),
        });
    }
    let canonical_exists = path_exists(store_dir)?;
    let staging_exists = path_exists(&paths.staging)?;
    let previous_exists = path_exists(&paths.previous)?;
    let canonical_replacement =
        canonical_exists && generation_matches(store_dir, &manifest.replacement_inventory);
    let staged_replacement =
        staging_exists && generation_matches(&paths.staging, &manifest.replacement_inventory);
    let verified_previous =
        previous_exists && generation_matches(&paths.previous, &manifest.source_inventory);

    if canonical_replacement && !staging_exists {
        establish_replacement_phase(paths, manifest)?;
        return Ok(RecoveredAuthority::Replacement);
    }
    if !canonical_exists && staged_replacement {
        fs::rename(&paths.staging, store_dir).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::PublishReplacement,
            path: store_dir.to_path_buf(),
            source,
        })?;
        if !generation_matches(store_dir, &manifest.replacement_inventory) {
            return Err(authority_undetermined(store_dir, paths));
        }
        establish_replacement_phase(paths, manifest)?;
        return Ok(RecoveredAuthority::Replacement);
    }
    if verified_previous {
        if canonical_exists {
            if staging_exists {
                return Err(authority_undetermined(store_dir, paths));
            }
            fs::rename(store_dir, &paths.staging).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::PublishReplacement,
                path: paths.staging.clone(),
                source,
            })?;
        }
        fs::rename(&paths.previous, store_dir).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::PublishPrevious,
            path: store_dir.to_path_buf(),
            source,
        })?;
        if !generation_matches(store_dir, &manifest.source_inventory) {
            return Err(authority_undetermined(store_dir, paths));
        }
        if path_exists(&paths.staging)? {
            let metadata =
                fs::symlink_metadata(&paths.staging).map_err(|source| CompactionError::Io {
                    operation: CompactionOperation::Cleanup,
                    path: paths.staging.clone(),
                    source,
                })?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(authority_undetermined(store_dir, paths));
            }
            fs::remove_dir_all(&paths.staging).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.staging.clone(),
                source,
            })?;
        }
        fs::remove_file(&paths.manifest).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.manifest.clone(),
            source,
        })?;
        return Ok(RecoveredAuthority::Previous);
    }
    Err(authority_undetermined(store_dir, paths))
}

pub(crate) fn recover_replacement_published_closed(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &mut CompactionManifest,
) -> Result<(), CompactionError> {
    if manifest.phase != ManifestPhase::ReplacementPublished
        || manifest.mode != ManifestMode::ClosedDirectory
        || manifest.scope != ManifestScope::Directory
        || !manifest.source_finalized
    {
        return Err(CompactionError::FailedClosed {
            detail: "closed ReplacementPublished recovery received contradictory manifest state"
                .to_owned(),
        });
    }
    let canonical_valid =
        path_exists(store_dir)? && generation_matches(store_dir, &manifest.replacement_inventory);
    let previous_valid = path_exists(&paths.previous)?
        && generation_matches(&paths.previous, &manifest.source_inventory);
    if !canonical_valid || !previous_valid || path_exists(&paths.staging)? {
        return Err(authority_undetermined(store_dir, paths));
    }
    let mut next = manifest.clone();
    next.phase = ManifestPhase::CleanupPending;
    publish_manifest_for_policy(paths, &next, manifest.durability)?;
    *manifest = next;
    Ok(())
}

fn establish_replacement_phase(
    paths: &MaintenanceArtifactPaths,
    manifest: &mut CompactionManifest,
) -> Result<(), CompactionError> {
    let mut next = manifest.clone();
    next.phase = ManifestPhase::ReplacementPublished;
    publish_manifest_for_policy(paths, &next, manifest.durability)?;
    *manifest = next;
    Ok(())
}

fn authority_undetermined(store_dir: &Path, paths: &MaintenanceArtifactPaths) -> CompactionError {
    CompactionError::AuthorityUndetermined {
        paths: vec![
            store_dir.to_path_buf(),
            paths.staging.clone(),
            paths.previous.clone(),
            paths.manifest.clone(),
        ],
    }
}

pub(crate) fn recover_prepared_closed(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    if manifest.phase != ManifestPhase::Prepared
        || manifest.mode != ManifestMode::ClosedDirectory
        || manifest.scope != ManifestScope::Directory
        || !manifest.source_finalized
    {
        return Err(CompactionError::FailedClosed {
            detail: "closed Prepared recovery received contradictory manifest state".to_owned(),
        });
    }

    let canonical_exists = path_exists(store_dir)?;
    let previous_exists = path_exists(&paths.previous)?;
    let canonical_valid =
        canonical_exists && generation_matches(store_dir, &manifest.source_inventory);
    let previous_valid =
        previous_exists && generation_matches(&paths.previous, &manifest.source_inventory);
    match (
        canonical_exists,
        canonical_valid,
        previous_exists,
        previous_valid,
    ) {
        (true, true, false, _) => {}
        (false, _, true, true) => {
            fs::rename(&paths.previous, store_dir).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::PublishPrevious,
                path: store_dir.to_path_buf(),
                source,
            })?;
        }
        _ => {
            let mut evidence = vec![store_dir.to_path_buf(), paths.previous.clone()];
            if path_exists(&paths.staging).unwrap_or(true) {
                evidence.push(paths.staging.clone());
            }
            return Err(CompactionError::AuthorityUndetermined { paths: evidence });
        }
    }

    if path_exists(&paths.staging)?
        && !generation_matches(&paths.staging, &manifest.replacement_inventory)
    {
        let metadata =
            fs::symlink_metadata(&paths.staging).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.staging.clone(),
                source,
            })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(CompactionError::AuthorityUndetermined {
                paths: vec![store_dir.to_path_buf(), paths.staging.clone()],
            });
        }
        fs::remove_dir_all(&paths.staging).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.staging.clone(),
            source,
        })?;
    }
    Ok(())
}

pub(crate) fn source_descriptors_match(anchor: &Path, manifest: &CompactionManifest) -> bool {
    let prefix_mode = manifest.phase == ManifestPhase::Prepared
        && manifest.mode == ManifestMode::OnlineFamily
        && !manifest.source_finalized;
    manifest.source_inventory.iter().all(|descriptor| {
        let path = anchor.join(&descriptor.relative_path);
        if !prefix_mode {
            return verify_descriptor(anchor, descriptor).is_ok();
        }
        let Ok(bytes) = fs::read(path) else {
            return false;
        };
        let Ok(length) = usize::try_from(descriptor.length) else {
            return false;
        };
        let Some(prefix) = bytes.get(..length) else {
            return false;
        };
        if crc32fast::hash(prefix) != descriptor.checksum {
            return false;
        }
        match descriptor.family {
            Some(StoreFamily::KeyValue) => classify_key_value_read_only(&bytes).is_ok(),
            Some(StoreFamily::KeySet) => classify_key_set_read_only(&bytes).is_ok(),
            Some(StoreFamily::KeyMap) => classify_key_map_read_only(&bytes).is_ok(),
            None => false,
        }
    })
}

fn path_exists(path: &Path) -> Result<bool, CompactionError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(CompactionError::Io {
            operation: CompactionOperation::Inspect,
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn generation_matches(location: &Path, descriptors: &[ArtifactDescriptor]) -> bool {
    let Ok(metadata) = fs::symlink_metadata(location) else {
        return false;
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return false;
    }
    let Some(parent) = location.parent() else {
        return false;
    };
    let Some(location_name) = location.file_name() else {
        return false;
    };
    let mut expected_names = BTreeSet::new();
    for source in descriptors {
        let Some(file_name) = source.relative_path.file_name() else {
            return false;
        };
        if !expected_names.insert(OsString::from(file_name)) {
            return false;
        }
        let mut translated = source.clone();
        translated.relative_path = PathBuf::from(location_name).join(file_name);
        if verify_descriptor(parent, &translated).is_err() {
            return false;
        }
    }
    let Ok(entries) = fs::read_dir(location) else {
        return false;
    };
    let mut actual_names = BTreeSet::new();
    for entry in entries {
        let Ok(entry) = entry else {
            return false;
        };
        let Ok(file_type) = entry.file_type() else {
            return false;
        };
        if !file_type.is_file() {
            return false;
        }
        actual_names.insert(entry.file_name());
    }
    actual_names == expected_names
}

#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct UntrustedMaintenanceEvidence {
    pub(crate) complete_generations: Vec<PathBuf>,
    pub(crate) invalid_generations: Vec<PathBuf>,
}

fn sibling_maintenance_path(store_dir: &Path, suffix: &str) -> io::Result<PathBuf> {
    let parent = store_dir.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "store directory has no parent for maintenance evidence",
        )
    })?;
    let leaf = store_dir.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "store directory has no leaf name for maintenance evidence",
        )
    })?;
    let mut name = OsString::from(".");
    name.push(leaf);
    name.push(".pigment-compact.");
    name.push(suffix);
    Ok(parent.join(name))
}

pub(crate) fn classify_untrusted_directory_generations(
    store_dir: &Path,
    mut generation_is_complete: impl FnMut(&Path) -> bool,
) -> io::Result<UntrustedMaintenanceEvidence> {
    let mut evidence = UntrustedMaintenanceEvidence::default();
    for suffix in ["next", "previous"] {
        let path = sibling_maintenance_path(store_dir, suffix)?;
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_dir() && generation_is_complete(&path) {
            evidence.complete_generations.push(path);
        } else {
            evidence.invalid_generations.push(path);
        }
    }
    Ok(evidence)
}

#[cfg(test)]
pub(crate) fn test_sentinel() {}
