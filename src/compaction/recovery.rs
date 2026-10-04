//! Interrupted-compaction recovery internals.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use super::manifest::{
    verify_descriptor, ArtifactDescriptor, ArtifactRole, CompactionManifest, ManifestMode,
    ManifestPhase, ManifestScope,
};
use super::publication::{
    directory_artifact_paths, publish_manifest_for_policy, read_published_manifest,
    MaintenanceArtifactPaths,
};
use crate::maintenance_coordination::FamilyWriters;
use crate::{CompactionError, CompactionOperation, RecoveryError, RecoveryOperation, StoreFamily};

/// Recovers the directory's closed maintenance. `locked` is the directory the caller's open lease
/// or closed claim locked (its identity, specs/011): the closed discard acts only there.
pub(crate) fn resolve_directory_maintenance(
    store_dir: &Path,
    locked: &Path,
) -> Result<bool, RecoveryError> {
    resolve_directory_maintenance_for_compaction(store_dir, locked)
        .map_err(|error| map_compaction_recovery_error(store_dir, error))
}

/// Recovers an open's directory-level maintenance, then its family's online maintenance.
/// `family_writers` says, once directory recovery is done, whether the open is the only live
/// writer of its family (specs/016 FR-2): only then does family recovery recover what a dead
/// online attempt left.
pub(crate) fn resolve_store_maintenance<'a>(
    store_dir: &Path,
    locked: &Path,
    family: super::inspection::InspectedFamily,
    family_writers: impl FnOnce() -> FamilyWriters<'a>,
) -> Result<bool, RecoveryError> {
    match fs::metadata(store_dir) {
        Ok(metadata) if !metadata.is_dir() => return Ok(false),
        _ => {}
    }
    let directory_recovered = resolve_directory_maintenance(store_dir, locked)?;
    #[cfg(test)]
    recovery_pause::reached(store_dir, recovery_pause::Point::FamilyRecovery);
    let dead_attempts = match family_writers() {
        FamilyWriters::OnlyThis { locked } => DeadAttempts::Recover { locked },
        FamilyWriters::NotExcluded => DeadAttempts::KeepTheirErrors,
    };
    let online_recovered = resolve_online_maintenance(store_dir, family, dead_attempts)
        .map_err(|error| map_compaction_recovery_error(store_dir, error))?;
    Ok(directory_recovered || online_recovered)
}

/// What family recovery does with the two states only a killed online attempt leaves -- a lone
/// manifest temporary, and a finalized `Prepared` whose source the cutover's moves split
/// (specs/016 FR-2).
#[derive(Clone, Copy, Debug)]
enum DeadAttempts<'a> {
    /// Recovers them, acting only in `locked`, the directory the open locked: the open is the
    /// only live writer of the family there, so no live attempt can own them.
    Recover { locked: &'a Path },
    /// Leaves them with the errors they returned at `1eb9de5`: a live online attempt of another
    /// instance of the family writes exactly that temporary and leaves exactly that split, and
    /// nothing here excludes one.
    KeepTheirErrors,
}

/// Recovers a family's online maintenance at the start of an online compaction. specs/016
/// recovers a dead attempt's leftovers at an open, not here (its plan, Decisions): they keep
/// their errors.
pub(crate) fn resolve_online_maintenance_for_compaction(
    store_dir: &Path,
    family: super::inspection::InspectedFamily,
) -> Result<bool, CompactionError> {
    resolve_online_maintenance(store_dir, family, DeadAttempts::KeepTheirErrors)
}

fn resolve_online_maintenance(
    store_dir: &Path,
    family: super::inspection::InspectedFamily,
    dead_attempts: DeadAttempts<'_>,
) -> Result<bool, CompactionError> {
    let paths = super::publication::family_artifact_paths(&store_dir.join(family.active_name()))
        .map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Inspect,
            path: store_dir.to_path_buf(),
            source,
        })?;
    let mut manifest = match read_published_manifest(&paths) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            if let DeadAttempts::Recover { locked } = dead_attempts {
                if discard_dead_first_publication(store_dir, locked, &paths, family)? {
                    return Ok(true);
                }
            }
            if [&paths.manifest_next, &paths.staging, &paths.previous]
                .into_iter()
                .any(|path| path_exists(path).unwrap_or(true))
            {
                return Err(authority_undetermined(store_dir, &paths));
            }
            return Ok(false);
        }
        Err(_) => return Err(authority_undetermined(store_dir, &paths)),
    };
    remove_unpublished_manifest_temp(&paths)?;
    validate_online_manifest_binding(store_dir, &paths, &manifest, family)?;
    match manifest.phase {
        ManifestPhase::Prepared => {
            if let (true, DeadAttempts::Recover { locked }) =
                (manifest.source_finalized, dead_attempts)
            {
                restore_split_online_source(store_dir, locked, &manifest)?;
            }
            recover_prepared_online(store_dir, &paths, &manifest)?;
        }
        ManifestPhase::PreviousPublished => {
            recover_previous_published_online(store_dir, &paths, &mut manifest)?;
            if recover_online_cleanup_with_checkpoint(store_dir, &paths, &mut manifest, |_| Ok(()))?
                == crate::CleanupStatus::Pending
            {
                return Err(CompactionError::FailedClosed {
                    detail: "online cleanup remains pending; retry after the filesystem permits exact cleanup"
                        .to_owned(),
                });
            }
        }
        ManifestPhase::ReplacementPublished | ManifestPhase::CleanupPending => {
            if recover_online_cleanup_with_checkpoint(store_dir, &paths, &mut manifest, |_| Ok(()))?
                == crate::CleanupStatus::Pending
            {
                return Err(CompactionError::FailedClosed {
                    detail: "online cleanup remains pending; retry after the filesystem permits exact cleanup"
                        .to_owned(),
                });
            }
        }
    }
    Ok(true)
}

fn recover_previous_published_online(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &mut CompactionManifest,
) -> Result<(), CompactionError> {
    if manifest.phase != ManifestPhase::PreviousPublished
        || manifest.mode != ManifestMode::OnlineFamily
        || !manifest.source_finalized
        || !complete_online_previous_matches(store_dir, paths, manifest)?
    {
        return Err(authority_undetermined(store_dir, paths));
    }
    let ManifestScope::Family { active_name, .. } = &manifest.scope else {
        return Err(authority_undetermined(store_dir, paths));
    };
    let active_path = store_dir.join(active_name);
    let staging_exists = path_exists(&paths.staging)?;
    let active_exists = path_exists(&active_path)?;
    match (staging_exists, active_exists) {
        (true, false) if online_staging_matches(store_dir, paths, manifest) => {
            fs::rename(&paths.staging, &active_path).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::PublishReplacement,
                path: active_path.clone(),
                source,
            })?;
            if manifest.durability == crate::DurabilityPolicy::Physical {
                crate::durability::synchronize_directory(store_dir).map_err(|source| {
                    CompactionError::Io {
                        operation: CompactionOperation::PublishReplacement,
                        path: store_dir.to_path_buf(),
                        source,
                    }
                })?;
            }
        }
        (false, true) => {}
        _ => return Err(authority_undetermined(store_dir, paths)),
    }
    if !online_replacement_prefix_matches(store_dir, manifest) {
        return Err(authority_undetermined(store_dir, paths));
    }
    let mut next = manifest.clone();
    next.phase = ManifestPhase::ReplacementPublished;
    super::publication::publish_manifest_for_policy(paths, &next, manifest.durability)?;
    *manifest = next;
    Ok(())
}

pub(crate) fn recover_online_cleanup_with_checkpoint(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &mut CompactionManifest,
    mut checkpoint: impl FnMut(super::OnlineCleanupStage) -> io::Result<()>,
) -> Result<crate::CleanupStatus, CompactionError> {
    if manifest.mode != ManifestMode::OnlineFamily
        || !manifest.source_finalized
        || !matches!(
            manifest.phase,
            ManifestPhase::ReplacementPublished | ManifestPhase::CleanupPending
        )
    {
        return Err(CompactionError::FailedClosed {
            detail: "online cleanup requires confirmed replacement authority".to_owned(),
        });
    }
    if !online_replacement_prefix_matches(store_dir, manifest)
        || path_exists(&paths.staging)?
        || !remaining_online_previous_is_valid(store_dir, paths, manifest)?
    {
        return Err(authority_undetermined(store_dir, paths));
    }
    if manifest.phase == ManifestPhase::ReplacementPublished {
        let mut next = manifest.clone();
        next.phase = ManifestPhase::CleanupPending;
        if super::publication::publish_manifest_for_policy(paths, &next, manifest.durability)
            .is_err()
        {
            return Ok(crate::CleanupStatus::Pending);
        }
        *manifest = next;
    }
    if checkpoint(super::OnlineCleanupStage::CleanupPendingPublished).is_err() {
        return Ok(crate::CleanupStatus::Pending);
    }

    for (index, source) in manifest.source_inventory.iter().enumerate() {
        let Some(file_name) = source.relative_path.file_name() else {
            return Err(authority_undetermined(store_dir, paths));
        };
        let previous_path = paths.previous.join(file_name);
        if !path_exists(&previous_path)? {
            continue;
        }
        if checkpoint(super::OnlineCleanupStage::BeforePreviousArtifact(index)).is_err()
            || fs::remove_file(&previous_path).is_err()
        {
            return Ok(crate::CleanupStatus::Pending);
        }
    }
    if path_exists(&paths.previous)?
        && (checkpoint(super::OnlineCleanupStage::BeforePreviousDirectory).is_err()
            || fs::remove_dir(&paths.previous).is_err())
    {
        return Ok(crate::CleanupStatus::Pending);
    }
    if crate::durability::synchronize_namespace_parent(store_dir, manifest.durability).is_err() {
        return Ok(crate::CleanupStatus::Pending);
    }
    if checkpoint(super::OnlineCleanupStage::BeforeManifest).is_err() {
        return Ok(crate::CleanupStatus::Pending);
    }
    match fs::remove_file(&paths.manifest) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Ok(crate::CleanupStatus::Pending),
    }
    if crate::durability::synchronize_namespace_parent(store_dir, manifest.durability).is_err() {
        return Ok(crate::CleanupStatus::Pending);
    }
    Ok(crate::CleanupStatus::Complete)
}

pub(crate) fn resolve_directory_maintenance_for_compaction(
    store_dir: &Path,
    locked: &Path,
) -> Result<bool, CompactionError> {
    let paths = directory_artifact_paths(store_dir).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Inspect,
        path: store_dir.to_path_buf(),
        source,
    })?;
    let mut manifest = match read_published_manifest(&paths) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            if discard_unpublished_closed_attempt(store_dir, locked, &paths)? {
                return Ok(true);
            }
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

/// What is at a path, read without following a final symlink.
///
/// The specs/015 rule that removes unpublished-attempt debris proves its state before it removes
/// anything, and a path it cannot read proves nothing: `Unknown` makes it leave the state to the
/// checks that follow, which answer as before specs/015. Only a removal that fails after the proof
/// returns its I/O error (FR-7).
enum PathEntry {
    Absent,
    Present(fs::Metadata),
    Unknown,
}

impl PathEntry {
    fn read(path: &Path) -> Self {
        match fs::symlink_metadata(path) {
            Ok(metadata) => Self::Present(metadata),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self::Absent,
            Err(_) => Self::Unknown,
        }
    }

    fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }

    /// Absent, or present and of the kind `kind` accepts.
    fn is_absent_or(&self, kind: fn(&fs::Metadata) -> bool) -> bool {
        match self {
            Self::Absent => true,
            Self::Present(metadata) => kind(metadata),
            Self::Unknown => false,
        }
    }

    /// Present and of the kind `kind` accepts.
    fn is_present_and(&self, kind: fn(&fs::Metadata) -> bool) -> bool {
        match self {
            Self::Present(metadata) => kind(metadata),
            Self::Absent | Self::Unknown => false,
        }
    }
}

/// The names of `directory`'s entries when every one is a regular file; `None` when one is not,
/// or when the directory cannot be read.
fn regular_file_names(directory: &Path) -> Option<Vec<OsString>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(directory).ok()? {
        let entry = entry.ok()?;
        if !entry.file_type().ok()?.is_file() {
            return None;
        }
        names.push(entry.file_name());
    }
    Some(names)
}

fn is_real_directory(metadata: &fs::Metadata) -> bool {
    metadata.is_dir() && !metadata.file_type().is_symlink()
}

fn is_regular_file(metadata: &fs::Metadata) -> bool {
    metadata.is_file() && !metadata.file_type().is_symlink()
}

/// Removes what a closed compaction leaves when it stops before its `Prepared` manifest is
/// published (specs/015 FR-3), and reports whether it removed anything. The caller has read the
/// main manifest and found none; nothing at all may be at its path either (a symlink to nothing
/// is not an absent manifest). Publication moves the canonical directory only after `Prepared`
/// is durable, so with no main manifest and no previous generation a complete canonical
/// directory holding at least one family is the authority, and nothing names the staging. Only
/// what compaction itself writes is removed: a staging directory holding nothing but
/// active-segment files of families the canonical directory holds, and a regular
/// `.manifest.next`. And the staging is removed only when each staged file is byte for byte what
/// a closed compaction of the canonical directory stages for its family, or a write of those bytes
/// stopped inside the header or a record (specs/015's third, fourth and fifth reviews): a
/// canonical directory changed outside the exclusion -- truncated at a record boundary, or rolled
/// back -- still validates, and the staging may then be the only copy of what it lost, a delete
/// included.
///
/// Every path it reads or removes is resolved once, here, from the directory `locked` -- the one
/// the caller's open lease or closed claim locked -- and only while the caller's spelling still
/// names it. So the state it proves and the entries it removes are in the same directory, and in
/// the one whose locks are held, whatever the working directory or a symlink on the caller's path
/// does meanwhile (fourth review). Errors still name the caller's spelling.
///
/// Anything else -- a path it cannot read included -- leaves every path as it is, for the
/// classification that follows.
fn discard_unpublished_closed_attempt(
    store_dir: &Path,
    locked: &Path,
    paths: &MaintenanceArtifactPaths,
) -> Result<bool, CompactionError> {
    #[cfg(test)]
    recovery_pause::reached(store_dir, recovery_pause::Point::DiscardEntry);
    let Ok(directory) = fs::canonicalize(store_dir) else {
        return Ok(false);
    };
    if directory != locked {
        return Ok(false);
    }
    #[cfg(test)]
    recovery_pause::reached(&directory, recovery_pause::Point::DiscardIdentified);
    let Ok(at) = directory_artifact_paths(&directory) else {
        return Ok(false);
    };
    let staging = PathEntry::read(&at.staging);
    let manifest_next = PathEntry::read(&at.manifest_next);
    if staging.is_absent() && manifest_next.is_absent() {
        return Ok(false);
    }
    if !PathEntry::read(&at.manifest).is_absent()
        || !PathEntry::read(&at.previous).is_absent()
        || !manifest_next.is_absent_or(is_regular_file)
        || !staging.is_absent_or(is_real_directory)
    {
        return Ok(false);
    }
    let staged_names = if staging.is_absent() {
        Vec::new()
    } else {
        let Some(names) = regular_file_names(&at.staging) else {
            return Ok(false);
        };
        names
    };
    // The names first, before the canonical directory is validated: each must be an active
    // segment's with a regular file of that name in the canonical directory.
    if !staged_names.iter().all(|name| {
        super::inspection::family_for_active_name(name).is_some()
            && PathEntry::read(&directory.join(name)).is_present_and(is_regular_file)
    }) {
        return Ok(false);
    }
    let Ok(canonical) = super::inspection::inspect_generation(&directory) else {
        return Ok(false);
    };
    let canonical_names = canonical
        .families
        .iter()
        .map(|family| OsString::from(family.family.active_name()))
        .collect::<BTreeSet<_>>();
    if canonical_names.is_empty()
        || !staged_names
            .iter()
            .all(|name| canonical_names.contains(name))
    {
        return Ok(false);
    }
    if !staged_names.is_empty()
        && !super::staging_is_what_compaction_stages_or_a_torn_write_of_it(
            &directory,
            &at.staging,
            &canonical,
        )
    {
        return Ok(false);
    }
    #[cfg(test)]
    recovery_pause::reached(&directory, recovery_pause::Point::DiscardProved);
    if !staging.is_absent() {
        fs::remove_dir_all(&at.staging).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.staging.clone(),
            source,
        })?;
    }
    if !manifest_next.is_absent() {
        fs::remove_file(&at.manifest_next).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.manifest_next.clone(),
            source,
        })?;
    }
    Ok(true)
}

/// Removes a family's manifest temporary left alone by an online attempt killed inside its first
/// publication (specs/016 FR-2), and reports whether it did. The caller has read the family
/// manifest and found none, and runs this only for an open that is the only live writer of the
/// family on `locked`, the directory it locked (`DeadAttempts::Recover`): no live attempt of the
/// family can be writing the temporary.
///
/// Online publication moves a source artifact only under a finalized `Prepared`, so with nothing
/// at the family manifest's path and no family staging or previous directory, a valid canonical
/// family is the authority, and the temporary, which never advanced a phase, names nothing.
/// Every path is resolved once, from `locked`, and only while the caller's spelling still names
/// it, as specs/015's closed discard does. Anything else -- a path it cannot read included --
/// leaves every path as it is, and the state keeps its error.
fn discard_dead_first_publication(
    store_dir: &Path,
    locked: &Path,
    paths: &MaintenanceArtifactPaths,
    family: super::inspection::InspectedFamily,
) -> Result<bool, CompactionError> {
    let Ok(directory) = fs::canonicalize(store_dir) else {
        return Ok(false);
    };
    if directory != locked {
        return Ok(false);
    }
    #[cfg(test)]
    recovery_pause::reached(&directory, recovery_pause::Point::DeadAttemptIdentified);
    let Ok(at) = super::publication::family_artifact_paths(&directory.join(family.active_name()))
    else {
        return Ok(false);
    };
    if !PathEntry::read(&at.manifest_next).is_present_and(is_regular_file)
        || !PathEntry::read(&at.manifest).is_absent()
        || !PathEntry::read(&at.staging).is_absent()
        || !PathEntry::read(&at.previous).is_absent()
        || super::inspection::inspect_open_family(&directory, family).is_err()
    {
        return Ok(false);
    }
    fs::remove_file(&at.manifest_next).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Cleanup,
        path: paths.manifest_next.clone(),
        source,
    })?;
    Ok(true)
}

/// Moves back the source artifacts that a finalized online `Prepared`'s cutover had already moved
/// into the previous directory when its process was killed (specs/016 FR-2), as closed recovery
/// restores a moved source; the abandonment that follows then finds the whole source in place.
/// The caller runs this only for an open that is the only live writer of the family on `locked`
/// (`DeadAttempts::Recover`): no live cutover of the family can be between its moves.
///
/// Everything is checked before the first move, and anything the manifest cannot account for
/// leaves every path as it is, for the checks that follow: the previous directory must be a real
/// directory, each of its entries a regular file named for a source artifact, matching that
/// artifact's descriptor and absent from the store, and every other source artifact must match
/// in the store. The source was frozen under exclusive coordination before the finalized manifest
/// was published, and its writer detached before any artifact moved, so the restored artifacts
/// are the exact source. Every path is resolved once, from `locked`, and only while the caller's
/// spelling still names it. A path it cannot read proves nothing.
///
/// The move asks for no-replace, which only Windows' write-through move (Physical) honours; every
/// other move is a rename that replaces its destination. What keeps a destination from being
/// replaced is the check before the first move, which holds because nothing else writes the
/// family's artifacts meanwhile.
fn restore_split_online_source(
    store_dir: &Path,
    locked: &Path,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    let Ok(directory) = fs::canonicalize(store_dir) else {
        return Ok(());
    };
    if directory != locked {
        return Ok(());
    }
    #[cfg(test)]
    recovery_pause::reached(&directory, recovery_pause::Point::DeadAttemptIdentified);
    let ManifestScope::Family { active_name, .. } = &manifest.scope else {
        return Ok(());
    };
    let Ok(at) = super::publication::family_artifact_paths(&directory.join(active_name)) else {
        return Ok(());
    };
    let PathEntry::Present(previous) = PathEntry::read(&at.previous) else {
        return Ok(());
    };
    let Some(previous_name) = at.previous.file_name() else {
        return Ok(());
    };
    if !is_real_directory(&previous) {
        return Ok(());
    }
    let mut by_name = std::collections::BTreeMap::new();
    for descriptor in &manifest.source_inventory {
        let Some(name) = descriptor.relative_path.file_name() else {
            return Ok(());
        };
        if by_name.insert(OsString::from(name), descriptor).is_some() {
            return Ok(());
        }
    }
    let Some(names) = regular_file_names(&at.previous) else {
        return Ok(());
    };
    let mut moved = BTreeSet::new();
    for name in names {
        let Some(descriptor) = by_name.get(&name) else {
            return Ok(());
        };
        let mut at_previous = (*descriptor).clone();
        at_previous.relative_path = PathBuf::from(previous_name).join(&name);
        if verify_descriptor(&directory, &at_previous).is_err()
            || !PathEntry::read(&directory.join(&descriptor.relative_path)).is_absent()
        {
            return Ok(());
        }
        moved.insert(name);
    }
    if moved.is_empty() {
        return Ok(());
    }
    for (name, descriptor) in &by_name {
        if !moved.contains(name) && verify_descriptor(&directory, descriptor).is_err() {
            return Ok(());
        }
    }
    for name in &moved {
        let relative = &by_name[name].relative_path;
        crate::durability::move_namespace(
            &at.previous.join(name),
            &directory.join(relative),
            manifest.durability,
            crate::durability::NamespaceMoveMode::NoReplace,
        )
        .map_err(|source| CompactionError::Io {
            operation: CompactionOperation::PublishPrevious,
            path: store_dir.join(relative),
            source,
        })?;
    }
    for synchronized in [at.previous.as_path(), directory.as_path()] {
        crate::durability::synchronize_namespace_parent(synchronized, manifest.durability)
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::PublishPrevious,
                path: store_dir.to_path_buf(),
                source,
            })?;
    }
    Ok(())
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
    let mut lock_file = None;
    for entry in entries {
        let Ok(entry) = entry else {
            return Ok(crate::CleanupStatus::Pending);
        };
        let Ok(file_type) = entry.file_type() else {
            return Ok(crate::CleanupStatus::Pending);
        };
        if crate::maintenance_coordination::is_inner_lock_file(&entry.file_name(), file_type) {
            lock_file = Some(entry.path());
            continue;
        }
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
    if lock_file.is_some_and(|path| {
        crate::maintenance_coordination::remove_retired_inner_lock(&path).is_err()
    }) {
        return Ok(crate::CleanupStatus::Pending);
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
        #[cfg(test)]
        crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
            crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
                phase: crate::test_support::fault_checkpoint::MaintenancePhase::PreviousPublished,
                cut: crate::test_support::fault_checkpoint::MaintenanceCut::RollbackRestore,
            },
        );
        if !generation_matches(store_dir, &manifest.source_inventory) {
            return Err(authority_undetermined(store_dir, paths));
        }
        return finish_closed_rollback(store_dir, paths);
    }
    // A rollback interrupted after its restore rename, before or after it removed staging
    // (specs/015 FR-4). The forward path never leaves the source at the canonical path with no
    // previous generation, so only the rollback did this: the source is the verified authority
    // again, and any staging is what the rollback had already decided to discard.
    if canonical_exists
        && !previous_exists
        && generation_matches(store_dir, &manifest.source_inventory)
    {
        return finish_closed_rollback(store_dir, paths);
    }
    Err(authority_undetermined(store_dir, paths))
}

/// The end of a closed rollback, once the verified source is back at the canonical path: remove
/// the staging directory, then the manifest.
fn finish_closed_rollback(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
) -> Result<RecoveredAuthority, CompactionError> {
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
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::PreviousPublished,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::RollbackCleanup,
        },
    );
    fs::remove_file(&paths.manifest).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Cleanup,
        path: paths.manifest.clone(),
        source,
    })?;
    Ok(RecoveredAuthority::Previous)
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
    if prefix_mode {
        return online_source_prefix_matches(anchor, manifest);
    }
    manifest
        .source_inventory
        .iter()
        .all(|descriptor| verify_descriptor(anchor, descriptor).is_ok())
}

pub(crate) fn recover_prepared_online(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    abandon_prepared_online(store_dir, paths, manifest)
}

pub(crate) fn abandon_prepared_online(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    validate_online_prepared_binding(store_dir, paths, manifest)?;
    remove_unpublished_manifest_temp(paths)?;
    if !source_descriptors_match(store_dir, manifest) {
        return Err(authority_undetermined(store_dir, paths));
    }
    if path_exists(&paths.previous)? {
        let metadata =
            fs::symlink_metadata(&paths.previous).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.previous.clone(),
                source,
            })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(authority_undetermined(store_dir, paths));
        }
        let mut entries = fs::read_dir(&paths.previous).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.previous.clone(),
            source,
        })?;
        match entries.next() {
            None => fs::remove_dir(&paths.previous).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.previous.clone(),
                source,
            })?,
            Some(Ok(_)) => return Err(authority_undetermined(store_dir, paths)),
            Some(Err(source)) => {
                return Err(CompactionError::Io {
                    operation: CompactionOperation::Cleanup,
                    path: paths.previous.clone(),
                    source,
                });
            }
        }
    }
    if path_exists(&paths.staging)? {
        let metadata =
            fs::symlink_metadata(&paths.staging).map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.staging.clone(),
                source,
            })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(authority_undetermined(store_dir, paths));
        }
        fs::remove_file(&paths.staging).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.staging.clone(),
            source,
        })?;
    }
    match fs::remove_file(&paths.manifest) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.manifest.clone(),
                source,
            });
        }
    }
    crate::durability::synchronize_namespace_parent(store_dir, manifest.durability).map_err(
        |source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: store_dir.to_path_buf(),
            source,
        },
    )?;
    Ok(())
}

fn validate_online_prepared_binding(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<(), CompactionError> {
    let family = match manifest.scope {
        ManifestScope::Family { family, .. } => family,
        ManifestScope::Directory => {
            return Err(CompactionError::InvalidArtifact {
                path: paths.manifest.clone(),
            });
        }
    };
    let inspected = inspected_family(family);
    validate_online_manifest_binding(store_dir, paths, manifest, inspected)?;
    if manifest.phase != ManifestPhase::Prepared {
        return Err(CompactionError::InvalidArtifact {
            path: paths.manifest.clone(),
        });
    }
    Ok(())
}

fn validate_online_manifest_binding(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
    expected_family: super::inspection::InspectedFamily,
) -> Result<(), CompactionError> {
    let ManifestScope::Family {
        family,
        active_name,
    } = &manifest.scope
    else {
        return Err(CompactionError::InvalidArtifact {
            path: paths.manifest.clone(),
        });
    };
    let expected_paths = super::publication::family_artifact_paths(&store_dir.join(active_name))
        .map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Inspect,
            path: store_dir.join(active_name),
            source,
        })?;
    let source_names_bound = online_source_inventory_is_canonical(manifest, *family, active_name);
    let replacement_name = paths.staging.file_name();
    let replacement_bound = manifest.replacement_inventory.iter().all(|descriptor| {
        descriptor.family == Some(*family)
            && descriptor.role == ArtifactRole::ReplacementPrefix
            && descriptor.relative_path.file_name() == replacement_name
            && descriptor.relative_path.components().count() == 1
    });
    if manifest.mode != ManifestMode::OnlineFamily
        || inspected_family(*family) != expected_family
        || &expected_paths != paths
        || paths.staging.file_name().map(PathBuf::from) != Some(manifest.staging_location.clone())
        || paths.previous.file_name().map(PathBuf::from) != Some(manifest.previous_location.clone())
        || !source_names_bound
        || !replacement_bound
    {
        return Err(CompactionError::InvalidArtifact {
            path: paths.manifest.clone(),
        });
    }
    Ok(())
}

fn inspected_family(family: StoreFamily) -> super::inspection::InspectedFamily {
    match family {
        StoreFamily::KeyValue => super::inspection::InspectedFamily::KeyValue,
        StoreFamily::KeySet => super::inspection::InspectedFamily::KeySet,
        StoreFamily::KeyMap => super::inspection::InspectedFamily::KeyMap,
    }
}

fn online_replacement_prefix_matches(store_dir: &Path, manifest: &CompactionManifest) -> bool {
    let ManifestScope::Family {
        family,
        active_name,
    } = &manifest.scope
    else {
        return false;
    };
    let [replacement] = manifest.replacement_inventory.as_slice() else {
        return false;
    };
    if replacement.family != Some(*family)
        || replacement.role != ArtifactRole::ReplacementPrefix
        || replacement.relative_path != manifest.staging_location
    {
        return false;
    }
    let inspected = inspected_family(*family);
    if active_name != Path::new(inspected.active_name()) {
        return false;
    }
    let Ok(inspection) = super::inspection::inspect_open_family(store_dir, inspected) else {
        return false;
    };
    let mut remaining = match usize::try_from(replacement.length) {
        Ok(length) => length,
        Err(_) => return false,
    };
    let mut hasher = crc32fast::Hasher::new();
    for segment in 0..inspection.sealed_segment_count {
        let path = store_dir.join(format!("{}.segment-{segment:020}", inspected.active_name()));
        if !hash_prefix_part(&path, &mut remaining, &mut hasher) {
            return false;
        }
        if remaining == 0 {
            return hasher.finalize() == replacement.checksum;
        }
    }
    if !hash_prefix_part(&store_dir.join(active_name), &mut remaining, &mut hasher)
        || remaining != 0
    {
        return false;
    }
    hasher.finalize() == replacement.checksum
}

fn hash_prefix_part(path: &Path, remaining: &mut usize, hasher: &mut crc32fast::Hasher) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut buffer = [0_u8; 8192];
    while *remaining > 0 {
        let wanted = buffer.len().min(*remaining);
        let Ok(count) = file.read(&mut buffer[..wanted]) else {
            return false;
        };
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        *remaining -= count;
    }
    true
}

fn remaining_online_previous_is_valid(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<bool, CompactionError> {
    let metadata = match fs::symlink_metadata(&paths.previous) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::Cleanup,
                path: paths.previous.clone(),
                source,
            });
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let Some(previous_name) = paths.previous.file_name() else {
        return Ok(false);
    };
    let mut expected = std::collections::BTreeMap::new();
    for source in &manifest.source_inventory {
        let Some(file_name) = source.relative_path.file_name() else {
            return Ok(false);
        };
        if expected.insert(OsString::from(file_name), source).is_some() {
            return Ok(false);
        }
    }
    for entry in fs::read_dir(&paths.previous).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Cleanup,
        path: paths.previous.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Cleanup,
            path: paths.previous.clone(),
            source,
        })?;
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            return Ok(false);
        }
        let Some(source) = expected.get(&entry.file_name()) else {
            return Ok(false);
        };
        let mut translated = (*source).clone();
        translated.relative_path = PathBuf::from(previous_name).join(entry.file_name());
        if verify_descriptor(store_dir, &translated).is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn complete_online_previous_matches(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> Result<bool, CompactionError> {
    let metadata = match fs::symlink_metadata(&paths.previous) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(CompactionError::Io {
                operation: CompactionOperation::Inspect,
                path: paths.previous.clone(),
                source,
            });
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let Some(previous_name) = paths.previous.file_name() else {
        return Ok(false);
    };
    let mut expected = BTreeSet::new();
    for source in &manifest.source_inventory {
        let Some(file_name) = source.relative_path.file_name() else {
            return Ok(false);
        };
        if !expected.insert(OsString::from(file_name)) {
            return Ok(false);
        }
        let mut translated = source.clone();
        translated.relative_path = PathBuf::from(previous_name).join(file_name);
        if verify_descriptor(store_dir, &translated).is_err() {
            return Ok(false);
        }
    }
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(&paths.previous).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Inspect,
        path: paths.previous.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Inspect,
            path: paths.previous.clone(),
            source,
        })?;
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            return Ok(false);
        }
        actual.insert(entry.file_name());
    }
    Ok(actual == expected)
}

fn online_staging_matches(
    store_dir: &Path,
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> bool {
    let [replacement] = manifest.replacement_inventory.as_slice() else {
        return false;
    };
    let mut staging = replacement.clone();
    staging.relative_path = match paths.staging.file_name() {
        Some(name) => PathBuf::from(name),
        None => return false,
    };
    verify_descriptor(store_dir, &staging).is_ok()
}

fn online_source_prefix_matches(anchor: &Path, manifest: &CompactionManifest) -> bool {
    let ManifestScope::Family {
        family,
        active_name,
    } = &manifest.scope
    else {
        return false;
    };
    let inspected_family = match family {
        StoreFamily::KeyValue => super::inspection::InspectedFamily::KeyValue,
        StoreFamily::KeySet => super::inspection::InspectedFamily::KeySet,
        StoreFamily::KeyMap => super::inspection::InspectedFamily::KeyMap,
    };
    if active_name != Path::new(inspected_family.active_name())
        || !online_source_inventory_is_canonical(manifest, *family, active_name)
    {
        return false;
    }
    let Ok(inspection) = super::inspection::inspect_open_family(anchor, inspected_family) else {
        return false;
    };
    let mut chain = Vec::new();
    for segment in 0..inspection.sealed_segment_count {
        let path = anchor.join(format!(
            "{}.segment-{segment:020}",
            inspected_family.active_name()
        ));
        let Ok(bytes) = fs::read(path) else {
            return false;
        };
        chain.extend_from_slice(&bytes);
    }
    let Ok(active) = fs::read(anchor.join(active_name)) else {
        return false;
    };
    chain.extend_from_slice(&active);

    let mut offset = 0_usize;
    for descriptor in &manifest.source_inventory {
        let Ok(length) = usize::try_from(descriptor.length) else {
            return false;
        };
        let Some(end) = offset.checked_add(length) else {
            return false;
        };
        let Some(prefix_part) = chain.get(offset..end) else {
            return false;
        };
        if crc32fast::hash(prefix_part) != descriptor.checksum {
            return false;
        }
        offset = end;
    }
    true
}

fn online_source_inventory_is_canonical(
    manifest: &CompactionManifest,
    family: StoreFamily,
    active_name: &Path,
) -> bool {
    let expected_active_name = match family {
        StoreFamily::KeyValue => "kv.wal.dat",
        StoreFamily::KeySet => "set.wal.dat",
        StoreFamily::KeyMap => "map.wal.dat",
    };
    if active_name != Path::new(expected_active_name) {
        return false;
    }
    let Some((active, sealed)) = manifest.source_inventory.split_last() else {
        return false;
    };
    if active.family != Some(family)
        || active.role != ArtifactRole::Active
        || active.relative_path != active_name
    {
        return false;
    }
    sealed.iter().enumerate().all(|(segment, descriptor)| {
        descriptor.family == Some(family)
            && descriptor.role == ArtifactRole::SealedSegment
            && descriptor.relative_path == format!("{}.segment-{segment:020}", expected_active_name)
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

pub(super) fn generation_matches(location: &Path, descriptors: &[ArtifactDescriptor]) -> bool {
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
        if crate::maintenance_coordination::is_inner_lock_file(&entry.file_name(), file_type) {
            continue;
        }
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

/// Parks the first arrival at one point of recovery for one store directory (specs/015), so that a
/// test can act on the directory while that recovery is parked:
/// - `FamilyRecovery`: an open whose directory-level recovery has run, before its family recovery
///   (third review), so that the family can be opened again and an online attempt started;
/// - `DiscardEntry` and `DiscardProved`: the closed discard (plan D3), on entry and once it has
///   proved the debris redundant, before its first removal (fourth review), so that a test can
///   change what the caller's path names, or run a second open through the same discard;
/// - `DeadAttemptIdentified`: either rule of specs/016 FR-2, once it has found the caller's path
///   naming the directory the open locked (that spec's first review), so that a test can point
///   the path at another directory before the rule reads or acts.
///
/// A pause names the store directory through its canonical path when it is reached, so tests
/// running in parallel do not see each other's.
#[cfg(test)]
pub(crate) mod recovery_pause {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
    use std::time::Duration;

    /// How long either side waits for the other, so a failing test cannot park an open for ever.
    const WATCHDOG: Duration = Duration::from_secs(30);

    /// Where recovery parks.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum Point {
        FamilyRecovery,
        DiscardEntry,
        /// Once the discard has found the caller's path naming the directory it locked, before
        /// it reads anything there.
        DiscardIdentified,
        DiscardProved,
        /// Once a dead online attempt's rule (specs/016 FR-2) has found the caller's path naming
        /// the directory it locked, before it reads anything there.
        DeadAttemptIdentified,
    }

    /// `(reached, released)`.
    #[derive(Default)]
    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl Gate {
        fn state(&self) -> MutexGuard<'_, (bool, bool)> {
            self.state.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }

    type Installed = Vec<((PathBuf, Point), Arc<Gate>)>;

    static PAUSES: Mutex<Installed> = Mutex::new(Vec::new());

    fn pauses() -> MutexGuard<'static, Installed> {
        PAUSES.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A pause of the next recovery of the installed directory at its point. Dropping it releases
    /// that recovery.
    pub(crate) struct Pause {
        key: (PathBuf, Point),
        gate: Arc<Gate>,
    }

    /// The next recovery of `store_dir` to reach `point` parks there; later ones pass.
    pub(crate) fn install(store_dir: &Path, point: Point) -> Pause {
        let key = (std::fs::canonicalize(store_dir).unwrap(), point);
        let gate = Arc::new(Gate::default());
        let mut pauses = pauses();
        assert!(
            pauses.iter().all(|(other, _)| *other != key),
            "one pause per store directory and point"
        );
        pauses.push((key.clone(), Arc::clone(&gate)));
        Pause { key, gate }
    }

    impl Pause {
        /// Waits until a recovery is parked.
        pub(crate) fn wait_reached(&self) {
            let state = self.gate.state();
            let (state, _) = self
                .gate
                .changed
                .wait_timeout_while(state, WATCHDOG, |(reached, _)| !*reached)
                .unwrap_or_else(PoisonError::into_inner);
            assert!(state.0, "no recovery reached its pause at {:?}", self.key.1);
        }

        /// Lets the parked recovery go on.
        pub(crate) fn release(&self) {
            self.gate.state().1 = true;
            self.gate.changed.notify_all();
        }
    }

    impl Drop for Pause {
        fn drop(&mut self) {
            self.release();
            pauses().retain(|(key, _)| *key != self.key);
        }
    }

    /// Parks here, once, while a pause is installed for `store_dir` at `point`.
    pub(super) fn reached(store_dir: &Path, point: Point) {
        if pauses().is_empty() {
            return;
        }
        let Ok(directory) = std::fs::canonicalize(store_dir) else {
            return;
        };
        let key = (directory, point);
        let gate = {
            let mut pauses = pauses();
            let Some(index) = pauses.iter().position(|(installed, _)| *installed == key) else {
                return;
            };
            pauses.remove(index).1
        };
        let mut state = gate.state();
        state.0 = true;
        gate.changed.notify_all();
        let _ = gate
            .changed
            .wait_timeout_while(state, WATCHDOG, |(_, released)| !*released)
            .unwrap_or_else(PoisonError::into_inner);
    }
}
