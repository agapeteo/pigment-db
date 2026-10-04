//! Private storage-compaction implementation.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::compaction::inspection::{
    exact_artifact_bytes_match, inspect_directory, inspect_generation, inspect_open_family,
    FamilyInspection, InspectedFamily,
};
use crate::compaction::manifest::{verify_descriptor, ArtifactDescriptor, ArtifactRole};
use crate::compaction::publication::{
    cleanup_closed_with_checkpoint, directory_artifact_paths, publish_closed_prepared,
    publish_closed_previous_with_checkpoint, publish_closed_replacement_with_checkpoint,
    publish_online_prepared, MaintenanceArtifactPaths,
};
use crate::wal::replay::{
    encode_current_key_map_snapshot_with_metadata, encode_current_key_set_snapshot_with_metadata,
    encode_current_key_value_snapshot_with_metadata, replay_key_map_tail, replay_key_set_tail,
    replay_key_value_tail, KeyMapSnapshot, KeySetSnapshot, KeyValueSnapshot, ReplaySnapshot,
    TailReplay,
};
use crate::{
    ClosedCompactionOptions, CompactionError, CompactionOperation, DirectoryCompactionOutcome,
    DurabilityPolicy, FamilyCompactionOutcome, StoreFamily,
};

#[allow(dead_code)]
pub(crate) struct PreparedOnlineCapture<'a, W: Write> {
    pub(crate) attempt: crate::maintenance_coordination::OnlineAttemptGuard<'a, W>,
    pub(crate) capture: CapturedFamily,
    pub(crate) paths: MaintenanceArtifactPaths,
    pub(crate) manifest: crate::compaction::manifest::CompactionManifest,
    pub(crate) generation_guard: crate::maintenance_coordination::StagingGenerationGuard,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OnlineCaptureStage {
    SnapshotCaptured,
    RecorderActivated,
    ManifestPrepared,
}

#[allow(dead_code)]
pub(crate) struct ValidatedOnlineStaging<'a, W: Write> {
    pub(crate) prepared: PreparedOnlineCapture<'a, W>,
    pub(crate) staging: CapturedFamily,
    pub(crate) replacement_inventory: Vec<ArtifactDescriptor>,
}

#[allow(dead_code)]
pub(crate) struct AppliedOnlineDelta<'a, W: Write> {
    pub(crate) staged: ValidatedOnlineStaging<'a, W>,
    pub(crate) replayed: usize,
    pub(crate) encoded_bytes: u64,
    pub(crate) accepted_buckets: Vec<u64>,
    pub(crate) group_frame_counts: Vec<usize>,
}

#[allow(dead_code)]
pub(crate) struct OnlineDeltaSummary {
    pub(crate) replayed: usize,
    pub(crate) encoded_bytes: u64,
    pub(crate) accepted_buckets: Vec<u64>,
    pub(crate) group_frame_counts: Vec<usize>,
}

#[allow(dead_code)]
pub(crate) struct CompletedOnlineCutover {
    pub(crate) family: StoreFamily,
    pub(crate) before_bytes: u64,
    pub(crate) after_bytes: u64,
    pub(crate) sealed_segments_removed: usize,
    pub(crate) replayed: usize,
    pub(crate) paths: MaintenanceArtifactPaths,
    pub(crate) manifest: crate::compaction::manifest::CompactionManifest,
    pub(crate) cleanup: crate::CleanupStatus,
}

impl CompletedOnlineCutover {
    pub(crate) fn into_outcome(self) -> FamilyCompactionOutcome {
        FamilyCompactionOutcome::online(
            self.family,
            self.before_bytes,
            self.after_bytes,
            self.sealed_segments_removed,
            self.replayed,
            self.cleanup,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OnlineCleanupStage {
    CleanupPendingPublished,
    BeforePreviousArtifact(usize),
    BeforePreviousDirectory,
    BeforeManifest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OnlineStagingStage {
    Encoding,
    Create,
    Write,
    Synchronize,
    Validation,
    Reopen,
}

pub(crate) fn prepare_online_staging<'a, W: Write>(
    mut prepared: PreparedOnlineCapture<'a, W>,
    mut checkpoint: impl FnMut(OnlineStagingStage) -> io::Result<()>,
) -> Result<ValidatedOnlineStaging<'a, W>, CompactionError> {
    let checkpoint_error = |stage, source| CompactionError::Io {
        operation: match stage {
            OnlineStagingStage::Validation | OnlineStagingStage::Reopen => {
                CompactionOperation::ValidateStaging
            }
            _ => CompactionOperation::WriteStaging,
        },
        path: prepared.paths.staging.clone(),
        source,
    };
    checkpoint(OnlineStagingStage::Encoding)
        .map_err(|source| checkpoint_error(OnlineStagingStage::Encoding, source))?;
    let encoded =
        encode_captured_family(&prepared.capture).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: prepared.paths.staging.clone(),
            source,
        })?;
    checkpoint(OnlineStagingStage::Create)
        .map_err(|source| checkpoint_error(OnlineStagingStage::Create, source))?;
    let mut staging_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&prepared.paths.staging)
        .map_err(|source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: prepared.paths.staging.clone(),
            source,
        })?;
    prepared.generation_guard.mark_staging_owned();
    checkpoint(OnlineStagingStage::Write)
        .map_err(|source| checkpoint_error(OnlineStagingStage::Write, source))?;
    staging_file
        .write_all(&encoded)
        .and_then(|()| staging_file.flush())
        .map_err(|source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: prepared.paths.staging.clone(),
            source,
        })?;
    checkpoint(OnlineStagingStage::Synchronize)
        .map_err(|source| checkpoint_error(OnlineStagingStage::Synchronize, source))?;
    if prepared.manifest.durability == DurabilityPolicy::Physical {
        staging_file
            .sync_all()
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::WriteStaging,
                path: prepared.paths.staging.clone(),
                source,
            })?;
    }
    drop(staging_file);
    let staging_name = prepared
        .paths
        .staging
        .file_name()
        .map(PathBuf::from)
        .ok_or_else(|| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: prepared.paths.staging.clone(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "online staging has no native file name",
            ),
        })?;
    let replacement_inventory = vec![ArtifactDescriptor {
        relative_path: staging_name,
        role: ArtifactRole::ReplacementPrefix,
        family: Some(prepared.capture.family),
        length: u64::try_from(encoded.len()).map_err(|_| CompactionError::InvalidArtifact {
            path: prepared.paths.staging.clone(),
        })?,
        checksum: crc32fast::hash(&encoded),
    }];

    checkpoint(OnlineStagingStage::Validation)
        .map_err(|source| checkpoint_error(OnlineStagingStage::Validation, source))?;
    let anchor = prepared
        .paths
        .staging
        .parent()
        .ok_or_else(|| CompactionError::Io {
            operation: CompactionOperation::ValidateStaging,
            path: prepared.paths.staging.clone(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "online staging has no parent anchor",
            ),
        })?;
    verify_descriptor(anchor, &replacement_inventory[0]).map_err(|_| {
        CompactionError::InvalidArtifact {
            path: prepared.paths.staging.clone(),
        }
    })?;
    checkpoint(OnlineStagingStage::Reopen)
        .map_err(|source| checkpoint_error(OnlineStagingStage::Reopen, source))?;
    let staging =
        capture_validated_online_staging(&prepared.paths.staging, prepared.capture.family)?;
    compare_captured_families(
        std::slice::from_ref(&prepared.capture),
        std::slice::from_ref(&staging),
    )?;
    Ok(ValidatedOnlineStaging {
        prepared,
        staging,
        replacement_inventory,
    })
}

pub(crate) fn apply_online_delta_to_staging<W: Write>(
    staged: &mut ValidatedOnlineStaging<'_, W>,
    delta: &crate::wal::DeltaRecorder,
) -> Result<OnlineDeltaSummary, CompactionError> {
    if delta.overflowed() {
        return Err(CompactionError::ConcurrentDeltaLimitExceeded {
            limit: delta.limit(),
        });
    }
    if !delta.wal_healthy() {
        return Err(CompactionError::FailedClosed {
            detail: "online compaction WAL became unhealthy during staging".to_owned(),
        });
    }
    let descriptor =
        staged
            .replacement_inventory
            .first()
            .ok_or_else(|| CompactionError::FailedClosed {
                detail: "validated online staging has no replacement descriptor".to_owned(),
            })?;
    let actual_len = fs::metadata(&staged.prepared.paths.staging)
        .map_err(|source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: staged.prepared.paths.staging.clone(),
            source,
        })?
        .len();
    if descriptor.length != actual_len {
        return Err(CompactionError::InvalidArtifact {
            path: staged.prepared.paths.staging.clone(),
        });
    }
    let encoded = crate::wal::replay::encode_current_v2_delta(actual_len, delta.groups()).map_err(
        |source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: staged.prepared.paths.staging.clone(),
            source,
        },
    )?;
    let encoded_len =
        u64::try_from(encoded.len()).map_err(|_| CompactionError::InvalidArtifact {
            path: staged.prepared.paths.staging.clone(),
        })?;
    if encoded_len != delta.used_bytes() {
        return Err(CompactionError::FailedClosed {
            detail: "online delta accounting diverged from regenerated V2 framing".to_owned(),
        });
    }
    if !encoded.is_empty() {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&staged.prepared.paths.staging)
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::WriteStaging,
                path: staged.prepared.paths.staging.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.flush())
            .and_then(|()| {
                if staged.prepared.manifest.durability == DurabilityPolicy::Physical {
                    file.sync_all()
                } else {
                    Ok(())
                }
            })
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::WriteStaging,
                path: staged.prepared.paths.staging.clone(),
                source,
            })?;
    }
    let bytes = fs::read(&staged.prepared.paths.staging).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::ValidateStaging,
        path: staged.prepared.paths.staging.clone(),
        source,
    })?;
    let replacement = staged
        .replacement_inventory
        .first_mut()
        .expect("replacement descriptor presence was checked");
    replacement.length =
        u64::try_from(bytes.len()).map_err(|_| CompactionError::InvalidArtifact {
            path: staged.prepared.paths.staging.clone(),
        })?;
    replacement.checksum = crc32fast::hash(&bytes);
    let anchor =
        staged
            .prepared
            .paths
            .staging
            .parent()
            .ok_or_else(|| CompactionError::InvalidArtifact {
                path: staged.prepared.paths.staging.clone(),
            })?;
    verify_descriptor(anchor, replacement).map_err(|_| CompactionError::InvalidArtifact {
        path: staged.prepared.paths.staging.clone(),
    })?;
    staged.staging = capture_validated_online_staging(
        &staged.prepared.paths.staging,
        staged.prepared.capture.family,
    )?;
    let (accepted_buckets, group_frame_counts) = delta.group_metadata();
    Ok(OnlineDeltaSummary {
        replayed: delta.group_count(),
        encoded_bytes: encoded_len,
        accepted_buckets,
        group_frame_counts,
    })
}

pub(crate) fn abandon_online_prepublication<W: Write>(
    staged: &ValidatedOnlineStaging<'_, W>,
) -> Result<(), CompactionError> {
    let store_dir = staged.prepared.paths.manifest.parent().ok_or_else(|| {
        CompactionError::InvalidArtifact {
            path: staged.prepared.paths.manifest.clone(),
        }
    })?;
    recovery::abandon_prepared_online(store_dir, &staged.prepared.paths, &staged.prepared.manifest)
}

pub(crate) fn validate_online_staging_against_live<W: Write>(
    staged: &ValidatedOnlineStaging<'_, W>,
    live_state: CapturedLogicalState,
    metadata: crate::wal::OnlineCaptureMetadata,
) -> Result<(), CompactionError> {
    if metadata.durability_policy != staged.prepared.manifest.durability {
        return Err(CompactionError::FailedClosed {
            detail: "online cutover durability policy changed during the attempt".to_owned(),
        });
    }
    let current = CapturedFamily {
        family: staged.prepared.capture.family,
        state: live_state,
        granularity_nanos: metadata.granularity_nanos,
        last_bucket: metadata.last_bucket,
        before_bytes: metadata.active_len,
        sealed_segment_count: 0,
    };
    compare_captured_families(
        std::slice::from_ref(&current),
        std::slice::from_ref(&staged.staging),
    )
}

pub(crate) fn finalize_online_prepared<W: Write>(
    staged: &mut ValidatedOnlineStaging<'_, W>,
    live_state: CapturedLogicalState,
    metadata: crate::wal::OnlineCaptureMetadata,
) -> Result<(), CompactionError> {
    validate_online_staging_against_live(staged, live_state.clone(), metadata)?;
    let store_dir = staged.prepared.paths.manifest.parent().ok_or_else(|| {
        CompactionError::InvalidArtifact {
            path: staged.prepared.paths.manifest.clone(),
        }
    })?;
    let inspected_family = match staged.prepared.capture.family {
        StoreFamily::KeyValue => InspectedFamily::KeyValue,
        StoreFamily::KeySet => InspectedFamily::KeySet,
        StoreFamily::KeyMap => InspectedFamily::KeyMap,
    };
    let inspection = inspect_open_family(store_dir, inspected_family).map_err(|error| {
        crate::maintenance::map_inspection_error(store_dir.to_path_buf(), error)
    })?;
    let (current, source_inventory) =
        capture_online_family(store_dir, &inspection, live_state, metadata)?;
    compare_captured_families(
        std::slice::from_ref(&current),
        std::slice::from_ref(&staged.staging),
    )?;
    crate::compaction::publication::publish_online_finalized_prepared(
        &staged.prepared.paths,
        &mut staged.prepared.manifest,
        source_inventory,
        staged.replacement_inventory.clone(),
    )?;
    staged.prepared.capture = current;
    Ok(())
}

pub(crate) fn fail_online_publication_closed<W: Write>(
    wal: &crate::wal::WalStorage<W>,
    token: u64,
    error: &CompactionError,
) -> Result<(), CompactionError> {
    wal.mark_online_maintenance_indeterminate(token, error.to_string())
        .map_err(|source| CompactionError::FailedClosed {
            detail: format!(
                "online publication failed ({error}) and the detached WAL could not be marked indeterminate: {source}"
            ),
        })
}

pub(crate) fn cleanup_online_publication(
    paths: &MaintenanceArtifactPaths,
    manifest: &mut crate::compaction::manifest::CompactionManifest,
    checkpoint: impl FnMut(OnlineCleanupStage) -> io::Result<()>,
) -> Result<crate::CleanupStatus, CompactionError> {
    let store_dir = paths
        .manifest
        .parent()
        .ok_or_else(|| CompactionError::InvalidArtifact {
            path: paths.manifest.clone(),
        })?;
    recovery::recover_online_cleanup_with_checkpoint(store_dir, paths, manifest, checkpoint)
}

pub(crate) fn complete_online_cutover<'a>(
    coordinator: &'a crate::maintenance_coordination::MaintenanceCoordinator,
    wal: &'a crate::wal::WalStorage<std::fs::File>,
    mut staged: ValidatedOnlineStaging<'a, std::fs::File>,
    capture_live_state: impl FnOnce() -> CapturedLogicalState,
    reopen: impl FnOnce(&Path) -> io::Result<std::fs::File>,
    cleanup_checkpoint: impl FnMut(OnlineCleanupStage) -> io::Result<()>,
) -> Result<CompletedOnlineCutover, CompactionError> {
    let mut completed = {
        let _exclusive = coordinator.exclusive();
        let delta = staged.prepared.attempt.detach_recorder().ok_or_else(|| {
            CompactionError::FailedClosed {
                detail: "online compaction lost its matching delta recorder".to_owned(),
            }
        })?;
        let applied = match apply_online_delta_to_staging(&mut staged, &delta) {
            Ok(applied) => applied,
            Err(error @ CompactionError::ConcurrentDeltaLimitExceeded { .. }) => {
                abandon_online_prepublication(&staged)?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let metadata = wal
            .online_capture_metadata()
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::ValidateStaging,
                path: staged.prepared.paths.staging.clone(),
                source,
            })?;
        finalize_online_prepared(&mut staged, capture_live_state(), metadata)?;

        let token = staged.prepared.attempt.token();
        let detached = wal
            .take_online_writer(token)
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::PublishPrevious,
                path: staged.prepared.paths.manifest.clone(),
                source,
            })?;
        let closed = detached.close();
        let expected_len = staged
            .replacement_inventory
            .first()
            .ok_or_else(|| CompactionError::InvalidArtifact {
                path: staged.prepared.paths.staging.clone(),
            })?
            .length;
        let publication = (|| {
            publication::publish_online_previous(
                &staged.prepared.paths,
                &mut staged.prepared.manifest,
            )?;
            let (active_path, writer) = publication::publish_online_replacement_with_reopen(
                &staged.prepared.paths,
                &mut staged.prepared.manifest,
                |active_path| {
                    let writer = reopen(active_path)?;
                    if writer.metadata()?.len() != expected_len {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "online replacement length changed before writer handoff",
                        ));
                    }
                    Ok(writer)
                },
            )?;
            wal.install_online_replacement_writer(
                closed,
                writer,
                active_path,
                expected_len,
                staged.staging.granularity_nanos,
                staged.staging.last_bucket,
            )
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::ReopenReplacement,
                path: staged.prepared.paths.manifest.clone(),
                source,
            })?;
            Ok::<(), CompactionError>(())
        })();
        if let Err(error) = publication {
            fail_online_publication_closed(wal, token, &error)?;
            return Err(error);
        }
        CompletedOnlineCutover {
            family: staged.prepared.capture.family,
            before_bytes: staged.prepared.capture.before_bytes,
            after_bytes: expected_len,
            sealed_segments_removed: staged.prepared.capture.sealed_segment_count,
            replayed: applied.replayed,
            paths: staged.prepared.paths.clone(),
            manifest: staged.prepared.manifest.clone(),
            cleanup: crate::CleanupStatus::Pending,
        }
    };
    completed.cleanup = cleanup_online_publication(
        &completed.paths,
        &mut completed.manifest,
        cleanup_checkpoint,
    )?;
    drop(staged);
    Ok(completed)
}

pub(crate) fn begin_online_capture<'a, W: Write>(
    coordinator: &'a crate::maintenance_coordination::MaintenanceCoordinator,
    wal: &'a crate::wal::WalStorage<W>,
    store_dir: &Path,
    inspected_family: InspectedFamily,
    max_delta_bytes: u64,
    capture_live_state: impl FnOnce() -> CapturedLogicalState,
    mut checkpoint: impl FnMut(OnlineCaptureStage),
) -> Result<PreparedOnlineCapture<'a, W>, CompactionError> {
    let attempt = crate::maintenance_coordination::OnlineAttemptGuard::claim(coordinator, wal)
        .map_err(|()| CompactionError::FailedClosed {
            detail: "online compaction is already active for this store instance".to_owned(),
        })?;
    recovery::resolve_online_maintenance_for_compaction(store_dir, inspected_family)?;
    let (capture, source_inventory, paths, manifest, generation_guard) = {
        let _exclusive = coordinator.exclusive();
        let live_state = capture_live_state();
        let metadata = wal
            .online_capture_metadata()
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Capture,
                path: store_dir.to_path_buf(),
                source,
            })?;
        let inspection = inspect_open_family(store_dir, inspected_family).map_err(|error| {
            crate::maintenance::map_inspection_error(store_dir.to_path_buf(), error)
        })?;
        let (capture, source_inventory) =
            capture_online_family(store_dir, &inspection, live_state, metadata)?;
        checkpoint(OnlineCaptureStage::SnapshotCaptured);

        attempt
            .activate_recorder(max_delta_bytes)
            .map_err(|()| CompactionError::FailedClosed {
                detail: "online compaction could not activate its WAL delta recorder".to_owned(),
            })?;
        checkpoint(OnlineCaptureStage::RecorderActivated);

        let active_path = store_dir.join(inspected_family.active_name());
        let paths = crate::compaction::publication::family_artifact_paths(&active_path).map_err(
            |source| CompactionError::Io {
                operation: CompactionOperation::WriteManifest,
                path: active_path.clone(),
                source,
            },
        )?;
        let manifest = publish_online_prepared(
            &paths,
            attempt.token(),
            capture.family,
            PathBuf::from(inspected_family.active_name()),
            metadata.durability_policy,
            source_inventory.clone(),
        )?;
        let generation_guard = crate::maintenance_coordination::StagingGenerationGuard::new(
            paths.clone(),
            manifest.operation_id,
            manifest.durability,
        );
        checkpoint(OnlineCaptureStage::ManifestPrepared);
        (capture, source_inventory, paths, manifest, generation_guard)
    };
    debug_assert_eq!(source_inventory, manifest.source_inventory);
    Ok(PreparedOnlineCapture {
        attempt,
        capture,
        paths,
        manifest,
        generation_guard,
    })
}

pub(crate) mod inspection;
pub(crate) mod manifest;
pub(crate) mod publication;
pub(crate) mod recovery;

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CapturedLogicalState {
    Value(KeyValueSnapshot),
    Set(KeySetSnapshot),
    Map(KeyMapSnapshot),
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedFamily {
    pub(crate) family: StoreFamily,
    pub(crate) state: CapturedLogicalState,
    pub(crate) granularity_nanos: u64,
    pub(crate) last_bucket: u64,
    pub(crate) before_bytes: u64,
    pub(crate) sealed_segment_count: usize,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedGeneration {
    pub(crate) source_dir: PathBuf,
    pub(crate) inventory: Vec<ArtifactDescriptor>,
    /// Each captured file's bytes, keyed as `inventory` names it; shared with the attempt's guard
    /// (`UnpublishedClosedStaging`), which compares the canonical directory with them.
    pub(crate) source_bytes: Arc<BTreeMap<PathBuf, Vec<u8>>>,
    pub(crate) families: Vec<CapturedFamily>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedClosedStaging {
    pub(crate) capture: CapturedGeneration,
    pub(crate) paths: MaintenanceArtifactPaths,
    pub(crate) replacement_inventory: Vec<ArtifactDescriptor>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedClosedStaging {
    pub(crate) staging: CapturedGeneration,
}

/// A closed compaction's staging directory from its creation until the attempt's `Prepared`
/// manifest is published (specs/015 FR-1). Dropped while still armed, it removes the staging
/// directory, but only while two things hold:
/// - the main manifest is what it was when staging was created; otherwise `Prepared` is in place,
///   and its recovery owns the staging (spec 008 FR-049);
/// - the canonical directory is still exactly the generation the attempt captured -- the same
///   names, and in each file the very bytes captured, not only the same length and CRC-32 (fifth
///   review) -- so the staging is a compaction of what the canonical directory holds and removing
///   it loses nothing. A source that changed under the claim (by the medium, or by a writer
///   outside the exclusion) may have left the staging the only complete copy of the captured
///   state; it stays for recovery, which removes it only if every staged file is what a closed
///   compaction of the canonical directory stages, or a torn write of it (FR-3), and otherwise
///   keeps its error.
///
/// Every path it reads or removes -- the canonical directory, the main manifest and the staging
/// directory -- is resolved once, before the staging directory is created through it, as the
/// claim resolves the store path.
/// So what it checks and what it removes do not depend on how the caller spelled the path (a
/// relative path whose parent is the empty path, or a symlink to the directory), nor on the
/// process's working directory when it drops.
pub(crate) struct UnpublishedClosedStaging {
    /// The staging directory as the caller spelled it, for messages.
    staging: PathBuf,
    resolved: Option<ResolvedClosedAttempt>,
    manifest_when_staged: ManifestBytes,
    source_inventory: Vec<ArtifactDescriptor>,
    source_bytes: Arc<BTreeMap<PathBuf, Vec<u8>>>,
    armed: bool,
}

/// The paths an attempt's guard acts on, resolved when its staging was created.
struct ResolvedClosedAttempt {
    source_dir: PathBuf,
    staging: PathBuf,
    manifest: PathBuf,
}

impl ResolvedClosedAttempt {
    fn resolve(paths: &MaintenanceArtifactPaths, source_dir: &Path) -> Option<Self> {
        Some(Self {
            source_dir: fs::canonicalize(source_dir).ok()?,
            staging: resolved_beside(&paths.staging)?,
            manifest: resolved_beside(&paths.manifest)?,
        })
    }
}

/// `path` with its parent directory resolved, so that it names the same entry whatever the
/// working directory is later. Its last component is kept as it is, since the entry itself may be
/// a symlink or absent. `None` when the parent does not resolve.
pub(super) fn resolved_beside(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    Some(fs::canonicalize(parent).ok()?.join(name))
}

/// The main manifest's bytes at one moment.
#[derive(Debug, Eq, PartialEq)]
enum ManifestBytes {
    Absent,
    Present(Vec<u8>),
    Unreadable,
}

impl ManifestBytes {
    /// `Absent` only when nothing at all is at the path: a symlink to nothing is something that
    /// is not a manifest, so it reads as `Unreadable`, as a directory does.
    fn read(manifest: &Path) -> Self {
        match fs::symlink_metadata(manifest) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Self::Absent,
            Err(_) => return Self::Unreadable,
            Ok(_) => {}
        }
        match fs::read(manifest) {
            Ok(bytes) => Self::Present(bytes),
            Err(_) => Self::Unreadable,
        }
    }
}

impl UnpublishedClosedStaging {
    /// Arms the guard for the staging directory just created at `resolved`'s staging path (or at
    /// the caller's spelling when nothing resolved).
    fn new(
        paths: &MaintenanceArtifactPaths,
        resolved: Option<ResolvedClosedAttempt>,
        capture: &CapturedGeneration,
    ) -> Self {
        let manifest_when_staged = resolved
            .as_ref()
            .map_or(ManifestBytes::Unreadable, |resolved| {
                ManifestBytes::read(&resolved.manifest)
            });
        Self {
            staging: paths.staging.clone(),
            resolved,
            manifest_when_staged,
            source_inventory: capture.inventory.clone(),
            source_bytes: Arc::clone(&capture.source_bytes),
            armed: true,
        }
    }

    /// The staging directory stays: `Prepared` names it, or a caller keeps it on purpose.
    pub(crate) fn keep_staging(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnpublishedClosedStaging {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(resolved) = &self.resolved else {
            log::warn!(
                "left unpublished compaction staging {}: the store directory's path could not be \
                 resolved when staging was created; the next open or compaction of the store \
                 decides whether to remove it",
                self.staging.display()
            );
            return;
        };
        let manifest_now = ManifestBytes::read(&resolved.manifest);
        if manifest_now == ManifestBytes::Unreadable
            || self.manifest_when_staged == ManifestBytes::Unreadable
            || manifest_now != self.manifest_when_staged
        {
            return;
        }
        if !generation_is_exactly(
            &resolved.source_dir,
            &self.source_inventory,
            &self.source_bytes,
        ) {
            log::warn!(
                "left unpublished compaction staging {}: the store directory is no longer the \
                 generation the compaction captured, so the staging may be the only complete copy \
                 of it",
                resolved.staging.display()
            );
            return;
        }
        let is_directory = fs::symlink_metadata(&resolved.staging)
            .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink());
        if is_directory {
            if let Err(error) = fs::remove_dir_all(&resolved.staging) {
                log::warn!(
                    "could not remove unpublished compaction staging {}: {error}; the next open \
                     or compaction of the store removes it",
                    resolved.staging.display()
                );
            }
        }
    }
}

/// Whether `location` holds exactly the generation captured as `inventory` and `bytes`: the same
/// names, each file's length and CRC-32 (`generation_matches`), and in each file the very bytes
/// captured. A matching CRC-32 is no proof that the bytes are the same (specs/015, fifth review).
fn generation_is_exactly(
    location: &Path,
    inventory: &[ArtifactDescriptor],
    bytes: &BTreeMap<PathBuf, Vec<u8>>,
) -> bool {
    recovery::generation_matches(location, inventory)
        && inventory.iter().all(|descriptor| {
            let (Some(name), Some(captured)) = (
                descriptor.relative_path.file_name(),
                bytes.get(&descriptor.relative_path),
            ) else {
                return false;
            };
            exact_artifact_bytes_match(&location.join(name), captured, descriptor.checksum)
                .unwrap_or(false)
        })
}

#[allow(dead_code)]
pub(crate) fn prepare_closed_staging(
    store_dir: &Path,
    options: ClosedCompactionOptions,
) -> Result<PreparedClosedStaging, CompactionError> {
    let (prepared, mut unpublished) = prepare_closed_staging_guarded(store_dir, options)?;
    unpublished.keep_staging();
    Ok(prepared)
}

/// `prepare_closed_staging`, returning the staging with its armed guard.
pub(crate) fn prepare_closed_staging_guarded(
    store_dir: &Path,
    options: ClosedCompactionOptions,
) -> Result<(PreparedClosedStaging, UnpublishedClosedStaging), CompactionError> {
    if options.durability_policy() == DurabilityPolicy::Physical {
        crate::durability::validate_compile_target()
            .map_err(|source| CompactionError::UnsupportedDurability { source })?;
    }
    let capture = capture_closed_generation(store_dir)?;
    let paths = directory_artifact_paths(store_dir).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::WriteStaging,
        path: store_dir.to_path_buf(),
        source,
    })?;
    // Resolved before the staging directory is created and created through that resolution, so
    // the guard names the entry this call created whatever the working directory does next.
    let resolved = ResolvedClosedAttempt::resolve(&paths, &capture.source_dir);
    let created_at = resolved
        .as_ref()
        .map_or(paths.staging.as_path(), |resolved| {
            resolved.staging.as_path()
        });
    fs::create_dir(created_at).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::WriteStaging,
        path: paths.staging.clone(),
        source,
    })?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::Prepared,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::StagingCreate,
        },
    );
    let unpublished = UnpublishedClosedStaging::new(&paths, resolved, &capture);
    let replacement_inventory =
        write_staging_families(&paths, &capture.families, options.durability_policy())?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::Prepared,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::StagingSync,
        },
    );
    Ok((
        PreparedClosedStaging {
            capture,
            paths,
            replacement_inventory,
        },
        unpublished,
    ))
}

#[allow(dead_code)]
pub(crate) fn validate_closed_staging(
    prepared: &PreparedClosedStaging,
) -> Result<ValidatedClosedStaging, CompactionError> {
    let anchor = prepared
        .paths
        .staging
        .parent()
        .ok_or_else(|| CompactionError::Io {
            operation: CompactionOperation::ValidateStaging,
            path: prepared.paths.staging.clone(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "staging directory has no parent anchor",
            ),
        })?;
    for descriptor in &prepared.replacement_inventory {
        verify_descriptor(anchor, descriptor).map_err(|_| CompactionError::InvalidArtifact {
            path: anchor.join(&descriptor.relative_path),
        })?;
    }
    let staging =
        capture_closed_generation(&prepared.paths.staging).map_err(|error| match error {
            CompactionError::Io { path, source, .. } => CompactionError::Io {
                operation: CompactionOperation::ValidateStaging,
                path,
                source,
            },
            error => error,
        })?;
    compare_captured_families(&prepared.capture.families, &staging.families)?;
    reopen_and_compare_public_state(&prepared.paths.staging, &prepared.capture.families)?;
    Ok(ValidatedClosedStaging { staging })
}

pub(crate) fn validate_published_closed_replacement(
    prepared: &PreparedClosedStaging,
) -> Result<CapturedGeneration, CompactionError> {
    let anchor = prepared
        .capture
        .source_dir
        .parent()
        .ok_or_else(|| CompactionError::Io {
            operation: CompactionOperation::ReopenReplacement,
            path: prepared.capture.source_dir.clone(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "published replacement has no parent anchor",
            ),
        })?;
    let source_name =
        prepared
            .capture
            .source_dir
            .file_name()
            .ok_or_else(|| CompactionError::Io {
                operation: CompactionOperation::ReopenReplacement,
                path: prepared.capture.source_dir.clone(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "published replacement has no native file name",
                ),
            })?;
    for descriptor in &prepared.replacement_inventory {
        let active_name = descriptor.relative_path.file_name().ok_or_else(|| {
            CompactionError::InvalidArtifact {
                path: descriptor.relative_path.clone(),
            }
        })?;
        let mut canonical = descriptor.clone();
        canonical.relative_path = PathBuf::from(source_name).join(active_name);
        verify_descriptor(anchor, &canonical).map_err(|_| CompactionError::InvalidArtifact {
            path: anchor.join(&canonical.relative_path),
        })?;
    }
    let replacement = capture_closed_generation_trusted(&prepared.capture.source_dir)?;
    compare_captured_families(&prepared.capture.families, &replacement.families)?;
    Ok(replacement)
}

fn compare_captured_families(
    source: &[CapturedFamily],
    replacement: &[CapturedFamily],
) -> Result<(), CompactionError> {
    if replacement.len() != source.len() {
        return Err(staging_mismatch("family count"));
    }
    for (source, replacement) in source.iter().zip(replacement) {
        if source.family != replacement.family || replacement.sealed_segment_count != 0 {
            return Err(staging_mismatch("family identity"));
        }
        if source.state != replacement.state {
            return Err(staging_mismatch("logical state"));
        }
        if source.granularity_nanos != replacement.granularity_nanos
            || source.last_bucket != replacement.last_bucket
        {
            return Err(staging_mismatch("timestamp metadata"));
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn revalidate_closed_source_inventory(
    prepared: &PreparedClosedStaging,
) -> Result<(), CompactionError> {
    let source_name = prepared
        .capture
        .source_dir
        .file_name()
        .ok_or_else(|| source_revalidation_error("directory has no native file name"))?;
    let mut current = BTreeMap::new();
    let entries =
        fs::read_dir(&prepared.capture.source_dir).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Capture,
            path: prepared.capture.source_dir.clone(),
            source,
        })?;
    for entry in entries {
        let entry = entry.map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Capture,
            path: prepared.capture.source_dir.clone(),
            source,
        })?;
        let file_type = entry.file_type().map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Capture,
            path: entry.path(),
            source,
        })?;
        if crate::maintenance_coordination::is_inner_lock_file(&entry.file_name(), file_type) {
            continue;
        }
        if !file_type.is_file() {
            return Err(source_revalidation_error(
                "source contains a non-file artifact",
            ));
        }
        let length = entry
            .metadata()
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Capture,
                path: entry.path(),
                source,
            })?
            .len();
        current.insert(PathBuf::from(source_name).join(entry.file_name()), length);
    }
    let expected = prepared
        .capture
        .inventory
        .iter()
        .map(|descriptor| (descriptor.relative_path.clone(), descriptor.length))
        .collect::<BTreeMap<_, _>>();
    if current != expected {
        return Err(source_revalidation_error(
            "native inventory or artifact length changed after capture",
        ));
    }
    let anchor = prepared
        .capture
        .source_dir
        .parent()
        .ok_or_else(|| source_revalidation_error("directory has no parent anchor"))?;
    for descriptor in &prepared.capture.inventory {
        let expected_bytes = prepared
            .capture
            .source_bytes
            .get(&descriptor.relative_path)
            .ok_or_else(|| source_revalidation_error("captured bytes are incomplete"))?;
        let path = anchor.join(&descriptor.relative_path);
        let matches = exact_artifact_bytes_match(&path, expected_bytes, descriptor.checksum)
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::Capture,
                path,
                source,
            })?;
        if !matches {
            return Err(source_revalidation_error(
                "artifact checksum or exact bytes changed after capture",
            ));
        }
    }
    Ok(())
}

fn source_revalidation_error(detail: &str) -> CompactionError {
    CompactionError::FailedClosed {
        detail: format!("closed source changed before publication: {detail}"),
    }
}

fn staging_mismatch(field: &str) -> CompactionError {
    CompactionError::FailedClosed {
        detail: format!("validated staging {field} does not match captured source"),
    }
}

fn reopen_and_compare_public_state(
    staging: &Path,
    families: &[CapturedFamily],
) -> Result<(), CompactionError> {
    for family in families {
        let matches = match &family.state {
            CapturedLogicalState::Value(expected) => {
                let store =
                    crate::key_value_store::DurableKeyValueStore::try_init_new_under_closed_claim(
                        staging,
                    )
                    .map_err(|error| staging_reopen_error(staging, error.to_string()))?
                    .into_store();
                store.size() == expected.len()
                    && expected
                        .iter()
                        .all(|(key, value)| store.get(key) == Some(value.clone()))
            }
            CapturedLogicalState::Set(expected) => {
                let store =
                    crate::key_set_store::DurableKeySetStore::try_init_new_under_closed_claim(
                        staging,
                    )
                    .map_err(|error| staging_reopen_error(staging, error.to_string()))?
                    .into_store();
                store.size() == expected.len()
                    && expected
                        .iter()
                        .all(|(key, values)| store.get_hashset(key).as_ref() == Some(values))
            }
            CapturedLogicalState::Map(expected) => {
                let store =
                    crate::key_map_store::DurableKeyMapStore::try_init_new_under_closed_claim(
                        staging,
                    )
                    .map_err(|error| staging_reopen_error(staging, error.to_string()))?
                    .into_store();
                store.size() == expected.len()
                    && expected
                        .iter()
                        .all(|(key, map)| store.get_sorted_map(key).as_ref() == Some(map))
            }
        };
        if !matches {
            return Err(staging_mismatch("public logical state"));
        }
    }
    Ok(())
}

fn staging_reopen_error(staging: &Path, detail: String) -> CompactionError {
    CompactionError::FailedClosed {
        detail: format!("staging reopen failed for {}: {detail}", staging.display()),
    }
}

fn capture_closed_generation(store_dir: &Path) -> Result<CapturedGeneration, CompactionError> {
    let inspection = inspect_directory(store_dir).map_err(|error| {
        crate::maintenance::map_inspection_error(store_dir.to_path_buf(), error)
    })?;
    capture_inspected_generation(store_dir, inspection)
}

fn capture_closed_generation_trusted(
    store_dir: &Path,
) -> Result<CapturedGeneration, CompactionError> {
    let inspection = inspect_generation(store_dir).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::ReopenReplacement,
        path: store_dir.to_path_buf(),
        source,
    })?;
    capture_inspected_generation(store_dir, inspection)
}

fn capture_inspected_generation(
    store_dir: &Path,
    inspection: crate::compaction::inspection::DirectoryInspection,
) -> Result<CapturedGeneration, CompactionError> {
    let source_name = store_dir.file_name().ok_or_else(|| CompactionError::Io {
        operation: CompactionOperation::Capture,
        path: store_dir.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            "closed compaction directory has no native file name",
        ),
    })?;
    let mut inventory = Vec::new();
    let mut source_bytes = BTreeMap::new();
    let mut families = Vec::with_capacity(inspection.families.len());
    for family in inspection.families {
        families.push(capture_family(
            store_dir,
            source_name,
            &family,
            &mut inventory,
            &mut source_bytes,
        )?);
    }
    Ok(CapturedGeneration {
        source_dir: store_dir.to_path_buf(),
        inventory,
        source_bytes: Arc::new(source_bytes),
        families,
    })
}

fn capture_family(
    store_dir: &Path,
    source_name: &std::ffi::OsStr,
    inspection: &FamilyInspection,
    inventory: &mut Vec<ArtifactDescriptor>,
    source_bytes: &mut BTreeMap<PathBuf, Vec<u8>>,
) -> Result<CapturedFamily, CompactionError> {
    let family: StoreFamily = inspection.family.into();
    let active_name = inspection.family.active_name();
    let mut chain = Vec::new();
    for segment in 0..inspection.sealed_segment_count {
        let name = format!("{active_name}.segment-{segment:020}");
        capture_artifact(
            store_dir,
            source_name,
            Path::new(&name),
            ArtifactRole::SealedSegment,
            family,
            inventory,
            source_bytes,
            &mut chain,
        )?;
    }
    capture_artifact(
        store_dir,
        source_name,
        Path::new(active_name),
        ArtifactRole::Active,
        family,
        inventory,
        source_bytes,
        &mut chain,
    )?;
    let (state, granularity_nanos, last_bucket) =
        replay_captured_state(inspection.family, &chain, store_dir)?;
    Ok(CapturedFamily {
        family,
        state,
        granularity_nanos,
        last_bucket,
        before_bytes: inspection.total_bytes,
        sealed_segment_count: inspection.sealed_segment_count,
    })
}

/// What replaying `chain` as `family`'s WAL recovers, with its timestamp metadata. A torn tail is
/// accepted, as an open accepts it; a chain the replay rejects is `InvalidArtifact` at `path`.
fn replay_captured_state(
    family: InspectedFamily,
    chain: &[u8],
    path: &Path,
) -> Result<(CapturedLogicalState, u64, u64), CompactionError> {
    Ok(match family {
        InspectedFamily::KeyValue => {
            let replay = accepted_tail(replay_key_value_tail(chain), path)?;
            (
                CapturedLogicalState::Value(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
        InspectedFamily::KeySet => {
            let replay = accepted_tail(replay_key_set_tail(chain), path)?;
            (
                CapturedLogicalState::Set(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
        InspectedFamily::KeyMap => {
            let replay = accepted_tail(replay_key_map_tail(chain), path)?;
            (
                CapturedLogicalState::Map(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
    })
}

/// Whether removing a closed staging directory loses nothing (specs/015 FR-3, after its fourth
/// and fifth reviews): every staged file is either byte for byte what a closed compaction of the
/// canonical directory stages for its family, or what a write of those bytes stopped part-way
/// leaves -- their first bytes, ending inside the header or inside a record.
///
/// A closed compaction stages, for each family, the state the canonical chain recovers, encoded
/// with that chain's timestamp metadata by an encoder that writes keys, members and entries in
/// sorted order (`encode_captured_state`). So a staged file holding exactly those bytes recovers
/// exactly what the canonical directory recovers, and removing it loses nothing. That is narrower
/// than equality of recovered states, the third review's rule: a staged file recovering the same
/// state in other bytes (a byte copy of a canonical file, say) proves nothing here and keeps its
/// error. It is also what bounds the cost by an open's own (plan IV): each family's canonical
/// state is replayed once, encoded, and dropped before the staged file is read a block at a time,
/// so no staged state is ever built.
///
/// A canonical key whose set has no members, or whose map has no entries, proves nothing: an open
/// publishes such a key, while its encoding is that of the key's absence, which is how a staged
/// snapshot records a delete.
///
/// A staged file that is the first bytes of those, ending inside the header or inside a record, is
/// what a staging write killed part-way through leaves (fifth review). Removing it loses nothing,
/// for two reasons that need each other. Its bytes, the gaps between its complete records
/// included, are the current encoding's own, so every absence it records -- a key sorting between
/// two of its complete records -- is an absence in the canonical state too: it is the byte
/// comparison, not the tear, that makes its deletions the canonical directory's. And its last
/// record (or its header) is cut, so it is no complete snapshot of any state and records nothing
/// about the keys beyond the cut. A comparison of only the facts in its complete records would
/// lose a delete. One that ends right after the header or on a record boundary is a complete
/// snapshot of a state with fewer facts, which records the rest as deleted, and cannot be told
/// from a write stopped there: it proves nothing. Nor does any other short file (a headerless
/// legacy file, say), which compaction never writes.
///
/// The canonical directory's `inspection` and the staged files' names were checked by the caller;
/// a read that fails answers `false`, since nothing is then proved.
pub(super) fn staging_is_what_compaction_stages_or_a_torn_write_of_it(
    store_dir: &Path,
    staging: &Path,
    canonical: &inspection::DirectoryInspection,
) -> bool {
    let Ok(entries) = fs::read_dir(staging) else {
        return false;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            return false;
        };
        let Some(family) = canonical
            .families
            .iter()
            .find(|family| entry.file_name() == family.family.active_name())
        else {
            return false;
        };
        let path = entry.path();
        // A regular file, not followed through a link: the caller checked this too, and the reads
        // below follow links.
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        let Some(expected) = staged_bytes_for(store_dir, family) else {
            return false;
        };
        if !file_is_all_or_a_torn_write_of(&path, &expected) {
            return false;
        }
    }
    true
}

/// What a closed compaction of `store_dir` stages for `family` now, or `None` when that cannot be
/// computed or would not record the canonical state (a read fails, the replay rejects the chain,
/// or a key has no members or entries).
fn staged_bytes_for(store_dir: &Path, family: &FamilyInspection) -> Option<Vec<u8>> {
    let (state, granularity_nanos, last_bucket) = recovered_family_state(store_dir, family).ok()?;
    if state.has_a_key_with_nothing_in_it() {
        return None;
    }
    encode_captured_state(&state, granularity_nanos, last_bucket).ok()
}

/// Whether the file at `path` holds exactly `expected`, a current-format snapshot, or its first
/// bytes ending inside its header or inside one of its records, read a block at a time.
fn file_is_all_or_a_torn_write_of(path: &Path, expected: &[u8]) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut block = vec![0; 64 * 1024];
    let mut offset = 0;
    loop {
        match file.read(&mut block) {
            Ok(0) => return offset == expected.len() || ends_inside_a_record(expected, offset),
            Ok(read) => {
                let end = offset + read;
                if end > expected.len() || block[..read] != expected[offset..end] {
                    return false;
                }
                offset = end;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }
}

/// Whether the first `len` bytes of `encoded`, a current-format snapshot, end inside its header or
/// inside one of its records, rather than right after its header or after a record. Only the
/// record lengths that `encoded` declares are read.
fn ends_inside_a_record(encoded: &[u8], len: usize) -> bool {
    use crate::wal::format::V2CodecProbe;
    if len < V2CodecProbe::HEADER_LEN {
        return true;
    }
    let mut end = V2CodecProbe::HEADER_LEN;
    while end < len {
        let Some(record_end) = encoded
            .get(end..)
            .and_then(V2CodecProbe::record_encoded_len)
            .and_then(|record_len| end.checked_add(record_len))
            .filter(|record_end| *record_end <= encoded.len())
        else {
            return false;
        };
        end = record_end;
    }
    end != len
}

/// What an open of `store_dir` recovers for `family`, with its timestamp metadata: its sealed
/// segments, then its active file, as `capture_family` reads them. The chain's bytes are dropped
/// before this returns.
fn recovered_family_state(
    store_dir: &Path,
    family: &FamilyInspection,
) -> Result<(CapturedLogicalState, u64, u64), CompactionError> {
    let active_name = family.family.active_name();
    let mut chain = Vec::new();
    let names = (0..family.sealed_segment_count)
        .map(|segment| format!("{active_name}.segment-{segment:020}"))
        .chain(std::iter::once(active_name.to_owned()));
    for name in names {
        let path = store_dir.join(name);
        let bytes = fs::read(&path).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::Inspect,
            path,
            source,
        })?;
        chain.extend_from_slice(&bytes);
    }
    replay_captured_state(family.family, &chain, store_dir)
}

impl CapturedLogicalState {
    /// Whether a key's set has no members, or its map no entries: a key an open publishes and
    /// a snapshot encoding cannot record.
    fn has_a_key_with_nothing_in_it(&self) -> bool {
        match self {
            Self::Value(_) => false,
            Self::Set(snapshot) => snapshot.values().any(|members| members.is_empty()),
            Self::Map(snapshot) => snapshot.values().any(|entries| entries.is_empty()),
        }
    }
}

fn capture_online_family(
    store_dir: &Path,
    inspection: &FamilyInspection,
    live_state: CapturedLogicalState,
    metadata: crate::wal::OnlineCaptureMetadata,
) -> Result<(CapturedFamily, Vec<ArtifactDescriptor>), CompactionError> {
    let family: StoreFamily = inspection.family.into();
    let active_name = inspection.family.active_name();
    let mut inventory = Vec::with_capacity(inspection.sealed_segment_count + 1);
    let mut chain = Vec::new();
    for segment in 0..inspection.sealed_segment_count {
        let name = format!("{active_name}.segment-{segment:020}");
        capture_online_artifact(
            store_dir,
            Path::new(&name),
            ArtifactRole::SealedSegment,
            family,
            &mut inventory,
            &mut chain,
        )?;
    }
    capture_online_artifact(
        store_dir,
        Path::new(active_name),
        ArtifactRole::Active,
        family,
        &mut inventory,
        &mut chain,
    )?;

    let active = inventory
        .last()
        .expect("online capture always includes an active artifact");
    let measured_total = inventory.iter().try_fold(0_u64, |total, descriptor| {
        total
            .checked_add(descriptor.length)
            .ok_or_else(|| CompactionError::InvalidArtifact {
                path: store_dir.to_path_buf(),
            })
    })?;
    if active.length != metadata.active_len || measured_total != inspection.total_bytes {
        return Err(CompactionError::InvalidArtifact {
            path: store_dir.join(active_name),
        });
    }

    let (source_state, granularity_nanos, last_bucket) = match inspection.family {
        InspectedFamily::KeyValue => {
            let replay = accepted_tail(replay_key_value_tail(&chain), store_dir)?;
            (
                CapturedLogicalState::Value(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
        InspectedFamily::KeySet => {
            let replay = accepted_tail(replay_key_set_tail(&chain), store_dir)?;
            (
                CapturedLogicalState::Set(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
        InspectedFamily::KeyMap => {
            let replay = accepted_tail(replay_key_map_tail(&chain), store_dir)?;
            (
                CapturedLogicalState::Map(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
    };
    if source_state != live_state
        || granularity_nanos != metadata.granularity_nanos
        || last_bucket != metadata.last_bucket
    {
        return Err(CompactionError::FailedClosed {
            detail: "open logical state or timestamp metadata diverged from its WAL".to_owned(),
        });
    }

    Ok((
        CapturedFamily {
            family,
            state: live_state,
            granularity_nanos,
            last_bucket,
            before_bytes: inspection.total_bytes,
            sealed_segment_count: inspection.sealed_segment_count,
        },
        inventory,
    ))
}

fn capture_online_artifact(
    store_dir: &Path,
    name: &Path,
    role: ArtifactRole,
    family: StoreFamily,
    inventory: &mut Vec<ArtifactDescriptor>,
    chain: &mut Vec<u8>,
) -> Result<(), CompactionError> {
    let path = store_dir.join(name);
    let bytes = fs::read(&path).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Capture,
        path: path.clone(),
        source,
    })?;
    let length = u64::try_from(bytes.len())
        .map_err(|_| CompactionError::InvalidArtifact { path: path.clone() })?;
    inventory.push(ArtifactDescriptor {
        relative_path: name.to_path_buf(),
        role,
        family: Some(family),
        length,
        checksum: crc32fast::hash(&bytes),
    });
    chain.extend_from_slice(&bytes);
    Ok(())
}

fn accepted_tail<S>(
    replay: TailReplay<S>,
    path: &Path,
) -> Result<ReplaySnapshot<S>, CompactionError> {
    match replay {
        TailReplay::Complete(replay) | TailReplay::RecoverableTail { replay, .. } => Ok(replay),
        TailReplay::Invalid(_) => Err(CompactionError::InvalidArtifact {
            path: path.to_path_buf(),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn capture_artifact(
    store_dir: &Path,
    source_name: &std::ffi::OsStr,
    name: &Path,
    role: ArtifactRole,
    family: StoreFamily,
    inventory: &mut Vec<ArtifactDescriptor>,
    source_bytes: &mut BTreeMap<PathBuf, Vec<u8>>,
    chain: &mut Vec<u8>,
) -> Result<(), CompactionError> {
    let path = store_dir.join(name);
    let bytes = fs::read(&path).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::Capture,
        path: path.clone(),
        source,
    })?;
    let length = u64::try_from(bytes.len())
        .map_err(|_| CompactionError::InvalidArtifact { path: path.clone() })?;
    let relative_path = PathBuf::from(source_name).join(name);
    inventory.push(ArtifactDescriptor {
        relative_path: relative_path.clone(),
        role,
        family: Some(family),
        length,
        checksum: crc32fast::hash(&bytes),
    });
    source_bytes.insert(relative_path, bytes.clone());
    chain.extend_from_slice(&bytes);
    Ok(())
}

fn write_staging_families(
    paths: &MaintenanceArtifactPaths,
    families: &[CapturedFamily],
    durability: DurabilityPolicy,
) -> Result<Vec<ArtifactDescriptor>, CompactionError> {
    let staging_name = paths
        .staging
        .file_name()
        .ok_or_else(|| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: paths.staging.clone(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "staging directory has no native file name",
            ),
        })?;
    let mut inventory = Vec::with_capacity(families.len());
    for family in families {
        let encoded = encode_captured_family(family).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: paths.staging.clone(),
            source,
        })?;
        let active_name = active_name(family.family);
        let path = paths.staging.join(active_name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::WriteStaging,
                path: path.clone(),
                source,
            })?;
        // Mid-write: the first family's file exists and holds nothing yet, or, killed inside its
        // bytes, all of them but the last (specs/015).
        #[cfg(test)]
        if families
            .first()
            .is_some_and(|first| std::ptr::eq(first, family))
        {
            use crate::test_support::fault_checkpoint::{
                exit_at_maintenance_fault, maintenance_fault_requested, MaintenanceCut,
                MaintenanceFaultPoint, MaintenancePhase,
            };
            exit_at_maintenance_fault(MaintenanceFaultPoint {
                phase: MaintenancePhase::Prepared,
                cut: MaintenanceCut::StagingWrite,
            });
            let torn = MaintenanceFaultPoint {
                phase: MaintenancePhase::Prepared,
                cut: MaintenanceCut::StagingWriteTorn,
            };
            if maintenance_fault_requested(torn) {
                let _ = file
                    .write_all(&encoded[..encoded.len() - 1])
                    .and_then(|()| file.flush());
                exit_at_maintenance_fault(torn);
                let _ = io::Seek::rewind(&mut file);
            }
        }
        file.write_all(&encoded)
            .and_then(|()| file.flush())
            .and_then(|()| {
                if durability == DurabilityPolicy::Physical {
                    file.sync_all()
                } else {
                    Ok(())
                }
            })
            .map_err(|source| CompactionError::Io {
                operation: CompactionOperation::WriteStaging,
                path: path.clone(),
                source,
            })?;
        inventory.push(ArtifactDescriptor {
            relative_path: PathBuf::from(staging_name).join(active_name),
            role: ArtifactRole::ReplacementPrefix,
            family: Some(family.family),
            length: u64::try_from(encoded.len())
                .map_err(|_| CompactionError::InvalidArtifact { path: path.clone() })?,
            checksum: crc32fast::hash(&encoded),
        });
    }
    if durability == DurabilityPolicy::Physical {
        #[cfg(not(target_os = "windows"))]
        {
            crate::durability::synchronize_directory(&paths.staging).map_err(|source| {
                CompactionError::Io {
                    operation: CompactionOperation::WriteStaging,
                    path: paths.staging.clone(),
                    source,
                }
            })?;
            let parent = paths.staging.parent().ok_or_else(|| CompactionError::Io {
                operation: CompactionOperation::WriteStaging,
                path: paths.staging.clone(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "staging directory has no parent",
                ),
            })?;
            crate::durability::synchronize_directory(parent).map_err(|source| {
                CompactionError::Io {
                    operation: CompactionOperation::WriteStaging,
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }
    }
    Ok(inventory)
}

fn encode_captured_family(family: &CapturedFamily) -> io::Result<Vec<u8>> {
    encode_captured_state(&family.state, family.granularity_nanos, family.last_bucket)
}

/// The staged file a closed compaction writes for `state` with this timestamp metadata.
fn encode_captured_state(
    state: &CapturedLogicalState,
    granularity_nanos: u64,
    last_bucket: u64,
) -> io::Result<Vec<u8>> {
    let encoded = match state {
        CapturedLogicalState::Value(snapshot) => encode_current_key_value_snapshot_with_metadata(
            snapshot,
            granularity_nanos,
            last_bucket,
        ),
        CapturedLogicalState::Set(snapshot) => {
            encode_current_key_set_snapshot_with_metadata(snapshot, granularity_nanos, last_bucket)
        }
        CapturedLogicalState::Map(snapshot) => {
            encode_current_key_map_snapshot_with_metadata(snapshot, granularity_nanos, last_bucket)
        }
    };
    encoded.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn capture_validated_online_staging(
    staging_path: &Path,
    family: StoreFamily,
) -> Result<CapturedFamily, CompactionError> {
    let bytes = fs::read(staging_path).map_err(|source| CompactionError::Io {
        operation: CompactionOperation::ValidateStaging,
        path: staging_path.to_path_buf(),
        source,
    })?;
    let (state, granularity_nanos, last_bucket) = match family {
        StoreFamily::KeyValue => {
            let replay = complete_staging(replay_key_value_tail(&bytes), staging_path)?;
            (
                CapturedLogicalState::Value(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
        StoreFamily::KeySet => {
            let replay = complete_staging(replay_key_set_tail(&bytes), staging_path)?;
            (
                CapturedLogicalState::Set(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
        StoreFamily::KeyMap => {
            let replay = complete_staging(replay_key_map_tail(&bytes), staging_path)?;
            (
                CapturedLogicalState::Map(replay.snapshot),
                replay.granularity_nanos,
                replay.last_bucket,
            )
        }
    };
    Ok(CapturedFamily {
        family,
        state,
        granularity_nanos,
        last_bucket,
        before_bytes: u64::try_from(bytes.len()).map_err(|_| CompactionError::InvalidArtifact {
            path: staging_path.to_path_buf(),
        })?,
        sealed_segment_count: 0,
    })
}

fn complete_staging<S>(
    replay: TailReplay<S>,
    path: &Path,
) -> Result<ReplaySnapshot<S>, CompactionError> {
    match replay {
        TailReplay::Complete(replay) => Ok(replay),
        TailReplay::RecoverableTail { .. } | TailReplay::Invalid(_) => {
            Err(CompactionError::InvalidArtifact {
                path: path.to_path_buf(),
            })
        }
    }
}

fn active_name(family: StoreFamily) -> &'static str {
    match family {
        StoreFamily::KeyValue => "kv.wal.dat",
        StoreFamily::KeySet => "set.wal.dat",
        StoreFamily::KeyMap => "map.wal.dat",
    }
}

/// A claim or lock failure: `FailedClosed` when another owner holds the directory, else I/O.
fn claim_lock_error(store_dir: &Path, source: std::io::Error) -> CompactionError {
    if source.kind() == std::io::ErrorKind::WouldBlock {
        CompactionError::FailedClosed {
            detail: source.to_string(),
        }
    } else {
        CompactionError::Io {
            operation: crate::CompactionOperation::Inspect,
            path: store_dir.to_path_buf(),
            source,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn compact_closed_directory(
    store_dir: &Path,
    options: ClosedCompactionOptions,
) -> Result<DirectoryCompactionOutcome, CompactionError> {
    let _claim = crate::maintenance_coordination::try_claim_closed(store_dir)
        .map_err(|source| claim_lock_error(store_dir, source))?;
    let _ = recovery::resolve_directory_maintenance_for_compaction(store_dir, _claim.identity())?;
    _claim
        .ensure_inner_lock()
        .map_err(|source| claim_lock_error(store_dir, source))?;
    let inspection = crate::inspect_storage(store_dir)?;
    if inspection.families().is_empty() {
        return Ok(DirectoryCompactionOutcome::empty());
    }
    // Declared after the claim, so it is dropped first, while the claim still holds the
    // replacement lock (and the inner lock, until it is retired below).
    let (prepared, mut unpublished) = prepare_closed_staging_guarded(store_dir, options)?;
    validate_closed_staging(&prepared)?;
    // Before the source is revalidated or any manifest published, so the directory being replaced
    // holds exactly the captured store files (specs/011 FR-4).
    _claim
        .retire_inner_lock()
        .map_err(|source| claim_lock_error(store_dir, source))?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::Prepared,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::StagingValidate,
        },
    );
    let mut manifest = publish_closed_prepared(&prepared, options.durability_policy())?;
    unpublished.keep_staging();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(()))?;
    publish_closed_replacement_with_checkpoint(&prepared, &mut manifest, |_| Ok(()))?;
    let cleanup = cleanup_closed_with_checkpoint(&prepared, &mut manifest, |_| Ok(()))?;

    let mut outcomes = Vec::with_capacity(prepared.capture.families.len());
    for family in &prepared.capture.families {
        let after_bytes = prepared
            .replacement_inventory
            .iter()
            .filter(|descriptor| descriptor.family == Some(family.family))
            .try_fold(0_u64, |total, descriptor| {
                total
                    .checked_add(descriptor.length)
                    .ok_or_else(|| CompactionError::FailedClosed {
                        detail: "compacted family byte total overflowed u64".to_owned(),
                    })
            })?;
        outcomes.push(FamilyCompactionOutcome::closed(
            family.family,
            family.before_bytes,
            after_bytes,
            family.sealed_segment_count,
            cleanup,
        ));
    }
    Ok(DirectoryCompactionOutcome::from_families(outcomes))
}

#[cfg(test)]
mod closed_tests;
#[cfg(test)]
mod inspection_tests;
#[cfg(test)]
mod online_attempt_recovery_tests;
#[cfg(test)]
mod online_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod unpublished_attempt_tests;

#[cfg(test)]
pub(crate) fn test_sentinel() {
    inspection::test_sentinel();
    manifest::test_sentinel();
    publication::test_sentinel();
    recovery::test_sentinel();
}
