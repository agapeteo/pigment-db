//! Private storage-compaction implementation.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::compaction::inspection::{
    exact_artifact_bytes_match, inspect_directory, inspect_generation, FamilyInspection,
    InspectedFamily,
};
use crate::compaction::manifest::{verify_descriptor, ArtifactDescriptor, ArtifactRole};
use crate::compaction::publication::{
    cleanup_closed_with_checkpoint, directory_artifact_paths, publish_closed_prepared,
    publish_closed_previous_with_checkpoint, publish_closed_replacement_with_checkpoint,
    MaintenanceArtifactPaths,
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
    pub(crate) source_bytes: BTreeMap<PathBuf, Vec<u8>>,
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

#[allow(dead_code)]
pub(crate) fn prepare_closed_staging(
    store_dir: &Path,
    options: ClosedCompactionOptions,
) -> Result<PreparedClosedStaging, CompactionError> {
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
    fs::create_dir(&paths.staging).map_err(|source| CompactionError::Io {
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
    let replacement_inventory =
        match write_staging_families(&paths, &capture.families, options.durability_policy()) {
            Ok(inventory) => inventory,
            Err(error) => {
                let _ = fs::remove_dir_all(&paths.staging);
                return Err(error);
            }
        };
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::Prepared,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::StagingWrite,
        },
    );
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::Prepared,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::StagingSync,
        },
    );
    Ok(PreparedClosedStaging {
        capture,
        paths,
        replacement_inventory,
    })
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
                let store = crate::key_value_store::DurableKeyValueStore::try_init_new(staging)
                    .map_err(|error| staging_reopen_error(staging, error.to_string()))?
                    .into_store();
                store.size() == expected.len()
                    && expected
                        .iter()
                        .all(|(key, value)| store.get(key) == Some(value.clone()))
            }
            CapturedLogicalState::Set(expected) => {
                let store = crate::key_set_store::DurableKeySetStore::try_init_new(staging)
                    .map_err(|error| staging_reopen_error(staging, error.to_string()))?
                    .into_store();
                store.size() == expected.len()
                    && expected
                        .iter()
                        .all(|(key, values)| store.get_hashset(key).as_ref() == Some(values))
            }
            CapturedLogicalState::Map(expected) => {
                let store = crate::key_map_store::DurableKeyMapStore::try_init_new(staging)
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
        source_bytes,
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
    let (state, granularity_nanos, last_bucket) = match inspection.family {
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
    Ok(CapturedFamily {
        family,
        state,
        granularity_nanos,
        last_bucket,
        before_bytes: inspection.total_bytes,
        sealed_segment_count: inspection.sealed_segment_count,
    })
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
        crate::durability::synchronize_directory(parent).map_err(|source| CompactionError::Io {
            operation: CompactionOperation::WriteStaging,
            path: parent.to_path_buf(),
            source,
        })?;
    }
    Ok(inventory)
}

fn encode_captured_family(family: &CapturedFamily) -> io::Result<Vec<u8>> {
    let encoded = match &family.state {
        CapturedLogicalState::Value(snapshot) => encode_current_key_value_snapshot_with_metadata(
            snapshot,
            family.granularity_nanos,
            family.last_bucket,
        ),
        CapturedLogicalState::Set(snapshot) => encode_current_key_set_snapshot_with_metadata(
            snapshot,
            family.granularity_nanos,
            family.last_bucket,
        ),
        CapturedLogicalState::Map(snapshot) => encode_current_key_map_snapshot_with_metadata(
            snapshot,
            family.granularity_nanos,
            family.last_bucket,
        ),
    };
    encoded.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn active_name(family: StoreFamily) -> &'static str {
    match family {
        StoreFamily::KeyValue => "kv.wal.dat",
        StoreFamily::KeySet => "set.wal.dat",
        StoreFamily::KeyMap => "map.wal.dat",
    }
}

#[allow(dead_code)]
pub(crate) fn compact_closed_directory(
    store_dir: &Path,
    options: ClosedCompactionOptions,
) -> Result<DirectoryCompactionOutcome, CompactionError> {
    let _claim =
        crate::maintenance_coordination::try_claim_closed(store_dir).map_err(|source| {
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
        })?;
    let _ = recovery::resolve_directory_maintenance_for_compaction(store_dir)?;
    let inspection = crate::inspect_storage(store_dir)?;
    if inspection.families().is_empty() {
        return Ok(DirectoryCompactionOutcome::empty());
    }
    let prepared = prepare_closed_staging(store_dir, options)?;
    validate_closed_staging(&prepared)?;
    #[cfg(test)]
    crate::test_support::fault_checkpoint::exit_at_maintenance_fault(
        crate::test_support::fault_checkpoint::MaintenanceFaultPoint {
            phase: crate::test_support::fault_checkpoint::MaintenancePhase::Prepared,
            cut: crate::test_support::fault_checkpoint::MaintenanceCut::StagingValidate,
        },
    );
    let mut manifest = publish_closed_prepared(&prepared, options.durability_policy())?;
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
mod online_tests;
#[cfg(test)]
mod recovery_tests;

#[cfg(test)]
pub(crate) fn test_sentinel() {
    inspection::test_sentinel();
    manifest::test_sentinel();
    publication::test_sentinel();
    recovery::test_sentinel();
}
