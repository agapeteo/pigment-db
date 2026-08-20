//! Private storage-compaction implementation.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::compaction::inspection::{inspect_directory, FamilyInspection, InspectedFamily};
use crate::compaction::manifest::{ArtifactDescriptor, ArtifactRole};
use crate::compaction::publication::{directory_artifact_paths, MaintenanceArtifactPaths};
use crate::wal::replay::{
    encode_current_key_map_snapshot_with_metadata, encode_current_key_set_snapshot_with_metadata,
    encode_current_key_value_snapshot_with_metadata, replay_key_map_tail, replay_key_set_tail,
    replay_key_value_tail, KeyMapSnapshot, KeySetSnapshot, KeyValueSnapshot, ReplaySnapshot,
    TailReplay,
};
use crate::{
    ClosedCompactionOptions, CompactionError, CompactionOperation, DirectoryCompactionOutcome,
    DurabilityPolicy, StoreFamily,
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
    let replacement_inventory =
        match write_staging_families(&paths, &capture.families, options.durability_policy()) {
            Ok(inventory) => inventory,
            Err(error) => {
                let _ = fs::remove_dir_all(&paths.staging);
                return Err(error);
            }
        };
    Ok(PreparedClosedStaging {
        capture,
        paths,
        replacement_inventory,
    })
}

fn capture_closed_generation(store_dir: &Path) -> Result<CapturedGeneration, CompactionError> {
    let inspection = inspect_directory(store_dir).map_err(|error| {
        crate::maintenance::map_inspection_error(store_dir.to_path_buf(), error)
    })?;
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
    _options: ClosedCompactionOptions,
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
    let inspection = crate::inspect_storage(store_dir)?;
    if inspection.families().is_empty() {
        return Ok(DirectoryCompactionOutcome::empty());
    }
    Err(CompactionError::FailedClosed {
        detail: "non-empty closed compaction is not implemented".to_owned(),
    })
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
