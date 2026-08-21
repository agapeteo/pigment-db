//! Crash-safe WAL artifact classification and publication.

use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Mutex;

use crate::config::DurabilityPolicy;
#[cfg(not(target_os = "windows"))]
use crate::durability::synchronize_directory;
use crate::durability::{
    preflight_directory, preflight_file, preflight_file_handle, validate_compile_target,
};
use crate::recovery::{classify_runtime_envelope, RuntimeEnvelopeClassification};
use crate::wal::format::{
    HeaderProbeClassification, V1CodecProbe, V2CodecProbe, V2HeaderProbeFields,
};
use crate::wal::replay::{CheckedFrames, ReplaySnapshot, TailReplay, ValidationError};
use crate::wal::WalStorage;
use crate::{RecoveryError, RecoveryOperation, RecoveryStatus};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoreKind {
    Value,
    Set,
    Map,
}

impl StoreKind {
    fn file_name(self) -> &'static str {
        match self {
            Self::Value => "kv.wal.dat",
            Self::Set => "set.wal.dat",
            Self::Map => "map.wal.dat",
        }
    }

    fn record_kind(self) -> u8 {
        match self {
            Self::Value => 1,
            Self::Set => 2,
            Self::Map => 3,
        }
    }
}

pub(crate) fn canonical_sealed_segment_id(name: &OsStr, active_name: &str) -> Option<u64> {
    let name = name.to_str()?;
    let encoded_id = name.strip_prefix(active_name)?.strip_prefix(".segment-")?;
    if encoded_id.len() != 20 || !encoded_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let id = encoded_id.parse::<u64>().ok()?;
    (format!("{id:020}") == encoded_id).then_some(id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactPaths {
    pub(crate) kind: StoreKind,
    pub(crate) active: PathBuf,
    pub(crate) legacy: PathBuf,
    pub(crate) staging: PathBuf,
}

impl ArtifactPaths {
    pub(crate) fn new(directory: &Path, kind: StoreKind) -> Self {
        let file_name = kind.file_name();
        Self {
            kind,
            active: directory.join(file_name),
            legacy: directory.join(format!(".{file_name}")),
            staging: directory.join(format!(".{file_name}.next")),
        }
    }
}

fn sealed_segment_paths(paths: &ArtifactPaths) -> Result<Vec<PathBuf>, RecoveryError> {
    let parent = paths
        .active
        .parent()
        .expect("WAL artifact must have a parent directory");
    if !parent.exists() {
        return Ok(Vec::new());
    }
    let active_name = paths
        .active
        .file_name()
        .expect("WAL artifact must have a file name")
        .to_string_lossy();
    let prefix = format!("{active_name}.segment-");
    let entries = fs::read_dir(parent)
        .map_err(|source| io_failure(RecoveryOperation::Inspect, parent, source))?;
    let mut segments = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|source| io_failure(RecoveryOperation::Inspect, parent, source))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(id) = name.strip_prefix(&prefix) else {
            continue;
        };
        if id.len() != 20 || !id.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let id = id
            .parse::<u64>()
            .map_err(|_| RecoveryError::InvalidArtifact { path: entry.path() })?;
        segments.push((id, entry.path()));
    }
    segments.sort_by_key(|(id, _)| *id);
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) enum ArtifactObservation<S> {
    Missing {
        path: PathBuf,
    },
    Complete {
        path: PathBuf,
        byte_len: u64,
        snapshot: S,
        prefixes: Vec<S>,
    },
    Incomplete {
        path: PathBuf,
        validated_len: u64,
    },
    Invalid {
        path: PathBuf,
    },
}

#[cfg(test)]
impl<S> ArtifactObservation<S> {
    pub(crate) fn missing(path: PathBuf) -> Self {
        Self::Missing { path }
    }

    pub(crate) fn complete(path: PathBuf, byte_len: u64, snapshot: S) -> Self {
        Self::Complete {
            path,
            byte_len,
            snapshot,
            prefixes: Vec::new(),
        }
    }

    pub(crate) fn complete_with_prefixes(
        path: PathBuf,
        byte_len: u64,
        snapshot: S,
        prefixes: Vec<S>,
    ) -> Self {
        Self::Complete {
            path,
            byte_len,
            snapshot,
            prefixes,
        }
    }

    fn exists(&self) -> bool {
        !matches!(self, Self::Missing { .. })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) enum RecoverySource {
    Empty,
    Active,
    Legacy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) enum RecoveryDecision {
    Use {
        source: RecoverySource,
        status: RecoveryStatus,
    },
    Conflict,
    Invalid {
        path: PathBuf,
    },
}

#[cfg(test)]
pub(crate) fn classify_artifacts<S: Eq>(
    active: &ArtifactObservation<S>,
    legacy: &ArtifactObservation<S>,
    staging: &ArtifactObservation<S>,
    is_compacted_prefix: impl Fn(&S, &S) -> bool,
) -> RecoveryDecision {
    let recovered_for_stage = if staging.exists() {
        RecoveryStatus::Recovered
    } else {
        RecoveryStatus::Normal
    };

    match legacy {
        ArtifactObservation::Missing { .. } => match active {
            ArtifactObservation::Missing { .. } => RecoveryDecision::Use {
                source: RecoverySource::Empty,
                status: recovered_for_stage,
            },
            ArtifactObservation::Complete { .. } => RecoveryDecision::Use {
                source: RecoverySource::Active,
                status: recovered_for_stage,
            },
            ArtifactObservation::Incomplete { path, .. }
            | ArtifactObservation::Invalid { path } => {
                RecoveryDecision::Invalid { path: path.clone() }
            }
        },
        ArtifactObservation::Complete {
            snapshot: legacy_snapshot,
            ..
        } => match active {
            ArtifactObservation::Missing { .. } | ArtifactObservation::Incomplete { .. } => {
                RecoveryDecision::Use {
                    source: RecoverySource::Legacy,
                    status: RecoveryStatus::Recovered,
                }
            }
            ArtifactObservation::Complete {
                snapshot: active_snapshot,
                prefixes,
                ..
            } if active_snapshot == legacy_snapshot
                || prefixes.iter().any(|prefix| prefix == legacy_snapshot) =>
            {
                RecoveryDecision::Use {
                    source: RecoverySource::Active,
                    status: RecoveryStatus::Recovered,
                }
            }
            ArtifactObservation::Complete {
                snapshot: active_snapshot,
                ..
            } if is_compacted_prefix(active_snapshot, legacy_snapshot) => RecoveryDecision::Use {
                source: RecoverySource::Legacy,
                status: RecoveryStatus::Recovered,
            },
            ArtifactObservation::Complete { .. } | ArtifactObservation::Invalid { .. } => {
                RecoveryDecision::Conflict
            }
        },
        ArtifactObservation::Incomplete { path, .. } | ArtifactObservation::Invalid { path } => {
            match active {
                ArtifactObservation::Missing { .. }
                | ArtifactObservation::Incomplete { .. }
                | ArtifactObservation::Invalid { .. } => {
                    RecoveryDecision::Invalid { path: path.clone() }
                }
                ArtifactObservation::Complete { .. } => RecoveryDecision::Conflict,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationCheckpoint {
    StagingCreated,
    FirstRecordWritten,
    MiddleRecordWritten,
    Validated,
    Synchronized,
    Published,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationFaultPoint {
    Fresh,
    Repair,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FreshOptionsProbe {
    pub(crate) kind: StoreKind,
    pub(crate) granularity_nanos: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FreshCandidateRole {
    Active,
    Recovery,
    Staging,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FreshInspection {
    Ready,
    InvalidOptions,
    Existing {
        role: FreshCandidateRole,
        path: PathBuf,
    },
}

#[cfg(test)]
pub(crate) fn inspect_fresh_candidate(
    paths: &ArtifactPaths,
    options: FreshOptionsProbe,
) -> Result<FreshInspection, RecoveryError> {
    if options.granularity_nanos == 0 {
        return Ok(FreshInspection::InvalidOptions);
    }
    let _requested_kind = options.kind;
    for (role, path) in [
        (FreshCandidateRole::Active, &paths.active),
        (FreshCandidateRole::Recovery, &paths.legacy),
        (FreshCandidateRole::Staging, &paths.staging),
    ] {
        if artifact_exists(path)? {
            return Ok(FreshInspection::Existing {
                role,
                path: path.clone(),
            });
        }
    }
    Ok(FreshInspection::Ready)
}

#[derive(Default)]
pub(crate) struct FreshCleanupRegistry {
    registered: Vec<PathBuf>,
    attempted: Vec<PathBuf>,
}

impl FreshCleanupRegistry {
    pub(crate) fn register_staging(&mut self, paths: &ArtifactPaths, path: &Path) -> bool {
        let staging_is_regular =
            fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file());
        let active_absent = matches!(
            fs::symlink_metadata(&paths.active),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        );
        let recovery_absent = matches!(
            fs::symlink_metadata(&paths.legacy),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        );
        if !self.registered.is_empty()
            || path != paths.staging
            || !staging_is_regular
            || !active_absent
            || !recovery_absent
        {
            return false;
        }
        self.registered.push(path.to_path_buf());
        true
    }

    pub(crate) fn registered(&self) -> &[PathBuf] {
        &self.registered
    }

    #[cfg(test)]
    pub(crate) fn attempted(&self) -> &[PathBuf] {
        &self.attempted
    }

    pub(crate) fn cleanup(&mut self) -> Result<(), RecoveryError> {
        for target in self.registered.clone() {
            self.attempted.push(target.clone());
            remove_obsolete(&target)
                .map_err(|source| io_failure(RecoveryOperation::Cleanup, &target, source))?;
            self.registered.retain(|registered| registered != &target);
        }
        Ok(())
    }

    fn commit_staging(&mut self, path: &Path) {
        assert_eq!(self.registered.as_slice(), [path.to_path_buf()]);
        self.registered.clear();
    }
}

pub(crate) fn create_fresh_staging(
    paths: &ArtifactPaths,
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, RecoveryError> {
    let handle = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&paths.staging)
        .map_err(|source| io_failure(RecoveryOperation::CreateStaging, &paths.staging, source))?;
    if !registry.register_staging(paths, &paths.staging) {
        drop(handle);
        let registration_error = io::Error::other("created staging role could not be registered");
        let _ = remove_obsolete(&paths.staging);
        return Err(io_failure(
            RecoveryOperation::CreateStaging,
            &paths.staging,
            registration_error,
        ));
    }
    Ok(handle)
}

#[derive(Debug)]
pub(crate) struct FreshPublicationFailure {
    pub(crate) operation: RecoveryOperation,
    pub(crate) path: PathBuf,
    pub(crate) cleanup_path: Option<PathBuf>,
    pub(crate) source: io::Error,
}

pub(crate) fn write_fresh_header_prefix(
    mut staging: std::fs::File,
    header: &[u8],
    written_len: usize,
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, FreshPublicationFailure> {
    let written_len = written_len.min(header.len());
    let write_result = staging.write_all(&header[..written_len]);
    if write_result.is_err() || written_len != header.len() {
        drop(staging);
        return Err(fail_fresh_before_publish(
            RecoveryOperation::WriteStaging,
            registry,
            write_result.err().unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WriteZero,
                    "fresh header write was incomplete",
                )
            }),
        ));
    }
    Ok(staging)
}

pub(crate) fn flush_fresh_header(
    mut staging: std::fs::File,
    inject_failure: bool,
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, FreshPublicationFailure> {
    let flush_result = if inject_failure {
        Err(io::Error::other("injected fresh-header flush failure"))
    } else {
        staging.flush()
    };
    if let Err(source) = flush_result {
        drop(staging);
        return Err(fail_fresh_before_publish(
            RecoveryOperation::WriteStaging,
            registry,
            source,
        ));
    }
    Ok(staging)
}

pub(crate) fn readback_fresh_header(
    mut staging: std::fs::File,
    inject_read_failure: bool,
    registry: &mut FreshCleanupRegistry,
) -> Result<(std::fs::File, Vec<u8>), FreshPublicationFailure> {
    let persisted = if inject_read_failure {
        Err(io::Error::other("injected staged-header read failure"))
    } else {
        staging.seek(SeekFrom::Start(0)).and_then(|_| {
            let mut persisted = Vec::new();
            staging.read_to_end(&mut persisted).map(|_| persisted)
        })
    };

    match persisted {
        Ok(bytes)
            if matches!(
                bytes.len(),
                V1CodecProbe::HEADER_LEN | V2CodecProbe::HEADER_LEN
            ) =>
        {
            Ok((staging, bytes))
        }
        Ok(_) => {
            drop(staging);
            Err(fail_fresh_before_publish(
                RecoveryOperation::Open,
                registry,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fresh staging has an invalid header length",
                ),
            ))
        }
        Err(source) => {
            drop(staging);
            Err(fail_fresh_before_publish(
                RecoveryOperation::Open,
                registry,
                source,
            ))
        }
    }
}

pub(crate) fn validate_fresh_header(
    staging: std::fs::File,
    persisted: &[u8],
    expected: &[u8],
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, FreshPublicationFailure> {
    let valid_v1 = V1CodecProbe::classify_header(persisted) == HeaderProbeClassification::Valid
        && V1CodecProbe::magic_is_valid(persisted)
        && V1CodecProbe::version_is_valid(persisted)
        && V1CodecProbe::header_length_is_valid(persisted)
        && V1CodecProbe::kind_is_valid(persisted)
        && V1CodecProbe::timestamp_unit_is_valid(persisted)
        && V1CodecProbe::flags_are_valid(persisted)
        && V1CodecProbe::granularity_is_valid(persisted)
        && V1CodecProbe::reserved_is_valid(persisted)
        && V1CodecProbe::header_crc_is_valid(persisted);
    let valid = (valid_v1 || V2CodecProbe::header_is_valid(persisted)) && persisted == expected;
    if valid {
        Ok(staging)
    } else {
        drop(staging);
        Err(fail_fresh_before_publish(
            RecoveryOperation::WriteStaging,
            registry,
            io::Error::new(
                io::ErrorKind::InvalidData,
                "fresh staging header validation failed",
            ),
        ))
    }
}

pub(crate) fn sync_fresh_header(
    staging: std::fs::File,
    inject_failure: bool,
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, FreshPublicationFailure> {
    let sync_result = if inject_failure {
        Err(io::Error::other("injected fresh-header sync failure"))
    } else {
        staging.sync_all()
    };
    if let Err(source) = sync_result {
        drop(staging);
        Err(fail_fresh_before_publish(
            RecoveryOperation::WriteStaging,
            registry,
            source,
        ))
    } else {
        Ok(staging)
    }
}

#[allow(dead_code)]
pub(crate) fn prepare_fresh_append(
    staging: std::fs::File,
    inject_failure: bool,
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, FreshPublicationFailure> {
    prepare_fresh_append_at(
        staging,
        V1CodecProbe::HEADER_LEN as u64,
        inject_failure,
        registry,
    )
}

fn prepare_fresh_append_at(
    mut staging: std::fs::File,
    append_offset: u64,
    inject_failure: bool,
    registry: &mut FreshCleanupRegistry,
) -> Result<std::fs::File, FreshPublicationFailure> {
    let positioned = if inject_failure {
        Err(io::Error::other(
            "injected append-handle preparation failure",
        ))
    } else {
        staging
            .seek(SeekFrom::Start(append_offset))
            .and_then(|offset| {
                (offset == append_offset)
                    .then_some(())
                    .ok_or_else(|| io::Error::other("unexpected append offset"))
            })
    };
    if let Err(source) = positioned {
        drop(staging);
        Err(fail_fresh_before_publish(
            RecoveryOperation::WriteStaging,
            registry,
            source,
        ))
    } else {
        Ok(staging)
    }
}

pub(crate) struct PublishedFresh {
    handle: std::fs::File,
}

#[allow(dead_code)]
pub(crate) fn publish_fresh_header(
    staging: std::fs::File,
    paths: &ArtifactPaths,
    inject_failure: bool,
    registry: &mut FreshCleanupRegistry,
) -> Result<PublishedFresh, FreshPublicationFailure> {
    publish_fresh_header_with_policy(
        staging,
        paths,
        inject_failure,
        registry,
        DurabilityPolicy::Buffered,
    )
}

fn publish_fresh_buffered(
    staging: std::fs::File,
    paths: &ArtifactPaths,
) -> io::Result<std::fs::File> {
    if paths.active.exists() {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "active path appeared before publication",
        ))
    } else {
        fs::rename(&paths.staging, &paths.active)?;
        Ok(staging)
    }
}

fn publish_fresh_header_with_policy(
    staging: std::fs::File,
    paths: &ArtifactPaths,
    inject_failure: bool,
    registry: &mut FreshCleanupRegistry,
    durability_policy: DurabilityPolicy,
) -> Result<PublishedFresh, FreshPublicationFailure> {
    #[cfg(target_os = "windows")]
    if durability_policy == DurabilityPolicy::Physical && !inject_failure {
        drop(staging);
        if let Err(source) = crate::durability::move_windows_namespace_write_through(
            &paths.staging,
            &paths.active,
            crate::durability::NamespaceMoveMode::NoReplace,
        ) {
            return Err(fail_fresh_before_publish_at(
                RecoveryOperation::Publish,
                paths.active.clone(),
                registry,
                source,
            ));
        }
        registry.commit_staging(&paths.staging);
        let handle = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&paths.active)
            .map_err(|source| FreshPublicationFailure {
                operation: RecoveryOperation::Open,
                path: paths.active.clone(),
                cleanup_path: None,
                source,
            })?;
        return Ok(PublishedFresh { handle });
    }

    let publish_result = if inject_failure {
        drop(staging);
        Err(io::Error::other("injected fresh-header publish failure"))
    } else {
        #[cfg(target_os = "windows")]
        {
            let _ = durability_policy;
            publish_fresh_buffered(staging, paths)
        }
        #[cfg(not(target_os = "windows"))]
        {
            let _ = durability_policy;
            publish_fresh_buffered(staging, paths)
        }
    };

    match publish_result {
        Err(source) => Err(fail_fresh_before_publish_at(
            RecoveryOperation::Publish,
            paths.active.clone(),
            registry,
            source,
        )),
        Ok(handle) => {
            registry.commit_staging(&paths.staging);
            Ok(PublishedFresh { handle })
        }
    }
}

pub(crate) fn handoff_fresh_handle(
    published: PublishedFresh,
) -> Result<std::fs::File, PublishedFresh> {
    Ok(published.handle)
}

#[allow(dead_code)]
pub(crate) fn initialize_fresh_v1(
    paths: &ArtifactPaths,
    header: &[u8; V1CodecProbe::HEADER_LEN],
    durability_policy: DurabilityPolicy,
) -> Result<WalStorage<std::fs::File>, RecoveryError> {
    let mut registry = FreshCleanupRegistry::default();
    let staging = create_fresh_staging(paths, &mut registry)?;
    let staging = write_fresh_header_prefix(staging, header, header.len(), &mut registry)
        .map_err(fresh_publication_error)?;
    let staging =
        flush_fresh_header(staging, false, &mut registry).map_err(fresh_publication_error)?;
    let (staging, persisted) =
        readback_fresh_header(staging, false, &mut registry).map_err(fresh_publication_error)?;
    let staging = validate_fresh_header(staging, &persisted, header, &mut registry)
        .map_err(fresh_publication_error)?;
    let staging = if durability_policy == DurabilityPolicy::Physical {
        if let Err(mut source) = preflight_file_handle(&staging, &paths.staging) {
            drop(staging);
            let cleanup_failed = registry.cleanup().is_err();
            if cleanup_failed {
                log::warn!(
                    "failed to remove non-authoritative staging after content preflight failure: {}",
                    paths.staging.display()
                );
                source = diagnose_staging_cleanup_failure(source, &paths.staging);
            }
            return Err(RecoveryError::UnsupportedDurability { source });
        }
        staging
    } else {
        sync_fresh_header(staging, false, &mut registry).map_err(fresh_publication_error)?
    };
    let staging =
        prepare_fresh_append(staging, false, &mut registry).map_err(fresh_publication_error)?;
    let published =
        publish_fresh_header_with_policy(staging, paths, false, &mut registry, durability_policy)
            .map_err(fresh_publication_error)?;
    if durability_policy == DurabilityPolicy::Physical {
        #[cfg(not(target_os = "windows"))]
        {
            let parent = paths
                .active
                .parent()
                .expect("WAL artifact must have a parent directory");
            synchronize_directory(parent)
                .map_err(|source| io_failure(RecoveryOperation::Publish, parent, source))?;
        }
    }
    let handle = match handoff_fresh_handle(published) {
        Ok(handle) => handle,
        Err(_) => unreachable!("prepared fresh handle handoff is infallible"),
    };
    let granularity_nanos = V1CodecProbe::granularity(header).unwrap();
    let base_bucket = V1CodecProbe::base_bucket(header).unwrap_or(0);
    Ok(WalStorage::from_prepared_file_with_timestamp_state(
        handle,
        V1CodecProbe::HEADER_LEN as u32,
        granularity_nanos,
        base_bucket,
    ))
}

pub(crate) fn initialize_fresh_v2(
    paths: &ArtifactPaths,
    header: &[u8; V2CodecProbe::HEADER_LEN],
    durability_policy: DurabilityPolicy,
) -> Result<WalStorage<std::fs::File>, RecoveryError> {
    let mut registry = FreshCleanupRegistry::default();
    let staging = create_fresh_staging(paths, &mut registry)?;
    let staging = write_fresh_header_prefix(staging, header, header.len(), &mut registry)
        .map_err(fresh_publication_error)?;
    let staging =
        flush_fresh_header(staging, false, &mut registry).map_err(fresh_publication_error)?;
    let (staging, persisted) =
        readback_fresh_header(staging, false, &mut registry).map_err(fresh_publication_error)?;
    let staging = validate_fresh_header(staging, &persisted, header, &mut registry)
        .map_err(fresh_publication_error)?;
    let staging = if durability_policy == DurabilityPolicy::Physical {
        if let Err(mut source) = preflight_file_handle(&staging, &paths.staging) {
            drop(staging);
            let cleanup_failed = registry.cleanup().is_err();
            if cleanup_failed {
                log::warn!(
                    "failed to remove non-authoritative staging after content preflight failure: {}",
                    paths.staging.display()
                );
                source = diagnose_staging_cleanup_failure(source, &paths.staging);
            }
            return Err(RecoveryError::UnsupportedDurability { source });
        }
        staging
    } else {
        sync_fresh_header(staging, false, &mut registry).map_err(fresh_publication_error)?
    };
    let staging = prepare_fresh_append_at(
        staging,
        V2CodecProbe::HEADER_LEN as u64,
        false,
        &mut registry,
    )
    .map_err(fresh_publication_error)?;
    let published =
        publish_fresh_header_with_policy(staging, paths, false, &mut registry, durability_policy)
            .map_err(fresh_publication_error)?;
    if durability_policy == DurabilityPolicy::Physical {
        #[cfg(not(target_os = "windows"))]
        {
            let parent = paths
                .active
                .parent()
                .expect("WAL artifact must have a parent directory");
            synchronize_directory(parent)
                .map_err(|source| io_failure(RecoveryOperation::Publish, parent, source))?;
        }
    }
    let handle = match handoff_fresh_handle(published) {
        Ok(handle) => handle,
        Err(_) => unreachable!("prepared fresh handle handoff is infallible"),
    };
    Ok(WalStorage::from_prepared_file_v2_with_timestamp_state(
        handle,
        V2CodecProbe::HEADER_LEN as u64,
        V2CodecProbe::header_granularity(header).unwrap(),
        V2CodecProbe::header_base_bucket(header).unwrap_or(0),
    ))
}

#[derive(Debug)]
struct StagingCleanupDiagnostic {
    preflight: io::Error,
    staging: PathBuf,
}

impl fmt::Display for StagingCleanupDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; cleanup failed for non-authoritative staging {}",
            self.preflight,
            self.staging.display()
        )
    }
}

impl Error for StagingCleanupDiagnostic {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.preflight)
    }
}

fn diagnose_staging_cleanup_failure(
    error: crate::durability::DurabilitySupportError,
    staging: &Path,
) -> crate::durability::DurabilitySupportError {
    match error {
        crate::durability::DurabilitySupportError::RequiredBarrierUnavailable {
            operation,
            path,
            source,
        } => {
            let kind = source.kind();
            crate::durability::DurabilitySupportError::RequiredBarrierUnavailable {
                operation,
                path,
                source: io::Error::new(
                    kind,
                    StagingCleanupDiagnostic {
                        preflight: source,
                        staging: staging.to_path_buf(),
                    },
                ),
            }
        }
        other => other,
    }
}

pub(crate) fn encode_key_value_repair_snapshot(
    snapshot: &crate::wal::replay::KeyValueSnapshot,
    header: &[u8; V1CodecProbe::HEADER_LEN],
) -> Vec<u8> {
    let mut entries = snapshot.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| *key);
    let records = entries
        .into_iter()
        .map(|(key, value)| {
            (
                crate::wal::model::PUT_ACT,
                bincode::serialize(&crate::wal::model::KeyValueData::new(
                    key.clone(),
                    value.clone(),
                ))
                .expect("accepted key/value snapshot payload must encode"),
            )
        })
        .collect();
    encode_repair_snapshot_group(header, records)
}

pub(crate) fn encode_key_set_repair_snapshot(
    snapshot: &crate::wal::replay::KeySetSnapshot,
    header: &[u8; V1CodecProbe::HEADER_LEN],
) -> Vec<u8> {
    let mut keys = snapshot.keys().collect::<Vec<_>>();
    keys.sort();
    let mut records = Vec::new();
    for key in keys {
        let mut values = snapshot[key].iter().collect::<Vec<_>>();
        values.sort();
        records.extend(values.into_iter().map(|value| {
            (
                crate::wal::model::SET_APPEND_ACT,
                bincode::serialize(&crate::wal::model::KeyValueData::new(
                    key.clone(),
                    value.clone(),
                ))
                .expect("accepted key/set snapshot payload must encode"),
            )
        }));
    }
    encode_repair_snapshot_group(header, records)
}

pub(crate) fn encode_key_map_repair_snapshot(
    snapshot: &crate::wal::replay::KeyMapSnapshot,
    header: &[u8; V1CodecProbe::HEADER_LEN],
) -> Vec<u8> {
    let mut keys = snapshot.keys().collect::<Vec<_>>();
    keys.sort();
    let mut records = Vec::new();
    for key in keys {
        records.extend(snapshot[key].iter().map(|(search_key, value)| {
            (
                crate::wal::model::MAP_PUT_ACT,
                bincode::serialize(&crate::model::SortedMapEntry::new(
                    key.clone(),
                    search_key.clone(),
                    value.clone(),
                ))
                .expect("accepted key/map snapshot payload must encode"),
            )
        }));
    }
    encode_repair_snapshot_group(header, records)
}

fn encode_repair_snapshot_group(
    header: &[u8; V1CodecProbe::HEADER_LEN],
    records: Vec<(u8, Vec<u8>)>,
) -> Vec<u8> {
    let mut encoded = header.to_vec();
    if records.is_empty() {
        return encoded;
    }
    let count = u32::try_from(records.len()).expect("accepted snapshot record count must fit u32");
    let mutation_start = V1CodecProbe::HEADER_LEN as u32;
    let timestamp_bucket = V1CodecProbe::base_bucket(header).unwrap_or(0);
    for (index, (action, payload)) in records.into_iter().enumerate() {
        let physical_start = u32::try_from(encoded.len()).expect("accepted snapshot must fit u32");
        encoded.extend_from_slice(&V1CodecProbe::encode_complete_record(
            crate::wal::format::RecordProbeFields {
                action,
                payload: &payload,
                physical_start,
                mutation_start,
                index: index as u32,
                count,
                timestamp_bucket,
            },
        ));
    }
    encoded
}

fn header_with_granularity_and_base_bucket(
    mut header: [u8; V1CodecProbe::HEADER_LEN],
    granularity_nanos: u64,
    base_bucket: u64,
) -> [u8; V1CodecProbe::HEADER_LEN] {
    header[16..24].copy_from_slice(&granularity_nanos.to_le_bytes());
    header[24..32].copy_from_slice(&base_bucket.to_le_bytes());
    let crc = crc32fast::hash(&header[..36]);
    header[36..40].copy_from_slice(&crc.to_le_bytes());
    header
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RepairPublicationFailure {
    pub(crate) operation: RecoveryOperation,
    pub(crate) path: PathBuf,
}

pub(crate) fn create_repair_staging(
    paths: &ArtifactPaths,
) -> Result<std::fs::File, RepairPublicationFailure> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&paths.staging)
        .map_err(|_| RepairPublicationFailure {
            operation: RecoveryOperation::CreateStaging,
            path: paths.staging.clone(),
        })
}

pub(crate) fn write_repair_snapshot_prefix(
    mut staging: std::fs::File,
    replacement: &[u8],
    written_len: usize,
    paths: &ArtifactPaths,
) -> Result<std::fs::File, RepairPublicationFailure> {
    let written_len = written_len.min(replacement.len());
    staging
        .write_all(&replacement[..written_len])
        .map_err(|_| RepairPublicationFailure {
            operation: RecoveryOperation::WriteStaging,
            path: paths.staging.clone(),
        })?;
    if written_len == replacement.len() {
        Ok(staging)
    } else {
        Err(RepairPublicationFailure {
            operation: RecoveryOperation::WriteStaging,
            path: paths.staging.clone(),
        })
    }
}

pub(crate) fn flush_repair_snapshot(
    mut staging: std::fs::File,
    inject_failure: bool,
    paths: &ArtifactPaths,
) -> Result<std::fs::File, RepairPublicationFailure> {
    if inject_failure {
        return Err(RepairPublicationFailure {
            operation: RecoveryOperation::WriteStaging,
            path: paths.staging.clone(),
        });
    }
    staging.flush().map_err(|_| RepairPublicationFailure {
        operation: RecoveryOperation::WriteStaging,
        path: paths.staging.clone(),
    })?;
    Ok(staging)
}

fn validate_repair_snapshot(
    mut staging: std::fs::File,
    expected_len: u64,
    paths: &ArtifactPaths,
    validate: &impl Fn(&[u8]) -> bool,
) -> Result<std::fs::File, RepairPublicationFailure> {
    let mut persisted = Vec::new();
    let valid = staging.seek(SeekFrom::Start(0)).is_ok()
        && staging.read_to_end(&mut persisted).is_ok()
        && persisted.len() as u64 == expected_len
        && validate(&persisted);
    if valid {
        Ok(staging)
    } else {
        Err(RepairPublicationFailure {
            operation: RecoveryOperation::WriteStaging,
            path: paths.staging.clone(),
        })
    }
}

#[cfg(test)]
pub(crate) fn validate_key_value_repair_snapshot(
    staging: std::fs::File,
    expected_snapshot: &crate::wal::replay::KeyValueSnapshot,
    expected_header: &[u8; V1CodecProbe::HEADER_LEN],
    paths: &ArtifactPaths,
) -> Result<std::fs::File, RepairPublicationFailure> {
    let expected_len = staging
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    validate_repair_snapshot(staging, expected_len, paths, &|persisted| {
        persisted.get(..V1CodecProbe::HEADER_LEN) == Some(expected_header.as_slice())
            && crate::wal::replay::replay_key_value(persisted)
                .is_ok_and(|replayed| &replayed.snapshot == expected_snapshot)
    })
}

pub(crate) fn sync_repair_snapshot(
    staging: std::fs::File,
    inject_failure: bool,
    paths: &ArtifactPaths,
) -> Result<std::fs::File, RepairPublicationFailure> {
    if inject_failure {
        return Err(RepairPublicationFailure {
            operation: RecoveryOperation::WriteStaging,
            path: paths.staging.clone(),
        });
    }
    staging.sync_all().map_err(|_| RepairPublicationFailure {
        operation: RecoveryOperation::WriteStaging,
        path: paths.staging.clone(),
    })?;
    Ok(staging)
}

#[cfg(test)]
pub(crate) fn publish_repair_snapshot(
    staging: std::fs::File,
    paths: &ArtifactPaths,
    inject_failure: bool,
) -> Result<u64, RepairPublicationFailure> {
    let expected_len = staging
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    drop(staging);
    if inject_failure {
        return Err(RepairPublicationFailure {
            operation: RecoveryOperation::Publish,
            path: paths.active.clone(),
        });
    }
    fs::rename(&paths.staging, &paths.active).map_err(|_| RepairPublicationFailure {
        operation: RecoveryOperation::Publish,
        path: paths.active.clone(),
    })?;
    Ok(expected_len)
}

fn publish_repair_snapshot_with_policy(
    staging: std::fs::File,
    paths: &ArtifactPaths,
    durability_policy: DurabilityPolicy,
) -> Result<u64, RecoveryError> {
    let expected_len = staging
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|source| io_failure(RecoveryOperation::Publish, &paths.staging, source))?;
    drop(staging);
    publish_namespace_no_replace(&paths.staging, &paths.active, durability_policy)
        .map_err(|source| io_failure(RecoveryOperation::Publish, &paths.active, source))?;
    Ok(expected_len)
}

pub(crate) struct ValidatedRepairHandle {
    handle: std::fs::File,
}

fn reopen_repair_snapshot(
    paths: &ArtifactPaths,
    expected_len: u64,
    validate: &impl Fn(&[u8]) -> bool,
) -> Result<ValidatedRepairHandle, RepairPublicationFailure> {
    let mut handle = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.active)
        .map_err(|_| RepairPublicationFailure {
            operation: RecoveryOperation::Open,
            path: paths.active.clone(),
        })?;
    let mut persisted = Vec::new();
    let valid = handle
        .metadata()
        .is_ok_and(|metadata| metadata.len() == expected_len)
        && handle.seek(SeekFrom::Start(0)).is_ok()
        && handle.read_to_end(&mut persisted).is_ok()
        && persisted.len() as u64 == expected_len
        && validate(&persisted);
    if !valid || handle.seek(SeekFrom::Start(expected_len)).is_err() {
        return Err(RepairPublicationFailure {
            operation: RecoveryOperation::Open,
            path: paths.active.clone(),
        });
    }
    Ok(ValidatedRepairHandle { handle })
}

#[cfg(test)]
pub(crate) fn reopen_key_value_repair_snapshot(
    paths: &ArtifactPaths,
    expected_len: u64,
    expected_snapshot: &crate::wal::replay::KeyValueSnapshot,
    expected_header: &[u8; V1CodecProbe::HEADER_LEN],
) -> Result<ValidatedRepairHandle, RepairPublicationFailure> {
    reopen_repair_snapshot(paths, expected_len, &|persisted| {
        persisted.get(..V1CodecProbe::HEADER_LEN) == Some(expected_header.as_slice())
            && crate::wal::replay::replay_key_value(persisted).is_ok_and(|replayed| {
                replayed.byte_len == expected_len && &replayed.snapshot == expected_snapshot
            })
    })
}

pub(crate) fn cleanup_blocking_repair_active(
    paths: &ArtifactPaths,
    selected_path: &Path,
    expected_obsolete: &[u8],
    inject_failure: bool,
) -> Result<(), RepairPublicationFailure> {
    let exclusive = selected_path == paths.legacy
        && paths.staging.is_file()
        && fs::read(&paths.active).is_ok_and(|bytes| bytes == expected_obsolete);
    if !exclusive {
        return Err(RepairPublicationFailure {
            operation: RecoveryOperation::Cleanup,
            path: paths.active.clone(),
        });
    }
    if inject_failure {
        return Err(RepairPublicationFailure {
            operation: RecoveryOperation::Cleanup,
            path: paths.active.clone(),
        });
    }
    remove_obsolete(&paths.active).map_err(|_| RepairPublicationFailure {
        operation: RecoveryOperation::Cleanup,
        path: paths.active.clone(),
    })?;
    Ok(())
}

pub(crate) struct CompletedRepairHandle {
    pub(crate) handle: std::fs::File,
    pub(crate) status: RecoveryStatus,
    #[allow(dead_code)]
    pub(crate) cleanup_deferred: bool,
}

pub(crate) fn cleanup_after_validated_repair(
    validated: ValidatedRepairHandle,
    paths: &ArtifactPaths,
    inject_failure: bool,
) -> Result<CompletedRepairHandle, RepairPublicationFailure> {
    let cleanup_deferred =
        paths.legacy.exists() && (inject_failure || remove_obsolete(&paths.legacy).is_err());
    Ok(CompletedRepairHandle {
        handle: validated.handle,
        status: RecoveryStatus::Recovered,
        cleanup_deferred,
    })
}

#[allow(dead_code)]
pub(crate) enum RepairAuthority<'a> {
    Active { obsolete_recovery: Option<&'a [u8]> },
    Recovery { obsolete_active: Option<&'a [u8]> },
}

#[cfg(test)]
pub(crate) fn publish_validated_repair(
    paths: &ArtifactPaths,
    authority: RepairAuthority<'_>,
    replacement: &[u8],
    validate: impl Fn(&[u8]) -> bool,
) -> Result<CompletedRepairHandle, RecoveryError> {
    publish_validated_repair_with_policy(
        paths,
        authority,
        replacement,
        validate,
        DurabilityPolicy::Buffered,
    )
}

pub(crate) fn publish_validated_repair_with_policy(
    paths: &ArtifactPaths,
    authority: RepairAuthority<'_>,
    replacement: &[u8],
    validate: impl Fn(&[u8]) -> bool,
    durability_policy: DurabilityPolicy,
) -> Result<CompletedRepairHandle, RecoveryError> {
    let parent = paths
        .active
        .parent()
        .expect("WAL artifact must have a parent directory");
    let staging = create_repair_staging(paths).map_err(repair_publication_error)?;
    let staging = write_repair_snapshot_prefix(staging, replacement, replacement.len(), paths)
        .map_err(repair_publication_error)?;
    let staging = flush_repair_snapshot(staging, false, paths).map_err(repair_publication_error)?;
    let staging = validate_repair_snapshot(staging, replacement.len() as u64, paths, &validate)
        .map_err(repair_publication_error)?;
    let staging = sync_repair_snapshot(staging, false, paths).map_err(repair_publication_error)?;

    match authority {
        RepairAuthority::Active { obsolete_recovery } => {
            if paths.legacy.exists() {
                let proven = obsolete_recovery.is_some_and(|expected| {
                    fs::read(&paths.legacy).is_ok_and(|bytes| bytes == expected)
                });
                if !proven {
                    return Err(repair_publication_error(RepairPublicationFailure {
                        operation: RecoveryOperation::Cleanup,
                        path: paths.legacy.clone(),
                    }));
                }
                remove_obsolete(&paths.legacy).map_err(|_| {
                    repair_publication_error(RepairPublicationFailure {
                        operation: RecoveryOperation::Cleanup,
                        path: paths.legacy.clone(),
                    })
                })?;
            }
            publish_namespace_no_replace(&paths.active, &paths.legacy, durability_policy)
                .map_err(|source| io_failure(RecoveryOperation::Publish, &paths.legacy, source))?;
            synchronize_published_namespace(parent, durability_policy)
                .map_err(|source| io_failure(RecoveryOperation::Publish, parent, source))?;
        }
        RepairAuthority::Recovery { obsolete_active } => {
            if paths.active.exists() {
                let expected = obsolete_active.ok_or_else(|| {
                    repair_publication_error(RepairPublicationFailure {
                        operation: RecoveryOperation::Cleanup,
                        path: paths.active.clone(),
                    })
                })?;
                cleanup_blocking_repair_active(paths, &paths.legacy, expected, false)
                    .map_err(repair_publication_error)?;
            }
        }
    }

    let expected_len = publish_repair_snapshot_with_policy(staging, paths, durability_policy)?;
    synchronize_published_namespace(parent, durability_policy)
        .map_err(|source| io_failure(RecoveryOperation::Publish, parent, source))?;
    let validated =
        reopen_repair_snapshot(paths, expected_len, &validate).map_err(repair_publication_error)?;
    if durability_policy == DurabilityPolicy::Buffered {
        return cleanup_after_validated_repair(validated, paths, false)
            .map_err(repair_publication_error);
    }

    let cleanup_deferred = if paths.legacy.exists() {
        match remove_obsolete(&paths.legacy) {
            Err(error) => {
                log::warn!(
                    "deferred stale WAL recovery cleanup for {}: {}",
                    paths.legacy.display(),
                    error
                );
                true
            }
            Ok(()) => match synchronize_published_namespace(parent, durability_policy) {
                Ok(()) => false,
                Err(error) => {
                    log::warn!(
                        "WAL recovery cleanup is durable-indeterminate for {}: {}",
                        paths.legacy.display(),
                        error
                    );
                    true
                }
            },
        }
    } else {
        false
    };
    Ok(CompletedRepairHandle {
        handle: validated.handle,
        status: RecoveryStatus::Recovered,
        cleanup_deferred,
    })
}

fn repair_publication_error(failure: RepairPublicationFailure) -> RecoveryError {
    io_failure(
        failure.operation,
        &failure.path,
        io::Error::other("repair publication checkpoint failed"),
    )
}

fn fresh_publication_error(failure: FreshPublicationFailure) -> RecoveryError {
    let source = match failure.cleanup_path {
        Some(cleanup_path) => io::Error::other(format!(
            "fresh publication failed ({}); cleanup also failed for {}",
            failure.source,
            cleanup_path.display(),
        )),
        None => failure.source,
    };
    io_failure(failure.operation, &failure.path, source)
}

fn fail_fresh_before_publish(
    operation: RecoveryOperation,
    registry: &mut FreshCleanupRegistry,
    source: io::Error,
) -> FreshPublicationFailure {
    let path = registry
        .registered()
        .first()
        .cloned()
        .expect("pre-publication failure requires registered staging");
    fail_fresh_before_publish_at(operation, path, registry, source)
}

fn fail_fresh_before_publish_at(
    operation: RecoveryOperation,
    path: PathBuf,
    registry: &mut FreshCleanupRegistry,
    source: io::Error,
) -> FreshPublicationFailure {
    let cleanup_path = match registry.cleanup() {
        Ok(()) => None,
        Err(RecoveryError::Io { path, .. }) => Some(path),
        Err(_) => Some(path.clone()),
    };
    FreshPublicationFailure {
        operation,
        path,
        cleanup_path,
        source,
    }
}

fn io_failure(operation: RecoveryOperation, path: &Path, source: io::Error) -> RecoveryError {
    RecoveryError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn publish_namespace_no_replace(
    source: &Path,
    destination: &Path,
    durability_policy: DurabilityPolicy,
) -> io::Result<()> {
    #[cfg(target_os = "windows")]
    if durability_policy == DurabilityPolicy::Physical {
        return crate::durability::move_windows_namespace_write_through(
            source,
            destination,
            crate::durability::NamespaceMoveMode::NoReplace,
        );
    }
    let _ = durability_policy;
    fs::rename(source, destination)
}

fn publish_namespace_replace_existing(
    source: &Path,
    destination: &Path,
    durability_policy: DurabilityPolicy,
) -> io::Result<()> {
    #[cfg(target_os = "windows")]
    if durability_policy == DurabilityPolicy::Physical {
        return crate::durability::move_windows_namespace_write_through(
            source,
            destination,
            crate::durability::NamespaceMoveMode::ReplaceExisting,
        );
    }
    let _ = durability_policy;
    fs::rename(source, destination)
}

fn synchronize_published_namespace(
    parent: &Path,
    durability_policy: DurabilityPolicy,
) -> io::Result<()> {
    if durability_policy != DurabilityPolicy::Physical {
        return Ok(());
    }
    #[cfg(target_os = "windows")]
    {
        let _ = parent;
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    {
        synchronize_directory(parent)
    }
}

pub(crate) fn artifact_exists(path: &Path) -> Result<bool, RecoveryError> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_failure(RecoveryOperation::Inspect, path, source)),
    }
}

#[cfg(test)]
static CLEANUP_FAULTS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

pub(crate) fn remove_obsolete(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if CLEANUP_FAULTS
        .lock()
        .unwrap()
        .iter()
        .any(|fault| fault == path)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected cleanup failure",
        ));
    }
    fs::remove_file(path)
}

#[cfg(test)]
pub(crate) struct CleanupFaultGuard(PathBuf);

#[cfg(test)]
impl Drop for CleanupFaultGuard {
    fn drop(&mut self) {
        let mut faults = CLEANUP_FAULTS.lock().unwrap();
        let index = faults
            .iter()
            .position(|fault| fault == &self.0)
            .expect("registered cleanup fault must remain until its guard drops");
        faults.swap_remove(index);
    }
}

#[cfg(test)]
pub(crate) fn fail_cleanup_for(path: PathBuf) -> CleanupFaultGuard {
    CLEANUP_FAULTS.lock().unwrap().push(path.clone());
    CleanupFaultGuard(path)
}

#[cfg(test)]
pub(crate) fn publish_replacement(
    paths: &ArtifactPaths,
    replacement: &[u8],
    validate: impl Fn(&[u8]) -> bool,
    observer: &mut impl FnMut(PublicationCheckpoint) -> io::Result<()>,
) -> Result<u64, RecoveryError> {
    publish_replacement_with_policy(
        paths,
        replacement,
        validate,
        observer,
        DurabilityPolicy::Buffered,
    )
}

fn publish_replacement_with_policy(
    paths: &ArtifactPaths,
    replacement: &[u8],
    validate: impl Fn(&[u8]) -> bool,
    observer: &mut impl FnMut(PublicationCheckpoint) -> io::Result<()>,
    durability_policy: DurabilityPolicy,
) -> Result<u64, RecoveryError> {
    let frames = if replacement.starts_with(b"PIGWAL\r\n") {
        let mut ranges = vec![(0, V1CodecProbe::HEADER_LEN)];
        let mut offset = V1CodecProbe::HEADER_LEN;
        while offset < replacement.len() {
            let payload_len = replacement
                .get(offset + 6..offset + 10)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
                .and_then(|length| usize::try_from(length).ok())
                .ok_or_else(|| RecoveryError::InvalidArtifact {
                    path: paths.staging.clone(),
                })?;
            let end = offset
                .checked_add(V1CodecProbe::EMPTY_RECORD_LEN)
                .and_then(|fixed| fixed.checked_add(payload_len))
                .filter(|end| *end <= replacement.len())
                .ok_or_else(|| RecoveryError::InvalidArtifact {
                    path: paths.staging.clone(),
                })?;
            ranges.push((offset, end));
            offset = end;
        }
        ranges
    } else {
        CheckedFrames::new(replacement)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RecoveryError::InvalidArtifact {
                path: paths.staging.clone(),
            })?
            .into_iter()
            .map(|frame| (frame.start_offset(), frame.end_offset()))
            .collect()
    };

    let mut staging = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.staging)
        .map_err(|source| io_failure(RecoveryOperation::CreateStaging, &paths.staging, source))?;
    observer(PublicationCheckpoint::StagingCreated)
        .map_err(|source| io_failure(RecoveryOperation::CreateStaging, &paths.staging, source))?;

    for (index, (start, end)) in frames.iter().enumerate() {
        staging
            .write_all(&replacement[*start..*end])
            .map_err(|source| {
                io_failure(RecoveryOperation::WriteStaging, &paths.staging, source)
            })?;
        if index == 0 {
            observer(PublicationCheckpoint::FirstRecordWritten).map_err(|source| {
                io_failure(RecoveryOperation::WriteStaging, &paths.staging, source)
            })?;
        }
        if frames.len() > 2 && index == frames.len() / 2 {
            observer(PublicationCheckpoint::MiddleRecordWritten).map_err(|source| {
                io_failure(RecoveryOperation::WriteStaging, &paths.staging, source)
            })?;
        }
    }
    staging
        .flush()
        .map_err(|source| io_failure(RecoveryOperation::WriteStaging, &paths.staging, source))?;

    let staged_bytes = fs::read(&paths.staging)
        .map_err(|source| io_failure(RecoveryOperation::Open, &paths.staging, source))?;
    if !validate(&staged_bytes) {
        return Err(RecoveryError::InvalidArtifact {
            path: paths.staging.clone(),
        });
    }
    observer(PublicationCheckpoint::Validated)
        .map_err(|source| io_failure(RecoveryOperation::WriteStaging, &paths.staging, source))?;

    staging
        .sync_all()
        .map_err(|source| io_failure(RecoveryOperation::WriteStaging, &paths.staging, source))?;
    observer(PublicationCheckpoint::Synchronized)
        .map_err(|source| io_failure(RecoveryOperation::WriteStaging, &paths.staging, source))?;
    drop(staging);

    publish_namespace_replace_existing(&paths.staging, &paths.active, durability_policy)
        .map_err(|source| io_failure(RecoveryOperation::Publish, &paths.active, source))?;
    let parent = paths
        .active
        .parent()
        .expect("WAL artifact must have a parent directory");
    synchronize_published_namespace(parent, durability_policy)
        .map_err(|source| io_failure(RecoveryOperation::Publish, parent, source))?;
    observer(PublicationCheckpoint::Published)
        .map_err(|source| io_failure(RecoveryOperation::Publish, &paths.active, source))?;

    Ok(staged_bytes.len() as u64)
}

pub(crate) struct InitializedWal<S> {
    pub(crate) snapshot: S,
    pub(crate) wal: WalStorage<std::fs::File>,
    pub(crate) status: RecoveryStatus,
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn initialize_snapshot<S: Clone + Eq + Default>(
    paths: &ArtifactPaths,
    replay: fn(&[u8]) -> Result<ReplaySnapshot<S>, ValidationError>,
    replay_tail: fn(&[u8]) -> TailReplay<S>,
    replay_against: fn(&[u8], &S) -> Result<ReplaySnapshot<S>, ValidationError>,
    encode: fn(&S) -> Vec<u8>,
    encode_repair: fn(&S, &[u8; V1CodecProbe::HEADER_LEN]) -> Vec<u8>,
    is_proper_snapshot_prefix: fn(&S, &S) -> bool,
    fresh_header: Option<[u8; V1CodecProbe::HEADER_LEN]>,
    requested_granularity_nanos: Option<u64>,
) -> Result<InitializedWal<S>, RecoveryError> {
    initialize_snapshot_impl(
        paths,
        replay,
        replay_tail,
        replay_against,
        encode,
        encode_repair,
        is_proper_snapshot_prefix,
        fresh_header,
        requested_granularity_nanos,
        DurabilityPolicy::Buffered,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn initialize_snapshot_with_policy<S: Clone + Eq + Default>(
    paths: &ArtifactPaths,
    replay: fn(&[u8]) -> Result<ReplaySnapshot<S>, ValidationError>,
    replay_tail: fn(&[u8]) -> TailReplay<S>,
    replay_against: fn(&[u8], &S) -> Result<ReplaySnapshot<S>, ValidationError>,
    encode: fn(&S) -> Vec<u8>,
    encode_repair: fn(&S, &[u8; V1CodecProbe::HEADER_LEN]) -> Vec<u8>,
    is_proper_snapshot_prefix: fn(&S, &S) -> bool,
    fresh_header: Option<[u8; V1CodecProbe::HEADER_LEN]>,
    requested_granularity_nanos: Option<u64>,
    durability_policy: DurabilityPolicy,
) -> Result<InitializedWal<S>, RecoveryError> {
    initialize_snapshot_impl(
        paths,
        replay,
        replay_tail,
        replay_against,
        encode,
        encode_repair,
        is_proper_snapshot_prefix,
        fresh_header,
        requested_granularity_nanos,
        durability_policy,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn initialize_snapshot_impl<S: Clone + Eq + Default>(
    paths: &ArtifactPaths,
    replay: fn(&[u8]) -> Result<ReplaySnapshot<S>, ValidationError>,
    replay_tail: fn(&[u8]) -> TailReplay<S>,
    replay_against: fn(&[u8], &S) -> Result<ReplaySnapshot<S>, ValidationError>,
    encode: fn(&S) -> Vec<u8>,
    encode_repair: fn(&S, &[u8; V1CodecProbe::HEADER_LEN]) -> Vec<u8>,
    is_proper_snapshot_prefix: fn(&S, &S) -> bool,
    fresh_header: Option<[u8; V1CodecProbe::HEADER_LEN]>,
    requested_granularity_nanos: Option<u64>,
    durability_policy: DurabilityPolicy,
    allow_v1_startup: bool,
) -> Result<InitializedWal<S>, RecoveryError> {
    if durability_policy == DurabilityPolicy::Physical {
        validate_compile_target()
            .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
    }
    let mut active_exists = artifact_exists(&paths.active)?;
    let legacy_exists = artifact_exists(&paths.legacy)?;
    let had_staging = artifact_exists(&paths.staging)?;
    let mut active_bytes = if active_exists {
        Some(
            fs::read(&paths.active)
                .map_err(|source| io_failure(RecoveryOperation::Open, &paths.active, source))?,
        )
    } else {
        None
    };
    let sealed_paths = sealed_segment_paths(paths)?;
    let legacy_bytes = if legacy_exists {
        Some(
            fs::read(&paths.legacy)
                .map_err(|source| io_failure(RecoveryOperation::Open, &paths.legacy, source))?,
        )
    } else {
        None
    };
    if !sealed_paths.is_empty() {
        if had_staging && active_exists {
            if let Err(error) = remove_obsolete(&paths.staging) {
                log::warn!(
                    "deferred stale V2 rotation staging cleanup for {}: {}",
                    paths.staging.display(),
                    error
                );
            }
        } else if had_staging {
            let staging_bytes = fs::read(&paths.staging)
                .map_err(|source| io_failure(RecoveryOperation::Open, &paths.staging, source))?;
            let mut candidate_chain = Vec::new();
            for segment_path in &sealed_paths {
                let segment = fs::read(segment_path)
                    .map_err(|source| io_failure(RecoveryOperation::Open, segment_path, source))?;
                candidate_chain.extend_from_slice(&segment);
            }
            candidate_chain.extend_from_slice(&staging_bytes);
            replay(&candidate_chain).map_err(|_| RecoveryError::InvalidArtifact {
                path: paths.staging.clone(),
            })?;
            if legacy_bytes
                .as_deref()
                .is_some_and(|legacy| !legacy.starts_with(&staging_bytes))
            {
                return Err(RecoveryError::AuthorityUndetermined {
                    active_path: None,
                    recovery_path: Some(paths.legacy.clone()),
                });
            }
            if durability_policy == DurabilityPolicy::Physical {
                preflight_file(&paths.staging)
                    .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&paths.staging)
                    .and_then(|file| file.sync_all())
                    .map_err(|source| {
                        io_failure(RecoveryOperation::WriteStaging, &paths.staging, source)
                    })?;
            }
            publish_namespace_no_replace(&paths.staging, &paths.active, durability_policy)
                .map_err(|source| io_failure(RecoveryOperation::Publish, &paths.active, source))?;
            let parent = paths
                .active
                .parent()
                .expect("WAL artifact must have a parent directory");
            synchronize_published_namespace(parent, durability_policy)
                .map_err(|source| io_failure(RecoveryOperation::Publish, parent, source))?;
            active_exists = true;
            active_bytes = Some(staging_bytes);
        }
        if !active_exists {
            return Err(RecoveryError::AuthorityUndetermined {
                active_path: None,
                recovery_path: None,
            });
        }
        let mut chain = Vec::new();
        for segment_path in &sealed_paths {
            let segment = fs::read(segment_path)
                .map_err(|source| io_failure(RecoveryOperation::Open, segment_path, source))?;
            chain.extend_from_slice(&segment);
        }
        let active_segment = active_bytes
            .as_deref()
            .expect("active existence checked for segmented chain");
        let sealed_len = chain.len();
        chain.extend_from_slice(active_segment);
        let replayed = match replay(&chain) {
            Ok(replayed) => replayed,
            Err(_) => {
                let TailReplay::RecoverableTail {
                    tail_offset,
                    accepted_header: Some(header),
                    ..
                } = replay_tail(&chain)
                else {
                    return Err(RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    });
                };
                if header.get(8..10) != Some(2_u16.to_le_bytes().as_slice()) {
                    return Err(RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    });
                }
                let active_tail_offset = tail_offset.checked_sub(sealed_len).ok_or_else(|| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                let replacement = active_segment
                    .get(..active_tail_offset)
                    .ok_or_else(|| RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    })?
                    .to_vec();
                let mut accepted_chain = chain[..sealed_len].to_vec();
                accepted_chain.extend_from_slice(&replacement);
                let accepted =
                    replay(&accepted_chain).map_err(|_| RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    })?;
                if durability_policy == DurabilityPolicy::Physical {
                    let parent = paths
                        .active
                        .parent()
                        .expect("WAL artifact must have a parent directory");
                    preflight_directory(parent)
                        .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
                    preflight_file(&paths.active)
                        .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
                }
                let expected = replacement.clone();
                let completed = publish_validated_repair_with_policy(
                    paths,
                    RepairAuthority::Active {
                        obsolete_recovery: legacy_bytes.as_deref(),
                    },
                    &replacement,
                    |persisted| persisted == expected,
                    durability_policy,
                )?;
                let offset = u64::try_from(replacement.len()).map_err(|_| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                let initialized = InitializedWal {
                    snapshot: accepted.snapshot,
                    wal: WalStorage::from_prepared_file_v2_with_timestamp_state(
                        completed.handle,
                        offset,
                        accepted.granularity_nanos,
                        accepted.last_bucket,
                    ),
                    status: completed.status,
                };
                initialized.wal.set_runtime_policy(durability_policy);
                return Ok(initialized);
            }
        };
        if let Some(legacy) = legacy_bytes.as_deref() {
            if !legacy.starts_with(active_segment) {
                return Err(RecoveryError::AuthorityUndetermined {
                    active_path: Some(paths.active.clone()),
                    recovery_path: Some(paths.legacy.clone()),
                });
            }
            if let Err(error) = remove_obsolete(&paths.legacy) {
                log::warn!(
                    "deferred stale segmented WAL recovery cleanup for {}: {}",
                    paths.legacy.display(),
                    error
                );
            }
        }
        if durability_policy == DurabilityPolicy::Physical {
            validate_compile_target()
                .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
            preflight_file(&paths.active)
                .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
        }
        let wal = WalStorage::try_open_file_based_v2_with_timestamp_state(
            &paths.active,
            active_segment.len() as u64,
            replayed.granularity_nanos,
            replayed.last_bucket,
        )
        .map_err(|source| io_failure(RecoveryOperation::Open, &paths.active, source))?;
        wal.set_runtime_policy(durability_policy);
        return Ok(InitializedWal {
            snapshot: replayed.snapshot,
            wal,
            status: if had_staging {
                RecoveryStatus::Recovered
            } else {
                RecoveryStatus::Normal
            },
        });
    }
    if !allow_v1_startup {
        if active_bytes.as_deref().is_some_and(|bytes| {
            classify_runtime_envelope(bytes, paths.kind.record_kind())
                == RuntimeEnvelopeClassification::RecognizedOlder
        }) {
            return Err(RecoveryError::MigrationRequired {
                path: paths.active.clone(),
            });
        }
        if active_bytes.is_none()
            && legacy_bytes.as_deref().is_some_and(|bytes| {
                classify_runtime_envelope(bytes, paths.kind.record_kind())
                    == RuntimeEnvelopeClassification::RecognizedOlder
            })
        {
            return Err(RecoveryError::MigrationRequired {
                path: paths.legacy.clone(),
            });
        }
    }
    let active_is_versioned = active_bytes
        .as_deref()
        .is_some_and(|bytes| bytes.starts_with(b"PIGWAL\r\n"));
    let legacy_is_versioned = legacy_bytes
        .as_deref()
        .is_some_and(|bytes| bytes.starts_with(b"PIGWAL\r\n"));
    let active_version = active_bytes.as_deref().and_then(|bytes| {
        bytes
            .get(8..10)
            .and_then(|version| version.try_into().ok())
            .map(u16::from_le_bytes)
    });
    let legacy_version = legacy_bytes.as_deref().and_then(|bytes| {
        bytes
            .get(8..10)
            .and_then(|version| version.try_into().ok())
            .map(u16::from_le_bytes)
    });
    if !allow_v1_startup && active_version == Some(1) {
        return match active_bytes.as_deref().map(replay_tail) {
            Some(TailReplay::Complete(_) | TailReplay::RecoverableTail { .. }) => {
                Err(RecoveryError::MigrationRequired {
                    path: paths.active.clone(),
                })
            }
            _ => Err(RecoveryError::InvalidArtifact {
                path: paths.active.clone(),
            }),
        };
    }
    if !allow_v1_startup && legacy_version == Some(1) {
        return match legacy_bytes.as_deref().map(replay_tail) {
            Some(TailReplay::Complete(_) | TailReplay::RecoverableTail { .. }) => {
                Err(RecoveryError::MigrationRequired {
                    path: paths.legacy.clone(),
                })
            }
            _ => Err(RecoveryError::InvalidArtifact {
                path: paths.legacy.clone(),
            }),
        };
    }
    let active_is_v1 = matches!(active_version, Some(1 | 2));
    let legacy_is_v1 = matches!(legacy_version, Some(1 | 2));
    let active_legacy_is_complete = active_bytes
        .as_deref()
        .is_some_and(|bytes| !active_is_versioned && replay(bytes).is_ok());
    if active_legacy_is_complete {
        return Err(RecoveryError::MigrationRequired {
            path: paths.active.clone(),
        });
    }
    let active_cannot_be_authoritative = active_bytes
        .as_deref()
        .is_none_or(|bytes| !active_is_versioned && replay(bytes).is_err());
    if active_cannot_be_authoritative
        && legacy_bytes
            .as_deref()
            .is_some_and(|bytes| !legacy_is_versioned && replay(bytes).is_ok())
    {
        return Err(RecoveryError::MigrationRequired {
            path: paths.legacy.clone(),
        });
    }

    enum Selected<S> {
        Empty(S),
        Active(ReplaySnapshot<S>),
        ActiveTail {
            replay: ReplaySnapshot<S>,
            header: Vec<u8>,
        },
        Legacy(ReplaySnapshot<S>),
    }

    let selected = match (active_bytes.as_deref(), legacy_bytes.as_deref()) {
        (None, None) => Selected::Empty(S::default()),
        (Some(active), None) => match replay_tail(active) {
            TailReplay::Complete(replayed) => Selected::Active(replayed),
            TailReplay::RecoverableTail {
                replay,
                accepted_header: Some(header),
                ..
            } => Selected::ActiveTail { replay, header },
            TailReplay::RecoverableTail { .. } | TailReplay::Invalid(_) => {
                return Err(RecoveryError::InvalidArtifact {
                    path: paths.active.clone(),
                });
            }
        },
        (None, Some(legacy)) => {
            Selected::Legacy(replay(legacy).map_err(|_| RecoveryError::InvalidArtifact {
                path: paths.legacy.clone(),
            })?)
        }
        (Some(active), Some(legacy)) => {
            let legacy_replay =
                replay(legacy).map_err(|_| RecoveryError::AuthorityUndetermined {
                    active_path: Some(paths.active.clone()),
                    recovery_path: Some(paths.legacy.clone()),
                })?;
            if let TailReplay::RecoverableTail {
                replay: active_replay,
                accepted_header: Some(header),
                ..
            } = replay_tail(active)
            {
                let active_reaches_legacy = replay_against(
                    &active[..active_replay.byte_len as usize],
                    &legacy_replay.snapshot,
                )
                .is_ok_and(|replayed| replayed.matched_target_prefix);
                let legacy_reaches_active = replay_against(legacy, &active_replay.snapshot)
                    .is_ok_and(|replayed| replayed.matched_target_prefix);
                if active_replay.snapshot == legacy_replay.snapshot || active_reaches_legacy {
                    Selected::ActiveTail {
                        replay: active_replay,
                        header,
                    }
                } else if legacy_reaches_active {
                    Selected::Legacy(legacy_replay)
                } else {
                    return Err(RecoveryError::AuthorityUndetermined {
                        active_path: Some(paths.active.clone()),
                        recovery_path: Some(paths.legacy.clone()),
                    });
                }
            } else {
                match replay_against(active, &legacy_replay.snapshot) {
                    Err(ValidationError::Truncated { .. }) => Selected::Legacy(legacy_replay),
                    Err(_) => {
                        return Err(RecoveryError::AuthorityUndetermined {
                            active_path: Some(paths.active.clone()),
                            recovery_path: Some(paths.legacy.clone()),
                        });
                    }
                    Ok(active_replay)
                        if active_replay.snapshot == legacy_replay.snapshot
                            || active_replay.matched_target_prefix =>
                    {
                        Selected::Active(active_replay)
                    }
                    Ok(active_replay)
                        if active_replay.compacted_snapshot_prefix
                            && is_proper_snapshot_prefix(
                                &active_replay.snapshot,
                                &legacy_replay.snapshot,
                            ) =>
                    {
                        Selected::Legacy(legacy_replay)
                    }
                    Ok(_) => {
                        return Err(RecoveryError::AuthorityUndetermined {
                            active_path: Some(paths.active.clone()),
                            recovery_path: Some(paths.legacy.clone()),
                        });
                    }
                }
            }
        }
    };

    if durability_policy == DurabilityPolicy::Physical {
        let parent = paths
            .active
            .parent()
            .expect("WAL artifact must have a parent directory");
        preflight_directory(parent)
            .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
        let selected_path = match &selected {
            Selected::Empty(_) => None,
            Selected::Active(_) | Selected::ActiveTail { .. } => Some(&paths.active),
            Selected::Legacy(_) => Some(&paths.legacy),
        };
        if let Some(path) = selected_path {
            preflight_file(path)
                .map_err(|source| RecoveryError::UnsupportedDurability { source })?;
        }
    }

    let status = if legacy_exists || had_staging {
        RecoveryStatus::Recovered
    } else {
        RecoveryStatus::Normal
    };
    let staging_clean = if had_staging {
        match remove_obsolete(&paths.staging) {
            Ok(()) => true,
            Err(error) => {
                log::warn!(
                    "deferred stale WAL staging cleanup for {}: {}",
                    paths.staging.display(),
                    error
                );
                false
            }
        }
    } else {
        true
    };

    let initialized = match selected {
        Selected::Empty(snapshot) => {
            let wal = match fresh_header {
                Some(header) => {
                    let granularity = requested_granularity_nanos
                        .unwrap_or_else(|| V1CodecProbe::granularity(&header).unwrap());
                    let header = V2CodecProbe::encode_header(V2HeaderProbeFields {
                        kind: header[12],
                        granularity_nanos: granularity,
                        base_bucket: 0,
                        segment_id: 0,
                        segment_base: 0,
                    });
                    initialize_fresh_v2(paths, &header, durability_policy)?
                }
                None => WalStorage::try_new_file_based(&paths.active).map_err(|source| {
                    io_failure(RecoveryOperation::CreateStaging, &paths.active, source)
                })?,
            };
            Ok(InitializedWal {
                snapshot,
                wal,
                status,
            })
        }
        Selected::ActiveTail {
            replay: accepted,
            header,
        } => {
            if header.get(8..10) == Some(2_u16.to_le_bytes().as_slice()) {
                let accepted_len = usize::try_from(accepted.byte_len).map_err(|_| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                let replacement = active_bytes
                    .as_deref()
                    .and_then(|bytes| bytes.get(..accepted_len))
                    .ok_or_else(|| RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    })?
                    .to_vec();
                let expected_snapshot = accepted.snapshot.clone();
                let completed = publish_validated_repair_with_policy(
                    paths,
                    RepairAuthority::Active {
                        obsolete_recovery: legacy_bytes.as_deref(),
                    },
                    &replacement,
                    |persisted| {
                        replay(persisted)
                            .is_ok_and(|validated| validated.snapshot == expected_snapshot)
                    },
                    durability_policy,
                )?;
                let offset = u64::try_from(replacement.len()).map_err(|_| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                let initialized = InitializedWal {
                    snapshot: accepted.snapshot,
                    wal: WalStorage::from_prepared_file_v2_with_timestamp_state(
                        completed.handle,
                        offset,
                        accepted.granularity_nanos,
                        accepted.last_bucket,
                    ),
                    status: completed.status,
                };
                initialized.wal.set_runtime_policy(durability_policy);
                return Ok(initialized);
            }
            let header: [u8; V1CodecProbe::HEADER_LEN] =
                header
                    .try_into()
                    .map_err(|_| RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    })?;
            let granularity = requested_granularity_nanos.unwrap_or(accepted.granularity_nanos);
            let repair_header =
                header_with_granularity_and_base_bucket(header, granularity, accepted.last_bucket);
            let accepted_len =
                usize::try_from(accepted.byte_len).map_err(|_| RecoveryError::InvalidArtifact {
                    path: paths.active.clone(),
                })?;
            let mut replacement = active_bytes
                .as_deref()
                .and_then(|bytes| bytes.get(..accepted_len))
                .ok_or_else(|| RecoveryError::InvalidArtifact {
                    path: paths.active.clone(),
                })?
                .to_vec();
            replacement[..V1CodecProbe::HEADER_LEN].copy_from_slice(&repair_header);
            let expected_bytes = replacement.clone();
            let completed = publish_validated_repair_with_policy(
                paths,
                RepairAuthority::Active {
                    obsolete_recovery: legacy_bytes.as_deref(),
                },
                &replacement,
                |persisted| persisted == expected_bytes,
                durability_policy,
            )?;
            let offset =
                u32::try_from(replacement.len()).map_err(|_| RecoveryError::InvalidArtifact {
                    path: paths.active.clone(),
                })?;
            Ok(InitializedWal {
                snapshot: accepted.snapshot,
                wal: WalStorage::from_prepared_file_with_timestamp_state(
                    completed.handle,
                    offset,
                    granularity,
                    accepted.last_bucket,
                ),
                status: completed.status,
            })
        }
        Selected::Active(active) => {
            if active_version == Some(1)
                && staging_clean
                && requested_granularity_nanos
                    .is_some_and(|requested| requested != active.granularity_nanos)
            {
                let requested = requested_granularity_nanos.unwrap();
                let header: [u8; V1CodecProbe::HEADER_LEN] = active_bytes
                    .as_deref()
                    .and_then(|bytes| bytes.get(..V1CodecProbe::HEADER_LEN))
                    .and_then(|bytes| bytes.try_into().ok())
                    .expect("selected V1 active has a validated header");
                let replacement_header =
                    header_with_granularity_and_base_bucket(header, requested, active.last_bucket);
                let replacement = encode_repair(&active.snapshot, &replacement_header);
                let expected = active.snapshot.clone();
                let expected_last_bucket = active.last_bucket;
                let completed = publish_validated_repair_with_policy(
                    paths,
                    RepairAuthority::Active {
                        obsolete_recovery: legacy_bytes.as_deref(),
                    },
                    &replacement,
                    |persisted| {
                        replay(persisted).is_ok_and(|validated| {
                            validated.snapshot == expected
                                && validated.granularity_nanos == requested
                                && validated.last_bucket == expected_last_bucket
                        })
                    },
                    durability_policy,
                )?;
                let offset = u32::try_from(replacement.len()).map_err(|_| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                let initialized = InitializedWal {
                    snapshot: active.snapshot,
                    wal: WalStorage::from_prepared_file_with_timestamp_state(
                        completed.handle,
                        offset,
                        requested,
                        active.last_bucket,
                    ),
                    status: completed.status,
                };
                initialized.wal.set_runtime_policy(durability_policy);
                return Ok(initialized);
            }
            let legacy_clean = if legacy_exists {
                match remove_obsolete(&paths.legacy) {
                    Ok(()) => true,
                    Err(error) => {
                        log::warn!(
                            "deferred stale WAL recovery cleanup for {}: {}",
                            paths.legacy.display(),
                            error
                        );
                        false
                    }
                }
            } else {
                true
            };
            let validated_len = if active_is_v1 {
                active.byte_len
            } else if staging_clean && legacy_clean {
                let replacement = encode(&active.snapshot);
                let expected = active.snapshot.clone();
                publish_replacement_with_policy(
                    paths,
                    &replacement,
                    |bytes| replay(bytes).is_ok_and(|result| result.snapshot == expected),
                    &mut |_| Ok(()),
                    durability_policy,
                )?
            } else {
                active.byte_len
            };
            let wal = if active_version == Some(2) {
                WalStorage::try_open_file_based_v2_with_timestamp_state(
                    &paths.active,
                    validated_len,
                    active.granularity_nanos,
                    active.last_bucket,
                )
            } else if active_version == Some(1) {
                WalStorage::try_open_file_based_v1_with_timestamp_state(
                    &paths.active,
                    validated_len,
                    active.granularity_nanos,
                    active.last_bucket,
                )
            } else {
                WalStorage::try_open_file_based(&paths.active, validated_len)
            }
            .map_err(|source| io_failure(RecoveryOperation::Open, &paths.active, source))?;
            Ok(InitializedWal {
                snapshot: active.snapshot,
                wal,
                status,
            })
        }
        Selected::Legacy(legacy) if !staging_clean => {
            if !legacy_is_v1 {
                return Err(RecoveryError::MigrationRequired {
                    path: paths.legacy.clone(),
                });
            }
            let wal = if legacy_version == Some(2) {
                WalStorage::try_open_file_based_v2_with_timestamp_state(
                    &paths.legacy,
                    legacy.byte_len,
                    legacy.granularity_nanos,
                    legacy.last_bucket,
                )
            } else {
                WalStorage::try_open_file_based_v1_with_timestamp_state(
                    &paths.legacy,
                    legacy.byte_len,
                    legacy.granularity_nanos,
                    legacy.last_bucket,
                )
            }
            .map_err(|source| io_failure(RecoveryOperation::Open, &paths.legacy, source))?;
            Ok(InitializedWal {
                snapshot: legacy.snapshot,
                wal,
                status,
            })
        }
        Selected::Legacy(legacy) => {
            if !legacy_is_v1 {
                return Err(RecoveryError::MigrationRequired {
                    path: paths.legacy.clone(),
                });
            }
            let replacement = legacy_bytes
                .as_ref()
                .expect("selected recovery bytes must exist")
                .clone();
            let expected = legacy.snapshot.clone();
            let completed = publish_validated_repair_with_policy(
                paths,
                RepairAuthority::Recovery {
                    obsolete_active: active_bytes.as_deref(),
                },
                &replacement,
                |bytes| replay(bytes).is_ok_and(|result| result.snapshot == expected),
                durability_policy,
            )?;
            let wal = if legacy_version == Some(2) {
                let offset = u64::try_from(replacement.len()).map_err(|_| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                WalStorage::from_prepared_file_v2_with_timestamp_state(
                    completed.handle,
                    offset,
                    legacy.granularity_nanos,
                    legacy.last_bucket,
                )
            } else {
                let offset = u32::try_from(replacement.len()).map_err(|_| {
                    RecoveryError::InvalidArtifact {
                        path: paths.active.clone(),
                    }
                })?;
                WalStorage::from_prepared_file_with_timestamp_state(
                    completed.handle,
                    offset,
                    legacy.granularity_nanos,
                    legacy.last_bucket,
                )
            };
            Ok(InitializedWal {
                snapshot: legacy.snapshot,
                wal,
                status: completed.status,
            })
        }
    }?;
    initialized.wal.set_runtime_policy(durability_policy);
    Ok(initialized)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::Path;
    use std::sync::{Mutex, Once};

    use crate::key_value_store::DurableKeyValueStore;
    use crate::wal::model::{KeyValueData, StoredAction};
    use crate::wal::replay::replay_key_value;
    use crate::RecoveryStatus;

    use super::{
        classify_artifacts, fail_cleanup_for, publish_replacement, ArtifactObservation,
        ArtifactPaths, PublicationCheckpoint, RecoveryDecision, RecoverySource, StoreKind,
    };

    struct TestLogger;
    static TEST_LOGGER: TestLogger = TestLogger;
    static TEST_LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static INSTALL_TEST_LOGGER: Once = Once::new();

    impl log::Log for TestLogger {
        fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
            true
        }

        fn log(&self, record: &log::Record<'_>) {
            TEST_LOGS.lock().unwrap().push(record.args().to_string());
        }

        fn flush(&self) {}
    }

    fn capture_logs() {
        INSTALL_TEST_LOGGER.call_once(|| {
            log::set_logger(&TEST_LOGGER).unwrap();
            log::set_max_level(log::LevelFilter::Trace);
        });
        TEST_LOGS.lock().unwrap().clear();
    }

    #[test]
    fn artifact_paths_and_active_authority_ignore_staging() {
        let directory = Path::new("store");
        let kv = ArtifactPaths::new(directory, StoreKind::Value);
        let set = ArtifactPaths::new(directory, StoreKind::Set);
        let map = ArtifactPaths::new(directory, StoreKind::Map);
        assert_eq!(kv.active, directory.join("kv.wal.dat"));
        assert_eq!(kv.legacy, directory.join(".kv.wal.dat"));
        assert_eq!(kv.staging, directory.join(".kv.wal.dat.next"));
        assert_eq!(set.active, directory.join("set.wal.dat"));
        assert_eq!(map.active, directory.join("map.wal.dat"));

        let active = ArtifactObservation::complete(kv.active.clone(), 0, ());
        let missing_active = ArtifactObservation::missing(kv.active.clone());
        let missing_legacy = ArtifactObservation::missing(kv.legacy.clone());
        let missing_staging = ArtifactObservation::missing(kv.staging.clone());
        assert_eq!(
            classify_artifacts(&active, &missing_legacy, &missing_staging, |_, _| false),
            RecoveryDecision::Use {
                source: RecoverySource::Active,
                status: RecoveryStatus::Normal,
            }
        );

        let incomplete_active = ArtifactObservation::<()>::Incomplete {
            path: kv.active.clone(),
            validated_len: 0,
        };
        assert!(matches!(
            incomplete_active,
            ArtifactObservation::Incomplete {
                validated_len: 0,
                ..
            }
        ));
        assert_eq!(
            classify_artifacts(
                &incomplete_active,
                &missing_legacy,
                &missing_staging,
                |_, _| false,
            ),
            RecoveryDecision::Invalid {
                path: kv.active.clone(),
            }
        );

        let invalid_legacy = ArtifactObservation::<()>::Invalid {
            path: kv.legacy.clone(),
        };
        assert_eq!(
            classify_artifacts(
                &missing_active,
                &invalid_legacy,
                &missing_staging,
                |_, _| false,
            ),
            RecoveryDecision::Invalid {
                path: kv.legacy.clone(),
            }
        );

        let active_with_legacy_prefix =
            ArtifactObservation::complete_with_prefixes(kv.active.clone(), 13, "new", vec!["old"]);
        let complete_legacy = ArtifactObservation::complete(kv.legacy.clone(), 7, "old");
        let missing_staging_for_prefix = ArtifactObservation::missing(kv.staging.clone());
        assert_eq!(
            classify_artifacts(
                &active_with_legacy_prefix,
                &complete_legacy,
                &missing_staging_for_prefix,
                |_, _| false,
            ),
            RecoveryDecision::Use {
                source: RecoverySource::Active,
                status: RecoveryStatus::Recovered,
            }
        );

        let complete_staging = ArtifactObservation::complete(kv.staging.clone(), 13, ());
        assert_eq!(
            classify_artifacts(&active, &missing_legacy, &complete_staging, |_, _| false),
            RecoveryDecision::Use {
                source: RecoverySource::Active,
                status: RecoveryStatus::Recovered,
            }
        );

        assert_eq!(
            classify_artifacts(
                &missing_active,
                &missing_legacy,
                &complete_staging,
                |_, _| false,
            ),
            RecoveryDecision::Use {
                source: RecoverySource::Empty,
                status: RecoveryStatus::Recovered,
            }
        );
    }

    fn append_action(bytes: &mut Vec<u8>, action: &StoredAction) {
        bytes.extend_from_slice(&action.act_type().to_ne_bytes());
        bytes.extend_from_slice(&action.crc().to_ne_bytes());
        let data_size =
            u32::try_from(action.data_size()).expect("legacy test payload must fit u32");
        bytes.extend_from_slice(&data_size.to_ne_bytes());
        bytes.extend_from_slice(action.data());
        bytes.extend_from_slice(&action.start_offset().to_ne_bytes());
    }

    fn compacted_kv_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        for (key, value) in [
            (b"alpha".to_vec(), b"uno".to_vec()),
            (b"empty".to_vec(), Vec::new()),
            (b"third".to_vec(), b"temporary".to_vec()),
        ] {
            let action =
                StoredAction::put_action(&(bytes.len() as u32), &KeyValueData::new(key, value));
            append_action(&mut bytes, &action);
        }
        let delete = StoredAction::delete_action(&(bytes.len() as u32), b"third");
        append_action(&mut bytes, &delete);
        bytes
    }

    #[test]
    fn publication_interruptions_leave_a_complete_authoritative_kv_wal() {
        let checkpoints = [
            PublicationCheckpoint::StagingCreated,
            PublicationCheckpoint::FirstRecordWritten,
            PublicationCheckpoint::MiddleRecordWritten,
            PublicationCheckpoint::Validated,
            PublicationCheckpoint::Synchronized,
            PublicationCheckpoint::Published,
        ];

        for target in checkpoints {
            let directory = tempfile::tempdir().unwrap();
            let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
            fs::copy(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy/kv.wal.dat"),
                &paths.active,
            )
            .unwrap();
            let replacement = compacted_kv_bytes();

            let result = publish_replacement(
                &paths,
                &replacement,
                |bytes| replay_key_value(bytes).is_ok(),
                &mut |checkpoint| {
                    if checkpoint == target {
                        Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "fault checkpoint",
                        ))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(
                result.is_err(),
                "checkpoint {target:?} should interrupt publication"
            );

            let reopen_error = match DurableKeyValueStore::try_init_new(directory.path()) {
                Ok(_) => panic!("complete legacy authority must require migration"),
                Err(error) => error,
            };
            assert!(matches!(
                reopen_error,
                crate::RecoveryError::MigrationRequired { path } if path == paths.active
            ));
            let replayed = replay_key_value(&fs::read(&paths.active).unwrap()).unwrap();
            assert_eq!(
                replayed.snapshot.get(b"alpha".as_slice()),
                Some(&b"uno".to_vec())
            );
            assert_eq!(
                replayed.snapshot.get(b"empty".as_slice()),
                Some(&Vec::new())
            );
            assert_eq!(replayed.snapshot.get(b"beta".as_slice()), None);
            assert_eq!(replayed.snapshot.get(b"third".as_slice()), None);
        }
    }

    #[test]
    fn cleanup_failure_keeps_active_usable_and_preserves_provenance() {
        capture_logs();
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let seed = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        seed.put(b"alpha".to_vec(), b"uno".to_vec());
        seed.put(b"empty".to_vec(), Vec::new());
        drop(seed);
        fs::copy(&paths.active, &paths.legacy).unwrap();
        let active_before = fs::read(&paths.active).unwrap();
        let _fault = fail_cleanup_for(paths.legacy.clone());

        let outcome = DurableKeyValueStore::try_init_new(directory.path()).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Recovered);
        assert_eq!(outcome.store().get(b"alpha"), Some(b"uno".to_vec()));
        outcome
            .store()
            .put(b"after-recovery".to_vec(), b"usable".to_vec());
        assert_eq!(
            outcome.store().get(b"after-recovery"),
            Some(b"usable".to_vec())
        );
        assert!(
            paths.legacy.exists(),
            "failed cleanup must retain provenance"
        );
        assert!(fs::read(&paths.active).unwrap().starts_with(&active_before));
        assert!(TEST_LOGS
            .lock()
            .unwrap()
            .iter()
            .any(|message| message.contains("deferred stale WAL recovery cleanup")));
    }

    #[test]
    fn publication_failure_reports_publish_operation_and_active_path() {
        let directory = tempfile::tempdir().unwrap();
        let paths = ArtifactPaths::new(directory.path(), StoreKind::Value);
        let replacement = compacted_kv_bytes();
        let result = publish_replacement(
            &paths,
            &replacement,
            |bytes| replay_key_value(bytes).is_ok(),
            &mut |checkpoint| {
                if checkpoint == PublicationCheckpoint::Synchronized {
                    fs::create_dir(&paths.active)?;
                }
                Ok(())
            },
        );
        match result {
            Err(crate::RecoveryError::Io {
                operation,
                path,
                source: _,
            }) => {
                assert_eq!(operation, crate::RecoveryOperation::Publish);
                assert_eq!(path, paths.active);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("publication fault unexpectedly succeeded"),
        }
    }
}
