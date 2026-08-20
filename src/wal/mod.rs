use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::RwLock;

use log::{error, info};

use crate::model::{SearchKey, SortedMapEntry, SortedMapKey};
use crate::wal::model::*;
use std::array::TryFromSliceError;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_GRANULARITY_NANOS: u64 = 60_000_000_000;

fn system_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) mod model;
pub(crate) mod recovery;
pub(crate) mod replay;

pub(crate) mod format;

#[cfg(test)]
mod truncation_tests;

#[cfg(test)]
#[path = "ordering_tests.rs"]
mod ordering_tests;

#[cfg(test)]
mod durability_tests;

#[cfg(test)]
mod maintenance_tests;

#[cfg(test)]
pub(crate) fn maintenance_test_sentinel() {}

struct WalState<W: Write> {
    offset: u64,
    active_len: u64,
    writer: W,
    rollback: Option<fn(&mut W, usize) -> std::io::Result<()>>,
    health: WalHealth,
    format: WalFormat,
    granularity_nanos: u64,
    last_bucket: u64,
    clock: fn() -> u64,
    durability_policy: crate::config::DurabilityPolicy,
    data_barrier: Option<crate::durability::DataBarrier<W>>,
    rollback_barrier: Option<crate::durability::DataBarrier<W>>,
    rotation: Option<RotationSupport<W>>,
    frame_buffer: Vec<u8>,
    delta_recorder: Option<DeltaRecorder>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedFrame {
    action: u8,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedMutation {
    timestamp_bucket: u64,
    frames: Vec<RecordedFrame>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeltaRecordResult {
    Recorded,
    Overflowed,
    AlreadyOverflowed,
}

#[derive(Debug)]
struct DeltaRecorder {
    token: u64,
    limit: u64,
    used_bytes: u64,
    groups: Vec<RecordedMutation>,
    overflowed: bool,
}

impl DeltaRecorder {
    fn new(token: u64, limit: u64) -> Self {
        Self {
            token,
            limit,
            used_bytes: 0,
            groups: Vec::new(),
            overflowed: false,
        }
    }

    fn record_group(
        &mut self,
        payload_lengths: impl IntoIterator<Item = usize>,
        build: impl FnOnce() -> RecordedMutation,
    ) -> DeltaRecordResult {
        if self.overflowed {
            return DeltaRecordResult::AlreadyOverflowed;
        }
        let Some(encoded_len) = checked_current_v2_group_encoded_len(payload_lengths) else {
            self.mark_overflowed();
            return DeltaRecordResult::Overflowed;
        };
        let Some(next_used) = self.used_bytes.checked_add(encoded_len) else {
            self.mark_overflowed();
            return DeltaRecordResult::Overflowed;
        };
        if next_used > self.limit {
            self.mark_overflowed();
            return DeltaRecordResult::Overflowed;
        }
        let group = build();
        debug_assert_eq!(
            checked_current_v2_group_encoded_len(
                group.frames.iter().map(|frame| frame.payload.len())
            ),
            Some(encoded_len)
        );
        self.groups.push(group);
        self.used_bytes = next_used;
        DeltaRecordResult::Recorded
    }

    fn mark_overflowed(&mut self) {
        self.overflowed = true;
        self.used_bytes = 0;
        self.groups.clear();
        self.groups.shrink_to_fit();
    }
}

impl<W: Write> WalState<W> {
    fn activate_delta(&mut self, token: u64, limit: u64) -> Result<(), ()> {
        if self.delta_recorder.is_some() {
            return Err(());
        }
        self.delta_recorder = Some(DeltaRecorder::new(token, limit));
        Ok(())
    }

    fn detach_delta(&mut self, token: u64) -> Option<DeltaRecorder> {
        if self
            .delta_recorder
            .as_ref()
            .is_none_or(|recorder| recorder.token != token)
        {
            return None;
        }
        self.delta_recorder.take()
    }
}

fn checked_current_v2_group_encoded_len(
    payload_lengths: impl IntoIterator<Item = usize>,
) -> Option<u64> {
    payload_lengths.into_iter().try_fold(0_u64, |total, len| {
        total
            .checked_add(format::V2CodecProbe::EMPTY_RECORD_LEN as u64)?
            .checked_add(u64::try_from(len).ok()?)
    })
}

struct RotationSupport<W: Write> {
    state: FileRotationState,
    rotate: fn(
        &mut W,
        &mut FileRotationState,
        u64,
        u64,
        crate::config::DurabilityPolicy,
    ) -> std::io::Result<()>,
}

struct FileRotationState {
    active_path: PathBuf,
    kind: u8,
    segment_id: u64,
    segment_base: u64,
    target_bytes: u64,
    force_before_next_mutation: bool,
    failed_closed: bool,
}

#[derive(Clone, Copy)]
enum WalFormat {
    Legacy,
    V1,
    V2,
}

enum WalHealth {
    Ready,
    FailedRollback { original: String, rollback: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Identifies a persistence step that rejected or left a mutation indeterminate.
pub enum PersistenceOperation {
    /// Appending the complete encoded logical mutation.
    Write,
    /// Flushing language/runtime buffers to the operating system.
    Flush,
    /// Synchronizing accepted mutation data to physical storage.
    SynchronizeData,
    /// Truncating an unaccepted mutation back to its checkpoint.
    Rollback,
    /// Fully synchronizing the rollback checkpoint.
    SynchronizeRollback,
}

#[derive(Debug)]
#[non_exhaustive]
/// Structured cause carried by fallible mutation [`std::io::Error`] values.
pub enum MutationFailure {
    /// The attempted bytes were durably rolled back; the instance remains usable.
    Rejected {
        /// Persistence step that failed.
        operation: PersistenceOperation,
        /// Original persistence failure.
        source: std::io::Error,
    },
    /// The attempted bytes could not be conclusively rolled back; the instance
    /// has failed closed.
    Indeterminate {
        /// Persistence step that failed first.
        operation: PersistenceOperation,
        /// Original persistence failure.
        source: std::io::Error,
        /// Rollback step that could not be confirmed.
        rollback_operation: PersistenceOperation,
        /// Rollback failure.
        rollback: std::io::Error,
    },
    /// A later mutation was refused before I/O because an earlier rollback was
    /// indeterminate.
    FailedClosed {
        /// Diagnostic for the original persistence failure.
        original: String,
        /// Diagnostic for the rollback failure.
        rollback: String,
    },
}

#[cfg(test)]
pub(crate) use MutationFailure as PrivateMutationFailure;

impl MutationFailure {
    /// Recovers a structured persistence failure from a fallible mutator's
    /// [`std::io::Error`] value.
    pub fn from_io_error(error: &std::io::Error) -> Option<&Self> {
        error
            .get_ref()
            .and_then(|source| source.downcast_ref::<Self>())
    }
}

impl fmt::Display for MutationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { operation, source } => {
                write!(formatter, "persistence {operation:?} rejected: {source}")
            }
            Self::Indeterminate {
                operation,
                source,
                rollback_operation,
                rollback,
            } => write!(
                formatter,
                "persistence {operation:?} failed ({source}); {rollback_operation:?} failed ({rollback})"
            ),
            Self::FailedClosed { original, rollback } => write!(
                formatter,
                "WAL is failed closed after persistence failure ({original}) and rollback failure ({rollback})"
            ),
        }
    }
}

impl std::error::Error for MutationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected { source, .. } | Self::Indeterminate { source, .. } => Some(source),
            Self::FailedClosed { .. } => None,
        }
    }
}

pub struct WalStorage<W: Write> {
    wal_state: RwLock<WalState<W>>,
}

impl WalStorage<File> {
    #[allow(dead_code)]
    pub fn new_file_based(file_path: &Path) -> Self {
        Self::try_new_file_based(file_path).unwrap()
    }

    pub(crate) fn try_new_file_based(file_path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(file_path)?;

        let wal_state = WalState {
            offset: 0,
            active_len: 0,
            writer: file,
            rollback: Some(rollback_file),
            health: WalHealth::Ready,
            format: WalFormat::Legacy,
            granularity_nanos: DEFAULT_GRANULARITY_NANOS,
            last_bucket: 0,
            clock: system_unix_nanos,
            durability_policy: crate::config::DurabilityPolicy::Buffered,
            data_barrier: Some(crate::durability::synchronize_file_data),
            rollback_barrier: Some(crate::durability::synchronize_file_all),
            rotation: None,
            frame_buffer: Vec::new(),
            delta_recorder: None,
        };
        let wal_state = RwLock::new(wal_state);

        Ok(WalStorage { wal_state })
    }

    pub(crate) fn try_open_file_based(
        file_path: &Path,
        validated_len: u64,
    ) -> std::io::Result<Self> {
        Self::try_open_file_based_with_format(
            file_path,
            validated_len,
            WalFormat::Legacy,
            DEFAULT_GRANULARITY_NANOS,
            0,
        )
    }

    pub(crate) fn try_open_file_based_v1_with_timestamp_state(
        file_path: &Path,
        validated_len: u64,
        granularity_nanos: u64,
        last_bucket: u64,
    ) -> std::io::Result<Self> {
        Self::try_open_file_based_with_format(
            file_path,
            validated_len,
            WalFormat::V1,
            granularity_nanos,
            last_bucket,
        )
    }

    pub(crate) fn try_open_file_based_v2_with_timestamp_state(
        file_path: &Path,
        validated_len: u64,
        granularity_nanos: u64,
        last_bucket: u64,
    ) -> std::io::Result<Self> {
        Self::try_open_file_based_with_format(
            file_path,
            validated_len,
            WalFormat::V2,
            granularity_nanos,
            last_bucket,
        )
    }

    fn try_open_file_based_with_format(
        file_path: &Path,
        validated_len: u64,
        format: WalFormat,
        granularity_nanos: u64,
        last_bucket: u64,
    ) -> std::io::Result<Self> {
        let offset = validated_len;
        let file = OpenOptions::new().append(true).open(file_path)?;
        if file.metadata()?.len() != validated_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL length changed after validation",
            ));
        }

        Ok(Self {
            wal_state: RwLock::new(WalState {
                offset,
                active_len: validated_len,
                writer: file,
                rollback: Some(rollback_file),
                health: WalHealth::Ready,
                format,
                granularity_nanos,
                last_bucket,
                clock: system_unix_nanos,
                durability_policy: crate::config::DurabilityPolicy::Buffered,
                data_barrier: Some(crate::durability::synchronize_file_data),
                rollback_barrier: Some(crate::durability::synchronize_file_all),
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_prepared_file(file: File, offset: u32) -> Self {
        Self::from_prepared_file_with_timestamp_state(file, offset, DEFAULT_GRANULARITY_NANOS, 0)
    }

    pub(crate) fn from_prepared_file_with_timestamp_state(
        file: File,
        offset: u32,
        granularity_nanos: u64,
        last_bucket: u64,
    ) -> Self {
        Self {
            wal_state: RwLock::new(WalState {
                offset: u64::from(offset),
                active_len: u64::from(offset),
                writer: file,
                rollback: Some(rollback_file),
                health: WalHealth::Ready,
                format: WalFormat::V1,
                granularity_nanos,
                last_bucket,
                clock: system_unix_nanos,
                durability_policy: crate::config::DurabilityPolicy::Buffered,
                data_barrier: Some(crate::durability::synchronize_file_data),
                rollback_barrier: Some(crate::durability::synchronize_file_all),
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        }
    }

    pub(crate) fn from_prepared_file_v2_with_timestamp_state(
        file: File,
        offset: u64,
        granularity_nanos: u64,
        last_bucket: u64,
    ) -> Self {
        Self {
            wal_state: RwLock::new(WalState {
                offset,
                active_len: offset,
                writer: file,
                rollback: Some(rollback_file),
                health: WalHealth::Ready,
                format: WalFormat::V2,
                granularity_nanos,
                last_bucket,
                clock: system_unix_nanos,
                durability_policy: crate::config::DurabilityPolicy::Buffered,
                data_barrier: Some(crate::durability::synchronize_file_data),
                rollback_barrier: Some(crate::durability::synchronize_file_all),
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn sync_all(&self) -> std::io::Result<()> {
        self.wal_state.read().unwrap().writer.sync_all()
    }

    pub(crate) fn enable_file_rotation(
        &self,
        active_path: PathBuf,
        target_bytes: u64,
        requested_granularity_nanos: Option<u64>,
    ) -> std::io::Result<()> {
        let mut header = [0_u8; format::V2CodecProbe::HEADER_LEN];
        let mut header_file = File::open(&active_path)?;
        header_file.read_exact(&mut header)?;
        if !format::V2CodecProbe::header_is_valid(&header) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "active WAL is not a valid V2 segment",
            ));
        }
        let kind = format::V2CodecProbe::header_kind(&header).unwrap();
        let segment_id = format::V2CodecProbe::header_segment_id(&header).unwrap();
        let segment_base = format::V2CodecProbe::header_segment_base(&header).unwrap();
        let persisted_granularity = format::V2CodecProbe::header_granularity(&header).unwrap();
        let active_len = header_file.metadata()?.len();

        let mut state = self.wal_state.write().unwrap();
        if !matches!(state.format, WalFormat::V2) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "runtime rotation requires V2 WAL storage",
            ));
        }
        state.active_len = active_len;
        state.offset = segment_base.checked_add(active_len).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 global offset overflow")
        })?;
        let force_before_next_mutation =
            requested_granularity_nanos.is_some_and(|requested| requested != persisted_granularity);
        state.granularity_nanos = requested_granularity_nanos.unwrap_or(persisted_granularity);
        state.rotation = Some(RotationSupport {
            state: FileRotationState {
                active_path,
                kind,
                segment_id,
                segment_base,
                target_bytes,
                force_before_next_mutation,
                failed_closed: false,
            },
            rotate: rotate_file_segment,
        });
        Ok(())
    }
}

fn sealed_segment_path(active_path: &Path, segment_id: u64) -> std::io::Result<PathBuf> {
    let file_name = active_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "active WAL path has no file name",
        )
    })?;
    Ok(active_path.with_file_name(format!(
        "{}.segment-{segment_id:020}",
        file_name.to_string_lossy()
    )))
}

fn rotation_staging_path(active_path: &Path) -> std::io::Result<PathBuf> {
    let file_name = active_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "active WAL path has no file name",
        )
    })?;
    Ok(active_path.with_file_name(format!(".{}.next", file_name.to_string_lossy())))
}

fn rotate_file_segment(
    writer: &mut File,
    rotation: &mut FileRotationState,
    granularity_nanos: u64,
    last_bucket: u64,
    durability_policy: crate::config::DurabilityPolicy,
) -> std::io::Result<()> {
    let active_len = writer.metadata()?.len();
    let next_segment_id = rotation.segment_id.checked_add(1).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 segment id overflow")
    })?;
    let next_segment_base = rotation
        .segment_base
        .checked_add(active_len)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 segment base overflow")
        })?;
    let sealed_path = sealed_segment_path(&rotation.active_path, rotation.segment_id)?;
    let staging_path = rotation_staging_path(&rotation.active_path)?;
    let header = format::V2CodecProbe::encode_header(format::V2HeaderProbeFields {
        kind: rotation.kind,
        granularity_nanos,
        base_bucket: last_bucket,
        segment_id: next_segment_id,
        segment_base: next_segment_base,
    });

    let mut staging = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&staging_path)?;
    let prepare_result = (|| {
        staging.write_all(&header)?;
        staging.flush()?;
        if durability_policy == crate::config::DurabilityPolicy::Physical {
            staging.sync_all()?;
        }
        writer.flush()?;
        if durability_policy == crate::config::DurabilityPolicy::Physical {
            writer.sync_data()?;
        }
        Ok::<(), std::io::Error>(())
    })();
    if let Err(error) = prepare_result {
        drop(staging);
        let _ = std::fs::remove_file(&staging_path);
        return Err(error);
    }
    drop(staging);

    std::fs::rename(&rotation.active_path, &sealed_path)?;
    if let Err(publish_error) = std::fs::rename(&staging_path, &rotation.active_path) {
        let restore_result = std::fs::rename(&sealed_path, &rotation.active_path);
        return match restore_result {
            Ok(()) => Err(publish_error),
            Err(restore_error) => {
                rotation.failed_closed = true;
                Err(std::io::Error::other(format!(
                    "V2 rotation publication failed ({publish_error}); active restoration failed ({restore_error})"
                )))
            }
        };
    }
    rotation.segment_id = next_segment_id;
    rotation.segment_base = next_segment_base;
    let reopened = OpenOptions::new()
        .append(true)
        .open(&rotation.active_path)
        .inspect_err(|_| {
            rotation.failed_closed = true;
        })?;
    *writer = reopened;
    if durability_policy == crate::config::DurabilityPolicy::Physical {
        let parent = rotation
            .active_path
            .parent()
            .ok_or_else(|| std::io::Error::other("active WAL path has no parent"))?;
        if let Err(error) = crate::durability::synchronize_directory(parent) {
            rotation.failed_closed = true;
            return Err(error);
        }
    }
    Ok(())
}

impl WalStorage<Vec<u8>> {
    pub fn new_vec_based() -> Self {
        let vec = Vec::new();

        let wal_state = WalState {
            offset: 0,
            active_len: 0,
            writer: vec,
            rollback: Some(rollback_vec),
            health: WalHealth::Ready,
            format: WalFormat::Legacy,
            granularity_nanos: DEFAULT_GRANULARITY_NANOS,
            last_bucket: 0,
            clock: system_unix_nanos,
            durability_policy: crate::config::DurabilityPolicy::Buffered,
            data_barrier: None,
            rollback_barrier: None,
            rotation: None,
            frame_buffer: Vec::new(),
            delta_recorder: None,
        };
        let wal_state = RwLock::new(wal_state);

        WalStorage { wal_state }
    }

    pub(crate) fn new_vec_based_v1(header: &[u8; 40]) -> Self {
        Self::new_vec_based_v1_with_clock(header, system_unix_nanos)
    }

    pub(crate) fn new_vec_based_v1_with_clock(header: &[u8; 40], clock: fn() -> u64) -> Self {
        let valid = format::V1CodecProbe::magic_is_valid(header)
            && format::V1CodecProbe::version_is_valid(header)
            && format::V1CodecProbe::header_length_is_valid(header)
            && format::V1CodecProbe::kind_is_valid(header)
            && format::V1CodecProbe::timestamp_unit_is_valid(header)
            && format::V1CodecProbe::flags_are_valid(header)
            && format::V1CodecProbe::granularity_is_valid(header)
            && format::V1CodecProbe::reserved_is_valid(header)
            && format::V1CodecProbe::header_crc_is_valid(header);
        assert!(valid, "vector-backed V1 storage requires a valid header");

        Self {
            wal_state: RwLock::new(WalState {
                offset: header.len() as u64,
                active_len: header.len() as u64,
                writer: header.to_vec(),
                rollback: Some(rollback_vec),
                health: WalHealth::Ready,
                format: WalFormat::V1,
                granularity_nanos: format::V1CodecProbe::granularity(header).unwrap(),
                last_bucket: format::V1CodecProbe::base_bucket(header).unwrap_or(0),
                clock,
                durability_policy: crate::config::DurabilityPolicy::Buffered,
                data_barrier: None,
                rollback_barrier: None,
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_vec_based_v1_with_probe_options(
        header: &[u8; 40],
        options: crate::config::DurableStoreOptions,
    ) -> Self {
        let storage = Self::new_vec_based_v1(header);
        storage.wal_state.write().unwrap().durability_policy = options.durability_policy();
        storage
    }
}

impl<W: Write> WalStorage<W> {
    pub(crate) fn set_runtime_policy(&self, policy: crate::config::DurabilityPolicy) {
        self.wal_state.write().unwrap().durability_policy = policy;
    }

    #[cfg(test)]
    pub(crate) fn runtime_policy_probe(&self) -> crate::config::DurabilityPolicy {
        self.wal_state.read().unwrap().durability_policy
    }

    #[cfg(test)]
    pub(crate) fn dispatch_data_barrier_probe(
        &self,
        barrier: crate::durability::DataBarrier<W>,
    ) -> std::io::Result<()> {
        let mut state = self.wal_state.write().unwrap();
        crate::durability::synchronize_data(&mut state.writer, barrier)
    }

    #[cfg(test)]
    pub(crate) fn install_rollback_barrier_probe(
        &self,
        barrier: crate::durability::DataBarrier<W>,
    ) {
        self.wal_state.write().unwrap().rollback_barrier = Some(barrier);
    }

    #[cfg(test)]
    pub(crate) fn new_with_rollback(
        writer: W,
        rollback: fn(&mut W, usize) -> std::io::Result<()>,
    ) -> Self {
        Self {
            wal_state: RwLock::new(WalState {
                offset: 0,
                active_len: 0,
                writer,
                rollback: Some(rollback),
                health: WalHealth::Ready,
                format: WalFormat::Legacy,
                granularity_nanos: DEFAULT_GRANULARITY_NANOS,
                last_bucket: 0,
                clock: || 0,
                durability_policy: crate::config::DurabilityPolicy::Buffered,
                data_barrier: None,
                rollback_barrier: None,
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_v1_with_rollback(
        writer: W,
        rollback: fn(&mut W, usize) -> std::io::Result<()>,
    ) -> Self {
        Self {
            wal_state: RwLock::new(WalState {
                offset: format::V1CodecProbe::HEADER_LEN as u64,
                active_len: format::V1CodecProbe::HEADER_LEN as u64,
                writer,
                rollback: Some(rollback),
                health: WalHealth::Ready,
                format: WalFormat::V1,
                granularity_nanos: DEFAULT_GRANULARITY_NANOS,
                last_bucket: 0,
                clock: || 0,
                durability_policy: crate::config::DurabilityPolicy::Buffered,
                data_barrier: None,
                rollback_barrier: None,
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_v1_with_physical_probe(
        writer: W,
        rollback: fn(&mut W, usize) -> std::io::Result<()>,
        data_barrier: crate::durability::DataBarrier<W>,
    ) -> Self {
        let storage = Self::new_v1_with_rollback(writer, rollback);
        storage.wal_state.write().unwrap().durability_policy =
            crate::config::DurabilityPolicy::Physical;
        storage.wal_state.write().unwrap().data_barrier = Some(data_barrier);
        storage
    }

    #[cfg(test)]
    pub(crate) fn new_v2_with_physical_probe(
        writer: W,
        rollback: fn(&mut W, usize) -> std::io::Result<()>,
        data_barrier: crate::durability::DataBarrier<W>,
    ) -> Self {
        Self {
            wal_state: RwLock::new(WalState {
                offset: format::V2CodecProbe::HEADER_LEN as u64,
                active_len: format::V2CodecProbe::HEADER_LEN as u64,
                writer,
                rollback: Some(rollback),
                health: WalHealth::Ready,
                format: WalFormat::V2,
                granularity_nanos: DEFAULT_GRANULARITY_NANOS,
                last_bucket: 0,
                clock: || 0,
                durability_policy: crate::config::DurabilityPolicy::Physical,
                data_barrier: Some(data_barrier),
                rollback_barrier: None,
                rotation: None,
                frame_buffer: Vec::new(),
                delta_recorder: None,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn commit_compute_batch(&self, actions: Vec<ComputeAction>) -> std::io::Result<()> {
        self.commit_compute_batch_with_format(actions, false)
    }

    pub(crate) fn commit_set_compute_batch(
        &self,
        actions: Vec<ComputeAction>,
    ) -> std::io::Result<()> {
        self.commit_compute_batch_with_format(actions, true)
    }

    pub(crate) fn commit_map_compute_batch(
        &self,
        actions: Vec<ComputeAction>,
    ) -> std::io::Result<()> {
        self.commit_compute_batch_with_format(actions, true)
    }

    fn commit_compute_batch_with_format(
        &self,
        actions: Vec<ComputeAction>,
        encode_v1: bool,
    ) -> std::io::Result<()> {
        if actions.is_empty() {
            return Ok(());
        }

        let mut state = self.wal_state.write().unwrap();
        ensure_ready(&state.health)?;
        if matches!(state.format, WalFormat::V2) {
            let count = u32::try_from(actions.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "V2 compute group has too many actions",
                )
            })?;
            let stored = actions
                .into_iter()
                .map(|action| stored_compute_action(0, action))
                .collect::<Vec<_>>();
            let encoded_len = stored.iter().try_fold(0_u64, |total, action| {
                total
                    .checked_add(format::V2CodecProbe::EMPTY_RECORD_LEN as u64)
                    .and_then(|total| total.checked_add(action.data().len() as u64))
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "V2 compute group length overflow",
                        )
                    })
            })?;
            maybe_rotate_before(&mut state, encoded_len)?;
            let checkpoint = state.offset;
            let physical_checkpoint = state.active_len;
            let timestamp_bucket = requested_timestamp_bucket(&state);
            let mut bytes = Vec::new();
            let mut offset = checkpoint;
            for (index, stored) in stored.iter().enumerate() {
                let frame =
                    format::V2CodecProbe::encode_complete_record(format::V2RecordProbeFields {
                        action: stored.v2_act_type(),
                        payload: stored.data(),
                        physical_start: offset,
                        mutation_start: checkpoint,
                        index: index as u32,
                        count,
                        timestamp_bucket,
                    });
                offset = offset.checked_add(frame.len() as u64).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 WAL offset overflow")
                })?;
                bytes.extend_from_slice(&frame);
            }
            if let Err(write_error) = state.writer.write_all(&bytes) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Write,
                    write_error,
                ));
            }
            if let Err(flush_error) = state.writer.flush() {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Flush,
                    flush_error,
                ));
            }
            if let Err(barrier_error) = synchronize_if_physical(&mut state) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::SynchronizeData,
                    barrier_error,
                ));
            }
            state.last_bucket = timestamp_bucket;
            state.offset = offset;
            state.active_len = physical_checkpoint + encoded_len;
            record_compute_delta_if_active(&mut state, timestamp_bucket, &stored);
            return Ok(());
        }
        let checkpoint = u32::try_from(state.offset).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "legacy/V1 WAL offset exceeds u32",
            )
        })?;
        let physical_checkpoint = state.active_len;
        if encode_v1 && matches!(state.format, WalFormat::V1) {
            let count = u32::try_from(actions.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "V1 compute group has too many actions",
                )
            })?;
            let timestamp_bucket = requested_timestamp_bucket(&state);
            let mut bytes = Vec::new();
            let mut offset = checkpoint;
            for (index, action) in actions.into_iter().enumerate() {
                let stored = stored_compute_action(offset, action);
                legacy_data_size(&stored)?;
                let frame =
                    format::V1CodecProbe::encode_complete_record(format::RecordProbeFields {
                        action: *stored.act_type(),
                        payload: stored.data(),
                        physical_start: offset,
                        mutation_start: checkpoint,
                        index: index as u32,
                        count,
                        timestamp_bucket,
                    });
                offset = offset
                    .checked_add(u32::try_from(frame.len()).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "V1 frame length exceeds supported offset range",
                        )
                    })?)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "V1 WAL offset overflow",
                        )
                    })?;
                bytes.extend_from_slice(&frame);
            }
            if let Err(write_error) = state.writer.write_all(&bytes) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Write,
                    write_error,
                ));
            }
            if let Err(flush_error) = state.writer.flush() {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Flush,
                    flush_error,
                ));
            }
            if let Err(barrier_error) = synchronize_if_physical(&mut state) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::SynchronizeData,
                    barrier_error,
                ));
            }
            state.last_bucket = timestamp_bucket;
            state.offset = u64::from(offset);
            state.active_len = u64::from(offset);
            return Ok(());
        }
        let (bytes, accepted_offset) = encode_compute_batch(checkpoint, actions)?;
        if let Err(write_error) = state.writer.write_all(&bytes) {
            return Err(rollback_or_fail(
                &mut state,
                physical_checkpoint,
                PersistenceOperation::Write,
                write_error,
            ));
        }
        if let Err(flush_error) = state.writer.flush() {
            return Err(rollback_or_fail(
                &mut state,
                physical_checkpoint,
                PersistenceOperation::Flush,
                flush_error,
            ));
        }
        state.offset = u64::from(accepted_offset);
        state.active_len = u64::from(accepted_offset);
        Ok(())
    }

    pub(crate) fn try_store_put_event(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let key_value = KeyValueData::new(key, value);
        self.try_accept_action(|offset| StoredAction::prepare_put(offset, &key_value))?;
        Ok(key_value.owned_key_value())
    }

    #[cfg(test)]
    fn offset(&self) -> u64 {
        self.wal_state.read().unwrap().offset
    }

    #[cfg(test)]
    pub fn store_put_event(&self, key: Vec<u8>, value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        self.try_store_put_event(key, value)
            .unwrap_or_else(|error| panic!("WAL put rejected: {error}"))
    }

    #[cfg(test)]
    pub fn store_delete_event(&self, key: &[u8]) {
        self.try_store_delete_event(key)
            .unwrap_or_else(|error| panic!("WAL delete rejected: {error}"));
    }

    pub(crate) fn try_store_delete_event(&self, key: &[u8]) -> std::io::Result<()> {
        self.try_accept_action(|offset| StoredAction::prepare_delete(offset, key))
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn store_append_to_set_event(&self, key: Vec<u8>, set_key: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        self.try_store_append_to_set_event(key, set_key)
            .unwrap_or_else(|error| panic!("WAL set append rejected: {error}"))
    }

    #[cfg(test)]
    pub(crate) fn try_store_append_to_set_event(
        &self,
        key: Vec<u8>,
        set_key: Vec<u8>,
    ) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let key_value = KeyValueData::new(key, set_key);
        self.try_accept_action(|offset| StoredAction::prepare_append_to_set(offset, &key_value))?;
        Ok(key_value.owned_key_value())
    }

    pub(crate) fn try_store_append_to_set_event_borrowed(
        &self,
        key: &[u8],
        value: Vec<u8>,
    ) -> std::io::Result<Vec<u8>> {
        self.try_accept_action(|offset| {
            StoredAction::prepare_append_to_set_borrowed(offset, key, &value)
        })?;
        Ok(value)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn store_remove_from_set_event(&self, key: Vec<u8>, value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        self.try_store_remove_from_set_event(key, value)
            .unwrap_or_else(|error| panic!("WAL set removal rejected: {error}"))
    }

    pub(crate) fn try_store_remove_from_set_event(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let key_value = KeyValueData::new(key, value);
        self.try_accept_action(|offset| StoredAction::prepare_remove_from_set(offset, &key_value))?;
        Ok(key_value.owned_key_value())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn store_put_to_map_event(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        element: Vec<u8>,
    ) -> (Vec<u8>, SearchKey, Vec<u8>) {
        self.try_store_put_to_map_event(key, search_key, element)
            .unwrap_or_else(|error| panic!("WAL map put rejected: {error}"))
    }

    pub(crate) fn try_store_put_to_map_event(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        element: Vec<u8>,
    ) -> std::io::Result<(Vec<u8>, SearchKey, Vec<u8>)> {
        let entry = SortedMapEntry::new(key, search_key, element);
        let data = bincode::serialize(&entry).expect("sorted element should serialize");
        self.try_accept_action(move |offset| StoredAction::prepare_sorted_map_put(offset, data))?;
        Ok(entry.entry())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn store_remove_from_sorted_map_event(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
    ) -> (Vec<u8>, SearchKey) {
        self.try_store_remove_from_sorted_map_event(key, search_key)
            .unwrap_or_else(|error| panic!("WAL map removal rejected: {error}"))
    }

    pub(crate) fn try_store_remove_from_sorted_map_event(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
    ) -> std::io::Result<(Vec<u8>, SearchKey)> {
        let sorted_map_key = SortedMapKey::new(key, search_key);
        self.try_accept_action(|offset| {
            StoredAction::prepare_remove_from_sorted_map(offset, &sorted_map_key)
        })?;
        Ok(sorted_map_key.owned())
    }

    fn try_accept_action(&self, build: impl FnOnce(&u32) -> StoredAction) -> std::io::Result<()> {
        let mut action = build(&0);
        let mut state = self.wal_state.write().unwrap();
        ensure_ready(&state.health)?;
        if matches!(state.format, WalFormat::V2) {
            let encoded_len = (format::V2CodecProbe::EMPTY_RECORD_LEN as u64)
                .checked_add(action.data().len() as u64)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 frame length overflow")
                })?;
            maybe_rotate_before(&mut state, encoded_len)?;
            let checkpoint = state.offset;
            let physical_checkpoint = state.active_len;
            let timestamp_bucket = requested_timestamp_bucket(&state);
            let mut frame = std::mem::take(&mut state.frame_buffer);
            format::V2CodecProbe::encode_complete_record_into(
                &mut frame,
                format::V2RecordProbeFields {
                    action: action.v2_act_type(),
                    payload: action.data(),
                    physical_start: checkpoint,
                    mutation_start: checkpoint,
                    index: 0,
                    count: 1,
                    timestamp_bucket,
                },
            );
            let frame_len = frame.len() as u64;
            let accepted_offset = checkpoint.checked_add(frame_len).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 WAL offset overflow")
            })?;
            let write_result = state.writer.write_all(&frame);
            state.frame_buffer = frame;
            if let Err(write_error) = write_result {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Write,
                    write_error,
                ));
            }
            if let Err(flush_error) = state.writer.flush() {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Flush,
                    flush_error,
                ));
            }
            if let Err(barrier_error) = synchronize_if_physical(&mut state) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::SynchronizeData,
                    barrier_error,
                ));
            }
            state.last_bucket = timestamp_bucket;
            state.offset = accepted_offset;
            state.active_len = physical_checkpoint + encoded_len;
            record_single_delta_if_active(&mut state, timestamp_bucket, &action);
            return Ok(());
        }
        let checkpoint = u32::try_from(state.offset).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "legacy/V1 WAL offset exceeds u32",
            )
        })?;
        let physical_checkpoint = state.active_len;
        if matches!(state.format, WalFormat::V1) {
            legacy_data_size(&action)?;
            let timestamp_bucket = requested_timestamp_bucket(&state);
            let frame = format::V1CodecProbe::encode_complete_record(format::RecordProbeFields {
                action: *action.act_type(),
                payload: action.data(),
                physical_start: checkpoint,
                mutation_start: checkpoint,
                index: 0,
                count: 1,
                timestamp_bucket,
            });
            let accepted_offset = checkpoint
                .checked_add(u32::try_from(frame.len()).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "V1 frame length exceeds supported offset range",
                    )
                })?)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "V1 WAL offset overflow")
                })?;
            if let Err(write_error) = state.writer.write_all(&frame) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Write,
                    write_error,
                ));
            }
            if let Err(flush_error) = state.writer.flush() {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::Flush,
                    flush_error,
                ));
            }
            if let Err(barrier_error) = synchronize_if_physical(&mut state) {
                return Err(rollback_or_fail(
                    &mut state,
                    physical_checkpoint,
                    PersistenceOperation::SynchronizeData,
                    barrier_error,
                ));
            }
            state.last_bucket = timestamp_bucket;
            state.offset = u64::from(accepted_offset);
            state.active_len = u64::from(accepted_offset);
            return Ok(());
        }
        action.set_start_offset(checkpoint);
        action.ensure_payload_crc();
        if let Err(write_error) = write_fallible(&mut state.writer, &action) {
            return Err(rollback_or_fail(
                &mut state,
                physical_checkpoint,
                PersistenceOperation::Write,
                write_error,
            ));
        }
        let mut accepted_offset = checkpoint;
        increment_offset(&mut accepted_offset, &action)?;
        state.offset = u64::from(accepted_offset);
        state.active_len = u64::from(accepted_offset);
        Ok(())
    }
}

#[inline]
fn record_single_delta_if_active<W: Write>(
    state: &mut WalState<W>,
    timestamp_bucket: u64,
    action: &StoredAction,
) {
    let Some(recorder) = state.delta_recorder.as_mut() else {
        return;
    };
    let action_kind = action.v2_act_type();
    let payload = action.data();
    let _ = recorder.record_group([payload.len()], || RecordedMutation {
        timestamp_bucket,
        frames: vec![RecordedFrame {
            action: action_kind,
            payload: payload.to_vec(),
        }],
    });
}

#[inline]
fn record_compute_delta_if_active<W: Write>(
    state: &mut WalState<W>,
    timestamp_bucket: u64,
    actions: &[StoredAction],
) {
    let Some(recorder) = state.delta_recorder.as_mut() else {
        return;
    };
    let _ = recorder.record_group(actions.iter().map(|action| action.data().len()), || {
        RecordedMutation {
            timestamp_bucket,
            frames: actions
                .iter()
                .map(|action| RecordedFrame {
                    action: action.v2_act_type(),
                    payload: action.data().to_vec(),
                })
                .collect(),
        }
    });
}

fn maybe_rotate_before<W: Write>(
    state: &mut WalState<W>,
    encoded_mutation_len: u64,
) -> std::io::Result<()> {
    let should_rotate = state.rotation.as_ref().is_some_and(|rotation| {
        rotation.state.force_before_next_mutation
            || (state.active_len > format::V2CodecProbe::HEADER_LEN as u64
                && state
                    .active_len
                    .checked_add(encoded_mutation_len)
                    .is_none_or(|next_len| next_len > rotation.state.target_bytes))
    });
    if !should_rotate {
        return Ok(());
    }

    let mut rotation = state.rotation.take().expect("rotation support checked");
    let prior_segment_id = rotation.state.segment_id;
    let result = (rotation.rotate)(
        &mut state.writer,
        &mut rotation.state,
        state.granularity_nanos,
        state.last_bucket,
        state.durability_policy,
    );
    if result.is_ok() || rotation.state.segment_id != prior_segment_id {
        rotation.state.force_before_next_mutation = false;
        state.active_len = format::V2CodecProbe::HEADER_LEN as u64;
        state.offset = rotation
            .state
            .segment_base
            .checked_add(state.active_len)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "V2 global offset overflow")
            })?;
    }
    if result.is_err() && rotation.state.failed_closed {
        state.health = WalHealth::FailedRollback {
            original: "V2 segment rotation could not establish durable publication".to_owned(),
            rollback: "the WAL instance must be reopened before another mutation".to_owned(),
        };
    }
    state.rotation = Some(rotation);
    result
}

fn requested_timestamp_bucket<W: Write>(state: &WalState<W>) -> u64 {
    let now = (state.clock)();
    (now - now % state.granularity_nanos).max(state.last_bucket)
}

fn synchronize_if_physical<W: Write>(state: &mut WalState<W>) -> std::io::Result<()> {
    if state.durability_policy == crate::config::DurabilityPolicy::Buffered {
        return Ok(());
    }
    let barrier = state
        .data_barrier
        .ok_or_else(|| std::io::Error::other("physical data barrier is unavailable"))?;
    crate::durability::synchronize_data(&mut state.writer, barrier)
}

fn ensure_ready(health: &WalHealth) -> std::io::Result<()> {
    match health {
        WalHealth::Ready => Ok(()),
        WalHealth::FailedRollback { original, rollback } => {
            Err(std::io::Error::other(MutationFailure::FailedClosed {
                original: original.clone(),
                rollback: rollback.clone(),
            }))
        }
    }
}

fn rollback_or_fail<W: Write>(
    state: &mut WalState<W>,
    checkpoint: u64,
    operation: PersistenceOperation,
    original: std::io::Error,
) -> std::io::Error {
    let truncate_result = match state.rollback {
        Some(rollback) => match usize::try_from(checkpoint) {
            Ok(checkpoint) => rollback(&mut state.writer, checkpoint),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "rollback checkpoint exceeds platform usize",
            )),
        },
        None => Err(std::io::Error::other("rollback unavailable")),
    };
    if let Err(rollback_error) = truncate_result {
        return indeterminate_failure(
            state,
            operation,
            original,
            PersistenceOperation::Rollback,
            rollback_error,
        );
    }
    if state.durability_policy == crate::config::DurabilityPolicy::Physical {
        let sync_result = match state.rollback_barrier {
            Some(barrier) => crate::durability::synchronize_data(&mut state.writer, barrier),
            None => Err(std::io::Error::other(
                "rollback synchronization unavailable",
            )),
        };
        if let Err(rollback_error) = sync_result {
            return indeterminate_failure(
                state,
                operation,
                original,
                PersistenceOperation::SynchronizeRollback,
                rollback_error,
            );
        }
    }
    let kind = original.kind();
    std::io::Error::new(
        kind,
        MutationFailure::Rejected {
            operation,
            source: original,
        },
    )
}

fn indeterminate_failure<W: Write>(
    state: &mut WalState<W>,
    operation: PersistenceOperation,
    original: std::io::Error,
    rollback_operation: PersistenceOperation,
    rollback_error: std::io::Error,
) -> std::io::Error {
    let original_message = original.to_string();
    let rollback_message = rollback_error.to_string();
    state.health = WalHealth::FailedRollback {
        original: original_message,
        rollback: rollback_message,
    };
    std::io::Error::other(MutationFailure::Indeterminate {
        operation,
        source: original,
        rollback_operation,
        rollback: rollback_error,
    })
}

fn rollback_file(file: &mut File, checkpoint: usize) -> std::io::Result<()> {
    file.set_len(checkpoint as u64)
}

fn rollback_vec(bytes: &mut Vec<u8>, checkpoint: usize) -> std::io::Result<()> {
    bytes.truncate(checkpoint);
    Ok(())
}

pub(crate) enum ComputeAction {
    Delete {
        key: Vec<u8>,
    },
    SetAppend {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    SetRemove {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    MapPut {
        key: Vec<u8>,
        search_key: SearchKey,
        value: Vec<u8>,
    },
    MapRemove {
        key: Vec<u8>,
        search_key: SearchKey,
    },
}

fn stored_compute_action(offset: u32, action: ComputeAction) -> StoredAction {
    match action {
        ComputeAction::Delete { key } => StoredAction::delete_action(&offset, &key),
        ComputeAction::SetAppend { key, value } => {
            StoredAction::append_to_set(&offset, &KeyValueData::new(key, value))
        }
        ComputeAction::SetRemove { key, value } => {
            StoredAction::remove_from_set(&offset, &KeyValueData::new(key, value))
        }
        ComputeAction::MapPut {
            key,
            search_key,
            value,
        } => StoredAction::put_to_sorted_map(&offset, &SortedMapEntry::new(key, search_key, value)),
        ComputeAction::MapRemove { key, search_key } => {
            StoredAction::remove_from_sorted_map(&offset, &SortedMapKey::new(key, search_key))
        }
    }
}

fn encode_compute_batch(
    start_offset: u32,
    actions: Vec<ComputeAction>,
) -> std::io::Result<(Vec<u8>, u32)> {
    let mut bytes = Vec::new();
    let mut offset = start_offset;
    for action in actions {
        let stored = match action {
            ComputeAction::Delete { key } => StoredAction::delete_action(&offset, &key),
            ComputeAction::SetAppend { key, value } => {
                StoredAction::append_to_set(&offset, &KeyValueData::new(key, value))
            }
            ComputeAction::SetRemove { key, value } => {
                StoredAction::remove_from_set(&offset, &KeyValueData::new(key, value))
            }
            ComputeAction::MapPut {
                key,
                search_key,
                value,
            } => StoredAction::put_to_sorted_map(
                &offset,
                &SortedMapEntry::new(key, search_key, value),
            ),
            ComputeAction::MapRemove { key, search_key } => {
                StoredAction::remove_from_sorted_map(&offset, &SortedMapKey::new(key, search_key))
            }
        };
        bytes.extend_from_slice(&stored.act_type().to_ne_bytes());
        bytes.extend_from_slice(&stored.crc().to_ne_bytes());
        let data_size = legacy_data_size(&stored)?;
        bytes.extend_from_slice(&data_size.to_ne_bytes());
        bytes.extend_from_slice(stored.data());
        bytes.extend_from_slice(&stored.start_offset().to_ne_bytes());
        increment_offset(&mut offset, &stored)?;
    }
    Ok((bytes, offset))
}

fn write_fallible<W: Write>(file: &mut W, put_action: &StoredAction) -> std::io::Result<()> {
    file.write_all(&put_action.act_type().to_ne_bytes())?;
    file.write_all(&put_action.crc().to_ne_bytes())?;
    file.write_all(&legacy_data_size(put_action)?.to_ne_bytes())?;
    file.write_all(put_action.data())?;
    file.write_all(&put_action.start_offset().to_ne_bytes())?;
    file.flush()
}

fn legacy_data_size(action: &StoredAction) -> std::io::Result<u32> {
    u32::try_from(action.data_size()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "legacy/V1 WAL payload exceeds u32",
        )
    })
}

fn increment_offset(offset: &mut u32, put_action: &StoredAction) -> std::io::Result<()> {
    let fixed_block_len = FIXED_BLOCK_LEN as u32;
    *offset = put_action
        .start_offset()
        .checked_add(legacy_data_size(put_action)?)
        .and_then(|end| end.checked_add(fixed_block_len))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "legacy/V1 WAL offset overflow",
            )
        })?;
    Ok(())
}

#[allow(dead_code)]
pub fn read_forward(bytes: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut result = HashMap::new();
    if bytes.is_empty() {
        return result;
    }
    let mut offset = 0;

    while offset < bytes.len() {
        let stored_action = build_action(&mut offset, bytes);

        let actual_crc = model::crc(stored_action.data());
        if actual_crc != *stored_action.crc() {
            panic!("wrong crc !!"); // todo: better error handling
        }

        match *stored_action.act_type() {
            model::DELETE_ACT => {
                result.remove(stored_action.data());
            }
            model::PUT_ACT => {
                let put_action: KeyValueData = bincode::deserialize(stored_action.data())
                    .expect("KeyValueData should be deserialized");
                let (key, value) = put_action.owned_key_value();
                result.insert(key, value);
            }
            _ => {
                panic!("not supported action type: {}", stored_action.act_type())
            }
        }
    }
    result
}

#[allow(dead_code)]
pub fn read_for_set(bytes: &[u8]) -> HashMap<Vec<u8>, HashSet<Vec<u8>>> {
    let mut result = HashMap::new();
    if bytes.is_empty() {
        return result;
    }
    let mut offset = 0;

    while offset < bytes.len() {
        let stored_action = build_action(&mut offset, bytes);

        let actual_crc = model::crc(stored_action.data());
        if actual_crc != *stored_action.crc() {
            panic!("wrong crc !!"); // todo: better error handling
        }

        match *stored_action.act_type() {
            model::DELETE_ACT => {
                result.remove(stored_action.data());
            }
            model::SET_APPEND_ACT => {
                let put_action: KeyValueData = bincode::deserialize(stored_action.data())
                    .expect("KeyValueData should be deserialized");
                let (key, set_element) = put_action.owned_key_value();

                match result.get_mut(&key) {
                    None => {
                        let mut hashset = HashSet::new();
                        hashset.insert(set_element);
                        result.insert(key, hashset);
                    }
                    Some(hashset) => {
                        hashset.insert(set_element);
                    }
                }
            }
            model::SET_REMOVE_ACT => {
                let put_action: KeyValueData = bincode::deserialize(stored_action.data())
                    .expect("KeyValueData should be deserialized");
                let (key, value) = put_action.owned_key_value();
                match result.get_mut(&key) {
                    None => {}
                    Some(hashset) => {
                        hashset.remove(&value);
                    }
                }
            }
            _ => {
                panic!("not supported action type: {}", stored_action.act_type())
            }
        }
    }
    result
}

#[allow(dead_code)]
pub fn read_for_map(bytes: &[u8]) -> HashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>> {
    let mut result = HashMap::new();
    if bytes.is_empty() {
        return result;
    }
    let mut offset = 0;

    while offset < bytes.len() {
        let stored_action = build_action(&mut offset, bytes);

        let actual_crc = model::crc(stored_action.data());
        if actual_crc != *stored_action.crc() {
            panic!("wrong crc !!"); // todo: better error handling
        }

        match *stored_action.act_type() {
            DELETE_ACT => {
                result.remove(stored_action.data());
            }
            MAP_PUT_ACT => {
                let put_action: SortedMapEntry = bincode::deserialize(stored_action.data())
                    .expect("SortedMapEntry should be deserialized");
                let (key, search_key, element) = put_action.entry();

                match result.get_mut(&key) {
                    None => {
                        let mut map = BTreeMap::new();
                        map.insert(search_key, element);
                        result.insert(key, map);
                    }
                    Some(map) => {
                        map.insert(search_key, element);
                    }
                }
            }
            MAP_REMOVE_ACT => {
                let remove_action: SortedMapKey = bincode::deserialize(stored_action.data())
                    .expect("SortedMapEntry should be deserialized");
                let (key, search_key) = remove_action.owned();
                match result.get_mut(&key) {
                    None => {}
                    Some(map) => {
                        map.remove(&search_key);
                    }
                }
            }
            _ => {
                panic!("not supported action type: {}", stored_action.act_type())
            }
        }
    }
    result
}

#[allow(dead_code)]
fn build_action(offset: &mut usize, bytes: &[u8]) -> StoredAction {
    let act_type_len = ACT_TYPE_FIELD_LEN as usize;
    let act_type_arr: [u8; 1] = bytes[*offset..*offset + act_type_len].try_into().unwrap();
    let act_type = u8::from_ne_bytes(act_type_arr);
    *offset += act_type_len;

    let crc_len = CRC32_FIELD_LEN as usize;
    let crc_slice = &bytes[*offset..*offset + crc_len];
    let crc_arr: [u8; 4] = crc_slice.try_into().unwrap();
    let crc = u32::from_ne_bytes(crc_arr);
    *offset += &crc_len;

    let data_size_len = DATA_SIZE_FIELD_LEN as usize;
    let data_size_slice = &bytes[*offset..*offset + data_size_len];
    let data_size_arr: [u8; 4] = data_size_slice.try_into().unwrap();
    let data_size = u32::from_ne_bytes(data_size_arr);
    *offset += &data_size_len;

    let data_len = data_size as usize;
    let data_slice = &bytes[*offset..*offset + data_len];
    let data: Vec<u8> = Vec::from(data_slice);
    *offset += &data_len;

    let block_start_len = BLOCK_START_OFFSET_LEN as usize;
    let block_start_slice = &bytes[*offset..*offset + block_start_len];
    let block_start_arr: [u8; 4] = block_start_slice.try_into().unwrap();
    let start_offset = u32::from_ne_bytes(block_start_arr);
    *offset += &block_start_len;

    StoredAction::new(act_type, crc, data, start_offset)
}

#[allow(dead_code)]
pub fn collect(bytes: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    info!("trying to read result from end");
    match read_backward(bytes) {
        Ok(val) => val,
        Err(_) => {
            error!("error happened while reading from end, reading bytes from start");
            read_forward(bytes)
        }
    }
}

#[allow(dead_code)]
pub fn read_backward(bytes: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>, ()> {
    let mut result = HashMap::new();
    let mut removed_keys = HashSet::new();

    let size = bytes.len();
    let mut offset = match prev_block_start_offset(size, bytes) {
        Ok(val) => val,
        Err(_err) => {
            return Err(());
        }
    };

    let mut stored_action = build_action(&mut offset, bytes);

    update_backward_reading_map(&stored_action, &mut result, &mut removed_keys);

    let mut last_consumed = stored_action.start_offset() == &0;

    while !last_consumed {
        let mut offset =
            match prev_block_start_offset(*stored_action.start_offset() as usize, bytes) {
                Ok(val) => val,
                Err(_) => {
                    return Err(());
                }
            };
        stored_action = build_action(&mut offset, bytes);
        update_backward_reading_map(&stored_action, &mut result, &mut removed_keys);
        if stored_action.start_offset() == &0 {
            last_consumed = true;
        }
    }
    Ok(result)
}

#[allow(dead_code)]
fn update_backward_reading_map(
    stored_action: &StoredAction,
    map: &mut HashMap<Vec<u8>, Vec<u8>>,
    removed_keys: &mut HashSet<Vec<u8>>,
) {
    match *stored_action.act_type() {
        model::DELETE_ACT => {
            let key = stored_action.data().to_vec();
            if !map.contains_key(&key) {
                let valid_crc = valid_crc(stored_action.crc(), stored_action.data());
                if !valid_crc {
                    panic!("not valid crc"); // todo: revert to forward
                }
                removed_keys.insert(key);
            }
        }
        model::PUT_ACT => {
            let put_action: KeyValueData = bincode::deserialize(stored_action.data())
                .expect("KeyValueData should be deserialized");
            let (key, value) = put_action.owned_key_value();

            if !map.contains_key(&key) && !removed_keys.contains(&key) {
                let valid_crc = valid_crc(stored_action.crc(), stored_action.data());
                if !valid_crc {
                    panic!("not valid crc"); // todo: revert to forward
                }
                map.insert(key, value);
            }
        }
        _ => {
            panic!("not supported action type: {}", stored_action.act_type())
        }
    }
}

#[allow(dead_code)]
fn prev_block_start_offset(idx: usize, bytes: &[u8]) -> Result<usize, TryFromSliceError> {
    let block_start_len = BLOCK_START_OFFSET_LEN as usize;
    let block_start_slice = &bytes[idx - block_start_len..idx];
    let block_start_arr: [u8; 4] = block_start_slice.try_into()?;
    Ok(u32::from_ne_bytes(block_start_arr) as usize)
}

#[allow(dead_code)]
fn valid_crc(expected_crc: &u32, data: &[u8]) -> bool {
    let actual_crc = model::crc(data);
    actual_crc == *expected_crc
}

#[cfg(test)]
mod file_tests {
    use super::WalStorage;

    #[test]
    fn fallible_file_storage_syncs_and_reopens_at_validated_length() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal.dat");

        let wal = WalStorage::try_new_file_based(&path).expect("create new WAL");
        wal.store_put_event(b"first".to_vec(), b"value".to_vec());
        wal.sync_all().expect("synchronize WAL");
        let validated_len = std::fs::metadata(&path).unwrap().len();
        drop(wal);

        let reopened = WalStorage::try_open_file_based(&path, validated_len)
            .expect("open existing WAL for append");
        reopened.store_put_event(b"second".to_vec(), b"value".to_vec());
        reopened.sync_all().expect("synchronize appended WAL");

        let bytes = std::fs::read(path).unwrap();
        let map = super::read_forward(&bytes);
        assert_eq!(map.get(b"first".as_slice()), Some(&b"value".to_vec()));
        assert_eq!(map.get(b"second".as_slice()), Some(&b"value".to_vec()));
    }
}

#[cfg(test)]
mod tests {
    use super::{read_for_set, ComputeAction, WalStorage};
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct WriterState {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
        fail_after: Option<usize>,
        fail_flush: bool,
    }

    #[derive(Clone, Default)]
    struct CountingWriter(Arc<Mutex<WriterState>>);

    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let mut state = self.0.lock().unwrap();
            state.writes += 1;
            match state.fail_after {
                Some(0) => Err(io::Error::other("injected write failure")),
                Some(remaining) => {
                    let written = remaining.min(bytes.len());
                    state.bytes.extend_from_slice(&bytes[..written]);
                    state.fail_after = Some(remaining - written);
                    Ok(written)
                }
                None => {
                    state.bytes.extend_from_slice(bytes);
                    Ok(bytes.len())
                }
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            let mut state = self.0.lock().unwrap();
            state.flushes += 1;
            if state.fail_flush {
                Err(io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    fn truncate(writer: &mut CountingWriter, checkpoint: usize) -> io::Result<()> {
        writer.0.lock().unwrap().bytes.truncate(checkpoint);
        Ok(())
    }

    #[test]
    fn compute_batch_success_writes_legacy_frames_once() {
        let writer = CountingWriter::default();
        let state = Arc::clone(&writer.0);
        let wal = WalStorage::new_with_rollback(writer, truncate);

        wal.commit_compute_batch(vec![
            ComputeAction::SetAppend {
                key: b"key".to_vec(),
                value: b"new".to_vec(),
            },
            ComputeAction::SetRemove {
                key: b"key".to_vec(),
                value: b"old".to_vec(),
            },
        ])
        .unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.writes, 1);
        assert_eq!(state.flushes, 1);
        assert_eq!(
            read_for_set(&state.bytes).get(b"key".as_slice()),
            Some(&[b"new".to_vec()].into_iter().collect())
        );
        assert_eq!(wal.offset(), state.bytes.len() as u64);
    }

    #[test]
    fn compute_batch_write_failure_restores_prefix() {
        let writer = CountingWriter::default();
        let state = Arc::clone(&writer.0);
        let wal = WalStorage::new_with_rollback(writer, truncate);
        wal.commit_compute_batch(vec![ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"original".to_vec(),
        }])
        .unwrap();
        let prefix = state.lock().unwrap().bytes.clone();
        let offset = wal.offset();
        state.lock().unwrap().fail_after = Some(5);

        assert!(wal
            .commit_compute_batch(vec![ComputeAction::SetAppend {
                key: b"key".to_vec(),
                value: b"rejected".to_vec(),
            }])
            .is_err());

        assert_eq!(state.lock().unwrap().bytes, prefix);
        assert_eq!(wal.offset(), offset);
    }

    #[test]
    fn compute_batch_flush_failure_restores_prefix() {
        let writer = CountingWriter::default();
        let state = Arc::clone(&writer.0);
        let wal = WalStorage::new_with_rollback(writer, truncate);
        wal.commit_compute_batch(vec![ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"original".to_vec(),
        }])
        .unwrap();
        let prefix = state.lock().unwrap().bytes.clone();
        let offset = wal.offset();
        state.lock().unwrap().fail_flush = true;

        assert!(wal
            .commit_compute_batch(vec![ComputeAction::SetAppend {
                key: b"key".to_vec(),
                value: b"rejected".to_vec(),
            }])
            .is_err());

        assert_eq!(state.lock().unwrap().bytes, prefix);
        assert_eq!(wal.offset(), offset);
    }
}

#[ignore]
#[test]
fn test_with_file() {
    let file_path = ".../sandbox/dcache/wal.dat";
    let path = std::path::Path::new(file_path);

    if path.exists() {
        let _ = std::fs::remove_file(file_path);
    }
    let wal = WalStorage::new_file_based(Path::new(file_path));

    wal.store_put_event(b"x".to_vec(), b"X".to_vec());
    wal.store_put_event(b"a".to_vec(), b"A".to_vec());
    wal.store_put_event(b"a".to_vec(), b"AAA".to_vec());
    wal.store_put_event(b"b".to_vec(), b"B!".to_vec());
    wal.store_delete_event(b"x");

    let bytes = std::fs::read(file_path).unwrap();
    let map = read_forward(&bytes);

    assert_eq!(map.get(b"a".as_slice()), Some(&b"AAA".to_vec()));
    assert_eq!(map.get(b"b".as_slice()), Some(&b"B!".to_vec()));
    assert_eq!(map.len(), 2);
}

#[test]
fn test_with_vec() {
    let wal = WalStorage::new_vec_based();

    wal.store_put_event(b"x".to_vec(), b"X".to_vec());
    wal.store_put_event(b"a".to_vec(), b"A".to_vec());
    wal.store_put_event(b"a".to_vec(), b"AAA".to_vec());
    wal.store_put_event(b"b".to_vec(), b"B!".to_vec());
    wal.store_delete_event(b"x");

    let map = collect(&wal.wal_state.read().unwrap().writer);
    // let map = read_forward(&wal.wal_state.read().unwrap().writer);
    // let map = read_backward(&wal.wal_state.read().unwrap().writer).unwrap();
    assert_eq!(map.get(b"a".as_slice()), Some(&b"AAA".to_vec()));
    assert_eq!(map.get(b"b".as_slice()), Some(&b"B!".to_vec()));
    assert_eq!(map.len(), 2);
}

#[test]
#[ignore]
fn test_read_backward() {
    use memmap::MmapOptions;

    let file_name = ".../sandbox/dcache/wal.dat.bk";
    let file = File::open(file_name).unwrap();
    let content_as_slice = unsafe { MmapOptions::new().map(&file).unwrap() };
    let bytes = content_as_slice.as_ref();

    let result = read_backward(bytes).unwrap();

    println!("result size: {}", result.len());
    for (k, v) in result {
        println!(
            "key: {}, value: {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }
}
