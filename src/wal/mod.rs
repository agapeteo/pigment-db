use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::RwLock;

use log::{error, info};

use crate::model::{SearchKey, SortedMapEntry, SortedMapKey};
use crate::wal::model::*;
use std::array::TryFromSliceError;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::path::Path;
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

struct WalState<W: Write> {
    offset: u32,
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
}

#[derive(Clone, Copy)]
enum WalFormat {
    Legacy,
    V1,
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

    fn try_open_file_based_with_format(
        file_path: &Path,
        validated_len: u64,
        format: WalFormat,
        granularity_nanos: u64,
        last_bucket: u64,
    ) -> std::io::Result<Self> {
        let offset = u32::try_from(validated_len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL length exceeds supported offset range",
            )
        })?;
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
                offset,
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
            }),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn sync_all(&self) -> std::io::Result<()> {
        self.wal_state.read().unwrap().writer.sync_all()
    }
}

impl WalStorage<Vec<u8>> {
    pub fn new_vec_based() -> Self {
        let vec = Vec::new();

        let wal_state = WalState {
            offset: 0,
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
                offset: header.len() as u32,
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
                offset: format::V1CodecProbe::HEADER_LEN as u32,
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
        let checkpoint = state.offset;
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
                    checkpoint,
                    PersistenceOperation::Write,
                    write_error,
                ));
            }
            if let Err(flush_error) = state.writer.flush() {
                return Err(rollback_or_fail(
                    &mut state,
                    checkpoint,
                    PersistenceOperation::Flush,
                    flush_error,
                ));
            }
            if let Err(barrier_error) = synchronize_if_physical(&mut state) {
                return Err(rollback_or_fail(
                    &mut state,
                    checkpoint,
                    PersistenceOperation::SynchronizeData,
                    barrier_error,
                ));
            }
            state.last_bucket = timestamp_bucket;
            state.offset = offset;
            return Ok(());
        }
        let (bytes, accepted_offset) = encode_compute_batch(checkpoint, actions);
        if let Err(write_error) = state.writer.write_all(&bytes) {
            return Err(rollback_or_fail(
                &mut state,
                checkpoint,
                PersistenceOperation::Write,
                write_error,
            ));
        }
        if let Err(flush_error) = state.writer.flush() {
            return Err(rollback_or_fail(
                &mut state,
                checkpoint,
                PersistenceOperation::Flush,
                flush_error,
            ));
        }
        state.offset = accepted_offset;
        Ok(())
    }

    pub(crate) fn try_store_put_event(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let key_value = KeyValueData::new(key, value);
        self.try_accept_action(|offset| StoredAction::put_action(offset, &key_value))?;
        Ok(key_value.owned_key_value())
    }

    #[cfg(test)]
    fn offset(&self) -> u32 {
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
        self.try_accept_action(|offset| StoredAction::delete_action(offset, key))
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
        self.try_accept_action(|offset| StoredAction::append_to_set(offset, &key_value))?;
        Ok(key_value.owned_key_value())
    }

    pub(crate) fn try_store_append_to_set_event_borrowed(
        &self,
        key: &[u8],
        value: Vec<u8>,
    ) -> std::io::Result<Vec<u8>> {
        self.try_accept_action(|offset| StoredAction::append_to_set_borrowed(offset, key, &value))?;
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
        self.try_accept_action(|offset| StoredAction::remove_from_set(offset, &key_value))?;
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
        let data_size = u32::try_from(data.len()).expect("sorted element exceeds WAL limit");
        let crc = model::crc(&data);
        self.try_accept_action(move |offset| {
            StoredAction::new(MAP_PUT_ACT, crc, data_size, data, *offset)
        })?;
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
            StoredAction::remove_from_sorted_map(offset, &sorted_map_key)
        })?;
        Ok(sorted_map_key.owned())
    }

    fn try_accept_action(&self, build: impl FnOnce(&u32) -> StoredAction) -> std::io::Result<()> {
        let mut state = self.wal_state.write().unwrap();
        ensure_ready(&state.health)?;
        let checkpoint = state.offset;
        let action = build(&checkpoint);
        if matches!(state.format, WalFormat::V1) {
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
                    checkpoint,
                    PersistenceOperation::Write,
                    write_error,
                ));
            }
            if let Err(flush_error) = state.writer.flush() {
                return Err(rollback_or_fail(
                    &mut state,
                    checkpoint,
                    PersistenceOperation::Flush,
                    flush_error,
                ));
            }
            if let Err(barrier_error) = synchronize_if_physical(&mut state) {
                return Err(rollback_or_fail(
                    &mut state,
                    checkpoint,
                    PersistenceOperation::SynchronizeData,
                    barrier_error,
                ));
            }
            state.last_bucket = timestamp_bucket;
            state.offset = accepted_offset;
            return Ok(());
        }
        if let Err(write_error) = write_fallible(&mut state.writer, &action) {
            return Err(rollback_or_fail(
                &mut state,
                checkpoint,
                PersistenceOperation::Write,
                write_error,
            ));
        }
        increment_offset(&mut state.offset, &action);
        Ok(())
    }
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
    checkpoint: u32,
    operation: PersistenceOperation,
    original: std::io::Error,
) -> std::io::Error {
    let truncate_result = match state.rollback {
        Some(rollback) => rollback(&mut state.writer, checkpoint as usize),
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

fn encode_compute_batch(start_offset: u32, actions: Vec<ComputeAction>) -> (Vec<u8>, u32) {
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
        bytes.extend_from_slice(&stored.data_size().to_ne_bytes());
        bytes.extend_from_slice(stored.data());
        bytes.extend_from_slice(&stored.start_offset().to_ne_bytes());
        increment_offset(&mut offset, &stored);
    }
    (bytes, offset)
}

fn write_fallible<W: Write>(file: &mut W, put_action: &StoredAction) -> std::io::Result<()> {
    file.write_all(&put_action.act_type().to_ne_bytes())?;
    file.write_all(&put_action.crc().to_ne_bytes())?;
    file.write_all(&put_action.data_size().to_ne_bytes())?;
    file.write_all(put_action.data())?;
    file.write_all(&put_action.start_offset().to_ne_bytes())?;
    file.flush()
}

fn increment_offset(offset: &mut u32, put_action: &StoredAction) {
    let fixed_block_len = FIXED_BLOCK_LEN as u32;
    let new_offset = put_action.start_offset() + put_action.data_size() + fixed_block_len;
    *offset = new_offset;
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

    StoredAction::new(act_type, crc, data_size, data, start_offset)
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
        assert_eq!(wal.offset(), state.bytes.len() as u32);
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
