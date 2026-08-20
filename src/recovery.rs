//! Recovery-aware initialization contracts.
//!
//! Normal startup creates or opens a portable V2 WAL segment chain through
//! validated same-directory staging. Complete legacy and V1 WALs are never
//! rewritten implicitly: callers receive [`RecoveryError::MigrationRequired`]
//! and must use the standalone `pigment-db-migrate` command. Truncated or
//! corrupt input is preserved for diagnosis.

use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::durability::DurabilitySupportError;
use crate::wal::format::V1CodecProbe;
use crate::wal::model::{
    DELETE_ACT, MAP_PUT_ACT, MAP_REMOVE_ACT, PUT_ACT, SET_APPEND_ACT, SET_REMOVE_ACT,
};
use crate::wal::replay::CheckedFrames;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeEnvelopeClassification {
    Current,
    RecognizedOlder,
    Invalid,
}

fn action_matches_family(expected_kind: u8, action: u8) -> bool {
    match expected_kind {
        1 => matches!(action, DELETE_ACT | PUT_ACT),
        2 => matches!(action, DELETE_ACT | SET_APPEND_ACT | SET_REMOVE_ACT),
        3 => matches!(action, DELETE_ACT | MAP_PUT_ACT | MAP_REMOVE_ACT),
        _ => false,
    }
}

fn complete_unversioned_envelope(bytes: &[u8], expected_kind: u8) -> bool {
    CheckedFrames::new(bytes)
        .all(|frame| frame.is_ok_and(|frame| action_matches_family(expected_kind, frame.action())))
}

fn complete_v1_envelope(bytes: &[u8], expected_kind: u8) -> bool {
    let header_valid = bytes.len() >= V1CodecProbe::HEADER_LEN
        && V1CodecProbe::magic_is_valid(bytes)
        && V1CodecProbe::version_is_valid(bytes)
        && V1CodecProbe::header_length_is_valid(bytes)
        && V1CodecProbe::kind_is_valid(bytes)
        && bytes[12] == expected_kind
        && V1CodecProbe::timestamp_unit_is_valid(bytes)
        && V1CodecProbe::flags_are_valid(bytes)
        && V1CodecProbe::granularity_is_valid(bytes)
        && V1CodecProbe::reserved_is_valid(bytes)
        && V1CodecProbe::header_crc_is_valid(bytes);
    if !header_valid {
        return false;
    }

    let mut offset = V1CodecProbe::HEADER_LEN;
    let mut group = None::<(u32, u32, u32, u64)>;
    let mut previous_timestamp = V1CodecProbe::base_bucket(bytes).unwrap_or(0);
    while offset < bytes.len() {
        let Some(payload_len) = bytes
            .get(offset + 6..offset + 10)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return false;
        };
        let Some(end) = offset
            .checked_add(V1CodecProbe::EMPTY_RECORD_LEN)
            .and_then(|fixed| fixed.checked_add(payload_len))
            .filter(|end| *end <= bytes.len())
        else {
            return false;
        };
        let frame = &bytes[offset..end];
        let action = frame[3];
        let structurally_valid = action_matches_family(expected_kind, action)
            && V1CodecProbe::record_marker_is_valid(frame)
            && V1CodecProbe::record_version_is_valid(frame)
            && V1CodecProbe::record_action_is_valid(frame)
            && V1CodecProbe::record_header_length_is_valid(frame)
            && V1CodecProbe::record_length_complement_is_valid(frame)
            && u32::try_from(offset)
                .is_ok_and(|start| V1CodecProbe::record_physical_start_is_valid(frame, start))
            && V1CodecProbe::record_mutation_start_is_valid(frame)
            && V1CodecProbe::record_index_count_are_valid(frame)
            && V1CodecProbe::record_timestamp_bucket(frame).is_some()
            && V1CodecProbe::record_crc_is_valid(frame);
        if !structurally_valid {
            return false;
        }

        let mutation_start = u32::from_le_bytes(frame[18..22].try_into().unwrap());
        let index = u32::from_le_bytes(frame[22..26].try_into().unwrap());
        let count = u32::from_le_bytes(frame[26..30].try_into().unwrap());
        let timestamp = u64::from_le_bytes(frame[30..38].try_into().unwrap());
        match group {
            None => {
                if index != 0
                    || usize::try_from(mutation_start).ok() != Some(offset)
                    || timestamp < previous_timestamp
                {
                    return false;
                }
                group = Some((mutation_start, 1, count, timestamp));
            }
            Some((expected_start, expected_index, expected_count, expected_timestamp)) => {
                if mutation_start != expected_start
                    || index != expected_index
                    || count != expected_count
                    || timestamp != expected_timestamp
                {
                    return false;
                }
                group = Some((
                    expected_start,
                    expected_index + 1,
                    expected_count,
                    timestamp,
                ));
            }
        }
        if group.is_some_and(|(_, next_index, count, _)| next_index == count) {
            previous_timestamp = timestamp;
            group = None;
        }
        offset = end;
    }
    group.is_none()
}

pub(crate) fn classify_runtime_envelope(
    bytes: &[u8],
    expected_kind: u8,
) -> RuntimeEnvelopeClassification {
    if bytes.starts_with(b"PIGWAL\r\n") {
        let version = bytes
            .get(8..10)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes);
        return match version {
            Some(2) => RuntimeEnvelopeClassification::Current,
            Some(1) if complete_v1_envelope(bytes, expected_kind) => {
                RuntimeEnvelopeClassification::RecognizedOlder
            }
            _ => RuntimeEnvelopeClassification::Invalid,
        };
    }
    if complete_unversioned_envelope(bytes, expected_kind) {
        RuntimeEnvelopeClassification::RecognizedOlder
    } else {
        RuntimeEnvelopeClassification::Invalid
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Describes whether initialization resolved interrupted-maintenance artifacts.
pub enum RecoveryStatus {
    /// The store opened without legacy recovery or staging artifacts.
    Normal,
    /// Startup safely resolved artifacts left by interrupted maintenance.
    Recovered,
}

#[must_use]
/// Owns an initialized store together with its recovery status.
pub struct RecoveryOutcome<S> {
    store: S,
    status: RecoveryStatus,
}

impl<S> RecoveryOutcome<S> {
    pub(crate) fn new(store: S, status: RecoveryStatus) -> Self {
        Self { store, status }
    }

    /// Returns the status reported by initialization.
    pub fn status(&self) -> RecoveryStatus {
        self.status
    }

    /// Borrows the initialized store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Consumes the outcome and returns the initialized store.
    pub fn into_store(self) -> S {
        self.store
    }

    /// Consumes the outcome and returns the store and status separately.
    pub fn into_parts(self) -> (S, RecoveryStatus) {
        (self.store, self.status)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Identifies the filesystem operation associated with a recovery I/O error.
pub enum RecoveryOperation {
    /// Inspecting whether a recognized artifact exists.
    Inspect,
    /// Opening or reading a recognized artifact.
    Open,
    /// Exclusively creating a same-directory staging artifact.
    CreateStaging,
    /// Writing, validating, or synchronizing staging.
    WriteStaging,
    /// Publishing completed staging under the active name.
    Publish,
    /// Removing an obsolete recovery or staging artifact.
    Cleanup,
}

#[derive(Debug)]
#[non_exhaustive]
/// A structured failure from a fallible durable-store initializer.
///
/// When authority cannot be established, recovery returns an error without
/// deleting or overwriting any potentially authoritative candidate.
pub enum RecoveryError {
    /// The requested durability policy cannot be honored by this platform or backing store.
    ///
    /// Capability failures occur before authority-changing startup work. Once
    /// all required preflights succeed, later failures use [`Self::Io`].
    UnsupportedDurability { source: DurabilitySupportError },
    /// A complete legacy or V1 WAL must be converted with the standalone migration tool.
    MigrationRequired { path: PathBuf },
    /// Replay provenance could not prove which candidate is authoritative.
    AuthorityUndetermined {
        active_path: Option<PathBuf>,
        recovery_path: Option<PathBuf>,
    },
    /// The only required source could not be replayed completely and safely.
    InvalidArtifact { path: PathBuf },
    /// A required filesystem operation failed.
    Io {
        operation: RecoveryOperation,
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedDurability { source } => write!(formatter, "{source}"),
            Self::MigrationRequired { path } => write!(
                formatter,
                "legacy or V1 WAL requires explicit migration with pigment-db-migrate: {}",
                path.display()
            ),
            Self::AuthorityUndetermined {
                active_path,
                recovery_path,
            } => write!(
                formatter,
                "could not determine authoritative WAL between active {active_path:?} and recovery {recovery_path:?}"
            ),
            Self::InvalidArtifact { path } => {
                write!(formatter, "invalid WAL artifact: {}", path.display())
            }
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "recovery {operation:?} failed for {}: {source}",
                path.display()
            ),
        }
    }
}

impl Error for RecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedDurability { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
