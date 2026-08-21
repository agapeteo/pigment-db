//! Private maintenance API assembly point.

use std::error::Error;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use crate::compaction::inspection::{
    error_classification, inspect_directory, inspect_open_family, DirectoryInspection,
    FamilyInspection, InspectedFamily, InspectionClassification,
};
use crate::{DurabilityPolicy, DurabilitySupportError};

/// Identifies one Pigment DB storage family.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StoreFamily {
    /// Key/value storage.
    KeyValue,
    /// Key/set storage.
    KeySet,
    /// Key/sorted-map storage.
    KeyMap,
}

/// Exact storage usage for one store family and authoritative generation.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyStorageStats {
    family: StoreFamily,
    active_bytes: u64,
    sealed_segment_bytes: u64,
    sealed_segment_count: usize,
    total_bytes: u64,
}

impl FamilyStorageStats {
    /// Returns the measured family.
    pub const fn family(&self) -> StoreFamily {
        self.family
    }

    /// Returns bytes in the active segment.
    pub const fn active_bytes(&self) -> u64 {
        self.active_bytes
    }

    /// Returns bytes in all sealed segments.
    pub const fn sealed_segment_bytes(&self) -> u64 {
        self.sealed_segment_bytes
    }

    /// Returns the number of sealed segments.
    pub const fn sealed_segment_count(&self) -> usize {
        self.sealed_segment_count
    }

    /// Returns active plus sealed bytes.
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Exact storage usage for all families in a database directory.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryStorageStats {
    families: Vec<FamilyStorageStats>,
    total_bytes: u64,
}

impl DirectoryStorageStats {
    /// Returns family statistics in deterministic family order.
    pub fn families(&self) -> &[FamilyStorageStats] {
        &self.families
    }

    /// Returns the checked sum of every family total.
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Options for closed, in-place directory compaction.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClosedCompactionOptions {
    durability_policy: DurabilityPolicy,
}

impl ClosedCompactionOptions {
    /// Returns the requested publication durability policy.
    pub const fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    /// Returns options using `durability_policy` for publication.
    pub const fn with_durability_policy(mut self, durability_policy: DurabilityPolicy) -> Self {
        self.durability_policy = durability_policy;
        self
    }
}

impl Default for ClosedCompactionOptions {
    fn default() -> Self {
        Self {
            durability_policy: DurabilityPolicy::Buffered,
        }
    }
}

/// Options for explicitly requested online compaction.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnlineCompactionOptions {
    max_delta_bytes: u64,
}

impl OnlineCompactionOptions {
    /// Returns the maximum encoded concurrent-delta bytes retained by an attempt.
    pub const fn max_delta_bytes(&self) -> u64 {
        self.max_delta_bytes
    }

    /// Returns options with the specified concurrent-delta byte limit.
    pub const fn with_max_delta_bytes(mut self, max_delta_bytes: u64) -> Self {
        self.max_delta_bytes = max_delta_bytes;
        self
    }
}

impl Default for OnlineCompactionOptions {
    fn default() -> Self {
        Self {
            max_delta_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Reports whether obsolete artifacts were removed after successful publication.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStatus {
    /// Publication and cleanup both completed.
    Complete,
    /// Publication completed, but safe cleanup remains pending.
    Pending,
}

/// Result of compacting one store family.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyCompactionOutcome {
    family: StoreFamily,
    before_bytes: u64,
    after_bytes: u64,
    sealed_segments_removed: usize,
    concurrent_mutations_replayed: usize,
    cleanup: CleanupStatus,
}

impl FamilyCompactionOutcome {
    pub(crate) const fn closed(
        family: StoreFamily,
        before_bytes: u64,
        after_bytes: u64,
        sealed_segments_removed: usize,
        cleanup: CleanupStatus,
    ) -> Self {
        Self {
            family,
            before_bytes,
            after_bytes,
            sealed_segments_removed,
            concurrent_mutations_replayed: 0,
            cleanup,
        }
    }

    pub(crate) const fn online(
        family: StoreFamily,
        before_bytes: u64,
        after_bytes: u64,
        sealed_segments_removed: usize,
        concurrent_mutations_replayed: usize,
        cleanup: CleanupStatus,
    ) -> Self {
        Self {
            family,
            before_bytes,
            after_bytes,
            sealed_segments_removed,
            concurrent_mutations_replayed,
            cleanup,
        }
    }

    /// Returns the compacted family.
    pub const fn family(&self) -> StoreFamily {
        self.family
    }

    /// Returns authoritative bytes before compaction.
    pub const fn before_bytes(&self) -> u64 {
        self.before_bytes
    }

    /// Returns authoritative bytes after compaction.
    pub const fn after_bytes(&self) -> u64 {
        self.after_bytes
    }

    /// Returns the number of sealed segments removed.
    pub const fn sealed_segments_removed(&self) -> usize {
        self.sealed_segments_removed
    }

    /// Returns accepted concurrent mutation groups replayed during online compaction.
    pub const fn concurrent_mutations_replayed(&self) -> usize {
        self.concurrent_mutations_replayed
    }

    /// Returns post-publication cleanup status.
    pub const fn cleanup(&self) -> CleanupStatus {
        self.cleanup
    }
}

/// Result of compacting every discovered family in a directory.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryCompactionOutcome {
    families: Vec<FamilyCompactionOutcome>,
}

impl DirectoryCompactionOutcome {
    /// Returns outcomes in deterministic family order.
    pub fn families(&self) -> &[FamilyCompactionOutcome] {
        &self.families
    }

    #[allow(dead_code)]
    pub(crate) fn empty() -> Self {
        Self {
            families: Vec::new(),
        }
    }

    pub(crate) fn from_families(families: Vec<FamilyCompactionOutcome>) -> Self {
        Self { families }
    }
}

/// Identifies the maintenance stage associated with a filesystem error.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionOperation {
    /// Reading and validating storage artifacts.
    Inspect,
    /// Capturing source state and byte identity.
    Capture,
    /// Writing replacement staging artifacts.
    WriteStaging,
    /// Reopening and validating staging.
    ValidateStaging,
    /// Publishing a maintenance manifest revision.
    WriteManifest,
    /// Publishing the retained previous generation.
    PublishPrevious,
    /// Publishing the validated replacement.
    PublishReplacement,
    /// Reopening the published replacement.
    ReopenReplacement,
    /// Removing descriptor-proven obsolete artifacts.
    Cleanup,
}

/// Structured failure from storage inspection or compaction.
#[non_exhaustive]
#[derive(Debug)]
pub enum CompactionError {
    /// A recognized older database must be converted with the external migration tool.
    MigrationRequired {
        /// Path to the recognized older artifact.
        path: PathBuf,
    },
    /// A required artifact is corrupt, malformed, incomplete, or unrecognized.
    InvalidArtifact {
        /// Path to the invalid artifact.
        path: PathBuf,
    },
    /// Available evidence cannot prove one authoritative generation.
    AuthorityUndetermined {
        /// Every path relevant to the unresolved authority decision.
        paths: Vec<PathBuf>,
    },
    /// Concurrent online mutations exceeded the caller's encoded-delta limit.
    ConcurrentDeltaLimitExceeded {
        /// Configured byte limit.
        limit: u64,
    },
    /// The requested durability policy cannot be honored.
    UnsupportedDurability {
        /// Platform or backing-store capability failure.
        source: DurabilitySupportError,
    },
    /// A filesystem operation failed.
    Io {
        /// Maintenance stage that failed.
        operation: CompactionOperation,
        /// Path operated on.
        path: PathBuf,
        /// Underlying I/O failure.
        source: io::Error,
    },
    /// The instance cannot safely continue the requested operation.
    FailedClosed {
        /// Actionable failure detail.
        detail: String,
    },
}

impl fmt::Display for CompactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MigrationRequired { path } => write!(
                formatter,
                "recognized older Pigment DB artifact at {} requires explicit migration with pigment-db-migrate",
                path.display()
            ),
            Self::InvalidArtifact { path } => {
                write!(formatter, "invalid Pigment DB artifact: {}", path.display())
            }
            Self::AuthorityUndetermined { paths } => {
                write!(formatter, "compaction authority is undetermined among {paths:?}")
            }
            Self::ConcurrentDeltaLimitExceeded { limit } => write!(
                formatter,
                "online compaction concurrent delta exceeded {limit} encoded bytes"
            ),
            Self::UnsupportedDurability { source } => write!(formatter, "{source}"),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "compaction {operation:?} failed for {}: {source}",
                path.display()
            ),
            Self::FailedClosed { detail } => {
                write!(formatter, "compaction failed closed: {detail}")
            }
        }
    }
}

impl Error for CompactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedDurability { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<InspectedFamily> for StoreFamily {
    fn from(family: InspectedFamily) -> Self {
        match family {
            InspectedFamily::KeyValue => Self::KeyValue,
            InspectedFamily::KeySet => Self::KeySet,
            InspectedFamily::KeyMap => Self::KeyMap,
        }
    }
}

impl From<FamilyInspection> for FamilyStorageStats {
    fn from(stats: FamilyInspection) -> Self {
        Self {
            family: stats.family.into(),
            active_bytes: stats.active_bytes,
            sealed_segment_bytes: stats.sealed_segment_bytes,
            sealed_segment_count: stats.sealed_segment_count,
            total_bytes: stats.total_bytes,
        }
    }
}

impl From<DirectoryInspection> for DirectoryStorageStats {
    fn from(stats: DirectoryInspection) -> Self {
        Self {
            families: stats.families.into_iter().map(Into::into).collect(),
            total_bytes: stats.total_bytes,
        }
    }
}

pub(crate) fn map_inspection_error(default_path: PathBuf, error: io::Error) -> CompactionError {
    match error_classification(&error).cloned() {
        Some(InspectionClassification::MigrationRequired { path }) => {
            CompactionError::MigrationRequired { path }
        }
        Some(InspectionClassification::InvalidArtifact { path }) => {
            CompactionError::InvalidArtifact { path }
        }
        Some(InspectionClassification::AuthorityUndetermined { paths }) => {
            CompactionError::AuthorityUndetermined { paths }
        }
        None if error.kind() == io::ErrorKind::InvalidData => {
            CompactionError::InvalidArtifact { path: default_path }
        }
        None => CompactionError::Io {
            operation: CompactionOperation::Inspect,
            path: default_path,
            source: error,
        },
    }
}

/// Inspects exact current-format storage usage without changing files or recovery state.
///
/// Inspection never repairs interrupted maintenance or legacy data. A
/// [`CompactionError::MigrationRequired`] result means the caller must run the
/// external `pigment-db-migrate` tool.
///
/// ```no_run
/// # fn main() -> Result<(), pigment_db::CompactionError> {
/// let stats = pigment_db::inspect_storage("database")?;
/// println!("{} bytes in {} families", stats.total_bytes(), stats.families().len());
/// # Ok(())
/// # }
/// ```
pub fn inspect_storage(
    store_dir: impl AsRef<Path>,
) -> Result<DirectoryStorageStats, CompactionError> {
    let store_dir = store_dir.as_ref();
    inspect_directory(store_dir)
        .map(Into::into)
        .map_err(|error| map_inspection_error(store_dir.to_path_buf(), error))
}

/// Compacts every current-format family in a closed directory in place.
///
/// The caller must close every store instance for `store_dir` before calling
/// this function. Pigment DB detects same-process overlap and returns
/// [`CompactionError::FailedClosed`] without changing storage.
///
/// [`CleanupStatus::Pending`] in a successful outcome means replacement
/// authority is established but obsolete evidence remains. Reopening the store
/// or invoking compaction again retries safe cleanup.
///
/// ```no_run
/// # fn main() -> Result<(), pigment_db::CompactionError> {
/// use pigment_db::{compact_directory_in_place, ClosedCompactionOptions};
/// let outcome = compact_directory_in_place("database", ClosedCompactionOptions::default())?;
/// for family in outcome.families() {
///     println!("{:?}: {} -> {} bytes", family.family(), family.before_bytes(), family.after_bytes());
/// }
/// # Ok(())
/// # }
/// ```
pub fn compact_directory_in_place(
    store_dir: impl AsRef<Path>,
    options: ClosedCompactionOptions,
) -> Result<DirectoryCompactionOutcome, CompactionError> {
    compact_directory_in_place_internal(store_dir.as_ref(), options)
}

pub(crate) fn compact_directory_in_place_internal(
    store_dir: &Path,
    options: ClosedCompactionOptions,
) -> Result<DirectoryCompactionOutcome, CompactionError> {
    crate::compaction::compact_closed_directory(store_dir, options)
}

pub(crate) fn file_family_storage_stats(
    store_dir: &Path,
    family: InspectedFamily,
) -> io::Result<FamilyInspection> {
    inspect_open_family(store_dir, family)
}

pub(crate) fn public_file_family_storage_stats(
    store_dir: &Path,
    family: InspectedFamily,
) -> Result<FamilyStorageStats, CompactionError> {
    file_family_storage_stats(store_dir, family)
        .map(Into::into)
        .map_err(|error| map_inspection_error(store_dir.to_path_buf(), error))
}

#[cfg(test)]
pub(crate) fn test_sentinel() {
    crate::compaction::test_sentinel();
    crate::wal::maintenance_test_sentinel();
}
