//! Recovery-aware initialization contracts.
//!
//! Normal startup creates or opens the portable, boundary-aware V1 WAL through
//! validated same-directory staging. Complete legacy WALs are never rewritten
//! implicitly: callers receive [`RecoveryError::MigrationRequired`] and must use
//! the standalone `pigment-db-migrate` command. Truncated or corrupt input is
//! preserved for diagnosis.

use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

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
    /// A complete legacy WAL must be converted with the standalone migration tool.
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
            Self::MigrationRequired { path } => write!(
                formatter,
                "legacy WAL requires explicit migration with pigment-db-migrate: {}",
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
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
