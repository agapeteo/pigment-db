//! Private storage-compaction implementation.

use std::path::Path;

use crate::{ClosedCompactionOptions, CompactionError, DirectoryCompactionOutcome};

pub(crate) mod inspection;
pub(crate) mod manifest;
pub(crate) mod publication;
pub(crate) mod recovery;

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
