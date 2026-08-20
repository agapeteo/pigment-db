//! Interrupted-compaction recovery internals.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct UntrustedMaintenanceEvidence {
    pub(crate) complete_generations: Vec<PathBuf>,
    pub(crate) invalid_generations: Vec<PathBuf>,
}

fn sibling_maintenance_path(store_dir: &Path, suffix: &str) -> io::Result<PathBuf> {
    let parent = store_dir.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "store directory has no parent for maintenance evidence",
        )
    })?;
    let leaf = store_dir.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "store directory has no leaf name for maintenance evidence",
        )
    })?;
    let mut name = OsString::from(".");
    name.push(leaf);
    name.push(".pigment-compact.");
    name.push(suffix);
    Ok(parent.join(name))
}

pub(crate) fn classify_untrusted_directory_generations(
    store_dir: &Path,
    mut generation_is_complete: impl FnMut(&Path) -> bool,
) -> io::Result<UntrustedMaintenanceEvidence> {
    let mut evidence = UntrustedMaintenanceEvidence::default();
    for suffix in ["next", "previous"] {
        let path = sibling_maintenance_path(store_dir, suffix)?;
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_dir() && generation_is_complete(&path) {
            evidence.complete_generations.push(path);
        } else {
            evidence.invalid_generations.push(path);
        }
    }
    Ok(evidence)
}

#[cfg(test)]
pub(crate) fn test_sentinel() {}
