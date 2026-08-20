//! Current-format artifact inspection internals.

use std::io;
use std::path::Path;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DirectoryInspection {
    pub(crate) families: Vec<FamilyInspection>,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FamilyInspection;

pub(crate) fn inspect_directory(store_dir: &Path) -> io::Result<DirectoryInspection> {
    let mut entries = std::fs::read_dir(store_dir)?;
    if entries.next().transpose()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "non-empty current-format inspection is not implemented",
        ));
    }
    Ok(DirectoryInspection {
        families: Vec::new(),
        total_bytes: 0,
    })
}

#[cfg(test)]
pub(crate) fn test_sentinel() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::maintenance_fixtures::snapshot_directory;

    #[test]
    fn empty_directory_is_zero_and_byte_identical() {
        let directory = tempfile::tempdir().unwrap();
        let before = snapshot_directory(directory.path()).unwrap();

        let inspected = inspect_directory(directory.path()).unwrap();

        assert!(inspected.families.is_empty());
        assert_eq!(inspected.total_bytes, 0);
        assert_eq!(snapshot_directory(directory.path()).unwrap(), before);
    }
}
