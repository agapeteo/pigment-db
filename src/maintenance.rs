//! Private maintenance API assembly point.

use std::io;
use std::path::Path;

use crate::compaction::inspection::{inspect_open_family, FamilyInspection, InspectedFamily};

pub(crate) fn file_family_storage_stats(
    store_dir: &Path,
    family: InspectedFamily,
) -> io::Result<FamilyInspection> {
    inspect_open_family(store_dir, family)
}

#[cfg(test)]
pub(crate) fn test_sentinel() {
    crate::compaction::test_sentinel();
    crate::wal::maintenance_test_sentinel();
}
