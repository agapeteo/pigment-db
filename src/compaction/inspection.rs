//! Current-format artifact inspection internals.

use std::io;
use std::path::Path;

use crate::wal::replay::{replay_key_map, replay_key_set, replay_key_value};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DirectoryInspection {
    pub(crate) families: Vec<FamilyInspection>,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FamilyInspection {
    pub(crate) family: InspectedFamily,
    pub(crate) active_bytes: u64,
    pub(crate) sealed_segment_bytes: u64,
    pub(crate) sealed_segment_count: usize,
    pub(crate) total_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum InspectedFamily {
    KeyValue,
    KeySet,
    KeyMap,
}

pub(crate) fn inspect_directory(store_dir: &Path) -> io::Result<DirectoryInspection> {
    let mut families = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(store_dir)? {
        let entry = entry?;
        let family = match entry.file_name().as_os_str() {
            name if name == "kv.wal.dat" => InspectedFamily::KeyValue,
            name if name == "set.wal.dat" => InspectedFamily::KeySet,
            name if name == "map.wal.dat" => InspectedFamily::KeyMap,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "non-active current-format inspection is not implemented",
                ));
            }
        };
        if !entry.file_type()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical active artifact is not a file",
            ));
        }
        let bytes = std::fs::read(entry.path())?;
        let is_current_v2 = bytes.starts_with(b"PIGWAL\r\n")
            && bytes
                .get(8..10)
                .and_then(|version| version.try_into().ok())
                .map(u16::from_le_bytes)
                == Some(2);
        if !is_current_v2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical active artifact is not current V2",
            ));
        }
        let replayed = match family {
            InspectedFamily::KeyValue => replay_key_value(&bytes).map(|_| ()),
            InspectedFamily::KeySet => replay_key_set(&bytes).map(|_| ()),
            InspectedFamily::KeyMap => replay_key_map(&bytes).map(|_| ()),
        };
        replayed.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        let active_bytes = u64::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "active length exceeds u64"))?;
        if families
            .insert(
                family,
                FamilyInspection {
                    family,
                    active_bytes,
                    sealed_segment_bytes: 0,
                    sealed_segment_count: 0,
                    total_bytes: active_bytes,
                },
            )
            .is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate active family artifact",
            ));
        }
    }
    let total_bytes = families.values().try_fold(0_u64, |total, family| {
        total.checked_add(family.total_bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "directory byte total overflow")
        })
    })?;
    Ok(DirectoryInspection {
        families: families.into_values().collect(),
        total_bytes,
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
