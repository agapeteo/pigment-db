//! Current-format artifact inspection internals.

use std::collections::{btree_map::Entry, BTreeMap};
use std::io;
use std::path::{Path, PathBuf};

use crate::wal::recovery::canonical_sealed_segment_id;
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

impl InspectedFamily {
    fn active_name(self) -> &'static str {
        match self {
            Self::KeyValue => "kv.wal.dat",
            Self::KeySet => "set.wal.dat",
            Self::KeyMap => "map.wal.dat",
        }
    }
}

#[derive(Default)]
struct FamilyArtifacts {
    active: Option<PathBuf>,
    sealed: BTreeMap<u64, PathBuf>,
}

fn family_for_active_name(name: &std::ffi::OsStr) -> Option<InspectedFamily> {
    [
        InspectedFamily::KeyValue,
        InspectedFamily::KeySet,
        InspectedFamily::KeyMap,
    ]
    .into_iter()
    .find(|family| name == family.active_name())
}

fn sealed_descriptor(name: &std::ffi::OsStr) -> Option<(InspectedFamily, u64)> {
    [
        InspectedFamily::KeyValue,
        InspectedFamily::KeySet,
        InspectedFamily::KeyMap,
    ]
    .into_iter()
    .find_map(|family| {
        canonical_sealed_segment_id(name, family.active_name()).map(|id| (family, id))
    })
}

fn invalid_artifact(detail: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail)
}

fn validate_current_chain(
    family: InspectedFamily,
    sealed: &BTreeMap<u64, PathBuf>,
    active: &Path,
) -> io::Result<(u64, u64)> {
    let mut chain = Vec::new();
    let mut sealed_segment_bytes = 0_u64;
    for path in sealed.values() {
        let bytes = std::fs::read(path)?;
        let byte_len = u64::try_from(bytes.len())
            .map_err(|_| invalid_artifact("sealed segment length exceeds u64"))?;
        sealed_segment_bytes = sealed_segment_bytes
            .checked_add(byte_len)
            .ok_or_else(|| invalid_artifact("sealed segment byte total overflow"))?;
        chain.extend_from_slice(&bytes);
    }
    let active_bytes = std::fs::read(active)?;
    let active_byte_len = u64::try_from(active_bytes.len())
        .map_err(|_| invalid_artifact("active length exceeds u64"))?;
    chain.extend_from_slice(&active_bytes);
    let is_current_v2 = chain.starts_with(b"PIGWAL\r\n")
        && chain
            .get(8..10)
            .and_then(|version| version.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(2);
    if !is_current_v2 {
        return Err(invalid_artifact(
            "canonical active artifact is not current V2",
        ));
    }
    let replayed = match family {
        InspectedFamily::KeyValue => replay_key_value(&chain).map(|_| ()),
        InspectedFamily::KeySet => replay_key_set(&chain).map(|_| ()),
        InspectedFamily::KeyMap => replay_key_map(&chain).map(|_| ()),
    };
    replayed.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    Ok((active_byte_len, sealed_segment_bytes))
}

pub(crate) fn inspect_directory(store_dir: &Path) -> io::Result<DirectoryInspection> {
    let mut artifacts = BTreeMap::<InspectedFamily, FamilyArtifacts>::new();
    for entry in std::fs::read_dir(store_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid_artifact("canonical artifact is not a file"));
        }
        let name = entry.file_name();
        if let Some(family) = family_for_active_name(&name) {
            let family_artifacts = artifacts.entry(family).or_default();
            if family_artifacts.active.replace(entry.path()).is_some() {
                return Err(invalid_artifact("duplicate active family artifact"));
            }
        } else if let Some((family, id)) = sealed_descriptor(&name) {
            let family_artifacts = artifacts.entry(family).or_default();
            if let Entry::Occupied(_) = family_artifacts.sealed.entry(id) {
                return Err(invalid_artifact("duplicate sealed segment identifier"));
            }
            family_artifacts.sealed.insert(id, entry.path());
        } else {
            return Err(invalid_artifact("unexpected directory artifact"));
        }
    }

    let mut families = BTreeMap::new();
    for (family, artifacts) in artifacts {
        let active = artifacts
            .active
            .ok_or_else(|| invalid_artifact("sealed segment chain has no active artifact"))?;
        for (expected, actual) in (0_u64..).zip(artifacts.sealed.keys().copied()) {
            if expected != actual {
                return Err(invalid_artifact("sealed segment chain is not contiguous"));
            }
        }
        let (active_bytes, sealed_segment_bytes) =
            validate_current_chain(family, &artifacts.sealed, &active)?;
        let total_bytes = active_bytes
            .checked_add(sealed_segment_bytes)
            .ok_or_else(|| invalid_artifact("family byte total overflow"))?;
        families.insert(
            family,
            FamilyInspection {
                family,
                active_bytes,
                sealed_segment_bytes,
                sealed_segment_count: artifacts.sealed.len(),
                total_bytes,
            },
        );
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
