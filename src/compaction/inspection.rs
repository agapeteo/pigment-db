//! Current-format artifact inspection internals.

use std::collections::{btree_map::Entry, BTreeMap};
use std::error::Error;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use crate::compaction::recovery::classify_untrusted_directory_generations;
use crate::recovery::{classify_runtime_envelope, RuntimeEnvelopeClassification};
use crate::wal::recovery::canonical_sealed_segment_id;
use crate::wal::replay::{
    classify_key_map_read_only, classify_key_set_read_only, classify_key_value_read_only,
    ValidationError,
};

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
#[allow(clippy::enum_variant_names)]
pub(crate) enum InspectedFamily {
    KeyValue,
    KeySet,
    KeyMap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InspectionClassification {
    MigrationRequired { path: PathBuf },
    InvalidArtifact { path: PathBuf },
    AuthorityUndetermined { paths: Vec<PathBuf> },
}

impl fmt::Display for InspectionClassification {
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
        }
    }
}

impl Error for InspectionClassification {}

pub(crate) fn error_classification(error: &io::Error) -> Option<&InspectionClassification> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<InspectionClassification>())
}

impl InspectedFamily {
    pub(crate) fn active_name(self) -> &'static str {
        match self {
            Self::KeyValue => "kv.wal.dat",
            Self::KeySet => "set.wal.dat",
            Self::KeyMap => "map.wal.dat",
        }
    }

    fn record_kind(self) -> u8 {
        match self {
            Self::KeyValue => 1,
            Self::KeySet => 2,
            Self::KeyMap => 3,
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

fn invalid_artifact_at(path: PathBuf) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        InspectionClassification::InvalidArtifact { path },
    )
}

fn migration_required(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        InspectionClassification::MigrationRequired {
            path: path.to_path_buf(),
        },
    )
}

fn checked_family_total(active_bytes: u64, sealed_segment_bytes: u64) -> io::Result<u64> {
    active_bytes
        .checked_add(sealed_segment_bytes)
        .ok_or_else(|| invalid_artifact("family byte total overflow"))
}

fn checked_directory_total<'a>(totals: impl IntoIterator<Item = &'a u64>) -> io::Result<u64> {
    totals.into_iter().try_fold(0_u64, |total, family| {
        total
            .checked_add(*family)
            .ok_or_else(|| invalid_artifact("directory byte total overflow"))
    })
}

pub(crate) fn exact_artifact_bytes_match(
    path: &Path,
    expected: &[u8],
    expected_checksum: u32,
) -> io::Result<bool> {
    let actual = std::fs::read(path)?;
    Ok(crc32fast::hash(&actual) == expected_checksum && actual == expected)
}

fn validate_current_chain(
    family: InspectedFamily,
    sealed: &BTreeMap<u64, PathBuf>,
    active: &Path,
) -> io::Result<(u64, u64)> {
    let mut chain = Vec::new();
    let mut sealed_boundaries = Vec::new();
    let mut sealed_segment_bytes = 0_u64;
    for path in sealed.values() {
        let bytes = std::fs::read(path)?;
        let byte_len = u64::try_from(bytes.len())
            .map_err(|_| invalid_artifact("sealed segment length exceeds u64"))?;
        sealed_segment_bytes = sealed_segment_bytes
            .checked_add(byte_len)
            .ok_or_else(|| invalid_artifact("sealed segment byte total overflow"))?;
        chain.extend_from_slice(&bytes);
        sealed_boundaries.push((chain.len(), path.clone()));
    }
    let active_bytes = std::fs::read(active)?;
    let active_byte_len = u64::try_from(active_bytes.len())
        .map_err(|_| invalid_artifact("active length exceeds u64"))?;
    chain.extend_from_slice(&active_bytes);
    if classify_runtime_envelope(&chain, family.record_kind())
        == RuntimeEnvelopeClassification::RecognizedOlder
    {
        let affected_path = sealed
            .values()
            .next()
            .map(PathBuf::as_path)
            .unwrap_or(active);
        return Err(migration_required(affected_path));
    }
    let is_current_v2 = chain.starts_with(b"PIGWAL\r\n")
        && chain
            .get(8..10)
            .and_then(|version| version.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(2);
    if !is_current_v2 {
        let path = sealed
            .values()
            .next()
            .cloned()
            .unwrap_or_else(|| active.to_path_buf());
        return Err(invalid_artifact_at(path));
    }
    let replayed = match family {
        InspectedFamily::KeyValue => classify_key_value_read_only(&chain).map(|_| ()),
        InspectedFamily::KeySet => classify_key_set_read_only(&chain).map(|_| ()),
        InspectedFamily::KeyMap => classify_key_map_read_only(&chain).map(|_| ()),
    };
    if let Err(error) = replayed {
        let offset = match error {
            ValidationError::Truncated { offset }
            | ValidationError::UnsupportedAction { offset, .. }
            | ValidationError::InvalidChecksum { offset }
            | ValidationError::InvalidStartOffset { offset, .. }
            | ValidationError::InvalidPayload { offset } => offset,
        };
        let path = sealed_boundaries
            .iter()
            .find(|(end, _)| offset < *end)
            .map(|(_, path)| path.clone())
            .unwrap_or_else(|| active.to_path_buf());
        return Err(invalid_artifact_at(path));
    }
    Ok((active_byte_len, sealed_segment_bytes))
}

fn inspect_family_artifacts(
    family: InspectedFamily,
    artifacts: FamilyArtifacts,
) -> io::Result<FamilyInspection> {
    let active = artifacts.active.ok_or_else(|| {
        artifacts
            .sealed
            .values()
            .next()
            .cloned()
            .map(invalid_artifact_at)
            .unwrap_or_else(|| invalid_artifact("family has no active artifact"))
    })?;
    for (expected, actual) in (0_u64..).zip(artifacts.sealed.keys().copied()) {
        if expected != actual {
            let path = artifacts
                .sealed
                .get(&actual)
                .cloned()
                .unwrap_or_else(|| active.clone());
            return Err(invalid_artifact_at(path));
        }
    }
    let (active_bytes, sealed_segment_bytes) =
        validate_current_chain(family, &artifacts.sealed, &active)?;
    let total_bytes = checked_family_total(active_bytes, sealed_segment_bytes)?;
    Ok(FamilyInspection {
        family,
        active_bytes,
        sealed_segment_bytes,
        sealed_segment_count: artifacts.sealed.len(),
        total_bytes,
    })
}

pub(crate) fn inspect_generation(store_dir: &Path) -> io::Result<DirectoryInspection> {
    let mut artifacts = BTreeMap::<InspectedFamily, FamilyArtifacts>::new();
    for entry in std::fs::read_dir(store_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid_artifact_at(entry.path()));
        }
        let name = entry.file_name();
        if let Some(family) = family_for_active_name(&name) {
            let family_artifacts = artifacts.entry(family).or_default();
            if family_artifacts.active.replace(entry.path()).is_some() {
                return Err(invalid_artifact_at(entry.path()));
            }
        } else if let Some((family, id)) = sealed_descriptor(&name) {
            let family_artifacts = artifacts.entry(family).or_default();
            if let Entry::Occupied(_) = family_artifacts.sealed.entry(id) {
                return Err(invalid_artifact_at(entry.path()));
            }
            family_artifacts.sealed.insert(id, entry.path());
        } else {
            return Err(invalid_artifact_at(entry.path()));
        }
    }

    let mut families = BTreeMap::new();
    for (family, artifacts) in artifacts {
        families.insert(family, inspect_family_artifacts(family, artifacts)?);
    }
    let family_totals = families.values().map(|family| &family.total_bytes);
    let total_bytes = checked_directory_total(family_totals)?;
    Ok(DirectoryInspection {
        families: families.into_values().collect(),
        total_bytes,
    })
}

pub(crate) fn inspect_open_family(
    store_dir: &Path,
    family: InspectedFamily,
) -> io::Result<FamilyInspection> {
    let mut artifacts = FamilyArtifacts::default();
    let sealed_prefix = format!("{}.segment-", family.active_name());
    for entry in std::fs::read_dir(store_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if family_for_active_name(&name) == Some(family) {
            if !entry.file_type()?.is_file() || artifacts.active.replace(entry.path()).is_some() {
                return Err(invalid_artifact_at(entry.path()));
            }
        } else if let Some(id) = canonical_sealed_segment_id(&name, family.active_name()) {
            if !entry.file_type()?.is_file() || artifacts.sealed.insert(id, entry.path()).is_some()
            {
                return Err(invalid_artifact_at(entry.path()));
            }
        } else if name
            .to_str()
            .is_some_and(|name| name.starts_with(&sealed_prefix))
        {
            return Err(invalid_artifact_at(entry.path()));
        }
    }
    inspect_family_artifacts(family, artifacts)
}

pub(crate) fn inspect_directory(store_dir: &Path) -> io::Result<DirectoryInspection> {
    let inspection = inspect_generation(store_dir)?;
    let evidence = classify_untrusted_directory_generations(store_dir, |path| {
        inspect_generation(path).is_ok_and(|generation| !generation.families.is_empty())
    })?;
    if !evidence.complete_generations.is_empty() {
        let mut paths = vec![store_dir.to_path_buf()];
        paths.extend(evidence.complete_generations);
        paths.extend(evidence.invalid_generations);
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            InspectionClassification::AuthorityUndetermined { paths },
        ));
    }
    if let Some(path) = evidence.invalid_generations.into_iter().next() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            InspectionClassification::InvalidArtifact { path },
        ));
    }
    Ok(inspection)
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

    #[test]
    fn synthetic_family_and_directory_totals_reject_overflow() {
        let family_error = checked_family_total(u64::MAX, 1).unwrap_err();
        assert_eq!(family_error.kind(), io::ErrorKind::InvalidData);
        assert!(family_error.to_string().contains("family"));

        let totals = [u64::MAX, 1];
        let directory_error = checked_directory_total(&totals).unwrap_err();
        assert_eq!(directory_error.kind(), io::ErrorKind::InvalidData);
        assert!(directory_error.to_string().contains("directory"));
    }
}
