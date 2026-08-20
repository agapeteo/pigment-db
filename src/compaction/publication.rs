//! Recoverable compaction-publication internals.

#![allow(dead_code)]

use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::manifest::{decode_manifest, encode_manifest, CompactionManifest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceArtifactPaths {
    pub(crate) manifest: PathBuf,
    pub(crate) manifest_next: PathBuf,
    pub(crate) staging: PathBuf,
    pub(crate) previous: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestPublishStage {
    Created,
    Written,
    Flushed,
    Renamed,
}

pub(crate) fn directory_artifact_paths(store_dir: &Path) -> io::Result<MaintenanceArtifactPaths> {
    artifact_paths(store_dir, true)
}

pub(crate) fn family_artifact_paths(active_path: &Path) -> io::Result<MaintenanceArtifactPaths> {
    artifact_paths(active_path, false)
}

fn artifact_paths(base: &Path, hidden: bool) -> io::Result<MaintenanceArtifactPaths> {
    let parent = base.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "maintenance scope has no parent directory",
        )
    })?;
    let base_name = base.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "maintenance scope has no native file name",
        )
    })?;
    let sibling = |suffix: &str| {
        let mut name = OsString::new();
        if hidden {
            name.push(".");
        }
        name.push(base_name);
        name.push(".pigment-compact.");
        name.push(suffix);
        parent.join(name)
    };
    Ok(MaintenanceArtifactPaths {
        manifest: sibling("manifest"),
        manifest_next: sibling("manifest.next"),
        staging: sibling("next"),
        previous: sibling("previous"),
    })
}

pub(crate) fn publish_manifest_buffered(
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
) -> io::Result<()> {
    publish_manifest_buffered_with_checkpoint(paths, manifest, |_| Ok(()))
}

pub(crate) fn publish_manifest_buffered_with_checkpoint(
    paths: &MaintenanceArtifactPaths,
    manifest: &CompactionManifest,
    mut checkpoint: impl FnMut(ManifestPublishStage) -> io::Result<()>,
) -> io::Result<()> {
    let encoded = encode_manifest(manifest)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))?;
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.manifest_next)?;
    checkpoint(ManifestPublishStage::Created)?;
    temporary.write_all(&encoded)?;
    checkpoint(ManifestPublishStage::Written)?;
    temporary.flush()?;
    checkpoint(ManifestPublishStage::Flushed)?;
    drop(temporary);
    fs::rename(&paths.manifest_next, &paths.manifest)?;
    checkpoint(ManifestPublishStage::Renamed)?;
    Ok(())
}

pub(crate) fn read_published_manifest(
    paths: &MaintenanceArtifactPaths,
) -> io::Result<Option<CompactionManifest>> {
    let encoded = match fs::read(&paths.manifest) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    decode_manifest(&encoded)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))
}

#[cfg(test)]
pub(crate) fn test_sentinel() {}
