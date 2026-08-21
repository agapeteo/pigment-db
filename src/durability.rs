//! Durability capability errors and internal barrier implementation.

#[cfg(target_os = "windows")]
mod windows;

use crate::config::DurabilityPolicy;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Mutex;

pub(crate) type DataBarrier<W> = fn(&mut W) -> io::Result<()>;

pub(crate) fn synchronize_data<W>(writer: &mut W, barrier: DataBarrier<W>) -> io::Result<()> {
    barrier(writer)
}

pub(crate) fn synchronize_file_data(file: &mut File) -> io::Result<()> {
    file.sync_data()
}

pub(crate) fn synchronize_file_all(file: &mut File) -> io::Result<()> {
    file.sync_all()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurabilityCapability {
    /// Full synchronization of WAL file contents and metadata.
    FileContent,
    /// Full synchronization of an authority-changing parent-directory entry.
    DirectoryEntry,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum DurabilitySupportError {
    /// Physical durability cannot be implemented by vector-backed storage.
    NoPhysicalBacking,
    /// The target lacks a safe standard-library directory-entry barrier.
    UnsupportedPlatform {
        /// Rust's target operating-system name.
        platform: &'static str,
    },
    /// A required runtime preflight failed on the actual backing filesystem.
    RequiredBarrierUnavailable {
        /// Capability that could not be established.
        operation: DurabilityCapability,
        /// File or directory checked by the preflight.
        path: Option<PathBuf>,
        /// Original open or synchronization failure.
        source: io::Error,
    },
}

impl fmt::Display for DurabilitySupportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPhysicalBacking => {
                formatter.write_str("physical durability requires file-backed storage")
            }
            Self::UnsupportedPlatform { platform } => {
                write!(
                    formatter,
                    "physical durability is unsupported on {platform}"
                )
            }
            Self::RequiredBarrierUnavailable {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "required {operation:?} durability barrier is unavailable for {path:?}: {source}"
            ),
        }
    }
}

impl Error for DurabilitySupportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RequiredBarrierUnavailable { source, .. } => Some(source),
            _ => None,
        }
    }
}

pub(crate) fn validate_memory_backing(
    policy: DurabilityPolicy,
) -> Result<(), DurabilitySupportError> {
    match policy {
        DurabilityPolicy::Buffered => Ok(()),
        DurabilityPolicy::Physical => Err(DurabilitySupportError::NoPhysicalBacking),
    }
}

fn validate_compile_target_name(platform: &'static str) -> Result<(), DurabilitySupportError> {
    match platform {
        "linux" | "macos" | "windows" => Ok(()),
        _ => Err(DurabilitySupportError::UnsupportedPlatform { platform }),
    }
}

pub(crate) fn validate_compile_target() -> Result<(), DurabilitySupportError> {
    validate_compile_target_name(std::env::consts::OS)
}

fn unavailable(
    operation: DurabilityCapability,
    path: &Path,
    source: io::Error,
) -> DurabilitySupportError {
    DurabilitySupportError::RequiredBarrierUnavailable {
        operation,
        path: Some(path.to_path_buf()),
        source,
    }
}

#[cfg(target_os = "windows")]
#[allow(dead_code)]
pub(crate) fn preflight_windows_file_content(
    directory: &Path,
) -> Result<(), DurabilitySupportError> {
    #[cfg(test)]
    if let Some(source) = injected_preflight_failure(DurabilityCapability::FileContent, directory) {
        return Err(unavailable(
            DurabilityCapability::FileContent,
            directory,
            source,
        ));
    }
    windows::preflight_file_content(directory)
        .map_err(|source| unavailable(DurabilityCapability::FileContent, directory, source))
}

#[cfg(target_os = "windows")]
#[allow(dead_code)]
pub(crate) fn preflight_windows_namespace(directory: &Path) -> Result<(), DurabilitySupportError> {
    #[cfg(test)]
    if let Some(source) =
        injected_preflight_failure(DurabilityCapability::DirectoryEntry, directory)
    {
        return Err(unavailable(
            DurabilityCapability::DirectoryEntry,
            directory,
            source,
        ));
    }
    windows::preflight_namespace(directory)
        .map_err(|source| unavailable(DurabilityCapability::DirectoryEntry, directory, source))
}

#[cfg(test)]
static PREFLIGHT_FAULTS: Mutex<Vec<(DurabilityCapability, PathBuf, io::ErrorKind)>> =
    Mutex::new(Vec::new());

#[cfg(test)]
static DIRECTORY_BARRIER_FAULTS: Mutex<Vec<(PathBuf, usize, io::ErrorKind)>> =
    Mutex::new(Vec::new());

#[cfg(test)]
static DIRECTORY_BARRIER_CALLS: Mutex<Vec<(PathBuf, usize)>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) struct PreflightFaultGuard {
    operation: DurabilityCapability,
    path: PathBuf,
}

#[cfg(test)]
pub(crate) struct DirectoryBarrierFaultGuard {
    path: PathBuf,
    call: usize,
}

#[cfg(test)]
impl Drop for DirectoryBarrierFaultGuard {
    fn drop(&mut self) {
        let mut faults = DIRECTORY_BARRIER_FAULTS.lock().unwrap();
        let index = faults
            .iter()
            .position(|(path, call, _)| path == &self.path && call == &self.call)
            .expect("registered directory barrier fault must remain until its guard drops");
        faults.swap_remove(index);
    }
}

#[cfg(test)]
pub(crate) fn fail_directory_barrier_for(
    path: PathBuf,
    call: usize,
    kind: io::ErrorKind,
) -> DirectoryBarrierFaultGuard {
    DIRECTORY_BARRIER_FAULTS
        .lock()
        .unwrap()
        .push((path.clone(), call, kind));
    DirectoryBarrierFaultGuard { path, call }
}

#[cfg(test)]
pub(crate) fn directory_barrier_calls(path: &Path) -> usize {
    DIRECTORY_BARRIER_CALLS
        .lock()
        .unwrap()
        .iter()
        .find(|(candidate, _)| candidate == path)
        .map(|(_, calls)| *calls)
        .unwrap_or(0)
}

#[cfg(test)]
impl Drop for PreflightFaultGuard {
    fn drop(&mut self) {
        let mut faults = PREFLIGHT_FAULTS.lock().unwrap();
        let index = faults
            .iter()
            .position(|(operation, path, _)| operation == &self.operation && path == &self.path)
            .expect("registered preflight fault must remain until its guard drops");
        faults.swap_remove(index);
    }
}

#[cfg(test)]
pub(crate) fn fail_preflight_for(
    operation: DurabilityCapability,
    path: PathBuf,
    kind: io::ErrorKind,
) -> PreflightFaultGuard {
    PREFLIGHT_FAULTS
        .lock()
        .unwrap()
        .push((operation, path.clone(), kind));
    PreflightFaultGuard { operation, path }
}

#[cfg(test)]
fn injected_preflight_failure(operation: DurabilityCapability, path: &Path) -> Option<io::Error> {
    PREFLIGHT_FAULTS
        .lock()
        .unwrap()
        .iter()
        .find(|(candidate, candidate_path, _)| candidate == &operation && candidate_path == path)
        .map(|(_, _, kind)| io::Error::new(*kind, "injected durability preflight failure"))
}

pub(crate) fn preflight_directory(path: &Path) -> Result<(), DurabilitySupportError> {
    #[cfg(target_os = "windows")]
    {
        preflight_windows_file_content(path)?;
        preflight_windows_namespace(path)
    }
    #[cfg(not(target_os = "windows"))]
    {
        #[cfg(test)]
        if let Some(source) = injected_preflight_failure(DurabilityCapability::DirectoryEntry, path)
        {
            return Err(unavailable(
                DurabilityCapability::DirectoryEntry,
                path,
                source,
            ));
        }
        let directory = File::open(path)
            .map_err(|source| unavailable(DurabilityCapability::DirectoryEntry, path, source))?;
        directory
            .sync_all()
            .map_err(|source| unavailable(DurabilityCapability::DirectoryEntry, path, source))
    }
}

pub(crate) fn preflight_file(path: &Path) -> Result<(), DurabilitySupportError> {
    let file = File::open(path)
        .map_err(|source| unavailable(DurabilityCapability::FileContent, path, source))?;
    preflight_file_handle(&file, path)
}

pub(crate) fn preflight_file_handle(
    file: &File,
    path: &Path,
) -> Result<(), DurabilitySupportError> {
    #[cfg(test)]
    if let Some(source) = injected_preflight_failure(DurabilityCapability::FileContent, path) {
        return Err(unavailable(DurabilityCapability::FileContent, path, source));
    }
    file.sync_all()
        .map_err(|source| unavailable(DurabilityCapability::FileContent, path, source))
}

pub(crate) fn synchronize_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    {
        let call = {
            let mut calls = DIRECTORY_BARRIER_CALLS.lock().unwrap();
            if let Some((_, count)) = calls.iter_mut().find(|(candidate, _)| candidate == path) {
                *count += 1;
                *count
            } else {
                calls.push((path.to_path_buf(), 1));
                1
            }
        };
        if let Some(kind) = DIRECTORY_BARRIER_FAULTS
            .lock()
            .unwrap()
            .iter()
            .find(|(candidate, candidate_call, _)| candidate == path && *candidate_call == call)
            .map(|(_, _, kind)| *kind)
        {
            return Err(io::Error::new(
                kind,
                "injected directory publication barrier failure",
            ));
        }
    }
    let directory = File::open(path)?;
    directory.sync_all()
}

#[cfg(test)]
pub(crate) fn validate_compile_target_probe(
    platform: &'static str,
) -> Result<(), DurabilitySupportError> {
    validate_compile_target_name(platform)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DurabilityHarnessProbe;

#[cfg(test)]
impl DurabilityHarnessProbe {
    pub(crate) const fn new() -> Self {
        Self
    }
}
