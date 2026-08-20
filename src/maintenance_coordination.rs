//! Process-local ownership coordination for file-backed maintenance.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

#[derive(Default)]
struct OwnershipState {
    open_leases: usize,
    closed_claimed: bool,
}

fn registry() -> &'static Mutex<HashMap<PathBuf, OwnershipState>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, OwnershipState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_registry() -> MutexGuard<'static, HashMap<PathBuf, OwnershipState>> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn canonical_directory_identity(store_dir: &Path) -> io::Result<PathBuf> {
    if let Ok(canonical) = std::fs::canonicalize(store_dir) {
        return Ok(canonical);
    }
    let mut cursor = if store_dir.is_absolute() {
        store_dir.to_path_buf()
    } else {
        std::env::current_dir()?.join(store_dir)
    };
    let mut missing = Vec::<OsString>::new();
    loop {
        match std::fs::canonicalize(&cursor) {
            Ok(mut canonical) => {
                for component in missing.iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let leaf = cursor.file_name().ok_or(error)?;
                missing.push(leaf.to_os_string());
                cursor = cursor
                    .parent()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "store directory has no existing ancestor",
                        )
                    })?
                    .to_path_buf();
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OpenDirectoryLease {
    identity: PathBuf,
}

impl Drop for OpenDirectoryLease {
    fn drop(&mut self) {
        let mut registry = lock_registry();
        let remove = if let Some(state) = registry.get_mut(&self.identity) {
            state.open_leases = state.open_leases.saturating_sub(1);
            state.open_leases == 0 && !state.closed_claimed
        } else {
            false
        };
        if remove {
            registry.remove(&self.identity);
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ClosedDirectoryClaim {
    identity: PathBuf,
}

impl Drop for ClosedDirectoryClaim {
    fn drop(&mut self) {
        let mut registry = lock_registry();
        let remove = if let Some(state) = registry.get_mut(&self.identity) {
            state.closed_claimed = false;
            state.open_leases == 0
        } else {
            false
        };
        if remove {
            registry.remove(&self.identity);
        }
    }
}

pub(crate) fn acquire_open_lease(store_dir: &Path) -> io::Result<OpenDirectoryLease> {
    let identity = canonical_directory_identity(store_dir)?;
    let mut registry = lock_registry();
    let state = registry.entry(identity.clone()).or_default();
    if state.closed_claimed {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "closed maintenance already owns this directory",
        ));
    }
    state.open_leases = state
        .open_leases
        .checked_add(1)
        .ok_or_else(|| io::Error::other("open-store lease count overflow"))?;
    Ok(OpenDirectoryLease { identity })
}

#[allow(dead_code)]
pub(crate) fn try_claim_closed(store_dir: &Path) -> io::Result<ClosedDirectoryClaim> {
    let identity = canonical_directory_identity(store_dir)?;
    let mut registry = lock_registry();
    let state = registry.entry(identity.clone()).or_default();
    if state.closed_claimed || state.open_leases != 0 {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "an open store or closed maintenance operation already owns this directory",
        ));
    }
    state.closed_claimed = true;
    Ok(ClosedDirectoryClaim { identity })
}
