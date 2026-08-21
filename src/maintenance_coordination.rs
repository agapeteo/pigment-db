//! Process-local ownership coordination for file-backed maintenance.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub(crate) struct MaintenanceCoordinator {
    gate: RwLock<()>,
    active_attempt: AtomicU64,
    next_attempt: AtomicU64,
}

impl Default for MaintenanceCoordinator {
    fn default() -> Self {
        Self {
            gate: RwLock::new(()),
            active_attempt: AtomicU64::new(0),
            next_attempt: AtomicU64::new(1),
        }
    }
}

impl MaintenanceCoordinator {
    pub(crate) fn shared(&self) -> RwLockReadGuard<'_, ()> {
        self.gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn try_begin_online(&self) -> Result<OnlineAttemptToken<'_>, ()> {
        let token = self.next_attempt.fetch_add(1, Ordering::Relaxed).max(1);
        self.active_attempt
            .compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ())?;
        Ok(OnlineAttemptToken {
            coordinator: self,
            token,
        })
    }
}

#[derive(Debug)]
pub(crate) struct OnlineAttemptToken<'a> {
    coordinator: &'a MaintenanceCoordinator,
    token: u64,
}

impl OnlineAttemptToken<'_> {
    pub(crate) const fn id(&self) -> u64 {
        self.token
    }
}

impl Drop for OnlineAttemptToken<'_> {
    fn drop(&mut self) {
        let _ = self.coordinator.active_attempt.compare_exchange(
            self.token,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

pub(crate) struct OnlineAttemptGuard<'a, W: Write> {
    attempt: OnlineAttemptToken<'a>,
    #[allow(dead_code)]
    wal: &'a crate::wal::WalStorage<W>,
}

impl<'a, W: Write> OnlineAttemptGuard<'a, W> {
    pub(crate) fn claim(
        coordinator: &'a MaintenanceCoordinator,
        wal: &'a crate::wal::WalStorage<W>,
    ) -> Result<Self, ()> {
        let attempt = coordinator.try_begin_online()?;
        Ok(Self { attempt, wal })
    }

    pub(crate) fn activate_recorder(&self, max_delta_bytes: u64) -> Result<(), ()> {
        self.wal
            .activate_delta_recorder(self.attempt.id(), max_delta_bytes)
    }

    pub(crate) fn begin(
        coordinator: &'a MaintenanceCoordinator,
        wal: &'a crate::wal::WalStorage<W>,
        max_delta_bytes: u64,
    ) -> Result<Self, ()> {
        let guard = Self::claim(coordinator, wal)?;
        guard.activate_recorder(max_delta_bytes)?;
        Ok(guard)
    }

    pub(crate) const fn token(&self) -> u64 {
        self.attempt.id()
    }

    pub(crate) fn detach_recorder(&self) -> Option<crate::wal::DeltaRecorder> {
        self.wal.detach_delta_recorder(self.attempt.id())
    }
}

impl<W: Write> Drop for OnlineAttemptGuard<'_, W> {
    fn drop(&mut self) {
        let _exclusive = self.attempt.coordinator.exclusive();
        self.wal.clear_delta_recorder(self.attempt.id());
    }
}

pub(crate) struct StagingGenerationGuard {
    paths: crate::compaction::publication::MaintenanceArtifactPaths,
    operation_id: [u8; 16],
    durability: crate::DurabilityPolicy,
    owns_staging: bool,
}

impl StagingGenerationGuard {
    pub(crate) fn new(
        paths: crate::compaction::publication::MaintenanceArtifactPaths,
        operation_id: [u8; 16],
        durability: crate::DurabilityPolicy,
    ) -> Self {
        Self {
            paths,
            operation_id,
            durability,
            owns_staging: false,
        }
    }

    pub(crate) fn mark_staging_owned(&mut self) {
        self.owns_staging = true;
    }
}

impl Drop for StagingGenerationGuard {
    fn drop(&mut self) {
        let owned_manifest = matches!(
            crate::compaction::publication::read_published_manifest(&self.paths),
            Ok(Some(manifest))
                if manifest.operation_id == self.operation_id
                    && manifest.mode == crate::compaction::manifest::ManifestMode::OnlineFamily
                    && manifest.phase == crate::compaction::manifest::ManifestPhase::Prepared
                    && !manifest.source_finalized
        );
        if !owned_manifest {
            return;
        }
        if self.owns_staging {
            match std::fs::symlink_metadata(&self.paths.staging) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    if std::fs::remove_file(&self.paths.staging).is_err() {
                        return;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                _ => return,
            }
        }
        match std::fs::remove_file(&self.paths.manifest) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return,
        }
        if self.durability == crate::DurabilityPolicy::Physical {
            if let Some(parent) = self.paths.manifest.parent() {
                let _ = crate::durability::synchronize_directory(parent);
            }
        }
    }
}

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
