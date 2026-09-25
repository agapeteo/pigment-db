//! Ownership of file-backed store directories: within a process through a registry of open leases
//! and closed-maintenance claims, and across processes through lock files (specs/011).

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub(crate) struct MaintenanceCoordinator {
    gate: RwLock<()>,
    coordinates_mutations: bool,
    active_attempt: AtomicU64,
    next_attempt: AtomicU64,
}

impl Default for MaintenanceCoordinator {
    fn default() -> Self {
        Self {
            gate: RwLock::new(()),
            coordinates_mutations: true,
            active_attempt: AtomicU64::new(0),
            next_attempt: AtomicU64::new(1),
        }
    }
}

impl MaintenanceCoordinator {
    pub(crate) fn disabled() -> Self {
        Self {
            gate: RwLock::new(()),
            coordinates_mutations: false,
            active_attempt: AtomicU64::new(0),
            next_attempt: AtomicU64::new(1),
        }
    }

    pub(crate) fn shared(&self) -> Option<RwLockReadGuard<'_, ()>> {
        self.coordinates_mutations.then(|| self.gate.read())
    }

    pub(crate) fn exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.gate.write()
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

    #[allow(dead_code)]
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

struct OwnershipState {
    open_leases: usize,
    closed_claimed: bool,
    /// Whether this entry's owners take lock files at all: false only for the staging reopen that
    /// a closed claim covers.
    takes_locks: bool,
    /// This process's hold on `<store>/.pigment-lock`, the steady-state lock that every view of
    /// the directory shares. Absent while directory-level maintenance is being recovered, and
    /// after a closed compaction has retired it.
    inner: Option<LockFile>,
    /// This process's hold on `<parent>/.<name>.pigment-lock`, which guards the phases in which
    /// the directory itself is replaced.
    _replacement: Option<LockFile>,
}

/// The name of the inner lock file, inside the store directory.
pub(crate) const INNER_LOCK_NAME: &str = ".pigment-lock";

/// Whether an open takes this process's locks on its directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessLockPolicy {
    /// An ordinary open: the directory is locked against every other process.
    Take,
    /// The reopen that validates a closed-compaction staging directory, which the compactor's
    /// claim on the store directory already covers. A lock here would leave a lock file inside a
    /// directory whose contents publication compares exactly.
    CoveredByClosedClaim,
}

/// A lock file this process holds.
struct LockFile {
    file: std::fs::File,
}

impl Drop for LockFile {
    /// Unlocks before the file closes. A child process spawned by any thread keeps a copy of the
    /// descriptor until it execs, and the lock belongs to the open file description, so closing
    /// alone would leave it held for as long as the child takes to exec: measured, 312 of 400
    /// immediate reopens were refused.
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl LockFile {
    /// Takes the lock at `path` for `identity`, creating the file if absent, or refuses.
    fn acquire(path: &Path, identity: &Path) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("cannot open lock file {}: {error}", path.display()),
                )
            })?;
        Self::lock(file, path, identity)
    }

    /// Takes the lock at `path` only if the file already exists and can be opened; refuses only
    /// when another process holds it. Nothing is created.
    fn check_existing(path: &Path, identity: &Path) -> io::Result<Option<Self>> {
        let Ok(file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
        else {
            return Ok(None);
        };
        match Self::lock(file, path, identity) {
            Ok(lock) => Ok(Some(lock)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(error),
            Err(_) => Ok(None),
        }
    }

    fn lock(file: std::fs::File, path: &Path, identity: &Path) -> io::Result<Self> {
        match file.try_lock() {
            Ok(()) => {
                record_owner(&file);
                Ok(Self { file })
            }
            Err(std::fs::TryLockError::WouldBlock) => Err(held_refusal(&file, path, identity)),
            Err(std::fs::TryLockError::Error(error)) => Err(io::Error::new(
                error.kind(),
                format!("cannot lock {}: {error}", path.display()),
            )),
        }
    }
}

fn held_refusal(file: &std::fs::File, path: &Path, identity: &Path) -> io::Error {
    // "Last recorded": a holder that could lock only read-only cannot replace an earlier record,
    // and process ids are relative to a pid namespace.
    let owner = recorded_owner(file)
        .map(|process| format!("; its last recorded owner is process {process}"))
        .unwrap_or_default();
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!(
            "store directory {} is already open in another process (lock file {} is held{owner})",
            identity.display(),
            path.display()
        ),
    )
}

/// The largest owner record a refusal reads: a process id and a newline fit many times over.
const OWNER_RECORD_LIMIT: u64 = 64;

/// Replaces the lock file's contents with this process's id, for the refusal another process
/// reports. Diagnostics only, so a failure to write it is ignored; nothing reads it to decide
/// ownership, and it is written only while the lock is held.
fn record_owner(file: &std::fs::File) {
    use std::io::{Seek, SeekFrom};
    let mut file = file;
    let _ = file
        .set_len(0)
        .and_then(|()| file.seek(SeekFrom::Start(0)))
        .and_then(|_| file.write_all(format!("{}\n", std::process::id()).as_bytes()));
}

/// The process id the lock's last owner recorded, when it can be read and parsed. On Windows a
/// holder's lock also forbids reading, so no process is named there.
fn recorded_owner(file: &std::fs::File) -> Option<u32> {
    use std::io::Read;
    let mut record = Vec::new();
    file.take(OWNER_RECORD_LIMIT)
        .read_to_end(&mut record)
        .ok()?;
    std::str::from_utf8(&record).ok()?.trim().parse().ok()
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
    match std::fs::metadata(store_dir) {
        Ok(metadata) if !metadata.is_dir() => {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "store directory path is not a directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
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

/// `<parent>/.<directory name>.pigment-lock`: beside the store directory, keyed by the parent's
/// entry for it, so it stays the same lock while closed compaction replaces the directory itself.
fn replacement_lock_path(identity: &Path) -> io::Result<PathBuf> {
    match (identity.parent(), identity.file_name()) {
        (Some(parent), Some(name)) => {
            let mut lock_name = OsString::from(".");
            lock_name.push(name);
            lock_name.push(".pigment-lock");
            Ok(parent.join(lock_name))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "store directory has no parent directory to hold its replacement lock",
        )),
    }
}

/// Whether directory-level maintenance -- a closed compaction, or one that was interrupted -- has
/// left artifacts for this directory: a staging or previous directory, or a manifest.
fn directory_maintenance_in_progress(identity: &Path) -> io::Result<bool> {
    let paths = crate::compaction::publication::directory_artifact_paths(identity)?;
    for path in [
        &paths.staging,
        &paths.previous,
        &paths.manifest,
        &paths.manifest_next,
    ] {
        match std::fs::symlink_metadata(path) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

/// The locks a new registry entry takes for an open.
///
/// - While directory-level maintenance is in progress, only the replacement lock: recovery may
///   replace the directory, so its inner lock is taken afterwards (`ensure_inner_lock`).
/// - For a directory that does not exist, none: the open fails as it always has, creating nothing.
/// - Otherwise the inner lock, plus the replacement lock if that file exists, so an open cannot
///   slip in while another process still holds it after a replacement.
fn open_locks(identity: &Path) -> io::Result<(Option<LockFile>, Option<LockFile>)> {
    if directory_maintenance_in_progress(identity)? {
        let replacement = LockFile::acquire(&replacement_lock_path(identity)?, identity)?;
        return Ok((None, Some(replacement)));
    }
    if !identity.is_dir() {
        return Ok((None, None));
    }
    let inner = LockFile::acquire(&identity.join(INNER_LOCK_NAME), identity)?;
    let replacement = LockFile::check_existing(&replacement_lock_path(identity)?, identity)?;
    Ok((Some(inner), replacement))
}

/// The locks a closed-maintenance claim takes: the inner lock unless maintenance must first be
/// recovered, then the replacement lock. The inner lock comes first, as it does for an open, so a
/// claim refused by an open owner creates nothing beside the directory.
fn claim_locks(identity: &Path) -> io::Result<(Option<LockFile>, Option<LockFile>)> {
    let inner = if !directory_maintenance_in_progress(identity)? && identity.is_dir() {
        Some(LockFile::acquire(
            &identity.join(INNER_LOCK_NAME),
            identity,
        )?)
    } else {
        None
    };
    let replacement = LockFile::acquire(&replacement_lock_path(identity)?, identity)?;
    Ok((inner, Some(replacement)))
}

/// Takes the inner lock of the directory now at `identity`, if this entry takes locks and does
/// not hold it yet: after a recovery that may have replaced the directory.
fn ensure_inner_lock(identity: &Path) -> io::Result<()> {
    let mut registry = lock_registry();
    let Some(state) = registry.get_mut(identity) else {
        return Ok(());
    };
    if !state.takes_locks || state.inner.is_some() || !identity.is_dir() {
        return Ok(());
    }
    state.inner = Some(LockFile::acquire(
        &identity.join(INNER_LOCK_NAME),
        identity,
    )?);
    Ok(())
}

#[derive(Debug)]
pub(crate) struct OpenDirectoryLease {
    identity: PathBuf,
}

impl OpenDirectoryLease {
    /// Takes the directory's inner lock once maintenance recovery has settled which directory is
    /// in place (specs/011 FR-5). A no-op when it is already held.
    pub(crate) fn ensure_inner_lock(&self) -> io::Result<()> {
        ensure_inner_lock(&self.identity)
    }
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

impl ClosedDirectoryClaim {
    /// Takes the directory's inner lock once the claim has recovered earlier maintenance.
    pub(crate) fn ensure_inner_lock(&self) -> io::Result<()> {
        ensure_inner_lock(&self.identity)
    }

    /// Releases and deletes the inner lock file before the directory's contents are revalidated
    /// and published, so the replaced directory holds exactly the store files that were
    /// captured. The replacement lock still excludes every other process until the claim ends.
    pub(crate) fn retire_inner_lock(&self) -> io::Result<()> {
        let retired = {
            let mut registry = lock_registry();
            registry
                .get_mut(&self.identity)
                .and_then(|state| state.inner.take())
        };
        if retired.is_some() {
            drop(retired);
            match std::fs::remove_file(self.identity.join(INNER_LOCK_NAME)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
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

#[cfg(test)]
pub(crate) fn acquire_open_lease(store_dir: &Path) -> io::Result<OpenDirectoryLease> {
    acquire_open_lease_with(store_dir, ProcessLockPolicy::Take)
}

pub(crate) fn acquire_open_lease_with(
    store_dir: &Path,
    policy: ProcessLockPolicy,
) -> io::Result<OpenDirectoryLease> {
    let identity = canonical_directory_identity(store_dir)?;
    let mut registry = lock_registry();
    if !registry.contains_key(&identity) {
        let takes_locks = policy == ProcessLockPolicy::Take;
        let (inner, replacement) = if takes_locks {
            open_locks(&identity)?
        } else {
            (None, None)
        };
        registry.insert(
            identity.clone(),
            OwnershipState {
                open_leases: 0,
                closed_claimed: false,
                takes_locks,
                inner,
                _replacement: replacement,
            },
        );
    }
    // A new entry has neither a claim nor a lease, so no refusal below can leave one behind.
    let state = registry.get_mut(&identity).expect("entry present");
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
    if let Some(state) = registry.get(&identity) {
        if state.closed_claimed || state.open_leases != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "an open store or closed maintenance operation already owns this directory",
            ));
        }
    }
    if !registry.contains_key(&identity) {
        let (inner, replacement) = claim_locks(&identity)?;
        registry.insert(
            identity.clone(),
            OwnershipState {
                open_leases: 0,
                closed_claimed: false,
                takes_locks: true,
                inner,
                _replacement: replacement,
            },
        );
    }
    let state = registry.get_mut(&identity).expect("entry present");
    state.closed_claimed = true;
    Ok(ClosedDirectoryClaim { identity })
}
