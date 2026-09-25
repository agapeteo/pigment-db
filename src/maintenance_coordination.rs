//! Ownership of file-backed store directories: within a process through a registry of open leases
//! and closed-maintenance claims, and across processes through lock files (specs/011).

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

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

/// A directory's registry slot. `Pending` while the thread creating the entry opens its lock
/// files outside the registry mutex; another thread opening the same directory waits for it.
enum Slot {
    Pending,
    Owned(OwnershipState),
}

struct OwnershipState {
    open_leases: usize,
    closed_claimed: bool,
    /// Whether this entry's owners take lock files at all: false only for the staging reopen that
    /// a closed claim covers.
    takes_locks: bool,
    /// This process's hold on `<store>/.pigment-lock`, the steady-state lock that every view of
    /// the directory shares.
    inner: InnerLock,
    /// This process's hold on `<parent>/.<name>.pigment-lock`, which guards the phases in which
    /// the directory itself is replaced.
    replacement: Option<LockFile>,
}

impl OwnershipState {
    fn new(takes_locks: bool, inner: Option<LockFile>, replacement: Option<LockFile>) -> Self {
        Self {
            open_leases: 0,
            closed_claimed: false,
            takes_locks,
            inner: inner.map_or(InnerLock::Absent, InnerLock::Held),
            replacement,
        }
    }

    /// Unlocks every lock this entry holds; the files close when the entry is dropped.
    fn unlock_all(&self) {
        if let InnerLock::Held(lock) = &self.inner {
            lock.unlock();
        }
        if let Some(lock) = &self.replacement {
            lock.unlock();
        }
    }
}

enum InnerLock {
    Held(LockFile),
    /// Not held: while directory-level maintenance is being recovered, after a closed compaction
    /// retired it, or for an entry that takes no locks.
    Absent,
    /// One thread is taking it after a recovery. Every other open of the directory waits for the
    /// outcome before it goes live, and takes the lock itself if that attempt failed.
    Acquiring,
}

/// The name of the inner lock file, inside the store directory.
pub(crate) const INNER_LOCK_NAME: &str = ".pigment-lock";

/// Whether a directory entry is a generation's inner lock file. The file is ownership state, not
/// a store artifact, so every inventory of a store generation skips it (FR-9). An open refused
/// part-way through another process's closed compaction can leave one in the source after the
/// claim retired its own, and an open that recovered can hold one while cleanup is still pending.
/// A cleanup that deletes a replaced generation deletes it too.
pub(crate) fn is_inner_lock_file(name: &std::ffi::OsStr, file_type: std::fs::FileType) -> bool {
    name == INNER_LOCK_NAME && file_type.is_file()
}

/// Deletes the inner lock file a replaced generation still holds, before its directory goes. It
/// may be gone already, and nothing else in the generation is touched.
pub(crate) fn remove_retired_inner_lock(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

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
    /// False only where the platform or filesystem cannot lock, and the lock was skipped (FR-11).
    locked: bool,
    /// Which file this is, where the platform says (device and inode on Unix), so an owner can
    /// tell whether the file it holds is still the one at the lock path.
    id: Option<FileId>,
}

/// A file's identity: device and inode. std exposes no stable equivalent on Windows.
type FileId = (u64, u64);

fn file_id(file: &std::fs::File) -> Option<FileId> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        file.metadata()
            .ok()
            .map(|metadata| (metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        None
    }
}

/// Whether the file identified by `held` is still the one at `path`. A file that cannot be
/// identified is taken to be; a path with no file, or another file, is not.
fn still_at(held: Option<FileId>, path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (held, std::fs::symlink_metadata(path)) {
            (None, _) => true,
            (Some(id), Ok(metadata)) => (metadata.dev(), metadata.ino()) == id,
            (Some(_), Err(_)) => false,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (held, path);
        true
    }
}

impl LockFile {
    /// Releases the lock without closing the file. A child process spawned by any thread keeps a
    /// copy of the descriptor until it execs, and the lock belongs to the open file description,
    /// so closing alone would leave it held for as long as the child takes to exec: measured, 312
    /// of 400 immediate reopens were refused.
    fn unlock(&self) {
        if self.locked {
            let _ = self.file.unlock();
        }
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        self.unlock();
    }
}

impl LockFile {
    /// Takes the lock at `path` for `identity`, creating the file if absent, or refuses.
    fn acquire(path: &Path, identity: &Path) -> io::Result<Self> {
        let (file, writable) = open_lock_file(path, true)?;
        Self::lock(file, writable, path, identity)
    }

    /// Takes the lock at `path` only if the file already exists and can be opened; refuses only
    /// when another process holds it. Nothing is created.
    fn check_existing(path: &Path, identity: &Path) -> io::Result<Option<Self>> {
        let Ok((file, writable)) = open_lock_file(path, false) else {
            return Ok(None);
        };
        match Self::lock(file, writable, path, identity) {
            Ok(lock) => Ok(Some(lock)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(error),
            Err(_) => Ok(None),
        }
    }

    fn lock(file: std::fs::File, writable: bool, path: &Path, identity: &Path) -> io::Result<Self> {
        #[cfg(test)]
        let attempt = match lock_seams::injected_lock_error(path) {
            Some(kind) => Err(std::fs::TryLockError::Error(io::Error::from(kind))),
            None => file.try_lock(),
        };
        #[cfg(not(test))]
        let attempt = file.try_lock();
        match attempt {
            Ok(()) => {
                if writable {
                    record_owner(&file);
                }
                let id = file_id(&file);
                Ok(Self {
                    file,
                    locked: true,
                    id,
                })
            }
            Err(std::fs::TryLockError::WouldBlock) => Err(held_refusal(&file, path, identity)),
            // Only what std reports as Unsupported: a target whose std takes no lock, ENOSYS or
            // EOPNOTSUPP on Unix, ERROR_CALL_NOT_IMPLEMENTED on Windows. The directory then keeps
            // the previous, process-local ownership rather than becoming impossible to open
            // (FR-11). Any other error refuses, including the codes other filesystems use to
            // decline a lock (ENOTSUP on Apple platforms, ENOLCK, ERROR_NOT_SUPPORTED).
            Err(std::fs::TryLockError::Error(error))
                if error.kind() == io::ErrorKind::Unsupported =>
            {
                log::warn!(
                    "pigment-db cannot lock {} ({error}); another process opening {} is not \
                     excluded",
                    path.display(),
                    identity.display()
                );
                Ok(Self {
                    file,
                    locked: false,
                    id: None,
                })
            }
            Err(std::fs::TryLockError::Error(error)) => Err(io::Error::new(
                error.kind(),
                format!("cannot lock {}: {error}", path.display()),
            )),
        }
    }
}

/// Opens a lock file for writing, creating it when `create` asks. A lock file this process may
/// only read -- created in advance for a directory it cannot write, left by another user, or on a
/// read-only mount -- is opened read-only instead: an exclusive lock still holds through it, and
/// only the owner record is lost. Reports whether the descriptor is writable.
///
/// A path that exists but is not a regular file -- a symlink, a directory, a FIFO -- is refused
/// before anything opens it: following a symlink would lock its target in the lock file's place
/// and overwrite it with the owner record. The check precedes the open, so a path replaced in
/// between is not caught; the store directory is the owner's own, and the replacement lock's
/// parent is the directory the store lives in.
fn open_lock_file(path: &Path, create: bool) -> io::Result<(std::fs::File, bool)> {
    let annotate = |error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("cannot open lock file {}: {error}", path.display()),
        )
    };
    #[cfg(test)]
    lock_seams::before_lock_file_open(path);
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("lock file {} is not a regular file", path.display()),
            ));
        }
        _ => {}
    }
    #[cfg(test)]
    let read_write = match lock_seams::injected_open_error(path) {
        Some(kind) => Err(io::Error::from(kind)),
        None => std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(path),
    };
    #[cfg(not(test))]
    let read_write = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .open(path);
    match read_write {
        Ok(file) => Ok((file, true)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::ReadOnlyFilesystem
            ) =>
        {
            std::fs::File::open(path)
                .map(|file| (file, false))
                .map_err(|_| annotate(error))
        }
        Err(error) => Err(annotate(error)),
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

fn registry() -> &'static Mutex<HashMap<PathBuf, Slot>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Slot>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_registry() -> MutexGuard<'static, HashMap<PathBuf, Slot>> {
    registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// How many symbolic links an identity follows by hand, as the kernel's own limit does.
const MAX_SYMLINK_HOPS: usize = 40;

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
    // A symlink whose target does not exist still names that target: most often a store
    // directory that another process's closed compaction has moved aside. Keyed by the link's own
    // name, the open would check another directory's lock files and maintenance artifacts, and
    // miss the claim that is replacing this one (FR-2).
    let mut hops = 0;
    while std::fs::symlink_metadata(&cursor).is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        hops += 1;
        if hops > MAX_SYMLINK_HOPS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "store directory {} is a chain of more than {MAX_SYMLINK_HOPS} symbolic links",
                    store_dir.display()
                ),
            ));
        }
        let target = std::fs::read_link(&cursor)?;
        cursor = match cursor.parent() {
            Some(parent) if target.is_relative() => parent.join(target),
            _ => target,
        };
        if let Ok(canonical) = std::fs::canonicalize(&cursor) {
            return Ok(canonical);
        }
    }
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

/// How many times an open looks at a directory that another process keeps replacing before it
/// gives up.
const OPEN_STATE_ATTEMPTS: usize = 3;

/// The locks a new registry entry takes for an open. Every entry that takes locks holds at least
/// one, because an open holding none could neither recover maintenance nor go live safely.
///
/// - While directory-level maintenance is in progress, only the replacement lock: recovery may
///   replace the directory, so its inner lock is taken afterwards (`ensure_inner_lock`).
/// - For a directory that exists, the inner lock, plus the replacement lock if that file exists,
///   so an open cannot slip in while another process still holds it after a replacement.
/// - For a directory that does not exist, none, and the open fails at once, creating nothing
///   (FR-8). Another process's closed compaction moves the directory aside while its claim holds
///   the replacement lock, and it may do so after the first check here, so a missing directory is
///   taken as missing only once maintenance has been asked about again.
fn open_locks(identity: &Path) -> io::Result<(Option<LockFile>, Option<LockFile>)> {
    for _ in 0..OPEN_STATE_ATTEMPTS {
        if directory_maintenance_in_progress(identity)? {
            let replacement = LockFile::acquire(&replacement_lock_path(identity)?, identity)?;
            return Ok((None, Some(replacement)));
        }
        #[cfg(test)]
        lock_seams::after_maintenance_check(identity);
        if identity.is_dir() {
            let inner = LockFile::acquire(&identity.join(INNER_LOCK_NAME), identity)?;
            let replacement =
                LockFile::check_existing(&replacement_lock_path(identity)?, identity)?;
            return Ok((Some(inner), replacement));
        }
        if !directory_maintenance_in_progress(identity)? && !identity.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("store directory {} does not exist", identity.display()),
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        format!(
            "store directory {} kept being replaced while it was opened; another process is \
             compacting it",
            identity.display()
        ),
    ))
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

/// How often a thread waiting on another thread's `Pending` entry for the same directory looks
/// again. Only opens of that one directory wait, and only while its lock files are opened.
const PENDING_POLL: Duration = Duration::from_millis(1);

/// Runs `admit` on the directory's entry, first creating the entry when there is none.
///
/// Creating it takes the directory's lock files, and that I/O runs with the registry mutex
/// released: the entry is `Pending` meanwhile, so a stalled lock file holds up opens of this
/// directory only (specs/011 FR-13). A failure to create inserts nothing, and neither does a
/// panic while creating, which would otherwise leave every later open of the directory waiting.
/// `admit` cannot refuse a new entry, which has neither a lease nor a claim, so nothing is left
/// behind by it either.
fn with_entry<T>(
    identity: &Path,
    create: impl FnOnce(&Path) -> io::Result<OwnershipState>,
    admit: impl FnOnce(&mut OwnershipState) -> io::Result<T>,
) -> io::Result<T> {
    let mut admit = Some(admit);
    loop {
        let mut registry = lock_registry();
        match registry.get_mut(identity) {
            Some(Slot::Owned(state)) => return (admit.take().expect("admitted once"))(state),
            Some(Slot::Pending) => {
                drop(registry);
                std::thread::sleep(PENDING_POLL);
            }
            None => {
                registry.insert(identity.to_path_buf(), Slot::Pending);
                break;
            }
        }
    }
    let mut pending = PendingEntry {
        identity,
        armed: true,
    };
    let created = create(identity);
    pending.armed = false;
    let mut registry = lock_registry();
    match created {
        Ok(state) => {
            let slot = registry
                .entry(identity.to_path_buf())
                .or_insert(Slot::Pending);
            *slot = Slot::Owned(state);
            let Slot::Owned(state) = slot else {
                unreachable!("the slot was just filled");
            };
            (admit.take().expect("admitted once"))(state)
        }
        Err(error) => {
            registry.remove(identity);
            Err(error)
        }
    }
}

/// Returns a directory's inner lock from `Acquiring` to `Absent` when the thread taking it unwinds,
/// so the opens waiting on that attempt take the lock themselves.
struct AcquiringInnerLock<'a> {
    identity: &'a Path,
    armed: bool,
}

impl Drop for AcquiringInnerLock<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut registry = lock_registry();
            if let Some(Slot::Owned(state)) = registry.get_mut(self.identity) {
                if matches!(state.inner, InnerLock::Acquiring) {
                    state.inner = InnerLock::Absent;
                }
            }
        }
    }
}

/// Removes a directory's `Pending` entry when the thread creating it unwinds instead of returning.
struct PendingEntry<'a> {
    identity: &'a Path,
    armed: bool,
}

impl Drop for PendingEntry<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut registry = lock_registry();
            if matches!(registry.get(self.identity), Some(Slot::Pending)) {
                registry.remove(self.identity);
            }
        }
    }
}

/// Applies `update` to the directory's entry and removes the entry when it reports no owner is
/// left. The removed entry's locks are unlocked under the mutex, so this process's next open of
/// the directory cannot find its own stale lock; its files close after the mutex is released.
fn release(identity: &Path, update: impl FnOnce(&mut OwnershipState) -> bool) {
    let removed = {
        let mut registry = lock_registry();
        let remove = match registry.get_mut(identity) {
            Some(Slot::Owned(state)) => update(state),
            _ => false,
        };
        if remove {
            let removed = registry.remove(identity);
            if let Some(Slot::Owned(state)) = &removed {
                state.unlock_all();
            }
            removed
        } else {
            None
        }
    };
    drop(removed);
}

/// Makes this entry hold the inner lock of the directory now at `identity`, before an open or a
/// claim goes on: after a recovery that may have replaced the directory, and after an open whose
/// lock file was retired with the directory while it was stalled (FR-2, FR-5).
///
/// - Not held: it is taken, with the registry mutex released.
/// - Being taken by another thread: this one waits for the outcome, and takes the lock itself if
///   that attempt failed. A guard clears the marker if the attempt unwinds.
/// - Held: kept only while it is still the file at the lock path, which is checked outside the
///   mutex. Otherwise it is released and the current file's lock taken. Where the platform gives
///   no file identity (Windows), a held lock is kept.
fn ensure_inner_lock(identity: &Path) -> io::Result<()> {
    let lock_path = identity.join(INNER_LOCK_NAME);
    loop {
        let mut registry = lock_registry();
        let Some(Slot::Owned(state)) = registry.get_mut(identity) else {
            return Ok(());
        };
        if !state.takes_locks {
            return Ok(());
        }
        let held = match &state.inner {
            InnerLock::Held(lock) => lock.id,
            // Another thread's attempt decides whether this open may go live: wait for it.
            InnerLock::Acquiring => {
                drop(registry);
                std::thread::sleep(PENDING_POLL);
                continue;
            }
            InnerLock::Absent => {
                state.inner = InnerLock::Acquiring;
                break;
            }
        };
        drop(registry);
        if still_at(held, &lock_path) {
            return Ok(());
        }
        let mut registry = lock_registry();
        let Some(Slot::Owned(state)) = registry.get_mut(identity) else {
            return Ok(());
        };
        if matches!(&state.inner, InnerLock::Held(lock) if lock.id == held) {
            let stale = std::mem::replace(&mut state.inner, InnerLock::Acquiring);
            drop(registry);
            drop(stale);
            break;
        }
    }
    let mut acquiring = AcquiringInnerLock {
        identity,
        armed: true,
    };
    let acquired = if identity.is_dir() {
        LockFile::acquire(&identity.join(INNER_LOCK_NAME), identity).map(Some)
    } else {
        Ok(None)
    };
    acquiring.armed = false;
    let mut registry = lock_registry();
    let Some(Slot::Owned(state)) = registry.get_mut(identity) else {
        return acquired.map(|_| ());
    };
    match acquired {
        Ok(Some(lock)) => {
            state.inner = InnerLock::Held(lock);
            Ok(())
        }
        Ok(None) => {
            state.inner = InnerLock::Absent;
            Ok(())
        }
        Err(error) => {
            state.inner = InnerLock::Absent;
            Err(error)
        }
    }
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

    /// The path recovery reads for `store_dir`. A symlink's own name locates no maintenance
    /// artifacts of the directory it names, so for a symlink it is the directory's identity.
    /// Otherwise it is `store_dir` itself, so that errors keep naming the path the caller gave.
    pub(crate) fn maintenance_path(&self, store_dir: &Path) -> PathBuf {
        match std::fs::symlink_metadata(store_dir) {
            Ok(metadata) if metadata.file_type().is_symlink() => self.identity.clone(),
            _ => store_dir.to_path_buf(),
        }
    }
}

impl Drop for OpenDirectoryLease {
    fn drop(&mut self) {
        release(&self.identity, |state| {
            state.open_leases = state.open_leases.saturating_sub(1);
            state.open_leases == 0 && !state.closed_claimed
        });
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
            match registry.get_mut(&self.identity) {
                Some(Slot::Owned(state)) => {
                    match std::mem::replace(&mut state.inner, InnerLock::Absent) {
                        InnerLock::Held(lock) => {
                            lock.unlock();
                            Some(lock)
                        }
                        other => {
                            state.inner = other;
                            None
                        }
                    }
                }
                _ => None,
            }
        };
        if let Some(lock) = retired {
            drop(lock);
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
        release(&self.identity, |state| {
            state.closed_claimed = false;
            state.open_leases == 0
        });
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
    let takes_locks = policy == ProcessLockPolicy::Take;
    with_entry(
        &identity,
        |identity| {
            let (inner, replacement) = if takes_locks {
                open_locks(identity)?
            } else {
                (None, None)
            };
            Ok(OwnershipState::new(takes_locks, inner, replacement))
        },
        |state| {
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
            Ok(())
        },
    )?;
    Ok(OpenDirectoryLease { identity })
}

#[allow(dead_code)]
pub(crate) fn try_claim_closed(store_dir: &Path) -> io::Result<ClosedDirectoryClaim> {
    let identity = canonical_directory_identity(store_dir)?;
    with_entry(
        &identity,
        |identity| {
            let (inner, replacement) = claim_locks(identity)?;
            Ok(OwnershipState::new(true, inner, replacement))
        },
        |state| {
            if state.closed_claimed || state.open_leases != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "an open store or closed maintenance operation already owns this directory",
                ));
            }
            state.closed_claimed = true;
            Ok(())
        },
    )?;
    Ok(ClosedDirectoryClaim { identity })
}

#[cfg(test)]
pub(crate) mod lock_seams {
    //! Private seams for lock-file I/O (specs/011), each keyed by the directory a lock file lives
    //! in, so tests running in parallel do not see each other's. A directory is keyed by the
    //! identity an open computes for it, so it may be one that does not exist yet.

    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex};

    type Gate = Arc<(Mutex<(bool, bool)>, Condvar)>;

    /// Where an open stalls.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum StallPoint {
        /// At the top of every lock-file open whose file lives in the directory.
        LockFileOpen,
        /// In an open of the directory, just after it first looked for maintenance artifacts.
        AfterMaintenanceCheck,
    }

    /// Stalls, each with its gate: `(entered, released)`.
    static STALLS: Mutex<Vec<(StallPoint, PathBuf, Gate)>> = Mutex::new(Vec::new());
    /// How long a test waits for an open to reach its stall before failing.
    const STALL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
    /// Lock errors reported in place of a real attempt.
    static LOCK_ERRORS: Mutex<Vec<(PathBuf, io::ErrorKind)>> = Mutex::new(Vec::new());
    /// Errors reported in place of the read-write open.
    static OPEN_ERRORS: Mutex<Vec<(PathBuf, io::ErrorKind)>> = Mutex::new(Vec::new());
    /// Directories whose next lock-file open panics.
    static PANICS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    fn guard<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn in_directory(path: &Path, directory: &Path) -> bool {
        path.parent() == Some(directory)
    }

    pub(super) fn before_lock_file_open(path: &Path) {
        if let Some(directory) = path.parent() {
            stall_at(StallPoint::LockFileOpen, directory);
        }
        // After any stall, so a test can make an open panic that is already under way.
        let panics = {
            let mut panics = guard(&PANICS);
            let before = panics.len();
            panics.retain(|directory| !in_directory(path, directory));
            panics.len() != before
        };
        if panics {
            panic!("injected panic opening {}", path.display());
        }
    }

    pub(super) fn after_maintenance_check(identity: &Path) {
        stall_at(StallPoint::AfterMaintenanceCheck, identity);
    }

    fn stall_at(point: StallPoint, directory: &Path) {
        let gate = guard(&STALLS)
            .iter()
            .find(|(at, stalled, _)| *at == point && stalled == directory)
            .map(|(_, _, gate)| gate.clone());
        let Some(gate) = gate else {
            return;
        };
        let (state, signal) = &*gate;
        let mut flags = state.lock().unwrap();
        flags.0 = true;
        signal.notify_all();
        while !flags.1 {
            flags = signal.wait(flags).unwrap();
        }
    }

    pub(super) fn injected_lock_error(path: &Path) -> Option<io::ErrorKind> {
        guard(&LOCK_ERRORS)
            .iter()
            .find(|(directory, _)| in_directory(path, directory))
            .map(|(_, kind)| *kind)
    }

    pub(super) fn injected_open_error(path: &Path) -> Option<io::ErrorKind> {
        guard(&OPEN_ERRORS)
            .iter()
            .find(|(directory, _)| in_directory(path, directory))
            .map(|(_, kind)| *kind)
    }

    pub(crate) fn inject_open_error(directory: &Path, kind: io::ErrorKind) {
        guard(&OPEN_ERRORS).push((
            super::canonical_directory_identity(directory).unwrap(),
            kind,
        ));
    }

    /// Makes the next lock-file open in `directory` panic, once.
    pub(crate) fn inject_panic(directory: &Path) {
        guard(&PANICS).push(super::canonical_directory_identity(directory).unwrap());
    }

    pub(crate) fn inject_lock_error(directory: &Path, kind: io::ErrorKind) {
        guard(&LOCK_ERRORS).push((
            super::canonical_directory_identity(directory).unwrap(),
            kind,
        ));
    }

    /// Stalls opens of one directory at one point until released or dropped.
    pub(crate) struct Stall {
        point: StallPoint,
        directory: PathBuf,
        gate: Gate,
    }

    impl Stall {
        /// Stalls every lock-file open in `directory`.
        pub(crate) fn install(directory: &Path) -> Self {
            Self::install_at(directory, StallPoint::LockFileOpen)
        }

        pub(crate) fn install_at(directory: &Path, point: StallPoint) -> Self {
            let directory = super::canonical_directory_identity(directory).unwrap();
            let gate: Gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
            guard(&STALLS).push((point, directory.clone(), gate.clone()));
            Self {
                point,
                directory,
                gate,
            }
        }

        /// Waits until some open has reached the stall, and fails the test if none does within
        /// the deadline rather than hanging it.
        pub(crate) fn wait_entered(&self) {
            let (state, signal) = &*self.gate;
            let started = std::time::Instant::now();
            let mut flags = state.lock().unwrap();
            while !flags.0 {
                let left = STALL_DEADLINE.saturating_sub(started.elapsed());
                if left.is_zero() {
                    drop(flags);
                    self.release();
                    panic!(
                        "no open reached the {:?} stall for {} within {STALL_DEADLINE:?}",
                        self.point,
                        self.directory.display()
                    );
                }
                flags = signal.wait_timeout(flags, left).unwrap().0;
            }
        }

        pub(crate) fn release(&self) {
            let (state, signal) = &*self.gate;
            state.lock().unwrap().1 = true;
            signal.notify_all();
            guard(&STALLS).retain(|(point, directory, _)| {
                !(*point == self.point && directory == &self.directory)
            });
        }
    }

    impl Drop for Stall {
        fn drop(&mut self) {
            self.release();
        }
    }
}

#[cfg(test)]
mod progress_tests {
    //! FR-13 (specs/011): lock-file I/O for one directory must not hold up other directories.

    use super::lock_seams::Stall;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn a_stalled_lock_file_holds_up_no_other_directory() {
        let stalled = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let dropped = tempfile::tempdir().unwrap();
        let held = crate::key_value_store::DurableKeyValueStore::try_init_new(dropped.path())
            .unwrap()
            .into_store();
        let stall = Stall::install(stalled.path());

        let stalled_path = stalled.path().to_path_buf();
        let stalled_open = std::thread::spawn(move || {
            crate::key_value_store::DurableKeyValueStore::try_init_new(&stalled_path).is_ok()
        });
        stall.wait_entered();

        let (done, finished) = mpsc::channel();
        let other_path = other.path().to_path_buf();
        std::thread::spawn(move || {
            let opened =
                crate::key_value_store::DurableKeyValueStore::try_init_new(&other_path).is_ok();
            drop(held);
            let _ = done.send(opened);
        });
        let progressed = finished.recv_timeout(Duration::from_secs(5));

        stall.release();
        let stalled_opened = stalled_open.join().unwrap();
        // Joined before asserting, so a failure leaves no thread parked on the gate.
        let _ = finished.recv_timeout(Duration::from_secs(30));

        assert_eq!(
            progressed,
            Ok(true),
            "another directory's open and a third's drop waited behind a stalled lock file"
        );
        assert!(stalled_opened);
    }
}

#[cfg(test)]
mod lock_error_tests {
    //! FR-11 (specs/011): a lock the platform or filesystem cannot take is skipped with a warning;
    //! any other lock error refuses the open.

    use super::lock_seams::{inject_lock_error, inject_open_error, inject_panic};
    use super::INNER_LOCK_NAME;
    use std::io;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn a_lock_the_platform_cannot_take_is_skipped_and_the_store_opens() {
        let directory = tempfile::tempdir().unwrap();
        inject_lock_error(directory.path(), io::ErrorKind::Unsupported);

        let opened = crate::key_value_store::DurableKeyValueStore::try_init_new(directory.path());

        assert!(opened.is_ok(), "{:?}", opened.err());
    }

    /// FR-11: a skipped lock is reported, naming the lock file and the directory left unexcluded.
    #[test]
    fn a_skipped_lock_is_reported_with_a_warning() {
        use crate::test_support::log_capture::{install, logged};

        install();
        let directory = tempfile::tempdir().unwrap();
        inject_lock_error(directory.path(), io::ErrorKind::Unsupported);
        let identity = super::canonical_directory_identity(directory.path()).unwrap();
        let lock = identity.join(INNER_LOCK_NAME).display().to_string();
        // The directory is asked for on its own, since the lock path already contains it.
        let unexcluded = format!("opening {} is not excluded", identity.display());
        let warned = |message: &str| message.contains(&lock) && message.contains(&unexcluded);

        // Another test may clear the shared buffer between an open and the look, so each attempt
        // opens again and so logs again.
        let found = (0..10).any(|_| {
            drop(crate::key_value_store::DurableKeyValueStore::try_init_new(
                directory.path(),
            ));
            logged(warned)
        });

        assert!(found, "no warning named {lock}");
    }

    #[test]
    fn any_other_lock_error_refuses_the_open() {
        let directory = tempfile::tempdir().unwrap();
        inject_lock_error(directory.path(), io::ErrorKind::Other);

        match crate::key_value_store::DurableKeyValueStore::try_init_new(directory.path()) {
            Err(crate::RecoveryError::Io { source, .. }) => {
                assert_eq!(source.kind(), io::ErrorKind::Other);
                assert!(source.to_string().contains(".pigment-lock"), "{source}");
            }
            other => panic!("expected an I/O refusal, got {:?}", other.map(|_| ())),
        }
    }

    /// FR-7: a lock file the filesystem will not open for writing, because it is mounted read-only,
    /// is opened read-only and locked, and nothing is recorded in it.
    #[test]
    fn a_read_only_filesystem_opens_the_lock_file_read_only_and_records_nothing() {
        let directory = tempfile::tempdir().unwrap();
        drop(crate::key_value_store::DurableKeyValueStore::try_init_new(directory.path()).unwrap());
        let lock = directory.path().join(INNER_LOCK_NAME);
        std::fs::write(&lock, b"stale\n").unwrap();
        inject_open_error(directory.path(), io::ErrorKind::ReadOnlyFilesystem);

        let opened = crate::key_value_store::DurableKeyValueStore::try_init_new(directory.path());

        assert!(opened.is_ok(), "{:?}", opened.err());
        let held = std::fs::File::open(&lock).unwrap().try_lock();
        assert!(
            matches!(held, Err(std::fs::TryLockError::WouldBlock)),
            "the read-only descriptor must hold the lock: {held:?}"
        );
        drop(opened);
        assert_eq!(std::fs::read(&lock).unwrap(), b"stale\n");
    }

    /// A panic while a directory's locks are being taken leaves nothing behind in the ownership
    /// registry: the next open of that directory in this process proceeds.
    #[test]
    fn a_panic_while_taking_locks_does_not_wedge_the_directory() {
        let directory = tempfile::tempdir().unwrap();
        inject_panic(directory.path());

        let panicked = std::panic::catch_unwind(|| {
            crate::key_value_store::DurableKeyValueStore::try_init_new(directory.path())
        });
        let (done, finished) = mpsc::channel();
        let path = directory.path().to_path_buf();
        std::thread::spawn(move || {
            let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&path);
            let _ = done.send(reopened.map(|_| ()).map_err(|error| error.to_string()));
        });
        let reopened = finished.recv_timeout(Duration::from_secs(5));

        assert!(panicked.is_err(), "the injected panic must have fired");
        assert_eq!(reopened, Ok(Ok(())), "the directory must open again");
    }
}
