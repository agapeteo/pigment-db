use dashmap::DashMap;
use log::info;

use std::io::Write;
use std::path::{Path, PathBuf};

use std::fs::File;

use crate::maintenance_coordination::{MaintenanceCoordinator, OpenDirectoryLease};
use crate::wal::format::V1CodecProbe;
use crate::wal::recovery::{
    encode_key_set_repair_snapshot, initialize_snapshot_with_policy, ArtifactPaths, StoreKind,
};
use crate::wal::replay::{
    encode_key_set_snapshot, key_set_is_proper_snapshot_prefix, replay_key_set,
    replay_key_set_against, replay_key_set_tail,
};
use crate::wal::{ComputeAction, WalStorage};
use crate::{DurableStoreOptions, RecoveryError, RecoveryOutcome, RecoveryStatus};
use dashmap::mapref::entry::Entry;
use std::collections::HashSet;

#[cfg(test)]
use crate::test_support::mutation_schedule::{MutationObserver, MutationPhase};

/// Mutations are ordered per logical outer key, while mutations of keys in different data-map shards remain concurrent except during shared WAL acceptance.
///
/// Different keys in the same DashMap shard may wait for one another during
/// synchronous mutations. Synchronous compute callbacks run while that shard
/// is guarded, so recursive access to the same map or shard is unsupported and
/// may deadlock. Asynchronous callbacks instead receive a private snapshot with
/// no DashMap guard held across `.await`; a changed same-key snapshot rejects
/// publication with [`std::io::ErrorKind::WouldBlock`]. A callback panic or
/// cancellation before acceptance discards its private candidate. These
/// guarantees do not change any public method signature or callback shape.
pub struct DurableKeySetStore<W: Write> {
    store: DashMap<Vec<u8>, HashSet<Vec<u8>>>,
    wal: WalStorage<W>,
    file_backing: Option<PathBuf>,
    _open_lease: Option<OpenDirectoryLease>,
    maintenance: MaintenanceCoordinator,
    #[cfg(test)]
    mutation_observer: MutationObserver,
}

impl DurableKeySetStore<File> {
    /// Compacts this open key/set store while reads and ordinary mutations continue.
    ///
    /// The operation is explicitly caller-triggered, inherits the store's opened
    /// durability policy, and bounds concurrent delta recording with `options`.
    /// Reads bypass maintenance coordination. Mutations pause only during
    /// snapshot capture and cutover; staging work runs outside the exclusive
    /// gate. Indeterminate publication keeps reads available but rejects later
    /// mutations until reopen resolves authority. A successful
    /// [`crate::CleanupStatus::Pending`] outcome is authoritative and cleanup is
    /// retried on reopen or another explicit compaction.
    pub fn try_compact_online(
        &self,
        options: crate::OnlineCompactionOptions,
    ) -> Result<crate::FamilyCompactionOutcome, crate::CompactionError> {
        let store_dir = self
            .file_backing
            .as_deref()
            .expect("file-backed store retains its directory identity");
        let capture = crate::compaction::begin_online_capture(
            &self.maintenance,
            &self.wal,
            store_dir,
            crate::compaction::inspection::InspectedFamily::KeySet,
            options.max_delta_bytes(),
            || {
                crate::compaction::CapturedLogicalState::Set(
                    self.store
                        .iter()
                        .map(|entry| (entry.key().clone(), entry.value().clone()))
                        .collect(),
                )
            },
            |_| {},
        )?;
        let staged = crate::compaction::prepare_online_staging(capture, |_| Ok(()))?;
        crate::compaction::complete_online_cutover(
            &self.maintenance,
            &self.wal,
            staged,
            || {
                crate::compaction::CapturedLogicalState::Set(
                    self.store
                        .iter()
                        .map(|entry| (entry.key().clone(), entry.value().clone()))
                        .collect(),
                )
            },
            |active_path| std::fs::OpenOptions::new().append(true).open(active_path),
            |_| Ok(()),
        )
        .map(crate::compaction::CompletedOnlineCutover::into_outcome)
    }

    #[cfg(test)]
    pub(crate) fn begin_online_capture_probe(
        &self,
        max_delta_bytes: u64,
        observer: crate::test_support::maintenance_schedule::MaintenanceObserver,
    ) -> Result<crate::compaction::PreparedOnlineCapture<'_, File>, crate::CompactionError> {
        let store_dir = self
            .file_backing
            .as_deref()
            .expect("online compaction requires file backing");
        crate::compaction::begin_online_capture(
            &self.maintenance,
            &self.wal,
            store_dir,
            crate::compaction::inspection::InspectedFamily::KeySet,
            max_delta_bytes,
            || {
                crate::compaction::CapturedLogicalState::Set(
                    self.store
                        .iter()
                        .map(|entry| (entry.key().clone(), entry.value().clone()))
                        .collect(),
                )
            },
            |stage| {
                let checkpoint = match stage {
                    crate::compaction::OnlineCaptureStage::SnapshotCaptured => {
                        crate::test_support::maintenance_schedule::MaintenanceCheckpoint::SnapshotCapture
                    }
                    crate::compaction::OnlineCaptureStage::RecorderActivated => {
                        crate::test_support::maintenance_schedule::MaintenanceCheckpoint::RecorderActivation
                    }
                    crate::compaction::OnlineCaptureStage::ManifestPrepared => {
                        crate::test_support::maintenance_schedule::MaintenanceCheckpoint::ManifestPrepared
                    }
                };
                observer.checkpoint(checkpoint);
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn apply_online_delta_probe<'a>(
        &'a self,
        mut staged: crate::compaction::ValidatedOnlineStaging<'a, File>,
    ) -> Result<crate::compaction::AppliedOnlineDelta<'a, File>, crate::CompactionError> {
        let applied = {
            let _exclusive = self.maintenance.exclusive();
            let delta = staged.prepared.attempt.detach_recorder().ok_or_else(|| {
                crate::CompactionError::FailedClosed {
                    detail: "online compaction lost its matching delta recorder".to_owned(),
                }
            })?;
            let applied =
                match crate::compaction::apply_online_delta_to_staging(&mut staged, &delta) {
                    Ok(applied) => applied,
                    Err(error @ crate::CompactionError::ConcurrentDeltaLimitExceeded { .. }) => {
                        crate::compaction::abandon_online_prepublication(&staged)?;
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
            let live_state = crate::compaction::CapturedLogicalState::Set(
                self.store
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect(),
            );
            let metadata = self.wal.online_capture_metadata().map_err(|source| {
                crate::CompactionError::Io {
                    operation: crate::CompactionOperation::ValidateStaging,
                    path: staged.prepared.paths.staging.clone(),
                    source,
                }
            })?;
            crate::compaction::validate_online_staging_against_live(&staged, live_state, metadata)?;
            Ok::<_, crate::CompactionError>(applied)
        }?;
        Ok(crate::compaction::AppliedOnlineDelta {
            staged,
            replayed: applied.replayed,
            encoded_bytes: applied.encoded_bytes,
            accepted_buckets: applied.accepted_buckets,
            group_frame_counts: applied.group_frame_counts,
        })
    }

    /// Returns exact storage usage for this open key/set generation.
    ///
    /// Vector-backed stores intentionally do not expose filesystem maintenance:
    ///
    /// ```compile_fail
    /// use pigment_db::key_set_store::DurableKeySetStore;
    /// let store = DurableKeySetStore::new_vec_based();
    /// let _ = store.storage_stats();
    /// ```
    pub fn storage_stats(&self) -> Result<crate::FamilyStorageStats, crate::CompactionError> {
        crate::maintenance::public_file_family_storage_stats(
            self.file_backing
                .as_deref()
                .expect("file-backed store retains its directory identity"),
            crate::compaction::inspection::InspectedFamily::KeySet,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn storage_stats_internal(
        &self,
    ) -> std::io::Result<crate::compaction::inspection::FamilyInspection> {
        crate::maintenance::file_family_storage_stats(
            self.file_backing
                .as_deref()
                .expect("file-backed store retains its directory identity"),
            crate::compaction::inspection::InspectedFamily::KeySet,
        )
    }

    #[cfg(test)]
    pub(crate) fn storage_stats_probe(
        &self,
    ) -> std::io::Result<crate::compaction::inspection::FamilyInspection> {
        self.storage_stats_internal()
    }

    /// Opens a file-backed key/set store and returns structured recovery status
    /// or error information without panicking for expected startup failures.
    pub fn try_init_new(
        store_dir: impl AsRef<Path>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError> {
        Self::try_init_new_configured(store_dir, None)
    }

    /// Opens a file-backed key/set store with explicit timestamp and durability options.
    ///
    /// A missing store is published as a complete V2 active segment. An
    /// explicit granularity change rotates before the next accepted mutation;
    /// unrelated options preserve the active segment's persisted granularity.
    /// Complete legacy and V1 input require the standalone migration command.
    /// Physical mode preflights and durably publishes filesystem authority.
    pub fn try_init_new_with_options(
        store_dir: impl AsRef<Path>,
        options: DurableStoreOptions,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError> {
        Self::try_init_new_configured(store_dir, Some(options))
    }

    fn try_init_new_configured(
        store_dir: impl AsRef<Path>,
        options: Option<DurableStoreOptions>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError> {
        let store_dir = store_dir.as_ref();
        let open_lease =
            crate::maintenance_coordination::acquire_open_lease(store_dir).map_err(|source| {
                RecoveryError::Io {
                    operation: crate::RecoveryOperation::Inspect,
                    path: store_dir.to_path_buf(),
                    source,
                }
            })?;
        let maintenance_recovered = crate::compaction::recovery::resolve_store_maintenance(
            store_dir,
            crate::compaction::inspection::InspectedFamily::KeySet,
        )?;
        let paths = ArtifactPaths::new(store_dir, StoreKind::Set);
        let durability_policy = options
            .map(DurableStoreOptions::durability_policy)
            .unwrap_or_default();
        let wal_segment_size = options.unwrap_or_default().wal_segment_size().as_bytes();
        let initialized = initialize_snapshot_with_policy(
            &paths,
            replay_key_set,
            replay_key_set_tail,
            replay_key_set_against,
            encode_key_set_snapshot,
            encode_key_set_repair_snapshot,
            key_set_is_proper_snapshot_prefix,
            Some(V1CodecProbe::encode_header_with_kind(2)),
            options.and_then(DurableStoreOptions::requested_granularity_nanos),
            durability_policy,
        )?;
        initialized
            .wal
            .enable_file_rotation(
                paths.active.clone(),
                wal_segment_size,
                options.and_then(DurableStoreOptions::requested_granularity_nanos),
            )
            .map_err(|source| RecoveryError::Io {
                operation: crate::RecoveryOperation::Open,
                path: paths.active.clone(),
                source,
            })?;
        let store = DashMap::new();
        for (key, values) in initialized.snapshot {
            store.insert(key, values);
        }
        let file_backing =
            std::fs::canonicalize(store_dir).map_err(|source| RecoveryError::Io {
                operation: crate::RecoveryOperation::Open,
                path: paths.active.clone(),
                source,
            })?;
        let recovery_status = if maintenance_recovered {
            RecoveryStatus::Recovered
        } else {
            initialized.status
        };
        Ok(RecoveryOutcome::new(
            DurableKeySetStore {
                store,
                wal: initialized.wal,
                file_backing: Some(file_backing),
                _open_lease: Some(open_lease),
                maintenance: MaintenanceCoordinator::default(),
                #[cfg(test)]
                mutation_observer: MutationObserver::default(),
            },
            recovery_status,
        ))
    }

    /// Opens a file-backed key/set store with the historical panic-on-error API.
    ///
    /// This compatibility wrapper delegates to [`Self::try_init_new`] and logs
    /// successful automatic recovery.
    pub fn init_new(store_dir: &str) -> Self {
        let outcome = Self::try_init_new(store_dir).unwrap_or_else(|error| panic!("{error}"));
        let (store, status) = outcome.into_parts();
        if status == RecoveryStatus::Recovered {
            info!("pigment-db recovered key/set WAL in {store_dir}");
        }
        store
    }
}

impl DurableKeySetStore<Vec<u8>> {
    #[allow(unused)]
    pub fn new_vec_based() -> Self {
        DurableKeySetStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based(),
            file_backing: None,
            _open_lease: None,
            maintenance: crate::maintenance_coordination::MaintenanceCoordinator::default(),
            #[cfg(test)]
            mutation_observer: MutationObserver::default(),
        }
    }

    /// Creates a vector-backed key/set store using V1 timestamp configuration.
    ///
    /// This compatibility wrapper panics if physical durability is requested.
    pub fn new_vec_based_with_options(options: DurableStoreOptions) -> Self {
        Self::try_new_vec_based_with_options(options)
            .unwrap_or_else(|error| panic!("vector-backed key/set construction failed: {error}"))
    }

    /// Tries to create a vector-backed key/set store with explicit options.
    /// Physical durability returns [`crate::DurabilitySupportError::NoPhysicalBacking`].
    pub fn try_new_vec_based_with_options(
        options: DurableStoreOptions,
    ) -> Result<Self, crate::DurabilitySupportError> {
        crate::durability::validate_memory_backing(options.durability_policy())?;
        let header =
            V1CodecProbe::encode_header_with_kind_and_granularity(2, options.granularity_nanos());
        Ok(DurableKeySetStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based_v1(&header),
            file_backing: None,
            _open_lease: None,
            maintenance: MaintenanceCoordinator::default(),
            #[cfg(test)]
            mutation_observer: MutationObserver::default(),
        })
    }

    #[cfg(test)]
    pub(crate) fn try_new_vec_based_with_probe_options(
        options: DurableStoreOptions,
    ) -> Result<Self, crate::durability::DurabilitySupportError> {
        Self::try_new_vec_based_with_options(options)
    }
}

impl<W: Write> DurableKeySetStore<W> {
    #[cfg(test)]
    pub(crate) fn maintenance_probe(&self) -> &MaintenanceCoordinator {
        &self.maintenance
    }

    #[cfg(test)]
    pub(crate) fn begin_online_probe(
        &self,
        max_delta_bytes: u64,
    ) -> Result<crate::maintenance_coordination::OnlineAttemptGuard<'_, W>, ()> {
        crate::maintenance_coordination::OnlineAttemptGuard::begin(
            &self.maintenance,
            &self.wal,
            max_delta_bytes,
        )
    }

    #[cfg(test)]
    pub(crate) fn delta_group_count_probe(&self) -> usize {
        self.wal.delta_group_count_probe()
    }

    #[cfg(test)]
    pub(crate) fn has_delta_recorder_probe(&self) -> bool {
        self.wal.has_delta_recorder_probe()
    }

    #[cfg(test)]
    pub(crate) fn install_clock_probe(&self, clock: fn() -> u64) {
        self.wal.install_clock_probe(clock);
    }

    #[cfg(test)]
    pub(crate) fn timestamp_state_probe(&self) -> (u64, u64) {
        let metadata = self.wal.online_capture_metadata().unwrap();
        (metadata.granularity_nanos, metadata.last_bucket)
    }

    #[cfg(test)]
    pub(crate) fn from_probe_parts(
        initial: impl IntoIterator<Item = (Vec<u8>, HashSet<Vec<u8>>)>,
        wal: WalStorage<W>,
        mutation_observer: MutationObserver,
    ) -> Self {
        Self {
            store: initial.into_iter().collect(),
            wal,
            file_backing: None,
            _open_lease: None,
            maintenance: MaintenanceCoordinator::default(),
            mutation_observer,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_append_probe(&self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        self.try_append_core(key, value)
    }

    #[cfg(test)]
    pub(crate) fn try_remove_from_set_callback_probe(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        callback: impl FnOnce(&[u8]),
    ) -> std::io::Result<()> {
        self.try_remove_from_set_callback_core(key, value, callback)
    }

    pub fn get_hashset(&self, key: &[u8]) -> Option<HashSet<Vec<u8>>> {
        match self.store.get(key) {
            None => None,
            Some(inner_val) => {
                let found_set = inner_val.value();
                let mut result = HashSet::with_capacity(found_set.len());
                for vec in found_set {
                    result.insert(vec.clone());
                }
                Some(result)
            }
        }
    }

    pub fn contains_in_set(&self, key: &[u8], set_key: &[u8]) -> bool {
        match self.store.get(key) {
            None => false,
            Some(inner_val) => inner_val.contains(set_key),
        }
    }

    pub fn append(&self, key: Vec<u8>, val: Vec<u8>) {
        self.try_append(key, val)
            .unwrap_or_else(|error| panic!("WAL set append rejected: {error}"));
    }

    /// Persists and then publishes one set member.
    pub fn try_append(&self, key: Vec<u8>, val: Vec<u8>) -> std::io::Result<()> {
        self.try_append_core(key, val)
    }

    pub(crate) fn try_append_core(&self, key: Vec<u8>, val: Vec<u8>) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let val = self
                    .wal
                    .try_store_append_to_set_event_borrowed(entry.key(), val)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                entry.get_mut().insert(val);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let val = self
                    .wal
                    .try_store_append_to_set_event_borrowed(entry.key(), val)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                #[cfg(test)]
                let published_key = entry.key().clone();
                let mut new_hashset = HashSet::new();
                new_hashset.insert(val);
                entry.insert(new_hashset);
                #[cfg(test)]
                self.mutation_observer
                    .notify(&published_key, MutationPhase::Published);
            }
        }
        Ok(())
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.store.contains_key(key)
    }

    pub fn remove_from_set(&self, key: Vec<u8>, set_entry: Vec<u8>) {
        self.try_remove_from_set(key, set_entry)
            .unwrap_or_else(|error| panic!("WAL set removal rejected: {error}"));
    }

    /// Persists and then publishes one member removal.
    pub fn try_remove_from_set(&self, key: Vec<u8>, set_entry: Vec<u8>) -> std::io::Result<()> {
        self.try_remove_from_set_core(key, set_entry)
    }

    pub(crate) fn try_remove_from_set_core(
        &self,
        key: Vec<u8>,
        set_entry: Vec<u8>,
    ) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        #[cfg(test)]
        let observed_key = key.clone();
        if let Some(mut entry) = self.store.get_mut(&key) {
            let removes_final_member = entry.len() == 1 && entry.contains(&set_entry);
            if !removes_final_member {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let (_key, set_entry) = self.wal.try_store_remove_from_set_event(key, set_entry)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                entry.remove(&set_entry);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
                return Ok(());
            }
        }
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let removes_final_member =
                    entry.get().len() == 1 && entry.get().contains(&set_entry);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                if removes_final_member {
                    self.wal.try_store_delete_event(entry.key())?;
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                    entry.remove();
                } else {
                    let (_key, set_entry) = self
                        .wal
                        .try_store_remove_from_set_event(entry.key().clone(), set_entry)?;
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                    entry.get_mut().remove(&set_entry);
                }
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                self.wal
                    .try_store_remove_from_set_event(entry.key().clone(), set_entry)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                drop(entry);
            }
        }
        #[cfg(test)]
        self.mutation_observer
            .notify(&observed_key, MutationPhase::Published);
        Ok(())
    }

    /// Computes a replacement set on an owned working copy and invokes `func` exactly once.
    ///
    /// The accepted net delta is persisted atomically before live publication. Empty results
    /// remove the outer key, exact no-ops write nothing, and persistence or rollback failures
    /// are returned as [`std::io::Error`] without publishing callback state. If rollback itself
    /// fails, live state is still unpublished but artifact repair is outside this API's scope.
    /// The per-key DashMap entry guard remains held for the operation; stronger cross-key
    /// synchronization is not provided.
    pub fn try_compute(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut HashSet<Vec<u8>>),
    ) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key.clone()) {
            Entry::Occupied(mut occupied_entry) => {
                let original = occupied_entry.get().clone();
                let mut working = original.clone();
                func(&mut working);

                if working.is_empty() {
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(&key, MutationPhase::AcceptanceEntered);
                    self.wal
                        .commit_set_compute_batch(vec![ComputeAction::Delete {
                            key: key.clone(),
                        }])?;
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(&key, MutationPhase::AcceptedBeforePublication);
                    occupied_entry.remove();
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(&key, MutationPhase::Published);
                    return Ok(());
                }
                if working == original {
                    return Ok(());
                }

                let mut additions: Vec<_> = working.difference(&original).cloned().collect();
                let mut removals: Vec<_> = original.difference(&working).cloned().collect();
                additions.sort();
                removals.sort();
                let mut actions = Vec::with_capacity(additions.len() + removals.len());
                actions.extend(additions.into_iter().map(|value| ComputeAction::SetAppend {
                    key: key.clone(),
                    value,
                }));
                actions.extend(removals.into_iter().map(|value| ComputeAction::SetRemove {
                    key: key.clone(),
                    value,
                }));
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::AcceptanceEntered);
                self.wal.commit_set_compute_batch(actions)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::AcceptedBeforePublication);
                *occupied_entry.get_mut() = working;
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::Published);
                Ok(())
            }
            Entry::Vacant(vacant_entry) => {
                let mut working = HashSet::new();
                func(&mut working);
                if working.is_empty() {
                    return Ok(());
                }
                let mut additions: Vec<_> = working.iter().cloned().collect();
                additions.sort();
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::AcceptanceEntered);
                self.wal.commit_set_compute_batch(
                    additions
                        .into_iter()
                        .map(|value| ComputeAction::SetAppend {
                            key: key.clone(),
                            value,
                        })
                        .collect(),
                )?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::AcceptedBeforePublication);
                vacant_entry.insert(working);
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::Published);
                Ok(())
            }
        }
    }

    /// Compatibility wrapper for [`Self::try_compute`] that panics on persistence failure.
    pub fn compute(&self, key: Vec<u8>, func: impl FnOnce(&mut HashSet<Vec<u8>>)) {
        self.try_compute(key, func)
            .unwrap_or_else(|error| panic!("set compute persistence failed: {error}"));
    }

    /// Asynchronous counterpart to [`Self::try_compute`].
    ///
    /// The callback runs exactly once against a private snapshot, without a
    /// DashMap guard held across `.await`. Persistence occurs after the callback
    /// only when the same-key value still matches that snapshot. An intervening
    /// same-key change returns [`std::io::ErrorKind::WouldBlock`] without WAL or
    /// live publication. Empty and no-op results otherwise follow synchronous
    /// semantics, and cancellation discards the private candidate.
    pub async fn try_compute_async(
        &self,
        key: Vec<u8>,
        func: impl AsyncFnOnce(&mut HashSet<Vec<u8>>),
    ) -> std::io::Result<()> {
        let original = self.store.get(&key).map(|entry| entry.clone());
        let mut working = original.clone().unwrap_or_default();
        func(&mut working).await;

        let _maintenance = self.maintenance.shared();
        let conflict = || {
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "key/set changed while async compute callback was pending",
            )
        };
        match (self.store.entry(key.clone()), original) {
            (Entry::Occupied(mut occupied_entry), Some(original)) => {
                if occupied_entry.get() != &original {
                    return Err(conflict());
                }
                if working.is_empty() {
                    self.wal
                        .commit_set_compute_batch(vec![ComputeAction::Delete {
                            key: key.clone(),
                        }])?;
                    occupied_entry.remove();
                    return Ok(());
                }
                if working == original {
                    return Ok(());
                }
                let mut additions: Vec<_> = working.difference(&original).cloned().collect();
                let mut removals: Vec<_> = original.difference(&working).cloned().collect();
                additions.sort();
                removals.sort();
                let mut actions = Vec::with_capacity(additions.len() + removals.len());
                actions.extend(additions.into_iter().map(|value| ComputeAction::SetAppend {
                    key: key.clone(),
                    value,
                }));
                actions.extend(removals.into_iter().map(|value| ComputeAction::SetRemove {
                    key: key.clone(),
                    value,
                }));
                self.wal.commit_set_compute_batch(actions)?;
                *occupied_entry.get_mut() = working;
                Ok(())
            }
            (Entry::Vacant(vacant_entry), None) => {
                if working.is_empty() {
                    return Ok(());
                }
                let mut additions: Vec<_> = working.iter().cloned().collect();
                additions.sort();
                self.wal.commit_set_compute_batch(
                    additions
                        .into_iter()
                        .map(|value| ComputeAction::SetAppend {
                            key: key.clone(),
                            value,
                        })
                        .collect(),
                )?;
                vacant_entry.insert(working);
                Ok(())
            }
            (Entry::Occupied(_), None) | (Entry::Vacant(_), Some(_)) => Err(conflict()),
        }
    }

    /// Compatibility wrapper for [`Self::try_compute_async`] that panics on
    /// persistence failure or optimistic conflict.
    pub async fn compute_async(&self, key: Vec<u8>, func: impl AsyncFnOnce(&mut HashSet<Vec<u8>>)) {
        self.try_compute_async(key, func)
            .await
            .unwrap_or_else(|error| panic!("async set compute failed: {error}"));
    }
    /// Computes only when `key` is present, otherwise returns `Ok(())` without invoking `func`.
    ///
    /// Eligible callbacks run once. Accepted empty results delete the outer key, no-ops write
    /// nothing, and commit/rollback errors leave the original live value unpublished.
    pub fn try_compute_if_present(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut HashSet<Vec<u8>>),
    ) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key.clone()) {
            Entry::Occupied(mut occupied_entry) => {
                let original = occupied_entry.get().clone();
                let mut working = original.clone();
                func(&mut working);
                if working.is_empty() {
                    self.wal
                        .commit_set_compute_batch(vec![ComputeAction::Delete {
                            key: key.clone(),
                        }])?;
                    occupied_entry.remove();
                    return Ok(());
                }
                if working == original {
                    return Ok(());
                }
                let mut additions: Vec<_> = working.difference(&original).cloned().collect();
                let mut removals: Vec<_> = original.difference(&working).cloned().collect();
                additions.sort();
                removals.sort();
                let mut actions = Vec::with_capacity(additions.len() + removals.len());
                actions.extend(additions.into_iter().map(|value| ComputeAction::SetAppend {
                    key: key.clone(),
                    value,
                }));
                actions.extend(removals.into_iter().map(|value| ComputeAction::SetRemove {
                    key: key.clone(),
                    value,
                }));
                self.wal.commit_set_compute_batch(actions)?;
                *occupied_entry.get_mut() = working;
                Ok(())
            }
            Entry::Vacant(_) => Ok(()),
        }
    }

    /// Compatibility wrapper for [`Self::try_compute_if_present`] that panics on error.
    pub fn compute_if_present(&self, key: Vec<u8>, func: impl FnOnce(&mut HashSet<Vec<u8>>)) {
        self.try_compute_if_present(key, func)
            .unwrap_or_else(|error| panic!("set compute-if-present persistence failed: {error}"));
    }

    /// Computes only when `key` is absent, otherwise returns `Ok(())` without invoking `func`.
    ///
    /// An eligible callback runs once. An empty result creates no key or WAL frame; a non-empty
    /// result is persisted before publication. Commit/rollback errors publish no callback state.
    pub fn try_compute_if_absent(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut HashSet<Vec<u8>>),
    ) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key.clone()) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(vacant_entry) => {
                let mut working = HashSet::new();
                func(&mut working);
                if working.is_empty() {
                    return Ok(());
                }
                let mut additions: Vec<_> = working.iter().cloned().collect();
                additions.sort();
                self.wal.commit_set_compute_batch(
                    additions
                        .into_iter()
                        .map(|value| ComputeAction::SetAppend {
                            key: key.clone(),
                            value,
                        })
                        .collect(),
                )?;
                vacant_entry.insert(working);
                Ok(())
            }
        }
    }

    /// Compatibility wrapper for [`Self::try_compute_if_absent`] that panics on error.
    pub fn compute_if_absent(&self, key: Vec<u8>, func: impl FnOnce(&mut HashSet<Vec<u8>>)) {
        self.try_compute_if_absent(key, func)
            .unwrap_or_else(|error| panic!("set compute-if-absent persistence failed: {error}"));
    }

    pub fn remove_from_set_callback(
        &self,
        key: Vec<u8>,
        set_entry: Vec<u8>,
        key_removed_callback: impl FnOnce(&[u8]),
    ) {
        self.try_remove_from_set_callback(key, set_entry, key_removed_callback)
            .unwrap_or_else(|error| panic!("WAL set callback removal rejected: {error}"));
    }

    /// Removes a member and calls `key_removed_callback` only after an accepted
    /// final-member deletion is published.
    pub fn try_remove_from_set_callback(
        &self,
        key: Vec<u8>,
        set_entry: Vec<u8>,
        key_removed_callback: impl FnOnce(&[u8]),
    ) -> std::io::Result<()> {
        self.try_remove_from_set_callback_core(key, set_entry, key_removed_callback)
    }

    pub(crate) fn try_remove_from_set_callback_core(
        &self,
        key: Vec<u8>,
        set_entry: Vec<u8>,
        key_removed_callback: impl FnOnce(&[u8]),
    ) -> std::io::Result<()> {
        #[cfg(test)]
        let observed_key = key.clone();
        let mut callback_entry = Some(set_entry);
        let removed_key = {
            let _maintenance = self.maintenance.shared();
            let removed_key = match self.store.entry(key) {
                Entry::Occupied(mut entry) => {
                    let removes_final_member = entry.get().len() == 1
                        && entry
                            .get()
                            .contains(callback_entry.as_ref().expect("removal entry"));
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::AcceptanceEntered);
                    if removes_final_member {
                        self.wal.try_store_delete_event(entry.key())?;
                        #[cfg(test)]
                        self.mutation_observer
                            .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                        entry.remove();
                        true
                    } else {
                        let set_entry = callback_entry.take().expect("removal entry");
                        let (_key, set_entry) = self
                            .wal
                            .try_store_remove_from_set_event(entry.key().clone(), set_entry)?;
                        #[cfg(test)]
                        self.mutation_observer
                            .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                        entry.get_mut().remove(&set_entry);
                        false
                    }
                }
                Entry::Vacant(entry) => {
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::AcceptanceEntered);
                    self.wal.try_store_remove_from_set_event(
                        entry.key().clone(),
                        callback_entry.take().expect("removal entry"),
                    )?;
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                    drop(entry);
                    false
                }
            };
            #[cfg(test)]
            self.mutation_observer
                .notify(&observed_key, MutationPhase::Published);
            removed_key
        };
        if removed_key {
            key_removed_callback(
                callback_entry
                    .as_deref()
                    .expect("final removal callback entry"),
            );
        }
        Ok(())
    }

    pub fn remove_key(&self, key: &[u8]) {
        self.try_remove_key(key)
            .unwrap_or_else(|error| panic!("WAL delete rejected: {error}"));
    }

    /// Persists an outer-key deletion before removing live state.
    pub fn try_remove_key(&self, key: &[u8]) -> std::io::Result<()> {
        self.try_remove_key_core(key)
    }

    pub(crate) fn try_remove_key_core(&self, key: &[u8]) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key.to_vec()) {
            Entry::Occupied(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                self.wal.try_store_delete_event(entry.key())?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                entry.remove();
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                self.wal.try_store_delete_event(entry.key())?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                drop(entry);
            }
        }
        #[cfg(test)]
        self.mutation_observer.notify(key, MutationPhase::Published);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.store.len()
    }
}

#[cfg(test)]
#[path = "mutation_ordering_tests/key_set.rs"]
mod mutation_ordering_tests;

#[cfg(test)]
mod tests {

    use super::{DurableKeySetStore, MutationObserver};
    use crate::wal::WalStorage;
    use dashmap::DashMap;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct FaultState {
        bytes: Vec<u8>,
        fail_after: Option<usize>,
        fail_flush: bool,
    }

    #[derive(Clone, Default)]
    struct FaultWriter(Arc<Mutex<FaultState>>);

    impl Write for FaultWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let mut state = self.0.lock().unwrap();
            match state.fail_after {
                Some(0) => Err(io::Error::other("injected write failure")),
                Some(remaining) => {
                    let written = remaining.min(bytes.len());
                    state.bytes.extend_from_slice(&bytes[..written]);
                    state.fail_after = Some(remaining - written);
                    Ok(written)
                }
                None => {
                    state.bytes.extend_from_slice(bytes);
                    Ok(bytes.len())
                }
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.0.lock().unwrap().fail_flush {
                Err(io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    fn rollback(writer: &mut FaultWriter, checkpoint: usize) -> io::Result<()> {
        writer.0.lock().unwrap().bytes.truncate(checkpoint);
        Ok(())
    }

    fn fault_store() -> (DurableKeySetStore<FaultWriter>, Arc<Mutex<FaultState>>) {
        let writer = FaultWriter::default();
        let state = Arc::clone(&writer.0);
        let store = DurableKeySetStore {
            store: DashMap::new(),
            wal: WalStorage::new_with_rollback(writer, rollback),
            file_backing: None,
            _open_lease: None,
            maintenance: crate::maintenance_coordination::MaintenanceCoordinator::default(),
            mutation_observer: MutationObserver::default(),
        };
        (store, state)
    }

    #[test]
    fn simple_test() {
        use super::*;

        let store = DurableKeySetStore::new_vec_based();

        store.append(b"a".to_vec(), b"apple".to_vec());
        store.append(b"a".to_vec(), b"article".to_vec());
        store.append(b"a".to_vec(), b"atmosphere".to_vec());

        store.append(b"b".to_vec(), b"banana".to_vec());

        store.append(b"c".to_vec(), b"cinema".to_vec());
        store.append(b"c".to_vec(), b"cinamon".to_vec());

        assert_eq!(store.size(), 3);

        let res_a = store.get_hashset(b"a").unwrap();

        assert!(res_a.contains(&b"apple".to_vec()[..]));
        assert!(res_a.contains(&b"article".to_vec()[..]));
        assert!(res_a.contains(&b"atmosphere".to_vec()[..]));
        assert!(!res_a.contains(&b"banana".to_vec()[..]));

        store.remove_from_set(b"a".to_vec(), b"article".to_vec());
        let res_a = store.get_hashset(b"a").unwrap();
        assert!(!res_a.contains(&b"article".to_vec()[..]));

        let res_b = store.get_hashset(b"b").unwrap();
        assert_eq!(res_b.len(), 1);
        assert!(res_b.contains(&b"banana".to_vec()[..]));
        assert!(!res_b.contains(&b"apple".to_vec()[..]));

        let res_c = store.get_hashset(b"c").unwrap();
        assert_eq!(res_c.len(), 2);
        assert!(res_c.contains(&b"cinema".to_vec()[..]));
        assert!(res_c.contains(&b"cinamon".to_vec()[..]));
        assert!(!res_c.contains(&b"apple".to_vec()[..]));

        store.remove_key(b"b");
        assert_eq!(store.size(), 2);
    }

    #[test]
    fn test_compute() {
        let store = crate::key_set_store::DurableKeySetStore::new_vec_based();

        store.compute(vec![0], |set| {
            set.insert(vec![1]);
        });
        store.compute(vec![0], |set| {
            set.insert(vec![2]);
        });

        let res_set = store.get_hashset(&[0]).unwrap();
        assert_eq!(res_set.len(), 2);

        assert_eq!(store.get_hashset(&[1]), None);
    }

    #[test]
    fn test_compute_if_present() {
        let store = crate::key_set_store::DurableKeySetStore::new_vec_based();

        store.compute_if_present(vec![0], |set| {
            set.insert(vec![1]);
        });
        let res_set = store.get_hashset(&[0]);
        assert_eq!(res_set, None);

        store.append(vec![0], vec![1]);

        store.compute_if_present(vec![0], |set| {
            set.insert(vec![2]);
        });

        let res_set = store.get_hashset(&[0]).unwrap();
        assert_eq!(res_set.len(), 2);

        assert_eq!(store.get_hashset(&[1]), None);
    }

    #[test]
    fn test_compute_if_absent() {
        let store = crate::key_set_store::DurableKeySetStore::new_vec_based();
        store.append(vec![0], vec![1]);

        store.compute_if_absent(vec![0], |set| {
            set.insert(vec![1]);
        });
        let res_set = store.get_hashset(&[0]).unwrap();
        assert_eq!(res_set.len(), 1);

        store.compute_if_absent(vec![1], |set| {
            set.insert(vec![3]);
        });

        let res_set = store.get_hashset(&[1]).unwrap();
        assert_eq!(res_set.len(), 1);

        assert_eq!(store.get_hashset(&[2]), None);
    }

    #[test]
    fn test_remove_if_empty() {
        use super::*;

        let store = DurableKeySetStore::new_vec_based();

        store.append(b"a".to_vec(), b"apple".to_vec());
        store.append(b"a".to_vec(), b"apricote".to_vec());

        store.append(b"b".to_vec(), b"banana".to_vec());

        assert_eq!(store.size(), 2);

        store.remove_from_set(b"a".to_vec(), b"apple".to_vec());
        assert_eq!(store.size(), 2);

        store.remove_from_set(b"a".to_vec(), b"apricote".to_vec());
        assert_eq!(store.size(), 1);

        store.remove_from_set(b"b".to_vec(), b"banana".to_vec());
        assert_eq!(store.size(), 0);
    }

    #[test]
    fn partial_write_rejection_keeps_set_live_and_replay_state() {
        let (store, state) = fault_store();
        store.append(b"key".to_vec(), b"original".to_vec());
        let prefix = state.lock().unwrap().bytes.clone();
        state.lock().unwrap().fail_after = Some(5);
        let calls = std::sync::atomic::AtomicUsize::new(0);

        assert!(store
            .try_compute(b"key".to_vec(), |set| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                set.insert(b"rejected".to_vec());
            })
            .is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            store.get_hashset(b"key").unwrap(),
            [b"original".to_vec()].into_iter().collect()
        );
        assert_eq!(state.lock().unwrap().bytes, prefix);
        assert_eq!(
            crate::wal::read_for_set(&prefix)
                .get(b"key".as_slice())
                .unwrap(),
            &[b"original".to_vec()].into_iter().collect()
        );
    }

    #[test]
    fn flush_rejection_errors_or_panics_without_set_publication() {
        for compatibility in [false, true] {
            let (store, state) = fault_store();
            store.append(b"key".to_vec(), b"original".to_vec());
            let prefix = state.lock().unwrap().bytes.clone();
            state.lock().unwrap().fail_flush = true;
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if compatibility {
                    store.compute(b"key".to_vec(), |set| {
                        set.insert(b"rejected".to_vec());
                    });
                    Ok(())
                } else {
                    store.try_compute(b"key".to_vec(), |set| {
                        set.insert(b"rejected".to_vec());
                    })
                }
            }));
            assert_eq!(outcome.is_err(), compatibility);
            if !compatibility {
                assert!(outcome.unwrap().is_err());
            }
            assert_eq!(
                store.get_hashset(b"key").unwrap(),
                [b"original".to_vec()].into_iter().collect()
            );
            assert_eq!(state.lock().unwrap().bytes, prefix);
            assert_eq!(
                crate::wal::read_for_set(&prefix)
                    .get(b"key".as_slice())
                    .unwrap(),
                &[b"original".to_vec()].into_iter().collect()
            );
        }
    }

    #[test]
    fn conditional_rejection_preserves_set_eligibility_and_state() {
        for present in [true, false] {
            for compatibility in [false, true] {
                let (store, state) = fault_store();
                if present {
                    store.append(b"key".to_vec(), b"original".to_vec());
                }
                state.lock().unwrap().fail_flush = true;
                let calls = std::sync::atomic::AtomicUsize::new(0);
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if present {
                        if compatibility {
                            store.compute_if_present(b"key".to_vec(), |set| {
                                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                set.insert(b"rejected".to_vec());
                            });
                            Ok(())
                        } else {
                            store.try_compute_if_present(b"key".to_vec(), |set| {
                                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                set.insert(b"rejected".to_vec());
                            })
                        }
                    } else if compatibility {
                        store.compute_if_absent(b"key".to_vec(), |set| {
                            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            set.insert(b"rejected".to_vec());
                        });
                        Ok(())
                    } else {
                        store.try_compute_if_absent(b"key".to_vec(), |set| {
                            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            set.insert(b"rejected".to_vec());
                        })
                    }
                }));
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                assert_eq!(outcome.is_err(), compatibility);
                if !compatibility {
                    assert!(outcome.unwrap().is_err());
                }
                assert_eq!(store.contains_key(b"key"), present);
                if present {
                    assert_eq!(
                        store.get_hashset(b"key").unwrap(),
                        [b"original".to_vec()].into_iter().collect()
                    );
                }
            }
        }
    }

    #[test]
    fn async_rejection_errors_or_panics_without_set_publication() {
        fn block_on<F: std::future::Future>(future: F) -> F::Output {
            use std::pin::pin;
            use std::task::{Context, Poll, Waker};
            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            let mut future = pin!(future);
            loop {
                if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
                    return value;
                }
            }
        }
        for compatibility in [false, true] {
            let (store, state) = fault_store();
            store.append(b"key".to_vec(), b"original".to_vec());
            state.lock().unwrap().fail_flush = true;
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if compatibility {
                    block_on(store.compute_async(b"key".to_vec(), async |set| {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        set.insert(b"rejected".to_vec());
                    }));
                    Ok(())
                } else {
                    block_on(store.try_compute_async(b"key".to_vec(), async |set| {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        set.insert(b"rejected".to_vec());
                    }))
                }
            }));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(outcome.is_err(), compatibility);
            if !compatibility {
                assert!(outcome.unwrap().is_err());
            }
            assert_eq!(
                store.get_hashset(b"key").unwrap(),
                [b"original".to_vec()].into_iter().collect()
            );
        }
    }
}
