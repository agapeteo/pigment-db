use std::fs::File;
#[cfg(test)]
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::maintenance_coordination::{MaintenanceCoordinator, OpenDirectoryLease};
use crate::wal::format::V1CodecProbe;
use crate::wal::recovery::{
    encode_key_value_repair_snapshot, initialize_snapshot_with_policy, ArtifactPaths, StoreKind,
};
use crate::wal::replay::{
    encode_key_value_snapshot, key_value_is_proper_snapshot_prefix, replay_key_value,
    replay_key_value_against, replay_key_value_tail,
};
use crate::wal::WalStorage;
use crate::{DurableStoreOptions, RecoveryError, RecoveryOutcome, RecoveryStatus};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use log::info;

#[cfg(test)]
use crate::test_support::mutation_schedule::{MutationObserver, MutationPhase};

/// Mutations are ordered per logical outer key, while mutations of keys in different data-map shards remain concurrent except during shared WAL acceptance.
///
/// Different keys in the same DashMap shard may wait for one another. A
/// compute callback runs while that shard is guarded, so recursively accessing
/// the same map or shard is unsupported and may deadlock. If the callback
/// panics before acceptance, its candidate is discarded and the guard is
/// released during unwinding. These guarantees do not change any public method
/// signature or compatibility panic behavior.
pub struct DurableKeyValueStore<W: Write> {
    store: DashMap<Vec<u8>, Vec<u8>>,
    wal: WalStorage<W>,
    file_backing: Option<PathBuf>,
    _open_lease: Option<OpenDirectoryLease>,
    maintenance: MaintenanceCoordinator,
    #[cfg(test)]
    mutation_observer: MutationObserver,
}

impl DurableKeyValueStore<File> {
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
            crate::compaction::inspection::InspectedFamily::KeyValue,
            max_delta_bytes,
            || {
                crate::compaction::CapturedLogicalState::Value(
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
            let live_state = crate::compaction::CapturedLogicalState::Value(
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
        };
        let applied = applied?;
        Ok(crate::compaction::AppliedOnlineDelta {
            staged,
            replayed: applied.replayed,
            encoded_bytes: applied.encoded_bytes,
            accepted_buckets: applied.accepted_buckets,
            group_frame_counts: applied.group_frame_counts,
        })
    }

    #[cfg(test)]
    pub(crate) fn complete_online_cutover_probe<'a>(
        &'a self,
        mut staged: crate::compaction::ValidatedOnlineStaging<'a, File>,
    ) -> Result<crate::compaction::CompletedOnlineCutover, crate::CompactionError> {
        let completed = {
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
            let live_state = crate::compaction::CapturedLogicalState::Value(
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
            crate::compaction::finalize_online_prepared(&mut staged, live_state, metadata)?;

            let token = staged.prepared.attempt.token();
            let detached = self.wal.take_online_writer(token).map_err(|source| {
                crate::CompactionError::Io {
                    operation: crate::CompactionOperation::PublishPrevious,
                    path: staged.prepared.paths.manifest.clone(),
                    source,
                }
            })?;
            let closed = detached.close();
            crate::compaction::publication::publish_online_previous(
                &staged.prepared.paths,
                &mut staged.prepared.manifest,
            )?;
            let expected_len = staged
                .replacement_inventory
                .first()
                .ok_or_else(|| crate::CompactionError::InvalidArtifact {
                    path: staged.prepared.paths.staging.clone(),
                })?
                .length;
            let (active_path, writer) =
                crate::compaction::publication::publish_online_replacement_with_reopen(
                    &staged.prepared.paths,
                    &mut staged.prepared.manifest,
                    |active_path| {
                        let writer = OpenOptions::new().append(true).open(active_path)?;
                        if writer.metadata()?.len() != expected_len {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "online replacement length changed before writer handoff",
                            ));
                        }
                        Ok(writer)
                    },
                )?;
            self.wal
                .install_online_replacement_writer(
                    closed,
                    writer,
                    active_path,
                    expected_len,
                    staged.staging.granularity_nanos,
                    staged.staging.last_bucket,
                )
                .map_err(|source| crate::CompactionError::Io {
                    operation: crate::CompactionOperation::ReopenReplacement,
                    path: staged.prepared.paths.manifest.clone(),
                    source,
                })?;
            crate::compaction::CompletedOnlineCutover {
                replayed: applied.replayed,
                paths: staged.prepared.paths.clone(),
                manifest: staged.prepared.manifest.clone(),
            }
        };
        drop(staged);
        Ok(completed)
    }

    /// Returns exact storage usage for this open key/value generation.
    ///
    /// Vector-backed stores intentionally do not expose filesystem maintenance:
    ///
    /// ```compile_fail
    /// use pigment_db::key_value_store::DurableKeyValueStore;
    /// let store = DurableKeyValueStore::new_vec_based();
    /// let _ = store.storage_stats();
    /// ```
    pub fn storage_stats(&self) -> Result<crate::FamilyStorageStats, crate::CompactionError> {
        crate::maintenance::public_file_family_storage_stats(
            self.file_backing
                .as_deref()
                .expect("file-backed store retains its directory identity"),
            crate::compaction::inspection::InspectedFamily::KeyValue,
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
            crate::compaction::inspection::InspectedFamily::KeyValue,
        )
    }

    #[cfg(test)]
    pub(crate) fn storage_stats_probe(
        &self,
    ) -> std::io::Result<crate::compaction::inspection::FamilyInspection> {
        self.storage_stats_internal()
    }

    /// Opens a file-backed key/value store without panicking on expected
    /// recovery or filesystem failures.
    ///
    /// The returned status is [`RecoveryStatus::Recovered`] when startup
    /// resolves legacy recovery or staging artifacts. Staging is never chosen
    /// as authority, and ambiguous candidates are preserved on error.
    pub fn try_init_new(
        store_dir: impl AsRef<Path>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError> {
        Self::try_init_new_configured(store_dir, None)
    }

    /// Opens a file-backed key/value store with explicit timestamp and durability options.
    ///
    /// A missing store is published as a complete V2 active segment. An
    /// explicit granularity change rotates before the next accepted mutation;
    /// unrelated options preserve the active segment's persisted granularity.
    /// Complete legacy and V1 input require the standalone migration command.
    /// Physical mode performs filesystem capability preflights and crash-safe
    /// namespace publication before returning a store.
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
        let maintenance_recovered =
            crate::compaction::recovery::resolve_directory_maintenance(store_dir)?;
        let paths = ArtifactPaths::new(store_dir, StoreKind::Value);
        let durability_policy = options
            .map(DurableStoreOptions::durability_policy)
            .unwrap_or_default();
        let wal_segment_size = options.unwrap_or_default().wal_segment_size().as_bytes();
        let initialized = initialize_snapshot_with_policy(
            &paths,
            replay_key_value,
            replay_key_value_tail,
            replay_key_value_against,
            encode_key_value_snapshot,
            encode_key_value_repair_snapshot,
            key_value_is_proper_snapshot_prefix,
            Some(V1CodecProbe::encode_header()),
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
        for (key, value) in initialized.snapshot {
            store.insert(key, value);
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
            DurableKeyValueStore {
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

    #[cfg(test)]
    pub(crate) fn try_init_new_with_probe_options(
        store_dir: impl AsRef<Path>,
        options: DurableStoreOptions,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError> {
        Self::try_init_new_configured(store_dir, Some(options))
    }

    /// Opens a file-backed key/value store using the historical panic-on-error API.
    ///
    /// This compatibility wrapper delegates to [`Self::try_init_new`] and logs
    /// exactly when automatic recovery succeeds.
    pub fn init_new(store_dir: &str) -> Self {
        let outcome = Self::try_init_new(store_dir).unwrap_or_else(|error| panic!("{error}"));
        let (store, status) = outcome.into_parts();
        if status == RecoveryStatus::Recovered {
            info!("pigment-db recovered key/value WAL in {store_dir}");
        }
        store
    }
}

impl DurableKeyValueStore<Vec<u8>> {
    #[allow(unused)]
    pub fn new_vec_based() -> Self {
        DurableKeyValueStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based(),
            file_backing: None,
            _open_lease: None,
            maintenance: MaintenanceCoordinator::default(),
            #[cfg(test)]
            mutation_observer: MutationObserver::default(),
        }
    }

    /// Creates a vector-backed key/value store using V1 timestamp configuration.
    ///
    /// This compatibility wrapper panics if physical durability is requested.
    pub fn new_vec_based_with_options(options: DurableStoreOptions) -> Self {
        Self::try_new_vec_based_with_options(options)
            .unwrap_or_else(|error| panic!("vector-backed key/value construction failed: {error}"))
    }

    /// Tries to create a vector-backed key/value store with explicit options.
    /// Physical durability returns [`crate::DurabilitySupportError::NoPhysicalBacking`].
    pub fn try_new_vec_based_with_options(
        options: DurableStoreOptions,
    ) -> Result<Self, crate::DurabilitySupportError> {
        crate::durability::validate_memory_backing(options.durability_policy())?;
        let header = V1CodecProbe::encode_header_with_granularity(options.granularity_nanos());
        Ok(DurableKeyValueStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based_v1(&header),
            file_backing: None,
            _open_lease: None,
            maintenance: MaintenanceCoordinator::default(),
            #[cfg(test)]
            mutation_observer: MutationObserver::default(),
        })
    }
}

#[cfg(test)]
impl<W: Write> DurableKeyValueStore<W> {
    pub(crate) fn runtime_policy_probe(&self) -> crate::config::DurabilityPolicy {
        self.wal.runtime_policy_probe()
    }
}

impl<W: Write> DurableKeyValueStore<W> {
    #[cfg(test)]
    pub(crate) fn maintenance_probe(&self) -> &MaintenanceCoordinator {
        &self.maintenance
    }

    #[cfg(test)]
    pub(crate) fn delta_group_count_probe(&self) -> usize {
        self.wal.delta_group_count_probe()
    }

    #[cfg(test)]
    pub(crate) fn delta_used_bytes_probe(&self) -> u64 {
        self.wal.delta_used_bytes_probe()
    }

    #[cfg(test)]
    pub(crate) fn has_delta_recorder_probe(&self) -> bool {
        self.wal.has_delta_recorder_probe()
    }

    #[cfg(test)]
    pub(crate) fn inject_live_value_probe(&self, key: Vec<u8>, value: Vec<u8>) {
        self.store.insert(key, value);
    }

    #[cfg(test)]
    pub(crate) fn install_clock_probe(&self, clock: fn() -> u64) {
        self.wal.install_clock_probe(clock);
    }

    #[cfg(test)]
    pub(crate) fn online_wal_state_probe(&self) -> crate::wal::OnlineWalStateProbe {
        self.wal.online_wal_state_probe()
    }

    #[cfg(test)]
    pub(crate) fn from_probe_parts(
        initial: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
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
    pub(crate) fn try_put_probe(&self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<()> {
        self.try_put_core(key, value)
    }

    #[cfg(test)]
    pub(crate) fn try_compute_probe(
        &self,
        key: Vec<u8>,
        function: impl FnOnce(Option<&[u8]>) -> Vec<u8>,
    ) -> std::io::Result<()> {
        self.try_compute_core(key, function)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.store.get(key) {
            None => None,
            Some(inner_val) => {
                let result = Vec::from(&inner_val.value()[..]);
                Some(result)
            }
        }
    }

    pub fn put(&self, key: Vec<u8>, val: Vec<u8>) {
        self.try_put(key, val)
            .unwrap_or_else(|error| panic!("WAL put rejected: {error}"));
    }

    /// Persists and then publishes a value replacement.
    pub fn try_put(&self, key: Vec<u8>, val: Vec<u8>) -> std::io::Result<()> {
        self.try_put_core(key, val)
    }

    pub(crate) fn try_put_core(&self, key: Vec<u8>, val: Vec<u8>) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        if let Some(mut entry) = self.store.get_mut(&key) {
            #[cfg(test)]
            self.mutation_observer
                .notify(entry.key(), MutationPhase::AcceptanceEntered);
            let (_key, val) = self.wal.try_store_put_event(key, val)?;
            #[cfg(test)]
            self.mutation_observer
                .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
            *entry = val;
            #[cfg(test)]
            self.mutation_observer
                .notify(entry.key(), MutationPhase::Published);
            return Ok(());
        }
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let (_key, val) = self.wal.try_store_put_event(entry.key().clone(), val)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                *entry.get_mut() = val;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let (_key, val) = self.wal.try_store_put_event(entry.key().clone(), val)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                #[cfg(test)]
                let published_key = entry.key().clone();
                entry.insert(val);
                #[cfg(test)]
                self.mutation_observer
                    .notify(&published_key, MutationPhase::Published);
            }
        }
        Ok(())
    }

    pub fn compute(&self, key: Vec<u8>, func: impl FnOnce(Option<&[u8]>) -> Vec<u8>) {
        self.try_compute(key, func)
            .unwrap_or_else(|error| panic!("WAL compute put rejected: {error}"));
    }

    /// Runs the callback once, persists its candidate, and publishes only on success.
    pub fn try_compute(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(Option<&[u8]>) -> Vec<u8>,
    ) -> std::io::Result<()> {
        self.try_compute_core(key, func)
    }

    pub(crate) fn try_compute_core(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(Option<&[u8]>) -> Vec<u8>,
    ) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let new_val = func(Some(entry.get().as_slice()));
                let (_key, new_val) = self.wal.try_store_put_event(entry.key().clone(), new_val)?;
                *entry.get_mut() = new_val;
            }
            Entry::Vacant(entry) => {
                let new_val = func(None);
                let (_key, new_val) = self.wal.try_store_put_event(entry.key().clone(), new_val)?;
                entry.insert(new_val);
            }
        };
        Ok(())
    }

    #[allow(clippy::result_unit_err)] // Public compatibility signature.
    pub fn increment_or_init(&self, key: Vec<u8>, increment_by: u64) -> Result<u64, ()> {
        self.try_increment_or_init(key, increment_by)
            .unwrap_or_else(|error| panic!("WAL increment rejected: {error}"))
    }

    #[allow(clippy::result_unit_err)]
    /// Persists a numeric increment while preserving the nested numeric-error result.
    ///
    /// The inner result is `Err(())` when the current value is not a native-endian
    /// `u64` or when the addition would overflow. Rejected increments leave both
    /// live and persisted state unchanged.
    pub fn try_increment_or_init(
        &self,
        key: Vec<u8>,
        increment_by: u64,
    ) -> std::io::Result<Result<u64, ()>> {
        self.try_increment_or_init_core(key, increment_by)
    }

    #[allow(clippy::result_unit_err)]
    pub(crate) fn try_increment_or_init_core(
        &self,
        key: Vec<u8>,
        increment_by: u64,
    ) -> std::io::Result<Result<u64, ()>> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let entry_bytes = entry.get().as_slice();
                let bytes_arr: [u8; 8] =
                    match <&[u8] as std::convert::TryInto<[u8; 8]>>::try_into(entry_bytes) {
                        Ok(arr) => arr,
                        Err(_) => {
                            return Ok(Err(()));
                        }
                    };
                let cur_num = u64::from_ne_bytes(bytes_arr);
                let Some(new_num) = cur_num.checked_add(increment_by) else {
                    return Ok(Err(()));
                };
                let (_key, new_num_bytes) = self
                    .wal
                    .try_store_put_event(entry.key().clone(), u64::to_ne_bytes(new_num).to_vec())?;
                *entry.get_mut() = new_num_bytes;
                Ok(Ok(new_num))
            }
            Entry::Vacant(entry) => {
                let new_num = increment_by;
                let (_key, new_num_bytes) = self
                    .wal
                    .try_store_put_event(entry.key().clone(), u64::to_ne_bytes(new_num).to_vec())?;
                entry.insert(new_num_bytes);
                Ok(Ok(new_num))
            }
        }
    }

    pub fn decrement(&self, key: Vec<u8>, decrement_by: u64) -> Option<Result<u64, ()>> {
        self.try_decrement(key, decrement_by)
            .unwrap_or_else(|error| panic!("WAL decrement rejected: {error}"))
    }

    #[allow(clippy::result_unit_err)]
    /// Persists a saturating decrement while preserving absence and invalid-value results.
    pub fn try_decrement(
        &self,
        key: Vec<u8>,
        decrement_by: u64,
    ) -> std::io::Result<Option<Result<u64, ()>>> {
        self.try_decrement_core(key, decrement_by)
    }

    #[allow(clippy::result_unit_err)]
    pub(crate) fn try_decrement_core(
        &self,
        key: Vec<u8>,
        decrement_by: u64,
    ) -> std::io::Result<Option<Result<u64, ()>>> {
        let _maintenance = self.maintenance.shared();
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let entry_bytes = entry.get().as_slice();
                let bytes_arr: [u8; 8] =
                    match <&[u8] as std::convert::TryInto<[u8; 8]>>::try_into(entry_bytes) {
                        Ok(arr) => arr,
                        Err(_) => {
                            return Ok(Some(Err(())));
                        }
                    };
                let cur_num = u64::from_ne_bytes(bytes_arr);
                let new_num = cur_num.saturating_sub(decrement_by);
                let (_key, new_num_bytes) = self
                    .wal
                    .try_store_put_event(entry.key().clone(), u64::to_ne_bytes(new_num).to_vec())?;
                *entry.get_mut() = new_num_bytes;
                Ok(Some(Ok(new_num)))
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn read_number(&self, key: &[u8]) -> Option<Result<u64, ()>> {
        self.store.get(key).map(|entry_bytes| {
            let byters_arr: [u8; 8] = match <&[u8] as std::convert::TryInto<[u8; 8]>>::try_into(
                entry_bytes.value().as_slice(),
            ) {
                Ok(arr) => arr,
                Err(_) => {
                    return Err(());
                }
            };
            Ok(u64::from_ne_bytes(byters_arr))
        })
    }

    pub fn set_number(&self, key: Vec<u8>, number: u64) {
        self.try_set_number(key, number)
            .unwrap_or_else(|error| panic!("WAL set-number rejected: {error}"));
    }

    /// Persists and publishes a native-endian `u64` value.
    pub fn try_set_number(&self, key: Vec<u8>, number: u64) -> std::io::Result<()> {
        self.try_set_number_core(key, number)
    }

    pub(crate) fn try_set_number_core(&self, key: Vec<u8>, number: u64) -> std::io::Result<()> {
        self.try_put_core(key, u64::to_ne_bytes(number).to_vec())
    }

    #[allow(unused)]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.store.contains_key(key)
    }

    pub fn remove(&self, key: &[u8]) {
        self.try_remove(key)
            .unwrap_or_else(|error| panic!("WAL delete rejected: {error}"));
    }

    /// Persists an outer-key deletion before removing live state.
    pub fn try_remove(&self, key: &[u8]) -> std::io::Result<()> {
        self.try_remove_core(key)
    }

    pub(crate) fn try_remove_core(&self, key: &[u8]) -> std::io::Result<()> {
        let _maintenance = self.maintenance.shared();
        let entry = self.store.entry(key.to_vec());
        #[cfg(test)]
        self.mutation_observer
            .notify(key, MutationPhase::AcceptanceEntered);
        self.wal.try_store_delete_event(key)?;
        #[cfg(test)]
        self.mutation_observer
            .notify(key, MutationPhase::AcceptedBeforePublication);
        match entry {
            Entry::Occupied(entry) => {
                entry.remove();
            }
            Entry::Vacant(entry) => drop(entry),
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
#[path = "mutation_ordering_tests/key_value.rs"]
mod mutation_ordering_tests;

mod tests {
    #[test]
    fn simple_test() {
        use super::*;

        let store = DurableKeyValueStore::new_vec_based();

        store.put(b"key_1".to_vec(), b"value_1".to_vec());
        store.put(b"key_2".to_vec(), b"value_2".to_vec());

        let res_1 = store.get(b"key_1");
        assert_eq!(res_1.unwrap(), b"value_1");

        let res_2 = store.get(b"key_2");
        assert_eq!(res_2.unwrap(), b"value_2");

        let res_none = store.get(b"missing_key");
        assert_eq!(res_none, None);

        store.remove(b"key_1");
        let res_none = store.get(b"key_1");
        assert_eq!(res_none, None);

        assert_eq!(store.size(), 1);
    }

    #[test]
    fn test_compute() {
        use super::*;

        let store = DurableKeyValueStore::new_vec_based();
        assert_eq!(store.get("a".to_string().as_bytes()), None);

        store.compute("a".to_string().into_bytes(), |_| {
            bincode::serialize::<usize>(&0).expect("0 should be serialized")
        });

        let found = store.get("a".to_string().as_bytes()).unwrap();
        let cur_num: usize = bincode::deserialize(found.as_slice()).unwrap();
        assert_eq!(cur_num, 0);

        store.compute("a".to_string().into_bytes(), |value| {
            let mut cur_num: usize = bincode::deserialize(value.unwrap()).unwrap();
            cur_num += 1;
            bincode::serialize::<usize>(&cur_num).unwrap()
        });
        let found = store.get("a".to_string().as_bytes()).unwrap();
        let cur_num: usize = bincode::deserialize(found.as_slice()).unwrap();
        assert_eq!(cur_num, 1);
    }

    #[test]
    fn test_speed_vec() {
        use super::*;
        use std::time::Instant;

        let start = Instant::now();
        let store = DurableKeyValueStore::new_vec_based();

        for i in 0..10_0000 {
            let bytes = format!("{}", i).into_bytes();
            store.put(bytes.clone(), bytes);
        }

        let duration = start.elapsed();
        print!("completed in {}", duration.as_secs_f32());
    }

    #[test]
    fn test_increment() {
        use super::*;

        let store = DurableKeyValueStore::new_vec_based();
        let start = std::time::Instant::now();

        for _ in 0..100_000 {
            store.increment_or_init(b"key".to_vec(), 1).unwrap();
        }

        let cur_value = store.read_number(b"key").unwrap().unwrap();
        let elapsed = start.elapsed().as_millis();
        println!("val: {}, elapsed millis: {}", cur_value, elapsed);
    }

    #[test]
    #[ignore]
    fn test_speed_file_ssd() {
        use super::*;
        use std::time::Instant;

        let store = DurableKeyValueStore::init_new(".../sandbox/dcache_requests");
        let start = Instant::now();

        for i in 0..10_000 {
            let bytes = format!("{}", i).into_bytes();
            store.put(bytes.clone(), bytes);
        }

        let duration = start.elapsed();
        print!("completed in {}", duration.as_secs_f32());
    }
}
