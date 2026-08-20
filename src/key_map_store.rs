use dashmap::DashMap;
use log::info;

use std::io::Write;
use std::path::{Path, PathBuf};

use std::fs::File;

use crate::maintenance_coordination::OpenDirectoryLease;
use crate::model::{Key, SearchKey};
use crate::wal::format::V1CodecProbe;
use crate::wal::recovery::{
    encode_key_map_repair_snapshot, initialize_snapshot_with_policy, ArtifactPaths, StoreKind,
};
use crate::wal::replay::{
    encode_key_map_snapshot, key_map_is_proper_snapshot_prefix, replay_key_map,
    replay_key_map_against, replay_key_map_tail,
};
use crate::wal::{ComputeAction, WalStorage};
use crate::{DurableStoreOptions, RecoveryError, RecoveryOutcome, RecoveryStatus};
use dashmap::mapref::entry::Entry;
use std::collections::BTreeMap;

#[cfg(test)]
use crate::test_support::mutation_schedule::{MutationObserver, MutationPhase};

/// Mutations are ordered per logical outer key, while mutations of keys in different data-map shards remain concurrent except during shared WAL acceptance.
///
/// Different keys in the same DashMap shard may wait for one another. A
/// compute callback runs while that shard is guarded, so recursively accessing
/// the same map or shard is unsupported and may deadlock. If the callback
/// panics before acceptance, its private candidate is discarded and the guard
/// is released during unwinding. These guarantees do not change any public
/// method signature, callback shape, or compatibility panic behavior; no async
/// conflict model is introduced for this store.
pub struct DurableKeyMapStore<W: Write> {
    store: DashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>,
    wal: WalStorage<W>,
    file_backing: Option<PathBuf>,
    _open_lease: Option<OpenDirectoryLease>,
    #[cfg(test)]
    mutation_observer: MutationObserver,
}

#[allow(unused)]
impl DurableKeyMapStore<File> {
    /// Returns exact storage usage for this open key/sorted-map generation.
    ///
    /// Vector-backed stores intentionally do not expose filesystem maintenance:
    ///
    /// ```compile_fail
    /// use pigment_db::key_map_store::DurableKeyMapStore;
    /// let store = DurableKeyMapStore::new_vec_based();
    /// let _ = store.storage_stats();
    /// ```
    pub fn storage_stats(&self) -> Result<crate::FamilyStorageStats, crate::CompactionError> {
        crate::maintenance::public_file_family_storage_stats(
            self.file_backing
                .as_deref()
                .expect("file-backed store retains its directory identity"),
            crate::compaction::inspection::InspectedFamily::KeyMap,
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
            crate::compaction::inspection::InspectedFamily::KeyMap,
        )
    }

    #[cfg(test)]
    pub(crate) fn storage_stats_probe(
        &self,
    ) -> std::io::Result<crate::compaction::inspection::FamilyInspection> {
        self.storage_stats_internal()
    }

    /// Opens a file-backed key/sorted-map store and returns structured recovery
    /// status or error information without panicking for expected failures.
    pub fn try_init_new(
        store_dir: impl AsRef<Path>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError> {
        Self::try_init_new_configured(store_dir, None)
    }

    /// Opens a file-backed key/sorted-map store with explicit timestamp and durability options.
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
        let paths = ArtifactPaths::new(store_dir, StoreKind::Map);
        let durability_policy = options
            .map(DurableStoreOptions::durability_policy)
            .unwrap_or_default();
        let wal_segment_size = options.unwrap_or_default().wal_segment_size().as_bytes();
        let initialized = initialize_snapshot_with_policy(
            &paths,
            replay_key_map,
            replay_key_map_tail,
            replay_key_map_against,
            encode_key_map_snapshot,
            encode_key_map_repair_snapshot,
            key_map_is_proper_snapshot_prefix,
            Some(V1CodecProbe::encode_header_with_kind(3)),
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
        Ok(RecoveryOutcome::new(
            DurableKeyMapStore {
                store,
                wal: initialized.wal,
                file_backing: Some(file_backing),
                _open_lease: Some(open_lease),
                #[cfg(test)]
                mutation_observer: MutationObserver::default(),
            },
            initialized.status,
        ))
    }

    /// Opens a file-backed key/sorted-map store with the historical panic-on-error API.
    ///
    /// This compatibility wrapper delegates to [`Self::try_init_new`] and logs
    /// successful automatic recovery.
    pub fn init_new(store_dir: &str) -> Self {
        let outcome = Self::try_init_new(store_dir).unwrap_or_else(|error| panic!("{error}"));
        let (store, status) = outcome.into_parts();
        if status == RecoveryStatus::Recovered {
            info!("pigment-db recovered key/sorted-map WAL in {store_dir}");
        }
        store
    }
}

impl DurableKeyMapStore<Vec<u8>> {
    #[allow(unused)]
    pub fn new_vec_based() -> Self {
        DurableKeyMapStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based(),
            file_backing: None,
            _open_lease: None,
            #[cfg(test)]
            mutation_observer: MutationObserver::default(),
        }
    }

    /// Creates a vector-backed key/sorted-map store using V1 timestamp configuration.
    ///
    /// This compatibility wrapper panics if physical durability is requested.
    pub fn new_vec_based_with_options(options: DurableStoreOptions) -> Self {
        Self::try_new_vec_based_with_options(options)
            .unwrap_or_else(|error| panic!("vector-backed key/map construction failed: {error}"))
    }

    /// Tries to create a vector-backed key/sorted-map store with explicit options.
    /// Physical durability returns [`crate::DurabilitySupportError::NoPhysicalBacking`].
    pub fn try_new_vec_based_with_options(
        options: DurableStoreOptions,
    ) -> Result<Self, crate::DurabilitySupportError> {
        crate::durability::validate_memory_backing(options.durability_policy())?;
        let header =
            V1CodecProbe::encode_header_with_kind_and_granularity(3, options.granularity_nanos());
        Ok(DurableKeyMapStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based_v1(&header),
            file_backing: None,
            _open_lease: None,
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

#[allow(unused)]
impl<W: Write> DurableKeyMapStore<W> {
    #[cfg(test)]
    pub(crate) fn from_probe_parts(
        initial: impl IntoIterator<Item = (Vec<u8>, BTreeMap<SearchKey, Vec<u8>>)>,
        wal: WalStorage<W>,
        mutation_observer: MutationObserver,
    ) -> Self {
        Self {
            store: initial.into_iter().collect(),
            file_backing: None,
            _open_lease: None,
            wal,
            mutation_observer,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_put_probe(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        value: Vec<u8>,
    ) -> std::io::Result<()> {
        self.try_put_core(key, search_key, value)
    }

    #[cfg(test)]
    pub(crate) fn try_pop_first_probe(
        &self,
        key: Vec<u8>,
    ) -> std::io::Result<Option<(SearchKey, Vec<u8>)>> {
        self.try_pop_first_core(key)
    }

    pub fn get_sorted_map(&self, key: &[u8]) -> Option<BTreeMap<SearchKey, Vec<u8>>> {
        match self.store.get(key) {
            None => None,
            Some(inner_val) => {
                let found = inner_val.value();
                let mut map = BTreeMap::new();
                for (k, v) in found {
                    map.insert(k.clone(), v.clone());
                }
                Some(map)
            }
        }
    }

    pub fn get_element(&self, key: &[u8], search_key: &SearchKey) -> Option<Vec<u8>> {
        match self.store.get(key) {
            None => None,
            Some(inner_val) => inner_val.value().get(search_key).cloned(),
        }
    }

    pub fn contains_in_map(&self, key: &[u8], search_key: &SearchKey) -> bool {
        match self.store.get(key) {
            None => false,
            Some(inner_val) => inner_val.value().contains_key(search_key),
        }
    }

    pub fn put(&self, key: Vec<u8>, search_key: SearchKey, val: Vec<u8>) {
        self.try_put(key, search_key, val)
            .unwrap_or_else(|error| panic!("WAL map put rejected: {error}"));
    }

    /// Persists and then publishes one sorted-map entry.
    pub fn try_put(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        val: Vec<u8>,
    ) -> std::io::Result<()> {
        self.try_put_core(key, search_key, val)
    }

    pub(crate) fn try_put_core(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        val: Vec<u8>,
    ) -> std::io::Result<()> {
        if let Some(mut entry) = self.store.get_mut(&key) {
            #[cfg(test)]
            self.mutation_observer
                .notify(entry.key(), MutationPhase::AcceptanceEntered);
            let (_key, search_key, val) =
                self.wal.try_store_put_to_map_event(key, search_key, val)?;
            #[cfg(test)]
            self.mutation_observer
                .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
            entry.insert(search_key, val);
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
                let (_key, search_key, val) =
                    self.wal
                        .try_store_put_to_map_event(entry.key().clone(), search_key, val)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                entry.get_mut().insert(search_key, val);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let (_key, search_key, val) =
                    self.wal
                        .try_store_put_to_map_event(entry.key().clone(), search_key, val)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                #[cfg(test)]
                let published_key = entry.key().clone();
                let mut new_sorted_map = BTreeMap::new();
                new_sorted_map.insert(search_key, val);
                entry.insert(new_sorted_map);
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

    pub fn contains_search_key(&self, key: &[u8], search_key: &SearchKey) -> bool {
        if let Some(entry) = self.store.get(key) {
            if entry.value().contains_key(search_key) {
                return true;
            }
        }
        false
    }

    pub fn remove_from_sorted_map(&self, key: Vec<u8>, search_key: SearchKey) -> Option<Vec<u8>> {
        self.try_remove_from_sorted_map(key, search_key)
            .unwrap_or_else(|error| panic!("WAL map removal rejected: {error}"))
    }

    /// Persists one entry removal and returns the value only after publication.
    pub fn try_remove_from_sorted_map(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
    ) -> std::io::Result<Option<Vec<u8>>> {
        self.try_remove_from_sorted_map_core(key, search_key)
    }

    pub(crate) fn try_remove_from_sorted_map_core(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
    ) -> std::io::Result<Option<Vec<u8>>> {
        if let Some(mut entry) = self.store.get_mut(&key) {
            let removes_final_entry = entry.len() == 1 && entry.contains_key(&search_key);
            if !removes_final_entry {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                self.wal
                    .try_store_remove_from_sorted_map_event(key, search_key.clone())?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                let old_value = entry.remove(&search_key);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
                return Ok(old_value);
            }
        }
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let old_value = entry.get().get(&search_key).cloned();
                let removes_final_entry = old_value.is_some() && entry.get().len() == 1;
                if removes_final_entry {
                    self.wal.try_store_delete_event(entry.key())?;
                } else {
                    self.wal.try_store_remove_from_sorted_map_event(
                        entry.key().clone(),
                        search_key.clone(),
                    )?;
                }
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                if removes_final_entry {
                    #[cfg(test)]
                    let published_key = entry.key().clone();
                    entry.remove();
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(&published_key, MutationPhase::Published);
                } else {
                    entry.get_mut().remove(&search_key);
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::Published);
                }
                Ok(old_value)
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                self.wal
                    .try_store_remove_from_sorted_map_event(entry.key().clone(), search_key)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
                Ok(None)
            }
        }
    }

    pub fn remove_from_sorted_map_callback(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        key_removed_callback: impl FnOnce(&SearchKey),
    ) {
        self.try_remove_from_sorted_map_callback(key, search_key, key_removed_callback)
            .unwrap_or_else(|error| panic!("WAL map callback removal rejected: {error}"));
    }

    /// Removes an entry and calls `key_removed_callback` only after an accepted
    /// final-entry deletion is published.
    pub fn try_remove_from_sorted_map_callback(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        key_removed_callback: impl FnOnce(&SearchKey),
    ) -> std::io::Result<()> {
        self.try_remove_from_sorted_map_callback_core(key, search_key, key_removed_callback)
    }

    pub(crate) fn try_remove_from_sorted_map_callback_core(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        key_removed_callback: impl FnOnce(&SearchKey),
    ) -> std::io::Result<()> {
        #[cfg(test)]
        let observed_key = key.clone();
        let removed_outer_key = match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let removes_final_entry =
                    entry.get().len() == 1 && entry.get().contains_key(&search_key);
                if removes_final_entry {
                    self.wal.try_store_delete_event(entry.key())?;
                } else {
                    self.wal.try_store_remove_from_sorted_map_event(
                        entry.key().clone(),
                        search_key.clone(),
                    )?;
                }
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                if removes_final_entry {
                    entry.remove();
                } else {
                    entry.get_mut().remove(&search_key);
                }
                removes_final_entry
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                self.wal.try_store_remove_from_sorted_map_event(
                    entry.key().clone(),
                    search_key.clone(),
                )?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                false
            }
        };
        #[cfg(test)]
        self.mutation_observer
            .notify(&observed_key, MutationPhase::Published);
        if removed_outer_key {
            key_removed_callback(&search_key);
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
            }
        }
        #[cfg(test)]
        self.mutation_observer.notify(key, MutationPhase::Published);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.store.len()
    }

    pub fn sorted_map_size(&self, key: &[u8]) -> Option<usize> {
        self.store.get(key).map(|v| v.value().len())
    }

    pub fn range_search_keys_filtered<P>(
        &self,
        key: &[u8],
        bound_start: std::ops::Bound<SearchKey>,
        bound_end: std::ops::Bound<SearchKey>,
        predicate: P,
    ) -> Option<Vec<SearchKey>>
    where
        P: FnMut(&SearchKey) -> bool,
    {
        self.store.get(key).map(|v| {
            v.value()
                .range((bound_start, bound_end))
                .map(|(k, v)| k.clone())
                .filter(predicate)
                .collect()
        })
    }

    pub fn range_search_keys(
        &self,
        key: &[u8],
        bound_start: std::ops::Bound<SearchKey>,
        bound_end: std::ops::Bound<SearchKey>,
    ) -> Option<Vec<SearchKey>> {
        self.store.get(key).map(|v| {
            v.value()
                .range((bound_start, bound_end))
                .map(|(k, v)| k.clone())
                .collect()
        })
    }

    pub fn range_entries(
        &self,
        key: &[u8],
        bound_start: std::ops::Bound<SearchKey>,
        bound_end: std::ops::Bound<SearchKey>,
    ) -> Option<Vec<(SearchKey, Vec<u8>)>> {
        self.store.get(key).map(|v| {
            v.value()
                .range((bound_start, bound_end))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
    }

    pub fn range_entries_filtered<P>(
        &self,
        key: &[u8],
        bound_start: std::ops::Bound<SearchKey>,
        bound_end: std::ops::Bound<SearchKey>,
        predicate: P,
    ) -> Option<Vec<(SearchKey, Vec<u8>)>>
    where
        P: FnMut(&(SearchKey, Vec<u8>)) -> bool,
    {
        self.store.get(key).map(|v| {
            v.value()
                .range((bound_start, bound_end))
                .map(|(k, v)| (k.clone(), v.clone()))
                .filter(predicate)
                .collect()
        })
    }

    pub fn first(&self, key: &[u8]) -> Option<(SearchKey, Vec<u8>)> {
        match self.store.get(key) {
            Some(found) => {
                if let Some((k, v)) = found.value().first_key_value() {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn last(&self, key: &[u8]) -> Option<(SearchKey, Vec<u8>)> {
        match self.store.get(key) {
            Some(found) => {
                if let Some((k, v)) = found.value().last_key_value() {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn pop_first(&self, key: Vec<u8>) -> Option<(SearchKey, Vec<u8>)> {
        self.try_pop_first(key)
            .unwrap_or_else(|error| panic!("WAL map pop-first rejected: {error}"))
    }

    /// Persists removal of the first ordered entry, then returns its search key and value.
    pub fn try_pop_first(&self, key: Vec<u8>) -> std::io::Result<Option<(SearchKey, Vec<u8>)>> {
        self.try_pop_first_core(key)
    }

    pub(crate) fn try_pop_first_core(
        &self,
        key: Vec<u8>,
    ) -> std::io::Result<Option<(SearchKey, Vec<u8>)>> {
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let Some(search_key) = entry.get().first_key_value().map(|(key, _)| key.clone())
                else {
                    return Ok(None);
                };
                let removes_final_entry = entry.get().len() == 1;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                if removes_final_entry {
                    self.wal.try_store_delete_event(entry.key())?;
                } else {
                    self.wal.try_store_remove_from_sorted_map_event(
                        entry.key().clone(),
                        search_key.clone(),
                    )?;
                }
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                let removed_value = if removes_final_entry {
                    #[cfg(test)]
                    let published_key = entry.key().clone();
                    let mut removed_map = entry.remove();
                    let removed_value = removed_map
                        .remove(&search_key)
                        .expect("selected first entry must remain guarded until publication");
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(&published_key, MutationPhase::Published);
                    removed_value
                } else {
                    let removed_value = entry
                        .get_mut()
                        .remove(&search_key)
                        .expect("selected first entry must remain guarded until publication");
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::Published);
                    removed_value
                };
                Ok(Some((search_key, removed_value)))
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn pop_last(&self, key: Vec<u8>) -> Option<(SearchKey, Vec<u8>)> {
        self.try_pop_last(key)
            .unwrap_or_else(|error| panic!("WAL map pop-last rejected: {error}"))
    }

    /// Persists removal of the last ordered entry, then returns its search key and value.
    pub fn try_pop_last(&self, key: Vec<u8>) -> std::io::Result<Option<(SearchKey, Vec<u8>)>> {
        self.try_pop_last_core(key)
    }

    pub(crate) fn try_pop_last_core(
        &self,
        key: Vec<u8>,
    ) -> std::io::Result<Option<(SearchKey, Vec<u8>)>> {
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let Some(search_key) = entry.get().last_key_value().map(|(key, _)| key.clone())
                else {
                    return Ok(None);
                };
                let removes_final_entry = entry.get().len() == 1;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                if removes_final_entry {
                    self.wal.try_store_delete_event(entry.key())?;
                } else {
                    self.wal.try_store_remove_from_sorted_map_event(
                        entry.key().clone(),
                        search_key.clone(),
                    )?;
                }
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                let removed_value = if removes_final_entry {
                    #[cfg(test)]
                    let published_key = entry.key().clone();
                    let mut removed_map = entry.remove();
                    let removed_value = removed_map
                        .remove(&search_key)
                        .expect("selected last entry must remain guarded until publication");
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(&published_key, MutationPhase::Published);
                    removed_value
                } else {
                    let removed_value = entry
                        .get_mut()
                        .remove(&search_key)
                        .expect("selected last entry must remain guarded until publication");
                    #[cfg(test)]
                    self.mutation_observer
                        .notify(entry.key(), MutationPhase::Published);
                    removed_value
                };
                Ok(Some((search_key, removed_value)))
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn append_ordered_element(&self, key: Vec<u8>, element: Vec<u8>) {
        self.try_append_ordered_element(key, element)
            .unwrap_or_else(|error| panic!("WAL map append rejected: {error}"));
    }

    /// Persists and publishes an entry under the next numeric search key.
    ///
    /// Keys whose first component is [`Key::USIZE`] participate in the numeric
    /// sequence, including composite keys. Empty and nonnumeric keys are
    /// ignored. Exhausting the `usize` keyspace returns
    /// [`std::io::ErrorKind::InvalidInput`] without writing or publishing.
    pub fn try_append_ordered_element(
        &self,
        key: Vec<u8>,
        element: Vec<u8>,
    ) -> std::io::Result<()> {
        self.try_append_ordered_element_core(key, element)
    }

    pub(crate) fn try_append_ordered_element_core(
        &self,
        key: Vec<u8>,
        element: Vec<u8>,
    ) -> std::io::Result<()> {
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let cur_num = {
                    let numeric_start = SearchKey::from(vec![Key::USIZE(0)]);
                    let numeric_end = SearchKey::from(vec![Key::I128(i128::MIN)]);
                    if let Some((last_numeric_key, _)) =
                        entry.get().range(numeric_start..numeric_end).next_back()
                    {
                        match last_numeric_key.first() {
                            Some(Key::USIZE(count)) => count.checked_add(1).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "ordered map numeric search-key space is exhausted",
                                )
                            })?,
                            _ => unreachable!("numeric search-key range must start with USIZE"),
                        }
                    } else {
                        0
                    }
                };
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let (_key, search_key, element) = self.wal.try_store_put_to_map_event(
                    entry.key().clone(),
                    cur_num.into(),
                    element,
                )?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                entry.get_mut().insert(search_key, element);
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::Published);
            }
            Entry::Vacant(entry) => {
                let mut map: BTreeMap<SearchKey, Vec<u8>> = BTreeMap::new();
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptanceEntered);
                let (_key, search_key, element) =
                    self.wal
                        .try_store_put_to_map_event(entry.key().clone(), 0.into(), element)?;
                #[cfg(test)]
                self.mutation_observer
                    .notify(entry.key(), MutationPhase::AcceptedBeforePublication);
                map.insert(search_key, element);
                #[cfg(test)]
                let published_key = entry.key().clone();
                entry.insert(map);
                #[cfg(test)]
                self.mutation_observer
                    .notify(&published_key, MutationPhase::Published);
            }
        }
        Ok(())
    }

    /// Computes a replacement ordered map on an owned working copy and invokes `func` once.
    ///
    /// Changed puts are persisted before removals in one rollback-capable WAL batch, then the
    /// result is published. Empty results delete the outer key and exact no-ops write nothing.
    /// Persistence or rollback errors leave the original live map in place. If rollback itself
    /// fails, live state remains unpublished but artifact repair is outside this API's scope. The
    /// per-key DashMap entry guard is held for the operation; stronger cross-key synchronization
    /// is not provided.
    pub fn try_compute(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) -> std::io::Result<()> {
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
                        .commit_map_compute_batch(vec![ComputeAction::Delete {
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
                let mut actions = Vec::new();
                actions.extend(
                    working
                        .iter()
                        .filter(|(search_key, value)| original.get(search_key) != Some(*value))
                        .map(|(search_key, value)| ComputeAction::MapPut {
                            key: key.clone(),
                            search_key: search_key.clone(),
                            value: value.clone(),
                        }),
                );
                actions.extend(
                    original
                        .keys()
                        .filter(|search_key| !working.contains_key(*search_key))
                        .map(|search_key| ComputeAction::MapRemove {
                            key: key.clone(),
                            search_key: search_key.clone(),
                        }),
                );
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::AcceptanceEntered);
                self.wal.commit_map_compute_batch(actions)?;
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
                let mut working = BTreeMap::new();
                func(&mut working);
                if working.is_empty() {
                    return Ok(());
                }
                #[cfg(test)]
                self.mutation_observer
                    .notify(&key, MutationPhase::AcceptanceEntered);
                self.wal.commit_map_compute_batch(
                    working
                        .iter()
                        .map(|(search_key, value)| ComputeAction::MapPut {
                            key: key.clone(),
                            search_key: search_key.clone(),
                            value: value.clone(),
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
    pub fn compute(&self, key: Vec<u8>, func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>)) {
        self.try_compute(key, func)
            .unwrap_or_else(|error| panic!("map compute persistence failed: {error}"));
    }

    /// Computes only for a present outer key; skipped calls return `Ok(())` without a callback.
    ///
    /// Eligible callbacks run once. Empty/no-op and persistence-error behavior matches
    /// [`Self::try_compute`], including no live publication when commit or rollback fails.
    pub fn try_compute_if_present(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) -> std::io::Result<()> {
        match self.store.entry(key.clone()) {
            Entry::Occupied(mut occupied_entry) => {
                let original = occupied_entry.get().clone();
                let mut working = original.clone();
                func(&mut working);
                if working.is_empty() {
                    self.wal
                        .commit_map_compute_batch(vec![ComputeAction::Delete {
                            key: key.clone(),
                        }])?;
                    occupied_entry.remove();
                    return Ok(());
                }
                if working == original {
                    return Ok(());
                }
                let mut actions = Vec::new();
                actions.extend(
                    working
                        .iter()
                        .filter(|(search_key, value)| original.get(search_key) != Some(*value))
                        .map(|(search_key, value)| ComputeAction::MapPut {
                            key: key.clone(),
                            search_key: search_key.clone(),
                            value: value.clone(),
                        }),
                );
                actions.extend(
                    original
                        .keys()
                        .filter(|search_key| !working.contains_key(*search_key))
                        .map(|search_key| ComputeAction::MapRemove {
                            key: key.clone(),
                            search_key: search_key.clone(),
                        }),
                );
                self.wal.commit_map_compute_batch(actions)?;
                *occupied_entry.get_mut() = working;
                Ok(())
            }
            Entry::Vacant(_) => Ok(()),
        }
    }

    /// Compatibility wrapper for [`Self::try_compute_if_present`] that panics on error.
    pub fn compute_if_present(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) {
        self.try_compute_if_present(key, func)
            .unwrap_or_else(|error| panic!("map compute-if-present persistence failed: {error}"));
    }

    /// Computes only for an absent outer key; skipped calls return `Ok(())` without a callback.
    ///
    /// An eligible callback runs once. Empty results create no key or WAL frame, while non-empty
    /// results are persisted before publication; commit/rollback errors publish nothing.
    pub fn try_compute_if_absent(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) -> std::io::Result<()> {
        match self.store.entry(key.clone()) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(vacant_entry) => {
                let mut working = BTreeMap::new();
                func(&mut working);
                if working.is_empty() {
                    return Ok(());
                }
                self.wal.commit_map_compute_batch(
                    working
                        .iter()
                        .map(|(search_key, value)| ComputeAction::MapPut {
                            key: key.clone(),
                            search_key: search_key.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                )?;
                vacant_entry.insert(working);
                Ok(())
            }
        }
    }

    /// Compatibility wrapper for [`Self::try_compute_if_absent`] that panics on error.
    pub fn compute_if_absent(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) {
        self.try_compute_if_absent(key, func)
            .unwrap_or_else(|error| panic!("map compute-if-absent persistence failed: {error}"));
    }
}

#[cfg(test)]
#[path = "mutation_ordering_tests/key_map.rs"]
mod mutation_ordering_tests;

#[cfg(test)]
mod tests {
    use crate::model::SearchKey;
    use crate::wal::WalStorage;
    use dashmap::DashMap;
    use std::collections::BTreeMap;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use super::{DurableKeyMapStore, MutationObserver};

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

    fn fault_store() -> (DurableKeyMapStore<FaultWriter>, Arc<Mutex<FaultState>>) {
        let writer = FaultWriter::default();
        let state = Arc::clone(&writer.0);
        let store = DurableKeyMapStore {
            store: DashMap::new(),
            wal: WalStorage::new_with_rollback(writer, rollback),
            file_backing: None,
            _open_lease: None,
            mutation_observer: MutationObserver::default(),
        };
        (store, state)
    }

    #[test]
    fn simple_test() {
        use super::*;

        let store = DurableKeyMapStore::new_vec_based();

        let key_1 = "key_1".as_bytes().to_vec();
        store.put(key_1.clone(), 3.into(), "c".as_bytes().to_vec());
        store.put(key_1.clone(), 1.into(), "a".as_bytes().to_vec());
        store.put(key_1.clone(), 2.into(), "b".as_bytes().to_vec());
        store.put(key_1.clone(), 3.into(), "c_".as_bytes().to_vec());

        let key_2 = "key_2".as_bytes().to_vec();
        store.put(key_2.clone(), 3.into(), "C".as_bytes().to_vec());
        store.put(key_2.clone(), 1.into(), "A".as_bytes().to_vec());
        store.put(key_2.clone(), 2.into(), "B".as_bytes().to_vec());

        assert_eq!(
            store.get_element(&key_1, &2.into()),
            Some("b".as_bytes().to_vec())
        );
        assert_eq!(
            store.get_element(&key_1, &3.into()),
            Some("c_".as_bytes().to_vec())
        );
        assert_eq!(
            store.get_element(&key_1, &1.into()),
            Some("a".as_bytes().to_vec())
        );

        assert_eq!(
            store.get_element(&key_2, &2.into()),
            Some("B".as_bytes().to_vec())
        );
        assert_eq!(
            store.get_element(&key_2, &3.into()),
            Some("C".as_bytes().to_vec())
        );
        assert_eq!(
            store.get_element(&key_2, &1.into()),
            Some("A".as_bytes().to_vec())
        );

        store.remove_from_sorted_map(key_1.clone(), 1.into());
        assert_eq!(store.get_element(&key_1, &1.into()), None);
    }

    // #[test]
    // fn test_store() {
    //     use super::*;
    //     let store = DurableKeyMapStore::init_new("/Users/emix/sandbox/stored_map_test/");
    //
    //     let key_1 = "key_1".as_bytes().to_vec();
    //     // store.put(key_1.clone(), 3.into(), "c".as_bytes().to_vec());
    //     // store.put(key_1.clone(), 1.into(), "a".as_bytes().to_vec());
    //     // store.put(key_1.clone(), 2.into(), "b".as_bytes().to_vec());
    //     // store.put(key_1.clone(), 3.into(), "c_".as_bytes().to_vec());
    //     //
    //     let key_2 = "key_2".as_bytes().to_vec();
    //     // store.put(key_2.clone(), 3.into(), "C".as_bytes().to_vec());
    //     // store.put(key_2.clone(), 1.into(), "A".as_bytes().to_vec());
    //     // store.put(key_2.clone(), 2.into(), "B".as_bytes().to_vec());
    //     //
    //     // store.remove_from_sorted_map(key_1.clone(), 1.into());
    //
    //
    //     assert_eq!(store.get_element(&key_1, &2.into()), Some("b".as_bytes().to_vec()));
    //     assert_eq!(store.get_element(&key_1, &3.into()), Some("c_".as_bytes().to_vec()));
    //     assert_eq!(store.get_element(&key_1, &1.into()), None);
    //
    //     assert_eq!(store.get_element(&key_2, &2.into()), Some("B".as_bytes().to_vec()));
    //     assert_eq!(store.get_element(&key_2, &3.into()), Some("C".as_bytes().to_vec()));
    //     assert_eq!(store.get_element(&key_2, &1.into()), Some("A".as_bytes().to_vec()));
    // }

    #[test]
    fn test_range() {
        let mut map: BTreeMap<SearchKey, &'static str> = BTreeMap::new();

        map.insert(1.into(), "a");
        map.insert(2.into(), "b");
        map.insert(3.into(), "c");
        map.insert(4.into(), "d");
        map.insert(5.into(), "e");
        map.insert(6.into(), "f");
        map.insert(7.into(), "g");

        let start: SearchKey = 2.into();
        let end: SearchKey = 5.into();

        for (key, str) in map.range(start..end) {
            println!("{:?} -> {}", key, str);
        }
    }

    #[test]
    fn test_ordered() {
        let store = DurableKeyMapStore::new_vec_based();
        let key: Vec<u8> = vec![0];

        (0..10).for_each(|i| {
            store.append_ordered_element(key.clone(), format!("{}", i).into_bytes());
        });

        let map = store.get_sorted_map(&key).unwrap();

        for (k, v) in map {
            println!("{:?} -> {}", k, String::from_utf8_lossy(v.as_slice()));
        }
    }

    #[test]
    fn partial_write_rejection_keeps_map_live_and_replay_state() {
        let (store, state) = fault_store();
        store.put(b"key".to_vec(), 1.into(), b"original".to_vec());
        let prefix = state.lock().unwrap().bytes.clone();
        state.lock().unwrap().fail_after = Some(5);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        assert!(store
            .try_compute(b"key".to_vec(), |map| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                map.insert(2.into(), b"rejected".to_vec());
            })
            .is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            store
                .get_sorted_map(b"key")
                .unwrap()
                .get(&SearchKey::from(1)),
            Some(&b"original".to_vec())
        );
        assert_eq!(state.lock().unwrap().bytes, prefix);
        assert_eq!(
            crate::wal::read_for_map(&prefix)
                .get(b"key".as_slice())
                .unwrap()
                .get(&SearchKey::from(1)),
            Some(&b"original".to_vec())
        );
    }

    #[test]
    fn flush_rejection_errors_or_panics_without_map_publication() {
        for compatibility in [false, true] {
            let (store, state) = fault_store();
            store.put(b"key".to_vec(), 1.into(), b"original".to_vec());
            let prefix = state.lock().unwrap().bytes.clone();
            state.lock().unwrap().fail_flush = true;
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if compatibility {
                    store.compute(b"key".to_vec(), |map| {
                        map.insert(2.into(), b"rejected".to_vec());
                    });
                    Ok(())
                } else {
                    store.try_compute(b"key".to_vec(), |map| {
                        map.insert(2.into(), b"rejected".to_vec());
                    })
                }
            }));
            assert_eq!(outcome.is_err(), compatibility);
            if !compatibility {
                assert!(outcome.unwrap().is_err());
            }
            assert_eq!(store.get_sorted_map(b"key").unwrap().len(), 1);
            assert_eq!(state.lock().unwrap().bytes, prefix);
            assert_eq!(
                crate::wal::read_for_map(&prefix)
                    .get(b"key".as_slice())
                    .unwrap()
                    .get(&SearchKey::from(1)),
                Some(&b"original".to_vec())
            );
        }
    }

    #[test]
    fn conditional_rejection_returns_errors_without_map_publication() {
        for present in [true, false] {
            let (store, state) = fault_store();
            if present {
                store.put(b"key".to_vec(), 1.into(), b"original".to_vec());
            }
            state.lock().unwrap().fail_flush = true;
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let result = if present {
                store.try_compute_if_present(b"key".to_vec(), |map| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    map.insert(2.into(), b"rejected".to_vec());
                })
            } else {
                store.try_compute_if_absent(b"key".to_vec(), |map| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    map.insert(2.into(), b"rejected".to_vec());
                })
            };
            assert!(result.is_err());
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(store.contains_key(b"key"), present);
            if present {
                assert_eq!(store.get_sorted_map(b"key").unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn conditional_compatibility_rejection_panics_without_map_publication() {
        for present in [true, false] {
            let (store, state) = fault_store();
            if present {
                store.put(b"key".to_vec(), 1.into(), b"original".to_vec());
            }
            state.lock().unwrap().fail_flush = true;
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if present {
                    store.compute_if_present(b"key".to_vec(), |map| {
                        map.insert(2.into(), b"rejected".to_vec());
                    });
                } else {
                    store.compute_if_absent(b"key".to_vec(), |map| {
                        map.insert(2.into(), b"rejected".to_vec());
                    });
                }
            }));
            assert!(outcome.is_err());
            assert_eq!(store.contains_key(b"key"), present);
            if present {
                assert_eq!(store.get_sorted_map(b"key").unwrap().len(), 1);
            }
        }
    }
}
