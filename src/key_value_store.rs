use std::fs::File;
use std::io::Write;
use std::path::Path;

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
    #[cfg(test)]
    mutation_observer: MutationObserver,
}

impl DurableKeyValueStore<File> {
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
    /// A missing store is published as a complete V1 header. An existing V1
    /// store honors this explicit granularity through validated staged
    /// compaction when needed. Complete legacy input still requires the
    /// standalone migration command. Physical mode performs filesystem capability
    /// preflights and crash-safe namespace publication before returning a store.
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
        let paths = ArtifactPaths::new(store_dir.as_ref(), StoreKind::Value);
        let durability_policy = options
            .map(DurableStoreOptions::durability_policy)
            .unwrap_or_default();
        let initialized = initialize_snapshot_with_policy(
            &paths,
            replay_key_value,
            replay_key_value_tail,
            replay_key_value_against,
            encode_key_value_snapshot,
            encode_key_value_repair_snapshot,
            key_value_is_proper_snapshot_prefix,
            Some(V1CodecProbe::encode_header()),
            options.map(DurableStoreOptions::granularity_nanos),
            durability_policy,
        )?;
        let store = DashMap::new();
        for (key, value) in initialized.snapshot {
            store.insert(key, value);
        }
        Ok(RecoveryOutcome::new(
            DurableKeyValueStore {
                store,
                wal: initialized.wal,
                #[cfg(test)]
                mutation_observer: MutationObserver::default(),
            },
            initialized.status,
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
    pub(crate) fn from_probe_parts(
        initial: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        wal: WalStorage<W>,
        mutation_observer: MutationObserver,
    ) -> Self {
        Self {
            store: initial.into_iter().collect(),
            wal,
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
