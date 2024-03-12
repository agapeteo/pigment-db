use dashmap::DashMap;
use log::info;

use std::io::Write;
use std::path::Path;

use memmap::MmapOptions;
use std::fs::File;

use crate::wal::WalStorage;
use dashmap::mapref::entry::Entry;
use std::collections::HashSet;

const SET_WAL_FILE_NAME: &str = "set.wal.dat";
const TMP_SET_WAL_FILE_NAME: &str = ".set.wal.dat";

pub struct DurableKeySetStore<W: Write> {
    store: DashMap<Vec<u8>, HashSet<Vec<u8>>>,
    wal: WalStorage<W>,
}

impl DurableKeySetStore<File> {
    pub fn init_new(store_dir: &str) -> Self {
        let store_dir_path = Path::new(store_dir);
        let wal_file_path = store_dir_path.join(SET_WAL_FILE_NAME);
        let tmp_wal_file_path = store_dir_path.join(TMP_SET_WAL_FILE_NAME);

        let store = DashMap::new();
        let mut found_set_wal = wal_file_path.exists();

        if found_set_wal {
            if std::fs::metadata(&wal_file_path).unwrap().len() == 0 {
                let _ = std::fs::remove_file(&wal_file_path);
                found_set_wal = false;
            } else {
                let _ = std::fs::rename(&wal_file_path, &tmp_wal_file_path).unwrap();
            }
        }

        let wal = WalStorage::new_file_based(wal_file_path.as_path());

        if found_set_wal {
            let file = File::open(&tmp_wal_file_path).unwrap();
            info!(
                "found KeySet WAL file: {}, trying to restore...",
                &wal_file_path.to_str().unwrap()
            );

            let content_as_slice = unsafe { MmapOptions::new().map(&file).unwrap() };

            let map = crate::wal::read_for_set(content_as_slice.as_ref());
            info!(
                "restored map with size: {}, adding new new WAL file",
                map.len()
            );

            for (each_key, set) in map {
                let mut key = each_key;
                for set_val in &set {
                    let (k, _) = wal.store_append_to_set_event(key, set_val.to_owned());
                    key = k;
                }
                store.insert(key, set);
            }
            info!("{} entries added to store", store.len());

            let _ = std::fs::remove_file(tmp_wal_file_path.as_path());
            info!(
                "removed old wal file {}",
                tmp_wal_file_path.to_str().unwrap()
            );
        } else {
            info!(
                "no previous wal log found, starting from scratch: {}",
                &wal_file_path.to_str().unwrap()
            );
        }

        DurableKeySetStore { store, wal }
    }
}

impl DurableKeySetStore<Vec<u8>> {
    #[allow(unused)]
    pub fn new_vec_based() -> Self {
        DurableKeySetStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based(),
        }
    }
}

impl<W: Write> DurableKeySetStore<W> {
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
        let (key, val) = self.wal.store_append_to_set_event(key, val);

        match self.store.get_mut(&key) {
            None => {
                let mut new_hashset = HashSet::new();
                new_hashset.insert(val);
                self.store.insert(key, new_hashset);
            }
            Some(ref mut hashset) => {
                hashset.insert(val);
            }
        }
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.store.contains_key(key)
    }

    pub fn remove_from_set(&self, key: Vec<u8>, set_entry: Vec<u8>) {
        let (key, set_entry) = self.wal.store_remove_from_set_event(key, set_entry);

        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().remove(&set_entry);
                if entry.get().is_empty() {
                    self.wal.store_delete_event(entry.key());
                    entry.remove();
                }
            }
            Entry::Vacant(_) => {}
        }
    }

    pub fn compute(&self, key: Vec<u8>, func: impl FnOnce(&mut HashSet<Vec<u8>>)) {
        let entry = self.store.entry(key);
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                let set = occupied_entry.get_mut();
                func(set);
            }
            Entry::Vacant(vacant_entry) => {
                let mut set = HashSet::new();
                func(&mut set);
                vacant_entry.insert(set);
            }
        };
    }

    pub fn compute_if_present(&self, key: Vec<u8>, func: impl FnOnce(&mut HashSet<Vec<u8>>)) {
        let entry = self.store.entry(key);
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                let set = occupied_entry.get_mut();
                func(set);
            }
            Entry::Vacant(_) => {}
        };
    }

    pub fn compute_if_absent(&self, key: Vec<u8>, func: impl FnOnce(&mut HashSet<Vec<u8>>)) {
        let entry = self.store.entry(key);
        match entry {
            Entry::Occupied(_) => {}
            Entry::Vacant(vacant_entry) => {
                let mut set = HashSet::new();
                func(&mut set);
                vacant_entry.insert(set);
            }
        };
    }

    pub fn remove_from_set_callback(
        &self,
        key: Vec<u8>,
        set_entry: Vec<u8>,
        key_removed_callback: impl FnOnce(&[u8]),
    ) {
        let (key, set_entry) = self.wal.store_remove_from_set_event(key, set_entry);

        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().remove(&set_entry);
                if entry.get().is_empty() {
                    self.wal.store_delete_event(entry.key());
                    entry.remove();

                    key_removed_callback(&set_entry);
                }
            }
            Entry::Vacant(_) => {}
        }
    }

    pub fn remove_key(&self, key: &[u8]) {
        self.wal.store_delete_event(key);

        self.store.remove(key);
    }

    pub fn size(&self) -> usize {
        self.store.len()
    }
}

mod tests {

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

        assert_eq!(res_a.contains(&b"apple".to_vec()[..]), true);
        assert_eq!(res_a.contains(&b"article".to_vec()[..]), true);
        assert_eq!(res_a.contains(&b"atmosphere".to_vec()[..]), true);
        assert_eq!(res_a.contains(&b"banana".to_vec()[..]), false);

        store.remove_from_set(b"a".to_vec(), b"article".to_vec());
        let res_a = store.get_hashset(b"a").unwrap();
        assert_eq!(res_a.contains(&b"article".to_vec()[..]), false);

        let res_b = store.get_hashset(b"b").unwrap();
        assert_eq!(res_b.len(), 1);
        assert_eq!(res_b.contains(&b"banana".to_vec()[..]), true);
        assert_eq!(res_b.contains(&b"apple".to_vec()[..]), false);

        let res_c = store.get_hashset(b"c").unwrap();
        assert_eq!(res_c.len(), 2);
        assert_eq!(res_c.contains(&b"cinema".to_vec()[..]), true);
        assert_eq!(res_c.contains(&b"cinamon".to_vec()[..]), true);
        assert_eq!(res_c.contains(&b"apple".to_vec()[..]), false);

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
}
