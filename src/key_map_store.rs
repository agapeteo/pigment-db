use dashmap::DashMap;
use log::info;

use std::io::Write;
use std::path::Path;

use memmap::MmapOptions;
use std::fs::File;

use crate::model::{Key, SearchKey};
use crate::wal::WalStorage;
use dashmap::mapref::entry::Entry;
use std::collections::BTreeMap;

const MAP_WAL_FILE_NAME: &str = "map.wal.dat";
const TMP_MAP_WAL_FILE_NAME: &str = ".map.wal.dat";

pub struct DurableKeyMapStore<W: Write> {
    store: DashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>,
    wal: WalStorage<W>,
}

#[allow(unused)]
impl DurableKeyMapStore<File> {
    pub fn init_new(store_dir: &str) -> Self {
        let store_dir_path = Path::new(store_dir);
        let wal_file_path = store_dir_path.join(MAP_WAL_FILE_NAME);
        let tmp_wal_file_path = store_dir_path.join(TMP_MAP_WAL_FILE_NAME);

        let store: DashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>> = DashMap::new();
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

            let map = crate::wal::read_for_map(content_as_slice.as_ref());
            info!(
                "restored map with size: {}, adding new new WAL file",
                map.len()
            );

            for (each_key, entry_map) in map {
                for (search_key, element) in entry_map {
                    let (key, search_key, element) =
                        wal.store_put_to_map_event(each_key.clone(), search_key, element);
                    match store.entry(each_key.clone()) {
                        Entry::Occupied(mut entry) => {
                            let found_map: &mut BTreeMap<SearchKey, Vec<u8>> = entry.get_mut();
                            found_map.insert(search_key, element);
                        }
                        Entry::Vacant(vacant) => {
                            let mut new_map = BTreeMap::new();
                            new_map.insert(search_key, element);
                            vacant.insert(new_map);
                        }
                    }
                }
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

        DurableKeyMapStore { store, wal }
    }
}

impl DurableKeyMapStore<Vec<u8>> {
    #[allow(unused)]
    pub fn new_vec_based() -> Self {
        DurableKeyMapStore {
            store: DashMap::new(),
            wal: WalStorage::new_vec_based(),
        }
    }
}

#[allow(unused)]
impl<W: Write> DurableKeyMapStore<W> {
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
        let (key, search_key, val) = self.wal.store_put_to_map_event(key, search_key, val);

        match self.store.get_mut(&key) {
            None => {
                let mut new_sorted_map = BTreeMap::new();
                new_sorted_map.insert(search_key, val);
                self.store.insert(key, new_sorted_map);
            }
            Some(ref mut sorted_map) => {
                sorted_map.insert(search_key, val);
            }
        }
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
        let (key, search_key) = self.wal.store_remove_from_sorted_map_event(key, search_key);

        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let old_value = entry.get_mut().remove(&search_key);
                if entry.get().is_empty() {
                    self.wal.store_delete_event(entry.key());
                    entry.remove();
                }
                old_value
            }
            Entry::Vacant(_) => None,
        }
    }

    pub fn remove_from_sorted_map_callback(
        &self,
        key: Vec<u8>,
        search_key: SearchKey,
        key_removed_callback: impl FnOnce(&SearchKey),
    ) {
        let (key, search_key) = self.wal.store_remove_from_sorted_map_event(key, search_key);

        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().remove(&search_key);
                if entry.get().is_empty() {
                    self.wal.store_delete_event(entry.key());
                    entry.remove();

                    key_removed_callback(&search_key);
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

    pub fn sorted_map_size(&self, key: &[u8]) -> Option<usize> {
        self.store.get(key).map(|v| v.value().len())
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
        match self.store.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let result = if let Some((search_key, _element)) = entry.get_mut().pop_first() {
                    let (element, search_key) =
                        self.wal.store_remove_from_sorted_map_event(key, search_key);
                    Some((search_key, element))
                } else {
                    None
                };
                if entry.get().is_empty() {
                    self.wal.store_delete_event(entry.key());
                    entry.remove();
                }
                result
            }
            Entry::Vacant(_) => None,
        }
    }

    pub fn pop_last(&self, key: Vec<u8>) -> Option<(SearchKey, Vec<u8>)> {
        match self.store.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let result = if let Some((search_key, _element)) = entry.get_mut().pop_last() {
                    let (element, search_key) =
                        self.wal.store_remove_from_sorted_map_event(key, search_key);
                    Some((search_key, element))
                } else {
                    None
                };
                if entry.get().is_empty() {
                    self.wal.store_delete_event(entry.key());
                    entry.remove();
                }
                result
            }
            Entry::Vacant(_) => None,
        }
    }

    pub fn append_ordered_element(&self, key: Vec<u8>, element: Vec<u8>) {
        match self.store.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let map = entry.get_mut();
                let cur_num = {
                    if let Some(last_entry) = map.last_entry() {
                        let last_search_key = last_entry.key().first().unwrap();
                        if let Key::USIZE(count) = last_search_key {
                            count + 1
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                };
                let (_key, search_key, element) =
                    self.wal
                        .store_put_to_map_event(key, cur_num.into(), element);
                map.insert(search_key, element);
            }
            Entry::Vacant(entry) => {
                let mut map: BTreeMap<SearchKey, Vec<u8>> = BTreeMap::new();
                let (_key, search_key, element) =
                    self.wal.store_put_to_map_event(key, 0.into(), element);
                map.insert(search_key, element);
                entry.insert(map);
            }
        }
    }

    pub fn compute(&self, key: Vec<u8>, func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>)) {
        let entry = self.store.entry(key);
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                let map = occupied_entry.get_mut();
                func(map);
            }
            Entry::Vacant(vacant_entry) => {
                let mut map = BTreeMap::new();
                func(&mut map);
                vacant_entry.insert(map);
            }
        };
    }

    pub fn compute_if_present(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) {
        let entry = self.store.entry(key);
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                let map = occupied_entry.get_mut();
                func(map);
            }
            Entry::Vacant(_) => {}
        };
    }

    pub fn compute_if_absent(
        &self,
        key: Vec<u8>,
        func: impl FnOnce(&mut BTreeMap<SearchKey, Vec<u8>>),
    ) {
        let entry = self.store.entry(key);
        match entry {
            Entry::Occupied(_) => {}
            Entry::Vacant(vacant_entry) => {
                let mut map = BTreeMap::new();
                func(&mut map);
                vacant_entry.insert(map);
            }
        };
    }
}

#[cfg(test)]
mod tests {

    use crate::model::SearchKey;
    use std::collections::BTreeMap;

    use super::DurableKeyMapStore;

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
}
