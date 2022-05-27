use std::fs::File;
use std::io::Write;
use std::path::Path;

use dashmap::DashMap;
use log::info;
use memmap::MmapOptions;

use dashmap::mapref::entry::Entry;
use crate::wal::WalStorage;

const KV_WAL_FILE_NAME: &str = "kv.wal.dat";
const TMP_KV_WAL_FILE_NAME: &str = ".kv.wal.dat";

pub struct DurableKeyValueStore<W: Write> {
    store: DashMap<Vec<u8>, Vec<u8>>,
    wal: WalStorage<W>,
}

impl DurableKeyValueStore<File> {
    pub fn init_new(store_dir: &str) -> Self {
        let store_dir_path = Path::new(store_dir);
        let wal_file_path = store_dir_path.join(KV_WAL_FILE_NAME);
        let tmp_wal_file_path = store_dir_path.join(TMP_KV_WAL_FILE_NAME);

        let store = DashMap::new();
        let mut found_kv_wal = wal_file_path.exists();

        if found_kv_wal {
            if std::fs::metadata(&wal_file_path).unwrap().len() == 0 {
                let _ = std::fs::remove_file(&wal_file_path);
                found_kv_wal = false;
            } else {
                let _ = std::fs::rename(&wal_file_path, &tmp_wal_file_path).unwrap();
            }
        }

        let wal = WalStorage::new_file_based(wal_file_path.as_path());

        if found_kv_wal {
            let file = File::open(&tmp_wal_file_path).unwrap();
            info!("found KeyValue WAL file: {}, trying to restore...", &wal_file_path.to_str().unwrap());

            let content_as_slice = unsafe { MmapOptions::new().map(&file).unwrap() };

            let map = crate::wal::collect(content_as_slice.as_ref());
            info!("restored map with size: {}, adding new new WAL file", map.len());

            for (k, v) in map {
                let (k, v) = wal.store_put_event(k, v);
                store.insert(k, v);
            }
            info!("{} entries added to store", store.len());

            let _ = std::fs::remove_file(tmp_wal_file_path.as_path());
            info!("removed old wal file {}", tmp_wal_file_path.to_str().unwrap());
        } else {
            info!("no previous wal log found, starting from scratch: {}", &wal_file_path.to_str().unwrap());
        }

        DurableKeyValueStore { store, wal }
    }
}

impl DurableKeyValueStore<Vec<u8>> {
    #[allow(unused)]
    pub fn new_vec_based() -> Self {
        DurableKeyValueStore { store: DashMap::new(), wal: WalStorage::new_vec_based() }
    }
}

impl<W: Write> DurableKeyValueStore<W> {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.store.get(key) {
            None => { None }
            Some(inner_val) => {
                let result = Vec::from(&inner_val.value()[..]);
                Some(result)
            }
        }
    }

    pub fn put(&self, key: Vec<u8>, val: Vec<u8>) {
        let (key, val) = self.wal.store_put_event(key, val);

        self.store.insert(key, val);
    }

    pub fn compute(&self, key: Vec<u8>, func: impl FnOnce(Option<&[u8]>) -> Vec<u8>) {
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let new_val = func(Some(entry.get().as_slice()));
                self.wal.store_put_event(entry.key().clone(), new_val.clone());
                *entry.get_mut() = new_val;
            }
            Entry::Vacant(entry) => {
                let new_val = func(None);
                self.wal.store_put_event(entry.key().clone(), new_val.clone());
                entry.insert(new_val);
            }
        };
    }

    pub fn increment_or_init(&self, key: Vec<u8>, increment_by: u64) -> Result<u64, ()> {
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let entry_bytes = entry.get().as_slice();
                let bytes_arr: [u8; 8] = match <&[u8] as std::convert::TryInto<[u8; 8]>>::try_into(entry_bytes) {
                    Ok(arr) => arr,
                    Err(_) => {
                        return Err(());
                    }
                };
                let cur_num = u64::from_ne_bytes(bytes_arr);
                let new_num = cur_num + increment_by;
                let new_num_bytes = u64::to_ne_bytes(new_num).to_vec();
                self.wal.store_put_event(entry.key().clone(), new_num_bytes.clone());
                *entry.get_mut() = new_num_bytes;
                Ok(new_num)
            }
            Entry::Vacant(entry) => {
                let new_num = increment_by;
                let new_num_bytes = u64::to_ne_bytes(new_num).to_vec();
                self.wal.store_put_event(entry.key().clone(), new_num_bytes.clone());
                entry.insert(new_num_bytes);
                Ok(new_num)
            }
        }
    }

    pub fn decrement(&self, key: Vec<u8>, decrement_by: u64) -> Option<Result<u64, ()>> {
        match self.store.entry(key) {
            Entry::Occupied(mut entry) => {
                let entry_bytes = entry.get().as_slice();
                let bytes_arr: [u8; 8] = match <&[u8] as std::convert::TryInto<[u8; 8]>>::try_into(entry_bytes) {
                    Ok(arr) => arr,
                    Err(_) => {
                        return Some(Err(()));
                    }
                };
                let cur_num = u64::from_ne_bytes(bytes_arr);
                let new_num = if decrement_by >= cur_num {
                    0
                } else {
                    cur_num - decrement_by
                };
                let new_num_bytes = u64::to_ne_bytes(new_num).to_vec();
                self.wal.store_put_event(entry.key().clone(), new_num_bytes.clone());
                *entry.get_mut() = new_num_bytes;
                Some(Ok(new_num))
            }
            Entry::Vacant(_) => {
                None
            }
        }
    }

    pub fn read_number(&self, key: &[u8]) -> Option<Result<u64, ()>> {
        self.store.get(key).map(|entry_bytes| {
            let byters_arr: [u8; 8] = match <&[u8] as std::convert::TryInto<[u8; 8]>>::try_into(entry_bytes.value().as_slice()) {
                Ok(arr) => arr,
                Err(_) => {
                    return Err(());
                }
            };
            Ok(u64::from_ne_bytes(byters_arr))
        })
    }
    
    pub fn set_number(&self, key: Vec<u8>, number: u64) {
        let value = u64::to_ne_bytes(number).to_vec();

        self.wal.store_put_event(key.clone(), value.clone());

        self.store.insert(key, value);
    }

    #[allow(unused)]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.store.contains_key(key)
    }

    pub fn remove(&self, key: &[u8]) {
        self.wal.store_delete_event(&key);

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

        store.compute("a".to_string().into_bytes(), |_| bincode::serialize::<usize>(&0).expect("0 should be serialized") );

        let found = store.get("a".to_string().as_bytes()).unwrap();
        let cur_num: usize = bincode::deserialize(found.as_slice()).unwrap();
        assert_eq!(cur_num, 0);

        store.compute("a".to_string().into_bytes(), |value| {
            let mut cur_num: usize = bincode::deserialize(value.unwrap()).unwrap();
            cur_num += 1;
            bincode::serialize::<usize>(&cur_num).unwrap()
        } );
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

