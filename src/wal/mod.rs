use std::sync::{RwLock};
use std::fs::{OpenOptions, File};
use std::borrow::{BorrowMut, Borrow};
use std::io::{Write};

use log::{info, error};


use std::convert::TryInto;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::array::TryFromSliceError;
use crate::wal::model::*;

mod model;

struct WalState<W: Write> {
    offset: u32,
    writer: W,
}

pub struct WalStorage<W: Write> {
    wal_state: RwLock<WalState<W>>
}

impl WalStorage<File> {
    pub fn new_file_based(file_path: &Path) -> Self {
        let file = OpenOptions::new().write(true).append(true).create_new(true)
            .open(file_path).unwrap();

        let wal_state = WalState { offset: 0, writer: file };
        let wal_state = RwLock::new(wal_state);

        WalStorage { wal_state }
    }
}

impl WalStorage<Vec<u8>> {
    pub fn new_vec_based() -> Self {
        let vec = Vec::new();

        let wal_state = WalState { offset: 0, writer: vec };
        let wal_state = RwLock::new(wal_state);

        WalStorage { wal_state }
    }
}

impl<W: Write> WalStorage<W> {
    pub fn store_put_event(&self, key: Vec<u8>, value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let mut w_lock = self.wal_state.write().unwrap();

        let key_value = KeyValueData::new(key, value);
        let put_action = StoredAction::put_action(w_lock.offset.borrow(), &key_value);

        write(w_lock.writer.borrow_mut(), &put_action);
        increment_offset(w_lock.offset.borrow_mut(), &put_action);

        key_value.owned_key_value()
    }

    pub fn store_delete_event(&self, key: &[u8]) {
        let mut w_lock = self.wal_state.write().unwrap();

        let put_action = StoredAction::delete_action(w_lock.offset.borrow(), key);

        write(w_lock.writer.borrow_mut(), &put_action);
        increment_offset(w_lock.offset.borrow_mut(), &put_action);
    }

    pub fn store_append_to_set_event(&self, key: Vec<u8>, set_key: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let mut w_lock = self.wal_state.write().unwrap();

        let key_value = KeyValueData::new(key, set_key);
        let put_action = StoredAction::append_to_set(w_lock.offset.borrow(), &key_value);

        write(w_lock.writer.borrow_mut(), &put_action);
        increment_offset(w_lock.offset.borrow_mut(), &put_action);

        key_value.owned_key_value()
    }

    pub fn store_remove_from_set_event(&self, key: Vec<u8>, value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
        let mut w_lock = self.wal_state.write().unwrap();

        let key_value = KeyValueData::new(key, value);
        let put_action = StoredAction::remove_from_set(w_lock.offset.borrow(), &key_value);

        write(w_lock.writer.borrow_mut(), &put_action);
        increment_offset(w_lock.offset.borrow_mut(), &put_action);

        key_value.owned_key_value()
    }
}

fn write<W: Write>(file: &mut W, put_action: &StoredAction) {
    let _ = file.write(&put_action.act_type().to_ne_bytes()).unwrap();
    let _ = file.write(&put_action.crc().to_ne_bytes()).unwrap();
    let _ = file.write(&put_action.data_size().to_ne_bytes()).unwrap();
    let _ = file.write(put_action.data()).unwrap();
    let _ = file.write(&put_action.start_offset().to_ne_bytes()).unwrap();
    let _ = file.flush().unwrap();
}

fn increment_offset(offset: &mut u32, put_action: &StoredAction) {
    let fixed_block_len = FIXED_BLOCK_LEN as u32;
    let new_offset = put_action.start_offset() + put_action.data_size() + fixed_block_len;
    *offset = new_offset;
}

pub fn read_forward(bytes: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut result = HashMap::new();
    if bytes.is_empty() {
        return result;
    }
    let mut offset = 0;

    while offset < bytes.len() {
        let stored_action = build_action(&mut offset, bytes);

        let actual_crc = model::crc(stored_action.data());
        if actual_crc != *stored_action.crc() {
            panic!("wrong crc !!"); // todo: better error handling
        }

        match *stored_action.act_type() {
            model::DELETE_ACT => {
                result.remove(stored_action.data());
            }
            model::PUT_ACT => {
                let put_action: KeyValueData = bincode::deserialize(stored_action.data()).expect("KeyValueData should be deserialized");
                let (key, value) = put_action.owned_key_value();
                result.insert(key, value);
            }
            _ => { panic!("not supported action type: {}", stored_action.act_type()) }
        }
    }
    result
}

pub fn read_for_set(bytes: &[u8]) -> HashMap<Vec<u8>, HashSet<Vec<u8>>> {
    let mut result = HashMap::new();
    if bytes.is_empty() {
        return result;
    }
    let mut offset = 0;

    while offset < bytes.len() {
        let stored_action = build_action(&mut offset, bytes);

        let actual_crc = model::crc(stored_action.data());
        if actual_crc != *stored_action.crc() {
            panic!("wrong crc !!"); // todo: better error handling
        }

        match *stored_action.act_type() {
            model::DELETE_ACT => {
                result.remove(stored_action.data());
            }
            model::SET_APPEND_ACT => {
                let put_action: KeyValueData = bincode::deserialize(stored_action.data()).expect("KeyValueData should be deserialized");
                let (key, set_element) = put_action.owned_key_value();

                match result.get_mut(&key) {
                    None => {
                        let mut hashset = HashSet::new();
                        hashset.insert(set_element);
                        result.insert(key, hashset);
                    }
                    Some(hashset) => {
                        hashset.insert(set_element);
                    }
                }
            }
            model::SET_REMOVE_ACT => {
                let put_action: KeyValueData = bincode::deserialize(stored_action.data()).expect("KeyValueData should be deserialized");
                let (key, value) = put_action.owned_key_value();
                match result.get_mut(&key) {
                    None => {}
                    Some(hashset) => { hashset.remove(&value); }
                }
            }
            _ => { panic!("not supported action type: {}", stored_action.act_type()) }
        }
    }
    result
}

fn build_action(offset: &mut usize, bytes: &[u8]) -> StoredAction {
    let act_type_len = ACT_TYPE_FIELD_LEN as usize;
    let act_type_arr: [u8; 1] = bytes[*offset..*offset + act_type_len].try_into().unwrap();
    let act_type = u8::from_ne_bytes(act_type_arr);
    *offset += act_type_len;

    let crc_len = CRC32_FIELD_LEN as usize;
    let crc_slice = &bytes[*offset..*offset + crc_len];
    let crc_arr: [u8; 4] = crc_slice.try_into().unwrap();
    let crc = u32::from_ne_bytes(crc_arr);
    *offset += &crc_len;

    let data_size_len = DATA_SIZE_FIELD_LEN as usize;
    let data_size_slice = &bytes[*offset..*offset + data_size_len];
    let data_size_arr: [u8; 4] = data_size_slice.try_into().unwrap();
    let data_size = u32::from_ne_bytes(data_size_arr);
    *offset += &data_size_len;

    let data_len = data_size as usize;
    let data_slice = &bytes[*offset..*offset + data_len];
    let data: Vec<u8> = Vec::from(data_slice);
    *offset += &data_len;

    let block_start_len = BLOCK_START_OFFSET_LEN as usize;
    let block_start_slice = &bytes[*offset..*offset + block_start_len];
    let block_start_arr: [u8; 4] = block_start_slice.try_into().unwrap();
    let start_offset = u32::from_ne_bytes(block_start_arr);
    *offset += &block_start_len;

    StoredAction::new(act_type, crc, data_size, data, start_offset)
}

pub fn collect(bytes: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    info!("trying to read result from end");
    match read_backward(bytes) {
        Ok(val) => { val }
        Err(_) => {
            error!("error happened while reading from end, reading bytes from start");
            read_forward(bytes)
        }
    }
}

pub fn read_backward(bytes: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>, ()> {
    let mut result = HashMap::new();
    let mut removed_keys = HashSet::new();

    let size = bytes.len();
    let mut offset = match prev_block_start_offset(size, bytes) {
        Ok(val) => val,
        Err(_err) => { return Err(()); }
    };

    let mut stored_action = build_action(&mut offset, bytes);

    update_backward_reading_map(&stored_action, &mut result, &mut removed_keys);

    let mut last_consumed = stored_action.start_offset() == &0;

    while !last_consumed {
        let mut offset = match prev_block_start_offset(*stored_action.start_offset() as usize, bytes) {
            Ok(val) => val,
            Err(_) => { return Err(()); }
        };
        stored_action = build_action(&mut offset, bytes);
        update_backward_reading_map(&stored_action, &mut result, &mut removed_keys);
        if stored_action.start_offset() == &0 {
            last_consumed = true;
        }
    }
    Ok(result)
}

fn update_backward_reading_map(stored_action: &StoredAction, map: &mut HashMap<Vec<u8>, Vec<u8>>, removed_keys: &mut HashSet<Vec<u8>>) {
    match *stored_action.act_type() {
        model::DELETE_ACT => {
            let key = stored_action.data().to_vec();
            if !map.contains_key(&key) {
                let valid_crc = valid_crc(stored_action.crc(), stored_action.data());
                if !valid_crc {
                    panic!("not valid crc"); // todo: revert to forward
                }
                removed_keys.insert(key);
            }
        }
        model::PUT_ACT => {
            let put_action: KeyValueData = bincode::deserialize(stored_action.data()).expect("KeyValueData should be deserialized");
            let (key, value) = put_action.owned_key_value();

            if !map.contains_key(&key) && !removed_keys.contains(&key) {
                let valid_crc = valid_crc(stored_action.crc(), stored_action.data());
                if !valid_crc {
                    panic!("not valid crc"); // todo: revert to forward
                }
                map.insert(key, value);
            }
        }
        _ => { panic!("not supported action type: {}", stored_action.act_type()) }
    }
}

fn prev_block_start_offset(idx: usize, bytes: &[u8]) -> Result<usize, TryFromSliceError> {
    let block_start_len = BLOCK_START_OFFSET_LEN as usize;
    let block_start_slice = &bytes[idx - block_start_len..idx];
    let block_start_arr: [u8; 4] = match block_start_slice.try_into() {
        Ok(arr) => arr,
        Err(error) => return Err(error)
    };
    Ok(u32::from_ne_bytes(block_start_arr) as usize)
}

fn valid_crc(expected_crc: &u32, data: &[u8]) -> bool {
    let actual_crc = model::crc(data);
    actual_crc == *expected_crc
}

#[ignore]
#[test]
fn test_with_file() {
    let file_path = ".../sandbox/dcache/wal.dat";
    let path = std::path::Path::new(file_path);

    if path.exists() {
        let _ = std::fs::remove_file(file_path);
    }
    let wal = WalStorage::new_file_based(Path::new(file_path));

    wal.store_put_event(b"x".to_vec(), b"X".to_vec());
    wal.store_put_event(b"a".to_vec(), b"A".to_vec());
    wal.store_put_event(b"a".to_vec(), b"AAA".to_vec());
    wal.store_put_event(b"b".to_vec(), b"B!".to_vec());
    wal.store_delete_event(&b"x".to_vec());


    let bytes = std::fs::read(file_path).unwrap();
    let map = read_forward(&bytes);

    assert_eq!(map.get(&b"a".to_vec()), Some(&b"AAA".to_vec()));
    assert_eq!(map.get(&b"b".to_vec()), Some(&b"B!".to_vec()));
    assert_eq!(map.len(), 2);
}

#[test]
fn test_with_vec() {
    let wal = WalStorage::new_vec_based();

    wal.store_put_event(b"x".to_vec(), b"X".to_vec());
    wal.store_put_event(b"a".to_vec(), b"A".to_vec());
    wal.store_put_event(b"a".to_vec(), b"AAA".to_vec());
    wal.store_put_event(b"b".to_vec(), b"B!".to_vec());
    wal.store_delete_event(&b"x".to_vec());

    let map = collect(&wal.wal_state.read().unwrap().writer);
    // let map = read_forward(&wal.wal_state.read().unwrap().writer);
    // let map = read_backward(&wal.wal_state.read().unwrap().writer).unwrap();
    assert_eq!(map.get(&b"a".to_vec()), Some(&b"AAA".to_vec()));
    assert_eq!(map.get(&b"b".to_vec()), Some(&b"B!".to_vec()));
    assert_eq!(map.len(), 2);
}

#[test]
#[ignore]
fn test_read_backward() {
    use memmap::MmapOptions;

    let file_name = ".../sandbox/dcache/wal.dat.bk";
    let file = File::open(file_name).unwrap();
    let content_as_slice = unsafe { MmapOptions::new().map(&file).unwrap() };
    let bytes = content_as_slice.as_ref();

    let result = read_backward(bytes).unwrap();

    println!("result size: {}", &result.len());
    for (k, v) in result {
        println!("key: {}, value: {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }
}