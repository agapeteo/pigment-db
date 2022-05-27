use serde::{Deserialize, Serialize};
use crc32fast::Hasher;

pub const ACT_TYPE_FIELD_LEN: u8 = 1;
pub const CRC32_FIELD_LEN: u8 = 4;
pub const DATA_SIZE_FIELD_LEN: u8 = 4;
pub const BLOCK_START_OFFSET_LEN: u8 = 4;
pub const FIXED_BLOCK_LEN: u8 =
    ACT_TYPE_FIELD_LEN + CRC32_FIELD_LEN + DATA_SIZE_FIELD_LEN + BLOCK_START_OFFSET_LEN;

pub const DELETE_ACT: u8 = 0;
pub const PUT_ACT: u8 = 1;
pub const SET_APPEND_ACT: u8 = 2;
pub const SET_REMOVE_ACT: u8 = 3;


#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValueData {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,

    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

impl KeyValueData {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        KeyValueData { key, value }
    }

    pub fn owned_key_value(self) -> (Vec<u8>, Vec<u8>) {
        (self.key, self.value)
    }
}

#[derive(Debug)]
pub struct StoredAction {
    act_type: u8,
    crc: u32,
    data_size: u32,
    data: Vec<u8>,
    start_offset: u32,
}

impl StoredAction {
    pub fn new(act_type: u8, crc: u32, data_size: u32, data: Vec<u8>, start_offset: u32) -> Self {
        StoredAction { act_type, crc, data_size, data, start_offset }
    }
}

impl StoredAction {
    pub fn put_action(offset: &u32, key_value: &KeyValueData) -> Self {
        let act_type = PUT_ACT;
        let data = bincode::serialize(&key_value).expect("key_value should be serialized with bincode");
        let crc = crc(&data);
        let data_size = data.len() as u32;
        let start_offset = *offset;

        StoredAction { act_type, crc, data_size, data, start_offset }
    }

    pub fn delete_action(offset: &u32, key: &[u8]) -> Self {
        let act_type = DELETE_ACT;
        let crc = crc(key);
        let data = key.to_vec();
        let data_size = data.len() as u32;
        let start_offset = *offset;

        StoredAction { act_type, crc, data_size, data, start_offset }
    }

    pub fn append_to_set(offset: &u32, key_value: &KeyValueData) -> Self {
        let act_type = SET_APPEND_ACT;
        let data = bincode::serialize(&key_value).expect("key_value should be serialized with bincode");
        let crc = crc(&data);
        let data_size = data.len() as u32;
        let start_offset = *offset;

        StoredAction { act_type, crc, data_size, data, start_offset }
    }

    pub fn remove_from_set(offset: &u32, key_value: &KeyValueData) -> Self {
        let act_type = SET_REMOVE_ACT;
        let data = bincode::serialize(&key_value).expect("key_value should be serialized with bincode");
        let crc = crc(&data);
        let data_size = data.len() as u32;
        let start_offset = *offset;

        StoredAction { act_type, crc, data_size, data, start_offset }
    }

    pub fn act_type(&self) -> &u8 {
        &self.act_type
    }

    pub fn crc(&self) -> &u32 {
        &self.crc
    }

    pub fn data_size(&self) -> &u32 {
        &self.data_size
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn start_offset(&self) -> &u32 {
        &self.start_offset
    }
}

pub fn crc(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);

    hasher.finalize()
}