use std::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KeyValueRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortedMapEntry {
    key: Vec<u8>,

    search_key: SearchKey,

    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

impl SortedMapEntry {
    pub fn new(key: Vec<u8>, search_key: SearchKey, value: Vec<u8>) -> Self {
        Self { key, search_key, value }
    }

    pub fn entry(self) -> (Vec<u8>, SearchKey, Vec<u8>) {
        (self.key, self.search_key, self.value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortedMapKey {
    key: Vec<u8>,
    search_key: SearchKey,
}

impl SortedMapKey {
    pub fn new(key: Vec<u8>, search_key: SearchKey) -> Self {
        Self { key, search_key }
    }
    pub fn owned(self) -> (Vec<u8>, SearchKey) {
        (self.key, self.search_key)
    }
 }

#[derive(Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SearchKey(Vec<Key>);

impl SearchKey {
    pub fn first(&self) -> Option<&Key> {
        self.0.first()
    }

    pub fn get(&self, idx: usize) -> Option<&Key> {
        self.0.get(idx)
    }

 }
impl From<usize> for SearchKey {
    fn from(value: usize) -> Self {
        Self(vec![Key::USIZE(value)])
    }
}

impl From<&'static str> for SearchKey {
    fn from(value: &'static str) -> Self {
        Self(vec![Key::Str(value.into())])
    }
}

impl Ord for SearchKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialEq<Self> for SearchKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for SearchKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Key {
    Bool(bool),
    I(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    USIZE(usize),
    I128(u64),
    U128(u128),
    Char(char),
    Str(String),
    Bytes(Vec<u8>),
}
