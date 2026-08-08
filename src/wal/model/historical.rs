//! Frozen sorted-map payload models for legacy, V1, and earlier V2 records.

use bincode::Options;
use serde::Deserialize;

use crate::model::{Key, SearchKey, SortedMapEntry, SortedMapKey};

#[derive(Deserialize)]
struct HistoricalSortedMapEntry {
    key: Vec<u8>,
    search_key: HistoricalSearchKey,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

#[derive(Deserialize)]
struct HistoricalSortedMapKey {
    key: Vec<u8>,
    search_key: HistoricalSearchKey,
}

#[derive(Deserialize)]
struct HistoricalSearchKey(Vec<HistoricalKey>);

#[derive(Deserialize)]
enum HistoricalKey {
    Bool(bool),
    I(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    Usize(usize),
    I128(u64),
    U128(u128),
    Char(char),
    Str(String),
    Bytes(Vec<u8>),
}

impl From<HistoricalKey> for Key {
    fn from(key: HistoricalKey) -> Self {
        match key {
            HistoricalKey::Bool(value) => Self::Bool(value),
            HistoricalKey::I(value) => Self::I(value),
            HistoricalKey::U8(value) => Self::U8(value),
            HistoricalKey::I16(value) => Self::I16(value),
            HistoricalKey::U16(value) => Self::U16(value),
            HistoricalKey::I32(value) => Self::I32(value),
            HistoricalKey::U32(value) => Self::U32(value),
            HistoricalKey::I64(value) => Self::I64(value),
            HistoricalKey::U64(value) => Self::U64(value),
            HistoricalKey::Usize(value) => Self::USIZE(value),
            HistoricalKey::I128(value) => Self::I128(i128::from(value)),
            HistoricalKey::U128(value) => Self::U128(value),
            HistoricalKey::Char(value) => Self::Char(value),
            HistoricalKey::Str(value) => Self::Str(value),
            HistoricalKey::Bytes(value) => Self::Bytes(value),
        }
    }
}

impl From<HistoricalSearchKey> for SearchKey {
    fn from(search_key: HistoricalSearchKey) -> Self {
        Self::from(search_key.0.into_iter().map(Key::from).collect::<Vec<_>>())
    }
}

pub(crate) fn decode_historical_sorted_map_entry(
    payload: &[u8],
) -> bincode::Result<SortedMapEntry> {
    strict_options()
        .deserialize::<HistoricalSortedMapEntry>(payload)
        .map(|entry| SortedMapEntry::new(entry.key, SearchKey::from(entry.search_key), entry.value))
}

pub(crate) fn decode_historical_sorted_map_key(payload: &[u8]) -> bincode::Result<SortedMapKey> {
    strict_options()
        .deserialize::<HistoricalSortedMapKey>(payload)
        .map(|key| SortedMapKey::new(key.key, SearchKey::from(key.search_key)))
}

pub(crate) fn decode_current_sorted_map_entry(payload: &[u8]) -> bincode::Result<SortedMapEntry> {
    strict_options().deserialize(payload)
}

pub(crate) fn decode_current_sorted_map_key(payload: &[u8]) -> bincode::Result<SortedMapKey> {
    strict_options().deserialize(payload)
}

fn strict_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}
