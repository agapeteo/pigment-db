//! Test-first home for the V1 WAL grammar.
//!
//! This module remains disconnected from release reads and writes until its codec
//! invariants are proven and the promotion task removes the test-only registration.

#![allow(dead_code)]

use super::model::{
    decode_current_sorted_map_entry, decode_current_sorted_map_key,
    decode_historical_sorted_map_entry, decode_historical_sorted_map_key, KeyValueData,
    MAP_PUT_V2_ACT, MAP_REMOVE_V2_ACT,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct V1CodecProbe;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeaderProbeClassification {
    Valid,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordBoundsError {
    Truncated,
    Overflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordProbeFields<'a> {
    pub(crate) action: u8,
    pub(crate) payload: &'a [u8],
    pub(crate) physical_start: u32,
    pub(crate) mutation_start: u32,
    pub(crate) index: u32,
    pub(crate) count: u32,
    pub(crate) timestamp_bucket: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct V2CodecProbe;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct V2HeaderProbeFields {
    pub(crate) kind: u8,
    pub(crate) granularity_nanos: u64,
    pub(crate) base_bucket: u64,
    pub(crate) segment_id: u64,
    pub(crate) segment_base: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct V2RecordProbeFields<'a> {
    pub(crate) action: u8,
    pub(crate) payload: &'a [u8],
    pub(crate) physical_start: u64,
    pub(crate) mutation_start: u64,
    pub(crate) index: u32,
    pub(crate) count: u32,
    pub(crate) timestamp_bucket: u64,
}

impl V1CodecProbe {
    pub(crate) const HEADER_LEN: usize = 40;
    pub(crate) const EMPTY_RECORD_LEN: usize = 46;
    const MAGIC: [u8; 8] = *b"PIGWAL\r\n";
    const VERSION: u16 = 1;
    const TIMESTAMP_UNIT_UNIX_NANOS: u8 = 1;
    const DEFAULT_GRANULARITY_NANOS: u64 = 60_000_000_000;
    const RECORD_MARKER: [u8; 2] = [0xa7, 0xd1];
    const RECORD_VERSION: u8 = 1;
    const RECORD_HEADER_LEN: u16 = 38;

    pub(crate) fn encode_header() -> [u8; Self::HEADER_LEN] {
        Self::encode_header_with_kind(1)
    }

    pub(crate) fn encode_header_with_kind(kind: u8) -> [u8; Self::HEADER_LEN] {
        let mut bytes = [0; Self::HEADER_LEN];
        bytes[..Self::MAGIC.len()].copy_from_slice(&Self::MAGIC);
        bytes[8..10].copy_from_slice(&Self::VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(Self::HEADER_LEN as u16).to_le_bytes());
        bytes[12] = kind;
        bytes[13] = Self::TIMESTAMP_UNIT_UNIX_NANOS;
        bytes[16..24].copy_from_slice(&Self::DEFAULT_GRANULARITY_NANOS.to_le_bytes());
        Self::write_header_crc(&mut bytes);
        bytes
    }

    pub(crate) fn encode_header_with_granularity(granularity: u64) -> [u8; Self::HEADER_LEN] {
        Self::encode_header_with_kind_and_granularity(1, granularity)
    }

    pub(crate) fn encode_header_with_kind_and_granularity(
        kind: u8,
        granularity: u64,
    ) -> [u8; Self::HEADER_LEN] {
        let mut bytes = Self::encode_header_with_kind(kind);
        bytes[16..24].copy_from_slice(&granularity.to_le_bytes());
        Self::write_header_crc(&mut bytes);
        bytes
    }

    pub(crate) fn encode_header_with_base_bucket(base_bucket: u64) -> [u8; Self::HEADER_LEN] {
        let mut bytes = Self::encode_header();
        bytes[24..32].copy_from_slice(&base_bucket.to_le_bytes());
        Self::write_header_crc(&mut bytes);
        bytes
    }

    pub(crate) fn magic_is_valid(bytes: &[u8]) -> bool {
        bytes.get(..Self::MAGIC.len()) == Some(Self::MAGIC.as_slice())
    }

    pub(crate) fn version_is_valid(bytes: &[u8]) -> bool {
        bytes
            .get(8..10)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(Self::VERSION)
    }

    pub(crate) fn header_length_is_valid(bytes: &[u8]) -> bool {
        bytes
            .get(10..12)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(Self::HEADER_LEN as u16)
    }

    pub(crate) fn kind_is_valid(bytes: &[u8]) -> bool {
        bytes.get(12).is_some_and(|kind| matches!(*kind, 1..=3))
    }

    pub(crate) fn timestamp_unit_is_valid(bytes: &[u8]) -> bool {
        bytes.get(13) == Some(&Self::TIMESTAMP_UNIT_UNIX_NANOS)
    }

    pub(crate) fn granularity_is_valid(bytes: &[u8]) -> bool {
        Self::granularity(bytes).is_some_and(|granularity| granularity != 0)
    }

    pub(crate) fn granularity(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(16..24)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn base_bucket(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(24..32)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn flags_are_valid(bytes: &[u8]) -> bool {
        bytes
            .get(14..16)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(0)
    }

    pub(crate) fn reserved_is_valid(bytes: &[u8]) -> bool {
        bytes
            .get(32..36)
            .is_some_and(|reserved| reserved.iter().all(|byte| *byte == 0))
    }

    pub(crate) fn header_crc_is_valid(bytes: &[u8]) -> bool {
        let Some(prefix) = bytes.get(..36) else {
            return false;
        };
        let Some(stored) = bytes
            .get(36..40)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        stored == crc32fast::hash(prefix)
    }

    pub(crate) fn classify_header(bytes: &[u8]) -> HeaderProbeClassification {
        if bytes.len() == Self::HEADER_LEN {
            HeaderProbeClassification::Valid
        } else {
            HeaderProbeClassification::Invalid
        }
    }

    fn write_header_crc(bytes: &mut [u8; Self::HEADER_LEN]) {
        let crc = crc32fast::hash(&bytes[..36]);
        bytes[36..40].copy_from_slice(&crc.to_le_bytes());
    }

    pub(crate) fn encode_record() -> Vec<u8> {
        Self::encode_record_parts(1, &[], Self::HEADER_LEN as u32)
    }

    pub(crate) fn encode_complete_record(fields: RecordProbeFields<'_>) -> Vec<u8> {
        let payload_len = u32::try_from(fields.payload.len()).expect("probe payload must fit u32");
        let mut bytes = vec![0; Self::EMPTY_RECORD_LEN + fields.payload.len()];
        bytes[..2].copy_from_slice(&Self::RECORD_MARKER);
        bytes[2] = Self::RECORD_VERSION;
        bytes[3] = fields.action;
        bytes[4..6].copy_from_slice(&Self::RECORD_HEADER_LEN.to_le_bytes());
        bytes[6..10].copy_from_slice(&payload_len.to_le_bytes());
        bytes[10..14].copy_from_slice(&(!payload_len).to_le_bytes());
        bytes[14..18].copy_from_slice(&fields.physical_start.to_le_bytes());
        bytes[18..22].copy_from_slice(&fields.mutation_start.to_le_bytes());
        bytes[22..26].copy_from_slice(&fields.index.to_le_bytes());
        bytes[26..30].copy_from_slice(&fields.count.to_le_bytes());
        bytes[30..38].copy_from_slice(&fields.timestamp_bucket.to_le_bytes());
        bytes[38..38 + fields.payload.len()].copy_from_slice(fields.payload);
        let footer_start = 38 + fields.payload.len();
        bytes[footer_start..footer_start + 4].copy_from_slice(&fields.physical_start.to_le_bytes());
        Self::write_record_crc(&mut bytes);
        bytes
    }

    pub(crate) fn encode_record_with_action(action: u8) -> Vec<u8> {
        Self::encode_record_parts(action, &[], Self::HEADER_LEN as u32)
    }

    pub(crate) fn encode_record_with_payload(payload: &[u8]) -> Vec<u8> {
        Self::encode_record_parts(1, payload, Self::HEADER_LEN as u32)
    }

    pub(crate) fn encode_record_at(physical_start: u32, payload: &[u8]) -> Vec<u8> {
        Self::encode_record_parts(1, payload, physical_start)
    }

    pub(crate) fn encode_record_with_mutation_start(
        physical_start: u32,
        mutation_start: u32,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut bytes = Self::encode_record_at(physical_start, payload);
        bytes[18..22].copy_from_slice(&mutation_start.to_le_bytes());
        Self::write_record_crc(&mut bytes);
        bytes
    }

    pub(crate) fn encode_record_with_group(index: u32, count: u32) -> Vec<u8> {
        let mut bytes = Self::encode_record();
        bytes[22..26].copy_from_slice(&index.to_le_bytes());
        bytes[26..30].copy_from_slice(&count.to_le_bytes());
        Self::write_record_crc(&mut bytes);
        bytes
    }

    pub(crate) fn encode_record_with_timestamp(timestamp_bucket: u64) -> Vec<u8> {
        let mut bytes = Self::encode_record();
        bytes[30..38].copy_from_slice(&timestamp_bucket.to_le_bytes());
        Self::write_record_crc(&mut bytes);
        bytes
    }

    fn encode_record_parts(action: u8, payload: &[u8], physical_start: u32) -> Vec<u8> {
        Self::encode_complete_record(RecordProbeFields {
            action,
            payload,
            physical_start,
            mutation_start: physical_start,
            index: 0,
            count: 1,
            timestamp_bucket: 0,
        })
    }

    pub(crate) fn record_marker_is_valid(bytes: &[u8]) -> bool {
        bytes.get(..2) == Some(Self::RECORD_MARKER.as_slice())
    }

    pub(crate) fn record_version_is_valid(bytes: &[u8]) -> bool {
        bytes.get(2) == Some(&Self::RECORD_VERSION)
    }

    pub(crate) fn record_action_is_valid(bytes: &[u8]) -> bool {
        bytes.get(3).is_some_and(|action| matches!(*action, 0..=5))
    }

    pub(crate) fn record_header_length_is_valid(bytes: &[u8]) -> bool {
        bytes
            .get(4..6)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(Self::RECORD_HEADER_LEN)
    }

    pub(crate) fn record_length_complement_is_valid(bytes: &[u8]) -> bool {
        let Some(length) = bytes
            .get(6..10)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        let Some(complement) = bytes
            .get(10..14)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        complement == !length
    }

    pub(crate) fn checked_record_end(
        physical_start: u32,
        payload_len: u32,
        available_record_len: usize,
    ) -> Result<u32, RecordBoundsError> {
        let record_len = payload_len
            .checked_add(Self::EMPTY_RECORD_LEN as u32)
            .ok_or(RecordBoundsError::Overflow)?;
        let physical_end = physical_start
            .checked_add(record_len)
            .ok_or(RecordBoundsError::Overflow)?;
        if available_record_len < record_len as usize {
            return Err(RecordBoundsError::Truncated);
        }
        Ok(physical_end)
    }

    pub(crate) fn record_physical_start_is_valid(bytes: &[u8], actual_start: u32) -> bool {
        let Some(header_start) = bytes
            .get(14..18)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        let Some(payload_len) = bytes
            .get(6..10)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return false;
        };
        let Some(footer_start) = 38_usize.checked_add(payload_len) else {
            return false;
        };
        let Some(footer_end) = footer_start.checked_add(4) else {
            return false;
        };
        let Some(footer_start_value) = bytes
            .get(footer_start..footer_end)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        header_start == actual_start && footer_start_value == actual_start
    }

    pub(crate) fn record_mutation_start_is_valid(bytes: &[u8]) -> bool {
        let Some(physical_start) = bytes
            .get(14..18)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        let Some(mutation_start) = bytes
            .get(18..22)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        mutation_start >= Self::HEADER_LEN as u32 && mutation_start <= physical_start
    }

    pub(crate) fn record_index_count_are_valid(bytes: &[u8]) -> bool {
        let Some(index) = bytes
            .get(22..26)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        let Some(count) = bytes
            .get(26..30)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        count != 0 && index < count
    }

    pub(crate) fn record_timestamp_bucket(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(30..38)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn payload_is_valid(store_kind: u8, action: u8, payload: &[u8]) -> bool {
        match (store_kind, action) {
            (1..=3, 0) => true,
            (1, 1) | (2, 2) | (2, 3) => bincode::deserialize::<KeyValueData>(payload).is_ok(),
            (3, 4) => decode_historical_sorted_map_entry(payload).is_ok(),
            (3, 5) => decode_historical_sorted_map_key(payload).is_ok(),
            _ => false,
        }
    }

    pub(crate) fn record_crc_is_valid(bytes: &[u8]) -> bool {
        let Some(payload_len) = bytes
            .get(6..10)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return false;
        };
        let Some(crc_start) = 42_usize.checked_add(payload_len) else {
            return false;
        };
        let Some(crc_end) = crc_start.checked_add(4) else {
            return false;
        };
        let Some(stored) = bytes
            .get(crc_start..crc_end)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        stored == crc32fast::hash(&bytes[..crc_start])
    }

    fn write_record_crc(bytes: &mut [u8]) {
        let payload_len = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
        let crc_start = 42 + payload_len;
        let crc = crc32fast::hash(&bytes[..crc_start]);
        bytes[crc_start..crc_start + 4].copy_from_slice(&crc.to_le_bytes());
    }
}

impl V2CodecProbe {
    pub(crate) const HEADER_LEN: usize = 64;
    pub(crate) const EMPTY_RECORD_LEN: usize = 66;
    const MAGIC: [u8; 8] = *b"PIGWAL\r\n";
    const VERSION: u16 = 2;
    const TIMESTAMP_UNIT_UNIX_NANOS: u8 = 1;
    const RECORD_MARKER: [u8; 2] = [0xa7, 0xd1];
    const RECORD_VERSION: u8 = 2;
    const RECORD_HEADER_LEN: u16 = 54;

    pub(crate) fn encode_header(fields: V2HeaderProbeFields) -> [u8; Self::HEADER_LEN] {
        let mut bytes = [0; Self::HEADER_LEN];
        bytes[..Self::MAGIC.len()].copy_from_slice(&Self::MAGIC);
        bytes[8..10].copy_from_slice(&Self::VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(Self::HEADER_LEN as u16).to_le_bytes());
        bytes[12] = fields.kind;
        bytes[13] = Self::TIMESTAMP_UNIT_UNIX_NANOS;
        bytes[16..24].copy_from_slice(&fields.granularity_nanos.to_le_bytes());
        bytes[24..32].copy_from_slice(&fields.base_bucket.to_le_bytes());
        bytes[32..40].copy_from_slice(&fields.segment_id.to_le_bytes());
        bytes[40..48].copy_from_slice(&fields.segment_base.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub(crate) fn header_is_valid(bytes: &[u8]) -> bool {
        if bytes.len() != Self::HEADER_LEN
            || bytes.get(..8) != Some(Self::MAGIC.as_slice())
            || bytes.get(8..10) != Some(Self::VERSION.to_le_bytes().as_slice())
            || bytes.get(10..12) != Some((Self::HEADER_LEN as u16).to_le_bytes().as_slice())
            || !bytes.get(12).is_some_and(|kind| matches!(*kind, 1..=3))
            || bytes.get(13) != Some(&Self::TIMESTAMP_UNIT_UNIX_NANOS)
            || bytes.get(14..16) != Some(0_u16.to_le_bytes().as_slice())
            || Self::header_granularity(bytes) == Some(0)
            || !bytes
                .get(48..60)
                .is_some_and(|reserved| reserved.iter().all(|byte| *byte == 0))
        {
            return false;
        }
        let Some(stored_crc) = bytes
            .get(60..64)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        stored_crc == crc32fast::hash(&bytes[..60])
    }

    pub(crate) fn header_kind(bytes: &[u8]) -> Option<u8> {
        bytes.get(12).copied()
    }

    pub(crate) fn header_granularity(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(16..24)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn header_base_bucket(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(24..32)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn header_segment_id(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(32..40)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn header_segment_base(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(40..48)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn encode_complete_record(fields: V2RecordProbeFields<'_>) -> Vec<u8> {
        let mut bytes = Vec::new();
        Self::encode_complete_record_into(&mut bytes, fields);
        bytes
    }

    pub(crate) fn encode_complete_record_into(
        bytes: &mut Vec<u8>,
        fields: V2RecordProbeFields<'_>,
    ) {
        let payload_len = u64::try_from(fields.payload.len()).expect("payload length must fit u64");
        let encoded_len = Self::EMPTY_RECORD_LEN
            .checked_add(fields.payload.len())
            .expect("encoded V2 record length must fit usize");
        bytes.clear();
        bytes.reserve(encoded_len);
        bytes.extend_from_slice(&Self::RECORD_MARKER);
        bytes.extend_from_slice(&[Self::RECORD_VERSION, fields.action]);
        bytes.extend_from_slice(&Self::RECORD_HEADER_LEN.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&(!payload_len).to_le_bytes());
        bytes.extend_from_slice(&fields.physical_start.to_le_bytes());
        bytes.extend_from_slice(&fields.mutation_start.to_le_bytes());
        bytes.extend_from_slice(&fields.index.to_le_bytes());
        bytes.extend_from_slice(&fields.count.to_le_bytes());
        bytes.extend_from_slice(&fields.timestamp_bucket.to_le_bytes());
        bytes.extend_from_slice(fields.payload);
        bytes.extend_from_slice(&fields.physical_start.to_le_bytes());
        let crc = crc32fast::hash(bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(bytes.len(), encoded_len);
    }

    pub(crate) fn checked_record_end(
        physical_start: u64,
        payload_len: u64,
        available_record_len: usize,
    ) -> Result<u64, RecordBoundsError> {
        let record_len = payload_len
            .checked_add(Self::EMPTY_RECORD_LEN as u64)
            .ok_or(RecordBoundsError::Overflow)?;
        let physical_end = physical_start
            .checked_add(record_len)
            .ok_or(RecordBoundsError::Overflow)?;
        if u64::try_from(available_record_len).map_err(|_| RecordBoundsError::Overflow)?
            < record_len
        {
            return Err(RecordBoundsError::Truncated);
        }
        Ok(physical_end)
    }

    pub(crate) fn record_physical_start_is_valid(bytes: &[u8], actual_start: u64) -> bool {
        let Some(header_start) = bytes
            .get(22..30)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
        else {
            return false;
        };
        let Some(payload_len) = bytes
            .get(6..14)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return false;
        };
        let Some(footer_start) = 54_usize.checked_add(payload_len) else {
            return false;
        };
        let Some(footer_end) = footer_start.checked_add(8) else {
            return false;
        };
        let Some(footer_start_value) = bytes
            .get(footer_start..footer_end)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
        else {
            return false;
        };
        header_start == actual_start && footer_start_value == actual_start
    }

    pub(crate) fn record_marker_is_valid(bytes: &[u8]) -> bool {
        bytes.get(..2) == Some(Self::RECORD_MARKER.as_slice())
    }

    pub(crate) fn record_version_is_valid(bytes: &[u8]) -> bool {
        bytes.get(2) == Some(&Self::RECORD_VERSION)
    }

    pub(crate) fn record_action_is_valid(bytes: &[u8]) -> bool {
        bytes.get(3).is_some_and(|action| matches!(*action, 0..=7))
    }

    pub(crate) fn record_header_length_is_valid(bytes: &[u8]) -> bool {
        bytes
            .get(4..6)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            == Some(Self::RECORD_HEADER_LEN)
    }

    pub(crate) fn record_length_complement_is_valid(bytes: &[u8]) -> bool {
        let Some(length) = bytes
            .get(6..14)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
        else {
            return false;
        };
        let Some(complement) = bytes
            .get(14..22)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
        else {
            return false;
        };
        complement == !length
    }

    pub(crate) fn record_mutation_start_is_valid(bytes: &[u8], segment_base: u64) -> bool {
        let Some(physical_start) = bytes
            .get(22..30)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
        else {
            return false;
        };
        let Some(mutation_start) = bytes
            .get(30..38)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
        else {
            return false;
        };
        mutation_start >= segment_base.saturating_add(Self::HEADER_LEN as u64)
            && mutation_start <= physical_start
    }

    pub(crate) fn record_index_count_are_valid(bytes: &[u8]) -> bool {
        let Some(index) = bytes
            .get(38..42)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        let Some(count) = bytes
            .get(42..46)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        count != 0 && index < count
    }

    pub(crate) fn record_timestamp_bucket(bytes: &[u8]) -> Option<u64> {
        bytes
            .get(46..54)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(crate) fn payload_is_valid(store_kind: u8, action: u8, payload: &[u8]) -> bool {
        match (store_kind, action) {
            (3, MAP_PUT_V2_ACT) => decode_current_sorted_map_entry(payload).is_ok(),
            (3, MAP_REMOVE_V2_ACT) => decode_current_sorted_map_key(payload).is_ok(),
            _ => V1CodecProbe::payload_is_valid(store_kind, action, payload),
        }
    }

    pub(crate) fn record_crc_is_valid(bytes: &[u8]) -> bool {
        let Some(payload_len) = bytes
            .get(6..14)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return false;
        };
        let Some(crc_start) = 62_usize.checked_add(payload_len) else {
            return false;
        };
        let Some(crc_end) = crc_start.checked_add(4) else {
            return false;
        };
        let Some(stored) = bytes
            .get(crc_start..crc_end)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        stored == crc32fast::hash(&bytes[..crc_start])
    }
}
