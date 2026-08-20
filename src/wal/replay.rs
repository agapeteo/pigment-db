//! Bounds-checked WAL replay support.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use super::format::{V1CodecProbe, V2CodecProbe};
use super::model::{
    crc, decode_current_sorted_map_entry, decode_current_sorted_map_key,
    decode_historical_sorted_map_entry, decode_historical_sorted_map_key, KeyValueData,
    StoredAction, DELETE_ACT, MAP_PUT_ACT, MAP_PUT_V2_ACT, MAP_REMOVE_ACT, MAP_REMOVE_V2_ACT,
    PUT_ACT, SET_APPEND_ACT, SET_REMOVE_ACT,
};
use crate::model::{SearchKey, SortedMapEntry};

const HEADER_LEN: usize = 1 + 4 + 4;
const FOOTER_LEN: usize = 4;

pub(crate) type KeyValueSnapshot = HashMap<Vec<u8>, Vec<u8>>;
pub(crate) type KeySetSnapshot = HashMap<Vec<u8>, HashSet<Vec<u8>>>;
pub(crate) type KeyMapSnapshot = HashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidationError {
    Truncated { offset: usize },
    UnsupportedAction { offset: usize, action: u8 },
    InvalidChecksum { offset: usize },
    InvalidStartOffset { offset: usize, stored: u32 },
    InvalidPayload { offset: usize },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { offset } => write!(formatter, "truncated WAL frame at {offset}"),
            Self::UnsupportedAction { offset, action } => {
                write!(formatter, "unsupported WAL action {action} at {offset}")
            }
            Self::InvalidChecksum { offset } => {
                write!(formatter, "invalid WAL checksum at {offset}")
            }
            Self::InvalidStartOffset { offset, stored } => write!(
                formatter,
                "WAL frame at {offset} stores inconsistent start offset {stored}"
            ),
            Self::InvalidPayload { offset } => write!(formatter, "invalid WAL payload at {offset}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckedFrame<'a> {
    action: u8,
    data: &'a [u8],
    start_offset: usize,
    end_offset: usize,
}

impl<'a> CheckedFrame<'a> {
    pub(crate) fn action(&self) -> u8 {
        self.action
    }

    pub(crate) fn data(&self) -> &'a [u8] {
        self.data
    }

    pub(crate) fn start_offset(&self) -> usize {
        self.start_offset
    }

    pub(crate) fn end_offset(&self) -> usize {
        self.end_offset
    }
}

pub(crate) struct CheckedFrames<'a> {
    bytes: &'a [u8],
    offset: usize,
    finished: bool,
}

impl<'a> CheckedFrames<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: 0,
            finished: false,
        }
    }

    fn fail(
        &mut self,
        error: ValidationError,
    ) -> Option<Result<CheckedFrame<'a>, ValidationError>> {
        self.finished = true;
        Some(Err(error))
    }
}

impl<'a> Iterator for CheckedFrames<'a> {
    type Item = Result<CheckedFrame<'a>, ValidationError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.offset == self.bytes.len() {
            return None;
        }

        let start = self.offset;
        let header_end = match start.checked_add(HEADER_LEN) {
            Some(end) if end <= self.bytes.len() => end,
            _ => return self.fail(ValidationError::Truncated { offset: start }),
        };
        let action = self.bytes[start];
        if !matches!(
            action,
            DELETE_ACT | PUT_ACT | SET_APPEND_ACT | SET_REMOVE_ACT | MAP_PUT_ACT | MAP_REMOVE_ACT
        ) {
            return self.fail(ValidationError::UnsupportedAction {
                offset: start,
                action,
            });
        }

        let expected_crc = u32::from_ne_bytes(self.bytes[start + 1..start + 5].try_into().unwrap());
        let data_len =
            u32::from_ne_bytes(self.bytes[start + 5..header_end].try_into().unwrap()) as usize;
        let data_end = match header_end.checked_add(data_len) {
            Some(end) if end <= self.bytes.len() => end,
            _ => return self.fail(ValidationError::Truncated { offset: start }),
        };
        let frame_end = match data_end.checked_add(FOOTER_LEN) {
            Some(end) if end <= self.bytes.len() => end,
            _ => return self.fail(ValidationError::Truncated { offset: start }),
        };
        let data = &self.bytes[header_end..data_end];
        if crc(data) != expected_crc {
            return self.fail(ValidationError::InvalidChecksum { offset: start });
        }

        let stored_start = u32::from_ne_bytes(self.bytes[data_end..frame_end].try_into().unwrap());
        if stored_start as usize != start {
            return self.fail(ValidationError::InvalidStartOffset {
                offset: start,
                stored: stored_start,
            });
        }

        self.offset = frame_end;
        Some(Ok(CheckedFrame {
            action,
            data,
            start_offset: start,
            end_offset: frame_end,
        }))
    }
}

#[derive(Clone, Copy, Debug)]
struct ReplayFrame<'a> {
    action: u8,
    data: &'a [u8],
    start_offset: usize,
    end_offset: usize,
    group_index: u32,
    group_count: u32,
    timestamp_bucket: u64,
}

impl<'a> ReplayFrame<'a> {
    fn action(&self) -> u8 {
        self.action
    }

    fn data(&self) -> &'a [u8] {
        self.data
    }

    fn start_offset(&self) -> usize {
        self.start_offset
    }

    fn is_group_end(&self) -> bool {
        self.group_index + 1 == self.group_count
    }

    fn timestamp_bucket(&self) -> u64 {
        self.timestamp_bucket
    }
}

fn checked_replay_frames(
    bytes: &[u8],
    expected_kind: u8,
) -> Result<Vec<ReplayFrame<'_>>, ValidationError> {
    if !bytes.starts_with(b"PIGWAL\r\n") {
        return CheckedFrames::new(bytes)
            .map(|frame| {
                frame.map(|frame| ReplayFrame {
                    action: frame.action(),
                    data: frame.data(),
                    start_offset: frame.start_offset(),
                    end_offset: frame.end_offset(),
                    group_index: 0,
                    group_count: 1,
                    timestamp_bucket: 0,
                })
            })
            .collect();
    }

    let version = bytes
        .get(8..10)
        .and_then(|value| value.try_into().ok())
        .map(u16::from_le_bytes);
    if version == Some(2) {
        return checked_v2_replay_frames(bytes, expected_kind);
    }

    let header_valid = bytes.len() >= V1CodecProbe::HEADER_LEN
        && V1CodecProbe::magic_is_valid(bytes)
        && V1CodecProbe::version_is_valid(bytes)
        && V1CodecProbe::header_length_is_valid(bytes)
        && V1CodecProbe::kind_is_valid(bytes)
        && bytes[12] == expected_kind
        && V1CodecProbe::timestamp_unit_is_valid(bytes)
        && V1CodecProbe::flags_are_valid(bytes)
        && V1CodecProbe::granularity_is_valid(bytes)
        && V1CodecProbe::reserved_is_valid(bytes)
        && V1CodecProbe::header_crc_is_valid(bytes);
    if !header_valid {
        return Err(ValidationError::InvalidPayload { offset: 0 });
    }

    let mut frames = Vec::new();
    let mut offset = V1CodecProbe::HEADER_LEN;
    while offset < bytes.len() {
        let fixed_header_end = offset
            .checked_add(38)
            .filter(|end| *end <= bytes.len())
            .ok_or(ValidationError::Truncated { offset })?;
        let payload_len = u32::from_le_bytes(
            bytes[offset + 6..offset + 10]
                .try_into()
                .expect("fixed V1 header bounds checked"),
        ) as usize;
        let end = offset
            .checked_add(V1CodecProbe::EMPTY_RECORD_LEN)
            .and_then(|fixed| fixed.checked_add(payload_len))
            .filter(|end| *end <= bytes.len())
            .ok_or(ValidationError::Truncated { offset })?;
        let frame = &bytes[offset..end];
        let action = frame[3];
        let structurally_valid = fixed_header_end <= bytes.len()
            && V1CodecProbe::record_marker_is_valid(frame)
            && V1CodecProbe::record_version_is_valid(frame)
            && V1CodecProbe::record_action_is_valid(frame)
            && V1CodecProbe::record_header_length_is_valid(frame)
            && V1CodecProbe::record_length_complement_is_valid(frame)
            && V1CodecProbe::record_physical_start_is_valid(frame, offset as u32)
            && V1CodecProbe::record_mutation_start_is_valid(frame)
            && V1CodecProbe::record_index_count_are_valid(frame)
            && V1CodecProbe::record_timestamp_bucket(frame).is_some()
            && V1CodecProbe::payload_is_valid(expected_kind, action, &frame[38..38 + payload_len])
            && V1CodecProbe::record_crc_is_valid(frame);
        if !structurally_valid {
            return Err(ValidationError::InvalidPayload { offset });
        }
        frames.push(ReplayFrame {
            action,
            data: &frame[38..38 + payload_len],
            start_offset: offset,
            end_offset: end,
            group_index: u32::from_le_bytes(frame[22..26].try_into().unwrap()),
            group_count: u32::from_le_bytes(frame[26..30].try_into().unwrap()),
            timestamp_bucket: u64::from_le_bytes(frame[30..38].try_into().unwrap()),
        });
        offset = end;
    }

    let mut group_start = 0;
    let mut previous_timestamp_bucket =
        V1CodecProbe::base_bucket(bytes).expect("validated V1 header has a base bucket");
    while group_start < frames.len() {
        let first = &bytes[frames[group_start].start_offset..frames[group_start].end_offset];
        let mutation_start = u32::from_le_bytes(first[18..22].try_into().unwrap());
        let count = u32::from_le_bytes(first[26..30].try_into().unwrap()) as usize;
        let timestamp_bucket = frames[group_start].timestamp_bucket;
        if mutation_start as usize != frames[group_start].start_offset
            || u32::from_le_bytes(first[22..26].try_into().unwrap()) != 0
            || group_start + count > frames.len()
        {
            return Err(ValidationError::Truncated {
                offset: frames[group_start].start_offset,
            });
        }
        if timestamp_bucket < previous_timestamp_bucket {
            return Err(ValidationError::InvalidPayload {
                offset: frames[group_start].start_offset,
            });
        }
        for group_index in 0..count {
            let frame_meta = &bytes[frames[group_start + group_index].start_offset
                ..frames[group_start + group_index].end_offset];
            if u32::from_le_bytes(frame_meta[18..22].try_into().unwrap()) != mutation_start
                || u32::from_le_bytes(frame_meta[22..26].try_into().unwrap()) != group_index as u32
                || u32::from_le_bytes(frame_meta[26..30].try_into().unwrap()) != count as u32
                || frames[group_start + group_index].timestamp_bucket != timestamp_bucket
            {
                return Err(ValidationError::InvalidPayload {
                    offset: frames[group_start + group_index].start_offset,
                });
            }
        }
        previous_timestamp_bucket = timestamp_bucket;
        group_start += count;
    }

    Ok(frames)
}

fn checked_v2_replay_frames(
    bytes: &[u8],
    expected_kind: u8,
) -> Result<Vec<ReplayFrame<'_>>, ValidationError> {
    let mut frames = Vec::new();
    let mut v2_offsets = Vec::new();
    let mut timestamp_segments = Vec::new();
    let mut cursor = 0_usize;
    let mut previous_segment = None::<(u64, u64, usize)>;
    while cursor < bytes.len() {
        let segment_start = cursor;
        let header_end = segment_start
            .checked_add(V2CodecProbe::HEADER_LEN)
            .filter(|end| *end <= bytes.len())
            .ok_or(ValidationError::Truncated {
                offset: segment_start,
            })?;
        let header = &bytes[segment_start..header_end];
        if !V2CodecProbe::header_is_valid(header)
            || V2CodecProbe::header_kind(header) != Some(expected_kind)
        {
            return Err(ValidationError::InvalidPayload {
                offset: segment_start,
            });
        }
        let segment_id = V2CodecProbe::header_segment_id(header).unwrap();
        let segment_base = V2CodecProbe::header_segment_base(header).unwrap();
        let base_bucket = V2CodecProbe::header_base_bucket(header).unwrap();
        if previous_segment.is_none() && (segment_id != 0 || segment_base != 0) {
            return Err(ValidationError::InvalidPayload {
                offset: segment_start,
            });
        }
        if let Some((previous_id, previous_base, previous_start)) = previous_segment {
            let expected_id =
                previous_id
                    .checked_add(1)
                    .ok_or(ValidationError::InvalidPayload {
                        offset: segment_start,
                    })?;
            let previous_len = u64::try_from(segment_start - previous_start).map_err(|_| {
                ValidationError::InvalidPayload {
                    offset: segment_start,
                }
            })?;
            let expected_base =
                previous_base
                    .checked_add(previous_len)
                    .ok_or(ValidationError::InvalidPayload {
                        offset: segment_start,
                    })?;
            if segment_id != expected_id || segment_base != expected_base {
                return Err(ValidationError::InvalidPayload {
                    offset: segment_start,
                });
            }
        }
        timestamp_segments.push((frames.len(), base_bucket, segment_start));
        previous_segment = Some((segment_id, segment_base, segment_start));
        cursor = header_end;

        while cursor < bytes.len() && !bytes[cursor..].starts_with(b"PIGWAL\r\n") {
            let offset = cursor;
            let fixed_header_end = offset
                .checked_add(54)
                .filter(|end| *end <= bytes.len())
                .ok_or(ValidationError::Truncated { offset })?;
            let payload_len_u64 = u64::from_le_bytes(
                bytes[offset + 6..offset + 14]
                    .try_into()
                    .expect("fixed V2 header bounds checked"),
            );
            let payload_len = usize::try_from(payload_len_u64)
                .map_err(|_| ValidationError::InvalidPayload { offset })?;
            let end = offset
                .checked_add(V2CodecProbe::EMPTY_RECORD_LEN)
                .and_then(|fixed| fixed.checked_add(payload_len))
                .filter(|end| *end <= bytes.len())
                .ok_or(ValidationError::Truncated { offset })?;
            let frame = &bytes[offset..end];
            let action = frame[3];
            let local_start = u64::try_from(offset - segment_start)
                .map_err(|_| ValidationError::InvalidPayload { offset })?;
            let global_start = segment_base
                .checked_add(local_start)
                .ok_or(ValidationError::InvalidPayload { offset })?;
            let structurally_valid = fixed_header_end <= bytes.len()
                && V2CodecProbe::record_marker_is_valid(frame)
                && V2CodecProbe::record_version_is_valid(frame)
                && V2CodecProbe::record_action_is_valid(frame)
                && V2CodecProbe::record_header_length_is_valid(frame)
                && V2CodecProbe::record_length_complement_is_valid(frame)
                && V2CodecProbe::record_physical_start_is_valid(frame, global_start)
                && V2CodecProbe::record_mutation_start_is_valid(frame, segment_base)
                && V2CodecProbe::record_index_count_are_valid(frame)
                && V2CodecProbe::record_timestamp_bucket(frame).is_some()
                && V2CodecProbe::payload_is_valid(
                    expected_kind,
                    action,
                    &frame[54..54 + payload_len],
                )
                && V2CodecProbe::record_crc_is_valid(frame);
            if !structurally_valid {
                return Err(ValidationError::InvalidPayload { offset });
            }
            frames.push(ReplayFrame {
                action,
                data: &frame[54..54 + payload_len],
                start_offset: offset,
                end_offset: end,
                group_index: u32::from_le_bytes(frame[38..42].try_into().unwrap()),
                group_count: u32::from_le_bytes(frame[42..46].try_into().unwrap()),
                timestamp_bucket: u64::from_le_bytes(frame[46..54].try_into().unwrap()),
            });
            v2_offsets.push((
                global_start,
                u64::from_le_bytes(frame[30..38].try_into().unwrap()),
            ));
            cursor = end;
        }
    }

    let mut group_start = 0;
    let mut previous_timestamp_bucket = timestamp_segments[0].1;
    let mut timestamp_segment_index = 1;
    while group_start < frames.len() {
        while timestamp_segments
            .get(timestamp_segment_index)
            .is_some_and(|(frame_index, _, _)| *frame_index == group_start)
        {
            let (_, base_bucket, segment_start) = timestamp_segments[timestamp_segment_index];
            if base_bucket != previous_timestamp_bucket {
                return Err(ValidationError::InvalidPayload {
                    offset: segment_start,
                });
            }
            timestamp_segment_index += 1;
        }
        let mutation_start = v2_offsets[group_start].1;
        let count = frames[group_start].group_count as usize;
        let timestamp_bucket = frames[group_start].timestamp_bucket;
        if mutation_start != v2_offsets[group_start].0
            || frames[group_start].group_index != 0
            || group_start + count > frames.len()
        {
            return Err(ValidationError::Truncated {
                offset: frames[group_start].start_offset,
            });
        }
        if let Some((frame_index, _, segment_start)) =
            timestamp_segments.get(timestamp_segment_index)
        {
            if *frame_index < group_start + count {
                return Err(ValidationError::InvalidPayload {
                    offset: *segment_start,
                });
            }
        }
        if timestamp_bucket < previous_timestamp_bucket {
            return Err(ValidationError::InvalidPayload {
                offset: frames[group_start].start_offset,
            });
        }
        for group_index in 0..count {
            if v2_offsets[group_start + group_index].1 != mutation_start
                || frames[group_start + group_index].group_index != group_index as u32
                || frames[group_start + group_index].group_count != count as u32
                || frames[group_start + group_index].timestamp_bucket != timestamp_bucket
            {
                return Err(ValidationError::InvalidPayload {
                    offset: frames[group_start + group_index].start_offset,
                });
            }
        }
        previous_timestamp_bucket = timestamp_bucket;
        group_start += count;
    }
    for (frame_index, base_bucket, segment_start) in &timestamp_segments[timestamp_segment_index..]
    {
        if *frame_index != frames.len() || *base_bucket != previous_timestamp_bucket {
            return Err(ValidationError::InvalidPayload {
                offset: *segment_start,
            });
        }
    }

    Ok(frames)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplaySnapshot<S> {
    pub(crate) snapshot: S,
    pub(crate) prefixes: Vec<S>,
    pub(crate) matched_target_prefix: bool,
    pub(crate) byte_len: u64,
    pub(crate) compacted_snapshot_prefix: bool,
    pub(crate) granularity_nanos: u64,
    pub(crate) last_bucket: u64,
}

fn initial_timestamp_state(bytes: &[u8]) -> (u64, u64) {
    if bytes.get(8..10) == Some(2_u16.to_le_bytes().as_slice())
        && bytes.len() >= V2CodecProbe::HEADER_LEN
    {
        (
            V2CodecProbe::header_granularity(bytes).unwrap(),
            V2CodecProbe::header_base_bucket(bytes).unwrap_or(0),
        )
    } else if bytes.starts_with(b"PIGWAL\r\n") && bytes.len() >= V1CodecProbe::HEADER_LEN {
        let granularity = V1CodecProbe::granularity(bytes).unwrap();
        let base_bucket = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        (granularity, base_bucket)
    } else {
        (60_000_000_000, 0)
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum TailReplay<S> {
    Complete(ReplaySnapshot<S>),
    RecoverableTail {
        replay: ReplaySnapshot<S>,
        tail_offset: usize,
        accepted_header: Option<Vec<u8>>,
    },
    Invalid(ValidationError),
}

pub(crate) fn replay_key_value_tail(bytes: &[u8]) -> TailReplay<KeyValueSnapshot> {
    replay_v1_tail(bytes, 1, replay_key_value)
}

pub(crate) fn replay_key_set_tail(bytes: &[u8]) -> TailReplay<KeySetSnapshot> {
    replay_v1_tail(bytes, 2, replay_key_set)
}

pub(crate) fn replay_key_map_tail(bytes: &[u8]) -> TailReplay<KeyMapSnapshot> {
    replay_v1_tail(bytes, 3, replay_key_map)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadOnlyReplayClassification {
    Complete,
    RecoverableTail,
}

fn classify_read_only<S>(
    replayed: TailReplay<S>,
) -> Result<ReadOnlyReplayClassification, ValidationError> {
    match replayed {
        TailReplay::Complete(_) => Ok(ReadOnlyReplayClassification::Complete),
        TailReplay::RecoverableTail { .. } => Ok(ReadOnlyReplayClassification::RecoverableTail),
        TailReplay::Invalid(error) => Err(error),
    }
}

pub(crate) fn classify_key_value_read_only(
    bytes: &[u8],
) -> Result<ReadOnlyReplayClassification, ValidationError> {
    classify_read_only(replay_key_value_tail(bytes))
}

pub(crate) fn classify_key_set_read_only(
    bytes: &[u8],
) -> Result<ReadOnlyReplayClassification, ValidationError> {
    classify_read_only(replay_key_set_tail(bytes))
}

pub(crate) fn classify_key_map_read_only(
    bytes: &[u8],
) -> Result<ReadOnlyReplayClassification, ValidationError> {
    classify_read_only(replay_key_map_tail(bytes))
}

fn replay_v1_tail<S>(
    bytes: &[u8],
    expected_kind: u8,
    replay: fn(&[u8]) -> Result<ReplaySnapshot<S>, ValidationError>,
) -> TailReplay<S> {
    if bytes.get(8..10) == Some(2_u16.to_le_bytes().as_slice()) {
        return replay_v2_tail(bytes, expected_kind, replay);
    }
    if let Some(offset) = terminal_fragment_start(bytes, expected_kind) {
        let accepted = match replay(&bytes[..offset]) {
            Ok(accepted) => Some((accepted, offset)),
            Err(ValidationError::Truncated {
                offset: group_start,
            }) if group_start < offset => complete_nonfinal_group_prefix_matches(
                &bytes[group_start..offset],
                group_start,
                expected_kind,
            )
            .filter(|group| {
                incomplete_group_member_matches(&bytes[offset..], offset, group_start, *group)
            })
            .and_then(|_| replay(&bytes[..group_start]).ok())
            .map(|accepted| (accepted, group_start)),
            Err(_) => None,
        };
        if let Some((accepted, tail_offset)) = accepted {
            return TailReplay::RecoverableTail {
                replay: accepted,
                tail_offset,
                accepted_header: bytes.get(..V1CodecProbe::HEADER_LEN).map(<[u8]>::to_vec),
            };
        }
    }

    match replay(bytes) {
        Ok(replay) => TailReplay::Complete(replay),
        Err(error @ ValidationError::Truncated { offset })
            if bytes.starts_with(b"PIGWAL\r\n")
                && (incomplete_v1_record_header_prefix_matches(
                    &bytes[offset..],
                    expected_kind,
                ) || incomplete_v1_payload_matches(
                    &bytes[offset..],
                    offset,
                    expected_kind,
                ) || incomplete_v1_footer_matches(&bytes[offset..], offset, expected_kind)
                    || complete_nonfinal_group_prefix_matches(
                        &bytes[offset..],
                        offset,
                        expected_kind,
                    )
                    .is_some()) =>
        {
            let (accepted, tail_offset) = match replay(&bytes[..offset]) {
                Ok(accepted) => (accepted, offset),
                Err(ValidationError::Truncated {
                    offset: group_start,
                }) if group_start < offset => {
                    let Some(group) = complete_nonfinal_group_prefix_matches(
                        &bytes[group_start..offset],
                        group_start,
                        expected_kind,
                    ) else {
                        return TailReplay::Invalid(error);
                    };
                    if !incomplete_group_member_matches(
                        &bytes[offset..],
                        offset,
                        group_start,
                        group,
                    ) {
                        return TailReplay::Invalid(error);
                    }
                    let Ok(accepted) = replay(&bytes[..group_start]) else {
                        return TailReplay::Invalid(error);
                    };
                    (accepted, group_start)
                }
                Err(_) => return TailReplay::Invalid(error),
            };
            TailReplay::RecoverableTail {
                replay: accepted,
                tail_offset,
                accepted_header: bytes.get(..V1CodecProbe::HEADER_LEN).map(<[u8]>::to_vec),
            }
        }
        Err(error) => TailReplay::Invalid(error),
    }
}

fn replay_v2_tail<S>(
    bytes: &[u8],
    expected_kind: u8,
    replay: fn(&[u8]) -> Result<ReplaySnapshot<S>, ValidationError>,
) -> TailReplay<S> {
    match replay(bytes) {
        Ok(replayed) => TailReplay::Complete(replayed),
        Err(error @ ValidationError::Truncated { offset })
            if bytes
                .get(..V2CodecProbe::HEADER_LEN)
                .is_some_and(V2CodecProbe::header_is_valid)
                && (incomplete_v2_record_matches(
                    &bytes[offset..],
                    offset,
                    bytes,
                    expected_kind,
                ) || complete_v2_group_prefix(
                    &bytes[offset..],
                    offset,
                    bytes,
                    expected_kind,
                )
                .is_some()) =>
        {
            match replay(&bytes[..offset]) {
                Ok(accepted) => TailReplay::RecoverableTail {
                    replay: accepted,
                    tail_offset: offset,
                    accepted_header: bytes.get(..V2CodecProbe::HEADER_LEN).map(<[u8]>::to_vec),
                },
                Err(ValidationError::Truncated {
                    offset: group_start,
                }) if group_start < offset => {
                    let Some(group) = complete_v2_group_prefix(
                        &bytes[group_start..offset],
                        group_start,
                        bytes,
                        expected_kind,
                    ) else {
                        return TailReplay::Invalid(error);
                    };
                    if !incomplete_v2_group_member_matches(&bytes[offset..], offset, bytes, group) {
                        return TailReplay::Invalid(error);
                    }
                    match replay(&bytes[..group_start]) {
                        Ok(accepted) => TailReplay::RecoverableTail {
                            replay: accepted,
                            tail_offset: group_start,
                            accepted_header: bytes
                                .get(..V2CodecProbe::HEADER_LEN)
                                .map(<[u8]>::to_vec),
                        },
                        Err(_) => TailReplay::Invalid(error),
                    }
                }
                Err(_) => TailReplay::Invalid(error),
            }
        }
        Err(error) => TailReplay::Invalid(error),
    }
}

fn incomplete_v2_record_matches(
    fragment: &[u8],
    offset: usize,
    bytes: &[u8],
    expected_kind: u8,
) -> bool {
    if fragment.is_empty() {
        return false;
    }
    let marker = [0xa7, 0xd1];
    let header_len = 54_u16.to_le_bytes();
    if !present_field_prefix_matches(fragment, 0, &marker)
        || !present_field_prefix_matches(fragment, 2, &[2])
        || fragment
            .get(3)
            .is_some_and(|action| !v2_action_matches_kind(expected_kind, *action))
        || !present_field_prefix_matches(fragment, 4, &header_len)
    {
        return false;
    }
    let Some(header) = bytes.get(..V2CodecProbe::HEADER_LEN) else {
        return false;
    };
    let Some(segment_base) = V2CodecProbe::header_segment_base(header) else {
        return false;
    };
    let Some(global_start) = segment_base.checked_add(offset as u64) else {
        return false;
    };
    if !present_field_prefix_matches(fragment, 22, &global_start.to_le_bytes()) {
        return false;
    }
    if let Some(length) = fragment
        .get(6..14)
        .and_then(|value| value.try_into().ok())
        .map(u64::from_le_bytes)
    {
        if !present_field_prefix_matches(fragment, 14, &(!length).to_le_bytes()) {
            return false;
        }
        let Ok(payload_len) = usize::try_from(length) else {
            return false;
        };
        let Some(complete_len) = V2CodecProbe::EMPTY_RECORD_LEN.checked_add(payload_len) else {
            return false;
        };
        if fragment.len() >= complete_len {
            return false;
        }
        if fragment.len() >= 54 + payload_len {
            let Some(action) = fragment.get(3).copied() else {
                return false;
            };
            if !V2CodecProbe::payload_is_valid(
                expected_kind,
                action,
                &fragment[54..54 + payload_len],
            ) {
                return false;
            }
            let available_footer = fragment.len().saturating_sub(54 + payload_len).min(8);
            if fragment[54 + payload_len..54 + payload_len + available_footer]
                != global_start.to_le_bytes()[..available_footer]
            {
                return false;
            }
        }
    }
    true
}

#[derive(Clone, Copy)]
struct OpenV2Group {
    mutation_start: u64,
    next_index: u32,
    count: u32,
    timestamp_bucket: u64,
}

fn complete_v2_group_prefix(
    prefix: &[u8],
    group_start: usize,
    bytes: &[u8],
    expected_kind: u8,
) -> Option<OpenV2Group> {
    let header = bytes.get(..V2CodecProbe::HEADER_LEN)?;
    let segment_base = V2CodecProbe::header_segment_base(header)?;
    let mutation_start = segment_base.checked_add(group_start as u64)?;
    let mut cursor = 0_usize;
    let mut next_index = 0_u32;
    let mut declared_count = None;
    let mut timestamp_bucket = None;
    while cursor < prefix.len() {
        let frame_start = group_start.checked_add(cursor)?;
        let frame_tail = &prefix[cursor..];
        let payload_len = frame_tail
            .get(6..14)
            .and_then(|value| value.try_into().ok())
            .map(u64::from_le_bytes)
            .and_then(|value| usize::try_from(value).ok())?;
        let frame_len = V2CodecProbe::EMPTY_RECORD_LEN.checked_add(payload_len)?;
        let frame = frame_tail.get(..frame_len)?;
        let global_start = segment_base.checked_add(frame_start as u64)?;
        let count = u32::from_le_bytes(frame.get(42..46)?.try_into().ok()?);
        let timestamp = u64::from_le_bytes(frame.get(46..54)?.try_into().ok()?);
        if !V2CodecProbe::record_marker_is_valid(frame)
            || !V2CodecProbe::record_version_is_valid(frame)
            || !V2CodecProbe::record_action_is_valid(frame)
            || !V2CodecProbe::record_header_length_is_valid(frame)
            || !V2CodecProbe::record_length_complement_is_valid(frame)
            || !V2CodecProbe::record_physical_start_is_valid(frame, global_start)
            || u64::from_le_bytes(frame.get(30..38)?.try_into().ok()?) != mutation_start
            || u32::from_le_bytes(frame.get(38..42)?.try_into().ok()?) != next_index
            || declared_count.is_some_and(|declared| declared != count)
            || timestamp_bucket.is_some_and(|declared| declared != timestamp)
            || !V2CodecProbe::payload_is_valid(
                expected_kind,
                frame[3],
                &frame[54..54 + payload_len],
            )
            || !V2CodecProbe::record_crc_is_valid(frame)
        {
            return None;
        }
        declared_count = Some(count);
        timestamp_bucket = Some(timestamp);
        next_index = next_index.checked_add(1)?;
        cursor = cursor.checked_add(frame_len)?;
    }
    let count = declared_count?;
    (next_index > 0 && next_index < count).then_some(OpenV2Group {
        mutation_start,
        next_index,
        count,
        timestamp_bucket: timestamp_bucket?,
    })
}

fn incomplete_v2_group_member_matches(
    fragment: &[u8],
    physical_start: usize,
    bytes: &[u8],
    group: OpenV2Group,
) -> bool {
    let Some(header) = bytes.get(..V2CodecProbe::HEADER_LEN) else {
        return false;
    };
    let Some(segment_base) = V2CodecProbe::header_segment_base(header) else {
        return false;
    };
    let Some(physical_start) = segment_base.checked_add(physical_start as u64) else {
        return false;
    };
    group.next_index < group.count
        && present_field_prefix_matches(fragment, 22, &physical_start.to_le_bytes())
        && present_field_prefix_matches(fragment, 30, &group.mutation_start.to_le_bytes())
        && present_field_prefix_matches(fragment, 38, &group.next_index.to_le_bytes())
        && present_field_prefix_matches(fragment, 42, &group.count.to_le_bytes())
        && present_field_prefix_matches(fragment, 46, &group.timestamp_bucket.to_le_bytes())
}

fn terminal_fragment_start(bytes: &[u8], expected_kind: u8) -> Option<usize> {
    if bytes.len() <= V1CodecProbe::HEADER_LEN
        || !bytes.starts_with(b"PIGWAL\r\n")
        || !V1CodecProbe::header_crc_is_valid(bytes)
    {
        return None;
    }
    (V1CodecProbe::HEADER_LEN..bytes.len())
        .rev()
        .find(|offset| {
            let tail = &bytes[*offset..];
            incomplete_v1_record_header_prefix_matches(tail, expected_kind)
                || incomplete_v1_payload_matches(tail, *offset, expected_kind)
                || incomplete_v1_footer_matches(tail, *offset, expected_kind)
                || complete_nonfinal_group_prefix_matches(tail, *offset, expected_kind).is_some()
        })
}

#[derive(Clone, Copy)]
struct OpenGroup {
    next_index: u32,
    count: u32,
    timestamp_bucket: u64,
}

fn complete_nonfinal_group_prefix_matches(
    tail: &[u8],
    group_start: usize,
    expected_kind: u8,
) -> Option<OpenGroup> {
    let Ok(group_start_u32) = u32::try_from(group_start) else {
        return None;
    };
    let mut cursor = 0;
    let mut member_index = 0_u32;
    let mut declared_count = None;
    let mut timestamp_bucket = None;

    while cursor < tail.len() {
        let frame_start = group_start.checked_add(cursor)?;
        let Ok(frame_start_u32) = u32::try_from(frame_start) else {
            return None;
        };
        let frame_tail = &tail[cursor..];
        if frame_tail.len() < 38
            || !complete_v1_record_header_is_valid(frame_tail, frame_start, expected_kind)
        {
            return None;
        }
        let payload_len = u32::from_le_bytes(frame_tail[6..10].try_into().unwrap()) as usize;
        let frame_len = match V1CodecProbe::EMPTY_RECORD_LEN.checked_add(payload_len) {
            Some(len) if len <= frame_tail.len() => len,
            _ => return None,
        };
        let frame = &frame_tail[..frame_len];
        let count = u32::from_le_bytes(frame[26..30].try_into().unwrap());
        let timestamp = u64::from_le_bytes(frame[30..38].try_into().unwrap());
        if u32::from_le_bytes(frame[18..22].try_into().unwrap()) != group_start_u32
            || u32::from_le_bytes(frame[22..26].try_into().unwrap()) != member_index
            || declared_count.is_some_and(|declared| declared != count)
            || timestamp_bucket.is_some_and(|declared| declared != timestamp)
            || !V1CodecProbe::payload_is_valid(
                expected_kind,
                frame[3],
                &frame[38..38 + payload_len],
            )
            || !V1CodecProbe::record_physical_start_is_valid(frame, frame_start_u32)
            || !V1CodecProbe::record_crc_is_valid(frame)
        {
            return None;
        }
        declared_count = Some(count);
        timestamp_bucket = Some(timestamp);
        member_index += 1;
        cursor += frame_len;
    }

    let count = declared_count?;
    (member_index > 0 && member_index < count).then_some(OpenGroup {
        next_index: member_index,
        count,
        timestamp_bucket: timestamp_bucket?,
    })
}

fn incomplete_group_member_matches(
    fragment: &[u8],
    physical_start: usize,
    group_start: usize,
    group: OpenGroup,
) -> bool {
    let (Ok(physical_start), Ok(group_start)) =
        (u32::try_from(physical_start), u32::try_from(group_start))
    else {
        return false;
    };
    if group.next_index >= group.count
        || !present_field_prefix_matches(fragment, 14, &physical_start.to_le_bytes())
        || !present_field_prefix_matches(fragment, 18, &group_start.to_le_bytes())
        || !present_field_prefix_matches(fragment, 22, &group.next_index.to_le_bytes())
        || !present_field_prefix_matches(fragment, 26, &group.count.to_le_bytes())
        || !present_field_prefix_matches(fragment, 30, &group.timestamp_bucket.to_le_bytes())
    {
        return false;
    }
    if let Some(length) = fragment
        .get(6..10)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
    {
        present_field_prefix_matches(fragment, 10, &(!length).to_le_bytes())
    } else {
        true
    }
}

fn present_field_prefix_matches(fragment: &[u8], start: usize, expected: &[u8]) -> bool {
    if fragment.len() <= start {
        return true;
    }
    let present = (fragment.len() - start).min(expected.len());
    fragment[start..start + present] == expected[..present]
}

fn incomplete_v1_footer_matches(tail: &[u8], offset: usize, expected_kind: u8) -> bool {
    if tail.len() < 38 || !complete_v1_record_header_is_valid(tail, offset, expected_kind) {
        return false;
    }
    let payload_len = u32::from_le_bytes(tail[6..10].try_into().unwrap()) as usize;
    let Some(payload_end) = 38_usize.checked_add(payload_len) else {
        return false;
    };
    let Some(record_end) = payload_end.checked_add(8) else {
        return false;
    };
    if tail.len() < payload_end || tail.len() >= record_end {
        return false;
    }
    let action = tail[3];
    if !V1CodecProbe::payload_is_valid(expected_kind, action, &tail[38..payload_end]) {
        return false;
    }
    let expected_footer = match u32::try_from(offset) {
        Ok(offset) => offset.to_le_bytes(),
        Err(_) => return false,
    };
    let available_footer = tail.len().saturating_sub(payload_end).min(4);
    tail[payload_end..payload_end + available_footer] == expected_footer[..available_footer]
}

fn incomplete_v1_payload_matches(tail: &[u8], offset: usize, expected_kind: u8) -> bool {
    if tail.len() < 38 || !complete_v1_record_header_is_valid(tail, offset, expected_kind) {
        return false;
    }
    let payload_len = u32::from_le_bytes(tail[6..10].try_into().unwrap()) as usize;
    38_usize
        .checked_add(payload_len)
        .is_some_and(|payload_end| tail.len() < payload_end)
}

fn complete_v1_record_header_is_valid(tail: &[u8], offset: usize, expected_kind: u8) -> bool {
    let action_matches_kind = tail
        .get(3)
        .is_some_and(|action| action_matches_kind(expected_kind, *action));
    V1CodecProbe::record_marker_is_valid(tail)
        && V1CodecProbe::record_version_is_valid(tail)
        && action_matches_kind
        && V1CodecProbe::record_header_length_is_valid(tail)
        && V1CodecProbe::record_length_complement_is_valid(tail)
        && tail
            .get(14..18)
            .and_then(|value| value.try_into().ok())
            .map(u32::from_le_bytes)
            == u32::try_from(offset).ok()
        && V1CodecProbe::record_mutation_start_is_valid(tail)
        && V1CodecProbe::record_index_count_are_valid(tail)
        && V1CodecProbe::record_timestamp_bucket(tail).is_some()
}

fn incomplete_v1_record_header_prefix_matches(tail: &[u8], expected_kind: u8) -> bool {
    if tail.is_empty() || tail.len() >= 38 {
        return false;
    }
    let header_len = 38_u16.to_le_bytes();
    tail.first().is_none_or(|byte| *byte == 0xa7)
        && tail.get(1).is_none_or(|byte| *byte == 0xd1)
        && tail.get(2).is_none_or(|byte| *byte == 1)
        && tail
            .get(3)
            .is_none_or(|action| action_matches_kind(expected_kind, *action))
        && tail.get(4).is_none_or(|byte| *byte == header_len[0])
        && tail.get(5).is_none_or(|byte| *byte == header_len[1])
}

fn action_matches_kind(expected_kind: u8, action: u8) -> bool {
    match expected_kind {
        1 => matches!(action, DELETE_ACT | PUT_ACT),
        2 => matches!(action, DELETE_ACT | SET_APPEND_ACT | SET_REMOVE_ACT),
        3 => matches!(action, DELETE_ACT | MAP_PUT_ACT | MAP_REMOVE_ACT),
        _ => false,
    }
}

fn v2_action_matches_kind(expected_kind: u8, action: u8) -> bool {
    action_matches_kind(expected_kind, action)
        || (expected_kind == 3 && matches!(action, MAP_PUT_V2_ACT | MAP_REMOVE_V2_ACT))
}

pub(crate) fn replay_key_value(
    bytes: &[u8],
) -> Result<ReplaySnapshot<KeyValueSnapshot>, ValidationError> {
    replay_key_value_with_target(bytes, None)
}

pub(crate) fn replay_key_value_against(
    bytes: &[u8],
    target: &KeyValueSnapshot,
) -> Result<ReplaySnapshot<KeyValueSnapshot>, ValidationError> {
    replay_key_value_with_target(bytes, Some(target))
}

fn replay_key_value_with_target(
    bytes: &[u8],
    target: Option<&KeyValueSnapshot>,
) -> Result<ReplaySnapshot<KeyValueSnapshot>, ValidationError> {
    let mut snapshot = HashMap::new();
    let mut matched_target_prefix = false;
    let mut compacted_snapshot_prefix = true;
    let mut snapshot_keys = HashSet::new();
    let (granularity_nanos, mut last_bucket) = initial_timestamp_state(bytes);

    for frame in checked_replay_frames(bytes, 1)? {
        last_bucket = last_bucket.max(frame.timestamp_bucket());
        match frame.action() {
            PUT_ACT => {
                let action: KeyValueData = bincode::deserialize(frame.data()).map_err(|_| {
                    ValidationError::InvalidPayload {
                        offset: frame.start_offset(),
                    }
                })?;
                let (key, value) = action.owned_key_value();
                compacted_snapshot_prefix &= snapshot_keys.insert(key.clone());
                snapshot.insert(key, value);
            }
            DELETE_ACT => {
                compacted_snapshot_prefix = false;
                snapshot.remove(frame.data());
            }
            _ => {
                return Err(ValidationError::InvalidPayload {
                    offset: frame.start_offset(),
                });
            }
        }
        if frame.is_group_end() {
            matched_target_prefix |= target.is_some_and(|target| &snapshot == target);
        }
    }

    Ok(ReplaySnapshot {
        snapshot,
        prefixes: Vec::new(),
        matched_target_prefix,
        byte_len: bytes.len() as u64,
        compacted_snapshot_prefix,
        granularity_nanos,
        last_bucket,
    })
}

fn append_action(bytes: &mut Vec<u8>, action: &StoredAction) {
    bytes.extend_from_slice(&action.act_type().to_ne_bytes());
    bytes.extend_from_slice(&action.crc().to_ne_bytes());
    let data_size =
        u32::try_from(action.data_size()).expect("legacy snapshot payload must fit u32");
    bytes.extend_from_slice(&data_size.to_ne_bytes());
    bytes.extend_from_slice(action.data());
    bytes.extend_from_slice(&action.start_offset().to_ne_bytes());
}

pub(crate) fn encode_key_value_snapshot(snapshot: &HashMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut entries = snapshot.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| *key);
    let mut bytes = Vec::new();
    for (key, value) in entries {
        let action = StoredAction::put_action(
            &(bytes.len() as u32),
            &KeyValueData::new(key.clone(), value.clone()),
        );
        append_action(&mut bytes, &action);
    }
    bytes
}

pub(crate) fn key_value_is_proper_snapshot_prefix(
    active: &HashMap<Vec<u8>, Vec<u8>>,
    legacy: &HashMap<Vec<u8>, Vec<u8>>,
) -> bool {
    active.len() < legacy.len()
        && active
            .iter()
            .all(|(key, value)| legacy.get(key) == Some(value))
}

pub(crate) fn replay_key_set(
    bytes: &[u8],
) -> Result<ReplaySnapshot<KeySetSnapshot>, ValidationError> {
    replay_key_set_with_target(bytes, None)
}

pub(crate) fn replay_key_set_against(
    bytes: &[u8],
    target: &KeySetSnapshot,
) -> Result<ReplaySnapshot<KeySetSnapshot>, ValidationError> {
    replay_key_set_with_target(bytes, Some(target))
}

fn replay_key_set_with_target(
    bytes: &[u8],
    target: Option<&KeySetSnapshot>,
) -> Result<ReplaySnapshot<KeySetSnapshot>, ValidationError> {
    let mut snapshot: HashMap<Vec<u8>, HashSet<Vec<u8>>> = HashMap::new();
    let mut matched_target_prefix = false;
    let mut compacted_snapshot_prefix = true;
    let mut snapshot_entries = HashSet::new();
    let (granularity_nanos, mut last_bucket) = initial_timestamp_state(bytes);

    for frame in checked_replay_frames(bytes, 2)? {
        last_bucket = last_bucket.max(frame.timestamp_bucket());
        match frame.action() {
            SET_APPEND_ACT => {
                let action: KeyValueData = bincode::deserialize(frame.data()).map_err(|_| {
                    ValidationError::InvalidPayload {
                        offset: frame.start_offset(),
                    }
                })?;
                let (key, value) = action.owned_key_value();
                compacted_snapshot_prefix &= snapshot_entries.insert((key.clone(), value.clone()));
                snapshot.entry(key).or_default().insert(value);
            }
            SET_REMOVE_ACT => {
                compacted_snapshot_prefix = false;
                let action: KeyValueData = bincode::deserialize(frame.data()).map_err(|_| {
                    ValidationError::InvalidPayload {
                        offset: frame.start_offset(),
                    }
                })?;
                let (key, value) = action.owned_key_value();
                if let Some(set) = snapshot.get_mut(&key) {
                    set.remove(&value);
                }
            }
            DELETE_ACT => {
                compacted_snapshot_prefix = false;
                snapshot.remove(frame.data());
            }
            _ => {
                return Err(ValidationError::InvalidPayload {
                    offset: frame.start_offset(),
                });
            }
        }
        if frame.is_group_end() {
            matched_target_prefix |= target.is_some_and(|target| &snapshot == target);
        }
    }

    Ok(ReplaySnapshot {
        snapshot,
        prefixes: Vec::new(),
        matched_target_prefix,
        byte_len: bytes.len() as u64,
        compacted_snapshot_prefix,
        granularity_nanos,
        last_bucket,
    })
}

pub(crate) fn encode_key_set_snapshot(snapshot: &HashMap<Vec<u8>, HashSet<Vec<u8>>>) -> Vec<u8> {
    let mut keys = snapshot.keys().collect::<Vec<_>>();
    keys.sort();
    let mut bytes = Vec::new();
    for key in keys {
        let mut values = snapshot[key].iter().collect::<Vec<_>>();
        values.sort();
        for value in values {
            let action = StoredAction::append_to_set(
                &(bytes.len() as u32),
                &KeyValueData::new(key.clone(), value.clone()),
            );
            append_action(&mut bytes, &action);
        }
    }
    bytes
}

pub(crate) fn key_set_is_proper_snapshot_prefix(
    active: &HashMap<Vec<u8>, HashSet<Vec<u8>>>,
    legacy: &HashMap<Vec<u8>, HashSet<Vec<u8>>>,
) -> bool {
    let active_len: usize = active.values().map(HashSet::len).sum();
    let legacy_len: usize = legacy.values().map(HashSet::len).sum();
    active_len < legacy_len
        && active.iter().all(|(key, values)| {
            legacy
                .get(key)
                .is_some_and(|legacy_values| values.is_subset(legacy_values))
        })
}

pub(crate) fn replay_key_map(
    bytes: &[u8],
) -> Result<ReplaySnapshot<KeyMapSnapshot>, ValidationError> {
    replay_key_map_with_target(bytes, None)
}

pub(crate) fn replay_key_map_against(
    bytes: &[u8],
    target: &KeyMapSnapshot,
) -> Result<ReplaySnapshot<KeyMapSnapshot>, ValidationError> {
    replay_key_map_with_target(bytes, Some(target))
}

fn replay_key_map_with_target(
    bytes: &[u8],
    target: Option<&KeyMapSnapshot>,
) -> Result<ReplaySnapshot<KeyMapSnapshot>, ValidationError> {
    let mut snapshot: HashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>> = HashMap::new();
    let mut matched_target_prefix = false;
    let mut compacted_snapshot_prefix = true;
    let mut snapshot_entries = BTreeSet::new();
    let (granularity_nanos, mut last_bucket) = initial_timestamp_state(bytes);

    for frame in checked_replay_frames(bytes, 3)? {
        last_bucket = last_bucket.max(frame.timestamp_bucket());
        match frame.action() {
            MAP_PUT_ACT | MAP_PUT_V2_ACT => {
                let action = if frame.action() == MAP_PUT_ACT {
                    decode_historical_sorted_map_entry(frame.data())
                } else {
                    decode_current_sorted_map_entry(frame.data())
                }
                .map_err(|_| ValidationError::InvalidPayload {
                    offset: frame.start_offset(),
                })?;
                let (key, search_key, value) = action.entry();
                compacted_snapshot_prefix &=
                    snapshot_entries.insert((key.clone(), search_key.clone()));
                snapshot.entry(key).or_default().insert(search_key, value);
            }
            MAP_REMOVE_ACT | MAP_REMOVE_V2_ACT => {
                compacted_snapshot_prefix = false;
                let action = if frame.action() == MAP_REMOVE_ACT {
                    decode_historical_sorted_map_key(frame.data())
                } else {
                    decode_current_sorted_map_key(frame.data())
                }
                .map_err(|_| ValidationError::InvalidPayload {
                    offset: frame.start_offset(),
                })?;
                let (key, search_key) = action.owned();
                if let Some(map) = snapshot.get_mut(&key) {
                    map.remove(&search_key);
                }
            }
            DELETE_ACT => {
                compacted_snapshot_prefix = false;
                snapshot.remove(frame.data());
            }
            _ => {
                return Err(ValidationError::InvalidPayload {
                    offset: frame.start_offset(),
                });
            }
        }
        if frame.is_group_end() {
            matched_target_prefix |= target.is_some_and(|target| &snapshot == target);
        }
    }

    Ok(ReplaySnapshot {
        snapshot,
        prefixes: Vec::new(),
        matched_target_prefix,
        byte_len: bytes.len() as u64,
        compacted_snapshot_prefix,
        granularity_nanos,
        last_bucket,
    })
}

pub(crate) fn encode_key_map_snapshot(
    snapshot: &HashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>,
) -> Vec<u8> {
    let mut keys = snapshot.keys().collect::<Vec<_>>();
    keys.sort();
    let mut bytes = Vec::new();
    for key in keys {
        for (search_key, value) in &snapshot[key] {
            let action = StoredAction::put_to_sorted_map(
                &(bytes.len() as u32),
                &SortedMapEntry::new(key.clone(), search_key.clone(), value.clone()),
            );
            append_action(&mut bytes, &action);
        }
    }
    bytes
}

pub(crate) fn key_map_is_proper_snapshot_prefix(
    active: &HashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>,
    legacy: &HashMap<Vec<u8>, BTreeMap<SearchKey, Vec<u8>>>,
) -> bool {
    let active_len: usize = active.values().map(BTreeMap::len).sum();
    let legacy_len: usize = legacy.values().map(BTreeMap::len).sum();
    active_len < legacy_len
        && active.iter().all(|(key, map)| {
            legacy.get(key).is_some_and(|legacy_map| {
                map.iter()
                    .all(|(search_key, value)| legacy_map.get(search_key) == Some(value))
            })
        })
}

#[cfg(test)]
mod tests {
    use super::{append_action, replay_key_value, CheckedFrames, ValidationError};
    use crate::wal::format::{V2CodecProbe, V2HeaderProbeFields, V2RecordProbeFields};
    use crate::wal::model::{KeyValueData, StoredAction};

    #[test]
    fn complete_v2_key_value_record_replays() {
        let mut bytes = V2CodecProbe::encode_header(V2HeaderProbeFields {
            kind: 1,
            granularity_nanos: 60_000_000_000,
            base_bucket: 0,
            segment_id: 0,
            segment_base: 0,
        })
        .to_vec();
        let payload =
            bincode::serialize(&KeyValueData::new(b"key".to_vec(), b"value".to_vec())).unwrap();
        bytes.extend_from_slice(&V2CodecProbe::encode_complete_record(V2RecordProbeFields {
            action: 1,
            payload: &payload,
            physical_start: V2CodecProbe::HEADER_LEN as u64,
            mutation_start: V2CodecProbe::HEADER_LEN as u64,
            index: 0,
            count: 1,
            timestamp_bucket: 0,
        }));

        let replay = replay_key_value(&bytes).expect("complete V2 record must replay");
        assert_eq!(
            replay.snapshot.get(b"key".as_slice()),
            Some(&b"value".to_vec())
        );
    }

    #[test]
    fn complete_replay_does_not_retain_a_snapshot_per_frame() {
        let mut bytes = Vec::new();
        for index in 0_u64..32 {
            let action = StoredAction::put_action(
                &(bytes.len() as u32),
                &KeyValueData::new(index.to_le_bytes().to_vec(), vec![index as u8]),
            );
            append_action(&mut bytes, &action);
        }

        let replay = replay_key_value(&bytes).expect("complete WAL should replay");

        assert!(
            replay.prefixes.is_empty(),
            "complete replay must not retain full logical snapshots for every frame"
        );
    }

    #[test]
    fn checked_frames_accept_valid_legacy_wal_and_reject_truncation() {
        let bytes = include_bytes!("../../tests/fixtures/legacy/kv.wal.dat");
        let frames = CheckedFrames::new(bytes)
            .collect::<Result<Vec<_>, _>>()
            .expect("frozen pre-feature WAL should validate");
        assert_eq!(frames.len(), 5);
        assert_eq!(frames.last().unwrap().end_offset(), bytes.len());

        let truncated = &bytes[..bytes.len() - 1];
        let error = CheckedFrames::new(truncated)
            .collect::<Result<Vec<_>, _>>()
            .expect_err("truncated footer must be rejected without panicking");
        assert!(matches!(error, ValidationError::Truncated { .. }));
    }
}
