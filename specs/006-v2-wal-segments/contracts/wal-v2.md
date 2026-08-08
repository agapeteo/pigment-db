# Contract: WAL V2 Binary Grammar and Segment Chain

All integer fields are little-endian. Header and record offsets below are byte offsets from the start of that structure.

## Segment header: 64 bytes

| Range | Width | Field | Validation |
|---|---:|---|---|
| `0..8` | 8 | Magic | `PIGWAL\r\n` |
| `8..10` | 2 | Version | `2` |
| `10..12` | 2 | Header length | `64` |
| `12` | 1 | Store kind | `1..=3` |
| `13` | 1 | Timestamp unit | Unix nanoseconds (`1`) |
| `14..16` | 2 | Flags | `0` |
| `16..24` | 8 | Granularity | Nonzero |
| `24..32` | 8 | Base bucket | Last accepted bucket at segment creation |
| `32..40` | 8 | Segment id | Consecutive within chain |
| `40..48` | 8 | Segment base | Previous base plus previous segment length |
| `48..60` | 12 | Reserved | All zero |
| `60..64` | 4 | CRC32 | CRC32 of bytes `0..60` |

## Record: 66 bytes plus payload

| Range | Width | Field | Validation |
|---|---:|---|---|
| `0..2` | 2 | Marker | `a7 d1` |
| `2` | 1 | Record version | `2` |
| `3` | 1 | Action | Existing action `0..=5`, valid for store family |
| `4..6` | 2 | Fixed header length | `54` |
| `6..14` | 8 | Payload length | Checked conversion/addition |
| `14..22` | 8 | Length complement | Bitwise complement of payload length |
| `22..30` | 8 | Physical start | `segment_base + local_record_offset` |
| `30..38` | 8 | Mutation start | First group member's physical start |
| `38..42` | 4 | Group index | Starts at zero, less than count |
| `42..46` | 4 | Group count | Nonzero and identical across group |
| `46..54` | 8 | Timestamp bucket | Identical across group, monotonic by accepted group |
| `54..54+L` | `L` | Payload | Valid for family/action |
| `54+L..62+L` | 8 | Footer start | Duplicate physical start |
| `62+L..66+L` | 4 | CRC32 | CRC32 of all preceding record bytes |

## Atomicity and recovery

- Rotation occurs before a complete logical mutation; one group never crosses segment files.
- A terminal partial single record is discarded.
- A terminal partial or complete-nonfinal group prefix causes the entire group to be discarded.
- Earlier corruption is not hidden by a later terminal fragment.
- A malformed identifier/base chain is invalid and is preserved for diagnosis.
- A single oversized mutation is valid in an otherwise empty segment.

## Naming

- Active: `kv.wal.dat`, `set.wal.dat`, or `map.wal.dat`.
- Sealed: `<active>.segment-<20-digit-zero-padded-id>`.
- Staging: `.<active>.next`.
- Recovery backup: `.<active>` during staged tail repair only.
