# Contract: WAL V1 Binary Grammar

All V1 integers are little-endian. Legacy grammar remains native-endian and is
selected only when byte zero is a legacy action (0–5) or the artifact is empty.
No artifact mixes grammars.

## File Header (40 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic `PIGWAL\r\n` |
| 8 | 2 | Version `1` |
| 10 | 2 | Header length `40` |
| 12 | 1 | Store kind: value `1`, set `2`, map `3` |
| 13 | 1 | Timestamp unit: Unix nanoseconds `1` |
| 14 | 2 | Flags, zero |
| 16 | 8 | Timestamp granularity, nonzero nanoseconds |
| 24 | 8 | Base accepted timestamp bucket |
| 32 | 4 | Reserved, zero |
| 36 | 4 | CRC32 over bytes `0..36` |

Magic begins with `0x50`, outside all legacy action values. A strict prefix of
the magic/header is invalid and preserved; it is never reconstructed or treated
as an empty database. A missing file follows
[fresh-v1-publication.md](fresh-v1-publication.md): active remains absent until a
complete persisted 40-byte header is strictly validated and atomically published.
New-file publication never exposes a partial active header.

## Physical Record (`46 + payload_len` bytes)

| Relative offset | Size | Field |
|---:|---:|---|
| 0 | 2 | Marker bytes `A7 D1` |
| 2 | 1 | Record version `1` |
| 3 | 1 | Existing action `0..5` |
| 4 | 2 | Fixed record-header length `38` |
| 6 | 4 | Payload length `N` |
| 10 | 4 | Bitwise complement of `N` |
| 14 | 4 | Physical record start offset |
| 18 | 4 | Logical mutation start offset |
| 22 | 4 | Zero-based mutation index |
| 26 | 4 | Nonzero mutation record count |
| 30 | 8 | Timestamp bucket, Unix nanoseconds |
| 38 | N | Existing action payload |
| 38+N | 4 | Repeated physical start offset |
| 42+N | 4 | CRC32 over bytes `0..42+N` |

Offsets include the 40-byte header and remain `u32`. Encode and replay use checked
arithmetic; exceeding `u32::MAX` is an explicit error within issue #9's existing
format limit.

## Mutation Invariants

- Single action: mutation start equals physical start, index 0, count 1.
- Multi-action: common mutation start/count/timestamp; consecutive physical
  records; indices exactly `0..count-1`.
- Replay buffers payload effects until member `count-1` validates.
- After a valid complete file header, EOF before the final member, between members,
  or inside a constant-matching next action record is a recoverable tail at the
  mutation start.
- A complete field contradiction, length-complement mismatch, invalid payload,
  offset/group/timestamp violation, or CRC mismatch is corruption.
- CRC32 detects accidental corruption; it is not authentication.

## Legacy and Migration Boundary

- Byte zero selects legacy only when it is action `0..5` or the existing artifact
  is zero bytes; the grammar never changes within a file.
- Complete legacy uses existing native-endian fields, payload-only CRC, action
  decoders, and frozen fixtures, but normal startup returns `MigrationRequired`
  without modifying or appending to it.
- Zero-byte existing files are complete empty legacy and also require migration;
  a missing file stages, validates, and atomically publishes a complete V1 header.
- Truncated/corrupt legacy is `InvalidArtifact`, never auto-shortened or migrated.
- The offline CLI writes a new wholly V1 destination. Migrated snapshots use base
  bucket zero and the selected/default granularity.
- Legacy native endian has no marker; migration never guesses alternate endian.
- Legacy bytes are never interpreted with V1 endian or full-envelope CRC rules.
