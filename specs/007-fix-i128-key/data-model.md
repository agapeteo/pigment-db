# Data Model: Full-Range Signed I128 Keys

## Public Key

- `I128` payload: signed integer in `[−2^127, 2^127−1]`.
- Comparison: derived signed numeric comparison within the existing enum-variant order.
- Logical size: 16 bytes.
- Serialization outside the managed WAL: current public signed representation; callers own migration of unversioned external blobs.

## Historical Wire Key

- Private persisted-only model with the original variant order.
- Historical `I128` payload: unsigned integer in `[0, 2^64−1]`.
- Valid only under legacy/V1 map actions `4`/`5` and earlier-V2 map actions `4`/`5`.
- Transition: `HistoricalKey::I128(value)` becomes `Key::I128(i128::from(value))`; every other variant maps value-for-value.

## Current Wire Key

- Uses the current public `Key`/`SearchKey` representation.
- Valid only under V2 map actions `6`/`7`.
- `I128` occupies 16 payload bytes and preserves the complete signed domain.

## Sorted-Map Record States

| Enclosing format | Action | Payload model | Result |
|------------------|--------|---------------|--------|
| Legacy | 4 / 5 | Historical put / remove | Normalize, then apply |
| V1 | 4 / 5 | Historical put / remove | Normalize, then apply |
| Earlier V2 | 4 / 5 | Historical put / remove | Normalize, then apply |
| Current V2 | 6 / 7 | Current signed put / remove | Apply directly |
| Any other pairing | Any | Mismatched/unknown | Reject before applying |

## State Transitions

1. **Decode historical**: validate frame → deserialize historical payload → widen all historical `I128` components → apply public mutation.
2. **Decode current**: validate frame/action → deserialize current payload → apply public mutation.
3. **Write runtime mutation**: serialize current public payload once → map put/remove to current V2 action → append atomically under existing WAL authority.
4. **Migrate/compact**: capture and validate source → normalize replay snapshot → serialize all map entries with current V2 put action → validate destination → confirm source unchanged.

## Invariants

- An action identifier selects exactly one payload model.
- No historical value is narrowed, sign-extended, or bit-reinterpreted.
- Replay never mutates public state until the selected payload fully deserializes and normalizes.
- Mixed V2 record ordering is determined solely by validated frame order.
- Current writers never emit historical map action identifiers.
