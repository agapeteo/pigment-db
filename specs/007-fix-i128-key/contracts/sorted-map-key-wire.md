# Persisted Contract: Versioned Sorted-Map Key Payloads

## Action registry

| Action | Store family | Meaning | Payload contract |
|--------|--------------|---------|------------------|
| `4` | Sorted map | Historical put | Historical entry with `I128(u64)` |
| `5` | Sorted map | Historical remove | Historical key with `I128(u64)` |
| `6` | Sorted map | Current put | Current entry with `I128(i128)` |
| `7` | Sorted map | Current remove | Current key with `I128(i128)` |

Actions `0..=3` retain their existing meanings. Actions `6` and `7` are invalid for legacy and V1 records and for key/value or key/set V2 records.

## Historical payload contract

- Enum discriminants, variant order, sequence lengths, field order, and scalar widths match the frozen pre-correction model.
- `I128` is enum discriminant `10` followed by exactly one unsigned 64-bit payload.
- Deserialization produces a private historical model. Conversion widens the payload numerically to a public signed 128-bit value.

## Current payload contract

- `I128` remains enum discriminant `10` but is followed by exactly one signed 128-bit payload.
- New runtime V2 put/remove/compute records use actions `6`/`7`.
- Offline migration and V2 compaction output uses current put action `6` for every snapshot entry.

## Validation

1. The V2 frame action must be valid for its store family.
2. Action `4`/`5` payloads must deserialize completely under the historical model.
3. Action `6`/`7` payloads must deserialize completely under the current model.
4. Trailing bytes, truncation, unknown actions, and mismatched payload widths are invalid.
5. A frame is applied only after the complete selected payload passes validation.

## Mixed-history replay

A valid V2 chain may contain actions `4`/`5` from an earlier writer and actions `6`/`7` from a current writer. Replay processes validated frames in physical mutation order and normalizes historical keys before applying them. Put/remove precedence is therefore identical across the version boundary.

## Migration

- Legacy and V1 sources accept only actions `4`/`5` for maps.
- Earlier and current V2 sources accept `4..=7` according to the table above.
- Output is a fresh V2 snapshot containing only action `6` map records.
- Source artifacts are captured, reread, and required to remain byte-identical.
