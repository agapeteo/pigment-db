# Frozen Historical I128 WAL Fixtures

These lowercase hexadecimal files are immutable inputs captured before correcting `Key::I128(u64)` to `Key::I128(i128)`. Tests decode the text to bytes but MUST NOT regenerate it from the implementation under test.

Every fixture contains:

- a historical sorted-map put for outer key `retained`, search key `I128(u64::MAX)`, value `old-max`;
- a historical sorted-map remove for outer key `missing`, search key `I128(0)`.

| Fixture | Binary bytes | SHA-256 |
|---------|-------------:|----------|
| `legacy-map.hex` | 112 | `85d26da3569c2df38e5c6b4ab0684e918dd6d9826c18b7335c96554f4d964589` |
| `v1-map.hex` | 218 | `85c6eb7c4da5072d5ecebef04456b6722e64e996ce421164d866e76c298ce963` |
| `earlier-v2-map.hex` | 282 | `10570a1ba66873b45f7f04da51506bc5b109d8811c9ef71dfd6922674a6bc62b` |

The byte layout is the pre-correction derived binary model: `I128` is enum discriminant `10` followed by an unsigned 64-bit payload. Legacy framing uses the platform-native little-endian layout captured on the repository's fixture platform; V1 and V2 framing is explicitly little-endian.
