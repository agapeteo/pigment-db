# Public API Contract: Signed I128 Keys

## Type correction

The public key variant is:

```rust
Key::I128(i128)
```

It accepts the complete signed 128-bit range, compares values using signed numeric order, and reports a logical size of 16 bytes.

## Source compatibility

Code using integer literals that fit both types generally continues to infer the corrected payload type. Code passing a `u64` variable must choose and write an explicit conversion, for example a checked or lossless widening appropriate to its domain. This source-level correction is intentional and approved by the feature specification.

No compatibility constructor retaining the old misleading unsigned payload is added.

## Durable behavior

- Public sorted-map put, remove, compute, get, range, pop, and reopen operations accept signed `I128` components wherever they accept `SearchKey`.
- Accepted full-range values reopen exactly with the same associated values and signed order.
- Historical persisted `I128(u64)` values appear through public reads as numerically equal nonnegative `I128(i128)` values.
- Key/value and key/set public contracts are unchanged.

## Direct serialization boundary

The `Serialize`/`Deserialize` representation of the corrected public enum contains the signed 128-bit payload. Automatic historical conversion is guaranteed for pigment-db-managed WAL formats, whose record action supplies a version context. Unversioned serialized `Key` blobs owned by callers are outside automatic migration scope.
