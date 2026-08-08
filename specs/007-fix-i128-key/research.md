# Research: Full-Range Signed I128 Keys

## Decision 1: Correct the public payload to `i128`

**Decision**: `Key::I128` stores `i128`, retains its enum position, derives signed ordering, and continues to report 16 bytes.

**Rationale**: This is the direct semantic correction requested by Issue #10 and covers the complete domain promised by the variant name.

**Alternatives considered**:

- Add a second signed variant: rejected because it leaves the misleading public variant and fragments ordering/API semantics.
- Keep `u64` and reinterpret its bits: rejected because it cannot represent the signed domain without surprising callers and wrong ordering.
- Store a byte array: rejected because it weakens type safety and requires custom comparison.

## Decision 2: Use distinct current V2 map action identifiers

**Decision**: Reserve action `6` for current signed sorted-map put and action `7` for current signed sorted-map remove. Actions `4` and `5` remain historical unsigned map put/remove.

**Rationale**: The enclosing frame already checks the action, so the identifier selects the decoder before any payload mutation. It has no collision, heuristic, extra envelope, or segment-level rewrite requirement and permits old/current records in one V2 chain.

**Alternatives considered**:

- Infer payload width from remaining bytes: rejected because variable-length surrounding fields make heuristic decoding ambiguous and malformed data could be accepted under the wrong schema.
- Prefix each payload with a magic/version tag: rejected because it adds bytes and a second discriminator when the action namespace already supplies an integrity-protected field.
- Change the V2 segment header version: rejected because a segment can legitimately contain historical and current records after reopening, and rewriting/sealing solely for this model change is unnecessary.
- Implement custom global `Key` serialization with two enum variants on wire: rejected because it changes direct serialization machinery for every key user and couples public serde to WAL history.

## Decision 3: Freeze historical wire models

**Decision**: Introduce private historical equivalents of `Key`, `SearchKey`, `SortedMapEntry`, and `SortedMapKey` with `I128(u64)`, preserving original enum order and field order exactly. Convert them explicitly into current public models.

**Rationale**: Derive-based binary layouts are positional. Dedicated models make the old contract reviewable and prevent a future public model edit from silently changing historical decoding.

**Alternatives considered**:

- Deserialize historical bytes into the new public model: rejected because it consumes 16 bytes for a field that historically contains 8.
- Patch payload bytes before deserialization: rejected because walking nested composite keys manually duplicates the binary serializer and is error-prone.
- Regenerate fixtures from the historical structs during tests: rejected because the constitution requires immutable legacy inputs independent of the implementation under test.

## Decision 4: Normalize by exact widening

**Decision**: Convert each historical `u64` payload with lossless numeric widening to `i128`.

**Rationale**: Every historical value fits exactly and remains nonnegative. Bit reinterpretation or sign extension would change values above the signed 64-bit maximum.

**Alternatives considered**:

- Cast through `i64`: rejected because values above `i64::MAX` become negative.
- Reject values above `i64::MAX`: rejected because all `u64` values were valid historical inputs.
- Preserve a hidden unsigned marker in public state: rejected because equality and ordering would become representation-dependent.

## Decision 5: Keep migration offline and source preserving

**Decision**: Existing offline migration reads historical actions into normalized state and writes V2 snapshot records with current action identifiers. Startup accepts valid mixed earlier/current V2 histories but does not rewrite them.

**Rationale**: This preserves source authority and avoids in-place crash states while ensuring compacted output has one current contract.

**Alternatives considered**:

- Rewrite old V2 records on open: rejected because startup would become a migration transaction and could damage the last authoritative source.
- Continue emitting historical actions when values happen to fit `u64`: rejected because new output would remain schema-ambiguous and negative values could not be represented.

## Decision 6: No new performance protocol

**Decision**: Use regression suites and code-path inspection rather than a quiet-machine throughput gate.

**Rationale**: The change adds no lock, system call, hash, or extra serialization pass. Current records expand only when an `I128` component is present, which is required data rather than incidental overhead.

**Alternatives considered**:

- Repeat the Issue #9 six-cell benchmark: rejected because it uses byte search keys and cannot measure this representation change meaningfully.
- Add an `I128` microbenchmark as a release gate: rejected because no pre-change implementation can perform the same signed-domain workload.
