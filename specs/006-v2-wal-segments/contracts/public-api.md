# Contract: Public Configuration and Startup

## Additive configuration

`WalSegmentSize` is a public validated nonzero byte count.

- `WalSegmentSize::default()` is 1 GiB (`1_073_741_824` bytes).
- `WalSegmentSize::try_from(0_u64)` returns `WalSegmentSizeError::Zero`.
- `WalSegmentSize::as_bytes()` returns the configured value.
- `DurableStoreOptions::with_wal_segment_size(...)` selects the runtime target.
- `DurableStoreOptions::wal_segment_size()` returns the selected target.

`TimestampGranularity` remains a validated nonzero duration.

- New stores default to one minute.
- Only calling `with_timestamp_granularity(...)` requests a persisted change.
- Passing options solely for durability or segment size does not reset an existing persisted granularity.
- A requested change is applied by rotation before the next accepted mutation, not during open.

## Startup outcomes

- Missing file-backed store: publish a complete empty V2 active segment and return `Normal`.
- Complete V2 active or segment chain: replay and return `Normal`.
- Recoverable terminal V2 tail or interrupted next-segment publication: repair and return `Recovered`.
- Complete or recoverable V1: return `MigrationRequired` naming the source path and migration CLI.
- Complete legacy: return `MigrationRequired`.
- Corrupt, partial-header, wrong-family, or inconsistent-chain input: return `InvalidArtifact` or `AuthorityUndetermined` and preserve evidence.

The existing compatibility initializers retain their panic-on-error behavior by wrapping the fallible startup APIs.

## Mutation behavior

- Existing public mutation signatures and live-state results are unchanged.
- Segment rotation is internal and does not expose partial logical state.
- A rotation failure rejects the mutation. If namespace authority may have advanced, the WAL instance fails closed until reopened.
- Buffered and physical acknowledgement policies retain their existing definitions.
