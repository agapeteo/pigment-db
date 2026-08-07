# Contract: Recovery and Timestamp Configuration API

Existing startup signatures remain:

```rust
DurableKeyValueStore::try_init_new(path)
DurableKeySetStore::try_init_new(path)
DurableKeyMapStore::try_init_new(path)
DurableKeyValueStore::init_new(path_as_str)
DurableKeySetStore::init_new(path_as_str)
DurableKeyMapStore::init_new(path_as_str)
```

For missing/new or valid V1 stores, successful results and compatibility behavior
remain unchanged. For complete legacy-format input, fallible initializers return a
new structured error equivalent to:

```rust
RecoveryError::MigrationRequired { path }
```

The existing `RecoveryError` is non-exhaustive. Compatibility wrappers retain
their signatures and panic with an actionable diagnostic naming
`pigment-db-migrate`. Truncated/corrupt legacy remains `InvalidArtifact` and is not
described as migratable.

Missing file-backed stores follow
[fresh-v1-publication.md](fresh-v1-publication.md): options and candidates validate
before mutation; every pre-publication error returns no store with active absent;
successful publication exposes only a complete validated header and returns the
existing normal new-store outcome. An existing partial/corrupt header remains a
structured invalid-artifact failure and is never treated as missing.

## Additive timestamp configuration

The crate adds public validated types equivalent to:

```rust
TimestampGranularity::try_from(std::time::Duration)
DurableStoreOptions::default()
DurableStoreOptions::with_timestamp_granularity(validated)
Durable*Store::try_init_new_with_options(path, options)
Durable*Store::new_vec_based_with_options(options)
```

All three families use one shared options type. Zero and durations whose
nanoseconds do not fit `u64` fail before filesystem mutation. Default is one
minute. A missing store uses selected/default configuration; existing V1 without
an explicit option honors persisted configuration; an explicit option may change
it through staged V1 compaction while preserving the last bucket. Options do not
authorize legacy migration and return `MigrationRequired` for complete legacy.

The underlying internal options behavior must be runtime GREEN before these
adapters are exposed. Adding the public symbols is a behavior-preserving refactor;
their first public contract tests must pass rather than manufacture RED through a
missing symbol or deliberately incorrect adapter.

## Outcome compatibility

`RecoveryStatus::Recovered` covers successful V1 tail repair and V1 artifact
resolution. A later stable reopen returns `Normal`. No success status is returned
for legacy input or failed repair. Corruption, invalid/partial V1 header, store-kind
mismatch, repair I/O, and authority ambiguity use the structured recovery error
surface. Point-in-time open/restore APIs remain absent.

## CLI boundary

Legacy migration is supported only through the standalone contract in
[migration-cli.md](migration-cli.md). The library exposes no supported migration
function, options, report, or error API. A doc-hidden zero-argument symbol used by
the package binary is an implementation bridge and outside this public contract.
