# Contract: Public Maintenance API

## Surface

The crate root re-exports the following documented, future-extensible types and functions. Public structs keep fields private and expose `const` getters where possible. Option types implement `Default` and consuming `with_*` builders consistent with `DurableStoreOptions`.

```rust
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum StoreFamily { KeyValue, KeySet, KeyMap }

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyStorageStats { /* private fields */ }

impl FamilyStorageStats {
    pub const fn family(&self) -> StoreFamily;
    pub const fn active_bytes(&self) -> u64;
    pub const fn sealed_segment_bytes(&self) -> u64;
    pub const fn sealed_segment_count(&self) -> usize;
    pub const fn total_bytes(&self) -> u64;
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryStorageStats { /* private fields */ }

impl DirectoryStorageStats {
    pub fn families(&self) -> &[FamilyStorageStats];
    pub const fn total_bytes(&self) -> u64;
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClosedCompactionOptions { /* private fields */ }

impl ClosedCompactionOptions {
    pub const fn durability_policy(&self) -> DurabilityPolicy;
    pub const fn with_durability_policy(self, policy: DurabilityPolicy) -> Self;
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnlineCompactionOptions { /* private fields */ }

impl OnlineCompactionOptions {
    pub const fn max_delta_bytes(&self) -> u64;
    pub const fn with_max_delta_bytes(self, bytes: u64) -> Self;
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStatus { Complete, Pending }

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyCompactionOutcome { /* private fields */ }

impl FamilyCompactionOutcome {
    pub const fn family(&self) -> StoreFamily;
    pub const fn before_bytes(&self) -> u64;
    pub const fn after_bytes(&self) -> u64;
    pub const fn sealed_segments_removed(&self) -> usize;
    pub const fn concurrent_mutations_replayed(&self) -> usize;
    pub const fn cleanup(&self) -> CleanupStatus;
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryCompactionOutcome { /* private fields */ }

impl DirectoryCompactionOutcome {
    pub fn families(&self) -> &[FamilyCompactionOutcome];
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionOperation {
    Inspect,
    Capture,
    WriteStaging,
    ValidateStaging,
    WriteManifest,
    PublishPrevious,
    PublishReplacement,
    ReopenReplacement,
    Cleanup,
}

#[non_exhaustive]
#[derive(Debug)]
pub enum CompactionError {
    MigrationRequired { path: PathBuf },
    InvalidArtifact { path: PathBuf },
    AuthorityUndetermined { paths: Vec<PathBuf> },
    ConcurrentDeltaLimitExceeded { limit: u64 },
    UnsupportedDurability { source: DurabilitySupportError },
    Io { operation: CompactionOperation, path: PathBuf, source: io::Error },
    FailedClosed { detail: String },
}

pub fn inspect_storage(
    store_dir: impl AsRef<Path>,
) -> Result<DirectoryStorageStats, CompactionError>;

pub fn compact_directory_in_place(
    store_dir: impl AsRef<Path>,
    options: ClosedCompactionOptions,
) -> Result<DirectoryCompactionOutcome, CompactionError>;
```

`CompactionError` implements `Display` and `std::error::Error`; `source()` returns the wrapped I/O or durability error. `MigrationRequired` display text names the path and instructs the caller to use `pigment-db-migrate`, without exposing a format-version id.

## Defaults and ordering

- `ClosedCompactionOptions::default()` selects `DurabilityPolicy::Buffered`.
- `OnlineCompactionOptions::default()` selects 8 MiB (`8 * 1024 * 1024`).
- Directory family statistics and outcomes are ordered `KeyValue`, `KeySet`, `KeyMap`.
- Directory totals use checked arithmetic; overflow is an operation-specific structured failure, never saturation.
- Empty-directory inspection returns zero; empty-directory closed compaction returns an empty successful outcome and creates no database artifacts.

## File-backed methods

Only file-backed specializations expose maintenance methods:

```rust
impl DurableKeyValueStore<File> {
    pub fn storage_stats(&self) -> Result<FamilyStorageStats, CompactionError>;
    pub fn try_compact_online(
        &self,
        options: OnlineCompactionOptions,
    ) -> Result<FamilyCompactionOutcome, CompactionError>;
}
```

Equivalent methods are added to `DurableKeySetStore<File>` and `DurableKeyMapStore<File>`. Vector-backed constructors remain source compatible and do not gain filesystem maintenance methods. Online compaction always inherits the opened store's durability policy.

## Behavioral contract

- `inspect_storage` is byte-for-byte and namespace read-only, including in the presence of recoverable WAL tails or maintenance evidence.
- Any unexpected store-directory entry returns `InvalidArtifact`; inspection and compaction preserve that entry and every other artifact byte-for-byte.
- `storage_stats` reports only the open family's current authoritative generation.
- Closed compaction rejects a same-process open-store overlap with `FailedClosed` before any artifact mutation.
- A second online attempt on the same instance returns `FailedClosed` immediately and creates no artifacts.
- Delta overflow returns `ConcurrentDeltaLimitExceeded` only at the attempt boundary; ordinary mutations continue against the original authority.
- Successful publication plus failed cleanup returns an outcome with `CleanupStatus::Pending` rather than an error.
- Legacy recognized data returns `MigrationRequired`; runtime never converts it.

## Public API acceptance evidence

Compile-time API tests prove all re-exports, exact method specialization, builders/defaults, non-exhaustive matching, `Send`/`Sync` properties already promised by store types, and error source chains. Rustdoc examples exercise empty inspection, closed compaction, and online options without depending on internal format ids.
