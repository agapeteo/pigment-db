# Public Contract: Durability Configuration and Fallible Mutations

## Configuration

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurabilityPolicy {
    #[default]
    Buffered,
    Physical,
}

impl DurableStoreOptions {
    pub fn with_durability_policy(
        self,
        durability_policy: DurabilityPolicy,
    ) -> Self;
}
```

`DurabilityPolicy` is re-exported from the crate root. The option is runtime-only:
it does not change V1 or legacy bytes, and a later no-options reopen selects
`Buffered` regardless of the previous process policy.

### File-backed construction

The existing methods and return types remain:

```rust
DurableKeyValueStore::<File>::try_init_new_with_options(
    store_dir,
    options,
) -> Result<RecoveryOutcome<Self>, RecoveryError>
```

The key/set and key/sorted-map methods have the same contract. A physical request
returns no store until capability checks and any startup publication barriers
complete. Unsupported physical mode uses an additive non-exhaustive recovery
error:

```rust
RecoveryError::UnsupportedDurability {
    source: DurabilitySupportError,
}
```

Existing `try_init_new` and `init_new` remain buffered. No file-backed infallible
options initializer is added solely for this feature.

Public physical construction is promoted only after the private capability,
fresh-publication, active-authority, recovery-authority, cleanup, crash/reopen,
and three-family matrices are GREEN. Passing capability preflight alone is not a
publicly releasable implementation state.

### In-memory construction

Each `Vec<u8>` family gains:

```rust
pub fn try_new_vec_based_with_options(
    options: DurableStoreOptions,
) -> Result<Self, DurabilitySupportError>;
```

Existing `new_vec_based_with_options(options) -> Self` delegates to the fallible
method. It preserves normal buffered/timestamp behavior and panics with an
actionable diagnostic when `Physical` is requested. `new_vec_based()` remains
unchanged and buffered.

## Support Error

```rust
#[derive(Debug)]
#[non_exhaustive]
pub enum DurabilitySupportError {
    NoPhysicalBacking,
    UnsupportedPlatform {
        platform: &'static str,
    },
    RequiredBarrierUnavailable {
        operation: DurabilityCapability,
        path: Option<PathBuf>,
        source: io::Error,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurabilityCapability {
    FileContent,
    DirectoryEntry,
}
```

The error implements `Display` and `Error`. A missing safe compile-time
implementation uses `UnsupportedPlatform`. Runtime physical startup performs
phase-based preflight on the actual backing filesystem:

1. Inspect active/recovery authority without mutation.
2. Preflight `DirectoryEntry` by opening and synchronizing the parent directory.
3. If an authority exists, preflight `FileContent` by fully synchronizing the
   selected active/recovery file.
4. If the store is missing, create/write/flush/validate non-authoritative staging
   and use `sync_all(staging)` as the `FileContent` preflight.

Any open or synchronization failure during these preflight operations returns
`RequiredBarrierUnavailable { operation, path, source }`, regardless of
`io::ErrorKind` or raw OS code. A failed parent preflight changes no artifact. A
failed missing-store content preflight creates no authority and cleans staging;
only diagnosed non-authoritative staging may remain if cleanup also fails.
Permission, capacity, media, and transient failures after successful preflight,
including later startup publication barriers, remain structured
operation/path-aware `RecoveryError::Io` rather than support errors.

## Mutation Error Classification

All fallible mutation APIs use `std::io::Result`. Their `io::Error` source is a
public non-exhaustive `MutationFailure`:

```rust
#[derive(Debug)]
#[non_exhaustive]
pub enum MutationFailure {
    Rejected {
        operation: PersistenceOperation,
        source: io::Error,
    },
    Indeterminate {
        operation: PersistenceOperation,
        source: io::Error,
        rollback_operation: PersistenceOperation,
        rollback: io::Error,
    },
    FailedClosed {
        original: String,
        rollback: String,
    },
}

impl MutationFailure {
    pub fn from_io_error(error: &io::Error) -> Option<&Self>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PersistenceOperation {
    Write,
    Flush,
    SynchronizeData,
    Rollback,
    SynchronizeRollback,
}
```

- `Rejected`: rollback was confirmed under the selected policy; no live state was
  published and the instance may continue.
- `Indeterminate`: rollback could not be confirmed; no live state was published
  and the instance is failed closed.
- `FailedClosed`: a later call was refused before writer/barrier I/O.

For `Rejected`, the outer `io::ErrorKind` matches the original source. For
`Indeterminate` and `FailedClosed`, it is `Other`. Diagnostics include the failing
operation and source without requiring callers to parse text for classification.

## Key/Value Fallible Mutations

| Additive method | Return contract |
|---|---|
| `try_put(key, value)` | `io::Result<()>` |
| `try_compute(key, callback)` | `io::Result<()>`; callback once, publication after persistence |
| `try_increment_or_init(key, amount)` | `io::Result<Result<u64, ()>>`; inner result preserves invalid-number behavior |
| `try_decrement(key, amount)` | `io::Result<Option<Result<u64, ()>>>` |
| `try_set_number(key, value)` | `io::Result<()>` |
| `try_remove(key)` | `io::Result<()>` |

Existing `put`, `compute`, `increment_or_init`, `decrement`, `set_number`, and
`remove` delegate to the matching fallible method and preserve their exact
successful results and historical panic-on-persistence-error behavior.

## Key/Set Fallible Mutations

| Additive or retained method | Return contract |
|---|---|
| `try_append(key, member)` | `io::Result<()>` |
| `try_remove_from_set(key, member)` | `io::Result<()>` |
| `try_remove_from_set_callback(key, member, callback)` | `io::Result<()>`; callback only after successful final-member publication |
| `try_remove_key(key)` | `io::Result<()>` |
| existing synchronous `try_compute*` family | Existing `io::Result<()>` signatures remain unchanged |
| existing `try_compute_async(key, callback)` | Existing `io::Result<()>` signature remains unchanged; cancellation while the callback is pending releases the per-key guard and performs no database persistence or publication |

Existing infallible counterparts remain thin panic wrappers. Exact no-op compute
results continue to issue no WAL write or barrier. The async callback is the only
yield point: once it returns `Ready`, WAL persistence and live publication run
synchronously without another cancellation point and produce the normal success
or typed persistence-failure result. Dropping the future while the callback is
pending discards its private working copy and does not undo external side effects
that callback code may already have performed.

## Key/Sorted-Map Fallible Mutations

| Additive or retained method | Return contract |
|---|---|
| `try_put(key, search_key, value)` | `io::Result<()>` |
| `try_remove_from_sorted_map(key, search_key)` | `io::Result<Option<Vec<u8>>>` |
| `try_remove_from_sorted_map_callback(key, search_key, callback)` | `io::Result<()>`; callback only after successful final-entry publication |
| `try_remove_key(key)` | `io::Result<()>` |
| `try_pop_first(key)` | `io::Result<Option<(SearchKey, Vec<u8>)>>` |
| `try_pop_last(key)` | `io::Result<Option<(SearchKey, Vec<u8>)>>` |
| `try_append_ordered_element(key, value)` | `io::Result<()>` |
| existing `try_compute*` family | Existing `io::Result<()>` signatures remain unchanged |

Existing return values, ordered-key generation, callback arguments, no-op
eligibility, and panic behavior remain unchanged.

## Compatibility Invariants

- Existing signatures are not removed or changed.
- Existing no-options construction stays buffered.
- Existing successful domain results and callback counts are identical.
- An error never publishes live mutation state or calls its post-publication
  callback.
- Existing set/map `try_compute*` remain source-compatible `std::io::Result<()>`.
- Existing key/set `try_compute_async` retains its pending-callback cancellation
  boundary and does not introduce cancellable or background persistence.
- V1 and legacy bytes, action identifiers, timestamps, and recovery statuses do
  not encode or infer the runtime durability policy.
