# Public Contract: Recovery-Aware Initialization

## Shared types

The crate root exposes one shared recovery contract for all file-backed stores:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryStatus {
    Normal,
    Recovered,
}

#[must_use]
pub struct RecoveryOutcome<S> {
    store: S,
    status: RecoveryStatus,
}

impl<S> RecoveryOutcome<S> {
    pub fn status(&self) -> RecoveryStatus;
    pub fn store(&self) -> &S;
    pub fn into_store(self) -> S;
    pub fn into_parts(self) -> (S, RecoveryStatus);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryOperation {
    Inspect,
    Open,
    CreateStaging,
    WriteStaging,
    Publish,
    Cleanup,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum RecoveryError {
    AuthorityUndetermined {
        active_path: Option<std::path::PathBuf>,
        recovery_path: Option<std::path::PathBuf>,
    },
    InvalidArtifact {
        path: std::path::PathBuf,
    },
    Io {
        operation: RecoveryOperation,
        path: std::path::PathBuf,
        source: std::io::Error,
    },
}
```

`RecoveryError` implements `Display` and `std::error::Error`. Its diagnostics identify the operation and relevant path without modifying the failed candidates. `io::Error` prevents `RecoveryError` from promising `Clone`, `Eq`, or `PartialEq`; callers match variants and inspect fields.

The concrete definitions may add private constructors or diagnostic fields, but must retain these observable variants and behaviors.

## Store initializers

Each file-backed store exposes the same pair of initializers:

```rust
impl DurableKeyValueStore<std::fs::File> {
    pub fn try_init_new(
        store_dir: impl AsRef<std::path::Path>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError>;

    pub fn init_new(store_dir: &str) -> Self;
}

impl DurableKeySetStore<std::fs::File> {
    pub fn try_init_new(
        store_dir: impl AsRef<std::path::Path>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError>;

    pub fn init_new(store_dir: &str) -> Self;
}

impl DurableKeyMapStore<std::fs::File> {
    pub fn try_init_new(
        store_dir: impl AsRef<std::path::Path>,
    ) -> Result<RecoveryOutcome<Self>, RecoveryError>;

    pub fn init_new(store_dir: &str) -> Self;
}
```

The vector-backed `new_vec_based()` constructors do not change.

## Behavioral contract

### `try_init_new`

- Returns `Normal` for a new store or ordinary startup with no interrupted-maintenance artifacts.
- Returns `Recovered` when legacy recovery or staging artifacts were recognized and safely resolved, even if obsolete-artifact cleanup must be deferred.
- Returns `AuthorityUndetermined` when candidate provenance cannot establish a safe source.
- Returns `InvalidArtifact` when the only required source cannot be fully validated.
- Returns `Io` for required filesystem failures with operation and path context.
- Never panics for expected artifact or filesystem failures.
- Never removes or overwrites potentially authoritative candidates on an error return.
- Owns the returned store; no recovery file handle or path borrow escapes.

### `init_new`

- Keeps the existing `init_new(&str) -> Self` signature and successful behavior.
- Delegates to `try_init_new`.
- Logs an informational recovery event exactly when the delegated result is `Recovered`.
- Returns the owned store for `Normal` and `Recovered` outcomes.
- Panics with the structured error's diagnostic if the fallible initializer returns an error, preserving the existing constructor's effective failure behavior.

## Compatibility contract

- Existing valid WAL artifacts remain readable without user migration.
- Existing call sites using `init_new` continue to compile.
- No public mutation or read method changes as part of this feature.
- Key/value, key/set, and key/sorted-map stores use the same status and error types.
- Recovery for one store kind does not initialize, remove, or block artifacts belonging exclusively to another store kind.
