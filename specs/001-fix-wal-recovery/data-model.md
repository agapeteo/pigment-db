# Data Model: Crash-Safe WAL Recovery

## Public entities

### RecoveryStatus

Represents whether initialization had to resolve artifacts left by interrupted maintenance.

| Value | Meaning |
|---|---|
| `Normal` | No interrupted-maintenance artifacts required recovery. Ordinary replay/compaction may still have occurred. |
| `Recovered` | Startup recognized and safely resolved or deferred cleanup of interrupted-maintenance artifacts. |

Validation rules:

- Finding staging or legacy recovery artifacts makes a successful outcome `Recovered`.
- Replaying an ordinary active WAL by itself remains `Normal`.
- Conflict/error paths do not produce a status.

### RecoveryOutcome&lt;S&gt;

Owns a successfully initialized store and its `RecoveryStatus`.

| Field | Type | Visibility | Rule |
|---|---|---|---|
| store | `S` | private | Exactly one initialized store is owned. |
| status | `RecoveryStatus` | private | Must be `Normal` or `Recovered`. |

Operations expose the status, borrow the store, consume the outcome into the store, or split it into parts. No artifact path or open recovery handle escapes initialization.

### RecoveryOperation

Categorizes the filesystem action associated with an initialization error.

| Value | Purpose |
|---|---|
| `Inspect` | Read metadata or enumerate recognized artifacts. |
| `Open` | Open an artifact for validation or append. |
| `CreateStaging` | Exclusively create the replacement staging file. |
| `WriteStaging` | Serialize or synchronize the replacement. |
| `Publish` | Replace the active name with completed staging. |
| `Cleanup` | Remove obsolete staging or legacy recovery artifacts. |

### RecoveryError

Structured failure returned by the fallible initializer.

| Variant | Data | Meaning |
|---|---|---|
| `AuthorityUndetermined` | recognized candidate paths | More than one candidate exists and provenance cannot prove which state is authoritative. |
| `InvalidArtifact` | path plus validation category | A required candidate is truncated, has an invalid checksum/action/payload, or cannot be fully replayed. |
| `Io` | operation, path, source error | A required filesystem operation failed. |

Validation rules:

- Errors preserve every potentially authoritative artifact.
- `Cleanup` errors after authority is established are logged and do not become `RecoveryError`; initialization succeeds with `Recovered`.
- Error and operation enums are non-exhaustive so diagnostics can grow compatibly.

## Internal entities

### StoreKind

Identifies the store whose artifacts are being managed: key/value, key/set, or key/sorted-map. Recovery operates on one kind at a time; a conflict for one kind does not block another kind in the same directory.

### ArtifactPaths

Derived from a store directory and `StoreKind`.

| Role | Key/value example | Rule |
|---|---|---|
| active | `kv.wal.dat` | Only name opened for normal appends. |
| legacy recovery | `.kv.wal.dat` | Produced by the pre-feature startup sequence. |
| staging | `.kv.wal.dat.next` | Same-directory, exclusively created, never authoritative. |

The set and sorted-map stores use the equivalent existing base filenames.

### ArtifactObservation

Classification produced without mutating the directory.

| State | Attributes | Meaning |
|---|---|---|
| `Missing` | path | Artifact does not exist. |
| `Complete` | path, byte length, logical snapshot, frame-prefix snapshots | Entire artifact replays successfully. Zero-length active is a complete empty state. |
| `Incomplete` | path, validated prefix length | Artifact ends before a full frame or full replay completes. |
| `Invalid` | path, validation category | CRC, action type, payload, offset, or structural validation failed. |

Only `Complete` active or legacy recovery artifacts can become sources. Staging remains non-authoritative even when complete.

### LogicalSnapshot

Store-specific logical contents after replay:

- Key/value: map of byte key to byte value.
- Key/set: map of byte key to a set of byte members.
- Key/sorted-map: map of byte key to an ordered map of search key to byte value.

Snapshots are compared logically, not by serialized byte order.

### RecoveryDecision

Pure result of inspecting recognized artifacts.

| Decision | Source | Mutation allowed |
|---|---|---|
| `InitializeEmpty` | none | Create active empty WAL. |
| `UseActiveNormal` | active | Compact/publish safely; return `Normal`. |
| `UseActiveRecovered` | active | Clean stale artifacts if possible; return `Recovered`. |
| `UseLegacyRecovered` | legacy recovery | Rebuild/publish active; return `Recovered`. |
| `Conflict` | none | Preserve all artifacts and return structured error. |

## State transitions

### New publication protocol

| Current state | Action | Next persistent state | Authority |
|---|---|---|---|
| active only | Create staging exclusively | active + staging | active |
| active + partial staging | Continue current attempt or interrupt | active + staging | active |
| active + complete validated staging | Close/synchronize staging | active + staging | active |
| active + synchronized staging | Rename staging over active | active only | replacement active |
| active only | Reopen active for append at validated byte length | active only | active |

Interruption before publication leaves active authoritative. Interruption after successful publication leaves the complete replacement at active. Startup never needs to promote a leftover stage.

### Legacy recovery classification

| Active | Legacy recovery | Proof | Decision |
|---|---|---|---|
| missing/empty/incomplete | complete | recovery is only full source | use legacy |
| complete | complete | logical states equal | use active |
| complete | complete | an active frame prefix equals recovery state | use active; later frames are post-replay mutations |
| complete snapshot prefix | complete | active is a proper replay prefix and never reaches recovery state | use legacy |
| complete | complete | none of the above | conflict; preserve both |

If active is proven newer and legacy cleanup fails, active opens without compaction so the provenance prefix remains available on the next retry. If legacy is selected, publication produces active containing the full legacy state before cleanup is attempted.

## Fault points

Tests name transitions rather than exposing them publicly:

- after artifact inspection
- after staging creation
- after first and middle snapshot records
- after staging validation
- after staging synchronization
- after publication rename
- before cleanup
- after cleanup failure
- after cleanup success

A test interruption returns immediately without rollback, leaving the same files a terminated process would leave.
