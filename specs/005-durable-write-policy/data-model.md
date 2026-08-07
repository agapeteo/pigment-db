# Data Model: Explicit Durable Write Acknowledgements

This feature changes runtime authority and error state only. Legacy and V1 file
headers, action records, logical groups, timestamps, checksums, and action meanings
remain byte-for-byte unchanged.

## Durability Policy

| Variant | Success means | Persisted | Supported backing |
|---|---|---|---|
| `Buffered` | The complete logical mutation was written/flushed into the process and OS-managed persistence path and published live | No | File and memory |
| `Physical` | The complete logical mutation was covered by a successful data barrier before live publication and return | No | Capability-approved file backing only |

Rules:

- `Buffered` is the default for every no-options constructor and reopen.
- A policy applies to one opened store instance and is never inferred from WAL
  bytes.
- `Physical` with memory backing is `NoPhysicalBacking`.
- `Physical` on a platform/filesystem without every required barrier is
  `RequiredBarrierUnavailable` or compile-time unsupported.
- No unsupported request may expose a store or become buffered implicitly.

## Durability Capability

| Field | Meaning | Validation |
|---|---|---|
| target support | Whether the compiled target has a safe standard-library implementation | Linux/macOS may proceed to runtime preflight; Windows/other targets are explicitly unsupported in this feature |
| namespace barrier | Ability to open and synchronize the parent directory | Parent-directory preflight succeeds before any cleanup, staging creation, repair, or publication |
| existing content barrier | Ability to fully synchronize the selected active/recovery WAL | `sync_all` on the non-mutating authority selection succeeds after the parent preflight |
| missing-store content barrier | Ability to fully synchronize the future WAL on the actual backing filesystem | Complete non-authoritative staging is written, flushed, validated, then `sync_all(staging)` succeeds before any authority rename |
| path | File or directory whose capability was checked | Included in structured errors where available |

Every open or synchronization failure while performing one of these preflights is
`RequiredBarrierUnavailable { operation, path, source }`, regardless of
`io::ErrorKind` or raw OS code. A successful preflight establishes availability,
not immunity from later media, permission, capacity, or transient failure. Errors
after successful preflight are ordinary path-aware persistence/recovery I/O errors
and follow the mutation/publication state machines.

## WAL Durability State

`WalState<W>` retains current offset, writer, format, timestamp granularity, last
accepted bucket, rollback behavior, and health. It adds an internal policy:

```text
Buffered
Physical {
  data_barrier(writer),
  full_barrier(writer)
}
```

The function-pointer shape preserves the current generic writer bound and permits
deterministic scripted writers without dynamic dispatch or a public trait.

## Logical Mutation Acceptance

One state machine covers a single record or one complete multi-record group.

```text
Ready
  → CheckpointCaptured(offset, last_bucket)
  → BytesWritten(complete logical mutation)
  → Flushed
  → [Physical only] DataSynchronized
  → Accepted(offset and last_bucket advance)
  → LivePublished
  → Success
```

Validation and authority rules:

- The per-key/shard guard is held throughout; the existing WAL write guard is
  held from checkpoint capture through accepted-state advance or rollback.
- Exactly one direct physical barrier covers each complete logical mutation. It
  is never issued per group member and is never shared across calls.
- `offset` and `last_bucket` do not advance on write, flush, or barrier failure.
- Live state and callbacks do not advance before `Accepted`.
- A process interruption after `DataSynchronized` but before `LivePublished` may
  replay the complete accepted bytes even if the caller did not observe success.

### Async compute cancellation boundary

The existing key/set `try_compute_async` callback is the operation's only yield
point:

```text
CallbackPending(per-key guard held, private working copy)
  ├─ future dropped/cancelled
  │    → release per-key guard
  │    → discard private working copy
  │    → no WAL write, flush, barrier, accepted-state advance, or live publication
  └─ callback returns Ready
       → run Logical Mutation Acceptance synchronously without another yield
       → return its normal success or typed persistence failure
```

Cancellation can therefore reject only a still-pending callback computation. It
cannot interrupt or orphan persistence after the callback has completed. Side
effects performed by user callback code outside the database are outside the
library's rollback boundary and are not undone when its future is dropped.

## Rejection and Fail-Closed State

```text
Write / Flush / DataSynchronized failure
  → TruncateToCheckpoint
      ├─ truncate fails → FailedClosed + Indeterminate
      └─ truncate succeeds → FullRollbackSynchronized
          ├─ sync_all succeeds → Ready + Rejected
          └─ sync_all fails → FailedClosed + Indeterminate

FailedClosed + later mutation
  → reject before writer/barrier access
```

| Outcome | Live state | Durable interpretation | Later mutation |
|---|---|---|---|
| `Rejected` | Unchanged | Pre-mutation checkpoint confirmed | Allowed |
| `Indeterminate` | Unchanged in current instance | Complete or incomplete attempted bytes may be durable | Refused; instance failed closed |
| `FailedClosed` | Unchanged | Prior indeterminate state still unresolved | Refused without I/O |

Reopen resolves an indeterminate image by the clarified issue #4 rule:

- Complete structurally valid logical mutation: replay it as authoritative.
- Incomplete terminal logical mutation: recover the accepted prefix.
- Complete structural corruption: preserve and fail under existing corruption
  rules.

## Mutation Failure

`MutationFailure` is public and non-exhaustive but remains the source carried by a
`std::io::Error` so existing and new try methods share `std::io::Result`.

| Variant | Fields | Meaning |
|---|---|---|
| `Rejected` | persistence operation, original I/O source | Durable rollback confirmed the preceding checkpoint |
| `Indeterminate` | failed operation, original source, rollback operation/source | Attempt and rollback could not be conclusively ordered on storage |
| `FailedClosed` | prior original/rollback diagnostics | Current instance refused a later mutation before I/O |

`PersistenceOperation` identifies `Write`, `Flush`, `SynchronizeData`,
`Rollback`, or `SynchronizeRollback`. `MutationFailure::from_io_error` returns a
borrowed typed classification without consuming the `io::Error`.

## Store Construction Outcome

```text
Options selected
  ├─ Buffered → existing startup flow
  └─ Physical
      → backing supports physical storage?
      → target supports required safe barriers?
      → inspect active/recovery authority without mutation
      → parent-directory preflight
          ├─ failure → no store + RequiredBarrierUnavailable; artifacts unchanged
          └─ success
              ├─ selected authority exists
              │   → sync_all(selected active/recovery) content preflight
              │       ├─ failure → no store + RequiredBarrierUnavailable
              │       └─ success → capability-approved private startup flow
              └─ store is missing
                  → create/write/flush/validate non-authoritative staging
                  → sync_all(staging) content preflight
                      ├─ failure → no store + RequiredBarrierUnavailable; no authority
                      └─ success → capability-approved private startup flow
```

`DurabilitySupportError` distinguishes `NoPhysicalBacking`, compile-time
unsupported target, and required barrier unavailable. `RecoveryError` gains an
additive unsupported-durability variant for file-backed initialization. A failed
parent preflight leaves artifacts unchanged. A failed missing-store content
preflight leaves no authority and removes staging when deterministic cleanup
succeeds; if cleanup also fails, only diagnosed non-authoritative staging may
remain. Permission, space, media, and transient failures after successful
preflight remain `RecoveryError::Io` with explicit operation/path.

Capability approval is an internal milestone, not public support. Public physical
construction is exposed only after the capability, fresh publication,
active-authority replacement, recovery-authority replacement, cleanup, and
crash/reopen matrices are GREEN for all three store families.

## Fresh Publication Authority

```text
Missing active
  → ParentDirectoryPreflighted
  → StagingCreated
  → CompleteHeaderWritten/Flushed/Validated
  → StagingFileSynchronizedAndContentPreflighted
  → AppendHandlePrepared
  → RenamedToActive
  → [Physical] PublicationDirectorySynchronized
  → Writable
```

- Before rename, staging is never authoritative.
- A parent-directory preflight failure creates or changes no artifact.
- A staging synchronization failure is a capability failure, creates no
  authority, and triggers deterministic staging cleanup.
- After rename but before a successful physical directory barrier, publication is
  indeterminate; initialization returns no store and leaves the complete active
  artifact for deterministic reopen.
- The prepared handle names the same inode after rename; no fallible reopen is
  introduced solely for fresh handoff.

## Active-Authority Replacement

```text
Active(authority) + Staging(validated and file-synchronized)
  → ActiveRenamedToRecovery
  → [Physical] BackupDirectorySynchronized
  → StagingRenamedToActive
  → [Physical] PublicationDirectorySynchronized
  → ActiveReopenedAndValidated(authority)
  → RecoveryRemoved
  → [Physical cleanup claimed] CleanupDirectorySynchronized
```

The backup-directory barrier makes recovery the durable authority before final
publication. If final publication fails, recovery remains. Cleanup begins only
after active publication is physically authoritative. Cleanup or cleanup-barrier
failure is deferrable and never removes the new active.

## Recovery-Authority Replacement

```text
Recovery(authority) + optional obsolete Active + Staging(validated/synchronized)
  → ObsoleteActiveRemovedIfProven
  → StagingRenamedToActive
  → [Physical] PublicationDirectorySynchronized
  → ActiveReopenedAndValidated(authority)
  → RecoveryRemoved
  → [Physical cleanup claimed] CleanupDirectorySynchronized
```

Recovery remains untouched until active publication is committed, so no
intermediate authority barrier is needed after obsolete-active removal.

## Durable Test Images

The test-only model keeps two views:

| View | Changes | Crash behavior |
|---|---|---|
| volatile | Every write, truncate, rename, and remove | Discarded unless covered by the corresponding barrier |
| durable | Updated only by successful file-data/full or directory barriers | Restored as the simulated post-power-loss filesystem |

After simulated loss, acceptance tests open the restored view with normal public
initializers and assert only public results and reads. The model does not exist in
normal builds.
