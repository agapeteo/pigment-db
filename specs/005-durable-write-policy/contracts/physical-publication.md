# Contract: Physical Store Publication

## Scope

This contract applies only when a file-backed store is explicitly opened with
`DurabilityPolicy::Physical`. Buffered startup retains its existing issue #4
publication behavior and performs no new capability probe or directory barrier.

A successful physical startup means both:

1. the complete selected file contents passed a full file barrier; and
2. every namespace transition needed to make that file authoritative passed a
   parent-directory barrier.

The implementation must reject physical startup before exposing a store if either
barrier is unavailable. It must not silently downgrade to buffered behavior.

Capability approval alone is not a public exposure point. Public physical
construction remains unavailable until fresh publication, active-authority
replacement, recovery-authority replacement, cleanup, crash/reopen, and all
three store-family matrices are GREEN.

## Authority Terms

- **active**: the normal database filename used by callers.
- **recovery**: a complete preserved authority used while replacing active.
- **staging**: a complete candidate that is not authoritative before publication.
- **authority transition**: a rename or removal that changes which pathname
  protects the recoverable state.
- **publication barrier**: `sync_all` on the parent directory after a namespace
  transition.

At every fallible checkpoint, at least one complete artifact must remain. The
implementation never removes the last complete authority and never guesses that
an indeterminate rename can be safely undone.

## Capability Gate

Before physical startup performs cleanup, repair, publication, or returns a
writable store, it must establish the required capabilities on the actual backing
filesystem in this order:

1. Reject a compile-time unsupported target.
2. Inspect active/recovery authority without mutating any artifact.
3. Open and synchronize the parent directory.
4. If a selected active/recovery authority exists, fully synchronize that file.
5. If the store is missing, create/write/flush/validate non-authoritative staging
   and fully synchronize staging before any authority rename. This required
   `sync_all(staging)` is the content preflight; no second probe file is created.

Linux and macOS may proceed only after the applicable runtime preflights succeed.
Windows and other targets without a safe standard-library directory-entry barrier
return `UnsupportedDurability`. Every open or synchronization failure during
preflight returns `RequiredBarrierUnavailable` with capability, path, and source,
regardless of error kind or raw OS code. A failed parent preflight leaves all
artifacts unchanged. A failed missing-store content preflight leaves no authority
and removes staging when deterministic cleanup succeeds; only diagnosed
non-authoritative staging may remain if cleanup also fails. Once both preflights
succeed, later permission, capacity, media, transient, or barrier failures are
ordinary operation/path-aware recovery I/O errors.

For an existing selected authority, preflight must precede stale-staging cleanup,
repair, replacement creation, or any other namespace mutation. The selected
authority and all namespace artifacts remain unchanged if either preflight fails.

## Fresh Store Publication

The required order is:

```text
target support gate
  -> inspect missing authority without mutation
  -> sync_all(parent directory)                 # namespace preflight
  -> write complete staging header
  -> flush staging
  -> validate staging
  -> sync_all(staging)                          # content preflight
  -> prepare append-capable handle
  -> rename(staging, active)
  -> sync_all(parent directory)
  -> expose the prepared handle and report success
```

Rules:

- Staging is not authoritative before the rename.
- Parent-directory preflight failure creates or changes no artifact.
- Staging content-preflight failure creates no authority and triggers
  deterministic staging cleanup. If that cleanup fails, the error diagnoses the
  remaining non-authoritative staging artifact.
- No store is exposed before the directory barrier succeeds.
- The append-capable handle is prepared before rename, so success does not depend
  on a new fallible reopen after authority has changed.
- If the rename succeeds but the directory barrier fails, startup returns an
  error and leaves the complete active artifact in place. It does not attempt an
  inverse rename whose durable outcome would also be unknown.
- On the next startup, normal authority selection determines whether active is
  durable and valid.

## Active-Authority Replacement

When active is the selected authority and startup must publish a replacement,
the required order is:

```text
create, flush, validate, and sync_all(staging)
  -> prove any pre-existing recovery artifact obsolete before removing it
  -> rename(active, recovery)
  -> sync_all(parent directory)              # recovery becomes durable authority
  -> rename(staging, active)
  -> sync_all(parent directory)              # replacement becomes durable authority
  -> reopen and validate active
  -> remove recovery
  -> sync_all(parent directory) if cleanup is reported complete
```

The barrier after `active -> recovery` is mandatory. Without it, a crash between
the two renames could lose the only durably named authority. If that barrier
fails, publication stops and recovery is left for the next startup. If final
publication or validation fails, recovery remains untouched.

Cleanup begins only after active is durably published and validated. A cleanup or
cleanup-directory-barrier failure may be reported or deferred according to the
existing recovery outcome, but it must not invalidate or remove the new active
authority.

## Recovery-Authority Replacement

When recovery is already the selected authority, the required order is:

```text
leave recovery untouched
  -> create, flush, validate, and sync_all(staging)
  -> remove active only if it is proven obsolete
  -> rename(staging, active)
  -> sync_all(parent directory)              # active becomes durable authority
  -> reopen and validate active
  -> remove recovery
  -> sync_all(parent directory) if cleanup is reported complete
```

No intermediate authority barrier is required after removing an obsolete active:
recovery remains the durable authority until the new active publication barrier
succeeds. Recovery must not be removed before active has been published and
validated.

## Cleanup Semantics

- Cleanup is ordered strictly after durable replacement authority.
- Removing an obsolete artifact is not claimed durable until the parent directory
  barrier succeeds.
- Failure to remove an obsolete artifact leaves a safe duplicate and is
  distinguishable from publication failure.
- Failure of the cleanup directory barrier leaves cleanup indeterminate; the new
  authority remains valid and later startup re-evaluates the remaining artifacts.
- Cleanup never attempts to compensate by removing the newly published active.

## Failure and Crash Matrix

| Boundary | Startup result | Required surviving authority after simulated power loss |
|---|---|---|
| compile-time target unsupported | `UnsupportedDurability` | artifacts unchanged |
| parent-directory preflight fails | `RequiredBarrierUnavailable(DirectoryEntry)` | artifacts unchanged |
| existing selected-file content preflight fails | `RequiredBarrierUnavailable(FileContent)` | selected authority and namespace unchanged |
| missing-store staging content preflight fails | `RequiredBarrierUnavailable(FileContent)` | no authority; staging removed, or diagnosed non-authoritative staging if cleanup also fails |
| replacement staging write/flush/validation/file barrier fails after preflight | recovery I/O error | previous active or recovery |
| fresh rename fails | error | no claimed store; staging may remain |
| fresh directory barrier fails | error | complete active may remain for deterministic reopen |
| active-to-recovery rename fails | error | active |
| backup directory barrier fails | error | active or recovery as restored durable namespace selects |
| staging-to-active rename fails | error | recovery |
| publication directory barrier fails | error | recovery or complete active as durable namespace selects |
| active reopen/validation fails | error | recovery retained |
| post-publication recovery removal fails | durable publication with deferred/failed cleanup | active |
| cleanup directory barrier fails | durable publication with indeterminate cleanup | active plus any restored obsolete artifact |

Tests update a shadow durable namespace only after successful directory barriers,
discard volatile changes at every row, and reopen through public initialization.
They assert the selected public state and artifacts rather than private flags.

The complete matrix is first proven through private construction seams. Only
after every row, cleanup outcome, crash/reopen checkpoint, and store family is
GREEN may the public physical options constructor delegate to that implementation;
the public adapter itself is expected to be GREEN on first execution.
