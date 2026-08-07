# Contract: Fresh V1 Header Publication

## Entry classification

Fresh creation is entered only after validated options and candidate inspection
prove that active and recovery are absent and no unresolved staging artifact blocks
exclusive creation. The following observed states are disjoint:

| Observed state | Outcome |
|---|---|
| active/recovery absent; staging absent or safely resolved | Fresh publication may begin |
| existing zero-byte active | Complete empty legacy; `MigrationRequired` |
| existing partial/corrupt V1 header | `InvalidV1Header`; preserve exact bytes |
| existing complete V1/legacy/recovery candidate | Normal authority classification; never fresh fallback |
| staging role cannot be proven or cleaned safely | Fail closed; active remains absent |

Validation failure never reclassifies an existing path as missing. A pre-existing
partial header is diagnostic evidence and is not deleted, reconstructed, or routed
through fresh creation.

## Publication protocol

For file-backed initialization:

1. Exclusively create same-directory staging; active remains absent.
2. Write exactly the complete 40-byte header and flush it.
3. Read back the persisted staged bytes; require exact length 40 and strict V1
   validation of magic, version, header length, kind, unit, flags, granularity,
   base bucket, reserved bytes, and CRC.
4. Perform the existing startup-maintenance synchronization.
5. Prepare the same staging handle for append at offset 40 before publication.
6. Rename staging to the absent active path. This rename is the commit point.
7. Hand the already-prepared handle to the WAL without another fallible filesystem
   operation; return the normal new-store startup outcome.

Every failure before step 6 returns no store and leaves active absent. Cleanup may
remove only the staging path created or proven disposable by this invocation; if
cleanup fails, staging may remain diagnostic but is never authoritative. A process
interruption after step 6 observes an exact, complete, validated 40-byte active
header and is treated as committed creation, never as failed partial publication.

### Cleanup ownership and sequencing

The publisher keeps an invocation-local cleanup registry that is empty before
exclusive creation and contains exactly the staging path immediately after that
creation succeeds. Before any header write, flush, readback, validation,
synchronization, handle preparation, or rename failure behavior is implemented,
one focused RED–GREEN slice proves the role-bounded cleanup transition for both
successful removal and injected removal failure.

Every later pre-commit failure handler invokes that already-GREEN transition. A
successful cleanup leaves no invocation-created staging path. A cleanup failure
preserves the exact registered staging artifact, reports both the original
checkpoint and cleanup operation/path, and never broadens the removal target.
Pre-existing active, recovery, unrelated, or unresolved paths are never registered
and therefore cannot be removed by this transition.

Vector-backed initialization has no pathname publication boundary, but uses the
same validated header codec and exposes the vector only after all 40 bytes validate.

## Required RED–GREEN checkpoints

Each behavior receives its own runtime RED immediately before minimum GREEN:

| Checkpoint | Required evidence |
|---|---|
| invalid options/candidate inspection | No filesystem mutation; existing artifacts unchanged |
| role-bounded staging cleanup | Only the registered staging target is attempted; success removes it and injected cleanup failure reports the exact diagnostic leftover while active remains absent |
| exclusive staging create | No later checkpoint; active absent; exact staging path becomes the sole invocation-owned cleanup target |
| every partial header write | Cuts 0–39 never create active; the proven cleanup transition yields either no staging or the exact diagnostic bytes and cleanup error |
| flush | No validation/sync/publish after failure; exact cleanup result asserted |
| persisted-byte read/length | Incomplete stored header never synchronizes or publishes; exact cleanup result asserted |
| strict staged validation | Wrong header/config/CRC never synchronizes or publishes; exact cleanup result asserted |
| synchronization | Unsynchronized header never publishes; exact cleanup result asserted |
| append-handle preparation | No publish until writable handoff can be guaranteed; exact cleanup result asserted |
| publish/rename | Rename failure leaves active absent and routes through the proven cleanup transition |
| post-commit prepared-handle handoff | Current initialization returns the same append-capable handle at offset 40 without any post-commit filesystem checkpoint; first append succeeds |

Post-commit interruption is a GREEN regression added immediately after rename
publication is GREEN, not a RED for handle handoff: it proves that active is exactly
40 valid bytes and that next startup opens normally. The handoff RED instead fails
because the current successful initialization cannot yet return the already-prepared
writable handle without a new fallible filesystem operation.

Every pre-commit test asserts active absence, no writable handle, no later checkpoint
invocation, exact untouched artifacts, the exact registered cleanup targets and
cleanup result, and deterministic next startup. Public matrices cover all three
families, default/non-default
granularity, missing versus zero-byte versus every 1–39-byte existing prefix,
header corruption, staging states, first append, and reopen.
