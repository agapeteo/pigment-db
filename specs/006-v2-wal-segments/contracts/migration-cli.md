# Contract: Offline V2 Migration and Compaction

## Invocation

```text
pigment-db-migrate --source <SOURCE_DIR> --destination <NEW_V2_DIR> [--timestamp-granularity-nanos <NONZERO_U64>]
```

## Preconditions

- Source is a readable directory containing at least one canonical family WAL.
- Destination does not exist, is outside the source tree, and is not an alias of the source.
- No unresolved recovery (`.NAME`) or staging (`.NAME.next`) artifact exists.
- Timestamp granularity is nonzero.

## Accepted sources

- Frozen complete legacy WAL.
- Complete V1 WAL.
- V1 WAL with a structurally recoverable terminal record/group tail.
- Complete one-segment V2 WAL.
- Complete segmented V2 chain consisting of deterministic sealed names plus active.

## Output

- Always V2.
- One active segment per discovered family and no sealed segment in the destination.
- Logical state equivalent to the last complete accepted source prefix.
- Requested output granularity with the source's last accepted timestamp bucket preserved.
- Exact reopened validation before success is reported.

## Failure and cleanup

- Existing destination is never overwritten.
- Source bytes are never modified or deleted.
- Invocation-owned destination artifacts are cleaned on a handled failure when possible.
- Cleanup failure is reported in addition to the original failure.
- Source artifacts are reread before success; any changed, missing, or newly introduced chain artifact rejects migration.
