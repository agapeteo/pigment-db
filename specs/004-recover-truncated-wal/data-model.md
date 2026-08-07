# Data Model: Boundary-Aware WAL Recovery

## File Header V1

| Field | Meaning | Validation |
|---|---|---|
| magic/version/header length | Whole-file format discriminator | Exact supported constants; partial header is invalid |
| store kind | Key/value, key/set, or key/sorted-map | Must match requested/canonical store |
| timestamp unit | Unix nanoseconds | V1 constant |
| granularity | Active bucket width | Nonzero `u64` nanoseconds |
| base bucket | Last accepted bucket represented by compacted base | Restored before append; never decreases |
| flags/reserved | Future expansion | Zero in V1 |
| header CRC32 | Header integrity | Covers every preceding header byte |

The header is configuration metadata, not a logical mutation. Header-only V1 is
a complete empty store. A partial/corrupt 40-byte header is preserved as invalid,
never reconstructed or treated as a recoverable action tail.

Header/frame codecs remain private and disconnected from startup and steady-state
writes until every required structural, bounds, CRC, and round-trip invariant is
GREEN. Public activation is a separate runtime RED–GREEN slice.

## New Store Publication

```text
Missing active
  → StagingReserved
  → HeaderWritten
  → Flushed
  → PersistedBytesReadAndValidated
  → Synchronized
  → AppendHandlePrepared(offset 40)
  → Published(active commit point)
  → Writable
```

Every transition before `Published` has its own failure proof and leaves the
active path absent. Successful staging creation registers that exact path as the
only invocation-owned cleanup target. The role-bounded cleanup transition is proven
before later pre-commit failure handlers compose it; successful cleanup removes the
registered staging path, while cleanup failure leaves and reports only that exact
diagnostic artifact. It never makes a partial header authoritative. A crash
after `Published` observes a complete validated header and is treated as committed
creation. Handoff after publication uses the already-prepared handle and introduces
no new fallible filesystem transition. A partial/corrupt header already present at
startup is not part of this state machine and remains preserved invalid.

## Physical Action Record V1

| Field | Meaning | Validation |
|---|---|---|
| marker/version/header length | V1 record identity | Exact constants or matching partial constant prefix at terminal EOF |
| action | Existing action identifier 0–5 | Supported by selected store/payload decoder |
| payload length/complement | Payload boundary | `complement == !length`; checked end within `u32` |
| physical start/footer | Position including file header | Both equal actual start |
| mutation start | First record offset for group | Common to group; equals first physical start |
| index/count | Group membership | `count > 0`; contiguous `index=0..count-1` |
| timestamp bucket | Logical acceptance time | Common to group; not below last accepted bucket |
| payload | Existing serialized action data | Existing store-specific meaning |
| CRC32 | Accidental-corruption check | Covers every other record byte |

## Logical Mutation Group

A group is one caller-visible mutation. Constituent actions remain private until
the final declared member validates.

```text
Absent → Pending(index 0..count-1) → Accepted(final member valid) → Applied
                         └────────→ RecoverableTail at terminal EOF
Any complete invariant/CRC/payload failure → Corrupt
```

Only `Accepted` creates an authority prefix. A pending group never appears. A
complete final group may replay after a crash even when the caller did not observe
completion.

## Checked Replay Result

| Variant | Data | Startup meaning |
|---|---|---|
| V1Complete | snapshot, accepted prefixes, byte length, last bucket/config | Candidate may enter V1 authority selection |
| V1RecoverableTail | accepted snapshot/prefixes, mutation start, last bucket/config | Candidate may be repaired only if selected |
| V1Corrupt | offset and reason | Preserve and fail |
| InvalidV1Header | available header bytes and reason | Preserve and fail; never repair |
| LegacyComplete | snapshot, source bytes, native-endian format | Normal startup returns `MigrationRequired`; CLI may migrate |
| LegacyInvalid | validated prefix/reason | Preserve and fail; CLI cannot migrate |

## Artifact Authority

Active, recovery (dot-prefixed), and staging retain issue #1 roles; none is called
“legacy” because legacy now names a byte format. Staging is never authoritative.
Complete/recoverable V1 candidates compare only accepted logical snapshots.
Selection precedes repair. Unprovable V1 combinations return
`AuthorityUndetermined`. A potentially authoritative legacy-format candidate
prevents normal cleanup/publication and returns `MigrationRequired` if complete or
`InvalidArtifact` if not complete.
Fresh publication begins only after this inspection proves active and recovery
absent and resolves staging under existing rules. Repair never invokes the fresh
state machine for an existing invalid candidate.

## Timestamp Configuration and State

`TimestampGranularity` is a nonzero duration fitting `u64` nanoseconds. Default is
60,000,000,000 ns. `WalState` owns granularity and last accepted bucket under its
existing lock. A requested bucket is floored wall time clamped to the last bucket;
rollback does not advance it. Reopen uses the maximum of header base bucket and
complete group buckets. Equal buckets use accepted group position.

## Repair State Machine

```text
Observed V1 candidates
  → strict checked replay
  → authority selected
  → complete: normal open/maintenance
  → recoverable tail: encode accepted logical snapshot
      → exclusive staging create
      → write → flush → validate → sync → close
      → publish rename → exact-length reopen
      → writable; status Recovered
  → any pre-publication failure: no writable store; source remains authority
  → post-publication cleanup failure: defer only when new active is proven authority
```

Repair output is one complete snapshot group, or a header-only empty snapshot,
with the last bucket/granularity preserved. Each arrow has an independent failure
test before its production behavior is added. This authority-preserving state
machine is distinct from source-less fresh publication.

## Migration Source

A migration source is an offline existing directory containing at least one
canonical active legacy file (`kv.wal.dat`, `set.wal.dat`, `map.wal.dat`). Canonical
names identify store kind. Validation requires:

- every recognized active file is complete legacy format;
- no recognized recovery/staging artifact exists;
- no canonical input is V1, truncated, corrupt, or incompatible with its kind;
- every source file is opened read-only and retained as exact original bytes;
- the source remains byte-identical through the final stability check.

## Migration Destination

A destination is an explicit path that does not exist as a file, directory, or
symlink. The CLI exclusively creates it and then exclusively creates one wholly V1
active file per source family. Outputs use bucket zero and selected/default
granularity. A destination becomes successful only after all output files are
flushed, synchronized, closed, reopened, strictly V1-validated, and logically
equal to their source snapshots.

```text
Observed
  → PreflightValidated
  → DestinationReserved
  → OutputCreated
  → Writing
  → Flushed
  → Synchronized
  → Reopened
  → OutputValidated
  → SourceStable
  → Complete(exit 0)
Any transition → Failed → ExactCleanupAttempted
```

A failed/interrupted destination is never overwritten or reported successful.
Retry requires a different absent path or explicit operator handling. The source
remains the rollback authority in every state. Fault evidence records the reached
checkpoint, exact created paths, original source-byte snapshots, and whether any
later checkpoint ran. Initial source read and final source reread I/O failures are
distinct exit-3 transitions; a successful reread with changed bytes is exit 7.
Success is legal only from `SourceStable` and is tested after every prior transition
has its focused RED–GREEN proof.

## CLI Outcome

| Exit | Meaning | Required artifact condition |
|---:|---|---|
| 0 | Success/help/version | Migration success only after full validation; help/version mutate nothing |
| 2 | Usage/configuration error | Source and destination unchanged |
| 3 | Source I/O error | No source write; destination not successful |
| 4 | Invalid/non-migratable source | Source unchanged; no successful destination |
| 5 | Destination already exists | Existing destination and source unchanged |
| 6 | Destination/validation/cleanup failure | Source unchanged; partial destination never successful |
| 7 | Source changed during migration | Non-success; output cleaned best-effort |
