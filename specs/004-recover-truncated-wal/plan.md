# Implementation Plan: Boundary-Aware WAL Recovery

**Branch**: `004-recover-truncated-wal` | **Date**: 2026-08-06 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/004-recover-truncated-wal/spec.md`

## Summary

Add a file-discriminated V1 WAL whose physical actions form explicitly committed
logical-mutation groups. Checked replay exposes only complete groups, classifies a
structurally incomplete terminal action record separately from corruption, and
repairs the selected accepted logical state through staged publication before the
store becomes writable. Creation of a missing store likewise writes and validates
a complete header in staging before atomically publishing the active path; every
pre-publication failure leaves the active path absent. Normal startup rejects every complete legacy-format WAL
with a structured migration-required error. A project-provided offline
`pigment-db-migrate` command reads a quiescent legacy database, exclusively creates
a separate destination, writes and validates wholly V1 outputs, and never modifies
the source. V1 groups also persist CRC-protected timestamp buckets with additive
granularity options and a one-minute default.

## Technical Context

**Language/Version**: Rust 2021 edition; validation toolchain Rust 1.97.0; no declared MSRV

**Primary Dependencies**: Existing `crc32fast` 1.4.2, `bincode` 1.3.3, `dashmap` 3.11.10, `log`, and standard filesystem/time/process APIs; no new runtime dependency or CLI parser dependency

**Storage**: One append-only local WAL per store family in a database directory, backed by `File` or `Vec<u8>`; legacy native-endian frames remain readable only by strict migration validation; V1 uses a file header and little-endian grouped frames; offsets remain `u32` pending issue #9

**Testing**: `cargo test`; crate-unit byte-cut, parser, fault-writer, clock, and subprocess seams; public integration reopen/compatibility tests; `std::process::Command` CLI tests; ignored release benchmarks; formatting, strict Clippy, docs, and three-platform CI

**Target Platform**: Linux, macOS, and Windows under the existing single-process-per-store-directory model; migration additionally requires an offline/quiescent source

**Project Type**: One Rust library package with an additive standalone binary target; the supported library API remains separate from the CLI-only migration interface

**Performance Goals**: Reuse all 36 issue #3 steady-state cells. Each one-worker median throughput ratio is at least 0.90, each eight-worker ratio at least 0.85, and each p95 latency ratio at most 1.25. A one-million-operation torn-tail recovery is at most 1.25 times matching complete-startup median over at least 11 samples.

**Constraints**: RED–GREEN one behavior at a time; no production checkpoint behavior before its runtime RED; never write a fresh V1 header directly to active; preserve an existing invalid header unchanged; preserve current signatures and behavior except the approved `MigrationRequired` legacy outcome; no legacy/V1 mixing or startup migration; migration source is read-only and destination must not exist; one flush per logical mutation; no `sync_data`/`sync_all` on steady-state writes; no action-meaning change, global mutation lock, historical retention, point-in-time startup, offset-width migration, or cryptographic authentication

**Scale/Scope**: Three store families, six action kinds, single- and multi-record mutations, every action-record byte cut, all protected fields at first/middle/final positions, staged new-store header publication, nine repair/publication checkpoints, distinct migration source/destination checkpoints, three reopens per recovered history, complete/truncated/corrupt legacy inputs, directory-level CLI migration, default/non-default timestamp granularities, clock rollback across restart, 36 performance cells, and a one-million-operation startup comparison

## Constitution Check

*GATE: Passed before research and re-checked after Phase 1 design.*

### Pre-research gate

- **I. RED–GREEN TDD**: PASS — each header/frame invariant, new-store publication checkpoint, mutation-group behavior, repair checkpoint, legacy rejection, private migration source/destination checkpoint, timestamp behavior, and private runner behavior is delivered as its own runtime RED followed by minimum GREEN. Pure codecs precede filesystem pipelines. Invocation-owned cleanup success and cleanup failure are GREEN before downstream failure handlers compose them, and no successful pipeline may implement a later failure checkpoint before that checkpoint's RED. Public option adapters and the binary bridge are behavior-preserving refactors over GREEN private behavior and must pass their first public tests; missing symbols and intentionally incorrect adapters are forbidden as RED evidence.
- **II. Durable/live integrity**: PASS — a missing store exposes no active path until a complete staged header validates and publishes; only a complete V1 group advances accepted state; repair publishes a validated logical snapshot without modifying its selected authority; migration opens sources read-only and writes only an exclusively created destination.
- **III. Compatibility**: PASS — the specification explicitly approves `MigrationRequired` for normal legacy startup and provides a tested migration path. Frozen inputs remain immutable, V1 is distinguished, action meanings remain, and other startup/API behavior is unchanged.
- **IV. Bounded concurrency/performance**: PASS — group/timestamp assignment stays inside the existing WAL acceptance lock; no new global or per-key coordination exists; the fixed baseline precedes production edits and every cell passes independently.
- **V. Public evidence/scope**: PASS — private seams inject clocks, writes, and process checkpoints only; acceptance uses public reads, reopen outcomes, CLI exit/output, and source/destination bytes. Issues #5/#9, authentication, and point-in-time retention remain excluded.
- **Project constraints**: PASS — all store families and three platforms are covered; the same package gains one no-dependency binary target, no unsafe code, and no additional supported migration library API.
- **Clarification gate**: PASS — all eleven recorded decisions are reflected and no planning unknown remains.

### Post-design gate

- **RED–GREEN delivery**: PASS — [quickstart.md](quickstart.md) separates pure codec tracers, new-store publication checkpoints, checkpoint-specific repair/migration failures, later GREEN end-to-end regressions, and public adapters that must pass on first exposure.
- **Authority and recovery**: PASS — [data-model.md](data-model.md) models staged new-store publication, complete/pending/corrupt V1 groups, migration-required legacy input, V1 authority selection, staged repair, and offline migration states.
- **Persisted compatibility**: PASS — [wal-v1.md](contracts/wal-v1.md) fixes collision-free discrimination, immutable legacy decoding, file-header rejection, checked arithmetic, full CRC coverage, and no mixed grammar.
- **Fresh publication**: PASS — [fresh-v1-publication.md](contracts/fresh-v1-publication.md) makes active visibility the commit point, proves every source-less pre-publication failure leaves active absent, and keeps pre-existing invalid headers immutable.
- **Public/CLI compatibility**: PASS — [recovery-api.md](contracts/recovery-api.md) defines the additive options and `MigrationRequired`; [migration-cli.md](contracts/migration-cli.md) fixes the only supported migration interface, exit behavior, and immutable-source/no-overwrite contract.
- **Failure evidence**: PASS — [fresh-v1-publication.md](contracts/fresh-v1-publication.md) and [tail-recovery.md](contracts/tail-recovery.md) define one proof per new-store and repair checkpoint respectively. Fresh publication proves role-bounded staging cleanup before later pre-commit failures and uses current-initialization handle return—not post-commit interruption—as the handoff RED. Migration proves destination-directory ownership registration, cleanup success, and cleanup failure before output creation or later checkpoint handlers compose cleanup; its remaining contracts cover initial/final source reads, synchronization, validation, source-change detection, and process interruption.
- **Concurrency/performance**: PASS — the existing WAL lock remains the only group/timestamp acceptance lock; migration is offline; steady-state writes retain one flush and issue #3 shard order; no benchmark failure can be averaged away.
- **Research completeness**: PASS — versioning, fresh header publication, grouping, truncation classification, repair, authority, timestamp API, CLI packaging, destination publication, native-endian limits, and constitutional TDD sequencing are resolved in [research.md](research.md).

## Project Structure

### Documentation (this feature)

```text
specs/004-recover-truncated-wal/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── wal-v1.md
│   ├── fresh-v1-publication.md
│   ├── recovery-api.md
│   ├── tail-recovery.md
│   └── migration-cli.md
├── checklists/requirements.md
└── tasks.md                         # Regenerated by $speckit-tasks
```

### Source Code (repository root)

```text
Cargo.toml                           # Declares pigment-db-migrate binary
src/
├── lib.rs                           # Existing exports plus hidden binary bridge
├── config.rs                        # Additive options/granularity contract
├── recovery.rs                      # Existing outcomes plus MigrationRequired
├── migration.rs                     # Crate-private migration engine/state
├── migration_cli.rs                 # Crate-private args/output/exit runner
├── bin/
│   └── pigment-db-migrate.rs        # Thin CLI-only adapter
├── key_value_store.rs               # Default/options initialization adapters
├── key_set_store.rs                 # Default/options initialization adapters
├── key_map_store.rs                 # Default/options initialization adapters
└── wal/
    ├── mod.rs                       # Group acceptance, timestamps, rollback state
    ├── model/mod.rs                 # Existing action payloads and legacy frames
    ├── format.rs                    # File discriminator and V1 header/frame codec
    ├── replay.rs                    # Strict legacy/V1 replay outcomes
    ├── recovery.rs                  # V1 authority selection and staged repair
    └── truncation_tests.rs          # Byte cuts, corruption, clocks, fault points

tests/
├── truncated_wal.rs
├── truncated_wal/
│   ├── support.rs
│   ├── contract.rs
│   ├── key_value.rs
│   ├── key_set.rs
│   ├── key_map.rs
│   ├── compatibility.rs
│   └── performance.rs
├── migration_cli.rs
├── migration_cli/
│   ├── support.rs
│   ├── contract.rs
│   ├── compatibility.rs
│   ├── failures.rs
│   └── process.rs
└── fixtures/legacy/                # Immutable inputs

.github/workflows/recovery.yml       # Fast V1 and CLI targets on three OSes
```

**Structure Decision**: Keep one Cargo package and one supported library surface.
`src/migration.rs` owns the private filesystem/conversion state machine;
`src/migration_cli.rs` owns only argument parsing, reporting, and exit mapping.
`Cargo.toml` declares the stable binary target at `src/bin/pigment-db-migrate.rs`.
The binary calls a single `#[doc(hidden)]` zero-argument package bridge because
Cargo binaries are separate crates; all migration options, results, errors,
parsers, codecs, and filesystem operations remain crate-private and absent from
the supported library contract. A private core crate or copied/path-included WAL
parser was rejected as more complex and more prone to safety-critical drift.

## Design Overview

### V1 file and logical group boundary

Every writable artifact is wholly V1. For a missing store, startup exclusively
creates staging, writes and flushes the complete 40-byte header, reads the persisted
bytes back for strict validation, performs startup synchronization, and prepares the
same append-capable handle at offset 40 before publishing by rename. The active
path is the commit point: every reported pre-publication failure leaves it absent;
an interruption after publication observes a complete valid active header and is a
completed creation, never a partial header. The already-prepared handle is handed
off without a new fallible filesystem step after publication. An existing zero-byte file is legacy-format input and requires
explicit migration. The header identifies store kind, timestamp unit and
granularity, base bucket, flags, and CRC. A partial or corrupt header already present
at startup is preserved as invalid, never reconstructed or treated as an empty store.

Successful exclusive creation registers exactly the invocation-created staging
path. A role-bounded cleanup transition proves both successful removal and exact
diagnostic leftovers on removal failure before later header-write or publication
failure handlers are introduced. Every subsequent pre-commit failure composes that
already-GREEN transition and cannot target active, recovery, unresolved, or
unrelated paths. Post-commit interruption is a GREEN rename-publication regression;
the distinct handoff RED observes that the current initialization cannot yet return
the already-prepared writable handle and complete its first append without another
fallible filesystem step.

Each action record contains protected payload length/complement, physical and
mutation offsets, group index/count, timestamp bucket, unchanged action payload,
repeated offset footer, and CRC32. A single action uses index 0/count 1. Compute
batches know the count before encoding. Replay buffers all member effects and
advances a logical prefix only when member `count-1` validates. The existing order
remains `data shard → WAL acceptance → live publication` with one flush.
Header/frame codecs remain disconnected from release startup and steady-state writes
until their structural, checked-bounds, CRC, and round-trip invariants are GREEN.
Public activation is a later RED–GREEN slice. No new blocking edge is added:
operations meeting at the existing WAL acceptance boundary may wait during encode,
write, flush, and bucket acceptance, while independent shard preparation retains
issue #3 progress behavior; its deterministic progress/deadlock suite remains a gate.

### Replay, authority, and staged repair

Byte zero selects V1 or legacy grammar once. V1 replay returns `Complete`,
`RecoverableTail`, or structured corruption. Only an incomplete action record or
open logical group after a complete valid V1 header is recoverable. A complete
field contradiction, length complement mismatch, CRC, offset, payload, group, or
timestamp violation is corruption. Every protected-field corruption is exercised
at first, middle, and final record positions.

Artifact roles are named active, recovery (dot-prefixed), and staging; “legacy” is
reserved for byte format. Authority selection compares accepted V1 logical
snapshots before any byte change. If any potentially authoritative artifact is
complete legacy format, normal startup preserves all artifacts and returns
`MigrationRequired`; truncated/corrupt legacy returns `InvalidArtifact`. It never
converts or cleans a legacy candidate. V1 ambiguity returns
`AuthorityUndetermined`.

After a V1 tail is selected, repair encodes the accepted logical state as one
complete snapshot group (or header-only empty state) and publishes it via
exclusive `.next` creation, write, flush, validation, startup synchronization,
close, rename, and exact-length reopen. Direct `set_len` is forbidden. Each create,
write, flush, validate, sync, publish, reopen, blocking cleanup, and deferrable
post-publication cleanup behavior receives its own RED before implementation.

### Offline legacy migration CLI

`pigment-db-migrate --source <LEGACY_DIR> --destination <V1_DIR>` treats the
database directory as the migration unit. It accepts only long options through
`args_os`, plus `--help`, `--version`, and optional
`--timestamp-granularity-nanos`. It discovers only canonical active files
`kv.wal.dat`, `set.wal.dat`, and `map.wal.dat`, requires at least one, and migrates
all found families together. Canonical filenames provide the store kind even for
empty/delete-only histories. It rejects V1 input, truncated/corrupt legacy,
unrecognized file types at canonical paths, and any recognized recovery/staging
artifact rather than guessing authority.

The source must be quiescent and is opened read-only. Before destination creation,
the engine validates all legacy inputs with native-endian, payload-only CRC rules
and retains their exact bytes and snapshots. It exclusively creates the destination
directory; an existing file, directory, or symlink is an error. Outputs use only
V1, bucket zero, and the default or explicitly validated granularity. Each output
is create-new, written, flushed, synchronized, closed, reopened, strictly replayed,
and compared with its legacy snapshot. The source is reread before success to
detect violated offline ownership.

Initial source open/read failure and final stability-reread failure are independent
source-I/O behaviors returning exit 3; a successful reread whose bytes differ is
the distinct source-changed outcome returning exit 7. The in-memory legacy-to-V1
snapshot codec is proven before filesystem publication. Destination directory
creation first registers the invocation-owned directory. Successful reverse-order
cleanup and cleanup-removal failure are then proven in separate runtime RED–GREEN
pairs using test-owned registered paths. Only after both cleanup transitions are
GREEN are output creation, partial write, flush, sync, close,
reopen/read, strict validation, final reread, and changed-source handlers introduced;
every such handled failure composes cleanup and asserts exact removed/remaining
paths. Only after all checkpoint slices are GREEN may the complete migration
success/parity regression pass. Process-interruption tests run only after the GREEN
private runner and thin executable exist.

Handled failure best-effort removes only exact destination files/directory created
by that invocation; cleanup failure or process interruption may leave a partial
destination. Such a destination is never reported successful and is never
overwritten on retry. Success is exit 0 after every family validates and the source
stability check passes. Exit 2 is usage/configuration, 3 source I/O, 4 invalid or
non-migratable source, 5 existing destination, 6 destination/validation/cleanup
failure, and 7 source changed. The CLI never panics for input or I/O failures.

Legacy integers have no endian marker, so migration is supported only on an
architecture matching the legacy writer's endianness; the tool never guesses an
alternate interpretation. V1 output is little-endian and portable thereafter.

### Timestamp configuration

`TimestampGranularity` validates a nonzero duration representable as `u64`
nanoseconds; default is 60 seconds. `DurableStoreOptions` carries it. Existing
initializers use persisted V1 granularity or the default for a missing new store.
They never open legacy. Additive file/vector option initializers may change an
existing V1 granularity through staged compaction. The migration CLI uses one
minute unless its explicit granularity option is valid.

Under the existing WAL lock, acceptance floors Unix wall-clock nanoseconds to the
configured duration and clamps it to the previous accepted bucket. State advances
only after successful write/flush; replay restores it; offsets order equal buckets.
Compaction and tail repair preserve the last bucket. A migrated legacy snapshot
starts at bucket zero. Test-only clocks are absent from normal builds.

### Compatibility and failure boundaries

`RecoveryError` remains non-exhaustive and gains `MigrationRequired { path }`.
Fallible initializers return it for complete legacy format; compatibility wrappers
retain their signatures and panic with an actionable migration-command diagnostic.
`RecoveryStatus::Recovered` remains the success status for V1 repair/artifact
resolution; no status is returned for migration-required failure.

Write/flush rejection retains issue #3 rollback and fail-closed behavior. A complete
final group may replay after an unobserved caller completion; a partial group never
appears. Steady-state physical synchronization remains issue #5. Existing callbacks,
pop results, numeric behavior, key semantics, action identifiers, and `u32` format
limit do not change.

## TDD Delivery Strategy

1. Capture immutable source hashes, fixtures, full suite, and the 36-cell/startup baseline before production edits.
2. RED/GREEN the pure V1 header codec one field/invariant at a time, including checked arithmetic and strict partial/corrupt-header rejection; no active-file publication exists yet.
3. RED/GREEN missing-store candidate inspection and the role-bounded staging cleanup transition first. Staging create registers its exact invocation-owned path; every later partial-write, flush, persisted-byte read/validation, synchronization, append-handle preparation, and rename failure composes the already-GREEN cleanup behavior. After rename is GREEN, add the post-commit interruption regression and require it to pass immediately. Then use successful current-initialization return of the same prepared writable handle and first append as the separate handoff RED/GREEN pair.
4. RED/GREEN the exact single-action frame codec one protected field/invariant at a time; give offset overflow and steady-state one-flush behavior their own REDs.
5. RED/GREEN complete legacy startup rejection and `MigrationRequired` before adding any migration engine.
6. RED/GREEN a pure in-memory complete-legacy-to-V1 snapshot codec and store-family parity without destination filesystem behavior.
7. RED/GREEN migration source discovery, initial open/read I/O, strict validation, final reread I/O, and changed-byte detection as separate behaviors before destination publication.
8. RED/GREEN destination directory creation and exact ownership registration, then prove successful reverse-order cleanup and cleanup-removal failure separately with test-owned registered paths. Only after both cleanup behaviors are GREEN, RED/GREEN output creation, partial write, flush, sync, reopen/read after ordinary close/drop, strict validation, final reread, and source-change handling; every handled failure composes the proven cleanup transition. Use process interruption for the non-reportable close boundary. Only then add complete directory migration success and logical-parity regressions.
9. RED/GREEN private CLI parsing, output, and exit cases one behavior at a time; expose the thin binary bridge only after the runner is GREEN, require its first basic executable contract to pass, and add child-process interruption regressions afterward.
10. RED/GREEN pure single-action tail classification and accepted-state snapshot encoding. Then prove staged repair create/write/flush/validate/sync/publish/reopen/cleanup checkpoints one at a time before connecting the successful repair pipeline and every action-record cut.
11. RED/GREEN grouped compute encoding/replay one behavior at a time and prove every member cut is all-or-none.
12. RED/GREEN protected fields individually and run the complete `field × first/middle/final record` matrix only as a later regression.
13. RED/GREEN default/non-default/internal option, reopen, clock rollback, rejection rollback, equal-bucket order, and compaction behaviors. Expose public option adapters only after the internal path is GREEN; public tests pass on first exposure.
14. Run all-store public matrices, migration fixtures, deterministic progress/deadlock conformance, issue #1–#3 regressions, three-platform CI, quality gates, and final performance only after correctness is GREEN.

## Complexity Tracking

No constitution violation requires an exception. The additive binary is required by
FR-037. Its single hidden zero-argument bridge is an implementation boundary, not a
supported migration library API; it avoids both a second private crate and duplicated
safety-critical parsers while keeping the CLI contract explicit and testable.
