# Implementation Plan: Explicit Durable Write Acknowledgements

**Branch**: `codex/005-durable-write-policy` | **Date**: 2026-08-07 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/005-durable-write-policy/spec.md`

## Summary

Add a runtime-only `DurabilityPolicy` to the existing store options. Existing
constructors remain buffered; file-backed options may explicitly select physical
durability. Physical mutation acceptance writes and flushes one complete logical
mutation under the existing WAL acceptance lock, performs one file-data barrier,
and only then advances accepted WAL state and publishes live state. A failure
rolls back to the preceding checkpoint and physically synchronizes that rollback;
successful truncate plus rollback synchronization is a confirmed rejection, while
an unconfirmed rollback returns an indeterminate error and fails the store closed.

Creation and recovery publication retain issue #4's staged files and add parent-
directory barriers at every authority-changing namespace transition. Linux and
macOS physical mode are enabled only when runtime directory and content preflights
succeed. For a missing store, the parent is probed first and validated staging
`sync_all` is the content preflight before any authority rename. Any preflight
failure returns `RequiredBarrierUnavailable`; later failures remain ordinary
operational I/O errors. Public physical construction is promoted only after every
fresh, active-authority, recovery-authority, and cleanup path is GREEN. Windows
and other unsupported targets expose no physical store. No persisted-format,
dependency, unsafe-code, or lock-graph change is required.

## Technical Context

**Language/Version**: Rust 2021 edition; validation toolchain Rust 1.97.0; no declared MSRV

**Primary Dependencies**: Existing `dashmap` 3.11.10, `crc32fast`, `bincode`, `log`, and Rust standard filesystem/synchronization APIs; no new production dependency

**Storage**: One append-only local WAL per store family, backed by `File` or `Vec<u8>`; current legacy and V1 bytes remain unchanged; durability policy is runtime-only and never persisted

**Testing**: `cargo test`; crate-unit scripted write/flush/data/full-barrier and rollback seams; deterministic volatile-versus-durable byte/namespace models; public integration construction, mutation, callback, concurrent-order, process/reopen, and compatibility tests; ignored release benchmarks; formatting, strict Clippy, docs, and three-platform CI

**Target Platform**: Buffered mode remains supported on Linux, macOS, and Windows. Strict physical mode is supported on Linux and macOS only when runtime file-content and parent-directory preflights succeed; any preflight failure returns `RequiredBarrierUnavailable`, while Windows and other compile-time unavailable environments return `UnsupportedPlatform`

**Project Type**: One Rust library crate with its existing migration binary; this feature changes only the supported library surface and private persistence implementation

**Performance Goals**: Preserve all 36 buffered issue #3/#4 cells. Add 18 file-backed physical cells and 18 matching minimal append-plus-barrier reference cells. Each one-worker median throughput ratio is at least 0.90, each eight-worker ratio at least 0.85, and each p95 latency ratio at most 1.25 against its matching baseline/reference, with 11 measured samples per side. Protocol v5 links the complete pre-feature comparator and candidate in one release process pinned to CPUs 12–19 and measures every comparison as five warmup plus eleven measured counterbalanced AB/BA pairs. It retains the established start-only schedule for buffered baseline/candidate pairs and per-operation rendezvous for physical/reference pairs, so every comparison has matching process state, CPU placement, and scheduling without injecting barrier-tail noise into microsecond buffered calls

**Constraints**: RED–GREEN one behavior at a time; current constructors remain buffered; exactly one direct data barrier per physical logical mutation and never one shared across calls; physical rollback uses truncate plus full file synchronization; no live publication before the required barrier; cancellation while the existing key/set async callback is pending releases the key guard with no WAL/live change, while the post-callback persistence segment is synchronous and non-yielding; directory preflight precedes missing-store staging and no authority changes before staging content preflight; public physical construction remains unavailable until all startup paths are GREEN; no new whole-operation global mutex, per-key registry, group-commit coordinator, persisted-format change, manual delayed-flush policy, replication, unsafe code, or production dependency; unsupported physical mode never downgrades

**Scale/Scope**: Three store families; six persisted action kinds; single- and multi-record mutations; every public mutator and callback outcome; existing key/set async callback cancellation; write, flush, data-barrier, truncate, rollback-barrier, and failed-closed boundaries; fresh, active-authority, and recovery-authority publication paths; Linux/macOS supported and Windows unsupported CI behavior; 54 independently gated comparison cells and a 72-row final report

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design.*

### Pre-research gate

- **I. RED–GREEN TDD**: PASS — internal options, in-memory rejection, each physical single-record/multi-record barrier, each failure/rollback transition, every namespace barrier, and every fallible adapter are separate runtime RED–GREEN slices. Existing buffered behavior is characterized with first-execution-GREEN compatibility tests before physical I/O and rerun after every physical slice; it is never forced to fail artificially. Public configuration/construction promotion occurs only over complete GREEN private behavior and must pass on first exposure; compilation failure is not RED evidence.
- **II. Durable/live integrity**: PASS — physical success is ordered `complete WAL bytes → direct data barrier → accepted WAL state → live publication`. Confirmed rollback produces `Rejected`; unconfirmed rollback produces `Indeterminate` and fails closed. Physical construction is not publicly exposed until every content/directory preflight and authority-publication path is GREEN.
- **III. Compatibility**: PASS — default policy, existing signatures, panic wrappers, callback eligibility, result shapes, V1/legacy bytes, reopening without options, and pending-callback async cancellation remain unchanged. Safer paths are additive and existing `std::io::Result` compute signatures are preserved.
- **IV. Bounded concurrency/performance**: PASS — the existing WAL write guard remains the only shared persistence-ordering boundary. Physical mode extends each logical mutation by exactly one direct barrier; barriers are not shared between calls and group commit is outside scope. Buffered mode gains no I/O or lock. Fixed 36/18/18 benchmark matrices precede production edits.
- **V. Public evidence/scope**: PASS — private fault seams only schedule failures and model discarded cache state; acceptance uses fallible operation results, public reads, callbacks, byte artifacts, and normal reopen. Group commit, format redesign, and unrelated WAL findings remain excluded.
- **Project constraints**: PASS — all three durable families are covered. The design adds no dependency or unsafe code; platform-specific behavior is capability-gated and tested. Buffered behavior remains three-platform.
- **Clarification gate**: PASS — all recorded decisions are represented; no `NEEDS CLARIFICATION` remains.

### Post-design gate

- **RED–GREEN delivery**: PASS — [quickstart.md](quickstart.md) orders baseline capture first, then internal policy, buffered first-GREEN characterization, in-memory rejection, physical behavior, capability/publication behavior, and only then public physical construction. Each new behavior has an adjacent runtime RED–GREEN pair; each store-family adapter is a separate first-execution-GREEN promotion.
- **State authority**: PASS — [data-model.md](data-model.md) defines the mutation acceptance, rollback/fail-closed, reopen, and staged namespace authority transitions.
- **Public compatibility**: PASS — [durability-api.md](contracts/durability-api.md) fixes additive options, construction errors, fallible mutators, nested legacy results, panic wrappers, and programmatic failure classification.
- **Physical acknowledgement**: PASS — [durable-acknowledgement.md](contracts/durable-acknowledgement.md) fixes barrier ordering, lock ownership, rejection versus indeterminate outcomes, and complete/incomplete reopen behavior.
- **Publication safety**: PASS — [physical-publication.md](contracts/physical-publication.md) places file and parent-directory barriers at fresh, active-authority, recovery-authority, and cleanup boundaries without removing the last authority.
- **Platform boundary**: PASS — current Rust standard-library behavior supports strong file barriers on Linux/macOS. Existing stores preflight the parent and selected file; missing stores preflight the parent before using validated staging synchronization as the content preflight. Any preflight failure is `RequiredBarrierUnavailable`; Windows returns explicit unsupported without FFI, unsafe code, or best-effort semantics.
- **Concurrency/performance**: PASS — one direct barrier per logical mutation reuses the existing WAL guard; shared barriers and group commit require a separate feature. [performance.md](contracts/performance.md) fixes 54 matching comparison cells, immutable provenance, a 72-row final report, and per-cell thresholds.
- **Research completeness**: PASS — policy, platform support, steady and rollback barriers, public errors, fallible mutation inventory, namespace sequencing, fault models, and benchmarks are resolved in [research.md](research.md).

## Project Structure

### Documentation (this feature)

```text
specs/005-durable-write-policy/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── durability-api.md
│   ├── durable-acknowledgement.md
│   ├── physical-publication.md
│   └── performance.md
├── benchmarks/
│   ├── README.md
│   ├── baseline.csv / baseline.md
│   ├── reference.csv / reference.md
│   ├── attempts/<capture-id>.csv / <capture-id>.md
│   └── final.csv / final.md
├── checklists/requirements.md
└── tasks.md                         # Generated by $speckit-tasks
```

### Source Code (repository root)

```text
src/
├── lib.rs                           # Re-export durability policy/errors
├── config.rs                        # Runtime policy and options
├── durability.rs                    # Private platform barriers/capability probe
├── recovery.rs                      # Additive support/sync operations and errors
├── key_value_store.rs               # Fallible mutators + compatibility wrappers
├── key_set_store.rs                 # Fallible mutators + compatibility wrappers
├── key_map_store.rs                 # Fallible mutators + compatibility wrappers
├── test_support/
│   ├── mod.rs
│   ├── fault_writer.rs          # Scripted barriers and durable-byte model
│   └── durability_snapshot.rs   # Test-only namespace durability model
└── wal/
    ├── mod.rs                       # Policy, barrier, rollback, health/error state
    ├── recovery.rs                  # Capability-gated staged publication barriers
    └── durability_tests.rs          # Private deterministic state-machine tests

tests/
├── durable_write_policy.rs
└── durable_write_policy/
    ├── support.rs
    ├── contract.rs
    ├── key_value.rs
    ├── key_set.rs
    ├── key_map.rs
    ├── compatibility.rs
    ├── recovery.rs
    └── performance.rs

.github/workflows/recovery.yml       # Linux/macOS physical and Windows rejection lanes
```

**Structure Decision**: Keep the existing single-crate layout and shared WAL.
`src/durability.rs` contains only private platform/capability operations; it is
not a new synchronization or public abstraction layer. `WalState` stores a small
policy enum with function pointers so generic test writers can model barriers
without changing `Durable*Store<W>` bounds or using dynamic dispatch. Public
integration tests stay separated by store family and consume only supported APIs;
private scripted scheduling remains in crate-unit tests.

## Design Overview

### Runtime policy and platform capability

`DurabilityPolicy::{Buffered, Physical}` is public, non-exhaustive, `Copy`, and
defaults to `Buffered`. `DurableStoreOptions` carries it beside timestamp
granularity. The policy is not written to the WAL; every open chooses the current
process policy, and reopening without options uses buffered compatibility behavior.

Private file-backed startup receives the selected policy before any authority-
changing cleanup, repair, publication, or writable handle is exposed. On Linux
and macOS, startup first inspects candidates without mutation and then performs a
parent-directory preflight. If an existing complete authority is selected, a
non-destructive full synchronization of that file is the content preflight. If no
store exists, the parent preflight occurs first; startup then creates, writes,
flushes, validates, and fully synchronizes non-authoritative staging, and that
required staging synchronization is the content preflight before rename.

Any directory or content preflight failure returns
`RequiredBarrierUnavailable { operation, path, source }` regardless of its I/O
kind. A failed directory preflight leaves artifacts unchanged. Failed fresh
content preflight runs deterministic staging cleanup and may leave only diagnosed
non-authoritative staging if cleanup also fails. After both preflights succeed,
permission, media, capacity, and other publication/runtime failures remain
operation/path-aware recovery or mutation I/O errors. Windows and other targets
without the safe standard-library contract return `UnsupportedPlatform`.
Buffered paths do not probe or synchronize.

In-memory options gain a fallible constructor returning `NoPhysicalBacking` for
physical mode. Existing infallible vector options constructors delegate and panic
with an actionable diagnostic, preserving their signatures. The policy types and
all file/vector construction adapters remain private during development and are
promoted publicly only after the full physical mutation, failure, capability, and
namespace-publication system is GREEN across all three store families.

### Mutation acceptance, rollback, and lock order

The lock order remains:

```text
DashMap entry/shard
  → existing WAL write guard
      → encode + write_all + flush
      → physical only: sync_data
      → accept offset/timestamp
  → release WAL guard
  → publish live state/callback
  → release entry/shard
```

The barrier stays inside the existing WAL guard. Every call receives exactly one
direct barrier for its complete logical mutation; it is never shared with another
call. Releasing the guard between append and barrier is forbidden because a failed
earlier mutation could otherwise truncate a later append. Shared barriers, group
commit, waiter coordination, and shared failure propagation require a separately
approved feature. One complete compute group receives one barrier, not one per
physical record.

Every write, flush, or data-barrier error invokes rollback while the WAL guard is
held. Buffered rollback keeps current semantics. Physical rollback truncates to
the captured offset and calls `sync_all` so exceptional file-length metadata is
durably restored. Successful truncate plus rollback synchronization produces a
confirmed `Rejected` result and permits later mutations. Failure of either step is
`Indeterminate` and fails closed. Offset and last timestamp bucket remain
unchanged until the original barrier succeeds.

```text
Ready → BytesWritten → Flushed → DataSynchronized → Accepted
  └─ write/flush/data failure
       → TruncateToCheckpoint
           ├─ failure → FailedClosed + Indeterminate
           └─ success → SynchronizeRollback
                           ├─ failure → FailedClosed + Indeterminate
                           └─ success → Ready + Rejected
```

After indeterminate failure, later mutations return `FailedClosed` before touching
the writer. The current instance never publishes the attempted state. On reopen,
a complete valid group is replayed and an incomplete group follows issue #4's
terminal-tail recovery rule.

### Fallible public mutation contract

New `try_*` counterparts return `std::io::Result` for every mutator that currently
only panics. Existing set/map `try_compute*` signatures remain unchanged. Methods
with existing domain results nest those results inside the I/O result. Existing
methods delegate to the fallible form and retain their success value, callback
eligibility, and panic behavior.

A public non-exhaustive `MutationFailure` is stored as the source of returned
`io::Error` values and can be recovered with `MutationFailure::from_io_error`.
It distinguishes confirmed `Rejected`, `Indeterminate`, and subsequent
`FailedClosed`, while `PersistenceOperation` identifies write, flush, data sync,
rollback, or rollback sync. This preserves existing `io::Result` signatures and
adds programmatic classification without string parsing.

The existing key/set `try_compute_async` callback remains the only yield point.
Dropping its future while that callback is pending releases the per-key guard,
discards the private working copy, and performs no WAL I/O, barrier, accepted-state
advance, or live publication. Once the callback returns `Ready`, persistence runs
synchronously in the same poll with no cancellation point and reaches the same
success/failure contract as synchronous compute. External side effects performed
inside a user callback remain outside library rollback control.

### Physical staged publication and public exposure

Staging contents continue to use `sync_all` before rename. Physical mode adds a
parent-directory barrier after each authority-changing namespace transition. No
public physical constructor is connected until every sequence below, its failure
matrix, and all three family adapters are GREEN:

- **Fresh**: preflight the parent; create, flush, and validate staging; use
  `sync_all(staging)` as the content preflight; prepare the append handle; rename
  staging to active; synchronize the parent; then expose the already-prepared
  handle. Preflight failure changes no authority. Directory-sync failure after
  rename returns no store and leaves the complete active artifact for deterministic
  reopen.
- **Active authority repair/configuration**: synchronize staging; ensure any stale
  recovery artifact is proven obsolete; rename active to recovery; synchronize
  the parent so recovery is the durable authority; rename staging to active;
  synchronize the parent so active is authoritative; reopen/validate; remove the
  recovery artifact only afterward. Cleanup plus cleanup-directory failure is
  deferred because active authority is already durable.
- **Recovery authority**: synchronize staging while recovery remains untouched;
  remove any proven-obsolete active if required; rename staging to active;
  synchronize the parent; reopen/validate; then remove and synchronize cleanup of
  recovery. No barrier between obsolete-active removal and publication is needed
  for authority because recovery remains intact.

Buffered publication retains existing issue #4 behavior and barriers; the new
directory barriers apply only to explicit physical mode. Cleanup never destroys
the last complete authority and never attempts to undo an indeterminate rename.

### Deterministic durability and performance evidence

The scripted writer gains separate write, flush, data-barrier, truncate, and full-
barrier faults plus volatile and durable byte images. Simulated power loss drops
the volatile image. Namespace tests maintain a test-only shadow snapshot updated
only at file/directory barrier checkpoints; a simulated crash restores that
snapshot and then opens the store through normal public APIs. Private seams choose
failure timing only; assertions use operation results, public reads/callbacks,
artifact bytes, and reopen outcomes.

Performance reuses five warmup pairs, eleven measured pairs, fixed 32-byte data,
minimum 100 ms/1,024 operations, ordinary write/successful removal/minimal callback,
and one/eight workers. Buffered captures 36 pre-change cells. Physical and its
minimal `Mutex<File>` `write_all → flush → sync_data` reference each capture
18 file-backed cells on the same explicit real-filesystem benchmark root.
Protocol v5 links both implementations in one release process and alternates the
comparator/candidate order within every AB/BA pair. It gives buffered
baseline/candidate cells one round-start rendezvous;
physical/reference cells additionally rendezvous after the preceding operation
and before each timed public call. The paired process is pinned to logical CPUs
12–19 after verifying eight distinct physical cores with no SMT siblings. The
public-call timer begins after whichever rendezvous the policy requires. All 54
comparisons produce 1,188 measured CSV rows in one write-once capture. Every cell
passes independently; the user is asked for a quiet-machine window before the
complete paired acceptance capture.

## TDD Delivery Strategy

1. Create only test roots, benchmark schema, scripted barrier seams, and immutable provenance; run the full baseline and capture 36 buffered plus 18 reference cells before production edits.
2. RED/GREEN private `DurabilityPolicy` default and selection without connecting production I/O.
3. Add first-execution-GREEN buffered single-record/multi-record characterization proving zero barrier calls, zero capability probes, and byte-for-byte/write/flush compatibility before any physical I/O implementation; rerun it after every later physical slice.
4. RED/GREEN private in-memory physical-policy rejection one store family and one construction path at a time.
5. RED/GREEN one direct physical single-record logical mutation barrier after write/flush and before accepted-state advance; then RED/GREEN one direct barrier for a complete multi-record set mutation and map mutation independently.
6. RED/GREEN confirmed rollback for write, flush, and data-barrier failures one at a time, including truncate plus full rollback synchronization and unchanged offset/timestamp/live state.
7. RED/GREEN truncate failure and rollback-barrier failure independently as indeterminate; prove subsequent operations fail closed without writer/barrier calls.
8. RED/GREEN a blocking data barrier showing old public state until release and durable/replayed state after successful completion; treat complete/incomplete issue #4 reopen behavior as first-execution-GREEN conformance unless a concrete gap is observed.
9. RED/GREEN private `MutationFailure` classification and one internal fallible mutator/result/callback shape at a time; keep all new public durability configuration and mutation adapters unexposed.
10. RED/GREEN unsupported target behavior; parent-directory preflight success/failure and artifact identity; existing-file content preflight; and missing-store directory-first plus staging-content-preflight behavior one invariant at a time.
11. RED/GREEN fresh staging creation/write/validation, content preflight, same-handle preparation, rename, directory publication, and every failure checkpoint independently.
12. RED/GREEN active-authority staging, backup rename/directory barrier, replacement rename/directory barrier, reopen, cleanup, and cleanup barrier independently; repeat the recovery-authority path without an unnecessary pre-publication barrier.
13. RED/GREEN volatile/durable byte and namespace crash models, then run every preflight/publication checkpoint through public-state-shaped private assertions while physical construction remains unexposed. Existing complete/incomplete issue #4 reopen behavior is first-execution-GREEN conformance unless a concrete gap produces a focused runtime RED.
14. Run the complete private capability, fresh, active-authority, recovery-authority, cleanup, crash/reopen, and three-family matrix as the physical-construction exposure gate.
15. Promote `DurabilityPolicy`, support/failure errors, and file/vector construction adapters one family at a time only after step 14 is GREEN; each public construction contract must pass on first execution.
16. Expose one public fallible mutator and compatibility wrapper/result/callback shape at a time over GREEN internals, requiring first-execution-GREEN public contracts and rerunning the family suite after each.
17. Run all-family physical mutation matrices, callback counts, pending-callback async cancellation/guard release, same/different-shard progress/deadlock conformance, issue #1–#4 reopen/fixture regressions, and Linux/macOS/Windows CI behavior.
18. After all correctness and quality gates are GREEN, pause for quiet-machine confirmation and capture all 54 candidate cells under a unique attempt ID. If a focused failure proves a protocol mismatch, preserve every capture, amend the protocol without changing thresholds, recapture complete affected comparator matrices from the pre-feature commit, obtain a new quiet-machine confirmation, and recapture all 54 candidate cells. Otherwise make the minimum GREEN production optimization and rerun every quality gate before approval/recapture. Only a complete passing attempt may become `final.csv` and feed the 72-row report. Threshold changes, shared barriers, or group commit require a new approved specification.

## Complexity Tracking

No constitution violation requires an exception. The private `WalDurability`
function-pointer enum extends the existing WAL boundary without a new lock or
trait-bound. The test-only durable-state model is required by FR-023 because an
ordinary process crash cannot emulate power loss of OS-acknowledged cache state.
Linux/macOS capability gating and explicit Windows rejection avoid both unsafe FFI
and a new dependency while honoring the no-best-effort clarification.
