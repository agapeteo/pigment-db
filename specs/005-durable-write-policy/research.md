# Phase 0 Research: Explicit Durable Write Acknowledgements

## Decision 1: Runtime-only buffered and physical policies

**Decision**: Add public non-exhaustive `DurabilityPolicy::{Buffered, Physical}`
to `DurableStoreOptions`. `Buffered` is the default. The policy applies only to
the currently opened instance and is not persisted in either legacy or V1 bytes.

**Rationale**: This directly implements the clarified compatibility choice. The
options fields are already private, so adding another `Copy` field is additive.
Runtime selection lets an application choose a different policy on reopen without
a WAL migration.

**Alternatives considered**: Physical-by-default and physical-only were rejected
by clarification. Persisting policy was rejected because it would change the V1
format and incorrectly turn a process policy into historical data.

## Decision 2: Use a file-data barrier for steady mutation acceptance

**Decision**: In physical mode call `File::sync_data()` exactly once after the
complete logical mutation has been written and flushed and before WAL offset,
timestamp, live state, callback, or success advances. A multi-record compute group
gets one barrier after its final record.

**Rationale**: Rust documents `sync_data` as the content-oriented alternative to
`sync_all`, intended to avoid unnecessary metadata work. Linux `fdatasync` also
persists metadata required to retrieve data, including file length; Windows maps
it to `FlushFileBuffers`; current Rust 1.97 uses `F_FULLFSYNC` on Apple targets.
This gives the strongest supported steady-state guarantee with the least required
work. [Rust `File` synchronization](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_data), [Linux `fsync`/`fdatasync`](https://man7.org/linux/man-pages/man2/fsync.2.html), [Rust 1.97 Unix implementation](https://github.com/rust-lang/rust/blob/1.97.0/library/std/src/sys/fs/unix.rs#L1391-L1425)

**Alternatives considered**: `flush` alone is the original defect. `sync_all` on
every append is safe but does avoidable metadata work. A background barrier returns
too early and remains buffered semantics. A manual `sync` method does not satisfy
per-mutation physical acknowledgement.

## Decision 3: Reuse the existing WAL acceptance lock

**Decision**: Perform the physical barrier while holding the existing
`RwLock<WalState<W>>::write` guard. Preserve lock order `DashMap entry/shard →
WAL acceptance → live publication`; the WAL never acquires a data shard.

**Rationale**: The single append-only WAL already serializes offset assignment,
write, flush, rollback, and timestamp acceptance. The barrier is part of that same
authority transition. Releasing the lock before it completes would allow a failed
earlier mutation to truncate later appended mutations.

**Alternatives considered**: A store-wide mutation mutex would serialize callback
preparation and publication. A second per-key registry duplicates DashMap without
removing the single-WAL boundary. Releasing the guard requires an epoch/group
coordinator and suffix-wide failure handling that is not required by this feature.

## Decision 4: Require one direct barrier per logical mutation

**Decision**: Implement exactly one direct barrier per accepted logical mutation
and never share that barrier across calls. Add no queue, condition variable,
leader epoch, waiter registry, or shared-barrier completion state in issue #5.

**Rationale**: The clarified specification explicitly selects direct per-call
barriers. This is the smallest safe implementation, matches the append-plus-
barrier reference, and keeps coordination at the existing WAL acceptance guard.
Group commit would add cancellation, panic, error broadcast, retained waiter, and
shared rollback semantics while callers still own DashMap guards.

**Alternatives considered**: Shared barriers or group commit are outside issue #5
and require a separately approved feature. A measured reference-gate failure must
first be fixed through allocation or write-path optimization without weakening the
direct acknowledgement contract.

## Decision 5: Physically synchronize rollback and fail closed when uncertain

**Decision**: On write, flush, or data-barrier failure, truncate the WAL to the
captured checkpoint and call `sync_all()` before accepting another mutation.
Successful truncate plus full barrier is a confirmed rejection. Truncate or
rollback-barrier failure is indeterminate and changes WAL health to failed closed;
later mutations return before any writer or barrier call.

**Rationale**: Rollback changes file-length metadata. Although `sync_data` covers
required length metadata on the initially supported platforms, `sync_all` is the
more conservative exceptional-path operation and has no steady-state performance
cost. Failing closed prevents new appends from depending on an unproved end offset.

**Alternatives considered**: Current `set_len` without synchronization can be
lost on power failure. Continuing after rollback failure risks appending after an
unknown authority. Deleting the file would destroy diagnostic and potentially
authoritative bytes.

## Decision 6: Preserve `std::io::Result` and add typed source classification

**Decision**: All new fallible mutators return `std::io::Result<existing-result>`.
Add public non-exhaustive `MutationFailure` and `PersistenceOperation`; embed the
failure as the returned `io::Error` source and provide
`MutationFailure::from_io_error`. Classify `Rejected`, `Indeterminate`, and
`FailedClosed` programmatically.

**Rationale**: Existing public set/map `try_compute*` methods already return
`std::io::Result<()>`; changing them would be source-breaking. A typed source
avoids string parsing while preserving established signatures and familiar I/O
composition. Underlying error kinds are retained for confirmed rejection; the two
state errors use `Other`.

**Alternatives considered**: Changing every try method to
`Result<_, MutationFailure>` breaks existing callers. String-only diagnostics are
not safely classifiable. Adding a parallel `*_detailed` API for every compute
variant would duplicate an already large public surface.

## Decision 7: Add one fallible counterpart per missing mutator

**Decision**: Add the following methods and make existing compatibility methods
thin panic wrappers:

- Key/value: `try_put`, `try_compute`, `try_increment_or_init`, `try_decrement`,
  `try_set_number`, `try_remove`.
- Key/set: `try_append`, `try_remove_from_set`,
  `try_remove_from_set_callback`, `try_remove_key`; retain existing
  `try_compute*` methods.
- Key/sorted-map: `try_put`, `try_remove_from_sorted_map`,
  `try_remove_from_sorted_map_callback`, `try_remove_key`, `try_pop_first`,
  `try_pop_last`, `try_append_ordered_element`; retain existing `try_compute*`.

Domain results nest inside I/O results, for example
`io::Result<Option<(SearchKey, Vec<u8>)>>` and
`io::Result<Result<u64, ()>>`.

**Rationale**: Every mutation shape can propagate a persistence failure without
altering old return, callback, or panic behavior. Moving existing bodies into the
fallible methods also ensures guards unwind before compatibility wrappers panic.

**Alternatives considered**: Exposing only puts leaves deletes, pops, callbacks,
numeric operations, and compute batches unable to report physical failures.
Replacing old methods or flattening domain errors would be breaking.

## Decision 8: Gate physical startup with phase-based preflights

**Decision**: Add fallible vector options constructors returning
`DurabilitySupportError::NoPhysicalBacking`; existing vector option constructors
delegate and panic. File-backed physical mode supports Linux and macOS only after
runtime parent-directory and content preflights succeed. Startup inspects authority
without mutation, preflights the parent, then either fully synchronizes the
selected complete active/recovery file or, for a missing store, creates and
validates non-authoritative staging whose required `sync_all` is the content
preflight. Windows and other compile-time unsupported targets return
`UnsupportedPlatform`.

Every open or synchronization failure while performing a preflight maps to
`RequiredBarrierUnavailable { operation, path, source }` regardless of
`io::ErrorKind` or raw OS code. A failed directory preflight changes no artifact.
A failed missing-store content preflight changes no authority and may leave only
diagnosed staging if deterministic cleanup also fails. Failures after both
preflights succeed remain structured operation/path-aware I/O errors. Public
physical construction remains unavailable until all publication paths are GREEN.

**Rationale**: In-memory storage cannot satisfy physical persistence. Linux
requires an explicit directory descriptor `fsync` in addition to file sync. Rust
1.97 maps Apple file synchronization to `F_FULLFSYNC`, and runtime preflights
safely enforce the actual backing filesystem boundary. Treating every preflight
failure uniformly avoids brittle cross-platform errno interpretation. Reusing
fresh staging as the content preflight avoids a second probe file and its own
namespace/cleanup state machine. Rust std does not expose a documented Windows
directory-entry barrier through ordinary `File`; implementing one would require
platform FFI/unsafe code or a dependency without a confidently equivalent
contract. [Linux directory-sync requirement](https://man7.org/linux/man-pages/man2/fsync.2.html), [Apple `F_FULLFSYNC`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html), [Windows `FlushFileBuffers`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)

**Alternatives considered**: Error-kind or raw-errno allowlists were rejected as
incomplete and brittle. A dedicated temporary content-probe file was rejected
because it adds namespace mutations and cleanup failure paths. No-op or automatic
buffered downgrade violates the clarification. Windows
`MoveFileEx(..., MOVEFILE_WRITE_THROUGH)` does not provide a complete std-based
file-and-directory contract and is deferred. Linux-only support was rejected
because current Rust supplies the required Apple full barrier and runtime
preflights can enforce the remaining boundary.

## Decision 9: Synchronize every authority-changing directory transition

**Decision**: Keep `sync_all` on a complete staged file. In physical mode:

1. Fresh creation preflights the parent, writes/validates staging, uses staging
   `sync_all` as the content preflight, renames staging to active, synchronizes the
   parent, then exposes the already-prepared handle.
2. Active-authority replacement synchronizes staging, renames active to recovery,
   synchronizes the parent, renames staging to active, synchronizes the parent,
   validates/reopens, then cleans recovery.
3. Recovery-authority replacement leaves recovery untouched through staging and
   active publication, synchronizes the parent after staging-to-active, validates,
   then cleans recovery.
4. A claimed durable cleanup synchronizes the parent after removal. Cleanup or
   cleanup-barrier failure after new authority is durable may defer without making
   the store unsafe.

**Rationale**: File synchronization alone does not guarantee that its directory
entry reached disk. The intermediate directory barrier after active-to-recovery is
essential: a crash between two renames must still leave one durably named
authority. [Linux `fsync(2)` directory note](https://man7.org/linux/man-pages/man2/fsync.2.html)

**Alternatives considered**: One directory barrier only after final publication
leaves a two-rename window with no established name. Undoing a rename after a
failed directory barrier guesses an indeterminate namespace outcome. Performing
new directory barriers in buffered mode changes its compatibility cost.

## Decision 10: Use deterministic durable-state models, not process exit alone

**Decision**: Extend the scripted writer with independent data/full barrier faults,
blocking, counts, and separate volatile/durable byte images. Add a test-only
namespace snapshot model updated only at explicit file/directory checkpoints.
Simulated power loss discards volatile-only bytes/names; tests then reopen the
restored durable view through normal public APIs.

**Rationale**: Killing a process does not discard the operating system's page
cache and therefore cannot prove power-loss durability. The model makes every
unbarriered write, rename, and removal deterministically lossy while public results
and reopen assertions remain the acceptance evidence.

**Alternatives considered**: Real power cycling is destructive and unsuitable for
normal tests. Sleep/timing tests are nondeterministic. Private-state-only assertions
would violate the constitution's public-evidence rule.

## Decision 11: Fix 54 comparison cells and a 72-row final report

**Decision**: Reuse issue #3/#4 methodology: five warmups, at least eleven samples,
fixed 32-byte data, at least 100 ms and 1,024 operations per sample, three store
families, ordinary write/successful removal/minimal callback, and one/eight
workers. Before production edits, capture 36 buffered baseline cells and 18
matching minimal `Mutex<File>` append-plus-barrier reference cells on one explicit
real-filesystem root. After implementation, capture 36 candidate buffered cells
and 18 candidate physical file cells. Evaluate 36 buffered and 18 physical
comparisons independently. The final report displays 36 buffered comparison rows,
18 physical candidate rows, and 18 reference rows. Record writes/second and p95
latency. Protocol v5 links pre-feature and candidate crates in one release process
and captures each comparison as five warmup plus eleven measured counterbalanced
AB/BA pairs. Buffered pairs use one round-start rendezvous. Physical/reference
pairs additionally rendezvous before every timed public call after completing the
preceding call; this keeps barrier wait outside call latency while preventing
either implementation from hiding serialized queue wait through repeated
same-thread lock reacquisition. The paired process is pinned to logical CPUs
12–19.

**Rationale**: Buffered and physical paths have different legitimate costs. The
buffered baseline catches compatibility regressions; the reference isolates
avoidable library overhead from device barrier latency. The clarified thresholds
apply to each matching cell and cannot be averaged.

The first complete candidate capture exposed that the original start-only
rendezvous let the minimal reference monopolize 64 consecutive mutex acquisitions;
its p95 excluded queue wait while real store calls necessarily performed per-key
preparation/publication between WAL acquisitions. A focused RED reproduced a
`24.654488x` ratio despite matching throughput, and replacing the existing WAL
`RwLock` with `Mutex` remained RED at `26.800922x`. The lock experiment was
reverted. Per-operation rendezvous made the focused physical comparison GREEN
without a production change or threshold relaxation.

The complete protocol-v2 recapture then passed all 18 physical cells but failed
two of 36 buffered cells only on microsecond-scale eight-worker p95. Isolated
reruns of the same buffered key/map path crossed the `1.25` threshold in both
directions while throughput remained above its floor; inline/direct-core
experiments did not stabilize it and were reverted. This demonstrates scheduler-
tail variance from applying the physical fairness barrier to buffered calls, not
stable avoidable production overhead.

Protocol v3 therefore selected scheduling by policy. Buffered baseline/candidate
pairs use the exact start-only protocol-v1 schedule; physical/reference pairs use
the exact per-operation protocol-v2 schedule. It reuses the immutable protocol-v1
buffered baseline (`6a4ca0b81f504459462c3870f0da1ce244a08313fa3afbf35284d104db3a3196`)
and protocol-v2 physical reference
(`26db4de357a656e63c0f11e02f850542eba326795221ccbb21048edc48e9f4cb`).
Their samples are never merged: each file supplies only its matching policy
matrix. Its complete candidate retry passed 40 of 54 cells: all 18 physical cells
passed, while 14 buffered cells failed against the older protocol-v1 baseline.

Repeated focused runs of the worst buffered cell remained below its immutable
baseline (`0.781797`–`0.836196` throughput ratios). An identical clean pre-feature
binary measured on the current machine also fell to `0.799078` of that immutable
baseline, while the protocol-v3 candidate compared with this contemporaneous
pre-feature run at `0.916707` throughput and `1.065481` p95-latency ratios. This
isolates the failure to heterogeneous-core/scheduler placement drift between
capture windows rather than a production regression. A force-inline production
experiment did not help and was reverted.

Protocol v4 keeps policy-selected scheduling and unchanged thresholds, but
eliminates that uncontrolled placement variable. CPUs 12–19 were verified as
eight distinct cores (core IDs 6–13), each in the 3,800 MHz maximum-frequency
class, with no SMT siblings in the set. The complete 36-cell pre-feature buffered
baseline, 18-cell pre-feature physical reference, and 54-cell candidate are
recaptured sequentially in one user-approved quiet window, each through
`taskset -c 12-19`. Protocol-v1 through protocol-v3 files remain immutable
historical evidence and are not protocol-v4 comparators. Any affinity/topology
drift invalidates the whole protocol-v4 attempt; partial recapture is forbidden.

The complete protocol-v4 attempt passed 50 of 54 comparisons. All physical and
file-backed buffered cells passed, while four high-throughput eight-worker vector
cells failed. T263 then alternated those four comparator/candidate pairs inside
one pinned process. Every aggregate comparison passed unchanged thresholds:
throughput ratios were `0.959967`, `0.977558`, `0.904036`, and `0.958273`; p95
ratios were `1.122928`, `0.964537`, `0.824834`, and `1.096040`. Mixed order effects
and uneven CPU use inside the affinity set showed within-window scheduler/frequency
noise, not a stable candidate-only regression.

Protocol v5 therefore applies T263's same-process counterbalanced AB/BA pairing to
all 54 comparisons. One process captures 36 pre-feature buffered/candidate pairs
and 18 append-plus-barrier/physical-candidate pairs into one write-once file, for
1,188 measured rows. Policy-selected schedules, fixed affinity, aggregation, and
thresholds remain unchanged. Protocol v1–v4 and focused T263 evidence remain
immutable history or diagnosis and are not protocol-v5 comparators.

**Alternatives considered**: Comparing physical directly with buffered is
hardware-dependent and misleading. Absolute targets are machine-specific. A
report-only physical mode fails the constitution's performance gate.

## Decision 12: Deliver configuration, WAL, API, and publication vertically

**Decision**: Capture baselines first. Then RED–GREEN private policy/default;
first-execution-GREEN buffered compatibility before physical work; RED–GREEN
private memory rejection; single-record and independently tested set/map
multi-record direct barriers;
confirmed and indeterminate rollback; blocking publication order; private fallible
behavior; target/directory/existing-file/missing-staging preflights; fresh
publication; active- and recovery-authority publication; cleanup and crash models.
Only after the complete private three-family exposure gate is GREEN are public
policy, construction, and one-at-a-time mutation adapters promoted. CI and final
benchmarks follow.

**Rationale**: The constitution requires one behavior-focused runtime RED before
each new production behavior. Existing buffered behavior is characterized GREEN
rather than forced to fail. WAL and namespace failure transitions are authority
changes and cannot be safely implemented as one large happy-path batch. Delaying
public construction prevents any intermediate commit from advertising a partial
physical contract.

**Alternatives considered**: Implementing the full physical pipeline before its
fault matrix makes later RED evidence artificial. Adding placeholder public APIs
uses compilation or intentionally wrong behavior instead of a valid RED.

## Decision 13: Preserve the existing async compute cancellation boundary

**Decision**: Treat the user callback await in key/set `try_compute_async` as the
only cancellation point. Dropping the future while the callback is pending
releases the per-key guard, discards the private working copy, and performs no WAL
write, flush, barrier, accepted-state advance, or live publication. Once the
callback returns `Ready`, execute persistence and publication synchronously in the
same poll to the normal success or typed failure result.

**Rationale**: The current method holds the existing DashMap entry guard while
awaiting the callback and applies that callback to a private clone. Its post-await
WAL path contains no yield, so this contract preserves current source and runtime
behavior while preventing a cancelled future from orphaning a commit or barrier.
It also makes guard release and same-key progress deterministic without adding a
store-wide lock, background work, or shared completion state. Side effects the
user callback performs outside the database cannot be rolled back by the library.

**Alternatives considered**: Making persistence independently cancellable would
require async I/O plus a cancellation-safe commit state machine and a way to
deliver an outcome after the caller disappears. Continuing persistence in a
detached task would change acknowledgement and lifetime semantics. Retaining the
per-key guard after cancellation would block same-key progress and violate normal
future-drop ownership.
