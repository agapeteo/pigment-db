# Research: V2 WAL Segments

## Decision 1: Use a versioned V2 grammar with `u64` boundaries

**Decision**: Store payload length, its complement, physical start, mutation start, and segment base as checked unsigned 64-bit values.

**Rationale**: This directly removes the 4 GiB wrap while preserving fixed-width, independently validated record boundaries.

**Alternatives considered**:

- Keep V1 and rotate below 4 GiB: rejected because one oversized record and cumulative recovery metadata still require explicit overflow behavior.
- Variable-length integers: rejected because they complicate partial-header classification and provide no practical advantage for a fixed binary WAL.
- Platform-sized offsets: rejected because persisted bytes must be portable and deterministic.

## Decision 2: Keep CRC32

**Decision**: Retain CRC32 for header and record accidental-corruption detection.

**Rationale**: The issue changes width and lifecycle, not the integrity threat model. CRC32 is already deployed, fast, and dependency-free within this crate.

**Alternatives considered**:

- BLAKE3: faster than many cryptographic hashes but adds a dependency and changes a non-security checksum contract without a governing requirement.
- Cryptographic authentication: rejected because no key-management or adversarial-tamper requirement exists.

## Decision 3: Rotate immutable numbered segments

**Decision**: Keep the canonical active filename and seal completed segments as `NAME.segment-NNNNNNNNNNNNNNNNNNNN`. Create the next active header in `.NAME.next` and rotate only between complete logical mutations.

**Rationale**: This bounds the active file without synchronous full-state snapshots or a global pause, preserves append order, and makes crash artifacts classifiable.

**Alternatives considered**:

- Synchronous snapshot compaction during write: rejected due to latency and global coordination.
- One global mutex around public mutations: rejected by the concurrency contract and performance requirement.
- Split large mutations across segments: rejected because it creates cross-file atomic group recovery.

## Decision 4: Oversized first mutation remains intact

**Decision**: An empty segment accepts one mutation even if its encoded size exceeds the target; the next mutation rotates first.

**Rationale**: A target is an operational bound, not a reason to reject otherwise representable user data or split atomic state.

**Alternatives considered**:

- Reject the mutation: rejected because target configuration should not reduce the value-size contract.
- Pre-create a dedicated blob segment: rejected as an unnecessary second storage model.

## Decision 5: V1 startup requires offline conversion

**Decision**: Public startup recognizes complete or recoverable V1 and returns `MigrationRequired`. The CLI accepts legacy, V1, or V2 and always writes a new V2 destination.

**Rationale**: Offline conversion preserves the source, avoids ambiguous in-place authority, and provides the same mechanism for explicit V2 compaction.

**Alternatives considered**:

- Automatic startup conversion: rejected because startup crashes would need another cross-version authority state machine and could surprise operators with heavy work.
- In-place rewrite: rejected because it cannot preserve a simple rollback source.

## Decision 6: Persist timestamp state per segment

**Decision**: Default to one-minute granularity, persist granularity and base bucket in every header, and apply an explicit change by rotating before the next mutation. Unrelated options do not request a timestamp change.

**Rationale**: Immutable segments cannot be rewritten safely on open. Per-segment state preserves monotonic accepted timestamps and prepares for a future time-based startup feature.

**Alternatives considered**:

- Rewrite the active header on open: rejected because it mutates authority before a user operation and conflicts with immutable history.
- One global sidecar configuration file: rejected because it introduces another crash-consistency authority.

## Decision 7: Retain sealed segments until explicit compaction

**Decision**: Runtime rotation never deletes sealed segments. The offline CLI compacts a validated chain into one V2 segment.

**Rationale**: Deletion requires proving a replacement durable snapshot and is a distinct lifecycle operation. Explicit offline compaction keeps the write path fast and authority simple.

**Alternatives considered**:

- Automatic background deletion: rejected because no background lifecycle, retention, or cancellation contract exists.
- Delete on reopen after replay: rejected because replay alone does not publish an equivalent replacement artifact.

## Decision 8: Diagnose the failed concurrent pair floor before remediation

**Decision**: Preserve Protocol V1 as a failed SC-005 attempt and run a separate focused diagnostic over the three eight-worker cells. Increase each variant from 16,384 to 65,536 operations per worker, retain counterbalanced same-process pairs and CPU affinity, and record Linux CPU/I/O pressure totals around every sample.

**Rationale**: All candidate median ratios passed, including parity or improvements at eight workers, while five isolated concurrent pairs failed. Longer samples and interference counters distinguish a repeatable candidate lower tail from scheduler or I/O pressure without weakening or rerunning away the failure.

**Alternatives considered**:

- Weaken or remove the per-pair floor: rejected because the approved constitution requires fixing a failed threshold rather than weakening it after measurement.
- Immediately rerun the identical acceptance matrix: rejected because a pass would not explain the valid failed attempt and would encourage result selection.
- Optimize the write path before diagnosis: rejected because the medians do not identify a sustained regression and an evidence-free change could reduce correctness or performance elsewhere.

## Decision 9: Classify CPU service with per-worker proc accounting

**Decision**: Retain Diagnostic V2's longer matrix and collect each worker's user-plus-system CPU ticks and voluntary/involuntary context-switch deltas from `/proc/thread-self/stat` and `/proc/thread-self/status`. Aggregate the eight workers per sample and signal operation completion before reading the ending counters.

**Rationale**: Diagnostic V2 showed a strong relationship between wall throughput and global CPU pressure, but PSI includes both the benchmark and other machine work. Per-worker CPU ticks distinguish extra candidate computation from lost wall-clock service, while context switches identify blocking and scheduling changes. Linux proc files require no unsafe code, elevated profiling capability, or new dependency.

**Alternatives considered**:

- Infer causality from global PSI alone: rejected because the pressure may be endogenous to the benchmark.
- Use `perf_event_open` from the runner: rejected because it requires unsafe platform calls and may be blocked by host profiling policy.
- Read only `/proc/self/stat`: rejected because process-level totals mix the coordinator and worker activity and cannot attribute scheduling across the measured workers.

## Decision 10: Pin coordinator and workers to distinct physical cores

**Decision**: Launch the acceptance runner with CPUs 11-19 available, pin the coordinator to CPU 11, and pin worker `n` to CPU `12 + n` before each sample. Verify every effective affinity through `/proc/thread-self/status` before releasing the timed barrier.

**Rationale**: Diagnostic V3 found nearly equal implementation CPU work overall and showed that whichever side received inflated CPU ticks and involuntary switches became the slow outlier. Fixed one-thread-per-core placement removes worker migration and coordinator competition while preserving identical baseline/candidate workloads, order, and thresholds. The host `taskset` command operates outside timed intervals and avoids unsafe code or new dependencies.

**Alternatives considered**:

- Modify production code: rejected because CPU work did not show a consistent candidate regression.
- Ignore or filter individual captured rows: rejected because it would weaken the approved every-pair gate after measurement.
- Change scheduler policy or move unrelated machine processes: rejected because elevated privileges and machine-wide side effects are unnecessary and unsafe.
- Use process affinity without thread placement: rejected because the valid failed captures demonstrate excessive within-mask scheduling variance.

## Decision 11: Reduce the V2 append critical section and allocation cost

**Decision**: Prepare format-independent action payloads before WAL-state acquisition; acquire WAL state once; apply footer offsets and payload-only CRC only for legacy output; and reuse a WAL-owned buffer for sequential V2 envelope encoding. Continue to use the V1/V2 full-envelope CRC as their authoritative integrity field.

**Rationale**: Fixed-affinity Protocol V2 still reproduced candidate-specific CPU inflation in key/value and key/set. A deterministic progress test proved action serialization held the exclusive WAL boundary. Protocols V3 and V4 narrowed locking but exposed steady allocation and redundant-checksum overhead. The final design preserves one ordered write critical section, performs no unsafe buffer initialization, retains byte-exact legacy output, and passes Protocol V5 with every pair at or above `0.896`.

**Alternatives considered**:

- Add a global mutation mutex: rejected because it violates the bounded-concurrency requirement and would serialize work outside the existing WAL authority boundary.
- Weaken or statistically filter the pair floor: rejected because Principle IV requires fixing failed thresholds in the implementation.
- Use unsafe uninitialized frame storage: rejected because sequential `Vec::extend_from_slice` encoding removes the zero-fill pass safely.
- Change the persisted V2 grammar or checksum algorithm: rejected because the existing grammar is correct and the failure was runtime overhead, not format integrity.
