# Validation Quickstart: Boundary-Aware WAL Recovery

## Prerequisites

- Run from the repository root with the paired-run Rust toolchain.
- Keep `tests/fixtures/legacy/` immutable and record hashes before work.
- Capture baseline/source provenance before any release-format edit.
- Use isolated directories and one process per store directory.
- Treat every migration source as offline; tests inject changes only to verify exit 7.

## 1. Baseline Before Production Changes

Capture the full suite and issue #3-compatible benchmark with five warmups and at
least 11 measured samples per cell. Preserve raw CSV, environment/toolchain,
source hashes, build mode, OS/CPU/filesystem, and exact workload schema.

```bash
cargo test --all-targets --all-features -- --test-threads=1
cargo test --release --test mutation_ordering mutation_ordering_performance_report -- --ignored --nocapture --test-threads=1
```

Do not change baselines or thresholds after production edits begin.

## 2. Vertical Runtime RED–GREEN Tracers

Run one named behavior, confirm its expected runtime failure, implement only that
behavior, rerun it GREEN, then run the relevant accumulated target.

```bash
cargo test wal::truncation_tests::header_magic_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_version_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_length_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_kind_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_timestamp_unit_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_granularity_is_nonzero -- --exact --nocapture
cargo test wal::truncation_tests::header_base_bucket_round_trips -- --exact --nocapture
cargo test wal::truncation_tests::header_flags_are_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_reserved_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::header_crc_covers_prefix -- --exact --nocapture
cargo test wal::truncation_tests::partial_file_header_is_invalid -- --exact --nocapture
cargo test wal::truncation_tests::record_marker_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_version_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_action_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_header_length_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_length_complement_and_bounds_are_checked -- --exact --nocapture
cargo test wal::truncation_tests::record_physical_start_and_footer_match -- --exact --nocapture
cargo test wal::truncation_tests::record_mutation_start_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_index_count_are_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_timestamp_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_payload_is_strict -- --exact --nocapture
cargo test wal::truncation_tests::record_crc_covers_envelope -- --exact --nocapture
cargo test wal::truncation_tests::record_offset_overflow_is_explicit -- --exact --nocapture
```

A compilation failure is not RED. Never add an intentionally wrong public option
adapter or CLI binary. Exercise the internal option/migration runner first; expose
thin public/binary adapters only after underlying behavior is GREEN, and require
their first contract tests to pass.

Keep the private header/frame codecs disconnected from startup and steady-state
writes until every required invariant is GREEN.

## 3. Fresh-File Publication Checkpoints One at a Time

Use [contracts/fresh-v1-publication.md](contracts/fresh-v1-publication.md). Run each
named behavior RED, implement only that transition, and rerun GREEN:

```bash
cargo test wal::truncation_tests::fresh_staging_create_registration_is_atomic -- --exact
cargo test wal::truncation_tests::fresh_staging_cleanup_is_role_bounded -- --exact
cargo test wal::truncation_tests::fresh_header_each_write_cut_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_header_flush_failure_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_header_read_failure_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_header_validation_failure_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_header_sync_failure_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_append_handoff_failure_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_header_publish_failure_leaves_active_absent -- --exact
cargo test wal::truncation_tests::fresh_post_commit_interruption_is_valid -- --exact
cargo test wal::truncation_tests::fresh_prepared_handle_handoff_is_infallible -- --exact
cargo test wal::truncation_tests::fresh_store_uses_v1_header -- --exact --nocapture
```

Prove role-bounded cleanup before any later pre-publication failure depends on it.
Every such failure asserts active absence, no writable handle, no later checkpoint,
exact untouched artifacts, exact registered cleanup targets/results, and
deterministic next startup. After rename becomes GREEN, the post-commit interruption
regression must pass immediately with an exact valid 40-byte active header. The next
RED is current-initialization handoff of the same prepared handle, including a first
append without a post-commit filesystem checkpoint. Separately prove that every
existing 1–39-byte prefix and corrupt
40-byte header remains byte-identical invalid input and never enters fresh creation.

## 4. Repair Checkpoints One at a Time

Do not implement the entire successful publication pipeline before its failure
proofs. For each checkpoint, run its RED, add minimum handling, and rerun GREEN:

```bash
cargo test wal::truncation_tests::repair_create_failure_preserves_authority -- --exact
cargo test wal::truncation_tests::repair_write_failure_preserves_authority -- --exact
cargo test wal::truncation_tests::repair_flush_failure_preserves_authority -- --exact
cargo test wal::truncation_tests::repair_validation_failure_preserves_authority -- --exact
cargo test wal::truncation_tests::repair_sync_failure_preserves_authority -- --exact
cargo test wal::truncation_tests::repair_publish_failure_preserves_authority -- --exact
cargo test wal::truncation_tests::repair_reopen_failure_returns_no_store -- --exact
cargo test wal::truncation_tests::blocking_cleanup_failure_fails_closed -- --exact
cargo test wal::truncation_tests::post_publish_cleanup_may_defer -- --exact
```

Every case asserts no writable handle, no later pre-publication checkpoint, exact
untouched bytes, selected authority availability, and deterministic next startup.

## 5. Public Store and Format Matrices

```bash
cargo test migration_required_preserves_complete_legacy -- --exact --nocapture
cargo test wal::truncation_tests::single_action_each_cut_recovers_prefix -- --exact --nocapture
cargo test wal::truncation_tests::compute_group_each_cut_is_all_or_none -- --exact --nocapture
cargo test wal::truncation_tests::protected_field_position_matrix_rejects_corruption -- --exact --nocapture
cargo test wal::truncation_tests::clock_buckets_never_decrease -- --exact --nocapture
cargo test --test truncated_wal -- --test-threads=1
cargo test --test recovery -- --test-threads=1
cargo test --test compute_persistence -- --test-threads=1
cargo test --test mutation_ordering -- --test-threads=1
cargo test wal::format::tests -- --nocapture
```

For repaired V1 histories assert `Recovered`, exact accepted public state, pending
group absence, post-repair append, and three stable `Normal` reopens. Validate every
action-record cut and every protected field at first/middle/final record positions.
Partial/corrupt file headers, complete corruption, and truncated legacy remain
unchanged errors. Complete legacy returns `MigrationRequired` unchanged. Fresh-file
matrices cover all store families, missing versus zero-byte versus existing partial
headers, stale staging, default/non-default granularity, first append, and reopen.

## 6. Offline Migration CLI

The CLI contract is [contracts/migration-cli.md](contracts/migration-cli.md).

```bash
cargo test wal::truncation_tests::migration_pure_snapshot_conversion -- --exact
cargo test wal::truncation_tests::migration_initial_source_read_failure -- --exact
cargo test wal::truncation_tests::migration_source_preflight_rejection -- --exact
cargo test wal::truncation_tests::migration_existing_destination_rejected -- --exact
cargo test wal::truncation_tests::migration_destination_directory_registration_is_atomic -- --exact
cargo test wal::truncation_tests::migration_cleanup_success -- --exact
cargo test wal::truncation_tests::migration_cleanup_failure -- --exact
cargo test wal::truncation_tests::migration_output_registration_is_atomic -- --exact
cargo test wal::truncation_tests::migration_partial_write_failure -- --exact
cargo test wal::truncation_tests::migration_flush_failure -- --exact
cargo test wal::truncation_tests::migration_sync_failure -- --exact
cargo test wal::truncation_tests::migration_reopen_read_failure -- --exact
cargo test wal::truncation_tests::migration_output_validation_failure -- --exact
cargo test wal::truncation_tests::migration_final_source_reread_failure -- --exact
cargo test wal::truncation_tests::migration_source_changed -- --exact
cargo test wal::truncation_tests::migration_engine_complete_source_to_new_v1 -- --exact
cargo test --test migration_cli -- --test-threads=1
cargo run --bin pigment-db-migrate -- --help
cargo run --bin pigment-db-migrate -- \
  --source tests/fixtures/legacy-database \
  --destination /tmp/pigment-db-v1-example
```

Automated tests—not the example path—must prove all frozen families, source byte
identity, logical equality, output validation, append/reopen, args/output/exit
codes, existing destinations, hidden recovery artifacts, initial source-read and
final source-reread failures, source changes, every write/sync/validation failure,
and child interruption. Cleanup success and cleanup-removal failure must be GREEN
before output creation; each subsequent handled failure must assert
the exact removed and diagnostic path sets. Never run migration against a live
application directory.

For every failure tracer, RED must reach the named checkpoint; a generic earlier
failure is not valid evidence. Complete single-/multi-family success is enabled only
after all source/destination checkpoints are GREEN. Private parsing/output/exit
behavior is GREEN before adding the thin binary. Its first executable contract must
pass, and child-process interruption cases run only afterward.

## 7. Timestamp Configuration

Validate default one-minute buckets, explicit supported granularities, rejection
before I/O, persisted V1 configuration, explicit configuration change through
staged compaction, migration bucket zero, clock forward/equal/backward before and
after restart, rollback without bucket advance, equal-bucket WAL order, and
header-only empty compaction.

```bash
cargo test --test truncated_wal timestamp -- --nocapture
cargo test --test truncated_wal options -- --nocapture
```

## 8. Final Gates

On a quiet machine compare candidate results with the immutable baseline. Pause
for user confirmation before the final benchmark, as requested.

```bash
cargo test --release --test truncated_wal truncated_wal_startup_performance -- --ignored --nocapture --test-threads=1
cargo test --release --test truncated_wal truncated_wal_steady_state_performance -- --ignored --nocapture --test-threads=1
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --all-features --no-deps
cargo test --all-targets --all-features -- --test-threads=1
git diff --check
```

Every one-worker throughput ratio is at least 0.90, every eight-worker ratio at
least 0.85, every p95 ratio at most 1.25, and one-million-operation recovery median
at most 1.25 times complete startup. Report every cell and writes/second; no average
may hide a failure.

## Pre-Production Checkpoint (2026-08-06)

Completed before connecting any V1 header, record, startup, or steady-state write
behavior:

```text
cargo test --all-targets --all-features -- --test-threads=1
PASS: 147 passed, 0 failed, 18 ignored

cargo fmt --check
PASS
```

The full suite included the streaming replay provenance regression and all 18
public recovery integration tests. The only compiler warnings were the deliberately
registered, not-yet-consumed `cfg(test)` V1 publication/fault-checkpoint probes.

## V1 Write-Activation Checkpoint (2026-08-06)

Completed after family-specific fresh publication, single-action V1 writes,
set/map grouped compute writes, complete-valid V1 replay, and path-independent
prepared-handle handoff were connected:

```text
cargo test wal::truncation_tests -- --test-threads=1
PASS: 41 passed, 0 failed

cargo test --test compute_persistence -- --test-threads=1
PASS: 22 passed, 0 failed, 1 ignored

cargo test --test mutation_ordering -- --test-threads=1
PASS: 23 passed, 0 failed, 4 ignored

cargo test wal::ordering_tests -- --test-threads=1
PASS: 4 passed, 0 failed
```

The no-mixed-grammar regression covered all six action identifiers and a reopened
append for each store family. The V1 value/set/map success paths each emitted one
complete frame or contiguous logical group with exactly one flush. Existing
rollback/order tests remained GREEN; V1 fault matrices remain assigned to their
later checkpoint tasks.

## Phase 2 Foundational Checkpoint (2026-08-06)

Completed after explicit missing/zero-byte/legacy/V1 discrimination, immutable
legacy rejection, family-kind validation, and the frozen compatibility matrices:

```text
cargo test --quiet --all-targets --all-features -- --test-threads=1
PASS: 198 passed, 0 failed, 18 ignored

cargo fmt --check
PASS
```

This checkpoint includes 18 public recovery tests, the three frozen legacy
fixtures, every 1–39-byte V1 header prefix, corrupt-header and cross-family-kind
preservation, all fresh-publication checkpoints, complete-valid V1 replay, compute
and callback persistence, and deterministic mutation-ordering suites.

Immutable inputs verified at this checkpoint:

- 36-row steady-state baseline:
  `benchmarks/baseline.csv`, SHA-256
  `d756820b4e863de7ce45ef61e31e7d13d7d16d9119bfe89709c15dbb26650733`
- 11-row, 1,000,000-operation startup baseline:
  `benchmarks/startup-baseline.csv`, SHA-256
  `2e1ea73db8f2b11148a8a5f35c2be89f5b1891372f5382f8e3f3db91a0ee1db4`
- Frozen legacy fixture hashes remain those recorded in
  `tests/fixtures/legacy/README.md`.

## Phase 3 Corruption and Migration Checkpoint (2026-08-06)

Completed after strict repair publication, offline all-family migration, the
private runner, thin executable, process interruption evidence, and public
append/reopen compatibility were GREEN:

```text
cargo fmt --all
PASS

cargo test -- --test-threads=1
PASS: 263 passed, 0 failed, 18 ignored
```

The aggregate run included 27 migration-engine unit tests, 10 private CLI-runner
tests, and 9 executable tests. The executable checkpoint trace terminated with
exit 86 after `destination-created`, `partial-output-written`,
`complete-output-written`, `output-validated`, and `before-success-output`;
every case emitted no false success and preserved the source bytes. Exit/output
matrices covered usage 2, unavailable source 3, nonmigratable source 4, every
existing destination shape 5, destination creation/write class 6, and the
internal changed-source mapping 7. The all-family frozen fixture migrated,
accepted one append per family, and reopened three times with identical state.

Immutable fixture hashes reverified after the full run:

- `kv.wal.dat`: `e48dee8c4a07db010778d08037ac96a6cd16ca5fb323ea40145bd1fa36cb75f2`
- `set.wal.dat`: `d81d058ae3eabff04e08a8f12cad339e223f05f1fe532c82766b0565611cb653`
- `map.wal.dat`: `4612530c4b7b95ef8cb557c0306b2f11a5598a053b31dffec9f9aedb9477e84e`

## Phase 4 US1 Tail-Recovery Checkpoint (2026-08-06)

Completed after all six terminal action shapes, open logical groups, staged repair,
public state matrices, and deterministic grouped-write concurrency were GREEN:

```text
cargo fmt --all -- --check
PASS

cargo test wal::truncation_tests -- --test-threads=1
PASS: 66 passed, 0 failed

cargo test --test truncated_wal -- --test-threads=1
PASS: 21 passed, 0 failed, 4 ignored

cargo test --test recovery -- --test-threads=1
PASS: 18 passed, 0 failed

cargo test --test compute_persistence -- --test-threads=1
PASS: 22 passed, 0 failed, 1 ignored

cargo test --test mutation_ordering -- --test-threads=1
PASS: 23 passed, 0 failed, 4 ignored

cargo test key_set_store::mutation_ordering_tests::grouped_compute_preserves_shard_progress_contract -- --exact --test-threads=1
PASS: 1 passed, 0 failed

cargo test key_map_store::mutation_ordering_tests::grouped_compute_preserves_shard_progress_contract -- --exact --test-threads=1
PASS: 1 passed, 0 failed
```

The public matrices cut every byte of value put/delete, set append/remove/delete,
map put/remove/delete, and multi-member set/map compute groups. Every selected tail
returned `Recovered`, exposed only the accepted value/membership/ordered-map state,
published through validated staging without direct truncation, accepted a later
callback and append, and produced identical `Normal` state on three reopens.
Complete exact-boundary and zero-length delete records remained normal. Complete
group artifacts replayed all effects, while every partial group cut replayed none.
The grouped concurrency regressions retained same-shard waiting, different-shard
progress, shared mutation start/count/timestamp metadata, and one WAL flush.

## Phase 5 US3 Authority and Timestamp Checkpoint (2026-08-06)

Completed after proof-based complete/recoverable authority selection, staged tail
repair, timestamp restoration, rollback-safe acceptance, compaction metadata, and
additive options were GREEN:

```text
cargo test --quiet -- --test-threads=1
PASS (all targets before the subsequently fixed partial-header metadata edge):
lib 193 passed / 9 ignored; compute 22 / 1; migration CLI 9 / 0;
mutation ordering 23 / 4; recovery 18 / 0

cargo test --test truncated_wal -- --test-threads=1
PASS: 28 passed, 0 failed, 4 ignored

cargo fmt --all
PASS
```

The timestamp matrix covers the one-minute default, persisted non-default
granularity, forward/equal/backward clocks across restart, write and flush
rejection rollback, nonempty and header-only repair compaction, explicit staged
configuration change, all three file/vector option adapters, grouped mutation
timestamps, and three stable reopens per store family. Partial 1–39-byte headers
remain structured invalid artifacts without panics or byte changes. Authority
selection accepts a recoverable active tail only when an accepted logical prefix
proves its relationship to the complete candidate; incomparable candidates remain
unchanged with `AuthorityUndetermined`.

## Phase 6 Compatibility Gate (2026-08-06)

```text
cargo test --test recovery -- --test-threads=1
PASS: 18 passed, 0 failed

cargo test --test compute_persistence -- --test-threads=1
PASS: 22 passed, 0 failed, 1 ignored

cargo test --test mutation_ordering -- --test-threads=1
PASS: 23 passed, 0 failed, 4 ignored

cargo test --test truncated_wal -- --test-threads=1
PASS: 28 passed, 0 failed, 4 ignored
```

The frozen legacy fixture hashes remained unchanged:

- `kv.wal.dat`: `e48dee8c4a07db010778d08037ac96a6cd16ca5fb323ea40145bd1fa36cb75f2`
- `set.wal.dat`: `d81d058ae3eabff04e08a8f12cad339e223f05f1fe532c82766b0565611cb653`
- `map.wal.dat`: `4612530c4b7b95ef8cb557c0306b2f11a5598a053b31dffec9f9aedb9477e84e`

This gate reverified issue #1 recovery/authority behavior, issue #2 compute
persistence, issue #3 deterministic ordering and shard progress, public V1 tail
recovery, legacy migration boundaries, and the additive timestamp option surface.

## Million-Operation Startup Gate (2026-08-06)

The release-mode paired gate ran 11 complete and 11 terminally torn startup
samples with 1,000,000 accepted operations per history. Raw rows are preserved in
`benchmarks/startup-final.csv`.

```text
complete median: 3,836,356,572 ns
torn median:     2,858,484,996 ns
torn/complete:   0.7451
threshold:       <= 1.2500
result:          PASS
```

The optimized repair path still uses validated same-directory staging and rename;
it copies the already strictly replayed accepted prefix, updates only the
CRC-protected base-bucket header, validates exact staged bytes, and never directly
truncates the active artifact.

## Final Performance and Quality Gate (2026-08-06)

After explicit quiet-machine confirmation, the release matrix completed all 36
cells. One initial p95 failure in `key_map/vector/ordinary_write/8` was preserved
as `benchmarks/candidate-failed-pre-optimization.csv`. Its focused threshold test
reproduced RED at `31,885 ns`; serializing/checksumming the map payload before WAL
acceptance made the focused test GREEN at `17,412 ns`. The final full-matrix cell
was `264,192.031` writes/s and `19,149 ns` p95.

```text
steady-state cells:          36/36 PASS
minimum 1-worker throughput: 0.950083 (required >=0.90)
minimum 8-worker throughput: 0.974522 (required >=0.85)
maximum p95 latency ratio:   1.179561 (required <=1.25)
startup torn/complete ratio: 0.7451   (required <=1.25)
retained-memory validation:  PASS, 0 live keys
```

Raw results, every paired ratio, writes/second, p95 latency, startup evidence,
commands, and provenance are recorded in `benchmarks/final.csv` and
`benchmarks/final.md`.

```text
cargo fmt --check
PASS

cargo clippy --all-targets --all-features -- -D warnings
PASS

cargo doc --all-features --no-deps
PASS

cargo test --all-targets --all-features -- --test-threads=1
PASS: 294 passed, 0 failed, 20 ignored

git diff --check
PASS
```

The final implementation/artifact audit confirmed:

- startup reports `MigrationRequired` for complete legacy input; only the
  explicit offline `pigment-db-migrate` binary performs conversion;
- per-key DashMap guards remain the mutation-ordering mechanism, with no global
  mutation lock added; the existing shared WAL acceptance lock remains only the
  append-order boundary;
- V1 uses the existing `crc32fast` dependency and adds no runtime dependency;
- recovery/repair publishes validated same-directory staging with rename and
  contains no direct `set_len`/`truncate` of the selected source (the production
  `set_len` is limited to rollback of a rejected in-progress append);
- fresh creation exclusively creates, writes, rereads, validates, synchronizes,
  and positions staging before rename, so active never exposes 1–39 header bytes;
- executable FR-001–FR-037 and SC-001–SC-018 traceability remains complete, and
  the contracts, data model, research, plan, tasks, and specification use the
  same authority, cleanup, migration, timestamp, and performance boundaries.
