# V2 WAL Performance Gate

This directory defines the matched quiet-machine acceptance gate for Issue #9.

## Comparator and candidate

- Baseline: clean source archive of `f5bf40e9861b544f867d5aa940fa52eb940b5e54`.
- Candidate: the complete `codex/009-v2-wal-segments` worktree identified by a source-tree checksum recorded at capture time.
- Both versions are linked into one release process so process order, affinity, filesystem, toolchain, and workload are matched.

## Matrix

- Store families: key/value, key/set, key/sorted-map.
- Workers: 1 and 8.
- Workload: unique 32-byte buffered file-backed writes.
- Default 1 GiB target; each sample stays well below the rotation boundary, isolating V2 framing and per-mutation rotation checks.
- Five warmup AB/BA pairs and eleven measured counterbalanced AB/BA pairs per cell.
- `16,384` operations per worker per variant.
- Affinity: `taskset -c 12-19`, verified at runner startup; these are distinct physical core IDs on the capture machine.

## Acceptance

For each of the six cells independently:

1. Median candidate writes/second divided by median baseline writes/second is at least `0.90`.
2. Every same-pair candidate/baseline writes/second ratio is at least `0.85`.
3. No operation or runner validation fails.

The complete attempt is invalid if affinity, linked source identity, output uniqueness, matrix completeness, pair ordering, or machine quietness is not proven.

## Failed-gate diagnostic

Protocol V1 is retained as a valid failed attempt because five eight-worker pairs fell below the `0.85` floor. Diagnostic V2 does not replace or override that result. It narrows the matrix to the three eight-worker cells, uses three warmup and eleven measured AB/BA pairs, and increases the work to `65,536` operations per worker per variant. Each sample also records global CPU `some`, I/O `some`, and I/O `full` pressure-stall deltas plus the one-minute load average before and after.

The diagnostic determines whether a candidate-specific lower tail reproduces over longer samples or aligns with system interference. It cannot satisfy SC-005; a complete fresh six-cell capture is required after the resulting remediation decision.

Diagnostic V2 reproduced the lower tail but showed that global CPU pressure strongly predicts which side of a pair is slow. Diagnostic V3 retains that protocol and adds aggregate worker CPU ticks and voluntary/involuntary context switches. Similar CPU ticks per operation with worse wall time indicates lost CPU service; materially higher candidate CPU ticks indicates implementation work suitable for profiling and optimization.

## Protocol V2 fixed-affinity acceptance

Diagnostic V3 classified the slow tail as scheduling variance rather than consistent candidate CPU work. Protocol V2 prospectively remediates only the environment:

- coordinator: CPU 11;
- worker `n`: CPU `12 + n`, giving CPU 12 for one worker and CPUs 12-19 for eight workers;
- every effective affinity verified before timing;
- task placement completed outside measured intervals;
- original six cells, five warmup pairs, eleven measured pairs, operations, ordering, and acceptance thresholds unchanged.

Protocol V2 is a new acceptance attempt. It does not erase or reinterpret Protocol V1 or either diagnostic. Its structurally valid capture failed SC-005: every median passed, but one key/value and two key/set eight-worker pairs missed the `0.85` floor. Those samples used materially more candidate worker CPU, while key/map passed comfortably. The result report recommends moving V2 action construction out of the exclusive WAL-state critical section under RED-GREEN coverage before preparing another fresh capture.

## Protocol V3 optimized acceptance

Protocol V3 retains Protocol V2 unchanged except for the source-identified candidate. V2 action construction now occurs after a released health/format read and before exclusive WAL-state acquisition; health is checked again under the write guard. A deterministic progress test failed before the change and passed after it. The complete suite and static gates passed before the new release binary was built.

Protocol V3 failed SC-005 despite passing key/value and key/map medians. The key/set one-worker median was `0.898`, and four concurrent pairs missed the floor. Its report identifies the newly added read-then-write lock sequence as remaining per-mutation overhead and recommends a GREEN refactor to prepare format-independent actions before any guard, with legacy footer offset applied only after the single write guard is acquired.

Protocol V4 validated that direction: key/set one-worker passed at `0.903` and its eight-worker median improved to `0.981`. Seven pairs still missed the floor. The next candidate reuses a WAL-owned frame buffer, avoids redundant zero initialization, and computes the payload-only CRC only for legacy output while retaining the authoritative V1/V2 envelope CRC.

Protocol V5 passes SC-005. Every median and all 66 pair ratios clear the unchanged thresholds; the minimum pair ratio is `0.896`. This is the accepted Issue #9 performance evidence.

## Artifacts

- `runner/main.rs`: linked release runner source.
- `runner/diagnostic_v2.rs`: focused longer-sample runner with system-pressure evidence.
- `runner/diagnostic_v3.rs`: longer-sample runner with worker CPU and scheduling evidence.
- `runner/protocol_v2.rs`: full fixed-affinity SC-005 acceptance runner.
- `runner/protocol_v3.rs`: unchanged fixed-affinity runner used for the optimized candidate retry.
- `runner/protocol_v4.rs` and `runner/protocol_v5.rs`: unchanged runners for the single-lock and final optimized candidates.
- `attempts/protocol-v1-20260807-224744.md`: provenance, exact command, and capture outcome.
- `attempts/diagnostic-v2-20260807-234009.md`: prepared diagnostic provenance and exact command.
- `attempts/diagnostic-v3-20260808-001649.md`: prepared worker-accounting diagnostic provenance and exact command.
- `attempts/protocol-v2-20260808-004543.md`: fixed-affinity acceptance provenance, exact command, and outcome.
- `attempts/protocol-v3-20260808-122242.md`: optimized-candidate acceptance provenance and exact command.
- `attempts/protocol-v4-20260808-123135.md`: single-lock candidate provenance and failed outcome.
- `attempts/protocol-v5-20260808-124401.md`: final optimized candidate provenance and passing outcome.
- `results/protocol-v1-20260807-224744.csv`: immutable raw capture.
- `results/protocol-v1-20260807-224744.md`: evaluated acceptance report and recommendation.
- `results/diagnostic-v2-20260807-234009.csv`: immutable longer-sample diagnostic capture.
- `results/diagnostic-v2-20260807-234009.md`: pressure correlation, classification limit, and recommendation.
- `results/diagnostic-v3-20260808-001649.csv`: immutable worker-accounting diagnostic capture.
- `results/diagnostic-v3-20260808-001649.md`: CPU/scheduling classification and environment-remediation recommendation.
- `results/protocol-v2-20260808-004543.csv`: immutable fixed-affinity acceptance capture.
- `results/protocol-v2-20260808-004543.md`: failed SC-005 decision, CPU evidence, and focused implementation recommendation.
- `results/protocol-v3-20260808-122242.csv`: immutable optimized-candidate fixed-affinity capture.
- `results/protocol-v3-20260808-122242.md`: failed SC-005 decision and single-lock remediation.
- `results/protocol-v4-20260808-123135.csv`: immutable single-lock fixed-affinity capture.
- `results/protocol-v4-20260808-123135.md`: failed decision and frame-encoding remediation.
- `results/protocol-v5-20260808-124401.csv`: immutable passing fixed-affinity capture.
- `results/protocol-v5-20260808-124401.md`: final SC-005 PASS decision.
