# Verification

Baseline at b6efa8ae0d8e117af3da1f044bc3b6e77e955e80 (before production edits):
release test_speed_vec, 100,000 ordinary put operations, five runs in seconds:
0.067185060, 0.062176550, 0.058656342, 0.056834050, 0.060951605.
Median 0.060951605 seconds; fixed acceptance gate median <= 0.079237087 seconds (+30%).

Validation is incremental; pending items are not claimed complete.

Candidate release samples: 0.059145574, 0.057697024, 0.057894810,
0.061950214, 0.059740063 seconds; median 0.059145574 (within gate).

RED evidence: initial batch test returned Unsupported; publication race test observed
ordinary operations bypassing the batch; bounds test incorrectly returned Applied.
GREEN: focused tests and cargo test --all-targets passed after each implementation.
V1 and V2 truncation at every byte within a group recovers no partial batch.
Write/partial-write/flush/barrier failures roll back or fail closed; concurrency has
exactly one winner. Rotation and compaction preserve committed values.

Compatibility: a standalone harness used both the original git dependency at b6efa8a
and the modified local crate. Old write -> new batch -> old read/write -> new reopen
passed with all five keys retained. No WAL version or original reader change required.
