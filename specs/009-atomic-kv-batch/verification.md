# Verification

Baseline at b6efa8ae0d8e117af3da1f044bc3b6e77e955e80 (before production edits):
release test_speed_vec, 100,000 ordinary put operations, five runs in seconds:
0.067185060, 0.062176550, 0.058656342, 0.056834050, 0.060951605.
Median 0.060951605 seconds; fixed acceptance gate median <= 0.079237087 seconds (+30%).

The database evidence below was recorded for implementation commit `550a55f`.
Downstream completion evidence, recorded after that commit, follows below.

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

## Cross-project completion

T004–T006 completed on 2026-09-03 after Pigment commit
`550a55f5d7499e308fbae03b80db2e31c366804e`. Penpack and WA recorded completion,
but this duplicated checklist was not updated at that time. This documentation
correction reconciles those records; it does not change code or the dependency pin.

- **T004 — host interface and SDK:** Penpack's explicitly granted, tenant-scoped
  conditional batch import and bounded decoder are implemented. Verification
  covers permissions, reserved keys, malformed frames and payload boundaries;
  all three new SDK contract tests pass. See the
  [Penpack verification](../../../penpack/specs/017-atomic-kv-batch/verification.md)
  and [guest-host ABI](../../../penpack/specs/017-atomic-kv-batch/contracts/guest-host-abi.md).
- **T005 — concurrent WA/UI persistence:** The regression moved from
  `[409, 201, 201]` to three successful `201` responses. Tests cover atomic
  edition/usage/receipt persistence, newer drafts, deletion, uncertain outcomes,
  response ordering and navigation. The WA suite passed 76 tests and the UI
  suite passed 136 tests, plus the intercepted-browser integration. See the
  [cross-project verification report](../../../penpack-wa/wa-panta-studio/openspec/changes/archive/2026-09-04-support-concurrent-generation/verification.md).
- **T006 — verification and deployment:** Pigment's full all-target suite passed
  562 tests (27 ignored); Penpack's full suite against the exact pinned commit
  passed 1671 tests (78 ignored). The recorded builds, formatting and strict
  Pigment/WA clippy checks passed. After the user restarted Penpack, the WA module
  was deployed with the new capability while preserving existing settings.
  The live test at 22:27:40 UTC generated three overlapping editions across two
  temporary books: all returned `201`, playback and byte ranges worked, and
  usage increased exactly 17 characters (10 / 7 per book). Receipt replay returned
  `200` without additional usage. Temporary books and media were removed.
  The linked cross-project report contains the deployment hash and test details.

Verification scope: the live test used Panta's deployed module; the separate
Penpack test-harness and wa-site managed E2E suites were not run. Existing ignored
tests were not counted as passes. This documentation correction only validates
checklist consistency, evidence links and whitespace; it does not claim new
code-test runs or additional billed requests. Commit `550a55f` remains the pinned
implementation revision; no commit amendment or push is part of this correction.

## Finalization checks — 2026-09-04

Re-ran `cargo test --locked --all-targets`: **562 passed, 27 ignored, 0 failed**.
`cargo clippy --locked --all-targets -- -D warnings` and `git diff --check`
passed. These are new verification runs, distinct from the original evidence
above. No library code or dependency pin changed, and no billed request was made.
The completed downstream checklist and corrected archived-WA evidence link are
being committed separately from implementation revision `550a55f`.
