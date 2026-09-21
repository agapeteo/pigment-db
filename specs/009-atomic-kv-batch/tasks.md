# Atomic KV tasks

- [x] T001 Contracts and performance baseline.
- [x] T002 RED/GREEN batch semantics and public coordination; refactor.
- [x] T003 RED/GREEN failures, recovery, compaction, compatibility; performance gate.
- [x] T004 Downstream evidence: the consumer's bounded batch interface and SDK.
- [x] T005 Downstream evidence: concurrent persistence in an application using the batch.
- [x] T006 Full verification; downstream deployment and a bounded live test of overlapping batches recorded as evidence.

T004–T006 track downstream integration, not additional Pigment library changes.
They completed after the Pigment implementation commit. See the
[cross-project completion evidence](verification.md#cross-project-completion)
for the host/SDK, WA/UI and live-test results and their verification scope.
