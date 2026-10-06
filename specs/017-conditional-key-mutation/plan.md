# Implementation and admission plan

## Constitution VI first

Opaque one-key expected bytes plus Keep/Put/Delete: no record decoding, consumer
prefix, shared callback, workflow, retry, age, capacity default, dependency or
new lock registry. The existing maintenance/transaction/entry/WAL order stays.
Motivation: Penpack's uniform User read-modify-blind-write defect; this is a
platform defect fix, not a product-specific receipt sequence. Existing native
credential-record compare/delete can use the same primitive unchanged.
Consumer-shaped fixture bytes stay opaque. Production types and requirements
use storage vocabulary. The byte-ABA scope test's identifier uses storage
history vocabulary; it promises no consumer lifecycle invariant.

The prepared patch is based on 96415c4 source, and applies to clean published
main b58c1eb (the intervening changes only update verification documents).
A future reviewed dependency main commit must exist and be published before
any consumer rev pin. No proposed SHA can be treated as that commit.
The larger downstream receipt/lifecycle changes remain separately governed;
this prerequisite and its path override do not admit them.

## Authority, compatibility and failure

Keep the existing entry guard from expected-byte comparison to publication,
maintenance shared for the same interval, and the existing transaction read
lease excluding batches. Reuse ordinary WAL event acceptance for changed
operations in Legacy/V1/V2. Keep the WAL health read explicit for every matching
call and preserve existing failure carriers. Return no persistence authority
on Conflict/Unchanged. No old method or persisted format changes.

RED/GREEN provenance comes from the pre-implementation prerequisite contract
and archived vertical candidate cycles, followed by actual native RED/GREEN.
The new formal spec packages that reviewed contract for prospective upstream
admission; it does not claim it preceded the earlier isolated prototype.

Run focused memory conformance, full ordinary dependency tests, real temporary
file replay/rotation/online compaction and process-crash checks, fmt and clippy.
Record source and log hashes. Deliberate poison-panic output is caught by its
scope test and is not a suite failure. Negative controls must use separate
build directories: a reused negative-control artifact already caused a false
gate failure and was corrected by cleaning and rebuilding the package.

## Limits and performance gate

Matching calls add a synchronized Ready read; changed operations retain the
original WAL check and Physical barrier. Existing co-shard, global batch and
shared WAL waiting remains. No throughput or latency threshold is invented.
Target configured/unconfigured measurements and owner-agreed latency bounds
remain required before downstream rollout. A clean file reopen or process kill
is not hardware power-loss proof, and no test changes historical uncertainty.
