# Atomic conditional KV batches
Status: approved for implementation, 2026-09-03.

**Motivation (provenance):** penpack's atomic guest KV batch import (penpack `specs/017-atomic-kv-batch`), used by Panta Studio to commit generated content, its account usage counter and an idempotence receipt together across concurrent generations. The consumer-side grant, tenant prefixing, byte ceilings, wire decoder and live overlap test are penpack's and the application's, and are recorded as downstream evidence in [verification.md](verification.md#cross-project-completion).

## User scenarios and requirements
- P1: Conditional replacement/deletion of 1–16 distinct entries compares expected opaque bytes or absence. Empty is distinct from absent. A mismatch changes nothing, including the WAL.
- P1: Commit every replacement and deletion as one WAL group before publishing. Coordinate ordinary KV writers and readers so no reader observes partial publication. A restart reconstructs all or none. Respect buffered/physical durability without silently changing policy.
- P1: A batch with no entries, more than 16 entries, or a repeated key is refused as invalid input before any lock is taken or anything is written. Keys and values are opaque bytes. Key namespacing, access control, byte ceilings on keys and values, and any wire encoding belong to the caller and are applied before a batch reaches the library.
- P2: Read-modify-write cycles over overlapping keys serialize: of competing batches built from the same observed state, exactly one applies and each other one returns Conflict with nothing written, so a counter key changed in the same batch as the data it accounts for stays exact. A batch whose expectations still hold after unrelated keys changed applies unaffected. An expectation of absence on a marker key the batch writes makes a retried batch idempotent, so a caller can retry without repeating work it did before submitting the batch. An expectation of absence prevents recreating a deleted key, and an expectation of an exact prior value prevents overwriting a newer one.
- Preserve existing public signatures and the encoding of existing records. Reuse V1/V2 group framing. New batches refuse unsupported legacy framing; old APIs remain supported.
- One process owns a store directory. A batch takes no caller callback, and the batch gate is taken and released within the one call; no lock outlives the call that takes it. No existing key/set or key/sorted-map API changes.

## Acceptance
Barrier-controlled concurrency, ordinary writer interference, WAL failure, reopen/truncation, compaction and invalid-batch tests must pass. Release ordinary-KV workload median overhead <=30% versus baseline measured before functional edits. Full suites and builds must pass. Unit tests use memory stores; restart integration uses isolated temporary files.
