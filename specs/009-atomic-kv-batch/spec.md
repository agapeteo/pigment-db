# Atomic conditional KV batches
Status: approved for implementation, 2026-09-03.

## User scenarios and requirements
- P1: Conditional replacement/deletion of 1–16 distinct entries compares expected opaque bytes or absence. Empty is distinct from absent. A mismatch changes nothing, including the WAL.
- P1: Commit every replacement and deletion as one WAL group before publishing. Coordinate ordinary KV writers and readers so no reader observes partial publication. A restart reconstructs all or none. Respect buffered/physical durability without silently changing policy.
- P1: Host capability requires an explicit grant; every raw key is checked against reserved keys and prefixed with the server-resolved canonical tenant. Input <=8 MiB, raw keys <=4 KiB, <=16 entries; bounded decoder rejects malformed lengths, duplicates and trailing data before allocation/mutation.
- P2: Generations within/across books append their completed editions with exact cumulative accounting and a durable receipt. Rebase on unrelated changes; save retries never rerun TTS. Do not resurrect deleted books/sections or overwrite new drafts.
- Preserve old signatures, record values and HTTP APIs. Reuse V1/V2 group framing. New batches refuse unsupported legacy framing; old APIs remain supported.
- One process owns a store directory. No locks span guest callbacks, TTS, HTTP or Docker. No changes to Object Store or gateway APIs.

## Acceptance
Barrier-controlled concurrency, ordinary writer interference, WAL failure, reopen/truncation, compaction, guest denial and payload tests must pass. Release ordinary-KV workload median overhead <=30% versus baseline measured before functional edits. Full suites/builds and bounded live overlap within and across books must pass. Unit tests use memory stores; restart integration uses isolated temporary files.
