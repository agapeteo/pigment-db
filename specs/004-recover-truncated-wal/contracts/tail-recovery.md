# Contract: Accepted V1 Tail and Repair

## Classification

| Selected bytes | Outcome |
|---|---|
| No active/recovery candidate | Follow [fresh-v1-publication.md](fresh-v1-publication.md); do not enter tail repair |
| Zero-byte or complete legacy format | `MigrationRequired`; preserve all artifacts |
| Truncated/corrupt legacy format | `InvalidArtifact`; preserve all artifacts |
| Complete V1 header/groups | Complete V1 candidate |
| V1 partial next action record after accepted group | Recoverable tail |
| V1 EOF with complete nonfinal group members | Recoverable tail at group start |
| V1 complete corrupt field/frame/group | Corrupt; preserve bytes |
| Partial/corrupt V1 file header | Invalid; preserve bytes |

Only accepted V1 groups participate in logical snapshot/prefix comparison. When
active and recovery candidates coexist, issue #1 equality/reached-prefix/
compacted-prefix proof selects authority before repair. Staging is disposable only
after its role is proven. Any potentially authoritative legacy-format candidate
blocks normal repair/cleanup. V1 ambiguity returns `AuthorityUndetermined`.
An existing partial/corrupt header never falls back to fresh creation.

## Repair guarantees

1. Encode the selected accepted logical state as a complete V1 snapshot group, or
   header-only empty state, preserving last bucket/configuration.
2. Exclusively create `.next`; write and flush it; replay-validate exact logical
   state/configuration; run startup synchronization; close it; publish by rename;
   and reopen the exact named length.
3. Return writable state only after the published bytes reopen as complete V1.
4. Return `Recovered`; later stable reopen returns `Normal`.
5. Never directly truncate the selected source or preserve physical record identity
   at the expense of safe staged publication.

## Checkpoint-specific failure proofs

Each checkpoint receives its own runtime RED immediately before its minimum GREEN:

| Checkpoint | Required result |
|---|---|
| exclusive staging create | Failure leaves selected authority and other artifacts unchanged |
| partial staging write | No later call; no writable handle; source authority retained |
| staging flush | Same pre-publication fail-closed result |
| staged-byte validation | Invalid replacement never synchronizes or publishes |
| staging synchronization | Unsynchronized replacement never publishes |
| publish/rename | Selected source remains available; no writable result |
| exact-length reopen | Startup errors; no writable result escapes |
| blocking cleanup before safe publication | Fail closed when staging/authority cannot be proven exclusive |
| obsolete cleanup after published authority | May defer cleanup and return `Recovered` only when the new active is already validated and authoritative |

Every test asserts no later checkpoint invocation, byte identity for untouched
artifacts, and a deterministic next-start outcome. Implementing the successful
pipeline does not permit failure behavior to precede these RED proofs.

## Required evidence

- Every byte cut in the action-record header, payload, and footer of all six shapes.
- Every compute member boundary and every byte within each member.
- Every protected field corrupted independently at first, middle, and final record.
- Partial/corrupt 40-byte V1 file headers rejected unchanged.
- Empty accepted prefix, post-repair append, and three reopenings.
- Active/recovery/staging V1 authority combinations and both cleanup classes.
- Rollback-failed full and partial groups.
- Forward/equal/backward clocks before and after restart.
- Public value, membership, ordered-map, key-existence, outcome, and absence checks.
