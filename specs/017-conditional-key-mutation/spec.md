# Single-key conditional mutation

Status: isolated proposal; not released or pinned by a downstream repository.
Baseline: PigmentDB 96415c4a31519be4d6ce67815eab90301791ad07.

A caller with an exact byte snapshot needs to replace or delete one value only
if that snapshot still matches, including on Legacy stores where grouped batch
framing is unavailable. The API accepts opaque keys and values without decoding their contents. Existing unconditional methods retain their contracts.

## Public contract

`DurableKeyValueStore::try_compare_exchange_one(key, expected, action)` returns
`io::Result<ConditionalResult>`. `expected: Option<&[u8]>` distinguishes absence
from an existing empty value. `ConditionalAction` is `Keep`, `Put(Vec<u8>)`, or
`Delete`; `ConditionalResult` is `Applied`, `Unchanged`, or `Conflict`.

1. Comparison and publication are serialized against same-key ordinary methods
   and every existing batch. Lock order remains maintenance shared → transaction
   read → entry/shard → WAL. An existing batch owns transaction write. Maintenance
   capture/cutover cannot split a conditional mutation's acceptance/publication.
2. Mismatched expected bytes return Conflict, with no WAL access, event or barrier.
   Conflict does not attest store health, durable absence or a prior outcome.
3. Matching calls observe WAL Ready health before action selection. Matching Keep,
   equal Put, and absent Delete return Unchanged without an event or barrier.
   Unchanged is not a fresh durable acknowledgement or lifecycle publication.
4. Changed Put/Delete use ordinary single-event WAL paths. Applied means those
   paths completed under the opened policy. Buffered does not promise power-loss
   survival. Physical follows the existing actual file barrier contract; memory
   fixtures cannot prove it. No grouped frame or format migration is introduced.
5. Preserve existing MutationFailure information: only Rejected establishes the
   existing rollback result. Indeterminate/FailedClosed/untyped errors cannot be
   advertised as definitely unapplied. The API adds no retry or recovery policy.
6. A known poisoned health lock yields an untyped error, not a panic or invented
   persistence certainty. A readiness observation does not guarantee against a
   separate trusted writer subsequently poisoning the WAL.
7. Byte equality does not detect delete/recreate ABA with identical bytes. Callers
   needing irreversible history must encode and maintain it themselves.
8. Every matching action adds one synchronized WAL health read. Changed actions
   then take the original WAL write/check. Shared shard, gate and WAL waiting
   remain. No contention isolation or zero-cost claim is made.

## Admission and verification

This is an opaque storage primitive. Its contract and implementation are identical
for every caller supplying the same byte precondition and action.

**Motivation / provenance:** Penpack's uniform User mutation repair motivates
this primitive; its existing credential-record compare/delete is another use.
The larger native receipt capability remains subject to separate admission;
this slice establishes no permission to implement consumer-specific behavior.
No consumer rev pin is available until this change lands on dependency main. Frozen RED/GREEN evidence for
initial candidate behaviors lives in the source-pinned external evidence bundle.
The new file tests extend conformance, not a new failing behavior claim.

Required: focused memory tests and the full ordinary dependency suite; actual
file rotation, online maintenance, synthetic torn-event replay and reopen under
explicit policies; formatting/build/API documentation checks; reviewed clean
upstream main landing/release before a downstream rev pin. Do not infer a shipped
API from a local path override. Hardware power-loss and target performance remain
separate evidence, as do any downstream feature's admission and cost gates.
