# Atomic KV implementation plan

## Technical context
Rust 2021 library crate. Existing records and their encodings remain unchanged; keys and values stay opaque bytes.

**Motivation (provenance):** penpack's atomic guest KV batch import (penpack `specs/017-atomic-kv-batch`), used by Panta Studio to commit generated content, its usage counter and an idempotence receipt together. The consumer's own keys and JSON records were unchanged by it.

## Constitution check
Principle VI: the contract is stated in storage terms (keys, expected bytes, replacements, one WAL group); the motivating consumer is named in the Motivation note above; the implementation `550a55f` is on `main`. No panic on caller input, unchecked size arithmetic, unbounded decode or recursion. An invalid batch fails closed before any lock or write. Every configured limit is enforced. Logical mutations are atomic; preserve API and disk compatibility. Test-first vertical slices; memory-only unit tests; isolated disk integration. The user approved the storage coordination gate because a lock held only by a batch's caller is bypassed by ordinary writes through the same store. Shared WAL adds only KV actions; set/map semantics stay unchanged. Consumers pin the tested `main` revision.

## Decisions
Lock order: maintenance shared -> transaction gate -> map/WAL locks. Ordinary mutations/read paths acquire read access; batch acquires exclusive access. Maintenance exclusive excludes mutation. No lock is held across an await or outside the call that takes it. Validate first; accept one complete WAL group, then publish under protection. A failed log rollback fails closed pending recovery. Separate get calls are not a multi-read snapshot.

The API takes a slice of entries, each a key, an optional expected value and an optional replacement, where None means absence or deletion. It returns Applied or Conflict, or an I/O error: InvalidInput for an empty, over-16 or repeated-key batch, and Unsupported for a legacy-framed store. The library caps entry count only. Byte ceilings on keys and values, key namespacing and any wire encoding belong to the caller.

Intended use: a caller reads the keys it will change, builds the replacements, and submits one batch that expects each value it read plus the absence of an idempotence marker key. On Conflict it re-reads and retries a bounded number of times, repeating only storage work. The caller's other writes to those keys must also be conditional, or a batch built from a stale read can overwrite them. Two batches contend only when they share a key.

## Performance and release
Record a matched release ordinary-KV baseline before edits; median overhead <=30%. The gate covers the storage commit only, not work a caller does before submitting a batch. Validate restart and old/new reader compatibility, do not assume it. The change lands on `main`, and consumers pin the tested `main` revision.
