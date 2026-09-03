# Atomic KV implementation plan

## Technical context
Rust 2021 pigment-db/Penpack, WASI core and Panta (Rust 2024). Existing book, usage and receipt keys/JSON remain unchanged.

## Constitution check
No untrusted panic, unchecked size arithmetic, unbounded decode or recursion. Tenant/permissions fail closed. Every configured limit is enforced. Logical mutations are atomic; preserve API and disk compatibility. Test-first vertical slices; memory-only Penpack unit tests; isolated disk integration. The user approved the storage coordination gate because WA/linker-only locks are bypassed by ordinary writes. Shared WAL adds only KV actions; set/map semantics stay unchanged. Pin the tested dependency for shipping.

## Decisions
Lock order: maintenance shared -> transaction gate -> map/WAL locks. Ordinary mutations/read paths acquire read access; batch acquires exclusive access. Maintenance exclusive excludes mutation. No locks span await or guest allocation. Validate first; accept one complete WAL group, then publish under protection. A failed log rollback fails closed pending recovery. Separate get calls are not a multi-read snapshot.

Versioned length-delimited host ABI with checked cursor decoding and optional-byte tags. Return scalar outcome Applied/Conflict/StorageUnavailable; malformed/forbidden input traps. Library caps entry count; raw byte/key limits belong to the host, before tenant prefixing.

WA reloads and appends one immutable generated edition, atomically checks book+usage+absent receipt, and retries boundedly without TTS. Existing book writes must use conditional storage. Different books may contend only over account usage.

## Performance and deployment
Record a matched release ordinary-KV baseline before edits; median overhead <=30%. Gate affects local commit only, not synthesis. Validate restart and old/new reader compatibility, do not assume it. Develop via explicit local Cargo patch; final dependency must be pinned to tested commit. Deploy host before new WASM, preserving descriptor settings and current data. Coordinate user process restart; never operate their terminal sessions.
