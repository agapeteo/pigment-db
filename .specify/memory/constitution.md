<!--
Sync Impact Report
- Version change: 1.1.0 -> 1.2.0 (MINOR: a Project Constraint's mandatory
  guidance expanded; no principle removed or redefined)
- Modified sections:
  - Project Constraints: single-process ownership of a store directory is now
    enforced by the lock files specs/011 defines, and remains a convention only
    where a lock cannot be taken.
- Motivating consumer: penpack (two processes appended to one WAL for twelve
  hours; see specs/011's Motivation note). Classified under VI as a
  library-vocabulary primitive landing on main before any consumer pins it.
- Dependent artifacts: specs/011-cross-process-directory-lock (spec, plan,
  tasks, verification).
- Compatibility impact: no persisted-format or public API change. The lock
  files' names, locations and locking are now a compatibility contract.
- Active work to revalidate: none.

Sync Impact Report
- Version change: unratified template -> 1.0.0
- Modified principles:
  - Placeholder principle 1 -> I. RED-GREEN Test-Driven Development
  - Placeholder principle 2 -> II. Durable and Live State Integrity
  - Placeholder principle 3 -> III. Compatibility Is an Explicit Contract
  - Placeholder principle 4 -> IV. Bounded Concurrency and Measured Performance
  - Placeholder principle 5 -> V. Public Evidence and Scope Discipline
- Added sections:
  - Project Constraints
  - Development Workflow and Quality Gates
  - Concrete governance and semantic-versioning rules
- Removed sections: template-only placeholder content and example comments
- Follow-up TODOs: none

Sync Impact Report
- Version change: 1.0.0 -> 1.1.0 (MINOR: principle added, mandatory review
  guidance expanded; nothing removed or redefined)
- Added principles:
  - VI. Consumer-Agnostic Library (NON-NEGOTIABLE)
- Modified sections:
  - Development Workflow and Quality Gates: item 6 names each principle's cure
    (VI is cured by its own remedy, not by amending a feature spec); item 7 —
    Principle VI is settled first in every review and Constitution Check.
- Dependent artifacts:
  - .specify/templates/plan-template.md: updated (VI added as the first gate)
- Compatibility impact: none on persisted data or the public API.
- Active work to revalidate before it resumes (Governance):
  - Branch admin-console-for-panta-studio: db490a7 (specs/010, titled for Studio
    administration), ce50b4f (bounded key-set pagination), 7aff8ad (batch limits
    raised for one consumer's grouped publication), 07d1ef9 (wip(021) storage
    work). Consumer-specific as held; any generic primitive is re-landed from
    main under VI.
  - Branch safety/admin-console-20260921-pigment-pre-wip: db490a7, ce50b4f and
    7aff8ad (not 07d1ef9).
- Consumer vocabulary on main at adoption (the closed list VI refers to),
  rewritten before this amendment was committed, by 225456b:
  - specs/009-atomic-kv-batch: spec.md stated requirements in one
    application's terms (books, editions, TTS, receipts); plan.md named the
    application outside a Motivation note and stated the design as
    "book+usage+absent receipt". Now in storage terms, with the consumer in
    marked Motivation notes; the verification.md cross-project evidence stays.
  - src/atomic_kv_tests.rs:46: the test
    competing_batches_have_one_winner_and_no_extra_receipt is now
    competing_batches_have_one_winner_and_losers_write_nothing (its
    b"book"/b"receipt" bytes are opaque test data and stay).
  - The atomic compare-exchange batch code on main is generic and stays.
-->
# pigment-db Constitution

## Core Principles

### I. RED-GREEN Test-Driven Development

Every issue and feature MUST be developed one behavior at a time with this cycle:

1. Write one behavior-focused test before changing production code.
2. Run that test and confirm it fails for the expected behavioral reason (RED).
3. Implement only the minimum production change needed to satisfy the test.
4. Run the test and confirm it passes (GREEN), then run the full relevant suite.

Refactoring MUST begin only while the relevant tests are green, and those tests
MUST be rerun after each refactor. A compilation failure or unrelated failure does
not constitute valid RED evidence. This sequence keeps every production change
connected to an observable requirement and makes regressions reproducible.

### II. Durable and Live State Integrity

For every operation accepted under the documented durability policy, the live
logical state and the state reconstructed after reopening MUST agree. A rejected,
interrupted, or abandoned operation MUST NOT silently supersede the last accepted
recoverable state.

Changes affecting WAL writes, replay, recovery, publication, or mutation ordering
MUST define the authoritative-state transition and test it at each relevant failure
or interruption boundary. If authority cannot be established without risking data
loss, the library MUST fail explicitly and preserve recoverable artifacts. Cleanup
MUST NOT destroy the last complete authoritative state.

### III. Compatibility Is an Explicit Contract

Existing public signatures, return semantics, callback eligibility, panic-versus-
error behavior, key-existence behavior, and valid persisted data MUST remain
compatible unless an approved specification explicitly defines a breaking change
and migration path.

On-disk format changes MUST be versioned and accompanied by compatibility or
migration tests. Frozen legacy fixtures are immutable inputs and MUST NOT be
regenerated by the implementation under test. When a safer fallible path is needed
without an approved breaking change, the implementation MUST add an API instead of
changing an existing signature.

### IV. Bounded Concurrency and Measured Performance

Coordination MUST be scoped to the smallest existing correctness boundary that
satisfies the specified invariant. A change MUST NOT introduce a whole-operation
global lock, unbounded per-key coordination state, or a new coordination layer
unless an approved specification demonstrates why the simpler existing boundaries
cannot work.

Concurrency-sensitive changes MUST define lock ownership and ordering, include
deterministic progress and deadlock tests, and state which independent operations
may block. Performance-sensitive changes MUST capture a reproducible baseline
before production edits, define measurable acceptance thresholds, and compare the
candidate under matching workloads and environments. A failed threshold MUST be
fixed in the implementation, not weakened after measurement.

### V. Public Evidence and Scope Discipline

Acceptance assertions for state and contract outcomes MUST use public reads,
operation results, or reopen behavior. Private test seams MAY schedule an otherwise
unobservable interleaving or failure, but MUST be absent from normal builds and
MUST NOT replace assertions against the public contract.

Each change MUST remain within its approved issue or specification. Unrelated
defects, format redesigns, dependency additions, and public contract changes MUST
be deferred to separately approved work. A plan that introduces additional
complexity MUST name the governing requirement and the simpler alternative it
rejects.

### VI. Consumer-Agnostic Library (NON-NEGOTIABLE)

pigment-db is a general-purpose embedded storage library. Its consumers — penpack,
and every application built on penpack — MUST NOT be visible in it. Its API, types,
persisted formats, limits, errors, tests, fixtures, branch names, and spec
directory names and titles MUST be stated in the library's own vocabulary: keys,
values, sets, sorted maps, batches, snapshots, segments, the WAL, compaction and
recovery.

A change is consumer-specific, and MUST be refused, if it adds:

- a consumer's or application's name, or a term used in a consumer's sense
  (penpack itself; penpack's hosts, tenants, content versions, endpoints, WASM
  modules or accounts; any application's domain objects), in an identifier, type,
  error, limit, cargo feature, branch name, or spec directory or title. A word the
  library already uses in its own sense — a format or WAL version, a Rust module, a
  key, a set, a snapshot — is not a hit;
- a consumer's key format, prefix or separator (`<PP_`, `{host}|`), record
  encoding, or limit;
- an API whose contract can only be described in a consumer's concepts, or whose
  parameters encode one consumer feature's sequence of calls;
- behaviour, coordination or a limit whose scope, keys or timing encode one
  consumer's workflow.

A primitive motivated by one consumer is admissible only when it is stated in the
library's own vocabulary, any consumer can use it unchanged through the public API
with its own parameters, it lands as its own specification and change on this
repository's `main` branch **before** any consumer pins it, and it satisfies
Principles I–V on its own. Atomic compare-exchange batches and bounded snapshots
are examples of the admissible shape. A revision reachable only from a branch named
for a consumer or an application is not admissible, whatever its code contains.
The change description MUST name the motivating consumer. Prose MAY name a
consumer only as provenance — a marked Motivation note, cross-project
verification evidence, or a comment or changelog note citing the consumer defect
a fix addresses or the consumer whose compatibility a constraint protects — and
every requirement MUST still hold with the name removed. Tests MAY use consumer-shaped values only as opaque bytes.

Classification is of the aggregate. The change description MUST name the consumer
feature it serves, and concealing it is itself a violation. A library change whose
motivating consumer feature is refused under that consumer's own constitution is
refused here too, however it is worded, and a set of library changes whose only
combined use is one consumer feature is judged as one change.

Principle V bounds how much a change does; this principle bounds whose requirement
it serves. Satisfying V, or holding an approved specification, does not satisfy VI.

Consumer vocabulary already on `main` when this principle was adopted (1.1.0) is
listed in the Sync Impact Report and was rewritten before the principle was
committed. That list is closed: vocabulary found later is a violation, not debt.

A violation is a **CRITICAL architectural defect**. It blocks implementation,
merge and release however correct or well tested the code is, and no feature
document, plan or per-feature approval waives it; only an amendment of this
principle can. Consumer-specific code is removed, not repaired in place, and any
generic primitive inside it is re-landed from `main` under the conditions above.
Specification prose written in a consumer's terms is rewritten; a persisted format
or public API already shipped is never removed to cure prose. A removal follows
Principle I: its RED is a public-behaviour test that fails while the
consumer-specific behaviour exists, such as a batch the consumer-shaped limit
refuses or a key the consumer's format mishandles.

**Rationale**: penpack's merge `961ab82` (2026-09-18, rolled back 2026-09-21)
pinned pigment-db `db490a7`, a bounded-snapshot change made on the branch
`admin-console-for-panta-studio`, whose spec is titled "Bounded key-set snapshots
for Studio administration" and calls itself an "approved narrow dependency" of one
application's admin console. The revision was on no `main` branch. Its code was
generic; its branch, specification and purpose were not. Principle V was satisfied
— the specification was approved — and nothing here asked whose requirement the
change served.

## Project Constraints

- pigment-db remains a Rust library crate; production dependencies MUST NOT be
  added without an approved plan explaining necessity and maintenance impact.
- Each file-backed store directory is owned by one process, and that ownership is
  enforced by the lock files specs/011 defines; their names, locations and lock
  semantics are a compatibility contract. Where a lock cannot be taken, single-
  process ownership remains a convention: unsupported platforms or filesystems,
  network filesystems, and closed maintenance while another mount view of the
  directory exists.
- Persistent-state changes MUST account for all three durable store families:
  key/value, key/set, and key/sorted-map, unless the specification explicitly
  demonstrates that a family is unaffected.
- Tests that rely on internal scheduling MUST respect Rust test-compilation
  boundaries: crate-private `cfg(test)` helpers belong to unit tests, while external
  integration tests MUST consume only interfaces available to their compiled crate.
- Unsafe code, new file formats, and platform-specific behavior require explicit
  rationale, bounded scope, and targeted tests in the implementation plan.

## Development Workflow and Quality Gates

1. Specifications MUST state observable requirements, compatibility boundaries,
   failure behavior, and measurable success criteria before implementation.
2. Plans MUST identify state authority, lock ordering, public/API impact, persisted-
   data impact, and performance validation whenever those concerns apply.
3. Tasks MUST preserve vertical RED-GREEN ordering. Setup and test infrastructure
   MAY precede the first RED, but production behavior MUST NOT change before its
   behavior-focused failing test is observed.
4. Before completion, the targeted tests and full relevant suite MUST pass. Rust
   changes MUST also pass formatting; new Clippy diagnostics MUST be resolved or
   explicitly documented as pre-existing.
5. Changes affecting persisted data MUST run applicable reopen, recovery, and
   frozen-fixture compatibility tests. Changes affecting concurrency or performance
   MUST run their deterministic conformance or benchmark gates.
6. Reviews MUST cite evidence for correctness, compatibility, and applicable
   performance claims. A constitution violation blocks implementation or release
   until it is cured: for Principles I–V, by fixing the change or amending the
   governing artifact through the process below; for Principle VI, by the remedy
   that principle states.
7. Every review and every plan's Constitution Check MUST settle Principle VI
   **first**, before correctness, compatibility or performance, recording a scan of
   the whole change for consumer and application vocabulary, the motivating
   consumer, and the `main`-branch revision any consumer will pin. A review that
   reports no Principle VI classification is incomplete.

## Governance

This constitution governs all feature specifications, implementation plans, task
lists, code changes, and reviews in this repository. More-specific project
instructions MAY impose stricter requirements but MUST NOT weaken these principles.
If guidance conflicts, this constitution takes precedence.

Amendments require an explicit constitution update describing the rationale,
affected principles, compatibility impact, and any required migration of active
work. The Sync Impact Report MUST be updated with every amendment, and affected
specifications, plans, or tasks MUST be revalidated before implementation resumes.

Constitution versions follow semantic versioning:

- **MAJOR**: removal or incompatible redefinition of a principle or governance rule.
- **MINOR**: addition of a principle or materially expanded mandatory guidance.
- **PATCH**: clarification or wording correction with no semantic policy change.

Every code review and Spec Kit analysis MUST verify applicable constitutional
rules. Exceptions require an explicit, approved constitution amendment; a feature
document alone cannot waive a principle.

**Version**: 1.2.0 | **Ratified**: 2026-08-06 | **Last Amended**: 2026-09-25
