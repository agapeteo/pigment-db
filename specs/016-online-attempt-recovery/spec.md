# Online attempt recovery
Status: approved for implementation, 2026-10-03, by Alex Gandzha (maintainer), including FR-1's new refusal (FR-4).
One return-semantics change the approved text does not name, made during implementation
(Amendments), and stated in full by the first review:
- an open that has proved a dead attempt's leftovers (FR-2) and whose removal of the temporary,
  move of an artifact back, or directory barrier after those moves then fails returns that I/O
  error (`RecoveryError::Io` with `operation: RecoveryOperation::Inspect`), where `9dff848`
  returned `AuthorityUndetermined`;
- once a split is restored, the abandonment that follows, unchanged since `9dff848`, runs where
  `9dff848` stopped before it: a removal or barrier there that fails returns its I/O error the
  same way, and a state the abandonment refuses (a family staging that is not a regular file)
  keeps `AuthorityUndetermined` with the source already moved back and the empty previous
  directory removed (plan.md, "Not done here", records the check that would keep its bytes).

**Signed off 2026-10-04 by Alex Gandzha (maintainer).** The approval of 2026-10-03 covered this
specification, its sign-off under the maintainer's name and its release; the change above is of the
kind signed off for spec 015's FR-7 (an I/O error where a proved state cannot be repaired), and the
first review found no blocker or major (verification.md).

**Motivation (provenance):** penpack finding V415, whose fix compacts its stores online. Spec 015
set out to recover two states an online attempt leaves only when its process is killed: a family's
lone `.manifest.next` (killed inside the first publication), and a finalized online source split
between the store directory and its previous directory (killed inside the cutover's source moves).
Both make every later open refuse, until an operator repairs the directory by hand. Spec 015's
reviews withdrew both rules: an open cannot tell a dead attempt's leftovers from a live attempt's
work in progress, because nothing excludes a second instance of the same family on the same
directory in the same process, and where locks are unsupported (spec 011 FR-11), nothing excludes
another process either. Restoring a live cutover's source makes that cutover fail and leaves the
store unopenable. At `1eb9de5` two instances of one family on one directory in one process that
both write already leave a WAL the next open refuses, with no compaction involved.

## User scenarios and requirements

- **FR-1 (P1): one instance of a family per directory per process.**
  - Opening a file-backed store of a family whose directory this process already holds open for
    that family is refused with an error naming the directory and the family, before any recovery
    runs. Spelling does not matter: the directory's identity decides (spec 011).
  - Dropping the instance ends the hold; a later open succeeds.
  - Other families of the same directory, and the same family of another directory, are
    unaffected.
- **FR-2 (P1): an open recovers a dead online attempt's leftovers.** With FR-1 and spec 011's
  cross-process exclusion, an opening instance is the only live writer of its family, so no online
  attempt of that family is live:
  - a lone `<active>.pigment-compact.manifest.next` with no main manifest and no previous directory,
    beside a valid canonical family, is removed;
  - a finalized online `Prepared` whose source artifacts are split between the canonical and
    previous locations is restored, each moved artifact verified against the manifest's
    descriptor, as closed recovery does;
  - the open reports `Recovered`.
- **FR-3: only where exclusion holds.** On a target or filesystem where spec 011 takes no lock
  (FR-11), FR-2 does not run and these states keep their errors.
- **FR-4: compatibility.**
  - FR-1 is a new refusal: a consumer that opens one family twice on one directory in one process
    must share one instance. This is the breaking change this spec exists to approve; such a
    consumer could already corrupt its WAL.
  - No persisted format changes.

## Acceptance

- A second open of the same family and directory in one process, through any spelling, is refused
  before recovery, and succeeds after the first is dropped; other families and directories open.
- A process killed inside an online attempt's first publication, and inside each source move of a
  cutover, for each family, reopens as `Recovered` with exact state.
- The live-cutover scenarios that spec 015's reviews used against the withdrawn rules (a second
  instance opening, compacting or recovering during a live cutover) are refused by FR-1 and leave
  the cutover to complete.
- Without locks (FR-11), the leftover states keep their errors.
- Full suites and builds pass on every operating system CI runs.

## Amendments

- 2026-10-03, from the plan, FR-1, wording: the refusal is `RecoveryError::Io` with
  `operation: RecoveryOperation::Inspect`, `path` the caller's path, and a source of kind
  `WouldBlock`, the shape spec 011 gives another process's open, so a consumer handles one
  refusal. Its message names the directory by its identity (the canonical path every spelling
  resolves to) and the family as `key/value`, `key/set` or `key/sorted-map`. `init_new` panics
  with that message.
- 2026-10-03, from the plan, FR-2 and FR-3, wording: an open is "the only live writer of its
  family" when its process holds the family (FR-1) and holds the directory's inner lock (spec
  011), really locked (not skipped under spec 011's FR-11) and still the file at the directory's
  lock path, decided once directory recovery is done. An open that found directory-level
  maintenance holds only the replacement lock until after its recovery, so FR-2 does not run in
  it; that loses nothing reachable, because directory recovery refuses a canonical directory
  holding a family's online artifacts before family recovery runs.
- 2026-10-03, from the plan, FR-2, scope: the rules run at an open, as FR-2 states, and not at an
  online compaction's start, where both states keep their errors from `9dff848`.
- 2026-10-03, from the plan, FR-2: the rules act only in the directory the open locked, resolved
  once, and only while the caller's path still names it, as spec 015's closed discard does; a
  path that cannot be read proves nothing, and the state keeps its error.
- 2026-10-03, from the plan, FR-2, return semantics (Status line): a removal or a move that fails
  after its state was proved returns its I/O error, as spec 015's FR-7 does for its rules.
- 2026-10-03, from the first review, FR-2, return semantics: the item awaiting sign-off in the
  Status line now names, besides the removal and the moves, D6's directory barrier, and what the
  abandonment does once a split is restored, which the earlier wording left out (plan.md II, D6,
  and III). The first line of the Status line, the approval, is unchanged, and no sign-off is
  recorded.
- 2026-10-03, from the first review, FR-2, wording: a "lone" `.manifest.next` also has no family
  staging (`<active>.pigment-compact.next`) beside it, as spec 008's contract requires before the
  canonical family is the authority; the rule has required it since it was written.

