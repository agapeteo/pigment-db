# Unpublished maintenance attempts
Status: approved for implementation, 2026-10-02. The amendments dated 2026-10-02 and 2026-10-03
below narrow or withdraw approved requirements and change return semantics:
- states covered by FR-3 (closed) and FR-4 that returned `AuthorityUndetermined` or
  `InvalidArtifact` at `1eb9de5` now recover (`Recovered`), FR-4's second window included (a
  rollback killed after its staging removal);
- FR-7: a removal that fails after its state was proved returns its I/O error
  (`CompactionError::Io { operation: Cleanup }` from a compaction, `RecoveryError::Io { operation:
  Inspect }` from an open), and so may FR-4's manifest removal;
- FR-1 and FR-7: a staging write that fails over a source changed under the claim keeps its staging,
  and the next open refuses where `1eb9de5`'s succeeded without what the source lost;
- FR-3 (online) and FR-5 are withdrawn: their states keep their `1eb9de5` errors.

**Signed off 2026-10-03 by Alex Gandzha (maintainer), on the condition that a final adversarial
review find no blocker or major; Review 6 (final) found none (verification.md).**

**Motivation (provenance):** found while measuring penpack finding V415. A closed compaction of a
copy of penpack's production store failed validation, and afterwards no open, inspection or
compaction would accept the directory: every open returned `AuthorityUndetermined` naming the store
and its staging directory, and `init_new` panicked. The store's own files were complete and had
never been moved. An investigation of the same code found the same outcome after a process kill at
six of the eleven closed-compaction cuts, after a failed first manifest publication in either kind
of compaction, after a kill inside an online cutover's source move, and after a kill inside a closed
rollback.

Spec 008's authority contract publishes `Prepared` before it builds staging (Closed compaction
sequence, step 3), and its classification rule ("Classification without a trustworthy manifest")
was written for that order: a complete staging generation with no manifest could only mean a lost
manifest. The code builds and validates staging first and publishes `Prepared` only afterwards; the
manifest codec refuses an unfinalized closed manifest, so it cannot simply be reordered. In the
code's order, staging with no manifest is the ordinary residue of every failure or kill before
publication, and it can never be authority: the canonical directory is moved only after `Prepared`
is durable.

## User scenarios and requirements

- **FR-1 (P1): a closed compaction that fails before publishing leaves nothing behind.**
  - Any failure or panic after the staging directory is created and before the `Prepared`
    manifest's publication succeeds removes that staging directory and any `.manifest.next` the
    attempt created, under the attempt's own claim, and returns the original error.
  - A failure after `Prepared` is published leaves its evidence, as today; recovery of `Prepared`
    already discards staging (spec 008 FR-049).
- **FR-2: a failed manifest publication leaves no `.manifest.next`.**
  - A manifest publication that fails before its rename removes the temporary file it created, for
    closed and online compaction alike, including an online finalize rewrite.
- **FR-3 (P1): recovery discards provable unpublished-attempt debris.**
  - At open and at the start of a compaction, under the replacement lock, when there is no main
    manifest (absent, not corrupt), no previous generation, and the canonical directory is a
    complete generation with at least one family:
    - a closed staging directory that is a real directory holding only regular files named for
      active segments of families the canonical directory holds (or nothing), and
    - a `.manifest.next` that is a regular file

    are removed, staging first, and the open reports `Recovered`.
  - The online analogue removes a lone `<active>.pigment-compact.manifest.next` when the family has
    no manifest, no previous directory, and its canonical family is valid. (Withdrawn after the
    third review; see Amendments.)
  - Every other state is unchanged: a missing or corrupt canonical directory, a previous generation,
    a corrupt main manifest, staging holding anything else, or staging that is a symlink still
    returns its current error and changes nothing.
- **FR-4: a closed rollback can be repeated.**
  - Recovery of `PreviousPublished` that finds the canonical directory equal to the source
    inventory, no previous generation, and staging present (a rollback interrupted after its restore
    rename) completes the rollback: it removes staging, then the manifest.
- **FR-5: an online cutover interrupted inside its source move restores the source.** (Withdrawn
  after the third review; see Amendments.)
  - A finalized online `Prepared` whose source artifacts are split between the canonical directory
    and its previous directory is recovered by restoring each moved artifact, verified against the
    manifest's descriptor, as closed recovery already does.
- **FR-6: the contract says what the code does.**
  - Spec 008's authority contract and plan state the implemented order: staging is built and
    validated before `Prepared`, and closed manifests are always finalized.
  - The classification section states that a missing main manifest, an absent previous generation
    and a complete canonical generation prove canonical authority, so owned staging and a
    `.manifest.next` are unpublished-attempt debris, removed under the replacement lock.
  - The fault-evidence paragraph requires every cut before `Prepared` to reopen with the exact old
    state.
- **FR-7: compatibility.**
  - No persisted format and no public signature changes.
  - States that returned `AuthorityUndetermined` or `InvalidArtifact` and are covered by FR-3
    (closed) and FR-4 now recover (FR-3 online and FR-5 were withdrawn; see Amendments). `inspect_storage` stays read-only (spec 008 FR-031) and keeps reporting such debris
    until an open or a compaction removes it.

## Acceptance

- A closed compaction made to fail at validation leaves no maintenance artifact; the store reopens
  three times with its exact state, and a following compaction succeeds.
- A process killed at each pre-`Prepared` cut (staging create, mid-write, staging sync, validation,
  manifest write, manifest sync), for each family, reopens as `Recovered` with exact state and no
  debris, and a following compaction succeeds. (Since the fifth review "mid-write" includes a kill
  inside the staged bytes, also for a store whose staged file spans many read blocks; such a kill
  keeps its error only when the bytes written stop exactly on a record boundary or right after
  the file header. Since the final review, two more qualifications: a real kill inside one large
  write stops at a page or page-cache folio boundary, not at an arbitrary byte, so in a store
  whose staged records all have one length a fixed share of such kills stop on a record boundary
  and keep their error -- one in three for 96-byte records; and any cut at which a family's file
  is staged keeps its error when that family holds a canonical key with no members or entries,
  which no current writer leaves.)
- A failed first manifest publication leaves no `.manifest.next`, for closed and online compaction;
  a planted lone online `.manifest.next` is removed at open (this half withdrawn after the third
  review: it keeps its error).
- Over-reach controls keep their current error and leave the directory byte-for-byte unchanged:
  canonical missing, canonical corrupt, previous present, staging holding a foreign file, staging as
  a symlink, a corrupt main manifest, staging holding a family the canonical directory lacks.
- An interrupted closed rollback, re-run, completes with the source state.
- A finalized online `Prepared` with a split source reopens with the original data and no debris
  (withdrawn after the third review: it keeps its error and its bytes).
- The existing cut-matrix test no longer accepts `AuthorityUndetermined`/`InvalidArtifact` at a
  pre-`Prepared` cut.
- Full suites and builds pass on every operating system CI runs.

## Amendments

- 2026-10-02, from the plan: FR-1's "published" means renamed into place. A `Prepared` publication
  that renamed its manifest and then failed a later barrier (a Physical directory sync) has
  published, and its staging is left to `Prepared` recovery like any failure after `Prepared`. The
  attempt tells the two apart by comparing the main manifest with its bytes when staging was
  created, so a manifest left by earlier maintenance does not keep the attempt's own staging.
  FR-1's `.manifest.next` half is met by FR-2: the attempt's only temporary is its publication's.
- 2026-10-02, from the plan: FR-4 also covers a rollback interrupted after it removed staging and
  before it removed the manifest. That state is FR-4's state without staging, the re-run refused it
  in the same way, and "a closed rollback can be repeated" needs both windows. Its acceptance line
  holds for a kill at either point.
- 2026-10-03, from the first review: FR-1 removes the attempt's staging only while the canonical
  directory is still exactly the generation the attempt captured. A source that changed under the
  claim (damaged by the medium, or written by a process outside specs/011's exclusion) can leave
  the staging the only complete copy of the captured state, and Principle II forbids removing it.
  The attempt then leaves its staging, and recovery decides as it does after a kill: FR-3 removes it
  when the canonical directory is a complete generation holding every staged family, and otherwise
  the open returns `AuthorityUndetermined` naming both, as before this spec.
- 2026-10-03, from the first review: "absent" in FR-3 (and in FR-1's comparison) means nothing is
  at the manifest's path. A symlink to nothing, or a directory, is not an absent manifest: FR-3's
  closed and online discards keep their current error beside one, and FR-1 leaves its staging.
- 2026-10-03, from the first review, FR-7: when an open or a compaction has proved a state to be
  debris under FR-3 to FR-5 but cannot remove it (permissions, a read-only mount, a file in use on
  Windows), it returns the I/O error of that removal or move (`RecoveryError::Io` from an open,
  `CompactionError::Io` from a compaction) where `1eb9de5` returned `AuthorityUndetermined` or
  `InvalidArtifact`. A removal or restore that stops part-way leaves a state that a later open
  recovers in the same way. (Superseded where it says "move" and "restore": with FR-5 withdrawn,
  no rule moves or restores anything; see the third review's FR-7 amendment.)
- 2026-10-03, from the second review, FR-3 (closed): a staging directory is removed only when it
  holds nothing the canonical directory lacks: every fact that replaying a staged file recovers (a
  key with its value, a set member, a map entry) is held by that family in the canonical
  directory. A staged file the replay rejects holds nothing an open could recover. Otherwise the
  open returns `AuthorityUndetermined` naming both, as at `1eb9de5`. This supersedes the first
  review's FR-1 amendment where it says FR-3 removes a staging whenever the canonical directory is
  a complete generation holding every staged family: a canonical directory truncated at a record
  boundary or rolled back, outside specs/011's exclusion, still is one, and the staging may then be
  the only copy of what it lost.
- 2026-10-03, from the second review, FR-3 (online) and FR-5: the lone family `.manifest.next` is
  removed, and a split source restored, only by an open or a compaction that is its process's only
  open instance of the family. Another open instance's live attempt writes exactly that temporary,
  and leaves exactly that split between its source moves and `PreviousPublished`; beside such an
  instance the state keeps its error from `1eb9de5`.
- 2026-10-03, from the second review, FR-3 to FR-5 and FR-7: a read that fails while a rule checks a
  state (a staging or previous directory it cannot list, a staged file it cannot read) proves
  nothing, and the state keeps its current error. FR-7's I/O error is returned only by a removal or
  a move that fails after the state was proved.
- 2026-10-03, from the second review, FR-1: the attempt compares the canonical directory with its
  capture where the store path resolved to when staging was created, so FR-1 holds whatever the
  path's spelling: a relative path whose parent is the empty path (the motivating defect's
  trigger) or a symlink to
  the directory. Spec 016 still owns what else those spellings break.
- 2026-10-03, from the third review, FR-3 (closed): a staging directory is removed only when each
  staged file either recovers exactly what that family's canonical files recover, or is shorter
  than a file header and so holds nothing (a staging write killed before its first byte, or inside
  its header). Equality, not the second review's containment: a staged state records a deletion
  only by a fact's absence, so a canonical directory that lost a delete -- truncated at a record
  boundary, torn inside the delete, or rolled back -- held every staged fact and one more, and the
  open removed the only copy of the state in which the fact was deleted and reported `Recovered`
  with it back. A canonical directory that gained a write the staging predates cannot be told from
  one that lost a delete, and now keeps its error too, as does a staged file torn past its header
  (a kill inside a staging write, which no cut stops at) unless its state equals the canonical one.
  A staged file holding at least a header that the replay rejects proves nothing: it may hold
  intact records the canonical directory lacks. Each of these keeps its error from `1eb9de5`. This
  supersedes the second review's FR-3 (closed) amendment.
- 2026-10-03, from the third review, FR-3 (online) and FR-5 are withdrawn: a lone family
  `.manifest.next`, and a finalized online source split between its canonical and previous
  locations, keep their errors from `1eb9de5`, and an open or a compaction changes nothing. The
  second review's condition -- the open is its process's only open instance of the family -- was
  decided when the open was admitted, before recovery ran, so an instance admitted later could
  open, compact and leave exactly that state while the first open was still recovering; and it
  counted only this process's instances, while where locks are not supported (specs/011 FR-11)
  another process's live attempt is excluded by nothing. In both cases the rule restored a live
  cutover's source, the cutover failed, and the store refused to open, where `1eb9de5` refused
  only the open that found the split. A sound rule needs coordination this spec does not add
  (recovery serialized per family, or live online attempts recorded where every opener sees them),
  which needs its own specification. FR-2 still keeps a failed online publication from leaving its
  temporary; only a process killed inside that publication, or inside a cutover's source moves,
  leaves these states. This supersedes the second review's FR-3 (online) and FR-5 amendment.
- 2026-10-03, from the third review, FR-1: the attempt resolves every path it acts on -- the
  canonical directory, the main manifest and the staging directory -- when staging is created, and
  checks and removes only through those. A relative store path read again when the attempt fails
  names another directory's artifacts if the process's working directory changed meanwhile.
- 2026-10-03, from the third review, FR-7: with FR-5 withdrawn, no rule moves anything, and FR-7's
  I/O error is returned only by a removal that fails after the state was proved. Closed
  `PreviousPublished` recovery reads the staging path before any rule runs and returns that read's
  I/O error as it did at `1eb9de5`; FR-4's completion reads it once more before removing it, which
  can fail only if the path became unreadable in between.
- 2026-10-03, from the fourth review, FR-3 (closed): a staged file is removed only when it is byte
  for byte what a closed compaction of the canonical directory stages for its family -- the state
  the family's canonical files recover, encoded as compaction encodes it, with the timestamp
  metadata those files carry -- or is shorter than a file header and holds nothing: the replay
  rejects it or recovers no key. This narrows the third review's equality of recovered states,
  which held two whole states at once and made an open finding debris peak above the bound plan IV
  sets (measured 1.52 times a clean open's peak). Each of these now keeps its error from `1eb9de5`:
  a staged file recovering the canonical state in other bytes (a byte copy of a canonical file,
  say); a canonical key whose set has no members or whose map has no entries (an open publishes
  it, while its encoding is the key's absence, which is how a staged snapshot records a delete;
  narrowed by the final review to a key of a family whose file is staged);
  and a file shorter than a header that recovers a key (a headerless legacy file, which compaction
  never writes). Every cut before `Prepared` still recovers (qualified by the fifth and final
  reviews: see the Acceptance line on kills inside a staging write). This supersedes the third
  review's FR-3 (closed) amendment where it says "recovers exactly what that family's canonical
  files recover".
- 2026-10-03, from the fourth review, FR-3 (closed): the rule reads and removes only in the
  directory that the open's lease or the compaction's claim locked (specs/011), resolved once when
  the rule starts, and only while the caller's path still names that directory. Otherwise the
  open or compaction answers as at `1eb9de5`. A path read again at each step named another
  directory once the working directory or a symlink on the path changed, and the rule removed a
  staging there that it had never compared.
- 2026-10-03, from the fourth review, FR-1 and FR-2: the attempt resolves its paths before it
  creates the staging directory and creates it through them; a manifest publication resolves its
  temporary's and the main manifest's paths before it creates the temporary, and creates, renames
  and removes through them. So each removes only the entry it created.
- 2026-10-03, from the fourth review, FR-7: a staging write that fails while the source changed
  under the claim leaves the staging (FR-1's first-review amendment), and the next open returns
  `InvalidArtifact` naming the staging, where `1eb9de5` removed the staging and the next open
  succeeded without what the source had lost. This is a change of return semantics like FR-7's
  `Io`, and the sign-off covers it.
- 2026-10-03, from the fifth review, FR-3 (closed): a staged file is removed only when it is byte
  for byte what a closed compaction of the canonical directory stages for its family, or the first
  bytes of that encoding ending inside its file header or inside one of its records -- what a
  staging write killed before its first byte or inside its bytes leaves. Every byte of such a
  file, the gaps between its complete records included, is the canonical directory's own encoding,
  so any deletion it records the canonical directory records too; and with its header or last
  record cut it is no complete snapshot, so it records nothing beyond the cut. Removing it
  therefore loses nothing. (Reason corrected by the final review, which found the earlier wording,
  "records no deletion", false: such a file records the absence of every key sorting between its
  complete records, and it is the byte comparison that makes those absences the canonical
  directory's own.) A file of those first bytes ending right after the header or on a record
  boundary is a complete snapshot of fewer facts, which records the rest as deleted, and keeps its
  error from `1eb9de5`, as does any other file, a short headerless legacy file included whatever
  its replay recovers. This supersedes the fourth review's FR-3 (closed) amendment where it says
  "or is shorter than a file header and holds nothing: the replay rejects it or recovers no key",
  and the third review's where it says a staged file torn past its header keeps its error: a plain
  process kill inside a staging write produces one, and the store then could not be opened, which
  is the defect this specification exists for.
- 2026-10-03, from the fifth review, FR-3 (closed): the comparison is with what the current
  encoder writes, so debris staged by an earlier release is removed only while the encoder writes
  the bytes that release wrote, and otherwise keeps its error from `1eb9de5`. A frozen fixture
  written by `1eb9de5` pins those bytes, for what its data can show: since the final review, in
  each family, keys whose order by length differs from their byte order, and sets and maps with
  several members or entries, of different lengths, in more than one key.
- 2026-10-03, from the fifth review, FR-1: "exactly the generation the attempt captured" (the
  first review's FR-1 amendment) means the same names and, in each file, the very bytes captured.
  A comparison of each file's length and CRC-32 read damage that kept both as no change, and the
  attempt removed the staging that was the only intact copy of the captured state.
- 2026-10-03, from the fifth review, FR-7: the removal's I/O error carries
  `operation: CompactionOperation::Cleanup` from a compaction's start, and
  `operation: RecoveryOperation::Inspect` from an open, as every compaction-side I/O error that
  directory recovery returns to an open has since before this specification.
- 2026-10-03, from the final review, FR-3 (closed), wording only (no behaviour changes):
  - the fourth review's refusal for a canonical key with no members or entries applies to a key
    of a family whose file is staged. A family with no staged file is not compared, and nothing of
    it is removed, so such a key there neither refuses nor is at risk;
  - a process killed inside one large staging write leaves a length that is a multiple of the page
    size or of a larger page-cache folio, so the fifth review's record-boundary exception is
    reached by a fixed share of real kills in a store whose staged records all have one length,
    not by rare coincidence; closing it needs a production change, which plan.md's "Not done here"
    records for its own decision;
  - the soundness reason in the fifth review's torn-write amendment is corrected where it stands.
- The amendments dated 2026-10-03 were made during implementation, in response to its reviews.
  They narrow or withdraw requirements approved on 2026-10-02, change the return semantics named
  in the Status line, and await the approver's sign-off, which this document does not record. So
  do the two amendments dated 2026-10-02, which were also made during implementation (from the
  plan), after the approved text, which has no Amendments section. The FR-4 one changes return
  semantics too, and the Status line does not yet name it (final review; the line is the
  requester's to amend): a closed rollback killed after its staging removal and before its
  manifest removal returned `AuthorityUndetermined` at `1eb9de5`; an open or a compaction now
  completes the rollback (an open reports `Recovered`), or returns that manifest removal's I/O
  error (`RecoveryError::Io`, `CompactionError::Io`) when it fails.
