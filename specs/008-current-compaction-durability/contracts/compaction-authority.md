# Contract: Compaction Authority, Publication, and Recovery

## Canonical scope and artifacts

Directory compaction owns a unique same-parent staging directory, previous-generation directory, main manifest, and unpublished manifest temporary. Online compaction owns equivalent family-scoped artifacts derived from the exact canonical active filename. Native path components are appended losslessly; every manifest path is relative to its anchor and cannot escape it.

The manifest is small, versioned, bounded, CRC32-checksummed, and contains only operation metadata and artifact descriptors. It identifies closed versus online mode and whether an online `Prepared` source inventory has been finalized. It does not change the V2 WAL record format and is not a compatibility layer.

## Publication preconditions

Before `Prepared`:

- same-process closed ownership or per-instance online attempt ownership is established;
- prior maintenance is resolved or safely left as an explicit error;
- every store-directory entry is canonical or recognized maintenance metadata; any unexpected entry has already returned `InvalidArtifact` without mutation;
- scope, family, names, segment continuity, and current V2 integrity validate.

Before `PreviousPublished`:

- replacement contents and parent namespace meet the requested durability policy;
- replacement reopens as current V2 and exactly matches required state/metadata;
- the manifest contains complete source and replacement descriptors;
- closed source is re-read byte-for-byte, or online source is frozen under exclusive maintenance coordination;
- no unrecorded accepted mutation can cross the cutover boundary.

Before `ReplacementPublished`, the verified old source is complete at the previous-generation location. Before `CleanupPending`, canonical replacement reopens and is proven authoritative. Old artifacts are not deleted before that point.

## Phase contract

| Phase | Canonical authority | Required preserved evidence | Legal recovery |
|-------|---------------------|-----------------------------|----------------|
| `Prepared` | Old source | Main manifest, source descriptors/prefix, any owned staging | Restore split old artifacts; accept valid WAL growth beyond an online prefix; discard staging only when it cannot be authority. |
| `PreviousPublished` | Transitioning; decision from evidence | Verified previous and complete candidate replacement | Prefer fully validated replacement; otherwise restore verified previous; ambiguity fails closed. |
| `ReplacementPublished` | Replacement after validation | Replacement plus previous until confirmation | Validate/select replacement; contradictory or missing evidence fails closed. |
| `CleanupPending` | Replacement | Replacement immutable prefix and exact obsolete descriptors | Serve replacement; retry only descriptor-proven cleanup; remove manifest last. |

Each phase file publication is atomic. In physical mode, the manifest content barrier and namespace barrier complete before the new phase is considered durable.

An online attempt writes `Prepared` before releasing its initial snapshot gate. Its source descriptor is then a verified prefix of the authoritative WAL, which may append and rotate during staging. At cutover, under exclusive coordination, it atomically rewrites `Prepared` with the exact final inventory and `source_finalized = true`. Publication cannot move source artifacts until that durable rewrite succeeds. Recovery from either online `Prepared` form uses the canonical old WAL and normal current-format recovery; valid advancement beyond the prefix is not a contradiction. Recovery does not restore a finalized online source split between its canonical and previous locations, although the `Prepared` row permits it: another open instance of the family -- in the same process, or in another process where locks are not supported -- leaves exactly that split between its source moves and `PreviousPublished` while its cutover is live, and recovery cannot tell the two apart. Such a split keeps `AuthorityUndetermined`.

> Added 2026-10-03 (specs/015 FR-5, after its second review): a sentence restoring the split for an open that was its process's only open instance of the family. Replaced 2026-10-03 after its third review, which withdrew FR-5: that condition was decided before recovery ran, and did not see another process; the last sentence now states what recovery does.

## Closed compaction sequence

1. Atomically claim a directory with no same-process open leases.
2. Read-only resolve authority and capture every family, exact file bytes/descriptors, logical state, family, granularity, and last bucket.
3. Build one current-V2 active segment per family in a unique same-parent staging directory.
4. Synchronize as requested; reopen three times in acceptance tests; compare complete state and metadata.
5. Re-read source entries/names/bytes. Any addition, removal, rename, length or same-length content change aborts publication with old authority intact. Then publish `Prepared`; a closed manifest is always finalized, recording the exact source and replacement inventories. An attempt that stops before `Prepared` is published removes its own staging only while the canonical directory is still exactly the generation it captured; otherwise it leaves the staging for recovery.
6. Publish old directory as previous; durably write `PreviousPublished`.
7. Publish staging canonically; durably write `ReplacementPublished`.
8. Reopen/validate canonical replacement; durably write `CleanupPending`.
9. Delete only owned checksum-matching old artifacts; remove manifest last. Failure returns pending cleanup.

> Corrected 2026-10-02 (specs/015 FR-6): step 3 said `Prepared` was published before staging was built; step 5 now places the publication. The implementation has always built and validated staging first, and the manifest codec refuses an unfinalized closed manifest, so the order above is the implemented one. Until `Prepared` is durable, staging is unpublished-attempt debris (below).

An empty directory is a read-only no-op. Repeating compaction is safe and preserves state.

## Classification without a trustworthy manifest

- If available complete evidence can identify more than one plausible authority, canonical authority is absent with a plausible prior/staging generation, or required phase evidence contradicts itself: return `AuthorityUndetermined` with every relevant path.
- If one complete authority is proven and malformed debris cannot represent a complete competitor: return `InvalidArtifact` for that debris.
- Never rename, delete, truncate, or synthesize evidence while returning either error.
- A valid main manifest controls recovery. A valid `.manifest.next` alone is an unpublished attempted revision and does not advance the durable phase.
- A missing main manifest (nothing at its path: not corrupt, and not a symlink to nothing), an absent previous generation and a complete canonical generation holding at least one family prove canonical authority, because publication moves the canonical directory only after `Prepared` is durable. An owned staging directory (a real directory holding only active-segment files of families the canonical generation holds, or nothing) whose every staged file is either byte for byte what a closed compaction of the canonical generation stages for its family -- the state the family's canonical files recover, encoded as compaction encodes it with their timestamp metadata -- or is the first bytes of that encoding ending inside its header or inside one of its records (a staging write stopped part-way, which is no complete snapshot of any state), and a regular `.manifest.next`, are then unpublished-attempt debris, provided no canonical key of a family whose file is staged has an empty member set or entry map (a family with no staged file is not compared, and nothing of it is removed): an open removes them under the replacement lock (or the locks its process already holds for the directory), a compaction under its claim, in the directory that lock or claim covers and only while the caller's path still names it, and the open reports `Recovered`. Any other staging keeps its error: a staged state records a deletion only by a fact's absence, so a canonical generation holding more than the staging -- one that lost a delete, or gained a write -- does not prove the staging redundant, and a staged file that is neither what compaction stages for the canonical state nor a write of it stopped inside a record (one the replay rejects, one cut on a record boundary or right after its header, which is a complete snapshot of fewer facts and records the rest as deleted, one recovering the same state in other bytes, or a headerless legacy file) may hold records nothing else holds, or proves nothing. The comparison is with what the current encoder writes, so debris an earlier release staged is removed only while the encoder writes the bytes that release wrote, and otherwise keeps its error. A path that cannot be read proves nothing, and keeps its error. A family's lone `.manifest.next` is not removed: another open instance of the family writes exactly that temporary during its first publication, and it keeps its error. Inspection stays read-only and keeps reporting such debris.

> Added 2026-10-02 (specs/015 FR-6): the last bullet. Without it, the residue of every failure or process exit before `Prepared` was classified as a competing authority, and the store could not be opened. Amended 2026-10-03 after specs/015's first review: what "missing" means, and which lock covers each removal. Amended again 2026-10-03 after its second review: "at least one family", the staging's containment, the family-instance condition, unreadable paths, and the lock an open holds when directory-level maintenance was in progress. Amended again 2026-10-03 after its third review: equality of states instead of containment, a staged file shorter than a header, and the family `.manifest.next` rule withdrawn. Amended again 2026-10-03 after its fourth review: byte equality with what compaction stages instead of equality of states, the short-file and empty-key conditions, and the directory the removal acts in. Amended again 2026-10-03 after its fifth review: a staged file stopped inside its header or a record of that encoding instead of any short file that holds nothing, and the encoder the comparison depends on. Amended again 2026-10-03 after its final review: the empty-key condition applies to the families whose files are staged, as the implementation has always applied it.

## Cleanup sequencing

Cleanup always follows authority confirmation. For closed generations, exact descriptor equality is required. For online replacement, the manifest verifies the immutable published prefix; current-V2 appends and rotations causally after that prefix are allowed. Previous artifacts still require exact equality. A missing cleanup target is already complete; a mismatching target is preserved and causes pending/error classification rather than deletion.

Recovery/open and the next explicit compaction retry pending cleanup. No timer or background task does so. Online replacement remains readable and writable while cleanup is pending.

## Current-format and legacy rules

- Replay accepts only current V2 plus a terminal tail already accepted by normal recovery rules.
- Compaction output is current V2 with the same family and timestamp semantics.
- Shallow recognition of a known older envelope returns `MigrationRequired` before artifacts change.
- Unknown/corrupt content returns `InvalidArtifact` unless it creates competing-authority ambiguity.
- Runtime maintenance never invokes the migration engine; frozen migration fixtures and outcomes remain byte-identical.

## Fault evidence

Test-only checkpoints cover staging create/write (before its first byte, and inside its bytes)/sync/validation; every manifest write/sync; old-to-previous and staging-to-canonical namespace operations; replacement reopen; phase rewrites; and each cleanup deletion. At every cut before `Prepared` is published, a new process must reopen the exact old state and remove the attempt's debris, with two exceptions that keep `AuthorityUndetermined` and every artifact: a cut at which a family's file is staged while that family holds a canonical key whose member set or entry map is empty (the classification above cannot prove that file redundant), and a process killed inside a staging write whose bytes stop exactly on a record boundary or right after the file header (a complete snapshot of fewer facts, which cannot be told from one recording deletes). A real kill stops a write at a page or page-cache folio boundary, not at any byte, so in a store whose staged records all have one length a fixed share of such kills stop on a record boundary (one in three for 96-byte records). At every later cut, it must select exact old or exact replacement state, or preserve all evidence and return `AuthorityUndetermined`. The last complete authority must remain present.

> Corrected 2026-10-02 (specs/015 FR-6): cuts before `Prepared` may no longer end in `AuthorityUndetermined`; their old state is provably the authority. Amended 2026-10-03 after specs/015's fifth review: the two exceptions, which the rule stated without them since its fourth review, and the cut inside a staging write's bytes. Amended again after its final review: the first exception is scoped to a staged family, and the second is reached at page or folio granularity.
