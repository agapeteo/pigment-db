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

An online attempt writes `Prepared` before releasing its initial snapshot gate. Its source descriptor is then a verified prefix of the authoritative WAL, which may append and rotate during staging. At cutover, under exclusive coordination, it atomically rewrites `Prepared` with the exact final inventory and `source_finalized = true`. Publication cannot move source artifacts until that durable rewrite succeeds. Recovery from either online `Prepared` form uses the canonical old WAL and normal current-format recovery; valid advancement beyond the prefix is not a contradiction.

## Closed compaction sequence

1. Atomically claim a directory with no same-process open leases.
2. Read-only resolve authority and capture every family, exact file bytes/descriptors, logical state, family, granularity, and last bucket.
3. Publish `Prepared` and build one current-V2 active segment per family in a unique same-parent staging directory.
4. Synchronize as requested; reopen three times in acceptance tests; compare complete state and metadata.
5. Re-read source entries/names/bytes. Any addition, removal, rename, length or same-length content change aborts publication with old authority intact.
6. Publish old directory as previous; durably write `PreviousPublished`.
7. Publish staging canonically; durably write `ReplacementPublished`.
8. Reopen/validate canonical replacement; durably write `CleanupPending`.
9. Delete only owned checksum-matching old artifacts; remove manifest last. Failure returns pending cleanup.

An empty directory is a read-only no-op. Repeating compaction is safe and preserves state.

## Classification without a trustworthy manifest

- If available complete evidence can identify more than one plausible authority, canonical authority is absent with a plausible prior/staging generation, or required phase evidence contradicts itself: return `AuthorityUndetermined` with every relevant path.
- If one complete authority is proven and malformed debris cannot represent a complete competitor: return `InvalidArtifact` for that debris.
- Never rename, delete, truncate, or synthesize evidence while returning either error.
- A valid main manifest controls recovery. A valid `.manifest.next` alone is an unpublished attempted revision and does not advance the durable phase.

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

Test-only checkpoints cover staging create/write/sync/validation; every manifest write/sync; old-to-previous and staging-to-canonical namespace operations; replacement reopen; phase rewrites; and each cleanup deletion. At every cut, a new process must select exact old or exact replacement state, or preserve all evidence and return `AuthorityUndetermined`. The last complete authority must remain present.
