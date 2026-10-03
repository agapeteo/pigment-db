//! Maintenance attempts that never published, and recovery steps that must be repeatable
//! (specs/015). Run on every operating system: the deletions here depend on how each filesystem
//! reports symlinks, directories and sharing.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::compaction::publication::{
    directory_artifact_paths, family_artifact_paths, MaintenanceArtifactPaths,
};
use crate::test_support::maintenance_fixtures::{
    active_name, assert_three_reopens, create_current_v2, create_segmented_v2, sealed_name,
    FixtureFamily,
};

const ALL_FAMILIES: [FixtureFamily; 3] = [
    FixtureFamily::KeyValue,
    FixtureFamily::KeySet,
    FixtureFamily::KeyMap,
];

/// One entry of a namespace snapshot. Unlike `snapshot_directory`, it records directories (an
/// empty staging directory is evidence too) and symlinks, by their target, without following them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Entry {
    Directory,
    File(Vec<u8>),
    Symlink(PathBuf),
}

pub(super) type Namespace = BTreeMap<PathBuf, Entry>;

pub(super) fn namespace(root: &Path) -> Namespace {
    fn visit(root: &Path, directory: &Path, snapshot: &mut Namespace) {
        let mut entries = std::fs::read_dir(directory)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            let kind = entry.file_type().unwrap();
            if kind.is_symlink() {
                snapshot.insert(relative, Entry::Symlink(std::fs::read_link(&path).unwrap()));
            } else if kind.is_dir() {
                snapshot.insert(relative, Entry::Directory);
                visit(root, &path, snapshot);
            } else {
                snapshot.insert(relative, Entry::File(std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot);
    snapshot
}

/// The namespace without the two lock files an open of `store` takes (specs/011): they record the
/// opener's process id, so they are the open's own state, not evidence it may change.
fn without_open_locks(mut snapshot: Namespace) -> Namespace {
    snapshot.remove(Path::new(".store.pigment-lock"));
    snapshot.remove(Path::new("store/.pigment-lock"));
    snapshot
}

/// A parent directory holding `store`, written by an earlier process and closed.
fn store_with(families: &[FixtureFamily], segmented: bool) -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    for family in families {
        if segmented {
            create_segmented_v2(&store_dir, *family);
        } else {
            create_current_v2(&store_dir, *family);
        }
    }
    (root, store_dir)
}

fn closed_paths(store_dir: &Path) -> MaintenanceArtifactPaths {
    directory_artifact_paths(store_dir).unwrap()
}

fn online_paths(store_dir: &Path, family: FixtureFamily) -> MaintenanceArtifactPaths {
    family_artifact_paths(&store_dir.join(active_name(family))).unwrap()
}

fn copy_files(source: &Path, destination: &Path) {
    std::fs::create_dir(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
    }
}

fn flip_middle_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn open_family(
    store_dir: &Path,
    family: FixtureFamily,
) -> Result<crate::RecoveryStatus, crate::RecoveryError> {
    match family {
        FixtureFamily::KeyValue => {
            crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir)
                .map(|outcome| outcome.status())
        }
        FixtureFamily::KeySet => crate::key_set_store::DurableKeySetStore::try_init_new(store_dir)
            .map(|outcome| outcome.status()),
        FixtureFamily::KeyMap => crate::key_map_store::DurableKeyMapStore::try_init_new(store_dir)
            .map(|outcome| outcome.status()),
    }
}

/// Opens `family` and requires the open to be refused with `expected`, leaving the parent
/// directory as it was apart from the open's own lock files.
fn assert_refused_unchanged(
    root: &Path,
    store_dir: &Path,
    family: FixtureFamily,
    label: &str,
    expected: impl Fn(&crate::RecoveryError) -> bool,
) {
    let before = without_open_locks(namespace(root));
    let result = open_family(store_dir, family);
    match &result {
        Err(error) if expected(error) => {}
        other => panic!("{label}: expected its current refusal, got {other:?}"),
    }
    assert_eq!(
        without_open_locks(namespace(root)),
        before,
        "{label}: a refused open changed the directory"
    );
}

fn undetermined(error: &crate::RecoveryError) -> bool {
    matches!(error, crate::RecoveryError::AuthorityUndetermined { .. })
}

fn invalid_at(path: PathBuf) -> impl Fn(&crate::RecoveryError) -> bool {
    move |error| matches!(error, crate::RecoveryError::InvalidArtifact { path: actual } if *actual == path)
}

/// States beside a closed compaction's artifacts that recovery cannot prove are an unpublished
/// attempt's: each keeps the error it returned before specs/015 and changes nothing.
#[test]
fn closed_debris_that_is_not_provably_unpublished_keeps_its_error_and_its_bytes() {
    let family = FixtureFamily::KeyValue;

    // The canonical directory is missing: staging could be the only copy.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    std::fs::remove_dir_all(&store_dir).unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "canonical missing",
        undetermined,
    );

    // The canonical directory does not validate.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    flip_middle_byte(&store_dir.join(active_name(family)));
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "canonical corrupt",
        undetermined,
    );

    // A previous generation exists: the canonical directory may have been moved.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    copy_files(&store_dir, &paths.previous);
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "previous present",
        undetermined,
    );

    // Staging holds a file compaction never writes.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    std::fs::write(paths.staging.join("foreign"), b"not written by compaction").unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "staging holds a foreign file",
        invalid_at(paths.staging.clone()),
    );

    // Staging holds the inner lock file, which the staging reopen never takes.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    std::fs::write(
        paths
            .staging
            .join(crate::maintenance_coordination::INNER_LOCK_NAME),
        b"",
    )
    .unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "staging holds a lock file",
        undetermined,
    );

    // The main manifest is corrupt: recovery cannot know what it said.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    std::fs::write(&paths.manifest, b"corrupt manifest").unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "corrupt main manifest",
        undetermined,
    );

    // Staging holds a family the canonical directory lacks.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    let other = root.path().join("other");
    std::fs::create_dir(&other).unwrap();
    create_current_v2(&other, FixtureFamily::KeySet);
    stage_one_file(
        &paths,
        FixtureFamily::KeySet,
        &compaction_staged_file(&other, FixtureFamily::KeySet),
    );
    std::fs::remove_dir_all(&other).unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "staging holds a family the canonical directory lacks",
        undetermined,
    );

    // The manifest temporary is a directory, not a file a publication creates.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    std::fs::create_dir(&paths.manifest_next).unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "manifest temporary is a directory",
        invalid_at(paths.manifest_next.clone()),
    );

    // Staging holds a directory named for the canonical family's active segment, with a file in
    // it: compaction writes only regular files into staging.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    std::fs::create_dir_all(paths.staging.join(active_name(family))).unwrap();
    std::fs::write(
        paths.staging.join(active_name(family)).join("kept"),
        b"not written by compaction",
    )
    .unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "staging holds a subdirectory named for an active segment",
        invalid_at(paths.staging.clone()),
    );

    // Staging holds a symlink named for the canonical family's active segment, to what a
    // compaction stages, elsewhere. The target's path is longer than a file header, so the link's
    // own length does not make it read as a short staged file.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    let target = root
        .path()
        .join("elsewhere-in-a-directory-whose-name-is-longer-than-a-staged-file-header");
    stage_what_compaction_stages(&store_dir, &target);
    std::fs::create_dir(&paths.staging).unwrap();
    #[cfg(unix)]
    let linked = std::os::unix::fs::symlink(
        target.join(active_name(family)),
        paths.staging.join(active_name(family)),
    );
    #[cfg(windows)]
    let linked = std::os::windows::fs::symlink_file(
        target.join(active_name(family)),
        paths.staging.join(active_name(family)),
    );
    match linked {
        Ok(()) => assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            "staging holds a symlink named for an active segment",
            invalid_at(paths.staging.clone()),
        ),
        Err(error) => eprintln!(
            "staging-entry-symlink control skipped: this platform refused the link: {error}"
        ),
    }

    // Staging holds a regular file named for a sealed segment of the canonical family: compaction
    // writes active segments only.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    std::fs::create_dir(&paths.staging).unwrap();
    std::fs::write(
        paths.staging.join(sealed_name(family, 0)),
        compaction_staged_file(&store_dir, family),
    )
    .unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "staging holds a sealed-segment name",
        invalid_at(paths.staging.clone()),
    );

    // The canonical directory holds no family, beside a lone manifest temporary: a compaction never
    // stages an empty directory, so the temporary is not provably its.
    let (root, store_dir) = store_with(&[], false);
    let paths = closed_paths(&store_dir);
    std::fs::write(&paths.manifest_next, b"an unpublished closed Prepared").unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "an empty canonical directory beside a lone temporary",
        invalid_at(paths.manifest_next.clone()),
    );

    // The main manifest path is a symlink to nothing: something is there, and it is not a
    // manifest, so the manifest is not absent.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    match symlink_to_nothing(root.path(), &paths.manifest) {
        Ok(()) => assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            "main manifest is a symlink to nothing",
            undetermined,
        ),
        Err(error) => {
            eprintln!("dangling-manifest control skipped: this platform refused the link: {error}")
        }
    }

    // The manifest temporary is a symlink to a regular file elsewhere: a publication creates a
    // regular file, so the link is not provably its, and neither the link nor its target may be
    // touched.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    match symlink_to_a_file(root.path(), &paths.manifest_next) {
        Ok(()) => assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            "manifest temporary is a symlink to a regular file",
            invalid_at(paths.manifest_next.clone()),
        ),
        Err(error) => {
            eprintln!(
                "temporary-as-symlink control skipped: this platform refused the link: {error}"
            )
        }
    }

    // Staging is a symlink to a directory holding a valid copy: neither the link nor its target
    // may be touched.
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    let target = root.path().join("elsewhere");
    stage_what_compaction_stages(&store_dir, &target);
    #[cfg(unix)]
    let linked = std::os::unix::fs::symlink(&target, &paths.staging);
    #[cfg(windows)]
    let linked = std::os::windows::fs::symlink_dir(&target, &paths.staging);
    match linked {
        Ok(()) => assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            "staging is a symlink",
            invalid_at(paths.staging.clone()),
        ),
        Err(error) => {
            eprintln!("staging-as-symlink control skipped: this platform refused the link: {error}")
        }
    }
}

/// States beside an online compaction's artifacts that recovery cannot prove are a lone
/// unpublished manifest temporary: each keeps the error it returned before specs/015.
#[test]
fn online_debris_that_is_not_a_lone_unpublished_temporary_keeps_its_error_and_its_bytes() {
    let family = FixtureFamily::KeyValue;

    let (root, store_dir) = store_with(&[family], false);
    let paths = online_paths(&store_dir, family);
    std::fs::write(&paths.manifest_next, b"unpublished").unwrap();
    std::fs::write(&paths.staging, b"staged").unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "temporary beside family staging",
        undetermined,
    );

    let (root, store_dir) = store_with(&[family], false);
    let paths = online_paths(&store_dir, family);
    std::fs::write(&paths.manifest_next, b"unpublished").unwrap();
    std::fs::create_dir(&paths.previous).unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "temporary beside a previous directory",
        undetermined,
    );

    let (root, store_dir) = store_with(&[family], false);
    let paths = online_paths(&store_dir, family);
    std::fs::write(&paths.manifest_next, b"unpublished").unwrap();
    flip_middle_byte(&store_dir.join(active_name(family)));
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "temporary beside a corrupt family",
        undetermined,
    );

    let (root, store_dir) = store_with(&[family], false);
    let paths = online_paths(&store_dir, family);
    std::fs::create_dir(&paths.manifest_next).unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "temporary is a directory",
        undetermined,
    );

    // The temporary is a symlink to a regular file elsewhere, which no publication creates.
    let (root, store_dir) = store_with(&[family], false);
    let paths = online_paths(&store_dir, family);
    match symlink_to_a_file(root.path(), &paths.manifest_next) {
        Ok(()) => assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            "temporary is a symlink to a regular file",
            undetermined,
        ),
        Err(error) => eprintln!(
            "family-temporary-as-symlink control skipped: this platform refused the link: {error}"
        ),
    }

    // The family manifest path is a symlink to nothing, so the family manifest is not absent.
    let (root, store_dir) = store_with(&[family], false);
    let paths = online_paths(&store_dir, family);
    std::fs::write(&paths.manifest_next, b"unpublished").unwrap();
    match symlink_to_nothing(root.path(), &paths.manifest) {
        Ok(()) => assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            "temporary beside a family manifest that is a symlink to nothing",
            undetermined,
        ),
        Err(error) => eprintln!(
            "dangling-family-manifest control skipped: this platform refused the link: {error}"
        ),
    }
}

/// Makes `link` a file symlink to a regular file under `root`, written for it.
fn symlink_to_a_file(root: &Path, link: &Path) -> std::io::Result<()> {
    let target = root.join("a-regular-file");
    std::fs::write(&target, b"an unpublished revision, elsewhere").unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(target, link)
    }
}

/// Makes `link` a file symlink to a path under `root` that does not exist.
fn symlink_to_nothing(root: &Path, link: &Path) -> std::io::Result<()> {
    let nowhere = root.join("elsewhere").join("manifest");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(nowhere, link)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(nowhere, link)
    }
}

/// The unit-test child that runs a closed compaction of the directory it is given.
const CLOSED_CHILD: &str = "compaction::recovery_tests::closed_compaction_checkpoint_child";

fn compact(store_dir: &Path) -> Result<crate::DirectoryCompactionOutcome, crate::CompactionError> {
    crate::compact_directory_in_place(store_dir, crate::ClosedCompactionOptions::default())
}

fn exists(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

/// FR-1: a compaction that fails validation removes its staging directory before it returns.
#[test]
fn a_compaction_that_fails_validation_removes_its_staging() {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        MAINTENANCE_CHILD_FAILED,
    };

    let (root, store_dir) = store_with(&ALL_FAMILIES, true);
    let paths = closed_paths(&store_dir);
    let before = namespace(root.path());
    let pause_dir = tempfile::tempdir().unwrap();
    let child = pause_maintenance_child(
        CLOSED_CHILD,
        &store_dir,
        pause_dir.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut: MaintenanceCut::StagingSync,
        },
    );
    // The staging generation loses one family's file, so its validation fails.
    std::fs::remove_file(paths.staging.join(active_name(FixtureFamily::KeyValue))).unwrap();
    assert_eq!(
        child.resume(),
        MAINTENANCE_CHILD_FAILED,
        "the compaction must fail its validation"
    );

    assert!(
        !exists(&paths.staging),
        "a compaction that failed validation left its staging directory"
    );
    assert_no_closed_debris(&paths, "after a failed validation");
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "the store's files changed"
    );
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
    compact(&store_dir).expect("a following compaction");
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
}

/// FR-1: so does one whose staging write fails. The guard is armed as soon as the staging
/// directory exists, so it covers the writes too (plan.md, "One guard from `create_dir` to
/// `Prepared`").
#[test]
fn a_compaction_whose_staging_write_fails_removes_its_staging() {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        MAINTENANCE_CHILD_FAILED,
    };

    let (root, store_dir) = store_with(&ALL_FAMILIES, true);
    let paths = closed_paths(&store_dir);
    let before = namespace(root.path());
    let pause_dir = tempfile::tempdir().unwrap();
    let child = pause_maintenance_child(
        CLOSED_CHILD,
        &store_dir,
        pause_dir.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut: MaintenanceCut::StagingCreate,
        },
    );
    // The empty staging directory gains a directory where each family's file is to be created,
    // so the first write fails.
    for family in ALL_FAMILIES {
        std::fs::create_dir(paths.staging.join(active_name(family))).unwrap();
    }
    assert_eq!(
        child.resume(),
        MAINTENANCE_CHILD_FAILED,
        "the compaction must fail its staging write"
    );

    assert!(
        !exists(&paths.staging),
        "a compaction whose staging write failed left its staging directory"
    );
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "the store's files changed"
    );
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
    compact(&store_dir).expect("a following compaction");
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
}

/// FR-1: so does one whose first manifest publication fails, or panics, before its rename.
#[test]
fn a_compaction_whose_first_manifest_publication_fails_or_panics_removes_its_staging() {
    use crate::compaction::publication::manifest_publication_faults::{inject, Fault};
    use crate::compaction::publication::ManifestPublishStage;

    let mut left = Vec::new();
    for fault in [Fault::Error, Fault::Panic] {
        let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], true);
        let paths = closed_paths(&store_dir);
        let _injection = inject(
            &paths.manifest_next,
            1,
            ManifestPublishStage::Written,
            fault,
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| compact(&store_dir)));
        match (fault, result) {
            (
                Fault::Error,
                Ok(Err(crate::CompactionError::Io {
                    operation: crate::CompactionOperation::WriteManifest,
                    ..
                })),
            ) => {}
            (Fault::Panic, Err(_)) => {}
            (fault, other) => panic!("{fault:?}: unexpected compaction result {other:?}"),
        }
        if exists(&paths.staging) {
            left.push(fault);
        }
    }
    assert!(
        left.is_empty(),
        "a compaction whose first manifest publication failed left its staging: {left:?}"
    );
}

/// FR-1: the staging is the attempt's own, and unpublished, as long as the main manifest is what
/// it was when staging was created, even when a manifest was already there.
#[test]
fn an_unpublished_staging_is_removed_beside_a_manifest_the_attempt_did_not_change() {
    let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let paths = closed_paths(&store_dir);
    std::fs::write(&paths.manifest, b"a manifest this attempt did not write").unwrap();
    let (prepared, unpublished) = super::prepare_closed_staging_guarded(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    assert!(prepared.paths.staging.is_dir());
    drop(unpublished);
    assert!(
        !exists(&paths.staging),
        "an unpublished staging beside an unchanged manifest was left"
    );
    assert_eq!(
        std::fs::read(&paths.manifest).unwrap(),
        b"a manifest this attempt did not write"
    );
}

/// Control for FR-1 (plan D1): a main manifest path that holds something other than a readable
/// manifest or nothing at all -- a directory, or a symlink to nothing -- leaves the staging, since
/// the guard cannot tell from it whether `Prepared` was published.
#[test]
fn a_main_manifest_that_is_neither_readable_nor_absent_leaves_the_staging() {
    let mut removed = Vec::new();

    let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let paths = closed_paths(&store_dir);
    std::fs::create_dir(&paths.manifest).unwrap();
    let (prepared, unpublished) = super::prepare_closed_staging_guarded(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    drop(unpublished);
    if !prepared.paths.staging.is_dir() {
        removed.push("a directory at the manifest path");
    }

    let (root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let paths = closed_paths(&store_dir);
    match symlink_to_nothing(root.path(), &paths.manifest) {
        Ok(()) => {
            let (prepared, unpublished) = super::prepare_closed_staging_guarded(
                &store_dir,
                crate::ClosedCompactionOptions::default(),
            )
            .unwrap();
            drop(unpublished);
            if !prepared.paths.staging.is_dir() {
                removed.push("a dangling symlink at the manifest path");
            }
        }
        Err(error) => eprintln!(
            "dangling-manifest-symlink control skipped: this platform refused the link: {error}"
        ),
    }
    assert!(
        removed.is_empty(),
        "the guard removed staging beside a manifest it could not read: {removed:?}"
    );
}

/// Control for FR-1: once the main manifest has changed (`Prepared` renamed into place), the
/// staging belongs to `Prepared` recovery, which discards it at the next open.
#[test]
fn a_staging_named_by_a_published_prepared_is_left_to_its_recovery() {
    use crate::compaction::publication::manifest_publication_faults::{inject, Fault};
    use crate::compaction::publication::ManifestPublishStage;

    let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let paths = closed_paths(&store_dir);
    let (prepared, unpublished) = super::prepare_closed_staging_guarded(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    std::fs::write(&paths.manifest, b"Prepared, renamed into place").unwrap();
    drop(unpublished);
    assert!(
        prepared.paths.staging.is_dir(),
        "a published staging was removed"
    );

    // The same when a manifest was already there and `Prepared` was renamed over it: the guard
    // compares the manifest's bytes, not whether one exists (plan.md, Decisions).
    let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let paths = closed_paths(&store_dir);
    std::fs::write(&paths.manifest, b"left by earlier maintenance").unwrap();
    let (prepared, unpublished) = super::prepare_closed_staging_guarded(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    std::fs::write(&paths.manifest, b"Prepared, renamed over it").unwrap();
    drop(unpublished);
    assert!(
        prepared.paths.staging.is_dir(),
        "a staging named by a Prepared renamed over an earlier manifest was removed"
    );

    // The same through a compaction whose `Prepared` rename succeeded and whose next step failed.
    let (root, store_dir) = store_with(&[FixtureFamily::KeyValue], true);
    let paths = closed_paths(&store_dir);
    let before = namespace(root.path());
    {
        let _injection = inject(
            &paths.manifest_next,
            1,
            ManifestPublishStage::Renamed,
            Fault::Error,
        );
        assert!(compact(&store_dir).is_err());
    }
    assert!(exists(&paths.manifest), "Prepared must be in place");
    assert!(paths.staging.is_dir(), "a published staging was removed");
    assert_eq!(
        open_family(&store_dir, FixtureFamily::KeyValue).unwrap(),
        crate::RecoveryStatus::Recovered
    );
    assert_no_closed_debris(&paths, "after Prepared recovery");
    assert_eq!(without_open_locks(namespace(root.path())), before);
    assert_three_reopens(&store_dir, FixtureFamily::KeyValue);
}

/// Pauses a closed compaction of `store_dir` once its staging has been built and validated and
/// its inner lock retired, applies `change` to the store, and resumes it. The compaction then
/// finds its source changed when it revalidates it, and fails before `Prepared`.
fn change_the_source_under_the_claim(store_dir: &Path, change: impl FnOnce()) {
    change_the_source_under_the_claim_at(
        store_dir,
        crate::test_support::fault_checkpoint::MaintenanceCut::StagingValidate,
        change,
    );
}

/// `change_the_source_under_the_claim`, pausing at `cut` (one at which the staging directory
/// exists) instead.
fn change_the_source_under_the_claim_at(
    store_dir: &Path,
    cut: crate::test_support::fault_checkpoint::MaintenanceCut,
    change: impl FnOnce(),
) {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceFaultPoint, MaintenancePhase, MAINTENANCE_CHILD_FAILED,
    };
    let pause_dir = tempfile::tempdir().unwrap();
    let child = pause_maintenance_child(
        CLOSED_CHILD,
        store_dir,
        pause_dir.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut,
        },
    );
    assert!(closed_paths(store_dir).staging.is_dir(), "staging exists");
    change();
    assert_eq!(
        child.resume(),
        MAINTENANCE_CHILD_FAILED,
        "the compaction must refuse a source that changed after its capture"
    );
}

/// FR-1 as amended by the review: a compaction whose source no longer validates when it is
/// revalidated -- damaged under the claim by the medium or by a writer outside the exclusion --
/// leaves its staging, which may be the only complete copy of the captured state, and the next
/// open names both, as before specs/015.
#[test]
fn a_compaction_whose_source_was_damaged_under_its_claim_keeps_its_staging() {
    let family = FixtureFamily::KeyValue;
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    change_the_source_under_the_claim(&store_dir, || {
        flip_middle_byte(&store_dir.join(active_name(family)));
    });
    assert!(
        paths.staging.is_dir(),
        "a compaction whose source was damaged under its claim removed its staging, the only \
         intact copy of the captured state"
    );
    let staging = paths.staging.clone();
    let canonical = store_dir.clone();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "source damaged under the claim",
        move |error| {
            matches!(
                error,
                crate::RecoveryError::AuthorityUndetermined {
                    active_path: Some(active),
                    recovery_path: Some(recovery),
                } if *active == canonical && *recovery == staging
            )
        },
    );
}

/// The amended FR-1, and FR-3 after the third review: a source replaced by another valid
/// generation is not the one the attempt captured either, so the attempt leaves its staging. The
/// next open finds a complete canonical generation holding one more key than the staging, which is
/// also what a canonical directory that lost a delete looks like, so it cannot prove the staging
/// redundant: it keeps the error it returned at `1eb9de5`, and both copies.
#[test]
fn a_compaction_whose_source_was_replaced_by_another_valid_generation_keeps_its_staging_and_its_error(
) {
    let family = FixtureFamily::KeyValue;
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    // Another valid generation of the family, holding one more key.
    let other = root.path().join("other");
    std::fs::create_dir(&other).unwrap();
    create_current_v2(&other, family);
    crate::key_value_store::DurableKeyValueStore::try_init_new(&other)
        .unwrap()
        .into_store()
        .try_put(b"replaced".to_vec(), b"generation".to_vec())
        .unwrap();
    let replacement = std::fs::read(other.join(active_name(family))).unwrap();
    std::fs::remove_dir_all(&other).unwrap();
    change_the_source_under_the_claim(&store_dir, || {
        std::fs::write(store_dir.join(active_name(family)), &replacement).unwrap();
    });
    assert!(
        paths.staging.is_dir(),
        "the attempt must leave a staging it cannot prove redundant"
    );
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "a source replaced by a valid generation holding one more key",
        undetermined,
    );
    assert_eq!(
        std::fs::read(store_dir.join(active_name(family))).unwrap(),
        replacement
    );
}

/// A closed `Prepared` manifest for `store_dir`'s real staging, as `publish_closed_prepared`
/// builds it.
fn closed_prepared_manifest(
    store_dir: &Path,
) -> (
    MaintenanceArtifactPaths,
    crate::compaction::manifest::CompactionManifest,
) {
    use crate::compaction::manifest::{
        CompactionManifest, ManifestMode, ManifestPhase, ManifestScope,
    };
    let prepared =
        super::prepare_closed_staging(store_dir, crate::ClosedCompactionOptions::default())
            .unwrap();
    let manifest = CompactionManifest {
        operation_id: *b"015-unpublished.",
        mode: ManifestMode::ClosedDirectory,
        scope: ManifestScope::Directory,
        phase: ManifestPhase::Prepared,
        source_finalized: true,
        durability: crate::DurabilityPolicy::Buffered,
        source_inventory: prepared.capture.inventory.clone(),
        staging_location: PathBuf::from(prepared.paths.staging.file_name().unwrap()),
        previous_location: PathBuf::from(prepared.paths.previous.file_name().unwrap()),
        replacement_inventory: prepared.replacement_inventory.clone(),
    };
    (prepared.paths, manifest)
}

/// FR-2: a manifest publication that fails or panics before its rename removes the temporary it
/// created, as a first publication and as a rewrite, and leaves the main manifest as it was.
#[test]
fn a_failed_manifest_publication_removes_the_temporary_it_created() {
    use crate::compaction::manifest::ManifestPhase;
    use crate::compaction::publication::{
        publish_manifest_buffered, publish_manifest_buffered_with_checkpoint, ManifestPublishStage,
    };

    let mut left = Vec::new();
    for rewrite in [false, true] {
        for stage in [
            ManifestPublishStage::Created,
            ManifestPublishStage::Written,
            ManifestPublishStage::Flushed,
        ] {
            for panics in [false, true] {
                let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
                let (paths, prepared) = closed_prepared_manifest(&store_dir);
                let mut next = prepared.clone();
                if rewrite {
                    publish_manifest_buffered(&paths, &prepared).unwrap();
                    next.phase = ManifestPhase::PreviousPublished;
                }
                let main_before = std::fs::read(&paths.manifest).ok();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    publish_manifest_buffered_with_checkpoint(&paths, &next, |reached| {
                        if reached == stage {
                            if panics {
                                panic!("injected panic at {reached:?}");
                            }
                            return Err(std::io::Error::other("injected failure"));
                        }
                        Ok(())
                    })
                }));
                assert!(
                    matches!(result, Ok(Err(_)) | Err(_)),
                    "the publication must fail"
                );
                assert_eq!(std::fs::read(&paths.manifest).ok(), main_before);
                if exists(&paths.manifest_next) {
                    left.push((rewrite, stage, panics));
                }
            }
        }
    }
    assert!(
        left.is_empty(),
        "failed publications (rewrite, stage, panicked) left their temporary: {left:?}"
    );
}

/// Control for FR-2: a temporary the publication did not create is not its to remove.
#[test]
fn a_publication_leaves_a_temporary_it_did_not_create() {
    use crate::compaction::publication::publish_manifest_buffered;

    let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let (paths, prepared) = closed_prepared_manifest(&store_dir);
    std::fs::write(&paths.manifest_next, b"another publication's temporary").unwrap();
    let error = publish_manifest_buffered(&paths, &prepared).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        std::fs::read(&paths.manifest_next).unwrap(),
        b"another publication's temporary"
    );
    assert!(!exists(&paths.manifest));
}

/// FR-2: a publication whose rename fails removes its temporary too. A directory holding a file
/// at the main manifest path makes the rename fail, and is left as it was.
#[test]
fn a_publication_whose_rename_fails_removes_its_temporary() {
    use crate::compaction::publication::publish_manifest_buffered;

    let (_root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let (paths, prepared) = closed_prepared_manifest(&store_dir);
    std::fs::create_dir(&paths.manifest).unwrap();
    std::fs::write(paths.manifest.join("kept"), b"in the way").unwrap();
    let error = publish_manifest_buffered(&paths, &prepared).unwrap_err();
    assert!(
        !exists(&paths.manifest_next),
        "a failed rename left its temporary ({error:?})"
    );
    assert_eq!(
        std::fs::read(paths.manifest.join("kept")).unwrap(),
        b"in the way"
    );
}

/// FR-2 through a closed compaction: a failed first publication leaves no maintenance artifact,
/// the store reopens with its exact state, and a following compaction succeeds.
#[test]
fn a_compaction_whose_first_manifest_publication_fails_leaves_no_temporary() {
    use crate::compaction::publication::manifest_publication_faults::{inject, Fault};
    use crate::compaction::publication::ManifestPublishStage;

    let mut left = Vec::new();
    for (stage, fault) in [
        (ManifestPublishStage::Created, Fault::Error),
        (ManifestPublishStage::Written, Fault::Error),
        (ManifestPublishStage::Flushed, Fault::Error),
        (ManifestPublishStage::Written, Fault::Panic),
    ] {
        let (root, store_dir) = store_with(&ALL_FAMILIES, true);
        let paths = closed_paths(&store_dir);
        let before = namespace(root.path());
        {
            let _injection = inject(&paths.manifest_next, 1, stage, fault);
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| compact(&store_dir)));
            assert!(
                matches!(result, Ok(Err(_)) | Err(_)),
                "{stage:?} {fault:?}: the compaction must fail"
            );
        }
        if exists(&paths.manifest_next) {
            left.push((stage, fault));
            continue;
        }
        assert_no_closed_debris(&paths, "after a failed first publication");
        assert_eq!(without_open_locks(namespace(root.path())), before);
        assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
        compact(&store_dir).expect("a following compaction");
        assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
    }
    assert!(
        left.is_empty(),
        "compactions whose first manifest publication failed left its temporary: {left:?}"
    );
}

/// The cuts a closed compaction passes before its `Prepared` manifest is renamed into place.
const PRE_PREPARED_CUTS: [crate::test_support::fault_checkpoint::MaintenanceCut; 7] = {
    use crate::test_support::fault_checkpoint::MaintenanceCut;
    [
        MaintenanceCut::StagingCreate,
        MaintenanceCut::StagingWrite,
        MaintenanceCut::StagingWriteTorn,
        MaintenanceCut::StagingSync,
        MaintenanceCut::StagingValidate,
        MaintenanceCut::ManifestWrite,
        MaintenanceCut::ManifestSync,
    ]
};

/// Runs a closed compaction of a fresh `families` store in a child process killed at
/// (`Prepared`, `cut`), and returns the parent, the store and the parent's namespace from before.
fn killed_before_prepared(
    families: &[FixtureFamily],
    cut: crate::test_support::fault_checkpoint::MaintenanceCut,
) -> (tempfile::TempDir, PathBuf, Namespace) {
    let (root, store_dir) = store_with(families, true);
    kill_before_prepared(root, store_dir, cut, &format!("{families:?}"))
}

/// Runs a closed compaction of `store_dir`, under `root`, in a child process killed at
/// (`Prepared`, `cut`), and returns them with the parent's namespace from before.
fn kill_before_prepared(
    root: tempfile::TempDir,
    store_dir: PathBuf,
    cut: crate::test_support::fault_checkpoint::MaintenanceCut,
    families: &str,
) -> (tempfile::TempDir, PathBuf, Namespace) {
    use crate::test_support::fault_checkpoint::{
        run_maintenance_checkpoint_child_with_evidence_root, MaintenanceFaultPoint,
        MaintenancePhase,
    };
    let before = namespace(root.path());
    run_maintenance_checkpoint_child_with_evidence_root(
        CLOSED_CHILD,
        &store_dir,
        root.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut,
        },
    );
    let paths = closed_paths(&store_dir);
    assert!(
        exists(&paths.staging) || exists(&paths.manifest_next),
        "{families} {cut:?}: the kill must leave the attempt's evidence"
    );
    assert!(!exists(&paths.manifest) && !exists(&paths.previous));
    (root, store_dir, before)
}

/// FR-3 and FR-7: a process killed at any cut before `Prepared`, for each family and for all
/// three together, reopens `Recovered` with its exact state and no maintenance artifact, and a
/// following compaction succeeds. Until an open runs, `inspect_storage` keeps reporting the
/// debris and changes nothing.
#[test]
fn every_pre_prepared_cut_reopens_the_untouched_source() {
    let fixtures: [&[FixtureFamily]; 4] = [
        &[FixtureFamily::KeyValue],
        &[FixtureFamily::KeySet],
        &[FixtureFamily::KeyMap],
        &ALL_FAMILIES,
    ];
    let mut refused = Vec::new();
    for families in fixtures {
        for cut in PRE_PREPARED_CUTS {
            let (root, store_dir, before) = killed_before_prepared(families, cut);
            let paths = closed_paths(&store_dir);

            let debris = namespace(root.path());
            assert!(
                crate::inspect_storage(&store_dir).is_err(),
                "{families:?} {cut:?}: inspection must keep reporting the debris"
            );
            assert_eq!(namespace(root.path()), debris, "inspection changed storage");

            match open_family(&store_dir, families[0]) {
                Ok(crate::RecoveryStatus::Recovered) => {}
                other => {
                    refused.push(format!("{families:?} {cut:?}: {other:?}"));
                    continue;
                }
            }
            assert_no_closed_debris(&paths, &format!("{families:?} {cut:?}"));
            assert_eq!(
                without_open_locks(namespace(root.path())),
                before,
                "{families:?} {cut:?}: the store's files changed"
            );
            assert_reopens_exactly(&store_dir, families);
            compact(&store_dir)
                .unwrap_or_else(|error| panic!("{families:?} {cut:?}: compaction: {error:?}"));
            assert_reopens_exactly(&store_dir, families);
        }
    }
    assert!(
        refused.is_empty(),
        "stores killed before Prepared did not reopen Recovered:\n{}",
        refused.join("\n")
    );
}

/// FR-3 at the start of a compaction: debris left by a killed attempt is removed by the next
/// compaction itself, with no open before it.
#[test]
fn a_compaction_started_over_unpublished_debris_removes_it_and_succeeds() {
    use crate::test_support::fault_checkpoint::MaintenanceCut;

    let mut refused = Vec::new();
    for cut in [
        MaintenanceCut::StagingWrite,
        MaintenanceCut::StagingWriteTorn,
        MaintenanceCut::ManifestSync,
    ] {
        let (_root, store_dir, _before) = killed_before_prepared(&ALL_FAMILIES, cut);
        match compact(&store_dir) {
            Ok(_) => {}
            Err(error) => {
                refused.push(format!("{cut:?}: {error:?}"));
                continue;
            }
        }
        assert_no_closed_debris(&closed_paths(&store_dir), &format!("{cut:?}"));
        assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
    }
    assert!(
        refused.is_empty(),
        "compactions started over unpublished debris failed:\n{}",
        refused.join("\n")
    );
}

/// FR-3: a closed manifest temporary with no staging beside it (what a publication's own cleanup
/// could not remove) is removed at open in the same way.
#[test]
fn a_lone_closed_manifest_temporary_is_removed_at_open() {
    let (root, store_dir) = store_with(&ALL_FAMILIES, true);
    let before = namespace(root.path());
    let paths = closed_paths(&store_dir);
    std::fs::write(&paths.manifest_next, b"an unpublished closed Prepared").unwrap();
    let opened = open_family(&store_dir, FixtureFamily::KeySet);
    assert!(
        matches!(opened, Ok(crate::RecoveryStatus::Recovered)),
        "a lone closed manifest temporary kept its store closed: {opened:?}"
    );
    assert_no_closed_debris(&paths, "after a lone temporary");
    assert_eq!(without_open_locks(namespace(root.path())), before);
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
}

/// FR-7 as amended: debris recovery has proven to be an unpublished attempt's, but cannot remove,
/// fails the open with the removal's I/O error, where 1eb9de5 returned `AuthorityUndetermined`.
/// Staging goes first (FR-3), so a staging directory whose files cannot be unlinked leaves the
/// temporary, and everything else, as it was.
#[cfg(unix)]
#[test]
fn debris_that_cannot_be_removed_fails_the_open_with_the_removal_error() {
    use std::os::unix::fs::PermissionsExt;

    /// Makes a directory read-only, and writable again when dropped, so the test's temporary
    /// directory can be removed whatever the test does.
    struct ReadOnly<'a>(&'a Path);
    impl Drop for ReadOnly<'_> {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    let family = FixtureFamily::KeyValue;
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    std::fs::write(&paths.manifest_next, b"an unpublished closed Prepared").unwrap();
    let read_only = ReadOnly(&paths.staging);
    std::fs::set_permissions(read_only.0, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = paths.staging.join("probe");
    if std::fs::write(&probe, b"").is_ok() {
        std::fs::remove_file(&probe).unwrap();
        eprintln!("skipped: this process may write into a read-only directory");
        return;
    }
    let before = without_open_locks(namespace(root.path()));
    let result = open_family(&store_dir, family);
    match &result {
        Err(crate::RecoveryError::Io { path, source, .. })
            if *path == paths.staging && source.kind() == std::io::ErrorKind::PermissionDenied => {}
        other => panic!("expected the staging removal's error, got {other:?}"),
    }
    assert!(
        exists(&paths.manifest_next),
        "the temporary was removed before staging"
    );
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "an open that could not remove the debris changed the directory"
    );
    drop(read_only);
    assert_eq!(
        open_family(&store_dir, family).unwrap(),
        crate::RecoveryStatus::Recovered,
        "once the debris can be removed, the next open removes it"
    );
    assert_no_closed_debris(&paths, "after the next open");
}

/// FR-3, online, withdrawn by the third review: a lone family manifest temporary, with no family
/// manifest, staging or previous directory beside a valid family, keeps the error it returned at
/// `1eb9de5`, and the open changes nothing -- alone, beside another open instance of the family,
/// and beside an open instance of another family. Another open instance's live first publication
/// writes exactly that temporary, in this process or in another where locks are not supported,
/// and nothing lets recovery tell it from debris.
#[test]
fn a_lone_online_manifest_temporary_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        let other_family = match family {
            FixtureFamily::KeyValue => FixtureFamily::KeySet,
            FixtureFamily::KeySet => FixtureFamily::KeyMap,
            FixtureFamily::KeyMap => FixtureFamily::KeyValue,
        };
        for held in [None, Some(family), Some(other_family)] {
            let (root, store_dir) = store_with(&ALL_FAMILIES, false);
            let held_open = held.map(|held| hold_open(&store_dir, held));
            let paths = online_paths(&store_dir, family);
            std::fs::write(&paths.manifest_next, b"an unpublished online Prepared").unwrap();
            assert_refused_unchanged(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}, with {held:?} held open: a lone online temporary"),
                undetermined,
            );
            drop(held_open);
        }
    }
}

/// A closed rollback killed part-way: a `PreviousPublished` attempt whose staging no longer
/// validates, recovered by a compaction in a child process that dies at `cut` -- once the previous
/// generation is back at the canonical path (`RollbackRestore`), or once staging is gone too
/// (`RollbackCleanup`). Returns the parent, the store and the store's own namespace from before
/// the attempt.
fn rollback_killed_at(
    family: FixtureFamily,
    cut: crate::test_support::fault_checkpoint::MaintenanceCut,
) -> (tempfile::TempDir, PathBuf, Namespace) {
    use crate::compaction::publication::{
        publish_closed_prepared, publish_closed_previous_with_checkpoint,
    };
    use crate::test_support::fault_checkpoint::{
        run_maintenance_checkpoint_child_with_evidence_root, MaintenanceCut, MaintenanceFaultPoint,
        MaintenancePhase,
    };

    let (root, store_dir) = store_with(&[family], true);
    let source = namespace(&store_dir);
    let prepared =
        super::prepare_closed_staging(&store_dir, crate::ClosedCompactionOptions::default())
            .unwrap();
    super::validate_closed_staging(&prepared).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    // The replacement no longer validates, so recovery restores the previous generation.
    flip_middle_byte(&prepared.paths.staging.join(active_name(family)));
    run_maintenance_checkpoint_child_with_evidence_root(
        CLOSED_CHILD,
        &store_dir,
        root.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::PreviousPublished,
            cut,
        },
    );
    let paths = closed_paths(&store_dir);
    assert!(store_dir.is_dir() && exists(&paths.manifest) && !exists(&paths.previous));
    assert_eq!(
        paths.staging.is_dir(),
        cut == MaintenanceCut::RollbackRestore,
        "{cut:?}"
    );
    (root, store_dir, source)
}

fn rollback_killed_after_its_restore(
    family: FixtureFamily,
) -> (tempfile::TempDir, PathBuf, Namespace) {
    rollback_killed_at(
        family,
        crate::test_support::fault_checkpoint::MaintenanceCut::RollbackRestore,
    )
}

/// FR-4: a closed rollback interrupted after its restore rename -- before or after it removed
/// staging -- completes when it is run again.
#[test]
fn an_interrupted_closed_rollback_completes_when_run_again() {
    use crate::test_support::fault_checkpoint::MaintenanceCut;

    let mut refused = Vec::new();
    for (family, cut) in ALL_FAMILIES.into_iter().flat_map(|family| {
        [
            MaintenanceCut::RollbackRestore,
            MaintenanceCut::RollbackCleanup,
        ]
        .map(|cut| (family, cut))
    }) {
        let (_root, store_dir, source) = rollback_killed_at(family, cut);
        match open_family(&store_dir, family) {
            Ok(crate::RecoveryStatus::Recovered) => {}
            other => {
                refused.push(format!("{family:?} {cut:?}: {other:?}"));
                continue;
            }
        }
        assert_no_closed_debris(&closed_paths(&store_dir), &format!("{family:?}"));
        let mut reopened = namespace(&store_dir);
        reopened.remove(Path::new(crate::maintenance_coordination::INNER_LOCK_NAME));
        assert_eq!(reopened, source, "{family:?}: the source changed");
        assert_three_reopens(&store_dir, family);
        compact(&store_dir).expect("a following compaction");
        assert_three_reopens(&store_dir, family);
    }
    assert!(
        refused.is_empty(),
        "interrupted rollbacks did not complete:\n{}",
        refused.join("\n")
    );
}

/// Control for FR-4: the same interrupted rollback beside evidence it cannot account for keeps
/// its error and its bytes.
#[test]
fn an_interrupted_rollback_beside_unexplained_evidence_keeps_its_error_and_its_bytes() {
    let family = FixtureFamily::KeyValue;

    let (root, store_dir, _) = rollback_killed_after_its_restore(family);
    copy_files(&store_dir, &closed_paths(&store_dir).previous);
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "a previous generation beside the restored source",
        undetermined,
    );

    let (root, store_dir, _) = rollback_killed_after_its_restore(family);
    let previous = closed_paths(&store_dir).previous;
    copy_files(&store_dir, &previous);
    flip_middle_byte(&previous.join(active_name(family)));
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "a damaged previous generation beside the restored source",
        undetermined,
    );

    let (root, store_dir, _) = rollback_killed_after_its_restore(family);
    let staging = closed_paths(&store_dir).staging;
    std::fs::remove_dir_all(&staging).unwrap();
    std::fs::write(&staging, b"not a staging directory").unwrap();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "staging that is a file",
        undetermined,
    );

    let (root, store_dir, _) = rollback_killed_after_its_restore(family);
    flip_middle_byte(&store_dir.join(active_name(family)));
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "a canonical directory that no longer matches the source",
        undetermined,
    );
}

fn assert_no_closed_debris(paths: &MaintenanceArtifactPaths, label: &str) {
    for path in [
        &paths.staging,
        &paths.previous,
        &paths.manifest,
        &paths.manifest_next,
    ] {
        assert!(
            std::fs::symlink_metadata(path).is_err(),
            "{label}: {} remains",
            path.display()
        );
    }
}

fn assert_reopens_exactly(store_dir: &Path, families: &[FixtureFamily]) {
    for family in families {
        assert_three_reopens(store_dir, *family);
    }
}

/// Opens `family` and keeps that instance open until the returned value is dropped.
fn hold_open(store_dir: &Path, family: FixtureFamily) -> Box<dyn std::any::Any> {
    match family {
        FixtureFamily::KeyValue => Box::new(
            crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir)
                .unwrap()
                .into_store(),
        ),
        FixtureFamily::KeySet => Box::new(
            crate::key_set_store::DurableKeySetStore::try_init_new(store_dir)
                .unwrap()
                .into_store(),
        ),
        FixtureFamily::KeyMap => Box::new(
            crate::key_map_store::DurableKeyMapStore::try_init_new(store_dir)
                .unwrap()
                .into_store(),
        ),
    }
}

/// Opens `family` in `directory` and adds one fact the fixture does not hold: for key/value and
/// key/sorted-map, a new value for the fixture's key or entry when `change_existing`, otherwise a
/// new key, member or entry.
fn add_a_fact(directory: &Path, family: FixtureFamily, change_existing: bool) {
    match family {
        FixtureFamily::KeyValue => {
            let store = crate::key_value_store::DurableKeyValueStore::try_init_new(directory)
                .unwrap()
                .into_store();
            if change_existing {
                store.put(b"alpha".to_vec(), b"changed".to_vec());
            } else {
                store.put(b"gamma".to_vec(), b"three".to_vec());
            }
        }
        FixtureFamily::KeySet => {
            let store = crate::key_set_store::DurableKeySetStore::try_init_new(directory)
                .unwrap()
                .into_store();
            store.append(b"group".to_vec(), b"green".to_vec());
        }
        FixtureFamily::KeyMap => {
            let store = crate::key_map_store::DurableKeyMapStore::try_init_new(directory)
                .unwrap()
                .into_store();
            if change_existing {
                store.put(
                    b"book".to_vec(),
                    crate::model::SearchKey::from(1),
                    b"changed".to_vec(),
                );
            } else {
                store.put(
                    b"book".to_vec(),
                    crate::model::SearchKey::from(9),
                    b"nine".to_vec(),
                );
            }
        }
    }
}

/// A staging directory holding `active`, as the family's only staged file.
fn stage_one_file(paths: &MaintenanceArtifactPaths, family: FixtureFamily, active: &[u8]) {
    std::fs::create_dir(&paths.staging).unwrap();
    std::fs::write(paths.staging.join(active_name(family)), active).unwrap();
}

/// What a closed compaction of `store_dir` stages: each family's active file, read after a
/// compaction of a copy of the directory renamed that staging into place.
fn what_compaction_stages(store_dir: &Path) -> Vec<(&'static std::ffi::OsStr, Vec<u8>)> {
    let scratch = tempfile::tempdir().unwrap();
    let copy = scratch.path().join("store");
    copy_files(store_dir, &copy);
    compact(&copy).unwrap();
    ALL_FAMILIES
        .iter()
        .filter_map(|family| {
            std::fs::read(copy.join(active_name(*family)))
                .ok()
                .map(|bytes| (active_name(*family), bytes))
        })
        .collect()
}

/// `family`'s file in what a closed compaction of `store_dir` stages.
fn compaction_staged_file(store_dir: &Path, family: FixtureFamily) -> Vec<u8> {
    what_compaction_stages(store_dir)
        .into_iter()
        .find(|(name, _)| *name == active_name(family))
        .map(|(_, bytes)| bytes)
        .unwrap()
}

/// Creates `staging` holding what a closed compaction of `store_dir` stages.
fn stage_what_compaction_stages(store_dir: &Path, staging: &Path) {
    let staged = what_compaction_stages(store_dir);
    std::fs::create_dir(staging).unwrap();
    for (name, bytes) in staged {
        std::fs::write(staging.join(name), bytes).unwrap();
    }
}

/// specs/015 FR-3 after the second review: a staging directory that holds a fact the canonical
/// directory lacks -- a key, a value, a set member or a map entry -- may be the only copy of it,
/// whatever changed the canonical directory. The open keeps the error it returned at `1eb9de5`
/// and changes nothing.
#[test]
fn a_staging_holding_a_fact_the_canonical_directory_lacks_keeps_its_error_and_its_bytes() {
    let mut cases = ALL_FAMILIES
        .into_iter()
        .map(|family| (family, false))
        .collect::<Vec<_>>();
    cases.push((FixtureFamily::KeyValue, true));
    cases.push((FixtureFamily::KeyMap, true));
    for (family, change_existing) in cases {
        let (root, store_dir) = store_with(&[family], false);
        let paths = closed_paths(&store_dir);
        let other = root.path().join("other");
        copy_files(&store_dir, &other);
        add_a_fact(&other, family, change_existing);
        let staged = compaction_staged_file(&other, family);
        std::fs::remove_dir_all(&other).unwrap();
        stage_one_file(&paths, family, &staged);
        assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            &format!("{family:?} (changed value: {change_existing}): staging holds a fact"),
            undetermined,
        );
    }
}

/// Writes `family`'s fixture fact and a second one into `directory`, then deletes the second, so
/// that the active file's last record is a delete. Returns the active file's bytes from before the
/// delete and its length after it.
fn store_whose_last_record_is_a_delete(directory: &Path, family: FixtureFamily) -> (Vec<u8>, u64) {
    let active = directory.join(active_name(family));
    match family {
        FixtureFamily::KeyValue => {
            let store = crate::key_value_store::DurableKeyValueStore::try_init_new(directory)
                .unwrap()
                .into_store();
            store.put(b"alpha".to_vec(), b"one".to_vec());
            store.put(b"beta".to_vec(), b"two".to_vec());
        }
        FixtureFamily::KeySet => {
            let store = crate::key_set_store::DurableKeySetStore::try_init_new(directory)
                .unwrap()
                .into_store();
            store.append(b"group".to_vec(), b"red".to_vec());
            store.append(b"group".to_vec(), b"blue".to_vec());
        }
        FixtureFamily::KeyMap => {
            let store = crate::key_map_store::DurableKeyMapStore::try_init_new(directory)
                .unwrap()
                .into_store();
            store.put(
                b"book".to_vec(),
                crate::model::SearchKey::from(1),
                b"one".to_vec(),
            );
            store.put(
                b"book".to_vec(),
                crate::model::SearchKey::from(2),
                b"two".to_vec(),
            );
        }
    }
    let before_delete = std::fs::read(&active).unwrap();
    match family {
        FixtureFamily::KeyValue => {
            crate::key_value_store::DurableKeyValueStore::try_init_new(directory)
                .unwrap()
                .into_store()
                .remove(b"beta");
        }
        FixtureFamily::KeySet => {
            crate::key_set_store::DurableKeySetStore::try_init_new(directory)
                .unwrap()
                .into_store()
                .remove_from_set(b"group".to_vec(), b"blue".to_vec());
        }
        FixtureFamily::KeyMap => {
            crate::key_map_store::DurableKeyMapStore::try_init_new(directory)
                .unwrap()
                .into_store()
                .remove_from_sorted_map(b"book".to_vec(), crate::model::SearchKey::from(2));
        }
    }
    crate::test_support::maintenance_fixtures::forget_inner_lock(directory);
    let after_delete = std::fs::metadata(&active).unwrap().len();
    assert!(
        after_delete > before_delete.len() as u64 + 2,
        "{family:?}: the delete must append a record"
    );
    (before_delete, after_delete)
}

/// How the canonical directory lost its last record, a delete, in
/// `a_source_that_lost_a_delete_under_the_claim_keeps_the_staging_that_holds_it`.
#[derive(Clone, Copy, Debug)]
enum LostDelete {
    /// Truncated at the record boundary before the delete: it still validates.
    TruncatedAtBoundary,
    /// Cut inside the delete record: a torn tail, which an open accepts and drops.
    TornInsideDelete,
    /// Rewritten with the bytes it held before the delete: a rollback to an older generation.
    RolledBack,
}

/// specs/015 FR-3 after the third review, through a real compaction: a source that loses a delete
/// under the claim (truncated at the record boundary before it, torn inside it, or rolled back to
/// before it) holds every fact the staging holds, and one more: the deleted one. The staging is
/// then the only copy of the state in which it is deleted, so the next open keeps both and names
/// them, as at `1eb9de5`, instead of reporting `Recovered` with the deleted fact back.
#[test]
fn a_source_that_lost_a_delete_under_the_claim_keeps_the_staging_that_holds_it() {
    for family in ALL_FAMILIES {
        for case in [
            LostDelete::TruncatedAtBoundary,
            LostDelete::TornInsideDelete,
            LostDelete::RolledBack,
        ] {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            let (before_delete, after_delete) =
                store_whose_last_record_is_a_delete(&store_dir, family);
            let paths = closed_paths(&store_dir);
            let active = store_dir.join(active_name(family));
            change_the_source_under_the_claim(&store_dir, || match case {
                LostDelete::TruncatedAtBoundary => std::fs::OpenOptions::new()
                    .write(true)
                    .open(&active)
                    .unwrap()
                    .set_len(before_delete.len() as u64)
                    .unwrap(),
                LostDelete::TornInsideDelete => std::fs::OpenOptions::new()
                    .write(true)
                    .open(&active)
                    .unwrap()
                    .set_len(after_delete - 2)
                    .unwrap(),
                LostDelete::RolledBack => std::fs::write(&active, &before_delete).unwrap(),
            });
            assert!(
                paths.staging.is_dir(),
                "{family:?} {case:?}: the attempt must leave its staging"
            );
            let staging = paths.staging.clone();
            let canonical = store_dir.clone();
            assert_refused_unchanged(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?} {case:?}: a source that lost a delete under the claim"),
                move |error| {
                    matches!(
                        error,
                        crate::RecoveryError::AuthorityUndetermined {
                            active_path: Some(active),
                            recovery_path: Some(recovery),
                        } if *active == canonical && *recovery == staging
                    )
                },
            );
        }
    }
}

/// What a staged file in `a_staging_not_proved_to_hold_the_canonical_state_keeps_its_error_and_its_bytes`
/// holds beside the canonical directory.
#[derive(Clone, Copy, Debug)]
enum NotProved {
    /// The canonical directory gained a write the staged file predates. It cannot be told from a
    /// canonical directory that lost a delete the staged file records by the fact's absence.
    CanonicalGained,
    /// The staged file is a closed compaction of a copy in which one fact was deleted; the
    /// canonical directory still holds it.
    StagedDelete,
    /// The staged file holds only its header, an empty state, beside a canonical directory that
    /// is not empty: a kill right after the header was written, or a complete snapshot of a state
    /// with no key. The two cannot be told apart.
    HeaderOnly,
    /// The staged file is what compaction stages cut at the end of its first record, beside a
    /// canonical directory holding two: a kill on a record boundary, or a complete snapshot of a
    /// state in which the second fact was deleted (fifth review). The two cannot be told apart.
    CutAtARecordBoundary,
    /// A byte copy of the canonical active file (fourth review): it recovers the canonical state,
    /// but it is neither what a closed compaction stages for it nor the first bytes of that, which
    /// is all the open compares.
    ByteCopy,
}

/// specs/015 FR-3 after the third, fourth and fifth reviews: a staged file that holds a complete
/// header is removed only when it is byte for byte what a closed compaction of the canonical
/// directory stages for its family, or the first bytes of that ending inside one of its records.
/// A staged state records a deletion only by a fact's absence, so a canonical directory holding
/// more is not proof that the staging is redundant. Each case keeps the error it returned at
/// `1eb9de5` and changes nothing.
#[test]
fn a_staging_not_proved_to_hold_the_canonical_state_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        for case in [
            NotProved::CanonicalGained,
            NotProved::StagedDelete,
            NotProved::HeaderOnly,
            NotProved::CutAtARecordBoundary,
            NotProved::ByteCopy,
        ] {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            let staged = match case {
                NotProved::CanonicalGained => {
                    create_current_v2(&store_dir, family);
                    let staged = compaction_staged_file(&store_dir, family);
                    add_a_fact(&store_dir, family, false);
                    crate::test_support::maintenance_fixtures::forget_inner_lock(&store_dir);
                    staged
                }
                NotProved::StagedDelete => {
                    let (before_delete, _) =
                        store_whose_last_record_is_a_delete(&store_dir, family);
                    let staged = compaction_staged_file(&store_dir, family);
                    std::fs::write(store_dir.join(active_name(family)), &before_delete).unwrap();
                    staged
                }
                NotProved::HeaderOnly => {
                    create_current_v2(&store_dir, family);
                    let mut staged = compaction_staged_file(&store_dir, family);
                    staged.truncate(crate::wal::format::V2CodecProbe::HEADER_LEN);
                    staged
                }
                NotProved::CutAtARecordBoundary => {
                    create_current_v2(&store_dir, family);
                    add_a_fact(&store_dir, family, false);
                    crate::test_support::maintenance_fixtures::forget_inner_lock(&store_dir);
                    let mut staged = compaction_staged_file(&store_dir, family);
                    let boundaries = record_boundaries(&staged);
                    assert_eq!(boundaries.len(), 3, "{family:?}: a header and two records");
                    staged.truncate(boundaries[1]);
                    staged
                }
                NotProved::ByteCopy => {
                    create_current_v2(&store_dir, family);
                    let staged = std::fs::read(store_dir.join(active_name(family))).unwrap();
                    assert_ne!(
                        staged,
                        compaction_staged_file(&store_dir, family),
                        "{family:?}: the fixture's active file must not be what compaction stages"
                    );
                    staged
                }
            };
            let paths = closed_paths(&store_dir);
            stage_one_file(&paths, family, &staged);
            assert_refused_unchanged(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?} {case:?}"),
                undetermined,
            );
        }
    }
}

/// Whether the replay rejects `bytes` as `family`'s file outright (neither complete nor a torn
/// tail an open accepts).
fn replay_rejects(family: FixtureFamily, bytes: &[u8]) -> bool {
    match family {
        FixtureFamily::KeyValue => crate::wal::replay::classify_key_value_read_only(bytes).is_err(),
        FixtureFamily::KeySet => crate::wal::replay::classify_key_set_read_only(bytes).is_err(),
        FixtureFamily::KeyMap => crate::wal::replay::classify_key_map_read_only(bytes).is_err(),
    }
}

/// specs/015 FR-3 after the third review: a staged file that holds at least a header and that the
/// replay rejects -- damaged in its header, or in a record -- may still hold intact records the
/// canonical directory lacks, so it proves nothing. (Since the fifth review only a write of what
/// compaction stages, stopped inside its header or a record, is removed besides the whole file.)
/// The open keeps the error it returned at `1eb9de5` and changes nothing.
#[test]
fn a_staged_file_the_replay_rejects_past_its_header_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        for damaged_at in ["header", "first record", "last record"] {
            let (root, store_dir) = store_with(&[family], false);
            let paths = closed_paths(&store_dir);
            let other = root.path().join("other");
            copy_files(&store_dir, &other);
            add_a_fact(&other, family, false);
            let mut staged = compaction_staged_file(&other, family);
            std::fs::remove_dir_all(&other).unwrap();
            let header = crate::wal::format::V2CodecProbe::HEADER_LEN;
            let at = match damaged_at {
                "header" => 0,
                "first record" => header + 8,
                _ => staged.len() - 3,
            };
            staged[at] ^= 0xff;
            assert!(
                replay_rejects(family, &staged),
                "{family:?} damaged in its {damaged_at}: the fixture must be rejected"
            );
            stage_one_file(&paths, family, &staged);
            assert_refused_unchanged(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}: a staged file damaged in its {damaged_at}"),
                invalid_at(paths.staging.clone()),
            );
        }
    }
}

/// specs/015 FR-3: every staged family is compared, not only the first one listed. With all three
/// families staged, each in turn holds a fact the canonical directory lacks while the other two are
/// copies of their canonical files; each open keeps the error it returned at `1eb9de5`.
#[test]
fn every_staged_family_is_compared() {
    for differing in ALL_FAMILIES {
        let (root, store_dir) = store_with(&ALL_FAMILIES, false);
        let paths = closed_paths(&store_dir);
        let other = root.path().join("other");
        copy_files(&store_dir, &other);
        add_a_fact(&other, differing, false);
        std::fs::create_dir(&paths.staging).unwrap();
        for family in ALL_FAMILIES {
            let source = if family == differing {
                &other
            } else {
                &store_dir
            };
            std::fs::write(
                paths.staging.join(active_name(family)),
                compaction_staged_file(source, family),
            )
            .unwrap();
        }
        std::fs::remove_dir_all(&other).unwrap();
        for opened in ALL_FAMILIES {
            assert_refused_unchanged(
                root.path(),
                &store_dir,
                opened,
                &format!(
                    "all three staged, {differing:?} holding a fact the canonical directory lacks, \
                     opened as {opened:?}"
                ),
                undetermined,
            );
        }
    }
}

/// specs/015 FR-3 and FR-7: a staged file the open cannot read proves nothing, and is no reason to
/// return the read's I/O error either. The open keeps the error it returned at `1eb9de5` and
/// changes nothing.
#[cfg(unix)]
#[test]
fn a_staged_file_that_cannot_be_read_keeps_its_error_and_its_bytes() {
    use std::os::unix::fs::PermissionsExt;
    let family = FixtureFamily::KeyValue;
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    // What a compaction of the canonical directory stages, which an open that could read it
    // would remove: unread, it proves nothing.
    stage_one_file(&paths, family, &compaction_staged_file(&store_dir, family));
    let file = paths.staging.join(active_name(family));
    let before = without_open_locks(namespace(root.path()));
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&file).is_ok() {
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        eprintln!("skipped: this process may read a file with no permissions");
        return;
    }
    let result = open_family(&store_dir, family);
    if std::fs::symlink_metadata(&file).is_ok() {
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    assert!(
        matches!(&result, Err(crate::RecoveryError::InvalidArtifact { path }) if *path == paths.staging),
        "an unreadable staged file: expected its current refusal, got {result:?}"
    );
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "a refused open changed the directory"
    );
}

/// What a staged file in
/// `a_staging_that_is_what_compaction_stages_or_a_torn_write_of_it_is_removed` holds.
#[derive(Clone, Copy, Debug)]
enum Redundant {
    /// What a closed compaction of the canonical directory stages for the family.
    Compacted,
    /// Nothing: the file a staging write killed before its first byte leaves (`StagingWrite`).
    Empty,
    /// The first bytes of what compaction stages, which the replay rejects: a kill while the
    /// header was being written. It holds no fact and records no absence.
    CutInsideHeader,
    /// The same, one byte short of a whole header: the longest file that is still shorter than
    /// a header.
    OneByteShortOfHeader,
    /// The first bytes of what compaction stages, cut inside a record past the header: a kill
    /// inside a staging write's bytes (fifth review). In a large store the cut is page-granular
    /// and past the first 64 KiB block, as a kill inside a large write leaves it.
    TornInsideARecord,
    /// All of what compaction stages but its last byte, which is inside its last record.
    TornShortOfItsLastByte,
}

/// The lengths at which `encoded`, a current-format snapshot, ends as a complete one: after its
/// header, and after each record. A prefix of any other length is torn.
fn record_boundaries(encoded: &[u8]) -> Vec<usize> {
    use crate::wal::format::V2CodecProbe;
    let mut boundaries = vec![V2CodecProbe::HEADER_LEN];
    let mut end = V2CodecProbe::HEADER_LEN;
    while end < encoded.len() {
        let payload = u64::from_le_bytes(encoded[end + 6..end + 14].try_into().unwrap());
        end += V2CodecProbe::EMPTY_RECORD_LEN + usize::try_from(payload).unwrap();
        boundaries.push(end);
    }
    assert_eq!(end, encoded.len(), "the snapshot ends on a record boundary");
    boundaries
}

/// Where `TornInsideARecord` cuts `encoded`: ten bytes into its first record, or, in a large
/// store, the first multiple of 4 KiB from the third 64 KiB block on that is no record boundary.
fn torn_inside_a_record(encoded: &[u8], size: StoreSize) -> usize {
    let boundaries = record_boundaries(encoded);
    let cut = match size {
        StoreSize::Fixture => crate::wal::format::V2CodecProbe::HEADER_LEN + 10,
        StoreSize::Large => (2 * STAGED_READ_BLOCK..encoded.len())
            .step_by(4096)
            .find(|cut| !boundaries.contains(cut))
            .unwrap(),
    };
    assert!(cut < encoded.len() && !boundaries.contains(&cut));
    cut
}

/// Control for FR-3 after the third, fourth and fifth reviews: a staging directory whose every
/// staged file is what a closed compaction of the canonical directory stages, or a write of it
/// stopped inside its header or inside one of its records, is removed. Since the fifth review
/// also for a store whose staged file spans many 64 KiB blocks, the size of a real store.
#[test]
fn a_staging_that_is_what_compaction_stages_or_a_torn_write_of_it_is_removed() {
    let mut refused = Vec::new();
    for (family, size) in ALL_FAMILIES
        .into_iter()
        .flat_map(|family| [(family, StoreSize::Fixture), (family, StoreSize::Large)])
    {
        for case in [
            Redundant::Compacted,
            Redundant::Empty,
            Redundant::CutInsideHeader,
            Redundant::OneByteShortOfHeader,
            Redundant::TornInsideARecord,
            Redundant::TornShortOfItsLastByte,
        ] {
            let (root, store_dir) = store_sized(family, size);
            let paths = closed_paths(&store_dir);
            let mut staged = compaction_staged_file(&store_dir, family);
            assert_spans_blocks_when_large(&staged, size, &format!("{family:?} {case:?}"));
            match case {
                Redundant::Compacted => {}
                Redundant::Empty => staged.clear(),
                Redundant::CutInsideHeader => staged.truncate(10),
                Redundant::OneByteShortOfHeader => {
                    staged.truncate(crate::wal::format::V2CodecProbe::HEADER_LEN - 1)
                }
                Redundant::TornInsideARecord => {
                    let cut = torn_inside_a_record(&staged, size);
                    staged.truncate(cut);
                }
                Redundant::TornShortOfItsLastByte => {
                    staged.pop();
                }
            }
            stage_one_file(&paths, family, &staged);
            let before = namespace(root.path());
            match open_family(&store_dir, family) {
                Ok(crate::RecoveryStatus::Recovered) => {}
                other => {
                    refused.push(format!("{family:?} {size:?} {case:?}: {other:?}"));
                    continue;
                }
            }
            assert_no_closed_debris(&paths, &format!("{family:?} {size:?} {case:?}"));
            let mut expected = without_open_locks(before);
            expected.retain(|path, _| !path.starts_with(paths.staging.file_name().unwrap()));
            assert_eq!(
                without_open_locks(namespace(root.path())),
                expected,
                "{family:?} {size:?} {case:?}: the open changed more than the staging"
            );
            assert_three_reopens(&store_dir, family);
        }
    }
    assert!(
        refused.is_empty(),
        "stagings that are what compaction stages or a torn write of it kept their stores closed:\n{}",
        refused.join("\n")
    );
}

/// Writes a key/value store in `store_dir` one put at a time, and returns its active file's length
/// after each put: the record boundaries.
fn store_written_one_put_at_a_time(store_dir: &Path, puts: usize) -> Vec<u64> {
    std::fs::create_dir(store_dir).unwrap();
    let store = crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir)
        .unwrap()
        .into_store();
    let mut lengths = Vec::new();
    for index in 0..puts {
        store
            .try_put(format!("key-{index}").into_bytes(), b"value".to_vec())
            .unwrap();
        lengths.push(
            std::fs::metadata(store_dir.join(active_name(FixtureFamily::KeyValue)))
                .unwrap()
                .len(),
        );
    }
    drop(store);
    crate::test_support::maintenance_fixtures::forget_inner_lock(store_dir);
    lengths
}

/// specs/015 FR-3 after the second review, through a real compaction: a source that loses its
/// last record under the claim -- truncated at a record boundary, so it still validates -- makes
/// the attempt fail and leave its staging (FR-1), and the staging is then the only copy of that
/// record. The next open keeps both and names them, as at `1eb9de5`.
#[test]
fn a_source_truncated_under_the_claim_keeps_the_staging_that_holds_what_it_lost() {
    let family = FixtureFamily::KeyValue;
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    let lengths = store_written_one_put_at_a_time(&store_dir, 5);
    let paths = closed_paths(&store_dir);
    change_the_source_under_the_claim(&store_dir, || {
        std::fs::OpenOptions::new()
            .write(true)
            .open(store_dir.join(active_name(family)))
            .unwrap()
            .set_len(lengths[3])
            .unwrap();
    });
    assert!(paths.staging.is_dir(), "the attempt must leave its staging");
    assert!(
        super::inspection::inspect_generation(&store_dir).is_ok(),
        "the truncated source must still validate"
    );
    let staging = paths.staging.clone();
    let canonical = store_dir.clone();
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "a source truncated at a record boundary under the claim",
        move |error| {
            matches!(
                error,
                crate::RecoveryError::AuthorityUndetermined {
                    active_path: Some(active),
                    recovery_path: Some(recovery),
                } if *active == canonical && *recovery == staging
            )
        },
    );
}

/// The same with the source rolled back under the claim to an older generation that validates:
/// the staging holds the write the rollback lost.
#[test]
fn a_source_rolled_back_under_the_claim_keeps_the_staging_that_holds_what_it_lost() {
    let family = FixtureFamily::KeyValue;
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    store_written_one_put_at_a_time(&store_dir, 2);
    let older = std::fs::read(store_dir.join(active_name(family))).unwrap();
    crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
        .unwrap()
        .into_store()
        .try_put(b"captured".to_vec(), b"write".to_vec())
        .unwrap();
    crate::test_support::maintenance_fixtures::forget_inner_lock(&store_dir);
    let paths = closed_paths(&store_dir);
    change_the_source_under_the_claim(&store_dir, || {
        std::fs::write(store_dir.join(active_name(family)), &older).unwrap();
    });
    assert!(paths.staging.is_dir(), "the attempt must leave its staging");
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        family,
        "a source rolled back to an older generation under the claim",
        undetermined,
    );
}

/// Makes a directory unreadable, and readable again when dropped.
#[cfg(unix)]
pub(super) struct Unreadable<'a>(pub(super) &'a Path);

#[cfg(unix)]
impl Unreadable<'_> {
    /// Removes every permission from the directory, or returns `None` when this process can read
    /// it anyway (root can), so that the caller skips.
    pub(super) fn new(directory: &Path) -> Option<Unreadable<'_>> {
        use std::os::unix::fs::PermissionsExt;
        let unreadable = Unreadable(directory);
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read_dir(directory).is_ok() {
            eprintln!("skipped: this process may read a directory with no permissions");
            return None;
        }
        Some(unreadable)
    }
}

#[cfg(unix)]
impl Drop for Unreadable<'_> {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o755));
    }
}

/// specs/015 FR-3 and FR-7 after the second review: a staging directory the open cannot read is
/// not provably debris. The open keeps the error it returned at `1eb9de5` instead of returning
/// the read's I/O error, and changes nothing; FR-7's I/O error is for a removal that fails after
/// the state was proved.
#[cfg(unix)]
#[test]
fn a_staging_directory_that_cannot_be_read_keeps_its_error_and_its_bytes() {
    let family = FixtureFamily::KeyValue;
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    let before = without_open_locks(namespace(root.path()));
    let Some(unreadable) = Unreadable::new(&paths.staging) else {
        return;
    };
    let result = open_family(&store_dir, family);
    drop(unreadable);
    assert!(
        matches!(&result, Err(crate::RecoveryError::InvalidArtifact { path }) if *path == paths.staging),
        "an unreadable staging directory: expected its current refusal, got {result:?}"
    );
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "a refused open changed the directory"
    );
}

/// Names the directory a relative-path compaction child changes into before it compacts `store`.
const RELATIVE_CHILD_PARENT_ENV: &str = "PIGMENT_DB_TEST_RELATIVE_COMPACTION_PARENT";

/// The unit-test child for the test below. It changes the process's working directory, which is
/// why it runs in a process of its own.
#[test]
fn relative_store_path_compaction_child() {
    use crate::compaction::publication::manifest_publication_faults::{inject, Fault};
    use crate::compaction::publication::ManifestPublishStage;

    let Some(parent) = std::env::var_os(RELATIVE_CHILD_PARENT_ENV) else {
        return;
    };
    std::env::set_current_dir(parent).unwrap();
    let store = Path::new("store");
    // The first publication fails, so the attempt stops before `Prepared` whether or not its
    // validation accepts a relative path (specs/016).
    let _injection = inject(
        &closed_paths(store).manifest_next,
        1,
        ManifestPublishStage::Written,
        Fault::Error,
    );
    let result = compact(store);
    eprintln!("relative compaction child: {result:?}");
    std::process::exit(if result.is_err() {
        crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED
    } else {
        0
    });
}

/// specs/015 FR-1 after the second review: the attempt's guard compares the canonical directory
/// with its capture wherever the caller's path spelling puts it. Given `store` relative to the
/// working directory -- the motivating defect's own trigger, whose parent is the empty path -- a
/// compaction that fails before `Prepared` still removes its staging.
#[test]
fn a_compaction_given_a_relative_store_path_that_fails_removes_its_staging() {
    let (root, store_dir) = store_with(&ALL_FAMILIES, true);
    let paths = closed_paths(&store_dir);
    let before = namespace(root.path());
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("compaction::unpublished_attempt_tests::relative_store_path_compaction_child")
        .arg("--exact")
        .arg("--nocapture")
        .env(RELATIVE_CHILD_PARENT_ENV, root.path())
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > std::time::Duration::from_secs(60) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the relative-path compaction child did not finish");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    #[cfg(windows)]
    crate::test_support::fault_checkpoint::wait_for_released_lock_files(root.path());
    assert_eq!(
        status.code(),
        Some(crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED),
        "the relative-path compaction must fail before Prepared"
    );
    assert!(
        !exists(&paths.staging),
        "a compaction given a relative store path left its staging"
    );
    assert_no_closed_debris(&paths, "after a relative-path compaction");
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "the store's files changed"
    );
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
}

/// The unit-test child for the test below: compacts `store` relative to `one/`, and, while the
/// compaction is parked at its cut, another thread moves the process's working directory to
/// `two/` when the parent asks it to. The harness's store directory names the parent of both.
#[test]
fn working_directory_change_compaction_child() {
    let Some(root) = crate::test_support::fault_checkpoint::maintenance_child_store_dir() else {
        return;
    };
    std::env::set_current_dir(root.join("one")).unwrap();
    let watcher = {
        let root = root.clone();
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            while !root.join("change").exists() {
                assert!(started.elapsed() < std::time::Duration::from_secs(30));
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            std::env::set_current_dir(root.join("two")).unwrap();
            std::fs::write(root.join("changed"), b"").unwrap();
        })
    };
    let result = compact(Path::new("store"));
    watcher.join().unwrap();
    eprintln!("working-directory child: {result:?}");
    std::process::exit(if result.is_err() {
        crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED
    } else {
        0
    });
}

/// specs/015 FR-1 after the third review: the guard acts on the paths it resolved when staging was
/// created, not on the caller's spelling read again when it drops. A compaction given `store`
/// relative to `one/`, whose process's working directory moves to `two/` while it runs, removes
/// its own staging in `one/` and leaves `two/`'s, which holds a fact `two/store` lacks and may be
/// the only copy of it.
///
/// After the fourth review, also at `StagingCreate`, between the staging directory's creation and
/// the guard's: the guard's paths are resolved before the directory is created, so the change
/// cannot bind the guard to another directory. Both stores are byte-identical, so a guard bound
/// to `two/` would find `two/store` equal to its capture and remove `two/`'s staging.
#[test]
fn a_working_directory_change_during_a_compaction_never_removes_another_directorys_staging() {
    use crate::test_support::fault_checkpoint::MaintenanceCut;
    for cut in [MaintenanceCut::StagingCreate, MaintenanceCut::StagingSync] {
        working_directory_change_at(cut);
    }
}

fn working_directory_change_at(cut: crate::test_support::fault_checkpoint::MaintenanceCut) {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceFaultPoint, MaintenancePhase, MAINTENANCE_CHILD_FAILED,
    };
    let family = FixtureFamily::KeyValue;
    let root = tempfile::tempdir().unwrap();
    for directory in ["one", "two"] {
        let store_dir = root.path().join(directory).join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        create_current_v2(&store_dir, family);
    }
    let one = root.path().join("one/store");
    let two = root.path().join("two/store");
    let other = root.path().join("other");
    copy_files(&two, &other);
    add_a_fact(&other, family, false);
    let staged = std::fs::read(other.join(active_name(family))).unwrap();
    std::fs::remove_dir_all(&other).unwrap();
    let two_paths = closed_paths(&two);
    stage_one_file(&two_paths, family, &staged);
    let two_before = namespace(&root.path().join("two"));
    let one_store_before = namespace(&one);
    let pause_dir = tempfile::tempdir().unwrap();
    // The child resolves `store` against `one/`; the harness's store directory names their parent.
    let child = pause_maintenance_child(
        "compaction::unpublished_attempt_tests::working_directory_change_compaction_child",
        root.path(),
        pause_dir.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut,
        },
    );
    assert!(
        closed_paths(&one).staging.is_dir(),
        "{cut:?}: the attempt's staging"
    );
    if let crate::test_support::fault_checkpoint::MaintenanceCut::StagingCreate = cut {
        // A main manifest beside `one/store` that the attempt did not write and that stays as it
        // is (earlier maintenance's, as plan.md's D1 decision names), and none beside
        // `two/store`: the guard, built after the change, must read the one in `one/` (fifth
        // review, probes).
        std::fs::write(
            closed_paths(&one).manifest,
            b"a main manifest earlier maintenance left",
        )
        .unwrap();
    }
    std::fs::write(root.path().join("change"), b"").unwrap();
    let started = std::time::Instant::now();
    while !root.path().join("changed").exists() {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "{cut:?}: the child never changed its working directory"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(
        child.resume(),
        MAINTENANCE_CHILD_FAILED,
        "{cut:?}: the compaction must fail before Prepared"
    );
    #[cfg(windows)]
    crate::test_support::fault_checkpoint::wait_for_released_lock_files(root.path());
    assert_eq!(
        namespace(&root.path().join("two")),
        two_before,
        "{cut:?}: a compaction of one/store changed two/, whose staging may be the only copy of a \
         fact"
    );
    assert!(
        !exists(&closed_paths(&one).staging),
        "{cut:?}: the attempt left its own staging in one/"
    );
    let mut one_store_after = namespace(&one);
    one_store_after.remove(Path::new(crate::maintenance_coordination::INNER_LOCK_NAME));
    let mut one_store_before = one_store_before;
    one_store_before.remove(Path::new(crate::maintenance_coordination::INNER_LOCK_NAME));
    assert_eq!(
        one_store_after, one_store_before,
        "{cut:?}: one/store changed"
    );
}

/// The same for a store path that is a symlink to the store directory: the staging the attempt
/// created beside the link is removed.
#[test]
fn a_compaction_given_a_symlinked_store_path_that_fails_removes_its_staging() {
    use crate::compaction::publication::manifest_publication_faults::{inject, Fault};
    use crate::compaction::publication::ManifestPublishStage;

    let (root, store_dir) = store_with(&[FixtureFamily::KeyValue], true);
    let link = root.path().join("link");
    #[cfg(unix)]
    let linked = std::os::unix::fs::symlink(&store_dir, &link);
    #[cfg(windows)]
    let linked = std::os::windows::fs::symlink_dir(&store_dir, &link);
    if let Err(error) = linked {
        eprintln!("skipped: this platform refused the link: {error}");
        return;
    }
    let paths = closed_paths(&link);
    let before = namespace(root.path());
    {
        let _injection = inject(
            &paths.manifest_next,
            1,
            ManifestPublishStage::Written,
            Fault::Error,
        );
        assert!(
            compact(&link).is_err(),
            "the compaction must fail before Prepared"
        );
    }
    assert!(
        !exists(&paths.staging),
        "a compaction given a symlinked store path left its staging beside the link"
    );
    assert_no_closed_debris(&paths, "beside the link");
    assert_no_closed_debris(&closed_paths(&store_dir), "beside the store");
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "the store's files changed"
    );
    assert_three_reopens(&store_dir, FixtureFamily::KeyValue);
}

/// Control for FR-1 (plan D1): the guard removes a real staging directory, never through a
/// symlink. A staging path replaced by a link to a directory elsewhere leaves both the link and
/// what it points to.
#[test]
fn the_guard_never_removes_through_a_staging_symlink() {
    let (root, store_dir) = store_with(&[FixtureFamily::KeyValue], false);
    let (prepared, unpublished) = super::prepare_closed_staging_guarded(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    let staging = prepared.paths.staging.clone();
    let elsewhere = root.path().join("elsewhere");
    std::fs::rename(&staging, &elsewhere).unwrap();
    #[cfg(unix)]
    let linked = std::os::unix::fs::symlink(&elsewhere, &staging);
    #[cfg(windows)]
    let linked = std::os::windows::fs::symlink_dir(&elsewhere, &staging);
    if let Err(error) = linked {
        drop(unpublished);
        eprintln!("skipped: this platform refused the link: {error}");
        return;
    }
    drop(unpublished);
    assert!(
        elsewhere
            .join(active_name(FixtureFamily::KeyValue))
            .is_file(),
        "the guard removed files through a staging symlink"
    );
    assert!(
        std::fs::symlink_metadata(&staging).is_ok_and(|metadata| metadata.file_type().is_symlink()),
        "the guard removed the staging symlink"
    );
}

/// Runs the unit test `exact_test_name` in a child process with the environment variable `key`
/// set to `value`, and returns its exit code. The child does nothing unless `key` is set.
fn run_unit_test_child(exact_test_name: &str, key: &str, value: &Path) -> Option<i32> {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg(exact_test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(key, value)
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code();
        }
        if started.elapsed() > std::time::Duration::from_secs(60) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the child {exact_test_name} did not finish");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// `root/one/store` and `root/two/store`, each the same key/value store.
fn two_stores(root: &Path) -> (PathBuf, PathBuf) {
    let family = FixtureFamily::KeyValue;
    let mut stores = ["one", "two"].into_iter().map(|directory| {
        let store_dir = root.join(directory).join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        create_current_v2(&store_dir, family);
        store_dir
    });
    (stores.next().unwrap(), stores.next().unwrap())
}

/// Names the parent of `one/` and `two/` for the guard child below.
const GUARD_MANIFEST_CHILD_ENV: &str = "PIGMENT_DB_TEST_GUARD_MANIFEST_ROOT";

/// The unit-test child for the test below. It changes the process's working directory, which is
/// why it runs in a process of its own.
#[test]
fn guard_manifest_resolution_child() {
    let Some(root) = std::env::var_os(GUARD_MANIFEST_CHILD_ENV).map(PathBuf::from) else {
        return;
    };
    std::env::set_current_dir(root.join("one")).unwrap();
    let (_prepared, unpublished) = super::prepare_closed_staging_guarded(
        Path::new("store"),
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    // A `Prepared` renamed into place beside the staging: the main manifest is no longer what it
    // was when staging was created, so recovery of `Prepared` owns the staging.
    std::fs::write(
        closed_paths(&root.join("one/store")).manifest,
        b"a Prepared renamed into place",
    )
    .unwrap();
    std::env::set_current_dir(root.join("two")).unwrap();
    drop(unpublished);
    std::process::exit(0);
}

/// specs/015 FR-1 after the third review, the main manifest's half (fourth review, probes): the
/// guard checks the main manifest it resolved when staging was created, not the one the caller's
/// spelling names when it drops. A guard for `store` relative to `one/`, whose `Prepared` was
/// renamed into place in `one/` before the working directory moved to `two/` (whose main manifest
/// is still absent, as `one/`'s was), leaves the staging that `Prepared` names.
#[test]
fn the_guard_checks_the_main_manifest_it_resolved_when_staging_was_created() {
    let root = tempfile::tempdir().unwrap();
    let (one, two) = two_stores(root.path());
    let two_before = namespace(&root.path().join("two"));
    assert_eq!(
        run_unit_test_child(
            "compaction::unpublished_attempt_tests::guard_manifest_resolution_child",
            GUARD_MANIFEST_CHILD_ENV,
            root.path(),
        ),
        Some(0),
        "the guard child must finish"
    );
    assert!(
        closed_paths(&one).staging.is_dir(),
        "the guard removed a staging that a published Prepared names, after the working \
         directory changed"
    );
    assert_eq!(
        namespace(&root.path().join("two")),
        two_before,
        "two/ changed"
    );
    let _ = two;
}

/// Names the parent of `one/` and `two/` for the publication child below.
const PUBLICATION_CHILD_ENV: &str = "PIGMENT_DB_TEST_PUBLICATION_ROOT";

/// The unit-test child for the test below: publishes a closed `Prepared` for `store` relative to
/// `one/`, and fails the publication once its temporary is created, after moving the process's
/// working directory to `two/`.
#[test]
fn publication_working_directory_child() {
    use crate::compaction::publication::{
        publish_manifest_buffered_with_checkpoint, ManifestPublishStage,
    };
    let Some(root) = std::env::var_os(PUBLICATION_CHILD_ENV).map(PathBuf::from) else {
        return;
    };
    let (_, manifest) = closed_prepared_manifest(&root.join("one/store"));
    std::env::set_current_dir(root.join("one")).unwrap();
    let relative = closed_paths(Path::new("store"));
    let two = root.join("two");
    let result = publish_manifest_buffered_with_checkpoint(&relative, &manifest, |stage| {
        if stage == ManifestPublishStage::Created {
            std::env::set_current_dir(&two).unwrap();
            return Err(std::io::Error::other(
                "injected failure after the working directory changed",
            ));
        }
        Ok(())
    });
    std::process::exit(if result.is_err() {
        crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED
    } else {
        0
    });
}

/// specs/015 FR-2 (fourth review): a failed publication removes the temporary it created, not
/// the entry the caller's spelling names when it fails. A publication for `store` relative to
/// `one/` that fails after the working directory moved to `two/` removes `one/`'s temporary and
/// leaves `two/`'s, which another publication may own.
#[test]
fn a_working_directory_change_during_a_publication_never_removes_another_directorys_temporary() {
    let root = tempfile::tempdir().unwrap();
    let (one, two) = two_stores(root.path());
    let two_temporary = closed_paths(&two).manifest_next;
    std::fs::write(&two_temporary, b"another publication's temporary").unwrap();
    assert_eq!(
        run_unit_test_child(
            "compaction::unpublished_attempt_tests::publication_working_directory_child",
            PUBLICATION_CHILD_ENV,
            root.path(),
        ),
        Some(crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED),
        "the publication must fail"
    );
    assert_eq!(
        std::fs::read(&two_temporary).ok().as_deref(),
        Some(b"another publication's temporary".as_slice()),
        "a publication in one/ removed two/'s temporary"
    );
    assert!(
        !exists(&closed_paths(&one).manifest_next),
        "the failed publication left its own temporary in one/"
    );
}

/// Makes `link` a symlink to the directory `target`.
fn link_directory(target: &Path, link: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_dir(target, link)
    }
}

/// Removes the directory symlink `link`, not its target.
fn unlink_directory(link: &Path) {
    #[cfg(unix)]
    std::fs::remove_file(link).unwrap();
    #[cfg(windows)]
    std::fs::remove_dir(link).unwrap();
}

/// What runs the closed discard in the two tests below.
#[derive(Clone, Copy, Debug)]
enum DiscardBy {
    Open,
    CompactionStart,
}

/// What an open or a compaction answered, for the two tests below.
#[derive(Debug, Eq, PartialEq)]
enum Answered {
    Ok,
    Undetermined,
    Other(String),
}

fn discard_by(by: DiscardBy, store_dir: &Path) -> Answered {
    match by {
        DiscardBy::Open => match open_family(store_dir, FixtureFamily::KeyValue) {
            Ok(_) => Answered::Ok,
            Err(crate::RecoveryError::AuthorityUndetermined { .. }) => Answered::Undetermined,
            Err(error) => Answered::Other(format!("{error:?}")),
        },
        DiscardBy::CompactionStart => match compact(store_dir) {
            Ok(_) => Answered::Ok,
            Err(crate::CompactionError::AuthorityUndetermined { .. }) => Answered::Undetermined,
            Err(error) => Answered::Other(format!("{error:?}")),
        },
    }
}

/// `one/store` and `two/store` with closed debris beside each, and `current`, a symlink to `one`.
/// `one/` holds what a compaction of its store stages. `two/` holds the same when
/// `two_redundant`, and otherwise a staged file holding a fact `two/store` lacks, of which it is
/// the only copy. `None` when the platform refuses the link.
fn two_debris_behind_a_link(
    root: &Path,
    two_redundant: bool,
) -> Option<(PathBuf, PathBuf, PathBuf)> {
    let family = FixtureFamily::KeyValue;
    let (one, two) = two_stores(root);
    stage_what_compaction_stages(&one, &closed_paths(&one).staging);
    if two_redundant {
        stage_what_compaction_stages(&two, &closed_paths(&two).staging);
    } else {
        let other = root.join("other");
        copy_files(&two, &other);
        add_a_fact(&other, family, false);
        stage_one_file(
            &closed_paths(&two),
            family,
            &compaction_staged_file(&other, family),
        );
        std::fs::remove_dir_all(&other).unwrap();
    }
    if let Err(error) = link_directory(&root.join("one"), &root.join("current")) {
        eprintln!("skipped: this platform refused the link: {error}");
        return None;
    }
    Some((root.join("current/store"), one, two))
}

/// specs/015 FR-3 after the fourth review: the closed discard reads and removes only in the
/// directory its open's lease or its compaction's claim locked, resolved once. An open, or a
/// compaction, of `current/store` with `current -> one` proves `one/`'s debris redundant; while
/// it is parked between its proof and its removal, `current` is pointed at `two/`, whose staging
/// is the only copy of a fact `two/store` lacks. The discard removes what it proved in `one/` and
/// leaves `two/` as it was (what the open or compaction then does through the caller's spelling
/// is spec 016's).
#[test]
fn a_discard_removes_only_what_it_proved_in_the_directory_it_locked() {
    use crate::compaction::recovery::recovery_pause::{install, Point};
    for by in [DiscardBy::Open, DiscardBy::CompactionStart] {
        let root = tempfile::tempdir().unwrap();
        let Some((store, one, _two)) = two_debris_behind_a_link(root.path(), false) else {
            return;
        };
        let two_before = without_open_locks(namespace(&root.path().join("two")));
        let pause = install(&store, Point::DiscardProved);
        let answered = std::thread::scope(|scope| {
            let discarding = scope.spawn(|| discard_by(by, &store));
            pause.wait_reached();
            unlink_directory(&root.path().join("current"));
            link_directory(&root.path().join("two"), &root.path().join("current")).unwrap();
            pause.release();
            discarding.join().unwrap()
        });
        drop(pause);
        assert_eq!(
            without_open_locks(namespace(&root.path().join("two"))),
            two_before,
            "{by:?}: a discard that proved one/'s debris changed two/ ({answered:?})"
        );
        assert!(
            !exists(&closed_paths(&one).staging),
            "{by:?}: the discard left what it proved in one/ ({answered:?})"
        );
    }
}

/// specs/015 FR-3 after the fourth review: the closed discard acts only when the caller's path
/// still names the directory its open or compaction locked. When `current` is pointed at `two/`
/// before the discard starts, the discard leaves both directories as they were, even though
/// `two/`'s debris is redundant too, and the open or compaction answers as at `1eb9de5`: the
/// classification of what the caller's path now names, `AuthorityUndetermined`.
#[test]
fn a_discard_leaves_a_directory_its_open_or_claim_did_not_lock() {
    use crate::compaction::recovery::recovery_pause::{install, Point};
    for by in [DiscardBy::Open, DiscardBy::CompactionStart] {
        let root = tempfile::tempdir().unwrap();
        let Some((store, _one, _two)) = two_debris_behind_a_link(root.path(), true) else {
            return;
        };
        let one_before = without_open_locks(namespace(&root.path().join("one")));
        let two_before = without_open_locks(namespace(&root.path().join("two")));
        let pause = install(&store, Point::DiscardEntry);
        let answered = std::thread::scope(|scope| {
            let discarding = scope.spawn(|| discard_by(by, &store));
            pause.wait_reached();
            unlink_directory(&root.path().join("current"));
            link_directory(&root.path().join("two"), &root.path().join("current")).unwrap();
            pause.release();
            discarding.join().unwrap()
        });
        drop(pause);
        assert_eq!(
            without_open_locks(namespace(&root.path().join("two"))),
            two_before,
            "{by:?}: a discard acted in two/, which its open or claim did not lock ({answered:?})"
        );
        assert_eq!(
            without_open_locks(namespace(&root.path().join("one"))),
            one_before,
            "{by:?}: one/ changed ({answered:?})"
        );
        assert_eq!(answered, Answered::Undetermined, "{by:?}");
    }
}

/// specs/015 FR-3 (fourth review, probes): the canonical chain a staged file is compared with is
/// replayed in the order an open replays it, sealed segments first and in order. A key rewritten
/// across sealed segments, staged by a compaction killed before `Prepared`, is removed at open.
#[test]
fn a_key_rewritten_across_sealed_segments_does_not_keep_its_staging() {
    let family = FixtureFamily::KeyValue;
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    let options = crate::DurableStoreOptions::default()
        .with_wal_segment_size(crate::WalSegmentSize::try_from(170_u64).unwrap());
    {
        let store = crate::key_value_store::DurableKeyValueStore::try_init_new_with_options(
            &store_dir, options,
        )
        .unwrap()
        .into_store();
        for (key, value) in [
            ("alpha", "1"),
            ("beta", "2"),
            ("alpha", "3"),
            ("gamma", "4"),
            ("alpha", "5"),
            ("delta", "6"),
        ] {
            store
                .try_put(key.as_bytes().to_vec(), value.as_bytes().to_vec())
                .unwrap();
        }
    }
    crate::test_support::maintenance_fixtures::forget_inner_lock(&store_dir);
    assert!(
        store_dir.join(sealed_name(family, 2)).is_file(),
        "the key must be rewritten across sealed segments"
    );
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    let opened = open_family(&store_dir, family);
    assert!(
        matches!(opened, Ok(crate::RecoveryStatus::Recovered)),
        "a staging that is what compaction stages kept the store closed: {opened:?}"
    );
    assert_no_closed_debris(&paths, "after a key rewritten across sealed segments");
    let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"alpha"), Some(b"5".to_vec()));
}

/// Whether replaying `bytes` as `family`'s file recovers at least one key.
fn replays_to_a_fact(family: FixtureFamily, bytes: &[u8]) -> bool {
    use crate::wal::replay::TailReplay;
    fn holds_a_key<S: IntoIterator>(replay: TailReplay<S>) -> bool {
        match replay {
            TailReplay::Complete(replay) | TailReplay::RecoverableTail { replay, .. } => {
                replay.snapshot.into_iter().next().is_some()
            }
            TailReplay::Invalid(_) => false,
        }
    }
    match family {
        FixtureFamily::KeyValue => holds_a_key(crate::wal::replay::replay_key_value_tail(bytes)),
        FixtureFamily::KeySet => holds_a_key(crate::wal::replay::replay_key_set_tail(bytes)),
        FixtureFamily::KeyMap => holds_a_key(crate::wal::replay::replay_key_map_tail(bytes)),
    }
}

/// specs/015 FR-3 (fourth and fifth reviews): a staged file shorter than a header is removed only
/// when it is the first bytes of what compaction stages. A headerless legacy-format file of under
/// 64 bytes that replays to a fact is not, and compaction never writes it, so the open keeps the
/// error it returned at `1eb9de5` and changes nothing.
#[test]
fn a_short_staged_file_that_replays_to_a_fact_keeps_its_error_and_its_bytes() {
    use std::collections::{HashMap, HashSet};
    for family in [FixtureFamily::KeyValue, FixtureFamily::KeySet] {
        let legacy = match family {
            FixtureFamily::KeyValue => crate::wal::replay::encode_key_value_snapshot(
                &HashMap::from([(b"zz".to_vec(), b"y".to_vec())]),
            ),
            _ => crate::wal::replay::encode_key_set_snapshot(&HashMap::from([(
                b"zz".to_vec(),
                HashSet::from([b"y".to_vec()]),
            )])),
        };
        assert!(
            legacy.len() < crate::wal::format::V2CodecProbe::HEADER_LEN,
            "{family:?}: the legacy file must be shorter than a header"
        );
        assert!(
            replays_to_a_fact(family, &legacy),
            "{family:?}: the legacy file must replay to a fact"
        );
        let (root, store_dir) = store_with(&[family], false);
        let paths = closed_paths(&store_dir);
        stage_one_file(&paths, family, &legacy);
        assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            &format!("{family:?}: a {}-byte legacy staged file", legacy.len()),
            invalid_at(paths.staging.clone()),
        );
    }
}

/// One complete current-format record appended at `offset`.
fn current_record(action: u8, payload: &[u8], offset: u64) -> Vec<u8> {
    crate::wal::format::V2CodecProbe::encode_complete_record(
        crate::wal::format::V2RecordProbeFields {
            action,
            payload,
            physical_start: offset,
            mutation_start: offset,
            index: 0,
            count: 1,
            timestamp_bucket: 0,
        },
    )
}

/// specs/015 FR-3 (fourth review): a canonical key left with no members or entries (its last
/// member removed, and the delete that followed lost, as no current writer leaves it) is a key an
/// open publishes (`contains_key`), while a staged snapshot records a deleted key by its absence.
/// The two encode alike, so the open cannot tell a staging that predates the key's emptying from
/// one that records its delete: it keeps the error it returned at `1eb9de5` and changes nothing.
#[test]
fn a_canonical_key_left_with_no_members_keeps_the_staging_that_records_its_absence() {
    use crate::model::{SearchKey, SortedMapKey};
    use crate::wal::replay::{
        encode_current_key_map_snapshot_with_metadata,
        encode_current_key_set_snapshot_with_metadata,
    };
    use std::collections::{BTreeMap, HashMap, HashSet};
    for family in [FixtureFamily::KeySet, FixtureFamily::KeyMap] {
        let (canonical, staged) = match family {
            FixtureFamily::KeySet => {
                let full = HashMap::from([
                    (b"group".to_vec(), HashSet::from([b"red".to_vec()])),
                    (b"other".to_vec(), HashSet::from([b"x".to_vec()])),
                ]);
                let mut canonical =
                    encode_current_key_set_snapshot_with_metadata(&full, 60_000_000_000, 0)
                        .unwrap();
                let payload = bincode::serialize(&crate::wal::model::KeyValueData::new(
                    b"group".to_vec(),
                    b"red".to_vec(),
                ))
                .unwrap();
                let offset = canonical.len() as u64;
                canonical.extend(current_record(
                    crate::wal::model::SET_REMOVE_ACT,
                    &payload,
                    offset,
                ));
                let rest = HashMap::from([(b"other".to_vec(), HashSet::from([b"x".to_vec()]))]);
                let staged =
                    encode_current_key_set_snapshot_with_metadata(&rest, 60_000_000_000, 0)
                        .unwrap();
                (canonical, staged)
            }
            _ => {
                let full = HashMap::from([
                    (
                        b"book".to_vec(),
                        BTreeMap::from([(SearchKey::from(1), b"one".to_vec())]),
                    ),
                    (
                        b"other".to_vec(),
                        BTreeMap::from([(SearchKey::from(1), b"x".to_vec())]),
                    ),
                ]);
                let mut canonical =
                    encode_current_key_map_snapshot_with_metadata(&full, 60_000_000_000, 0)
                        .unwrap();
                let payload =
                    bincode::serialize(&SortedMapKey::new(b"book".to_vec(), SearchKey::from(1)))
                        .unwrap();
                let offset = canonical.len() as u64;
                canonical.extend(current_record(
                    crate::wal::model::MAP_REMOVE_V2_ACT,
                    &payload,
                    offset,
                ));
                let rest = HashMap::from([(
                    b"other".to_vec(),
                    BTreeMap::from([(SearchKey::from(1), b"x".to_vec())]),
                )]);
                let staged =
                    encode_current_key_map_snapshot_with_metadata(&rest, 60_000_000_000, 0)
                        .unwrap();
                (canonical, staged)
            }
        };
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        std::fs::write(store_dir.join(active_name(family)), &canonical).unwrap();
        let paths = closed_paths(&store_dir);
        stage_one_file(&paths, family, &staged);
        assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            &format!("{family:?}: a canonical key with no members beside a staging without it"),
            undetermined,
        );
    }
}

/// specs/015 FR-1 and FR-7 (fourth review): a staging write that fails while the source changed
/// under the claim leaves the staging, which may hold the only copy of what the source lost, and
/// the next open keeps the error of a staging it cannot prove redundant. (At `1eb9de5` the failed
/// write removed the staging, and the next open succeeded without the lost family.)
#[test]
fn a_staging_write_that_fails_over_a_changed_source_keeps_the_staging_and_its_error() {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        MAINTENANCE_CHILD_FAILED,
    };
    let (root, store_dir) = store_with(&ALL_FAMILIES, false);
    let paths = closed_paths(&store_dir);
    let staged_set = compaction_staged_file(&store_dir, FixtureFamily::KeySet);
    let pause_dir = tempfile::tempdir().unwrap();
    let child = pause_maintenance_child(
        CLOSED_CHILD,
        &store_dir,
        pause_dir.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut: MaintenanceCut::StagingCreate,
        },
    );
    // The third family's file cannot be created, and the source loses a family meanwhile.
    std::fs::create_dir(paths.staging.join(active_name(FixtureFamily::KeyMap))).unwrap();
    std::fs::remove_file(store_dir.join(active_name(FixtureFamily::KeySet))).unwrap();
    assert_eq!(
        child.resume(),
        MAINTENANCE_CHILD_FAILED,
        "the staging write must fail"
    );
    #[cfg(windows)]
    crate::test_support::fault_checkpoint::wait_for_released_lock_files(root.path());
    assert_eq!(
        std::fs::read(paths.staging.join(active_name(FixtureFamily::KeySet))).ok(),
        Some(staged_set),
        "the failed attempt removed the only copy of the family its source lost"
    );
    assert_refused_unchanged(
        root.path(),
        &store_dir,
        FixtureFamily::KeyValue,
        "a staging write that failed over a changed source",
        invalid_at(paths.staging.clone()),
    );
}

/// specs/015 FR-3 and plan IV (fourth review): two first opens of different families in one
/// process both run the closed discard. With the first parked after its proof, a first open of
/// another family removes the debris and reports `Recovered`; the parked open then fails with its
/// removal's `NotFound` (`RecoveryError::Io`), as plan.md records. Nothing is lost, no debris is
/// left, and every family reopens with its exact state.
#[test]
fn two_first_opens_racing_through_the_discard_lose_nothing_and_leave_no_debris() {
    use crate::compaction::recovery::recovery_pause::{install, Point};
    let (_root, store_dir) = store_with(&ALL_FAMILIES, false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    std::fs::write(&paths.manifest_next, b"an unpublished closed Prepared").unwrap();
    let pause = install(&store_dir, Point::DiscardProved);
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| open_family(&store_dir, FixtureFamily::KeyValue));
        pause.wait_reached();
        let second = crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir)
            .map(|outcome| (outcome.status(), outcome.into_store()));
        pause.release();
        (first.join().unwrap(), second)
    });
    drop(pause);
    let (second_status, second_store) = second.expect("the second open");
    assert_eq!(second_status, crate::RecoveryStatus::Recovered);
    match &first {
        Err(crate::RecoveryError::Io { path, source, .. })
            if *path == paths.staging && source.kind() == std::io::ErrorKind::NotFound => {}
        other => panic!("the parked open: expected its removal's NotFound, got {other:?}"),
    }
    drop(second_store);
    assert_no_closed_debris(&paths, "after two first opens raced");
    assert_reopens_exactly(&store_dir, &ALL_FAMILIES);
}

/// How large a store a test builds.
#[derive(Clone, Copy, Debug)]
enum StoreSize {
    /// The family's one-fact fixture (`create_current_v2`): what a compaction stages for it fits
    /// in one 64 KiB block.
    Fixture,
    /// The fixture and `LARGE_STORE_RECORDS` more keys, each with a 200-byte value, member or
    /// entry: what a compaction stages for it spans many 64 KiB blocks.
    Large,
}

/// The records `StoreSize::Large` adds to the fixture.
const LARGE_STORE_RECORDS: usize = 3_000;

/// The size of a block in which D3 reads a staged file.
const STAGED_READ_BLOCK: usize = 64 * 1024;

/// A closed `family` store in `store_dir` of `size`.
fn store_of_size(store_dir: &Path, family: FixtureFamily, size: StoreSize) {
    std::fs::create_dir_all(store_dir).unwrap();
    create_current_v2(store_dir, family);
    if let StoreSize::Large = size {
        let key = |index: usize| format!("key-{index:06}").into_bytes();
        match family {
            FixtureFamily::KeyValue => {
                let store = crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir)
                    .unwrap()
                    .into_store();
                for index in 0..LARGE_STORE_RECORDS {
                    store.try_put(key(index), vec![b'v'; 200]).unwrap();
                }
            }
            FixtureFamily::KeySet => {
                let store = crate::key_set_store::DurableKeySetStore::try_init_new(store_dir)
                    .unwrap()
                    .into_store();
                for index in 0..LARGE_STORE_RECORDS {
                    store.append(key(index), vec![b'm'; 200]);
                }
            }
            FixtureFamily::KeyMap => {
                let store = crate::key_map_store::DurableKeyMapStore::try_init_new(store_dir)
                    .unwrap()
                    .into_store();
                for index in 0..LARGE_STORE_RECORDS {
                    store.put(
                        key(index),
                        crate::model::SearchKey::from(index),
                        vec![b'v'; 200],
                    );
                }
            }
        }
        crate::test_support::maintenance_fixtures::forget_inner_lock(store_dir);
    }
}

/// A one-family store of `size` in a fresh parent directory, written by an earlier process and
/// closed.
fn store_sized(family: FixtureFamily, size: StoreSize) -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    store_of_size(&store_dir, family, size);
    (root, store_dir)
}

/// Requires `staged` to span several read blocks when the store is `Large`.
fn assert_spans_blocks_when_large(staged: &[u8], size: StoreSize, label: &str) {
    if let StoreSize::Large = size {
        assert!(
            staged.len() > 4 * STAGED_READ_BLOCK,
            "{label}: the staged file must span several blocks: {} bytes",
            staged.len()
        );
    }
}

/// One fact of a state changed to another whose encoding has the same length.
#[derive(Clone, Copy, Debug)]
enum SameLength {
    /// The last-sorting key's value, its last-sorting set member, or its last-sorting map entry's
    /// value.
    Fact,
    /// The last-sorting key itself, renamed.
    Key,
    /// The last-sorting map entry's search key (sorted maps only).
    EntryKey,
    /// The value of the key sorting in the middle, its last-sorting set member, or its
    /// last-sorting map entry's value (final review; large stores only): the difference lies in a
    /// middle 64 KiB block of the staged file, neither its first nor its last.
    MiddleFact,
}

/// `bytes` with its last byte one greater: the same length, and, for the last-sorting bytes of a
/// collection, absent from it.
fn bumped(bytes: &[u8]) -> Vec<u8> {
    let mut bumped = bytes.to_vec();
    let last = bumped.last_mut().unwrap();
    assert!(*last < u8::MAX, "the fixture's bytes must be bumpable");
    *last += 1;
    bumped
}

/// The key in the middle of `keys` in sorted order, which is the order the snapshot encoder
/// writes them in.
fn middle_sorting_key<'a>(keys: impl Iterator<Item = &'a Vec<u8>>) -> Vec<u8> {
    let mut keys = keys.collect::<Vec<_>>();
    keys.sort();
    keys[keys.len() / 2].clone()
}

/// What a closed compaction of `store_dir` stages for `family` -- the canonical state, encoded
/// with the canonical chain's own timestamp metadata -- with one fact changed as `change` says,
/// to another of the same encoded length. It is a well-formed snapshot of the same length as
/// what compaction stages, and differs from it only inside the last-sorting key's records (the
/// middle-sorting key's, for `SameLength::MiddleFact`).
fn staged_with_one_same_length_change(
    store_dir: &Path,
    family: FixtureFamily,
    change: SameLength,
) -> Vec<u8> {
    use super::CapturedLogicalState;
    let canonical = super::inspection::inspect_generation(store_dir).unwrap();
    let inspected = canonical
        .families
        .iter()
        .find(|inspected| inspected.family.active_name() == active_name(family))
        .unwrap();
    let (mut state, granularity, last_bucket) =
        super::recovered_family_state(store_dir, inspected).unwrap();
    match (&mut state, change) {
        (CapturedLogicalState::Value(snapshot), SameLength::Fact) => {
            let key = snapshot.keys().max().unwrap().clone();
            let value = bumped(&snapshot[&key]);
            snapshot.insert(key, value);
        }
        (CapturedLogicalState::Value(snapshot), SameLength::Key) => {
            let key = snapshot.keys().max().unwrap().clone();
            let value = snapshot.remove(&key).unwrap();
            snapshot.insert(bumped(&key), value);
        }
        (CapturedLogicalState::Value(snapshot), SameLength::MiddleFact) => {
            let key = middle_sorting_key(snapshot.keys());
            let value = bumped(&snapshot[&key]);
            snapshot.insert(key, value);
        }
        (CapturedLogicalState::Set(snapshot), SameLength::Fact) => {
            let key = snapshot.keys().max().unwrap().clone();
            let members = snapshot.get_mut(&key).unwrap();
            let member = members.iter().max().unwrap().clone();
            members.remove(&member);
            members.insert(bumped(&member));
        }
        (CapturedLogicalState::Set(snapshot), SameLength::Key) => {
            let key = snapshot.keys().max().unwrap().clone();
            let members = snapshot.remove(&key).unwrap();
            snapshot.insert(bumped(&key), members);
        }
        (CapturedLogicalState::Set(snapshot), SameLength::MiddleFact) => {
            let key = middle_sorting_key(snapshot.keys());
            let members = snapshot.get_mut(&key).unwrap();
            let member = members.iter().max().unwrap().clone();
            members.remove(&member);
            members.insert(bumped(&member));
        }
        (CapturedLogicalState::Map(snapshot), SameLength::Fact) => {
            let key = snapshot.keys().max().unwrap().clone();
            let entries = snapshot.get_mut(&key).unwrap();
            let (_, value) = entries.iter_mut().next_back().unwrap();
            *value = bumped(value);
        }
        (CapturedLogicalState::Map(snapshot), SameLength::Key) => {
            let key = snapshot.keys().max().unwrap().clone();
            let entries = snapshot.remove(&key).unwrap();
            snapshot.insert(bumped(&key), entries);
        }
        (CapturedLogicalState::Map(snapshot), SameLength::MiddleFact) => {
            let key = middle_sorting_key(snapshot.keys());
            let entries = snapshot.get_mut(&key).unwrap();
            let (_, value) = entries.iter_mut().next_back().unwrap();
            *value = bumped(value);
        }
        (CapturedLogicalState::Map(snapshot), SameLength::EntryKey) => {
            let key = snapshot.keys().max().unwrap().clone();
            let entries = snapshot.get_mut(&key).unwrap();
            let (search_key, value) = entries.pop_last().unwrap();
            let crate::model::Key::USIZE(index) = search_key.first().unwrap().clone() else {
                panic!("the fixture's search keys are numbers");
            };
            entries.insert(crate::model::SearchKey::from(index + 1), value);
        }
        (_, SameLength::EntryKey) => unreachable!("only a sorted map has entry keys"),
    }
    super::encode_captured_state(&state, granularity, last_bucket).unwrap()
}

/// specs/015 FR-3 (fifth review, probes): a staged file is removed only when it is byte for byte
/// what a closed compaction of the canonical directory stages, or the first bytes of that ending
/// inside its header or one of its records, and every byte counts, not only its length and its
/// first bytes. Each staged file here is a well-formed snapshot of the same length
/// as what compaction stages, holding one value, set member, map entry or key the canonical
/// directory lacks, of which it may be the only copy; in a large store, the first byte that
/// differs lies past the first 64 KiB block, and (final review) for one case in a middle block,
/// neither the first two nor the last two. Each keeps the error it returned at `1eb9de5` and
/// changes nothing.
#[test]
fn a_staging_differing_in_one_fact_of_the_same_length_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        for size in [StoreSize::Fixture, StoreSize::Large] {
            let mut changes = match family {
                FixtureFamily::KeyMap => {
                    vec![SameLength::Fact, SameLength::Key, SameLength::EntryKey]
                }
                _ => vec![SameLength::Fact, SameLength::Key],
            };
            if let StoreSize::Large = size {
                changes.push(SameLength::MiddleFact);
            }
            for change in &changes {
                let label = format!("{family:?} {size:?} {change:?}");
                let (root, store_dir) = store_sized(family, size);
                let expected = compaction_staged_file(&store_dir, family);
                let staged = staged_with_one_same_length_change(&store_dir, family, *change);
                assert_eq!(staged.len(), expected.len(), "{label}: the same length");
                assert_spans_blocks_when_large(&staged, size, &label);
                let first_difference = staged
                    .iter()
                    .zip(&expected)
                    .position(|(staged, expected)| staged != expected)
                    .unwrap_or_else(|| panic!("{label}: the staged file must differ"));
                assert!(
                    first_difference >= crate::wal::format::V2CodecProbe::HEADER_LEN,
                    "{label}: the difference must lie past the header"
                );
                if let StoreSize::Large = size {
                    assert!(
                        first_difference > STAGED_READ_BLOCK,
                        "{label}: the difference must lie past the first block: \
                         {first_difference}"
                    );
                }
                if let SameLength::MiddleFact = change {
                    let end_of_difference = staged.len()
                        - staged
                            .iter()
                            .rev()
                            .zip(expected.iter().rev())
                            .position(|(staged, expected)| staged != expected)
                            .unwrap();
                    assert!(
                        first_difference > 2 * STAGED_READ_BLOCK
                            && end_of_difference + 2 * STAGED_READ_BLOCK < staged.len(),
                        "{label}: the difference must lie in a middle block: \
                         [{first_difference}, {end_of_difference}) of {}",
                        staged.len()
                    );
                }
                stage_one_file(&closed_paths(&store_dir), family, &staged);
                assert_refused_unchanged(
                    root.path(),
                    &store_dir,
                    family,
                    &format!("{label}: a staged fact the canonical directory lacks"),
                    undetermined,
                );
            }
        }
    }
}

/// FR-3 (fifth review, probes): a store whose staged file spans many 64 KiB blocks -- the size of
/// a real store, which every other kill test is not -- killed at each cut before `Prepared`, for
/// each family, reopens `Recovered` with its exact state and no maintenance artifact.
#[test]
fn every_pre_prepared_cut_of_a_store_larger_than_a_block_reopens_the_untouched_source() {
    let mut refused = Vec::new();
    for family in ALL_FAMILIES {
        for cut in PRE_PREPARED_CUTS {
            let label = format!("{family:?} {cut:?}");
            let (root, store_dir) = store_sized(family, StoreSize::Large);
            let (root, store_dir, before) =
                kill_before_prepared(root, store_dir, cut, &format!("{family:?}"));
            let paths = closed_paths(&store_dir);
            if let Ok(staged) = std::fs::read(paths.staging.join(active_name(family))) {
                if !staged.is_empty() {
                    assert_spans_blocks_when_large(&staged, StoreSize::Large, &label);
                }
            }
            match open_family(&store_dir, family) {
                Ok(crate::RecoveryStatus::Recovered) => {}
                other => {
                    refused.push(format!("{label}: {other:?}"));
                    continue;
                }
            }
            assert_no_closed_debris(&paths, &label);
            assert_eq!(
                without_open_locks(namespace(root.path())),
                before,
                "{label}: the store's files changed"
            );
            assert_three_reopens(&store_dir, family);
        }
    }
    assert!(
        refused.is_empty(),
        "large stores killed before Prepared did not reopen Recovered:\n{}",
        refused.join("\n")
    );
}

/// What `one/` holds in `every_read_of_the_discards_proof_is_in_the_directory_it_locked`, beside
/// `two/`, which the caller's path names from the moment the discard has found it naming `one/`.
/// In each, a proof read in `two/` would remove `one/`'s staging.
#[derive(Clone, Copy, Debug)]
enum LockedHolds {
    /// A staging holding a fact `one/store` lacks, of which it is the only copy; `two/store` is
    /// byte-identical to `one/store`, and `two/`'s staging is what a compaction of it stages.
    UnprovedStaging,
    /// A previous generation beside what a compaction of `one/store` stages; `two/` holds the
    /// same staging and no previous generation.
    PreviousGeneration,
    /// A staging holding a fact `one/store` lacks; `two/store` holds that fact, and the staging is
    /// what a compaction of `two/store` stages.
    StagingOfAnotherCanonicalDirectory,
}

/// `one/` and `two/` holding `holds`, and `current`, a symlink to `one`; `None` when the platform
/// refuses the link.
fn locked_and_other_directory(root: &Path, holds: LockedHolds) -> Option<PathBuf> {
    let family = FixtureFamily::KeyValue;
    let (one, two) = two_stores(root);
    let with_a_fact = root.join("with-a-fact");
    copy_files(&one, &with_a_fact);
    add_a_fact(&with_a_fact, family, false);
    crate::test_support::maintenance_fixtures::forget_inner_lock(&with_a_fact);
    match holds {
        LockedHolds::UnprovedStaging => {
            stage_one_file(
                &closed_paths(&one),
                family,
                &compaction_staged_file(&with_a_fact, family),
            );
            stage_what_compaction_stages(&two, &closed_paths(&two).staging);
        }
        LockedHolds::PreviousGeneration => {
            stage_what_compaction_stages(&one, &closed_paths(&one).staging);
            copy_files(&one, &closed_paths(&one).previous);
            stage_what_compaction_stages(&two, &closed_paths(&two).staging);
        }
        LockedHolds::StagingOfAnotherCanonicalDirectory => {
            stage_one_file(
                &closed_paths(&one),
                family,
                &compaction_staged_file(&with_a_fact, family),
            );
            std::fs::remove_dir_all(&two).unwrap();
            copy_files(&with_a_fact, &two);
        }
    }
    std::fs::remove_dir_all(&with_a_fact).unwrap();
    if let Err(error) = link_directory(&root.join("one"), &root.join("current")) {
        eprintln!("skipped: this platform refused the link: {error}");
        return None;
    }
    Some(root.join("current/store"))
}

/// specs/015 FR-3 after the fourth review, the proof's half (fifth review, probes): once the
/// discard has found the caller's path naming the directory its open or claim locked, every read
/// its proof makes -- the staging it compares, the canonical directory it compares it with, and
/// whether a previous generation exists -- is in that directory, not in whatever the caller's path
/// names later. With `current` pointed from `one/` to `two/` right after that check, a proof read
/// in `two/` would remove `one/`'s staging, which in each case here must stay; `one/` is left as it
/// was, for an open and for a compaction's start.
#[test]
fn every_read_of_the_discards_proof_is_in_the_directory_it_locked() {
    use crate::compaction::recovery::recovery_pause::{install, Point};
    for holds in [
        LockedHolds::UnprovedStaging,
        LockedHolds::PreviousGeneration,
        LockedHolds::StagingOfAnotherCanonicalDirectory,
    ] {
        for by in [DiscardBy::Open, DiscardBy::CompactionStart] {
            let root = tempfile::tempdir().unwrap();
            let Some(store) = locked_and_other_directory(root.path(), holds) else {
                return;
            };
            let one_before = without_open_locks(namespace(&root.path().join("one")));
            let pause = install(&store, Point::DiscardIdentified);
            let answered = std::thread::scope(|scope| {
                let discarding = scope.spawn(|| discard_by(by, &store));
                pause.wait_reached();
                unlink_directory(&root.path().join("current"));
                link_directory(&root.path().join("two"), &root.path().join("current")).unwrap();
                pause.release();
                discarding.join().unwrap()
            });
            drop(pause);
            assert_eq!(
                without_open_locks(namespace(&root.path().join("one"))),
                one_before,
                "{holds:?} {by:?}: the discard removed one/'s staging on a proof read in two/ \
                 ({answered:?})"
            );
        }
    }
}

/// Names the parent of `one/` and `two/` for the rename child below.
const PUBLICATION_RENAME_CHILD_ENV: &str = "PIGMENT_DB_TEST_PUBLICATION_RENAME_ROOT";

/// The unit-test child for the test below: publishes a closed `Prepared` for `store` relative to
/// `one/`, moves the process's working directory to `two/` once the temporary is written, and
/// lets the publication go on to its rename.
#[test]
fn publication_rename_working_directory_child() {
    use crate::compaction::publication::{
        publish_manifest_buffered_with_checkpoint, ManifestPublishStage,
    };
    let Some(root) = std::env::var_os(PUBLICATION_RENAME_CHILD_ENV).map(PathBuf::from) else {
        return;
    };
    let (_, manifest) = closed_prepared_manifest(&root.join("one/store"));
    std::fs::write(
        root.join("encoded"),
        crate::compaction::manifest::encode_manifest(&manifest).unwrap(),
    )
    .unwrap();
    std::env::set_current_dir(root.join("one")).unwrap();
    let relative = closed_paths(Path::new("store"));
    let two = root.join("two");
    let result = publish_manifest_buffered_with_checkpoint(&relative, &manifest, |stage| {
        if stage == ManifestPublishStage::Written {
            std::env::set_current_dir(&two).unwrap();
        }
        Ok(())
    });
    std::process::exit(if result.is_ok() {
        0
    } else {
        crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED
    });
}

/// specs/015 FR-2 (fourth review), the rename's half (fifth review, probes): a publication
/// renames the temporary it created into its own directory's main manifest, whatever the working
/// directory does meanwhile. A publication for `store` relative to `one/`, whose working directory
/// moves to `two/` once its temporary is written, publishes `one/`'s manifest with the bytes it
/// encoded and leaves no temporary, and leaves `two/` -- whose temporary may be another
/// publication's -- byte-identical.
#[test]
fn a_working_directory_change_during_a_publication_renames_only_into_its_own_directory() {
    let root = tempfile::tempdir().unwrap();
    let (one, two) = two_stores(root.path());
    std::fs::write(
        closed_paths(&two).manifest_next,
        b"another publication's temporary",
    )
    .unwrap();
    let two_before = namespace(&root.path().join("two"));
    let code = run_unit_test_child(
        "compaction::unpublished_attempt_tests::publication_rename_working_directory_child",
        PUBLICATION_RENAME_CHILD_ENV,
        root.path(),
    );
    assert_eq!(
        namespace(&root.path().join("two")),
        two_before,
        "a publication in one/ changed two/ (child exit {code:?})"
    );
    assert_eq!(code, Some(0), "the publication must succeed");
    let one_paths = closed_paths(&one);
    assert_eq!(
        std::fs::read(&one_paths.manifest).ok(),
        std::fs::read(root.path().join("encoded")).ok(),
        "one/'s main manifest is not what the publication encoded"
    );
    assert!(
        !exists(&one_paths.manifest_next),
        "the publication left a temporary in one/"
    );
}

/// specs/015 FR-3 (fifth review): a staging write cut at a record boundary leaves a complete
/// snapshot of a state with fewer facts, which records the rest as deleted. A large store's staged
/// file cut at a record boundary past its first 64 KiB block keeps the error it returned at
/// `1eb9de5` and changes nothing, for each family.
#[test]
fn a_staging_cut_at_a_record_boundary_past_the_first_block_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        let (root, store_dir) = store_sized(family, StoreSize::Large);
        let mut staged = compaction_staged_file(&store_dir, family);
        let cut = *record_boundaries(&staged)
            .iter()
            .find(|boundary| **boundary > 2 * STAGED_READ_BLOCK)
            .unwrap();
        assert!(cut < staged.len());
        staged.truncate(cut);
        stage_one_file(&closed_paths(&store_dir), family, &staged);
        assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            &format!("{family:?}: a staged file cut at a record boundary at {cut}"),
            undetermined,
        );
    }
}

/// The size of a memory page on every system CI runs: a process killed inside one large `write`
/// leaves a file whose length is a multiple of it, or of a larger page-cache folio.
const PAGE: usize = 4096;

/// specs/015 FR-3, a residual pinned (final review; plan.md, "Not done here"): a process killed
/// inside a staging write's one large `write` system call does not leave a prefix of any length.
/// It leaves one whose length is a multiple of the page size, or of a larger page-cache folio (the
/// final review measured multiples of 2 MiB for every one of six kills on Linux ext4). When a
/// store's staged records all have one length, a fixed share of those multiples are record
/// boundaries: here, 96-byte records after the 64-byte header, so one page multiple in three. A
/// cut there is a complete snapshot of fewer facts, which records the rest as deleted, and keeps
/// the error it returned at `1eb9de5` with every artifact; a cut at any other page multiple ends
/// inside a record and is removed. Both halves are today's rule, pinned so that the rate at which
/// a real kill leaves the store unopenable is visible rather than read as a rare coincidence.
#[test]
fn a_staging_write_killed_on_a_page_boundary_keeps_its_error_only_where_a_record_ends() {
    use std::collections::BTreeSet;
    let family = FixtureFamily::KeyValue;
    let scratch = tempfile::tempdir().unwrap();
    let source = scratch.path().join("store");
    std::fs::create_dir(&source).unwrap();
    {
        let store = crate::key_value_store::DurableKeyValueStore::try_init_new(&source)
            .unwrap()
            .into_store();
        // Six-byte keys and eight-byte values: records of one length.
        for index in 0_u64..290 {
            store
                .try_put(
                    index.to_be_bytes()[2..].to_vec(),
                    index.to_le_bytes().to_vec(),
                )
                .unwrap();
        }
    }
    crate::test_support::maintenance_fixtures::forget_inner_lock(&source);
    let expected = compaction_staged_file(&source, family);
    let boundaries = record_boundaries(&expected);
    assert_eq!(
        boundaries
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([96]),
        "the fixture's staged records all have one length"
    );
    let page_cuts = (PAGE..expected.len()).step_by(PAGE).collect::<Vec<_>>();
    let on_a_boundary = page_cuts
        .iter()
        .filter(|cut| boundaries.contains(cut))
        .count();
    assert_eq!(
        (on_a_boundary, page_cuts.len()),
        (2, 6),
        "one page multiple in three ends a record: {page_cuts:?}"
    );
    for cut in page_cuts {
        let label = format!("a staged file cut at the page multiple {cut}");
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        copy_files(&source, &store_dir);
        let paths = closed_paths(&store_dir);
        stage_one_file(&paths, family, &expected[..cut]);
        if boundaries.contains(&cut) {
            assert_refused_unchanged(
                root.path(),
                &store_dir,
                family,
                &format!("{label}, a record boundary"),
                undetermined,
            );
            continue;
        }
        let mut expected_namespace = without_open_locks(namespace(root.path()));
        expected_namespace.retain(|path, _| !path.starts_with(paths.staging.file_name().unwrap()));
        let opened = open_family(&store_dir, family);
        assert!(
            matches!(opened, Ok(crate::RecoveryStatus::Recovered)),
            "{label}, inside a record: {opened:?}"
        );
        assert_no_closed_debris(&paths, &label);
        assert_eq!(
            without_open_locks(namespace(root.path())),
            expected_namespace,
            "{label}: the open changed more than the staging"
        );
        let store = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
            .unwrap()
            .into_store();
        assert_eq!(store.size(), 290, "{label}");
        for index in [0_u64, 289] {
            assert_eq!(
                store.get(&index.to_be_bytes()[2..]),
                Some(index.to_le_bytes().to_vec()),
                "{label}"
            );
        }
    }
}

/// One legacy-format (headerless) record: what `wal::replay` writes for a legacy snapshot.
fn legacy_record(action: &crate::wal::model::StoredAction, crc: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&action.act_type().to_ne_bytes());
    bytes.extend_from_slice(&crc.to_ne_bytes());
    bytes.extend_from_slice(&u32::try_from(action.data().len()).unwrap().to_ne_bytes());
    bytes.extend_from_slice(action.data());
    bytes.extend_from_slice(&action.start_offset().to_ne_bytes());
    bytes
}

/// Appends `action` to `bytes` as a legacy record at its offset, with its own checksum, or with a
/// wrong one when `damaged`.
fn push_legacy(
    bytes: &mut Vec<u8>,
    action: impl FnOnce(&u32) -> crate::wal::model::StoredAction,
    damaged: bool,
) {
    let action = action(&u32::try_from(bytes.len()).unwrap());
    let crc = *action.crc() ^ u32::from(damaged);
    bytes.extend(legacy_record(&action, crc));
}

/// specs/015 FR-3 (fifth review): a staged file shorter than a header holds nothing only when it
/// is the first bytes of what a closed compaction stages for its family: a staging write killed
/// before its first byte or inside its header. A headerless legacy-format file of under 64 bytes
/// holding intact records -- a put and the delete after it, which replay to no key, or a put
/// before a record the replay rejects -- is not what compaction writes, and may be the only copy
/// of those records. The open keeps the error it returned at `1eb9de5` and changes nothing.
#[test]
fn a_short_staged_file_holding_records_compaction_never_writes_keeps_its_error_and_its_bytes() {
    use crate::wal::model::{KeyValueData, StoredAction};
    let put = |offset: &u32| {
        StoredAction::put_action(offset, &KeyValueData::new(b"zz".to_vec(), b"y".to_vec()))
    };
    let mut put_then_delete = Vec::new();
    push_legacy(&mut put_then_delete, put, false);
    push_legacy(
        &mut put_then_delete,
        |offset| StoredAction::delete_action(offset, b"zz"),
        false,
    );
    let mut put_then_rejected = Vec::new();
    push_legacy(&mut put_then_rejected, put, false);
    push_legacy(
        &mut put_then_rejected,
        |offset| StoredAction::delete_action(offset, b"zz"),
        true,
    );
    push_legacy(
        &mut put_then_rejected,
        |offset| StoredAction::delete_action(offset, b"qq"),
        false,
    );
    let member = || KeyValueData::new(b"z".to_vec(), b"y".to_vec());
    let mut append_then_remove = Vec::new();
    push_legacy(
        &mut append_then_remove,
        |offset| StoredAction::append_to_set(offset, &member()),
        false,
    );
    push_legacy(
        &mut append_then_remove,
        |offset| StoredAction::remove_from_set(offset, &member()),
        false,
    );
    let mut append_then_rejected = Vec::new();
    push_legacy(
        &mut append_then_rejected,
        |offset| StoredAction::append_to_set(offset, &member()),
        false,
    );
    push_legacy(
        &mut append_then_rejected,
        |offset| StoredAction::delete_action(offset, b"zz"),
        true,
    );
    push_legacy(
        &mut append_then_rejected,
        |offset| StoredAction::delete_action(offset, b"qq"),
        false,
    );
    let entry = || {
        crate::model::SortedMapEntry::new(Vec::new(), crate::model::SearchKey::from(1), Vec::new())
    };
    let mut entry_then_rejected = Vec::new();
    push_legacy(
        &mut entry_then_rejected,
        |offset| StoredAction::put_to_sorted_map(offset, &entry()),
        false,
    );
    push_legacy(
        &mut entry_then_rejected,
        |offset| StoredAction::delete_action(offset, b""),
        true,
    );
    let cases = [
        (
            FixtureFamily::KeyValue,
            "a put, then its delete",
            put_then_delete,
        ),
        (
            FixtureFamily::KeyValue,
            "a put, then a record the replay rejects",
            put_then_rejected,
        ),
        (
            FixtureFamily::KeySet,
            "an append, then its removal",
            append_then_remove,
        ),
        (
            FixtureFamily::KeySet,
            "an append, then a record the replay rejects",
            append_then_rejected,
        ),
        (
            FixtureFamily::KeyMap,
            "an entry, then a record the replay rejects",
            entry_then_rejected,
        ),
    ];
    let mut accepted = Vec::new();
    for (family, label, staged) in cases {
        let label = format!(
            "{family:?}: {label} ({} bytes; replay rejects it: {}, recovers a key: {})",
            staged.len(),
            replay_rejects(family, &staged),
            replays_to_a_fact(family, &staged)
        );
        assert!(
            staged.len() < crate::wal::format::V2CodecProbe::HEADER_LEN,
            "{label}: the file must be shorter than a header"
        );
        let (root, store_dir) = store_with(&[family], false);
        let paths = closed_paths(&store_dir);
        stage_one_file(&paths, family, &staged);
        let before = without_open_locks(namespace(root.path()));
        match open_family(&store_dir, family) {
            Err(crate::RecoveryError::InvalidArtifact { path }) if path == paths.staging => {}
            other => {
                accepted.push(format!("{label}: {other:?}"));
                continue;
            }
        }
        assert_eq!(
            without_open_locks(namespace(root.path())),
            before,
            "{label}: a refused open changed the directory"
        );
    }
    assert!(
        accepted.is_empty(),
        "short staged files compaction never writes did not keep their error:\n{}",
        accepted.join("\n")
    );
}

/// Changes the file at `path` while keeping its length and its CRC-32: its middle byte flipped,
/// and four bytes beside it solved over GF(2) so that the CRC-32 is what it was.
fn damage_keeping_length_and_crc(path: &Path) {
    let original = std::fs::read(path).unwrap();
    let original_crc = crc32fast::hash(&original);
    let mut bytes = original.clone();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    let solved = if middle + 9 <= bytes.len() {
        middle + 4
    } else {
        middle - 8
    };
    bytes[solved..solved + 4].fill(0);
    let base = crc32fast::hash(&bytes);
    // The CRC-32 is affine in the message bits: column `bit` is what flipping that bit of the four
    // solved bytes does to it.
    let mut columns = [0_u32; 32];
    for (bit, column) in columns.iter_mut().enumerate() {
        bytes[solved + bit / 8] ^= 1 << (bit % 8);
        *column = crc32fast::hash(&bytes) ^ base;
        bytes[solved + bit / 8] ^= 1 << (bit % 8);
    }
    let target = original_crc ^ base;
    // One row per CRC bit: the 32 coefficients, and the right-hand side in bit 32.
    let mut rows = (0..32)
        .map(|row| {
            let coefficients = (0..32)
                .filter(|column| (columns[*column] >> row) & 1 == 1)
                .fold(0_u64, |row, column| row | (1 << column));
            coefficients | (u64::from((target >> row) & 1) << 32)
        })
        .collect::<Vec<_>>();
    let mut pivots = Vec::new();
    for column in 0..32 {
        let next = pivots.len();
        let Some(found) = (next..32).find(|row| (rows[*row] >> column) & 1 == 1) else {
            continue;
        };
        rows.swap(next, found);
        for row in 0..32 {
            if row != next && (rows[row] >> column) & 1 == 1 {
                rows[row] ^= rows[next];
            }
        }
        pivots.push((next, column));
    }
    assert_eq!(pivots.len(), 32, "four bytes reach every CRC-32");
    for (row, column) in pivots {
        if (rows[row] >> 32) & 1 == 1 {
            bytes[solved + column / 8] ^= 1 << (column % 8);
        }
    }
    assert_eq!(bytes.len(), original.len());
    assert_eq!(
        crc32fast::hash(&bytes),
        original_crc,
        "the CRC-32 is unchanged"
    );
    assert_ne!(bytes, original, "the bytes changed");
    std::fs::write(path, bytes).unwrap();
}

/// Where `a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging` damages
/// the source, and when.
#[derive(Clone, Copy, Debug)]
enum CrcPreservingDamage {
    /// The active file of a one-family store, once its staging has been validated.
    ActiveFile,
    /// The first sealed segment of a segmented one-family store, once its staging has been
    /// validated (final review): every captured file is compared, not only the active one.
    SealedSegment,
    /// One family's active file in a store of all three, once its staging has been validated
    /// (final review): every family is compared, not only the first one captured.
    OneOfThreeFamilies,
    /// The active file of a one-family store, while the attempt is parked just after creating its
    /// staging directory and before its guard exists (final review): the guard compares the bytes
    /// the attempt captured, not the source as it was when the guard was armed.
    ActiveFileBeforeTheGuard,
}

/// specs/015 FR-1 as amended by the first review (fifth review): the attempt's guard keeps its
/// staging unless the canonical directory is exactly the generation it captured, byte for byte.
/// Damage under the claim that keeps each file's length and CRC-32 -- which the attempt's own
/// revalidation, comparing exact bytes, has just found -- leaves the staging, the only intact copy
/// of the captured state, and the next open names both, as at `1eb9de5`. For each family, and
/// (final review) whichever captured file is damaged -- a sealed segment, or the active file of a
/// family captured after another -- and whenever: before the guard exists too, so that what it
/// compares with is the capture.
#[test]
fn a_compaction_whose_source_was_damaged_keeping_length_and_crc_keeps_its_staging() {
    use crate::test_support::fault_checkpoint::MaintenanceCut;
    let cases = ALL_FAMILIES.into_iter().flat_map(|family| {
        [
            CrcPreservingDamage::ActiveFile,
            CrcPreservingDamage::SealedSegment,
            CrcPreservingDamage::OneOfThreeFamilies,
            CrcPreservingDamage::ActiveFileBeforeTheGuard,
        ]
        .map(|damage| (family, damage))
    });
    let mut removed = Vec::new();
    for (family, damage) in cases {
        let label = format!("{family:?} {damage:?}");
        let (families, segmented, damaged, cut) = match damage {
            CrcPreservingDamage::ActiveFile => (
                vec![family],
                false,
                active_name(family).to_owned(),
                MaintenanceCut::StagingValidate,
            ),
            CrcPreservingDamage::SealedSegment => (
                vec![family],
                true,
                sealed_name(family, 0),
                MaintenanceCut::StagingValidate,
            ),
            CrcPreservingDamage::OneOfThreeFamilies => (
                ALL_FAMILIES.to_vec(),
                false,
                active_name(family).to_owned(),
                MaintenanceCut::StagingValidate,
            ),
            CrcPreservingDamage::ActiveFileBeforeTheGuard => (
                vec![family],
                false,
                active_name(family).to_owned(),
                MaintenanceCut::StagingCreate,
            ),
        };
        let (root, store_dir) = store_with(&families, segmented);
        let paths = closed_paths(&store_dir);
        let damaged = store_dir.join(damaged);
        assert!(damaged.is_file(), "{label}: the damaged file exists");
        change_the_source_under_the_claim_at(&store_dir, cut, || {
            damage_keeping_length_and_crc(&damaged);
        });
        if !paths.staging.is_dir() {
            removed.push(label);
            continue;
        }
        let staging = paths.staging.clone();
        let canonical = store_dir.clone();
        assert_refused_unchanged(
            root.path(),
            &store_dir,
            family,
            &format!("{label}: source damaged under the claim, keeping its length and CRC-32"),
            move |error| {
                matches!(
                    error,
                    crate::RecoveryError::AuthorityUndetermined {
                        active_path: Some(active),
                        recovery_path: Some(recovery),
                    } if *active == canonical && *recovery == staging
                )
            },
        );
    }
    assert!(
        removed.is_empty(),
        "compactions whose source changed in its bytes, but not in its length or its CRC-32, \
         removed their staging, the only intact copy of the captured state:\n{}",
        removed.join("\n")
    );
}

/// The frozen debris fixture (`tests/fixtures/unpublished_closed_staging`, written by `1eb9de5`):
/// each file's directory (`store` or `staging`), name and bytes.
const FROZEN_DEBRIS: [(&str, &str, &[u8]); 10] = [
    (
        "store",
        "kv.wal.dat",
        include_bytes!("../../tests/fixtures/unpublished_closed_staging/store/kv.wal.dat"),
    ),
    (
        "store",
        "kv.wal.dat.segment-00000000000000000000",
        include_bytes!(
            "../../tests/fixtures/unpublished_closed_staging/store/kv.wal.dat.segment-00000000000000000000"
        ),
    ),
    (
        "store",
        "set.wal.dat",
        include_bytes!("../../tests/fixtures/unpublished_closed_staging/store/set.wal.dat"),
    ),
    (
        "store",
        "set.wal.dat.segment-00000000000000000000",
        include_bytes!(
            "../../tests/fixtures/unpublished_closed_staging/store/set.wal.dat.segment-00000000000000000000"
        ),
    ),
    (
        "store",
        "set.wal.dat.segment-00000000000000000001",
        include_bytes!(
            "../../tests/fixtures/unpublished_closed_staging/store/set.wal.dat.segment-00000000000000000001"
        ),
    ),
    (
        "store",
        "map.wal.dat",
        include_bytes!("../../tests/fixtures/unpublished_closed_staging/store/map.wal.dat"),
    ),
    (
        "store",
        "map.wal.dat.segment-00000000000000000000",
        include_bytes!(
            "../../tests/fixtures/unpublished_closed_staging/store/map.wal.dat.segment-00000000000000000000"
        ),
    ),
    (
        "staging",
        "kv.wal.dat",
        include_bytes!("../../tests/fixtures/unpublished_closed_staging/staging/kv.wal.dat"),
    ),
    (
        "staging",
        "set.wal.dat",
        include_bytes!("../../tests/fixtures/unpublished_closed_staging/staging/set.wal.dat"),
    ),
    (
        "staging",
        "map.wal.dat",
        include_bytes!("../../tests/fixtures/unpublished_closed_staging/staging/map.wal.dat"),
    ),
];

/// Requires `store_dir` to hold the frozen debris fixture's logical contents (its README).
fn assert_frozen_debris_contents(store_dir: &Path) {
    use crate::model::SearchKey;
    use std::collections::HashSet;
    let values = crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir)
        .unwrap()
        .into_store();
    assert_eq!(values.size(), 6);
    for (key, value) in [
        (b"aardvark".as_slice(), b"first".as_slice()),
        (b"alpha", b"one"),
        (b"b", b"bee"),
        (b"empty", b""),
        (b"gamma", b"three"),
        (b"zz", b"last"),
    ] {
        assert_eq!(values.get(key), Some(value.to_vec()));
    }
    assert_eq!(values.get(b"beta"), None);
    drop(values);
    let sets = crate::key_set_store::DurableKeySetStore::try_init_new(store_dir)
        .unwrap()
        .into_store();
    assert_eq!(sets.size(), 3);
    for (key, members) in [
        (
            b"group".as_slice(),
            [b"a-long-member".as_slice(), b"b", b"red"].as_slice(),
        ),
        (b"other", &[b"x"]),
        (b"zz", &[b"m1", b"m22"]),
    ] {
        assert_eq!(
            sets.get_hashset(key),
            Some(
                members
                    .iter()
                    .map(|member| member.to_vec())
                    .collect::<HashSet<_>>()
            )
        );
    }
    drop(sets);
    let maps = crate::key_map_store::DurableKeyMapStore::try_init_new(store_dir)
        .unwrap()
        .into_store();
    assert_eq!(maps.size(), 3);
    for (key, entry, value) in [
        (b"book".as_slice(), 1_usize, Some(b"one".as_slice())),
        (b"book", 2, None),
        (b"book", 3, Some(b"three")),
        (b"other", 7, Some(b"seven")),
        (b"other", 9, Some(b"nine")),
        (b"zz", 5, Some(b"five")),
        (b"zz", 6, Some(b"six")),
    ] {
        assert_eq!(
            maps.get_element(key, &SearchKey::from(entry)),
            value.map(<[u8]>::to_vec),
            "{key:?} {entry}"
        );
    }
}

/// specs/015 FR-3 (fifth review): a staging is removed only when each staged file is byte for byte
/// what a closed compaction of the canonical directory stages now, or the first bytes of that
/// ending inside its header or one of its records, so debris left before an upgrade is removed
/// only while the snapshot encoder writes the bytes the earlier revision wrote. The frozen fixture
/// -- a store and what `1eb9de5`'s closed compaction staged for it, both written by `1eb9de5` -- is
/// removed at the first open of each family, which reports `Recovered` and changes nothing else,
/// and every family then holds its exact contents. A change to the encoder's output fails this
/// test instead of silently returning such debris to `1eb9de5`'s error.
#[test]
fn debris_staged_by_an_earlier_revision_is_removed_at_open() {
    for opened in ALL_FAMILIES {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        let paths = closed_paths(&store_dir);
        for directory in [&store_dir, &paths.staging] {
            std::fs::create_dir(directory).unwrap();
        }
        for (directory, name, bytes) in FROZEN_DEBRIS {
            let directory = if directory == "store" {
                &store_dir
            } else {
                &paths.staging
            };
            std::fs::write(directory.join(name), bytes).unwrap();
        }
        let mut expected = namespace(root.path());
        expected.retain(|path, _| !path.starts_with(paths.staging.file_name().unwrap()));
        let opened_as = open_family(&store_dir, opened);
        assert!(
            matches!(opened_as, Ok(crate::RecoveryStatus::Recovered)),
            "{opened:?}: debris staged by 1eb9de5 kept the store closed: {opened_as:?}"
        );
        assert_no_closed_debris(&paths, &format!("{opened:?}"));
        assert_eq!(
            without_open_locks(namespace(root.path())),
            expected,
            "{opened:?}: the open changed more than the staging"
        );
        assert_frozen_debris_contents(&store_dir);
    }
}

/// specs/015 FR-3 and FR-7 (fifth review, documents): a canonical directory the open cannot
/// search makes each staged name's check in it read as unknown, which proves nothing, and the open
/// keeps the error it returned at `1eb9de5` and changes nothing.
#[cfg(unix)]
#[test]
fn a_canonical_directory_that_cannot_be_searched_keeps_its_error_and_its_bytes() {
    use std::os::unix::fs::PermissionsExt;
    let family = FixtureFamily::KeyValue;
    let (root, store_dir) = store_with(&[family], false);
    let paths = closed_paths(&store_dir);
    stage_what_compaction_stages(&store_dir, &paths.staging);
    let before = without_open_locks(namespace(root.path()));
    std::fs::set_permissions(&store_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::symlink_metadata(store_dir.join(active_name(family))).is_ok() {
        std::fs::set_permissions(&store_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!("skipped: this process may search a directory with no permissions");
        return;
    }
    let result = open_family(&store_dir, family);
    std::fs::set_permissions(&store_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        matches!(&result, Err(error) if undetermined(error)),
        "a canonical directory that cannot be searched: expected its current refusal, got \
         {result:?}"
    );
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "a refused open changed the directory"
    );
}
