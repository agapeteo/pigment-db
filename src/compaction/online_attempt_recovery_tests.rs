//! Online maintenance attempts killed part-way, and the exclusion that makes recovering them
//! sound (specs/016). An open of a family is the only live writer of that family on its
//! directory once this process holds the family (FR-1) and specs/011's inner lock, so the two
//! states a killed online attempt leaves -- a family's lone `.manifest.next`, and a finalized
//! `Prepared` whose source is split by the cutover's moves -- are a dead attempt's and recover
//! (FR-2); where specs/011 took no lock they keep their errors (FR-3).
//!
//! Run on every operating system: the recovery moves and removes files, which each filesystem
//! reports and renames in its own way.

use std::path::{Path, PathBuf};

use crate::compaction::publication::manifest_publication_faults::{
    self, Fault, KILLED_INSIDE_PUBLICATION,
};
use crate::compaction::publication::online_source_move_exit::{self, KILLED_INSIDE_SOURCE_MOVES};
use crate::compaction::publication::{
    family_artifact_paths, read_published_manifest, MaintenanceArtifactPaths, ManifestPublishStage,
};
use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::model::SearchKey;
use crate::test_support::maintenance_fixtures::{active_name, FixtureFamily};

use super::unpublished_attempt_tests::{namespace, Namespace};

const ALL_FAMILIES: [FixtureFamily; 3] = [
    FixtureFamily::KeyValue,
    FixtureFamily::KeySet,
    FixtureFamily::KeyMap,
];

/// A small WAL segment, so that every family's source spans several artifacts and a cutover has
/// several moves to be killed between.
fn segmented() -> crate::DurableStoreOptions {
    crate::DurableStoreOptions::default()
        .with_wal_segment_size(crate::WalSegmentSize::try_from(170_u64).unwrap())
}

/// An open store of one family.
#[allow(clippy::enum_variant_names)]
enum Store {
    KeyValue(DurableKeyValueStore<std::fs::File>),
    KeySet(DurableKeySetStore<std::fs::File>),
    KeyMap(DurableKeyMapStore<std::fs::File>),
}

impl Store {
    fn try_open(
        directory: &Path,
        family: FixtureFamily,
    ) -> Result<(Self, crate::RecoveryStatus), crate::RecoveryError> {
        Ok(match family {
            FixtureFamily::KeyValue => {
                let (store, status) = DurableKeyValueStore::try_init_new(directory)?.into_parts();
                (Self::KeyValue(store), status)
            }
            FixtureFamily::KeySet => {
                let (store, status) = DurableKeySetStore::try_init_new(directory)?.into_parts();
                (Self::KeySet(store), status)
            }
            FixtureFamily::KeyMap => {
                let (store, status) = DurableKeyMapStore::try_init_new(directory)?.into_parts();
                (Self::KeyMap(store), status)
            }
        })
    }

    fn try_open_with_options(
        directory: &Path,
        family: FixtureFamily,
        options: crate::DurableStoreOptions,
    ) -> Result<(Self, crate::RecoveryStatus), crate::RecoveryError> {
        Ok(match family {
            FixtureFamily::KeyValue => {
                let (store, status) =
                    DurableKeyValueStore::try_init_new_with_options(directory, options)?
                        .into_parts();
                (Self::KeyValue(store), status)
            }
            FixtureFamily::KeySet => {
                let (store, status) =
                    DurableKeySetStore::try_init_new_with_options(directory, options)?.into_parts();
                (Self::KeySet(store), status)
            }
            FixtureFamily::KeyMap => {
                let (store, status) =
                    DurableKeyMapStore::try_init_new_with_options(directory, options)?.into_parts();
                (Self::KeyMap(store), status)
            }
        })
    }

    fn open(directory: &Path, family: FixtureFamily) -> Self {
        Self::try_open(directory, family)
            .unwrap_or_else(|error| panic!("{family:?}: open failed: {error:?}"))
            .0
    }

    fn open_segmented(directory: &Path, family: FixtureFamily) -> Self {
        match family {
            FixtureFamily::KeyValue => Self::KeyValue(
                DurableKeyValueStore::try_init_new_with_options(directory, segmented())
                    .unwrap()
                    .into_store(),
            ),
            FixtureFamily::KeySet => Self::KeySet(
                DurableKeySetStore::try_init_new_with_options(directory, segmented())
                    .unwrap()
                    .into_store(),
            ),
            FixtureFamily::KeyMap => Self::KeyMap(
                DurableKeyMapStore::try_init_new_with_options(directory, segmented())
                    .unwrap()
                    .into_store(),
            ),
        }
    }

    /// Ten facts, one of them rewritten and one deleted, so that an exact state records a
    /// deletion as well as values.
    fn write_facts(&self) {
        match self {
            Self::KeyValue(store) => {
                for index in 0..10 {
                    store
                        .try_put(
                            format!("key-{index}").into_bytes(),
                            format!("value-{index}").into_bytes(),
                        )
                        .unwrap();
                }
                store
                    .try_put(b"key-0".to_vec(), b"rewritten".to_vec())
                    .unwrap();
                store.try_remove(b"key-1").unwrap();
            }
            Self::KeySet(store) => {
                for index in 0..10 {
                    store
                        .try_append(
                            format!("group-{}", index % 3).into_bytes(),
                            format!("member-{index}").into_bytes(),
                        )
                        .unwrap();
                }
                store
                    .try_remove_from_set(b"group-1".to_vec(), b"member-4".to_vec())
                    .unwrap();
            }
            Self::KeyMap(store) => {
                for index in 0..10 {
                    store
                        .try_put(
                            format!("book-{}", index % 3).into_bytes(),
                            SearchKey::from(index),
                            format!("entry-{index}").into_bytes(),
                        )
                        .unwrap();
                }
                store
                    .try_remove_from_sorted_map(b"book-1".to_vec(), SearchKey::from(4))
                    .unwrap();
            }
        }
    }

    /// One more fact, written after recovery, which must survive a reopen.
    fn write_one_more(&self) {
        match self {
            Self::KeyValue(store) => store.try_put(b"after".to_vec(), b"accepted".to_vec()),
            Self::KeySet(store) => store.try_append(b"after".to_vec(), b"accepted".to_vec()),
            Self::KeyMap(store) => store
                .try_put(b"after".to_vec(), SearchKey::from(0), b"accepted".to_vec())
                .map(|_| ()),
        }
        .unwrap();
    }

    /// Every fact the store holds, through its public reads, one sorted line each.
    fn state(&self) -> Vec<String> {
        let mut lines = Vec::new();
        match self {
            Self::KeyValue(store) => store.for_each_entry(|key, value| {
                lines.push(format!("{key:?} = {value:?}"));
            }),
            Self::KeySet(store) => store.for_each_set(|key, members| {
                let mut members = members.iter().collect::<Vec<_>>();
                members.sort();
                lines.push(format!("{key:?} = {members:?}"));
            }),
            Self::KeyMap(store) => store.for_each_sorted_map(|key, entries| {
                lines.push(format!("{key:?} = {entries:?}"));
            }),
        }
        lines.sort();
        lines
    }

    fn try_compact_online(&self) -> Result<(), crate::CompactionError> {
        let options = crate::OnlineCompactionOptions::default;
        match self {
            Self::KeyValue(store) => store.try_compact_online(options()).map(|_| ()),
            Self::KeySet(store) => store.try_compact_online(options()).map(|_| ()),
            Self::KeyMap(store) => store.try_compact_online(options()).map(|_| ()),
        }
    }
}

fn family_paths(store_dir: &Path, family: FixtureFamily) -> MaintenanceArtifactPaths {
    family_artifact_paths(&store_dir.join(active_name(family))).unwrap()
}

/// A parent directory holding `store`, which an earlier process wrote with ten facts across
/// several WAL segments and closed. Returns the parent, the store and the store's exact state.
fn written_store(family: FixtureFamily) -> (tempfile::TempDir, PathBuf, Vec<String>) {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    let store = Store::open_segmented(&store_dir, family);
    store.write_facts();
    let state = store.state();
    drop(store);
    (root, store_dir, state)
}

/// The namespace without the two lock files an open of `store` takes (specs/011).
fn without_open_locks(mut snapshot: Namespace) -> Namespace {
    snapshot.remove(Path::new(".store.pigment-lock"));
    snapshot.remove(Path::new("store/.pigment-lock"));
    snapshot
}

fn exists(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

fn assert_no_family_debris(paths: &MaintenanceArtifactPaths, label: &str) {
    for path in [
        &paths.manifest,
        &paths.manifest_next,
        &paths.staging,
        &paths.previous,
    ] {
        assert!(!exists(path), "{label}: {} remains", path.display());
    }
}

/// The store reopens three times with exactly `state`, takes a write, compacts online, and keeps
/// the write and `state` across one more reopen.
fn assert_reopens_exactly(store_dir: &Path, family: FixtureFamily, state: &[String], label: &str) {
    for _ in 0..3 {
        let (store, status) = Store::try_open(store_dir, family)
            .unwrap_or_else(|error| panic!("{label}: a reopen failed: {error:?}"));
        assert_eq!(status, crate::RecoveryStatus::Normal, "{label}: a reopen");
        assert_eq!(store.state(), state, "{label}: a reopen's state");
    }
    let store = Store::open(store_dir, family);
    store.write_one_more();
    let expected = store.state();
    assert_ne!(expected, state, "{label}: the write must change the state");
    store
        .try_compact_online()
        .unwrap_or_else(|error| panic!("{label}: a following compaction failed: {error:?}"));
    drop(store);
    let store = Store::open(store_dir, family);
    assert_eq!(
        store.state(),
        expected,
        "{label}: after a following compaction"
    );
}

/// Where the child below is killed.
#[derive(Clone, Copy, Debug)]
enum Kill {
    /// Inside the attempt's first publication, the unfinalized `Prepared`, at a stage before its
    /// rename.
    FirstPublication(ManifestPublishStage),
    /// Inside the cutover, once it has moved this many source artifacts into the previous
    /// directory.
    AfterMoves(usize),
    /// Inside the cutover's `PreviousPublished` publication, once every source artifact moved.
    PreviousPublishedPublication,
    /// Inside the attempt's second publication, the rewrite that finalizes its `Prepared`: the
    /// unfinalized `Prepared` and the staging remain, and no source artifact has moved.
    FinalizeRewrite,
}

impl Kill {
    fn encode(self) -> String {
        match self {
            Self::FirstPublication(stage) => format!("first-publication:{stage:?}"),
            Self::AfterMoves(moves) => format!("after-moves:{moves}"),
            Self::PreviousPublishedPublication => "previous-published-publication".to_owned(),
            Self::FinalizeRewrite => "finalize-rewrite".to_owned(),
        }
    }

    fn decode(encoded: &str) -> Self {
        match encoded.split_once(':') {
            Some(("first-publication", stage)) => Self::FirstPublication(match stage {
                "Created" => ManifestPublishStage::Created,
                "Written" => ManifestPublishStage::Written,
                "Flushed" => ManifestPublishStage::Flushed,
                other => panic!("unknown stage {other}"),
            }),
            Some(("after-moves", moves)) => Self::AfterMoves(moves.parse().unwrap()),
            None if encoded == "previous-published-publication" => {
                Self::PreviousPublishedPublication
            }
            None if encoded == "finalize-rewrite" => Self::FinalizeRewrite,
            _ => panic!("unknown kill point {encoded}"),
        }
    }

    fn exit_code(self) -> i32 {
        match self {
            Self::FirstPublication(_)
            | Self::PreviousPublishedPublication
            | Self::FinalizeRewrite => KILLED_INSIDE_PUBLICATION,
            Self::AfterMoves(_) => KILLED_INSIDE_SOURCE_MOVES,
        }
    }
}

const KILL_STORE_ENV: &str = "PIGMENT_DB_ONLINE_KILL_STORE";
const KILL_FAMILY_ENV: &str = "PIGMENT_DB_ONLINE_KILL_FAMILY";
const KILL_AT_ENV: &str = "PIGMENT_DB_ONLINE_KILL_AT";
/// Set to `physical` for a child whose open, and so whose attempt's manifest, asks for
/// `DurabilityPolicy::Physical`.
const KILL_DURABILITY_ENV: &str = "PIGMENT_DB_ONLINE_KILL_DURABILITY";
/// The child's compaction returned instead of being killed.
const CHILD_SURVIVED: i32 = 93;
/// The child could not open the store it was to compact.
const CHILD_COULD_NOT_OPEN: i32 = 94;

fn family_name(family: FixtureFamily) -> &'static str {
    match family {
        FixtureFamily::KeyValue => "kv",
        FixtureFamily::KeySet => "set",
        FixtureFamily::KeyMap => "map",
    }
}

fn family_named(name: &str) -> FixtureFamily {
    ALL_FAMILIES
        .into_iter()
        .find(|family| family_name(*family) == name)
        .unwrap()
}

/// The unit-test child for the tests below: a process that opens the store it is given, starts
/// an online compaction of one family, and is ended at its kill point, running no destructor.
#[test]
fn killed_online_attempt_child() {
    let Some(store_dir) = std::env::var_os(KILL_STORE_ENV).map(PathBuf::from) else {
        return;
    };
    let family = family_named(&std::env::var(KILL_FAMILY_ENV).unwrap());
    let kill = Kill::decode(&std::env::var(KILL_AT_ENV).unwrap());
    let paths = family_paths(&store_dir, family);
    let _injection = match kill {
        Kill::FirstPublication(stage) => Some(manifest_publication_faults::inject(
            &paths.manifest_next,
            1,
            stage,
            Fault::Exit,
        )),
        // The attempt's third publication: its unfinalized `Prepared`, the finalize rewrite, then
        // `PreviousPublished`.
        Kill::PreviousPublishedPublication => Some(manifest_publication_faults::inject(
            &paths.manifest_next,
            3,
            ManifestPublishStage::Written,
            Fault::Exit,
        )),
        Kill::FinalizeRewrite => Some(manifest_publication_faults::inject(
            &paths.manifest_next,
            2,
            ManifestPublishStage::Created,
            Fault::Exit,
        )),
        Kill::AfterMoves(moves) => {
            online_source_move_exit::inject(&paths.previous, moves);
            None
        }
    };
    let options = match std::env::var(KILL_DURABILITY_ENV).as_deref() {
        Ok("physical") => crate::DurableStoreOptions::default()
            .with_durability_policy(crate::DurabilityPolicy::Physical),
        _ => crate::DurableStoreOptions::default(),
    };
    let Ok((store, _)) = Store::try_open_with_options(&store_dir, family, options) else {
        std::process::exit(CHILD_COULD_NOT_OPEN);
    };
    let result = store.try_compact_online();
    eprintln!("the killed child's compaction returned {result:?}");
    std::process::exit(CHILD_SURVIVED);
}

/// Runs the child above on `store_dir` and requires it to have been killed at `kill`.
fn kill_an_online_attempt(store_dir: &Path, family: FixtureFamily, kill: Kill) {
    kill_an_online_attempt_under(store_dir, family, kill, crate::DurabilityPolicy::Buffered);
}

/// `kill_an_online_attempt`, with the child's open, and so its attempt's manifest, under
/// `durability`.
fn kill_an_online_attempt_under(
    store_dir: &Path,
    family: FixtureFamily,
    kill: Kill,
    durability: crate::DurabilityPolicy,
) {
    let durability = if durability == crate::DurabilityPolicy::Physical {
        "physical"
    } else {
        "default"
    };
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("compaction::online_attempt_recovery_tests::killed_online_attempt_child")
        .arg("--exact")
        .arg("--nocapture")
        .env(KILL_STORE_ENV, store_dir)
        .env(KILL_FAMILY_ENV, family_name(family))
        .env(KILL_AT_ENV, kill.encode())
        .env(KILL_DURABILITY_ENV, durability)
        .status()
        .unwrap();
    assert_eq!(
        status.code(),
        Some(kill.exit_code()),
        "{family:?} {kill:?}: the child was not killed where it was asked to be"
    );
    wait_for_the_dead_childs_locks(store_dir);
}

/// Windows releases the locks of a process that has ended asynchronously (specs/011,
/// Compatibility), so an open made as soon as the child has exited could be refused by the
/// child's own lock. There this waits, at most five seconds, until this process can take each
/// lock file the child may have held, and releases it at once. Elsewhere a lock is gone once its
/// owner has exited, as `tests/directory_lock.rs` relies on, and nothing waits, so a lock left
/// behind still fails the test that meets it.
fn wait_for_the_dead_childs_locks(store_dir: &Path) {
    if !cfg!(windows) {
        return;
    }
    let name = store_dir.file_name().unwrap().to_str().unwrap();
    let locks = [
        store_dir.join(".pigment-lock"),
        store_dir
            .parent()
            .unwrap()
            .join(format!(".{name}.pigment-lock")),
    ];
    let patience = std::time::Duration::from_secs(5);
    let started = std::time::Instant::now();
    for lock in locks {
        while started.elapsed() < patience {
            let free = match std::fs::File::open(&lock) {
                Err(error) => error.kind() == std::io::ErrorKind::NotFound,
                Ok(file) => file.try_lock().is_ok() && file.unlock().is_ok(),
            };
            if free {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

/// How many artifacts the family's source spans: its sealed segments and its active file.
fn source_artifacts(store_dir: &Path, family: FixtureFamily) -> usize {
    let sealed_prefix = format!("{}.segment-", active_name(family).to_str().unwrap());
    let sealed = std::fs::read_dir(store_dir)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&sealed_prefix))
        })
        .count();
    sealed + 1
}

/// A store whose online attempt of `family` was killed inside its cutover after `moves` source
/// moves: a finalized `Prepared` with its source split between the store and the previous
/// directory. Returns the parent, the store, the exact state and the family's paths.
fn split_by_a_killed_cutover(
    family: FixtureFamily,
    moves: usize,
) -> (
    tempfile::TempDir,
    PathBuf,
    Vec<String>,
    MaintenanceArtifactPaths,
) {
    let (root, store_dir, state) = written_store(family);
    kill_an_online_attempt(&store_dir, family, Kill::AfterMoves(moves));
    let paths = family_paths(&store_dir, family);
    let manifest = read_published_manifest(&paths).unwrap().unwrap();
    assert!(
        manifest.source_finalized,
        "{family:?}: a cutover moves only a finalized source"
    );
    assert_eq!(
        std::fs::read_dir(&paths.previous).unwrap().count(),
        moves,
        "{family:?}: the previous directory holds what the cutover moved"
    );
    (root, store_dir, state, paths)
}

/// The names of the source artifacts a split moved into the previous directory, and of those it
/// left in the store, from the finalized manifest.
fn moved_and_unmoved(paths: &MaintenanceArtifactPaths) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let manifest = read_published_manifest(paths).unwrap().unwrap();
    manifest
        .source_inventory
        .iter()
        .map(|descriptor| PathBuf::from(descriptor.relative_path.file_name().unwrap()))
        .partition(|name| exists(&paths.previous.join(name)))
}

fn undetermined(result: &Result<crate::RecoveryStatus, crate::RecoveryError>) -> bool {
    matches!(
        result,
        Err(crate::RecoveryError::AuthorityUndetermined { .. })
    )
}

/// Opens `family` and requires its error from `1eb9de5` (`AuthorityUndetermined`), with the
/// parent directory as it was apart from the open's own lock files.
fn assert_keeps_its_error_and_its_bytes(
    root: &Path,
    store_dir: &Path,
    family: FixtureFamily,
    label: &str,
) {
    let before = without_open_locks(namespace(root));
    let result = Store::try_open(store_dir, family).map(|(_, status)| status);
    assert!(
        undetermined(&result),
        "{label}: expected its error from 1eb9de5, got {result:?}"
    );
    assert_eq!(
        without_open_locks(namespace(root)),
        before,
        "{label}: a refused open changed the directory"
    );
}

/// FR-2: a process killed inside an online attempt's first publication leaves its family's lone
/// `.manifest.next`. The next open removes it, reports `Recovered` with the exact state, and the
/// store reopens, takes writes and compacts as before.
#[test]
fn a_process_killed_inside_its_first_online_publication_reopens_recovered() {
    let mut refused = Vec::new();
    for family in ALL_FAMILIES {
        for stage in [
            ManifestPublishStage::Created,
            ManifestPublishStage::Written,
            ManifestPublishStage::Flushed,
        ] {
            let label = format!("{family:?} killed at {stage:?}");
            let (_root, store_dir, state) = written_store(family);
            kill_an_online_attempt(&store_dir, family, Kill::FirstPublication(stage));
            let paths = family_paths(&store_dir, family);
            assert!(exists(&paths.manifest_next), "{label}: no temporary left");
            for path in [&paths.manifest, &paths.staging, &paths.previous] {
                assert!(!exists(path), "{label}: the kill left {}", path.display());
            }
            let opened = Store::try_open(&store_dir, family);
            let (store, status) = match opened {
                Ok(opened) => opened,
                Err(error) => {
                    refused.push(format!("{label}: {error:?}"));
                    continue;
                }
            };
            assert_eq!(status, crate::RecoveryStatus::Recovered, "{label}");
            assert_eq!(store.state(), state, "{label}");
            drop(store);
            assert_no_family_debris(&paths, &label);
            assert_reopens_exactly(&store_dir, family, &state, &label);
        }
    }
    assert!(
        refused.is_empty(),
        "kills inside a first online publication kept their stores closed:\n{}",
        refused.join("\n")
    );
}

/// FR-2: a process killed inside a cutover's source moves, after each of them, and inside its
/// `PreviousPublished` publication, leaves a finalized `Prepared` whose source is split between
/// the store and the previous directory. The next open restores each moved artifact, verified
/// against the manifest, reports `Recovered` with the exact state, and the store reopens, takes
/// writes and compacts as before.
#[test]
fn a_process_killed_inside_its_cutovers_source_moves_reopens_recovered() {
    let mut refused = Vec::new();
    for family in ALL_FAMILIES {
        let (_root, store_dir, _) = written_store(family);
        let artifacts = source_artifacts(&store_dir, family);
        assert!(
            artifacts >= 3,
            "{family:?}: the source must span several artifacts"
        );
        let kills = (1..=artifacts)
            .map(Kill::AfterMoves)
            .chain([Kill::PreviousPublishedPublication]);
        for kill in kills {
            let label = format!("{family:?} killed {kill:?}");
            let (_root, store_dir, state) = written_store(family);
            kill_an_online_attempt(&store_dir, family, kill);
            let paths = family_paths(&store_dir, family);
            let (moved, _) = moved_and_unmoved(&paths);
            assert!(!moved.is_empty(), "{label}: nothing was moved");
            let opened = Store::try_open(&store_dir, family);
            let (store, status) = match opened {
                Ok(opened) => opened,
                Err(error) => {
                    refused.push(format!("{label}: {error:?}"));
                    continue;
                }
            };
            assert_eq!(status, crate::RecoveryStatus::Recovered, "{label}");
            assert_eq!(store.state(), state, "{label}");
            drop(store);
            assert_no_family_debris(&paths, &label);
            assert_reopens_exactly(&store_dir, family, &state, &label);
        }
    }
    assert!(
        refused.is_empty(),
        "kills inside a cutover's source moves kept their stores closed:\n{}",
        refused.join("\n")
    );
}

/// A family's online leftovers beside directory-level maintenance artifacts keep the open's error
/// and every byte. An open that finds directory-level artifacts holds only specs/011's
/// replacement lock while it recovers, and directory recovery refuses a canonical directory that
/// holds a family's online artifacts before family recovery runs, so FR-2 never acts there
/// (plan.md, Decisions). Each family, beside a lone closed `.manifest.next`.
#[test]
fn online_leftovers_beside_directory_maintenance_keep_the_opens_error_and_their_bytes() {
    for family in ALL_FAMILIES {
        for kill in [
            Kill::FirstPublication(ManifestPublishStage::Written),
            Kill::AfterMoves(1),
        ] {
            let label = format!("{family:?} killed {kill:?}");
            let (root, store_dir, _) = written_store(family);
            kill_an_online_attempt(&store_dir, family, kill);
            let closed =
                crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
            std::fs::write(&closed.manifest_next, b"an unpublished closed Prepared").unwrap();
            let before = without_open_locks(namespace(root.path()));
            let result = Store::try_open(&store_dir, family).map(|(_, status)| status);
            assert!(
                matches!(&result, Err(crate::RecoveryError::InvalidArtifact { path }) if *path == store_dir),
                "{label}: expected directory recovery's refusal, got {result:?}"
            );
            assert_eq!(
                without_open_locks(namespace(root.path())),
                before,
                "{label}: a refused open changed the directory"
            );
        }
    }
}

/// A lone family `.manifest.next` that the rule cannot prove is a dead first publication's keeps
/// its error from `1eb9de5` and its bytes (FR-2's over-reach controls), each family.
#[test]
fn a_family_temporary_that_is_not_provably_lone_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        let planted = |plant: &dyn Fn(&Path, &MaintenanceArtifactPaths), label: &str| {
            let (root, store_dir, _) = written_store(family);
            let paths = family_paths(&store_dir, family);
            plant(&store_dir, &paths);
            assert_keeps_its_error_and_its_bytes(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}: {label}"),
            );
        };
        let temporary = |paths: &MaintenanceArtifactPaths| {
            std::fs::write(&paths.manifest_next, b"an unpublished online Prepared").unwrap();
        };
        planted(
            &|_, paths| {
                temporary(paths);
                std::fs::write(&paths.staging, b"staged").unwrap();
            },
            "a temporary beside family staging",
        );
        planted(
            &|_, paths| {
                temporary(paths);
                std::fs::create_dir(&paths.previous).unwrap();
            },
            "a temporary beside a previous directory",
        );
        planted(
            &|store_dir, paths| {
                temporary(paths);
                let active = store_dir.join(active_name(family));
                let mut bytes = std::fs::read(&active).unwrap();
                let middle = bytes.len() / 2;
                bytes[middle] ^= 0xff;
                std::fs::write(active, bytes).unwrap();
            },
            "a temporary beside a corrupt family",
        );
        planted(
            &|store_dir, paths| {
                temporary(paths);
                for entry in std::fs::read_dir(store_dir).unwrap() {
                    let entry = entry.unwrap();
                    let name = entry.file_name();
                    if name
                        .to_str()
                        .is_some_and(|name| name.starts_with(active_name(family).to_str().unwrap()))
                        && entry.path() != paths.manifest_next
                    {
                        std::fs::remove_file(entry.path()).unwrap();
                    }
                }
            },
            "a temporary beside no family at all",
        );
        planted(
            &|_, paths| {
                temporary(paths);
                std::fs::write(&paths.manifest, b"not a manifest").unwrap();
            },
            "a temporary beside a corrupt family manifest",
        );
        planted(
            &|_, paths| std::fs::create_dir(&paths.manifest_next).unwrap(),
            "a temporary that is a directory",
        );
        let (root, store_dir, _) = written_store(family);
        let paths = family_paths(&store_dir, family);
        let target = root.path().join("a-regular-file");
        std::fs::write(&target, b"an unpublished revision, elsewhere").unwrap();
        match symlink_file(&target, &paths.manifest_next) {
            Ok(()) => assert_keeps_its_error_and_its_bytes(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}: a temporary that is a symlink to a regular file"),
            ),
            Err(error) => eprintln!("symlink control skipped: the platform refused it: {error}"),
        }
        let (root, store_dir, _) = written_store(family);
        let paths = family_paths(&store_dir, family);
        temporary(&paths);
        match symlink_file(&root.path().join("nowhere"), &paths.manifest) {
            Ok(()) => assert_keeps_its_error_and_its_bytes(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}: a temporary beside a family manifest linking to nothing"),
            ),
            Err(error) => eprintln!("symlink control skipped: the platform refused it: {error}"),
        }
    }
}

fn symlink_file(target: &Path, link: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(target, link)
    }
}

/// A split the finalized manifest cannot account for keeps its error from `1eb9de5` and its
/// bytes (FR-2's over-reach controls), each family: every moved artifact must verify against its
/// descriptor and be absent from the store, every unmoved one must verify in the store, and the
/// previous directory may hold nothing else.
#[test]
fn a_split_the_manifest_cannot_account_for_keeps_its_error_and_its_bytes() {
    #[derive(Clone, Copy, Debug)]
    enum Damage {
        MovedArtifactChanged,
        MovedArtifactAlsoInTheStore,
        ForeignFileInPrevious,
        SubdirectoryInPrevious,
        UnmovedArtifactChanged,
        UnmovedArtifactMissing,
    }
    for family in ALL_FAMILIES {
        for damage in [
            Damage::MovedArtifactChanged,
            Damage::MovedArtifactAlsoInTheStore,
            Damage::ForeignFileInPrevious,
            Damage::SubdirectoryInPrevious,
            Damage::UnmovedArtifactChanged,
            Damage::UnmovedArtifactMissing,
        ] {
            let (root, store_dir, _, paths) = split_by_a_killed_cutover(family, 1);
            let (moved, unmoved) = moved_and_unmoved(&paths);
            let flip_last_byte = |path: &Path| {
                let mut bytes = std::fs::read(path).unwrap();
                let last = bytes.len() - 1;
                bytes[last] ^= 0xff;
                std::fs::write(path, bytes).unwrap();
            };
            match damage {
                Damage::MovedArtifactChanged => flip_last_byte(&paths.previous.join(&moved[0])),
                Damage::MovedArtifactAlsoInTheStore => {
                    std::fs::copy(paths.previous.join(&moved[0]), store_dir.join(&moved[0]))
                        .unwrap();
                }
                Damage::ForeignFileInPrevious => {
                    std::fs::write(paths.previous.join("foreign"), b"not a source").unwrap();
                }
                Damage::SubdirectoryInPrevious => {
                    std::fs::create_dir(paths.previous.join("foreign")).unwrap();
                }
                Damage::UnmovedArtifactChanged => {
                    flip_last_byte(&store_dir.join(unmoved.last().unwrap()));
                }
                Damage::UnmovedArtifactMissing => {
                    std::fs::remove_file(store_dir.join(unmoved.last().unwrap())).unwrap();
                }
            }
            assert_keeps_its_error_and_its_bytes(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}: {damage:?}"),
            );
        }
    }
}

/// A split beside an unfinalized `Prepared` keeps its error from `1eb9de5` and its bytes:
/// publication never moves a source artifact under an unfinalized manifest, whose source
/// inventory is only a prefix of a WAL that was still growing. Each family.
#[test]
fn a_split_beside_an_unfinalized_prepared_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        let (root, store_dir, _) = written_store(family);
        kill_an_online_attempt(&store_dir, family, Kill::FinalizeRewrite);
        let paths = family_paths(&store_dir, family);
        let manifest = read_published_manifest(&paths).unwrap().unwrap();
        assert!(
            !manifest.source_finalized,
            "{family:?}: an unfinalized Prepared"
        );
        // The empty temporary the kill left is removed by recovery of any published manifest, so
        // it goes first: what is left is a split the open must refuse without changing anything.
        std::fs::remove_file(&paths.manifest_next).unwrap();
        let first = manifest.source_inventory[0]
            .relative_path
            .file_name()
            .unwrap()
            .to_owned();
        std::fs::create_dir(&paths.previous).unwrap();
        std::fs::rename(store_dir.join(&first), paths.previous.join(&first)).unwrap();
        assert_keeps_its_error_and_its_bytes(
            root.path(),
            &store_dir,
            family,
            &format!("{family:?}: a split beside an unfinalized Prepared"),
        );
    }
}

/// A previous directory that is a symlink to a directory holding the moved artifact is not the
/// cutover's: the split keeps its error from `1eb9de5`, and the link and its target their bytes.
#[test]
fn a_previous_directory_that_is_a_symlink_keeps_its_error_and_its_bytes() {
    for family in ALL_FAMILIES {
        let (root, store_dir, _, paths) = split_by_a_killed_cutover(family, 1);
        let elsewhere = root.path().join("elsewhere");
        std::fs::rename(&paths.previous, &elsewhere).unwrap();
        let linked = {
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(&elsewhere, &paths.previous)
            }
            #[cfg(windows)]
            {
                std::os::windows::fs::symlink_dir(&elsewhere, &paths.previous)
            }
        };
        match linked {
            Ok(()) => assert_keeps_its_error_and_its_bytes(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?}: a previous directory that is a symlink"),
            ),
            Err(error) => eprintln!("symlink control skipped: the platform refused it: {error}"),
        }
    }
}

/// A previous directory the open cannot read proves nothing: the split keeps its error from
/// `1eb9de5`, not the read's I/O error, and its bytes.
#[cfg(unix)]
#[test]
fn a_previous_directory_that_cannot_be_read_keeps_its_error_and_its_bytes() {
    use super::unpublished_attempt_tests::Unreadable;

    for family in ALL_FAMILIES {
        let (root, store_dir, _, paths) = split_by_a_killed_cutover(family, 1);
        let before = without_open_locks(namespace(root.path()));
        let Some(unreadable) = Unreadable::new(&paths.previous) else {
            return;
        };
        let result = Store::try_open(&store_dir, family).map(|(_, status)| status);
        drop(unreadable);
        assert!(
            undetermined(&result),
            "{family:?}: an unreadable previous directory: expected its error from 1eb9de5, got \
             {result:?}"
        );
        assert_eq!(
            without_open_locks(namespace(root.path())),
            before,
            "{family:?}: a refused open changed the directory"
        );
    }
}

/// FR-3: where specs/011 takes no lock (FR-11, simulated by injecting `Unsupported` for both lock
/// files), nothing excludes another process's live online attempt, so a lone family temporary and
/// a split source keep their errors from `1eb9de5` and their bytes, each family.
#[test]
fn without_locks_a_dead_attempts_leftovers_keep_their_errors_and_their_bytes() {
    use crate::maintenance_coordination::lock_seams::inject_lock_error;

    for family in ALL_FAMILIES {
        for kill in [
            Kill::FirstPublication(ManifestPublishStage::Written),
            Kill::AfterMoves(1),
        ] {
            let (root, store_dir, _) = written_store(family);
            kill_an_online_attempt(&store_dir, family, kill);
            inject_lock_error(&store_dir, std::io::ErrorKind::Unsupported);
            inject_lock_error(root.path(), std::io::ErrorKind::Unsupported);
            assert_keeps_its_error_and_its_bytes(
                root.path(),
                &store_dir,
                family,
                &format!("{family:?} killed {kill:?}, opened without locks"),
            );
        }
    }
}

/// FR-1 with FR-2: a second open of a family while the first instance's online attempt is live
/// -- its unfinalized `Prepared` published and its staging written (the first review's H3) -- is
/// refused before recovery, so it cannot abandon that attempt; the attempt then completes and
/// every key survives a reopen. At `1eb9de5` the second open answered `Recovered`, removed the live
/// attempt's manifest and staging, and the attempt's cutover failed.
#[test]
fn a_second_open_during_a_live_online_attempt_is_refused_and_the_attempt_completes() {
    use crate::test_support::maintenance_schedule::MaintenanceObserver;

    let directory = tempfile::tempdir().unwrap();
    let first = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    first
        .try_put(b"stable".to_vec(), b"authority".to_vec())
        .unwrap();
    let capture = first
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    let paths = staged.prepared.paths.clone();
    assert!(exists(&paths.manifest) && exists(&paths.staging));
    let before = namespace(directory.path());
    let second = DurableKeyValueStore::try_init_new(directory.path()).map(|opened| opened.status());
    let after = namespace(directory.path());
    let completed = first.complete_online_cutover_probe(staged);
    assert!(
        matches!(&second, Err(crate::RecoveryError::Io { source, .. }) if source.kind() == std::io::ErrorKind::WouldBlock),
        "a second open during a live online attempt must be refused, got {second:?}"
    );
    assert_eq!(after, before, "the refused open changed the directory");
    completed.expect("the live attempt completes");
    first
        .try_put(b"after".to_vec(), b"accepted".to_vec())
        .unwrap();
    drop(first);
    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"stable"), Some(b"authority".to_vec()));
    assert_eq!(reopened.get(b"after"), Some(b"accepted".to_vec()));
    assert_no_family_debris(&paths, "after the attempt");
}

/// FR-2's restore, interrupted part-way by a kill, leaves a split of another shape: it moves the
/// artifacts back one at a time, in name order, while the cutover moved them in inventory order.
/// The next open completes it from any subset still moved, and from none (every artifact back,
/// the previous directory empty), reporting `Recovered` with the exact state.
#[test]
fn a_restore_interrupted_part_way_is_completed_by_the_next_open() {
    let mut refused = Vec::new();
    for family in ALL_FAMILIES {
        for moved_back in [1, usize::MAX] {
            let label = format!("{family:?}, {moved_back} moved back");
            let (_root, store_dir, state, paths) = split_by_a_killed_cutover(family, 3);
            let (moved, _) = moved_and_unmoved(&paths);
            let mut moved = moved;
            moved.sort();
            for name in moved.iter().take(moved_back) {
                std::fs::rename(paths.previous.join(name), store_dir.join(name)).unwrap();
            }
            match Store::try_open(&store_dir, family) {
                Ok((store, status)) => {
                    assert_eq!(status, crate::RecoveryStatus::Recovered, "{label}");
                    assert_eq!(store.state(), state, "{label}");
                }
                Err(error) => {
                    refused.push(format!("{label}: {error:?}"));
                    continue;
                }
            }
            assert_no_family_debris(&paths, &label);
            assert_reopens_exactly(&store_dir, family, &state, &label);
        }
    }
    assert!(
        refused.is_empty(),
        "restores interrupted part-way kept their stores closed:\n{}",
        refused.join("\n")
    );
}

/// The live-cutover scenarios specs/015's reviews used against the rules it withdrew, each
/// family: while one instance's cutover is parked between its source moves and
/// `PreviousPublished`, a second open of the family is refused before recovery (FR-1), and a
/// second compaction of the instance is refused by its own attempt; neither changes anything, and
/// the cutover completes with the exact state.
#[test]
fn a_live_cutover_is_left_to_complete_whoever_else_opens_or_compacts_its_family() {
    use crate::compaction::publication::online_source_move_pause;

    for family in ALL_FAMILIES {
        let (root, store_dir, state) = written_store(family);
        let first = Store::open(&store_dir, family);
        let paths = family_paths(&store_dir, family);
        let pause = online_source_move_pause::install(&paths.previous);
        // Nothing is asserted while the cutover is parked, so a failure cannot leave it parked.
        let (before, second, compacted_again, after, compacted) = std::thread::scope(|scope| {
            let compaction = scope.spawn(|| first.try_compact_online());
            pause.wait_reached();
            let before = namespace(root.path());
            let second = Store::try_open(&store_dir, family).map(|(_, status)| status);
            let compacted_again = first.try_compact_online();
            let after = namespace(root.path());
            pause.release();
            (
                before,
                second,
                compacted_again,
                after,
                compaction.join().unwrap(),
            )
        });
        drop(pause);
        let label = format!("{family:?}");
        assert!(
            before.contains_key(
                &PathBuf::from("store").join(paths.previous.strip_prefix(&store_dir).unwrap())
            ),
            "{label}: the cutover must be parked after its source moves"
        );
        assert!(
            matches!(&second, Err(crate::RecoveryError::Io { source, .. }) if source.kind() == std::io::ErrorKind::WouldBlock),
            "{label}: a second open during a live cutover must be refused, got {second:?}"
        );
        assert!(
            matches!(
                compacted_again,
                Err(crate::CompactionError::FailedClosed { .. })
            ),
            "{label}: a second compaction during a live cutover must be refused, got \
             {compacted_again:?}"
        );
        assert_eq!(after, before, "{label}: the refusals changed the directory");
        compacted.unwrap_or_else(|error| panic!("{label}: the live cutover failed: {error:?}"));
        assert_eq!(first.state(), state, "{label}: the compacted instance");
        drop(first);
        assert_no_family_debris(&paths, &label);
        assert_reopens_exactly(&store_dir, family, &state, &label);
    }
}

/// FR-2's rules act only in the directory the open locked, resolved once, and only while the
/// caller's path still names it, as specs/015's closed discard does. An open of `current/store`,
/// with `current -> one`, locks `one/store`; while it is parked before family recovery, `current`
/// is pointed at `two/`, whose store holds a dead attempt's leftovers too. The rules then act in
/// neither directory, and the open answers as at `1eb9de5`. Each family, each leftover.
#[test]
fn a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked() {
    use super::recovery::recovery_pause::{install, Point};

    for family in ALL_FAMILIES {
        for kill in [
            Kill::FirstPublication(ManifestPublishStage::Written),
            Kill::AfterMoves(1),
        ] {
            let label = format!("{family:?} killed {kill:?}");
            let root = tempfile::tempdir().unwrap();
            for side in ["one", "two"] {
                let store_dir = root.path().join(side).join("store");
                std::fs::create_dir_all(&store_dir).unwrap();
                let store = Store::open_segmented(&store_dir, family);
                store.write_facts();
                drop(store);
                kill_an_online_attempt(&store_dir, family, kill);
            }
            let current = root.path().join("current");
            if let Err(error) = link_directory(&root.path().join("one"), &current) {
                eprintln!("skipped: the platform refused the link: {error}");
                return;
            }
            let store_dir = current.join("store");
            let one_before = without_open_locks(namespace(&root.path().join("one")));
            let two_before = without_open_locks(namespace(&root.path().join("two")));
            let pause = install(&store_dir, Point::FamilyRecovery);
            let answered = std::thread::scope(|scope| {
                let opening =
                    scope.spawn(|| Store::try_open(&store_dir, family).map(|(_, status)| status));
                pause.wait_reached();
                unlink_directory(&current);
                link_directory(&root.path().join("two"), &current).unwrap();
                pause.release();
                opening.join().unwrap()
            });
            drop(pause);
            assert_eq!(
                without_open_locks(namespace(&root.path().join("two"))),
                two_before,
                "{label}: a rule acted in two/, which the open did not lock ({answered:?})"
            );
            assert_eq!(
                without_open_locks(namespace(&root.path().join("one"))),
                one_before,
                "{label}: a rule acted in one/ after the caller's path stopped naming it \
                 ({answered:?})"
            );
            assert!(undetermined(&answered), "{label}: {answered:?}");
        }
    }
}

/// FR-2's rules act only in the directory the open locked, resolved once (spec.md, Amendments):
/// the half `a_dead_attempt_is_recovered_only_in_the_directory_the_open_locked` cannot see. Once a
/// rule has found the caller's path naming that directory, every read and write it makes is
/// there: with `current` pointed from `one/` to `two/` right after that check, a rule reading or
/// acting through the caller's path would change `two/`, which holds a dead attempt of its own and
/// which this open never locked. Each family, each leftover.
#[test]
fn every_read_and_write_of_the_rules_is_in_the_directory_the_open_locked() {
    use super::recovery::recovery_pause::{install, Point};

    for family in ALL_FAMILIES {
        for kill in [
            Kill::FirstPublication(ManifestPublishStage::Written),
            Kill::AfterMoves(1),
        ] {
            let label = format!("{family:?} killed {kill:?}");
            let root = tempfile::tempdir().unwrap();
            for side in ["one", "two"] {
                let store_dir = root.path().join(side).join("store");
                std::fs::create_dir_all(&store_dir).unwrap();
                let store = Store::open_segmented(&store_dir, family);
                store.write_facts();
                drop(store);
                kill_an_online_attempt(&store_dir, family, kill);
            }
            let current = root.path().join("current");
            if let Err(error) = link_directory(&root.path().join("one"), &current) {
                eprintln!("skipped: the platform refused the link: {error}");
                return;
            }
            let store_dir = current.join("store");
            let two_before = without_open_locks(namespace(&root.path().join("two")));
            let pause = install(&store_dir, Point::DeadAttemptIdentified);
            let answered = std::thread::scope(|scope| {
                let opening =
                    scope.spawn(|| Store::try_open(&store_dir, family).map(|(_, status)| status));
                pause.wait_reached();
                unlink_directory(&current);
                link_directory(&root.path().join("two"), &current).unwrap();
                pause.release();
                opening.join().unwrap()
            });
            drop(pause);
            assert_eq!(
                without_open_locks(namespace(&root.path().join("two"))),
                two_before,
                "{label}: a rule read or acted in two/, which the open did not lock ({answered:?})"
            );
        }
    }
}

/// spec.md's Status line, the item awaiting sign-off, for D4: a dead first publication's lone
/// temporary that the open has proved but cannot remove (here the store directory is read-only)
/// fails the open with the removal's I/O error, changes nothing, and the next open that can
/// remove it reports `Recovered` with the exact state. Each family; skips where this process may
/// write into a read-only directory.
#[cfg(unix)]
#[test]
fn a_proved_lone_temporary_that_cannot_be_removed_fails_with_the_removal_error() {
    for family in ALL_FAMILIES {
        let (root, store_dir, state) = written_store(family);
        kill_an_online_attempt(
            &store_dir,
            family,
            Kill::FirstPublication(ManifestPublishStage::Written),
        );
        let paths = family_paths(&store_dir, family);
        let Some(read_only) = ReadOnly::new(&store_dir) else {
            return;
        };
        let before = without_open_locks(namespace(root.path()));
        let result = Store::try_open(&store_dir, family).map(|(_, status)| status);
        match &result {
            Err(crate::RecoveryError::Io {
                operation: crate::RecoveryOperation::Inspect,
                path,
                source,
            }) if *path == paths.manifest_next
                && source.kind() == std::io::ErrorKind::PermissionDenied => {}
            other => panic!("{family:?}: expected the removal's error, got {other:?}"),
        }
        assert_eq!(
            without_open_locks(namespace(root.path())),
            before,
            "{family:?}: an open that could not remove the temporary changed the directory"
        );
        drop(read_only);
        let (store, status) = Store::try_open(&store_dir, family).unwrap();
        assert_eq!(status, crate::RecoveryStatus::Recovered, "{family:?}");
        assert_eq!(store.state(), state, "{family:?}");
        drop(store);
        assert_no_family_debris(&paths, &format!("{family:?}"));
    }
}

/// spec.md's Status line, the item awaiting sign-off, for D6: a split the open has proved but
/// cannot move back (here the previous directory is read-only) fails the open with the move's
/// I/O error, at the artifact's path in the store, changes nothing, and the next open that can
/// move it back reports `Recovered` with the exact state. Each family; skips where this process
/// may write into a read-only directory.
#[cfg(unix)]
#[test]
fn a_proved_split_that_cannot_be_moved_back_fails_with_the_moves_error() {
    for family in ALL_FAMILIES {
        let (root, store_dir, state, paths) = split_by_a_killed_cutover(family, 1);
        let (moved, _) = moved_and_unmoved(&paths);
        let Some(read_only) = ReadOnly::new(&paths.previous) else {
            return;
        };
        let before = without_open_locks(namespace(root.path()));
        let result = Store::try_open(&store_dir, family).map(|(_, status)| status);
        match &result {
            Err(crate::RecoveryError::Io {
                operation: crate::RecoveryOperation::Inspect,
                path,
                source,
            }) if *path == store_dir.join(&moved[0])
                && source.kind() == std::io::ErrorKind::PermissionDenied => {}
            other => panic!("{family:?}: expected the move's error, got {other:?}"),
        }
        assert_eq!(
            without_open_locks(namespace(root.path())),
            before,
            "{family:?}: an open that could not move the split back changed the directory"
        );
        drop(read_only);
        let (store, status) = Store::try_open(&store_dir, family).unwrap();
        assert_eq!(status, crate::RecoveryStatus::Recovered, "{family:?}");
        assert_eq!(store.state(), state, "{family:?}");
        drop(store);
        assert_no_family_debris(&paths, &format!("{family:?}"));
    }
}

/// A directory made read-only for as long as this lives, then writable again (also when a test
/// fails part-way, so its temporary directory can be removed).
#[cfg(unix)]
struct ReadOnly(PathBuf);

#[cfg(unix)]
impl ReadOnly {
    /// `None`, with the directory left writable, where this process may write into a read-only
    /// directory anyway (as root).
    fn new(directory: &Path) -> Option<Self> {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o555)).unwrap();
        let read_only = Self(directory.to_path_buf());
        let probe = directory.join("probe");
        if std::fs::write(&probe, b"").is_ok() {
            std::fs::remove_file(&probe).unwrap();
            eprintln!("skipped: this process may write into a read-only directory");
            return None;
        }
        Some(read_only)
    }
}

#[cfg(unix)]
impl Drop for ReadOnly {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;

        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

/// D6 runs only for a manifest bound to the family being opened: its paths come from the
/// manifest's own scope, so it runs after `validate_online_manifest_binding`. Another family's
/// finalized `Prepared` at this family's manifest path (here the key/set split's own manifest,
/// copied to the key/value path) keeps its error and every byte, the key/set split included.
#[test]
fn a_split_named_by_a_manifest_bound_to_another_family_keeps_its_error_and_its_bytes() {
    let (root, store_dir, _, set_paths) = split_by_a_killed_cutover(FixtureFamily::KeySet, 1);
    let kv_paths = family_paths(&store_dir, FixtureFamily::KeyValue);
    std::fs::copy(&set_paths.manifest, &kv_paths.manifest).unwrap();
    let before = without_open_locks(namespace(root.path()));
    let result = Store::try_open(&store_dir, FixtureFamily::KeyValue).map(|(_, status)| status);
    assert!(
        matches!(&result, Err(crate::RecoveryError::InvalidArtifact { path }) if *path == kv_paths.manifest),
        "a manifest bound to another family: expected InvalidArtifact, got {result:?}"
    );
    assert_eq!(
        without_open_locks(namespace(root.path())),
        before,
        "an open of key/value restored the key/set split its manifest path named"
    );
}

/// FR-2 under `DurabilityPolicy::Physical`, the manifest's durability, under which the restore's
/// moves are write-through on Windows and its directory barriers are real elsewhere: a process
/// killed inside its first online publication, and after one and two source moves of its
/// cutover, reopens `Recovered` with the exact state. Each family.
#[test]
fn under_physical_durability_a_killed_attempt_reopens_recovered() {
    let mut refused = Vec::new();
    for family in ALL_FAMILIES {
        for kill in [
            Kill::FirstPublication(ManifestPublishStage::Written),
            Kill::AfterMoves(1),
            Kill::AfterMoves(2),
        ] {
            let label = format!("{family:?} killed {kill:?} under Physical");
            let (_root, store_dir, state) = written_store(family);
            kill_an_online_attempt_under(
                &store_dir,
                family,
                kill,
                crate::DurabilityPolicy::Physical,
            );
            let paths = family_paths(&store_dir, family);
            if let Kill::AfterMoves(_) = kill {
                let manifest = read_published_manifest(&paths).unwrap().unwrap();
                assert_eq!(
                    manifest.durability,
                    crate::DurabilityPolicy::Physical,
                    "{label}: the attempt's manifest"
                );
            }
            match Store::try_open(&store_dir, family) {
                Ok((store, status)) => {
                    assert_eq!(status, crate::RecoveryStatus::Recovered, "{label}");
                    assert_eq!(store.state(), state, "{label}");
                }
                Err(error) => {
                    refused.push(format!("{label}: {error:?}"));
                    continue;
                }
            }
            assert_no_family_debris(&paths, &label);
            assert_reopens_exactly(&store_dir, family, &state, &label);
        }
    }
    assert!(
        refused.is_empty(),
        "kills under Physical durability kept their stores closed:\n{}",
        refused.join("\n")
    );
}

/// D6 makes its moves durable before the abandonment gives up the finalized manifest's authority
/// by removing it: under `DurabilityPolicy::Physical` it synchronizes the previous directory and
/// the store directory, and a failed barrier of either fails the open with that I/O error while
/// the finalized manifest is still in place, so the next open recovers with the exact state.
/// Each family, each barrier. Not on Windows, where a Physical move is write-through and the
/// directory barrier does nothing.
#[cfg(not(windows))]
#[test]
fn a_restore_whose_directory_barrier_fails_keeps_its_manifest_for_the_next_open() {
    use crate::durability::{directory_barrier_calls, fail_directory_barrier_for};

    for family in ALL_FAMILIES {
        for previous in [true, false] {
            let label = format!(
                "{family:?}, the {} directory's barrier failing",
                if previous { "previous" } else { "store" }
            );
            let (_root, store_dir, state) = written_store(family);
            kill_an_online_attempt_under(
                &store_dir,
                family,
                Kill::AfterMoves(1),
                crate::DurabilityPolicy::Physical,
            );
            let paths = family_paths(&store_dir, family);
            let directory = std::fs::canonicalize(&store_dir).unwrap();
            let barrier = if previous {
                directory.join(paths.previous.file_name().unwrap())
            } else {
                directory
            };
            let fault = fail_directory_barrier_for(
                barrier.clone(),
                directory_barrier_calls(&barrier) + 1,
                std::io::ErrorKind::Other,
            );
            let result = Store::try_open(&store_dir, family).map(|(_, status)| status);
            drop(fault);
            match &result {
                Err(crate::RecoveryError::Io {
                    operation: crate::RecoveryOperation::Inspect,
                    path,
                    source,
                }) if *path == store_dir
                    && source
                        .to_string()
                        .contains("injected directory publication barrier failure") => {}
                other => panic!("{label}: expected the barrier's error, got {other:?}"),
            }
            assert!(
                exists(&paths.manifest),
                "{label}: the finalized manifest was removed before the restore was durable"
            );
            let (moved, _) = moved_and_unmoved(&paths);
            assert!(
                moved.is_empty(),
                "{label}: the restore had not moved {moved:?} back"
            );
            let (store, status) = Store::try_open(&store_dir, family)
                .unwrap_or_else(|error| panic!("{label}: the next open failed: {error:?}"));
            assert_eq!(status, crate::RecoveryStatus::Recovered, "{label}");
            assert_eq!(store.state(), state, "{label}");
            drop(store);
            assert_no_family_debris(&paths, &label);
        }
    }
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
