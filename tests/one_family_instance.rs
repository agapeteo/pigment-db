//! One open instance of a family per store directory per process (specs/016 FR-1).
//!
//! A second open of a family whose directory this process already holds open for that family is
//! refused before any recovery runs, whatever the path's spelling, with the error specs/011 gives
//! another process's open: `RecoveryError::Io` of kind `WouldBlock`, here naming the directory
//! and the family. Dropping the instance ends the hold. Other families of the directory, and the
//! family in another directory, are unaffected.

use std::fs;
use std::path::{Path, PathBuf};

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::{OnlineCompactionOptions, RecoveryError, RecoveryOperation, RecoveryStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
enum Family {
    KeyValue,
    KeySet,
    KeyMap,
}

const ALL_FAMILIES: [Family; 3] = [Family::KeyValue, Family::KeySet, Family::KeyMap];

impl Family {
    /// How the refusal names the family.
    fn named(self) -> &'static str {
        match self {
            Self::KeyValue => "key/value",
            Self::KeySet => "key/set",
            Self::KeyMap => "key/sorted-map",
        }
    }
}

/// An open instance of one family.
#[allow(clippy::enum_variant_names)]
enum Instance {
    KeyValue(DurableKeyValueStore<fs::File>),
    KeySet(DurableKeySetStore<fs::File>),
    KeyMap(DurableKeyMapStore<fs::File>),
}

impl Instance {
    fn try_open(directory: &Path, family: Family) -> Result<(Self, RecoveryStatus), RecoveryError> {
        Ok(match family {
            Family::KeyValue => {
                let (store, status) = DurableKeyValueStore::try_init_new(directory)?.into_parts();
                (Self::KeyValue(store), status)
            }
            Family::KeySet => {
                let (store, status) = DurableKeySetStore::try_init_new(directory)?.into_parts();
                (Self::KeySet(store), status)
            }
            Family::KeyMap => {
                let (store, status) = DurableKeyMapStore::try_init_new(directory)?.into_parts();
                (Self::KeyMap(store), status)
            }
        })
    }

    fn open(directory: &Path, family: Family) -> Self {
        Self::try_open(directory, family)
            .unwrap_or_else(|error| panic!("{family:?}: open failed: {error:?}"))
            .0
    }

    fn write(&self, index: usize) {
        match self {
            Self::KeyValue(store) => store.try_put(
                format!("key-{index}").into_bytes(),
                format!("value-{index}").into_bytes(),
            ),
            Self::KeySet(store) => {
                store.try_append(b"group".to_vec(), format!("member-{index}").into_bytes())
            }
            Self::KeyMap(store) => store
                .try_put(
                    b"book".to_vec(),
                    SearchKey::from(index),
                    format!("entry-{index}").into_bytes(),
                )
                .map(|_| ()),
        }
        .unwrap_or_else(|error| panic!("write {index} failed: {error}"));
    }

    fn holds(&self, index: usize) -> bool {
        match self {
            Self::KeyValue(store) => {
                store.get(format!("key-{index}").as_bytes())
                    == Some(format!("value-{index}").into_bytes())
            }
            Self::KeySet(store) => {
                store.contains_in_set(b"group", format!("member-{index}").as_bytes())
            }
            Self::KeyMap(store) => {
                store.get_element(b"book", &SearchKey::from(index))
                    == Some(format!("entry-{index}").into_bytes())
            }
        }
    }

    fn compact_online(&self) {
        match self {
            Self::KeyValue(store) => store
                .try_compact_online(OnlineCompactionOptions::default())
                .map(|_| ()),
            Self::KeySet(store) => store
                .try_compact_online(OnlineCompactionOptions::default())
                .map(|_| ()),
            Self::KeyMap(store) => store
                .try_compact_online(OnlineCompactionOptions::default())
                .map(|_| ()),
        }
        .unwrap_or_else(|error| panic!("online compaction failed: {error:?}"));
    }
}

/// A parent directory holding an empty store directory.
fn store_dir() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("store");
    fs::create_dir(&store).unwrap();
    (root, store)
}

/// Every entry under `root`, files with their bytes. A lock file is recorded by its presence:
/// Windows refuses every read of a locked range, even through the holder's own second handle.
fn snapshot(root: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    fn collect(root: &Path, directory: &Path, out: &mut Vec<(PathBuf, Option<Vec<u8>>)>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            if path.is_dir() {
                out.push((relative, None));
                collect(root, &path, out);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".pigment-lock"))
            {
                out.push((relative, Some(b"<lock file>".to_vec())));
            } else {
                out.push((relative, Some(fs::read(&path).unwrap())));
            }
        }
    }
    let mut entries = Vec::new();
    collect(root, root, &mut entries);
    entries.sort();
    entries
}

/// `path` relative to the working directory, through `..` components, without changing the
/// working directory (tests run in parallel). `None` where the two share no root (another
/// Windows drive).
fn relative_to_the_working_directory(path: &Path) -> Option<PathBuf> {
    let here = fs::canonicalize(std::env::current_dir().ok()?).ok()?;
    let there = fs::canonicalize(path).ok()?;
    let here = here.components().collect::<Vec<_>>();
    let there = there.components().collect::<Vec<_>>();
    if here.first() != there.first() {
        return None;
    }
    let common = here
        .iter()
        .zip(&there)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for _ in common..here.len() {
        relative.push("..");
    }
    for component in &there[common..] {
        relative.push(component.as_os_str());
    }
    Some(relative)
}

/// Every spelling of `store` this test can make: as given, with a trailing separator, with `.`,
/// through a sibling directory and `..`, relative to the working directory, and through a
/// symlink.
fn spellings(root: &Path, store: &Path) -> Vec<PathBuf> {
    fs::create_dir_all(root.join("sibling")).unwrap();
    let mut spelled = vec![
        store.to_path_buf(),
        PathBuf::from(format!("{}{}", store.display(), std::path::MAIN_SEPARATOR)),
        store.join("."),
        root.join("sibling").join("..").join("store"),
    ];
    match relative_to_the_working_directory(store) {
        Some(relative) => spelled.push(relative),
        None => eprintln!("relative spelling skipped: no common root with the working directory"),
    }
    let alias = root.join("alias");
    #[cfg(unix)]
    let linked = std::os::unix::fs::symlink(store, &alias);
    #[cfg(windows)]
    let linked = std::os::windows::fs::symlink_dir(store, &alias);
    match linked {
        Ok(()) => spelled.push(alias),
        Err(error) => eprintln!("symlink spelling skipped: the platform refused it: {error}"),
    }
    spelled
}

/// Requires `result` to be FR-1's refusal of `spelled`: `Io` of kind `WouldBlock` under
/// `Inspect`, at the path the caller gave, naming the directory and the family.
fn assert_refused(
    result: Result<(Instance, RecoveryStatus), RecoveryError>,
    spelled: &Path,
    store: &Path,
    family: Family,
) {
    let directory = fs::canonicalize(store).unwrap();
    match result {
        Err(RecoveryError::Io {
            operation: RecoveryOperation::Inspect,
            path,
            source,
        }) => {
            assert_eq!(
                path, spelled,
                "{family:?}: the refusal names the caller's path"
            );
            assert_eq!(
                source.kind(),
                std::io::ErrorKind::WouldBlock,
                "{family:?} at {}: {source}",
                spelled.display()
            );
            let message = source.to_string();
            assert!(
                message.contains(&directory.display().to_string())
                    && message.contains(family.named()),
                "{family:?} at {}: the refusal must name the directory and the family: \
                 {message}",
                spelled.display()
            );
        }
        Err(other) => panic!(
            "{family:?} at {}: expected FR-1's refusal, got {other:?}",
            spelled.display()
        ),
        Ok((_, status)) => panic!(
            "{family:?} at {}: a second instance of the family opened ({status:?})",
            spelled.display()
        ),
    }
}

/// FR-1: while one instance of a family is open on a directory, every other open of that family
/// there is refused, through every spelling of the directory, and changes nothing.
#[test]
fn a_second_open_of_an_open_family_is_refused_through_every_spelling() {
    for family in ALL_FAMILIES {
        let (root, store) = store_dir();
        let spelled = spellings(root.path(), &store);
        let first = Instance::open(&store, family);
        first.write(0);
        let before = snapshot(root.path());
        for spelling in &spelled {
            assert_refused(
                Instance::try_open(spelling, family),
                spelling,
                &store,
                family,
            );
        }
        assert_eq!(
            snapshot(root.path()),
            before,
            "{family:?}: a refused open changed the directory"
        );
        first.write(1);
        assert!(
            first.holds(0) && first.holds(1),
            "{family:?}: the open instance"
        );
    }
}

/// FR-1, `try_init_new_with_options` and `init_new`: the first is refused as `try_init_new` is,
/// and the second panics with the refusal's message.
#[test]
fn every_open_entry_point_is_refused() {
    let (_root, store) = store_dir();
    let _first = DurableKeyValueStore::try_init_new(&store).unwrap();
    let options = pigment_db::DurableStoreOptions::default();
    assert_refused(
        DurableKeyValueStore::try_init_new_with_options(&store, options)
            .map(|outcome| outcome.into_parts())
            .map(|(store, status)| (Instance::KeyValue(store), status)),
        &store,
        &store,
        Family::KeyValue,
    );
    let spelled = store.to_str().unwrap().to_owned();
    let panicked = std::panic::catch_unwind(|| DurableKeyValueStore::init_new(&spelled));
    let message = match panicked {
        Ok(_) => panic!("init_new opened a second instance of the family"),
        Err(payload) => payload
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default(),
    };
    assert!(
        message.contains("key/value")
            && message.contains(&fs::canonicalize(&store).unwrap().display().to_string()),
        "init_new must panic with the refusal: {message}"
    );
}

/// FR-1: the refusal comes before any recovery. A lone closed-compaction `.manifest.next`, which
/// an admitted open removes (specs/015 FR-3), is still there after the refused open.
#[test]
fn the_refusal_comes_before_any_recovery() {
    let (root, store) = store_dir();
    let first = Instance::open(&store, Family::KeyValue);
    first.write(0);
    let temporary = root.path().join(".store.pigment-compact.manifest.next");
    fs::write(&temporary, b"an unpublished closed Prepared").unwrap();
    let before = snapshot(root.path());
    assert_refused(
        Instance::try_open(&store, Family::KeyValue),
        &store,
        &store,
        Family::KeyValue,
    );
    assert_eq!(
        snapshot(root.path()),
        before,
        "the refused open recovered maintenance it had no right to"
    );
    drop(first);
}

/// FR-1: dropping the instance ends the hold, and the next open of the family goes on with what
/// the first instance wrote.
#[test]
fn dropping_the_instance_ends_the_hold() {
    for family in ALL_FAMILIES {
        let (_root, store) = store_dir();
        let first = Instance::open(&store, family);
        first.write(0);
        drop(first);
        for round in 1..4 {
            let (again, status) = Instance::try_open(&store, family)
                .unwrap_or_else(|error| panic!("{family:?}: reopen {round}: {error:?}"));
            assert_eq!(status, RecoveryStatus::Normal);
            assert!(again.holds(0), "{family:?}: reopen {round}");
            again.write(round);
        }
    }
}

/// FR-1: dropping an instance ends its hold while an instance of another family keeps the
/// directory open, so that the directory's registry entry outlives the first instance; the family
/// then opens again with what it wrote.
#[test]
fn dropping_the_instance_ends_the_hold_while_another_family_keeps_the_directory_open() {
    for family in ALL_FAMILIES {
        let other = match family {
            Family::KeyValue => Family::KeySet,
            Family::KeySet => Family::KeyMap,
            Family::KeyMap => Family::KeyValue,
        };
        let (_root, store) = store_dir();
        let keeper = Instance::open(&store, other);
        let first = Instance::open(&store, family);
        first.write(0);
        drop(first);
        let (again, _) = Instance::try_open(&store, family).unwrap_or_else(|error| {
            panic!("{family:?}: reopened beside an open {other:?} instance: {error:?}")
        });
        assert!(again.holds(0), "{family:?}");
        keeper.write(1);
        assert!(keeper.holds(1), "{other:?}");
    }
}

/// FR-1: an open that fails holds nothing afterwards. Here the family's online maintenance
/// temporary is a directory, which every open refuses; once it is gone the family opens.
#[test]
fn an_open_that_fails_holds_nothing() {
    for family in ALL_FAMILIES {
        let (_root, store) = store_dir();
        drop(Instance::open(&store, family));
        let active = match family {
            Family::KeyValue => "kv.wal.dat",
            Family::KeySet => "set.wal.dat",
            Family::KeyMap => "map.wal.dat",
        };
        let temporary = store.join(format!("{active}.pigment-compact.manifest.next"));
        fs::create_dir(&temporary).unwrap();
        assert!(
            Instance::try_open(&store, family).is_err(),
            "{family:?}: the planted state must refuse"
        );
        fs::remove_dir(&temporary).unwrap();
        assert!(
            Instance::try_open(&store, family).is_ok(),
            "{family:?}: a failed open kept the family"
        );
    }
}

/// FR-1: other families of the directory, and the same family of another directory, open beside
/// an open instance, and each instance keeps what it wrote.
#[test]
fn other_families_and_other_directories_are_unaffected() {
    let (_root, store) = store_dir();
    let (_other_root, other) = store_dir();
    let instances = ALL_FAMILIES
        .into_iter()
        .map(|family| (family, Instance::open(&store, family)))
        .collect::<Vec<_>>();
    let elsewhere = ALL_FAMILIES
        .into_iter()
        .map(|family| (family, Instance::open(&other, family)))
        .collect::<Vec<_>>();
    for (family, instance) in instances.iter().chain(&elsewhere) {
        instance.write(7);
        assert!(instance.holds(7), "{family:?}");
    }
}

/// The first review of specs/015 measured H1: two key/value instances on one directory in one
/// process both accepted writes, and the next open refused the WAL they had both appended to.
/// Under FR-1 the second open is refused, and every write the first accepted survives.
#[test]
fn two_instances_can_no_longer_write_one_wal() {
    for family in ALL_FAMILIES {
        let (_root, store) = store_dir();
        let first = Instance::open(&store, family);
        let second = Instance::try_open(&store, family);
        for index in 0..4 {
            first.write(index);
            if let Ok((second, _)) = &second {
                second.write(100 + index);
            }
        }
        assert_refused(second, &store, &store, family);
        drop(first);
        let reopened = Instance::open(&store, family);
        assert!(
            (0..4).all(|index| reopened.holds(index)),
            "{family:?}: an accepted write was lost"
        );
    }
}

/// The first review of specs/015 measured H2: a write by a second instance after the first
/// instance's online compaction was acknowledged and then missing after a reopen. Under FR-1 the
/// second instance cannot be opened, and every write the first accepted, before and after its
/// compaction, survives.
#[test]
fn a_second_instance_cannot_lose_writes_across_an_online_compaction() {
    for family in ALL_FAMILIES {
        let (_root, store) = store_dir();
        let first = Instance::open(&store, family);
        first.write(0);
        let second = Instance::try_open(&store, family);
        first.compact_online();
        if let Ok((second, _)) = &second {
            second.write(1);
        }
        first.write(2);
        assert_refused(second, &store, &store, family);
        drop(first);
        let reopened = Instance::open(&store, family);
        assert!(
            reopened.holds(0) && reopened.holds(2),
            "{family:?}: an accepted write was lost"
        );
    }
}

/// FR-1's hold belongs to the family's own instance, and a refusal leaves nothing behind. Each
/// family's hold refuses a second open while another family's instance created the directory's
/// registry entry, and still refuses once an instance of a third family has been opened and
/// dropped. Once every instance is dropped, the refused opens have left the directory owned by
/// nothing: a closed compaction, which this process refuses while any open of the directory is
/// counted, goes on, and the family opens again with what it wrote.
#[test]
fn a_familys_hold_is_its_own_and_a_refusal_leaves_nothing_behind() {
    for family in ALL_FAMILIES {
        let others = ALL_FAMILIES
            .into_iter()
            .filter(|other| *other != family)
            .collect::<Vec<_>>();
        let (_root, store) = store_dir();
        let keeper = Instance::open(&store, others[0]);
        let first = Instance::open(&store, family);
        assert_refused(Instance::try_open(&store, family), &store, &store, family);
        drop(Instance::open(&store, others[1]));
        assert_refused(Instance::try_open(&store, family), &store, &store, family);
        first.write(0);
        drop(first);
        drop(keeper);
        pigment_db::compact_directory_in_place(
            &store,
            pigment_db::ClosedCompactionOptions::default(),
        )
        .unwrap_or_else(|error| {
            panic!("{family:?}: a refused open left the directory owned: {error:?}")
        });
        let (again, _) = Instance::try_open(&store, family)
            .unwrap_or_else(|error| panic!("{family:?}: reopened after compaction: {error:?}"));
        assert!(again.holds(0), "{family:?}");
    }
}
