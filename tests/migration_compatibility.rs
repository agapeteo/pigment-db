//! Runtime maintenance and external migration compatibility tests.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    compact_directory_in_place, inspect_storage, ClosedCompactionOptions, CompactionError,
    RecoveryError,
};

#[test]
fn every_frozen_legacy_family_requires_the_external_tool_without_mutation() {
    let fixture_directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy");
    for (name, family) in [
        ("kv.wal.dat", "value"),
        ("set.wal.dat", "set"),
        ("map.wal.dat", "map"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let store_directory = root.path().join("store");
        std::fs::create_dir(&store_directory).unwrap();
        let artifact = store_directory.join(name);
        std::fs::copy(fixture_directory.join(name), &artifact).unwrap();
        let before = namespace(root.path(), &store_directory);

        let open_error = match family {
            "value" => match DurableKeyValueStore::try_init_new(&store_directory) {
                Err(error) => error,
                Ok(_) => panic!("legacy value store unexpectedly opened"),
            },
            "set" => match DurableKeySetStore::try_init_new(&store_directory) {
                Err(error) => error,
                Ok(_) => panic!("legacy set store unexpectedly opened"),
            },
            "map" => match DurableKeyMapStore::try_init_new(&store_directory) {
                Err(error) => error,
                Ok(_) => panic!("legacy map store unexpectedly opened"),
            },
            _ => unreachable!(),
        };
        assert!(matches!(
            open_error,
            RecoveryError::MigrationRequired { ref path } if path == &artifact
        ));
        assert!(open_error.to_string().contains("pigment-db-migrate"));
        assert_only_locks_taken(
            root.path(),
            &store_directory,
            &before,
            &[Lock::Inner],
            &format!("open {family}"),
        );

        let inspect_error = inspect_storage(&store_directory).unwrap_err();
        assert!(matches!(
            inspect_error,
            CompactionError::MigrationRequired { ref path } if path == &artifact
        ));
        assert!(inspect_error.to_string().contains("pigment-db-migrate"));
        assert_only_locks_taken(
            root.path(),
            &store_directory,
            &before,
            &[Lock::Inner],
            &format!("inspect {family}"),
        );

        let compact_error =
            compact_directory_in_place(&store_directory, ClosedCompactionOptions::default())
                .unwrap_err();
        assert!(matches!(
            compact_error,
            CompactionError::MigrationRequired { ref path } if path == &artifact
        ));
        assert!(compact_error.to_string().contains("pigment-db-migrate"));
        assert_only_locks_taken(
            root.path(),
            &store_directory,
            &before,
            &[Lock::Inner, Lock::Replacement],
            &format!("compact {family}"),
        );
    }
}

/// The store directory's two lock files (specs/011).
#[derive(Clone, Copy, Debug)]
enum Lock {
    /// `<store>/.pigment-lock`, which an open takes.
    Inner,
    /// `.<store name>.pigment-lock` beside it, which a claim or a maintenance-state open takes.
    Replacement,
}

/// A namespace snapshot without the store directory's lock files, whose contents record whichever
/// process locked them last, together with the lock files present.
struct Namespace {
    without_locks: Vec<(std::path::PathBuf, Option<Vec<u8>>)>,
    locks: Vec<std::path::PathBuf>,
}

fn lock_path(root: &std::path::Path, store: &std::path::Path, lock: Lock) -> std::path::PathBuf {
    let relative = store.strip_prefix(root).unwrap();
    match lock {
        Lock::Inner => relative.join(".pigment-lock"),
        Lock::Replacement => {
            let mut name = std::ffi::OsString::from(".");
            name.push(relative.file_name().unwrap());
            name.push(".pigment-lock");
            relative.with_file_name(name)
        }
    }
}

fn namespace(root: &std::path::Path, store: &std::path::Path) -> Namespace {
    let lock_paths = [
        lock_path(root, store, Lock::Inner),
        lock_path(root, store, Lock::Replacement),
    ];
    let (locks, without_locks) = namespace_snapshot(root)
        .into_iter()
        .partition::<Vec<_>, _>(|(path, _)| lock_paths.contains(path));
    Namespace {
        without_locks,
        locks: locks.into_iter().map(|(path, _)| path).collect(),
    }
}

/// Asserts that an operation refused for a reason other than ownership changed nothing but the
/// lock files it takes before it reads anything: every lock file in `taken` now exists, no lock
/// file that existed before is gone, and everything else is byte-for-byte as it was.
fn assert_only_locks_taken(
    root: &std::path::Path,
    store: &std::path::Path,
    before: &Namespace,
    taken: &[Lock],
    operation: &str,
) {
    let after = namespace(root, store);
    assert_eq!(after.without_locks, before.without_locks, "{operation}");
    for lock in &before.locks {
        assert!(after.locks.contains(lock), "{operation} removed {lock:?}");
    }
    for &lock in taken {
        assert!(
            after.locks.contains(&lock_path(root, store, lock)),
            "{operation} must leave the {lock:?} lock file"
        );
    }
}

fn namespace_snapshot(root: &std::path::Path) -> Vec<(std::path::PathBuf, Option<Vec<u8>>)> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        snapshot: &mut Vec<(std::path::PathBuf, Option<Vec<u8>>)>,
    ) {
        let mut entries = std::fs::read_dir(current)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                snapshot.push((relative, None));
                visit(root, &path, snapshot);
            } else {
                snapshot.push((relative, Some(std::fs::read(path).unwrap())));
            }
        }
    }

    let mut snapshot = Vec::new();
    visit(root, root, &mut snapshot);
    snapshot
}

#[test]
fn current_invalid_and_ambiguous_evidence_stays_distinct_across_runtime_entry_points() {
    fn assert_invalid_everywhere(
        case: &str,
        root: &std::path::Path,
        store: &std::path::Path,
        path: &std::path::Path,
    ) {
        let before = namespace(root, store);
        let open_error = match DurableKeyValueStore::try_init_new(store) {
            Err(error) => error,
            Ok(_) => panic!("invalid current evidence unexpectedly opened: {case}"),
        };
        assert!(
            matches!(open_error, RecoveryError::InvalidArtifact { path: ref found } if found == path)
        );
        assert_only_locks_taken(
            root,
            store,
            &before,
            &[Lock::Inner],
            &format!("open {case}"),
        );
        assert!(matches!(
            inspect_storage(store),
            Err(CompactionError::InvalidArtifact { path: found }) if found == path
        ));
        assert_only_locks_taken(
            root,
            store,
            &before,
            &[Lock::Inner],
            &format!("inspect {case}"),
        );
        assert!(matches!(
            compact_directory_in_place(store, ClosedCompactionOptions::default()),
            Err(CompactionError::InvalidArtifact { path: found }) if found == path
        ));
        assert_only_locks_taken(
            root,
            store,
            &before,
            &[Lock::Inner, Lock::Replacement],
            &format!("compact {case}"),
        );
    }

    let corrupt_root = tempfile::tempdir().unwrap();
    let corrupt_store = corrupt_root.path().join("store");
    std::fs::create_dir(&corrupt_store).unwrap();
    {
        let store = DurableKeyValueStore::try_init_new(&corrupt_store)
            .unwrap()
            .into_store();
        store.put(b"key".to_vec(), b"value".to_vec());
    }
    let corrupt_path = corrupt_store.join("kv.wal.dat");
    let mut corrupt = std::fs::read(&corrupt_path).unwrap();
    corrupt[0] ^= 0xff;
    std::fs::write(&corrupt_path, corrupt).unwrap();
    assert_invalid_everywhere(
        "corrupt",
        corrupt_root.path(),
        &corrupt_store,
        &corrupt_path,
    );

    let wrong_seed = tempfile::tempdir().unwrap();
    {
        let set = DurableKeySetStore::try_init_new(wrong_seed.path())
            .unwrap()
            .into_store();
        set.append(b"set".to_vec(), b"member".to_vec());
    }
    let wrong_root = tempfile::tempdir().unwrap();
    let wrong_store = wrong_root.path().join("store");
    std::fs::create_dir(&wrong_store).unwrap();
    let wrong_path = wrong_store.join("kv.wal.dat");
    std::fs::copy(wrong_seed.path().join("set.wal.dat"), &wrong_path).unwrap();
    assert_invalid_everywhere("wrong-family", wrong_root.path(), &wrong_store, &wrong_path);

    let malformed_root = tempfile::tempdir().unwrap();
    let malformed_store = malformed_root.path().join("store");
    std::fs::create_dir(&malformed_store).unwrap();
    {
        let store = DurableKeyValueStore::try_init_new(&malformed_store)
            .unwrap()
            .into_store();
        store.put(b"key".to_vec(), b"value".to_vec());
    }
    let malformed_path = malformed_store.join("kv.wal.dat.segment-not-a-number");
    std::fs::write(&malformed_path, b"malformed").unwrap();
    assert_invalid_everywhere(
        "malformed-name",
        malformed_root.path(),
        &malformed_store,
        &malformed_path,
    );

    let ambiguous_root = tempfile::tempdir().unwrap();
    let ambiguous_store = ambiguous_root.path().join("store");
    std::fs::create_dir(&ambiguous_store).unwrap();
    {
        let store = DurableKeyValueStore::try_init_new(&ambiguous_store)
            .unwrap()
            .into_store();
        store.put(b"old".to_vec(), b"authority".to_vec());
    }
    let previous = ambiguous_root
        .path()
        .join(".store.pigment-compact.previous");
    std::fs::create_dir(&previous).unwrap();
    {
        let store = DurableKeyValueStore::try_init_new(&previous)
            .unwrap()
            .into_store();
        store.put(b"new".to_vec(), b"candidate".to_vec());
    }
    let before = namespace(ambiguous_root.path(), &ambiguous_store);
    assert!(matches!(
        DurableKeyValueStore::try_init_new(&ambiguous_store),
        Err(RecoveryError::AuthorityUndetermined { .. })
    ));
    // Directory-level maintenance is in progress, so the open takes the replacement lock only.
    assert_only_locks_taken(
        ambiguous_root.path(),
        &ambiguous_store,
        &before,
        &[Lock::Replacement],
        "open ambiguous",
    );
    assert!(matches!(
        inspect_storage(&ambiguous_store),
        Err(CompactionError::AuthorityUndetermined { .. })
    ));
    assert_only_locks_taken(
        ambiguous_root.path(),
        &ambiguous_store,
        &before,
        &[Lock::Replacement],
        "inspect ambiguous",
    );
    assert!(matches!(
        compact_directory_in_place(&ambiguous_store, ClosedCompactionOptions::default()),
        Err(CompactionError::AuthorityUndetermined { .. })
    ));
    assert_only_locks_taken(
        ambiguous_root.path(),
        &ambiguous_store,
        &before,
        &[Lock::Replacement],
        "compact ambiguous",
    );
}
