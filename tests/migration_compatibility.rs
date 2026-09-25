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
        let before = namespace_except_lock_file(root.path(), &store_directory);

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
        assert!(
            store_directory.join(".pigment-lock").is_file(),
            "open {family}"
        );
        assert_eq!(
            namespace_except_lock_file(root.path(), &store_directory),
            before,
            "open {family}"
        );

        let inspect_error = inspect_storage(&store_directory).unwrap_err();
        assert!(matches!(
            inspect_error,
            CompactionError::MigrationRequired { ref path } if path == &artifact
        ));
        assert!(inspect_error.to_string().contains("pigment-db-migrate"));
        assert_eq!(
            namespace_except_lock_file(root.path(), &store_directory),
            before,
            "inspect {family}"
        );

        let compact_error =
            compact_directory_in_place(&store_directory, ClosedCompactionOptions::default())
                .unwrap_err();
        assert!(matches!(
            compact_error,
            CompactionError::MigrationRequired { ref path } if path == &artifact
        ));
        assert!(compact_error.to_string().contains("pigment-db-migrate"));
        assert_eq!(
            namespace_except_lock_file(root.path(), &store_directory),
            before,
            "compact {family}"
        );
    }
}

/// The namespace without the store directory's two lock files (specs/011): the inner one an open
/// creates in the directory and the replacement one a closed claim creates beside it, each before
/// anything is read. An open or claim refused for any other reason leaves them behind; nothing
/// else may change.
fn namespace_except_lock_file(
    root: &std::path::Path,
    store: &std::path::Path,
) -> Vec<(std::path::PathBuf, Option<Vec<u8>>)> {
    let relative = store.strip_prefix(root).unwrap();
    let inner = relative.join(".pigment-lock");
    let mut replacement_name = std::ffi::OsString::from(".");
    replacement_name.push(relative.file_name().unwrap());
    replacement_name.push(".pigment-lock");
    let replacement = relative.with_file_name(replacement_name);
    namespace_snapshot(root)
        .into_iter()
        .filter(|(path, _)| *path != inner && *path != replacement)
        .collect()
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
        let before = namespace_except_lock_file(root, store);
        let open_error = match DurableKeyValueStore::try_init_new(store) {
            Err(error) => error,
            Ok(_) => panic!("invalid current evidence unexpectedly opened: {case}"),
        };
        assert!(
            matches!(open_error, RecoveryError::InvalidArtifact { path: ref found } if found == path)
        );
        assert_eq!(namespace_except_lock_file(root, store), before);
        assert!(matches!(
            inspect_storage(store),
            Err(CompactionError::InvalidArtifact { path: found }) if found == path
        ));
        assert_eq!(namespace_except_lock_file(root, store), before);
        assert!(matches!(
            compact_directory_in_place(store, ClosedCompactionOptions::default()),
            Err(CompactionError::InvalidArtifact { path: found }) if found == path
        ));
        assert_eq!(namespace_except_lock_file(root, store), before);
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
    let before = namespace_except_lock_file(ambiguous_root.path(), &ambiguous_store);
    assert!(matches!(
        DurableKeyValueStore::try_init_new(&ambiguous_store),
        Err(RecoveryError::AuthorityUndetermined { .. })
    ));
    assert_eq!(
        namespace_except_lock_file(ambiguous_root.path(), &ambiguous_store),
        before
    );
    assert!(matches!(
        inspect_storage(&ambiguous_store),
        Err(CompactionError::AuthorityUndetermined { .. })
    ));
    assert_eq!(
        namespace_except_lock_file(ambiguous_root.path(), &ambiguous_store),
        before
    );
    assert!(matches!(
        compact_directory_in_place(&ambiguous_store, ClosedCompactionOptions::default()),
        Err(CompactionError::AuthorityUndetermined { .. })
    ));
    assert_eq!(
        namespace_except_lock_file(ambiguous_root.path(), &ambiguous_store),
        before
    );
}
