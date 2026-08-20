//! Private closed-compaction behavior tests.

use crate::maintenance_coordination::{acquire_open_lease, try_claim_closed};
use crate::test_support::maintenance_fixtures::snapshot_directory;

#[test]
fn open_or_opening_directory_blocks_closed_claim_without_cross_directory_coordination() {
    let root = tempfile::tempdir().unwrap();
    let open_dir = root.path().join("open");
    let independent_dir = root.path().join("independent");
    std::fs::create_dir(&open_dir).unwrap();
    std::fs::create_dir(&independent_dir).unwrap();

    let store = crate::key_value_store::DurableKeyValueStore::try_init_new(&open_dir)
        .unwrap()
        .into_store();
    let before = snapshot_directory(root.path()).unwrap();
    let open_error = try_claim_closed(&open_dir).unwrap_err();
    assert_eq!(open_error.kind(), std::io::ErrorKind::WouldBlock);
    let alias = open_dir.join("..").join("open");
    assert_eq!(
        try_claim_closed(&alias).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(snapshot_directory(root.path()).unwrap(), before);

    let independent_claim = try_claim_closed(&independent_dir).unwrap();
    assert_eq!(
        acquire_open_lease(&independent_dir).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(independent_claim);

    drop(store);
    let released_claim = try_claim_closed(&open_dir).unwrap();
    drop(released_claim);

    let opening_lease = acquire_open_lease(&open_dir).unwrap();
    let opening_error = try_claim_closed(&open_dir).unwrap_err();
    assert_eq!(opening_error.kind(), std::io::ErrorKind::WouldBlock);
    drop(opening_lease);
    assert!(try_claim_closed(&open_dir).is_ok());
}

#[test]
fn empty_closed_compaction_is_an_artifact_free_no_op() {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("empty-store");
    std::fs::create_dir(&store_dir).unwrap();
    let before = snapshot_directory(root.path()).unwrap();

    let outcome = crate::maintenance::compact_directory_in_place_internal(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();

    assert!(outcome.families().is_empty());
    assert_eq!(snapshot_directory(root.path()).unwrap(), before);
}
