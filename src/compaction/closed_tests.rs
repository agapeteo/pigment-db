//! Private closed-compaction behavior tests.

use crate::maintenance_coordination::{acquire_open_lease, try_claim_closed};
use crate::test_support::maintenance_fixtures::{
    active_name, create_segmented_v2, snapshot_directory, FixtureFamily,
};
use crate::wal::format::V2CodecProbe;

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

#[test]
fn closed_capture_builds_one_current_active_per_family_in_unique_sibling_staging() {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("mixed-store");
    std::fs::create_dir(&store_dir).unwrap();
    for family in [
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
        FixtureFamily::KeyMap,
    ] {
        create_segmented_v2(&store_dir, family);
    }
    let source_before = snapshot_directory(&store_dir).unwrap();

    let prepared =
        super::prepare_closed_staging(&store_dir, crate::ClosedCompactionOptions::default())
            .unwrap();

    assert_eq!(snapshot_directory(&store_dir).unwrap(), source_before);
    assert_eq!(prepared.capture.source_dir, store_dir);
    assert_eq!(prepared.capture.families.len(), 3);
    assert_eq!(prepared.capture.source_bytes.len(), source_before.len());
    for (name, bytes) in &source_before {
        assert_eq!(
            prepared
                .capture
                .source_bytes
                .get(&std::path::Path::new("mixed-store").join(name)),
            Some(bytes)
        );
    }
    assert_eq!(prepared.paths.staging.parent(), Some(root.path()));
    assert_ne!(prepared.paths.staging, prepared.capture.source_dir);
    assert_eq!(prepared.replacement_inventory.len(), 3);

    let staged = snapshot_directory(&prepared.paths.staging).unwrap();
    assert_eq!(staged.len(), 3);
    for (family, kind) in [
        (FixtureFamily::KeyValue, 1),
        (FixtureFamily::KeySet, 2),
        (FixtureFamily::KeyMap, 3),
    ] {
        let bytes = staged
            .get(std::path::Path::new(active_name(family)))
            .unwrap();
        assert!(V2CodecProbe::header_is_valid(
            &bytes[..V2CodecProbe::HEADER_LEN]
        ));
        assert_eq!(
            V2CodecProbe::header_kind(&bytes[..V2CodecProbe::HEADER_LEN]),
            Some(kind)
        );
    }

    let staging_before = snapshot_directory(&prepared.paths.staging).unwrap();
    let collision = super::prepare_closed_staging(
        &prepared.capture.source_dir,
        crate::ClosedCompactionOptions::default(),
    );
    assert!(collision.is_err());
    assert_eq!(
        snapshot_directory(&prepared.paths.staging).unwrap(),
        staging_before
    );
    assert_eq!(
        snapshot_directory(&prepared.capture.source_dir).unwrap(),
        source_before
    );
}
