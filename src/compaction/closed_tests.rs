//! Private closed-compaction behavior tests.

use crate::maintenance_coordination::{acquire_open_lease, try_claim_closed};
use crate::test_support::maintenance_fixtures::{
    active_name, create_segmented_v2, snapshot_directory, FixtureFamily,
};
use crate::wal::format::V2CodecProbe;
use crate::wal::replay::{
    encode_current_key_set_snapshot_with_metadata, encode_current_key_value_snapshot_with_metadata,
    KeySetSnapshot,
};

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

#[test]
fn staging_family_state_or_timestamp_mismatch_rejects_validation_without_publication() {
    #[derive(Clone, Copy)]
    enum Mismatch {
        Family,
        State,
        Timestamp,
    }

    for mismatch in [Mismatch::Family, Mismatch::State, Mismatch::Timestamp] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let source_before = snapshot_directory(&store_dir).unwrap();
        let mut prepared =
            super::prepare_closed_staging(&store_dir, crate::ClosedCompactionOptions::default())
                .unwrap();
        assert!(super::validate_closed_staging(&prepared).is_ok());
        let captured = &prepared.capture.families[0];
        let super::CapturedLogicalState::Value(snapshot) = &captured.state else {
            panic!("fixture must capture key/value state");
        };
        let mismatching = match mismatch {
            Mismatch::Family => encode_current_key_set_snapshot_with_metadata(
                &KeySetSnapshot::new(),
                captured.granularity_nanos,
                captured.last_bucket,
            )
            .unwrap(),
            Mismatch::State => {
                let mut changed = snapshot.clone();
                changed.insert(b"unexpected".to_vec(), b"state".to_vec());
                encode_current_key_value_snapshot_with_metadata(
                    &changed,
                    captured.granularity_nanos,
                    captured.last_bucket,
                )
                .unwrap()
            }
            Mismatch::Timestamp => encode_current_key_value_snapshot_with_metadata(
                snapshot,
                captured.granularity_nanos + 1,
                captured.last_bucket + 1,
            )
            .unwrap(),
        };
        let staged_active = prepared.paths.staging.join("kv.wal.dat");
        std::fs::write(&staged_active, &mismatching).unwrap();
        let descriptor = prepared
            .replacement_inventory
            .iter_mut()
            .find(|descriptor| descriptor.relative_path.ends_with("kv.wal.dat"))
            .unwrap();
        descriptor.length = u64::try_from(mismatching.len()).unwrap();
        descriptor.checksum = crc32fast::hash(&mismatching);
        let all_before_validation = snapshot_directory(root.path()).unwrap();

        assert!(super::validate_closed_staging(&prepared).is_err());

        assert_eq!(
            snapshot_directory(root.path()).unwrap(),
            all_before_validation
        );
        assert_eq!(snapshot_directory(&store_dir).unwrap(), source_before);
        assert!(!prepared.paths.manifest.exists());
        assert!(!prepared.paths.previous.exists());
    }
}

#[test]
fn final_source_inventory_rejects_add_remove_rename_and_length_changes_without_mutation() {
    #[derive(Clone, Copy)]
    enum Change {
        Add,
        Remove,
        Rename,
        Length,
    }

    for change in [Change::Add, Change::Remove, Change::Rename, Change::Length] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let prepared =
            super::prepare_closed_staging(&store_dir, crate::ClosedCompactionOptions::default())
                .unwrap();
        super::validate_closed_staging(&prepared).unwrap();
        assert!(super::revalidate_closed_source_inventory(&prepared).is_ok());

        let active = store_dir.join("kv.wal.dat");
        match change {
            Change::Add => std::fs::write(store_dir.join("unexpected"), b"added").unwrap(),
            Change::Remove => std::fs::remove_file(&active).unwrap(),
            Change::Rename => std::fs::rename(&active, store_dir.join("renamed.wal")).unwrap(),
            Change::Length => {
                use std::io::Write as _;
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&active)
                    .unwrap()
                    .write_all(b"changed-length")
                    .unwrap();
            }
        }
        let before_revalidation = snapshot_directory(root.path()).unwrap();

        assert!(super::revalidate_closed_source_inventory(&prepared).is_err());

        assert_eq!(
            snapshot_directory(root.path()).unwrap(),
            before_revalidation
        );
        assert!(!prepared.paths.manifest.exists());
        assert!(!prepared.paths.previous.exists());
    }
}
