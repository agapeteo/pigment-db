//! Private inspection behavior tests.

use super::inspection::{inspect_directory, InspectedFamily};
use crate::test_support::maintenance_fixtures::{
    active_name, create_current_v2, create_segmented_v2, sealed_name, snapshot_directory,
    FixtureFamily,
};

#[test]
fn active_current_v2_families_report_exact_bytes_in_deterministic_order() {
    let directory = tempfile::tempdir().unwrap();
    for family in [
        FixtureFamily::KeyMap,
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
    ] {
        create_current_v2(directory.path(), family);
    }
    let before = snapshot_directory(directory.path()).unwrap();

    let inspected = inspect_directory(directory.path()).unwrap();

    let expected_families = [
        (InspectedFamily::KeyValue, "kv.wal.dat"),
        (InspectedFamily::KeySet, "set.wal.dat"),
        (InspectedFamily::KeyMap, "map.wal.dat"),
    ];
    assert_eq!(inspected.families.len(), expected_families.len());
    let mut expected_total = 0_u64;
    for (actual, (family, name)) in inspected.families.iter().zip(expected_families) {
        let bytes = u64::try_from(before.get(std::path::Path::new(name)).unwrap().len()).unwrap();
        assert_eq!(actual.family, family);
        assert_eq!(actual.active_bytes, bytes);
        assert_eq!(actual.sealed_segment_bytes, 0);
        assert_eq!(actual.sealed_segment_count, 0);
        assert_eq!(actual.total_bytes, bytes);
        expected_total += bytes;
    }
    assert_eq!(inspected.total_bytes, expected_total);
    assert_eq!(snapshot_directory(directory.path()).unwrap(), before);
}

#[test]
fn segmented_storage_is_exact_and_unexpected_entries_fail_without_mutation() {
    let segmented = tempfile::tempdir().unwrap();
    create_current_v2(segmented.path(), FixtureFamily::KeyValue);
    let store = crate::key_value_store::DurableKeyValueStore::try_init_new_with_options(
        segmented.path(),
        crate::DurableStoreOptions::default()
            .with_wal_segment_size(crate::WalSegmentSize::try_from(170_u64).unwrap()),
    )
    .unwrap()
    .into_store();
    store.put(b"beta".to_vec(), b"two".to_vec());
    store.put(b"gamma".to_vec(), b"three".to_vec());
    drop(store);
    let before = snapshot_directory(segmented.path()).unwrap();
    let inspected = inspect_directory(segmented.path()).unwrap();
    let family = &inspected.families[0];
    let active = u64::try_from(
        before
            .get(std::path::Path::new("kv.wal.dat"))
            .unwrap()
            .len(),
    )
    .unwrap();
    let sealed: Vec<_> = before
        .iter()
        .filter(|(name, _)| name.to_string_lossy().contains(".segment-"))
        .collect();
    let sealed_bytes = sealed
        .iter()
        .map(|(_, bytes)| u64::try_from(bytes.len()).unwrap())
        .sum::<u64>();
    assert_eq!(family.active_bytes, active);
    assert_eq!(family.sealed_segment_bytes, sealed_bytes);
    assert_eq!(family.sealed_segment_count, sealed.len());
    assert_eq!(family.total_bytes, active + sealed_bytes);
    assert_eq!(snapshot_directory(segmented.path()).unwrap(), before);

    let unexpected = tempfile::tempdir().unwrap();
    create_current_v2(unexpected.path(), FixtureFamily::KeyValue);
    std::fs::write(unexpected.path().join("notes.txt"), b"caller data").unwrap();
    let before = snapshot_directory(unexpected.path()).unwrap();
    let error = inspect_directory(unexpected.path()).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(snapshot_directory(unexpected.path()).unwrap(), before);
}

fn malformed_segment_name(root: &std::path::Path) {
    create_segmented_v2(root, FixtureFamily::KeyValue);
    std::fs::rename(
        root.join(sealed_name(FixtureFamily::KeyValue, 0)),
        root.join("kv.wal.dat.segment-0"),
    )
    .unwrap();
}

fn missing_leading_segment(root: &std::path::Path) {
    create_current_v2(root, FixtureFamily::KeyValue);
    std::fs::copy(
        root.join(active_name(FixtureFamily::KeyValue)),
        root.join(sealed_name(FixtureFamily::KeyValue, 1)),
    )
    .unwrap();
}

fn missing_middle_segment(root: &std::path::Path) {
    create_current_v2(root, FixtureFamily::KeyValue);
    for id in [0, 2] {
        std::fs::copy(
            root.join(active_name(FixtureFamily::KeyValue)),
            root.join(sealed_name(FixtureFamily::KeyValue, id)),
        )
        .unwrap();
    }
}

fn wrong_family_artifact(root: &std::path::Path) {
    create_current_v2(root, FixtureFamily::KeyMap);
    std::fs::rename(
        root.join(active_name(FixtureFamily::KeyMap)),
        root.join(active_name(FixtureFamily::KeyValue)),
    )
    .unwrap();
}

fn corrupt_active_header(root: &std::path::Path) {
    create_current_v2(root, FixtureFamily::KeyValue);
    let path = root.join(active_name(FixtureFamily::KeyValue));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn corrupt_active_record(root: &std::path::Path) {
    create_current_v2(root, FixtureFamily::KeyValue);
    let path = root.join(active_name(FixtureFamily::KeyValue));
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn corrupt_sealed_record(root: &std::path::Path) {
    create_segmented_v2(root, FixtureFamily::KeyValue);
    let path = root.join(sealed_name(FixtureFamily::KeyValue, 0));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn unknown_entry(root: &std::path::Path) {
    create_current_v2(root, FixtureFamily::KeyValue);
    std::fs::write(root.join("notes.txt"), b"not a Pigment DB artifact").unwrap();
}

#[test]
fn invalid_artifacts_are_rejected_without_mutation() {
    let cases: &[(&str, fn(&std::path::Path))] = &[
        ("malformed segment name", malformed_segment_name),
        ("missing leading segment", missing_leading_segment),
        ("missing middle segment", missing_middle_segment),
        ("wrong-family canonical artifact", wrong_family_artifact),
        ("corrupt active header", corrupt_active_header),
        ("corrupt active record", corrupt_active_record),
        ("corrupt sealed record", corrupt_sealed_record),
        ("unknown entry", unknown_entry),
    ];

    for (name, arrange) in cases {
        let directory = tempfile::tempdir().unwrap();
        arrange(directory.path());
        let before = snapshot_directory(directory.path()).unwrap();

        let error = match inspect_directory(directory.path()) {
            Ok(_) => panic!("{name} must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{name}");
        assert_eq!(
            snapshot_directory(directory.path()).unwrap(),
            before,
            "{name} must remain byte-identical"
        );
    }
}
