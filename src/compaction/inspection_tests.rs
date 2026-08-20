//! Private inspection behavior tests.

use super::inspection::{inspect_directory, InspectedFamily};
use crate::test_support::maintenance_fixtures::{
    create_current_v2, snapshot_directory, FixtureFamily,
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
