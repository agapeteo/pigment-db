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
