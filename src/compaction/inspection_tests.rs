//! Private inspection behavior tests.

use super::inspection::{inspect_directory, InspectedFamily};
use crate::test_support::maintenance_fixtures::{
    active_name, create_current_v2, create_safe_tail_v2, create_segmented_v2, sealed_name,
    snapshot_directory, FixtureFamily,
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

type InvalidArtifactCase = (&'static str, fn(&std::path::Path));

#[test]
fn invalid_artifacts_are_rejected_without_mutation() {
    let cases: &[InvalidArtifactCase] = &[
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

#[test]
fn recoverable_terminal_tail_is_measured_without_repair() {
    for family in [
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
        FixtureFamily::KeyMap,
    ] {
        let directory = tempfile::tempdir().unwrap();
        create_safe_tail_v2(directory.path(), family);
        let before = snapshot_directory(directory.path()).unwrap();
        let active_bytes = u64::try_from(
            before
                .get(std::path::Path::new(active_name(family)))
                .unwrap()
                .len(),
        )
        .unwrap();

        let inspected = inspect_directory(directory.path()).unwrap();

        assert_eq!(inspected.families.len(), 1);
        assert_eq!(inspected.families[0].active_bytes, active_bytes);
        assert_eq!(inspected.families[0].sealed_segment_bytes, 0);
        assert_eq!(inspected.families[0].sealed_segment_count, 0);
        assert_eq!(inspected.families[0].total_bytes, active_bytes);
        assert_eq!(inspected.total_bytes, active_bytes);
        assert_eq!(snapshot_directory(directory.path()).unwrap(), before);
    }
}

fn v1_header(kind: u8) -> [u8; 40] {
    let mut bytes = [0; 40];
    bytes[..8].copy_from_slice(b"PIGWAL\r\n");
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&40_u16.to_le_bytes());
    bytes[12] = kind;
    bytes[13] = 1;
    bytes[16..24].copy_from_slice(&60_000_000_000_u64.to_le_bytes());
    let crc = crc32fast::hash(&bytes[..36]);
    bytes[36..40].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn opaque_v1_value_envelope() -> Vec<u8> {
    use crate::wal::format::{RecordProbeFields, V1CodecProbe};

    let mut bytes = V1CodecProbe::encode_header_with_kind(1).to_vec();
    bytes.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
        action: 1,
        payload: &[0xff],
        physical_start: V1CodecProbe::HEADER_LEN as u32,
        mutation_start: V1CodecProbe::HEADER_LEN as u32,
        index: 0,
        count: 1,
        timestamp_bucket: 0,
    }));
    bytes
}

#[test]
fn recognized_older_envelopes_require_external_migration_without_mutation() {
    let cases = [
        (
            "legacy key/value",
            FixtureFamily::KeyValue,
            include_bytes!("../../tests/fixtures/legacy/kv.wal.dat").to_vec(),
        ),
        (
            "legacy key/set",
            FixtureFamily::KeySet,
            include_bytes!("../../tests/fixtures/legacy/set.wal.dat").to_vec(),
        ),
        (
            "legacy key/map",
            FixtureFamily::KeyMap,
            include_bytes!("../../tests/fixtures/legacy/map.wal.dat").to_vec(),
        ),
        (
            "V1 key/value",
            FixtureFamily::KeyValue,
            v1_header(1).to_vec(),
        ),
        ("V1 key/set", FixtureFamily::KeySet, v1_header(2).to_vec()),
        ("V1 key/map", FixtureFamily::KeyMap, v1_header(3).to_vec()),
        (
            "V1 opaque application payload",
            FixtureFamily::KeyValue,
            opaque_v1_value_envelope(),
        ),
    ];

    for (name, family, bytes) in &cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(active_name(*family));
        std::fs::write(&path, bytes).unwrap();
        let before = snapshot_directory(directory.path()).unwrap();

        let error = inspect_directory(directory.path()).unwrap_err();
        let message = error.to_string();

        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported, "{name}");
        assert!(
            message.contains(path.to_str().unwrap()),
            "{name}: {message}"
        );
        assert!(message.contains("pigment-db-migrate"), "{name}: {message}");
        assert_eq!(
            snapshot_directory(directory.path()).unwrap(),
            before,
            "{name} must remain byte-identical"
        );
    }
}

fn sibling_maintenance_path(store_dir: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut name = std::ffi::OsString::from(".");
    name.push(store_dir.file_name().unwrap());
    name.push(".pigment-compact.");
    name.push(suffix);
    store_dir.parent().unwrap().join(name)
}

#[test]
fn complete_competing_generations_are_ambiguous_but_non_competing_debris_is_invalid() {
    for suffix in ["previous", "next"] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_current_v2(&store_dir, FixtureFamily::KeyValue);
        let competitor = sibling_maintenance_path(&store_dir, suffix);
        std::fs::create_dir(&competitor).unwrap();
        create_current_v2(&competitor, FixtureFamily::KeyValue);
        let before = snapshot_directory(root.path()).unwrap();

        let error = inspect_directory(&store_dir).unwrap_err();
        let message = error.to_string();

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists, "{suffix}");
        assert!(
            message.contains(store_dir.to_str().unwrap()),
            "{suffix}: {message}"
        );
        assert!(
            message.contains(competitor.to_str().unwrap()),
            "{suffix}: {message}"
        );
        assert_eq!(snapshot_directory(root.path()).unwrap(), before, "{suffix}");
    }

    for suffix in ["previous", "next"] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_current_v2(&store_dir, FixtureFamily::KeyValue);
        let debris = sibling_maintenance_path(&store_dir, suffix);
        std::fs::write(&debris, b"incomplete maintenance debris").unwrap();
        let before = snapshot_directory(root.path()).unwrap();

        let error = inspect_directory(&store_dir).unwrap_err();
        let message = error.to_string();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{suffix}");
        assert!(
            message.contains(debris.to_str().unwrap()),
            "{suffix}: {message}"
        );
        assert_eq!(snapshot_directory(root.path()).unwrap(), before, "{suffix}");
    }
}

#[test]
fn open_store_adapters_report_only_their_current_family_without_recovery() {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    for family in [
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
        FixtureFamily::KeyMap,
    ] {
        create_segmented_v2(&store_dir, family);
    }
    let value = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();
    let set = crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();
    let map = crate::key_map_store::DurableKeyMapStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();

    let previous = sibling_maintenance_path(&store_dir, "previous");
    std::fs::create_dir(&previous).unwrap();
    create_current_v2(&previous, FixtureFamily::KeyValue);
    std::fs::write(
        store_dir.join(".kv.wal.dat.next"),
        b"unresolved recovery evidence",
    )
    .unwrap();
    let before = snapshot_directory(root.path()).unwrap();
    let store_snapshot = snapshot_directory(&store_dir).unwrap();

    let cases = [
        (
            value.storage_stats_probe(),
            InspectedFamily::KeyValue,
            FixtureFamily::KeyValue,
        ),
        (
            set.storage_stats_probe(),
            InspectedFamily::KeySet,
            FixtureFamily::KeySet,
        ),
        (
            map.storage_stats_probe(),
            InspectedFamily::KeyMap,
            FixtureFamily::KeyMap,
        ),
    ];
    for (actual, inspected_family, fixture_family) in cases {
        let actual = actual.unwrap();
        let active_path = std::path::Path::new(active_name(fixture_family));
        let active_bytes = u64::try_from(store_snapshot.get(active_path).unwrap().len()).unwrap();
        let prefix = format!("{}.segment-", active_name(fixture_family).to_string_lossy());
        let sealed: Vec<_> = store_snapshot
            .iter()
            .filter(|(path, _)| path.to_string_lossy().starts_with(&prefix))
            .collect();
        let sealed_bytes = sealed
            .iter()
            .map(|(_, bytes)| u64::try_from(bytes.len()).unwrap())
            .sum::<u64>();
        assert_eq!(actual.family, inspected_family);
        assert_eq!(actual.active_bytes, active_bytes);
        assert_eq!(actual.sealed_segment_bytes, sealed_bytes);
        assert_eq!(actual.sealed_segment_count, sealed.len());
        assert_eq!(actual.total_bytes, active_bytes + sealed_bytes);
    }
    assert_eq!(snapshot_directory(root.path()).unwrap(), before);
}
