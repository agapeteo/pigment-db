//! Frozen legacy/V1-to-V2 migration and V2 compaction compatibility matrix.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::RecoveryStatus;
use pigment_db::{DurableStoreOptions, WalSegmentSize};

#[test]
fn frozen_all_family_migration_preserves_sources_and_supports_append_and_three_reopens() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let fixture_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy");
    let names = ["kv.wal.dat", "set.wal.dat", "map.wal.dat"];
    let mut frozen = Vec::new();
    for name in names {
        let bytes = std::fs::read(fixture_dir.join(name)).unwrap();
        std::fs::write(source.join(name), &bytes).unwrap();
        frozen.push((name, bytes));
    }
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(String::from_utf8(output.stdout).unwrap().lines().count(), 1);
    for (name, bytes) in &frozen {
        assert_eq!(std::fs::read(source.join(name)).unwrap(), *bytes, "{name}");
    }

    let destination_before_retry =
        names.map(|name| (name, std::fs::read(destination.join(name)).unwrap()));
    let retry = run_migration(&source, &destination);
    assert_eq!(retry.status.code(), Some(5));
    assert!(retry.stdout.is_empty());
    assert!(String::from_utf8_lossy(&retry.stderr).contains("already exists"));
    for (name, bytes) in destination_before_retry {
        assert_eq!(
            std::fs::read(destination.join(name)).unwrap(),
            bytes,
            "{name}"
        );
    }

    {
        let outcome = DurableKeyValueStore::try_init_new(&destination).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Normal);
        assert_eq!(outcome.store().get(b"alpha"), Some(b"uno".to_vec()));
        assert_eq!(outcome.store().get(b"empty"), Some(Vec::new()));
        assert_eq!(outcome.store().get(b"beta"), None);
        outcome.store().put(b"after".to_vec(), b"kv".to_vec());
    }
    {
        let outcome = DurableKeySetStore::try_init_new(&destination).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Normal);
        assert_eq!(
            outcome.store().get_hashset(b"group").unwrap(),
            std::collections::HashSet::from([b"red".to_vec()])
        );
        assert!(outcome.store().get_hashset(b"removed").is_none());
        outcome.store().append(b"after".to_vec(), b"set".to_vec());
    }
    {
        let outcome = DurableKeyMapStore::try_init_new(&destination).unwrap();
        assert_eq!(outcome.status(), RecoveryStatus::Normal);
        assert_eq!(
            outcome
                .store()
                .get_element(b"book", &SearchKey::from(2_usize)),
            Some(b"b2".to_vec())
        );
        assert_eq!(
            outcome
                .store()
                .get_element(b"other", &SearchKey::from(3_usize)),
            Some(b"c".to_vec())
        );
        outcome
            .store()
            .put(b"after".to_vec(), SearchKey::from(4_usize), b"map".to_vec());
    }

    for reopening in 1..=3 {
        let kv = DurableKeyValueStore::try_init_new(&destination).unwrap();
        assert_eq!(kv.status(), RecoveryStatus::Normal, "reopen {reopening}");
        assert_eq!(kv.store().get(b"alpha"), Some(b"uno".to_vec()));
        assert_eq!(kv.store().get(b"after"), Some(b"kv".to_vec()));
        drop(kv);

        let set = DurableKeySetStore::try_init_new(&destination).unwrap();
        assert_eq!(set.status(), RecoveryStatus::Normal, "reopen {reopening}");
        assert!(set.store().contains_in_set(b"group", b"red"));
        assert!(set.store().contains_in_set(b"after", b"set"));
        drop(set);

        let map = DurableKeyMapStore::try_init_new(&destination).unwrap();
        assert_eq!(map.status(), RecoveryStatus::Normal, "reopen {reopening}");
        assert_eq!(
            map.store().get_element(b"after", &SearchKey::from(4_usize)),
            Some(b"map".to_vec()),
            "reopen {reopening}"
        );
    }

    for (name, bytes) in frozen {
        assert_eq!(std::fs::read(source.join(name)).unwrap(), bytes, "{name}");
    }
}

#[test]
fn complete_v1_source_is_offline_migrated_to_v2_without_source_changes() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let source_file = source.join("kv.wal.dat");
    let v1 = empty_v1_header(1, 60_000_000_000);
    std::fs::write(&source_file, v1).unwrap();
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(&source_file).unwrap(), v1);
    let migrated = std::fs::read(destination.join("kv.wal.dat")).unwrap();
    assert_eq!(u16::from_le_bytes(migrated[8..10].try_into().unwrap()), 2);
    let _ = DurableKeyValueStore::try_init_new(&destination)
        .expect("migrated empty V1 source must open as V2");
}

#[test]
fn recoverable_v1_terminal_tail_is_migrated_from_its_complete_prefix() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let source_file = source.join("kv.wal.dat");
    let mut v1_with_tail = empty_v1_header(1, 60_000_000_000).to_vec();
    v1_with_tail.push(0xa7);
    std::fs::write(&source_file, &v1_with_tail).unwrap();
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(&source_file).unwrap(), v1_with_tail);
    let migrated = std::fs::read(destination.join("kv.wal.dat")).unwrap();
    assert_eq!(u16::from_le_bytes(migrated[8..10].try_into().unwrap()), 2);
    let outcome = DurableKeyValueStore::try_init_new(&destination)
        .expect("migrated complete V1 prefix must open");
    assert!(outcome.store().get(b"missing").is_none());
}

#[test]
fn v1_migration_preserves_the_last_accepted_timestamp_bucket() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let source_file = source.join("kv.wal.dat");
    let expected_bucket = 1_840_000_000_000_000_000_u64;
    let mut v1 = empty_v1_header(1, 60_000_000_000);
    v1[24..32].copy_from_slice(&expected_bucket.to_le_bytes());
    let crc = crc32fast::hash(&v1[..36]);
    v1[36..40].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&source_file, v1).unwrap();
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let migrated = std::fs::read(destination.join("kv.wal.dat")).unwrap();
    assert_eq!(
        u64::from_le_bytes(migrated[24..32].try_into().unwrap()),
        expected_bucket
    );
}

#[test]
fn segmented_v2_source_is_offline_compacted_to_one_v2_segment() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(170_u64).expect("small nonzero segment target"),
    );
    {
        let store = DurableKeyValueStore::try_init_new_with_options(&source, options)
            .unwrap()
            .into_store();
        store.put(b"first".to_vec(), b"one".to_vec());
        store.put(b"second".to_vec(), b"two".to_vec());
    }
    let source_before = directory_bytes(&source);
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(directory_bytes(&source), source_before);
    assert!(destination.join("kv.wal.dat").is_file());
    assert_eq!(
        std::fs::read_dir(&destination).unwrap().count(),
        1,
        "offline compaction must emit one active V2 segment"
    );
    let reopened = DurableKeyValueStore::try_init_new(&destination)
        .expect("compacted V2 output must reopen")
        .into_store();
    assert_eq!(reopened.get(b"first"), Some(b"one".to_vec()));
    assert_eq!(reopened.get(b"second"), Some(b"two".to_vec()));
}

fn directory_bytes(directory: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn empty_v1_header(kind: u8, granularity: u64) -> [u8; 40] {
    let mut header = [0_u8; 40];
    header[..8].copy_from_slice(b"PIGWAL\r\n");
    header[8..10].copy_from_slice(&1_u16.to_le_bytes());
    header[10..12].copy_from_slice(&40_u16.to_le_bytes());
    header[12] = kind;
    header[13] = 1;
    header[16..24].copy_from_slice(&granularity.to_le_bytes());
    let crc = crc32fast::hash(&header[..36]);
    header[36..40].copy_from_slice(&crc.to_le_bytes());
    header
}

fn run_migration(source: &std::path::Path, destination: &std::path::Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pigment-db-migrate"))
        .arg("--source")
        .arg(source)
        .arg("--destination")
        .arg(destination)
        .output()
        .expect("migration executable must launch")
}
