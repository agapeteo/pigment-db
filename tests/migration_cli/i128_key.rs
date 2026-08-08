//! Frozen historical I128 migration compatibility.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::{Key, SearchKey};
use pigment_db::{DurableStoreOptions, WalSegmentSize};

#[test]
fn historical_i128_sources_migrate_to_current_v2_without_source_changes() {
    for (case, fixture) in [
        (
            "legacy",
            include_str!("../fixtures/i128_key/legacy-map.hex"),
        ),
        ("v1", include_str!("../fixtures/i128_key/v1-map.hex")),
        (
            "earlier-v2",
            include_str!("../fixtures/i128_key/earlier-v2-map.hex"),
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let source_path = source.join("map.wal.dat");
        let frozen = decode_hex(fixture);
        std::fs::write(&source_path, &frozen).unwrap();
        let destination = root.path().join("destination");

        let output = run_migration(&source, &destination);

        assert!(
            output.status.success(),
            "{case}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read(&source_path).unwrap(), frozen, "{case}");
        let migrated = std::fs::read(destination.join("map.wal.dat")).unwrap();
        assert_eq!(v2_actions(&migrated), vec![6], "{case}");

        let reopened = DurableKeyMapStore::try_init_new(&destination)
            .unwrap_or_else(|error| panic!("{case}: migrated map failed to reopen: {error}"))
            .into_store();
        assert_eq!(
            reopened.get_element(
                b"retained",
                &SearchKey::from(vec![Key::I128(i128::from(u64::MAX))]),
            ),
            Some(b"old-max".to_vec()),
            "{case}",
        );
    }
}

#[test]
fn current_segmented_signed_i128_source_compacts_without_source_changes() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let minimum = SearchKey::from(vec![Key::I128(i128::MIN)]);
    let maximum = SearchKey::from(vec![Key::I128(i128::MAX)]);
    let options = DurableStoreOptions::default().with_wal_segment_size(
        WalSegmentSize::try_from(180_u64).expect("small nonzero segment target"),
    );
    {
        let store = DurableKeyMapStore::try_init_new_with_options(&source, options)
            .unwrap()
            .into_store();
        store.put(b"signed".to_vec(), minimum.clone(), b"minimum".to_vec());
        store.put(b"signed".to_vec(), maximum.clone(), b"maximum".to_vec());
    }
    let source_before = directory_bytes(&source);
    assert!(
        source_before.len() >= 2,
        "small target must create segments"
    );
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(directory_bytes(&source), source_before);
    let migrated = std::fs::read(destination.join("map.wal.dat")).unwrap();
    assert_eq!(v2_actions(&migrated), vec![6, 6]);
    let reopened = DurableKeyMapStore::try_init_new(&destination)
        .unwrap()
        .into_store();
    assert_eq!(
        reopened.get_element(b"signed", &minimum),
        Some(b"minimum".to_vec())
    );
    assert_eq!(
        reopened.get_element(b"signed", &maximum),
        Some(b"maximum".to_vec())
    );
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

fn directory_bytes(path: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn v2_actions(bytes: &[u8]) -> Vec<u8> {
    let mut actions = Vec::new();
    let mut offset = 64_usize;
    while offset < bytes.len() {
        actions.push(bytes[offset + 3]);
        let payload_len =
            u64::from_le_bytes(bytes[offset + 6..offset + 14].try_into().unwrap()) as usize;
        offset += 66 + payload_len;
    }
    assert_eq!(offset, bytes.len());
    actions
}

fn decode_hex(text: &str) -> Vec<u8> {
    let digits = text.trim().as_bytes();
    assert_eq!(digits.len() % 2, 0);
    digits
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("fixture contains a non-lowercase-hex byte"),
    }
}
