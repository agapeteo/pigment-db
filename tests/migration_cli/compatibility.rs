//! Frozen legacy-to-V1 migration compatibility matrix.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::RecoveryStatus;

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

fn run_migration(source: &std::path::Path, destination: &std::path::Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pigment-db-migrate"))
        .arg("--source")
        .arg(source)
        .arg("--destination")
        .arg(destination)
        .output()
        .expect("migration executable must launch")
}
