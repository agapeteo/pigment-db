//! Child-process interruption regressions for migration publication.

#[test]
fn child_termination_leaves_only_diagnostic_destination_artifacts() {
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy/kv.wal.dat");
    let fixture_bytes = std::fs::read(&fixture).unwrap();

    for checkpoint in [
        "destination-created",
        "partial-output-written",
        "complete-output-written",
        "output-validated",
        "before-success-output",
    ] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let source_file = source.join("kv.wal.dat");
        std::fs::write(&source_file, &fixture_bytes).unwrap();
        let destination = root.path().join("destination");

        let output = std::process::Command::new(env!("CARGO_BIN_EXE_pigment-db-migrate"))
            .args(["--source", source.to_str().unwrap()])
            .args(["--destination", destination.to_str().unwrap()])
            .env("PIGMENT_DB_MIGRATION_TEST_CHECKPOINT", checkpoint)
            .output()
            .expect("checkpoint child must launch");

        assert_eq!(output.status.code(), Some(86), "{checkpoint}");
        assert!(output.stdout.is_empty(), "{checkpoint}");
        assert!(output.stderr.is_empty(), "{checkpoint}");
        assert_eq!(
            std::fs::read(&source_file).unwrap(),
            fixture_bytes,
            "{checkpoint}"
        );
        assert!(destination.is_dir(), "{checkpoint}");
        let output_path = destination.join("kv.wal.dat");
        match checkpoint {
            "destination-created" => assert!(!output_path.exists()),
            "partial-output-written" => {
                let len = std::fs::metadata(output_path).unwrap().len();
                assert!(len > 0 && len < 40, "partial length was {len}");
            }
            _ => assert!(std::fs::metadata(output_path).unwrap().len() >= 40),
        }
    }
}
