//! Migration filesystem, validation, output, and exit failure matrix.

#[test]
fn usage_failures_exit_two_with_one_usage_line_and_no_stdout() {
    for args in [
        vec!["--unknown", "value"],
        vec![
            "--source",
            "one",
            "--source",
            "two",
            "--destination",
            "dest",
        ],
        vec!["--source"],
        vec![
            "--source",
            "source",
            "--destination",
            "dest",
            "--timestamp-granularity-nanos",
            "0",
        ],
    ] {
        let output = run_raw(&args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.starts_with("error:"), "{args:?}: {stderr}");
        assert_eq!(stderr.matches("Usage:").count(), 1, "{args:?}: {stderr}");
    }
}

#[test]
fn unavailable_source_exits_three_without_creating_destination() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("missing-source");
    let destination = root.path().join("destination");

    let output = run_migration(&source, &destination);

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.starts_with("error:"));
    assert!(stderr.contains(source.to_str().unwrap()));
    assert!(!destination.exists());
}

#[test]
fn nonmigratable_sources_exit_four_and_remain_byte_identical() {
    for case in ["v1", "truncated", "corrupt", "unresolved"] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let source_file = source.join("kv.wal.dat");
        let mut bytes = frozen_key_value();
        match case {
            "v1" => bytes = b"PIGWAL\r\n".to_vec(),
            "truncated" => {
                bytes.pop();
            }
            "corrupt" => bytes[1] ^= 0xff,
            "unresolved" => {
                std::fs::write(source.join(".kv.wal.dat.next"), b"diagnostic").unwrap();
            }
            _ => unreachable!(),
        }
        std::fs::write(&source_file, &bytes).unwrap();
        let destination = root.path().join("destination");

        let output = run_migration(&source, &destination);

        assert_eq!(output.status.code(), Some(4), "{case}");
        assert!(output.stdout.is_empty(), "{case}");
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .starts_with("error:"));
        assert_eq!(std::fs::read(source_file).unwrap(), bytes, "{case}");
        assert!(!destination.exists(), "{case}");
    }
}

#[test]
fn every_existing_destination_shape_exits_five_without_overwrite() {
    for shape in ["file", "directory", "nonempty-directory"] {
        let root = tempfile::tempdir().unwrap();
        let source = create_source(root.path());
        let destination = root.path().join("destination");
        match shape {
            "file" => std::fs::write(&destination, b"existing-file").unwrap(),
            "directory" => std::fs::create_dir(&destination).unwrap(),
            "nonempty-directory" => {
                std::fs::create_dir(&destination).unwrap();
                std::fs::write(destination.join("owned-by-user"), b"existing").unwrap();
            }
            _ => unreachable!(),
        }
        let before = destination_snapshot(&destination);

        let output = run_migration(&source, &destination);

        assert_eq!(output.status.code(), Some(5), "{shape}");
        assert!(output.stdout.is_empty(), "{shape}");
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("already exists"));
        assert_eq!(destination_snapshot(&destination), before, "{shape}");
    }
}

#[cfg(unix)]
#[test]
fn existing_destination_symlink_exits_five_without_touching_target() {
    let root = tempfile::tempdir().unwrap();
    let source = create_source(root.path());
    let target = root.path().join("target");
    std::fs::write(&target, b"target-bytes").unwrap();
    let destination = root.path().join("destination");
    std::os::unix::fs::symlink(&target, &destination).unwrap();

    let output = run_migration(&source, &destination);

    assert_eq!(output.status.code(), Some(5));
    assert_eq!(std::fs::read(target).unwrap(), b"target-bytes");
    assert!(std::fs::symlink_metadata(destination)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[test]
fn destination_creation_failure_exits_six_and_preserves_source() {
    let root = tempfile::tempdir().unwrap();
    let source = create_source(root.path());
    let source_file = source.join("kv.wal.dat");
    let before = std::fs::read(&source_file).unwrap();
    let destination = root.path().join("missing-parent").join("destination");

    let output = run_migration(&source, &destination);

    assert_eq!(output.status.code(), Some(6));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .starts_with("error:"));
    assert_eq!(std::fs::read(source_file).unwrap(), before);
    assert!(!destination.exists());
}

fn create_source(root: &std::path::Path) -> std::path::PathBuf {
    let source = root.join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("kv.wal.dat"), frozen_key_value()).unwrap();
    source
}

fn frozen_key_value() -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy/kv.wal.dat"),
    )
    .unwrap()
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

fn run_raw(args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pigment-db-migrate"))
        .args(args)
        .output()
        .expect("migration executable must launch")
}

fn destination_snapshot(path: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    if path.is_file() {
        return vec![(
            path.file_name().unwrap().to_owned(),
            std::fs::read(path).unwrap(),
        )];
    }
    let mut snapshot = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect::<Vec<_>>();
    snapshot.sort_by(|left, right| left.0.cmp(&right.0));
    snapshot
}
