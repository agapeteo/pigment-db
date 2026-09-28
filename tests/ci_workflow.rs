use std::fs;
use std::path::Path;

/// A file's text with CRLF line endings folded to LF, so a Windows checkout matches the same
/// multi-line needles.
fn read_text(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        .replace("\r\n", "\n")
}

#[test]
fn recovery_workflow_runs_every_dedicated_issue_regression_target() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");

    for target in [
        "async_compute_conflicts",
        "ci_workflow",
        "directory_lock",
        "i128_key",
        "map_pop_return_values",
        "numeric_increment_overflow",
        "ordered_map_append",
        "sorted_map_enumeration",
        "v2_wal_segments",
    ] {
        let command = format!("cargo test --test {target} -- --test-threads=1");
        assert!(
            workflow.lines().any(|line| line.trim() == command),
            "recovery workflow must run `{command}`"
        );
    }
}

#[test]
fn recovery_workflow_runs_the_complete_suite_on_linux() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");
    let complete_linux_gate = [
        "      - name: Complete regression suite",
        "        if: runner.os == 'Linux'",
        "        run: cargo test --all-targets --all-features -- --test-threads=1",
    ]
    .join("\n");

    assert!(
        workflow.contains(&complete_linux_gate),
        "recovery workflow must run the complete all-target/all-feature suite on Linux"
    );
}

#[test]
fn recovery_workflow_does_not_use_yaml_ambiguous_inline_run_commands() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");

    let ambiguous_commands = workflow
        .lines()
        .filter_map(|line| line.trim().strip_prefix("run: "))
        .filter(|command| command.contains(": "))
        .collect::<Vec<_>>();

    assert!(
        ambiguous_commands.is_empty(),
        "inline `run:` commands containing `: ` are ambiguous YAML scalars; use a block scalar: {ambiguous_commands:?}"
    );
}

#[test]
fn windows_recovery_failure_is_reported_as_a_structured_annotation() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");

    for required in [
        "- name: Public recovery scenarios (Windows diagnostics)",
        "if: runner.os == 'Windows'",
        "shell: pwsh",
        "::error title=Windows recovery tests::",
        "exit $exit_code",
    ] {
        assert!(
            workflow.contains(required),
            "Windows recovery failures must preserve and annotate test output; missing `{required}`"
        );
    }
}

#[test]
fn windows_physical_durability_failure_is_reported_as_a_structured_annotation() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");

    for required in [
        "- name: Windows physical durability and buffered compatibility matrix",
        "if: runner.os == 'Windows'",
        "shell: pwsh",
        "cargo test --test windows_physical_durability -- --test-threads=1 --nocapture",
        "::error title=Windows physical durability tests::",
        "exit $exit_code",
    ] {
        assert!(
            workflow.contains(required),
            "Windows physical durability failures must preserve and annotate test output; missing `{required}`"
        );
    }
}

#[test]
fn windows_workflow_runs_native_boundary_and_private_physical_fault_models() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");

    for required in [
        "- name: Windows native boundary and physical fault models",
        "if: runner.os == 'Windows'",
        "cargo test durability::windows::tests -- --test-threads=1",
        "cargo test wal::durability_tests:: -- --test-threads=1",
        "cargo test compaction::recovery_tests:: -- --test-threads=1",
        "::error title=Windows native durability boundary::",
        "::error title=Windows WAL durability fault models::",
        "::error title=Windows compaction recovery fault models::",
        "$failed = $false",
        "$failed = $true",
        "if ($failed) { exit 1 }",
        "Select-Object -Last 40",
    ] {
        assert!(
            workflow.contains(required),
            "Windows CI must execute the private native boundary and physical fault-model suites; missing `{required}`"
        );
    }
}

#[test]
fn maintenance_public_api_is_narrow_while_implementation_modules_remain_private() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/maintenance.rs",
        "src/compaction/mod.rs",
        "src/compaction/inspection.rs",
        "src/compaction/manifest.rs",
        "src/compaction/publication.rs",
        "src/compaction/recovery.rs",
        "src/compaction/inspection_tests.rs",
        "src/compaction/closed_tests.rs",
        "src/compaction/recovery_tests.rs",
        "src/compaction/online_tests.rs",
        "src/wal/maintenance_tests.rs",
    ] {
        assert!(root.join(relative).is_file(), "missing private {relative}");
    }

    let crate_root = read_text(&root.join("src/lib.rs"));
    assert!(crate_root.lines().any(|line| line == "mod compaction;"));
    assert!(crate_root.lines().any(|line| line == "mod maintenance;"));
    assert!(!crate_root.contains("pub mod compaction"));
    assert!(!crate_root.contains("pub mod maintenance"));
    assert!(crate_root.contains("compact_directory_in_place, inspect_storage"));
    assert!(!crate_root.contains("pub use compaction"));

    let compaction = read_text(&root.join("src/compaction/mod.rs"));
    for module in ["inspection", "manifest", "publication", "recovery"] {
        assert!(
            compaction
                .lines()
                .any(|line| line == format!("pub(crate) mod {module};")),
            "private compaction module `{module}` is not registered"
        );
    }
    for module in [
        "inspection_tests",
        "closed_tests",
        "recovery_tests",
        "online_tests",
    ] {
        let registration = format!("mod {module};");
        assert!(compaction.contains("#[cfg(test)]"));
        assert!(compaction.lines().any(|line| line == registration));
    }

    let wal = read_text(&root.join("src/wal/mod.rs"));
    assert!(wal.contains("#[cfg(test)]\nmod maintenance_tests;"));
}

#[test]
fn windows_unsafe_and_dependency_are_confined_to_the_durability_boundary() {
    fn rust_files(directory: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_files(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let boundary = root.join("src/durability/windows.rs");
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    let unsafe_files = files
        .into_iter()
        .filter(|path| {
            fs::read_to_string(path)
                .unwrap()
                .lines()
                .any(|line| line.contains("unsafe {") || line.contains("unsafe fn"))
        })
        .collect::<Vec<_>>();
    assert_eq!(unsafe_files, [boundary]);

    let crate_root = read_text(&root.join("src/lib.rs"));
    assert!(crate_root.contains("#![deny(unsafe_code)]"));
    let cargo = read_text(&root.join("Cargo.toml"));
    assert!(cargo.contains("[target.'cfg(windows)'.dependencies]"));
    assert_eq!(cargo.matches("windows-sys").count(), 1);
    assert!(cargo.contains("features = [\"Win32_Storage_FileSystem\"]"));
}

#[test]
fn directory_ownership_tests_run_on_every_operating_system() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");
    // `run:` directly after `name:` leaves no room for an `if:` gating the step to one OS.
    let every_os_step = [
        "      - name: Directory ownership seams and cross-process claims",
        "        run: |",
        "          cargo test maintenance_coordination:: -- --test-threads=1",
        "          cargo test compaction::recovery_tests::ownership:: -- --test-threads=1",
    ]
    .join("\n");

    assert!(
        workflow.contains(&every_os_step),
        "recovery workflow must run the directory ownership unit tests on every OS"
    );
}

#[test]
fn the_declared_minimum_toolchain_checks_every_target() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("Cargo.toml"))
        .expect("read Cargo.toml")
        .replace("\r\n", "\n");
    let declared = manifest
        .lines()
        .find_map(|line| line.trim().strip_prefix("rust-version = "))
        .expect("Cargo.toml declares a rust-version")
        .trim_matches('"');
    let toolchain = if declared.matches('.').count() == 1 {
        format!("{declared}.0")
    } else {
        declared.to_owned()
    };
    let workflow_path = root.join(".github").join("workflows").join("recovery.yml");
    // A checkout with CRLF line endings would otherwise defeat every multi-line match.
    let workflow = fs::read_to_string(&workflow_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read recovery workflow {}: {error}",
                workflow_path.display()
            )
        })
        .replace("\r\n", "\n");
    // Clippy's incompatible_msrv lint misses trait impls and test code, so only a build on the
    // declared toolchain enforces it.
    let msrv_job = [
        "  minimum-supported-rust:".to_owned(),
        "    name: Minimum supported Rust".to_owned(),
        "    runs-on: ubuntu-latest".to_owned(),
        "    env:".to_owned(),
        "      CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS: fallback".to_owned(),
        "    steps:".to_owned(),
        "      - uses: actions/checkout@v4".to_owned(),
        format!("      - uses: dtolnay/rust-toolchain@{toolchain}"),
        "      - name: Check every target on the declared minimum toolchain".to_owned(),
        "        run: cargo check --all-targets".to_owned(),
    ]
    .join("\n");

    assert!(
        workflow.contains(&msrv_job),
        "recovery workflow must check every target on rust-version {declared}"
    );
}
