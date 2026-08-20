use std::fs;
use std::path::Path;

#[test]
fn recovery_workflow_runs_every_dedicated_issue_regression_target() {
    let workflow_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github")
        .join("workflows")
        .join("recovery.yml");
    let workflow = fs::read_to_string(&workflow_path).unwrap_or_else(|error| {
        panic!(
            "failed to read recovery workflow {}: {error}",
            workflow_path.display()
        )
    });

    for target in [
        "async_compute_conflicts",
        "ci_workflow",
        "i128_key",
        "map_pop_return_values",
        "numeric_increment_overflow",
        "ordered_map_append",
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
    let workflow = fs::read_to_string(&workflow_path).unwrap_or_else(|error| {
        panic!(
            "failed to read recovery workflow {}: {error}",
            workflow_path.display()
        )
    });
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
fn maintenance_skeletons_are_registered_without_public_crate_exports() {
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

    let crate_root = fs::read_to_string(root.join("src/lib.rs")).expect("read crate root");
    assert!(crate_root.lines().any(|line| line == "mod compaction;"));
    assert!(crate_root.lines().any(|line| line == "mod maintenance;"));
    assert!(!crate_root.contains("pub mod compaction"));
    assert!(!crate_root.contains("pub mod maintenance"));
    assert!(!crate_root.contains("pub use maintenance"));

    let compaction = fs::read_to_string(root.join("src/compaction/mod.rs"))
        .expect("read compaction module root");
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

    let wal = fs::read_to_string(root.join("src/wal/mod.rs")).expect("read WAL module root");
    assert!(wal.contains("#[cfg(test)]\nmod maintenance_tests;"));
}
