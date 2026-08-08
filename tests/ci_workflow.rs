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
