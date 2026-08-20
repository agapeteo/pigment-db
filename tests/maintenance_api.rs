//! Public maintenance API contract tests.

#![allow(dead_code)]

#[path = "maintenance_support/mod.rs"]
mod maintenance_support;

use std::collections::BTreeSet;

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CoverageCase {
    requirement: String,
    test_name: String,
}

fn coverage_manifest() -> Vec<CoverageCase> {
    (1..=94)
        .map(|number| CoverageCase {
            requirement: format!("FR-{number:03}"),
            test_name: format!("maintenance_fr_{number:03}_contract"),
        })
        .chain((1..=10).map(|number| CoverageCase {
            requirement: format!("SC-{number:03}"),
            test_name: format!("maintenance_sc_{number:03}_acceptance"),
        }))
        .collect()
}

#[test]
fn requirements_coverage_manifest_maps_every_fr_and_sc_exactly_once() {
    let cases = coverage_manifest();
    let ids: BTreeSet<_> = cases.iter().map(|case| case.requirement.as_str()).collect();
    let names: BTreeSet<_> = cases.iter().map(|case| case.test_name.as_str()).collect();
    assert_eq!(cases.len(), 104);
    assert_eq!(ids.len(), 104, "requirement IDs must not repeat");
    assert_eq!(names.len(), 104, "auditable test names must not repeat");

    let spec = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("specs/008-current-compaction-durability/spec.md"),
    )
    .expect("read feature specification");
    let documented: BTreeSet<_> = spec
        .lines()
        .filter_map(|line| {
            line.strip_prefix("- **")
                .and_then(|line| line.split_once("**:"))
                .map(|(id, _)| id)
                .filter(|id| id.starts_with("FR-") || id.starts_with("SC-"))
        })
        .collect();
    assert_eq!(ids, documented, "manifest and specification IDs diverged");
}
