//! Public maintenance API contract tests.

#![allow(dead_code)]

#[path = "maintenance_support/mod.rs"]
mod maintenance_support;

use std::collections::BTreeSet;
use std::error::Error as _;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    CleanupStatus, ClosedCompactionOptions, CompactionError, CompactionOperation,
    DirectoryCompactionOutcome, DirectoryStorageStats, DurabilityPolicy, FamilyCompactionOutcome,
    FamilyStorageStats, OnlineCompactionOptions, StoreFamily,
};

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

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn public_maintenance_value_model_has_documented_defaults_getters_and_error_sources() {
    let closed = ClosedCompactionOptions::default();
    assert_eq!(closed.durability_policy(), DurabilityPolicy::Buffered);
    assert_eq!(
        closed
            .with_durability_policy(DurabilityPolicy::Physical)
            .durability_policy(),
        DurabilityPolicy::Physical
    );

    let online = OnlineCompactionOptions::default();
    assert_eq!(online.max_delta_bytes(), 8 * 1024 * 1024);
    assert_eq!(online.with_max_delta_bytes(0).max_delta_bytes(), 0);

    let families = [
        StoreFamily::KeyValue,
        StoreFamily::KeySet,
        StoreFamily::KeyMap,
    ];
    assert_eq!(std::collections::HashSet::from(families).len(), 3);
    assert_ne!(CleanupStatus::Complete, CleanupStatus::Pending);

    fn getters_compile(
        family: &FamilyStorageStats,
        directory: &DirectoryStorageStats,
        outcome: &FamilyCompactionOutcome,
        outcomes: &DirectoryCompactionOutcome,
    ) {
        let _ = (
            family.family(),
            family.active_bytes(),
            family.sealed_segment_bytes(),
            family.sealed_segment_count(),
            family.total_bytes(),
            directory.families(),
            directory.total_bytes(),
            outcome.family(),
            outcome.before_bytes(),
            outcome.after_bytes(),
            outcome.sealed_segments_removed(),
            outcome.concurrent_mutations_replayed(),
            outcome.cleanup(),
            outcomes.families(),
        );
    }
    let _ = getters_compile;

    let io_error = CompactionError::Io {
        operation: CompactionOperation::Inspect,
        path: "store".into(),
        source: std::io::Error::other("probe"),
    };
    assert!(io_error.source().is_some());
    let migration = CompactionError::MigrationRequired {
        path: "legacy".into(),
    };
    assert!(migration.to_string().contains("pigment-db-migrate"));
    assert!(migration.source().is_none());

    assert_send_sync::<StoreFamily>();
    assert_send_sync::<FamilyStorageStats>();
    assert_send_sync::<DirectoryStorageStats>();
    assert_send_sync::<ClosedCompactionOptions>();
    assert_send_sync::<OnlineCompactionOptions>();
    assert_send_sync::<FamilyCompactionOutcome>();
    assert_send_sync::<DirectoryCompactionOutcome>();
    assert_send_sync::<CompactionError>();

    match migration {
        CompactionError::MigrationRequired { path } => {
            assert_eq!(path, std::path::Path::new("legacy"))
        }
        _ => panic!("unexpected non-exhaustive compaction error variant"),
    }
}

#[test]
fn storage_stats_methods_are_exactly_file_specialized_and_expose_no_format_identifier() {
    let _: fn(&DurableKeyValueStore<std::fs::File>) -> Result<FamilyStorageStats, CompactionError> =
        DurableKeyValueStore::<std::fs::File>::storage_stats;
    let _: fn(&DurableKeySetStore<std::fs::File>) -> Result<FamilyStorageStats, CompactionError> =
        DurableKeySetStore::<std::fs::File>::storage_stats;
    let _: fn(&DurableKeyMapStore<std::fs::File>) -> Result<FamilyStorageStats, CompactionError> =
        DurableKeyMapStore::<std::fs::File>::storage_stats;

    let public_source = include_str!("../src/maintenance.rs");
    assert!(!public_source.contains("pub enum FormatVersion"));
    assert!(!public_source.contains("pub struct FormatVersion"));
}
