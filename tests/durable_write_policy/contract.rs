//! Public API and requirement-traceability contracts.

use std::io;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::{
    DurabilityPolicy, DurabilitySupportError, DurableStoreOptions, MutationFailure,
    PersistenceOperation,
};

const COVERAGE: &[(&str, &str)] = &[
    ("FR-001", "public_policy_variants"),
    ("FR-002", "default_construction_is_buffered"),
    ("FR-003", "physical_configuration_is_additive"),
    ("FR-004", "buffered_success_contract"),
    ("FR-005", "physical_success_waits_for_barrier"),
    ("FR-006", "barrier_precedes_live_publication"),
    ("FR-007", "one_direct_barrier_per_mutation"),
    ("FR-008", "concurrent_wal_order_is_group_atomic"),
    ("FR-009", "every_mutator_reports_persistence_failure"),
    ("FR-010", "compatibility_panics_retain_cause"),
    ("FR-011", "unconfirmed_mutation_is_not_live"),
    ("FR-012", "rollback_is_fully_synchronized"),
    ("FR-013", "indeterminate_failure_fails_closed"),
    ("FR-014", "rejection_and_reopen_outcomes"),
    ("FR-015", "fresh_publication_is_physically_discoverable"),
    ("FR-016", "startup_replacement_is_physically_published"),
    ("FR-017", "cleanup_preserves_last_authority"),
    ("FR-018", "all_store_families_and_mutators"),
    ("FR-019", "physical_memory_is_rejected"),
    ("FR-020", "policy_is_runtime_only"),
    ("FR-021", "legacy_and_v1_bytes_are_unchanged"),
    ("FR-022", "public_compatibility_matrix"),
    ("FR-023", "durable_image_fault_matrix"),
    ("FR-024", "immutable_comparator_provenance"),
    ("FR-025", "buffered_cells_pass_independently"),
    ("FR-026", "physical_cells_pass_reference_gate"),
    ("FR-027", "final_report_has_72_rows"),
    ("FR-028", "coordination_stays_at_wal_boundary"),
    ("FR-029", "phase_based_capability_preflights"),
    ("FR-030", "async_compute_cancellation_boundary"),
    ("SC-001", "physical_success_survives_power_loss"),
    ("SC-002", "failure_never_publishes_live_state"),
    ("SC-003", "rollback_failure_rejects_later_writes"),
    ("SC-004", "concurrent_barrier_and_wal_order_matrix"),
    ("SC-005", "namespace_publication_survives_crash"),
    ("SC-006", "frozen_compatibility_matrix"),
    ("SC-007", "buffered_performance_gate"),
    ("SC-008", "physical_performance_gate"),
    ("SC-009", "performance_report_schema"),
    ("SC-010", "typed_and_compatibility_failure_matrix"),
    ("SC-011", "durability_rustdoc_contract"),
    ("SC-012", "all_memory_families_reject_physical"),
    ("SC-013", "all_family_indeterminate_reopen_matrix"),
    ("SC-014", "capability_failure_matrix"),
    ("SC-015", "async_cancellation_and_guard_release"),
];

#[test]
fn coverage_manifest_maps_every_requirement_once() {
    let mut requirements = COVERAGE.iter().map(|entry| entry.0).collect::<Vec<_>>();
    requirements.sort_unstable();
    requirements.dedup();
    assert_eq!(requirements.len(), 45);
    for index in 1..=30 {
        assert!(requirements.contains(&format!("FR-{index:03}").as_str()));
    }
    for index in 1..=15 {
        assert!(requirements.contains(&format!("SC-{index:03}").as_str()));
    }
    assert!(COVERAGE.iter().all(|entry| !entry.1.is_empty()));
}

#[test]
fn public_policy_and_failure_contracts_are_structured() {
    let physical =
        DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);

    assert!(matches!(
        DurableKeyValueStore::try_new_vec_based_with_options(physical),
        Err(DurabilitySupportError::NoPhysicalBacking)
    ));
    assert!(matches!(
        DurableKeySetStore::try_new_vec_based_with_options(physical),
        Err(DurabilitySupportError::NoPhysicalBacking)
    ));
    assert!(matches!(
        DurableKeyMapStore::try_new_vec_based_with_options(physical),
        Err(DurabilitySupportError::NoPhysicalBacking)
    ));

    let error = io::Error::other(MutationFailure::Rejected {
        operation: PersistenceOperation::SynchronizeData,
        source: io::Error::new(io::ErrorKind::PermissionDenied, "barrier"),
    });
    assert!(matches!(
        MutationFailure::from_io_error(&error),
        Some(MutationFailure::Rejected {
            operation: PersistenceOperation::SynchronizeData,
            ..
        })
    ));
}

#[cfg(target_os = "windows")]
#[test]
fn windows_physical_file_construction_is_supported_for_every_family() {
    let options = DurableStoreOptions::default().with_durability_policy(DurabilityPolicy::Physical);
    let values = super::support::scratch_directory("pigment-windows-physical-value-");
    drop(DurableKeyValueStore::try_init_new_with_options(values.path(), options).unwrap());
    let sets = super::support::scratch_directory("pigment-windows-physical-set-");
    drop(DurableKeySetStore::try_init_new_with_options(sets.path(), options).unwrap());
    let maps = super::support::scratch_directory("pigment-windows-physical-map-");
    drop(DurableKeyMapStore::try_init_new_with_options(maps.path(), options).unwrap());
}
