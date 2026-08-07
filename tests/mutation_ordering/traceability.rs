//! Public contract-ID and mutation-family traceability tests.

use std::collections::HashSet;

#[derive(Clone, Copy)]
struct ContractCase {
    id: &'static str,
    test: &'static str,
}

#[derive(Clone, Copy)]
struct FamilyCase {
    store: &'static str,
    family: &'static str,
    test: &'static str,
}

const EXPECTED_CONTRACT_IDS: &[&str] = &[
    "CMO-ORDER-1",
    "CMO-ORDER-2",
    "CMO-ORDER-3",
    "CMO-READ-1",
    "CMO-READ-2",
    "CMO-CALL-1",
    "CMO-CALL-2",
    "CMO-CALL-3",
    "CMO-PREFIX-1",
    "CMO-PREFIX-2",
    "CMO-PREFIX-3",
    "CMO-PREFIX-4",
    "CMO-CROSS-1",
    "CMO-CROSS-2",
    "CMO-CROSS-3",
    "CMO-CROSS-4",
    "CMO-FAIL-1",
    "CMO-FAIL-2",
    "CMO-FAIL-3",
    "CMO-FAIL-4",
];

const EXPECTED_FAMILIES: &[(&str, &str)] = &[
    ("key/value", "put"),
    ("key/value", "compute"),
    ("key/value", "increment_or_init"),
    ("key/value", "decrement"),
    ("key/value", "set_number"),
    ("key/value", "remove"),
    ("key/set", "append"),
    ("key/set", "remove_from_set"),
    ("key/set", "remove_from_set_callback"),
    ("key/set", "remove_key"),
    ("key/set", "try_compute"),
    ("key/set", "compute"),
    ("key/set", "try_compute_async"),
    ("key/set", "compute_async"),
    ("key/set", "try_compute_if_present"),
    ("key/set", "compute_if_present"),
    ("key/set", "try_compute_if_absent"),
    ("key/set", "compute_if_absent"),
    ("key/sorted-map", "put"),
    ("key/sorted-map", "remove_from_sorted_map"),
    ("key/sorted-map", "remove_from_sorted_map_callback"),
    ("key/sorted-map", "remove_key"),
    ("key/sorted-map", "pop_first"),
    ("key/sorted-map", "pop_last"),
    ("key/sorted-map", "append_ordered_element"),
    ("key/sorted-map", "try_compute"),
    ("key/sorted-map", "compute"),
    ("key/sorted-map", "try_compute_if_present"),
    ("key/sorted-map", "compute_if_present"),
    ("key/sorted-map", "try_compute_if_absent"),
    ("key/sorted-map", "compute_if_absent"),
];

const CONTRACT_CASES: &[ContractCase] = &[
    ContractCase {
        id: "CMO-ORDER-1",
        test: "contract_order_nonoverlap",
    },
    ContractCase {
        id: "CMO-ORDER-2",
        test: "overlap_uses_one_live_and_reopened_order",
    },
    ContractCase {
        id: "CMO-ORDER-3",
        test: "multi_action_batch_is_indivisible",
    },
    ContractCase {
        id: "CMO-READ-1",
        test: "callback_working_state_is_invisible",
    },
    ContractCase {
        id: "CMO-READ-2",
        test: "accepted_before_publication_read_is_atomic",
    },
    ContractCase {
        id: "CMO-CALL-1",
        test: "eligible_callback_runs_once",
    },
    ContractCase {
        id: "CMO-CALL-2",
        test: "ineligible_callback_is_not_invoked",
    },
    ContractCase {
        id: "CMO-CALL-3",
        test: "panic_or_cancel_discards_candidate",
    },
    ContractCase {
        id: "CMO-PREFIX-1",
        test: "interrupt_before_acceptance_reopens_prior_prefix",
    },
    ContractCase {
        id: "CMO-PREFIX-2",
        test: "interrupt_after_acceptance_reopens_complete_mutation",
    },
    ContractCase {
        id: "CMO-PREFIX-3",
        test: "interrupt_after_publication_reopens_published_state",
    },
    ContractCase {
        id: "CMO-PREFIX-4",
        test: "interrupted_contender_contributes_no_action",
    },
    ContractCase {
        id: "CMO-CROSS-1",
        test: "different_shard_progresses_during_preparation",
    },
    ContractCase {
        id: "CMO-CROSS-2",
        test: "different_shard_waits_only_for_wal_acceptance",
    },
    ContractCase {
        id: "CMO-CROSS-3",
        test: "different_shard_progresses_before_publication",
    },
    ContractCase {
        id: "CMO-CROSS-4",
        test: "same_shard_contention_preserves_independent_state",
    },
    ContractCase {
        id: "CMO-FAIL-1",
        test: "explicit_write_error_restores_checkpoint",
    },
    ContractCase {
        id: "CMO-FAIL-2",
        test: "flush_error_restores_checkpoint",
    },
    ContractCase {
        id: "CMO-FAIL-3",
        test: "rollback_failure_marks_wal_fail_closed",
    },
    ContractCase {
        id: "CMO-FAIL-4",
        test: "compatibility_panic_occurs_after_guard_release",
    },
];

// Each name identifies one independently auditable row. Multiple rows may be
// exercised by the same table-driven public test, but no row can silently
// disappear or alias another family.
const FAMILY_CASES: &[FamilyCase] = &[
    FamilyCase {
        store: "key/value",
        family: "put",
        test: "key_value_family_put",
    },
    FamilyCase {
        store: "key/value",
        family: "compute",
        test: "key_value_family_compute",
    },
    FamilyCase {
        store: "key/value",
        family: "increment_or_init",
        test: "key_value_family_increment_or_init",
    },
    FamilyCase {
        store: "key/value",
        family: "decrement",
        test: "key_value_family_decrement",
    },
    FamilyCase {
        store: "key/value",
        family: "set_number",
        test: "key_value_family_set_number",
    },
    FamilyCase {
        store: "key/value",
        family: "remove",
        test: "key_value_family_remove",
    },
    FamilyCase {
        store: "key/set",
        family: "append",
        test: "key_set_family_append",
    },
    FamilyCase {
        store: "key/set",
        family: "remove_from_set",
        test: "key_set_family_remove_from_set",
    },
    FamilyCase {
        store: "key/set",
        family: "remove_from_set_callback",
        test: "key_set_family_remove_from_set_callback",
    },
    FamilyCase {
        store: "key/set",
        family: "remove_key",
        test: "key_set_family_remove_key",
    },
    FamilyCase {
        store: "key/set",
        family: "try_compute",
        test: "key_set_family_try_compute",
    },
    FamilyCase {
        store: "key/set",
        family: "compute",
        test: "key_set_family_compute",
    },
    FamilyCase {
        store: "key/set",
        family: "try_compute_async",
        test: "key_set_family_try_compute_async",
    },
    FamilyCase {
        store: "key/set",
        family: "compute_async",
        test: "key_set_family_compute_async",
    },
    FamilyCase {
        store: "key/set",
        family: "try_compute_if_present",
        test: "key_set_family_try_compute_if_present",
    },
    FamilyCase {
        store: "key/set",
        family: "compute_if_present",
        test: "key_set_family_compute_if_present",
    },
    FamilyCase {
        store: "key/set",
        family: "try_compute_if_absent",
        test: "key_set_family_try_compute_if_absent",
    },
    FamilyCase {
        store: "key/set",
        family: "compute_if_absent",
        test: "key_set_family_compute_if_absent",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "put",
        test: "key_map_family_put",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "remove_from_sorted_map",
        test: "key_map_family_remove_from_sorted_map",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "remove_from_sorted_map_callback",
        test: "key_map_family_remove_from_sorted_map_callback",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "remove_key",
        test: "key_map_family_remove_key",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "pop_first",
        test: "key_map_family_pop_first",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "pop_last",
        test: "key_map_family_pop_last",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "append_ordered_element",
        test: "key_map_family_append_ordered_element",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "try_compute",
        test: "key_map_family_try_compute",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "compute",
        test: "key_map_family_compute",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "try_compute_if_present",
        test: "key_map_family_try_compute_if_present",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "compute_if_present",
        test: "key_map_family_compute_if_present",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "try_compute_if_absent",
        test: "key_map_family_try_compute_if_absent",
    },
    FamilyCase {
        store: "key/sorted-map",
        family: "compute_if_absent",
        test: "key_map_family_compute_if_absent",
    },
];

#[test]
fn manifest_maps_every_contract_id_exactly_once() {
    let expected: HashSet<_> = EXPECTED_CONTRACT_IDS.iter().copied().collect();
    let actual: HashSet<_> = CONTRACT_CASES.iter().map(|case| case.id).collect();
    assert_eq!(
        actual.len(),
        CONTRACT_CASES.len(),
        "duplicate contract ID in traceability manifest"
    );
    assert_eq!(actual, expected, "missing or unexpected contract ID");
    assert_unique_test_names(CONTRACT_CASES.iter().map(|case| case.test));
}

#[test]
fn manifest_maps_every_public_mutation_family_exactly_once() {
    let expected: HashSet<_> = EXPECTED_FAMILIES.iter().copied().collect();
    let actual: HashSet<_> = FAMILY_CASES
        .iter()
        .map(|case| (case.store, case.family))
        .collect();
    assert_eq!(
        actual.len(),
        FAMILY_CASES.len(),
        "duplicate mutation family in traceability manifest"
    );
    assert_eq!(actual, expected, "missing or unexpected mutation family");
    assert_unique_test_names(FAMILY_CASES.iter().map(|case| case.test));
}

fn assert_unique_test_names<'a>(names: impl Iterator<Item = &'a str>) {
    let mut unique = HashSet::new();
    for name in names {
        assert!(unique.insert(name), "duplicate mapped test name: {name}");
    }
}
