//! RED–GREEN tests for private durability state machines.

use super::{
    format::{V1CodecProbe, V2CodecProbe},
    ComputeAction, PersistenceOperation, PrivateMutationFailure, WalStorage,
};
use crate::config::DurabilityPolicy;
use crate::durability::{
    directory_barrier_calls, fail_directory_barrier_for, fail_preflight_for, preflight_directory,
    preflight_file, validate_compile_target_probe, validate_memory_backing, DurabilityCapability,
    DurabilityHarnessProbe, DurabilitySupportError,
};
use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::model::SearchKey;
use crate::test_support::durability_snapshot::{
    restore_image, DurabilitySnapshot, NamespaceOperation,
};
use crate::test_support::fault_writer::{
    rollback_scripted, sync_all_scripted, sync_data_scripted, BarrierKind, ScriptedWriter,
    WriterEvent, WriterFault,
};
use crate::test_support::mutation_schedule::{MutationObserver, MutationPhase};
use crate::wal::recovery::fail_cleanup_for;
use crate::wal::replay::{replay_key_value, replay_key_value_tail, TailReplay};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn private_durability_harness_is_test_only() {
    assert_eq!(DurabilityHarnessProbe::new(), DurabilityHarnessProbe);
    assert_eq!(
        crate::config::durability_probe_options(false).granularity_nanos(),
        60_000_000_000
    );
}

#[test]
fn private_policy_defaults_to_buffered_and_selects_physical_per_open() {
    let buffered = crate::config::durability_probe_options(false);
    let physical = crate::config::durability_probe_options(true);

    assert!(format!("{buffered:?}").contains("Buffered"));
    assert!(format!("{physical:?}").contains("Physical"));
}

#[test]
fn private_policy_is_per_open_runtime_state_and_is_absent_from_v1_bytes() {
    let header = V1CodecProbe::encode_header();
    let buffered = WalStorage::new_vec_based_v1_with_probe_options(
        &header,
        crate::config::durability_probe_options(false),
    );
    let physical = WalStorage::new_vec_based_v1_with_probe_options(
        &header,
        crate::config::durability_probe_options(true),
    );

    assert_eq!(buffered.runtime_policy_probe(), DurabilityPolicy::Buffered);
    assert_eq!(physical.runtime_policy_probe(), DurabilityPolicy::Physical);
    assert_eq!(
        buffered.wal_state.read().unwrap().writer,
        physical.wal_state.read().unwrap().writer
    );
    assert_eq!(
        buffered
            .wal_state
            .read()
            .unwrap()
            .writer
            .as_ref()
            .unwrap()
            .as_slice(),
        &header
    );
}

#[test]
fn buffered_single_and_multi_record_commits_keep_existing_io_counts() {
    let (single_writer, single_handle) = ScriptedWriter::scripted(None, false, None);
    let single = WalStorage::new_v1_with_rollback(single_writer, rollback_scripted);
    single
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap();
    assert_eq!(
        single_handle.events(),
        vec![WriterEvent::Write, WriterEvent::Flush]
    );
    assert_eq!(single_handle.write_calls(), 1);
    assert_eq!(single_handle.flush_calls(), 1);
    assert_eq!(single_handle.data_barrier_calls(), 0);
    assert_eq!(single_handle.full_barrier_calls(), 0);

    let (group_writer, group_handle) = ScriptedWriter::scripted(None, false, None);
    let group = WalStorage::new_v1_with_rollback(group_writer, rollback_scripted);
    group
        .commit_set_compute_batch(vec![
            ComputeAction::SetAppend {
                key: b"key".to_vec(),
                value: b"one".to_vec(),
            },
            ComputeAction::SetAppend {
                key: b"key".to_vec(),
                value: b"two".to_vec(),
            },
        ])
        .unwrap();
    assert_eq!(
        group_handle.events(),
        vec![WriterEvent::Write, WriterEvent::Flush]
    );
    assert_eq!(group_handle.write_calls(), 1);
    assert_eq!(group_handle.flush_calls(), 1);
    assert_eq!(group_handle.data_barrier_calls(), 0);
    assert_eq!(group_handle.full_barrier_calls(), 0);
}

#[test]
fn buffered_construction_performs_no_durability_preflights() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);
    let namespace = DurabilitySnapshot::new(None);

    assert_eq!(wal.runtime_policy_probe(), DurabilityPolicy::Buffered);
    assert!(handle.events().is_empty());
    assert_eq!(handle.data_barrier_calls(), 0);
    assert_eq!(handle.full_barrier_calls(), 0);
    assert_eq!(namespace.calls(NamespaceOperation::FileBarrier), 0);
    assert_eq!(namespace.calls(NamespaceOperation::DirectoryBarrier), 0);
}

#[test]
fn private_key_value_physical_memory_request_is_rejected_before_store_exposure() {
    assert!(validate_memory_backing(DurabilityPolicy::Buffered).is_ok());
    assert!(matches!(
        validate_memory_backing(DurabilityPolicy::Physical),
        Err(DurabilitySupportError::NoPhysicalBacking)
    ));
}

#[test]
fn private_key_set_physical_memory_construction_routes_to_backing_rejection() {
    let result = DurableKeySetStore::try_new_vec_based_with_probe_options(
        crate::config::durability_probe_options(true),
    );
    assert!(matches!(
        result,
        Err(DurabilitySupportError::NoPhysicalBacking)
    ));
}

#[test]
fn private_key_map_physical_memory_construction_routes_to_backing_rejection() {
    let result = DurableKeyMapStore::try_new_vec_based_with_probe_options(
        crate::config::durability_probe_options(true),
    );
    assert!(matches!(
        result,
        Err(DurabilitySupportError::NoPhysicalBacking)
    ));
}

#[test]
fn windows_is_supported_while_unknown_targets_are_rejected_before_filesystem_work() {
    let namespace = DurabilitySnapshot::new(None);

    assert!(validate_compile_target_probe("windows").is_ok());
    assert!(matches!(
        validate_compile_target_probe("unsupported-test-target"),
        Err(DurabilitySupportError::UnsupportedPlatform {
            platform: "unsupported-test-target"
        })
    ));
    assert!(namespace.events().is_empty());
}

#[cfg(not(target_os = "windows"))]
#[test]
fn parent_directory_preflight_maps_every_open_failure_to_support_error() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing-parent");

    let error = preflight_directory(&missing).unwrap_err();
    match error {
        DurabilitySupportError::RequiredBarrierUnavailable {
            operation,
            path,
            source,
        } => {
            assert_eq!(operation, DurabilityCapability::DirectoryEntry);
            assert_eq!(path.as_deref(), Some(missing.as_path()));
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        }
        other => panic!("unexpected support error: {other:?}"),
    }
}

#[cfg(target_os = "windows")]
#[test]
fn windows_preflight_maps_content_before_namespace_without_authority_changes() {
    let directory = tempfile::tempdir().unwrap();
    let content_fault = fail_preflight_for(
        DurabilityCapability::FileContent,
        directory.path().to_path_buf(),
        std::io::ErrorKind::PermissionDenied,
    );
    let namespace_fault = fail_preflight_for(
        DurabilityCapability::DirectoryEntry,
        directory.path().to_path_buf(),
        std::io::ErrorKind::CrossesDevices,
    );

    let content_error = preflight_directory(directory.path()).unwrap_err();
    assert!(matches!(
        content_error,
        DurabilitySupportError::RequiredBarrierUnavailable {
            operation: DurabilityCapability::FileContent,
            ref path,
            ref source,
        } if path.as_deref() == Some(directory.path())
            && source.kind() == std::io::ErrorKind::PermissionDenied
    ));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);

    drop(content_fault);
    let namespace_error = preflight_directory(directory.path()).unwrap_err();
    assert!(matches!(
        namespace_error,
        DurabilitySupportError::RequiredBarrierUnavailable {
            operation: DurabilityCapability::DirectoryEntry,
            ref path,
            ref source,
        } if path.as_deref() == Some(directory.path())
            && source.kind() == std::io::ErrorKind::CrossesDevices
    ));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    drop(namespace_fault);
}

#[test]
fn selected_file_preflight_maps_every_open_failure_to_support_error() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing-authority");

    let error = preflight_file(&missing).unwrap_err();
    match error {
        DurabilitySupportError::RequiredBarrierUnavailable {
            operation,
            path,
            source,
        } => {
            assert_eq!(operation, DurabilityCapability::FileContent);
            assert_eq!(path.as_deref(), Some(missing.as_path()));
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        }
        other => panic!("unexpected support error: {other:?}"),
    }
}

#[test]
fn fresh_physical_startup_preflights_and_exposes_only_a_complete_active_store() {
    let directory = tempfile::tempdir().unwrap();
    let options = crate::config::durability_probe_options(true);

    let outcome =
        DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options).unwrap();

    assert_eq!(
        outcome.store().runtime_policy_probe(),
        DurabilityPolicy::Physical
    );
    assert!(directory.path().join("kv.wal.dat").is_file());
    assert!(!directory.path().join(".kv.wal.dat.next").exists());
}

#[test]
fn physical_parent_preflight_failure_preserves_every_startup_artifact() {
    for kind in [
        std::io::ErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::Other,
    ] {
        let directory = tempfile::tempdir().unwrap();
        drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
        let active = directory.path().join("kv.wal.dat");
        let staging = directory.path().join(".kv.wal.dat.next");
        std::fs::write(&staging, b"stale but non-authoritative").unwrap();
        let active_before = std::fs::read(&active).unwrap();
        let staging_before = std::fs::read(&staging).unwrap();
        let _fault = fail_preflight_for(
            DurabilityCapability::DirectoryEntry,
            directory.path().to_path_buf(),
            kind,
        );

        let error = match DurableKeyValueStore::try_init_new_with_probe_options(
            directory.path(),
            crate::config::durability_probe_options(true),
        ) {
            Err(error) => error,
            Ok(_) => panic!("failed parent preflight must not expose a store"),
        };

        assert!(matches!(
            error,
            crate::RecoveryError::UnsupportedDurability {
                source: DurabilitySupportError::RequiredBarrierUnavailable {
                    operation: DurabilityCapability::DirectoryEntry,
                    ..
                }
            }
        ));
        assert_eq!(std::fs::read(&active).unwrap(), active_before);
        assert_eq!(std::fs::read(&staging).unwrap(), staging_before);
    }
}

#[test]
fn selected_authority_content_preflight_failure_precedes_stale_cleanup() {
    for kind in [
        std::io::ErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::Other,
    ] {
        let directory = tempfile::tempdir().unwrap();
        drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
        let active = directory.path().join("kv.wal.dat");
        let staging = directory.path().join(".kv.wal.dat.next");
        std::fs::write(&staging, b"stale but non-authoritative").unwrap();
        let active_before = std::fs::read(&active).unwrap();
        let staging_before = std::fs::read(&staging).unwrap();
        let _fault = fail_preflight_for(DurabilityCapability::FileContent, active.clone(), kind);

        let error = match DurableKeyValueStore::try_init_new_with_probe_options(
            directory.path(),
            crate::config::durability_probe_options(true),
        ) {
            Err(error) => error,
            Ok(_) => panic!("failed content preflight must not expose a store"),
        };

        assert!(matches!(
            error,
            crate::RecoveryError::UnsupportedDurability {
                source: DurabilitySupportError::RequiredBarrierUnavailable {
                    operation: DurabilityCapability::FileContent,
                    ..
                }
            }
        ));
        assert_eq!(std::fs::read(&active).unwrap(), active_before);
        assert_eq!(std::fs::read(&staging).unwrap(), staging_before);
    }
}

#[test]
fn fresh_staging_content_preflight_failure_creates_no_authority_and_cleans_staging() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let staging = directory.path().join(".kv.wal.dat.next");
    let _fault = fail_preflight_for(
        DurabilityCapability::FileContent,
        staging.clone(),
        std::io::ErrorKind::PermissionDenied,
    );

    let error = match DurableKeyValueStore::try_init_new_with_probe_options(
        directory.path(),
        crate::config::durability_probe_options(true),
    ) {
        Err(error) => error,
        Ok(_) => panic!("failed staging content preflight must not expose a store"),
    };

    match error {
        crate::RecoveryError::UnsupportedDurability {
            source:
                DurabilitySupportError::RequiredBarrierUnavailable {
                    operation,
                    path,
                    source,
                },
        } => {
            assert_eq!(operation, DurabilityCapability::FileContent);
            assert_eq!(path.as_deref(), Some(staging.as_path()));
            assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
        }
        other => panic!("unexpected recovery error: {other:?}"),
    }
    assert!(!active.exists());
    assert!(!staging.exists());
}

#[test]
fn failed_staging_cleanup_is_diagnosed_as_the_only_remaining_non_authority() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let staging = directory.path().join(".kv.wal.dat.next");
    let _preflight_fault = fail_preflight_for(
        DurabilityCapability::FileContent,
        staging.clone(),
        std::io::ErrorKind::PermissionDenied,
    );
    let _cleanup_fault = fail_cleanup_for(staging.clone());

    let error = match DurableKeyValueStore::try_init_new_with_probe_options(
        directory.path(),
        crate::config::durability_probe_options(true),
    ) {
        Err(error) => error,
        Ok(_) => panic!("failed staging content preflight must not expose a store"),
    };

    match error {
        crate::RecoveryError::UnsupportedDurability {
            source:
                DurabilitySupportError::RequiredBarrierUnavailable {
                    operation,
                    path,
                    source,
                },
        } => {
            assert_eq!(operation, DurabilityCapability::FileContent);
            assert_eq!(path.as_deref(), Some(staging.as_path()));
            assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            assert!(source.to_string().contains("cleanup"));
            assert!(source.to_string().contains(&staging.display().to_string()));
        }
        other => panic!("unexpected recovery error: {other:?}"),
    }
    assert!(!active.exists());
    assert_eq!(
        std::fs::read(&staging).unwrap().len(),
        V2CodecProbe::HEADER_LEN
    );
}

#[cfg(not(target_os = "windows"))]
#[test]
fn fresh_publication_requires_parent_barrier_before_store_exposure() {
    let directory = tempfile::tempdir().unwrap();
    let active = directory.path().join("kv.wal.dat");
    let _fault =
        fail_directory_barrier_for(directory.path().to_path_buf(), 1, std::io::ErrorKind::Other);

    let error = match DurableKeyValueStore::try_init_new_with_probe_options(
        directory.path(),
        crate::config::durability_probe_options(true),
    ) {
        Err(error) => error,
        Ok(_) => panic!("failed publication barrier must not expose a store"),
    };

    assert!(matches!(
        error,
        crate::RecoveryError::Io {
            operation: crate::RecoveryOperation::Publish,
            ref path,
            ..
        } if path == directory.path()
    ));
    assert_eq!(directory_barrier_calls(directory.path()), 1);
    assert_eq!(
        std::fs::read(&active).unwrap().len(),
        V2CodecProbe::HEADER_LEN
    );
    assert!(DurableKeyValueStore::try_init_new(directory.path()).is_ok());
}

#[cfg(not(target_os = "windows"))]
#[test]
fn deferred_granularity_rotation_fails_closed_when_directory_barrier_fails() {
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let active = directory.path().join("kv.wal.dat");
    let recovery = directory.path().join(".kv.wal.dat");
    let staging = directory.path().join(".kv.wal.dat.next");
    let original = std::fs::read(&active).unwrap();
    let _fault =
        fail_directory_barrier_for(directory.path().to_path_buf(), 1, std::io::ErrorKind::Other);
    let granularity = crate::TimestampGranularity::try_from(Duration::from_secs(1)).unwrap();
    let options =
        crate::config::durability_probe_options(true).with_timestamp_granularity(granularity);

    let outcome =
        DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options).unwrap();
    assert_eq!(directory_barrier_calls(directory.path()), 0);
    let error = outcome
        .store()
        .try_put(b"rejected".to_vec(), b"value".to_vec())
        .expect_err("failed rotation publication barrier must reject the mutation");
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(directory_barrier_calls(directory.path()), 1);
    assert!(active.is_file());
    assert_eq!(
        std::fs::read(
            directory
                .path()
                .join("kv.wal.dat.segment-00000000000000000000")
        )
        .unwrap(),
        original
    );
    assert!(!recovery.exists());
    assert!(!staging.exists());
    assert!(outcome
        .store()
        .try_put(b"later".to_vec(), b"value".to_vec())
        .is_err());
}

#[cfg(not(target_os = "windows"))]
#[test]
fn deferred_granularity_rotation_publishes_one_new_segment_before_mutation() {
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let recovery = directory.path().join(".kv.wal.dat");
    let _fault =
        fail_directory_barrier_for(directory.path().to_path_buf(), 2, std::io::ErrorKind::Other);
    let granularity = crate::TimestampGranularity::try_from(Duration::from_secs(1)).unwrap();
    let options =
        crate::config::durability_probe_options(true).with_timestamp_granularity(granularity);

    let outcome =
        DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options).unwrap();
    assert_eq!(directory_barrier_calls(directory.path()), 0);
    outcome
        .store()
        .try_put(b"accepted".to_vec(), b"value".to_vec())
        .unwrap();
    assert_eq!(directory_barrier_calls(directory.path()), 1);
    assert!(directory.path().join("kv.wal.dat").is_file());
    assert!(!recovery.exists());
    let active = std::fs::read(directory.path().join("kv.wal.dat")).unwrap();
    assert_eq!(
        u64::from_le_bytes(active[16..24].try_into().unwrap()),
        1_000_000_000
    );
}

#[test]
fn opening_with_changed_granularity_does_not_rewrite_immutable_segment() {
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let active = directory.path().join("kv.wal.dat");
    let original = std::fs::read(&active).unwrap();
    let _fault =
        fail_directory_barrier_for(directory.path().to_path_buf(), 3, std::io::ErrorKind::Other);
    let granularity = crate::TimestampGranularity::try_from(Duration::from_secs(1)).unwrap();
    let options =
        crate::config::durability_probe_options(true).with_timestamp_granularity(granularity);

    let outcome =
        DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options).unwrap();

    assert_eq!(directory_barrier_calls(directory.path()), 0);
    assert_eq!(
        outcome.store().runtime_policy_probe(),
        DurabilityPolicy::Physical
    );
    assert_eq!(std::fs::read(active).unwrap(), original);
    drop(outcome);
    assert!(DurableKeyValueStore::try_init_new(directory.path()).is_ok());
}

#[test]
fn recovery_authority_remains_available_when_publication_barrier_fails() {
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let active = directory.path().join("kv.wal.dat");
    let recovery = directory.path().join(".kv.wal.dat");
    std::fs::rename(&active, &recovery).unwrap();
    let _fault =
        fail_directory_barrier_for(directory.path().to_path_buf(), 1, std::io::ErrorKind::Other);

    let error = match DurableKeyValueStore::try_init_new_with_probe_options(
        directory.path(),
        crate::config::durability_probe_options(true),
    ) {
        Err(error) => error,
        Ok(_) => panic!("failed recovery publication barrier must not expose a store"),
    };

    assert!(matches!(
        error,
        crate::RecoveryError::Io {
            operation: crate::RecoveryOperation::Publish,
            ref path,
            ..
        } if path == directory.path()
    ));
    assert_eq!(directory_barrier_calls(directory.path()), 1);
    assert!(active.is_file());
    assert!(recovery.is_file());
}

#[test]
fn active_cleanup_removal_failure_defers_only_obsolete_recovery() {
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let staging = directory.path().join(".kv.wal.dat.next");
    std::fs::write(&staging, b"obsolete-rotation-staging").unwrap();
    let cleanup_fault = fail_cleanup_for(staging.clone());

    let outcome = DurableKeyValueStore::try_init_new_with_probe_options(
        directory.path(),
        crate::config::durability_probe_options(true),
    )
    .unwrap();

    assert!(directory.path().join("kv.wal.dat").is_file());
    assert!(staging.is_file());
    drop(outcome);
    drop(cleanup_fault);
    assert!(DurableKeyValueStore::try_init_new(directory.path()).is_ok());
    assert!(!staging.exists());
}

#[test]
fn prepared_v2_repair_preserves_an_offset_above_four_gibibytes() {
    let file = tempfile::tempfile().unwrap();
    let offset = u64::from(u32::MAX) + 4096;
    file.set_len(offset).unwrap();

    let wal =
        WalStorage::from_prepared_file_v2_with_timestamp_state(file, offset, 60_000_000_000, 0);

    let state = wal.wal_state.read().unwrap();
    assert_eq!(state.offset, offset);
    assert_eq!(state.active_len, offset);
}

#[test]
fn recovery_cleanup_barrier_failure_preserves_published_active_authority() {
    let directory = tempfile::tempdir().unwrap();
    drop(DurableKeyValueStore::try_init_new(directory.path()).unwrap());
    let active = directory.path().join("kv.wal.dat");
    let recovery = directory.path().join(".kv.wal.dat");
    std::fs::rename(&active, &recovery).unwrap();
    let _fault =
        fail_directory_barrier_for(directory.path().to_path_buf(), 2, std::io::ErrorKind::Other);

    let outcome = DurableKeyValueStore::try_init_new_with_probe_options(
        directory.path(),
        crate::config::durability_probe_options(true),
    )
    .unwrap();

    assert_eq!(directory_barrier_calls(directory.path()), 2);
    assert!(active.is_file());
    drop(outcome);
    assert!(DurableKeyValueStore::try_init_new(directory.path()).is_ok());
}

#[test]
fn durable_namespace_model_covers_fresh_active_and_recovery_publication_sequences() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let active = root.join("kv.wal.dat");
    let recovery = root.join(".kv.wal.dat");
    let staging = root.join(".kv.wal.dat.next");

    let mut fresh = DurabilitySnapshot::new(None);
    fresh.write(&staging, b"fresh").unwrap();
    fresh.sync_file(&staging).unwrap();
    fresh.rename(&staging, &active).unwrap();
    fresh.sync_directory(root).unwrap();
    let fresh_image = fresh.simulate_power_loss();
    assert_eq!(fresh_image.files.get(&active), Some(&b"fresh".to_vec()));

    let mut replacement = DurabilitySnapshot::new(None);
    replacement.write(&active, b"old").unwrap();
    replacement.sync_file(&active).unwrap();
    replacement.sync_directory(root).unwrap();
    replacement.write(&staging, b"new").unwrap();
    replacement.sync_file(&staging).unwrap();
    replacement.rename(&active, &recovery).unwrap();
    replacement.sync_directory(root).unwrap();
    replacement.rename(&staging, &active).unwrap();
    replacement.sync_directory(root).unwrap();
    replacement.remove(&recovery).unwrap();
    replacement.sync_directory(root).unwrap();
    let replacement_image = replacement.simulate_power_loss();
    assert_eq!(replacement_image.files.get(&active), Some(&b"new".to_vec()));
    assert!(!replacement_image.files.contains_key(&recovery));

    let restore_root = tempfile::tempdir().unwrap();
    let restored_active = restore_root.path().join("kv.wal.dat");
    let restored_image = crate::test_support::durability_snapshot::DurableNamespaceImage {
        files: [(restored_active.clone(), b"new".to_vec())]
            .into_iter()
            .collect(),
    };
    restore_image(restore_root.path(), &restored_image).unwrap();
    assert_eq!(std::fs::read(restored_active).unwrap(), b"new");
}

#[test]
fn private_data_barrier_operation_dispatches_through_the_writer_seam() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_rollback(writer, rollback_scripted);

    wal.dispatch_data_barrier_probe(sync_data_scripted).unwrap();

    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(handle.events(), vec![WriterEvent::DataBarrier]);
}

#[test]
fn physical_single_record_acceptance_requires_write_flush_then_one_data_barrier() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);

    wal.try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap();

    assert_eq!(
        handle.events(),
        vec![
            WriterEvent::Write,
            WriterEvent::Flush,
            WriterEvent::DataBarrier,
        ]
    );
    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(handle.full_barrier_calls(), 0);
}

#[test]
fn failed_physical_barrier_does_not_advance_offset_or_timestamp_bucket() {
    let (writer, handle) =
        ScriptedWriter::scripted(Some(WriterFault::DataBarrierCall(1)), false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(error.to_string().contains("data barrier"));
    let state = wal.wal_state.read().unwrap();
    assert_eq!(state.offset, V1CodecProbe::HEADER_LEN as u64);
    assert_eq!(state.last_bucket, 0);
    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(handle.truncate_calls(), 1);
}

#[test]
fn physical_set_group_receives_one_barrier_after_its_final_member() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.commit_set_compute_batch(vec![
        ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"one".to_vec(),
        },
        ComputeAction::SetAppend {
            key: b"key".to_vec(),
            value: b"two".to_vec(),
        },
    ])
    .unwrap();

    assert_eq!(handle.write_calls(), 1);
    assert_eq!(handle.flush_calls(), 1);
    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(
        handle.events(),
        vec![
            WriterEvent::Write,
            WriterEvent::Flush,
            WriterEvent::DataBarrier,
        ]
    );
}

#[test]
fn physical_map_group_receives_one_barrier_after_its_final_member() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.commit_map_compute_batch(vec![
        ComputeAction::MapPut {
            key: b"key".to_vec(),
            search_key: SearchKey::from(1_usize),
            value: b"one".to_vec(),
        },
        ComputeAction::MapPut {
            key: b"key".to_vec(),
            search_key: SearchKey::from(2_usize),
            value: b"two".to_vec(),
        },
    ])
    .unwrap();

    assert_eq!(handle.write_calls(), 1);
    assert_eq!(handle.flush_calls(), 1);
    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(
        handle.events(),
        vec![
            WriterEvent::Write,
            WriterEvent::Flush,
            WriterEvent::DataBarrier,
        ]
    );
}

#[test]
fn physical_exact_no_op_performs_no_wal_or_barrier_io() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);

    wal.commit_set_compute_batch(Vec::new()).unwrap();
    wal.commit_map_compute_batch(Vec::new()).unwrap();

    assert!(handle.events().is_empty());
    assert_eq!(handle.write_calls(), 0);
    assert_eq!(handle.flush_calls(), 0);
    assert_eq!(handle.data_barrier_calls(), 0);
}

#[test]
fn concurrent_physical_calls_each_own_one_complete_barrier() {
    let (writer, handle) = ScriptedWriter::scripted(None, false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);

    std::thread::scope(|scope| {
        scope.spawn(|| {
            wal.try_store_put_event(b"left".to_vec(), b"one".to_vec())
                .unwrap();
        });
        scope.spawn(|| {
            wal.try_store_put_event(b"right".to_vec(), b"two".to_vec())
                .unwrap();
        });
    });

    assert_eq!(handle.write_calls(), 2);
    assert_eq!(handle.flush_calls(), 2);
    assert_eq!(handle.data_barrier_calls(), 2);
    for group in handle.events().chunks_exact(3) {
        assert_eq!(
            group,
            [
                WriterEvent::Write,
                WriterEvent::Flush,
                WriterEvent::DataBarrier,
            ]
        );
    }
}

#[test]
fn confirmed_rejection_is_typed_through_io_error_and_preserves_source_kind() {
    let (writer, _) = ScriptedWriter::scripted(Some(WriterFault::DataBarrierCall(1)), false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();
    let original_kind = error.kind();
    let failure = PrivateMutationFailure::from_io_error(&error)
        .expect("confirmed rejection must be carried as the io::Error source");
    match failure {
        PrivateMutationFailure::Rejected { operation, source } => {
            assert_eq!(*operation, PersistenceOperation::SynchronizeData);
            assert_eq!(error.kind(), source.kind());
            assert_eq!(original_kind, source.kind());
        }
        other => panic!("expected confirmed rejection, got {other:?}"),
    }
}

#[test]
fn failed_data_barrier_truncates_then_fully_synchronizes_the_rollback() {
    let (writer, handle) =
        ScriptedWriter::scripted(Some(WriterFault::DataBarrierCall(1)), false, None);
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&error),
        Some(PrivateMutationFailure::Rejected {
            operation: PersistenceOperation::SynchronizeData,
            ..
        })
    ));
    assert_eq!(
        handle.events(),
        vec![
            WriterEvent::Write,
            WriterEvent::Flush,
            WriterEvent::DataBarrier,
            WriterEvent::Truncate,
            WriterEvent::FullBarrier,
        ]
    );
    assert_eq!(handle.full_barrier_calls(), 1);
}

#[test]
fn partial_physical_write_is_durably_rolled_back_and_rejected_as_write() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::PartialWriteCall {
            call: 1,
            written: 7,
        }),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&error),
        Some(PrivateMutationFailure::Rejected {
            operation: PersistenceOperation::Write,
            ..
        })
    ));
    assert_eq!(handle.bytes(), header);
    assert_eq!(handle.durable_bytes(), header);
    assert_eq!(handle.flush_calls(), 0);
    assert_eq!(handle.truncate_calls(), 1);
    assert_eq!(handle.full_barrier_calls(), 1);
}

#[test]
fn physical_flush_failure_is_durably_rolled_back_and_rejected_as_flush() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::FlushCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&error),
        Some(PrivateMutationFailure::Rejected {
            operation: PersistenceOperation::Flush,
            ..
        })
    ));
    assert_eq!(handle.bytes(), header);
    assert_eq!(handle.durable_bytes(), header);
    assert_eq!(handle.write_calls(), 1);
    assert_eq!(handle.flush_calls(), 1);
    assert_eq!(handle.truncate_calls(), 1);
    assert_eq!(handle.full_barrier_calls(), 1);
    assert_eq!(handle.data_barrier_calls(), 0);
}

#[test]
fn confirmed_rejection_restores_checkpoint_and_allows_the_next_mutation() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::FlushCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    wal.try_store_put_event(b"rejected".to_vec(), b"old".to_vec())
        .unwrap_err();
    {
        let state = wal.wal_state.read().unwrap();
        assert_eq!(state.offset, V1CodecProbe::HEADER_LEN as u64);
        assert_eq!(state.last_bucket, 0);
    }

    wal.try_store_put_event(b"accepted".to_vec(), b"new".to_vec())
        .unwrap();
    let state = wal.wal_state.read().unwrap();
    assert!(state.offset > V1CodecProbe::HEADER_LEN as u64);
    assert_eq!(handle.truncate_calls(), 1);
    assert_eq!(handle.full_barrier_calls(), 1);
    assert_eq!(handle.data_barrier_calls(), 1);
}

#[test]
fn failed_checkpoint_truncate_is_indeterminate_and_skips_rollback_sync() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_many_with_bytes(
        vec![
            WriterFault::DataBarrierCall(1),
            WriterFault::TruncateCall(1),
        ],
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&error),
        Some(PrivateMutationFailure::Indeterminate {
            operation: PersistenceOperation::SynchronizeData,
            rollback_operation: PersistenceOperation::Rollback,
            ..
        })
    ));
    assert_ne!(handle.bytes(), header);
    assert_eq!(handle.truncate_calls(), 1);
    assert_eq!(handle.full_barrier_calls(), 0);
}

#[test]
fn failed_rollback_sync_is_indeterminate_after_successful_truncate() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_many_with_bytes(
        vec![
            WriterFault::DataBarrierCall(1),
            WriterFault::FullBarrierCall(1),
        ],
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);

    let error = wal
        .try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&error),
        Some(PrivateMutationFailure::Indeterminate {
            operation: PersistenceOperation::SynchronizeData,
            rollback_operation: PersistenceOperation::SynchronizeRollback,
            ..
        })
    ));
    assert_eq!(handle.bytes(), header);
    assert_eq!(handle.truncate_calls(), 1);
    assert_eq!(handle.full_barrier_calls(), 1);
}

#[test]
fn indeterminate_instance_rejects_later_mutation_before_any_writer_access() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_many_with_bytes(
        vec![
            WriterFault::DataBarrierCall(1),
            WriterFault::TruncateCall(1),
        ],
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    wal.try_store_put_event(b"first".to_vec(), b"value".to_vec())
        .unwrap_err();
    let events_before = handle.events();

    let later = wal
        .try_store_put_event(b"later".to_vec(), b"value".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&later),
        Some(PrivateMutationFailure::FailedClosed { .. })
    ));
    assert_eq!(handle.events(), events_before);
}

#[test]
fn complete_indeterminate_v1_bytes_replay_as_authoritative_state() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_many_with_bytes(
        vec![
            WriterFault::DataBarrierCall(1),
            WriterFault::TruncateCall(1),
        ],
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    wal.try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    let replayed = replay_key_value(&handle.bytes()).unwrap();
    assert_eq!(
        replayed.snapshot.get(b"key".as_slice()),
        Some(&b"value".to_vec())
    );
}

#[test]
fn incomplete_indeterminate_v1_bytes_classify_as_recoverable_tail() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_many_with_bytes(
        vec![
            WriterFault::PartialWriteCall {
                call: 1,
                written: 8,
            },
            WriterFault::TruncateCall(1),
        ],
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    wal.try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap_err();

    let TailReplay::RecoverableTail {
        replay,
        tail_offset,
        ..
    } = replay_key_value_tail(&handle.bytes())
    else {
        panic!("partial terminal V1 frame must use accepted-prefix recovery");
    };
    assert_eq!(tail_offset, V1CodecProbe::HEADER_LEN);
    assert!(replay.snapshot.is_empty());
}

#[test]
fn structurally_complete_corruption_is_preserved_and_rejected() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(None, false, None, header.to_vec());
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    wal.try_store_put_event(b"key".to_vec(), b"value".to_vec())
        .unwrap();
    let mut corrupted = handle.bytes();
    corrupted[V1CodecProbe::HEADER_LEN] = 0xff;
    let preserved = corrupted.clone();

    assert!(replay_key_value(&corrupted).is_err());
    assert!(matches!(
        replay_key_value_tail(&corrupted),
        TailReplay::Invalid(_)
    ));
    assert_eq!(corrupted, preserved);
}

#[test]
fn confirmed_rejection_does_not_share_or_consume_the_next_call_barrier() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let first = wal.try_store_put_event(b"first".to_vec(), b"old".to_vec());
    let second = wal.try_store_put_event(b"second".to_vec(), b"new".to_vec());

    assert!(first.is_err());
    assert!(second.is_ok());
    assert_eq!(handle.data_barrier_calls(), 2);
    assert_eq!(handle.full_barrier_calls(), 1);
    assert_eq!(
        replay_key_value(&handle.bytes())
            .unwrap()
            .snapshot
            .get(b"second".as_slice()),
        Some(&b"new".to_vec())
    );
}

#[test]
fn blocked_physical_barrier_keeps_key_value_publication_ineligible() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) =
        ScriptedWriter::scripted_with_bytes(None, false, Some(BarrierKind::Data), header.to_vec());
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyValueStore::from_probe_parts(
        [(b"key".to_vec(), b"old".to_vec())],
        wal,
        MutationObserver::default(),
    );
    let (sender, receiver) = std::sync::mpsc::channel();

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| store.put(b"key".to_vec(), b"new".to_vec()));
        handle.wait_until_barrier_blocked(BarrierKind::Data);
        let reader = scope.spawn(|| sender.send(store.get(b"key")).unwrap());
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        handle.release_barrier();
        worker.join().unwrap();
        reader.join().unwrap();
    });
    assert_eq!(receiver.recv().unwrap(), Some(b"new".to_vec()));
}

#[test]
fn blocked_physical_barrier_keeps_set_final_member_callback_ineligible() {
    let header = V1CodecProbe::encode_header_with_kind(2);
    let (writer, handle) =
        ScriptedWriter::scripted_with_bytes(None, false, Some(BarrierKind::Data), header.to_vec());
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeySetStore::from_probe_parts(
        [(b"key".to_vec(), HashSet::from([b"member".to_vec()]))],
        wal,
        MutationObserver::default(),
    );
    let callbacks = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            store.remove_from_set_callback(b"key".to_vec(), b"member".to_vec(), |_| {
                callbacks.fetch_add(1, Ordering::SeqCst);
            });
        });
        handle.wait_until_barrier_blocked(BarrierKind::Data);
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        handle.release_barrier();
        worker.join().unwrap();
    });
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(store.get_hashset(b"key"), None);
}

#[test]
fn blocked_physical_barrier_keeps_map_callback_and_result_ineligible() {
    let header = V1CodecProbe::encode_header_with_kind(3);
    let (writer, handle) =
        ScriptedWriter::scripted_with_bytes(None, false, Some(BarrierKind::Data), header.to_vec());
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyMapStore::from_probe_parts(
        [(
            b"callback".to_vec(),
            BTreeMap::from([(SearchKey::from(1_usize), b"one".to_vec())]),
        )],
        wal,
        MutationObserver::default(),
    );
    let callbacks = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            store.remove_from_sorted_map_callback(
                b"callback".to_vec(),
                SearchKey::from(1_usize),
                |_| {
                    callbacks.fetch_add(1, Ordering::SeqCst);
                },
            );
        });
        handle.wait_until_barrier_blocked(BarrierKind::Data);
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        handle.release_barrier();
        worker.join().unwrap();
    });
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
}

#[test]
fn blocked_physical_barrier_keeps_map_removal_result_ineligible() {
    let header = V1CodecProbe::encode_header_with_kind(3);
    let (writer, handle) =
        ScriptedWriter::scripted_with_bytes(None, false, Some(BarrierKind::Data), header.to_vec());
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyMapStore::from_probe_parts(
        [(
            b"result".to_vec(),
            BTreeMap::from([(SearchKey::from(1_usize), b"value".to_vec())]),
        )],
        wal,
        MutationObserver::default(),
    );
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            sender
                .send(store.remove_from_sorted_map(b"result".to_vec(), SearchKey::from(1_usize)))
                .unwrap();
        });
        handle.wait_until_barrier_blocked(BarrierKind::Data);
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        handle.release_barrier();
        worker.join().unwrap();
    });
    assert_eq!(receiver.recv().unwrap(), Some(b"value".to_vec()));
}

#[test]
fn successful_barrier_bytes_are_authoritative_before_live_publication() {
    let header = V1CodecProbe::encode_header();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(None, false, None, header.to_vec());
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let (observer, gate) =
        MutationObserver::one_shot(b"key".to_vec(), MutationPhase::AcceptedBeforePublication);
    let store = DurableKeyValueStore::from_probe_parts([], wal, observer);
    let (sender, receiver) = std::sync::mpsc::channel();

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            store.put(b"key".to_vec(), b"value".to_vec());
            sender.send(()).unwrap();
        });
        gate.wait_until_reached();
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert_eq!(
            replay_key_value(&handle.durable_bytes())
                .unwrap()
                .snapshot
                .get(b"key".as_slice()),
            Some(&b"value".to_vec())
        );
        gate.release();
        worker.join().unwrap();
    });
    assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
}

#[test]
fn rejection_unwinds_key_guard_without_callback_or_live_publication() {
    let header = V1CodecProbe::encode_header_with_kind(2);
    let (writer, handle) = ScriptedWriter::scripted_many_with_bytes(
        vec![WriterFault::DataBarrierCall(1)],
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeySetStore::from_probe_parts(
        [(b"key".to_vec(), HashSet::from([b"member".to_vec()]))],
        wal,
        MutationObserver::default(),
    );
    let callbacks = AtomicUsize::new(0);

    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.remove_from_set_callback(b"key".to_vec(), b"member".to_vec(), |_| {
            callbacks.fetch_add(1, Ordering::SeqCst);
        });
    }));
    assert!(rejected.is_err());
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    assert!(store.contains_in_set(b"key", b"member"));

    store.append(b"key".to_vec(), b"later".to_vec());
    assert!(store.contains_in_set(b"key", b"later"));
    assert_eq!(handle.data_barrier_calls(), 2);
}

#[test]
fn private_key_value_simple_core_returns_typed_persistence_failure() {
    let header = V1CodecProbe::encode_header();
    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyValueStore::from_probe_parts(
        [(b"key".to_vec(), b"old".to_vec())],
        wal,
        MutationObserver::default(),
    );

    let error = store
        .try_put_probe(b"key".to_vec(), b"new".to_vec())
        .unwrap_err();

    assert!(matches!(
        PrivateMutationFailure::from_io_error(&error),
        Some(PrivateMutationFailure::Rejected {
            operation: PersistenceOperation::SynchronizeData,
            ..
        })
    ));
    assert_eq!(store.get(b"key"), Some(b"old".to_vec()));
}

#[test]
fn private_key_value_compute_core_returns_typed_error_after_one_callback() {
    let header = V1CodecProbe::encode_header();
    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyValueStore::from_probe_parts(
        [(b"key".to_vec(), b"old".to_vec())],
        wal,
        MutationObserver::default(),
    );
    let callbacks = AtomicUsize::new(0);

    let error = store
        .try_compute_probe(b"key".to_vec(), |_| {
            callbacks.fetch_add(1, Ordering::SeqCst);
            b"new".to_vec()
        })
        .unwrap_err();

    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert!(PrivateMutationFailure::from_io_error(&error).is_some());
    assert_eq!(store.get(b"key"), Some(b"old".to_vec()));
}

#[test]
fn private_key_set_simple_core_returns_typed_persistence_failure() {
    let header = V1CodecProbe::encode_header_with_kind(2);
    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeySetStore::from_probe_parts([], wal, MutationObserver::default());

    let error = store
        .try_append_probe(b"key".to_vec(), b"member".to_vec())
        .unwrap_err();

    assert!(PrivateMutationFailure::from_io_error(&error).is_some());
    assert!(!store.contains_in_set(b"key", b"member"));
}

#[test]
fn private_key_set_callback_core_returns_typed_error_without_callback() {
    let header = V1CodecProbe::encode_header_with_kind(2);
    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeySetStore::from_probe_parts(
        [(b"key".to_vec(), HashSet::from([b"member".to_vec()]))],
        wal,
        MutationObserver::default(),
    );
    let callbacks = AtomicUsize::new(0);

    let error = store
        .try_remove_from_set_callback_probe(b"key".to_vec(), b"member".to_vec(), |_| {
            callbacks.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap_err();

    assert!(PrivateMutationFailure::from_io_error(&error).is_some());
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    assert!(store.contains_in_set(b"key", b"member"));
}

#[test]
fn private_key_map_simple_core_returns_typed_persistence_failure() {
    let header = V1CodecProbe::encode_header_with_kind(3);
    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyMapStore::from_probe_parts([], wal, MutationObserver::default());

    let error = store
        .try_put_probe(b"key".to_vec(), SearchKey::from(1_usize), b"value".to_vec())
        .unwrap_err();

    assert!(PrivateMutationFailure::from_io_error(&error).is_some());
    assert_eq!(store.get_sorted_map(b"key"), None);
}

#[test]
fn private_key_map_ordered_core_returns_typed_error_without_result_publication() {
    let header = V1CodecProbe::encode_header_with_kind(3);
    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::DataBarrierCall(1)),
        false,
        None,
        header.to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let search_key = SearchKey::from(1_usize);
    let store = DurableKeyMapStore::from_probe_parts(
        [(
            b"key".to_vec(),
            BTreeMap::from([(search_key.clone(), b"value".to_vec())]),
        )],
        wal,
        MutationObserver::default(),
    );

    let error = store.try_pop_first_probe(b"key".to_vec()).unwrap_err();

    assert!(PrivateMutationFailure::from_io_error(&error).is_some());
    assert_eq!(
        store.get_element(b"key", &search_key),
        Some(b"value".to_vec())
    );
}
