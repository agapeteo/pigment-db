//! Private compaction-recovery behavior tests.

use crate::compaction::manifest::ManifestPhase;
use crate::compaction::manifest::{
    ArtifactDescriptor, ArtifactRole, CompactionManifest, ManifestMode, ManifestScope,
};
use crate::compaction::publication::{
    cleanup_closed_with_checkpoint, publish_closed_prepared,
    publish_closed_previous_with_checkpoint, publish_closed_replacement_with_checkpoint,
    read_published_manifest, ClosedCleanupStage, ClosedPreviousStage, ClosedReplacementStage,
};
use crate::test_support::durability_snapshot::{DurabilitySnapshot, DurableNamespaceImage};
use crate::test_support::maintenance_fixtures::{
    active_name, create_current_v2, create_segmented_v2, snapshot_directory, FixtureFamily,
};

fn prepared_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    super::PreparedClosedStaging,
) {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
    let prepared =
        super::prepare_closed_staging(&store_dir, crate::ClosedCompactionOptions::default())
            .unwrap();
    super::validate_closed_staging(&prepared).unwrap();
    super::revalidate_closed_source_inventory(&prepared).unwrap();
    (root, store_dir, prepared)
}

fn replacement_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    super::PreparedClosedStaging,
    crate::compaction::manifest::CompactionManifest,
) {
    let (root, store_dir, prepared) = prepared_fixture();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    publish_closed_replacement_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    (root, store_dir, prepared, manifest)
}

#[test]
fn prepared_retains_old_authority_and_previous_move_precedes_phase_advance() {
    let (root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let manifest = publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    assert_eq!(manifest.phase, ManifestPhase::Prepared);
    assert_eq!(snapshot_directory(&store_dir).unwrap(), old);
    assert!(!prepared.paths.previous.exists());
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::Prepared
    );
    assert!(prepared.paths.staging.is_dir());
    drop(root);

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    let interrupted = publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |stage| {
        if stage == ClosedPreviousStage::SourceMoved {
            Err(std::io::Error::other("injected after previous move"))
        } else {
            Ok(())
        }
    });
    assert!(interrupted.is_err());
    assert!(!store_dir.exists());
    assert_eq!(snapshot_directory(&prepared.paths.previous).unwrap(), old);
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::Prepared
    );
    assert!(prepared.paths.staging.is_dir());

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    let mut stages = Vec::new();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |stage| {
        stages.push(stage);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        stages,
        [
            ClosedPreviousStage::SourceMoved,
            ClosedPreviousStage::PhasePublished
        ]
    );
    assert!(!store_dir.exists());
    assert_eq!(snapshot_directory(&prepared.paths.previous).unwrap(), old);
    assert_eq!(manifest.phase, ManifestPhase::PreviousPublished);
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::PreviousPublished
    );
    assert!(prepared.paths.staging.is_dir());
}

#[test]
fn only_validated_replacement_becomes_canonical_before_replacement_phase() {
    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    let active = prepared.paths.staging.join("kv.wal.dat");
    let mut corrupt = std::fs::read(&active).unwrap();
    *corrupt.last_mut().unwrap() ^= 0xff;
    std::fs::write(&active, corrupt).unwrap();
    let before_rejection = snapshot_directory(_root.path()).unwrap();
    assert!(
        publish_closed_replacement_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).is_err()
    );
    assert_eq!(snapshot_directory(_root.path()).unwrap(), before_rejection);
    assert!(!store_dir.exists());
    assert_eq!(snapshot_directory(&prepared.paths.previous).unwrap(), old);
    assert_eq!(manifest.phase, ManifestPhase::PreviousPublished);

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let staged = snapshot_directory(&prepared.paths.staging).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    let interrupted =
        publish_closed_replacement_with_checkpoint(&prepared, &mut manifest, |stage| {
            if stage == ClosedReplacementStage::ReplacementMoved {
                Err(std::io::Error::other("injected after replacement move"))
            } else {
                Ok(())
            }
        });
    assert!(interrupted.is_err());
    assert_eq!(snapshot_directory(&store_dir).unwrap(), staged);
    assert_eq!(snapshot_directory(&prepared.paths.previous).unwrap(), old);
    assert!(!prepared.paths.staging.exists());
    assert_eq!(manifest.phase, ManifestPhase::PreviousPublished);
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::PreviousPublished
    );

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let staged = snapshot_directory(&prepared.paths.staging).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    let mut stages = Vec::new();
    publish_closed_replacement_with_checkpoint(&prepared, &mut manifest, |stage| {
        stages.push(stage);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        stages,
        [
            ClosedReplacementStage::ReplacementMoved,
            ClosedReplacementStage::ReplacementReopened,
            ClosedReplacementStage::PhasePublished,
        ]
    );
    assert_eq!(snapshot_directory(&store_dir).unwrap(), staged);
    assert_eq!(snapshot_directory(&prepared.paths.previous).unwrap(), old);
    assert!(!prepared.paths.staging.exists());
    assert_eq!(manifest.phase, ManifestPhase::ReplacementPublished);
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::ReplacementPublished
    );
}

#[test]
fn cleanup_is_phase_ordered_exact_manifest_last_and_faults_are_pending() {
    let (root, store_dir, prepared, mut manifest) = replacement_fixture();
    let canonical = snapshot_directory(&store_dir).unwrap();
    let previous = snapshot_directory(&prepared.paths.previous).unwrap();
    let status = cleanup_closed_with_checkpoint(&prepared, &mut manifest, |stage| {
        if stage == ClosedCleanupStage::CleanupPendingPublished {
            Err(std::io::Error::other("pause before cleanup"))
        } else {
            Ok(())
        }
    })
    .unwrap();
    assert_eq!(status, crate::CleanupStatus::Pending);
    assert_eq!(manifest.phase, ManifestPhase::CleanupPending);
    assert_eq!(snapshot_directory(&store_dir).unwrap(), canonical);
    assert_eq!(
        snapshot_directory(&prepared.paths.previous).unwrap(),
        previous
    );
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::CleanupPending
    );
    drop(root);

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    let previous_file = std::fs::read_dir(&prepared.paths.previous)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut changed = std::fs::read(&previous_file).unwrap();
    *changed.last_mut().unwrap() ^= 0xff;
    std::fs::write(&previous_file, changed).unwrap();
    let previous = snapshot_directory(&prepared.paths.previous).unwrap();
    assert_eq!(
        cleanup_closed_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap(),
        crate::CleanupStatus::Pending
    );
    assert_eq!(
        snapshot_directory(&prepared.paths.previous).unwrap(),
        previous
    );
    assert!(store_dir.is_dir());
    assert!(prepared.paths.manifest.is_file());

    for fault in [
        ClosedCleanupStage::BeforePreviousArtifact(0),
        ClosedCleanupStage::BeforePreviousDirectory,
        ClosedCleanupStage::BeforeManifest,
    ] {
        let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
        let canonical = snapshot_directory(&store_dir).unwrap();
        let status = cleanup_closed_with_checkpoint(&prepared, &mut manifest, |stage| {
            if stage == fault {
                Err(std::io::Error::other("injected cleanup fault"))
            } else {
                Ok(())
            }
        })
        .unwrap();
        assert_eq!(status, crate::CleanupStatus::Pending);
        assert_eq!(snapshot_directory(&store_dir).unwrap(), canonical);
        assert_eq!(manifest.phase, ManifestPhase::CleanupPending);
        assert!(prepared.paths.manifest.is_file());
    }

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    let canonical = snapshot_directory(&store_dir).unwrap();
    let artifact_count = prepared.capture.inventory.len();
    let mut stages = Vec::new();
    let status = cleanup_closed_with_checkpoint(&prepared, &mut manifest, |stage| {
        stages.push(stage);
        Ok(())
    })
    .unwrap();
    assert_eq!(status, crate::CleanupStatus::Complete);
    let mut expected = vec![ClosedCleanupStage::CleanupPendingPublished];
    expected.extend((0..artifact_count).map(ClosedCleanupStage::BeforePreviousArtifact));
    expected.push(ClosedCleanupStage::BeforePreviousDirectory);
    expected.push(ClosedCleanupStage::BeforeManifest);
    assert_eq!(stages, expected);
    assert_eq!(snapshot_directory(&store_dir).unwrap(), canonical);
    assert!(!prepared.paths.previous.exists());
    assert!(!prepared.paths.manifest.exists());
}

/// specs/011, FR-9: a lock file that a refused open left in the source after the claim retired its
/// own travels into `.previous`, and either cleanup deletes it with the generation: the
/// compactor's own, and recovery's. A directory of that name is not a lock file: it still keeps
/// cleanup pending, and in the canonical directory it leaves the authority undetermined.
#[test]
fn a_lock_file_in_the_replaced_generation_is_deleted_with_it() {
    use crate::maintenance_coordination::INNER_LOCK_NAME;

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    std::fs::write(prepared.paths.previous.join(INNER_LOCK_NAME), b"1\n").unwrap();
    let canonical = snapshot_directory(&store_dir).unwrap();
    assert_eq!(
        cleanup_closed_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap(),
        crate::CleanupStatus::Complete
    );
    assert_eq!(snapshot_directory(&store_dir).unwrap(), canonical);
    assert!(!prepared.paths.previous.exists());
    assert!(!prepared.paths.manifest.exists());

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    std::fs::write(prepared.paths.previous.join(INNER_LOCK_NAME), b"1\n").unwrap();
    assert_eq!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        )
        .unwrap(),
        crate::CleanupStatus::Complete
    );
    assert!(!prepared.paths.previous.exists());
    assert!(!prepared.paths.manifest.exists());

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    std::fs::create_dir(prepared.paths.previous.join(INNER_LOCK_NAME)).unwrap();
    assert_eq!(
        cleanup_closed_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap(),
        crate::CleanupStatus::Pending
    );
    assert!(prepared.paths.previous.join(INNER_LOCK_NAME).is_dir());
    assert_eq!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        )
        .unwrap(),
        crate::CleanupStatus::Pending
    );
    assert!(prepared.paths.previous.join(INNER_LOCK_NAME).is_dir());

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    std::fs::create_dir(store_dir.join(INNER_LOCK_NAME)).unwrap();
    assert!(matches!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        ),
        Err(crate::CompactionError::AuthorityUndetermined { .. })
    ));
    assert!(prepared.paths.previous.is_dir());
}

#[test]
fn prepared_recovery_restores_verified_old_and_discards_only_incomplete_owned_staging() {
    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let staging = snapshot_directory(&prepared.paths.staging).unwrap();
    let manifest = publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    crate::compaction::recovery::recover_prepared_closed(&store_dir, &prepared.paths, &manifest)
        .unwrap();
    assert_eq!(snapshot_directory(&store_dir).unwrap(), old);
    assert_eq!(
        snapshot_directory(&prepared.paths.staging).unwrap(),
        staging
    );
    assert!(!prepared.paths.previous.exists());

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    assert!(
        publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |stage| {
            if stage == ClosedPreviousStage::SourceMoved {
                Err(std::io::Error::other("injected split Prepared"))
            } else {
                Ok(())
            }
        })
        .is_err()
    );
    crate::compaction::recovery::recover_prepared_closed(&store_dir, &prepared.paths, &manifest)
        .unwrap();
    assert_eq!(snapshot_directory(&store_dir).unwrap(), old);
    assert!(!prepared.paths.previous.exists());
    crate::compaction::recovery::recover_prepared_closed(&store_dir, &prepared.paths, &manifest)
        .unwrap();

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let manifest = publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    let staged_active = prepared.paths.staging.join("kv.wal.dat");
    std::fs::write(&staged_active, b"incomplete-owned-staging").unwrap();
    crate::compaction::recovery::recover_prepared_closed(&store_dir, &prepared.paths, &manifest)
        .unwrap();
    assert_eq!(snapshot_directory(&store_dir).unwrap(), old);
    assert!(!prepared.paths.staging.exists());
    assert!(prepared.paths.manifest.is_file());
}

#[test]
fn only_unfinalized_online_prepared_accepts_valid_source_prefix_advancement() {
    let directory = tempfile::tempdir().unwrap();
    create_current_v2(directory.path(), FixtureFamily::KeyValue);
    let active = directory.path().join(active_name(FixtureFamily::KeyValue));
    let prefix = std::fs::read(&active).unwrap();
    let descriptor = ArtifactDescriptor {
        relative_path: std::path::PathBuf::from(active_name(FixtureFamily::KeyValue)),
        role: ArtifactRole::Active,
        family: Some(crate::StoreFamily::KeyValue),
        length: u64::try_from(prefix.len()).unwrap(),
        checksum: crc32fast::hash(&prefix),
    };
    let store = crate::key_value_store::DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"advanced".to_vec(), b"value".to_vec());
    drop(store);
    assert!(std::fs::metadata(&active).unwrap().len() > descriptor.length);

    let mut manifest = CompactionManifest {
        operation_id: *b"prefix-advance-1",
        mode: ManifestMode::OnlineFamily,
        scope: ManifestScope::Family {
            family: crate::StoreFamily::KeyValue,
            active_name: std::path::PathBuf::from(active_name(FixtureFamily::KeyValue)),
        },
        phase: ManifestPhase::Prepared,
        source_finalized: false,
        durability: crate::DurabilityPolicy::Buffered,
        source_inventory: vec![descriptor],
        staging_location: std::path::PathBuf::from("kv.wal.dat.pigment-compact.next"),
        previous_location: std::path::PathBuf::from("kv.wal.dat.pigment-compact.previous"),
        replacement_inventory: Vec::new(),
    };
    assert!(crate::compaction::recovery::source_descriptors_match(
        directory.path(),
        &manifest
    ));
    manifest.source_finalized = true;
    assert!(!crate::compaction::recovery::source_descriptors_match(
        directory.path(),
        &manifest
    ));
}

#[test]
fn previous_published_prefers_valid_replacement_then_previous_else_preserves_ambiguity() {
    for replacement_already_canonical in [false, true] {
        let (_root, store_dir, prepared) = prepared_fixture();
        let old = snapshot_directory(&store_dir).unwrap();
        let staged = snapshot_directory(&prepared.paths.staging).unwrap();
        let mut manifest =
            publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
        publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
        if replacement_already_canonical {
            assert!(publish_closed_replacement_with_checkpoint(
                &prepared,
                &mut manifest,
                |stage| if stage == ClosedReplacementStage::ReplacementMoved {
                    Err(std::io::Error::other("injected moved candidate"))
                } else {
                    Ok(())
                }
            )
            .is_err());
        }
        let selected = crate::compaction::recovery::recover_previous_published_closed(
            &store_dir,
            &prepared.paths,
            &mut manifest,
        )
        .unwrap();
        assert_eq!(
            selected,
            crate::compaction::recovery::RecoveredAuthority::Replacement
        );
        assert_eq!(snapshot_directory(&store_dir).unwrap(), staged);
        assert_eq!(snapshot_directory(&prepared.paths.previous).unwrap(), old);
        assert!(!prepared.paths.staging.exists());
        assert_eq!(manifest.phase, ManifestPhase::ReplacementPublished);
    }

    let (_root, store_dir, prepared) = prepared_fixture();
    let old = snapshot_directory(&store_dir).unwrap();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    std::fs::write(
        prepared.paths.staging.join("kv.wal.dat"),
        b"invalid replacement",
    )
    .unwrap();
    let selected = crate::compaction::recovery::recover_previous_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    assert_eq!(
        selected,
        crate::compaction::recovery::RecoveredAuthority::Previous
    );
    assert_eq!(snapshot_directory(&store_dir).unwrap(), old);
    assert!(!prepared.paths.previous.exists());
    assert!(!prepared.paths.staging.exists());
    assert!(!prepared.paths.manifest.exists());

    let (_root, store_dir, prepared) = prepared_fixture();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap();
    std::fs::write(
        prepared.paths.staging.join("kv.wal.dat"),
        b"invalid replacement",
    )
    .unwrap();
    let previous_file = std::fs::read_dir(&prepared.paths.previous)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(previous_file, b"invalid previous").unwrap();
    let evidence = snapshot_directory(_root.path()).unwrap();
    let error = crate::compaction::recovery::recover_previous_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        crate::CompactionError::AuthorityUndetermined { .. }
    ));
    assert_eq!(snapshot_directory(_root.path()).unwrap(), evidence);
}

#[test]
fn replacement_published_confirms_only_valid_canonical_while_retaining_previous() {
    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    let canonical = snapshot_directory(&store_dir).unwrap();
    let previous = snapshot_directory(&prepared.paths.previous).unwrap();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    assert_eq!(manifest.phase, ManifestPhase::CleanupPending);
    assert_eq!(snapshot_directory(&store_dir).unwrap(), canonical);
    assert_eq!(
        snapshot_directory(&prepared.paths.previous).unwrap(),
        previous
    );
    assert_eq!(
        read_published_manifest(&prepared.paths)
            .unwrap()
            .unwrap()
            .phase,
        ManifestPhase::CleanupPending
    );

    for missing_previous in [false, true] {
        let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
        if missing_previous {
            std::fs::remove_dir_all(&prepared.paths.previous).unwrap();
        } else {
            let canonical_file = std::fs::read_dir(&store_dir)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            std::fs::write(canonical_file, b"invalid canonical replacement").unwrap();
        }
        let evidence = snapshot_directory(_root.path()).unwrap();
        let error = crate::compaction::recovery::recover_replacement_published_closed(
            &store_dir,
            &prepared.paths,
            &mut manifest,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            crate::CompactionError::AuthorityUndetermined { .. }
        ));
        assert_eq!(snapshot_directory(_root.path()).unwrap(), evidence);
        assert_eq!(manifest.phase, ManifestPhase::ReplacementPublished);
    }
}

#[test]
fn cleanup_pending_validates_replacement_and_retries_missing_exact_targets_idempotently() {
    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    std::fs::remove_dir_all(&prepared.paths.previous).unwrap();
    assert_eq!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        )
        .unwrap(),
        crate::CleanupStatus::Complete
    );
    assert!(!prepared.paths.manifest.exists());
    assert_eq!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        )
        .unwrap(),
        crate::CleanupStatus::Complete
    );

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    let previous_file = std::fs::read_dir(&prepared.paths.previous)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut changed = std::fs::read(&previous_file).unwrap();
    *changed.last_mut().unwrap() ^= 0xff;
    std::fs::write(&previous_file, changed).unwrap();
    let evidence = snapshot_directory(_root.path()).unwrap();
    assert_eq!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        )
        .unwrap(),
        crate::CleanupStatus::Pending
    );
    assert_eq!(snapshot_directory(_root.path()).unwrap(), evidence);

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    let canonical = snapshot_directory(&store_dir).unwrap();
    let partial = crate::compaction::recovery::recover_cleanup_pending_closed_with_checkpoint(
        &store_dir,
        &prepared.paths,
        &manifest,
        |stage| {
            if stage == crate::compaction::recovery::RecoveryCleanupStage::Artifact(1) {
                Err(std::io::Error::other("injected after first cleanup target"))
            } else {
                Ok(())
            }
        },
    )
    .unwrap();
    assert_eq!(partial, crate::CleanupStatus::Pending);
    assert_eq!(snapshot_directory(&store_dir).unwrap(), canonical);
    assert!(prepared.paths.previous.is_dir());
    assert_eq!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        )
        .unwrap(),
        crate::CleanupStatus::Complete
    );
    assert!(!prepared.paths.previous.exists());
    assert!(!prepared.paths.manifest.exists());

    let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    let canonical_file = std::fs::read_dir(&store_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(canonical_file, b"invalid replacement").unwrap();
    let evidence = snapshot_directory(_root.path()).unwrap();
    assert!(matches!(
        crate::compaction::recovery::recover_cleanup_pending_closed(
            &store_dir,
            &prepared.paths,
            &manifest,
        ),
        Err(crate::CompactionError::AuthorityUndetermined { .. })
    ));
    assert_eq!(snapshot_directory(_root.path()).unwrap(), evidence);
}

fn copy_generation(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
    }
}

#[test]
fn untrusted_manifest_evidence_distinguishes_ambiguity_from_invalid_debris_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    create_current_v2(&store_dir, FixtureFamily::KeyValue);
    let paths = crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
    assert!(
        crate::compaction::recovery::classify_untrusted_closed_authority(&store_dir, &paths)
            .is_ok()
    );

    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    create_current_v2(&store_dir, FixtureFamily::KeyValue);
    let paths = crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
    std::fs::write(&paths.manifest, b"corrupt manifest").unwrap();
    let evidence = snapshot_directory(root.path()).unwrap();
    assert!(matches!(
        crate::compaction::recovery::classify_untrusted_closed_authority(&store_dir, &paths),
        Err(crate::CompactionError::InvalidArtifact { ref path }) if path == &paths.manifest
    ));
    assert_eq!(snapshot_directory(root.path()).unwrap(), evidence);

    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    create_current_v2(&store_dir, FixtureFamily::KeyValue);
    let paths = crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
    std::fs::create_dir(&paths.staging).unwrap();
    std::fs::write(paths.staging.join("junk"), b"invalid").unwrap();
    let evidence = snapshot_directory(root.path()).unwrap();
    assert!(matches!(
        crate::compaction::recovery::classify_untrusted_closed_authority(&store_dir, &paths),
        Err(crate::CompactionError::InvalidArtifact { ref path }) if path == &paths.staging
    ));
    assert_eq!(snapshot_directory(root.path()).unwrap(), evidence);

    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    create_current_v2(&store_dir, FixtureFamily::KeyValue);
    let paths = crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
    copy_generation(&store_dir, &paths.staging);
    let evidence = snapshot_directory(root.path()).unwrap();
    assert!(matches!(
        crate::compaction::recovery::classify_untrusted_closed_authority(&store_dir, &paths),
        Err(crate::CompactionError::AuthorityUndetermined { .. })
    ));
    assert_eq!(snapshot_directory(root.path()).unwrap(), evidence);

    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    create_current_v2(&store_dir, FixtureFamily::KeyValue);
    let paths = crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
    std::fs::rename(&store_dir, &paths.previous).unwrap();
    let evidence = snapshot_directory(root.path()).unwrap();
    assert!(matches!(
        crate::compaction::recovery::classify_untrusted_closed_authority(&store_dir, &paths),
        Err(crate::CompactionError::AuthorityUndetermined { .. })
    ));
    assert_eq!(snapshot_directory(root.path()).unwrap(), evidence);

    let (_root, store_dir, prepared) = prepared_fixture();
    let mut contradictory =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    contradictory.phase = ManifestPhase::CleanupPending;
    crate::compaction::publication::publish_manifest_buffered(&prepared.paths, &contradictory)
        .unwrap();
    let evidence = snapshot_directory(_root.path()).unwrap();
    assert!(matches!(
        crate::compaction::recovery::classify_untrusted_closed_authority(
            &store_dir,
            &prepared.paths
        ),
        Err(crate::CompactionError::AuthorityUndetermined { .. })
    ));
    assert_eq!(snapshot_directory(_root.path()).unwrap(), evidence);
}

#[test]
fn every_file_initializer_resolves_maintenance_before_ordinary_wal_recovery() {
    for family in [
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
        FixtureFamily::KeyMap,
    ] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, family);
        let prepared =
            super::prepare_closed_staging(&store_dir, crate::ClosedCompactionOptions::default())
                .unwrap();
        let mut manifest =
            publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
        assert!(
            publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |stage| {
                if stage == ClosedPreviousStage::SourceMoved {
                    Err(std::io::Error::other("injected split Prepared"))
                } else {
                    Ok(())
                }
            })
            .is_err()
        );

        let status = match family {
            FixtureFamily::KeyValue => {
                let outcome =
                    crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).unwrap();
                assert_eq!(outcome.store().get(b"alpha"), Some(b"one".to_vec()));
                outcome.status()
            }
            FixtureFamily::KeySet => {
                let outcome =
                    crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir).unwrap();
                assert!(outcome
                    .store()
                    .get_hashset(b"group")
                    .unwrap()
                    .contains(b"red".as_slice()));
                outcome.status()
            }
            FixtureFamily::KeyMap => {
                let outcome =
                    crate::key_map_store::DurableKeyMapStore::try_init_new(&store_dir).unwrap();
                assert_eq!(
                    outcome
                        .store()
                        .get_element(b"book", &crate::model::SearchKey::from(1)),
                    Some(b"one".to_vec())
                );
                outcome.status()
            }
        };
        assert_eq!(status, crate::RecoveryStatus::Recovered);
        assert!(!prepared.paths.staging.exists());
        assert!(!prepared.paths.previous.exists());
        assert!(!prepared.paths.manifest.exists());
    }

    let (_root, store_dir, prepared) = prepared_fixture();
    let mut manifest =
        publish_closed_prepared(&prepared, crate::DurabilityPolicy::Buffered).unwrap();
    assert!(
        publish_closed_previous_with_checkpoint(&prepared, &mut manifest, |stage| {
            if stage == ClosedPreviousStage::SourceMoved {
                Err(std::io::Error::other("injected split Prepared"))
            } else {
                Ok(())
            }
        })
        .is_err()
    );
    let previous_file = std::fs::read_dir(&prepared.paths.previous)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(previous_file, b"invalid old authority").unwrap();
    let evidence = snapshot_directory(_root.path()).unwrap();
    assert!(crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).is_err());
    // A maintenance-state open takes the replacement lock before it reads the evidence
    // (specs/011 FR-5), so a refused one leaves that lock file beside the directory, and nothing
    // else.
    let mut after = snapshot_directory(_root.path()).unwrap();
    assert!(after
        .remove(std::path::Path::new(".store.pigment-lock"))
        .is_some());
    assert_eq!(after, evidence);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClosedFaultCut {
    StagingCreate,
    StagingWrite,
    StagingSync,
    StagingValidate,
    ManifestWrite,
    ManifestSync,
    PreviousMove,
    PreviousPhaseRewrite,
    ReplacementMove,
    ReplacementReopen,
    ReplacementPhaseRewrite,
    CleanupPhaseRewrite,
    Cleanup,
}

struct ModeledClosedPaths {
    parent: std::path::PathBuf,
    source: std::path::PathBuf,
    staging: std::path::PathBuf,
    previous: std::path::PathBuf,
    manifest: std::path::PathBuf,
    manifest_next: std::path::PathBuf,
}

fn modeled_closed_paths(family: FixtureFamily) -> ModeledClosedPaths {
    let parent = std::path::PathBuf::from("/modeled-parent");
    let active = std::path::Path::new(active_name(family));
    ModeledClosedPaths {
        source: parent.join("store").join(active),
        staging: parent.join("store.compaction-next").join(active),
        previous: parent.join("store.compaction-previous").join(active),
        manifest: parent.join("store.compaction-manifest"),
        manifest_next: parent.join("store.compaction-manifest.next"),
        parent,
    }
}

fn finish_modeled_manifest_publication(
    snapshot: &mut DurabilitySnapshot,
    paths: &ModeledClosedPaths,
    phase: &[u8],
    policy: crate::DurabilityPolicy,
) {
    snapshot.write(&paths.manifest_next, phase).unwrap();
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_file(&paths.manifest_next).unwrap();
    }
    snapshot
        .rename_replace(&paths.manifest_next, &paths.manifest)
        .unwrap();
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_directory(&paths.parent).unwrap();
    }
}

fn modeled_closed_fault_image(
    family: FixtureFamily,
    policy: crate::DurabilityPolicy,
    cut: ClosedFaultCut,
) -> (ModeledClosedPaths, DurableNamespaceImage) {
    let paths = modeled_closed_paths(family);
    let mut snapshot = DurabilitySnapshot::new(None);
    snapshot.write(&paths.source, b"old-complete").unwrap();
    snapshot.sync_file(&paths.source).unwrap();
    snapshot.sync_directory(&paths.parent).unwrap();

    if cut == ClosedFaultCut::StagingCreate {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    snapshot.write(&paths.staging, b"new-partial").unwrap();
    if cut == ClosedFaultCut::StagingWrite {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    snapshot.write(&paths.staging, b"new-complete").unwrap();
    if cut == ClosedFaultCut::StagingSync {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_file(&paths.staging).unwrap();
    }
    if cut == ClosedFaultCut::StagingValidate {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }

    snapshot
        .write(&paths.manifest_next, b"Prepared-partial")
        .unwrap();
    if cut == ClosedFaultCut::ManifestWrite {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    snapshot.write(&paths.manifest_next, b"Prepared").unwrap();
    if cut == ClosedFaultCut::ManifestSync {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_file(&paths.manifest_next).unwrap();
    }
    snapshot
        .rename_replace(&paths.manifest_next, &paths.manifest)
        .unwrap();
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_directory(&paths.parent).unwrap();
    }

    snapshot.rename(&paths.source, &paths.previous).unwrap();
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_directory(&paths.parent).unwrap();
    }
    if cut == ClosedFaultCut::PreviousMove {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    finish_modeled_manifest_publication(&mut snapshot, &paths, b"PreviousPublished", policy);
    if cut == ClosedFaultCut::PreviousPhaseRewrite {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }

    snapshot.rename(&paths.staging, &paths.source).unwrap();
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_directory(&paths.parent).unwrap();
    }
    if cut == ClosedFaultCut::ReplacementMove {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    if cut == ClosedFaultCut::ReplacementReopen {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    finish_modeled_manifest_publication(&mut snapshot, &paths, b"ReplacementPublished", policy);
    if cut == ClosedFaultCut::ReplacementPhaseRewrite {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    finish_modeled_manifest_publication(&mut snapshot, &paths, b"CleanupPending", policy);
    if cut == ClosedFaultCut::CleanupPhaseRewrite {
        return (paths, modeled_interruption_image(&mut snapshot, policy));
    }
    snapshot.remove(&paths.previous).unwrap();
    if policy == crate::DurabilityPolicy::Physical {
        snapshot.sync_directory(&paths.parent).unwrap();
    }
    (paths, modeled_interruption_image(&mut snapshot, policy))
}

fn modeled_interruption_image(
    snapshot: &mut DurabilitySnapshot,
    policy: crate::DurabilityPolicy,
) -> DurableNamespaceImage {
    match policy {
        crate::DurabilityPolicy::Buffered => snapshot.volatile_image(),
        crate::DurabilityPolicy::Physical => snapshot.simulate_power_loss(),
    }
}

#[test]
fn every_closed_fault_cut_retains_a_complete_authority_for_all_families_and_policies() {
    let cuts = [
        ClosedFaultCut::StagingCreate,
        ClosedFaultCut::StagingWrite,
        ClosedFaultCut::StagingSync,
        ClosedFaultCut::StagingValidate,
        ClosedFaultCut::ManifestWrite,
        ClosedFaultCut::ManifestSync,
        ClosedFaultCut::PreviousMove,
        ClosedFaultCut::PreviousPhaseRewrite,
        ClosedFaultCut::ReplacementMove,
        ClosedFaultCut::ReplacementReopen,
        ClosedFaultCut::ReplacementPhaseRewrite,
        ClosedFaultCut::CleanupPhaseRewrite,
        ClosedFaultCut::Cleanup,
    ];
    for family in [
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
        FixtureFamily::KeyMap,
    ] {
        for policy in [
            crate::DurabilityPolicy::Buffered,
            crate::DurabilityPolicy::Physical,
        ] {
            for cut in cuts {
                let (paths, image) = modeled_closed_fault_image(family, policy, cut);
                let authorities = [
                    image.files.get(&paths.source),
                    image.files.get(&paths.previous),
                    image.files.get(&paths.staging),
                ];
                assert!(
                    authorities.iter().flatten().any(|bytes| {
                        bytes.as_slice() == b"old-complete" || bytes.as_slice() == b"new-complete"
                    }),
                    "{family:?} {policy:?} {cut:?} lost every complete authority: {image:?}"
                );
                assert!(
                    image.files.get(&paths.source).is_none_or(|bytes| {
                        bytes.as_slice() == b"old-complete" || bytes.as_slice() == b"new-complete"
                    }),
                    "{family:?} {policy:?} {cut:?} exposed partial canonical bytes"
                );
            }
        }
    }
}

#[test]
fn closed_compaction_checkpoint_child() {
    let Some(store_dir) = crate::test_support::fault_checkpoint::maintenance_child_store_dir()
    else {
        return;
    };
    crate::maintenance::compact_directory_in_place_internal(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    )
    .unwrap();
    if crate::test_support::fault_checkpoint::maintenance_child_pauses() {
        std::process::exit(
            crate::test_support::fault_checkpoint::MAINTENANCE_PAUSED_CHILD_COMPLETED,
        );
    }
    panic!("checkpoint child completed without reaching the requested cut");
}

/// R1 (specs/011): while another process holds a closed-compaction claim -- staging validated
/// and the inner lock retired, the directory moved aside, or the replacement published -- an open
/// is refused by the replacement lock, and opens once the compaction has finished.
#[test]
fn an_open_is_refused_while_another_process_holds_a_closed_claim() {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        MAINTENANCE_PAUSED_CHILD_COMPLETED,
    };
    for point in [
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut: MaintenanceCut::StagingValidate,
        },
        MaintenanceFaultPoint {
            phase: MaintenancePhase::PreviousPublished,
            cut: MaintenanceCut::PreviousPublish,
        },
        MaintenanceFaultPoint {
            phase: MaintenancePhase::ReplacementPublished,
            cut: MaintenanceCut::ReopenValidation,
        },
    ] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let pause_dir = tempfile::tempdir().unwrap();
        let child = pause_maintenance_child(
            "compaction::recovery_tests::closed_compaction_checkpoint_child",
            &store_dir,
            pause_dir.path(),
            point,
        );

        let refused = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir);

        assert_eq!(
            child.resume(),
            MAINTENANCE_PAUSED_CHILD_COMPLETED,
            "{point:?}"
        );
        match refused {
            Err(crate::RecoveryError::Io {
                operation: crate::RecoveryOperation::Inspect,
                source,
                ..
            }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::WouldBlock, "{point:?}");
                assert!(
                    source.to_string().contains(".store.pigment-lock"),
                    "{point:?}: the replacement lock must refuse: {source}"
                );
            }
            Err(error) => panic!("{point:?}: expected a WouldBlock refusal, got {error:?}"),
            Ok(_) => panic!("{point:?}: an open during another process's compaction must fail"),
        }
        let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()), "{point:?}");
    }
}

/// A closed compaction whose cleanup cannot finish yet: the replacement is canonical and the
/// manifest is `CleanupPending`, but one artifact of `.previous` no longer matches its descriptor,
/// so cleanup deletes nothing. Returns that artifact and its original bytes, so a test can let
/// cleanup succeed later.
fn cleanup_pending_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    super::PreparedClosedStaging,
    std::path::PathBuf,
    Vec<u8>,
) {
    let (root, store_dir, prepared, mut manifest) = replacement_fixture();
    crate::compaction::recovery::recover_replacement_published_closed(
        &store_dir,
        &prepared.paths,
        &mut manifest,
    )
    .unwrap();
    let previous_file = std::fs::read_dir(&prepared.paths.previous)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let original = std::fs::read(&previous_file).unwrap();
    let mut changed = original.clone();
    *changed.last_mut().unwrap() ^= 0xff;
    std::fs::write(&previous_file, changed).unwrap();
    (root, store_dir, prepared, previous_file, original)
}

/// Opens the key/value family of a directory whose cleanup stays pending, and checks the state
/// the later steps start from: the open succeeded, the manifest remains, and the open took the
/// directory's inner lock (specs/011, FR-5 step 3).
fn open_while_cleanup_is_pending(
    store_dir: &std::path::Path,
    prepared: &super::PreparedClosedStaging,
) -> crate::key_value_store::DurableKeyValueStore<std::fs::File> {
    let outcome = crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir).unwrap();
    assert_eq!(outcome.status(), crate::RecoveryStatus::Recovered);
    assert!(
        prepared.paths.manifest.is_file(),
        "cleanup must still be pending"
    );
    assert!(
        store_dir
            .join(crate::maintenance_coordination::INNER_LOCK_NAME)
            .is_file(),
        "the open must hold the inner lock of the directory it recovered"
    );
    outcome.into_store()
}

fn assert_cleanup_complete(prepared: &super::PreparedClosedStaging) {
    assert!(
        !prepared.paths.manifest.exists(),
        "the manifest must be gone"
    );
    assert!(
        !prepared.paths.previous.exists(),
        "`.previous` must be gone"
    );
}

/// specs/011: the inner lock an open takes while cleanup stays pending is not a store artifact,
/// so once cleanup can proceed, the next open finishes it.
#[test]
fn a_reopen_finishes_a_cleanup_that_an_earlier_open_left_pending() {
    let (_root, store_dir, prepared, previous_file, original) = cleanup_pending_fixture();
    drop(open_while_cleanup_is_pending(&store_dir, &prepared));
    std::fs::write(&previous_file, original).unwrap();

    let outcome = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).unwrap();

    assert_eq!(outcome.status(), crate::RecoveryStatus::Recovered);
    assert_cleanup_complete(&prepared);
    assert_eq!(outcome.into_store().get(b"alpha"), Some(b"one".to_vec()));
}

/// specs/011: a second family opened in the same process, while the first still holds the
/// directory, finishes the cleanup the first open left pending.
#[test]
fn a_second_family_finishes_a_cleanup_the_first_left_pending() {
    let (_root, store_dir, prepared, previous_file, original) = cleanup_pending_fixture();
    let first = open_while_cleanup_is_pending(&store_dir, &prepared);
    std::fs::write(&previous_file, original).unwrap();

    let second = crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir);

    assert!(second.is_ok(), "{:?}", second.err());
    assert_cleanup_complete(&prepared);
    assert_eq!(first.get(b"alpha"), Some(b"one".to_vec()));
}

/// specs/011: compacting again retries the cleanup an earlier open left pending, as
/// `compact_directory_in_place` documents.
#[test]
fn a_compaction_retry_finishes_a_cleanup_that_an_earlier_open_left_pending() {
    let (_root, store_dir, prepared, previous_file, original) = cleanup_pending_fixture();
    drop(open_while_cleanup_is_pending(&store_dir, &prepared));
    std::fs::write(&previous_file, original).unwrap();

    let compacted =
        crate::compact_directory_in_place(&store_dir, crate::ClosedCompactionOptions::default());

    assert!(compacted.is_ok(), "{:?}", compacted.err());
    assert_cleanup_complete(&prepared);
    let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
}

/// specs/011: an open that checked for maintenance before another process's closed compaction
/// staged anything, and reaches its inner lock only after the claim retired it, is refused by
/// the replacement lock. The lock file it creates on the way must not break the compaction: the
/// compaction completes and the directory reopens with its data.
#[test]
fn an_open_stalled_across_another_processs_compaction_neither_opens_nor_breaks_it() {
    use crate::maintenance_coordination::lock_seams::Stall;
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        MAINTENANCE_PAUSED_CHILD_COMPLETED,
    };
    for point in [
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut: MaintenanceCut::StagingValidate,
        },
        MaintenanceFaultPoint {
            phase: MaintenancePhase::ReplacementPublished,
            cut: MaintenanceCut::ReopenValidation,
        },
    ] {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let stall = Stall::install(&store_dir);
        let opener = {
            let store_dir = store_dir.clone();
            std::thread::spawn(move || {
                crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).map(|_| ())
            })
        };
        stall.wait_entered();
        let pause_dir = tempfile::tempdir().unwrap();
        let child = pause_maintenance_child(
            "compaction::recovery_tests::closed_compaction_checkpoint_child",
            &store_dir,
            pause_dir.path(),
            point,
        );

        stall.release();
        let refused = opener.join().unwrap();
        assert!(
            store_dir
                .join(crate::maintenance_coordination::INNER_LOCK_NAME)
                .is_file(),
            "{point:?}: the refused open must have left its lock file in the directory"
        );
        let exit = child.resume();

        match refused {
            Err(crate::RecoveryError::Io {
                operation: crate::RecoveryOperation::Inspect,
                source,
                ..
            }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::WouldBlock, "{point:?}");
                assert!(
                    source.to_string().contains(".store.pigment-lock"),
                    "{point:?}: the replacement lock must refuse: {source}"
                );
            }
            Err(error) => panic!("{point:?}: expected a WouldBlock refusal, got {error:?}"),
            Ok(()) => panic!("{point:?}: an open during another process's compaction must fail"),
        }
        assert_eq!(
            exit, MAINTENANCE_PAUSED_CHILD_COMPLETED,
            "{point:?}: the compaction must complete"
        );
        let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()), "{point:?}");
        drop(reopened);
        let mut left = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        left.sort();
        assert_eq!(left, [".store.pigment-lock", "store"], "{point:?}");
    }
}

/// Whether some descriptor other than a fresh one holds `store_dir`'s inner lock. A lock taken
/// through another descriptor refuses this one in the same process as in any other.
fn inner_lock_is_held(store_dir: &std::path::Path) -> bool {
    let file =
        std::fs::File::open(store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME))
            .expect("the inner lock file must exist");
    match file.try_lock() {
        Ok(()) => false,
        Err(std::fs::TryLockError::WouldBlock) => true,
        Err(std::fs::TryLockError::Error(error)) => panic!("cannot probe the inner lock: {error}"),
    }
}

/// Leaves `store_dir` as a closed compaction killed after moving the directory aside, so the next
/// open or claim must recover before it can do anything else.
fn interrupted_compaction(store_dir: &std::path::Path) {
    use crate::test_support::fault_checkpoint::{
        run_maintenance_checkpoint_child_with_evidence_root, MaintenanceCut, MaintenanceFaultPoint,
        MaintenancePhase,
    };
    create_segmented_v2(store_dir, FixtureFamily::KeyValue);
    // Asserts that the child exited at exactly this cut. The directory is moved aside by then, so
    // the evidence is taken of its parent.
    run_maintenance_checkpoint_child_with_evidence_root(
        "compaction::recovery_tests::closed_compaction_checkpoint_child",
        store_dir,
        store_dir.parent().unwrap(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::PreviousPublished,
            cut: MaintenanceCut::PreviousPublish,
        },
    );
    assert!(
        crate::compaction::publication::directory_artifact_paths(store_dir)
            .unwrap()
            .manifest
            .is_file(),
        "the interrupted compaction must leave maintenance to recover"
    );
}

/// specs/011, FR-5 step 3: an open that recovered interrupted maintenance holds the inner lock of
/// the directory recovery left in place. Only the inner lock is shared by every mount view of the
/// directory; the replacement lock the open also holds lives in its own view's parent.
#[test]
fn an_open_that_recovered_holds_the_inner_lock() {
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    interrupted_compaction(&store_dir);

    let outcome = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).unwrap();

    assert_eq!(outcome.status(), crate::RecoveryStatus::Recovered);
    assert!(inner_lock_is_held(&store_dir));
    #[cfg(unix)]
    assert_eq!(
        std::fs::read_to_string(store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME))
            .unwrap(),
        format!("{}\n", std::process::id())
    );
    drop(outcome);
    assert!(!inner_lock_is_held(&store_dir));
}

/// specs/011: a closed-maintenance claim that recovered interrupted maintenance holds the inner
/// lock while it stages the next compaction, until it retires it before publication.
#[test]
fn a_claim_that_recovered_holds_the_inner_lock_while_it_stages() {
    use crate::test_support::fault_checkpoint::{
        pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        MAINTENANCE_PAUSED_CHILD_COMPLETED,
    };
    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    interrupted_compaction(&store_dir);
    let pause_dir = tempfile::tempdir().unwrap();
    let child = pause_maintenance_child(
        "compaction::recovery_tests::closed_compaction_checkpoint_child",
        &store_dir,
        pause_dir.path(),
        MaintenanceFaultPoint {
            phase: MaintenancePhase::Prepared,
            cut: MaintenanceCut::StagingSync,
        },
    );

    let held = inner_lock_is_held(&store_dir);
    #[cfg(unix)]
    let owner =
        std::fs::read_to_string(store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME))
            .unwrap();
    #[cfg(unix)]
    let child_id = child.id();

    assert_eq!(child.resume(), MAINTENANCE_PAUSED_CHILD_COMPLETED);
    assert!(held, "the claim must hold the inner lock while it stages");
    #[cfg(unix)]
    assert_eq!(owner, format!("{child_id}\n"));
    let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
}

/// specs/011, FR-13: the inner lock an open takes after recovering is also opened outside the
/// ownership registry's mutex. While that lock-file open is stalled for one directory, another
/// directory opens and a third directory's store is dropped.
#[test]
fn a_stalled_inner_lock_after_recovery_holds_up_no_other_directory() {
    use crate::maintenance_coordination::lock_seams::Stall;
    use std::sync::mpsc;
    use std::time::Duration;

    let root = tempfile::tempdir().unwrap();
    let store_dir = root.path().join("store");
    std::fs::create_dir(&store_dir).unwrap();
    interrupted_compaction(&store_dir);
    let manifest = crate::compaction::publication::directory_artifact_paths(&store_dir)
        .unwrap()
        .manifest;
    let other = tempfile::tempdir().unwrap();
    let dropped = tempfile::tempdir().unwrap();
    let held = crate::key_value_store::DurableKeyValueStore::try_init_new(dropped.path())
        .unwrap()
        .into_store();
    let stall = Stall::install(&store_dir);

    let recovering = {
        let store_dir = store_dir.clone();
        std::thread::spawn(move || {
            crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
                .map(|outcome| outcome.status())
        })
    };
    stall.wait_entered();
    let recovered_before_the_stall = !manifest.exists();
    let (done, finished) = mpsc::channel();
    let other_path = other.path().to_path_buf();
    std::thread::spawn(move || {
        let opened =
            crate::key_value_store::DurableKeyValueStore::try_init_new(&other_path).is_ok();
        drop(held);
        let _ = done.send(opened);
    });
    let progressed = finished.recv_timeout(Duration::from_secs(5));

    stall.release();
    let recovered = recovering.join().unwrap();
    // Joined before asserting, so a failure leaves no thread parked on the gate.
    let _ = finished.recv_timeout(Duration::from_secs(30));

    assert!(
        recovered_before_the_stall,
        "the stall must hold the inner lock taken after recovery"
    );
    assert_eq!(
        progressed,
        Ok(true),
        "another directory's open and a third's drop waited behind a stalled lock file"
    );
    assert_eq!(recovered.unwrap(), crate::RecoveryStatus::Recovered);
}

fn reopen_after_checkpoint(
    store_dir: &std::path::Path,
    family: FixtureFamily,
) -> Result<crate::RecoveryStatus, crate::RecoveryError> {
    match family {
        FixtureFamily::KeyValue => {
            crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir)
                .map(|outcome| outcome.status())
        }
        FixtureFamily::KeySet => crate::key_set_store::DurableKeySetStore::try_init_new(store_dir)
            .map(|outcome| outcome.status()),
        FixtureFamily::KeyMap => crate::key_map_store::DurableKeyMapStore::try_init_new(store_dir)
            .map(|outcome| outcome.status()),
    }
}

#[test]
fn every_closed_checkpoint_process_exit_reopens_exact_state_or_preserves_explicit_evidence() {
    use crate::test_support::fault_checkpoint::{
        run_maintenance_checkpoint_child_with_evidence_root, MaintenanceCut, MaintenanceFaultPoint,
        MaintenancePhase,
    };

    let points = [
        (MaintenancePhase::Prepared, MaintenanceCut::StagingCreate),
        (MaintenancePhase::Prepared, MaintenanceCut::StagingWrite),
        (MaintenancePhase::Prepared, MaintenanceCut::StagingSync),
        (MaintenancePhase::Prepared, MaintenanceCut::StagingValidate),
        (MaintenancePhase::Prepared, MaintenanceCut::ManifestWrite),
        (MaintenancePhase::Prepared, MaintenanceCut::ManifestSync),
        (MaintenancePhase::Prepared, MaintenanceCut::ManifestPublish),
        (
            MaintenancePhase::PreviousPublished,
            MaintenanceCut::PreviousPublish,
        ),
        (
            MaintenancePhase::PreviousPublished,
            MaintenanceCut::ManifestWrite,
        ),
        (
            MaintenancePhase::PreviousPublished,
            MaintenanceCut::ManifestSync,
        ),
        (
            MaintenancePhase::PreviousPublished,
            MaintenanceCut::ManifestPublish,
        ),
        (
            MaintenancePhase::ReplacementPublished,
            MaintenanceCut::ReplacementPublish,
        ),
        (
            MaintenancePhase::ReplacementPublished,
            MaintenanceCut::ReopenValidation,
        ),
        (
            MaintenancePhase::ReplacementPublished,
            MaintenanceCut::ManifestWrite,
        ),
        (
            MaintenancePhase::ReplacementPublished,
            MaintenanceCut::ManifestSync,
        ),
        (
            MaintenancePhase::ReplacementPublished,
            MaintenanceCut::ManifestPublish,
        ),
        (
            MaintenancePhase::CleanupPending,
            MaintenanceCut::ManifestWrite,
        ),
        (
            MaintenancePhase::CleanupPending,
            MaintenanceCut::ManifestSync,
        ),
        (
            MaintenancePhase::CleanupPending,
            MaintenanceCut::ManifestPublish,
        ),
        (MaintenancePhase::CleanupPending, MaintenanceCut::Cleanup),
    ];

    for family in [
        FixtureFamily::KeyValue,
        FixtureFamily::KeySet,
        FixtureFamily::KeyMap,
    ] {
        for (phase, cut) in points {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            create_segmented_v2(&store_dir, family);
            let point = MaintenanceFaultPoint { phase, cut };
            let evidence = run_maintenance_checkpoint_child_with_evidence_root(
                "compaction::recovery_tests::closed_compaction_checkpoint_child",
                &store_dir,
                root.path(),
                point,
            );
            let paths =
                crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
            assert!(
                paths.staging.exists()
                    || paths.previous.exists()
                    || paths.manifest.exists()
                    || paths.manifest_next.exists(),
                "{family:?} {phase:?} {cut:?} must leave maintenance evidence"
            );
            let _ = evidence;
            // With maintenance in progress, the reopen takes only the replacement lock the claim
            // created, and records its own process id in it before it reads anything
            // (specs/011). A refused reopen must leave everything else as it found it, including
            // any inner lock file the claim left.
            let without_replacement_lock =
                |mut snapshot: crate::test_support::maintenance_fixtures::DirectoryByteSnapshot| {
                    assert!(
                        snapshot
                            .remove(std::path::Path::new(".store.pigment-lock"))
                            .is_some(),
                        "{family:?} {phase:?} {cut:?}: the replacement lock file must exist"
                    );
                    snapshot
                };
            let before_reopen = without_replacement_lock(snapshot_directory(root.path()).unwrap());
            match reopen_after_checkpoint(&store_dir, family) {
                Ok(status) => {
                    assert_eq!(status, crate::RecoveryStatus::Recovered);
                    crate::test_support::maintenance_fixtures::assert_three_reopens(
                        &store_dir, family,
                    );
                }
                Err(error) => {
                    assert!(
                        matches!(
                            error,
                            crate::RecoveryError::AuthorityUndetermined { .. }
                                | crate::RecoveryError::InvalidArtifact { .. }
                        ),
                        "{family:?} {phase:?} {cut:?} returned unexpected {error:?}"
                    );
                    assert_eq!(
                        without_replacement_lock(snapshot_directory(root.path()).unwrap()),
                        before_reopen
                    );
                }
            }
        }
    }
}
