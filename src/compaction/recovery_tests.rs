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
    if let Err(error) = crate::maintenance::compact_directory_in_place_internal(
        &store_dir,
        crate::ClosedCompactionOptions::default(),
    ) {
        eprintln!("maintenance child: compaction failed: {error:?}");
        std::process::exit(crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED);
    }
    if crate::test_support::fault_checkpoint::maintenance_child_pauses() {
        std::process::exit(
            crate::test_support::fault_checkpoint::MAINTENANCE_PAUSED_CHILD_COMPLETED,
        );
    }
    panic!("checkpoint child completed without reaching the requested cut");
}

/// Cross-process directory ownership (specs/011) at closed maintenance, grouped so that CI runs
/// them on every operating system.
mod ownership {
    use super::*;

    /// Every entry of `directory` by name, with a regular file's bytes; symlinks and directories are
    /// listed without following them.
    fn entries_of(directory: &std::path::Path) -> Vec<(std::ffi::OsString, Option<Vec<u8>>)> {
        let mut entries = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let bytes = entry
                    .file_type()
                    .unwrap()
                    .is_file()
                    .then(|| std::fs::read(entry.path()).unwrap());
                (entry.file_name(), bytes)
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries
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

        // Anything else of that name, or a near name, is foreign: both cleanups stay pending and
        // `.previous` keeps every artifact.
        type Plant = fn(&std::path::Path);
        let foreign: [(&str, Plant); 3] = [
            ("a directory", |previous| {
                std::fs::create_dir(previous.join(INNER_LOCK_NAME)).unwrap()
            }),
            ("a near name", |previous| {
                std::fs::write(previous.join(format!("{INNER_LOCK_NAME}.old")), b"1\n").unwrap()
            }),
            ("a symlink", |previous| {
                #[cfg(unix)]
                std::os::unix::fs::symlink(
                    previous.join("kv.wal.dat"),
                    previous.join(INNER_LOCK_NAME),
                )
                .unwrap();
                #[cfg(not(unix))]
                std::fs::create_dir(previous.join(INNER_LOCK_NAME)).unwrap();
            }),
        ];
        for (what, plant) in foreign {
            let (_root, store_dir, prepared, mut manifest) = replacement_fixture();
            plant(&prepared.paths.previous);
            let previous = entries_of(&prepared.paths.previous);
            assert_eq!(
                cleanup_closed_with_checkpoint(&prepared, &mut manifest, |_| Ok(())).unwrap(),
                crate::CleanupStatus::Pending,
                "{what}"
            );
            assert_eq!(entries_of(&prepared.paths.previous), previous, "{what}");
            assert_eq!(
                crate::compaction::recovery::recover_cleanup_pending_closed(
                    &store_dir,
                    &prepared.paths,
                    &manifest,
                )
                .unwrap(),
                crate::CleanupStatus::Pending,
                "{what}"
            );
            assert_eq!(entries_of(&prepared.paths.previous), previous, "{what}");
        }

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
            // FR-4: the claim deleted its inner lock file before it published anything.
            assert!(
                !store_dir
                    .join(crate::maintenance_coordination::INNER_LOCK_NAME)
                    .exists(),
                "{point:?}"
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
        crate::compaction::PreparedClosedStaging,
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
        prepared: &crate::compaction::PreparedClosedStaging,
    ) -> crate::key_value_store::DurableKeyValueStore<std::fs::File> {
        let outcome =
            crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir).unwrap();
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

    fn assert_cleanup_complete(prepared: &crate::compaction::PreparedClosedStaging) {
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

        let outcome =
            crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).unwrap();

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

        let compacted = crate::compact_directory_in_place(
            &store_dir,
            crate::ClosedCompactionOptions::default(),
        );

        assert!(compacted.is_ok(), "{:?}", compacted.err());
        assert_cleanup_complete(&prepared);
        let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
            .unwrap()
            .into_store();
        assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()));
    }

    /// What an open, stalled at `stall` while another process's closed compaction ran to `point`,
    /// did once released, and what the directory looked like afterwards.
    struct StalledOpen {
        refused: Result<(), crate::RecoveryError>,
        left_a_lock_file: bool,
        compaction_exit: i32,
        root: Vec<std::ffi::OsString>,
    }

    fn stalled_open_across_a_compaction(
        point: crate::test_support::fault_checkpoint::MaintenanceFaultPoint,
        stall: crate::maintenance_coordination::lock_seams::StallPoint,
    ) -> StalledOpen {
        use crate::maintenance_coordination::lock_seams::Stall;
        use crate::test_support::fault_checkpoint::pause_maintenance_child;

        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let stalled = Stall::install_at(&store_dir, stall);
        let opener = {
            let store_dir = store_dir.clone();
            std::thread::spawn(move || {
                crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).map(|_| ())
            })
        };
        stalled.wait_entered();
        let pause_dir = tempfile::tempdir().unwrap();
        let child = pause_maintenance_child(
            "compaction::recovery_tests::closed_compaction_checkpoint_child",
            &store_dir,
            pause_dir.path(),
            point,
        );

        stalled.release();
        let refused = opener.join().unwrap();
        let left_a_lock_file = store_dir
            .join(crate::maintenance_coordination::INNER_LOCK_NAME)
            .is_file();
        let compaction_exit = child.resume();

        let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
            .unwrap_or_else(|error| panic!("{point:?}: the directory must reopen: {error:?}"))
            .into_store();
        assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()), "{point:?}");
        drop(reopened);
        let mut left = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        left.sort();
        StalledOpen {
            refused,
            left_a_lock_file,
            compaction_exit,
            root: left,
        }
    }

    /// Asserts that `open` was refused for `kind` with a message naming `named`, that the
    /// compaction completed, and that nothing but the directory and its replacement lock remain.
    fn assert_refused_and_harmless(
        point: crate::test_support::fault_checkpoint::MaintenanceFaultPoint,
        open: StalledOpen,
        kind: std::io::ErrorKind,
        named: &str,
    ) {
        match open.refused {
            Err(crate::RecoveryError::Io {
                operation: crate::RecoveryOperation::Inspect,
                source,
                ..
            }) => {
                assert_eq!(source.kind(), kind, "{point:?}: {source}");
                assert!(
                    source.to_string().contains(named),
                    "{point:?}: the refusal must name {named}: {source}"
                );
            }
            Err(error) => panic!("{point:?}: expected an Inspect refusal, got {error:?}"),
            Ok(()) => panic!("{point:?}: an open during another process's compaction must fail"),
        }
        assert_eq!(
            open.compaction_exit,
            crate::test_support::fault_checkpoint::MAINTENANCE_PAUSED_CHILD_COMPLETED,
            "{point:?}: the compaction must complete"
        );
        assert_eq!(open.root, [".store.pigment-lock", "store"], "{point:?}");
    }

    /// specs/011: an open that checked for maintenance before another process's closed compaction
    /// staged anything, and reaches its inner lock only once the claim has retired it, neither
    /// opens nor breaks the compaction. Where the directory is present it creates a lock file and
    /// is refused by the replacement lock; while the directory is moved aside it finds no directory
    /// and creates nothing.
    #[test]
    fn an_open_stalled_across_another_processs_compaction_neither_opens_nor_breaks_it() {
        use crate::maintenance_coordination::lock_seams::StallPoint;
        use crate::test_support::fault_checkpoint::{
            MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        };
        let inner_lock = std::path::Path::new("store")
            .join(crate::maintenance_coordination::INNER_LOCK_NAME)
            .display()
            .to_string();
        for (point, directory_present) in [
            (
                MaintenanceFaultPoint {
                    phase: MaintenancePhase::Prepared,
                    cut: MaintenanceCut::StagingValidate,
                },
                true,
            ),
            (
                MaintenanceFaultPoint {
                    phase: MaintenancePhase::PreviousPublished,
                    cut: MaintenanceCut::PreviousPublish,
                },
                false,
            ),
            (
                MaintenanceFaultPoint {
                    phase: MaintenancePhase::ReplacementPublished,
                    cut: MaintenanceCut::ReopenValidation,
                },
                true,
            ),
        ] {
            let open = stalled_open_across_a_compaction(point, StallPoint::LockFileOpen);
            assert_eq!(
                open.left_a_lock_file, directory_present,
                "{point:?}: a refused open leaves a lock file exactly where it found a directory"
            );
            if directory_present {
                assert_refused_and_harmless(
                    point,
                    open,
                    std::io::ErrorKind::WouldBlock,
                    ".store.pigment-lock",
                );
            } else {
                assert_refused_and_harmless(point, open, std::io::ErrorKind::NotFound, &inner_lock);
            }
        }
    }

    /// specs/011: an open stalled after it found no maintenance, and before it looks for the
    /// directory, is refused by the replacement lock at every stage of another process's closed
    /// compaction. That includes the stages at which the compaction has moved the directory
    /// aside: finding no directory there, the open asks again whether maintenance is under way,
    /// rather than going on with no lock at all.
    #[test]
    fn an_open_stalled_before_it_looks_for_the_directory_is_refused_by_the_claim() {
        use crate::maintenance_coordination::lock_seams::StallPoint;
        use crate::test_support::fault_checkpoint::{
            MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
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
                phase: MaintenancePhase::PreviousPublished,
                cut: MaintenanceCut::ManifestPublish,
            },
            MaintenanceFaultPoint {
                phase: MaintenancePhase::ReplacementPublished,
                cut: MaintenanceCut::ReopenValidation,
            },
        ] {
            let open = stalled_open_across_a_compaction(point, StallPoint::AfterMaintenanceCheck);
            assert_refused_and_harmless(
                point,
                open,
                std::io::ErrorKind::WouldBlock,
                ".store.pigment-lock",
            );
        }
    }

    /// specs/011, FR-9: only a regular file of that name is skipped. A directory named like the
    /// lock file, appearing in the source after the claim retired its lock, fails the compaction
    /// closed before it publishes anything.
    #[test]
    fn a_directory_named_like_the_lock_file_fails_the_compaction_closed() {
        use crate::test_support::fault_checkpoint::{
            pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
        };
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let pause_dir = tempfile::tempdir().unwrap();
        let child = pause_maintenance_child(
            "compaction::recovery_tests::closed_compaction_checkpoint_child",
            &store_dir,
            pause_dir.path(),
            MaintenanceFaultPoint {
                phase: MaintenancePhase::Prepared,
                cut: MaintenanceCut::StagingValidate,
            },
        );
        let foreign = store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME);
        std::fs::create_dir(&foreign).unwrap();

        let exit = child.resume();

        // Its staging stays as evidence, as it does for any source that changed after capture.
        assert_eq!(
            exit,
            crate::test_support::fault_checkpoint::MAINTENANCE_CHILD_FAILED,
            "the compaction must fail closed"
        );
        let paths = crate::compaction::publication::directory_artifact_paths(&store_dir).unwrap();
        assert!(!paths.manifest.exists(), "nothing may be published");
        assert!(
            !paths.previous.exists(),
            "the source must not be moved aside"
        );
        assert!(store_dir.join("kv.wal.dat").is_file());
        assert!(foreign.is_dir());
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
            Err(std::fs::TryLockError::Error(error)) => {
                panic!("cannot probe the inner lock: {error}")
            }
        }
    }

    /// Leaves `store_dir` as a closed compaction killed after moving the directory aside, so the next
    /// open or claim must recover before it can do anything else.
    fn interrupted_compaction(store_dir: &std::path::Path) {
        use crate::test_support::fault_checkpoint::{
            run_maintenance_checkpoint_child_with_evidence_root, MaintenanceCut,
            MaintenanceFaultPoint, MaintenancePhase,
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

    /// specs/011, FR-5 step 3: an open of any family that recovered interrupted maintenance holds
    /// the inner lock of the directory recovery left in place. Only the inner lock is shared by
    /// every mount view of the directory; the replacement lock the open also holds lives in its
    /// own view's parent.
    #[test]
    fn an_open_that_recovered_holds_the_inner_lock() {
        for family in [
            FixtureFamily::KeyValue,
            FixtureFamily::KeySet,
            FixtureFamily::KeyMap,
        ] {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            interrupted_compaction(&store_dir);

            let held = open_family(&store_dir, family);

            assert_eq!(held.status, crate::RecoveryStatus::Recovered, "{family:?}");
            assert!(inner_lock_is_held(&store_dir), "{family:?}");
            #[cfg(unix)]
            assert_eq!(
                std::fs::read_to_string(
                    store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME)
                )
                .unwrap(),
                format!("{}\n", std::process::id()),
                "{family:?}"
            );
            drop(held);
            assert!(!inner_lock_is_held(&store_dir), "{family:?}");
        }
    }

    /// An open store of any family, kept only to hold the directory.
    struct OpenFamily {
        status: crate::RecoveryStatus,
        _store: Box<dyn std::any::Any + Send>,
    }

    fn open_family(store_dir: &std::path::Path, family: FixtureFamily) -> OpenFamily {
        let (status, store): (_, Box<dyn std::any::Any + Send>) = match family {
            FixtureFamily::KeyValue => {
                let outcome =
                    crate::key_value_store::DurableKeyValueStore::try_init_new(store_dir).unwrap();
                (outcome.status(), Box::new(outcome.into_store()))
            }
            FixtureFamily::KeySet => {
                let outcome =
                    crate::key_set_store::DurableKeySetStore::try_init_new(store_dir).unwrap();
                (outcome.status(), Box::new(outcome.into_store()))
            }
            FixtureFamily::KeyMap => {
                let outcome =
                    crate::key_map_store::DurableKeyMapStore::try_init_new(store_dir).unwrap();
                (outcome.status(), Box::new(outcome.into_store()))
            }
        };
        OpenFamily {
            status,
            _store: store,
        }
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
        let owner = std::fs::read_to_string(
            store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME),
        )
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

    /// Opens `store_dir`'s key/value family on a thread, after an interrupted compaction, and
    /// returns once that open is stalled taking the inner lock after its recovery.
    fn open_stalled_in_its_post_recovery_lock(
        store_dir: &std::path::Path,
    ) -> (
        crate::maintenance_coordination::lock_seams::Stall,
        std::thread::JoinHandle<Result<(), crate::RecoveryError>>,
    ) {
        use crate::maintenance_coordination::lock_seams::Stall;
        interrupted_compaction(store_dir);
        let manifest = crate::compaction::publication::directory_artifact_paths(store_dir)
            .unwrap()
            .manifest;
        let stall = Stall::install(store_dir);
        let first = {
            let store_dir = store_dir.to_path_buf();
            std::thread::spawn(move || {
                crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir).map(|_| ())
            })
        };
        stall.wait_entered();
        assert!(
            !manifest.exists(),
            "the stall must hold the inner lock taken after recovery"
        );
        (stall, first)
    }

    type SecondFamily = Result<crate::key_set_store::DurableKeySetStore<std::fs::File>, String>;

    /// Opens `store_dir`'s key/set family on a thread. The store, or the refusal's kind and text,
    /// arrives on the returned channel, so the test holds the store while it looks at the lock.
    fn open_second_family(store_dir: &std::path::Path) -> std::sync::mpsc::Receiver<SecondFamily> {
        let (done, finished) = std::sync::mpsc::channel();
        let store_dir = store_dir.to_path_buf();
        std::thread::spawn(move || {
            let opened = crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir)
                .map(|outcome| outcome.into_store())
                .map_err(|error| match error {
                    crate::RecoveryError::Io { source, .. } => {
                        format!("{:?}: {source}", source.kind())
                    }
                    other => format!("{other:?}"),
                });
            let _ = done.send(opened);
        });
        finished
    }

    /// The second family's result: whatever arrived before the first open was released, or, if
    /// nothing had, whatever arrives after.
    fn second_family_result(
        early: Result<SecondFamily, std::sync::mpsc::RecvTimeoutError>,
        second: &std::sync::mpsc::Receiver<SecondFamily>,
    ) -> SecondFamily {
        early.unwrap_or_else(|_| {
            second
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("the second open must finish once the first has")
        })
    }

    /// Whether a fresh descriptor is refused `store_dir`'s inner lock; `false` if there is no
    /// such file.
    fn inner_lock_file_is_held(store_dir: &std::path::Path) -> bool {
        store_dir
            .join(crate::maintenance_coordination::INNER_LOCK_NAME)
            .is_file()
            && inner_lock_is_held(store_dir)
    }

    /// specs/011, FR-5 step 3: a second family opened in the same process while the first is
    /// still taking the inner lock after recovery does not go live without it. It waits for that
    /// attempt, and when the attempt fails because another holder has the lock, it is refused
    /// too.
    #[test]
    fn a_second_family_does_not_go_live_while_the_first_is_still_taking_the_inner_lock() {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        let (stall, first) = open_stalled_in_its_post_recovery_lock(&store_dir);
        // Another holder of the inner lock: a descriptor of its own refuses this process's as
        // another process's would.
        let holder = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME))
            .unwrap();
        holder.try_lock().unwrap();

        let second = open_second_family(&store_dir);
        let early = second.recv_timeout(std::time::Duration::from_millis(500));
        stall.release();
        let first = first.join().unwrap();
        let second = second_family_result(early, &second);
        drop(holder);

        match first {
            Err(crate::RecoveryError::Io { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::WouldBlock, "first");
            }
            other => panic!(
                "the first family must be refused, got {:?}",
                other.map(|_| ())
            ),
        }
        match second {
            Err(refusal) => assert!(refusal.starts_with("WouldBlock"), "second: {refusal}"),
            Ok(_) => panic!("the second family went live while another holder had the inner lock"),
        }
    }

    /// A panic while the first open takes the inner lock after recovery does not leave the
    /// directory's entry marked as taking it: the second family, waiting on that attempt, takes
    /// the lock itself.
    #[test]
    fn a_panic_while_taking_the_inner_lock_after_recovery_leaves_it_to_the_next_open() {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        let (stall, first) = open_stalled_in_its_post_recovery_lock(&store_dir);
        crate::maintenance_coordination::lock_seams::inject_panic(&store_dir);

        let second = open_second_family(&store_dir);
        let early = second.recv_timeout(std::time::Duration::from_millis(500));
        stall.release();
        let first = first.join();
        let second = second_family_result(early, &second);
        let held = inner_lock_file_is_held(&store_dir);

        assert!(first.is_err(), "the injected panic must have fired");
        assert!(second.is_ok(), "{:?}", second.as_ref().err());
        assert!(held, "the open second family must hold the inner lock");
    }

    /// Ways to name a store directory through symbolic links, each made under `root` for `store`:
    /// a plain alias, the same alias written with a trailing separator or `.`, and a chain whose
    /// middle link's target itself ends in a separator.
    #[cfg(unix)]
    fn alias_forms(
        root: &std::path::Path,
        store: &std::path::Path,
    ) -> Vec<(&'static str, std::path::PathBuf)> {
        std::os::unix::fs::symlink(store, root.join("alias")).unwrap();
        std::os::unix::fs::symlink(store, root.join("mid")).unwrap();
        std::os::unix::fs::symlink("mid/", root.join("outer")).unwrap();
        vec![
            ("alias", root.join("alias")),
            ("alias/", root.join("alias/")),
            ("alias/.", root.join("alias/.")),
            ("outer -> mid/ -> store", root.join("outer")),
        ]
    }

    /// specs/011, FR-2: a symlink alias names the directory it points to, however the path to it
    /// is written, including while another process's closed compaction has moved that directory
    /// aside. The alias open is then refused by that claim's replacement lock, as an open of the
    /// real path is.
    #[cfg(unix)]
    #[test]
    fn an_alias_opened_while_the_directory_is_moved_aside_is_refused_by_the_claim() {
        use crate::test_support::fault_checkpoint::{
            pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
            MAINTENANCE_PAUSED_CHILD_COMPLETED,
        };
        let probe = tempfile::tempdir().unwrap();
        let forms = alias_forms(probe.path(), &probe.path().join("store")).len();
        for form in 0..forms {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
            let pause_dir = tempfile::tempdir().unwrap();
            let child = pause_maintenance_child(
                "compaction::recovery_tests::closed_compaction_checkpoint_child",
                &store_dir,
                pause_dir.path(),
                MaintenanceFaultPoint {
                    phase: MaintenancePhase::PreviousPublished,
                    cut: MaintenanceCut::PreviousPublish,
                },
            );
            assert!(!store_dir.exists(), "the directory must be moved aside");
            let (name, alias) = alias_forms(root.path(), &store_dir).swap_remove(form);

            let refused = crate::key_value_store::DurableKeyValueStore::try_init_new(&alias);
            let exit = child.resume();

            match refused {
                Err(crate::RecoveryError::Io { source, .. }) => {
                    assert_eq!(
                        source.kind(),
                        std::io::ErrorKind::WouldBlock,
                        "{name}: {source}"
                    );
                    assert!(
                        source.to_string().contains(".store.pigment-lock"),
                        "{name}: the claim's replacement lock must refuse: {source}"
                    );
                }
                other => panic!(
                    "{name}: expected a WouldBlock refusal, got {:?}",
                    other.map(|_| ())
                ),
            }
            assert_eq!(exit, MAINTENANCE_PAUSED_CHILD_COMPLETED, "{name}");
        }
    }

    /// specs/011, FR-5: an open through a symlink alias, however the path to it is written,
    /// recovers the directory's interrupted maintenance, as an open of the real path does.
    /// Recovering against the alias's own name found nothing, so the open went live over a
    /// directory still to be recovered, and a later open of the real path rolled back what it had
    /// written.
    #[cfg(unix)]
    #[test]
    fn an_alias_open_recovers_the_directory_it_names() {
        use crate::test_support::fault_checkpoint::{
            run_maintenance_checkpoint_child_with_evidence_root, MaintenanceCut,
            MaintenanceFaultPoint, MaintenancePhase,
        };
        let probe = tempfile::tempdir().unwrap();
        let forms = alias_forms(probe.path(), &probe.path().join("store")).len();
        for form in 0..forms {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
            run_maintenance_checkpoint_child_with_evidence_root(
                "compaction::recovery_tests::closed_compaction_checkpoint_child",
                &store_dir,
                root.path(),
                MaintenanceFaultPoint {
                    phase: MaintenancePhase::ReplacementPublished,
                    cut: MaintenanceCut::ReplacementPublish,
                },
            );
            let manifest = crate::compaction::publication::directory_artifact_paths(&store_dir)
                .unwrap()
                .manifest;
            assert!(manifest.is_file(), "the compaction must be left to recover");
            // Made after the child, whose evidence snapshot takes only files and directories.
            let (name, alias) = alias_forms(root.path(), &store_dir).swap_remove(form);

            let outcome = crate::key_value_store::DurableKeyValueStore::try_init_new(&alias)
                .unwrap_or_else(|error| panic!("{name}: {error:?}"));

            assert_eq!(outcome.status(), crate::RecoveryStatus::Recovered, "{name}");
            assert!(
                !manifest.exists(),
                "{name}: the alias open must finish the recovery"
            );
            let store = outcome.into_store();
            store.put(b"late".to_vec(), b"kept".to_vec());
            drop(store);
            let reopened = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
                .unwrap()
                .into_store();
            assert_eq!(reopened.get(b"late"), Some(b"kept".to_vec()), "{name}");
            assert_eq!(reopened.get(b"alpha"), Some(b"one".to_vec()), "{name}");
        }
    }

    /// Leaves `store_dir` replaced under an open of it: the open, stalled between taking the inner
    /// lock and checking the replacement lock, holds the lock file of a directory another
    /// process's closed compaction has since retired. `after_compaction` runs once the compaction
    /// has finished and before the open is released. Returns the open, still running.
    #[cfg(unix)]
    fn open_across_a_replacement(
        root: &std::path::Path,
        store_dir: &std::path::Path,
        after_compaction: impl FnOnce(),
    ) -> std::thread::JoinHandle<
        Result<crate::key_value_store::DurableKeyValueStore<std::fs::File>, crate::RecoveryError>,
    > {
        use crate::maintenance_coordination::lock_seams::{Stall, StallPoint};
        use crate::test_support::fault_checkpoint::{
            pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
            MAINTENANCE_PAUSED_CHILD_COMPLETED,
        };
        let before_the_directory = Stall::install_at(store_dir, StallPoint::AfterMaintenanceCheck);
        let opener = {
            let store_dir = store_dir.to_path_buf();
            std::thread::spawn(move || {
                crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
                    .map(|outcome| outcome.into_store())
            })
        };
        before_the_directory.wait_entered();
        let pause_dir = tempfile::tempdir().unwrap();
        let child = pause_maintenance_child(
            "compaction::recovery_tests::closed_compaction_checkpoint_child",
            store_dir,
            pause_dir.path(),
            MaintenanceFaultPoint {
                phase: MaintenancePhase::Prepared,
                cut: MaintenanceCut::StagingValidate,
            },
        );
        // The replacement lock lives in the parent: stall its open, after the inner lock is taken.
        let before_the_replacement_lock = Stall::install(root);
        before_the_directory.release();
        before_the_replacement_lock.wait_entered();
        assert!(
            inner_lock_file_is_held(store_dir),
            "the open must hold the retired directory's lock file by now"
        );
        assert_eq!(child.resume(), MAINTENANCE_PAUSED_CHILD_COMPLETED);
        after_compaction();
        before_the_replacement_lock.release();
        opener
    }

    /// specs/011, FR-2: an open stalled between taking the inner lock and checking the replacement
    /// lock, while another process's compaction replaces the directory, ends up holding the lock
    /// file of the directory that was retired. It must not go live on that: before it does, it
    /// takes the lock of the directory now in place. That holds whether the lock path is empty,
    /// or holds another file, such as one a refused open left (FR-4).
    #[cfg(unix)]
    #[test]
    fn an_open_whose_inner_lock_was_retired_under_it_takes_the_current_one() {
        for leave_a_file in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let store_dir = root.path().join("store");
            std::fs::create_dir(&store_dir).unwrap();
            create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
            let lock_path = store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME);
            let opener = open_across_a_replacement(root.path(), &store_dir, || {
                if leave_a_file {
                    std::fs::write(&lock_path, b"").unwrap();
                }
            });

            let opened = opener.join().unwrap();
            let held = inner_lock_file_is_held(&store_dir);

            let store = opened.unwrap_or_else(|error| panic!("the open must succeed: {error:?}"));
            assert!(
                held,
                "the open went live without the lock of the directory in place (a file left at \
                 the lock path: {leave_a_file})"
            );
            drop(store);
        }
    }

    /// specs/011, FR-2: when two families of one process both find their shared inner lock
    /// stale, the one that looks second does not release the lock the first has just re-taken.
    /// Holdings are compared, not inodes, since the retired file's inode can be given to the new
    /// one.
    #[cfg(unix)]
    #[test]
    fn a_second_stale_verdict_keeps_the_lock_the_first_re_took() {
        use crate::maintenance_coordination::lock_seams::{Stall, StallPoint};
        use std::time::{Duration, Instant};

        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        // The first family to compare its held lock parks there; the other goes on and re-takes.
        let first_check = Stall::install_once_at(&store_dir, StallPoint::HeldLockCheck);
        let (set_done, set_finished) = std::sync::mpsc::channel();
        let kv = open_across_a_replacement(root.path(), &store_dir, || {
            // Started while the key/value entry is still being created, so both share it.
            let store_dir = store_dir.clone();
            std::thread::spawn(move || {
                let opened = crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir)
                    .map(|outcome| outcome.into_store());
                let _ = set_done.send(opened);
            });
        });
        first_check.wait_entered();
        let started = Instant::now();
        let mut set = None;
        while !kv.is_finished() && set.is_none() {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the family that did not park must finish"
            );
            set = set_finished.try_recv().ok();
            std::thread::sleep(Duration::from_millis(5));
        }
        // Now the parked family goes on. Were it to release the new lock, its re-take would stop
        // here, with no one holding the directory's lock.
        let retake = Stall::install(&store_dir);
        first_check.release();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(kv.is_finished() && set.is_some()) && Instant::now() < deadline {
            if set.is_none() {
                set = set_finished.try_recv().ok();
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let finished = kv.is_finished() && set.is_some();
        let held = inner_lock_file_is_held(&store_dir);
        retake.release();
        let kv = kv.join().unwrap();
        let set = set.unwrap_or_else(|| set_finished.recv().unwrap());

        assert!(
            finished && held,
            "the second family released the lock the first had re-taken (both finished: \
             {finished}, lock held: {held})"
        );
        assert!(kv.is_ok() && set.is_ok());
    }

    /// specs/011, FR-2: a family opened later in the same process, on an entry whose inner lock
    /// file was replaced at the lock path, holds the lock of the file now there.
    #[cfg(unix)]
    #[test]
    fn a_later_family_takes_the_current_lock_when_the_held_one_was_replaced() {
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        let first = crate::key_value_store::DurableKeyValueStore::try_init_new(&store_dir)
            .unwrap()
            .into_store();
        let lock_path = store_dir.join(crate::maintenance_coordination::INNER_LOCK_NAME);
        std::fs::remove_file(&lock_path).unwrap();
        std::fs::write(&lock_path, b"").unwrap();
        assert!(!inner_lock_file_is_held(&store_dir));

        let later = crate::key_set_store::DurableKeySetStore::try_init_new(&store_dir)
            .unwrap()
            .into_store();

        assert!(
            inner_lock_file_is_held(&store_dir),
            "the later family went live on the replaced lock"
        );
        drop((first, later));
    }

    /// specs/011, FR-2: a closed-maintenance claim whose inner lock was retired under it, by
    /// another process's compaction while it stalled between its two locks, holds the lock of the
    /// directory now in place once it has made sure of it, and retires that one.
    #[cfg(unix)]
    #[test]
    fn a_claim_whose_inner_lock_was_retired_under_it_takes_the_current_one() {
        use crate::maintenance_coordination::lock_seams::Stall;
        use crate::test_support::fault_checkpoint::{
            pause_maintenance_child, MaintenanceCut, MaintenanceFaultPoint, MaintenancePhase,
            MAINTENANCE_PAUSED_CHILD_COMPLETED,
        };
        let root = tempfile::tempdir().unwrap();
        let store_dir = root.path().join("store");
        std::fs::create_dir(&store_dir).unwrap();
        create_segmented_v2(&store_dir, FixtureFamily::KeyValue);
        let before_the_inner_lock = Stall::install(&store_dir);
        let claimant = {
            let store_dir = store_dir.clone();
            std::thread::spawn(move || {
                crate::maintenance_coordination::try_claim_closed(&store_dir)
            })
        };
        before_the_inner_lock.wait_entered();
        let pause_dir = tempfile::tempdir().unwrap();
        let child = pause_maintenance_child(
            "compaction::recovery_tests::closed_compaction_checkpoint_child",
            &store_dir,
            pause_dir.path(),
            MaintenanceFaultPoint {
                phase: MaintenancePhase::Prepared,
                cut: MaintenanceCut::StagingValidate,
            },
        );
        let before_the_replacement_lock = Stall::install(root.path());
        before_the_inner_lock.release();
        before_the_replacement_lock.wait_entered();
        assert_eq!(child.resume(), MAINTENANCE_PAUSED_CHILD_COMPLETED);
        before_the_replacement_lock.release();
        let claim = claimant.join().unwrap().expect("the claim must be granted");

        claim.ensure_inner_lock().unwrap();
        let held = inner_lock_file_is_held(&store_dir);
        claim.retire_inner_lock().unwrap();

        assert!(
            held,
            "the claim went on without the lock of the directory in place"
        );
        assert!(!store_dir
            .join(crate::maintenance_coordination::INNER_LOCK_NAME)
            .exists());
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
