//! Private compaction-recovery behavior tests.

use crate::compaction::manifest::ManifestPhase;
use crate::compaction::publication::{
    cleanup_closed_with_checkpoint, publish_closed_prepared,
    publish_closed_previous_with_checkpoint, publish_closed_replacement_with_checkpoint,
    read_published_manifest, ClosedCleanupStage, ClosedPreviousStage, ClosedReplacementStage,
};
use crate::test_support::maintenance_fixtures::{
    create_segmented_v2, snapshot_directory, FixtureFamily,
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
