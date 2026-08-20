//! Private compaction-recovery behavior tests.

use crate::compaction::manifest::ManifestPhase;
use crate::compaction::publication::{
    publish_closed_prepared, publish_closed_previous_with_checkpoint, read_published_manifest,
    ClosedPreviousStage,
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
