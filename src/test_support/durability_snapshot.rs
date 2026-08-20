//! Test-only volatile/durable filesystem namespace model.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NamespaceOperation {
    Write,
    Rename,
    WriteThroughMove,
    Remove,
    ExactCleanup,
    FileBarrier,
    DirectoryBarrier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NamespaceFault {
    pub(crate) operation: NamespaceOperation,
    pub(crate) call: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NamespaceEvent {
    pub(crate) operation: NamespaceOperation,
    pub(crate) path: PathBuf,
    pub(crate) destination: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DurableNamespaceImage {
    pub(crate) files: BTreeMap<PathBuf, Vec<u8>>,
}

#[derive(Debug, Default)]
pub(crate) struct DurabilitySnapshot {
    volatile: BTreeMap<PathBuf, Vec<u8>>,
    durable: BTreeMap<PathBuf, Vec<u8>>,
    synchronized_contents: BTreeMap<PathBuf, Vec<u8>>,
    fault: Option<NamespaceFault>,
    calls: BTreeMap<NamespaceOperationKey, usize>,
    events: Vec<NamespaceEvent>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NamespaceOperationKey {
    Write,
    Rename,
    WriteThroughMove,
    Remove,
    ExactCleanup,
    FileBarrier,
    DirectoryBarrier,
}

impl From<NamespaceOperation> for NamespaceOperationKey {
    fn from(operation: NamespaceOperation) -> Self {
        match operation {
            NamespaceOperation::Write => Self::Write,
            NamespaceOperation::Rename => Self::Rename,
            NamespaceOperation::WriteThroughMove => Self::WriteThroughMove,
            NamespaceOperation::Remove => Self::Remove,
            NamespaceOperation::ExactCleanup => Self::ExactCleanup,
            NamespaceOperation::FileBarrier => Self::FileBarrier,
            NamespaceOperation::DirectoryBarrier => Self::DirectoryBarrier,
        }
    }
}

impl DurabilitySnapshot {
    pub(crate) fn new(fault: Option<NamespaceFault>) -> Self {
        Self {
            fault,
            ..Self::default()
        }
    }

    pub(crate) fn write(&mut self, path: impl Into<PathBuf>, bytes: &[u8]) -> io::Result<()> {
        let path = path.into();
        self.record(NamespaceOperation::Write, path.clone(), None)?;
        self.volatile.insert(path.clone(), bytes.to_vec());
        self.synchronized_contents.remove(&path);
        Ok(())
    }

    pub(crate) fn rename(
        &mut self,
        source: impl Into<PathBuf>,
        destination: impl Into<PathBuf>,
    ) -> io::Result<()> {
        self.move_namespace(
            source.into(),
            destination.into(),
            NamespaceOperation::Rename,
            false,
        )
    }

    pub(crate) fn rename_replace(
        &mut self,
        source: impl Into<PathBuf>,
        destination: impl Into<PathBuf>,
    ) -> io::Result<()> {
        self.move_namespace(
            source.into(),
            destination.into(),
            NamespaceOperation::Rename,
            true,
        )
    }

    pub(crate) fn write_through_move(
        &mut self,
        source: impl Into<PathBuf>,
        destination: impl Into<PathBuf>,
    ) -> io::Result<()> {
        self.move_namespace(
            source.into(),
            destination.into(),
            NamespaceOperation::WriteThroughMove,
            false,
        )
    }

    pub(crate) fn write_through_replace(
        &mut self,
        source: impl Into<PathBuf>,
        destination: impl Into<PathBuf>,
    ) -> io::Result<()> {
        self.move_namespace(
            source.into(),
            destination.into(),
            NamespaceOperation::WriteThroughMove,
            true,
        )
    }

    pub(crate) fn remove(&mut self, path: impl Into<PathBuf>) -> io::Result<()> {
        let path = path.into();
        self.record(NamespaceOperation::Remove, path.clone(), None)?;
        self.volatile.remove(&path);
        self.synchronized_contents.remove(&path);
        Ok(())
    }

    pub(crate) fn remove_exact(
        &mut self,
        path: impl Into<PathBuf>,
        expected: &DurableNamespaceImage,
    ) -> io::Result<()> {
        let path = path.into();
        self.record(NamespaceOperation::ExactCleanup, path.clone(), None)?;
        let actual = self.subtree(&path, &self.volatile);
        if actual != expected.files {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact cleanup evidence does not match the current namespace",
            ));
        }
        self.volatile.retain(|entry, _| !is_within(entry, &path));
        self.synchronized_contents
            .retain(|entry, _| !is_within(entry, &path));
        Ok(())
    }

    pub(crate) fn image_below(&self, path: impl AsRef<Path>) -> DurableNamespaceImage {
        DurableNamespaceImage {
            files: self.subtree(path.as_ref(), &self.volatile),
        }
    }

    pub(crate) fn sync_file(&mut self, path: impl Into<PathBuf>) -> io::Result<()> {
        let path = path.into();
        self.record(NamespaceOperation::FileBarrier, path.clone(), None)?;
        let bytes = self.volatile.get(&path).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "snapshot file barrier path is absent",
            )
        })?;
        self.synchronized_contents
            .insert(path.clone(), bytes.clone());
        if self.durable.contains_key(&path) {
            self.durable.insert(path, bytes);
        }
        Ok(())
    }

    pub(crate) fn sync_directory(&mut self, path: impl Into<PathBuf>) -> io::Result<()> {
        let path = path.into();
        self.record(NamespaceOperation::DirectoryBarrier, path, None)?;
        let previous = self.durable.clone();
        self.durable = self
            .volatile
            .keys()
            .map(|path| {
                let bytes = self
                    .synchronized_contents
                    .get(path)
                    .or_else(|| previous.get(path))
                    .cloned()
                    .unwrap_or_default();
                (path.clone(), bytes)
            })
            .collect();
        Ok(())
    }

    pub(crate) fn volatile_image(&self) -> DurableNamespaceImage {
        DurableNamespaceImage {
            files: self.volatile.clone(),
        }
    }

    pub(crate) fn durable_image(&self) -> DurableNamespaceImage {
        DurableNamespaceImage {
            files: self.durable.clone(),
        }
    }

    pub(crate) fn simulate_power_loss(&mut self) -> DurableNamespaceImage {
        self.volatile = self.durable.clone();
        self.synchronized_contents = self.durable.clone();
        self.durable_image()
    }

    pub(crate) fn calls(&self, operation: NamespaceOperation) -> usize {
        self.calls
            .get(&NamespaceOperationKey::from(operation))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn events(&self) -> &[NamespaceEvent] {
        &self.events
    }

    fn move_namespace(
        &mut self,
        source: PathBuf,
        destination: PathBuf,
        operation: NamespaceOperation,
        replace: bool,
    ) -> io::Result<()> {
        self.record(operation, source.clone(), Some(destination.clone()))?;
        let moved: Vec<_> = self
            .volatile
            .iter()
            .filter(|(path, _)| is_within(path, &source))
            .map(|(path, bytes)| {
                let relative = path.strip_prefix(&source).expect("matched source prefix");
                (path.clone(), destination.join(relative), bytes.clone())
            })
            .collect();
        if moved.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "snapshot namespace move source is absent",
            ));
        }
        let destination_exists = self
            .volatile
            .keys()
            .any(|path| is_within(path, &destination));
        if destination_exists && !replace {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "snapshot namespace move destination exists",
            ));
        }
        if operation == NamespaceOperation::WriteThroughMove
            && moved
                .iter()
                .any(|(old, _, bytes)| self.synchronized_contents.get(old) != Some(bytes))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "write-through move requires synchronized source contents",
            ));
        }

        if replace {
            self.volatile
                .retain(|path, _| !is_within(path, &destination));
            self.synchronized_contents
                .retain(|path, _| !is_within(path, &destination));
            if operation == NamespaceOperation::WriteThroughMove {
                self.durable
                    .retain(|path, _| !is_within(path, &destination));
            }
        }
        for (old, new, bytes) in &moved {
            self.volatile.remove(old);
            self.volatile.insert(new.clone(), bytes.clone());
            if let Some(synchronized) = self.synchronized_contents.remove(old) {
                self.synchronized_contents.insert(new.clone(), synchronized);
            }
        }
        if operation == NamespaceOperation::WriteThroughMove {
            self.durable.retain(|path, _| !is_within(path, &source));
            for (_, new, bytes) in moved {
                self.durable.insert(new, bytes);
            }
        }
        Ok(())
    }

    fn subtree(
        &self,
        root: &Path,
        image: &BTreeMap<PathBuf, Vec<u8>>,
    ) -> BTreeMap<PathBuf, Vec<u8>> {
        image
            .iter()
            .filter(|(path, _)| is_within(path, root))
            .map(|(path, bytes)| (path.clone(), bytes.clone()))
            .collect()
    }

    fn record(
        &mut self,
        operation: NamespaceOperation,
        path: PathBuf,
        destination: Option<PathBuf>,
    ) -> io::Result<()> {
        let call = self
            .calls
            .entry(NamespaceOperationKey::from(operation))
            .or_default();
        *call += 1;
        self.events.push(NamespaceEvent {
            operation,
            path,
            destination,
        });
        if self.fault
            == Some(NamespaceFault {
                operation,
                call: *call,
            })
        {
            return Err(io::Error::other("scripted namespace operation failed"));
        }
        Ok(())
    }
}

fn is_within(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

pub(crate) fn restore_image(root: &Path, image: &DurableNamespaceImage) -> io::Result<()> {
    for (path, bytes) in &image.files {
        let relative = path.strip_prefix(root).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot path lies outside restore root",
            )
        })?;
        let destination = root.join(relative);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(destination, bytes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_and_directory_barriers_define_the_durable_image() {
        let root = PathBuf::from("/model");
        let staging = root.join("staging");
        let active = root.join("active");
        let mut snapshot = DurabilitySnapshot::new(None);

        snapshot.write(&staging, b"candidate").unwrap();
        snapshot.sync_file(&staging).unwrap();
        assert!(snapshot.durable_image().files.is_empty());
        snapshot.rename(&staging, &active).unwrap();
        assert!(snapshot.durable_image().files.is_empty());
        snapshot.sync_directory(&root).unwrap();
        assert_eq!(
            snapshot.durable_image().files.get(&active),
            Some(&b"candidate".to_vec())
        );
    }

    #[test]
    fn rename_remove_and_crash_restore_only_directory_barrier_state() {
        let root = PathBuf::from("/model");
        let active = root.join("active");
        let recovery = root.join("recovery");
        let mut snapshot = DurabilitySnapshot::new(None);
        snapshot.write(&active, b"old").unwrap();
        snapshot.sync_file(&active).unwrap();
        snapshot.sync_directory(&root).unwrap();
        snapshot.rename(&active, &recovery).unwrap();
        assert!(snapshot.volatile_image().files.contains_key(&recovery));
        assert!(snapshot.durable_image().files.contains_key(&active));
        snapshot.simulate_power_loss();
        assert!(snapshot.volatile_image().files.contains_key(&active));
        assert!(!snapshot.volatile_image().files.contains_key(&recovery));

        snapshot.rename(&active, &recovery).unwrap();
        snapshot.sync_directory(&root).unwrap();
        snapshot.remove(&recovery).unwrap();
        snapshot.simulate_power_loss();
        assert!(snapshot.volatile_image().files.contains_key(&recovery));
    }

    #[test]
    fn every_namespace_operation_can_fail_at_an_exact_call() {
        for operation in [
            NamespaceOperation::Write,
            NamespaceOperation::Rename,
            NamespaceOperation::WriteThroughMove,
            NamespaceOperation::Remove,
            NamespaceOperation::ExactCleanup,
            NamespaceOperation::FileBarrier,
            NamespaceOperation::DirectoryBarrier,
        ] {
            let mut snapshot = DurabilitySnapshot::new(Some(NamespaceFault { operation, call: 1 }));
            let root = PathBuf::from("/model");
            let source = root.join("source");
            let destination = root.join("destination");
            if operation != NamespaceOperation::Write {
                snapshot.volatile.insert(source.clone(), b"bytes".to_vec());
            }
            let result = match operation {
                NamespaceOperation::Write => snapshot.write(&source, b"bytes"),
                NamespaceOperation::Rename => snapshot.rename(&source, &destination),
                NamespaceOperation::WriteThroughMove => {
                    snapshot.write_through_move(&source, &destination)
                }
                NamespaceOperation::Remove => snapshot.remove(&source),
                NamespaceOperation::ExactCleanup => {
                    let expected = snapshot.image_below(&source);
                    snapshot.remove_exact(&source, &expected)
                }
                NamespaceOperation::FileBarrier => snapshot.sync_file(&source),
                NamespaceOperation::DirectoryBarrier => snapshot.sync_directory(&root),
            };
            assert!(result.is_err(), "{operation:?} must fail");
            assert_eq!(snapshot.calls(operation), 1);
            assert_eq!(snapshot.events().len(), 1);
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ModeledCompactionPhase {
        Prepared,
        PreviousPublished,
        ReplacementPublished,
        CleanupPending,
    }

    struct CompactionPaths {
        parent: PathBuf,
        source: PathBuf,
        staging: PathBuf,
        previous: PathBuf,
        manifest: PathBuf,
        manifest_next: PathBuf,
    }

    fn modeled_paths() -> CompactionPaths {
        let parent = PathBuf::from("/model");
        CompactionPaths {
            source: parent.join("store"),
            staging: parent.join("store.compaction-staging"),
            previous: parent.join("store.compaction-previous"),
            manifest: parent.join("compaction.manifest"),
            manifest_next: parent.join("compaction.manifest.next"),
            parent,
        }
    }

    fn persist_manifest(
        snapshot: &mut DurabilitySnapshot,
        paths: &CompactionPaths,
        phase: ModeledCompactionPhase,
    ) {
        snapshot
            .write(&paths.manifest_next, format!("{phase:?}").as_bytes())
            .unwrap();
        snapshot.sync_file(&paths.manifest_next).unwrap();
        snapshot
            .rename_replace(&paths.manifest_next, &paths.manifest)
            .unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();
    }

    fn crash_image_at(phase: ModeledCompactionPhase) -> DurableNamespaceImage {
        let paths = modeled_paths();
        let mut snapshot = DurabilitySnapshot::new(None);
        let source_file = paths.source.join("active.wal");
        let staging_file = paths.staging.join("active.wal");
        snapshot.write(&source_file, b"old-complete").unwrap();
        snapshot.sync_file(&source_file).unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();

        snapshot.write(&staging_file, b"new-complete").unwrap();
        snapshot.sync_file(&staging_file).unwrap();
        persist_manifest(&mut snapshot, &paths, ModeledCompactionPhase::Prepared);
        if phase == ModeledCompactionPhase::Prepared {
            return snapshot.simulate_power_loss();
        }

        snapshot.rename(&paths.source, &paths.previous).unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();
        persist_manifest(
            &mut snapshot,
            &paths,
            ModeledCompactionPhase::PreviousPublished,
        );
        if phase == ModeledCompactionPhase::PreviousPublished {
            return snapshot.simulate_power_loss();
        }

        snapshot.rename(&paths.staging, &paths.source).unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();
        persist_manifest(
            &mut snapshot,
            &paths,
            ModeledCompactionPhase::ReplacementPublished,
        );
        if phase == ModeledCompactionPhase::ReplacementPublished {
            return snapshot.simulate_power_loss();
        }

        persist_manifest(
            &mut snapshot,
            &paths,
            ModeledCompactionPhase::CleanupPending,
        );
        snapshot.simulate_power_loss()
    }

    #[test]
    fn every_manifest_phase_power_loss_retains_a_complete_authority() {
        let paths = modeled_paths();
        for phase in [
            ModeledCompactionPhase::Prepared,
            ModeledCompactionPhase::PreviousPublished,
            ModeledCompactionPhase::ReplacementPublished,
            ModeledCompactionPhase::CleanupPending,
        ] {
            let image = crash_image_at(phase);
            let source = image.files.get(&paths.source.join("active.wal"));
            let previous = image.files.get(&paths.previous.join("active.wal"));
            let staging = image.files.get(&paths.staging.join("active.wal"));
            match phase {
                ModeledCompactionPhase::Prepared => {
                    assert_eq!(source, Some(&b"old-complete".to_vec()));
                    assert_eq!(staging, Some(&b"new-complete".to_vec()));
                    assert!(previous.is_none());
                }
                ModeledCompactionPhase::PreviousPublished => {
                    assert!(source.is_none());
                    assert_eq!(previous, Some(&b"old-complete".to_vec()));
                    assert_eq!(staging, Some(&b"new-complete".to_vec()));
                }
                ModeledCompactionPhase::ReplacementPublished
                | ModeledCompactionPhase::CleanupPending => {
                    assert_eq!(source, Some(&b"new-complete".to_vec()));
                    assert_eq!(previous, Some(&b"old-complete".to_vec()));
                    assert!(staging.is_none());
                }
            }
            assert_eq!(
                image.files.get(&paths.manifest),
                Some(&format!("{phase:?}").into_bytes())
            );
            assert!(!image.files.contains_key(&paths.manifest_next));
        }
    }

    #[test]
    fn write_through_family_and_directory_moves_survive_without_a_later_barrier() {
        let root = PathBuf::from("/model");
        let mut snapshot = DurabilitySnapshot::new(None);
        let family_staging = root.join("family.staging");
        let family_active = root.join("family.active");
        snapshot.write(&family_active, b"old").unwrap();
        snapshot.sync_file(&family_active).unwrap();
        snapshot.sync_directory(&root).unwrap();
        snapshot.write(&family_staging, b"new").unwrap();
        snapshot.sync_file(&family_staging).unwrap();
        snapshot
            .write_through_replace(&family_staging, &family_active)
            .unwrap();
        assert_eq!(
            snapshot.simulate_power_loss().files.get(&family_active),
            Some(&b"new".to_vec())
        );

        let directory_staging = root.join("directory.staging");
        let directory_active = root.join("directory.active");
        let staged_file = directory_staging.join("active.wal");
        snapshot.write(&staged_file, b"directory").unwrap();
        snapshot.sync_file(&staged_file).unwrap();
        snapshot
            .write_through_move(&directory_staging, &directory_active)
            .unwrap();
        assert_eq!(
            snapshot
                .simulate_power_loss()
                .files
                .get(&directory_active.join("active.wal")),
            Some(&b"directory".to_vec())
        );
    }

    #[test]
    fn exact_cleanup_rejects_changed_evidence_and_preserves_every_artifact() {
        let paths = modeled_paths();
        let mut snapshot = DurabilitySnapshot::new(None);
        let previous_file = paths.previous.join("active.wal");
        snapshot.write(&previous_file, b"old").unwrap();
        snapshot.sync_file(&previous_file).unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();
        let before = snapshot.volatile_image();
        let mut wrong = snapshot.image_below(&paths.previous);
        wrong
            .files
            .insert(previous_file.clone(), b"changed".to_vec());

        assert!(snapshot.remove_exact(&paths.previous, &wrong).is_err());
        assert_eq!(snapshot.volatile_image(), before);

        let exact = snapshot.image_below(&paths.previous);
        snapshot.remove_exact(&paths.previous, &exact).unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();
        assert!(!snapshot
            .simulate_power_loss()
            .files
            .contains_key(&previous_file));
    }

    #[test]
    fn corrupt_or_contradictory_evidence_is_preserved_across_power_loss() {
        let paths = modeled_paths();
        let mut snapshot = DurabilitySnapshot::new(None);
        let source = paths.source.join("active.wal");
        let previous = paths.previous.join("active.wal");
        snapshot.write(&source, b"source").unwrap();
        snapshot.sync_file(&source).unwrap();
        snapshot.write(&previous, b"competing").unwrap();
        snapshot.sync_file(&previous).unwrap();
        snapshot.write(&paths.manifest, b"corrupt").unwrap();
        snapshot.sync_file(&paths.manifest).unwrap();
        snapshot.sync_directory(&paths.parent).unwrap();

        let before = snapshot.durable_image();
        assert_eq!(snapshot.simulate_power_loss(), before);
        assert_eq!(before.files.get(&source), Some(&b"source".to_vec()));
        assert_eq!(before.files.get(&previous), Some(&b"competing".to_vec()));
        assert_eq!(
            before.files.get(&paths.manifest),
            Some(&b"corrupt".to_vec())
        );
    }
}
