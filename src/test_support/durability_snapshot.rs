//! Test-only volatile/durable filesystem namespace model.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NamespaceOperation {
    Write,
    Rename,
    Remove,
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
    Remove,
    FileBarrier,
    DirectoryBarrier,
}

impl From<NamespaceOperation> for NamespaceOperationKey {
    fn from(operation: NamespaceOperation) -> Self {
        match operation {
            NamespaceOperation::Write => Self::Write,
            NamespaceOperation::Rename => Self::Rename,
            NamespaceOperation::Remove => Self::Remove,
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
        let source = source.into();
        let destination = destination.into();
        self.record(
            NamespaceOperation::Rename,
            source.clone(),
            Some(destination.clone()),
        )?;
        let bytes = self.volatile.remove(&source).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "snapshot rename source is absent")
        })?;
        self.volatile.insert(destination.clone(), bytes);
        if let Some(bytes) = self.synchronized_contents.remove(&source) {
            self.synchronized_contents.insert(destination, bytes);
        }
        Ok(())
    }

    pub(crate) fn remove(&mut self, path: impl Into<PathBuf>) -> io::Result<()> {
        let path = path.into();
        self.record(NamespaceOperation::Remove, path.clone(), None)?;
        self.volatile.remove(&path);
        self.synchronized_contents.remove(&path);
        Ok(())
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
            NamespaceOperation::Remove,
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
                NamespaceOperation::Remove => snapshot.remove(&source),
                NamespaceOperation::FileBarrier => snapshot.sync_file(&source),
                NamespaceOperation::DirectoryBarrier => snapshot.sync_directory(&root),
            };
            assert!(result.is_err(), "{operation:?} must fail");
            assert_eq!(snapshot.calls(operation), 1);
            assert_eq!(snapshot.events().len(), 1);
        }
    }
}
