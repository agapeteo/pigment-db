//! Private offline legacy-to-V1 migration engine.

#![allow(dead_code)]

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::model::SortedMapEntry;
use crate::wal::format::{RecordProbeFields, V1CodecProbe};
use crate::wal::model::{KeyValueData, MAP_PUT_ACT, SET_APPEND_ACT};
use crate::wal::replay::{
    replay_key_map, replay_key_set, replay_key_value, KeyMapSnapshot, KeySetSnapshot,
    KeyValueSnapshot, ValidationError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MigrationProbe;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MigrationCheckpoint {
    InitialSourceRead,
    FinalSourceRead,
    SourceChanged,
    Preflight,
    DestinationInspection,
    CreateDestination,
    Cleanup,
    WriteOutput,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MigrationFamily {
    Value,
    Set,
    Map,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveredSource {
    pub(crate) family: MigrationFamily,
    pub(crate) path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MigrationFailure {
    pub(crate) checkpoint: MigrationCheckpoint,
    pub(crate) path: PathBuf,
    pub(crate) detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HandledMigrationFailure {
    pub(crate) original: MigrationFailure,
    pub(crate) cleanup: Option<MigrationFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedSource {
    pub(crate) path: PathBuf,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedLegacyKeyValue {
    pub(crate) source: CapturedSource,
    pub(crate) snapshot: KeyValueSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MigrationSuccess {
    pub(crate) family: MigrationFamily,
    pub(crate) source_path: PathBuf,
    pub(crate) output_path: PathBuf,
    pub(crate) entries: usize,
    pub(crate) bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryMigrationSuccess {
    pub(crate) families: Vec<MigrationSuccess>,
}

struct PreparedMigration {
    source: CapturedSource,
    family: MigrationFamily,
    output_name: &'static str,
    replacement: Vec<u8>,
    entries: usize,
}

#[derive(Default)]
pub(crate) struct OwnedPathRegistry {
    owned: Vec<PathBuf>,
    attempted: Vec<PathBuf>,
}

impl OwnedPathRegistry {
    pub(crate) fn owned(&self) -> &[PathBuf] {
        &self.owned
    }

    pub(crate) fn attempted(&self) -> &[PathBuf] {
        &self.attempted
    }

    pub(crate) fn register_created_file(&mut self, path: &Path) -> bool {
        let parent_owned = path
            .parent()
            .is_some_and(|parent| self.owned.first().is_some_and(|owned| owned == parent));
        if parent_owned && path.is_file() && !self.owned.iter().any(|owned| owned == path) {
            self.owned.push(path.to_path_buf());
            true
        } else {
            false
        }
    }
}

impl MigrationProbe {
    pub(crate) fn migrate_directory(
        source: &Path,
        destination: &Path,
        granularity: u64,
    ) -> Result<DirectoryMigrationSuccess, HandledMigrationFailure> {
        let preflight = (|| -> Result<Vec<PreparedMigration>, MigrationFailure> {
            let source_metadata = std::fs::metadata(source).map_err(|error| MigrationFailure {
                checkpoint: MigrationCheckpoint::InitialSourceRead,
                path: source.to_path_buf(),
                detail: error.to_string(),
            })?;
            if !source_metadata.is_dir() {
                return Err(MigrationFailure {
                    checkpoint: MigrationCheckpoint::Preflight,
                    path: source.to_path_buf(),
                    detail: "migration source must be a readable directory".to_owned(),
                });
            }
            if granularity == 0 {
                return Err(MigrationFailure {
                    checkpoint: MigrationCheckpoint::Preflight,
                    path: source.to_path_buf(),
                    detail: "timestamp granularity must be nonzero".to_owned(),
                });
            }
            Self::validate_path_relation(source, destination)?;
            Self::inspect_destination(destination)?;
            Self::reject_unresolved_artifacts(source)?;
            Self::discover_canonical_sources(source)?
                .into_iter()
                .map(|discovered| Self::prepare_source(discovered, granularity))
                .collect()
        })();
        let prepared = preflight.map_err(|original| HandledMigrationFailure {
            original,
            cleanup: None,
        })?;

        let mut registry = OwnedPathRegistry::default();
        if let Err(original) = Self::create_destination(destination, &mut registry) {
            return Err(Self::handle_failure_with_cleanup(
                original,
                &mut registry,
                None,
            ));
        }
        exit_at_process_checkpoint("destination-created");

        let mut successes = Vec::with_capacity(prepared.len());
        for item in &prepared {
            let output_path = destination.join(item.output_name);
            let output = match Self::create_output(&output_path, &mut registry, false) {
                Ok(output) => output,
                Err(original) => {
                    return Err(Self::handle_failure_with_cleanup(
                        original,
                        &mut registry,
                        None,
                    ));
                }
            };
            #[cfg(debug_assertions)]
            let mut output = output;
            #[cfg(debug_assertions)]
            if process_checkpoint_requested("partial-output-written") {
                let partial_len = item
                    .replacement
                    .len()
                    .min(V1CodecProbe::HEADER_LEN.saturating_sub(1))
                    .max(1);
                output
                    .write_all(&item.replacement[..partial_len])
                    .expect("debug migration checkpoint partial write must succeed");
                output
                    .flush()
                    .expect("debug migration checkpoint partial flush must succeed");
                std::process::exit(86);
            }
            let output = Self::write_output_prefix(
                output,
                &item.replacement,
                item.replacement.len(),
                &output_path,
                &mut registry,
            )?;
            exit_at_process_checkpoint("complete-output-written");
            let output = Self::flush_output(output, false, &output_path, &mut registry)?;
            let output = Self::sync_output(output, false, &output_path, &mut registry)?;
            let reopened = Self::reopen_output(output, false, &output_path, &mut registry)?;
            Self::validate_reopened_prepared(reopened, item, &output_path, &mut registry)?;
            exit_at_process_checkpoint("output-validated");
            successes.push(MigrationSuccess {
                family: item.family,
                source_path: item.source.path.clone(),
                output_path,
                entries: item.entries,
                bytes: item.replacement.len(),
            });
        }

        for item in &prepared {
            let reread = Self::reread_source(&item.source, false, &mut registry)?;
            Self::verify_source_stable(&item.source, &reread, &mut registry)?;
        }

        Ok(DirectoryMigrationSuccess {
            families: successes,
        })
    }

    fn prepare_source(
        discovered: DiscoveredSource,
        granularity: u64,
    ) -> Result<PreparedMigration, MigrationFailure> {
        let source = Self::capture_source(&discovered.path, false)?;
        if source.bytes.starts_with(b"PIGWAL\r\n") {
            return Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Preflight,
                path: source.path,
                detail: "V1 source is already current and is not migratable".to_owned(),
            });
        }
        let (replacement, entries, output_name) = match discovered.family {
            MigrationFamily::Value => {
                let snapshot = replay_key_value(&source.bytes)
                    .map_err(|error| Self::legacy_validation_failure(&source.path, error))?
                    .snapshot;
                let entries = snapshot.len();
                (
                    Self::encode_key_value_snapshot(
                        &snapshot,
                        V1CodecProbe::encode_header_with_kind_and_granularity(1, granularity),
                    ),
                    entries,
                    "kv.wal.dat",
                )
            }
            MigrationFamily::Set => {
                let snapshot = replay_key_set(&source.bytes)
                    .map_err(|error| Self::legacy_validation_failure(&source.path, error))?
                    .snapshot;
                let entries = snapshot.values().map(std::collections::HashSet::len).sum();
                (
                    Self::encode_key_set_snapshot(&snapshot, granularity),
                    entries,
                    "set.wal.dat",
                )
            }
            MigrationFamily::Map => {
                let snapshot = replay_key_map(&source.bytes)
                    .map_err(|error| Self::legacy_validation_failure(&source.path, error))?
                    .snapshot;
                let entries = snapshot.values().map(std::collections::BTreeMap::len).sum();
                (
                    Self::encode_key_map_snapshot(&snapshot, granularity),
                    entries,
                    "map.wal.dat",
                )
            }
        };
        Ok(PreparedMigration {
            source,
            family: discovered.family,
            output_name,
            replacement,
            entries,
        })
    }

    fn legacy_validation_failure(path: &Path, error: ValidationError) -> MigrationFailure {
        let detail = match error {
            ValidationError::Truncated { .. } => "truncated legacy WAL source",
            ValidationError::InvalidPayload { .. } => {
                "legacy WAL payload is incompatible with canonical family"
            }
            _ => "corrupt legacy WAL source",
        };
        MigrationFailure {
            checkpoint: MigrationCheckpoint::Preflight,
            path: path.to_path_buf(),
            detail: detail.to_owned(),
        }
    }

    fn validate_reopened_prepared(
        bytes: Vec<u8>,
        prepared: &PreparedMigration,
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<(), HandledMigrationFailure> {
        let replay_valid = match prepared.family {
            MigrationFamily::Value => replay_key_value(&bytes).is_ok(),
            MigrationFamily::Set => replay_key_set(&bytes).is_ok(),
            MigrationFamily::Map => replay_key_map(&bytes).is_ok(),
        };
        if replay_valid && bytes == prepared.replacement {
            Ok(())
        } else {
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "reopened migration output failed exact V1/config/logical validation"
                    .to_owned(),
            };
            Err(Self::handle_failure_with_cleanup(original, registry, None))
        }
    }

    pub(crate) fn migrate_validated_key_value(
        validated: ValidatedLegacyKeyValue,
        destination: &Path,
        granularity: u64,
    ) -> Result<MigrationSuccess, HandledMigrationFailure> {
        let ValidatedLegacyKeyValue { source, snapshot } = validated;
        let mut registry = OwnedPathRegistry::default();
        if let Err(original) = Self::inspect_destination(destination) {
            return Err(HandledMigrationFailure {
                original,
                cleanup: None,
            });
        }
        if let Err(original) = Self::create_destination(destination, &mut registry) {
            return Err(Self::handle_failure_with_cleanup(
                original,
                &mut registry,
                None,
            ));
        }

        let replacement = Self::key_value_snapshot_to_v1_with_granularity(&snapshot, granularity);
        let output_path = destination.join("kv.wal.dat");
        let output = match Self::create_output(&output_path, &mut registry, false) {
            Ok(output) => output,
            Err(original) => {
                return Err(Self::handle_failure_with_cleanup(
                    original,
                    &mut registry,
                    None,
                ));
            }
        };
        let output = Self::write_output_prefix(
            output,
            &replacement,
            replacement.len(),
            &output_path,
            &mut registry,
        )?;
        let output = Self::flush_output(output, false, &output_path, &mut registry)?;
        let output = Self::sync_output(output, false, &output_path, &mut registry)?;
        let reopened = Self::reopen_output(output, false, &output_path, &mut registry)?;
        let reopened = Self::validate_reopened_key_value(
            reopened,
            &snapshot,
            granularity,
            &output_path,
            &mut registry,
        )?;
        let reread = Self::reread_source(&source, false, &mut registry)?;
        Self::verify_source_stable(&source, &reread, &mut registry)?;

        Ok(MigrationSuccess {
            family: MigrationFamily::Value,
            source_path: source.path,
            output_path,
            entries: snapshot.len(),
            bytes: reopened.len(),
        })
    }

    pub(crate) fn verify_source_stable(
        captured: &CapturedSource,
        reread: &[u8],
        registry: &mut OwnedPathRegistry,
    ) -> Result<(), HandledMigrationFailure> {
        if captured.bytes == reread {
            Ok(())
        } else {
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::SourceChanged,
                path: captured.path.clone(),
                detail: "source changed during offline migration".to_owned(),
            };
            Err(Self::handle_failure_with_cleanup(original, registry, None))
        }
    }

    pub(crate) fn reread_source(
        captured: &CapturedSource,
        inject_failure: bool,
        registry: &mut OwnedPathRegistry,
    ) -> Result<Vec<u8>, HandledMigrationFailure> {
        if inject_failure {
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::FinalSourceRead,
                path: captured.path.clone(),
                detail: "injected final source reread failure".to_owned(),
            };
            return Err(Self::handle_failure_with_cleanup(original, registry, None));
        }
        std::fs::read(&captured.path).map_err(|error| {
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::FinalSourceRead,
                path: captured.path.clone(),
                detail: error.to_string(),
            };
            Self::handle_failure_with_cleanup(original, registry, None)
        })
    }

    pub(crate) fn validate_reopened_key_value(
        bytes: Vec<u8>,
        expected_snapshot: &KeyValueSnapshot,
        expected_granularity: u64,
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<Vec<u8>, HandledMigrationFailure> {
        let validation = (|| -> Result<(), String> {
            if bytes.len() < V1CodecProbe::HEADER_LEN || !bytes.starts_with(b"PIGWAL\r\n") {
                return Err("reopened migration output is not V1".to_owned());
            }
            let actual_granularity = u64::from_le_bytes(
                bytes[16..24]
                    .try_into()
                    .expect("V1 header length checked before granularity read"),
            );
            if actual_granularity != expected_granularity {
                return Err(format!(
                    "reopened migration output granularity {actual_granularity} does not match requested {expected_granularity}"
                ));
            }
            let replayed = replay_key_value(&bytes)
                .map_err(|error| format!("reopened migration output is invalid: {error}"))?;
            if &replayed.snapshot != expected_snapshot {
                return Err(
                    "reopened migration output logical state does not match source".to_owned(),
                );
            }
            Ok(())
        })();

        match validation {
            Ok(()) => Ok(bytes),
            Err(detail) => {
                let original = MigrationFailure {
                    checkpoint: MigrationCheckpoint::WriteOutput,
                    path: path.to_path_buf(),
                    detail,
                };
                Err(Self::handle_failure_with_cleanup(original, registry, None))
            }
        }
    }

    pub(crate) fn reopen_output(
        output: std::fs::File,
        inject_failure: bool,
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<Vec<u8>, HandledMigrationFailure> {
        drop(output);
        if inject_failure {
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "injected output reopen/read failure".to_owned(),
            };
            return Err(Self::handle_failure_with_cleanup(original, registry, None));
        }
        match std::fs::read(path) {
            Ok(bytes) => Ok(bytes),
            Err(error) => {
                let original = MigrationFailure {
                    checkpoint: MigrationCheckpoint::WriteOutput,
                    path: path.to_path_buf(),
                    detail: error.to_string(),
                };
                Err(Self::handle_failure_with_cleanup(original, registry, None))
            }
        }
    }

    pub(crate) fn sync_output(
        output: std::fs::File,
        inject_failure: bool,
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<std::fs::File, HandledMigrationFailure> {
        if inject_failure {
            drop(output);
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "injected output synchronization failure".to_owned(),
            };
            return Err(Self::handle_failure_with_cleanup(original, registry, None));
        }
        if let Err(error) = output.sync_all() {
            drop(output);
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: error.to_string(),
            };
            Err(Self::handle_failure_with_cleanup(original, registry, None))
        } else {
            Ok(output)
        }
    }

    pub(crate) fn flush_output(
        mut output: std::fs::File,
        inject_failure: bool,
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<std::fs::File, HandledMigrationFailure> {
        if inject_failure {
            drop(output);
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "injected output flush failure".to_owned(),
            };
            return Err(Self::handle_failure_with_cleanup(original, registry, None));
        }
        if let Err(error) = output.flush() {
            drop(output);
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: error.to_string(),
            };
            Err(Self::handle_failure_with_cleanup(original, registry, None))
        } else {
            Ok(output)
        }
    }

    pub(crate) fn write_output_prefix(
        mut output: std::fs::File,
        replacement: &[u8],
        written_len: usize,
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<std::fs::File, HandledMigrationFailure> {
        let written_len = written_len.min(replacement.len());
        if let Err(error) = output.write_all(&replacement[..written_len]) {
            drop(output);
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: error.to_string(),
            };
            return Err(Self::handle_failure_with_cleanup(original, registry, None));
        }
        if written_len == replacement.len() {
            Ok(output)
        } else {
            drop(output);
            let original = MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "partial migration output write".to_owned(),
            };
            Err(Self::handle_failure_with_cleanup(original, registry, None))
        }
    }

    pub(crate) fn create_output(
        path: &Path,
        registry: &mut OwnedPathRegistry,
        inject_failure: bool,
    ) -> Result<std::fs::File, MigrationFailure> {
        if inject_failure {
            return Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "injected output create failure".to_owned(),
            });
        }
        let output = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: error.to_string(),
            })?;
        if registry.register_created_file(path) {
            Ok(output)
        } else {
            drop(output);
            let _ = std::fs::remove_file(path);
            Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::WriteOutput,
                path: path.to_path_buf(),
                detail: "created output could not be registered as invocation-owned".to_owned(),
            })
        }
    }

    pub(crate) fn handle_failure_with_cleanup(
        original: MigrationFailure,
        registry: &mut OwnedPathRegistry,
        inject_cleanup_failure: Option<&Path>,
    ) -> HandledMigrationFailure {
        let cleanup = cleanup_owned_with_fault(registry, inject_cleanup_failure).err();
        HandledMigrationFailure { original, cleanup }
    }

    pub(crate) fn cleanup_owned(registry: &mut OwnedPathRegistry) -> Result<(), MigrationFailure> {
        cleanup_owned_with_fault(registry, None)
    }

    pub(crate) fn create_destination(
        path: &Path,
        registry: &mut OwnedPathRegistry,
    ) -> Result<(), MigrationFailure> {
        std::fs::create_dir(path).map_err(|error| MigrationFailure {
            checkpoint: MigrationCheckpoint::CreateDestination,
            path: path.to_path_buf(),
            detail: error.to_string(),
        })?;
        registry.owned.push(path.to_path_buf());
        Ok(())
    }

    pub(crate) fn validate_legacy_key_value(
        source: CapturedSource,
    ) -> Result<ValidatedLegacyKeyValue, MigrationFailure> {
        if source.bytes.starts_with(b"PIGWAL\r\n") {
            return Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Preflight,
                path: source.path,
                detail: "V1 source is already current and is not migratable".to_owned(),
            });
        }
        match replay_key_value(&source.bytes) {
            Ok(replayed) => Ok(ValidatedLegacyKeyValue {
                source,
                snapshot: replayed.snapshot,
            }),
            Err(ValidationError::Truncated { .. }) => Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Preflight,
                path: source.path,
                detail: "truncated legacy WAL source".to_owned(),
            }),
            Err(ValidationError::InvalidPayload { .. }) => Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Preflight,
                path: source.path,
                detail: "legacy WAL payload is incompatible with canonical family".to_owned(),
            }),
            Err(_) => Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Preflight,
                path: source.path,
                detail: "corrupt legacy WAL source".to_owned(),
            }),
        }
    }

    pub(crate) fn inspect_destination(path: &Path) -> Result<(), MigrationFailure> {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::DestinationInspection,
                path: path.to_path_buf(),
                detail: "migration destination already exists".to_owned(),
            }),
            Err(error) => Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::DestinationInspection,
                path: path.to_path_buf(),
                detail: error.to_string(),
            }),
        }
    }

    pub(crate) fn validate_path_relation(
        source: &Path,
        destination: &Path,
    ) -> Result<(), MigrationFailure> {
        let canonical_source = std::fs::canonicalize(source).map_err(|error| MigrationFailure {
            checkpoint: MigrationCheckpoint::DestinationInspection,
            path: source.to_path_buf(),
            detail: error.to_string(),
        })?;
        let resolved_destination =
            resolve_destination(destination).map_err(|detail| MigrationFailure {
                checkpoint: MigrationCheckpoint::DestinationInspection,
                path: destination.to_path_buf(),
                detail,
            })?;
        if resolved_destination == canonical_source
            || resolved_destination.starts_with(&canonical_source)
        {
            Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::DestinationInspection,
                path: destination.to_path_buf(),
                detail: "destination must be outside the source directory".to_owned(),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn reject_unresolved_artifacts(directory: &Path) -> Result<(), MigrationFailure> {
        for canonical in ["kv.wal.dat", "set.wal.dat", "map.wal.dat"] {
            for name in [format!(".{canonical}"), format!(".{canonical}.next")] {
                let path = directory.join(name);
                match std::fs::symlink_metadata(&path) {
                    Ok(_) => {
                        return Err(MigrationFailure {
                            checkpoint: MigrationCheckpoint::Preflight,
                            path,
                            detail: "unresolved recovery or staging artifact".to_owned(),
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(MigrationFailure {
                            checkpoint: MigrationCheckpoint::Preflight,
                            path,
                            detail: error.to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn discover_canonical_sources(
        directory: &Path,
    ) -> Result<Vec<DiscoveredSource>, MigrationFailure> {
        let discovered = [
            (MigrationFamily::Value, "kv.wal.dat"),
            (MigrationFamily::Set, "set.wal.dat"),
            (MigrationFamily::Map, "map.wal.dat"),
        ]
        .into_iter()
        .filter_map(|(family, name)| {
            let path = directory.join(name);
            path.exists().then_some(DiscoveredSource { family, path })
        })
        .collect::<Vec<_>>();
        if discovered.is_empty() {
            Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Preflight,
                path: directory.to_path_buf(),
                detail: "source directory contains no canonical WAL files".to_owned(),
            })
        } else {
            Ok(discovered)
        }
    }

    pub(crate) fn capture_source(
        path: &Path,
        inject_read_failure: bool,
    ) -> Result<CapturedSource, MigrationFailure> {
        let mut source =
            OpenOptions::new()
                .read(true)
                .open(path)
                .map_err(|error| MigrationFailure {
                    checkpoint: MigrationCheckpoint::InitialSourceRead,
                    path: path.to_path_buf(),
                    detail: error.to_string(),
                })?;
        let mut bytes = Vec::new();
        if inject_read_failure {
            return Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::InitialSourceRead,
                path: path.to_path_buf(),
                detail: "injected initial source read failure".to_owned(),
            });
        }
        source
            .read_to_end(&mut bytes)
            .map_err(|error| MigrationFailure {
                checkpoint: MigrationCheckpoint::InitialSourceRead,
                path: path.to_path_buf(),
                detail: error.to_string(),
            })?;
        Ok(CapturedSource {
            path: path.to_path_buf(),
            bytes,
        })
    }

    pub(crate) fn key_value_snapshot_to_v1(snapshot: &KeyValueSnapshot) -> Vec<u8> {
        Self::encode_key_value_snapshot(snapshot, V1CodecProbe::encode_header())
    }

    fn encode_key_value_snapshot(
        snapshot: &KeyValueSnapshot,
        header: [u8; V1CodecProbe::HEADER_LEN],
    ) -> Vec<u8> {
        let mut converted = header.to_vec();
        let mut entries = snapshot.iter().collect::<Vec<_>>();
        entries.sort_by_key(|(key, _)| *key);
        for (key, value) in entries {
            let payload = bincode::serialize(&KeyValueData::new(key.clone(), value.clone()))
                .expect("captured legacy key/value state must encode");
            let start = converted.len() as u32;
            converted.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
                action: 1,
                payload: &payload,
                physical_start: start,
                mutation_start: start,
                index: 0,
                count: 1,
                timestamp_bucket: 0,
            }));
        }
        converted
    }

    pub(crate) fn key_value_snapshot_to_v1_with_granularity(
        snapshot: &KeyValueSnapshot,
        granularity_nanos: u64,
    ) -> Vec<u8> {
        Self::encode_key_value_snapshot(
            snapshot,
            V1CodecProbe::encode_header_with_granularity(granularity_nanos),
        )
    }

    fn encode_key_set_snapshot(snapshot: &KeySetSnapshot, granularity: u64) -> Vec<u8> {
        let header = V1CodecProbe::encode_header_with_kind_and_granularity(2, granularity);
        let mut converted = header.to_vec();
        let mut keys = snapshot.keys().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            let mut values = snapshot[key].iter().collect::<Vec<_>>();
            values.sort();
            for value in values {
                let payload = bincode::serialize(&KeyValueData::new(key.clone(), value.clone()))
                    .expect("captured legacy key/set state must encode");
                Self::append_v1_snapshot_record(&mut converted, SET_APPEND_ACT, &payload);
            }
        }
        converted
    }

    fn encode_key_map_snapshot(snapshot: &KeyMapSnapshot, granularity: u64) -> Vec<u8> {
        let header = V1CodecProbe::encode_header_with_kind_and_granularity(3, granularity);
        let mut converted = header.to_vec();
        let mut keys = snapshot.keys().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            for (search_key, value) in &snapshot[key] {
                let payload = bincode::serialize(&SortedMapEntry::new(
                    key.clone(),
                    search_key.clone(),
                    value.clone(),
                ))
                .expect("captured legacy key/map state must encode");
                Self::append_v1_snapshot_record(&mut converted, MAP_PUT_ACT, &payload);
            }
        }
        converted
    }

    fn append_v1_snapshot_record(converted: &mut Vec<u8>, action: u8, payload: &[u8]) {
        let start = converted.len() as u32;
        converted.extend_from_slice(&V1CodecProbe::encode_complete_record(RecordProbeFields {
            action,
            payload,
            physical_start: start,
            mutation_start: start,
            index: 0,
            count: 1,
            timestamp_bucket: 0,
        }));
    }
}

#[cfg(debug_assertions)]
fn process_checkpoint_requested(checkpoint: &str) -> bool {
    std::env::var_os("PIGMENT_DB_MIGRATION_TEST_CHECKPOINT")
        .is_some_and(|requested| requested == checkpoint)
}

#[cfg(debug_assertions)]
pub(crate) fn exit_at_process_checkpoint(checkpoint: &str) {
    if process_checkpoint_requested(checkpoint) {
        std::process::exit(86);
    }
}

#[cfg(not(debug_assertions))]
pub(crate) fn exit_at_process_checkpoint(_checkpoint: &str) {}

fn cleanup_owned_with_fault(
    registry: &mut OwnedPathRegistry,
    inject_failure: Option<&Path>,
) -> Result<(), MigrationFailure> {
    for target in registry.owned.clone().into_iter().rev() {
        registry.attempted.push(target.clone());
        if inject_failure == Some(target.as_path()) {
            return Err(MigrationFailure {
                checkpoint: MigrationCheckpoint::Cleanup,
                path: target,
                detail: "injected cleanup removal failure".to_owned(),
            });
        }
        let removal = if target.is_dir() {
            std::fs::remove_dir(&target)
        } else {
            std::fs::remove_file(&target)
        };
        removal.map_err(|error| MigrationFailure {
            checkpoint: MigrationCheckpoint::Cleanup,
            path: target.clone(),
            detail: error.to_string(),
        })?;
        registry.owned.pop();
    }
    Ok(())
}

fn resolve_destination(destination: &Path) -> Result<PathBuf, String> {
    if let Ok(canonical) = std::fs::canonicalize(destination) {
        return Ok(canonical);
    }
    let absolute = if destination.is_absolute() {
        destination.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join(destination)
    };
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    loop {
        if let Ok(canonical) = std::fs::canonicalize(ancestor) {
            return Ok(suffix
                .iter()
                .rev()
                .fold(canonical, |resolved, component| resolved.join(component)));
        }
        let name = ancestor
            .file_name()
            .ok_or_else(|| "destination has no resolvable existing ancestor".to_owned())?;
        suffix.push(name.to_os_string());
        ancestor = ancestor
            .parent()
            .ok_or_else(|| "destination has no resolvable existing ancestor".to_owned())?;
    }
}

#[cfg(test)]
mod tests {
    use super::{MigrationCheckpoint, MigrationFamily, MigrationProbe, OwnedPathRegistry};
    use crate::model::{SearchKey, SortedMapEntry};
    use crate::wal::format::V1CodecProbe;
    use crate::wal::replay::{replay_key_map, replay_key_set, replay_key_value};

    #[test]
    fn one_family_legacy_snapshot_converts_to_complete_v1_bytes() {
        let snapshot = std::collections::HashMap::from([
            (b"alpha".to_vec(), b"one".to_vec()),
            (b"beta".to_vec(), b"two".to_vec()),
        ]);

        let converted = MigrationProbe::key_value_snapshot_to_v1(&snapshot);

        assert!(converted.starts_with(b"PIGWAL\r\n"));
        assert_eq!(converted[12], 1);
        assert_eq!(
            u64::from_le_bytes(converted[16..24].try_into().unwrap()),
            60_000_000_000
        );
        let replayed = replay_key_value(&converted).unwrap();
        assert_eq!(replayed.snapshot, snapshot);
        assert_eq!(replayed.byte_len, converted.len() as u64);
        assert!(converted.len() > V1CodecProbe::HEADER_LEN);
    }

    #[test]
    fn selected_granularity_is_encoded_without_changing_logical_state() {
        let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let selected = 123_456_789_u64;

        let converted =
            MigrationProbe::key_value_snapshot_to_v1_with_granularity(&snapshot, selected);

        assert_eq!(
            u64::from_le_bytes(converted[16..24].try_into().unwrap()),
            selected
        );
        let replayed = replay_key_value(&converted).unwrap();
        assert_eq!(replayed.snapshot, snapshot);
    }

    #[test]
    fn initial_canonical_source_read_failure_has_exact_diagnostic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("kv.wal.dat");
        let source = b"immutable-legacy-source";
        std::fs::write(&path, source).unwrap();

        let failure = MigrationProbe::capture_source(&path, true)
            .expect_err("injected initial read failure must be reported");

        assert_eq!(failure.checkpoint, MigrationCheckpoint::InitialSourceRead);
        assert_eq!(failure.path, path);
        assert!(failure.detail.contains("injected"));
        assert_eq!(std::fs::read(&path).unwrap(), source);

        let missing = directory.path().join("set.wal.dat");
        let open_failure = MigrationProbe::capture_source(&missing, false)
            .expect_err("missing canonical source must report an open failure");
        assert_eq!(
            open_failure.checkpoint,
            MigrationCheckpoint::InitialSourceRead
        );
        assert_eq!(open_failure.path, missing);
        assert!(!open_failure.detail.is_empty());
    }

    #[test]
    fn preflight_requires_at_least_one_canonical_source() {
        let empty = tempfile::tempdir().unwrap();

        let failure = MigrationProbe::discover_canonical_sources(empty.path())
            .expect_err("an empty source directory is not migratable");

        assert_eq!(failure.checkpoint, MigrationCheckpoint::Preflight);
        assert_eq!(failure.path, empty.path());
        assert!(failure.detail.contains("canonical"));

        let populated = tempfile::tempdir().unwrap();
        std::fs::write(populated.path().join("kv.wal.dat"), b"legacy").unwrap();
        let discovered = MigrationProbe::discover_canonical_sources(populated.path()).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].family, MigrationFamily::Value);
        assert_eq!(discovered[0].path, populated.path().join("kv.wal.dat"));
    }

    #[test]
    fn preflight_rejects_recognized_recovery_and_staging_artifacts_without_cleanup() {
        for unresolved_name in [".kv.wal.dat", ".kv.wal.dat.next"] {
            let directory = tempfile::tempdir().unwrap();
            let active = directory.path().join("kv.wal.dat");
            let unresolved = directory.path().join(unresolved_name);
            let active_bytes = b"canonical-source";
            let unresolved_bytes = b"unresolved-authority";
            std::fs::write(&active, active_bytes).unwrap();
            std::fs::write(&unresolved, unresolved_bytes).unwrap();

            let failure = MigrationProbe::reject_unresolved_artifacts(directory.path())
                .expect_err("recognized unresolved artifacts must block migration");

            assert_eq!(failure.checkpoint, MigrationCheckpoint::Preflight);
            assert_eq!(failure.path, unresolved);
            assert!(failure.detail.contains("unresolved"));
            assert_eq!(std::fs::read(active).unwrap(), active_bytes);
            assert_eq!(std::fs::read(unresolved).unwrap(), unresolved_bytes);
        }
    }

    #[test]
    fn preflight_rejects_v1_source_as_nonmigratable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("kv.wal.dat");
        let v1 = V1CodecProbe::encode_header();
        std::fs::write(&path, v1).unwrap();
        let captured = MigrationProbe::capture_source(&path, false).unwrap();

        let failure = MigrationProbe::validate_legacy_key_value(captured)
            .expect_err("V1 is already current and must not enter legacy migration");

        assert_eq!(failure.checkpoint, MigrationCheckpoint::Preflight);
        assert_eq!(failure.path, path);
        assert!(failure.detail.contains("V1"));
        assert_eq!(std::fs::read(path).unwrap(), v1);
    }

    #[test]
    fn preflight_rejects_truncated_legacy_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("kv.wal.dat");
        let mut truncated = legacy_put_bytes(b"key", b"value");
        truncated.pop();
        std::fs::write(&path, &truncated).unwrap();
        let captured = MigrationProbe::capture_source(&path, false).unwrap();

        let failure = MigrationProbe::validate_legacy_key_value(captured)
            .expect_err("truncated legacy input must not be guessed or converted");

        assert_eq!(failure.checkpoint, MigrationCheckpoint::Preflight);
        assert_eq!(failure.path, path);
        assert!(failure.detail.contains("truncated"));
        assert_eq!(std::fs::read(path).unwrap(), truncated);
    }

    #[test]
    fn preflight_rejects_complete_corrupt_legacy_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("kv.wal.dat");
        let mut corrupt = legacy_put_bytes(b"key", b"value");
        corrupt[1] ^= 0xff;
        std::fs::write(&path, &corrupt).unwrap();
        let captured = MigrationProbe::capture_source(&path, false).unwrap();

        let failure = MigrationProbe::validate_legacy_key_value(captured)
            .expect_err("complete corrupt legacy input must be rejected");

        assert_eq!(failure.checkpoint, MigrationCheckpoint::Preflight);
        assert_eq!(failure.path, path);
        assert!(failure.detail.contains("corrupt"));
        assert_eq!(std::fs::read(path).unwrap(), corrupt);
    }

    #[test]
    fn preflight_rejects_payload_from_wrong_canonical_family() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("kv.wal.dat");
        let mut set_record = legacy_put_bytes(b"set", b"member");
        set_record[0] = 2;
        std::fs::write(&path, &set_record).unwrap();
        let captured = MigrationProbe::capture_source(&path, false).unwrap();

        let failure = MigrationProbe::validate_legacy_key_value(captured)
            .expect_err("set payload in canonical value WAL must be rejected");

        assert_eq!(failure.checkpoint, MigrationCheckpoint::Preflight);
        assert_eq!(failure.path, path);
        assert!(failure.detail.contains("family"));
        assert_eq!(std::fs::read(path).unwrap(), set_record);
    }

    #[test]
    fn destination_inspection_rejects_existing_file_directory_and_symlink() {
        let root = tempfile::tempdir().unwrap();

        let file = root.path().join("existing-file");
        std::fs::write(&file, b"evidence").unwrap();
        assert_existing_destination_rejected(&file);
        assert_eq!(std::fs::read(&file).unwrap(), b"evidence");

        let directory = root.path().join("existing-directory");
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("evidence"), b"untouched").unwrap();
        assert_existing_destination_rejected(&directory);
        assert_eq!(
            std::fs::read(directory.join("evidence")).unwrap(),
            b"untouched"
        );

        #[cfg(unix)]
        {
            let target = root.path().join("target");
            std::fs::create_dir(&target).unwrap();
            let symlink = root.path().join("existing-symlink");
            std::os::unix::fs::symlink(&target, &symlink).unwrap();
            assert_existing_destination_rejected(&symlink);
            assert_eq!(std::fs::read_link(symlink).unwrap(), target);
        }
    }

    #[test]
    fn destination_must_not_equal_or_be_inside_source() {
        let source = tempfile::tempdir().unwrap();
        let evidence = source.path().join("kv.wal.dat");
        std::fs::write(&evidence, b"immutable-source").unwrap();

        for destination in [source.path().to_path_buf(), source.path().join("nested-v1")] {
            let failure = MigrationProbe::validate_path_relation(source.path(), &destination)
                .expect_err("destination at or below source must be rejected");
            assert_eq!(
                failure.checkpoint,
                MigrationCheckpoint::DestinationInspection
            );
            assert_eq!(failure.path, destination);
            assert!(failure.detail.contains("source"));
            assert_eq!(std::fs::read(&evidence).unwrap(), b"immutable-source");
        }
    }

    #[test]
    fn destination_create_registers_only_successfully_created_directory() {
        let root = tempfile::tempdir().unwrap();
        let failed = root.path().join("missing-parent").join("destination");
        let mut failed_registry = OwnedPathRegistry::default();
        let failure = MigrationProbe::create_destination(&failed, &mut failed_registry)
            .expect_err("destination creation with a missing parent must fail");
        assert_eq!(failure.checkpoint, MigrationCheckpoint::CreateDestination);
        assert_eq!(failure.path, failed);
        assert!(!failed.exists());
        assert!(failed_registry.owned().is_empty());

        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        assert!(destination.is_dir());
        assert_eq!(registry.owned(), [destination]);
    }

    #[test]
    fn owned_cleanup_removes_files_then_directory_in_reverse_order() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output = destination.join("kv.wal.dat");
        std::fs::write(&output, b"partial-output").unwrap();
        assert!(registry.register_created_file(&output));

        MigrationProbe::cleanup_owned(&mut registry).unwrap();

        assert_eq!(registry.attempted(), [output, destination.clone()]);
        assert!(registry.owned().is_empty());
        assert!(!destination.exists());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    #[test]
    fn cleanup_failure_preserves_original_and_stops_broadening_targets() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output = destination.join("kv.wal.dat");
        std::fs::write(&output, b"partial-output").unwrap();
        assert!(registry.register_created_file(&output));
        let original = super::MigrationFailure {
            checkpoint: MigrationCheckpoint::WriteOutput,
            path: output.clone(),
            detail: "synthetic partial write".to_owned(),
        };

        let handled = MigrationProbe::handle_failure_with_cleanup(
            original.clone(),
            &mut registry,
            Some(&output),
        );

        assert_eq!(handled.original, original);
        let cleanup = handled.cleanup.expect("cleanup failure must be retained");
        assert_eq!(cleanup.checkpoint, MigrationCheckpoint::Cleanup);
        assert_eq!(cleanup.path, output);
        assert_eq!(registry.attempted(), std::slice::from_ref(&output));
        assert_eq!(registry.owned(), [destination.clone(), output.clone()]);
        assert_eq!(std::fs::read(output).unwrap(), b"partial-output");
        assert!(destination.exists());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    #[test]
    fn output_create_registers_only_owned_file_and_composes_green_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();

        let failed_destination = root.path().join("failed-destination");
        let mut failed_registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&failed_destination, &mut failed_registry).unwrap();
        let failed_output = failed_destination.join("kv.wal.dat");
        let original =
            MigrationProbe::create_output(&failed_output, &mut failed_registry, true).unwrap_err();
        assert_eq!(
            failed_registry.owned(),
            std::slice::from_ref(&failed_destination)
        );
        assert!(!failed_output.exists());
        let handled =
            MigrationProbe::handle_failure_with_cleanup(original, &mut failed_registry, None);
        assert!(handled.cleanup.is_none());
        assert!(!failed_destination.exists());

        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        assert_eq!(registry.owned(), [destination.clone(), output_path.clone()]);
        drop(output);
        let downstream = super::MigrationFailure {
            checkpoint: MigrationCheckpoint::WriteOutput,
            path: output_path.clone(),
            detail: "synthetic downstream failure".to_owned(),
        };
        let handled = MigrationProbe::handle_failure_with_cleanup(downstream, &mut registry, None);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination]);
        assert!(registry.owned().is_empty());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    #[test]
    fn partial_output_header_or_body_write_composes_owned_cleanup() {
        let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let replacement = MigrationProbe::key_value_snapshot_to_v1(&snapshot);
        for cut in [V1CodecProbe::HEADER_LEN - 1, replacement.len() - 1] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source.wal");
            std::fs::write(&source, b"immutable-source").unwrap();
            let destination = root.path().join("destination");
            let mut registry = OwnedPathRegistry::default();
            MigrationProbe::create_destination(&destination, &mut registry).unwrap();
            let output_path = destination.join("kv.wal.dat");
            let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();

            let handled = match MigrationProbe::write_output_prefix(
                output,
                &replacement,
                cut,
                &output_path,
                &mut registry,
            ) {
                Ok(_) => panic!("partial migration output cut {cut} must fail"),
                Err(handled) => handled,
            };

            assert_eq!(
                handled.original.checkpoint,
                MigrationCheckpoint::WriteOutput
            );
            assert_eq!(handled.original.path, output_path);
            assert!(handled.cleanup.is_none());
            assert_eq!(registry.attempted(), [output_path, destination.clone()]);
            assert!(registry.owned().is_empty());
            assert!(!destination.exists());
            assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
        }
    }

    #[test]
    fn output_flush_failure_composes_owned_cleanup() {
        let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let replacement = MigrationProbe::key_value_snapshot_to_v1(&snapshot);
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        let output = MigrationProbe::write_output_prefix(
            output,
            &replacement,
            replacement.len(),
            &output_path,
            &mut registry,
        )
        .unwrap();

        let handled = match MigrationProbe::flush_output(output, true, &output_path, &mut registry)
        {
            Ok(_) => panic!("injected output flush failure must stop migration"),
            Err(handled) => handled,
        };

        assert_eq!(
            handled.original.checkpoint,
            MigrationCheckpoint::WriteOutput
        );
        assert_eq!(handled.original.path, output_path);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination.clone()]);
        assert!(!destination.exists());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    #[test]
    fn output_sync_failure_composes_owned_cleanup() {
        let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let replacement = MigrationProbe::key_value_snapshot_to_v1(&snapshot);
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        let output = MigrationProbe::write_output_prefix(
            output,
            &replacement,
            replacement.len(),
            &output_path,
            &mut registry,
        )
        .unwrap();
        let output =
            MigrationProbe::flush_output(output, false, &output_path, &mut registry).unwrap();

        let handled = match MigrationProbe::sync_output(output, true, &output_path, &mut registry) {
            Ok(_) => panic!("injected output sync failure must stop migration"),
            Err(handled) => handled,
        };

        assert_eq!(
            handled.original.checkpoint,
            MigrationCheckpoint::WriteOutput
        );
        assert_eq!(handled.original.path, output_path);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination.clone()]);
        assert!(!destination.exists());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    #[test]
    fn output_reopen_read_failure_composes_owned_cleanup() {
        let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let replacement = MigrationProbe::key_value_snapshot_to_v1(&snapshot);
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        let output = MigrationProbe::write_output_prefix(
            output,
            &replacement,
            replacement.len(),
            &output_path,
            &mut registry,
        )
        .unwrap();
        let output =
            MigrationProbe::flush_output(output, false, &output_path, &mut registry).unwrap();
        let output =
            MigrationProbe::sync_output(output, false, &output_path, &mut registry).unwrap();

        let handled = MigrationProbe::reopen_output(output, true, &output_path, &mut registry)
            .expect_err("injected reopen/read failure must stop migration");

        assert_eq!(
            handled.original.checkpoint,
            MigrationCheckpoint::WriteOutput
        );
        assert_eq!(handled.original.path, output_path);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination.clone()]);
        assert!(!destination.exists());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    #[test]
    fn reopened_output_validation_rejects_malformed_v1_and_cleans_owned_paths() {
        assert_reopened_validation_rejected(b"not-v1".to_vec(), 60_000_000_000);
    }

    #[test]
    fn reopened_output_validation_rejects_wrong_granularity_and_cleans_owned_paths() {
        let snapshot = std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let candidate =
            MigrationProbe::key_value_snapshot_to_v1_with_granularity(&snapshot, 123_456_789);
        assert_reopened_validation_rejected(candidate, 60_000_000_000);
    }

    #[test]
    fn reopened_output_validation_rejects_wrong_logical_state_and_cleans_owned_paths() {
        let wrong_snapshot =
            std::collections::HashMap::from([(b"other".to_vec(), b"state".to_vec())]);
        let candidate = MigrationProbe::key_value_snapshot_to_v1(&wrong_snapshot);
        assert_reopened_validation_rejected(candidate, 60_000_000_000);
    }

    #[test]
    fn final_source_reread_failure_cleans_owned_destination() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source.wal");
        std::fs::write(&source_path, b"immutable-source").unwrap();
        let captured = MigrationProbe::capture_source(&source_path, false).unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        drop(output);

        let handled = MigrationProbe::reread_source(&captured, true, &mut registry)
            .expect_err("injected final source reread failure must stop migration");

        assert_eq!(
            handled.original.checkpoint,
            MigrationCheckpoint::FinalSourceRead
        );
        assert_eq!(handled.original.path, source_path);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination.clone()]);
        assert!(!destination.exists());
        assert_eq!(std::fs::read(&captured.path).unwrap(), captured.bytes);
    }

    #[test]
    fn changed_source_after_successful_reread_cleans_owned_destination() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source.wal");
        std::fs::write(&source_path, b"captured-source").unwrap();
        let captured = MigrationProbe::capture_source(&source_path, false).unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        drop(output);
        std::fs::write(&source_path, b"changed-source").unwrap();

        let reread = MigrationProbe::reread_source(&captured, false, &mut registry).unwrap();
        let handled = MigrationProbe::verify_source_stable(&captured, &reread, &mut registry)
            .expect_err("changed source bytes must stop migration");

        assert_eq!(
            handled.original.checkpoint,
            MigrationCheckpoint::SourceChanged
        );
        assert_eq!(handled.original.path, source_path);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination.clone()]);
        assert!(!destination.exists());
        assert_eq!(std::fs::read(&captured.path).unwrap(), b"changed-source");
    }

    #[test]
    fn complete_single_family_migration_preserves_source_and_publishes_validated_v1() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("kv.wal.dat");
        let source_bytes = legacy_put_bytes(b"key", b"value");
        std::fs::write(&source_path, &source_bytes).unwrap();
        let captured = MigrationProbe::capture_source(&source_path, false).unwrap();
        let validated = MigrationProbe::validate_legacy_key_value(captured).unwrap();
        let expected_snapshot = validated.snapshot.clone();
        let destination = root.path().join("destination");
        let granularity = 123_456_789;

        let success =
            MigrationProbe::migrate_validated_key_value(validated, &destination, granularity)
                .expect("complete single-family migration must succeed");

        let output_path = destination.join("kv.wal.dat");
        assert_eq!(success.family, MigrationFamily::Value);
        assert_eq!(success.source_path, source_path);
        assert_eq!(success.output_path, output_path);
        assert_eq!(success.entries, expected_snapshot.len());
        let output = std::fs::read(&success.output_path).unwrap();
        assert_eq!(success.bytes, output.len());
        assert_eq!(
            u64::from_le_bytes(output[16..24].try_into().unwrap()),
            granularity
        );
        assert_eq!(
            replay_key_value(&output).unwrap().snapshot,
            expected_snapshot
        );
        assert_eq!(std::fs::read(&success.source_path).unwrap(), source_bytes);
        assert_eq!(std::fs::read_dir(&destination).unwrap().count(), 1);
    }

    #[test]
    fn complete_directory_migration_converts_all_present_families() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let kv = legacy_put_bytes(b"kv-key", b"kv-value");
        let set_payload = bincode::serialize(&crate::wal::model::KeyValueData::new(
            b"set-key".to_vec(),
            b"set-value".to_vec(),
        ))
        .unwrap();
        let set = legacy_record_bytes(crate::wal::model::SET_APPEND_ACT, &set_payload);
        let map_payload = bincode::serialize(&SortedMapEntry::new(
            b"map-key".to_vec(),
            SearchKey::from(7_usize),
            b"map-value".to_vec(),
        ))
        .unwrap();
        let map = legacy_record_bytes(crate::wal::model::MAP_PUT_ACT, &map_payload);
        std::fs::write(source.join("kv.wal.dat"), &kv).unwrap();
        std::fs::write(source.join("set.wal.dat"), &set).unwrap();
        std::fs::write(source.join("map.wal.dat"), &map).unwrap();
        let destination = root.path().join("destination");

        let success = MigrationProbe::migrate_directory(&source, &destination, 60_000_000_000)
            .expect("all present canonical families must migrate together");

        assert_eq!(success.families.len(), 3);
        assert_eq!(
            replay_key_value(&std::fs::read(destination.join("kv.wal.dat")).unwrap())
                .unwrap()
                .snapshot
                .get(b"kv-key".as_slice()),
            Some(&b"kv-value".to_vec())
        );
        assert!(
            replay_key_set(&std::fs::read(destination.join("set.wal.dat")).unwrap())
                .unwrap()
                .snapshot
                .get(b"set-key".as_slice())
                .unwrap()
                .contains(b"set-value".as_slice())
        );
        assert_eq!(
            replay_key_map(&std::fs::read(destination.join("map.wal.dat")).unwrap())
                .unwrap()
                .snapshot
                .get(b"map-key".as_slice())
                .unwrap()
                .get(&SearchKey::from(7_usize)),
            Some(&b"map-value".to_vec())
        );
        assert_eq!(std::fs::read(source.join("kv.wal.dat")).unwrap(), kv);
        assert_eq!(std::fs::read(source.join("set.wal.dat")).unwrap(), set);
        assert_eq!(std::fs::read(source.join("map.wal.dat")).unwrap(), map);
    }

    #[test]
    fn complete_directory_migration_accepts_empty_and_delete_only_families() {
        for (case, legacy) in [
            ("empty", Vec::new()),
            (
                "delete-only",
                legacy_record_bytes(crate::wal::model::DELETE_ACT, b"missing"),
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            std::fs::create_dir(&source).unwrap();
            for name in ["kv.wal.dat", "set.wal.dat", "map.wal.dat"] {
                std::fs::write(source.join(name), &legacy).unwrap();
            }
            let destination = root.path().join("destination");

            let success = MigrationProbe::migrate_directory(&source, &destination, 60_000_000_000)
                .unwrap_or_else(|error| panic!("{case} families must migrate: {error:?}"));

            assert_eq!(success.families.len(), 3, "{case}");
            let kv = std::fs::read(destination.join("kv.wal.dat")).unwrap();
            let set = std::fs::read(destination.join("set.wal.dat")).unwrap();
            let map = std::fs::read(destination.join("map.wal.dat")).unwrap();
            assert!(replay_key_value(&kv).unwrap().snapshot.is_empty(), "{case}");
            assert!(replay_key_set(&set).unwrap().snapshot.is_empty(), "{case}");
            assert!(replay_key_map(&map).unwrap().snapshot.is_empty(), "{case}");
            assert_eq!(kv.len(), V1CodecProbe::HEADER_LEN, "{case}");
            assert_eq!(set.len(), V1CodecProbe::HEADER_LEN, "{case}");
            assert_eq!(map.len(), V1CodecProbe::HEADER_LEN, "{case}");
            for name in ["kv.wal.dat", "set.wal.dat", "map.wal.dat"] {
                assert_eq!(std::fs::read(source.join(name)).unwrap(), legacy, "{case}");
            }
        }
    }

    fn assert_reopened_validation_rejected(candidate: Vec<u8>, expected_granularity: u64) {
        let expected_snapshot =
            std::collections::HashMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.wal");
        std::fs::write(&source, b"immutable-source").unwrap();
        let destination = root.path().join("destination");
        let mut registry = OwnedPathRegistry::default();
        MigrationProbe::create_destination(&destination, &mut registry).unwrap();
        let output_path = destination.join("kv.wal.dat");
        let output = MigrationProbe::create_output(&output_path, &mut registry, false).unwrap();
        drop(output);

        let handled = MigrationProbe::validate_reopened_key_value(
            candidate,
            &expected_snapshot,
            expected_granularity,
            &output_path,
            &mut registry,
        )
        .expect_err("invalid reopened output must stop migration");

        assert_eq!(
            handled.original.checkpoint,
            MigrationCheckpoint::WriteOutput
        );
        assert_eq!(handled.original.path, output_path);
        assert!(handled.cleanup.is_none());
        assert_eq!(registry.attempted(), [output_path, destination.clone()]);
        assert!(!destination.exists());
        assert_eq!(std::fs::read(source).unwrap(), b"immutable-source");
    }

    fn assert_existing_destination_rejected(path: &std::path::Path) {
        let failure = MigrationProbe::inspect_destination(path)
            .expect_err("migration destination must never be overwritten");
        assert_eq!(
            failure.checkpoint,
            MigrationCheckpoint::DestinationInspection
        );
        assert_eq!(failure.path, path);
        assert!(failure.detail.contains("exists"));
    }

    fn legacy_put_bytes(key: &[u8], value: &[u8]) -> Vec<u8> {
        let payload = bincode::serialize(&crate::wal::model::KeyValueData::new(
            key.to_vec(),
            value.to_vec(),
        ))
        .unwrap();
        let mut bytes = vec![1];
        bytes.extend_from_slice(&crate::wal::model::crc(&payload).to_ne_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&0_u32.to_ne_bytes());
        bytes
    }

    fn legacy_record_bytes(action: u8, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![action];
        bytes.extend_from_slice(&crate::wal::model::crc(payload).to_ne_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&0_u32.to_ne_bytes());
        bytes
    }
}
