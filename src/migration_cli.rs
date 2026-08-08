//! Private argument, diagnostic, and exit-code runner for migration.

#![allow(dead_code)]

use std::ffi::OsString;
use std::path::PathBuf;

use crate::migration::{
    exit_at_process_checkpoint, HandledMigrationFailure, MigrationCheckpoint, MigrationProbe,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MigrationCliProbe;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MigrationCliOptions {
    pub(crate) source: PathBuf,
    pub(crate) destination: PathBuf,
    pub(crate) granularity_nanos: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MigrationCliCommand {
    Migrate(MigrationCliOptions),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MigrationCliParseError {
    Unimplemented,
    InvalidGranularity,
    UnknownOption(OsString),
    DuplicateOption(OsString),
    MissingValue(OsString),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MigrationCliRunResult {
    pub(crate) exit_code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

impl MigrationCliProbe {
    pub(crate) fn render_migration_failure(
        error: HandledMigrationFailure,
    ) -> MigrationCliRunResult {
        let exit_code = match error.original.checkpoint {
            MigrationCheckpoint::InitialSourceRead | MigrationCheckpoint::FinalSourceRead => 3,
            MigrationCheckpoint::Preflight => 4,
            MigrationCheckpoint::DestinationInspection
                if error.original.detail.contains("already exists") =>
            {
                5
            }
            MigrationCheckpoint::DestinationInspection => 2,
            MigrationCheckpoint::CreateDestination
            | MigrationCheckpoint::Cleanup
            | MigrationCheckpoint::WriteOutput => 6,
            MigrationCheckpoint::SourceChanged => 7,
        };
        let mut stderr = format!(
            "error: {:?} at {}: {}\n",
            error.original.checkpoint,
            error.original.path.display(),
            error.original.detail
        );
        if let Some(cleanup) = error.cleanup {
            stderr.push_str(&format!(
                "error: cleanup at {}: {}\n",
                cleanup.path.display(),
                cleanup.detail
            ));
        }
        MigrationCliRunResult {
            exit_code,
            stdout: String::new(),
            stderr,
        }
    }

    pub(crate) fn run_args_os<I, S>(args: I) -> MigrationCliRunResult
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let args = args.into_iter().map(Into::into).collect::<Vec<OsString>>();
        if args.as_slice() == [OsString::from("--help")] {
            return MigrationCliRunResult {
                exit_code: 0,
                stdout: concat!(
                    "Usage: pigment-db-migrate --source <SOURCE_DIR> --destination <V2_DIR>\n",
                    "                           [--timestamp-granularity-nanos <NONZERO_U64>]\n",
                    "       pigment-db-migrate --help\n",
                    "       pigment-db-migrate --version\n",
                )
                .to_owned(),
                stderr: String::new(),
            };
        }
        if args.as_slice() == [OsString::from("--version")] {
            return MigrationCliRunResult {
                exit_code: 0,
                stdout: format!("pigment-db-migrate {}\n", env!("CARGO_PKG_VERSION")),
                stderr: String::new(),
            };
        }
        let command = match Self::parse_args_os(args) {
            Ok(command) => command,
            Err(_) => {
                return MigrationCliRunResult {
                    exit_code: 2,
                    stdout: String::new(),
                    stderr: concat!(
                        "error: invalid arguments\n",
                        "Usage: pigment-db-migrate --source <SOURCE_DIR> --destination <V2_DIR>\n",
                    )
                    .to_owned(),
                };
            }
        };
        let MigrationCliCommand::Migrate(options) = command;
        match MigrationProbe::migrate_directory(
            &options.source,
            &options.destination,
            options.granularity_nanos,
        ) {
            Ok(success) => {
                exit_at_process_checkpoint("before-success-output");
                let entries = success
                    .families
                    .iter()
                    .map(|family| family.entries)
                    .sum::<usize>();
                let bytes = success
                    .families
                    .iter()
                    .map(|family| family.bytes)
                    .sum::<usize>();
                MigrationCliRunResult {
                    exit_code: 0,
                    stdout: format!(
                        "migrated {} family(s), {entries} entries, {bytes} bytes to {}\n",
                        success.families.len(),
                        options.destination.display()
                    ),
                    stderr: String::new(),
                }
            }
            Err(error) => Self::render_migration_failure(error),
        }
    }

    pub(crate) fn parse_args_os<I, S>(
        args: I,
    ) -> Result<MigrationCliCommand, MigrationCliParseError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let args = args.into_iter().map(Into::into).collect::<Vec<OsString>>();
        let mut source = None;
        let mut destination = None;
        let mut granularity_nanos = 60_000_000_000;
        let mut granularity_seen = false;
        let mut index = 0;
        while index < args.len() {
            let option = &args[index];
            if option != "--source"
                && option != "--destination"
                && option != "--timestamp-granularity-nanos"
            {
                return Err(MigrationCliParseError::UnknownOption(option.clone()));
            }
            let Some(value) = args.get(index + 1) else {
                return Err(MigrationCliParseError::MissingValue(option.clone()));
            };
            if value.to_str().is_some_and(|value| value.starts_with("--")) {
                return Err(MigrationCliParseError::MissingValue(option.clone()));
            }
            if option == "--source" {
                if source.is_some() {
                    return Err(MigrationCliParseError::DuplicateOption(option.clone()));
                }
                source = Some(PathBuf::from(value));
            } else if option == "--destination" {
                if destination.is_some() {
                    return Err(MigrationCliParseError::DuplicateOption(option.clone()));
                }
                destination = Some(PathBuf::from(value));
            } else if option == "--timestamp-granularity-nanos" {
                if granularity_seen {
                    return Err(MigrationCliParseError::DuplicateOption(option.clone()));
                }
                granularity_seen = true;
                granularity_nanos = value
                    .to_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|value| *value != 0)
                    .ok_or(MigrationCliParseError::InvalidGranularity)?;
            }
            index += 2;
        }
        match (source, destination) {
            (Some(source), Some(destination)) => {
                Ok(MigrationCliCommand::Migrate(MigrationCliOptions {
                    source,
                    destination,
                    granularity_nanos,
                }))
            }
            (None, _) => Err(MigrationCliParseError::MissingValue("--source".into())),
            (_, None) => Err(MigrationCliParseError::MissingValue("--destination".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MigrationCliCommand, MigrationCliProbe};

    #[test]
    fn parses_required_source_and_destination_options() {
        let command = MigrationCliProbe::parse_args_os([
            "--source",
            "legacy-directory",
            "--destination",
            "v1-directory",
        ])
        .expect("valid required migration options must parse");

        let MigrationCliCommand::Migrate(options) = command;
        assert_eq!(options.source, std::path::PathBuf::from("legacy-directory"));
        assert_eq!(
            options.destination,
            std::path::PathBuf::from("v1-directory")
        );
        assert_eq!(options.granularity_nanos, 60_000_000_000);
    }

    #[test]
    fn parses_only_nonzero_u64_optional_granularity() {
        let command = MigrationCliProbe::parse_args_os([
            "--source",
            "legacy-directory",
            "--timestamp-granularity-nanos",
            "123456789",
            "--destination",
            "v1-directory",
        ])
        .expect("valid selected granularity must parse");
        let MigrationCliCommand::Migrate(options) = command;
        assert_eq!(options.granularity_nanos, 123_456_789);

        for invalid in ["0", "not-a-number", "18446744073709551616"] {
            let error = MigrationCliProbe::parse_args_os([
                "--source",
                "legacy-directory",
                "--destination",
                "v1-directory",
                "--timestamp-granularity-nanos",
                invalid,
            ])
            .expect_err("granularity must be a nonzero u64");
            assert_eq!(
                error,
                super::MigrationCliParseError::InvalidGranularity,
                "{invalid}"
            );
        }
    }

    #[test]
    fn rejects_unknown_option_with_exact_os_string() {
        let error = MigrationCliProbe::parse_args_os([
            "--source",
            "legacy-directory",
            "--mystery",
            "value",
            "--destination",
            "v1-directory",
        ])
        .expect_err("unknown migration options must be rejected");

        assert_eq!(
            error,
            super::MigrationCliParseError::UnknownOption("--mystery".into())
        );
    }

    #[test]
    fn rejects_duplicate_option_without_last_value_winning() {
        let error = MigrationCliProbe::parse_args_os([
            "--source",
            "first-source",
            "--destination",
            "v1-directory",
            "--source",
            "second-source",
        ])
        .expect_err("duplicate migration options must be rejected");

        assert_eq!(
            error,
            super::MigrationCliParseError::DuplicateOption("--source".into())
        );
    }

    #[test]
    fn rejects_missing_option_values_at_end_or_before_another_option() {
        for (args, missing) in [
            (vec!["--source"], "--source"),
            (
                vec!["--source", "--destination", "v1-directory"],
                "--source",
            ),
            (vec!["--source", "legacy-directory"], "--destination"),
            (Vec::new(), "--source"),
        ] {
            let error = MigrationCliProbe::parse_args_os(args)
                .expect_err("every value-taking option must have a value");
            assert_eq!(
                error,
                super::MigrationCliParseError::MissingValue(missing.into())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn preserves_non_utf8_source_and_destination_paths() {
        use std::os::unix::ffi::OsStringExt;

        let source = std::ffi::OsString::from_vec(b"legacy-\xff".to_vec());
        let destination = std::ffi::OsString::from_vec(b"v1-\xfe".to_vec());
        let command = MigrationCliProbe::parse_args_os([
            std::ffi::OsString::from("--source"),
            source.clone(),
            std::ffi::OsString::from("--destination"),
            destination.clone(),
        ])
        .expect("OS-native non-UTF-8 paths must parse without conversion");

        let MigrationCliCommand::Migrate(options) = command;
        assert_eq!(options.source.as_os_str(), source);
        assert_eq!(options.destination.as_os_str(), destination);
    }

    #[test]
    fn help_returns_documented_output_without_filesystem_mutation() {
        let root = tempfile::tempdir().unwrap();
        let sentinel = root.path().join("sentinel");
        std::fs::write(&sentinel, b"unchanged").unwrap();

        let result = MigrationCliProbe::run_args_os(["--help"]);

        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.starts_with("Usage: pigment-db-migrate"));
        assert!(result.stdout.contains("--timestamp-granularity-nanos"));
        assert!(result.stderr.is_empty());
        assert_eq!(std::fs::read(sentinel).unwrap(), b"unchanged");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn version_returns_package_version_without_filesystem_mutation() {
        let root = tempfile::tempdir().unwrap();
        let sentinel = root.path().join("sentinel");
        std::fs::write(&sentinel, b"unchanged").unwrap();

        let result = MigrationCliProbe::run_args_os(["--version"]);

        assert_eq!(result.exit_code, 0);
        assert_eq!(
            result.stdout,
            format!("pigment-db-migrate {}\n", env!("CARGO_PKG_VERSION"))
        );
        assert!(result.stderr.is_empty());
        assert_eq!(std::fs::read(sentinel).unwrap(), b"unchanged");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn successful_migration_emits_one_final_summary_only() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("kv.wal.dat"), []).unwrap();
        let destination = root.path().join("destination");

        let result = MigrationCliProbe::run_args_os([
            std::ffi::OsString::from("--source"),
            source.as_os_str().to_owned(),
            std::ffi::OsString::from("--destination"),
            destination.as_os_str().to_owned(),
        ]);

        assert_eq!(result.exit_code, 0);
        assert_eq!(
            result.stdout,
            format!(
                "migrated 1 family(s), 0 entries, 64 bytes to {}\n",
                destination.display()
            )
        );
        assert_eq!(result.stdout.lines().count(), 1);
        assert!(result.stderr.is_empty());
        assert!(destination.join("kv.wal.dat").is_file());
    }

    #[test]
    fn maps_internal_outcomes_to_deterministic_exit_codes_and_diagnostics() {
        use crate::migration::{HandledMigrationFailure, MigrationCheckpoint, MigrationFailure};

        let cases = [
            (MigrationCheckpoint::InitialSourceRead, "unavailable", 3),
            (MigrationCheckpoint::FinalSourceRead, "reread failed", 3),
            (MigrationCheckpoint::Preflight, "corrupt legacy", 4),
            (
                MigrationCheckpoint::DestinationInspection,
                "migration destination already exists",
                5,
            ),
            (MigrationCheckpoint::CreateDestination, "create failed", 6),
            (MigrationCheckpoint::WriteOutput, "write failed", 6),
            (MigrationCheckpoint::Cleanup, "cleanup failed", 6),
            (MigrationCheckpoint::SourceChanged, "source changed", 7),
            (
                MigrationCheckpoint::DestinationInspection,
                "destination must be outside the source directory",
                2,
            ),
        ];
        for (checkpoint, detail, expected_exit) in cases {
            let result = MigrationCliProbe::render_migration_failure(HandledMigrationFailure {
                original: MigrationFailure {
                    checkpoint,
                    path: std::path::PathBuf::from("diagnostic-path"),
                    detail: detail.to_owned(),
                },
                cleanup: None,
            });
            assert_eq!(result.exit_code, expected_exit, "{checkpoint:?}");
            assert!(result.stdout.is_empty(), "{checkpoint:?}");
            assert!(result.stderr.starts_with("error:"), "{checkpoint:?}");
            assert!(result.stderr.contains("diagnostic-path"), "{checkpoint:?}");
            assert!(result.stderr.contains(detail), "{checkpoint:?}");
        }

        let usage = MigrationCliProbe::run_args_os(["--unknown", "value"]);
        assert_eq!(usage.exit_code, 2);
        assert!(usage.stdout.is_empty());
        assert_eq!(usage.stderr.matches("Usage:").count(), 1);
    }
}
