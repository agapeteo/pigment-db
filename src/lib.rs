pub mod key_map_store;
pub mod key_set_store;
pub mod key_value_store;
pub mod model;
pub mod recovery;
pub use recovery::{RecoveryError, RecoveryOperation, RecoveryOutcome, RecoveryStatus};
mod config;
pub use config::{DurableStoreOptions, TimestampGranularity, TimestampGranularityError};
mod wal;

mod migration;

mod migration_cli;

/// Internal executable bridge for the supported offline migration CLI.
#[doc(hidden)]
pub fn __run_migration_cli() -> i32 {
    use std::io::Write;

    let result = migration_cli::MigrationCliProbe::run_args_os(std::env::args_os().skip(1));
    let mut stdout = std::io::stdout().lock();
    let mut stderr = std::io::stderr().lock();
    if stdout.write_all(result.stdout.as_bytes()).is_err()
        || stderr.write_all(result.stderr.as_bytes()).is_err()
    {
        return 6;
    }
    result.exit_code
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
