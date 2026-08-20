//! Embedded concurrent stores with write-ahead-log recovery.
//!
//! Existing constructors and default options retain buffered write-plus-flush
//! acknowledgements. File-backed stores can opt into [`DurabilityPolicy::Physical`]
//! for direct per-mutation persistence barriers and crash-safe startup publication.
//! The policy is selected per opening and is not encoded in WAL bytes.

pub mod key_map_store;
pub mod key_set_store;
pub mod key_value_store;
pub mod model;
pub mod recovery;
pub use recovery::{RecoveryError, RecoveryOperation, RecoveryOutcome, RecoveryStatus};
mod compaction;
mod config;
mod durability;
mod maintenance;
mod maintenance_coordination;
pub use config::{
    DurabilityPolicy, DurableStoreOptions, TimestampGranularity, TimestampGranularityError,
    WalSegmentSize, WalSegmentSizeError,
};
pub use durability::{DurabilityCapability, DurabilitySupportError};
pub use maintenance::{
    inspect_storage, CleanupStatus, ClosedCompactionOptions, CompactionError, CompactionOperation,
    DirectoryCompactionOutcome, DirectoryStorageStats, FamilyCompactionOutcome, FamilyStorageStats,
    OnlineCompactionOptions, StoreFamily,
};
mod wal;
pub use wal::{MutationFailure, PersistenceOperation};

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
    fn private_maintenance_skeleton_is_linked() {
        super::maintenance::test_sentinel();
    }

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
