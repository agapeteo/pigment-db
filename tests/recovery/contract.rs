use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::recovery::{RecoveryError, RecoveryOperation, RecoveryOutcome, RecoveryStatus};

fn assert_borrowing_accessors<S>(outcome: &RecoveryOutcome<S>) {
    let _: RecoveryStatus = outcome.status();
    let _: &S = outcome.store();
}

fn assert_consuming_accessors<S>(outcome: RecoveryOutcome<S>) {
    let _: (S, RecoveryStatus) = outcome.into_parts();
}

fn assert_into_store<S>(outcome: RecoveryOutcome<S>) {
    let _: S = outcome.into_store();
}

#[test]
fn public_recovery_contract_is_structured_and_compatible() {
    assert_eq!(RecoveryStatus::Normal, RecoveryStatus::Normal);
    assert_ne!(RecoveryStatus::Normal, RecoveryStatus::Recovered);

    let active = PathBuf::from("kv.wal.dat");
    let recovery = PathBuf::from(".kv.wal.dat");
    let conflict = RecoveryError::AuthorityUndetermined {
        active_path: Some(active.clone()),
        recovery_path: Some(recovery.clone()),
    };
    match conflict {
        RecoveryError::AuthorityUndetermined {
            active_path,
            recovery_path,
        } => {
            assert_eq!(active_path, Some(active));
            assert_eq!(recovery_path, Some(recovery));
        }
        other => panic!("unexpected error: {other}"),
    }

    let io_error = RecoveryError::Io {
        operation: RecoveryOperation::Open,
        path: PathBuf::from("kv.wal.dat"),
        source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
    };
    assert!(io_error.source().is_some());
    assert!(io_error.to_string().contains("kv.wal.dat"));

    let _: fn(&str) -> DurableKeyValueStore<File> = DurableKeyValueStore::init_new;
    let _: fn(&str) -> DurableKeySetStore<File> = DurableKeySetStore::init_new;
    let _: fn(&str) -> DurableKeyMapStore<File> = DurableKeyMapStore::init_new;

    let _ = assert_borrowing_accessors::<DurableKeyValueStore<File>>;
    let _ = assert_consuming_accessors::<DurableKeyValueStore<File>>;
    let _ = assert_into_store::<DurableKeyValueStore<File>>;
}
