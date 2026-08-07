//! WAL ordering, rollback, and fail-closed unit tests.

use super::WalStorage;
use crate::test_support::fault_writer::{rollback_scripted, ScriptedWriter, WriterFault};

#[test]
fn borrowed_set_append_emits_the_identical_legacy_frame() {
    let owned = WalStorage::new_vec_based();
    owned
        .try_store_append_to_set_event(b"key".to_vec(), b"member".to_vec())
        .expect("owned append must be accepted");

    let borrowed = WalStorage::new_vec_based();
    borrowed
        .try_store_append_to_set_event_borrowed(b"key", b"member".to_vec())
        .expect("borrowed append must be accepted");

    assert_eq!(
        borrowed.wal_state.read().unwrap().writer,
        owned.wal_state.read().unwrap().writer,
        "borrowed-key optimization must not change persisted bytes"
    );
}

/// CMO-FAIL-1: a write error after earlier record segments restores the checkpoint.
#[test]
fn partial_record_write_error_rolls_back_and_unlocks_wal() {
    let (writer, handle) = ScriptedWriter::new(WriterFault::WriteCall(8), false);
    let wal = WalStorage::new_with_rollback(writer, rollback_scripted);
    wal.try_store_put_event(b"seed".to_vec(), b"before".to_vec())
        .expect("seed action must be accepted");
    let checkpoint_bytes = handle.bytes();
    let checkpoint_offset = wal.offset();

    let error = wal
        .try_store_put_event(b"rejected".to_vec(), b"after".to_vec())
        .expect_err("partial record must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(
        handle.bytes(),
        checkpoint_bytes,
        "rejected record bytes must roll back exactly"
    );
    assert_eq!(wal.offset(), checkpoint_offset);

    wal.try_store_put_event(b"later".to_vec(), b"progress".to_vec())
        .expect("WAL lock must be released after rejection");
    assert!(handle.bytes().len() > checkpoint_bytes.len());
}

/// CMO-FAIL-2: a flush error restores bytes and offset before returning.
#[test]
fn flush_error_rolls_back_and_unlocks_wal() {
    let (writer, handle) = ScriptedWriter::new(WriterFault::FlushCall(2), false);
    let wal = WalStorage::new_with_rollback(writer, rollback_scripted);
    wal.try_store_put_event(b"seed".to_vec(), b"before".to_vec())
        .expect("seed action must be accepted");
    let checkpoint_bytes = handle.bytes();
    let checkpoint_offset = wal.offset();

    let error = wal
        .try_store_put_event(b"rejected".to_vec(), b"after".to_vec())
        .expect_err("flush must be rejected");
    assert!(error.to_string().contains("flush rejection"));
    assert_eq!(handle.bytes(), checkpoint_bytes);
    assert_eq!(wal.offset(), checkpoint_offset);
    assert_eq!(handle.flush_calls(), 2);

    wal.try_store_put_event(b"later".to_vec(), b"progress".to_vec())
        .expect("WAL lock must be released after flush rejection");
    assert_eq!(handle.flush_calls(), 3);
}

/// CMO-FAIL-3: rollback failure permanently fails closed before writer access.
#[test]
fn rollback_failure_is_composite_and_future_writes_fail_closed() {
    let (writer, handle) = ScriptedWriter::new(WriterFault::WriteCall(3), true);
    let wal = WalStorage::new_with_rollback(writer, rollback_scripted);

    let error = wal
        .try_store_put_event(b"rejected".to_vec(), b"value".to_vec())
        .expect_err("write and rollback must fail");
    let message = error.to_string();
    assert!(message.contains("scripted write rejection"));
    assert!(message.contains("scripted rollback rejection"));
    let calls_after_failure = handle.write_calls();

    let later = wal
        .try_store_put_event(b"later".to_vec(), b"value".to_vec())
        .expect_err("failed WAL must reject later mutation");
    assert!(later.to_string().contains("rollback"));
    assert_eq!(
        handle.write_calls(),
        calls_after_failure,
        "fail-closed WAL must not call the writer again"
    );
}
