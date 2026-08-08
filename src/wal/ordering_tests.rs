//! WAL ordering, rollback, and fail-closed unit tests.

use std::fs::OpenOptions;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use super::format::{V2CodecProbe, V2HeaderProbeFields};
use super::model::{KeyValueData, StoredAction};
use super::WalStorage;
use crate::test_support::fault_writer::{rollback_scripted, ScriptedWriter, WriterFault};

fn v2_file_wal() -> (tempfile::TempDir, WalStorage<std::fs::File>) {
    let directory = tempfile::tempdir().expect("temporary WAL directory");
    let path = directory.path().join("store.data");
    std::fs::write(
        &path,
        V2CodecProbe::encode_header(V2HeaderProbeFields {
            kind: 1,
            granularity_nanos: 60_000_000_000,
            base_bucket: 0,
            segment_id: 0,
            segment_base: 0,
        }),
    )
    .expect("V2 header");
    let file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("appendable V2 WAL");
    (
        directory,
        WalStorage::from_prepared_file_v2_with_timestamp_state(
            file,
            V2CodecProbe::HEADER_LEN as u64,
            60_000_000_000,
            0,
        ),
    )
}

#[test]
fn slow_v2_action_preparation_does_not_block_another_append() {
    let (_directory, wal) = v2_file_wal();
    let wal = Arc::new(wal);
    let (preparation_started_tx, preparation_started_rx) = mpsc::channel();
    let (release_preparation_tx, release_preparation_rx) = mpsc::channel();

    let slow_wal = Arc::clone(&wal);
    let slow = std::thread::spawn(move || {
        slow_wal.try_accept_action(|offset| {
            preparation_started_tx
                .send(())
                .expect("announce slow action preparation");
            release_preparation_rx
                .recv()
                .expect("release slow action preparation");
            StoredAction::put_action(
                offset,
                &KeyValueData::new(b"slow".to_vec(), b"value".to_vec()),
            )
        })
    });
    preparation_started_rx
        .recv()
        .expect("slow action reached preparation");

    let (fast_done_tx, fast_done_rx) = mpsc::channel();
    let fast_wal = Arc::clone(&wal);
    let fast = std::thread::spawn(move || {
        fast_done_tx
            .send(fast_wal.try_store_put_event(b"fast".to_vec(), b"value".to_vec()))
            .expect("report fast append");
    });

    let fast_result = fast_done_rx.recv_timeout(Duration::from_millis(500));
    release_preparation_tx
        .send(())
        .expect("release slow action after progress observation");
    let slow_result = slow.join().expect("slow append thread");
    fast.join().expect("fast append thread");

    assert!(
        fast_result.is_ok(),
        "an action that is still being prepared must not own the exclusive WAL append lock"
    );
    fast_result.unwrap().expect("fast append must be accepted");
    slow_result.expect("slow append must be accepted");
}

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
