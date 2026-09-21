use super::*;
use std::sync::{mpsc, Arc};
use std::time::Duration;

fn entry(key: &[u8], expected: Option<&[u8]>, replacement: Option<&[u8]>) -> CompareExchangeEntry {
    CompareExchangeEntry {
        key: key.to_vec(),
        expected: expected.map(Vec::from),
        replacement: replacement.map(Vec::from),
    }
}

#[test]
fn batch_committed_during_online_compaction_is_one_delta_and_survives_cutover() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"book".to_vec(), b"old".to_vec());
    let capture = store
        .begin_online_capture_probe(
            u64::MAX,
            crate::test_support::maintenance_schedule::MaintenanceObserver::default(),
        )
        .unwrap();
    let staged = crate::compaction::prepare_online_staging(capture, |_| Ok(())).unwrap();
    store
        .try_compare_exchange_batch(&[
            entry(b"book", Some(b"old"), Some(b"new")),
            entry(b"usage", None, Some(b"1")),
            entry(b"receipt", None, Some(b"done")),
        ])
        .unwrap();
    assert_eq!(store.delta_group_count_probe(), 1);
    store.complete_online_cutover_probe(staged).unwrap();
    assert_eq!(store.get(b"book"), Some(b"new".to_vec()));
    drop(store);
    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(reopened.size(), 3);
    assert_eq!(reopened.get(b"receipt"), Some(b"done".to_vec()));
}

#[test]
fn competing_batches_have_one_winner_and_losers_write_nothing() {
    let store = Arc::new(DurableKeyValueStore::new_vec_based_with_options(
        DurableStoreOptions::default(),
    ));
    let barrier = Arc::new(std::sync::Barrier::new(9));
    let workers: Vec<_> = (0..8)
        .map(|i| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .try_compare_exchange_batch(&[
                        entry(b"book", None, Some(&[i])),
                        entry(&[i], None, Some(b"receipt")),
                    ])
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let applied = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .filter(|r| *r == CompareExchangeResult::Applied)
        .count();
    assert_eq!(applied, 1);
    assert_eq!(store.size(), 2);
}

#[test]
fn batch_wal_rejection_leaves_all_live_values_and_rolls_back_or_fails_closed() {
    use crate::test_support::fault_writer::{
        rollback_scripted, sync_all_scripted, sync_data_scripted, ScriptedWriter, WriterFault,
    };
    use crate::wal::format::V1CodecProbe;
    for fault in [
        WriterFault::WriteCall(1),
        WriterFault::PartialWriteCall {
            call: 1,
            written: 80,
        },
        WriterFault::FlushCall(1),
        WriterFault::DataBarrierCall(1),
    ] {
        for rollback_fails in [false, true] {
            let header = V1CodecProbe::encode_header().to_vec();
            let (writer, handle) = ScriptedWriter::scripted_with_bytes(
                Some(fault),
                rollback_fails,
                None,
                header.clone(),
            );
            let wal = WalStorage::new_v1_with_physical_probe(
                writer,
                rollback_scripted,
                sync_data_scripted,
            );
            wal.install_rollback_barrier_probe(sync_all_scripted);
            let store =
                DurableKeyValueStore::from_probe_parts([], wal, MutationObserver::default());
            let entries = [
                entry(b"book", None, Some(b"new")),
                entry(b"usage", None, Some(b"1")),
                entry(b"receipt", None, Some(b"done")),
            ];
            assert!(store.try_compare_exchange_batch(&entries).is_err());
            assert_eq!(store.size(), 0);
            if rollback_fails {
                assert!(store.try_put(b"later".to_vec(), vec![]).is_err());
            } else {
                assert_eq!(handle.bytes(), header);
                assert_eq!(
                    store.try_compare_exchange_batch(&entries).unwrap(),
                    CompareExchangeResult::Applied
                );
            }
        }
    }
}

#[test]
fn v1_every_cut_in_a_batch_recovers_no_partial_batch() {
    use crate::test_support::fault_writer::{rollback_scripted, ScriptedWriter};
    use crate::wal::{
        format::V1CodecProbe,
        replay::{replay_key_value, replay_key_value_tail, TailReplay},
    };
    let header = V1CodecProbe::encode_header().to_vec();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(None, false, None, header.clone());
    let store = DurableKeyValueStore::from_probe_parts(
        [],
        WalStorage::new_v1_with_rollback(writer, rollback_scripted),
        MutationObserver::default(),
    );
    let entries = [
        entry(b"book", None, Some(b"new")),
        entry(b"usage", None, Some(b"1")),
        entry(b"receipt", None, Some(b"done")),
    ];
    store.try_compare_exchange_batch(&entries).unwrap();
    let bytes = handle.bytes();
    for cut in header.len() + 1..bytes.len() {
        match replay_key_value_tail(&bytes[..cut]) {
            TailReplay::RecoverableTail { replay, .. } => {
                assert!(replay.snapshot.is_empty(), "cut {cut}")
            }
            other => panic!("cut {cut}: {other:?}"),
        }
    }
    assert_eq!(replay_key_value(&bytes).unwrap().snapshot.len(), 3);
    assert_eq!(handle.flush_calls(), 1);
}

#[test]
fn batch_rejects_invalid_bounds_and_legacy_without_mutating() {
    let store = DurableKeyValueStore::new_vec_based_with_options(DurableStoreOptions::default());
    for entries in [
        vec![],
        vec![entry(b"x", None, Some(b"a")), entry(b"x", None, Some(b"b"))],
        (0..17).map(|i| entry(&[i], None, Some(b"a"))).collect(),
    ] {
        assert_eq!(
            store
                .try_compare_exchange_batch(&entries)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(store.size(), 0);
    }
    let legacy = DurableKeyValueStore::new_vec_based();
    assert_eq!(
        legacy
            .try_compare_exchange_batch(&[entry(b"x", None, Some(b"a"))])
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(legacy.size(), 0);
}

#[test]
fn batch_holds_readers_and_ordinary_mutations_until_publication() {
    let mut store =
        DurableKeyValueStore::new_vec_based_with_options(DurableStoreOptions::default());
    store.put(b"book".to_vec(), b"old".to_vec());
    let (observer, gate) =
        MutationObserver::one_shot(b"book".to_vec(), MutationPhase::AcceptedBeforePublication);
    store.mutation_observer = observer;
    let store = Arc::new(store);
    let worker_store = store.clone();
    let batch = std::thread::spawn(move || {
        worker_store
            .try_compare_exchange_batch(&[
                entry(b"book", Some(b"old"), Some(b"new")),
                entry(b"receipt", None, Some(b"committed")),
            ])
            .unwrap()
    });
    gate.wait_until_reached();
    let (tx, rx) = mpsc::channel();
    let reader_store = store.clone();
    let reader_tx = tx.clone();
    let reader = std::thread::spawn(move || {
        let value = reader_store.get(b"book");
        reader_tx.send(()).unwrap();
        value
    });
    let writer_store = store.clone();
    let writer = std::thread::spawn(move || {
        writer_store.put(b"other".to_vec(), b"ordinary".to_vec());
        tx.send(()).unwrap();
    });
    let completed_early = rx.recv_timeout(Duration::from_millis(150)).is_ok();
    gate.release();
    assert_eq!(batch.join().unwrap(), CompareExchangeResult::Applied);
    let value = reader.join().unwrap();
    writer.join().unwrap();
    assert!(
        !completed_early,
        "a public operation bypassed the batch publication gate"
    );
    assert_eq!(value, Some(b"new".to_vec()));
}
