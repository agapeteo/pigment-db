//! Conditional-key behavior; private probes observe persistence, not application identity.

use super::{ConditionalAction, ConditionalResult, DurableKeyValueStore};

#[test]
fn conditional_key_keep_absent_does_not_manufacture_a_value() {
    let store = DurableKeyValueStore::new_vec_based();
    let result = store
        .try_compare_exchange_one(b"account".to_vec(), None, ConditionalAction::Keep)
        .unwrap();
    assert_eq!(store.get(b"account"), None);
    assert_eq!(store.size(), 0);
    assert_eq!(result, ConditionalResult::Unchanged);
}

#[test]
fn conditional_key_equal_put_is_unchanged_without_a_new_wal_event() {
    let store = DurableKeyValueStore::new_vec_based();
    store.put(b"account".to_vec(), b"same".to_vec());
    let before = store.online_wal_state_probe().offset;
    let result = store
        .try_compare_exchange_one(
            b"account".to_vec(),
            Some(b"same"),
            ConditionalAction::Put(b"same".to_vec()),
        )
        .unwrap();
    assert_eq!(store.online_wal_state_probe().offset, before);
    assert_eq!(result, ConditionalResult::Unchanged);
}

#[test]
fn conditional_key_delete_absent_is_unchanged_without_a_delete_frame() {
    let store = DurableKeyValueStore::new_vec_based();
    let before = store.online_wal_state_probe();
    let result = store
        .try_compare_exchange_one(b"missing".to_vec(), None, ConditionalAction::Delete)
        .unwrap();
    assert_eq!(store.online_wal_state_probe(), before);
    assert_eq!(store.get(b"missing"), None);
    assert_eq!(result, ConditionalResult::Unchanged);
}

#[test]
fn conditional_key_matching_keep_refuses_prior_unconfirmed_wal() {
    use crate::test_support::fault_writer::{
        rollback_scripted, sync_all_scripted, sync_data_scripted, ScriptedWriter, WriterFault,
    };
    use crate::test_support::mutation_schedule::MutationObserver;
    use crate::wal::{format::V1CodecProbe, WalStorage};
    use crate::MutationFailure;

    let (writer, _) = ScriptedWriter::scripted_with_bytes(
        Some(WriterFault::PartialWriteCall {
            call: 1,
            written: 8,
        }),
        true,
        None,
        V1CodecProbe::encode_header().to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyValueStore::from_probe_parts([], wal, MutationObserver::default());
    let failed = store
        .try_compare_exchange_one(
            b"account".to_vec(),
            None,
            ConditionalAction::Put(b"candidate".to_vec()),
        )
        .unwrap_err();
    assert!(matches!(
        MutationFailure::from_io_error(&failed),
        Some(MutationFailure::Indeterminate { .. })
    ));
    assert_eq!(store.get(b"account"), None);

    let refused = store
        .try_compare_exchange_one(b"account".to_vec(), None, ConditionalAction::Keep)
        .unwrap_err();
    assert!(matches!(
        MutationFailure::from_io_error(&refused),
        Some(MutationFailure::FailedClosed { .. })
    ));
    assert_eq!(store.get(b"account"), None);
}

#[test]
fn conditional_key_exact_match_matrix_on_legacy_and_v1_memory() {
    use crate::DurableStoreOptions;
    for store in [
        DurableKeyValueStore::new_vec_based(),
        DurableKeyValueStore::try_new_vec_based_with_options(DurableStoreOptions::default())
            .unwrap(),
    ] {
        let initial = store.online_wal_state_probe();
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"key".to_vec(),
                    Some(b""),
                    ConditionalAction::Put(b"bad".to_vec())
                )
                .unwrap(),
            ConditionalResult::Conflict
        );
        assert_eq!(store.online_wal_state_probe(), initial);
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), None, ConditionalAction::Put(vec![]))
                .unwrap(),
            ConditionalResult::Applied
        );
        assert_eq!(store.get(b"key"), Some(vec![]));
        let empty = store.online_wal_state_probe();
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), None, ConditionalAction::Delete)
                .unwrap(),
            ConditionalResult::Conflict
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"key".to_vec(), Some(b""), ConditionalAction::Keep)
                .unwrap(),
            ConditionalResult::Unchanged
        );
        assert_eq!(store.online_wal_state_probe(), empty);
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"key".to_vec(),
                    Some(b""),
                    ConditionalAction::Put(b"changed".to_vec())
                )
                .unwrap(),
            ConditionalResult::Applied
        );
        let changed = store.online_wal_state_probe();
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"key".to_vec(),
                    Some(b""),
                    ConditionalAction::Put(b"stale".to_vec())
                )
                .unwrap(),
            ConditionalResult::Conflict
        );
        assert_eq!(store.online_wal_state_probe(), changed);
        assert_eq!(store.get(b"key"), Some(b"changed".to_vec()));
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"key".to_vec(),
                    Some(b"changed"),
                    ConditionalAction::Delete
                )
                .unwrap(),
            ConditionalResult::Applied
        );
        assert_eq!(store.get(b"key"), None);
        assert_eq!(store.size(), 0);
    }
}

#[test]
fn conditional_key_legacy_batch_capability_refusal_is_not_a_single_event_limit() {
    use super::CompareExchangeEntry;
    let store = DurableKeyValueStore::new_vec_based();
    let old_batch = store
        .try_compare_exchange_batch(&[CompareExchangeEntry {
            key: b"key".to_vec(),
            expected: None,
            replacement: Some(b"value".to_vec()),
        }])
        .unwrap_err();
    assert_eq!(old_batch.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(store.get(b"key"), None);
    assert_eq!(
        store
            .try_compare_exchange_one(
                b"key".to_vec(),
                None,
                ConditionalAction::Put(b"value".to_vec())
            )
            .unwrap(),
        ConditionalResult::Applied
    );
    assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
}

#[test]
fn conditional_key_only_one_absent_creation_wins_same_key_race() {
    use std::sync::{Arc, Barrier};
    let store = Arc::new(DurableKeyValueStore::new_vec_based());
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0_u8..8)
        .map(|value| {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                (
                    value,
                    store
                        .try_compare_exchange_one(
                            b"one".to_vec(),
                            None,
                            ConditionalAction::Put(vec![value]),
                        )
                        .unwrap(),
                )
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    let winners: Vec<_> = results
        .iter()
        .filter(|(_, result)| *result == ConditionalResult::Applied)
        .collect();
    assert_eq!(winners.len(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|(_, result)| *result == ConditionalResult::Conflict)
            .count(),
        7
    );
    assert_eq!(store.get(b"one"), Some(vec![winners[0].0]));
}

#[test]
fn conditional_key_original_put_compute_remove_still_append() {
    let store = DurableKeyValueStore::new_vec_based();
    store.put(b"key".to_vec(), b"same".to_vec());
    let first = store.online_wal_state_probe().offset;
    store.put(b"key".to_vec(), b"same".to_vec());
    let second = store.online_wal_state_probe().offset;
    assert!(second > first);
    store.compute(b"key".to_vec(), |current| current.unwrap().to_vec());
    let third = store.online_wal_state_probe().offset;
    assert!(third > second);
    store.remove(b"absent");
    assert!(store.online_wal_state_probe().offset > third);
}

#[test]
fn conditional_key_noop_refuses_poisoned_health_without_a_success_or_panic() {
    let store = DurableKeyValueStore::new_vec_based();
    store.put(b"key".to_vec(), b"same".to_vec());
    store.wal.poison_conditional_health_probe();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.try_compare_exchange_one(b"key".to_vec(), Some(b"same"), ConditionalAction::Keep)
    }))
    .expect("a poisoned health check must return an error, not panic");
    let error = result.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert!(
        crate::MutationFailure::from_io_error(&error).is_none(),
        "no invented persistence certainty"
    );
    assert_eq!(store.get(b"key"), Some(b"same".to_vec()));
}

#[test]
fn conditional_key_v2_changed_events_use_barrier_but_noops_and_conflicts_do_not() {
    use crate::test_support::fault_writer::{
        rollback_scripted, sync_all_scripted, sync_data_scripted, ScriptedWriter,
    };
    use crate::test_support::mutation_schedule::MutationObserver;
    use crate::wal::{
        format::{V2CodecProbe, V2HeaderProbeFields},
        WalStorage,
    };
    let header = V2CodecProbe::encode_header(V2HeaderProbeFields {
        kind: 1,
        granularity_nanos: 60_000_000_000,
        base_bucket: 0,
        segment_id: 0,
        segment_base: 0,
    })
    .to_vec();
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(None, false, None, header);
    let wal = WalStorage::new_v2_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = DurableKeyValueStore::from_probe_parts([], wal, MutationObserver::default());
    assert_eq!(
        store
            .try_compare_exchange_one(
                b"key".to_vec(),
                None,
                ConditionalAction::Put(b"value".to_vec())
            )
            .unwrap(),
        ConditionalResult::Applied
    );
    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(handle.bytes(), handle.durable_bytes());
    let before = store.online_wal_state_probe();
    let events = handle.events();
    assert_eq!(
        store
            .try_compare_exchange_one(b"key".to_vec(), Some(b"value"), ConditionalAction::Keep)
            .unwrap(),
        ConditionalResult::Unchanged
    );
    assert_eq!(
        store
            .try_compare_exchange_one(
                b"key".to_vec(),
                Some(b"value"),
                ConditionalAction::Put(b"value".to_vec())
            )
            .unwrap(),
        ConditionalResult::Unchanged
    );
    assert_eq!(
        store
            .try_compare_exchange_one(b"absent".to_vec(), None, ConditionalAction::Delete)
            .unwrap(),
        ConditionalResult::Unchanged
    );
    assert_eq!(
        store
            .try_compare_exchange_one(b"key".to_vec(), Some(b"stale"), ConditionalAction::Delete)
            .unwrap(),
        ConditionalResult::Conflict
    );
    assert_eq!(store.online_wal_state_probe(), before);
    assert_eq!(handle.events(), events);
    assert_eq!(handle.data_barrier_calls(), 1);
    assert_eq!(
        store
            .try_compare_exchange_one(b"key".to_vec(), Some(b"value"), ConditionalAction::Delete)
            .unwrap(),
        ConditionalResult::Applied
    );
    assert_eq!(handle.data_barrier_calls(), 2);
    assert_eq!(store.get(b"key"), None);
}

#[test]
fn conditional_key_failed_delete_never_publishes_and_preserves_uncertainty() {
    use crate::test_support::fault_writer::{
        rollback_scripted, sync_all_scripted, sync_data_scripted, ScriptedWriter, WriterFault,
    };
    use crate::test_support::mutation_schedule::MutationObserver;
    use crate::wal::{format::V1CodecProbe, WalStorage};
    use crate::MutationFailure;
    for fault in [
        WriterFault::WriteCall(2),
        WriterFault::PartialWriteCall {
            call: 2,
            written: 8,
        },
        WriterFault::FlushCall(2),
        WriterFault::DataBarrierCall(2),
    ] {
        for rollback_fails in [false, true] {
            let (writer, handle) = ScriptedWriter::scripted_with_bytes(
                Some(fault),
                rollback_fails,
                None,
                V1CodecProbe::encode_header().to_vec(),
            );
            let wal = WalStorage::new_v1_with_physical_probe(
                writer,
                rollback_scripted,
                sync_data_scripted,
            );
            wal.install_rollback_barrier_probe(sync_all_scripted);
            let store =
                DurableKeyValueStore::from_probe_parts([], wal, MutationObserver::default());
            assert_eq!(
                store
                    .try_compare_exchange_one(
                        b"key".to_vec(),
                        None,
                        ConditionalAction::Put(b"old".to_vec())
                    )
                    .unwrap(),
                ConditionalResult::Applied
            );
            let before = handle.bytes();
            let failed = store
                .try_compare_exchange_one(b"key".to_vec(), Some(b"old"), ConditionalAction::Delete)
                .unwrap_err();
            assert_eq!(store.get(b"key"), Some(b"old".to_vec()));
            if rollback_fails {
                assert!(matches!(
                    MutationFailure::from_io_error(&failed),
                    Some(MutationFailure::Indeterminate { .. })
                ));
                let closed = store
                    .try_compare_exchange_one(
                        b"key".to_vec(),
                        Some(b"old"),
                        ConditionalAction::Keep,
                    )
                    .unwrap_err();
                assert!(matches!(
                    MutationFailure::from_io_error(&closed),
                    Some(MutationFailure::FailedClosed { .. })
                ));
                let events = handle.events();
                assert_eq!(
                    store
                        .try_compare_exchange_one(b"key".to_vec(), None, ConditionalAction::Delete)
                        .unwrap(),
                    ConditionalResult::Conflict
                );
                assert_eq!(
                    handle.events(),
                    events,
                    "conflict is no-I/O observation, not recovery"
                );
            } else {
                assert!(matches!(
                    MutationFailure::from_io_error(&failed),
                    Some(MutationFailure::Rejected { .. })
                ));
                assert_eq!(handle.bytes(), before);
                assert_eq!(
                    store
                        .try_compare_exchange_one(
                            b"key".to_vec(),
                            Some(b"old"),
                            ConditionalAction::Delete
                        )
                        .unwrap(),
                    ConditionalResult::Applied
                );
                assert_eq!(store.get(b"key"), None);
            }
        }
    }
}

#[test]
fn conditional_key_byte_match_does_not_detect_delete_recreate_aba() {
    let store = DurableKeyValueStore::new_vec_based();
    store
        .try_compare_exchange_one(
            b"account".to_vec(),
            None,
            ConditionalAction::Put(b"same bytes".to_vec()),
        )
        .unwrap();
    let snapshot = store.get(b"account").unwrap();
    store
        .try_compare_exchange_one(
            b"account".to_vec(),
            Some(&snapshot),
            ConditionalAction::Delete,
        )
        .unwrap();
    store
        .try_compare_exchange_one(
            b"account".to_vec(),
            None,
            ConditionalAction::Put(snapshot.clone()),
        )
        .unwrap();
    assert_eq!(
        store
            .try_compare_exchange_one(
                b"account".to_vec(),
                Some(&snapshot),
                ConditionalAction::Keep
            )
            .unwrap(),
        ConditionalResult::Unchanged
    );
}

#[test]
fn conditional_key_holds_existing_batch_gate_until_barrier_and_publication() {
    use super::{CompareExchangeEntry, CompareExchangeResult};
    use crate::test_support::fault_writer::{
        rollback_scripted, sync_all_scripted, sync_data_scripted, BarrierKind, ScriptedWriter,
    };
    use crate::test_support::mutation_schedule::{MutationObserver, WATCHDOG};
    use crate::wal::{format::V1CodecProbe, WalStorage};
    use std::sync::{mpsc, Arc};
    let (writer, handle) = ScriptedWriter::scripted_with_bytes(
        None,
        false,
        Some(BarrierKind::Data),
        V1CodecProbe::encode_header().to_vec(),
    );
    let wal = WalStorage::new_v1_with_physical_probe(writer, rollback_scripted, sync_data_scripted);
    wal.install_rollback_barrier_probe(sync_all_scripted);
    let store = Arc::new(DurableKeyValueStore::from_probe_parts(
        [],
        wal,
        MutationObserver::default(),
    ));
    let keys = crate::test_support::shard_keys::select_shard_keys(&store.store);
    let one_key = keys.anchor.clone();
    let batch_key = keys.different_shard.clone();
    let one_store = Arc::clone(&store);
    let worker_key = one_key.clone();
    let one = std::thread::spawn(move || {
        one_store.try_compare_exchange_one(
            worker_key,
            None,
            ConditionalAction::Put(b"value".to_vec()),
        )
    });
    handle.wait_until_barrier_blocked(BarrierKind::Data);
    // Sample before the batch exists: its own guard cannot mask a missing one-key guard.
    let conditional_holds_transaction_read = store.transaction.try_write().is_none();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let batch_store = Arc::clone(&store);
    let worker_batch_key = batch_key.clone();
    let batch = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = batch_store.try_compare_exchange_batch(&[CompareExchangeEntry {
            key: worker_batch_key,
            expected: Some(b"not-present".to_vec()),
            replacement: Some(b"after".to_vec()),
        }]);
        done_tx.send(()).unwrap();
        result
    });
    started_rx.recv_timeout(WATCHDOG).unwrap();
    let completion_before_release = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    handle.release_barrier();
    assert_eq!(one.join().unwrap().unwrap(), ConditionalResult::Applied);
    if matches!(
        completion_before_release,
        Err(mpsc::RecvTimeoutError::Timeout)
    ) {
        done_rx.recv_timeout(WATCHDOG).unwrap();
    }
    assert_eq!(
        batch.join().unwrap().unwrap(),
        CompareExchangeResult::Conflict
    );
    assert!(
        conditional_holds_transaction_read,
        "conditional operation lost its transaction read guard"
    );
    assert!(
        matches!(
            completion_before_release,
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "mismatching different-shard batch did not wait for the conditional guard"
    );
    assert_eq!(store.get(&one_key), Some(b"value".to_vec()));
    assert_eq!(store.get(&batch_key), None);
}

#[test]
fn conditional_key_changed_events_replay_in_legacy_v1_and_v2_formats() {
    use crate::test_support::fault_writer::{
        rollback_scripted, sync_data_scripted, ScriptedWriter,
    };
    use crate::test_support::mutation_schedule::MutationObserver;
    use crate::wal::{
        format::{V1CodecProbe, V2CodecProbe, V2HeaderProbeFields},
        replay::replay_key_value,
        WalStorage,
    };
    for format in 0..3 {
        let header = match format {
            0 => Vec::new(),
            1 => V1CodecProbe::encode_header().to_vec(),
            _ => V2CodecProbe::encode_header(V2HeaderProbeFields {
                kind: 1,
                granularity_nanos: 60_000_000_000,
                base_bucket: 0,
                segment_id: 0,
                segment_base: 0,
            })
            .to_vec(),
        };
        let (writer, handle) = ScriptedWriter::scripted_with_bytes(None, false, None, header);
        let wal = match format {
            0 => WalStorage::new_with_rollback(writer, rollback_scripted),
            1 => WalStorage::new_v1_with_rollback(writer, rollback_scripted),
            _ => WalStorage::new_v2_with_physical_probe(
                writer,
                rollback_scripted,
                sync_data_scripted,
            ),
        };
        let store = DurableKeyValueStore::from_probe_parts([], wal, MutationObserver::default());
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"a".to_vec(),
                    None,
                    ConditionalAction::Put(b"one".to_vec())
                )
                .unwrap(),
            ConditionalResult::Applied
        );
        assert_eq!(
            store
                .try_compare_exchange_one(
                    b"a".to_vec(),
                    Some(b"one"),
                    ConditionalAction::Put(b"two".to_vec())
                )
                .unwrap(),
            ConditionalResult::Applied
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"b".to_vec(), None, ConditionalAction::Put(vec![]))
                .unwrap(),
            ConditionalResult::Applied
        );
        assert_eq!(
            store
                .try_compare_exchange_one(b"a".to_vec(), Some(b"two"), ConditionalAction::Delete)
                .unwrap(),
            ConditionalResult::Applied
        );
        let replay = replay_key_value(&handle.bytes()).unwrap();
        assert_eq!(replay.snapshot.len(), 1);
        assert_eq!(replay.snapshot.get(b"b".as_slice()), Some(&vec![]));
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), Some(vec![]));
    }
}
