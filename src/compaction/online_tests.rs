//! Private online-compaction behavior tests.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::maintenance_coordination::MaintenanceCoordinator;
use crate::model::SearchKey;
use crate::test_support::maintenance_schedule::{MaintenanceCheckpoint, MaintenanceObserver};

#[test]
fn coordinator_is_constant_per_instance_exclusive_and_immediately_single_attempt() {
    assert!(std::mem::size_of::<MaintenanceCoordinator>() <= 64);
    let primary = Arc::new(MaintenanceCoordinator::default());
    let unrelated = Arc::new(MaintenanceCoordinator::default());

    let exclusive = primary.exclusive();
    let (acquired_tx, acquired_rx) = mpsc::sync_channel(0);
    let waiting = Arc::clone(&primary);
    let waiter = std::thread::spawn(move || {
        let _shared = waiting.shared();
        acquired_tx.send(()).unwrap();
    });
    assert!(acquired_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    let unrelated_shared = unrelated.shared();
    drop(unrelated_shared);
    drop(exclusive);
    acquired_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();

    let poisoned = Arc::clone(&primary);
    assert!(std::thread::spawn(move || {
        let _exclusive = poisoned.exclusive();
        panic!("scripted maintenance panic");
    })
    .join()
    .is_err());
    drop(primary.shared());

    let first = primary.try_begin_online().unwrap();
    assert_ne!(first.id(), 0);
    assert!(primary.try_begin_online().is_err());
    assert!(unrelated.try_begin_online().is_ok());
    drop(first);
    assert!(primary.try_begin_online().is_ok());

    let key_value = DurableKeyValueStore::new_vec_based();
    let unrelated_key_value = DurableKeyValueStore::new_vec_based();
    let key_set = DurableKeySetStore::new_vec_based();
    let key_map = DurableKeyMapStore::new_vec_based();
    let key_value_attempt = key_value.maintenance_probe().try_begin_online().unwrap();
    assert!(key_value.maintenance_probe().try_begin_online().is_err());
    assert!(unrelated_key_value
        .maintenance_probe()
        .try_begin_online()
        .is_ok());
    assert!(key_set.maintenance_probe().try_begin_online().is_ok());
    assert!(key_map.maintenance_probe().try_begin_online().is_ok());
    drop(key_value_attempt);
    assert!(key_value.maintenance_probe().try_begin_online().is_ok());
}

#[test]
fn reads_bypass_maintenance_and_post_publication_callbacks_hold_no_store_guard() {
    let key_value = Arc::new(DurableKeyValueStore::new_vec_based());
    key_value.put(b"value".to_vec(), 7_u64.to_ne_bytes().to_vec());
    let key_set = Arc::new(DurableKeySetStore::new_vec_based());
    key_set.append(b"set".to_vec(), b"member".to_vec());
    let key_map = Arc::new(DurableKeyMapStore::new_vec_based());
    key_map.put(b"map".to_vec(), SearchKey::from(1), b"element".to_vec());

    let value_exclusive = key_value.maintenance_probe().exclusive();
    let set_exclusive = key_set.maintenance_probe().exclusive();
    let map_exclusive = key_map.maintenance_probe().exclusive();
    let (read_tx, read_rx) = mpsc::channel();
    let value_reader = Arc::clone(&key_value);
    let value_tx = read_tx.clone();
    let value_read = std::thread::spawn(move || {
        assert_eq!(
            value_reader.get(b"value"),
            Some(7_u64.to_ne_bytes().to_vec())
        );
        assert_eq!(value_reader.read_number(b"value"), Some(Ok(7)));
        assert!(value_reader.contains(b"value"));
        assert_eq!(value_reader.size(), 1);
        value_tx.send(()).unwrap();
    });
    let set_reader = Arc::clone(&key_set);
    let set_tx = read_tx.clone();
    let set_read = std::thread::spawn(move || {
        assert!(set_reader.contains_key(b"set"));
        assert!(set_reader.contains_in_set(b"set", b"member"));
        assert!(set_reader.get_hashset(b"set").is_some());
        assert_eq!(set_reader.size(), 1);
        set_tx.send(()).unwrap();
    });
    let map_reader = Arc::clone(&key_map);
    let map_read = std::thread::spawn(move || {
        assert!(map_reader.contains_key(b"map"));
        assert!(map_reader.contains_search_key(b"map", &SearchKey::from(1)));
        assert!(map_reader.contains_in_map(b"map", &SearchKey::from(1)));
        assert_eq!(
            map_reader.get_element(b"map", &SearchKey::from(1)),
            Some(b"element".to_vec())
        );
        assert!(map_reader.get_sorted_map(b"map").is_some());
        assert_eq!(map_reader.first(b"map").unwrap().0, SearchKey::from(1));
        assert_eq!(map_reader.last(b"map").unwrap().0, SearchKey::from(1));
        assert_eq!(map_reader.sorted_map_size(b"map"), Some(1));
        assert_eq!(map_reader.size(), 1);
        read_tx.send(()).unwrap();
    });
    for _ in 0..3 {
        read_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    value_read.join().unwrap();
    set_read.join().unwrap();
    map_read.join().unwrap();
    drop(map_exclusive);
    drop(set_exclusive);
    drop(value_exclusive);

    assert_callback_releases_set_guards();
    assert_callback_releases_map_guards();
}

fn assert_callback_releases_set_guards() {
    let store = Arc::new(DurableKeySetStore::new_vec_based());
    store.append(b"callback-set".to_vec(), b"member".to_vec());
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let callback_store = Arc::clone(&store);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        worker_store.remove_from_set_callback(
            b"callback-set".to_vec(),
            b"member".to_vec(),
            move |_| {
                assert!(!callback_store.contains_key(b"callback-set"));
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            },
        );
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (exclusive_tx, exclusive_rx) = mpsc::sync_channel(0);
    let exclusive_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let _exclusive = exclusive_store.maintenance_probe().exclusive();
        exclusive_tx.send(()).unwrap();
    });
    let acquired_while_callback_paused = exclusive_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    if acquired_while_callback_paused.is_err() {
        exclusive_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    waiter.join().unwrap();
    assert!(acquired_while_callback_paused.is_ok());
}

fn assert_callback_releases_map_guards() {
    let store = Arc::new(DurableKeyMapStore::new_vec_based());
    store.put(
        b"callback-map".to_vec(),
        SearchKey::from(1),
        b"element".to_vec(),
    );
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let callback_store = Arc::clone(&store);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        worker_store.remove_from_sorted_map_callback(
            b"callback-map".to_vec(),
            SearchKey::from(1),
            move |_| {
                assert!(!callback_store.contains_key(b"callback-map"));
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            },
        );
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (exclusive_tx, exclusive_rx) = mpsc::sync_channel(0);
    let exclusive_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let _exclusive = exclusive_store.maintenance_probe().exclusive();
        exclusive_tx.send(()).unwrap();
    });
    let acquired_while_callback_paused = exclusive_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    if acquired_while_callback_paused.is_err() {
        exclusive_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    waiter.join().unwrap();
    assert!(acquired_while_callback_paused.is_ok());
}

#[test]
fn async_conflict_cancellation_and_callback_panic_leave_no_delta_or_coordination() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    let key = b"conflict".to_vec();
    store.append(key.clone(), b"seed".to_vec());
    let attempt = store.begin_online_probe(u64::MAX).unwrap();
    assert_ne!(attempt.token(), 0);
    assert!(store.begin_online_probe(u64::MAX).is_err());

    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let conflict_store = Arc::clone(&store);
    let conflict_key = key.clone();
    let conflict = std::thread::spawn(move || {
        block_on_online(
            conflict_store.try_compute_async(conflict_key, async move |working| {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                working.insert(b"stale-candidate".to_vec());
            }),
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    store.append(key.clone(), b"concurrent".to_vec());
    release_tx.send(()).unwrap();
    let conflict_error = conflict.join().unwrap().unwrap_err();
    assert_eq!(conflict_error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(store.delta_group_count_probe(), 1);

    store.try_compute(key.clone(), |_| {}).unwrap();
    assert_eq!(store.delta_group_count_probe(), 1);

    let callback_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store
            .try_compute(b"panic".to_vec(), |_| {
                panic!("scripted sync callback panic")
            })
            .unwrap();
    }));
    assert!(callback_panic.is_err());
    assert_eq!(store.delta_group_count_probe(), 1);
    drop(store.maintenance_probe().exclusive());

    {
        let future = store.try_compute_async(b"cancelled".to_vec(), async |working| {
            working.insert(b"discarded".to_vec());
            std::future::pending::<()>().await;
        });
        let mut future = pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }
    assert_eq!(store.delta_group_count_probe(), 1);
    drop(store.maintenance_probe().exclusive());

    let async_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_online(store.try_compute_async(b"async-panic".to_vec(), async |_| {
            panic!("scripted async callback panic");
        }))
    }));
    assert!(async_panic.is_err());
    assert_eq!(store.delta_group_count_probe(), 1);
    drop(store.maintenance_probe().exclusive());

    drop(attempt);
    assert!(!store.has_delta_recorder_probe());
    let next = store.begin_online_probe(u64::MAX).unwrap();
    drop(next);
    assert!(!store.has_delta_recorder_probe());
}

#[test]
fn online_prepared_capture_has_no_mutation_gap() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    store.put(b"captured".to_vec(), b"before".to_vec());

    let (observer, controller) = MaintenanceObserver::controlled([
        MaintenanceCheckpoint::SnapshotCapture,
        MaintenanceCheckpoint::RecorderActivation,
        MaintenanceCheckpoint::ManifestPrepared,
    ]);
    let (capture_tx, capture_rx) = mpsc::sync_channel(0);
    let (drop_tx, drop_rx) = mpsc::sync_channel(0);
    let worker_store = Arc::clone(&store);
    let capture_worker = std::thread::spawn(move || {
        let capture = worker_store
            .begin_online_capture_probe(u64::MAX, observer)
            .unwrap();
        capture_tx
            .send((
                capture.capture.clone(),
                capture.paths.clone(),
                capture.manifest.clone(),
            ))
            .unwrap();
        drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(capture);
    });

    controller.wait_until_reached(MaintenanceCheckpoint::SnapshotCapture);
    let (mutation_tx, mutation_rx) = mpsc::sync_channel(0);
    let mutation_store = Arc::clone(&store);
    let mutation = std::thread::spawn(move || {
        mutation_store
            .try_put(b"concurrent".to_vec(), b"after".to_vec())
            .unwrap();
        mutation_tx.send(()).unwrap();
    });
    assert!(mutation_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    controller.release(MaintenanceCheckpoint::SnapshotCapture);
    controller.wait_until_reached(MaintenanceCheckpoint::RecorderActivation);
    assert!(mutation_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    assert!(store.maintenance_probe().try_begin_online().is_err());

    controller.release(MaintenanceCheckpoint::RecorderActivation);
    controller.wait_until_reached(MaintenanceCheckpoint::ManifestPrepared);
    assert!(mutation_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    let active = directory.path().join("kv.wal.dat");
    let expected_paths = crate::compaction::publication::family_artifact_paths(&active).unwrap();
    let published = crate::compaction::publication::read_published_manifest(&expected_paths)
        .unwrap()
        .unwrap();
    assert_eq!(
        published.phase,
        crate::compaction::manifest::ManifestPhase::Prepared
    );
    assert_eq!(
        published.mode,
        crate::compaction::manifest::ManifestMode::OnlineFamily
    );
    assert!(!published.source_finalized);
    assert_eq!(published.durability, crate::DurabilityPolicy::Buffered);
    assert_eq!(published.replacement_inventory, Vec::new());
    assert!(crate::compaction::recovery::source_descriptors_match(
        directory.path(),
        &published
    ));

    controller.release(MaintenanceCheckpoint::ManifestPrepared);
    let (captured, paths, manifest) = capture_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(paths, expected_paths);
    assert_eq!(manifest, published);
    assert_eq!(captured.family, crate::StoreFamily::KeyValue);
    let crate::compaction::CapturedLogicalState::Value(snapshot) = captured.state else {
        panic!("key/value capture returned the wrong family state");
    };
    assert_eq!(
        snapshot.get(b"captured".as_slice()),
        Some(&b"before".to_vec())
    );
    assert!(!snapshot.contains_key(b"concurrent".as_slice()));
    assert_eq!(captured.granularity_nanos, 60_000_000_000);
    assert!(captured.last_bucket > 0);

    mutation_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    mutation.join().unwrap();
    assert_eq!(store.get(b"concurrent"), Some(b"after".to_vec()));
    assert_eq!(store.delta_group_count_probe(), 1);
    assert!(store.has_delta_recorder_probe());

    drop_tx.send(()).unwrap();
    capture_worker.join().unwrap();
    assert!(!store.has_delta_recorder_probe());
}

#[test]
fn online_prepared_recovery_accepts_append_rotation_and_requires_finalization() {
    let directory = tempfile::tempdir().unwrap();
    let options = crate::DurableStoreOptions::default()
        .with_wal_segment_size(crate::WalSegmentSize::try_from(170_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.put(b"first".to_vec(), b"one".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let initial = capture.manifest.clone();
    let paths = capture.paths.clone();
    assert!(!initial.source_finalized);

    store.put(b"second".to_vec(), b"two".to_vec());
    store.put(b"third".to_vec(), b"three".to_vec());
    assert!(directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000")
        .is_file());
    assert!(crate::compaction::recovery::source_descriptors_match(
        directory.path(),
        &initial
    ));

    drop(capture);
    drop(store);
    crate::compaction::recovery::recover_prepared_online(directory.path(), &paths, &initial)
        .unwrap();
    assert!(!paths.manifest.exists());

    let reopened = DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options)
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"first"), Some(b"one".to_vec()));
    assert_eq!(reopened.get(b"second"), Some(b"two".to_vec()));
    assert_eq!(reopened.get(b"third"), Some(b"three".to_vec()));

    let second = reopened
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let mut finalizing = second.manifest.clone();
    assert!(crate::compaction::publication::online_publication_ready(&finalizing).is_err());
    let operation_id = finalizing.operation_id;
    let phase = finalizing.phase;
    std::fs::write(&second.paths.staging, b"validated-replacement-prefix").unwrap();
    let replacement_inventory = vec![crate::compaction::manifest::ArtifactDescriptor {
        relative_path: std::path::PathBuf::from(second.paths.staging.file_name().unwrap()),
        role: crate::compaction::manifest::ArtifactRole::ReplacementPrefix,
        family: Some(crate::StoreFamily::KeyValue),
        length: u64::try_from(b"validated-replacement-prefix".len()).unwrap(),
        checksum: crc32fast::hash(b"validated-replacement-prefix"),
    }];
    let finalized_source_inventory = finalizing.source_inventory.clone();
    crate::compaction::publication::publish_online_finalized_prepared(
        &second.paths,
        &mut finalizing,
        finalized_source_inventory,
        replacement_inventory.clone(),
    )
    .unwrap();
    assert_eq!(finalizing.operation_id, operation_id);
    assert_eq!(finalizing.phase, phase);
    assert!(finalizing.source_finalized);
    assert_eq!(finalizing.replacement_inventory, replacement_inventory);
    assert_eq!(
        crate::compaction::publication::read_published_manifest(&second.paths)
            .unwrap()
            .unwrap(),
        finalizing
    );
    crate::compaction::publication::online_publication_ready(&finalizing).unwrap();
    assert!(crate::compaction::recovery::source_descriptors_match(
        directory.path(),
        &finalizing
    ));
    let durable_finalized = finalizing.clone();
    assert!(
        crate::compaction::publication::publish_online_finalized_prepared(
            &second.paths,
            &mut finalizing,
            durable_finalized.source_inventory.clone(),
            durable_finalized.replacement_inventory.clone(),
        )
        .is_err()
    );
    assert_eq!(
        crate::compaction::publication::read_published_manifest(&second.paths)
            .unwrap()
            .unwrap(),
        durable_finalized
    );
    drop(second);
}

#[test]
fn finalized_prepared_recovery_discards_an_empty_previous_directory_before_the_first_move() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"stable".to_vec(), b"authority".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    let paths = staged.prepared.paths.clone();
    std::fs::create_dir(&paths.previous).unwrap();

    let error = match store.complete_online_cutover_probe(staged) {
        Ok(_) => panic!("preexisting previous directory unexpectedly allowed publication"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        crate::CompactionError::Io {
            operation: crate::CompactionOperation::PublishPrevious,
            ..
        }
    ));
    assert!(paths.previous.is_dir());
    assert_eq!(std::fs::read_dir(&paths.previous).unwrap().count(), 0);
    assert!(paths.staging.is_file());
    drop(store);

    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .expect("empty pre-move directory cannot compete with exact old authority")
        .into_store();
    assert_eq!(reopened.get(b"stable"), Some(b"authority".to_vec()));
    reopened
        .try_put(b"after-reopen".to_vec(), b"accepted".to_vec())
        .unwrap();
    assert!(!paths.previous.exists());
    assert!(!paths.staging.exists());
    assert!(!paths.manifest.exists());
}

#[test]
fn staging_encode_and_validation_run_without_exclusive_maintenance() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    store.put(b"snapshot".to_vec(), b"captured".to_vec());
    let (observer, controller) = MaintenanceObserver::controlled([
        MaintenanceCheckpoint::StagingEncode,
        MaintenanceCheckpoint::StagingValidation,
    ]);
    let (staged_tx, staged_rx) = mpsc::sync_channel(0);
    let (drop_tx, drop_rx) = mpsc::sync_channel(0);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        let capture = worker_store
            .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
            .unwrap();
        let staged = super::prepare_online_staging(capture, |stage| {
            match stage {
                super::OnlineStagingStage::Encoding => {
                    observer.checkpoint(MaintenanceCheckpoint::StagingEncode)
                }
                super::OnlineStagingStage::Validation => {
                    observer.checkpoint(MaintenanceCheckpoint::StagingValidation)
                }
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        staged_tx
            .send((
                staged.staging.clone(),
                staged.prepared.paths.clone(),
                staged.replacement_inventory.clone(),
            ))
            .unwrap();
        drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(staged);
    });

    controller.wait_until_reached(MaintenanceCheckpoint::StagingEncode);
    assert_staging_pause_allows_progress(&store, b"during-encode", b"one");
    controller.release(MaintenanceCheckpoint::StagingEncode);

    controller.wait_until_reached(MaintenanceCheckpoint::StagingValidation);
    assert_staging_pause_allows_progress(&store, b"during-validation", b"two");
    controller.release(MaintenanceCheckpoint::StagingValidation);

    let (staging, paths, replacement_inventory) =
        staged_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(staging.family, crate::StoreFamily::KeyValue);
    let crate::compaction::CapturedLogicalState::Value(snapshot) = staging.state else {
        panic!("key/value staging returned the wrong family state");
    };
    assert_eq!(
        snapshot.get(b"snapshot".as_slice()),
        Some(&b"captured".to_vec())
    );
    assert!(!snapshot.contains_key(b"during-encode".as_slice()));
    assert!(!snapshot.contains_key(b"during-validation".as_slice()));
    assert_eq!(replacement_inventory.len(), 1);
    assert_eq!(
        replacement_inventory[0].relative_path,
        std::path::PathBuf::from(paths.staging.file_name().unwrap())
    );
    assert!(paths.staging.is_file());
    assert_eq!(store.delta_group_count_probe(), 2);

    drop_tx.send(()).unwrap();
    worker.join().unwrap();
    assert!(!store.has_delta_recorder_probe());
}

#[test]
fn single_action_delta_replays_once_in_wal_acceptance_order() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"same".to_vec(), b"base".to_vec());
    store.put(b"remove".to_vec(), b"present".to_vec());
    store.put(b"recreate".to_vec(), b"old".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    let initial_staging_bytes = staged.replacement_inventory[0].length;

    store.put(b"same".to_vec(), b"first".to_vec());
    store.put(b"distinct".to_vec(), b"value".to_vec());
    store.put(b"same".to_vec(), b"second".to_vec());
    store.remove(b"remove");
    store.remove(b"recreate");
    store.put(b"recreate".to_vec(), b"new".to_vec());
    assert_eq!(store.delta_group_count_probe(), 6);

    let applied = store.apply_online_delta_probe(staged).unwrap();
    assert_eq!(applied.replayed, 6);
    assert!(applied.encoded_bytes > 0);
    assert_eq!(
        applied.staged.replacement_inventory[0].length,
        initial_staging_bytes + applied.encoded_bytes
    );
    assert!(!store.has_delta_recorder_probe());
    let crate::compaction::CapturedLogicalState::Value(snapshot) = &applied.staged.staging.state
    else {
        panic!("key/value delta application returned the wrong family state");
    };
    assert_eq!(snapshot.get(b"same".as_slice()), Some(&b"second".to_vec()));
    assert_eq!(
        snapshot.get(b"distinct".as_slice()),
        Some(&b"value".to_vec())
    );
    assert!(!snapshot.contains_key(b"remove".as_slice()));
    assert_eq!(snapshot.get(b"recreate".as_slice()), Some(&b"new".to_vec()));
    assert_eq!(snapshot.len(), store.size());
    drop(applied);
}

static ONLINE_TEST_CLOCK: AtomicU64 = AtomicU64::new(0);

fn online_test_clock() -> u64 {
    ONLINE_TEST_CLOCK.load(Ordering::SeqCst)
}

#[test]
fn compute_delta_groups_remain_atomic_and_preserve_accepted_timestamps() {
    let directory = tempfile::tempdir().unwrap();
    let options = crate::DurableStoreOptions::default().with_timestamp_granularity(
        crate::TimestampGranularity::try_from(Duration::from_nanos(1)).unwrap(),
    );
    let store = DurableKeySetStore::try_init_new_with_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.install_clock_probe(online_test_clock);
    ONLINE_TEST_CLOCK.store(100, Ordering::SeqCst);
    store.append(b"group".to_vec(), b"keep".to_vec());
    store.append(b"group".to_vec(), b"remove".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();

    ONLINE_TEST_CLOCK.store(200, Ordering::SeqCst);
    store.append(b"group".to_vec(), b"ordinary-before".to_vec());
    ONLINE_TEST_CLOCK.store(300, Ordering::SeqCst);
    store
        .try_compute(b"group".to_vec(), |working| {
            working.remove(b"remove".as_slice());
            working.insert(b"compute-a".to_vec());
            working.insert(b"compute-b".to_vec());
        })
        .unwrap();
    ONLINE_TEST_CLOCK.store(400, Ordering::SeqCst);
    store.append(b"group".to_vec(), b"ordinary-after".to_vec());
    assert_eq!(store.delta_group_count_probe(), 3);

    let applied = store.apply_online_delta_probe(staged).unwrap();
    assert_eq!(applied.replayed, 3);
    assert_eq!(applied.group_frame_counts, vec![1, 3, 1]);
    assert_eq!(applied.accepted_buckets, vec![200, 300, 400]);
    assert_eq!(applied.staged.staging.granularity_nanos, 1);
    assert_eq!(applied.staged.staging.last_bucket, 400);
    assert_eq!(store.timestamp_state_probe(), (1, 400));
    let crate::compaction::CapturedLogicalState::Set(snapshot) = &applied.staged.staging.state
    else {
        panic!("key/set delta application returned the wrong family state");
    };
    let final_set = snapshot.get(b"group".as_slice()).unwrap();
    for expected in [
        b"keep".as_slice(),
        b"ordinary-before".as_slice(),
        b"compute-a".as_slice(),
        b"compute-b".as_slice(),
        b"ordinary-after".as_slice(),
    ] {
        assert!(final_set.contains(expected));
    }
    assert!(!final_set.contains(b"remove".as_slice()));
    assert_eq!(final_set.len(), 5);
    drop(applied);
}

#[test]
fn cutover_rejects_exact_live_mismatch_before_namespace_publication() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"snapshot".to_vec(), b"old".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    let paths = staged.prepared.paths.clone();
    let initial_manifest = staged.prepared.manifest.clone();
    assert!(!initial_manifest.source_finalized);

    store.put(b"recorded".to_vec(), b"accepted".to_vec());
    store.inject_live_value_probe(b"unrecorded".to_vec(), b"must-reject".to_vec());
    let active = directory.path().join("kv.wal.dat");
    let active_before_cutover = std::fs::read(&active).unwrap();
    assert!(store.apply_online_delta_probe(staged).is_err());

    assert_eq!(std::fs::read(&active).unwrap(), active_before_cutover);
    assert!(!paths.previous.exists());
    assert!(!paths.staging.exists());
    assert!(!paths.manifest.exists());
    assert!(!paths.manifest_next.exists());
    assert!(!store.has_delta_recorder_probe());
}

#[test]
fn bounded_delta_zero_exact_and_one_group_over_preserve_original_authority() {
    struct Case {
        name: &'static str,
        limit: u64,
        mutations: usize,
        overflows: bool,
    }

    let exact_group_bytes = one_put_delta_encoded_len();
    let cases = [
        Case {
            name: "zero",
            limit: 0,
            mutations: 0,
            overflows: false,
        },
        Case {
            name: "exact",
            limit: exact_group_bytes,
            mutations: 1,
            overflows: false,
        },
        Case {
            name: "one-group-over",
            limit: exact_group_bytes,
            mutations: 2,
            overflows: true,
        },
    ];

    for case in cases {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"snapshot".to_vec(), case.name.as_bytes().to_vec());
        let capture = store
            .begin_online_capture_probe(case.limit, MaintenanceObserver::default())
            .unwrap();
        let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
        let paths = staged.prepared.paths.clone();

        if case.mutations >= 1 {
            store.put(b"delta-a".to_vec(), b"value-a".to_vec());
        }
        if case.mutations >= 2 {
            store.put(b"delta-b".to_vec(), b"value-b".to_vec());
        }
        let active = directory.path().join("kv.wal.dat");
        let authoritative_before_cutover = std::fs::read(&active).unwrap();
        let result = store.apply_online_delta_probe(staged);

        match (case.overflows, result) {
            (true, Err(crate::CompactionError::ConcurrentDeltaLimitExceeded { limit })) => {
                assert_eq!(limit, exact_group_bytes);
                assert_eq!(
                    std::fs::read(&active).unwrap(),
                    authoritative_before_cutover
                );
                assert!(
                    !paths.staging.exists(),
                    "{} staging survived abort",
                    case.name
                );
                assert!(
                    !paths.manifest.exists(),
                    "{} manifest survived abort",
                    case.name
                );
                assert!(!paths.previous.exists());
                assert!(!store.has_delta_recorder_probe());

                store.put(b"after-abort".to_vec(), b"still-writable".to_vec());
                assert_eq!(store.get(b"delta-a"), Some(b"value-a".to_vec()));
                assert_eq!(store.get(b"delta-b"), Some(b"value-b".to_vec()));
            }
            (true, Err(error)) => panic!("{} returned wrong error: {error}", case.name),
            (true, Ok(_)) => panic!("{} unexpectedly succeeded", case.name),
            (false, Ok(applied)) => {
                assert_eq!(applied.replayed, case.mutations);
                assert_eq!(
                    applied.encoded_bytes,
                    exact_group_bytes * case.mutations as u64
                );
                assert!(!paths.previous.exists());
                drop(applied);
            }
            (false, Err(error)) => panic!("{} failed: {error}", case.name),
        };

        if case.overflows {
            drop(store);
            let reopened = DurableKeyValueStore::try_init_new(directory.path())
                .unwrap()
                .into_store();
            assert_eq!(reopened.get(b"delta-a"), Some(b"value-a".to_vec()));
            assert_eq!(reopened.get(b"delta-b"), Some(b"value-b".to_vec()));
            assert_eq!(
                reopened.get(b"after-abort"),
                Some(b"still-writable".to_vec())
            );
        }
    }
}

#[test]
fn successful_cutover_installs_fresh_writer_and_rotates_only_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let options = crate::DurableStoreOptions::default()
        .with_timestamp_granularity(
            crate::TimestampGranularity::try_from(Duration::from_nanos(1)).unwrap(),
        )
        .with_wal_segment_size(crate::WalSegmentSize::try_from(1_u64).unwrap());
    let store = DurableKeyValueStore::try_init_new_with_probe_options(directory.path(), options)
        .unwrap()
        .into_store();
    store.install_clock_probe(online_test_clock);
    ONLINE_TEST_CLOCK.store(100, Ordering::SeqCst);
    store.put(b"snapshot".to_vec(), vec![b'x'; 256]);

    let active = directory.path().join("kv.wal.dat");
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    let completed = store.complete_online_cutover_probe(staged).unwrap();
    assert_eq!(completed.replayed, 0);
    assert_eq!(
        completed.manifest.phase,
        crate::compaction::manifest::ManifestPhase::CleanupPending
    );
    assert_eq!(completed.cleanup, crate::CleanupStatus::Complete);
    assert!(completed.manifest.source_finalized);
    assert!(!completed.paths.previous.exists());
    assert!(!completed.paths.manifest.exists());
    assert!(!directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000")
        .exists());

    let replacement_before_mutation = std::fs::read(&active).unwrap();
    let installed = store.online_wal_state_probe();
    assert_eq!(installed.offset, replacement_before_mutation.len() as u64);
    assert_eq!(
        installed.active_len,
        replacement_before_mutation.len() as u64
    );
    assert_eq!(installed.granularity_nanos, 1);
    assert_eq!(installed.last_bucket, 100);
    assert_eq!(installed.frame_buffer_len, 0);
    assert_eq!(installed.rotation_segment_id, Some(0));
    assert_eq!(installed.rotation_segment_base, Some(0));
    assert_eq!(installed.force_before_next_mutation, Some(false));

    ONLINE_TEST_CLOCK.store(200, Ordering::SeqCst);
    store.put(b"after-cutover".to_vec(), b"accepted".to_vec());
    let sealed_replacement = directory
        .path()
        .join("kv.wal.dat.segment-00000000000000000000");
    assert_eq!(
        std::fs::read(&sealed_replacement).unwrap(),
        replacement_before_mutation
    );
    assert_eq!(store.get(b"after-cutover"), Some(b"accepted".to_vec()));
    let after = store.online_wal_state_probe();
    assert_eq!(after.granularity_nanos, 1);
    assert_eq!(after.last_bucket, 200);
    assert_eq!(after.rotation_segment_id, Some(1));
    assert_eq!(
        after.rotation_segment_base,
        Some(replacement_before_mutation.len() as u64)
    );
    assert_eq!(after.active_len, std::fs::metadata(&active).unwrap().len());
    assert_eq!(
        after.offset,
        after.rotation_segment_base.unwrap() + after.active_len
    );
}

#[test]
fn replacement_reopen_failure_preserves_live_reads_and_fails_closed_before_io() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"snapshot".to_vec(), b"readable".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    let paths = staged.prepared.paths.clone();
    store.put(b"delta".to_vec(), b"also-readable".to_vec());

    let error = match store.complete_online_cutover_with_reopen_probe(staged, |_| {
        Err(std::io::Error::other(
            "scripted authoritative replacement reopen failure",
        ))
    }) {
        Ok(_) => panic!("scripted replacement reopen unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        crate::CompactionError::Io {
            operation: crate::CompactionOperation::ReopenReplacement,
            ..
        }
    ));

    assert_eq!(store.get(b"snapshot"), Some(b"readable".to_vec()));
    assert_eq!(store.get(b"delta"), Some(b"also-readable".to_vec()));
    assert!(paths.previous.is_dir());
    assert!(!paths.staging.exists());
    assert!(directory.path().join("kv.wal.dat").is_file());
    assert_eq!(
        crate::compaction::publication::read_published_manifest(&paths)
            .unwrap()
            .unwrap()
            .phase,
        crate::compaction::manifest::ManifestPhase::PreviousPublished
    );

    let evidence =
        crate::test_support::maintenance_fixtures::snapshot_directory(directory.path()).unwrap();
    assert!(store
        .try_put(b"rejected-put".to_vec(), b"never-published".to_vec())
        .is_err());
    assert!(store.try_remove(b"snapshot").is_err());
    assert!(store
        .try_compute(b"delta".to_vec(), |_| b"never-published".to_vec())
        .is_err());
    assert_eq!(store.get(b"snapshot"), Some(b"readable".to_vec()));
    assert_eq!(store.get(b"delta"), Some(b"also-readable".to_vec()));
    assert_eq!(
        crate::test_support::maintenance_fixtures::snapshot_directory(directory.path()).unwrap(),
        evidence,
        "failed-closed mutations must not touch WAL or maintenance evidence"
    );
    assert!(!store.has_delta_recorder_probe());
    assert!(store.maintenance_probe().try_begin_online().is_ok());

    drop(store);
    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    assert_eq!(reopened.get(b"snapshot"), Some(b"readable".to_vec()));
    assert_eq!(reopened.get(b"delta"), Some(b"also-readable".to_vec()));
    reopened
        .try_put(b"after-recovery".to_vec(), b"writes-resumed".to_vec())
        .unwrap();
    assert!(!paths.previous.exists());
    assert!(!paths.manifest.exists());
}

#[test]
fn post_publication_cleanup_failure_is_pending_and_retries_only_in_foreground() {
    #[derive(Clone, Copy, Debug)]
    enum Retry {
        Reopen,
        NextCompaction,
    }

    for retry in [Retry::Reopen, Retry::NextCompaction] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"snapshot".to_vec(), b"retained".to_vec());
        let capture = store
            .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
            .unwrap();
        let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
        let paths = staged.prepared.paths.clone();
        let completed = store
            .complete_online_cutover_with_cleanup_probe(staged, |stage| {
                if stage == super::OnlineCleanupStage::BeforePreviousArtifact(0) {
                    Err(std::io::Error::other(
                        "scripted old-generation cleanup failure",
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap();
        assert_eq!(completed.cleanup, crate::CleanupStatus::Pending);
        assert_eq!(
            completed.manifest.phase,
            crate::compaction::manifest::ManifestPhase::CleanupPending
        );
        assert!(paths.previous.is_dir());
        assert!(paths.manifest.is_file());

        store.put(b"after-publication".to_vec(), b"still-writable".to_vec());
        assert_eq!(
            store.get(b"after-publication"),
            Some(b"still-writable".to_vec())
        );
        assert!(
            paths.previous.is_dir(),
            "cleanup must not run in background"
        );

        match retry {
            Retry::Reopen => {
                drop(store);
                let reopened = DurableKeyValueStore::try_init_new(directory.path())
                    .unwrap()
                    .into_store();
                assert_eq!(
                    reopened.get(b"after-publication"),
                    Some(b"still-writable".to_vec())
                );
                assert!(!paths.previous.exists());
                assert!(!paths.manifest.exists());
            }
            Retry::NextCompaction => {
                let next = store
                    .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
                    .unwrap();
                assert_eq!(
                    next.capture.state,
                    crate::compaction::CapturedLogicalState::Value(
                        std::collections::HashMap::from([
                            (b"snapshot".to_vec(), b"retained".to_vec()),
                            (b"after-publication".to_vec(), b"still-writable".to_vec()),
                        ])
                    )
                );
                assert!(!paths.previous.exists());
                assert_ne!(next.manifest.operation_id, completed.manifest.operation_id);
                drop(next);
            }
        }
    }
}

#[test]
fn every_prepublication_panic_and_cancellation_clears_only_its_attempt_artifacts() {
    fn assert_ready_after_unwind(
        store: &DurableKeyValueStore<std::fs::File>,
        directory: &std::path::Path,
    ) {
        let paths =
            crate::compaction::publication::family_artifact_paths(&directory.join("kv.wal.dat"))
                .unwrap();
        assert!(!paths.manifest.exists());
        assert!(!paths.manifest_next.exists());
        assert!(!paths.staging.exists());
        assert!(!paths.previous.exists());
        assert!(!store.has_delta_recorder_probe());
        let next = store.maintenance_probe().try_begin_online().unwrap();
        drop(next);
        store
            .try_put(b"after-unwind".to_vec(), b"accepted".to_vec())
            .unwrap();
        assert_eq!(store.get(b"after-unwind"), Some(b"accepted".to_vec()));
    }

    for target in [
        super::OnlineCaptureStage::SnapshotCaptured,
        super::OnlineCaptureStage::RecorderActivated,
        super::OnlineCaptureStage::ManifestPrepared,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"authority".to_vec(), b"unchanged".to_vec());
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.begin_online_capture_with_checkpoint_probe(u64::MAX, |stage| {
                if stage == target {
                    panic!("scripted {target:?} panic");
                }
            });
        }));
        assert!(unwind.is_err(), "{target:?} did not unwind");
        assert_eq!(store.get(b"authority"), Some(b"unchanged".to_vec()));
        assert_ready_after_unwind(&store, directory.path());
    }

    for target in [
        super::OnlineStagingStage::Encoding,
        super::OnlineStagingStage::Create,
        super::OnlineStagingStage::Write,
        super::OnlineStagingStage::Synchronize,
        super::OnlineStagingStage::Validation,
        super::OnlineStagingStage::Reopen,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"authority".to_vec(), b"unchanged".to_vec());
        let capture = store
            .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
            .unwrap();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = super::prepare_online_staging(capture, |stage| {
                if stage == target {
                    panic!("scripted {target:?} panic");
                }
                Ok(())
            });
        }));
        assert!(unwind.is_err(), "{target:?} did not unwind");
        assert_eq!(store.get(b"authority"), Some(b"unchanged".to_vec()));
        assert_ready_after_unwind(&store, directory.path());
    }

    for cancel_after_staging in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let capture = store
            .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
            .unwrap();
        if cancel_after_staging {
            drop(super::prepare_online_staging(capture, |_| Ok(())).unwrap());
        } else {
            drop(capture);
        }
        assert_ready_after_unwind(&store, directory.path());
    }
}

#[test]
fn paused_first_attempt_keeps_progress_and_losing_second_call_artifact_free() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store(),
    );
    store.put(b"snapshot".to_vec(), b"captured".to_vec());
    let (observer, controller) =
        MaintenanceObserver::controlled([MaintenanceCheckpoint::StagingEncode]);
    let (ownership_tx, ownership_rx) = mpsc::sync_channel(0);
    let (drop_tx, drop_rx) = mpsc::sync_channel(0);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        let capture = worker_store
            .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
            .unwrap();
        ownership_tx
            .send((
                capture.attempt.token(),
                capture.manifest.operation_id,
                capture.paths.clone(),
            ))
            .unwrap();
        let staged = super::prepare_online_staging(capture, |stage| {
            if stage == super::OnlineStagingStage::Encoding {
                observer.checkpoint(MaintenanceCheckpoint::StagingEncode);
            }
            Ok(())
        })
        .unwrap();
        drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(staged);
    });

    let (winning_token, operation_id, paths) =
        ownership_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    controller.wait_until_reached(MaintenanceCheckpoint::StagingEncode);
    assert_eq!(
        u64::from_le_bytes(operation_id[..8].try_into().unwrap()),
        winning_token
    );
    let manifest_before_loser = std::fs::read(&paths.manifest).unwrap();
    assert!(store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .is_err());
    assert_eq!(
        std::fs::read(&paths.manifest).unwrap(),
        manifest_before_loser
    );
    assert!(!paths.manifest_next.exists());
    assert!(!paths.staging.exists());
    assert!(!paths.previous.exists());
    assert!(store.has_delta_recorder_probe());
    assert_eq!(store.delta_group_count_probe(), 0);

    assert_eq!(store.get(b"snapshot"), Some(b"captured".to_vec()));
    store.put(b"while-paused".to_vec(), b"accepted".to_vec());
    assert_eq!(store.get(b"while-paused"), Some(b"accepted".to_vec()));
    assert_eq!(store.delta_group_count_probe(), 1);
    assert_eq!(
        std::fs::read(&paths.manifest).unwrap(),
        manifest_before_loser
    );

    controller.release(MaintenanceCheckpoint::StagingEncode);
    drop_tx.send(()).unwrap();
    worker.join().unwrap();
    assert!(!store.has_delta_recorder_probe());
}

#[test]
fn every_prepublication_staging_failure_cleans_only_owned_artifacts_and_restores_progress() {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Failure {
        Create,
        Write,
        Synchronize,
        Reopen,
        Mismatch,
        ExistingCollision,
    }

    for failure in [
        Failure::Create,
        Failure::Write,
        Failure::Synchronize,
        Failure::Reopen,
        Failure::Mismatch,
        Failure::ExistingCollision,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyValueStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"authority".to_vec(), b"old-writer".to_vec());
        let active = directory.path().join("kv.wal.dat");
        let authority_before = std::fs::read(&active).unwrap();
        let capture = store
            .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
            .unwrap();
        let paths = capture.paths.clone();
        let granularity_nanos = capture.capture.granularity_nanos;
        let last_bucket = capture.capture.last_bucket;
        let collision = b"not-owned-by-this-attempt".to_vec();
        if failure == Failure::ExistingCollision {
            std::fs::write(&paths.staging, &collision).unwrap();
        }
        let staging_path = paths.staging.clone();
        let result = super::prepare_online_staging(capture, move |stage| {
            let injected = match failure {
                Failure::Create => stage == super::OnlineStagingStage::Create,
                Failure::Write => stage == super::OnlineStagingStage::Write,
                Failure::Synchronize => stage == super::OnlineStagingStage::Synchronize,
                Failure::Reopen => stage == super::OnlineStagingStage::Reopen,
                Failure::Mismatch | Failure::ExistingCollision => false,
            };
            if injected {
                return Err(std::io::Error::other(format!(
                    "scripted {failure:?} failure"
                )));
            }
            if failure == Failure::Mismatch && stage == super::OnlineStagingStage::Reopen {
                let mismatch = std::collections::HashMap::from([(
                    b"different".to_vec(),
                    b"valid-current-state".to_vec(),
                )]);
                let encoded = crate::wal::replay::encode_current_key_value_snapshot_with_metadata(
                    &mismatch,
                    granularity_nanos,
                    last_bucket,
                )
                .unwrap();
                std::fs::write(&staging_path, encoded).unwrap();
            }
            Ok(())
        });
        assert!(result.is_err(), "{failure:?} unexpectedly succeeded");

        assert_eq!(std::fs::read(&active).unwrap(), authority_before);
        assert!(!paths.manifest.exists(), "{failure:?} manifest survived");
        assert!(!paths.manifest_next.exists());
        assert!(!paths.previous.exists());
        if failure == Failure::ExistingCollision {
            assert_eq!(std::fs::read(&paths.staging).unwrap(), collision);
        } else {
            assert!(!paths.staging.exists(), "{failure:?} staging survived");
        }
        assert!(!store.has_delta_recorder_probe());
        let next_attempt = store.maintenance_probe().try_begin_online().unwrap();
        drop(next_attempt);

        store.put(b"after-failure".to_vec(), b"accepted".to_vec());
        assert_eq!(
            store.get(b"after-failure"),
            Some(b"accepted".to_vec()),
            "{failure:?} did not restore writer progress"
        );
    }
}

fn one_put_delta_encoded_len() -> u64 {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"snapshot".to_vec(), b"calibration".to_vec());
    let capture = store
        .begin_online_capture_probe(u64::MAX, MaintenanceObserver::default())
        .unwrap();
    let staged = super::prepare_online_staging(capture, |_| Ok(())).unwrap();
    store.put(b"delta-a".to_vec(), b"value-a".to_vec());
    let exact = store.delta_used_bytes_probe();
    assert!(exact > 0);
    drop(staged);
    exact
}

fn assert_staging_pause_allows_progress(
    store: &Arc<DurableKeyValueStore<std::fs::File>>,
    key: &[u8],
    value: &[u8],
) {
    assert_eq!(store.get(b"snapshot"), Some(b"captured".to_vec()));
    let (mutation_tx, mutation_rx) = mpsc::sync_channel(0);
    let mutation_store = Arc::clone(store);
    let key = key.to_vec();
    let value = value.to_vec();
    let mutation = std::thread::spawn(move || {
        mutation_store.try_put(key, value).unwrap();
        mutation_tx.send(()).unwrap();
    });
    mutation_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    mutation.join().unwrap();

    let (exclusive_tx, exclusive_rx) = mpsc::sync_channel(0);
    let exclusive_store = Arc::clone(store);
    let exclusive = std::thread::spawn(move || {
        let _exclusive = exclusive_store.maintenance_probe().exclusive();
        exclusive_tx.send(()).unwrap();
    });
    exclusive_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    exclusive.join().unwrap();
}

fn block_on_online<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
