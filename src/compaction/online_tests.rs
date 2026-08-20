//! Private online-compaction behavior tests.

use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::maintenance_coordination::MaintenanceCoordinator;

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
