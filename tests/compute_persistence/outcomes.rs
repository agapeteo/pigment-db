use crate::support::{assert_map_reopens, assert_set_reopens, block_on, wal_bytes};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::model::SearchKey;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn set_present_to_empty_is_one_outer_delete() {
    for mode in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"key".to_vec(), b"one".to_vec());
        store.append(b"key".to_vec(), b"two".to_vec());
        let before = wal_bytes(directory.path(), "set.wal.dat").len();
        match mode {
            0 => store
                .try_compute(b"key".to_vec(), |set| set.clear())
                .unwrap(),
            1 => store.compute(b"key".to_vec(), |set| set.clear()),
            2 => store
                .try_compute_if_present(b"key".to_vec(), |set| set.clear())
                .unwrap(),
            3 => store.compute_if_present(b"key".to_vec(), |set| set.clear()),
            4 => {
                block_on(store.try_compute_async(b"key".to_vec(), async |set| set.clear())).unwrap()
            }
            5 => block_on(store.compute_async(b"key".to_vec(), async |set| set.clear())),
            _ => unreachable!(),
        }
        assert!(!store.contains_key(b"key"));
        assert_eq!(
            wal_bytes(directory.path(), "set.wal.dat").len() - before,
            46 + b"key".len()
        );
        drop(store);
        assert_set_reopens(directory.path(), b"key", &None);
    }
}

#[test]
fn set_absent_to_empty_writes_nothing_and_creates_no_key() {
    for mode in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let before = wal_bytes(directory.path(), "set.wal.dat");
        match mode {
            0 => store.try_compute(b"key".to_vec(), |_| {}).unwrap(),
            1 => store.compute(b"key".to_vec(), |_| {}),
            2 => store
                .try_compute_if_absent(b"key".to_vec(), |_| {})
                .unwrap(),
            3 => store.compute_if_absent(b"key".to_vec(), |_| {}),
            4 => block_on(store.try_compute_async(b"key".to_vec(), async |_| {})).unwrap(),
            5 => block_on(store.compute_async(b"key".to_vec(), async |_| {})),
            _ => unreachable!(),
        }
        assert!(!store.contains_key(b"key"));
        assert_eq!(wal_bytes(directory.path(), "set.wal.dat"), before);
        drop(store);
        assert_set_reopens(directory.path(), b"key", &None);
    }
}

#[test]
fn map_present_to_empty_is_one_outer_delete() {
    for mode in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"key".to_vec(), 1.into(), b"one".to_vec());
        store.put(b"key".to_vec(), 2.into(), b"two".to_vec());
        let before = wal_bytes(directory.path(), "map.wal.dat").len();
        match mode {
            0 => store
                .try_compute(b"key".to_vec(), |map| map.clear())
                .unwrap(),
            1 => store.compute(b"key".to_vec(), |map| map.clear()),
            2 => store
                .try_compute_if_present(b"key".to_vec(), |map| map.clear())
                .unwrap(),
            3 => store.compute_if_present(b"key".to_vec(), |map| map.clear()),
            _ => unreachable!(),
        }
        assert!(!store.contains_key(b"key"));
        assert_eq!(
            wal_bytes(directory.path(), "map.wal.dat").len() - before,
            46 + b"key".len()
        );
        drop(store);
        assert_map_reopens(directory.path(), b"key", &None);
    }
}

#[test]
fn map_absent_to_empty_writes_nothing_and_creates_no_key() {
    for mode in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let before = wal_bytes(directory.path(), "map.wal.dat");
        match mode {
            0 => store.try_compute(b"key".to_vec(), |_| {}).unwrap(),
            1 => store.compute(b"key".to_vec(), |_| {}),
            2 => store
                .try_compute_if_absent(b"key".to_vec(), |_| {})
                .unwrap(),
            3 => store.compute_if_absent(b"key".to_vec(), |_| {}),
            _ => unreachable!(),
        }
        assert!(!store.contains_key(b"key"));
        assert_eq!(wal_bytes(directory.path(), "map.wal.dat"), before);
        drop(store);
        assert_map_reopens(directory.path(), b"key", &None);
    }
}

#[test]
fn set_exact_noop_writes_nothing() {
    for mode in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"key".to_vec(), b"same".to_vec());
        let before = wal_bytes(directory.path(), "set.wal.dat");
        let calls = AtomicUsize::new(0);
        match mode {
            0 => store
                .try_compute(b"key".to_vec(), |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap(),
            1 => store.compute(b"key".to_vec(), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }),
            2 => store
                .try_compute_if_present(b"key".to_vec(), |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap(),
            3 => store.compute_if_present(b"key".to_vec(), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }),
            4 => block_on(store.try_compute_async(b"key".to_vec(), async |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }))
            .unwrap(),
            5 => block_on(store.compute_async(b"key".to_vec(), async |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            })),
            _ => unreachable!(),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(wal_bytes(directory.path(), "set.wal.dat"), before);
        drop(store);
        assert_set_reopens(
            directory.path(),
            b"key",
            &Some([b"same".to_vec()].into_iter().collect()),
        );
    }
}

#[test]
fn map_exact_noop_writes_nothing() {
    for mode in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"key".to_vec(), 1.into(), b"same".to_vec());
        let before = wal_bytes(directory.path(), "map.wal.dat");
        let calls = AtomicUsize::new(0);
        match mode {
            0 => store
                .try_compute(b"key".to_vec(), |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap(),
            1 => store.compute(b"key".to_vec(), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }),
            2 => store
                .try_compute_if_present(b"key".to_vec(), |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap(),
            3 => store.compute_if_present(b"key".to_vec(), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }),
            _ => unreachable!(),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(wal_bytes(directory.path(), "map.wal.dat"), before);
        drop(store);
        let expected: BTreeMap<SearchKey, Vec<u8>> =
            [(1.into(), b"same".to_vec())].into_iter().collect();
        assert_map_reopens(directory.path(), b"key", &Some(expected));
    }
}

#[test]
fn set_regression_outcomes_converge() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.append(b"key".to_vec(), b"same".to_vec());
    let before = wal_bytes(directory.path(), "set.wal.dat");
    store
        .try_compute(b"key".to_vec(), |set| {
            set.insert(b"same".to_vec());
            set.remove(b"same".as_slice());
            set.insert(b"same".to_vec());
        })
        .unwrap();
    assert_eq!(wal_bytes(directory.path(), "set.wal.dat"), before);
    drop(store);
    assert_set_reopens(
        directory.path(),
        b"key",
        &Some([b"same".to_vec()].into_iter().collect()),
    );

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store
        .try_compute(b"binary".to_vec(), |set| {
            set.insert(Vec::new());
            set.insert(vec![0, 255, 0]);
        })
        .unwrap();
    store.append(b"other".to_vec(), b"isolated".to_vec());
    drop(store);
    assert_set_reopens(
        directory.path(),
        b"binary",
        &Some([Vec::new(), vec![0, 255, 0]].into_iter().collect()),
    );
    assert_set_reopens(
        directory.path(),
        b"other",
        &Some([b"isolated".to_vec()].into_iter().collect()),
    );

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.append(b"present".to_vec(), b"keep".to_vec());
    store.append(b"other".to_vec(), b"isolated".to_vec());
    let calls = AtomicUsize::new(0);
    store
        .try_compute_if_present(b"absent".to_vec(), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    store
        .try_compute_if_absent(b"present".to_vec(), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(store);
    assert_set_reopens(directory.path(), b"absent", &None);
    assert_set_reopens(
        directory.path(),
        b"present",
        &Some([b"keep".to_vec()].into_iter().collect()),
    );
    assert_set_reopens(
        directory.path(),
        b"other",
        &Some([b"isolated".to_vec()].into_iter().collect()),
    );
}

#[test]
fn map_regression_outcomes_converge() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"key".to_vec(), 1.into(), b"same".to_vec());
    let before = wal_bytes(directory.path(), "map.wal.dat");
    store
        .try_compute(b"key".to_vec(), |map| {
            map.insert(1.into(), b"same".to_vec());
        })
        .unwrap();
    assert_eq!(wal_bytes(directory.path(), "map.wal.dat"), before);
    drop(store);
    let expected: BTreeMap<SearchKey, Vec<u8>> =
        [(1.into(), b"same".to_vec())].into_iter().collect();
    assert_map_reopens(directory.path(), b"key", &Some(expected));

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store
        .try_compute(b"binary".to_vec(), |map| {
            map.insert(SearchKey::from(Vec::<u8>::new()), Vec::new());
            map.insert(SearchKey::from(vec![0, 255, 0]), vec![255, 0]);
        })
        .unwrap();
    store.put(b"other".to_vec(), 9.into(), b"isolated".to_vec());
    let expected: BTreeMap<SearchKey, Vec<u8>> = [
        (SearchKey::from(Vec::<u8>::new()), Vec::new()),
        (SearchKey::from(vec![0, 255, 0]), vec![255, 0]),
    ]
    .into_iter()
    .collect();
    drop(store);
    assert_map_reopens(directory.path(), b"binary", &Some(expected));
    let other: BTreeMap<SearchKey, Vec<u8>> =
        [(9.into(), b"isolated".to_vec())].into_iter().collect();
    assert_map_reopens(directory.path(), b"other", &Some(other));

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"present".to_vec(), 1.into(), b"keep".to_vec());
    store.put(b"other".to_vec(), 9.into(), b"isolated".to_vec());
    let calls = AtomicUsize::new(0);
    store
        .try_compute_if_present(b"absent".to_vec(), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    store
        .try_compute_if_absent(b"present".to_vec(), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(store);
    assert_map_reopens(directory.path(), b"absent", &None);
    let present: BTreeMap<SearchKey, Vec<u8>> =
        [(1.into(), b"keep".to_vec())].into_iter().collect();
    let other: BTreeMap<SearchKey, Vec<u8>> =
        [(9.into(), b"isolated".to_vec())].into_iter().collect();
    assert_map_reopens(directory.path(), b"present", &Some(present));
    assert_map_reopens(directory.path(), b"other", &Some(other));
}
