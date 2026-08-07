use crate::support::{assert_set_reopens, block_on, callback_count, count, increment, wal_bytes};
use pigment_db::key_set_store::DurableKeySetStore;
use std::collections::HashSet;

fn expected(values: &[&[u8]]) -> Option<HashSet<Vec<u8>>> {
    Some(values.iter().map(|value| value.to_vec()).collect())
}

#[test]
fn try_compute_persists_mixed_delta() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"key".to_vec(), b"keep".to_vec());
        store.append(b"key".to_vec(), b"remove".to_vec());
        store.append(b"other".to_vec(), b"isolated".to_vec());
        let calls = callback_count();
        if compatibility {
            store.compute(b"key".to_vec(), |set| {
                increment(&calls);
                set.remove(b"remove".as_slice());
                set.insert(b"add".to_vec());
            });
        } else {
            store
                .try_compute(b"key".to_vec(), |set| {
                    increment(&calls);
                    set.remove(b"remove".as_slice());
                    set.insert(b"add".to_vec());
                })
                .unwrap();
        }
        assert_eq!(count(&calls), 1);
        assert_eq!(store.get_hashset(b"key"), expected(&[b"keep", b"add"]));
        assert_eq!(store.get_hashset(b"other"), expected(&[b"isolated"]));
        drop(store);
        assert_set_reopens(directory.path(), b"key", &expected(&[b"keep", b"add"]));
        assert_set_reopens(directory.path(), b"other", &expected(&[b"isolated"]));
    }
}

#[test]
fn compute_creates_absent_non_empty_set() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let calls = callback_count();
        if compatibility {
            store.compute(b"new".to_vec(), |set| {
                increment(&calls);
                set.insert(b"member".to_vec());
            });
        } else {
            store
                .try_compute(b"new".to_vec(), |set| {
                    increment(&calls);
                    set.insert(b"member".to_vec());
                })
                .unwrap();
        }
        assert_eq!(count(&calls), 1);
        assert_eq!(store.get_hashset(b"new"), expected(&[b"member"]));
        drop(store);
        assert_set_reopens(directory.path(), b"new", &expected(&[b"member"]));
    }
}

#[test]
fn compute_if_present_obeys_eligibility_and_persists() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"present".to_vec(), b"old".to_vec());
        let calls = callback_count();
        if compatibility {
            store.compute_if_present(b"present".to_vec(), |set| {
                increment(&calls);
                set.insert(b"new".to_vec());
            });
        } else {
            store
                .try_compute_if_present(b"present".to_vec(), |set| {
                    increment(&calls);
                    set.insert(b"new".to_vec());
                })
                .unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_set_reopens(directory.path(), b"present", &expected(&[b"old", b"new"]));
    }

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    let before = wal_bytes(directory.path(), "set.wal.dat");
    let calls = callback_count();
    store
        .try_compute_if_present(b"absent".to_vec(), |_| increment(&calls))
        .unwrap();
    store.compute_if_present(b"absent".to_vec(), |_| increment(&calls));
    assert_eq!(count(&calls), 0);
    assert_eq!(wal_bytes(directory.path(), "set.wal.dat"), before);
    drop(store);
    assert_set_reopens(directory.path(), b"absent", &None);
}

#[test]
fn compute_if_absent_obeys_eligibility_and_persists() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let calls = callback_count();
        if compatibility {
            store.compute_if_absent(b"absent".to_vec(), |set| {
                increment(&calls);
                set.insert(b"created".to_vec());
            });
        } else {
            store
                .try_compute_if_absent(b"absent".to_vec(), |set| {
                    increment(&calls);
                    set.insert(b"created".to_vec());
                })
                .unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_set_reopens(directory.path(), b"absent", &expected(&[b"created"]));
    }

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeySetStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.append(b"present".to_vec(), b"keep".to_vec());
    let before = wal_bytes(directory.path(), "set.wal.dat");
    let calls = callback_count();
    store
        .try_compute_if_absent(b"present".to_vec(), |_| increment(&calls))
        .unwrap();
    store.compute_if_absent(b"present".to_vec(), |_| increment(&calls));
    assert_eq!(count(&calls), 0);
    assert_eq!(wal_bytes(directory.path(), "set.wal.dat"), before);
    drop(store);
    assert_set_reopens(directory.path(), b"present", &expected(&[b"keep"]));
}

#[test]
fn try_compute_async_persists_mixed_delta() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"async".to_vec(), b"keep".to_vec());
        store.append(b"async".to_vec(), b"remove".to_vec());
        let calls = callback_count();
        if compatibility {
            block_on(store.compute_async(b"async".to_vec(), async |set| {
                increment(&calls);
                set.remove(b"remove".as_slice());
                set.insert(b"add".to_vec());
            }));
        } else {
            block_on(store.try_compute_async(b"async".to_vec(), async |set| {
                increment(&calls);
                set.remove(b"remove".as_slice());
                set.insert(b"add".to_vec());
            }))
            .unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_set_reopens(directory.path(), b"async", &expected(&[b"keep", b"add"]));
    }
}

#[test]
fn compute_async_creates_absent_non_empty_set() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let calls = callback_count();
        if compatibility {
            block_on(store.compute_async(b"async-new".to_vec(), async |set| {
                increment(&calls);
                set.insert(b"created".to_vec());
            }));
        } else {
            block_on(store.try_compute_async(b"async-new".to_vec(), async |set| {
                increment(&calls);
                set.insert(b"created".to_vec());
            }))
            .unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_set_reopens(directory.path(), b"async-new", &expected(&[b"created"]));
    }
}
