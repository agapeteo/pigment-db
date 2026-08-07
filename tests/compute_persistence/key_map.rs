use crate::support::{assert_map_reopens, callback_count, count, increment, wal_bytes};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;
use std::collections::BTreeMap;

fn expected(values: &[(usize, &[u8])]) -> Option<BTreeMap<SearchKey, Vec<u8>>> {
    Some(
        values
            .iter()
            .map(|(key, value)| ((*key).into(), value.to_vec()))
            .collect(),
    )
}

#[test]
fn try_compute_persists_mixed_delta() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"map".to_vec(), 1.into(), b"remove".to_vec());
        store.put(b"map".to_vec(), 2.into(), b"replace-old".to_vec());
        store.put(b"map".to_vec(), 3.into(), b"keep".to_vec());
        store.put(b"other".to_vec(), 9.into(), b"isolated".to_vec());
        let calls = callback_count();
        let mutate = |map: &mut BTreeMap<SearchKey, Vec<u8>>| {
            increment(&calls);
            map.remove(&SearchKey::from(1));
            map.insert(2.into(), b"replace-new".to_vec());
            map.insert(4.into(), b"add".to_vec());
        };
        if compatibility {
            store.compute(b"map".to_vec(), mutate);
        } else {
            store.try_compute(b"map".to_vec(), mutate).unwrap();
        }
        assert_eq!(count(&calls), 1);
        assert_eq!(
            store.get_sorted_map(b"map"),
            expected(&[(2, b"replace-new"), (3, b"keep"), (4, b"add")])
        );
        drop(store);
        assert_map_reopens(
            directory.path(),
            b"map",
            &expected(&[(2, b"replace-new"), (3, b"keep"), (4, b"add")]),
        );
        assert_map_reopens(directory.path(), b"other", &expected(&[(9, b"isolated")]));
    }
}

#[test]
fn compute_creates_absent_non_empty_map() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let calls = callback_count();
        let mutate = |map: &mut BTreeMap<SearchKey, Vec<u8>>| {
            increment(&calls);
            map.insert(2.into(), b"two".to_vec());
            map.insert(1.into(), b"one".to_vec());
        };
        if compatibility {
            store.compute(b"new".to_vec(), mutate);
        } else {
            store.try_compute(b"new".to_vec(), mutate).unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_map_reopens(
            directory.path(),
            b"new",
            &expected(&[(1, b"one"), (2, b"two")]),
        );
    }
}

#[test]
fn compute_if_present_obeys_eligibility_and_persists() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"present".to_vec(), 1.into(), b"one".to_vec());
        let calls = callback_count();
        let mutate = |map: &mut BTreeMap<SearchKey, Vec<u8>>| {
            increment(&calls);
            map.insert(2.into(), b"two".to_vec());
        };
        if compatibility {
            store.compute_if_present(b"present".to_vec(), mutate);
        } else {
            store
                .try_compute_if_present(b"present".to_vec(), mutate)
                .unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_map_reopens(
            directory.path(),
            b"present",
            &expected(&[(1, b"one"), (2, b"two")]),
        );
    }

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    let before = wal_bytes(directory.path(), "map.wal.dat");
    let calls = callback_count();
    store
        .try_compute_if_present(b"absent".to_vec(), |_| increment(&calls))
        .unwrap();
    store.compute_if_present(b"absent".to_vec(), |_| increment(&calls));
    assert_eq!(count(&calls), 0);
    assert_eq!(wal_bytes(directory.path(), "map.wal.dat"), before);
    drop(store);
    assert_map_reopens(directory.path(), b"absent", &None);
}

#[test]
fn compute_if_absent_obeys_eligibility_and_persists() {
    for compatibility in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        let calls = callback_count();
        let mutate = |map: &mut BTreeMap<SearchKey, Vec<u8>>| {
            increment(&calls);
            map.insert(1.into(), b"created".to_vec());
        };
        if compatibility {
            store.compute_if_absent(b"absent".to_vec(), mutate);
        } else {
            store
                .try_compute_if_absent(b"absent".to_vec(), mutate)
                .unwrap();
        }
        assert_eq!(count(&calls), 1);
        drop(store);
        assert_map_reopens(directory.path(), b"absent", &expected(&[(1, b"created")]));
    }

    let directory = tempfile::tempdir().unwrap();
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .unwrap()
        .into_store();
    store.put(b"present".to_vec(), 1.into(), b"keep".to_vec());
    let before = wal_bytes(directory.path(), "map.wal.dat");
    let calls = callback_count();
    store
        .try_compute_if_absent(b"present".to_vec(), |_| increment(&calls))
        .unwrap();
    store.compute_if_absent(b"present".to_vec(), |_| increment(&calls));
    assert_eq!(count(&calls), 0);
    assert_eq!(wal_bytes(directory.path(), "map.wal.dat"), before);
    drop(store);
    assert_map_reopens(directory.path(), b"present", &expected(&[(1, b"keep")]));
}
