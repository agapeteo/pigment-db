use crate::support::{assert_map_reopens, assert_set_reopens};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::model::SearchKey;
use std::collections::{BTreeMap, HashSet};

#[test]
fn one_hundred_set_histories_converge() {
    for history in 0_usize..100 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeySetStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.append(b"other".to_vec(), history.to_ne_bytes().to_vec());
        let mut model = HashSet::new();
        for step in 0_usize..15 {
            match (history + step) % 5 {
                0 => {
                    let value = (history * 31 + step).to_ne_bytes().to_vec();
                    store
                        .try_compute(b"key".to_vec(), |set| {
                            set.insert(value.clone());
                        })
                        .unwrap();
                    model.insert(value);
                }
                1 => {
                    let value = (history * 31 + step.saturating_sub(1))
                        .to_ne_bytes()
                        .to_vec();
                    store
                        .try_compute(b"key".to_vec(), |set| {
                            set.remove(&value);
                        })
                        .unwrap();
                    model.remove(&value);
                }
                2 => {
                    store.try_compute(b"key".to_vec(), |_| {}).unwrap();
                }
                3 if step % 7 == 0 => {
                    store
                        .try_compute(b"key".to_vec(), |set| set.clear())
                        .unwrap();
                    model.clear();
                }
                _ => {
                    let value = vec![history as u8, step as u8, 0, 255];
                    store
                        .try_compute(b"key".to_vec(), |set| {
                            set.insert(value.clone());
                        })
                        .unwrap();
                    model.insert(value);
                }
            }
        }
        let expected = (!model.is_empty()).then_some(model);
        assert_eq!(store.get_hashset(b"key"), expected);
        drop(store);
        assert_set_reopens(directory.path(), b"key", &expected);
        assert_set_reopens(
            directory.path(),
            b"other",
            &Some([history.to_ne_bytes().to_vec()].into_iter().collect()),
        );
    }
}

#[test]
fn one_hundred_map_histories_converge() {
    for history in 0_usize..100 {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableKeyMapStore::try_init_new(directory.path())
            .unwrap()
            .into_store();
        store.put(b"other".to_vec(), 99.into(), history.to_ne_bytes().to_vec());
        let mut model = BTreeMap::new();
        for step in 0_usize..15 {
            let search_key = SearchKey::from((history * 17 + step) % 11);
            match (history + step) % 5 {
                0 | 1 => {
                    let value = vec![history as u8, step as u8, (history + step) as u8];
                    store
                        .try_compute(b"key".to_vec(), |map| {
                            map.insert(search_key.clone(), value.clone());
                        })
                        .unwrap();
                    model.insert(search_key, value);
                }
                2 => {
                    store
                        .try_compute(b"key".to_vec(), |map| {
                            map.remove(&search_key);
                        })
                        .unwrap();
                    model.remove(&search_key);
                }
                3 => {
                    store.try_compute(b"key".to_vec(), |_| {}).unwrap();
                }
                _ if step % 7 == 0 => {
                    store
                        .try_compute(b"key".to_vec(), |map| map.clear())
                        .unwrap();
                    model.clear();
                }
                _ => {
                    let value = vec![0, 255, history as u8, step as u8];
                    store
                        .try_compute(b"key".to_vec(), |map| {
                            map.insert(search_key.clone(), value.clone());
                        })
                        .unwrap();
                    model.insert(search_key, value);
                }
            }
        }
        let expected = (!model.is_empty()).then_some(model);
        assert_eq!(store.get_sorted_map(b"key"), expected);
        drop(store);
        assert_map_reopens(directory.path(), b"key", &expected);
        let other = [(SearchKey::from(99), history.to_ne_bytes().to_vec())]
            .into_iter()
            .collect();
        assert_map_reopens(directory.path(), b"other", &Some(other));
    }
}
