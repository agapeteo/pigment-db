use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::model::SearchKey;
use std::collections::{BTreeMap, HashSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

const ITEMS: usize = 10_000;
const SAMPLES: usize = 11;

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn sample(mut workload: impl FnMut() -> Duration) -> Duration {
    median((0..SAMPLES).map(|_| workload()).collect())
}

fn base_set() -> HashSet<Vec<u8>> {
    (0..ITEMS)
        .map(|index| index.to_ne_bytes().to_vec())
        .collect()
}

fn base_map() -> BTreeMap<SearchKey, Vec<u8>> {
    (0..ITEMS)
        .map(|index| (index.into(), index.to_ne_bytes().to_vec()))
        .collect()
}

#[test]
#[ignore = "release-only benchmark report"]
fn compute_10k_medians() {
    for profile in ["sparse", "mixed", "full"] {
        let set_corrected = sample(|| {
            let store = DurableKeySetStore::new_vec_based();
            for index in 0..ITEMS {
                store.append(b"key".to_vec(), index.to_ne_bytes().to_vec());
            }
            let started = Instant::now();
            store
                .try_compute(b"key".to_vec(), |set| match profile {
                    "sparse" => {
                        set.insert(ITEMS.to_ne_bytes().to_vec());
                    }
                    "mixed" => {
                        for index in 0_usize..500 {
                            set.remove(&index.to_ne_bytes().to_vec());
                            set.insert((ITEMS + index).to_ne_bytes().to_vec());
                        }
                    }
                    "full" => {
                        set.clear();
                        set.extend((ITEMS..ITEMS * 2).map(|index| index.to_ne_bytes().to_vec()));
                    }
                    _ => unreachable!(),
                })
                .unwrap();
            let elapsed = started.elapsed();
            black_box(store.get_hashset(b"key"));
            elapsed
        });

        let map_corrected = sample(|| {
            let store = DurableKeyMapStore::new_vec_based();
            for index in 0..ITEMS {
                store.put(b"key".to_vec(), index.into(), index.to_ne_bytes().to_vec());
            }
            let started = Instant::now();
            store
                .try_compute(b"key".to_vec(), |map| match profile {
                    "sparse" => {
                        map.insert((ITEMS - 1).into(), b"replacement".to_vec());
                    }
                    "mixed" => {
                        for index in 0..250 {
                            map.remove(&SearchKey::from(index));
                            map.insert((ITEMS + index).into(), b"added".to_vec());
                        }
                        for index in 250..750 {
                            map.insert(index.into(), b"replacement".to_vec());
                        }
                    }
                    "full" => {
                        for index in 0..ITEMS {
                            map.insert(index.into(), b"replacement".to_vec());
                        }
                    }
                    _ => unreachable!(),
                })
                .unwrap();
            let elapsed = started.elapsed();
            black_box(store.get_sorted_map(b"key"));
            elapsed
        });

        let set_baseline = sample(|| {
            let mut set = base_set();
            let started_state = set.clone();
            let started = Instant::now();
            match profile {
                "sparse" => {
                    set.insert(ITEMS.to_ne_bytes().to_vec());
                }
                "mixed" => {
                    for index in 0_usize..500 {
                        set.remove(index.to_ne_bytes().as_slice());
                        set.insert((ITEMS + index).to_ne_bytes().to_vec());
                    }
                }
                "full" => {
                    set.clear();
                    set.extend((ITEMS..ITEMS * 2).map(|index| index.to_ne_bytes().to_vec()));
                }
                _ => unreachable!(),
            }
            let elapsed = started.elapsed();
            black_box((started_state, set));
            elapsed
        });

        let map_baseline = sample(|| {
            let mut map = base_map();
            let started = Instant::now();
            match profile {
                "sparse" => {
                    map.insert((ITEMS - 1).into(), b"replacement".to_vec());
                }
                "mixed" => {
                    for index in 0..250 {
                        map.remove(&SearchKey::from(index));
                        map.insert((ITEMS + index).into(), b"added".to_vec());
                    }
                    for index in 250..750 {
                        map.insert(index.into(), b"replacement".to_vec());
                    }
                }
                "full" => {
                    for index in 0..ITEMS {
                        map.insert(index.into(), b"replacement".to_vec());
                    }
                }
                _ => unreachable!(),
            }
            let elapsed = started.elapsed();
            black_box(map);
            elapsed
        });

        let set_durable = sample(|| {
            let store = DurableKeySetStore::new_vec_based();
            for index in 0..ITEMS {
                store.append(b"key".to_vec(), index.to_ne_bytes().to_vec());
            }
            let started = Instant::now();
            match profile {
                "sparse" => store.append(b"key".to_vec(), ITEMS.to_ne_bytes().to_vec()),
                "mixed" => {
                    for index in 0_usize..500 {
                        store.remove_from_set(b"key".to_vec(), index.to_ne_bytes().to_vec());
                        store.append(b"key".to_vec(), (ITEMS + index).to_ne_bytes().to_vec());
                    }
                }
                "full" => {
                    for index in 0..ITEMS {
                        store.remove_from_set(b"key".to_vec(), index.to_ne_bytes().to_vec());
                        store.append(b"key".to_vec(), (ITEMS + index).to_ne_bytes().to_vec());
                    }
                }
                _ => unreachable!(),
            }
            let elapsed = started.elapsed();
            black_box(store.get_hashset(b"key"));
            elapsed
        });

        let map_durable = sample(|| {
            let store = DurableKeyMapStore::new_vec_based();
            for index in 0..ITEMS {
                store.put(b"key".to_vec(), index.into(), index.to_ne_bytes().to_vec());
            }
            let started = Instant::now();
            match profile {
                "sparse" => store.put(b"key".to_vec(), (ITEMS - 1).into(), b"replacement".to_vec()),
                "mixed" => {
                    for index in 0..250 {
                        store.remove_from_sorted_map(b"key".to_vec(), index.into());
                        store.put(b"key".to_vec(), (ITEMS + index).into(), b"added".to_vec());
                    }
                    for index in 250..750 {
                        store.put(b"key".to_vec(), index.into(), b"replacement".to_vec());
                    }
                }
                "full" => {
                    for index in 0..ITEMS {
                        store.put(b"key".to_vec(), index.into(), b"replacement".to_vec());
                    }
                }
                _ => unreachable!(),
            }
            let elapsed = started.elapsed();
            black_box(store.get_sorted_map(b"key"));
            elapsed
        });

        println!(
            "FINAL profile={profile} set_corrected={set_corrected:?} set_equivalent={set_durable:?} set_pre_feature={set_baseline:?} map_corrected={map_corrected:?} map_equivalent={map_durable:?} map_pre_feature={map_baseline:?}"
        );
    }
}
