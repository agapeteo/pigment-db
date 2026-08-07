//! Public compatibility coverage for the durability-policy feature.

use std::cell::Cell;
use std::fs;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::DurableStoreOptions;

use super::support::scratch_directory;

#[test]
fn buffered_bytes_and_no_options_reopen_remain_compatible_for_every_family() {
    let root = scratch_directory("pigment-buffered-reopen-");
    let value_dir = root.path().join("value");
    let set_dir = root.path().join("set");
    let map_dir = root.path().join("map");
    for directory in [&value_dir, &set_dir, &map_dir] {
        fs::create_dir(directory).unwrap();
    }

    let value =
        DurableKeyValueStore::try_init_new_with_options(&value_dir, DurableStoreOptions::default())
            .unwrap()
            .into_store();
    value.put(b"key".to_vec(), b"value".to_vec());
    drop(value);
    let value_bytes = fs::read(value_dir.join("kv.wal.dat")).unwrap();
    let value = DurableKeyValueStore::try_init_new(&value_dir)
        .unwrap()
        .into_store();
    assert_eq!(value.get(b"key"), Some(b"value".to_vec()));
    drop(value);
    assert_eq!(fs::read(value_dir.join("kv.wal.dat")).unwrap(), value_bytes);

    let set =
        DurableKeySetStore::try_init_new_with_options(&set_dir, DurableStoreOptions::default())
            .unwrap()
            .into_store();
    set.append(b"key".to_vec(), b"member".to_vec());
    drop(set);
    let set_bytes = fs::read(set_dir.join("set.wal.dat")).unwrap();
    let set = DurableKeySetStore::try_init_new(&set_dir)
        .unwrap()
        .into_store();
    assert!(set.contains_in_set(b"key", b"member"));
    drop(set);
    assert_eq!(fs::read(set_dir.join("set.wal.dat")).unwrap(), set_bytes);

    let map =
        DurableKeyMapStore::try_init_new_with_options(&map_dir, DurableStoreOptions::default())
            .unwrap()
            .into_store();
    map.put(b"key".to_vec(), SearchKey::from(1_usize), b"entry".to_vec());
    drop(map);
    let map_bytes = fs::read(map_dir.join("map.wal.dat")).unwrap();
    let map = DurableKeyMapStore::try_init_new(&map_dir)
        .unwrap()
        .into_store();
    assert_eq!(
        map.get_element(b"key", &SearchKey::from(1_usize)),
        Some(b"entry".to_vec())
    );
    drop(map);
    assert_eq!(fs::read(map_dir.join("map.wal.dat")).unwrap(), map_bytes);
}

#[test]
fn buffered_exact_no_op_callbacks_run_once_without_wal_growth() {
    let root = scratch_directory("pigment-buffered-noop-");
    let set_dir = root.path().join("set");
    let map_dir = root.path().join("map");
    fs::create_dir(&set_dir).unwrap();
    fs::create_dir(&map_dir).unwrap();

    let set = DurableKeySetStore::try_init_new(&set_dir)
        .unwrap()
        .into_store();
    set.append(b"key".to_vec(), b"member".to_vec());
    let set_length = fs::metadata(set_dir.join("set.wal.dat")).unwrap().len();
    let set_callbacks = Cell::new(0);
    set.try_compute(b"key".to_vec(), |_| {
        set_callbacks.set(set_callbacks.get() + 1)
    })
    .unwrap();
    assert_eq!(set_callbacks.get(), 1);
    assert_eq!(
        fs::metadata(set_dir.join("set.wal.dat")).unwrap().len(),
        set_length
    );

    let map = DurableKeyMapStore::try_init_new(&map_dir)
        .unwrap()
        .into_store();
    map.put(b"key".to_vec(), SearchKey::from(1_usize), b"entry".to_vec());
    let map_length = fs::metadata(map_dir.join("map.wal.dat")).unwrap().len();
    let map_callbacks = Cell::new(0);
    map.try_compute(b"key".to_vec(), |_| {
        map_callbacks.set(map_callbacks.get() + 1)
    })
    .unwrap();
    assert_eq!(map_callbacks.get(), 1);
    assert_eq!(
        fs::metadata(map_dir.join("map.wal.dat")).unwrap().len(),
        map_length
    );
}
