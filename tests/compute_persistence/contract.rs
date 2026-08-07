use crate::support::block_on;
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;

#[test]
fn fallible_and_compatibility_signatures() {
    let store = DurableKeySetStore::new_vec_based();
    let fallible: std::io::Result<()> = store.try_compute(b"set".to_vec(), |_| {});
    assert!(fallible.is_ok());
    let compatibility: () = store.compute(b"set".to_vec(), |_| {});
    assert_eq!(compatibility, ());
    let fallible: std::io::Result<()> = store.try_compute_if_present(b"set".to_vec(), |_| {});
    assert!(fallible.is_ok());
    let compatibility: () = store.compute_if_present(b"set".to_vec(), |_| {});
    assert_eq!(compatibility, ());
    let fallible: std::io::Result<()> = store.try_compute_if_absent(b"occupied".to_vec(), |_| {});
    assert!(fallible.is_ok());
    let compatibility: () = store.compute_if_absent(b"occupied".to_vec(), |_| {});
    assert_eq!(compatibility, ());
    let fallible: std::io::Result<()> =
        block_on(store.try_compute_async(b"async".to_vec(), async |_| {}));
    assert!(fallible.is_ok());
    let compatibility: () = block_on(store.compute_async(b"async".to_vec(), async |_| {}));
    assert_eq!(compatibility, ());
}

#[test]
fn map_fallible_and_compatibility_signatures() {
    let store = DurableKeyMapStore::new_vec_based();
    store.put(b"map".to_vec(), 0.into(), b"value".to_vec());
    let fallible: std::io::Result<()> = store.try_compute(b"map".to_vec(), |_| {});
    assert!(fallible.is_ok());
    let compatibility: () = store.compute(b"map".to_vec(), |_| {});
    assert_eq!(compatibility, ());
    let fallible: std::io::Result<()> = store.try_compute_if_present(b"map".to_vec(), |_| {});
    assert!(fallible.is_ok());
    let compatibility: () = store.compute_if_present(b"map".to_vec(), |_| {});
    assert_eq!(compatibility, ());
    let fallible: std::io::Result<()> = store.try_compute_if_absent(b"other-map".to_vec(), |_| {});
    assert!(fallible.is_ok());
    let compatibility: () = store.compute_if_absent(b"other-map".to_vec(), |_| {});
    assert_eq!(compatibility, ());
}
