use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::{Key, SearchKey};

#[test]
fn ordered_append_ignores_later_non_numeric_keys_without_overwriting_zero() {
    let directory = tempfile::tempdir().expect("create ordered-append directory");
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize ordered-append store")
        .into_store();
    let outer_key = b"map".to_vec();

    store.put(
        outer_key.clone(),
        SearchKey::from(0_usize),
        b"zero".to_vec(),
    );
    store.put(
        outer_key.clone(),
        SearchKey::from("later"),
        b"string".to_vec(),
    );

    store
        .try_append_ordered_element(outer_key.clone(), b"appended".to_vec())
        .expect("mixed search-key variants must not prevent append");

    assert_eq!(
        store.get_element(&outer_key, &SearchKey::from(0_usize)),
        Some(b"zero".to_vec())
    );
    assert_eq!(
        store.get_element(&outer_key, &SearchKey::from(1_usize)),
        Some(b"appended".to_vec())
    );
    drop(store);

    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("reopen ordered-append store")
        .into_store();
    assert_eq!(
        reopened.get_element(&outer_key, &SearchKey::from(0_usize)),
        Some(b"zero".to_vec())
    );
    assert_eq!(
        reopened.get_element(&outer_key, &SearchKey::from(1_usize)),
        Some(b"appended".to_vec())
    );
}

#[test]
fn ordered_append_rejects_an_exhausted_numeric_keyspace_without_publication() {
    let store = DurableKeyMapStore::new_vec_based();
    let outer_key = b"map".to_vec();
    let maximum = SearchKey::from(usize::MAX);
    store.put(outer_key.clone(), maximum.clone(), b"maximum".to_vec());

    let error = store
        .try_append_ordered_element(outer_key.clone(), b"wrapped".to_vec())
        .expect_err("usize::MAX has no valid ordered successor");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(store.get_sorted_map(&outer_key).unwrap().len(), 1);
    assert_eq!(
        store.get_element(&outer_key, &maximum),
        Some(b"maximum".to_vec())
    );
}

#[test]
fn ordered_append_ignores_an_empty_search_key() {
    let store = DurableKeyMapStore::new_vec_based();
    let outer_key = b"map".to_vec();
    let empty = SearchKey::from(Vec::<Key>::new());
    store.put(outer_key.clone(), empty.clone(), b"empty".to_vec());

    store
        .try_append_ordered_element(outer_key.clone(), b"zero".to_vec())
        .expect("an empty heterogeneous key must not panic");

    assert_eq!(
        store.get_element(&outer_key, &empty),
        Some(b"empty".to_vec())
    );
    assert_eq!(
        store.get_element(&outer_key, &SearchKey::from(0_usize)),
        Some(b"zero".to_vec())
    );
}

#[test]
fn ordered_append_uses_the_numeric_first_component_of_composite_keys() {
    let store = DurableKeyMapStore::new_vec_based();
    let outer_key = b"map".to_vec();
    let composite = SearchKey::from(vec![Key::USIZE(4), Key::Str("suffix".to_owned())]);
    store.put(outer_key.clone(), composite.clone(), b"composite".to_vec());

    store
        .try_append_ordered_element(outer_key.clone(), b"next".to_vec())
        .expect("composite numeric-prefix keys participate in append ordering");

    assert_eq!(
        store.get_element(&outer_key, &composite),
        Some(b"composite".to_vec())
    );
    assert_eq!(
        store.get_element(&outer_key, &SearchKey::from(5_usize)),
        Some(b"next".to_vec())
    );
}
