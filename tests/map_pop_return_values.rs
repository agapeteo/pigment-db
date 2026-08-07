use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::model::SearchKey;

#[test]
fn pop_first_returns_removed_values_for_retained_and_deleted_outer_maps() {
    let directory = tempfile::tempdir().expect("create pop-first return-value directory");
    let retained_key = b"retained-map".to_vec();
    let deleted_key = b"deleted-map".to_vec();
    let first = SearchKey::from(1);
    let second = SearchKey::from(2);
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize pop-first return-value store")
        .into_store();

    store.put(retained_key.clone(), first.clone(), b"first-value".to_vec());
    store.put(
        retained_key.clone(),
        second.clone(),
        b"second-value".to_vec(),
    );
    store.put(deleted_key.clone(), first.clone(), b"only-value".to_vec());

    assert_eq!(
        store
            .try_pop_first(retained_key.clone())
            .expect("persist non-final pop-first"),
        Some((first.clone(), b"first-value".to_vec()))
    );
    assert_eq!(
        store.pop_first(deleted_key.clone()),
        Some((first.clone(), b"only-value".to_vec()))
    );
    assert_eq!(
        store.get_element(&retained_key, &second),
        Some(b"second-value".to_vec())
    );
    assert_eq!(store.get_sorted_map(&deleted_key), None);

    drop(store);
    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("reopen pop-first return-value store")
        .into_store();
    assert_eq!(
        reopened.get_element(&retained_key, &second),
        Some(b"second-value".to_vec())
    );
    assert_eq!(reopened.get_sorted_map(&deleted_key), None);
}

#[test]
fn pop_last_returns_removed_values_for_retained_and_deleted_outer_maps() {
    let directory = tempfile::tempdir().expect("create pop-last return-value directory");
    let retained_key = b"retained-map".to_vec();
    let deleted_key = b"deleted-map".to_vec();
    let first = SearchKey::from(1);
    let last = SearchKey::from(2);
    let store = DurableKeyMapStore::try_init_new(directory.path())
        .expect("initialize pop-last return-value store")
        .into_store();

    store.put(retained_key.clone(), first.clone(), b"first-value".to_vec());
    store.put(retained_key.clone(), last.clone(), b"last-value".to_vec());
    store.put(deleted_key.clone(), last.clone(), b"only-value".to_vec());

    assert_eq!(
        store
            .try_pop_last(retained_key.clone())
            .expect("persist non-final pop-last"),
        Some((last.clone(), b"last-value".to_vec()))
    );
    assert_eq!(
        store.pop_last(deleted_key.clone()),
        Some((last, b"only-value".to_vec()))
    );
    assert_eq!(
        store.get_element(&retained_key, &first),
        Some(b"first-value".to_vec())
    );
    assert_eq!(store.get_sorted_map(&deleted_key), None);

    drop(store);
    let reopened = DurableKeyMapStore::try_init_new(directory.path())
        .expect("reopen pop-last return-value store")
        .into_store();
    assert_eq!(
        reopened.get_element(&retained_key, &first),
        Some(b"first-value".to_vec())
    );
    assert_eq!(reopened.get_sorted_map(&deleted_key), None);
}
