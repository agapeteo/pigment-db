use pigment_db::key_value_store::DurableKeyValueStore;

#[test]
fn overflowing_increment_is_rejected_without_changing_number() {
    let store = DurableKeyValueStore::new_vec_based();
    let key = b"counter".to_vec();

    assert_eq!(
        store.increment_or_init(key.clone(), u64::MAX - 1),
        Ok(u64::MAX - 1)
    );
    assert_eq!(store.increment_or_init(key.clone(), 1), Ok(u64::MAX));
    assert_eq!(store.increment_or_init(key.clone(), 1), Err(()));
    assert_eq!(store.read_number(&key), Some(Ok(u64::MAX)));
}

#[test]
fn try_overflow_is_rejected_without_persisting_a_wrapped_value() {
    let directory = tempfile::tempdir().expect("create numeric overflow directory");
    let key = b"counter".to_vec();
    let store = DurableKeyValueStore::try_init_new(directory.path())
        .expect("initialize numeric overflow store")
        .into_store();

    store
        .try_set_number(key.clone(), u64::MAX)
        .expect("persist maximum counter value");
    assert_eq!(
        store
            .try_increment_or_init(key.clone(), 1)
            .expect("overflow is a numeric rejection, not an I/O failure"),
        Err(())
    );
    drop(store);

    let reopened = DurableKeyValueStore::try_init_new(directory.path())
        .expect("reopen numeric overflow store")
        .into_store();
    assert_eq!(reopened.read_number(&key), Some(Ok(u64::MAX)));
}
