pub mod key_value_store;
pub mod key_set_store;
pub mod model;
mod wal;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
