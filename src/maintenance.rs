//! Private maintenance API assembly point.

#[cfg(test)]
pub(crate) fn test_sentinel() {
    crate::compaction::test_sentinel();
    crate::wal::maintenance_test_sentinel();
}
