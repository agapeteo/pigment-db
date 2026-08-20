//! Private storage-compaction implementation.

pub(crate) mod inspection;
pub(crate) mod manifest;
pub(crate) mod publication;
pub(crate) mod recovery;

#[cfg(test)]
mod closed_tests;
#[cfg(test)]
mod inspection_tests;
#[cfg(test)]
mod online_tests;
#[cfg(test)]
mod recovery_tests;

#[cfg(test)]
pub(crate) fn test_sentinel() {
    inspection::test_sentinel();
    manifest::test_sentinel();
    publication::test_sentinel();
    recovery::test_sentinel();
}
