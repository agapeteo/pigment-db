//! Windows namespace-durability boundary.
//!
//! Standard Rust rename APIs do not expose Windows write-through namespace
//! publication. The native call will therefore live in this module, with safe
//! path and operation wrappers presented to the rest of the crate.

#![allow(dead_code, unsafe_code)]

use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NamespaceMoveMode {
    NoReplace,
    ReplaceExisting,
}

pub(super) fn move_file_write_through(
    _source: &Path,
    _destination: &Path,
    _mode: NamespaceMoveMode,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Windows write-through namespace publication is not implemented yet",
    ))
}
