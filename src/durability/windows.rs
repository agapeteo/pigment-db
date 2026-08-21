//! Windows namespace-durability boundary.
//!
//! Standard Rust rename APIs do not expose Windows write-through namespace
//! publication. The native call will therefore live in this module, with safe
//! path and operation wrappers presented to the rest of the crate.

#![allow(dead_code, unsafe_code)]

use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use windows_sys::core::PCWSTR;
use windows_sys::Win32::Storage::FileSystem::{
    MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MOVE_FILE_FLAGS,
};

#[derive(Debug)]
struct WidePath {
    units: Vec<u16>,
}

impl TryFrom<&Path> for WidePath {
    type Error = io::Error;

    fn try_from(path: &Path) -> io::Result<Self> {
        let mut units = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if units.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows paths passed to native durability operations cannot contain NUL",
            ));
        }
        units.push(0);
        Ok(Self { units })
    }
}

impl WidePath {
    /// Returns a pointer valid for exactly as long as this owned encoding is alive.
    fn as_pcwstr(&self) -> PCWSTR {
        self.units.as_ptr()
    }

    #[cfg(test)]
    fn units(&self) -> &[u16] {
        &self.units
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NamespaceMoveMode {
    NoReplace,
    ReplaceExisting,
}

fn move_flags(mode: NamespaceMoveMode) -> MOVE_FILE_FLAGS {
    match mode {
        NamespaceMoveMode::NoReplace => MOVEFILE_WRITE_THROUGH,
        NamespaceMoveMode::ReplaceExisting => MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING,
    }
}

pub(super) fn move_file_write_through(
    source: &Path,
    destination: &Path,
    mode: NamespaceMoveMode,
) -> io::Result<()> {
    let source = WidePath::try_from(source)?;
    let destination = WidePath::try_from(destination)?;
    // SAFETY: both pointers reference owned, NUL-terminated UTF-16 buffers
    // that remain alive for the complete call. Flags never permit a copy
    // fallback, so success represents a same-volume namespace move.
    let result = unsafe {
        MoveFileExW(
            source.as_pcwstr(),
            destination.as_pcwstr(),
            move_flags(mode),
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::path::PathBuf;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_COPY_ALLOWED, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    #[test]
    fn unicode_and_supported_long_paths_are_lossless_utf16() {
        for path in [
            PathBuf::from(r"C:\pigment\数据\🦀\database"),
            PathBuf::from(format!(r"\\?\C:\pigment\{}\database", "a".repeat(300))),
        ] {
            let expected: Vec<_> = path
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();

            let encoded = WidePath::try_from(path.as_path()).unwrap();

            assert_eq!(encoded.units(), expected);
        }
    }

    #[test]
    fn native_pointer_is_owned_stable_and_exactly_nul_terminated() {
        let encoded = WidePath::try_from(Path::new(r"C:\pigment\database")).unwrap();
        let pointer = encoded.as_pcwstr();
        let moved = encoded;

        assert_eq!(pointer, moved.as_pcwstr());
        assert_eq!(moved.units().last(), Some(&0));
        assert_eq!(moved.units().iter().filter(|unit| **unit == 0).count(), 1);
    }

    #[test]
    fn interior_nul_is_rejected_before_ffi() {
        let path = PathBuf::from(OsString::from_wide(&[
            b'C' as u16,
            b':' as u16,
            b'\\' as u16,
            b'a' as u16,
            0,
            b'b' as u16,
        ]));

        let error = WidePath::try_from(path.as_path()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn namespace_move_flags_are_exact_and_never_allow_cross_volume_copy() {
        assert_eq!(
            move_flags(NamespaceMoveMode::NoReplace),
            MOVEFILE_WRITE_THROUGH
        );
        assert_eq!(
            move_flags(NamespaceMoveMode::ReplaceExisting),
            MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING
        );
        for mode in [
            NamespaceMoveMode::NoReplace,
            NamespaceMoveMode::ReplaceExisting,
        ] {
            assert_eq!(move_flags(mode) & MOVEFILE_COPY_ALLOWED, 0);
        }
    }

    #[test]
    fn same_directory_moves_enforce_destination_mode_and_preserve_win32_error() {
        let directory = tempfile::tempdir().unwrap();
        let fresh_source = directory.path().join("fresh-source");
        let fresh_destination = directory.path().join("fresh-destination");
        std::fs::write(&fresh_source, b"fresh").unwrap();

        move_file_write_through(
            &fresh_source,
            &fresh_destination,
            NamespaceMoveMode::NoReplace,
        )
        .unwrap();
        assert!(!fresh_source.exists());
        assert_eq!(std::fs::read(&fresh_destination).unwrap(), b"fresh");

        let conflict_source = directory.path().join("conflict-source");
        let conflict_destination = directory.path().join("conflict-destination");
        std::fs::write(&conflict_source, b"replacement").unwrap();
        std::fs::write(&conflict_destination, b"original").unwrap();

        let error = move_file_write_through(
            &conflict_source,
            &conflict_destination,
            NamespaceMoveMode::NoReplace,
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(183));
        assert_eq!(std::fs::read(&conflict_source).unwrap(), b"replacement");
        assert_eq!(std::fs::read(&conflict_destination).unwrap(), b"original");

        move_file_write_through(
            &conflict_source,
            &conflict_destination,
            NamespaceMoveMode::ReplaceExisting,
        )
        .unwrap();
        assert!(!conflict_source.exists());
        assert_eq!(
            std::fs::read(&conflict_destination).unwrap(),
            b"replacement"
        );
    }
}
