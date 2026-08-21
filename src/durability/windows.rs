//! Windows namespace-durability boundary.
//!
//! Standard Rust rename APIs do not expose Windows write-through namespace
//! publication. The native call will therefore live in this module, with safe
//! path and operation wrappers presented to the rest of the crate.

#![allow(dead_code, unsafe_code)]

use std::io;
use std::io::{Read, Write};
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::path::{Component, Prefix};
use std::sync::atomic::{AtomicU64, Ordering};
use windows_sys::core::PCWSTR;
use windows_sys::Win32::Storage::FileSystem::{
    MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MOVE_FILE_FLAGS,
};

#[cfg(test)]
thread_local! {
    static NATIVE_MOVE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_native_move_calls() {
    NATIVE_MOVE_CALLS.set(0);
}

#[cfg(test)]
pub(super) fn native_move_calls() -> usize {
    NATIVE_MOVE_CALLS.get()
}

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
        if units.len() + 1 >= 260 && path.is_absolute() {
            match path.components().next() {
                Some(Component::Prefix(prefix)) => match prefix.kind() {
                    Prefix::Disk(_) => {
                        let mut verbatim = r"\\?\".encode_utf16().collect::<Vec<_>>();
                        verbatim.extend_from_slice(&units);
                        units = verbatim;
                    }
                    Prefix::UNC(_, _) => {
                        let mut verbatim = r"\\?\UNC\".encode_utf16().collect::<Vec<_>>();
                        verbatim.extend_from_slice(&units[2..]);
                        units = verbatim;
                    }
                    Prefix::Verbatim(_)
                    | Prefix::VerbatimUNC(_, _)
                    | Prefix::VerbatimDisk(_)
                    | Prefix::DeviceNS(_) => {}
                },
                _ => {}
            }
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

static NEXT_PREFLIGHT_ID: AtomicU64 = AtomicU64::new(1);

fn create_disposable(
    directory: &Path,
    role: &str,
) -> io::Result<(std::path::PathBuf, std::fs::File, Vec<u8>)> {
    for _ in 0..1_024 {
        let id = NEXT_PREFLIGHT_ID.fetch_add(1, Ordering::Relaxed);
        let token = format!("pigment-db-preflight-{}-{id}", std::process::id()).into_bytes();
        let path = directory.join(format!(
            ".pigment-db-preflight-{}-{id}-{role}",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file, token)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique Windows durability preflight artifact",
    ))
}

pub(super) fn preflight_file_content(directory: &Path) -> io::Result<()> {
    let (path, mut file, token) = create_disposable(directory, "content")?;
    let operation = (|| {
        file.write_all(&token)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        let mut reopened = std::fs::File::open(&path)?;
        let mut persisted = Vec::new();
        reopened.read_to_end(&mut persisted)?;
        if persisted != token {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows file-content preflight reopened different bytes",
            ));
        }
        Ok(())
    })();
    let cleanup = std::fs::remove_file(&path);
    match (operation, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn synchronize_disposable(mut file: std::fs::File, token: &[u8]) -> io::Result<()> {
    file.write_all(token)?;
    file.flush()?;
    file.sync_all()
}

fn validate_disposable(path: &Path, token: &[u8]) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::other(
            "Windows durability preflight artifact changed file type",
        ));
    }
    if std::fs::read(path)? != token {
        return Err(io::Error::other(
            "Windows durability preflight artifact changed identity",
        ));
    }
    Ok(())
}

fn cleanup_disposable(path: &Path, accepted_tokens: &[&[u8]]) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(io::Error::other(
                "refusing to remove a replaced Windows preflight artifact",
            ));
        }
        Ok(_) => {}
    }
    let bytes = std::fs::read(path)?;
    if !accepted_tokens.iter().any(|token| *token == bytes) {
        return Err(io::Error::other(
            "refusing to remove a Windows preflight artifact with changed identity",
        ));
    }
    std::fs::remove_file(path)
}

pub(super) fn preflight_namespace(directory: &Path) -> io::Result<()> {
    let (source, source_file, source_token) = create_disposable(directory, "move-source")?;
    let destination = source.with_extension("move-destination");
    let mut replacement_source = None;
    let mut replacement_token = Vec::new();
    let operation = (|| {
        synchronize_disposable(source_file, &source_token)?;
        move_file_write_through(&source, &destination, NamespaceMoveMode::NoReplace)?;
        validate_disposable(&destination, &source_token)?;

        let (path, file, token) = create_disposable(directory, "replace-source")?;
        replacement_source = Some(path.clone());
        replacement_token = token;
        synchronize_disposable(file, &replacement_token)?;
        move_file_write_through(&path, &destination, NamespaceMoveMode::ReplaceExisting)?;
        validate_disposable(&destination, &replacement_token)?;
        Ok(())
    })();

    let mut cleanup_error = cleanup_disposable(&source, &[&source_token]).err();
    if let Some(path) = &replacement_source {
        cleanup_error = cleanup_error
            .or_else(|| cleanup_disposable(path, &[replacement_token.as_slice()]).err());
    }
    cleanup_error = cleanup_error.or_else(|| {
        cleanup_disposable(
            &destination,
            &[source_token.as_slice(), replacement_token.as_slice()],
        )
        .err()
    });
    match (operation, cleanup_error) {
        (Err(error), _) => Err(error),
        (Ok(()), Some(error)) => Err(error),
        (Ok(()), None) => Ok(()),
    }
}

pub(super) fn move_file_write_through(
    source: &Path,
    destination: &Path,
    mode: NamespaceMoveMode,
) -> io::Result<()> {
    #[cfg(test)]
    NATIVE_MOVE_CALLS.set(NATIVE_MOVE_CALLS.get() + 1);
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
    fn long_absolute_drive_paths_gain_the_required_verbatim_prefix() {
        let path = PathBuf::from(format!(r"C:\pigment\{}\database", "a".repeat(300)));
        let expected = format!(r"\\?\C:\pigment\{}\database", "a".repeat(300));

        let encoded = WidePath::try_from(path.as_path()).unwrap();
        let expected = OsString::from(expected)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();

        assert_eq!(encoded.units(), expected);
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

    #[test]
    fn non_delete_sharing_handle_conflict_has_no_rename_fallback() {
        use std::os::windows::fs::OpenOptionsExt;

        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("held-source");
        let destination = directory.path().join("held-destination");
        std::fs::write(&source, b"authoritative").unwrap();
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&source)
            .unwrap();

        let error = move_file_write_through(&source, &destination, NamespaceMoveMode::NoReplace)
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(32));
        assert_eq!(std::fs::read(&source).unwrap(), b"authoritative");
        assert!(!destination.exists());
        drop(held);
    }
}
