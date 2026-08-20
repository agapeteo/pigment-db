//! Public-only maintenance integration-test helpers.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(crate) const CHECKPOINT_EXIT_CODE: i32 = 87;
pub(crate) const CHILD_MODE_ENV: &str = "PIGMENT_DB_MAINTENANCE_CHILD_MODE";
pub(crate) const STORE_DIR_ENV: &str = "PIGMENT_DB_MAINTENANCE_STORE_DIR";
pub(crate) const PHASE_ENV: &str = "PIGMENT_DB_MAINTENANCE_PHASE";
pub(crate) const CUT_ENV: &str = "PIGMENT_DB_MAINTENANCE_CUT";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Family {
    KeyValue,
    KeySet,
    KeyMap,
}

pub(crate) fn active_name(family: Family) -> &'static OsStr {
    OsStr::new(match family {
        Family::KeyValue => "kv.wal.dat",
        Family::KeySet => "set.wal.dat",
        Family::KeyMap => "map.wal.dat",
    })
}

pub(crate) fn sealed_name(family: Family, segment: u64) -> OsString {
    OsString::from(format!(
        "{}.segment-{segment:020}",
        active_name(family).to_string_lossy()
    ))
}

pub(crate) fn byte_snapshot(root: &Path) -> io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut snapshot = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "integration fixture directory must contain only files",
            ));
        }
        snapshot.insert(
            PathBuf::from(entry.file_name()),
            std::fs::read(entry.path())?,
        );
    }
    Ok(snapshot)
}

pub(crate) fn run_checkpoint_child(
    exact_test_name: &str,
    store_dir: &Path,
    phase: &str,
    cut: &str,
) -> (BTreeMap<PathBuf, Vec<u8>>, BTreeMap<PathBuf, Vec<u8>>) {
    let before = byte_snapshot(store_dir).expect("snapshot before checkpoint child");
    let executable = std::env::current_exe().expect("locate integration-test executable");
    let mut child = std::process::Command::new(executable)
        .arg(exact_test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, "1")
        .env(STORE_DIR_ENV, store_dir)
        .env(PHASE_ENV, phase)
        .env(CUT_ENV, cut)
        .spawn()
        .expect("spawn maintenance integration child");
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("poll maintenance child") {
            assert_eq!(status.code(), Some(CHECKPOINT_EXIT_CODE));
            break;
        }
        if started.elapsed() >= Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("maintenance integration child timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let after = byte_snapshot(store_dir).expect("snapshot after checkpoint child");
    (before, after)
}
