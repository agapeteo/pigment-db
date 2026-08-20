//! Reusable current-format maintenance fixtures.

#![allow(dead_code)]

use crate::key_map_store::DurableKeyMapStore;
use crate::key_set_store::DurableKeySetStore;
use crate::key_value_store::DurableKeyValueStore;
use crate::model::SearchKey;
use crate::{DurableStoreOptions, WalSegmentSize};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FixtureFamily {
    KeyValue,
    KeySet,
    KeyMap,
}

pub(crate) type DirectoryByteSnapshot = BTreeMap<PathBuf, Vec<u8>>;

pub(crate) fn active_name(family: FixtureFamily) -> &'static OsStr {
    OsStr::new(match family {
        FixtureFamily::KeyValue => "kv.wal.dat",
        FixtureFamily::KeySet => "set.wal.dat",
        FixtureFamily::KeyMap => "map.wal.dat",
    })
}

pub(crate) fn sealed_name(family: FixtureFamily, segment: u64) -> OsString {
    let active = active_name(family).to_string_lossy();
    OsString::from(format!("{active}.segment-{segment:020}"))
}

pub(crate) fn snapshot_directory(root: &Path) -> io::Result<DirectoryByteSnapshot> {
    fn visit(
        root: &Path,
        directory: &Path,
        snapshot: &mut DirectoryByteSnapshot,
    ) -> io::Result<()> {
        let mut entries: Vec<_> = std::fs::read_dir(directory)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_dir() {
                visit(root, &path, snapshot)?;
            } else if kind.is_file() {
                snapshot.insert(
                    path.strip_prefix(root)
                        .expect("snapshot path remains below root")
                        .to_owned(),
                    std::fs::read(path)?,
                );
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "maintenance byte snapshots accept only files and directories",
                ));
            }
        }
        Ok(())
    }

    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot)?;
    Ok(snapshot)
}

pub(crate) fn create_current_v2(root: &Path, family: FixtureFamily) {
    match family {
        FixtureFamily::KeyValue => {
            let store = DurableKeyValueStore::try_init_new(root)
                .unwrap()
                .into_store();
            store.put(b"alpha".to_vec(), b"one".to_vec());
        }
        FixtureFamily::KeySet => {
            let store = DurableKeySetStore::try_init_new(root).unwrap().into_store();
            store.append(b"group".to_vec(), b"red".to_vec());
        }
        FixtureFamily::KeyMap => {
            let store = DurableKeyMapStore::try_init_new(root).unwrap().into_store();
            store.put(b"book".to_vec(), SearchKey::from(1), b"one".to_vec());
        }
    }
}

pub(crate) fn create_segmented_v2(root: &Path, family: FixtureFamily) {
    let options = DurableStoreOptions::default()
        .with_wal_segment_size(WalSegmentSize::try_from(170_u64).unwrap());
    match family {
        FixtureFamily::KeyValue => {
            let store = DurableKeyValueStore::try_init_new_with_options(root, options)
                .unwrap()
                .into_store();
            store.put(b"alpha".to_vec(), b"one".to_vec());
            store.put(b"beta".to_vec(), b"two".to_vec());
        }
        FixtureFamily::KeySet => {
            let store = DurableKeySetStore::try_init_new_with_options(root, options)
                .unwrap()
                .into_store();
            store.append(b"group".to_vec(), b"red".to_vec());
            store.append(b"group".to_vec(), b"blue".to_vec());
        }
        FixtureFamily::KeyMap => {
            let store = DurableKeyMapStore::try_init_new_with_options(root, options)
                .unwrap()
                .into_store();
            store.put(b"book".to_vec(), SearchKey::from(1), b"one".to_vec());
            store.put(b"book".to_vec(), SearchKey::from(2), b"two".to_vec());
        }
    }
    assert!(root.join(sealed_name(family, 0)).is_file());
}

pub(crate) fn create_safe_tail_v2(root: &Path, family: FixtureFamily) {
    create_current_v2(root, family);
    use std::io::Write as _;
    let mut active = std::fs::OpenOptions::new()
        .append(true)
        .open(root.join(active_name(family)))
        .unwrap();
    active.write_all(&[0xa7]).unwrap();
    active.flush().unwrap();
}

pub(crate) fn assert_three_reopens(root: &Path, family: FixtureFamily) {
    for _ in 0..3 {
        match family {
            FixtureFamily::KeyValue => {
                let store = DurableKeyValueStore::try_init_new(root)
                    .unwrap()
                    .into_store();
                assert_eq!(store.get(b"alpha"), Some(b"one".to_vec()));
            }
            FixtureFamily::KeySet => {
                let store = DurableKeySetStore::try_init_new(root).unwrap().into_store();
                assert!(store
                    .get_hashset(b"group")
                    .unwrap()
                    .contains(b"red".as_slice()));
            }
            FixtureFamily::KeyMap => {
                let store = DurableKeyMapStore::try_init_new(root).unwrap().into_store();
                assert_eq!(
                    store.get_element(b"book", &SearchKey::from(1)),
                    Some(b"one".to_vec())
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_snapshots_current_segmented_safe_tail_and_reopens_are_reusable() {
        for family in [
            FixtureFamily::KeyValue,
            FixtureFamily::KeySet,
            FixtureFamily::KeyMap,
        ] {
            assert_eq!(
                sealed_name(family, 7).to_string_lossy(),
                format!(
                    "{}.segment-00000000000000000007",
                    active_name(family).to_string_lossy()
                )
            );

            let current = tempfile::tempdir().unwrap();
            create_current_v2(current.path(), family);
            let first = snapshot_directory(current.path()).unwrap();
            assert!(first.contains_key(Path::new(active_name(family))));
            assert_three_reopens(current.path(), family);

            let segmented = tempfile::tempdir().unwrap();
            create_segmented_v2(segmented.path(), family);
            assert!(snapshot_directory(segmented.path()).unwrap().len() >= 2);

            let safe_tail = tempfile::tempdir().unwrap();
            create_safe_tail_v2(safe_tail.path(), family);
            assert_eq!(
                snapshot_directory(safe_tail.path())
                    .unwrap()
                    .get(Path::new(active_name(family)))
                    .unwrap()
                    .last(),
                Some(&0xa7)
            );
        }
    }
}
