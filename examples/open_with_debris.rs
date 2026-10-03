//! Open cost with and without a closed compaction's unpublished debris: the performance gate of
//! specs/015 (plan.md, IV).
//!
//! ```text
//! cargo build --release --locked --example open_with_debris
//! open_with_debris build SHAPE DIR     writes the store DIR/clean/store
//! open_with_debris stage DIR           writes DIR/debris, DIR/torn and DIR/refusal
//! open_with_debris open FAMILY DIR     opens DIR/store as FAMILY (kv, set or map)
//! ```
//!
//! `build` writes one of these shapes through the public API, with default options:
//! - `map1`: a sorted map of 2,000,000 keys with one entry each;
//! - `set1`: a key/set family of 2,000,000 keys with one member each;
//! - `map10`: a sorted map of 200,000 keys with ten entries each;
//! - `kv`: a key/value family of 1,000,000 keys with 131-byte values;
//! - `multi`: the key/value family of 600,000 keys and a key/set family of 1,000 keys with one
//!   member each, in one directory.
//!
//! `stage` compacts a copy of `DIR/clean/store` in place and, beside three more copies of the
//! store, writes what that compaction staged (the compacted copy's active files) as a staging
//! directory with no manifest: whole (`debris`, which an open removes), with its largest file cut
//! by one byte (`torn`, a staging write killed inside its bytes, which an open removes since the
//! fifth review), and with the last byte of its largest file changed (`refusal`, which an open
//! must compare to its end and then refuse).
//!
//! `open` opens the store in this process and prints the open's status, its time in seconds and
//! the process's peak resident set in KiB (`VmHWM` from `/proc/self/status`; `n/a` where there is
//! none). Run one process per open, each on a fresh copy of the same directory, for example:
//!
//! ```text
//! for run in 1 2; do
//!   rm -rf work && cp -a DIR/debris work && sync && open_with_debris open map work
//! done
//! ```
//!
//! To compare two revisions, build this file against each and run the two binaries alternately on
//! one machine over the same data. `Cargo.lock` is not tracked, so a `git archive` or a fresh
//! clone of a revision has none and `--locked` refuses to build there: copy the working tree's
//! `Cargo.lock` into the other revision's copy first, so that both build against the same
//! dependency versions.

use std::path::{Path, PathBuf};
use std::time::Instant;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;

/// The staging directory beside a store named `store`.
const STAGING: &str = ".store.pigment-compact.next";

/// Every family's active file name.
const ACTIVE_FILES: [&str; 3] = ["kv.wal.dat", "set.wal.dat", "map.wal.dat"];

fn key_values(store: &Path, keys: u64) {
    let values = DurableKeyValueStore::try_init_new(store)
        .unwrap()
        .into_store();
    for key in 0..keys {
        values.put(
            format!("key-{key:012}").into_bytes(),
            format!("value-{key:012}-{}", "0123456789abcdef".repeat(7)).into_bytes(),
        );
    }
}

fn key_sets(store: &Path, keys: u64, members: u64) {
    let sets = DurableKeySetStore::try_init_new(store)
        .unwrap()
        .into_store();
    for key in 0..keys {
        for member in 0..members {
            sets.append(
                format!("key-{key:09}").into_bytes(),
                format!("m{member:06}").into_bytes(),
            );
        }
    }
}

fn key_maps(store: &Path, keys: u64, entries: usize) {
    let maps = DurableKeyMapStore::try_init_new(store)
        .unwrap()
        .into_store();
    for key in 0..keys {
        for entry in 0..entries {
            maps.put(
                format!("key-{key:09}").into_bytes(),
                SearchKey::from(entry),
                format!("v{entry:06}").into_bytes(),
            );
        }
    }
}

/// Copies the regular files of `from` into a new directory `to`, except a lock file.
fn copy_store(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() && entry.file_name() != ".pigment-lock" {
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
}

fn build(shape: &str, dir: &Path) {
    let store = dir.join("clean").join("store");
    std::fs::create_dir_all(&store).unwrap();
    match shape {
        "map1" => key_maps(&store, 2_000_000, 1),
        "set1" => key_sets(&store, 2_000_000, 1),
        "map10" => key_maps(&store, 200_000, 10),
        "kv" => key_values(&store, 1_000_000),
        "multi" => {
            key_values(&store, 600_000);
            key_sets(&store, 1_000, 1);
        }
        other => panic!("unknown shape {other}"),
    }
}

fn stage(dir: &Path) {
    let clean = dir.join("clean").join("store");
    let compacted = dir.join("compacted").join("store");
    copy_store(&clean, &compacted);
    pigment_db::compact_directory_in_place(
        &compacted,
        pigment_db::ClosedCompactionOptions::default(),
    )
    .unwrap();
    for variant in ["debris", "torn", "refusal"] {
        let store = dir.join(variant).join("store");
        copy_store(&clean, &store);
        let staging = dir.join(variant).join(STAGING);
        std::fs::create_dir(&staging).unwrap();
        let mut largest: Option<(u64, PathBuf)> = None;
        for name in ACTIVE_FILES {
            if store.join(name).is_file() {
                let staged = staging.join(name);
                let length = std::fs::copy(compacted.join(name), &staged).unwrap();
                if largest.as_ref().is_none_or(|(most, _)| length > *most) {
                    largest = Some((length, staged));
                }
            }
        }
        let (_, largest) = largest.expect("the store holds a family");
        let mut bytes = std::fs::read(&largest).unwrap();
        match variant {
            "torn" => {
                bytes.pop();
            }
            "refusal" => *bytes.last_mut().unwrap() ^= 0xff,
            _ => {}
        }
        std::fs::write(&largest, bytes).unwrap();
    }
    std::fs::remove_dir_all(dir.join("compacted")).unwrap();
}

fn peak_resident_kib() -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmHWM"))
                .and_then(|line| line.split_whitespace().nth(1).map(str::to_owned))
        })
        .unwrap_or_else(|| "n/a".to_owned())
}

fn open(family: &str, dir: &Path) {
    let store = dir.join("store");
    let started = Instant::now();
    let status = match family {
        "kv" => DurableKeyValueStore::try_init_new(&store).map(|opened| opened.status()),
        "set" => DurableKeySetStore::try_init_new(&store).map(|opened| opened.status()),
        "map" => DurableKeyMapStore::try_init_new(&store).map(|opened| opened.status()),
        other => panic!("unknown family {other}"),
    };
    let elapsed = started.elapsed().as_secs_f64();
    let status = match status {
        Ok(status) => format!("{status:?}"),
        Err(error) => format!("{error:?}")
            .split([' ', '{', '('])
            .next()
            .unwrap_or("Err")
            .to_owned(),
    };
    let staging_left = dir.join(STAGING).exists();
    println!(
        "{status} {elapsed:.2} {} staging-left={staging_left}",
        peak_resident_kib()
    );
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["build", shape, dir] => build(shape, Path::new(dir)),
        ["stage", dir] => stage(Path::new(dir)),
        ["open", family, dir] => open(family, Path::new(dir)),
        _ => {
            eprintln!(
                "usage: open_with_debris build SHAPE DIR | stage DIR | open FAMILY DIR (see the \
                 file's documentation)"
            );
            std::process::exit(2);
        }
    }
}
