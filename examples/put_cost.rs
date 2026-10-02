//! Put cost per store family and store kind: the performance gate of specs/014.
//!
//! ```text
//! cargo run --release --locked --example put_cost -- [RUNS] [PUTS]
//! ```
//!
//! For each of the three families, on a memory store and on a file-backed store (default options:
//! Buffered durability, one segment, in a fresh temporary directory), it times RUNS runs (default
//! 7) of PUTS writes (default 100,000) of distinct keys with 100-byte values, and prints the
//! median, fastest and slowest run in nanoseconds per write. Every key and value is built before
//! the clock starts, and the runs of the six configurations are interleaved, so drift in the
//! machine's load reaches all of them alike.
//!
//! To compare two revisions, build this file against each and run the two binaries alternately
//! on one machine (specs/014-tracked-storage-stats/plan.md, Performance gate).

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use pigment_db::DurableStoreOptions;
use std::path::Path;
use std::time::{Duration, Instant};

const VALUE_BYTES: usize = 100;

#[derive(Clone, Copy)]
enum Family {
    Value,
    Set,
    SortedMap,
}

#[derive(Clone, Copy)]
enum Kind {
    Memory,
    File,
}

fn records(puts: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..puts)
        .map(|index| {
            (
                format!("key-{index:09}").into_bytes(),
                vec![b'v'; VALUE_BYTES],
            )
        })
        .collect()
}

fn time_writes(
    records: Vec<(Vec<u8>, Vec<u8>)>,
    mut write: impl FnMut(usize, Vec<u8>, Vec<u8>),
) -> Duration {
    let start = Instant::now();
    for (index, (key, value)) in records.into_iter().enumerate() {
        write(index, key, value);
    }
    start.elapsed()
}

/// One run: a fresh store, then `puts` timed writes. The store is dropped after the clock stops.
fn run(family: Family, kind: Kind, puts: usize) -> Duration {
    let records = records(puts);
    let directory = tempfile::tempdir().expect("create a temporary store directory");
    let path: &Path = directory.path();
    let options = DurableStoreOptions::default();
    match (family, kind) {
        (Family::Value, Kind::Memory) => {
            let store = DurableKeyValueStore::new_vec_based();
            time_writes(records, |_, key, value| store.put(key, value))
        }
        (Family::Value, Kind::File) => {
            let store = DurableKeyValueStore::try_init_new_with_options(path, options)
                .expect("open a key/value store")
                .into_store();
            time_writes(records, |_, key, value| store.put(key, value))
        }
        (Family::Set, Kind::Memory) => {
            let store = DurableKeySetStore::new_vec_based();
            time_writes(records, |_, key, value| store.append(key, value))
        }
        (Family::Set, Kind::File) => {
            let store = DurableKeySetStore::try_init_new_with_options(path, options)
                .expect("open a key/set store")
                .into_store();
            time_writes(records, |_, key, value| store.append(key, value))
        }
        (Family::SortedMap, Kind::Memory) => {
            let store = DurableKeyMapStore::new_vec_based();
            time_writes(records, |index, key, value| {
                store.put(key, SearchKey::from(index), value)
            })
        }
        (Family::SortedMap, Kind::File) => {
            let store = DurableKeyMapStore::try_init_new_with_options(path, options)
                .expect("open a key/sorted-map store")
                .into_store();
            time_writes(records, |index, key, value| {
                store.put(key, SearchKey::from(index), value)
            })
        }
    }
}

fn argument(position: usize, default: usize) -> usize {
    std::env::args().nth(position).map_or(default, |text| {
        text.parse()
            .unwrap_or_else(|_| panic!("argument {position} is not a count: {text}"))
    })
}

fn main() {
    let runs = argument(1, 7).max(1);
    let puts = argument(2, 100_000).max(1);
    let configurations = [
        ("key/value", Family::Value, "memory", Kind::Memory),
        ("key/value", Family::Value, "file", Kind::File),
        ("key/set", Family::Set, "memory", Kind::Memory),
        ("key/set", Family::Set, "file", Kind::File),
        ("key/sorted-map", Family::SortedMap, "memory", Kind::Memory),
        ("key/sorted-map", Family::SortedMap, "file", Kind::File),
    ];
    let mut nanos_per_put = vec![Vec::with_capacity(runs); configurations.len()];
    for _ in 0..runs {
        for (samples, &(_, family, _, kind)) in nanos_per_put.iter_mut().zip(&configurations) {
            samples.push(run(family, kind, puts).as_nanos() / puts as u128);
        }
    }
    for (samples, &(family, _, kind, _)) in nanos_per_put.iter_mut().zip(&configurations) {
        samples.sort_unstable();
        println!(
            "family={family} store={kind} median_ns={} min_ns={} max_ns={} runs={runs} puts={puts}",
            samples[samples.len() / 2],
            samples[0],
            samples[samples.len() - 1],
        );
    }
}
