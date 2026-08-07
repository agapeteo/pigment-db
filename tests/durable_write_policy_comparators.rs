//! Protocol-v4 CPU-pinned buffered and direct-barrier comparator captures.
//!
//! This target intentionally uses only APIs available at the pre-feature commit
//! so the file plus its shared protocol/support modules can be installed into a
//! clean checkout of that commit without copying production changes.

#[path = "durable_write_policy/benchmark_protocol.rs"]
mod benchmark_protocol;
#[path = "durable_write_policy/support.rs"]
mod support;

use benchmark_protocol::{measure_cell, BenchStore};
use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::{SearchKey, SortedMapEntry, SortedMapKey};
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::hint::black_box;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use support::{
    benchmark_root, capture_id, collect_provenance, fixed_bytes, output_path, write_capture_csv,
    CaptureRow, CellKey, Implementation, Policy, StorageMode, StoreFamily, Workload, SAMPLE_COUNT,
};
use tempfile::TempDir;

const V1_RECORD_OVERHEAD: usize = 46;

enum KeyValueStore {
    Vector(DurableKeyValueStore<Vec<u8>>),
    File {
        store: DurableKeyValueStore<File>,
        _directory: TempDir,
    },
}

impl KeyValueStore {
    fn new(storage: StorageMode, root: &Path, label: &str) -> Self {
        match storage {
            StorageMode::Vector => Self::Vector(DurableKeyValueStore::new_vec_based()),
            StorageMode::File => {
                let directory = sample_directory(root, label);
                let store = DurableKeyValueStore::try_init_new(directory.path())
                    .expect("initialize pre-feature key/value benchmark store")
                    .into_store();
                Self::File {
                    store,
                    _directory: directory,
                }
            }
        }
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.put(key, value),
            Self::File { store, .. } => store.put(key, value),
        }
    }

    fn remove(&self, key: &[u8]) {
        match self {
            Self::Vector(store) => store.remove(key),
            Self::File { store, .. } => store.remove(key),
        }
    }

    fn compute(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.compute(key, |_| value),
            Self::File { store, .. } => store.compute(key, |_| value),
        }
    }
}

impl BenchStore for KeyValueStore {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize) {
        if workload == Workload::Remove {
            for operation in base..base + count {
                self.put(operation_key(worker, operation), fixed_bytes(operation));
            }
        }
    }

    fn operate(&self, workload: Workload, worker: usize, operation: usize) {
        match workload {
            Workload::Write => self.put(worker_key(worker), fixed_bytes(operation)),
            Workload::Remove => self.remove(&operation_key(worker, operation)),
            Workload::Callback => self.compute(worker_key(worker), fixed_bytes(operation)),
        }
    }
}

enum KeySetStore {
    Vector(DurableKeySetStore<Vec<u8>>),
    File {
        store: DurableKeySetStore<File>,
        _directory: TempDir,
    },
}

impl KeySetStore {
    fn new(storage: StorageMode, root: &Path, label: &str) -> Self {
        match storage {
            StorageMode::Vector => Self::Vector(DurableKeySetStore::new_vec_based()),
            StorageMode::File => {
                let directory = sample_directory(root, label);
                let store = DurableKeySetStore::try_init_new(directory.path())
                    .expect("initialize pre-feature key/set benchmark store")
                    .into_store();
                Self::File {
                    store,
                    _directory: directory,
                }
            }
        }
    }

    fn append(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::Vector(store) => store.append(key, member),
            Self::File { store, .. } => store.append(key, member),
        }
    }

    fn remove(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::Vector(store) => store.remove_from_set(key, member),
            Self::File { store, .. } => store.remove_from_set(key, member),
        }
    }

    fn compute(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::Vector(store) => store.compute(key, |set| {
                if !set.remove(&member) {
                    set.insert(member);
                }
            }),
            Self::File { store, .. } => store.compute(key, |set| {
                if !set.remove(&member) {
                    set.insert(member);
                }
            }),
        }
    }
}

impl BenchStore for KeySetStore {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize) {
        if workload == Workload::Remove {
            for operation in base..base + count {
                self.append(worker_key(worker), fixed_bytes(operation));
            }
        }
    }

    fn operate(&self, workload: Workload, worker: usize, operation: usize) {
        match workload {
            Workload::Write => self.append(worker_key(worker), fixed_bytes(operation)),
            Workload::Remove => self.remove(worker_key(worker), fixed_bytes(operation)),
            Workload::Callback => self.compute(worker_key(worker), fixed_bytes(operation % 2)),
        }
    }
}

enum KeyMapStore {
    Vector(DurableKeyMapStore<Vec<u8>>),
    File {
        store: DurableKeyMapStore<File>,
        _directory: TempDir,
    },
}

impl KeyMapStore {
    fn new(storage: StorageMode, root: &Path, label: &str) -> Self {
        match storage {
            StorageMode::Vector => Self::Vector(DurableKeyMapStore::new_vec_based()),
            StorageMode::File => {
                let directory = sample_directory(root, label);
                let store = DurableKeyMapStore::try_init_new(directory.path())
                    .expect("initialize pre-feature key/map benchmark store")
                    .into_store();
                Self::File {
                    store,
                    _directory: directory,
                }
            }
        }
    }

    fn put(&self, key: Vec<u8>, search_key: SearchKey, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.put(key, search_key, value),
            Self::File { store, .. } => store.put(key, search_key, value),
        }
    }

    fn remove(&self, key: Vec<u8>, search_key: SearchKey) {
        match self {
            Self::Vector(store) => {
                black_box(store.remove_from_sorted_map(key, search_key));
            }
            Self::File { store, .. } => {
                black_box(store.remove_from_sorted_map(key, search_key));
            }
        }
    }

    fn compute(&self, key: Vec<u8>, search_key: SearchKey, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.compute(key, |map| {
                map.insert(search_key, value);
            }),
            Self::File { store, .. } => store.compute(key, |map| {
                map.insert(search_key, value);
            }),
        }
    }
}

impl BenchStore for KeyMapStore {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize) {
        if workload == Workload::Remove {
            for operation in base..base + count {
                self.put(
                    worker_key(worker),
                    SearchKey::from(fixed_bytes(operation)),
                    fixed_bytes(operation),
                );
            }
        }
    }

    fn operate(&self, workload: Workload, worker: usize, operation: usize) {
        match workload {
            Workload::Write => self.put(
                worker_key(worker),
                SearchKey::from(fixed_bytes(0)),
                fixed_bytes(operation),
            ),
            Workload::Remove => {
                self.remove(worker_key(worker), SearchKey::from(fixed_bytes(operation)))
            }
            Workload::Callback => self.compute(
                worker_key(worker),
                SearchKey::from(fixed_bytes(1)),
                fixed_bytes(operation),
            ),
        }
    }
}

struct ReferenceStore {
    file: Mutex<File>,
    bytes: Vec<u8>,
    _directory: TempDir,
}

impl ReferenceStore {
    fn new(root: &Path, key: CellKey) -> Self {
        let directory = sample_directory(root, "reference");
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(directory.path().join("reference.wal"))
            .expect("create reference WAL");
        Self {
            file: Mutex::new(file),
            bytes: vec![0x5a; reference_record_len(key.family, key.workload)],
            _directory: directory,
        }
    }
}

impl BenchStore for ReferenceStore {
    fn prepare(&self, _workload: Workload, _worker: usize, _base: usize, _count: usize) {}

    fn operate(&self, _workload: Workload, _worker: usize, _operation: usize) {
        let mut file = self.file.lock().expect("reference mutex poisoned");
        file.write_all(&self.bytes).expect("reference write_all");
        file.flush().expect("reference flush");
        file.sync_data().expect("reference sync_data");
    }
}

fn capture_buffered_rows(root: &Path) -> Vec<CaptureRow> {
    let mut rows = Vec::with_capacity(36 * SAMPLE_COUNT);
    for family in StoreFamily::ALL {
        for storage in StorageMode::ALL {
            for workload in Workload::ALL {
                for workers in [1, 8] {
                    let key = CellKey {
                        family,
                        storage,
                        workload,
                        workers,
                    };
                    let label = format!(
                        "{}-{}-{}-{workers}",
                        family.as_str(),
                        storage.as_str(),
                        workload.as_str()
                    );
                    println!("capturing protocol-v4 buffered {label}");
                    let cell = match family {
                        StoreFamily::KeyValue => {
                            measure_cell(key, Implementation::PigmentDb, Policy::Buffered, || {
                                KeyValueStore::new(storage, root, &label)
                            })
                        }
                        StoreFamily::KeySet => {
                            measure_cell(key, Implementation::PigmentDb, Policy::Buffered, || {
                                KeySetStore::new(storage, root, &label)
                            })
                        }
                        StoreFamily::KeyMap => {
                            measure_cell(key, Implementation::PigmentDb, Policy::Buffered, || {
                                KeyMapStore::new(storage, root, &label)
                            })
                        }
                    };
                    rows.extend(cell);
                }
            }
        }
    }
    assert_matrix(&rows, 36, Policy::Buffered);
    rows
}

fn capture_reference_rows(root: &Path) -> Vec<CaptureRow> {
    let mut rows = Vec::with_capacity(18 * SAMPLE_COUNT);
    for family in StoreFamily::ALL {
        for workload in Workload::ALL {
            for workers in [1, 8] {
                let key = CellKey {
                    family,
                    storage: StorageMode::File,
                    workload,
                    workers,
                };
                println!(
                    "capturing protocol-v4 reference {}-{}-{workers}",
                    family.as_str(),
                    workload.as_str()
                );
                rows.extend(measure_cell(
                    key,
                    Implementation::Reference,
                    Policy::Reference,
                    || ReferenceStore::new(root, key),
                ));
            }
        }
    }
    assert_matrix(&rows, 18, Policy::Reference);
    rows
}

fn assert_matrix(rows: &[CaptureRow], expected_cells: usize, policy: Policy) {
    assert_eq!(rows.len(), expected_cells * SAMPLE_COUNT);
    assert!(rows.iter().all(|row| row.policy == policy));
    let keys: HashSet<_> = rows.iter().map(|row| row.key).collect();
    assert_eq!(keys.len(), expected_cells);
    for key in keys {
        assert_eq!(
            rows.iter().filter(|row| row.key == key).count(),
            SAMPLE_COUNT
        );
    }
}

fn reference_record_len(family: StoreFamily, workload: Workload) -> usize {
    let key = fixed_bytes(0);
    let value = fixed_bytes(1);
    let payload_len = match (family, workload) {
        (StoreFamily::KeyValue, Workload::Remove) => key.len(),
        (StoreFamily::KeyValue | StoreFamily::KeySet, _) => {
            bincode::serialized_size(&ReferenceKeyValue { key, value })
                .expect("reference key/value size") as usize
        }
        (StoreFamily::KeyMap, Workload::Remove) => {
            bincode::serialized_size(&SortedMapKey::new(key, SearchKey::from(fixed_bytes(2))))
                .expect("reference map remove size") as usize
        }
        (StoreFamily::KeyMap, _) => bincode::serialized_size(&SortedMapEntry::new(
            key,
            SearchKey::from(fixed_bytes(2)),
            value,
        ))
        .expect("reference map put size") as usize,
    };
    V1_RECORD_OVERHEAD + payload_len
}

#[derive(Serialize)]
struct ReferenceKeyValue {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

fn worker_key(worker: usize) -> Vec<u8> {
    fixed_bytes(worker)
}

fn operation_key(worker: usize, operation: usize) -> Vec<u8> {
    fixed_bytes(worker.wrapping_mul(1_000_003).wrapping_add(operation))
}

fn sample_directory(root: &Path, label: &str) -> TempDir {
    tempfile::Builder::new()
        .prefix(label)
        .tempdir_in(root)
        .expect("create benchmark sample directory")
}

#[test]
#[ignore = "release-only protocol-v4 pre-feature buffered capture"]
fn capture_protocol_v4_buffered_baseline() {
    let root = benchmark_root().expect("validate explicit benchmark root");
    let output = output_path().expect("resolve protocol-v4 baseline output path");
    let capture_id = capture_id().expect("resolve unique protocol-v4 baseline capture ID");
    assert!(
        !output.exists(),
        "protocol-v4 baseline capture is write-once: {}",
        output.display()
    );
    let provenance = collect_provenance(&capture_id, &root).expect("collect provenance");
    let rows = capture_buffered_rows(&root);
    write_capture_csv(&output, &provenance, &rows).expect("write protocol-v4 buffered baseline");
}

#[test]
#[ignore = "release-only protocol-v4 pre-feature append-plus-barrier capture"]
fn capture_protocol_v4_physical_reference() {
    let root = benchmark_root().expect("validate explicit benchmark root");
    let output = output_path().expect("resolve protocol-v4 reference output path");
    let capture_id = capture_id().expect("resolve unique protocol-v4 reference capture ID");
    assert!(
        !output.exists(),
        "protocol-v4 reference capture is write-once: {}",
        output.display()
    );
    let provenance = collect_provenance(&capture_id, &root).expect("collect provenance");
    let rows = capture_reference_rows(&root);
    write_capture_csv(&output, &provenance, &rows).expect("write protocol-v4 physical reference");
}

#[test]
fn fixed_reference_sizes_match_v1_payload_shapes() {
    assert_eq!(
        reference_record_len(StoreFamily::KeyValue, Workload::Write),
        126
    );
    assert_eq!(
        reference_record_len(StoreFamily::KeyValue, Workload::Remove),
        78
    );
    assert_eq!(
        reference_record_len(StoreFamily::KeySet, Workload::Callback),
        126
    );
    assert_eq!(
        reference_record_len(StoreFamily::KeyMap, Workload::Write),
        178
    );
    assert_eq!(
        reference_record_len(StoreFamily::KeyMap, Workload::Remove),
        138
    );
}
