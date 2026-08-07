use crate::alternating_protocol::{run_alternating_pairs, DiagnosticVariant};
use crate::protocol_v5::{
    complete_comparison_matrix, Comparator, ComparisonCell, ComparisonPolicy, StorageMode,
    StoreFamily, WorkerSchedule, Workload,
};
use pigment_db_baseline as baseline;
use pigment_db_candidate as candidate;
use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const OPERATIONS_PER_ROUND: usize = 64;
const MIN_OPERATIONS: usize = 1_024;
const MIN_SAMPLE_DURATION: Duration = Duration::from_millis(100);
const WARMUP_PAIR_COUNT: usize = 5;
const SAMPLE_PAIR_COUNT: usize = 11;
const PAYLOAD_BYTES: usize = 32;

trait BenchStore: Sync {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize);
    fn operate(&self, workload: Workload, worker: usize, operation: usize);
}

#[allow(clippy::enum_variant_names)]
enum BaselineStore {
    KeyValueVector(baseline::key_value_store::DurableKeyValueStore<Vec<u8>>),
    KeyValueFile {
        store: baseline::key_value_store::DurableKeyValueStore<File>,
        _directory: TempDir,
    },
    KeySetVector(baseline::key_set_store::DurableKeySetStore<Vec<u8>>),
    KeySetFile {
        store: baseline::key_set_store::DurableKeySetStore<File>,
        _directory: TempDir,
    },
    KeyMapVector(baseline::key_map_store::DurableKeyMapStore<Vec<u8>>),
    KeyMapFile {
        store: baseline::key_map_store::DurableKeyMapStore<File>,
        _directory: TempDir,
    },
}

impl BaselineStore {
    fn new(cell: ComparisonCell, root: &Path, label: &str) -> Self {
        assert_eq!(cell.policy, ComparisonPolicy::Buffered);
        match (cell.family, cell.storage) {
            (StoreFamily::KeyValue, StorageMode::Vector) => Self::KeyValueVector(
                baseline::key_value_store::DurableKeyValueStore::new_vec_based(),
            ),
            (StoreFamily::KeyValue, StorageMode::File) => {
                let directory = sample_directory(root, label);
                let store =
                    baseline::key_value_store::DurableKeyValueStore::try_init_new(directory.path())
                        .expect("initialize pre-feature key/value store")
                        .into_store();
                Self::KeyValueFile {
                    store,
                    _directory: directory,
                }
            }
            (StoreFamily::KeySet, StorageMode::Vector) => {
                Self::KeySetVector(baseline::key_set_store::DurableKeySetStore::new_vec_based())
            }
            (StoreFamily::KeySet, StorageMode::File) => {
                let directory = sample_directory(root, label);
                let store =
                    baseline::key_set_store::DurableKeySetStore::try_init_new(directory.path())
                        .expect("initialize pre-feature key/set store")
                        .into_store();
                Self::KeySetFile {
                    store,
                    _directory: directory,
                }
            }
            (StoreFamily::KeyMap, StorageMode::Vector) => {
                Self::KeyMapVector(baseline::key_map_store::DurableKeyMapStore::new_vec_based())
            }
            (StoreFamily::KeyMap, StorageMode::File) => {
                let directory = sample_directory(root, label);
                let store =
                    baseline::key_map_store::DurableKeyMapStore::try_init_new(directory.path())
                        .expect("initialize pre-feature key/map store")
                        .into_store();
                Self::KeyMapFile {
                    store,
                    _directory: directory,
                }
            }
        }
    }

    fn put_key_value(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::KeyValueVector(store) => store.put(key, value),
            Self::KeyValueFile { store, .. } => store.put(key, value),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn remove_key_value(&self, key: &[u8]) {
        match self {
            Self::KeyValueVector(store) => store.remove(key),
            Self::KeyValueFile { store, .. } => store.remove(key),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn compute_key_value(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::KeyValueVector(store) => store.compute(key, |_| value),
            Self::KeyValueFile { store, .. } => store.compute(key, |_| value),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn append_set(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::KeySetVector(store) => store.append(key, member),
            Self::KeySetFile { store, .. } => store.append(key, member),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn remove_set(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::KeySetVector(store) => store.remove_from_set(key, member),
            Self::KeySetFile { store, .. } => store.remove_from_set(key, member),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn compute_set(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::KeySetVector(store) => store.compute(key, |set| {
                if !set.remove(&member) {
                    set.insert(member);
                }
            }),
            Self::KeySetFile { store, .. } => store.compute(key, |set| {
                if !set.remove(&member) {
                    set.insert(member);
                }
            }),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn put_map(&self, key: Vec<u8>, search_tag: usize, value: Vec<u8>) {
        let search_key = baseline::model::SearchKey::from(fixed_bytes(search_tag));
        match self {
            Self::KeyMapVector(store) => store.put(key, search_key, value),
            Self::KeyMapFile { store, .. } => store.put(key, search_key, value),
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn remove_map(&self, key: Vec<u8>, search_tag: usize) {
        let search_key = baseline::model::SearchKey::from(fixed_bytes(search_tag));
        match self {
            Self::KeyMapVector(store) => {
                black_box(store.remove_from_sorted_map(key, search_key));
            }
            Self::KeyMapFile { store, .. } => {
                black_box(store.remove_from_sorted_map(key, search_key));
            }
            _ => panic!("pre-feature store family mismatch"),
        }
    }

    fn compute_map(&self, key: Vec<u8>, search_tag: usize, value: Vec<u8>) {
        let search_key = baseline::model::SearchKey::from(fixed_bytes(search_tag));
        match self {
            Self::KeyMapVector(store) => store.compute(key, |map| {
                map.insert(search_key, value);
            }),
            Self::KeyMapFile { store, .. } => store.compute(key, |map| {
                map.insert(search_key, value);
            }),
            _ => panic!("pre-feature store family mismatch"),
        }
    }
}

impl BenchStore for BaselineStore {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize) {
        if workload != Workload::Remove {
            return;
        }
        for operation in base..base + count {
            match self {
                Self::KeyValueVector(_) | Self::KeyValueFile { .. } => {
                    self.put_key_value(operation_key(worker, operation), fixed_bytes(operation));
                }
                Self::KeySetVector(_) | Self::KeySetFile { .. } => {
                    self.append_set(worker_key(worker), fixed_bytes(operation));
                }
                Self::KeyMapVector(_) | Self::KeyMapFile { .. } => {
                    self.put_map(worker_key(worker), operation, fixed_bytes(operation))
                }
            }
        }
    }

    fn operate(&self, workload: Workload, worker: usize, operation: usize) {
        match self {
            Self::KeyValueVector(_) | Self::KeyValueFile { .. } => match workload {
                Workload::Write => self.put_key_value(worker_key(worker), fixed_bytes(operation)),
                Workload::Remove => self.remove_key_value(&operation_key(worker, operation)),
                Workload::Callback => {
                    self.compute_key_value(worker_key(worker), fixed_bytes(operation));
                }
            },
            Self::KeySetVector(_) | Self::KeySetFile { .. } => match workload {
                Workload::Write => self.append_set(worker_key(worker), fixed_bytes(operation)),
                Workload::Remove => self.remove_set(worker_key(worker), fixed_bytes(operation)),
                Workload::Callback => {
                    self.compute_set(worker_key(worker), fixed_bytes(operation % 2));
                }
            },
            Self::KeyMapVector(_) | Self::KeyMapFile { .. } => match workload {
                Workload::Write => {
                    self.put_map(worker_key(worker), 0, fixed_bytes(operation));
                }
                Workload::Remove => self.remove_map(worker_key(worker), operation),
                Workload::Callback => {
                    self.compute_map(worker_key(worker), 1, fixed_bytes(operation));
                }
            },
        }
    }
}

#[allow(clippy::enum_variant_names)]
enum CandidateStore {
    KeyValueVector(candidate::key_value_store::DurableKeyValueStore<Vec<u8>>),
    KeyValueFile {
        store: candidate::key_value_store::DurableKeyValueStore<File>,
        _directory: TempDir,
    },
    KeySetVector(candidate::key_set_store::DurableKeySetStore<Vec<u8>>),
    KeySetFile {
        store: candidate::key_set_store::DurableKeySetStore<File>,
        _directory: TempDir,
    },
    KeyMapVector(candidate::key_map_store::DurableKeyMapStore<Vec<u8>>),
    KeyMapFile {
        store: candidate::key_map_store::DurableKeyMapStore<File>,
        _directory: TempDir,
    },
}

impl CandidateStore {
    fn new(cell: ComparisonCell, root: &Path, label: &str) -> Self {
        match (cell.family, cell.storage) {
            (StoreFamily::KeyValue, StorageMode::Vector) => {
                assert_eq!(cell.policy, ComparisonPolicy::Buffered);
                Self::KeyValueVector(
                    candidate::key_value_store::DurableKeyValueStore::new_vec_based(),
                )
            }
            (StoreFamily::KeyValue, StorageMode::File) => {
                let directory = sample_directory(root, label);
                let store =
                    candidate::key_value_store::DurableKeyValueStore::try_init_new_with_options(
                        directory.path(),
                        candidate_options(cell.policy),
                    )
                    .expect("initialize candidate key/value store")
                    .into_store();
                Self::KeyValueFile {
                    store,
                    _directory: directory,
                }
            }
            (StoreFamily::KeySet, StorageMode::Vector) => {
                assert_eq!(cell.policy, ComparisonPolicy::Buffered);
                Self::KeySetVector(candidate::key_set_store::DurableKeySetStore::new_vec_based())
            }
            (StoreFamily::KeySet, StorageMode::File) => {
                let directory = sample_directory(root, label);
                let store =
                    candidate::key_set_store::DurableKeySetStore::try_init_new_with_options(
                        directory.path(),
                        candidate_options(cell.policy),
                    )
                    .expect("initialize candidate key/set store")
                    .into_store();
                Self::KeySetFile {
                    store,
                    _directory: directory,
                }
            }
            (StoreFamily::KeyMap, StorageMode::Vector) => {
                assert_eq!(cell.policy, ComparisonPolicy::Buffered);
                Self::KeyMapVector(candidate::key_map_store::DurableKeyMapStore::new_vec_based())
            }
            (StoreFamily::KeyMap, StorageMode::File) => {
                let directory = sample_directory(root, label);
                let store =
                    candidate::key_map_store::DurableKeyMapStore::try_init_new_with_options(
                        directory.path(),
                        candidate_options(cell.policy),
                    )
                    .expect("initialize candidate key/map store")
                    .into_store();
                Self::KeyMapFile {
                    store,
                    _directory: directory,
                }
            }
        }
    }

    fn put_key_value(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::KeyValueVector(store) => store.put(key, value),
            Self::KeyValueFile { store, .. } => store.put(key, value),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn remove_key_value(&self, key: &[u8]) {
        match self {
            Self::KeyValueVector(store) => store.remove(key),
            Self::KeyValueFile { store, .. } => store.remove(key),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn compute_key_value(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::KeyValueVector(store) => store.compute(key, |_| value),
            Self::KeyValueFile { store, .. } => store.compute(key, |_| value),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn append_set(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::KeySetVector(store) => store.append(key, member),
            Self::KeySetFile { store, .. } => store.append(key, member),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn remove_set(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::KeySetVector(store) => store.remove_from_set(key, member),
            Self::KeySetFile { store, .. } => store.remove_from_set(key, member),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn compute_set(&self, key: Vec<u8>, member: Vec<u8>) {
        match self {
            Self::KeySetVector(store) => store.compute(key, |set| {
                if !set.remove(&member) {
                    set.insert(member);
                }
            }),
            Self::KeySetFile { store, .. } => store.compute(key, |set| {
                if !set.remove(&member) {
                    set.insert(member);
                }
            }),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn put_map(&self, key: Vec<u8>, search_tag: usize, value: Vec<u8>) {
        let search_key = candidate::model::SearchKey::from(fixed_bytes(search_tag));
        match self {
            Self::KeyMapVector(store) => store.put(key, search_key, value),
            Self::KeyMapFile { store, .. } => store.put(key, search_key, value),
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn remove_map(&self, key: Vec<u8>, search_tag: usize) {
        let search_key = candidate::model::SearchKey::from(fixed_bytes(search_tag));
        match self {
            Self::KeyMapVector(store) => {
                black_box(store.remove_from_sorted_map(key, search_key));
            }
            Self::KeyMapFile { store, .. } => {
                black_box(store.remove_from_sorted_map(key, search_key));
            }
            _ => panic!("candidate store family mismatch"),
        }
    }

    fn compute_map(&self, key: Vec<u8>, search_tag: usize, value: Vec<u8>) {
        let search_key = candidate::model::SearchKey::from(fixed_bytes(search_tag));
        match self {
            Self::KeyMapVector(store) => store.compute(key, |map| {
                map.insert(search_key, value);
            }),
            Self::KeyMapFile { store, .. } => store.compute(key, |map| {
                map.insert(search_key, value);
            }),
            _ => panic!("candidate store family mismatch"),
        }
    }
}

impl BenchStore for CandidateStore {
    fn prepare(&self, workload: Workload, worker: usize, base: usize, count: usize) {
        if workload != Workload::Remove {
            return;
        }
        for operation in base..base + count {
            match self {
                Self::KeyValueVector(_) | Self::KeyValueFile { .. } => {
                    self.put_key_value(operation_key(worker, operation), fixed_bytes(operation));
                }
                Self::KeySetVector(_) | Self::KeySetFile { .. } => {
                    self.append_set(worker_key(worker), fixed_bytes(operation));
                }
                Self::KeyMapVector(_) | Self::KeyMapFile { .. } => {
                    self.put_map(worker_key(worker), operation, fixed_bytes(operation))
                }
            }
        }
    }

    fn operate(&self, workload: Workload, worker: usize, operation: usize) {
        match self {
            Self::KeyValueVector(_) | Self::KeyValueFile { .. } => match workload {
                Workload::Write => self.put_key_value(worker_key(worker), fixed_bytes(operation)),
                Workload::Remove => self.remove_key_value(&operation_key(worker, operation)),
                Workload::Callback => {
                    self.compute_key_value(worker_key(worker), fixed_bytes(operation));
                }
            },
            Self::KeySetVector(_) | Self::KeySetFile { .. } => match workload {
                Workload::Write => self.append_set(worker_key(worker), fixed_bytes(operation)),
                Workload::Remove => self.remove_set(worker_key(worker), fixed_bytes(operation)),
                Workload::Callback => {
                    self.compute_set(worker_key(worker), fixed_bytes(operation % 2));
                }
            },
            Self::KeyMapVector(_) | Self::KeyMapFile { .. } => match workload {
                Workload::Write => {
                    self.put_map(worker_key(worker), 0, fixed_bytes(operation));
                }
                Workload::Remove => self.remove_map(worker_key(worker), operation),
                Workload::Callback => {
                    self.compute_map(worker_key(worker), 1, fixed_bytes(operation));
                }
            },
        }
    }
}

struct ReferenceStore {
    file: Mutex<File>,
    bytes: Vec<u8>,
    _directory: TempDir,
}

impl ReferenceStore {
    fn new(cell: ComparisonCell, root: &Path, label: &str) -> Self {
        assert_eq!(cell.policy, ComparisonPolicy::Physical);
        let directory = sample_directory(root, label);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(directory.path().join("reference.wal"))
            .expect("create append-plus-barrier reference");
        Self {
            file: Mutex::new(file),
            bytes: vec![0x5a; reference_record_len(cell.family, cell.workload)],
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

#[derive(Clone)]
struct SourceProvenance {
    commit: String,
    dirty_hash: String,
}

#[derive(Clone)]
struct RunProvenance {
    capture_id: String,
    baseline: SourceProvenance,
    candidate: SourceProvenance,
    toolchain: String,
    target: String,
    os: String,
    cpu: String,
    filesystem: String,
    benchmark_root: PathBuf,
    affinity: String,
}

struct SampleRow {
    cell: ComparisonCell,
    pair_index: usize,
    position: usize,
    variant: DiagnosticVariant,
    operations: usize,
    elapsed: Duration,
    p95_latency_ns: u128,
}

impl SampleRow {
    fn ops_per_second(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64()
    }
}

struct SampleMeasurement {
    operations: usize,
    elapsed: Duration,
    p95_latency_ns: u128,
}

fn measure_variant(
    cell: ComparisonCell,
    variant: DiagnosticVariant,
    root: &Path,
    label: &str,
) -> SampleMeasurement {
    match (variant, cell.comparator) {
        (DiagnosticVariant::Baseline, Comparator::PreFeature) => {
            run_sample(&BaselineStore::new(cell, root, label), cell)
        }
        (DiagnosticVariant::Baseline, Comparator::AppendPlusBarrier) => {
            run_sample(&ReferenceStore::new(cell, root, label), cell)
        }
        (DiagnosticVariant::Candidate, _) => {
            run_sample(&CandidateStore::new(cell, root, label), cell)
        }
    }
}

fn run_sample(store: &impl BenchStore, cell: ComparisonCell) -> SampleMeasurement {
    let mut elapsed = Duration::ZERO;
    let mut latencies = Vec::new();
    let mut base = 0;
    while elapsed < MIN_SAMPLE_DURATION || latencies.len() < MIN_OPERATIONS {
        for worker in 0..cell.workers {
            store.prepare(cell.workload, worker, base, OPERATIONS_PER_ROUND);
        }
        let (round_elapsed, mut round_latencies) = run_round(store, cell, base);
        elapsed += round_elapsed;
        latencies.append(&mut round_latencies);
        base += OPERATIONS_PER_ROUND;
    }
    let operations = latencies.len();
    assert!(operations >= MIN_OPERATIONS);
    assert!(elapsed >= MIN_SAMPLE_DURATION);
    SampleMeasurement {
        operations,
        elapsed,
        p95_latency_ns: p95_nanos(&mut latencies),
    }
}

fn run_round(
    store: &impl BenchStore,
    cell: ComparisonCell,
    base: usize,
) -> (Duration, Vec<Duration>) {
    if cell.workers == 1 {
        let mut latencies = Vec::with_capacity(OPERATIONS_PER_ROUND);
        let wall_started = Instant::now();
        for operation in base..base + OPERATIONS_PER_ROUND {
            let call_started = Instant::now();
            store.operate(cell.workload, 0, operation);
            latencies.push(call_started.elapsed());
        }
        return (wall_started.elapsed(), latencies);
    }

    std::thread::scope(|scope| {
        let start_barrier = Arc::new(Barrier::new(cell.workers + 1));
        let operation_barrier = match cell.schedule {
            WorkerSchedule::StartOnly => None,
            WorkerSchedule::PerOperation => Some(Arc::new(Barrier::new(cell.workers))),
        };
        let mut handles = Vec::with_capacity(cell.workers);
        for worker in 0..cell.workers {
            let start_barrier = Arc::clone(&start_barrier);
            let operation_barrier = operation_barrier.clone();
            handles.push(scope.spawn(move || {
                let mut latencies = Vec::with_capacity(OPERATIONS_PER_ROUND);
                start_barrier.wait();
                for operation in base..base + OPERATIONS_PER_ROUND {
                    if let Some(operation_barrier) = &operation_barrier {
                        operation_barrier.wait();
                    }
                    let call_started = Instant::now();
                    store.operate(cell.workload, worker, operation);
                    latencies.push(call_started.elapsed());
                }
                latencies
            }));
        }
        start_barrier.wait();
        let wall_started = Instant::now();
        let mut latencies = Vec::with_capacity(cell.workers * OPERATIONS_PER_ROUND);
        for handle in handles {
            latencies.extend(handle.join().expect("protocol-v5 worker panicked"));
        }
        (wall_started.elapsed(), latencies)
    })
}

fn run_capture(root: &Path) -> Vec<SampleRow> {
    let matrix = complete_comparison_matrix();
    assert_eq!(matrix.len(), 54);
    let mut rows = Vec::with_capacity(matrix.len() * SAMPLE_PAIR_COUNT * 2);
    for cell in matrix {
        let cell_label = cell_label(cell);
        println!("warming protocol-v5 pair {cell_label}");
        run_alternating_pairs(WARMUP_PAIR_COUNT, |pair_index, position, variant| {
            let label = format!("warm-{cell_label}-{pair_index}-{position}");
            black_box(measure_variant(cell, variant, root, &label));
        });

        println!("capturing protocol-v5 pair {cell_label}");
        run_alternating_pairs(SAMPLE_PAIR_COUNT, |pair_index, position, variant| {
            let label = format!("sample-{cell_label}-{pair_index}-{position}");
            let measurement = measure_variant(cell, variant, root, &label);
            rows.push(SampleRow {
                cell,
                pair_index,
                position,
                variant,
                operations: measurement.operations,
                elapsed: measurement.elapsed,
                p95_latency_ns: measurement.p95_latency_ns,
            });
        });
    }
    assert_eq!(rows.len(), 54 * SAMPLE_PAIR_COUNT * 2);
    rows
}

fn validate_linked_implementations(root: &Path) {
    for cell in complete_comparison_matrix() {
        for variant in [DiagnosticVariant::Baseline, DiagnosticVariant::Candidate] {
            let label = format!("validate-{}-{}", cell_label(cell), variant_name(variant));
            match (variant, cell.comparator) {
                (DiagnosticVariant::Baseline, Comparator::PreFeature) => {
                    let store = BaselineStore::new(cell, root, &label);
                    store.prepare(cell.workload, 0, 0, 1);
                    store.operate(cell.workload, 0, 0);
                }
                (DiagnosticVariant::Baseline, Comparator::AppendPlusBarrier) => {
                    let store = ReferenceStore::new(cell, root, &label);
                    store.operate(cell.workload, 0, 0);
                }
                (DiagnosticVariant::Candidate, _) => {
                    let store = CandidateStore::new(cell, root, &label);
                    store.prepare(cell.workload, 0, 0, 1);
                    store.operate(cell.workload, 0, 0);
                }
            }
        }
    }
}

fn candidate_options(policy: ComparisonPolicy) -> candidate::DurableStoreOptions {
    match policy {
        ComparisonPolicy::Buffered => candidate::DurableStoreOptions::default(),
        ComparisonPolicy::Physical => candidate::DurableStoreOptions::default()
            .with_durability_policy(candidate::DurabilityPolicy::Physical),
    }
}

fn reference_record_len(family: StoreFamily, workload: Workload) -> usize {
    match (family, workload) {
        (StoreFamily::KeyValue, Workload::Remove) => 78,
        (StoreFamily::KeyValue | StoreFamily::KeySet, _) => 126,
        (StoreFamily::KeyMap, Workload::Remove) => 138,
        (StoreFamily::KeyMap, _) => 178,
    }
}

fn fixed_bytes(tag: usize) -> Vec<u8> {
    let mut bytes = vec![b'x'; PAYLOAD_BYTES];
    bytes[..std::mem::size_of::<usize>()].copy_from_slice(&tag.to_ne_bytes());
    bytes
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
        .expect("create protocol-v5 sample directory")
}

fn p95_nanos(latencies: &mut [Duration]) -> u128 {
    assert!(!latencies.is_empty());
    latencies.sort_unstable();
    let index = ((latencies.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(latencies.len() - 1);
    latencies[index].as_nanos()
}

fn family_name(family: StoreFamily) -> &'static str {
    match family {
        StoreFamily::KeyValue => "key_value",
        StoreFamily::KeySet => "key_set",
        StoreFamily::KeyMap => "key_map",
    }
}

fn storage_name(storage: StorageMode) -> &'static str {
    match storage {
        StorageMode::Vector => "vector",
        StorageMode::File => "file",
    }
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::Write => "ordinary_write",
        Workload::Remove => "successful_remove",
        Workload::Callback => "minimal_callback",
    }
}

fn policy_name(policy: ComparisonPolicy) -> &'static str {
    match policy {
        ComparisonPolicy::Buffered => "buffered",
        ComparisonPolicy::Physical => "physical",
    }
}

fn comparator_name(comparator: Comparator) -> &'static str {
    match comparator {
        Comparator::PreFeature => "pre_feature_pigment_db",
        Comparator::AppendPlusBarrier => "append_plus_barrier",
    }
}

fn variant_name(variant: DiagnosticVariant) -> &'static str {
    match variant {
        DiagnosticVariant::Baseline => "comparator",
        DiagnosticVariant::Candidate => "candidate",
    }
}

fn implementation_name(row: &SampleRow) -> &'static str {
    match (row.variant, row.cell.comparator) {
        (DiagnosticVariant::Baseline, Comparator::PreFeature) => "pre_feature_pigment_db",
        (DiagnosticVariant::Baseline, Comparator::AppendPlusBarrier) => "mutex_file_reference",
        (DiagnosticVariant::Candidate, _) => "candidate_pigment_db",
    }
}

fn row_policy_name(row: &SampleRow) -> &'static str {
    match row.variant {
        DiagnosticVariant::Candidate => policy_name(row.cell.policy),
        DiagnosticVariant::Baseline => match row.cell.comparator {
            Comparator::PreFeature => "buffered",
            Comparator::AppendPlusBarrier => "append_plus_barrier",
        },
    }
}

fn cell_label(cell: ComparisonCell) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        policy_name(cell.policy),
        family_name(cell.family),
        storage_name(cell.storage),
        workload_name(cell.workload),
        cell.workers
    )
}

fn affinity_list() -> String {
    std::fs::read_to_string("/proc/self/status")
        .expect("read process status")
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("find process affinity")
        .trim()
        .to_owned()
}

fn command_output(program: &str, args: &[&str]) -> String {
    let output = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("run {program}: {error}"));
    assert!(output.status.success(), "{program} failed");
    String::from_utf8(output.stdout)
        .expect("command output must be UTF-8")
        .trim()
        .to_owned()
}

fn source_provenance(path: &Path) -> SourceProvenance {
    let status = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .current_dir(path)
        .output()
        .expect("collect source status");
    assert!(status.status.success());
    let mut hasher = DefaultHasher::new();
    status.stdout.hash(&mut hasher);
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(path)
        .output()
        .expect("collect source commit");
    assert!(commit.status.success());
    SourceProvenance {
        commit: String::from_utf8(commit.stdout)
            .expect("commit must be UTF-8")
            .trim()
            .to_owned(),
        dirty_hash: format!("default-hasher:{:016x}", hasher.finish()),
    }
}

fn benchmark_root() -> PathBuf {
    let root = std::env::var_os("PIGMENT_DB_V5_BENCH_ROOT")
        .map(PathBuf::from)
        .expect("PIGMENT_DB_V5_BENCH_ROOT must name a real-filesystem directory");
    assert!(root.is_absolute() && root.is_dir());
    let root = root.canonicalize().expect("canonicalize benchmark root");
    let filesystem_type = command_output(
        "findmnt",
        &[
            "-T",
            root.to_str().unwrap_or_default(),
            "-n",
            "-o",
            "FSTYPE",
        ],
    );
    assert!(
        !matches!(filesystem_type.as_str(), "tmpfs" | "ramfs" | "devtmpfs"),
        "benchmark root cannot use an in-memory filesystem"
    );
    root
}

fn collect_provenance(root: &Path) -> RunProvenance {
    let capture_id = std::env::var("PIGMENT_DB_V5_CAPTURE_ID")
        .expect("PIGMENT_DB_V5_CAPTURE_ID must name the write-once capture");
    let baseline_root = std::env::var_os("PIGMENT_DB_V5_BASELINE_ROOT")
        .map(PathBuf::from)
        .expect("PIGMENT_DB_V5_BASELINE_ROOT must name the pre-feature source");
    let candidate_root = std::env::var_os("PIGMENT_DB_V5_CANDIDATE_ROOT")
        .map(PathBuf::from)
        .expect("PIGMENT_DB_V5_CANDIDATE_ROOT must name the candidate source");
    let verbose = command_output("rustc", &["--version", "--verbose"]);
    let target = verbose
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or("unknown")
        .to_owned();
    let toolchain = verbose.lines().next().unwrap_or("unknown").to_owned();
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .find_map(|line| line.strip_prefix("model name\t: "))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_owned());
    RunProvenance {
        capture_id,
        baseline: source_provenance(&baseline_root),
        candidate: source_provenance(&candidate_root),
        toolchain,
        target,
        os: command_output("uname", &["-srm"]),
        cpu,
        filesystem: command_output(
            "findmnt",
            &[
                "-T",
                root.to_str().unwrap_or_default(),
                "-n",
                "-o",
                "SOURCE,FSTYPE,OPTIONS",
            ],
        ),
        benchmark_root: root.to_owned(),
        affinity: affinity_list(),
    }
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn write_capture(path: &Path, provenance: &RunProvenance, rows: &[SampleRow]) {
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap_or_else(|error| panic!("create write-once capture {}: {error}", path.display()));
    writeln!(
        output,
        "capture_id,baseline_commit,baseline_dirty_hash,candidate_commit,candidate_dirty_hash,toolchain,target,os,cpu,filesystem,benchmark_root,affinity,variant,implementation,policy,comparator,pair_index,position,store_family,storage_mode,workload,workers,payload_bytes,warmup_pair_count,sample_index,operations,elapsed_ns,ops_per_second,p95_latency_ns,failed_operations"
    )
    .expect("write protocol-v5 header");
    for row in rows {
        let fields = [
            provenance.capture_id.clone(),
            provenance.baseline.commit.clone(),
            provenance.baseline.dirty_hash.clone(),
            provenance.candidate.commit.clone(),
            provenance.candidate.dirty_hash.clone(),
            provenance.toolchain.clone(),
            provenance.target.clone(),
            provenance.os.clone(),
            provenance.cpu.clone(),
            provenance.filesystem.clone(),
            provenance.benchmark_root.display().to_string(),
            provenance.affinity.clone(),
            variant_name(row.variant).to_owned(),
            implementation_name(row).to_owned(),
            row_policy_name(row).to_owned(),
            comparator_name(row.cell.comparator).to_owned(),
            row.pair_index.to_string(),
            row.position.to_string(),
            family_name(row.cell.family).to_owned(),
            storage_name(row.cell.storage).to_owned(),
            workload_name(row.cell.workload).to_owned(),
            row.cell.workers.to_string(),
            PAYLOAD_BYTES.to_string(),
            WARMUP_PAIR_COUNT.to_string(),
            row.pair_index.to_string(),
            row.operations.to_string(),
            row.elapsed.as_nanos().to_string(),
            format!("{:.6}", row.ops_per_second()),
            row.p95_latency_ns.to_string(),
            "0".to_owned(),
        ];
        writeln!(
            output,
            "{}",
            fields.map(|field| csv_field(&field)).join(",")
        )
        .expect("write protocol-v5 row");
    }
    output.flush().expect("flush protocol-v5 capture");
}

fn main() {
    let root = benchmark_root();
    let provenance = collect_provenance(&root);
    assert_eq!(
        provenance.affinity, "12-19",
        "protocol v5 requires CPUs 12-19"
    );

    if std::env::var_os("PIGMENT_DB_V5_VALIDATE_ONLY").is_some() {
        validate_linked_implementations(&root);
        println!(
            "protocol-v5 linked validation passed: baseline_commit={} baseline_dirty_hash={} candidate_commit={} candidate_dirty_hash={} affinity={}",
            provenance.baseline.commit,
            provenance.baseline.dirty_hash,
            provenance.candidate.commit,
            provenance.candidate.dirty_hash,
            provenance.affinity
        );
        return;
    }

    let output = std::env::var_os("PIGMENT_DB_V5_OUTPUT")
        .map(PathBuf::from)
        .expect("PIGMENT_DB_V5_OUTPUT must name the write-once capture CSV");
    assert!(!output.exists(), "protocol-v5 output already exists");
    let rows = run_capture(&root);
    write_capture(&output, &provenance, &rows);
}
