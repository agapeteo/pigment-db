//! Ignored paired performance and retained-memory gates.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const WARMUPS: usize = 5;
const SAMPLES: usize = 11;
const VALUE_BYTES: usize = 32;
const MIN_SAMPLE_DURATION: Duration = Duration::from_millis(100);
const MIN_OPERATIONS_PER_SAMPLE: usize = 1_024;
const BENCHMARK_OUTPUT_ENV: &str = "PIGMENT_DB_BENCHMARK_OUTPUT";
const BENCHMARK_BASELINE_ENV: &str = "PIGMENT_DB_BENCHMARK_BASELINE";
const BENCHMARK_CANDIDATE_ENV: &str = "PIGMENT_DB_BENCHMARK_CANDIDATE";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum StoreKind {
    Value,
    Set,
    Map,
}

impl StoreKind {
    fn name(self) -> &'static str {
        match self {
            Self::Value => "key_value",
            Self::Set => "key_set",
            Self::Map => "key_map",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Mode {
    Vector,
    File,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::File => "file",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Profile {
    Write,
    Remove,
    Callback,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Self::Write => "ordinary_write",
            Self::Remove => "successful_remove",
            Self::Callback => "minimal_callback",
        }
    }

    fn operations_per_worker(self) -> usize {
        match self {
            Self::Write => 8,
            Self::Remove => 1,
            Self::Callback => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    store: StoreKind,
    mode: Mode,
    profile: Profile,
    workers: usize,
}

struct CellResult {
    key: CellKey,
    operations_per_sample: usize,
    median_throughput: f64,
    p95_latency: Duration,
}

#[derive(Clone, Copy)]
struct MemoryResult {
    cycles: usize,
    before: u64,
    after: u64,
    delta: u64,
}

trait BenchStore: Sync {
    fn prepare(&self, profile: Profile, workers: usize);
    fn operate(&self, profile: Profile, worker: usize, operation: usize);
}

enum KeyValueBenchStore {
    Vector(DurableKeyValueStore<Vec<u8>>),
    File {
        store: DurableKeyValueStore<File>,
        _directory: TempDir,
    },
}

impl KeyValueBenchStore {
    fn new(mode: Mode) -> Self {
        match mode {
            Mode::Vector => Self::Vector(DurableKeyValueStore::new_vec_based()),
            Mode::File => {
                let directory = tempfile::tempdir().expect("create key/value benchmark directory");
                let store = DurableKeyValueStore::try_init_new(directory.path())
                    .expect("initialize key/value benchmark store")
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

impl BenchStore for KeyValueBenchStore {
    fn prepare(&self, profile: Profile, workers: usize) {
        if profile == Profile::Remove {
            for worker in 0..workers {
                self.put(fixed_key(worker), fixed_value(0));
            }
        }
    }

    fn operate(&self, profile: Profile, worker: usize, operation: usize) {
        let key = fixed_key(worker);
        match profile {
            Profile::Write => self.put(key, fixed_value(operation)),
            Profile::Remove => self.remove(&key),
            Profile::Callback => self.compute(key, fixed_value(operation)),
        }
    }
}

enum KeySetBenchStore {
    Vector(DurableKeySetStore<Vec<u8>>),
    File {
        store: DurableKeySetStore<File>,
        _directory: TempDir,
    },
}

impl KeySetBenchStore {
    fn new(mode: Mode) -> Self {
        match mode {
            Mode::Vector => Self::Vector(DurableKeySetStore::new_vec_based()),
            Mode::File => {
                let directory = tempfile::tempdir().expect("create key/set benchmark directory");
                let store = DurableKeySetStore::try_init_new(directory.path())
                    .expect("initialize key/set benchmark store")
                    .into_store();
                Self::File {
                    store,
                    _directory: directory,
                }
            }
        }
    }

    fn append(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.append(key, value),
            Self::File { store, .. } => store.append(key, value),
        }
    }

    fn remove(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.remove_from_set(key, value),
            Self::File { store, .. } => store.remove_from_set(key, value),
        }
    }

    fn compute_toggle(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Vector(store) => store.compute(key, |set| {
                if !set.remove(&value) {
                    set.insert(value);
                }
            }),
            Self::File { store, .. } => store.compute(key, |set| {
                if !set.remove(&value) {
                    set.insert(value);
                }
            }),
        }
    }
}

impl BenchStore for KeySetBenchStore {
    fn prepare(&self, profile: Profile, workers: usize) {
        if profile == Profile::Remove {
            for worker in 0..workers {
                let key = fixed_key(worker);
                self.append(key.clone(), fixed_value(0));
                self.append(key, fixed_value(1));
            }
        }
    }

    fn operate(&self, profile: Profile, worker: usize, operation: usize) {
        let key = fixed_key(worker);
        match profile {
            Profile::Write => self.append(key, fixed_value(operation)),
            Profile::Remove => self.remove(key, fixed_value(1)),
            Profile::Callback => self.compute_toggle(key, fixed_value(2)),
        }
    }
}

enum KeyMapBenchStore {
    Vector(DurableKeyMapStore<Vec<u8>>),
    File {
        store: DurableKeyMapStore<File>,
        _directory: TempDir,
    },
}

impl KeyMapBenchStore {
    fn new(mode: Mode) -> Self {
        match mode {
            Mode::Vector => Self::Vector(DurableKeyMapStore::new_vec_based()),
            Mode::File => {
                let directory = tempfile::tempdir().expect("create key/map benchmark directory");
                let store = DurableKeyMapStore::try_init_new(directory.path())
                    .expect("initialize key/map benchmark store")
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

    fn compute_insert(&self, key: Vec<u8>, search_key: SearchKey, value: Vec<u8>) {
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

impl BenchStore for KeyMapBenchStore {
    fn prepare(&self, profile: Profile, workers: usize) {
        if profile == Profile::Remove {
            for worker in 0..workers {
                let key = fixed_key(worker);
                self.put(key.clone(), SearchKey::from(0), fixed_value(0));
                self.put(key, SearchKey::from(1), fixed_value(1));
            }
        }
    }

    fn operate(&self, profile: Profile, worker: usize, operation: usize) {
        let key = fixed_key(worker);
        match profile {
            Profile::Write => self.put(key, SearchKey::from(0), fixed_value(operation)),
            Profile::Remove => self.remove(key, SearchKey::from(1)),
            Profile::Callback => {
                self.compute_insert(key, SearchKey::from(2), fixed_value(operation % 2))
            }
        }
    }
}

fn fixed_key(worker: usize) -> Vec<u8> {
    let mut key = vec![b'k'; VALUE_BYTES];
    key[..std::mem::size_of::<usize>()].copy_from_slice(&worker.to_ne_bytes());
    key
}

fn fixed_value(tag: usize) -> Vec<u8> {
    let mut value = vec![b'v'; VALUE_BYTES];
    value[..std::mem::size_of::<usize>()].copy_from_slice(&tag.to_ne_bytes());
    value
}

fn run_once<S: BenchStore>(
    store: &S,
    profile: Profile,
    workers: usize,
    operation_base: usize,
) -> (Duration, Vec<Duration>) {
    let operations = profile.operations_per_worker();
    store.prepare(profile, workers);

    if workers == 1 {
        let mut latencies = Vec::with_capacity(operations);
        let wall_started = Instant::now();
        for operation in operation_base..operation_base + operations {
            let call_started = Instant::now();
            store.operate(profile, 0, operation);
            latencies.push(call_started.elapsed());
        }
        return (wall_started.elapsed(), latencies);
    }

    std::thread::scope(|scope| {
        let barrier = Arc::new(Barrier::new(workers + 1));
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                let mut latencies = Vec::with_capacity(operations);
                barrier.wait();
                for operation in operation_base..operation_base + operations {
                    let call_started = Instant::now();
                    store.operate(profile, worker, operation);
                    latencies.push(call_started.elapsed());
                }
                latencies
            }));
        }

        let wall_started = Instant::now();
        barrier.wait();
        let mut latencies = Vec::with_capacity(workers * operations);
        for handle in handles {
            latencies.extend(handle.join().expect("benchmark worker must not panic"));
        }
        (wall_started.elapsed(), latencies)
    })
}

fn run_sample<S: BenchStore>(store: &S, key: CellKey) -> (Duration, Vec<Duration>) {
    let mut elapsed = Duration::ZERO;
    let mut latencies = Vec::new();
    let mut operation_base = 0;
    while elapsed < MIN_SAMPLE_DURATION || latencies.len() < MIN_OPERATIONS_PER_SAMPLE {
        let (round_elapsed, mut round_latencies) =
            run_once(store, key.profile, key.workers, operation_base);
        operation_base += round_latencies.len() / key.workers;
        elapsed += round_elapsed;
        latencies.append(&mut round_latencies);
    }
    (elapsed, latencies)
}

fn measure<S: BenchStore>(key: CellKey, mut build: impl FnMut() -> S) -> CellResult {
    for _ in 0..WARMUPS {
        let store = build();
        black_box(run_sample(&store, key));
    }

    let mut throughputs = Vec::with_capacity(SAMPLES);
    let mut latencies = Vec::new();
    let mut operations_per_sample = usize::MAX;
    for _ in 0..SAMPLES {
        let store = build();
        let (elapsed, sample_latencies) = run_sample(&store, key);
        operations_per_sample = operations_per_sample.min(sample_latencies.len());
        throughputs.push(sample_latencies.len() as f64 / elapsed.as_secs_f64());
        latencies.extend(sample_latencies);
    }

    throughputs.sort_by(f64::total_cmp);
    latencies.sort_unstable();
    let p95_index = ((latencies.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(latencies.len() - 1);

    CellResult {
        key,
        operations_per_sample,
        median_throughput: throughputs[throughputs.len() / 2],
        p95_latency: latencies[p95_index],
    }
}

fn run_matrix(label: &str) -> Vec<CellResult> {
    let mut rows = Vec::with_capacity(36);
    for store in [StoreKind::Value, StoreKind::Set, StoreKind::Map] {
        for mode in [Mode::Vector, Mode::File] {
            for profile in [Profile::Write, Profile::Remove, Profile::Callback] {
                for workers in [1, 8] {
                    let key = CellKey {
                        store,
                        mode,
                        profile,
                        workers,
                    };
                    let result = match store {
                        StoreKind::Value => measure(key, || KeyValueBenchStore::new(mode)),
                        StoreKind::Set => measure(key, || KeySetBenchStore::new(mode)),
                        StoreKind::Map => measure(key, || KeyMapBenchStore::new(mode)),
                    };
                    println!(
                        "BENCH,{label},{},{},{},{},{SAMPLES},{},{:.3},{}",
                        store.name(),
                        mode.name(),
                        profile.name(),
                        workers,
                        result.operations_per_sample,
                        result.median_throughput,
                        result.p95_latency.as_nanos()
                    );
                    rows.push(result);
                }
            }
        }
    }

    let keys: HashSet<_> = rows.iter().map(|row| row.key).collect();
    assert_eq!(rows.len(), 36, "benchmark matrix must emit 36 cells");
    assert_eq!(keys.len(), 36, "benchmark cell keys must be unique");
    if let Some(path) = std::env::var_os(BENCHMARK_OUTPUT_ENV) {
        write_benchmark_csv(std::path::Path::new(&path), &rows)
            .expect("write generated benchmark CSV");
    }
    rows
}

fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kibibytes = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kibibytes * 1024)
}

fn unique_key(index: usize) -> Vec<u8> {
    let mut key = vec![0_u8; VALUE_BYTES];
    key[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_ne_bytes());
    key
}

fn run_memory_profile(label: &str) -> Vec<MemoryResult> {
    let mut rows = Vec::new();
    for cycles in [1_000, 1_000_000] {
        let before = resident_bytes().unwrap_or(0);
        let store = DurableKeyValueStore::new_vec_based();
        for index in 0..cycles {
            let key = unique_key(index);
            store.put(key.clone(), fixed_value(index));
            store.remove(&key);
        }
        let retained_keys = store.size();
        let after = resident_bytes().unwrap_or(0);
        assert_eq!(retained_keys, 0, "create/delete must retain no live keys");
        println!(
            "MEMORY,{label},key_value,vector,create_delete,{cycles},{before},{after},{},{}",
            after.saturating_sub(before),
            retained_keys
        );
        rows.push(MemoryResult {
            cycles,
            before,
            after,
            delta: after.saturating_sub(before),
        });
        black_box(store);
    }
    rows
}

fn baseline_cells() -> HashMap<CellKey, (f64, u128)> {
    if let Some(path) = std::env::var_os(BENCHMARK_BASELINE_ENV) {
        return read_baseline_cells(std::path::Path::new(&path))
            .expect("read generated benchmark baseline");
    }
    parse_baseline_cells(include_str!(
        "../../specs/003-fix-mutation-ordering/benchmarks/pre-feature.csv"
    ))
    .expect("parse immutable benchmark baseline")
}

fn read_baseline_cells(path: &std::path::Path) -> std::io::Result<HashMap<CellKey, (f64, u128)>> {
    parse_baseline_cells(&std::fs::read_to_string(path)?)
}

fn read_candidate_rows(path: &std::path::Path) -> std::io::Result<Vec<CellResult>> {
    let contents = std::fs::read_to_string(path)?;
    let mut rows = Vec::new();
    for line in contents.lines().skip(1) {
        let fields: Vec<_> = line.split(',').collect();
        if fields.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("benchmark row has {} fields", fields.len()),
            ));
        }
        let store = match fields[0] {
            "key_value" => StoreKind::Value,
            "key_set" => StoreKind::Set,
            "key_map" => StoreKind::Map,
            value => return Err(invalid_benchmark_field("store", value)),
        };
        let mode = match fields[1] {
            "vector" => Mode::Vector,
            "file" => Mode::File,
            value => return Err(invalid_benchmark_field("mode", value)),
        };
        let profile = match fields[2] {
            "ordinary_write" => Profile::Write,
            "successful_remove" => Profile::Remove,
            "minimal_callback" => Profile::Callback,
            value => return Err(invalid_benchmark_field("profile", value)),
        };
        rows.push(CellResult {
            key: CellKey {
                store,
                mode,
                profile,
                workers: parse_benchmark_field("workers", fields[3])?,
            },
            operations_per_sample: parse_benchmark_field("operations", fields[5])?,
            median_throughput: parse_benchmark_field("throughput", fields[6])?,
            p95_latency: Duration::from_nanos(parse_benchmark_field("p95", fields[7])?),
        });
    }
    Ok(rows)
}

fn parse_baseline_cells(contents: &str) -> std::io::Result<HashMap<CellKey, (f64, u128)>> {
    let mut rows = HashMap::new();
    for line in contents.lines().skip(1) {
        let fields: Vec<_> = line.split(',').collect();
        if fields.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("benchmark row has {} fields", fields.len()),
            ));
        }
        let store = match fields[0] {
            "key_value" => StoreKind::Value,
            "key_set" => StoreKind::Set,
            "key_map" => StoreKind::Map,
            value => return Err(invalid_benchmark_field("store", value)),
        };
        let mode = match fields[1] {
            "vector" => Mode::Vector,
            "file" => Mode::File,
            value => return Err(invalid_benchmark_field("mode", value)),
        };
        let profile = match fields[2] {
            "ordinary_write" => Profile::Write,
            "successful_remove" => Profile::Remove,
            "minimal_callback" => Profile::Callback,
            value => return Err(invalid_benchmark_field("profile", value)),
        };
        let workers = parse_benchmark_field("workers", fields[3])?;
        let throughput = parse_benchmark_field("throughput", fields[6])?;
        let p95_ns = parse_benchmark_field("p95", fields[7])?;
        if rows
            .insert(
                CellKey {
                    store,
                    mode,
                    profile,
                    workers,
                },
                (throughput, p95_ns),
            )
            .is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "duplicate benchmark cell",
            ));
        }
    }
    Ok(rows)
}

fn parse_benchmark_field<T: std::str::FromStr>(name: &str, value: &str) -> std::io::Result<T> {
    value
        .parse()
        .map_err(|_| invalid_benchmark_field(name, value))
}

fn invalid_benchmark_field(name: &str, value: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("invalid benchmark {name}: {value}"),
    )
}

fn write_benchmark_csv(path: &std::path::Path, rows: &[CellResult]) -> std::io::Result<()> {
    use std::fmt::Write as _;

    let mut csv = String::from(
        "store,mode,profile,workers,samples,ops_per_sample,median_throughput,p95_latency_ns\n",
    );
    for row in rows {
        writeln!(
            csv,
            "{},{},{},{},{SAMPLES},{},{:.3},{}",
            row.key.store.name(),
            row.key.mode.name(),
            row.key.profile.name(),
            row.key.workers,
            row.operations_per_sample,
            row.median_throughput,
            row.p95_latency.as_nanos(),
        )
        .expect("write benchmark row to string");
    }
    std::fs::write(path, csv)
}

fn assert_candidate_cells(candidate: &[CellResult]) {
    let baseline = baseline_cells();
    let mut failures = Vec::new();
    for row in candidate {
        let (baseline_throughput, baseline_p95) = baseline[&row.key];
        let minimum_ratio = if row.key.workers == 1 { 0.90 } else { 0.85 };
        let throughput_ratio = row.median_throughput / baseline_throughput;
        let latency_ratio = row.p95_latency.as_nanos() as f64 / baseline_p95 as f64;
        println!(
            "RATIO,{},{},{},{},{throughput_ratio:.6},{latency_ratio:.6}",
            row.key.store.name(),
            row.key.mode.name(),
            row.key.profile.name(),
            row.key.workers,
        );
        if throughput_ratio < minimum_ratio {
            failures.push(format!(
                "throughput {throughput_ratio:.3} < {minimum_ratio:.2} for {}/{}/{}/{}",
                row.key.store.name(),
                row.key.mode.name(),
                row.key.profile.name(),
                row.key.workers
            ));
        }
        if latency_ratio > 1.25 {
            failures.push(format!(
                "p95 {latency_ratio:.3} > 1.25 for {}/{}/{}/{}",
                row.key.store.name(),
                row.key.mode.name(),
                row.key.profile.name(),
                row.key.workers
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "candidate cell failures:\n{}",
        failures.join("\n")
    );
}

fn baseline_memory() -> HashMap<usize, u64> {
    include_str!("../../specs/003-fix-mutation-ordering/benchmarks/pre-feature-memory.csv")
        .lines()
        .skip(1)
        .map(|line| {
            let fields: Vec<_> = line.split(',').collect();
            (
                fields[3].parse().expect("parse baseline memory cycles"),
                fields[6].parse().expect("parse baseline memory delta"),
            )
        })
        .collect()
}

fn assert_retained_memory(candidate: &[MemoryResult]) {
    let baseline = baseline_memory();
    let added: HashMap<_, _> = candidate
        .iter()
        .map(|row| {
            assert!(row.after >= row.before || row.delta == 0);
            (row.cycles, row.delta.saturating_sub(baseline[&row.cycles]))
        })
        .collect();
    let allowed = (added[&1_000] as f64 * 1.10).ceil() as u64;
    assert!(
        added[&1_000_000] <= allowed,
        "added retained memory grew with historical keys: 1k={}, 1m={}, allowed={allowed}",
        added[&1_000],
        added[&1_000_000]
    );
}

#[test]
fn benchmark_samples_use_stable_operation_volume() {
    let key = CellKey {
        store: StoreKind::Value,
        mode: Mode::Vector,
        profile: Profile::Write,
        workers: 1,
    };
    let result = measure(key, || KeyValueBenchStore::new(Mode::Vector));
    assert!(
        result.operations_per_sample >= 1_024,
        "each sample must amortize timer and scheduler noise"
    );
}

#[test]
fn generated_benchmark_csv_can_drive_candidate_pairing() {
    let directory = tempfile::tempdir().expect("create benchmark CSV directory");
    let path = directory.path().join("baseline.csv");
    let key = CellKey {
        store: StoreKind::Value,
        mode: Mode::Vector,
        profile: Profile::Write,
        workers: 1,
    };
    let row = CellResult {
        key,
        operations_per_sample: 1_024,
        median_throughput: 12_345.0,
        p95_latency: Duration::from_nanos(678),
    };
    write_benchmark_csv(&path, &[row]).expect("write generated benchmark CSV");
    let parsed = read_baseline_cells(&path).expect("read generated benchmark CSV");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[&key], (12_345.0, 678));
    let candidate = read_candidate_rows(&path).expect("read generated candidate CSV");
    assert_eq!(candidate.len(), 1);
    assert_eq!(candidate[0].key, key);
    assert_eq!(candidate[0].operations_per_sample, 1_024);
}

#[test]
#[ignore = "release-only immutable pre-feature benchmark"]
fn paired_baseline() {
    black_box(run_matrix("baseline"));
    black_box(run_memory_profile("baseline"));
}

#[test]
#[ignore = "release-only paired candidate benchmark"]
fn paired_candidate() {
    let rows = match std::env::var_os(BENCHMARK_CANDIDATE_ENV) {
        Some(path) => read_candidate_rows(std::path::Path::new(&path))
            .expect("read generated benchmark candidate"),
        None => run_matrix("candidate"),
    };
    assert_candidate_cells(&rows);
    let memory = run_memory_profile("candidate");
    assert_retained_memory(&memory);
    black_box((rows, memory));
}

pub(super) fn assert_key_map_vector_eight_worker_write_threshold() {
    let key = CellKey {
        store: StoreKind::Map,
        mode: Mode::Vector,
        profile: Profile::Write,
        workers: 8,
    };
    let result = measure(key, || KeyMapBenchStore::new(Mode::Vector));
    println!(
        "BENCH,candidate,{},{},{},{},{SAMPLES},{},{:.3},{}",
        result.key.store.name(),
        result.key.mode.name(),
        result.key.profile.name(),
        result.key.workers,
        result.operations_per_sample,
        result.median_throughput,
        result.p95_latency.as_nanos()
    );
    assert_candidate_cells(&[result]);
}

#[test]
#[ignore = "release-only retained-ordering-memory report"]
fn retained_ordering_memory() {
    let rows = run_memory_profile("candidate");
    assert_retained_memory(&rows);
    black_box(rows);
}
