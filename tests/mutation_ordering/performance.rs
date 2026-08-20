//! Ignored paired performance and retained-memory gates.

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
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
const BENCHMARK_CAPTURE_ID_ENV: &str = "PIGMENT_DB_COMPACTION_CAPTURE_ID";
const BENCHMARK_DIRTY_SHA256_ENV: &str = "PIGMENT_DB_COMPACTION_DIRTY_SHA256";
const BENCHMARK_HARNESS_SHA256_ENV: &str = "PIGMENT_DB_COMPACTION_HARNESS_SHA256";
const BENCHMARK_SMOKE_ENV: &str = "PIGMENT_DB_COMPACTION_BENCHMARK_SMOKE";

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

#[derive(Clone, Debug)]
struct CellResult {
    key: CellKey,
    operations_per_sample: usize,
    median_throughput: f64,
    p95_latency: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RunParameters {
    warmups: usize,
    samples: usize,
    min_sample_duration: Duration,
    min_operations_per_sample: usize,
}

impl RunParameters {
    const ACCEPTANCE: Self = Self {
        warmups: WARMUPS,
        samples: SAMPLES,
        min_sample_duration: MIN_SAMPLE_DURATION,
        min_operations_per_sample: MIN_OPERATIONS_PER_SAMPLE,
    };

    const SMOKE: Self = Self {
        warmups: 0,
        samples: 1,
        min_sample_duration: Duration::ZERO,
        min_operations_per_sample: 1,
    };
}

#[derive(Debug, Eq, PartialEq)]
struct CaptureProvenance {
    capture_id: String,
    commit: String,
    dirty_sha256: String,
    harness_sha256: String,
    rustc: String,
    cargo: String,
    target: String,
    os: String,
    cpu: String,
    filesystem: String,
    affinity: String,
    working_directory: PathBuf,
    temporary_directory: PathBuf,
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

fn run_sample<S: BenchStore>(
    store: &S,
    key: CellKey,
    parameters: RunParameters,
) -> (Duration, Vec<Duration>) {
    let mut elapsed = Duration::ZERO;
    let mut latencies = Vec::new();
    let mut operation_base = 0;
    while elapsed < parameters.min_sample_duration
        || latencies.len() < parameters.min_operations_per_sample
    {
        let (round_elapsed, mut round_latencies) =
            run_once(store, key.profile, key.workers, operation_base);
        operation_base += round_latencies.len() / key.workers;
        elapsed += round_elapsed;
        latencies.append(&mut round_latencies);
    }
    (elapsed, latencies)
}

fn measure<S: BenchStore>(
    key: CellKey,
    parameters: RunParameters,
    mut build: impl FnMut() -> S,
) -> CellResult {
    for _ in 0..parameters.warmups {
        let store = build();
        black_box(run_sample(&store, key, parameters));
    }

    let mut throughputs = Vec::with_capacity(parameters.samples);
    let mut latencies = Vec::new();
    let mut operations_per_sample = usize::MAX;
    for _ in 0..parameters.samples {
        let store = build();
        let (elapsed, sample_latencies) = run_sample(&store, key, parameters);
        operations_per_sample = operations_per_sample.min(sample_latencies.len());
        throughputs.push(sample_latencies.len() as f64 / elapsed.as_secs_f64());
        latencies.extend(sample_latencies);
    }

    CellResult {
        key,
        operations_per_sample,
        median_throughput: median(&mut throughputs),
        p95_latency: aggregate_p95(&mut latencies),
    }
}

fn feature_cell_keys() -> Vec<CellKey> {
    let mut keys = Vec::with_capacity(36);
    for store in [StoreKind::Value, StoreKind::Set, StoreKind::Map] {
        for mode in [Mode::Vector, Mode::File] {
            for profile in [Profile::Write, Profile::Remove, Profile::Callback] {
                for workers in [1, 8] {
                    keys.push(CellKey {
                        store,
                        mode,
                        profile,
                        workers,
                    });
                }
            }
        }
    }
    keys
}

fn run_matrix(label: &str) -> Vec<CellResult> {
    let parameters = if std::env::var_os(BENCHMARK_SMOKE_ENV).is_some() {
        RunParameters::SMOKE
    } else {
        RunParameters::ACCEPTANCE
    };
    let mut rows = Vec::with_capacity(36);
    for key in feature_cell_keys() {
        let result = match key.store {
            StoreKind::Value => measure(key, parameters, || KeyValueBenchStore::new(key.mode)),
            StoreKind::Set => measure(key, parameters, || KeySetBenchStore::new(key.mode)),
            StoreKind::Map => measure(key, parameters, || KeyMapBenchStore::new(key.mode)),
        };
        println!(
            "BENCH,{label},{},{},{},{},{},{},{:.3},{}",
            key.store.name(),
            key.mode.name(),
            key.profile.name(),
            key.workers,
            parameters.samples,
            result.operations_per_sample,
            result.median_throughput,
            result.p95_latency.as_nanos()
        );
        rows.push(result);
    }

    validate_capture_rows(&rows, parameters).expect("benchmark matrix must be complete and valid");
    if let Some(path) = std::env::var_os(BENCHMARK_OUTPUT_ENV) {
        let path = Path::new(&path);
        write_benchmark_csv(path, &rows, parameters).expect("write generated benchmark CSV");
        if std::env::var_os(BENCHMARK_CAPTURE_ID_ENV).is_some() {
            let provenance = collect_capture_provenance()
                .expect("collect feature benchmark environment provenance");
            write_capture_metadata(path, label, parameters, &provenance)
                .expect("write feature benchmark provenance");
        }
    }
    rows
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one value");
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn aggregate_p95(values: &mut [Duration]) -> Duration {
    assert!(!values.is_empty(), "p95 requires at least one value");
    values.sort_unstable();
    let index = ((values.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn validate_capture_rows(rows: &[CellResult], parameters: RunParameters) -> io::Result<()> {
    let expected: HashSet<_> = feature_cell_keys().into_iter().collect();
    let actual: HashSet<_> = rows.iter().map(|row| row.key).collect();
    if rows.len() != expected.len() || actual != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "benchmark capture must contain every feature cell exactly once",
        ));
    }
    for row in rows {
        if row.operations_per_sample < parameters.min_operations_per_sample {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "benchmark sample does not meet the operation floor",
            ));
        }
        if !row.median_throughput.is_finite() || row.median_throughput <= 0.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "benchmark throughput must be finite and positive",
            ));
        }
        if row.p95_latency.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "benchmark p95 latency must be positive",
            ));
        }
    }
    Ok(())
}

fn validate_sha256(value: &str) -> io::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SHA-256 digest must contain exactly 64 lowercase hexadecimal characters",
        ))
    }
}

fn validate_source_digest(actual: &str, expected: &str) -> io::Result<()> {
    validate_sha256(actual)?;
    validate_sha256(expected)?;
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "benchmark harness digest does not match the frozen source",
        ))
    }
}

fn threshold_passes(workers: usize, throughput_ratio: f64, latency_ratio: f64) -> bool {
    let minimum_throughput = if workers == 1 { 0.90 } else { 0.85 };
    throughput_ratio >= minimum_throughput && latency_ratio <= 1.25
}

fn collect_capture_provenance() -> io::Result<CaptureProvenance> {
    let capture_id = required_environment(BENCHMARK_CAPTURE_ID_ENV)?;
    let dirty_sha256 = required_environment(BENCHMARK_DIRTY_SHA256_ENV)?;
    let harness_sha256 = required_environment(BENCHMARK_HARNESS_SHA256_ENV)?;
    validate_sha256(&dirty_sha256)?;
    validate_sha256(&harness_sha256)?;
    let actual_harness_sha256 = command_output("sha256sum", &[file!()])?
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned();
    validate_source_digest(&actual_harness_sha256, &harness_sha256)?;

    let rustc_verbose = command_output("rustc", &["--version", "--verbose"])?;
    let target = rustc_verbose
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or("unknown")
        .to_owned();
    let rustc = rustc_verbose.lines().next().unwrap_or("unknown").to_owned();
    let temporary_directory = std::env::temp_dir().canonicalize()?;
    let filesystem = command_output(
        "findmnt",
        &[
            "-T",
            temporary_directory.to_str().unwrap_or_default(),
            "-n",
            "-o",
            "SOURCE,FSTYPE,OPTIONS",
        ],
    )?
    .trim()
    .to_owned();
    if filesystem.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "benchmark filesystem provenance is empty",
        ));
    }

    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let affinity = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .unwrap_or("unknown")
        .to_owned();
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .find_map(|line| line.strip_prefix("model name\t: "))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_owned());

    Ok(CaptureProvenance {
        capture_id,
        commit: command_output("git", &["rev-parse", "HEAD"])?
            .trim()
            .to_owned(),
        dirty_sha256,
        harness_sha256,
        rustc,
        cargo: command_output("cargo", &["--version"])?.trim().to_owned(),
        target,
        os: command_output("uname", &["-srm"])?.trim().to_owned(),
        cpu,
        filesystem,
        affinity,
        working_directory: std::env::current_dir()?.canonicalize()?,
        temporary_directory,
    })
}

fn required_environment(name: &str) -> io::Result<String> {
    std::env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is required for an immutable feature capture"),
        )
    })
}

fn command_output(program: &str, arguments: &[&str]) -> io::Result<String> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!("{program} failed")));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn write_capture_metadata(
    csv_path: &Path,
    label: &str,
    parameters: RunParameters,
    provenance: &CaptureProvenance,
) -> io::Result<()> {
    use std::fmt::Write as _;

    let metadata_path = PathBuf::from(format!("{}.metadata", csv_path.display()));
    let mut metadata = String::new();
    for (name, value) in [
        ("capture_id", provenance.capture_id.clone()),
        ("label", label.to_owned()),
        ("commit", provenance.commit.clone()),
        ("dirty_sha256", provenance.dirty_sha256.clone()),
        ("harness_sha256", provenance.harness_sha256.clone()),
        ("rustc", provenance.rustc.clone()),
        ("cargo", provenance.cargo.clone()),
        ("target", provenance.target.clone()),
        ("os", provenance.os.clone()),
        ("cpu", provenance.cpu.clone()),
        ("filesystem", provenance.filesystem.clone()),
        ("affinity", provenance.affinity.clone()),
        (
            "working_directory",
            provenance.working_directory.display().to_string(),
        ),
        (
            "temporary_directory",
            provenance.temporary_directory.display().to_string(),
        ),
        ("payload_bytes", VALUE_BYTES.to_string()),
        ("warmups", parameters.warmups.to_string()),
        ("samples", parameters.samples.to_string()),
        (
            "min_sample_duration_ms",
            parameters.min_sample_duration.as_millis().to_string(),
        ),
        (
            "min_operations_per_sample",
            parameters.min_operations_per_sample.to_string(),
        ),
    ] {
        writeln!(metadata, "{name}={}", value.replace(['\r', '\n'], " "))
            .expect("write benchmark metadata field");
    }
    std::fs::write(metadata_path, metadata)
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

fn write_benchmark_csv(
    path: &Path,
    rows: &[CellResult],
    parameters: RunParameters,
) -> io::Result<()> {
    use std::fmt::Write as _;

    let mut csv = String::from(
        "store,mode,profile,workers,samples,ops_per_sample,median_throughput,p95_latency_ns\n",
    );
    for row in rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{:.3},{}",
            row.key.store.name(),
            row.key.mode.name(),
            row.key.profile.name(),
            row.key.workers,
            parameters.samples,
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
    let result = measure(key, RunParameters::ACCEPTANCE, || {
        KeyValueBenchStore::new(Mode::Vector)
    });
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
    write_benchmark_csv(&path, &[row], RunParameters::ACCEPTANCE)
        .expect("write generated benchmark CSV");
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
    if std::env::var_os(BENCHMARK_SMOKE_ENV).is_none() {
        black_box(run_memory_profile("baseline"));
    }
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
    if std::env::var_os(BENCHMARK_SMOKE_ENV).is_none() {
        let memory = run_memory_profile("candidate");
        assert_retained_memory(&memory);
        black_box((rows, memory));
    } else {
        black_box(rows);
    }
}

#[test]
fn feature_matrix_contains_every_cell_exactly_once() {
    let keys = feature_cell_keys();
    let unique: HashSet<_> = keys.iter().copied().collect();
    assert_eq!(keys.len(), 36);
    assert_eq!(unique.len(), 36);
    assert!(keys.iter().all(|key| matches!(key.workers, 1 | 8)));
}

#[test]
fn benchmark_median_selects_the_middle_of_eleven_samples() {
    let mut values = [11.0, 1.0, 8.0, 3.0, 7.0, 2.0, 10.0, 4.0, 9.0, 5.0, 6.0];
    assert_eq!(median(&mut values), 6.0);
}

#[test]
fn benchmark_p95_aggregates_all_public_call_latencies() {
    let mut values: Vec<_> = (1..=20).map(Duration::from_nanos).collect();
    assert_eq!(aggregate_p95(&mut values), Duration::from_nanos(19));
}

#[test]
fn benchmark_source_digest_requires_an_exact_sha256_match() {
    let frozen = "a".repeat(64);
    assert!(validate_source_digest(&frozen, &frozen).is_ok());
    assert!(validate_source_digest(&"b".repeat(64), &frozen).is_err());
    assert!(validate_source_digest("not-a-sha256", &frozen).is_err());
}

#[test]
fn benchmark_capture_validation_rejects_missing_duplicate_and_invalid_cells() {
    let valid: Vec<_> = feature_cell_keys()
        .into_iter()
        .map(|key| CellResult {
            key,
            operations_per_sample: MIN_OPERATIONS_PER_SAMPLE,
            median_throughput: 1.0,
            p95_latency: Duration::from_nanos(1),
        })
        .collect();
    assert!(validate_capture_rows(&valid, RunParameters::ACCEPTANCE).is_ok());

    let mut missing = valid.clone();
    missing.pop();
    assert!(validate_capture_rows(&missing, RunParameters::ACCEPTANCE).is_err());

    let mut duplicate = valid.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(validate_capture_rows(&duplicate, RunParameters::ACCEPTANCE).is_err());

    let mut invalid = valid;
    invalid[0].median_throughput = f64::NAN;
    assert!(validate_capture_rows(&invalid, RunParameters::ACCEPTANCE).is_err());
}

#[test]
fn benchmark_thresholds_are_inclusive() {
    assert!(threshold_passes(1, 0.90, 1.25));
    assert!(threshold_passes(8, 0.85, 1.25));
    assert!(!threshold_passes(1, 0.899_999, 1.0));
    assert!(!threshold_passes(8, 0.849_999, 1.0));
    assert!(!threshold_passes(8, 1.0, 1.250_001));
}

pub(super) fn assert_key_map_vector_eight_worker_write_threshold() {
    let key = CellKey {
        store: StoreKind::Map,
        mode: Mode::Vector,
        profile: Profile::Write,
        workers: 8,
    };
    let result = measure(key, RunParameters::ACCEPTANCE, || {
        KeyMapBenchStore::new(Mode::Vector)
    });
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
