//! Shared public-only integration and benchmark helpers.
#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc::Receiver, Arc, Barrier};
use std::time::Duration;

use pigment_db::key_map_store::DurableKeyMapStore;
use pigment_db::key_set_store::DurableKeySetStore;
use pigment_db::key_value_store::DurableKeyValueStore;
use pigment_db::model::SearchKey;

pub const PAYLOAD_BYTES: usize = 32;
pub const WARMUP_COUNT: usize = 5;
pub const SAMPLE_COUNT: usize = 11;
pub const MIN_OPERATIONS: usize = 1_024;
pub const MIN_SAMPLE_DURATION: Duration = Duration::from_millis(100);
pub const BENCHMARK_ROOT_ENV: &str = "PIGMENT_DB_DURABILITY_BENCH_ROOT";
pub const BENCHMARK_OUTPUT_ENV: &str = "PIGMENT_DB_DURABILITY_OUTPUT";
pub const BENCHMARK_CAPTURE_ID_ENV: &str = "PIGMENT_DB_DURABILITY_CAPTURE_ID";

static NEXT_RUN: AtomicU64 = AtomicU64::new(0);

pub const WATCHDOG: Duration = Duration::from_secs(10);
pub type SetSnapshot = Option<HashSet<Vec<u8>>>;
pub type MapSnapshot = Option<BTreeMap<SearchKey, Vec<u8>>>;

#[derive(Clone, Default)]
pub struct CallbackCounter(Arc<AtomicUsize>);

impl CallbackCounter {
    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

pub fn scratch_directory(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create scratch directory")
}

pub fn restore_files(root: &Path, files: &[(PathBuf, Vec<u8>)]) -> io::Result<()> {
    for (relative, bytes) in files {
        if relative.is_absolute()
            || relative.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::RootDir
                )
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "restore path must remain relative to its scratch root",
            ));
        }
        let destination = root.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(destination, bytes)?;
    }
    Ok(())
}

pub fn run_process(program: &OsStr, args: &[&OsStr]) -> std::process::Output {
    Command::new(program)
        .args(args)
        .output()
        .expect("run durability test process")
}

pub fn recv_with_watchdog<T>(receiver: &Receiver<T>, context: &str) -> T {
    receiver
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|error| panic!("{context}: {error}"))
}

pub fn run_concurrently<F, G>(left: F, right: G)
where
    F: FnOnce() + Send,
    G: FnOnce() + Send,
{
    std::thread::scope(|scope| {
        let barrier = Arc::new(Barrier::new(3));
        let left_barrier = Arc::clone(&barrier);
        let left = scope.spawn(move || {
            left_barrier.wait();
            left();
        });
        let right_barrier = Arc::clone(&barrier);
        let right = scope.spawn(move || {
            right_barrier.wait();
            right();
        });
        barrier.wait();
        left.join().expect("left durability worker must join");
        right.join().expect("right durability worker must join");
    });
}

pub fn assert_key_value_reopens(directory: &Path, key: &[u8], expected: &Option<Vec<u8>>) {
    let store = DurableKeyValueStore::try_init_new(directory)
        .expect("reopen key/value store")
        .into_store();
    assert_eq!(&store.get(key), expected);
}

pub fn assert_key_set_reopens(directory: &Path, key: &[u8], expected: &SetSnapshot) {
    let store = DurableKeySetStore::try_init_new(directory)
        .expect("reopen key/set store")
        .into_store();
    assert_eq!(&store.get_hashset(key), expected);
}

pub fn assert_key_map_reopens(directory: &Path, key: &[u8], expected: &MapSnapshot) {
    let store = DurableKeyMapStore::try_init_new(directory)
        .expect("reopen key/map store")
        .into_store();
    assert_eq!(&store.get_sorted_map(key), expected);
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum StoreFamily {
    KeyValue,
    KeySet,
    KeyMap,
}

impl StoreFamily {
    pub const ALL: [Self; 3] = [Self::KeyValue, Self::KeySet, Self::KeyMap];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::KeyValue => "key_value",
            Self::KeySet => "key_set",
            Self::KeyMap => "key_map",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StorageMode {
    Vector,
    File,
}

impl StorageMode {
    pub const ALL: [Self; 2] = [Self::Vector, Self::File];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::File => "file",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Workload {
    Write,
    Remove,
    Callback,
}

impl Workload {
    pub const ALL: [Self; 3] = [Self::Write, Self::Remove, Self::Callback];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Write => "ordinary_write",
            Self::Remove => "successful_remove",
            Self::Callback => "minimal_callback",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Implementation {
    PigmentDb,
    Reference,
}

impl Implementation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PigmentDb => "pigment_db",
            Self::Reference => "mutex_file_reference",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Policy {
    Buffered,
    Physical,
    Reference,
}

impl Policy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Physical => "physical",
            Self::Reference => "append_plus_barrier",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CellKey {
    pub family: StoreFamily,
    pub storage: StorageMode,
    pub workload: Workload,
    pub workers: usize,
}

#[derive(Clone, Debug)]
pub struct Provenance {
    pub capture_id: String,
    pub commit: String,
    pub dirty_hash: String,
    pub toolchain: String,
    pub target: String,
    pub os: String,
    pub cpu: String,
    pub filesystem: String,
    pub benchmark_root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct CaptureRow {
    pub sample_index: usize,
    pub key: CellKey,
    pub implementation: Implementation,
    pub policy: Policy,
    pub operations: usize,
    pub elapsed: Duration,
    pub p95_latency_ns: u128,
    pub failed_operations: usize,
}

impl CaptureRow {
    pub fn ops_per_second(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64()
    }
}

pub fn fixed_bytes(tag: usize) -> Vec<u8> {
    let mut bytes = vec![b'x'; PAYLOAD_BYTES];
    bytes[..std::mem::size_of::<usize>()].copy_from_slice(&tag.to_ne_bytes());
    bytes
}

pub fn p95_nanos(latencies: &mut [Duration]) -> u128 {
    assert!(
        !latencies.is_empty(),
        "a valid sample has latency observations"
    );
    latencies.sort_unstable();
    let index = ((latencies.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(latencies.len() - 1);
    latencies[index].as_nanos()
}

pub fn benchmark_root() -> io::Result<PathBuf> {
    let raw = std::env::var_os(BENCHMARK_ROOT_ENV).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{BENCHMARK_ROOT_ENV} must name an explicit real-filesystem directory"),
        )
    })?;
    let root = PathBuf::from(raw);
    if !root.is_absolute() || !root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "benchmark root must be an existing absolute directory: {}",
                root.display()
            ),
        ));
    }
    let root = root.canonicalize()?;
    let filesystem = command_output(
        "findmnt",
        [
            "-T",
            root.to_str().unwrap_or_default(),
            "-n",
            "-o",
            "FSTYPE",
        ],
    )?;
    match filesystem.trim() {
        "tmpfs" | "ramfs" | "devtmpfs" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("benchmark root uses an in-memory filesystem: {filesystem}"),
        )),
        "" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark filesystem could not be identified",
        )),
        _ => Ok(root),
    }
}

pub fn output_path() -> io::Result<PathBuf> {
    std::env::var_os(BENCHMARK_OUTPUT_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{BENCHMARK_OUTPUT_ENV} must name the immutable capture CSV"),
            )
        })
}

pub fn capture_id() -> io::Result<String> {
    std::env::var(BENCHMARK_CAPTURE_ID_ENV).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{BENCHMARK_CAPTURE_ID_ENV} must name the unique candidate attempt"),
        )
    })
}

pub fn unique_sample_path(root: &Path, label: &str) -> PathBuf {
    let sequence = NEXT_RUN.fetch_add(1, Ordering::Relaxed);
    root.join(format!(
        "pigment-db-durability-{}-{}-{sequence}",
        std::process::id(),
        sanitize(label)
    ))
}

pub fn collect_provenance(capture_id: &str, root: &Path) -> io::Result<Provenance> {
    let status = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .output()?;
    if !status.status.success() {
        return Err(io::Error::other(
            "git status failed while collecting provenance",
        ));
    }
    let mut hasher = DefaultHasher::new();
    status.stdout.hash(&mut hasher);
    let verbose = command_output("rustc", ["--version", "--verbose"])?;
    let target = verbose
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or("unknown")
        .to_owned();
    let toolchain = verbose.lines().next().unwrap_or("unknown").to_owned();
    let cpu = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .find_map(|line| line.strip_prefix("model name\t: "))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_owned());
    Ok(Provenance {
        capture_id: capture_id.to_owned(),
        commit: command_output("git", ["rev-parse", "HEAD"])?
            .trim()
            .to_owned(),
        dirty_hash: format!("default-hasher:{:016x}", hasher.finish()),
        toolchain,
        target,
        os: command_output("uname", ["-srm"])?.trim().to_owned(),
        cpu,
        filesystem: command_output(
            "findmnt",
            [
                "-T",
                root.to_str().unwrap_or_default(),
                "-n",
                "-o",
                "SOURCE,FSTYPE,OPTIONS",
            ],
        )?
        .trim()
        .to_owned(),
        benchmark_root: root.to_owned(),
    })
}

pub fn write_capture_csv(
    path: &Path,
    provenance: &Provenance,
    rows: &[CaptureRow],
) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "capture CSV has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;
    let mut output = String::from(concat!(
        "capture_id,commit,dirty_hash,toolchain,target,os,cpu,filesystem,benchmark_root,",
        "implementation,policy,store_family,storage_mode,workload,workers,payload_bytes,",
        "warmup_count,sample_index,operations,elapsed_ns,ops_per_second,p95_latency_ns,",
        "failed_operations\n"
    ));
    for row in rows {
        let fields = [
            provenance.capture_id.clone(),
            provenance.commit.clone(),
            provenance.dirty_hash.clone(),
            provenance.toolchain.clone(),
            provenance.target.clone(),
            provenance.os.clone(),
            provenance.cpu.clone(),
            provenance.filesystem.clone(),
            provenance.benchmark_root.display().to_string(),
            row.implementation.as_str().to_owned(),
            row.policy.as_str().to_owned(),
            row.key.family.as_str().to_owned(),
            row.key.storage.as_str().to_owned(),
            row.key.workload.as_str().to_owned(),
            row.key.workers.to_string(),
            PAYLOAD_BYTES.to_string(),
            WARMUP_COUNT.to_string(),
            row.sample_index.to_string(),
            row.operations.to_string(),
            row.elapsed.as_nanos().to_string(),
            format!("{:.6}", row.ops_per_second()),
            row.p95_latency_ns.to_string(),
            row.failed_operations.to_string(),
        ];
        output.push_str(&fields.map(|field| csv_field(&field)).join(","));
        output.push('\n');
    }
    fs::write(path, output)
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn command_output<I, S>(program: &str, args: I) -> io::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!("{program} failed")));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn sanitize(label: &str) -> String {
    label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}
