use pigment_db_baseline as baseline;
use pigment_db_candidate as candidate;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

const INITIAL_AFFINITY: &str = "11-19";
const COORDINATOR_CPU: usize = 11;
const WORKER_CPU_START: usize = 12;
const MAX_WORKERS: usize = 8;
const WARMUP_PAIRS: usize = 5;
const MEASURED_PAIRS: usize = 11;
const OPERATIONS_PER_WORKER: usize = 16_384;
const VALIDATION_OPERATIONS: usize = 256;

#[derive(Clone, Copy)]
enum Family {
    Value,
    Set,
    Map,
}

impl Family {
    const ALL: [Self; 3] = [Self::Value, Self::Set, Self::Map];

    fn name(self) -> &'static str {
        match self {
            Self::Value => "key_value",
            Self::Set => "key_set",
            Self::Map => "key_map",
        }
    }
}

struct SystemSnapshot {
    cpu_some_stall_us: u64,
    io_some_stall_us: u64,
    io_full_stall_us: u64,
    load_one: f64,
}

impl SystemSnapshot {
    fn capture() -> Self {
        let cpu = std::fs::read_to_string("/proc/pressure/cpu").expect("read /proc/pressure/cpu");
        let io = std::fs::read_to_string("/proc/pressure/io").expect("read /proc/pressure/io");
        let load = std::fs::read_to_string("/proc/loadavg").expect("read /proc/loadavg");
        Self {
            cpu_some_stall_us: parse_pressure_total(&cpu, "some")
                .expect("parse CPU some pressure total"),
            io_some_stall_us: parse_pressure_total(&io, "some")
                .expect("parse I/O some pressure total"),
            io_full_stall_us: parse_pressure_total(&io, "full")
                .expect("parse I/O full pressure total"),
            load_one: load
                .split_whitespace()
                .next()
                .expect("load average field")
                .parse()
                .expect("parse one-minute load average"),
        }
    }
}

#[derive(Clone, Copy)]
struct ThreadSnapshot {
    cpu_ticks: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
}

impl ThreadSnapshot {
    fn capture() -> Self {
        let stat =
            std::fs::read_to_string("/proc/thread-self/stat").expect("read worker thread stat");
        let status =
            std::fs::read_to_string("/proc/thread-self/status").expect("read worker thread status");
        let (voluntary_context_switches, involuntary_context_switches) =
            parse_thread_context_switches(&status).expect("parse worker context switches");
        Self {
            cpu_ticks: parse_thread_cpu_ticks(&stat).expect("parse worker CPU ticks"),
            voluntary_context_switches,
            involuntary_context_switches,
        }
    }
}

#[derive(Default)]
struct WorkerMetrics {
    cpu_ticks: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
}

impl WorkerMetrics {
    fn between(before: ThreadSnapshot, after: ThreadSnapshot) -> Self {
        Self {
            cpu_ticks: after.cpu_ticks.saturating_sub(before.cpu_ticks),
            voluntary_context_switches: after
                .voluntary_context_switches
                .saturating_sub(before.voluntary_context_switches),
            involuntary_context_switches: after
                .involuntary_context_switches
                .saturating_sub(before.involuntary_context_switches),
        }
    }

    fn add(&mut self, other: Self) {
        self.cpu_ticks = self.cpu_ticks.saturating_add(other.cpu_ticks);
        self.voluntary_context_switches = self
            .voluntary_context_switches
            .saturating_add(other.voluntary_context_switches);
        self.involuntary_context_switches = self
            .involuntary_context_switches
            .saturating_add(other.involuntary_context_switches);
    }
}

struct WorkerRun {
    elapsed: Duration,
    metrics: WorkerMetrics,
}

struct Measurement {
    elapsed: Duration,
    worker_cpu_ticks: u64,
    worker_voluntary_context_switches: u64,
    worker_involuntary_context_switches: u64,
    cpu_some_stall_us: u64,
    io_some_stall_us: u64,
    io_full_stall_us: u64,
    load_one_before: f64,
    load_one_after: f64,
}

impl Measurement {
    fn capture(operation: impl FnOnce() -> WorkerRun) -> Self {
        let before = SystemSnapshot::capture();
        let run = operation();
        let after = SystemSnapshot::capture();
        Self {
            elapsed: run.elapsed,
            worker_cpu_ticks: run.metrics.cpu_ticks,
            worker_voluntary_context_switches: run.metrics.voluntary_context_switches,
            worker_involuntary_context_switches: run.metrics.involuntary_context_switches,
            cpu_some_stall_us: after
                .cpu_some_stall_us
                .saturating_sub(before.cpu_some_stall_us),
            io_some_stall_us: after
                .io_some_stall_us
                .saturating_sub(before.io_some_stall_us),
            io_full_stall_us: after
                .io_full_stall_us
                .saturating_sub(before.io_full_stall_us),
            load_one_before: before.load_one,
            load_one_after: after.load_one,
        }
    }
}

struct Row {
    pair: usize,
    position: usize,
    family: Family,
    workers: usize,
    implementation: &'static str,
    measurement: Measurement,
}

impl Row {
    fn operations(&self) -> usize {
        self.workers * OPERATIONS_PER_WORKER
    }

    fn writes_per_second(&self) -> f64 {
        self.operations() as f64 / self.measurement.elapsed.as_secs_f64()
    }
}

fn worker_cpu(worker: usize) -> Option<usize> {
    (worker < MAX_WORKERS).then(|| WORKER_CPU_START + worker)
}

fn worker_cpu_list(workers: usize) -> String {
    if workers == 1 {
        WORKER_CPU_START.to_string()
    } else {
        format!("{WORKER_CPU_START}-{}", WORKER_CPU_START + workers - 1)
    }
}

fn parse_allowed_list(status: &str) -> Option<&str> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .map(str::trim)
}

fn current_allowed_list() -> String {
    let status =
        std::fs::read_to_string("/proc/thread-self/status").expect("read current thread status");
    parse_allowed_list(&status)
        .expect("current thread status contains Cpus_allowed_list")
        .to_owned()
}

fn current_thread_id() -> u32 {
    std::fs::read_to_string("/proc/thread-self/stat")
        .expect("read current thread stat")
        .split_whitespace()
        .next()
        .expect("thread stat contains id")
        .parse()
        .expect("parse thread id")
}

fn pin_task(cpu: usize, task_id: u32) {
    let output = Command::new("taskset")
        .args(["-pc", &cpu.to_string(), &task_id.to_string()])
        .output()
        .expect("execute taskset for benchmark thread");
    assert!(
        output.status.success(),
        "taskset failed for task {task_id} on CPU {cpu}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_pressure_total(contents: &str, class: &str) -> Option<u64> {
    contents
        .lines()
        .find(|line| line.split_whitespace().next() == Some(class))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))?
        .parse()
        .ok()
}

fn parse_thread_cpu_ticks(contents: &str) -> Option<u64> {
    let (_, fields) = contents.rsplit_once(") ")?;
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    user_ticks.checked_add(system_ticks)
}

fn parse_thread_context_switches(contents: &str) -> Option<(u64, u64)> {
    let parse_named = |name: &str| {
        contents
            .lines()
            .find_map(|line| line.strip_prefix(name))?
            .trim()
            .parse::<u64>()
            .ok()
    };
    Some((
        parse_named("voluntary_ctxt_switches:")?,
        parse_named("nonvoluntary_ctxt_switches:")?,
    ))
}

fn fixed_key(worker: usize, operation: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&(worker as u64).to_le_bytes());
    key.extend_from_slice(&(operation as u64).to_le_bytes());
    key
}

fn run_workers<F>(workers: usize, operations_per_worker: usize, operation: F) -> WorkerRun
where
    F: Fn(usize, usize) + Send + Sync + 'static,
{
    assert!((1..=MAX_WORKERS).contains(&workers));
    let operation = Arc::new(operation);
    let pin_barrier = Arc::new(Barrier::new(workers + 1));
    let start_barrier = Arc::new(Barrier::new(workers + 1));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (verified_tx, verified_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let handles = (0..workers)
        .map(|worker| {
            let operation = Arc::clone(&operation);
            let pin_barrier = Arc::clone(&pin_barrier);
            let start_barrier = Arc::clone(&start_barrier);
            let ready_tx = ready_tx.clone();
            let verified_tx = verified_tx.clone();
            let completed_tx = completed_tx.clone();
            std::thread::spawn(move || {
                ready_tx
                    .send((worker, current_thread_id()))
                    .expect("report worker task id");
                pin_barrier.wait();
                let expected_cpu = worker_cpu(worker).expect("supported worker index");
                assert_eq!(
                    current_allowed_list(),
                    expected_cpu.to_string(),
                    "worker affinity was not pinned"
                );
                verified_tx.send(()).expect("report verified affinity");
                let before = ThreadSnapshot::capture();
                start_barrier.wait();
                for index in 0..operations_per_worker {
                    operation(worker, index);
                }
                completed_tx.send(()).expect("report worker completion");
                let after = ThreadSnapshot::capture();
                WorkerMetrics::between(before, after)
            })
        })
        .collect::<Vec<_>>();
    drop(ready_tx);
    drop(verified_tx);
    drop(completed_tx);
    for _ in 0..workers {
        let (worker, task_id) = ready_rx
            .recv()
            .expect("worker exited before affinity setup");
        pin_task(worker_cpu(worker).expect("supported worker index"), task_id);
    }
    pin_barrier.wait();
    for _ in 0..workers {
        verified_rx
            .recv()
            .expect("worker exited before affinity verification");
    }
    let started = Instant::now();
    start_barrier.wait();
    for _ in 0..workers {
        completed_rx.recv().expect("benchmark worker exited early");
    }
    let elapsed = started.elapsed();
    let mut metrics = WorkerMetrics::default();
    for handle in handles {
        metrics.add(handle.join().expect("benchmark worker panicked"));
    }
    WorkerRun { elapsed, metrics }
}

macro_rules! measure_variant {
    ($db:ident, $family:expr, $workers:expr, $operations:expr, $root:expr, $label:expr) => {{
        let directory = tempfile::Builder::new()
            .prefix($label)
            .tempdir_in($root)
            .expect("create benchmark sample directory");
        match $family {
            Family::Value => {
                let store = Arc::new(
                    $db::key_value_store::DurableKeyValueStore::try_init_new(directory.path())
                        .expect("initialize key/value benchmark store")
                        .into_store(),
                );
                Measurement::capture(move || {
                    run_workers($workers, $operations, move |worker, operation| {
                        store.put(fixed_key(worker, operation), vec![0x51; 32]);
                    })
                })
            }
            Family::Set => {
                let store = Arc::new(
                    $db::key_set_store::DurableKeySetStore::try_init_new(directory.path())
                        .expect("initialize key/set benchmark store")
                        .into_store(),
                );
                Measurement::capture(move || {
                    run_workers($workers, $operations, move |worker, operation| {
                        store.append(fixed_key(worker, operation), vec![0x52; 32]);
                    })
                })
            }
            Family::Map => {
                let store = Arc::new(
                    $db::key_map_store::DurableKeyMapStore::try_init_new(directory.path())
                        .expect("initialize key/map benchmark store")
                        .into_store(),
                );
                Measurement::capture(move || {
                    run_workers($workers, $operations, move |worker, operation| {
                        store.put(
                            fixed_key(worker, operation),
                            $db::model::SearchKey::from(operation),
                            vec![0x53; 32],
                        );
                    })
                })
            }
        }
    }};
}

fn measure_baseline(
    family: Family,
    workers: usize,
    operations: usize,
    root: &Path,
    label: &str,
) -> Measurement {
    measure_variant!(baseline, family, workers, operations, root, label)
}

fn measure_candidate(
    family: Family,
    workers: usize,
    operations: usize,
    root: &Path,
    label: &str,
) -> Measurement {
    measure_variant!(candidate, family, workers, operations, root, label)
}

fn validate_linked(root: &Path) {
    for family in Family::ALL {
        for workers in [1, 8] {
            let baseline = measure_baseline(
                family,
                workers,
                VALIDATION_OPERATIONS,
                root,
                "validate-baseline",
            );
            let candidate = measure_candidate(
                family,
                workers,
                VALIDATION_OPERATIONS,
                root,
                "validate-candidate",
            );
            assert!(!baseline.elapsed.is_zero());
            assert!(!candidate.elapsed.is_zero());
        }
    }
}

fn run_capture(root: &Path) -> Vec<Row> {
    let mut rows = Vec::with_capacity(3 * 2 * MEASURED_PAIRS * 2);
    for family in Family::ALL {
        for workers in [1, 8] {
            println!("warming {}/{workers}", family.name());
            for pair in 0..WARMUP_PAIRS {
                if pair % 2 == 0 {
                    let _ = measure_baseline(
                        family,
                        workers,
                        OPERATIONS_PER_WORKER,
                        root,
                        "warm-baseline",
                    );
                    let _ = measure_candidate(
                        family,
                        workers,
                        OPERATIONS_PER_WORKER,
                        root,
                        "warm-candidate",
                    );
                } else {
                    let _ = measure_candidate(
                        family,
                        workers,
                        OPERATIONS_PER_WORKER,
                        root,
                        "warm-candidate",
                    );
                    let _ = measure_baseline(
                        family,
                        workers,
                        OPERATIONS_PER_WORKER,
                        root,
                        "warm-baseline",
                    );
                }
            }
            println!("capturing {}/{workers}", family.name());
            for pair in 0..MEASURED_PAIRS {
                let implementations = if pair % 2 == 0 {
                    ["baseline", "candidate"]
                } else {
                    ["candidate", "baseline"]
                };
                for (position, implementation) in implementations.into_iter().enumerate() {
                    let measurement = match implementation {
                        "baseline" => measure_baseline(
                            family,
                            workers,
                            OPERATIONS_PER_WORKER,
                            root,
                            "sample-baseline",
                        ),
                        "candidate" => measure_candidate(
                            family,
                            workers,
                            OPERATIONS_PER_WORKER,
                            root,
                            "sample-candidate",
                        ),
                        _ => unreachable!(),
                    };
                    rows.push(Row {
                        pair,
                        position,
                        family,
                        workers,
                        implementation,
                        measurement,
                    });
                }
            }
        }
    }
    rows
}

fn write_capture(path: &Path, capture_id: &str, rows: &[Row]) {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap_or_else(|error| panic!("create write-once output {}: {error}", path.display()));
    writeln!(
        output,
        "capture_id,baseline_commit,candidate_tree,coordinator_cpu,worker_cpus,pair,position,family,workers,implementation,operations,elapsed_ns,writes_per_second,worker_cpu_ticks,worker_voluntary_context_switches,worker_involuntary_context_switches,cpu_some_stall_us,io_some_stall_us,io_full_stall_us,load_one_before,load_one_after"
    )
    .unwrap();
    let candidate_tree = std::env::var("PIGMENT_DB_V2_CANDIDATE_TREE")
        .expect("PIGMENT_DB_V2_CANDIDATE_TREE is required");
    for row in rows {
        writeln!(
            output,
            "{capture_id},f5bf40e9861b544f867d5aa940fa52eb940b5e54,{candidate_tree},{COORDINATOR_CPU},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{},{:.2},{:.2}",
            worker_cpu_list(row.workers),
            row.pair,
            row.position,
            row.family.name(),
            row.workers,
            row.implementation,
            row.operations(),
            row.measurement.elapsed.as_nanos(),
            row.writes_per_second(),
            row.measurement.worker_cpu_ticks,
            row.measurement.worker_voluntary_context_switches,
            row.measurement.worker_involuntary_context_switches,
            row.measurement.cpu_some_stall_us,
            row.measurement.io_some_stall_us,
            row.measurement.io_full_stall_us,
            row.measurement.load_one_before,
            row.measurement.load_one_after,
        )
        .unwrap();
    }
    output.flush().unwrap();
}

fn main() {
    assert_eq!(
        current_allowed_list(),
        INITIAL_AFFINITY,
        "unexpected initial CPU affinity"
    );
    pin_task(COORDINATOR_CPU, std::process::id());
    assert_eq!(
        current_allowed_list(),
        COORDINATOR_CPU.to_string(),
        "coordinator affinity was not pinned"
    );
    let root = PathBuf::from(
        std::env::var_os("PIGMENT_DB_V2_BENCH_ROOT").expect("PIGMENT_DB_V2_BENCH_ROOT is required"),
    )
    .canonicalize()
    .expect("canonicalize benchmark root");
    validate_linked(&root);
    if std::env::var_os("PIGMENT_DB_V2_VALIDATE_ONLY").is_some() {
        println!("linked fixed-affinity validation passed for one and eight workers");
        return;
    }
    let capture_id =
        std::env::var("PIGMENT_DB_V2_CAPTURE_ID").expect("PIGMENT_DB_V2_CAPTURE_ID is required");
    let output = PathBuf::from(
        std::env::var_os("PIGMENT_DB_V2_OUTPUT").expect("PIGMENT_DB_V2_OUTPUT is required"),
    );
    assert!(!output.exists(), "capture output already exists");
    let rows = run_capture(&root);
    write_capture(&output, &capture_id, &rows);
}

#[cfg(test)]
mod tests {
    use super::{parse_allowed_list, worker_cpu};

    #[test]
    fn every_supported_worker_maps_to_one_distinct_reserved_cpu() {
        assert_eq!(worker_cpu(0), Some(12));
        assert_eq!(worker_cpu(7), Some(19));
        assert_eq!(worker_cpu(8), None);
    }

    #[test]
    fn affinity_parser_returns_the_effective_cpu_list() {
        let status = concat!(
            "Name:\tworker\n",
            "Cpus_allowed:\t00080000\n",
            "Cpus_allowed_list:\t19\n",
        );

        assert_eq!(parse_allowed_list(status), Some("19"));
    }
}
